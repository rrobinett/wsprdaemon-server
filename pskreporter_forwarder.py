#!/usr/bin/env python3
"""PSKReporter forwarder (Phase 2 PR 2).

Runs on each wd{10,20,30} alongside wsprdaemon_server.  Polls
``psk.spots`` for rows the client wants forwarded to PSKReporter
(``forward_to_pskreporter = 1``) and ships them via the existing
ftlib-pskreporter library — same wire format the clients use
themselves on the direct path.

Leadership
----------
To avoid 3× delivery (one per wd server), only the elected leader
forwards.  Leadership comes from a small HTTP-published file on
gw1.wsprdaemon.org:

    http://gw1.wsprdaemon.org/pskreporter-leader.txt   →  "wd10\n"

The gw1 elector (``pskreporter_forward_elector.py``) writes that
file based on ClickHouse-port probes.  Non-leaders idle but keep
their watermark synced with the source clock so a fail-over takes
over with no replay gap.

Watermark
---------
Per-host file at ``/var/lib/wsprdaemon/pskreporter_forwarder/watermark``
stores one ISO-8601 UTC timestamp.  Each pass queries::

    SELECT ... FROM psk.spots
    WHERE forward_to_pskreporter = 1
      AND ingested_at > '<watermark>'
    ORDER BY ingested_at, mode, rx_sign, time, frequency
    LIMIT <batch>

Once a batch is enqueued into PskReporter, the watermark advances to
the batch's max ``ingested_at``.  PSKReporter de-dups on its end so
a small overlap window during failover (operator forces a leader
change) is harmless — Phil's dedup keys on time+freq+call+grid.

Wire format
-----------
``ftlib-pskreporter``'s ``PskReporter`` class takes a (callsign, grid,
antenna) station identity.  We instantiate one per (rx_sign, rx_loc)
seen in the batch, so PSKReporter sees the spots attributed to the
real receivers, not to wsprdaemon-server.  ``antenna`` is tagged
``"wsprdaemon-forwarded"`` so the audit trail is clear.
"""
from __future__ import annotations

import argparse
import logging
import os
import signal
import sys
import time
import urllib.request
from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Dict, List, Optional, Sequence, Tuple

logger = logging.getLogger("pskreporter-forwarder")


VERSION = "0.1.0"


# ── Constants / defaults ────────────────────────────────────────────────────

DEFAULT_LEADER_URL = "http://gw1.wsprdaemon.org/pskreporter-leader.txt"
DEFAULT_WATERMARK_PATH = Path("/var/lib/wsprdaemon/pskreporter_forwarder/watermark")
DEFAULT_DB_HOST = "localhost"
DEFAULT_DB_NATIVE_PORT = 9000
DEFAULT_BATCH = 500              # rows per poll cycle
DEFAULT_POLL_SEC = 30            # poll cadence when leader
DEFAULT_LEADER_POLL_SEC = 60     # leader-flag refresh cadence
DEFAULT_HTTP_TIMEOUT = 5
# How far back to look on the very first run when no watermark exists.
# Five minutes is enough to capture any in-flight cycle without
# back-flooding PSKReporter with day-old spots from a stale db.
COLD_START_LOOKBACK_SEC = 300


# Mode normalization: psk.spots stores lowercase "ft8"/"ft4"; PSKReporter
# expects uppercase mode tags. Anything we don't recognize is logged + skipped.
_MODE_MAP = {
    "ft8":    "FT8",
    "ft4":    "FT4",
    "msk144": "MSK144",
}


# ── Stop signal ─────────────────────────────────────────────────────────────

_STOP = False


def _install_signal_handlers() -> None:
    def _handle(signum, frame):
        global _STOP
        _STOP = True
        logger.info("signal %s received → shutdown requested", signum)
    signal.signal(signal.SIGTERM, _handle)
    signal.signal(signal.SIGINT, _handle)


# ── Leader source ───────────────────────────────────────────────────────────

@dataclass
class LeaderState:
    """Cached result of the most recent leader probe."""
    elected: Optional[str] = None      # short hostname, e.g. "wd10"
    last_fetch: float = 0.0
    last_error: Optional[str] = None


def fetch_leader(url: str, *, timeout: float = DEFAULT_HTTP_TIMEOUT) -> str:
    """Fetch the elected leader hostname from the gw1 elector.

    Returns the stripped hostname string.  Raises ``urllib.error.URLError``
    or ``OSError`` on transport failure — caller decides whether to
    treat that as "stay in current state" or "fail closed".
    """
    with urllib.request.urlopen(url, timeout=timeout) as resp:
        body = resp.read().decode("utf-8", errors="replace")
    return body.strip()


def is_self_leader(state: LeaderState, *, this_host: str) -> bool:
    """Are we currently the elected leader?

    On the conservative side: if we have NO valid leader info (cold
    start failed, gw1 unreachable, etc.), default to NOT-leader.  Loss
    of contact with gw1 is exactly when we DON'T want N servers each
    deciding they're in charge.
    """
    if state.elected is None:
        return False
    return state.elected.lower() == this_host.lower()


# ── Watermark persistence ────────────────────────────────────────────────────

def read_watermark(path: Path) -> Optional[datetime]:
    """Return the persisted UTC datetime or None on first-run / corrupt
    file.  Caller decides the cold-start fallback (typically `now - 5 min`).
    """
    try:
        raw = path.read_text().strip()
    except (FileNotFoundError, IsADirectoryError):
        return None
    except OSError as exc:
        logger.warning("watermark read failed (%s); cold-starting", exc)
        return None
    if not raw:
        return None
    try:
        ts = datetime.fromisoformat(raw.replace("Z", "+00:00"))
    except ValueError:
        logger.warning("watermark contents not ISO 8601 (%r); cold-starting", raw)
        return None
    if ts.tzinfo is None:
        ts = ts.replace(tzinfo=timezone.utc)
    return ts.astimezone(timezone.utc)


def write_watermark(path: Path, ts: datetime) -> None:
    """Atomically replace the watermark file with the given UTC timestamp."""
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_suffix(path.suffix + ".tmp")
    tmp.write_text(ts.astimezone(timezone.utc).isoformat() + "\n")
    tmp.replace(path)


# ── Spot fetch + forwarding ─────────────────────────────────────────────────

@dataclass
class PskRow:
    """A single psk.spots row, normalized for the forwarder hot path."""
    time: datetime
    ingested_at: datetime
    mode: str
    rx_sign: str
    rx_loc: str
    tx_call: str
    grid: str
    frequency: int        # Hz (absolute)
    snr_db: Optional[int]
    dt: Optional[float]


def fetch_pending(
    ch_client,
    *,
    since: datetime,
    batch_size: int,
    db: str = "psk",
    table: str = "spots",
) -> List[PskRow]:
    """Pull up to `batch_size` rows from psk.spots newer than `since`.

    Filter: ``forward_to_pskreporter = 1``.  Rows missing rx_sign or
    tx_call are quietly skipped — those aren't sendable to PSKReporter
    (no station-of-origin or no caller).  Time-ordering keeps the
    watermark advance simple.
    """
    sql = (
        f"SELECT time, ingested_at, mode, rx_sign, rx_loc, tx_call, "
        f"grid, frequency, snr_db, dt "
        f"FROM {db}.{table} "
        f"WHERE forward_to_pskreporter = 1 "
        f"  AND ingested_at > %(since)s "
        f"ORDER BY ingested_at, mode, rx_sign, time, frequency "
        f"LIMIT %(lim)s"
    )
    rows = ch_client.execute(sql, {"since": since, "lim": batch_size})
    out: List[PskRow] = []
    for r in rows:
        (rtime, ringested, rmode, rxs, rxl, txc,
         grid, freq, snr, dt) = r
        if not rxs or not txc:
            continue
        # ch-driver returns naive datetimes assuming the column's TZ
        # (DateTime is UTC by our schema); normalize to UTC-aware for
        # downstream arithmetic.
        if rtime.tzinfo is None:
            rtime = rtime.replace(tzinfo=timezone.utc)
        if ringested.tzinfo is None:
            ringested = ringested.replace(tzinfo=timezone.utc)
        out.append(PskRow(
            time=rtime, ingested_at=ringested, mode=str(rmode or "").lower(),
            rx_sign=str(rxs), rx_loc=str(rxl or ""),
            tx_call=str(txc), grid=str(grid or ""),
            frequency=int(freq or 0),
            snr_db=int(snr) if snr is not None else None,
            dt=float(dt) if dt is not None else None,
        ))
    return out


def _pskreporter_for_station(
    cache: Dict[Tuple[str, str], object],
    *,
    rx_sign: str,
    rx_loc: str,
    dummy: bool,
):
    """Get or create the ftlib PskReporter instance for this rx station.

    One instance per (rx_sign, rx_loc) — the library is keyed on the
    receiver's identity (callsign + grid).  Antenna tag identifies
    the forwarder path explicitly so the audit trail is unambiguous.
    """
    key = (rx_sign, rx_loc)
    inst = cache.get(key)
    if inst is None:
        from pskreporter import PskReporter   # ftlib-pskreporter
        inst = PskReporter(
            callsign=rx_sign,
            grid=rx_loc,
            antenna="wsprdaemon-forwarded",
            dummy=dummy,
            tcp=False,    # UDP is PSKReporter's primary path
        )
        cache[key] = inst
    return inst


def forward_rows(
    rows: Sequence[PskRow],
    *,
    psk_cache: Dict[Tuple[str, str], object],
    dummy: bool = False,
) -> int:
    """Enqueue rows into PskReporter (ftlib).  Returns count enqueued.

    Skips rows with an unrecognized mode and rows missing the spot
    timestamp.  Doesn't block on actual delivery — the library has
    its own background timer that fans out to UDP.
    """
    enq = 0
    for row in rows:
        mode_up = _MODE_MAP.get(row.mode)
        if mode_up is None:
            logger.debug("skipping unrecognized mode %r", row.mode)
            continue
        try:
            inst = _pskreporter_for_station(
                psk_cache, rx_sign=row.rx_sign, rx_loc=row.rx_loc,
                dummy=dummy,
            )
            inst.spot(
                callsign=row.tx_call,
                frequency=row.frequency,
                mode=mode_up,
                timestamp=row.time.timestamp(),
                db=row.snr_db,
                locator=row.grid or None,
                dt=row.dt,
            )
            enq += 1
        except Exception:
            logger.exception("PskReporter enqueue failed for %s/%s",
                             row.rx_sign, row.tx_call)
    return enq


# ── Main loop ───────────────────────────────────────────────────────────────

@dataclass
class ForwarderConfig:
    this_host: str
    leader_url: str = DEFAULT_LEADER_URL
    watermark_path: Path = DEFAULT_WATERMARK_PATH
    ch_host: str = DEFAULT_DB_HOST
    ch_port: int = DEFAULT_DB_NATIVE_PORT
    ch_user: str = "default"
    ch_password: str = ""
    batch_size: int = DEFAULT_BATCH
    poll_sec: int = DEFAULT_POLL_SEC
    leader_poll_sec: int = DEFAULT_LEADER_POLL_SEC
    dummy: bool = False
    psk_cache: Dict[Tuple[str, str], object] = field(default_factory=dict)


def run_loop(cfg: ForwarderConfig, *, ch_client_factory=None) -> int:
    """Main polling loop.  Returns the exit code on a clean stop."""
    if ch_client_factory is None:
        from clickhouse_driver import Client as _Cl
        def ch_client_factory():
            return _Cl(
                host=cfg.ch_host, port=cfg.ch_port,
                user=cfg.ch_user, password=cfg.ch_password,
                send_receive_timeout=60,
            )

    ch = ch_client_factory()
    watermark = read_watermark(cfg.watermark_path)
    if watermark is None:
        watermark = datetime.now(timezone.utc) - timedelta(seconds=COLD_START_LOOKBACK_SEC)
        logger.info("cold start: watermark seeded to %s", watermark.isoformat())

    leader = LeaderState()
    while not _STOP:
        now = time.monotonic()
        if now - leader.last_fetch >= cfg.leader_poll_sec or leader.elected is None:
            try:
                elected = fetch_leader(cfg.leader_url)
                if elected != (leader.elected or ""):
                    logger.info("leader: %r → %r", leader.elected, elected)
                leader.elected = elected
                leader.last_error = None
            except Exception as exc:
                leader.last_error = str(exc)
                logger.warning("leader fetch failed (%s); current=%r",
                               exc, leader.elected)
            leader.last_fetch = now

        if not is_self_leader(leader, this_host=cfg.this_host):
            # Idle.  Keep watermark fresh-ish so failover finds a small
            # lookback window, not a year-old replay.  Bump only when
            # the source has clearly moved past us.
            try:
                rows = fetch_pending(
                    ch, since=watermark, batch_size=1,
                    # Use a tiny LIMIT just to find the max ingested_at.
                )
                if rows:
                    new_wm = max(r.ingested_at for r in rows)
                    if new_wm > watermark:
                        watermark = new_wm
                        write_watermark(cfg.watermark_path, watermark)
            except Exception as exc:
                logger.warning("idle-watermark refresh failed: %s", exc)
            _interruptible_sleep(cfg.poll_sec)
            continue

        # Leader path: ship until either the batch goes empty or stop is set.
        try:
            rows = fetch_pending(
                ch, since=watermark, batch_size=cfg.batch_size,
            )
        except Exception as exc:
            logger.exception("fetch_pending failed: %s", exc)
            _interruptible_sleep(cfg.poll_sec)
            continue
        if not rows:
            _interruptible_sleep(cfg.poll_sec)
            continue
        enq = forward_rows(rows, psk_cache=cfg.psk_cache, dummy=cfg.dummy)
        new_wm = max(r.ingested_at for r in rows)
        watermark = new_wm
        write_watermark(cfg.watermark_path, watermark)
        logger.info("leader: enqueued %d/%d rows; watermark now %s",
                    enq, len(rows), watermark.isoformat())
        # If we got a full batch, loop again immediately to drain.
        if len(rows) >= cfg.batch_size:
            continue
        _interruptible_sleep(cfg.poll_sec)
    logger.info("stop requested; exiting")
    return 0


def _interruptible_sleep(seconds: float) -> None:
    """Sleep in small chunks so SIGTERM unblocks within ~1 s."""
    end = time.monotonic() + seconds
    while not _STOP and time.monotonic() < end:
        time.sleep(min(1.0, end - time.monotonic()))


# ── CLI ─────────────────────────────────────────────────────────────────────

def main(argv: Optional[List[str]] = None) -> int:
    p = argparse.ArgumentParser(
        description="PSKReporter forwarder (Phase 2 PR 2)")
    p.add_argument("--this-host", required=True,
                   help="this server's short hostname (e.g. wd10, wd20, wd30); "
                        "compared case-insensitively to the leader file")
    p.add_argument("--leader-url", default=DEFAULT_LEADER_URL,
                   help=f"URL of the gw1-published leader name "
                        f"(default: {DEFAULT_LEADER_URL})")
    p.add_argument("--watermark-path", default=str(DEFAULT_WATERMARK_PATH),
                   type=Path,
                   help=f"watermark file (default: {DEFAULT_WATERMARK_PATH})")
    p.add_argument("--clickhouse-host", default=DEFAULT_DB_HOST)
    p.add_argument("--clickhouse-port", type=int, default=DEFAULT_DB_NATIVE_PORT)
    p.add_argument("--clickhouse-user", default="default")
    p.add_argument("--clickhouse-password", default="")
    p.add_argument("--batch-size", type=int, default=DEFAULT_BATCH)
    p.add_argument("--poll-sec", type=int, default=DEFAULT_POLL_SEC)
    p.add_argument("--leader-poll-sec", type=int, default=DEFAULT_LEADER_POLL_SEC)
    p.add_argument("--dummy", action="store_true",
                   help="don't actually POST to PSKReporter (test mode); the "
                        "ftlib PskReporter prints spots to stdout instead")
    p.add_argument("--version", action="version", version=f"%(prog)s {VERSION}")
    p.add_argument("-v", "--verbose", action="count", default=0)
    args = p.parse_args(argv)

    level = logging.INFO if args.verbose == 0 else logging.DEBUG
    logging.basicConfig(
        level=level,
        format="%(asctime)s %(levelname)s %(name)s: %(message)s",
        datefmt="%Y-%m-%dT%H:%M:%S",
    )
    _install_signal_handlers()

    cfg = ForwarderConfig(
        this_host=args.this_host,
        leader_url=args.leader_url,
        watermark_path=args.watermark_path,
        ch_host=args.clickhouse_host,
        ch_port=args.clickhouse_port,
        ch_user=args.clickhouse_user,
        ch_password=args.clickhouse_password,
        batch_size=args.batch_size,
        poll_sec=args.poll_sec,
        leader_poll_sec=args.leader_poll_sec,
        dummy=args.dummy,
    )
    return run_loop(cfg)


if __name__ == "__main__":
    sys.exit(main())
