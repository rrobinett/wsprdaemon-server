#!/usr/bin/env python3
"""wsprnet.org forwarder — post a station's WSPR spots on its behalf.

Runs on each wd{10,20,30}.  Polls `wsprdaemon.spots` for rows a station
asked us to forward, collapses multi-receiver duplicates, renders
wsprnet.org MEPT lines and POSTs them to /meptspots.php.  Only the
gw1-elected leader actually posts; the others idle but keep their
watermark fresh so failover is gap-free.  Mirrors
`pskreporter_forwarder.py` in every structural respect.

WHY THIS EXISTS, AND WHY IT SAVES MORE THAN IT LOOKS

A station today sends its WSPR spots TWICE: once inside the .tbz to the
WD servers, and again, separately, to wsprnet.org.  Same observations,
two connections, two formats, two transmissions.  For a site on a
metered or high-latency link the second copy is pure waste.

Everything a MEPT line needs is already in the extended row:

    date,time <- time          sync <- sync_quality    snr   <- snr
    dt        <- dt            freq <- frequency_mhz   call  <- tx_sign
    grid      <- tx_loc        pwr  <- power           drift <- drift
    ntype     <- code

So the wsprnet submission is DERIVABLE from data the server already
holds.  The station stops computing it, stops formatting it, stops
opening a connection for it, and stops sending it.  The duplicate copy
is not compressed -- it ceases to exist.

⛔ THE EXTENDED ROWS ARE NEVER DEDUPLICATED, AND THAT IS NOT AN OVERSIGHT.
A site running several receivers records each receiver's own view of the
same transmission, and that per-receiver detail IS the science -- it is
why `wsprdaemon.spots` exists as something richer than wsprnet.  Only
the DERIVED wsprnet product is collapsed, at the moment of posting.  One
transmission, two products, each built where it is needed.

⛔ DEDUP KEY MUST MATCH THE CLIENT'S.  hs_uploader's
`dedup_records_for_wsprnet` keys on (date, time, callsign, freq rounded
to 1e-4 MHz) and keeps the largest SNR, so that the producer-side audit
and the transport agree on counts.  This file reimplements that key
EXACTLY, because a server that deduped differently would post a
different set than the station's own audit expects and nobody could
reconcile the two.  Change one, change both.
"""
from __future__ import annotations

import argparse
import logging
import os
import signal
import socket
import sys
import time
import urllib.error
import urllib.request
from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Dict, List, Optional, Tuple

logger = logging.getLogger("wsprnet_forwarder")

VERSION = "1.0.0"

DEFAULT_LEADER_URL = "http://gw1.wsprdaemon.org/wsprnet-leader.txt"
DEFAULT_WATERMARK_PATH = Path("/var/lib/wsprdaemon/wsprnet_forwarder/watermark")
DEFAULT_REPORTERS_PATH = Path("/etc/wsprdaemon/wsprnet_reporters.json")
DEFAULT_DB_HOST = "localhost"
DEFAULT_DB_NATIVE_PORT = 9000
DEFAULT_POLL_SEC = 30
DEFAULT_LEADER_POLL_SEC = 60
DEFAULT_HTTP_TIMEOUT = 15
DEFAULT_URL = "http://wsprnet.org/meptspots.php"
DEFAULT_COLD_START_MINUTES = 5

# wsprnet.org's documented per-transaction cap, matching
# wd-upload-wsprnet's MAX_SPOTS and hs_uploader's MAX_SPOTS_PER_UPLOAD.
# Larger uploads are silently truncated by the far end, which is the
# worst possible failure: it looks like success.
MAX_SPOTS_PER_UPLOAD = 999
DEFAULT_BATCH = 2000          # rows pulled per poll, before dedup

# Byte-identical to wd-upload-wsprnet and hs_uploader.transports.wsprnet.
_BOUNDARY = b"--------WD4MeptBoundary"
_CRLF = b"\r\n"
_HASH_UNRESOLVED = "<...>"

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
    elected: Optional[str] = None
    last_fetch: float = 0.0
    last_error: Optional[str] = None


def fetch_leader(url: str, *, timeout: float = DEFAULT_HTTP_TIMEOUT) -> str:
    with urllib.request.urlopen(url, timeout=timeout) as resp:
        return resp.read().decode("utf-8", errors="replace").strip()


def is_self_leader(state: LeaderState, *, this_host: str,
                   pin: Optional[str] = None) -> bool:
    """Are we the elected leader?

    ⛔ NO LEADER INFORMATION MEANS NOT LEADER.  Losing contact with the
    elector is precisely when we must not have three servers each
    deciding they are in charge: wsprnet would get every spot three
    times, from three addresses, and no amount of its own dedup makes
    that a defensible thing to have done.
    """
    if pin is not None:
        return pin.strip().lower() == this_host.lower()
    if state.elected is None:
        return False
    return state.elected.lower() == this_host.lower()


# ── Watermark persistence ────────────────────────────────────────────────────

def read_watermark(path: Path) -> Optional[datetime]:
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
        logger.warning("watermark not ISO 8601 (%r); cold-starting", raw)
        return None
    return (ts if ts.tzinfo else ts.replace(tzinfo=timezone.utc)).astimezone(timezone.utc)


def write_watermark(path: Path, ts: datetime) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_suffix(path.suffix + ".tmp")
    tmp.write_text(ts.astimezone(timezone.utc).isoformat() + "\n")
    tmp.replace(path)


# ── Rows ─────────────────────────────────────────────────────────────────────

@dataclass
class SpotRow:
    """One wsprdaemon.spots row, normalised for the forwarder hot path."""
    time: datetime
    ingested_at: datetime
    rx_sign: str
    rx_loc: str
    rx_id: str
    tx_sign: str
    tx_loc: str
    frequency_mhz: float
    snr: Optional[float]
    drift: Optional[int]
    power: Optional[int]
    sync_quality: Optional[float]
    dt: Optional[float]
    code: Optional[int]


SELECT_COLUMNS = ("time, ingested_at, rx_sign, rx_loc, rx_id, tx_sign, tx_loc, "
                  "frequency_mhz, snr, drift, power, sync_quality, dt, code")


def fetch_pending(ch_client, *, since: datetime, batch_size: int,
                  db: str = "wsprdaemon", table: str = "spots") -> List[SpotRow]:
    """Rows newer than ``since`` that a station asked us to forward.

    The selector is `proxy_upload = 1`.  That column already exists and
    is already populated end to end — the WsprDaemon client sets field
    33 of the enhanced spot line, and the server parses it — so nothing
    here needs a schema migration, and a station that has not opted in
    is invisible to this query.
    """
    sql = (
        f"SELECT {SELECT_COLUMNS} FROM {db}.{table} "
        f"WHERE proxy_upload = 1 AND ingested_at > %(since)s "
        f"ORDER BY ingested_at, time, rx_sign, frequency_mhz "
        f"LIMIT %(lim)s"
    )
    out: List[SpotRow] = []
    for r in ch_client.execute(sql, {"since": since, "lim": batch_size}):
        (t, ing, rxs, rxl, rxid, txs, txl, fmhz,
         snr, drift, power, sync, dt, code) = r
        if not txs or not rxs:
            continue
        if t.tzinfo is None:
            t = t.replace(tzinfo=timezone.utc)
        if ing.tzinfo is None:
            ing = ing.replace(tzinfo=timezone.utc)
        out.append(SpotRow(
            time=t, ingested_at=ing,
            rx_sign=str(rxs), rx_loc=str(rxl or ""), rx_id=str(rxid or ""),
            tx_sign=str(txs).strip(), tx_loc=str(txl or "").strip(),
            frequency_mhz=float(fmhz or 0.0),
            snr=float(snr) if snr is not None else None,
            drift=int(drift) if drift is not None else None,
            power=int(power) if power is not None else None,
            sync_quality=float(sync) if sync is not None else None,
            dt=float(dt) if dt is not None else None,
            code=int(code) if code is not None else None,
        ))
    return out


# ── Dedup: collapse multi-receiver views of ONE transmission ────────────────

def dedup_for_wsprnet(rows: List[SpotRow]) -> List[SpotRow]:
    """One row per (date, time, callsign, freq@1e-4 MHz), best SNR wins.

    ⛔⛔ CALL THIS WITH ONE REPORTER'S ROWS ONLY.  The key carries no
    receiver identity, because on a station there is only one reporter and
    the duplicates being collapsed are its own receivers hearing one
    transmission.  Hand it a mixed batch — which is ALL this server ever
    reads from `wsprdaemon.spots` — and the first site to hear a
    transmission silences every other site's independent report of it.
    Measured on wd30, 2026-10-27: 2,752 real rows from 55 sites in one
    4-minute window collapsed to 388, an 86 % loss of exactly the
    multi-receiver coverage the network exists to produce, and wsprnet
    would have shown one receiver per transmission for the whole
    proxied fleet.  `forward_rows` therefore groups FIRST and dedups
    inside each group.

    ⛔ THIS KEY IS A SHARED CONTRACT, NOT A LOCAL CHOICE.  It is the key
    hs_uploader.transports.wsprnet.dedup_records_for_wsprnet uses, so the
    station's own audit and this server's posting agree on how many spots
    a cycle should yield.  Dedup differently here and the two counts
    diverge with no way to tell which is right.

    The quantisation is deliberate in both directions: 1e-4 MHz (~100 Hz)
    is finer than WSPR's bandwidth, so two transmitters genuinely on
    different carriers stay distinct; and coarser than wsprd's float
    output, so one transmitter seen by two receivers with negligible
    drift collapses to one spot.

    Ties on SNR keep the FIRST row seen, matching the audit table's
    INSERT-OR-IGNORE first-write-wins semantics.
    """
    best: Dict[Tuple, Tuple[int, SpotRow, float]] = {}
    passthrough: List[Tuple[int, SpotRow]] = []
    for pos, row in enumerate(rows):
        if not row.tx_sign or not row.frequency_mhz:
            passthrough.append((pos, row))
            continue
        key = (row.time.strftime("%y%m%d"), row.time.strftime("%H%M"),
               row.tx_sign, round(row.frequency_mhz, 4))
        snr = row.snr if row.snr is not None else float("-inf")
        prev = best.get(key)
        if prev is None or snr > prev[2]:
            best[key] = (prev[0] if prev else pos, row, snr)
    merged = [(p, r) for (p, r, _s) in best.values()] + passthrough
    merged.sort(key=lambda pr: pr[0])
    return [r for _p, r in merged]


# ── MEPT rendering ───────────────────────────────────────────────────────────

def row_to_mept(row: SpotRow) -> Optional[str]:
    """One wsprnet.org MEPT line, byte-compatible with wd-upload-wsprnet.

        "%-6s %4s %5.2f %3d %5.2f %12.7f %-14s %-6s %2d %2d %4d"
        date   time sync  snr  dt    freq       call   grid pwr drift ntype

    "<...>" is wsprd's placeholder for a hash callsign it could NOT
    resolve; posting it would attribute a reception to a station nobody
    can identify, so it is dropped.  An angle-bracketed RESOLVED
    callsign passes through — wsprnet accepts those and strips the
    brackets itself.
    """
    call = (row.tx_sign or "").strip()
    if not call or call == _HASH_UNRESOLVED:
        return None
    if not row.frequency_mhz:
        return None
    return "%-6s %4s %5.2f %3d %5.2f %12.7f %-14s %-6s %2d %2d %4d" % (
        row.time.strftime("%y%m%d"),
        row.time.strftime("%H%M"),
        row.sync_quality if row.sync_quality is not None else 0.0,
        int(round(row.snr)) if row.snr is not None else 0,
        row.dt if row.dt is not None else 0.0,
        row.frequency_mhz,
        call,
        (row.tx_loc or "").strip(),
        row.power if row.power is not None else 0,
        row.drift if row.drift is not None else 0,
        row.code if row.code is not None else 0,
    )


def build_multipart(*, version: str, call: str, grid: str,
                    allmept: bytes) -> bytes:
    """The multipart/form-data body wd-upload-wsprnet sends, byte for byte."""
    def text_field(name: str, value: str) -> bytes:
        return (b"--" + _BOUNDARY + _CRLF
                + b'Content-Disposition: form-data; name="' + name.encode() + b'"'
                + _CRLF + _CRLF + value.encode("utf-8") + _CRLF)
    return (text_field("version", version)
            + text_field("call", call)
            + text_field("grid", grid)
            + b"--" + _BOUNDARY + _CRLF
            + b'Content-Disposition: form-data; name="allmept"; '
            + b'filename="spots.txt"' + _CRLF
            + b"Content-Type: text/plain" + _CRLF + _CRLF
            + allmept + _CRLF
            + b"--" + _BOUNDARY + b"--" + _CRLF)


# ── Reporter identity ────────────────────────────────────────────────────────

def reporter_key(row: SpotRow) -> str:
    """The precise key naming ONE receiver: `<rx_sign>/<rx_id>`.

    ⛔ rx_id ALONE IS NOT A KEY.  It is a receiver name local to its
    station — `KA9Q_DXE`, `rx888-1` — so two unrelated sites routinely
    carry the same one, and an override filed under a bare rx_id would
    silently reassign another operator's spots to this site's callsign.
    Always qualify it with the rx_sign.
    """
    return f"{row.rx_sign}/{row.rx_id}"


def reporter_for(row: SpotRow, overrides: Dict[str, Dict[str, str]]
                 ) -> Tuple[str, str]:
    """The (call, grid) this site posts to wsprnet UNDER.

    ⛔ NOT ALWAYS THE SPOT'S rx_sign.  An operator running several
    receivers may report them to wsprnet under ONE identity, which can
    differ from any individual receiver's call.  That mapping is the one
    thing a MEPT line cannot be derived from the extended row, so it is
    carried per receiver rather than guessed.

    Default — and correct for every single-receiver site — is the row's
    own rx_sign/rx_loc.  Lookup is by `<rx_sign>/<rx_id>` first (one
    receiver) then `<rx_sign>` (every receiver at that site).
    """
    for key in (reporter_key(row), row.rx_sign):
        if key and key in overrides:
            o = overrides[key]
            return (o.get("call") or row.rx_sign, o.get("grid") or row.rx_loc)
    return (row.rx_sign, row.rx_loc)


def group_by_reporter(rows: List[SpotRow], overrides: Dict[str, Dict[str, str]]
                      ) -> Dict[Tuple[str, str], List[SpotRow]]:
    """Rows bucketed by the identity they must be POSTed under.

    One POST per reporter: the call/grid are multipart FORM fields, not
    per-line fields, so spots from two different reporters cannot share
    a transaction without misattributing one of them.
    """
    out: Dict[Tuple[str, str], List[SpotRow]] = {}
    for row in rows:
        out.setdefault(reporter_for(row, overrides), []).append(row)
    return out


def load_reporter_overrides(path: Path) -> Dict[str, Dict[str, str]]:
    """Per-site wsprnet identities, or {} when the file is absent.

    Absent is the normal case and must not be an error: every
    single-receiver site is handled correctly by the rx_sign default.
    A BROKEN file, though, is different from an absent one — it means
    someone intended an override and we would silently post under the
    wrong callsign.  That logs at ERROR and the overrides are empty, so
    the failure is loud rather than wrong-and-quiet.
    """
    import json
    try:
        raw = json.loads(path.read_text())
    except FileNotFoundError:
        return {}
    except Exception as exc:                        # noqa: BLE001
        logger.error("reporter overrides %s unreadable (%s) — posting under "
                     "each spot's own rx_sign", path, exc)
        return {}
    if not isinstance(raw, dict):
        logger.error("reporter overrides %s is not an object — ignoring", path)
        return {}
    return {k: v for k, v in raw.items() if isinstance(v, dict)}


def load_declared_identities(ch_client, *, db: str = "wsprdaemon",
                             table: str = "wsprnet_reporter"
                             ) -> Dict[str, Dict[str, str]]:
    """Identities STATIONS declared, via `routing.json` at ingest.

    This is the normal path: the operator of a station knows which
    callsign it reports under, and says so once in routing.json; the
    server records it here.  `load_reporter_overrides` stays as the
    server operator's manual escape hatch and is layered ON TOP, because
    a wd admin fixing a misattribution at 02:00 must not be undone by
    the next tar from the station that caused it.

    A query failure returns {} rather than raising: the fallback is each
    row's own rx_sign, which is right for every single-receiver site, so
    the degraded behaviour is correct for most sites and merely wrong in
    the way it was before anyone declared anything.
    """
    sql = (f"SELECT `key`, argMax(`call`, updated_at), "
           f"argMax(grid, updated_at) FROM {db}.{table} GROUP BY `key`")
    try:
        rows = ch_client.execute(sql)
    except Exception as exc:                        # noqa: BLE001
        logger.warning("declared identities unreadable (%s) — posting under "
                       "each spot's own rx_sign", exc)
        return {}
    out: Dict[str, Dict[str, str]] = {}
    for key, call, grid in rows:
        if key and call:
            out[str(key)] = {"call": str(call), "grid": str(grid or "")}
    return out


# ── Posting ──────────────────────────────────────────────────────────────────

def post_mept(url: str, body: bytes, *, timeout: float,
              opener=None) -> Tuple[bool, str]:
    """POST one multipart body.  Returns (ok, detail)."""
    req = urllib.request.Request(
        url, data=body,
        headers={"Content-Type":
                 "multipart/form-data; boundary=" + _BOUNDARY.decode(),
                 "Content-Length": str(len(body))})
    try:
        _open = opener or urllib.request.urlopen
        with _open(req, timeout=timeout) as resp:
            code = getattr(resp, "status", None) or resp.getcode()
            text = resp.read().decode("utf-8", errors="replace")[:200]
        return (200 <= int(code) < 300), f"HTTP {code}: {text.strip()}"
    except urllib.error.HTTPError as exc:
        return False, f"HTTP {exc.code}"
    except Exception as exc:                        # noqa: BLE001
        return False, f"{type(exc).__name__}: {exc}"


def forward_rows(rows: List[SpotRow], *, url: str, version: str,
                 overrides: Dict[str, Dict[str, str]],
                 timeout: float = DEFAULT_HTTP_TIMEOUT,
                 max_per_upload: int = MAX_SPOTS_PER_UPLOAD,
                 opener=None) -> Tuple[int, int]:
    """Dedup, group by reporter, POST.  Returns (posted, failed).

    ⛔ CHUNKED AT max_per_upload.  wsprnet SILENTLY TRUNCATES a larger
    transaction, which is the worst failure shape available: it returns
    success and drops the tail, so nothing anywhere reports a loss.
    """
    posted = failed = 0
    # ⛔ GROUP FIRST, THEN DEDUP WITHIN THE GROUP.  The dedup key has no
    # receiver in it (see dedup_for_wsprnet), so deduping the mixed batch
    # first would let one site's spot delete 54 other sites' independent
    # reports of the same transmission.
    for (call, grid), group in group_by_reporter(rows, overrides).items():
        group = dedup_for_wsprnet(group)
        lines = [ln for ln in (row_to_mept(r) for r in group) if ln]
        if not lines:
            continue
        for i in range(0, len(lines), max_per_upload):
            chunk = lines[i:i + max_per_upload]
            body = build_multipart(
                version=version, call=call, grid=grid,
                allmept=("\n".join(chunk) + "\n").encode())
            ok, detail = post_mept(url, body, timeout=timeout, opener=opener)
            if ok:
                posted += len(chunk)
                logger.info("posted %d spot(s) as %s/%s — %s",
                            len(chunk), call, grid, detail)
            else:
                failed += len(chunk)
                logger.warning("POST failed for %s/%s (%d spots): %s",
                               call, grid, len(chunk), detail)
    return posted, failed


# ── Run loop ─────────────────────────────────────────────────────────────────

@dataclass
class ForwarderConfig:
    this_host: str
    leader_url: str = DEFAULT_LEADER_URL
    pin_leader: Optional[str] = None
    watermark_path: Path = DEFAULT_WATERMARK_PATH
    reporters_path: Path = DEFAULT_REPORTERS_PATH
    db_host: str = DEFAULT_DB_HOST
    db_port: int = DEFAULT_DB_NATIVE_PORT
    db_user: str = "default"
    db_password: str = ""
    url: str = DEFAULT_URL
    poll_sec: int = DEFAULT_POLL_SEC
    leader_poll_sec: int = DEFAULT_LEADER_POLL_SEC
    batch: int = DEFAULT_BATCH
    http_timeout: float = DEFAULT_HTTP_TIMEOUT
    dry_run: bool = False
    once: bool = False


def _interruptible_sleep(seconds: float) -> None:
    deadline = time.monotonic() + seconds
    while not _STOP and time.monotonic() < deadline:
        time.sleep(min(1.0, deadline - time.monotonic()))


def run_loop(cfg: ForwarderConfig, *, ch_client_factory=None) -> int:
    """Poll, forward when leader, keep the watermark fresh when not.

    ⛔ A NON-LEADER STILL ADVANCES ITS WATERMARK.  If the idle servers
    let their watermark go stale, the moment one is promoted it would
    re-post every spot since the last time it led — wsprnet would get
    hours of duplicates from a new address at the worst possible moment.
    Failover has to be gap-free in BOTH directions: no hole, no replay.
    """
    _install_signal_handlers()
    if ch_client_factory is None:                   # pragma: no cover
        from clickhouse_driver import Client
        def ch_client_factory():
            return Client(host=cfg.db_host, port=cfg.db_port,
                          user=cfg.db_user, password=cfg.db_password)

    client = ch_client_factory()
    state = LeaderState()
    since = read_watermark(cfg.watermark_path)
    if since is None:
        since = datetime.now(timezone.utc) - timedelta(
            minutes=DEFAULT_COLD_START_MINUTES)
        logger.info("cold start — watermark set to %s", since.isoformat())

    was_leader: Optional[bool] = None
    while not _STOP:
        now = time.monotonic()
        # A pinned host answers the leader question by itself.  Probing the
        # elector anyway costs a network round trip per poll -- and, when
        # gw1 is unreachable, a full HTTP timeout per poll -- to compute an
        # answer the pin has already decided.
        if cfg.pin_leader is None and now - state.last_fetch >= cfg.leader_poll_sec:
            try:
                state.elected = fetch_leader(cfg.leader_url,
                                             timeout=cfg.http_timeout)
                state.last_error = None
            except Exception as exc:                # noqa: BLE001
                state.last_error = str(exc)
                logger.warning("leader probe failed (%s) — staying %s",
                               exc, "leader" if was_leader else "idle")
            state.last_fetch = now

        leader = is_self_leader(state, this_host=cfg.this_host,
                                pin=cfg.pin_leader)
        if leader != was_leader:
            logger.info("role: %s (elected=%s)",
                        "LEADER — forwarding" if leader else "idle",
                        state.elected)
            was_leader = leader

        try:
            rows = fetch_pending(client, since=since, batch_size=cfg.batch)
        except Exception as exc:                    # noqa: BLE001
            logger.error("ClickHouse query failed: %s", exc)
            rows = []

        if rows:
            high = max(r.ingested_at for r in rows)
            if leader and not cfg.dry_run:
                # Station declarations first, operator file on top.
                overrides = load_declared_identities(client)
                overrides.update(load_reporter_overrides(cfg.reporters_path))
                posted, failed = forward_rows(
                    rows, url=cfg.url, version=VERSION,
                    overrides=overrides, timeout=cfg.http_timeout)
                # ⛔ ADVANCE ONLY ON A CLEAN PASS.  Advancing past spots
                # that failed to post loses them with no record anywhere;
                # re-posting a duplicate is harmless, since wsprnet
                # de-duplicates on its own side.  Favour the duplicate.
                if failed == 0:
                    since = high
                    write_watermark(cfg.watermark_path, since)
                else:
                    logger.warning("%d spot(s) failed — holding the watermark "
                                   "so the next pass retries them", failed)
            else:
                if leader and cfg.dry_run:
                    logger.info("DRY RUN: would forward %d row(s)", len(rows))
                since = high
                write_watermark(cfg.watermark_path, since)

        if cfg.once:
            break
        _interruptible_sleep(cfg.poll_sec)
    return 0


def main(argv: Optional[List[str]] = None) -> int:
    p = argparse.ArgumentParser(description=__doc__.split("\n")[0])
    p.add_argument("--leader-url", default=os.environ.get(
        "WSPRNET_FORWARDER_LEADER_URL", DEFAULT_LEADER_URL))
    p.add_argument("--pin-leader", default=os.environ.get(
        "WSPRNET_FORWARDER_PIN_LEADER") or None,
        help="static leader override for staging; at most one host may set it")
    p.add_argument("--this-host", default=socket.gethostname().split(".")[0])
    p.add_argument("--watermark", type=Path, default=DEFAULT_WATERMARK_PATH)
    p.add_argument("--reporters", type=Path, default=DEFAULT_REPORTERS_PATH)
    p.add_argument("--clickhouse-host", default=DEFAULT_DB_HOST)
    p.add_argument("--clickhouse-port", type=int, default=DEFAULT_DB_NATIVE_PORT)
    p.add_argument("--clickhouse-user", default="default")
    p.add_argument("--clickhouse-password", default="")
    p.add_argument("--url", default=DEFAULT_URL)
    p.add_argument("--poll-sec", type=int, default=DEFAULT_POLL_SEC)
    p.add_argument("--batch", type=int, default=DEFAULT_BATCH)
    p.add_argument("--dry-run", action="store_true",
                   help="read and log, POST nothing")
    p.add_argument("--once", action="store_true")
    p.add_argument("-v", "--verbose", action="count", default=0)
    a = p.parse_args(argv)

    logging.basicConfig(
        level=logging.DEBUG if a.verbose else logging.INFO,
        format="%(asctime)s %(levelname)s:%(name)s:%(message)s")
    # -v raises OUR level, not the world's.  clickhouse-driver logs a line
    # per column per block at DEBUG: on a real batch that is thousands of
    # lines in which the forwarder's own handful cannot be found, so the
    # flag meant for diagnosing a problem is what hides it.
    for noisy in ("clickhouse_driver", "urllib3", "tzlocal"):
        logging.getLogger(noisy).setLevel(logging.WARNING)
    cfg = ForwarderConfig(
        this_host=a.this_host, leader_url=a.leader_url,
        pin_leader=a.pin_leader, watermark_path=a.watermark,
        reporters_path=a.reporters, db_host=a.clickhouse_host,
        db_port=a.clickhouse_port, db_user=a.clickhouse_user,
        db_password=a.clickhouse_password, url=a.url,
        poll_sec=a.poll_sec, batch=a.batch, dry_run=a.dry_run, once=a.once)
    logger.info("wsprnet_forwarder %s on %s → %s (leader: %s)",
                VERSION, cfg.this_host, cfg.url,
                cfg.pin_leader or cfg.leader_url)
    return run_loop(cfg)


if __name__ == "__main__":
    sys.exit(main())
