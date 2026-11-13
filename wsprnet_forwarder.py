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
import re
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

VERSION = "1.1.0"

DEFAULT_LEADER_URL = "http://gw1.wsprdaemon.org/wsprnet-leader.txt"
DEFAULT_WATERMARK_PATH = Path("/var/lib/wsprdaemon/wsprnet_forwarder/watermark")
DEFAULT_REPORTERS_PATH = Path("/etc/wsprdaemon/wsprnet_reporters.json")
DEFAULT_DB_HOST = "localhost"
DEFAULT_DB_NATIVE_PORT = 9000
DEFAULT_POLL_SEC = 30
DEFAULT_LEADER_POLL_SEC = 60
# ⛔ wsprnet is SLOW, and its own reply measures it: "Processing took 9033
# milliseconds" for 101 spots — about 89 ms per spot, so a full
# MAX_SPOTS_PER_UPLOAD upload is ~90 s of work on their side.  A timeout
# shorter than that does NOT fail safe: wsprnet ingests the spots anyway,
# we never see the reply, the watermark is held, and the next pass
# re-posts a batch wsprnet already has.  The backlog then grows, the next
# POST is bigger and times out sooner — self-reinforcing.  wd30 spent
# 2026-10-27 doing exactly that, at 4.3x the traffic for zero extra spots.
# Keep at least 2x the measured worst case (test_the_timeout_must_outlast_
# wsprnets_OWN_measured_processing_rate pins it).
#
# ⛔ 2026-11-13: wsprnet got far slower than that — 255 s for 123 spots from
# K9TRV-5 (2.1 s per spot), 157 s for 54 — and the 180 s timeout fired 353
# times in 24 h on 999-spot posts.  Each timeout held the watermark while
# wsprnet often went on recording, the next pass re-posted, and overlapping
# requests put ~0.3 % duplicates on wsprnet while the backlog grew to 2-3 h.
# So: post DEFAULT_MAX_PER_UPLOAD (250) at a time, wait up to 600 s, and after
# any failure hold off RETRY_HOLDOFF_SEC before posting again (run_loop).
# test_a_CHUNK_finishes_inside_the_timeout_at_wsprnets_SLOWEST_measured_rate
# pins the chunk/timeout pair to that measurement.
DEFAULT_HTTP_TIMEOUT = 600
RETRY_HOLDOFF_SEC = 300
DEFAULT_URL = "http://wsprnet.org/meptspots.php"
DEFAULT_COLD_START_MINUTES = 5

# wsprnet.org's documented per-transaction cap, matching
# wd-upload-wsprnet's MAX_SPOTS and hs_uploader's MAX_SPOTS_PER_UPLOAD.
# Larger uploads are silently truncated by the far end, which is the
# worst possible failure: it looks like success.
MAX_SPOTS_PER_UPLOAD = 999
# What we actually post per transaction: well under the cap, so one upload
# finishes inside DEFAULT_HTTP_TIMEOUT even when wsprnet is at its slowest
# (see DEFAULT_HTTP_TIMEOUT).  A smaller transaction also means a failure
# re-posts 250 spots, not 999.  Same figure as WD's client (8a48999).
DEFAULT_MAX_PER_UPLOAD = 250
# Sized against the batching window below: one 5-minute window across this
# fleet is ~8,000 rows (2.3 M WSPR spots/day), so a 2,000-row cap would have
# meant four passes per window and a forwarder that falls permanently behind
# the moment the fleet grows.
DEFAULT_BATCH = 20000         # rows pulled per fetch, before dedup

# ⚡ HOW OFTEN WE ACTUALLY POST, as opposed to how often we look.
#
# Measured on this fleet 2026-10-27: a wsprnet transaction costs ~1.5 kB of
# connection setup over plain HTTP and ~8.9 kB over HTTPS, against 74 B per
# spot.  Posting once per 2-minute cycle, as the stations do, means 79 % of a
# small site's upload is overhead — and it means this proxy posts exactly as
# OFTEN as the 85 clients it replaced, so the fleet-wide saving comes only
# from skipping TLS and would vanish the day we enable it.  Batching several
# cycles into one transaction is the saving that does not depend on that:
# 683.7 -> 179.6 MB/day fleet-wide at this interval.
#
# The cost is latency: a spot reaches wsprnet up to this long after its
# cycle.  wsprnet stores the spot's OWN timestamp, so nothing is distorted
# by the delay — only its appearance is later.  Set 0 to post every poll.
DEFAULT_POST_INTERVAL_SEC = 300

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
    version: Optional[str] = None


SELECT_COLUMNS = ("time, ingested_at, rx_sign, rx_loc, rx_id, tx_sign, tx_loc, "
                  "frequency_mhz, snr, drift, power, sync_quality, dt, code, "
                  "version")


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
         snr, drift, power, sync, dt, code, ver) = r
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
            version=str(ver) if ver else None,
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

# wsprdaemon.spots `code` (the station's pkt_mode) -> wsprnet's MEPT mode
# column.  The scales agree for WSPR-2 and FST4W-120 only: wsprdaemon numbers
# FST4W by period in minutes plus one, wsprnet by the period.  Same table as
# hs_uploader.wspr_modes.  A code not listed passes through as WSPR.
_WD_TO_WSPRNET_MODE = {2: 2, 3: 3, 6: 5, 16: 15, 31: 30}


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
    code = row.code if row.code is not None else 0
    head = (row.time.strftime("%y%m%d"), row.time.strftime("%H%M"),
            row.sync_quality if row.sync_quality is not None else 0.0)
    tail = (row.dt if row.dt is not None else 0.0, row.frequency_mhz, call,
            (row.tx_loc or "").strip(),
            row.power if row.power is not None else 0)
    snr = row.snr if row.snr is not None else 0
    ntype = _WD_TO_WSPRNET_MODE.get(code)
    if ntype is None or ntype == 2:
        return "%-6s %4s %5.2f %3d %5.2f %12.7f %-14s %-6s %2d %2d %4d" % (
            *head, int(round(snr)), *tail,
            row.drift if row.drift is not None else 0, code)
    # wd-decode's F-mode line: SNR %3.0f, drift written as 0.
    return "%-6s %4s %5.1f %3.0f %5.2f %12.7f %-14s %-6s %2d  0 %4d" % (
        *head, float(snr), *tail, ntype)


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

WSPRNET_VERSION_UNKNOWN = "WD_unknown"


def wsprnet_version(row: SpotRow) -> str:
    """The software version wsprnet is told produced this spot.

    ⛔ THE STATION'S VERSION, NEVER THIS FORWARDER'S.  wsprnet uses this
    field for software-population statistics, and a station that moves to
    server-side posting must not silently start reporting as something
    else — every proxied site would collapse into one bogus version and
    the real spread across the fleet would disappear from wsprnet's view.
    Stations send `WD_<version>` (hs-uploader
    `transports/wsprnet.py`: `version=f"WD_{self.version}"`), and the
    version each tar declared is already stored per row, so we reproduce
    exactly what the station itself would have sent.

    ⚠ Unknown is reported as unknown, not guessed.  Filling in a
    plausible "4.0" would be a false claim about someone else's software.
    In practice opting in requires an hs-uploader that always declares a
    version, so a missing one means something upstream changed.
    """
    v = (row.version or "").strip()
    return f"WD_{v}" if v else WSPRNET_VERSION_UNKNOWN


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
                      ) -> Dict[Tuple[str, str, str], List[SpotRow]]:
    """Rows bucketed by (call, grid, version) — one POST each.

    call, grid AND version are all multipart FORM fields, not per-line
    fields, so two rows that disagree on any of them cannot share a
    transaction without one of them being misreported.  Two reporters in
    one POST misattributes the spots; two versions in one POST tells
    wsprnet the wrong software produced half of them.

    In practice a site runs one version, so this splits nothing extra —
    except across an upgrade, which is exactly when it matters.
    """
    out: Dict[Tuple[str, str, str], List[SpotRow]] = {}
    for row in rows:
        call, grid = reporter_for(row, overrides)
        out.setdefault((call, grid, wsprnet_version(row)), []).append(row)
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


# ── Skip what wsprnet already holds (wspr.rx) ────────────────────────────────
#
# ⚡ WHY: a POST that times out on OUR side is usually ingested on wsprnet's
# anyway.  The watermark then (correctly) holds, and every later pass
# re-posts those rows.  wsprnet reads 100, finds 100 duplicates, hits its
# cutoff, and the cursor creeps forward by 100 rows per window.  Measured
# on wd30 2026-10-30: 9,005 pending rows, 4,102 already on wsprnet, about
# 7 hours to drain at one window per 5 minutes.  wd30 scrapes wsprnet's
# own public record into wspr.rx, so the forwarder can ask before posting.
#
# ⛔ THE FILTER IS A SAVING, NEVER A GATE.  If the lookup fails, the batch
# goes out unfiltered, exactly as before.  If the scraper lags, a spot just
# posted is not found yet and is posted again, as before.  Both cost only
# a missed saving.  The only way to LOSE a spot is a false match, so the
# key is exact and errs towards "not found".

# How far wspr.rx's `frequency` may sit from ours and still count as the same
# spot.  wsprnet keeps 6 decimals of MHz (1 Hz) and the scraper truncates
# int(mhz * 1e6), so a spot we posted as 14.0971234 comes back as 14097123
# or 14097122.  10 Hz covers that with margin, stays far below the 100 Hz
# dedup quantum, and keeps every band apart.
WSPR_RX_FREQ_TOLERANCE_HZ = 10


def _minute_epoch(t: datetime) -> int:
    """The spot's time as wsprnet stores it: the MEPT line's YYMMDD HHMM."""
    if t.tzinfo is None:
        t = t.replace(tzinfo=timezone.utc)
    return int(t.replace(second=0, microsecond=0).timestamp())


class WsprRxLookup:
    """Which of a batch's spots wsprnet already holds, per wspr.rx.

    The match key is (reporter call, tx_sign, minute, frequency ±10 Hz):

    * reporter call -- the `call` FORM FIELD we post under (after any
      override), because that is what wsprnet stores as Reporter.  The
      row's own rx_sign would look under a name wsprnet never saw.
    * tx_sign -- compared VERBATIM.  The scraper stores wsprnet's Reporter
      and CallSign as given, with no upper() and no suffix strip, so any
      normalisation here could only manufacture matches.  A bracketed
      "<CALL>" never matches, since wsprnet strips the brackets; that
      costs a duplicate, never a spot.
    * minute -- the YYMMDD HHMM the MEPT line carried, compared as epoch
      seconds so neither side's timezone setting can shift it.
    * frequency -- stands in for band.  wspr.rx's `band` is wsprnet's own
      code and wsprdaemon.spots' is metres, with no shared mapping in this
      repo; a 10 Hz window is tighter than any band and implies it.

    The query uses wspr.rx's sorting key prefix (rx_sign IN ...) and its
    partition column (time BETWEEN the batch's own first and last minute),
    so it reads only those reporters' granules in those months.

    A failure returns None and warns once per failure streak; the caller
    then posts unfiltered.
    """

    def __init__(self, ch_client, *, db: str = "wspr", table: str = "rx",
                 tolerance_hz: int = WSPR_RX_FREQ_TOLERANCE_HZ):
        self.ch = ch_client
        self.db, self.table = db, table
        self.tolerance_hz = tolerance_hz
        self.failing = False

    def __call__(self, probes: List[Tuple[str, SpotRow]]
                 ) -> Optional[List[bool]]:
        if not probes:
            return []
        minutes = [_minute_epoch(r.time) for _c, r in probes]
        sql = (f"SELECT rx_sign, tx_sign, toUnixTimestamp(time), frequency "
               f"FROM {self.db}.{self.table} "
               f"WHERE rx_sign IN %(calls)s "
               f"AND time BETWEEN toDateTime(%(t0)s) AND toDateTime(%(t1)s)")
        params = {"calls": tuple(sorted({c for c, _r in probes})),
                  "t0": min(minutes), "t1": max(minutes)}
        try:
            held: Dict[Tuple[str, str, int], List[int]] = {}
            for rxs, txs, epoch, freq in self.ch.execute(sql, params):
                held.setdefault((str(rxs), str(txs), int(epoch)),
                                []).append(int(freq))
        except Exception as exc:                    # noqa: BLE001
            if not self.failing:
                logger.warning("wspr.rx lookup failed (%s) — posting every "
                               "batch unfiltered until it recovers", exc)
            self.failing = True
            return None
        if self.failing:
            logger.info("wspr.rx lookup recovered — skipping spots wsprnet "
                        "already holds again")
            self.failing = False
        out = []
        for (call, row), minute in zip(probes, minutes):
            hz = round(row.frequency_mhz * 1e6)
            out.append(any(abs(f - hz) <= self.tolerance_hz
                           for f in held.get((call, row.tx_sign, minute), ())))
        return out


# ── Posting ──────────────────────────────────────────────────────────────────

_ADDED_RE = re.compile(r"(\d+)\s+out of\s+(\d+)\s+spot", re.I)
# ⛔ wsprnet's OWN words, captured from a real reply on wd30 2026-10-27:
#     "Upload limit of 1000 spots or 100 dupes reached.<br>
#      0 out of 100 spot(s) added<br>"
# It is not a spot cap — 999 per transaction is fine.  It is a DUPLICATE
# cutoff: after 100 dupes wsprnet abandons the rest of the upload, and the
# spots it never reached are NOT processed.  It still answers HTTP 200.
# Advancing the watermark over that tail loses it with no record anywhere,
# which is the exact failure shape this file has already been bitten by
# twice (dry-run, and the cross-site dedup).
_LIMIT_RE = re.compile(r"Upload limit of[^<.]*reached", re.I)


def _wsprnet_cut(text: str) -> Optional[int]:
    """How many spots wsprnet processed before giving up, or None.

    None means it read the whole upload.  A number means it stopped there
    and the remainder of that chunk was never looked at.
    """
    if not _LIMIT_RE.search(text):
        return None
    pairs = _ADDED_RE.findall(text)
    return sum(int(b) for _, b in pairs) if pairs else 0


def _wsprnet_verdict(text: str,
                     expected: Optional[int] = None) -> str:
    """wsprnet's answer, reduced to the one line that means anything.

    It replies with a full HTML page, so logging it raw puts a web page in
    the journal every cycle and buries the forwarder's own lines.  The
    sentence that matters is "N out of M spot(s) added": M is what we sent,
    N is what was new, and M-N is wsprnet de-duplicating against what the
    station or another receiver already reported.

    ⚠ `added=0` is NOT by itself a failure — re-posting after a watermark
    rewind legitimately adds nothing.  But a run of `0 out of M` when no
    one else is posting those spots means we are talking to wsprnet and
    being ignored, which looks identical to success at the HTTP layer.
    That is why the numbers are surfaced rather than just "HTTP 200".
    """
    pairs = _ADDED_RE.findall(text)
    if not pairs:
        return " ".join(text.split())[:160] or "(empty response)"
    # ⛔ SUM EVERY BLOCK.  wsprnet answers a large upload with one such
    # sentence PER BLOCK, so reading only the first under-reports by the
    # block size: a 999-spot upload that landed in full read back as
    # "0 of 100" and looked like a total duplicate rejection.
    added = sum(int(a) for a, _ in pairs)
    counted = sum(int(b) for _, b in pairs)
    verdict = f"{added} of {counted} spots added"
    if _LIMIT_RE.search(text):
        verdict += (f" — \u26a0 wsprnet STOPPED EARLY at {counted}"
                    + (f" of {expected}" if expected is not None else "")
                    + " (duplicate cutoff); the rest was NOT processed")
    elif expected is not None and counted != expected:
        # HTTP 200, a cheerful verdict, and a tail wsprnet never
        # acknowledged.  Nothing downstream can see this but us.
        verdict += (f" — \u26a0 SENT {expected}, wsprnet accounted for "
                    f"only {counted}")
    return verdict


def post_mept(url: str, body: bytes, *, timeout: float,
              expected: Optional[int] = None,
              opener=None) -> Tuple[bool, str, Optional[int]]:
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
            text = resp.read().decode("utf-8", errors="replace")
        return ((200 <= int(code) < 300),
                f"HTTP {code}: {_wsprnet_verdict(text, expected)}",
                _wsprnet_cut(text))
    except urllib.error.HTTPError as exc:
        return False, f"HTTP {exc.code}", None
    except Exception as exc:                        # noqa: BLE001
        return False, f"{type(exc).__name__}: {exc}", None


def forward_rows(rows: List[SpotRow], *, url: str, version: str = "",
                 overrides: Dict[str, Dict[str, str]],
                 timeout: float = DEFAULT_HTTP_TIMEOUT,
                 max_per_upload: int = DEFAULT_MAX_PER_UPLOAD,
                 opener=None,
                 on_wsprnet=None) -> Tuple[int, int, Optional[datetime]]:
    """Dedup, group by reporter, POST.

    Returns (posted, failed, stopped_at).  `stopped_at` is None when every
    chunk was read in full.  Otherwise it is the `ingested_at` of the LAST
    row wsprnet actually processed — the furthest the watermark may safely
    advance, because everything after it was never looked at.

    ⚠ `version` is NOT what gets POSTed.  Each group reports the
    STATION's own version (see wsprnet_version); this argument is kept
    only so callers that logged it still work.

    ⛔ CHUNKED AT max_per_upload.  wsprnet SILENTLY TRUNCATES a larger
    transaction, which is the worst failure shape available: it returns
    success and drops the tail, so nothing anywhere reports a loss.

    `on_wsprnet`, when given, is a WsprRxLookup.  Spots it finds already on
    wsprnet are not posted and count as delivered: they raise neither
    `posted` nor `failed`, so the caller's watermark passes over them as
    over accepted spots.  It returning None (lookup failed) posts all.
    """
    posted = failed = 0
    stopped_at: Optional[datetime] = None
    # ⛔ GROUP FIRST, THEN DEDUP WITHIN THE GROUP.  The dedup key has no
    # receiver in it (see dedup_for_wsprnet), so deduping the mixed batch
    # first would let one site's spot delete 54 other sites' independent
    # reports of the same transmission.
    #
    # Each row stays paired with its line, so a cut at line k names row k:
    # the rows posted, not the rows fetched, are what wsprnet counted.
    prepared = []
    for (call, grid, ver), group in group_by_reporter(rows, overrides).items():
        pairs = [(r, ln) for r in dedup_for_wsprnet(group)
                 for ln in (row_to_mept(r),) if ln]
        if pairs:
            prepared.append((call, grid, ver, pairs))

    if on_wsprnet is not None and prepared:
        probes = [(call, r) for call, _g, _v, pairs in prepared
                  for r, _ln in pairs]
        known = on_wsprnet(probes)
        if known is not None:
            flags = iter(known)
            prepared = [(c, g, v, [p for p in pairs if not next(flags)])
                        for c, g, v, pairs in prepared]
            logger.info("%d of %d already on wsprnet (wspr.rx); posting "
                        "the rest", sum(known), len(probes))

    for call, grid, ver, pairs in prepared:
        if not pairs:
            continue
        lines = [ln for _r, ln in pairs]
        for i in range(0, len(lines), max_per_upload):
            chunk = lines[i:i + max_per_upload]
            body = build_multipart(
                version=ver, call=call, grid=grid,
                allmept=("\n".join(chunk) + "\n").encode())
            ok, detail, cut = post_mept(url, body, timeout=timeout,
                                        expected=len(chunk), opener=opener)
            if not ok:
                failed += len(chunk)
                logger.warning("POST failed for %s/%s (%s, %d spots): %s",
                               call, grid, ver, len(chunk), detail)
                continue
            posted += len(chunk) if cut is None else cut
            logger.info("posted %d spot(s) as %s/%s (%s) — %s",
                        len(chunk), call, grid, ver, detail)
            if cut is None:
                continue
            # ⛔ wsprnet gave up partway.  Everything after row i+cut in
            # THIS group was never processed, so the watermark must stop
            # there — and there is no point posting this group's later
            # chunks, which would hit the same cutoff and add only
            # duplicates.  Take the EARLIEST such point across groups:
            # one cursor serves them all, so the safe answer is the most
            # conservative one.
            if cut <= 0:
                # It read none of this chunk.  There is no safe point to
                # advance to, so say FAILED and let the watermark hold
                # outright — the existing, already-tested path.
                failed += len(chunk)
                break
            here = pairs[min(i + cut - 1, len(pairs) - 1)][0].ingested_at
            if stopped_at is None or here < stopped_at:
                stopped_at = here
            break
    return posted, failed, stopped_at


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
    post_interval_sec: int = DEFAULT_POST_INTERVAL_SEC
    http_timeout: float = DEFAULT_HTTP_TIMEOUT
    max_per_upload: int = DEFAULT_MAX_PER_UPLOAD
    retry_holdoff_sec: float = RETRY_HOLDOFF_SEC
    dry_run: bool = False
    once: bool = False
    # Skip spots wspr.rx shows wsprnet already holds (see WsprRxLookup).
    skip_known: bool = True


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
    # One instance for the life of the loop, so a failing lookup warns once
    # per failure streak rather than once per pass.
    on_wsprnet = WsprRxLookup(client) if cfg.skip_known else None
    logger.info("skip spots already on wsprnet (wspr.rx): %s",
                "ON" if on_wsprnet else "OFF")
    since = read_watermark(cfg.watermark_path)
    if since is None:
        since = datetime.now(timezone.utc) - timedelta(
            minutes=DEFAULT_COLD_START_MINUTES)
        logger.info("cold start — watermark set to %s", since.isoformat())

    was_leader: Optional[bool] = None
    # Post on the first pass, then once per window.
    last_post = time.monotonic() - cfg.post_interval_sec
    # Set after a pass with failed POSTs: no posting before this moment.
    not_before = 0.0
    if cfg.post_interval_sec:
        logger.info("batching: fetch and post once per %d s "
                    "(polling every %d s)", cfg.post_interval_sec, cfg.poll_sec)
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

        # ⚡ THE BATCHING WINDOW.  We keep POLLING at poll_sec — that is
        # how the role stays current and how a shutdown stays responsive —
        # but only FETCH AND POST once per post_interval_sec.
        #
        # ⛔ The window is enforced by not QUERYING, never by holding rows
        # in memory.  Rows wait in ClickHouse, where they are already
        # durable, and the watermark is the only cursor.  An accumulator
        # would put minutes of unposted spots in a process that can be
        # restarted by a deploy, and they would be lost with the log line
        # that promised them — the same shape of failure the dry-run
        # watermark bug had.
        if now - last_post < cfg.post_interval_sec or now < not_before:
            _interruptible_sleep(cfg.poll_sec)
            continue
        last_post = now

        try:
            rows = fetch_pending(client, since=since, batch_size=cfg.batch)
        except Exception as exc:                    # noqa: BLE001
            logger.error("ClickHouse query failed: %s", exc)
            rows = []
        drained_full = len(rows) >= cfg.batch

        if rows:
            high = max(r.ingested_at for r in rows)
            if leader and not cfg.dry_run:
                # Station declarations first, operator file on top.
                overrides = load_declared_identities(client)
                overrides.update(load_reporter_overrides(cfg.reporters_path))
                posted, failed, stopped_at = forward_rows(
                    rows, url=cfg.url, version=VERSION,
                    overrides=overrides, timeout=cfg.http_timeout,
                    max_per_upload=cfg.max_per_upload,
                    on_wsprnet=on_wsprnet)
                # ⛔ ADVANCE ONLY ON A CLEAN PASS.  Advancing past spots
                # that failed to post loses them with no record anywhere;
                # re-posting a duplicate is harmless, since wsprnet
                # de-duplicates on its own side.  Favour the duplicate.
                if failed == 0 and stopped_at is not None:
                    # ⛔ wsprnet hit its 100-duplicate cutoff and abandoned
                    # the rest of the upload — while still answering 200.
                    # Advance only as far as it actually READ, never to
                    # `high`: the tail it never looked at must come back
                    # on the next pass, not be marked done.
                    since = stopped_at
                    write_watermark(cfg.watermark_path, since)
                    logger.warning(
                        "wsprnet stopped early (duplicate cutoff) — "
                        "watermark advanced only to %s, not %s, so the "
                        "unread tail is retried rather than skipped",
                        stopped_at.isoformat(), high.isoformat())
                elif failed == 0:
                    since = high
                    write_watermark(cfg.watermark_path, since)
                else:
                    # ⛔ HOLD OFF BEFORE THE RETRY.  A timed-out POST is often
                    # still being recorded by wsprnet, and re-posting while it
                    # is is exactly how both copies end up on wsprnet (wd30,
                    # 2026-11-13: 353 timeouts/day, ~0.3 % duplicates).  Wait
                    # long enough for it to finish; then the spots it did take
                    # are either skipped (wspr.rx) or refused as duplicates.
                    not_before = time.monotonic() + cfg.retry_holdoff_sec
                    logger.warning("%d spot(s) failed — holding the watermark "
                                   "and retrying in %d s, once wsprnet has "
                                   "finished with them", failed,
                                   cfg.retry_holdoff_sec)
            elif cfg.dry_run:
                # ⛔ DRY RUN MUST NOT ADVANCE THE WATERMARK.  It is the same
                # process that goes live later, and the unit file tells
                # operators to stage this way — so advancing here means the
                # flip to live silently skips exactly the spots they just
                # watched it promise to post.  Measured on wd30 2026-10-27:
                # 40 AC0G/B4 spots from cycle 03:06 were logged "would
                # forward", the watermark moved past them, and the live
                # restart never saw them again.
                logger.info("DRY RUN: would forward %d row(s) — watermark "
                            "HELD at %s so going live still posts them",
                            len(rows), since.isoformat())
            else:
                # Idle (not leader): keep the cursor fresh so a promotion is
                # gap-free.  This one MUST advance — a stale idle watermark
                # makes a newly promoted server replay hours of spots at the
                # worst possible moment.
                since = high
                write_watermark(cfg.watermark_path, since)

        if cfg.once:
            break
        if drained_full:
            # ⛔ A FULL BATCH MEANS THERE IS MORE.  Waiting the whole window
            # again would let the backlog grow by a window every window, and
            # the only symptom would be spots reaching wsprnet later and
            # later — never an error.  Go straight back for the rest.
            last_post = time.monotonic() - cfg.post_interval_sec
        _interruptible_sleep(cfg.poll_sec)
    return 0


def _env_flag(name: str) -> bool:
    """A staging switch readable from the unit's EnvironmentFile.

    ⚠ Only an explicit affirmative turns it on.  An operator who means
    to disable dry-run writes `=0` or `=false` far more often than they
    delete the line, and if those read as true the forwarder would keep
    silently posting nothing long after someone believed it was live.
    """
    return (os.environ.get(name) or "").strip().lower() in (
        "1", "true", "yes", "on")


def _env_flag_default_on(name: str) -> bool:
    """A switch that is ON unless the EnvironmentFile explicitly says no.

    The mirror of _env_flag, for a setting whose safe default is on: only
    `0`, `false`, `no` or `off` disables it, so a blank or garbled line
    leaves the default in place.
    """
    return (os.environ.get(name) or "").strip().lower() not in (
        "0", "false", "no", "off")


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
    p.add_argument("--post-interval-sec", type=int,
                   default=int(os.environ.get(
                       "WSPRNET_FORWARDER_POST_INTERVAL_SEC")
                       or DEFAULT_POST_INTERVAL_SEC),
                   help="seconds between wsprnet transactions; several WSPR "
                        "cycles ride one POST (0 = post every poll). "
                        "Env: WSPRNET_FORWARDER_POST_INTERVAL_SEC")
    p.add_argument("--dry-run", action="store_true",
                   default=_env_flag("WSPRNET_FORWARDER_DRY_RUN"),
                   help="read and log, POST nothing "
                        "(or set WSPRNET_FORWARDER_DRY_RUN=1)")
    p.add_argument("--skip-known", action=argparse.BooleanOptionalAction,
                   default=_env_flag_default_on("WSPRNET_FORWARDER_SKIP_KNOWN"),
                   help="before each POST, drop spots wspr.rx shows wsprnet "
                        "already holds (default on; "
                        "WSPRNET_FORWARDER_SKIP_KNOWN=0 disables)")
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
        poll_sec=a.poll_sec, batch=a.batch,
        post_interval_sec=max(0, a.post_interval_sec),
        dry_run=a.dry_run, once=a.once, skip_known=a.skip_known)
    logger.info("wsprnet_forwarder %s on %s → %s (leader: %s)",
                VERSION, cfg.this_host, cfg.url,
                cfg.pin_leader or cfg.leader_url)
    return run_loop(cfg)


if __name__ == "__main__":
    sys.exit(main())
