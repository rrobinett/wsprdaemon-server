#!/usr/bin/env python3
"""wsprnet_validator.py — which WSPR spots never reached wsprnet.org?

Every WD and Sigmond station sends its spots twice: once in its .tbz to the
WD servers (`wsprdaemon.spots`), and once to wsprnet.org, either directly
(WsprDaemon's own uploader, hs-uploader) or through `wsprnet_forwarder.py`
on a WD server.  `wsprnet_scraper.py` copies wsprnet's database back into
`wspr.rx`.  So every server already holds both sides, and a spot in
`wsprdaemon.spots` that is not in `wspr.rx` is a spot the station meant to
report and wsprnet never recorded.

Nothing compared the two until now.  The 2026-11-13 WSPRnet audit had to do
it by hand, and found the hs-uploader direct path losing 7-15 % of its spots
and the WD client duplicating 0.5 %, months after both began.  This service
does that comparison every hour and keeps the result:

  wsprdaemon.wsprnet_validation   per (hour, reporter): spots we hold, how
                                  many wsprnet has, how many are missing,
                                  wsprnet-only rows, duplicate rows
  wsprdaemon.wsprnet_missing      one row per missing spot (30-day TTL)
  wsprdaemon.wsprnet_missing_current
                                  view: only the latest verdict per hour

and logs a WARNING per reporter whose losses or duplicates cross a
threshold, or which stopped appearing on wsprnet altogether.

How a spot is matched (the same rules as the forwarder's `WsprRxLookup`,
and as the `wsprdaemon.rx_ext` view):

  * reporter: the identity the station reports UNDER — the row's rx_sign,
    unless the station declared another in `wsprdaemon.wsprnet_reporter`
    or the operator's overrides file says so (`<rx_sign>/<rx_id>` first,
    then `<rx_sign>`), exactly as `wsprnet_forwarder.reporter_for()`.
  * band: wsprnet's code, integer MHz (-1 below 300 kHz).
  * tx_sign: angle brackets stripped; the unresolved hash `<...>` is never
    posted, so it is never expected.
  * time: the cycle minute.
  Multi-receiver rows of one transmission collapse to one expected spot.

What it does NOT prove:

  * A spot that wsprnet rejected on purpose (no grid, a bad call) counts as
    missing.  Those are rare (WB6CXC/7: 1 per 2-minute cycle) and steady,
    so a rise is still a real signal.
  * A reporter whose spots appear on wsprnet under a different call looks
    absent.  `extra_on_wsprnet` (rows wsprnet has under the reporter that we
    do not) and the absent status make that visible rather than hiding it.
  * `wspr.rx` lags wsprnet by minutes to hours, so hours newer than
    `--settle-hours` are not judged, and each run re-judges the last
    `--lookback-hours`, so a late scrape replaces an early "missing".

Run hourly by `wsprnet-validator.timer`.  Ad hoc:

    wsprnet_validator.py run --dry-run          # judge, print, write nothing
    wsprnet_validator.py report --hours 24      # table from stored results
    wsprnet_validator.py missing --reporter AB4EJ --hours 24
"""
from __future__ import annotations

import argparse
import logging
import os
import sys
from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Dict, Iterable, List, Optional, Sequence, Tuple

sys.path.insert(0, os.path.dirname(os.path.realpath(__file__)))

VERSION = "1.0.0"

logger = logging.getLogger("wsprnet_validator")

DEFAULT_LOOKBACK_HOURS = 30
DEFAULT_SETTLE_HOURS = 4
DEFAULT_REPORTERS_PATH = Path("/etc/wsprdaemon/wsprnet_reporters.json")
DEFAULT_STATE_DIR = Path("/var/lib/wsprdaemon/wsprnet_validator")

# Alert thresholds, judged over the newest ALERT_WINDOW_HOURS settled hours.
ALERT_WINDOW_HOURS = 6
ALERT_MISSING_PCT = 2.0
ALERT_DUP_PCT = 1.0
# Fewer expected spots than this in a window says nothing either way.
MIN_SPOTS_FOR_STATUS = 30
# Below this fraction found, the reporter is not on wsprnet under this call
# at all (uploads switched off, or posted under another call) rather than
# losing spots.  A station that was reporting and drops below it is alerted.
ABSENT_FRACTION = 0.10

DB = "wsprdaemon"

DDL = [
    f"""
CREATE TABLE IF NOT EXISTS {DB}.wsprnet_validation
(
    `hour`             DateTime                CODEC(Delta(4), ZSTD(3)),
    `reporter`         LowCardinality(String),
    `proxy`            UInt8,
    `wd_spots`         UInt32,
    `on_wsprnet`       UInt32,
    `missing`          UInt32,
    `wsprnet_keys`     UInt32,
    `extra_on_wsprnet` UInt32,
    `dup_identical`    UInt32,
    `dup_diff_snr`     UInt32,
    `checked_at`       DateTime                CODEC(Delta(4), ZSTD(3))
)
ENGINE = ReplacingMergeTree(checked_at)
PARTITION BY toYYYYMM(hour)
ORDER BY (hour, reporter)
TTL hour + INTERVAL 180 DAY
""",
    f"""
CREATE TABLE IF NOT EXISTS {DB}.wsprnet_missing
(
    `hour`        DateTime                CODEC(Delta(4), ZSTD(3)),
    `reporter`    LowCardinality(String),
    `time`        DateTime                CODEC(Delta(4), ZSTD(3)),
    `band`        Int16,
    `tx_sign`     LowCardinality(String),
    `rx_sign`     LowCardinality(String),
    `rx_id`       LowCardinality(String),
    `frequency`   UInt64                  CODEC(Delta(8), ZSTD(3)),
    `snr`         Int8,
    `proxy`       UInt8,
    `checked_at`  DateTime                CODEC(Delta(4), ZSTD(3))
)
ENGINE = MergeTree
PARTITION BY toYYYYMM(hour)
ORDER BY (hour, reporter, time, band, tx_sign)
TTL hour + INTERVAL 30 DAY
""",
    # Each run re-judges every hour in its window with one checked_at, and
    # writes the missing rows BEFORE the summary.  So the newest summary
    # checked_at for an hour names the one complete verdict for that hour;
    # a run that died between the two inserts is simply not visible.
    f"""
CREATE VIEW IF NOT EXISTS {DB}.wsprnet_missing_current AS
SELECT * FROM {DB}.wsprnet_missing
WHERE (hour, checked_at) IN
    (SELECT hour, max(checked_at) FROM {DB}.wsprnet_validation GROUP BY hour)
""",
]


# ── identity ────────────────────────────────────────────────────────────────

def identity_map(declared: Dict[str, Dict[str, str]],
                 overrides: Dict[str, Dict[str, str]]
                 ) -> Tuple[List[str], List[str]]:
    """(keys, calls) for SQL `transform()`: the call each key reports under.

    Overrides win over station declarations, as in the forwarder.  An entry
    with no call is dropped (a grid alone cannot name a reporter).  A
    sentinel '' -> '' pair keeps both arrays non-empty, because ClickHouse
    cannot type an empty array literal for transform().
    """
    merged: Dict[str, str] = {}
    for source in (declared, overrides):
        for key, ident in source.items():
            call = (ident or {}).get("call")
            if key and call:
                merged[key] = call
    keys = [""] + sorted(merged)
    return keys, [""] + [merged[k] for k in keys[1:]]


# ── SQL ─────────────────────────────────────────────────────────────────────

# reporter_for(): `<rx_sign>/<rx_id>` first, then `<rx_sign>`, else rx_sign.
_REPORTER_EXPR = (
    "transform(concat(toString(rx_sign), '/', toString(rx_id)), "
    "%(id_keys)s, %(id_calls)s, "
    "transform(toString(rx_sign), %(id_keys)s, %(id_calls)s, "
    "toString(rx_sign)))")

# What each station expects on wsprnet: one spot per (reporter, minute, band,
# tx_sign), whichever receiver heard it best.
_WD_SUB = f"""
    SELECT {_REPORTER_EXPR} AS reporter,
           time,
           toInt16(if(frequency < 300000, -1,
                      toInt64(intDiv(frequency, 1000000)))) AS band,
           replaceRegexpOne(toString(tx_sign), '^<(.+)>$', '\\\\1') AS txs,
           max(proxy_upload) AS proxy,
           argMax(toString(rx_sign), snr) AS src_rx_sign,
           argMax(toString(rx_id), snr) AS src_rx_id,
           argMax(frequency, snr) AS freq,
           max(snr) AS best_snr
    FROM {DB}.spots
    WHERE time >= %(t0)s AND time < %(t1)s
      AND tx_sign != '<...>' AND frequency > 0
    GROUP BY reporter, time, band, txs
"""

# What wsprnet recorded, as the scraper copied it back.
_WN_SUB = """
    SELECT toString(rx_sign) AS reporter, time, band,
           toString(tx_sign) AS txs,
           count() AS n,
           max(snr) = min(snr) AS same
    FROM wspr.rx
    WHERE time >= %(t0)s AND time < %(t1)s
    GROUP BY reporter, time, band, txs
"""

_JOINED = f"""
    SELECT w.*, r.n AS n, r.same AS same
    FROM ({_WD_SUB}) AS w
    LEFT JOIN ({_WN_SUB}) AS r USING (reporter, time, band, txs)
"""

SUMMARY_SELECT = f"""
SELECT j.hour, j.reporter, j.proxy, j.wd_spots, j.on_wsprnet, j.missing,
       k.wsprnet_keys,
       toUInt32(greatest(toInt64(k.wsprnet_keys) - j.on_wsprnet, 0))
           AS extra_on_wsprnet,
       j.dup_identical, j.dup_diff_snr,
       toDateTime(%(checked_at)s) AS checked_at
FROM (
    SELECT toStartOfHour(time) AS hour, reporter,
           max(proxy) AS proxy,
           toUInt32(count()) AS wd_spots,
           toUInt32(countIf(n > 0)) AS on_wsprnet,
           toUInt32(countIf(n = 0)) AS missing,
           toUInt32(sumIf(n - 1, n > 1 AND same)) AS dup_identical,
           toUInt32(sumIf(n - 1, n > 1 AND NOT same)) AS dup_diff_snr
    FROM ({_JOINED})
    GROUP BY hour, reporter
) AS j
LEFT JOIN (
    SELECT toStartOfHour(time) AS hour, reporter,
           toUInt32(count()) AS wsprnet_keys
    FROM ({_WN_SUB})
    GROUP BY hour, reporter
) AS k USING (hour, reporter)
ORDER BY j.hour, j.reporter
SETTINGS join_use_nulls = 0
"""

MISSING_SELECT = f"""
SELECT toStartOfHour(time) AS hour, reporter, time, band, txs AS tx_sign,
       src_rx_sign AS rx_sign, src_rx_id AS rx_id, freq AS frequency,
       best_snr AS snr, proxy, toDateTime(%(checked_at)s) AS checked_at
FROM ({_JOINED})
WHERE n = 0
SETTINGS join_use_nulls = 0
"""

SUMMARY_COLUMNS = ("hour", "reporter", "proxy", "wd_spots", "on_wsprnet",
                   "missing", "wsprnet_keys", "extra_on_wsprnet",
                   "dup_identical", "dup_diff_snr", "checked_at")

STORED_SUMMARY = f"""
SELECT reporter, max(proxy), sum(wd_spots), sum(on_wsprnet), sum(missing),
       sum(extra_on_wsprnet), sum(dup_identical), sum(dup_diff_snr)
FROM {DB}.wsprnet_validation FINAL
WHERE hour >= %(t0)s AND hour < %(t1)s
GROUP BY reporter
"""


# ── judging ─────────────────────────────────────────────────────────────────

@dataclass
class Tally:
    reporter: str
    proxy: int = 0
    wd_spots: int = 0
    on_wsprnet: int = 0
    missing: int = 0
    extra_on_wsprnet: int = 0
    dup_identical: int = 0
    dup_diff_snr: int = 0

    @property
    def missing_pct(self) -> float:
        return 100.0 * self.missing / self.wd_spots if self.wd_spots else 0.0

    @property
    def dup_pct(self) -> float:
        return (100.0 * self.dup_identical / self.on_wsprnet
                if self.on_wsprnet else 0.0)

    @property
    def status(self) -> str:
        """'reporting', 'absent' (not on wsprnet under this call), or 'few'."""
        if self.wd_spots < MIN_SPOTS_FOR_STATUS:
            return "few"
        if self.on_wsprnet < ABSENT_FRACTION * self.wd_spots:
            return "absent"
        return "reporting"


def tallies_from_rows(rows: Iterable[Sequence]) -> Dict[str, Tally]:
    out: Dict[str, Tally] = {}
    for (rep, proxy, wd, on, miss, extra, dup_i, dup_d) in rows:
        out[str(rep)] = Tally(str(rep), int(proxy), int(wd), int(on),
                              int(miss), int(extra), int(dup_i), int(dup_d))
    return out


def judge(recent: Dict[str, Tally], earlier: Dict[str, Tally]
          ) -> List[str]:
    """Alert lines for the recent window; `earlier` decides 'stopped'."""
    alerts: List[str] = []
    for rep in sorted(recent):
        t = recent[rep]
        path = "proxied" if t.proxy else "direct"
        if t.status == "absent":
            before = earlier.get(rep)
            if before is not None and before.status == "reporting":
                alerts.append(
                    f"{rep} ({path}): STOPPED appearing on wsprnet — "
                    f"{t.on_wsprnet} of {t.wd_spots} spots found, was "
                    f"{100 - before.missing_pct:.1f}% before")
            continue
        if t.status != "reporting":
            continue
        if t.missing_pct > ALERT_MISSING_PCT:
            alerts.append(
                f"{rep} ({path}): {t.missing} of {t.wd_spots} spots "
                f"({t.missing_pct:.1f}%) missing from wsprnet")
        if t.dup_pct > ALERT_DUP_PCT:
            alerts.append(
                f"{rep} ({path}): {t.dup_identical} identical duplicate "
                f"rows on wsprnet ({t.dup_pct:.1f}%)")
    return alerts


def format_table(tallies: Dict[str, Tally]) -> str:
    head = (f"{'reporter':<16} {'path':<7} {'status':<9} {'ours':>7} "
            f"{'wsprnet':>7} {'missing':>7} {'miss%':>6} {'dup':>5} "
            f"{'extra':>6}")
    lines = [head, "-" * len(head)]
    order = sorted(tallies.values(),
                   key=lambda t: (t.status != "reporting", -t.missing_pct,
                                  t.reporter))
    for t in order:
        lines.append(
            f"{t.reporter:<16.16} {'proxy' if t.proxy else 'direct':<7} "
            f"{t.status:<9} {t.wd_spots:>7} {t.on_wsprnet:>7} "
            f"{t.missing:>7} {t.missing_pct:>6.2f} {t.dup_identical:>5} "
            f"{t.extra_on_wsprnet:>6}")
    return "\n".join(lines)


# ── windows ─────────────────────────────────────────────────────────────────

def floor_hour(t: datetime) -> datetime:
    return t.replace(minute=0, second=0, microsecond=0)


def run_window(now: datetime, lookback_h: int, settle_h: int
               ) -> Tuple[datetime, datetime]:
    """[t0, t1): whole hours, the newest `settle_h` left for the scraper."""
    if lookback_h <= settle_h:
        raise ValueError("lookback must exceed settle")
    t1 = floor_hour(now) - timedelta(hours=settle_h)
    return t1 - timedelta(hours=lookback_h - settle_h), t1


def _naive_utc(t: datetime) -> datetime:
    """clickhouse_driver sends naive datetimes as-is; keep them UTC."""
    return t.astimezone(timezone.utc).replace(tzinfo=None)


# ── the run ─────────────────────────────────────────────────────────────────

@dataclass
class Config:
    lookback_hours: int = DEFAULT_LOOKBACK_HOURS
    settle_hours: int = DEFAULT_SETTLE_HOURS
    reporters_path: Path = DEFAULT_REPORTERS_PATH
    state_dir: Optional[Path] = DEFAULT_STATE_DIR
    dry_run: bool = False


def setup_tables(ch) -> None:
    for stmt in DDL:
        ch.execute(stmt)


def load_identities(ch, reporters_path: Path) -> Tuple[List[str], List[str]]:
    import wsprnet_forwarder as wf
    declared = wf.load_declared_identities(ch)
    overrides = wf.load_reporter_overrides(reporters_path)
    return identity_map(declared, overrides)


def stored_tallies(ch, t0: datetime, t1: datetime) -> Dict[str, Tally]:
    return tallies_from_rows(ch.execute(
        STORED_SUMMARY, {"t0": _naive_utc(t0), "t1": _naive_utc(t1)}))


def validate(ch, cfg: Config, now: Optional[datetime] = None) -> int:
    now = now or datetime.now(timezone.utc)
    t0, t1 = run_window(now, cfg.lookback_hours, cfg.settle_hours)
    keys, calls = load_identities(ch, cfg.reporters_path)
    params = {"t0": _naive_utc(t0), "t1": _naive_utc(t1),
              "checked_at": _naive_utc(now.replace(microsecond=0)),
              "id_keys": keys, "id_calls": calls}
    logger.info("judging %s .. %s UTC (%d identity mappings)",
                t0.strftime("%Y-%m-%d %H:%M"), t1.strftime("%Y-%m-%d %H:%M"),
                len(keys) - 1)

    if cfg.dry_run:
        rows = ch.execute(SUMMARY_SELECT, params)
        agg: Dict[str, Tally] = {}
        for r in rows:
            rec = dict(zip(SUMMARY_COLUMNS, r))
            t = agg.setdefault(rec["reporter"], Tally(rec["reporter"]))
            t.proxy = max(t.proxy, int(rec["proxy"]))
            for f in ("wd_spots", "on_wsprnet", "missing",
                      "extra_on_wsprnet", "dup_identical", "dup_diff_snr"):
                setattr(t, f, getattr(t, f) + int(rec[f]))
        print(format_table(agg))
        return 0

    setup_tables(ch)
    # Missing rows first: the summary's checked_at is what publishes them.
    ch.execute(f"INSERT INTO {DB}.wsprnet_missing " + MISSING_SELECT, params)
    ch.execute(f"INSERT INTO {DB}.wsprnet_validation " + SUMMARY_SELECT,
               params)

    a0 = t1 - timedelta(hours=ALERT_WINDOW_HOURS)
    recent = stored_tallies(ch, a0, t1)
    earlier = stored_tallies(ch, a0 - timedelta(hours=24), a0)
    alerts = judge(recent, earlier)
    for line in alerts:
        logger.warning("%s", line)

    total = sum(t.wd_spots for t in recent.values()
                if t.status == "reporting")
    lost = sum(t.missing for t in recent.values() if t.status == "reporting")
    logger.info("last %d settled hours: %d reporters, %d of %d spots missing "
                "(%.2f%%), %d alerts", ALERT_WINDOW_HOURS, len(recent), lost,
                total, 100.0 * lost / total if total else 0.0, len(alerts))

    if cfg.state_dir is not None:
        write_report(cfg.state_dir, ch, t1, alerts, now)
    return 0


def write_report(state_dir: Path, ch, t1: datetime, alerts: List[str],
                 now: datetime) -> None:
    """latest.txt: the last settled 24 h, for a human with ssh."""
    day = stored_tallies(ch, t1 - timedelta(hours=24), t1)
    text = (f"wsprnet_validator {VERSION}, run {now:%Y-%m-%d %H:%M} UTC\n"
            f"window {t1 - timedelta(hours=24):%Y-%m-%d %H:%M} .. "
            f"{t1:%Y-%m-%d %H:%M} UTC\n\n"
            + ("ALERTS (last %d h):\n  " % ALERT_WINDOW_HOURS
               + "\n  ".join(alerts) + "\n\n" if alerts else
               "No alerts in the last %d h.\n\n" % ALERT_WINDOW_HOURS)
            + format_table(day) + "\n")
    try:
        state_dir.mkdir(parents=True, exist_ok=True)
        tmp = state_dir / "latest.txt.tmp"
        tmp.write_text(text)
        tmp.replace(state_dir / "latest.txt")
    except OSError as exc:
        logger.warning("could not write %s/latest.txt: %s", state_dir, exc)


# ── CLI ─────────────────────────────────────────────────────────────────────

def _client(a):
    from clickhouse_driver import Client
    return Client(host=a.clickhouse_host, port=a.clickhouse_port,
                  user=a.clickhouse_user, password=a.clickhouse_password,
                  settings={"max_execution_time": 600})


def cmd_report(ch, hours: int) -> int:
    t1 = floor_hour(datetime.now(timezone.utc))
    print(format_table(stored_tallies(ch, t1 - timedelta(hours=hours), t1)))
    return 0


def cmd_missing(ch, reporter: str, hours: int) -> int:
    t1 = floor_hour(datetime.now(timezone.utc)) + timedelta(hours=1)
    rows = ch.execute(
        f"SELECT time, band, tx_sign, rx_sign, rx_id, frequency, snr, proxy "
        f"FROM {DB}.wsprnet_missing_current "
        f"WHERE reporter = %(rep)s AND hour >= %(t0)s ORDER BY time, band",
        {"rep": reporter, "t0": _naive_utc(t1 - timedelta(hours=hours))})
    for (t, band, tx, rx, rxid, freq, snr, proxy) in rows:
        print(f"{t:%Y-%m-%d %H:%M} {band:>4} {tx:<12} {freq / 1e6:12.6f} "
              f"{snr:>4} {rx}/{rxid} {'proxy' if proxy else 'direct'}")
    print(f"{len(rows)} missing", file=sys.stderr)
    return 0


def main(argv: Optional[List[str]] = None) -> int:
    p = argparse.ArgumentParser(description=__doc__.split("\n")[0])
    p.add_argument("--clickhouse-host", default="localhost")
    p.add_argument("--clickhouse-port", type=int, default=9000)
    p.add_argument("--clickhouse-user",
                   default=os.environ.get("CLICKHOUSE_USER", "default"))
    p.add_argument("--clickhouse-password",
                   default=os.environ.get("CLICKHOUSE_PASSWORD", ""))
    p.add_argument("-v", "--verbose", action="store_true")
    sub = p.add_subparsers(dest="cmd", required=True)
    r = sub.add_parser("run", help="judge the recent hours and store results")
    r.add_argument("--lookback-hours", type=int,
                   default=DEFAULT_LOOKBACK_HOURS)
    r.add_argument("--settle-hours", type=int, default=DEFAULT_SETTLE_HOURS)
    r.add_argument("--reporters", type=Path, default=DEFAULT_REPORTERS_PATH)
    r.add_argument("--state-dir", type=Path, default=DEFAULT_STATE_DIR)
    r.add_argument("--dry-run", action="store_true",
                   help="print the verdict, write nothing")
    rp = sub.add_parser("report", help="per-reporter table from stored runs")
    rp.add_argument("--hours", type=int, default=24)
    m = sub.add_parser("missing", help="list one reporter's missing spots")
    m.add_argument("--reporter", required=True)
    m.add_argument("--hours", type=int, default=24)
    a = p.parse_args(argv)

    logging.basicConfig(level=logging.DEBUG if a.verbose else logging.INFO,
                        format="%(levelname)s: %(message)s")
    for noisy in ("clickhouse_driver", "tzlocal"):
        logging.getLogger(noisy).setLevel(logging.WARNING)

    ch = _client(a)
    if a.cmd == "run":
        return validate(ch, Config(
            lookback_hours=a.lookback_hours, settle_hours=a.settle_hours,
            reporters_path=a.reporters, state_dir=a.state_dir,
            dry_run=a.dry_run))
    if a.cmd == "report":
        return cmd_report(ch, a.hours)
    return cmd_missing(ch, a.reporter, a.hours)


if __name__ == "__main__":
    sys.exit(main())
