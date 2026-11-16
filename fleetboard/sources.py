"""Per-reporter arrivals from wd30's ClickHouse tables.

Liveness comes from the server's own clock: `last` reads ingested_at, the time
a row reached wd30.  A station's own timestamps (`time`) feed statistics only
(usual gap, active days, hourly counts), because a station with a wrong clock
must not look alive or dead by its own word.

Only `fetch` touches ClickHouse.  `queries` builds SQL from a caller-supplied
`now`, and `parse` turns result tuples into an Activity, so both stay testable
without a database.
"""
from __future__ import annotations

from dataclasses import dataclass, field
from datetime import date, datetime

SOURCES = ("wspr_noise", "wspr_spots", "ft8", "codar", "hfdl")
HOURLY_SOURCES = ("wspr_spots", "ft8")

EPOCH = datetime(1970, 1, 1)

BASELINE_DAYS = 7
EXPECT_WINDOW_DAYS = 14

# source -> (table, reporter column, extra WHERE clause)
_SPEC = {
    "wspr_noise": ("wsprdaemon.noise", "site", ""),
    "wspr_spots": ("wsprdaemon.spots", "rx_sign", ""),
    "ft8": ("psk.spots", "rx_sign", "mode = 'ft8'"),
    "codar": ("sigmondb.capture", "reporter_id", "rate_hz_per_s != 0"),
    "hfdl": ("sigmondb.capture", "reporter_id", "rate_hz_per_s = 0"),
}

Key = tuple[str, str]


@dataclass(frozen=True)
class Activity:
    last_arrival: dict[Key, datetime] = field(default_factory=dict)
    days_active: dict[Key, frozenset[date]] = field(default_factory=dict)
    usual_gap_s: dict[Key, float] = field(default_factory=dict)
    hourly: dict[Key, dict[datetime, int]] = field(default_factory=dict)


def _lit(t: datetime) -> str:
    return f"toDateTime('{t:%Y-%m-%d %H:%M:%S}', 'UTC')"


def _where(col: str, now: datetime, days_back: str, extra: str) -> str:
    cond = f"{col} > {_lit(now)} - INTERVAL {days_back}"
    return f"{cond} AND {extra}" if extra else cond


def queries(now: datetime) -> dict[str, str]:
    """name -> SQL, four per source; reporter is the first column of each."""
    out: dict[str, str] = {}
    for src, (table, rep, extra) in _SPEC.items():
        out[f"{src}_last"] = (
            f"SELECT {rep} AS reporter, max(ingested_at) FROM {table} "
            f"WHERE {_where('ingested_at', now, f'{EXPECT_WINDOW_DAYS} DAY', extra)} "
            f"GROUP BY reporter"
        )
        out[f"{src}_days"] = (
            f"SELECT {rep} AS reporter, toDate(time, 'UTC') AS day FROM {table} "
            f"WHERE {_where('time', now, f'{EXPECT_WINDOW_DAYS} DAY', extra)} "
            f"GROUP BY reporter, day"
        )
        out[f"{src}_gap"] = (
            f"SELECT reporter, arrayReduce('quantileExact(0.9)', gaps) FROM ("
            f"SELECT {rep} AS reporter, "
            f"arrayPopFront(arrayDifference(arraySort("
            f"groupUniqArray(toUnixTimestamp(toStartOfMinute(time)))))) AS gaps "
            f"FROM {table} "
            f"WHERE {_where('time', now, f'{BASELINE_DAYS} DAY', extra)} "
            f"GROUP BY reporter) WHERE length(gaps) > 0"
        )
        if src in HOURLY_SOURCES:
            out[f"{src}_hourly"] = (
                f"SELECT {rep} AS reporter, toStartOfHour(time, 'UTC') AS hour, count() "
                f"FROM {table} "
                f"WHERE {_where('time', now, f'{BASELINE_DAYS} DAY + INTERVAL 3 HOUR', extra)} "
                f"GROUP BY reporter, hour"
            )
    return out


def parse(results: dict[str, list[tuple]]) -> Activity:
    last: dict[Key, datetime] = {}
    days: dict[Key, set[date]] = {}
    gap: dict[Key, float] = {}
    hourly: dict[Key, dict[datetime, int]] = {}
    for src in SOURCES:
        for rep, t in results.get(f"{src}_last", []):
            if t is not None and t != EPOCH:
                last[(src, rep)] = t
        for rep, d in results.get(f"{src}_days", []):
            days.setdefault((src, rep), set()).add(d)
        for rep, g in results.get(f"{src}_gap", []):
            gap[(src, rep)] = float(g)
        if src in HOURLY_SOURCES:
            for rep, h, n in results.get(f"{src}_hourly", []):
                hourly.setdefault((src, rep), {})[h] = int(n)
    return Activity(
        last_arrival=last,
        days_active={k: frozenset(v) for k, v in days.items()},
        usual_gap_s=gap,
        hourly=hourly,
    )


def fetch(client, now: datetime) -> Activity:
    """Run every query on `client` (clickhouse_connect) and parse the rows."""
    return parse({name: client.query(sql).result_rows
                  for name, sql in queries(now).items()})
