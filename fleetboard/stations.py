"""The station map and its fold.

A station reports under several names: per source, a reporter id, plus aliases (the
same receiver filed under an older callsign).  load_map groups the fleet.station_map
rows into Stations; fold merges per-reporter Activity into per-station activity, so the
checks judge a station, not a spelling.  A reporter in no station is ignored.
"""
from __future__ import annotations

import json
from dataclasses import dataclass, field
from datetime import date, datetime

from .sources import SOURCES, Activity, DriftRow

MAP_KEYS = ("name", "grid6", "source", "reporter", "is_alias", "expect_add", "expect_drop")
_REQUIRED = ("name", "source", "reporter")
GRID_CHARS = 6


@dataclass(frozen=True)
class Station:
    name: str
    grid6: str
    reporters: dict[str, tuple[str, ...]]
    expect_add: tuple[str, ...] = ()
    expect_drop: tuple[str, ...] = ()


@dataclass
class StationActivity:
    last_arrival: dict[str, datetime] = field(default_factory=dict)
    days_active: dict[str, frozenset[date]] = field(default_factory=dict)
    usual_gap_s: dict[str, float] = field(default_factory=dict)
    hourly: dict[str, dict[datetime, int]] = field(default_factory=dict)
    drift: DriftRow | None = None   # the station's latest drift row


def _check_row(r) -> str | None:
    """Why a map row is unfit, or None.  Unknown keys are refused, not dropped:
    the map must carry only what the board may show (no hosts or reach)."""
    if not isinstance(r, dict):
        return "not a JSON object"
    extra = sorted(set(r) - set(MAP_KEYS))
    if extra:
        return "unknown keys %s" % extra
    for k in _REQUIRED:
        if not isinstance(r.get(k), str) or not r[k]:
            return "%s must be a non-empty string" % k
    if r["source"] not in SOURCES:
        return "unknown source %r" % r["source"]
    grid = r.get("grid6", "")
    if not isinstance(grid, str) or len(grid) > GRID_CHARS:
        return "grid6 must be a string of at most %d characters" % GRID_CHARS
    if r.get("is_alias", 0) not in (0, 1):
        return "is_alias must be 0 or 1"
    for k in ("expect_add", "expect_drop"):
        v = r.get(k) or []
        if not isinstance(v, list) or any(x not in SOURCES for x in v):
            return "%s must list known sources" % k
    return None


def parse_map_lines(lines) -> list[dict]:
    """JSON Lines -> map rows, every one checked.  Raises ValueError naming the
    first bad line, or when there is no row at all, so load-map can refuse
    before it empties fleet.station_map."""
    rows = []
    for n, line in enumerate(lines, 1):
        if not line.strip():
            continue
        try:
            r = json.loads(line)
        except ValueError as e:
            raise ValueError("line %d: not JSON (%s)" % (n, e)) from None
        why = _check_row(r)
        if why:
            raise ValueError("line %d: %s" % (n, why))
        rows.append(r)
    if not rows:
        raise ValueError("no rows")
    return rows


def load_map(rows: list[dict]) -> list[Station]:
    by_name: dict[str, list[dict]] = {}
    for r in rows:
        by_name.setdefault(r["name"], []).append(r)
    out = []
    for name in sorted(by_name):
        rs = by_name[name]
        names: dict[str, set[str]] = {}
        for r in rs:
            names.setdefault(r["source"], set()).add(r["reporter"])
        out.append(Station(
            name=name,
            grid6=rs[0]["grid6"],
            reporters={s: tuple(sorted(v)) for s, v in names.items()},
            expect_add=tuple(rs[0].get("expect_add") or ()),
            expect_drop=tuple(rs[0].get("expect_drop") or ()),
        ))
    return out


def fold(activity: Activity, stations: list[Station]) -> dict[str, StationActivity]:
    result = {}
    for st in stations:
        sa = StationActivity()
        for source in SOURCES:
            keys = [(source, n) for n in st.reporters.get(source, ())]
            arrivals = [activity.last_arrival[k] for k in keys if k in activity.last_arrival]
            if arrivals:
                sa.last_arrival[source] = max(arrivals)
            days = [activity.days_active[k] for k in keys if k in activity.days_active]
            if days:
                sa.days_active[source] = frozenset().union(*days)
            gaps = [activity.usual_gap_s[k] for k in keys if k in activity.usual_gap_s]
            if gaps:
                sa.usual_gap_s[source] = min(gaps)
            hours = [activity.hourly[k] for k in keys if k in activity.hourly]
            if hours:
                total: dict[datetime, int] = {}
                for h in hours:
                    for t, n in h.items():
                        total[t] = total.get(t, 0) + n
                sa.hourly[source] = total
        # A drift row names the station's own reporter id; take the newest row
        # under any name the station answers to, aliases included.
        names = {st.name}.union(*(set(v) for v in st.reporters.values()))
        rows = [activity.drift[n] for n in names if n in activity.drift]
        if rows:
            sa.drift = max(rows, key=lambda r: r.time)
        result[st.name] = sa
    return result
