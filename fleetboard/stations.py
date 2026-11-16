"""The station map and its fold.

A station reports under several names: per source, a reporter id, plus aliases (the
same receiver filed under an older callsign).  load_map groups the fleet.station_map
rows into Stations; fold merges per-reporter Activity into per-station activity, so the
checks judge a station, not a spelling.  A reporter in no station is ignored.
"""
from __future__ import annotations

from dataclasses import dataclass, field
from datetime import date, datetime

from .sources import SOURCES, Activity


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
        result[st.name] = sa
    return result
