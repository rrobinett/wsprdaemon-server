"""The public rows: what the fleet board shows, and nothing more.

build() turns stations, their activity, the live alarms and this run's findings
into rows for two tables, fleet_board (one row per mapped station) and
fleet_alarms (one row per live alarm).  Public output carries station names and
6-character grid squares only.  An alarm row leaves out acked_by and note: an
acknowledgement's author and wording stay private.

rank orders the board worst first: off the air, then stations with live alarms
(most alarms first), then the rest, ties by name.  A grey station, one we cannot
judge, sorts last.
"""
from __future__ import annotations

import json
from datetime import datetime, timedelta, timezone

from .alarms import LIVE, alarm_hours, live_map
from .checks import Finding, expected_sources, live_subjects, spots_vs_usual, verdict
from .sources import BASELINE_DAYS, SOURCES, USUAL_SOURCES

SCHEMAS = {
    "fleet_board": [
        ("station", "String"), ("grid6", "String"), ("verdict", "String"),
        ("sources", "String"), ("spots_vs_usual", "String"),
        ("n_open", "UInt16"), ("rank", "UInt16"), ("built_at_utc", "Float64"),
    ],
    "fleet_alarms": [
        ("station", "String"), ("check", "String"), ("subject", "String"),
        ("first_seen_utc", "Float64"), ("last_seen_utc", "Float64"),
        ("state", "String"), ("numbers", "String"), ("built_at_utc", "Float64"),
    ],
}


def _unix(t: datetime) -> float:
    return t.replace(tzinfo=timezone.utc).timestamp()


def _source_state(source, expected, own):
    if source not in expected:
        return "-"
    for f in own:
        if f.subject == source and f.check == "never_started":
            return "never"
    for f in own:
        if f.subject == source and f.check == "client_quiet":
            return "quiet"
    return "ok"


def _sources_json(station, sa, findings, expected):
    own = [f for f in findings if f.station == station.name]
    out = {}
    for s in SOURCES:
        last = sa.last_arrival.get(s)
        out[s] = {"expected": s in expected,
                  "last_arrival_utc": None if last is None else _unix(last),
                  "state": _source_state(s, expected, own)}
    return json.dumps(out, sort_keys=True)


def _usual_json(sa, now, excluded, expected):
    out = {}
    for s in USUAL_SOURCES:
        if s not in expected:
            continue
        r = spots_vs_usual(sa, s, now, excluded)
        if r is not None:
            out[s] = r
    return json.dumps(out, sort_keys=True)


def build(stations, acts, alarms, findings: list[Finding], built_at: float,
          excluded_hours=None, now: datetime | None = None) -> dict[str, list[dict]]:
    """alarms: the live alarms.  excluded_hours: station -> hours the baseline
    skips (the run passes the full alarm history; default reads `alarms`).
    now: the check time (default: built_at)."""
    if now is None:
        now = datetime.fromtimestamp(built_at, timezone.utc).replace(tzinfo=None)
    if excluded_hours is None:
        excluded_hours = alarm_hours(alarms, now - timedelta(days=BASELINE_DAYS + 1))
    live = [a for a in alarms if a.state in LIVE]
    n_open: dict[str, int] = {}
    for a in live:
        n_open[a.station] = n_open.get(a.station, 0) + 1
    rows = []
    lmap = live_map(live)
    for st in stations:
        sa = acts[st.name]
        expected = expected_sources(st, sa, live_subjects(lmap, st.name))
        rows.append({
            "station": st.name, "grid6": st.grid6[:6],
            "verdict": verdict(st, sa, findings),
            "sources": _sources_json(st, sa, findings, expected),
            "spots_vs_usual": _usual_json(sa, now, excluded_hours.get(st.name, set()), expected),
            "n_open": n_open.get(st.name, 0), "rank": 0, "built_at_utc": built_at,
        })

    def order(r):
        if r["verdict"] == "grey":
            return (3, 0, r["station"])
        if r["verdict"] == "off":
            return (0, 0, r["station"])
        if r["n_open"]:
            return (1, -r["n_open"], r["station"])
        return (2, 0, r["station"])

    for i, r in enumerate(sorted(rows, key=order)):
        r["rank"] = i
    arows = [{"station": a.station, "check": a.check, "subject": a.subject,
              "first_seen_utc": _unix(a.first_seen), "last_seen_utc": _unix(a.last_seen),
              "state": a.state, "numbers": a.numbers, "built_at_utc": built_at}
             for a in sorted(live, key=lambda a: (a.station, a.check, a.subject, a.first_seen))]
    return {"fleet_board": rows, "fleet_alarms": arows}
