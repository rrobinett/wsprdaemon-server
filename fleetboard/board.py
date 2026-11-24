"""The public rows: what the fleet board shows, and nothing more.

build() turns stations, their activity, the live alarms and this run's findings
into rows for two tables, fleet_board (one row per mapped station) and
fleet_alarms (one row per live alarm).  Public output carries station names and
6-character grid squares only.  An alarm row leaves out acked_by and note: an
acknowledgement's author and wording stay private.

rank orders the board worst first: off the air, then stations with open alarms
(most first), then the rest, ties by name.  A grey station, one we cannot judge,
sorts last.  An acknowledged alarm means an admin is on it, so it does not count
toward rank: a station whose only live alarms are acknowledged ranks with the
healthy, while its verdict still shows the condition and n_acked counts them.
"""
from __future__ import annotations

import hashlib
import json
from datetime import datetime, timedelta, timezone

from .alarms import LIVE, alarm_hours, live_map
from .release import version_state
from .checks import DRIFT_CHECKS, Finding, drift_stale, item_kind, expected_sources, live_subjects, spots_vs_usual, verdict
from .sources import BASELINE_DAYS, SOURCES, USUAL_SOURCES

SCHEMAS = {
    "fleet_board": [
        ("station", "String"), ("grid6", "String"), ("verdict", "String"),
        ("sources", "String"), ("spots_vs_usual", "String"), ("drift", "String"),
        ("version", "String"),
        ("n_open", "UInt16"), ("n_acked", "UInt16"), ("rank", "UInt16"),
        ("built_at_utc", "Float64"),
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


def _drift_json(sa, now):
    """The drift column: counts from the station's latest drift report, or ''
    with none.  A stale report shows its age and counts, and raises no alarm."""
    row = sa.drift
    if row is None:
        return ""
    return json.dumps({"n_fault": row.n_fault, "n_drift": row.n_drift,
                       "n_reminder": len(row.reminders),
                       "stale": drift_stale(sa, now),
                       "time_utc": _unix(row.ingested or row.time)},
                      sort_keys=True)


def _version_json(sa, now, latest):
    """The version column: the release the station last aligned to, judged
    against the newest blessed one (`latest`, None when unknown), or '' with no
    drift report.  The report arrives daily, so a stale one is marked."""
    row = sa.drift
    if row is None:
        return ""
    return json.dumps({"release": row.release, "baseline": row.baseline, "latest": latest,
                       "state": version_state(row.release, row.baseline, latest),
                       "stale": drift_stale(sa, now)}, sort_keys=True)


def _public_alarm(a):
    """(subject, numbers) as published.  A drift alarm's detail, fix, key and
    reminder text hold paths, commands and operator prose; they stay in the
    private fleet.alarm table.  The public row keeps kind, severity, a canned
    text and an opaque id (8 hex of sha256 of the private key) so open and close
    still pair."""
    if a.check not in DRIFT_CHECKS:
        return a.subject, a.numbers
    try:
        n = json.loads(a.numbers or "{}")
    except ValueError:
        n = {}
    key = str(n.get("key") or a.subject)
    kind = item_kind(key)
    oid = hashlib.sha256(key.encode("utf-8", "replace")).hexdigest()[:8]
    if a.check == "test_restore":
        text, sev = "test ending: " + kind, n.get("severity", "reminder")
    else:
        text, sev = "drift fault: " + kind, "fault"
    return ("%s %s %s" % (a.station, kind, oid),
            json.dumps({"id": oid, "kind": kind, "severity": sev, "text": text}, sort_keys=True))


def build(stations, acts, alarms, findings: list[Finding], built_at: float,
          excluded_hours=None, now: datetime | None = None,
          latest_release: str | None = None) -> dict[str, list[dict]]:
    """alarms: the live alarms.  latest_release: the newest blessed sigmond tag
    (release.latest), None when unknown.  excluded_hours: station -> hours the baseline
    skips (the run passes the full alarm history; default reads `alarms`).
    now: the check time (default: built_at)."""
    if now is None:
        now = datetime.fromtimestamp(built_at, timezone.utc).replace(tzinfo=None)
    if excluded_hours is None:
        excluded_hours = alarm_hours(alarms, now - timedelta(days=BASELINE_DAYS + 1))
    live = [a for a in alarms if a.state in LIVE]
    n_open: dict[str, int] = {}
    n_acked: dict[str, int] = {}
    for a in live:
        count = n_open if a.state == "open" else n_acked
        count[a.station] = count.get(a.station, 0) + 1
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
            "drift": _drift_json(sa, now),
            "version": _version_json(sa, now, latest_release),
            "n_open": n_open.get(st.name, 0), "n_acked": n_acked.get(st.name, 0),
            "rank": 0, "built_at_utc": built_at,
        })

    def order(r):
        if r["verdict"] == "grey":
            return (3, 0, r["station"])
        if r["n_acked"] and not r["n_open"]:
            return (2, 0, r["station"])
        if r["verdict"] == "off":
            return (0, 0, r["station"])
        if r["n_open"]:
            return (1, -r["n_open"], r["station"])
        return (2, 0, r["station"])

    for i, r in enumerate(sorted(rows, key=order)):
        r["rank"] = i
    arows = []
    for a in sorted(live, key=lambda a: (a.station, a.check, a.subject, a.first_seen)):
        subject, numbers = _public_alarm(a)
        arows.append({"station": a.station, "check": a.check, "subject": subject,
                      "first_seen_utc": _unix(a.first_seen), "last_seen_utc": _unix(a.last_seen),
                      "state": a.state, "numbers": numbers, "built_at_utc": built_at})
    return {"fleet_board": rows, "fleet_alarms": arows}
