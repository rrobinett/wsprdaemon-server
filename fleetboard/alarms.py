"""Alarm state as pure functions: findings in, rows to write out.

fleet.alarm is append-only (ReplacingMergeTree), so `step` returns the rows
that changed and the caller inserts them.  An alarm is one instance of a
condition, named by (station, check, subject, first_seen).  It clears only
after CLEAR_AFTER consecutive checks without the finding, so one lucky check
does not close an outage.  An ack names one instance by its first_seen; a
condition that clears and returns opens a new instance that needs a new ack.
Cleared alarms still feed alarm_hours: the usual-activity baseline must skip
hours that fell inside an outage.

`hold` names stations whose live alarms stay frozen this run.  When wd30's
ingest stalls, its silent stations get no findings, and without the hold three
such runs would clear a real outage (and drop its ack) on wd30's fault.
"""
from __future__ import annotations

import json
from dataclasses import dataclass, replace
from datetime import datetime, timedelta

CLEAR_AFTER = 3
LIVE = ("open", "acknowledged")
_HOURLY_CHECKS = ("off_air", "client_quiet")


@dataclass
class Alarm:
    station: str
    check: str
    subject: str
    first_seen: datetime
    last_seen: datetime
    state: str
    false_streak: int
    numbers: str
    updated_at: datetime


@dataclass(frozen=True)
class Ack:
    station: str
    check: str
    subject: str
    first_seen: datetime
    acked_by: str
    note: str
    acked_at: datetime


def step(prev, findings, acks, now, hold=frozenset()):
    """hold: stations whose live alarms are left untouched (no streak change,
    not returned)."""
    now = now.replace(microsecond=0)
    live = {(a.station, a.check, a.subject): a for a in prev if a.state in LIVE}
    held = {k for k, a in live.items() if a.station in hold}
    changed: dict[tuple, Alarm] = {}
    found = set()
    for fd in findings:
        key = (fd.station, fd.check, fd.subject)
        if key in held:
            continue
        found.add(key)
        numbers = json.dumps(fd.numbers, sort_keys=True)
        old = live.get(key)
        if old is None:
            changed[key] = Alarm(*key, now, now, "open", 0, numbers, now)
        else:
            changed[key] = replace(old, last_seen=now, false_streak=0,
                                   numbers=numbers, updated_at=now)
    for key, old in live.items():
        if key in found or key in held:
            continue
        streak = old.false_streak + 1
        changed[key] = replace(
            old, false_streak=streak, updated_at=now,
            state="cleared" if streak >= CLEAR_AFTER else old.state)
    for ack in acks:
        cur = changed.get((ack.station, ack.check, ack.subject))
        if cur and cur.state == "open" and cur.first_seen == ack.first_seen:
            changed[(ack.station, ack.check, ack.subject)] = replace(
                cur, state="acknowledged", updated_at=now)
    return list(changed.values())


def alarm_hours(alarms, since):
    """station -> naive-UTC hour starts covered by its alarms, from `since`'s hour."""
    floor = since.replace(minute=0, second=0, microsecond=0)
    out: dict[str, set[datetime]] = {}
    for a in alarms:
        if a.check in _HOURLY_CHECKS:
            stations = [a.station]
        elif a.check == "ingest_stalled":
            stations = json.loads(a.numbers or "{}").get("silent", [])
        else:
            continue
        h = a.first_seen.replace(minute=0, second=0, microsecond=0)
        end = a.last_seen.replace(minute=0, second=0, microsecond=0)
        while h <= end:
            if h >= floor:
                for s in stations:
                    out.setdefault(s, set()).add(h)
            h += timedelta(hours=1)
    return out
