"""The station checks: off the air, client quiet, never started, activity low.

Each check reads one station's folded activity at a caller-supplied `now` and
returns Findings; nothing here touches a database, so every threshold stays
testable.  Liveness reads last_arrival (the server's ingested_at); the station's
own timestamps feed only the statistics (usual gap, active days, hourly counts).

Two rules keep the board from shouting about one fault many times:
- one cause, one alarm: a station off the air shows only that, not a quiet
  client and a low count for each of its sources as well;
- when more than half the stations with history fall silent at once, the
  fault lies in wd30's ingest, not in the stations, so one ingest_stalled
  finding replaces their off_air findings and those stations show grey.  The
  guard needs at least INGEST_MIN_STATIONS stations with history: in a smaller
  fleet a majority is one or two stations, too few to blame the server.

A client is quiet by one of two rules, chosen by how its source behaves:
- wspr_noise arrives every 2 minutes, day and night: quiet after its usual gap
  x QUIET_FACTOR (floored at QUIET_MIN_S);
- ft8, CODAR and HFDL follow the band and the day, so a gap rule cries wolf
  every night.  They are quiet only when QUIET_ACTIVE_HOURS hours that are
  usually active for that source (rows on ACTIVE_DAYS_MIN of the past
  BASELINE_DAYS days) have passed with no row.
wspr_spots never raises client_quiet: wspr_noise carries WSPR liveness, and a
quiet band shows as activity_low.

Long silence must not read as health.  A live alarm keeps its subject
expected, and keeps a quiet source quiet after its baseline has aged out,
until a row arrives; a station with a live off_air alarm is judged, never grey.
"""
from __future__ import annotations

import re
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from statistics import mean, median

from .sources import BASELINE_DAYS, USUAL_SOURCES
from .stations import Station, StationActivity

OFF_AIR_MIN_S = 1800
QUIET_FACTOR = 3
QUIET_MIN_S = 1800
NEVER_STARTED_DAYS = 7
EXPECT_MIN_DAYS = 3
LOW_HOURS = 3
LOW_FRACTION = 0.30
LOW_MIN_MEDIAN_PER_H = 20
FAST_SOURCE_MAX_GAP_S = 600
ACTIVE_DAYS_MIN = 5
QUIET_ACTIVE_HOURS = 2
GAP_QUIET_SOURCES = ("wspr_noise",)
HOURS_QUIET_SOURCES = ("ft8", "codar", "hfdl")
# A live alarm of these checks keeps its subject (a source) expected.
_SUBJECT_CHECKS = ("client_quiet", "never_started")
# Below this many stations with history, silent stations get ordinary off_air
# findings and the ingest_stalled guard stays off.
INGEST_MIN_STATIONS = 4

INGEST_STATION = "wd30"

# The drift report: a row older than this is stale, and says nothing.
DRIFT_STALE_S = 48 * 3600
# A test whose exception ended arrives as a fault with this detail prefix.
TEST_ENDED = "temporary test ended"


@dataclass(frozen=True)
class Finding:
    station: str
    check: str      # off_air | client_quiet | never_started | activity_low | ingest_stalled | drift_fault | test_restore
    subject: str    # source name, or ""
    numbers: dict   # the inputs the verdict used; JSON-serialisable


def _unix(t: datetime | None) -> float | None:
    return None if t is None else t.replace(tzinfo=timezone.utc).timestamp()


def _has_history(sa: StationActivity) -> bool:
    return any(sa.days_active.values()) or bool(sa.last_arrival)


def live_subjects(live: dict[str, set[tuple[str, str]]], name: str) -> set[str]:
    """The sources a station's live client_quiet and never_started alarms name."""
    return {subj for check, subj in live.get(name, ()) if check in _SUBJECT_CHECKS}


def expected_sources(station: Station, sa: StationActivity, live: set[str] = frozenset()) -> set[str]:
    """Learned (active on EXPECT_MIN_DAYS of the window), added by hand, or the
    subject of a live alarm; less any the operator dropped."""
    learned = {s for s, days in sa.days_active.items() if len(days) >= EXPECT_MIN_DAYS}
    return (learned | set(station.expect_add) | set(live)) - set(station.expect_drop)


def _off_air(station: Station, sa: StationActivity, now: datetime,
             expected: set[str]) -> Finding | None:
    """No row from any source for 30 min, or 3x the slowest usual gap when every
    expected source is slow (CODAR, HFDL).

    A station with no expected source that has a gap (new, or all dropped) is
    judged by the gaps of every source it has; a slow-only newcomer must not
    cry wolf at 30 min.  The bare floor applies only with no gaps at all."""
    gaps = [sa.usual_gap_s[s] for s in expected if s in sa.usual_gap_s]
    if not gaps:
        gaps = list(sa.usual_gap_s.values())
    if any(g <= FAST_SOURCE_MAX_GAP_S for g in gaps):
        threshold = OFF_AIR_MIN_S
    else:
        threshold = max([OFF_AIR_MIN_S] + [QUIET_FACTOR * g for g in gaps])
    latest = max(sa.last_arrival.values(), default=None)
    silence = None if latest is None else (now - latest).total_seconds()
    if silence is not None and silence <= threshold:
        return None
    return Finding(station.name, "off_air", "",
                   {"silence_s": silence, "threshold_s": float(threshold),
                    "latest_utc": _unix(latest)})


def _never_started(station: Station, sa: StationActivity, now: datetime) -> list[Finding]:
    since = now.date() - timedelta(days=NEVER_STARTED_DAYS)
    out = []
    for source in sorted(set(station.expect_add) - set(station.expect_drop)):
        if not any(d > since for d in sa.days_active.get(source, ())):
            out.append(Finding(station.name, "never_started", source,
                               {"days": NEVER_STARTED_DAYS}))
    return out


def _usual_hours(sa: StationActivity, source: str, now: datetime,
                 excluded_hours: set[datetime]) -> set[int]:
    """Hours of the day (UTC) with a row on ACTIVE_DAYS_MIN or more of the past
    BASELINE_DAYS days.  Alarm hours count as neither active nor idle."""
    counts = sa.hourly.get(source, {})
    top = now.replace(minute=0, second=0, microsecond=0)
    days: dict[int, int] = {}
    for k in range(1, BASELINE_DAYS * 24 + 1):
        t = top - timedelta(hours=k)
        if t not in excluded_hours and counts.get(t, 0) > 0:
            days[t.hour] = days.get(t.hour, 0) + 1
    return {h for h, n in days.items() if n >= ACTIVE_DAYS_MIN}


def _missed_hours(last: datetime | None, now: datetime, usual: set[int]) -> int:
    """Complete usually-active hours since the hour of the last arrival, looking
    back at most BASELINE_DAYS days."""
    top = now.replace(minute=0, second=0, microsecond=0)
    t = top - timedelta(days=BASELINE_DAYS)
    if last is not None:
        t = max(t, last.replace(minute=0, second=0, microsecond=0) + timedelta(hours=1))
    n = 0
    while t < top:
        n += t.hour in usual
        t += timedelta(hours=1)
    return n


def _quiet_one(station: Station, sa: StationActivity, now: datetime, source: str,
               excluded_hours: set[datetime], held: bool) -> Finding | None:
    """held: the source has a live client_quiet alarm.  When its own rule has no
    baseline left (the silence outlived it), the alarm holds until a row arrives."""
    last = sa.last_arrival.get(source)
    silence = None if last is None else (now - last).total_seconds()
    numbers = {"silence_s": silence, "last_utc": _unix(last)}
    if source in GAP_QUIET_SOURCES:
        gap = sa.usual_gap_s.get(source)
        if gap is not None:
            threshold = max(QUIET_MIN_S, QUIET_FACTOR * gap)
            if silence is None or silence > threshold:
                return Finding(station.name, "client_quiet", source,
                               dict(numbers, threshold_s=float(threshold), usual_gap_s=gap))
            return None
    elif source in HOURS_QUIET_SOURCES:
        usual = _usual_hours(sa, source, now, excluded_hours)
        if usual:
            missed = _missed_hours(last, now, usual)
            if missed >= QUIET_ACTIVE_HOURS and (silence is None or silence >= QUIET_MIN_S):
                return Finding(station.name, "client_quiet", source,
                               dict(numbers, active_hours_missed=missed))
            return None
    else:
        return None
    if held and (silence is None or silence >= QUIET_MIN_S):
        return Finding(station.name, "client_quiet", source, dict(numbers, held_by_alarm=True))
    return None


def _client_quiet(station: Station, sa: StationActivity, now: datetime, sources: set[str],
                  excluded_hours: set[datetime], held: set[str]) -> list[Finding]:
    out = []
    for source in sorted(sources):
        f = _quiet_one(station, sa, now, source, excluded_hours, source in held)
        if f:
            out.append(f)
    return out


def _usual(sa: StationActivity, source: str, now: datetime,
           excluded_hours: set[datetime]) -> tuple[int, float, float] | None:
    """The last 3 complete hours against each hour's median over the past 7
    days, leaving out hours the station sat under an alarm.  Returns
    (last, usual, mean baseline per hour), or None with no baseline or under
    7 days of history."""
    days = sa.days_active.get(source)
    if not days or (now.date() - min(days)).days < BASELINE_DAYS:
        return None
    counts = sa.hourly.get(source, {})
    top = now.replace(minute=0, second=0, microsecond=0)
    hours = [top - timedelta(hours=k) for k in range(1, LOW_HOURS + 1)]
    baselines = []
    for h in hours:
        past = [h - timedelta(days=d) for d in range(1, BASELINE_DAYS + 1)]
        kept = [counts.get(p, 0) for p in past if p not in excluded_hours]
        if not kept:
            return None
        baselines.append(median(kept))
    return sum(counts.get(h, 0) for h in hours), sum(baselines), mean(baselines)


def spots_vs_usual(sa: StationActivity, source: str, now: datetime,
                   excluded_hours: set[datetime]) -> dict | None:
    """{"last3h", "usual3h"} for one source, or None with no baseline or under
    7 days of history.  The activity_low check and the public board share it."""
    r = _usual(sa, source, now, excluded_hours)
    if r is None:
        return None
    return {"last3h": int(r[0]), "usual3h": int(round(r[1]))}


def _activity_low(station: Station, sa: StationActivity, now: datetime,
                  source: str, alarm: set[datetime]) -> Finding | None:
    r = _usual(sa, source, now, alarm)
    if r is None:
        return None
    last, usual, mean_base = r
    if mean_base >= LOW_MIN_MEDIAN_PER_H and last < LOW_FRACTION * usual:
        return Finding(station.name, "activity_low", source,
                       {"last3h": int(last), "usual3h": int(round(usual))})
    return None


def _source_findings(station: Station, sa: StationActivity, now: datetime,
                     alarm: set[datetime], expected: set[str],
                     live_quiet: set[str]) -> list[Finding]:
    never = _never_started(station, sa, now)
    unstarted = {f.subject for f in never}
    quiet = _client_quiet(station, sa, now, expected - unstarted, alarm, live_quiet)
    out = never + quiet
    # One source, one alarm: a quiet source's low count is the same fault.
    quiet_sources = {f.subject for f in quiet}
    for source in sorted((expected & set(USUAL_SOURCES)) - quiet_sources):
        f = _activity_low(station, sa, now, source, alarm)
        if f:
            out.append(f)
    return out


DRIFT_CHECKS = ("drift_fault", "test_restore")


def drift_hold(stations, acts, now) -> set:
    """(station, check) pairs of stations whose drift row went stale: their live
    drift alarms stay frozen until a fresh row arrives."""
    return {(st.name, c) for st in stations
            if drift_stale(acts.get(st.name) or StationActivity(), now)
            for c in DRIFT_CHECKS}


def drift_stale(sa: StationActivity, now: datetime) -> bool:
    """Stale by the server's clock (ingested_at); the station's own time only
    stands in for a row without one."""
    if sa.drift is None:
        return False
    return (now - (sa.drift.ingested or sa.drift.time)).total_seconds() > DRIFT_STALE_S


_KIND = re.compile(r"[a-z][a-z0-9_-]{0,23}")


def item_kind(key: str) -> str:
    """The kind of a drift item key ("unit:x.service" -> "unit"), or "item" when
    the key does not start with a plain kind word: a stray keyed by a path must
    not leak the path as its kind."""
    kind = key.partition(":")[0]
    return kind if _KIND.fullmatch(kind) and ":" in key else "item"


def _drift_findings(station: Station, sa: StationActivity, now: datetime) -> list[Finding]:
    """One finding per fault item and per reminder, from the latest drift row.

    A stale row (older than DRIFT_STALE_S) yields none: the board shows it as
    stale, and off_air already covers a dark station.  An ended test is a
    test_restore, like a reminder, because the baseline wants restoring.  Drift
    items with no fault raise nothing; the board counts them."""
    row = sa.drift
    if row is None or drift_stale(sa, now):
        return []
    out = {}
    for it in row.faults:
        key = str(it.get("key") or "%s:%s" % (it.get("kind", ""), it.get("subject", "")))
        detail = str(it.get("detail", ""))
        check = "test_restore" if detail.startswith(TEST_ENDED) else "drift_fault"
        out[(check, key)] = Finding(station.name, check, "%s %s" % (station.name, key),
                                    {"key": key, "kind": item_kind(key), "severity": "fault",
                                     "detail": detail, "fix": str(it.get("fix", ""))})
    for r in row.reminders:
        key = str(r.get("key", ""))
        text = "%s's test of %s (%s) ends %s; restore with: %s" % (
            r.get("by", "?"), key, r.get("reason", ""), r.get("until", "?"), r.get("fix", ""))
        out[("test_restore", key)] = Finding(station.name, "test_restore",
                                             "%s %s" % (station.name, key),
                                             {"key": key, "kind": item_kind(key),
                                              "severity": "reminder", "text": text})
    return list(out.values())


def evaluate(stations: list[Station], acts: dict[str, StationActivity], now: datetime,
             alarm_hours: dict[str, set[datetime]],
             live: dict[str, set[tuple[str, str]]] | None = None) -> list[Finding]:
    """alarm_hours: station -> the UTC hour starts it sat under off_air or
    client_quiet, left out of the activity baseline.
    live: station -> {(check, subject)} of its live alarms."""
    live = live or {}
    with_history = []
    off: dict[str, Finding] = {}
    for st in stations:
        sa = acts.get(st.name) or StationActivity()
        own = live.get(st.name, set())
        if not _has_history(sa) and ("off_air", "") not in own:
            continue
        expected = expected_sources(st, sa, live_subjects(live, st.name))
        with_history.append((st, sa, expected))
        f = _off_air(st, sa, now, expected)
        if f:
            off[st.name] = f
    stalled = (len(with_history) >= INGEST_MIN_STATIONS
               and len(off) > len(with_history) / 2)
    out: list[Finding] = []
    for st, sa, expected in with_history:
        if st.name in off:
            if not stalled:
                out.append(off[st.name])
            continue
        live_quiet = {subj for check, subj in live.get(st.name, ()) if check == "client_quiet"}
        out += _source_findings(st, sa, now, alarm_hours.get(st.name, set()), expected, live_quiet)
    for st in stations:
        out += _drift_findings(st, acts.get(st.name) or StationActivity(), now)
    if stalled:
        out.append(Finding(INGEST_STATION, "ingest_stalled", "",
                           {"n_silent": len(off), "n_with_history": len(with_history),
                            "silent": sorted(off)}))
    return out


def verdict(station: Station, sa: StationActivity, findings: list[Finding]) -> str:
    """grey: silenced under an ingest stall (we cannot tell), or no history and
    no finding; off: off the air, even after its history aged out; warn: any
    other finding; ok otherwise."""
    if any(f.check == "ingest_stalled" and station.name in f.numbers.get("silent", ())
           for f in findings):
        return "grey"
    own = [f for f in findings if f.station == station.name]
    if any(f.check == "off_air" for f in own):
        return "off"
    if not _has_history(sa):
        return "grey"
    return "warn" if own else "ok"
