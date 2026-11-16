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
"""
from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from statistics import mean, median

from .sources import BASELINE_DAYS, HOURLY_SOURCES
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
# Below this many stations with history, silent stations get ordinary off_air
# findings and the ingest_stalled guard stays off.
INGEST_MIN_STATIONS = 4

INGEST_STATION = "wd30"


@dataclass(frozen=True)
class Finding:
    station: str
    check: str      # off_air | client_quiet | never_started | activity_low | ingest_stalled
    subject: str    # source name, or ""
    numbers: dict   # the inputs the verdict used; JSON-serialisable


def _unix(t: datetime | None) -> float | None:
    return None if t is None else t.replace(tzinfo=timezone.utc).timestamp()


def _has_history(sa: StationActivity) -> bool:
    return any(sa.days_active.values()) or bool(sa.last_arrival)


def expected_sources(station: Station, sa: StationActivity) -> set[str]:
    learned = {s for s, days in sa.days_active.items() if len(days) >= EXPECT_MIN_DAYS}
    return (learned | set(station.expect_add)) - set(station.expect_drop)


def _off_air(station: Station, sa: StationActivity, now: datetime) -> Finding | None:
    """No row from any source for 30 min, or 3x the slowest usual gap when every
    expected source is slow (CODAR, HFDL)."""
    expected = expected_sources(station, sa)
    gaps = [sa.usual_gap_s[s] for s in expected if s in sa.usual_gap_s]
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


def _client_quiet(station: Station, sa: StationActivity, now: datetime,
                  sources: set[str]) -> list[Finding]:
    out = []
    for source in sorted(sources):
        gap = sa.usual_gap_s.get(source)
        if gap is None:
            continue
        threshold = max(QUIET_MIN_S, QUIET_FACTOR * gap)
        last = sa.last_arrival.get(source)
        silence = None if last is None else (now - last).total_seconds()
        if silence is None or silence > threshold:
            out.append(Finding(station.name, "client_quiet", source,
                               {"silence_s": silence, "threshold_s": float(threshold),
                                "usual_gap_s": gap, "last_utc": _unix(last)}))
    return out


def _activity_low(station: Station, sa: StationActivity, now: datetime,
                  source: str, alarm: set[datetime]) -> Finding | None:
    """The last 3 complete hours against each hour's median over the past 7
    days, leaving out hours the station sat under an alarm."""
    days = sa.days_active.get(source)
    if not days or (now.date() - min(days)).days < BASELINE_DAYS:
        return None
    counts = sa.hourly.get(source, {})
    top = now.replace(minute=0, second=0, microsecond=0)
    hours = [top - timedelta(hours=k) for k in range(1, LOW_HOURS + 1)]
    baselines = []
    for h in hours:
        past = [h - timedelta(days=d) for d in range(1, BASELINE_DAYS + 1)]
        kept = [counts.get(p, 0) for p in past if p not in alarm]
        if not kept:
            return None
        baselines.append(median(kept))
    last = sum(counts.get(h, 0) for h in hours)
    usual = sum(baselines)
    if mean(baselines) >= LOW_MIN_MEDIAN_PER_H and last < LOW_FRACTION * usual:
        return Finding(station.name, "activity_low", source,
                       {"last3h": int(last), "usual3h": int(round(usual))})
    return None


def _source_findings(station: Station, sa: StationActivity, now: datetime,
                     alarm: set[datetime]) -> list[Finding]:
    never = _never_started(station, sa, now)
    unstarted = {f.subject for f in never}
    expected = expected_sources(station, sa)
    out = never + _client_quiet(station, sa, now, expected - unstarted)
    for source in sorted(expected & set(HOURLY_SOURCES)):
        f = _activity_low(station, sa, now, source, alarm)
        if f:
            out.append(f)
    return out


def evaluate(stations: list[Station], acts: dict[str, StationActivity], now: datetime,
             alarm_hours: dict[str, set[datetime]]) -> list[Finding]:
    """alarm_hours: station -> the UTC hour starts it sat under off_air or
    client_quiet, left out of the activity baseline."""
    with_history = []
    off: dict[str, Finding] = {}
    for st in stations:
        sa = acts.get(st.name) or StationActivity()
        if not _has_history(sa):
            continue
        with_history.append((st, sa))
        f = _off_air(st, sa, now)
        if f:
            off[st.name] = f
    stalled = (len(with_history) >= INGEST_MIN_STATIONS
               and len(off) > len(with_history) / 2)
    out: list[Finding] = []
    for st, sa in with_history:
        if st.name in off:
            if not stalled:
                out.append(off[st.name])
            continue
        out += _source_findings(st, sa, now, alarm_hours.get(st.name, set()))
    if stalled:
        out.append(Finding(INGEST_STATION, "ingest_stalled", "",
                           {"n_silent": len(off), "n_with_history": len(with_history),
                            "silent": sorted(off)}))
    return out


def verdict(station: Station, sa: StationActivity, findings: list[Finding]) -> str:
    """grey: no history, or silenced under an ingest stall (we cannot tell);
    off: off the air; warn: any other finding; ok otherwise."""
    if not _has_history(sa):
        return "grey"
    if any(f.check == "ingest_stalled" and station.name in f.numbers.get("silent", ())
           for f in findings):
        return "grey"
    own = [f for f in findings if f.station == station.name]
    if any(f.check == "off_air" for f in own):
        return "off"
    return "warn" if own else "ok"
