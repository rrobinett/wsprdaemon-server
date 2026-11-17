"""Tests for fleetboard.checks: the four station checks and the ingest guard."""
from __future__ import annotations

import json
import sys
import types as _types
from datetime import datetime, timedelta
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
for _mod in ('clickhouse_connect', 'clickhouse_driver'):
    if _mod not in sys.modules:
        _stub = _types.ModuleType(_mod)
        _stub.Client = object
        _stub.get_client = lambda **kw: None
        sys.modules[_mod] = _stub

from fleetboard.checks import Finding, evaluate, expected_sources, verdict  # noqa: E402
from fleetboard.stations import Station, StationActivity  # noqa: E402

NOW = datetime(2026, 11, 16, 12, 10, 0)
HOUR = NOW.replace(minute=0)
LAST3 = [HOUR - timedelta(hours=k) for k in (1, 2, 3)]


def _st(name="A", add=(), drop=()):
    return Station(name=name, grid6="EN16ov", reporters={}, expect_add=add, expect_drop=drop)


def _days(n, end=NOW):
    """n consecutive active days ending on `end`'s date."""
    return frozenset((end - timedelta(days=k)).date() for k in range(n))


def _add(sa, source, *, ago, gap, ndays=10):
    sa.last_arrival[source] = NOW - ago
    sa.usual_gap_s[source] = gap
    sa.days_active[source] = _days(ndays)
    return sa


def _live(name="A", add=(), drop=()):
    """A healthy station: wspr_noise fresh every 2 minutes for 10 days."""
    return _st(name, add, drop), _add(StationActivity(), "wspr_noise",
                                      ago=timedelta(minutes=1), gap=120.0)


def _run(pairs, alarm_hours=None, bystanders=2):
    """Evaluate `pairs` beside healthy bystanders, which raise no findings.
    They keep the single-station tests independent of INGEST_MIN_STATIONS;
    the ingest tests pass bystanders=0 to control N exactly."""
    pairs = list(pairs) + [_live(f"ZZ{i}") for i in range(bystanders)]
    return evaluate([st for st, _ in pairs], {st.name: sa for st, sa in pairs},
                    NOW, alarm_hours or {})


def _checks(findings):
    return sorted((f.station, f.check, f.subject) for f in findings)


# --- off the air ---------------------------------------------------------

def test_off_air_after_30_min_of_silence_from_fast_sources():
    st, sa = _live()
    sa.last_arrival["wspr_noise"] = NOW - timedelta(minutes=29)
    assert _run([(st, sa)]) == []
    sa.last_arrival["wspr_noise"] = NOW - timedelta(minutes=31)
    fs = _run([(st, sa)])
    assert _checks(fs) == [("A", "off_air", "")]
    assert fs[0].numbers["silence_s"] == 31 * 60
    assert fs[0].numbers["threshold_s"] == 1800


def test_off_air_waits_3x_slowest_gap_for_slow_only_station():
    st = _st()
    sa = _add(StationActivity(), "hfdl", ago=timedelta(hours=5), gap=7200.0)
    assert _run([(st, sa)]) == []
    sa.last_arrival["hfdl"] = NOW - timedelta(hours=6, minutes=1)
    fs = _run([(st, sa)])
    assert _checks(fs) == [("A", "off_air", "")]
    assert fs[0].numbers["threshold_s"] == 3 * 7200


def test_off_air_uses_newest_arrival_from_any_source_even_unexpected():
    # hfdl is slow and fresh, but on only 1 day: not expected.  It still keeps
    # the station on the air; the fast wspr_noise sets the 30-min threshold.
    st, sa = _live()
    sa.last_arrival["wspr_noise"] = NOW - timedelta(hours=2)
    sa.last_arrival["hfdl"] = NOW - timedelta(minutes=5)
    sa.days_active["hfdl"] = _days(1)
    fs = _run([(st, sa)])
    assert not any(f.check == "off_air" for f in fs)
    assert _checks(fs) == [("A", "client_quiet", "wspr_noise")]


def test_off_air_fast_source_overrides_slow_gap():
    # A fast source present: threshold stays 30 min although codar's gap is 2 h.
    st, sa = _live()
    _add(sa, "codar", ago=timedelta(minutes=40), gap=7200.0)
    sa.last_arrival["wspr_noise"] = NOW - timedelta(minutes=40)
    fs = _run([(st, sa)])
    assert _checks(fs) == [("A", "off_air", "")]
    assert fs[0].numbers["threshold_s"] == 1800
    assert fs[0].numbers["latest_utc"] == (NOW - timedelta(minutes=40) - datetime(1970, 1, 1)).total_seconds()


def test_off_air_when_history_but_no_arrival():
    st = _st()
    sa = StationActivity(days_active={"wspr_noise": _days(5)}, usual_gap_s={"wspr_noise": 120.0})
    fs = _run([(st, sa)])
    assert _checks(fs) == [("A", "off_air", "")]
    assert fs[0].numbers["latest_utc"] is None
    assert fs[0].numbers["silence_s"] is None



def test_off_air_uses_every_gap_when_no_source_is_expected():
    # A new station: hfdl on 1 day only, so nothing is expected yet.  Its gap
    # still sets the threshold (3 x 2 h), rather than the 30-min floor.
    st = _st()
    sa = _add(StationActivity(), "hfdl", ago=timedelta(hours=1), gap=7200.0, ndays=1)
    assert expected_sources(st, sa) == set()
    assert _run([(st, sa)]) == []
    sa.last_arrival["hfdl"] = NOW - timedelta(hours=6, minutes=1)
    fs = _run([(st, sa)])
    assert _checks(fs) == [("A", "off_air", "")]
    assert fs[0].numbers["threshold_s"] == 3 * 7200

# --- client quiet ----------------------------------------------------------

def test_client_quiet_threshold_is_3x_gap_floored_at_30_min():
    st, sa = _live()
    _add(sa, "ft8", ago=timedelta(minutes=29), gap=60.0)      # 3x60 = 3 min -> floor 30 min
    _add(sa, "codar", ago=timedelta(minutes=50), gap=1200.0)  # 3x1200 = 60 min
    assert _run([(st, sa)]) == []
    sa.last_arrival["ft8"] = NOW - timedelta(minutes=31)
    sa.last_arrival["codar"] = NOW - timedelta(minutes=61)
    fs = _run([(st, sa)])
    assert _checks(fs) == [("A", "client_quiet", "codar"), ("A", "client_quiet", "ft8")]
    by = {f.subject: f.numbers for f in fs}
    assert by["ft8"]["threshold_s"] == 1800
    assert by["codar"]["threshold_s"] == 3600


def test_client_quiet_ignores_unexpected_and_dropped_sources():
    st, sa = _live(drop=("codar",))
    _add(sa, "codar", ago=timedelta(days=1), gap=600.0)
    _add(sa, "ft8", ago=timedelta(days=1), gap=60.0, ndays=2)  # 2 days < 3: not learned
    assert expected_sources(st, sa) == {"wspr_noise"}
    assert _run([(st, sa)]) == []


# --- never started ---------------------------------------------------------

def test_never_started_needs_expect_add_and_7_days_silence():
    st, sa = _live("K3LR", add=("codar",))
    fs = _run([(st, sa)])
    assert _checks(fs) == [("K3LR", "never_started", "codar")]

    # A codar day 8 days back still counts as never started; a usual gap
    # must not add a client_quiet on top.
    sa.days_active["codar"] = frozenset({(NOW - timedelta(days=8)).date()})
    sa.usual_gap_s["codar"] = 600.0
    assert _checks(_run([(st, sa)])) == [("K3LR", "never_started", "codar")]

    # A codar day 2 days back: started (and now merely quiet).
    sa.days_active["codar"] = frozenset({(NOW - timedelta(days=2)).date()})
    sa.last_arrival["codar"] = NOW - timedelta(days=2)
    assert _checks(_run([(st, sa)])) == [("K3LR", "client_quiet", "codar")]



def test_never_started_counts_a_day_exactly_7_days_back_as_silent():
    st, sa = _live("K3LR", add=("codar",))
    sa.days_active["codar"] = frozenset({(NOW - timedelta(days=7)).date()})
    assert _checks(_run([(st, sa)])) == [("K3LR", "never_started", "codar")]

def test_expect_drop_beats_expect_add():
    st, sa = _live(add=("codar",), drop=("codar",))
    assert _run([(st, sa)]) == []


# --- activity low ----------------------------------------------------------

def _spots(per_hour_baseline, last3, *, hist_days=8, outage_days=()):
    """wspr_spots: `per_hour_baseline` in every hour of the past `hist_days`
    days, 0 in the hours of `outage_days`, and `last3` spread over the last 3 h."""
    st, sa = _live()
    _add(sa, "wspr_spots", ago=timedelta(minutes=1), gap=120.0, ndays=hist_days + 1)
    hourly = {}
    for d in range(1, hist_days + 1):
        for h in LAST3:
            hourly[h - timedelta(days=d)] = 0 if d in outage_days else per_hour_baseline
    counts = [last3 // 3 + (1 if k < last3 % 3 else 0) for k in range(3)]
    for h, n in zip(LAST3, counts):
        hourly[h] = n
    sa.hourly["wspr_spots"] = hourly
    return st, sa


def test_activity_low():
    st, sa = _spots(100, 50)
    fs = _run([(st, sa)])
    assert _checks(fs) == [("A", "activity_low", "wspr_spots")]
    assert fs[0].numbers == {"last3h": 50, "usual3h": 300}
    st, sa = _spots(15, 2)
    assert _run([(st, sa)]) == []


def test_activity_low_not_raised_at_or_above_30_percent():
    st, sa = _spots(100, 90)
    assert _run([(st, sa)]) == []


def test_baseline_skips_alarm_hours():
    # Outage at the same hours on days 1-4: the plain median would be 0 and
    # hide the drop.  Excluding those alarm hours restores the median of 100.
    outage = (1, 2, 3, 4)
    st, sa = _spots(100, 50, outage_days=outage)
    assert _run([(st, sa)]) == []
    alarm = {"A": {h - timedelta(days=d) for d in outage for h in LAST3}}
    fs = _run([(st, sa)], alarm)
    assert _checks(fs) == [("A", "activity_low", "wspr_spots")]
    assert fs[0].numbers == {"last3h": 50, "usual3h": 300}


def test_baseline_skips_hour_with_every_day_excluded():
    # One of the three hours sat under alarm on all 7 baseline days: no
    # baseline for it, so the check is skipped.
    st, sa = _spots(100, 50)
    alarm = {"A": {LAST3[0] - timedelta(days=d) for d in range(1, 8)}}
    assert _run([(st, sa)], alarm) == []


def test_under_7_days_history_raises_no_activity_alarm():
    st, sa = _spots(100, 50, hist_days=5)
    assert _run([(st, sa)]) == []



def test_activity_low_fires_with_history_exactly_7_days_back():
    st, sa = _spots(100, 50, hist_days=7)
    assert min(sa.days_active["wspr_spots"]) == NOW.date() - timedelta(days=7)
    assert _checks(_run([(st, sa)])) == [("A", "activity_low", "wspr_spots")]


def test_quiet_source_raises_no_activity_low():
    # One source, one alarm: wspr_spots silent 4 h against a strong baseline.
    st, sa = _spots(100, 0)
    sa.last_arrival["wspr_spots"] = NOW - timedelta(hours=4)
    assert _checks(_run([(st, sa)])) == [("A", "client_quiet", "wspr_spots")]

# --- one cause, one alarm --------------------------------------------------

def test_off_air_suppresses_quiet_and_low():
    st, sa = _spots(100, 0)
    st = _st("A", add=("codar",))
    _add(sa, "ft8", ago=timedelta(hours=2), gap=60.0)
    sa.last_arrival["wspr_noise"] = NOW - timedelta(hours=2)
    sa.last_arrival["wspr_spots"] = NOW - timedelta(hours=2)
    fs = _run([(st, sa)])
    assert _checks(fs) == [("A", "off_air", "")]
    assert verdict(st, sa, fs) == "off"


# --- ingest stalled --------------------------------------------------------

def test_mass_silence_raises_one_ingest_alarm():
    pairs = [_live(f"S{i}") for i in range(10)]
    for _, sa in pairs[:6]:
        sa.last_arrival["wspr_noise"] = NOW - timedelta(hours=1)
    fs = _run(pairs, bystanders=0)
    assert fs == [Finding("wd30", "ingest_stalled", "", fs[0].numbers)]
    assert fs[0].numbers["n_silent"] == 6
    assert fs[0].numbers["n_with_history"] == 10
    assert fs[0].numbers["silent"] == [f"S{i}" for i in range(6)]
    json.dumps(fs[0].numbers)
    for st, sa in pairs[:6]:
        assert verdict(st, sa, fs) == "grey"
    for st, sa in pairs[6:]:
        assert verdict(st, sa, fs) == "ok"


def test_mass_silence_keeps_findings_of_live_stations():
    pairs = [_live(f"S{i}") for i in range(10)]
    for _, sa in pairs[:6]:
        sa.last_arrival["wspr_noise"] = NOW - timedelta(hours=1)
    st9, sa9 = pairs[9]
    _add(sa9, "ft8", ago=timedelta(hours=1), gap=60.0)
    fs = _run(pairs, bystanders=0)
    assert _checks(fs) == [("S9", "client_quiet", "ft8"), ("wd30", "ingest_stalled", "")]
    assert verdict(st9, sa9, fs) == "warn"


def test_small_fleet_silence_is_off_air_not_ingest_stall():
    # Under INGEST_MIN_STATIONS stations with history, the guard stays off:
    # one of two silent is a station fault, not wd30's.
    st, sa = _live("S0")
    sa.last_arrival["wspr_noise"] = NOW - timedelta(hours=1)
    pairs = [(st, sa), _live("S1")]
    fs = _run(pairs, bystanders=0)
    assert _checks(fs) == [("S0", "off_air", "")]
    assert verdict(st, sa, fs) == "off"
    # Two of three silent is a majority, but still under the minimum.
    pairs.append(_live("S2"))
    pairs[1][1].last_arrival["wspr_noise"] = NOW - timedelta(hours=1)
    fs = _run(pairs, bystanders=0)
    assert _checks(fs) == [("S0", "off_air", ""), ("S1", "off_air", "")]


def test_ingest_guard_fires_at_four_stations():
    pairs = [_live(f"S{i}") for i in range(4)]
    for _, sa in pairs[:3]:
        sa.last_arrival["wspr_noise"] = NOW - timedelta(hours=1)
    assert _checks(_run(pairs, bystanders=0)) == [("wd30", "ingest_stalled", "")]


def test_half_silent_is_not_ingest_stalled():
    pairs = [_live(f"S{i}") for i in range(10)]
    for _, sa in pairs[:5]:
        sa.last_arrival["wspr_noise"] = NOW - timedelta(hours=1)
    fs = _run(pairs, bystanders=0)
    assert _checks(fs) == [(f"S{i}", "off_air", "") for i in range(5)]


def test_grey_stations_do_not_count_toward_ingest_stall():
    pairs = [_live(f"S{i}") for i in range(3)]
    pairs += [(_st(f"G{i}"), StationActivity()) for i in range(10)]
    pairs[0][1].last_arrival["wspr_noise"] = NOW - timedelta(hours=1)
    assert _checks(_run(pairs, bystanders=0)) == [("S0", "off_air", "")]


# --- grey and verdict ------------------------------------------------------

def test_grey_station_raises_nothing():
    st = _st("NEW", add=("codar", "ft8"))
    sa = StationActivity()
    fs = _run([(st, sa)])
    assert fs == []
    assert verdict(st, sa, fs) == "grey"


def test_verdict_ok_warn_off():
    st, sa = _live()
    assert verdict(st, sa, _run([(st, sa)])) == "ok"
    _add(sa, "ft8", ago=timedelta(hours=1), gap=60.0)
    assert verdict(st, sa, _run([(st, sa)])) == "warn"
    sa.last_arrival["wspr_noise"] = NOW - timedelta(hours=1)
    assert verdict(st, sa, _run([(st, sa)])) == "off"


def test_numbers_are_json_serialisable():
    st, sa = _live(add=("codar",))
    _add(sa, "ft8", ago=timedelta(hours=1), gap=60.0)
    for f in _run([(st, sa)]):
        json.dumps(f.numbers)
    sa.last_arrival["wspr_noise"] = NOW - timedelta(hours=1)
    sa.last_arrival.pop("ft8")
    for f in _run([(st, sa)]):
        json.dumps(f.numbers)
