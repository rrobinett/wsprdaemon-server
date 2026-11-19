"""The drift check: faults and test reminders open alarms, drift alone shows."""
from __future__ import annotations

import json
import sys
import types as _types
from datetime import datetime, timedelta, timezone
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
for _mod in ('clickhouse_connect', 'clickhouse_driver'):
    if _mod not in sys.modules:
        _stub = _types.ModuleType(_mod)
        _stub.Client = object
        _stub.get_client = lambda **kw: None
        sys.modules[_mod] = _stub

from fleetboard import alarms as al  # noqa: E402
from fleetboard.board import build  # noqa: E402
from fleetboard.checks import evaluate  # noqa: E402
from fleetboard.checks import drift_hold  # noqa: E402
from fleetboard.sources import Activity, DriftRow, parse, queries  # noqa: E402
from fleetboard.stations import Station, StationActivity, fold  # noqa: E402

NOW = datetime(2026, 11, 18, 12, 0, 0)


def _fault(key, detail="d", fix="smd fix"):
    kind, _, subject = key.partition(":")
    return {"kind": kind, "subject": subject, "detail": detail, "fix": fix, "key": key}


def _row(age_h=1, faults=(), reminders=(), n_drift=0):
    t = NOW - timedelta(hours=age_h)
    return DriftRow(t, t, len(faults), n_drift, tuple(faults), tuple(reminders))


def _st(name="A"):
    return Station(name=name, grid6="EN16ov", reporters={})


def _findings(row, name="A"):
    sa = StationActivity(drift=row)
    return [f for f in evaluate([_st(name)], {name: sa}, NOW, {}) if f.check != "off_air"]


def test_fault_opens_and_clears_alarm():
    fs = _findings(_row(faults=[_fault("unit:x.service")]))
    assert [(f.check, f.subject) for f in fs] == [("drift_fault", "A unit:x.service")]
    opened = al.step([], fs, [], NOW)
    assert [a.state for a in opened] == ["open"]
    prev, t = opened, NOW
    for _ in range(al.CLEAR_AFTER):          # later rows lack it: clears, like the others
        t += timedelta(hours=1)
        prev = al.step(prev, _findings(_row()), [], t)
    assert prev[0].state == "cleared"


def test_reminder_opens_test_restore():
    rem = {"station": "A", "key": "unit:x.service", "by": "mjh", "reason": "rust fusion",
           "until": "2026-11-19", "fix": "smd align --apply"}
    fs = _findings(_row(reminders=[rem]))
    assert [(f.check, f.subject) for f in fs] == [("test_restore", "A unit:x.service")]
    text = fs[0].numbers["text"]
    for part in ("mjh", "rust fusion", "2026-11-19", "smd align --apply"):
        assert part in text


def test_ended_test_fault_is_a_test_restore():
    f = _fault("unit:x.service", detail="temporary test ended 2026-11-17; restore baseline: smd fix")
    assert [x.check for x in _findings(_row(faults=[f]))] == ["test_restore"]


def test_drift_only_no_alarm():
    row = _row(n_drift=3)
    assert _findings(row) == []
    sa = StationActivity(last_arrival={"ft8": NOW}, drift=row)
    out = build([_st()], {"A": sa}, [], [], NOW.replace(tzinfo=timezone.utc).timestamp(), now=NOW)
    d = json.loads(out["fleet_board"][0]["drift"])
    assert d["n_drift"] == 3 and d["n_fault"] == 0 and not d["stale"]
    assert out["fleet_alarms"] == []


def test_stale_row_shows_not_alarms():
    row = _row(age_h=49, faults=[_fault("unit:x.service")])
    assert _findings(row) == []
    sa = StationActivity(last_arrival={"ft8": NOW}, drift=row)
    out = build([_st()], {"A": sa}, [], [], NOW.replace(tzinfo=timezone.utc).timestamp(), now=NOW)
    assert json.loads(out["fleet_board"][0]["drift"])["stale"] is True
    assert out["fleet_alarms"] == []


def test_no_row_shows_empty_drift_column():
    out = build([_st()], {"A": StationActivity()}, [], [], 0.0, now=NOW)
    assert out["fleet_board"][0]["drift"] == ""


def test_one_station_many_faults_one_alarm_each():
    fs = _findings(_row(faults=[_fault("unit:a.service"), _fault("unit:b.service"),
                                _fault("pkg:c")]))
    alarms = al.step([], fs, [], NOW)
    assert sorted(a.subject for a in alarms) == ["A pkg:c", "A unit:a.service", "A unit:b.service"]
    assert all(a.check == "drift_fault" for a in alarms)


def test_alias_reporter_row_folds_onto_its_station():
    st = Station(name="AC0G-B4", grid6="EN16ov",
                 reporters={"wspr_noise": ("AC0G-B4",), "ft8": ("AC0G/B4",)})
    assert fold(Activity(drift={"AC0G-B4": _row()}), [st])["AC0G-B4"].drift is not None
    row = _row(faults=[_fault("unit:x.service")])
    act = Activity(drift={"AC0G/B4": row})
    assert fold(act, [st])["AC0G-B4"].drift == row
    assert fold(Activity(drift={"OTHER": row}), [st])["AC0G-B4"].drift is None


def test_parse_reads_the_latest_row():
    res = {"drift_latest": [("A", NOW, NOW, 1, 2, json.dumps([_fault("unit:x")]), "not json")]}
    r = parse(res).drift["A"]
    assert (r.n_fault, r.n_drift, r.reminders) == (1, 2, ())
    assert r.faults[0]["key"] == "unit:x"
    assert "drift_latest" in queries(NOW)


def _public(findings, now=NOW):
    live = al.step([], findings, [], now)
    sa = StationActivity(last_arrival={"ft8": now})
    return build([_st()], {"A": sa}, live, findings, now.replace(tzinfo=timezone.utc).timestamp(),
                 now=now)


def test_public_rows_carry_no_path_command_or_operator_text():
    secret = ["/etc/sigmond/overrides.toml", "smd admin instance remove", "deadbeef", "'10.3.2.1'",
              "mjh", "lab box behind"]
    f = _fault("stray:/opt/secret/dir", detail="/etc/sigmond/overrides.toml sets x = '10.3.2.1' deadbeef",
               fix="smd admin instance remove a b")
    rem = {"station": "A", "key": "unit:x.service", "by": "mjh", "reason": "lab box behind NAT",
           "until": "2026-11-19", "fix": "smd admin instance remove a b"}
    fs = _findings(_row(faults=[f], reminders=[rem]))
    assert any("/etc" in x.numbers["detail"] for x in fs)           # private keeps it
    out = _public(fs)
    blob = json.dumps(out)
    for bad in secret + ["/opt", "secret", "dir"]:
        assert bad not in blob, bad
    subjects = [a["subject"] for a in out["fleet_alarms"]]
    assert len(set(subjects)) == 2 and all(x.startswith("A ") for x in subjects)
    texts = sorted(json.loads(a["numbers"])["text"] for a in out["fleet_alarms"])
    assert texts == ["drift fault: stray", "test ending: unit"]


def test_public_id_is_stable_so_open_and_close_pair():
    f = _fault("unit:x.service")
    a = _public(_findings(_row(faults=[f])))["fleet_alarms"][0]
    b = _public(_findings(_row(faults=[f])))["fleet_alarms"][0]
    assert a["subject"] == b["subject"]


def test_stale_row_holds_live_drift_alarms_open():
    fs = _findings(_row(faults=[_fault("unit:x.service")]))
    prev = al.step([], fs, [], NOW)
    stale = StationActivity(drift=_row(age_h=60, faults=[_fault("unit:x.service")]))
    later = NOW + timedelta(hours=1)
    findings = evaluate([_st()], {"A": stale}, later, {})
    hold = drift_hold([_st()], {"A": stale}, later)
    t = later
    for _ in range(5):
        assert al.step(prev, findings, [], t, hold=hold) == []
        t += timedelta(hours=1)
    # board shows it stale
    out = build([_st()], {"A": stale}, prev, [], later.replace(tzinfo=timezone.utc).timestamp(),
                now=later)
    assert json.loads(out["fleet_board"][0]["drift"])["stale"] is True


def test_hold_spares_other_checks_of_the_station():
    prev = al.step([], [Finding_("off_air", "")], [], NOW)
    out = al.step(prev, [], [], NOW + timedelta(hours=1),
                  hold={("A", "drift_fault"), ("A", "test_restore")})
    assert out and out[0].false_streak == 1


def Finding_(check, subject):
    from fleetboard.checks import Finding
    return Finding("A", check, subject, {})


def test_future_stamped_row_neither_wins_nor_keeps_fresh():
    year = NOW + timedelta(days=365)
    real = NOW - timedelta(hours=72)
    res = {"drift_latest": [("A", year, real, 0, 0, "[]", "[]")]}
    assert parse(res, now=NOW).drift == {}                  # time a year ahead: dropped
    res = {"drift_latest": [("A", year, NOW, 0, 0, "[]", "[]")]}
    assert parse(res, now=NOW).drift == {}
    # a fast-but-sane station clock: freshness reads ingested, so it still goes stale
    old = DriftRow(NOW + timedelta(minutes=5), NOW - timedelta(hours=72))
    assert drift_hold([_st()], {"A": StationActivity(drift=old)}, NOW)
    sql = queries(NOW)
    assert "ingested_at DESC" in sql["drift_latest"] and "INTERVAL 600 SECOND" in sql["drift_latest"]


def test_row_under_display_name_not_in_reporters_is_not_folded():
    st = Station(name="AC0G-B4", grid6="", reporters={"ft8": ("AC0G/B4",)})
    assert fold(Activity(drift={"AC0G-B4": _row()}), [st])["AC0G-B4"].drift is None
