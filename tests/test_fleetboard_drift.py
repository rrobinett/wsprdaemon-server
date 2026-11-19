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
from fleetboard.sources import Activity, DriftRow, parse, queries  # noqa: E402
from fleetboard.stations import Station, StationActivity, fold  # noqa: E402

NOW = datetime(2026, 11, 18, 12, 0, 0)


def _fault(key, detail="d", fix="smd fix"):
    kind, _, subject = key.partition(":")
    return {"kind": kind, "subject": subject, "detail": detail, "fix": fix, "key": key}


def _row(age_h=1, faults=(), reminders=(), n_drift=0):
    return DriftRow(NOW - timedelta(hours=age_h), len(faults), n_drift,
                    tuple(faults), tuple(reminders))


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
    row = _row(faults=[_fault("unit:x.service")])
    act = Activity(drift={"AC0G/B4": row})
    assert fold(act, [st])["AC0G-B4"].drift == row
    assert fold(Activity(drift={"OTHER": row}), [st])["AC0G-B4"].drift is None


def test_parse_reads_the_latest_row():
    res = {"drift_latest": [("A", NOW, 1, 2, json.dumps([_fault("unit:x")]), "not json")]}
    r = parse(res).drift["A"]
    assert (r.n_fault, r.n_drift, r.reminders) == (1, 2, ())
    assert r.faults[0]["key"] == "unit:x"
    assert "drift_latest" in queries(NOW)
