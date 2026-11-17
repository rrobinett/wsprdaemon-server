"""Tests for fleetboard.board: public rows, grid6, rank."""
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

from fleetboard.alarms import Alarm  # noqa: E402
from fleetboard.board import build  # noqa: E402
from fleetboard.checks import Finding  # noqa: E402
from fleetboard.stations import Station, StationActivity  # noqa: E402

NOW = datetime(2026, 11, 16, 12, 10, 0)
BUILT = NOW.replace(tzinfo=None).timestamp()


def _st(name, grid="EN16ov"):
    return Station(name=name, grid6=grid, reporters={})


def _live(name, n=1, check="client_quiet", state="open"):
    return [Alarm(name, check, "s%d" % i, NOW, NOW, state, 0, "{}", NOW) for i in range(n)]


def _sa(recent=True):
    last = NOW - timedelta(minutes=1 if recent else 400)
    return StationActivity(last_arrival={"ft8": last})


def test_public_rows_carry_no_ack_fields():
    a = Alarm("A", "client_quiet", "ft8", NOW, NOW, "acknowledged", 0, "{}", NOW)
    out = build([_st("A")], {"A": _sa()}, [a], [], BUILT, now=NOW)
    assert len(out["fleet_alarms"]) == 1
    row = out["fleet_alarms"][0]
    assert row["state"] == "acknowledged"
    assert not {"acked_by", "note"} & set(row)


def test_grid6_truncates_finer_grids():
    out = build([_st("A", "CM87tj12")], {"A": _sa()}, [], [], BUILT, now=NOW)
    assert out["fleet_board"][0]["grid6"] == "CM87tj"


def test_rank_worst_first_grey_last():
    sts = [_st(n) for n in ("G", "W1", "W2", "OK", "OFF")]
    acts = {n: _sa() for n in ("W1", "W2", "OK", "OFF")}
    acts["G"] = StationActivity()
    findings = [Finding("OFF", "off_air", "", {})]
    alarms = _live("OFF") + _live("W1") + _live("W2", 2)
    out = build(sts, acts, alarms, findings, BUILT, now=NOW)
    order = [r["station"] for r in sorted(out["fleet_board"], key=lambda r: r["rank"])]
    assert order == ["OFF", "W2", "W1", "OK", "G"]
    by = {r["station"]: r for r in out["fleet_board"]}
    assert by["W2"]["n_open"] == 2 and by["G"]["verdict"] == "grey"


def test_sources_json_states():
    st = Station(name="A", grid6="EN16ov", reporters={}, expect_add=("codar",))
    sa = _sa()
    findings = [Finding("A", "never_started", "codar", {})]
    r = build([st], {"A": sa}, [], findings, BUILT, now=NOW)["fleet_board"][0]
    src = json.loads(r["sources"])
    assert src["codar"] == {"expected": True, "last_arrival_utc": None, "state": "never"}
    assert src["ft8"]["state"] == "-" and src["ft8"]["last_arrival_utc"] is not None
