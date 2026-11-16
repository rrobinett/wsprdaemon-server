"""Tests for fleetboard.stations: map grouping and the alias fold."""
from __future__ import annotations

import sys
import types as _types
from datetime import date, datetime, timedelta
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
for _mod in ('clickhouse_connect', 'clickhouse_driver'):
    if _mod not in sys.modules:
        _stub = _types.ModuleType(_mod)
        _stub.Client = object
        _stub.get_client = lambda **kw: None
        sys.modules[_mod] = _stub

from fleetboard.sources import Activity  # noqa: E402
from fleetboard.stations import fold, load_map  # noqa: E402

NOW = datetime(2026, 11, 16, 12, 0, 0)


def _row(name, source, reporter, alias=0, add=(), drop=()):
    return dict(name=name, grid6="EN16ov", source=source, reporter=reporter,
                is_alias=alias, expect_add=list(add), expect_drop=list(drop),
                loaded_at=NOW)


def test_load_map_groups_rows_by_name():
    rows = [_row("A", "ft8", "A", add=["codar"]), _row("A", "ft8", "A-OLD", 1, add=["codar"]),
            _row("A", "wspr_spots", "A", add=["codar"]), _row("B", "ft8", "B")]
    st = {s.name: s for s in load_map(rows)}
    assert set(st) == {"A", "B"}
    assert st["A"].reporters == {"ft8": ("A", "A-OLD"), "wspr_spots": ("A",)}
    assert st["A"].expect_add == ("codar",)
    assert st["A"].grid6 == "EN16ov"
    assert "hfdl" not in st["A"].reporters


def test_fold_merges_aliases_into_one_station():
    stn = load_map([_row("N8UR-SIGMOND", "ft8", "N8UR-SIGMOND"),
                    _row("N8UR-SIGMOND", "ft8", "N8UR", 1)])
    two_ago = NOW - timedelta(days=2)
    act = Activity(
        last_arrival={("ft8", "N8UR"): two_ago, ("ft8", "N8UR-SIGMOND"): NOW},
        days_active={("ft8", "N8UR"): frozenset({two_ago.date(), NOW.date()}),
                     ("ft8", "N8UR-SIGMOND"): frozenset({NOW.date()})},
        usual_gap_s={("ft8", "N8UR"): 30.0, ("ft8", "N8UR-SIGMOND"): 15.0},
        hourly={("ft8", "N8UR"): {NOW: 2}, ("ft8", "N8UR-SIGMOND"): {NOW: 3}},
    )
    sa = fold(act, stn)["N8UR-SIGMOND"]
    assert sa.last_arrival["ft8"] == NOW
    assert sa.days_active["ft8"] == frozenset({two_ago.date(), NOW.date()})
    assert sa.usual_gap_s["ft8"] == 15.0
    assert sa.hourly["ft8"] == {NOW: 5}
    assert "codar" not in sa.last_arrival


def test_reporter_in_no_station_is_ignored():
    stn = load_map([_row("A", "ft8", "A"), _row("Empty", "codar", "E")])
    act = Activity(last_arrival={("ft8", "STRANGER"): NOW})
    out = fold(act, stn)
    assert set(out) == {"A", "Empty"}
    assert out["A"].last_arrival == {} and out["Empty"].days_active == {}
