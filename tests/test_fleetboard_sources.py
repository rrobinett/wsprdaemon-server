"""Tests for fleetboard.sources: pure parse/query-building, no database."""
from __future__ import annotations

import sys
import types as _types
from datetime import date, datetime
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
for _mod in ('clickhouse_connect', 'clickhouse_driver'):
    if _mod not in sys.modules:
        _stub = _types.ModuleType(_mod)
        _stub.Client = object
        _stub.get_client = lambda **kw: None
        sys.modules[_mod] = _stub

from fleetboard import sources  # noqa: E402

NOW = datetime(2026, 11, 16, 12, 0, 0)


def test_last_arrival_keys_source_and_reporter():
    t = datetime(2026, 11, 16, 11, 55)
    a = sources.parse({"wspr_spots_last": [("AC0G", t)], "ft8_last": [("AC0G", t)]})
    assert a.last_arrival == {("wspr_spots", "AC0G"): t, ("ft8", "AC0G"): t}


def test_epoch_ingested_at_is_no_arrival():
    a = sources.parse({"wspr_noise_last": [("OLD", sources.EPOCH)]})
    assert a.last_arrival == {}


def test_hourly_only_for_hourly_sources():
    h = datetime(2026, 11, 16, 10)
    a = sources.parse({"wspr_spots_hourly": [("A", h, 30)],
                       "hfdl_hourly": [("A", h, 5)]})
    assert a.hourly == {("wspr_spots", "A"): {h: 30}}
    assert not any(k.endswith("_hourly") and k.startswith(("wspr_noise", "codar", "hfdl"))
                   for k in sources.queries(NOW))


def test_days_and_gap_parse():
    d1, d2 = date(2026, 11, 14), date(2026, 11, 15)
    a = sources.parse({"codar_days": [("S", d1), ("S", d2), ("S", d1)],
                       "codar_gap": [("S", 120)]})
    assert a.days_active == {("codar", "S"): frozenset({d1, d2})}
    assert a.usual_gap_s == {("codar", "S"): 120.0}


def test_queries_use_ingested_at_for_last_and_time_for_statistics():
    q = sources.queries(NOW)
    lasts = [s for n, s in q.items() if n.endswith("_last")]
    assert len(lasts) == len(sources.SOURCES)
    assert all("ingested_at" in s for s in lasts)
    for n, s in q.items():
        if n.endswith(("_gap", "_days")):
            assert "ingested_at" not in s


def test_codar_and_hfdl_split_on_rate():
    q = sources.queries(NOW)
    assert "rate_hz_per_s != 0" in q["codar_last"]
    assert "rate_hz_per_s = 0" in q["hfdl_last"]


def test_ft8_only():
    q = sources.queries(NOW)
    assert all("mode = 'ft8'" in s for n, s in q.items() if n.startswith("ft8_"))


def test_every_query_bounded_and_reporter_first():
    for n, s in sources.queries(NOW).items():
        assert "INTERVAL" in s and "2026-11-16 12:00:00" in s, n
        assert " AS reporter" in s, n


def test_fetch_runs_every_query():
    class R:  # stands in for a clickhouse_connect result
        result_rows: list = []
    seen = []

    class C:
        def query(self, sql):
            seen.append(sql)
            return R()
    a = sources.fetch(C(), NOW)
    assert len(seen) == len(sources.queries(NOW))
    assert a.last_arrival == {}


def test_window_lengths_pinned():
    from datetime import timezone  # noqa: F401
    for n, s in sources.queries(NOW).items():
        if n.endswith("_last"):
            assert "ingested_at > toDateTime('2026-11-16 12:00:00', 'UTC') - INTERVAL 14 DAY" in s, n
        elif n.endswith("_days"):
            assert "time > toDateTime('2026-11-16 12:00:00', 'UTC') - INTERVAL 14 DAY" in s, n
        elif n.endswith("_gap"):
            assert "time > toDateTime('2026-11-16 12:00:00', 'UTC') - INTERVAL 7 DAY" in s, n
        elif n.endswith("_hourly"):
            assert "time > toDateTime('2026-11-16 12:00:00', 'UTC') - INTERVAL 171 HOUR" in s, n
            assert "+ INTERVAL" not in s, n


def test_gap_sql_quantile_and_leading_zero():
    for n, s in sources.queries(NOW).items():
        if n.endswith("_gap"):
            assert "quantileExact(0.9)" in s, n
            assert "arrayPopFront(arrayDifference(" in s, n


def test_aware_datetimes_normalised_to_naive_utc():
    from datetime import timedelta, timezone
    t = datetime(2026, 11, 16, 11, 55, tzinfo=timezone.utc)
    est = timezone(timedelta(hours=-5))
    h = datetime(2026, 11, 16, 5, tzinfo=est)  # 10:00 UTC
    a = sources.parse({
        "ft8_last": [("A", t), ("EPOCHAWARE", datetime(1970, 1, 1, tzinfo=timezone.utc))],
        "wspr_spots_hourly": [("A", h, 7)],
    })
    assert a.last_arrival == {("ft8", "A"): datetime(2026, 11, 16, 11, 55)}
    assert a.hourly == {("wspr_spots", "A"): {datetime(2026, 11, 16, 10): 7}}
