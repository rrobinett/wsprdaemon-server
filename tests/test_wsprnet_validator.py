"""wsprnet_validator: which spots never reached wsprnet.org.

The properties that matter, each of which would make the report lie:

  * Identity: a spot is expected under the call the station reports UNDER,
    resolved in the forwarder's order, or every proxied multi-receiver site
    (KFS, KPH) reads as 100 % missing.
  * Settling: hours wspr.rx has not caught up on are never judged.
  * Publication order: missing rows are written before the summary that
    makes them current, so a run that dies half way shows nothing new.
  * A station that never reports to wsprnet is "absent", not "losing 100 %",
    and is alerted only when it STOPS.

Run with:  python3 -m pytest tests/test_wsprnet_validator.py -v
"""
from __future__ import annotations

import sys
from datetime import datetime, timedelta, timezone
from pathlib import Path

import pytest

HERE = Path(__file__).resolve().parent
sys.path.insert(0, str(HERE.parent))

import wsprnet_validator as wv  # noqa: E402

NOW = datetime(2026, 11, 14, 22, 37, 12, tzinfo=timezone.utc)


# ---------- identity ----------

def test_overrides_win_over_declared_identities():
    keys, calls = wv.identity_map(
        {"KFS/SW/KFS-SW": {"call": "KFS", "grid": "CM87tj"}},
        {"KFS/SW/KFS-SW": {"call": "KFS-OVR"}})
    assert dict(zip(keys, calls))["KFS/SW/KFS-SW"] == "KFS-OVR"


def test_an_identity_without_a_call_is_dropped():
    keys, _ = wv.identity_map({"X/rx1": {"grid": "FN20"}}, {})
    assert "X/rx1" not in keys


def test_identity_arrays_are_never_empty():
    """ClickHouse cannot type `transform(x, [], [], x)`: the sentinel pair
    keeps the query valid on a server where nobody declared anything."""
    keys, calls = wv.identity_map({}, {})
    assert keys == [""] and calls == [""]


def test_identity_arrays_stay_aligned():
    keys, calls = wv.identity_map(
        {"B/r": {"call": "BB"}, "A/r": {"call": "AA"}}, {"C": {"call": "CC"}})
    assert dict(zip(keys, calls)) == {"": "", "A/r": "AA", "B/r": "BB",
                                      "C": "CC"}


def test_reporter_expression_tries_the_receiver_key_before_the_site_key():
    """Same order as wsprnet_forwarder.reporter_for()."""
    expr = wv._REPORTER_EXPR
    assert expr.index("concat(toString(rx_sign), '/', toString(rx_id))") \
        < expr.index("transform(toString(rx_sign)")


def test_the_unresolved_hash_is_never_expected():
    assert "tx_sign != '<...>'" in wv._WD_SUB


# ---------- windows ----------

def test_window_is_whole_hours_and_leaves_the_newest_unjudged():
    t0, t1 = wv.run_window(NOW, 30, 4)
    assert t1 == datetime(2026, 11, 14, 18, 0, tzinfo=timezone.utc)
    assert t0 == t1 - timedelta(hours=26)


def test_lookback_must_exceed_settle():
    with pytest.raises(ValueError):
        wv.run_window(NOW, 4, 4)


# ---------- judging ----------

def _t(rep, wd, on, *, miss=None, dup=0, proxy=0):
    return wv.Tally(rep, proxy, wd, on, wd - on if miss is None else miss,
                    0, dup, 0)


def test_status_absent_few_reporting():
    assert _t("A", 1000, 0).status == "absent"
    assert _t("A", 1000, 99).status == "absent"
    assert _t("A", 1000, 100).status == "reporting"
    assert _t("A", wv.MIN_SPOTS_FOR_STATUS - 1, 0).status == "few"


def test_losses_over_threshold_alert():
    alerts = wv.judge({"AB4EJ": _t("AB4EJ", 1000, 900)}, {})
    assert len(alerts) == 1 and "100 of 1000" in alerts[0]


def test_losses_under_threshold_do_not_alert():
    assert wv.judge({"AB4EJ": _t("AB4EJ", 1000, 990)}, {}) == []


def test_duplicates_over_threshold_alert():
    alerts = wv.judge({"GB0PSC": _t("GB0PSC", 1000, 1000, dup=50)}, {})
    assert len(alerts) == 1 and "duplicate" in alerts[0]


def test_a_station_that_never_reports_to_wsprnet_is_not_an_alert():
    """VY0ERC, TuhuraOtago: uploads off.  100 % 'missing' every hour would
    bury every real alert."""
    assert wv.judge({"VY0ERC": _t("VY0ERC", 500, 0)},
                    {"VY0ERC": _t("VY0ERC", 2000, 0)}) == []


def test_a_station_that_stops_reporting_is_an_alert():
    alerts = wv.judge({"N8UR": _t("N8UR", 500, 3)},
                      {"N8UR": _t("N8UR", 2000, 1995)})
    assert len(alerts) == 1 and "STOPPED" in alerts[0]


def test_table_puts_the_worst_reporting_station_first():
    table = wv.format_table({"A": _t("A", 1000, 999),
                             "B": _t("B", 1000, 950),
                             "C": _t("C", 1000, 0)})
    rows = table.splitlines()[2:]
    assert [r.split()[0] for r in rows] == ["B", "A", "C"]


# ---------- the run ----------

class FakeCH:
    def __init__(self, stored=()):
        self.calls = []
        self.stored = list(stored)

    def execute(self, sql, params=None):
        self.calls.append((sql, params))
        if "wsprnet_reporter" in sql:
            return []
        if "FROM wsprdaemon.wsprnet_validation FINAL" in sql:
            return self.stored
        return []


def _cfg(tmp_path, **kw):
    return wv.Config(reporters_path=tmp_path / "none.json",
                     state_dir=tmp_path / "state", **kw)


def test_missing_rows_are_inserted_before_the_summary(tmp_path):
    ch = FakeCH()
    wv.validate(ch, _cfg(tmp_path), now=NOW)
    inserts = [s for s, _ in ch.calls if s.startswith("INSERT")]
    assert inserts[0].startswith("INSERT INTO wsprdaemon.wsprnet_missing")
    assert inserts[1].startswith("INSERT INTO wsprdaemon.wsprnet_validation")


def test_both_inserts_carry_one_checked_at_and_window(tmp_path):
    ch = FakeCH()
    wv.validate(ch, _cfg(tmp_path), now=NOW)
    ps = [p for s, p in ch.calls if s.startswith("INSERT")]
    assert ps[0] == ps[1]
    assert ps[0]["t1"] == datetime(2026, 11, 14, 18, 0)
    assert ps[0]["checked_at"] == datetime(2026, 11, 14, 22, 37, 12)


def test_dry_run_writes_nothing(tmp_path, capsys):
    ch = FakeCH()
    wv.validate(ch, _cfg(tmp_path, dry_run=True), now=NOW)
    assert not any(s.startswith(("INSERT", "\nCREATE", "CREATE"))
                   for s, _ in ch.calls)
    assert not (tmp_path / "state").exists()


def test_report_file_lists_alerts(tmp_path):
    ch = FakeCH(stored=[("AB4EJ", 0, 1000, 800, 200, 0, 0, 0)])
    wv.validate(ch, _cfg(tmp_path), now=NOW)
    text = (tmp_path / "state" / "latest.txt").read_text()
    assert "ALERTS" in text and "AB4EJ" in text


def test_sql_has_no_bare_percent_signs():
    """clickhouse_driver substitutes params with Python's % operator, so a
    literal % in the SQL would raise or corrupt the query."""
    import re
    for sql in (wv.SUMMARY_SELECT, wv.MISSING_SELECT, wv.STORED_SUMMARY):
        assert not re.search(r"%(?!\()", sql)
