"""sigmondb row files and loader.  Fake client; no ClickHouse."""
import logging
import os
import sys
from datetime import datetime

import pytest

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from sigmondb import ddl, load, rows  # noqa: E402


class FakeClient:
    """Records inserts.  Like clickhouse-connect, it rejects None in a non-Nullable column."""

    def __init__(self):
        self.calls = []

    def insert(self, table, data, column_names=None):
        tname = table.split(".")[-1]
        types = ddl.COLUMN_TYPES[tname]
        for row in data:
            for c, v in zip(column_names, row):
                if v is None and not ddl.is_nullable(types[c]):
                    raise ValueError("Invalid None value in non-Nullable column, column name: " + c)
        self.calls.append((table, [tuple(r) for r in data], list(column_names)))


_DEFAULT = {"String": "x", "LowCardinality(String)": "x", "Float64": 1.0, "Float32": 1.0,
            "UInt32": 1, "UInt16": 1, "UInt8": 1, "Int64": 1, "DateTime": datetime(2026, 11, 8),
            "DateTime64(3)": datetime(2026, 11, 8), "DateTime64(9)": datetime(2026, 11, 8)}


def valid_row(table, **kw):
    """A row with a real value in every non-Nullable column and None in Nullable ones."""
    r = {}
    for c, t in ddl.COLUMN_TYPES[table].items():
        if ddl.is_nullable(t):
            r[c] = None
        elif t.startswith("Map"):
            r[c] = {}
        elif t.startswith("Array"):
            r[c] = []
        elif t.startswith("FixedString"):
            r[c] = "0" * 64
        else:
            r[c] = _DEFAULT[t]
    r.update(kw)
    return r


def _arrival(i=0, **kw):
    r = valid_row("arrival")
    r.update(time=datetime(2026, 11, 8, 12, 0, 1, 250000), reporter_id="AC0G", tx_id="",
             group_key="g", centre_hz=5.0e6, rate_hz_per_s=0.0, delay_ns=2 ** 53 + 1 + i,
             detector_version="v", engineering={"tier": "T6"})
    r.update(kw)
    return r


def test_round_trip_keeps_integer_ns(tmp_path):
    p = tmp_path / "a.jsonl"
    src = [_arrival(0), _arrival(1, delay_ns=-5, engineering={})]
    assert rows.write_rows(p, src) == 2
    got = rows.read_rows(p)
    assert got == src
    assert type(got[0]["delay_ns"]) is int and got[0]["delay_ns"] == 2 ** 53 + 1
    assert got[0]["time"] == datetime(2026, 11, 8, 12, 0, 1, 250000)
    assert got[0]["time"].tzinfo is None
    assert got[0]["delay_sigma_ns"] is None


def test_unknown_keys_dropped_not_fatal(caplog):
    r = _arrival()
    r["bogus"] = 1
    with caplog.at_level(logging.WARNING):
        out = load.whitelist("arrival", r)
    assert "bogus" not in out
    assert set(out) <= set(ddl.COLUMNS["arrival"])
    assert sum("bogus" in m for m in caplog.messages) == 1


def test_insert_batches(tmp_path):
    p = tmp_path / "a.jsonl"
    rows.write_rows(p, [_arrival(i) for i in range(120_001)])
    c = FakeClient()
    assert load.insert_file(c, "arrival", p, batch=50_000) == 120_001
    assert [len(x[1]) for x in c.calls] == [50_000, 50_000, 20_001]
    assert c.calls[0][0] == "sigmondb.arrival"
    assert c.calls[0][2] == list(ddl.COLUMNS["arrival"])


def test_second_load_sends_identical_rows(tmp_path):
    p = tmp_path / "a.jsonl"
    rows.write_rows(p, [_arrival(i, engineering={"b": "2", "a": "1"}) for i in range(5)])
    c = FakeClient()
    load.insert_file(c, "arrival", p)
    load.insert_file(c, "arrival", p)
    assert c.calls[0][1] == c.calls[1][1]


def _loaded(tmp_path, table, rows_):
    p = tmp_path / (table + ".jsonl")
    rows.write_rows(p, rows_)
    c = FakeClient()
    load.insert_file(c, table, p)
    return c


def test_none_in_string_columns_goes_as_empty_string(tmp_path):
    tx = valid_row("transmitter", note=None)
    c = _loaded(tmp_path, "transmitter", [tx])
    row = dict(zip(c.calls[0][2], c.calls[0][1][0]))
    assert row["note"] == ""
    assert row["lat"] is None  # Nullable columns keep None
    wf = _loaded(tmp_path, "waveform", [valid_row("waveform", measured_by=None)])
    assert dict(zip(wf.calls[0][2], wf.calls[0][1][0]))["measured_by"] == ""


def test_margin_db_is_nullable(tmp_path):
    assert ddl.is_nullable(ddl.COLUMN_TYPES["arrival"]["margin_db"])
    c = _loaded(tmp_path, "arrival", [_arrival(margin_db=None)])
    assert dict(zip(c.calls[0][2], c.calls[0][1][0]))["margin_db"] is None


def test_none_in_other_non_nullable_raises_before_sending(tmp_path):
    p = tmp_path / "arrival.jsonl"
    good = [_arrival(i) for i in range(3)]
    rows.write_rows(p, good + [_arrival(9, level_db=None), _arrival(10, level_db=None)])
    c = FakeClient()
    with pytest.raises(ValueError) as e:
        load.insert_file(c, "arrival", p, batch=2)
    msg = str(e.value)
    assert "arrival" in msg and "level_db" in msg and "2 rows" in msg
    assert c.calls == []  # nothing sent, even though a full batch preceded the bad rows


def test_unknown_none_string_does_not_hide_strict_check(tmp_path):
    with pytest.raises(ValueError, match="delay_ns"):
        _loaded(tmp_path, "arrival", [_arrival(delay_ns=None)])


def test_insert_rows_checks_everything_before_sending():
    c = FakeClient()
    good = valid_row("attribution")
    bad = dict(good, n_stations=None)
    with pytest.raises(ValueError, match="n_stations"):
        load.insert_rows(c, "attribution", [good, bad])
    assert c.calls == []
    assert load.insert_rows(c, "attribution", [dict(good, extra="x")]) == 1


def test_insert_rows_takes_a_generator():
    c = FakeClient()
    good = valid_row("attribution")
    assert load.insert_rows(c, "attribution", (dict(good) for _ in range(3))) == 3
    assert len(c.calls) == 1 and len(c.calls[0][1]) == 3
