"""sigmondb row files and loader.  Fake client; no ClickHouse."""
import logging
import os
import sys
from datetime import datetime

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from sigmondb import ddl, load, rows  # noqa: E402


class FakeClient:
    def __init__(self):
        self.calls = []

    def insert(self, table, data, column_names=None):
        self.calls.append((table, [tuple(r) for r in data], list(column_names)))


def _arrival(i=0, **kw):
    r = {c: None for c in ddl.COLUMNS["arrival"]}
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
