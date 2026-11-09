"""python -m sigmondb verbs.  No ClickHouse: clickhouse_connect is stubbed."""
import os
import sys
import types
from datetime import datetime

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from sigmondb import cli, ddl, rows  # noqa: E402


class FakeClient:
    def __init__(self):
        self.commands, self.inserts = [], []

    def command(self, sql):
        self.commands.append(sql)

    def insert(self, table, data, column_names=None):
        self.inserts.append((table, len(data)))


def _stub(monkeypatch):
    seen = {}
    mod = types.ModuleType("clickhouse_connect")

    def get_client(**kw):
        seen.update(kw)
        seen["client"] = FakeClient()
        return seen["client"]

    mod.get_client = get_client
    monkeypatch.setitem(sys.modules, "clickhouse_connect", mod)
    return seen


def test_cli_reads_credentials_from_conf_only(tmp_path, monkeypatch):
    conf = tmp_path / "clickhouse.conf"
    conf.write_text('# c\nCLICKHOUSE_HOST="db.example"\nCLICKHOUSE_PORT="8123"\n'
                    'CLICKHOUSE_ROOT_ADMIN_USER="admin"\n'
                    'CLICKHOUSE_ROOT_ADMIN_PASSWORD="s3cret pw"\n')
    seen = _stub(monkeypatch)
    d = tmp_path / "out"
    d.mkdir()
    rows.write_rows(d / "arrival.jsonl", [{c: None for c in ddl.COLUMNS["arrival"]}
                                          | dict(time=datetime(2026, 11, 8), delay_ns=1)])
    rows.write_rows(d / "transmitter.jsonl", [])
    assert cli.main(["load", "--dir", str(d), "--conf", str(conf)]) == 0
    assert (seen["host"], seen["port"], seen["username"], seen["password"]) == (
        "db.example", 8123, "admin", "s3cret pw")
    assert ("sigmondb.arrival", 1) in seen["client"].inserts
    # no verb takes a password argument
    for verb in ("load", "solve"):
        for flag in ("--password", "--pw", "--passwd"):
            try:
                cli.main([verb, flag, "x"])
            except SystemExit as e:
                assert e.code == 2
            else:
                raise AssertionError("accepted " + flag)


def test_load_order_follows_tables(tmp_path, monkeypatch):
    conf = tmp_path / "c.conf"
    conf.write_text('CLICKHOUSE_HOST="h"\nCLICKHOUSE_PORT="1"\nCLICKHOUSE_ROOT_ADMIN_USER="u"\n'
                    'CLICKHOUSE_ROOT_ADMIN_PASSWORD="p"\n')
    seen = _stub(monkeypatch)
    for t in ("solution", "arrival", "receiver"):
        rows.write_rows(tmp_path / f"{t}.jsonl", [{c: None for c in ddl.COLUMNS[t]}])
    cli.main(["load", "--dir", str(tmp_path), "--conf", str(conf)])
    assert [t for t, _ in seen["client"].inserts] == [
        "sigmondb.receiver", "sigmondb.arrival", "sigmondb.solution"]


def test_solve_from_files(tmp_path):
    from test_sigmondb_groundwave import _arrivals
    rows.write_rows(tmp_path / "a.jsonl", _arrivals(1_000_000))
    out = tmp_path / "sol.jsonl"
    assert cli.main(["solve", "--arrivals", str(tmp_path / "a.jsonl"), "--out", str(out)]) == 0
    got = rows.read_rows(out)
    assert len(got) == 1 and got[0]["method"] == "ground_wave"


def test_seed_writes_row_files(tmp_path):
    here = os.path.dirname(os.path.abspath(__file__))
    rx = os.path.join(os.path.dirname(here), "sigmondb", "seed", "receivers.toml")
    out = tmp_path / "o"
    assert cli.main(["seed", "--receivers", rx, "--snapshot", "2026-11-09",
                     "--out", str(out)]) == 0
    assert len(rows.read_rows(out / "receiver.jsonl")) > 0
