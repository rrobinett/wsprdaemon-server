"""python -m fleetboard run: stubbed connect, fetch and publish; no database."""
from __future__ import annotations

import sys
import types as _types
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
for _mod in ('clickhouse_connect', 'clickhouse_driver'):
    if _mod not in sys.modules:
        _stub = _types.ModuleType(_mod)
        _stub.Client = object
        _stub.get_client = lambda **kw: None
        sys.modules[_mod] = _stub

from fleetboard import cli  # noqa: E402
from fleetboard.sources import Activity  # noqa: E402


class FakeClient:
    def __init__(self):
        self.commands, self.inserts = [], []

    def command(self, sql):
        self.commands.append(sql)

    def query(self, sql, **kw):
        rows = []
        if "station_map" in sql:
            rows = [("A", "EN16ov", "ft8", "A1", [], [])]
        return _types.SimpleNamespace(result_rows=rows)

    def insert(self, table, data, column_names=None):
        self.inserts.append((table, data, column_names))


def _stub(monkeypatch):
    client = FakeClient()
    published = []
    monkeypatch.setattr(cli, "connect", lambda conf: client)
    monkeypatch.setattr(cli.sources, "fetch", lambda c, now: Activity())
    monkeypatch.setattr(cli, "publish_board", lambda *a, **k: published.append(a) or [])
    return client, published


def _env(monkeypatch, creds):
    for k in ("CH_PUB_USER", "CH_PUB_PASSWORD", "CH_PUB_HOSTS"):
        monkeypatch.delenv(k, raising=False)
    if creds:
        monkeypatch.setenv("CH_PUB_USER", "u")
        monkeypatch.setenv("CH_PUB_PASSWORD", "p")
        monkeypatch.setenv("CH_PUB_HOSTS", "http://h1:8123")


def test_run_without_credentials_exits_2(monkeypatch, capsys):
    client, published = _stub(monkeypatch)
    _env(monkeypatch, False)
    assert cli.main(["run", "--conf", "x"]) == 2
    assert "CH_PUB_USER" in capsys.readouterr().err
    assert not published and not client.inserts


def test_run_no_publish_writes_alarms_only(monkeypatch):
    client, published = _stub(monkeypatch)
    _env(monkeypatch, False)
    # a station with no history is grey: no findings, so no alarm rows either
    assert cli.main(["run", "--conf", "x", "--no-publish"]) == 0
    assert not published
    assert all(t == "fleet.alarm" for t, _, _ in client.inserts)


def test_run_publishes_with_credentials(monkeypatch):
    client, published = _stub(monkeypatch)
    _env(monkeypatch, True)
    assert cli.main(["run", "--conf", "x"]) == 0
    assert len(published) == 1
    tables = published[0][0]
    assert [r["station"] for r in tables["fleet_board"]] == ["A"]
    assert tables["fleet_board"][0]["verdict"] == "grey"
