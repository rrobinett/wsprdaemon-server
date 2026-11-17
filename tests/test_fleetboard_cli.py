"""python -m fleetboard run: stubbed connect, fetch and publish; no database."""
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

from fleetboard import cli  # noqa: E402
from fleetboard.sources import Activity  # noqa: E402
from sigmondb import publish  # noqa: E402


def _now():
    return datetime.now(timezone.utc).replace(tzinfo=None, microsecond=0)


class FakeClient:
    def __init__(self, events, alarm_rows=()):
        self.events, self.sql, self.alarm_rows = events, [], list(alarm_rows)
        self.inserts = []

    def command(self, sql):
        pass

    def query(self, sql, **kw):
        self.sql.append(sql)
        rows = []
        if "station_map" in sql:
            rows = [("A", "EN16ov", "ft8", "A1", [], [])]
        elif "fleet.alarm FINAL" in sql:
            rows = self.alarm_rows
        return _types.SimpleNamespace(result_rows=rows)

    def insert(self, table, data, column_names=None):
        self.inserts.append((table, data, column_names))
        self.events.append("insert " + table)


def _silent():
    """Activity for station A: ft8 history, last arrival five hours ago -> off_air."""
    now = _now()
    return Activity(last_arrival={("ft8", "A1"): now - timedelta(hours=5)},
                    days_active={("ft8", "A1"): frozenset({now.date()})})


def _stub(monkeypatch, activity=None, alarm_rows=()):
    events = []
    client = FakeClient(events, alarm_rows)
    published = []
    monkeypatch.setattr(cli, "connect", lambda conf: client)
    monkeypatch.setattr(cli.sources, "fetch",
                        lambda c, now: activity if activity is not None else Activity())

    def replace_table(ch, table, rows, schema=None, database=None):
        events.append("publish " + table)
        published.append((table, rows, schema, database))
        return len(rows)

    monkeypatch.setattr(publish, "replace_table", replace_table)
    return client, published, events


def _env(monkeypatch, creds):
    for k in ("CH_PUB_USER", "CH_PUB_PASSWORD", "CH_PUB_HOSTS"):
        monkeypatch.delenv(k, raising=False)
    if creds:
        monkeypatch.setenv("CH_PUB_USER", "u")
        monkeypatch.setenv("CH_PUB_PASSWORD", "p")
        monkeypatch.setenv("CH_PUB_HOSTS", "http://h1:8123")


def test_run_without_credentials_exits_2(monkeypatch, capsys):
    client, published, _ = _stub(monkeypatch, _silent())
    _env(monkeypatch, False)
    assert cli.main(["run", "--conf", "x"]) == 2
    assert "CH_PUB_USER" in capsys.readouterr().err
    assert not published and not client.inserts


def test_run_no_publish_writes_alarms_only(monkeypatch):
    client, published, _ = _stub(monkeypatch, _silent())
    _env(monkeypatch, False)
    assert cli.main(["run", "--conf", "x", "--no-publish"]) == 0
    assert not published
    assert len(client.inserts) == 1
    table, data, cols = client.inserts[0]
    assert table == "fleet.alarm" and cols == cli.ALARM_COLS
    assert len(data) == 1 and data[0][:3] == ["A", "off_air", ""] and data[0][5] == "open"


def test_run_inserts_alarms_before_publishing(monkeypatch):
    client, published, events = _stub(monkeypatch, _silent())
    _env(monkeypatch, True)
    assert cli.main(["run", "--conf", "x"]) == 0
    assert events == ["insert fleet.alarm", "publish fleet_board", "publish fleet_alarms"]
    assert [p[2] is not None and p[3] for p in published] == ["fleet", "fleet"]
    rows = dict((t, r) for t, r, _, _ in published)
    assert rows["fleet_board"][0]["verdict"] == "off"
    assert [r["check"] for r in rows["fleet_alarms"]] == ["off_air"]


def test_failed_publish_exits_1_with_scrubbed_message_and_alarm_kept(monkeypatch, capsys):
    client, published, events = _stub(monkeypatch, _silent())
    _env(monkeypatch, True)
    monkeypatch.setenv("CH_PUB_PASSWORD", "sekret")

    def boom(ch, table, rows, schema=None, database=None):
        raise publish.PublishError(ch.scrub("HTTP 500 bad password sekret"))

    monkeypatch.setattr(publish, "replace_table", boom)
    assert cli.main(["run", "--conf", "x"]) == 1
    cap = capsys.readouterr()
    assert "FAILED" in cap.out and "sekret" not in cap.out + cap.err
    assert events == ["insert fleet.alarm"]


def test_run_reads_alarms_and_acks_with_their_windows(monkeypatch):
    client, _, _ = _stub(monkeypatch)
    _env(monkeypatch, False)
    assert cli.main(["run", "--conf", "x", "--no-publish"]) == 0
    alarm_q = next(q for q in client.sql if "fleet.alarm FINAL" in q)
    assert "state != 'cleared'" in alarm_q
    assert "last_seen > toDateTime(" in alarm_q
    ack_q = next(q for q in client.sql if "fleet.alarm_ack" in q)
    assert "acked_at > toDateTime(" in ack_q
    import re
    lit = lambda q: datetime.strptime(re.search(r"toDateTime\('([^']+)'", q).group(1),  # noqa: E731
                                      "%Y-%m-%d %H:%M:%S")
    assert abs((_now() - lit(alarm_q)) - timedelta(days=8)) < timedelta(minutes=1)
    assert abs((_now() - lit(ack_q)) - timedelta(days=30)) < timedelta(minutes=1)


def test_live_alarm_published_cleared_alarm_only_feeds_excluded_hours(monkeypatch):
    now = _now()
    live = ("A", "client_quiet", "ft8", now - timedelta(hours=2), now - timedelta(hours=1),
            "open", 0, "{}", now)
    old = ("A", "client_quiet", "codar", now - timedelta(hours=40), now - timedelta(hours=38),
           "cleared", 3, "{}", now)
    client, published, _ = _stub(monkeypatch, _silent(), [live, old])
    _env(monkeypatch, True)
    seen = {}
    real = cli.evaluate

    def spy(stations, acts, now_, hours):
        seen.update(hours)
        return real(stations, acts, now_, hours)

    monkeypatch.setattr(cli, "evaluate", spy)
    assert cli.main(["run", "--conf", "x"]) == 0
    alarms = {t: r for t, r, _, _ in published}["fleet_alarms"]
    assert sorted((r["check"], r["subject"]) for r in alarms) == [
        ("client_quiet", "ft8"), ("off_air", "")]
    hour = (now - timedelta(hours=40)).replace(minute=0, second=0)
    assert hour in seen["A"]
