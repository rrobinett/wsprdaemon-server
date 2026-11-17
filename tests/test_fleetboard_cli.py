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
    def __init__(self, events, alarm_rows=(), map_rows=None):
        self.events, self.sql, self.alarm_rows = events, [], list(alarm_rows)
        self.map_rows = [("A", "EN16ov", "ft8", "A1", [], [])] if map_rows is None else map_rows
        self.inserts, self.commands, self.params, self.ack_rows = [], [], [], []
        self.fail_insert = False

    def command(self, sql):
        self.commands.append(sql)

    def query(self, sql, **kw):
        self.sql.append(sql)
        self.params.append(kw.get("parameters"))
        rows = []
        if sql.startswith("SELECT first_seen FROM fleet.alarm"):
            rows = self.ack_rows
        elif "station_map" in sql:
            rows = self.map_rows
        elif "fleet.alarm FINAL" in sql:
            rows = self.alarm_rows
        return _types.SimpleNamespace(result_rows=rows)

    def insert(self, table, data, column_names=None):
        if self.fail_insert:
            raise RuntimeError("Code: 241. MEMORY_LIMIT_EXCEEDED")
        self.inserts.append((table, data, column_names))
        self.events.append("insert " + table)


def _silent():
    """Activity for station A: ft8 history, last arrival five hours ago -> off_air."""
    now = _now()
    return Activity(last_arrival={("ft8", "A1"): now - timedelta(hours=5)},
                    days_active={("ft8", "A1"): frozenset({now.date()})})


def _stub(monkeypatch, activity=None, alarm_rows=(), map_rows=None):
    events = []
    client = FakeClient(events, alarm_rows, map_rows)
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

    def spy(stations, acts, now_, hours, **kw):
        seen.update(hours)
        return real(stations, acts, now_, hours, **kw)

    monkeypatch.setattr(cli, "evaluate", spy)
    assert cli.main(["run", "--conf", "x"]) == 0
    alarms = {t: r for t, r, _, _ in published}["fleet_alarms"]
    assert sorted((r["check"], r["subject"]) for r in alarms) == [
        ("client_quiet", "ft8"), ("off_air", "")]
    hour = (now - timedelta(hours=40)).replace(minute=0, second=0)
    assert hour in seen["A"]


# --- C1: datetimes reach the insert as aware UTC ---------------------------

import calendar  # noqa: E402
import contextlib  # noqa: E402
import os  # noqa: E402
import time  # noqa: E402


@contextlib.contextmanager
def _tz(name):
    """Run with the process in time zone `name` (wd30 runs America/Chicago)."""
    old = os.environ.get("TZ")
    os.environ["TZ"] = name
    time.tzset()
    try:
        yield
    finally:
        if old is None:
            os.environ.pop("TZ", None)
        else:
            os.environ["TZ"] = old
        time.tzset()


def _epoch(naive_utc):
    return calendar.timegm(naive_utc.timetuple())


def test_alarm_datetimes_insert_as_utc_in_a_chicago_process(monkeypatch):
    client, _, _ = _stub(monkeypatch, _silent())
    _env(monkeypatch, False)
    with _tz("America/Chicago"):
        before = _epoch(_now())
        assert cli.main(["run", "--conf", "x", "--no-publish"]) == 0
        after = _epoch(_now())
        (table, data, cols), = client.inserts
        row = dict(zip(cols, data[0]))
        for k in ("first_seen", "last_seen", "updated_at"):
            assert before <= int(row[k].timestamp()) <= after, k


def test_load_map_loaded_at_inserts_as_utc_in_a_chicago_process(monkeypatch):
    client, _, _ = _stub(monkeypatch)
    line = json.dumps({"name": "A", "grid6": "EN16ov", "source": "ft8", "reporter": "A1",
                       "is_alias": 0, "expect_add": [], "expect_drop": []})
    monkeypatch.setattr(sys, "stdin", __import__("io").StringIO(line + "\n"))
    with _tz("America/Chicago"):
        before = _epoch(_now())
        assert cli.main(["load-map", "--conf", "x"]) == 0
        after = _epoch(_now())
        (table, data, cols), = client.inserts
        v = dict(zip(cols, data[0]))["loaded_at"]
        assert before <= int(v.timestamp()) <= after


# --- I2: an empty station map is not a healthy fleet -----------------------

def test_run_with_empty_map_exits_2_writes_and_publishes_nothing(monkeypatch, capsys):
    now = _now()
    live = ("A", "off_air", "", now - timedelta(hours=2), now - timedelta(minutes=5),
            "open", 0, "{}", now - timedelta(minutes=5))
    client, published, _ = _stub(monkeypatch, _silent(), [live], map_rows=[])
    _env(monkeypatch, True)
    assert cli.main(["run", "--conf", "x"]) == 2
    assert "station map" in capsys.readouterr().err
    assert not client.inserts and not published


# --- I4: an ingest stall freezes the silent stations' alarms ---------------

def test_run_holds_alarms_of_stations_silent_under_an_ingest_stall(monkeypatch):
    now = _now()
    names = ["S%d" % i for i in range(5)]
    maprows = [(n, "EN16ov", "wspr_noise", n, [], []) for n in names]
    act = Activity(
        last_arrival={("wspr_noise", n): now - timedelta(hours=2 if i < 3 else 0, minutes=1)
                      for i, n in enumerate(names)},
        days_active={("wspr_noise", n): frozenset({now.date()}) for n in names})
    first = now - timedelta(days=1)
    acked = ("S0", "off_air", "", first, now - timedelta(minutes=5), "acknowledged", 0, "{}",
             now - timedelta(minutes=5))
    client, published, _ = _stub(monkeypatch, act, [acked], map_rows=maprows)
    _env(monkeypatch, True)
    assert cli.main(["run", "--conf", "x"]) == 0
    (table, data, cols), = client.inserts
    written = [dict(zip(cols, r)) for r in data]
    assert [(w["station"], w["check"]) for w in written] == [("wd30", "ingest_stalled")]
    alarms = {t: r for t, r, _, _ in published}["fleet_alarms"]
    s0 = [a for a in alarms if a["station"] == "S0"]
    assert [(a["check"], a["state"]) for a in s0] == [("off_air", "acknowledged")]


# --- I6: live alarms reach the checks --------------------------------------

def test_live_off_air_keeps_a_station_with_aged_out_history_off(monkeypatch):
    now = _now()
    live = ("A", "off_air", "", now - timedelta(days=40), now - timedelta(minutes=5),
            "acknowledged", 0, "{}", now - timedelta(minutes=5))
    client, published, _ = _stub(monkeypatch, Activity(), [live])
    _env(monkeypatch, True)
    assert cli.main(["run", "--conf", "x"]) == 0
    (table, data, cols), = client.inserts
    row = dict(zip(cols, data[0]))
    assert (row["check"], row["state"], row["false_streak"]) == ("off_air", "acknowledged", 0)
    board = {t: r for t, r, _, _ in published}["fleet_board"]
    assert board[0]["verdict"] == "off"


# --- I1: ack and load-map run on wd30 with the admin conf ------------------

def _ack_argv(*extra):
    return ["ack", "--conf", "x", "--station", "UACNJ", "--check", "off_air",
            "--subject", "", "--note", "site visit Fri", "--by", "mjh", *extra]


def test_ack_inserts_one_row_naming_the_live_first_seen(monkeypatch, capsys):
    client, _, _ = _stub(monkeypatch)
    first = datetime(2026, 11, 16, 10, 0, 0)
    client.ack_rows = [(first,)]
    with _tz("America/Chicago"):
        before = _epoch(_now())
        assert cli.main(_ack_argv()) == 0
        after = _epoch(_now())
        (table, data, cols), = client.inserts
        assert table == "fleet.alarm_ack" and cols == cli.ACK_COLS
        row = dict(zip(cols, data[0]))
        assert int(row["first_seen"].timestamp()) == _epoch(first)
        assert before <= int(row["acked_at"].timestamp()) <= after
    assert (row["station"], row["check"], row["subject"]) == ("UACNJ", "off_air", "")
    assert (row["acked_by"], row["note"]) == ("mjh", "site visit Fri")
    q = next(q for q in client.sql if q.startswith("SELECT first_seen"))
    assert "FINAL" in q and "state IN ('open', 'acknowledged')" in q
    assert client.params[-1] == {"station": "UACNJ", "check": "off_air", "subject": ""}
    assert "acknowledged UACNJ off_air" in capsys.readouterr().out


def test_ack_without_live_alarm_exits_1_and_writes_nothing(monkeypatch, capsys):
    client, _, _ = _stub(monkeypatch)
    assert cli.main(_ack_argv()) == 1
    assert "no live alarm" in capsys.readouterr().err
    assert not client.inserts


def _map_line(**over):
    r = {"name": "A", "grid6": "EN16ov", "source": "ft8", "reporter": "A1",
         "is_alias": 0, "expect_add": [], "expect_drop": []}
    r.update(over)
    return json.dumps(r) + "\n"


def _load(monkeypatch, client, text):
    monkeypatch.setattr(sys, "stdin", __import__("io").StringIO(text))
    return cli.main(["load-map", "--conf", "x"])


def test_load_map_refuses_bad_input_before_truncate(monkeypatch, capsys):
    bad = ["", "not json\n", _map_line(source="wspr"), _map_line(name=""),
           _map_line(reporter=""), _map_line(host="kph1"), _map_line(grid6="EN16ov12"),
           _map_line(expect_add=["nope"]), _map_line(is_alias=2), '["A"]\n',
           _map_line() + _map_line(source="bogus")]
    for text in bad:
        client, _, _ = _stub(monkeypatch)
        assert _load(monkeypatch, client, text) == 1, text
        assert not any("TRUNCATE" in c for c in client.commands), text
        assert not client.inserts, text
    assert "load-map" in capsys.readouterr().err


def test_load_map_says_loudly_when_insert_fails_after_truncate(monkeypatch, capsys):
    client, _, _ = _stub(monkeypatch)
    client.fail_insert = True
    assert _load(monkeypatch, client, _map_line()) == 1
    assert any("TRUNCATE" in c for c in client.commands)
    err = capsys.readouterr().err
    assert "EMPTY" in err and "MEMORY_LIMIT_EXCEEDED" in err


def test_load_map_good_input_truncates_then_inserts(monkeypatch):
    client, _, _ = _stub(monkeypatch)
    assert _load(monkeypatch, client, _map_line() + "\n" + _map_line(source="codar", is_alias=1)) == 0
    assert client.commands[-1] == "TRUNCATE TABLE fleet.station_map"
    (table, data, cols), = client.inserts
    assert [r[:5] for r in data] == [["A", "EN16ov", "ft8", "A1", 0], ["A", "EN16ov", "codar", "A1", 1]]
