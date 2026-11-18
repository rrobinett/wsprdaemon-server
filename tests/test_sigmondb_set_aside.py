"""sigmondb set-aside windows: the shared SQL condition, its readers, the CLI verb.
No ClickHouse: the fragment's semantics are checked by a small Python rendering of its lambda."""
import os
import re
import sys
from datetime import datetime, timezone

import pytest

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from sigmondb import cli, ddl, publish  # noqa: E402
from sigmondb import set_aside as sa  # noqa: E402

T = datetime


# --- the fragment -------------------------------------------------------------------------

def _norm(s):
    return re.sub(r"\s+", " ", s).strip()


@pytest.mark.parametrize("scope", ["codar", "hfdl"])
def test_fragment_text(scope):
    f = sa.not_set_aside(scope)
    assert f.startswith("NOT arrayExists(w -> ")
    assert "w.1 = reporter_id AND time >= w.2 AND time < w.3" in f          # half-open
    assert "FROM sigmondb.set_aside FINAL" in f
    assert "WHERE active = 1 AND scope IN ('%s', 'all')" % scope in f
    assert "groupArray((reporter_id, from_time, to_time))" in f


def test_fragment_takes_other_columns_and_refuses_bad_ones():
    f = sa.not_set_aside("codar", reporter="rid", time="t0")
    assert "w.1 = rid AND t0 >= w.2 AND t0 < w.3" in f
    for bad in ("time; DROP", "x)", "", "1a"):
        with pytest.raises(ValueError):
            sa.not_set_aside("codar", time=bad)


@pytest.mark.parametrize("scope", ["all", "", "CODAR", "wspr"])
def test_reader_scope_is_codar_or_hfdl(scope):
    with pytest.raises(ValueError):
        sa.not_set_aside(scope)
    with pytest.raises(ValueError):
        sa.solution_not_set_aside(scope)


def test_solution_fragment_text():
    f = sa.solution_not_set_aside("codar")
    assert "has(reporter_ids, w.1) AND w.2 <= window_to AND w.3 > window_from" in f
    assert "FROM sigmondb.set_aside FINAL WHERE active = 1 AND scope IN ('codar', 'all')" in f


def _as_python(fragment):
    """The lambda of a fragment as a Python predicate (w, row) -> bool."""
    body = re.match(r"NOT arrayExists\(w -> (.*), \(SELECT", fragment).group(1)
    body = re.sub(r"has\((\w+), w\.1\)", r"(w[0] in row['\1'])", body)
    body = re.sub(r"w\.(\d)", lambda m: "w[%d]" % (int(m.group(1)) - 1), body)
    body = re.sub(r"\b(reporter_id|time|window_from|window_to)\b", r"row['\1']", body)
    body = body.replace(" AND ", " and ").replace(" = ", " == ")
    return eval("lambda w, row: " + body)   # noqa: S307  (our own fragment)


def _kept(fragment, windows, row):
    pred = _as_python(fragment)
    return not any(pred(w, row) for w in windows)


W = ("AB4EJ", T(2026, 11, 12), T(2026, 11, 18, 2, 9))


def test_arrival_window_is_half_open():
    f = sa.not_set_aside("codar")
    row = lambda rid, t: dict(reporter_id=rid, time=t)  # noqa: E731
    assert _kept(f, [W], row("AB4EJ", T(2026, 11, 11, 23, 59, 59)))
    assert not _kept(f, [W], row("AB4EJ", T(2026, 11, 12)))             # from: inside
    assert not _kept(f, [W], row("AB4EJ", T(2026, 11, 18, 2, 8, 59)))
    assert _kept(f, [W], row("AB4EJ", T(2026, 11, 18, 2, 9)))           # to: outside
    assert _kept(f, [W], row("W2NAF", T(2026, 11, 13)))                 # other reporter
    assert _kept(f, [], row("AB4EJ", T(2026, 11, 13)))


def test_solution_overlap_semantics():
    f = sa.solution_not_set_aside("codar")
    sol = lambda rids, a, b: dict(reporter_ids=rids, window_from=a, window_to=b)  # noqa: E731
    w = ("K3LR/D", T(2026, 11, 12), T(2026, 11, 13))
    # any reporter of the solution counts
    assert not _kept(f, [w], sol(["W2NAF", "K3LR/D"], T(2026, 11, 12, 6), T(2026, 11, 12, 7)))
    assert _kept(f, [w], sol(["W2NAF"], T(2026, 11, 12, 6), T(2026, 11, 12, 7)))
    # overlap at either side, and a solution spanning the whole window
    assert not _kept(f, [w], sol(["K3LR/D"], T(2026, 11, 11), T(2026, 11, 12, 0, 0, 1)))
    assert not _kept(f, [w], sol(["K3LR/D"], T(2026, 11, 12, 23), T(2026, 11, 14)))
    assert not _kept(f, [w], sol(["K3LR/D"], T(2026, 11, 1), T(2026, 11, 30)))
    # the solution's closed end touching the window's start: overlaps
    assert not _kept(f, [w], sol(["K3LR/D"], T(2026, 11, 11), T(2026, 11, 12)))
    # the window's open end touching the solution's start: no overlap
    assert _kept(f, [w], sol(["K3LR/D"], T(2026, 11, 13), T(2026, 11, 14)))
    assert _kept(f, [w], sol(["K3LR/D"], T(2026, 11, 10), T(2026, 11, 11)))


# --- the schema -----------------------------------------------------------------------------

def test_set_aside_table_ddl():
    s = _norm(ddl.TABLES["set_aside"])
    for col in ("`scope` LowCardinality(String)", "`reporter_id` String",
                "`from_time` DateTime64(3)", "`to_time` DateTime64(3)", "`reason` String",
                "`set_by` String", "`set_at` DateTime", "`active` UInt8 DEFAULT 1"):
        assert col in s
    assert "ENGINE = ReplacingMergeTree(set_at)" in s
    assert "ORDER BY (scope, reporter_id, from_time)" in s
    assert "ingested_at" not in s
    assert ddl.COLUMNS["set_aside"] == ("scope", "reporter_id", "from_time", "to_time",
                                        "reason", "set_by", "set_at", "active")
    assert "set_aside" not in ddl.TABLE_ORDER       # load never bulk-loads it


def test_ensure_schema_creates_set_aside():
    class C:
        calls = []

        def command(self, sql):
            self.calls.append(sql)

    c = C()
    ddl.ensure_schema(c)
    assert any(x.startswith("CREATE TABLE IF NOT EXISTS sigmondb.set_aside") for x in c.calls)


# --- the readers ----------------------------------------------------------------------------

from test_sigmondb_cli import _arr, _db, _ns  # noqa: E402

CODAR = sa.not_set_aside("codar")


def _arrival_sql(client):
    return [k["sql"] for k in client.calls if " FROM sigmondb.arrival FINAL" in k["sql"]]


def test_from_db_reads_through_the_fragment(monkeypatch):
    c = _db(monkeypatch, {"arrival": [_arr("K3LR/D", "g", "v")]})
    cli._from_db(_ns())
    [sql] = _arrival_sql(c)
    assert CODAR in sql
    c = _db(monkeypatch, {"arrival": [_arr("K3LR/D", "g", "v")]})
    cli._from_db(_ns("--detector-version", "v"))
    [sql] = _arrival_sql(c)
    assert CODAR in sql


def test_attribution_reads_through_the_fragment(monkeypatch):
    c = _db(monkeypatch, {})
    cli._attribution_inputs(c, 1.0)
    [sql] = _arrival_sql(c)
    assert CODAR in sql


def test_publish_reads_through_the_fragment():
    for name, (cols, sql) in publish.AGG_QUERIES.items():
        assert CODAR in sql, name
    assert sa.solution_not_set_aside("codar") in publish.SOURCE_WHERE["solution"]

    from test_sigmondb_publish import FakeSource
    src = FakeSource(solution=[], transmitter=[], receiver=[], waveform=[])
    publish.read_source(src, out=lambda s: None)
    sol = [q for q in src.sql if "FROM sigmondb.solution FINAL" in q]
    assert sol and sa.solution_not_set_aside("codar") in sol[0]


def test_a_missing_set_aside_table_fails_loudly(monkeypatch):
    """No set_aside table: the read raises, never returns rows it could not check."""
    class NoTable:
        def query(self, sql, parameters=None, **kw):
            if "sigmondb.set_aside" in sql:
                raise RuntimeError("Code: 60. Table sigmondb.set_aside does not exist")
            from test_sigmondb_cli import _Result
            return _Result([])

    monkeypatch.setattr(cli, "connect", lambda conf: NoTable())
    with pytest.raises(RuntimeError, match="set_aside"):
        cli._from_db(_ns())
    with pytest.raises(RuntimeError, match="set_aside"):
        cli._attribution_inputs(NoTable(), 1.0)
    with pytest.raises(RuntimeError, match="set_aside"):
        publish.read_source(NoTable(), out=lambda s: None)


# --- the CLI verb ---------------------------------------------------------------------------

class _Res:
    def __init__(self, rows):
        self.result_rows = rows


class SAClient:
    """set_aside rows in memory; count() queries answer `counts` in order."""

    def __init__(self, rows=(), counts=()):
        self.rows, self.counts = list(rows), list(counts)
        self.queries, self.commands, self.inserted = [], [], []

    def query(self, sql, parameters=None, **kw):
        self.queries.append((sql, parameters))
        if sql.startswith("SELECT count()"):
            return _Res([(self.counts.pop(0),)])
        cols = ddl.COLUMNS["set_aside"]
        rows = self.rows
        if parameters and "rid" in parameters:
            rows = [r for r in rows if r["scope"] == parameters["scope"]
                    and r["reporter_id"] == parameters["rid"]
                    and sa.ch_time(r["from_time"]) == parameters["from_time"]]
        if "active = 1" in sql:
            rows = [r for r in rows if r["active"]]
        return _Res([tuple(r[c] for c in cols) for r in rows])

    def command(self, sql):
        self.commands.append(sql)

    def insert(self, table, data, column_names=None):
        self.inserted += [(table, dict(zip(column_names, row))) for row in data]


def _client(monkeypatch, client):
    monkeypatch.setattr(cli, "connect", lambda conf: client)
    return client


ADD = ["set-aside", "add", "--scope", "codar", "--reporter", "AB4EJ",
       "--from", "2026-11-12T00:00:00Z", "--to", "2026-11-18T02:09:00Z",
       "--reason", "codar-sounder chunk drop (7a06713)", "--by", "mjh"]


def _with(args, flag, value):
    a = list(args)
    a[a.index(flag) + 1] = value
    return a


@pytest.mark.parametrize("flag,value", [
    ("--to", "2026-11-12T00:00:00Z"),              # to == from
    ("--to", "2026-11-11T00:00:00Z"),              # to < from
    ("--scope", "wspr"),
    ("--scope", ""),
    ("--reason", "   "),
    ("--reason", ""),
    ("--reporter", ""),
    ("--from", "yesterday"),
])
def test_add_refuses(monkeypatch, capsys, flag, value):
    def no_connect(conf):
        raise AssertionError("connected for a refused row")

    monkeypatch.setattr(cli, "connect", no_connect)
    assert cli.main(_with(ADD, flag, value)) == 2
    assert "set-aside add:" in capsys.readouterr().err


def test_add_writes_one_row_after_ensure_schema(monkeypatch):
    c = _client(monkeypatch, SAClient())
    assert cli.main(ADD) == 0
    assert any("sigmondb.set_aside" in s for s in c.commands)
    [(table, row)] = c.inserted
    assert table == "sigmondb.set_aside"
    assert row["scope"] == "codar" and row["reporter_id"] == "AB4EJ" and row["active"] == 1
    assert row["from_time"] == T(2026, 11, 12, tzinfo=timezone.utc)
    assert row["to_time"] == T(2026, 11, 18, 2, 9, tzinfo=timezone.utc)
    assert row["set_by"] == "mjh" and row["reason"].startswith("codar-sounder")


def test_add_converts_a_zoned_time_to_utc(monkeypatch):
    c = _client(monkeypatch, SAClient())
    assert cli.main(_with(ADD, "--from", "2026-11-11T18:00:00-06:00")) == 0
    assert c.inserted[0][1]["from_time"] == T(2026, 11, 12, tzinfo=timezone.utc)


def test_add_dry_run_counts_and_writes_nothing(monkeypatch, capsys):
    c = _client(monkeypatch, SAClient(counts=[3475, 16]))
    assert cli.main(ADD + ["--dry-run"]) == 0
    out = capsys.readouterr().out
    assert "codar AB4EJ [2026-11-12 00:00:00, 2026-11-18 02:09:00)" in out
    assert "3475 arrival rows and 16 solution rows" in out
    assert c.inserted == [] and c.commands == []
    (asql, ap), (ssql, sp) = c.queries
    assert "FROM sigmondb.arrival FINAL" in asql and "rate_hz_per_s != 0" in asql
    assert "FROM sigmondb.solution FINAL" in ssql
    assert ap == {"rid": "AB4EJ", "f": "2026-11-12 00:00:00.000", "t": "2026-11-18 02:09:00.000"}


def test_add_dry_run_hfdl_counts_no_solutions(monkeypatch, capsys):
    c = _client(monkeypatch, SAClient(counts=[7]))
    assert cli.main(_with(ADD, "--scope", "hfdl") + ["--dry-run"]) == 0
    assert "7 arrival rows and no solution rows" in capsys.readouterr().out
    [(asql, _)] = c.queries
    assert "rate_hz_per_s = 0" in asql


def _row(active=1, set_at=T(2026, 11, 18, 3)):
    return dict(scope="codar", reporter_id="AB4EJ", from_time=T(2026, 11, 12),
                to_time=T(2026, 11, 18, 2, 9), reason="r", set_by="mjh", set_at=set_at,
                active=active)


def test_list_shows_active_unless_all(monkeypatch, capsys):
    c = _client(monkeypatch, SAClient([_row(), dict(_row(active=0), reporter_id="W2NAF")]))
    assert cli.main(["set-aside", "list"]) == 0
    out = capsys.readouterr().out
    assert "AB4EJ" in out and "W2NAF" not in out
    assert "FROM sigmondb.set_aside FINAL WHERE active = 1" in c.queries[0][0]
    assert cli.main(["set-aside", "list", "--all"]) == 0
    out = capsys.readouterr().out
    assert "AB4EJ" in out and "W2NAF" in out


WD = ["set-aside", "withdraw", "--scope", "codar", "--reporter", "AB4EJ",
      "--from", "2026-11-12T00:00:00Z", "--by", "rob"]


def test_withdraw_inserts_the_same_key_inactive_and_later(monkeypatch):
    c = _client(monkeypatch, SAClient([_row(set_at=T(2026, 11, 18, 3))]))
    assert cli.main(WD) == 0
    [(table, row)] = c.inserted
    assert table == "sigmondb.set_aside"
    assert (row["scope"], row["reporter_id"], row["from_time"]) == (
        "codar", "AB4EJ", T(2026, 11, 12, tzinfo=timezone.utc))
    assert row["to_time"] == T(2026, 11, 18, 2, 9, tzinfo=timezone.utc)
    assert row["active"] == 0 and row["set_by"] == "rob"
    assert row["set_at"] > T(2026, 11, 18, 3, tzinfo=timezone.utc)
    assert "from_time = toDateTime64({from_time:String}, 3, 'UTC')" in c.queries[0][0]


def test_withdrawal_set_at_always_follows():
    """ReplacingMergeTree(set_at) keeps the largest set_at: a withdrawal in the same second
    as the add must still win."""
    cur = _row(set_at=T(2026, 11, 18, 3, 0, 0))
    assert sa.withdrawal_row(cur, "rob", T(2026, 11, 18, 3, 0, 0))["set_at"] == \
        T(2026, 11, 18, 3, 0, 1)
    assert sa.withdrawal_row(cur, "rob", T(2026, 11, 18, 2))["set_at"] == T(2026, 11, 18, 3, 0, 1)
    assert sa.withdrawal_row(cur, "rob", T(2026, 11, 18, 4))["set_at"] == T(2026, 11, 18, 4)


def test_latest_set_at_wins():
    """What FINAL does with ReplacingMergeTree(set_at): per key, the largest set_at."""
    add = _row(set_at=T(2026, 11, 18, 3))
    wd = sa.withdrawal_row(add, "rob", T(2026, 11, 18, 4))
    readd = dict(add, set_at=T(2026, 11, 18, 5), active=1)

    def final(rows):
        best = {}
        for r in rows:
            k = (r["scope"], r["reporter_id"], r["from_time"])
            if k not in best or r["set_at"] >= best[k]["set_at"]:
                best[k] = r
        return list(best.values())

    assert [r["active"] for r in final([add, wd])] == [0]
    assert [r["active"] for r in final([wd, add])] == [0]          # merge order does not matter
    assert [r["active"] for r in final([add, wd, readd])] == [1]


def test_withdraw_refuses_unknown_and_withdrawn(monkeypatch, capsys):
    c = _client(monkeypatch, SAClient([]))
    assert cli.main(WD) == 1 and c.inserted == []
    assert "no window" in capsys.readouterr().err
    c = _client(monkeypatch, SAClient([_row(active=0)]))
    assert cli.main(WD) == 1 and c.inserted == []
    assert "already withdrawn" in capsys.readouterr().err


def test_parse_utc():
    assert sa.parse_utc("2026-11-18T02:09:00Z") == T(2026, 11, 18, 2, 9)
    assert sa.parse_utc("2026-11-18 02:09") == T(2026, 11, 18, 2, 9)
    assert sa.parse_utc("2026-11-17T20:09:00-06:00") == T(2026, 11, 18, 2, 9)
    assert sa.parse_utc("2026-11-18T02:09:00.123456Z") == T(2026, 11, 18, 2, 9, 0, 123000)
    assert sa.ch_time(T(2026, 11, 18, 2, 9, 0, 123000)) == "2026-11-18 02:09:00.123"
