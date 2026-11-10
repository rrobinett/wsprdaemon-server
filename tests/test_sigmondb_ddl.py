"""sigmondb DDL: tables, SQL mirror, ensure_schema.  No ClickHouse needed."""
import os
import re
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from sigmondb import ddl  # noqa: E402

SQL_FILE = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))),
                        "schema", "sigmondb.sql")


def _norm(s):
    return re.sub(r"\s+", " ", s).strip()


def test_every_table_is_created_in_order():
    stmts = ddl.ddl_statements()
    assert stmts[0].startswith("CREATE DATABASE IF NOT EXISTS sigmondb")
    names = []
    for s in stmts[1:]:
        m = re.match(r"CREATE (?:TABLE|MATERIALIZED VIEW) IF NOT EXISTS sigmondb\.(\w+)", s)
        assert m, s[:80]
        names.append(m.group(1))
    assert names == list(ddl.TABLES)
    assert names[-1] == "heard"
    for t in ("transmitter", "waveform", "receiver", "capture", "arrival", "solution", "path"):
        assert t in names


def test_dedup_keys_match_the_spec():
    arr = _norm(ddl.TABLES["arrival"])
    cap = _norm(ddl.TABLES["capture"])
    assert ("ORDER BY (reporter_id, centre_hz, rate_hz_per_s, time, detector_version, "
            "intDiv(delay_ns, 1000))") in arr
    assert "ORDER BY (reporter_id, centre_hz, rate_hz_per_s, time, detector_version)" in cap


def test_sql_file_mirrors_ddl():
    with open(SQL_FILE) as f:
        text = re.sub(r"--[^\n]*", "", f.read())
    file_stmts = [_norm(p) for p in text.split(";") if p.strip()]
    assert file_stmts == [_norm(s) for s in ddl.ddl_statements() + list(ddl.MIGRATIONS)]


def test_ensure_schema_runs_each_statement_once():
    class Fake:
        def __init__(self):
            self.calls = []

        def command(self, sql):
            self.calls.append(sql)

    c = Fake()
    ddl.ensure_schema(c)
    expect = ddl.ddl_statements() + list(ddl.MIGRATIONS)
    assert len(c.calls) == len(expect)
    assert c.calls == expect


def test_columns_whitelist_names_real_columns():
    assert set(ddl.COLUMNS) <= set(ddl.TABLES)
    for t, cols in ddl.COLUMNS.items():
        assert "ingested_at" not in cols
        for c in cols:
            assert re.search(r"`%s`" % re.escape(c), ddl.TABLES[t]), (t, c)


def test_provenance_block_on_capture_and_arrival():
    for t in ("capture", "arrival"):
        for c in ("u_epoch_ns", "k", "p", "a_level", "a_level_provenance",
                  "counter_epoch_id", "origin", "engineering"):
            assert c in ddl.COLUMNS[t]
        assert "`u_epoch_ns` Nullable(Float64)" in ddl.TABLES[t]
        assert "`engineering` Map(String, String)" in ddl.TABLES[t]


def test_no_nullable_in_order_by():
    for t, sql in ddl.TABLES.items():
        m = re.search(r"ORDER BY \(([^)]*(?:\([^)]*\)[^)]*)*)\)", sql)
        if not m:
            continue
        for col in re.findall(r"\w+", m.group(1)):
            assert not re.search(r"`%s`\s+Nullable" % col, sql), (t, col)
