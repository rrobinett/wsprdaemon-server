"""A declaration must reach all three servers, including when it CHANGES.

A station declares the identity it merges under once, in routing.json; the
server that ingested the tar records it.  The forwarders run under an
elector, so the server that POSTS is not necessarily the one that
INGESTED — an unreplicated declaration means spots are reported under an
identity the operator did not choose.

The trap this file exists to hold shut: `ch-sync-peers` identifies rows on
a ReplacingMergeTree by cityHash64 of the peer's SORTING KEY, and pulls
only hashes it does not already hold.  Ordered by `key` alone, a site that
changes the callsign it merges under keeps the same key, hashes
identically, and the change never leaves the server it landed on.  That is
the same blind spot that kept a corrected rx_loc from propagating on
2026-10-27 and forced three separate ALTERs.
"""
from __future__ import annotations

import re
import sys
import types as _types
from pathlib import Path

import pytest

HERE = Path(__file__).resolve().parent
ROOT = HERE.parent
sys.path.insert(0, str(ROOT))

for _mod in ('clickhouse_connect', 'clickhouse_driver'):
    if _mod not in sys.modules:
        sys.modules[_mod] = _types.ModuleType(_mod)

import wsprdaemon_server as wds  # noqa: E402

SYNC = (ROOT / 'ch-sync-peers.sh').read_text()
IDENTITY_TABLES = ('psk.reporter', 'wsprdaemon.wsprnet_reporter')


class FakeResult:
    def __init__(self, rows):
        self.result_rows = rows


class FakeClient:
    """Records DDL and answers the layout probe with a scripted table."""

    def __init__(self, sorting_key=None, rows=0):
        self._sorting_key = sorting_key
        self._rows = rows
        self.commands = []

    def query(self, sql):
        if 'system.tables' in sql:
            return FakeResult([] if self._sorting_key is None
                              else [(self._sorting_key,)])
        if 'count()' in sql:
            return FakeResult([(self._rows,)])
        return FakeResult([])

    def command(self, sql):
        self.commands.append(sql)

    @property
    def ddl(self):
        return "\n".join(self.commands)


def _setup_both(client):
    wds.setup_wsprnet_reporter_table(client, {'clickhouse_database': 'wsprdaemon'})
    wds.setup_psk_reporter_table(client, {'clickhouse_psk_database': 'psk'})


# ── the sorting key is the replication contract ──────────────────────────────

def test_BOTH_identity_tables_order_by_the_PAYLOAD_not_just_the_key():
    """`key` alone makes a changed declaration hash identically to the old
    one, so the peers never pull it.  (key, call, grid) makes a change a
    new row."""
    c = FakeClient()
    _setup_both(c)
    orders = re.findall(r'ORDER BY \(([^)]*)\)', c.ddl)
    assert len(orders) == 2, c.ddl
    for o in orders:
        cleaned = o.replace('`', '').replace(' ', '')
        assert cleaned == 'key,call,grid', o


def test_the_tables_stay_REPLACING_so_a_repeated_declaration_collapses():
    """Every flush rewrites the declaration.  Without collapsing, the table
    would grow by a row per receiver per flush, forever."""
    c = FakeClient()
    _setup_both(c)
    assert c.ddl.count('ReplacingMergeTree(updated_at)') == 2


def test_the_sorting_key_constant_and_the_DDL_cannot_drift_apart():
    c = FakeClient()
    _setup_both(c)
    wanted = wds.REPORTER_SORTING_KEY.replace(' ', '')
    for o in re.findall(r'ORDER BY \(([^)]*)\)', c.ddl):
        assert o.replace('`', '').replace(' ', '') == wanted


# ── the layout guard ─────────────────────────────────────────────────────────

def test_an_ABSENT_table_is_simply_created():
    c = FakeClient(sorting_key=None)
    _setup_both(c)
    assert not any(s.strip().startswith('DROP') for s in c.commands)
    assert c.ddl.count('CREATE TABLE IF NOT EXISTS') == 2


def test_a_table_ALREADY_on_the_right_key_is_left_alone():
    c = FakeClient(sorting_key='key, call, grid', rows=5000)
    _setup_both(c)
    assert not any(s.strip().startswith('DROP') for s in c.commands)


def test_an_EMPTY_table_on_the_OLD_key_is_rebuilt():
    c = FakeClient(sorting_key='key', rows=0)
    _setup_both(c)
    drops = [s for s in c.commands if s.strip().startswith('DROP')]
    assert len(drops) == 2, c.commands


def test_a_POPULATED_table_on_the_old_key_is_NEVER_dropped(capsys):
    """Dropping an operator's declarations to fix storage layout is not a
    decision this code gets to make unattended on three live servers."""
    c = FakeClient(sorting_key='key', rows=17)
    _setup_both(c)
    assert not any(s.strip().startswith('DROP') for s in c.commands)


def test_a_populated_table_on_the_old_key_SAYS_SO_LOUDLY(monkeypatch):
    """Silence here is the whole failure mode: the table keeps working,
    keeps answering reads, and quietly stops replicating changes."""
    said = []
    monkeypatch.setattr(wds, 'log', lambda msg, lvl='INFO': said.append((lvl, msg)))
    c = FakeClient(sorting_key='key', rows=17)
    _setup_both(c)
    warnings = [m for lvl, m in said if lvl == 'WARNING']
    assert len(warnings) == 2, said
    for m in warnings:
        assert 'NOT replicate' in m
        assert '17' in m                      # how much is at stake
        assert 'key, call, grid' in m         # and what to move it to


# ── the sync script ──────────────────────────────────────────────────────────

def _tables_line():
    m = re.search(r'^TABLES="([^"]*)"', SYNC, re.M)
    assert m, 'TABLES= not found in ch-sync-peers.sh'
    return m.group(1).split()


@pytest.mark.parametrize('table', IDENTITY_TABLES)
def test_the_identity_table_is_actually_SYNCED(table):
    """A table the forwarder reads but the sync never carries is a table
    that only one of three servers can be trusted about."""
    assert table in _tables_line()


@pytest.mark.parametrize('table', IDENTITY_TABLES)
def test_each_identity_table_declares_WHICH_COLUMN_carries_its_time(table):
    """These are keyed on when the declaration was made, not on a spot
    instant — there is no `time` column to window on."""
    assert re.search(r'^TABLE_TIME_COL\[%s\]="updated_at"' % re.escape(table),
                     SYNC, re.M), table


def test_the_window_uses_the_PER_TABLE_time_column_not_a_hardcoded_one():
    """The mutation that matters: reverting to a bare `time` silently
    breaks every table whose clock column is named anything else."""
    win = re.search(r'^\s*where_win=.*$', SYNC, re.M)
    hour = re.search(r'^\s*hour_expr=.*$', SYNC, re.M)
    assert win and hour
    for line in (win.group(0), hour.group(0)):
        assert '${tcol}' in line, line
        assert not re.search(r'[^_${]time\b', line.replace('${tcol}', '')), line


def test_every_table_given_a_time_column_is_one_we_actually_sync():
    """A mapping for a table that is not in TABLES is dead config that
    reads like cover."""
    mapped = re.findall(r'^TABLE_TIME_COL\[([^\]]+)\]', SYNC, re.M)
    listed = _tables_line()
    assert mapped, 'no TABLE_TIME_COL entries found'
    for t in mapped:
        assert t in listed, t


def test_tcol_DEFAULTS_to_time_so_the_spot_tables_are_unaffected():
    assert re.search(r'tcol="\$\{TABLE_TIME_COL\[\$table\]:-time\}"', SYNC)


# ── the readers still resolve newest-wins ────────────────────────────────────

@pytest.mark.parametrize('module,table', [
    ('pskreporter_forwarder', 'reporter'),
    ('wsprnet_forwarder', 'wsprnet_reporter'),
])
def test_the_forwarder_resolves_the_NEWEST_declaration_per_key(module, table):
    """With several rows per key now possible by design, a reader that did
    not take argMax(updated_at) could report under a superseded identity."""
    src = (ROOT / f'{module}.py').read_text()
    start = src.index('def load_declared_identities')
    end = src.index('\ndef ', start + 1)
    body = src[start:end]
    m = re.search(r'sql = \((.*?)\)\n', body, re.S)
    assert m, module
    sql = m.group(1)
    # the name may arrive via a module constant, so look module-wide
    assert table in src, (module, table)
    assert 'argMax' in sql and 'updated_at' in sql, sql
    assert 'GROUP BY' in sql and '`key`' in sql, sql
