"""Tests for the Phase 1 station.heartbeat ingest path.

No ClickHouse: these exercise the parsing helpers against synthetic
extracted-tar trees, the shape tests/test_psk_ingest.py already uses.

Design: sigmond/tasks/plan-status-via-uploader.md.
Wire contract: sigmond/lib/sigmond/heartbeat_schema.py.

Three rules carry the weight here, and each has a test that fails when it is
broken:

  * ⛔ `received_at` IS THE TAR'S MTIME.  Availability comes from ARRIVAL,
    never from anything a station says about itself — a station that has
    stopped cannot report that it stopped.  `emitted_at` is forensics.

  * ⛔ `role` AND `host_id` COME FROM THE TAR PATH.  Inferring "pm" from a
    `rac` block would misfile every Proxmox host that carries no RAC switch
    script, because pm-heartbeat.py leaves `rac` out on exactly those hosts
    — so the inference fails for the hosts least likely to be noticed.

  * ⛔ STATUS IS THE PASSENGER, NOT THE CARGO.  A tar whose spots parsed must
    never be rejected because its status did not.

Run with:  python3 -m pytest tests/test_status_ingest.py -v
Or with:   python3 tests/test_status_ingest.py
"""
from __future__ import annotations

import json
import os
import sys
import tempfile
from datetime import datetime
from pathlib import Path

HERE = Path(__file__).resolve().parent
sys.path.insert(0, str(HERE.parent))

import types as _types  # noqa: E402
for _mod in ('clickhouse_connect', 'clickhouse_driver'):
    if _mod not in sys.modules:
        _stub = _types.ModuleType(_mod)
        _stub.Client = object
        _stub.get_client = lambda **kw: None
        sys.modules[_mod] = _stub

import wsprdaemon_server as ws  # noqa: E402

ARRIVAL = datetime(2026, 10, 26, 21, 30, 0)


def _envelope(**over):
    env = {
        "kind": "station_heartbeat",
        "schema_version": 1,
        "station": "AC0G-ND",
        "callsign": "AC0G",
        "grid": "EN16ov",
        "emitted_at": "2026-10-26T21:28:13Z",
        "interval_sec": 60,
        "rollup": {"verdict": "INVALID", "block": "timing",
                   "reason": "FUSE refclock stale for 412 s"},
        "blocks": {
            "versions": {"verdict": "VALID", "reason": "all components current"},
            "timing": {"verdict": "INVALID",
                       "reason": "FUSE refclock stale for 412 s"},
            "gaps": {"verdict": "INCONCLUSIVE", "reason": "first run, no delta"},
        },
    }
    env.update(over)
    return env


def _write(root: Path, role: str, host: str, name: str, envelopes):
    path = root / 'status' / role / host / name
    path.parent.mkdir(parents=True, exist_ok=True)
    with open(path, 'w') as fh:
        for env in envelopes:
            fh.write(json.dumps(env) + '\n')
    return path


# ---------- status_row: one envelope -> one row ----------

def test_a_row_carries_the_arrival_time_not_the_emitted_time():
    """⛔ The rule the whole design rests on.  If received_at ever came from
    the envelope, a station whose clock froze would report itself fresh
    forever — and that is precisely the station the board must catch."""
    row = ws.status_row(_envelope(), role='vm', host_id='ac0g-nd',
                        received_at=ARRIVAL, gateway='gw1')
    assert row['received_at'] == ARRIVAL
    assert row['emitted_at'] == datetime(2026, 10, 26, 21, 28, 13)
    assert row['emitted_at'] != row['received_at']


def test_role_and_host_id_come_from_the_path_not_the_blocks():
    """⛔ A 'pm' envelope that carries no rac block must still file as pm.
    pm-heartbeat.py omits rac on hosts with no RAC switch script, so any
    inference from blocks misfiles exactly the least-watched hosts."""
    env = _envelope()
    assert 'rac' not in env['blocks']
    row = ws.status_row(env, role='pm', host_id='b4-proxmox',
                        received_at=ARRIVAL, gateway='gw2')
    assert row['role'] == 'pm'
    assert row['host_id'] == 'b4-proxmox'


def test_block_verdicts_and_reasons_become_maps():
    row = ws.status_row(_envelope(), role='vm', host_id='h',
                        received_at=ARRIVAL, gateway='gw1')
    assert row['verdict'] == {'versions': 'VALID', 'timing': 'INVALID',
                              'gaps': 'INCONCLUSIVE'}
    assert row['reason']['timing'] == 'FUSE refclock stale for 412 s'
    assert row['rollup_verdict'] == 'INVALID'
    assert row['rollup_block'] == 'timing'


def test_a_block_this_server_has_never_heard_of_is_still_stored():
    """ADDITIVE FIELDS ARE ALLOWED AT EVERY DEPTH (validate()'s rule).  A
    station one release ahead must not be dropped for a block we do not
    know, or the fleet loses visibility of the hosts updated most recently
    — which is backwards."""
    env = _envelope()
    env['blocks']['quantum_flux'] = {'verdict': 'VALID', 'reason': 'nominal'}
    env['some_new_top_level'] = 42
    row = ws.status_row(env, role='vm', host_id='h',
                        received_at=ARRIVAL, gateway='gw1')
    assert row['verdict']['quantum_flux'] == 'VALID'
    assert json.loads(row['payload'])['some_new_top_level'] == 42


def test_the_payload_is_the_envelope_verbatim_and_the_sha_matches_it():
    """The rule that a vanished block can never read as healthy only holds
    if the original is recoverable.  The derived columns are a convenience;
    the envelope is the evidence."""
    import hashlib
    env = _envelope()
    row = ws.status_row(env, role='vm', host_id='h',
                        received_at=ARRIVAL, gateway='gw1')
    assert json.loads(row['payload']) == env
    assert row['envelope_sha'] == hashlib.sha256(
        row['payload'].encode()).hexdigest()
    assert len(row['envelope_sha']) == 64


def test_two_identical_envelopes_share_a_sha_so_a_retry_collapses():
    """A retry landing after a partial success yields two rows on the same
    ORDER BY key; ReplacingMergeTree then keeps one."""
    a = ws.status_row(_envelope(), role='vm', host_id='h',
                      received_at=ARRIVAL, gateway='gw1')
    b = ws.status_row(_envelope(), role='vm', host_id='h',
                      received_at=datetime(2026, 10, 26, 21, 35, 0),
                      gateway='gw2')
    assert a['envelope_sha'] == b['envelope_sha']
    assert a['station'] == b['station'] and a['emitted_at'] == b['emitted_at']


def test_a_non_heartbeat_line_is_rejected():
    assert ws.status_row({'kind': 'something_else', 'station': 'X'},
                         role='vm', host_id='h', received_at=ARRIVAL,
                         gateway='gw1') is None


def test_an_envelope_with_no_station_is_rejected():
    """station and emitted_at are what the row is keyed on.  A row keyed on
    a guess is worse than no row."""
    for bad in ('', '   ', None, 123):
        assert ws.status_row(_envelope(station=bad), role='vm', host_id='h',
                             received_at=ARRIVAL, gateway='gw1') is None


def test_an_unparseable_emitted_at_keeps_the_row():
    """emitted_at is forensic.  Losing the row would discard the ARRIVAL
    signal, which is the one that actually matters."""
    row = ws.status_row(_envelope(emitted_at='not a timestamp'), role='vm',
                        host_id='h', received_at=ARRIVAL, gateway='gw1')
    assert row is not None
    assert row['received_at'] == ARRIVAL
    assert row['emitted_at'] == datetime(1970, 1, 1)


def test_epoch_and_offset_timestamps_both_parse_to_naive_utc():
    epoch = ws.status_row(_envelope(emitted_at=1793482093), role='vm',
                          host_id='h', received_at=ARRIVAL, gateway='gw1')
    offset = ws.status_row(_envelope(emitted_at='2026-10-26T16:28:13-05:00'),
                           role='vm', host_id='h', received_at=ARRIVAL,
                           gateway='gw1')
    assert epoch['emitted_at'].tzinfo is None
    assert offset['emitted_at'] == datetime(2026, 10, 26, 21, 28, 13)


def test_a_missing_rollup_or_blocks_does_not_lose_the_row():
    row = ws.status_row({'kind': 'station_heartbeat', 'station': 'AC0G-ND'},
                        role='vm', host_id='h', received_at=ARRIVAL,
                        gateway='gw1')
    assert row is not None
    assert row['rollup_verdict'] == '' and row['verdict'] == {}


def test_an_absurd_interval_cannot_overflow_the_uint16_column():
    row = ws.status_row(_envelope(interval_sec=10 ** 9), role='vm',
                        host_id='h', received_at=ARRIVAL, gateway='gw1')
    assert row['interval_sec'] == 65535
    row = ws.status_row(_envelope(interval_sec='sixty'), role='vm',
                        host_id='h', received_at=ARRIVAL, gateway='gw1')
    assert row['interval_sec'] == 0


# ---------- the gateway comes from the spool directory ----------

def test_the_gateway_is_read_from_the_incoming_directory():
    assert ws.status_gateway_from_dir('/var/spool/wsprdaemon/from-gw1') == 'gw1'
    assert ws.status_gateway_from_dir(Path('/srv/incoming/from-gw2')) == 'gw2'
    assert ws.status_gateway_from_dir('/srv/incoming/uploads') == ''


# ---------- process_status_files: the tree ----------

def test_scans_role_and_host_subdirs_and_returns_one_row_per_line():
    with tempfile.TemporaryDirectory() as tmp:
        root = Path(tmp)
        _write(root, 'vm', 'ac0g-nd', '20261026T212800Z.jsonl',
               [_envelope(), _envelope(emitted_at='2026-10-26T21:29:13Z')])
        _write(root, 'pm', 'b4-proxmox', '20261026T212500Z.jsonl',
               [_envelope(station='AC0G-B4')])
        rows = ws.process_status_files(root, 'status', ARRIVAL, 'gw1')
        assert len(rows) == 3
        assert {r['role'] for r in rows} == {'vm', 'pm'}
        assert {r['host_id'] for r in rows} == {'ac0g-nd', 'b4-proxmox'}
        assert all(r['received_at'] == ARRIVAL for r in rows)
        assert all(r['gateway'] == 'gw1' for r in rows)


def test_a_tar_with_no_status_subdir_yields_nothing_and_does_not_raise():
    with tempfile.TemporaryDirectory() as tmp:
        assert ws.process_status_files(Path(tmp), 'status', ARRIVAL, 'gw1') == []


def test_one_bad_line_does_not_cost_the_file():
    """⛔ Containment.  The smallest unit that can fail should be the only
    thing that fails: one malformed line must not discard the envelopes
    around it."""
    with tempfile.TemporaryDirectory() as tmp:
        root = Path(tmp)
        path = root / 'status' / 'vm' / 'h' / 'x.jsonl'
        path.parent.mkdir(parents=True)
        with open(path, 'w') as fh:
            fh.write(json.dumps(_envelope()) + '\n')
            fh.write('{ not json\n')
            fh.write('\n')
            fh.write(json.dumps({'kind': 'not_a_heartbeat'}) + '\n')
            fh.write(json.dumps(_envelope(station='AC0G-B4')) + '\n')
        rows = ws.process_status_files(root, 'status', ARRIVAL, 'gw1')
        assert [r['station'] for r in rows] == ['AC0G-ND', 'AC0G-B4']


def test_a_file_at_the_wrong_depth_is_skipped_not_misfiled():
    """status/<ROLE>/<HOST>/<file>.  A file directly under a role has no
    host, and inventing one would attribute a heartbeat to a host that did
    not send it."""
    with tempfile.TemporaryDirectory() as tmp:
        root = Path(tmp)
        shallow = root / 'status' / 'vm' / 'x.jsonl'
        shallow.parent.mkdir(parents=True)
        shallow.write_text(json.dumps(_envelope()) + '\n')
        assert ws.process_status_files(root, 'status', ARRIVAL, 'gw1') == []


def test_an_unknown_role_is_stored_as_sent_not_dropped():
    """A new role is a schema decision on both sides.  Dropping the rows
    would hide the hosts using it; renaming them would lie.  Store and warn."""
    with tempfile.TemporaryDirectory() as tmp:
        root = Path(tmp)
        _write(root, 'container', 'h', 'x.jsonl', [_envelope()])
        rows = ws.process_status_files(root, 'status', ARRIVAL, 'gw1')
        assert len(rows) == 1 and rows[0]['role'] == 'container'


def test_an_unreadable_file_does_not_cost_the_other_files():
    with tempfile.TemporaryDirectory() as tmp:
        root = Path(tmp)
        good = _write(root, 'vm', 'a', 'good.jsonl', [_envelope()])
        bad = _write(root, 'vm', 'b', 'bad.jsonl', [_envelope()])
        if os.geteuid() != 0:
            bad.chmod(0o000)
            rows = ws.process_status_files(root, 'status', ARRIVAL, 'gw1')
            bad.chmod(0o644)
            assert len(rows) == 1 and rows[0]['host_id'] == 'a'
        assert good.exists()


# ---------- the column set matches the table ----------

def test_every_row_key_is_a_column_in_the_schema_file():
    """⛔ A row key the table does not have fails the INSERT for the whole
    batch — every station's heartbeat in that flush, not just the one.  The
    schema file and the inline CREATE TABLE are both checked, because they
    are two copies of the same contract."""
    row = ws.status_row(_envelope(), role='vm', host_id='h',
                        received_at=ARRIVAL, gateway='gw1')
    sql = (HERE.parent / 'schema' / 'station.heartbeat.sql').read_text()
    import re
    declared = set(re.findall(r'^\s*`(\w+)`\s', sql, re.M))
    assert declared, "no columns parsed out of the schema file"
    missing = set(row) - declared
    assert not missing, f"row keys with no column: {sorted(missing)}"
    # ingested_at has a DEFAULT and is deliberately not written by us.
    assert 'ingested_at' not in row


def test_the_inline_create_table_matches_the_schema_file():
    import inspect, re
    src = inspect.getsource(ws.setup_status_tables)
    inline = set(re.findall(r'`(\w+)`\s+(?:DateTime|LowCardinality|UInt16|'
                            r'Map|FixedString|String)', src))
    sql = (HERE.parent / 'schema' / 'station.heartbeat.sql').read_text()
    declared = set(re.findall(r'^\s*`(\w+)`\s+(?:DateTime|LowCardinality|'
                              r'UInt16|Map|FixedString|String)', sql, re.M))
    assert inline == declared, (
        f"inline-only: {sorted(inline - declared)}; "
        f"file-only: {sorted(declared - inline)}")


if __name__ == '__main__':
    import pytest
    sys.exit(pytest.main([__file__, '-v']))
