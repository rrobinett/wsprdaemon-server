"""Tests for the Phase 2 PR 1 psk-ingest path.

These do NOT touch ClickHouse — they exercise the parsing/scanning helpers
against synthetic extracted-tar trees in a temp directory.

Run with:  python3 -m pytest tests/test_psk_ingest.py -v
Or with:   python3 tests/test_psk_ingest.py
"""
from __future__ import annotations

import json
import sys
import tempfile
from datetime import datetime, timezone
from pathlib import Path

HERE = Path(__file__).resolve().parent
sys.path.insert(0, str(HERE.parent))

# Stub out the clickhouse client libs so tests don't need them installed.
# We only exercise pure-Python parsing/scanning helpers — no DB calls are
# made. If/when we add CH-touching tests, swap to a real venv.
import types as _types  # noqa: E402
for _mod in ('clickhouse_connect', 'clickhouse_driver'):
    if _mod not in sys.modules:
        _stub = _types.ModuleType(_mod)
        _stub.Client = object  # for `clickhouse_driver.Client(...)`
        _stub.get_client = lambda **kw: None  # for `clickhouse_connect.get_client(...)`
        sys.modules[_mod] = _stub

import wsprdaemon_server as ws  # noqa: E402


def _write_jsonl(path: Path, rows):
    path.parent.mkdir(parents=True, exist_ok=True)
    with open(path, 'w') as f:
        for row in rows:
            f.write(json.dumps(row) + '\n')


def test_load_routing_missing_file_returns_safe_default():
    with tempfile.TemporaryDirectory() as td:
        routing = ws.load_routing(Path(td))
        assert routing == {'default': {'forward_to_pskreporter': True}}


def test_load_routing_reads_per_receiver_overrides():
    with tempfile.TemporaryDirectory() as td:
        td = Path(td)
        (td / 'routing.json').write_text(json.dumps({
            'default': {'forward_to_pskreporter': True},
            'AC0G=ND_EN16ov/KA9Q_DXE': {'forward_to_pskreporter': False},
        }))
        routing = ws.load_routing(td)
        assert ws._forward_flag(routing, 'AC0G=ND_EN16ov/KA9Q_DXE') is False
        assert ws._forward_flag(routing, 'AC0G=ND_EN16ov/OTHER') is True
        # No 'default' / missing entry → safe default True
        assert ws._forward_flag({}, 'whatever') is True


def test_process_mode_files_jt9_jsonl():
    with tempfile.TemporaryDirectory() as td:
        td = Path(td)
        spot_file = td / 'ft8' / 'AC0G=ND_EN16ov' / 'KA9Q_DXE' / '20' / '260518_1230_ft8.jsonl'
        rows = [
            {
                'time':          '2026-05-18T12:30:15+00:00',
                'mode':          'ft8',
                'decoder_kind':  'jt9',
                'score':         50,
                'snr_db':        -12,
                'dt':            0.2,
                'frequency':     14_074_580,
                'message':       'AC0G K1ABC FN42',
                'tx_call':       'K1ABC',
                'rx_call':       'AC0G',
                'grid':          'FN42',
                'report':        None,
                'processing_version': 'psk-recorder/0.6.0',
            },
            {
                'time':          '2026-05-18T12:30:15+00:00',
                'mode':          'ft8',
                'decoder_kind':  'jt9',
                'score':         42,
                'snr_db':        -18,
                'dt':            -0.5,
                'frequency':     14_074_900,
                'message':       'CQ W1XYZ EN42',
                'tx_call':       'W1XYZ',
                'grid':          'EN42',
            },
        ]
        _write_jsonl(spot_file, rows)

        routing = {
            'default': {'forward_to_pskreporter': True},
            'AC0G=ND_EN16ov/KA9Q_DXE': {'forward_to_pskreporter': False},
        }
        out = ws.process_mode_files(td, 'ft8', routing, host_id='b4-100')

        assert len(out) == 2
        for row in out:
            assert row['rx_site'] == 'AC0G=ND_EN16ov'
            assert row['receiver'] == 'KA9Q_DXE'
            assert row['rx_sign'] == 'AC0G/ND'  # `=` decoded back to `/`
            assert row['rx_loc'] == 'EN16ov'
            assert row['mode'] == 'ft8'
            assert row['band'] == 20
            assert row['forward_to_pskreporter'] is False  # per-receiver override
            assert row['host_id'] == 'b4-100'


def test_process_mode_files_no_routing_defaults_to_forward():
    with tempfile.TemporaryDirectory() as td:
        td = Path(td)
        spot_file = td / 'ft4' / 'AC0G=ND_EN16ov' / 'KA9Q_DXE' / '20' / '260518_1230_ft4.jsonl'
        _write_jsonl(spot_file, [{
            'time':          '2026-05-18T12:30:00+00:00',
            'mode':          'ft4',
            'decoder_kind':  'jt9',
            'frequency':     14_080_000,
            'message':       'CQ K1ABC FN42',
            'tx_call':       'K1ABC',
            'grid':          'FN42',
        }])
        routing = {'default': {'forward_to_pskreporter': True}}
        out = ws.process_mode_files(td, 'ft4', routing, host_id='b4-100')
        assert len(out) == 1
        assert out[0]['forward_to_pskreporter'] is True


def test_process_mode_files_missing_mode_dir_returns_empty():
    with tempfile.TemporaryDirectory() as td:
        td = Path(td)
        # No ft8/ tree at all — must not raise
        out = ws.process_mode_files(td, 'ft8', {}, host_id='b4-100')
        assert out == []


def test_process_mode_files_skips_bad_jsonl_line():
    with tempfile.TemporaryDirectory() as td:
        td = Path(td)
        spot_file = td / 'ft8' / 'AC0G=ND_EN16ov' / 'KA9Q_DXE' / '20' / '260518_1230_ft8.jsonl'
        spot_file.parent.mkdir(parents=True, exist_ok=True)
        with open(spot_file, 'w') as f:
            f.write('{"time": "2026-05-18T12:30:15+00:00", "frequency": 14074580}\n')
            f.write('this is not json\n')
            f.write('{"time": "2026-05-18T12:30:30+00:00", "frequency": 14074900}\n')
        out = ws.process_mode_files(td, 'ft8', {}, host_id='b4-100')
        # Bad line silently skipped, good lines returned.
        assert len(out) == 2


def test_convert_psk_spot_accepts_iso_string():
    row = {
        'time': '2026-05-18T12:30:15+00:00',
        'mode': 'ft8',
        'rx_sign': 'AC0G/ND',
        'frequency': 14_074_580,
        'forward_to_pskreporter': True,
    }
    out = ws.convert_psk_spot_to_clickhouse(row)
    assert isinstance(out['time'], datetime)
    assert out['time'].tzinfo is None  # CH driver wants naive
    assert out['time'].year == 2026
    assert out['mode'] == 'ft8'
    assert out['frequency'] == 14_074_580
    assert out['forward_to_pskreporter'] == 1


def test_convert_psk_spot_accepts_epoch():
    row = {'time': 1_747_577_415.0, 'frequency': 14_074_580}
    out = ws.convert_psk_spot_to_clickhouse(row)
    assert isinstance(out['time'], datetime)


def test_convert_psk_spot_accepts_datetime_aware():
    row = {
        'time': datetime(2026, 5, 18, 12, 30, 15, tzinfo=timezone.utc),
        'frequency': 14_074_580,
    }
    out = ws.convert_psk_spot_to_clickhouse(row)
    assert out['time'].tzinfo is None
    assert out['time'].hour == 12


def test_convert_psk_spot_forward_flag_defaults_to_one():
    # Field missing → defaults to 1 (forward), matching the schema DEFAULT.
    out = ws.convert_psk_spot_to_clickhouse({'time': '2026-05-18T12:30:00Z', 'frequency': 1})
    assert out['forward_to_pskreporter'] == 1


def test_process_spot_files_accepts_new_wspr_root():
    """Backward-compat: server accepts the new `wspr/spots/...` tar layout
    in addition to legacy `wsprdaemon/spots/...`. Both still write to the
    same wsprdaemon.spots ClickHouse table.
    """
    with tempfile.TemporaryDirectory() as td:
        td = Path(td)
        # Create one spot file under the new wspr/ root, with one valid
        # 34-field line that matches the wsprdaemon spot format.
        spot_file = td / 'wspr' / 'spots' / 'AC0G=ND_EN16ov' / 'KA9Q_DXE' / '20' / '260518_1230_spots.txt'
        spot_file.parent.mkdir(parents=True, exist_ok=True)
        line = (
            '260518 1230 5 -12 0.3 14.097150 K1ABC FN42 33 0 '
            '1 50 4500 100 0 1 0 1 -125.5 -124.0 '
            '20 EN16ov AC0G/ND 2000 270.5 41.0 -71.0 90.5 41.0 -73.0 '
            '0.0 0.0 0 1\n'
        )
        spot_file.write_text(line)
        spots = ws.process_spot_files(td, client_version='2.27.0-test')
        assert len(spots) == 1
        assert spots[0]['rx_sign'] == 'AC0G/ND'
        assert spots[0]['rx_loc'] == 'EN16ov'


def test_process_noise_files_accepts_new_wspr_root():
    with tempfile.TemporaryDirectory() as td:
        td = Path(td)
        noise_file = td / 'wspr' / 'noise' / 'AC0G=ND_EN16ov' / 'KA9Q_DXE' / '20' / '260518_1230_noise.txt'
        noise_file.parent.mkdir(parents=True, exist_ok=True)
        # 15 space-separated fields; [12]=rms_level, [13]=c2_level, [14]=ov
        noise_file.write_text(
            '0 0 0 0 0 0 0 0 0 0 0 0 -130.5 -129.0 7\n'
        )
        recs = ws.process_noise_files(td, running_jobs=None,
                                       receiver_descriptions=None)
        assert len(recs) == 1
        assert recs[0]['site'] == 'AC0G/ND'
        assert recs[0]['receiver'] == 'KA9Q_DXE'
        assert recs[0]['rx_loc'] == 'EN16ov'
        assert recs[0]['band'] == '20'
        assert abs(recs[0]['rms_level'] - (-130.5)) < 1e-3
        assert abs(recs[0]['c2_level'] - (-129.0)) < 1e-3
        assert recs[0]['ov'] == 7


def test_extract_tbz_handles_bz2():
    import io as _io, tarfile, tempfile, bz2
    with tempfile.TemporaryDirectory() as td:
        td = Path(td)
        # Build a tiny bz2 tar in memory.
        buf = _io.BytesIO()
        with tarfile.open(fileobj=buf, mode='w:bz2') as tf:
            data = b'hello bz2\n'
            ti = tarfile.TarInfo(name='hello.txt'); ti.size = len(data)
            tf.addfile(ti, _io.BytesIO(data))
        out_dir = td / 'out'
        out_dir.mkdir()
        ok = ws.extract_tbz(buf.getvalue(), out_dir)
        assert ok, "bz2 tar should extract"
        assert (out_dir / 'hello.txt').read_bytes() == b'hello bz2\n'


def test_extract_tbz_handles_zstd():
    import io as _io, tarfile, tempfile
    try:
        import zstandard
    except ImportError:
        return  # zstandard not installed locally; skip silently
    with tempfile.TemporaryDirectory() as td:
        td = Path(td)
        # Build an uncompressed tar in memory, then zstd-frame it.
        raw_buf = _io.BytesIO()
        with tarfile.open(fileobj=raw_buf, mode='w:') as tf:
            data = b'hello zstd\n'
            ti = tarfile.TarInfo(name='hello.txt'); ti.size = len(data)
            tf.addfile(ti, _io.BytesIO(data))
        cctx = zstandard.ZstdCompressor(level=9)
        zst_bytes = cctx.compress(raw_buf.getvalue())
        out_dir = td / 'out'
        out_dir.mkdir()
        ok = ws.extract_tbz(zst_bytes, out_dir)
        assert ok, "zstd tar should extract"
        assert (out_dir / 'hello.txt').read_bytes() == b'hello zstd\n'


def test_extract_tbz_rejects_unknown_compression():
    import tempfile
    with tempfile.TemporaryDirectory() as td:
        out_dir = Path(td) / 'out'
        out_dir.mkdir()
        ok = ws.extract_tbz(b"\xff\xfe junk header", out_dir)
        assert ok is False


if __name__ == '__main__':
    # Lightweight runner so we don't require pytest just to smoke-test.
    import inspect
    failures = 0
    for name, fn in sorted(globals().items()):
        if name.startswith('test_') and callable(fn) and inspect.getmodule(fn) is sys.modules[__name__]:
            try:
                fn()
                print(f"PASS  {name}")
            except Exception as e:
                failures += 1
                print(f"FAIL  {name}: {e}")
                import traceback; traceback.print_exc()
    print(f"\n{'='*60}\n{failures} failure(s)")
    sys.exit(1 if failures else 0)


# ---------- declared PSKReporter identity (psk.reporter) ----------
#
# Why it exists: `pskreporter_forwarder` used to decide WHO a row was
# reported under from a fleet-wide env var that stripped the callsign
# suffix.  On wd30 2026-10-27 that mapped both AC0G/B4 (Missouri) and
# AC0G/ND (North Dakota) to one identity, so one site's FT8 spots would
# have been reported from the other's location and de-duplicated against
# it.  rob's rule: unique by default; merging is declared, and names what
# it merges with.


def test_a_receiver_that_declares_NOTHING_gets_no_identity_row():
    """⛔ Silence must stay silence.  Inventing an identity here is how a
    station ends up reported as somebody else."""
    assert ws._psk_reporter({}, 'AC0G=B4_EM38ww/KA9Q_0') is None
    assert ws._psk_reporter({'default': {}}, 'AC0G=B4_EM38ww/KA9Q_0') is None


def test_a_per_receiver_declaration_beats_the_default():
    routing = {
        'default': {'psk_reporter': {'call': 'AC0G', 'grid': 'EM38ww'}},
        'AC0G=ND_EN16ov/KA9Q_DXE': {
            'psk_reporter': {'call': 'AC0G/ND', 'grid': 'EN16ov'}},
    }
    assert ws._psk_reporter(routing, 'AC0G=ND_EN16ov/KA9Q_DXE') == \
        ('AC0G/ND', 'EN16ov')
    assert ws._psk_reporter(routing, 'AC0G=B4_EM38ww/KA9Q_0') == \
        ('AC0G', 'EM38ww')


def test_a_declaration_with_no_CALL_is_dropped_whole():
    """A grid alone cannot name a reporter, and taking the call from the
    path while honouring the stated grid would report real spots from a
    location the operator never claimed."""
    routing = {'default': {'psk_reporter': {'grid': 'EM38ww'}}}
    assert ws._psk_reporter(routing, 'AC0G=B4_EM38ww/KA9Q_0') is None


def test_the_declaration_rides_the_rows_but_is_NOT_a_psk_spots_column():
    """⛔ `_psk_call` must never reach the INSERT: an unknown column fails
    the whole batch, which is every station's spots in that flush."""
    with tempfile.TemporaryDirectory() as td:
        td = Path(td)
        _write_jsonl(
            td / 'ft8' / 'AC0G=ND_EN16ov' / 'KA9Q_DXE' / '20'
            / '261027_1230_ft8.jsonl',
            [{'time': '2026-10-27T12:30:15+00:00', 'mode': 'ft8',
              'tx_call': 'K1ABC', 'grid': 'FN42', 'frequency': 14_074_580}])
        routing = {'AC0G=ND_EN16ov/KA9Q_DXE':
                   {'psk_reporter': {'call': 'AC0G/ND', 'grid': 'EN16ov'}}}
        out = ws.process_mode_files(td, 'ft8', routing, host_id='nd-100')
        assert out[0]['_psk_call'] == 'AC0G/ND'
        assert out[0]['_psk_grid'] == 'EN16ov'
        converted = ws.convert_psk_spot_to_clickhouse(out[0])
        assert '_psk_call' not in converted and '_psk_grid' not in converted


def test_one_identity_row_per_receiver_not_per_spot():
    """A tar carrying 40,000 FT8 rows from one receiver declares ONE
    identity; inserting it per row would make a rarely-changing fact the
    busiest write in the flush."""
    rows = ([{'rx_sign': 'AC0G/B1', '_psk_call': 'AC0G', '_psk_grid': 'EM38ww'}] * 500
            + [{'rx_sign': 'AC0G/B2', '_psk_call': 'AC0G', '_psk_grid': 'EM38ww'}] * 500)
    out = ws.psk_reporter_rows(rows)
    assert sorted(r['key'] for r in out) == ['AC0G/B1', 'AC0G/B2']
    assert all(r['call'] == 'AC0G' for r in out)


def test_rows_that_declared_nothing_produce_no_identity_rows():
    assert ws.psk_reporter_rows(
        [{'rx_sign': 'AC0G/ND', 'tx_call': 'K1ABC'}]) == []
    # and a declaration with no rx_sign cannot be keyed
    assert ws.psk_reporter_rows([{'_psk_call': 'AC0G'}]) == []


def test_the_key_is_spelled_as_THE_FORWARDER_LOOKS_IT_UP():
    """⛔ One vocabulary.  The forwarder keys on the bare rx_sign, which in
    psk.spots already names a single receiver; a key written any other way
    is a row no lookup will ever find."""
    import pskreporter_forwarder as pf
    row = ws.psk_reporter_rows(
        [{'rx_sign': 'AC0G/ND', '_psk_call': 'AC0G', '_psk_grid': 'EM38ww'}])[0]
    psk_row = pf.PskRow(
        time=datetime(2026, 10, 27, tzinfo=timezone.utc),
        ingested_at=datetime(2026, 10, 27, tzinfo=timezone.utc),
        mode='ft8', rx_sign='AC0G/ND', rx_loc='EN16ov',
        tx_call='K1ABC', grid='FN42', frequency=14_074_580,
        snr_db=-12, dt=0.1)
    assert pf.reporter_for(psk_row, {row['key']: {'call': row['call'],
                                                  'grid': row['grid']}}) \
        == ('AC0G', 'EM38ww')


def test_every_psk_identity_row_key_is_a_column_in_the_schema():
    """⛔ A row key with no column fails the INSERT for the WHOLE batch —
    every station's spots in that flush, not just the one.  The DDL lives
    in two files (schema/psk_reporter.sql and the inline copy a fresh wd
    bootstraps from), so the two can drift apart silently."""
    import re
    row_keys = set(ws.psk_reporter_rows(
        [{'rx_sign': 'AC0G/B1', '_psk_call': 'AC0G', '_psk_grid': 'EM38ww'}])[0])
    sql_file = (HERE.parent / "schema" / "psk_reporter.sql").read_text()
    src = (HERE.parent / "wsprdaemon_server.py").read_text()
    inline = src.split("CREATE TABLE IF NOT EXISTS {db}.{PSK_REPORTER_TABLE}")[1]
    inline = inline.split('""")')[0]
    for text, where in ((sql_file, "schema file"), (inline, "inline DDL")):
        cols = set(re.findall(r"`(\w+)`\s+(?:String|DateTime)", text))
        assert cols == row_keys, f"{where} disagrees: {cols ^ row_keys}"
