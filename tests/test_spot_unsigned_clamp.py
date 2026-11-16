"""An out-of-range value in one unsigned spot column must not block a flush.

On 2026-11-16 a spot with sync_quality outside UInt16 made every insert fail
("'H' format requires 0 <= number <= 65535"), and with it every flush of
spots, noise, psk and chirp rows for six hours.

Run with:  python3 -m pytest tests/test_spot_unsigned_clamp.py -v
"""
from __future__ import annotations

import sys
import types as _types
from pathlib import Path

HERE = Path(__file__).resolve().parent
sys.path.insert(0, str(HERE.parent))
for _mod in ('clickhouse_connect', 'clickhouse_driver'):
    if _mod not in sys.modules:
        _stub = _types.ModuleType(_mod)
        _stub.Client = object
        _stub.get_client = lambda **kw: None
        sys.modules[_mod] = _stub

import pytest  # noqa: E402

import wsprdaemon_server as ws  # noqa: E402


def _spot(**extra):
    spot = {'date': '261116', 'time': '1628', 'freq_hz': 14_097_100.0,
            'band': 20, 'power_dbm': 37, 'snr': -20, 'dt': 0.1,
            'rx_id': 'AC0G/B1A', 'rx_sign': 'AC0G', 'tx_sign': 'K1ABC'}
    spot.update(extra)
    return spot


@pytest.mark.parametrize('field,value,want', [
    ('sync_quality', -3, 0),
    ('sync_quality', 70000, 65535),
    ('sync_quality', 812, 812),
    ('blocksize', -1, 0),
    ('nhardmin', 99999, 65535),
    ('osd_decode', 300, 255),
    ('ipass', -2, 0),
])
def test_unsigned_columns_clamped(field, value, want):
    assert ws.convert_spot_to_clickhouse(_spot(**{field: value}))[field] == want


def test_clamp_names_the_reporter(monkeypatch):
    said = []
    monkeypatch.setattr(ws, 'log', lambda msg, level='INFO': said.append((level, msg)))
    ws.convert_spot_to_clickhouse(_spot(sync_quality=-3))
    assert any('AC0G/B1A' in m and 'sync_quality' in m and '-3' in m for _, m in said)


def test_in_range_says_nothing(monkeypatch):
    said = []
    monkeypatch.setattr(ws, 'log', lambda msg, level='INFO': said.append(msg))
    ws.convert_spot_to_clickhouse(_spot(sync_quality=5))
    assert not said
