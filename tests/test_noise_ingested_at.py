"""Noise rows carry the server's arrival time (`ingested_at`).

Liveness for the fleet monitor comes from when the SERVER received a row,
never from the station's own timestamp.  No ClickHouse needed.
"""
from __future__ import annotations

import sys
import tempfile
import types as _types
from datetime import datetime, timedelta
from pathlib import Path

HERE = Path(__file__).resolve().parent
sys.path.insert(0, str(HERE.parent))

for _mod in ('clickhouse_connect', 'clickhouse_driver'):
    if _mod not in sys.modules:
        _stub = _types.ModuleType(_mod)
        _stub.Client = object
        _stub.get_client = lambda **kw: None
        sys.modules[_mod] = _stub

import wsprdaemon_server as ws  # noqa: E402


def test_noise_column_default_is_a_constant():
    seen = []

    class C:
        def command(self, sql):
            seen.append(sql)

    ws.ensure_noise_ingested_at(
        C(), {'clickhouse_database': 'wsprdaemon',
              'clickhouse_noise_table': 'noise'})
    assert ('ADD COLUMN IF NOT EXISTS `ingested_at` DateTime '
            'DEFAULT toDateTime(0)') in seen[0]
    assert 'now()' not in seen[0]
    assert 'wsprdaemon.noise' in seen[0]


def test_noise_insert_rows_carry_ingested_at():
    with tempfile.TemporaryDirectory() as td:
        d = Path(td) / 'wspr' / 'noise' / 'AC0G=ND_EN16ov' / 'KA9Q_DXE' / '20'
        d.mkdir(parents=True)
        fields = ' '.join(['0'] * 12 + ['-120.5', '-130.25', '3'])
        (d / 'AC0G_KA9Q_DXE_20_20260101_000000_noise.txt').write_text(fields + '\n')
        rows = ws.process_noise_files(Path(td), None, None)
    assert len(rows) == 1
    ts = rows[0]['ingested_at']
    assert isinstance(ts, datetime)
    assert abs(datetime.utcnow() - ts) < timedelta(seconds=5)
    # the station's own time stays a separate column
    assert rows[0]['time'] == datetime(2026, 1, 1, 0, 0, 0)
