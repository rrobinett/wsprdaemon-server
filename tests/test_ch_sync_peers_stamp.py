"""A spot copied from a peer takes THIS host's ingest time.

The wsprnet forwarder selects `proxy_upload = 1 AND ingested_at > watermark`.
`ch-sync-peers` used to copy a peer's ingested_at as it stood: 0 on a peer
whose ingest never sets it (wd10).  On wd30 on 2026-10-30, every AC0G/B4 spot
after 14:00Z arrived only through that copy, carried ingested_at = 1970, sat
below the watermark, and never reached wsprnet.
"""
from __future__ import annotations

import re
import subprocess
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
SYNC = (ROOT / 'ch-sync-peers.sh').read_text()


def _select_cols(table: str, cols: str) -> str:
    stamp = re.search(r'^STAMP_INGESTED_AT_TABLES=.*$', SYNC, re.M).group(0)
    fn = re.search(r'^select_cols\(\) \{.*?^\}', SYNC, re.M | re.S).group(0)
    script = f'{stamp}\n{fn}\nselect_cols "$1" "$2"\n'
    out = subprocess.run(['bash', '-c', script, 'x', table, cols],
                         capture_output=True, text=True, check=True)
    return out.stdout.strip()


COLS = 'time, ingested_at, rx_sign, proxy_upload'


def test_spots_copy_stamps_this_hosts_ingest_time():
    assert _select_cols('wsprdaemon.spots', COLS) == \
        'time, now() AS ingested_at, rx_sign, proxy_upload'


def test_ingested_at_first_or_last_is_stamped_too():
    assert _select_cols('wsprdaemon.spots', 'ingested_at, time') == \
        'now() AS ingested_at, time'
    assert _select_cols('wsprdaemon.spots', 'time, ingested_at') == \
        'time, now() AS ingested_at'


def test_a_column_merely_named_like_it_is_left_alone():
    cols = 'time, ingested_at_peer, rx_sign'
    assert _select_cols('wsprdaemon.spots', cols) == cols


def test_psk_spots_keep_the_peers_value():
    # Stamping psk.spots would hand the pskreporter forwarder rows that
    # reached this host from wd10's pskreporter loader.
    assert _select_cols('psk.spots', COLS) == COLS


def test_every_copy_selects_through_select_cols():
    inserts = re.findall(r'INSERT INTO \$\{table\} \(\$\{cols\}\) SELECT (\$\{\w+\})', SYNC)
    assert inserts and set(inserts) == {'${scols}'}
