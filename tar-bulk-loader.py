#!/usr/bin/env python3
"""
tar-bulk-loader.py  v1.0

Bulk-loads WSPR spots and noise from tar archives of tbz files directly
into ClickHouse, without extracting to disk.

Each tar file contains a flat directory of tbz files, each of which
contains the same wsprdaemon directory structure as files delivered
from gateways to wsprdaemon_server.

Key differences from wsprdaemon_server.py:
  - Reads tbz files in-memory from tar archives (no disk extraction)
  - Accumulates a large batch across many tbz files before inserting
  - Inserts in large batches (default 100k rows) for high throughput
  - Writes to staging tables (spots_2025, noise_2025) by default
  - Tracks progress in a state file so runs are resumable
  - --dry-run: parse only, no inserts
  - --limit N: stop after N tbz files (for testing)
  - --tar FILE: process only this tar file (default: all tars in dir)

Usage:
    ./tar-bulk-loader.py --tar-dir /srv/wd_archive/wd0-tar-files \\
        --clickhouse-user chadmin --clickhouse-password ch2025wd \\
        [--spots-table spots_2025] [--noise-table noise_2025] \\
        [--dry-run] [--limit 1000] [--tar TARFILE] [-v]

State file: ./tar-bulk-loader-state.json
    Records which tar files have been fully processed so runs are
    resumable.  Delete it to restart from scratch.
"""

import argparse
import io
import json
import logging
import os
import re
import sys
import tarfile
import time
from datetime import datetime
from pathlib import Path
from typing import Dict, List, Optional, Tuple

try:
    import clickhouse_connect
except ImportError:
    print("ERROR: clickhouse_connect not installed")
    print("  pip install clickhouse-connect")
    sys.exit(1)

VERSION = "1.0"

STATE_FILE = "./tar-bulk-loader-state.json"
DEFAULT_BATCH_SIZE  = 100_000
DEFAULT_SPOTS_TABLE = "spots_2025"
DEFAULT_NOISE_TABLE = "noise_2025"
DEFAULT_DB          = "wsprdaemon"

# ---------------------------------------------------------------
# Logging
# ---------------------------------------------------------------

def setup_logging(verbosity: int):
    level = logging.WARNING if verbosity == 0 else \
            logging.INFO    if verbosity == 1 else \
            logging.DEBUG
    logging.basicConfig(
        format='[%(asctime)s] %(levelname)s: %(message)s',
        datefmt='%Y-%m-%d %H:%M:%S',
        level=level,
        stream=sys.stdout)

def log(msg: str, level: str = "INFO"):
    logging.getLogger().log(
        {'DEBUG': logging.DEBUG, 'INFO': logging.INFO,
         'WARNING': logging.WARNING, 'ERROR': logging.ERROR
        }.get(level, logging.INFO), msg)

# ---------------------------------------------------------------
# State (resumable)
# ---------------------------------------------------------------

def load_state(state_file: str) -> Dict:
    if os.path.exists(state_file):
        try:
            with open(state_file) as f:
                return json.load(f)
        except Exception as e:
            log(f"Warning: could not load state file: {e}", "WARNING")
    return {"completed_tars": [], "total_spots": 0, "total_noise": 0, "total_tbz": 0}

def save_state(state: Dict, state_file: str):
    try:
        with open(state_file, 'w') as f:
            json.dump(state, f, indent=2)
    except Exception as e:
        log(f"Warning: could not save state: {e}", "WARNING")

# ---------------------------------------------------------------
# Parsing (copied/adapted from wsprdaemon_server.py)
# ---------------------------------------------------------------

def band_str_to_meters(band_str: str) -> Optional[int]:
    m = re.match(r'^(\d+)', band_str)
    return int(m.group(1)) if m else None

def decode_rx_site_dir(rx_site_dir: str) -> Tuple[str, str]:
    m = re.match(r'^(.+)_([A-Ra-r]{2}[0-9]{2}[A-Xa-x]{0,2})$', rx_site_dir)
    if m:
        return m.group(1).replace('=', '/'), m.group(2)
    return rx_site_dir.replace('=', '/'), ''

def parse_wsprd_output_lines(lines: List[str],
                              client_version: Optional[str]) -> List[Dict]:
    spots = []
    for line_num, line in enumerate(lines, 1):
        line = line.strip()
        if not line:
            continue
        parts = line.split()
        if len(parts) < 34:
            continue
        try:
            spot = {
                'date':          parts[0],
                'time':          parts[1],
                'sync_quality':  float(parts[2]),
                'snr':           int(float(parts[3])),
                'dt':            float(parts[4]),
                'freq_hz':       float(parts[5]) * 1_000_000.0,
                'tx_sign':       parts[6],
                'tx_loc':        parts[7] if parts[7].lower() != 'none' else '',
                'power_dbm':     int(float(parts[8])),
                'drift':         int(float(parts[9])),
                'decode_cycles': int(float(parts[10])),
                'jitter':        int(float(parts[11])),
                'blocksize':     int(float(parts[12])),
                'metric':        int(float(parts[13])),
                'osd_decode':    int(float(parts[14])),
                'ipass':         int(float(parts[15])),
                'nhardmin':      int(float(parts[16])),
                'code':          int(float(parts[17])),
                'rms_noise':     float(parts[18]),
                'c2_noise':      float(parts[19]),
                'band_m':        int(float(parts[20])),
                'rx_loc':        parts[21],
                'rx_sign_file':  parts[22],
                'distance':      int(float(parts[23])),
                'rx_azimuth':    float(parts[24]),
                'rx_lat':        float(parts[25]),
                'rx_lon':        float(parts[26]),
                'azimuth':       float(parts[27]),
                'tx_lat':        float(parts[28]),
                'tx_lon':        float(parts[29]),
                'v_lat':         float(parts[30]),
                'v_lon':         float(parts[31]),
                'ov_count':      int(float(parts[32])),
                'proxy_upload':  int(float(parts[33])),
            }
            if client_version:
                spot['client_version'] = client_version
            spots.append(spot)
        except (ValueError, IndexError):
            continue
    return spots

def convert_spot_to_clickhouse(spot: Dict, band: int, rx_sign: str,
                                rx_id: str) -> Dict:
    date_str = spot['date']
    time_str = spot['time']
    year   = 2000 + int(date_str[0:2])
    month  = int(date_str[2:4])
    day    = int(date_str[4:6])
    hour   = int(time_str[0:2])
    minute = int(time_str[2:4])
    ts = datetime(year, month, day, hour, minute)

    freq_hz  = spot['freq_hz']
    freq_mhz = freq_hz / 1_000_000.0

    return {
        'time':          ts,
        'band':          band,
        'rx_sign':       rx_sign,
        'rx_lat':        spot.get('rx_lat', 0.0),
        'rx_lon':        spot.get('rx_lon', 0.0),
        'rx_loc':        spot.get('rx_loc', ''),
        'tx_sign':       spot['tx_sign'],
        'tx_loc':        spot.get('tx_loc', ''),
        'tx_lat':        spot.get('tx_lat', 0.0),
        'tx_lon':        spot.get('tx_lon', 0.0),
        'distance':      spot.get('distance', 0),
        'azimuth':       spot.get('azimuth', 0.0),
        'rx_azimuth':    spot.get('rx_azimuth', 0.0),
        'frequency':     int(freq_hz),
        'frequency_mhz': freq_mhz,
        'power':         spot['power_dbm'],
        'snr':           spot['snr'],
        'drift':         spot.get('drift', 0),
        'rx_id':         rx_id,
        'dt':            spot['dt'],
        'sync_quality':  spot.get('sync_quality', 0.0),
        'decode_cycles': spot.get('decode_cycles', 0),
        'jitter':        spot.get('jitter', 0),
        'blocksize':     spot.get('blocksize', 0),
        'metric':        spot.get('metric', 0),
        'osd_decode':    spot.get('osd_decode', 0),
        'nhardmin':      spot.get('nhardmin', 0),
        'ipass':         spot.get('ipass', 0),
        'code':          spot.get('code', 0),
        'rms_noise':     spot.get('rms_noise', 0.0),
        'c2_noise':      spot.get('c2_noise', 0.0),
        'v_lat':         spot.get('v_lat', 0.0),
        'v_lon':         spot.get('v_lon', 0.0),
        'ov_count':      spot.get('ov_count', 0),
        'proxy_upload':  spot.get('proxy_upload', 0),
        'band_m':        spot.get('band_m', band),
        'version':       spot.get('client_version', None),
        'rx_status':     'No Info',
    }

# ---------------------------------------------------------------
# In-memory tbz processing
# ---------------------------------------------------------------

def process_tbz_in_memory(tbz_data: bytes,
                           tbz_name: str) -> Tuple[List[Dict], List[Dict]]:
    """Parse a tbz file from bytes, return (spots, noise_records)."""
    spots_out = []
    noise_out = []

    try:
        with tarfile.open(fileobj=io.BytesIO(tbz_data), mode='r:bz2') as tbz:
            members = tbz.getmembers()

            # Read uploads_config.txt for client_version
            client_version = None
            for m in members:
                if 'uploads_config.txt' in m.name:
                    try:
                        f = tbz.extractfile(m)
                        if f:
                            for line in f.read().decode('utf-8', errors='replace').splitlines():
                                if line.startswith('CLIENT_VERSION='):
                                    client_version = line.split('=', 1)[1].strip('"\'')
                                    break
                    except Exception:
                        pass
                    break

            for member in members:
                if not member.isfile():
                    continue

                parts = member.name.replace('\\', '/').split('/')
                # Spots: wsprdaemon/spots/RX_SITE/RECEIVER/BAND/YYMMDD_HHMM_spots.txt
                if len(parts) >= 5 and parts[-1].endswith('_spots.txt') \
                        and ('spots.d' in parts or 'spots' in parts):
                    idx = parts.index('spots.d') if 'spots.d' in parts else parts.index('spots')
                    if len(parts) - idx < 5:
                        continue
                    rx_site_dir = parts[idx + 1]
                    rx_id       = parts[idx + 2]
                    band_str    = parts[idx + 3]
                    band = band_str_to_meters(band_str)
                    if band is None:
                        continue
                    rx_sign_dir, rx_grid_dir = decode_rx_site_dir(rx_site_dir)
                    try:
                        f = tbz.extractfile(member)
                        if not f:
                            continue
                        lines = f.read().decode('utf-8', errors='replace').splitlines()
                    except Exception as e:
                        log(f"Error reading spots from {member.name}: {e}", "DEBUG")
                        continue
                    raw_spots = parse_wsprd_output_lines(lines, client_version)
                    for s in raw_spots:
                        rx_sign = s.pop('rx_sign_file', None) or rx_sign_dir
                        if not s.get('rx_loc'):
                            s['rx_loc'] = rx_grid_dir
                        ch = convert_spot_to_clickhouse(s, band, rx_sign, rx_id)
                        spots_out.append(ch)

                # Noise: wsprdaemon/noise/RX_SITE/RECEIVER/BAND/YYMMDD_HHMM_noise.txt
                elif len(parts) >= 5 and parts[-1].endswith('_noise.txt') \
                        and ('noise.d' in parts or 'noise' in parts):
                    idx = parts.index('noise.d') if 'noise.d' in parts else parts.index('noise')
                    if len(parts) - idx < 5:
                        continue
                    rx_site_dir = parts[idx + 1]
                    rx_id       = parts[idx + 2]
                    band_str    = parts[idx + 3]
                    rx_sign_dir, rx_grid_dir = decode_rx_site_dir(rx_site_dir)
                    m2 = re.match(r'(\d{6})_(\d{4})_noise\.txt', parts[-1])
                    if not m2:
                        continue
                    try:
                        year   = 2000 + int(m2.group(1)[0:2])
                        month  = int(m2.group(1)[2:4])
                        day    = int(m2.group(1)[4:6])
                        hour   = int(m2.group(2)[0:2])
                        minute = int(m2.group(2)[2:4])
                        ts = datetime(year, month, day, hour, minute)
                    except ValueError:
                        continue
                    try:
                        f = tbz.extractfile(member)
                        if not f:
                            continue
                        content = f.read().decode('utf-8', errors='replace').strip()
                        fields = content.split()
                        if len(fields) != 15:
                            continue
                        ov_raw = int(float(fields[14]))
                        ov_val = max(-2147483648, min(2147483647, ov_raw))
                        noise_out.append({
                            'time':      ts,
                            'site':      rx_sign_dir,
                            'receiver':  rx_id,
                            'rx_loc':    rx_grid_dir,
                            'band':      band_str,
                            'rms_level': float(fields[12]),
                            'c2_level':  float(fields[13]),
                            'ov':        ov_val,
                        })
                    except Exception as e:
                        log(f"Error reading noise from {member.name}: {e}", "DEBUG")
                        continue

    except Exception as e:
        log(f"Error opening tbz {tbz_name}: {e}", "WARNING")

    return spots_out, noise_out

# ---------------------------------------------------------------
# CH insert
# ---------------------------------------------------------------

def insert_batch(client, table: str, records: List[Dict],
                 batch_size: int, dry_run: bool, label: str) -> bool:
    if not records:
        return True
    if dry_run:
        log(f"DRY RUN: would insert {len(records):,} {label}", "INFO")
        return True
    column_names = list(records[0].keys())
    total = len(records)
    for i in range(0, total, batch_size):
        batch = records[i:i + batch_size]
        data = [[row[col] for col in column_names] for row in batch]
        try:
            client.insert(table, data, column_names=column_names)
            log(f"Inserted {label} batch {i // batch_size + 1} "
                f"({len(batch):,} rows)", "DEBUG")
        except Exception as e:
            log(f"Error inserting {label} batch: {e}", "ERROR")
            return False
    return True

# ---------------------------------------------------------------
# Table creation
# ---------------------------------------------------------------

def ensure_staging_tables(client, db: str,
                           spots_table: str, noise_table: str,
                           dry_run: bool):
    if dry_run:
        return
    client.command(f"""
        CREATE TABLE IF NOT EXISTS {db}.{spots_table} AS {db}.spots
        ENGINE = ReplacingMergeTree()
        PARTITION BY toYYYYMM(time)
        ORDER BY (time, rx_sign, tx_sign, frequency)
        SETTINGS index_granularity = 8192
    """)
    log(f"Staging table {db}.{spots_table} ready", "INFO")

    client.command(f"""
        CREATE TABLE IF NOT EXISTS {db}.{noise_table} AS {db}.noise
        ENGINE = ReplacingMergeTree()
        PARTITION BY toYYYYMM(time)
        ORDER BY (time, site, receiver, band)
        SETTINGS index_granularity = 8192
    """)
    log(f"Staging table {db}.{noise_table} ready", "INFO")

# ---------------------------------------------------------------
# Main
# ---------------------------------------------------------------

def main():
    parser = argparse.ArgumentParser(
        description=f'tar-bulk-loader v{VERSION} - bulk import WSPR tar archives to ClickHouse')
    parser.add_argument('--tar-dir', default='/srv/wd_archive/wd0-tar-files',
                        help='Directory containing tar files')
    parser.add_argument('--tar', default=None,
                        help='Process only this single tar file (overrides --tar-dir)')
    parser.add_argument('--clickhouse-user',     required=True)
    parser.add_argument('--clickhouse-password', required=True)
    parser.add_argument('--clickhouse-host',     default='localhost')
    parser.add_argument('--clickhouse-port',     default=8123, type=int)
    parser.add_argument('--db',           default=DEFAULT_DB)
    parser.add_argument('--spots-table',  default=DEFAULT_SPOTS_TABLE,
                        help=f'Destination spots table (default: {DEFAULT_SPOTS_TABLE})')
    parser.add_argument('--noise-table',  default=DEFAULT_NOISE_TABLE,
                        help=f'Destination noise table (default: {DEFAULT_NOISE_TABLE})')
    parser.add_argument('--batch-size',   default=DEFAULT_BATCH_SIZE, type=int,
                        help=f'Rows per CH insert (default: {DEFAULT_BATCH_SIZE})')
    parser.add_argument('--limit',        default=0, type=int,
                        help='Stop after N tbz files (0 = unlimited, for testing)')
    parser.add_argument('--dry-run',      action='store_true',
                        help='Parse only, no database inserts')
    parser.add_argument('--state-file',   default=STATE_FILE,
                        help='Progress state file for resumable runs')
    parser.add_argument('--reset',        action='store_true',
                        help='Ignore state file and restart from scratch')
    parser.add_argument('-v', '--verbose', action='count', default=0)
    args = parser.parse_args()

    setup_logging(args.verbose)
    log(f"=== tar-bulk-loader v{VERSION} ===", "INFO")

    # Collect tar files to process
    if args.tar:
        tar_files = [Path(args.tar)]
    else:
        tar_dir = Path(args.tar_dir)
        tar_files = sorted(tar_dir.glob('*.tar'))

    if not tar_files:
        print(f"No tar files found")
        sys.exit(1)

    log(f"Found {len(tar_files)} tar file(s)", "INFO")

    # Load state
    state = {} if args.reset else load_state(args.state_file)
    completed = set(state.get("completed_tars", []))
    grand_spots = state.get("total_spots", 0)
    grand_noise = state.get("total_noise", 0)
    grand_tbz   = state.get("total_tbz",   0)

    # Connect to CH
    client = None
    if not args.dry_run:
        try:
            client = clickhouse_connect.get_client(
                host=args.clickhouse_host, port=args.clickhouse_port,
                username=args.clickhouse_user, password=args.clickhouse_password)
            log("Connected to ClickHouse", "INFO")
        except Exception as e:
            log(f"Failed to connect to ClickHouse: {e}", "ERROR")
            sys.exit(1)
        ensure_staging_tables(client, args.db,
                               args.spots_table, args.noise_table,
                               args.dry_run)

    # Process tar files
    for tar_path in tar_files:
        tar_name = str(tar_path)

        if tar_name in completed:
            log(f"SKIP (already done): {tar_path.name}", "INFO")
            continue

        print(f"\n{'='*60}")
        print(f"TAR: {tar_path.name}")
        print(f"{'='*60}")

        tar_spots = 0
        tar_noise = 0
        tar_tbz   = 0
        spots_buf: List[Dict] = []
        noise_buf: List[Dict] = []
        tbz_limit_hit = False

        try:
            with tarfile.open(tar_path, mode='r:') as outer:
                members = [m for m in outer.getmembers()
                           if m.isfile() and m.name.endswith('.tbz')]
                total_tbz = len(members)
                print(f"  Contains {total_tbz:,} tbz files")

                t_start = time.time()

                for idx, member in enumerate(members, 1):
                    # Extract tbz bytes in memory
                    try:
                        f = outer.extractfile(member)
                        if not f:
                            continue
                        tbz_data = f.read()
                    except Exception as e:
                        log(f"Error reading {member.name}: {e}", "WARNING")
                        continue

                    tbz_name = Path(member.name).name
                    s, n = process_tbz_in_memory(tbz_data, tbz_name)
                    spots_buf.extend(s)
                    noise_buf.extend(n)
                    tar_tbz   += 1
                    tar_spots += len(s)
                    tar_noise += len(n)

                    # Limit check (after processing so we get exactly N tbz)
                    if args.limit and grand_tbz + tar_tbz >= args.limit:
                        tbz_limit_hit = True
                        print(f"\n  --limit {args.limit} reached, stopping")
                        break

                    # Flush when buffer is large enough
                    if len(spots_buf) >= args.batch_size:
                        if not insert_batch(client,
                                            f'{args.db}.{args.spots_table}',
                                            spots_buf, args.batch_size,
                                            args.dry_run, "spots"):
                            print("ERROR: spots insert failed, aborting tar")
                            break
                        spots_buf = []

                    if len(noise_buf) >= args.batch_size:
                        if not insert_batch(client,
                                            f'{args.db}.{args.noise_table}',
                                            noise_buf, args.batch_size,
                                            args.dry_run, "noise"):
                            print("ERROR: noise insert failed, aborting tar")
                            break

                        noise_buf = []

                    # Progress every 1000 tbz files
                    if idx % 1000 == 0:
                        elapsed = time.time() - t_start
                        rate = idx / elapsed if elapsed > 0 else 0
                        eta  = (total_tbz - idx) / rate if rate > 0 else 0
                        print(f"  {idx:>6,}/{total_tbz:,} tbz  "
                              f"{tar_spots:>9,} spots  "
                              f"{tar_noise:>7,} noise  "
                              f"{rate:.0f} tbz/s  "
                              f"ETA {eta/60:.0f}m")

        except Exception as e:
            log(f"Error opening tar {tar_path.name}: {e}", "ERROR")
            continue

        # Flush remaining buffers
        if spots_buf:
            insert_batch(client, f'{args.db}.{args.spots_table}',
                         spots_buf, args.batch_size, args.dry_run, "spots")
        if noise_buf:
            insert_batch(client, f'{args.db}.{args.noise_table}',
                         noise_buf, args.batch_size, args.dry_run, "noise")

        grand_spots += tar_spots
        grand_noise += tar_noise
        grand_tbz   += tar_tbz

        elapsed = time.time() - t_start if 't_start' in dir() else 0
        print(f"\n  Done: {tar_tbz:,} tbz  {tar_spots:,} spots  "
              f"{tar_noise:,} noise  ({elapsed:.0f}s)")

        # Mark tar complete unless we hit the limit mid-tar
        if not tbz_limit_hit:
            completed.add(tar_name)
            state["completed_tars"] = list(completed)

        state["total_spots"] = grand_spots
        state["total_noise"] = grand_noise
        state["total_tbz"]   = grand_tbz
        save_state(state, args.state_file)

        if tbz_limit_hit:
            break

    print(f"\n{'='*60}")
    print(f"GRAND TOTAL")
    print(f"{'='*60}")
    print(f"  tbz files : {grand_tbz:,}")
    print(f"  spots     : {grand_spots:,}")
    print(f"  noise     : {grand_noise:,}")
    print(f"  State file: {args.state_file}")


if __name__ == '__main__':
    main()
