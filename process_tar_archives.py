#!/usr/bin/env python3
"""
Process tar archives containing .tbz files and import spots/noise data into ClickHouse

Usage: python3 process_tar_archives.py --clickhouse-user <user> --clickhouse-password <pass> --tar-file <file.tar>
       python3 process_tar_archives.py --clickhouse-user <user> --clickhouse-password <pass> --tar-dir <dir>
"""

import argparse
import os
import re
import sys
import tarfile
import tempfile
import shutil
from pathlib import Path
from datetime import datetime
from typing import Dict, List, Optional, Tuple
import clickhouse_connect

VERSION = "2.0.0"  # Fixed: 34-field spot parser, rglob directory structure, correct noise schema

# Default configuration
DEFAULT_CONFIG = {
    'clickhouse_host': 'localhost',
    'clickhouse_port': 8123,
    'clickhouse_database': 'wsprdaemon',
    'clickhouse_spots_table': 'spots_extended',
    'clickhouse_noise_table': 'noise',
    'max_spots_per_insert': 50000,
    'max_noise_per_insert': 50000,
}


def log(message: str, level: str = "INFO"):
    """Simple logging to stderr"""
    timestamp = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    print(f"[{timestamp}] {level}: {message}", file=sys.stderr)
    sys.stderr.flush()


def extract_tbz(tbz_data, extraction_dir: Path) -> bool:
    """Extract a .tbz file from bytes to the extraction directory"""
    try:
        import io
        with tarfile.open(fileobj=io.BytesIO(tbz_data), mode='r:bz2') as tar:
            tar.extractall(path=extraction_dir)
        return True
    except Exception as e:
        log(f"Failed to extract tbz: {e}", "ERROR")
        return False


def extract_tbz_from_path(tbz_path: Path, extraction_dir: Path) -> bool:
    """Extract a .tbz file from disk to the extraction directory"""
    try:
        with tarfile.open(tbz_path, 'r:bz2') as tar:
            tar.extractall(path=extraction_dir)
        return True
    except Exception as e:
        log(f"Failed to extract {tbz_path}: {e}", "ERROR")
        return False


def get_client_version(extraction_dir: Path) -> Tuple[Optional[str], Optional[str], Optional[str]]:
    """Extract CLIENT_VERSION, RUNNING_JOBS, and RECEIVER_DESCRIPTIONS from uploads_config.txt"""
    config_file = extraction_dir / "uploads_config.txt"
    if not config_file.exists():
        return None, None, None
    
    client_version = None
    running_jobs = None
    receiver_descriptions = None
    
    try:
        with open(config_file, 'r') as f:
            for line in f:
                line = line.strip()
                if line.startswith('CLIENT_VERSION='):
                    client_version = line.split('=', 1)[1].strip('"\'')
                elif line.startswith('RUNNING_JOBS='):
                    running_jobs = line.split('=', 1)[1].strip('"\'')
                elif line.startswith('RECEIVER_DESCRIPTIONS='):
                    receiver_descriptions = line.split('=', 1)[1].strip()
    except Exception as e:
        log(f"Error reading uploads_config.txt: {e}", "WARNING")
    
    return client_version, running_jobs, receiver_descriptions


def band_str_to_meters(band_str: str) -> Optional[int]:
    """Convert a band string to metres.

    Handles numeric bands and named variants used by wsprdaemon:
        '17'   -> 17   '60eu' -> 60   '80eu' -> 80
    Returns None for unrecognised strings.
    """
    m = re.match(r'^(\d+)', band_str)
    if m:
        return int(m.group(1))
    return None


def decode_rx_site_dir(rx_site_dir: str) -> Tuple[str, str]:
    """Decode a RX_SITE directory name into (rx_sign, rx_grid).

    wsprdaemon encodes as:  CALLSIGN=SUFFIX_GRID  e.g. AC0G=ND_EN16ov
    where '=' replaces '/' in the callsign.
    Returns (rx_sign, rx_grid); falls back to (raw, '') if format not recognised.
    """
    m = re.match(r'^(.+)_([A-Ra-r]{2}[0-9]{2}[A-Xa-x]{0,2})$', rx_site_dir)
    if m:
        return m.group(1).replace('=', '/'), m.group(2)
    return rx_site_dir.replace('=', '/'), ''


def parse_wsprd_output(file_path: Path, client_version: Optional[str]) -> List[Dict]:
    """Parse a wsprdaemon extended spot file and return a list of spot records.

    Each line has exactly 34 space-separated fields produced by
    create_enhanced_spots_file_and_queue_to_posting_daemon() in decoding.sh
    (output_field_name_list order):

      0  spot_date                YYMMDD
      1  spot_time                HHMM
      2  spot_sync_quality        float
      3  spot_snr                 int dB
      4  spot_dt                  float seconds
      5  spot_freq                float MHz
      6  spot_call  (tx_sign)     string
      7  spot_grid  (tx_loc)      string (or 'none')
      8  spot_pwr                 int dBm
      9  spot_drift               int Hz/min
     10  spot_cycles              int
     11  spot_jitter              int
     12  spot_blocksize           int
     13  spot_metric              int
     14  spot_decodetype          int
     15  spot_ipass               int
     16  spot_nhardmin            int
     17  spot_pkt_mode (code)     int
     18  wspr_cycle_rms_noise     float dBm  (sox RMS)
     19  wspr_cycle_fft_noise     float dBm  (C2 FFT)
     20  band                     int metres
     21  real_receiver_grid       string (rx_loc)
     22  real_receiver_call_sign  string (rx_sign)
     23  km (distance)            int
     24  rx_az (rx_azimuth)       float degrees
     25  rx_lat                   float
     26  rx_lon                   float
     27  tx_az (azimuth)          float degrees
     28  tx_lat                   float
     29  tx_lon                   float
     30  v_lat                    float
     31  v_lon                    float
     32  wspr_cycle_kiwi_overloads_count  int
     33  proxy_upload_this_spot   int
    """
    spots = []

    try:
        with open(file_path, 'r') as f:
            for line_num, line in enumerate(f, 1):
                line = line.strip()
                if not line:
                    continue

                parts = line.split()
                if len(parts) < 34:
                    log(f"Skipping short line {line_num} ({len(parts)} fields) in "
                        f"{file_path.name}: {line}", "DEBUG")
                    continue

                try:
                    spot = {
                        'date':          parts[0],
                        'time':          parts[1],
                        'sync_quality':  float(parts[2]),
                        'snr':           int(float(parts[3])),
                        'dt':            float(parts[4]),
                        'freq_hz':       float(parts[5]) * 1_000_000.0,  # MHz -> Hz
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
                        'rms_noise':     float(parts[18]),   # wspr_cycle_rms_noise
                        'c2_noise':      float(parts[19]),   # wspr_cycle_fft_noise
                        'band_m':        int(float(parts[20])),
                        'rx_loc':        parts[21],
                        'rx_sign_file':  parts[22],          # authoritative rx callsign
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

                except (ValueError, IndexError) as e:
                    log(f"Error parsing line {line_num} in {file_path.name}: {e} | {line}",
                        "DEBUG")
                    continue

    except Exception as e:
        log(f"Error reading {file_path}: {e}", "ERROR")

    return spots
def process_spot_files(extraction_dir: Path, client_version: Optional[str]) -> List[Dict]:
    """Process all spot files inside an extracted tbz and return spot records.

    Expected directory structure:
        wsprdaemon/spots/RX_SITE/RECEIVER/BAND/YYMMDD_HHMM_spots.txt

    rx_sign and rx_loc come from the parsed spot line (fields 22 and 21).
    Directory-decoded values are used only as fallbacks.
    """
    all_spots = []

    spots_root = extraction_dir / 'wsprdaemon' / 'spots'
    if not spots_root.exists():
        return []

    wsprd_files = list(spots_root.rglob('*_spots.txt'))
    if not wsprd_files:
        return []

    for wsprd_file in wsprd_files:
        rel_parts = wsprd_file.relative_to(spots_root).parts
        if len(rel_parts) < 4:
            log(f"Skipping spot file with unexpected path depth: {wsprd_file}", "WARNING")
            continue

        rx_site_dir = rel_parts[0]
        rx_id       = rel_parts[1]
        band_str    = rel_parts[2]

        band = band_str_to_meters(band_str)
        if band is None:
            log(f"Skipping spot file with unrecognised band '{band_str}': {wsprd_file}",
                "WARNING")
            continue

        rx_sign_dir, rx_grid_dir = decode_rx_site_dir(rx_site_dir)

        spots = parse_wsprd_output(wsprd_file, client_version)

        for spot in spots:
            spot['rx_id']  = rx_id
            spot['band']   = band
            spot['rx_sign'] = spot.pop('rx_sign_file', None) or rx_sign_dir
            if not spot.get('rx_loc'):
                spot['rx_loc'] = rx_grid_dir

        all_spots.extend(spots)

    return all_spots


def convert_spot_to_clickhouse(spot: Dict) -> Dict:
    """Convert a parsed spot record to a ClickHouse insert dict."""
    date_str = spot['date']   # YYMMDD
    time_str = spot['time']   # HHMM

    year   = 2000 + int(date_str[0:2])
    month  = int(date_str[2:4])
    day    = int(date_str[4:6])
    hour   = int(time_str[0:2])
    minute = int(time_str[2:4])
    timestamp = datetime(year, month, day, hour, minute)

    freq_hz  = spot['freq_hz']
    freq_mhz = freq_hz / 1_000_000.0

    ch_record = {
        'time':          timestamp,
        'band':          spot['band'],
        'rx_sign':       spot['rx_sign'],
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
        'rx_id':         spot['rx_id'],
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
        'band_m':        spot.get('band_m', spot.get('band', 0)),
        'version':       spot.get('client_version', None),
        'rx_status':     'No Info',
    }

    return ch_record


def process_noise_files(extraction_dir: Path) -> List[Dict]:
    """Process noise files inside an extracted tbz and return noise records.

    Expected directory structure:
        wsprdaemon/noise/RX_SITE/RECEIVER/BAND/YYMMDD_HHMM_noise.txt

    File content: exactly 15 space-separated values (NOISE_LINE_FIELDS_COUNT=15):
        fields  0-11  12 sox dB measurements (3 windows x 4 stats)
        field   12    rms_level  — calibrated RMS noise (sox, Float32 dBm)
        field   13    c2_level   — calibrated C2/FFT noise (Float32 dBm)
        field   14    ov         — A/D overload count (Int32)

    Maps to noise table columns: time, site, receiver, rx_loc, band (String),
                                  rms_level, c2_level, ov
    """
    noise_records = []

    noise_root = extraction_dir / 'wsprdaemon' / 'noise'
    if not noise_root.exists():
        return []

    noise_files = list(noise_root.rglob('*_noise.txt'))
    if not noise_files:
        return []

    for noise_file in noise_files:
        rel_parts = noise_file.relative_to(noise_root).parts
        if len(rel_parts) < 4:
            log(f"Skipping noise file with unexpected path depth: {noise_file}", "WARNING")
            continue

        rx_site_dir = rel_parts[0]
        rx_id       = rel_parts[1]   # maps to 'receiver' column
        band_str    = rel_parts[2]   # stored as String: '17', '60eu', etc.

        rx_sign_dir, rx_grid_dir = decode_rx_site_dir(rx_site_dir)

        m = re.match(r'(\d{6})_(\d{4})_noise\.txt', noise_file.name)
        if not m:
            log(f"Skipping noise file with unexpected name: {noise_file.name}", "WARNING")
            continue

        date_str = m.group(1)
        time_str = m.group(2)
        try:
            timestamp = datetime(
                2000 + int(date_str[0:2]), int(date_str[2:4]), int(date_str[4:6]),
                int(time_str[0:2]), int(time_str[2:4])
            )
        except ValueError as e:
            log(f"Skipping noise file with bad timestamp {noise_file.name}: {e}", "WARNING")
            continue

        try:
            content = noise_file.read_text().strip()
            if not content:
                continue

            fields = content.split()
            if len(fields) != 15:
                log(f"Skipping noise file with {len(fields)} fields "
                    f"(expected 15): {noise_file.name}", "WARNING")
                continue

            noise_records.append({
                'time':      timestamp,
                'site':      rx_sign_dir,   # rx callsign  e.g. AC0G/ND
                'receiver':  rx_id,         # rx device id e.g. KA9Q_DXE
                'rx_loc':    rx_grid_dir,   # Maidenhead   e.g. EN16ov
                'band':      band_str,      # String       e.g. '17', '60eu'
                'rms_level': float(fields[12]),
                'c2_level':  float(fields[13]),
                'ov':        int(float(fields[14])),
            })

        except Exception as e:
            log(f"Error processing noise file {noise_file}: {e}", "WARNING")

    return noise_records


def insert_spots(client, spots: List[Dict], database: str, table: str,
                max_per_insert: int = 50000) -> bool:
    """Insert spots into ClickHouse in batches"""
    if not spots:
        return True
    
    try:
        ch_records = [convert_spot_to_clickhouse(spot) for spot in spots]
        
        total = len(ch_records)
        for i in range(0, total, max_per_insert):
            batch = ch_records[i:i+max_per_insert]
            client.insert(f'{database}.{table}', batch)
        
        return True
        
    except Exception as e:
        log(f"Error inserting spots: {e}", "ERROR")
        return False


def insert_noise(client, noise_records: List[Dict], database: str, table: str,
                max_per_insert: int = 50000) -> bool:
    """Insert noise records into ClickHouse in batches"""
    if not noise_records:
        return True
    
    try:
        total = len(noise_records)
        for i in range(0, total, max_per_insert):
            batch = noise_records[i:i+max_per_insert]
            client.insert(f'{database}.{table}', batch)
        
        return True
        
    except Exception as e:
        log(f"Error inserting noise: {e}", "ERROR")
        return False


def process_single_tbz(tbz_path: Path, extraction_dir: Path, client, config: Dict,
                       dry_run: bool = False) -> Tuple[int, int]:
    """Process a single .tbz file and return (spots_count, noise_count)"""
    
    # Clean extraction directory
    if extraction_dir.exists():
        shutil.rmtree(extraction_dir)
    extraction_dir.mkdir(parents=True, exist_ok=True)
    
    # Extract
    if not extract_tbz_from_path(tbz_path, extraction_dir):
        return 0, 0
    
    # Get client version
    client_version, running_jobs, receiver_descriptions = get_client_version(extraction_dir)
    
    # Process spots
    spots = process_spot_files(extraction_dir, client_version)
    spots_count = len(spots)
    
    if spots and not dry_run:
        insert_spots(client, spots, config['clickhouse_database'],
                    config['clickhouse_spots_table'], config['max_spots_per_insert'])
    
    # Process noise
    noise_records = process_noise_files(extraction_dir)
    noise_count = len(noise_records)
    
    if noise_records and not dry_run:
        insert_noise(client, noise_records, config['clickhouse_database'],
                    config['clickhouse_noise_table'], config['max_noise_per_insert'])
    
    return spots_count, noise_count


def process_tar_archive(tar_path: Path, client, config: Dict, 
                        dry_run: bool = False, progress_interval: int = 100) -> Tuple[int, int, int]:
    """
    Process a tar archive containing .tbz files
    Returns (tbz_count, total_spots, total_noise)
    """
    log(f"Processing tar archive: {tar_path.name}", "INFO")
    
    tbz_count = 0
    total_spots = 0
    total_noise = 0
    
    # Create temporary directory for extraction
    with tempfile.TemporaryDirectory(prefix='wsprd_archive_') as tmpdir:
        extraction_dir = Path(tmpdir) / 'extract'
        extraction_dir.mkdir()
        
        try:
            with tarfile.open(tar_path, 'r') as tar:
                members = [m for m in tar.getmembers() if m.name.endswith('.tbz')]
                total_tbz = len(members)
                log(f"Found {total_tbz} .tbz files in archive", "INFO")
                
                for i, member in enumerate(members):
                    try:
                        # Extract the tbz file to temp location
                        tar.extract(member, path=tmpdir)
                        tbz_file = Path(tmpdir) / member.name
                        
                        # Process the tbz
                        spots_count, noise_count = process_single_tbz(
                            tbz_file, extraction_dir, client, config, dry_run
                        )
                        
                        tbz_count += 1
                        total_spots += spots_count
                        total_noise += noise_count
                        
                        # Clean up extracted tbz
                        if tbz_file.exists():
                            tbz_file.unlink()
                        
                        # Progress report
                        if (i + 1) % progress_interval == 0:
                            log(f"Progress: {i+1}/{total_tbz} tbz files processed "
                                f"({total_spots} spots, {total_noise} noise records)", "INFO")
                    
                    except Exception as e:
                        log(f"Error processing {member.name}: {e}", "WARNING")
                        continue
        
        except Exception as e:
            log(f"Error reading tar archive {tar_path}: {e}", "ERROR")
            return 0, 0, 0
    
    log(f"Completed {tar_path.name}: {tbz_count} tbz files, "
        f"{total_spots} spots, {total_noise} noise records", "INFO")
    
    return tbz_count, total_spots, total_noise


def main():
    parser = argparse.ArgumentParser(
        description='Process tar archives containing .tbz files and import to ClickHouse'
    )
    parser.add_argument('--clickhouse-user', required=True, help='ClickHouse username')
    parser.add_argument('--clickhouse-password', required=True, help='ClickHouse password')
    parser.add_argument('--clickhouse-host', default='localhost', help='ClickHouse host')
    parser.add_argument('--clickhouse-port', type=int, default=8123, help='ClickHouse port')
    parser.add_argument('--database', default='wsprdaemon', help='ClickHouse database')
    parser.add_argument('--tar-file', help='Single tar file to process')
    parser.add_argument('--tar-dir', help='Directory containing tar files to process')
    parser.add_argument('--dry-run', action='store_true', help='Parse files but do not insert to database')
    parser.add_argument('--progress', type=int, default=100, help='Progress report interval (tbz files)')
    parser.add_argument('--version', action='version', version=f'%(prog)s {VERSION}')
    
    args = parser.parse_args()
    
    if not args.tar_file and not args.tar_dir:
        parser.error("Must specify either --tar-file or --tar-dir")
    
    # Build config
    config = DEFAULT_CONFIG.copy()
    config['clickhouse_host'] = args.clickhouse_host
    config['clickhouse_port'] = args.clickhouse_port
    config['clickhouse_database'] = args.database
    
    # Connect to ClickHouse
    if not args.dry_run:
        try:
            client = clickhouse_connect.get_client(
                host=config['clickhouse_host'],
                port=config['clickhouse_port'],
                username=args.clickhouse_user,
                password=args.clickhouse_password
            )
            log("Connected to ClickHouse", "INFO")
        except Exception as e:
            log(f"Failed to connect to ClickHouse: {e}", "ERROR")
            sys.exit(1)
    else:
        client = None
        log("DRY RUN - no database inserts will be performed", "INFO")
    
    # Get list of tar files to process
    tar_files = []
    if args.tar_file:
        tar_files.append(Path(args.tar_file))
    if args.tar_dir:
        tar_dir = Path(args.tar_dir)
        tar_files.extend(sorted(tar_dir.glob('*.tar')))
    
    if not tar_files:
        log("No tar files found", "ERROR")
        sys.exit(1)
    
    log(f"Found {len(tar_files)} tar files to process", "INFO")
    
    # Process each tar file
    grand_total_tbz = 0
    grand_total_spots = 0
    grand_total_noise = 0
    
    for tar_file in tar_files:
        if not tar_file.exists():
            log(f"Tar file not found: {tar_file}", "WARNING")
            continue
        
        tbz_count, spots_count, noise_count = process_tar_archive(
            tar_file, client, config, args.dry_run, args.progress
        )
        
        grand_total_tbz += tbz_count
        grand_total_spots += spots_count
        grand_total_noise += noise_count
    
    log("=" * 60, "INFO")
    log(f"GRAND TOTAL: {grand_total_tbz} tbz files, "
        f"{grand_total_spots} spots, {grand_total_noise} noise records", "INFO")


if __name__ == '__main__':
    main()
