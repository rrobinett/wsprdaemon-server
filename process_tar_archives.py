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

VERSION = "1.0.0"

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


def parse_wsprd_output(file_path: Path, client_version: Optional[str]) -> List[Dict]:
    """Parse wsprd output file and return list of spot records"""
    spots = []
    
    try:
        with open(file_path, 'r') as f:
            for line_num, line in enumerate(f, 1):
                line = line.strip()
                if not line:
                    continue
                
                parts = line.split()
                if len(parts) < 10:
                    continue
                
                try:
                    # Parse basic fields
                    spot = {
                        'date': parts[0],      # YYMMDD
                        'time': parts[1],      # HHMM
                        'snr': int(parts[2]),
                        'dt': float(parts[3]),
                        'freq_hz': float(parts[4]),
                        'drift': int(parts[5]),
                        'callsign': parts[6],
                        'grid': parts[7],
                        'power_dbm': int(parts[8]),
                        'distance': int(parts[9]) if len(parts) > 9 else 0,
                    }
                    
                    # Extended fields from wsprd 3.x+
                    if len(parts) >= 14:
                        spot.update({
                            'azimuth': int(parts[10]),
                            'c2_noise': float(parts[11]),
                            'jitter': int(parts[12]),
                            'blocksize': int(parts[13]),
                        })
                    
                    # Even more extended fields
                    if len(parts) >= 22:
                        spot.update({
                            'sync_quality': int(parts[14]),
                            'decode_cycles': int(parts[15]),
                            'rms_noise': float(parts[16]),
                            'ov_count': int(parts[17]),
                            'metric': int(parts[18]),
                            'osd_decode': int(parts[19]),
                            'nhardmin': int(parts[20]),
                            'ipass': int(parts[21]),
                        })
                    
                    if client_version:
                        spot['client_version'] = client_version
                    
                    spots.append(spot)
                    
                except (ValueError, IndexError):
                    continue
    
    except Exception as e:
        log(f"Error reading {file_path}: {e}", "ERROR")
    
    return spots


def process_spot_files(extraction_dir: Path, client_version: Optional[str]) -> List[Dict]:
    """Process all spot files (*_wsprd.txt or *_spots.txt) and return spot records"""
    all_spots = []
    
    # Find spot files - match both _wsprd.txt and _spots.txt patterns
    spot_files = list(extraction_dir.glob('*_wsprd.txt'))
    spot_files.extend(extraction_dir.glob('*_spots.txt'))
    spot_files = [f for f in spot_files if f.name != 'uploads_config.txt']
    
    if not spot_files:
        return []
    
    for spot_file in spot_files:
        # Extract rx_id and band from filename
        # Try patterns: RX_ID,BAND_wsprd.txt or RX_ID,BAND_spots.txt
        match = re.match(r'([^,]+),(\d+)_(?:wsprd|spots)\.txt', spot_file.name)
        if not match:
            continue
        
        rx_id = match.group(1)
        band = int(match.group(2))
        
        spots = parse_wsprd_output(spot_file, client_version)
        
        # Add rx_id and band to each spot
        for spot in spots:
            spot['rx_id'] = rx_id
            spot['band'] = band
            # Use rx_id as rx_sign (we don't have the lookup table for archived data)
            spot['rx_sign'] = rx_id
            spot['rx_loc'] = ''
            spot['rx_lat'] = 0.0
            spot['rx_lon'] = 0.0
        
        all_spots.extend(spots)
    
    return all_spots


def convert_spot_to_clickhouse(spot: Dict) -> Dict:
    """Convert a spot record to ClickHouse format"""
    date_str = spot['date']  # YYMMDD
    time_str = spot['time']  # HHMM
    
    year = 2000 + int(date_str[0:2])
    month = int(date_str[2:4])
    day = int(date_str[4:6])
    hour = int(time_str[0:2])
    minute = int(time_str[2:4])
    
    timestamp = datetime(year, month, day, hour, minute)
    
    freq_hz = spot['freq_hz']
    freq_mhz = freq_hz / 1_000_000.0
    
    ch_record = {
        'time': timestamp,
        'band': spot['band'],
        'rx_sign': spot['rx_sign'],
        'rx_lat': spot['rx_lat'],
        'rx_lon': spot['rx_lon'],
        'rx_loc': spot['rx_loc'],
        'tx_sign': spot['callsign'],
        'tx_loc': spot['grid'],
        'distance': spot['distance'],
        'frequency': int(freq_hz),
        'frequency_mhz': freq_mhz,
        'power': spot['power_dbm'],
        'snr': spot['snr'],
        'drift': spot['drift'],
        'rx_id': spot['rx_id'],
        'dt': spot['dt'],
    }
    
    # Add extended fields if present
    for field in ['azimuth', 'c2_noise', 'jitter', 'blocksize', 'sync_quality',
                  'decode_cycles', 'rms_noise', 'ov_count', 'metric', 'osd_decode',
                  'nhardmin', 'ipass']:
        if field in spot:
            ch_record[field] = spot[field]
    
    if 'client_version' in spot:
        ch_record['version'] = spot['client_version']
    
    return ch_record


def process_noise_files(extraction_dir: Path) -> List[Dict]:
    """Process noise files and return noise records"""
    noise_records = []
    
    noise_files = list(extraction_dir.glob('*_noise.txt'))
    
    if not noise_files:
        return []
    
    for noise_file in noise_files:
        match = re.match(r'([^,]+),(\d+)_noise\.txt', noise_file.name)
        if not match:
            continue
        
        rx_id = match.group(1)
        band = int(match.group(2))
        
        try:
            with open(noise_file, 'r') as f:
                for line in f:
                    line = line.strip()
                    if not line:
                        continue
                    
                    parts = line.split()
                    if len(parts) < 4:
                        continue
                    
                    date_str = parts[0]  # YYMMDD
                    time_str = parts[1]  # HHMM
                    freq_hz = int(parts[2])
                    noise_level = float(parts[3])
                    
                    year = 2000 + int(date_str[0:2])
                    month = int(date_str[2:4])
                    day = int(date_str[4:6])
                    hour = int(time_str[0:2])
                    minute = int(time_str[2:4])
                    
                    timestamp = datetime(year, month, day, hour, minute)
                    
                    noise_records.append({
                        'time': timestamp,
                        'rx_id': rx_id,
                        'band_m': band,
                        'freq_hz': freq_hz,
                        'noise_level': noise_level,
                        'noise_count': 1
                    })
        
        except Exception as e:
            log(f"Error processing noise file {noise_file.name}: {e}", "WARNING")
    
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
