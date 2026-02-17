#!/usr/bin/env python3
"""
WSPRDAEMON Server - Process .tbz files from wsprdaemon clients
Usage: wsprdaemon_server.py --clickhouse-user <user> --clickhouse-password <pass> [options]
"""

import argparse
import json
import sys
import time
import os
import re
import tarfile
import shutil
import subprocess
from pathlib import Path
from typing import Dict, List, Optional, Tuple
from datetime import datetime
import clickhouse_connect
import logging

# Version
VERSION = "2.14.0"  # Added: system setup for tmpfiles.d and required directories

# Default configuration
DEFAULT_CONFIG = {
    'clickhouse_host': 'localhost',
    'clickhouse_port': 8123,
    'clickhouse_user': '',
    'clickhouse_password': '',
    'clickhouse_database': 'wsprdaemon',
    'clickhouse_spots_table': 'spots_extended',
    'clickhouse_noise_table': 'noise',
    'incoming_tbz_dirs': [],  # Must be specified via --incoming-dirs
    'extraction_dir': '/var/lib/wsprdaemon/extraction',
    'processed_tbz_file': '/var/lib/wsprdaemon/wsprdaemon/processed_tbz_list.txt',
    'max_processed_file_size': 1000000,
    'max_spots_per_insert': 50000,
    'max_noise_per_insert': 50000,
    'loop_interval': 10
}

# Logging configuration
LOG_FILE = 'wsprdaemon_server.log'
LOG_MAX_BYTES = 10 * 1024 * 1024  # 10MB
LOG_KEEP_RATIO = 0.75

class TruncatingFileHandler(logging.FileHandler):
    """File handler that truncates to newest 75% when file grows too large"""

    def __init__(self, filename, max_bytes, keep_ratio=0.75):
        self.max_bytes = max_bytes
        self.keep_ratio = keep_ratio
        super().__init__(filename, mode='a', encoding='utf-8')

    def emit(self, record):
        """Emit a record, truncating file if needed"""
        super().emit(record)
        self.check_truncate()

    def check_truncate(self):
        """Check file size and truncate if needed"""
        try:
            if os.path.exists(self.baseFilename):
                current_size = os.path.getsize(self.baseFilename)
                if current_size > self.max_bytes:
                    self.truncate_file()
        except Exception as e:
            print(f"Error checking log file size: {e}")

    def truncate_file(self):
        """Keep only the newest 75% of the file"""
        try:
            with open(self.baseFilename, 'r', encoding='utf-8') as f:
                lines = f.readlines()

            keep_count = int(len(lines) * self.keep_ratio)
            if keep_count < 1:
                keep_count = 1

            new_lines = lines[-keep_count:]

            with open(self.baseFilename, 'w', encoding='utf-8') as f:
                f.write(f"[Log truncated - kept newest {self.keep_ratio*100:.0f}% of {len(lines)} lines]\n")
                f.writelines(new_lines)

            old_size = sum(len(line.encode('utf-8')) for line in lines)
            new_size = os.path.getsize(self.baseFilename)
            logging.info(f"Log file truncated from {old_size:,} to {new_size:,} bytes")

        except Exception as e:
            print(f"Error truncating log file: {e}")


def setup_logging(log_file=None, max_bytes=LOG_MAX_BYTES, keep_ratio=LOG_KEEP_RATIO, verbosity=0):
    """Setup logging - either to file OR console, not both
    
    verbosity levels:
        0: WARNING and ERROR only
        1: INFO + WARNING + ERROR
        2+: DEBUG + INFO + WARNING + ERROR
    """
    logger = logging.getLogger()
    
    # Set level based on verbosity
    if verbosity == 0:
        logger.setLevel(logging.WARNING)
    elif verbosity == 1:
        logger.setLevel(logging.INFO)
    else:
        logger.setLevel(logging.DEBUG)
    
    logger.handlers.clear()

    if log_file:
        file_handler = TruncatingFileHandler(log_file, max_bytes, keep_ratio)
        file_formatter = logging.Formatter('[%(asctime)s] %(levelname)s: %(message)s',
                                          datefmt='%Y-%m-%d %H:%M:%S')
        file_handler.setFormatter(file_formatter)
        logger.addHandler(file_handler)
    else:
        console_handler = logging.StreamHandler()
        console_formatter = logging.Formatter('[%(asctime)s] %(levelname)s: %(message)s',
                                             datefmt='%Y-%m-%d %H:%M:%S')
        console_handler.setFormatter(console_formatter)
        logger.addHandler(console_handler)

    return logger


def log(message: str, level: str = "INFO"):
    """Log a message at the specified level"""
    logger = logging.getLogger()
    level_map = {
        'DEBUG': logging.DEBUG,
        'INFO': logging.INFO,
        'WARNING': logging.WARNING,
        'ERROR': logging.ERROR,
        'CRITICAL': logging.CRITICAL
    }
    logger.log(level_map.get(level, logging.INFO), message)
    # Force flush to disk
    for handler in logger.handlers:
        handler.flush()


def setup_system_directories(wspr_user: str = "wsprdaemon", wspr_group: str = "wsprdaemon") -> bool:
    """Setup system directories and systemd-tmpfiles.d configuration
    
    This ensures all required directories exist and persist across reboots.
    Must be run as root during installation/setup.
    """
    try:
        import pwd
        import grp
        
        # Get UID and GID
        try:
            uid = pwd.getpwnam(wspr_user).pw_uid
            gid = grp.getgrnam(wspr_group).gr_gid
        except KeyError:
            log(f"User {wspr_user} or group {wspr_group} doesn't exist - skipping system setup", "WARNING")
            return True  # Don't fail if running as non-root during development
        
        # Required directories
        required_dirs = [
            "/var/log/wsprdaemon",
            "/var/lib/wsprdaemon",
            "/var/spool/wsprdaemon",
            "/tmp/wsprdaemon"
        ]
        
        log("Setting up system directories...", "INFO")
        for dir_path in required_dirs:
            path = Path(dir_path)
            if not path.exists():
                log(f"Creating {dir_path}", "INFO")
                path.mkdir(parents=True, exist_ok=True)
                os.chown(dir_path, uid, gid)
                os.chmod(dir_path, 0o755)
            else:
                log(f"Directory exists: {dir_path}", "DEBUG")
        
        # Create systemd-tmpfiles.d configuration
        # This ensures /tmp/wsprdaemon is created on every boot
        tmpfiles_conf = Path("/etc/tmpfiles.d/wsprdaemon.conf")
        if not tmpfiles_conf.exists():
            log("Creating systemd-tmpfiles.d configuration...", "INFO")
            tmpfiles_content = f"""# WSPRDAEMON temporary directories
# Type  Path              Mode  User         Group        Age  Argument
d       /tmp/wsprdaemon   0755  {wspr_user}  {wspr_group}  -    -
"""
            tmpfiles_conf.write_text(tmpfiles_content)
            
            # Apply the configuration immediately
            try:
                subprocess.run(
                    ["systemd-tmpfiles", "--create", str(tmpfiles_conf)],
                    check=True,
                    capture_output=True
                )
                log("Applied tmpfiles.d configuration", "INFO")
            except subprocess.CalledProcessError as e:
                log(f"Warning: Could not apply tmpfiles.d: {e}", "WARNING")
            except FileNotFoundError:
                log("Warning: systemd-tmpfiles command not found", "WARNING")
        else:
            log("tmpfiles.d configuration already exists", "DEBUG")
        
        # Create logrotate configuration
        logrotate_conf = Path("/etc/logrotate.d/wsprdaemon")
        if not logrotate_conf.exists():
            log("Creating logrotate configuration...", "INFO")
            logrotate_content = f"""/var/log/wsprdaemon/*.log {{
    daily
    rotate 30
    compress
    delaycompress
    missingok
    notifempty
    create 0644 {wspr_user} {wspr_group}
    sharedscripts
    postrotate
        systemctl reload wsprdaemon_server@*.service > /dev/null 2>&1 || true
    endscript
}}
"""
            logrotate_conf.write_text(logrotate_content)
            log("Created logrotate configuration", "INFO")
        else:
            log("logrotate configuration already exists", "DEBUG")
        
        log("System setup complete", "INFO")
        return True
        
    except PermissionError:
        log("Permission denied - system setup requires root privileges", "WARNING")
        log("Run with sudo to set up system directories and tmpfiles.d", "WARNING")
        return True  # Don't fail - directories might already exist
    except Exception as e:
        log(f"Error during system setup: {e}", "ERROR")
        return False


def setup_clickhouse_tables(admin_user: str, admin_password: str,
                           config: Dict) -> bool:
    """Setup ClickHouse database and tables (assumes admin user already exists)"""
    try:
        # Connect as admin user directly
        log(f"Connecting to ClickHouse as {admin_user}...", "INFO")
        admin_client = clickhouse_connect.get_client(
            host=config['clickhouse_host'],
            port=config['clickhouse_port'],
            username=admin_user,
            password=admin_password
        )

        # Create database if not exists
        result = admin_client.query(
            f"SELECT 1 FROM system.databases WHERE name = '{config['clickhouse_database']}'"
        )
        if not result.result_rows:
            log(f"Creating database {config['clickhouse_database']}...", "INFO")
            admin_client.command(f"CREATE DATABASE {config['clickhouse_database']}")
            log(f"Database {config['clickhouse_database']} created", "INFO")
        else:
            log(f"Database {config['clickhouse_database']} already exists", "INFO")

        # Create spots table - HARMONIZED with wsprnet.spots schema
        create_spots_table_sql = f"""
        CREATE TABLE IF NOT EXISTS {config['clickhouse_database']}.{config['clickhouse_spots_table']}
        (
            -- Harmonized columns matching wsprnet.spots schema (with aliases for old names)
            id           Nullable(UInt64)        CODEC(Delta(8), ZSTD(1)),
            time         DateTime                CODEC(Delta(4), ZSTD(1)),
            band         Int16                   CODEC(T64, ZSTD(1)),
            rx_sign      LowCardinality(String)  CODEC(LZ4),
            rx_lat       Float32                 CODEC(Delta(4), ZSTD(3)),
            rx_lon       Float32                 CODEC(Delta(4), ZSTD(3)),
            rx_loc       LowCardinality(String)  CODEC(LZ4),
            tx_sign      LowCardinality(String)  CODEC(LZ4),
            tx_lat       Float32                 CODEC(Delta(4), ZSTD(3)),
            tx_lon       Float32                 CODEC(Delta(4), ZSTD(3)),
            tx_loc       LowCardinality(String)  CODEC(LZ4),
            distance     Int32                   CODEC(T64, ZSTD(1)),
            azimuth      Int32                   CODEC(T64, ZSTD(1)),
            rx_azimuth   Int32                   CODEC(T64, ZSTD(1)),
            frequency    UInt64                  CODEC(Delta(8), ZSTD(3)),
            power        Int8                    CODEC(T64, ZSTD(1)),
            snr          Int8                    CODEC(Delta(4), ZSTD(3)),
            drift        Int8                    CODEC(Delta(4), ZSTD(3)),
            version      LowCardinality(Nullable(String)) CODEC(LZ4),
            code         Int8                    CODEC(ZSTD(1)),
            
            -- Wsprdaemon-specific additional fields
            frequency_mhz Float64                CODEC(Delta(8), ZSTD(3)),
            rx_id        LowCardinality(String)  CODEC(LZ4),
            v_lat        Float32                 CODEC(Delta(4), ZSTD(3)),
            v_lon        Float32                 CODEC(Delta(4), ZSTD(3)),
            c2_noise     Float32                 CODEC(Delta(4), ZSTD(3)),
            sync_quality UInt16                  CODEC(ZSTD(1)),
            dt           Float32                 CODEC(Delta(4), ZSTD(3)),
            decode_cycles UInt32                 CODEC(T64, ZSTD(1)),
            jitter       Int16                   CODEC(T64, ZSTD(1)),
            rms_noise    Float32                 CODEC(Delta(4), ZSTD(3)),
            blocksize    UInt16                  CODEC(T64, ZSTD(1)),
            metric       Int16                   CODEC(T64, ZSTD(1)),
            osd_decode   UInt8                   CODEC(T64, ZSTD(1)),
            nhardmin     UInt16                  CODEC(T64, ZSTD(1)),
            ipass        UInt8                   CODEC(T64, ZSTD(1)),
            proxy_upload UInt8                   CODEC(T64, ZSTD(1)),
            ov_count     UInt32                  CODEC(T64, ZSTD(1)),
            rx_status    LowCardinality(String) DEFAULT 'No Info' CODEC(LZ4),
            band_m       Int16                   CODEC(T64, ZSTD(1)),
            
            -- Compatibility aliases for wsprnet.org queries
            Spotnum      UInt64  ALIAS id,
            Date         UInt32  ALIAS toUnixTimestamp(time),
            Reporter     String  ALIAS rx_sign,
            ReporterGrid String  ALIAS rx_loc,
            dB           Int8    ALIAS snr,
            freq         Float64 ALIAS frequency_mhz,
            MHz          Float64 ALIAS frequency_mhz,
            CallSign     String  ALIAS tx_sign,
            Grid         String  ALIAS tx_loc,
            Power        Int8    ALIAS power,
            Drift        Int8    ALIAS drift,
            Band         Int16   ALIAS band,
            rx_az        UInt16  ALIAS rx_azimuth,
            frequency_hz UInt64  ALIAS frequency
        )
        ENGINE = MergeTree()
        PARTITION BY toYYYYMM(time)
        ORDER BY time
        SETTINGS index_granularity = 8192
        """
        admin_client.command(create_spots_table_sql)
        log(f"Table {config['clickhouse_database']}.{config['clickhouse_spots_table']} ready", "INFO")

        # Create noise table
        create_noise_table_sql = f"""
        CREATE TABLE IF NOT EXISTS {config['clickhouse_database']}.{config['clickhouse_noise_table']}
        (
            time         DateTime                CODEC(Delta(4), ZSTD(1)),
            rx_id        LowCardinality(String)  CODEC(LZ4),
            rx_sign      LowCardinality(String)  CODEC(LZ4),
            rx_grid      LowCardinality(String)  CODEC(LZ4),
            band_m       Int16                   CODEC(T64, ZSTD(1)),
            freq_hz      UInt64                  CODEC(Delta(8), ZSTD(3)),
            noise_level  Float32                 CODEC(Delta(4), ZSTD(3)),
            noise_count  UInt32                  CODEC(T64, ZSTD(1))
        )
        ENGINE = MergeTree()
        PARTITION BY toYYYYMM(time)
        ORDER BY (rx_id, time, band_m)
        SETTINGS index_granularity = 8192
        """
        admin_client.command(create_noise_table_sql)
        log(f"Table {config['clickhouse_database']}.{config['clickhouse_noise_table']} ready", "INFO")

        return True

    except Exception as e:
        log(f"Setup failed: {e}", "ERROR")
        import traceback
        traceback.print_exc()
        return False


def find_tbz_files(dirs: List[str]) -> List[Path]:
    """Find all .tbz files in the specified directories"""
    tbz_files = []
    for directory in dirs:
        dir_path = Path(directory)
        if dir_path.exists() and dir_path.is_dir():
            tbz_files.extend(dir_path.glob('*.tbz'))
    return sorted(tbz_files)


def is_tbz_processed(tbz_file: Path, processed_file: Path) -> bool:
    """Check if a .tbz file has already been processed"""
    if not processed_file.exists():
        return False
    
    try:
        with open(processed_file, 'r') as f:
            processed = set(line.strip() for line in f if line.strip())
        return str(tbz_file) in processed
    except Exception as e:
        log(f"Error reading processed file: {e}", "WARNING")
        return False


def mark_tbz_processed(tbz_file: Path, processed_file: Path, max_size: int):
    """Mark a .tbz file as processed, truncating list if it gets too large"""
    processed_file.parent.mkdir(parents=True, exist_ok=True)
    
    # Read existing entries
    entries = []
    if processed_file.exists():
        try:
            with open(processed_file, 'r') as f:
                entries = [line.strip() for line in f if line.strip()]
        except Exception as e:
            log(f"Error reading processed file: {e}", "WARNING")
    
    # Add new entry
    entries.append(str(tbz_file))
    
    # Truncate if too large (keep newest 75%)
    current_size = sum(len(e.encode('utf-8')) + 1 for e in entries)  # +1 for newline
    if current_size > max_size:
        keep_count = int(len(entries) * 0.75)
        if keep_count < 1:
            keep_count = 1
        entries = entries[-keep_count:]
        log(f"Truncated processed file to {keep_count} entries", "INFO")
    
    # Write back
    try:
        with open(processed_file, 'w') as f:
            for entry in entries:
                f.write(f"{entry}\n")
    except Exception as e:
        log(f"Error writing processed file: {e}", "ERROR")


def extract_tbz(tbz_file: Path, extraction_dir: Path) -> bool:
    """Extract a .tbz file to the extraction directory"""
    try:
        with tarfile.open(tbz_file, 'r:bz2') as tar:
            tar.extractall(path=extraction_dir)
        return True
    except Exception as e:
        log(f"Failed to extract {tbz_file.name}: {e}", "ERROR")
        return False


def get_client_version(extraction_dir: Path) -> Tuple[Optional[str], Optional[str], Optional[str]]:
    """Extract CLIENT_VERSION, RUNNING_JOBS, and RECEIVER_DESCRIPTIONS from uploads_config.txt
    
    Returns:
        Tuple of (client_version, running_jobs, receiver_descriptions)
        All can be None if not found
    """
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
                    # This is a bash array, extract the content
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
                    log(f"Skipping malformed line {line_num} in {file_path.name}: {line}", "DEBUG")
                    continue
                
                try:
                    # Parse basic fields (common to all wsprd versions)
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
                    
                    # Extended fields from wsprd 3.x+ (if available)
                    if len(parts) >= 14:
                        spot.update({
                            'azimuth': int(parts[10]),
                            'c2_noise': float(parts[11]),
                            'jitter': int(parts[12]),
                            'blocksize': int(parts[13]),
                        })
                    
                    # Even more extended fields from latest wsprd
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
                    
                    # Add client version if available
                    if client_version:
                        spot['client_version'] = client_version
                    
                    spots.append(spot)
                    
                except (ValueError, IndexError) as e:
                    log(f"Error parsing line {line_num} in {file_path.name}: {e}", "DEBUG")
                    continue
    
    except Exception as e:
        log(f"Error reading {file_path}: {e}", "ERROR")
    
    return spots


def process_spot_files(extraction_dir: Path, client_version: Optional[str],
                      client, database: str, table: str) -> List[Dict]:
    """Process all wsprd output files and return combined spot records"""
    all_spots = []
    
    # Find all wsprd output files
    wsprd_files = list(extraction_dir.glob('*.txt'))
    wsprd_files = [f for f in wsprd_files if f.name != 'uploads_config.txt']
    
    if not wsprd_files:
        log("No wsprd output files found", "DEBUG")
        return []
    
    log(f"Processing {len(wsprd_files)} wsprd output files", "DEBUG")
    
    for wsprd_file in wsprd_files:
        # Extract rx_id and band from filename
        # Format: RX_ID,BAND_wsprd.txt or similar
        match = re.match(r'([^,]+),(\d+)_wsprd\.txt', wsprd_file.name)
        if not match:
            log(f"Skipping file with unexpected name: {wsprd_file.name}", "WARNING")
            continue
        
        rx_id = match.group(1)
        band = int(match.group(2))
        
        # Parse the file
        spots = parse_wsprd_output(wsprd_file, client_version)
        
        # Get receiver info from ClickHouse (rx_sign, rx_loc, rx_lat, rx_lon)
        try:
            result = client.query(f"""
                SELECT rx_sign, rx_loc, rx_lat, rx_lon
                FROM {database}.{table}
                WHERE rx_id = '{rx_id}'
                ORDER BY time DESC
                LIMIT 1
            """)
            
            if result.result_rows:
                rx_sign, rx_loc, rx_lat, rx_lon = result.result_rows[0]
            else:
                # Fallback: use rx_id as rx_sign if not found
                rx_sign = rx_id
                rx_loc = ''
                rx_lat = 0.0
                rx_lon = 0.0
                log(f"No receiver info found for {rx_id}, using rx_id as rx_sign", "DEBUG")
        except Exception as e:
            log(f"Error getting receiver info for {rx_id}: {e}", "DEBUG")
            rx_sign = rx_id
            rx_loc = ''
            rx_lat = 0.0
            rx_lon = 0.0
        
        # Add rx_id, band, and receiver info to each spot
        for spot in spots:
            spot['rx_id'] = rx_id
            spot['band'] = band
            spot['rx_sign'] = rx_sign
            spot['rx_loc'] = rx_loc
            spot['rx_lat'] = rx_lat
            spot['rx_lon'] = rx_lon
        
        all_spots.extend(spots)
    
    log(f"Processed {len(all_spots)} total spots", "DEBUG")
    return all_spots


def convert_spot_to_clickhouse(spot: Dict) -> Dict:
    """Convert a spot record to ClickHouse format"""
    # Parse date/time
    date_str = spot['date']  # YYMMDD
    time_str = spot['time']  # HHMM
    
    # Convert YYMMDD to YYYY-MM-DD
    year = 2000 + int(date_str[0:2])
    month = int(date_str[2:4])
    day = int(date_str[4:6])
    hour = int(time_str[0:2])
    minute = int(time_str[2:4])
    
    timestamp = datetime(year, month, day, hour, minute)
    
    # Convert frequency to MHz
    freq_hz = spot['freq_hz']
    freq_mhz = freq_hz / 1_000_000.0
    
    # Build ClickHouse record
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
    if 'azimuth' in spot:
        ch_record['azimuth'] = spot['azimuth']
    if 'c2_noise' in spot:
        ch_record['c2_noise'] = spot['c2_noise']
    if 'jitter' in spot:
        ch_record['jitter'] = spot['jitter']
    if 'blocksize' in spot:
        ch_record['blocksize'] = spot['blocksize']
    if 'sync_quality' in spot:
        ch_record['sync_quality'] = spot['sync_quality']
    if 'decode_cycles' in spot:
        ch_record['decode_cycles'] = spot['decode_cycles']
    if 'rms_noise' in spot:
        ch_record['rms_noise'] = spot['rms_noise']
    if 'ov_count' in spot:
        ch_record['ov_count'] = spot['ov_count']
    if 'metric' in spot:
        ch_record['metric'] = spot['metric']
    if 'osd_decode' in spot:
        ch_record['osd_decode'] = spot['osd_decode']
    if 'nhardmin' in spot:
        ch_record['nhardmin'] = spot['nhardmin']
    if 'ipass' in spot:
        ch_record['ipass'] = spot['ipass']
    if 'client_version' in spot:
        ch_record['version'] = spot['client_version']
    
    return ch_record


def insert_spots(client, spots: List[Dict], database: str, table: str, 
                max_per_insert: int = 50000) -> bool:
    """Insert spots into ClickHouse in batches"""
    if not spots:
        return True
    
    try:
        # Convert spots to ClickHouse format
        ch_records = [convert_spot_to_clickhouse(spot) for spot in spots]
        
        # Insert in batches
        total = len(ch_records)
        for i in range(0, total, max_per_insert):
            batch = ch_records[i:i+max_per_insert]
            client.insert(f'{database}.{table}', batch)
            log(f"Inserted batch {i//max_per_insert + 1} ({len(batch)} spots)", "DEBUG")
        
        return True
        
    except Exception as e:
        log(f"Error inserting spots: {e}", "ERROR")
        import traceback
        traceback.print_exc()
        return False


def process_noise_files(extraction_dir: Path, running_jobs: Optional[str],
                       receiver_descriptions: Optional[str]) -> List[Dict]:
    """Process noise files and return noise records"""
    noise_records = []
    
    # Find all noise files
    noise_files = list(extraction_dir.glob('*_noise.txt'))
    
    if not noise_files:
        log("No noise files found", "DEBUG")
        return []
    
    log(f"Processing {len(noise_files)} noise files", "DEBUG")
    
    for noise_file in noise_files:
        # Extract rx_id and band from filename
        match = re.match(r'([^,]+),(\d+)_noise\.txt', noise_file.name)
        if not match:
            log(f"Skipping noise file with unexpected name: {noise_file.name}", "WARNING")
            continue
        
        rx_id = match.group(1)
        band = int(match.group(2))
        
        # Parse noise file
        try:
            with open(noise_file, 'r') as f:
                for line in f:
                    line = line.strip()
                    if not line:
                        continue
                    
                    parts = line.split()
                    if len(parts) < 4:
                        continue
                    
                    # Parse: YYMMDD HHMM freq_hz noise_level
                    date_str = parts[0]  # YYMMDD
                    time_str = parts[1]  # HHMM
                    freq_hz = int(parts[2])
                    noise_level = float(parts[3])
                    
                    # Convert date/time
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
                        'noise_count': 1  # Placeholder
                    })
        
        except Exception as e:
            log(f"Error processing noise file {noise_file.name}: {e}", "WARNING")
    
    log(f"Processed {len(noise_records)} noise records", "DEBUG")
    return noise_records


def insert_noise(client, noise_records: List[Dict], database: str, table: str,
                max_per_insert: int = 50000) -> bool:
    """Insert noise records into ClickHouse in batches"""
    if not noise_records:
        return True
    
    try:
        # Insert in batches
        total = len(noise_records)
        for i in range(0, total, max_per_insert):
            batch = noise_records[i:i+max_per_insert]
            client.insert(f'{database}.{table}', batch)
            log(f"Inserted noise batch {i//max_per_insert + 1} ({len(batch)} records)", "DEBUG")
        
        return True
        
    except Exception as e:
        log(f"Error inserting noise: {e}", "ERROR")
        import traceback
        traceback.print_exc()
        return False


def main():
    parser = argparse.ArgumentParser(description='WSPRDAEMON Server - Process .tbz files')
    parser.add_argument('--clickhouse-user', required=True, help='ClickHouse username')
    parser.add_argument('--clickhouse-password', required=True, help='ClickHouse password')
    parser.add_argument('--incoming-dirs', required=True,
                       help='Comma-separated list of directories to monitor for .tbz files')
    parser.add_argument('--config', help='Path to JSON config file (optional)')
    parser.add_argument('--loop', type=int, metavar='SECONDS',
                       help='Loop interval in seconds (default: run once and exit)')
    parser.add_argument('--log-file', help='Log file path (default: console output)')
    parser.add_argument('-v', '--verbose', action='count', default=0,
                       help='Increase verbosity (use -v for INFO, -vv for DEBUG)')
    parser.add_argument('--setup-system', action='store_true',
                       help='Set up system directories and tmpfiles.d (requires root)')
    parser.add_argument('--version', action='version', version=f'%(prog)s {VERSION}')
    
    args = parser.parse_args()

    # Setup logging (either to file OR console)
    setup_logging(log_file=args.log_file, verbosity=args.verbose)
    
    log("=== WSPRDAEMON Server Starting ===", "INFO")
    log(f"Version: {VERSION}", "INFO")
    log(f"Verbosity level: {args.verbose}", "INFO")

    # Run system setup if requested
    if args.setup_system:
        log("Running system setup...", "INFO")
        if not setup_system_directories():
            log("System setup failed", "ERROR")
            sys.exit(1)
        log("System setup completed successfully", "INFO")
        if not args.loop:
            # If just doing setup, exit
            sys.exit(0)

    # Load configuration
    config = DEFAULT_CONFIG.copy()
    if args.config:
        with open(args.config) as f:
            config.update(json.load(f))

    # Override with command line credentials
    config['clickhouse_user'] = args.clickhouse_user
    config['clickhouse_password'] = args.clickhouse_password

    # Parse incoming directories from command line
    config['incoming_tbz_dirs'] = [d.strip() for d in args.incoming_dirs.split(',') if d.strip()]
    if not config['incoming_tbz_dirs']:
        log("ERROR: No valid incoming directories specified", "ERROR")
        sys.exit(1)

    log(f"Incoming directories: {config['incoming_tbz_dirs']}", "INFO")

    # Always run setup to ensure database and tables exist
    log("Running setup to ensure ClickHouse is configured...", "INFO")
    success = setup_clickhouse_tables(
        admin_user=args.clickhouse_user,
        admin_password=args.clickhouse_password,
        config=config
    )

    if not success:
        log("Setup failed - cannot continue", "ERROR")
        sys.exit(1)

    # Connect to ClickHouse
    try:
        client = clickhouse_connect.get_client(
            host=config['clickhouse_host'],
            port=config['clickhouse_port'],
            username=config['clickhouse_user'],
            password=config['clickhouse_password']
        )
        log("Connected to ClickHouse", "INFO")
    except Exception as e:
        log(f"Failed to connect to ClickHouse: {e}", "ERROR")
        sys.exit(1)

    # Ensure extraction directory exists
    extraction_dir = Path(config['extraction_dir'])
    extraction_dir.mkdir(parents=True, exist_ok=True)

    processed_file = Path(config['processed_tbz_file'])

    # Main loop
    loop_count = 0
    while True:
        loop_count += 1
        log(f"=== Processing cycle {loop_count} ===", "INFO")

        # Find .tbz files
        tbz_files = find_tbz_files(config['incoming_tbz_dirs'])
        
        if not tbz_files:
            log("No .tbz files found", "INFO")
            if not args.loop:
                break
            log(f"Sleeping {args.loop} seconds...", "DEBUG")
            time.sleep(args.loop)
            continue

        log(f"Found {len(tbz_files)} .tbz files", "INFO")

        # Filter out already processed files
        unprocessed = [f for f in tbz_files if not is_tbz_processed(f, processed_file)]
        
        # Clean up zombie files (exist on disk but already marked processed)
        zombies = [f for f in tbz_files if is_tbz_processed(f, processed_file)]
        if zombies:
            log(f"Found {len(zombies)} zombie files (marked processed but not deleted)", "INFO")
            for zombie in zombies:
                try:
                    zombie.unlink()
                    log(f"Deleted zombie: {zombie.name}", "DEBUG")
                except Exception as e:
                    log(f"Failed to delete zombie {zombie.name}: {e}", "WARNING")
            log(f"Cleaned up {len(zombies)} zombie files", "INFO")
        
        if not unprocessed:
            log("All .tbz files have been processed", "INFO")
            if not args.loop:
                break
            log(f"Sleeping {args.loop} seconds...", "DEBUG")
            time.sleep(args.loop)
            continue

        log(f"Found {len(unprocessed)} unprocessed .tbz files", "INFO")

        # Process each .tbz file
        for tbz_file in unprocessed:
            log(f"Processing {tbz_file.name}...", "INFO")

            # Clean extraction directory
            if extraction_dir.exists():
                shutil.rmtree(extraction_dir)
            extraction_dir.mkdir(parents=True, exist_ok=True)

            # Extract
            if not extract_tbz(tbz_file, extraction_dir):
                log(f"Failed to extract {tbz_file.name} (corrupt?), deleting", "ERROR")
                try:
                    tbz_file.unlink()
                    log(f"Deleted corrupt file: {tbz_file.name}", "WARNING")
                except Exception as e:
                    log(f"Failed to delete corrupt file {tbz_file.name}: {e}", "ERROR")
                continue

            # Get CLIENT_VERSION, RUNNING_JOBS, and RECEIVER_DESCRIPTIONS from uploads_config.txt
            client_version, running_jobs, receiver_descriptions = get_client_version(extraction_dir)
            if client_version:
                log(f"Using CLIENT_VERSION: {client_version}", "DEBUG")
            else:
                log("No CLIENT_VERSION found in uploads_config.txt", "DEBUG")

            # Process spots with retry
            spots = process_spot_files(extraction_dir, client_version, 
                                      client, config['clickhouse_database'], 
                                      config['clickhouse_spots_table'])
            if spots:
                max_retries = 3
                retry_delay = 2  # seconds
                success = False
                
                for attempt in range(max_retries):
                    if insert_spots(client, spots, config['clickhouse_database'], 
                                  config['clickhouse_spots_table'], config['max_spots_per_insert']):
                        log(f"Successfully processed {len(spots)} spots from {tbz_file.name}", "INFO")
                        success = True
                        break
                    else:
                        if attempt < max_retries - 1:
                            log(f"Failed to insert spots from {tbz_file.name}, retrying in {retry_delay} seconds... (attempt {attempt+1}/{max_retries})", "WARNING")
                            time.sleep(retry_delay)
                            retry_delay *= 2  # Exponential backoff
                        else:
                            log(f"Failed to insert spots from {tbz_file.name} after {max_retries} attempts", "ERROR")
                
                if not success:
                    log(f"Skipping {tbz_file.name} - will retry on next cycle", "WARNING")
                    continue

            # Process noise with retry
            noise_records = process_noise_files(extraction_dir, running_jobs, receiver_descriptions)
            if noise_records:
                max_retries = 3
                retry_delay = 2  # seconds
                success = False
                
                for attempt in range(max_retries):
                    if insert_noise(client, noise_records, config['clickhouse_database'],
                                  config['clickhouse_noise_table'], config['max_noise_per_insert']):
                        log(f"Successfully processed {len(noise_records)} noise records from {tbz_file.name}", "INFO")
                        success = True
                        break
                    else:
                        if attempt < max_retries - 1:
                            log(f"Failed to insert noise from {tbz_file.name}, retrying in {retry_delay} seconds... (attempt {attempt+1}/{max_retries})", "WARNING")
                            time.sleep(retry_delay)
                            retry_delay *= 2  # Exponential backoff
                        else:
                            log(f"Failed to insert noise from {tbz_file.name} after {max_retries} attempts", "ERROR")
                
                if not success:
                    log(f"Skipping {tbz_file.name} - will retry on next cycle", "WARNING")
                    continue

            # Mark as processed
            mark_tbz_processed(tbz_file, processed_file, config['max_processed_file_size'])

            # Delete source .tbz file
            try:
                tbz_file.unlink()
                log(f"Deleted {tbz_file.name}", "INFO")
            except Exception as e:
                log(f"Failed to delete {tbz_file.name}: {e}", "WARNING")

        # Clean up extraction directory
        if extraction_dir.exists():
            shutil.rmtree(extraction_dir)

        if not args.loop:
            break

        log(f"Sleeping {args.loop} seconds...", "DEBUG")
        time.sleep(args.loop)


if __name__ == '__main__':
    main()
