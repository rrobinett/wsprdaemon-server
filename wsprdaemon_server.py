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
VERSION = "2.20.0"  # Fixed: O(n^2) processed-file scan replaced with single load per cycle

# Default configuration
DEFAULT_CONFIG = {
    'clickhouse_host': 'localhost',
    'clickhouse_port': 8123,
    'clickhouse_user': '',
    'clickhouse_password': '',
    'clickhouse_database': 'wsprdaemon',
    'clickhouse_spots_table': 'spots',
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

        # Create spots table
        #   - azimuth/rx_azimuth are Float32 (not Int32 — fractional degrees matter)
        #   - id column omitted (no upstream source in tbz data)
        #   - ALIAS columns omitted (add manually if needed)
        #   - ReplacingMergeTree so re-runs are idempotent
        create_spots_table_sql = f"""
        CREATE TABLE IF NOT EXISTS {config['clickhouse_database']}.{config['clickhouse_spots_table']}
        (
            time          DateTime                          CODEC(Delta(4), ZSTD(1)),
            band          Int16                             CODEC(T64, ZSTD(1)),
            rx_sign       LowCardinality(String)            CODEC(LZ4),
            rx_lat        Float32                           CODEC(Delta(4), ZSTD(3)),
            rx_lon        Float32                           CODEC(Delta(4), ZSTD(3)),
            rx_loc        LowCardinality(String)            CODEC(LZ4),
            tx_sign       LowCardinality(String)            CODEC(LZ4),
            tx_lat        Float32                           CODEC(Delta(4), ZSTD(3)),
            tx_lon        Float32                           CODEC(Delta(4), ZSTD(3)),
            tx_loc        LowCardinality(String)            CODEC(LZ4),
            distance      Int32                             CODEC(T64, ZSTD(1)),
            azimuth       Float32                           CODEC(Delta(4), ZSTD(3)),
            rx_azimuth    Float32                           CODEC(Delta(4), ZSTD(3)),
            frequency     UInt64                            CODEC(Delta(8), ZSTD(3)),
            power         Int8                              CODEC(T64, ZSTD(1)),
            snr           Int8                              CODEC(Delta(4), ZSTD(3)),
            drift         Int8                              CODEC(Delta(4), ZSTD(3)),
            version       LowCardinality(Nullable(String))  CODEC(LZ4),
            code          Int8                              CODEC(ZSTD(1)),
            frequency_mhz Float64                           CODEC(Delta(8), ZSTD(3)),
            rx_id         LowCardinality(String)            CODEC(LZ4),
            v_lat         Float32                           CODEC(Delta(4), ZSTD(3)),
            v_lon         Float32                           CODEC(Delta(4), ZSTD(3)),
            c2_noise      Float32                           CODEC(Delta(4), ZSTD(3)),
            sync_quality  UInt16                            CODEC(ZSTD(1)),
            dt            Float32                           CODEC(Delta(4), ZSTD(3)),
            decode_cycles UInt32                            CODEC(T64, ZSTD(1)),
            jitter        Int16                             CODEC(T64, ZSTD(1)),
            rms_noise     Float32                           CODEC(Delta(4), ZSTD(3)),
            blocksize     UInt16                            CODEC(T64, ZSTD(1)),
            metric        Int16                             CODEC(T64, ZSTD(1)),
            osd_decode    UInt8                             CODEC(T64, ZSTD(1)),
            nhardmin      UInt16                            CODEC(T64, ZSTD(1)),
            ipass         UInt8                             CODEC(T64, ZSTD(1)),
            proxy_upload  UInt8                             CODEC(T64, ZSTD(1)),
            ov_count      UInt32                            CODEC(T64, ZSTD(1)),
            rx_status     LowCardinality(String) DEFAULT 'No Info' CODEC(LZ4),
            band_m        Int16                             CODEC(T64, ZSTD(1))
        )
        ENGINE = ReplacingMergeTree()
        PARTITION BY toYYYYMM(time)
        ORDER BY (time, rx_sign, tx_sign, frequency)
        SETTINGS index_granularity = 8192
        """
        admin_client.command(create_spots_table_sql)
        log(f"Table {config['clickhouse_database']}.{config['clickhouse_spots_table']} ready", "INFO")

        # Create noise table
        #   site     = rx callsign from RX_SITE directory  (e.g. AC0G/ND)
        #   receiver = rx device from RECEIVER directory   (e.g. KA9Q_DXE)
        #   rx_loc   = Maidenhead grid from RX_SITE suffix (e.g. EN16ov)
        #   band     = band string from BAND directory     (e.g. '17', '60eu')
        #   rms_level / c2_level = fields[12] / fields[13] from the 15-field noise line
        #   ov       = fields[14], A/D overload count
        create_noise_table_sql = f"""
        CREATE TABLE IF NOT EXISTS {config['clickhouse_database']}.{config['clickhouse_noise_table']}
        (
            time       DateTime                CODEC(Delta(4), ZSTD(1)),
            site       LowCardinality(String)  CODEC(LZ4),
            receiver   LowCardinality(String)  CODEC(LZ4),
            rx_loc     LowCardinality(String)  CODEC(LZ4),
            band       LowCardinality(String)  CODEC(LZ4),
            rms_level  Float32                 CODEC(Delta(4), ZSTD(3)),
            c2_level   Float32                 CODEC(Delta(4), ZSTD(3)),
            ov         Int32                   CODEC(T64, ZSTD(1))
        )
        ENGINE = ReplacingMergeTree()
        PARTITION BY toYYYYMM(time)
        ORDER BY (time, site, receiver, band)
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


def load_processed_set(processed_file: Path) -> set:
    """Load the set of already-processed tbz file paths from disk."""
    if not processed_file.exists():
        return set()
    try:
        with open(processed_file, 'r') as f:
            return set(line.strip() for line in f if line.strip())
    except Exception as e:
        log(f"Error reading processed file: {e}", "WARNING")
        return set()


def is_tbz_processed(tbz_file: Path, processed_set: set) -> bool:
    """Check if a .tbz file has already been processed (uses pre-loaded set)."""
    return str(tbz_file) in processed_set


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
    """Parse a wsprdaemon extended spot file and return a list of spot records.

    The file is produced by decoding.sh create_enhanced_spots_file_and_queue_to_posting_daemon().
    Each line has exactly 34 space-separated fields in this order (defined by
    output_field_name_list in decoding.sh):

      0  spot_date                  YYMMDD
      1  spot_time                  HHMM
      2  spot_sync_quality          float
      3  spot_snr                   int   dB
      4  spot_dt                    float seconds
      5  spot_freq                  float MHz
      6  spot_call  (tx_sign)       string
      7  spot_grid  (tx_loc)        string  (or 'none' when absent)
      8  spot_pwr                   int   dBm
      9  spot_drift                 int   Hz/min
     10  spot_cycles (decode_cycles) int
     11  spot_jitter                int
     12  spot_blocksize             int
     13  spot_metric                int
     14  spot_decodetype (osd_decode) int
     15  spot_ipass                 int
     16  spot_nhardmin              int
     17  spot_pkt_mode (code)       int
     18  wspr_cycle_rms_noise       float dBm  (sox RMS measurement)
     19  wspr_cycle_fft_noise       float dBm  (C2 FFT measurement)
     20  band                       int   metres (e.g. 17, 20, 40)
     21  real_receiver_grid (rx_loc) string
     22  real_receiver_call_sign (rx_sign) string
     23  km   (distance)            int
     24  rx_az (rx_azimuth)         float degrees
     25  rx_lat                     float
     26  rx_lon                     float
     27  tx_az (azimuth)            float degrees
     28  tx_lat                     float
     29  tx_lon                     float
     30  v_lat                      float
     31  v_lon                      float
     32  wspr_cycle_kiwi_overloads_count (ov_count) int
     33  proxy_upload_this_spot     int  (0 or 1)
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
                        # Timing / identity
                        'date':          parts[0],            # YYMMDD
                        'time':          parts[1],            # HHMM
                        'sync_quality':  float(parts[2]),
                        'snr':           int(float(parts[3])),
                        'dt':            float(parts[4]),
                        'freq_hz':       float(parts[5]) * 1_000_000.0,  # MHz -> Hz
                        'tx_sign':       parts[6],
                        'tx_loc':        parts[7] if parts[7].lower() != 'none' else '',
                        'power_dbm':     int(float(parts[8])),
                        'drift':         int(float(parts[9])),
                        # Decoder quality metrics
                        'decode_cycles': int(float(parts[10])),
                        'jitter':        int(float(parts[11])),
                        'blocksize':     int(float(parts[12])),
                        'metric':        int(float(parts[13])),
                        'osd_decode':    int(float(parts[14])),
                        'ipass':         int(float(parts[15])),
                        'nhardmin':      int(float(parts[16])),
                        'code':          int(float(parts[17])),  # spot_pkt_mode
                        # Noise levels (RMS=sox, c2=FFT) — note field order in file
                        'rms_noise':     float(parts[18]),   # wspr_cycle_rms_noise
                        'c2_noise':      float(parts[19]),   # wspr_cycle_fft_noise
                        # Derived geo fields from add_derived()
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


def band_str_to_meters(band_str: str) -> Optional[int]:
    """Convert a band string to metres.  Handles numeric bands and named variants.

    wsprdaemon uses the band directory name as the band identifier, e.g.:
        '17'    -> 17  (17-metre band)
        '60eu'  -> 60  (European 60m allocation, treated as 60m)
        '80eu'  -> 80
    Returns None for unrecognised strings.
    """
    # Strip trailing non-numeric suffix (e.g. '60eu' -> '60')
    m = re.match(r'^(\d+)', band_str)
    if m:
        return int(m.group(1))
    return None


def decode_rx_site_dir(rx_site_dir: str) -> Tuple[str, str]:
    """Decode a RX_SITE directory name into (rx_sign, rx_grid).

    wsprdaemon encodes the callsign/grid as:
        CALLSIGN=SUFFIX_GRID   e.g.  AC0G=ND_EN16ov
    where '=' replaces '/' in the callsign and '_GRID' is appended.

    Returns (rx_sign, rx_grid).  If the format is not recognised both
    values default to the raw directory string / empty string.
    """
    # Split off the grid: last '_XXXXXX' segment (4–6 char Maidenhead)
    m = re.match(r'^(.+)_([A-Ra-r]{2}[0-9]{2}[A-Xa-x]{0,2})$', rx_site_dir)
    if m:
        sign_part = m.group(1).replace('=', '/')
        grid_part = m.group(2)
        return sign_part, grid_part
    # Fallback: no recognisable grid suffix
    return rx_site_dir.replace('=', '/'), ''


def process_spot_files(extraction_dir: Path, client_version: Optional[str],
                       client, database: str, table: str) -> List[Dict]:
    """Process all spot files inside an extracted tbz and return spot records.

    Expected directory structure inside the tbz:
        wsprdaemon/spots/RX_SITE/RECEIVER/BAND/YYMMDD_HHMM_spots.txt

    rx_sign and rx_loc come directly from the parsed spot line (fields 22 and 21).
    The directory-decoded values are used only as a fallback when those fields
    are absent or empty.
    """
    all_spots = []

    spots_root = extraction_dir / 'wsprdaemon' / 'spots'
    if not spots_root.exists():
        log("No spots directory found in tbz", "DEBUG")
        return []

    wsprd_files = list(spots_root.rglob('*_spots.txt'))
    if not wsprd_files:
        log("No spot files found", "DEBUG")
        return []

    log(f"Processing {len(wsprd_files)} spot files", "DEBUG")

    for wsprd_file in wsprd_files:
        # Path: spots_root / RX_SITE / RECEIVER / BAND / YYMMDD_HHMM_spots.txt
        rel_parts = wsprd_file.relative_to(spots_root).parts
        if len(rel_parts) < 4:
            log(f"Skipping spot file with unexpected path depth: {wsprd_file}", "WARNING")
            continue

        rx_site_dir = rel_parts[0]   # e.g. AC0G=ND_EN16ov
        rx_id       = rel_parts[1]   # e.g. KA9Q_DXE
        band_str    = rel_parts[2]   # e.g. '17', '60eu'

        band = band_str_to_meters(band_str)
        if band is None:
            log(f"Skipping spot file with unrecognised band '{band_str}': {wsprd_file}",
                "WARNING")
            continue

        # Decode directory-based identity (fallback values)
        rx_sign_dir, rx_grid_dir = decode_rx_site_dir(rx_site_dir)

        spots = parse_wsprd_output(wsprd_file, client_version)

        for spot in spots:
            spot['rx_id']  = rx_id
            spot['band']   = band
            # rx_sign_file (field 22) is the authoritative callsign written by decoding.sh
            spot['rx_sign'] = spot.pop('rx_sign_file', None) or rx_sign_dir
            # rx_loc (field 21) is already in spot; use dir value only if blank
            if not spot.get('rx_loc'):
                spot['rx_loc'] = rx_grid_dir

        all_spots.extend(spots)

    log(f"Processed {len(all_spots)} total spots", "DEBUG")
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
        'band':          spot['band'],          # metres, e.g. 17
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


def insert_spots(client, spots: List[Dict], database: str, table: str,
                max_per_insert: int = 50000) -> bool:
    """Insert spots into ClickHouse in batches"""
    if not spots:
        return True

    try:
        ch_records = [convert_spot_to_clickhouse(spot) for spot in spots]
        # Use column order from first record so clickhouse_connect receives
        # a list-of-lists with explicit column_names rather than keying by int
        column_names = list(ch_records[0].keys())
        total = len(ch_records)
        for i in range(0, total, max_per_insert):
            batch = ch_records[i:i+max_per_insert]
            data = [[row[col] for col in column_names] for row in batch]
            client.insert(f'{database}.{table}', data, column_names=column_names)
            log(f"Inserted batch {i//max_per_insert + 1} ({len(batch)} spots)", "DEBUG")

        return True

    except Exception as e:
        log(f"Error inserting spots: {e}", "ERROR")
        import traceback
        traceback.print_exc()
        return False


def process_noise_files(extraction_dir: Path, running_jobs: Optional[str],
                        receiver_descriptions: Optional[str]) -> List[Dict]:
    """Process noise files inside an extracted tbz and return noise records.

    Expected directory structure inside the tbz:
        wsprdaemon/noise/RX_SITE/RECEIVER/BAND/YYMMDD_HHMM_noise.txt

    File content (from noise-graphing.sh queue_noise_signal_levels_to_wsprdaemon):
        A single line of exactly 15 space-separated values:
          fields 0-12  : 13 sox dB measurement floats (3 windows × 4 stats + min RMS)
          field  13    : rms_level  — sox RMS noise after calibration (Float32 dBm)
          field  14    : c2_level   — C2/FFT noise after calibration  (Float32 dBm)
          field  15    : ov         — A/D overload count              (Int32)

    Wait — decoding.sh line 2414 builds:
        sox_signals_rms_fft_and_overload_info = "${rms_line} ${fft_noise_level_float} ${new_sdr_overloads_count}"
    where rms_line itself is 13 values (12 sox dB values + the selected rms scalar).
    Total = 13 + 1 + 1 = 15.  NOISE_LINE_FIELDS_COUNT = 15 confirms this.

    So field indices (0-based):
        [12]  rms_level  (the min-of-pre/post RMS Tr dB scalar, sox-calibrated)
        [13]  c2_level   (fft_noise_level_float, C2-calibrated)
        [14]  ov         (integer overload count)

    Noise table schema columns: time, site, receiver, rx_loc, band (String), rms_level, c2_level, ov
    """
    noise_records = []

    noise_root = extraction_dir / 'wsprdaemon' / 'noise'
    if not noise_root.exists():
        log("No noise directory found in tbz", "DEBUG")
        return []

    noise_files = list(noise_root.rglob('*_noise.txt'))
    if not noise_files:
        log("No noise files found", "DEBUG")
        return []

    log(f"Processing {len(noise_files)} noise files", "DEBUG")

    for noise_file in noise_files:
        # Path: noise_root / RX_SITE / RECEIVER / BAND / YYMMDD_HHMM_noise.txt
        rel_parts = noise_file.relative_to(noise_root).parts
        if len(rel_parts) < 4:
            log(f"Skipping noise file with unexpected path depth: {noise_file}", "WARNING")
            continue

        rx_site_dir = rel_parts[0]   # e.g. AC0G=ND_EN16ov
        rx_id       = rel_parts[1]   # RECEIVER dir  -> maps to 'receiver' column
        band_str    = rel_parts[2]   # e.g. '17', '60eu' -> stored as String in noise table

        rx_sign_dir, rx_grid_dir = decode_rx_site_dir(rx_site_dir)

        # Parse timestamp from filename: YYMMDD_HHMM_noise.txt
        m = re.match(r'(\d{6})_(\d{4})_noise\.txt', noise_file.name)
        if not m:
            log(f"Skipping noise file with unexpected name: {noise_file.name}", "WARNING")
            continue

        date_str = m.group(1)   # YYMMDD
        time_str = m.group(2)   # HHMM
        try:
            year   = 2000 + int(date_str[0:2])
            month  = int(date_str[2:4])
            day    = int(date_str[4:6])
            hour   = int(time_str[0:2])
            minute = int(time_str[2:4])
            timestamp = datetime(year, month, day, hour, minute)
        except ValueError as e:
            log(f"Skipping noise file with bad timestamp {noise_file.name}: {e}", "WARNING")
            continue

        # Parse the 15-field noise line
        try:
            with open(noise_file, 'r') as f:
                content = f.read().strip()
            if not content:
                continue

            fields = content.split()
            if len(fields) != 15:
                log(f"Skipping noise file with {len(fields)} fields "
                    f"(expected 15): {noise_file.name}", "WARNING")
                continue

            rms_level = float(fields[12])
            c2_level  = float(fields[13])
            ov        = int(float(fields[14]))

            noise_records.append({
                'time':       timestamp,
                'site':       rx_sign_dir,   # rx callsign  e.g. AC0G/ND
                'receiver':   rx_id,         # rx device id e.g. KA9Q_DXE
                'rx_loc':     rx_grid_dir,   # Maidenhead   e.g. EN16ov
                'band':       band_str,      # String       e.g. '17', '60eu'
                'rms_level':  rms_level,
                'c2_level':   c2_level,
                'ov':         ov,
                # Optional metadata from uploads_config.txt
            })

        except Exception as e:
            log(f"Error processing noise file {noise_file}: {e}", "WARNING")

    log(f"Processed {len(noise_records)} noise records", "DEBUG")
    return noise_records


def insert_noise(client, noise_records: List[Dict], database: str, table: str,
                max_per_insert: int = 50000) -> bool:
    """Insert noise records into ClickHouse in batches"""
    if not noise_records:
        return True

    try:
        column_names = list(noise_records[0].keys())
        total = len(noise_records)
        for i in range(0, total, max_per_insert):
            batch = noise_records[i:i+max_per_insert]
            data = [[row[col] for col in column_names] for row in batch]
            client.insert(f'{database}.{table}', data, column_names=column_names)
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
    parser.add_argument('--dry-run', action='store_true',
                       help='Parse tbz files but do not insert to database')
    parser.add_argument('--spots-table', default=None,
                       help='Override default spots table name')
    parser.add_argument('--noise-table', default=None,
                       help='Override default noise table name')
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

    # Apply table name overrides
    if args.spots_table:
        config['clickhouse_spots_table'] = args.spots_table
    if args.noise_table:
        config['clickhouse_noise_table'] = args.noise_table

    # Dry-run mode
    config['dry_run'] = args.dry_run
    if args.dry_run:
        log("DRY RUN mode - no database inserts will be performed", "INFO")

    # Parse incoming directories from command line
    config['incoming_tbz_dirs'] = [d.strip() for d in args.incoming_dirs.split(',') if d.strip()]
    if not config['incoming_tbz_dirs']:
        log("ERROR: No valid incoming directories specified", "ERROR")
        sys.exit(1)

    log(f"Incoming directories: {config['incoming_tbz_dirs']}", "INFO")

    # Run table setup (skip in dry-run mode)
    if not config.get('dry_run'):
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

        # Load processed set once — O(1) per file instead of O(n) disk reads
        processed_set = load_processed_set(processed_file)

        # Filter out already processed files
        unprocessed = [f for f in tbz_files if not is_tbz_processed(f, processed_set)]

        # Clean up zombie files (exist on disk but already marked processed)
        zombies = [f for f in tbz_files if is_tbz_processed(f, processed_set)]
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

            # Process spots
            spots = process_spot_files(extraction_dir, client_version,
                                      client, config['clickhouse_database'],
                                      config['clickhouse_spots_table'])
            log(f"Parsed {len(spots)} spots from {tbz_file.name}", "INFO")
            if spots and not config.get('dry_run'):
                max_retries = 3
                retry_delay = 2  # seconds
                success = False

                for attempt in range(max_retries):
                    if insert_spots(client, spots, config['clickhouse_database'],
                                  config['clickhouse_spots_table'], config['max_spots_per_insert']):
                        log(f"Inserted {len(spots)} spots from {tbz_file.name}", "INFO")
                        success = True
                        break
                    else:
                        if attempt < max_retries - 1:
                            log(f"Failed to insert spots from {tbz_file.name}, retrying in {retry_delay} seconds... (attempt {attempt+1}/{max_retries})", "WARNING")
                            time.sleep(retry_delay)
                            retry_delay *= 2
                        else:
                            log(f"Failed to insert spots from {tbz_file.name} after {max_retries} attempts", "ERROR")

                if not success:
                    log(f"Skipping {tbz_file.name} - will retry on next cycle", "WARNING")
                    continue

            # Process noise
            noise_records = process_noise_files(extraction_dir, running_jobs, receiver_descriptions)
            log(f"Parsed {len(noise_records)} noise records from {tbz_file.name}", "INFO")
            if noise_records and not config.get('dry_run'):
                max_retries = 3
                retry_delay = 2  # seconds
                success = False

                for attempt in range(max_retries):
                    if insert_noise(client, noise_records, config['clickhouse_database'],
                                  config['clickhouse_noise_table'], config['max_noise_per_insert']):
                        log(f"Inserted {len(noise_records)} noise records from {tbz_file.name}", "INFO")
                        success = True
                        break
                    else:
                        if attempt < max_retries - 1:
                            log(f"Failed to insert noise from {tbz_file.name}, retrying in {retry_delay} seconds... (attempt {attempt+1}/{max_retries})", "WARNING")
                            time.sleep(retry_delay)
                            retry_delay *= 2
                        else:
                            log(f"Failed to insert noise from {tbz_file.name} after {max_retries} attempts", "ERROR")
                
                if not success:
                    log(f"Skipping {tbz_file.name} - will retry on next cycle", "WARNING")
                    continue

            # Mark as processed and delete source (skip in dry-run)
            if not config.get('dry_run'):
                mark_tbz_processed(tbz_file, processed_file, config['max_processed_file_size'])
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
