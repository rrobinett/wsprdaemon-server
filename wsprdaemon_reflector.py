#!/usr/bin/env python3
"""
WSPRDAEMON Reflector - Distribute .tbz files from clients to multiple servers
Usage: wsprdaemon_reflector.py --config /etc/wsprdaemon/reflector_destinations.json [options]

Simple design:
  1. Scan for .tbz files in /home/*/uploads/
  2. Copy each file to temp name in each queue dir, then rename (atomic)
  3. If ALL queues successful -> delete source file
  4. Rsync workers sync queue dirs to destinations with --remove-source-files
"""

VERSION = "2.1.0"

import argparse
import json
import sys
import time
import os
import subprocess
import threading
import shutil
import glob
import signal
from pathlib import Path
from typing import Dict, List
import logging

# Default configuration
DEFAULT_CONFIG = {
    'incoming_pattern': '/home/*/uploads/*.tbz',
    'queue_base_dir': '/var/spool/wsprdaemon/reflector',
    'destinations': [],
    'scan_interval': 5,
    'rsync_interval': 5,
    'rsync_bandwidth_limit': 20000,  # KB/s
    'rsync_timeout': 300,
}

# Logging configuration
LOG_FILE = '/var/log/wsprdaemon/reflector.log'
LOG_MAX_BYTES = 10 * 1024 * 1024  # 10MB
LOG_KEEP_RATIO = 0.75


class TruncatingFileHandler(logging.FileHandler):
    """File handler that truncates to newest portion when file grows too large"""

    def __init__(self, filename, max_bytes, keep_ratio=0.75):
        self.max_bytes = max_bytes
        self.keep_ratio = keep_ratio
        super().__init__(filename, mode='a', encoding='utf-8')

    def emit(self, record):
        super().emit(record)
        self.check_truncate()

    def check_truncate(self):
        try:
            if os.path.exists(self.baseFilename):
                current_size = os.path.getsize(self.baseFilename)
                if current_size > self.max_bytes:
                    self.truncate_file()
        except Exception as e:
            print(f"Error checking log file size: {e}")

    def truncate_file(self):
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
        except Exception as e:
            print(f"Error truncating log file: {e}")


def setup_logging(log_file=None, max_bytes=LOG_MAX_BYTES, keep_ratio=LOG_KEEP_RATIO, verbosity=0):
    """Setup logging with verbosity levels"""
    logger = logging.getLogger()

    if verbosity == 0:
        logger.setLevel(logging.WARNING)
    elif verbosity == 1:
        logger.setLevel(logging.INFO)
    else:
        logger.setLevel(logging.DEBUG)

    logger.handlers.clear()

    if log_file:
        # Ensure log directory exists
        log_dir = os.path.dirname(log_file)
        if log_dir:
            os.makedirs(log_dir, exist_ok=True)
        file_handler = TruncatingFileHandler(log_file, max_bytes, keep_ratio)
        file_formatter = logging.Formatter(
            '[%(asctime)s] %(levelname)s: %(message)s',
            datefmt='%Y-%m-%d %H:%M:%S'
        )
        file_handler.setFormatter(file_formatter)
        logger.addHandler(file_handler)
    else:
        console_handler = logging.StreamHandler()
        console_formatter = logging.Formatter(
            '[%(asctime)s] %(levelname)s: %(message)s',
            datefmt='%Y-%m-%d %H:%M:%S'
        )
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
    for handler in logger.handlers:
        handler.flush()


def verify_destination_rsync(destination: Dict) -> bool:
    """Check if rsync is installed on destination, install if missing. Returns True if rsync is available."""
    name = destination['name']
    user = destination['user']
    host = destination['host']
    ssh_key = destination.get('ssh_key', '/home/wsprdaemon/.ssh/id_rsa')
    
    ssh_base = f"ssh -i {ssh_key} -o StrictHostKeyChecking=no -o ConnectTimeout=10 {user}@{host}"
    
    # Check if rsync exists
    log(f"Checking rsync on {name} ({host})...", "INFO")
    try:
        result = subprocess.run(
            f"{ssh_base} 'which rsync'",
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            universal_newlines=True,
            timeout=15,
            shell=True
        )
        if result.returncode == 0:
            log(f"{name}: rsync found at {result.stdout.strip()}", "INFO")
            return True
    except subprocess.TimeoutExpired:
        log(f"{name}: SSH connection timed out", "ERROR")
        return False
    except Exception as e:
        log(f"{name}: Error checking rsync: {e}", "ERROR")
        return False
    
    # rsync not found, try to install it
    log(f"{name}: rsync not found, attempting to install...", "WARNING")
    
    # Try apt-get (Debian/Ubuntu)
    try:
        result = subprocess.run(
            f"{ssh_base} 'sudo apt-get update -qq && sudo apt-get install -y -qq rsync'",
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            universal_newlines=True,
            timeout=120,
            shell=True
        )
        
        if result.returncode == 0:
            log(f"{name}: rsync installed successfully via apt-get", "INFO")
            return True
        else:
            log(f"{name}: apt-get install failed: {result.stderr.strip()}", "DEBUG")
    except subprocess.TimeoutExpired:
        log(f"{name}: rsync installation timed out", "ERROR")
    except Exception as e:
        log(f"{name}: Error installing rsync via apt-get: {e}", "DEBUG")
    
    # Try yum as fallback (RHEL/CentOS)
    try:
        result = subprocess.run(
            f"{ssh_base} 'sudo yum install -y rsync'",
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            universal_newlines=True,
            timeout=120,
            shell=True
        )
        
        if result.returncode == 0:
            log(f"{name}: rsync installed successfully via yum", "INFO")
            return True
    except:
        pass
    
    log(f"{name}: Could not install rsync - destination will be skipped", "ERROR")
    return False


def verify_all_destinations(config: Dict) -> List[Dict]:
    """Verify rsync on all destinations, return list of valid destinations"""
    valid_destinations = []
    
    for dest in config['destinations']:
        if verify_destination_rsync(dest):
            valid_destinations.append(dest)
        else:
            log(f"Destination {dest['name']} will be disabled due to missing rsync", "ERROR")
    
    return valid_destinations


class FileScanner(threading.Thread):
    """Scan for new .tbz files and queue them for distribution"""

    def __init__(self, config: Dict, stop_event: threading.Event):
        super().__init__(name="Scanner", daemon=True)
        self.config = config
        self.stop_event = stop_event
        self.queue_base = Path(config['queue_base_dir'])
        self.dest_names = [d['name'] for d in config['destinations']]

    def run(self):
        log("File scanner thread started", "INFO")

        while not self.stop_event.is_set():
            try:
                self.scan_and_queue()
            except Exception as e:
                log(f"Scanner error: {e}", "ERROR")
                import traceback
                log(traceback.format_exc(), "DEBUG")

            self.stop_event.wait(self.config['scan_interval'])

        log("File scanner thread stopped", "INFO")

    def scan_and_queue(self):
        """Scan for new files and queue them for transfer using atomic copy-rename"""
        pattern = self.config.get('incoming_pattern', '/home/*/uploads/*.tbz')
        log(f"Scanning with pattern: {pattern}", "DEBUG")

        files = glob.glob(pattern)
        if not files:
            log("No .tbz files found", "DEBUG")
            return

        log(f"Found {len(files)} .tbz files to process", "DEBUG")

        for filepath in files:
            self.process_file(filepath)

    def process_file(self, filepath: str):
        """Process a single file: copy to all queues, then delete source if all succeed"""
        filename = os.path.basename(filepath)
        log(f"Processing: {filename}", "DEBUG")

        # Track success for each destination
        success_count = 0
        total_dests = len(self.dest_names)

        for dest_name in self.dest_names:
            dest_queue = self.queue_base / dest_name
            final_path = dest_queue / filename
            temp_path = dest_queue / f".{filename}.tmp"

            # Skip if already in queue (previous partial run)
            if final_path.exists():
                log(f"{filename} already in queue for {dest_name}", "DEBUG")
                success_count += 1
                continue

            # Copy to temp file, then rename (atomic)
            try:
                # Ensure queue dir exists
                dest_queue.mkdir(parents=True, exist_ok=True)

                # Remove stale temp file if exists
                if temp_path.exists():
                    temp_path.unlink()

                # Copy to temp
                shutil.copy2(filepath, temp_path)

                # Atomic rename
                temp_path.rename(final_path)

                log(f"Queued {filename} for {dest_name}", "INFO")
                success_count += 1

            except Exception as e:
                log(f"Failed to queue {filename} for {dest_name}: {e}", "ERROR")
                # Clean up temp file on failure
                if temp_path.exists():
                    try:
                        temp_path.unlink()
                    except:
                        pass

        # If ALL destinations got the file, delete the source
        if success_count == total_dests:
            try:
                os.unlink(filepath)
                log(f"Deleted source: {filename} (queued to {success_count}/{total_dests} destinations)", "INFO")
            except Exception as e:
                log(f"Failed to delete source {filepath}: {e}", "ERROR")
        else:
            log(f"{filename}: queued to {success_count}/{total_dests} destinations, keeping source", "WARNING")


class RsyncWorker(threading.Thread):
    """Worker thread that rsyncs files to a specific destination"""

    def __init__(self, destination: Dict, config: Dict, stop_event: threading.Event):
        super().__init__(name=f"Rsync-{destination['name']}", daemon=True)
        self.destination = destination
        self.config = config
        self.stop_event = stop_event
        self.queue_dir = Path(config['queue_base_dir']) / destination['name']

    def run(self):
        log(f"Rsync worker for {self.destination['name']} started", "INFO")
        while not self.stop_event.is_set():
            try:
                self.sync_files()
            except Exception as e:
                log(f"Rsync worker {self.destination['name']} error: {e}", "ERROR")
            self.stop_event.wait(self.config['rsync_interval'])
        log(f"Rsync worker for {self.destination['name']} stopped", "INFO")

    def sync_files(self):
        """Rsync queued files to destination"""
        if not self.queue_dir.exists():
            self.queue_dir.mkdir(parents=True, exist_ok=True)
            return

        # Count .tbz files (not temp files)
        queued_files = list(self.queue_dir.glob('*.tbz'))
        if not queued_files:
            log(f"No files queued for {self.destination['name']}", "DEBUG")
            return

        log(f"Found {len(queued_files)} files to sync to {self.destination['name']}", "INFO")

        remote_path = f"{self.destination['user']}@{self.destination['host']}:{self.destination['path']}/"
        ssh_key = self.destination.get('ssh_key', '/home/wsprdaemon/.ssh/id_rsa')
        ssh_cmd = f'ssh -i {ssh_key} -o StrictHostKeyChecking=no'

        rsync_cmd = [
            'rsync',
            '-a',
            '-e', ssh_cmd,
            '--remove-source-files',  # Delete from queue after successful transfer
            f'--bwlimit={self.config["rsync_bandwidth_limit"]}',
            f'--timeout={self.config["rsync_timeout"]}',
            '--exclude', '.*',  # Exclude temp files starting with dot
            str(self.queue_dir) + '/',
            remote_path
        ]

        try:
            log(f"Running rsync to {self.destination['name']}", "DEBUG")
            result = subprocess.run(
                rsync_cmd,
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                universal_newlines=True,
                timeout=self.config['rsync_timeout'] + 30
            )
            if result.returncode == 0:
                log(f"Successfully synced to {self.destination['name']}", "INFO")
            else:
                log(f"Rsync to {self.destination['name']} failed (rc={result.returncode}): {result.stderr.strip()}", "ERROR")
        except subprocess.TimeoutExpired:
            log(f"Rsync to {self.destination['name']} timed out", "ERROR")
        except Exception as e:
            log(f"Rsync to {self.destination['name']} error: {e}", "ERROR")


def main():
    parser = argparse.ArgumentParser(description='WSPRDAEMON Reflector')
    parser.add_argument('--config', required=True, help='Path to config file (JSON)')
    parser.add_argument('--log-file', default=LOG_FILE, help='Path to log file')
    parser.add_argument('--log-max-mb', type=int, default=10, help='Max log file size in MB')
    parser.add_argument('--verbose', type=int, default=1, choices=range(0, 10),
                        help='Verbosity level 0-9 (0=WARNING+ERROR, 1=INFO, 2+=DEBUG)')
    parser.add_argument('--skip-rsync-check', action='store_true',
                        help='Skip rsync verification on destinations at startup')
    args = parser.parse_args()

    setup_logging(args.log_file, args.log_max_mb * 1024 * 1024, verbosity=args.verbose)

    log(f"=== WSPRDAEMON Reflector v{VERSION} Starting ===", "INFO")
    log(f"Verbosity level: {args.verbose}", "INFO")

    config = DEFAULT_CONFIG.copy()
    try:
        with open(args.config, 'r', encoding='utf-8') as f:
            loaded_config = json.load(f)
            config.update(loaded_config)
        log(f"Loaded configuration from {args.config}", "INFO")
    except Exception as e:
        log(f"Error loading config: {e}", "ERROR")
        sys.exit(1)

    if not config['destinations']:
        log("No destinations configured", "ERROR")
        sys.exit(1)

    log(f"Configured {len(config['destinations'])} destinations: {[d['name'] for d in config['destinations']]}", "INFO")

    # Verify rsync on all destinations
    if not args.skip_rsync_check:
        log("Verifying rsync on destination servers...", "INFO")
        valid_destinations = verify_all_destinations(config)
        if not valid_destinations:
            log("No valid destinations available - exiting", "ERROR")
            sys.exit(1)
        config['destinations'] = valid_destinations
        log(f"Verified {len(valid_destinations)} destinations ready", "INFO")
    else:
        log("Skipping rsync verification (--skip-rsync-check)", "WARNING")

    stop_event = threading.Event()

    # Ensure queue directories exist
    for dest in config['destinations']:
        queue_dir = Path(config['queue_base_dir']) / dest['name']
        queue_dir.mkdir(parents=True, exist_ok=True)
        log(f"Queue directory: {queue_dir}", "DEBUG")

    threads = []

    # Start scanner
    scanner = FileScanner(config, stop_event)
    scanner.start()
    threads.append(scanner)

    # Start rsync workers
    for dest in config['destinations']:
        worker = RsyncWorker(dest, config, stop_event)
        worker.start()
        threads.append(worker)

    log(f"Started {len(threads)} worker threads", "INFO")

    # Handle signals for graceful shutdown
    def signal_handler(signum, frame):
        log(f"Received signal {signum}, shutting down...", "INFO")
        stop_event.set()

    signal.signal(signal.SIGTERM, signal_handler)
    signal.signal(signal.SIGINT, signal_handler)

    try:
        while not stop_event.is_set():
            time.sleep(10)
    except KeyboardInterrupt:
        log("Received keyboard interrupt", "INFO")
    finally:
        log("Stopping worker threads...", "INFO")
        stop_event.set()
        for thread in threads:
            thread.join(timeout=5)
        log(f"WSPRDAEMON Reflector v{VERSION} stopped", "INFO")


if __name__ == '__main__':
    main()
