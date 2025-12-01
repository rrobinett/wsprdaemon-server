#!/usr/bin/env python3
"""
WSPRDAEMON Reflector - Distribute .tbz files from clients to multiple servers
Usage: wsprdaemon_reflector.py --config /etc/wsprdaemon/reflector_destinations.json [options]

Simple design:
  1. Scan for .tbz files in /home/*/uploads/
  2. For /home/noisegraphs/uploads/: wait 10s then validate with 'tar tf', delete if invalid
  3. Copy each file to temp name in each queue dir, then rename (atomic)
  4. If ALL queues successful -> delete source file
  5. Rsync workers sync queue dirs to destinations with --remove-source-files
"""

VERSION = "2.3.0"

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

DEFAULT_CONFIG = {
    'incoming_pattern': '/home/*/uploads/*.tbz',
    'queue_base_dir': '/var/spool/wsprdaemon/reflector',
    'destinations': [],
    'scan_interval': 5,
    'rsync_interval': 5,
    'rsync_bandwidth_limit': 20000,
    'rsync_timeout': 300,
    'noisegraphs_min_age_seconds': 10,
}

NOISEGRAPHS_UPLOADS = '/home/noisegraphs/uploads'
LOG_FILE = '/var/log/wsprdaemon/reflector.log'
LOG_MAX_BYTES = 10 * 1024 * 1024
LOG_KEEP_RATIO = 0.75


class TruncatingFileHandler(logging.FileHandler):
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
                if os.path.getsize(self.baseFilename) > self.max_bytes:
                    self.truncate_file()
        except Exception as e:
            print(f"Error checking log file size: {e}")

    def truncate_file(self):
        try:
            with open(self.baseFilename, 'r', encoding='utf-8') as f:
                lines = f.readlines()
            keep_count = max(1, int(len(lines) * self.keep_ratio))
            with open(self.baseFilename, 'w', encoding='utf-8') as f:
                f.write(f"[Log truncated - kept newest {self.keep_ratio*100:.0f}%]\n")
                f.writelines(lines[-keep_count:])
        except Exception as e:
            print(f"Error truncating log file: {e}")


def setup_logging(log_file=None, max_bytes=LOG_MAX_BYTES, keep_ratio=LOG_KEEP_RATIO, verbosity=0):
    logger = logging.getLogger()
    logger.setLevel([logging.WARNING, logging.INFO, logging.DEBUG][min(verbosity, 2)])
    logger.handlers.clear()
    formatter = logging.Formatter('[%(asctime)s] %(levelname)s: %(message)s', datefmt='%Y-%m-%d %H:%M:%S')
    if log_file:
        os.makedirs(os.path.dirname(log_file), exist_ok=True) if os.path.dirname(log_file) else None
        handler = TruncatingFileHandler(log_file, max_bytes, keep_ratio)
    else:
        handler = logging.StreamHandler()
    handler.setFormatter(formatter)
    logger.addHandler(handler)
    return logger


def log(message: str, level: str = "INFO"):
    logger = logging.getLogger()
    level_map = {'DEBUG': logging.DEBUG, 'INFO': logging.INFO, 'WARNING': logging.WARNING, 'ERROR': logging.ERROR}
    logger.log(level_map.get(level, logging.INFO), message)
    for handler in logger.handlers:
        handler.flush()


def verify_destination_rsync(destination: Dict) -> bool:
    name, user, host = destination['name'], destination['user'], destination['host']
    ssh_key = destination.get('ssh_key', '/home/wsprdaemon/.ssh/id_rsa')
    ssh_base = f"ssh -i {ssh_key} -o StrictHostKeyChecking=no -o ConnectTimeout=10 {user}@{host}"
    
    log(f"Checking rsync on {name} ({host})...", "INFO")
    try:
        result = subprocess.run(f"{ssh_base} 'which rsync'", stdout=subprocess.PIPE, stderr=subprocess.PIPE,
                                universal_newlines=True, timeout=15, shell=True)
        if result.returncode == 0:
            log(f"{name}: rsync found at {result.stdout.strip()}", "INFO")
            return True
    except subprocess.TimeoutExpired:
        log(f"{name}: SSH connection timed out", "ERROR")
        return False
    except Exception as e:
        log(f"{name}: Error checking rsync: {e}", "ERROR")
        return False
    
    log(f"{name}: rsync not found, attempting to install...", "WARNING")
    for cmd in ["'sudo apt-get update -qq && sudo apt-get install -y -qq rsync'", "'sudo yum install -y rsync'"]:
        try:
            result = subprocess.run(f"{ssh_base} {cmd}", stdout=subprocess.PIPE, stderr=subprocess.PIPE,
                                    universal_newlines=True, timeout=120, shell=True)
            if result.returncode == 0:
                log(f"{name}: rsync installed successfully", "INFO")
                return True
        except:
            pass
    
    log(f"{name}: Could not install rsync - destination will be skipped", "ERROR")
    return False


def verify_all_destinations(config: Dict) -> List[Dict]:
    valid = []
    for dest in config['destinations']:
        if verify_destination_rsync(dest):
            valid.append(dest)
        else:
            log(f"Destination {dest['name']} disabled due to missing rsync", "ERROR")
    return valid


def is_noisegraphs_file(filepath: str) -> bool:
    return filepath.startswith(NOISEGRAPHS_UPLOADS + '/')


def get_file_age(filepath: str) -> float:
    try:
        return time.time() - os.path.getmtime(filepath)
    except OSError:
        return 0


def validate_tbz_file(filepath: str) -> bool:
    try:
        result = subprocess.run(['tar', 'tf', filepath], stdout=subprocess.DEVNULL, stderr=subprocess.PIPE, timeout=30)
        if result.returncode == 0:
            return True
        log(f"tar tf failed for {os.path.basename(filepath)}: {result.stderr.decode().strip()}", "DEBUG")
        return False
    except subprocess.TimeoutExpired:
        log(f"tar tf timed out for {os.path.basename(filepath)}", "WARNING")
        return False
    except Exception as e:
        log(f"tar tf error for {os.path.basename(filepath)}: {e}", "ERROR")
        return False


class FileScanner(threading.Thread):
    def __init__(self, config: Dict, stop_event: threading.Event):
        super().__init__(name="Scanner", daemon=True)
        self.config = config
        self.stop_event = stop_event
        self.queue_base = Path(config['queue_base_dir'])
        self.dest_names = [d['name'] for d in config['destinations']]
        self.min_age = config.get('noisegraphs_min_age_seconds', 10)

    def run(self):
        log("File scanner thread started", "INFO")
        log(f"Noisegraphs validation: wait {self.min_age}s then tar tf", "INFO")
        while not self.stop_event.is_set():
            try:
                self.scan_and_queue()
            except Exception as e:
                log(f"Scanner error: {e}", "ERROR")
            self.stop_event.wait(self.config['scan_interval'])
        log("File scanner thread stopped", "INFO")

    def scan_and_queue(self):
        pattern = self.config.get('incoming_pattern', '/home/*/uploads/*.tbz')
        files = glob.glob(pattern)
        if not files:
            log("No .tbz files found", "DEBUG")
            return
        log(f"Found {len(files)} .tbz files", "DEBUG")
        for filepath in files:
            if is_noisegraphs_file(filepath):
                self.handle_noisegraphs_file(filepath)
            else:
                self.process_file(filepath)

    def handle_noisegraphs_file(self, filepath: str):
        filename = os.path.basename(filepath)
        age = get_file_age(filepath)
        if age < self.min_age:
            log(f"Noisegraphs file waiting: {filename} ({self.min_age - age:.0f}s remaining)", "DEBUG")
            return
        if validate_tbz_file(filepath):
            log(f"Noisegraphs file validated: {filename}", "DEBUG")
            self.process_file(filepath)
        else:
            try:
                os.unlink(filepath)
                log(f"Deleted invalid noisegraphs file: {filename}", "WARNING")
            except Exception as e:
                log(f"Failed to delete invalid file {filename}: {e}", "ERROR")

    def process_file(self, filepath: str):
        filename = os.path.basename(filepath)
        log(f"Processing: {filename}", "DEBUG")
        success_count = 0
        total_dests = len(self.dest_names)

        for dest_name in self.dest_names:
            dest_queue = self.queue_base / dest_name
            final_path = dest_queue / filename
            temp_path = dest_queue / f".{filename}.tmp"

            if final_path.exists():
                log(f"{filename} already in queue for {dest_name}", "DEBUG")
                success_count += 1
                continue

            try:
                dest_queue.mkdir(parents=True, exist_ok=True)
                if temp_path.exists():
                    temp_path.unlink()
                shutil.copy2(filepath, temp_path)
                temp_path.rename(final_path)
                log(f"Queued {filename} for {dest_name}", "INFO")
                success_count += 1
            except Exception as e:
                log(f"Failed to queue {filename} for {dest_name}: {e}", "ERROR")
                if temp_path.exists():
                    try:
                        temp_path.unlink()
                    except:
                        pass

        if success_count == total_dests:
            try:
                os.unlink(filepath)
                log(f"Deleted source: {filename} (queued to {success_count}/{total_dests} destinations)", "INFO")
            except Exception as e:
                log(f"Failed to delete source {filepath}: {e}", "ERROR")
        else:
            log(f"{filename}: queued to {success_count}/{total_dests} destinations, keeping source", "WARNING")


class RsyncWorker(threading.Thread):
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
        if not self.queue_dir.exists():
            self.queue_dir.mkdir(parents=True, exist_ok=True)
            return
        queued_files = list(self.queue_dir.glob('*.tbz'))
        if not queued_files:
            log(f"No files queued for {self.destination['name']}", "DEBUG")
            return
        log(f"Found {len(queued_files)} files to sync to {self.destination['name']}", "INFO")

        ssh_key = self.destination.get('ssh_key', '/home/wsprdaemon/.ssh/id_rsa')
        rsync_cmd = [
            'rsync', '-a', '-e', f'ssh -i {ssh_key} -o StrictHostKeyChecking=no',
            '--remove-source-files', f'--bwlimit={self.config["rsync_bandwidth_limit"]}',
            f'--timeout={self.config["rsync_timeout"]}', '--exclude', '.*',
            str(self.queue_dir) + '/',
            f"{self.destination['user']}@{self.destination['host']}:{self.destination['path']}/"
        ]

        try:
            result = subprocess.run(rsync_cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE,
                                    universal_newlines=True, timeout=self.config['rsync_timeout'] + 30)
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
    parser.add_argument('--skip-rsync-check', action='store_true', help='Skip rsync verification at startup')
    args = parser.parse_args()

    setup_logging(args.log_file, args.log_max_mb * 1024 * 1024, verbosity=args.verbose)
    log(f"=== WSPRDAEMON Reflector v{VERSION} Starting ===", "INFO")

    config = DEFAULT_CONFIG.copy()
    try:
        with open(args.config, 'r', encoding='utf-8') as f:
            config.update(json.load(f))
        log(f"Loaded configuration from {args.config}", "INFO")
    except Exception as e:
        log(f"Error loading config: {e}", "ERROR")
        sys.exit(1)

    if not config['destinations']:
        log("No destinations configured", "ERROR")
        sys.exit(1)

    log(f"Configured {len(config['destinations'])} destinations: {[d['name'] for d in config['destinations']]}", "INFO")

    if not args.skip_rsync_check:
        log("Verifying rsync on destination servers...", "INFO")
        config['destinations'] = verify_all_destinations(config)
        if not config['destinations']:
            log("No valid destinations available - exiting", "ERROR")
            sys.exit(1)
        log(f"Verified {len(config['destinations'])} destinations ready", "INFO")

    stop_event = threading.Event()
    for dest in config['destinations']:
        (Path(config['queue_base_dir']) / dest['name']).mkdir(parents=True, exist_ok=True)

    threads = []
    scanner = FileScanner(config, stop_event)
    scanner.start()
    threads.append(scanner)

    for dest in config['destinations']:
        worker = RsyncWorker(dest, config, stop_event)
        worker.start()
        threads.append(worker)

    log(f"Started {len(threads)} worker threads", "INFO")

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
