#!/usr/bin/env python3
"""
WSPRDAEMON Reflector - Distribute .tbz files from clients to multiple servers
Usage: wsprdaemon_reflector.py --config /etc/wsprdaemon/reflector_destinations.json [options]

Simple design:
  1. Scan for .tbz files in /home/*/uploads/
  2. Validate ALL .tbz files with 'tar tf' ONCE (track by inode)
  3. Hard link (or copy if cross-filesystem) to each queue dir
  4. If ALL queues successful -> delete source file
  5. Rsync workers check free space on destination, then sync with --remove-source-files

v2.6.5 changes:
  - FIX: Skip sync when disk space check times out (don't assume OK)
  - Treat timeout/error as "unknown" and skip sync to be safe
"""

VERSION = "2.6.5"

import argparse
import fnmatch
import json
import sys
import time
import os
import subprocess
import threading
import shutil
import signal
from pathlib import Path
from typing import Dict, List, Optional, Tuple, Set, Iterator
import logging

DEFAULT_CONFIG = {
    'incoming_pattern': '/home/*/uploads/*.tbz',
    'queue_base_dir': '/var/spool/wsprdaemon/reflector',
    'destinations': [],
    'scan_interval': 5,
    'rsync_interval': 5,
    'rsync_bandwidth_limit': 20000,
    'rsync_timeout': 300,
    'min_age_seconds': 10,  # Wait this long before validating (for partial uploads)
    'min_free_space_percent': 25,  # Remote destination minimum free space
    'quarantine_dir': None,
    'max_files_per_scan': 1000,
    'delete_patterns': ['AI6VN_25*'],  # Delete files matching these patterns
    'corrupt_min_age_seconds': 10,  # Only delete corrupt files older than this
    'local_max_used_percent': 80,  # Start purging queues when local disk exceeds this
    'queue_purge_batch': 500,  # Number of files to purge at a time from largest queue
    'heartbeat_interval': 60,  # Log heartbeat every N seconds
    'tar_timeout': 30,  # Timeout for tar validation
}

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


def check_remote_free_space(destination: Dict, path: str, min_percent: int = 25) -> Optional[float]:
    """Check free space on remote server. Returns free percentage or None on error/timeout."""
    name, user, host = destination['name'], destination['user'], destination['host']
    ssh_key = destination.get('ssh_key', '/home/wsprdaemon/.ssh/id_rsa')
    ssh_base = f"ssh -i {ssh_key} -o StrictHostKeyChecking=no -o ConnectTimeout=10 {user}@{host}"
    
    try:
        result = subprocess.run(
            f"{ssh_base} 'df -P {path} 2>/dev/null | tail -1'",
            stdout=subprocess.PIPE, stderr=subprocess.PIPE,
            universal_newlines=True, timeout=15, shell=True
        )
        if result.returncode == 0 and result.stdout.strip():
            parts = result.stdout.strip().split()
            if len(parts) >= 5:
                capacity_str = parts[4].rstrip('%')
                used_percent = int(capacity_str)
                free_percent = 100 - used_percent
                return free_percent
    except subprocess.TimeoutExpired:
        log(f"{name}: Timeout checking disk space", "WARNING")
    except Exception as e:
        log(f"{name}: Error checking disk space: {e}", "WARNING")
    
    return None


def check_local_used_percent(path: str) -> Optional[float]:
    """Check used percentage on local filesystem. Returns used percentage or None on error."""
    try:
        stat = os.statvfs(path)
        total = stat.f_blocks * stat.f_frsize
        free = stat.f_bavail * stat.f_frsize
        if total > 0:
            used = total - free
            return (used / total) * 100
    except Exception as e:
        log(f"Error checking local disk space for {path}: {e}", "WARNING")
    return None


def same_filesystem(path1: str, path2: str) -> bool:
    """Check if two paths are on the same filesystem (for hard link support)."""
    try:
        return os.stat(path1).st_dev == os.stat(path2).st_dev
    except OSError:
        return False


def get_file_age(filepath: str) -> float:
    try:
        return time.time() - os.path.getmtime(filepath)
    except OSError:
        return 0


def get_file_inode(filepath: str) -> Optional[int]:
    """Get inode number for a file (unique identifier on filesystem)."""
    try:
        return os.stat(filepath).st_ino
    except OSError:
        return None


def matches_pattern(filename: str, patterns: List[str]) -> bool:
    """Check if filename matches any of the patterns."""
    for pattern in patterns:
        if fnmatch.fnmatch(filename, pattern):
            return True
    return False


def validate_tbz_file(filepath: str, timeout: int = 30) -> Tuple[Optional[bool], str]:
    """Validate a .tbz file with tar tf.
    
    Uses subprocess with timeout and SIGKILL fallback to prevent hangs.
    
    Returns:
        (True, "") - file is valid
        (False, "reason") - file is DEFINITELY corrupt (tar found actual errors)
        (None, "reason") - validation inconclusive (timeout/error), should retry later
    """
    process = None
    try:
        process = subprocess.Popen(
            ['tar', 'tf', filepath],
            stdout=subprocess.DEVNULL,
            stderr=subprocess.PIPE,
            preexec_fn=os.setsid  # Create new process group for clean kill
        )
        
        try:
            _, stderr = process.communicate(timeout=timeout)
            
            if process.returncode == 0:
                return True, ""
            
            error_msg = stderr.decode().strip() if stderr else f"exit code {process.returncode}"
            
            # Only return False (definitely corrupt) for actual tar corruption errors
            corruption_indicators = [
                "unexpected eof",
                "truncated",
                "corrupted",
                "invalid tar",
                "not in gzip format",
                "invalid compressed data",
                "crc error",
                "length error",
            ]
            
            error_lower = error_msg.lower()
            for indicator in corruption_indicators:
                if indicator in error_lower:
                    return False, error_msg
            
            # For other errors (permission, busy file, etc), return None to retry later
            return None, error_msg
            
        except subprocess.TimeoutExpired:
            # Try SIGTERM first
            try:
                os.killpg(os.getpgid(process.pid), signal.SIGTERM)
                time.sleep(0.5)
            except:
                pass
            
            # If still running, SIGKILL
            if process.poll() is None:
                try:
                    os.killpg(os.getpgid(process.pid), signal.SIGKILL)
                except:
                    pass
                try:
                    process.kill()
                except:
                    pass
            
            return None, f"timeout after {timeout}s (killed)"
            
    except FileNotFoundError:
        return None, "file not found (may have been moved)"
    except Exception as e:
        return None, str(e)
    finally:
        # Ensure process is cleaned up
        if process and process.poll() is None:
            try:
                process.kill()
            except:
                pass


def scan_upload_dirs(stop_event: threading.Event) -> Iterator[str]:
    """Scan /home/*/uploads/ directories for files. Yields filepaths.
    
    Uses os.scandir() for fast, interruptible scanning.
    Checks stop_event between directories to allow quick shutdown.
    """
    home_dir = Path('/home')
    
    try:
        for user_entry in os.scandir(home_dir):
            if stop_event.is_set():
                return
            
            if not user_entry.is_dir():
                continue
            
            uploads_path = Path(user_entry.path) / 'uploads'
            
            # Handle symlinks
            if uploads_path.is_symlink():
                try:
                    uploads_path = uploads_path.resolve()
                except OSError:
                    continue
            
            if not uploads_path.is_dir():
                continue
            
            try:
                for file_entry in os.scandir(uploads_path):
                    if stop_event.is_set():
                        return
                    
                    if file_entry.is_file():
                        yield file_entry.path
                        
            except PermissionError:
                continue
            except OSError as e:
                log(f"Error scanning {uploads_path}: {e}", "DEBUG")
                continue
                
    except PermissionError:
        log("Permission denied scanning /home", "ERROR")
    except OSError as e:
        log(f"Error scanning /home: {e}", "ERROR")


class QueueManager:
    """Manages local queue directories and prevents overflow."""
    
    def __init__(self, config: Dict):
        self.queue_base = Path(config['queue_base_dir'])
        self.local_max_used = config.get('local_max_used_percent', 80)
        self.purge_batch = config.get('queue_purge_batch', 500)
        self.last_check_time = 0
        self.check_interval = 30  # seconds
        self.last_warning_time = 0
    
    def check_and_purge_if_needed(self) -> bool:
        """Check local disk space and purge from largest queue if needed.
        Returns True if OK to continue, False if critically full."""
        now = time.time()
        if now - self.last_check_time < self.check_interval:
            return True
        
        self.last_check_time = now
        used_percent = check_local_used_percent(str(self.queue_base))
        
        if used_percent is None:
            return True  # Can't check, assume OK
        
        if used_percent > self.local_max_used:
            if now - self.last_warning_time > 60:
                log(f"Local disk {used_percent:.1f}% used (max {self.local_max_used}%) - purging from largest queue", "WARNING")
                self.last_warning_time = now
            
            self.purge_from_largest_queue()
            return True  # Continue after purging
        
        return True
    
    def get_queue_sizes(self) -> Dict[str, int]:
        """Get file count for each queue directory."""
        sizes = {}
        try:
            for queue_dir in self.queue_base.iterdir():
                if queue_dir.is_dir():
                    # Use scandir for speed instead of glob
                    count = sum(1 for e in os.scandir(queue_dir) if e.name.endswith('.tbz'))
                    sizes[queue_dir.name] = count
        except Exception as e:
            log(f"Error getting queue sizes: {e}", "ERROR")
        return sizes
    
    def purge_from_largest_queue(self):
        """Purge oldest files from the queue with the most files."""
        sizes = self.get_queue_sizes()
        if not sizes:
            return
        
        # Find queue with most files
        largest_queue = max(sizes, key=sizes.get)
        largest_count = sizes[largest_queue]
        
        if largest_count == 0:
            return
        
        queue_dir = self.queue_base / largest_queue
        
        try:
            # Get files with mtime using scandir (faster than glob + stat)
            files_with_mtime = []
            for entry in os.scandir(queue_dir):
                if entry.name.endswith('.tbz'):
                    try:
                        files_with_mtime.append((entry.path, entry.stat().st_mtime))
                    except OSError:
                        pass
            
            if not files_with_mtime:
                return
            
            # Sort by mtime, oldest first
            files_with_mtime.sort(key=lambda x: x[1])
            
            to_delete = min(self.purge_batch, len(files_with_mtime))
            log(f"Purging {to_delete} oldest files from {largest_queue} (has {largest_count} files)", "WARNING")
            
            deleted = 0
            for filepath, _ in files_with_mtime[:to_delete]:
                try:
                    os.unlink(filepath)
                    deleted += 1
                except Exception as e:
                    log(f"Failed to delete {os.path.basename(filepath)}: {e}", "DEBUG")
            
            log(f"Purged {deleted} files from {largest_queue}", "WARNING")
            
        except Exception as e:
            log(f"Error purging from {largest_queue}: {e}", "ERROR")


class FileScanner(threading.Thread):
    def __init__(self, config: Dict, stop_event: threading.Event, queue_manager: QueueManager):
        super().__init__(name="Scanner", daemon=True)
        self.config = config
        self.stop_event = stop_event
        self.queue_manager = queue_manager
        self.queue_base = Path(config['queue_base_dir'])
        self.dest_names = [d['name'] for d in config['destinations']]
        self.min_age = config.get('min_age_seconds', 10)
        self.corrupt_min_age = config.get('corrupt_min_age_seconds', 10)
        self.quarantine_dir = config.get('quarantine_dir')
        self.max_files_per_scan = config.get('max_files_per_scan', 1000)
        self.delete_patterns = config.get('delete_patterns', ['AI6VN_25*'])
        self.heartbeat_interval = config.get('heartbeat_interval', 60)
        self.tar_timeout = config.get('tar_timeout', 30)
        
        # Track validated files by inode to avoid re-validating
        self.validated_inodes: Set[int] = set()
        self.corrupt_inodes: Dict[int, Tuple[float, str]] = {}  # inode -> (first_seen_time, reason)
        self.inconclusive_inodes: Dict[int, Tuple[int, str]] = {}  # inode -> (retry_count, reason)
        
        # Check if we can use hard links (same filesystem)
        self.can_hardlink = {}
        
        # Watchdog state
        self.last_heartbeat = time.time()
        self.current_file = None  # Track which file we're currently processing
        self.files_processed_since_heartbeat = 0

    def run(self):
        log("File scanner thread started", "INFO")
        log(f"Validation: wait {self.min_age}s then tar tf (timeout {self.tar_timeout}s, once per file)", "INFO")
        log(f"Corrupt file min age before deletion: {self.corrupt_min_age}s", "INFO")
        log(f"Max files per scan: {self.max_files_per_scan}", "INFO")
        log(f"Heartbeat interval: {self.heartbeat_interval}s", "INFO")
        if self.delete_patterns:
            log(f"Auto-delete patterns: {self.delete_patterns}", "INFO")
        if self.quarantine_dir:
            log(f"Quarantine directory: {self.quarantine_dir}", "INFO")
            os.makedirs(self.quarantine_dir, exist_ok=True)
        
        while not self.stop_event.is_set():
            try:
                self.scan_and_queue()
                self.maybe_log_heartbeat()
            except Exception as e:
                log(f"Scanner error: {e}", "ERROR")
                import traceback
                log(f"Traceback: {traceback.format_exc()}", "DEBUG")
            
            # Use wait() instead of sleep() for quick interrupt
            self.stop_event.wait(self.config['scan_interval'])
        
        log("File scanner thread stopped", "INFO")

    def maybe_log_heartbeat(self):
        """Log periodic heartbeat to show scanner is alive."""
        now = time.time()
        if now - self.last_heartbeat >= self.heartbeat_interval:
            queue_sizes = self.queue_manager.get_queue_sizes()
            queue_info = ", ".join(f"{k}:{v}" for k, v in sorted(queue_sizes.items()))
            log(f"HEARTBEAT: Scanner alive, processed {self.files_processed_since_heartbeat} files, queues: {queue_info}", "INFO")
            self.last_heartbeat = now
            self.files_processed_since_heartbeat = 0

    def check_hardlink_support(self, source_dir: str) -> bool:
        """Check if hard links work between source_dir and queue_base."""
        if source_dir in self.can_hardlink:
            return self.can_hardlink[source_dir]
        
        can_link = same_filesystem(source_dir, str(self.queue_base))
        self.can_hardlink[source_dir] = can_link
        
        if can_link:
            log(f"Hard links supported for {source_dir}", "DEBUG")
        else:
            log(f"Hard links NOT supported for {source_dir} (cross-filesystem), using copy", "INFO")
        
        return can_link

    def scan_and_queue(self):
        # Check local disk space and purge if needed
        self.queue_manager.check_and_purge_if_needed()
        
        # Use interruptible scanner instead of glob
        processed = 0
        deleted_unwanted = 0
        tbz_files = []
        
        for filepath in scan_upload_dirs(self.stop_event):
            if self.stop_event.is_set():
                return
            
            filename = os.path.basename(filepath)
            
            # Check for files to delete (any extension)
            if matches_pattern(filename, self.delete_patterns):
                try:
                    os.unlink(filepath)
                    deleted_unwanted += 1
                    log(f"Deleted unwanted file: {filename}", "DEBUG")
                except Exception as e:
                    log(f"Failed to delete {filename}: {e}", "WARNING")
                continue
            
            # Collect .tbz files for processing
            if filename.endswith('.tbz'):
                tbz_files.append(filepath)
                
                # Limit batch size
                if len(tbz_files) >= self.max_files_per_scan:
                    break
        
        if deleted_unwanted > 0:
            log(f"Deleted {deleted_unwanted} unwanted files", "INFO")
        
        if not tbz_files:
            log("No .tbz files found", "DEBUG")
            return
        
        total_found = len(tbz_files)
        if total_found >= self.max_files_per_scan:
            log(f"Found {total_found}+ .tbz files, processing {self.max_files_per_scan} this cycle", "INFO")
        else:
            log(f"Found {total_found} .tbz files", "DEBUG")
        
        for filepath in tbz_files:
            if self.stop_event.is_set():
                return
            
            # Re-check disk space periodically during large batches
            if processed > 0 and processed % 100 == 0:
                self.queue_manager.check_and_purge_if_needed()
                self.maybe_log_heartbeat()
            
            if self.handle_tbz_file(filepath):
                processed += 1
                self.files_processed_since_heartbeat += 1
        
        if processed > 0:
            log(f"Processed {processed} files this cycle", "DEBUG")

    def handle_tbz_file(self, filepath: str) -> bool:
        """Handle a .tbz file. Returns True if processed/handled, False if skipped."""
        filename = os.path.basename(filepath)
        age = get_file_age(filepath)
        
        # Wait for file to be old enough (might still be uploading)
        if age < self.min_age:
            return False
        
        inode = get_file_inode(filepath)
        if inode is None:
            return False  # File disappeared
        
        # Already validated successfully? Process it directly
        if inode in self.validated_inodes:
            self.process_file(filepath)
            return True
        
        # Known corrupt file? Check if old enough to delete
        if inode in self.corrupt_inodes:
            first_seen, reason = self.corrupt_inodes[inode]
            time_known_corrupt = time.time() - first_seen
            if time_known_corrupt >= self.corrupt_min_age:
                # Old enough - delete or quarantine
                self.delete_corrupt_file(filepath, filename, reason)
                del self.corrupt_inodes[inode]
            return True  # Handled (or waiting to delete)
        
        # Previously had inconclusive validation?
        if inode in self.inconclusive_inodes:
            retry_count, reason = self.inconclusive_inodes[inode]
            # Retry every 100 cycles
            if retry_count < 100:
                self.inconclusive_inodes[inode] = (retry_count + 1, reason)
                return False
            else:
                # Reset and try again
                del self.inconclusive_inodes[inode]
        
        # Track which file we're validating (for debugging hangs)
        self.current_file = filename
        log(f"Validating: {filename}", "DEBUG")
        
        # Validate the file
        is_valid, reason = validate_tbz_file(filepath, timeout=self.tar_timeout)
        
        self.current_file = None
        
        if is_valid is True:
            # File is valid - remember this and process it
            self.validated_inodes.add(inode)
            log(f"Validated: {filename}", "DEBUG")
            self.process_file(filepath)
            return True
            
        elif is_valid is False:
            # File is DEFINITELY corrupt - track it, will delete when old enough
            self.corrupt_inodes[inode] = (time.time(), reason)
            log(f"Corrupt file detected: {filename} ({reason}) - will delete after {self.corrupt_min_age}s", "WARNING")
            return True
                    
        else:
            # Validation inconclusive - track and retry later
            self.inconclusive_inodes[inode] = (1, reason)
            log(f"Validation inconclusive for {filename}: {reason}", "WARNING")
            return False

    def delete_corrupt_file(self, filepath: str, filename: str, reason: str):
        """Delete or quarantine a corrupt file."""
        if self.quarantine_dir:
            try:
                quarantine_path = os.path.join(self.quarantine_dir, filename)
                shutil.move(filepath, quarantine_path)
                log(f"Quarantined corrupt file: {filename} ({reason})", "WARNING")
            except Exception as e:
                log(f"Failed to quarantine {filename}: {e}", "ERROR")
        else:
            try:
                os.unlink(filepath)
                log(f"Deleted corrupt file: {filename} ({reason})", "WARNING")
            except Exception as e:
                log(f"Failed to delete corrupt file {filename}: {e}", "ERROR")

    def process_file(self, filepath: str):
        """Queue file to all destinations. Delete source only if ALL succeed."""
        filename = os.path.basename(filepath)
        source_dir = os.path.dirname(filepath)
        use_hardlink = self.check_hardlink_support(source_dir)
        
        log(f"Processing: {filename} ({'hardlink' if use_hardlink else 'copy'})", "DEBUG")
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

                if use_hardlink:
                    os.link(filepath, final_path)
                    log(f"Linked {filename} for {dest_name}", "INFO")
                else:
                    shutil.copy2(filepath, temp_path)
                    temp_path.rename(final_path)
                    log(f"Copied {filename} for {dest_name}", "INFO")
                
                success_count += 1

            except OSError as e:
                if e.errno == 28:  # No space left on device
                    log(f"No space left on device while queueing {filename} for {dest_name} - will purge", "ERROR")
                    self.queue_manager.purge_from_largest_queue()
                else:
                    log(f"Failed to queue {filename} for {dest_name}: {e}", "ERROR")
            except Exception as e:
                log(f"Failed to queue {filename} for {dest_name}: {e}", "ERROR")
            finally:
                if temp_path.exists():
                    try:
                        temp_path.unlink()
                    except:
                        pass

        if success_count == total_dests:
            # Clean up validation cache for this inode
            inode = get_file_inode(filepath)
            if inode:
                self.validated_inodes.discard(inode)
                self.corrupt_inodes.pop(inode, None)
                self.inconclusive_inodes.pop(inode, None)
            
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
        self.min_free_percent = config.get('min_free_space_percent', 25)
        self.last_space_warning = 0
        self.consecutive_space_failures = 0  # Track consecutive failures to check space

    def run(self):
        log(f"Rsync worker for {self.destination['name']} started", "INFO")
        while not self.stop_event.is_set():
            try:
                self.sync_files()
            except Exception as e:
                log(f"Rsync worker {self.destination['name']} error: {e}", "ERROR")
            
            # Use wait() instead of sleep() for quick interrupt
            self.stop_event.wait(self.config['rsync_interval'])
        
        log(f"Rsync worker for {self.destination['name']} stopped", "INFO")

    def sync_files(self):
        if self.stop_event.is_set():
            return
            
        if not self.queue_dir.exists():
            self.queue_dir.mkdir(parents=True, exist_ok=True)
            return
        
        # Use scandir for speed
        queued_count = sum(1 for e in os.scandir(self.queue_dir) if e.name.endswith('.tbz'))
        if queued_count == 0:
            log(f"No files queued for {self.destination['name']}", "DEBUG")
            return

        # Check remote disk space
        free_percent = check_remote_free_space(
            self.destination, 
            self.destination['path'], 
            self.min_free_percent
        )
        
        # If we can't determine free space (timeout/error), skip sync to be safe
        if free_percent is None:
            self.consecutive_space_failures += 1
            now = time.time()
            if now - self.last_space_warning > 300:
                log(f"{self.destination['name']}: Cannot check disk space (timeout/error) - skipping sync (queued: {queued_count})", "WARNING")
                self.last_space_warning = now
            return
        
        # Reset failure counter on successful check
        self.consecutive_space_failures = 0
        
        if free_percent < self.min_free_percent:
            now = time.time()
            if now - self.last_space_warning > 300:
                log(f"{self.destination['name']}: Low disk space ({free_percent:.0f}% free, need {self.min_free_percent}%) - skipping sync (queued: {queued_count})", "ERROR")
                self.last_space_warning = now
            return
        else:
            log(f"{self.destination['name']}: Disk space OK ({free_percent:.0f}% free)", "DEBUG")

        log(f"Found {queued_count} files to sync to {self.destination['name']}", "INFO")

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
    log(f"Remote min free space: {config.get('min_free_space_percent', 25)}%", "INFO")
    log(f"Local max used before purge: {config.get('local_max_used_percent', 80)}%", "INFO")
    log(f"Queue purge batch size: {config.get('queue_purge_batch', 500)} files", "INFO")

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

    queue_manager = QueueManager(config)
    
    threads = []
    scanner = FileScanner(config, stop_event, queue_manager)
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
            stop_event.wait(10)  # Use wait() for quick response to signals
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
