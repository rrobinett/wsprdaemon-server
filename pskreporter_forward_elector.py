#!/usr/bin/env python3
"""PSKReporter forwarder elector (Phase 2 PR 2).

Runs on gw1 (the wsprdaemon.org SFTP gateway).  Probes each wd server
candidate every PROBE_SEC and writes the currently-healthy preferred
leader's short hostname into a small static file that nginx serves at

    http://gw1.wsprdaemon.org/pskreporter-leader.txt

Each ``pskreporter_forwarder`` daemon on wd10/wd20/wd30 polls that
URL to decide whether IT is the active leader.

Election is preference-ordered, not consensus-based:

  * candidates  = [wd10, wd20, wd30]   (operator-configurable)
  * leader      = the FIRST candidate that responds to the probe
  * if NONE respond → keep the last known leader (don't blank the file)

A probe is a TCP connect to the ClickHouse native port (9000 by
default).  We use that rather than ICMP because the forwarder also
needs ClickHouse — so probe-success and forwarder-success are
correlated, which is what we want.

When all probes fail, we explicitly DON'T flip leadership to
"nobody": that would tell every forwarder to stand down and we'd
silently drop PSKReporter delivery during a gateway-side network
glitch.  The last-known-good leader stays elected, and the forwarder
on that side keeps retrying.

Operator override
-----------------
An operator can pin a specific leader at any time by writing to the
output file directly (or via ``--pin <hostname>``).  This daemon
treats the file as the source of truth on read, so a manual write
flows through to the next read cycle naturally.  Reverting to
auto-mode is just a matter of removing the pin (or restarting).
"""
from __future__ import annotations

import argparse
import logging
import signal
import socket
import sys
import time
from dataclasses import dataclass
from pathlib import Path
from typing import List, Optional

logger = logging.getLogger("pskreporter-forward-elector")


VERSION = "0.1.0"

DEFAULT_OUTPUT_PATH = Path("/var/www/html/pskreporter-leader.txt")
DEFAULT_CANDIDATES = ("wd10.wsprdaemon.org", "wd20.wsprdaemon.org",
                      "wd30.wsprdaemon.org")
DEFAULT_PROBE_SEC = 30
DEFAULT_PROBE_TIMEOUT_SEC = 3
DEFAULT_CLICKHOUSE_PORT = 9000


_STOP = False


def _install_signal_handlers() -> None:
    def _handle(signum, frame):
        global _STOP
        _STOP = True
        logger.info("signal %s received → shutdown requested", signum)
    signal.signal(signal.SIGTERM, _handle)
    signal.signal(signal.SIGINT, _handle)


def probe_candidate(host: str, *, port: int = DEFAULT_CLICKHOUSE_PORT,
                    timeout: float = DEFAULT_PROBE_TIMEOUT_SEC) -> bool:
    """TCP connect to (host, port).  True iff the SYN-ACK lands within
    `timeout`.  Anything else (DNS failure, RST, timeout) → False.
    """
    try:
        with socket.create_connection((host, port), timeout=timeout):
            return True
    except (OSError, socket.gaierror):
        return False


def short_name(fqdn: str) -> str:
    """`wd10.wsprdaemon.org` → `wd10`.  Robust to bare-host inputs."""
    return fqdn.split(".", 1)[0].lower()


def pick_leader(candidates: List[str],
                *, probe_fn=probe_candidate) -> Optional[str]:
    """Return the short name of the first healthy candidate, or None."""
    for fqdn in candidates:
        if probe_fn(fqdn):
            return short_name(fqdn)
    return None


def read_current(output_path: Path) -> Optional[str]:
    try:
        return output_path.read_text().strip() or None
    except (FileNotFoundError, IsADirectoryError):
        return None
    except OSError as exc:
        logger.warning("read_current failed: %s", exc)
        return None


def write_leader(output_path: Path, name: str) -> None:
    """Atomically replace the output file with ``<name>\\n``."""
    output_path.parent.mkdir(parents=True, exist_ok=True)
    tmp = output_path.with_suffix(output_path.suffix + ".tmp")
    tmp.write_text(name + "\n")
    tmp.replace(output_path)


@dataclass
class ElectorConfig:
    candidates: List[str]
    output_path: Path = DEFAULT_OUTPUT_PATH
    probe_sec: int = DEFAULT_PROBE_SEC
    probe_timeout: float = DEFAULT_PROBE_TIMEOUT_SEC
    pin: Optional[str] = None    # if set, freeze leadership on this name


def run_loop(cfg: ElectorConfig, *, probe_fn=probe_candidate) -> int:
    current = read_current(cfg.output_path)
    if current is not None:
        logger.info("startup: existing leader file = %r", current)

    if cfg.pin:
        # Static override: write the pinned value and re-write it whenever
        # an outside party clobbers the file.
        logger.info("pin mode: forcing leader = %r", cfg.pin)
        write_leader(cfg.output_path, cfg.pin)

    while not _STOP:
        if cfg.pin:
            # Honor any external clobbers (operator briefly hand-edits
            # to debug) but always re-establish the pin.
            current = read_current(cfg.output_path)
            if current != cfg.pin:
                write_leader(cfg.output_path, cfg.pin)
                logger.info("pin: re-established %r (was %r)", cfg.pin, current)
            _interruptible_sleep(cfg.probe_sec)
            continue

        elected = pick_leader(cfg.candidates,
                              probe_fn=lambda h: probe_fn(
                                  h, timeout=cfg.probe_timeout,
                              ))
        if elected is None:
            logger.warning(
                "no candidate healthy this cycle; "
                "keeping previous leader = %r", current,
            )
        else:
            if elected != current:
                write_leader(cfg.output_path, elected)
                logger.info("elected %r (was %r)", elected, current)
                current = elected
            else:
                logger.debug("leader unchanged = %r", current)
        _interruptible_sleep(cfg.probe_sec)
    logger.info("stop requested; exiting")
    return 0


def _interruptible_sleep(seconds: float) -> None:
    end = time.monotonic() + seconds
    while not _STOP and time.monotonic() < end:
        time.sleep(min(1.0, end - time.monotonic()))


# ── CLI ─────────────────────────────────────────────────────────────────────

def main(argv: Optional[List[str]] = None) -> int:
    p = argparse.ArgumentParser(
        description="PSKReporter forwarder elector (Phase 2 PR 2)")
    p.add_argument("--candidate", action="append", default=None,
                   help="wd candidate hostname; repeat in preference "
                        "order. Default: wd10, wd20, wd30")
    p.add_argument("--output-path", default=str(DEFAULT_OUTPUT_PATH),
                   type=Path,
                   help=f"file to write the elected name into "
                        f"(default: {DEFAULT_OUTPUT_PATH})")
    p.add_argument("--probe-sec", type=int, default=DEFAULT_PROBE_SEC)
    p.add_argument("--probe-timeout", type=float,
                   default=DEFAULT_PROBE_TIMEOUT_SEC)
    p.add_argument("--pin", default=None,
                   help="freeze leadership on this short hostname; "
                        "useful for maintenance windows")
    p.add_argument("--version", action="version", version=f"%(prog)s {VERSION}")
    p.add_argument("-v", "--verbose", action="count", default=0)
    args = p.parse_args(argv)

    candidates = args.candidate or list(DEFAULT_CANDIDATES)

    level = logging.INFO if args.verbose == 0 else logging.DEBUG
    logging.basicConfig(
        level=level,
        format="%(asctime)s %(levelname)s %(name)s: %(message)s",
        datefmt="%Y-%m-%dT%H:%M:%S",
    )
    _install_signal_handlers()

    cfg = ElectorConfig(
        candidates=candidates,
        output_path=args.output_path,
        probe_sec=args.probe_sec,
        probe_timeout=args.probe_timeout,
        pin=args.pin,
    )
    return run_loop(cfg)


if __name__ == "__main__":
    sys.exit(main())
