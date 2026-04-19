# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## What This Project Is

WSPRDAEMON is a WSPR (Weak Signal Propagation Reporter) data collection and distribution platform. Radio receivers ("Kiwis") upload compressed spot/noise data to this server, which validates, stores, and cross-distributes it. A companion scraper also pulls public data from wsprnet.org.

## Installation & Running Services

There is no build step. The project runs directly from Python source under a venv managed by the install scripts.

```bash
# Install everything (venv at /opt/wsprdaemon-server/venv, systemd services, ClickHouse users)
sudo bash install-servers.sh

# Install the reflector service only
sudo bash install-reflector.sh

# Manage individual services
sudo systemctl status  wsprdaemon_server@wsprdaemon
sudo systemctl status  wsprnet_scraper@wsprnet
sudo systemctl status  wsprdaemon_reflector@reflector

# Tail live logs
sudo journalctl -fu wsprdaemon_server@wsprdaemon
sudo journalctl -fu wsprnet_scraper@wsprnet
sudo journalctl -fu wsprdaemon_reflector@reflector
```

Python dependencies: `requests`, `clickhouse-connect`, `numpy` (Python 3.10+).

## Architecture

Three long-running Python services communicate via the filesystem and a ClickHouse database.

### Data Flow

```
Radio receivers (Kiwis)
    │  upload .tbz files
    ▼
wsprdaemon_reflector.py   ← validates tarballs, hard-links to per-dest queues,
    │                        rsync-pushes to each destination server
    ▼
wsprdaemon_server.py      ← extracts .tbz, parses spots & noise CSVs,
    │                        inserts into ClickHouse
    ▼
ClickHouse DB             ← wsprdaemon.spots, wsprdaemon.noise, wspr.rx
    ▲
wsprnet_scraper.py        ← downloads JSON from wsprnet.org, inserts into wspr.rx
```

### Service Summaries

| File | Role | Threading model |
|------|------|-----------------|
| `wsprdaemon_server.py` | Processes .tbz uploads from receivers | Producer thread → bounded queue → N extraction workers |
| `wsprnet_scraper.py` | Scrapes wsprnet.org | Download thread → insert thread (producer-consumer) |
| `wsprdaemon_reflector.py` | Distributes .tbz to multiple servers | Scanner → per-destination rsync worker threads |

### ClickHouse Schema

- `wspr.rx` — public WSPR spots from wsprnet.org (`schema/wspr.rx.sql`)
- `wsprdaemon.spots` — spots uploaded directly by receivers
- `wsprdaemon.noise` — noise measurements uploaded by receivers

All tables use `ReplacingMergeTree` with ZSTD(3) compression and delta codecs.

### Key Runtime Directories

| Path | Purpose |
|------|---------|
| `/var/spool/wsprdaemon/server/` | Incoming .tbz upload staging |
| `/var/spool/wsprdaemon/reflector/` | Per-destination rsync queues |
| `/tmp/wsprdaemon/extraction/` | Temporary extraction workspace |
| `/opt/wsprdaemon-server/` | Installed venv and config |

### Configuration

Each service reads a JSON config file (path passed as a CLI argument or defaulting to `/opt/wsprdaemon-server/<service>.conf`). Example configs are in `config-examples/`. The install scripts write the initial configs.

### Startup Optimisation (recent)

`wsprdaemon_server.py` uses an in-memory cache of already-processed file hashes to skip re-processing on restart. The cache is written atomically and a bad-directory cap prevents unbounded quarantine growth.

## Database Tools

`database-tools/` contains standalone scripts for ClickHouse migration, backup, sync, and schema inspection. Each has its own README.

## Common Patterns

- All three services loop indefinitely; they are controlled solely via systemd.
- Inter-thread communication uses `queue.Queue` with explicit `maxsize` to apply backpressure.
- File operations use hard links where possible to avoid copying large .tbz files.
- Remote rsync uses `--bwlimit` and checks free space on the destination before transferring.
- Retry logic throughout uses exponential backoff with jitter.
