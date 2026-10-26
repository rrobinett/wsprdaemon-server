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

## psk.spots ingestion (Phase 2 PR 1, default off)

`wsprdaemon_server.py` can also ingest FT8/FT4/MSK144 spots from the same
.tbz pipeline into the `psk.spots` ClickHouse table. The feature is
**off by default** during rollout — flip it on per host with either:

```bash
sudo systemctl edit wsprdaemon_server@wsprdaemon   # add Environment=WSPRDAEMON_INGEST_PSK=1
# or, ad hoc:
WSPRDAEMON_INGEST_PSK=1 wsprdaemon_server.py ...
# or, --ingest-psk on the CLI
```

Expected tar layout (Phase 2): modes live as **peer subdirs** at the tar
root, alongside the existing `wsprdaemon/` (or new `wspr/`) wspr tree:

```
<tar root>/
├── wspr/spots/RX_SITE/RECEIVER/BAND/YYMMDD_HHMM_spots.txt    # WSPR (new root)
├── wsprdaemon/spots/...                                       # WSPR (legacy root, still accepted)
├── ft8/RX_SITE/RECEIVER/BAND/YYMMDD_HHMM_ft8.jsonl
├── ft4/RX_SITE/RECEIVER/BAND/YYMMDD_HHMM_ft4.jsonl
└── routing.json                                               # optional per-receiver forwarding flags
```

Each `*.jsonl` file is one ClickHouse row per line — directly the dict
that `psk-recorder`'s `ch_tailer` writes to local SQLite. The server
fills in path-derived fields (`rx_site`, `receiver`, `band`) and the
`forward_to_pskreporter` flag from `routing.json` before insert.

`routing.json` (optional, at tar root):

```json
{
  "default": {"forward_to_pskreporter": true},
  "AC0G=ND_EN16ov/KA9Q_DXE": {"forward_to_pskreporter": false}
}
```

Default when missing: forward everything. The flag governs whether the
forthcoming gw1-elected `pskreporter_forwarder` service (Phase 2 PR 2)
re-posts the row to pskreporter.info.

Adding a new modulation = add a string to `config['psk_modes']` (default
`['ft8', 'ft4', 'msk144']`). Schema is mode-agnostic.

Tests: `python3 tests/test_psk_ingest.py` — exercises parsing + scanning
helpers against synthetic extracted-tar trees. No ClickHouse required.

## station.heartbeat ingestion (status/ subdir, default off)

`wsprdaemon_server.py` can also ingest station STATUS envelopes from the same
.tbz pipeline into the `station.heartbeat` ClickHouse table.  Off by default,
exactly like psk ingestion:

```bash
sudo systemctl edit wsprdaemon_server@wsprdaemon   # Environment=WSPRDAEMON_INGEST_STATUS=1
# or --ingest-status on the CLI
```

Tar layout — `status` is a peer subdir at the tar root, beside `wspr/` and the
psk modes:

```
<tar root>/status/<ROLE>/<HOST_ID>/<YYYYMMDDTHHMMSSZ>.jsonl    ROLE is 'vm' or 'pm'
```

One envelope per line.  The wire contract is
`sigmond/lib/sigmond/heartbeat_schema.py` (`kind = "station_heartbeat"`,
`schema_version = 1`), and this server does not re-specify it: additive fields
are allowed at every depth, so a station one release ahead is never dropped
for carrying a field we have not learned about yet.

Three rules govern the parse, and each exists because its opposite is a bug
that hides itself:

- **⛔ `received_at` is the TAR'S MTIME.**  Availability comes from ARRIVAL,
  never from anything a station says about itself — a station that has stopped
  cannot report that it stopped.  The mtime survives the reflector's hard-link
  and `rsync -a`, so it is one server clock's reading of the gateway arrival,
  identical on wd10, wd20 and wd30.  `emitted_at` is stored for forensics and
  must never decide liveness.
- **⛔ `role` and `host_id` come from the TAR PATH**, never from which blocks
  are present.  Inferring `pm` from a `rac` block would misfile every Proxmox
  host that carries no RAC switch script, because `pm-heartbeat.py` omits
  `rac` on exactly those hosts — the inference fails for the hosts least
  likely to be noticed.
- **⛔ Status is the PASSENGER in this tar, not the cargo.**  A tar whose
  spots parsed is never rejected because its status did not, and a status
  insert that fails never pins a tar on the gateway: that would turn a status
  fault into a SPOT outage.  The next envelope is 60 s away.

`gateway` is read from the incoming spool directory (`from-gw1` → `gw1`), not
from the envelope, for the same reason: it is a property of where the tar
landed.

Schema: `schema/station.heartbeat.sql`, mirrored inline in
`setup_status_tables()` so a fresh wd self-bootstraps.  A test asserts the two
copies agree — a row key with no column fails the INSERT for the whole batch,
which is every station's heartbeat in that flush, not just the one.

Block verdicts and reasons are stored as `Map`s rather than columns on
purpose: adding a block is a schema change in `heartbeat_schema.py` and must
not ALSO be a ClickHouse migration.  `payload` holds the envelope verbatim,
because the rule that a vanished block can never read as healthy only holds if
the original is recoverable.

Tests: `python3 tests/test_status_ingest.py` — 21, against synthetic extracted
trees, no ClickHouse required.  All twelve guards are mutation-checked.

Design and remaining phases: `sigmond/tasks/plan-status-via-uploader.md`.

## PSKReporter forwarder (Phase 2 PR 2)

Two new daemons:

* `pskreporter_forwarder.py` — runs on each wd{10,20,30}. Polls
  `psk.spots WHERE forward_to_pskreporter=1 AND ingested_at > <watermark>`
  every 30 s and ships rows via `ftlib-pskreporter`. Only the
  gw1-elected leader actually forwards; the others idle but keep
  their watermark fresh so failover is gap-free. systemd unit:
  `pskreporter-forwarder.service`.

* `pskreporter_forward_elector.py` — runs on gw1. TCP-probes the
  ClickHouse port on each wd candidate (default order: wd10, wd20,
  wd30) every 30 s and writes the first-healthy short hostname to
  `/var/www/html/pskreporter-leader.txt`. nginx serves the file at
  `http://gw1.wsprdaemon.org/pskreporter-leader.txt` — each forwarder
  polls that URL. systemd unit: `pskreporter-forward-elector.service`.

Why a static file via nginx: gw1 doesn't need SSH into the wd
servers (and shouldn't), and the wd servers don't need to coordinate
with each other. Failure mode: if gw1 is unreachable, each wd's
forwarder falls back to its last-known leader state — losing
contact with gw1 doesn't drop PSKReporter delivery.

Operator override: pin a leader with `--pin wd20` on the elector,
or by hand-editing the published file. Auto mode resumes on
restart.

Per-spot routing flag (set by psk-recorder Phase 2 PR 3):

| `PSK_DELIVERY_MODE` (client) | `forward_to_pskreporter` (row) | Forwarder action |
|---|---|---|
| `server` (default)           | 1     | POST to PSKReporter |
| `both`                       | 0     | skip (client posts direct) |
| `direct`                     | n/a   | row never in psk.spots |

Tests: `python3 tests/test_pskreporter_forwarder.py` — fakes for
clickhouse-driver, ftlib PskReporter, and the leader URL fetch.
18 pass; no ClickHouse or network required.
