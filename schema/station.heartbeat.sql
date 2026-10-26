-- station.heartbeat — one row per station status envelope.
--
-- Provenance: sigmond/tasks/plan-status-via-uploader.md, Phase 1 (2026-10-26).
-- Wire contract: sigmond/lib/sigmond/heartbeat_schema.py,
-- kind = "station_heartbeat", schema_version = 1.
--
-- Envelopes arrive as `status/<ROLE>/<HOST_ID>/<ts>.jsonl` inside the same
-- .tbz the station already ships to gw1/gw2 for spots.  They are NOT a
-- separate delivery channel; moving status here is what puts it on all three
-- WD servers, queryable beside the spots and FTx data the same station
-- produces.
--
-- ⛔ `received_at` IS THE TAR'S MTIME, AND THAT IS LOAD-BEARING.
-- Availability comes from ARRIVAL, never from anything a station says about
-- itself, because a station that has stopped cannot report that it stopped.
-- The client writes the tar into uploads/ on the gateway, the reflector
-- hard-links it (mtime preserved) and rsync -a preserves mtime to every WD
-- server — so this column is the instant the station finished delivering at
-- the gateway, on one server clock, identical on all three boards, with
-- nothing stamped by the station.  `emitted_at` is FORENSICS ONLY and must
-- never be used to decide whether a station is alive.

CREATE DATABASE IF NOT EXISTS station;

CREATE TABLE IF NOT EXISTS station.heartbeat
(
    `received_at`    DateTime               CODEC(Delta(4), ZSTD(3)),  -- tar mtime = gateway arrival
    `ingested_at`    DateTime DEFAULT now() CODEC(Delta(4), ZSTD(3)),
    `station`        LowCardinality(String) CODEC(ZSTD(3)),
    `host_id`        LowCardinality(String) CODEC(ZSTD(3)),
    `role`           LowCardinality(String) CODEC(ZSTD(3)),  -- 'vm' | 'pm', from the TAR PATH
    `gateway`        LowCardinality(String) CODEC(ZSTD(3)),  -- from the from-gwN spool dir
    `callsign`       LowCardinality(String) CODEC(ZSTD(3)),
    `grid`           LowCardinality(String) CODEC(ZSTD(3)),
    `emitted_at`     DateTime               CODEC(Delta(4), ZSTD(3)),  -- FORENSICS ONLY
    `interval_sec`   UInt16                 CODEC(ZSTD(3)),
    `schema_version` UInt16                 CODEC(ZSTD(3)),
    `rollup_verdict` LowCardinality(String) CODEC(ZSTD(3)),
    `rollup_block`   LowCardinality(String) CODEC(ZSTD(3)),
    `rollup_reason`  String                 CODEC(ZSTD(3)),
    `verdict`        Map(LowCardinality(String), LowCardinality(String)),
    `reason`         Map(LowCardinality(String), String),
    `envelope_sha`   FixedString(64)        CODEC(ZSTD(3)),
    `payload`        String                 CODEC(ZSTD(3))   -- the envelope, verbatim
)
ENGINE = ReplacingMergeTree(ingested_at)
PARTITION BY toYYYYMM(received_at)
ORDER BY (station, emitted_at, envelope_sha)
TTL received_at + INTERVAL 13 MONTH;

-- Choices worth their rationale:
--
-- * `verdict` / `reason` are Maps, not columns.  Adding a block is a schema
--   change in heartbeat_schema.py; it must not ALSO be a ClickHouse
--   migration, or the fleet loses visibility of exactly the hosts that were
--   updated most recently.
--
-- * `payload` holds the envelope verbatim.  The rule that a block which
--   vanished can never read as healthy only holds if the original is
--   recoverable.  The derived columns are a convenience; the envelope is the
--   evidence.
--
-- * ORDER BY (station, emitted_at, envelope_sha) collapses duplicates.  A
--   retry landing after a partial success yields two rows with the same key
--   and ReplacingMergeTree keeps one.  Their `received_at` differ by the
--   delivery gap, so the surviving value is whichever merged last — a
--   difference of seconds, said here so nobody later reads it as exact.
