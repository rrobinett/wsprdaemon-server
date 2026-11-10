-- sigmondb: registry of chirp (CODAR) and HFDL receptions.
--
-- Provenance: /home/mjh/hamsci/tasks/chirp-registry/2026-11-09-chirp-registry-design.md
-- (Tables section); plan 2026-11-09-sigmondb-first-build, Task 1.
-- Mirrors sigmondb/ddl.py; tests/test_sigmondb_ddl.py keeps the two equal.
-- Regenerate: python3 -m sigmondb.ddl > schema/sigmondb.sql
--
-- Dedup keys: ReplacingMergeTree(ingested_at) collapses a re-ingested row
-- onto its key.  arrival keys on delay to the microsecond (intDiv(delay_ns, 1000)),
-- so a re-run of the detector at the same version replaces rather than doubles
-- only while each delay lands in the same 1 us bin; a delay that moves across a
-- bin edge between runs leaves two rows.
-- `heard` is a materialized view into heard_state; read it with
-- maxMerge(last_time) and argMaxMerge(last_level_db).

CREATE DATABASE IF NOT EXISTS sigmondb;

CREATE TABLE IF NOT EXISTS sigmondb.transmitter
(
    `tx_id` String CODEC(ZSTD(3)),
    `kind` LowCardinality(String) CODEC(ZSTD(3)),
    `network` LowCardinality(String) CODEC(ZSTD(3)),
    `name` String CODEC(ZSTD(3)),
    `status` LowCardinality(String) CODEC(ZSTD(3)),
    `lat` Nullable(Float64) CODEC(ZSTD(3)),
    `lon` Nullable(Float64) CODEC(ZSTD(3)),
    `pos_source` LowCardinality(String) CODEC(ZSTD(3)),
    `pos_sigma_m` Nullable(Float32) CODEC(ZSTD(3)),
    `valid_from` DateTime CODEC(Delta(4), ZSTD(3)),
    `version` UInt32 CODEC(ZSTD(3)),
    `source` String CODEC(ZSTD(3)),
    `note` String CODEC(ZSTD(3)),
    `ingested_at` DateTime DEFAULT now() CODEC(Delta(4), ZSTD(3))
)
ENGINE = ReplacingMergeTree(ingested_at)
PARTITION BY toYYYYMM(valid_from)
ORDER BY (tx_id, valid_from, version);

CREATE TABLE IF NOT EXISTS sigmondb.waveform
(
    `tx_id` String CODEC(ZSTD(3)),
    `valid_from` DateTime CODEC(Delta(4), ZSTD(3)),
    `centre_hz` Float64 CODEC(ZSTD(3)),
    `rate_hz_per_s` Float64 CODEC(ZSTD(3)),
    `period_s` Nullable(Float64) CODEC(ZSTD(3)),
    `duration_s` Nullable(Float64) CODEC(ZSTD(3)),
    `bandwidth_hz` Nullable(Float64) CODEC(ZSTD(3)),
    `gate_period_s` Nullable(Float64) CODEC(ZSTD(3)),
    `gate_duty` Nullable(Float64) CODEC(ZSTD(3)),
    `gate_phase_s` Nullable(Float64) CODEC(ZSTD(3)),
    `frame_s` Nullable(Float64) CODEC(ZSTD(3)),
    `keying_step_ns` Nullable(Float64) CODEC(ZSTD(3)),
    `keying_modulus_ns` Nullable(Float64) CODEC(ZSTD(3)),
    `source` String CODEC(ZSTD(3)),
    `measured_by` String CODEC(ZSTD(3)),
    `ingested_at` DateTime DEFAULT now() CODEC(Delta(4), ZSTD(3))
)
ENGINE = ReplacingMergeTree(ingested_at)
ORDER BY (tx_id, centre_hz, valid_from);

CREATE TABLE IF NOT EXISTS sigmondb.receiver
(
    `reporter_id` String CODEC(ZSTD(3)),
    `host_id` String CODEC(ZSTD(3)),
    `radiod_id` String CODEC(ZSTD(3)),
    `lat` Nullable(Float64) CODEC(ZSTD(3)),
    `lon` Nullable(Float64) CODEC(ZSTD(3)),
    `alt_m` Nullable(Float64) CODEC(ZSTD(3)),
    `pos_source` LowCardinality(String) CODEC(ZSTD(3)),
    `pos_sigma_m` Nullable(Float32) CODEC(ZSTD(3)),
    `antenna` String CODEC(ZSTD(3)),
    `receiver_chain` String CODEC(ZSTD(3)),
    `coherence_group` String CODEC(ZSTD(3)),
    `a_level` LowCardinality(String) CODEC(ZSTD(3)),
    `valid_from` DateTime CODEC(Delta(4), ZSTD(3)),
    `version` UInt32 CODEC(ZSTD(3)),
    `ingested_at` DateTime DEFAULT now() CODEC(Delta(4), ZSTD(3))
)
ENGINE = ReplacingMergeTree(ingested_at)
ORDER BY (reporter_id, valid_from, version);

CREATE TABLE IF NOT EXISTS sigmondb.capture
(
    `time` DateTime64(3) CODEC(Delta(8), ZSTD(3)),
    `reporter_id` String CODEC(ZSTD(3)),
    `centre_hz` Float64 CODEC(ZSTD(3)),
    `rate_hz_per_s` Float64 CODEC(ZSTD(3)),
    `duration_s` Float32 CODEC(ZSTD(3)),
    `n_sweeps` Nullable(UInt32) CODEC(ZSTD(3)),
    `expected` Map(String, Float32),
    `null_max_db` Nullable(Float32) CODEC(ZSTD(3)),
    `null_median_db` Nullable(Float32) CODEC(ZSTD(3)),
    `n_expected` Nullable(UInt32) CODEC(ZSTD(3)),
    `n_found` Nullable(UInt32) CODEC(ZSTD(3)),
    `detector_version` LowCardinality(String) CODEC(ZSTD(3)),
    `ring_ref` String CODEC(ZSTD(3)),
    `schema_version` UInt16 CODEC(ZSTD(3)),
    `u_epoch_ns` Nullable(Float64) CODEC(ZSTD(3)),
    `k` Nullable(Float32) CODEC(ZSTD(3)),
    `p` Nullable(Float32) CODEC(ZSTD(3)),
    `a_level` LowCardinality(String) CODEC(ZSTD(3)),
    `a_level_provenance` LowCardinality(String) CODEC(ZSTD(3)),
    `counter_epoch_id` String CODEC(ZSTD(3)),
    `origin` LowCardinality(String) CODEC(ZSTD(3)),
    `engineering` Map(String, String),
    `received_at` Nullable(DateTime) CODEC(ZSTD(3)),
    `ingested_at` DateTime DEFAULT now() CODEC(Delta(4), ZSTD(3))
)
ENGINE = ReplacingMergeTree(ingested_at)
PARTITION BY toYYYYMM(time)
ORDER BY (reporter_id, centre_hz, rate_hz_per_s, time, detector_version);

CREATE TABLE IF NOT EXISTS sigmondb.arrival
(
    `time` DateTime64(9) CODEC(Delta(8), ZSTD(3)),
    `reporter_id` String CODEC(ZSTD(3)),
    `tx_id` String CODEC(ZSTD(3)),
    `group_key` LowCardinality(String) CODEC(ZSTD(3)),
    `centre_hz` Float64 CODEC(ZSTD(3)),
    `rate_hz_per_s` Float64 CODEC(ZSTD(3)),
    `delay_ns` Int64 CODEC(ZSTD(3)),
    `delay_sigma_ns` Nullable(Float32) CODEC(ZSTD(3)),
    `level_db` Float32 CODEC(ZSTD(3)),
    `margin_db` Nullable(Float32) CODEC(ZSTD(3)),
    `half_band_db` Nullable(Float32) CODEC(ZSTD(3)),
    `clean_tag` LowCardinality(String) CODEC(ZSTD(3)),
    `doppler_hz` Nullable(Float32) CODEC(ZSTD(3)),
    `phase_rate` Nullable(Float32) CODEC(ZSTD(3)),
    `carrier_phase_rad` Nullable(Float32) CODEC(ZSTD(3)),
    `s4` Nullable(Float32) CODEC(ZSTD(3)),
    `sigma_phi_rad` Nullable(Float32) CODEC(ZSTD(3)),
    `detector_version` LowCardinality(String) CODEC(ZSTD(3)),
    `schema_version` UInt16 CODEC(ZSTD(3)),
    `u_epoch_ns` Nullable(Float64) CODEC(ZSTD(3)),
    `k` Nullable(Float32) CODEC(ZSTD(3)),
    `p` Nullable(Float32) CODEC(ZSTD(3)),
    `a_level` LowCardinality(String) CODEC(ZSTD(3)),
    `a_level_provenance` LowCardinality(String) CODEC(ZSTD(3)),
    `counter_epoch_id` String CODEC(ZSTD(3)),
    `origin` LowCardinality(String) CODEC(ZSTD(3)),
    `engineering` Map(String, String),
    `received_at` Nullable(DateTime) CODEC(ZSTD(3)),
    `ingested_at` DateTime DEFAULT now() CODEC(Delta(4), ZSTD(3))
)
ENGINE = ReplacingMergeTree(ingested_at)
PARTITION BY toYYYYMM(time)
ORDER BY (reporter_id, centre_hz, rate_hz_per_s, time, detector_version, intDiv(delay_ns, 1000));

CREATE TABLE IF NOT EXISTS sigmondb.solution
(
    `tx_id` String CODEC(ZSTD(3)),
    `cluster_key` String CODEC(ZSTD(3)),
    `solve_id` String CODEC(ZSTD(3)),
    `window_from` DateTime64(3) CODEC(Delta(8), ZSTD(3)),
    `window_to` DateTime64(3) CODEC(Delta(8), ZSTD(3)),
    `reporter_ids` Array(String),
    `fixed_delay_ns` Int64 CODEC(ZSTD(3)),
    `delay_sd_ns` Float64 CODEC(ZSTD(3)),
    `offset_ns` Nullable(Int64) CODEC(ZSTD(3)),
    `offset_sigma_ns` Nullable(Float64) CODEC(ZSTD(3)),
    `lat` Nullable(Float64) CODEC(ZSTD(3)),
    `lon` Nullable(Float64) CODEC(ZSTD(3)),
    `pos_sigma_m` Nullable(Float32) CODEC(ZSTD(3)),
    `method` LowCardinality(String) CODEC(ZSTD(3)),
    `n_stations` UInt16 CODEC(ZSTD(3)),
    `n_arrivals` UInt32 CODEC(ZSTD(3)),
    `set_aside` Map(String, String),
    `solver_version` LowCardinality(String) CODEC(ZSTD(3)),
    `inputs_digest` FixedString(64) CODEC(ZSTD(3)),
    `ingested_at` DateTime DEFAULT now() CODEC(Delta(4), ZSTD(3))
)
ENGINE = ReplacingMergeTree(ingested_at)
PARTITION BY toYYYYMM(window_from)
ORDER BY (cluster_key, solve_id);

CREATE TABLE IF NOT EXISTS sigmondb.path
(
    `time` DateTime CODEC(Delta(4), ZSTD(3)),
    `tx_id` String CODEC(ZSTD(3)),
    `reporter_id` String CODEC(ZSTD(3)),
    `excess_delay_ns` Nullable(Float64) CODEC(ZSTD(3)),
    `mode` LowCardinality(String) CODEC(ZSTD(3)),
    `virtual_height_km` Nullable(Float32) CODEC(ZSTD(3)),
    `solver_version` LowCardinality(String) CODEC(ZSTD(3)),
    `ingested_at` DateTime DEFAULT now() CODEC(Delta(4), ZSTD(3))
)
ENGINE = ReplacingMergeTree(ingested_at)
PARTITION BY toYYYYMM(time)
ORDER BY (tx_id, reporter_id, time);

CREATE TABLE IF NOT EXISTS sigmondb.heard_state
(
    `reporter_id` String,
    `tx_id` String,
    `group_key` LowCardinality(String),
    `last_time` AggregateFunction(max, DateTime64(9)),
    `last_level_db` AggregateFunction(argMax, Float32, DateTime64(9))
)
ENGINE = AggregatingMergeTree
ORDER BY (reporter_id, tx_id, group_key);

CREATE MATERIALIZED VIEW IF NOT EXISTS sigmondb.heard
TO sigmondb.heard_state
AS SELECT
    reporter_id,
    tx_id,
    group_key,
    maxState(time) AS last_time,
    argMaxState(level_db, time) AS last_level_db
FROM sigmondb.arrival
GROUP BY reporter_id, tx_id, group_key;

ALTER TABLE sigmondb.capture ADD COLUMN IF NOT EXISTS received_at Nullable(DateTime) CODEC(ZSTD(3)) AFTER engineering;

ALTER TABLE sigmondb.arrival ADD COLUMN IF NOT EXISTS received_at Nullable(DateTime) CODEC(ZSTD(3)) AFTER engineering;
