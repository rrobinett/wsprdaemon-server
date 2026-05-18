-- psk.spots — FT8 / FT4 / MSK144 / ... spots ingested from psk-recorder via
-- the wsprdaemon-server tar pipeline. Schema is mode-agnostic on purpose so
-- future modulations (msk144, q65, ...) can land in the same table by tagging
-- `mode` and (optionally) using mode-specific scoring in `score` / `snr_db`.
--
-- Provenance:
--   * Producer:    psk-recorder ch_tailer (sigmond.hamsci_ch.Writer)
--   * Transport:   tar payload via gw{1,2} SFTP, mode/<RX_SITE>/<RECEIVER>/<BAND>/...
--   * Forwarding:  the `forward_to_pskreporter` flag tells the gw1-elected
--                  pskreporter_forwarder service whether to POST this row to
--                  pskreporter.info. Populated server-side from the per-tar
--                  routing.json (default true) — never overrides client intent.
--
-- This table coexists with Andrew Roland's `pskreporter.rx` (PSK Reporter
-- scraper output, different ingest path, different schema). Do not merge.
--
-- Created: 2026-05-18 (Phase 2 PR 1)
--
CREATE TABLE IF NOT EXISTS psk.spots
(
    -- Timing / identity
    `time`                  DateTime               CODEC(Delta(4), ZSTD(3)),
    `ingested_at`           DateTime DEFAULT now() CODEC(Delta(4), ZSTD(3)),
    `mode`                  LowCardinality(String) CODEC(ZSTD(3)),

    -- Decoder provenance (lets the forwarder distinguish jt9 vs decode_ft8 lines)
    `decoder_kind`          LowCardinality(String) CODEC(ZSTD(3)),

    -- Receiver fan-out keys: a host can run multiple radiods, each with its
    -- own callsign+grid. rx_sign+receiver+host_id together uniquely identify
    -- the receive chain that produced the spot.
    `rx_sign`               LowCardinality(String) CODEC(ZSTD(3)),
    `rx_loc`                LowCardinality(String) CODEC(ZSTD(3)),
    `rx_site`               LowCardinality(String) CODEC(ZSTD(3)),
    `receiver`              LowCardinality(String) CODEC(ZSTD(3)),
    `host_id`               LowCardinality(String) CODEC(ZSTD(3)),

    -- Decoded spot content
    `tx_call`               LowCardinality(String) CODEC(ZSTD(3)),
    `grid`                  LowCardinality(String) CODEC(ZSTD(3)),
    `report`                Nullable(Int16)        CODEC(ZSTD(3)),
    `message`               String                 CODEC(ZSTD(3)),

    -- Numeric measurements
    `frequency`             UInt64    CODEC(Delta(8), ZSTD(3)),  -- absolute Hz
    `band`                  Int16     CODEC(T64, ZSTD(3)),       -- metres (0 = unknown)
    `snr_db`                Nullable(Int16)  CODEC(ZSTD(3)),
    `score`                 Nullable(Int16)  CODEC(ZSTD(3)),     -- jt9 sync or ft8_lib score
    `dt`                    Float32   CODEC(Delta(4), ZSTD(3)),
    `spectral_width_hz`     Nullable(Float32) CODEC(ZSTD(3)),

    -- Routing flag — server's pskreporter_forwarder picks up rows WHERE
    -- forward_to_pskreporter = true AND not-yet-forwarded.
    `forward_to_pskreporter` UInt8    DEFAULT 1 CODEC(ZSTD(3)),

    -- Processing-pipeline version (psk-recorder build identifier)
    `processing_version`    LowCardinality(String) CODEC(ZSTD(3)),

    INDEX rx_sign_idx rx_sign TYPE bloom_filter GRANULARITY 1,
    INDEX tx_call_idx tx_call TYPE bloom_filter GRANULARITY 1
)
ENGINE = ReplacingMergeTree
PARTITION BY toYYYYMM(time)
ORDER BY (mode, rx_sign, receiver, time, frequency)
SETTINGS index_granularity = 8192,
         min_age_to_force_merge_seconds = 120;
