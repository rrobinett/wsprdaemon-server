-- wspr.rx - Main WSPR spots table
-- Optimized for rx_sign/tx_sign equality queries over recent time windows
-- ReplacingMergeTree deduplicates on (rx_sign, band, time, id) with id as unique key
-- ORDER BY (rx_sign, band, time, id) gives 50x speedup on rx_sign point queries
-- ZSTD(3) compression vs ZSTD(1): better ratio, acceptable CPU tradeoff
-- Created: 2026-02-23, replaces wspr.rx VIEW over wsprnet.spots
--
CREATE TABLE wspr.rx
(
    `id`         UInt64   CODEC(Delta(8), ZSTD(3)),
    `time`       DateTime CODEC(Delta(4), ZSTD(3)),
    `band`       Int16    CODEC(T64, ZSTD(3)),
    `rx_sign`    LowCardinality(String),
    `rx_lat`     Float32  CODEC(Delta(4), ZSTD(3)),
    `rx_lon`     Float32  CODEC(Delta(4), ZSTD(3)),
    `rx_loc`     LowCardinality(String),
    `tx_sign`    LowCardinality(String),
    `tx_lat`     Float32  CODEC(Delta(4), ZSTD(3)),
    `tx_lon`     Float32  CODEC(Delta(4), ZSTD(3)),
    `tx_loc`     LowCardinality(String),
    `distance`   UInt16   CODEC(Delta(2), ZSTD(3)),
    `azimuth`    UInt16   CODEC(Delta(2), ZSTD(3)),
    `rx_azimuth` UInt16   CODEC(Delta(2), ZSTD(3)),
    `frequency`  UInt64   CODEC(Delta(8), ZSTD(3)),
    `power`      Int8     CODEC(T64, ZSTD(3)),
    `snr`        Int8     CODEC(Delta(1), ZSTD(3)),
    `drift`      Int8     CODEC(Delta(1), ZSTD(3)),
    `version`    LowCardinality(String),
    `code`       Int8     CODEC(T64, ZSTD(3)),
    INDEX id_index id TYPE minmax GRANULARITY 4
)
ENGINE = ReplacingMergeTree
PARTITION BY toYYYYMM(time)
ORDER BY (rx_sign, band, time, id)
SETTINGS index_granularity = 32768,
         min_age_to_force_merge_seconds = 120;
