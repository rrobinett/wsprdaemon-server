-- wsprdaemon.wsprnet_validation, wsprdaemon.wsprnet_missing,
-- view wsprdaemon.wsprnet_missing_current
--
-- Written hourly by wsprnet_validator.py: which spots stations sent to the
-- WD servers (wsprdaemon.spots) never appeared on wsprnet.org (wspr.rx).
-- Mirrored inline in wsprnet_validator.DDL so a fresh server self-bootstraps;
-- keep the two in step.
--
-- Every run re-judges each hour in its window under one checked_at, writing
-- wsprnet_missing BEFORE wsprnet_validation.  Read missing spots through
-- wsprnet_missing_current, which keeps only each hour's newest complete
-- verdict; superseded rows age out with the 30-day TTL.

CREATE TABLE IF NOT EXISTS wsprdaemon.wsprnet_validation
(
    `hour`             DateTime                CODEC(Delta(4), ZSTD(3)),
    `reporter`         LowCardinality(String),
    `proxy`            UInt8,
    `wd_spots`         UInt32,
    `on_wsprnet`       UInt32,
    `missing`          UInt32,
    `wsprnet_keys`     UInt32,
    `extra_on_wsprnet` UInt32,
    `dup_identical`    UInt32,
    `dup_diff_snr`     UInt32,
    `checked_at`       DateTime                CODEC(Delta(4), ZSTD(3))
)
ENGINE = ReplacingMergeTree(checked_at)
PARTITION BY toYYYYMM(hour)
ORDER BY (hour, reporter)
TTL hour + INTERVAL 180 DAY;

CREATE TABLE IF NOT EXISTS wsprdaemon.wsprnet_missing
(
    `hour`        DateTime                CODEC(Delta(4), ZSTD(3)),
    `reporter`    LowCardinality(String),
    `time`        DateTime                CODEC(Delta(4), ZSTD(3)),
    `band`        Int16,
    `tx_sign`     LowCardinality(String),
    `rx_sign`     LowCardinality(String),
    `rx_id`       LowCardinality(String),
    `frequency`   UInt64                  CODEC(Delta(8), ZSTD(3)),
    `snr`         Int8,
    `proxy`       UInt8,
    `checked_at`  DateTime                CODEC(Delta(4), ZSTD(3))
)
ENGINE = MergeTree
PARTITION BY toYYYYMM(hour)
ORDER BY (hour, reporter, time, band, tx_sign)
TTL hour + INTERVAL 30 DAY;

CREATE VIEW IF NOT EXISTS wsprdaemon.wsprnet_missing_current AS
SELECT * FROM wsprdaemon.wsprnet_missing
WHERE (hour, checked_at) IN
    (SELECT hour, max(checked_at) FROM wsprdaemon.wsprnet_validation GROUP BY hour);
