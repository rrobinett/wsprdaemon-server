-- Migration script to add running_jobs and receiver_descriptions columns to wsprdaemon.noise ONLY
-- Run this with: clickhouse-client --user wsprdaemon-admin --password wd-admin --multiquery < add_config_columns.sql

-- Add columns to wsprdaemon.noise with ZSTD compression
ALTER TABLE wsprdaemon.noise 
ADD COLUMN IF NOT EXISTS running_jobs Nullable(String) CODEC(ZSTD(1));

ALTER TABLE wsprdaemon.noise 
ADD COLUMN IF NOT EXISTS receiver_descriptions Nullable(String) CODEC(ZSTD(1));

-- Verify the columns were added to noise
DESCRIBE TABLE wsprdaemon.noise;
