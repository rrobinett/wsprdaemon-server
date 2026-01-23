#!/bin/bash
# optimize_wsprnet_spots_live.sh - Minimal disruption to live wsprnet-scraper
# 
# This script optimizes the wsprnet.spots table structure while keeping the
# scraper running during bulk migration. Only stops scraper for ~10 seconds
# for the final atomic swap.
#
# Usage: ./optimize_wsprnet_spots_live.sh
#

set -e

CLICKHOUSE_USER="${CLICKHOUSE_USER:-chadmin}"
SCRAPER_SERVICE="wsprnet_scraper@wsprnet.service"
LOG_FILE="/tmp/wsprnet_optimization_$(date +%Y%m%d_%H%M%S).log"

# Prompt for ClickHouse password once at the start
echo "Enter ClickHouse password for user $CLICKHOUSE_USER:"
read -s CLICKHOUSE_PASSWORD
echo ""

# Test the password immediately
if ! clickhouse-client --user="$CLICKHOUSE_USER" --password="$CLICKHOUSE_PASSWORD" --query="SELECT 1" &>/dev/null; then
    echo "ERROR: Invalid ClickHouse credentials"
    exit 1
fi

# Function to log with timestamp
log() {
    echo "[$(date '+%Y-%m-%d %H:%M:%S')] $*" | tee -a "$LOG_FILE"
}

# Function to run ClickHouse query
ch_query() {
    clickhouse-client --user="$CLICKHOUSE_USER" --password="$CLICKHOUSE_PASSWORD" --query="$1"
}

# Function to run ClickHouse multiquery
ch_multiquery() {
    clickhouse-client --user="$CLICKHOUSE_USER" --password="$CLICKHOUSE_PASSWORD" --multiquery
}

log "=== Live WsprNet.spots Optimization ==="
log "Log file: $LOG_FILE"
log "Scraper will only be stopped for ~10 seconds during table swap"
echo ""

# Check if scraper is running
if ! systemctl is-active --quiet "$SCRAPER_SERVICE"; then
    log "WARNING: $SCRAPER_SERVICE is not currently running"
    read -p "Continue anyway? (yes/no): " confirm
    [ "$confirm" != "yes" ] && exit 1
fi

# Check current row count
log "Checking current data volume..."
CURRENT_ROWS=$(ch_query "SELECT count() FROM wsprnet.spots")
CURRENT_SIZE=$(ch_query "SELECT formatReadableSize(sum(bytes)) FROM system.parts WHERE database='wsprnet' AND table='spots' AND active")
log "Current rows: $CURRENT_ROWS"
log "Current size: $CURRENT_SIZE"
echo ""

# Verify we have permission to stop/start service
if ! sudo -n systemctl status "$SCRAPER_SERVICE" &>/dev/null; then
    log "ERROR: Need sudo permission to control $SCRAPER_SERVICE"
    log "Run: sudo -v"
    exit 1
fi

read -p "Ready to begin optimization? (yes/no): " confirm
[ "$confirm" != "yes" ] && exit 1

# Step 1: Create optimized table (scraper keeps running)
log ""
log "Step 1: Creating optimized table (scraper still running)..."
ch_multiquery <<'SQL'
CREATE TABLE IF NOT EXISTS wsprnet.spots_optimized
(
    `id` UInt64 CODEC(Delta(8), ZSTD(1)),
    `time` DateTime CODEC(Delta(4), ZSTD(1)),
    `band` Int16 CODEC(T64, ZSTD(1)),
    `rx_sign` LowCardinality(String),
    `rx_lat` Float32 CODEC(ZSTD(1)),
    `rx_lon` Float32 CODEC(ZSTD(1)),
    `rx_loc` LowCardinality(String),
    `tx_sign` LowCardinality(String),
    `tx_lat` Float32 CODEC(ZSTD(1)),
    `tx_lon` Float32 CODEC(ZSTD(1)),
    `tx_loc` LowCardinality(String),
    `distance` UInt16 CODEC(T64, ZSTD(1)),
    `azimuth` UInt16 CODEC(T64, ZSTD(1)),
    `rx_azimuth` UInt16 CODEC(T64, ZSTD(1)),
    `frequency` UInt64 CODEC(T64, ZSTD(1)),
    `power` Int8 CODEC(T64, ZSTD(1)),
    `snr` Int8 CODEC(ZSTD(1)),
    `drift` Int8 CODEC(ZSTD(1)),
    `version` LowCardinality(String),
    `code` Int8,
    `Spotnum` UInt64 ALIAS id,
    `Date` UInt32 ALIAS toUnixTimestamp(time),
    `Reporter` String ALIAS rx_sign,
    `ReporterGrid` String ALIAS rx_loc,
    `dB` Int8 ALIAS snr,
    `MHz` Float32 ALIAS frequency / 1000000.,
    `CallSign` String ALIAS tx_sign,
    `Grid` String ALIAS tx_loc,
    `Power` Int8 ALIAS power,
    `Drift` Int8 ALIAS drift,
    `Band` Int16 ALIAS band,
    `rx_az` UInt16 ALIAS rx_azimuth,
    INDEX id_index id TYPE minmax GRANULARITY 4
)
ENGINE = ReplacingMergeTree
PARTITION BY toYYYYMM(time)
ORDER BY (band, time, id)
SETTINGS index_granularity = 32768, min_age_to_force_merge_seconds = 120;
SQL
log "✓ Optimized table created"

# Step 2: Copy bulk of existing data (scraper keeps running)
log ""
log "Step 2: Copying existing data (scraper still running)..."
log "This may take several minutes depending on data volume..."
START_TIME=$(date +%s)

ch_query "INSERT INTO wsprnet.spots_optimized SELECT * FROM wsprnet.spots"

MIGRATED_ROWS=$(ch_query "SELECT count() FROM wsprnet.spots_optimized")
MIGRATION_DURATION=$(($(date +%s) - START_TIME))
log "✓ Migrated $MIGRATED_ROWS rows in $MIGRATION_DURATION seconds"

# Step 3: BRIEF scraper stop for final sync
log ""
log "Step 3: Stopping scraper for final sync..."
STOP_START_TIME=$(date +%s)
sudo systemctl stop "$SCRAPER_SERVICE"
sleep 2

# Copy any new records added during migration
log "Copying records added during bulk migration..."
NEW_RECORDS=$(ch_query "SELECT count() FROM wsprnet.spots WHERE time >= toDateTime($START_TIME)")
log "Found $NEW_RECORDS new records to sync"

if [ "$NEW_RECORDS" -gt 0 ]; then
    ch_query "INSERT INTO wsprnet.spots_optimized SELECT * FROM wsprnet.spots WHERE time >= toDateTime($START_TIME)"
    log "✓ Synced $NEW_RECORDS new records"
fi

# Atomic swap
log "Swapping tables (atomic operation)..."
ch_query "EXCHANGE TABLES wsprnet.spots AND wsprnet.spots_optimized"
log "✓ Tables swapped"

# Restart scraper
log "Restarting scraper..."
sudo systemctl start "$SCRAPER_SERVICE"
sleep 2

STOP_DURATION=$(($(date +%s) - STOP_START_TIME))

# Verify scraper is running
if systemctl is-active --quiet "$SCRAPER_SERVICE"; then
    log "✓ Scraper restarted successfully (downtime: ${STOP_DURATION}s)"
else
    log "ERROR: Scraper failed to restart!"
    log "Manual intervention required - check: sudo systemctl status $SCRAPER_SERVICE"
    exit 1
fi

# Step 4: Verification
log ""
log "Step 4: Verifying migration..."
sleep 3  # Give scraper time to write a record

FINAL_ROWS=$(ch_query "SELECT count() FROM wspr.rx")
NEW_STRUCTURE=$(ch_query "SELECT engine FROM system.tables WHERE database='wsprnet' AND name='spots'")
log "Final row count via wspr.rx: $FINAL_ROWS"
log "New table engine: $NEW_STRUCTURE"

# Check if scraper is writing to new table
RECENT_SPOTS=$(ch_query "SELECT count() FROM wsprnet.spots WHERE time >= now() - INTERVAL 60 SECOND")
log "Spots in last 60 seconds: $RECENT_SPOTS (scraper activity check)"

if [ "$FINAL_ROWS" -ge "$MIGRATED_ROWS" ]; then
    log "✓ Migration successful - row count verified"
    
    # Show the new optimized structure
    log ""
    log "New optimized table structure:"
    ch_query "SELECT 
        engine, 
        partition_key, 
        sorting_key,
        primary_key
    FROM system.tables 
    WHERE database='wsprnet' AND name='spots' 
    FORMAT Vertical" | tee -a "$LOG_FILE"
    
    log ""
    log "Cleaning up old table..."
    ch_query "DROP TABLE IF EXISTS wsprnet.spots_optimized"
    log "✓ Cleanup complete"
else
    log "ERROR: Row count decreased ($CURRENT_ROWS -> $FINAL_ROWS)!"
    log "Old table preserved as wsprnet.spots_optimized for investigation"
    exit 1
fi

TOTAL_TIME=$(($(date +%s) - START_TIME))

log ""
log "=== OPTIMIZATION COMPLETE ==="
log "Total time: ${TOTAL_TIME}s"
log "Scraper downtime: ${STOP_DURATION}s"
log "Records migrated: $MIGRATED_ROWS"
log ""
log "Key improvements:"
log "  ✓ ReplacingMergeTree engine (automatic deduplication)"
log "  ✓ ORDER BY (band, time, id) - band-first for faster queries"
log "  ✓ index_granularity = 32768 (optimized for bulk data)"
log "  ✓ Auto-merge enabled (keeps data compact)"
log "  ✓ minmax index on id"
log ""
log "Your wspr.rx view continues to work seamlessly"
log "Ready to load billions of cleaned records!"
log ""
log "Full log saved to: $LOG_FILE"
