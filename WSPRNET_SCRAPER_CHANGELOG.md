# WSPRNET Scraper v2.5.0 - Changelog

## Changes Made

### 1. Target Table Changed: wsprnet.spots → wspr.rx

**Changed lines:**
- Line 34: `'clickhouse_database': 'wspr'` (was 'wsprnet')
- Line 35: `'clickhouse_table': 'rx'` (was 'spots')

**Why:** 
- The wspr.rx table is a MergeTree table (not a View)
- Supports PREWHERE for much faster time-based queries
- Matches your migration plan to replace the wspr.rx view with a real table

### 2. Enhanced Caching for Failed Inserts

**Modified function:** `insert_cached_file()` (lines 511-588)

**New Features:**
- Tracks retry count in cache file metadata
- Records last error message and timestamp
- Keeps failed spots in cache for automatic retry
- Shows retry count in log messages

**How it works:**
When an insert fails:
1. Cache file is updated with retry metadata:
   ```json
   {
     "spots": [...],
     "retry_count": 1,
     "last_error": "Connection timeout",
     "last_retry": "2026-01-28T01:23:45.678Z"
   }
   ```
2. File stays in cache directory
3. Will be retried on next insert cycle
4. Logs show: "Failed to insert file.json (retry 1): error message"

### 3. Exponential Backoff for Failed Inserts

**Modified function:** `insert_thread_worker()` (lines 560-598)

**New Features:**
- Tracks consecutive insert failures
- Implements exponential backoff delay
- Prevents hammering database during outages

**Backoff schedule:**
- 1st failure: 5 seconds
- 2nd failure: 10 seconds
- 3rd failure: 20 seconds
- 4th failure: 30 seconds
- 5th+ failure: 60 seconds (max)

**Example log output:**
```
[2026-01-28 01:23:45] ERROR: Failed to insert spots_123456.json (retry 1): Connection refused
[2026-01-28 01:23:45] INFO: Spots cached for retry - file will be retried later
[2026-01-28 01:23:45] WARNING: Insert failed, backing off for 5 seconds (failure #1)
```

### 4. Table ORDER BY Updated

**Changed:** Table creation in `setup_clickhouse_tables()` (line 676)

**Old:** `ORDER BY (time, id)`
**New:** `ORDER BY (time, band, id)`

**Why:**
- Matches the migration script changes
- Optimizes for time-based PREWHERE queries
- Band as secondary sort key still efficient for band filtering

### 5. Documentation Updates

**Updated:**
- Version: 2.4.0 → 2.5.0
- Header documentation explains retry mechanism
- Startup banner shows new architecture

## Behavioral Changes

### Before (v2.4.0):
- Wrote to `wsprnet.spots` table
- Failed inserts were logged but file stayed in cache
- No retry delay (would immediately retry failing file)
- No tracking of retry attempts

### After (v2.5.0):
- Writes to `wspr.rx` table (supports PREWHERE!)
- Failed inserts tracked with metadata
- Exponential backoff prevents database hammering
- Retry count visible in logs
- Automatic recovery when database comes back online

## Deployment Notes

### Prerequisites:
1. Run the migration script first to create wspr.rx table:
   ```bash
   ./migrate_wspr_rx_with_service_control.sh
   ```

2. This will:
   - Create wspr.rx table with ORDER BY (time, band, id)
   - Copy data from wsprnet.spots
   - Enable PREWHERE support

### Installing Updated Scraper:
```bash
# Stop the scraper
sudo systemctl stop wsprnet_scraper

# Backup old version
sudo cp /usr/local/bin/wsprnet_scraper.py /usr/local/bin/wsprnet_scraper.py.v2.4.0

# Install new version
sudo cp wsprnet_scraper.py /usr/local/bin/wsprnet_scraper.py
sudo chmod +x /usr/local/bin/wsprnet_scraper.py

# Start the scraper
sudo systemctl start wsprnet_scraper

# Check status
sudo systemctl status wsprnet_scraper

# Watch logs
sudo journalctl -u wsprnet_scraper -f
```

## Cache Files

### New Cache File Format:
```json
{
  "timestamp": "2026-01-28T01:23:45.678Z",
  "count": 150,
  "spots": [ ... ],
  "retry_count": 2,
  "last_error": "Connection to ClickHouse failed",
  "last_retry": "2026-01-28T01:25:45.678Z"
}
```

### Cache Management:
- Successful inserts: file deleted automatically
- Failed inserts: file kept with retry metadata
- Files processed in chronological order (oldest first)
- No manual cleanup needed - retries automatic

## Testing

### Test the changes:
```bash
# Monitor scraper startup
sudo journalctl -u wsprnet_scraper -f

# You should see:
# - "WSPRNET Scraper version 2.5.0 starting..."
# - "Insert thread: Connected to ClickHouse"
# - "Inserted X spots from file.json (highest id: Y)"

# Check target table
clickhouse-client --query="SELECT COUNT(*) FROM wspr.rx"

# Verify PREWHERE works
clickhouse-client --query="
  SELECT COUNT(*) FROM wspr.rx 
  PREWHERE time >= today() - 1
"
```

### Test retry mechanism:
```bash
# Temporarily stop ClickHouse
sudo systemctl stop clickhouse-server

# Watch scraper logs - should see:
# - "Failed to insert" errors
# - "Spots cached for retry"
# - "Insert failed, backing off for X seconds"

# Restart ClickHouse
sudo systemctl start clickhouse-server

# Scraper should automatically resume inserting cached files
```

## Rollback Plan

If issues occur:
```bash
# Stop scraper
sudo systemctl stop wsprnet_scraper

# Restore old version
sudo cp /usr/local/bin/wsprnet_scraper.py.v2.4.0 /usr/local/bin/wsprnet_scraper.py

# Revert database (if needed)
# - wspr.rx table can stay (compatible)
# - Or restore from backup
```

## Performance Impact

**Positive:**
- PREWHERE queries on wspr.rx are MUCH faster (10-100x for time-based filters)
- Backoff prevents CPU waste during outages
- Retry tracking shows health status

**Neutral:**
- Same download performance
- Same insert throughput when healthy
- Slightly larger cache files (added metadata ~100 bytes)

## Summary

This update makes the scraper more robust and optimizes for your client's PREWHERE query pattern. The key improvements:

1. ✓ Writes to wspr.rx (PREWHERE supported!)
2. ✓ Automatic retry with tracking
3. ✓ Exponential backoff prevents hammering
4. ✓ Clear visibility into failures
5. ✓ Graceful recovery from outages

The scraper will now cache spots reliably during any wspr.rx downtime and automatically recover when the table is available again.
