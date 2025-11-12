# Database Maintenance Tools

Utilities for managing ClickHouse databases on WD1 and WD2.

## Synchronization Scripts

### sync_wsprnet_spots.sh
Synchronizes wsprnet.spots table between WD1 and WD2 using unique `id` column.
- Detects missing rows on each server
- Syncs bidirectionally
- Handles large datasets efficiently

**Usage:**
```bash
# Run on either WD1 or WD2
cd ~/wsprdaemon-server/database-tools
bash sync_wsprnet_spots.sh
```

### sync_wsprdaemon_tables.sh
Synchronizes wsprdaemon.spots and wsprdaemon.noise tables between WD1 and WD2.
- Uses epoch/time for deduplication
- Handles both spots and noise data
- Bidirectional sync

**Usage:**
```bash
# Run on either WD1 or WD2
bash sync_wsprdaemon_tables.sh
```

### sync-spots.sh
Legacy sync script for spots data.

### wd1-2-wd2-merge.sh
Merges data from WD1 to WD2.

### sync-wd2-users-v2.sh / sync-wd2-users.sh
Synchronizes ClickHouse user accounts between servers.

## Maintenance Utilities

### dedup_wsprtables.sh
Removes duplicate entries from WSPR tables.

**Usage:**
```bash
bash dedup_wsprtables.sh
```

### show_rows.sh
Shows row counts for WSPR tables on WD1 and WD2.

**Usage:**
```bash
bash show_rows.sh
```

### show-both-tables.sh
Displays information about tables on both servers.

### ch-flush.sh
Flushes ClickHouse logs and forces table optimization.

## Database Schema

### add_config_columns.sql
SQL script to add configuration columns to existing tables.

### grants.txt
User permissions and grants reference.

## Notes

- All sync scripts should be run as user `wsprdaemon`
- Requires SSH key authentication between WD1 and WD2
- ClickHouse `chadmin` user needs appropriate permissions
- Large syncs can take considerable time and resources

## Common Tasks

### Check table sizes
```bash
echo "SELECT database, table, formatReadableSize(total_bytes) as size 
FROM system.tables 
WHERE database IN ('wsprnet', 'wsprdaemon') 
ORDER BY total_bytes DESC" | clickhouse-client --user chadmin
```

### Check row counts
```bash
bash show_rows.sh
```

### Sync missing data between servers
```bash
# For wsprnet data
bash sync_wsprnet_spots.sh

# For wsprdaemon data
bash sync_wsprdaemon_tables.sh
```

### Remove duplicates
```bash
bash dedup_wsprtables.sh
```
