# PostgreSQL to ClickHouse Migration Guide

## Overview
These scripts migrate data from PostgreSQL tables to ClickHouse tables with:
- Incremental batch processing
- Resume capability on errors
- Progress tracking
- Schema validation

## Table Mappings

PostgreSQL (tutorial database) → ClickHouse (wsprdaemon database):
- `wsprdaemon_spots` → `wsprdaemon.spots_extended`
- `wsprdaemon_noise` → `wsprdaemon.noise`

## Files

1. **test_pg_to_ch_migration.sh** - Test migration with a few rows
2. **migrate_pg_to_ch.sh** - Full incremental migration (single table)
3. **migrate_all_tables.sh** - Migrate both tables sequentially

## Migration Process

### Step 1: Test Migration (IMPORTANT!)

Run this first to validate schemas and test with 10 rows:

```bash
chmod +x test_pg_to_ch_migration.sh
./test_pg_to_ch_migration.sh
```

This will:
- Check PostgreSQL and ClickHouse table schemas
- Compare column names and types
- Export 10 test rows from PostgreSQL
- Import to temporary ClickHouse test table
- Verify data integrity
- Report if migration is ready

**Only proceed if tests pass!**

### Step 2: Full Migration

After successful testing, migrate the tables:

#### Option A: Migrate both tables
```bash
chmod +x migrate_all_tables.sh
./migrate_all_tables.sh
```

#### Option B: Migrate one table at a time
```bash
chmod +x migrate_pg_to_ch.sh

# Migrate wsprdaemon_spots to spots_extended
./migrate_pg_to_ch.sh wsprdaemon_spots spots_extended

# Migrate wsprdaemon_noise to noise
./migrate_pg_to_ch.sh wsprdaemon_noise noise
```

### Step 3: Monitor Progress

Watch the migration in real-time:
```bash
tail -f /tmp/pg_to_ch_migration.log
```

Check progress files:
```bash
cat /tmp/pg_to_ch_progress/wsprdaemon_spots.offset
cat /tmp/pg_to_ch_progress/wsprdaemon_noise.offset
```

## Configuration

Environment variables (with defaults):

```bash
# Batch size (rows per batch)
BATCH_SIZE=10000

# PostgreSQL connection
PG_HOST=localhost
PG_USER=wdread
PG_PASSWORD=JTWSPR2008
PG_DB=tutorial

# ClickHouse connection
CH_HOST=localhost
CH_USER=default
CH_DB=wsprdaemon

# Progress tracking
PROGRESS_DIR=/tmp/pg_to_ch_progress
LOG_FILE=/tmp/pg_to_ch_migration.log
```

Example with custom settings:
```bash
BATCH_SIZE=5000 PG_HOST=wd1 ./migrate_pg_to_ch.sh spots_extended
```

## Resume on Error

If migration fails or is interrupted:

1. Fix the issue (network, disk space, etc.)
2. Simply run the same command again
3. Migration will resume from last saved offset

```bash
# Will automatically resume from where it stopped
./migrate_pg_to_ch.sh wsprdaemon_spots spots_extended
```

Progress is saved after each batch in:
- `/tmp/pg_to_ch_progress/wsprdaemon_spots.offset`
- `/tmp/pg_to_ch_progress/wsprdaemon_noise.offset`

## Reset Migration

To start over from scratch:

```bash
# Remove progress files
rm -f /tmp/pg_to_ch_progress/wsprdaemon_spots.*
rm -f /tmp/pg_to_ch_progress/wsprdaemon_noise.*

# Then re-run migration
./migrate_pg_to_ch.sh wsprdaemon_spots spots_extended
```

## Verification

After migration, verify row counts:

```bash
# PostgreSQL
psql -U wdread -d tutorial -c "SELECT COUNT(*) FROM wsprdaemon_spots;"
psql -U wdread -d tutorial -c "SELECT COUNT(*) FROM wsprdaemon_noise;"

# ClickHouse
clickhouse-client --query="SELECT COUNT(*) FROM wsprdaemon.spots_extended"
clickhouse-client --query="SELECT COUNT(*) FROM wsprdaemon.noise"
```

## Troubleshooting

### Schema Mismatch
If test shows column name differences:
- Check PostgreSQL schema: `\d table_name` in psql
- Check ClickHouse schema: `DESCRIBE TABLE wsprdaemon.table_name`
- Ensure columns match exactly

### Import Errors
If ClickHouse import fails:
- Check data types are compatible
- Look for NULL values if columns are NOT NULL
- Check for special characters in data
- Review `/tmp/pg_to_ch_migration.log`

### Performance Tuning
- Increase `BATCH_SIZE` for faster migration (but more memory)
- Decrease `BATCH_SIZE` if hitting memory limits
- Default 10,000 rows per batch is usually good

### Disk Space
Monitor temp directory:
```bash
df -h /tmp
```

Clean up if needed:
```bash
rm -f /tmp/pg_to_ch_temp/*
```

## Expected Timeline

Approximate migration times (depends on data size and system):
- 1 million rows: ~5-15 minutes
- 10 million rows: ~30-90 minutes  
- 100 million rows: ~5-15 hours

Rate typically: 1,000-10,000 rows/second

## Log Files

- Migration log: `/tmp/pg_to_ch_migration.log`
- Test log: `/tmp/pg_to_ch_test.log`
- Progress: `/tmp/pg_to_ch_progress/*.offset`
- Completion markers: `/tmp/pg_to_ch_progress/*.complete`

## Safety Features

1. **No data loss**: Only reads from PostgreSQL, never deletes
2. **Incremental**: Processes in small batches
3. **Resumable**: Saves progress after each batch
4. **Verified**: Compares row counts before marking complete
5. **Logged**: All actions logged with timestamps
