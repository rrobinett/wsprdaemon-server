# Fix Maidenhead Grid Coordinates Script

This script recalculates and updates the `rx_lat`, `rx_lon`, `tx_lat`, and `tx_lon` columns in your ClickHouse tables using the corrected Maidenhead grid conversion algorithm.

## Overview

The original `maidenhead_to_latlon()` function had bugs that caused:
- 6-character grids: ~56km offset (double centering)
- 4-character grids: ~2-3km offset (wrong convention)

This script will:
1. Read all rows from the specified tables
2. Recalculate coordinates from the grid squares (rx_loc, tx_loc)
3. Update rows where coordinates differ by >0.001 degrees

## Files

- `fix_grid_coordinates.py` - Python script that does the work
- `fix_grid_coordinates.sh` - Bash wrapper that sources credentials

## Prerequisites

1. Python 3 with clickhouse-connect:
   ```bash
   pip3 install clickhouse-connect --break-system-packages
   ```

2. ClickHouse credentials in `/etc/wsprdaemon/clickhouse.conf`

3. The script needs write access to the databases (uses `CLICKHOUSE_ROOT_ADMIN_USER`)

## Usage

### Step 1: Dry Run (Recommended)

First, run in dry-run mode to see what would be changed:

```bash
# See what would be updated in first 1000 rows
./fix_grid_coordinates.sh --dry-run --limit 1000

# Full dry run
./fix_grid_coordinates.sh --dry-run
```

### Step 2: Test on Limited Rows

Test the actual update on a small subset:

```bash
./fix_grid_coordinates.sh --limit 10000 --verbose
```

### Step 3: Full Update

Once you're confident, run the full update:

```bash
# Update both tables
./fix_grid_coordinates.sh

# Or update only one table
./fix_grid_coordinates.sh --skip-wsprnet      # Only wsprdaemon.spots_extended
./fix_grid_coordinates.sh --skip-wsprdaemon   # Only wsprnet.spots
```

## Options

```
--dry-run              Show what would be updated without making changes
--verbose              Enable verbose logging
--batch-size N         Rows per batch (default: 10000)
--limit N              Limit total rows to process (for testing)
--skip-wsprnet         Skip wsprnet.spots table
--skip-wsprdaemon      Skip wsprdaemon.spots_extended table
```

## Performance

- Processes ~10,000-50,000 rows per second depending on hardware
- Uses batched ALTER TABLE UPDATE for efficiency
- Progress updates every 100,000 rows

Example timing for 10 million rows: ~3-5 minutes

## What Gets Updated

The script only updates rows where coordinates differ by >0.001 degrees from the corrected calculation. This means:

- **Most 6-character grids** will be updated (~56km error fixed)
- **All 4-character grids** will be updated (~2-3km error fixed)
- Rows with invalid grids (-999, -999) remain unchanged

## Safety

- Uses ClickHouse's transactional ALTER TABLE UPDATE
- Dry-run mode available for testing
- Can limit rows processed for incremental testing
- Logs all operations

## Example Session

```bash
# 1. Test with dry run
wsprdaemon@WD30:~$ ./fix_grid_coordinates.sh --dry-run --limit 1000
[2026-01-19 14:30:00] INFO: Maidenhead Grid Coordinate Fix Tool v1.0.0
[2026-01-19 14:30:00] INFO: *** DRY RUN MODE - No changes will be made ***
[2026-01-19 14:30:00] INFO: Connected to ClickHouse at localhost:8123
[2026-01-19 14:30:00] INFO: Processing table: wsprnet.spots
[2026-01-19 14:30:00] INFO: Total rows to process: 1,000
[2026-01-19 14:30:00] INFO: Progress: 1,000/1,000 (100.0%) - 812 updated
[2026-01-19 14:30:00] INFO: Completed wsprnet.spots: 1,000 processed, 812 updated

# 2. Looks good, run for real
wsprdaemon@WD30:~$ ./fix_grid_coordinates.sh
[2026-01-19 14:35:00] INFO: Maidenhead Grid Coordinate Fix Tool v1.0.0
[2026-01-19 14:35:00] INFO: Connected to ClickHouse at localhost:8123
[2026-01-19 14:35:00] INFO: Processing table: wsprnet.spots
[2026-01-19 14:35:00] INFO: Total rows to process: 5,234,567
[2026-01-19 14:35:10] INFO: Progress: 100,000/5,234,567 (1.9%) - 81,234 updated
[2026-01-19 14:35:20] INFO: Progress: 200,000/5,234,567 (3.8%) - 162,455 updated
...
[2026-01-19 14:40:00] INFO: Completed wsprnet.spots: 5,234,567 processed, 4,256,789 updated
```

## Troubleshooting

**Error: clickhouse_connect not found**
```bash
pip3 install clickhouse-connect --break-system-packages
```

**Error: Permission denied**
```bash
chmod +x fix_grid_coordinates.sh
```

**Very slow performance**
- Increase `--batch-size` (try 50000)
- Check ClickHouse server load
- Consider running during off-peak hours

**Want to verify results**
After running, you can spot-check some grids:
```sql
SELECT rx_loc, rx_lat, rx_lon, tx_loc, tx_lat, tx_lon 
FROM wsprnet.spots 
LIMIT 10;
```

Compare against your good-grid-calcs.csv to verify.
