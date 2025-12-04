# Database Maintenance Tools

Streamlined utilities for managing ClickHouse tables between WD1 and WD2.

## Quick Start

**To sync all tables between WD1 and WD2:**
```bash
cd ~/wsprdaemon-server/database-tools
bash sync-all.sh
```

## Core Synchronization Scripts

### sync-all.sh ⭐ **[RECOMMENDED]**
Master wrapper that syncs all tables between WD1 and WD2.

**Usage:**
```bash
bash sync-all.sh                # Sync all tables (default)
bash sync-all.sh --wsprnet-only    # Only sync wsprnet.spots
bash sync-all.sh --wsprdaemon-only # Only sync wsprdaemon tables
```

**What it does:**
1. Syncs `wsprnet.spots` table using `sync_wsprnet_spots.sh`
2. Syncs `wsprdaemon.spots_extended` and `wsprdaemon.noise` using `sync_wsprdaemon_tables.sh`
3. Provides summary of all operations

---

### sync_wsprnet_spots.sh
Bidirectional sync for `wsprnet.spots` table using unique `id` column.

**Features:**
- Works with **live data** - does NOT stop wsprnet_scraper services
- Detects missing rows on each server
- Bidirectional sync (both WD1 ↔ WD2)
- Optional deduplication
- Efficient gap detection using `id` column

**Usage:**
```bash
bash sync_wsprnet_spots.sh              # Full sync with deduplication
bash sync_wsprnet_spots.sh --sync-only  # Skip deduplication
bash sync_wsprnet_spots.sh --dedupe-only # Only deduplicate
```

---

### sync_wsprdaemon_tables.sh
Bidirectional sync for `wsprdaemon.spots_extended` and `wsprdaemon.noise` tables.

**Features:**
- **Stops services** during sync for data consistency
- Uses ClickHouse EXCEPT operator for robust row comparison
- Works correctly with different table schemas
- Bidirectional sync (both WD1 ↔ WD2)
- Automatic deduplication

**Usage:**
```bash
bash sync_wsprdaemon_tables.sh       # Sync both tables (default)
bash sync_wsprdaemon_tables.sh both  # Sync both tables
bash sync_wsprdaemon_tables.sh spots # Sync only spots_extended
bash sync_wsprdaemon_tables.sh noise # Sync only noise
```

**⚠️ Note:** This script stops `wsprdaemon_server` services on both servers during sync.

---

## Utility Scripts

### show_rows.sh
Displays row counts for all WSPR tables on both WD1 and WD2.

**Usage:**
```bash
bash show_rows.sh
```

**Output:**
```
==============================================
>>> Row counts for wsprdaemon and wsprnet tables
==============================================
--- WD1 ---
wsprdaemon.spots_extended: 123.45 million
wsprdaemon.noise:          45.67 million
wsprnet.spots:             234.56 million
--- WD2 ---
wsprdaemon.spots_extended: 123.45 million
wsprdaemon.noise:          45.67 million
wsprnet.spots:             234.56 million
==============================================
```

---

### dedup_wsprtables.sh
Standalone deduplication for `wsprdaemon.spots_extended` and `wsprdaemon.noise`.

**Usage:**
```bash
bash dedup_wsprtables.sh
```

**Note:** The main sync scripts already include deduplication, so this is rarely needed.

---

## Maintenance Commands

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

### Manual ClickHouse queries
```bash
# Local server
clickhouse-client --user chadmin

# Remote server
ssh WD1 clickhouse-client --user chadmin
ssh WD2 clickhouse-client --user chadmin
```

---

## Common Workflows

### Daily Sync
```bash
# Run once per day to keep servers synchronized
bash sync-all.sh
```

### After System Restart
```bash
# Check row counts
bash show_rows.sh

# Sync if needed
bash sync-all.sh
```

### Troubleshooting Sync Issues
```bash
# 1. Check row counts
bash show_rows.sh

# 2. Try syncing individual tables
bash sync_wsprnet_spots.sh --sync-only
bash sync_wsprdaemon_tables.sh spots

# 3. If still problematic, try with deduplication
bash sync_wsprnet_spots.sh
bash sync_wsprdaemon_tables.sh both
```

---

## System Requirements

- **User:** Run as `wsprdaemon` user
- **SSH:** Password-less SSH keys between WD1 and WD2
- **ClickHouse:** User `chadmin` with appropriate permissions
- **Network:** Servers must be able to connect to each other on port 9000

### Test connectivity
```bash
# From WD1
ssh WD2 "clickhouse-client --user chadmin --query 'SELECT 1'"

# From WD2  
ssh WD1 "clickhouse-client --user chadmin --query 'SELECT 1'"
```

---

## File Reference

### Core Scripts (keep these)
- `sync-all.sh` - Master wrapper for all sync operations ⭐
- `sync_wsprnet_spots.sh` - wsprnet.spots synchronization
- `sync_wsprdaemon_tables.sh` - wsprdaemon tables synchronization
- `show_rows.sh` - Display row counts utility
- `README.md` - This file

### Optional Utilities
- `dedup_wsprtables.sh` - Standalone deduplication
- `add_config_columns.sql` - SQL for adding config columns
- `grants.txt` - User permissions reference

### Cleanup
- `cleanup-deprecated.sh` - Remove old/deprecated scripts

### Dangerous ⚠️
- `clickhouse-complete-uninstall.sh` - **COMPLETELY REMOVES ClickHouse and ALL DATA**
  - Only use for fresh installs
  - Requires multiple confirmations
  - **THIS DELETES EVERYTHING**

---

## Troubleshooting

### "Connection refused" errors
Check ClickHouse is running:
```bash
sudo systemctl status clickhouse-server
```

### "Authentication failed" errors  
Verify chadmin user exists and has permissions:
```bash
clickhouse-client --user chadmin --query "SELECT currentUser()"
```

### Tables don't match after sync
1. Check if services are writing during sync
2. Run deduplication
3. Run sync again

### Services won't start after sync
Check logs:
```bash
sudo journalctl -u wsprdaemon_server@wsprdaemon -n 100
sudo tail -100 /var/log/clickhouse-server/clickhouse-server.log
```

---

## Version History

- **v3.1** - Current streamlined version
  - Added `sync-all.sh` master wrapper
  - Improved EXCEPT-based sync for wsprdaemon tables
  - Better error handling and logging
  - Removed deprecated one-directional sync scripts

---

## Support

For issues or questions:
1. Check row counts: `bash show_rows.sh`
2. Review logs: `sudo journalctl -u wsprdaemon_server@wsprdaemon`
3. Test connectivity between servers
4. Check ClickHouse status on both servers
