#!/bin/bash
#
# sync_wsprnet_spots.sh - Sync wsprnet.spots tables between WD1 and WD2
# Version: 1.0 - Works with live data using unique id column for gap detection
# Run as user wsprdaemon on either WD1 or WD2

set -e

SCRIPT_VERSION="1.0"

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

log_info() {
    echo -e "${GREEN}[INFO]${NC} $1"
}

log_warn() {
    echo -e "${YELLOW}[WARN]${NC} $1"
}

log_error() {
    echo -e "${RED}[ERROR]${NC} $1"
}

# Usage function
usage() {
    cat << EOF
Usage: $0 [OPTIONS]

Sync wsprnet.spots table between WD1 and WD2 using unique id column.
This script works with live data - does NOT stop wsprnet_scraper services.

OPTIONS:
    --dedupe-only   Only deduplicate, do not sync
    --sync-only     Only sync, do not deduplicate
    -v, --version   Show version
    -h, --help      Show this help message

EXAMPLES:
    $0                  # Full sync: dedupe + bidirectional sync
    $0 --dedupe-only    # Only remove duplicates
    $0 --sync-only      # Only sync missing rows
    $0 --version        # Show version

NOTES:
    - Uses the unique 'id' column for efficient gap detection
    - Does NOT stop wsprnet_scraper - works with live data
    - Syncs in chunks to minimize impact on running services

EOF
    exit 0
}

# Check for help flag
if [[ "$1" == "--help" || "$1" == "-h" ]]; then
    usage
fi

# Check for version flag
if [[ "$1" == "--version" || "$1" == "-v" ]]; then
    echo "sync_wsprnet_spots.sh version ${SCRIPT_VERSION}"
    exit 0
fi

# Parse operation mode
DO_DEDUPE=true
DO_SYNC=true

if [[ "$1" == "--dedupe-only" ]]; then
    DO_SYNC=false
elif [[ "$1" == "--sync-only" ]]; then
    DO_DEDUPE=false
fi

# Determine which server we're on
CURRENT_HOST=$(hostname -s)
if [[ "$CURRENT_HOST" == "WD1" ]]; then
    LOCAL_HOST="WD1"
    REMOTE_HOST="WD2"
elif [[ "$CURRENT_HOST" == "WD2" ]]; then
    LOCAL_HOST="WD2"
    REMOTE_HOST="WD1"
else
    log_error "Unknown host: $CURRENT_HOST. Must be run on WD1 or WD2"
    exit 1
fi

log_info "Running on $LOCAL_HOST, remote host is $REMOTE_HOST"

# ClickHouse connection settings
CH_USER="chadmin"
CH_PASSWORD=""
CH_DB="wsprnet"
CH_TABLE="spots"

# Function to run ClickHouse query locally
run_local_query() {
    clickhouse-client --user "$CH_USER" --password "$CH_PASSWORD" --query "$1"
}

# Function to run ClickHouse query on remote host
run_remote_query() {
    local query="$1"
    ssh "$REMOTE_HOST" "clickhouse-client --user '$CH_USER' --password '$CH_PASSWORD'" <<< "$query"
}

# Function to get row count
get_row_count() {
    local host=$1
    if [[ "$host" == "$LOCAL_HOST" ]]; then
        run_local_query "SELECT count() FROM ${CH_DB}.${CH_TABLE}"
    else
        run_remote_query "SELECT count() FROM ${CH_DB}.${CH_TABLE}"
    fi
}

# Function to get id range
get_id_range() {
    local host=$1
    if [[ "$host" == "$LOCAL_HOST" ]]; then
        run_local_query "SELECT min(id) as min_id, max(id) as max_id FROM ${CH_DB}.${CH_TABLE} FORMAT TSV"
    else
        run_remote_query "SELECT min(id) as min_id, max(id) as max_id FROM ${CH_DB}.${CH_TABLE} FORMAT TSV"
    fi
}

# Function to deduplicate table by id (keeps first occurrence)
deduplicate_table() {
    local host=$1
    log_info "Deduplicating ${CH_DB}.${CH_TABLE} on $host..."
    
    local before_count
    if [[ "$host" == "$LOCAL_HOST" ]]; then
        before_count=$(run_local_query "SELECT count() FROM ${CH_DB}.${CH_TABLE}")
    else
        before_count=$(run_remote_query "SELECT count() FROM ${CH_DB}.${CH_TABLE}")
    fi
    
    log_info "Row count before deduplication on $host: $before_count"
    
    # Get the original table schema
    local create_stmt
    if [[ "$host" == "$LOCAL_HOST" ]]; then
        create_stmt=$(run_local_query "SHOW CREATE TABLE ${CH_DB}.${CH_TABLE} FORMAT TSVRaw")
    else
        create_stmt=$(run_remote_query "SHOW CREATE TABLE ${CH_DB}.${CH_TABLE} FORMAT TSVRaw")
    fi
    
    # Create temp table with same schema
    local temp_create=$(echo "$create_stmt" | sed "s/${CH_TABLE}/${CH_TABLE}_temp/g")
    
    if [[ "$host" == "$LOCAL_HOST" ]]; then
        run_local_query "DROP TABLE IF EXISTS ${CH_DB}.${CH_TABLE}_temp" 2>/dev/null || true
        run_local_query "$temp_create"
        
        log_info "Inserting unique rows by id into temp table on $host..."
        # Use argMin to keep the first occurrence of each id
        run_local_query "
            INSERT INTO ${CH_DB}.${CH_TABLE}_temp 
            SELECT * FROM ${CH_DB}.${CH_TABLE}
            WHERE id IN (
                SELECT id FROM (
                    SELECT id, min(rowNumberInAllBlocks()) as rn
                    FROM ${CH_DB}.${CH_TABLE}
                    GROUP BY id
                )
            )
        "
        
        log_info "Swapping tables on $host..."
        run_local_query "RENAME TABLE ${CH_DB}.${CH_TABLE} TO ${CH_DB}.${CH_TABLE}_old, ${CH_DB}.${CH_TABLE}_temp TO ${CH_DB}.${CH_TABLE}"
        run_local_query "DROP TABLE ${CH_DB}.${CH_TABLE}_old"
        
        local after_count=$(run_local_query "SELECT count() FROM ${CH_DB}.${CH_TABLE}")
    else
        run_remote_query "DROP TABLE IF EXISTS ${CH_DB}.${CH_TABLE}_temp" 2>/dev/null || true
        run_remote_query "$temp_create"
        
        log_info "Inserting unique rows by id into temp table on $host..."
        run_remote_query "
            INSERT INTO ${CH_DB}.${CH_TABLE}_temp 
            SELECT * FROM ${CH_DB}.${CH_TABLE}
            WHERE id IN (
                SELECT id FROM (
                    SELECT id, min(rowNumberInAllBlocks()) as rn
                    FROM ${CH_DB}.${CH_TABLE}
                    GROUP BY id
                )
            )
        "
        
        log_info "Swapping tables on $host..."
        run_remote_query "RENAME TABLE ${CH_DB}.${CH_TABLE} TO ${CH_DB}.${CH_TABLE}_old, ${CH_DB}.${CH_TABLE}_temp TO ${CH_DB}.${CH_TABLE}"
        run_remote_query "DROP TABLE ${CH_DB}.${CH_TABLE}_old"
        
        local after_count=$(run_remote_query "SELECT count() FROM ${CH_DB}.${CH_TABLE}")
    fi
    
    log_info "Row count after deduplication on $host: $after_count"
    log_info "Removed $((before_count - after_count)) duplicate rows on $host"
}

# Function to find missing id ranges
find_missing_ids() {
    local src_host=$1
    local dst_host=$2
    
    log_info "Finding missing IDs from $src_host that are not in $dst_host..."
    
    if [[ "$src_host" == "$LOCAL_HOST" ]]; then
        # Check what IDs from local are missing on remote
        local missing_count=$(run_remote_query "
            SELECT count() 
            FROM remote('${LOCAL_HOST}', '${CH_DB}.${CH_TABLE}', '${CH_USER}', '${CH_PASSWORD}')
            WHERE id NOT IN (SELECT id FROM ${CH_DB}.${CH_TABLE})
        ")
    else
        # Check what IDs from remote are missing on local
        local missing_count=$(run_local_query "
            SELECT count() 
            FROM remote('${REMOTE_HOST}', '${CH_DB}.${CH_TABLE}', '${CH_USER}', '${CH_PASSWORD}')
            WHERE id NOT IN (SELECT id FROM ${CH_DB}.${CH_TABLE})
        ")
    fi
    
    echo "$missing_count"
}

# Function to sync missing rows by id
sync_missing_rows() {
    local src_host=$1
    local dst_host=$2
    
    log_info "Syncing missing rows from $src_host to $dst_host..."
    
    # Find how many rows are missing
    local missing_count=$(find_missing_ids "$src_host" "$dst_host")
    log_info "Found $missing_count missing rows to transfer"
    
    if [[ "$missing_count" == "0" ]]; then
        log_info "No rows to sync"
        return
    fi
    
    if [[ "$src_host" == "$LOCAL_HOST" ]]; then
        # Transfer from local to remote
        log_info "Transferring $missing_count rows from $src_host to $dst_host..."
        
        ssh "$REMOTE_HOST" "clickhouse-client --user '$CH_USER' --password '$CH_PASSWORD'" <<EOF
INSERT INTO ${CH_DB}.${CH_TABLE}
SELECT * FROM remote('${LOCAL_HOST}', '${CH_DB}.${CH_TABLE}', '${CH_USER}', '${CH_PASSWORD}')
WHERE id NOT IN (SELECT id FROM ${CH_DB}.${CH_TABLE})
EOF
        
    else
        # Transfer from remote to local
        log_info "Transferring $missing_count rows from $src_host to $dst_host..."
        
        run_local_query "
INSERT INTO ${CH_DB}.${CH_TABLE}
SELECT * FROM remote('${REMOTE_HOST}', '${CH_DB}.${CH_TABLE}', '${CH_USER}', '${CH_PASSWORD}')
WHERE id NOT IN (SELECT id FROM ${CH_DB}.${CH_TABLE})
"
    fi
    
    log_info "Transfer completed"
}

# Main execution
main() {
    log_info "=== WSPRNET Spots Sync v${SCRIPT_VERSION} Started ==="
    log_info "Local: $LOCAL_HOST, Remote: $REMOTE_HOST"
    log_info "Table: ${CH_DB}.${CH_TABLE}"
    log_info "Mode: $([ "$DO_DEDUPE" == "true" ] && echo -n "Dedupe " || true)$([ "$DO_SYNC" == "true" ] && echo -n "Sync" || true)"
    log_warn "Working with LIVE data - wsprnet_scraper services remain running"
    
    log_info ""
    log_info "=========================================="
    log_info "Initial Status"
    log_info "=========================================="
    
    # Get initial counts
    local_count=$(get_row_count "$LOCAL_HOST")
    remote_count=$(get_row_count "$REMOTE_HOST")
    log_info "  $LOCAL_HOST: $local_count rows"
    log_info "  $REMOTE_HOST: $remote_count rows"
    
    # Get id ranges
    local_range=$(get_id_range "$LOCAL_HOST")
    remote_range=$(get_id_range "$REMOTE_HOST")
    log_info "  $LOCAL_HOST ID range: $local_range"
    log_info "  $REMOTE_HOST ID range: $remote_range"
    
    # Deduplication phase
    if [[ "$DO_DEDUPE" == "true" ]]; then
        log_info ""
        log_info "=========================================="
        log_info "Deduplication Phase"
        log_info "=========================================="
        
        deduplicate_table "$LOCAL_HOST"
        deduplicate_table "$REMOTE_HOST"
        
        # Get counts after deduplication
        log_info "Row counts after deduplication:"
        local_count=$(get_row_count "$LOCAL_HOST")
        remote_count=$(get_row_count "$REMOTE_HOST")
        log_info "  $LOCAL_HOST: $local_count rows"
        log_info "  $REMOTE_HOST: $remote_count rows"
    fi
    
    # Sync phase
    if [[ "$DO_SYNC" == "true" ]]; then
        log_info ""
        log_info "=========================================="
        log_info "Bidirectional Sync Phase"
        log_info "=========================================="
        
        # Sync from local to remote
        sync_missing_rows "$LOCAL_HOST" "$REMOTE_HOST"
        
        # Sync from remote to local
        sync_missing_rows "$REMOTE_HOST" "$LOCAL_HOST"
    fi
    
    # Final status
    log_info ""
    log_info "=========================================="
    log_info "Final Status"
    log_info "=========================================="
    
    local_count=$(get_row_count "$LOCAL_HOST")
    remote_count=$(get_row_count "$REMOTE_HOST")
    log_info "  $LOCAL_HOST: $local_count rows"
    log_info "  $REMOTE_HOST: $remote_count rows"
    
    local_range=$(get_id_range "$LOCAL_HOST")
    remote_range=$(get_id_range "$REMOTE_HOST")
    log_info "  $LOCAL_HOST ID range: $local_range"
    log_info "  $REMOTE_HOST ID range: $remote_range"
    
    # Check if they match
    if [[ "$local_count" == "$remote_count" ]]; then
        log_info "✓ Tables are synchronized!"
    else
        log_warn "⚠ Tables differ by $((local_count - remote_count)) rows"
        log_warn "This is normal if scraper is actively adding data"
    fi
    
    log_info ""
    log_info "=== WSPRNET Spots Sync Completed ==="
}

# Run main function
main
