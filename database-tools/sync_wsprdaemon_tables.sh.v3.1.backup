#!/bin/bash
#
# sync_wsprdaemon_tables.sh - Bidirectional sync and deduplication for wsprdaemon tables
# Version: 3.1 - Uses EXCEPT for robust row comparison across different table schemas
# Run as user wsprdaemon on either WD1 or WD2

set -e

SCRIPT_VERSION="3.1"

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

Efficiently sync and deduplicate wsprdaemon tables between WD1 and WD2 using 
ClickHouse's native remote() function for direct server-to-server transfers.

OPTIONS:
    spots       Sync only wsprdaemon.spots_extended table
    noise       Sync only wsprdaemon.noise table
    both        Sync both tables (default)
    -v, --version   Show version
    -h, --help      Show this help message

EXAMPLES:
    $0              # Sync both tables (default)
    $0 both         # Sync both tables
    $0 spots        # Sync only spots_extended
    $0 noise        # Sync only noise
    $0 --version    # Show version

NOTES:
    This version uses ClickHouse EXCEPT operator for robust comparison
    of all columns, working correctly with different table schemas.

EOF
    exit 0
}

# Check for help flag
if [[ "$1" == "--help" || "$1" == "-h" ]]; then
    usage
fi

# Check for version flag
if [[ "$1" == "--version" || "$1" == "-v" ]]; then
    echo "sync_wsprdaemon_tables.sh version ${SCRIPT_VERSION}"
    exit 0
fi

# Parse table selection argument
SYNC_MODE="${1:-both}"
case "$SYNC_MODE" in
    spots)
        TABLES_TO_SYNC=("spots_extended")
        ;;
    noise)
        TABLES_TO_SYNC=("noise")
        ;;
    both)
        TABLES_TO_SYNC=("spots_extended" "noise")
        ;;
    *)
        log_error "Invalid argument: $SYNC_MODE"
        echo "Valid options: spots, noise, both"
        echo "Run '$0 --help' for usage information"
        exit 1
        ;;
esac

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
CH_DB="wsprdaemon"

# Function to run ClickHouse query locally
run_local_query() {
    clickhouse-client --user "$CH_USER" --password "$CH_PASSWORD" --query "$1"
}

# Function to run ClickHouse query on remote host
run_remote_query() {
    local query="$1"
    ssh "$REMOTE_HOST" "clickhouse-client --user '$CH_USER' --password '$CH_PASSWORD'" <<< "$query"
}

# Function to stop services on both servers
stop_services() {
    log_info "Stopping wsprdaemon_server services on both servers..."
    
    sudo systemctl stop wsprdaemon_server@wsprdaemon.service || true
    ssh "$REMOTE_HOST" "sudo systemctl stop wsprdaemon_server@wsprdaemon.service" || true
    
    sleep 2
    log_info "Services stopped"
}

# Function to start services on both servers
start_services() {
    log_info "Starting wsprdaemon_server services on both servers..."
    
    sudo systemctl start wsprdaemon_server@wsprdaemon.service
    ssh "$REMOTE_HOST" "sudo systemctl start wsprdaemon_server@wsprdaemon.service"
    
    log_info "Services started"
}

# Function to get row count
get_row_count() {
    local host=$1
    local table=$2
    if [[ "$host" == "$LOCAL_HOST" ]]; then
        run_local_query "SELECT count() FROM ${CH_DB}.${table}"
    else
        run_remote_query "SELECT count() FROM ${CH_DB}.${table}"
    fi
}

# Function to deduplicate table
deduplicate_table() {
    local host=$1
    local table=$2
    log_info "Deduplicating ${CH_DB}.${table} on $host..."
    
    local before_count
    if [[ "$host" == "$LOCAL_HOST" ]]; then
        before_count=$(run_local_query "SELECT count() FROM ${CH_DB}.${table}")
    else
        before_count=$(run_remote_query "SELECT count() FROM ${CH_DB}.${table}")
    fi
    
    log_info "Row count before deduplication on $host: $before_count"
    
    # Get the original table schema
    local create_stmt
    if [[ "$host" == "$LOCAL_HOST" ]]; then
        create_stmt=$(run_local_query "SHOW CREATE TABLE ${CH_DB}.${table} FORMAT TSVRaw")
    else
        create_stmt=$(run_remote_query "SHOW CREATE TABLE ${CH_DB}.${table} FORMAT TSVRaw")
    fi
    
    # Create temp table with same schema
    local temp_create=$(echo "$create_stmt" | sed "s/${table}/${table}_temp/g")
    
    if [[ "$host" == "$LOCAL_HOST" ]]; then
        run_local_query "DROP TABLE IF EXISTS ${CH_DB}.${table}_temp" 2>/dev/null || true
        run_local_query "$temp_create"
        
        log_info "Inserting distinct rows into temp table on $host..."
        run_local_query "INSERT INTO ${CH_DB}.${table}_temp SELECT DISTINCT * FROM ${CH_DB}.${table}"
        
        log_info "Swapping tables on $host..."
        run_local_query "RENAME TABLE ${CH_DB}.${table} TO ${CH_DB}.${table}_old, ${CH_DB}.${table}_temp TO ${CH_DB}.${table}"
        run_local_query "DROP TABLE ${CH_DB}.${table}_old"
        
        local after_count=$(run_local_query "SELECT count() FROM ${CH_DB}.${table}")
    else
        run_remote_query "DROP TABLE IF EXISTS ${CH_DB}.${table}_temp" 2>/dev/null || true
        run_remote_query "$temp_create"
        
        log_info "Inserting distinct rows into temp table on $host..."
        run_remote_query "INSERT INTO ${CH_DB}.${table}_temp SELECT DISTINCT * FROM ${CH_DB}.${table}"
        
        log_info "Swapping tables on $host..."
        run_remote_query "RENAME TABLE ${CH_DB}.${table} TO ${CH_DB}.${table}_old, ${CH_DB}.${table}_temp TO ${CH_DB}.${table}"
        run_remote_query "DROP TABLE ${CH_DB}.${table}_old"
        
        local after_count=$(run_remote_query "SELECT count() FROM ${CH_DB}.${table}")
    fi
    
    log_info "Row count after deduplication on $host: $after_count"
    log_info "Removed $((before_count - after_count)) duplicate rows on $host"
}

# Function to sync missing rows using EXCEPT - works for any table schema
sync_missing_rows_efficient() {
    local src_host=$1
    local dst_host=$2
    local table=$3
    
    log_info "Syncing missing rows from $src_host to $dst_host for table ${table}..."
    
    # Count how many rows are missing before sync
    local missing_count
    if [[ "$src_host" == "$LOCAL_HOST" ]]; then
        missing_count=$(run_remote_query "SELECT count() FROM (SELECT * FROM remote('${LOCAL_HOST}', '${CH_DB}.${table}', '${CH_USER}', '${CH_PASSWORD}') EXCEPT SELECT * FROM ${CH_DB}.${table})")
    else
        missing_count=$(run_local_query "SELECT count() FROM (SELECT * FROM remote('${REMOTE_HOST}', '${CH_DB}.${table}', '${CH_USER}', '${CH_PASSWORD}') EXCEPT SELECT * FROM ${CH_DB}.${table})")
    fi
    
    log_info "Found $missing_count missing rows to transfer"
    
    if [[ "$missing_count" == "0" ]]; then
        log_info "No rows to sync"
        return
    fi
    
    if [[ "$src_host" == "$LOCAL_HOST" ]]; then
        # Source is local, destination is remote
        log_info "Transferring rows from $src_host to $dst_host..."
        
        ssh "$REMOTE_HOST" "clickhouse-client --user '$CH_USER' --password '$CH_PASSWORD'" <<EOF
INSERT INTO ${CH_DB}.${table}
SELECT * FROM remote('${LOCAL_HOST}', '${CH_DB}.${table}', '${CH_USER}', '${CH_PASSWORD}')
EXCEPT
SELECT * FROM ${CH_DB}.${table}
EOF
        
    else
        # Source is remote, destination is local
        log_info "Transferring rows from $src_host to $dst_host..."
        
        run_local_query "
INSERT INTO ${CH_DB}.${table}
SELECT * FROM remote('${REMOTE_HOST}', '${CH_DB}.${table}', '${CH_USER}', '${CH_PASSWORD}')
EXCEPT
SELECT * FROM ${CH_DB}.${table}
"
    fi
    
    log_info "Transfer completed"
}

# Main execution
main() {
    log_info "=== WSPRDAEMON Table Sync v${SCRIPT_VERSION} Started ==="
    log_info "Local: $LOCAL_HOST, Remote: $REMOTE_HOST"
    log_info "Tables to sync: ${TABLES_TO_SYNC[*]}"
    log_info "Using ClickHouse remote() with EXCEPT for schema-agnostic sync"
    
    # Stop services once at the beginning
    stop_services
    
    # Process each table
    for CH_TABLE in "${TABLES_TO_SYNC[@]}"; do
        log_info ""
        log_info "=========================================="
        log_info "Processing table: ${CH_DB}.${CH_TABLE}"
        log_info "=========================================="
        
        # Get initial counts
        log_info "Initial row counts:"
        local_count=$(get_row_count "$LOCAL_HOST" "$CH_TABLE")
        remote_count=$(get_row_count "$REMOTE_HOST" "$CH_TABLE")
        log_info "  $LOCAL_HOST: $local_count rows"
        log_info "  $REMOTE_HOST: $remote_count rows"
        
        # Deduplicate both tables first
        deduplicate_table "$LOCAL_HOST" "$CH_TABLE"
        deduplicate_table "$REMOTE_HOST" "$CH_TABLE"
        
        # Get counts after deduplication
        log_info "Row counts after deduplication:"
        local_count=$(get_row_count "$LOCAL_HOST" "$CH_TABLE")
        remote_count=$(get_row_count "$REMOTE_HOST" "$CH_TABLE")
        log_info "  $LOCAL_HOST: $local_count rows"
        log_info "  $REMOTE_HOST: $remote_count rows"
        
        # Bidirectional sync using EXCEPT
        log_info "Performing bidirectional sync..."
        
        # Sync from local to remote
        sync_missing_rows_efficient "$LOCAL_HOST" "$REMOTE_HOST" "$CH_TABLE"
        
        # Sync from remote to local
        sync_missing_rows_efficient "$REMOTE_HOST" "$LOCAL_HOST" "$CH_TABLE"
        
        # Final deduplication to clean up any edge cases
        log_info "Final deduplication pass..."
        deduplicate_table "$LOCAL_HOST" "$CH_TABLE"
        deduplicate_table "$REMOTE_HOST" "$CH_TABLE"
        
        # Get final counts
        log_info "Final row counts for ${CH_TABLE}:"
        local_count=$(get_row_count "$LOCAL_HOST" "$CH_TABLE")
        remote_count=$(get_row_count "$REMOTE_HOST" "$CH_TABLE")
        log_info "  $LOCAL_HOST: $local_count rows"
        log_info "  $REMOTE_HOST: $remote_count rows"
        
        # Check if they match
        if [[ "$local_count" == "$remote_count" ]]; then
            log_info "✓ Tables are synchronized!"
        else
            log_warn "⚠ Tables still differ by $((local_count - remote_count)) rows"
        fi
    done
    
    # Start services once at the end
    start_services
    
    log_info ""
    log_info "=== WSPRDAEMON Table Sync Completed ==="
}

# Trap to ensure services are restarted on error
cleanup() {
    if [[ $? -ne 0 ]]; then
        log_error "Script failed, attempting to restart services..."
        start_services 2>/dev/null || true
    fi
}
trap cleanup EXIT

# Run main function
main
