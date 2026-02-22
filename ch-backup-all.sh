#!/bin/bash
# ch-backup-all.sh - Backup all ClickHouse tables to a date-named directory
# Default format: Native+gzip (smaller, better for network transfer)
# Native BACKUP format: --native (faster restore, larger files)
# Default destination: /srv/wd_archive/ch-archives/
# Override destination: --dest /mnt/ch_archive
# Large tables (> LARGE_TABLE_GB) are backed up one at a time sequentially.
# Small tables are backed up in parallel (up to MAX_PARALLEL at once).
# Version: 3.2.0

CH_USER="chadmin"
CH_PASS="ch2025wd"
CH_OS_USER="clickhouse"
DEFAULT_DEST="/srv/wd_archive"
BACKUP_FORMAT="gzip"
LARGE_TABLE_GB=20      # tables larger than this run sequentially
MAX_PARALLEL=4         # max concurrent jobs for small tables

# --- Parse arguments ---
DEST_MOUNT=""
while [[ $# -gt 0 ]]; do
    case "$1" in
        --dest)
            DEST_MOUNT="$2"
            shift 2
            ;;
        --native)
            BACKUP_FORMAT="native"
            shift
            ;;
        --parallel)
            MAX_PARALLEL="$2"
            shift 2
            ;;
        --large-threshold)
            LARGE_TABLE_GB="$2"
            shift 2
            ;;
        --help|-h)
            echo "Usage: $0 [--dest <mount_point>] [--native] [--parallel N] [--large-threshold GB]"
            echo ""
            echo "  --dest PATH           Backup destination mount point (default: $DEFAULT_DEST)"
            echo "  --native              Use ClickHouse native BACKUP format instead of Native+gzip"
            echo "  --parallel N          Max concurrent jobs for small tables (default: $MAX_PARALLEL)"
            echo "  --large-threshold GB  Tables larger than this run sequentially (default: ${LARGE_TABLE_GB}GB)"
            echo ""
            echo "Examples:"
            echo "  $0                              # gzip backup to $DEFAULT_DEST"
            echo "  $0 --dest /mnt/ch_archive1      # gzip backup to secondary drive"
            echo "  $0 --native --dest /mnt/ch_archive1"
            exit 0
            ;;
        *)
            echo "ERROR: Unknown argument: $1"
            echo "Usage: $0 [--dest <mount_point>] [--native] [--parallel N]"
            exit 1
            ;;
    esac
done

DEST_MOUNT="${DEST_MOUNT:-$DEFAULT_DEST}"

# --- Validate destination is a real mount point ---
if ! mountpoint -q "$DEST_MOUNT"; then
    echo "ERROR: '$DEST_MOUNT' is not a mounted filesystem."
    echo ""
    echo "Currently mounted drives:"
    df -h | grep -E '^/dev/'
    exit 1
fi

# --- Check available space ---
AVAIL_GB=$(df -BG "$DEST_MOUNT" | awk 'NR==2 {gsub("G",""); print $4}')
echo "Destination:       $DEST_MOUNT"
echo "Available space:   ${AVAIL_GB}GB"
echo "Backup format:     ${BACKUP_FORMAT}"
echo "Large table cutoff: ${LARGE_TABLE_GB}GB (sequential)"
echo "Small table jobs:  up to ${MAX_PARALLEL} parallel"
if [[ "$AVAIL_GB" -lt 100 ]]; then
    echo "WARNING: Less than 100GB available on $DEST_MOUNT"
fi

# --- Set up backup directory ---
BACKUP_BASE="${DEST_MOUNT}/ch-archives"
DATE_DIR=$(date +%Y-%m-%d_%H%M%S)
BACKUP_DIR="${BACKUP_BASE}/${DATE_DIR}"

if [[ "$BACKUP_FORMAT" == "native" ]]; then
    sudo mkdir -p "$BACKUP_DIR" || { echo "ERROR: Cannot create $BACKUP_DIR"; exit 1; }
    sudo chown -R ${CH_OS_USER}:${CH_OS_USER} "$BACKUP_DIR"
else
    mkdir -p "$BACKUP_DIR" || { echo "ERROR: Cannot create $BACKUP_DIR"; exit 1; }
fi

echo "Backup directory:  $BACKUP_DIR"
echo "Started:           $(date)"
echo ""

# --- Get all tables with sizes in bytes for sorting ---
# Columns: database, name, total_rows, human_size, bytes
TABLES=$(clickhouse-client --user "$CH_USER" --password "$CH_PASS" --query "
    SELECT database, name, total_rows, formatReadableSize(total_bytes), total_bytes
    FROM system.tables
    WHERE database NOT IN ('system', 'information_schema', 'INFORMATION_SCHEMA', 'TEMPORARY')
    AND engine NOT LIKE '%View%'
    AND engine NOT LIKE '%Distributed%'
    AND engine NOT IN ('Memory', 'Log', 'TinyLog')
    ORDER BY total_bytes DESC
    FORMAT TSV")

if [[ -z "$TABLES" ]]; then
    echo "ERROR: No tables found or cannot connect to ClickHouse."
    exit 1
fi

# Split into large (sequential) and small (parallel) lists
LARGE_THRESHOLD_BYTES=$(( LARGE_TABLE_GB * 1024 * 1024 * 1024 ))
LARGE_TABLES=()
SMALL_TABLES=()
while IFS=$'\t' read -r db tbl rows size bytes; do
    if [[ "$bytes" -gt "$LARGE_THRESHOLD_BYTES" ]]; then
        LARGE_TABLES+=("${db}|${tbl}|${rows}|${size}")
    else
        SMALL_TABLES+=("${db}|${tbl}|${rows}|${size}")
    fi
done <<< "$TABLES"

TABLE_COUNT=$(echo "$TABLES" | wc -l)
echo "Tables to back up: $TABLE_COUNT  (${#LARGE_TABLES[@]} large sequential, ${#SMALL_TABLES[@]} small parallel)"
echo ""

# --- Helper: back up one table, print result ---
FAILED_TABLES=()

backup_one() {
    local db="$1"
    local tbl="$2"
    local full_table="${db}.${tbl}"

    if [[ "$BACKUP_FORMAT" == "native" ]]; then
        sudo mkdir -p "$BACKUP_DIR/$db"
        sudo chown ${CH_OS_USER}:${CH_OS_USER} "$BACKUP_DIR/$db"
        clickhouse-client --user "$CH_USER" --password "$CH_PASS" \
            --query "BACKUP TABLE ${full_table} TO File('${BACKUP_DIR}/${db}/${tbl}/')" \
            > /dev/null 2>&1
    else
        mkdir -p "$BACKUP_DIR/$db"
        clickhouse-client --user "$CH_USER" --password "$CH_PASS" \
            --query "SELECT * FROM ${full_table} FORMAT Native" | \
            gzip -1 > "${BACKUP_DIR}/${db}/${tbl}.native.gz"
    fi
}

report_result() {
    local status="$1"
    local db="$2"
    local tbl="$3"
    local full_table="${db}.${tbl}"
    local backup_size

    if [[ "$BACKUP_FORMAT" == "native" ]]; then
        backup_size=$(du -sh "$BACKUP_DIR/$db/$tbl" 2>/dev/null | cut -f1)
    else
        backup_size=$(du -sh "$BACKUP_DIR/$db/${tbl}.native.gz" 2>/dev/null | cut -f1)
    fi

    if [[ $status -eq 0 ]]; then
        printf "OK:   %-50s (%s)\n" "$full_table" "$backup_size"
    else
        printf "FAIL: %-50s\n" "$full_table"
        FAILED_TABLES+=("$full_table")
    fi
}

# --- Phase 1: Large tables, one at a time ---
if [[ ${#LARGE_TABLES[@]} -gt 0 ]]; then
    echo "=== Phase 1: Large tables (sequential) ==="
    for entry in "${LARGE_TABLES[@]}"; do
        IFS='|' read -r db tbl rows size <<< "$entry"
        echo "Backing up: ${db}.${tbl}  (${rows} rows, ${size})"
        backup_one "$db" "$tbl"
        report_result $? "$db" "$tbl"
    done
    echo ""
fi

# --- Phase 2: Small tables, in parallel ---
if [[ ${#SMALL_TABLES[@]} -gt 0 ]]; then
    echo "=== Phase 2: Small tables (parallel, max ${MAX_PARALLEL}) ==="
    declare -A PIDS
    declare -A JOB_DB
    declare -A JOB_TBL
    ACTIVE=0
    idx=0

    while [[ $idx -lt ${#SMALL_TABLES[@]} || $ACTIVE -gt 0 ]]; do
        # Launch jobs up to MAX_PARALLEL
        while [[ $idx -lt ${#SMALL_TABLES[@]} && $ACTIVE -lt $MAX_PARALLEL ]]; do
            IFS='|' read -r db tbl rows size <<< "${SMALL_TABLES[$idx]}"
            echo "Starting: ${db}.${tbl}  (${rows} rows, ${size})"
            backup_one "$db" "$tbl" &
            pid=$!
            PIDS[$idx]=$pid
            JOB_DB[$idx]=$db
            JOB_TBL[$idx]=$tbl
            ACTIVE=$((ACTIVE + 1))
            idx=$((idx + 1))
        done

        # Reap one completed job
        for job_idx in "${!PIDS[@]}"; do
            pid=${PIDS[$job_idx]}
            if ! kill -0 "$pid" 2>/dev/null; then
                wait "$pid"
                status=$?
                report_result $status "${JOB_DB[$job_idx]}" "${JOB_TBL[$job_idx]}"
                unset PIDS[$job_idx]
                ACTIVE=$((ACTIVE - 1))
                break
            fi
        done
        sleep 0.5
    done
    echo ""
fi

# --- Summary ---
FAILED=${#FAILED_TABLES[@]}
echo "Completed:  $(date)"
echo "Backup dir: $BACKUP_DIR"
TOTAL=$(du -sh "$BACKUP_DIR" 2>/dev/null | cut -f1)
echo "Total size: $TOTAL"
echo ""

if [[ $FAILED -eq 0 ]]; then
    echo "All ${TABLE_COUNT} tables backed up successfully"
    exit 0
else
    echo "WARNING: $FAILED of ${TABLE_COUNT} table(s) failed:"
    for t in "${FAILED_TABLES[@]}"; do
        echo "  $t"
    done
    exit 1
fi
