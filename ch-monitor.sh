#!/bin/bash
#
# ch-monitor.sh - ClickHouse monitoring tool
# Version: 1.1.0
# Contacts CH on localhost so it can run on any WD server (WD10, WD20, WD30)
#
# Usage: ch-monitor.sh [command] [options]
#
# Commands:
#   dbs                   List user databases, sizes, tables, row counts, deltas, rates
#   diag <db> <table>     Diagnose a table (live count, parts, recent activity)

set -euo pipefail

VERSION="1.2.1"
CH_CONF="/etc/wsprdaemon/clickhouse.conf"
STATE_FILE="/var/lib/wsprdaemon/ch-monitor-state.tsv"

# ============================================================================
# Load credentials
# ============================================================================
if [[ ! -f "$CH_CONF" ]]; then
    echo "ERROR: ClickHouse config not found: $CH_CONF" >&2
    exit 1
fi
source "$CH_CONF"

CH_USER="${CLICKHOUSE_ROOT_ADMIN_USER}"
CH_PASS="${CLICKHOUSE_ROOT_ADMIN_PASSWORD}"
CH_HOST="localhost"
CH_PORT="${CLICKHOUSE_PORT:-8123}"

if [[ -z "$CH_USER" || -z "$CH_PASS" ]]; then
    echo "ERROR: CLICKHOUSE_ROOT_ADMIN_USER or CLICKHOUSE_ROOT_ADMIN_PASSWORD not set in $CH_CONF" >&2
    exit 1
fi

# ============================================================================
# Helper: run a CH query and return tab-separated results
# ============================================================================
ch_query() {
    local sql="$1"
    clickhouse-client \
        --host "$CH_HOST" \
        --port 9000 \
        --user "$CH_USER" \
        --password "$CH_PASS" \
        --format TabSeparated \
        --query "$sql" 2>/dev/null
}

# ============================================================================
# Helper: human-readable byte sizes
# ============================================================================
human_bytes() {
    local bytes="$1"
    if   (( bytes >= 1073741824 )); then printf "%.1f GB" "$(echo "scale=1; $bytes/1073741824" | bc)"
    elif (( bytes >= 1048576 ));    then printf "%.1f MB" "$(echo "scale=1; $bytes/1048576"    | bc)"
    elif (( bytes >= 1024 ));       then printf "%.1f KB" "$(echo "scale=1; $bytes/1024"       | bc)"
    else printf "%d B" "$bytes"
    fi
}

# ============================================================================
# State file helpers - track row counts between runs
# State file format: db TAB table TAB rows TAB epoch_timestamp
# ============================================================================
state_get_rows() {
    local db="$1" tbl="$2"
    if [[ -f "$STATE_FILE" ]]; then
        awk -F'\t' -v db="$db" -v tbl="$tbl" \
            '$1==db && $2==tbl {print $3; found=1; exit} END{if(!found) print -1}' \
            "$STATE_FILE"
    else
        echo -1
    fi
}

state_get_time() {
    local db="$1" tbl="$2"
    if [[ -f "$STATE_FILE" ]]; then
        awk -F'\t' -v db="$db" -v tbl="$tbl" \
            '$1==db && $2==tbl {print $4; found=1; exit} END{if(!found) print 0}' \
            "$STATE_FILE"
    else
        echo 0
    fi
}

state_update() {
    local db="$1" tbl="$2" rows="$3" epoch="$4"
    local tmp
    tmp=$(mktemp)
    if [[ -f "$STATE_FILE" ]]; then
        awk -F'\t' -v db="$db" -v tbl="$tbl" '$1!=db || $2!=tbl' "$STATE_FILE" >> "$tmp" || true
    fi
    printf '%s\t%s\t%s\t%s\n' "$db" "$tbl" "$rows" "$epoch" >> "$tmp"
    mv "$tmp" "$STATE_FILE"
}

# ============================================================================
# Command: dbs
# List user-created databases, their total sizes, and each table's size,
# row count, delta since last run, and insert rate
# ============================================================================
cmd_dbs() {
    local SYSTEM_DBS="'system','information_schema','INFORMATION_SCHEMA'"
    local now_epoch
    now_epoch=$(date +%s)

    local db_rows
    db_rows=$(ch_query "
        SELECT
            database,
            SUM(total_bytes) AS total_bytes,
            COUNT()          AS table_count
        FROM system.tables
        WHERE database NOT IN ($SYSTEM_DBS)
          AND engine NOT LIKE '%View%'
        GROUP BY database
        ORDER BY total_bytes DESC
    ")

    if [[ -z "$db_rows" ]]; then
        echo "No user-created databases found."
        exit 0
    fi

    echo "========================================================"
    echo "  ClickHouse Databases on $CH_HOST  ($(date '+%Y-%m-%d %H:%M:%S'))"
    echo "========================================================"

    while IFS=$'\t' read -r db total_bytes table_count; do
        local db_size
        db_size=$(human_bytes "$total_bytes")
        printf "\n  DATABASE: %-30s  size: %s  (%s tables)\n" \
               "$db" "$db_size" "$table_count"
        printf "  %s\n" "$(printf '%.0s-' {1..78})"

        local table_rows
        table_rows=$(ch_query "
            SELECT name, total_bytes, total_rows
            FROM system.tables
            WHERE database = '$db'
              AND engine NOT LIKE '%View%'
            ORDER BY total_bytes DESC
        ")

        if [[ -z "$table_rows" ]]; then
            printf "    (no tables)\n"
        else
            printf "  %-28s  %10s  %13s  %13s  %10s\n" \
                   "TABLE" "SIZE" "ROWS" "+ROWS" "ROWS/SEC"
            printf "  %-28s  %10s  %13s  %13s  %10s\n" \
                   "-----" "----" "----" "-----" "--------"

            while IFS=$'\t' read -r tname tbytes trows; do
                trows="${trows:-0}"
                local t_size delta_str rate_str
                t_size=$(human_bytes "${tbytes:-0}")

                local prev_rows prev_time
                prev_rows=$(state_get_rows "$db" "$tname")
                prev_time=$(state_get_time "$db" "$tname")

                if [[ "$prev_rows" -ge 0 && "$prev_time" -gt 0 ]]; then
                    local delta elapsed
                    delta=$(( trows - prev_rows ))
                    elapsed=$(( now_epoch - prev_time ))
                    if [[ $delta -lt 0 ]]; then
                        delta_str="(reset)"
                        rate_str="-"
                    elif [[ $elapsed -gt 0 ]]; then
                        rate=$(awk "BEGIN{printf \"%.1f\", $delta/$elapsed}")
                        delta_str="+${delta}"
                        rate_str="${rate}/s"
                    else
                        delta_str="+${delta}"
                        rate_str="-"
                    fi
                else
                    delta_str="(first run)"
                    rate_str="-"
                fi

                printf "  %-28s  %10s  %13s  %13s  %10s\n" \
                       "$tname" "$t_size" "$trows" "$delta_str" "$rate_str"

                state_update "$db" "$tname" "$trows" "$now_epoch"
            done <<< "$table_rows"
        fi
    done <<< "$db_rows"

    echo ""
    echo "  Incoming tbz queue:"
    printf "  %-40s  %s\n" "DIRECTORY" "QUEUED"
    printf "  %-40s  %s\n" "---------" "------"
    for spooldir in /var/spool/wsprdaemon/from-gw1 /var/spool/wsprdaemon/from-gw2; do
        if [[ -d "$spooldir" ]]; then
            count=$(find "$spooldir" -maxdepth 1 -name "*.tbz" 2>/dev/null | wc -l)
            printf "  %-40s  %s\n" "$spooldir" "$count"
        else
            printf "  %-40s  %s\n" "$spooldir" "(not found)"
        fi
    done

    echo ""
    echo "========================================================"
}

# ============================================================================
# Command: diag <database> <table>
# ============================================================================
cmd_diag() {
    local db="${1:-wsprdaemon}"
    local tbl="${2:-spots_extended}"

    echo "=== Diagnosing ${db}.${tbl} ==="
    echo ""

    echo "--- system.tables metadata ---"
    ch_query "
        SELECT name, engine, total_rows, total_bytes, metadata_modification_time
        FROM system.tables
        WHERE database='$db' AND name='$tbl'
    " | column -t
    echo ""

    echo "--- SELECT COUNT() (live count, bypasses metadata cache) ---"
    ch_query "SELECT COUNT() FROM ${db}.${tbl}"
    echo ""

    echo "--- Most recent data parts (shows last insert activity) ---"
    ch_query "
        SELECT partition, name, rows, bytes_on_disk, modification_time
        FROM system.parts
        WHERE database='$db' AND table='$tbl' AND active=1
        ORDER BY modification_time DESC
        LIMIT 10
    " | column -t
    echo ""

    echo "--- All databases/tables containing '$tbl' in name ---"
    ch_query "
        SELECT database, name, engine, total_rows, total_bytes
        FROM system.tables
        WHERE name LIKE '%${tbl}%'
        ORDER BY database, name
    " | column -t
    echo ""

    echo "--- Recent merges/mutations on this table ---"
    ch_query "
        SELECT database, table, elapsed, progress, num_parts, result_part_name
        FROM system.merges
        WHERE database='$db' AND table='$tbl'
    " | column -t
    echo "(empty = no active merges)"
}

# ============================================================================
# Usage
# ============================================================================
usage() {
    echo "ch-monitor.sh v$VERSION - ClickHouse monitoring tool"
    echo ""
    echo "Usage: $0 [command] [args]"
    echo ""
    echo "Commands:"
    echo "  dbs                   List user databases, sizes, rows, deltas, rates (default)"
    echo "  diag <db> <table>     Diagnose a table (live count, parts, recent activity)"
    echo ""
    echo "Options:"
    echo "  -v, --version         Show version"
    echo "  -h, --help            Show this help"
    echo ""
    echo "State is stored in: $STATE_FILE"
    exit 1
}

# ============================================================================
# Main
# ============================================================================
COMMAND="${1:-dbs}"
case "$COMMAND" in
    dbs)          cmd_dbs ;;
    diag)         cmd_diag "${2:-wsprdaemon}" "${3:-spots_extended}" ;;
    -v|--version) echo "ch-monitor.sh v$VERSION" ;;
    -h|--help)    usage ;;
    *)            echo "Unknown command: $COMMAND"; echo ""; usage ;;
esac
