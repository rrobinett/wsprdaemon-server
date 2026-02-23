#!/bin/bash
#
# ch-backup-all.sh - Backup all ClickHouse user tables to dated archive directory
# Version: 1.2.0
#
# Modes:
#   --local  [base_dir]   Fast uncompressed native format for local HD
#                         Default: /srv/ch_archive/ch-backups
#   --offsite [base_dir]  pigz-compressed native.gz for offsite HD
#                         Default: /mnt/offsite/ch-backups
#   --status [backup_dir] Show progress of running/completed backup
#
# Usage:
#   ch-backup-all.sh --local                   Nightly local backup
#   ch-backup-all.sh --offsite /mnt/usb1       Offsite backup to USB drive
#   ch-backup-all.sh --status                  Check most recent backup progress
#   ch-backup-all.sh --status /path/to/backup  Check specific backup progress
#

set -euo pipefail

VERSION="1.2.0"
CH_CONF="/etc/wsprdaemon/clickhouse.conf"
STATE_FILE_NAME="backup-state.tsv"

DEFAULT_LOCAL_BASE="/srv/ch_archive/ch-backups"
DEFAULT_OFFSITE_BASE="/mnt/offsite/ch-backups"

STATUS_SEARCH_PATHS=(
    /srv/ch_archive/ch-backups
    /mnt/wd_archive1/ch-archives
    /srv/wd_archive/ch-archives
    /mnt/offsite/ch-backups
)

if [[ ! -f "$CH_CONF" ]]; then
    echo "ERROR: ClickHouse config not found: $CH_CONF" >&2
    exit 1
fi
source "$CH_CONF"
CH_USER="${CLICKHOUSE_ROOT_ADMIN_USER}"
CH_PASS="${CLICKHOUSE_ROOT_ADMIN_PASSWORD}"

ch_query() {
    clickhouse-client --user "$CH_USER" --password "$CH_PASS" \
        --max_execution_time 0 --receive_timeout 604800 \
        --query "$1"
}

human_bytes() {
    local bytes="$1"
    if   (( bytes >= 1073741824 )); then printf "%.1f GB" "$(echo "scale=1; $bytes/1073741824" | bc)"
    elif (( bytes >= 1048576 ));    then printf "%.1f MB" "$(echo "scale=1; $bytes/1048576"    | bc)"
    elif (( bytes >= 1024 ));       then printf "%.1f KB" "$(echo "scale=1; $bytes/1024"       | bc)"
    else printf "%d B" "$bytes"
    fi
}

cmd_status() {
    local archive_dir="${1:-}"

    if [[ -z "$archive_dir" ]]; then
        for base in "${STATUS_SEARCH_PATHS[@]}"; do
            if [[ -d "$base" ]]; then
                local candidate
                candidate=$(ls -1td "${base}"/20* 2>/dev/null | head -1)
                if [[ -n "$candidate" && -f "${candidate}/${STATE_FILE_NAME}" ]]; then
                    archive_dir="$candidate"
                    break
                fi
            fi
        done
    fi

    if [[ -z "$archive_dir" || ! -d "$archive_dir" ]]; then
        echo "ERROR: No backup directory found. Pass path as argument." >&2
        exit 1
    fi

    local state_file="${archive_dir}/${STATE_FILE_NAME}"
    if [[ ! -f "$state_file" ]]; then
        echo "ERROR: No state file found in ${archive_dir}" >&2
        exit 1
    fi

    local mode
    mode=$(grep "^#mode:" "$state_file" | cut -d: -f2 || echo "unknown")

    echo "========================================================"
    echo "  Backup Status: $(basename "$archive_dir")"
    echo "  Directory:     $archive_dir"
    echo "  Mode:          ${mode}"
    echo "  Time now:      $(date -u)"
    echo "========================================================"
    echo ""

    local total=0 done_count=0 running=0 pending=0
    local total_expected_bytes=0 total_written_bytes=0

    printf "  %-42s  %10s  %10s  %6s  %s\n" "TABLE" "EXPECTED" "WRITTEN" "PCT" "STATUS"
    printf "  %-42s  %10s  %10s  %6s  %s\n" "-----" "--------" "-------" "---" "------"

    while IFS=$'\t' read -r db tbl expected_rows expected_bytes; do
        [[ "$db" == \#* ]] && continue
        total=$(( total + 1 ))
        total_expected_bytes=$(( total_expected_bytes + expected_bytes ))

        local outfile_gz="${archive_dir}/${db}.${tbl}.native.gz"
        local outfile_native="${archive_dir}/${db}.${tbl}.native"
        local outfile status written_bytes pct

        if [[ -f "$outfile_gz" ]]; then
            outfile="$outfile_gz"
        elif [[ -f "$outfile_native" ]]; then
            outfile="$outfile_native"
        else
            outfile=""
        fi

        if [[ -n "$outfile" ]]; then
            written_bytes=$(stat -c%s "$outfile" 2>/dev/null || echo 0)
            total_written_bytes=$(( total_written_bytes + written_bytes ))
            if lsof "$outfile" 2>/dev/null | grep -qE "pigz|clickhouse"; then
                status="RUNNING"
                running=$(( running + 1 ))
            else
                status="DONE"
                done_count=$(( done_count + 1 ))
            fi
            if (( expected_bytes > 0 )); then
                pct=$(awk "BEGIN{printf \"%.0f%%\", 100*${written_bytes}/${expected_bytes}}")
            else
                pct="?"
            fi
        else
            written_bytes=0
            status="PENDING"
            pct="0%"
            pending=$(( pending + 1 ))
        fi

        printf "  %-42s  %10s  %10s  %6s  %s\n" \
               "${db}.${tbl}" \
               "$(human_bytes "$expected_bytes")" \
               "$(human_bytes "$written_bytes")" \
               "$pct" "$status"

    done < "$state_file"

    echo ""
    echo "  Summary: ${done_count} done / ${running} running / ${pending} pending / ${total} total"
    echo "  Written: $(human_bytes "$total_written_bytes") of $(human_bytes "$total_expected_bytes")"
    if (( total_expected_bytes > 0 )); then
        local overall_pct
        overall_pct=$(awk "BEGIN{printf \"%.1f%%\", 100*${total_written_bytes}/${total_expected_bytes}}")
        echo "  Overall: ${overall_pct} complete"
    fi
    echo "  Dir size: $(du -sh "$archive_dir" 2>/dev/null | cut -f1)"
    echo "========================================================"
}

run_backup() {
    local backup_dir="$1"
    local compress="$2"   # "yes" or "no"
    local mode="$3"

    mkdir -p "$backup_dir"
    echo "Mode:             $mode ($([ "$compress" = yes ] && echo "pigz compressed" || echo "uncompressed native"))"
    echo "Backup directory: $backup_dir"
    echo "Started:          $(date -u)"
    echo ""

    local tables
    tables=$(ch_query "
        SELECT database, name, total_rows, total_bytes
        FROM system.tables
        WHERE database NOT IN ('system','information_schema','INFORMATION_SCHEMA')
          AND engine NOT LIKE '%View%'
        ORDER BY total_bytes DESC
        FORMAT TSV")

    { echo "#mode:${mode}"; echo "$tables"; } > "${backup_dir}/${STATE_FILE_NAME}"

    local pids=()
    declare -A table_pids

    while IFS=$'\t' read -r db tbl rows bytes; do
        local size
        size=$(human_bytes "${bytes:-0}")
        echo "Starting: ${db}.${tbl}  (${rows} rows, ${size})"

        ch_query "SHOW CREATE TABLE ${db}.${tbl}" \
            > "${backup_dir}/${db}.${tbl}.schema.sql"

        if [[ "$compress" == "yes" ]]; then
            clickhouse-client --user "$CH_USER" --password "$CH_PASS" \
                --max_execution_time 0 --receive_timeout 604800 \
                --query "SELECT * FROM ${db}.${tbl} FORMAT Native" \
                | pigz > "${backup_dir}/${db}.${tbl}.native.gz" &
        else
            clickhouse-client --user "$CH_USER" --password "$CH_PASS" \
                --max_execution_time 0 --receive_timeout 604800 \
                --query "SELECT * FROM ${db}.${tbl} FORMAT Native" \
                > "${backup_dir}/${db}.${tbl}.native" &
        fi

        local pid=$!
        pids+=($pid)
        table_pids[$pid]="${db}.${tbl}"
        echo "  PID $pid"
    done <<< "$tables"

    echo ""
    echo "All ${#pids[@]} backup jobs running."
    echo "Monitor with: $0 --status $backup_dir"
    echo "Waiting for completion..."
    echo ""

    local failed=0
    for pid in "${pids[@]}"; do
        local tbl="${table_pids[$pid]}"
        if wait "$pid"; then
            local f sz
            f=$(ls "${backup_dir}/${tbl}.native.gz" "${backup_dir}/${tbl}.native" 2>/dev/null | head -1)
            sz=$(ls -lh "$f" 2>/dev/null | awk '{print $5}')
            echo "OK:   ${tbl}  (${sz})"
        else
            echo "FAIL: ${tbl}"
            failed=$(( failed + 1 ))
        fi
    done

    echo ""
    echo "Completed: $(date -u)"
    echo "Backup dir: $backup_dir"
    echo "Total size: $(du -sh "$backup_dir" | cut -f1)"
    if (( failed > 0 )); then
        echo "FAILURES: $failed tables failed"
        exit 1
    fi
    echo "All tables backed up successfully"
}

usage() {
    echo "ch-backup-all.sh v${VERSION}"
    echo ""
    echo "Usage:"
    echo "  $0 --local   [base_dir]    Uncompressed backup to local HD"
    echo "                              Default: ${DEFAULT_LOCAL_BASE}"
    echo "  $0 --offsite [base_dir]    pigz-compressed backup for offsite HD"
    echo "                              Default: ${DEFAULT_OFFSITE_BASE}"
    echo "  $0 --status  [backup_dir]  Show progress (auto-finds most recent)"
    echo "  $0 --version"
}

CMD="${1:-}"
shift || true

case "$CMD" in
    --local)
        BASE="${1:-$DEFAULT_LOCAL_BASE}"
        run_backup "${BASE}/$(date -u '+%Y-%m-%d_%H%M%S')" "no" "local"
        ;;
    --offsite)
        BASE="${1:-$DEFAULT_OFFSITE_BASE}"
        run_backup "${BASE}/$(date -u '+%Y-%m-%d_%H%M%S')" "yes" "offsite"
        ;;
    --status)
        cmd_status "${1:-}"
        ;;
    --version)
        echo "ch-backup-all.sh v${VERSION}"
        ;;
    --help|-h|"")
        usage
        ;;
    *)
        echo "ERROR: Unknown command: $CMD"
        echo ""
        usage
        exit 1
        ;;
esac
