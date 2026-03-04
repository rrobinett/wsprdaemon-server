#!/bin/bash
#
# ch-backup-all.sh - Backup all ClickHouse user tables using native BACKUP command
# Version: 3.0.0
#
# Usage:
#   ch-backup-all.sh --local   [base_dir]   Native backup to local HD (default: /srv/wd_archive/ch-backups)
#   ch-backup-all.sh --offsite [base_dir]   Native backup to offsite HD (default: /mnt/offsite/ch-backups)
#   ch-backup-all.sh --status  [backup_dir] Show progress of most recent or specified backup
#   ch-backup-all.sh --restore [backup_dir] Restore all tables from a backup directory
#

set -euo pipefail

VERSION="3.2.0"
CH_CONF="/etc/wsprdaemon/clickhouse.conf"
STATE_FILE_NAME="backup-state.tsv"

DEFAULT_LOCAL_BASE="/srv/wd_archive/ch-backups"
DEFAULT_OFFSITE_BASE="/mnt/offsite/ch-backups"

STATUS_SEARCH_PATHS=(
    /mnt/ch_archive1/ch-backups
    /srv/wd_archive/ch-backups
    /srv/ch_archive/ch-backups
    /mnt/wd_archive1/ch-archives
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

find_backup_dir() {
    local arg="${1:-}"
    if [[ -n "$arg" ]]; then
        echo "$arg"
        return
    fi
    for base in "${STATUS_SEARCH_PATHS[@]}"; do
        if [[ -d "$base" ]]; then
            local candidate
            candidate=$(ls -1td "${base}"/20* 2>/dev/null | head -1)
            if [[ -n "$candidate" && -f "${candidate}/${STATE_FILE_NAME}" ]]; then
                echo "$candidate"
                return
            fi
        fi
    done
    echo ""
}

cmd_status() {
    local archive_dir
    archive_dir=$(find_backup_dir "${1:-}")
    if [[ -z "$archive_dir" || ! -d "$archive_dir" ]]; then
        echo "ERROR: No backup directory found. Pass path as argument." >&2; exit 1
    fi

    local state_file="${archive_dir}/${STATE_FILE_NAME}"
    if [[ ! -f "$state_file" ]]; then
        echo "ERROR: No state file found in ${archive_dir}" >&2; exit 1
    fi

    local mode
    mode=$(grep "^#mode:" "$state_file" | cut -d: -f2 || echo "unknown")

    local backup_name start_epoch now_epoch elapsed_sec
    backup_name=$(basename "$archive_dir")
    start_epoch=$(date -u -d "${backup_name:0:10} ${backup_name:11:2}:${backup_name:13:2}:${backup_name:15:2}" '+%s' 2>/dev/null || echo 0)
    now_epoch=$(date -u '+%s')
    elapsed_sec=$(( now_epoch - start_epoch ))

    echo "========================================================"
    echo "  ch-backup-all.sh v${VERSION}"
    echo "  Backup Status: ${backup_name}"
    echo "  Directory:     $archive_dir"
    echo "  Mode:          ${mode}"
    echo "  Started:       ${backup_name:0:10} ${backup_name:11:2}:${backup_name:13:2}:${backup_name:15:2} UTC"
    echo "  Time now:      $(date -u)"
    echo "  Elapsed:       $(( elapsed_sec / 3600 ))h $(( (elapsed_sec % 3600) / 60 ))m $(( elapsed_sec % 60 ))s"
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

        local outdir="${archive_dir}/${db}.${tbl}"
        local status written_bytes pct

        if [[ -d "$outdir" ]]; then
            written_bytes=$(sudo du -sb "$outdir" 2>/dev/null | awk '{print $1}' || echo 0)
            total_written_bytes=$(( total_written_bytes + written_bytes ))
            # Check system.backups for running status
            local running_check
            running_check=$(ch_query "
                SELECT count() FROM system.backups
                WHERE status = 'CREATING_BACKUP'
                AND name LIKE '%${db}.${tbl}%'" 2>/dev/null || echo 0)
            if [[ "$running_check" -gt 0 ]]; then
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

    if (( total_expected_bytes > 0 && total_written_bytes > 0 )); then
        local overall_pct
        overall_pct=$(awk "BEGIN{printf \"%.1f%%\", 100*${total_written_bytes}/${total_expected_bytes}}")
        echo "  Overall: ${overall_pct} complete"

        if (( running > 0 && elapsed_sec > 0 )); then
            local remaining_bytes eta_sec eta_epoch eta_str eta_min
            remaining_bytes=$(( total_expected_bytes - total_written_bytes ))
            if (( remaining_bytes > 0 )); then
                eta_sec=$(awk "BEGIN{printf \"%d\", ${remaining_bytes} * ${elapsed_sec} / ${total_written_bytes}}")
                eta_epoch=$(( now_epoch + eta_sec ))
                eta_str=$(date -u -d "@${eta_epoch}" '+%Y-%m-%d %H:%M:%S UTC')
                eta_min=$(( eta_sec / 60 ))
                echo "  ETA:     ${eta_str} (in ${eta_min}m)"
            else
                echo "  ETA:     complete"
            fi
        fi
    fi

    echo "  Dir size: $(sudo du -sh "$archive_dir" 2>/dev/null | cut -f1)"
    echo "========================================================"
}

run_backup() {
    local backup_dir="$1"
    local mode="$2"

    mkdir -p "$backup_dir"
    # Ensure clickhouse user can write to backup dir
    chown clickhouse:clickhouse "$backup_dir"

    echo "ch-backup-all.sh v${VERSION}"
    echo "Mode:             $mode (ClickHouse native backup)"
    echo "Backup directory: $backup_dir"
    echo "Started:          $(date -u)"
    echo ""

    # Get all user tables ordered by compressed size descending
    local tables
    tables=$(ch_query "
        SELECT t.database, t.name, t.total_rows,
               coalesce(p.compressed_bytes, t.total_bytes) AS estimated_bytes
        FROM system.tables t
        LEFT JOIN (
            SELECT database, table, sum(data_compressed_bytes) AS compressed_bytes
            FROM system.parts WHERE active = 1
            GROUP BY database, table
        ) p ON p.database = t.database AND p.table = t.name
        WHERE t.database NOT IN ('system','information_schema','INFORMATION_SCHEMA')
          AND t.engine NOT LIKE '%View%'
        ORDER BY estimated_bytes DESC
        FORMAT TSV")

    { echo "#mode:${mode}"; echo "$tables"; } > "${backup_dir}/${STATE_FILE_NAME}"

    local failed=0

    while IFS=$'\t' read -r db tbl rows bytes; do
        local size
        size=$(human_bytes "${bytes:-0}")
        echo "Starting: ${db}.${tbl}  (${rows} rows, ${size})"

        # Save schema
        ch_query "SHOW CREATE TABLE ${db}.${tbl}" \
            > "${backup_dir}/${db}.${tbl}.schema.sql"

        # Run native backup sequentially
        if ch_query "BACKUP TABLE ${db}.${tbl} TO File('${backup_dir}/${db}.${tbl}')"; then
            local sz
            sz=$(sudo du -sh "${backup_dir}/${db}.${tbl}" 2>/dev/null | cut -f1)
            echo "OK:   ${db}.${tbl}  (${sz})"
        else
            echo "FAIL: ${db}.${tbl}"
            failed=$(( failed + 1 ))
        fi
    done <<< "$tables"

    echo ""
    echo "Completed: $(date -u)"
    echo "Backup dir: $backup_dir"
    echo "Total size: $(sudo du -sh "$backup_dir" | cut -f1)"
    if (( failed > 0 )); then
        echo "FAILURES: $failed tables failed"
        exit 1
    fi
    echo "All tables backed up successfully"
}

cmd_restore() {
    local archive_dir="${1:-}"
    if [[ -z "$archive_dir" || ! -d "$archive_dir" ]]; then
        echo "ERROR: Pass backup directory as argument." >&2; exit 1
    fi
    local state_file="${archive_dir}/${STATE_FILE_NAME}"
    if [[ ! -f "$state_file" ]]; then
        echo "ERROR: No state file found in ${archive_dir}" >&2; exit 1
    fi

    echo "Restoring from: $archive_dir"
    while IFS=$'\t' read -r db tbl rows bytes; do
        [[ "$db" == \#* ]] && continue
        local outdir="${archive_dir}/${db}.${tbl}"
        if [[ -d "$outdir" ]]; then
            echo "Restoring ${db}.${tbl} ..."
            ch_query "RESTORE TABLE ${db}.${tbl} FROM File('${outdir}')"
            echo "  OK"
        else
            echo "  SKIP ${db}.${tbl} (no backup dir found)"
        fi
    done < "$state_file"
    echo "Restore complete."
}

usage() {
    echo "ch-backup-all.sh v${VERSION}"
    echo ""
    echo "Usage:"
    echo "  $0 --local   [base_dir]    Native backup to local HD"
    echo "                              Default: ${DEFAULT_LOCAL_BASE}"
    echo "  $0 --offsite [base_dir]    Native backup to offsite HD"
    echo "                              Default: ${DEFAULT_OFFSITE_BASE}"
    echo "  $0 --status  [backup_dir]  Show progress (auto-finds most recent)"
    echo "  $0 --restore [backup_dir]  Restore all tables from backup"
    echo "  $0 --version"
}

CMD="${1:-}"
shift || true

case "$CMD" in
    --local)
        BASE="${1:-$DEFAULT_LOCAL_BASE}"
        run_backup "${BASE}/$(date -u '+%Y-%m-%d_%H%M%S')" "local"
        ;;
    --offsite)
        BASE="${1:-$DEFAULT_OFFSITE_BASE}"
        run_backup "${BASE}/$(date -u '+%Y-%m-%d_%H%M%S')" "offsite"
        ;;
    --status)
        cmd_status "${1:-}"
        ;;
    --restore)
        cmd_restore "${1:-}"
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
