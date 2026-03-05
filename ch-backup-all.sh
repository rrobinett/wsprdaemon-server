#!/bin/bash
#
# ch-backup-all.sh - Backup all ClickHouse user tables
# Version: 3.3.0
#
# Usage:
#   ch-backup-all.sh --local        [base_dir]  ClickHouse native backup (default: /mnt/ch_archive1/ch-backups)
#   ch-backup-all.sh --local-zstd   [base_dir]  zstd-compressed pipe backup (faster on spinning disk)
#   ch-backup-all.sh --offsite      [base_dir]  ClickHouse native backup (default: /mnt/offsite/ch-backups)
#   ch-backup-all.sh --offsite-zstd [base_dir]  zstd-compressed pipe backup
#   ch-backup-all.sh --status       [backup_dir] Show progress of most recent or specified backup
#   ch-backup-all.sh --refresh      [backup_dir] Refresh stale size estimates in state file
#   ch-backup-all.sh --restore      [backup_dir] Restore all tables from a native backup
#

set -euo pipefail

VERSION="3.14.0"
CH_CONF="/etc/wsprdaemon/clickhouse.conf"
STATE_FILE_NAME="backup-state.tsv"

DEFAULT_LOCAL_BASE="/srv/wd_archive/ch-archives"
DEFAULT_OFFSITE_BASE="/mnt/offsite/ch-backups"

ZSTD_CORES=4          # cores per zstd instance; with 9 parallel tables x 4 = 36 total
ZSTD_LEVEL_LOCAL=1    # local backup: fast compression, space less critical
ZSTD_LEVEL_OFFSITE=3  # offsite backup: better compression for smaller transfer size

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
        echo "$arg"; return
    fi
    # Find the most recently modified backup across all search paths
    local best="" best_time=0
    for base in "${STATUS_SEARCH_PATHS[@]}"; do
        [[ -d "$base" ]] || continue
        for candidate in "${base}"/20*/; do
            [[ -f "${candidate}/${STATE_FILE_NAME}" ]] || continue
            local mtime
            mtime=$(stat -c%Y "$candidate" 2>/dev/null || echo 0)
            if (( mtime > best_time )); then
                best_time=$mtime
                best="$candidate"
            fi
        done
    done
    echo "${best%/}"
}

get_current_sizes() {
    ch_query "
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
        FORMAT TSV"
}

cmd_refresh() {
    local archive_dir
    archive_dir=$(find_backup_dir "${1:-}")
    if [[ -z "$archive_dir" || ! -d "$archive_dir" ]]; then
        echo "ERROR: No backup directory found." >&2; exit 1
    fi
    local state_file="${archive_dir}/${STATE_FILE_NAME}"
    local mode
    mode=$(grep "^#mode:" "$state_file" | cut -d: -f2 || echo "unknown")
    echo "Refreshing size estimates in: $state_file"
    { echo "#mode:${mode}"; get_current_sizes; } > "${state_file}"
    echo "Done. Run --status to see updated sizes."
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

    # Snapshot running processes once before the loop to avoid stdin conflict
    local ps_tmp
    ps_tmp=$(mktemp)
    ps auxww 2>/dev/null > "$ps_tmp"

    while IFS=$'\t' read -r db tbl expected_rows expected_bytes; do
        [[ "$db" == \#* ]] && continue
        total=$(( total + 1 ))
        total_expected_bytes=$(( total_expected_bytes + expected_bytes ))

        local outdir="${archive_dir}/${db}.${tbl}"
        local outfile_zst="${archive_dir}/${db}.${tbl}.native.zst"
        local outfile_gz="${archive_dir}/${db}.${tbl}.native.gz"
        local status written_bytes pct

        # Determine output and whether it's running
        if [[ -d "$outdir" ]]; then
            # Native backup mode
            written_bytes=$(sudo du -sb "$outdir" 2>/dev/null | awk '{print $1}' || echo 0)
            if lsof "$outdir" 2>/dev/null | grep -q "clickhous"; then
                status="RUNNING"; running=$(( running + 1 ))
            else
                status="DONE"; done_count=$(( done_count + 1 ))
            fi
        elif [[ -f "$outfile_zst" ]]; then
            written_bytes=$(stat -c%s "$outfile_zst" 2>/dev/null || echo 0)
            if grep -v grep "$ps_tmp" | grep -qF "${outfile_zst}"; then
                status="RUNNING"; running=$(( running + 1 ))
            else
                status="DONE"; done_count=$(( done_count + 1 ))
            fi
        elif [[ -f "$outfile_gz" ]]; then
            written_bytes=$(stat -c%s "$outfile_gz" 2>/dev/null || echo 0)
            if grep -v grep "$ps_tmp" | grep -qF "${outfile_gz}"; then
                status="RUNNING"; running=$(( running + 1 ))
            else
                status="DONE"; done_count=$(( done_count + 1 ))
            fi
        else
            written_bytes=0
            status="PENDING"; pending=$(( pending + 1 ))
        fi

        total_written_bytes=$(( total_written_bytes + written_bytes ))

        if (( expected_bytes > 0 && written_bytes > 0 )); then
            pct=$(awk "BEGIN{printf \"%.0f%%\", 100*${written_bytes}/${expected_bytes}}")
        else
            pct="0%"
        fi

        printf "  %-42s  %10s  %10s  %6s  %s\n" \
               "${db}.${tbl}" \
               "$(human_bytes "$expected_bytes")" \
               "$(human_bytes "$written_bytes")" \
               "$pct" "$status"

    done < "$state_file"
    rm -f "$ps_tmp"

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

run_native_backup() {
    local backup_dir="$1"
    local mode="$2"

    mkdir -p "$backup_dir"
    chown clickhouse:clickhouse "$backup_dir"

    echo "ch-backup-all.sh v${VERSION}"
    echo "Mode:             $mode (ClickHouse native backup)"
    echo "Backup directory: $backup_dir"
    echo "Started:          $(date -u)"
    echo "Monitor with:     $0 --status"
    echo ""

    local tables
    tables=$(get_current_sizes)
    { echo "#mode:${mode}"; echo "$tables"; } > "${backup_dir}/${STATE_FILE_NAME}"

    local failed=0
    while IFS=$'\t' read -r db tbl rows bytes; do
        local size
        size=$(human_bytes "${bytes:-0}")
        echo "Starting: ${db}.${tbl}  (${rows} rows, ${size})"
        ch_query "SHOW CREATE TABLE ${db}.${tbl}" > "${backup_dir}/${db}.${tbl}.schema.sql"
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
    echo "Total size: $(sudo du -sh "$backup_dir" | cut -f1)"
    if (( failed > 0 )); then echo "FAILURES: $failed tables failed"; exit 1; fi
    echo "All tables backed up successfully"
}

run_zstd_backup() {
    local backup_dir="$1"
    local mode="$2"
    local zstd_level="$3"

    mkdir -p "$backup_dir"

    echo "ch-backup-all.sh v${VERSION}"
    echo "Mode:             $mode (zstd -${zstd_level} compressed, ${ZSTD_CORES} cores/table)"
    echo "Backup directory: $backup_dir"
    echo "Started:          $(date -u)"
    echo ""

    local tables
    tables=$(get_current_sizes)
    { echo "#mode:${mode}"; echo "$tables"; } > "${backup_dir}/${STATE_FILE_NAME}"

    local pids=()
    declare -A table_pids

    while IFS=$'\t' read -r db tbl rows bytes; do
        local size
        size=$(human_bytes "${bytes:-0}")
        echo "Starting: ${db}.${tbl}  (${rows} rows, ${size})"
        ch_query "SHOW CREATE TABLE ${db}.${tbl}" > "${backup_dir}/${db}.${tbl}.schema.sql"

        clickhouse-client --user "$CH_USER" --password "$CH_PASS" \
            --max_execution_time 0 --receive_timeout 604800 \
            --query "SELECT * FROM ${db}.${tbl} FORMAT Native" \
            | zstd -q -T"${ZSTD_CORES}" -"${zstd_level}" -o "${backup_dir}/${db}.${tbl}.native.zst" &

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
            local sz
            sz=$(ls -lh "${backup_dir}/${tbl}.native.zst" 2>/dev/null | awk '{print $5}')
            echo "OK:   ${tbl}  (${sz})"
        else
            echo "FAIL: ${tbl}"
            failed=$(( failed + 1 ))
        fi
    done

    echo ""
    echo "Completed: $(date -u)"
    echo "Total size: $(du -sh "$backup_dir" | cut -f1)"
    if (( failed > 0 )); then echo "FAILURES: $failed tables failed"; exit 1; fi
    echo "All tables backed up successfully"
}

run_zstd_backup_seq() {
    local backup_dir="$1"
    local mode="$2"
    local zstd_level="$3"

    mkdir -p "$backup_dir"

    echo "ch-backup-all.sh v${VERSION}"
    echo "Mode:             $mode (zstd -${zstd_level} sequential, ${ZSTD_CORES} cores/table)"
    echo "Backup directory: $backup_dir"
    echo "Started:          $(date -u)"
    echo "Monitor with:     $0 --status"
    echo ""

    local tables
    tables=$(get_current_sizes)
    { echo "#mode:${mode}"; echo "$tables"; } > "${backup_dir}/${STATE_FILE_NAME}"

    local failed=0
    while IFS=$'\t' read -r db tbl rows bytes; do
        local size
        size=$(human_bytes "${bytes:-0}")
        echo "Starting: ${db}.${tbl}  (${rows} rows, ${size})"
        ch_query "SHOW CREATE TABLE ${db}.${tbl}" > "${backup_dir}/${db}.${tbl}.schema.sql"

        if clickhouse-client --user "$CH_USER" --password "$CH_PASS" \
                --max_execution_time 0 --receive_timeout 604800 \
                --query "SELECT * FROM ${db}.${tbl} FORMAT Native" \
                | zstd -q -T"${ZSTD_CORES}" -"${zstd_level}" -o "${backup_dir}/${db}.${tbl}.native.zst"; then
            local sz
            sz=$(ls -lh "${backup_dir}/${db}.${tbl}.native.zst" 2>/dev/null | awk '{print $5}')
            echo "OK:   ${db}.${tbl}  (${sz})"
        else
            echo "FAIL: ${db}.${tbl}"
            failed=$(( failed + 1 ))
        fi
    done <<< "$tables"

    echo ""
    echo "Completed: $(date -u)"
    echo "Total size: $(du -sh "$backup_dir" | cut -f1)"
    if (( failed > 0 )); then echo "FAILURES: $failed tables failed"; exit 1; fi
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
            echo "  SKIP ${db}.${tbl} (no native backup dir found)"
        fi
    done < "$state_file"
    echo "Restore complete."
}

usage() {
    echo "ch-backup-all.sh v${VERSION}"
    echo ""
    echo "Usage:"
    echo "  $0 --local        [base_dir]   ClickHouse native backup (sequential, low CPU)"
    echo "                                  Default: ${DEFAULT_LOCAL_BASE}"
    echo "  $0 --local-zstd   [base_dir]   zstd pipe backup (parallel, faster on spinning disk)"
    echo "                                  Default: ${DEFAULT_LOCAL_BASE}"
    echo "  $0 --offsite      [base_dir]   ClickHouse native backup for offsite"
    echo "                                  Default: ${DEFAULT_OFFSITE_BASE}"
    echo "  $0 --offsite-zstd [base_dir]   zstd pipe backup for offsite"
    echo "                                  Default: ${DEFAULT_OFFSITE_BASE}"
    echo "  $0 --status       [backup_dir] Show progress (auto-finds most recent)"
    echo "  $0 --refresh      [backup_dir] Refresh stale size estimates"
    echo "  $0 --restore      [backup_dir] Restore all tables from native backup"
    echo "  $0 --version"
}

CMD="${1:-}"
shift || true

case "$CMD" in
    --local)
        BASE="${1:-$DEFAULT_LOCAL_BASE}"
        run_native_backup "${BASE}/$(date -u '+%Y-%m-%d_%H%M%S')" "local-native"
        ;;
    --local-zstd)
        BASE="${1:-$DEFAULT_LOCAL_BASE}"
        run_zstd_backup "${BASE}/$(date -u '+%Y-%m-%d_%H%M%S')" "local-zstd" "${ZSTD_LEVEL_LOCAL}"
        ;;
    --offsite)
        BASE="${1:-$DEFAULT_OFFSITE_BASE}"
        run_native_backup "${BASE}/$(date -u '+%Y-%m-%d_%H%M%S')" "offsite-native"
        ;;
    --offsite-zstd)
        BASE="${1:-$DEFAULT_OFFSITE_BASE}"
        run_zstd_backup "${BASE}/$(date -u '+%Y-%m-%d_%H%M%S')" "offsite-zstd" "${ZSTD_LEVEL_OFFSITE}"
        ;;
    --local-zstd-seq)
        BASE="${1:-$DEFAULT_LOCAL_BASE}"
        run_zstd_backup_seq "${BASE}/$(date -u '+%Y-%m-%d_%H%M%S')" "local-zstd-seq" "${ZSTD_LEVEL_LOCAL}"
        ;;
    --offsite-zstd-seq)
        BASE="${1:-$DEFAULT_OFFSITE_BASE}"
        run_zstd_backup_seq "${BASE}/$(date -u '+%Y-%m-%d_%H%M%S')" "offsite-zstd-seq" "${ZSTD_LEVEL_OFFSITE}"
        ;;
    --status)
        cmd_status "${1:-}"
        ;;
    --refresh)
        cmd_refresh "${1:-}"
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
        usage
        exit 1
        ;;
esac
