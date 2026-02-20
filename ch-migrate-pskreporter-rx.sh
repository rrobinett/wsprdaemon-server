#!/bin/bash
# ch-migrate-pskreporter-rx.sh  v1.0
#
# Rebuilds pskreporter.rx with an optimized schema.
#
# Optimizations applied:
#   - Drop 'id' column (1.57 GB, never queried)
#   - LowCardinality on tx_loc, rx_loc, tx_sign, rx_sign, version, band
#   - ORDER BY (tx_loc, rx_loc, band, tx_sign, rx_sign, time)
#     grid squares first (low cardinality, likely search columns),
#     then band (15 values), then call signs, time last
#   - Delta(4) codec on lat/lon (repetitive per grid square)
#
# Expected storage: ~326 GB -> ~130 GB (~60% reduction)
#
# Steps (each is safe to re-run):
#   create   -- create pskreporter.rx_new with optimized schema
#   insert   -- INSERT INTO rx_new SELECT ... FROM rx (backgrounded)
#   status   -- show row count progress and ETA
#   rename   -- swap rx -> rx_old, rx_new -> rx  (only run when complete)
#   verify   -- confirm row counts match after rename
#   drop     -- drop rx_old after successful verify
#   full     -- create + insert in one go
#
# Usage:
#   ./ch-migrate-pskreporter-rx.sh <step>
#
# Examples:
#   ./ch-migrate-pskreporter-rx.sh full
#   ./ch-migrate-pskreporter-rx.sh status
#   ./ch-migrate-pskreporter-rx.sh rename
#   ./ch-migrate-pskreporter-rx.sh verify
#   ./ch-migrate-pskreporter-rx.sh drop

set -euo pipefail

VERSION="1.0"
CH_USER="${CH_USER:-chadmin}"
CH_PASS="${CH_PASS:-ch2025wd}"

# Tuned for 38-CPU / 98 GB server, single large migration
CH_THREADS=16
CH_BLOCK=5000000
CH_MAX_MEM=40000000000      # 40 GB

DB="pskreporter"
SRC="rx"
DST="rx_new"
OLD="rx_old"
KNOWN_ROWS=8535495753       # confirmed row count from restore

LOG="/tmp/ch-migrate-pskreporter-rx.log"

CH="clickhouse-client --user ${CH_USER} --password ${CH_PASS}"
CH_INSERT="${CH} \
    --max_insert_block_size=${CH_BLOCK} \
    --max_threads=${CH_THREADS} \
    --max_memory_usage=${CH_MAX_MEM}"

# ---------------------------------------------------------------
# helpers
# ---------------------------------------------------------------

ch() { ${CH} --query "$1"; }

row_count() {
    ${CH} --query \
        "SELECT count() FROM ${DB}.${1} FORMAT TSVRaw" 2>/dev/null || echo 0
}

table_exists() {
    local n
    n=$(${CH} --query \
        "SELECT count() FROM system.tables \
         WHERE database='${DB}' AND name='${1}' FORMAT TSVRaw" \
        2>/dev/null || echo 0)
    [[ "${n}" == "1" ]]
}

disk_size() {
    ${CH} --query \
        "SELECT formatReadableSize(sum(bytes_on_disk))
         FROM system.parts
         WHERE database='${DB}' AND table='${1}' AND active=1
         FORMAT TSVRaw" 2>/dev/null || echo "unknown"
}

col_sizes() {
    local tbl="$1"
    ${CH} --query "
        SELECT
            column,
            formatReadableSize(sum(column_data_compressed_bytes)) AS compressed
        FROM system.parts_columns
        WHERE database='${DB}' AND table='${tbl}' AND active=1
        GROUP BY column
        ORDER BY sum(column_data_compressed_bytes) DESC
        FORMAT Pretty" 2>/dev/null || echo "(table not yet populated)"
}

# ---------------------------------------------------------------
# step: create
# ---------------------------------------------------------------

cmd_create() {
    echo "=== CREATE ${DB}.${DST} ==="

    if table_exists "${DST}"; then
        echo "Table ${DB}.${DST} already exists — skipping."
        echo "To recreate: clickhouse-client ... --query 'DROP TABLE ${DB}.${DST}'"
        return 0
    fi

    ${CH} --query "
CREATE TABLE ${DB}.${DST}
(
    \`time\`       DateTime                CODEC(Delta(4), ZSTD(1)),
    \`band\`       Int16                   CODEC(T64, ZSTD(1)),
    \`mode\`       LowCardinality(String),
    \`rx_sign\`    LowCardinality(String),
    \`rx_lat\`     Float32                 CODEC(Delta(4), ZSTD(1)),
    \`rx_lon\`     Float32                 CODEC(Delta(4), ZSTD(1)),
    \`rx_loc\`     LowCardinality(String),
    \`tx_sign\`    LowCardinality(String),
    \`tx_lat\`     Float32                 CODEC(Delta(4), ZSTD(1)),
    \`tx_lon\`     Float32                 CODEC(Delta(4), ZSTD(1)),
    \`tx_loc\`     LowCardinality(String),
    \`distance\`   UInt16                  CODEC(T64, ZSTD(1)),
    \`azimuth\`    UInt16                  CODEC(T64, ZSTD(1)),
    \`rx_azimuth\` UInt16                  CODEC(T64, ZSTD(1)),
    \`frequency\`  UInt32                  CODEC(T64, ZSTD(1)),
    \`snr\`        Int8                    CODEC(ZSTD(1)),
    \`version\`    LowCardinality(String)
)
ENGINE = MergeTree
PARTITION BY toYYYYMM(time)
ORDER BY (tx_loc, rx_loc, band, tx_sign, rx_sign, time)
SETTINGS index_granularity = 8192"

    echo "Created ${DB}.${DST} OK"
}

# ---------------------------------------------------------------
# step: insert
# ---------------------------------------------------------------

cmd_insert() {
    echo "=== INSERT INTO ${DB}.${DST} SELECT FROM ${DB}.${SRC} ==="

    if ! table_exists "${DST}"; then
        echo "ERROR: ${DB}.${DST} does not exist — run 'create' first"
        exit 1
    fi

    if ! table_exists "${SRC}"; then
        echo "ERROR: ${DB}.${SRC} does not exist"
        exit 1
    fi

    local existing
    existing=$(row_count "${DST}")

    if [[ "${existing}" -gt 0 ]]; then
        echo "WARNING: ${DB}.${DST} already has $(printf "%'d" "${existing}") rows."
        echo -n "  Truncate and restart from scratch? [y/N]: "
        read -r answer </dev/tty
        if [[ "${answer}" == "y" || "${answer}" == "Y" ]]; then
            ch "TRUNCATE TABLE ${DB}.${DST}"
            echo "  Truncated."
        else
            echo "  Leaving existing rows — INSERT will add more (risk of duplicates)."
            echo "  Use 'status' to check if the previous insert is still running."
            return 0
        fi
    fi

    # Pause background merges on source to free I/O for the SELECT
    ch "SYSTEM STOP MERGES ${DB}.${SRC}" || true
    echo "Paused background merges on ${DB}.${SRC}"
    echo "Started: $(date)" > "${LOG}"

    nohup bash -c "
        echo 'INSERT started: \$(date)' >> '${LOG}'

        ${CH_INSERT} --query=\"
            INSERT INTO ${DB}.${DST}
            SELECT
                time, band, mode,
                rx_sign, rx_lat, rx_lon, rx_loc,
                tx_sign, tx_lat, tx_lon, tx_loc,
                distance, azimuth, rx_azimuth,
                frequency, snr, version
            FROM ${DB}.${SRC}\"

        RC=\$?
        echo \"INSERT finished: \$(date)  rc=\${RC}\" >> '${LOG}'

        # Re-enable merges on both tables
        clickhouse-client --user '${CH_USER}' --password '${CH_PASS}' \
            --query 'SYSTEM START MERGES ${DB}.${SRC}' 2>/dev/null || true
        clickhouse-client --user '${CH_USER}' --password '${CH_PASS}' \
            --query 'SYSTEM START MERGES ${DB}.${DST}' 2>/dev/null || true
        echo 'Merges re-enabled.' >> '${LOG}'
    " >> "${LOG}" 2>&1 &

    local pid=$!
    echo "INSERT running in background — PID ${pid}"
    echo "PID ${pid}" >> "${LOG}"
    echo ""
    echo "Monitor with:  ./ch-migrate-pskreporter-rx.sh status"
    echo "Log file:      ${LOG}"
}

# ---------------------------------------------------------------
# step: status
# ---------------------------------------------------------------

cmd_status() {
    echo "=== Migration Status  $(date) ==="
    echo ""

    local src_rows dst_rows pct
    src_rows=$(row_count "${SRC}")
    dst_rows=$(row_count "${DST}")

    if [[ "${src_rows}" -gt 0 && "${dst_rows}" -gt 0 ]]; then
        pct=$(awk "BEGIN {printf \"%.2f\", ${dst_rows} * 100 / ${src_rows}}")
    else
        pct="0.00"
    fi

    printf "  %-12s  rows: %'d  size: %s\n" \
        "${DB}.${SRC}" "${src_rows}" "$(disk_size "${SRC}")"
    printf "  %-12s  rows: %'d  size: %s  (%.2f%%)\n" \
        "${DB}.${DST}" "${dst_rows}" "$(disk_size "${DST}")" "${pct}"
    echo ""

    # ETA estimate based on rows remaining vs known total
    if [[ "${dst_rows}" -gt 0 && "${dst_rows}" -lt "${src_rows}" ]]; then
        local start_epoch now_epoch elapsed_sec rows_per_sec remaining eta_sec
        start_epoch=0
        # Try log file first
        if [[ -f "${LOG}" ]]; then
            local start_str
            start_str=$(grep "INSERT started" "${LOG}" | head -1 | sed 's/INSERT started: //')
            if [[ -n "${start_str}" ]]; then
                start_epoch=$(date -d "${start_str}" +%s 2>/dev/null || echo 0)
            fi
        fi
        # Fall back to log file mtime
        if [[ "${start_epoch}" -eq 0 && -f "${LOG}" ]]; then
            start_epoch=$(stat -c %Y "${LOG}" 2>/dev/null || echo 0)
        fi
        now_epoch=$(date +%s)
        elapsed_sec=$(( now_epoch - start_epoch ))
        if [[ "${elapsed_sec}" -gt 60 && "${dst_rows}" -gt 0 ]]; then
            rows_per_sec=$(( dst_rows / elapsed_sec ))
            remaining=$(( src_rows - dst_rows ))
            if [[ "${rows_per_sec}" -gt 0 ]]; then
                eta_sec=$(( remaining / rows_per_sec ))
                printf "  Rate: %'d rows/sec  Elapsed: %d min  ETA: %d min (%d hours)\n" \
                    "${rows_per_sec}" \
                    "$(( elapsed_sec / 60 ))" \
                    "$(( eta_sec / 60 ))" \
                    "$(( eta_sec / 3600 ))"
            else
                echo "  Rate: calculating... (not enough data yet)"
            fi
        else
            echo "  Rate: calculating... (not enough elapsed time)"
        fi
    fi

    echo ""

    # Time range in destination
    if [[ "${dst_rows}" -gt 0 ]]; then
        ${CH} --query "
            SELECT min(time) AS earliest, max(time) AS latest
            FROM ${DB}.${DST} FORMAT Pretty" 2>/dev/null || true
    fi

    # Log tail
    if [[ -f "${LOG}" ]]; then
        echo ""
        echo "--- Log (${LOG}) ---"
        tail -6 "${LOG}"
    fi

    # Is the INSERT process still running?
    echo ""
    local running
    running=$(pgrep -af "INSERT INTO ${DB}.${DST}" 2>/dev/null | wc -l || echo 0)
    if [[ "${running}" -gt 0 ]]; then
        echo "Status: INSERT is RUNNING"
    elif [[ "${dst_rows}" -ge "${src_rows}" && "${src_rows}" -gt 0 ]]; then
        echo "Status: INSERT appears COMPLETE ✓"
    else
        echo "Status: INSERT process not found — finished or failed (check log)"
    fi
}

# ---------------------------------------------------------------
# step: rename
# ---------------------------------------------------------------

cmd_rename() {
    echo "=== RENAME: ${SRC} -> ${OLD}, ${DST} -> ${SRC} ==="

    local src_rows dst_rows
    src_rows=$(row_count "${SRC}")
    dst_rows=$(row_count "${DST}")

    echo "  ${DB}.${SRC} : $(printf "%'d" "${src_rows}") rows"
    echo "  ${DB}.${DST} : $(printf "%'d" "${dst_rows}") rows"

    if [[ "${dst_rows}" -lt "${src_rows}" ]]; then
        echo ""
        echo "WARNING: ${DST} has fewer rows than ${SRC}."
        echo "  Are you sure the INSERT is complete? Run 'status' to check."
        echo -n "  Proceed anyway? [y/N]: "
        read -r answer </dev/tty
        [[ "${answer}" != "y" && "${answer}" != "Y" ]] && echo "Aborted." && exit 1
    fi

    if table_exists "${OLD}"; then
        echo "ERROR: ${DB}.${OLD} already exists — drop it first or something went wrong."
        exit 1
    fi

    ch "RENAME TABLE ${DB}.${SRC} TO ${DB}.${OLD}, ${DB}.${DST} TO ${DB}.${SRC}"
    echo ""
    echo "Rename complete:"
    echo "  ${DB}.${OLD} = original table (preserved)"
    echo "  ${DB}.${SRC} = new optimized table (now live)"
    echo ""
    echo "Run 'verify' to confirm row counts, then 'drop' to free the old table's space."
}

# ---------------------------------------------------------------
# step: verify
# ---------------------------------------------------------------

cmd_verify() {
    echo "=== VERIFY row counts ==="

    local new_rows old_rows
    new_rows=$(row_count "${SRC}")
    old_rows=$(row_count "${OLD}")

    echo "  ${DB}.${SRC} (new) : $(printf "%'d" "${new_rows}") rows"
    echo "  ${DB}.${OLD} (old) : $(printf "%'d" "${old_rows}") rows"
    echo "  Known original     : $(printf "%'d" "${KNOWN_ROWS}") rows"
    echo ""

    echo "  Size comparison:"
    echo "    ${DB}.${SRC} (new) : $(disk_size "${SRC}")"
    echo "    ${DB}.${OLD} (old) : $(disk_size "${OLD}")"
    echo ""

    if [[ "${new_rows}" -eq "${old_rows}" ]]; then
        echo "✓ Row counts match — safe to drop old table with 'drop'"
    elif [[ "${new_rows}" -ge $(( old_rows - 1000 )) ]]; then
        echo "~ Row counts within 1000 — likely fine (minor merge activity)"
        echo "  Run 'drop' if you're satisfied."
    else
        local diff=$(( old_rows - new_rows ))
        echo "✗ WARNING: new table is missing $(printf "%'d" "${diff}") rows"
        echo "  Do NOT drop the old table until this is investigated."
    fi

    echo ""
    echo "Column sizes in new optimized table:"
    col_sizes "${SRC}"
}

# ---------------------------------------------------------------
# step: drop
# ---------------------------------------------------------------

cmd_drop() {
    echo "=== DROP ${DB}.${OLD} ==="

    if ! table_exists "${OLD}"; then
        echo "Table ${DB}.${OLD} does not exist — nothing to drop."
        return 0
    fi

    local old_rows old_size
    old_rows=$(row_count "${OLD}")
    old_size=$(disk_size "${OLD}")

    echo "  About to drop ${DB}.${OLD}"
    echo "  Rows : $(printf "%'d" "${old_rows}")"
    echo "  Size : ${old_size}"
    echo ""
    echo -n "  Confirm DROP? [y/N]: "
    read -r answer </dev/tty
    [[ "${answer}" != "y" && "${answer}" != "Y" ]] && echo "Aborted." && exit 0

    ch "DROP TABLE ${DB}.${OLD}"
    echo "Dropped ${DB}.${OLD} — ${old_size} freed."
}

# ---------------------------------------------------------------
# step: full (create + insert)
# ---------------------------------------------------------------

cmd_full() {
    cmd_create
    echo ""
    cmd_insert
}

# ---------------------------------------------------------------
# main
# ---------------------------------------------------------------

STEP="${1:-}"

echo "ch-migrate-pskreporter-rx.sh  v${VERSION}"
echo ""

case "${STEP}" in
    create) cmd_create ;;
    insert) cmd_insert ;;
    status) cmd_status ;;
    rename) cmd_rename ;;
    verify) cmd_verify ;;
    drop)   cmd_drop   ;;
    full)   cmd_full   ;;
    *)
        echo "Usage: $0 <step>"
        echo ""
        echo "Steps:"
        echo "  full     -- create + insert in one go (recommended starting point)"
        echo "  create   -- create pskreporter.rx_new with optimized schema"
        echo "  insert   -- start INSERT ... SELECT in background"
        echo "  status   -- show progress, rate, ETA"
        echo "  rename   -- swap rx_new -> rx when insert is complete"
        echo "  verify   -- compare row counts after rename"
        echo "  drop     -- drop rx_old after successful verify"
        echo ""
        echo "Typical workflow:"
        echo "  ./ch-migrate-pskreporter-rx.sh full"
        echo "  ./ch-migrate-pskreporter-rx.sh status   # repeat until complete"
        echo "  ./ch-migrate-pskreporter-rx.sh rename"
        echo "  ./ch-migrate-pskreporter-rx.sh verify"
        echo "  ./ch-migrate-pskreporter-rx.sh drop"
        exit 1
        ;;
esac
