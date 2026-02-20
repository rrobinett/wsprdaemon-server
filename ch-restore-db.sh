#!/bin/bash
# ch-restore-db.sh  v2.0
#
# Parallel restore of ClickHouse databases from a backup made by
# backup_clickhouse_parallel.sh.
#
# Resource defaults are tuned for a server with 38 CPUs / 98 GB RAM,
# using ~18 CPUs and ~48 GB RAM:
#   PARALLEL_TABLES=6     -- tables restored concurrently
#   PIGZ_THREADS=3        -- decompression threads per table (6x3=18 total)
#   CH_THREADS=3          -- CH INSERT threads per table
#   CH_BLOCK=2000000      -- rows per INSERT block (~4 GB RAM per table stream)
#
# Backup format:
#   {backup_dir}/{db}/{table}.schema.sql    -- SHOW CREATE TABLE output
#   {backup_dir}/{db}/{table}.native.gz     -- Native format gzipped data
#   {backup_dir}/{db}/{table}.EMPTY         -- marker for empty tables
#
# Usage:
#   ch-restore-db.sh <backup_dir> [options...] [db1 db2 ...]
#
# Options (order-independent, combinable):
#   dry              Preview only - no ClickHouse changes made
#   <integer>        Insert only that many rows per table (test mode)
#   drop             Drop existing tables before restore (no prompt)
#   resume           Skip tables that already have rows (restart after failure)
#   peek             Pretty-print first row of each table after restore
#   jobs=N           Override parallel table count (default: 6)
#   pigz=N           Override pigz threads per table (default: 3)
#   chthreads=N      Override CH INSERT threads per table (default: 3)
#   block=N          Override CH INSERT block size rows (default: 2000000)
#
# Examples:
#   ./ch-restore-db.sh $BACKUP dry pskreporter
#   ./ch-restore-db.sh $BACKUP 10 drop peek pskreporter
#   ./ch-restore-db.sh $BACKUP drop resume pskreporter
#   ./ch-restore-db.sh $BACKUP drop jobs=4 pigz=4 pskreporter
#   ./ch-restore-db.sh $BACKUP                              # all databases
#
# Credentials via env vars:
#   CH_USER=chadmin   CH_PASS=ch2025wd

VERSION="2.0"

# ---- tunable defaults (18 CPU / 48 GB budget) ----
# 6 parallel tables x 3 pigz threads  = 18 decompression threads
# 6 parallel tables x 3 CH threads    = 18 CH INSERT threads
# 6 parallel tables x ~4 GB per stream = ~24 GB peak; CH internal buffers ~24 GB
PARALLEL_TABLES=6
PIGZ_THREADS=3
CH_THREADS=3
CH_BLOCK=2000000        # rows per insert block
CH_MAX_MEM=8000000000   # 8 GB max memory per INSERT (6x = 48 GB)

CH_USER="${CH_USER:-chadmin}"
CH_PASS="${CH_PASS:-ch2025wd}"

DRY_RUN=0
MAX_ROWS=0
DROP_EXISTING=0
RESUME=0
PEEK=0

# ---- parse arguments ----
if [[ $# -lt 1 ]]; then
    cat <<EOF
Usage: $0 <backup_dir> [dry|<max_rows>] [drop] [resume] [peek]
           [jobs=N] [pigz=N] [chthreads=N] [block=N] [db1 db2 ...]
EOF
    exit 1
fi

BACKUP_DIR="$1"; shift

TARGET_DBS=()
for arg in "$@"; do
    case "${arg}" in
        dry|DRY)             DRY_RUN=1 ;;
        drop|DROP)           DROP_EXISTING=1 ;;
        resume|RESUME)       RESUME=1 ;;
        peek|PEEK)           PEEK=1 ;;
        jobs=*)              PARALLEL_TABLES="${arg#jobs=}" ;;
        pigz=*)              PIGZ_THREADS="${arg#pigz=}" ;;
        chthreads=*)         CH_THREADS="${arg#chthreads=}" ;;
        block=*)             CH_BLOCK="${arg#block=}" ;;
        ''|*[!0-9]*)         TARGET_DBS+=("${arg}") ;;   # DB name
        *)                   MAX_ROWS="${arg}" ;;          # integer = row limit
    esac
done

if [[ ! -d "${BACKUP_DIR}" ]]; then
    echo "ERROR: backup directory not found: '${BACKUP_DIR}'"
    exit 1
fi

# CH client base command with performance tuning
CH="clickhouse-client --user ${CH_USER} --password ${CH_PASS}"
CH_INSERT="${CH} \
    --max_insert_block_size=${CH_BLOCK} \
    --max_threads=${CH_THREADS} \
    --max_memory_usage=${CH_MAX_MEM} \
    --async_insert=0"

# Decompressor: prefer pigz (parallel), fall back to zcat
if command -v pigz &>/dev/null; then
    DECOMP="pigz -d -c -p ${PIGZ_THREADS}"
    DECOMP_NAME="pigz (${PIGZ_THREADS} threads)"
else
    DECOMP="zcat"
    DECOMP_NAME="zcat (single-thread; install pigz for better performance)"
fi

echo "=========================================="
echo "ClickHouse Parallel Restore  v${VERSION}"
echo "=========================================="
echo "Backup dir      : ${BACKUP_DIR}"
echo "Parallel tables : ${PARALLEL_TABLES}"
echo "Decompressor    : ${DECOMP_NAME}"
echo "CH INSERT threads/table : ${CH_THREADS}"
echo "CH block size   : $(printf "%'d" ${CH_BLOCK}) rows"
echo "CH max mem/table: $(( CH_MAX_MEM / 1024 / 1024 / 1024 )) GB"

MODE_DESC="FULL restore"
[[ "${DRY_RUN}"       == "1" ]] && MODE_DESC="DRY RUN (no changes)"
[[ "${MAX_ROWS}"      != "0" ]] && MODE_DESC="TEST (max ${MAX_ROWS} rows/table)"
[[ "${DROP_EXISTING}" == "1" ]] && MODE_DESC+=" + DROP existing"
[[ "${RESUME}"        == "1" ]] && MODE_DESC+=" + RESUME"
[[ "${PEEK}"          == "1" ]] && MODE_DESC+=" + PEEK"
echo "Mode            : ${MODE_DESC}"

# Verify credentials
if [[ "${DRY_RUN}" != "1" ]]; then
    if ! ${CH} --query "SELECT 1 FORMAT TSVRaw" &>/dev/null; then
        echo "ERROR: Cannot connect to ClickHouse as ${CH_USER}"
        exit 1
    fi
    echo "Auth            : OK (${CH_USER})"
fi

# Determine target databases
if [[ ${#TARGET_DBS[@]} -eq 0 ]]; then
    mapfile -t TARGET_DBS < <(
        find "${BACKUP_DIR}" -mindepth 1 -maxdepth 1 -type d -printf '%f\n' | sort
    )
    echo "Restoring       : all databases: ${TARGET_DBS[*]}"
else
    echo "Restoring       : ${TARGET_DBS[*]}"
fi
echo ""

# ---- shared state via temp files (parallel-safe) ----
TMPDIR_STATE=$(mktemp -d)
trap 'rm -rf "${TMPDIR_STATE}"' EXIT

SUCCESS_FILE="${TMPDIR_STATE}/success"
SKIP_FILE="${TMPDIR_STATE}/skip"
ERROR_FILE="${TMPDIR_STATE}/error"
touch "${SUCCESS_FILE}" "${SKIP_FILE}" "${ERROR_FILE}"

log_success() { echo "$1" >> "${SUCCESS_FILE}"; }
log_skip()    { echo "$1" >> "${SKIP_FILE}";    }
log_error()   { echo "$1" >> "${ERROR_FILE}";   }

# ---- helpers ----

clean_schema() {
    # Normalise literal \n sequences and strip BOM
    sed 's/\\n/\n/g' "$1" | sed 's/^\xef\xbb\xbf//'
}

peek_table() {
    local db="$1" table="$2"
    local rows
    rows=$(${CH} --query "SELECT count() FROM ${db}.${table} FORMAT TSVRaw" 2>/dev/null || echo 0)
    if [[ "${rows}" -eq 0 ]]; then
        echo "  [${db}.${table}] PEEK: table is empty"
        return
    fi
    echo "  [${db}.${table}] PEEK: first row (${rows} rows total):"
    ${CH} --query "SELECT * FROM ${db}.${table} LIMIT 1 FORMAT Pretty" \
        | sed "s/^/    [${table}] /"
}

# restore_table is called in a subshell by the parallel runner.
# It writes its own timestamped log lines prefixed with [db.table].
restore_table() {
    local db="$1" table="$2" db_dir="$3"
    local tag="[${db}.${table}]"

    local schema="${db_dir}/${table}.schema.sql"
    local data="${db_dir}/${table}.native.gz"
    local empty_marker="${db_dir}/${table}.EMPTY"

    if [[ ! -f "${schema}" ]]; then
        echo "  ${tag} SKIP - no schema file"
        log_skip "${db}.${table}"
        return
    fi

    # Check existing table
    local exists row_count
    exists=$(${CH} --query \
        "SELECT count() FROM system.tables WHERE database='${db}' AND name='${table}' FORMAT TSVRaw" \
        2>/dev/null || echo "0")

    if [[ "${exists}" == "1" ]]; then
        row_count=$(${CH} --query \
            "SELECT count() FROM ${db}.${table} FORMAT TSVRaw" 2>/dev/null || echo "0")

        if [[ "${RESUME}" == "1" && "${row_count}" -gt 0 ]]; then
            if [[ "${MAX_ROWS}" != "0" && "${row_count}" -ge "${MAX_ROWS}" ]]; then
                echo "  ${tag} RESUME: already has ${row_count} rows - skipping"
                [[ "${PEEK}" == "1" ]] && peek_table "${db}" "${table}"
                log_skip "${db}.${table}"
                return
            elif [[ "${MAX_ROWS}" == "0" ]]; then
                echo "  ${tag} RESUME: already has ${row_count} rows - skipping"
                [[ "${PEEK}" == "1" ]] && peek_table "${db}" "${table}"
                log_skip "${db}.${table}"
                return
            fi
        fi

        if [[ "${DROP_EXISTING}" == "1" ]]; then
            echo "  ${tag} Dropping existing table (${row_count} rows) ..."
            ${CH} --query "DROP TABLE IF EXISTS ${db}.${table}"
        else
            # In parallel mode we can't do interactive prompts safely.
            # If drop wasn't specified and table exists, skip with a warning.
            echo "  ${tag} WARNING: table exists (${row_count} rows) - use 'drop' to overwrite, skipping"
            log_skip "${db}.${table}"
            return
        fi
    fi

    # Create table
    echo "  ${tag} $(date +%H:%M:%S) Creating table ..."
    local create_err
    if create_err=$(clean_schema "${schema}" | ${CH} 2>&1); then
        echo "  ${tag} Table created OK"
    else
        echo "  ${tag} FAILED to create table: ${create_err}"
        echo "  ${tag} Schema first 5 lines:"
        head -5 "${schema}" | cat -A | sed "s/^/    ${tag} /"
        log_error "${db}.${table}"
        return
    fi

    # Load data
    if [[ -f "${empty_marker}" ]]; then
        echo "  ${tag} Empty table (EMPTY marker) - done."
        log_success "${db}.${table}"
        return
    fi

    if [[ ! -f "${data}" ]]; then
        echo "  ${tag} WARNING: no .native.gz and no .EMPTY marker"
        log_error "${db}.${table}"
        return
    fi

    local size
    size=$(du -sh "${data}" | cut -f1)
    echo "  ${tag} $(date +%H:%M:%S) Loading data (${size} compressed) ..."

    local insert_ok=0
    if [[ "${MAX_ROWS}" != "0" ]]; then
        # Test mode: limit rows via clickhouse-local
        if ${DECOMP} "${data}" \
            | clickhouse-local \
                --input-format=Native \
                --output-format=Native \
                --query="SELECT * FROM table LIMIT ${MAX_ROWS}" \
            | ${CH_INSERT} --query="INSERT INTO ${db}.${table} FORMAT Native"; then
            insert_ok=1
        fi
    else
        if ${DECOMP} "${data}" \
            | ${CH_INSERT} --query="INSERT INTO ${db}.${table} FORMAT Native"; then
            insert_ok=1
        fi
    fi

    if [[ "${insert_ok}" != "1" ]]; then
        echo "  ${tag} FAILED data insert"
        log_error "${db}.${table}"
        return
    fi

    local loaded
    loaded=$(${CH} --query "SELECT count() FROM ${db}.${table} FORMAT TSVRaw")
    echo "  ${tag} $(date +%H:%M:%S) Done - $(printf "%'d" "${loaded}") rows loaded"

    [[ "${PEEK}" == "1" ]] && peek_table "${db}" "${table}"

    log_success "${db}.${table}"
}

export -f restore_table clean_schema peek_table log_success log_skip log_error
export CH CH_INSERT DECOMP DRY_RUN MAX_ROWS DROP_EXISTING RESUME PEEK
export SUCCESS_FILE SKIP_FILE ERROR_FILE

# ---- parallel runner ----
# Spawns up to PARALLEL_TABLES background jobs, prints live output.

run_parallel() {
    local -a job_pids=()
    local job_count=0

    while IFS= read -r line; do
        # Wait if at job limit
        while [[ ${job_count} -ge ${PARALLEL_TABLES} ]]; do
            for i in "${!job_pids[@]}"; do
                if ! kill -0 "${job_pids[$i]}" 2>/dev/null; then
                    unset "job_pids[$i]"
                    (( job_count-- ))
                fi
            done
            [[ ${job_count} -ge ${PARALLEL_TABLES} ]] && sleep 0.3
        done

        # Launch job
        read -r db table db_dir <<< "${line}"
        (restore_table "${db}" "${table}" "${db_dir}") &
        job_pids+=($!)
        (( job_count++ ))

    done

    # Wait for all remaining jobs
    wait
}

# ---- main loop ----
for db in "${TARGET_DBS[@]}"; do
    db_dir="${BACKUP_DIR}/${db}"

    if [[ ! -d "${db_dir}" ]]; then
        echo "WARNING: no backup directory for '${db}': ${db_dir}"
        log_skip "${db}"
        continue
    fi

    echo "=========================================="
    echo "Database: ${db}"
    echo "=========================================="

    mapfile -t tables < <(
        find "${db_dir}" -maxdepth 1 -name '*.schema.sql' -printf '%f\n' \
        | sed 's/\.schema\.sql$//' | sort
    )

    if [[ ${#tables[@]} -eq 0 ]]; then
        echo "  No schema files found in ${db_dir}"
        log_skip "${db}"
        continue
    fi

    echo "  Tables (${#tables[@]}): ${tables[*]}"
    echo ""

    # DRY RUN
    if [[ "${DRY_RUN}" == "1" ]]; then
        for table in "${tables[@]}"; do
            s="${db_dir}/${table}.schema.sql"
            d="${db_dir}/${table}.native.gz"
            e="${db_dir}/${table}.EMPTY"
            echo "  [DRY-RUN] ${db}.${table}"
            [[ -f "${s}" ]] \
                && echo "    schema: EXISTS ($(wc -l < "${s}") lines) | $(head -1 "${s}")" \
                || echo "    schema: MISSING"
            if   [[ -f "${e}" ]]; then echo "    data  : (empty table)"
            elif [[ -f "${d}" ]]; then echo "    data  : EXISTS ($(du -sh "${d}" | cut -f1))"
            else                       echo "    data  : WARNING - missing"
            fi
        done
        echo ""
        continue
    fi

    # Create database
    ${CH} --query "CREATE DATABASE IF NOT EXISTS ${db}"

    # Build work list and run in parallel
    {
        for table in "${tables[@]}"; do
            echo "${db} ${table} ${db_dir}"
        done
    } | run_parallel

    echo ""
    echo "  Database ${db} complete."
    echo ""
done

# ---- summary ----
n_success=$(wc -l < "${SUCCESS_FILE}")
n_skip=$(wc -l < "${SKIP_FILE}")
n_error=$(wc -l < "${ERROR_FILE}")

echo "=========================================="
echo "Summary"
echo "=========================================="
echo "  Restored : ${n_success}"
echo "  Skipped  : ${n_skip}"
echo "  Errors   : ${n_error}"

if [[ "${n_error}" -gt 0 ]]; then
    echo ""
    echo "  Failed tables:"
    sed 's/^/    /' "${ERROR_FILE}"
fi

echo "  Completed: $(date)"

[[ "${n_error}" -gt 0 ]] && exit 1 || exit 0
