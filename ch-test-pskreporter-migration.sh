#!/bin/bash
# ch-test-pskreporter-migration.sh  v1.0
#
# Tests the pskreporter.rx schema migration by:
#   1. Creating pskreporter.rx_new with the optimized schema
#   2. Inserting TEST_ROWS rows from rx into rx_new
#   3. Displaying row N from each table side by side (Vertical format)
#   4. Showing column sizes in both tables
#   5. Projecting full-table savings based on per-row sizes
#
# Usage:
#   ./ch-test-pskreporter-migration.sh [test_rows] [display_row]
#
#   test_rows    rows to insert (default: 1000)
#   display_row  which row to display for comparison (default: 10)
#
# Examples:
#   ./ch-test-pskreporter-migration.sh
#   ./ch-test-pskreporter-migration.sh 10000 50
#
# To clean up afterward:
#   clickhouse-client -u chadmin --password ch2025wd \
#       --query "DROP TABLE IF EXISTS pskreporter.rx_new"

set -euo pipefail

VERSION="1.0"
CH_USER="${CH_USER:-chadmin}"
CH_PASS="${CH_PASS:-ch2025wd}"

TEST_ROWS="${1:-1000}"
DISPLAY_ROW="${2:-10}"          # 1-based
DISPLAY_OFFSET=$(( DISPLAY_ROW - 1 ))

DB="pskreporter"
SRC="rx"
DST="rx_new"
FULL_ROWS=8535495753            # known total rows

CH="clickhouse-client --user ${CH_USER} --password ${CH_PASS}"

# ---------------------------------------------------------------
# helpers
# ---------------------------------------------------------------

ch() { ${CH} --query "$1"; }

table_exists() {
    local n
    n=$(${CH} --query \
        "SELECT count() FROM system.tables \
         WHERE database='${DB}' AND name='${1}' FORMAT TSVRaw" \
        2>/dev/null || echo 0)
    [[ "${n}" == "1" ]]
}

row_count() {
    ${CH} --query \
        "SELECT count() FROM ${DB}.${1} FORMAT TSVRaw" 2>/dev/null || echo 0
}

col_sizes() {
    local tbl="$1"
    ${CH} --query "
        SELECT
            column,
            formatReadableSize(sum(column_data_compressed_bytes))   AS compressed,
            formatReadableSize(sum(column_data_uncompressed_bytes)) AS uncompressed,
            round(sum(column_data_uncompressed_bytes) /
                  sum(column_data_compressed_bytes), 1)             AS ratio
        FROM system.parts_columns
        WHERE database='${DB}' AND table='${tbl}' AND active=1
        GROUP BY column
        ORDER BY sum(column_data_compressed_bytes) DESC
        FORMAT Pretty"
}

total_compressed() {
    ${CH} --query "
        SELECT sum(column_data_compressed_bytes)
        FROM system.parts_columns
        WHERE database='${DB}' AND table='${1}' AND active=1
        FORMAT TSVRaw" 2>/dev/null || echo 0
}

# ---------------------------------------------------------------
# banner
# ---------------------------------------------------------------

echo "============================================================"
echo "  pskreporter.rx migration test  v${VERSION}"
echo "============================================================"
echo "  Test rows    : $(printf "%'d" "${TEST_ROWS}")"
echo "  Display row  : ${DISPLAY_ROW}"
echo "  Full table   : $(printf "%'d" "${FULL_ROWS}") rows"
echo ""

# ---------------------------------------------------------------
# step 1: verify source exists
# ---------------------------------------------------------------

if ! table_exists "${SRC}"; then
    echo "ERROR: ${DB}.${SRC} does not exist"
    exit 1
fi

src_rows=$(row_count "${SRC}")
echo "Source ${DB}.${SRC}: $(printf "%'d" "${src_rows}") rows confirmed"
echo ""

# ---------------------------------------------------------------
# step 2: create rx_new (drop first if exists)
# ---------------------------------------------------------------

echo "------------------------------------------------------------"
echo "Step 1: Create ${DB}.${DST}"
echo "------------------------------------------------------------"

if table_exists "${DST}"; then
    echo "  ${DB}.${DST} already exists — dropping it for a clean test"
    ch "DROP TABLE ${DB}.${DST}"
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

echo "  Created ${DB}.${DST} OK"
echo ""

# ---------------------------------------------------------------
# step 3: insert TEST_ROWS rows
# ---------------------------------------------------------------

echo "------------------------------------------------------------"
echo "Step 2: Insert $(printf "%'d" "${TEST_ROWS}") rows into ${DB}.${DST}"
echo "------------------------------------------------------------"

${CH} --query "
    INSERT INTO ${DB}.${DST}
    SELECT
        time, band, mode,
        rx_sign, rx_lat, rx_lon, rx_loc,
        tx_sign, tx_lat, tx_lon, tx_loc,
        distance, azimuth, rx_azimuth,
        frequency, snr, version
    FROM ${DB}.${SRC}
    LIMIT ${TEST_ROWS}"

dst_rows=$(row_count "${DST}")
echo "  Inserted: $(printf "%'d" "${dst_rows}") rows"
echo ""

# ---------------------------------------------------------------
# step 4: display row N from each table side by side
# ---------------------------------------------------------------

echo "------------------------------------------------------------"
echo "Step 3: Row ${DISPLAY_ROW} comparison (Vertical format)"
echo "------------------------------------------------------------"
echo ""
echo "Strategy: fetch row ${DISPLAY_ROW} from rx_new (small table, fast),"
echo "then look up the same row in rx by (time, tx_sign, rx_sign) match."
echo ""

# Fetch the anchor row from rx_new using its natural ORDER BY (fast - small table)
anchor=$(${CH} --query "
    SELECT
        toString(time), toString(band), mode,
        rx_sign, toString(rx_lat), toString(rx_lon), rx_loc,
        tx_sign, toString(tx_lat), toString(tx_lon), tx_loc,
        toString(distance), toString(azimuth), toString(rx_azimuth),
        toString(frequency), toString(snr), version
    FROM ${DB}.${DST}
    ORDER BY (tx_loc, rx_loc, band, tx_sign, rx_sign, time)
    LIMIT 1 OFFSET ${DISPLAY_OFFSET}
    FORMAT TSVRaw")

IFS=$'\t' read -ra anchor_fields <<< "${anchor}"
ANCHOR_TIME="${anchor_fields[0]}"
ANCHOR_TX="${anchor_fields[7]}"
ANCHOR_RX="${anchor_fields[3]}"

echo "Anchor row: time=${ANCHOR_TIME}  tx_sign=${ANCHOR_TX}  rx_sign=${ANCHOR_RX}"
echo ""

echo ">>> ${DB}.${DST} (OPTIMIZED) — row ${DISPLAY_ROW} <<<"
${CH} --query "
    SELECT *
    FROM ${DB}.${DST}
    ORDER BY (tx_loc, rx_loc, band, tx_sign, rx_sign, time)
    LIMIT 1 OFFSET ${DISPLAY_OFFSET}
    FORMAT Vertical"

echo ""
echo ">>> ${DB}.${SRC} (ORIGINAL) — matching row (by time + tx_sign + rx_sign) <<<"
${CH} --query "
    SELECT *
    FROM ${DB}.${SRC}
    WHERE time = '${ANCHOR_TIME}'
      AND tx_sign = '${ANCHOR_TX}'
      AND rx_sign = '${ANCHOR_RX}'
    LIMIT 1
    FORMAT Vertical"

# ---------------------------------------------------------------
# step 5: field-by-field comparison
# ---------------------------------------------------------------

echo ""
echo "------------------------------------------------------------"
echo "Step 4: Field-by-field match check"
echo "------------------------------------------------------------"

new_row="${anchor}"

orig_row=$(${CH} --query "
    SELECT
        toString(time), toString(band), mode,
        rx_sign, toString(rx_lat), toString(rx_lon), rx_loc,
        tx_sign, toString(tx_lat), toString(tx_lon), tx_loc,
        toString(distance), toString(azimuth), toString(rx_azimuth),
        toString(frequency), toString(snr), version
    FROM ${DB}.${SRC}
    WHERE time = '${ANCHOR_TIME}'
      AND tx_sign = '${ANCHOR_TX}'
      AND rx_sign = '${ANCHOR_RX}'
    LIMIT 1
    FORMAT TSVRaw")

fields=(time band mode rx_sign rx_lat rx_lon rx_loc
        tx_sign tx_lat tx_lon tx_loc
        distance azimuth rx_azimuth frequency snr version)

IFS=$'\t' read -ra orig_fields <<< "${orig_row}"
IFS=$'\t' read -ra new_fields  <<< "${new_row}"

all_match=1
printf "  %-14s  %-30s  %-30s  %s\n" "FIELD" "ORIGINAL" "OPTIMIZED" "MATCH"
printf "  %-14s  %-30s  %-30s  %s\n" "-----" "--------" "---------" "-----"

for i in "${!fields[@]}"; do
    orig_val="${orig_fields[$i]:-<missing>}"
    new_val="${new_fields[$i]:-<missing>}"
    if [[ "${orig_val}" == "${new_val}" ]]; then
        match="✓"
    else
        match="✗ MISMATCH"
        all_match=0
    fi
    printf "  %-14s  %-30s  %-30s  %s\n" \
        "${fields[$i]}" \
        "${orig_val:0:30}" \
        "${new_val:0:30}" \
        "${match}"
done

echo ""
if [[ "${all_match}" == "1" ]]; then
    echo "  ✓ All fields match"
else
    echo "  ✗ Some fields differ (Float32 lat/lon differences are expected)"
fi

# ---------------------------------------------------------------
# step 6: column size comparison + projection
# ---------------------------------------------------------------

echo ""
echo "------------------------------------------------------------"
echo "Step 5: Column sizes — ${DB}.${SRC} (original, full table)"
echo "------------------------------------------------------------"
col_sizes "${SRC}"

echo ""
echo "------------------------------------------------------------"
echo "Step 6: Column sizes — ${DB}.${DST} (optimized, ${TEST_ROWS} rows)"
echo "------------------------------------------------------------"
col_sizes "${DST}"

# ---------------------------------------------------------------
# step 7: project full-table savings
# ---------------------------------------------------------------

echo ""
echo "------------------------------------------------------------"
echo "Step 7: Projected full-table savings"
echo "------------------------------------------------------------"

orig_bytes=$(total_compressed "${SRC}")
new_bytes=$(total_compressed  "${DST}")

if [[ "${new_bytes}" -gt 0 && "${dst_rows}" -gt 0 ]]; then
    # Project new table size if it held all FULL_ROWS rows
    projected_new=$(awk "BEGIN {
        printf \"%.0f\", ${new_bytes} * ${FULL_ROWS} / ${dst_rows}
    }")
    saving=$(( orig_bytes - projected_new ))
    pct=$(awk "BEGIN {printf \"%.1f\", ${saving} * 100 / ${orig_bytes}}")

    orig_gb=$(awk  "BEGIN {printf \"%.1f\", ${orig_bytes}      / 1073741824}")
    proj_gb=$(awk  "BEGIN {printf \"%.1f\", ${projected_new}   / 1073741824}")
    save_gb=$(awk  "BEGIN {printf \"%.1f\", ${saving}          / 1073741824}")

    echo "  Original table size   : ${orig_gb} GB"
    echo "  Projected new size    : ${proj_gb} GB  (extrapolated from ${TEST_ROWS} rows)"
    echo "  Projected saving      : ${save_gb} GB  (${pct}%)"
    echo ""
    echo "  Note: projection improves in accuracy with larger TEST_ROWS."
    echo "  LowCardinality dictionaries are small fixed overhead, so"
    echo "  small test sizes slightly overestimate the new table size."
else
    echo "  (insufficient data for projection)"
fi

echo ""
echo "============================================================"
echo "  Test complete"
echo "============================================================"
echo ""
echo "To clean up:  clickhouse-client -u ${CH_USER} --password ${CH_PASS} \\"
echo "                  --query 'DROP TABLE IF EXISTS ${DB}.${DST}'"
echo ""
echo "To run full migration when ready:"
echo "  ./ch-migrate-pskreporter-rx.sh full"
