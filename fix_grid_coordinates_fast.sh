#!/bin/bash
# Fast Maidenhead Grid Coordinate Fix
# Uses INSERT SELECT to recreate tables with corrected coordinates
# Much faster than ALTER TABLE UPDATE (millions of rows/sec vs hundreds/sec)

set -e

# Source ClickHouse credentials
if [[ -f /etc/wsprdaemon/clickhouse.conf ]]; then
    source /etc/wsprdaemon/clickhouse.conf
else
    echo "ERROR: /etc/wsprdaemon/clickhouse.conf not found" >&2
    exit 1
fi

# Use chadmin (root admin user) for permissions
CH_USER="${CLICKHOUSE_ROOT_ADMIN_USER}"
CH_PASS="${CLICKHOUSE_ROOT_ADMIN_PASSWORD}"

if [[ -z "$CH_USER" || -z "$CH_PASS" ]]; then
    echo "ERROR: CLICKHOUSE_ROOT_ADMIN_USER or CLICKHOUSE_ROOT_ADMIN_PASSWORD not set" >&2
    exit 1
fi

echo "========================================================================"
echo "Fast Maidenhead Grid Coordinate Fix"
echo "Using ClickHouse user: $CH_USER"
echo "Method: INSERT SELECT with inline coordinate recalculation"
echo "========================================================================"
echo ""

# Function to fix a table
fix_table() {
    local DATABASE=$1
    local TABLE=$2
    
    echo "========================================================================"
    echo "Processing: ${DATABASE}.${TABLE}"
    echo "========================================================================"
    
    # Get row count
    echo "Counting rows..."
    ROW_COUNT=$(clickhouse-client --user="$CH_USER" --password="$CH_PASS" \
        --query "SELECT formatReadableQuantity(count()) FROM ${DATABASE}.${TABLE}")
    echo "Total rows: $ROW_COUNT"
    echo ""
    
    echo "Creating new table with corrected coordinates..."
    echo "This will take a few minutes..."
    echo ""
    
    # Different approach for wsprdaemon vs wsprnet tables
    if [[ "$TABLE" == "spots_extended" ]]; then
        # wsprdaemon.spots_extended has many extra columns
        # Use a two-step process: copy structure, then insert with corrections
        time clickhouse-client --user="$CH_USER" --password="$CH_PASS" --multiquery << EOF
-- Create new table structure
CREATE TABLE ${DATABASE}.${TABLE}_fixed AS ${DATABASE}.${TABLE};

-- Copy all data with corrected coordinates
INSERT INTO ${DATABASE}.${TABLE}_fixed
SELECT 
    * REPLACE (
        -- Recalculate rx_lat from rx_loc
        CASE 
            WHEN length(rx_loc) >= 6 AND toInt32OrZero(substring(rx_loc, 4, 1)) >= 0 THEN
                round((ascii(substring(rx_loc, 2, 1)) - 65) * 10 - 90 + 
                toInt32OrZero(substring(rx_loc, 4, 1)) +
                (ascii(lower(substring(rx_loc, 6, 1))) - 97) * (1.0/24.0) +
                (0.5/24.0), 3)
            WHEN length(rx_loc) >= 4 AND toInt32OrZero(substring(rx_loc, 4, 1)) >= 0 THEN
                round((ascii(substring(rx_loc, 2, 1)) - 65) * 10 - 90 + 
                toInt32OrZero(substring(rx_loc, 4, 1)) +
                11 * (1.0/24.0) + (0.5/24.0), 3)
            ELSE rx_lat
        END AS rx_lat,
        -- Recalculate rx_lon from rx_loc
        CASE 
            WHEN length(rx_loc) >= 6 AND toInt32OrZero(substring(rx_loc, 3, 1)) >= 0 THEN
                round((ascii(substring(rx_loc, 1, 1)) - 65) * 20 - 180 + 
                toInt32OrZero(substring(rx_loc, 3, 1)) * 2 +
                (ascii(lower(substring(rx_loc, 5, 1))) - 97) * (2.0/24.0) +
                (1.0/24.0), 3)
            WHEN length(rx_loc) >= 4 AND toInt32OrZero(substring(rx_loc, 3, 1)) >= 0 THEN
                round((ascii(substring(rx_loc, 1, 1)) - 65) * 20 - 180 + 
                toInt32OrZero(substring(rx_loc, 3, 1)) * 2 +
                11 * (2.0/24.0) + (1.0/24.0), 3)
            ELSE rx_lon
        END AS rx_lon,
        -- Recalculate tx_lat from tx_loc
        CASE 
            WHEN length(tx_loc) >= 6 AND toInt32OrZero(substring(tx_loc, 4, 1)) >= 0 THEN
                round((ascii(substring(tx_loc, 2, 1)) - 65) * 10 - 90 + 
                toInt32OrZero(substring(tx_loc, 4, 1)) +
                (ascii(lower(substring(tx_loc, 6, 1))) - 97) * (1.0/24.0) +
                (0.5/24.0), 3)
            WHEN length(tx_loc) >= 4 AND toInt32OrZero(substring(tx_loc, 4, 1)) >= 0 THEN
                round((ascii(substring(tx_loc, 2, 1)) - 65) * 10 - 90 + 
                toInt32OrZero(substring(tx_loc, 4, 1)) +
                11 * (1.0/24.0) + (0.5/24.0), 3)
            ELSE tx_lat
        END AS tx_lat,
        -- Recalculate tx_lon from tx_loc
        CASE 
            WHEN length(tx_loc) >= 6 AND toInt32OrZero(substring(tx_loc, 3, 1)) >= 0 THEN
                round((ascii(substring(tx_loc, 1, 1)) - 65) * 20 - 180 + 
                toInt32OrZero(substring(tx_loc, 3, 1)) * 2 +
                (ascii(lower(substring(tx_loc, 5, 1))) - 97) * (2.0/24.0) +
                (1.0/24.0), 3)
            WHEN length(tx_loc) >= 4 AND toInt32OrZero(substring(tx_loc, 3, 1)) >= 0 THEN
                round((ascii(substring(tx_loc, 1, 1)) - 65) * 20 - 180 + 
                toInt32OrZero(substring(tx_loc, 3, 1)) * 2 +
                11 * (2.0/24.0) + (1.0/24.0), 3)
            ELSE tx_lon
        END AS tx_lon
    )
FROM ${DATABASE}.${TABLE}
SETTINGS max_insert_threads = 8;
EOF
    else
        # wsprnet.spots has the standard columns
        time clickhouse-client --user="$CH_USER" --password="$CH_PASS" --multiquery << EOF
-- Create new table structure
CREATE TABLE ${DATABASE}.${TABLE}_fixed AS ${DATABASE}.${TABLE};

-- Copy all data with corrected coordinates
INSERT INTO ${DATABASE}.${TABLE}_fixed
SELECT 
    * REPLACE (
        -- Recalculate rx_lat from rx_loc
        CASE 
            WHEN length(rx_loc) >= 6 AND toInt32OrZero(substring(rx_loc, 4, 1)) >= 0 THEN
                round((ascii(substring(rx_loc, 2, 1)) - 65) * 10 - 90 + 
                toInt32OrZero(substring(rx_loc, 4, 1)) +
                (ascii(lower(substring(rx_loc, 6, 1))) - 97) * (1.0/24.0) +
                (0.5/24.0), 3)
            WHEN length(rx_loc) >= 4 AND toInt32OrZero(substring(rx_loc, 4, 1)) >= 0 THEN
                round((ascii(substring(rx_loc, 2, 1)) - 65) * 10 - 90 + 
                toInt32OrZero(substring(rx_loc, 4, 1)) +
                11 * (1.0/24.0) + (0.5/24.0), 3)
            ELSE rx_lat
        END AS rx_lat,
        -- Recalculate rx_lon from rx_loc
        CASE 
            WHEN length(rx_loc) >= 6 AND toInt32OrZero(substring(rx_loc, 3, 1)) >= 0 THEN
                round((ascii(substring(rx_loc, 1, 1)) - 65) * 20 - 180 + 
                toInt32OrZero(substring(rx_loc, 3, 1)) * 2 +
                (ascii(lower(substring(rx_loc, 5, 1))) - 97) * (2.0/24.0) +
                (1.0/24.0), 3)
            WHEN length(rx_loc) >= 4 AND toInt32OrZero(substring(rx_loc, 3, 1)) >= 0 THEN
                round((ascii(substring(rx_loc, 1, 1)) - 65) * 20 - 180 + 
                toInt32OrZero(substring(rx_loc, 3, 1)) * 2 +
                11 * (2.0/24.0) + (1.0/24.0), 3)
            ELSE rx_lon
        END AS rx_lon,
        -- Recalculate tx_lat from tx_loc
        CASE 
            WHEN length(tx_loc) >= 6 AND toInt32OrZero(substring(tx_loc, 4, 1)) >= 0 THEN
                round((ascii(substring(tx_loc, 2, 1)) - 65) * 10 - 90 + 
                toInt32OrZero(substring(tx_loc, 4, 1)) +
                (ascii(lower(substring(tx_loc, 6, 1))) - 97) * (1.0/24.0) +
                (0.5/24.0), 3)
            WHEN length(tx_loc) >= 4 AND toInt32OrZero(substring(tx_loc, 4, 1)) >= 0 THEN
                round((ascii(substring(tx_loc, 2, 1)) - 65) * 10 - 90 + 
                toInt32OrZero(substring(tx_loc, 4, 1)) +
                11 * (1.0/24.0) + (0.5/24.0), 3)
            ELSE tx_lat
        END AS tx_lat,
        -- Recalculate tx_lon from tx_loc
        CASE 
            WHEN length(tx_loc) >= 6 AND toInt32OrZero(substring(tx_loc, 3, 1)) >= 0 THEN
                round((ascii(substring(tx_loc, 1, 1)) - 65) * 20 - 180 + 
                toInt32OrZero(substring(tx_loc, 3, 1)) * 2 +
                (ascii(lower(substring(tx_loc, 5, 1))) - 97) * (2.0/24.0) +
                (1.0/24.0), 3)
            WHEN length(tx_loc) >= 4 AND toInt32OrZero(substring(tx_loc, 3, 1)) >= 0 THEN
                round((ascii(substring(tx_loc, 1, 1)) - 65) * 20 - 180 + 
                toInt32OrZero(substring(tx_loc, 3, 1)) * 2 +
                11 * (2.0/24.0) + (1.0/24.0), 3)
            ELSE tx_lon
        END AS tx_lon
    )
FROM ${DATABASE}.${TABLE}
SETTINGS max_insert_threads = 8;
EOF
    fi
    
    if [[ $? -ne 0 ]]; then
        echo "ERROR: Failed to create fixed table" >&2
        return 1
    fi
    
    echo ""
    echo "Swapping tables (old table will be kept as ${TABLE}_old)..."
    clickhouse-client --user="$CH_USER" --password="$CH_PASS" --multiquery << EOF
RENAME TABLE 
    ${DATABASE}.${TABLE} TO ${DATABASE}.${TABLE}_old,
    ${DATABASE}.${TABLE}_fixed TO ${DATABASE}.${TABLE};
EOF
    
    if [[ $? -ne 0 ]]; then
        echo "ERROR: Failed to swap tables" >&2
        return 1
    fi
    
    echo ""
    echo "✓ Successfully fixed ${DATABASE}.${TABLE}"
    echo "  Old table saved as: ${DATABASE}.${TABLE}_old"
    echo ""
}

# Main execution
START_TIME=$(date +%s)

echo "Starting coordinate fix process..."
echo "Start time: $(date)"
echo ""

# Fix wsprdaemon.spots_extended (smaller table first)
fix_table "wsprdaemon" "spots_extended"

echo ""
echo "========================================================================"
echo ""

# Fix wsprnet.spots (larger table)
fix_table "wsprnet" "spots"

END_TIME=$(date +%s)
ELAPSED=$((END_TIME - START_TIME))
MINUTES=$((ELAPSED / 60))
SECONDS=$((ELAPSED % 60))

echo ""
echo "========================================================================"
echo "ALL TABLES FIXED SUCCESSFULLY!"
echo "========================================================================"
echo "Total time: ${MINUTES}m ${SECONDS}s"
echo "End time: $(date)"
echo ""
echo "Old tables preserved as backup:"
echo "  - wsprdaemon.spots_extended_old"
echo "  - wsprnet.spots_old"
echo ""
echo "To verify the fix, run:"
echo "  clickhouse-client --query \"SELECT rx_loc, rx_lat, rx_lon FROM wsprnet.spots WHERE rx_loc IN ('JO31', 'FN42qc') LIMIT 5\""
echo ""
echo "To drop the old tables and free up space (AFTER VERIFICATION):"
echo "  clickhouse-client --query 'DROP TABLE wsprdaemon.spots_extended_old'"
echo "  clickhouse-client --query 'DROP TABLE wsprnet.spots_old'"
echo ""
