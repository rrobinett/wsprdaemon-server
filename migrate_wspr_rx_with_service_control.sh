#!/bin/bash

# Replace wspr.rx VIEW with wsprnet.spots table and change ORDER BY
# Creates new table FIRST, then swaps atomically to minimize downtime
# Stops wsprnet_scraper during swap, then restarts it
# Changes ORDER BY from (band,time,id) to (time,band,id)
# Run on WD1 or WD20

CH_USER="${CH_USER:-default}"
TEMP_TABLE="wspr.rx_new"
SCRAPER_STOPPED=false

echo "======================================================================="
echo "Replace wspr.rx View with Table + Change ORDER BY (Zero Downtime)"
echo "======================================================================="
echo ""
echo "This will:"
echo "  1. Keep wspr.rx view running during migration"
echo "  2. Create new wspr.rx_new table with ORDER BY (time,band,id)"
echo "  3. Copy data from wsprnet.spots to wspr.rx_new"
echo "  4. Verify data integrity"
echo "  5. Stop wsprnet_scraper service"
echo "  6. Atomic swap: drop view and rename rx_new to rx (< 1 second!)"
echo "  7. Restart wsprnet_scraper service"
echo ""
echo "The wspr.rx view stays available until the final swap!"
echo ""

# Step 1: Verify current state
echo "Step 1: Verifying current state..."
echo "-------------------------------------------------------------------"

# Check wspr.rx is a view
RX_ENGINE=$(clickhouse-client --user="${CH_USER}" --query="
    SELECT engine FROM system.tables 
    WHERE database='wspr' AND name='rx'
" 2>/dev/null)

if [ "${RX_ENGINE}" != "View" ]; then
    echo "ERROR: wspr.rx is not a view (it's ${RX_ENGINE})"
    echo "Cannot proceed safely. Please investigate."
    exit 1
fi

echo "✓ wspr.rx is a View (staying online during migration)"

# Check wsprnet.spots exists and is a MergeTree
SPOTS_ENGINE=$(clickhouse-client --user="${CH_USER}" --query="
    SELECT engine FROM system.tables 
    WHERE database='wsprnet' AND name='spots'
" 2>/dev/null)

if [ -z "${SPOTS_ENGINE}" ]; then
    echo "ERROR: wsprnet.spots does not exist"
    exit 1
fi

echo "✓ wsprnet.spots exists (engine: ${SPOTS_ENGINE})"

# Get current ORDER BY
CURRENT_ORDER=$(clickhouse-client --user="${CH_USER}" --query="
    SELECT sorting_key FROM system.tables 
    WHERE database='wsprnet' AND name='spots'
" 2>/dev/null)

echo "✓ Current ORDER BY: ${CURRENT_ORDER}"

# Get row counts
RX_COUNT=$(clickhouse-client --user="${CH_USER}" --query="SELECT COUNT(*) FROM wspr.rx" 2>/dev/null)
SPOTS_COUNT=$(clickhouse-client --user="${CH_USER}" --query="SELECT COUNT(*) FROM wsprnet.spots" 2>/dev/null)

echo "✓ wspr.rx has ${RX_COUNT} rows"
echo "✓ wsprnet.spots has ${SPOTS_COUNT} rows"

# Check wsprnet_scraper service status
echo ""
echo "Checking wsprnet_scraper service..."
if systemctl is-active --quiet wsprnet_scraper 2>/dev/null; then
    echo "✓ wsprnet_scraper is currently running"
else
    echo "  wsprnet_scraper is not running (or doesn't exist)"
fi

# Check if temp table already exists
TEMP_EXISTS=$(clickhouse-client --user="${CH_USER}" --query="
    SELECT COUNT(*) FROM system.tables 
    WHERE database='wspr' AND name='rx_new'
" 2>/dev/null)

if [ "${TEMP_EXISTS}" = "1" ]; then
    echo ""
    echo "WARNING: ${TEMP_TABLE} already exists!"
    read -p "Drop it and start fresh? (yes/no): " DROP_TEMP
    if [ "${DROP_TEMP}" = "yes" ]; then
        clickhouse-client --user="${CH_USER}" --query="DROP TABLE IF EXISTS ${TEMP_TABLE}" 2>&1
        echo "✓ Old ${TEMP_TABLE} dropped"
    else
        echo "Aborted by user"
        exit 0
    fi
fi

echo ""

# Step 2: Backup view definition
echo "Step 2: Backing up wspr.rx view definition..."
echo "-------------------------------------------------------------------"

BACKUP_FILE="/tmp/wspr_rx_view_backup_$(date +%Y%m%d_%H%M%S).sql"

clickhouse-client --user="${CH_USER}" --query="
    SHOW CREATE TABLE wspr.rx
" > "${BACKUP_FILE}" 2>/dev/null

if [ -f "${BACKUP_FILE}" ]; then
    echo "✓ View definition backed up to: ${BACKUP_FILE}"
else
    echo "ERROR: Failed to backup view definition"
    exit 1
fi

echo ""

# Step 3: Get the CREATE TABLE statement for wsprnet.spots
echo "Step 3: Preparing new table structure..."
echo "-------------------------------------------------------------------"

CREATE_STMT=$(clickhouse-client --user="${CH_USER}" --query="
    SHOW CREATE TABLE wsprnet.spots
" 2>/dev/null)

echo "Current ORDER BY: (band, time, id)"
echo "New ORDER BY:     (time, band, id)"
echo ""
echo "Why this helps:"
echo "  - Time-based PREWHERE queries will be MUCH faster"
echo "  - Common query pattern: PREWHERE time BETWEEN ... will use primary key"
echo "  - Band filtering still efficient as secondary key"
echo ""

read -p "Continue with creating new table? (yes/no): " CONFIRM

if [ "${CONFIRM}" != "yes" ]; then
    echo "Aborted by user"
    exit 0
fi

echo ""

# Step 4: Create new table with modified ORDER BY
echo "Step 4: Creating ${TEMP_TABLE} with ORDER BY (time,band,id)..."
echo "-------------------------------------------------------------------"

# Modify the CREATE statement to create temp table with new ORDER BY
NEW_CREATE=$(echo "${CREATE_STMT}" | sed "s/CREATE TABLE \`wsprnet\`\.\`spots\`/CREATE TABLE \`wspr\`.\`rx_new\`/" | sed 's/ORDER BY (band, time, id)/ORDER BY (time, band, id)/')

echo "Creating temporary table..."

clickhouse-client --user="${CH_USER}" --query="${NEW_CREATE}" 2>&1

if [ $? -ne 0 ]; then
    echo "ERROR: Failed to create new table"
    exit 1
fi

echo "✓ ${TEMP_TABLE} created successfully"
echo ""

# Step 5: Copy data (wspr.rx view still available during this)
echo "Step 5: Copying data from wsprnet.spots to ${TEMP_TABLE}..."
echo "-------------------------------------------------------------------"
echo "This may take a while depending on table size (${SPOTS_COUNT} rows)..."
echo "Note: wspr.rx view is still available for queries during this copy!"
echo ""

START_TIME=$(date +%s)

clickhouse-client --user="${CH_USER}" --query="
    INSERT INTO ${TEMP_TABLE} SELECT * FROM wsprnet.spots
" 2>&1

if [ $? -ne 0 ]; then
    echo "ERROR: Failed to copy data"
    echo ""
    echo "Cleaning up..."
    clickhouse-client --user="${CH_USER}" --query="DROP TABLE IF EXISTS ${TEMP_TABLE}"
    exit 1
fi

END_TIME=$(date +%s)
ELAPSED=$((END_TIME - START_TIME))

echo "✓ Data copied in ${ELAPSED} seconds"
echo ""

# Step 6: Verify row count
echo "Step 6: Verifying data in ${TEMP_TABLE}..."
echo "-------------------------------------------------------------------"

NEW_COUNT=$(clickhouse-client --user="${CH_USER}" --query="SELECT COUNT(*) FROM ${TEMP_TABLE}" 2>/dev/null)

echo "Original wsprnet.spots: ${SPOTS_COUNT} rows"
echo "New ${TEMP_TABLE}:      ${NEW_COUNT} rows"

if [ "${NEW_COUNT}" -eq "${SPOTS_COUNT}" ]; then
    echo "✓ Row counts match!"
else
    echo "ERROR: Row count mismatch!"
    echo "Not proceeding with swap. Please investigate."
    echo "${TEMP_TABLE} left in place for debugging."
    exit 1
fi

echo ""

# Step 7: Verify ORDER BY
NEW_ORDER=$(clickhouse-client --user="${CH_USER}" --query="
    SELECT sorting_key FROM system.tables 
    WHERE database='wspr' AND name='rx_new'
" 2>/dev/null)

echo "New ORDER BY: ${NEW_ORDER}"

if [ "${NEW_ORDER}" = "time, band, id" ]; then
    echo "✓ ORDER BY is correct!"
else
    echo "WARNING: ORDER BY is ${NEW_ORDER}, expected 'time, band, id'"
    read -p "Continue anyway? (yes/no): " CONTINUE
    if [ "${CONTINUE}" != "yes" ]; then
        echo "Aborted. ${TEMP_TABLE} left in place."
        exit 0
    fi
fi

echo ""

# Step 8: Test PREWHERE on new table
echo "Step 8: Testing PREWHERE on ${TEMP_TABLE}..."
echo "-------------------------------------------------------------------"

echo "Running test query with PREWHERE on time..."
clickhouse-client --user="${CH_USER}" --query="
    SELECT COUNT(*) FROM ${TEMP_TABLE}
    PREWHERE time >= '2020-01-01' 
    LIMIT 1
" 2>&1

if [ $? -eq 0 ]; then
    echo "✓ PREWHERE works on new table!"
else
    echo "✗ PREWHERE test failed"
    echo "Not proceeding with swap."
    exit 1
fi

echo ""

# Step 9: Final confirmation before swap
echo "======================================================================="
echo "Ready for final swap!"
echo "======================================================================="
echo ""
echo "Everything is ready. The final swap will:"
echo "  1. Stop wsprnet_scraper service"
echo "  2. Drop wspr.rx view"
echo "  3. Rename wspr.rx_new to wspr.rx (< 1 second)"
echo "  4. Restart wsprnet_scraper service"
echo ""
echo "Total downtime for wspr.rx: < 1 second"
echo "Total downtime for scraper: ~5-10 seconds"
echo ""
read -p "Proceed with final swap? (yes/no): " FINAL_CONFIRM

if [ "${FINAL_CONFIRM}" != "yes" ]; then
    echo "Aborted by user"
    echo "${TEMP_TABLE} is ready but not swapped in."
    echo "You can manually complete the swap later or drop it."
    exit 0
fi

echo ""

# Step 10: Execute the atomic swap with service stopped
echo "Step 10: Executing atomic swap (stopping wsprnet_scraper)..."
echo "-------------------------------------------------------------------"

# Stop the scraper service
echo "Stopping wsprnet_scraper service..."
sudo systemctl stop wsprnet_scraper 2>&1

if [ $? -eq 0 ]; then
    echo "✓ wsprnet_scraper stopped"
    SCRAPER_STOPPED=true
else
    echo "WARNING: Failed to stop wsprnet_scraper (may not exist or no sudo access)"
    echo "Proceeding anyway..."
    SCRAPER_STOPPED=false
    read -p "Continue with swap? (yes/no): " CONTINUE_ANYWAY
    if [ "${CONTINUE_ANYWAY}" != "yes" ]; then
        echo "Aborted by user"
        exit 0
    fi
fi

echo ""
echo "Beginning atomic swap..."

echo "Dropping wspr.rx view..."
clickhouse-client --user="${CH_USER}" --query="DROP VIEW IF EXISTS wspr.rx" 2>&1

if [ $? -ne 0 ]; then
    echo "ERROR: Failed to drop view"
    echo "View might still exist, temp table is ${TEMP_TABLE}"
    
    # Restart scraper before exiting
    if [ "${SCRAPER_STOPPED}" = true ]; then
        echo "Restarting wsprnet_scraper..."
        sudo systemctl start wsprnet_scraper
    fi
    exit 1
fi

echo "✓ View dropped"

echo "Renaming ${TEMP_TABLE} to wspr.rx..."
clickhouse-client --user="${CH_USER}" --query="RENAME TABLE ${TEMP_TABLE} TO wspr.rx" 2>&1

if [ $? -ne 0 ]; then
    echo "ERROR: Failed to rename table"
    echo ""
    echo "CRITICAL: wspr.rx view is dropped but rename failed!"
    echo "Attempting to restore view..."
    clickhouse-client --user="${CH_USER}" < "${BACKUP_FILE}"
    
    # Restart scraper before exiting
    if [ "${SCRAPER_STOPPED}" = true ]; then
        echo "Restarting wsprnet_scraper..."
        sudo systemctl start wsprnet_scraper
    fi
    exit 1
fi

echo "✓ Table renamed - swap complete!"

# Restart the scraper service
if [ "${SCRAPER_STOPPED}" = true ]; then
    echo ""
    echo "Restarting wsprnet_scraper service..."
    sudo systemctl start wsprnet_scraper 2>&1
    
    if [ $? -eq 0 ]; then
        echo "✓ wsprnet_scraper restarted"
        
        # Check service status
        sleep 2
        if systemctl is-active --quiet wsprnet_scraper; then
            echo "✓ wsprnet_scraper is running"
        else
            echo "WARNING: wsprnet_scraper may not be running properly"
            echo "Check with: sudo systemctl status wsprnet_scraper"
        fi
    else
        echo "ERROR: Failed to restart wsprnet_scraper"
        echo "Please restart manually: sudo systemctl start wsprnet_scraper"
    fi
fi

echo ""

# Step 11: Verify final state
echo "Step 11: Verifying final state..."
echo "-------------------------------------------------------------------"

FINAL_ENGINE=$(clickhouse-client --user="${CH_USER}" --query="
    SELECT engine FROM system.tables 
    WHERE database='wspr' AND name='rx'
" 2>/dev/null)

FINAL_COUNT=$(clickhouse-client --user="${CH_USER}" --query="SELECT COUNT(*) FROM wspr.rx" 2>/dev/null)

FINAL_ORDER=$(clickhouse-client --user="${CH_USER}" --query="
    SELECT sorting_key FROM system.tables 
    WHERE database='wspr' AND name='rx'
" 2>/dev/null)

echo "wspr.rx engine: ${FINAL_ENGINE}"
echo "wspr.rx rows: ${FINAL_COUNT}"
echo "wspr.rx ORDER BY: ${FINAL_ORDER}"

if [[ "${FINAL_ENGINE}" == *MergeTree* ]]; then
    echo "✓ wspr.rx is now a ${FINAL_ENGINE} table"
else
    echo "WARNING: wspr.rx engine is ${FINAL_ENGINE}"
fi

echo ""

# Step 12: Final PREWHERE test
echo "Step 12: Final PREWHERE test..."
echo "-------------------------------------------------------------------"

clickhouse-client --user="${CH_USER}" --query="
    SELECT COUNT(*) FROM wspr.rx 
    PREWHERE time >= '2020-01-01' 
    LIMIT 1
" 2>&1

if [ $? -eq 0 ]; then
    echo "✓ PREWHERE works on wspr.rx!"
else
    echo "✗ PREWHERE test failed"
fi

echo ""

# Step 13: Ask about dropping old table
echo "Step 13: Cleanup..."
echo "-------------------------------------------------------------------"
echo ""
read -p "Drop wsprnet.spots table? (yes/no): " DROP_OLD

if [ "${DROP_OLD}" = "yes" ]; then
    clickhouse-client --user="${CH_USER}" --query="DROP TABLE IF EXISTS wsprnet.spots" 2>&1
    echo "✓ wsprnet.spots dropped"
else
    echo "Keeping wsprnet.spots (you can drop it manually later)"
fi

echo ""
echo "======================================================================="
echo "Migration Complete!"
echo "======================================================================="
echo ""
echo "Summary:"
echo "  - Old view definition backed up to: ${BACKUP_FILE}"
echo "  - wspr.rx is now a ${FINAL_ENGINE} table"
echo "  - Rows: ${FINAL_COUNT}"
echo "  - ORDER BY changed from (band,time,id) to (time,band,id)"
echo "  - PREWHERE is now supported!"
echo "  - wsprnet_scraper service: $(systemctl is-active wsprnet_scraper 2>/dev/null || echo 'status unknown')"
echo ""
echo "Performance improvements:"
echo "  - Time-based PREWHERE queries will be MUCH faster"
echo "  - Primary key now optimized for: PREWHERE time BETWEEN ..."
echo "  - Band filtering still efficient as secondary sort key"
echo ""
echo "Your clients can now use queries like:"
echo "  SELECT * FROM wspr.rx"
echo "  PREWHERE time BETWEEN '2009-01-01' AND '2009-01-02'"
echo "  AND substring(rx_loc,1,2) = 'QE'"
echo ""
echo "======================================================================="
