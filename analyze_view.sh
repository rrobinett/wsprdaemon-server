#!/bin/bash

# Show view definition and find underlying base tables
# Run on WD1 or WD20

CH_USER="${CH_USER:-default}"
VIEW_DB="${1:-wspr}"
VIEW_NAME="${2:-rx}"

echo "======================================================================="
echo "View Analysis: ${VIEW_DB}.${VIEW_NAME}"
echo "======================================================================="
echo ""

# Confirm it's a view
ENGINE=$(clickhouse-client --user="${CH_USER}" --query="
    SELECT engine FROM system.tables 
    WHERE database='${VIEW_DB}' AND name='${VIEW_NAME}'
" 2>/dev/null)

echo "Engine: ${ENGINE}"

if [ "${ENGINE}" != "View" ]; then
    echo "This is not a view!"
    exit 1
fi

echo ""
echo "======================================================================="
echo "View Definition:"
echo "======================================================================="
echo ""

clickhouse-client --user="${CH_USER}" --query="
    SHOW CREATE TABLE ${VIEW_DB}.${VIEW_NAME}
" 2>/dev/null

echo ""
echo "======================================================================="
echo "Underlying Tables:"
echo "======================================================================="
echo ""

# Get the SELECT query from the view
VIEW_SELECT=$(clickhouse-client --user="${CH_USER}" --query="
    SELECT as_select FROM system.tables 
    WHERE database='${VIEW_DB}' AND name='${VIEW_NAME}'
" 2>/dev/null)

echo "View queries from:"
echo "${VIEW_SELECT}" | grep -oP '(FROM|JOIN)\s+\K[a-zA-Z0-9_.]+' | sort -u | while read -r TABLE; do
    echo "  - ${TABLE}"
    
    # Check if this table supports PREWHERE
    if [[ "${TABLE}" == *.* ]]; then
        # Fully qualified name
        DB=$(echo "${TABLE}" | cut -d. -f1)
        TBL=$(echo "${TABLE}" | cut -d. -f2)
    else
        # Same database
        DB="${VIEW_DB}"
        TBL="${TABLE}"
    fi
    
    TABLE_ENGINE=$(clickhouse-client --user="${CH_USER}" --query="
        SELECT engine FROM system.tables 
        WHERE database='${DB}' AND name='${TBL}'
    " 2>/dev/null)
    
    if [[ "${TABLE_ENGINE}" == *MergeTree* ]]; then
        echo "    Engine: ${TABLE_ENGINE} ✓ PREWHERE supported"
    else
        echo "    Engine: ${TABLE_ENGINE} ✗ PREWHERE not supported"
    fi
done

echo ""
echo "======================================================================="
echo "How to Use PREWHERE:"
echo "======================================================================="
echo ""
echo "Option 1: Use WHERE on the view (slower)"
echo "-------------------------------------------------------------------"
echo "SELECT * FROM ${VIEW_DB}.${VIEW_NAME}"
echo "WHERE time BETWEEN '2009-01-01' AND '2009-01-02'"
echo "  AND substring(rx_loc,1,2) = 'QE'"
echo ""
echo "Option 2: Query base table directly with PREWHERE (faster)"
echo "-------------------------------------------------------------------"

# Extract first base table
FIRST_TABLE=$(echo "${VIEW_SELECT}" | grep -oP 'FROM\s+\K[a-zA-Z0-9_.]+' | head -1)

if [ -n "${FIRST_TABLE}" ]; then
    echo "SELECT * FROM ${FIRST_TABLE}"
    echo "PREWHERE time BETWEEN '2009-01-01' AND '2009-01-02'"
    echo "  AND substring(rx_loc,1,2) = 'QE'"
    echo ""
    echo "Note: Make sure ${FIRST_TABLE} has the same columns as the view!"
fi

echo "======================================================================="
