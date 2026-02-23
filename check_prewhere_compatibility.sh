#!/bin/bash

# Identify which tables support PREWHERE (views don't, MergeTree tables do)
# Run on WD1 or WD20

CH_USER="${CH_USER:-default}"
TARGET_DB="${1:-wsprnet}"

echo "======================================================================="
echo "PREWHERE Compatibility Check - Database: ${TARGET_DB}"
echo "======================================================================="
echo ""

echo "Tables and their PREWHERE support:"
echo "-------------------------------------------------------------------"
printf "%-30s %-20s %-15s\n" "Table Name" "Engine" "PREWHERE?"
echo "-------------------------------------------------------------------"

clickhouse-client --user="${CH_USER}" --query="
SELECT 
    name,
    engine,
    CASE 
        WHEN engine LIKE '%MergeTree%' THEN '✓ YES'
        WHEN engine = 'View' THEN '✗ NO (View)'
        WHEN engine = 'MaterializedView' THEN '✗ NO (MatView)'
        ELSE '? Unknown'
    END AS prewhere_support
FROM system.tables 
WHERE database='${TARGET_DB}'
ORDER BY 
    CASE 
        WHEN engine LIKE '%MergeTree%' THEN 1
        WHEN engine = 'MaterializedView' THEN 2
        WHEN engine = 'View' THEN 3
        ELSE 4
    END,
    name
FORMAT TSV
" | while IFS=$'\t' read -r name engine support; do
    printf "%-30s %-20s %-15s\n" "$name" "$engine" "$support"
done

echo ""
echo "======================================================================="
echo "How to handle Views:"
echo "======================================================================="
echo ""
echo "Views don't support PREWHERE. Instead:"
echo ""
echo "1. Use WHERE instead of PREWHERE on views:"
echo "   SELECT * FROM wspr.rx"
echo "   WHERE time BETWEEN '2009-01-01' AND '2009-01-02'"
echo "   AND substring(rx_loc,1,2) = 'QE'"
echo ""
echo "2. OR: Query the underlying base table directly with PREWHERE"
echo ""

# Find what wspr.rx view is based on
echo "-------------------------------------------------------------------"
echo "Checking what wspr.rx view is based on..."
echo "-------------------------------------------------------------------"

VIEW_DEFINITION=$(clickhouse-client --user="${CH_USER}" --query="
    SELECT view_definition 
    FROM system.tables 
    WHERE database='wspr' AND name='rx'
" 2>/dev/null)

if [ -n "${VIEW_DEFINITION}" ]; then
    echo "View definition:"
    echo "${VIEW_DEFINITION}"
    echo ""
    
    # Try to extract base table name
    BASE_TABLE=$(echo "${VIEW_DEFINITION}" | grep -oP 'FROM\s+\K[a-zA-Z0-9_.]+' | head -1)
    
    if [ -n "${BASE_TABLE}" ]; then
        echo "Appears to query from: ${BASE_TABLE}"
        echo ""
        echo "You can query the base table directly with PREWHERE:"
        echo "  SELECT * FROM ${BASE_TABLE}"
        echo "  PREWHERE time BETWEEN '2009-01-01' AND '2009-01-02'"
        echo "  AND substring(rx_loc,1,2) = 'QE'"
    fi
fi

echo ""
echo "======================================================================="
echo "PREWHERE Best Practices:"
echo "======================================================================="
echo ""
echo "✓ DO use PREWHERE on MergeTree tables for filtering"
echo "✓ DO put most selective conditions in PREWHERE"
echo "✓ DO use PREWHERE for columns with good compression"
echo ""
echo "✗ DON'T use PREWHERE on Views (use WHERE)"
echo "✗ DON'T use PREWHERE on Merge tables"
echo "✗ DON'T use PREWHERE on Dictionary tables"
echo ""
echo "======================================================================="
