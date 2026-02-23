#!/bin/bash

# Quick fix for PREWHERE on views
# Run on WD1 or WD20

echo "======================================================================="
echo "PREWHERE Error Fix"
echo "======================================================================="
echo ""
echo "Your query failed because wspr.rx is a VIEW, not a base table."
echo "Views don't support PREWHERE."
echo ""
echo "======================================================================="
echo "Solution 1: Use WHERE instead of PREWHERE"
echo "======================================================================="
echo ""
echo "Change this:"
echo "  SELECT * FROM wspr.rx"
echo "  PREWHERE time BETWEEN '2009-01-01' AND '2009-01-02'"
echo "  AND substring(rx_loc,1,2) = 'QE'"
echo ""
echo "To this:"
echo "  SELECT * FROM wspr.rx"
echo "  WHERE time BETWEEN '2009-01-01' AND '2009-01-02'"
echo "  AND substring(rx_loc,1,2) = 'QE'"
echo ""
echo "======================================================================="
echo "Solution 2: Query the underlying base table"
echo "======================================================================="
echo ""

# Find the base table
echo "Finding what table wspr.rx is based on..."

clickhouse-client --query="
    SELECT 'Base table:', as_select 
    FROM system.tables 
    WHERE database='wspr' AND name='rx'
    FORMAT Vertical
" 2>/dev/null

echo ""
echo "Once you know the base table name, you can query it directly with PREWHERE"
echo ""
echo "======================================================================="
echo "Why this matters:"
echo "======================================================================="
echo ""
echo "PREWHERE is a ClickHouse optimization that only works on MergeTree tables."
echo "It filters rows BEFORE reading all columns, which is much faster."
echo ""
echo "Views are just saved SELECT queries - they don't store data themselves."
echo "So PREWHERE doesn't apply to them."
echo ""
echo "Use WHERE for views, PREWHERE for MergeTree base tables."
echo "======================================================================="
