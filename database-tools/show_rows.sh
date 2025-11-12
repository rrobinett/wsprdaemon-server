#!/bin/bash
# show_rows.sh - Display row counts for WSPR tables on WD1 and WD2

echo "=============================================="
echo ">>> Row counts for wsprdaemon and wsprnet tables"
echo "=============================================="

for host in WD1 WD2; do
    echo "--- $host ---"
    if [[ "$host" == "$(hostname -s)" ]]; then
        # Local queries (no --host needed)
        echo -n "wsprdaemon.spots_extended: "
        clickhouse-client --user chadmin --query "SELECT formatReadableQuantity(count()) FROM wsprdaemon.spots_extended" 2>/dev/null || echo "ERROR"
        
        echo -n "wsprdaemon.noise:          "
        clickhouse-client --user chadmin --query "SELECT formatReadableQuantity(count()) FROM wsprdaemon.noise" 2>/dev/null || echo "ERROR"
        
        echo -n "wsprnet.spots:             "
        clickhouse-client --user chadmin --query "SELECT formatReadableQuantity(count()) FROM wsprnet.spots" 2>/dev/null || echo "ERROR"
    else
        # Remote queries via SSH
        echo -n "wsprdaemon.spots_extended: "
        ssh "$host" "clickhouse-client --user chadmin --query 'SELECT formatReadableQuantity(count()) FROM wsprdaemon.spots_extended'" 2>/dev/null || echo "ERROR"
        
        echo -n "wsprdaemon.noise:          "
        ssh "$host" "clickhouse-client --user chadmin --query 'SELECT formatReadableQuantity(count()) FROM wsprdaemon.noise'" 2>/dev/null || echo "ERROR"
        
        echo -n "wsprnet.spots:             "
        ssh "$host" "clickhouse-client --user chadmin --query 'SELECT formatReadableQuantity(count()) FROM wsprnet.spots'" 2>/dev/null || echo "ERROR"
    fi
done

echo "=============================================="
