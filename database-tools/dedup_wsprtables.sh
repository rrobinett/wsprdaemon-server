#!/bin/bash
set -euo pipefail

SERVICE="wsprdaemon_server@wsprdaemon.service"
DB="wsprdaemon"
CH="clickhouse-client --user=chadmin --database=$DB --multiline"

echo ">>> Stopping $SERVICE..."
sudo systemctl stop "$SERVICE"

dedup_table() {
    local tbl="$1"
    local tmp="${tbl}_dedup"
    
    echo
    echo "=============================================="
    echo ">>> Deduplicating table: $tbl"
    echo "=============================================="
    
    # Count original rows
    local count_orig
    count_orig="$($CH -q "SELECT count() FROM $tbl;")"
    echo "Original rows: $(printf "%'d\n" "$count_orig")"
    
    # Get the exact CREATE TABLE statement and modify it for temp table
    local create_ddl
    create_ddl="$($CH -q "SHOW CREATE TABLE $tbl FORMAT TSVRaw" | sed "s/CREATE TABLE $DB\\.$tbl/CREATE TABLE $DB.$tmp/")"
    
    # Create temporary dedup table with same structure
    echo "Creating temporary deduplicated table..."
    echo "$create_ddl" | $CH -n
    
    # Insert deduplicated rows
    echo "Inserting DISTINCT rows..."
    $CH -q "INSERT INTO $tmp SELECT DISTINCT * FROM $tbl;"
    
    # Count deduplicated rows
    local count_dedup
    count_dedup="$($CH -q "SELECT count() FROM $tmp;")"
    echo "Deduplicated rows: $(printf "%'d\n" "$count_dedup")"
    
    if [[ "$count_dedup" -eq "$count_orig" ]]; then
        echo "✅ No duplicates found — dropping temporary table."
        $CH -q "DROP TABLE $tmp;"
    else
        echo "⚡ Duplicates detected — replacing original table."
        $CH -q "
            DROP TABLE $tbl;
            RENAME TABLE $tmp TO $tbl;
        "
    fi
    
    echo ">>> $tbl deduplication complete."
}

# Deduplicate spots_extended and noise
dedup_table "spots_extended"
dedup_table "noise"

echo
echo ">>> Restarting $SERVICE..."
sudo systemctl start "$SERVICE"
echo ">>> ✅ Deduplication complete for all tables."
