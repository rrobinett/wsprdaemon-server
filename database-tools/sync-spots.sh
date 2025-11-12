#!/bin/bash
# sync_gaps_batched.sh - Find and sync ID gaps in batches
# This finds missing IDs on WD1 and syncs them from WD2

BATCH_SIZE=1000000  # Process 1M IDs at a time
WD1_HOST="WD1"
WD2_HOST="WD2"
USER="chadmin"
PASSWORD="chadmin"

echo "=== Finding ID gaps on $WD1_HOST (batched approach) ==="

# Get min and max IDs from WD1
read MIN_ID MAX_ID < <(clickhouse-client --user $USER --password $PASSWORD --host $WD1_HOST --query "
SELECT min(id), max(id) FROM wsprnet.spots FORMAT TabSeparated")

if [ -z "$MIN_ID" ] || [ -z "$MAX_ID" ]; then
    echo "ERROR: Could not get ID range from $WD1_HOST"
    exit 1
fi

echo "ID range on $WD1_HOST: $MIN_ID to $MAX_ID"
echo "Batch size: $BATCH_SIZE"
echo ""

TOTAL_MISSING=0
BATCH_COUNT=0

# Process in batches
for ((start=$MIN_ID; start<=$MAX_ID; start+=$BATCH_SIZE)); do
    end=$((start + BATCH_SIZE - 1))
    if [ $end -gt $MAX_ID ]; then
        end=$MAX_ID
    fi
    
    BATCH_COUNT=$((BATCH_COUNT + 1))
    echo "=== Batch $BATCH_COUNT: IDs $start to $end ==="
    
    # Find missing IDs in this batch
    MISSING=$(clickhouse-client --user $USER --password $PASSWORD --host $WD1_HOST --query "
    WITH missing_ids AS (
        SELECT arrayJoin(range($start, $end + 1)) AS id
        WHERE id NOT IN (SELECT id FROM wsprnet.spots WHERE id BETWEEN $start AND $end)
    )
    SELECT count() FROM missing_ids
    " --format TabSeparated 2>&1)
    
    if [ $? -ne 0 ]; then
        echo "  ERROR checking for missing IDs: $MISSING"
        continue
    fi
    
    if [ "$MISSING" -gt 0 ]; then
        echo "  Found $MISSING missing IDs, syncing from $WD2_HOST..."
        
        RESULT=$(clickhouse-client --user $USER --password $PASSWORD --host $WD1_HOST <<EOF 2>&1
INSERT INTO wsprnet.spots
SELECT wd2.*
FROM remote('$WD2_HOST:9000', 'wsprnet.spots', '$USER', '$PASSWORD') AS wd2
WHERE wd2.id >= $start 
  AND wd2.id <= $end
  AND wd2.id NOT IN (SELECT id FROM wsprnet.spots WHERE id BETWEEN $start AND $end);
EOF
)
        
        if [ $? -eq 0 ]; then
            TOTAL_MISSING=$((TOTAL_MISSING + MISSING))
            echo "  ✓ Synced $MISSING rows"
        else
            echo "  ✗ ERROR syncing: $RESULT"
        fi
    else
        echo "  ✓ No gaps in this batch"
    fi
    
    # Small delay to avoid overwhelming the database
    sleep 1
done

echo ""
echo "=== Summary ==="
echo "Processed $BATCH_COUNT batches"
echo "Total missing rows synced: $TOTAL_MISSING"
echo ""

# Show final statistics
echo "=== Final verification ==="
clickhouse-client --user $USER --password $PASSWORD --host $WD1_HOST <<'EOF'
SELECT 
    count() as total_rows,
    min(id) as min_id,
    max(id) as max_id,
    max(id) - min(id) + 1 as expected,
    (max(id) - min(id) + 1) - count() as still_missing,
    round(((max(id) - min(id) + 1) - count()) * 100.0 / (max(id) - min(id) + 1), 2) as missing_pct
FROM wsprnet.spots
FORMAT Vertical;
EOF
