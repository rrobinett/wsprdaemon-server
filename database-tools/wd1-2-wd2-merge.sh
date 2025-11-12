#!/bin/bash
# merge_6months.sh - Merge 6 months one month at a time

echo "=== Merging last 6 months from WD1 to WD2 ==="

for month in {5..0}; do
    start_month=$((month + 1))
    end_month=$month
    
    echo ""
    echo "=== Processing month -$start_month to -$end_month ==="
    
    clickhouse-client --user chadmin --password chadmin <<EOF
INSERT INTO wsprnet.spots
SELECT wd1.*
FROM remote('wd1:9000', 'wsprnet.spots', 'chadmin', 'chadmin') AS wd1
LEFT ANTI JOIN wsprnet.spots AS wd2 ON wd1.id = wd2.id
WHERE wd1.time >= now() - INTERVAL ${start_month} MONTH
  AND wd1.time < now() - INTERVAL ${end_month} MONTH;

SELECT 'Completed month -${start_month} to -${end_month}' as status FORMAT Pretty;
EOF
    
    if [ $? -eq 0 ]; then
        echo "✓ Month -$start_month to -$end_month completed successfully"
    else
        echo "✗ Month -$start_month to -$end_month failed"
    fi
    
    sleep 2
done

echo ""
echo "=== Final statistics ==="
clickhouse-client --user chadmin --password chadmin <<'EOF'
SELECT 
    count() as total_rows,
    min(id) as min_id,
    max(id) as max_id,
    max(id) - min(id) + 1 as expected,
    (max(id) - min(id) + 1) - count() as missing,
    round(((max(id) - min(id) + 1) - count()) * 100.0 / (max(id) - min(id) + 1), 2) as missing_pct
FROM wsprnet.spots 
WHERE time >= now() - INTERVAL 6 MONTH
FORMAT Vertical;
EOF

echo ""
echo "=== Merge complete ==="
