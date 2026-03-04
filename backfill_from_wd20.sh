#!/bin/bash
# backfill_from_wd20.sh
# Run this on wd10 (or adjust hosts as needed)

WD20_HOST="wd20"          # adjust to IP or hostname
WD20_PORT=8123
WD10_HOST="localhost"
WD10_PORT=8123
CH_USER="chadmin"
CH_PASS="ch2025wd"
TABLE="wspr.rx"

# Set these to the outage window - check wd10 logs or query for last/first record
GAP_START="2026-02-25 00:00:00"   # adjust - time wd10 went down
GAP_END="2026-02-25 16:00:00"     # adjust - time wd10 came back up

echo "Querying gap boundaries from wd10..."
LAST_BEFORE=$(clickhouse-client --host $WD10_HOST --port 9000 \
    --user $CH_USER --password $CH_PASS \
    --query "SELECT max(time) FROM ${TABLE} WHERE time < '${GAP_START}'")
FIRST_AFTER=$(clickhouse-client --host $WD10_HOST --port 9000 \
    --user $CH_USER --password $CH_PASS \
    --query "SELECT min(time) FROM ${TABLE} WHERE time > '${GAP_START}'")

echo "wd10 last record before gap: ${LAST_BEFORE}"
echo "wd10 first record after gap: ${FIRST_AFTER}"
echo "Will fill from wd20 between: ${LAST_BEFORE} and ${FIRST_AFTER}"
read -p "Proceed? [y/N] " confirm
[[ "${confirm}" != "y" ]] && exit 0

echo "Counting rows to transfer from wd20..."
COUNT=$(clickhouse-client --host $WD20_HOST --port 9000 \
    --user $CH_USER --password $CH_PASS \
    --query "SELECT count() FROM ${TABLE} WHERE time > '${LAST_BEFORE}' AND time < '${FIRST_AFTER}'")
echo "Rows to transfer: ${COUNT}"

echo "Starting transfer via pipe..."
clickhouse-client --host $WD20_HOST --port 9000 \
    --user $CH_USER --password $CH_PASS \
    --query "SELECT * FROM ${TABLE} WHERE time > '${LAST_BEFORE}' AND time < '${FIRST_AFTER}' FORMAT Native" \
| clickhouse-client --host $WD10_HOST --port 9000 \
    --user $CH_USER --password $CH_PASS \
    --query "INSERT INTO ${TABLE} FORMAT Native"

echo "Done. Verifying..."
COUNT_WD10=$(clickhouse-client --host $WD10_HOST --port 9000 \
    --user $CH_USER --password $CH_PASS \
    --query "SELECT count() FROM ${TABLE} WHERE time > '${LAST_BEFORE}' AND time < '${FIRST_AFTER}'")
echo "wd10 now has ${COUNT_WD10} rows in that window (expected ${COUNT})"
