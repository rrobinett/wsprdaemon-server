#!/bin/bash
#
# ch-system-log-ttl.sh - apply the system-log TTL to EXISTING system.* tables
# Version: 1.0.0
#
# config.d/wd-system-log-ttl.xml only affects system log tables created after
# it is loaded.  This opt-in helper runs ALTER TABLE ... MODIFY TTL on the
# tables that already exist and lists renamed leftovers (query_log_0, ...).
# It is never run by install-servers.sh.
#
# Usage: ch-system-log-ttl.sh            # print the statements only
#        sudo ch-system-log-ttl.sh --apply   # run them
#
# TTL values are read from the drop-in XML so there is one source of truth.

set -euo pipefail

CH_CONF="/etc/wsprdaemon/clickhouse.conf"
XML="/etc/clickhouse-server/config.d/wd-system-log-ttl.xml"
APPLY=false

case "${1:-}" in
    "") ;;
    --apply) APPLY=true ;;
    -h|--help) sed -n '2,15p' "$0"; exit 0 ;;
    *) echo "Unknown option: $1" >&2; exit 1 ;;
esac

[[ -f "$XML" ]] || { echo "ERROR: $XML not found - run install-servers.sh first" >&2; exit 1; }
[[ -f "$CH_CONF" ]] || { echo "ERROR: $CH_CONF not found" >&2; exit 1; }
# shellcheck disable=SC1090
source "$CH_CONF"
CH=(clickhouse-client --user "$CLICKHOUSE_ROOT_ADMIN_USER" --password "$CLICKHOUSE_ROOT_ADMIN_PASSWORD")

existing=$("${CH[@]}" --query "SELECT name FROM system.tables WHERE database='system'")

# "table<TAB>ttl expression" per element that has a <ttl>
specs=$(python3 -I - "$XML" <<'PY'
import sys, xml.etree.ElementTree as ET
for el in ET.parse(sys.argv[1]).getroot():
    t = el.find("ttl")
    if t is not None and t.text:
        print(f"{el.tag}\t{t.text.strip()}")
PY
)

echo "-- Existing system log tables: MODIFY TTL"
while IFS=$'\t' read -r table ttl; do
    grep -qx "$table" <<< "$existing" || continue
    stmt="ALTER TABLE system.$table MODIFY TTL $ttl"
    if $APPLY; then
        echo "applying: $stmt"
        "${CH[@]}" --query "$stmt"
    else
        echo "$stmt;"
    fi
done <<< "$specs"

echo ""
echo "-- Renamed leftovers (safe to DROP once confirmed not needed):"
left=$("${CH[@]}" --query "SELECT name, formatReadableSize(sum(bytes_on_disk)) FROM system.tables t LEFT JOIN system.parts p ON p.database=t.database AND p.table=t.name AND p.active WHERE t.database='system' AND match(t.name, '_[0-9]+\$') GROUP BY name ORDER BY name" --format TSV)
if [[ -z "$left" ]]; then
    echo "(none)"
else
    while IFS=$'\t' read -r name size; do
        echo "DROP TABLE system.$name;   -- $size"
    done <<< "$left"
fi
$APPLY || { echo ""; echo "(dry run - re-run with --apply to execute the ALTERs; DROPs are never run)"; }
