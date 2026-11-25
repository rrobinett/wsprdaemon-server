#!/bin/bash
#
# ch-system-log-ttl.sh - apply the system-log TTL to EXISTING system.* tables
# Version: 1.1.0
#
# config.d/wd-system-log-ttl.xml only affects system log tables created after
# it is loaded.  This opt-in helper runs ALTER TABLE ... MODIFY TTL on the
# tables that already exist and lists renamed leftovers (query_log_0, ...).
# It is never run by install-servers.sh.
#
# Usage: ch-system-log-ttl.sh            # print the statements only
#        sudo ch-system-log-ttl.sh --apply   # run them
#
# TTL values are read from the repo template XML (single source of truth).
# The installed drop-in is a filtered copy; if it is absent the template next
# to this script is used.  The ClickHouse password is never put on argv: it
# goes in a 0600 client --config-file under a mktemp dir removed on exit.
# opentelemetry_span_log is skipped (see SKIP_TABLES).

set -euo pipefail

CH_CONF="/etc/wsprdaemon/clickhouse.conf"
SELF_DIR="$(cd "$(dirname "$(readlink -f "${BASH_SOURCE[0]}")")" && pwd)"
XML="$SELF_DIR/clickhouse-config/wd-system-log-ttl.xml"
[[ -f "$XML" ]] || XML="/etc/clickhouse-server/config.d/wd-system-log-ttl.xml"
# ALTER TABLE system.opentelemetry_span_log MODIFY TTL fails on ClickHouse
# 25.11 with Code 44 ILLEGAL_COLUMN ("Cannot add subcolumn attribute.values:
# column with this name already exists"), so never attempt it.
SKIP_TABLES=" opentelemetry_span_log "
APPLY=false

case "${1:-}" in
    "") ;;
    --apply) APPLY=true ;;
    -h|--help) sed -n '2,15p' "$0"; exit 0 ;;
    *) echo "Unknown option: $1" >&2; exit 1 ;;
esac

[[ -f "$XML" ]] || { echo "ERROR: TTL template/drop-in XML not found" >&2; exit 1; }
[[ -f "$CH_CONF" ]] || { echo "ERROR: $CH_CONF not found" >&2; exit 1; }
# shellcheck disable=SC1090
source "$CH_CONF"

umask 077
CH_TMP=$(mktemp -d)
trap 'rm -rf "$CH_TMP"' EXIT
# credentials go to python via its environment, not argv (argv shows in ps)
CH_U="$CLICKHOUSE_ROOT_ADMIN_USER" CH_P="$CLICKHOUSE_ROOT_ADMIN_PASSWORD" \
python3 -I - "$CH_TMP/client.xml" <<'PY'
import os, sys
from xml.sax.saxutils import escape
with open(sys.argv[1], "w") as f:
    f.write("<config><user>%s</user><password>%s</password></config>\n"
            % (escape(os.environ["CH_U"]), escape(os.environ["CH_P"])))
PY
CH=(clickhouse-client --config-file "$CH_TMP/client.xml")

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
    if [[ "$SKIP_TABLES" == *" $table "* ]]; then
        echo "-- skipping system.$table (Code 44 ILLEGAL_COLUMN on ClickHouse 25.11)"
        continue
    fi
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
