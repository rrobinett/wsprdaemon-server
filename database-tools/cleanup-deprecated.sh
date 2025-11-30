#!/bin/bash
#
# cleanup-deprecated.sh - Remove deprecated sync scripts
#
# This removes old/deprecated scripts that have been replaced by better versions.

set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

echo "=== Cleanup Deprecated Database Tools ==="
echo ""
echo "This will remove the following deprecated scripts:"
echo "  - sync-spots.sh           (replaced by sync_wsprnet_spots.sh)"
echo "  - wd1-2-wd2-merge.sh      (replaced by sync_wsprnet_spots.sh)"
echo "  - ch-flush.sh             (renamed to clickhouse-complete-uninstall.sh)"
echo "  - show-both-tables.sh     (debugging script, rarely used)"
echo "  - sync-wd2-users.sh       (old version)"
echo "  - sync-wd2-users-v2.sh    (user sync, not table sync)"
echo ""

read -p "Remove these deprecated files? [y/N] " confirm
if [[ ! "$confirm" =~ ^[Yy]$ ]]; then
    echo "Aborted."
    exit 0
fi

cd "$SCRIPT_DIR"

# Remove deprecated scripts
for file in sync-spots.sh wd1-2-wd2-merge.sh ch-flush.sh show-both-tables.sh sync-wd2-users.sh sync-wd2-users-v2.sh; do
    if [[ -f "$file" ]]; then
        echo "Removing $file..."
        rm -f "$file"
    fi
done

echo ""
echo "✓ Cleanup complete!"
echo ""
echo "Remaining scripts:"
echo "  Core sync scripts:"
echo "    - sync-all.sh                      (master wrapper)"
echo "    - sync_wsprnet_spots.sh           (wsprnet.spots sync)"
echo "    - sync_wsprdaemon_tables.sh       (wsprdaemon tables sync)"
echo "  Utilities:"
echo "    - show_rows.sh                    (display row counts)"
echo "    - dedup_wsprtables.sh             (standalone deduplication)"
echo "  Dangerous:"
echo "    - clickhouse-complete-uninstall.sh (complete CH removal)"
echo ""
