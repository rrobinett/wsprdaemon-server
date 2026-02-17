#!/bin/bash
# Wrapper script to fix Maidenhead grid coordinates in ClickHouse tables
# Sources credentials from /etc/wsprdaemon/clickhouse.conf

set -e

# Source ClickHouse credentials
if [[ -f /etc/wsprdaemon/clickhouse.conf ]]; then
    source /etc/wsprdaemon/clickhouse.conf
else
    echo "ERROR: /etc/wsprdaemon/clickhouse.conf not found" >&2
    exit 1
fi

# Determine Python path
if [[ -f /opt/wsprdaemon-server/venv/bin/python3 ]]; then
    PYTHON="/opt/wsprdaemon-server/venv/bin/python3"
elif command -v python3 &> /dev/null; then
    PYTHON="python3"
else
    echo "ERROR: python3 not found" >&2
    exit 1
fi

# Script location
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
SCRIPT="${SCRIPT_DIR}/fix_grid_coordinates.py"

if [[ ! -f "$SCRIPT" ]]; then
    echo "ERROR: fix_grid_coordinates.py not found at $SCRIPT" >&2
    exit 1
fi

# Default options
DRY_RUN=""
VERBOSE=""
BATCH_SIZE="10000"
LIMIT=""
SKIP_WSPRNET=""
SKIP_WSPRDAEMON=""

# Parse arguments
while [[ $# -gt 0 ]]; do
    case $1 in
        --dry-run)
            DRY_RUN="--dry-run"
            shift
            ;;
        --verbose)
            VERBOSE="--verbose"
            shift
            ;;
        --batch-size)
            BATCH_SIZE="$2"
            shift 2
            ;;
        --limit)
            LIMIT="--limit $2"
            shift 2
            ;;
        --skip-wsprnet)
            SKIP_WSPRNET="--skip-wsprnet"
            shift
            ;;
        --skip-wsprdaemon)
            SKIP_WSPRDAEMON="--skip-wsprdaemon"
            shift
            ;;
        -h|--help)
            cat << EOF
Fix Maidenhead Grid Coordinates in ClickHouse Tables

Usage: $0 [OPTIONS]

Options:
  --dry-run              Show what would be updated without making changes
  --verbose              Enable verbose logging
  --batch-size N         Rows per batch (default: 10000)
  --limit N              Limit total rows to process (for testing)
  --skip-wsprnet         Skip wsprnet.spots table
  --skip-wsprdaemon      Skip wsprdaemon.spots_extended table
  -h, --help             Show this help

Examples:
  # Dry run to see what would be changed
  $0 --dry-run

  # Test with limited rows
  $0 --dry-run --limit 1000

  # Actually fix the coordinates
  $0

  # Fix only wsprdaemon table
  $0 --skip-wsprnet

Credentials are loaded from /etc/wsprdaemon/clickhouse.conf
EOF
            exit 0
            ;;
        *)
            echo "Unknown option: $1" >&2
            echo "Use --help for usage information" >&2
            exit 1
            ;;
    esac
done

# Run the Python script
exec "$PYTHON" "$SCRIPT" \
    --clickhouse-host "${CLICKHOUSE_HOST}" \
    --clickhouse-port "${CLICKHOUSE_PORT}" \
    --clickhouse-user "${CLICKHOUSE_ROOT_ADMIN_USER}" \
    --clickhouse-password "${CLICKHOUSE_ROOT_ADMIN_PASSWORD}" \
    --batch-size "${BATCH_SIZE}" \
    ${DRY_RUN} \
    ${VERBOSE} \
    ${LIMIT} \
    ${SKIP_WSPRNET} \
    ${SKIP_WSPRDAEMON}
