#!/bin/bash
#
# sync-all.sh - Master wrapper to sync all ClickHouse tables between WD1 and WD2
# Run as user wsprdaemon on either WD1 or WD2
#
# This script runs both:
#   1. sync_wsprnet_spots.sh - for wsprnet.spots table
#   2. sync_wsprdaemon_tables.sh - for wsprdaemon.spots_extended and wsprdaemon.noise tables

set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

log_info() {
    echo -e "${GREEN}[INFO]${NC} $1"
}

log_warn() {
    echo -e "${YELLOW}[WARN]${NC} $1"
}

log_error() {
    echo -e "${RED}[ERROR]${NC} $1"
}

log_section() {
    echo -e "${BLUE}╔═══════════════════════════════════════════════════════════════╗${NC}"
    echo -e "${BLUE}║${NC} $1"
    echo -e "${BLUE}╚═══════════════════════════════════════════════════════════════╝${NC}"
}

# Usage function
usage() {
    cat << EOF
Usage: $0 [OPTIONS]

Master script to sync all ClickHouse tables between WD1 and WD2.

This runs:
  1. sync_wsprnet_spots.sh     - Syncs wsprnet.spots
  2. sync_wsprdaemon_tables.sh - Syncs wsprdaemon.spots_extended and wsprdaemon.noise

OPTIONS:
    --wsprnet-only      Only sync wsprnet.spots table
    --wsprdaemon-only   Only sync wsprdaemon tables
    -h, --help          Show this help message

EXAMPLES:
    $0                      # Sync all tables (default)
    $0 --wsprnet-only       # Only sync wsprnet.spots
    $0 --wsprdaemon-only    # Only sync wsprdaemon tables

EOF
    exit 0
}

# Parse arguments
DO_WSPRNET=true
DO_WSPRDAEMON=true

if [[ "$1" == "--help" || "$1" == "-h" ]]; then
    usage
fi

if [[ "$1" == "--wsprnet-only" ]]; then
    DO_WSPRDAEMON=false
elif [[ "$1" == "--wsprdaemon-only" ]]; then
    DO_WSPRNET=false
fi

# Determine which server we're on
CURRENT_HOST=$(hostname -s)
if [[ "$CURRENT_HOST" != "WD1" && "$CURRENT_HOST" != "WD2" ]]; then
    log_error "Unknown host: $CURRENT_HOST. Must be run on WD1 or WD2"
    exit 1
fi

log_section "Master Sync Started on $CURRENT_HOST"
START_TIME=$(date +%s)

# Track success/failure
WSPRNET_STATUS="skipped"
WSPRDAEMON_STATUS="skipped"

# Sync wsprnet.spots
if [[ "$DO_WSPRNET" == "true" ]]; then
    echo ""
    log_section "Step 1/2: Syncing wsprnet.spots"
    echo ""
    
    if [[ -x "$SCRIPT_DIR/sync_wsprnet_spots.sh" ]]; then
        if bash "$SCRIPT_DIR/sync_wsprnet_spots.sh"; then
            WSPRNET_STATUS="✓ success"
        else
            WSPRNET_STATUS="✗ FAILED"
            log_error "wsprnet sync failed!"
        fi
    else
        log_error "sync_wsprnet_spots.sh not found or not executable"
        WSPRNET_STATUS="✗ missing"
    fi
fi

# Sync wsprdaemon tables
if [[ "$DO_WSPRDAEMON" == "true" ]]; then
    echo ""
    log_section "Step 2/2: Syncing wsprdaemon tables"
    echo ""
    
    if [[ -x "$SCRIPT_DIR/sync_wsprdaemon_tables.sh" ]]; then
        if bash "$SCRIPT_DIR/sync_wsprdaemon_tables.sh" both; then
            WSPRDAEMON_STATUS="✓ success"
        else
            WSPRDAEMON_STATUS="✗ FAILED"
            log_error "wsprdaemon sync failed!"
        fi
    else
        log_error "sync_wsprdaemon_tables.sh not found or not executable"
        WSPRDAEMON_STATUS="✗ missing"
    fi
fi

# Final summary
END_TIME=$(date +%s)
DURATION=$((END_TIME - START_TIME))

echo ""
log_section "Sync Complete - Summary"
echo ""
echo "  wsprnet.spots sync:       $WSPRNET_STATUS"
echo "  wsprdaemon tables sync:   $WSPRDAEMON_STATUS"
echo ""
echo "  Duration: ${DURATION}s"
echo ""

# Exit with error if any sync failed
if [[ "$WSPRNET_STATUS" == *"FAILED"* ]] || [[ "$WSPRDAEMON_STATUS" == *"FAILED"* ]]; then
    log_error "One or more syncs failed!"
    exit 1
fi

log_info "All syncs completed successfully"
