#!/bin/bash
# WSPRNET Cache Management Script
# Quick commands for monitoring and managing the persistent cache

CACHE_DIR="/var/lib/wsprnet/cache"
LOG_FILE="/var/log/wsprdaemon/wsprnet_scraper.log"

function show_usage() {
    cat << EOF
WSPRNET Cache Management Commands

Setup:
  $0 setup           - Create and configure cache directory
  $0 install         - Install new version of scraper

Monitor:
  $0 status          - Show cache status
  $0 count           - Count cached files and spots
  $0 watch           - Watch cache activity in real-time
  $0 logs            - Show cache-related log entries

Test:
  $0 test-failure    - Simulate ClickHouse failure
  $0 test-recovery   - Recover from simulated failure

Maintenance:
  $0 backup          - Backup cached files
  $0 clean           - Remove all cached files (dangerous!)

EOF
}

function setup_cache() {
    echo "Setting up cache directory..."
    sudo mkdir -p "$CACHE_DIR"
    sudo chown wsprdaemon:wsprdaemon "$CACHE_DIR"
    sudo chmod 755 "$CACHE_DIR"
    echo "✓ Cache directory created: $CACHE_DIR"
    ls -ld "$CACHE_DIR"
}

function install_scraper() {
    echo "Installing enhanced wsprnet_scraper.py..."
    
    if [[ ! -f wsprnet_scraper.py ]]; then
        echo "ERROR: wsprnet_scraper.py not found in current directory"
        exit 1
    fi
    
    # Backup old version
    if [[ -f /usr/local/bin/wsprnet_scraper.py ]]; then
        echo "Backing up current version..."
        sudo cp /usr/local/bin/wsprnet_scraper.py /usr/local/bin/wsprnet_scraper.py.backup
        echo "✓ Backup saved to /usr/local/bin/wsprnet_scraper.py.backup"
    fi
    
    # Install new version
    sudo cp wsprnet_scraper.py /usr/local/bin/wsprnet_scraper.py
    sudo chmod 755 /usr/local/bin/wsprnet_scraper.py
    echo "✓ Installed to /usr/local/bin/wsprnet_scraper.py"
    
    # Restart service
    echo "Restarting service..."
    sudo systemctl restart wsprnet_scraper
    sleep 2
    sudo systemctl status wsprnet_scraper --no-pager -l
}

function show_status() {
    echo "=== Cache Status ==="
    echo ""
    
    echo "Cache Directory:"
    if [[ -d "$CACHE_DIR" ]]; then
        ls -ld "$CACHE_DIR"
        echo ""
        
        CACHE_COUNT=$(find "$CACHE_DIR" -name "spots_*.json" 2>/dev/null | wc -l)
        echo "Cached Files: $CACHE_COUNT"
        
        if [[ $CACHE_COUNT -gt 0 ]]; then
            echo ""
            echo "Cache Files:"
            ls -lh "$CACHE_DIR"/spots_*.json | tail -10
            
            echo ""
            echo "Total Spots Cached:"
            total_spots=0
            for f in "$CACHE_DIR"/spots_*.json; do
                count=$(jq -r '.spot_count' "$f" 2>/dev/null || echo "0")
                total_spots=$((total_spots + count))
            done
            echo "$total_spots spots"
        fi
    else
        echo "Cache directory does not exist: $CACHE_DIR"
        echo "Run: $0 setup"
    fi
    
    echo ""
    echo "=== Service Status ==="
    sudo systemctl status wsprnet_scraper --no-pager -l | head -20
}

function count_cache() {
    if [[ ! -d "$CACHE_DIR" ]]; then
        echo "Cache directory does not exist: $CACHE_DIR"
        exit 1
    fi
    
    CACHE_COUNT=$(find "$CACHE_DIR" -name "spots_*.json" 2>/dev/null | wc -l)
    echo "Cached Files: $CACHE_COUNT"
    
    if [[ $CACHE_COUNT -eq 0 ]]; then
        echo "No cached files (system healthy)"
        exit 0
    fi
    
    echo ""
    echo "Breakdown:"
    total_spots=0
    total_size=0
    
    for f in "$CACHE_DIR"/spots_*.json; do
        count=$(jq -r '.spot_count' "$f" 2>/dev/null || echo "0")
        size=$(stat -f%z "$f" 2>/dev/null || stat -c%s "$f" 2>/dev/null || echo "0")
        timestamp=$(basename "$f" | sed 's/spots_//; s/.json//')
        
        total_spots=$((total_spots + count))
        total_size=$((total_size + size))
        
        echo "  $(basename "$f"): $count spots"
    done
    
    echo ""
    echo "Total: $total_spots spots in $CACHE_COUNT files"
    echo "Size: $(echo "$total_size" | awk '{print $1/1024/1024}' | xargs printf "%.2f") MB"
}

function watch_cache() {
    echo "Watching cache activity (Ctrl-C to stop)..."
    echo ""
    tail -f "$LOG_FILE" | grep --line-buffered -i -E "(cache|replay|cached)"
}

function show_logs() {
    echo "=== Recent Cache Activity ==="
    echo ""
    grep -i -E "(cache|replay)" "$LOG_FILE" | tail -50
}

function test_failure() {
    echo "=== Testing ClickHouse Failure Scenario ==="
    echo ""
    
    echo "Stopping ClickHouse..."
    sudo systemctl stop clickhouse-server
    echo "✓ ClickHouse stopped"
    echo ""
    
    echo "Waiting for 2-3 scraper cycles (4-6 minutes)..."
    echo "This will create cached files as scraper tries to insert..."
    echo ""
    echo "Watch logs in another terminal:"
    echo "  tail -f $LOG_FILE | grep cache"
    echo ""
    echo "When ready to recover, run: $0 test-recovery"
}

function test_recovery() {
    echo "=== Testing Recovery Scenario ==="
    echo ""
    
    echo "Starting ClickHouse..."
    sudo systemctl start clickhouse-server
    echo "✓ ClickHouse started"
    echo ""
    
    echo "Waiting for ClickHouse to be ready..."
    sleep 5
    
    echo "Cache files will be replayed on next scraper cycle"
    echo ""
    echo "Watch logs:"
    echo "  tail -f $LOG_FILE | grep -i replay"
    echo ""
    echo "Check cache status:"
    echo "  $0 status"
}

function backup_cache() {
    if [[ ! -d "$CACHE_DIR" ]]; then
        echo "Cache directory does not exist: $CACHE_DIR"
        exit 1
    fi
    
    BACKUP_DIR="${CACHE_DIR}_backup_$(date +%Y%m%d_%H%M%S)"
    
    echo "Creating backup: $BACKUP_DIR"
    sudo mkdir -p "$BACKUP_DIR"
    sudo cp -v "$CACHE_DIR"/spots_*.json "$BACKUP_DIR"/ 2>/dev/null
    
    COUNT=$(ls "$BACKUP_DIR"/spots_*.json 2>/dev/null | wc -l)
    echo "✓ Backed up $COUNT files to $BACKUP_DIR"
}

function clean_cache() {
    if [[ ! -d "$CACHE_DIR" ]]; then
        echo "Cache directory does not exist: $CACHE_DIR"
        exit 1
    fi
    
    COUNT=$(find "$CACHE_DIR" -name "spots_*.json" 2>/dev/null | wc -l)
    
    if [[ $COUNT -eq 0 ]]; then
        echo "No cache files to clean"
        exit 0
    fi
    
    echo "WARNING: This will delete $COUNT cached files!"
    echo "These spots will be LOST if not already inserted into ClickHouse."
    echo ""
    read -p "Are you sure? Type 'yes' to confirm: " confirm
    
    if [[ "$confirm" != "yes" ]]; then
        echo "Cancelled"
        exit 0
    fi
    
    echo "Removing cache files..."
    sudo rm -fv "$CACHE_DIR"/spots_*.json
    echo "✓ Cache cleaned"
}

# Main command dispatcher
case "${1:-help}" in
    setup)
        setup_cache
        ;;
    install)
        install_scraper
        ;;
    status)
        show_status
        ;;
    count)
        count_cache
        ;;
    watch)
        watch_cache
        ;;
    logs)
        show_logs
        ;;
    test-failure)
        test_failure
        ;;
    test-recovery)
        test_recovery
        ;;
    backup)
        backup_cache
        ;;
    clean)
        clean_cache
        ;;
    help|*)
        show_usage
        ;;
esac
