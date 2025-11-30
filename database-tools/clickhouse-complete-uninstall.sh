#!/bin/bash
#
# clickhouse-complete-uninstall.sh - COMPLETELY REMOVE ClickHouse
#
# ⚠️  WARNING WARNING WARNING WARNING WARNING WARNING ⚠️
# This script COMPLETELY REMOVES ClickHouse including:
#   - All data files
#   - All configuration
#   - All logs
#   - Everything
#
# THIS WILL DELETE ALL YOUR DATA!
# 
# Only use this if you want to start completely fresh.
# DO NOT run this on production servers with data you want to keep!

set -e

RED='\033[0;31m'
YELLOW='\033[1;33m'
NC='\033[0m'

echo -e "${RED}╔═══════════════════════════════════════════════════════════════╗${NC}"
echo -e "${RED}║                         ⚠️  WARNING ⚠️                         ║${NC}"
echo -e "${RED}║                                                               ║${NC}"
echo -e "${RED}║  This will COMPLETELY REMOVE ClickHouse and DELETE ALL DATA  ║${NC}"
echo -e "${RED}║                                                               ║${NC}"
echo -e "${RED}╚═══════════════════════════════════════════════════════════════╝${NC}"
echo ""
echo -e "${YELLOW}This will remove:${NC}"
echo "  - ClickHouse server and client packages"
echo "  - All configuration files in /etc/clickhouse-*/"
echo "  - ALL DATA in /var/lib/clickhouse/"
echo "  - All logs in /var/log/clickhouse-server/"
echo "  - Repository configuration"
echo ""
echo -e "${RED}THIS CANNOT BE UNDONE!${NC}"
echo ""

# Require explicit confirmation
read -p "Type 'DELETE EVERYTHING' to continue: " confirmation
if [[ "$confirmation" != "DELETE EVERYTHING" ]]; then
    echo "Aborted."
    exit 1
fi

echo ""
read -p "Are you ABSOLUTELY SURE? Type 'YES' to proceed: " final_confirm
if [[ "$final_confirm" != "YES" ]]; then
    echo "Aborted."
    exit 1
fi

echo ""
echo "Proceeding with complete ClickHouse removal..."
echo ""

# Stop the service first
echo "Stopping ClickHouse service..."
sudo systemctl stop clickhouse-server 2>/dev/null || true
sudo systemctl disable clickhouse-server 2>/dev/null || true

# Remove packages
echo "Removing ClickHouse packages..."
sudo apt remove --purge -y clickhouse-server clickhouse-client clickhouse-common-static
sudo apt autoremove -y

# Remove configuration files
echo "Removing configuration files..."
sudo rm -rf /etc/clickhouse-server/
sudo rm -rf /etc/clickhouse-client/

# Remove data directories (THIS DELETES ALL YOUR DATA!)
echo "DELETING ALL DATA..."
sudo rm -rf /var/lib/clickhouse/

# Remove log files
echo "Removing log files..."
sudo rm -rf /var/log/clickhouse-server/

# Remove the repository configuration
echo "Removing repository configuration..."
sudo rm -f /etc/apt/sources.list.d/clickhouse.list

# Remove the GPG key
sudo rm -f /usr/share/keyrings/clickhouse-keyring.gpg

# Remove any systemd unit files
echo "Removing systemd unit files..."
sudo rm -f /etc/systemd/system/clickhouse-server.service
sudo rm -f /lib/systemd/system/clickhouse-server.service

# Reload systemd
sudo systemctl daemon-reload

# Clean apt cache
sudo apt update

echo ""
echo "ClickHouse has been completely removed."
