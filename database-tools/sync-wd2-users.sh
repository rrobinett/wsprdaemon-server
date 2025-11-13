#!/bin/bash
#
# sync-wd2-users.sh - Rename wsprdaemon-admin to chadmin on WD2
#
# This script converts wsprdaemon-admin to chadmin in the ClickHouse
# XML configuration file (users.d/wsprdaemon.xml)
#
# Version: 1.0
# Date: 2025-11-03

set -e

echo "=== Renaming wsprdaemon-admin to chadmin on WD2 ==="
echo ""
echo "This will:"
echo "  1. Backup /etc/clickhouse-server/users.d/wsprdaemon.xml"
echo "  2. Add new chadmin user with admin privileges (no password)"
echo "  3. Comment out wsprdaemon-admin in wsprdaemon.xml"
echo "  4. Restart ClickHouse server"
echo "  5. Update /etc/wsprdaemon/clickhouse.conf to use chadmin"
echo ""
echo "NOTE: You'll need sudo access to modify ClickHouse config files"
echo ""
read -p "Continue? (y/N): " -n 1 -r
echo
if [[ ! $REPLY =~ ^[Yy]$ ]]; then
    echo "Aborted."
    exit 0
fi

echo ""
echo "Step 1: Backing up wsprdaemon.xml..."
sudo cp /etc/clickhouse-server/users.d/wsprdaemon.xml /etc/clickhouse-server/users.d/wsprdaemon.xml.backup-$(date +%Y%m%d-%H%M%S)

echo "Step 2: Adding chadmin user to wsprdaemon.xml..."

# Add chadmin user before the closing </users> tag
# chadmin gets no password (like current setup) and same network restrictions
sudo sed -i '/<\/users>/i\
        <!-- CHADMIN Admin User - Renamed from wsprdaemon-admin -->\
        <chadmin>\
            <password></password>\
            <!-- Networks allowed: localhost and 10.0.0.0/8 -->\
            <networks>\
                <ip>127.0.0.1</ip>\
                <ip>::1</ip>\
                <ip>10.0.0.0/8</ip>\
            </networks>\
            <profile>default</profile>\
            <quota>default</quota>\
            <!-- Grant full access using access_management flag -->\
            <access_management>1</access_management>\
        </chadmin>' /etc/clickhouse-server/users.d/wsprdaemon.xml

echo "Step 3: Commenting out wsprdaemon-admin in wsprdaemon.xml..."
# Comment out the wsprdaemon-admin section by wrapping it in <!-- -->
sudo sed -i '/<wsprdaemon-admin>/,/<\/wsprdaemon-admin>/{
    s/^/<!-- /
    s/$/ -->/
}' /etc/clickhouse-server/users.d/wsprdaemon.xml

echo "Step 4: Restarting ClickHouse server..."
sudo systemctl restart clickhouse-server

echo ""
echo "Waiting 5 seconds for restart to complete..."
sleep 5

echo ""
echo "Step 5: Verifying new user..."
if sudo clickhouse-client --user chadmin --query "SELECT 'SUCCESS: chadmin works!' AS test" | grep -q SUCCESS; then
    echo "  ✓ chadmin user is working!"
else
    echo "  ✗ ERROR: chadmin user test failed"
    echo "  Rolling back..."
    sudo cp /etc/clickhouse-server/users.d/wsprdaemon.xml.backup-* /etc/clickhouse-server/users.d/wsprdaemon.xml
    sudo systemctl restart clickhouse-server
    exit 1
fi

echo ""
echo "Step 6: Updating config files to use chadmin..."

# Update clickhouse.conf
if [ -f /etc/wsprdaemon/clickhouse.conf ]; then
    sudo sed -i 's/CLICKHOUSE_USER="wsprdaemon-admin"/CLICKHOUSE_USER="chadmin"/g' /etc/wsprdaemon/clickhouse.conf
    echo "  ✓ Updated /etc/wsprdaemon/clickhouse.conf"
fi

echo ""
echo "=== User Rename Complete! ==="
echo ""
echo "Changes made:"
echo "  • Created XML-based user 'chadmin' with full admin privileges"
echo "  • Commented out 'wsprdaemon-admin' in wsprdaemon.xml"
echo "  • Updated /etc/wsprdaemon/clickhouse.conf"
echo ""
echo "Backup saved to: /etc/clickhouse-server/users.d/wsprdaemon.xml.backup-*"
echo ""
echo "Next steps:"
echo "  1. Verify: sudo clickhouse-client --user chadmin --query 'SHOW DATABASES'"
echo "  2. Restart services: sudo systemctl restart wsprnet_scraper@wsprnet wsprdaemon_server@wsprdaemon"
echo "  3. Check logs to ensure services work with new user"
echo ""
echo "If something goes wrong, restore with:"
echo "  sudo cp /etc/clickhouse-server/users.d/wsprdaemon.xml.backup-* /etc/clickhouse-server/users.d/wsprdaemon.xml"
echo "  sudo systemctl restart clickhouse-server"
echo ""
