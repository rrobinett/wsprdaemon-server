#!/bin/bash
#
# sync-wd2-users.sh - Rename wsprdaemon-admin to chadmin on WD2
#
# This script converts wsprdaemon-admin to chadmin in the ClickHouse
# XML configuration file (users.d/wsprdaemon.xml)
#
# Version: 1.1
# Date: 2025-11-03

set -e

echo "=== Renaming wsprdaemon-admin to chadmin on WD2 ==="
echo ""
echo "This will:"
echo "  1. Backup /etc/clickhouse-server/users.d/wsprdaemon.xml"
echo "  2. Replace wsprdaemon-admin with chadmin (keeping same config)"
echo "  3. Restart ClickHouse server"
echo "  4. Update /etc/wsprdaemon/clickhouse.conf to use chadmin"
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

echo "Step 2: Checking if chadmin already exists in the file..."
if sudo grep -q '<chadmin>' /etc/clickhouse-server/users.d/wsprdaemon.xml; then
    echo "  ✓ chadmin already exists in XML file"
else
    echo "  Adding chadmin user to wsprdaemon.xml..."
    # Use a temporary file for safety
    sudo cp /etc/clickhouse-server/users.d/wsprdaemon.xml /tmp/wsprdaemon.xml.tmp
    
    # Add chadmin section right after wsprdaemon-admin closing tag
    sudo awk '
        /<\/wsprdaemon-admin>/ {
            print
            print ""
            print "        <!-- CHADMIN Admin User - Renamed from wsprdaemon-admin -->"
            print "        <chadmin>"
            print "            <password></password>"
            print "            <!-- Networks allowed: localhost and 10.0.0.0/8 -->"
            print "            <networks>"
            print "                <ip>127.0.0.1</ip>"
            print "                <ip>::1</ip>"
            print "                <ip>10.0.0.0/8</ip>"
            print "            </networks>"
            print "            <profile>default</profile>"
            print "            <quota>default</quota>"
            print "            <!-- Grant full access using access_management flag -->"
            print "            <access_management>1</access_management>"
            print "        </chadmin>"
            next
        }
        { print }
    ' /tmp/wsprdaemon.xml.tmp > /tmp/wsprdaemon.xml.new
    
    # Replace the original
    sudo mv /tmp/wsprdaemon.xml.new /etc/clickhouse-server/users.d/wsprdaemon.xml
    sudo rm -f /tmp/wsprdaemon.xml.tmp
fi

echo "Step 3: Checking if wsprdaemon-admin is commented out..."
if sudo grep -q '<!-- <wsprdaemon-admin>' /etc/clickhouse-server/users.d/wsprdaemon.xml; then
    echo "  ✓ wsprdaemon-admin already commented out"
else
    echo "  Commenting out wsprdaemon-admin section..."
    # Use awk to properly comment out the entire section
    sudo awk '
        /<wsprdaemon-admin>/ {
            print "        <!-- <wsprdaemon-admin>"
            in_section = 1
            next
        }
        /<\/wsprdaemon-admin>/ && in_section {
            print "        </wsprdaemon-admin> -->"
            in_section = 0
            next
        }
        in_section {
            print "            " $0
            next
        }
        { print }
    ' /etc/clickhouse-server/users.d/wsprdaemon.xml > /tmp/wsprdaemon.xml.commented
    
    sudo mv /tmp/wsprdaemon.xml.commented /etc/clickhouse-server/users.d/wsprdaemon.xml
fi

echo "Step 4: Validating XML syntax..."
if sudo xmllint --noout /etc/clickhouse-server/users.d/wsprdaemon.xml 2>/dev/null; then
    echo "  ✓ XML is valid"
else
    echo "  ✗ XML validation failed! Installing xmllint and checking..."
    sudo apt install -y libxml2-utils >/dev/null 2>&1
    if ! sudo xmllint --noout /etc/clickhouse-server/users.d/wsprdaemon.xml; then
        echo "  ERROR: XML is invalid! Restoring backup..."
        sudo cp /etc/clickhouse-server/users.d/wsprdaemon.xml.backup-* /etc/clickhouse-server/users.d/wsprdaemon.xml
        exit 1
    fi
fi

echo "Step 5: Restarting ClickHouse server..."
sudo systemctl restart clickhouse-server

echo ""
echo "Waiting 5 seconds for restart to complete..."
sleep 5

echo ""
echo "Step 6: Verifying new user..."
if sudo clickhouse-client --user chadmin --query "SELECT 'SUCCESS: chadmin works!' AS test" 2>/dev/null | grep -q SUCCESS; then
    echo "  ✓ chadmin user is working!"
else
    echo "  ✗ ERROR: chadmin user test failed"
    echo "  Rolling back..."
    sudo cp /etc/clickhouse-server/users.d/wsprdaemon.xml.backup-* /etc/clickhouse-server/users.d/wsprdaemon.xml
    sudo systemctl restart clickhouse-server
    exit 1
fi

echo ""
echo "Step 7: Updating config files to use chadmin..."

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
