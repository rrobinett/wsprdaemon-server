#!/bin/bash
# deploy_wsprdaemon_server.sh - Deploy/update wsprdaemon-server services

set -e

echo "=== WSPRDAEMON Server Deployment Script ==="
echo "This will update to the latest wsprdaemon-server code"
echo ""

# Detect hostname
HOSTNAME=$(hostname)
echo "Running on: $HOSTNAME"

# Determine what services this host should run based on hostname pattern
# WD0* (WD0, WD00, WD000, etc.) = reflectors
# WD[1-9]* (WD1, WD2, WD10, WD99, etc.) = servers
if [[ "$HOSTNAME" =~ ^WD0 ]]; then
    INSTALL_TYPE="reflector"
    NEEDS_VENV=false
    echo "Detected reflector server (hostname starts with WD0)"
elif [[ "$HOSTNAME" =~ ^WD[1-9] ]]; then
    INSTALL_TYPE="servers"
    NEEDS_VENV=true
    echo "Detected data server (hostname starts with WD[1-9])"
else
    echo "  Unknown hostname pattern - defaulting to servers installation"
    INSTALL_TYPE="servers"
    NEEDS_VENV=true
fi

echo "Installation type: $INSTALL_TYPE (venv needed: $NEEDS_VENV)"

# Stop running services
echo ""
echo "Step 1: Stopping services..."
sudo systemctl stop wsprnet_scraper@wsprnet 2>/dev/null && echo "  Stopped wsprnet_scraper" || echo "  wsprnet_scraper not running"
sudo systemctl stop wsprdaemon_server@wsprdaemon 2>/dev/null && echo "  Stopped wsprdaemon_server" || echo "  wsprdaemon_server not running"
sudo systemctl stop wsprdaemon_reflector@reflector 2>/dev/null && echo "  Stopped wsprdaemon_reflector" || echo "  wsprdaemon_reflector not running"

# Backup current configurations
echo ""
echo "Step 2: Backing up configurations..."
BACKUP_DIR="/root/wsprdaemon_backup_$(date +%Y%m%d-%H%M%S)"
sudo mkdir -p $BACKUP_DIR
if [[ -d /etc/wsprdaemon ]]; then
    sudo cp -a /etc/wsprdaemon $BACKUP_DIR/etc-wsprdaemon
    echo "  Backed up configs to $BACKUP_DIR"
fi

# Update repository
echo ""
echo "Step 3: Updating repository..."
if [[ -d ~/wsprdaemon-server ]]; then
    cd ~/wsprdaemon-server
    echo "  Pulling latest changes..."
    git pull
else
    echo "  Cloning repository..."
    cd ~
    git clone https://github.com/rrobinett/wsprdaemon-server.git wsprdaemon-server
    cd ~/wsprdaemon-server
fi

# Run installation
echo ""
echo "Step 4: Running installation..."
if [[ "$INSTALL_TYPE" == "servers" ]]; then
    echo "  Installing scraper and server services for $HOSTNAME"
    sudo bash install-servers.sh
elif [[ "$INSTALL_TYPE" == "reflector" ]]; then
    echo "  Installing reflector service for $HOSTNAME"
    sudo bash install-reflector.sh
fi

# Update existing config files to use new venv path (only for servers)
if [[ "$NEEDS_VENV" == true ]]; then
    echo ""
    echo "Step 5: Updating configuration files to use /opt/wsprdaemon-server/venv..."
    if [[ -f /etc/wsprdaemon/wsprnet.conf ]]; then
        sudo sed -i 's|VENV_PYTHON=.*|VENV_PYTHON="/opt/wsprdaemon-server/venv/bin/python3"|g' /etc/wsprdaemon/wsprnet.conf
        echo "  ✓ Updated wsprnet.conf"
    fi
    if [[ -f /etc/wsprdaemon/wsprdaemon.conf ]]; then
        sudo sed -i 's|VENV_PYTHON=.*|VENV_PYTHON="/opt/wsprdaemon-server/venv/bin/python3"|g' /etc/wsprdaemon/wsprdaemon.conf
        echo "  ✓ Updated wsprdaemon.conf"
    fi
else
    echo ""
    echo "Step 5: Configuration update skipped (not needed for reflector)"
fi

# Verify installation
echo ""
echo "Step 6: Verifying installation..."
if [[ "$INSTALL_TYPE" == "servers" ]]; then
    ls -l /usr/local/bin/wsprnet_scraper.py 2>/dev/null && echo "  ✓ wsprnet_scraper.py" || echo "  ✗ wsprnet_scraper.py missing"
    ls -l /usr/local/bin/wsprdaemon_server.py 2>/dev/null && echo "  ✓ wsprdaemon_server.py" || echo "  ✗ wsprdaemon_server.py missing"
elif [[ "$INSTALL_TYPE" == "reflector" ]]; then
    ls -l /usr/local/bin/wsprdaemon_reflector.py 2>/dev/null && echo "  ✓ wsprdaemon_reflector.py" || echo "  ✗ wsprdaemon_reflector.py missing"
fi

if [[ "$NEEDS_VENV" == true ]]; then
    ls -ld /opt/wsprdaemon-server/venv 2>/dev/null && echo "  ✓ Python venv in /opt" || echo "  ✗ Python venv missing"
else
    echo "  ✓ Reflector uses system Python (no venv needed)"
fi

# Test installation
if [[ "$NEEDS_VENV" == true ]]; then
    echo ""
    echo "Step 7: Testing Python virtual environment..."
    if /opt/wsprdaemon-server/venv/bin/python3 --version; then
        echo "  ✓ Python venv working"
    else
        echo "  ✗ Python venv not working!"
        exit 1
    fi
else
    echo ""
    echo "Step 7: Testing system Python..."
    if /usr/bin/python3 --version; then
        echo "  ✓ System Python working"
    else
        echo "  ✗ System Python not working!"
        exit 1
    fi
fi

# Start services
echo ""
echo "Step 8: Starting services..."
if [[ "$INSTALL_TYPE" == "servers" ]]; then
    sudo systemctl start wsprnet_scraper@wsprnet && echo "  ✓ Started wsprnet_scraper" || echo "  ✗ Failed to start wsprnet_scraper"
    sudo systemctl start wsprdaemon_server@wsprdaemon && echo "  ✓ Started wsprdaemon_server" || echo "  ✗ Failed to start wsprdaemon_server"
elif [[ "$INSTALL_TYPE" == "reflector" ]]; then
    sudo systemctl start wsprdaemon_reflector@reflector && echo "  ✓ Started wsprdaemon_reflector" || echo "  ✗ Failed to start wsprdaemon_reflector"
fi

# Wait a moment for services to start
sleep 3

# Check status
echo ""
echo "Step 9: Checking service status..."
if [[ "$INSTALL_TYPE" == "servers" ]]; then
    echo ""
    echo "=== WSPRNET Scraper Status ==="
    sudo systemctl status wsprnet_scraper@wsprnet --no-pager -l | head -20
    echo ""
    echo "=== WSPRDAEMON Server Status ==="
    sudo systemctl status wsprdaemon_server@wsprdaemon --no-pager -l | head -20
elif [[ "$INSTALL_TYPE" == "reflector" ]]; then
    echo ""
    echo "=== Reflector Status ==="
    sudo systemctl status wsprdaemon_reflector@reflector --no-pager -l | head -20
fi

echo ""
echo "=== Deployment Complete ==="
echo ""
echo "Repository: ~/wsprdaemon-server"
if [[ "$NEEDS_VENV" == true ]]; then
    echo "Installation: /opt/wsprdaemon-server/venv"
else
    echo "Installation: System Python (no venv)"
fi
echo "Configuration: /etc/wsprdaemon/"
echo "Backup: $BACKUP_DIR"
echo ""
echo "Monitor logs with:"
if [[ "$INSTALL_TYPE" == "servers" ]]; then
    echo "  sudo tail -f /var/log/wsprdaemon/wsprnet_scraper.log"
    echo "  sudo tail -f /var/log/wsprdaemon/wsprdaemon_server.log"
elif [[ "$INSTALL_TYPE" == "reflector" ]]; then
    echo "  sudo tail -f /var/log/wsprdaemon/reflector.log"
fi
echo ""
echo "Check recent log entries:"
if [[ "$INSTALL_TYPE" == "servers" ]]; then
    echo "  sudo journalctl -u wsprnet_scraper@wsprnet -n 50 --no-pager"
    echo "  sudo journalctl -u wsprdaemon_server@wsprdaemon -n 50 --no-pager"
elif [[ "$INSTALL_TYPE" == "reflector" ]]; then
    echo "  sudo journalctl -u wsprdaemon_reflector@reflector -n 50 --no-pager"
fi
