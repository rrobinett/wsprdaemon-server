#!/bin/bash
# install-servers.sh - Install WSPRNET Scraper and WSPRDAEMON Server Services

set -e

# Check if running as root
if [[ $EUID -ne 0 ]]; then
   echo "This script must be run as root (use sudo)"
   exit 1
fi

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
INSTALL_USER="wsprdaemon"
INSTALL_DIR="/opt/wsprdaemon-server"
VENV_DIR="$INSTALL_DIR/venv"

echo "Installing WSPRDAEMON Server Services..."
echo "Script directory: $SCRIPT_DIR"
echo "Installation directory: $INSTALL_DIR"
echo "Virtual environment: $VENV_DIR"

# Create wsprdaemon user if it doesn't exist
if ! id -u $INSTALL_USER >/dev/null 2>&1; then
    echo "Creating $INSTALL_USER user..."
    useradd -r -s /bin/bash -d /home/$INSTALL_USER -m $INSTALL_USER
fi

# Create installation directory
echo "Creating installation directory..."
mkdir -p $INSTALL_DIR
chown $INSTALL_USER:$INSTALL_USER $INSTALL_DIR
echo "  Created $INSTALL_DIR"

# Create necessary data directories (but don't recursively chown if they already exist)
echo "Creating data directories..."
for dir in /var/spool/wsprdaemon /var/lib/wsprdaemon/wsprnet /var/lib/wsprdaemon/wsprdaemon /var/log/wsprdaemon /etc/wsprdaemon; do
    if [[ ! -d "$dir" ]]; then
        mkdir -p "$dir"
        chown $INSTALL_USER:$INSTALL_USER "$dir"
        echo "  Created $dir"
    else
        # Just fix ownership of the directory itself, not recursively
        chown $INSTALL_USER:$INSTALL_USER "$dir"
        echo "  Directory $dir already exists"
    fi
done
echo "  Data directories ready"

# Create Python virtual environment if it doesn't exist
if [[ ! -d $VENV_DIR ]]; then
    echo "Creating Python virtual environment at $VENV_DIR..."
    echo "  This may take a minute..."
    python3 -m venv $VENV_DIR
    chown -R $INSTALL_USER:$INSTALL_USER $VENV_DIR
    echo "  Virtual environment created"
else
    echo "Virtual environment already exists at $VENV_DIR"
fi

# Install Python dependencies
echo "Installing Python dependencies..."
$VENV_DIR/bin/pip install --upgrade pip --quiet
$VENV_DIR/bin/pip install requests clickhouse-connect --quiet
echo "  Dependencies installed"

# Install Python scripts
echo "Installing Python scripts..."
cp "$SCRIPT_DIR/wsprnet_scraper.py" /usr/local/bin/
cp "$SCRIPT_DIR/wsprdaemon_server.py" /usr/local/bin/
chmod +x /usr/local/bin/wsprnet_scraper.py
chmod +x /usr/local/bin/wsprdaemon_server.py
echo "  Python scripts installed"

# Install wrapper scripts
echo "Installing wrapper scripts..."
cp "$SCRIPT_DIR/wsprnet_scraper.sh" /usr/local/bin/
cp "$SCRIPT_DIR/wsprdaemon_server.sh" /usr/local/bin/
cp "$SCRIPT_DIR/wsprnet_cache_manager.sh" /usr/local/bin/
chmod +x /usr/local/bin/wsprnet_scraper.sh
chmod +x /usr/local/bin/wsprdaemon_server.sh
chmod +x /usr/local/bin/wsprnet_cache_manager.sh
echo "  Wrapper scripts installed"

# Install systemd service files
echo "Installing systemd service files..."
cp "$SCRIPT_DIR/wsprnet_scraper@.service" /etc/systemd/system/
cp "$SCRIPT_DIR/wsprdaemon_server@.service" /etc/systemd/system/
chmod 644 /etc/systemd/system/wsprnet_scraper@.service
chmod 644 /etc/systemd/system/wsprdaemon_server@.service
echo "  Service files installed"

# Create configuration files if they don't exist
if [[ ! -f /etc/wsprdaemon/clickhouse.conf ]]; then
    echo "Creating /etc/wsprdaemon/clickhouse.conf..."
    cat > /etc/wsprdaemon/clickhouse.conf << 'CONFEOF'
#!/bin/bash
# ClickHouse Connection Configuration
CLICKHOUSE_HOST="localhost"
CLICKHOUSE_PORT="8123"
CLICKHOUSE_DEFAULT_PASSWORD="CHANGEME"
CLICKHOUSE_WSPRNET_ADMIN_USER="wsprnet-admin"
CLICKHOUSE_WSPRNET_ADMIN_PASSWORD="CHANGEME"
CLICKHOUSE_WSPRNET_READONLY_USER="wsprnet-reader"
CLICKHOUSE_WSPRNET_READONLY_PASSWORD="CHANGEME"
CLICKHOUSE_WSPRDAEMON_ADMIN_USER="wsprdaemon-admin"
CLICKHOUSE_WSPRDAEMON_ADMIN_PASSWORD="CHANGEME"
CLICKHOUSE_WSPRDAEMON_READONLY_USER="wsprdaemon-reader"
CLICKHOUSE_WSPRDAEMON_READONLY_PASSWORD="CHANGEME"
CONFEOF
    chown root:$INSTALL_USER /etc/wsprdaemon/clickhouse.conf
    chmod 640 /etc/wsprdaemon/clickhouse.conf
    echo "  WARNING: Edit /etc/wsprdaemon/clickhouse.conf and set your credentials!"
else
    echo "Configuration file /etc/wsprdaemon/clickhouse.conf already exists"
fi

if [[ ! -f /etc/wsprdaemon/wsprnet.conf ]]; then
    echo "Creating /etc/wsprdaemon/wsprnet.conf..."
    cat > /etc/wsprdaemon/wsprnet.conf << 'CONFEOF'
#!/bin/bash
# WSPRNET Scraper Configuration
if [[ -f /etc/wsprdaemon/clickhouse.conf ]]; then
    source /etc/wsprdaemon/clickhouse.conf
fi
WSPRNET_USERNAME="CHANGEME"
WSPRNET_PASSWORD="CHANGEME"
VERBOSITY="1"
SESSION_FILE="/var/lib/wsprdaemon/wsprnet_session.json"
LOG_FILE="/var/log/wsprdaemon/wsprnet_scraper.log"
LOG_MAX_MB="10"
VENV_PYTHON="/opt/wsprdaemon-server/venv/bin/python3"
SCRAPER_SCRIPT="/usr/local/bin/wsprnet_scraper.py"
LOOP_INTERVAL="120"
CONFEOF
    chown root:$INSTALL_USER /etc/wsprdaemon/wsprnet.conf
    chmod 640 /etc/wsprdaemon/wsprnet.conf
    echo "  WARNING: Edit /etc/wsprdaemon/wsprnet.conf and set your credentials!"
else
    echo "Configuration file /etc/wsprdaemon/wsprnet.conf already exists"
fi

if [[ ! -f /etc/wsprdaemon/wsprdaemon.conf ]]; then
    echo "Creating /etc/wsprdaemon/wsprdaemon.conf..."
    cat > /etc/wsprdaemon/wsprdaemon.conf << 'CONFEOF'
#!/bin/bash
# WSPRDAEMON Server Configuration
if [[ -f /etc/wsprdaemon/clickhouse.conf ]]; then
    source /etc/wsprdaemon/clickhouse.conf
fi
VERBOSITY="1"
LOG_FILE="/var/log/wsprdaemon/wsprdaemon_server.log"
LOG_MAX_MB="10"
VENV_PYTHON="/opt/wsprdaemon-server/venv/bin/python3"
SCRAPER_SCRIPT="/usr/local/bin/wsprdaemon_server.py"
LOOP_INTERVAL="10"
EXTRACTION_DIR="/tmp/wsprdaemon"
CONFEOF
    chown root:$INSTALL_USER /etc/wsprdaemon/wsprdaemon.conf
    chmod 640 /etc/wsprdaemon/wsprdaemon.conf
else
    echo "Configuration file /etc/wsprdaemon/wsprdaemon.conf already exists"
fi

# Reload systemd
echo "Reloading systemd..."
systemctl daemon-reload

echo ""
echo "=== Installation complete! ==="
echo ""
echo "Installation directory: $INSTALL_DIR"
echo "Virtual environment: $VENV_DIR"
echo "Configuration files: /etc/wsprdaemon/"
echo "Scripts: /usr/local/bin/"
echo "Service templates: /etc/systemd/system/"
echo ""
echo "Next steps:"
echo "1. Edit /etc/wsprdaemon/clickhouse.conf and set ClickHouse credentials"
echo "2. Edit /etc/wsprdaemon/wsprnet.conf and set WSPRNET credentials"
echo "3. Enable services:"
echo "     sudo systemctl enable wsprnet_scraper@wsprnet"
echo "     sudo systemctl enable wsprdaemon_server@wsprdaemon"
echo "4. Start services:"
echo "     sudo systemctl start wsprnet_scraper@wsprnet"
echo "     sudo systemctl start wsprdaemon_server@wsprdaemon"
echo "5. Check status:"
echo "     sudo systemctl status wsprnet_scraper@wsprnet"
echo "     sudo systemctl status wsprdaemon_server@wsprdaemon"
