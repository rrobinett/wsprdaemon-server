#!/bin/bash
# install-reflector.sh - Install WSPRDAEMON Reflector Service

set -e

# Check if running as root
if [[ $EUID -ne 0 ]]; then
   echo "This script must be run as root (use sudo)"
   exit 1
fi

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
INSTALL_USER="wsprdaemon"
VENV_DIR="/home/$INSTALL_USER/wsprdaemon-server/venv"

echo "Installing WSPRDAEMON Reflector Service..."
echo "Script directory: $SCRIPT_DIR"
echo "Virtual environment: $VENV_DIR"

# Create wsprdaemon user if it doesn't exist
if ! id -u $INSTALL_USER >/dev/null 2>&1; then
    echo "Creating $INSTALL_USER user..."
    useradd -r -s /bin/bash -d /home/$INSTALL_USER -m $INSTALL_USER
fi

# Create necessary directories
echo "Creating data directories..."
mkdir -p /var/spool/wsprdaemon/reflector
mkdir -p /var/lib/wsprdaemon
mkdir -p /var/log/wsprdaemon
mkdir -p /etc/wsprdaemon

chown -R $INSTALL_USER:$INSTALL_USER /var/spool/wsprdaemon
chown -R $INSTALL_USER:$INSTALL_USER /var/lib/wsprdaemon
chown -R $INSTALL_USER:$INSTALL_USER /var/log/wsprdaemon

# Create Python virtual environment if it doesn't exist
if [[ ! -d $VENV_DIR ]]; then
    echo "Creating Python virtual environment..."
    sudo -u $INSTALL_USER python3 -m venv $VENV_DIR
fi

# Install Python scripts
echo "Installing Python script..."
cp "$SCRIPT_DIR/wsprdaemon_reflector.py" /usr/local/bin/
chmod +x /usr/local/bin/wsprdaemon_reflector.py

# Install wrapper script
echo "Installing wrapper script..."
cp "$SCRIPT_DIR/wsprdaemon_reflector.sh" /usr/local/bin/
chmod +x /usr/local/bin/wsprdaemon_reflector.sh

# Install systemd service file
echo "Installing systemd service file..."
cp "$SCRIPT_DIR/wsprdaemon_reflector@.service" /etc/systemd/system/
chmod 644 /etc/systemd/system/wsprdaemon_reflector@.service

# Create example configuration if none exists
if [[ ! -f /etc/wsprdaemon/reflector_destinations.json ]]; then
    echo "Creating example configuration..."
    cat > /etc/wsprdaemon/reflector_destinations.json << 'JSONEOF'
{
  "incoming_pattern": "/home/*/uploads/*.tbz",
  "spool_base_dir": "/var/spool/wsprdaemon/reflector",
  "destinations": [
    {
      "name": "SERVER1",
      "user": "wsprdaemon",
      "host": "server1.example.com",
      "path": "/var/spool/wsprdaemon/incoming"
    },
    {
      "name": "SERVER2",
      "user": "wsprdaemon",
      "host": "server2.example.com",
      "path": "/var/spool/wsprdaemon/incoming"
    }
  ],
  "scan_interval": 10,
  "rsync_interval": 5,
  "rsync_bandwidth_limit": 20000,
  "rsync_timeout": 300,
  "log_file": "/var/log/wsprdaemon/reflector.log",
  "log_max_mb": 10,
  "verbosity": 1
}
JSONEOF
    chown $INSTALL_USER:$INSTALL_USER /etc/wsprdaemon/reflector_destinations.json
    chmod 644 /etc/wsprdaemon/reflector_destinations.json
    echo ""
    echo "IMPORTANT: Edit /etc/wsprdaemon/reflector_destinations.json with your actual server details"
fi

# Reload systemd
systemctl daemon-reload

echo ""
echo "Installation complete!"
echo ""
echo "Next steps:"
echo "  1. Edit /etc/wsprdaemon/reflector_destinations.json with your destination servers"
echo "  2. Set up SSH key authentication from wsprdaemon user to destination servers:"
echo "     sudo -u wsprdaemon ssh-keygen -t rsa -b 4096 -N '' -f /home/wsprdaemon/.ssh/id_rsa"
echo "     sudo -u wsprdaemon ssh-copy-id wsprdaemon@server1.example.com"
echo "     sudo -u wsprdaemon ssh-copy-id wsprdaemon@server2.example.com"
echo "  3. Enable the service:"
echo "     sudo systemctl enable wsprdaemon_reflector@reflector"
echo "  4. Start the service:"
echo "     sudo systemctl start wsprdaemon_reflector@reflector"
echo "  5. Check status:"
echo "     sudo systemctl status wsprdaemon_reflector@reflector"
echo ""
