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

echo "Installing WSPRDAEMON Reflector Service..."
echo "Script directory: $SCRIPT_DIR"
echo "Note: Reflector uses only standard Python libraries, no venv needed"

# Create wsprdaemon user if it doesn't exist
if ! id -u $INSTALL_USER >/dev/null 2>&1; then
    echo "Creating $INSTALL_USER user..."
    useradd -r -s /bin/bash -d /home/$INSTALL_USER -m $INSTALL_USER
fi

# Create necessary data directories
echo "Creating data directories..."
for dir in /var/spool/wsprdaemon/reflector /var/lib/wsprdaemon /var/log/wsprdaemon /etc/wsprdaemon; do
    if [[ ! -d "$dir" ]]; then
        mkdir -p "$dir"
        chown $INSTALL_USER:$INSTALL_USER "$dir"
        echo "  Created $dir"
    else
        chown $INSTALL_USER:$INSTALL_USER "$dir"
        echo "  Directory $dir already exists"
    fi
done
echo "  Data directories ready"

# Install Python scripts
echo "Installing Python script..."
cp "$SCRIPT_DIR/wsprdaemon_reflector.py" /usr/local/bin/
chmod +x /usr/local/bin/wsprdaemon_reflector.py
echo "  Python script installed"

# Install wrapper script
echo "Installing wrapper script..."
cp "$SCRIPT_DIR/wsprdaemon_reflector.sh" /usr/local/bin/
chmod +x /usr/local/bin/wsprdaemon_reflector.sh
echo "  Wrapper script installed"

# Install systemd service file
echo "Installing systemd service file..."
cp "$SCRIPT_DIR/wsprdaemon_reflector@.service" /etc/systemd/system/
chmod 644 /etc/systemd/system/wsprdaemon_reflector@.service
echo "  Service file installed"

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
    echo "  IMPORTANT: Edit /etc/wsprdaemon/reflector_destinations.json with your actual server details"
else
    echo "Configuration file /etc/wsprdaemon/reflector_destinations.json already exists"
fi

# Reload systemd
echo "Reloading systemd..."
systemctl daemon-reload

echo ""
echo "=== Installation complete! ==="
echo ""
echo "Configuration: /etc/wsprdaemon/reflector_destinations.json"
echo "Scripts: /usr/local/bin/"
echo "Service: /etc/systemd/system/wsprdaemon_reflector@.service"
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
