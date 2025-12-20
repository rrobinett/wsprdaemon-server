#!/bin/bash
# install-reflector.sh - Install WSPRDAEMON Reflector Service
# Version 1.1 - Added --check and --sync options

set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
INSTALL_USER="wsprdaemon"
VERSION="1.1"

# Files that get installed to /usr/local/bin
MANAGED_FILES=(
    "wsprdaemon_reflector.py"
    "wsprdaemon_reflector.sh"
)

# Function to compare and sync files
check_and_sync_files() {
    local mode="$1"  # "check" or "sync"
    local needs_restart=0
    local files_updated=0
    local files_differ=0
    local repo_updated=0
    
    echo ""
    echo "=== Checking file synchronization ==="
    echo "Repository: $SCRIPT_DIR"
    echo "Installed:  /usr/local/bin"
    echo ""
    
    for file in "${MANAGED_FILES[@]}"; do
        local repo_file="$SCRIPT_DIR/$file"
        local installed_file="/usr/local/bin/$file"
        
        # Check if both files exist
        if [[ ! -f "$repo_file" ]]; then
            echo "⚠ $file: Missing from repository"
            continue
        fi
        
        if [[ ! -f "$installed_file" ]]; then
            echo "✗ $file: Not installed"
            files_differ=$((files_differ + 1))
            if [[ "$mode" == "sync" ]]; then
                echo "  → Installing from repository..."
                cp "$repo_file" "$installed_file"
                chmod +x "$installed_file"
                echo "  ✓ Installed"
                files_updated=$((files_updated + 1))
                needs_restart=1
            fi
            continue
        fi
        
        # Compare content using diff
        if diff -q "$repo_file" "$installed_file" >/dev/null 2>&1; then
            echo "✓ $file: In sync"
        else
            # Files differ - determine which is newer by timestamp
            local repo_time=$(stat -c %Y "$repo_file" 2>/dev/null)
            local installed_time=$(stat -c %Y "$installed_file" 2>/dev/null)
            local repo_date=$(stat -c %y "$repo_file" 2>/dev/null | cut -d. -f1)
            local installed_date=$(stat -c %y "$installed_file" 2>/dev/null | cut -d. -f1)
            
            files_differ=$((files_differ + 1))
            
            if [[ "$installed_time" -gt "$repo_time" ]]; then
                echo "✗ $file: DIFFERS (installed is newer)"
                echo "    Repo:      $repo_date"
                echo "    Installed: $installed_date"
                
                if [[ "$mode" == "sync" ]]; then
                    echo "  → Updating repository version..."
                    cp "$installed_file" "$repo_file"
                    echo "  ✓ Repository updated (remember to git commit)"
                    files_updated=$((files_updated + 1))
                    repo_updated=$((repo_updated + 1))
                fi
            else
                echo "✗ $file: DIFFERS (repo is newer)"
                echo "    Repo:      $repo_date"
                echo "    Installed: $installed_date"
                
                if [[ "$mode" == "sync" ]]; then
                    echo "  → Updating installed version..."
                    cp "$repo_file" "$installed_file"
                    chmod +x "$installed_file"
                    echo "  ✓ Updated"
                    files_updated=$((files_updated + 1))
                    needs_restart=1
                fi
            fi
        fi
    done
    
    echo ""
    echo "=== Summary ==="
    
    if [[ "$mode" == "check" ]]; then
        if [[ $files_differ -gt 0 ]]; then
            echo "Files out of sync: $files_differ"
            echo ""
            echo "Run 'sudo $0 --sync' to synchronize files"
            return 1
        else
            echo "All files are in sync ✓"
            return 0
        fi
    else
        # sync mode
        if [[ $files_updated -gt 0 ]]; then
            echo "Files synchronized: $files_updated"
            if [[ $needs_restart -eq 1 ]]; then
                echo ""
                echo "⚠ RESTART REQUIRED: Installed files were updated."
                echo "  Run: sudo systemctl restart wsprdaemon_reflector@reflector"
            fi
            if [[ $repo_updated -gt 0 ]]; then
                echo ""
                echo "⚠ COMMIT REQUIRED: Repository files were updated from installed versions."
                echo "  Run: cd $SCRIPT_DIR && git add -A && git commit -m 'Sync scripts from /usr/local/bin'"
            fi
        else
            echo "All files already in sync ✓"
        fi
        return 0
    fi
}

# Handle --check argument
if [[ "${1:-}" == "--check" ]]; then
    check_and_sync_files "check"
    exit $?
fi

# Handle --sync argument
if [[ "${1:-}" == "--sync" ]]; then
    if [[ $EUID -ne 0 ]]; then
        echo "This command requires root (use sudo)"
        exit 1
    fi
    check_and_sync_files "sync"
    exit $?
fi

# Handle --version argument
if [[ "${1:-}" == "--version" || "${1:-}" == "-v" ]]; then
    echo "install-reflector.sh version $VERSION"
    exit 0
fi

# Handle --help argument
if [[ "${1:-}" == "--help" || "${1:-}" == "-h" ]]; then
    echo "install-reflector.sh v$VERSION - Install WSPRDAEMON Reflector Service"
    echo ""
    echo "Usage: $0 [option]"
    echo ""
    echo "Options:"
    echo "  (no args)   Full installation"
    echo "  --check     Compare repo vs installed file timestamps"
    echo "  --sync      Sync files (newer overwrites older, both directions)"
    echo "  --version   Show version"
    echo "  --help      Show this help"
    echo ""
    exit 0
fi

# === Full Installation ===

# Check if running as root
if [[ $EUID -ne 0 ]]; then
   echo "This script must be run as root (use sudo)"
   exit 1
fi

echo "Installing WSPRDAEMON Reflector Service v$VERSION..."
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
echo "To check file sync status later: $0 --check"
echo "To sync files: sudo $0 --sync"
echo ""
