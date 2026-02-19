#!/bin/bash
# install-servers.sh - Install WSPRNET Scraper and WSPRDAEMON Server Services
# Version: 2.0.0 - Enhanced --validate: python/package versions, script versions,
#                  spool dir file counts, table row counts, data freshness check
#
# Usage: sudo ./install-servers.sh --ch-admin USERNAME --ch-admin-password PASSWORD
#        ./install-servers.sh --check      # Verify symlinks point to this repo
#        sudo ./install-servers.sh --sync  # Repair any broken/missing symlinks
#        sudo ./install-servers.sh --validate  # Check services and tables are running
#
# This script:
#   - Creates ClickHouse root admin user (credentials from command line, not stored in git)
#   - Configures ClickHouse 'default' user as read-only with password 'wsprdaemon'
#   - Installs Python scripts and wrapper scripts as symlinks: /usr/local/bin -> repo
#   - Creates systemd service files
#   - Creates configuration file templates

set -e

VERSION="2.3.0"
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
INSTALL_USER="wsprdaemon"
INSTALL_DIR="/opt/wsprdaemon-server"
VENV_DIR="$INSTALL_DIR/venv"

# Files that get installed to /usr/local/bin
MANAGED_FILES=(
    "wsprnet_scraper.py"
    "wsprnet_scraper.sh"
    "wsprdaemon_server.py"
    "wsprdaemon_server.sh"
    "wsprnet_cache_manager.sh"
)

# Function to install a single managed file as a symlink
install_symlink() {
    local repo_file="$1"
    local link="/usr/local/bin/$(basename "$repo_file")"

    if [[ ! -f "$repo_file" ]]; then
        return 0  # optional file not present in repo, skip
    fi

    chmod +x "$repo_file"

    if [[ -L "$link" ]]; then
        local current_target
        current_target=$(readlink "$link")
        if [[ "$current_target" == "$repo_file" ]]; then
            return 0  # already correct
        fi
        echo "  Updating symlink: $link -> $repo_file (was -> $current_target)"
        ln -sf "$repo_file" "$link"
    elif [[ -f "$link" ]]; then
        echo "  Replacing plain file with symlink: $link -> $repo_file"
        rm -f "$link"
        ln -s "$repo_file" "$link"
    else
        echo "  Creating symlink: $link -> $repo_file"
        ln -s "$repo_file" "$link"
    fi
}

# Function to check/repair symlinks
check_and_sync_files() {
    local mode="$1"  # "check" or "sync"
    local problems=0
    local fixed=0

    echo ""
    echo "=== Checking symlinks: /usr/local/bin -> $SCRIPT_DIR ==="
    echo ""

    for file in "${MANAGED_FILES[@]}"; do
        local repo_file="$SCRIPT_DIR/$file"
        local link="/usr/local/bin/$file"

        # File is optional if not in repo
        if [[ ! -f "$repo_file" ]]; then
            if [[ -e "$link" || -L "$link" ]]; then
                echo "⚠ $file: In /usr/local/bin but not in repo (orphan)"
            fi
            continue
        fi

        if [[ -L "$link" ]]; then
            local target
            target=$(readlink "$link")
            if [[ "$target" == "$repo_file" ]]; then
                echo "✓ $file: symlink -> $repo_file"
            else
                echo "✗ $file: symlink points to wrong target: $target"
                problems=$((problems + 1))
                if [[ "$mode" == "sync" ]]; then
                    ln -sf "$repo_file" "$link"
                    chmod +x "$repo_file"
                    echo "  → Fixed: now -> $repo_file"
                    fixed=$((fixed + 1))
                fi
            fi
        elif [[ -f "$link" ]]; then
            echo "✗ $file: plain file (not a symlink) in /usr/local/bin"
            problems=$((problems + 1))
            if [[ "$mode" == "sync" ]]; then
                rm -f "$link"
                ln -s "$repo_file" "$link"
                chmod +x "$repo_file"
                echo "  → Replaced with symlink -> $repo_file"
                fixed=$((fixed + 1))
            fi
        else
            echo "✗ $file: missing from /usr/local/bin"
            problems=$((problems + 1))
            if [[ "$mode" == "sync" ]]; then
                ln -s "$repo_file" "$link"
                chmod +x "$repo_file"
                echo "  → Created symlink -> $repo_file"
                fixed=$((fixed + 1))
            fi
        fi
    done

    echo ""
    echo "=== Summary ==="

    if [[ "$mode" == "check" ]]; then
        if [[ $problems -gt 0 ]]; then
            echo "Problems found: $problems"
            echo "Run 'sudo $0 --sync' to repair symlinks"
            return 1
        else
            echo "All symlinks are correct ✓"
            echo "Note: 'git pull' changes take effect immediately - no reinstall needed"
            return 0
        fi
    else
        if [[ $fixed -gt 0 ]]; then
            echo "Symlinks repaired: $fixed"
        else
            echo "All symlinks already correct ✓"
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

# Handle --validate argument
if [[ "${1:-}" == "--validate" ]]; then
    if [[ $EUID -ne 0 ]]; then
        echo "This command requires root (use sudo)"
        exit 1
    fi
    echo "=== Validating WSPRDAEMON Server installation ==="
    echo ""
    VALIDATE_OK=true

    # --- Python version ---
    echo "--- Python ---"
    if [[ -x "$VENV_DIR/bin/python3" ]]; then
        py_ver=$("$VENV_DIR/bin/python3" --version 2>&1)
        echo "  venv python: $py_ver ✓"
    else
        echo "  venv python not found: $VENV_DIR/bin/python3 ✗"
        VALIDATE_OK=false
    fi

    # --- Python packages ---
    echo ""
    echo "--- Python packages ---"
    for pkg in clickhouse_connect requests numpy; do
        if ver=$("$VENV_DIR/bin/python3" -c "
import importlib, importlib.metadata
try:
    ver = importlib.metadata.version('$pkg')
except Exception:
    import $pkg
    ver = getattr($pkg, '__version__', 'installed')
print(ver)
" 2>/dev/null); then
            echo "  $pkg: $ver ✓"
        else
            echo "  $pkg: NOT INSTALLED ✗"
            VALIDATE_OK=false
        fi
    done

    # --- Script versions ---
    echo ""
    echo "--- Script versions ---"
    # Find venv python - may not be in $VENV_DIR if running --validate standalone
    VENV_PY=""
    for candidate in "$VENV_DIR/bin/python3" /opt/wsprdaemon-server/venv/bin/python3; do
        if [[ -x "$candidate" ]]; then
            VENV_PY="$candidate"
            break
        fi
    done
    for script in wsprdaemon_server.py wsprnet_scraper.py; do
        link="/usr/local/bin/$script"
        if [[ -L "$link" || -f "$link" ]]; then
            target=$(readlink "$link" 2>/dev/null || echo "not a symlink")
            if [[ -n "$VENV_PY" ]]; then
                ver=$("$VENV_PY" "$link" --version 2>/dev/null || echo "unknown")
            else
                ver="(venv not found)"
            fi
            echo "  $script: $ver  ->  $target"
        else
            echo "  $script: not found in /usr/local/bin ✗"
            VALIDATE_OK=false
        fi
    done

    # --- Symlinks ---
    echo ""
    echo "--- Symlinks ---"
    check_and_sync_files "check" || VALIDATE_OK=false

    # --- Spool directories ---
    echo ""
    echo "--- Spool directories ---"
    for dir in /var/spool/wsprdaemon/from-gw1 /var/spool/wsprdaemon/from-gw2; do
        if [[ -d "$dir" ]]; then
            count=$(find "$dir" -name "*.tbz" 2>/dev/null | wc -l)
            owner=$(stat -c "%U:%G" "$dir")
            echo "  $dir: $count tbz files, owner=$owner ✓"
        else
            echo "  $dir: missing ✗"
            VALIDATE_OK=false
        fi
    done

    # --- Log files ---
    echo ""
    echo "--- Log files ---"
    for log in /var/log/wsprdaemon/wsprdaemon_server.log /var/log/wsprdaemon/wsprnet_scraper.log; do
        if [[ -f "$log" ]]; then
            lines=$(wc -l < "$log")
            modified=$(stat -c "%y" "$log" | cut -d. -f1)
            echo "  $log: $lines lines, last modified $modified ✓"
        else
            echo "  $log: not yet created (service may not have run)"
        fi
    done

    # --- Service Status ---
    echo ""
    echo "--- Service Status ---"
    for svc in wsprnet_scraper@wsprnet wsprdaemon_server@wsprdaemon; do
        if systemctl is-active --quiet "$svc" 2>/dev/null; then
            since=$(systemctl show "$svc" --property=ActiveEnterTimestamp --value 2>/dev/null || echo "unknown")
            echo "  $svc: RUNNING ✓  (since $since)"
        else
            state=$(systemctl is-active "$svc" 2>/dev/null || echo "unknown")
            echo "  $svc: $state ✗"
            journalctl -u "$svc" -n 10 --no-pager 2>/dev/null | sed "s/^/    /" || true
            VALIDATE_OK=false
        fi
    done

    # --- ClickHouse connectivity and tables ---
    echo ""
    echo "--- ClickHouse ---"
    if [[ ! -f /etc/wsprdaemon/clickhouse.conf ]]; then
        echo "  /etc/wsprdaemon/clickhouse.conf not found ✗"
        VALIDATE_OK=false
    else
        source /etc/wsprdaemon/clickhouse.conf
        CH="clickhouse-client --user $CLICKHOUSE_ROOT_ADMIN_USER --password $CLICKHOUSE_ROOT_ADMIN_PASSWORD"

        if $CH --query "SELECT 1" >/dev/null 2>&1; then
            echo "  ClickHouse connectivity: OK ✓"
        else
            echo "  ClickHouse connectivity: FAILED ✗"
            VALIDATE_OK=false
        fi

        echo ""
        echo "  Tables in wsprdaemon database:"
        tables=$($CH --query "SHOW TABLES FROM wsprdaemon" 2>/dev/null || echo "")
        if [[ -z "$tables" ]]; then
            echo "    (none yet - service may still be starting)"
        else
            while IFS= read -r table; do
                row_count=$($CH --query "SELECT count() FROM wsprdaemon.$table" 2>/dev/null || echo "error")
                min_time=$($CH --query "SELECT min(time) FROM wsprdaemon.$table" 2>/dev/null || echo "")
                max_time=$($CH --query "SELECT max(time) FROM wsprdaemon.$table" 2>/dev/null || echo "")
                printf "    %-30s %12s rows  %s .. %s
" "$table" "$row_count" "$min_time" "$max_time"
                # Warn if table is empty after services have been running
                if [[ "$row_count" == "0" ]] && systemctl is-active --quiet wsprdaemon_server@wsprdaemon; then
                    echo "    WARNING: $table is empty but service is running ✗"
                    VALIDATE_OK=false
                fi
            done <<< "$tables"
        fi

        # Check recent data - warn if newest spot is older than 30 minutes
        echo ""
        if echo "$tables" | grep -q "^spots$"; then
            newest=$($CH --query "SELECT max(time) FROM wsprdaemon.spots" 2>/dev/null || echo "")
            if [[ -n "$newest" && "$newest" != "1970-01-01 00:00:00" ]]; then
                age_seconds=$($CH --query "SELECT toUnixTimestamp(now()) - toUnixTimestamp(max(time)) FROM wsprdaemon.spots" 2>/dev/null || echo "9999")
                age_minutes=$(( age_seconds / 60 ))
                if [[ $age_minutes -lt 30 ]]; then
                    echo "  Most recent spot: $newest (${age_minutes}m ago) ✓"
                else
                    echo "  Most recent spot: $newest (${age_minutes}m ago) - WARNING: may be stale ✗"
                    VALIDATE_OK=false
                fi
            fi
        fi
    fi

    # --- tmpfiles.d / runtime dirs ---
    echo ""
    echo "--- Runtime directories ---"
    for dir in /tmp/wsprdaemon /var/lib/wsprdaemon /var/log/wsprdaemon; do
        if [[ -d "$dir" ]]; then
            owner=$(stat -c "%U:%G" "$dir")
            echo "  $dir: exists, owner=$owner ✓"
        else
            echo "  $dir: missing ✗"
            VALIDATE_OK=false
        fi
    done

    echo ""
    if [[ "$VALIDATE_OK" == "true" ]]; then
        echo "=== All checks passed ✓ ==="
    else
        echo "=== Some checks failed - see above ==="
        exit 1
    fi
    exit 0
fi

# Handle --version argument
if [[ "${1:-}" == "--version" || "${1:-}" == "-v" ]]; then
    echo "install-servers.sh version $VERSION"
    exit 0
fi

# Parse command line arguments for full install
CH_ADMIN_USER=""
CH_ADMIN_PASSWORD=""

usage() {
    echo "install-servers.sh v$VERSION - Install WSPRNET Scraper and WSPRDAEMON Server"
    echo ""
    echo "Usage: $0 --ch-admin USERNAME --ch-admin-password PASSWORD"
    echo "       $0 --check       # Verify symlinks point to this repo"
    echo "       $0 --sync        # Repair broken/missing symlinks (requires sudo)"
    echo "       $0 --validate    # Check services and ClickHouse tables are running"
    echo "       $0 --version     # Show version"
    echo ""
    echo "Required arguments for full install:"
    echo "  --ch-admin USERNAME           ClickHouse root admin username"
    echo "  --ch-admin-password PASSWORD  ClickHouse root admin password"
    echo ""
    echo "Example:"
    echo "  sudo $0 --ch-admin chadmin --ch-admin-password 'MySecretPass123'"
    exit 1
}

# Handle --help argument
if [[ "${1:-}" == "-h" || "${1:-}" == "--help" ]]; then
    usage
fi

while [[ $# -gt 0 ]]; do
    case $1 in
        --ch-admin)
            CH_ADMIN_USER="$2"
            shift 2
            ;;
        --ch-admin-password)
            CH_ADMIN_PASSWORD="$2"
            shift 2
            ;;
        -h|--help)
            usage
            ;;
        *)
            echo "Unknown option: $1"
            usage
            ;;
    esac
done

if [[ -z "$CH_ADMIN_USER" ]] || [[ -z "$CH_ADMIN_PASSWORD" ]]; then
    echo "ERROR: Both --ch-admin and --ch-admin-password are required"
    echo ""
    usage
fi

# Check if running as root
if [[ $EUID -ne 0 ]]; then
   echo "This script must be run as root (use sudo)"
   exit 1
fi

echo "Installing WSPRDAEMON Server Services v$VERSION..."
echo "Script directory: $SCRIPT_DIR"
echo "Installation directory: $INSTALL_DIR"
echo "Virtual environment: $VENV_DIR"
echo "ClickHouse admin user: $CH_ADMIN_USER"

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
for dir in /var/spool/wsprdaemon /var/spool/wsprdaemon/from-gw1 /var/spool/wsprdaemon/from-gw2 /var/lib/wsprdaemon /var/lib/wsprdaemon/wsprnet /var/lib/wsprdaemon/wsprnet/cache /var/lib/wsprdaemon/wsprdaemon /var/log/wsprdaemon /etc/wsprdaemon /tmp/wsprdaemon; do
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
$VENV_DIR/bin/pip install requests clickhouse-connect numpy --quiet
echo "  Dependencies installed"

# Install scripts as symlinks so git pull takes effect immediately
echo "Installing scripts as symlinks -> $SCRIPT_DIR ..."
for file in "${MANAGED_FILES[@]}"; do
    install_symlink "$SCRIPT_DIR/$file"
done
echo "  Scripts installed"

# ============================================================================
# Configure ClickHouse users
# ============================================================================
echo "Configuring ClickHouse users..."

# Check if ClickHouse is installed
if [[ ! -d /etc/clickhouse-server ]]; then
    echo "  WARNING: /etc/clickhouse-server not found - skipping ClickHouse user configuration"
    echo "  You will need to manually create ClickHouse users"
else
    # Create users.d directory if it doesn't exist
    mkdir -p /etc/clickhouse-server/users.d

    # Remove any existing definitions for this admin user (could be in other XML files)
    echo "  Removing any existing definitions for user: $CH_ADMIN_USER"
    for xmlfile in /etc/clickhouse-server/users.d/*.xml; do
        if [[ -f "$xmlfile" ]]; then
            # Check if this file contains a definition for our admin user
            if grep -q "<${CH_ADMIN_USER}>" "$xmlfile" 2>/dev/null; then
                # If it's not the dedicated file for this user, remove the user block
                if [[ "$xmlfile" != "/etc/clickhouse-server/users.d/${CH_ADMIN_USER}.xml" ]]; then
                    echo "    Removing $CH_ADMIN_USER from $xmlfile"
                    # Create temp file without this user block
                    sed -i "/<${CH_ADMIN_USER}>/,/<\/${CH_ADMIN_USER}>/d" "$xmlfile"
                fi
            fi
        fi
    done

    # Create/update root admin user XML file (always overwrites)
    echo "  Creating/updating ClickHouse root admin user: $CH_ADMIN_USER"
    cat > /etc/clickhouse-server/users.d/${CH_ADMIN_USER}.xml << CHADMINEOF
<?xml version="1.0"?>
<clickhouse>
    <users>
        <${CH_ADMIN_USER}>
            <password>${CH_ADMIN_PASSWORD}</password>
            <networks>
                <ip>::1</ip>
                <ip>127.0.0.1</ip>
            </networks>
            <profile>default</profile>
            <quota>default</quota>
            <access_management>1</access_management>
            <named_collection_control>1</named_collection_control>
            <show_named_collections>1</show_named_collections>
            <show_named_collections_secrets>1</show_named_collections_secrets>
        </${CH_ADMIN_USER}>
    </users>
</clickhouse>
CHADMINEOF
    chmod 600 /etc/clickhouse-server/users.d/${CH_ADMIN_USER}.xml
    chown clickhouse:clickhouse /etc/clickhouse-server/users.d/${CH_ADMIN_USER}.xml
    echo "    Created /etc/clickhouse-server/users.d/${CH_ADMIN_USER}.xml"

    # Configure default user as read-only with password (only if not already configured)
    # Check if default user already has a password in users.xml or users.d
    if grep -q "<default>" /etc/clickhouse-server/users.xml 2>/dev/null && \
       grep -A 20 "<default>" /etc/clickhouse-server/users.xml | grep -q "<password"; then
        echo "  Skipping default user config - already has password in users.xml"
    elif ls /etc/clickhouse-server/users.d/*default*.xml 2>/dev/null | grep -v "default-readonly.xml" | head -1 | xargs -I {} grep -l "<default>" {} 2>/dev/null; then
        echo "  Skipping default user config - already configured in users.d"
    else
        echo "  Configuring 'default' user as read-only with password 'wsprdaemon'"
        cat > /etc/clickhouse-server/users.d/default-readonly.xml << 'DEFAULTEOF'
<?xml version="1.0"?>
<clickhouse>
    <users>
        <default>
            <password>wsprdaemon</password>
            <networks>
                <ip>::/0</ip>
            </networks>
            <profile>readonly</profile>
            <quota>default</quota>
            <!-- No access_management = read-only -->
        </default>
    </users>
    <profiles>
        <readonly>
            <readonly>1</readonly>
        </readonly>
    </profiles>
</clickhouse>
DEFAULTEOF
        chmod 644 /etc/clickhouse-server/users.d/default-readonly.xml
        chown clickhouse:clickhouse /etc/clickhouse-server/users.d/default-readonly.xml
        echo "    Created /etc/clickhouse-server/users.d/default-readonly.xml"
    fi

    # Restart ClickHouse to apply user changes
    if systemctl is-active --quiet clickhouse-server; then
        echo "  Restarting ClickHouse to apply user changes..."
        systemctl restart clickhouse-server
        sleep 2
        if systemctl is-active --quiet clickhouse-server; then
            echo "    ClickHouse restarted successfully"
        else
            echo "    WARNING: ClickHouse may have failed to restart"
            echo "    Check: sudo systemctl status clickhouse-server"
        fi
    else
        echo "  Note: ClickHouse is not running - user config will apply on next start"
    fi
fi

# ============================================================================
# (wrapper scripts are installed as symlinks above)

# ============================================================================
# Create systemd service files
# ============================================================================
echo "Creating systemd service files..."

cat > /etc/systemd/system/wsprnet_scraper@.service << 'SERVICEEOF'
[Unit]
Description=WSPRNET Scraper (%i)
After=network.target clickhouse-server.service
Wants=clickhouse-server.service

[Service]
Type=simple
User=wsprdaemon
Group=wsprdaemon
ExecStart=/usr/local/bin/wsprnet_scraper.sh /etc/wsprdaemon/%i.conf
Restart=on-failure
RestartSec=60

# Memory limits
MemoryMax=1G
MemoryHigh=768M

# Security hardening
NoNewPrivileges=true
PrivateTmp=false
ProtectSystem=strict
ProtectHome=read-only
ReadWritePaths=/var/log/wsprdaemon /var/lib/wsprdaemon

# Logging
StandardOutput=journal
StandardError=journal
SyslogIdentifier=wsprnet-scraper-%i

[Install]
WantedBy=multi-user.target
SERVICEEOF

cat > /etc/systemd/system/wsprdaemon_server@.service << 'SERVICEEOF'
[Unit]
Description=WSPRDAEMON Server (%i)
After=network.target clickhouse-server.service
Wants=clickhouse-server.service

[Service]
Type=simple
User=wsprdaemon
Group=wsprdaemon
ExecStart=/usr/local/bin/wsprdaemon_server.sh /etc/wsprdaemon/%i.conf
Restart=on-failure
RestartSec=60

# Memory limits
MemoryMax=2G
MemoryHigh=1.5G

# Security hardening
NoNewPrivileges=true
PrivateTmp=false
ProtectSystem=strict
ProtectHome=read-only
ReadWritePaths=/var/log/wsprdaemon /var/lib/wsprdaemon /var/spool/wsprdaemon /tmp/wsprdaemon

# Logging
StandardOutput=journal
StandardError=journal
SyslogIdentifier=wsprdaemon-server-%i

[Install]
WantedBy=multi-user.target
SERVICEEOF

chmod 644 /etc/systemd/system/wsprnet_scraper@.service
chmod 644 /etc/systemd/system/wsprdaemon_server@.service
echo "  Service files created"

# ============================================================================
# Create/update configuration files
# ============================================================================

# Always update clickhouse.conf with current admin credentials
echo "Updating /etc/wsprdaemon/clickhouse.conf..."
cat > /etc/wsprdaemon/clickhouse.conf << CONFEOF
#!/bin/bash
# ClickHouse Connection Configuration
# File: /etc/wsprdaemon/clickhouse.conf
# Permissions: chmod 640, chown root:wsprdaemon
#
# This file contains ALL ClickHouse credentials.
# Do NOT commit real passwords to git.

# ClickHouse server connection
CLICKHOUSE_HOST="localhost"
CLICKHOUSE_PORT="8123"

# Default user - read-only access (configured in ClickHouse users.d)
CLICKHOUSE_DEFAULT_PASSWORD="wsprdaemon"

# Root admin user - full access to all databases
# Created by install-servers.sh in /etc/clickhouse-server/users.d/
CLICKHOUSE_ROOT_ADMIN_USER="${CH_ADMIN_USER}"
CLICKHOUSE_ROOT_ADMIN_PASSWORD="${CH_ADMIN_PASSWORD}"

# WSPRNET database read-only user (created by wsprnet_scraper.py)
CLICKHOUSE_WSPRNET_READONLY_USER="wsprnet-reader"
CLICKHOUSE_WSPRNET_READONLY_PASSWORD="wsprdaemon"

# WSPRDAEMON database read-only user (created by wsprdaemon_server.py)
CLICKHOUSE_WSPRDAEMON_READONLY_USER="wsprdaemon-reader"
CLICKHOUSE_WSPRDAEMON_READONLY_PASSWORD="wsprdaemon"
CONFEOF
chown root:$INSTALL_USER /etc/wsprdaemon/clickhouse.conf
chmod 640 /etc/wsprdaemon/clickhouse.conf
echo "  Updated /etc/wsprdaemon/clickhouse.conf with root admin credentials"

# Update wsprnet.conf - preserve WSPRNET credentials if file exists
WSPRNET_USER_SAVED=""
WSPRNET_PASS_SAVED=""
if [[ -f /etc/wsprdaemon/wsprnet.conf ]]; then
    # Try to preserve existing WSPRNET credentials
    WSPRNET_USER_SAVED=$(grep "^WSPRNET_USERNAME=" /etc/wsprdaemon/wsprnet.conf 2>/dev/null | cut -d'"' -f2)
    WSPRNET_PASS_SAVED=$(grep "^WSPRNET_PASSWORD=" /etc/wsprdaemon/wsprnet.conf 2>/dev/null | cut -d'"' -f2)
fi
# Use saved values if not CHANGEME, otherwise use CHANGEME
[[ "$WSPRNET_USER_SAVED" == "CHANGEME" || -z "$WSPRNET_USER_SAVED" ]] && WSPRNET_USER_SAVED="CHANGEME"
[[ "$WSPRNET_PASS_SAVED" == "CHANGEME" || -z "$WSPRNET_PASS_SAVED" ]] && WSPRNET_PASS_SAVED="CHANGEME"

echo "Updating /etc/wsprdaemon/wsprnet.conf..."
cat > /etc/wsprdaemon/wsprnet.conf << CONFEOF
#!/bin/bash
# WSPRNET Scraper Configuration
if [[ -f /etc/wsprdaemon/clickhouse.conf ]]; then
    source /etc/wsprdaemon/clickhouse.conf
fi
WSPRNET_USERNAME="${WSPRNET_USER_SAVED}"
WSPRNET_PASSWORD="${WSPRNET_PASS_SAVED}"
VERBOSITY="1"
SESSION_FILE="/var/lib/wsprdaemon/wsprnet_session.json"
LOG_FILE="/var/log/wsprdaemon/wsprnet_scraper.log"
LOG_MAX_MB="10"
VENV_PYTHON="/opt/wsprdaemon-server/venv/bin/python3"
SCRAPER_SCRIPT="/usr/local/bin/wsprnet_scraper.py"
LOOP_INTERVAL="20"
# Path to cache directory for downloaded spot files
WSPRNET_CACHE_DIR="/var/lib/wsprdaemon/wsprnet/cache"
CONFEOF
chown root:$INSTALL_USER /etc/wsprdaemon/wsprnet.conf
chmod 640 /etc/wsprdaemon/wsprnet.conf
if [[ "$WSPRNET_USER_SAVED" == "CHANGEME" ]]; then
    echo "  WARNING: Edit /etc/wsprdaemon/wsprnet.conf and set your WSPRNET credentials!"
else
    echo "  Preserved existing WSPRNET credentials"
fi

# Update wsprdaemon.conf
echo "Updating /etc/wsprdaemon/wsprdaemon.conf..."
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
# Comma-separated list of directories to scan for incoming .tbz files
INCOMING_DIRS="/var/spool/wsprdaemon/from-gw1,/var/spool/wsprdaemon/from-gw2"
CONFEOF
chown root:$INSTALL_USER /etc/wsprdaemon/wsprdaemon.conf
chmod 640 /etc/wsprdaemon/wsprdaemon.conf
echo "  Updated /etc/wsprdaemon/wsprdaemon.conf"

# Reload systemd
echo "Reloading systemd..."
systemctl daemon-reload

# ============================================================================
# Validate ClickHouse is reachable with admin credentials
# ============================================================================
echo ""
echo "Validating ClickHouse connectivity..."
if clickhouse-client --user "$CH_ADMIN_USER" --password "$CH_ADMIN_PASSWORD" \
        --query "SELECT 1" >/dev/null 2>&1; then
    echo "  ClickHouse reachable as $CH_ADMIN_USER ✓"
else
    echo "  WARNING: Cannot connect to ClickHouse as $CH_ADMIN_USER"
    echo "  Check: sudo systemctl status clickhouse-server"
    echo "  The services will not start correctly until ClickHouse is running."
fi

# ============================================================================
# Enable and start services
# ============================================================================
echo ""
echo "Enabling services to start on reboot..."
systemctl enable wsprnet_scraper@wsprnet
systemctl enable wsprdaemon_server@wsprdaemon
echo "  Services enabled ✓"

echo ""
echo "Starting services..."

# Stop first in case already running with old config
systemctl stop wsprnet_scraper@wsprnet 2>/dev/null || true
systemctl stop wsprdaemon_server@wsprdaemon 2>/dev/null || true
sleep 1

systemctl start wsprnet_scraper@wsprnet
systemctl start wsprdaemon_server@wsprdaemon
echo "  Services started"

# ============================================================================
# Post-install validation - reuse --validate logic
# ============================================================================
echo ""
echo "Waiting 10 seconds for services to initialize..."
sleep 10

echo ""
echo "ClickHouse users configured:"
echo "  - Root admin: $CH_ADMIN_USER (localhost only, full access)"
echo "  - default: read-only with password 'wsprdaemon' (any host)"
echo ""
echo "Useful commands:"
echo "  systemctl status wsprdaemon_server@wsprdaemon"
echo "  journalctl -u wsprdaemon_server@wsprdaemon -f"
echo "  journalctl -u wsprnet_scraper@wsprnet -f"
echo "  $0 --check          # verify symlinks"
echo "  sudo $0 --sync      # repair symlinks"
echo "  sudo $0 --validate  # full health check"
echo ""
echo "NOTE: Scripts run directly from $SCRIPT_DIR via symlinks."
echo "      'git pull' in that directory takes effect immediately -"
echo "      no reinstall or restart needed for script changes."
echo ""

# Run full validation
exec "$0" --validate
