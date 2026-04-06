#!/bin/bash
# install-reflector.sh - Install WSPRDAEMON Reflector Service
# Version: 2.0.0 - Switched to symlinks, added --validate mode
#
# Usage: sudo ./install-reflector.sh          # Full install
#        ./install-reflector.sh --check        # Verify symlinks point to this repo
#        sudo ./install-reflector.sh --sync    # Repair broken/missing symlinks
#        sudo ./install-reflector.sh --validate # Check service and config are healthy

set -e

VERSION="2.1.0"
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
INSTALL_USER="wsprdaemon"
SERVICE_NAME="wsprdaemon_reflector@reflector"
REFLECTOR_CONF="/etc/wsprdaemon/reflector_destinations.json"
LOG_FILE="/var/log/wsprdaemon/reflector.log"

# Files that get installed to /usr/local/bin as symlinks
MANAGED_FILES=(
    "wsprdaemon_reflector.py"
    "wsprdaemon_reflector.sh"
)

# ============================================================================
# Symlink check/sync (mirrors install-servers.sh pattern)
# ============================================================================
check_and_sync_files() {
    local mode="$1"  # "check" or "sync"
    local problems=0 fixed=0

    echo ""
    echo "=== Checking symlinks: /usr/local/bin -> $SCRIPT_DIR ==="
    echo ""

    for file in "${MANAGED_FILES[@]}"; do
        local repo_file="$SCRIPT_DIR/$file"
        local link="/usr/local/bin/$file"

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

# ============================================================================
# --check
# ============================================================================
if [[ "${1:-}" == "--check" ]]; then
    check_and_sync_files "check"
    exit $?
fi

# ============================================================================
# --sync
# ============================================================================
if [[ "${1:-}" == "--sync" ]]; then
    if [[ $EUID -ne 0 ]]; then echo "Requires root (use sudo)"; exit 1; fi
    check_and_sync_files "sync"
    exit $?
fi

# ============================================================================
# --validate
# ============================================================================
if [[ "${1:-}" == "--validate" ]]; then
    if [[ $EUID -ne 0 ]]; then echo "Requires root (use sudo)"; exit 1; fi

    echo "=== Validating WSPRDAEMON Reflector installation ==="
    echo ""
    VALIDATE_OK=true

    # --- Script versions ---
    echo "--- Script versions ---"
    PY3=$(which python3 2>/dev/null || echo "python3")
    for script in wsprdaemon_reflector.py wsprdaemon_reflector.sh; do
        link="/usr/local/bin/$script"
        if [[ -L "$link" || -f "$link" ]]; then
            target=$(readlink "$link" 2>/dev/null || echo "not a symlink")
            real="$(readlink -f "$link" 2>/dev/null || echo "$link")"
            case "$script" in
                *.py)
                    ver=$("$PY3" "$link" --version 2>/dev/null || echo "unknown")
                    ;;
                *.sh)
                    # Shell wrapper has no VERSION - get it from paired .py
                    py_real="$(readlink -f "/usr/local/bin/${script%.sh}.py" 2>/dev/null || true)"
                    ver=$("$PY3" "$py_real" --version 2>/dev/null || echo "unknown")
                    [[ "$ver" != "unknown" ]] && ver="${ver} (via .py)"
                    ;;
            esac
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

    # --- Config file ---
    echo ""
    echo "--- Configuration ---"
    if [[ -f "$REFLECTOR_CONF" ]]; then
        echo "  $REFLECTOR_CONF: exists ✓"
        # Show destinations
        if command -v python3 >/dev/null 2>&1; then
            python3 - << PYEOF
import json, sys
try:
    with open("$REFLECTOR_CONF") as f:
        cfg = json.load(f)
    dests = cfg.get("destinations", [])
    print(f"  Destinations ({len(dests)}):")
    for d in dests:
        print(f"    {d['name']}: {d['user']}@{d['host']}:{d['path']}")
    print(f"  scan_interval: {cfg.get('scan_interval','?')}s  rsync_interval: {cfg.get('rsync_interval','?')}s")
    # Check for placeholder values
    for d in dests:
        if "example.com" in d.get("host",""):
            print(f"  WARNING: {d['name']} still has example.com placeholder ✗")
            sys.exit(1)
except Exception as e:
    print(f"  ERROR parsing config: {e}")
    sys.exit(1)
PYEOF
        fi
    else
        echo "  $REFLECTOR_CONF: NOT FOUND ✗"
        VALIDATE_OK=false
    fi

    # --- SSH connectivity to each destination ---
    echo ""
    echo "--- SSH connectivity ---"
    if [[ -f "$REFLECTOR_CONF" ]]; then
        # Use python3 to reliably parse JSON and emit user@host pairs
        while IFS=$'\t' read -r user host; do
            if sudo -u "$INSTALL_USER" ssh -o BatchMode=yes -o ConnectTimeout=5 \
                    "${user}@${host}" "echo ok" >/dev/null 2>&1; then
                echo "  ${user}@${host}: SSH OK ✓"
            else
                echo "  ${user}@${host}: SSH FAILED ✗"
                VALIDATE_OK=false
            fi
        done < <(python3 -c "
import json
with open('$REFLECTOR_CONF') as f:
    cfg = json.load(f)
for d in cfg.get('destinations', []):
    print(d['user'] + chr(9) + d['host'])
" 2>/dev/null) || true
    fi

    # --- Spool directory ---
    echo ""
    echo "--- Spool directory ---"
    spool="/var/spool/wsprdaemon/reflector"
    if [[ -d "$spool" ]]; then
        queued=$(find "$spool" -name "*.tbz" 2>/dev/null | wc -l)
        owner=$(stat -c "%U:%G" "$spool")
        echo "  $spool: $queued tbz files queued, owner=$owner ✓"
    else
        echo "  $spool: missing ✗"
        VALIDATE_OK=false
    fi

    # --- Log file ---
    echo ""
    echo "--- Log file ---"
    if [[ -f "$LOG_FILE" ]]; then
        lines=$(wc -l < "$LOG_FILE")
        modified=$(stat -c "%y" "$LOG_FILE" | cut -d. -f1)
        # Show last heartbeat
        last_hb=$(grep "HEARTBEAT" "$LOG_FILE" 2>/dev/null | tail -1 || echo "(none)")
        echo "  $LOG_FILE: $lines lines, last modified $modified ✓"
        echo "  Last heartbeat: $last_hb"
    else
        echo "  $LOG_FILE: not yet created (service may not have run)"
    fi

    # --- Service status ---
    echo ""
    echo "--- Service status ---"
    if systemctl is-active --quiet "$SERVICE_NAME" 2>/dev/null; then
        since=$(systemctl show "$SERVICE_NAME" --property=ActiveEnterTimestamp --value 2>/dev/null || echo "unknown")
        echo "  $SERVICE_NAME: RUNNING ✓  (since $since)"
    else
        state=$(systemctl is-active "$SERVICE_NAME" 2>/dev/null || echo "unknown")
        echo "  $SERVICE_NAME: $state ✗"
        journalctl -u "$SERVICE_NAME" -n 10 --no-pager 2>/dev/null | sed 's/^/    /' || true
        VALIDATE_OK=false
    fi

    echo ""
    if [[ "$VALIDATE_OK" == "true" ]]; then
        echo "=== All checks passed ✓ ==="
    else
        echo "=== Some checks failed - see above ==="
        exit 1
    fi
    exit 0
fi

# ============================================================================
# --version / --help
# ============================================================================
if [[ "${1:-}" == "--version" || "${1:-}" == "-v" ]]; then
    echo "install-reflector.sh v$VERSION"
    exit 0
fi

if [[ "${1:-}" == "--help" || "${1:-}" == "-h" ]]; then
    echo "install-reflector.sh v$VERSION - Install WSPRDAEMON Reflector Service"
    echo ""
    echo "Usage: $0 [option]"
    echo ""
    echo "Options:"
    echo "  (no args)       Full installation (reflector)"
    echo "  --install-frp [--token=TOKEN] [--fqdn=FQDN]"
    echo "                  Install/update frps-secure + auth plugin (gateway servers)"
    echo "                  --token=  reuse a shared token (default: generate new)"
    echo "                  --fqdn=   public DNS name for TLS cert (default: hostname -f)"
    echo "  --check         Verify symlinks point to this repo"
    echo "  --sync          Repair broken/missing symlinks (requires sudo)"
    echo "  --validate      Full health check: symlinks, config, SSH, service"
    echo "  --version       Show version"
    echo "  --help          Show this help"
    exit 0
fi

# ============================================================================
# --install-frp  (idempotent frps-secure + auth plugin setup for gateway hosts)
# ============================================================================
if [[ "${1:-}" == "--install-frp" ]]; then
    if [[ $EUID -ne 0 ]]; then echo "Requires root (use sudo)"; exit 1; fi

    # Optional: --token=<value> to reuse an existing shared token across gateways
    # Optional: --fqdn=<value> to override hostname for TLS cert CN/SAN
    FORCED_TOKEN=""
    FORCED_FQDN=""
    for arg in "${@:2}"; do
        if [[ "$arg" == --token=* ]]; then
            FORCED_TOKEN="${arg#--token=}"
        elif [[ "$arg" == --fqdn=* ]]; then
            FORCED_FQDN="${arg#--fqdn=}"
        fi
    done

    FRP_VERSION="0.64.0"
    FRP_USER="frp"
    FRP_HOME="/home/frp"
    FRP_BIN="$FRP_HOME/bin/frps"
    FRP_CONF="$FRP_HOME/frps-secure.toml"
    FRP_PLUGIN="$FRP_HOME/frps-auth-plugin.py"
    FRP_TLS_DIR="$FRP_HOME/tls"

    echo "=== Installing frps-secure v$FRP_VERSION on ${FORCED_FQDN:-$(hostname -f)} ==="

    # --- frp user ---
    if ! id -u "$FRP_USER" >/dev/null 2>&1; then
        useradd -r -s /sbin/nologin -d "$FRP_HOME" -m "$FRP_USER"
        echo "  Created user: $FRP_USER"
    else
        echo "  User $FRP_USER: already exists"
    fi
    mkdir -p "$FRP_HOME/bin" "$FRP_TLS_DIR"
    chown -R "$FRP_USER:$FRP_USER" "$FRP_HOME"

    # --- frps binary ---
    if [[ -f "$FRP_BIN" ]] && "$FRP_BIN" --version 2>/dev/null | grep -q "$FRP_VERSION"; then
        echo "  frps binary: already v$FRP_VERSION"
    else
        ARCH=$(uname -m)
        [[ "$ARCH" == "aarch64" ]] && ARCH="arm64" || ARCH="amd64"
        TARBALL="frp_${FRP_VERSION}_linux_${ARCH}.tar.gz"
        URL="https://github.com/fatedier/frp/releases/download/v${FRP_VERSION}/${TARBALL}"
        echo "  Downloading frps $FRP_VERSION ($ARCH)..."
        TMP=$(mktemp -d)
        curl -fsSL "$URL" -o "$TMP/$TARBALL"
        tar -xzf "$TMP/$TARBALL" -C "$TMP"
        cp "$TMP/frp_${FRP_VERSION}_linux_${ARCH}/frps" "$FRP_BIN"
        chmod 755 "$FRP_BIN"
        chown "$FRP_USER:$FRP_USER" "$FRP_BIN"
        rm -rf "$TMP"
        echo "  frps installed: $FRP_BIN"
    fi

    # --- TLS certificate (self-signed, 10 years) ---
    FQDN="${FORCED_FQDN:-$(hostname -f)}"
    # Regenerate if cert is missing OR if --fqdn= was supplied and CN doesn't match
    NEED_CERT=false
    if [[ ! -f "$FRP_TLS_DIR/server.crt" || ! -f "$FRP_TLS_DIR/server.key" ]]; then
        NEED_CERT=true
    elif [[ -n "$FORCED_FQDN" ]]; then
        CERT_CN=$(openssl x509 -noout -subject -in "$FRP_TLS_DIR/server.crt" 2>/dev/null \
                  | sed 's/.*CN *= *//')
        if [[ "$CERT_CN" != "$FQDN" ]]; then
            NEED_CERT=true
            echo "  TLS cert: CN=$CERT_CN doesn't match --fqdn=$FQDN — regenerating"
        fi
    fi
    if $NEED_CERT; then
        openssl req -x509 -newkey rsa:4096 \
            -keyout "$FRP_TLS_DIR/server.key" \
            -out "$FRP_TLS_DIR/server.crt" \
            -days 3650 -nodes \
            -subj "/CN=$FQDN" \
            -addext "subjectAltName=DNS:$FQDN" 2>/dev/null
        chown "$FRP_USER:$FRP_USER" "$FRP_TLS_DIR/server.crt" "$FRP_TLS_DIR/server.key"
        chmod 600 "$FRP_TLS_DIR/server.key"
        echo "  TLS cert: generated for $FQDN"
    else
        echo "  TLS cert: already exists ($FRP_TLS_DIR/server.crt)"
    fi

    # --- Auth token (forced > existing > generated) ---
    if [[ -n "$FORCED_TOKEN" ]]; then
        FRP_TOKEN="$FORCED_TOKEN"
        echo "  Token: using provided token ($FRP_TOKEN)"
    elif [[ -f "$FRP_CONF" ]] && grep -q '^token' "$FRP_CONF" 2>/dev/null; then
        EXISTING_TOKEN=$(grep '^token' "$FRP_CONF" | sed 's/.*= *"\(.*\)"/\1/')
        echo "  Token: already set ($EXISTING_TOKEN)"
        FRP_TOKEN="$EXISTING_TOKEN"
    else
        FRP_TOKEN=$(openssl rand -hex 16)
        echo "  Token: generated ($FRP_TOKEN)"
    fi

    # --- frps-secure.toml (write only if missing or token changed) ---
    NEED_WRITE=true
    if [[ -f "$FRP_CONF" ]]; then
        if grep -q "\"$FRP_TOKEN\"" "$FRP_CONF" 2>/dev/null; then
            NEED_WRITE=false
            echo "  Config $FRP_CONF: already up to date"
        fi
    fi
    if [[ "$NEED_WRITE" == "true" ]]; then
        sed "s/REPLACE_WITH_TOKEN/$FRP_TOKEN/" \
            "$SCRIPT_DIR/frp/frps-secure.toml.template" > "$FRP_CONF"
        chown "$FRP_USER:$FRP_USER" "$FRP_CONF"
        echo "  Config: wrote $FRP_CONF"
    fi

    # --- Auth plugin ---
    cp "$SCRIPT_DIR/frp/frps-auth-plugin.py" "$FRP_PLUGIN"
    chmod 755 "$FRP_PLUGIN"
    chown "$FRP_USER:$FRP_USER" "$FRP_PLUGIN"
    touch "$FRP_HOME/frps-secure.log"
    chown "$FRP_USER:$FRP_USER" "$FRP_HOME/frps-secure.log"
    echo "  Auth plugin: $FRP_PLUGIN"

    # --- Systemd services ---
    for svc in frps-secure frps-auth-plugin; do
        cp "$SCRIPT_DIR/frp/${svc}.service" "/etc/systemd/system/${svc}.service"
        echo "  Service: /etc/systemd/system/${svc}.service"
    done
    systemctl daemon-reload
    systemctl enable frps-auth-plugin frps-secure
    systemctl restart frps-auth-plugin
    sleep 1
    systemctl restart frps-secure
    sleep 2

    echo ""
    echo "=== frps-secure installation complete ==="
    echo ""
    echo "  Shared token (distribute to clients):  $FRP_TOKEN"
    echo "  TLS cert for clients: $FRP_TLS_DIR/server.crt"
    echo ""
    echo "  Status:"
    systemctl is-active frps-auth-plugin frps-secure | paste - - | \
        awk '{print "  frps-auth-plugin: "$1"  frps-secure: "$2}'
    echo ""
    echo "  Dashboard: http://$FQDN:7501  (or via 10.x.x.x:7501)"
    echo "  Logs: sudo tail -f $FRP_HOME/frps-secure.log"
    echo ""
    echo "  Client wsprdaemon.conf settings:"
    echo "    rac_server         = $FQDN"
    echo "    rac_token          = $FRP_TOKEN"
    echo "    rac_tls_ca         = /etc/wsprdaemon/$(echo "$FQDN" | cut -d. -f1 | tr '[:upper:]' '[:lower:]')-ca.crt"
    echo "    rac_fallback_server = <other-gateway-hostname>"
    exit 0
fi

# ============================================================================
# Full installation
# ============================================================================
if [[ $EUID -ne 0 ]]; then
    echo "This script must be run as root (use sudo)"
    exit 1
fi

echo "Installing WSPRDAEMON Reflector Service v$VERSION..."
echo "Script directory: $SCRIPT_DIR"

# Create wsprdaemon user if needed
if ! id -u $INSTALL_USER >/dev/null 2>&1; then
    echo "Creating $INSTALL_USER user..."
    useradd -r -s /bin/bash -d /home/$INSTALL_USER -m $INSTALL_USER
fi

# Create data directories
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

# Install as symlinks
echo "Installing scripts as symlinks -> $SCRIPT_DIR ..."
for file in "${MANAGED_FILES[@]}"; do
    local_file="$SCRIPT_DIR/$file"
    link="/usr/local/bin/$file"
    if [[ ! -f "$local_file" ]]; then
        echo "  Skipping $file (not in repo)"
        continue
    fi
    chmod +x "$local_file"
    if [[ -L "$link" && "$(readlink "$link")" == "$local_file" ]]; then
        echo "  $file: already correct ✓"
    elif [[ -f "$link" || -L "$link" ]]; then
        echo "  Replacing $link -> $local_file"
        ln -sf "$local_file" "$link"
    else
        echo "  Creating symlink: $link -> $local_file"
        ln -s "$local_file" "$link"
    fi
done
echo "  Scripts installed"

# Install systemd service file from repo
echo "Installing systemd service file..."
if [[ -f "$SCRIPT_DIR/wsprdaemon_reflector@.service" ]]; then
    cp "$SCRIPT_DIR/wsprdaemon_reflector@.service" /etc/systemd/system/
    chmod 644 /etc/systemd/system/wsprdaemon_reflector@.service
    echo "  Service file installed"
else
    echo "  WARNING: wsprdaemon_reflector@.service not found in repo"
fi

# Create example config if none exists
if [[ ! -f "$REFLECTOR_CONF" ]]; then
    echo "Creating example configuration..."
    cat > "$REFLECTOR_CONF" << 'JSONEOF'
{
  "incoming_pattern": "/home/*/uploads/*.tbz",
  "spool_base_dir": "/var/spool/wsprdaemon/reflector",
  "destinations": [
    {
      "name": "SERVER1",
      "user": "wsprdaemon",
      "host": "server1.example.com",
      "path": "/var/spool/wsprdaemon/incoming"
    }
  ],
  "scan_interval": 2,
  "rsync_interval": 2,
  "rsync_bandwidth_limit": 20000,
  "rsync_timeout": 300,
  "log_file": "/var/log/wsprdaemon/reflector.log",
  "log_max_mb": 10,
  "verbosity": 1
}
JSONEOF
    chown $INSTALL_USER:$INSTALL_USER "$REFLECTOR_CONF"
    chmod 644 "$REFLECTOR_CONF"
    echo "  IMPORTANT: Edit $REFLECTOR_CONF with your actual server details"
else
    echo "  Config $REFLECTOR_CONF already exists - not overwritten"
fi

# Reload systemd, enable and start
echo "Reloading systemd..."
systemctl daemon-reload

echo "Enabling and starting $SERVICE_NAME..."
systemctl enable "$SERVICE_NAME"
systemctl restart "$SERVICE_NAME"
sleep 3

echo ""
echo "Useful commands:"
echo "  systemctl status $SERVICE_NAME"
echo "  journalctl -u $SERVICE_NAME -f"
echo "  $0 --check       # verify symlinks"
echo "  sudo $0 --sync   # repair symlinks"
echo "  sudo $0 --validate  # full health check"
echo ""
echo "NOTE: Scripts run directly from $SCRIPT_DIR via symlinks."
echo "      'git pull' in that directory takes effect immediately."
echo ""

# Run validate at end
exec "$0" --validate
