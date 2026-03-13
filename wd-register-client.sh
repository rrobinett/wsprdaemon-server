#!/bin/bash
###############################################################################
### wd-register-client.sh v3.25.0
###
### Script to register WSPRDAEMON client stations for SFTP uploads
### Creates user accounts on gateway servers and configures client access
###
### v3.32.0 Changes:
###   - CHANGE: wd-versions now sorts by version (oldest first) by default.
###         Use --sort rac to get RAC number order instead.
###
### v3.31.0 Changes:
###   - FIX: wd-versions no longer prints duplicates when --sort version used.
###         All results are now collected silently then printed in a single pass.
###   - FIX: wd-versions-sort now uses awk+tab-delimited sort so version strings
###         containing spaces (e.g. "3.2.3 (live)") sort correctly as one field.
###   - FIX: wd-versions-login and wd-login use same awk column parsing fix.
###
### v3.30.0 Changes:
###   - NEW: wd-versions-sort [LOGFILE]
###         Re-sorts an existing wd-versions log by version number without
###         rescanning. Uses most recent wd-versions-*.log if no file given.
###   - NEW: wd-versions-login [LOGFILE]
###         Walks RACs sorted oldest-version-first from a log file, SSHing
###         in one at a time. After each session: Enter=next, s=skip, q=quit.
###   - NEW: wd-login <RAC_NUMBER> [LOGFILE]
###         SSH directly into a single RAC by number. Looks up the login
###         user from the versions log; falls back to 'wsprdaemon'.
###
### v3.29.0 Changes:
###   - FIX: wd-versions method 3 (git branch+count) now rejects branch names
###         that don't look like version strings (e.g. 'master', 'main').
###         These now fall through to method 4 (wsprdaemon.sh -V) which
###         correctly returns the real version number.
###   - CLEAN: Active port list no longer printed (just the count).
###
### v3.28.0 Changes:
###   - FIX: wd-versions now validates version strings before accepting them.
###         wd_version.txt files containing a literal $(cd ...) shell expression
###         (RAC 14, 34 etc) are now rejected and fall through to next method.
###   - NEW: Method 3 added — git symbolic-ref --short HEAD + git rev-list
###         --count HEAD (the 'wdvv' alias pattern). Catches sites like RAC 14
###         where wd_version.txt is corrupted but git history is intact.
###   - FIX: wsprdaemon.sh -V timeout increased from 15 to 30 seconds to
###         handle slow clients (RAC status log lines can add 5+ seconds).
###   - Version detection order: wd_version.txt → grep VERSION= →
###         git branch+count → wsprdaemon.sh -V
###
### v3.27.0 Changes:
###   - FIX: wd-versions now tries three version detection methods in order:
###       1. ~/wsprdaemon/wd_version.txt + git rev-list --count HEAD (newest)
###       2. grep 'declare -r VERSION=' ~/wsprdaemon/wsprdaemon.sh (older)
###       3. ~/wsprdaemon/wsprdaemon.sh -V => parse "Version = X.X.X" (oldest)
###         Method 3 uses timeout 15 to avoid hanging on slow clients.
###
### v3.26.0 Changes:
###   - FIX: wd-versions now does a parallel port scan first (batches of 40
###         nc probes at once) so the active RAC list is built in seconds.
###         Passphrase prompt comes AFTER the scan, only when we know there
###         is work to do. Results stream live to terminal as each RAC is
###         queried rather than waiting for all results before printing.
###   - NEW: wd-versions --parallel N to tune batch size (default: 40)
###
### v3.25.0 Changes:
###   - NEW: wd-versions command
###         SSHes into every active RAC (autologin required) and reports
###         the installed WD version. Tries new-style wd_version.txt +
###         git rev-list first, falls back to grep in wsprdaemon.sh.
###         Supports --timeout N and --sort rac|version|site options.
###         Writes a timestamped log file of results.
###
### v3.14.0 Changes:
###   - FIX: scan-and-clean-all now includes RAC# and port in every log line
###         (was only printing to terminal, not to log file)
###   - FIX: SSH autologin failures now report specific reason: host key
###         mismatch, no pubkey, connection refused, or timeout
###   - FIX: Summary now splits "skipped" into "no autologin" vs "port closed"
###
### v3.13.0 Changes:
###   - FIX: wd-client-to-server-setup now applies the 'user' field from the
###         SSR config (client_user_name) to override the default 'wsprdaemon'
###         arg. RAC 161 (user=alan) and others with non-default logins now
###         connect correctly for scp, ssh, and all subsequent operations.
###   - NEW: Version number printed on every invocation for easy verification.
###
### v3.12.0 Changes:
###   - FIX: setup-autologin now looks up the 'user' field from the SSR
###         config for each RAC and uses that as the SSH login name instead
###         of always using the global client_user default (wsprdaemon).
###         RACs like RAC 161 with user=alan now connect correctly.
###
### v3.11.0 Changes:
###   - FIX: install_autologin_key now installs keys manually via sshpass+ssh
###         instead of ssh-copy-id, which was leaving behind ssh-copy-id.*
###         temp directories in ~/.ssh on every failed connection attempt.
###
### v3.10.0 Changes:
###   - IMPROVE: show-rac-last-login now derives RAC# from remotePort
###         (port - 35800 = RAC# for SSH ports, port - 45800 = RAC# for
###         WEB ports). Pairs SSH and WEB proxies for the same RAC into a
###         single row. Non-RAC ports are shown in a separate section.
###
### v3.9.0 Changes:
###   - REWRITE: show-rac-last-login now uses the frps REST API instead of
###         log parsing. Queries http://localhost:7500/api/proxy/tcp with
###         admin credentials to get live proxy name, remote port, status,
###         lastStartTime, and lastCloseTime for every registered proxy.
###         Much more reliable and doesn't require log file access.
###   - REMOVED: frps log file parsing (replaced by API)
###
### v3.8.0 Changes:
###   - NEW: show-rac-last-login command
###         Parses the frps log file to report when each RAC client last
###         connected its proxy tunnel. Correlates proxy name (e.g. "rac003")
###         to RAC number and port. Accepts optional --log <file> and
###         --since <days> arguments. Output is sorted by last-seen time.
###
### v3.7.0 Changes:
###   - NEW: setup-autologin prompts once for a session-only fallback
###         password at startup. When a RAC has no stored config password,
###         OR when the stored password fails ssh-copy-id, the fallback is
###         tried automatically. The fallback is never written to disk.
###
### v3.6.0 Changes:
###   - NEW: setup-autologin command
###         Scans all RAC ports 1-213; for each reachable port that lacks
###         passwordless SSH access, looks up the RAC password from the
###         encrypted SSR config and uses sshpass + ssh-copy-id to install
###         the local public key, enabling future autologin.
###
### v3.5.0 Changes:
###   - NEW: manual-setup mode for creating accounts without RAC access
###   - Usage: ./wd-register-client.sh manual-setup <USERNAME> <SSH_PUBKEY>
###   - Creates user on both gw1 and gw2, outputs config line for client
###
### v3.4.2 Changes:
###   - FIX: Add sudo to rm when verifying peer gateway upload (wsprdaemon can't
###          delete files owned by SFTP user without sudo)
###
### v3.4.1 Changes:
###   - Test SFTP upload to peer gateway after creating user there
###
### v3.4.0 Changes:
###   - Use WD_SERVER_USER_LIST array format (gw1 always first, gw2 second)
###   - Create user on BOTH gw1 and gw2 for redundancy
###   - Remove old scalar WD_SERVER_USER when updating config
###
### v3.3.1 Changes:
###   - FIX: Remove obsolete wd00/wd0 references
###   - WD_SERVER_FQDN now auto-detects gw1 or gw2 based on hostname
###   - SFTP test uses StrictHostKeyChecking=accept-new
###
### v3.3.0 Changes:
###   - Use sshpass for password authentication (gw1/gw2 keys removed from clients)
###   - Prompts for password if not stored in encrypted config
###   - Auto-installs sshpass if not present
###
### v3.2.1 Changes:
###   - FIX: Add SSH_OPTS to auto-accept new host keys (StrictHostKeyChecking=accept-new)
###
### v3.2.0 Changes:
###   - Display RAC credentials (site, description, password) before SSH login
###
### v3.1.0 Changes:
###   - FIX: Always connect to RAC clients via GW2 (localhost, mesh, or VPN)
###   - Added -V/--version flag
###
### v3.0.0 Changes:
###   - SECURITY: Now uses encrypted config from wd-rac project
###   - Passphrase is prompted interactively (NEVER cached on gw1/gw2)
###
### Usage:
###   ./wd-register-client.sh <RAC_NUMBER> [client_user]
###   ./wd-register-client.sh clean-gw-keys <RAC_NUMBER> [client_user]
###   ./wd-register-client.sh show-rac-last-login [--log FILE] [--since DAYS] [--connected] [--sort time|rac]
###   ./wd-register-client.sh setup-autologin [client_user]
###   ./wd-register-client.sh scan-and-clean-all [client_user]
###   ./wd-register-client.sh wd-versions [--timeout N] [--sort rac|version|site]
###   ./wd-register-client.sh -V|--version
###
###############################################################################

declare VERSION="3.32.0"

if [[ -f ~/wsprdaemon/bash-aliases ]]; then
    source ~/wsprdaemon/bash-aliases
fi

###############################################################################
### wd-rac Encrypted Config Integration (v3.0)
###
### SECURITY: This script runs on gw1/gw2 gateway servers which have direct
### FRP tunnel access to all RAC client machines. To minimize the risk of a
### compromised gateway learning RAC credentials:
###   - Passphrase is NEVER cached (prompted every time)
###   - Decrypted RAC table is NEVER saved to disk
###   - Credentials are held only in local variables during the session
###############################################################################

### wd-rac project paths
declare WD_RAC_PROJECT_DIR="${HOME}/wd-rac"
declare WD_RAC_ADMIN_CONF="${WD_RAC_PROJECT_DIR}/conf/rac-master.conf.enc"
declare WD_RAC_HAMSCI_CONF="${WD_RAC_PROJECT_DIR}/conf/rac-hamsci-master.conf.enc"
declare WD_RAC_REPO_URL="git@github.com:rrobinett/wd-rac.git"  # Update with actual repo

### In-memory RAC data (populated from encrypted config)
declare -a WD_RAC_IDS=()
declare -a WD_RAC_SITES=()
declare -a WD_RAC_USERS=()
declare -a WD_RAC_ACCOUNTS=()
declare -a WD_RAC_PASSWORDS=()
declare -a WD_RAC_DESCRIPTIONS=()
declare WD_RAC_COUNT=0
declare WD_RAC_LOADED=0
declare WD_RAC_LAST_INDEX=""

###############################################################################
### Encryption/Decryption (NO CACHING - security requirement)
###############################################################################

### Detect openssl capabilities
openssl_supports_pbkdf2() {
    openssl enc -aes-256-cbc -pbkdf2 -nosalt -k test -in /dev/null 2>/dev/null
    return $?
}

### Decrypt data with passphrase - returns plaintext to stdout
decrypt_data() {
    local passphrase="$1"
    local ciphertext="$2"
    
    if openssl_supports_pbkdf2; then
        echo "${ciphertext}" | openssl enc -aes-256-cbc -d -salt -pbkdf2 -iter 100000 -base64 -pass "pass:${passphrase}" 2>/dev/null
    else
        echo "${ciphertext}" | openssl enc -aes-256-cbc -d -salt -base64 -pass "pass:${passphrase}" 2>/dev/null
    fi
}

### Decrypt a config file - returns plaintext to stdout
decrypt_conf_file() {
    local conf_file=$1
    local passphrase=$2
    
    [[ ! -f "${conf_file}" ]] && return 1
    
    local ciphertext
    ciphertext=$(<"${conf_file}")
    
    local plaintext
    plaintext=$(decrypt_data "${passphrase}" "${ciphertext}")
    
    if [[ -z "${plaintext}" ]]; then
        return 1
    fi
    
    echo "${plaintext}"
    return 0
}

###############################################################################
### INI Config Parser (matches ssr.sh v3.8 format)
###############################################################################

### Parse INI content and populate in-memory arrays
### INI format:
###   [rac_id]
###   site = CALLSIGN
###   user = username
###   account = userid
###   password = secret
###   description = Site description
parse_ini_content() {
    local content="$1"
    
    local current_rac_id=""
    local current_site=""
    local current_user=""
    local current_account=""
    local current_password=""
    local current_description=""
    
    ### Function to save current entry
    save_entry() {
        if [[ -n "${current_rac_id}" && -n "${current_site}" ]]; then
            WD_RAC_IDS+=("${current_rac_id}")
            WD_RAC_SITES+=("${current_site}")
            WD_RAC_USERS+=("${current_user}")
            WD_RAC_ACCOUNTS+=("${current_account}")
            WD_RAC_PASSWORDS+=("${current_password}")
            WD_RAC_DESCRIPTIONS+=("${current_description}")
            ((WD_RAC_COUNT++))
            (( ${verbosity-0} > 1 )) && echo "[DEBUG] Added RAC ${current_rac_id}: ${current_site}" >&2
        fi
    }
    
    while IFS= read -r line || [[ -n "$line" ]]; do
        ### Skip comments and empty lines
        [[ "${line}" =~ ^[[:space:]]*# ]] && continue
        [[ "${line}" =~ ^[[:space:]]*$ ]] && continue
        
        ### Section header [rac_id]
        if [[ "${line}" =~ ^\[([0-9]+)\] ]]; then
            save_entry
            current_rac_id="${BASH_REMATCH[1]}"
            current_site=""
            current_user=""
            current_account=""
            current_password=""
            current_description=""
            continue
        fi
        
        ### Key = value pairs
        if [[ "${line}" =~ ^[[:space:]]*([a-z_]+)[[:space:]]*=[[:space:]]*(.*) ]]; then
            local key="${BASH_REMATCH[1]}"
            local value="${BASH_REMATCH[2]}"
            ### Trim trailing whitespace
            value="${value%"${value##*[![:space:]]}"}"
            
            case "${key}" in
                site)        current_site="${value}" ;;
                user)        current_user="${value}" ;;
                account)     current_account="${value}" ;;
                password)    current_password="${value}" ;;
                description) current_description="${value}" ;;
            esac
        fi
    done <<< "${content}"
    
    ### Save last entry
    save_entry
}

###############################################################################
### wd-rac Project Installation Check
###############################################################################

### Check if wd-rac project is installed and has encrypted configs
check_wd_rac_installed() {
    if [[ ! -d "${WD_RAC_PROJECT_DIR}" ]]; then
        echo "ERROR: wd-rac project not found at ${WD_RAC_PROJECT_DIR}"
        echo ""
        echo "To install, run:"
        echo "  cd ~"
        echo "  git clone ${WD_RAC_REPO_URL}"
        echo ""
        echo "Then ensure the encrypted config files are in place:"
        echo "  ${WD_RAC_ADMIN_CONF}"
        return 1
    fi
    
    if [[ ! -f "${WD_RAC_ADMIN_CONF}" ]]; then
        echo "ERROR: Encrypted RAC config not found: ${WD_RAC_ADMIN_CONF}"
        echo ""
        echo "The wd-rac project is installed, but the encrypted admin config is missing."
        echo "Contact your admin to obtain the encrypted config files."
        return 1
    fi
    
    return 0
}

###############################################################################
### Load RAC Configuration (interactive passphrase, NO caching)
###############################################################################

### Load encrypted RAC configs - prompts for passphrase each time
### SECURITY: Passphrase and decrypted content are NEVER cached
load_rac_configs() {
    ### Check if already loaded this session
    if [[ ${WD_RAC_LOADED} -eq 1 ]]; then
        return 0
    fi
    
    ### Verify wd-rac installation
    if ! check_wd_rac_installed; then
        return 1
    fi
    
    ### Prompt for passphrase (no echo)
    local passphrase
    echo "" >&2
    echo "=== RAC Configuration Access ===" >&2
    echo "Enter passphrase to decrypt RAC credentials." >&2
    echo "(Passphrase is NOT cached for security on gateway servers)" >&2
    read -sp "Passphrase: " passphrase >&2
    echo "" >&2
    
    if [[ -z "${passphrase}" ]]; then
        echo "ERROR: No passphrase entered" >&2
        return 1
    fi
    
    ### Clear any existing data
    WD_RAC_IDS=()
    WD_RAC_SITES=()
    WD_RAC_USERS=()
    WD_RAC_ACCOUNTS=()
    WD_RAC_PASSWORDS=()
    WD_RAC_DESCRIPTIONS=()
    WD_RAC_COUNT=0
    
    ### Decrypt and parse admin config
    local content
    content=$(decrypt_conf_file "${WD_RAC_ADMIN_CONF}" "${passphrase}")
    
    if [[ -z "${content}" ]]; then
        echo "ERROR: Failed to decrypt RAC config (wrong passphrase?)" >&2
        ### Clear passphrase from memory
        passphrase=""
        return 1
    fi
    
    (( ${verbosity-0} )) && echo "Decrypted admin RAC config" >&2
    parse_ini_content "${content}"
    
    ### Also try HamSCI config with same passphrase
    if [[ -f "${WD_RAC_HAMSCI_CONF}" ]]; then
        content=$(decrypt_conf_file "${WD_RAC_HAMSCI_CONF}" "${passphrase}")
        if [[ -n "${content}" ]]; then
            (( ${verbosity-0} )) && echo "Decrypted HamSCI RAC config" >&2
            parse_ini_content "${content}"
        fi
    fi
    
    ### Clear passphrase and content from memory
    passphrase=""
    content=""
    
    if [[ ${WD_RAC_COUNT} -eq 0 ]]; then
        echo "ERROR: No RAC entries found in config" >&2
        return 1
    fi
    
    echo "Loaded ${WD_RAC_COUNT} RAC entries" >&2
    WD_RAC_LOADED=1
    return 0
}

### Find RAC index by ID
find_rac_by_id() {
    local wanted_id=$1
    local i
    
    for (( i=0; i < WD_RAC_COUNT; i++ )); do
        if [[ "${WD_RAC_IDS[$i]}" == "${wanted_id}" ]]; then
            echo "$i"
            return 0
        fi
    done
    
    echo "-1"
    return 1
}

function wd-sshd-conf-add-sssftponly-tests() {
    # Check if sftponly group configuration already exists in sshd_config
    if grep -q "^Match Group sftponly" /etc/ssh/sshd_config; then
        echo "SFTP-only configuration already exists in sshd_config"
    else
        echo "Adding SFTP-only configuration to sshd_config"

        # Backup the original config
        if ! sudo cp /etc/ssh/sshd_config /etc/ssh/sshd_config.backup.$(date +%Y%m%d_%H%M%S); then
            echo "ERROR: Failed to backup sshd_config"
            return 1
        fi

        # Add the SFTP configuration
        if ! sudo tee -a /etc/ssh/sshd_config << 'EOF'

# Default: SFTP-only access for sftponly group
Match Group sftponly
    ChrootDirectory /home/%u
    ForceCommand internal-sftp
    AllowTcpForwarding no
    X11Forwarding no
    PasswordAuthentication no

# Override: Allow SSH for admin users (modify as needed)
Match User root
    ChrootDirectory none
    ForceCommand none
    AllowTcpForwarding yes
    X11Forwarding yes
EOF
        then
            echo "ERROR: Failed to add SFTP configuration to sshd_config"
            return 1
        fi

        # Test the configuration before restarting
        if ! sudo sshd -t; then
            echo "ERROR: Invalid sshd_config syntax. Restoring backup."
            sudo cp /etc/ssh/sshd_config.backup.$(date +%Y%m%d_%H%M%S | head -1) /etc/ssh/sshd_config
            return 1
        fi

        # Restart SSH service
        if ! sudo systemctl restart ssh; then
            echo "ERROR: Failed to restart SSH service"
            return 1
        fi

        echo "Successfully added SFTP-only configuration and restarted SSH"
    fi
}


function add-rsync-client() {
    local client="$1"
    local pubkey="$2"
    local base="/srv/wd-uploads"

    if [[ -z "$client" || -z "$pubkey" ]]; then
        echo "Usage: add_rsync_client <client_name> <ssh_pubkey_file>"
        return 1
    fi

    # Sanitize client name for Linux username (replace / with _)
    local sanitized_client=$(echo "$client" | tr '/' '_')

    # Create client system user (no shell, no password)
    sudo useradd -m -d "$base/$sanitized_client" -s /usr/sbin/nologin "$sanitized_client"

    # Create upload directory
    sudo mkdir -p "$base/$sanitized_client"
    sudo chown "$sanitized_client:$sanitized_client" "$base/$sanitized_client"
    sudo chmod 750 "$base/$sanitized_client"

    # Setup SSH directory for the client
    sudo -u "$sanitized_client" mkdir -p "$base/$sanitized_client/.ssh"
    sudo chmod 700 "$base/$sanitized_client/.ssh"

    # Install client public key with rsync-only restriction
    sudo bash -c "echo 'command=\"/usr/bin/rsync --server --sender -logDtprze.iLsfxC .\" $(cat "$pubkey")' >> $base/$sanitized_client/.ssh/authorized_keys"

    sudo chmod 600 "$base/$sanitized_client/.ssh/authorized_keys"
    sudo chown -R "$sanitized_client:$sanitized_client" "$base/$sanitized_client/.ssh"

    echo "✅ Client '$client' (username: '$sanitized_client') added."
    echo "Upload path: rsync -avz file.tar $sanitized_client@$(hostname -f):$base/$sanitized_client/"
}

### ================= Setup a RAC client to upload WD spot and noise files =======================

###  Returns 0 if arg is an unsigned integer, else 1
function is_int()  { if [[ "$1" =~ ^-?[0-9]+$ ]]; then return 0; else return 1; fi }
function is_uint() { if [[ "$1" =~   ^[0-9]+$ ]]; then return 0; else return 1; fi }

### Given:   RAC_ID
### Returns: that server's WD user and password
### NOTE: This now uses encrypted config from wd-rac project
function wd-ssr-client-lookup()
{
    local -n __client_user_name=$1
    local -n __client_user_password=$2
    local rac_id="${3-}"
    
    if ! is_uint ${rac_id}; then
        echo "ERROR: the RAC argument '${rac_id}' is not an unsigned integer"
        return 1
    fi

    ### Load encrypted RAC configs (prompts for passphrase if not already loaded)
    if ! load_rac_configs; then
        echo "ERROR: Failed to load RAC configuration"
        return 1
    fi

    ### Find RAC by ID
    local rac_index
    rac_index=$(find_rac_by_id "${rac_id}")
    
    if [[ "${rac_index}" == "-1" ]]; then
        echo "ERROR: can't find RAC ${rac_id} in encrypted config"
        return 1
    fi

    (( ${verbosity-0} > 1 )) && echo "Found RAC_ID=${rac_id} at index ${rac_index}: ${WD_RAC_SITES[${rac_index}]}"

    ### Get user and password from the loaded config
    local rac_client_user_name="${WD_RAC_USERS[${rac_index}]}"
    local rac_client_user_password="${WD_RAC_PASSWORDS[${rac_index}]}"
    
    ### If user is empty, default to account field
    if [[ -z "${rac_client_user_name}" ]]; then
        rac_client_user_name="${WD_RAC_ACCOUNTS[${rac_index}]}"
    fi

    (( ${verbosity-0} > 1 )) && echo "Found that RAC_ID=${rac_id} Linux client's user name is '${rac_client_user_name}'"

    __client_user_name="${rac_client_user_name}"
    __client_user_password="${rac_client_user_password}"

    ### Save index for display function
    WD_RAC_LAST_INDEX="${rac_index}"

    return 0
}

### Display RAC login credentials (call after wd-ssr-client-lookup)
### Also prompts for password if not stored in config
### Sets: WD_RAC_SESSION_PASSWORD for use with sshpass
declare WD_RAC_SESSION_PASSWORD=""

function display_rac_credentials() {
    local rac_id=$1
    
    if [[ -z "${WD_RAC_LAST_INDEX:-}" || "${WD_RAC_LAST_INDEX}" == "-1" ]]; then
        return 1
    fi
    
    local idx="${WD_RAC_LAST_INDEX}"
    local site="${WD_RAC_SITES[${idx}]}"
    local description="${WD_RAC_DESCRIPTIONS[${idx}]}"
    local account="${WD_RAC_ACCOUNTS[${idx}]}"
    local password="${WD_RAC_PASSWORDS[${idx}]}"
    
    echo ""
    echo "=== RAC #${rac_id} Login Credentials ==="
    echo "  Site:        ${site}"
    [[ -n "${description}" ]] && echo "  Description: ${description}"
    echo "  Account:     ${account}"
    
    if [[ -n "${password}" ]]; then
        echo "  Password:    ${password}"
        WD_RAC_SESSION_PASSWORD="${password}"
    else
        echo "  Password:    (not stored in config)"
        echo ""
        echo "No password stored for this RAC. Please enter the login password:"
        read -sp "Password for ${account}@RAC#${rac_id}: " WD_RAC_SESSION_PASSWORD
        echo ""
    fi
    echo "==========================================="
    echo ""
}

### Check if sshpass is available, install if needed
check_sshpass() {
    if command -v sshpass &>/dev/null; then
        return 0
    fi
    
    echo "sshpass is required but not installed."
    echo "Installing sshpass..."
    if sudo apt-get install -y sshpass &>/dev/null; then
        echo "sshpass installed successfully."
        return 0
    else
        echo "ERROR: Failed to install sshpass. Please install it manually:"
        echo "  sudo apt-get install sshpass"
        return 1
    fi
}

### SSH wrapper that uses sshpass when password is available
### Usage: ssh_with_pass [ssh options] user@host "command"
ssh_with_pass() {
    if [[ -n "${WD_RAC_SESSION_PASSWORD}" ]]; then
        sshpass -p "${WD_RAC_SESSION_PASSWORD}" ssh ${SSH_OPTS} "$@"
    else
        ssh ${SSH_OPTS} "$@"
    fi
}

### SCP wrapper that uses sshpass when password is available
### Usage: scp_with_pass [scp options] source dest
scp_with_pass() {
    if [[ -n "${WD_RAC_SESSION_PASSWORD}" ]]; then
        sshpass -p "${WD_RAC_SESSION_PASSWORD}" scp ${SSH_OPTS} "$@"
    else
        scp ${SSH_OPTS} "$@"
    fi
}

###############################################################################
### GW2 Connection Detection
###############################################################################

### Network addresses for RAC access (all go through GW2)
declare GW2_MESH_IP="10.112.0.2"
declare WD_RAC_VPN_IP="10.111.220.1"
declare RAC_TEST_ID=61              # RAC ID used for connectivity testing
declare RAC_BASE_PORT=35800

### SSH options for connecting to RAC clients
### - StrictHostKeyChecking=accept-new: auto-accept new hosts, reject changed keys
### - ConnectTimeout=10: don't hang forever on unreachable hosts
declare SSH_OPTS="-o StrictHostKeyChecking=accept-new -o ConnectTimeout=10"

### Connection state
declare WD_RAC_SERVER=""
declare WD_RAC_CONNECTION_PATH=""

### Determine upload server FQDN based on where we're running
### If on gw1 or gw2, use that server's FQDN; otherwise default to gw1
determine_server_fqdn() {
    local hostname=$(hostname -s | tr '[:upper:]' '[:lower:]')
    
    case "${hostname}" in
        gw1|gw1-*)
            echo "gw1.wsprdaemon.org"
            ;;
        gw2|gw2-*)
            echo "gw2.wsprdaemon.org"
            ;;
        *)
            # Default to gw1 if running elsewhere
            echo "gw1.wsprdaemon.org"
            ;;
    esac
}

WD_SERVER_FQDN=$(determine_server_fqdn)

### Test if a port is open
test_port() {
    local host=$1
    local port=$2
    local timeout=${3:-2}
    
    ### Try nc with timeout
    if nc -z -w ${timeout} "${host}" "${port}" 2>/dev/null; then
        return 0
    fi
    return 1
}

### Find the best path to reach GW2/RAC ports
### Sets: WD_RAC_SERVER, WD_RAC_CONNECTION_PATH
find_gw2_connection() {
    local test_port_num=$((RAC_BASE_PORT + RAC_TEST_ID))
    
    ### 1. Try localhost (running on GW2)
    (( ${verbosity-0} > 1 )) && echo "Testing localhost:${test_port_num}..." >&2
    if test_port localhost "${test_port_num}" 1; then
        WD_RAC_SERVER="localhost"
        WD_RAC_CONNECTION_PATH="localhost (on GW2)"
        (( ${verbosity-0} )) && echo "Using connection path: ${WD_RAC_CONNECTION_PATH}" >&2
        return 0
    fi
    
    ### 2. Try GW2 via wg-mesh VPN
    (( ${verbosity-0} > 1 )) && echo "Testing ${GW2_MESH_IP}:${test_port_num}..." >&2
    if test_port "${GW2_MESH_IP}" "${test_port_num}" 2; then
        WD_RAC_SERVER="${GW2_MESH_IP}"
        WD_RAC_CONNECTION_PATH="gw2 via wg-mesh (${GW2_MESH_IP})"
        (( ${verbosity-0} )) && echo "Using connection path: ${WD_RAC_CONNECTION_PATH}" >&2
        return 0
    fi
    
    ### 3. Try GW2 via wd-rac VPN
    (( ${verbosity-0} > 1 )) && echo "Testing ${WD_RAC_VPN_IP}:${test_port_num}..." >&2
    if test_port "${WD_RAC_VPN_IP}" "${test_port_num}" 2; then
        WD_RAC_SERVER="${WD_RAC_VPN_IP}"
        WD_RAC_CONNECTION_PATH="gw2 via wd-rac VPN (${WD_RAC_VPN_IP})"
        (( ${verbosity-0} )) && echo "Using connection path: ${WD_RAC_CONNECTION_PATH}" >&2
        return 0
    fi
    
    ### No path found
    WD_RAC_SERVER=""
    WD_RAC_CONNECTION_PATH=""
    return 1
}

### Ensure we have a valid connection to GW2
ensure_gw2_connection() {
    if [[ -n "${WD_RAC_SERVER}" ]]; then
        return 0
    fi
    
    if ! find_gw2_connection; then
        echo "ERROR: Cannot reach GW2/RAC servers"
        echo ""
        echo "Tried:"
        echo "  - localhost:$((RAC_BASE_PORT + RAC_TEST_ID)) (on GW2)"
        echo "  - ${GW2_MESH_IP}:$((RAC_BASE_PORT + RAC_TEST_ID)) (wg-mesh VPN)"
        echo "  - ${WD_RAC_VPN_IP}:$((RAC_BASE_PORT + RAC_TEST_ID)) (wd-rac VPN)"
        echo ""
        echo "Check your VPN connection or run this script on GW2."
        return 1
    fi
    
    return 0
}

# List of gateway servers whose keys should be removed from client known_hosts
# This is needed when gateway servers are rebuilt and get new SSH host keys
declare -a GW_SERVERS=("gw1" "gw2" "gw1.wsprdaemon.org" "gw2.wsprdaemon.org")

function wd-remove-gw-keys-from-client() {
    local client_ip_port=$1
    local client_user=$2
    
    (( ${verbosity-0} )) && echo "Removing stale GW1/GW2 host keys from client's known_hosts file"
    
    # Build the ssh-keygen -R commands for each gateway server
    local keygen_cmds=""
    for gw in "${GW_SERVERS[@]}"; do
        keygen_cmds+="ssh-keygen -R ${gw} 2>/dev/null || true; "
        (( ${verbosity-0} > 1 )) && echo "  Will remove: ${gw}"
    done
    
    # Also backup the known_hosts file before modification
    keygen_cmds="cp ~/.ssh/known_hosts ~/.ssh/known_hosts.backup.\$(date +%Y%m%d_%H%M%S) 2>/dev/null || true; ${keygen_cmds}"
    
    # Execute on the client
    if ssh_with_pass -p "${client_ip_port}" "${client_user}@${WD_RAC_SERVER}" "${keygen_cmds} exit 0"; then
        (( ${verbosity-0} )) && echo "Successfully removed GW1/GW2 keys from client's known_hosts"
    else
        echo "Warning: Could not remove GW keys from client (this is usually okay if they weren't present)"
    fi
    
    return 0
}

function wd-client-to-server-setup()
{
    local client_rac=$1
    local client_user=${2-wsprdaemon}
    local rc

    # Validate input BEFORE using it in arithmetic
    if ! is_uint "${client_rac}" ; then
        echo "ERROR: this RAC number argument to this function '${client_rac}' is empty or not an unsigned integer"
        echo "Usage: wd-register-client.sh <RAC_NUMBER>"
        echo "Example: wd-register-client.sh 4"
        return 1
    fi

    # Ensure we have a valid connection to GW2
    if ! ensure_gw2_connection; then
        return 1
    fi
    echo "Connecting via: ${WD_RAC_CONNECTION_PATH}"
    echo "Upload server:  ${WD_SERVER_FQDN}"

    set -u
    local client_ip_port=$(( 35800 + client_rac ))

    local client_user_password
    local client_user_name
    wd-ssr-client-lookup "client_user_name" "client_user_password" ${client_rac}
    rc=$? ; if (( rc )); then
        echo "ERROR: can't find user with RAC ${client_rac} in encrypted config"
        return 1
    fi

    ### Override client_user with the name from the SSR config (e.g. 'alan' for RAC 161)
    ### The command-line arg is only a fallback when the config has no user field.
    if [[ -n "${client_user_name}" ]]; then
        if [[ "${client_user_name}" != "${client_user}" ]]; then
            echo "Using SSR config login user '${client_user_name}' (not default '${client_user}')"
        fi
        client_user="${client_user_name}"
    fi

    ### Display the RAC credentials and get password (prompts if not in config)
    display_rac_credentials ${client_rac}
    
    ### Check sshpass is available (needed for password auth)
    if ! check_sshpass; then
        return 1
    fi

    (( ${verbosity-0} > 1 )) && echo "Testing to see if RAC client has already been setup so we can autologin"

    nc -z ${WD_RAC_SERVER} ${client_ip_port} >/dev/null 2>&1
    rc=$? ; if (( rc )); then
        echo "ERROR: Can't open a connection to ${WD_RAC_SERVER}:${client_ip_port} for RAC ${client_rac}, so we can't get this client's upload info"
        return 1
    fi

    (( ${verbosity-0} )) && echo "netcat opened a connection to ${WD_RAC_SERVER}:${client_ip_port} for RAC ${client_rac}"

    (( ${verbosity-0} )) && echo "Get a copy of that server's public key so it can login here on this server"
    if ! scp_with_pass -P ${client_ip_port} ${client_user}@${WD_RAC_SERVER}:~/.ssh/id_*.pub  /tmp/rac_${client_rac}_key.pub > /dev/null 2>&1 ; then
        echo "ERROR: Can't scp a copy of that RAC's public key file, so we can't copy it to this server"
        return 1
    fi
    (( ${verbosity-0} )) && echo "A copy of RAC#${client_rac} pub file has been saved in /tmp/rac_${client_rac}_key.pub"

    # Remove any stale GW1/GW2 host keys from the client's known_hosts
    wd-remove-gw-keys-from-client "${client_ip_port}" "${client_user}"

    (( ${verbosity-0} )) && echo "Learn that server's most recent wsprnet.org REPORTER_ID so we can use it as the linux account name on this server"
    local newest_log_file_path=$(ssh_with_pass -p ${client_ip_port} ${client_user}@${WD_RAC_SERVER} 'find ${PWD} -type f -name "upload_to_wsprnet_daemon.log" -printf "%T@ %p\n" | sort -n | tail -1 | cut -d" " -f2-')    
    if [[ -z "${newest_log_file_path}" ]]; then
        echo "ERROR: couldn't find 'upload_to_wsprnet_daemon.log' on RAC#${client_rac}"
        return 1
    fi
    (( ${verbosity-0} )) && echo "Found the wsprnet.org log file path on that server is ${newest_log_file_path}"

    local ssh_cmd=$(printf "awk '/Uploading/ {line = \$0} END { split(line, f); print f[9] }' %s" "${newest_log_file_path}")
    local client_reporter_id=$(ssh_with_pass -p ${client_ip_port} ${client_user}@${WD_RAC_SERVER} "${ssh_cmd}")

    if [[ -z "${client_reporter_id}" ]]; then
        echo "Can't find a  REPORTER_ID from that RAC's ${newest_log_file_path}"
        return 1
    fi
    (( ${verbosity-0} )) && echo "Found the most recent REPORTER_ID is ${client_reporter_id}.  See that value is declared in the RAC's wsprdaemon.conf"
    
    # CRITICAL FIX: Sanitize the reporter ID for use as a Linux username
    # Replace forward slashes with underscores (e.g., "KFS/SW" becomes "KFS_SW")
    local sanitized_reporter_id=$(echo "${client_reporter_id}" | tr '/' '_')
    
    if [[ "${client_reporter_id}" != "${sanitized_reporter_id}" ]]; then
        echo "NOTE: Reporter ID '${client_reporter_id}' contains invalid characters for Linux username"
        echo "      Using sanitized username: '${sanitized_reporter_id}'"
    fi
    
    # Build the WD_SERVER_USER_LIST array - gw1 is ALWAYS first
    local wd_server_user_list="WD_SERVER_USER_LIST=(\"${sanitized_reporter_id}@gw1.wsprdaemon.org\" \"${sanitized_reporter_id}@gw2.wsprdaemon.org\")"
    echo "Setting client config: ${wd_server_user_list}"
    
    local config_file="\${HOME}/wsprdaemon/wsprdaemon.conf"
    # Remove old WD_SERVER_USER scalar if present, and old WD_SERVER_USER_LIST
    # Then add the new WD_SERVER_USER_LIST array
    ssh_with_pass -p "${client_ip_port}" "${client_user}@${WD_RAC_SERVER}" \
        "sed -i '/^[[:space:]]*WD_SERVER_USER=/d' \"${config_file}\"; \
         sed -i '/^[[:space:]]*WD_SERVER_USER_LIST=/d' \"${config_file}\"; \
         echo '${wd_server_user_list}  # Added by the WD server. Do not modify or delete' >> \"${config_file}\""
    
    # Verify it was added
    ssh_with_pass -p "${client_ip_port}" "${client_user}@${WD_RAC_SERVER}" "grep -q '^WD_SERVER_USER_LIST=' \"${config_file}\""
    rc=$?; if (( rc )); then
        echo "ERROR: failed to add WD_SERVER_USER_LIST to ${config_file} on RAC #${client_rac}"
        return 1
    fi
    (( ${verbosity-0} )) && echo "Added WD_SERVER_USER_LIST to ${config_file} on RAC #${client_rac}"

    # Create sftponly group if it doesn't exist
    if ! getent group sftponly > /dev/null 2>&1; then
        if ! sudo groupadd sftponly; then
            echo "ERROR: Failed to create sftponly group"
            return 1
        fi
        echo "Created sftponly group"
    else
        (( ${verbosity-0} )) && echo "sftponly group already exists"
    fi
    ### If needed, modify the sshd to restrict the sftponly group 
    wd-sshd-conf-add-sssftponly-tests

    # Create the user account (no password, no shell) - USE SANITIZED USERNAME
    if id "${sanitized_reporter_id}" > /dev/null 2>&1; then
        echo "User '${sanitized_reporter_id}' with id '$(id "${sanitized_reporter_id}" )' already exists on this server, so no need to create that user account"
    else
        if ! sudo useradd -m -s /bin/false -G sftponly "${sanitized_reporter_id}"; then
            echo "ERROR: Failed to create user '${sanitized_reporter_id}'"
            return 1
        fi
        echo "Created new user '${sanitized_reporter_id}' (original reporter ID: '${client_reporter_id}')"
        
        # CRITICAL FIX: For chroot SFTP, the home directory MUST be owned by root
        # and NOT writable by the user. This is an OpenSSH security requirement.
        if ! sudo chown root:root "/home/${sanitized_reporter_id}"; then
            echo "ERROR: Failed to set root ownership on home directory"
            return 1
        fi
        
        if ! sudo chmod 755 "/home/${sanitized_reporter_id}"; then
            echo "ERROR: Failed to set permissions on home directory"
            return 1
        fi
        
        echo "Set proper chroot ownership (root:root) on /home/${sanitized_reporter_id}"
    fi

    # Create the SSH directory and set proper permissions - USE SANITIZED USERNAME
    if ! sudo mkdir -p "/home/${sanitized_reporter_id}/.ssh"; then
        echo "ERROR: Failed to create .ssh directory"
        return 1
    fi

    if ! sudo chmod 700 "/home/${sanitized_reporter_id}/.ssh"; then
        echo "ERROR: Failed to set permissions on .ssh directory"
        return 1
    fi

    # Get the client's public key and add it to authorized_keys - USE SANITIZED USERNAME
    local -a client_key_list
    mapfile -t client_key_list < <(ssh_with_pass -p "${client_ip_port}" "${client_user}@${WD_RAC_SERVER}" "cat ~/.ssh/*.pub")
    if (( ${#client_key_list[@]} == 0 )); then
        echo "ERROR: can't find a public key on client"
        return 1
    fi
    echo "Got ${#client_key_list[@]} client public keys. Saving the first key to /home/${sanitized_reporter_id}/.ssh/authorized_keys"
    if sudo grep -q "${client_key_list[0]}" "/home/${sanitized_reporter_id}/.ssh/authorized_keys" 2>/dev/null; then
        echo "This client's public key is already in its /home/${sanitized_reporter_id}/.ssh/authorized_key"
    else
        if ! echo "${client_key_list[0]}" | sudo tee "/home/${sanitized_reporter_id}/.ssh/authorized_keys" > /dev/null; then
            echo "Failed to add this client's public key to its /home/${sanitized_reporter_id}/.ssh/authorized_key on this server"
            return 1
        fi
        echo "Added this client's public key to its /home/${sanitized_reporter_id}/.ssh/authorized_key on this server"
    fi

    # Set proper permissions on authorized_keys - USE SANITIZED USERNAME
    if ! sudo chmod 600 "/home/${sanitized_reporter_id}/.ssh/authorized_keys"; then
        echo "ERROR: Failed to set permissions on authorized_keys"
        return 1
    fi

    # CRITICAL: .ssh directory must be owned by the USER, not root - USE SANITIZED USERNAME
    # Only the chroot directory (home) needs to be root-owned
    # The SSH daemon needs to read authorized_keys as the user
    if ! sudo chown -R "${sanitized_reporter_id}:${sanitized_reporter_id}" "/home/${sanitized_reporter_id}/.ssh"; then
        echo "ERROR: Failed to set ownership of .ssh directory"
        return 1
    fi

    # Lock the account (disable password login) - USE SANITIZED USERNAME
    if ! sudo passwd -l "${sanitized_reporter_id}"; then
        echo "ERROR: Failed to lock user account"
        return 1
    fi

    # Create uploads directory - THIS directory can be owned by the user - USE SANITIZED USERNAME
    if ! sudo mkdir -p "/home/${sanitized_reporter_id}/uploads"; then
        echo "ERROR: Failed to create uploads directory"
        return 1
    fi

    if ! sudo chown "${sanitized_reporter_id}:${sanitized_reporter_id}" "/home/${sanitized_reporter_id}/uploads"; then
        echo "ERROR: Failed to set ownership of uploads directory"
        return 1
    fi

    if ! sudo chmod 755 "/home/${sanitized_reporter_id}/uploads"; then
        echo "ERROR: Failed to set permissions on uploads directory"
        return 1
    fi
    echo "User ${sanitized_reporter_id}'s Linux account is set up on this server (${WD_SERVER_FQDN})"

    ###########################################################################
    ### Create duplicate user on peer gateway server
    ###########################################################################
    local peer_gateway=""
    local peer_fqdn=""
    local local_hostname=$(hostname -s | tr '[:upper:]' '[:lower:]')
    
    case "${local_hostname}" in
        gw1|gw1-*)
            peer_gateway="gw2"
            peer_fqdn="gw2.wsprdaemon.org"
            ;;
        gw2|gw2-*)
            peer_gateway="gw1"
            peer_fqdn="gw1.wsprdaemon.org"
            ;;
    esac
    
    if [[ -n "${peer_gateway}" ]]; then
        echo ""
        echo "Creating duplicate user on peer gateway: ${peer_fqdn}"
        
        # Get the client's public key that we already retrieved
        local client_pubkey="${client_key_list[0]}"
        
        # Create user on peer gateway via SSH
        # This assumes wsprdaemon user on local can SSH to peer gateway
        if ssh -o StrictHostKeyChecking=accept-new -o ConnectTimeout=10 "${peer_gateway}" "
            # Create sftponly group if needed
            if ! getent group sftponly > /dev/null 2>&1; then
                sudo groupadd sftponly
            fi
            
            # Create user if not exists
            if ! id '${sanitized_reporter_id}' > /dev/null 2>&1; then
                sudo useradd -m -s /bin/false -G sftponly '${sanitized_reporter_id}'
                sudo chown root:root '/home/${sanitized_reporter_id}'
                sudo chmod 755 '/home/${sanitized_reporter_id}'
            fi
            
            # Setup SSH directory and key
            sudo mkdir -p '/home/${sanitized_reporter_id}/.ssh'
            sudo chmod 700 '/home/${sanitized_reporter_id}/.ssh'
            echo '${client_pubkey}' | sudo tee '/home/${sanitized_reporter_id}/.ssh/authorized_keys' > /dev/null
            sudo chmod 600 '/home/${sanitized_reporter_id}/.ssh/authorized_keys'
            sudo chown -R '${sanitized_reporter_id}:${sanitized_reporter_id}' '/home/${sanitized_reporter_id}/.ssh'
            sudo passwd -l '${sanitized_reporter_id}'
            
            # Create uploads directory
            sudo mkdir -p '/home/${sanitized_reporter_id}/uploads'
            sudo chown '${sanitized_reporter_id}:${sanitized_reporter_id}' '/home/${sanitized_reporter_id}/uploads'
            sudo chmod 755 '/home/${sanitized_reporter_id}/uploads'
            
            echo 'User setup complete on peer'
        " 2>/dev/null; then
            echo "User ${sanitized_reporter_id} created on ${peer_fqdn}"
            
            # Test SFTP upload to peer gateway
            echo "Testing SFTP upload: RAC client -> ${peer_fqdn} as ${sanitized_reporter_id}"
            local peer_test_file="sftp_peer_test_$(date +%s).txt"
            local peer_test_content="SFTP peer test at $(date)"
            
            if ssh_with_pass -p "${client_ip_port}" "${client_user}@${WD_RAC_SERVER}" \
                "echo '${peer_test_content}' > /tmp/${peer_test_file} && \
                 sftp -o StrictHostKeyChecking=accept-new -P ${SFTP_PORT:-22} ${sanitized_reporter_id}@${peer_fqdn} <<< 'put /tmp/${peer_test_file} uploads/${peer_test_file}' && \
                 rm -f /tmp/${peer_test_file}"; then
                
                # Verify file arrived on peer (check via SSH to peer)
                # Note: need sudo for rm since file is owned by the SFTP user, not wsprdaemon
                if ssh -o StrictHostKeyChecking=accept-new -o ConnectTimeout=10 "${peer_gateway}" \
                    "test -f '/home/${sanitized_reporter_id}/uploads/${peer_test_file}' && sudo rm -f '/home/${sanitized_reporter_id}/uploads/${peer_test_file}'" 2>/dev/null; then
                    echo "SUCCESS: SFTP upload test to ${peer_fqdn} passed"
                else
                    echo "WARNING: Could not verify upload on ${peer_fqdn}"
                fi
            else
                echo "WARNING: SFTP upload test to ${peer_fqdn} failed"
                echo "         Client may need to retry upload to ${peer_fqdn} later"
            fi
        else
            echo "WARNING: Could not create user on ${peer_fqdn} (peer may be unreachable)"
            echo "         You may need to manually run this script on ${peer_gateway} later"
        fi
    fi

    ###########################################################################
    ### Test SFTP upload capability
    ###########################################################################
    local test_file="sftp_test_$(date +%s).txt"
    local test_content="SFTP test from ${WD_RAC_SERVER} at $(date)"

    # Create a test file on the remote server and attempt upload
    echo ""
    echo "Testing SFTP upload: RAC client -> ${WD_SERVER_FQDN} as ${sanitized_reporter_id}"
    if ! ssh_with_pass -p "${client_ip_port}" "${client_user}@${WD_RAC_SERVER}" \
        "echo '${test_content}' > /tmp/${test_file} && \
         sftp -o StrictHostKeyChecking=accept-new -P ${SFTP_PORT:-22} ${sanitized_reporter_id}@${WD_SERVER_FQDN} <<< 'put /tmp/${test_file} uploads/${test_file}' && \
         rm -f /tmp/${test_file}"; then
        echo "ERROR: SFTP upload test failed for user ${sanitized_reporter_id}"
        echo "       Client tried: sftp ${sanitized_reporter_id}@${WD_SERVER_FQDN}"
        return 1
    fi

    # Verify the file was uploaded successfully - USE SANITIZED USERNAME
    if [[ ! -f "/home/${sanitized_reporter_id}/uploads/${test_file}" ]]; then
        echo "ERROR: Test file was not found in uploads directory"
        return 1
    fi

    # Verify the file content matches - USE SANITIZED USERNAME
    local uploaded_content=$(cat "/home/${sanitized_reporter_id}/uploads/${test_file}")
    if [[ "${uploaded_content}" != "${test_content}" ]]; then
        echo "ERROR: Uploaded file content does not match expected content"
        return 1
    fi

    # Clean up test file - USE SANITIZED USERNAME
    sudo rm -f "/home/${sanitized_reporter_id}/uploads/${test_file}"

    echo "SUCCESS: SFTP upload test passed for user ${sanitized_reporter_id} (reporter ID: ${client_reporter_id})"
}

###############################################################################
### Manual Setup Mode (no RAC access required)
###############################################################################

### Create user accounts manually when RAC access is not available
### Usage: wd-register-client.sh manual-setup <USERNAME> <SSH_PUBKEY_FILE_OR_STRING>
manual_setup_user() {
    local username="$1"
    local pubkey_input="$2"
    
    # Validate username
    if [[ ! "${username}" =~ ^[A-Z0-9]+$ ]]; then
        echo "ERROR: Username must contain only uppercase letters and numbers"
        echo "       Got: '${username}'"
        return 1
    fi
    
    # Get the public key - either from file or direct string
    local pubkey=""
    if [[ -f "${pubkey_input}" ]]; then
        pubkey=$(<"${pubkey_input}")
    else
        pubkey="${pubkey_input}"
    fi
    
    # Validate it looks like an SSH public key
    if [[ ! "${pubkey}" =~ ^ssh- ]]; then
        echo "ERROR: Public key must start with 'ssh-rsa', 'ssh-ed25519', etc."
        echo "       Got: '${pubkey:0:50}...'"
        return 1
    fi
    
    echo "=== Manual User Setup ==="
    echo "Username: ${username}"
    echo "Public key: ${pubkey:0:60}..."
    echo ""
    
    # Determine local and peer gateways
    local local_hostname=$(hostname -s | tr '[:upper:]' '[:lower:]')
    local local_fqdn=""
    local peer_gateway=""
    local peer_fqdn=""
    
    case "${local_hostname}" in
        gw1|gw1-*)
            local_fqdn="gw1.wsprdaemon.org"
            peer_gateway="gw2"
            peer_fqdn="gw2.wsprdaemon.org"
            ;;
        gw2|gw2-*)
            local_fqdn="gw2.wsprdaemon.org"
            peer_gateway="gw1"
            peer_fqdn="gw1.wsprdaemon.org"
            ;;
        *)
            echo "ERROR: Must run on gw1 or gw2"
            return 1
            ;;
    esac
    
    ###########################################################################
    ### Create user on local gateway
    ###########################################################################
    
    echo "Creating user on local gateway: ${local_fqdn}"
    
    # Check for sftponly group (from wd-client-to-server-setup)
    wd-ensure-sftponly-group
    
    # Create user if doesn't exist
    if id "${username}" > /dev/null 2>&1; then
        echo "User ${username} already exists on local gateway"
    else
        if ! sudo useradd -m -s /bin/false -G sftponly "${username}"; then
            echo "ERROR: Failed to create user ${username}"
            return 1
        fi
        echo "Created user ${username}"
    fi
    
    # Set chroot ownership (must be root:root)
    if ! sudo chown root:root "/home/${username}"; then
        echo "ERROR: Failed to set chroot ownership"
        return 1
    fi
    
    # Create .ssh directory
    if ! sudo mkdir -p "/home/${username}/.ssh"; then
        echo "ERROR: Failed to create .ssh directory"
        return 1
    fi
    
    # Install public key
    if ! echo "${pubkey}" | sudo tee "/home/${username}/.ssh/authorized_keys" > /dev/null; then
        echo "ERROR: Failed to write authorized_keys"
        return 1
    fi
    
    # Set permissions
    if ! sudo chmod 700 "/home/${username}/.ssh"; then
        echo "ERROR: Failed to set .ssh permissions"
        return 1
    fi
    
    if ! sudo chmod 600 "/home/${username}/.ssh/authorized_keys"; then
        echo "ERROR: Failed to set authorized_keys permissions"
        return 1
    fi
    
    if ! sudo chown -R "${username}:${username}" "/home/${username}/.ssh"; then
        echo "ERROR: Failed to set .ssh ownership"
        return 1
    fi
    
    # Lock the account (disable password login)
    if ! sudo passwd -l "${username}" > /dev/null 2>&1; then
        echo "ERROR: Failed to lock user account"
        return 1
    fi
    
    # Create uploads directory
    if ! sudo mkdir -p "/home/${username}/uploads"; then
        echo "ERROR: Failed to create uploads directory"
        return 1
    fi
    
    if ! sudo chown "${username}:${username}" "/home/${username}/uploads"; then
        echo "ERROR: Failed to set uploads ownership"
        return 1
    fi
    
    if ! sudo chmod 755 "/home/${username}/uploads"; then
        echo "ERROR: Failed to set uploads permissions"
        return 1
    fi
    
    echo "User ${username} setup complete on ${local_fqdn}"
    
    ###########################################################################
    ### Create duplicate user on peer gateway
    ###########################################################################
    
    if [[ -n "${peer_gateway}" ]]; then
        echo ""
        echo "Creating duplicate user on peer gateway: ${peer_fqdn}"
        
        if ssh -o StrictHostKeyChecking=accept-new -o ConnectTimeout=10 "${peer_gateway}" "
            # Create sftponly group if needed
            if ! getent group sftponly > /dev/null 2>&1; then
                sudo groupadd sftponly
            fi
            
            # Create user if not exists
            if ! id '${username}' > /dev/null 2>&1; then
                sudo useradd -m -s /bin/false -G sftponly '${username}'
                sudo chown root:root '/home/${username}'
                sudo chmod 755 '/home/${username}'
            fi
            
            # Setup SSH directory and key
            sudo mkdir -p '/home/${username}/.ssh'
            sudo chmod 700 '/home/${username}/.ssh'
            echo '${pubkey}' | sudo tee '/home/${username}/.ssh/authorized_keys' > /dev/null
            sudo chmod 600 '/home/${username}/.ssh/authorized_keys'
            sudo chown -R '${username}:${username}' '/home/${username}/.ssh'
            sudo passwd -l '${username}' > /dev/null 2>&1
            
            # Create uploads directory
            sudo mkdir -p '/home/${username}/uploads'
            sudo chown '${username}:${username}' '/home/${username}/uploads'
            sudo chmod 755 '/home/${username}/uploads'
            
            echo 'User setup complete on peer'
        " 2>/dev/null; then
            echo "User ${username} created on ${peer_fqdn}"
        else
            echo "WARNING: Could not create user on ${peer_fqdn} (peer may be unreachable)"
            echo "         Run this script on ${peer_gateway} to complete setup"
        fi
    fi
    
    ###########################################################################
    ### Output config line for wsprdaemon.conf
    ###########################################################################
    
    echo ""
    echo "=== Setup Complete ==="
    echo ""
    echo "Add this line to the CLIENT's /etc/wsprdaemon/wsprdaemon.conf:"
    echo ""
    echo "WD_SERVER_USER_LIST=(\"${username}@${local_fqdn}\" \"${username}@${peer_fqdn}\")"
    echo ""
    echo "Test SFTP upload from client:"
    echo "  echo 'test' > /tmp/test.txt"
    echo "  sftp ${username}@${local_fqdn} <<< 'put /tmp/test.txt uploads/test.txt'"
    echo ""
}

###############################################################################
### setup-autologin: Walk all RAC ports; install SSH key where autologin
### is not yet working, using the password from the encrypted SSR config.
###############################################################################

### Try to install our SSH public key on a single RAC client using sshpass.
### Returns 0 on success, 1 on failure.
### Sets WD_AUTOLOGIN_RESULT to a human-readable status string.
declare WD_AUTOLOGIN_RESULT=""

install_autologin_key() {
    local client_ip_port="$1"
    local client_user="$2"
    local password="$3"

    ### Find our local public key (prefer ed25519, fall back to ecdsa, rsa)
    local local_pubkey_file=""
    for f in ~/.ssh/id_ed25519.pub ~/.ssh/id_ecdsa.pub ~/.ssh/id_rsa.pub; do
        if [[ -f "$f" ]]; then
            local_pubkey_file="$f"
            break
        fi
    done

    if [[ -z "${local_pubkey_file}" ]]; then
        WD_AUTOLOGIN_RESULT="No local public key found (~/.ssh/id_*.pub)"
        return 1
    fi

    local pubkey
    pubkey=$(<"${local_pubkey_file}")

    local install_cmd="mkdir -p ~/.ssh && chmod 700 ~/.ssh && \
         grep -qxF '${pubkey}' ~/.ssh/authorized_keys 2>/dev/null || \
         echo '${pubkey}' >> ~/.ssh/authorized_keys && \
         chmod 600 ~/.ssh/authorized_keys"

    ### Helper: run ssh with our existing key, no password
    _ssh_keyonly() {
        ssh ${SSH_OPTS} -o BatchMode=yes \
            -o PasswordAuthentication=no \
            -p "${client_ip_port}" \
            "${client_user}@${WD_RAC_SERVER}" \
            "${install_cmd}" 2>&1
    }

    ### First try: use our existing SSH key (no password needed).
    local ssh_err
    ssh_err=$(_ssh_keyonly)
    local ssh_rc=$?

    ### If host key mismatch, clear the stale entry and retry once
    if [[ ${ssh_rc} -ne 0 ]] && echo "${ssh_err}" | grep -q "REMOTE HOST IDENTIFICATION\|Host key verification failed"; then
        ssh-keygen -f "${HOME}/.ssh/known_hosts" -R "[${WD_RAC_SERVER}]:${client_ip_port}" >/dev/null 2>&1
        ssh_err=$(_ssh_keyonly)
        ssh_rc=$?
    fi

    if [[ ${ssh_rc} -eq 0 ]]; then
        WD_AUTOLOGIN_RESULT="Key installed via existing SSH key (${local_pubkey_file})"
        return 0
    fi

    ### Classify the failure reason
    local key_fail_reason="unknown"
    if echo "${ssh_err}" | grep -q "Permission denied\|publickey"; then
        key_fail_reason="pubkey auth rejected"
    elif echo "${ssh_err}" | grep -q "Connection refused\|connect to host"; then
        key_fail_reason="connection refused"
    elif echo "${ssh_err}" | grep -q "timed out\|Operation timed out"; then
        key_fail_reason="timeout"
    fi
    WD_AUTOLOGIN_RESULT="existing-key failed: ${key_fail_reason}"

    if sshpass -p "${password}" \
            ssh ${SSH_OPTS} \
            -p "${client_ip_port}" \
            "${client_user}@${WD_RAC_SERVER}" \
            "${install_cmd}" \
            2>/dev/null; then
        WD_AUTOLOGIN_RESULT="Key installed via password from ${local_pubkey_file}"
        return 0
    else
        WD_AUTOLOGIN_RESULT="sshpass+ssh failed (wrong password or connection refused)"
        return 1
    fi
}

### Main command handler for setup-autologin
cmd_setup_autologin() {
    ### Usage:
    ###   setup-autologin all              — scan all RACs 1..213
    ###   setup-autologin <NUM>            — test a single RAC
    ###   setup-autologin <N1,N2,N3>       — test a comma-separated list
    ###   setup-autologin --retry-failed   — retry RACs from last log
    local client_user="wsprdaemon"
    local max_rac="213"
    local retry_failed=0
    local -a rac_list=()

    ### Shift through args
    local -a args=("$@")
    local i=0
    while (( i < ${#args[@]} )); do
        local arg="${args[$i]}"
        case "${arg}" in
            --retry-failed)
                retry_failed=1
                ;;
            all)
                rac_list=()   # explicit all — clear any prior list
                ;;
            [0-9]*)
                ### Accept "8" or "8,41,44" 
                IFS=',' read -ra nums <<< "${arg}"
                for n in "${nums[@]}"; do
                    [[ "${n}" =~ ^[0-9]+$ ]] && rac_list+=("${n}")
                done
                ;;
            *)
                client_user="${arg}"
                ;;
        esac
        (( i++ ))
    done

    ### If --retry-failed, find the most recent log and extract failed RAC numbers
    if (( retry_failed )); then
        local latest_log
        latest_log=$(ls -t wd-autologin-setup-*.log 2>/dev/null | head -1)
        if [[ -z "${latest_log}" ]]; then
            echo "ERROR: No wd-autologin-setup-*.log files found in current directory"
            return 1
        fi
        echo "Retrying failed RACs from: ${latest_log}"
        ### Parse the summary section at the end of the log.
        ### Lines look like:  "  RAC #41"  or "  RAC #8"
        ### We grab everything under "Failed (N) —" and "WARNING" sections.
        local in_failed_section=0
        while IFS= read -r line; do
            if [[ "${line}" =~ ^Failed.*manual\ attention ]]; then
                in_failed_section=1
                continue
            fi
            ### Stop at the next section header or blank summary line
            if (( in_failed_section )); then
                if [[ "${line}" =~ ^No\ password|^=|^Scan|^Summary ]]; then
                    in_failed_section=0
                    continue
                fi
                if [[ "${line}" =~ RAC\ #([0-9]+) ]]; then
                    rac_list+=("${BASH_REMATCH[1]}")
                fi
            fi
        done < "${latest_log}"

        if [[ ${#rac_list[@]} -eq 0 ]]; then
            echo "No FAILED or WARNING RACs found in ${latest_log}"
            return 0
        fi
        echo "Found ${#rac_list[@]} RACs to retry: ${rac_list[*]}"
        echo ""
    fi

    ### Ensure we can reach GW2 / RAC ports at all
    if ! ensure_gw2_connection; then
        return 1
    fi
    echo "Using connection: ${WD_RAC_CONNECTION_PATH}"
    echo ""

    ### Load encrypted RAC config once up-front so we have all passwords in memory.
    ### load_rac_configs prompts for the SSR passphrase (never cached on disk).
    if ! load_rac_configs; then
        echo "ERROR: Cannot load RAC configs; aborting."
        return 1
    fi

    ### Ensure sshpass is available
    if ! check_sshpass; then
        return 1
    fi

    ###########################################################################
    ### Prompt once for a session-only fallback password.
    ### This is tried when:
    ###   (a) a RAC has no password stored in the SSR config, OR
    ###   (b) a RAC has a stored password but ssh-copy-id fails with it.
    ### The fallback is held only in this local variable — never written to
    ### disk — so it is safe to use the well-known site default here without
    ### embedding it in the script itself.
    ###########################################################################
    local fallback_password=""
    echo "=== Fallback Password ===" >&2
    echo "Enter a default password to try when a RAC has no stored password" >&2
    echo "or when its stored password fails. This is held in memory only —" >&2
    echo "never written to disk. Press Enter to skip fallback attempts." >&2
    read -sp "Fallback password (blank = none): " fallback_password >&2
    echo "" >&2
    if [[ -z "${fallback_password}" ]]; then
        echo "No fallback password set — will skip RACs with no/wrong stored password." >&2
    else
        echo "Fallback password set (session only)." >&2
    fi
    echo "" >&2

    local log_file="wd-autologin-setup-$(date +%Y%m%d_%H%M%S).log"
    if (( retry_failed )) && [[ ${#rac_list[@]} -gt 0 ]]; then
        echo "Retrying ${#rac_list[@]} failed RACs: ${rac_list[*]}"
    elif [[ ${#rac_list[@]} -eq 1 ]]; then
        echo "Testing single RAC #${rac_list[0]}..."
    elif [[ ${#rac_list[@]} -gt 1 ]]; then
        echo "Testing ${#rac_list[@]} specified RACs: ${rac_list[*]}"
    else
        echo "Scanning all RACs 1-${max_rac}..."
    fi
    echo "Log file: ${log_file}"
    echo ""
    echo "Started at $(date)" | tee "${log_file}"
    echo "======================================" | tee -a "${log_file}"

    declare -i already_count=0
    declare -i installed_count=0
    declare -i failed_count=0
    declare -i no_port_count=0
    declare -i no_password_count=0
    declare -a installed_racs=()
    declare -a failed_racs=()
    declare -a no_password_racs=()

    ### Build the list of RACs to scan
    local -a scan_racs=()
    if [[ ${#rac_list[@]} -gt 0 ]]; then
        scan_racs=("${rac_list[@]}")
    else
        for (( i=1; i<=max_rac; i++ )); do scan_racs+=("$i"); done
    fi

    for rac in "${scan_racs[@]}"; do
        local client_ip_port=$(( RAC_BASE_PORT + rac ))

        printf "RAC #%-3d port %-5d  " "${rac}" "${client_ip_port}"

        ### 1. Is the port reachable at all?
        if ! nc -z -w 2 "${WD_RAC_SERVER}" "${client_ip_port}" >/dev/null 2>&1; then
            printf "SKIP  (port closed)\n" | tee -a "${log_file}"
            (( no_port_count++ ))
            continue
        fi

        ### 2. Look up SSR config for this RAC (user, password, site) — do this
        ###    BEFORE the autologin check so we test with the correct username.
        local rac_index
        rac_index=$(find_rac_by_id "${rac}")

        local rac_password=""
        local rac_site="unknown"
        local rac_user="${client_user}"   # default, overridden if in SSR config

        if [[ "${rac_index}" != "-1" ]]; then
            rac_password="${WD_RAC_PASSWORDS[${rac_index}]}"
            rac_site="${WD_RAC_SITES[${rac_index}]}"
            local config_user="${WD_RAC_USERS[${rac_index}]}"
            [[ -z "${config_user}" ]] && config_user="${WD_RAC_ACCOUNTS[${rac_index}]}"
            [[ -n "${config_user}" ]] && rac_user="${config_user}"
        fi

        ### 3. Can we already autologin as the correct user?
        ###    If host key mismatch, clear stale entry and retry.
        local _check_err
        _check_err=$(ssh ${SSH_OPTS} -p "${client_ip_port}" -o BatchMode=yes \
                "${rac_user}@${WD_RAC_SERVER}" "exit 0" 2>&1)
        if [[ $? -eq 0 ]]; then
            printf "OK    (autologin already works as %s)\n" "${rac_user}" | tee -a "${log_file}"
            (( already_count++ ))
            continue
        fi
        if echo "${_check_err}" | grep -q "REMOTE HOST IDENTIFICATION\|Host key verification failed"; then
            ssh-keygen -f "${HOME}/.ssh/known_hosts" -R "[${WD_RAC_SERVER}]:${client_ip_port}" >/dev/null 2>&1
        fi

        printf "SETUP (autologin missing%s, user: %s)...\n" \
            "${rac_site:+", site: ${rac_site}"}" "${rac_user}" | tee -a "${log_file}"

        ### 4. Try installing our pubkey via existing SSH trust (no password).
        ###    This works when gw2 already has a key on the RAC even if we don't.
        local key_installed=0
        local attempt_label="existing SSH key"
        if install_autologin_key "${client_ip_port}" "${rac_user}" ""; then
            key_installed=1
        else
            printf "      ... existing-key install failed (%s), " "${WD_AUTOLOGIN_RESULT}" | tee -a "${log_file}"

            ### 5. Fall back to passwords: stored config first, then fallback
            local -a passwords_to_try=()
            if [[ -n "${rac_password}" ]]; then
                passwords_to_try+=("${rac_password}")
            fi
            if [[ -n "${fallback_password}" && "${fallback_password}" != "${rac_password}" ]]; then
                passwords_to_try+=("${fallback_password}")
            fi

            if (( ${#passwords_to_try[@]} == 0 )); then
                printf "\n      SKIP  (no password available and existing-key install failed)\n" | tee -a "${log_file}"
                (( no_password_count++ ))
                no_password_racs+=("${rac}")
                continue
            fi

            for try_password in "${passwords_to_try[@]}"; do
                if [[ "${try_password}" == "${rac_password}" && -n "${rac_password}" ]]; then
                    attempt_label="stored config password"
                else
                    attempt_label="fallback password"
                fi
                if install_autologin_key "${client_ip_port}" "${rac_user}" "${try_password}"; then
                    key_installed=1
                    break
                fi
                printf "      ... %s failed, " "${attempt_label}" | tee -a "${log_file}"
            done
        fi

        if (( key_installed == 0 )); then
            printf "      FAILED  — all passwords tried, none worked (%s)\n" \
                "${WD_AUTOLOGIN_RESULT}" | tee -a "${log_file}"
            (( failed_count++ ))
            failed_racs+=("${rac}")
            continue
        fi

        ### 6. Verify autologin actually works now
        if ssh ${SSH_OPTS} -p "${client_ip_port}" -o BatchMode=yes \
                "${rac_user}@${WD_RAC_SERVER}" "exit 0" >/dev/null 2>&1; then
            printf "      SUCCESS — autologin verified via %s (%s)\n" \
                "${attempt_label}" "${WD_AUTOLOGIN_RESULT}" | tee -a "${log_file}"
            (( installed_count++ ))
            installed_racs+=("${rac}")
        else
            printf "      WARNING — key installed but autologin verify still failed\n" \
                | tee -a "${log_file}"
            (( failed_count++ ))
            failed_racs+=("${rac}")
        fi
    done

    ### Clear fallback password from memory
    fallback_password=""

    echo "" | tee -a "${log_file}"
    echo "======================================" | tee -a "${log_file}"
    echo "Scan completed at $(date)" | tee -a "${log_file}"
    echo "" | tee -a "${log_file}"
    echo "Summary:" | tee -a "${log_file}"
    echo "  Already working:   ${already_count}" | tee -a "${log_file}"
    echo "  Newly installed:   ${installed_count}" | tee -a "${log_file}"
    echo "  Failed:            ${failed_count}" | tee -a "${log_file}"
    echo "  No port / offline: ${no_port_count}" | tee -a "${log_file}"
    echo "  No password:       ${no_password_count}" | tee -a "${log_file}"
    echo "  Total scanned:     ${max_rac}" | tee -a "${log_file}"
    echo "" | tee -a "${log_file}"

    if (( ${#installed_racs[@]} > 0 )); then
        echo "Newly set up (${#installed_racs[@]}):" | tee -a "${log_file}"
        for rac in "${installed_racs[@]}"; do
            echo "  RAC #${rac}" | tee -a "${log_file}"
        done
        echo "" | tee -a "${log_file}"
    fi

    if (( ${#failed_racs[@]} > 0 )); then
        echo "Failed (${#failed_racs[@]}) — may need manual attention:" | tee -a "${log_file}"
        for rac in "${failed_racs[@]}"; do
            echo "  RAC #${rac}" | tee -a "${log_file}"
        done
        echo "" | tee -a "${log_file}"
    fi

    if (( ${#no_password_racs[@]} > 0 )); then
        echo "No password available (${#no_password_racs[@]}) — skipped:" | tee -a "${log_file}"
        for rac in "${no_password_racs[@]}"; do
            echo "  RAC #${rac}" | tee -a "${log_file}"
        done
        echo "" | tee -a "${log_file}"
    fi

    echo "Full log saved to: ${log_file}"
    return 0
}

###############################################################################
### show-rac-last-login: Query the frps REST API to show proxy status,
### last connect time, and remote port for every registered frpc client.
###
### The frps dashboard API at http://localhost:7500/api/proxy/tcp returns
### a JSON array of all proxies (online and offline) with fields:
###   name           — proxy/callsign name (e.g. "W7WKR-K1", "KFS-SE")
###   conf.remotePort — the TCP port assigned on the server
###   status         — "online" or "offline"
###   lastStartTime  — last time proxy came online  ("MM-DD HH:MM:SS")
###   lastCloseTime  — last time proxy went offline ("MM-DD HH:MM:SS")
###   todayTrafficIn / todayTrafficOut — bytes today
###   curConns       — current active connections through this proxy
###
### Usage:
###   ./wd-register-client.sh show-rac-last-login [OPTIONS]
###
### Options:
###   --api-url  <url>   frps API base URL (default: http://localhost:7500)
###   --api-user <user>  frps dashboard username (default: admin)
###   --api-pass <pass>  frps dashboard password (default: admin)
###   --online           only show proxies currently online
###   --offline          only show proxies currently offline
###   --filter  <str>    only show proxies whose name contains <str>
###   --sort    name|port|time|close|status
###                      sort column (default: time = lastStartTime desc)
###   --portwarn <N>     warn if remotePort doesn't look like a RAC port
###                      (i.e. not in range RAC_BASE_PORT .. RAC_BASE_PORT+999)
###############################################################################

###############################################################################
### cmd_wd_versions — SSH into every active RAC and report the WD version
###
### Version detection (tried in order):
###   1. NEW: ~/wsprdaemon/wd_version.txt + git rev-list --count HEAD
###   2. OLD: grep 'declare -r VERSION=' ~/wsprdaemon/wsprdaemon.sh
###
### The SSH login user is taken from the encrypted SSR config for each RAC,
### exactly as setup-autologin does.  Only RACs with open ports and working
### autologin are queried — no password prompts during the scan.
###
### Usage:
###   ./wd-register-client.sh wd-versions [--timeout N] [--sort rac|version|site]
###
### Options:
###   --timeout N   SSH ConnectTimeout in seconds (default: 8)
###   --sort        Sort output by rac (default), version, or site
###############################################################################

cmd_wd_versions() {
    local opt_timeout=8
    local opt_sort="version"   ### default: oldest version first
    local opt_nc_timeout=1      # nc timeout per port during parallel scan
    local opt_parallel=40       # max simultaneous nc probes
    local max_rac=213

    while [[ $# -gt 0 ]]; do
        case "$1" in
            --timeout)  opt_timeout="$2";  shift 2 ;;
            --sort)     opt_sort="$2";     shift 2 ;;
            --parallel) opt_parallel="$2"; shift 2 ;;
            *) echo "ERROR: Unknown option: $1" >&2
               echo "Usage: $0 wd-versions [--timeout N] [--sort rac|version|site] [--parallel N]" >&2
               return 1 ;;
        esac
    done

    if ! ensure_gw2_connection; then
        return 1
    fi
    echo "Using connection: ${WD_RAC_CONNECTION_PATH}"
    echo ""

    ###########################################################################
    ### STEP 1: Parallel port scan — fast, no passphrase needed yet
    ### Fire up to opt_parallel nc probes at once, collect open port list
    ###########################################################################
    echo "Step 1: Scanning ports 1-${max_rac} in parallel (batch size ${opt_parallel})..."
    local -a open_racs=()
    local -a pids=()
    local tmp_dir
    tmp_dir=$(mktemp -d)
    trap "rm -rf '${tmp_dir}'" RETURN

    local rac
    for (( rac=1; rac<=max_rac; rac++ )); do
        local port=$(( RAC_BASE_PORT + rac ))
        ### Launch nc probe in background; write rac number to tmp file if open
        ( nc -z -w ${opt_nc_timeout} "${WD_RAC_SERVER}" "${port}" >/dev/null 2>&1 \
            && echo "${rac}" > "${tmp_dir}/${rac}" ) &
        pids+=($!)

        ### Throttle: wait for a batch to finish before launching more
        if (( ${#pids[@]} >= opt_parallel )); then
            wait "${pids[@]}" 2>/dev/null
            pids=()
        fi
    done
    ### Wait for any remaining probes
    wait "${pids[@]}" 2>/dev/null

    ### Collect results in order
    for (( rac=1; rac<=max_rac; rac++ )); do
        [[ -f "${tmp_dir}/${rac}" ]] && open_racs+=("${rac}")
    done

    echo "Found ${#open_racs[@]} active RAC ports."
    echo ""

    if [[ ${#open_racs[@]} -eq 0 ]]; then
        echo "No active RAC ports found. Check GW2 connection."
        return 1
    fi

    ###########################################################################
    ### STEP 2: Load RAC config (prompts for passphrase) — now that we know
    ###         which RACs are alive, we know we have work to do
    ###########################################################################
    if ! load_rac_configs; then
        echo "ERROR: Cannot load RAC configs; aborting."
        return 1
    fi
    echo ""

    ###########################################################################
    ### STEP 3: SSH into each active RAC and get version — one at a time,
    ###         streaming results live to terminal and log file
    ###########################################################################
    local log_file="wd-versions-$(date +%Y%m%d_%H%M%S).log"
    echo "Step 2: Querying WD version on ${#open_racs[@]} active RACs..."
    echo "Log file: ${log_file}"
    echo ""

    ### Header
    printf "%-6s %-14s %-20s %s\n" "RAC" "SITE" "USER" "WD VERSION" | tee "${log_file}"
    printf '%0.s-' {1..72} | tee -a "${log_file}"; echo "" | tee -a "${log_file}"

    ### SSH options: BatchMode=yes so we never block waiting for a password
    local ver_ssh_opts="-o StrictHostKeyChecking=accept-new -o BatchMode=yes -o ConnectTimeout=${opt_timeout}"

    ### Remote command: four methods tried in order, stopping at first valid result.
    ### "Valid" means the result matches a version pattern (digits/dots/dashes),
    ### NOT a shell expression like $(cd ...) stored literally in wd_version.txt.
    ###
    ###   1. wd_version.txt content (validated) + git rev-list --count HEAD
    ###   2. grep 'declare -r VERSION=' wsprdaemon.sh
    ###   3. git symbolic-ref --short HEAD + git rev-list --count HEAD (wdvv alias)
    ###   4. wsprdaemon.sh -V => parse "Version = X.X.X" (timeout 30)
    local remote_cmd='
        wd_root=~/wsprdaemon
        ver_file="${wd_root}/wd_version.txt"
        wd_sh="${wd_root}/wsprdaemon.sh"

        ### Helper: does a string look like a real version (not a shell expression)?
        is_ver() { [[ "$1" =~ ^[0-9]+\.[0-9] ]]; }

        ### Method 1: wd_version.txt + git commit count
        if [[ -f "${ver_file}" ]]; then
            ver=$(< "${ver_file}")
            ver="${ver%%[[:space:]]*}"
            if is_ver "${ver}"; then
                if cd "${wd_root}" 2>/dev/null && git rev-parse --git-dir >/dev/null 2>&1; then
                    cnt=$(git rev-list --count HEAD 2>/dev/null)
                    echo "${ver}-${cnt}"
                else
                    echo "${ver}"
                fi
                exit 0
            fi
        fi

        ### Method 2: grep declare -r VERSION= from wsprdaemon.sh
        if [[ -f "${wd_sh}" ]]; then
            line=$(grep -m1 "declare -r VERSION=" "${wd_sh}" 2>/dev/null)
            ver=$(echo "${line}" | sed "s/.*VERSION=[\"'"'"']\{0,1\}\([^\"'"'"' ]*\).*/\1/")
            if is_ver "${ver}"; then
                echo "${ver}"
                exit 0
            fi
        fi

        ### Method 3: git branch + commit count (wdvv pattern)
        ### Only use the branch name if it looks like a version (starts with digits).
        ### Branches named 'master', 'main', etc. are not version strings — skip to method 4.
        if cd "${wd_root}" 2>/dev/null && git rev-parse --git-dir >/dev/null 2>&1; then
            branch=$(git symbolic-ref --short HEAD 2>/dev/null)
            cnt=$(git rev-list --count HEAD 2>/dev/null)
            if is_ver "${branch}" && [[ -n "${cnt}" ]]; then
                echo "${branch}-${cnt}"
                exit 0
            fi
        fi

        ### Method 4: wsprdaemon.sh -V => parse "Version = X.X.X"
        if [[ -x "${wd_sh}" ]]; then
            ver=$(timeout 30 "${wd_sh}" -V 2>/dev/null | grep -m1 "^Version" | sed "s/Version *= *//")
            if is_ver "${ver}"; then
                echo "${ver} (live)"
                exit 0
            fi
        fi

        echo "unknown"
    '

    ### Collect all results, then sort and print in single pass
    local -a results=()
    local -i queried=0 failed=0

    for rac in "${open_racs[@]}"; do
        local client_ip_port=$(( RAC_BASE_PORT + rac ))

        ### Look up user and site from config
        local rac_index
        rac_index=$(find_rac_by_id "${rac}")
        local rac_user="wsprdaemon"
        local rac_site="?"
        if [[ "${rac_index}" != "-1" ]]; then
            rac_site="${WD_RAC_SITES[${rac_index}]:-?}"
            local cfg_user="${WD_RAC_USERS[${rac_index}]:-}"
            [[ -z "${cfg_user}" ]] && cfg_user="${WD_RAC_ACCOUNTS[${rac_index}]:-}"
            [[ -n "${cfg_user}" ]] && rac_user="${cfg_user}"
        fi

        ### Query version via SSH
        local version
        version=$(ssh ${ver_ssh_opts} -p "${client_ip_port}" \
            "${rac_user}@${WD_RAC_SERVER}" "${remote_cmd}" 2>/dev/null)

        if [[ -z "${version}" ]]; then
            version="no autologin / SSH failed"
            (( failed++ ))
        else
            (( queried++ ))
        fi

        results+=("$(printf '%05d\t%s\t%s\t%s' "${rac}" "${rac_site}" "${rac_user}" "${version}")")
    done

    ### Sort results array
    local -a sorted=()
    case "${opt_sort}" in
        version) mapfile -t sorted < <(printf '%s\n' "${results[@]}" | sort -t$'\t' -k4,4 -k1,1n) ;;
        site)    mapfile -t sorted < <(printf '%s\n' "${results[@]}" | sort -t$'\t' -k2,2 -k1,1n) ;;
        rac|*)   mapfile -t sorted < <(printf '%s\n' "${results[@]}" | sort -t$'\t' -k1,1n) ;;
    esac

    ### Print results (single pass — no duplicates)
    for row in "${sorted[@]}"; do
        IFS=$'\t' read -r rac_padded site user version <<< "${row}"
        printf "%-6d %-14s %-20s %s\n" "$(( 10#${rac_padded} ))" "${site}" "${user}" "${version}" | tee -a "${log_file}"
    done

    echo "" | tee -a "${log_file}"
    printf '%0.s-' {1..72} | tee -a "${log_file}"; echo "" | tee -a "${log_file}"
    printf "Queried: %d   No autologin/failed: %d   Port closed (skipped): %d\n" \
        "${queried}" "${failed}" "$(( max_rac - ${#open_racs[@]} ))" | tee -a "${log_file}"
    echo "Full log: ${log_file}"
    return 0
}

###############################################################################
### find_latest_versions_log — find most recent wd-versions-*.log or use arg
###############################################################################
find_versions_log() {
    local __result_var=$1
    local candidate="${2:-}"

    if [[ -n "${candidate}" ]]; then
        if [[ ! -f "${candidate}" ]]; then
            echo "ERROR: Log file not found: ${candidate}" >&2
            return 1
        fi
        printf -v "${__result_var}" '%s' "${candidate}"
        return 0
    fi

    local latest
    latest=$(ls -t wd-versions-*.log 2>/dev/null | head -1)
    if [[ -z "${latest}" ]]; then
        echo "ERROR: No wd-versions-*.log found in current directory." >&2
        echo "       Run 'wd-versions' first, or supply a log file path." >&2
        return 1
    fi
    printf -v "${__result_var}" '%s' "${latest}"
    return 0
}

###############################################################################
### cmd_wd_versions_sort — re-sort an existing wd-versions log by version
###
### Usage: ./wd-register-client.sh wd-versions-sort [LOGFILE]
###############################################################################
cmd_wd_versions_sort() {
    local log_file=""
    if ! find_versions_log log_file "${1:-}"; then
        return 1
    fi
    echo "Sorting by version: ${log_file}"
    echo ""

    ### Print header
    head -2 "${log_file}"

    ### Extract data rows (lines starting with digit), convert to tab-delimited,
    ### sort on the 4th tab field (WD VERSION), then reformat for display.
    ### awk converts the fixed-width columns to tab-delimited for reliable sort.
    grep -E '^[0-9]' "${log_file}" \
        | awk '{printf "%s\t%s\t%s\t",$1,$2,$3; for(i=4;i<=NF;i++) printf "%s%s",$i,(i<NF?" ":""); print ""}' \
        | sort -t$'\t' -k4,4 -k1,1n \
        | awk -F$'\t' '{printf "%-6s %-14s %-20s %s\n",$1,$2,$3,$4}'

    echo ""
    printf '%0.s-' {1..72}; echo ""
    grep -E '^(Queried|Full log)' "${log_file}" || true
}

###############################################################################
### cmd_wd_versions_login — walk RACs oldest-version-first, SSH in one at a time
###
### Usage: ./wd-register-client.sh wd-versions-login [LOGFILE]
###
### Reads the log, sorts by version (oldest first), SSHes into each RAC
### interactively. After you exit the SSH session, prompts: [Enter]=next,
### s=skip, q=quit.
###############################################################################
cmd_wd_versions_login() {
    local log_file=""
    if ! find_versions_log log_file "${1:-}"; then
        return 1
    fi

    if ! ensure_gw2_connection; then
        return 1
    fi

    ### Build sorted list oldest-version-first, skip no-autologin/unknown rows
    local -a rows=()
    while IFS=$'\t' read -r rac site user version; do
        [[ "${version}" =~ "no autologin" ]] && continue
        [[ "${version}" =~ "unknown" ]] && continue
        rows+=("${rac}	${site}	${user}	${version}")
    done < <(
        grep -E '^[0-9]' "${log_file}" \
            | awk '{printf "%s\t%s\t%s\t",$1,$2,$3; for(i=4;i<=NF;i++) printf "%s%s",$i,(i<NF?" ":""); print ""}' \
            | sort -t$'\t' -k4,4 -k1,1n
    )

    local total=${#rows[@]}
    echo "Found ${total} RACs with known versions in: ${log_file}"
    echo "Sorted oldest-first. You will be SSHed in one at a time."
    echo "After each session: Enter=next  s=skip  q=quit"
    echo ""

    local -i idx=0
    for line in "${rows[@]}"; do
        (( idx++ ))
        local rac site user version
        IFS=$'\t' read -r rac site user version <<< "${line}"

        echo "──────────────────────────────────────────────────────────────────────"
        printf "  [%d/%d]  RAC %-4d  site=%-14s  user=%-12s  version=%s\n" \
            "${idx}" "${total}" "${rac}" "${site}" "${user}" "${version}"
        echo "──────────────────────────────────────────────────────────────────────"

        local client_ip_port=$(( RAC_BASE_PORT + rac ))

        ### Prompt before connecting
        local choice
        read -rp "  Press Enter to SSH in, 's' to skip, 'q' to quit: " choice
        case "${choice,,}" in
            q|quit) echo "Quitting."; return 0 ;;
            s|skip) echo "  Skipped RAC ${rac}."; echo ""; continue ;;
        esac

        echo "  Connecting to RAC ${rac} as ${user}@${WD_RAC_SERVER}:${client_ip_port}..."
        ssh ${SSH_OPTS} -p "${client_ip_port}" "${user}@${WD_RAC_SERVER}"
        local rc=$?
        echo ""
        (( rc != 0 )) && echo "  (SSH exited with code ${rc})"
    done

    echo "All ${total} RACs visited."
}

###############################################################################
### cmd_wd_login — SSH directly into a single RAC by number
###
### Usage: ./wd-register-client.sh wd-login <RAC_NUMBER> [LOGFILE]
###
### Looks up the user from the most recent wd-versions log (or supplied log).
### Falls back to 'wsprdaemon' if RAC not found in log.
###############################################################################
cmd_wd_login() {
    local rac="${1:-}"
    local log_file=""

    if [[ -z "${rac}" ]] || ! is_uint "${rac}"; then
        echo "ERROR: wd-login requires a RAC number"
        echo "Usage: $0 wd-login <RAC_NUMBER> [LOGFILE]"
        return 1
    fi

    if ! ensure_gw2_connection; then
        return 1
    fi

    ### Try to find user from log file
    find_versions_log log_file "${2:-}" 2>/dev/null || true

    local rac_user="wsprdaemon"
    local rac_site="?"
    local rac_version="?"

    if [[ -n "${log_file}" && -f "${log_file}" ]]; then
        local match
        match=$(grep -E "^${rac}[[:space:]]" "${log_file}" | head -1)
        if [[ -n "${match}" ]]; then
            local tab_row
            tab_row=$(echo "${match}" | awk '{printf "%s\t%s\t%s\t",$1,$2,$3; for(i=4;i<=NF;i++) printf "%s%s",$i,(i<NF?" ":""); print ""}')
            IFS=$'\t' read -r _ rac_site rac_user rac_version <<< "${tab_row}"
        fi
    fi

    local client_ip_port=$(( RAC_BASE_PORT + rac ))

    echo "RAC ${rac}  site=${rac_site}  user=${rac_user}  version=${rac_version}"
    echo "Connecting to ${rac_user}@${WD_RAC_SERVER}:${client_ip_port}..."
    ssh ${SSH_OPTS} -p "${client_ip_port}" "${rac_user}@${WD_RAC_SERVER}"
}

### frps API defaults — override with --api-* options or environment vars
declare FRPS_API_URL="${FRPS_API_URL:-http://localhost:7500}"
declare FRPS_API_USER="${FRPS_API_USER:-admin}"
declare FRPS_API_PASS="${FRPS_API_PASS:-admin}"

### Check that jq and curl are available
check_api_deps() {
    local missing=()
    command -v curl &>/dev/null || missing+=(curl)
    command -v jq   &>/dev/null || missing+=(jq)
    if (( ${#missing[@]} > 0 )); then
        echo "ERROR: Required tools not found: ${missing[*]}"
        echo "Install with:  sudo apt-get install ${missing[*]}"
        return 1
    fi
    return 0
}

### Fetch proxy list from frps API, return raw JSON
fetch_frps_proxies() {
    local api_url="$1" api_user="$2" api_pass="$3"
    local response http_code

    response=$(curl -s -w "\n%{http_code}" \
        -u "${api_user}:${api_pass}" \
        "${api_url}/api/proxy/tcp" 2>/dev/null)

    http_code=$(echo "${response}" | tail -1)
    local body=$(echo "${response}" | head -n -1)

    case "${http_code}" in
        200) echo "${body}"; return 0 ;;
        401) echo "ERROR: Authentication failed — check --api-user / --api-pass" >&2; return 1 ;;
        000) echo "ERROR: Cannot connect to frps API at ${api_url}" >&2
             echo "       Is frps running?  Is the dashboard enabled in frps.toml?" >&2
             return 1 ;;
        *)   echo "ERROR: frps API returned HTTP ${http_code}" >&2
             echo "       Response: ${body}" >&2
             return 1 ;;
    esac
}

### Parse "MM-DD HH:MM:SS" frps timestamp — prepend current year
### Returns epoch seconds, or 0 if empty/unparseable
frps_api_ts_to_epoch() {
    local ts="$1"
    [[ -z "${ts}" || "${ts}" == "0001-01-01 00:00:00" ]] && echo 0 && return
    local year
    year=$(date +%Y)
    # frps format: "02-26 20:15:35"  — month-day hour:min:sec
    date -d "${year}/${ts//-//}" +%s 2>/dev/null || echo 0
}

### Human-readable duration from epoch to now
epoch_to_ago() {
    local epoch=$1
    (( epoch == 0 )) && echo "never" && return
    local now
    now=$(date +%s)
    local age=$(( now - epoch ))
    if   (( age <   0 ));       then echo "future?"
    elif (( age < 120 ));       then echo "just now"
    elif (( age < 3600 ));      then echo "$(( age/60 ))m ago"
    elif (( age < 86400 ));     then echo "$(( age/3600 ))h $(( (age%3600)/60 ))m ago"
    elif (( age < 86400*7 ));   then echo "$(( age/86400 ))d $(( (age%86400)/3600 ))h ago"
    elif (( age < 86400*365 )); then echo "$(( age/86400 ))d ago"
    else                             echo "$(( age/86400/365 ))y+ ago"
    fi
}

cmd_show_rac_last_login() {
    local opt_api_url="${FRPS_API_URL}"
    local opt_api_user="${FRPS_API_USER}"
    local opt_api_pass="${FRPS_API_PASS}"
    local opt_online_only=0
    local opt_offline_only=0
    local opt_filter=""
    local opt_sort="rac"

    ### Parse options
    while [[ $# -gt 0 ]]; do
        case "$1" in
            --api-url)   opt_api_url="$2";   shift 2 ;;
            --api-user)  opt_api_user="$2";  shift 2 ;;
            --api-pass)  opt_api_pass="$2";  shift 2 ;;
            --online)    opt_online_only=1;  shift ;;
            --offline)   opt_offline_only=1; shift ;;
            --filter)    opt_filter="$2";    shift 2 ;;
            --sort)      opt_sort="$2";      shift 2 ;;
            *) echo "ERROR: Unknown option: $1" >&2
               echo "Usage: $0 show-rac-last-login [--api-url URL] [--api-user U] [--api-pass P]" >&2
               echo "       [--online] [--offline] [--filter STR] [--sort rac|name|time|close|status]" >&2
               return 1 ;;
        esac
    done

    if ! check_api_deps; then return 1; fi

    echo "Querying frps API: ${opt_api_url}/api/proxy/tcp"
    local json
    if ! json=$(fetch_frps_proxies "${opt_api_url}" "${opt_api_user}" "${opt_api_pass}"); then
        return 1
    fi

    local proxy_count
    proxy_count=$(echo "${json}" | jq '.proxies | length' 2>/dev/null)
    if [[ -z "${proxy_count}" || "${proxy_count}" == "null" ]]; then
        echo "ERROR: Unexpected API response format."
        echo "${json}" | head -10
        return 1
    fi
    echo "Found ${proxy_count} registered proxies."
    echo ""

    ###########################################################################
    ### Derive RAC# from remotePort:
    ###   35800 + RAC# => SSH/data port  (proxy name = callsign, e.g. W7WKR-K1)
    ###   45800 + RAC# => WEB port       (proxy name = callsign-WEB)
    ### For each RAC# we collect both rows and display as one combined line.
    ###########################################################################

    ### Associative arrays keyed by RAC# (integer string)
    declare -A rac_name=()          # callsign (from SSH proxy)
    declare -A rac_ssh_status=()    # online/offline
    declare -A rac_ssh_start=()     # lastStartTime string
    declare -A rac_ssh_start_ep=()  # lastStartTime epoch
    declare -A rac_ssh_close=()     # lastCloseTime string
    declare -A rac_web_status=()
    declare -A rac_conns=()         # curConns on SSH proxy
    declare -A rac_traffic=()       # todayTrafficIn on SSH proxy
    declare -a other_proxies=()     # proxies whose port doesn't fit either range

    while IFS=$'\t' read -r name port status last_start last_close traffic_in conns; do
        local rac_num=-1
        local is_web=0

        if (( port > 35800 && port <= 35800+999 )); then
            rac_num=$(( port - 35800 ))
            is_web=0
        elif (( port > 45800 && port <= 45800+999 )); then
            rac_num=$(( port - 45800 ))
            is_web=1
        fi

        if (( rac_num < 1 )); then
            other_proxies+=("${name}	${port}	${status}	${last_start}")
            continue
        fi

        local start_ep
        start_ep=$(frps_api_ts_to_epoch "${last_start}")

        if (( is_web == 0 )); then
            rac_name["${rac_num}"]="${name}"
            rac_ssh_status["${rac_num}"]="${status}"
            rac_ssh_start["${rac_num}"]="${last_start}"
            rac_ssh_start_ep["${rac_num}"]="${start_ep}"
            rac_ssh_close["${rac_num}"]="${last_close}"
            rac_conns["${rac_num}"]="${conns}"
            rac_traffic["${rac_num}"]="${traffic_in}"
        else
            rac_web_status["${rac_num}"]="${status}"
            ### If we haven't seen the SSH proxy for this RAC yet, record name from WEB
            [[ -z "${rac_name[${rac_num}]:-}" ]] && \
                rac_name["${rac_num}"]="${name%-WEB}"
        fi

    done < <(echo "${json}" | jq -r '
        .proxies[] |
        [
            .name,
            (.conf.remotePort | tostring),
            .status,
            (.lastStartTime  // ""),
            (.lastCloseTime  // ""),
            (.todayTrafficIn // 0 | tostring),
            (.curConns       // 0 | tostring)
        ] | @tsv
    ')

    ###########################################################################
    ### Build sortable row array: "sort_key\trac_num\t..."
    ###########################################################################
    local -a rows=()

    for rac_num in "${!rac_name[@]}"; do
        local name="${rac_name[${rac_num}]}"
        local ssh_st="${rac_ssh_status[${rac_num}]:-?}"
        local web_st="${rac_web_status[${rac_num}]:-?}"
        local last_start="${rac_ssh_start[${rac_num}]:-}"
        local start_ep="${rac_ssh_start_ep[${rac_num}]:-0}"
        local last_close="${rac_ssh_close[${rac_num}]:-}"
        local conns="${rac_conns[${rac_num}]:-0}"
        local traffic_in="${rac_traffic[${rac_num}]:-0}"

        ### Filters
        [[ -n "${opt_filter}"    ]] && [[ "${name}" != *"${opt_filter}"* ]] && continue
        (( opt_online_only  )) && [[ "${ssh_st}" != "online"  ]] && continue
        (( opt_offline_only )) && [[ "${ssh_st}" != "offline" ]] && continue

        local ago
        ago=$(epoch_to_ago "${start_ep}")

        ### Format traffic
        local traffic_str="-"
        if (( traffic_in > 0 )); then
            if   (( traffic_in > 1048576 )); then traffic_str="$(( traffic_in/1048576 ))MB"
            elif (( traffic_in > 1024 ));    then traffic_str="$(( traffic_in/1024 ))KB"
            else                                  traffic_str="${traffic_in}B"
            fi
        fi

        ### Combined status: SSH/WEB
        local ssh_disp="${ssh_st}"
        [[ "${ssh_st}" == "online"  ]] && ssh_disp="UP"
        [[ "${ssh_st}" == "offline" ]] && ssh_disp="down"
        local web_disp="${web_st}"
        [[ "${web_st}" == "online"  ]] && web_disp="UP"
        [[ "${web_st}" == "offline" ]] && web_disp="down"
        local combined_status="${ssh_disp}/${web_disp}"

        ### Sort key
        local sort_key
        case "${opt_sort}" in
            name)   sort_key="${name}" ;;
            time)   sort_key=$(printf "%020d" "${start_ep}") ;;
            close)  sort_key="${last_close:-0000-00-00 00:00:00}" ;;
            status) sort_key="${ssh_st}" ;;
            rac|*)  sort_key=$(printf "%05d" "${rac_num}") ;;
        esac

        rows+=("${sort_key}	${rac_num}	${name}	${combined_status}	${last_start}	${ago}	${last_close}	${traffic_str}	${conns}")
    done

    if (( ${#rows[@]} == 0 )); then
        echo "No RAC proxies match the current filters."
    else
        ### Sort — close and time are descending (most recent first), others ascending
        local -a sorted_rows=()
        if [[ "${opt_sort}" == "time" || "${opt_sort}" == "close" ]]; then
            mapfile -t sorted_rows < <(printf '%s\n' "${rows[@]}" | sort -t$'\t' -k1 -r)
        else
            mapfile -t sorted_rows < <(printf '%s\n' "${rows[@]}" | sort -t$'\t' -k1)
        fi

        local online_count=0 offline_count=0

        printf "%-5s  %-5s  %-22s  %-9s  %-16s  %-16s  %-16s  %s\n" \
            "RAC#" "PORT" "PROXY NAME" "SSH/WEB" "LAST CONNECT" "AGO" "LAST CLOSE" "TODAY"
        printf "%s\n" \
            "-----  -----  ----------------------  ---------  ----------------  ----------------  ----------------  -------"

        for row in "${sorted_rows[@]}"; do
            IFS=$'\t' read -r _key rac_num name status last_start ago last_close traffic conns <<< "${row}"
            local ssh_port=$(( 35800 + rac_num ))
            printf "%-5s  %-5s  %-22s  %-9s  %-16s  %-16s  %-16s  %s\n" \
                "${rac_num}" "${ssh_port}" "${name}" "${status}" \
                "${last_start:--}" "${ago}" "${last_close:--}" "${traffic}"
            [[ "${status}" == UP/* ]] && (( online_count++ )) || (( offline_count++ ))
        done

        echo ""
        echo "RAC proxies: ${#sorted_rows[@]} total  |  SSH online: ${online_count}  |  SSH offline: ${offline_count}"
    fi

    ###########################################################################
    ### Show non-RAC proxies separately (ports outside 35800/45800 ranges)
    ###########################################################################
    if (( ${#other_proxies[@]} > 0 && !opt_online_only && !opt_offline_only )); then
        echo ""
        echo "--- Non-RAC proxies (${#other_proxies[@]}) ---"
        printf "%-25s  %-7s  %-8s  %s\n" "NAME" "PORT" "STATUS" "LAST CONNECT"
        for row in "${other_proxies[@]}"; do
            IFS=$'\t' read -r name port status last_start <<< "${row}"
            printf "%-25s  %-7s  %-8s  %s\n" "${name}" "${port}" "${status}" "${last_start:--}"
        done
    fi

    echo ""
    return 0
}

###############################################################################
### Command Line Handling
###############################################################################

show_version() {
    echo "wd-register-client.sh version ${VERSION}"
    echo "Project: ${WD_RAC_PROJECT_DIR}"
}

### Always print version
echo "wd-register-client.sh v${VERSION}"

# Handle -V/--version flag — short version already printed above; show project path and exit
if [[ "${1-}" == "-V" || "${1-}" == "--version" ]]; then
    echo "Project: ${WD_RAC_PROJECT_DIR}"
    exit 0
fi

# Handle manual-setup command (no RAC access needed)
if [[ "${1-}" == "manual-setup" ]]; then
    if [[ $# -lt 3 ]]; then
        echo "ERROR: manual-setup requires USERNAME and SSH_PUBKEY"
        echo "Usage: $0 manual-setup <USERNAME> <SSH_PUBKEY_FILE_OR_STRING>"
        echo ""
        echo "Examples:"
        echo "  $0 manual-setup N6GN4 ~/.ssh/id_ed25519.pub"
        echo "  $0 manual-setup N6GN4 'ssh-ed25519 AAAA...'"
        exit 1
    fi
    
    username="$2"
    pubkey_input="$3"
    
    manual_setup_user "${username}" "${pubkey_input}"
    exit $?
fi

# Validate arguments before calling the function
if [[ $# -lt 1 ]]; then
    echo "ERROR: Missing required argument"
    echo ""
    echo "Usage: $0 <RAC_NUMBER> [client_user]"
    echo "   or: $0 manual-setup <USERNAME> <SSH_PUBKEY_FILE_OR_STRING>"
    echo "   or: $0 clean-gw-keys <RAC_NUMBER> [client_user]"
    echo "   or: $0 show-rac-last-login [--api-url URL] [--api-user U] [--api-pass P] [--online] [--offline] [--filter STR] [--sort name|port|time|status]"
    echo "   or: $0 setup-autologin <all|NUM|N1,N2,...|--retry-failed>"
    echo "   or: $0 scan-and-clean-all [client_user]"
    echo "   or: $0 -V|--version"
    echo ""
    echo "Arguments:"
    echo "  RAC_NUMBER    : The Remote Access Channel number (positive integer)"
    echo "  client_user   : Optional. The username on the client (default: wsprdaemon)"
    echo "  USERNAME      : Uppercase username for manual setup (e.g., N6GN4)"
    echo "  SSH_PUBKEY... : Path to SSH public key file OR the key itself as a string"
    echo ""
    echo "Commands:"
    echo "  $0 <RAC_NUMBER>                         - Full client registration via RAC"
    echo "  $0 manual-setup <USER> <PUBKEY>         - Create user without RAC access"
    echo "  $0 clean-gw-keys <RAC_NUMBER>           - Only remove GW1/GW2 keys from one client"
    echo "  $0 show-rac-last-login               - Query frps API for proxy status, port, and last connect time"
    echo "  $0 setup-autologin all             - Install SSH pubkey on all RACs lacking autologin"
    echo "  $0 setup-autologin 8               - Test/install on single RAC"
    echo "  $0 setup-autologin 8,41,44         - Test/install on specific RACs"
    echo "  $0 setup-autologin --retry-failed  - Retry RACs that failed in last log"
    echo "  $0 scan-and-clean-all                   - Scan all RACs 1-213 and clean where possible"
    echo "  $0 wd-versions                          - Report WD version on all active RACs"
    echo "  $0 wd-versions --sort version           - Sort results by version string"
    echo "  $0 wd-versions --timeout 15             - Use longer SSH timeout (default: 8s)"
    echo "  $0 wd-versions-sort [LOGFILE]           - Re-sort existing log by version (no rescan)"
    echo "  $0 wd-versions-login [LOGFILE]          - SSH into RACs oldest-first, one at a time"
    echo "  $0 wd-login <RAC>                       - SSH directly into a specific RAC"
    echo "  $0 -V|--version                         - Show version"
    echo ""
    echo "Prerequisites:"
    echo "  For RAC commands: Requires wd-rac project with encrypted config files:"
    echo "    ${WD_RAC_PROJECT_DIR}/conf/rac-master.conf.enc"
    echo "  For manual-setup: No prerequisites, just username and SSH public key"
    echo ""
    echo "Examples:"
    echo "  $0 129                                  # Register RAC 129"
    echo "  $0 129 myuser                           # Register with custom username"
    echo "  $0 manual-setup N6GN4 ~/.ssh/id_ed25519.pub  # Manual setup with key file"
    echo "  $0 manual-setup N6GN4 'ssh-ed25519 AAAA...'  # Manual setup with key string"
    echo "  $0 clean-gw-keys 129                    # Clean only RAC 129"
    echo "  $0 scan-and-clean-all                   # Scan and clean all accessible RACs"
    exit 1
fi

# Check for clean-gw-keys command
if [[ "$1" == "clean-gw-keys" ]]; then
    if [[ $# -lt 2 ]]; then
        echo "ERROR: clean-gw-keys requires a RAC_NUMBER"
        echo "Usage: $0 clean-gw-keys <RAC_NUMBER> [client_user]"
        exit 1
    fi
    
    client_rac="$2"
    client_user="${3-wsprdaemon}"
    
    if ! is_uint "${client_rac}"; then
        echo "ERROR: RAC_NUMBER must be a positive integer, got: '$client_rac'"
        exit 1
    fi
    
    # Ensure we have a valid connection to GW2
    if ! ensure_gw2_connection; then
        exit 1
    fi
    
    set -u
    client_ip_port=$(( 35800 + client_rac ))
    
    echo "Cleaning GW1/GW2 keys from RAC #${client_rac} via ${WD_RAC_CONNECTION_PATH}"
    
    # Test connection first
    if ! nc -z ${WD_RAC_SERVER} ${client_ip_port} >/dev/null 2>&1; then
        echo "ERROR: Can't connect to ${WD_RAC_SERVER}:${client_ip_port}"
        exit 1
    fi
    
    wd-remove-gw-keys-from-client "${client_ip_port}" "${client_user}"
    exit 0
fi

# Check for show-rac-last-login command
if [[ "$1" == "show-rac-last-login" ]]; then
    shift
    cmd_show_rac_last_login "$@"
    exit $?
fi

# Check for wd-versions command
if [[ "$1" == "wd-versions" ]]; then
    shift
    cmd_wd_versions "$@"
    exit $?
fi

# Check for wd-versions-sort command
if [[ "$1" == "wd-versions-sort" ]]; then
    shift
    cmd_wd_versions_sort "$@"
    exit $?
fi

# Check for wd-versions-login command
if [[ "$1" == "wd-versions-login" ]]; then
    shift
    cmd_wd_versions_login "$@"
    exit $?
fi

# Check for wd-login command
if [[ "$1" == "wd-login" ]]; then
    shift
    cmd_wd_login "$@"
    exit $?
fi

# Check for setup-autologin command
if [[ "$1" == "setup-autologin" ]]; then
    shift
    cmd_setup_autologin "$@"
    exit $?
fi

# Check for scan-and-clean-all command
if [[ "$1" == "scan-and-clean-all" ]]; then
    client_user="${2-wsprdaemon}"
    
    # Ensure we have a valid connection to GW2
    if ! ensure_gw2_connection; then
        exit 1
    fi
    echo "Using connection: ${WD_RAC_CONNECTION_PATH}"
    echo ""
    
    log_file="wd-gw-cleanup-$(date +%Y%m%d_%H%M%S).log"
    
    echo "Scanning RACs 1-213 and cleaning GW keys where possible..."
    echo "Log file: ${log_file}"
    echo ""
    echo "Started at $(date)" | tee "${log_file}"
    echo "======================================" | tee -a "${log_file}"
    
    declare -i cleaned_count=0
    declare -i failed_count=0
    declare -i skipped_count=0
    declare -a cleaned_racs=()
    declare -a failed_racs=()
    
    declare -i no_port_count=0

    for client_rac in {1..213}; do
        client_ip_port=$(( 35800 + client_rac ))

        # All output goes through tee so RAC# appears in both terminal and log
        printf "RAC #%-3d port %-5d  " "${client_rac}" "${client_ip_port}" | tee -a "${log_file}"

        # Test if port is open
        if ! nc -z -w 2 ${WD_RAC_SERVER} ${client_ip_port} >/dev/null 2>&1; then
            echo "SKIP  - port closed" | tee -a "${log_file}"
            ((no_port_count++))
            continue
        fi

        # Test if we can SSH in (only tests autologin, doesn't use password)
        ssh_err=$(ssh ${SSH_OPTS} -p "${client_ip_port}" -o BatchMode=yes \
            "${client_user}@${WD_RAC_SERVER}" "exit 0" 2>&1)
        if [[ $? -ne 0 ]]; then
            # Classify the SSH failure reason
            reason="unknown SSH error"
            if echo "${ssh_err}" | grep -qi "host key\|REMOTE HOST IDENTIFICATION"; then
                reason="host key mismatch (run clean-gw-keys first)"
            elif echo "${ssh_err}" | grep -qi "Permission denied\|publickey"; then
                reason="no autologin key (run setup-autologin)"
            elif echo "${ssh_err}" | grep -qi "Connection refused"; then
                reason="connection refused (SSH not listening)"
            elif echo "${ssh_err}" | grep -qi "timed out\|No route"; then
                reason="connection timed out"
            fi
            echo "SKIP  - autologin failed: ${reason}" | tee -a "${log_file}"
            ((skipped_count++))
            continue
        fi

        # We can autologin, so clean the GW keys
        if wd-remove-gw-keys-from-client "${client_ip_port}" "${client_user}" >/dev/null 2>&1; then
            echo "CLEANED" | tee -a "${log_file}"
            ((cleaned_count++))
            cleaned_racs+=("${client_rac}")
        else
            echo "FAILED - error cleaning keys" | tee -a "${log_file}"
            ((failed_count++))
            failed_racs+=("${client_rac}")
        fi
    done
    
    echo "" | tee -a "${log_file}"
    echo "======================================" | tee -a "${log_file}"
    echo "Scan completed at $(date)" | tee -a "${log_file}"
    echo "" | tee -a "${log_file}"
    echo "Summary:" | tee -a "${log_file}"
    echo "  Cleaned:      ${cleaned_count}" | tee -a "${log_file}"
    echo "  Failed:       ${failed_count}" | tee -a "${log_file}"
    echo "  No autologin: ${skipped_count}" | tee -a "${log_file}"
    echo "  Port closed:  ${no_port_count}" | tee -a "${log_file}"
    echo "  Total:        213" | tee -a "${log_file}"
    echo "" | tee -a "${log_file}"
    
    if (( ${#cleaned_racs[@]} > 0 )); then
        echo "RACs cleaned (${#cleaned_racs[@]}):" | tee -a "${log_file}"
        for rac in "${cleaned_racs[@]}"; do
            echo "  RAC #${rac}" | tee -a "${log_file}"
        done
        echo "" | tee -a "${log_file}"
    fi
    
    if (( ${#failed_racs[@]} > 0 )); then
        echo "RACs with errors (${#failed_racs[@]}):" | tee -a "${log_file}"
        for rac in "${failed_racs[@]}"; do
            echo "  RAC #${rac}" | tee -a "${log_file}"
        done
        echo "" | tee -a "${log_file}"
    fi
    
    echo "Full log saved to: ${log_file}"
    exit 0
fi

if ! is_uint "$1"; then
    echo "ERROR: RAC_NUMBER must be a positive integer, got: '$1'"
    echo ""
    echo "Usage: $0 <RAC_NUMBER> [client_user]"
    echo ""
    echo "Example:"
    echo "  $0 129"
    exit 1
fi

wd-client-to-server-setup $@
