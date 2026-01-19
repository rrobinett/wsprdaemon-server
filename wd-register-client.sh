#!/bin/bash
###############################################################################
### wd-register-client.sh v3.4.2
###
### Script to register WSPRDAEMON client stations for SFTP uploads
### Creates user accounts on gateway servers and configures client access
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
###   ./wd-register-client.sh scan-and-clean-all [client_user]
###   ./wd-register-client.sh -V|--version
###
###############################################################################

declare VERSION="3.4.2"

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
    wd-ssr-client-lookup "client_user_name" "client_user_password" ${client_rac}
    rc=$? ; if (( rc )); then
        echo "ERROR: can't find user with RAC ${client_rac} in encrypted config"
        return 1
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
### Command Line Handling
###############################################################################

show_version() {
    echo "wd-register-client.sh version ${VERSION}"
    echo "Project: ${WD_RAC_PROJECT_DIR}"
}

# Handle -V/--version flag first
if [[ "${1-}" == "-V" || "${1-}" == "--version" ]]; then
    show_version
    exit 0
fi

# Validate arguments before calling the function
if [[ $# -lt 1 ]]; then
    echo "ERROR: Missing required argument"
    echo ""
    echo "Usage: $0 <RAC_NUMBER> [client_user]"
    echo "   or: $0 clean-gw-keys <RAC_NUMBER> [client_user]"
    echo "   or: $0 scan-and-clean-all [client_user]"
    echo "   or: $0 -V|--version"
    echo ""
    echo "Arguments:"
    echo "  RAC_NUMBER    : The Remote Access Channel number (positive integer)"
    echo "  client_user   : Optional. The username on the client (default: wsprdaemon)"
    echo ""
    echo "Commands:"
    echo "  $0 <RAC_NUMBER>                  - Full client registration"
    echo "  $0 clean-gw-keys <RAC_NUMBER>    - Only remove GW1/GW2 keys from one client"
    echo "  $0 scan-and-clean-all            - Scan all RACs 1-213 and clean where possible"
    echo "  $0 -V|--version                  - Show version"
    echo ""
    echo "Prerequisites:"
    echo "  Requires wd-rac project with encrypted config files:"
    echo "    ${WD_RAC_PROJECT_DIR}/conf/rac-master.conf.enc"
    echo "  You will be prompted for the passphrase (NOT cached for security)."
    echo ""
    echo "Example:"
    echo "  $0 129                           # Register RAC 129"
    echo "  $0 129 myuser                    # Register with custom username"
    echo "  $0 clean-gw-keys 129             # Clean only RAC 129"
    echo "  $0 scan-and-clean-all            # Scan and clean all accessible RACs"
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
    
    for client_rac in {1..213}; do
        client_ip_port=$(( 35800 + client_rac ))
        
        # Show progress
        printf "RAC #%-3d [%3d/213] " "${client_rac}" "${client_rac}"
        
        # Test if port is open
        if ! nc -z -w 2 ${WD_RAC_SERVER} ${client_ip_port} >/dev/null 2>&1; then
            echo "SKIP - No connection" | tee -a "${log_file}"
            ((skipped_count++))
            continue
        fi
        
        # Test if we can SSH in (only tests autologin, doesn't use password)
        if ! ssh ${SSH_OPTS} -p "${client_ip_port}" -o BatchMode=yes "${client_user}@${WD_RAC_SERVER}" "exit 0" >/dev/null 2>&1; then
            echo "SKIP - Can't autologin" | tee -a "${log_file}"
            ((skipped_count++))
            continue
        fi
        
        # We can autologin, so clean the GW keys
        if wd-remove-gw-keys-from-client "${client_ip_port}" "${client_user}" >/dev/null 2>&1; then
            echo "CLEANED" | tee -a "${log_file}"
            ((cleaned_count++))
            cleaned_racs+=("${client_rac}")
        else
            echo "FAILED - Error cleaning keys" | tee -a "${log_file}"
            ((failed_count++))
            failed_racs+=("${client_rac}")
        fi
    done
    
    echo "" | tee -a "${log_file}"
    echo "======================================" | tee -a "${log_file}"
    echo "Scan completed at $(date)" | tee -a "${log_file}"
    echo "" | tee -a "${log_file}"
    echo "Summary:" | tee -a "${log_file}"
    echo "  Cleaned: ${cleaned_count}" | tee -a "${log_file}"
    echo "  Failed:  ${failed_count}" | tee -a "${log_file}"
    echo "  Skipped: ${skipped_count}" | tee -a "${log_file}"
    echo "  Total:   213" | tee -a "${log_file}"
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
