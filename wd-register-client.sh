if [[ -f ~/wsprdaemon/bash-aliases ]]; then
    source ~/wsprdaemon/bash-aliases
fi

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

declare SSR_CONF_FILE=~/.ssr.conf             ### Default to load the WD conf file
declare SSR_CONF_LOCAL_FILE=~/.ssr.conf.local   ### If present contains additional logins

###  Returns 0 if arg is an unsigned integer, else 1
function is_int()  { if [[ "$1" =~ ^-?[0-9]+$ ]]; then return 0; else return 1; fi }
function is_uint() { if [[ "$1" =~   ^[0-9]+$ ]]; then return 0; else return 1; fi }

function ssr_channel_id_to_rac_list_index() {
    local -n __return_rac_index=$1
    local wanted_rac_channel_id=$2

    local ssr_entry_index
    for (( ssr_entry_index=0; ssr_entry_index < ${#FRPS_REMOTE_ACCESS_LIST[@]}; ++ssr_entry_index )); do
        local rac_entry_list=(${FRPS_REMOTE_ACCESS_LIST[${ssr_entry_index}]//,/ })
        local entry_rac_channel_id=${rac_entry_list[0]}
        if [[ ${entry_rac_channel_id} == ${wanted_rac_channel_id} ]]; then
            (( ${verbosity-0} > 1 )) && echo "Found RAC_ID ${wanted_rac_channel_id} in FRPS_REMOTE_ACCESS_LIST[${ssr_entry_index}]" 1>&2
            __return_rac_index=${ssr_entry_index}
            return 0
        fi
    done
    (( ${verbosity-0} )) && echo "Couldn't find RAC_ID ${wanted_rac_channel_id} in FRPS_REMOTE_ACCESS_LIST[${ssr_entry_index}]" 1>&2
    __return_rac_index=-1
    return 0
 }

 ### Given:   RAC_ID
 ### Returns: that server's WD user and password
function wd-ssr-client-lookup()
{
    local -n __client_user_name=$1
    local -n __client_user_password=$2
    local rac_id="${3-}"
    if ! is_uint ${rac_id}; then
        echo "ERROR: the RAC argument '${rac_id}' is not an unsigned integer"
        return 1
    fi

    if ! [[ ${SSR_CONF_FILE} ]]; then
        echo "ERROR: ' '${SSR_CONF_FILE}' file does not exist on this server"
        return 1
    fi

    (( ${verbosity-0} > 1 )) && echo "Reading '${SSR_CONF_FILE}' file"
    source ${SSR_CONF_FILE}

    if [[ -f ${SSR_CONF_LOCAL_FILE} ]]; then
        (( ${verbosity-0} )) && echo "Reading '${SSR_CONF_FILE}' file"
        source ${SSR_CONF_LOCAL_FILE}
    fi

    local rac_list_index
    ssr_channel_id_to_rac_list_index "rac_list_index" ${rac_id}
    rc=$? ; if (( rc < 0 )); then
        echo "ERROR: can't find RAC ${rac_id}"
    fi
    (( ${verbosity-0} > 1 )) && echo "Found that RAC_ID=${rac_id} is found in FRPS_REMOTE_ACCESS_LIST[${rac_list_index}]: '${FRPS_REMOTE_ACCESS_LIST[${rac_list_index}]}'"

    local user_password_list=( $( echo "${FRPS_REMOTE_ACCESS_LIST[${rac_list_index}]}" | cut -d',' -f5) )
    local rac_client_user_name=${user_password_list[0]}
    local rac_client_user_password=${user_password_list[1]}
    (( ${verbosity-0} > 1 )) && echo "Found that RAC_ID=${rac_id} reports that its WD Linux client's user name is '${rac_client_user_name}' and password is '${rac_client_user_password}'"

    __client_user_name=${rac_client_user_name}
    __client_user_password=${rac_client_user_password}

    return 0
}

### Configuration - sourced from /etc/wsprdaemon/client-register.conf if available
CONFIG_FILE="/etc/wsprdaemon/client-register.conf"

# Default values (used if config file doesn't exist)
WD_RAC_SERVER="${WD_RAC_SERVER:-wd0}"
WD_SERVER_FQDN="${WD_SERVER_FQDN:-wd00.wsprdaemon.org}"
WD_BACKUP_SERVERS="${WD_BACKUP_SERVERS:-wd0.wsprdaemon.org}"  # Space-separated list of backup servers to replicate to

# Source config file if it exists
if [[ -f "${CONFIG_FILE}" ]]; then
    (( ${verbosity-0} )) && echo "Loading configuration from ${CONFIG_FILE}"
    source "${CONFIG_FILE}"
else
    (( ${verbosity-0} )) && echo "Using default configuration (${CONFIG_FILE} not found)"
fi

# Build array of all servers for WD_SERVER_USER_LIST
declare -a ALL_WD_SERVERS=("${WD_SERVER_FQDN}")
if [[ -n "${WD_BACKUP_SERVERS}" ]]; then
    for backup_server in ${WD_BACKUP_SERVERS}; do
        ALL_WD_SERVERS+=("${backup_server}")
    done
fi

### Replicate user account from WD00 (local) to a remote server
function wd-replicate-user-to-server()
{
    local username=$1
    local target_server=$2
    
    if [[ -z "$username" ]]; then
        echo "ERROR: wd-replicate-user-to-server requires username argument"
        return 1
    fi
    
    if [[ -z "$target_server" ]]; then
        echo "ERROR: wd-replicate-user-to-server requires target_server argument"
        return 1
    fi
    
    # Verify user exists locally first
    if ! id "$username" > /dev/null 2>&1; then
        echo "ERROR: User '$username' does not exist on local server (WD00)"
        return 1
    fi
    
    # Verify authorized_keys exists locally (using sudo since we may not have permission)
    if ! sudo test -f "/home/${username}/.ssh/authorized_keys"; then
        echo "ERROR: /home/${username}/.ssh/authorized_keys does not exist on local server"
        return 1
    fi
    
    echo ""
    echo "=== Replicating user '$username' to server ${target_server} ==="
    
    # Check if target server is reachable
    if ! ssh -o ConnectTimeout=5 "${target_server}" "echo 'Server connection test'" > /dev/null 2>&1; then
        echo "WARNING: Cannot connect to server ${target_server}"
        echo "         User account will only be available on WD00"
        return 1
    fi
    
    # Check if sftponly group exists on target, create if not
    if ! ssh "${target_server}" "getent group sftponly > /dev/null 2>&1"; then
        echo "Creating sftponly group on ${target_server}..."
        if ! ssh "${target_server}" "sudo groupadd sftponly"; then
            echo "ERROR: Failed to create sftponly group on ${target_server}"
            return 1
        fi
    fi
    
    # Check if user exists on target server
    if ssh "${target_server}" "id '${username}' > /dev/null 2>&1"; then
        echo "User '$username' already exists on ${target_server}, updating configuration..."
    else
        echo "Creating user '$username' on ${target_server}..."
        if ! ssh "${target_server}" "sudo useradd -m -s /bin/false -G sftponly '${username}'"; then
            echo "ERROR: Failed to create user on ${target_server}"
            return 1
        fi
        
        # Set root ownership on home directory for chroot
        if ! ssh "${target_server}" "sudo chown root:root '/home/${username}' && sudo chmod 755 '/home/${username}'"; then
            echo "ERROR: Failed to set chroot permissions on ${target_server}"
            return 1
        fi
        
        # Lock the account
        if ! ssh "${target_server}" "sudo passwd -l '${username}'"; then
            echo "ERROR: Failed to lock user account on ${target_server}"
            return 1
        fi
        
        echo "Created user '$username' on ${target_server}"
    fi
    
    # Create .ssh directory on target server
    echo "Setting up .ssh directory on ${target_server}..."
    if ! ssh "${target_server}" "sudo mkdir -p '/home/${username}/.ssh' && \
                                sudo chmod 700 '/home/${username}/.ssh'"; then
        echo "ERROR: Failed to create .ssh directory on ${target_server}"
        return 1
    fi
    
    # Copy authorized_keys from WD00 to target server
    echo "Copying authorized_keys from WD00 to ${target_server}..."
    local authorized_keys_content=$(sudo cat "/home/${username}/.ssh/authorized_keys")
    if [[ -z "$authorized_keys_content" ]]; then
        echo "ERROR: authorized_keys file is empty on WD00"
        return 1
    fi
    
    if ! echo "$authorized_keys_content" | ssh "${target_server}" "sudo tee '/home/${username}/.ssh/authorized_keys' > /dev/null"; then
        echo "ERROR: Failed to copy authorized_keys to ${target_server}"
        return 1
    fi
    
    # Set proper permissions on target server
    if ! ssh "${target_server}" "sudo chmod 600 '/home/${username}/.ssh/authorized_keys' && \
                                sudo chown -R '${username}:${username}' '/home/${username}/.ssh'"; then
        echo "ERROR: Failed to set .ssh permissions on ${target_server}"
        return 1
    fi
    
    # Create uploads directory on target server
    echo "Creating uploads directory on ${target_server}..."
    if ! ssh "${target_server}" "sudo mkdir -p '/home/${username}/uploads' && \
                                sudo chown '${username}:${username}' '/home/${username}/uploads' && \
                                sudo chmod 755 '/home/${username}/uploads'"; then
        echo "ERROR: Failed to create uploads directory on ${target_server}"
        return 1
    fi
    
    echo "✅ Successfully replicated user '$username' to ${target_server}"
    
    return 0
}

### Replicate user to all configured backup servers
function wd-replicate-user-to-all-backups()
{
    local username=$1
    local success_count=0
    local fail_count=0
    declare -a successful_servers=("${WD_SERVER_FQDN}")  # WD00 is always included
    
    if [[ -z "${WD_BACKUP_SERVERS}" ]]; then
        (( ${verbosity-0} )) && echo "No backup servers configured for replication"
        return 0
    fi
    
    for backup_server in ${WD_BACKUP_SERVERS}; do
        if wd-replicate-user-to-server "${username}" "${backup_server}"; then
            ((success_count++))
            successful_servers+=("${backup_server}")
        else
            ((fail_count++))
        fi
    done
    
    # Export list of successful servers for use by calling functions
    _SUCCESSFUL_SERVERS=("${successful_servers[@]}")
    
    if (( fail_count > 0 )); then
        echo ""
        echo "⚠️  Replication completed with ${fail_count} failure(s) and ${success_count} success(es)"
        return 1
    else
        echo ""
        echo "✅ User replicated to all ${success_count} backup server(s)"
        return 0
    fi
}

### Build WD_SERVER_USER_LIST bash array declaration for client's wsprdaemon.conf
function build-wd-server-user-list()
{
    local username=$1
    shift
    local -a servers=("$@")
    
    if [[ -z "$username" ]]; then
        echo "ERROR: build-wd-server-user-list requires username"
        return 1
    fi
    
    if (( ${#servers[@]} == 0 )); then
        echo "ERROR: build-wd-server-user-list requires at least one server"
        return 1
    fi
    
    # Build the array declaration
    local list_str="WD_SERVER_USER_LIST=("
    for server in "${servers[@]}"; do
        list_str+="\"${username}@${server}\" "
    done
    list_str+=")"
    
    echo "${list_str}"
}

### Print formatted server list for display
function format-server-list()
{
    local username=$1
    shift
    local -a servers=("$@")
    
    echo "Client can upload to:"
    local idx=0
    for server in "${servers[@]}"; do
        if (( idx == 0 )); then
            echo "  ${username}@${server}  (primary)"
        else
            echo "  ${username}@${server}  (backup)"
        fi
        ((idx++))
    done
}

### Replicate existing users to a target server
function wd-replicate-existing-users()
{
    local target_server=$1
    shift
    local -a usernames=("$@")
    
    if [[ -z "$target_server" ]]; then
        echo "ERROR: --replicate requires target server argument"
        echo "Usage: $0 --replicate <SERVER> <USER1> [<USER2> ...]"
        return 1
    fi
    
    if (( ${#usernames[@]} == 0 )); then
        echo "ERROR: --replicate requires at least one username"
        echo "Usage: $0 --replicate <SERVER> <USER1> [<USER2> ...]"
        return 1
    fi
    
    echo "=== Replication Mode ==="
    echo "Target server: ${target_server}"
    echo "Users to replicate: ${usernames[*]}"
    echo ""
    
    local success_count=0
    local fail_count=0
    local skip_count=0
    
    for username in "${usernames[@]}"; do
        echo "----------------------------------------"
        echo "Processing user: ${username}"
        
        # Check if user exists locally
        if ! id "$username" > /dev/null 2>&1; then
            echo "⚠️  User '$username' does not exist on local server (WD00) - SKIPPING"
            ((skip_count++))
            continue
        fi
        
        # Check if user already exists on target server
        if ssh "${target_server}" "id '${username}' > /dev/null 2>&1"; then
            echo "ℹ️  User '$username' already exists on ${target_server}"
            echo "   Updating configuration..."
        fi
        
        # Replicate the user
        if wd-replicate-user-to-server "${username}" "${target_server}"; then
            ((success_count++))
        else
            ((fail_count++))
        fi
    done
    
    echo ""
    echo "========================================"
    echo "Replication Summary:"
    echo "  Successful: ${success_count}"
    echo "  Failed:     ${fail_count}"
    echo "  Skipped:    ${skip_count}"
    echo "  Total:      ${#usernames[@]}"
    echo "========================================"
    
    if (( fail_count > 0 )); then
        return 1
    else
        return 0
    fi
}



### Manual registration mode - no SSH to client required
function wd-client-manual-setup()
{
    local username=$1
    local pubkey_file=$2
    
    if [[ -z "$username" || -z "$pubkey_file" ]]; then
        echo "ERROR: Manual mode requires username and public key file"
        return 1
    fi
    
    if [[ ! -f "$pubkey_file" ]]; then
        echo "ERROR: Public key file not found: $pubkey_file"
        return 1
    fi
    
    # Sanitize username (replace / with _)
    local sanitized_username=$(echo "$username" | tr '/' '_')
    
    if [[ "$username" != "$sanitized_username" ]]; then
        echo "NOTE: Username '$username' contains invalid characters for Linux username"
        echo "      Using sanitized username: '$sanitized_username'"
    fi
    
    echo "=== Manual Registration Mode ==="
    echo "Creating SFTP-only user: $sanitized_username"
    
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

    # Create the user account (no password, no shell)
    if id "${sanitized_username}" > /dev/null 2>&1; then
        echo "User '${sanitized_username}' with id '$(id "${sanitized_username}")' already exists on this server"
        echo "Will update their public key"
    else
        if ! sudo useradd -m -s /bin/false -G sftponly "${sanitized_username}"; then
            echo "ERROR: Failed to create user '${sanitized_username}'"
            return 1
        fi
        echo "Created new user '${sanitized_username}'"
        
        # For chroot SFTP, the home directory MUST be owned by root
        if ! sudo chown root:root "/home/${sanitized_username}"; then
            echo "ERROR: Failed to set root ownership on home directory"
            return 1
        fi
        
        if ! sudo chmod 755 "/home/${sanitized_username}"; then
            echo "ERROR: Failed to set permissions on home directory"
            return 1
        fi
        
        echo "Set proper chroot ownership (root:root) on /home/${sanitized_username}"
    fi

    # Create the SSH directory and set proper permissions
    if ! sudo mkdir -p "/home/${sanitized_username}/.ssh"; then
        echo "ERROR: Failed to create .ssh directory"
        return 1
    fi

    if ! sudo chmod 700 "/home/${sanitized_username}/.ssh"; then
        echo "ERROR: Failed to set permissions on .ssh directory"
        return 1
    fi

    # Install the provided public key
    local pubkey_content=$(cat "$pubkey_file")
    if [[ -z "$pubkey_content" ]]; then
        echo "ERROR: Public key file is empty: $pubkey_file"
        return 1
    fi
    
    if sudo grep -q "$pubkey_content" "/home/${sanitized_username}/.ssh/authorized_keys" 2>/dev/null; then
        echo "This client's public key is already in /home/${sanitized_username}/.ssh/authorized_keys"
    else
        if ! echo "$pubkey_content" | sudo tee "/home/${sanitized_username}/.ssh/authorized_keys" > /dev/null; then
            echo "ERROR: Failed to add public key to /home/${sanitized_username}/.ssh/authorized_keys"
            return 1
        fi
        echo "Added client's public key to /home/${sanitized_username}/.ssh/authorized_keys"
    fi

    # Set proper permissions on authorized_keys
    if ! sudo chmod 600 "/home/${sanitized_username}/.ssh/authorized_keys"; then
        echo "ERROR: Failed to set permissions on authorized_keys"
        return 1
    fi

    # .ssh directory must be owned by the user
    if ! sudo chown -R "${sanitized_username}:${sanitized_username}" "/home/${sanitized_username}/.ssh"; then
        echo "ERROR: Failed to set ownership of .ssh directory"
        return 1
    fi

    # Lock the account (disable password login)
    if ! sudo passwd -l "${sanitized_username}"; then
        echo "ERROR: Failed to lock user account"
        return 1
    fi

    # Create uploads directory
    if ! sudo mkdir -p "/home/${sanitized_username}/uploads"; then
        echo "ERROR: Failed to create uploads directory"
        return 1
    fi

    if ! sudo chown "${sanitized_username}:${sanitized_username}" "/home/${sanitized_username}/uploads"; then
        echo "ERROR: Failed to set ownership of uploads directory"
        return 1
    fi

    if ! sudo chmod 755 "/home/${sanitized_username}/uploads"; then
        echo "ERROR: Failed to set permissions on uploads directory"
        return 1
    fi
    
    echo ""
    echo "=== SUCCESS: User ${sanitized_username} is configured for SFTP-only access on WD00 ==="
    
    # Replicate user account to all backup servers
    declare -a _SUCCESSFUL_SERVERS=("${WD_SERVER_FQDN}")  # Start with primary
    wd-replicate-user-to-all-backups "${sanitized_username}"
    
    # Build the WD_SERVER_USER_LIST array for client's config
    local wd_server_user_list=$(build-wd-server-user-list "${sanitized_username}" "${_SUCCESSFUL_SERVERS[@]}")
    
    echo ""
    echo "============================================================================"
    echo "Email the following line to the client to add to their wsprdaemon.conf:"
    echo "============================================================================"
    echo ""
    echo "${wd_server_user_list}"
    echo ""
    echo "============================================================================"
    echo ""
    format-server-list "${sanitized_username}" "${_SUCCESSFUL_SERVERS[@]}"
    echo ""
    
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

    set -u
    local client_ip_port=$(( 35800 + client_rac ))

    local client_user_password
    wd-ssr-client-lookup "client_user_name" "client_user_password" ${client_rac}
    rc=$? ; if (( rc )); then
        echo "ERROR: can't find user with RAC ${client_rac} in .ssr.conf"
        return 1
    fi
    (( ${verbosity-0} )) && echo "Found user '${client_user_name}' with password '${client_user_password}' for RAC ${client_rac}"

    local client_db_dir="~/wsprdaemon/signal-levels"
    (( ${verbosity-0} )) && echo "Looking for the RAC's most recent WD database in ${client_db_dir} on the client's IP:port ${WD_RAC_SERVER}:${client_ip_port}"
    local client_most_recent_spot_db=$(ssh -p "${client_ip_port}" "${client_user}@${WD_RAC_SERVER}" "ls -t ${client_db_dir}/wsprdaemon_spots_*.csv 2> /dev/null | head -1")
    rc=$? ; if (( rc )); then
        echo "ERROR: can't get a list of spot databases from RAC's ${client_db_dir}"
        return 1
    fi
    if [[ -z ${client_most_recent_spot_db} ]]; then
        echo "ERROR: found no spot databases in RAC's ${client_db_dir}"
        return 1
    fi
    (( ${verbosity-0} )) && echo "Found the RAC's most recent WD database '${client_most_recent_spot_db}'"

    local -a reporter_id_list
    mapfile -t reporter_id_list < <(ssh -p "${client_ip_port}" "${client_user}@${WD_RAC_SERVER}" "tail -1000 ${client_most_recent_spot_db} | awk -F, '{print \$3}' | sort | uniq -c | sort -n | tail -1")
    rc=$? ; if (( rc )); then
        echo "ERROR: can't extract the client's REPORTER_ID from ${client_most_recent_spot_db}"
        return 1
    fi
    if (( ${#reporter_id_list[@]} == 0 )); then
        echo "ERROR: found no REPORTER_ID rows in ${client_most_recent_spot_db}"
        return 1
    fi
    local client_reporter_id=${reporter_id_list[1]}
    if [[ -z ${client_reporter_id} ]]; then
        echo "ERROR: the most recent REPORTER_ID in ${client_most_recent_spot_db} is empty"
        return 1
    fi
    (( ${verbosity-0} )) && echo "Found the most recent REPORTER_ID is ${client_reporter_id}.  See that value is declared in the RAC's wsprdaemon.conf"
    
    # CRITICAL FIX: Sanitize the reporter ID for use as a Linux username
    # Replace forward slashes with underscores (e.g., "KFS/SW" becomes "KFS_SW")
    local sanitized_reporter_id=$(echo "${client_reporter_id}" | tr '/' '_')
    
    if [[ "${client_reporter_id}" != "${sanitized_reporter_id}" ]]; then
        echo "NOTE: Reporter ID '${client_reporter_id}' contains invalid characters for Linux username"
        echo "      Using sanitized username: '${sanitized_reporter_id}'"
        echo "      Will configure WD_SERVER_USER_LIST in client's wsprdaemon.conf"
    fi

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
    mapfile -t client_key_list < <(ssh -p "${client_ip_port}" "${client_user}@${WD_RAC_SERVER}" "cat ~/.ssh/*.pub")
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
    (( ${verbosity-0} )) && echo "User ${sanitized_reporter_id}'s Linux account is set up on this server"

    # Test SFTP upload capability - USE SANITIZED USERNAME
    local test_file="sftp_test_$(date +%s).txt"
    local test_content="SFTP test from ${WD_RAC_SERVER} at $(date)"

    # Create a test file on the remote server and attempt upload
    (( ${verbosity-0} )) && echo "Testing file uploads from User ${sanitized_reporter_id}'s WD client server"
    if ! ssh -p "${client_ip_port}" "${client_user}@${WD_RAC_SERVER}" \
        "echo '${test_content}' > /tmp/${test_file} && \
         sftp -o StrictHostKeyChecking=no -P ${SFTP_PORT:-22} ${sanitized_reporter_id}@${WD_SERVER_FQDN} <<< 'put /tmp/${test_file} uploads/${test_file}' && \
         rm -f /tmp/${test_file}"; then
        echo "ERROR: SFTP upload test failed for user ${sanitized_reporter_id}"
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

    echo ""
    echo "SUCCESS: SFTP upload test passed for user ${sanitized_reporter_id} (reporter ID: ${client_reporter_id})"
    
    # Replicate user account to all backup servers
    declare -a _SUCCESSFUL_SERVERS=("${WD_SERVER_FQDN}")  # Start with primary
    wd-replicate-user-to-all-backups "${sanitized_reporter_id}"
    
    # Build the WD_SERVER_USER_LIST array declaration
    local wd_server_user_list=$(build-wd-server-user-list "${sanitized_reporter_id}" "${_SUCCESSFUL_SERVERS[@]}")
    
    # Write the array to the client's wsprdaemon.conf
    local config_file="\${HOME}/wsprdaemon/wsprdaemon.conf"
    echo "Writing WD_SERVER_USER_LIST to client's wsprdaemon.conf..."
    
    # Remove old WD_SERVER_USER or WD_SERVER_USER_LIST lines and add new array
    ssh -p "${client_ip_port}" "${client_user}@${WD_RAC_SERVER}" \
        "sed -i '/^[[:space:]]*WD_SERVER_USER=/d' \"${config_file}\"; \
         sed -i '/^[[:space:]]*WD_SERVER_USER_LIST=/d' \"${config_file}\"; \
         echo '${wd_server_user_list} # Added by the WD server. Do not modify or delete' >> \"${config_file}\""
    
    # Verify it was written
    ssh -p "${client_ip_port}" "${client_user}@${WD_RAC_SERVER}" "grep -q '^WD_SERVER_USER_LIST=' \"${config_file}\""
    rc=$?; if (( rc )); then
        echo "ERROR: failed to add WD_SERVER_USER_LIST to ${config_file} on RAC #${client_rac}"
        return 1
    fi
    (( ${verbosity-0} )) && echo "Successfully wrote WD_SERVER_USER_LIST to ${config_file} on RAC #${client_rac}"
    
    echo ""
    echo "============================================================================"
    echo "The following array has been added to the client's wsprdaemon.conf:"
    echo "============================================================================"
    echo ""
    echo "${wd_server_user_list}"
    echo ""
    echo "============================================================================"
    echo ""
    format-server-list "${sanitized_reporter_id}" "${_SUCCESSFUL_SERVERS[@]}"
    echo ""
}

# Parse command line arguments
if [[ $# -lt 1 ]]; then
    cat << 'EOF'
ERROR: Missing required argument

Usage: 
  Automatic mode (SSH to client):
    ./wd-register-client.sh <RAC_NUMBER> [client_user]
  
  Manual mode (no SSH to client):
    ./wd-register-client.sh --manual <RAC_NUMBER> <pubkey_file>
    ./wd-register-client.sh --manual <username> <pubkey_file>
  
  Replicate mode (sync existing users to backup server):
    ./wd-register-client.sh --replicate <SERVER> <USER1> [<USER2> ...]

Arguments:
  RAC_NUMBER    : The Remote Access Channel number (positive integer)
  username      : Linux username to create on server (alphanumeric, _ allowed)
  pubkey_file   : Path to file containing client's SSH public key
  client_user   : Optional. Username on client (default: wsprdaemon)
  SERVER        : Target server hostname/FQDN for replication
  USER1, USER2  : Usernames of existing local users to replicate

Examples:
  Automatic registration (connects to client via SSH):
    ./wd-register-client.sh 129
    ./wd-register-client.sh 129 myuser

  Manual registration (client provides public key directly):
    ./wd-register-client.sh --manual 129 /path/to/client_id_rsa.pub
    ./wd-register-client.sh --manual KJ6MKI /path/to/client_id_rsa.pub

  Replicate existing users to backup server:
    ./wd-register-client.sh --replicate wd1.wsprdaemon.org KJ6MKI W3XYZ
    ./wd-register-client.sh --replicate wd0.wsprdaemon.org KJ6MKI

Features:
  - Creates SFTP-only user account with chroot jail
  - Automatically replicates users to configured backup servers
  - Creates WD_SERVER_USER_LIST array in client's wsprdaemon.conf
  - Clients can SFTP to any configured server
  - Manual mode skips SSH connection and SFTP upload test
  - Replicate mode syncs existing users to any target server

Configuration:
  Config file: /etc/wsprdaemon/client-register.conf
  Variables: WD_SERVER_FQDN, WD_RAC_SERVER, WD_BACKUP_SERVERS
EOF
    exit 1
fi

# Check if this is replicate mode
if [[ "$1" == "--replicate" ]]; then
    shift
    if [[ $# -lt 2 ]]; then
        echo "ERROR: --replicate requires at least 2 arguments: <SERVER> <USER1> [<USER2> ...]"
        echo ""
        echo "Examples:"
        echo "  ./wd-register-client.sh --replicate wd1.wsprdaemon.org KJ6MKI"
        echo "  ./wd-register-client.sh --replicate wd0.wsprdaemon.org KJ6MKI W3XYZ N6GN"
        exit 1
    fi
    
    target_server="$1"
    shift
    usernames=("$@")
    
    wd-replicate-existing-users "${target_server}" "${usernames[@]}"
    exit $?
fi

# Check if this is manual mode
if [[ "$1" == "--manual" ]]; then
    shift
    if [[ $# -ne 2 ]]; then
        echo "ERROR: Manual mode requires exactly 2 arguments: <RAC_NUMBER|username> <pubkey_file>"
        echo ""
        echo "Examples:"
        echo "  ./wd-register-client.sh --manual 129 /tmp/client_key.pub"
        echo "  ./wd-register-client.sh --manual KJ6MKI /tmp/client_key.pub"
        exit 1
    fi
    
    username_or_rac="$1"
    pubkey_file="$2"
    
    # Determine if first arg is RAC number or username
    if is_uint "$username_or_rac"; then
        # It's a RAC number - we need to derive the username from RAC lookup
        client_rac="$username_or_rac"
        
        # Load config and lookup RAC info
        if [[ ! -f ${SSR_CONF_FILE} ]]; then
            echo "ERROR: ${SSR_CONF_FILE} file does not exist on this server"
            exit 1
        fi
        
        source ${SSR_CONF_FILE}
        if [[ -f ${SSR_CONF_LOCAL_FILE} ]]; then
            source ${SSR_CONF_LOCAL_FILE}
        fi
        
        # For manual mode with RAC, we'll use "rac${client_rac}" as the username
        # unless we can get better info from the config
        username="rac${client_rac}"
        
        echo "Manual mode: Using RAC #${client_rac} -> creating username: ${username}"
    else
        # It's a username directly
        username="$username_or_rac"
        echo "Manual mode: Using provided username: ${username}"
    fi
    
    wd-client-manual-setup "$username" "$pubkey_file"
    exit $?
fi

# Normal automatic mode - validate RAC number
if ! is_uint "$1"; then
    echo "ERROR: RAC_NUMBER must be a positive integer, got: '$1'"
    echo ""
    echo "Usage: $0 <RAC_NUMBER> [client_user]"
    echo "   or: $0 --manual <RAC_NUMBER|username> <pubkey_file>"
    echo ""
    echo "Example:"
    echo "  $0 129"
    echo "  $0 --manual 129 /tmp/key.pub"
    exit 1
fi

wd-client-to-server-setup $@
