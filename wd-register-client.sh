#!/bin/bash
#
# wd-register-client.sh v2.10.7
# 
# Script to register WSPRDAEMON client stations for SFTP uploads
# Creates user accounts on gateway servers and configures client access
#
# Usage:
#   ./wd-register-client.sh <client_rac_number> [--verbose]
#   ./wd-register-client.sh <client_rac_number> <reporter_id>  # Manual override
#   ./wd-register-client.sh --config <reporter_id> <psk>       # Create accounts (PSK=SSH pubkey)
#   ./wd-register-client.sh --scan-racs                        # Scan all RACs
#   ./wd-register-client.sh --register-batch                   # Register all RACs from need-reg file
#   ./wd-register-client.sh --update-clients                   # SSH to RACs with old versions
#   ./wd-register-client.sh --update-ssr                       # Update .ssr.conf with REPORTER_IDs
#   ./wd-register-client.sh --version
#
# Examples:
#   ./wd-register-client.sh 84              # Register client RAC 84 (auto-detect ID)
#   ./wd-register-client.sh 84 KJ6MKI       # Register with manual reporter ID
#   ./wd-register-client.sh 84 --verbose    # Register with verbose output  
#   ./wd-register-client.sh --config KJ6MKI "ssh-rsa AAAAB3..."  # Create accounts + output config
#   ./wd-register-client.sh --scan-racs     # Check connectivity to all RACs
#   ./wd-register-client.sh --register-batch # Register all RACs needing config
#   ./wd-register-client.sh --update-clients # Update RACs with old WD versions
#   ./wd-register-client.sh --update-ssr    # Generate .ssr.conf.updated with REPORTER_IDs
#   ./wd-register-client.sh --version       # Show version only
#
# Changes in v2.10.7:
#   - NEW: Added RECEIVER_LIST fallback for Reporter ID (for new RACs without uploads)
#   - Gets third field from first RECEIVER_LIST element in wsprdaemon.conf
#   - Helps with RACs like #64 that haven't decoded/uploaded spots yet
#
# Changes in v2.10.6:
#   - FIXED: Reporter ID now extracted using same method as Linux user account name
#   - Looks for "my call CALLSIGN and/or" pattern in upload_to_wsprnet_daemon.log
#   - This matches how the registration function determines the account name
#   - Note: REMOTE_ACCESS_CHANNEL in wsprdaemon.conf equals RAC number
#
# Changes in v2.10.5:
#   - FIXED: TCP State column properly aligned (10 chars to match header)
#   - IMPROVED: Reporter ID now extracted from wsprdaemon.conf REPORTER_ID field
#   - Already checks localhost first for RAC ports, then falls back to remote gateway
#
# Changes in v2.10.4:
#   - FIXED: TCP State column width now matches header (was 1 char too narrow)
#   - IMPROVED: Check localhost first for RAC ports, then fallback to remote gateway
#   - FIXED: Reporter ID now shows actual ID from upload log (not wd_user from .ssr.conf)
#
# Changes in v2.10.3:
#   - IMPROVED: WD Cfg column now shows "✓ OK" or "✗ NO" for better readability
#   - FIXED: Column header alignment - all headers now properly align with data
#   - TCP State column renamed and resized for consistent formatting
#
# Changes in v2.10.2:
#   - FIXED: Always use gw2 for RAC connections (RAC ports only exist on gw2)
#   - Script now works correctly when run from either GW1 or GW2
#   - WD_RAC_SERVER always set to "gw2" regardless of which server runs script
#
# Changes in v2.10.1:
#   - FIXED: When running on GW1, use gw1 for RAC connections (not gw2)
#   - RAC ports are only accessible locally on each gateway
#   - WD_RAC_SERVER now correctly set to local server name
#
# Changes in v2.10.0:
#   - NEW: Track and report count of WD cfg active RACs in --scan-racs
#   - NEW: List RACs where SSH OK but WD config missing (✗)
#   - NEW: Save need-registration list to 'wd-register-clients.need-reg'
#   - NEW: --register-batch option to register all RACs from need-reg file
#   - NEW: Track WD versions and identify highest version
#   - NEW: Save RACs with outdated versions to 'wd-register-clients.need-update'
#   - NEW: --update-clients option to SSH to each RAC for manual updates
#
# Changes in v2.9.2:
#   - NEW: --scan-racs shows WD Cfg column (✓/✗ if WD_SERVER_USER_LIST configured)
#   - NEW: --scan-racs shows WD Version column (from wd_version.txt-git count)
#   - FIXED: Suppressed verbose output from ssh-copy-id
#
# Changes in v2.9.1:
#   - IMPROVED: --scan-racs now auto-fixes SSH key issues using sshpass
#   - FIXED: Password extraction now uses field 5, second space-separated word
#   - NEW: --update-ssr generates .ssr.conf.updated with REPORTER_IDs
#   - Uses password from .ssr.conf field 5 with ssh-copy-id
#   - Shows "✓ Fixed" status when keys are successfully installed
#   - Lists failed RACs with manual fix commands if auto-fix fails
#   - Uses StrictHostKeyChecking=no and UserKnownHostsFile=/dev/null
#   - Requires sshpass package (apt install sshpass)
#
# Changes in v2.9.0:
#   - NEW: --config <reporter_id> <psk> mode for manual client setup
#   - Creates user accounts on both gw1 and gw2 servers
#   - Adds PSK to user's .ssh/authorized_keys on both servers
#   - Outputs WD_SERVER_USER_LIST line for client's wsprdaemon.conf
#   - Does not require RAC tunnel or SSH access to client
#
# Changes in v2.8.0:
#   - NEW: --scan-racs option to check connectivity to all RACs
#   - Uses nc (netcat) to test TCP ports for each RAC
#   - Shows active vs inactive clients with SSH test
#   - Reports summary of all RACs in .ssr.conf
#
# Changes in v2.7.6:
#   - IMPROVED: Add warning comment to wsprdaemon.conf configuration line
#   - Line now includes: "### WARNING: DO NOT REMOVE OR CHANGE THIS LINE..."
#   - Makes it clear the line is managed by the WD SERVER
#
# Changes in v2.7.5:
#   - CRITICAL: Check if client has SSH keys, create them if missing
#   - CRITICAL: Abort on SSH key issues (don't report false success)
#   - NEW: ensure_client_ssh_keys() function creates keys if missing
#   - FIXED: Script now stops if keys can't be created or installed
#
# Changes in v2.7.4:
#   - FIXED: Properly detect if user already exists (use getent passwd, not id)
#   - FIXED: Handle case where group exists but user doesn't (use -g flag)
#   - IMPROVED: Better debugging output when user creation fails
#   - No longer fails if user already exists on remote server
#
# Changes in v2.7.3:
#   - CRITICAL: Don't force UID/GID matching between servers (causes conflicts)
#   - CRITICAL: Abort on user creation failure (don't report false success)
#   - Only usernames need to match between servers, not UIDs
#   - Better error reporting when user creation fails
#
# Changes in v2.7.2:
#   - FIXED: Directory ownership on remote servers (uploads/.ssh must be owned by user)
#   - FIXED: Set ownership BEFORE setting restrictive permissions
#   - CHANGED: Remove ALL WD_SERVER_USER lines, only use WD_SERVER_USER_LIST
#   - IMPROVED: Better ownership checking during replication
#
# Changes in v2.7.1:
#   - FIXED: Create group before user on remote servers
#   - FIXED: Handle client connection failures gracefully
#   - Shows manual configuration instructions if client unreachable
#   - Continues with partial success when client config fails
#
# Changes in v2.7.0:
#   - FIXED: Account lock detection now correctly distinguishes locked accounts
#   - Only accounts with ! or !! passwords are truly locked
#   - Accounts with * password are NOT locked (SSH keys work)
#   - Prevents unnecessary account modifications
#
# Changes in v2.6.9:
#   - FIXED: SFTP test now runs FROM CLIENT perspective via SSH
#   - Test connects to client first, then tests SFTP from there
#   - Provides better explanation when tests fail
#   - Always continues configuration regardless of test results
#
# Changes in v2.6.8:
#   - FIXED: Hostname detection now case-insensitive (recognizes GW2 correctly)
#   - FIXED: SFTP test improved with better diagnostics and alternate verification
#   - FIXED: Shows all configured servers in final output
#   - Always continues configuration even if SFTP tests fail
#
# Changes in v2.6.7:
#   - FIXED: Check and unlock locked accounts (both local and remote)
#   - FIXED: Only configure groups/directories if not already properly set up
#   - Checks account status with 'passwd -S' and unlocks if needed
#   - Verifies existing directory permissions before modifying
#
# Changes in v2.6.6:
#   - FIXED: SSH username is field 3, not field 4!
#   - Format: "RAC,wd_user,ssh_user,ssh_pass,legacy,description,forwards"
#   - Example: "84,kj6mki-rz,wsprdaemon,AUTO,..." → uses 'wsprdaemon'
#
# Changes in v2.6.5:
#   - FIXED: Correctly parse .ssr.conf FRPS_REMOTE_ACCESS_LIST array format
#   - SSH username is field 4 (first word) from comma-separated entry
#   - Format: "RAC,wd_user,wd_pass,ssh_user ssh_pass,description,forwards"
#
# Changes in v2.6.4:
#   - FIXED: Client username now extracted from .ssr.conf file for the RAC
#   - No longer hardcoded to 'pi' or 'wsprdaemon'
#   - Reads from format: "RAC user@host:port" in ~/.ssr.conf
#
# Changes in v2.6.3:
#   - FIXED: Port calculation now uses correct formula (35800 + RAC)
#   - Was incorrectly using 2200 + RAC
#
# Changes in v2.6.2:
#   - Enhanced debugging for reporter ID extraction failures
#   - Added manual reporter ID override option
#   - Better error messages showing what files were checked
#   - Tests SSH connection before attempting extraction
#
# Changes in v2.6.1:
#   - FIXED: Diagnostic output from get_client_reporter_id no longer interferes with return value
#   - All diagnostic messages now properly sent to stderr
#
# Changes in v2.6.0:
#   - Extract client_reporter_id from upload_to_wsprnet_daemon.log instead of CSV
#   - Falls back to CSV if log method fails
#   - Added --version argument (shows version and exits)
#   - Script always displays version at startup
#
# Changes in v2.5.0:
#   - Only lock passwords for NEW users, not existing ones
#   - Compare and auto-fix authorized_keys before declaring failure
#   - Always configure client for BOTH servers regardless of test results
#
# Author: AI6VN (with assistance from Claude)
# Date: November 2025

VERSION="2.10.7"
SCRIPT_NAME="wd-register-client.sh"

# Source bash aliases if available
if [[ -f ~/wsprdaemon/bash-aliases ]]; then
    source ~/wsprdaemon/bash-aliases
fi

# Function to display version
function show_version() {
    echo "$SCRIPT_NAME version $VERSION"
    echo "WSPRDAEMON Client Registration Tool"
    echo ""
}

# Function to generate wsprdaemon.conf lines (--config mode)
# Usage: generate_config_output <reporter_id> <psk>
# Creates user accounts on gw1 and gw2, adds PSK to authorized_keys, and outputs config lines
function generate_config_output() {
    local reporter_id="$1"
    local psk="$2"
    
    if [[ -z "$reporter_id" || -z "$psk" ]]; then
        echo "ERROR: --config requires both reporter_id and psk arguments"
        echo ""
        echo "Usage: $SCRIPT_NAME --config <reporter_id> <psk>"
        echo ""
        echo "Example: $SCRIPT_NAME --config KJ6MKI 'ssh-rsa AAAAB3Nza... user@host'"
        echo ""
        echo "The PSK is the client's SSH public key (contents of ~/.ssh/id_rsa.pub)"
        echo ""
        echo "This creates user accounts on both gateway servers, adds the PSK to"
        echo "authorized_keys, and outputs the line for the client's wsprdaemon.conf."
        return 1
    fi
    
    # Sanitize the reporter ID for use as Linux username
    local sanitized_reporter_id=$(echo "$reporter_id" | tr '/' '_' | tr '.' '_' | tr '-' '_' | tr '@' '_')
    
    if [[ "$reporter_id" != "$sanitized_reporter_id" ]]; then
        echo ""
        echo "NOTE: Reporter ID '$reporter_id' contains invalid characters for Linux username"
        echo "      Using sanitized username: '$sanitized_reporter_id'"
    fi
    
    echo ""
    echo "=== Creating user accounts for $reporter_id ==="
    echo ""
    
    # Ensure SSHD is configured for SFTP-only access
    if ! wd-sshd-conf-add-sftponly; then
        echo "ERROR: Failed to configure SSHD"
        return 1
    fi
    
    # Ensure sftponly group exists
    if ! ensure_sftponly_group; then
        echo "ERROR: Failed to ensure sftponly group"
        return 1
    fi
    
    # Setup local user
    if ! setup_local_user "$reporter_id" "$sanitized_reporter_id"; then
        echo "ERROR: Failed to setup local user"
        return 1
    fi
    
    # Add the PSK to the user's authorized_keys file
    echo ""
    echo "=== Adding PSK to authorized_keys ==="
    local auth_keys_file="/home/${sanitized_reporter_id}/.ssh/authorized_keys"
    
    # Check if PSK already exists in authorized_keys
    if sudo grep -qF "$psk" "$auth_keys_file" 2>/dev/null; then
        echo "✓ PSK already exists in $auth_keys_file"
    else
        echo "Adding PSK to $auth_keys_file..."
        if ! echo "$psk" | sudo tee -a "$auth_keys_file" > /dev/null; then
            echo "ERROR: Failed to add PSK to authorized_keys"
            return 1
        fi
        # Ensure correct ownership and permissions
        sudo chown "${sanitized_reporter_id}:${sanitized_reporter_id}" "$auth_keys_file"
        sudo chmod 600 "$auth_keys_file"
        echo "✓ PSK added to local authorized_keys"
    fi
    
    # Determine backup server
    local hostname=$(hostname | tr '[:upper:]' '[:lower:]')
    local backup_server=""
    
    case "$hostname" in
        *gw1*)
            backup_server="gw2.wsprdaemon.org"
            ;;
        *gw2*)
            backup_server="gw1.wsprdaemon.org"
            ;;
        *)
            backup_server="gw2.wsprdaemon.org"
            ;;
    esac
    
    # Replicate to backup server
    echo ""
    if ! replicate_user_to_server "$reporter_id" "$sanitized_reporter_id" "$backup_server"; then
        echo ""
        echo "WARNING: Failed to replicate user to $backup_server"
        echo "You may need to manually create the user on $backup_server"
    else
        # Add the PSK to authorized_keys on the backup server too
        echo "Adding PSK to authorized_keys on $backup_server..."
        local remote_auth_keys="/home/${sanitized_reporter_id}/.ssh/authorized_keys"
        
        # Check if PSK already exists on remote
        if ssh "$backup_server" "sudo grep -qF '$psk' '$remote_auth_keys'" 2>/dev/null; then
            echo "✓ PSK already exists on $backup_server"
        else
            if ssh "$backup_server" "echo '$psk' | sudo tee -a '$remote_auth_keys' > /dev/null && sudo chown '${sanitized_reporter_id}:${sanitized_reporter_id}' '$remote_auth_keys' && sudo chmod 600 '$remote_auth_keys'" 2>/dev/null; then
                echo "✓ PSK added to authorized_keys on $backup_server"
            else
                echo "WARNING: Could not add PSK on $backup_server"
                echo "You may need to manually add the PSK to $remote_auth_keys"
            fi
        fi
    fi
    
    echo ""
    echo "========================================="
    echo "✓ User accounts created on both servers!"
    echo "========================================="
    echo ""
    echo "# ============================================================"
    echo "# WSPRDAEMON Server Upload Configuration"
    echo "# Generated by $SCRIPT_NAME v$VERSION"
    echo "# Date: $(date '+%Y-%m-%d %H:%M:%S')"
    echo "# Reporter ID: $reporter_id"
    if [[ "$reporter_id" != "$sanitized_reporter_id" ]]; then
        echo "# Sanitized Username: $sanitized_reporter_id"
    fi
    echo "# ============================================================"
    echo ""
    echo "# Add this line to the client's ~/wsprdaemon/wsprdaemon.conf file:"
    echo "# (Remove any existing WD_SERVER_USER or WD_SERVER_USER_LIST lines first)"
    echo ""
    echo "### WARNING: DO NOT REMOVE OR CHANGE THIS LINE which was added by the WD SERVER!"
    echo "WD_SERVER_USER_LIST=(\"${sanitized_reporter_id}@gw1.wsprdaemon.org\" \"${sanitized_reporter_id}@gw2.wsprdaemon.org\")"
    echo ""
    
    return 0
}

# Function to add SFTP-only configuration to sshd_config
function wd-sshd-conf-add-sftponly() {
    echo "=== Configuring SSHD for SFTP-only access ==="
    
    # Check if sftponly group configuration already exists
    if grep -q "^Match Group sftponly" /etc/ssh/sshd_config; then
        echo "✓ SFTP-only configuration already exists in sshd_config"
        return 0
    fi
    
    echo "Adding SFTP-only configuration to sshd_config..."
    
    # Backup the original config
    if ! sudo cp /etc/ssh/sshd_config /etc/ssh/sshd_config.backup.$(date +%Y%m%d_%H%M%S); then
        echo "ERROR: Failed to backup sshd_config"
        return 1
    fi
    
    # Add the SFTP configuration
    if ! sudo tee -a /etc/ssh/sshd_config << 'EOF' >/dev/null

# SFTP-only access for sftponly group
Match Group sftponly
    ChrootDirectory /home/%u
    ForceCommand internal-sftp
    AllowTcpForwarding no
    X11Forwarding no
    PasswordAuthentication no
EOF
    then
        echo "ERROR: Failed to add SFTP configuration to sshd_config"
        return 1
    fi
    
    # Test the configuration
    if ! sudo sshd -t 2>/dev/null; then
        echo "ERROR: Invalid sshd_config syntax. Restoring backup."
        local latest_backup=$(ls -t /etc/ssh/sshd_config.backup.* | head -1)
        sudo cp "$latest_backup" /etc/ssh/sshd_config
        return 1
    fi
    
    # Restart SSH service
    if ! sudo systemctl restart sshd; then
        echo "ERROR: Failed to restart SSH service"
        return 1
    fi
    
    echo "✓ SFTP-only configuration added successfully"
    return 0
}

# Function to create SFTP-only group
function ensure_sftponly_group() {
    if getent group sftponly >/dev/null 2>&1; then
        echo "✓ Group 'sftponly' already exists"
    else
        echo "Creating 'sftponly' group..."
        if sudo groupadd sftponly; then
            echo "✓ Group 'sftponly' created"
        else
            echo "ERROR: Failed to create 'sftponly' group"
            return 1
        fi
    fi
    return 0
}

# Function to setup user account on local server
function setup_local_user() {
    local username="$1"
    local sanitized_username="$2"
    
    echo ""
    echo "=== Setting up local user account for $username ==="
    
    # Check if user already exists
    if id "$sanitized_username" >/dev/null 2>&1; then
        echo "✓ User '$sanitized_username' already exists"
        
        # Check if account is TRULY locked (! or !! in shadow file, not * which is OK for SSH)
        local shadow_entry=$(sudo getent shadow "$sanitized_username" | cut -d: -f2)
        if [[ "$shadow_entry" =~ ^!+[^*] ]] || [[ "$shadow_entry" == "!" ]] || [[ "$shadow_entry" == "!!" ]]; then
            echo "  Account is LOCKED (password field starts with !) - unlocking..."
            sudo usermod -p '*' "$sanitized_username"
            echo "  ✓ Account unlocked (key-only authentication enabled)"
        else
            # Check the passwd -S status for informational purposes
            local passwd_status=$(sudo passwd -S "$sanitized_username" 2>/dev/null | awk '{print $2}')
            if [[ "$passwd_status" == "L" ]] || [[ "$passwd_status" == "LK" ]]; then
                echo "  ✓ Account shows as 'L' but has '*' password (SSH keys work) - no action needed"
            else
                echo "  ✓ Account status is '$passwd_status' - SSH keys enabled"
            fi
        fi
        local user_exists=1
    else
        echo "Creating user '$sanitized_username'..."
        if ! sudo useradd -m -d "/home/$sanitized_username" -s /usr/sbin/nologin "$sanitized_username"; then
            echo "ERROR: Failed to create user '$sanitized_username'"
            return 1
        fi
        echo "✓ User created"
        local user_exists=0
    fi
    
    # Only set disabled password for NEW users
    if [[ $user_exists -eq 0 ]]; then
        echo "Setting disabled password for new user..."
        if ! sudo usermod -p '*' "$sanitized_username"; then
            echo "ERROR: Failed to set disabled password"
            return 1
        fi
        echo "✓ Password disabled (key-only authentication)"
    fi
    
    # Check if already in sftponly group
    if groups "$sanitized_username" 2>/dev/null | grep -q sftponly; then
        echo "✓ User already in 'sftponly' group"
    else
        echo "Adding user to 'sftponly' group..."
        if ! sudo usermod -a -G sftponly "$sanitized_username"; then
            echo "ERROR: Failed to add user to sftponly group"
            return 1
        fi
        echo "✓ Added to sftponly group"
    fi
    
    # Setup SSH directory
    local ssh_dir="/home/$sanitized_username/.ssh"
    echo "Setting up SSH directory..."
    if ! sudo mkdir -p "$ssh_dir"; then
        echo "ERROR: Failed to create SSH directory"
        return 1
    fi
    
    # Create authorized_keys file
    sudo touch "$ssh_dir/authorized_keys"
    
    # Set ownership FIRST (before restrictive permissions)
    sudo chown "$sanitized_username:$sanitized_username" "$ssh_dir"
    sudo chown "$sanitized_username:$sanitized_username" "$ssh_dir/authorized_keys"
    
    # Then set permissions
    sudo chmod 700 "$ssh_dir"
    sudo chmod 600 "$ssh_dir/authorized_keys"
    echo "✓ SSH directory configured"
    
    # Setup upload directory
    local upload_dir="/home/$sanitized_username/uploads"
    echo "Setting up upload directory..."
    if ! sudo mkdir -p "$upload_dir"; then
        echo "ERROR: Failed to create upload directory"
        return 1
    fi
    
    # Set ownership and permissions
    sudo chown "$sanitized_username:$sanitized_username" "$upload_dir"
    sudo chmod 755 "$upload_dir"
    echo "✓ Upload directory configured"
    
    # Fix chroot directory permissions
    echo "Setting chroot directory permissions..."
    sudo chown root:root "/home/$sanitized_username"
    sudo chmod 755 "/home/$sanitized_username"
    echo "✓ Chroot directory permissions set"
    
    return 0
}

# Function to replicate user to backup server
function replicate_user_to_server() {
    local username="$1"
    local sanitized_username="$2"
    local server="$3"
    
    echo ""
    echo "=== Replicating user to $server ==="
    
    # Get local user's info (for reference only)
    local uid=$(id -u "$sanitized_username" 2>/dev/null)
    local gid=$(id -g "$sanitized_username" 2>/dev/null)
    local groupname=$(id -gn "$sanitized_username" 2>/dev/null)
    
    if [[ -z "$uid" || -z "$gid" ]]; then
        echo "ERROR: Could not get UID/GID for user '$sanitized_username'"
        return 1
    fi
    
    echo "Local user $sanitized_username: UID=$uid, GID=$gid, Group=$groupname"
    echo "Note: Remote server will use its own available UID/GID"
    
    # Check if user exists on remote server (more thorough check)
    echo "Checking if user exists on $server..."
    if ssh "$server" "getent passwd '$sanitized_username' >/dev/null 2>&1"; then
        echo "✓ User '$sanitized_username' already exists on $server"
        
        # Check if account is TRULY locked on remote (! or !! in shadow, not *)
        echo "  Checking account status on $server..."
        local remote_shadow=$(ssh "$server" "sudo getent shadow '$sanitized_username' 2>/dev/null | cut -d: -f2")
        if [[ "$remote_shadow" =~ ^!+[^*] ]] || [[ "$remote_shadow" == "!" ]] || [[ "$remote_shadow" == "!!" ]]; then
            echo "  Account is LOCKED on $server (password starts with !) - unlocking..."
            ssh "$server" "sudo usermod -p '*' '$sanitized_username'"
            echo "  ✓ Account unlocked on $server"
        else
            local remote_passwd_status=$(ssh "$server" "sudo passwd -S '$sanitized_username' 2>/dev/null | awk '{print \$2}'")
            if [[ "$remote_passwd_status" == "L" ]] || [[ "$remote_passwd_status" == "LK" ]]; then
                echo "  ✓ Account on $server shows 'L' but has '*' password - SSH keys work, no action needed"
            else
                echo "  ✓ Account on $server status is '$remote_passwd_status' - SSH keys enabled"
            fi
        fi
        local remote_user_exists=1
    else
        echo "User does not exist on $server, creating..."
        
        # Check if a group with the same name already exists
        local group_exists=$(ssh "$server" "getent group '$sanitized_username' >/dev/null 2>&1 && echo 'yes' || echo 'no'")
        
        if [[ "$group_exists" == "yes" ]]; then
            echo "  Group '$sanitized_username' already exists on $server"
            echo "  Creating user '$sanitized_username' using existing group..."
            if ! ssh "$server" "sudo useradd -m -g '$sanitized_username' -d '/home/$sanitized_username' -s /usr/sbin/nologin '$sanitized_username'"; then
                echo "ERROR: Failed to create user on $server even with existing group"
                echo "Debugging: Checking what exists on $server..."
                ssh "$server" "
                    echo '  User check:' && getent passwd '$sanitized_username' || echo '    User does not exist'
                    echo '  Group check:' && getent group '$sanitized_username' || echo '    Group does not exist'
                    echo '  Home dir:' && ls -ld '/home/$sanitized_username' 2>/dev/null || echo '    No home directory'
                "
                return 1
            fi
        else
            echo "  Creating new user '$sanitized_username' (server will assign UID/GID)..."
            if ! ssh "$server" "sudo useradd -m -d '/home/$sanitized_username' -s /usr/sbin/nologin '$sanitized_username'"; then
                echo "ERROR: Failed to create user on $server"
                echo "Debugging: Checking what exists on $server..."
                ssh "$server" "
                    echo '  User check:' && getent passwd '$sanitized_username' || echo '    User does not exist'
                    echo '  Group check:' && getent group '$sanitized_username' || echo '    Group does not exist'
                "
                return 1
            fi
        fi
        echo "  ✓ User created successfully on $server"
        local remote_user_exists=0
    fi
    
    # Only set disabled password for NEW users on remote
    if [[ $remote_user_exists -eq 0 ]]; then
        echo "Setting disabled password for new remote user..."
        ssh "$server" "sudo usermod -p '*' '$sanitized_username'"
        echo "✓ Remote password disabled"
    fi
    
    # Check if already in sftponly group
    echo "Checking group membership on $server..."
    if ssh "$server" "groups '$sanitized_username' 2>/dev/null | grep -q sftponly"; then
        echo "  ✓ User already in 'sftponly' group on $server"
    else
        echo "  Adding user to 'sftponly' group on $server..."
        ssh "$server" "sudo groupadd -f sftponly 2>/dev/null || true"
        ssh "$server" "sudo usermod -a -G sftponly '$sanitized_username'"
        echo "  ✓ Added to sftponly group on $server"
    fi
    
    # Check if directories exist and have correct permissions
    echo "Checking directories on $server..."
    local dirs_ok=$(ssh "$server" "
        if [[ -d /home/$sanitized_username/.ssh && -d /home/$sanitized_username/uploads ]]; then
            # Check ownership and permissions
            ssh_owner=\$(stat -c '%U:%G' /home/$sanitized_username/.ssh 2>/dev/null)
            upload_owner=\$(stat -c '%U:%G' /home/$sanitized_username/uploads 2>/dev/null)
            ssh_perm=\$(stat -c '%a' /home/$sanitized_username/.ssh 2>/dev/null)
            upload_perm=\$(stat -c '%a' /home/$sanitized_username/uploads 2>/dev/null)
            home_owner=\$(stat -c '%U' /home/$sanitized_username 2>/dev/null)
            
            if [[ \"\$ssh_perm\" == \"700\" && \"\$upload_perm\" == \"755\" && \"\$home_owner\" == \"root\" && \"\$ssh_owner\" == \"$sanitized_username:$sanitized_username\" && \"\$upload_owner\" == \"$sanitized_username:$sanitized_username\" ]]; then
                echo 'ok'
            else
                echo 'fix_perms'
            fi
        else
            echo 'create'
        fi
    ")
    
    if [[ "$dirs_ok" == "ok" ]]; then
        echo "  ✓ Directories already configured correctly on $server"
    else
        if [[ "$dirs_ok" == "fix_perms" ]]; then
            echo "  Fixing directory ownership and permissions on $server..."
        else
            echo "  Creating directories on $server..."
        fi
        ssh "$server" "
            sudo mkdir -p /home/$sanitized_username/{.ssh,uploads}
            sudo touch /home/$sanitized_username/.ssh/authorized_keys
            
            # Set ownership FIRST (before setting restrictive permissions)
            sudo chown $sanitized_username:$sanitized_username /home/$sanitized_username/.ssh
            sudo chown $sanitized_username:$sanitized_username /home/$sanitized_username/.ssh/authorized_keys
            sudo chown $sanitized_username:$sanitized_username /home/$sanitized_username/uploads
            sudo chown root:root /home/$sanitized_username
            
            # Then set permissions
            sudo chmod 755 /home/$sanitized_username
            sudo chmod 700 /home/$sanitized_username/.ssh
            sudo chmod 600 /home/$sanitized_username/.ssh/authorized_keys
            sudo chmod 755 /home/$sanitized_username/uploads
        "
        echo "  ✓ Directories configured with correct ownership on $server"
    fi
    
    return 0
}

# Function to generate updated .ssr.conf with REPORTER_IDs
# Replaces first word of field 6 (description) with the sanitized REPORTER_ID
function generate_updated_ssr_conf() {
    echo ""
    echo "========================================="
    echo "Generating updated .ssr.conf with REPORTER_IDs"
    echo "========================================="
    echo ""
    
    # Check for .ssr.conf file
    local ssr_conf_file="${HOME}/.ssr.conf"
    if [[ ! -f "$ssr_conf_file" ]]; then
        echo "ERROR: .ssr.conf file not found at $ssr_conf_file"
        return 1
    fi
    
    # Source the .ssr.conf file
    source "$ssr_conf_file"
    
    if [[ ${#FRPS_REMOTE_ACCESS_LIST[@]} -eq 0 ]]; then
        echo "ERROR: No RAC entries found in .ssr.conf"
        return 1
    fi
    
    # Always use gw2 for RAC connections (RAC ports only on gw2)
    local WD_RAC_SERVER="gw2"
    
    echo "Reading from: $ssr_conf_file"
    echo "Total RAC entries: ${#FRPS_REMOTE_ACCESS_LIST[@]}"
    echo "Using $WD_RAC_SERVER for SSH connections"
    echo ""
    
    # Output file
    local output_file="${HOME}/.ssr.conf.updated"
    
    # Start building the new array
    echo "# Updated .ssr.conf with REPORTER_IDs" > "$output_file"
    echo "# Generated by $SCRIPT_NAME v$VERSION on $(date '+%Y-%m-%d %H:%M:%S')" >> "$output_file"
    echo "#" >> "$output_file"
    echo "# Format: RAC,wd_user,ssh_user,AUTO,ssh_user password,REPORTER_ID description,forwards" >> "$output_file"
    echo "" >> "$output_file"
    echo "declare FRPS_REMOTE_ACCESS_LIST=(" >> "$output_file"
    
    local updated_count=0
    local skipped_count=0
    local failed_count=0
    
    for entry in "${FRPS_REMOTE_ACCESS_LIST[@]}"; do
        # Parse the entry
        local rac=$(echo "$entry" | cut -d',' -f1)
        local wd_user=$(echo "$entry" | cut -d',' -f2)
        local ssh_user=$(echo "$entry" | cut -d',' -f3)
        local field4=$(echo "$entry" | cut -d',' -f4)
        local field5=$(echo "$entry" | cut -d',' -f5)
        local description=$(echo "$entry" | cut -d',' -f6)
        local forwards=$(echo "$entry" | cut -d',' -f7-)
        
        # Skip comment lines or empty RACs
        if [[ -z "$rac" ]] || [[ "$rac" =~ ^# ]] || [[ ! "$rac" =~ ^[0-9]+$ ]]; then
            echo "    \"$entry\"" >> "$output_file"
            continue
        fi
        
        local port=$((35800 + rac))
        local reporter_id=""
        local new_entry=""
        
        # Try to get REPORTER_ID from client via SSH
        if nc -z -w 2 "$WD_RAC_SERVER" "$port" 2>/dev/null; then
            # Port is open, try SSH
            if timeout 5 ssh -o ConnectTimeout=3 -o BatchMode=yes -o StrictHostKeyChecking=no -o UserKnownHostsFile=/dev/null -p "$port" "${ssh_user}@${WD_RAC_SERVER}" "exit" 2>/dev/null; then
                # SSH works, try to get reporter ID
                reporter_id=$(get_client_reporter_id "$rac" "$ssh_user" "$port" "$WD_RAC_SERVER" 2>/dev/null)
                
                if [[ -n "$reporter_id" ]]; then
                    # Sanitize the reporter ID
                    local sanitized_id=$(echo "$reporter_id" | tr '/' '_' | tr '.' '_' | tr '-' '_' | tr '@' '_')
                    
                    # Extract the rest of the description (after first word)
                    local desc_rest=$(echo "$description" | cut -d' ' -f2-)
                    
                    # Build new description with sanitized reporter ID
                    local new_description="${sanitized_id} ${desc_rest}"
                    
                    # Rebuild the entry
                    new_entry="${rac},${wd_user},${ssh_user},${field4},${field5},${new_description}"
                    if [[ -n "$forwards" ]]; then
                        new_entry="${new_entry},${forwards}"
                    fi
                    
                    printf "  RAC %3s: %s -> %s\n" "$rac" "${description%% *}" "$sanitized_id"
                    ((updated_count++))
                else
                    echo "  RAC $rac: Could not extract REPORTER_ID (keeping original)"
                    new_entry="$entry"
                    ((failed_count++))
                fi
            else
                echo "  RAC $rac: SSH failed (keeping original)"
                new_entry="$entry"
                ((skipped_count++))
            fi
        else
            echo "  RAC $rac: Port closed (keeping original)"
            new_entry="$entry"
            ((skipped_count++))
        fi
        
        echo "    \"$new_entry\"" >> "$output_file"
    done
    
    echo ")" >> "$output_file"
    
    echo ""
    echo "========================================="
    echo "SUMMARY"
    echo "========================================="
    printf "Updated with REPORTER_ID:  %3d\n" "$updated_count"
    printf "Skipped (offline/no SSH):  %3d\n" "$skipped_count"
    printf "Failed to get ID:          %3d\n" "$failed_count"
    echo ""
    echo "Output written to: $output_file"
    echo ""
    echo "To use the updated file:"
    echo "  cp $output_file $ssr_conf_file"
    echo ""
    
    return 0
}

# Function to scan all RACs for connectivity
function scan_all_racs() {
    echo ""
    echo "========================================="
    echo "WSPRDAEMON RAC Connectivity Scanner"
    echo "========================================="
    echo ""
    
    # Check for .ssr.conf file
    local ssr_conf_file="${HOME}/.ssr.conf"
    if [[ ! -f "$ssr_conf_file" ]]; then
        echo "ERROR: .ssr.conf file not found at $ssr_conf_file"
        exit 1
    fi
    
    # Check if nc is available
    if ! command -v nc >/dev/null 2>&1; then
        echo "ERROR: 'nc' (netcat) is not installed."
        echo "Install with: sudo apt-get install netcat-openbsd"
        exit 1
    fi
    
    # Check if sshpass is available (for auto-fix)
    local sshpass_available=0
    if command -v sshpass >/dev/null 2>&1; then
        sshpass_available=1
    else
        echo "NOTE: 'sshpass' not installed - cannot auto-fix SSH key issues"
        echo "      Install with: sudo apt-get install sshpass"
        echo ""
    fi
    
    # Source the .ssr.conf file
    source "$ssr_conf_file"
    
    if [[ ${#FRPS_REMOTE_ACCESS_LIST[@]} -eq 0 ]]; then
        echo "ERROR: No RAC entries found in .ssr.conf"
        exit 1
    fi
    
    echo "Configuration: $ssr_conf_file"
    echo "Total RAC entries: ${#FRPS_REMOTE_ACCESS_LIST[@]}"
    echo ""
    
    # Determine which server we're on and set both gateways
    local hostname=$(hostname | tr '[:upper:]' '[:lower:]')
    local localhost_server=""
    local remote_server=""
    
    if [[ "$hostname" == *"gw1"* ]]; then
        localhost_server="localhost"
        remote_server="gw2"
    elif [[ "$hostname" == *"gw2"* ]]; then
        localhost_server="localhost"
        remote_server="gw1"
    else
        # Unknown host, use gw2 as primary
        localhost_server="gw2"
        remote_server="gw1"
    fi
    
    echo "Testing from: $(hostname)"
    echo "Will check: $localhost_server first, then $remote_server if needed"
    echo "Port formula: 35800 + RAC number"
    echo ""
    
    # Track statistics
    local total_racs=0
    local active_racs=0
    local inactive_racs=0
    local ssh_ok=0
    local ssh_fail=0
    local ssh_fixed=0
    local wd_cfg_active=0
    local wd_cfg_needed=0
    local active_list=()
    local inactive_list=()
    local ssh_fail_list=()
    local need_reg_list=()
    local wd_versions=()
    local highest_version=""
    local version_mismatch_list=()
    
    # Header
    printf "%-4s | %-6s | %-10s | %-8s | %-6s | %-12s | %-15s | %s\n" "RAC" "Port" "TCP State" "SSH Test" "WD Cfg" "WD Version" "Reporter ID" "Description"
    printf "%-4s-+-%-6s-+-%-10s-+-%-8s-+-%-6s-+-%-12s-+-%-15s-+-%s\n" "----" "------" "----------" "--------" "------" "------------" "---------------" "--------------------"
    
    # Test each RAC
    for entry in "${FRPS_REMOTE_ACCESS_LIST[@]}"; do
        # Parse the entry
        # Format: "RAC,wd_user,ssh_user,AUTO,ssh_user password,description,forwards"
        # Example: "13,k8nvh3,wsprdaemon,AUTO,wsprdaemon LRS-75-5?,K8NVH3 RX888...,..."
        local rac=$(echo "$entry" | cut -d',' -f1)
        local wd_user=$(echo "$entry" | cut -d',' -f2)
        local ssh_user=$(echo "$entry" | cut -d',' -f3)
        local field5=$(echo "$entry" | cut -d',' -f5)
        local ssh_pass=$(echo "$field5" | awk '{print $2}')  # Second space-separated word
        local description=$(echo "$entry" | cut -d',' -f6)
        
        # Skip comment lines or empty RACs
        if [[ -z "$rac" ]] || [[ "$rac" =~ ^# ]] || [[ ! "$rac" =~ ^[0-9]+$ ]]; then
            continue
        fi
        
        # Calculate port
        local port=$((35800 + rac))
        
        # Test TCP connectivity - try localhost first, then remote
        local tcp_status="✗ CLOSED  "   # 10 display chars (padded)
        local tcp_symbol=""            # Symbol included in status
        local ssh_status="   -    "    # 8 display chars, centered dash
        local wd_cfg="  -   "          # 6 display chars
        local wd_version="     -      " # 12 display chars
        local connected_server=""      # Which server we connected to
        local reporter_id="$wd_user"   # Default to wd_user, will try to get actual ID
        
        # Try localhost first
        if nc -z -w 2 "$localhost_server" "$port" 2>/dev/null; then
            tcp_status="✓ OPEN    "     # 10 display chars (padded)
            connected_server="$localhost_server"
            ((active_racs++))
            active_list+=("$rac:$wd_user:$port")
        # If localhost fails, try remote server
        elif nc -z -w 2 "$remote_server" "$port" 2>/dev/null; then
            tcp_status="✓ OPEN    "     # 10 display chars (padded)
            connected_server="$remote_server"
            ((active_racs++))
            active_list+=("$rac:$wd_user:$port")
        fi
        
        # If we have a connection, test SSH and get other info
        if [[ -n "$connected_server" ]]; then
            
            # Test SSH access for active connections
            if timeout 5 ssh -o ConnectTimeout=3 -o BatchMode=yes -o StrictHostKeyChecking=no -o UserKnownHostsFile=/dev/null -p "$port" "${ssh_user}@${connected_server}" "exit" 2>/dev/null; then
                ssh_status="✓ OK    "   # 8 display chars
                ((ssh_ok++))
                
                # Try to get actual reporter ID from client's upload log (same source as Linux user)
                local reporter_id=""
                reporter_id=$(timeout 5 ssh -o ConnectTimeout=3 -o BatchMode=yes -o StrictHostKeyChecking=no -o UserKnownHostsFile=/dev/null -p "$port" "${ssh_user}@${connected_server}" "
                    # Get reporter ID from the wsprnet upload daemon log (look for 'my call' pattern)
                    if [[ -f ~/wsprdaemon/uploads/wsprnet/spots/upload_to_wsprnet_daemon.log ]]; then
                        id=\$(grep 'my call' ~/wsprdaemon/uploads/wsprnet/spots/upload_to_wsprnet_daemon.log 2>/dev/null | tail -1 | sed -n 's/.*my call \\([^ ]*\\) and\\/or.*/\\1/p')
                        if [[ -n \"\$id\" ]]; then
                            echo \"\$id\"
                            exit 0
                        fi
                    fi
                    
                    # Fallback: Get from RECEIVER_LIST in wsprdaemon.conf (for new RACs without uploads yet)
                    if [[ -f ~/wsprdaemon/wsprdaemon.conf ]]; then
                        # Source the config to get RECEIVER_LIST array
                        source ~/wsprdaemon/wsprdaemon.conf 2>/dev/null
                        if [[ \${#RECEIVER_LIST[@]} -gt 0 ]]; then
                            # Get third space-separated field from first element
                            echo \"\${RECEIVER_LIST[0]}\" | awk '{print \$3}'
                        fi
                    fi
                " 2>/dev/null | tr -d ' \r\n' | head -c 15)
                if [[ -z "$reporter_id" ]]; then
                    reporter_id="$wd_user"  # Fall back to wd_user if we can't get it
                fi
                
                # Check for WD_SERVER_USER_LIST in wsprdaemon.conf
                if timeout 5 ssh -o ConnectTimeout=3 -o BatchMode=yes -o StrictHostKeyChecking=no -o UserKnownHostsFile=/dev/null -p "$port" "${ssh_user}@${connected_server}" "grep -q '^WD_SERVER_USER_LIST=' ~/wsprdaemon/wsprdaemon.conf 2>/dev/null" 2>/dev/null; then
                    wd_cfg="✓ OK  "
                    ((wd_cfg_active++))
                else
                    wd_cfg="✗ NO  "
                    ((wd_cfg_needed++))
                    need_reg_list+=("$rac:$wd_user:$port:$ssh_user")
                fi
                
                # Get WD version
                local ver=$(timeout 5 ssh -o ConnectTimeout=3 -o BatchMode=yes -o StrictHostKeyChecking=no -o UserKnownHostsFile=/dev/null -p "$port" "${ssh_user}@${connected_server}" "cd ~/wsprdaemon 2>/dev/null && echo \"\$(< wd_version.txt)-\$(git rev-list --count HEAD 2>/dev/null)\" 2>/dev/null" 2>/dev/null)
                if [[ -n "$ver" && "$ver" != "-" ]]; then
                    # Pad/truncate to 12 chars
                    wd_version=$(printf "%-12s" "${ver:0:12}")
                    wd_versions+=("$ver")
                    # Store RAC info with version for later comparison
                    version_mismatch_list+=("$rac:$wd_user:$port:$ssh_user:$ver")
                fi
            else
                # SSH failed - try to fix with ssh-copy-id if we have the password
                local fixed=0
                if [[ $sshpass_available -eq 1 && -n "$ssh_pass" && "$ssh_pass" != "AUTO" && "$ssh_pass" != "*" && "$ssh_pass" != "?" ]]; then
                    # Try to install our public key using the password (suppress output)
                    if sshpass -p "$ssh_pass" ssh-copy-id -o StrictHostKeyChecking=no -o UserKnownHostsFile=/dev/null -p "$port" "${ssh_user}@${connected_server}" &>/dev/null; then
                        # Verify it worked
                        if timeout 5 ssh -o ConnectTimeout=3 -o BatchMode=yes -o StrictHostKeyChecking=no -o UserKnownHostsFile=/dev/null -p "$port" "${ssh_user}@${connected_server}" "exit" 2>/dev/null; then
                            ssh_status="✓ Fixed "   # 8 display chars
                            ((ssh_fixed++))
                            ((ssh_ok++))
                            fixed=1
                            
                            # Try to get actual reporter ID from client's upload log (same source as Linux user)
                            reporter_id=$(timeout 5 ssh -o ConnectTimeout=3 -o BatchMode=yes -o StrictHostKeyChecking=no -o UserKnownHostsFile=/dev/null -p "$port" "${ssh_user}@${connected_server}" "
                                # Get reporter ID from the wsprnet upload daemon log (look for 'my call' pattern)
                                if [[ -f ~/wsprdaemon/uploads/wsprnet/spots/upload_to_wsprnet_daemon.log ]]; then
                                    id=\$(grep 'my call' ~/wsprdaemon/uploads/wsprnet/spots/upload_to_wsprnet_daemon.log 2>/dev/null | tail -1 | sed -n 's/.*my call \\([^ ]*\\) and\\/or.*/\\1/p')
                                    if [[ -n \"\$id\" ]]; then
                                        echo \"\$id\"
                                        exit 0
                                    fi
                                fi
                                
                                # Fallback: Get from RECEIVER_LIST in wsprdaemon.conf (for new RACs without uploads yet)
                                if [[ -f ~/wsprdaemon/wsprdaemon.conf ]]; then
                                    # Source the config to get RECEIVER_LIST array
                                    source ~/wsprdaemon/wsprdaemon.conf 2>/dev/null
                                    if [[ \${#RECEIVER_LIST[@]} -gt 0 ]]; then
                                        # Get third space-separated field from first element
                                        echo \"\${RECEIVER_LIST[0]}\" | awk '{print \$3}'
                                    fi
                                fi
                            " 2>/dev/null | tr -d ' \r\n' | head -c 15)
                            if [[ -z "$reporter_id" ]]; then
                                reporter_id="$wd_user"  # Fall back to wd_user if we can't get it
                            fi
                            
                            # Now check WD config and version since SSH works
                            if timeout 5 ssh -o ConnectTimeout=3 -o BatchMode=yes -o StrictHostKeyChecking=no -o UserKnownHostsFile=/dev/null -p "$port" "${ssh_user}@${connected_server}" "grep -q '^WD_SERVER_USER_LIST=' ~/wsprdaemon/wsprdaemon.conf 2>/dev/null" 2>/dev/null; then
                                wd_cfg="✓ OK  "
                                ((wd_cfg_active++))
                            else
                                wd_cfg="✗ NO  "
                                ((wd_cfg_needed++))
                                need_reg_list+=("$rac:$wd_user:$port:$ssh_user")
                            fi
                            
                            local ver=$(timeout 5 ssh -o ConnectTimeout=3 -o BatchMode=yes -o StrictHostKeyChecking=no -o UserKnownHostsFile=/dev/null -p "$port" "${ssh_user}@${connected_server}" "cd ~/wsprdaemon 2>/dev/null && echo \"\$(< wd_version.txt)-\$(git rev-list --count HEAD 2>/dev/null)\" 2>/dev/null" 2>/dev/null)
                            if [[ -n "$ver" && "$ver" != "-" ]]; then
                                wd_version=$(printf "%-12s" "${ver:0:12}")
                                wd_versions+=("$ver")
                                # Store RAC info with version for later comparison
                                version_mismatch_list+=("$rac:$wd_user:$port:$ssh_user:$ver")
                            fi
                        fi
                    fi
                fi
                
                if [[ $fixed -eq 0 ]]; then
                    ssh_status="✗ Fail  "   # 8 display chars
                    ((ssh_fail++))
                    ssh_fail_list+=("$rac:$wd_user:$port:$ssh_user:$ssh_pass")
                fi
            fi
        else
            ((inactive_racs++))
            inactive_list+=("$rac:$wd_user:$port")
        fi
        
        # Format output (use %s for pre-padded columns)
        printf "%3s  | %6s | %-10s | %s | %s | %s | %-15s | %s\n" \
            "$rac" \
            "$port" \
            "$tcp_status" \
            "$ssh_status" \
            "$wd_cfg" \
            "$wd_version" \
            "${reporter_id:0:15}" \
            "${description:0:30}"
        
        ((total_racs++))
    done
    
    # Summary statistics
    echo ""
    echo "========================================="
    echo "SUMMARY STATISTICS"
    echo "========================================="
    printf "Total RACs tested:     %3d\n" "$total_racs"
    printf "TCP Ports OPEN:        %3d (%.1f%%)\n" "$active_racs" $(echo "scale=1; $active_racs * 100 / $total_racs" | bc 2>/dev/null || echo "0")
    printf "TCP Ports CLOSED:      %3d (%.1f%%)\n" "$inactive_racs" $(echo "scale=1; $inactive_racs * 100 / $total_racs" | bc 2>/dev/null || echo "0")
    
    if [[ $active_racs -gt 0 ]]; then
        printf "SSH Access OK:         %3d (%.1f%% of active)\n" "$ssh_ok" $(echo "scale=1; $ssh_ok * 100 / $active_racs" | bc 2>/dev/null || echo "0")
        if [[ $ssh_fixed -gt 0 ]]; then
            printf "SSH Keys Auto-Fixed:   %3d\n" "$ssh_fixed"
        fi
        printf "SSH Access Failed:     %3d (%.1f%% of active)\n" "$ssh_fail" $(echo "scale=1; $ssh_fail * 100 / $active_racs" | bc 2>/dev/null || echo "0")
        
        echo ""
        printf "WD Config Active:      %3d (%.1f%% of SSH OK)\n" "$wd_cfg_active" $(echo "scale=1; $wd_cfg_active * 100 / $ssh_ok" | bc 2>/dev/null || echo "0")
        printf "WD Config Needed:      %3d (SSH OK but no config)\n" "$wd_cfg_needed"
    fi
    
    echo ""
    echo "========================================="
    echo "ACTIVE RACS (TCP Port OPEN)"
    echo "========================================="
    if [[ ${#active_list[@]} -gt 0 ]]; then
        for item in "${active_list[@]}"; do
            IFS=':' read -r rac user port <<< "$item"
            printf "  RAC %3s (port %s): %s\n" "$rac" "$port" "$user"
        done
    else
        echo "  None"
    fi
    
    # Show SSH failures with manual fix instructions
    if [[ ${#ssh_fail_list[@]} -gt 0 ]]; then
        echo ""
        echo "========================================="
        echo "SSH FAILURES - Manual Fix Required"
        echo "========================================="
        for item in "${ssh_fail_list[@]}"; do
            IFS=':' read -r rac user port ssh_user ssh_pass <<< "$item"
            echo "  RAC $rac ($user):"
            if [[ -z "$ssh_pass" || "$ssh_pass" == "AUTO" || "$ssh_pass" == "*" || "$ssh_pass" == "?" ]]; then
                echo "    No password available in .ssr.conf"
                echo "    Manual fix: ssh -p $port ${ssh_user}@${WD_RAC_SERVER}"
                echo "                Then add this server's public key to ~/.ssh/authorized_keys"
            else
                echo "    Try: sshpass -p '$ssh_pass' ssh-copy-id -o StrictHostKeyChecking=no -p $port ${ssh_user}@${WD_RAC_SERVER}"
            fi
            echo ""
        done
    fi
    
    echo ""
    echo "========================================="
    echo "INACTIVE RACS (TCP Port CLOSED)"
    echo "========================================="
    if [[ ${#inactive_list[@]} -gt 0 ]]; then
        if [[ ${#inactive_list[@]} -gt 20 ]]; then
            echo "  Showing first 20 of ${#inactive_list[@]} inactive RACs:"
            for i in {0..19}; do
                [[ $i -ge ${#inactive_list[@]} ]] && break
                IFS=':' read -r rac user port <<< "${inactive_list[$i]}"
                printf "  RAC %3s (port %s): %s\n" "$rac" "$port" "$user"
            done
            echo "  ... and $((${#inactive_list[@]} - 20)) more"
        else
            for item in "${inactive_list[@]}"; do
                IFS=':' read -r rac user port <<< "$item"
                printf "  RAC %3s (port %s): %s\n" "$rac" "$port" "$user"
            done
        fi
    else
        echo "  None"
    fi
    
    echo ""
    echo "========================================="
    echo "Test completed at $(date '+%Y-%m-%d %H:%M:%S %Z')"
    echo ""
    
    # Save RACs needing registration
    if [[ ${#need_reg_list[@]} -gt 0 ]]; then
        echo ""
        echo "========================================="
        echo "RACS NEEDING REGISTRATION"
        echo "========================================="
        echo "RACs with SSH OK but WD Config missing (✗):"
        
        # Save to file
        local reg_file="wd-register-clients.need-reg"
        > "$reg_file"  # Clear file
        
        for item in "${need_reg_list[@]}"; do
            IFS=':' read -r rac user port ssh_user <<< "$item"
            printf "  RAC %3s (port %s): %s (user: %s)\n" "$rac" "$port" "$user" "$ssh_user"
            echo "$rac" >> "$reg_file"
        done
        
        echo ""
        echo "Saved ${#need_reg_list[@]} RACs to: $reg_file"
        echo "To register all: ./wd-register-client.sh --register-batch"
    fi
    
    # Find highest version and identify mismatches
    if [[ ${#wd_versions[@]} -gt 0 ]]; then
        # Find highest version (sort by git count number after the dash)
        highest_version=$(printf '%s\n' "${wd_versions[@]}" | sort -t'-' -k2 -n | tail -1)
        
        echo ""
        echo "========================================="
        echo "WD VERSION ANALYSIS"
        echo "========================================="
        echo "Highest version found: $highest_version"
        echo ""
        
        # Find RACs with mismatched versions
        local mismatch_count=0
        local update_file="wd-register-clients.need-update"
        > "$update_file"  # Clear file
        
        echo "RACs with older versions:"
        for item in "${version_mismatch_list[@]}"; do
            IFS=':' read -r rac user port ssh_user ver <<< "$item"
            if [[ "$ver" != "$highest_version" ]]; then
                printf "  RAC %3s (port %s): %s has version %s\n" "$rac" "$port" "$user" "$ver"
                echo "$rac:$port:$ssh_user" >> "$update_file"
                ((mismatch_count++))
            fi
        done
        
        if [[ $mismatch_count -gt 0 ]]; then
            echo ""
            echo "Saved $mismatch_count RACs to: $update_file"
            echo "To update all: ./wd-register-client.sh --update-clients"
        else
            echo "All RACs have the same version: $highest_version"
        fi
    fi
    
    echo ""
    echo "========================================="
    
    exit 0
}

# Function to ensure client has SSH keys (create if missing)
function ensure_client_ssh_keys() {
    local client_rac="$1"
    local client_user="$2"
    local client_ip_port="$3"
    local WD_RAC_SERVER="$4"
    
    echo ""
    echo "=== Ensuring client has SSH keys ==="
    
    # Check if client has SSH keys
    echo "  Checking for existing SSH keys on client..."
    local has_keys=$(ssh -o ConnectTimeout=10 -p "${client_ip_port}" "${client_user}@${WD_RAC_SERVER}" "
        if [[ -f ~/.ssh/id_rsa.pub ]] || [[ -f ~/.ssh/id_ed25519.pub ]]; then
            echo 'yes'
        else
            echo 'no'
        fi
    " 2>/dev/null)
    
    if [[ "$has_keys" == "yes" ]]; then
        echo "  ✓ Client already has SSH keys"
        return 0
    fi
    
    echo "  ✗ Client has no SSH keys - creating them..."
    
    # Create SSH keys on the client
    if ! ssh -o ConnectTimeout=10 -p "${client_ip_port}" "${client_user}@${WD_RAC_SERVER}" "
        # Create .ssh directory if it doesn't exist
        mkdir -p ~/.ssh
        chmod 700 ~/.ssh
        
        # Generate SSH key pair (ed25519 is modern and secure)
        ssh-keygen -t ed25519 -f ~/.ssh/id_ed25519 -N '' -C 'wsprdaemon-client' >/dev/null 2>&1
        
        # Also create RSA key for compatibility
        ssh-keygen -t rsa -b 2048 -f ~/.ssh/id_rsa -N '' -C 'wsprdaemon-client' >/dev/null 2>&1
        
        echo 'Keys created successfully'
    " 2>/dev/null; then
        echo "  ✗ ERROR: Failed to create SSH keys on client"
        echo "    Manual intervention required:"
        echo "    1. SSH to client: ssh -p ${client_ip_port} ${client_user}@${WD_RAC_SERVER}"
        echo "    2. Run: ssh-keygen -t ed25519 -N '' -f ~/.ssh/id_ed25519"
        return 1
    fi
    
    echo "  ✓ SSH keys created on client"
    return 0
}

# Function to diagnose and fix authorized_keys on all servers
function diagnose_and_fix_authorized_keys() {
    local client_rac="$1"
    local client_user="$2" 
    local client_ip_port="$3"
    local sanitized_username="$4"
    local WD_RAC_SERVER="$5"
    shift 5
    local servers=("$@")
    
    echo ""
    echo "=== Diagnosing authorized_keys on all servers ==="
    
    # Get client's public key from RAC
    echo "  1. Getting client's public key from RAC server..."
    local client_key=$(ssh -p "${client_ip_port}" "${client_user}@${WD_RAC_SERVER}" "cat ~/.ssh/id_rsa.pub 2>/dev/null || cat ~/.ssh/id_ecdsa.pub 2>/dev/null || cat ~/.ssh/id_ed25519.pub 2>/dev/null" 2>/dev/null | head -1)
    
    if [[ -z "$client_key" ]]; then
        echo "    ✗ ERROR: Client has no SSH keys!"
        echo ""
        echo "========================================="
        echo "✗ CRITICAL ERROR: No SSH keys on client"
        echo "========================================="
        echo ""
        echo "The client at RAC $client_rac has no SSH keys."
        echo "Without keys, the client cannot upload to the servers."
        echo ""
        echo "This should have been detected and fixed earlier."
        echo "Manual fix required:"
        echo "  1. SSH to client: ssh -p ${client_ip_port} ${client_user}@${WD_RAC_SERVER}"
        echo "  2. Generate keys: ssh-keygen -t ed25519 -N '' -f ~/.ssh/id_ed25519"
        echo "  3. Re-run this script"
        return 1
    fi
    
    # Get fingerprint of client's key
    local client_fingerprint=$(echo "$client_key" | ssh-keygen -lf - 2>/dev/null | awk '{print $2}')
    echo "    Client's key fingerprint: $client_fingerprint"
    
    local fixes_made=0
    local server_num=2
    
    # Check each server
    for server_fqdn in "${servers[@]}"; do
        echo "  $server_num. Checking $server_fqdn..."
        
        # Determine if this is local or remote
        local is_local=0
        if [[ "$server_fqdn" == *"$(hostname)"* ]] || [[ "$(hostname)" == *"${server_fqdn%%.*}"* ]]; then
            is_local=1
        fi
        
        # Get current authorized_keys
        if [[ $is_local -eq 1 ]]; then
            local current_key=$(sudo cat "/home/$sanitized_username/.ssh/authorized_keys" 2>/dev/null | head -1)
        else
            local current_key=$(ssh "$server_fqdn" "sudo cat '/home/$sanitized_username/.ssh/authorized_keys' 2>/dev/null | head -1" 2>/dev/null)
        fi
        
        if [[ -z "$current_key" ]]; then
            echo "    ✗ No authorized_keys found or empty file"
            echo "    Installing client's key..."
            
            if [[ $is_local -eq 1 ]]; then
                echo "$client_key" | sudo tee "/home/$sanitized_username/.ssh/authorized_keys" >/dev/null
                sudo chown "$sanitized_username:$sanitized_username" "/home/$sanitized_username/.ssh/authorized_keys"
                sudo chmod 600 "/home/$sanitized_username/.ssh/authorized_keys"
            else
                echo "$client_key" | ssh "$server_fqdn" "sudo tee '/home/$sanitized_username/.ssh/authorized_keys' >/dev/null && sudo chown '$sanitized_username:$sanitized_username' '/home/$sanitized_username/.ssh/authorized_keys' && sudo chmod 600 '/home/$sanitized_username/.ssh/authorized_keys'"
            fi
            
            echo "    ✓ Installed client's key on $server_fqdn"
            ((fixes_made++))
        else
            # Compare fingerprints
            local server_fingerprint=$(echo "$current_key" | ssh-keygen -lf - 2>/dev/null | awk '{print $2}')
            
            if [[ "$client_fingerprint" != "$server_fingerprint" ]]; then
                echo "    ✗ KEY MISMATCH on $server_fqdn!"
                echo "       Client fingerprint:  $client_fingerprint"
                echo "       Server fingerprint:  $server_fingerprint"
                echo "    Replacing with client's key..."
                
                if [[ $is_local -eq 1 ]]; then
                    echo "$client_key" | sudo tee "/home/$sanitized_username/.ssh/authorized_keys" >/dev/null
                    sudo chown "$sanitized_username:$sanitized_username" "/home/$sanitized_username/.ssh/authorized_keys"
                    sudo chmod 600 "/home/$sanitized_username/.ssh/authorized_keys"
                else
                    echo "$client_key" | ssh "$server_fqdn" "sudo tee '/home/$sanitized_username/.ssh/authorized_keys' >/dev/null && sudo chown '$sanitized_username:$sanitized_username' '/home/$sanitized_username/.ssh/authorized_keys' && sudo chmod 600 '/home/$sanitized_username/.ssh/authorized_keys'"
                fi
                
                echo "    ✓ Replaced with client's key on $server_fqdn"
                ((fixes_made++))
            else
                echo "    ✓ Key matches on $server_fqdn"
            fi
        fi
        
        ((server_num++))
    done
    
    if [[ $fixes_made -gt 0 ]]; then
        echo ""
        echo "  Fixed $fixes_made server(s) with incorrect/missing keys"
    else
        echo ""
        echo "  All servers have correct keys"
    fi
    
    return 0
}

# Function to test SFTP uploads from CLIENT to servers
function test_sftp_uploads() {
    local sanitized_username="$1"
    local client_rac="$2"
    local client_user="$3"
    local client_ip_port="$4"
    local WD_RAC_SERVER="$5"
    shift 5
    local servers=("$@")
    
    echo ""
    echo "=== Testing SFTP uploads from CLIENT to servers ==="
    echo "NOTE: These tests run FROM the client's perspective via SSH"
    
    local success_count=0
    local total_count=${#servers[@]}
    local working_servers=()
    
    for server_fqdn in "${servers[@]}"; do
        echo "  Testing SFTP upload from client to $server_fqdn..."
        
        # Create a unique test file name
        local test_filename=".wd_upload_test_$$_$(date +%s)"
        
        # Test FROM THE CLIENT by SSHing to the client and running SFTP from there
        echo "    Running test from client (RAC $client_rac)..."
        if ssh -p "${client_ip_port}" "${client_user}@${WD_RAC_SERVER}" "
            # Create test file on client
            echo 'Test from client at \$(date)' > /tmp/$test_filename
            
            # Try SFTP upload from client to server
            sftp -o BatchMode=yes -o ConnectTimeout=10 -o StrictHostKeyChecking=no ${sanitized_username}@${server_fqdn} >/dev/null 2>&1 <<EOF
cd uploads
put /tmp/$test_filename
ls $test_filename
quit
EOF
            rc=\$?
            rm -f /tmp/$test_filename
            exit \$rc
        " 2>/dev/null; then
            echo "    ✓ SUCCESS: Client can upload to $server_fqdn"
            working_servers+=("$server_fqdn")
            ((success_count++))
            
            # Clean up test file on server
            local current_hostname=$(hostname | tr '[:upper:]' '[:lower:]')
            if [[ "$server_fqdn" == *"$current_hostname"* ]]; then
                sudo rm -f "/home/$sanitized_username/uploads/$test_filename" 2>/dev/null
            else
                ssh "$server_fqdn" "sudo rm -f '/home/$sanitized_username/uploads/$test_filename'" 2>/dev/null
            fi
        else
            echo "    ✗ FAILED: Client cannot upload to $server_fqdn"
            echo "    Note: This could be normal if client doesn't have the private key yet"
        fi
    done
    
    echo ""
    if [[ $success_count -eq $total_count ]]; then
        echo "  SUCCESS: All SFTP upload tests passed ($success_count/$total_count)"
        return 0
    elif [[ $success_count -gt 0 ]]; then
        echo "  WARNING: Only $success_count of $total_count servers passed SFTP tests"
        echo "           Working servers: ${working_servers[*]}"
        echo "           Client will be configured for ALL servers anyway"
        return 0
    else
        echo "  NOTE: SFTP tests failed - this is normal if client doesn't have SSH keys yet"
        echo "        Client has been configured and can set up keys later"
        return 0  # Always return success to continue
    fi
}

# Function to configure client for dual-server access
function configure_client_access() {
    local client_rac="$1"
    local client_user="$2"
    local client_ip_port="$3"
    local sanitized_username="$4"
    local WD_RAC_SERVER="$5"
    shift 5
    local servers=("$@")
    
    echo ""
    echo "=== Configuring client for multi-server access ==="
    
    # Build the WD_SERVER_USER_LIST - ALWAYS put gw1 first
    local server_list=""
    
    # First add gw1 if present
    for server in "${servers[@]}"; do
        if [[ "$server" == *"gw1"* ]]; then
            server_list="\"${sanitized_username}@${server}\""
            break
        fi
    done
    
    # Then add other servers
    for server in "${servers[@]}"; do
        if [[ "$server" != *"gw1"* ]]; then
            if [[ -n "$server_list" ]]; then
                server_list="$server_list \"${sanitized_username}@${server}\""
            else
                server_list="\"${sanitized_username}@${server}\""
            fi
        fi
    done
    
    # Configure on client
    local config_file="\${HOME}/wsprdaemon/wsprdaemon.conf"
    
    # Add WD_SERVER_USER for backward compatibility (primary server)
    local primary_server="${servers[0]}"
    for server in "${servers[@]}"; do
        if [[ "$server" == *"gw1"* ]]; then
            primary_server="$server"
            break
        fi
    done
    
    echo "  Writing configuration to client..."
    if ! ssh -o ConnectTimeout=10 -p "${client_ip_port}" "${client_user}@${WD_RAC_SERVER}" "
        # Remove ALL old WD_SERVER_USER lines (both single and list)
        sed -i '/^[[:space:]]*WD_SERVER_USER=/d' ${config_file}
        sed -i '/^[[:space:]]*WD_SERVER_USER_LIST=/d' ${config_file}
        
        # Add the new WD_SERVER_USER_LIST line with warning comment
        echo '### WARNING: DO NOT REMOVE OR CHANGE THIS LINE which was added by the WD SERVER!' >> ${config_file}
        echo 'WD_SERVER_USER_LIST=($server_list)' >> ${config_file}
        
        # Verify the changes
        echo 'Configuration updated:'
        grep -E '^WD_SERVER_USER|^###.*WD SERVER' ${config_file} || echo 'No WD_SERVER_USER lines found'
    " 2>/dev/null; then
        echo "  ⚠ WARNING: Could not write configuration to client"
        echo "    Possible causes:"
        echo "    - Client may be offline or unreachable"
        echo "    - SSH port forwarding may have issues"
        echo "    - Connection on port ${client_ip_port} may be blocked"
        echo ""
        echo "    Manual configuration needed on client:"
        echo "    1. Remove any existing WD_SERVER_USER lines from ${config_file}"
        echo "    2. Add these lines:"
        echo "       ### WARNING: DO NOT REMOVE OR CHANGE THIS LINE which was added by the WD SERVER!"
        echo "       WD_SERVER_USER_LIST=($server_list)"
        return 1
    fi
    
    # Verify configuration
    echo "  Verifying configuration..."
    if ! ssh -o ConnectTimeout=10 -p "${client_ip_port}" "${client_user}@${WD_RAC_SERVER}" "grep -E '^WD_SERVER_USER_LIST=|^###.*WD SERVER' ${config_file} 2>/dev/null" 2>/dev/null; then
        echo "  ⚠ WARNING: Could not verify configuration on client"
        echo "    Client may need manual configuration"
        local configured_list=""
    else
        local configured_list=$(ssh -p "${client_ip_port}" "${client_user}@${WD_RAC_SERVER}" "grep '^WD_SERVER_USER_LIST=' ${config_file} 2>/dev/null")
    fi
    
    if [[ -n "$configured_list" ]]; then
        echo ""
        echo "  ✓ Client configured successfully!"
        echo ""
        echo "  $configured_list"
        echo ""
        echo "  Client can upload to:"
        local is_first=1
        for server in "${servers[@]}"; do
            if [[ "$server" == *"gw1"* && $is_first -eq 1 ]]; then
                echo "    ${sanitized_username}@${server}  (primary)"
                is_first=0
            else
                echo "    ${sanitized_username}@${server}  (backup)"
            fi
        done
    else
        echo "  ✗ ERROR: Failed to configure client"
        return 1
    fi
    
    return 0
}

# Function to extract client reporter ID from log file
function get_client_reporter_id() {
    local client_rac="$1"
    local client_user="$2"
    local client_ip_port="$3"
    local WD_RAC_SERVER="$4"
    
    echo "" >&2
    echo "=== Extracting client reporter ID ===" >&2
    
    # First test the SSH connection
    if ! ssh -o ConnectTimeout=5 -p "${client_ip_port}" "${client_user}@${WD_RAC_SERVER}" "true" 2>/dev/null; then
        echo "  ✗ ERROR: Cannot connect to client via SSH on port ${client_ip_port}" >&2
        echo "  Check that port forwarding is working for RAC ${client_rac}" >&2
        return 1
    fi
    echo "  ✓ SSH connection to client successful" >&2
    
    # Try to get from the upload log file
    local log_path="~/wsprdaemon/uploads/wsprnet/spots/upload_to_wsprnet_daemon.log"
    echo "  Checking log file: $log_path" >&2
    
    # First check if file exists
    local file_exists=$(ssh -p "${client_ip_port}" "${client_user}@${WD_RAC_SERVER}" "[[ -f $log_path ]] && echo 'yes' || echo 'no'" 2>/dev/null)
    
    if [[ "$file_exists" == "yes" ]]; then
        echo "    Log file found, extracting reporter ID..." >&2
        local reporter_id=$(ssh -p "${client_ip_port}" "${client_user}@${WD_RAC_SERVER}" "
            grep 'my call' $log_path 2>/dev/null | tail -1 | sed -n 's/.*my call \([^ ]*\) and\/or.*/\1/p'
        " 2>/dev/null)
        
        if [[ -n "$reporter_id" ]]; then
            echo "  ✓ Found reporter ID from log: $reporter_id" >&2
            echo "$reporter_id"
            return 0
        else
            echo "    No 'my call' pattern found in log file" >&2
        fi
    else
        echo "    Log file not found" >&2
    fi
    
    # Fallback to RECEIVER_LIST in wsprdaemon.conf (for new RACs without uploads yet)
    echo "  Trying RECEIVER_LIST in wsprdaemon.conf..." >&2
    local conf_path="~/wsprdaemon/wsprdaemon.conf"
    
    # Check if wsprdaemon.conf exists
    file_exists=$(ssh -p "${client_ip_port}" "${client_user}@${WD_RAC_SERVER}" "[[ -f $conf_path ]] && echo 'yes' || echo 'no'" 2>/dev/null)
    
    if [[ "$file_exists" == "yes" ]]; then
        echo "    Config file found, checking RECEIVER_LIST..." >&2
        reporter_id=$(ssh -p "${client_ip_port}" "${client_user}@${WD_RAC_SERVER}" "
            # Source the config to get RECEIVER_LIST array
            source ~/wsprdaemon/wsprdaemon.conf 2>/dev/null
            if [[ \${#RECEIVER_LIST[@]} -gt 0 ]]; then
                # Get third space-separated field from first element
                echo \"\${RECEIVER_LIST[0]}\" | awk '{print \$3}'
            fi
        " 2>/dev/null)
        
        if [[ -n "$reporter_id" ]]; then
            echo "  ✓ Found reporter ID from RECEIVER_LIST: $reporter_id" >&2
            echo "$reporter_id"
            return 0
        else
            echo "    No RECEIVER_LIST found or empty" >&2
        fi
    else
        echo "    Config file not found" >&2
    fi
    
    # Fallback to CSV method if log method fails
    echo "  Log file method failed, trying CSV database..." >&2
    local csv_path="~/wsprdaemon/spots.csv"
    
    # Check if CSV exists
    file_exists=$(ssh -p "${client_ip_port}" "${client_user}@${WD_RAC_SERVER}" "[[ -f $csv_path ]] && echo 'yes' || echo 'no'" 2>/dev/null)
    
    if [[ "$file_exists" == "yes" ]]; then
        echo "    CSV file found, extracting field 7..." >&2
        reporter_id=$(ssh -p "${client_ip_port}" "${client_user}@${WD_RAC_SERVER}" "
            tail -1 $csv_path 2>/dev/null | cut -d',' -f7
        " 2>/dev/null)
        
        if [[ -n "$reporter_id" ]]; then
            echo "  ✓ Found reporter ID from CSV: $reporter_id" >&2
            echo "$reporter_id"
            return 0
        else
            echo "    CSV file empty or invalid format" >&2
        fi
    else
        echo "    CSV file not found" >&2
    fi
    
    # Last resort - try to find any spots files
    echo "  Looking for alternative file locations..." >&2
    local alt_files=$(ssh -p "${client_ip_port}" "${client_user}@${WD_RAC_SERVER}" "
        find ~ -name 'upload_to_wsprnet_daemon.log' -o -name 'spots.csv' 2>/dev/null | head -5
    " 2>/dev/null)
    
    if [[ -n "$alt_files" ]]; then
        echo "    Found files at:" >&2
        echo "$alt_files" | sed 's/^/      /' >&2
        echo "    Please check if these paths are correct" >&2
    fi
    
    echo "  ✗ ERROR: Could not extract reporter ID from client" >&2
    echo "    Please run: ./wd-register-client-debug.sh ${client_rac}" >&2
    echo "    to diagnose the issue" >&2
    return 1
}

# Function to register all RACs from the need-reg file
function register_batch() {
    local reg_file="wd-register-clients.need-reg"
    
    echo ""
    echo "========================================="
    echo "BATCH REGISTRATION"
    echo "========================================="
    
    if [[ ! -f "$reg_file" ]]; then
        echo "ERROR: File not found: $reg_file"
        echo "Run './wd-register-client.sh --scan-racs' first to generate the list"
        exit 1
    fi
    
    local rac_count=$(wc -l < "$reg_file")
    if [[ $rac_count -eq 0 ]]; then
        echo "No RACs to register in $reg_file"
        exit 0
    fi
    
    echo "Found $rac_count RACs to register"
    echo ""
    
    local success=0
    local failed=0
    
    while IFS= read -r rac; do
        echo "========================================="
        echo "Registering RAC $rac..."
        echo "========================================="
        
        # Call the script with the RAC number
        if ./wd-register-client.sh "$rac"; then
            ((success++))
            echo "✓ RAC $rac registered successfully"
        else
            ((failed++))
            echo "✗ RAC $rac registration failed"
        fi
        echo ""
        
        # Small delay between registrations
        sleep 2
    done < "$reg_file"
    
    echo "========================================="
    echo "BATCH REGISTRATION COMPLETE"
    echo "========================================="
    echo "Successful: $success"
    echo "Failed:     $failed"
    echo ""
    
    exit 0
}

# Function to SSH to RACs with outdated WD versions for manual updates
function update_clients() {
    local update_file="wd-register-clients.need-update"
    
    echo ""
    echo "========================================="
    echo "CLIENT UPDATE SESSION"
    echo "========================================="
    
    if [[ ! -f "$update_file" ]]; then
        echo "ERROR: File not found: $update_file"
        echo "Run './wd-register-client.sh --scan-racs' first to generate the list"
        exit 1
    fi
    
    local rac_count=$(wc -l < "$update_file")
    if [[ $rac_count -eq 0 ]]; then
        echo "No RACs need updating in $update_file"
        exit 0
    fi
    
    # Always use gw2 for RAC connections (RAC ports only on gw2)
    local WD_RAC_SERVER="gw2"
    
    echo "Found $rac_count RACs with outdated versions"
    echo "You will be connected to each RAC for manual update"
    echo ""
    echo "Update commands to run on each client:"
    echo "  cd ~/wsprdaemon"
    echo "  git pull"
    echo "  ./wsprdaemon.sh -V  # Check version"
    echo "  exit  # When done"
    echo ""
    echo "Press Enter to continue or Ctrl-C to abort..."
    read
    
    while IFS=':' read -r rac port ssh_user; do
        echo ""
        echo "========================================="
        echo "Connecting to RAC $rac (port $port)..."
        echo "========================================="
        echo "Commands: cd ~/wsprdaemon && git pull"
        echo ""
        
        # SSH to the client
        ssh -o StrictHostKeyChecking=no -o UserKnownHostsFile=/dev/null -p "$port" "${ssh_user}@${WD_RAC_SERVER}"
        
        echo "✓ Disconnected from RAC $rac"
        echo ""
        echo "Press Enter for next RAC or Ctrl-C to stop..."
        read
    done < "$update_file"
    
    echo ""
    echo "========================================="
    echo "CLIENT UPDATE SESSION COMPLETE"
    echo "========================================="
    echo "Re-run './wd-register-client.sh --scan-racs' to verify versions"
    echo ""
    
    exit 0
}

# Main function
function main() {
    # Handle version argument
    if [[ "${1:-}" == "-v" || "${1:-}" == "--version" ]]; then
        echo "$SCRIPT_NAME version $VERSION"
        exit 0
    fi
    
    # Handle scan-racs argument
    if [[ "${1:-}" == "--scan-racs" || "${1:-}" == "--scan" ]]; then
        scan_all_racs
        exit 0
    fi
    
    # Handle --config argument (generate config lines only, no SSH required)
    if [[ "${1:-}" == "--config" ]]; then
        show_version
        generate_config_output "${2:-}" "${3:-}"
        exit $?
    fi
    
    # Handle --update-ssr argument (generate updated .ssr.conf with REPORTER_IDs)
    if [[ "${1:-}" == "--update-ssr" ]]; then
        show_version
        generate_updated_ssr_conf
        exit $?
    fi
    
    # Handle --register-batch argument (register all RACs from need-reg file)
    if [[ "${1:-}" == "--register-batch" ]]; then
        show_version
        register_batch
        exit $?
    fi
    
    # Handle --update-clients argument (SSH to clients with outdated versions)
    if [[ "${1:-}" == "--update-clients" ]]; then
        show_version
        update_clients
        exit $?
    fi
    
    # Always show version when running
    show_version
    
    # Parse arguments
    local client_rac="${1:-}"
    local manual_reporter_id=""
    local verbosity=0
    
    # Check if second argument is reporter ID or verbose flag
    if [[ -n "${2:-}" ]]; then
        if [[ "$2" == "--verbose" || "$2" == "-v" ]]; then
            verbosity=1
        else
            # Assume it's a manual reporter ID
            manual_reporter_id="$2"
            echo "Using manually specified reporter ID: $manual_reporter_id"
        fi
    fi
    
    # Check for verbose as third argument if manual ID was provided
    if [[ -n "$manual_reporter_id" && "${3:-}" == "--verbose" ]]; then
        verbosity=1
    fi
    
    if [[ -z "$client_rac" ]]; then
        echo "Usage: $SCRIPT_NAME <client_rac_number> [<reporter_id>] [--verbose]"
        echo "       $SCRIPT_NAME --config <reporter_id> <psk>"
        echo "       $SCRIPT_NAME --scan-racs"
        echo "       $SCRIPT_NAME --register-batch"
        echo "       $SCRIPT_NAME --update-clients"
        echo "       $SCRIPT_NAME --update-ssr"
        echo "       $SCRIPT_NAME --version"
        echo ""
        echo "Examples:"
        echo "  $SCRIPT_NAME 84                        # Register RAC 84 (auto-detect ID)"
        echo "  $SCRIPT_NAME 84 KJ6MKI                 # Register with manual reporter ID"
        echo "  $SCRIPT_NAME 84 --verbose              # Register with verbose output"
        echo "  $SCRIPT_NAME --config KJ6MKI 'ssh-rsa AAA...'  # Create accounts + output config"
        echo "  $SCRIPT_NAME --scan-racs               # Check all RACs + auto-fix SSH keys"
        echo "  $SCRIPT_NAME --register-batch          # Register all RACs from need-reg file"
        echo "  $SCRIPT_NAME --update-clients          # SSH to RACs with old WD versions"
        echo "  $SCRIPT_NAME --update-ssr              # Generate .ssr.conf with REPORTER_IDs"
        echo "  $SCRIPT_NAME --version                 # Show version"
        echo ""
        echo "The --config mode creates user accounts on both servers, adds the client's"
        echo "SSH public key (PSK) to authorized_keys, and outputs the wsprdaemon.conf line."
        echo ""
        echo "The --update-ssr mode scans active RACs, extracts their REPORTER_IDs, and"
        echo "generates ~/.ssr.conf.updated with REPORTER_IDs in the description field."
        exit 1
    fi
    
    # Auto-detect configuration based on hostname (case-insensitive)
    local hostname=$(hostname | tr '[:upper:]' '[:lower:]')
    local WD_SERVER_FQDN=""
    local WD_BACKUP_SERVERS=""
    local WD_RAC_SERVER=""  # Will be set based on hostname
    
    echo "Detecting gateway configuration..."
    echo "  Hostname: $(hostname)"
    
    case "$hostname" in
        *gw1*)
            WD_SERVER_FQDN="gw1.wsprdaemon.org"
            WD_BACKUP_SERVERS="gw2.wsprdaemon.org"
            WD_RAC_SERVER="gw2"  # RAC ports are only on gw2
            echo "  ✓ Detected GW1 - Primary: gw1, Backup: gw2"
            ;;
        *gw2*)
            WD_SERVER_FQDN="gw2.wsprdaemon.org"
            WD_BACKUP_SERVERS="gw1.wsprdaemon.org"
            WD_RAC_SERVER="gw2"  # RAC ports are only on gw2
            echo "  ✓ Detected GW2 - Primary: gw2, Backup: gw1"
            ;;
        *)
            WD_SERVER_FQDN="gw1.wsprdaemon.org"
            WD_BACKUP_SERVERS="gw2.wsprdaemon.org"
            WD_RAC_SERVER="gw2"  # RAC ports are only on gw2
            echo "  ✓ Unknown hostname - Defaulting to Primary: gw1, Backup: gw2"
            ;;
    esac
    
    # Load configuration file if it exists
    if [[ -f "client-register.conf" ]]; then
        echo "  Loading configuration from client-register.conf..."
        source client-register.conf
    fi
    
    echo ""
    echo "Configuration:"
    echo "  Primary Server: $WD_SERVER_FQDN"
    echo "  Backup Servers: $WD_BACKUP_SERVERS"
    echo "  RAC Server: $WD_RAC_SERVER"
    echo "  Client RAC: $client_rac"
    
    # Calculate port (35800 + RAC number)
    local client_ip_port=$((35800 + client_rac))
    echo "  Client Port: $client_ip_port"
    
    # Get client username from .ssr.conf file for this RAC
    local ssr_conf_file="${HOME}/.ssr.conf"
    if [[ ! -f "$ssr_conf_file" ]]; then
        echo "ERROR: .ssr.conf file not found at $ssr_conf_file"
        exit 1
    fi
    
    # Source the .ssr.conf to load the FRPS_REMOTE_ACCESS_LIST array
    source "$ssr_conf_file"
    
    # Find the entry for this RAC
    # Format: "RAC,wd_user,wd_pass,ssh_user,ssh_pass,description,port_forwards"
    local client_entry=""
    for entry in "${FRPS_REMOTE_ACCESS_LIST[@]}"; do
        if [[ "$entry" =~ ^${client_rac}, ]]; then
            client_entry="$entry"
            break
        fi
    done
    
    if [[ -z "$client_entry" ]]; then
        echo "ERROR: No entry found for RAC $client_rac in .ssr.conf"
        exit 1
    fi
    
    # Parse the SSH username (field 3) from the entry
    local client_user=$(echo "$client_entry" | cut -d',' -f3)
    if [[ -z "$client_user" ]]; then
        echo "ERROR: Could not extract SSH username for RAC $client_rac from: $client_entry"
        exit 1
    fi
    
    echo "  Client User: $client_user (from .ssr.conf)"
    
    # Ensure SSHD is configured for SFTP-only access
    if ! wd-sshd-conf-add-sftponly; then
        echo "ERROR: Failed to configure SSHD"
        exit 1
    fi
    
    # Ensure sftponly group exists
    if ! ensure_sftponly_group; then
        echo "ERROR: Failed to ensure sftponly group"
        exit 1
    fi
    
    # Get client reporter ID (manual or auto-detect)
    local client_reporter_id=""
    if [[ -n "$manual_reporter_id" ]]; then
        client_reporter_id="$manual_reporter_id"
        echo "Using manual reporter ID: $client_reporter_id"
    else
        client_reporter_id=$(get_client_reporter_id "$client_rac" "$client_user" "$client_ip_port" "$WD_RAC_SERVER")
        if [[ -z "$client_reporter_id" ]]; then
            echo ""
            echo "ERROR: Could not determine client reporter ID automatically"
            echo ""
            echo "You can specify the reporter ID manually:"
            echo "  $SCRIPT_NAME $client_rac <REPORTER_ID>"
            echo ""
            echo "Example:"
            echo "  $SCRIPT_NAME $client_rac KJ6MKI"
            exit 1
        fi
    fi
    
    # Sanitize the reporter ID for use as Linux username
    local sanitized_reporter_id=$(echo "$client_reporter_id" | tr '/' '_' | tr '.' '_' | tr '-' '_' | tr '@' '_')
    
    if [[ "$client_reporter_id" != "$sanitized_reporter_id" ]]; then
        echo ""
        echo "NOTE: Reporter ID '$client_reporter_id' contains invalid characters for Linux username"
        echo "      Using sanitized username: '$sanitized_reporter_id'"
    fi
    
    # Setup local user
    if ! setup_local_user "$client_reporter_id" "$sanitized_reporter_id"; then
        echo "ERROR: Failed to setup local user"
        exit 1
    fi
    
    # Build list of all servers (primary + backups)
    local all_servers=("$WD_SERVER_FQDN")
    for server in $WD_BACKUP_SERVERS; do
        all_servers+=("$server")
    done
    
    # Replicate to backup servers
    for backup_server in $WD_BACKUP_SERVERS; do
        if ! replicate_user_to_server "$client_reporter_id" "$sanitized_reporter_id" "$backup_server"; then
            echo ""
            echo "========================================="
            echo "✗ CRITICAL ERROR: Failed to replicate user to $backup_server"
            echo "========================================="
            echo ""
            echo "The user account could not be created on $backup_server."
            echo "This is a critical failure - both servers must have the user account."
            echo ""
            echo "Common causes:"
            echo "  1. User already exists with different settings"
            echo "  2. SSH connection issues to $backup_server"
            echo "  3. Permission problems on $backup_server"
            echo ""
            echo "To fix manually:"
            echo "  ssh $backup_server"
            echo "  sudo useradd -m -s /usr/sbin/nologin '$sanitized_reporter_id'"
            echo "  sudo usermod -p '*' '$sanitized_reporter_id'"
            echo "  sudo usermod -a -G sftponly '$sanitized_reporter_id'"
            echo ""
            echo "Then re-run this script."
            exit 1
        fi
    done
    
    # Ensure client has SSH keys (create if missing)
    if ! ensure_client_ssh_keys "$client_rac" "$client_user" "$client_ip_port" "$WD_RAC_SERVER"; then
        echo ""
        echo "========================================="
        echo "✗ CRITICAL ERROR: Client SSH key setup failed"
        echo "========================================="
        echo ""
        echo "Cannot continue without SSH keys on the client."
        echo "The client needs SSH keys to upload WSPR data to the servers."
        echo ""
        exit 1
    fi
    
    # Diagnose and fix authorized_keys on all servers
    if ! diagnose_and_fix_authorized_keys "$client_rac" "$client_user" "$client_ip_port" "$sanitized_reporter_id" "$WD_RAC_SERVER" "${all_servers[@]}"; then
        echo ""
        echo "========================================="
        echo "✗ CRITICAL ERROR: SSH key installation failed"
        echo "========================================="
        echo ""
        echo "Could not install client's SSH key on the servers."
        echo "Without this, the client cannot upload WSPR data."
        echo ""
        echo "Manual intervention required - see error messages above."
        exit 1
    fi
    
    # Test SFTP uploads to all servers (from client's perspective)
    if ! test_sftp_uploads "$sanitized_reporter_id" "$client_rac" "$client_user" "$client_ip_port" "$WD_RAC_SERVER" "${all_servers[@]}"; then
        echo "WARNING: Some SFTP tests had issues (continuing with configuration)"
    fi
    
    # Configure client for multi-server access
    # ALWAYS configure for all servers regardless of test results
    if ! configure_client_access "$client_rac" "$client_user" "$client_ip_port" "$sanitized_reporter_id" "$WD_RAC_SERVER" "${all_servers[@]}"; then
        echo "WARNING: Could not configure client automatically"
        echo ""
        echo "========================================="
        echo "⚠ Server setup completed, client needs manual config!"
        echo "========================================="
        echo ""
        echo "Server-side configuration:"
        echo "  ✓ User accounts created on all servers"
        echo "  ✓ SSH keys installed"
        echo "  ✓ Directories configured"
        echo ""
        echo "Client-side configuration needed:"
        echo "  1. SSH to client: ssh -p ${client_ip_port} ${client_user}@${WD_RAC_SERVER}"
        echo "  2. Edit ~/wsprdaemon/wsprdaemon.conf"
        echo "  3. Remove any existing WD_SERVER_USER or WD_SERVER_USER_LIST lines"
        echo "  4. Add these lines:"
        echo "     ### WARNING: DO NOT REMOVE OR CHANGE THIS LINE which was added by the WD SERVER!"
        echo "     WD_SERVER_USER_LIST=(\"${sanitized_reporter_id}@gw1.wsprdaemon.org\" \"${sanitized_reporter_id}@gw2.wsprdaemon.org\")"
        echo ""
    else
        echo ""
        echo "========================================="
        echo "✓ Client registration completed!"
        echo "========================================="
        echo ""
    fi
    
    echo "Reporter: $client_reporter_id"
    echo "Username: $sanitized_reporter_id"
    echo "Servers configured: ${#all_servers[@]}"
    echo ""
    
    return 0
}

# Run main function
main "$@"
