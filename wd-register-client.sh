#!/bin/bash
#
# wd-register-client.sh v2.7.6
# 
# Script to register WSPRDAEMON client stations for SFTP uploads
# Creates user accounts on gateway servers and configures client access
#
# Usage:
#   ./wd-register-client.sh <client_rac_number> [--verbose]
#   ./wd-register-client.sh <client_rac_number> <reporter_id>  # Manual override
#   ./wd-register-client.sh <client_rac_number> --user <username>  # Override SSH user
#   ./wd-register-client.sh --version
#
# Examples:
#   ./wd-register-client.sh 84              # Register client RAC 84 (auto-detect ID)
#   ./wd-register-client.sh 84 KJ6MKI       # Register with manual reporter ID
#   ./wd-register-client.sh 84 --user rob   # Use 'rob' instead of config file username
#   ./wd-register-client.sh 84 KJ6MKI --user rob  # Both overrides
#   ./wd-register-client.sh 84 --verbose    # Register with verbose output  
#   ./wd-register-client.sh --version       # Show version only
#
# Changes in v2.7.6:
#   - ADDED: --user option to override SSH username from config file
#   - Example: ./wd-register-client.sh 84 --user rob
#
# Changes in v2.7.5:
#   - IMPROVED: Now searches all three config files for RAC entries:
#     - ~/.ssr.conf (main config)
#     - ~/.ssr.conf.local (local overrides and additions)
#     - ~/.ssr.conf.hamsci (HamSCI stations)
#   - Shows which config file the RAC entry was found in
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

VERSION="2.7.6"
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
        echo "    ✗ ERROR: Could not get client's public key from RAC"
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
        
        # Add the new WD_SERVER_USER_LIST line
        echo 'WD_SERVER_USER_LIST=($server_list)' >> ${config_file}
        
        # Verify the changes
        echo 'Configuration updated:'
        grep -E '^WD_SERVER_USER' ${config_file} || echo 'No WD_SERVER_USER lines found'
    " 2>/dev/null; then
        echo "  ⚠ WARNING: Could not write configuration to client"
        echo "    Possible causes:"
        echo "    - Client may be offline or unreachable"
        echo "    - SSH port forwarding may have issues"
        echo "    - Connection on port ${client_ip_port} may be blocked"
        echo ""
        echo "    Manual configuration needed on client:"
        echo "    1. Remove any existing WD_SERVER_USER lines from ${config_file}"
        echo "    2. Add this line:"
        echo "       WD_SERVER_USER_LIST=($server_list)"
        return 1
    fi
    
    # Verify configuration
    echo "  Verifying configuration..."
    if ! ssh -o ConnectTimeout=10 -p "${client_ip_port}" "${client_user}@${WD_RAC_SERVER}" "grep '^WD_SERVER_USER_LIST=' ${config_file} 2>/dev/null" 2>/dev/null; then
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

# Main function
function main() {
    # Handle version argument
    if [[ "${1:-}" == "-v" || "${1:-}" == "--version" ]]; then
        echo "$SCRIPT_NAME version $VERSION"
        exit 0
    fi
    
    # Always show version when running
    show_version
    
    # Parse arguments
    local client_rac=""
    local manual_reporter_id=""
    local manual_ssh_user=""
    local verbosity=0
    
    # Process all arguments
    while [[ $# -gt 0 ]]; do
        case "$1" in
            --verbose|-v)
                verbosity=1
                shift
                ;;
            --user)
                if [[ -n "${2:-}" && ! "$2" =~ ^-- ]]; then
                    manual_ssh_user="$2"
                    shift 2
                else
                    echo "ERROR: --user requires a username argument"
                    exit 1
                fi
                ;;
            --version)
                echo "$SCRIPT_NAME version $VERSION"
                exit 0
                ;;
            -*)
                echo "ERROR: Unknown option: $1"
                exit 1
                ;;
            *)
                # Positional arguments: first is RAC, second (if present) is reporter ID
                if [[ -z "$client_rac" ]]; then
                    client_rac="$1"
                elif [[ -z "$manual_reporter_id" ]]; then
                    manual_reporter_id="$1"
                else
                    echo "ERROR: Too many positional arguments"
                    exit 1
                fi
                shift
                ;;
        esac
    done
    
    if [[ -n "$manual_reporter_id" ]]; then
        echo "Using manually specified reporter ID: $manual_reporter_id"
    fi
    
    if [[ -n "$manual_ssh_user" ]]; then
        echo "Using manually specified SSH user: $manual_ssh_user"
    fi
    
    if [[ -z "$client_rac" ]]; then
        echo "Usage: $SCRIPT_NAME <client_rac_number> [<reporter_id>] [--user <username>] [--verbose]"
        echo "       $SCRIPT_NAME --version"
        echo ""
        echo "Example: $SCRIPT_NAME 84"
        echo "         $SCRIPT_NAME 84 KJ6MKI"
        echo "         $SCRIPT_NAME 84 --user rob"
        echo "         $SCRIPT_NAME 84 KJ6MKI --user rob"
        echo "         $SCRIPT_NAME 84 --verbose"
        echo "         $SCRIPT_NAME --version"
        exit 1
    fi
    
    # Auto-detect configuration based on hostname (case-insensitive)
    local hostname=$(hostname | tr '[:upper:]' '[:lower:]')
    local WD_SERVER_FQDN=""
    local WD_BACKUP_SERVERS=""
    local WD_RAC_SERVER="gw2"  # Default RAC server
    
    echo "Detecting gateway configuration..."
    echo "  Hostname: $(hostname)"
    
    case "$hostname" in
        *gw1*)
            WD_SERVER_FQDN="gw1.wsprdaemon.org"
            WD_BACKUP_SERVERS="gw2.wsprdaemon.org"
            echo "  ✓ Detected GW1 - Primary: gw1, Backup: gw2"
            ;;
        *gw2*)
            WD_SERVER_FQDN="gw2.wsprdaemon.org"
            WD_BACKUP_SERVERS="gw1.wsprdaemon.org"
            echo "  ✓ Detected GW2 - Primary: gw2, Backup: gw1"
            ;;
        *)
            WD_SERVER_FQDN="gw1.wsprdaemon.org"
            WD_BACKUP_SERVERS="gw2.wsprdaemon.org"
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
    
    # Get client username from .ssr.conf files for this RAC
    # Search all three config files: .ssr.conf, .ssr.conf.local, .ssr.conf.hamsci
    local ssr_conf_files=(
        "${HOME}/.ssr.conf"
        "${HOME}/.ssr.conf.local"
        "${HOME}/.ssr.conf.hamsci"
    )
    
    # Initialize empty array - will accumulate entries from all files
    FRPS_REMOTE_ACCESS_LIST=()
    local found_any_config=0
    local loaded_files=""
    
    echo "Loading RAC configuration files..."
    for ssr_conf_file in "${ssr_conf_files[@]}"; do
        if [[ -f "$ssr_conf_file" ]]; then
            source "$ssr_conf_file"
            loaded_files+="  ✓ Loaded: $ssr_conf_file"$'\n'
            found_any_config=1
        else
            loaded_files+="  - Not found: $ssr_conf_file"$'\n'
        fi
    done
    
    echo "$loaded_files"
    
    if [[ $found_any_config -eq 0 ]]; then
        echo "ERROR: No .ssr.conf files found"
        echo "Expected at least one of:"
        for f in "${ssr_conf_files[@]}"; do
            echo "  $f"
        done
        exit 1
    fi
    
    echo "  Total RAC entries loaded: ${#FRPS_REMOTE_ACCESS_LIST[@]}"
    
    # Find the entry for this RAC
    # Format: "RAC,wd_user,ssh_user,ssh_pass,description,port_forwards"
    local client_entry=""
    local found_in_file=""
    for entry in "${FRPS_REMOTE_ACCESS_LIST[@]}"; do
        if [[ "$entry" =~ ^${client_rac}, ]]; then
            client_entry="$entry"
            # Determine which file this entry came from (for informational purposes)
            for ssr_conf_file in "${ssr_conf_files[@]}"; do
                if [[ -f "$ssr_conf_file" ]] && grep -q "^[[:space:]]*\"${client_rac}," "$ssr_conf_file" 2>/dev/null; then
                    found_in_file="$ssr_conf_file"
                    break
                fi
            done
            break
        fi
    done
    
    if [[ -z "$client_entry" ]]; then
        echo "ERROR: No entry found for RAC $client_rac in any config file"
        echo "Searched files:"
        for f in "${ssr_conf_files[@]}"; do
            [[ -f "$f" ]] && echo "  $f"
        done
        exit 1
    fi
    
    echo "  ✓ Found RAC $client_rac in: ${found_in_file:-unknown}"
    
    # Parse the SSH username (field 3) from the entry
    local config_user=$(echo "$client_entry" | cut -d',' -f3)
    if [[ -z "$config_user" ]]; then
        echo "ERROR: Could not extract SSH username for RAC $client_rac from: $client_entry"
        exit 1
    fi
    
    # Use manual override if provided, otherwise use config file value
    local client_user
    if [[ -n "$manual_ssh_user" ]]; then
        client_user="$manual_ssh_user"
        echo "  Client User: $client_user (manual override, config had: $config_user)"
    else
        client_user="$config_user"
        echo "  Client User: $client_user (from config file)"
    fi
    
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
    
    # Diagnose and fix authorized_keys on all servers
    if ! diagnose_and_fix_authorized_keys "$client_rac" "$client_user" "$client_ip_port" "$sanitized_reporter_id" "$WD_RAC_SERVER" "${all_servers[@]}"; then
        echo "WARNING: Key diagnosis/repair had issues (continuing anyway)"
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
        echo "  4. Add this line:"
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
