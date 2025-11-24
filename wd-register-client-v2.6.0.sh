#!/bin/bash
#
# wd-register-client.sh v2.6.0
# 
# Script to register WSPRDAEMON client stations for SFTP uploads
# Creates user accounts on gateway servers and configures client access
#
# Changes in v2.6.0:
#   - Extract client_reporter_id from upload_to_wsprnet_daemon.log instead of CSV
#   - Improved error handling for missing log files
#
# Changes in v2.5.0:
#   - Only lock passwords for NEW users, not existing ones
#   - Compare and auto-fix authorized_keys before declaring failure
#   - Always configure client for BOTH servers regardless of test results
#
# Author: AI6VN (with assistance from Claude)
# Date: November 2025

VERSION="2.6.0"
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
        echo "✓ User '$sanitized_username' already exists - skipping password lock"
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
    
    # Add to sftponly group
    echo "Adding user to 'sftponly' group..."
    if ! sudo usermod -a -G sftponly "$sanitized_username"; then
        echo "ERROR: Failed to add user to sftponly group"
        return 1
    fi
    echo "✓ Added to sftponly group"
    
    # Setup SSH directory
    local ssh_dir="/home/$sanitized_username/.ssh"
    echo "Setting up SSH directory..."
    if ! sudo mkdir -p "$ssh_dir"; then
        echo "ERROR: Failed to create SSH directory"
        return 1
    fi
    
    # Set permissions
    sudo chmod 700 "$ssh_dir"
    sudo touch "$ssh_dir/authorized_keys"
    sudo chmod 600 "$ssh_dir/authorized_keys"
    sudo chown -R "$sanitized_username:$sanitized_username" "$ssh_dir"
    echo "✓ SSH directory configured"
    
    # Setup upload directory
    local upload_dir="/home/$sanitized_username/uploads"
    echo "Setting up upload directory..."
    if ! sudo mkdir -p "$upload_dir"; then
        echo "ERROR: Failed to create upload directory"
        return 1
    fi
    
    sudo chmod 755 "$upload_dir"
    sudo chown "$sanitized_username:$sanitized_username" "$upload_dir"
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
    
    # Get local user's UID and GID
    local uid=$(id -u "$sanitized_username" 2>/dev/null)
    local gid=$(id -g "$sanitized_username" 2>/dev/null)
    
    if [[ -z "$uid" || -z "$gid" ]]; then
        echo "ERROR: Could not get UID/GID for user '$sanitized_username'"
        return 1
    fi
    
    echo "Local user $sanitized_username: UID=$uid, GID=$gid"
    
    # Check if user exists on remote server
    if ssh "$server" "id '$sanitized_username' 2>/dev/null" >/dev/null; then
        echo "✓ User already exists on $server - skipping password operations"
        local remote_user_exists=1
    else
        echo "Creating user on $server..."
        if ! ssh "$server" "sudo useradd -m -u $uid -g $gid -d '/home/$sanitized_username' -s /usr/sbin/nologin '$sanitized_username' 2>/dev/null || true"; then
            echo "WARNING: useradd reported an issue (user might already exist)"
        fi
        local remote_user_exists=0
    fi
    
    # Only set disabled password for NEW users on remote
    if [[ $remote_user_exists -eq 0 ]]; then
        echo "Setting disabled password for new remote user..."
        ssh "$server" "sudo usermod -p '*' '$sanitized_username'"
        echo "✓ Remote password disabled"
    fi
    
    # Ensure sftponly group exists and add user
    echo "Configuring groups on $server..."
    ssh "$server" "sudo groupadd -f sftponly 2>/dev/null || true"
    ssh "$server" "sudo usermod -a -G sftponly '$sanitized_username'"
    echo "✓ Groups configured"
    
    # Setup directories
    echo "Setting up directories on $server..."
    ssh "$server" "
        sudo mkdir -p /home/$sanitized_username/{.ssh,uploads}
        sudo chmod 700 /home/$sanitized_username/.ssh
        sudo chmod 755 /home/$sanitized_username/uploads
        sudo touch /home/$sanitized_username/.ssh/authorized_keys
        sudo chmod 600 /home/$sanitized_username/.ssh/authorized_keys
        sudo chown -R $sanitized_username:$sanitized_username /home/$sanitized_username/{.ssh,uploads}
        sudo chown root:root /home/$sanitized_username
        sudo chmod 755 /home/$sanitized_username
    "
    echo "✓ Directories configured on $server"
    
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

# Function to test SFTP uploads to all servers
function test_sftp_uploads() {
    local sanitized_username="$1"
    shift
    local servers=("$@")
    
    echo ""
    echo "=== Testing SFTP uploads to all servers ==="
    
    local success_count=0
    local total_count=${#servers[@]}
    local working_servers=()
    
    for server_fqdn in "${servers[@]}"; do
        echo "  Testing SFTP upload to $server_fqdn..."
        
        # Create a test file
        local test_file="/tmp/.wd_upload_test_$$"
        echo "Test upload at $(date)" > "$test_file"
        
        # Try SFTP upload
        if sftp -q -o BatchMode=yes -o StrictHostKeyChecking=no "${sanitized_username}@${server_fqdn}" <<< "put $test_file uploads/" >/dev/null 2>&1; then
            
            # Verify file arrived
            local is_local=0
            if [[ "$server_fqdn" == *"$(hostname)"* ]] || [[ "$(hostname)" == *"${server_fqdn%%.*}"* ]]; then
                is_local=1
            fi
            
            if [[ $is_local -eq 1 ]]; then
                if sudo ls "/home/$sanitized_username/uploads/$(basename $test_file)" >/dev/null 2>&1; then
                    sudo rm -f "/home/$sanitized_username/uploads/$(basename $test_file)"
                    echo "    ✓ SUCCESS: Upload to $server_fqdn passed"
                    working_servers+=("$server_fqdn")
                    ((success_count++))
                else
                    echo "    ✗ FAILED: Upload appeared to work but file not found"
                fi
            else
                if ssh "$server_fqdn" "sudo ls '/home/$sanitized_username/uploads/$(basename $test_file)' >/dev/null 2>&1"; then
                    ssh "$server_fqdn" "sudo rm -f '/home/$sanitized_username/uploads/$(basename $test_file)'"
                    echo "    ✓ SUCCESS: Upload to $server_fqdn passed"
                    working_servers+=("$server_fqdn")
                    ((success_count++))
                else
                    echo "    ✗ FAILED: Upload appeared to work but file not found"
                fi
            fi
        else
            echo "    ✗ FAILED: SFTP upload to $server_fqdn failed"
        fi
        
        rm -f "$test_file"
    done
    
    echo ""
    if [[ $success_count -eq $total_count ]]; then
        echo "  SUCCESS: All SFTP upload tests passed ($success_count/$total_count)"
        return 0
    elif [[ $success_count -gt 0 ]]; then
        echo "  WARNING: Only $success_count of $total_count servers passed SFTP tests"
        echo "           Working servers: ${working_servers[*]}"
        echo "           Client will be configured for ALL servers anyway"
        return 0  # Return success so configuration continues
    else
        echo "  ERROR: All SFTP upload tests failed (0/$total_count)"
        return 1
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
    ssh -p "${client_ip_port}" "${client_user}@${WD_RAC_SERVER}" "
        # Handle WD_SERVER_USER (backward compatibility)
        sed -i '/^[[:space:]]*WD_SERVER_USER=/d' ${config_file}
        echo 'WD_SERVER_USER=\"${sanitized_username}@${primary_server}\"' >> ${config_file}
        
        # Handle WD_SERVER_USER_LIST
        sed -i '/^[[:space:]]*WD_SERVER_USER_LIST=/d' ${config_file}
        echo 'WD_SERVER_USER_LIST=($server_list)' >> ${config_file}
    "
    
    # Verify configuration
    echo "  Verifying configuration..."
    local configured_list=$(ssh -p "${client_ip_port}" "${client_user}@${WD_RAC_SERVER}" "grep '^WD_SERVER_USER_LIST=' ${config_file} 2>/dev/null")
    
    if [[ -n "$configured_list" ]]; then
        echo ""
        echo "  ✓ Client configured successfully!"
        echo ""
        echo "  $configured_list"
        echo ""
        echo "  Client can upload to:"
        for server in "${servers[@]}"; do
            if [[ "$server" == *"gw1"* ]]; then
                echo "    ${sanitized_username}@${server}  (primary)"
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
    
    echo ""
    echo "=== Extracting client reporter ID ==="
    
    # Try to get from the upload log file
    local log_path="~/wsprdaemon/uploads/wsprnet/spots/upload_to_wsprnet_daemon.log"
    echo "  Checking log file: $log_path"
    
    local reporter_id=$(ssh -p "${client_ip_port}" "${client_user}@${WD_RAC_SERVER}" "
        if [[ -f $log_path ]]; then
            grep 'my call' $log_path | tail -1 | sed -n 's/.*my call \([^ ]*\) and\/or.*/\1/p'
        fi
    " 2>/dev/null)
    
    if [[ -n "$reporter_id" ]]; then
        echo "  ✓ Found reporter ID from log: $reporter_id"
        echo "$reporter_id"
        return 0
    fi
    
    # Fallback to CSV method if log method fails
    echo "  Log file method failed, trying CSV database..."
    local csv_path="~/wsprdaemon/spots.csv"
    
    reporter_id=$(ssh -p "${client_ip_port}" "${client_user}@${WD_RAC_SERVER}" "
        if [[ -f $csv_path ]]; then
            tail -1 $csv_path | cut -d',' -f7
        fi
    " 2>/dev/null)
    
    if [[ -n "$reporter_id" ]]; then
        echo "  ✓ Found reporter ID from CSV: $reporter_id"
        echo "$reporter_id"
        return 0
    fi
    
    echo "  ✗ ERROR: Could not extract reporter ID from client"
    return 1
}

# Main function
function main() {
    show_version
    
    # Parse arguments
    local client_rac="${1:-}"
    local verbosity=0
    
    if [[ "$2" == "-v" || "$2" == "--verbose" ]]; then
        verbosity=1
    fi
    
    if [[ -z "$client_rac" ]]; then
        echo "Usage: $SCRIPT_NAME <client_rac_number> [-v|--verbose]"
        echo ""
        echo "Example: $SCRIPT_NAME 84"
        echo "         $SCRIPT_NAME 84 -v"
        exit 1
    fi
    
    # Auto-detect configuration based on hostname
    local hostname=$(hostname)
    local WD_SERVER_FQDN=""
    local WD_BACKUP_SERVERS=""
    local WD_RAC_SERVER="gw2"  # Default RAC server
    
    echo "Detecting gateway configuration..."
    echo "  Hostname: $hostname"
    
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
    
    # Calculate port
    local client_ip_port=$((2200 + client_rac))
    echo "  Client Port: $client_ip_port"
    
    # Setup SSH for RAC access
    local client_user="pi"
    
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
    
    # Get client reporter ID
    local client_reporter_id=$(get_client_reporter_id "$client_rac" "$client_user" "$client_ip_port" "$WD_RAC_SERVER")
    if [[ -z "$client_reporter_id" ]]; then
        echo "ERROR: Could not determine client reporter ID"
        exit 1
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
            echo "WARNING: Failed to replicate to $backup_server (continuing anyway)"
        fi
    done
    
    # Diagnose and fix authorized_keys on all servers
    if ! diagnose_and_fix_authorized_keys "$client_rac" "$client_user" "$client_ip_port" "$sanitized_reporter_id" "$WD_RAC_SERVER" "${all_servers[@]}"; then
        echo "WARNING: Key diagnosis/repair had issues (continuing anyway)"
    fi
    
    # Test SFTP uploads to all servers
    if ! test_sftp_uploads "$sanitized_reporter_id" "${all_servers[@]}"; then
        echo "WARNING: Some SFTP tests failed (continuing with configuration)"
    fi
    
    # Configure client for multi-server access
    # ALWAYS configure for all servers regardless of test results
    if ! configure_client_access "$client_rac" "$client_user" "$client_ip_port" "$sanitized_reporter_id" "$WD_RAC_SERVER" "${all_servers[@]}"; then
        echo "ERROR: Failed to configure client access"
        exit 1
    fi
    
    echo ""
    echo "========================================="
    echo "✓ Client registration completed!"
    echo "========================================="
    echo ""
    echo "Reporter: $client_reporter_id"
    echo "Username: $sanitized_reporter_id"
    echo "Servers configured: ${#all_servers[@]}"
    echo ""
    
    return 0
}

# Run main function
main "$@"
