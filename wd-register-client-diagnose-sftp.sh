#!/bin/bash
# Diagnostic script for SFTP failures to gw2
# Run this on GW1 or GW2 to check user G3ZIL setup

USERNAME="${1:-G3ZIL}"
echo "=== Diagnosing SFTP setup for user $USERNAME ==="
echo ""

# Function to check a server
check_server() {
    local server="$1"
    local is_local="$2"
    
    echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
    echo "Checking $server"
    echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
    
    if [[ "$is_local" == "yes" ]]; then
        # Local checks
        echo "1. User existence:"
        if id "$USERNAME" 2>/dev/null; then
            id "$USERNAME"
        else
            echo "   ✗ User does not exist!"
            return 1
        fi
        
        echo ""
        echo "2. Account status:"
        sudo passwd -S "$USERNAME" 2>/dev/null || echo "   Cannot check"
        
        echo ""
        echo "3. Shadow entry:"
        local shadow=$(sudo getent shadow "$USERNAME" | cut -d: -f2)
        echo "   Password field: $shadow"
        if [[ "$shadow" =~ ^! ]]; then
            echo "   ⚠ LOCKED with ! - SSH keys won't work!"
        elif [[ "$shadow" == "*" ]]; then
            echo "   ✓ Has * - SSH keys should work"
        else
            echo "   Status: $shadow"
        fi
        
        echo ""
        echo "4. Groups:"
        groups "$USERNAME" 2>/dev/null || echo "   Cannot check"
        
        echo ""
        echo "5. Home directory:"
        ls -ld "/home/$USERNAME" 2>/dev/null || echo "   Missing!"
        
        echo ""
        echo "6. Upload directory:"
        ls -ld "/home/$USERNAME/uploads" 2>/dev/null || echo "   Missing!"
        
        echo ""
        echo "7. SSH directory:"
        ls -ld "/home/$USERNAME/.ssh" 2>/dev/null || echo "   Missing!"
        
        echo ""
        echo "8. Authorized keys:"
        if sudo test -f "/home/$USERNAME/.ssh/authorized_keys"; then
            local size=$(sudo stat -c%s "/home/$USERNAME/.ssh/authorized_keys")
            echo "   File size: $size bytes"
            echo "   Permissions: $(sudo stat -c%a "/home/$USERNAME/.ssh/authorized_keys")"
            echo "   Owner: $(sudo stat -c%U:%G "/home/$USERNAME/.ssh/authorized_keys")"
            echo "   Fingerprint:"
            sudo ssh-keygen -lf "/home/$USERNAME/.ssh/authorized_keys" 2>/dev/null | sed 's/^/   /'
        else
            echo "   ✗ No authorized_keys file!"
        fi
        
        echo ""
        echo "9. SSHD config for sftponly group:"
        if grep -A5 "^Match Group sftponly" /etc/ssh/sshd_config 2>/dev/null; then
            echo "   ✓ Found sftponly configuration"
        else
            echo "   ✗ No sftponly configuration in sshd_config!"
        fi
        
    else
        # Remote checks via SSH
        echo "Running remote checks on $server..."
        ssh "$server" "
            echo '1. User existence:'
            id '$USERNAME' 2>/dev/null || echo '   ✗ User does not exist!'
            
            echo ''
            echo '2. Account status:'
            sudo passwd -S '$USERNAME' 2>/dev/null || echo '   Cannot check'
            
            echo ''
            echo '3. Shadow entry:'
            shadow=\$(sudo getent shadow '$USERNAME' | cut -d: -f2)
            echo \"   Password field: \$shadow\"
            if [[ \"\$shadow\" =~ ^! ]]; then
                echo '   ⚠ LOCKED with ! - SSH keys wont work!'
            elif [[ \"\$shadow\" == '*' ]]; then
                echo '   ✓ Has * - SSH keys should work'
            fi
            
            echo ''
            echo '4. Groups:'
            groups '$USERNAME' 2>/dev/null || echo '   Cannot check'
            
            echo ''
            echo '5. Home directory:'
            ls -ld '/home/$USERNAME' 2>/dev/null || echo '   Missing!'
            
            echo ''
            echo '6. Upload directory:'
            ls -ld '/home/$USERNAME/uploads' 2>/dev/null || echo '   Missing!'
            
            echo ''
            echo '7. Authorized keys:'
            if sudo test -f '/home/$USERNAME/.ssh/authorized_keys'; then
                echo '   ✓ File exists'
                echo -n '   Fingerprint: '
                sudo ssh-keygen -lf '/home/$USERNAME/.ssh/authorized_keys' 2>/dev/null | awk '{print \$2}'
            else
                echo '   ✗ No authorized_keys file!'
            fi
        " 2>/dev/null
    fi
}

# Determine which server we're on
HOSTNAME=$(hostname | tr '[:upper:]' '[:lower:]')
case "$HOSTNAME" in
    *gw1*)
        echo "Running on GW1"
        check_server "GW1 (local)" "yes"
        echo ""
        check_server "gw2.wsprdaemon.org" "no"
        ;;
    *gw2*)
        echo "Running on GW2"
        check_server "GW2 (local)" "yes"
        echo ""
        check_server "gw1.wsprdaemon.org" "no"
        ;;
    *)
        echo "Unknown server, checking both remotely"
        check_server "gw1.wsprdaemon.org" "no"
        echo ""
        check_server "gw2.wsprdaemon.org" "no"
        ;;
esac

echo ""
echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
echo "Testing SFTP connectivity"
echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"

# Test SFTP from current server (won't work without private key)
echo ""
echo "From this server (won't work without private key):"
for server in gw1.wsprdaemon.org gw2.wsprdaemon.org; do
    echo -n "  Testing $USERNAME@$server: "
    if timeout 5 sftp -o BatchMode=yes -o ConnectTimeout=3 "$USERNAME@$server" <<< "quit" >/dev/null 2>&1; then
        echo "✓ SUCCESS"
    else
        echo "✗ FAILED (expected without private key)"
    fi
done

echo ""
echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
echo "Manual test commands for CLIENT (RAC 106):"
echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
echo ""
echo "1. SSH to the client:"
echo "   ssh -p 35906 wsprdaemon@gw2"
echo ""
echo "2. From the client, test each server:"
echo "   # Test GW1"
echo "   sftp -v G3ZIL@gw1.wsprdaemon.org"
echo ""
echo "   # Test GW2"  
echo "   sftp -v G3ZIL@gw2.wsprdaemon.org"
echo ""
echo "3. If GW2 fails, check DNS from client:"
echo "   nslookup gw2.wsprdaemon.org"
echo "   ping -c 1 gw2.wsprdaemon.org"
echo ""
echo "4. Try IP instead of hostname:"
echo "   sftp G3ZIL@<GW2_IP_ADDRESS>"
echo ""

echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
echo "Common issues:"
echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
echo "1. Account locked with '!' password (not '*')"
echo "2. Missing sftponly group membership"
echo "3. Incorrect directory permissions"
echo "4. DNS resolution issues from client"
echo "5. Firewall blocking port 22"
echo "6. Different SSHD config on gw2"
