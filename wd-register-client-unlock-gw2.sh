#!/bin/bash
# Quick fix script to ensure user is properly unlocked on GW2
# Usage: ./unlock-user-gw2.sh G3ZIL

USERNAME="${1:-G3ZIL}"

echo "=== Ensuring user $USERNAME is unlocked on GW2 ==="
echo ""

# Function to unlock on a server
unlock_on_server() {
    local server="$1"
    echo "Checking $server..."
    
    ssh "$server" "
        # Check current status
        echo -n '  Current shadow entry: '
        sudo getent shadow '$USERNAME' | cut -d: -f2
        
        # Unlock the account properly
        echo '  Setting password to * (allows SSH keys)...'
        sudo usermod -p '*' '$USERNAME'
        
        # Verify
        echo -n '  New shadow entry: '
        sudo getent shadow '$USERNAME' | cut -d: -f2
        
        # Check passwd status
        echo -n '  passwd -S status: '
        sudo passwd -S '$USERNAME' | awk '{print \$2}'
        
        # Ensure in sftponly group
        echo '  Ensuring sftponly group membership...'
        sudo usermod -a -G sftponly '$USERNAME'
        
        # Show groups
        echo -n '  Groups: '
        groups '$USERNAME'
        
        # Fix permissions just in case
        echo '  Fixing permissions...'
        sudo chown root:root /home/'$USERNAME'
        sudo chmod 755 /home/'$USERNAME'
        sudo chown -R '$USERNAME':'$USERNAME' /home/'$USERNAME'/.ssh /home/'$USERNAME'/uploads
        sudo chmod 700 /home/'$USERNAME'/.ssh
        sudo chmod 600 /home/'$USERNAME'/.ssh/authorized_keys
        sudo chmod 755 /home/'$USERNAME'/uploads
        
        echo '  ✓ Done on $server'
    " 2>/dev/null
}

# Check which server we're on
HOSTNAME=$(hostname | tr '[:upper:]' '[:lower:]')
if [[ "$HOSTNAME" == *"gw1"* ]]; then
    echo "Running from GW1, fixing GW2..."
    unlock_on_server "gw2.wsprdaemon.org"
elif [[ "$HOSTNAME" == *"gw2"* ]]; then
    echo "Running on GW2, fixing locally..."
    
    # Local fix
    echo -n "  Current shadow entry: "
    sudo getent shadow "$USERNAME" | cut -d: -f2
    
    echo "  Setting password to * (allows SSH keys)..."
    sudo usermod -p '*' "$USERNAME"
    
    echo -n "  New shadow entry: "
    sudo getent shadow "$USERNAME" | cut -d: -f2
    
    echo -n "  passwd -S status: "
    sudo passwd -S "$USERNAME" | awk '{print $2}'
    
    echo "  Ensuring sftponly group membership..."
    sudo usermod -a -G sftponly "$USERNAME"
    
    echo -n "  Groups: "
    groups "$USERNAME"
    
    echo "  Fixing permissions..."
    sudo chown root:root "/home/$USERNAME"
    sudo chmod 755 "/home/$USERNAME"
    sudo chown -R "$USERNAME:$USERNAME" "/home/$USERNAME/.ssh" "/home/$USERNAME/uploads"
    sudo chmod 700 "/home/$USERNAME/.ssh"
    sudo chmod 600 "/home/$USERNAME/.ssh/authorized_keys"
    sudo chmod 755 "/home/$USERNAME/uploads"
    
    echo "  ✓ Done on GW2 (local)"
    
    echo ""
    echo "Also checking GW1..."
    unlock_on_server "gw1.wsprdaemon.org"
else
    echo "Unknown hostname, please run on GW1 or GW2"
    exit 1
fi

echo ""
echo "=== Testing from client perspective ==="
echo "SSH to client and test:"
echo "  ssh -p 35906 wsprdaemon@gw2"
echo "  sftp G3ZIL@gw2.wsprdaemon.org"
echo ""
echo "Or test the upload directly:"
echo '  echo "test" > /tmp/test.txt'
echo "  sftp G3ZIL@gw2.wsprdaemon.org:/uploads/ <<< 'put /tmp/test.txt'"
