#!/bin/bash
# Compare G3ZIL setup between GW1 and GW2
# This will show what's different

USERNAME="${1:-G3ZIL}"
echo "=== Comparing $USERNAME setup on GW1 vs GW2 ==="
echo ""

# Get info from both servers
echo "Collecting data from both servers..."
echo ""

# GW1 data
echo "GW1 Status:"
echo "-----------"
ssh gw1.wsprdaemon.org "
    echo -n 'Shadow password: '
    sudo getent shadow '$USERNAME' 2>/dev/null | cut -d: -f2
    echo -n 'Account status: '
    sudo passwd -S '$USERNAME' 2>/dev/null | awk '{print \$2}'
    echo -n 'Groups: '
    groups '$USERNAME' 2>/dev/null
    echo -n 'Home perms: '
    stat -c '%a %U:%G' /home/'$USERNAME' 2>/dev/null
    echo -n 'Uploads perms: '
    stat -c '%a %U:%G' /home/'$USERNAME'/uploads 2>/dev/null
    echo -n 'SSH dir perms: '
    stat -c '%a %U:%G' /home/'$USERNAME'/.ssh 2>/dev/null
    echo -n 'Auth keys: '
    if sudo test -f /home/'$USERNAME'/.ssh/authorized_keys; then
        echo 'exists'
        echo -n 'Key fingerprint: '
        sudo ssh-keygen -lf /home/'$USERNAME'/.ssh/authorized_keys 2>/dev/null | awk '{print \$2}'
    else
        echo 'missing'
    fi
" 2>/dev/null

echo ""
echo "GW2 Status:"
echo "-----------"
ssh gw2.wsprdaemon.org "
    echo -n 'Shadow password: '
    sudo getent shadow '$USERNAME' 2>/dev/null | cut -d: -f2
    echo -n 'Account status: '
    sudo passwd -S '$USERNAME' 2>/dev/null | awk '{print \$2}'
    echo -n 'Groups: '
    groups '$USERNAME' 2>/dev/null
    echo -n 'Home perms: '
    stat -c '%a %U:%G' /home/'$USERNAME' 2>/dev/null
    echo -n 'Uploads perms: '
    stat -c '%a %U:%G' /home/'$USERNAME'/uploads 2>/dev/null
    echo -n 'SSH dir perms: '
    stat -c '%a %U:%G' /home/'$USERNAME'/.ssh 2>/dev/null
    echo -n 'Auth keys: '
    if sudo test -f /home/'$USERNAME'/.ssh/authorized_keys; then
        echo 'exists'
        echo -n 'Key fingerprint: '
        sudo ssh-keygen -lf /home/'$USERNAME'/.ssh/authorized_keys 2>/dev/null | awk '{print \$2}'
    else
        echo 'missing'
    fi
" 2>/dev/null

echo ""
echo "=== Key Differences ==="
echo ""

# Check for specific issues
echo "Checking for problems..."

# Check if locked on GW2
gw2_shadow=$(ssh gw2.wsprdaemon.org "sudo getent shadow '$USERNAME' 2>/dev/null | cut -d: -f2" 2>/dev/null)
if [[ "$gw2_shadow" =~ ^! ]]; then
    echo "⚠️  PROBLEM: Account is LOCKED on GW2 (password starts with !)"
    echo "   Fix: Run ./wd-register-client-unlock-gw2.sh $USERNAME"
elif [[ "$gw2_shadow" == "*" ]]; then
    echo "✓ Account on GW2 has * password (SSH keys should work)"
else
    echo "? Account on GW2 has password: $gw2_shadow"
fi

# Check groups
if ! ssh gw2.wsprdaemon.org "groups '$USERNAME' 2>/dev/null | grep -q sftponly" 2>/dev/null; then
    echo "⚠️  PROBLEM: User not in sftponly group on GW2"
    echo "   Fix: ssh gw2.wsprdaemon.org 'sudo usermod -a -G sftponly $USERNAME'"
fi

# Check home directory ownership
gw2_home=$(ssh gw2.wsprdaemon.org "stat -c '%U' /home/'$USERNAME' 2>/dev/null" 2>/dev/null)
if [[ "$gw2_home" != "root" ]]; then
    echo "⚠️  PROBLEM: Home directory on GW2 not owned by root (owned by $gw2_home)"
    echo "   Fix: ssh gw2.wsprdaemon.org 'sudo chown root:root /home/$USERNAME'"
fi

echo ""
echo "=== Quick Fix Commands ==="
echo ""
echo "To fix common issues on GW2:"
echo "  ssh gw2.wsprdaemon.org"
echo "  sudo usermod -p '*' $USERNAME          # Unlock account"
echo "  sudo usermod -a -G sftponly $USERNAME  # Add to group"
echo "  sudo chown root:root /home/$USERNAME   # Fix home ownership"
echo "  sudo chmod 755 /home/$USERNAME         # Fix home permissions"
