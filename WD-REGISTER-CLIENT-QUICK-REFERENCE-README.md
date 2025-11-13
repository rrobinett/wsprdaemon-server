# wd-register-client.sh Quick Reference

## Initial Setup

```bash
# 1. Create and configure the config file
sudo mkdir -p /etc/wsprdaemon
sudo cp client-register.conf.example /etc/wsprdaemon/client-register.conf
sudo vi /etc/wsprdaemon/client-register.conf

# 2. Edit these values:
#    WD_SERVER_FQDN="wd00.wsprdaemon.org"
#    WD_BACKUP_SERVERS="wd0.wsprdaemon.org wd1.wsprdaemon.org"

# 3. Test SSH to backup servers
for server in wd0.wsprdaemon.org wd1.wsprdaemon.org; do
    ssh "$server" "echo OK" && echo "$server: ✓" || echo "$server: ✗"
done
```

## Common Operations

### Register New Client (Automatic Mode)

```bash
# Standard registration via RAC
./wd-register-client.sh 129

# With custom username on client
./wd-register-client.sh 129 pi

# With verbose output for debugging
verbosity=2 ./wd-register-client.sh 129
```

### Register New Client (Manual Mode)

```bash
# When client emails you their public key
./wd-register-client.sh --manual KJ6MKI /tmp/client_pubkey.pub

# Then email them this line for their wsprdaemon.conf:
# (shown in output after running command above)
```

### Add Existing Users to New Server

```bash
# Single user
./wd-register-client.sh --replicate wd2.wsprdaemon.org KJ6MKI

# Multiple specific users
./wd-register-client.sh --replicate wd2.wsprdaemon.org KJ6MKI W3XYZ N6GN

# All non-system users
./wd-register-client.sh --replicate wd2.wsprdaemon.org \
    $(getent passwd | awk -F: '$3>=1000 && $3<65534 {print $1}')

# From a list file
./wd-register-client.sh --replicate wd2.wsprdaemon.org $(cat user_list.txt)
```

### Update Existing Client Config

```bash
# Re-run registration to update WD_SERVER_USER_LIST
# (adds new backup servers to client's config)
./wd-register-client.sh 129
```

## Verification & Testing

### Verify User on All Servers

```bash
username="KJ6MKI"

# Check user exists on all servers
for server in wd00.wsprdaemon.org wd0.wsprdaemon.org wd1.wsprdaemon.org; do
    ssh "$server" "id $username" 2>/dev/null && echo "$server: ✓" || echo "$server: ✗"
done

# Check authorized_keys on all servers
for server in wd00.wsprdaemon.org wd0.wsprdaemon.org wd1.wsprdaemon.org; do
    echo "=== $server ==="
    ssh "$server" "sudo cat /home/$username/.ssh/authorized_keys"
done

# Check uploads directory exists
for server in wd00.wsprdaemon.org wd0.wsprdaemon.org wd1.wsprdaemon.org; do
    ssh "$server" "ls -ld /home/$username/uploads" && echo "$server: ✓" || echo "$server: ✗"
done
```

### Test Client SFTP Access

```bash
username="KJ6MKI"

# Test from client (if you have SSH access)
sftp $username@wd00.wsprdaemon.org <<< "ls uploads"
sftp $username@wd0.wsprdaemon.org <<< "ls uploads"

# Test file upload
echo "test" > /tmp/test.txt
sftp $username@wd00.wsprdaemon.org <<< "put /tmp/test.txt uploads/"
```

### Verify Client Config

```bash
# SSH to client via RAC
ssh -p 35929 wsprdaemon@wd0 "grep WD_SERVER_USER_LIST ~/wsprdaemon/wsprdaemon.conf"

# Should output something like:
# WD_SERVER_USER_LIST=("KJ6MKI@wd00.wsprdaemon.org" "KJ6MKI@wd0.wsprdaemon.org")
```

## Maintenance

### List All Client Users

```bash
# All users in sftponly group
getent group sftponly | cut -d: -f4 | tr ',' '\n'

# With home directories
for user in $(getent group sftponly | cut -d: -f4 | tr ',' '\n'); do
    echo "$user: $(ls -ld /home/$user 2>/dev/null | awk '{print $1}')"
done
```

### Audit User Replication Status

```bash
# Check which users exist on which servers
for user in $(getent group sftponly | cut -d: -f4 | tr ',' '\n'); do
    echo -n "$user: "
    for server in wd00.wsprdaemon.org wd0.wsprdaemon.org wd1.wsprdaemon.org; do
        ssh "$server" "id $user" 2>/dev/null >/dev/null && echo -n "✓ $server " || echo -n "✗ $server "
    done
    echo
done
```

### Re-sync User Keys

```bash
# If a user's key changes, re-run replication
username="KJ6MKI"

# First update key on WD00 manually, then:
for server in wd0.wsprdaemon.org wd1.wsprdaemon.org; do
    ./wd-register-client.sh --replicate "$server" "$username"
done
```

## Troubleshooting

### Debug Mode

```bash
# Run with maximum verbosity
verbosity=2 ./wd-register-client.sh 129

# Check if config file is loaded
verbosity=1 ./wd-register-client.sh 129 | head -5
# Should show: "Loading configuration from /etc/wsprdaemon/client-register.conf"
```

### Connection Issues

```bash
# Test basic SSH connectivity
ssh wd0.wsprdaemon.org "echo 'SSH OK'"

# Test sudo access
ssh wd0.wsprdaemon.org "sudo id"

# Test with timeout
timeout 10 ssh wd0.wsprdaemon.org "echo 'OK'"
```

### Manual User Deletion

```bash
username="KJ6MKI"

# Delete from all servers
for server in wd00.wsprdaemon.org wd0.wsprdaemon.org wd1.wsprdaemon.org; do
    echo "Deleting from $server..."
    ssh "$server" "sudo userdel -r $username"
done
```

### Check sftponly Group on All Servers

```bash
for server in wd00.wsprdaemon.org wd0.wsprdaemon.org wd1.wsprdaemon.org; do
    echo "=== $server ==="
    ssh "$server" "getent group sftponly"
done
```

## Configuration Examples

### Minimal Config (Single Backup)

```bash
# /etc/wsprdaemon/client-register.conf
WD_RAC_SERVER="wd0"
WD_SERVER_FQDN="wd00.wsprdaemon.org"
WD_BACKUP_SERVERS="wd0.wsprdaemon.org"
```

### Full Config (Multiple Backups)

```bash
# /etc/wsprdaemon/client-register.conf
WD_RAC_SERVER="wd0"
WD_SERVER_FQDN="wd00.wsprdaemon.org"
WD_BACKUP_SERVERS="wd0.wsprdaemon.org wd1.wsprdaemon.org wd2.wsprdaemon.org"
WD_RAC_BASE_PORT=35800
SFTP_PORT=22
verbosity=1
```

### No Automatic Replication

```bash
# /etc/wsprdaemon/client-register.conf
WD_RAC_SERVER="wd0"
WD_SERVER_FQDN="wd00.wsprdaemon.org"
WD_BACKUP_SERVERS=""  # Empty = no automatic replication
```

## Batch Operations

### Replicate All Users to New Server

```bash
# Get list of all client users
users=$(getent group sftponly | cut -d: -f4 | tr ',' ' ')

# Replicate to new server
./wd-register-client.sh --replicate wd3.wsprdaemon.org $users
```

### Update All Existing Clients

```bash
# Re-run registration for all RACs to update their configs
# (adds new backup servers to their WD_SERVER_USER_LIST)
for rac in 129 130 131 132; do
    echo "Updating RAC $rac..."
    ./wd-register-client.sh "$rac"
done
```

### Generate Report of All Users

```bash
# Create CSV of users and their servers
echo "Username,WD00,WD0,WD1" > user_report.csv
for user in $(getent group sftponly | cut -d: -f4 | tr ',' '\n'); do
    wd00=$(ssh wd00.wsprdaemon.org "id $user" 2>/dev/null && echo "Yes" || echo "No")
    wd0=$(ssh wd0.wsprdaemon.org "id $user" 2>/dev/null && echo "Yes" || echo "No")
    wd1=$(ssh wd1.wsprdaemon.org "id $user" 2>/dev/null && echo "Yes" || echo "No")
    echo "$user,$wd00,$wd0,$wd1" >> user_report.csv
done
cat user_report.csv
```

## Emergency Procedures

### Rebuild Backup Server from Scratch

```bash
# 1. Install base OS on new server
# 2. Set up SSH keys for passwordless access
# 3. Replicate all users at once:

users=$(ssh wd00.wsprdaemon.org "getent group sftponly | cut -d: -f4 | tr ',' ' '")
./wd-register-client.sh --replicate new-server.wsprdaemon.org $users
```

### Remove Compromised Key

```bash
username="KJ6MKI"

# Delete authorized_keys on all servers
for server in wd00.wsprdaemon.org wd0.wsprdaemon.org wd1.wsprdaemon.org; do
    ssh "$server" "sudo rm /home/$username/.ssh/authorized_keys"
done

# Have client generate new key and re-register
./wd-register-client.sh --manual "$username" /tmp/new_pubkey.pub
```

## Handy Aliases

Add to your ~/.bashrc:

```bash
# List all WSPR client users
alias wspr-users='getent group sftponly | cut -d: -f4 | tr "," "\n"'

# Check user on all servers
wspr-check-user() {
    local user="$1"
    for server in wd00.wsprdaemon.org wd0.wsprdaemon.org wd1.wsprdaemon.org; do
        ssh "$server" "id $user" 2>/dev/null && echo "$server: ✓" || echo "$server: ✗"
    done
}

# Replicate user to all backup servers
wspr-replicate() {
    local user="$1"
    for server in wd0.wsprdaemon.org wd1.wsprdaemon.org; do
        ./wd-register-client.sh --replicate "$server" "$user"
    done
}
```

## Exit Codes

- `0` - Success
- `1` - Error (user creation, replication, or SSH failed)

## Environment Variables

- `verbosity` - Set to 1 or 2 for debug output
- All config file variables can be overridden via environment

Example:
```bash
WD_BACKUP_SERVERS="wd0.wsprdaemon.org" verbosity=2 ./wd-register-client.sh 129
```
