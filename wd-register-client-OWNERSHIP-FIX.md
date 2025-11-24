# Directory Ownership Fix - v2.7.2

## The Problem Found
When comparing G3ZIL setup on GW1 vs GW2:

```
GW1 Status (WORKING):
Uploads perms: 755 G3ZIL:G3ZIL    ✓
SSH dir perms: 700 G3ZIL:G3ZIL    ✓

GW2 Status (FAILING):
Uploads perms: 755 root:root      ✗
SSH dir perms: 700 root:root      ✗
```

The user G3ZIL couldn't access their own directories on GW2!

## Why SFTP Failed
1. SSH daemon reads `/home/G3ZIL/.ssh/authorized_keys` to verify the key
2. But the file is owned by root:root
3. G3ZIL user can't read it → authentication fails
4. Even if auth worked, G3ZIL can't write to uploads (owned by root)

## The Fix in v2.7.2

### Local Server Setup
```bash
# Create directories
sudo mkdir -p "$ssh_dir"
sudo touch "$ssh_dir/authorized_keys"

# Set ownership FIRST (before restrictive permissions)
sudo chown "$user:$user" "$ssh_dir"
sudo chown "$user:$user" "$ssh_dir/authorized_keys"

# THEN set permissions
sudo chmod 700 "$ssh_dir"
sudo chmod 600 "$ssh_dir/authorized_keys"
```

### Remote Server Replication
The script now:
1. Checks BOTH permissions AND ownership
2. Sets ownership before setting restrictive permissions
3. Validates: `ssh_owner == "$user:$user"`

## Config File Cleanup
Also fixed in v2.7.2:
- Removes ALL `WD_SERVER_USER="..."` lines (deprecated)
- Only keeps `WD_SERVER_USER_LIST=(...)` for multi-server support

Before:
```bash
WD_SERVER_USER="G3ZIL@gw1.wsprdaemon.org"
WD_SERVER_USER_LIST=("G3ZIL@gw1.wsprdaemon.org" "G3ZIL@gw2.wsprdaemon.org")
```

After:
```bash
WD_SERVER_USER_LIST=("G3ZIL@gw1.wsprdaemon.org" "G3ZIL@gw2.wsprdaemon.org")
```

## Testing the Fix
After running v2.7.2:
```bash
# Check ownership on GW2
ssh gw2.wsprdaemon.org 'ls -ld /home/G3ZIL/{uploads,.ssh}'

# Should show:
drwxr-xr-x G3ZIL G3ZIL /home/G3ZIL/uploads
drwx------ G3ZIL G3ZIL /home/G3ZIL/.ssh

# Test SFTP from client
ssh -p 35906 wsprdaemon@gw2
sftp G3ZIL@gw2.wsprdaemon.org  # Should work now!
```

## Summary
The core issue was that directories were created with correct permissions (755, 700) but wrong ownership (root:root instead of user:user). The fix ensures ownership is set correctly and BEFORE restrictive permissions are applied.
