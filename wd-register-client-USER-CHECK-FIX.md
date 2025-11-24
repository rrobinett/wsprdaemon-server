# User Existence Check Fix - v2.7.4

## The Problem
When trying to create KV0S on GW2:
```
Creating user on gw2.wsprdaemon.org...
  Creating user 'KV0S' (server will assign UID/GID)...
useradd: group KV0S exists - if you want to add this user to that group, use -g.
ERROR: Failed to create user on gw2.wsprdaemon.org
ABORTING: Cannot continue without user account on all servers
```

## Why It Failed

### The Situation
1. Group `KV0S` existed on GW2 (from previous partial creation)
2. User `KV0S` did NOT exist on GW2
3. Script tried: `useradd KV0S` without specifying the group
4. useradd complained: "group KV0S exists - use -g"

### The Root Cause
- Script used `id` command to check if user exists
- `id` can fail in various ways
- Script thought user didn't exist when it should have checked more carefully

## The Fix in v2.7.4

### 1. Better User Existence Check
```bash
# OLD: Using id (less reliable)
if ssh "$server" "id 'KV0S' 2>/dev/null" >/dev/null; then

# NEW: Using getent passwd (more reliable)
if ssh "$server" "getent passwd 'KV0S' >/dev/null 2>&1"; then
```

### 2. Handle Existing Groups
```bash
# Check if group exists
group_exists=$(ssh "$server" "getent group 'KV0S' >/dev/null 2>&1 && echo 'yes'")

if [[ "$group_exists" == "yes" ]]; then
    # Use the existing group with -g flag
    useradd -m -g KV0S ... KV0S
else
    # Create new user (and group)
    useradd -m ... KV0S
fi
```

### 3. Better Debugging
When creation fails, now shows:
```
Debugging: Checking what exists on gw2.wsprdaemon.org...
  User check: KV0S:x:1025:1026:...  (or "User does not exist")
  Group check: KV0S:x:1026:...      (or "Group does not exist")
  Home dir: /home/KV0S              (or "No home directory")
```

## Manual Fix If Needed

If you have a partial user/group situation:

### Option 1: Complete the user creation
```bash
ssh gw2.wsprdaemon.org
sudo useradd -m -g KV0S -s /usr/sbin/nologin KV0S
```

### Option 2: Clean up and start fresh
```bash
ssh gw2.wsprdaemon.org
sudo userdel KV0S 2>/dev/null       # Remove user if exists
sudo groupdel KV0S 2>/dev/null      # Remove group if exists
sudo rm -rf /home/KV0S              # Remove home directory
# Then re-run the script
```

## Summary

- **Before v2.7.4**: Failed when group existed but user didn't
- **After v2.7.4**: Detects existing groups and uses them properly
- **Key insight**: Always check both user AND group existence separately!
