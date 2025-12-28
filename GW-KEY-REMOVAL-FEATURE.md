# GW Key Removal Feature - wd-register-client.sh

## What Was Added

The script now automatically removes stale GW1 and GW2 SSH host keys from client `known_hosts` files during registration. This prevents SSH connection failures when gateway servers are rebuilt with new host keys.

## Three Ways to Use It

### 1. Automatic Removal During Registration

When you run the normal registration command, the script now automatically:
- Backs up the client's `~/.ssh/known_hosts` file
- Removes entries for: `gw1`, `gw2`, `gw1.wsprdaemon.org`, `gw2.wsprdaemon.org`
- Continues with normal registration

```bash
# Normal registration - now includes GW key cleanup
./wd-register-client.sh 129
```

### 2. Standalone Clean Command (Single RAC)

You can now clean GW keys from a client without doing full registration:

```bash
# Just remove GW keys from RAC #129
./wd-register-client.sh clean-gw-keys 129

# With custom username
./wd-register-client.sh clean-gw-keys 129 myuser
```

### 3. Scan and Clean All RACs (NEW!)

Automatically scan all RACs 1-213, test which ones you can autologin to, and clean GW keys from those:

```bash
# Scan all RACs and clean where possible
./wd-register-client.sh scan-and-clean-all

# With custom username
./wd-register-client.sh scan-and-clean-all myuser
```

The scan-and-clean-all mode will:
- Test connection to each RAC port (35801-36013)
- Try to autologin via SSH
- If autologin succeeds, remove GW keys
- Create a timestamped log file with results
- Show summary of cleaned/failed/skipped RACs

## Scan and Clean All - Details

### Output Example

```
Scanning RACs 1-213 and cleaning GW keys where possible...
Log file: wd-gw-cleanup-20241207_153045.log

Started at Sat Dec 7 15:30:45 UTC 2024
======================================
RAC #1   [  1/213] SKIP - No connection
RAC #2   [  2/213] SKIP - No connection
RAC #3   [  3/213] CLEANED
RAC #4   [  4/213] SKIP - Can't autologin
...
RAC #129 [129/213] CLEANED
...

======================================
Scan completed at Sat Dec 7 15:45:22 UTC 2024

Summary:
  Cleaned: 45
  Failed:  2
  Skipped: 166
  Total:   213

RACs cleaned (45):
  RAC #3
  RAC #15
  RAC #129
  ...

Full log saved to: wd-gw-cleanup-20241207_153045.log
```

### Log File Format

The log file contains:
- Timestamp of when scan started/ended
- Status for each RAC (CLEANED, FAILED, SKIP)
- Summary statistics
- Complete list of cleaned RACs
- Complete list of failed RACs (if any)

### What Gets Skipped

RACs are skipped if:
- Port is not open (no connection to 35800+RAC)
- SSH autologin fails (no key-based authentication configured)
- These are normal and expected

### What Gets Cleaned

RACs are cleaned if:
- Port is open
- SSH autologin succeeds
- GW key removal completes successfully

### What Fails

RACs fail if:
- Connection works
- Autologin works
- But GW key removal returns an error

## When To Use Each Mode

### Automatic (during normal registration):
- Setting up a new client
- Re-registering an existing client

### Manual clean-gw-keys (single RAC):
- GW1 or GW2 has been rebuilt and you know which RAC needs cleaning
- Testing the cleanup on a specific client
- You want to clean up known_hosts without re-running full registration

### scan-and-clean-all:
- GW1 or GW2 has been rebuilt and you want to clean ALL accessible clients
- You don't know which RACs need cleaning
- Periodic maintenance to ensure all clients have clean known_hosts
- After infrastructure changes affecting multiple clients

## How It Works

The function `wd-remove-gw-keys-from-client()` runs these commands on the client:

```bash
# Backup known_hosts
cp ~/.ssh/known_hosts ~/.ssh/known_hosts.backup.$(date +%Y%m%d_%H%M%S)

# Remove entries for each gateway server
ssh-keygen -R gw1
ssh-keygen -R gw2
ssh-keygen -R gw1.wsprdaemon.org
ssh-keygen -R gw2.wsprdaemon.org
```

## Customization

To add/remove gateway servers from the cleanup list, edit this line in the script:

```bash
declare -a GW_SERVERS=("gw1" "gw2" "gw1.wsprdaemon.org" "gw2.wsprdaemon.org")
```

Example - add IP addresses:
```bash
declare -a GW_SERVERS=("gw1" "gw2" "gw1.wsprdaemon.org" "gw2.wsprdaemon.org" "192.168.1.10" "192.168.1.11")
```

To change the RAC range for scan-and-clean-all, edit line 473:
```bash
for client_rac in {1..213}; do
```

## Verbosity

The script respects the `verbosity` variable for individual operations:

```bash
# Run single cleanup with verbose output
verbosity=1 ./wd-register-client.sh clean-gw-keys 129
```

Note: The scan-and-clean-all mode always shows progress and summary, regardless of verbosity setting.

## Examples

### After GW Rebuild

When you rebuild GW1 or GW2:

```bash
# Clean all accessible RACs at once
./wd-register-client.sh scan-and-clean-all

# Review the log file
cat wd-gw-cleanup-*.log
```

### Periodic Maintenance

```bash
#!/bin/bash
# Weekly cleanup cron job
# Add to crontab: 0 2 * * 0 /path/to/weekly-gw-cleanup.sh

cd /home/wsprdaemon
./wd-register-client.sh scan-and-clean-all > /var/log/wd-gw-cleanup.log 2>&1
```

### Clean Specific Range

To clean only RACs 100-150:

```bash
#!/bin/bash
for rac in {100..150}; do
    echo "Checking RAC #${rac}"
    ./wd-register-client.sh clean-gw-keys ${rac} 2>/dev/null || true
done
```

## Error Handling

The function is designed to be safe:
- Backs up `known_hosts` before modifying
- Uses `|| true` to continue if a key doesn't exist
- Returns success even if some removals fail
- Shows warnings but doesn't fail the overall operation
- Scan mode continues even if individual RACs fail

## Performance

Scanning 213 RACs typically takes:
- ~5-10 minutes if most RACs are offline (quick port check fails)
- ~15-30 minutes if many RACs are online (SSH connection attempts)

The script uses:
- 2 second timeout for netcat port checks
- 5 second timeout for SSH connection attempts

## Testing

Test on a few RACs first:

```bash
# Test single RAC
./wd-register-client.sh clean-gw-keys 129

# Test small range manually
for rac in {125..130}; do
    ./wd-register-client.sh clean-gw-keys ${rac}
done

# Then run full scan
./wd-register-client.sh scan-and-clean-all
```
