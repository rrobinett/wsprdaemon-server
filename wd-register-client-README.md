# wd-register-client.sh v2.7.0

## ⚠️ Critical Fix in v2.7.0
**Lock Detection Fixed**: Only unlocks accounts that are TRULY locked
- `*` password = NOT locked (SSH keys work) ✓
- `!` or `!!` password = LOCKED (needs unlocking) ✗
- Stops unnecessary account modifications

## Quick Start
```bash
# Check version
./wd-register-client.sh --version

# Register with auto-detected reporter ID
./wd-register-client.sh 84

# Register with manual reporter ID (if auto-detect fails)
./wd-register-client.sh 84 KJ6MKI

# Register with verbose output
./wd-register-client.sh 84 --verbose
```

## Port Calculation
The script connects to clients via SSH using port: **35800 + RAC number**
- RAC 84 → Port 35884
- RAC 85 → Port 35885
- etc.

## Required: .ssr.conf File
The script requires `~/.ssr.conf` containing a FRPS_REMOTE_ACCESS_LIST array:
```bash
declare FRPS_REMOTE_ACCESS_LIST=(
    "84,kj6mki-rz,wsprdaemon,AUTO,legacy,KJ6MKI site,-L ports"
    "85,other-id,sshuser,pass,legacy,Site desc,-L ports"
)
```
The script extracts the SSH username from field 3 (e.g., 'wsprdaemon').

## Troubleshooting Reporter ID Extraction

If you get "Could not determine client reporter ID", run the debug script:
```bash
./wd-register-client-debug.sh 84
```

Or specify the reporter ID manually:
```bash
./wd-register-client.sh 84 KJ6MKI
```

## Version Info
- Script always displays "wd-register-client.sh version 2.7.0" when running
- Use `--version` or `-v` to show version only and exit
- Version is stored in script as `VERSION="2.7.0"`

## Files in wsprdaemon-server repository
- `wd-register-client.sh` - Main script (no version in filename)
- `wd-register-client-debug.sh` - Debug tool for extraction issues
- `wd-register-client-docs.md` - Full documentation
- `wd-register-client-debug-guide.md` - Troubleshooting guide
- `wd-register-client-README.md` - This file
- `wd-register-client-CHANGELOG.md` - Version history

## What's New in v2.7.0  
- **FIXED**: Lock detection - only unlocks truly locked accounts (! or !!)
- **FIXED**: No longer unnecessarily modifies accounts with * password
- **IMPROVED**: Better status reporting explaining lock states
- **FIXED**: SFTP tests run from client perspective
- **FIXED**: Hostname detection case-insensitive
- **FIXED**: Correct server replication (no self-replication)
