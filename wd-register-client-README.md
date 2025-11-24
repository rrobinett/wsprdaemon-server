# wd-register-client.sh v2.7.4

## ⚠️ Critical Fix in v2.7.4
**User Detection Fixed**: No longer fails when user should exist
- Properly checks if user exists (uses `getent passwd`)
- Handles case where group exists but user doesn't
- Better debugging when things go wrong

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
- Script always displays "wd-register-client.sh version 2.7.4" when running
- Use `--version` or `-v` to show version only and exit
- Version is stored in script as `VERSION="2.7.4"`

## Files in wsprdaemon-server repository
- `wd-register-client.sh` - Main script (no version in filename)
- `wd-register-client-debug.sh` - Debug tool for extraction issues
- `wd-register-client-docs.md` - Full documentation
- `wd-register-client-debug-guide.md` - Troubleshooting guide
- `wd-register-client-README.md` - This file
- `wd-register-client-CHANGELOG.md` - Version history

## What's New in v2.7.4  
- **FIXED**: Proper user existence check (uses getent passwd)
- **FIXED**: Handles existing groups correctly (uses -g flag)
- **IMPROVED**: Better debugging output
- **FIXED**: No UID/GID matching between servers
- **FIXED**: Script aborts on critical failures
- All fixes from v2.7.3 and earlier versions
