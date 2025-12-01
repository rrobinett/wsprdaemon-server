# wd-register-client.sh v2.8.0

## 🆕 NEW: RAC Connectivity Scanner
**Scan all RACs**: Check which clients are online/offline
```bash
./wd-register-client.sh --scan-racs
```
- Tests TCP connectivity to all RACs in .ssr.conf
- Shows active vs inactive clients
- Tests SSH access for each active RAC
- Provides summary statistics

## Quick Start
```bash
# Check version
./wd-register-client.sh --version

# Scan all RACs for connectivity
./wd-register-client.sh --scan-racs

# Register with auto-detected reporter ID
./wd-register-client.sh 84

# Register with manual reporter ID (if auto-detect fails)
./wd-register-client.sh 84 KJ6MKI

# Register with verbose output
./wd-register-client.sh 84 --verbose
```

## RAC Scanner Feature

The `--scan-racs` option checks connectivity to all RACs defined in ~/.ssr.conf:

```bash
./wd-register-client.sh --scan-racs
```

### Example Output:
```
=========================================
WSPRDAEMON RAC Connectivity Scanner
=========================================
Found 126 RAC entries in .ssr.conf
Testing from gateway: gw2

RAC | Port  | Status       | Client Info
----|-------|--------------|-------------------------------------------
  0 | 35800 | ✓ ACTIVE     | kfs (KFS WD-3 wd_client)
    └─ SSH: ✓ OK (user: wd_client)
 74 | 35874 | ✓ ACTIVE     | kv0s (KV0S)
    └─ SSH: ✓ OK (user: wsprdaemon)
 84 | 35884 | ✗ INACTIVE   | kj6mki-rz (KJ6MKI's Ryzen)
126 | 35926 | ✓ ACTIVE     | kd2om (KD2OM)
    └─ SSH: ✗ Failed (user: wsprdaemon)

=========================================
Summary:
=========================================
Total RACs tested: 126
Active RACs:       45
Inactive RACs:     81
```

### Features:
- Tests TCP port connectivity (port = 35800 + RAC)
- Tests SSH access for active RACs
- Shows client description from .ssr.conf
- Provides summary statistics
- Works from either GW1 or GW2

### Requirements:
- `nc` (netcat) must be installed: `sudo apt-get install netcat`
- ~/.ssr.conf must exist with FRPS_REMOTE_ACCESS_LIST array

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
- Script always displays "wd-register-client.sh version 2.8.0" when running
- Use `--version` or `-v` to show version only and exit
- Version is stored in script as `VERSION="2.8.0"`

## Files in wsprdaemon-server repository
- `wd-register-client.sh` - Main script (no version in filename)
- `wd-register-client-debug.sh` - Debug tool for extraction issues
- `wd-register-client-docs.md` - Full documentation
- `wd-register-client-debug-guide.md` - Troubleshooting guide
- `wd-register-client-README.md` - This file
- `wd-register-client-CHANGELOG.md` - Version history

## What's New in v2.8.0  
- **NEW**: `--scan-racs` option to check connectivity to all RACs
- **NEW**: Uses nc (netcat) to test TCP ports (35800 + RAC)
- **NEW**: Tests SSH access for active RACs
- **NEW**: Summary report shows active vs inactive clients
- **IMPROVED**: Warning comment in wsprdaemon.conf (v2.7.6)
- All fixes from v2.7.x series
