# wd-register-client.sh v2.6.0 Documentation

## Overview
The `wd-register-client.sh` script automates the registration and configuration of WSPRDAEMON client stations for uploading data to the gateway servers (gw1 and gw2, formerly known as wd00 and wd0).

## Version History

### v2.6.0 (November 2025)
- **NEW**: Extract client_reporter_id from upload_to_wsprnet_daemon.log instead of CSV database
- Continued refinement of authentication diagnostics

### v2.5.0 (November 22, 2025) 
- **MAJOR FIX**: Only lock passwords for NEW users, not existing ones
- **MAJOR FIX**: Compare and fix authorized_keys fingerprints before declaring failure
- **MAJOR FIX**: Always configure client for BOTH servers, even if one fails tests
- Added comprehensive SSH key fingerprint diagnostics
- Auto-repair of mismatched authorized_keys

### v2.4.0
- Direct key installation from client
- Improved diagnostics for authentication issues

### v2.3.0
- Enhanced SSHD configuration checking
- Better error messages for missing configurations

### v2.2.0
- Fixed chroot directory permissions
- Added automatic SSHD config validation

### v2.1.0
- Fixed SFTP testing to test BOTH servers directly
- Ensured gw1 always comes first in WD_SERVER_USER_LIST
- Fixed typos (authorized_key → authorized_keys)

### v2.0.0
- **BREAKING**: Complete migration from wd00/wd0 to gw1/gw2
- Added automatic gateway detection
- Version numbering system introduced

## Key Features

### 1. Client Reporter ID Extraction
The script now extracts the client's callsign from their most recent log entry:

```bash
# Old method (from CSV database):
client_reporter_id=$(ssh ... "tail -1 ~/wsprdaemon/spots.csv | cut -d',' -f7")

# New method (from log file):
client_reporter_id=$(ssh ... "grep 'my call' ~/wsprdaemon/uploads/wsprnet/spots/upload_to_wsprnet_daemon.log | tail -1 | sed -n 's/.*my call \([^ ]*\) and\/or.*/\1/p'")
```

This extracts callsigns like "G3ZIL" from log lines like:
```
Thu 20 Nov 2025 17:24:35 UTC: upload_to_wsprnet_daemon() Dropping my call G3ZIL and/or spot lines which match regex ''
```

### 2. Special Character Handling
Automatically sanitizes reporter IDs with invalid characters for Linux usernames:
- `KFS/SW` → `KFS_SW`
- Replaces: `/`, `.`, `-`, `@` with underscores

### 3. Smart User Management
- **NEW USERS**: Creates account with disabled password (`usermod -p '*'`)
- **EXISTING USERS**: Skips password modification entirely
- Never uses `passwd -l` which breaks key authentication

### 4. SSH Key Diagnostics & Auto-Repair

The script performs comprehensive key verification:

```
Diagnosing authorized_keys on all servers...
  1. Getting client's public key from RAC server...
     Client's key fingerprint: SHA256:TGwLoZQD7Bw+z...
  
  2. Checking gw2.wsprdaemon.org...
     ✗ KEY MISMATCH on gw2.wsprdaemon.org!
        Client fingerprint:  SHA256:TGwLoZQD7Bw+z...
        Server fingerprint:  SHA256:different123...
     Replacing with client's key...
     ✓ Replaced with client's key on gw2.wsprdaemon.org
  
  3. Checking gw1.wsprdaemon.org...
     ✓ Key matches on gw1.wsprdaemon.org
```

### 5. Dual Gateway Support
- Automatically detects which gateway it's running on
- Replicates accounts to peer gateway
- Tests SFTP to BOTH gateways independently
- Configures client with both servers regardless of test results

## Usage

### Basic Usage
```bash
# Register client on RAC 84
./wd-register-client.sh 84
```

### With Verbosity
```bash
# Verbose output for debugging
./wd-register-client.sh 84 -v
```

## Configuration

### Automatic Detection
The script automatically configures based on hostname:
- Running on `gw1`: Primary=gw1, Backup=gw2
- Running on `gw2`: Primary=gw2, Backup=gw1  
- Other hostnames: Primary=gw1, Backup=gw2

### Manual Override
Create `client-register.conf` to override defaults:

```bash
# Primary WSPRDAEMON server
WD_SERVER_FQDN="gw1.wsprdaemon.org"

# Backup servers for replication
WD_BACKUP_SERVERS="gw2.wsprdaemon.org"

# RAC server hostname
WD_RAC_SERVER="gw2"
```

## Process Flow

1. **Extract Client Info**
   - Connect to RAC server via port forwarding
   - Extract reporter_id from log file
   - Sanitize for Linux username compatibility

2. **Create/Update Local User**
   - Check if user exists
   - Create with SFTP-only restrictions if new
   - Skip password operations for existing users

3. **Setup Chroot Environment**
   - Create upload directories
   - Set proper permissions (755)
   - Configure for internal-sftp

4. **Key Management**
   - Get client's public key from RAC
   - Compare fingerprints on all servers
   - Auto-repair mismatched keys
   - Ensure consistent authorization

5. **Replicate to Peer Servers**
   - Copy user structure to backup gateways
   - Maintain consistent UIDs/GIDs
   - Replicate SSH keys

6. **Test Connectivity**
   - Test SFTP uploads to ALL servers
   - Report success/failure for each
   - Continue even if some fail

7. **Configure Client**
   - Write WD_SERVER_USER_LIST with ALL servers
   - Primary (gw1) always listed first
   - Client can attempt both regardless of test results

## Troubleshooting

### Common Issues

#### 1. Authentication Failures
```
Permission denied (publickey).
```
**Solution**: Script now auto-detects and fixes key mismatches. Re-run the script.

#### 2. Memory Errors on GW2
```
-bash: fork: Cannot allocate memory
```
**Solution**: Add swap space to server with limited RAM.

#### 3. User Already Exists
```
useradd: user 'KJ6MKI' already exists
```
**Solution**: Script now handles existing users gracefully, updating only necessary components.

#### 4. SFTP Test Failures
```
ERROR: SFTP test to gw2.wsprdaemon.org failed
```
**Solution**: Script still configures client for both servers. User can manually test later.

### Debug Mode
Enable verbose output to see detailed operations:
```bash
./wd-register-client.sh 84 -v
```

This shows:
- Exact commands being executed
- SSH key fingerprints
- File operations
- Network tests

## Server Architecture

```
┌─────────────┐      ┌─────────────┐
│    GW1      │◄────►│    GW2      │
│(Primary)    │      │(Backup)     │
│             │      │             │
│ Port 22     │      │ Port 22     │
└──────▲──────┘      └──────▲──────┘
       │                     │
       └──────────┬──────────┘
                  │
           ┌──────▼──────┐
           │  RAC Server │
           │  Port 22xx  │
           │             │
           │  Client 84  │
           └─────────────┘
```

## Files Created/Modified

### On Gateway Servers
```
/home/KJ6MKI/                    # User home directory
├── .ssh/
│   └── authorized_keys          # Client's public key
├── uploads/                     # SFTP upload directory
│   └── .wd_upload_test_XXXXX   # Test file (temporary)
└── [chroot environment]
```

### On Client (RAC)
```
~/wsprdaemon/wsprdaemon.conf
  WD_SERVER_USER="KJ6MKI@gw1.wsprdaemon.org"  # Backward compatibility
  WD_SERVER_USER_LIST=("KJ6MKI@gw1.wsprdaemon.org" "KJ6MKI@gw2.wsprdaemon.org")
```

## Security Considerations

1. **SFTP-Only Access**: Users are restricted to SFTP with chroot
2. **No Shell Access**: `/usr/sbin/nologin` prevents interactive login
3. **Key-Only Authentication**: Password authentication disabled
4. **Chroot Isolation**: Users confined to their home directory
5. **Minimal Permissions**: Only upload directory is writable

## Maintenance

### Verify User Setup
```bash
# Check user exists and has correct shell
getent passwd KJ6MKI

# Verify group membership
groups KJ6MKI

# Check authorized_keys
sudo cat /home/KJ6MKI/.ssh/authorized_keys

# Test SFTP access
sftp KJ6MKI@gw1.wsprdaemon.org
```

### Remove User
```bash
# On each gateway
sudo userdel -r KJ6MKI
```

### Update Keys
Re-run the script - it will detect mismatches and update automatically.

## Support

For issues or questions about the WSPRDAEMON infrastructure:
- Check the latest version of this script
- Review system logs: `/var/log/auth.log`
- Enable verbose mode for detailed diagnostics

---
*Generated for wd-register-client.sh v2.6.0*  
*Part of the WSPRDAEMON infrastructure managed by AI6VN*
