# wd-register-client.sh Changelog

## v2.7.0 - Correct Lock Detection (November 23, 2025)
### Fixed
- **Account lock detection now works correctly**:
  - Only considers accounts with `!` or `!!` as truly locked
  - Accounts with `*` password are NOT locked (SSH keys work)
  - Prevents unnecessary `usermod` commands
- **Better status reporting**:
  - Shows when account has 'L' status but SSH keys work
  - Explains why no action is needed

### Technical Details
- `passwd -S` shows 'L' for both `*` and `!` passwords
- Only `!` or `!!` actually blocks SSH key authentication
- Script now checks the shadow file directly

## v2.6.9 - SFTP Test Fix (November 23, 2025)
### Fixed
- **SFTP tests now run from CLIENT perspective**:
  - SSHs to the client first
  - Runs SFTP from client to servers
  - This is the correct way to test
- **Better failure handling**:
  - Explains why tests might fail
  - Notes that client may not have keys yet
  - Always continues configuration

### Why the Change
The old test was backwards - it tried to SFTP FROM the server TO the server as KJ6MKI, 
but the server doesn't have KJ6MKI's private key. Now it SSHs to the client and tests 
from there, which is how the actual uploads will work.

## v2.6.8 - Hostname Detection & SFTP Test Fixes (November 23, 2025)
### Fixed
- **Hostname detection**: Now case-insensitive (properly recognizes GW2, gw2, Gw2, etc.)
- **Correct server replication**: When on GW2, replicates to GW1 (not itself)
- **SFTP test improvements**:
  - Better diagnostics showing local vs remote tests
  - Unique test filenames with timestamp
  - Alternate verification methods
  - Always continues configuration even if tests fail
- **Output clarity**: Shows all configured servers in final output

### Improved
- More detailed SFTP test output for debugging
- Better error messages when tests fail
- Clearer indication of which server is being tested

## v2.6.7 - Account Unlocking & Smart Configuration (November 23, 2025)
### Fixed
- **CRITICAL**: Checks and unlocks locked accounts on all servers
  - Uses `passwd -S` to check if account is locked (status 'L')
  - Unlocks with `usermod -p '*'` to enable key-only auth
  - Fixes "User KJ6MKI not allowed because account is locked" errors
- **IMPROVED**: Only configures groups/directories when needed
  - Checks if user already in sftponly group
  - Verifies directory existence and permissions
  - Only creates/fixes what's necessary

### Example Output
```
✓ User 'KJ6MKI' already exists
  Account is LOCKED - unlocking...
  ✓ Account unlocked (key-only authentication enabled)
✓ User already in 'sftponly' group
✓ Directories already configured correctly
```

## v2.6.6 - Correct Field Number (November 23, 2025) 
### Fixed
- **CRITICAL**: SSH username is field 3, not field 4!
- Correct format: "RAC,wd_user,ssh_user,ssh_pass,legacy,description,forwards"
- Example: "84,kj6mki-rz,wsprdaemon,AUTO,..." extracts "wsprdaemon" from field 3

## v2.6.5 - Correct .ssr.conf Parsing (November 23, 2025)
### Fixed
- **CRITICAL**: Now correctly parses .ssr.conf FRPS_REMOTE_ACCESS_LIST array
- Sources the file and iterates through bash array entries
- Extracts SSH username from field 4 (comma-separated)
- Format: "RAC,wd_user,wd_pass,ssh_user ssh_pass,description,forwards"

## v2.6.4 - Client Username Fix (November 23, 2025)
### Fixed
- **CRITICAL**: Client username now extracted from .ssr.conf file
  - Was: Hardcoded to 'pi' 
  - Now: Reads from ~/.ssr.conf entry for the RAC
- Format expected in .ssr.conf: `RAC user@host:port`
- Example: `84 wsprdaemon@host:35884` → extracts 'wsprdaemon'

## v2.6.3 - Critical Port Fix (November 23, 2025)
### Fixed
- **CRITICAL**: Port calculation was using wrong formula
  - Was: 2200 + RAC (e.g., RAC 84 = port 2284)  
  - Now: 35800 + RAC (e.g., RAC 84 = port 35884)
- This prevented all SSH connections to clients

## v2.6.2 - Enhanced Debugging (November 23, 2025)
### Added
- Manual reporter ID override: `./wd-register-client.sh 84 KJ6MKI`
- Debug script: `wd-register-client-debug.sh` for diagnosing extraction issues
- SSH connection test before attempting extraction
- File existence checks showing exactly which files were found/missing

## v2.6.1 - Bug Fix (November 23, 2025)
### Fixed
- Diagnostic messages from get_client_reporter_id were being captured as return value
- All echo statements in function now use `>&2` to send to stderr

## v2.6.0 - New Reporter ID Extraction (November 23, 2025)
### Changed
- Extracts client_reporter_id from log file instead of CSV
- Falls back to CSV method if log fails
- Added `--version` argument support

## v2.5.0 - Major Fixes (November 22, 2025)
### Fixed
1. Password Lock Issue: Only lock passwords for NEW users
2. Authorized Keys Mismatch: Compare and auto-repair mismatched keys
3. Partial Configuration: Always configure client for BOTH servers

## Quick Usage
```bash
# Basic registration
./wd-register-client.sh 84

# With verbose output
./wd-register-client.sh 84 -v
```

## Files Available
- `wd-register-client-v2.6.0.sh` - Main script
- `wd-register-client-v2.6.0-docs.md` - Full documentation
- `wd-register-client-debug-guide.md` - Troubleshooting guide
- `CHANGELOG-v2.6.0.md` - This file
