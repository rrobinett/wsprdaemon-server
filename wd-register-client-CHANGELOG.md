# wd-register-client.sh Changelog

## v2.7.4 - User Existence Check Fix (November 24, 2025)
### Fixed
- **Proper user existence check**:
  - Uses `getent passwd` instead of `id` (more reliable)
  - Correctly detects if user exists vs doesn't exist
- **Handle existing groups**:
  - If group exists but user doesn't, use `-g` flag
  - Fixes "group KV0S exists" error
- **Better debugging**:
  - Shows what exists when creation fails
  - Checks user, group, and home directory

### Example of the fix:
```
# Before: Failed with "group KV0S exists"
# After: Detects group exists, uses it: useradd -g KV0S ...
```

## v2.7.3 - No UID Matching & Proper Error Handling (November 24, 2025)
### Critical Fixes
- **No UID/GID matching between servers**:
  - Was trying to force same UID/GID on all servers (causes conflicts)
  - Now lets each server assign its own UID/GID
  - Only usernames need to match for SFTP to work
- **Abort on critical failures**:
  - Script now STOPS if user creation fails
  - No more false "success" messages when things fail
  - Clear error messages explaining what went wrong

### Why UID Matching Failed
When KV0S had UID 1019 on GW1, but UID 1019 was already used on GW2 (by G3ZIL),
the script failed to create the user but continued anyway. Now it:
1. Doesn't force UID matching
2. Stops immediately on failure
3. Provides manual fix instructions

## v2.7.2 - Directory Ownership Fix & Config Cleanup (November 24, 2025)
### Fixed
- **CRITICAL: Directory ownership on remote servers**:
  - `.ssh` and `uploads` directories must be owned by the user, not root
  - This was causing SFTP failures on GW2
  - Now sets ownership BEFORE setting restrictive permissions
  - Checks both permissions AND ownership when validating

### Changed
- **Client configuration cleanup**:
  - Removes ALL `WD_SERVER_USER=` lines (deprecated)
  - Only uses `WD_SERVER_USER_LIST=()` for multi-server support
  - Cleaner wsprdaemon.conf with no duplicate entries

### Example of the fix:
```
# Before (on GW2):
/home/G3ZIL/uploads owner: root:root  ✗
/home/G3ZIL/.ssh owner: root:root     ✗

# After (on GW2):
/home/G3ZIL/uploads owner: G3ZIL:G3ZIL ✓
/home/G3ZIL/.ssh owner: G3ZIL:G3ZIL    ✓
```

## v2.7.1 - Group Creation & Connection Handling (November 23, 2025)
### Fixed
- **Group creation on remote servers**:
  - Creates group with matching GID before creating user
  - Handles cases where GID already exists
  - Fixes "invalid user" errors during replication
- **Client connection failures**:
  - Gracefully handles unreachable clients
  - Shows manual configuration instructions
  - Continues with partial success
  - Better timeout and error messages

### Improved
- Shows what succeeded vs what needs manual intervention
- Provides exact configuration lines for manual setup
- Better error messages explaining connection failures

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
