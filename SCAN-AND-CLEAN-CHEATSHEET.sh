#!/bin/bash
# Quick Reference: wd-register-client.sh scan-and-clean-all
# 
# This cheat sheet shows common usage patterns for the RAC scanning feature

# ============================================================================
# BASIC USAGE
# ============================================================================

# Scan all RACs 1-213 and clean GW keys where you can autologin
./wd-register-client.sh scan-and-clean-all

# With custom username (default is 'wsprdaemon')
./wd-register-client.sh scan-and-clean-all myuser


# ============================================================================
# WHAT IT DOES
# ============================================================================
# For each RAC from 1-213:
#   1. Tests if port 35800+RAC is open
#   2. If open, tries SSH autologin
#   3. If autologin works, removes GW1/GW2 keys from client's ~/.ssh/known_hosts
#   4. Logs results to timestamped file: wd-gw-cleanup-YYYYMMDD_HHMMSS.log


# ============================================================================
# TYPICAL SCENARIOS
# ============================================================================

# After rebuilding GW1 or GW2 with new SSH keys:
./wd-register-client.sh scan-and-clean-all
# This cleans all accessible clients so they can reconnect to gateways


# Periodic maintenance (recommended monthly or after infrastructure changes):
./wd-register-client.sh scan-and-clean-all | tee maintenance-$(date +%Y%m%d).log


# Run as cron job (weekly, Sunday 2am):
# 0 2 * * 0 cd /home/wsprdaemon && ./wd-register-client.sh scan-and-clean-all >> /var/log/wd-gw-weekly.log 2>&1


# ============================================================================
# READING THE OUTPUT
# ============================================================================

# Real-time output shows:
# RAC #129 [129/213] CLEANED        - Successfully removed GW keys
# RAC #130 [130/213] SKIP - No connection    - Port not open
# RAC #131 [131/213] SKIP - Can't autologin  - No SSH key configured
# RAC #132 [132/213] FAILED - Error cleaning - Unexpected error

# At the end you'll see:
# Summary:
#   Cleaned: 45    - Successfully cleaned
#   Failed:  2     - Had errors during cleaning
#   Skipped: 166   - No connection or can't autologin
#   Total:   213   - All RACs checked


# ============================================================================
# LOG FILE ANALYSIS
# ============================================================================

# Find most recent log file
ls -lt wd-gw-cleanup-*.log | head -1

# View summary only
tail -20 wd-gw-cleanup-*.log

# See all cleaned RACs
grep "CLEANED" wd-gw-cleanup-*.log

# See all failures
grep "FAILED" wd-gw-cleanup-*.log

# Count by status
grep -c "CLEANED" wd-gw-cleanup-*.log
grep -c "SKIP" wd-gw-cleanup-*.log
grep -c "FAILED" wd-gw-cleanup-*.log

# Get list of cleaned RAC numbers only
grep "RACs cleaned" -A 1000 wd-gw-cleanup-*.log | grep "RAC #" | awk '{print $3}' | tr -d '#'


# ============================================================================
# SINGLE RAC OPERATIONS (ALTERNATIVES)
# ============================================================================

# Clean just one RAC
./wd-register-client.sh clean-gw-keys 129

# Clean a specific range (e.g., RACs 100-110)
for rac in {100..110}; do
    ./wd-register-client.sh clean-gw-keys ${rac}
done

# Clean only RACs from your .ssr.conf
source ~/.ssr.conf
for rac_entry in "${FRPS_REMOTE_ACCESS_LIST[@]}"; do
    rac_id=$(echo "$rac_entry" | cut -d',' -f1)
    ./wd-register-client.sh clean-gw-keys ${rac_id}
done


# ============================================================================
# CUSTOMIZATION
# ============================================================================

# To change RAC range (edit the script, line ~473):
# for client_rac in {1..213}; do
# Change to:
# for client_rac in {1..300}; do    # Scan RACs 1-300
# for client_rac in {100..200}; do  # Scan only RACs 100-200

# To add more gateway servers (edit line ~172):
# declare -a GW_SERVERS=("gw1" "gw2" "gw1.wsprdaemon.org" "gw2.wsprdaemon.org")
# Add more:
# declare -a GW_SERVERS=("gw1" "gw2" "gw3" "gw1.wsprdaemon.org" "gw2.wsprdaemon.org" "gw3.wsprdaemon.org")


# ============================================================================
# PERFORMANCE NOTES
# ============================================================================

# Typical run times:
# - Most RACs offline: ~5-10 minutes (quick port checks fail fast)
# - Many RACs online: ~15-30 minutes (SSH connection attempts take time)

# Timeouts used:
# - Port check (nc): 2 seconds
# - SSH login: 5 seconds

# To speed up for specific ranges, use individual clean-gw-keys instead


# ============================================================================
# TROUBLESHOOTING
# ============================================================================

# If all RACs show "SKIP - Can't autologin":
# - Check you can manually SSH: ssh -p 35929 wsprdaemon@wd0
# - Verify SSH keys are set up properly
# - Try with explicit username: ./wd-register-client.sh scan-and-clean-all wsprdaemon

# If many show "FAILED - Error cleaning":
# - Check the log file for specific errors
# - Test one manually: ./wd-register-client.sh clean-gw-keys <RAC_NUMBER>
# - Check client's ~/.ssh permissions and disk space

# If it's too slow:
# - Clean only known active RACs instead of all 213
# - Use individual clean-gw-keys for specific RACs
# - Run during off-hours


# ============================================================================
# SAFETY NOTES
# ============================================================================

# This operation is safe because:
# - Only removes specific host keys (gw1, gw2)
# - Creates backup of known_hosts before modification
# - Doesn't disconnect existing SSH sessions
# - Won't affect clients you can't autologin to
# - Logs everything for audit trail


# ============================================================================
# EXAMPLE WORKFLOW AFTER GW REBUILD
# ============================================================================

# 1. GW1 or GW2 was rebuilt with new SSH keys
# 2. Clients can't connect, showing "REMOTE HOST IDENTIFICATION HAS CHANGED"
# 3. Run the scan:
./wd-register-client.sh scan-and-clean-all

# 4. Review results:
tail -30 wd-gw-cleanup-*.log

# 5. Test a few cleaned clients:
ssh -p 35929 wsprdaemon@wd0 "ssh gw1.wsprdaemon.org 'echo Success'"

# 6. If any failed, investigate:
grep FAILED wd-gw-cleanup-*.log
# Then manually clean those:
./wd-register-client.sh clean-gw-keys <FAILED_RAC_NUMBER>
