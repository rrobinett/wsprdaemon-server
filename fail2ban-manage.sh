#!/bin/bash
# fail2ban-manage.sh - Comprehensive fail2ban management tool
# Version: 1.3
# Includes permanent unban with database cleanup, username display, maxretry config,
# and CIDR consolidation for optimizing iptables rules

VERSION="1.3"

# Database location
DB_FILE="/var/lib/fail2ban/fail2ban.sqlite3"
MAXRETRY_FILE="/etc/fail2ban/maxretry-invalidusers"

# Function to check if running with sudo
check_sudo() {
    if [ "$EUID" -ne 0 ]; then 
        echo "This command requires sudo privileges"
        exit 1
    fi
}

# Function to get all active jails
get_jails() {
    fail2ban-client status 2>/dev/null | grep "Jail list" | sed 's/.*://;s/,/ /g'
}

# Function to get failed username(s) for an IP from journal
get_failed_users_for_ip() {
    local ip=$1
    # Search journal for failed attempts from this IP, extract usernames
    journalctl -u ssh.service --no-pager -g "$ip" 2>/dev/null | \
        grep -oE "(Invalid user [^ ]+ from|Failed password for [^ ]+ from)" | \
        sed -E 's/Invalid user ([^ ]+) from/\1/; s/Failed password for ([^ ]+) from/\1/' | \
        sort -u | tr '\n' ',' | sed 's/,$//'
}

# Function to get current maxretry setting
get_maxretry() {
    if [ -f "$MAXRETRY_FILE" ]; then
        cat "$MAXRETRY_FILE"
    else
        # Try to read from config
        grep -h "^maxretry" /etc/fail2ban/jail.d/permanent-ban-invalidusers.conf 2>/dev/null | head -1 | awk '{print $3}'
    fi
}

case "$1" in
    version)
        echo "fail2ban-manage version $VERSION"
        ;;
    
    status)
        check_sudo
        echo "=== Fail2ban Status ==="
        echo "Service: $(systemctl is-active fail2ban)"
        echo "Uptime: $(systemctl show fail2ban -p ActiveEnterTimestamp --value)"
        
        # Show maxretry setting
        maxretry=$(get_maxretry)
        if [ ! -z "$maxretry" ]; then
            echo "Invalid user/root ban threshold: $maxretry failed attempts"
        fi
        echo ""
        fail2ban-client status
        echo ""
        
        for jail in $(get_jails); do
            echo "[$jail]"
            status=$(fail2ban-client status "$jail" 2>/dev/null)
            echo "$status" | grep -E "Currently failed|Total failed|Currently banned|Total banned" | sed 's/^/  /'
            bans=$(echo "$status" | grep "Banned IP list:" | sed 's/.*Banned IP list://')
            if [ ! -z "$bans" ] && [ "$bans" != "" ]; then
                echo "  Banned IPs:$bans"
            fi
            echo ""
        done
        ;;
    
    banned)
        check_sudo
        echo "=== All Banned IPs ==="
        echo "(Format: IP - attempted usernames)"
        echo ""
        total=0
        for jail in $(get_jails); do
            bans=$(fail2ban-client status "$jail" 2>/dev/null | grep "Banned IP list:" | sed 's/.*Banned IP list://')
            if [ ! -z "$bans" ] && [ "$bans" != "" ]; then
                echo "[$jail]"
                for ip in $bans; do
                    users=$(get_failed_users_for_ip "$ip")
                    if [ -z "$users" ]; then
                        echo "  $ip"
                    else
                        echo "  $ip ($users)"
                    fi
                    total=$((total + 1))
                done
                echo ""
            fi
        done
        echo "Total banned IPs: $total"
        ;;
    
    unban)
        check_sudo
        if [ -z "$2" ]; then
            echo "Usage: $0 unban <ip> [jail]"
            echo "   or: $0 unban all  (unban all IPs)"
            exit 1
        fi
        
        if [ "$2" = "all" ]; then
            echo "Unbanning all IPs from all jails..."
            for jail in $(get_jails); do
                ips=$(fail2ban-client status "$jail" 2>/dev/null | grep "Banned IP list:" | sed 's/.*Banned IP list://;s/\s\+/\n/g')
                for ip in $ips; do
                    [ ! -z "$ip" ] && fail2ban-client set "$jail" unbanip "$ip" 2>/dev/null && \
                        echo "  ✓ Unbanned $ip from $jail"
                done
            done
            if [ -f "$DB_FILE" ]; then
                count=$(sqlite3 "$DB_FILE" "SELECT COUNT(*) FROM bans;" 2>/dev/null)
                sqlite3 "$DB_FILE" "DELETE FROM bans;" 2>/dev/null
                echo "  ✓ Removed $count entries from database"
            fi
        else
            ip="$2"
            jail="$3"
            
            echo "Unbanning $ip..."
            
            # Remove from specific jail or all jails
            if [ ! -z "$jail" ]; then
                fail2ban-client set "$jail" unbanip "$ip" 2>/dev/null && \
                    echo "  ✓ Removed from $jail"
            else
                for j in $(get_jails); do
                    fail2ban-client set "$j" unbanip "$ip" 2>/dev/null && \
                        echo "  ✓ Removed from $j"
                done
            fi
            
            # Remove from database - CRITICAL for permanent unbans
            if [ -f "$DB_FILE" ]; then
                count=$(sqlite3 "$DB_FILE" "SELECT COUNT(*) FROM bans WHERE ip='$ip';" 2>/dev/null)
                if [ "$count" -gt "0" ]; then
                    sqlite3 "$DB_FILE" "DELETE FROM bans WHERE ip='$ip';" 2>/dev/null
                    echo "  ✓ Removed $count database entries"
                else
                    echo "  ℹ No database entries found"
                fi
            fi
            
            # Verify removal
            if ! iptables -L -n 2>/dev/null | grep -q "$ip"; then
                echo "✓ IP $ip completely unbanned"
            else
                echo "⚠ Warning: IP may still be in iptables"
            fi
        fi
        ;;
    
    ban)
        check_sudo
        if [ -z "$2" ] || [ -z "$3" ]; then
            echo "Usage: $0 ban <jail> <ip>"
            exit 1
        fi
        fail2ban-client set "$2" banip "$3"
        echo "✓ Banned $3 in $2"
        ;;
    
    whitelist)
        check_sudo
        if [ -z "$2" ]; then
            echo "Usage: $0 whitelist <ip> [add|remove|check]"
            echo "   or: $0 whitelist list"
            exit 1
        fi
        
        if [ "$2" = "list" ]; then
            echo "=== Whitelisted IPs ==="
            grep -h "ignoreip" /etc/fail2ban/jail.local /etc/fail2ban/jail.d/*.conf 2>/dev/null | \
                sort -u | sed 's/ignoreip.*=//' | tr ' ' '\n' | sort -u | grep -v "^$\|127.0.0.1\|::1"
            exit 0
        fi
        
        IP="$2"
        ACTION="${3:-add}"
        
        case "$ACTION" in
            add)
                echo "Adding $IP to whitelist..."
                
                # First unban from all active jails
                for jail in $(get_jails); do
                    fail2ban-client set "$jail" unbanip "$IP" 2>/dev/null
                done
                
                # Remove from database
                if [ -f "$DB_FILE" ]; then
                    sqlite3 "$DB_FILE" "DELETE FROM bans WHERE ip='$IP';" 2>/dev/null
                fi
                
                # Add to config files
                for conf in /etc/fail2ban/jail.local /etc/fail2ban/jail.d/*.conf; do
                    if [ -f "$conf" ] && grep -q "^ignoreip" "$conf"; then
                        if ! grep -q "ignoreip.*$IP" "$conf"; then
                            sed -i "/^ignoreip/s/$/ $IP/" "$conf"
                            echo "  ✓ Added to $(basename $conf)"
                        fi
                    fi
                done
                
                systemctl reload fail2ban
                echo "✓ IP $IP is now whitelisted"
                ;;
                
            remove)
                echo "Removing $IP from whitelist..."
                for conf in /etc/fail2ban/jail.local /etc/fail2ban/jail.d/*.conf; do
                    if [ -f "$conf" ]; then
                        sed -i "s/ $IP//g; s/$IP //g" "$conf" 2>/dev/null
                    fi
                done
                systemctl reload fail2ban
                echo "✓ IP $IP removed from whitelist"
                ;;
                
            check)
                echo "Checking if $IP is whitelisted..."
                found=0
                for conf in /etc/fail2ban/jail.local /etc/fail2ban/jail.d/*.conf; do
                    if [ -f "$conf" ] && grep -q "$IP" "$conf" 2>/dev/null; then
                        echo "  ✓ Found in $(basename $conf)"
                        found=1
                    fi
                done
                if [ $found -eq 0 ]; then
                    echo "  ✗ Not whitelisted"
                fi
                ;;
        esac
        ;;
    
    maxretry)
        check_sudo
        if [ -z "$2" ]; then
            echo "=== Max Retry Settings ==="
            current=$(get_maxretry)
            if [ ! -z "$current" ]; then
                echo "Current setting for invalid users/root: $current failed attempts before ban"
            else
                echo "No custom maxretry setting found"
            fi
            echo ""
            echo "Usage: $0 maxretry <number>"
            echo "       Sets max failed attempts before ban for invalid users and root"
            echo ""
            echo "Example: $0 maxretry 3"
            exit 0
        fi
        
        if ! [[ "$2" =~ ^[0-9]+$ ]]; then
            echo "Error: maxretry must be a positive number"
            exit 1
        fi
        
        NEW_MAXRETRY="$2"
        
        echo "Setting maxretry to $NEW_MAXRETRY for invalid users/root..."
        
        # Save to tracking file
        echo "$NEW_MAXRETRY" > "$MAXRETRY_FILE"
        
        # Update jail config
        JAIL_CONF="/etc/fail2ban/jail.d/permanent-ban-invalidusers.conf"
        if [ -f "$JAIL_CONF" ]; then
            sed -i "s/^maxretry = .*/maxretry = $NEW_MAXRETRY/" "$JAIL_CONF"
            echo "  ✓ Updated $JAIL_CONF"
        else
            echo "  ⚠ Config file not found: $JAIL_CONF"
        fi
        
        # Restart fail2ban to apply
        echo "Restarting fail2ban..."
        systemctl restart fail2ban
        sleep 2
        
        echo "✓ Max retry set to $NEW_MAXRETRY"
        echo ""
        echo "Invalid users and root login attempts will be banned after $NEW_MAXRETRY failed attempts"
        ;;
    
    investigate)
        check_sudo
        if [ -z "$2" ]; then
            echo "Usage: $0 investigate <ip>"
            exit 1
        fi
        
        IP="$2"
        echo "=== Investigating $IP ==="
        
        # Check which jails have it banned
        echo ""
        echo "Current Status:"
        banned_in=""
        for jail in $(get_jails); do
            if fail2ban-client status "$jail" 2>/dev/null | grep -q "$IP"; then
                echo "  ✗ BANNED in $jail"
                banned_in="$banned_in $jail"
            fi
        done
        if [ -z "$banned_in" ]; then
            echo "  ✓ Not currently banned"
        fi
        
        # Check database
        if [ -f "$DB_FILE" ]; then
            echo ""
            echo "Database Records:"
            sqlite3 "$DB_FILE" -header -column \
                "SELECT jail, datetime(timeofban, 'unixepoch', 'localtime') as banned_at,
                 datetime(timeofban + bantime, 'unixepoch', 'localtime') as expires_at
                 FROM bans WHERE ip='$IP';" 2>/dev/null || echo "  No database records"
        fi
        
        # Check fail2ban logs
        echo ""
        echo "Fail2ban Events:"
        grep "$IP" /var/log/fail2ban.log 2>/dev/null | tail -5 | sed 's/^/  /'
        
        # Check SSH attempts
        echo ""
        echo "SSH Authentication Attempts:"
        attempts=$(journalctl -u ssh.service 2>/dev/null | grep -c "$IP")
        echo "  Total attempts: $attempts"
        
        if [ $attempts -gt 0 ]; then
            echo ""
            echo "Recent SSH Logs:"
            journalctl -u ssh.service 2>/dev/null | grep "$IP" | tail -5 | sed 's/^/  /'
            
            echo ""
            echo "Usernames Attempted:"
            journalctl -u ssh.service 2>/dev/null | grep "$IP" | \
                grep -oE "(Invalid user |Failed password for )[^ ]+" | \
                sed 's/Invalid user //;s/Failed password for //' | \
                sort | uniq -c | sort -rn | sed 's/^/  /'
        fi
        ;;
    
    test)
        check_sudo
        echo "=== Testing Configuration ==="
        fail2ban-client -t
        ;;
    
    logs)
        check_sudo
        echo "=== Following fail2ban logs (Ctrl+C to stop) ==="
        journalctl -u fail2ban.service -f
        ;;
    
    watch)
        check_sudo
        echo "=== Watching for SSH attacks (Ctrl+C to stop) ==="
        echo "Monitoring for: root, admin, test, ubuntu, and invalid users"
        echo ""
        
        journalctl -fu ssh.service | while read line; do
            if echo "$line" | grep -qE "Invalid user"; then
                echo "[INVALID USER] $line"
            elif echo "$line" | grep -qE "Failed password for root"; then
                echo "[ROOT ATTEMPT] $line"
            elif echo "$line" | grep -qE "Failed password for (admin|test|ubuntu)"; then
                echo "[RESERVED USER] $line"
            elif echo "$line" | grep -qE "Accepted publickey"; then
                echo "[LOGIN OK] $line"
            elif echo "$line" | grep -qE "Failed password"; then
                echo "[FAILED LOGIN] $line"
            fi
        done
        ;;
    
    stats)
        check_sudo
        echo "=== Fail2ban Statistics ==="
        
        # Service info
        echo ""
        echo "Service Status:"
        echo "  Status: $(systemctl is-active fail2ban)"
        echo "  Started: $(systemctl show fail2ban -p ActiveEnterTimestamp --value)"
        
        # Show maxretry setting
        maxretry=$(get_maxretry)
        if [ ! -z "$maxretry" ]; then
            echo "  Invalid user/root threshold: $maxretry attempts"
        fi
        
        # Calculate totals
        total_banned=0
        total_failed=0
        active_bans=0
        
        for jail in $(get_jails); do
            status=$(fail2ban-client status "$jail" 2>/dev/null)
            banned=$(echo "$status" | grep "Total banned:" | awk '{print $NF}')
            failed=$(echo "$status" | grep "Total failed:" | awk '{print $NF}')
            current=$(echo "$status" | grep "Currently banned:" | awk '{print $NF}')
            
            [ ! -z "$banned" ] && total_banned=$((total_banned + banned))
            [ ! -z "$failed" ] && total_failed=$((total_failed + failed))
            [ ! -z "$current" ] && active_bans=$((active_bans + current))
        done
        
        echo ""
        echo "Totals:"
        echo "  Total Failed Attempts: $total_failed"
        echo "  Total Banned IPs: $total_banned"
        echo "  Currently Banned: $active_bans"
        
        if [ -f "$DB_FILE" ]; then
            db_count=$(sqlite3 "$DB_FILE" "SELECT COUNT(*) FROM bans;" 2>/dev/null)
            echo "  Database Entries: $db_count"
        fi
        
        # Top banned IPs today
        echo ""
        echo "Today's Activity:"
        today_bans=$(grep "$(date +%Y-%m-%d)" /var/log/fail2ban.log 2>/dev/null | grep -c "Ban")
        echo "  Bans today: $today_bans"
        ;;
    
    consolidate)
        check_sudo
        ACTION="${2:-analyze}"
        THRESHOLD="${3:-3}"
        
        case "$ACTION" in
            analyze)
                echo "=== Consolidation Analysis ==="
                echo "Finding /24 and /16 blocks with multiple banned IPs..."
                echo "(Threshold: $THRESHOLD+ IPs to recommend consolidation)"
                echo ""
                
                # Collect all banned IPs from iptables f2b chains
                ALL_IPS=$(iptables-save 2>/dev/null | grep -E "^-A f2b-" | grep -oE '[0-9]+\.[0-9]+\.[0-9]+\.[0-9]+' | sort -u)
                
                if [ -z "$ALL_IPS" ]; then
                    echo "No banned IPs found in iptables."
                    exit 0
                fi
                
                TOTAL_IPS=$(echo "$ALL_IPS" | wc -l)
                echo "Total individual IP bans: $TOTAL_IPS"
                echo ""
                
                # Analyze /16 blocks
                echo "=== /16 Blocks (major consolidation opportunities) ==="
                echo "$ALL_IPS" | sed 's/\.[0-9]*\.[0-9]*$/.0.0\/16/' | sort | uniq -c | sort -rn | \
                    awk -v t="$THRESHOLD" '$1 >= t {printf "  %s - %d IPs (saves %d rules)\n", $2, $1, $1-1}' | head -10
                
                echo ""
                echo "=== /24 Blocks ==="
                echo "$ALL_IPS" | sed 's/\.[0-9]*$/.0\/24/' | sort | uniq -c | sort -rn | \
                    awk -v t="$THRESHOLD" '$1 >= t {printf "  %s - %d IPs (saves %d rules)\n", $2, $1, $1-1}' | head -20
                
                echo ""
                echo "To consolidate a block, run:"
                echo "  $0 consolidate apply <CIDR>  (e.g., $0 consolidate apply 45.78.0.0/16)"
                echo ""
                echo "To change threshold: $0 consolidate analyze <threshold>"
                ;;
            
            apply)
                CIDR="$3"
                if [ -z "$CIDR" ]; then
                    echo "Usage: $0 consolidate apply <CIDR>"
                    echo "Example: $0 consolidate apply 45.78.0.0/16"
                    echo "         $0 consolidate apply 101.47.160.0/24"
                    exit 1
                fi
                
                # Validate CIDR format
                if ! echo "$CIDR" | grep -qE '^[0-9]+\.[0-9]+\.[0-9]+\.[0-9]+/(8|16|24|32)$'; then
                    echo "Error: Invalid CIDR format. Use format like 45.78.0.0/16 or 192.168.1.0/24"
                    exit 1
                fi
                
                # Extract prefix for matching
                MASK=$(echo "$CIDR" | grep -oE '/[0-9]+$' | tr -d '/')
                case "$MASK" in
                    8)  PREFIX=$(echo "$CIDR" | cut -d. -f1)"." ;;
                    16) PREFIX=$(echo "$CIDR" | cut -d. -f1-2)"." ;;
                    24) PREFIX=$(echo "$CIDR" | cut -d. -f1-3)"." ;;
                    32) PREFIX=$(echo "$CIDR" | sed 's/\/32//') ;;
                esac
                
                echo "=== Consolidating $CIDR ==="
                echo "Prefix match: ${PREFIX}*"
                echo ""
                
                # Find all f2b chains
                CHAINS=$(iptables-save 2>/dev/null | grep -oE "f2b-[a-zA-Z0-9-]+" | sort -u)
                
                total_removed=0
                for chain in $CHAINS; do
                    chain_removed=0
                    # Find matching IPs in this chain
                    MATCHING_IPS=$(iptables -L "$chain" -n 2>/dev/null | awk '/REJECT|DROP/ {print $4}' | grep "^${PREFIX}")
                    
                    for ip in $MATCHING_IPS; do
                        # Determine action type (REJECT or DROP)
                        ACTION_TYPE=$(iptables -L "$chain" -n 2>/dev/null | grep "$ip" | awk '{print $1}' | head -1)
                        
                        if [ "$ACTION_TYPE" = "REJECT" ]; then
                            iptables -D "$chain" -s "$ip" -j REJECT --reject-with icmp-port-unreachable 2>/dev/null && \
                                echo "  Removed $ip from $chain" && chain_removed=$((chain_removed + 1))
                        elif [ "$ACTION_TYPE" = "DROP" ]; then
                            iptables -D "$chain" -s "$ip" -j DROP 2>/dev/null && \
                                echo "  Removed $ip from $chain" && chain_removed=$((chain_removed + 1))
                        fi
                    done
                    
                    # Add the CIDR block if we removed any IPs from this chain
                    if [ $chain_removed -gt 0 ]; then
                        total_removed=$((total_removed + chain_removed))
                        # Check if CIDR already exists
                        if ! iptables -L "$chain" -n 2>/dev/null | grep -q "$CIDR"; then
                            # Use same action as the chain typically uses
                            if iptables -L "$chain" -n 2>/dev/null | grep -q "REJECT"; then
                                iptables -I "$chain" 1 -s "$CIDR" -j REJECT --reject-with icmp-port-unreachable
                            else
                                iptables -I "$chain" 1 -s "$CIDR" -j DROP
                            fi
                            echo "  ✓ Added $CIDR to $chain"
                        fi
                    fi
                done
                
                if [ $total_removed -eq 0 ]; then
                    echo "No matching IPs found for $CIDR"
                else
                    echo ""
                    echo "✓ Consolidated $total_removed individual IPs into $CIDR"
                    echo ""
                    echo "⚠ Note: This change is temporary (lost on reboot/restart)."
                    echo "  To make permanent, add to /etc/fail2ban/action.d/ or use ipset."
                fi
                ;;
            
            apply-all)
                THRESHOLD="${3:-5}"
                CIDR_SIZE="${4:-16}"  # Default to /16, can specify 24
                
                echo "=== Applying All Consolidations ==="
                echo "Threshold: $THRESHOLD+ IPs per block"
                echo "Block size: /$CIDR_SIZE"
                echo ""
                
                ALL_IPS=$(iptables-save 2>/dev/null | grep -E "^-A f2b-" | grep -oE '[0-9]+\.[0-9]+\.[0-9]+\.[0-9]+' | sort -u)
                
                if [ -z "$ALL_IPS" ]; then
                    echo "No banned IPs found."
                    exit 0
                fi
                
                # Get list of CIDRs to consolidate
                if [ "$CIDR_SIZE" = "16" ]; then
                    CIDRS=$(echo "$ALL_IPS" | sed 's/\.[0-9]*\.[0-9]*$/.0.0\/16/' | sort | uniq -c | sort -rn | \
                        awk -v t="$THRESHOLD" '$1 >= t {print $2}')
                else
                    CIDRS=$(echo "$ALL_IPS" | sed 's/\.[0-9]*$/.0\/24/' | sort | uniq -c | sort -rn | \
                        awk -v t="$THRESHOLD" '$1 >= t {print $2}')
                fi
                
                if [ -z "$CIDRS" ]; then
                    echo "No blocks found with $THRESHOLD+ IPs."
                    exit 0
                fi
                
                # Count what we're about to do
                CIDR_COUNT=$(echo "$CIDRS" | wc -l)
                echo "Found $CIDR_COUNT blocks to consolidate:"
                echo "$CIDRS" | sed 's/^/  /'
                echo ""
                
                read -p "Proceed? [y/N] " confirm
                if [[ ! "$confirm" =~ ^[Yy]$ ]]; then
                    echo "Aborted."
                    exit 0
                fi
                
                echo ""
                grand_total=0
                for cidr in $CIDRS; do
                    echo "--- Processing $cidr ---"
                    
                    # Extract prefix for matching
                    MASK=$(echo "$cidr" | grep -oE '/[0-9]+$' | tr -d '/')
                    case "$MASK" in
                        8)  PREFIX=$(echo "$cidr" | cut -d. -f1)"." ;;
                        16) PREFIX=$(echo "$cidr" | cut -d. -f1-2)"." ;;
                        24) PREFIX=$(echo "$cidr" | cut -d. -f1-3)"." ;;
                    esac
                    
                    CHAINS=$(iptables-save 2>/dev/null | grep -oE "f2b-[a-zA-Z0-9-]+" | sort -u)
                    
                    cidr_total=0
                    for chain in $CHAINS; do
                        chain_removed=0
                        MATCHING_IPS=$(iptables -L "$chain" -n 2>/dev/null | awk '/REJECT|DROP/ {print $4}' | grep "^${PREFIX}")
                        
                        for ip in $MATCHING_IPS; do
                            ACTION_TYPE=$(iptables -L "$chain" -n 2>/dev/null | grep "$ip" | awk '{print $1}' | head -1)
                            
                            if [ "$ACTION_TYPE" = "REJECT" ]; then
                                iptables -D "$chain" -s "$ip" -j REJECT --reject-with icmp-port-unreachable 2>/dev/null && \
                                    chain_removed=$((chain_removed + 1))
                            elif [ "$ACTION_TYPE" = "DROP" ]; then
                                iptables -D "$chain" -s "$ip" -j DROP 2>/dev/null && \
                                    chain_removed=$((chain_removed + 1))
                            fi
                        done
                        
                        if [ $chain_removed -gt 0 ]; then
                            cidr_total=$((cidr_total + chain_removed))
                            if ! iptables -L "$chain" -n 2>/dev/null | grep -q "$cidr"; then
                                if iptables -L "$chain" -n 2>/dev/null | grep -q "REJECT"; then
                                    iptables -I "$chain" 1 -s "$cidr" -j REJECT --reject-with icmp-port-unreachable
                                else
                                    iptables -I "$chain" 1 -s "$cidr" -j DROP
                                fi
                            fi
                        fi
                    done
                    
                    [ $cidr_total -gt 0 ] && echo "  ✓ Consolidated $cidr_total IPs into $cidr"
                    grand_total=$((grand_total + cidr_total))
                done
                
                echo ""
                echo "=== Summary ==="
                echo "✓ Consolidated $grand_total individual IPs into $CIDR_COUNT CIDR blocks"
                
                # Show new rule count
                NEW_COUNT=$(iptables-save 2>/dev/null | grep -E "^-A f2b-" | wc -l)
                echo "  New total iptables rules in f2b chains: $NEW_COUNT"
                echo ""
                echo "⚠ Changes are temporary. Will be lost on fail2ban restart."
                ;;
            
            recommend)
                echo "=== Recommended Consolidations ==="
                THRESHOLD="${3:-5}"
                
                ALL_IPS=$(iptables-save 2>/dev/null | grep -E "^-A f2b-" | grep -oE '[0-9]+\.[0-9]+\.[0-9]+\.[0-9]+' | sort -u)
                
                echo "Generating consolidation commands (threshold: $THRESHOLD+ IPs)..."
                echo ""
                
                # /16 recommendations
                echo "# Large blocks (/16):"
                echo "$ALL_IPS" | sed 's/\.[0-9]*\.[0-9]*$/.0.0\/16/' | sort | uniq -c | sort -rn | \
                    awk -v t="$THRESHOLD" -v cmd="$0" '$1 >= t {printf "%s consolidate apply %s  # %d IPs\n", cmd, $2, $1}'
                
                echo ""
                echo "# Smaller blocks (/24) not covered by /16 above:"
                echo "$ALL_IPS" | sed 's/\.[0-9]*$/.0\/24/' | sort | uniq -c | sort -rn | \
                    awk -v t="$THRESHOLD" -v cmd="$0" '$1 >= t {printf "%s consolidate apply %s  # %d IPs\n", cmd, $2, $1}' | head -15
                ;;
            
            *)
                echo "Usage: $0 consolidate <subcommand> [options]"
                echo ""
                echo "Subcommands:"
                echo "  analyze [threshold]           - Show consolidation opportunities (default: 3)"
                echo "  apply <CIDR>                  - Consolidate IPs into a CIDR block"
                echo "  apply-all [threshold] [size]  - Consolidate all blocks (default: 5+ IPs, /16)"
                echo "  recommend [threshold]         - Generate consolidation commands"
                echo ""
                echo "Examples:"
                echo "  $0 consolidate analyze"
                echo "  $0 consolidate analyze 5          # Only show blocks with 5+ IPs"
                echo "  $0 consolidate apply 45.78.0.0/16"
                echo "  $0 consolidate apply-all          # All /16 blocks with 5+ IPs"
                echo "  $0 consolidate apply-all 10       # All /16 blocks with 10+ IPs"
                echo "  $0 consolidate apply-all 3 24     # All /24 blocks with 3+ IPs"
                ;;
        esac
        ;;
    
    reload)
        check_sudo
        echo "Reloading fail2ban..."
        systemctl reload fail2ban
        echo "✓ Fail2ban reloaded"
        ;;
    
    restart)
        check_sudo
        echo "Restarting fail2ban..."
        systemctl restart fail2ban
        sleep 2
        echo "✓ Fail2ban restarted"
        $0 status
        ;;
    
    *)
        echo "fail2ban-manage v$VERSION - Comprehensive fail2ban management"
        echo ""
        echo "Usage: $0 <command> [options]"
        echo ""
        echo "Commands:"
        echo "  status              - Show all jails and their status"
        echo "  banned              - List all banned IPs with attempted usernames"
        echo "  unban <ip>          - Permanently unban IP (removes from DB)"
        echo "  unban all           - Unban all IPs"
        echo "  ban <jail> <ip>     - Manually ban an IP"
        echo "  maxretry [N]        - Show or set max failed attempts before ban"
        echo "  whitelist <ip> add  - Add IP to whitelist"
        echo "  whitelist list      - Show all whitelisted IPs"
        echo "  investigate <ip>    - Show why IP was banned"
        echo "  test                - Test configuration"
        echo "  logs                - Follow fail2ban logs"
        echo "  watch               - Watch for SSH attacks"
        echo "  stats               - Show statistics"
        echo "  consolidate         - Analyze/apply CIDR consolidation"
        echo "  reload              - Reload fail2ban"
        echo "  restart             - Restart fail2ban"
        echo "  version             - Show version"
        echo ""
        echo "Examples:"
        echo "  $0 unban 192.168.1.100"
        echo "  $0 maxretry 3           # Set ban after 3 failed attempts"
        echo "  $0 whitelist 10.0.0.5 add"
        echo "  $0 investigate 45.78.219.24"
        echo "  $0 consolidate analyze       # Find CIDR consolidation opportunities"
        echo "  $0 consolidate apply 45.78.0.0/16"
        ;;
esac
