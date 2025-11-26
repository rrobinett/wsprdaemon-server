#!/bin/bash
# fail2ban-manage.sh - Comprehensive fail2ban management tool
# Version: 1.1
# Includes permanent unban with database cleanup

VERSION="1.1"

# Database location
DB_FILE="/var/lib/fail2ban/fail2ban.sqlite3"

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

case "$1" in
    version)
        echo "fail2ban-manage version $VERSION"
        ;;
    
    status)
        check_sudo
        echo "=== Fail2ban Status ==="
        echo "Service: $(systemctl is-active fail2ban)"
        echo "Uptime: $(systemctl show fail2ban -p ActiveEnterTimestamp --value)"
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
        total=0
        for jail in $(get_jails); do
            bans=$(fail2ban-client status "$jail" 2>/dev/null | grep "Banned IP list:" | sed 's/.*Banned IP list://')
            if [ ! -z "$bans" ] && [ "$bans" != "" ]; then
                echo "$jail:$bans"
                count=$(echo "$bans" | wc -w)
                total=$((total + count))
            fi
        done
        echo ""
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
        attempts=$(journalctl -u sshd.service 2>/dev/null | grep -c "$IP")
        echo "  Total attempts: $attempts"
        
        if [ $attempts -gt 0 ]; then
            echo ""
            echo "Recent SSH Logs:"
            journalctl -u sshd.service 2>/dev/null | grep "$IP" | tail -5 | sed 's/^/  /'
            
            echo ""
            echo "Usernames Attempted:"
            journalctl -u sshd.service 2>/dev/null | grep "$IP" | \
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
        
        journalctl -fu sshd.service | while read line; do
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
        echo "  banned              - List all banned IPs"
        echo "  unban <ip>          - Permanently unban IP (removes from DB)"
        echo "  unban all           - Unban all IPs"
        echo "  ban <jail> <ip>     - Manually ban an IP"
        echo "  whitelist <ip> add  - Add IP to whitelist"
        echo "  whitelist list      - Show all whitelisted IPs"
        echo "  investigate <ip>    - Show why IP was banned"
        echo "  test                - Test configuration"
        echo "  logs                - Follow fail2ban logs"
        echo "  watch               - Watch for SSH attacks"
        echo "  stats               - Show statistics"
        echo "  reload              - Reload fail2ban"
        echo "  restart             - Restart fail2ban"
        echo "  version             - Show version"
        echo ""
        echo "Examples:"
        echo "  $0 unban 192.168.1.100"
        echo "  $0 whitelist 10.0.0.5 add"
        echo "  $0 investigate 45.78.219.24"
        ;;
esac
