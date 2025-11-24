#!/bin/bash
# Fail2ban management script v2.0
# Protected networks: 10.0.0.0/8, 172.30.31.0/24

RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m'

# Function to check if IP is in protected range
is_protected_ip() {
    local ip=$1
    # Check if IP is in 10.0.0.0/8 or 172.30.31.0/24
    if [[ $ip =~ ^10\. ]] || [[ $ip =~ ^172\.30\.31\. ]] || [[ $ip == "127.0.0.1" ]]; then
        return 0  # Protected
    fi
    return 1  # Not protected
}

case "$1" in
    status)
        echo -e "${GREEN}=== Fail2ban Status ===${NC}"
        sudo systemctl is-active fail2ban
        echo ""
        sudo fail2ban-client status
        echo ""
        echo -e "${YELLOW}Protected Networks:${NC} 10.0.0.0/8, 172.30.31.0/24"
        echo ""
        for jail in $(sudo fail2ban-client status | grep "Jail list" | sed 's/.*://;s/,//g'); do
            echo -e "${YELLOW}[$jail]${NC}"
            sudo fail2ban-client status "$jail" | grep -E "Currently failed|Total failed|Currently banned|Total banned"
            bans=$(sudo fail2ban-client status "$jail" | grep "Banned IP list:" | sed 's/.*Banned IP list://')
            if [ ! -z "$bans" ] && [ "$bans" != "" ]; then
                echo "  Banned IPs: $bans"
            fi
            echo ""
        done
        ;;
    
    banned)
        echo -e "${GREEN}=== All Banned IPs ===${NC}"
        for jail in $(sudo fail2ban-client status | grep "Jail list" | sed -E 's/^[^:]+:[ \t]+//' | sed 's/,//g'); do
            echo -e "${YELLOW}Jail: $jail${NC}"
            banned_list=$(sudo fail2ban-client status "$jail" | grep "Banned IP list:" | sed 's/.*://')
            if [ ! -z "$banned_list" ]; then
                for ip in $banned_list; do
                    if is_protected_ip "$ip"; then
                        echo -e "  ${RED}$ip (WARNING: LOCAL IP SHOULD NOT BE BANNED!)${NC}"
                    else
                        echo "  $ip"
                    fi
                done
            fi
        done
        ;;
    
    unban)
        if [ -z "$2" ]; then
            echo "Usage: $0 unban <ip>"
            echo "   or: $0 unban <jail> <ip>"
            exit 1
        fi
        if [ -z "$3" ]; then
            # Unban from all jails
            ip="$2"
            if is_protected_ip "$ip"; then
                echo -e "${YELLOW}Note: $ip is in protected range and should never be banned${NC}"
            fi
            echo "Unbanning $ip from all jails..."
            for jail in $(sudo fail2ban-client status | grep "Jail list" | sed 's/.*://;s/,//g'); do
                sudo fail2ban-client set "$jail" unbanip "$ip" 2>/dev/null && \
                    echo -e "${GREEN}  Unbanned from $jail${NC}"
            done
        else
            # Unban from specific jail
            if is_protected_ip "$3"; then
                echo -e "${YELLOW}Note: $3 is in protected range and should never be banned${NC}"
            fi
            sudo fail2ban-client set "$2" unbanip "$3"
            echo -e "${GREEN}Unbanned $3 from $2${NC}"
        fi
        ;;
    
    ban)
        if [ -z "$2" ] || [ -z "$3" ]; then
            echo "Usage: $0 ban <jail> <ip>"
            exit 1
        fi
        if is_protected_ip "$3"; then
            echo -e "${RED}ERROR: Cannot ban $3 - IP is in protected range${NC}"
            echo "Protected ranges: 10.0.0.0/8, 172.30.31.0/24"
            exit 1
        fi
        sudo fail2ban-client set "$2" banip "$3"
        echo -e "${GREEN}Banned $3 in $2${NC}"
        ;;
    
    check-local)
        echo -e "${GREEN}=== Checking for Banned Local IPs ===${NC}"
        found=0
        for jail in $(sudo fail2ban-client status | grep "Jail list" | sed 's/.*://;s/,//g'); do
            banned_ips=$(sudo fail2ban-client status "$jail" | grep "Banned IP list:" | sed 's/.*://')
            for ip in $banned_ips; do
                if is_protected_ip "$ip"; then
                    echo -e "${RED}WARNING: Local IP $ip is banned in $jail!${NC}"
                    echo "  Run: $0 unban $ip"
                    found=1
                fi
            done
        done
        if [ $found -eq 0 ]; then
            echo -e "${GREEN}✓ No local IPs are banned${NC}"
        fi
        ;;
    
    whitelist)
        echo -e "${GREEN}=== Current Whitelist Configuration ===${NC}"
        echo "Protected Networks: 10.0.0.0/8, 172.30.31.0/24"
        echo ""
        echo "Configuration files:"
        grep -h "^ignoreip" /etc/fail2ban/jail.local /etc/fail2ban/jail.d/*.conf 2>/dev/null | sort -u
        ;;
    
    test)
        echo -e "${GREEN}=== Testing Configuration ===${NC}"
        sudo fail2ban-client -d 2>&1 | head -20
        ;;
    
    logs)
        sudo journalctl -u fail2ban.service -f
        ;;
    
    watch)
        echo -e "${GREEN}=== Watching for Invalid Login Attempts ===${NC}"
        echo "Monitoring for: root, admin, test, ubuntu, and non-existent users"
        echo -e "${YELLOW}Protected networks: 10.0.0.0/8, 172.30.31.0/24${NC}"
        echo "Press Ctrl+C to stop"
        echo ""
        sudo journalctl -fu sshd.service | while read line; do
            if echo "$line" | grep -qE "Invalid user"; then
                echo -e "${RED}[INVALID USER]${NC} $line"
            elif echo "$line" | grep -qE "Failed password for root"; then
                echo -e "${RED}[ROOT ATTEMPT]${NC} $line"
            elif echo "$line" | grep -qE "Failed password for (admin|test|ubuntu)"; then
                echo -e "${YELLOW}[RESERVED USER]${NC} $line"
            fi
        done
        ;;
    
    stats)
        echo -e "${GREEN}=== Fail2ban Statistics ===${NC}"
        echo "Uptime: $(sudo fail2ban-client status | grep "Number of jail" | head -1)"
        echo -e "${YELLOW}Protected Networks:${NC} 10.0.0.0/8, 172.30.31.0/24"
        echo ""
        total_banned=0
        total_failed=0
        for jail in $(sudo fail2ban-client status | grep "Jail list" | sed 's/.*://;s/,//g'); do
            banned=$(sudo fail2ban-client status "$jail" | grep "Total banned:" | awk '{print $NF}')
            failed=$(sudo fail2ban-client status "$jail" | grep "Total failed:" | awk '{print $NF}')
            [ ! -z "$banned" ] && total_banned=$((total_banned + banned))
            [ ! -z "$failed" ] && total_failed=$((total_failed + failed))
        done
        echo "Total Failed Attempts: $total_failed"
        echo "Total Banned IPs: $total_banned"
        ;;
    
    *)
        echo "Usage: $0 {status|banned|unban|ban|check-local|whitelist|test|logs|watch|stats}"
        echo ""
        echo "  status       - Show all jails and their status"
        echo "  banned       - List all banned IPs"
        echo "  unban        - Unban IP from all jails"
        echo "  ban          - Manually ban an IP (blocked for local IPs)"
        echo "  check-local  - Check if any local IPs are banned"
        echo "  whitelist    - Show whitelist configuration"
        echo "  test         - Test configuration"
        echo "  logs         - Follow fail2ban logs"
        echo "  watch        - Watch for invalid login attempts"
        echo "  stats        - Show statistics"
        echo ""
        echo -e "${YELLOW}Protected networks: 10.0.0.0/8, 172.30.31.0/24${NC}"
        ;;
esac
