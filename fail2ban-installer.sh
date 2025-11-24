#!/bin/bash
# fail2ban-installer.sh - Complete fail2ban setup with local network protection
# Version 2.0 - Never bans 10.0.0.0/8 or 172.30.31.0/24

set -e  # Exit on error

# Color codes for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

# Protected networks that should never be banned
LOCAL_NETWORKS="10.0.0.0/8 172.30.31.0/24"

echo -e "${GREEN}=== Fail2ban Installer for WSPRDAEMON v2.0 ===${NC}"
echo "This will set up fail2ban with permanent banning for invalid users and root attempts"
echo -e "${YELLOW}Protected networks (never banned): $LOCAL_NETWORKS${NC}"
echo ""

# Check if running as root or with sudo
if [ "$EUID" -ne 0 ]; then 
    echo -e "${RED}Please run with sudo: sudo bash $0${NC}"
    exit 1
fi

# Get additional IP to whitelist
echo -e "${YELLOW}Enter additional management IP to whitelist (or press Enter to skip):${NC}"
read -p "IP Address: " WHITELIST_IP

if [ ! -z "$WHITELIST_IP" ]; then
    WHITELIST_SETTING="ignoreip = 127.0.0.1/8 ::1 $LOCAL_NETWORKS $WHITELIST_IP"
    echo -e "${GREEN}Will whitelist: $LOCAL_NETWORKS $WHITELIST_IP${NC}"
else
    WHITELIST_SETTING="ignoreip = 127.0.0.1/8 ::1 $LOCAL_NETWORKS"
    echo -e "${GREEN}Will whitelist: $LOCAL_NETWORKS${NC}"
fi

echo -e "\n${GREEN}[1/8] Installing fail2ban...${NC}"
apt update
apt install -y fail2ban

echo -e "\n${GREEN}[2/8] Creating persistent database configuration...${NC}"
mkdir -p /etc/fail2ban/fail2ban.d/
cat > /etc/fail2ban/fail2ban.d/persistent.conf << 'EOF'
[Definition]
# Database to store persistent bans
dbfile = /var/lib/fail2ban/fail2ban.sqlite3
# Purge database entries older than 1 year
dbpurgeage = 1y
EOF

mkdir -p /var/lib/fail2ban/

echo -e "\n${GREEN}[3/8] Creating filter for invalid/non-existent users...${NC}"
cat > /etc/fail2ban/filter.d/sshd-invalidusers.conf << 'EOF'
[INCLUDES]
before = common.conf

[Definition]
# Match any of these patterns for invalid/reserved users
failregex = ^.*Invalid user .* from <HOST>.*$
            ^.*Failed password for invalid user .* from <HOST>.*$
            ^.*Failed password for root from <HOST>.*$
            ^.*Failed password for (root|admin|user|ubuntu|test|guest|oracle|postgres|mysql|www-data|ftp|nobody|daemon|bin|sys|sync|games|man|mail|news|www|backup|list|proxy|gnats|irc|debian|apache|nginx|tomcat|git|svn|jenkins|nagios|zabbix|administrator|Admin|support|operator|ftpuser|default|web|deploy|dev|staff|sales|marketing|pi|odoo|redis|mongodb|elasticsearch|cassandra|hadoop|spark|kafka) from <HOST>.*$
            ^.*User (root|admin|user|ubuntu|test|guest|oracle|postgres|mysql|www-data|ftp|nobody|daemon|bin|sys|sync|games|man|mail|news|www|backup|list|proxy|gnats|irc|debian|apache|nginx|tomcat|git|svn|jenkins|nagios|zabbix|administrator|Admin|support|operator|ftpuser|default|web|deploy|dev|staff|sales|marketing|pi|odoo|redis|mongodb|elasticsearch|cassandra|hadoop|spark|kafka) from <HOST> not allowed because not listed in AllowUsers$
            ^.*authentication failure.*rhost=<HOST>.*user=(root|admin|user|ubuntu|test|guest|oracle|postgres|mysql|www-data|ftp|nobody|daemon|bin|sys|sync|games|man|mail|news|www|backup|list|proxy|gnats|irc|debian|apache|nginx|tomcat|git|svn|jenkins|nagios|zabbix|administrator|Admin|support|operator|ftpuser|default|web|deploy|dev|staff|sales|marketing|pi|odoo|redis|mongodb|elasticsearch|cassandra|hadoop|spark|kafka).*$
            ^.*pam_unix\(sshd:auth\): check pass; user unknown.*rhost=<HOST>.*$

ignoreregex =

[Init]
maxlines = 10

journalmatch = _SYSTEMD_UNIT=sshd.service + _COMM=sshd
EOF

echo -e "\n${GREEN}[4/8] Creating filter for root login attempts...${NC}"
cat > /etc/fail2ban/filter.d/sshd-root.conf << 'EOF'
[Definition]
failregex = ^.*Failed password for root from <HOST>.*$
            ^.*authentication failure.*rhost=<HOST>.*user=root.*$
ignoreregex =
journalmatch = _SYSTEMD_UNIT=sshd.service + _COMM=sshd
EOF

echo -e "\n${GREEN}[5/8] Creating jail configuration with local network protection...${NC}"
mkdir -p /etc/fail2ban/jail.d/

# Main jail configuration
cat > /etc/fail2ban/jail.local << EOF
[DEFAULT]
$WHITELIST_SETTING
bantime = 3600
findtime = 600
maxretry = 5
backend = systemd
destemail = root@localhost
sender = fail2ban@\$(hostname -f)
action = %(action_)s

[sshd]
enabled = true
port = ssh
filter = sshd
journalmatch = _SYSTEMD_UNIT=ssh.service + _COMM=sshd
maxretry = 3
bantime = 3600
findtime = 600

[recidive]
enabled = true
filter = recidive
logpath = /var/log/fail2ban.log
action = %(banaction)s[name=%(__name__)s, protocol="%(protocol)s", port="%(port)s"]
bantime = 86400
findtime = 86400
maxretry = 3
EOF

echo -e "\n${GREEN}[6/8] Creating permanent ban configuration for invalid users...${NC}"
cat > /etc/fail2ban/jail.d/permanent-ban-invalidusers.conf << EOF
[sshd-invalidusers]
enabled = true
port = 0:65535
filter = sshd-invalidusers
backend = systemd
journalmatch = _SYSTEMD_UNIT=ssh.service + _COMM=sshd
maxretry = 1
# 10 years = essentially permanent
bantime = 315360000
findtime = 86400
action = %(action_)s
$WHITELIST_SETTING

[sshd-root]
enabled = true
port = 0:65535
filter = sshd-root
backend = systemd
journalmatch = _SYSTEMD_UNIT=ssh.service + _COMM=sshd
maxretry = 1
bantime = 315360000
findtime = 86400
action = %(action_)s
$WHITELIST_SETTING
EOF

echo -e "\n${GREEN}[7/8] Creating management script...${NC}"
mkdir -p ~/bin

cat > ~/bin/f2b-manage.sh << 'SCRIPT_EOF'
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
SCRIPT_EOF

chmod +x ~/bin/f2b-manage.sh

echo -e "\n${GREEN}[8/8] Starting and enabling fail2ban...${NC}"
systemctl enable fail2ban
systemctl restart fail2ban

# Wait for fail2ban to fully start
sleep 3

# Check for any currently banned local IPs and unban them
echo -e "\n${GREEN}Checking for banned local IPs...${NC}"
~/bin/f2b-manage.sh check-local

echo -e "\n${GREEN}=== Installation Complete ===${NC}"
echo ""

# Show status
~/bin/f2b-manage.sh status

echo -e "\n${GREEN}=== Quick Reference ===${NC}"
echo "Management script: ~/bin/f2b-manage.sh"
echo ""
echo "Common commands:"
echo "  ~/bin/f2b-manage.sh status      - Check status"
echo "  ~/bin/f2b-manage.sh banned      - List banned IPs"
echo "  ~/bin/f2b-manage.sh check-local - Check for banned local IPs"
echo "  ~/bin/f2b-manage.sh watch       - Watch for attacks"
echo "  ~/bin/f2b-manage.sh unban IP    - Unban an IP"
echo ""
echo -e "${YELLOW}Configuration:${NC}"
echo "  - Protected Networks: 10.0.0.0/8, 172.30.31.0/24"
echo "  - Root attempts: 1 try = 10 year ban"
echo "  - Invalid users: 1 try = 10 year ban"
echo "  - Normal SSH: 3 tries = 1 hour ban"
echo "  - Recidive: 3 rebans = 1 day ban"

echo ""
echo -e "${GREEN}Done! Fail2ban is protecting your server.${NC}"
echo -e "${GREEN}Local networks (10.0.0.0/8, 172.30.31.0/24) will never be banned.${NC}"
