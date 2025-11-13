# Stop the service first
sudo systemctl stop clickhouse-server 2>/dev/null || true
sudo systemctl disable clickhouse-server 2>/dev/null || true

# Remove packages
sudo apt remove --purge -y clickhouse-server clickhouse-client clickhouse-common-static
sudo apt autoremove -y

# Remove configuration files
sudo rm -rf /etc/clickhouse-server/
sudo rm -rf /etc/clickhouse-client/

# Remove data directories (THIS DELETES ALL YOUR DATA!)
sudo rm -rf /var/lib/clickhouse/

# Remove log files
sudo rm -rf /var/log/clickhouse-server/

# Remove the repository configuration
sudo rm -f /etc/apt/sources.list.d/clickhouse.list

# Remove the GPG key
sudo rm -f /usr/share/keyrings/clickhouse-keyring.gpg

# Remove any systemd unit files
sudo rm -f /etc/systemd/system/clickhouse-server.service
sudo rm -f /lib/systemd/system/clickhouse-server.service

# Reload systemd
sudo systemctl daemon-reload

# Clean apt cache
sudo apt update
