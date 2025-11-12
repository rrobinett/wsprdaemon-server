# WSPRDAEMON Server Services

Server-side services for the WSPRDAEMON WSPR propagation monitoring system. This repository contains three main services:

1. **wsprnet_scraper** - Scrapes WSPR spot data from wsprnet.org
2. **wsprdaemon_server** - Processes uploaded .tbz files from wsprdaemon clients  
3. **wsprdaemon_reflector** - Distributes uploaded files to multiple servers

## Architecture
```
┌─────────────────────┐
│ WSPRDAEMON Clients  │
│ (Receivers/Kiwis)   │
└──────────┬──────────┘
           │ Upload .tbz files
           ▼
┌─────────────────────┐     ┌──────────────────┐
│ Reflector Service   │────▶│ Server 1         │
│ (Distribution)      │     │ - Scraper        │
└─────────────────────┘     │ - Server         │
           │                 │ - ClickHouse     │
           │                 └──────────────────┘
           ▼
┌──────────────────┐
│ Server 2         │
│ - Scraper        │
│ - Server         │
│ - ClickHouse     │
└──────────────────┘
```

## Prerequisites

### System Requirements
- Ubuntu 24.04 LTS (or similar Debian-based system)
- Python 3.10 or later
- ClickHouse 24.x or later
- 4GB+ RAM (8GB+ recommended)
- Fast storage (SSD recommended for database)

### Required Packages
```bash
sudo apt update
sudo apt install -y python3 python3-pip python3-venv git rsync
```

### ClickHouse Installation
The server services require ClickHouse to be installed and running:
```bash
# Install ClickHouse
curl https://clickhouse.com/ | sh
sudo ./clickhouse install

# Start ClickHouse
sudo systemctl start clickhouse-server
sudo systemctl enable clickhouse-server

# Set default user password
clickhouse-client --password
# Then: ALTER USER default IDENTIFIED BY 'your_secure_password';
```

## Installation

### 1. Clone the Repository
```bash
cd /home/wsprdaemon
git clone https://github.com/rrobinett/wsprdaemon-server.git wsprdaemon
cd wsprdaemon
```

### 2. Install WSPRNET Scraper and WSPRDAEMON Server

For servers that will process WSPR data:
```bash
sudo bash install-servers.sh
```

This installs:
- Python scripts to `/usr/local/bin/`
- Wrapper scripts to `/usr/local/bin/`
- Systemd services to `/etc/systemd/system/`
- Creates `/etc/wsprdaemon/` config directory
- Creates Python virtual environment with dependencies

### 3. Install Reflector Service

For servers that will distribute files:
```bash
sudo bash install-reflector.sh
```

## Configuration

### ClickHouse Configuration

Create `/etc/wsprdaemon/clickhouse.conf`:
```bash
sudo cp config-examples/clickhouse.conf.example /etc/wsprdaemon/clickhouse.conf
sudo nano /etc/wsprdaemon/clickhouse.conf
```

Set strong passwords for all database users.

### WSPRNET Scraper Configuration

Create `/etc/wsprdaemon/wsprnet.conf`:
```bash
sudo cp config-examples/wsprnet.conf.example /etc/wsprdaemon/wsprnet.conf
sudo nano /etc/wsprdaemon/wsprnet.conf
```

**Required:** Set your wsprnet.org username and password (register at wsprnet.org if needed).

### WSPRDAEMON Server Configuration

Create `/etc/wsprdaemon/wsprdaemon.conf`:
```bash
sudo cp config-examples/wsprdaemon.conf.example /etc/wsprdaemon/wsprdaemon.conf
sudo nano /etc/wsprdaemon/wsprdaemon.conf
```

### Reflector Configuration

Create `/etc/wsprdaemon/reflector_destinations.json`:
```bash
sudo cp config-examples/reflector_destinations.json.example /etc/wsprdaemon/reflector_destinations.json
sudo nano /etc/wsprdaemon/reflector_destinations.json
```

Configure destination servers, SSH credentials, and rsync settings.

### Set Proper Permissions
```bash
sudo chown root:wsprdaemon /etc/wsprdaemon/*.conf
sudo chmod 640 /etc/wsprdaemon/*.conf
sudo chown wsprdaemon:wsprdaemon /etc/wsprdaemon/*.json
sudo chmod 644 /etc/wsprdaemon/*.json
```

## Service Management

### WSPRNET Scraper
```bash
# Enable and start service
sudo systemctl enable wsprnet_scraper@wsprnet
sudo systemctl start wsprnet_scraper@wsprnet

# Check status
sudo systemctl status wsprnet_scraper@wsprnet

# View logs
sudo journalctl -u wsprnet_scraper@wsprnet -f
# or
sudo tail -f /var/log/wsprdaemon/wsprnet_scraper.log
```

### WSPRDAEMON Server
```bash
# Enable and start service
sudo systemctl enable wsprdaemon_server@wsprdaemon
sudo systemctl start wsprdaemon_server@wsprdaemon

# Check status  
sudo systemctl status wsprdaemon_server@wsprdaemon

# View logs
sudo journalctl -u wsprdaemon_server@wsprdaemon -f
# or
sudo tail -f /var/log/wsprdaemon/wsprdaemon_server.log
```

### Reflector Service
```bash
# Enable and start service
sudo systemctl enable wsprdaemon_reflector@reflector
sudo systemctl start wsprdaemon_reflector@reflector

# Check status
sudo systemctl status wsprdaemon_reflector@reflector

# View logs
sudo journalctl -u wsprdaemon_reflector@reflector -f
# or
sudo tail -f /var/log/wsprdaemon/reflector.log
```

## Database Schema

### WSPRNET Database
- `wsprnet.spots` - All WSPR spots from wsprnet.org
- `wsprnet.spots_recent` - Last 7 days (faster queries)
- `wsprnet.spots_frequency_overflow` - Spots with out-of-band frequencies

### WSPRDAEMON Database
- `wsprdaemon.spots` - WSPR spots from local receivers
- `wsprdaemon.noise` - Background noise measurements

See the individual service documentation in the `docs/` directory for detailed schema information.

## Troubleshooting

### Service Won't Start
1. Check systemd status: `sudo systemctl status service_name`
2. Check logs: `sudo journalctl -u service_name -n 100`
3. Verify configuration files exist and have correct permissions
4. Test Python venv: `/home/wsprdaemon/wsprdaemon/venv/bin/python3 --version`

### Database Connection Errors
1. Verify ClickHouse is running: `sudo systemctl status clickhouse-server`
2. Test connection: `clickhouse-client --password`
3. Check credentials in `/etc/wsprdaemon/clickhouse.conf`
4. Verify database users exist (see service documentation)

### WSPRNET Login Failures
1. Verify credentials in `/etc/wsprdaemon/wsprnet.conf`
2. Test login manually at wsprnet.org
3. Check session file: `/var/lib/wsprdaemon/wsprnet_session.json`
4. Clear session file and restart service to force re-login

### Reflector Not Distributing Files
1. Verify SSH key authentication to destination servers
2. Check rsync connectivity: `sudo -u wsprdaemon rsync -avz --dry-run /tmp/ user@host:/path/`
3. Verify all directories are on the same filesystem (for hard links)
4. Check spool directories: `ls -la /var/spool/wsprdaemon/reflector/`

## Security Notes

- **Credentials**: All configuration files containing passwords should have 640 permissions
- **SSH Keys**: The wsprdaemon user needs SSH key access to destination servers (reflector only)
- **Firewall**: ClickHouse port 8123 should only be accessible from trusted servers
- **Updates**: Keep ClickHouse and system packages updated

## Documentation

- [WSPRNET Scraper Details](docs/README-SCRAPER.md)
- [WSPRNET Scraper Caching](docs/WSPRNET_SCRAPER_CACHING_README.md)
- [Reflector Service](docs/REFLECTOR_README.md)

## Contributing

This is a specialized system for WSPR data collection. If you have improvements or bug fixes:

1. Test thoroughly on a non-production system
2. Ensure no private credentials are included
3. Document any configuration changes needed
4. Submit pull requests with clear descriptions

## License

MIT License - See LICENSE file for details

## Support

For issues or questions about WSPRDAEMON server services, please open an issue on GitHub.

For WSPR protocol questions, see: http://wsprnet.org  
For ClickHouse questions, see: https://clickhouse.com/docs

## Related Repositories

- [WSPRDAEMON Client](https://github.com/rrobinett/wsprdaemon) - Client-side receiver and processing code
