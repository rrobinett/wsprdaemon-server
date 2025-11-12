#!/bin/bash
# Wrapper script for wsprdaemon_reflector service

set -e

CONFIG_FILE="${1:-/etc/wsprdaemon/reflector_destinations.json}"

if [[ ! -f "$CONFIG_FILE" ]]; then
    echo "ERROR: Configuration file not found: $CONFIG_FILE"
    exit 1
fi

echo "Starting WSPRDAEMON Reflector..."
echo "Configuration: $CONFIG_FILE"

# Use system python3 (no venv needed - only standard libraries)
# Note: Python script expects --config flag, not positional argument
exec /usr/bin/python3 /usr/local/bin/wsprdaemon_reflector.py --config "$CONFIG_FILE"
