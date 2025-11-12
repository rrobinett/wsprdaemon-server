#!/bin/bash
# Wrapper script for wsprdaemon_reflector service

CONFIG_FILE="${1:-/etc/wsprdaemon/reflector_destinations.json}"

echo "Starting WSPRDAEMON Reflector..."
echo "Configuration: $CONFIG_FILE"

# Use system python3 (no venv needed - only standard libraries)
exec /usr/bin/python3 /usr/local/bin/wsprdaemon_reflector.py "$CONFIG_FILE"
