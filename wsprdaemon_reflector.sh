#!/bin/bash
# Wrapper script for wsprdaemon_reflector service

set -e

CONFIG_FILE="${1:-/etc/wsprdaemon/reflector_destinations.json}"

if [[ ! -f "$CONFIG_FILE" ]]; then
    echo "ERROR: Configuration file not found: $CONFIG_FILE"
    exit 1
fi

# Extract log settings from JSON config
LOG_FILE=$(python3 -c "import json; print(json.load(open('$CONFIG_FILE')).get('log_file', '/var/log/wsprdaemon/reflector.log'))")
LOG_MAX_MB=$(python3 -c "import json; print(json.load(open('$CONFIG_FILE')).get('log_max_mb', 10))")
VERBOSITY=$(python3 -c "import json; print(json.load(open('$CONFIG_FILE')).get('verbosity', 1))")

echo "Starting WSPRDAEMON Reflector..."
echo "Configuration: $CONFIG_FILE"
echo "Log file: $LOG_FILE"

# Use system python3 (no venv needed - only standard libraries)
exec /usr/bin/python3 /usr/local/bin/wsprdaemon_reflector.py \
    --config "$CONFIG_FILE" \
    --log-file "$LOG_FILE" \
    --log-max-mb "$LOG_MAX_MB" \
    --verbose "$VERBOSITY"
