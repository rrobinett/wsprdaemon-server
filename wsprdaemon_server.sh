#!/bin/bash
#
# wsprdaemon_server.sh - Wrapper script for WSPRDAEMON Server
# Version: 4.1
# Date: 2025-11-29
# Changes: Uses root admin credentials for initial setup

set -e

CONFIG_FILE="$1"
if [[ -z "${CONFIG_FILE}" ]] || [[ ! -f "${CONFIG_FILE}" ]]; then
    echo "ERROR: Config file not found: ${CONFIG_FILE}" >&2
    echo "Usage: $0 /path/to/config.conf" >&2
    exit 1
fi

echo "Loading configuration from: ${CONFIG_FILE}"
source "${CONFIG_FILE}"

if [[ -f /etc/wsprdaemon/clickhouse.conf ]]; then
    source /etc/wsprdaemon/clickhouse.conf
else
    echo "ERROR: ClickHouse config not found: /etc/wsprdaemon/clickhouse.conf" >&2
    exit 1
fi

required_vars=(
    "CLICKHOUSE_ROOT_ADMIN_USER"
    "CLICKHOUSE_ROOT_ADMIN_PASSWORD"
    "CLICKHOUSE_WSPRDAEMON_READONLY_USER"
    "CLICKHOUSE_WSPRDAEMON_READONLY_PASSWORD"
    "VENV_PYTHON"
    "SCRAPER_SCRIPT"
    "LOG_FILE"
    "LOOP_INTERVAL"
    "INCOMING_DIRS"
)

missing_vars=()
for var in "${required_vars[@]}"; do
    if [[ -z "${!var+x}" ]] || [[ -z "${!var}" ]]; then
        missing_vars+=("  $var")
    fi
done

if [[ ${#missing_vars[@]} -gt 0 ]]; then
    echo "ERROR: Missing required configuration variables:" >&2
    printf '%s\n' "${missing_vars[@]}" >&2
    exit 1
fi

if [[ ! -x "${VENV_PYTHON}" ]]; then
    echo "ERROR: Python executable not found: ${VENV_PYTHON}" >&2
    exit 1
fi

if [[ ! -f "${SCRAPER_SCRIPT}" ]]; then
    echo "ERROR: Server script not found: ${SCRAPER_SCRIPT}" >&2
    exit 1
fi

mkdir -p "$(dirname "${LOG_FILE}")"

echo "Starting WSPRDAEMON Server with config: ${CONFIG_FILE}"
echo "Python: ${VENV_PYTHON}"
echo "Script: ${SCRAPER_SCRIPT}"
echo "Log: ${LOG_FILE}"
echo "Loop interval: ${LOOP_INTERVAL} seconds"
echo "Incoming dirs: ${INCOMING_DIRS}"

exec "${VENV_PYTHON}" "${SCRAPER_SCRIPT}" \
    --clickhouse-user "${CLICKHOUSE_ROOT_ADMIN_USER}" \
    --clickhouse-password "${CLICKHOUSE_ROOT_ADMIN_PASSWORD}" \
    --log-file "${LOG_FILE}" \
    --log-max-mb "${LOG_MAX_MB:-10}" \
    --loop "${LOOP_INTERVAL}" \
    --verbose "${VERBOSITY:-1}" \
    --incoming-dirs "${INCOMING_DIRS}"
