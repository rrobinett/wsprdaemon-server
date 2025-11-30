#!/bin/bash
#
# wsprnet_scraper.sh - Wrapper script for WSPRNET Scraper
# Version: 4.1
# Date: 2025-11-29
# Changes: Fixed argument names to match wsprnet_scraper.py argparse
#          Added missing --session-file, --setup-readonly-user, --setup-readonly-password

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
    "CLICKHOUSE_WSPRNET_READONLY_USER"
    "CLICKHOUSE_WSPRNET_READONLY_PASSWORD"
    "WSPRNET_USERNAME"
    "WSPRNET_PASSWORD"
    "VENV_PYTHON"
    "SCRAPER_SCRIPT"
    "LOG_FILE"
    "LOOP_INTERVAL"
    "WSPRNET_CACHE_DIR"
    "SESSION_FILE"
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
    echo "ERROR: Scraper script not found: ${SCRAPER_SCRIPT}" >&2
    exit 1
fi

mkdir -p "$(dirname "${LOG_FILE}")"
mkdir -p "$(dirname "${SESSION_FILE}")"
mkdir -p "${WSPRNET_CACHE_DIR}"

echo "Starting WSPRNET Scraper..."
echo "Python: ${VENV_PYTHON}"
echo "Script: ${SCRAPER_SCRIPT}"
echo "Log: ${LOG_FILE}"
echo "Session: ${SESSION_FILE}"
echo "Loop interval: ${LOOP_INTERVAL} seconds"
echo "Cache dir: ${WSPRNET_CACHE_DIR}"

# Note: Python script uses --username/--password (not --wsprnet-user/--wsprnet-password)
# Uses root admin to create wsprnet-admin and wsprnet-reader users
exec "${VENV_PYTHON}" "${SCRAPER_SCRIPT}" \
    --session-file "${SESSION_FILE}" \
    --username "${WSPRNET_USERNAME}" \
    --password "${WSPRNET_PASSWORD}" \
    --clickhouse-user "${CLICKHOUSE_ROOT_ADMIN_USER}" \
    --clickhouse-password "${CLICKHOUSE_ROOT_ADMIN_PASSWORD}" \
    --setup-readonly-user "${CLICKHOUSE_WSPRNET_READONLY_USER}" \
    --setup-readonly-password "${CLICKHOUSE_WSPRNET_READONLY_PASSWORD}" \
    --log-file "${LOG_FILE}" \
    --log-max-mb "${LOG_MAX_MB:-10}" \
    --loop "${LOOP_INTERVAL}" \
    --verbose "${VERBOSITY:-1}" \
    --cache-dir "${WSPRNET_CACHE_DIR}"
