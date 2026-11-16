"""DDL for the fleet database: the station map, the alarm ledger, and acknowledgements.

Every statement is idempotent, so the CLI can run them all on each start.
alarm is a ReplacingMergeTree on updated_at: a check rewrites its row as it changes
state, and the newest row per (station, check, subject, first_seen) wins.
"""
from __future__ import annotations

DATABASE = "fleet"


def statements() -> list[str]:
    return [
        "CREATE DATABASE IF NOT EXISTS fleet",
        "CREATE TABLE IF NOT EXISTS fleet.station_map ("
        "name String, grid6 String, source LowCardinality(String), reporter String, "
        "is_alias UInt8, expect_add Array(String), expect_drop Array(String), "
        "loaded_at DateTime) "
        "ENGINE = MergeTree ORDER BY (name, source, reporter)",
        "CREATE TABLE IF NOT EXISTS fleet.alarm ("
        "station String, check LowCardinality(String), subject String, "
        "first_seen DateTime, last_seen DateTime, state LowCardinality(String), "
        "false_streak UInt8, numbers String, updated_at DateTime) "
        "ENGINE = ReplacingMergeTree(updated_at) "
        "ORDER BY (station, check, subject, first_seen)",
        "CREATE TABLE IF NOT EXISTS fleet.alarm_ack ("
        "station String, check String, subject String, first_seen DateTime, "
        "acked_by String, note String, acked_at DateTime) "
        "ENGINE = MergeTree ORDER BY (station, check, subject, first_seen, acked_at)",
    ]
