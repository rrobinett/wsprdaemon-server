"""Tests for fleetboard.ddl."""
from __future__ import annotations

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from fleetboard import ddl  # noqa: E402


def _find(name):
    return next(s for s in ddl.statements() if "fleet.%s (" % name in s)


def test_ddl_alarm_is_replacing_on_updated_at():
    s = _find("alarm")
    assert "ReplacingMergeTree(updated_at)" in s
    assert "ORDER BY (station, check, subject, first_seen)" in s


def test_ddl_is_rerunnable_and_complete():
    sts = ddl.statements()
    assert sts[0] == "CREATE DATABASE IF NOT EXISTS fleet"
    assert all("IF NOT EXISTS" in s for s in sts)
    assert len(sts) == 4
    assert "ORDER BY (name, source, reporter)" in _find("station_map")
    assert "acked_at" in _find("alarm_ack")


def test_replace_table_takes_a_schema_and_database():
    from sigmondb import publish

    class Rec:
        def __init__(self):
            self.sql = []

        def run(self, sql, data=None):
            self.sql.append(sql)
            return "0"

    ch = Rec()
    publish.replace_table(ch, "fleet_board", [{"station": "A"}],
                          schema=[("station", "String")], database="sigmondb_public")
    assert ch.sql[1].startswith(
        "CREATE TABLE sigmondb_public.fleet_board__new (`station` String)")
    assert any("RENAME TABLE sigmondb_public.fleet_board__new TO "
               "sigmondb_public.fleet_board" in s for s in ch.sql)
