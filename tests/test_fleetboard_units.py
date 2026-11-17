"""fleetboard.service mirrors sigmondb-publish.service; the timer fires every 5 minutes."""
import configparser
import os

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))


def _unit(fn):
    cp = configparser.ConfigParser(strict=False, interpolation=None)
    cp.optionxform = str
    with open(os.path.join(ROOT, fn)) as f:
        cp.read_file(f)
    return cp


def test_service_mirrors_publish_unit():
    s, p = _unit("fleetboard.service")["Service"], _unit("sigmondb-publish.service")["Service"]
    for k in ("User", "Group", "WorkingDirectory", "EnvironmentFile", "NoNewPrivileges"):
        assert s[k] == p[k]
    assert s["ExecStart"] == ("/opt/wsprdaemon-server/venv/bin/python -m fleetboard run "
                              "--conf /etc/wsprdaemon/clickhouse.conf")
    assert s["SyslogIdentifier"] == "fleetboard" and s["TimeoutStartSec"] == "4min"


def test_timer():
    t = _unit("fleetboard.timer")
    assert t["Timer"]["OnCalendar"] == "*:0/5" and t["Timer"]["Persistent"] == "true"
    assert "RandomizedDelaySec" not in t["Timer"]
    assert t["Install"]["WantedBy"] == "timers.target"
