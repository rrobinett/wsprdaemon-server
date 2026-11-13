"""The hourly chain: publish pulls solve pulls attribute, in that order."""
import configparser
import os

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))


def _unit(name):
    cp = configparser.ConfigParser(strict=False, interpolation=None)
    cp.optionxform = str
    with open(os.path.join(ROOT, f"{name}.service")) as f:
        cp.read_file(f)
    return cp


def test_publish_pulls_solve_pulls_attribute_in_order():
    pub, sol, att = (_unit(n) for n in ("sigmondb-publish", "sigmondb-solve", "sigmondb-attribute"))
    assert "sigmondb-solve.service" in pub["Unit"]["Wants"] and "sigmondb-solve.service" in pub["Unit"]["After"]
    assert "sigmondb-attribute.service" in sol["Unit"]["Wants"] and "sigmondb-attribute.service" in sol["Unit"]["After"]
    assert "Requires" not in pub["Unit"] and "Requires" not in sol["Unit"]
    assert sol["Service"]["ExecStart"].endswith("solve --from-db --write-db")
    assert att["Service"]["ExecStart"].endswith("-m sigmondb attribute")
    assert not os.path.exists(os.path.join(ROOT, "sigmondb-attribute.timer"))


def test_new_units_match_the_brief():
    for n, tmo in (("sigmondb-attribute", "30min"), ("sigmondb-solve", "15min")):
        u = _unit(n)
        s = u["Service"]
        assert s["Type"] == "oneshot" and s["User"] == "wsprdaemon" and s["Group"] == "wsprdaemon"
        assert s["WorkingDirectory"] == "/home/wsprdaemon/wsprdaemon-server"
        assert s["ExecStart"].startswith("/opt/wsprdaemon-server/venv/bin/python -m sigmondb ")
        assert s["TimeoutStartSec"] == tmo and s["NoNewPrivileges"] == "true"
        assert s["SyslogIdentifier"] == n
        assert s["Nice"] == "10" and s["IOSchedulingClass"] == "idle"     # gentle on wd30
        assert "clickhouse-server.service" in u["Unit"]["After"]
