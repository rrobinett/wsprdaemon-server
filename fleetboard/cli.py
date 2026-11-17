"""python -m fleetboard: ddl, load-map, run.

run is the 5-minute job: read the sources, judge every station, move the alarms,
write the changed alarms to fleet.alarm on this host, then replace the two public
tables on each host in CH_PUB_HOSTS.  Credentials for this host come from
--conf; the public servers' come from the environment (CH_PUB_USER,
CH_PUB_PASSWORD, CH_PUB_HOSTS), which the unit's EnvironmentFile= sets.
"""
import argparse
import json
import os
import sys
from datetime import datetime, timedelta, timezone

from fleetboard import alarms as al
from fleetboard import board, ddl, sources
from fleetboard.checks import evaluate
from fleetboard.stations import fold, load_map
from sigmondb import publish
from sigmondb.cli import connect

DEFAULT_CONF = "/etc/wsprdaemon/clickhouse.conf"
ALARM_COLS = ["station", "check", "subject", "first_seen", "last_seen", "state",
              "false_streak", "numbers", "updated_at"]
ACK_COLS = ["station", "check", "subject", "first_seen", "acked_by", "note", "acked_at"]
MAP_COLS = ["name", "grid6", "source", "reporter", "is_alias", "expect_add",
            "expect_drop", "loaded_at"]
ACK_DAYS = 30


def _naive(t):
    return sources._naive_utc(t)


def _alarm(r):
    r = dict(zip(ALARM_COLS, r))
    for k in ("first_seen", "last_seen", "updated_at"):
        r[k] = _naive(r[k])
    return al.Alarm(**r)


def _ack(r):
    r = dict(zip(ACK_COLS, r))
    for k in ("first_seen", "acked_at"):
        r[k] = _naive(r[k])
    return al.Ack(**r)


def read_map(client):
    cols = ["name", "grid6", "source", "reporter", "expect_add", "expect_drop"]
    res = client.query("SELECT %s FROM fleet.station_map" % ", ".join(cols))
    return load_map([dict(zip(cols, r)) for r in res.result_rows])


def read_alarms(client, now):
    since = now - timedelta(days=sources.BASELINE_DAYS + 1)
    res = client.query(
        "SELECT %s FROM fleet.alarm FINAL WHERE state != 'cleared' OR last_seen > %s"
        % (", ".join(ALARM_COLS), sources._lit(since)))
    return [_alarm(r) for r in res.result_rows]


def read_acks(client, now):
    res = client.query("SELECT %s FROM fleet.alarm_ack WHERE acked_at > %s"
                       % (", ".join(ACK_COLS), sources._lit(now - timedelta(days=ACK_DAYS))))
    return [_ack(r) for r in res.result_rows]


def cmd_ddl(a):
    for s in ddl.statements():
        print(s + ";")
    return 0


def cmd_load_map(a):
    now = datetime.now(timezone.utc).replace(tzinfo=None, microsecond=0)
    rows = []
    for line in sys.stdin:
        if not line.strip():
            continue
        r = json.loads(line)
        rows.append([r["name"], r["grid6"], r["source"], r["reporter"],
                     int(r.get("is_alias", 0)), list(r.get("expect_add") or ()),
                     list(r.get("expect_drop") or ()), now])
    client = connect(a.conf)
    for s in ddl.statements():
        client.command(s)
    client.command("TRUNCATE TABLE fleet.station_map")
    if rows:
        client.insert("fleet.station_map", rows, column_names=MAP_COLS)
    print("station_map: %d rows" % len(rows))
    return 0


def cmd_run(a):
    user, password = os.environ.get("CH_PUB_USER"), os.environ.get("CH_PUB_PASSWORD")
    hosts = (os.environ.get("CH_PUB_HOSTS") or "").split()
    if not a.no_publish and not (user and password and hosts):
        print("run: CH_PUB_USER, CH_PUB_PASSWORD and CH_PUB_HOSTS must all be set "
              "(or pass --no-publish)", file=sys.stderr)
        return 2
    now = datetime.now(timezone.utc).replace(tzinfo=None, microsecond=0)
    client = connect(a.conf)
    for s in ddl.statements():
        client.command(s)
    stations = read_map(client)
    acts = fold(sources.fetch(client, now), stations)
    prev = read_alarms(client, now)
    acks = read_acks(client, now)
    since = now - timedelta(days=sources.BASELINE_DAYS + 1)
    hours = al.alarm_hours(prev, since)
    findings = evaluate(stations, acts, now, hours)
    changed = al.step(prev, findings, acks, now)
    if changed:
        client.insert("fleet.alarm",
                      [[getattr(c, k) for k in ALARM_COLS] for c in changed],
                      column_names=ALARM_COLS)
    print("run: %d stations, %d findings, %d alarms changed"
          % (len(stations), len(findings), len(changed)), flush=True)
    merged = {(x.station, x.check, x.subject): x for x in prev if x.state in al.LIVE}
    for c in changed:
        key = (c.station, c.check, c.subject)
        if c.state in al.LIVE:
            merged[key] = c
        else:
            merged.pop(key, None)
    if a.no_publish:
        return 0
    tables = board.build(stations, acts, list(merged.values()), findings,
                         publish.now_utc(), excluded_hours=hours, now=now)
    fails = publish.publish(tables, hosts, user, password, timeout=a.timeout,
                            out=lambda s: print(s, flush=True), names=tuple(board.SCHEMAS),
                            schemas=board.SCHEMAS, database=ddl.DATABASE)
    if fails:
        print("run: %d of %d host+table replacements FAILED"
              % (len(fails), len(hosts) * len(board.SCHEMAS)), file=sys.stderr)
        return 1
    return 0


def build_parser():
    p = argparse.ArgumentParser(prog="python -m fleetboard")
    sub = p.add_subparsers(dest="verb", required=True)
    sub.add_parser("ddl", help="print the schema SQL").set_defaults(fn=cmd_ddl)
    s = sub.add_parser("load-map", help="JSON Lines on stdin replace fleet.station_map")
    s.add_argument("--conf", default=DEFAULT_CONF)
    s.set_defaults(fn=cmd_load_map)
    s = sub.add_parser("run", help="check the stations, move the alarms, publish the board")
    s.add_argument("--conf", default=DEFAULT_CONF)
    s.add_argument("--no-publish", action="store_true",
                   help="write the alarms on this host and publish nothing")
    s.add_argument("--timeout", type=float, default=30.0, help="seconds per HTTP request")
    s.set_defaults(fn=cmd_run)
    return p


def main(argv=None):
    a = build_parser().parse_args(argv)
    return a.fn(a)


if __name__ == "__main__":
    sys.exit(main())
