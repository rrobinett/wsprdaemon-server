"""python -m sigmondb: ddl, seed, backfill-codar, backfill-hfdl, load, solve.

Credentials come from /etc/wsprdaemon/clickhouse.conf (--conf overrides the path in tests).
No verb takes a password.  clickhouse_connect is imported only where a verb needs it.
"""
import argparse
import re
import sys
from datetime import date
from pathlib import Path

from sigmondb import ddl
from sigmondb.groundwave import fixed_clusters, ground_wave_solutions
from sigmondb.load import insert_file
from sigmondb.rows import read_rows, write_rows

DEFAULT_CONF = "/etc/wsprdaemon/clickhouse.conf"
SOLVER_VERSION = "groundwave-1"
_LINE = re.compile(r'^\s*(?:export\s+)?([A-Z_][A-Z0-9_]*)=(.*?)\s*$')


def read_conf(path):
    out = {}
    for line in Path(path).read_text().splitlines():
        m = _LINE.match(line)
        if not m:
            continue
        v = m.group(2)
        if len(v) >= 2 and v[0] == v[-1] and v[0] in "\"'":
            v = v[1:-1]
        out[m.group(1)] = v
    return out


def connect(conf_path):
    import clickhouse_connect
    c = read_conf(conf_path)
    return clickhouse_connect.get_client(
        host=c["CLICKHOUSE_HOST"], port=int(c["CLICKHOUSE_PORT"]),
        username=c["CLICKHOUSE_ROOT_ADMIN_USER"], password=c["CLICKHOUSE_ROOT_ADMIN_PASSWORD"])


def _out_dir(p):
    d = Path(p)
    d.mkdir(parents=True, exist_ok=True)
    return d


def _write(d, table, rows):
    n = write_rows(d / (table + ".jsonl"), rows)
    print("%s: %d rows" % (table, n))


def cmd_ddl(a):
    print(ddl.sql_text())
    return 0


def cmd_seed(a):
    from sigmondb.hfdl_gs import parse_systable
    from sigmondb.hfrnet import parse_hfrnet
    from sigmondb.receivers import load_receivers
    snap = date.fromisoformat(a.snapshot)
    d = _out_dir(a.out)
    tx, wf = [], []
    if a.hfrnet:
        t, w = parse_hfrnet(a.hfrnet, snap)
        tx += t
        wf += w
    if a.systable:
        t, w = parse_systable(a.systable, snap)
        tx += t
        wf += w
    if a.hfrnet or a.systable:
        _write(d, "transmitter", tx)
        _write(d, "waveform", wf)
    if a.receivers:
        _write(d, "receiver", load_receivers(a.receivers))
    return 0


def cmd_backfill_codar(a):
    from sigmondb import backfill_codar as bc
    from sigmondb.provenance import load_minutes
    root, caps, arrs = Path(a.root), [], []
    for s in bc.SETS:
        minutes = load_minutes([root / m for m in s["minutes"] if (root / m).exists()])
        if s["kind"] == "tsv":
            centre, rate = bc.EXACT_GROUPS[s["group"]]
            c, r = bc.read_t6_tsv(root / s["path"], root / s["capture_dir"], s["reporter_id"],
                                  centre, rate, bc.PERIOD_4463_S, minutes)
        else:
            c, r = bc.read_res_jsonl(root / s["path"], s["reporter_id"], minutes)
        caps += c
        arrs += r
    d = _out_dir(a.out)
    _write(d, "capture", caps)
    _write(d, "arrival", arrs)
    return 0


def cmd_backfill_hfdl(a):
    from sigmondb import backfill_hfdl as bh
    root, caps, arrs = Path(a.root), [], []
    for s in bh.SETS:
        c, r = bh.read_squitters(root / s["path"], s["reporter_id"])
        caps += c
        arrs += r
    d = _out_dir(a.out)
    _write(d, "capture", caps)
    _write(d, "arrival", arrs)
    return 0


def cmd_load(a):
    d = Path(a.dir)
    client = connect(a.conf)
    ddl.ensure_schema(client)
    for table in ddl._SPEC:
        p = d / (table + ".jsonl")
        if p.exists():
            print("%s: %d rows" % (table, insert_file(client, table, p)))
    return 0


def _from_db(a):
    client = connect(a.conf)
    def fetch(table, where=""):
        cols = list(ddl.COLUMNS[table])
        res = client.query("SELECT %s FROM %s.%s %s" % (", ".join(cols), ddl.DATABASE, table,
                                                       where))
        return [dict(zip(cols, r)) for r in res.result_rows]
    arr = fetch("arrival", "WHERE u_epoch_ns IS NOT NULL AND u_epoch_ns <= 1000")
    for r in arr:  # clickhouse returns tz-aware UTC datetimes; the solver uses naive UTC
        if r["time"].tzinfo is not None:
            r["time"] = r["time"].replace(tzinfo=None)
    return arr, fetch("transmitter"), fetch("receiver")


def cmd_solve(a):
    if a.from_db:
        arr, tx, rx = _from_db(a)
    else:
        arr = read_rows(a.arrivals)
        tx = read_rows(a.transmitters) if a.transmitters else []
        rx = read_rows(a.receivers) if a.receivers else []
    sols = ground_wave_solutions(fixed_clusters(arr), tx, rx, SOLVER_VERSION)
    n = write_rows(a.out, sols)
    print("solution: %d rows" % n)
    return 0


def build_parser():
    p = argparse.ArgumentParser(prog="python -m sigmondb")
    sub = p.add_subparsers(dest="verb", required=True)
    sub.add_parser("ddl", help="print the schema SQL").set_defaults(fn=cmd_ddl)
    s = sub.add_parser("seed", help="write transmitter, waveform and receiver rows")
    s.add_argument("--hfrnet")
    s.add_argument("--systable")
    s.add_argument("--receivers")
    s.add_argument("--snapshot", required=True, help="YYYY-MM-DD")
    s.add_argument("--out", required=True)
    s.set_defaults(fn=cmd_seed)
    for name, fn in (("backfill-codar", cmd_backfill_codar), ("backfill-hfdl", cmd_backfill_hfdl)):
        s = sub.add_parser(name)
        s.add_argument("--root", required=True, help="the hamsci root holding tasks/")
        s.add_argument("--out", required=True)
        s.set_defaults(fn=fn)
    s = sub.add_parser("load", help="insert every <table>.jsonl in DIR")
    s.add_argument("--dir", required=True)
    s.add_argument("--conf", default=DEFAULT_CONF)
    s.set_defaults(fn=cmd_load)
    s = sub.add_parser("solve")
    s.add_argument("--out", required=True)
    g = s.add_mutually_exclusive_group(required=True)
    g.add_argument("--from-db", action="store_true")
    g.add_argument("--arrivals")
    s.add_argument("--transmitters", help="with --arrivals: transmitter.jsonl")
    s.add_argument("--receivers", help="with --arrivals: receiver.jsonl")
    s.add_argument("--conf", default=DEFAULT_CONF)
    s.set_defaults(fn=cmd_solve)
    return p


def main(argv=None):
    a = build_parser().parse_args(argv)
    return a.fn(a)


if __name__ == "__main__":
    sys.exit(main())
