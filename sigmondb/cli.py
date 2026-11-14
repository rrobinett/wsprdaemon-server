"""python -m sigmondb: ddl, seed, backfill-codar, backfill-hfdl, load, attribute, receiver-positions, solve, publish.

Credentials come from /etc/wsprdaemon/clickhouse.conf (--conf overrides the path in tests).
No verb takes a password.  publish reads the public servers' credentials from the
environment (CH_PUB_USER, CH_PUB_PASSWORD, CH_PUB_HOSTS), which its unit's
EnvironmentFile= sets.  clickhouse_connect is imported only where a verb needs it.
"""
import argparse
import re
import sys
from datetime import date, datetime, timezone
from pathlib import Path

from sigmondb import ddl
from sigmondb.groundwave import fixed_clusters, ground_wave_solutions
from sigmondb.load import insert_file, insert_rows
from sigmondb.receivers import DEFAULT_POSITIONS
from sigmondb.rows import read_rows, write_rows

DEFAULT_CONF = "/etc/wsprdaemon/clickhouse.conf"
SOLVER_VERSION = "groundwave-3"
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


def _write(d, table, rows, source=None):
    """seed writes <table>.jsonl; a back-fill writes <table>.<source>.jsonl."""
    name = table + ("." + source if source else "") + ".jsonl"
    n = write_rows(d / name, rows)
    print("%s: %d rows" % (name, n))


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
    _write(d, "capture", caps, "codar")
    _write(d, "arrival", arrs, "codar")
    return 0


def cmd_backfill_hfdl(a):
    from sigmondb import backfill_hfdl as bh
    root, caps, arrs = Path(a.root), [], []
    for s in bh.SETS:
        c, r = bh.read_squitters(root / s["path"], s["reporter_id"])
        caps += c
        arrs += r
    d = _out_dir(a.out)
    _write(d, "capture", caps, "hfdl")
    _write(d, "arrival", arrs, "hfdl")
    return 0


def _table_files(d, table):
    """<table>.jsonl and <table>.*.jsonl in d, sorted by name."""
    return sorted(set(d.glob(table + ".jsonl")) | set(d.glob(table + ".*.jsonl")),
                  key=lambda p: p.name)


def cmd_load(a):
    d = Path(a.dir)
    client = connect(a.conf)
    ddl.ensure_schema(client)
    for table in ddl.TABLE_ORDER:
        for p in _table_files(d, table):
            print("%s: %d rows" % (p.name, insert_file(client, table, p)))
    return 0


def _latest_detector(arr):
    """Keep, per (reporter_id, group_key, time), only the rows of the latest detector_version.
    Keyed on the capture time too: different captures keep their own version, so live and
    back-fill rows of one group both count.  Within one capture "latest" is the largest version
    string in lexical order: right for the dated names in use (offline-2026-11 < offline-2026-12),
    wrong for names that do not sort by date."""
    best = {}
    for r in arr:
        k = (r["reporter_id"], r["group_key"], r["time"])
        if k not in best or r["detector_version"] > best[k]:
            best[k] = r["detector_version"]
    return [r for r in arr if r["detector_version"] == best[(r["reporter_id"], r["group_key"], r["time"])]]


def _utc_naive(t):
    """The solver works in naive UTC.  A tz-aware value is converted, never relabelled."""
    if t is not None and t.tzinfo is not None:
        return t.astimezone(timezone.utc).replace(tzinfo=None)
    return t


def _fetch(client, table, cols, where="", params=None):
    """SELECT cols FROM sigmondb.<table> FINAL <where>, as dicts, every time naive UTC.
    FINAL so a re-ingested row counts once."""
    cols = list(cols)
    sql = "SELECT %s FROM %s.%s FINAL %s" % (", ".join(cols), ddl.DATABASE, table, where)
    try:
        res = client.query(sql, parameters=params, query_tz="UTC")
    except TypeError:   # clickhouse-connect without query_tz: the conversion below still holds
        res = client.query(sql, parameters=params)
    out = [dict(zip(cols, r)) for r in res.result_rows]
    for r in out:
        for c in ("time", "valid_from", "window_from", "window_to", "run_at"):
            if c in r:
                r[c] = _utc_naive(r[c])
    return out


def _latest(rows, key):
    """The latest row per rows[key], by (valid_from, version); first-seen order kept."""
    best = {}
    for r in rows:
        k = r[key]
        rank = (r.get("valid_from") or datetime.min, r.get("version") or 0)
        if k not in best or rank > best[k][0]:
            best[k] = (rank, r)
    return [r for _, r in best.values()]


TRANSPORTERS = Path(__file__).parent / "seed" / "transporters.toml"


def load_transporters(path=TRANSPORTERS):
    """radiod_id -> [reporter ids allowed to carry that radiod's receptions], from the seed."""
    import tomllib
    with open(path, "rb") as fh:
        doc = tomllib.load(fh)
    return {k: list(v) for k, v in doc.get("transporters", {}).items()}


def _at_radiod(arrivals, receivers, transporters):
    """Count a carried arrival at the receiver whose radiod heard it.

    An arrival takes the reporter_id of the receiver row whose radiod_id equals its
    engineering['radiod'] only when its own reporter is a listed transporter for that radiod
    (seed/transporters.toml); it keeps its old label as carried_by.  A transporter such as
    KFS-MAIN runs no radiod (Michael, 11-13), so its rows count at KFS-OMNI, never as a second
    station.  engineering['radiod'] comes from the station, so any other reporter keeps its
    own reporter_id: naming another station's radiod must not fake a second station.  A
    radiod_id that two receivers share relabels nothing, and says so on stderr.
    In memory only: the rows in the database keep their labels.  Returns new dicts."""
    owners = {}
    for r in receivers:
        if r.get("radiod_id"):
            owners.setdefault(r["radiod_id"], set()).add(r["reporter_id"])
    by_radiod = {}
    for rd, rids in sorted(owners.items()):
        if len(rids) > 1:
            print("radiod %s belongs to %d receivers (%s); its arrivals keep their own labels"
                  % (rd, len(rids), ", ".join(sorted(rids))), file=sys.stderr)
        else:
            by_radiod[rd] = next(iter(rids))
    out = []
    for a in arrivals:
        rd = (a.get("engineering") or {}).get("radiod")
        rid = by_radiod.get(rd)
        if rid and rid != a["reporter_id"] and a["reporter_id"] in transporters.get(rd, ()):
            a = dict(a, reporter_id=rid, carried_by=a["reporter_id"])
        out.append(a)
    return out


def _one_per_capture(arrivals):
    """Keep the first row per (reporter_id, group_key, time, delay to the microsecond), the
    arrival table's own grain (intDiv truncates toward zero).  After _at_radiod, one capture
    carried under two labels (KFS-MAIN and KFS/MAIN) would otherwise count twice, and skew a
    cluster's n, median and MAD.  Returns (kept, dropped count)."""
    seen, kept = set(), []
    for a in arrivals:
        d = a["delay_ns"]
        k = (a["reporter_id"], a["group_key"], a["time"], -(-d // 1000) if d < 0 else d // 1000)
        if k not in seen:
            seen.add(k)
            kept.append(a)
    return kept, len(arrivals) - len(kept)


def _carried(arr, rx, latest_detector=True):
    """Count each arrival at its radiod (_at_radiod), THEN keep the latest detector per capture,
    THEN one row per capture.  In that order: judged on the old labels, one capture carried
    under KFS-MAIN and KFS/MAIN by two detector versions would keep both rows and reach
    KFS-OMNI twice.  Returns (arrivals, duplicates dropped)."""
    arr = _at_radiod(arr, rx, load_transporters())
    if latest_detector:
        arr = _latest_detector(arr)
    return _one_per_capture(arr)


def _hours(days):
    """A day count as whole hours, for an integer INTERVAL."""
    return int(round(float(days) * 24))


def _load_positions(a):
    """The private precise positions for attribute and solve, or None when the file is absent.
    A group- or other-readable file raises ValueError, so the run stops before the database.
    Prints a count, never a value."""
    import os
    from sigmondb.receivers import load_positions
    path = os.path.expanduser(a.positions)
    if not os.path.exists(path):
        print("positions: no private file; using sigmondb.receiver as stored")
        return None
    return load_positions(path)


def _overlay_rows(rows, positions):
    """rows with the private positions laid over them, in memory only."""
    if positions is None:
        return rows
    from sigmondb.receivers import overlay_positions
    out = overlay_positions(rows, positions)
    n = len({r["reporter_id"] for r in rows if r.get("reporter_id") in positions})
    print("positions: %d receiver(s) at full precision from the private file" % n)
    return out


def _from_db(a, client=None, positions=None):
    """(arrivals, transmitters, receivers) from the registry, every table read FINAL so a
    re-ingested row counts once.  Arrivals: anchored only, within --days; by default the
    latest detector_version per (reporter_id, group_key, time), judged after each arrival
    counts at the receiver whose radiod heard it (_carried); --detector-version picks one."""
    client = client or connect(a.conf)

    def fetch(table, where="", params=None):
        return _fetch(client, table, ddl.COLUMNS[table], where, params)

    where = ("WHERE u_epoch_ns IS NOT NULL AND u_epoch_ns <= 1000 "
             "AND time > now() - INTERVAL {hours:UInt32} HOUR")
    params = {"hours": _hours(a.days)}
    if a.detector_version:
        arr = fetch("arrival", where + " AND detector_version = {dv:String}",
                    dict(params, dv=a.detector_version))
    else:
        arr = fetch("arrival", where, params)
    rx = _overlay_rows(fetch("receiver"), positions)
    arr, dropped = _carried(arr, _latest(rx, "reporter_id"),
                            latest_detector=not a.detector_version)
    if dropped:
        print("solve: %d duplicate carried arrivals dropped" % dropped, file=sys.stderr)
    return arr, fetch("transmitter"), rx


_ATTR_ARRIVAL_COLS = ("time", "reporter_id", "group_key", "delay_ns", "level_db",
                      "detector_version", "engineering")
_ATTR_WHERE = ("WHERE u_epoch_ns IS NOT NULL AND u_epoch_ns <= 1000 AND clean_tag = 'A' "
               "AND rate_hz_per_s != 0 AND time > now() - INTERVAL {hours:UInt32} HOUR")


def _attribution_inputs(client, days, positions=None):
    """(arrivals, sites, rxpos, coherence) for attribution.run_attribution.

    arrivals: T6-anchored, CLEAN 'A', chirped, within `days`; the latest detector per capture;
    live when engineering carries sweep_repetition_hz; counted at their radiod.
    sites: the latest codar transmitter row per tx_id with a position, one site per waveform
    (latest per centre); a transmitter with no waveform drops out.  Sorted by tx_id, so ties
    break the same way every run.
    rxpos: reporter_id -> (lat, lon, pos_source) from the latest receiver row with a position;
    coherence: reporter_id -> coherence_group for the same rows."""
    arr = _fetch(client, "arrival", _ATTR_ARRIVAL_COLS, _ATTR_WHERE, {"hours": _hours(days)})
    for r in arr:
        eng = r.get("engineering") or {}
        r["live"] = "sweep_repetition_hz" in eng
        if eng.get("sweep_repetition_hz"):
            r["sweep_repetition_hz"] = eng["sweep_repetition_hz"]
    tx = _latest(_fetch(client, "transmitter", ddl.COLUMNS["transmitter"],
                        "WHERE kind = 'codar' ORDER BY tx_id"), "tx_id")
    wf = _fetch(client, "waveform", ddl.COLUMNS["waveform"], "ORDER BY tx_id")
    # The latest row per (tx_id, centre) among rows WITH a period; a centre with none keeps its
    # latest row, period None: analyse_group skips it for the period yet keeps it as a rival.
    # Dropping it could turn an ambiguous fit into a name.
    keyed = [dict(w, _k=(w["tx_id"], w["centre_hz"])) for w in wf]
    best = {w["_k"]: w for w in _latest([w for w in keyed if not w.get("period_s")], "_k")}
    best.update((w["_k"], w) for w in _latest([w for w in keyed if w.get("period_s")], "_k"))
    wf_by = {}
    for w in best.values():
        wf_by.setdefault(w["tx_id"], []).append(w)
    noper = sorted({w["tx_id"] for w in best.values() if not w.get("period_s")})
    if noper:
        print("attribute: %d waveform(s) with no period_s kept as candidates: %s"
              % (len(noper), ", ".join(noper)))
    sites = []
    for t in sorted(tx, key=lambda t: t["tx_id"]):
        if t.get("lat") is None or t.get("lon") is None:
            continue
        for w in sorted(wf_by.get(t["tx_id"], []), key=lambda w: w["centre_hz"]):
            sites.append({"id": t["tx_id"], "lat": t["lat"], "lon": t["lon"],
                          "freq": w["centre_hz"], "rate": w["rate_hz_per_s"],
                          "period": w["period_s"]})
    rx = _overlay_rows(_fetch(client, "receiver", ddl.COLUMNS["receiver"]), positions)
    rx = _latest(rx, "reporter_id")
    placed = [r for r in rx if r.get("lat") is not None and r.get("lon") is not None]
    rxpos = {r["reporter_id"]: (r["lat"], r["lon"], r.get("pos_source") or "") for r in placed}
    coherence = {r["reporter_id"]: r.get("coherence_group") or "" for r in placed}
    arr, dropped = _carried(arr, rx)
    if dropped:
        print("attribute: %d duplicate carried arrivals dropped" % dropped)
    return arr, sites, rxpos, coherence


_NAMED = frozenset({"named", "named_two_stations"})
_FITS_GW900 = frozenset({"fits_gw900", "fits_gw900_two_stations"})


def _summary(policy_name, n_groups, rows):
    """attribute <policy>: <n> groups, <n> named, <n> ambiguous, <n> unattributable; the
    ground-wave-only pass adds its fits, which never carry a name."""
    count = lambda st: sum(1 for r in rows if r["status"] in st)  # noqa: E731
    line = "attribute %s: %d groups, %d named, %d ambiguous, %d unattributable" % (
        policy_name, n_groups, count(_NAMED), count({"ambiguous"}), count({"unattributable"}))
    if policy_name == "gw_only_900km":
        line += ", %d fits_gw900 (never named)" % count(_FITS_GW900)
    return line


def cmd_attribute(a):
    """Name the CODAR radar behind each sweep group's clusters, under the default path policy
    and then the ground-wave-only 900 km pass, and write every fit to sigmondb.attribution
    (or to --out as JSON Lines, leaving the database alone).  Exits 0 when nothing gets named;
    a database error propagates, so systemd records a failure."""
    from sigmondb import attribution as at
    positions = _load_positions(a)
    client = connect(a.conf)
    if not a.out:                       # --out leaves the database alone; so does reading
        ddl.ensure_schema(client)
    arr, sites, rxpos, coherence = _attribution_inputs(client, a.days, positions)
    if not arr:
        print("attribute: no arrivals in the last %g days; nothing written" % a.days)
        if a.out:
            write_rows(a.out, [])
        return 0
    run_at = datetime.now(timezone.utc).replace(tzinfo=None, microsecond=0)
    times = [r["time"] for r in arr]
    digest = at.inputs_digest(arr)
    rows = []
    for policy in (at.DEFAULT, at.GW900):
        groups = at.run_attribution(arr, sites, rxpos, policy, n_fake=a.n_fake,
                                    coherence=coherence)
        mine = at.attribution_rows(groups, policy, run_at, min(times), max(times), digest)
        print(_summary(policy.name, len(groups), mine))
        for r in mine:
            if r["status"] in _NAMED | _FITS_GW900:
                print("  %s %s %s: %d stations, offset %d ns (%s), p_null %s" % (
                    r["group_key"], r["tx_id"], r["status"], r["n_stations"], r["offset_ns"],
                    r["offset_from"], "n/a" if r["p_null"] is None else "%.3f" % r["p_null"]))
        rows += mine
    if a.out:
        print("attribution: %d rows to %s" % (write_rows(a.out, rows), a.out))
        return 0
    print("attribution: %d rows" % insert_rows(client, "attribution", rows))
    return 0


def cmd_receiver_positions(a):
    """Insert a new receiver version for each reporter whose GPSDO position the private file
    gives.  Prints counts and unknown ids; never a coordinate."""
    from sigmondb.receivers import load_positions, position_rows
    positions = load_positions(a.file)
    client = connect(a.conf)
    now = datetime.now(timezone.utc).replace(tzinfo=None, microsecond=0)
    rows, unknown = position_rows(_fetch(client, "receiver", ddl.COLUMNS["receiver"]),
                                  positions, now)
    if rows:
        ddl.ensure_schema(client)
        insert_rows(client, "receiver", rows)
    known = len(positions) - len(unknown)
    print("receiver-positions: %d updated, %d unchanged, %d unknown: %s"
          % (len(rows), known - len(rows), len(unknown), " ".join(unknown)))
    return 0


def _read_receivers(path):
    if str(path).endswith(".toml"):
        from sigmondb.receivers import load_receivers
        return load_receivers(path)
    return read_rows(path)


# The latest attribution run per (group_key, mode_policy), among runs no older than
# attribution.ATTRIB_MAX_AGE_H.  run_id hashes the inputs, so a run on changed inputs adds rows
# beside the old run's, and FINAL alone would keep both.  A group missing from every recent run
# yields no rows, so its name lapses instead of going stale.
_LATEST_ATTRIBUTION = (
    "WHERE (group_key, mode_policy, run_at) IN (SELECT group_key, mode_policy, max(run_at) "
    "FROM %s.attribution WHERE run_at > now() - INTERVAL {max_age_h:UInt32} HOUR "
    "GROUP BY group_key, mode_policy)" % ddl.DATABASE)


def _latest_attributions(client):
    """sigmondb.attribution rows of the latest recent run per (group_key, mode_policy); []
    when the table holds none.  apply_attribution picks the default-policy names among them
    and checks the age again."""
    from sigmondb.attribution import ATTRIB_MAX_AGE_H
    return _fetch(client, "attribution", ddl.COLUMNS["attribution"], _LATEST_ATTRIBUTION,
                  {"max_age_h": ATTRIB_MAX_AGE_H})


def cmd_solve(a):
    """Fixed ground-wave clusters -> sigmondb.solution rows (--write-db) or JSON Lines (--out).
    Names come only from default-policy attribution rows (attribution.apply_attribution);
    a cluster left unnamed gets no offset."""
    from sigmondb.attribution import apply_attribution
    client = None
    positions = _load_positions(a) if (a.from_db or a.receivers) else None
    if a.from_db:
        client = connect(a.conf)
        ddl.ensure_schema(client)
        arr, tx, rx = _from_db(a, client, positions)
        attrs = [] if a.no_attribution else _latest_attributions(client)
    else:
        arr = read_rows(a.arrivals)
        tx = read_rows(a.transmitters) if a.transmitters else []
        rx = _overlay_rows(_read_receivers(a.receivers), positions) if a.receivers else []
        attrs = read_rows(a.attributions) if a.attributions else []
    aside = {}
    clusters = fixed_clusters(arr, rx, max_sd_ns=a.max_sd_us * 1000.0, set_aside=aside)
    for rid, why in sorted(aside.items()):
        print("set aside %s: %s" % (rid, why), file=sys.stderr)
    station_named = {c["cluster_key"] for c in clusters if any(c.get("tx_ids") or [])}
    for key, why in sorted(apply_attribution(clusters, attrs).items()):
        print("attribution not applied %s: %s" % (key, why), file=sys.stderr)
    attributed = sum(1 for c in clusters
                     if any(c.get("tx_ids") or []) and c["cluster_key"] not in station_named)
    sols = ground_wave_solutions(clusters, tx, rx, SOLVER_VERSION,
                                 max_ground_km=a.max_ground_km)
    print("solve: %d clusters, %d station-named, %d attributed"
          % (len(clusters), len(station_named), attributed))
    if a.out:
        print("solution: %d rows" % write_rows(a.out, sols))
        return 0
    client = client or connect(a.conf)
    ddl.ensure_schema(client)
    print("solution: %d rows" % insert_rows(client, "solution", sols))
    return 0


def cmd_publish(a):
    """Build sigmondb_public from this host's sigmondb and replace it on every public host.
    Exit 1 when any host+table failed, 2 when the environment lacks credentials."""
    import os
    from sigmondb import publish
    user, password = os.environ.get("CH_PUB_USER"), os.environ.get("CH_PUB_PASSWORD")
    hosts = (os.environ.get("CH_PUB_HOSTS") or "").split()
    if not (user and password and hosts):
        print("publish: CH_PUB_USER, CH_PUB_PASSWORD and CH_PUB_HOSTS must all be set",
              file=sys.stderr)
        return 2
    built_at = publish.now_utc()
    tables = publish.build_all(publish.read_source(connect(a.conf)), built_at)
    fails = publish.publish(tables, hosts, user, password, timeout=a.timeout,
                            out=lambda s: print(s, flush=True))
    if fails:
        print("publish: %d of %d host+table replacements FAILED"
              % (len(fails), len(hosts) * len(publish.TABLES)), file=sys.stderr)
        return 1
    return 0


def build_parser():
    p = argparse.ArgumentParser(prog="python -m sigmondb")
    sub = p.add_subparsers(dest="verb", required=True)
    sub.add_parser("ddl", help="print the schema SQL; it creates nothing (load creates the schema)",
                   description="Print the schema SQL to stdout.  This verb creates nothing: "
                               "`load` creates the database and tables before it inserts.").set_defaults(fn=cmd_ddl)
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
    s = sub.add_parser("load", help="insert every <table>.jsonl and <table>.*.jsonl in DIR; creates the schema first")
    s.add_argument("--dir", required=True)
    s.add_argument("--conf", default=DEFAULT_CONF)
    s.set_defaults(fn=cmd_load)
    s = sub.add_parser("attribute",
                       help="name the CODAR radar behind each cluster; write sigmondb.attribution")
    s.add_argument("--days", type=float, default=30.0,
                   help="arrivals from the last DAYS days (default 30)")
    s.add_argument("--n-fake", type=int, default=400,
                   help="fake sites per group for the null test (default 400)")
    s.add_argument("--out", help="write JSON Lines here and leave the database alone")
    s.add_argument("--positions", default=DEFAULT_POSITIONS,
                   help="private GPSDO positions (JSON, mode 600), laid over the receiver rows "
                        "in memory only (default %(default)s)")
    s.add_argument("--conf", default=DEFAULT_CONF)
    s.set_defaults(fn=cmd_attribute)
    s = sub.add_parser("receiver-positions",
                       help="write receiver versions with GPSDO positions from a private file")
    s.add_argument("--file", required=True, help="JSON, mode 600; never printed")
    s.add_argument("--conf", default=DEFAULT_CONF)
    s.set_defaults(fn=cmd_receiver_positions)
    s = sub.add_parser("solve", help="fixed ground-wave clusters, named from the latest "
                                      "attribution run; write sigmondb.solution or --out")
    o = s.add_mutually_exclusive_group(required=True)
    o.add_argument("--out", help="write JSON Lines here and leave the database alone")
    o.add_argument("--write-db", action="store_true",
                   help="insert the solutions into sigmondb.solution")
    g = s.add_mutually_exclusive_group(required=True)
    g.add_argument("--from-db", action="store_true")
    g.add_argument("--arrivals")
    s.add_argument("--transmitters", help="with --arrivals: transmitter.jsonl")
    s.add_argument("--receivers",
                   help="with --arrivals: receiver.jsonl, or the seed receivers.toml")
    s.add_argument("--max-sd-us", type=float, default=20.0,
                   help="largest delay sd, in microseconds, a fixed cluster may have (default 20)")
    s.add_argument("--max-ground-km", type=float, default=300.0,
                   help="farthest transmitter, in km, whose fixed cluster may give an offset "
                        "as a ground wave (default 300)")
    s.add_argument("--detector-version",
                   help="with --from-db: use only this detector_version (default: the latest, "
                        "in lexical order, per reporter and group)")
    s.add_argument("--days", type=float, default=30.0,
                   help="with --from-db: arrivals from the last DAYS days (default 30)")
    s.add_argument("--no-attribution", action="store_true",
                   help="with --from-db: attach no names from sigmondb.attribution")
    s.add_argument("--attributions",
                   help="with --arrivals: attribution rows (JSON Lines) to name clusters from")
    s.add_argument("--positions", default=DEFAULT_POSITIONS,
                   help="private GPSDO positions (JSON, mode 600), laid over the receiver rows "
                        "in memory only (default %(default)s)")
    s.add_argument("--conf", default=DEFAULT_CONF)
    s.set_defaults(fn=cmd_solve)
    s = sub.add_parser("publish",
                       help="build sigmondb_public and replace each table whole on every "
                            "host in CH_PUB_HOSTS")
    s.add_argument("--conf", default=DEFAULT_CONF, help="this host's sigmondb credentials")
    s.add_argument("--timeout", type=float, default=30.0, help="seconds per HTTP request")
    s.set_defaults(fn=cmd_publish)
    return p


def main(argv=None):
    p = build_parser()
    a = p.parse_args(argv)
    if a.verb == "solve":
        # Refuse a flag the chosen source would ignore, rather than ignore it silently.
        if a.no_attribution and not a.from_db:
            p.error("solve: --no-attribution works only with --from-db; "
                    "with --arrivals, leave out --attributions instead")
        if a.attributions and a.from_db:
            p.error("solve: --attributions works only with --arrivals; "
                    "--from-db reads sigmondb.attribution")
    return a.fn(a)


if __name__ == "__main__":
    sys.exit(main())
