"""The chirp-state job: the hourly oneshot that moves each chirp through its lifecycle.

It reads the fleet's evidence, founds new `seen` chirps from sweeps no chirp explains, notes
online-list priors, gathers evidence, locates probing chirps, and asks chirp_lifecycle for each
chirp's next state.  run() reads and decides; write() inserts the log first, then local rejects,
then identity rows, then the chirp rows.  A crash between the log and the chirp rows leaves a
state the next run rebuilds from the log, with no second log row.

Rules the job keeps (plan Task 9 and the controller's rulings of 2026-11-27):
- State comes from the latest chirp_log row per chirp (current_states), never from chirp.state.
- One move per chirp per run.  A chirp founded in this run moves on the next run.
- A sweep that matches a rejected chirp counts only for that chirp: it founds nothing and
  feeds no other chirp's evidence.  A rejected chirp heard again does not move; the run counts
  it as heard_while_rejected.
- Priors (transmitter rows with source hfrnet or giro) fill prior_ids, suggested_name and a
  kind.  They never change a state.
- A chirp row goes out only when its content changed (updated_at aside), so a rerun on the
  same evidence writes nothing.
- Output carries counts and reporter ids, never a coordinate.
"""
import math
from dataclasses import dataclass, field
from datetime import datetime, timezone

from sigmondb import attribution, cli, ddl
from sigmondb import chirp_evidence as ce
from sigmondb.chirp import (Signature, founding_clusters, group_key, matches, median_signature,
                            new_chirp_id)
from sigmondb.chirp_lifecycle import STATES, next_state
from sigmondb.chirp_locate import locate
from sigmondb.half_sweep import to_sweep_start
from sigmondb.load import insert_rows
from sigmondb.publish import latlon_to_grid6
from sigmondb.receivers import grid_to_latlon
from sigmondb.rows import write_rows

ACTOR = "chirp-state"
PRIOR_SOURCES = ("hfrnet", "giro")


@dataclass
class JobResult:
    log_rows: list = field(default_factory=list)
    chirp_rows: list = field(default_factory=list)
    transmitter_rows: list = field(default_factory=list)
    waveform_rows: list = field(default_factory=list)
    reject_rows: list = field(default_factory=list)
    skipped_new: dict = field(default_factory=dict)
    states: dict = field(default_factory=dict)        # state -> chirps in it after the run
    moves: dict = field(default_factory=dict)         # "from->to" -> count
    founded: int = 0
    located: int = 0
    heard_while_rejected: int = 0
    errors: int = 0

    def rows(self):
        """{table: rows} in write order: the log first, the chirp rows last."""
        return {"chirp_log": self.log_rows, "chirp_reject_local": self.reject_rows,
                "transmitter": self.transmitter_rows, "waveform": self.waveform_rows,
                "chirp": self.chirp_rows}


# --- reading ----------------------------------------------------------------------------------

def _naive(v):
    """Naive UTC for a tz-aware datetime; any other value unchanged."""
    if isinstance(v, datetime) and v.tzinfo is not None:
        return v.astimezone(timezone.utc).replace(tzinfo=None)
    return v


def _naive_row(r):
    return {k: _naive(v) for k, v in r.items()}


def _query(client, sql, params):
    try:
        return client.query(sql, parameters=params, query_tz="UTC")
    except TypeError:   # clickhouse-connect without query_tz
        return client.query(sql, parameters=params)


def _tables(client):
    """The table names sigmondb holds.  A fresh host lacks the chirp tables; the job reads
    each absent table as empty and never creates one."""
    res = _query(client, "SELECT name FROM system.tables WHERE database = {db:String}",
                 {"db": ddl.DATABASE})
    return {r[0] for r in res.result_rows}


def _agg_table(sql):
    return sql.split(" FROM %s." % ddl.DATABASE, 1)[1].split()[0]


def read_inputs(client, rules):
    """Every row the run needs, each datetime naive UTC.  Absent tables read as empty."""
    have = _tables(client)

    def fetch(table, cols=None, where="", params=None):
        if table not in have:
            return []
        return [_naive_row(r) for r in cli._fetch(client, table, cols or ddl.COLUMNS[table],
                                                    where, params)]

    params = ce.agg_params(rules)
    aggs = {}
    for name, (cols, sql) in ce.AGG_SQL.items():
        if _agg_table(sql) not in have:
            aggs[name] = []
            continue
        aggs[name] = [_naive_row(dict(zip(cols, r)))
                      for r in _query(client, sql, params).result_rows]

    receivers = fetch("receiver")
    t6 = fetch("arrival", cli._ATTR_ARRIVAL_COLS, cli._ATTR_WHERE, params)
    for r in t6:
        r["live"] = "sweep_repetition_hz" in (r.get("engineering") or {})
    t6, _dropped = cli._carried(t6, cli._latest(receivers, "reporter_id"))
    t6 = to_sweep_start(t6, ACTOR)

    tx = [t for t in fetch("transmitter", where="WHERE source IN ('hfrnet', 'giro') "
                                                "AND kind != 'hfdl'")
          if t.get("source") in PRIOR_SOURCES and t.get("kind") != "hfdl"
          and not str(t["tx_id"]).startswith("hfdl:")]
    tx = cli._latest(tx, "tx_id")
    ids = {t["tx_id"] for t in tx}
    wf = [w for w in fetch("waveform", where="WHERE tx_id IN (SELECT tx_id FROM %s.transmitter "
                                             "WHERE source IN ('hfrnet', 'giro'))" % ddl.DATABASE)
          if w["tx_id"] in ids]
    wf = cli._latest([dict(w, _k=(w["tx_id"], w["centre_hz"])) for w in wf], "_k")

    return {
        "chirp": fetch("chirp"),
        "log": fetch("chirp_log"),
        "rejects": [r for r in fetch("chirp_reject_local") if r.get("active", 1)],
        "aggs": aggs,
        "t6": t6,
        "receivers": receivers,
        "attributions": ([_naive_row(r) for r in cli._latest_attributions(client)]
                         if "attribution" in have else []),
        "priors": _prior_sites(tx, wf),
    }


def current_states(log_rows):
    """{chirp_id: (to_state, at)} from the latest log row per chirp.  Rows at one instant keep
    the later one in input order."""
    out = {}
    for r in log_rows:
        at = _naive(r["at"])
        cur = out.get(r["chirp_id"])
        if cur is None or at >= cur[1]:
            out[r["chirp_id"]] = (r["to_state"], at)
    return out


# --- priors -----------------------------------------------------------------------------------

def _prior_sites(tx, wf):
    """[(transmitter row, Signature)], one per listed waveform, sorted by tx_id and centre."""
    by = {t["tx_id"]: t for t in tx}
    out = []
    for w in sorted(wf, key=lambda w: (w["tx_id"], w["centre_hz"])):
        period = w.get("period_s")
        out.append((by[w["tx_id"]], Signature(w["centre_hz"], w["rate_hz_per_s"],
                                              period if period else 1.0,
                                              w.get("bandwidth_hz"), w.get("duration_s"))))
    return out


def _priors(c, priors, rules):
    """(sorted prior tx_ids, the nearest matching prior or None).  Nearest to the chirp's
    position when it has one and the prior is listed with one; else the first by tx_id."""
    sig = ce.chirp_signature(c)
    hits = {}
    for t, psig in priors:
        if matches(sig, psig, rules):
            hits.setdefault(t["tx_id"], t)
    if not hits:
        return [], None
    ids = sorted(hits)
    nearest = hits[ids[0]]
    if c.get("lat") is not None and c.get("lon") is not None:
        placed = [hits[i] for i in ids if hits[i].get("lat") is not None
                  and hits[i].get("lon") is not None]
        if placed:
            nearest = min(placed, key=lambda t: (attribution.gc_km((c["lat"], c["lon"]),
                                                                   (t["lat"], t["lon"])),
                                                 t["tx_id"]))
    return ids, nearest


# --- locating ---------------------------------------------------------------------------------

def _station_clusters(c, t6, rxpos, coherence, site_of, rules):
    """(independent station -> usable clusters, period in ms) from the T6 arrivals that match
    the chirp.  analyse_group collapses co-sited reporters, drops reporters with no position
    and flags rung and twin clusters; a station keeps its clusters only when one is not set
    aside."""
    sig = ce.chirp_signature(c)
    by = {}
    for a in t6:
        try:
            centre, rate = ce._key_centre_rate(a["group_key"])
        except (KeyError, ValueError, IndexError):
            continue
        if matches(sig, ce._sig_like(centre, rate, sig), rules):
            by.setdefault(a["reporter_id"], []).append(
                (attribution._epoch(a["time"]), a["delay_ns"] / 1e6,
                 float(a.get("level_db") or 0.0), bool(a.get("live"))))
    if not by:
        return {}, sig.period_s * 1e3
    g = attribution.analyse_group(c["group_key"], by, [], rxpos, None, coherence, site_of)
    return ({st: cl for st, cl in g["stations"].items()
             if any(not attribution._set_aside(x) for x in cl)}, g["P"])


# --- the run ----------------------------------------------------------------------------------

def _same(a, b):
    """Equal as stored: floats within Float32 rounding, sequences as lists, times naive."""
    a, b = _naive(a), _naive(b)
    if isinstance(a, float) and isinstance(b, (int, float)) or \
            isinstance(b, float) and isinstance(a, (int, float)):
        if math.isnan(a) or math.isnan(b):
            return math.isnan(a) and math.isnan(b)
        return math.isclose(a, b, rel_tol=1e-6, abs_tol=1e-9)
    if isinstance(a, (list, tuple)) or isinstance(b, (list, tuple)):
        return list(a or ()) == list(b or ())
    return a == b


def _changed(old, new):
    if old is None:
        return True
    return any(not _same(old.get(k), new.get(k)) for k in ddl.COLUMNS["chirp"]
               if k != "updated_at")


def _founded_row(cid, cluster, now, rules):
    m = median_signature(cluster)
    hours = [s["hour"] for s in cluster]
    r = {k: None for k in ddl.COLUMNS["chirp"]}
    r.update(chirp_id=cid, group_key=group_key(m.centre_hz, m.rate_hz_per_s, m.period_s,
                                               m.duration_s),
             kind="unknown", state="seen", state_since=now, centre_hz=m.centre_hz,
             span_hz=m.span_hz, rate_hz_per_s=m.rate_hz_per_s, period_s=m.period_s,
             duration_s=m.duration_s, pos_source="", first_heard=min(hours),
             last_heard=max(hours), reporter_ids=[], n_stations=0, heard_hours=0, tx_id="",
             prior_ids=[], suggested_name="", pin="", pin_by="", pin_why="",
             rules_version=rules.rules_version, updated_at=now)
    return r


def _log(now, cid, from_state, to_state, why, evidence, reporters, rules):
    return dict(at=now, chirp_id=cid, from_state=from_state, to_state=to_state, actor=ACTOR,
                why=why, evidence=dict(evidence), reporter_ids=sorted(reporters),
                rules_version=rules.rules_version)


def run(client, rules, now, out=print):
    """Read the fleet's evidence, move every chirp, and return the rows to write.  Writes
    nothing: write() or write_files() does."""
    now = _naive(now).replace(microsecond=0)
    inp = read_inputs(client, rules)
    res = JobResult()
    states = current_states(inp["log"])
    stored = {c["chirp_id"]: c for c in inp["chirp"]}

    # 2. State per chirp from the log.
    live = []
    for cid in sorted(stored):
        if cid not in states:
            out("chirp-state: %s has no log row; skipped" % cid)
            res.errors += 1
            continue
        st, since = states[cid]
        live.append(dict(stored[cid], state=st, state_since=since))

    # 3. Match each sweep row to the chirps; found new chirps from the rest.
    aggs = inp["aggs"]
    sigs = [ce.chirp_signature(c) for c in live]
    index = ce._ByCentre(sigs, rules)
    unmatched, kept = [], []
    for s in aggs.get("sweep_hourly") or ():
        sg = ce._row_sig(s)
        hit = [i for i in index.candidates(sg) if matches(sigs[i], sg, rules)]
        if any(live[i]["state"] == "rejected" for i in hit):
            continue                    # counts only for the rejected chirp
        kept.append(s)
        if not hit:
            unmatched.append(s)

    cap = rules.seen.max_new_per_reporter_per_run
    taken = set(stored)
    log_only = set(states) - set(stored)
    used, founded = {}, []
    for cl in founding_clusters(unmatched, rules):
        reps = sorted({s["reporter_id"] for s in cl})
        if not any(used.get(r, 0) < cap for r in reps):
            for r in reps:
                res.skipped_new[r] = res.skipped_new.get(r, 0) + 1
            continue
        for r in reps:
            used[r] = used.get(r, 0) + 1
        base = new_chirp_id(cl, now.date())
        cid, k = base, 1
        while cid in taken:
            k += 1
            cid = "%s-%d" % (base, k)
        taken.add(cid)
        row = _founded_row(cid, cl, now, rules)
        if cid in log_only:
            # A crash left the founding log row without its chirp row: rebuild the row only.
            row["state"], row["state_since"] = states[cid]
        else:
            res.log_rows.append(_log(now, cid, "", "seen", "founded",
                                     {"n_rows": str(len(cl)),
                                      "hours": str(len({s["hour"] for s in cl})),
                                      "reporters": ",".join(reps),
                                      "group_key": row["group_key"]}, reps, rules))
            res.founded += 1
        founded.append(row)
    if res.skipped_new:
        out("chirp-state: skipped_new " + " ".join(
            "%s=%d" % kv for kv in sorted(res.skipped_new.items())))

    everyone = live + founded
    new_ids = {c["chirp_id"] for c in founded}

    # 4. Priors: names and kinds, never a state.
    for c in everyone:
        ids, nearest = _priors(c, inp["priors"], rules)
        c["prior_ids"] = ids
        c["suggested_name"] = nearest["tx_id"] if nearest else ""
        if nearest and (c.get("kind") or "unknown") == "unknown" and nearest.get("kind"):
            c["kind"] = nearest["kind"]

    # 5. Evidence, positions for probing chirps, then the moves.
    common = (inp["t6"], inp["receivers"], inp["attributions"], rules, now)
    rejected = [c for c in everyone if c["state"] == "rejected"]
    others = [c for c in everyone if c["state"] != "rejected"]
    evs = ce.gather(others, dict(aggs, sweep_hourly=kept), *common, rejects=inp["rejects"])
    if rejected:
        evs.update(ce.gather(rejected, aggs, *common, rejects=inp["rejects"]))

    rxpos, coherence = ce._receivers(inp["receivers"])
    reps = {r["reporter_id"] for rows in aggs.values() for r in rows}
    reps |= {a["reporter_id"] for a in inp["t6"]}
    site_of = ce.independent(sorted(reps), rxpos, coherence, rules)
    station_pos = {rid: grid_to_latlon(latlon_to_grid6(lat, lon))
                   for rid, (lat, lon, _src) in rxpos.items()}

    for c in everyone:
        cid, ev = c["chirp_id"], evs[c["chirp_id"]]
        if c["state"] == "probing" and cid not in new_ids:
            st_clusters, P = _station_clusters(c, inp["t6"], rxpos, coherence, site_of, rules)
            ev.position = locate(st_clusters, rxpos, P, rules)
            if ev.position.get("status") == "located":
                res.located += 1
                c.update(lat=ev.position["lat"], lon=ev.position["lon"],
                         pos_sigma_m=ev.position["pos_sigma_m"], pos_source="located")

        if cid not in new_ids:
            try:
                mv = next_state(c, ev, rules, now, station_pos=station_pos)
            except ValueError:
                out("chirp-state: %s is in unknown state %r; skipped" % (cid, c["state"]))
                res.errors += 1
                c["_skip"] = True
                continue
            if c["state"] == "rejected" and ev.last_heard and ev.last_heard > c["state_since"]:
                res.heard_while_rejected += 1
            if mv is not None:
                _apply_move(c, ev, mv, now, rules, res)

        c.update(reporter_ids=sorted(ev.hearers), n_stations=len(ev.hearers),
                 heard_hours=ev.heard_hours_bits, rules_version=rules.rules_version)
        heard = [t for t in (c.get("last_heard"), ev.last_heard) if t is not None]
        c["last_heard"] = max(heard) if heard else None

    # 6. The chirp rows whose content changed.
    for c in everyone:
        if c.get("_skip"):
            continue
        res.states[c["state"]] = res.states.get(c["state"], 0) + 1
        row = {k: c.get(k) for k in ddl.COLUMNS["chirp"]}
        row["updated_at"] = now
        if _changed(stored.get(c["chirp_id"]), row):
            res.chirp_rows.append(row)

    # 7. Counts only.
    out("chirp-state: %d chirps: %s" % (sum(res.states.values()), " ".join(
        "%s=%d" % (s, res.states[s]) for s in STATES if res.states.get(s)) or "none"))
    out("chirp-state: moves %s" % (" ".join(
        "%s=%d" % kv for kv in sorted(res.moves.items())) or "none"))
    out("chirp-state: founded %d, located %d, heard_while_rejected %d, errors %d"
        % (res.founded, res.located, res.heard_while_rejected, res.errors))
    out("chirp-state: rows %s" % " ".join(
        "%s=%d" % (t, len(rows)) for t, rows in res.rows().items()))
    return res


def _apply_move(c, ev, mv, now, rules, res):
    """Log the move, then the local reject and identity rows it implies."""
    cid, frm = c["chirp_id"], c["state"]
    res.log_rows.append(_log(now, cid, frm, mv.to_state, mv.why, mv.evidence, ev.hearers, rules))
    key = "%s->%s" % (frm, mv.to_state)
    res.moves[key] = res.moves.get(key, 0) + 1
    c["state"], c["state_since"] = mv.to_state, now
    if mv.local_reject:
        res.reject_rows.append(dict(
            reporter_id=mv.local_reject, centre_hz=c["centre_hz"],
            rate_hz_per_s=c["rate_hz_per_s"],
            period_s=c["period_s"] if c.get("period_s") is not None else 1.0,
            span_hz=c.get("span_hz"), chirp_id=cid,
            reason="%s; neighbours %s" % (mv.why, mv.evidence.get("neighbours", "")),
            set_by=ACTOR, set_at=now, active=1))
    if mv.to_state == "established" and not c.get("tx_id"):
        named = ev.named_by_attribution
        if named and not named.startswith("cand:"):
            c["tx_id"] = named          # attribution tied it to a listed site
        else:
            c["tx_id"] = cid
            res.transmitter_rows.append(_transmitter(c, now))
            res.waveform_rows.append(_waveform(c, ev, now))


def _transmitter(c, now):
    kind = c.get("kind") or "unknown"
    t = {k: None for k in ddl.COLUMNS["transmitter"]}
    t.update(tx_id=c["chirp_id"], kind=kind, network="", name="", status="established",
             lat=c.get("lat"), lon=c.get("lon"), pos_source=c.get("pos_source") or "",
             pos_sigma_m=c.get("pos_sigma_m"), valid_from=now, version=1, source="discovered",
             note="")
    return t


def _waveform(c, ev, now):
    w = {k: None for k in ddl.COLUMNS["waveform"]}
    w.update(tx_id=c["chirp_id"], valid_from=now, centre_hz=c["centre_hz"],
             rate_hz_per_s=c["rate_hz_per_s"], period_s=c.get("period_s"),
             duration_s=c.get("duration_s"), bandwidth_hz=c.get("span_hz"),
             gate_period_s=c.get("gate_period_s"), source="measured",
             measured_by=",".join(sorted(ev.hearers)))
    return w


# --- writing ----------------------------------------------------------------------------------

def write(client, res, out=print):
    """Insert every row set, the log first and the chirp rows last."""
    for table, rows in res.rows().items():
        if rows:
            out("%s: %d rows" % (table, insert_rows(client, table, rows)))


def write_files(d, res, out=print):
    """Every row set as <table>.jsonl in d; the database stays untouched."""
    for table, rows in res.rows().items():
        out("%s.jsonl: %d rows" % (table, write_rows(d / (table + ".jsonl"), rows)))
