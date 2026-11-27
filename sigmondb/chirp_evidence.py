"""Evidence per chirp: what the fleet heard, where it listened and heard nothing, what
chirp-recorder found, and how steady the T6 timing held.

The lifecycle (chirp_lifecycle) moves chirps on this evidence; the menu and publish read it
too.  AGG_SQL holds the four aggregates the job fetches; gather turns fetched rows into one
Evidence per chirp and does no I/O.  Times are naive UTC datetimes throughout.

Rules that shape the evidence (spec: tasks/chirp-registry/2026-11-13-chirp-scout-design.md):
- "Heard" means a matching sweep or a matching arrival of any origin, so a chirp that
  chirp-recorder still hears never reads as silent because the scout missed it.
- Silence counts only where a scout listened: a station is silent for an hour when it
  dwelled over the chirp's centre (|centre - dwell centre| <= dwell span / 2) and reported
  no matching sweep or arrival.  A station counts each silent hour once, however many
  dwells and views (coarse and fine) covered it, so a dwell count means distinct hours.
- covered_silent, the silence that can make a chirp dormant or retired, counts only hours
  after the chirp's last hearing: silence before it says nothing about whether the chirp
  went quiet.  silent_hours counts the whole window; a candidate's local reject reads it.
- A sweep matching a reporter's active local reject never counts for that reporter, as
  hearing or as silence; a co-sited partner's own dwells still count.  So too a withheld
  sweep (the job's: one that matched a rejected chirp): for any chirp it matches, its
  (reporter, hour) is neither hearing nor silence.
- Reporters at one site (one coherence group, or within independence.co_site_km) count once.
"""
import math
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from typing import Optional

from sigmondb import attribution
from sigmondb.chirp import CentreIndex, Signature, matches, median_signature
from sigmondb.set_aside import not_set_aside

# --- the aggregates ---------------------------------------------------------------------------

# Every aggregate reads the last {hours:UInt32} hours; agg_params(rules) gives the value.
# Set-aside rows ('codar' or 'all' windows) and HFDL stay out of each one: arrival by
# group_key and tx_id, sweep by matched_key, capture (which has no group_key) by its rate,
# as set_aside.dry_run_queries tells CODAR from HFDL captures.  scout_dwell carries no HFDL
# marker; a dwell says only where a scout listened.
#
# ClickHouse aliases are query-wide: an alias that names a column replaces that column in
# every other expression and in WHERE.  So no alias here may appear in another SELECT
# expression or in WHERE (min(clipped) AS clipped once nested an aggregate inside
# medianIf(..., clipped = 0)); GROUP BY uses the aliases on purpose.  The columns tuples are
# positional, so a row's keys never depend on an alias.
_SINCE = "time > now() - INTERVAL {hours:UInt32} HOUR AND "
_KEPT = not_set_aside("codar")
_ARRIVAL_KEPT = ("NOT startsWith(group_key, 'hfdl') AND tx_id NOT LIKE 'hfdl:%' AND " + _KEPT)

AGG_SQL = {
    "sweep_hourly": (
        ("reporter_id", "centre_hz", "rate_hz_per_s", "period_s", "span_hz", "duration_s",
         "clipped", "hour", "n", "snr_med", "n_dwells"),
        "/* agg:sweep_hourly */ SELECT reporter_id, round(centre_hz, -2) AS centre_hz, "
        "round(rate_hz_per_s, 0) AS rate_hz_per_s, round(period_s, 3) AS period_s, "
        "medianIf(span_hz, clipped = 0) AS span_hz, "
        "medianIf(duration_s, clipped = 0) AS duration_s, "
        "min(clipped) AS all_clipped, toStartOfHour(time) AS hour, count() AS n, "
        "median(snr_db) AS snr_med, uniqExact(time) AS n_dwells "
        "FROM sigmondb.sweep FINAL WHERE " + _SINCE +
        "NOT startsWith(matched_key, 'hfdl') AND " + _KEPT +
        " GROUP BY reporter_id, centre_hz, rate_hz_per_s, period_s, hour"),
    "dwell_hourly": (
        ("reporter_id", "centre_hz", "span_hz", "hour", "n"),
        "/* agg:dwell_hourly */ SELECT reporter_id, centre_hz, span_hz, "
        "toStartOfHour(time) AS hour, count() AS n "
        "FROM sigmondb.scout_dwell FINAL WHERE " + _SINCE + _KEPT +
        " GROUP BY reporter_id, centre_hz, span_hz, hour"),
    "capture_recent": (
        ("reporter_id", "centre_hz", "rate_hz_per_s", "first_t", "last_t", "n", "n_found"),
        "/* agg:capture_recent */ SELECT reporter_id, round(centre_hz, -2) AS centre_hz, "
        "round(rate_hz_per_s, 0) AS rate_rounded, min(time) AS first_t, max(time) AS last_t, "
        "count() AS n, countIf(n_found > 0) AS n_found "
        "FROM sigmondb.capture FINAL WHERE " + _SINCE + "rate_hz_per_s != 0 AND " + _KEPT +
        " GROUP BY reporter_id, centre_hz, rate_rounded"),
    "arrival_hourly": (
        ("reporter_id", "centre_hz", "rate_hz_per_s", "hour", "n", "n_t6"),
        "/* agg:arrival_hourly */ SELECT reporter_id, round(centre_hz, -2) AS centre_hz, "
        "round(rate_hz_per_s, 0) AS rate_hz_per_s, toStartOfHour(time) AS hour, count() AS n, "
        "countIf(origin = 't6_anchor' AND u_epoch_ns <= 1000) AS n_t6 "
        "FROM sigmondb.arrival FINAL WHERE " + _SINCE + _ARRIVAL_KEPT +
        " GROUP BY reporter_id, centre_hz, rate_hz_per_s, hour"),
}


def agg_params(rules):
    """The query parameters for AGG_SQL: the last max(retired.after_days,
    menu.history_days) days, in whole hours."""
    return {"hours": int(round(max(rules.retired.after_days, rules.menu.history_days) * 24))}


# --- the evidence -----------------------------------------------------------------------------

@dataclass
class Evidence:
    hearers: dict = field(default_factory=dict)          # independent station -> hours heard
    heard_hours_bits: int = 0                            # bit h: heard in UTC hour h, history window
    last_heard: Optional[datetime] = None
    covered_silent: dict = field(default_factory=dict)   # station -> silent covering hours after last_heard
    silent_hours: dict = field(default_factory=dict)     # station -> silent covering hours, whole window
    probe_stations: set = field(default_factory=set)
    probe_found: set = field(default_factory=set)
    waveform_agree: int = 0
    steady_t6: dict = field(default_factory=dict)        # station -> hours the T6 cluster held
    t6_hearers: set = field(default_factory=set)
    position: Optional[dict] = None                      # filled by the job from locate
    named_by_attribution: Optional[str] = None
    shared_signature: list = field(default_factory=list)


def independent(reporters, rxpos, coherence, rules):
    """{reporter: site representative}.  Reporters in one non-empty coherence group, or within
    rules.independence.co_site_km of each other, form one site (attribution.site_map)."""
    return attribution.site_map({"": {r: () for r in reporters}}, rxpos, coherence,
                                co_site_km=rules.independence.co_site_km)


def chirp_signature(c):
    """A chirp row's Signature.  A chirp with no period counts as a 1 s sweep, as group_key
    does."""
    period = c.get("period_s")
    return Signature(c["centre_hz"], c["rate_hz_per_s"], 1.0 if period is None else period,
                     c.get("span_hz"), c.get("duration_s"))


def _known(x):
    """None for a missing value: medianIf over no whole streaks gives NaN."""
    return None if x is None or (isinstance(x, float) and math.isnan(x)) else x


def _row_sig(r):
    return Signature(r["centre_hz"], r["rate_hz_per_s"], r["period_s"], _known(r.get("span_hz")),
                     _known(r.get("duration_s")))


def _sig_like(centre, rate, sig):
    """A signature with only a centre and a rate (arrivals, captures), compared on those: it
    borrows the chirp's period and leaves the span unknown, so the centre test applies."""
    return Signature(centre, rate, sig.period_s)


def _key_centre_rate(gk):
    parts = str(gk).split()
    return float(parts[0]) * 1e6, float(parts[1]) * 1e3


def _receivers(receivers):
    """(rxpos, coherence) from the latest receiver row per reporter (valid_from, version)."""
    latest = {}
    for r in receivers or ():
        k = (r.get("valid_from") or datetime.min, r.get("version") or 0)
        rid = r["reporter_id"]
        if rid not in latest or k >= latest[rid][0]:
            latest[rid] = (k, r)
    rxpos, coherence = {}, {}
    for rid, (_k, r) in latest.items():
        if r.get("lat") is not None and r.get("lon") is not None:
            rxpos[rid] = (r["lat"], r["lon"], r.get("pos_source") or "")
        if r.get("coherence_group"):
            coherence[rid] = r["coherence_group"]
    return rxpos, coherence


def _active_rejects(rejects):
    out = {}
    for r in rejects or ():
        if r.get("active", 1):
            out.setdefault(r["reporter_id"], []).append(_row_sig(r))
    return out


# Row indices bucketed by centre, so a chirp tests only rows that can match it (chirp.py).
_ByCentre = CentreIndex


def _steady_hours(rows, P, max_mad_us):
    """The longest span, in hours, of any cluster of (time, delay_ms) rows that holds within
    max_mad_us; None when no cluster does."""
    cl, _ = attribution.clusters_of([(t, d % P, 0.0) for t, d in rows], P, members=True)
    best = None
    for c in cl:
        if c["mad_us"] <= max_mad_us:
            ts = [m[0] for m in c["rows"]]
            h = (max(ts) - min(ts)).total_seconds() / 3600.0
            best = h if best is None else max(best, h)
    return best


def _named(chirp_row, sig, attributions, rules, now):
    """The tx_id the latest attribution run per group_key gives this chirp's signature: the
    chirp's own tx_id when among the names, else the one name when only one, else None.
    The latest run is the greatest (run_at, run_id) among default-policy rows of that
    group_key; only its rows that may name (attribution.row_names) count.  HFDL never."""
    latest = {}
    for a in attributions or ():
        if a.get("mode_policy") == "default":
            k = (a.get("run_at") or datetime.min, a.get("run_id") or "")
            gk = a.get("group_key")
            if gk not in latest or k > latest[gk]:
                latest[gk] = k
    names = set()
    for a in attributions or ():
        if a.get("mode_policy") != "default":
            continue
        if (a.get("run_at") or datetime.min, a.get("run_id") or "") != latest[a.get("group_key")]:
            continue
        if not attribution.row_names(a, now) or not a.get("tx_id"):
            continue
        if str(a["tx_id"]).startswith("hfdl:") or str(a.get("group_key", "")).startswith("hfdl"):
            continue
        try:
            centre, rate = _key_centre_rate(a["group_key"])
        except (KeyError, ValueError, IndexError):
            continue
        if matches(sig, _sig_like(centre, rate, sig), rules):
            names.add(a["tx_id"])
    own = chirp_row.get("tx_id")
    if own and own in names:
        return own
    return next(iter(names)) if len(names) == 1 else None


def gather(chirps, aggregates, arrivals_t6, receivers, attributions, rules, now, rejects=(),
           withheld=()):
    """{chirp_id: Evidence} from already-fetched rows.

    aggregates: AGG_SQL name -> rows (dicts); a missing name counts as no rows.
    arrivals_t6: T6 arrivals as the attribution inputs give them (cli._ATTR_WHERE, _carried):
    time, reporter_id, group_key, delay_ns.  receivers: receiver rows.  attributions: rows of
    sigmondb.attribution.  rejects: chirp_reject_local rows; withdrawn ones (active = 0) do
    not apply.  withheld: sweep_hourly rows that count for no chirp as hearing or as silence
    (the job passes the sweeps that matched a rejected chirp).  now: naive UTC."""
    aggregates = aggregates or {}
    sweeps = list(aggregates.get("sweep_hourly") or ())
    n_kept = len(sweeps)
    sweeps += list(withheld or ())
    dwells = list(aggregates.get("dwell_hourly") or ())
    caps = list(aggregates.get("capture_recent") or ())
    arrs = list(aggregates.get("arrival_hourly") or ())
    t6 = list(arrivals_t6 or ())
    rxpos, coherence = _receivers(receivers)
    reps = {r["reporter_id"] for r in sweeps + dwells + caps + arrs + t6}
    site = independent(sorted(reps), rxpos, coherence, rules)
    rej = _active_rejects(rejects)
    bits_from = now - timedelta(days=rules.dormant.history_days)
    max_mad = rules.established.max_cluster_mad_us

    sweep_sigs = [_row_sig(s) for s in sweeps]
    rejected = [i >= n_kept or any(matches(r, sg, rules) for r in rej.get(s["reporter_id"], ()))
                for i, (s, sg) in enumerate(zip(sweeps, sweep_sigs))]
    t6_keys = []
    for a in t6:
        try:
            t6_keys.append(_key_centre_rate(a["group_key"]))
        except (KeyError, ValueError, IndexError):
            t6_keys.append((a.get("centre_hz"), a.get("rate_hz_per_s")))
    t6_ok = [i for i, (c_, r_) in enumerate(t6_keys) if c_ is not None and r_ is not None]
    sweep_ix = _ByCentre(sweep_sigs, rules)
    arr_ix = _ByCentre([Signature(a["centre_hz"], a["rate_hz_per_s"], 1.0) for a in arrs], rules)
    cap_ix = _ByCentre([Signature(p["centre_hz"], p["rate_hz_per_s"], 1.0) for p in caps], rules)
    t6_ix = _ByCentre([Signature(t6_keys[i][0], t6_keys[i][1], 1.0) for i in t6_ok], rules)

    sigs = {c["chirp_id"]: chirp_signature(c) for c in chirps}
    out = {}
    for c in chirps:
        cid, sig = c["chirp_id"], sigs[c["chirp_id"]]
        ev = Evidence()
        ev.shared_signature = sorted(o for o, osig in sigs.items()
                                     if o != cid and matches(sig, osig, rules))
        heard = {}                     # site -> set of hours (counted sweeps and arrivals)
        blocked = set()                # (reporter, hour) with a rejected or withheld sweep
        whole = {}                     # site -> whole matching sweep rows
        for i in sweep_ix.candidates(sig):
            s, sg, rj = sweeps[i], sweep_sigs[i], rejected[i]
            if not matches(sig, sg, rules):
                continue
            st = site.get(s["reporter_id"], s["reporter_id"])
            if rj:
                blocked.add((s["reporter_id"], s["hour"]))
                continue
            heard.setdefault(st, set()).add(s["hour"])
            if not s.get("clipped"):
                whole.setdefault(st, []).append(s)
        for i in arr_ix.candidates(sig):
            a = arrs[i]
            if matches(sig, _sig_like(a["centre_hz"], a["rate_hz_per_s"], sig), rules):
                st = site.get(a["reporter_id"], a["reporter_id"])
                heard.setdefault(st, set()).add(a["hour"])
        ev.hearers = {st: sorted(hs) for st, hs in sorted(heard.items())}
        all_hours = [h for hs in heard.values() for h in hs]
        if all_hours:
            ev.last_heard = max(all_hours)
        for h in set(all_hours):
            if h >= bits_from:
                ev.heard_hours_bits |= 1 << h.hour

        silent = {}                    # site -> hours it dwelled over the chirp and heard nothing
        for d in dwells:
            if abs(sig.centre_hz - d["centre_hz"]) > d["span_hz"] / 2:
                continue
            st = site.get(d["reporter_id"], d["reporter_id"])
            if d["hour"] in heard.get(st, ()) or (d["reporter_id"], d["hour"]) in blocked:
                continue
            silent.setdefault(st, set()).add(d["hour"])
        ev.silent_hours = {st: len(hs) for st, hs in sorted(silent.items())}
        after = {st: sum(1 for h in hs if ev.last_heard is None or h > ev.last_heard)
                 for st, hs in sorted(silent.items())}
        ev.covered_silent = {st: n for st, n in after.items() if n}

        for i in cap_ix.candidates(sig):
            p = caps[i]
            if matches(sig, _sig_like(p["centre_hz"], p["rate_hz_per_s"], sig), rules):
                st = site.get(p["reporter_id"], p["reporter_id"])
                ev.probe_stations.add(st)
                if p.get("n_found"):
                    ev.probe_found.add(st)

        ev.waveform_agree = sum(1 for rows in whole.values()
                                if matches(sig, median_signature(rows), rules))

        P = sig.period_s * 1e3
        by_reporter = {}
        for j in t6_ix.candidates(sig):
            a = t6[t6_ok[j]]
            centre, rate = t6_keys[t6_ok[j]]
            if matches(sig, _sig_like(centre, rate, sig), rules):
                by_reporter.setdefault(a["reporter_id"], []).append((a["time"], a["delay_ns"] / 1e6))
        for rid, rows in sorted(by_reporter.items()):
            st = site.get(rid, rid)
            ev.t6_hearers.add(st)
            h = _steady_hours(rows, P, max_mad)
            if h is not None and h > ev.steady_t6.get(st, -1.0):
                ev.steady_t6[st] = h

        ev.named_by_attribution = _named(c, sig, attributions, rules, now)
        out[cid] = ev
    return out
