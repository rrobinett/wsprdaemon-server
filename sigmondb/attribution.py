"""Attribution: name each CODAR arrival cluster's radar by cross-station consistency.

A port of the study in tasks/t3-augment-study/attribute.py (2026-11-13), same arithmetic.

The physics.  A peak in sweep group G at station i arrives at

    D_i = O_tx + p_i(tx)   (mod the sweep period)

O_tx, the radar's sweep start against the UTC second, stays unknown; published offsets never
enter (Michael's rule of 11-07).  p_i comes from geometry: ground wave = distance / c, skywave = a
one- or two-hop mirror path at virtual height h over a spherical earth.  The right radar makes
O = D_i - p_i agree across every T6 station that hears the peak; a wrong radar scatters by the
geometry.

Inputs, all from sigmondb: arrival rows timed by T6, transmitter positions, the waveform rows
(each site's centre, sweep rate and period; they replace the study's HFRNet stationList.csv),
and sigmondb.receiver for receiver positions and their source (it replaces the study's
stations.json).  This module reads nothing itself; the `attribute` verb hands it the rows.

Choices, stated once:
- c = 299 792.458 km/s for ground wave and skywave alike.  Sea-path ground wave runs about 0.03 %
  slower than c; at 300 km that makes 0.3 us, far below every other error here.
- Ground wave counts as a possible mode only within reach: 300 km for a coastal receiver
  (mostly sea path), 100 km for an inland one.  A Policy may override both (the 900 km
  ground-wave-only pass tests the stage-2 idea that K3LR's and W2NAF's fixed day tracks travel
  by ground wave over 500-900 km).  Ground wave also needs a receiver at its GPSDO position;
  a typed grid can sit 4 km off, so such a receiver gets skywave modes only.
- Skywave: 1F off a mirror at h = 300 km, tolerance = half the 250-350 km spread plus 0.15 ms of
  ionospheric wander, up to 3800 km; 2F from 3000 km.  Ground wave only for a cluster that holds
  still (MAD <= 50 us).  No IRI and no ray trace yet.
- Gating ladder: a gated sweep puts rungs at m / (|k| Tg) (Tg = 1945.6 us at 4-5 MHz, 972.8 us
  at 13 MHz, from codar-mf/clean.py and codar-t6/day1108-13mhz).  Back-fill rows went through
  CLEAN, which tags rungs 'R'; only 'A' rows enter.  Live rows (codar-sounder) carry no CLEAN, so
  a cluster sitting m rungs from a stronger cluster at the same station counts as a rung here
  and never gets a radar of its own.
- Twins: KFS repeats each arrival 1.632 ms later.  A cluster 0-5 ms after one with the same
  rows, level and spread counts as a detector duplicate.
- Images: sites at the same rate but another centre (25 kHz away at 13 MHz) leak into a filter
  shifted by -(c_true - c_filter) / k.  A cluster that matches a stronger cluster of such a
  neighbour group at the same station counts as an image.
- One site, one vote: reporters that share a coherence group, or stand within 50 km of each
  other, count as one station.  Two labels on one path would hand any site a free
  "two-station" fit.
- Null: the same search run on fake sites scattered over the candidates' region (at least
  100 km from every real candidate) says how often geometry alone yields an equally tight
  match.  A name only stands when real sites beat the fakes.
"""
import calendar
import hashlib
import math
import random
import statistics
import zlib
from collections import defaultdict
from dataclasses import dataclass

C_KM_S = 299_792.458
RE = 6371.0
H_NOM, H_LO, H_HI = 300.0, 250.0, 350.0
IONO_TOL_MS = 0.15          # skywave wander beyond the h range (night swing 0.3-0.5 ms peak-peak)
GW_TOL_MS = 0.03            # ground wave: site position (~100 m) + cluster scatter
GAP_MS = 0.12               # cluster gap
TIGHT_US = 50.0             # MAD-sigma at or below which a cluster may be ground wave
SAME_SITE_KM = 50.0         # candidates closer than this cannot be told apart; named together
CO_SITE_KM = 50.0           # receivers closer than this count as one station
RUNG_TOL_MS = 0.15
IMAGE_TOL_MS = 0.4
COASTAL = frozenset({"KFS-OMNI", "WB6CXC/7", "UACNJ"})   # paths run mostly over sea
P_NULL_MAX = 0.05           # a name stands only when fakes match this well in at most 5 % of draws
N_FAKE = 400                # fake sites per group, at least 100 km from every candidate
ATTRIB_VERSION = "attribute-1"


@dataclass(frozen=True)
class Policy:
    """Which propagation modes a pass allows.  gw_reach_km None: 300 km coastal, 100 km inland."""
    name: str
    gw_reach_km: float | None
    gw_only: bool
    max_centre_hz: float | None


DEFAULT = Policy("default", None, False, None)
GW900 = Policy("gw_only_900km", 900.0, True, 6e6)


# ---------------------------------------------------------------- geometry

def gc_km(a, b):
    p1, l1, p2, l2 = map(math.radians, (a[0], a[1], b[0], b[1]))
    h = math.sin((p2 - p1) / 2) ** 2 + math.cos(p1) * math.cos(p2) * math.sin((l2 - l1) / 2) ** 2
    return 2 * RE * math.asin(min(1.0, math.sqrt(h)))


def sky_km(d, h, hops):
    th = d / RE / (2 * hops)
    r = RE + h
    return 2 * hops * math.sqrt(RE ** 2 + r ** 2 - 2 * RE * r * math.cos(th))


def modes(d, rx, policy, tight=True, *, gw_ok):
    """Possible propagation modes for a path of d km: (name, delay ms, tolerance ms).

    Ground wave only for a cluster that holds still (tight), since a ground-wave path cannot
    wander, and only at a receiver whose position came from its GPSDO (gw_ok); 1F up to
    3800 km, 2F from 3000 km.  gw_ok has no default: a caller that forgets it fails rather
    than granting ground wave to a typed-grid receiver."""
    out = []
    reach = (policy.gw_reach_km if policy.gw_reach_km is not None
             else (300.0 if rx in COASTAL else 100.0))
    if d <= reach and tight and gw_ok:
        out.append(("G", d / C_KM_S * 1e3, GW_TOL_MS))
    if policy.gw_only:
        return out
    for n in (1, 2):
        if (n == 1 and d > 3800) or (n == 2 and d < 3000):
            continue
        lo, nom, hi = (sky_km(d, h, n) / C_KM_S * 1e3 for h in (H_LO, H_NOM, H_HI))
        out.append((f"{n}F", nom, (hi - lo) / 2 + IONO_TOL_MS))
    return out


def cdiff(a, b, P):
    """Signed circular difference a - b, in (-P/2, P/2]."""
    x = (a - b) % P
    return x - P if x > P / 2 else x


# ---------------------------------------------------------------- clusters

def mad_us(xs):
    m = statistics.median(xs)
    return 1.4826 * statistics.median(abs(x - m) for x in xs) * 1e3


def clusters_of(rows, P):
    """Gap clustering of delays (ms, mod P).  rows: (t, d_ms, level).  Returns (clusters, captures)."""
    caps = len({t for t, _, _ in rows})
    if not rows:
        return [], 0
    min_n = max(4, math.ceil(0.15 * caps))
    pts = sorted(rows, key=lambda r: r[1])
    # Start the scan in the widest gap so a cluster straddling 0/P stays whole.
    gaps = [((pts[(i + 1) % len(pts)][1] - pts[i][1]) % P, i) for i in range(len(pts))]
    start = (max(gaps)[1] + 1) % len(pts)
    pts = pts[start:] + pts[:start]
    groups, cur = [], [pts[0]]
    for p in pts[1:]:
        if (p[1] - cur[-1][1]) % P <= GAP_MS:
            cur.append(p)
        else:
            groups.append(cur)
            cur = [p]
    groups.append(cur)
    out = []
    for g in groups:
        ncap = len({t for t, _, _ in g})
        if ncap < min_n:
            continue
        base = g[0][1]
        xs = [base + (p[1] - base) % P for p in g]
        xs.sort()
        med = statistics.median(xs) % P
        out.append({"D": med, "n": len(g), "caps": ncap, "mad_us": mad_us(xs),
                    "p10_90_us": (xs[int(0.9 * (len(xs) - 1))] - xs[int(0.1 * (len(xs) - 1))]) * 1e3,
                    "level": statistics.median(p[2] for p in g), "flags": []})
    return out, caps


# ---------------------------------------------------------------- the three guards

def rung_spacing_ms(centre_hz, rate):
    tg = 1945.6e-6 if centre_hz < 8e6 else 972.8e-6
    return 1e3 / (abs(rate) * tg)


def flag_rungs(cl, R, P, live):
    for b in cl:
        for a in cl:
            if a is b or not (a["n"] > b["n"] or (a["n"] == b["n"] and a["level"] > b["level"])):
                continue
            dd = cdiff(b["D"], a["D"], P)
            m = round(dd / R)
            if m != 0 and abs(m) <= 3 and abs(dd - m * R) <= RUNG_TOL_MS:
                b["flags"].append(f"rung {m:+d} of {a['D']:.3f}" + ("" if live else " (CLEAN passed it)"))
                b["rung"] = True
                break


def flag_twins(cl, P):
    """A detector duplicate: same rows, level and spread, a fixed offset later (KFS: 1.632 ms)."""
    for a in cl:
        for b in cl:
            if a is b or b.get("twin") or a.get("twin"):
                continue
            dd = cdiff(b["D"], a["D"], P)
            if (0 < dd < 5 and abs(a["n"] - b["n"]) <= max(2, 0.03 * a["n"])
                    and abs(a["level"] - b["level"]) < 0.5
                    and abs(a["mad_us"] - b["mad_us"]) <= max(3, 0.15 * a["mad_us"])):
                b["flags"].append(f"twin of {a['D']:.3f} (+{dd:.3f} ms, same rows and level)")
                b["twin"] = True


def flag_images(groups):
    """Same signed rate, centres 3-30 kHz apart: a weaker cluster at D_true - (c_true - c_f)/k.

    Strongest clusters act as sources first; a cluster already marked an image never acts as one.
    Each image also enters its group's set_aside."""
    allc = [(c["n"], c["level"], g, st, c) for g in groups for st, cl in g["stations"].items() for c in cl]
    allc.sort(key=lambda x: (-x[0], -x[1]))
    for nb, lb, gb, st, b in allc:
        if b.get("image"):
            continue
        for ga in groups:
            if ga is gb or abs(ga["rate"] - gb["rate"]) > 0.005 * abs(ga["rate"]) or abs(ga["P"] - gb["P"]) > 1e-6:
                continue
            dc = gb["centre"] - ga["centre"]
            if abs(dc) < 3000 or abs(dc) > 30000:
                continue
            shift = -dc / ga["rate"] * 1e3                  # ms; the -99.26 groups at ND bear out the sign
            for a in ga["stations"].get(st, []):
                if a.get("image") or (a["n"], a["level"]) >= (nb, lb):
                    continue
                if abs(cdiff(a["D"], b["D"] + shift, ga["P"])) <= IMAGE_TOL_MS:
                    a["flags"].append(f"image of {gb['key']} {b['D']:.3f} (shift {shift % ga['P']:.2f} ms)")
                    a["image"] = True
                    ga.setdefault("set_aside", {}).setdefault(_cluster_key(st, a), a["flags"][0])


def _cluster_key(st, c):
    return f"{st}|{int(round(c['D'] * 1e3))}"


# ---------------------------------------------------------------- per group

def parse_key(gk):
    parts = gk.split()
    period = None
    if len(parts) > 2 and parts[2].endswith("s"):
        period = float(parts[2][:-1])
    return float(parts[0]) * 1e6, float(parts[1]) * 1e3, period


def candidates_for(centre, rate, sites):
    """Sites within 3 kHz of the centre and 0.5 % (at least 500 Hz/s) of the rate; same sign
    preferred.  Returns (candidates, how many opposite-sign sites were dropped)."""
    exact = [s for s in sites if abs(s["freq"] - centre) <= 3000
             and abs(abs(s["rate"]) - abs(rate)) <= max(500.0, 0.005 * abs(rate))]
    same_sign = [s for s in exact if (s["rate"] > 0) == (rate > 0)]
    return same_sign or exact, len(exact) - len(same_sign)


def site_map(rows_by_group, rxpos, coherence):
    """Each reporter's site representative, chosen once across every group.

    rows_by_group: group key -> {reporter: rows}.  Reporters that share a non-empty coherence
    group, or stand within CO_SITE_KM of each other, form one site.  The site counts as the
    member ranked highest by (has a position, position from its GPSDO, captures summed over all
    groups, name).  A GPSDO position keeps ground-wave reach.  One choice for all groups keeps
    flag_images, which pairs clusters across groups by reporter, comparing like with like.
    Returns {reporter: representative}."""
    coherence = coherence or {}
    caps = {}
    for rows_by_st in rows_by_group.values():
        for st, rows in rows_by_st.items():
            caps[st] = caps.get(st, 0) + len({r[0] for r in rows})
    sts = sorted(caps)
    parent = {s: s for s in sts}

    def find(s):
        while parent[s] != s:
            parent[s] = parent[parent[s]]
            s = parent[s]
        return s

    for i, a in enumerate(sts):
        for b in sts[i + 1:]:
            ca, cb = coherence.get(a, ""), coherence.get(b, "")
            same = bool(ca) and ca == cb
            if not same and a in rxpos and b in rxpos:
                same = gc_km(rxpos[a][:2], rxpos[b][:2]) <= CO_SITE_KM
            if same:
                parent[find(b)] = find(a)
    members = {}
    for s in sts:
        members.setdefault(find(s), []).append(s)
    out = {}
    for ms in members.values():
        best = max(ms, key=lambda s: (s in rxpos, s in rxpos and rxpos[s][2] == "gpsdo",
                                      caps[s], s))
        out.update((s, best) for s in ms)
    return out


def collapse_colocated(rows_by_st, rxpos, coherence, site_of=None):
    """Count reporters at one site as one station.

    site_of comes from site_map over every group; without it, this group alone decides.  Only
    each site's representative keeps its rows.  The others go to the returned set-aside map,
    even in a group where the representative heard nothing: a site's vote never changes hands
    between groups.  Returns (rows_by_st kept, {reporter: reason})."""
    if site_of is None:
        site_of = site_map({"": rows_by_st}, rxpos, coherence)
    kept, aside = {}, {}
    for st in sorted(rows_by_st):
        rep = site_of.get(st, st)
        if rep == st:
            kept[st] = rows_by_st[st]
        else:
            aside[st] = f"co-located with {rep}; counted once"
    return kept, aside


def analyse_group(gk, rows_by_st, sites, rxpos, eng_period, coherence=None, site_of=None):
    """Clusters per station for one sweep group, with the rung and twin guards applied.

    rows_by_st: reporter -> [(t, d_ms, level, live)].  rxpos: reporter -> (lat, lon, pos_source).
    site_of: site_map over every group; a caller running several groups must pass it.
    Images need every group at once; flag_images runs after this, across groups."""
    centre, rate, kperiod = parse_key(gk)
    cands, opp_sign = candidates_for(centre, rate, sites)
    if kperiod:
        P_s = kperiod
    elif any(s.get("period") for s in cands):          # a site with no period still competes
        P_s = statistics.mode(round(s["period"], 4) for s in cands if s.get("period"))
    elif eng_period:
        P_s = 1.0 / eng_period
    else:
        P_s = 1.0
    P = P_s * 1e3
    R = rung_spacing_ms(centre, rate)
    g = {"key": gk, "centre": centre, "rate": rate, "P": P, "R": R, "cands": cands,
         "opp_sign": opp_sign, "stations": {}, "caps": {}, "sets": [], "named": [], "notes": [],
         "set_aside": {}}
    rows_by_st, aside = collapse_colocated(rows_by_st, rxpos, coherence, site_of)
    g["set_aside"].update(aside)
    for st, rows in rows_by_st.items():
        if st not in rxpos:
            g["notes"].append(f"{st}: no receiver position; left out")
            g["set_aside"][st] = "no receiver position"
            continue
        cl, caps = clusters_of([(t, d % P, lv) for t, d, lv, _ in rows], P)
        live = any(l for *_, l in rows)
        flag_twins(cl, P)
        flag_rungs(cl, R, P, live)
        for c in cl:
            if c["flags"]:
                g["set_aside"][_cluster_key(st, c)] = c["flags"][0]
        g["stations"][st] = cl
        g["caps"][st] = caps
    return g


# ---------------------------------------------------------------- matching

def _set_aside(c):
    return c.get("rung") or c.get("image") or c.get("twin")


def _gw_ok(rxpos, st):
    """Ground wave needs the receiver at its GPSDO position; a typed grid can sit 4 km off."""
    return rxpos[st][2] == "gpsdo"


def entries(site_pos, st_clusters, rxpos, P, policy):
    """Every (station, cluster index, mode, O, tol, d km) a site allows."""
    out = []
    for st, cl in st_clusters.items():
        d = gc_km(site_pos, rxpos[st][:2])
        for j, c in enumerate(cl):
            if _set_aside(c):
                continue
            for mname, p, tol in modes(d, st, policy, c["mad_us"] <= TIGHT_US,
                                       gw_ok=_gw_ok(rxpos, st)):
                out.append((st, j, mname, (c["D"] - p) % P, tol, d))
    return out


def best_sets(ents, P, min_st=2):
    """Tuples, one entry per station, whose O agree within each entry's tolerance.

    Returns a list of (n_stations, spread_us, Obar, members) sorted best first, keeping for each
    set of clusters its tightest tuple."""
    by_st = defaultdict(list)
    for e in ents:
        by_st[e[0]].append(e)
    sts = sorted(by_st)
    found = {}
    for a in ents:
        members = [a]
        for st in sts:
            if st == a[0]:
                continue
            cands = [e for e in by_st[st] if abs(cdiff(e[3], a[3], P)) <= e[4] + a[4]]
            if cands:
                members.append(min(cands, key=lambda e: abs(cdiff(e[3], a[3], P)) / e[4]))
        if len(members) < min_st:
            continue
        # refine: weighted mean, then drop members outside their own tolerance
        for _ in range(2):
            w = [1 / m[4] ** 2 for m in members]
            ob = (a[3] + sum(wi * cdiff(m[3], a[3], P) for wi, m in zip(w, members)) / sum(w)) % P
            keep = [m for m in members if abs(cdiff(m[3], ob, P)) <= m[4]]
            if len(keep) == len(members):
                break
            members = keep
        if len(members) < min_st:
            continue
        os_ = [cdiff(m[3], ob, P) for m in members]
        spread = (max(os_) - min(os_)) * 1e3
        key = tuple(sorted((m[0], m[1]) for m in members))
        if key not in found or spread < found[key][1]:
            found[key] = (len(members), spread, ob, sorted(members))
    return sorted(found.values(), key=lambda s: (-s[0], s[1]))


def tuple_fit(site_pos, tup, st_clusters, rxpos, P, policy):
    """Best agreement a site gives for a fixed tuple of (station, cluster).

    Returns (spread us, Obar, all within tolerance, modes), or None when the site allows no
    mode at some station."""
    choices = []
    for st, j in tup:
        d = gc_km(site_pos, rxpos[st][:2])
        c = st_clusters[st][j]
        choices.append([(st, j, m, (c["D"] - p) % P, tol)
                        for m, p, tol in modes(d, st, policy, c["mad_us"] <= TIGHT_US,
                                               gw_ok=_gw_ok(rxpos, st))])
    best = None

    def rec(i, picked):
        nonlocal best
        if i == len(choices):
            w = [1 / m[4] ** 2 for m in picked]
            a = picked[0][3]
            ob = (a + sum(wi * cdiff(m[3], a, P) for wi, m in zip(w, picked)) / sum(w)) % P
            os_ = [cdiff(m[3], ob, P) for m in picked]
            ok = all(abs(o) <= m[4] for o, m in zip(os_, picked))
            spread = (max(os_) - min(os_)) * 1e3
            if best is None or (ok, -spread) > (best[2], -best[0]):
                best = (spread, ob, ok, [m[2] for m in picked])
            return
        for c in choices[i]:
            rec(i + 1, picked + [c])
    rec(0, [])
    return best


def fake_sites(cands, n, seed, avoid_km=100.0, avoid_from=None):
    """n random positions in the candidates' extent +- 5 degrees, each at least avoid_km from
    every site in avoid_from (default: the candidates)."""
    avoid_from = avoid_from or cands
    rnd = random.Random(seed)
    la = [s["lat"] for s in cands]
    lo = [s["lon"] for s in cands]
    box = (min(la) - 5, max(la) + 5, min(lo) - 5, max(lo) + 5)
    out = []
    tries = 0
    while len(out) < n and tries < n * 50:
        tries += 1
        p = (rnd.uniform(box[0], box[1]), rnd.uniform(box[2], box[3]))
        if all(gc_km(p, (s["lat"], s["lon"])) >= avoid_km for s in avoid_from):
            out.append(p)
    return out


# ---------------------------------------------------------------- attribution

_ORDER = {"named": 0, "named_two_stations": 1, "ambiguous": 2}


def attribute_group(g, rxpos, policy, n_fake, seed):
    """Fit every candidate site to the group's clusters and judge each fit.

    Fills g["named"] (one entry per site that took a tuple, best first) and g["verdict"] (None,
    or why no fit can be tried).  A fit gets a name only when no rival site fits the same
    clusters within tolerance and fake sites match as well in at most P_NULL_MAX of draws;
    otherwise it reads ambiguous."""
    g["policy"] = policy.name
    sts = {s: c for s, c in g["stations"].items() if any(not _set_aside(x) for x in c)}
    P = g["P"]
    g["named"] = []
    if not g["cands"]:
        g["verdict"] = "no HFRNet site matches this centre and rate"
        return
    if len(sts) < 2:
        g["verdict"] = (f"only {len(sts)} T6 station(s) with usable clusters: cross-station "
                        "consistency cannot name a radar")
        return

    def eligible(ents):                         # a site some mode links to two or more stations
        return len({e[0] for e in ents}) >= 2

    allsets = []
    for s in g["cands"]:
        for n, spread, ob, mem in best_sets(entries((s["lat"], s["lon"]), sts, rxpos, P, policy), P):
            allsets.append((n, spread, ob, mem, s))
    allsets.sort(key=lambda x: (-x[0], x[1]))
    # null: fake sites scattered where the real candidates that yield tuples lie
    live_c = [s for s in g["cands"] if any(x[4] is s for x in allsets)] or g["cands"]
    fakes = fake_sites(live_c, n_fake, seed, avoid_from=g["cands"]) if n_fake else []
    fbest = []
    for p in fakes:
        ents = entries(p, sts, rxpos, P, policy)
        if eligible(ents):                      # fakes no mode can link cannot test anything
            fbest.append(best_sets(ents, P))
    g["n_fake"] = len(fbest)

    def p_null(n, spread):
        if not fbest:
            return float("nan")
        hit = sum(1 for bs in fbest if any(x[0] >= n and x[1] <= spread for x in bs))
        return hit / len(fbest)

    used, named = set(), {}
    for n, spread, ob, mem, s in allsets:
        if s["id"] in named or any((m[0], m[1]) in used for m in mem):
            continue
        tup = tuple((m[0], m[1]) for m in mem)
        # runner-up: the best other site for the same clusters
        rivals, alias = [], []
        for o in g["cands"]:
            if o is s:
                continue
            if gc_km((s["lat"], s["lon"]), (o["lat"], o["lon"])) < SAME_SITE_KM:
                alias.append(o["id"])
                continue
            f = tuple_fit((o["lat"], o["lon"]), tup, sts, rxpos, P, policy)
            if f is None:                     # the rival allows no mode at some station
                f = (float("inf"), None, False, [])
            rivals.append((not f[2], f[0], o, f))
        rivals.sort(key=lambda r: (r[0], r[1]))
        ru = rivals[0] if rivals else None
        # A spread finer than the model's own tolerance proves nothing more than a fit within it,
        # so fakes count as equally good down to the tuple's mean tolerance.
        floor = statistics.mean(m[4] for m in mem) * 1e3
        pn = p_null(n, max(spread, floor))
        flags = []
        if ru and not ru[0]:
            flags.append(f"runner-up {ru[2]['id']} also fits ({ru[1]:.0f} us)")
        if pn == pn and pn > P_NULL_MAX:
            flags.append(f"fake sites match as well in {pn:.0%} of draws")
        status = "ambiguous" if flags else ("named" if n >= 3 else "named_two_stations")
        gmodes = [m for m in mem if m[2] == "G"]
        if gmodes:
            w = [1 / m[4] ** 2 for m in gmodes]
            a = gmodes[0][3]
            o_g = (a + sum(wi * cdiff(m[3], a, P) for wi, m in zip(w, gmodes)) / sum(w)) % P
        else:
            o_g = None
        named[s["id"]] = {"site": s, "alias": alias, "n": n, "spread": spread, "O": ob,
                          "O_ground": o_g, "mem": mem, "runner": ru, "p_null": pn,
                          "flags": flags, "status": status}
        used |= {(m[0], m[1]) for m in mem}
    g["named"] = sorted(named.values(), key=lambda x: (_ORDER[x["status"]], -x["n"], x["spread"]))
    g["sets_total"] = len(allsets)
    g["verdict"] = None


def _epoch(t):
    """Whole seconds: a number as given, a naive-UTC datetime by calendar, never local time."""
    if hasattr(t, "timetuple"):
        return calendar.timegm(t.utctimetuple() if t.tzinfo else t.timetuple())
    return int(t)


def run_attribution(arrivals, sites, rxpos, policy, n_fake=N_FAKE, coherence=None):
    """Run one path policy over every sweep group in the arrivals.

    arrivals: dicts with time, reporter_id, group_key, delay_ns, level_db, live (and, when
    known, sweep_repetition_hz).  sites: dicts with id, lat, lon, freq, rate, period.  rxpos:
    reporter -> (lat, lon, pos_source).  The site representative of each reporter comes from
    one site_map over every group, so all groups (and flag_images across them) count the same
    reporter.  Groups at or above policy.max_centre_hz are skipped.  The fake-site seed is the
    group key's CRC-32, so hourly runs on the same data agree.  Returns the groups, sorted by key."""
    by = defaultdict(lambda: defaultdict(list))
    eng = {}
    for a in arrivals:
        gk = a["group_key"]
        by[gk][a["reporter_id"]].append((_epoch(a["time"]), a["delay_ns"] / 1e6,
                                         float(a["level_db"]), bool(a["live"])))
        sr = a.get("sweep_repetition_hz")
        if sr:
            eng[gk] = float(sr)
    site_of = site_map(by, rxpos, coherence)
    keys = [gk for gk in sorted(by)
            if policy.max_centre_hz is None or parse_key(gk)[0] < policy.max_centre_hz]
    groups = [analyse_group(gk, by[gk], sites, rxpos, eng.get(gk), coherence, site_of)
              for gk in keys]
    flag_images(groups)
    for g in groups:
        attribute_group(g, rxpos, policy, n_fake, zlib.crc32(g["key"].encode()))
    return groups


# ---------------------------------------------------------------- rows

def inputs_digest(arrivals):
    """sha256 hex over the sorted reporter|group|time|delay lines, as groundwave._key builds
    them.  The digest names the inputs, so an hourly run on unchanged data repeats its run_id."""
    lines = sorted("%s|%s|%s|%d" % (a["reporter_id"], a["group_key"], a["time"].isoformat(),
                                    a["delay_ns"]) for a in arrivals)
    return hashlib.sha256("\n".join(lines).encode()).hexdigest()


# Michael, 11-13: the ground-wave-only 900 km pass records its fits and never attaches names.
# So its rows never carry the word "named"; apply_attribution reads only "named" statuses.
_STATUS_AS = {GW900.name: {"named": "fits_gw900", "named_two_stations": "fits_gw900_two_stations"}}


def _ns(ms):
    return int(round(ms * 1e6))


def attribution_rows(groups, policy, run_at, window_from, window_to, digest):
    """sigmondb.attribution rows for one policy's groups.

    One row per fit in g["named"], best first, and one unattributable row (tx_id '', flags =
    [why]) per group with a verdict or with no fit.  Every row carries its group's set_aside and
    the eligible fake-site count (n_fake) beside p_null, so a reader sees the sampling
    uncertainty.  run_id hashes the inputs: a run on changed inputs adds rows beside the old
    ones, so readers take the latest run per (group_key, mode_policy) by argMax(run_at), not
    FINAL."""
    as_ = _STATUS_AS.get(policy.name, {})
    out = []
    for g in groups:
        if g.get("policy", policy.name) != policy.name:
            raise ValueError("group %s ran under %s, not %s" % (g["key"], g["policy"], policy.name))
        base = {"run_at": run_at, "run_id": "attr:%s:%s" % (ATTRIB_VERSION, digest[:12]),
                "window_from": window_from, "window_to": window_to, "group_key": g["key"],
                "period_ns": _ns(g["P"]), "mode_policy": policy.name,
                "n_fake": g.get("n_fake", 0), "set_aside": dict(g.get("set_aside", {})),
                "solver_version": ATTRIB_VERSION, "inputs_digest": digest}
        if g.get("verdict") or not g["named"]:
            usable = sorted(st for st, cl in g["stations"].items()
                            if any(not _set_aside(c) for c in cl))
            why = g.get("verdict") or "no candidate site fits two or more stations within tolerance"
            out.append(dict(base, tx_id="", alias_tx_ids=[], status="unattributable",
                            n_stations=len(usable), reporter_ids=usable, modes=[],
                            cluster_delays_ns=[], cluster_mad_ns=[], offset_ns=None,
                            offset_from="", spread_ns=None, p_null=None, runner_up_tx_id="",
                            runner_up_spread_ns=None, runner_up_fits=0, flags=[why]))
            continue
        for nm in g["named"]:
            cls = [g["stations"][m[0]][m[1]] for m in nm["mem"]]
            ground = nm["O_ground"] is not None
            ru = nm["runner"]
            pn = nm["p_null"]
            out.append(dict(
                base, tx_id=nm["site"]["id"], alias_tx_ids=list(nm["alias"]),
                status=as_.get(nm["status"], nm["status"]), n_stations=nm["n"],
                reporter_ids=[m[0] for m in nm["mem"]], modes=[m[2] for m in nm["mem"]],
                cluster_delays_ns=[_ns(c["D"]) for c in cls],
                cluster_mad_ns=[c["mad_us"] * 1e3 for c in cls],
                offset_ns=_ns(nm["O_ground"] if ground else nm["O"]),
                offset_from="ground_wave" if ground else "skywave_model",
                spread_ns=nm["spread"] * 1e3, p_null=pn if pn == pn else None,
                runner_up_tx_id=ru[2]["id"] if ru else "",
                runner_up_spread_ns=(ru[1] * 1e3 if ru and math.isfinite(ru[1]) else None),
                runner_up_fits=int(bool(ru) and not ru[0]), flags=list(nm["flags"])))
    return out
