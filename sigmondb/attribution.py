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
import math
import statistics
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


def modes(d, rx, policy, tight=True, gw_ok=True):
    """Possible propagation modes for a path of d km: (name, delay ms, tolerance ms).

    Ground wave only for a cluster that holds still (tight), since a ground-wave path cannot
    wander, and only at a receiver whose position came from its GPSDO (gw_ok); 1F up to
    3800 km, 2F from 3000 km."""
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


def collapse_colocated(rows_by_st, rxpos, coherence):
    """Count reporters at one site as one station.

    Reporters that share a non-empty coherence group, or stand within CO_SITE_KM of each other,
    form one site.  The site keeps the reporter with the most captures (a reporter with a
    position beats one without, so a site never loses its position); the others go to the
    returned set-aside map.  Returns (rows_by_st kept, {reporter: reason})."""
    coherence = coherence or {}
    sts = sorted(rows_by_st)
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
    sites = {}
    for s in sts:
        sites.setdefault(find(s), []).append(s)
    kept, aside = {}, {}
    for members in sites.values():
        best = max(members, key=lambda s: (s in rxpos, len({r[0] for r in rows_by_st[s]}), s))
        kept[best] = rows_by_st[best]
        for s in members:
            if s != best:
                aside[s] = f"co-located with {best}; counted once"
    return dict(sorted(kept.items())), aside


def analyse_group(gk, rows_by_st, sites, rxpos, eng_period, coherence=None):
    """Clusters per station for one sweep group, with the rung and twin guards applied.

    rows_by_st: reporter -> [(t, d_ms, level, live)].  rxpos: reporter -> (lat, lon, pos_source).
    Images need every group at once; flag_images runs after this, across groups."""
    centre, rate, kperiod = parse_key(gk)
    cands, opp_sign = candidates_for(centre, rate, sites)
    if kperiod:
        P_s = kperiod
    elif cands:
        P_s = statistics.mode(round(s["period"], 4) for s in cands)
    elif eng_period:
        P_s = 1.0 / eng_period
    else:
        P_s = 1.0
    P = P_s * 1e3
    R = rung_spacing_ms(centre, rate)
    g = {"key": gk, "centre": centre, "rate": rate, "P": P, "R": R, "cands": cands,
         "opp_sign": opp_sign, "stations": {}, "caps": {}, "sets": [], "named": [], "notes": [],
         "set_aside": {}}
    rows_by_st, aside = collapse_colocated(rows_by_st, rxpos, coherence)
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
