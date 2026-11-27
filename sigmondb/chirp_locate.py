"""A chirp's position from the arrival phases several T6 stations see.

Each station i hears the chirp at D_i = O_tx + p_i (mod the period), where p_i comes from the
path model attribution uses (ground wave = distance / c; 1F/2F = a mirror path at virtual
height h).  O_tx, the transmitter's sweep start against the UTC second, stays unknown: no
published offset enters (Michael's rule of 11-07, as in attribution.py).  A trial position
fits when O = D_i - p_i agrees across the stations, so each tuple fixes only differences
between stations, and three stations give two equations in two unknowns.

The search (attribution's per-site arithmetic, scanned over a grid):
1. Fewer than established.min_t6_stations independent stations (the caller collapses
   co-sited reporters first): too_few_stations.
2. Coarse grid at 1 degree over the box of the hearing stations plus 35 degrees (the
   3800 km skywave limit).  Latitude clips to [-90, 90] and longitude to [-180, 180]; the box
   never wraps the antimeridian, since the fleet sits in the Americas.  A coarse point may
   sit up to half a cell diagonal from the transmitter, so every tolerance there widens by
   that distance over c; otherwise ground wave, with its 30 us tolerance, could never show
   on the coarse grid.
3. Refine at 0.1 degree within 2 degrees of each coarse point that scores within 1.5 times
   the best coarse score, now at the model's own tolerances.
4. The winner: among refined points whose best set holds every station within each
   member's tolerance, the lowest score.  None: no_fit.
5. Feasible points: every evaluated point at which the winning tuple, with each station's
   mode held fixed, fits within every member's tolerance.  Feasible points
   that split, by single linkage at 2 * max_pos_sigma_km, into more than one region:
   ambiguous, with each region's centre in "note".
6. Otherwise located at the winner.  pos_sigma_m is the RMS distance of the feasible points
   from it, floored at the refine step (about 11 km).

Why score = spread_us + the members' mean tolerance in us, and why feasibility holds the
modes fixed: 1F carries a tolerance near 0.48 ms (half the 250-350 km height spread plus
0.15 ms of ionospheric wander), which admits about 140 km of path difference.  Judged on
spread alone, or with the modes left free, nearly every grid point fits by 1F, and close
ground-wave delays also close exactly on a 1F "twin" about 120 km away (measured
2026-11-27: 1.0 us against the true site's 0.0 us).  The score therefore prefers the fit
whose model claims most, and the feasible region is that fit's own.  A fix that only 1F
supports yields a feasible region hundreds of km wide, so its pos_sigma_m fails the
established gate (max_pos_sigma_km); that refusal is deliberate, not a defect.
"""
import math

import numpy as np

from sigmondb import attribution
from sigmondb.attribution import C_KM_S, RE, cdiff, modes

COARSE_DEG = 1.0
FINE_DEG = 0.1
SKY_REACH_DEG = 35.0        # 3800 km skywave limit, in degrees of arc (rounded up)
REFINE_DEG = 2.0
REFINE_FACTOR = 1.5
KM_PER_DEG = math.pi * RE / 180.0
# The farthest a coarse point sits from the transmitter: half the cell diagonal at the equator.
COARSE_SLACK_MS = 0.5 * math.sqrt(2.0) * COARSE_DEG * KM_PER_DEG / C_KM_S * 1e3


def _result(status, n, stations, lat=None, lon=None, sigma_m=None, spread_us=None):
    return {"status": status, "lat": lat, "lon": lon, "pos_sigma_m": sigma_m,
            "spread_us": spread_us, "n_stations": n, "stations": sorted(stations)}


def _best(point, st_clusters, rxpos, P, policy, slack_ms, n):
    """The lowest-scoring set at point that holds all n stations, or None.

    Returns (score_us, spread_us, members), members as (station, cluster, mode, tol ms).
    Each entry's cluster index carries its mode and tolerance, so best_sets keeps a ground-wave
    tuple and a 1F tuple over the same clusters apart rather than merging them."""
    ents = [(st, (j, m, tol), m, o, tol + slack_ms, d)
            for st, j, m, o, tol, d in attribution.entries(point, st_clusters, rxpos, P, policy)]
    best = None
    for k, spread, _ob, mem in attribution.best_sets(ents, P, min_st=n):
        if k < n:
            break
        tols = [e[1][2] for e in mem]
        score = spread + sum(tols) / len(tols) * 1e3
        if best is None or score < best[0]:
            best = (score, spread, [(e[0], e[1][0], e[1][1], e[1][2]) for e in mem])
    return best


def _fixed_fit(point, tup, st_clusters, rxpos, P, policy):
    """The winning tuple at point, each station's mode held fixed: (spread us, all within
    tolerance), or None when some station's mode does not reach this point."""
    picked = []
    for st, j, mname, _ in tup:
        c = st_clusters[st][j]
        d = attribution.gc_km(point, rxpos[st][:2])
        hit = [(p, tol) for m, p, tol in modes(d, st, policy, c["mad_us"] <= attribution.TIGHT_US,
                                               gw_ok=attribution._gw_ok(rxpos, st))
               if m == mname]
        if not hit:
            return None
        p, tol = hit[0]
        picked.append(((c["D"] - p) % P, tol))
    w = [1 / tol ** 2 for _, tol in picked]
    a = picked[0][0]
    ob = (a + sum(wi * cdiff(o, a, P) for wi, (o, _) in zip(w, picked)) / sum(w)) % P
    os_ = [cdiff(o, ob, P) for o, _ in picked]
    return (max(os_) - min(os_)) * 1e3, all(abs(x) <= tol for x, (_, tol) in zip(os_, picked))


def _axis(lo, hi, step):
    """Grid values that are whole multiples of step within [lo, hi]."""
    i0, i1 = math.ceil(lo / step - 1e-9), math.floor(hi / step + 1e-9)
    return [round(i * step, 6) for i in range(i0, i1 + 1)]


def _km_from(lat, lon, lats, lons):
    """Great-circle km from (lat, lon) to each of the arrays lats, lons."""
    p1, l1 = math.radians(lat), math.radians(lon)
    p2, l2 = np.radians(lats), np.radians(lons)
    h = np.sin((p2 - p1) / 2) ** 2 + math.cos(p1) * np.cos(p2) * np.sin((l2 - l1) / 2) ** 2
    return 2 * RE * np.arcsin(np.minimum(1.0, np.sqrt(h)))


def _regions(points, link_km):
    """Single-linkage groups of points (lat, lon) at link_km.  Returns lists of indices."""
    lats = np.array([p[0] for p in points])
    lons = np.array([p[1] for p in points])
    left = np.ones(len(points), dtype=bool)
    out = []
    for seed in range(len(points)):
        if not left[seed]:
            continue
        left[seed] = False
        group, frontier = [seed], [seed]
        while frontier:
            i = frontier.pop()
            near = np.flatnonzero(left & (_km_from(lats[i], lons[i], lats, lons) <= link_km))
            left[near] = False
            group.extend(near.tolist())
            frontier.extend(near.tolist())
        out.append(group)
    return out


def locate(st_clusters, rxpos, period_ms, rules, policy=attribution.DEFAULT):
    """Position of a chirp from per-station arrival clusters.

    st_clusters: independent station -> clusters (attribution.clusters_of), already collapsed
    to one reporter per site.  rxpos: station -> (lat, lon, pos_source).  period_ms: the
    folding period P.  Returns a dict with status (located, ambiguous, too_few_stations,
    no_fit), lat, lon, pos_sigma_m, spread_us, n_stations, stations; ambiguous adds note."""
    P = float(period_ms)
    est = rules.established
    names = list(st_clusters)
    n = len(names)
    if n < est.min_t6_stations:
        return _result("too_few_stations", n, names)

    lats = [rxpos[s][0] for s in names]
    lons = [rxpos[s][1] for s in names]
    la_ax = _axis(max(-90.0, min(lats) - SKY_REACH_DEG), min(90.0, max(lats) + SKY_REACH_DEG),
                  COARSE_DEG)
    lo_ax = _axis(max(-180.0, min(lons) - SKY_REACH_DEG), min(180.0, max(lons) + SKY_REACH_DEG),
                  COARSE_DEG)
    coarse = {}
    for la in la_ax:
        for lo in lo_ax:
            b = _best((la, lo), st_clusters, rxpos, P, policy, COARSE_SLACK_MS, n)
            if b is not None:
                coarse[(la, lo)] = b
    if not coarse:
        return _result("no_fit", n, names)

    cut = REFINE_FACTOR * min(b[0] for b in coarse.values())
    steps = int(round(REFINE_DEG / FINE_DEG))
    fine = {}
    for (la, lo), b in coarse.items():
        if b[0] > cut:
            continue
        for i in range(-steps, steps + 1):
            for k in range(-steps, steps + 1):
                pt = (round(la + i * FINE_DEG, 6), round(lo + k * FINE_DEG, 6))
                if pt in fine or not (-90 <= pt[0] <= 90 and -180 <= pt[1] <= 180):
                    continue
                fine[pt] = _best(pt, st_clusters, rxpos, P, policy, 0.0, n)

    win = None
    for pt, b in fine.items():
        if b is None or not _fixed_fit(pt, b[2], st_clusters, rxpos, P, policy)[1]:
            continue
        if win is None or b[0] < win[1][0]:
            win = (pt, b)
    if win is None:
        return _result("no_fit", n, names)
    (wlat, wlon), (_score, spread, tup) = win

    feasible = []
    for pt in list(coarse) + list(fine):
        f = _fixed_fit(pt, tup, st_clusters, rxpos, P, policy)
        if f is not None and f[1]:
            feasible.append(pt)
    feasible = sorted(set(feasible))
    regions = _regions(feasible, 2 * est.max_pos_sigma_km)
    stations = [m[0] for m in tup]
    if len(regions) > 1:
        centres = [(round(float(np.mean([feasible[i][0] for i in g])), 1),
                    round(float(np.mean([feasible[i][1] for i in g])), 1)) for g in regions]
        r = _result("ambiguous", n, stations)
        r["note"] = ("feasible regions centred at "
                     + " and ".join(f"({la:.1f}, {lo:.1f})" for la, lo in centres))
        return r
    km = _km_from(wlat, wlon, np.array([p[0] for p in feasible]),
                  np.array([p[1] for p in feasible]))
    sigma_m = max(float(np.sqrt(np.mean(km ** 2))), FINE_DEG * KM_PER_DEG) * 1e3
    return _result("located", n, stations, wlat, wlon, round(sigma_m, 1), round(spread, 3))
