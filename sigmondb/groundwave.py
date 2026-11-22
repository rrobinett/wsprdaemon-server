"""Ground-wave solver: fixed-path arrival clusters -> solution rows.

A ground wave keeps one delay through dawn and dusk.  An E hop appears after sunrise and
leaves after sunset; F-layer arrivals swing at night and fade by day.  The cluster method
mirrors tasks/codar-t6/stage2-groundwave/track.py: sort delays, split at gaps, keep clusters
seen in several captures.  Here a cluster must also show up by day and by night with
medians that agree.  Only anchored rows count (u_epoch_ns not null and <= 1000), and only
swept signals (rate_hz_per_s != 0): a chirp gives the delay, a fixed tone does not.

delay_ns lives modulo the sweep period P, which the group key gives (half_sweep.period_and_sweep,
the parser the readers already use).  So clustering runs on the circle: the scan starts in the
widest gap, a cluster may run across 0/P, and its median, day/night test and SD come from the
delays unwrapped around the cluster; the median folds back into [0, P).  A group whose key gives
no period clusters on the line, as before: nothing tells where its wrap falls.  The offset, a
phase modulo P too, comes out in [-P/2, P/2).

Day and night come from the sun's elevation at the receiver, arrival by arrival: night below
-12 deg, day above +12 deg, and twilight between counts for neither.  No UTC hours are fixed,
so the test holds at any longitude.  The transmitter's sky does not enter: an unnamed cluster
has no transmitter position, and within ground-wave range the two skies nearly agree.
"""
import hashlib
import math
import statistics
from collections import defaultdict
from datetime import datetime

from sigmondb.half_sweep import period_and_sweep

C_M_PER_S = 299_792_458.0
EARTH_R_M = 6_371_008.8
U_MAX_NS = 1000.0
MIN_PART = 3   # arrivals needed in each of day and night to judge a cluster
NIGHT_DEG = -12.0
DAY_DEG = 12.0
MAX_GROUND_KM = 300.0
_J2000 = datetime(2000, 1, 1, 12)


def solar_elevation_deg(t, lat, lon):
    """Sun elevation in degrees at naive-UTC time t, latitude lat, east longitude lon.

    The low-precision solar position of the Astronomical Almanac (USNO; Meeus ch. 25 gives the
    same series), good to about 0.01 deg in declination and right ascension from 1950 to 2050.
    Refraction is ignored; it matters only within a degree of the horizon, far from +-12 deg.
    """
    d = (t - _J2000).total_seconds() / 86400.0
    g = math.radians((357.529 + 0.98560028 * d) % 360.0)
    q = (280.459 + 0.98564736 * d) % 360.0
    lam = math.radians(q + 1.915 * math.sin(g) + 0.020 * math.sin(2 * g))
    eps = math.radians(23.439 - 0.00000036 * d)
    ra = math.atan2(math.cos(eps) * math.sin(lam), math.cos(lam))
    dec = math.asin(math.sin(eps) * math.sin(lam))
    gmst_deg = (280.46061837 + 360.98564736629 * d) % 360.0
    ha = math.radians(gmst_deg + lon) - ra
    phi = math.radians(lat)
    s = math.sin(phi) * math.sin(dec) + math.cos(phi) * math.cos(dec) * math.cos(ha)
    return math.degrees(math.asin(max(-1.0, min(1.0, s))))


def _key(r):
    return "%s|%s|%s|%d" % (r["reporter_id"], r["group_key"], r["time"].isoformat(),
                            r["delay_ns"])


def _positions(receivers):
    """reporter_id -> (lat, lon) from receiver rows (latest per id) or from a ready map."""
    if isinstance(receivers, dict):
        return {k: v for k, v in receivers.items()
                if v is not None and v[0] is not None and v[1] is not None}
    out = {}
    for rid, r in _latest(receivers or [], "reporter_id").items():
        if r.get("lat") is not None and r.get("lon") is not None:
            out[rid] = (r["lat"], r["lon"])
    return out


def period_ns(group_key):
    """The sweep period of a group key in ns (half_sweep.period_and_sweep), or None."""
    ps = period_and_sweep(group_key)
    return round(ps[0] * 1e9) if ps else None


def _split(rs, gap_ns, P):
    """Gap clusters of rows by delay_ns, as [(rows, delays)].

    With a period P the delays sit on a circle: the scan starts after the widest gap, so a
    cluster that runs across 0/P stays whole, and each cluster's delays come back unwrapped
    from its first one (they may pass P).  With P None the scan runs on the line and the
    delays come back as stored."""
    if not P:
        rs = sorted(rs, key=lambda r: r["delay_ns"])
        pos = [r["delay_ns"] for r in rs]
    else:
        rs = sorted(rs, key=lambda r: r["delay_ns"] % P)
        pos = [r["delay_ns"] % P for r in rs]
        n = len(rs)
        widest = max(range(n), key=lambda i: ((pos[(i + 1) % n] - pos[i]) % P, -i))
        start = (widest + 1) % n
        rs, pos = rs[start:] + rs[:start], pos[start:] + pos[:start]
    out, cur, xs = [], [rs[0]], [pos[0]]
    for r, x in zip(rs[1:], pos[1:]):
        step = (x - xs[-1]) % P if P else x - xs[-1]
        if step > gap_ns:
            out.append((cur, xs))
            cur, xs = [r], [x]
        else:
            cur.append(r)
            xs.append(xs[-1] + step)
    out.append((cur, xs))
    return out


def fixed_clusters(arrivals, receivers, gap_ns=120_000, min_captures=6, max_sd_ns=20_000.0,
                   night_deg=NIGHT_DEG, day_deg=DAY_DEG, set_aside=None):
    """Clusters that hold one delay by day and by night at their receiver.

    receivers: receiver rows (reporter_id, lat, lon, ...) or a map reporter_id -> (lat, lon).
    set_aside: optional dict; for each reporter_id whose arrivals were dropped before
    clustering it receives the reason (no receiver position, unswept signal).
    """
    pos = _positions(receivers)
    dropped = defaultdict(lambda: defaultdict(int))
    groups = defaultdict(list)
    for r in arrivals:
        u = r.get("u_epoch_ns")
        if u is None or u > U_MAX_NS:
            continue
        # Only a T6-anchored arrival is a ground-wave ruler.  hfdl-recorder (1f92973) labels rows with the
        # station's own tier; a "t6" row (T6 held, anchor never matched) can carry a sub-us sigma too.
        if r.get("origin", "t6_anchor") != "t6_anchor":
            continue
        rid = r["reporter_id"]
        if not r.get("rate_hz_per_s"):
            dropped[rid]["unswept signal"] += 1
            continue
        if rid not in pos:
            dropped[rid]["no receiver position"] += 1
            continue
        groups[(rid, r["group_key"])].append(r)
    if set_aside is not None:
        for rid, why in sorted(dropped.items()):
            set_aside[rid] = "; ".join("%s (%d arrivals)" % (k, n) for k, n in sorted(why.items()))
    out = []
    for (rid, gkey), rs in sorted(groups.items()):
        lat, lon = pos[rid]
        P = period_ns(gkey)
        found = []
        for cl, xs in _split(rs, gap_ns, P):
            if len({r["time"] for r in cl}) < min_captures:
                continue
            el = [solar_elevation_deg(r["time"], lat, lon) for r in cl]
            dn = [x for x, e in zip(xs, el) if e < night_deg]
            dd = [x for x, e in zip(xs, el) if e > day_deg]
            if len(dn) < MIN_PART or len(dd) < MIN_PART:
                continue
            if abs(statistics.median(dn) - statistics.median(dd)) > max_sd_ns:
                continue
            sd = statistics.stdev(xs)
            if sd > max_sd_ns:
                continue
            med = int(round(statistics.median(xs)))
            tok = round(med / 1000.0)
            if P:
                med %= P
                tok %= max(1, round(P / 1000.0))    # 999.9997 ms of 1 s names the 0 us cluster
            times = [r["time"] for r in cl]
            found.append(dict(
                cluster_key="%s|%s|%d" % (rid, gkey, tok),
                fixed_delay_ns=med, delay_sd_ns=sd, n_arrivals=len(cl),
                n_night=len(dn), n_day=len(dd),
                window_from=min(times), window_to=max(times), reporter_ids=[rid],
                group_key=gkey, tx_ids=sorted({r.get("tx_id") or "" for r in cl}),
                arrival_keys=sorted(_key(r) for r in cl)))
        out.extend(sorted(found, key=lambda c: c["fixed_delay_ns"]))
    return out


def great_circle_m(lat1, lon1, lat2, lon2):
    p1, p2 = math.radians(lat1), math.radians(lat2)
    dl = math.radians(lon2 - lon1)
    a = (math.sin((p2 - p1) / 2) ** 2
         + math.cos(p1) * math.cos(p2) * math.sin(dl / 2) ** 2)
    return 2 * EARTH_R_M * math.asin(min(1.0, math.sqrt(a)))


def _latest(rows, key):
    best = {}
    for r in rows:
        k = r[key]
        if k not in best or (r.get("valid_from"), r.get("version") or 0) >= (
                best[k].get("valid_from"), best[k].get("version") or 0):
            best[k] = r
    return best


def ground_wave_solutions(clusters, transmitters, receivers, solver_version,
                          max_ground_km=MAX_GROUND_KM):
    """One solution row per cluster.  A cluster with one named transmitter within
    max_ground_km of its one receiver gets offset = fixed delay - distance/c, and

        offset_sigma = sqrt((delay_sd / sqrt(n))^2 + (sqrt(rx_sigma^2 + tx_sigma^2) / c)^2).

    The offset, a phase modulo the group key's period P, comes back in [-P/2, P/2): a fixed
    delay of 958.94 ms in a 1 s period gives about -41 ms (early), not +959 ms (late).  A key
    with no period leaves the offset unfolded.

    An unknown position sigma counts as 0 and set_aside names it.  Beyond max_ground_km the
    path is not a ground wave: no offset, and set_aside["range"] gives the distance.
    """
    txs = _latest(transmitters, "tx_id")
    rxs = _latest(receivers, "reporter_id")
    out = []
    for c in clusters:
        named = [t for t in c.get("tx_ids", []) if t]
        tx_id, offset, osig, lat, lon, sig, aside = "", None, None, None, None, None, {}
        tx = txs.get(named[0]) if len(named) == 1 and len(c["tx_ids"]) == 1 else None
        if tx is not None:
            tx_id = tx["tx_id"]
            lat, lon, sig = tx.get("lat"), tx.get("lon"), tx.get("pos_sigma_m")
            rx = rxs.get(c["reporter_ids"][0]) if len(c["reporter_ids"]) == 1 else None
            if (lat is not None and lon is not None and rx is not None
                    and rx.get("lat") is not None and rx.get("lon") is not None):
                d = great_circle_m(lat, lon, rx["lat"], rx["lon"])
                if d > max_ground_km * 1000.0:
                    aside["range"] = "%.0f km" % (d / 1000.0)
                else:
                    offset = c["fixed_delay_ns"] - round(d / C_M_PER_S * 1e9)
                    P = period_ns(c.get("group_key"))
                    if P:               # a phase modulo P: the value nearest zero
                        offset = (offset + P // 2) % P - P // 2
                    rs, ts = rx.get("pos_sigma_m"), sig
                    if rs is None:
                        aside["rx_pos_sigma"] = "unknown"
                        rs = 0.0
                    if ts is None:
                        aside["tx_pos_sigma"] = "unknown"
                        ts = 0.0
                    pos_ns = math.hypot(rs, ts) / C_M_PER_S * 1e9
                    osig = math.hypot(c["delay_sd_ns"] / math.sqrt(c["n_arrivals"]), pos_ns)
        digest = hashlib.sha256("\n".join(sorted(c["arrival_keys"])).encode()).hexdigest()
        out.append(dict(
            tx_id=tx_id, cluster_key=c["cluster_key"],
            solve_id="gw:%s:%s" % (solver_version, digest[:12]),
            window_from=c["window_from"], window_to=c["window_to"],
            reporter_ids=list(c["reporter_ids"]), fixed_delay_ns=c["fixed_delay_ns"],
            delay_sd_ns=c["delay_sd_ns"], offset_ns=offset, offset_sigma_ns=osig,
            lat=lat, lon=lon, pos_sigma_m=sig, method="ground_wave",
            n_stations=len(c["reporter_ids"]), n_arrivals=c["n_arrivals"], set_aside=aside,
            solver_version=solver_version, inputs_digest=digest))
    return out
