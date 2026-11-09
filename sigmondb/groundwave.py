"""Ground-wave solver: fixed-path arrival clusters -> solution rows.

A ground wave keeps one delay through dawn and dusk.  An E hop appears after sunrise and
leaves after sunset; F-layer arrivals swing at night and fade by day.  The cluster method
mirrors tasks/codar-t6/stage2-groundwave/track.py: sort delays, split at gaps, keep clusters
seen in several captures.  Here a cluster must also show up by day and by night with
medians that agree.  Only anchored rows count (u_epoch_ns not null and <= 1000), and only
swept signals (rate_hz_per_s != 0): a chirp gives the delay, a fixed tone does not.

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
        rs = sorted(rs, key=lambda r: r["delay_ns"])
        clusters, cur = [], [rs[0]]
        for r in rs[1:]:
            if r["delay_ns"] - cur[-1]["delay_ns"] > gap_ns:
                clusters.append(cur)
                cur = []
            cur.append(r)
        clusters.append(cur)
        for cl in clusters:
            if len({r["time"] for r in cl}) < min_captures:
                continue
            el = [solar_elevation_deg(r["time"], lat, lon) for r in cl]
            dn = [r["delay_ns"] for r, e in zip(cl, el) if e < night_deg]
            dd = [r["delay_ns"] for r, e in zip(cl, el) if e > day_deg]
            if len(dn) < MIN_PART or len(dd) < MIN_PART:
                continue
            if abs(statistics.median(dn) - statistics.median(dd)) > max_sd_ns:
                continue
            delays = [r["delay_ns"] for r in cl]
            sd = statistics.stdev(delays)
            if sd > max_sd_ns:
                continue
            med = int(round(statistics.median(delays)))
            times = [r["time"] for r in cl]
            out.append(dict(
                cluster_key="%s|%s|%d" % (rid, gkey, round(med / 1000.0)),
                fixed_delay_ns=med, delay_sd_ns=sd, n_arrivals=len(cl),
                n_night=len(dn), n_day=len(dd),
                window_from=min(times), window_to=max(times), reporter_ids=[rid],
                group_key=gkey, tx_ids=sorted({r.get("tx_id") or "" for r in cl}),
                arrival_keys=sorted(_key(r) for r in cl)))
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
