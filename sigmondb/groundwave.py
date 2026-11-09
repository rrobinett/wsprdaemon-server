"""Ground-wave solver: fixed-path arrival clusters -> solution rows.

A ground wave keeps one delay through dawn and dusk.  An E hop appears after sunrise and
leaves after sunset; F-layer arrivals swing at night and fade by day.  The cluster method
mirrors tasks/codar-t6/stage2-groundwave/track.py: sort delays, split at gaps, keep clusters
seen in several captures.  Here a cluster must also show up by day and by night with
medians that agree.  Only anchored rows count (u_epoch_ns not null and <= 1000).
"""
import hashlib
import math
import statistics
from collections import defaultdict

C_M_PER_S = 299_792_458.0
EARTH_R_M = 6_371_008.8
U_MAX_NS = 1000.0
MIN_PART = 3   # arrivals needed in each of day and night to judge a cluster


def _hour(t):
    return t.hour + t.minute / 60.0 + t.second / 3600.0 + t.microsecond / 3.6e9


def _within(h, win):
    lo, hi = win
    return (lo <= h < hi) if lo <= hi else (h >= lo or h < hi)


def _key(r):
    return "%s|%s|%s|%d" % (r["reporter_id"], r["group_key"], r["time"].isoformat(),
                            r["delay_ns"])


def fixed_clusters(arrivals, gap_ns=120_000, min_captures=6, max_sd_ns=20_000.0,
                   night=(22.5, 11.5), day=(12.5, 21.5)):
    groups = defaultdict(list)
    for r in arrivals:
        u = r.get("u_epoch_ns")
        if u is None or u > U_MAX_NS:
            continue
        groups[(r["reporter_id"], r["group_key"])].append(r)
    out = []
    for (rid, gkey), rs in sorted(groups.items()):
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
            dn = [r["delay_ns"] for r in cl if _within(_hour(r["time"]), night)]
            dd = [r["delay_ns"] for r in cl if _within(_hour(r["time"]), day)]
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


def ground_wave_solutions(clusters, transmitters, receivers, solver_version):
    txs = _latest(transmitters, "tx_id")
    rxs = _latest(receivers, "reporter_id")
    out = []
    for c in clusters:
        named = [t for t in c.get("tx_ids", []) if t]
        tx_id, offset, lat, lon, sig = "", None, None, None, None
        tx = txs.get(named[0]) if len(named) == 1 and len(c["tx_ids"]) == 1 else None
        if tx is not None:
            tx_id = tx["tx_id"]
            lat, lon, sig = tx.get("lat"), tx.get("lon"), tx.get("pos_sigma_m")
            rx = rxs.get(c["reporter_ids"][0]) if len(c["reporter_ids"]) == 1 else None
            if (lat is not None and lon is not None and rx is not None
                    and rx.get("lat") is not None and rx.get("lon") is not None):
                d = great_circle_m(lat, lon, rx["lat"], rx["lon"])
                offset = c["fixed_delay_ns"] - round(d / C_M_PER_S * 1e9)
        digest = hashlib.sha256("\n".join(sorted(c["arrival_keys"])).encode()).hexdigest()
        out.append(dict(
            tx_id=tx_id, cluster_key=c["cluster_key"],
            solve_id="gw:%s:%s" % (solver_version, digest[:12]),
            window_from=c["window_from"], window_to=c["window_to"],
            reporter_ids=list(c["reporter_ids"]), fixed_delay_ns=c["fixed_delay_ns"],
            delay_sd_ns=c["delay_sd_ns"], offset_ns=offset, offset_sigma_ns=None,
            lat=lat, lon=lon, pos_sigma_m=sig, method="ground_wave",
            n_stations=len(c["reporter_ids"]), n_arrivals=c["n_arrivals"], set_aside={},
            solver_version=solver_version, inputs_digest=digest))
    return out
