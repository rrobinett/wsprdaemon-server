"""Group matching, ground-wave range and path midpoints (pure functions).

Arrivals carry an empty tx_id and a group_key.  match_group maps one to the
sites that share its centre and sweep; it never names a single site.
"""
import math

from .groundwave import great_circle_m

MATCH_FREQ_HZ = 3000.0


def match_rate_tol(rate: float) -> float:
    return max(500.0, 0.005 * abs(rate))


def coast_of(lon: float) -> str:
    return "E" if lon > -100 else "W"


def match_group(group_key, centre_hz, rate_hz_per_s, station, codar_txs):
    """Sites sharing the arrival's centre and sweep, on the coast nearest `station`."""
    tol = match_rate_tol(rate_hz_per_s)
    cand = [s for s in codar_txs
            if abs(s["centre_hz"] - centre_hz) <= MATCH_FREQ_HZ
            and abs(s["rate_hz_per_s"] - rate_hz_per_s) <= tol]
    if not cand:
        return None
    coasts = {}
    for s in cand:
        coasts.setdefault(coast_of(s["lon"]), []).append(s)

    def dmin(c):
        return min(great_circle_m(station[0], station[1], s["lat"], s["lon"])
                   for s in coasts[c])

    near = min(coasts, key=dmin)
    return {"group_id": f"{group_key} {near}",
            "coast": near,
            "members": sorted(s["tx_id"] for s in coasts[near]),
            "both_coasts": len(coasts) > 1,
            "nearest_km": dmin(near) / 1000.0}


def ground_range_km(centre_hz: float) -> float:
    if centre_hz < 6e6:
        return 250.0
    if centre_hz >= 12e6:
        return 150.0
    return 200.0


def path_kind(distance_km: float, centre_hz: float) -> str:
    return "ground_range" if distance_km <= ground_range_km(centre_hz) else "skywave"


def gc_midpoint(lat1, lon1, lat2, lon2):
    p1, l1, p2, l2 = map(math.radians, (lat1, lon1, lat2, lon2))
    bx = math.cos(p2) * math.cos(l2 - l1)
    by = math.cos(p2) * math.sin(l2 - l1)
    lat = math.atan2(math.sin(p1) + math.sin(p2),
                     math.hypot(math.cos(p1) + bx, by))
    lon = l1 + math.atan2(by, math.cos(p1) + bx)
    return (math.degrees(lat), (math.degrees(lon) + 540) % 360 - 180)
