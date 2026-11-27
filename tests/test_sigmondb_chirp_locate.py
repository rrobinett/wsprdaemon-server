"""chirp_locate: a chirp's position from cross-station arrival phases.  No ClickHouse.

Arrivals come from a made-up site and made-up stations through attribution's own ground-wave
and 1F models; no real station's position enters.
"""
import inspect
import os
import random
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from sigmondb import attribution  # noqa: E402
from sigmondb.chirp import load_rules  # noqa: E402
from sigmondb import chirp_locate  # noqa: E402
from sigmondb.chirp_locate import locate  # noqa: E402

RULES = load_rules()
P = 1000.0          # a 1 s sweep, in ms
O_TX = 123.456      # the transmitter's sweep start against the UTC second: unknown to locate
SITE = (38.0, -123.0)
KEYS = {"status", "lat", "lon", "pos_sigma_m", "spread_us", "n_stations", "stations"}


def delay_ms(site, pos, rid, policy=attribution.DEFAULT):
    """Ground wave where the policy allows it, else 1F at the nominal height."""
    d = attribution.gc_km(site, pos)
    ms = dict((m, p) for m, p, _ in attribution.modes(d, rid, policy, True, gw_ok=True))
    return ms["G"] if "G" in ms else ms["1F"]


def clusters(d_ms, seed):
    """One station's clusters from 20 captures scattered 2 us about d_ms."""
    rnd = random.Random(seed)
    rows = [(t, (d_ms + rnd.gauss(0, 0.002)) % P, 1.0) for t in range(20)]
    cl, _ = attribution.clusters_of(rows, P)
    return cl


def scene(site, stations, policy=attribution.DEFAULT):
    rxpos = {rid: (lat, lon, "gpsdo") for rid, (lat, lon) in stations.items()}
    st = {rid: clusters(O_TX + delay_ms(site, pos, rid, policy), i)
          for i, (rid, pos) in enumerate(sorted(stations.items()))}
    return st, rxpos


NEAR = {"RX-A": (38.5, -123.3), "RX-B": (37.6, -122.6), "RX-C": (38.2, -122.2)}


def test_three_ground_wave_stations_locate():
    st, rxpos = scene(SITE, NEAR)
    assert all(attribution.gc_km(SITE, p) <= 250 for p in NEAR.values())
    r = locate(st, rxpos, P, RULES)
    assert set(r) >= KEYS
    assert r["status"] == "located", r
    assert attribution.gc_km(SITE, (r["lat"], r["lon"])) <= 20
    assert r["n_stations"] == 3 and sorted(r["stations"]) == sorted(NEAR)
    # floored at the refine step (0.1 degree, about 11.1 km)
    assert 11_000 < r["pos_sigma_m"] <= RULES.established.max_pos_sigma_km * 1e3


def test_off_grid_site_still_locates():
    """The site sits on no grid point, so grid alignment cannot carry the fit.  A 1F
    twin about 120 km away also closes these delays; ground wave must win."""
    site = (38.03, -122.97)
    st, rxpos = scene(site, NEAR)
    r = locate(st, rxpos, P, RULES)
    assert r["status"] == "located", r
    assert attribution.gc_km(site, (r["lat"], r["lon"])) <= 20


def test_coarse_slack_lets_ground_wave_show():
    """A coarse point about 44 km from the site misses ground wave's 30 us tolerance; the
    coarse slack (half a cell diagonal over c) keeps the all-ground-wave tuple in view, so
    the refine step searches around the site rather than only around 1F fits."""
    st, rxpos = scene(SITE, NEAR)
    pt = (38.0, -122.5)
    tight = chirp_locate._best(pt, st, rxpos, P, attribution.DEFAULT, 0.0, 3)
    slack = chirp_locate._best(pt, st, rxpos, P, attribution.DEFAULT,
                               chirp_locate.COARSE_SLACK_MS, 3)
    assert tight is None or {m[2] for m in tight[2]} != {"G"}
    assert {m[2] for m in slack[2]} == {"G"}


def test_two_stations_too_few():
    two = {k: NEAR[k] for k in ("RX-A", "RX-B")}
    st, rxpos = scene(SITE, two)
    r = locate(st, rxpos, P, RULES)
    assert r["status"] == "too_few_stations"
    assert set(r) >= KEYS
    assert r["lat"] is None and r["lon"] is None and r["pos_sigma_m"] is None
    assert r["n_stations"] == 2


def test_mirror_geometry_ambiguous():
    """Three stations on one meridian (a great circle): the site and its mirror across it,
    about 176 km apart, see identical distances.  Ground wave over 900 km (attribution.GW900)
    keeps both islands tight; under DEFAULT the 100 km inland reach forces the stations so
    close that the two islands stretch toward the line and link within 2 * max_pos_sigma_km,
    and the result reads located with pos_sigma near 100 km, which the established gate
    refuses."""
    line = {"RX-A": (36.5, -124.0), "RX-B": (38.0, -124.0), "RX-C": (39.5, -124.0)}
    st, rxpos = scene(SITE, line, attribution.GW900)
    r = locate(st, rxpos, P, RULES, attribution.GW900)
    assert r["status"] == "ambiguous", r
    assert r["lat"] is None and r["lon"] is None and r["pos_sigma_m"] is None
    assert sorted(r["stations"]) == sorted(line)
    assert "(38.0, -123.0)" in r["note"] and "(38.0, -125.0)" in r["note"]


def test_inconsistent_delays_no_fit():
    rnd = random.Random(7)
    rxpos = {rid: (lat, lon, "gpsdo") for rid, (lat, lon) in NEAR.items()}
    st = {rid: clusters(rnd.uniform(0, P), i) for i, rid in enumerate(sorted(NEAR))}
    r = locate(st, rxpos, P, RULES)
    assert r["status"] == "no_fit", r
    assert r["lat"] is None and r["spread_us"] is None


def test_no_published_offset_used():
    params = list(inspect.signature(locate).parameters)
    assert params == ["st_clusters", "rxpos", "period_ms", "rules", "policy"]
    assert not any("offset" in p for p in params)
    st, rxpos = scene(SITE, NEAR)
    plain = locate(st, rxpos, P, RULES)
    # A transmitter row carrying a published offset, in scope and riding on every cluster,
    # changes nothing.
    tx_row = {"tx_id": "made-up", "lat": SITE[0], "lon": SITE[1], "offset_ms": O_TX + 7.0}
    with_tx = {rid: [dict(c, tx=tx_row, offset_ms=tx_row["offset_ms"]) for c in cl]
               for rid, cl in st.items()}
    assert locate(with_tx, rxpos, P, RULES) == plain
