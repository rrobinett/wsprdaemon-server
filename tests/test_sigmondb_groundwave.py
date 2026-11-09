"""sigmondb ground-wave solver.  No ClickHouse."""
import math
import os
import random
import sys
from datetime import datetime, timedelta

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from sigmondb.groundwave import (  # noqa: E402
    C_M_PER_S, fixed_clusters, ground_wave_solutions, solar_elevation_deg)

RATE = -25_733.913                    # the 4.463 MHz sweep, Hz/s
K3LR = dict(reporter_id="K3LR/D", lat=41.1875, lon=-80.458333)      # EN91se
WB6CXC = dict(reporter_id="WB6CXC/7", lat=48.479167, lon=-123.041667)  # CN88ll
RX = [K3LR, WB6CXC]


def _arrivals(centre, n=40, spread=2_000, night_h=(3, 9), day_h=(15, 20), u=230.0,
              tx_id="", day_centre=None, seed=1, reporter_id="K3LR/D", rate=RATE,
              day=datetime(2026, 11, 7)):
    """Alternate night and day arrivals.  Defaults: K3LR on 11-07, 03-09 Z (local night)
    and 15-20 Z (local day, sun above +12 deg)."""
    rnd = random.Random(seed)
    out = []
    for i in range(n):
        night = i % 2 == 0
        lo, hi = night_h if night else day_h
        span_s = (hi - lo) * 3600 - 60
        t = day + timedelta(hours=lo, seconds=((i // 2) * 997) % span_s)
        c = centre if (night or day_centre is None) else day_centre
        out.append(dict(time=t, reporter_id=reporter_id, tx_id=tx_id, group_key="4.4630 -25.73",
                        rate_hz_per_s=rate,
                        delay_ns=round(c + rnd.uniform(-spread, spread) / 2), u_epoch_ns=u))
    return out


# --- solar elevation ------------------------------------------------------------------------
# At upper culmination the sun stands at 90 - |lat - dec| degrees, at lower culmination at
# lat + dec - 90 (northern hemisphere).  At a solstice the declination equals the obliquity of
# the ecliptic, 23.44 deg (Meeus, Astronomical Algorithms, ch. 22 and 25).  Scanning a day in
# one-minute steps finds both culminations without needing the equation of time.

def _day_extremes(lat, lon, day):
    els = [solar_elevation_deg(day + timedelta(minutes=m), lat, lon) for m in range(1440)]
    return max(els), min(els)


def test_solar_noon_at_june_solstice():
    hi, _ = _day_extremes(40.0, -75.0, datetime(2026, 6, 21))
    assert abs(hi - (90.0 - (40.0 - 23.44))) < 0.1          # 73.44 deg


def test_solar_midnight_at_december_solstice():
    _, lo = _day_extremes(40.0, -75.0, datetime(2026, 12, 21))
    assert abs(lo - (40.0 - 23.44 - 90.0)) < 0.1           # -73.44 deg


def test_equinox_noon_at_the_equator_is_overhead_and_local_midnight_is_dark():
    # 2026-03-20 12:07 UTC: sun near Greenwich meridian, declination near 0.
    assert solar_elevation_deg(datetime(2026, 3, 20, 12, 7), 0.0, 0.0) > 89.0
    assert solar_elevation_deg(datetime(2026, 3, 20, 0, 7), 0.0, 0.0) < -89.0


# --- clusters -------------------------------------------------------------------------------

def test_steady_cluster_through_day_and_night_is_fixed():
    got = fixed_clusters(_arrivals(5_047_000), RX)
    assert len(got) == 1
    assert abs(got[0]["fixed_delay_ns"] - 5_047_000) < 2_000
    assert got[0]["n_arrivals"] == 40
    assert got[0]["reporter_ids"] == ["K3LR/D"]


def test_k3lr_cluster_spanning_a_real_night_and_day_is_fixed():
    # night 00-10 Z and day 14-21 Z at 41 N, 80 W in November
    got = fixed_clusters(_arrivals(42_557_000, night_h=(0, 10), day_h=(14, 21)), RX)
    assert len(got) == 1


def test_night_only_cluster_is_not_fixed():
    assert fixed_clusters(_arrivals(5_047_000, day_h=(3, 9)), RX) == []


def test_drifting_cluster_is_not_fixed():
    assert fixed_clusters(_arrivals(12_520_000, day_centre=12_475_000), RX) == []


def test_unanchored_rows_never_count():
    assert fixed_clusters(_arrivals(5_047_000, u=None), RX) == []
    assert fixed_clusters(_arrivals(5_047_000, u=5000.0), RX) == []


def test_wb6cxc_afternoon_utc_is_not_night():
    # 11-08, 17:55-21:30 Z and 22:30-23:58 Z: all local morning and afternoon at CN88ll.
    # The old fixed UTC windows called 22:30-23:58 Z "night" and fixed this cluster.
    arr = _arrivals(30_000_000, reporter_id="WB6CXC/7", day=datetime(2026, 11, 8),
                    night_h=(22.5, 23.96), day_h=(17.92, 21.5))
    for r in arr:
        assert r["time"].date() == datetime(2026, 11, 8).date()
    assert fixed_clusters(arr, RX) == []


def test_twilight_arrivals_count_for_neither():
    # K3LR 11-07: sun between -12 and +12 deg from about 11:00 to 13:30 Z.
    tw = [r for r in _arrivals(5_047_000, night_h=(11, 13), day_h=(11, 13))]
    for r in tw:
        assert -12.0 <= solar_elevation_deg(r["time"], K3LR["lat"], K3LR["lon"]) <= 12.0
    # twilight plus real day only: a pre-dawn arrival must not pass for night
    assert fixed_clusters(tw + _arrivals(5_047_000, n=10, night_h=(15, 20)), RX) == []
    assert min(solar_elevation_deg(r["time"], K3LR["lat"], K3LR["lon"]) for r in tw) < -3
    # twilight plus real night only: a post-sunrise arrival must not pass for day
    assert fixed_clusters(tw + _arrivals(5_047_000, n=10, day_h=(3, 9)), RX) == []
    assert max(solar_elevation_deg(r["time"], K3LR["lat"], K3LR["lon"]) for r in tw) > 3


def test_receiver_without_position_is_set_aside_with_a_reason():
    aside = {}
    rx = [dict(reporter_id="K3LR/D", lat=None, lon=None)]
    assert fixed_clusters(_arrivals(5_047_000), rx, set_aside=aside) == []
    assert "K3LR/D" in aside and "position" in aside["K3LR/D"]
    aside = {}
    assert fixed_clusters(_arrivals(5_047_000), [], set_aside=aside) == []
    assert "position" in aside["K3LR/D"]


def test_positions_may_be_a_map():
    got = fixed_clusters(_arrivals(5_047_000), {"K3LR/D": (K3LR["lat"], K3LR["lon"])})
    assert len(got) == 1


def test_unswept_arrivals_never_form_a_ground_wave_cluster():
    aside = {}
    assert fixed_clusters(_arrivals(5_047_000, rate=0.0), RX, set_aside=aside) == []
    assert "unswept" in aside["K3LR/D"]


# --- solutions ------------------------------------------------------------------------------

def _tx(lat=41.0, lon=-80.0, sig=10.0, tx_id="hfrnet:Rutgers"):
    return [dict(tx_id=tx_id, lat=lat, lon=lon, pos_sigma_m=sig,
                 valid_from=datetime(2026, 11, 9), version=1)]


def test_named_cluster_gets_an_offset():
    cl = fixed_clusters(_arrivals(1_000_000, tx_id="hfrnet:Rutgers"), RX)
    rx = [dict(reporter_id="K3LR/D", lat=41.0, lon=-81.0, pos_sigma_m=3000.0)]
    sol = ground_wave_solutions(cl, _tx(), rx, "gw-test")
    assert len(sol) == 1
    s = sol[0]
    assert s["tx_id"] == "hfrnet:Rutgers" and s["method"] == "ground_wave"
    assert abs(s["offset_ns"] - (1_000_000 - 279_925)) < 200     # 83.92 km
    assert len(s["inputs_digest"]) == 64 and s["solver_version"] == "gw-test"
    assert s["n_stations"] == 1 and s["n_arrivals"] == 40
    assert s["set_aside"] == {}


def test_offset_sigma_adds_delay_scatter_and_both_positions():
    cl = fixed_clusters(_arrivals(1_000_000, tx_id="hfrnet:Rutgers"), RX)
    cl[0]["delay_sd_ns"], cl[0]["n_arrivals"] = 6_000.0, 36
    rx = [dict(reporter_id="K3LR/D", lat=41.0, lon=-81.0, pos_sigma_m=3000.0)]
    s = ground_wave_solutions(cl, _tx(sig=4000.0), rx, "v")[0]
    # sqrt((6000/6)^2 + (5000 m / c)^2) = sqrt(1000^2 + 16678.2^2) = 16708.2 ns
    assert abs(s["offset_sigma_ns"] - 16_708.2) < 0.1
    assert abs(s["offset_sigma_ns"]
               - math.hypot(1000.0, 5000.0 / C_M_PER_S * 1e9)) < 1e-6


def test_unknown_tx_sigma_counts_zero_and_says_so():
    cl = fixed_clusters(_arrivals(1_000_000, tx_id="hfrnet:Rutgers"), RX)
    cl[0]["delay_sd_ns"], cl[0]["n_arrivals"] = 6_000.0, 36
    rx = [dict(reporter_id="K3LR/D", lat=41.0, lon=-81.0, pos_sigma_m=3000.0)]
    s = ground_wave_solutions(cl, _tx(sig=None), rx, "v")[0]
    assert abs(s["offset_sigma_ns"] - math.hypot(1000.0, 3000.0 / C_M_PER_S * 1e9)) < 1e-6
    assert s["set_aside"] == {"tx_pos_sigma": "unknown"}


def test_named_cluster_beyond_ground_range_gets_no_offset():
    cl = fixed_clusters(_arrivals(1_000_000, tx_id="hfrnet:Rutgers"), RX)
    rx = [dict(reporter_id="K3LR/D", lat=41.0, lon=-81.0, pos_sigma_m=3000.0)]
    far = _tx(lat=41.0, lon=-75.0)                                    # ~503 km
    s = ground_wave_solutions(cl, far, rx, "v")[0]
    assert s["offset_ns"] is None and s["offset_sigma_ns"] is None
    assert s["set_aside"]["range"] == "503 km"
    s = ground_wave_solutions(cl, far, rx, "v", max_ground_km=600)[0]
    assert s["offset_ns"] is not None and "range" not in s["set_aside"]


def test_unnamed_cluster_has_no_offset():
    cl = fixed_clusters(_arrivals(1_000_000), RX)
    sol = ground_wave_solutions(cl, [], [dict(reporter_id="K3LR/D", lat=40.0, lon=-75.0)], "v")
    assert sol[0]["offset_ns"] is None and sol[0]["tx_id"] == ""
    again = ground_wave_solutions(cl, [], [], "v")
    assert again[0]["inputs_digest"] == sol[0]["inputs_digest"]
