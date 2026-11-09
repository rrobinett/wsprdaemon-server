"""sigmondb ground-wave solver.  No ClickHouse."""
import os
import random
import sys
from datetime import datetime, timedelta

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from sigmondb.groundwave import fixed_clusters, ground_wave_solutions  # noqa: E402


def _arrivals(centre, n=40, spread=2_000, night_h=(2, 11), day_h=(13, 21), u=230.0,
              tx_id="", day_centre=None, seed=1):
    rnd = random.Random(seed)
    out = []
    for i in range(n):
        night = i % 2 == 0
        lo, hi = night_h if night else day_h
        t = datetime(2026, 11, 7, lo) + timedelta(minutes=(i // 2) * 7 % ((hi - lo) * 60))
        t += timedelta(seconds=i)
        c = centre if (night or day_centre is None) else day_centre
        out.append(dict(time=t, reporter_id="K3LR/D", tx_id=tx_id, group_key="4.4630 -25.73",
                        delay_ns=round(c + rnd.uniform(-spread, spread) / 2), u_epoch_ns=u))
    return out


def test_steady_cluster_through_day_and_night_is_fixed():
    got = fixed_clusters(_arrivals(5_047_000))
    assert len(got) == 1
    assert abs(got[0]["fixed_delay_ns"] - 5_047_000) < 2_000
    assert got[0]["n_arrivals"] == 40
    assert got[0]["reporter_ids"] == ["K3LR/D"]


def test_night_only_cluster_is_not_fixed():
    assert fixed_clusters(_arrivals(5_047_000, day_h=(3, 10))) == []


def test_drifting_cluster_is_not_fixed():
    assert fixed_clusters(_arrivals(12_520_000, day_centre=12_475_000)) == []


def test_unanchored_rows_never_count():
    assert fixed_clusters(_arrivals(5_047_000, u=None)) == []
    assert fixed_clusters(_arrivals(5_047_000, u=5000.0)) == []


def test_wrapping_night_window_counts_late_hours():
    # night rows at 23-24 Z fall inside the wrapping (22.5, 11.5) window
    got = fixed_clusters(_arrivals(5_047_000, night_h=(23, 24)))
    assert len(got) == 1


def test_named_cluster_gets_an_offset():
    cl = fixed_clusters(_arrivals(1_000_000, tx_id="hfrnet:Rutgers"))
    tx = [dict(tx_id="hfrnet:Rutgers", lat=40.0, lon=-74.0, pos_sigma_m=10.0,
               valid_from=datetime(2026, 11, 9), version=1)]
    rx = [dict(reporter_id="K3LR/D", lat=40.0, lon=-75.0)]
    sol = ground_wave_solutions(cl, tx, rx, "gw-test")
    assert len(sol) == 1
    s = sol[0]
    assert s["tx_id"] == "hfrnet:Rutgers" and s["method"] == "ground_wave"
    assert abs(s["offset_ns"] - (1_000_000 - 284_085)) < 200
    assert len(s["inputs_digest"]) == 64 and s["solver_version"] == "gw-test"
    assert s["n_stations"] == 1 and s["n_arrivals"] == 40


def test_unnamed_cluster_has_no_offset():
    cl = fixed_clusters(_arrivals(1_000_000))
    sol = ground_wave_solutions(cl, [], [dict(reporter_id="K3LR/D", lat=40.0, lon=-75.0)], "v")
    assert sol[0]["offset_ns"] is None and sol[0]["tx_id"] == ""
    again = ground_wave_solutions(cl, [], [], "v")
    assert again[0]["inputs_digest"] == sol[0]["inputs_digest"]
