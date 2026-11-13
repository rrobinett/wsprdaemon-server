"""sigmondb attribution: the study's method (tasks/t3-augment-study/attribute.py), ported.

No ClickHouse.  Every position here is synthetic or a typed-grid centre; no operator's home
position enters the repo.
"""
import os
import random
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from sigmondb.attribution import (  # noqa: E402
    DEFAULT, GW900, analyse_group, collapse_colocated, flag_rungs, flag_twins, gc_km, modes,
    parse_key, rung_spacing_ms, sky_km)

# Hand-made sites: one per sweep group the tests use.  Positions are synthetic.
SITES = [
    {"id": "hfrnet:TSTA", "lat": 39.5, "lon": -74.0, "freq": 4.463e6, "rate": -25_733.9,
     "period": 1.0},
    {"id": "hfrnet:TSTB", "lat": 37.0, "lon": -123.0, "freq": 4.785e6, "rate": -25_733.9,
     "period": 1.0},
]
RX = {"KFS-OMNI": (37.5, -122.5, "gpsdo"),          # synthetic, not anyone's home
      "K3LR/D": (41.1875, -80.458333, "grid")}       # EN91se grid centre


def _cluster_rows(D_ms, width_us, n_captures, live=False, level=20.0, t0=0, seed=1):
    """One row per capture, delays uniform over D +- width/2, capture times t0 + 600*i."""
    rnd = random.Random(seed)
    w = width_us / 1e3
    return [(t0 + 600 * i, D_ms + rnd.uniform(-w / 2, w / 2), level, live)
            for i in range(n_captures)]


def test_sky_and_ground_delays_match_the_prototype():
    assert abs(gc_km((41.0, -75.0), (41.0, -76.0)) - 83.9) < 0.1
    assert abs(sky_km(564.0, 300.0, 1) - 832.48) < 0.01        # K3LR-ASSA 1F at h = 300 km
    assert [m[0] for m in modes(90.0, "UACNJ", DEFAULT)] == ["G", "1F"]
    assert [m[0] for m in modes(150.0, "K3LR/D", DEFAULT)] == ["1F"]
    assert [m[0] for m in modes(600.0, "K3LR/D", GW900)] == ["G"]
    assert [m[0] for m in modes(90.0, "UACNJ", DEFAULT, gw_ok=False)] == ["1F"]   # typed grid


def test_modes_reach_and_hops():
    assert [m[0] for m in modes(250.0, "KFS-OMNI", DEFAULT)] == ["G", "1F"]       # coastal 300 km
    assert [m[0] for m in modes(250.0, "K3LR/D", DEFAULT, tight=False)] == ["1F"]
    assert [m[0] for m in modes(3500.0, "K3LR/D", DEFAULT)] == ["1F", "2F"]
    assert [m[0] for m in modes(4000.0, "K3LR/D", DEFAULT)] == ["2F"]
    assert modes(950.0, "K3LR/D", GW900) == []


def test_parse_key():
    assert parse_key("4.4630 -25.73") == (4.463e6, -25730.0, None)
    assert parse_key("13.4750 -99.26 0.5s") == (13.475e6, -99260.0, 0.5)


def test_kfs_twin_is_set_aside():
    # 100 captures; cluster at 499.444 ms and a twin 1.632 ms later, same rows and level.
    g = analyse_group("4.7850 -25.73", {"KFS-OMNI": _cluster_rows(499.444, 5, 100, live=True)
                                        + _cluster_rows(501.076, 5, 100, live=True)}, SITES, RX, None)
    cl = g["stations"]["KFS-OMNI"]
    assert [c.get("twin", False) for c in sorted(cl, key=lambda c: c["D"])] == [False, True]
    assert any(v.startswith("twin of") for v in g["set_aside"].values())


def test_live_rung_is_set_aside():
    R = rung_spacing_ms(4.785e6, -25_733.9)                      # 19.98 ms
    rows = _cluster_rows(517.121, 20, 200, live=True, level=21.0) + \
        _cluster_rows(517.121 + R, 20, 120, live=True, level=19.0)
    g = analyse_group("4.7850 -25.73", {"KFS-OMNI": rows}, SITES, RX, None)
    weak = min(g["stations"]["KFS-OMNI"], key=lambda c: c["n"])
    assert weak.get("rung") and weak["flags"][0].startswith("rung +1")


def test_colocated_reporters_count_once():
    rows = _cluster_rows(40.0, 10, 50)
    here = (37.0, -122.0, "gpsdo")                       # synthetic, not anyone's home
    g = analyse_group("4.4630 -25.73", {"RX-A": rows, "RX-B": rows[:30]},
                      SITES, {"RX-A": here, "RX-B": (37.01, -122.0, "gpsdo")}, None)
    assert list(g["stations"]) == ["RX-A"]
    assert "co-located with RX-A" in g["set_aside"]["RX-B"]


def test_coherence_group_counts_once_even_far_apart():
    rows = _cluster_rows(40.0, 10, 50)
    rx = {"RX-A": (37.0, -122.0, "gpsdo"), "RX-B": (45.0, -100.0, "gpsdo"),
          "RX-C": (30.0, -90.0, "gpsdo")}
    kept, aside = collapse_colocated({"RX-A": rows[:20], "RX-B": rows, "RX-C": rows}, rx,
                                     {"RX-A": "site1", "RX-B": "site1", "RX-C": ""})
    assert sorted(kept) == ["RX-B", "RX-C"]
    assert aside == {"RX-A": "co-located with RX-B; counted once"}


def test_reporter_without_position_is_set_aside():
    g = analyse_group("4.4630 -25.73", {"N8UR": _cluster_rows(40.0, 10, 50)}, SITES, RX, None)
    assert g["stations"] == {}
    assert g["set_aside"]["N8UR"] == "no receiver position"


def test_period_from_key_candidates_then_engineering():
    rows = {"KFS-OMNI": _cluster_rows(100.0, 10, 20)}
    assert analyse_group("4.7850 -25.73", rows, SITES, RX, None)["P"] == 1000.0
    assert analyse_group("4.7850 -25.73 0.5s", rows, SITES, RX, None)["P"] == 500.0
    assert analyse_group("9.0000 -25.73", rows, SITES, RX, 2.0)["P"] == 500.0
    assert analyse_group("9.0000 -25.73", rows, SITES, RX, None)["P"] == 1000.0


# The study's own case: KFS-MAIN, group 4.7850 -25.73, 30 days to 11-13 (attribution-result.md).
# (D ms, MAD us, rows, level dB, the flags the study printed).
STUDY_KFS = [
    (479.470, 5, 266, 22.0, ["rung -1 of 499.444"]),
    (481.102, 5, 266, 22.0, ["twin of 479.470 (+1.632 ms, same rows and level)",
                             "rung -1 of 501.076"]),
    (497.148, 58, 718, 19.1, ["rung -1 of 517.121"]),
    (498.780, 58, 747, 19.1, ["rung -1 of 518.753"]),
    (499.444, 5, 395, 23.2, []),
    (499.988, 58, 381, 21.6, []),
    (501.076, 5, 395, 23.3, ["twin of 499.444 (+1.632 ms, same rows and level)"]),
    (501.620, 58, 337, 21.8, []),
    (516.503, 5, 290, 17.5, []),
    (517.121, 58, 1035, 21.0, []),
    (518.135, 5, 288, 17.5, ["twin of 516.503 (+1.632 ms, same rows and level)"]),
    (518.753, 63, 1437, 21.0, []),
    (520.039, 128, 253, 18.0, ["rung +1 of 499.988"]),
    (521.049, 5, 271, 22.0, ["rung +1 of 501.076"]),
    (537.095, 58, 746, 19.1, ["rung +1 of 517.121"]),
    (538.727, 58, 759, 19.2, ["twin of 537.095 (+1.632 ms, same rows and level)",
                              "rung +1 of 518.753"]),
]


def test_study_kfs_clusters_get_the_study_flags():
    cl = [{"D": D, "mad_us": mad, "n": n, "level": lv, "flags": []}
          for D, mad, n, lv, _ in STUDY_KFS]
    _, rate, _ = parse_key("4.7850 -25.73")
    flag_twins(cl, 1000.0)
    flag_rungs(cl, rung_spacing_ms(4.785e6, rate), 1000.0, live=True)
    assert [c["flags"] for c in cl] == [f for *_, f in STUDY_KFS]
    assert sum(1 for c in cl if c.get("rung")) == 8
    assert sum(1 for c in cl if c.get("twin")) == 4
