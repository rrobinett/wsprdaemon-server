"""sigmondb attribution: the study's method (tasks/t3-augment-study/attribute.py), ported.

No ClickHouse.  Every position here is synthetic or a typed-grid centre; no operator's home
position enters the repo.
"""
import json
import os
import random
import sys
from pathlib import Path

import pytest

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from sigmondb.attribution import (  # noqa: E402
    C_KM_S, DEFAULT, GW900, GW_TOL_MS, N_FAKE, P_NULL_MAX, Policy, analyse_group, attribute_group,
    candidates_for, cdiff, collapse_colocated, entries, fake_sites, flag_images, flag_rungs,
    flag_twins, gc_km, modes, parse_key, run_attribution, rung_spacing_ms, site_map, sky_km,
    tuple_fit)
from sigmondb.attribution import null_flag, null_upper_bound  # noqa: E402
from sigmondb.receivers import grid_to_latlon  # noqa: E402

FX = Path(__file__).parent / "fixtures" / "sigmondb"

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
    assert [m[0] for m in modes(90.0, "UACNJ", DEFAULT, gw_ok=True)] == ["G", "1F"]
    assert [m[0] for m in modes(150.0, "K3LR/D", DEFAULT, gw_ok=True)] == ["1F"]
    assert [m[0] for m in modes(600.0, "K3LR/D", GW900, gw_ok=True)] == ["G"]
    assert [m[0] for m in modes(90.0, "UACNJ", DEFAULT, gw_ok=False)] == ["1F"]   # typed grid


def test_modes_demands_gw_ok():
    # A caller that forgets gw_ok must fail, never grant ground wave to a typed grid.
    with pytest.raises(TypeError):
        modes(90.0, "UACNJ", DEFAULT)
    with pytest.raises(TypeError):
        modes(90.0, "UACNJ", DEFAULT, True, True)


def test_modes_reach_and_hops():
    assert [m[0] for m in modes(250.0, "KFS-OMNI", DEFAULT, gw_ok=True)] == ["G", "1F"]  # coastal
    assert [m[0] for m in modes(250.0, "K3LR/D", DEFAULT, tight=False, gw_ok=True)] == ["1F"]
    assert [m[0] for m in modes(3500.0, "K3LR/D", DEFAULT, gw_ok=True)] == ["1F", "2F"]
    assert [m[0] for m in modes(4000.0, "K3LR/D", DEFAULT, gw_ok=True)] == ["2F"]
    assert modes(950.0, "K3LR/D", GW900, gw_ok=True) == []


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


def _image_groups(site_of):
    """A 13.4750 MHz cluster at A and B (A hears more), and its image in the 13.5000 MHz group,
    where B hears more.  A and B share one coherence group."""
    rx = {"A": (37.0, -122.0, "gpsdo"), "B": (37.01, -122.0, "gpsdo")}
    coh = {"A": "s", "B": "s"}
    shift = -(13.4750e6 - 13.5000e6) / -99_260.0 * 1e3
    d2 = (100.0 + shift) % 500.0
    by_group = {"13.4750 -99.26 0.5s": {"A": _cluster_rows(100.0, 4, 200, True, 25.0),
                                        "B": _cluster_rows(100.0, 4, 100, True, 25.0)},
                "13.5000 -99.26 0.5s": {"A": _cluster_rows(d2, 4, 50, True, 15.0),
                                        "B": _cluster_rows(d2, 4, 80, True, 15.0)}}
    if site_of == "all":
        site_of = site_map(by_group, rx, coh)
    return [analyse_group(gk, rows, [], rx, None, coh, site_of) for gk, rows in by_group.items()]


def test_site_chosen_once_so_cross_group_image_is_flagged():
    g1, g2 = _image_groups("all")
    assert list(g1["stations"]) == list(g2["stations"]) == ["A"]     # 250 captures beat 180
    flag_images([g1, g2])
    assert [c.get("image", False) for c in g2["stations"]["A"]] == [True]
    assert any(v.startswith("image of 13.4750") for v in g2["set_aside"].values())
    assert g2["set_aside"]["B"] == "co-located with A; counted once"


def test_per_group_choice_would_miss_that_image():
    # The control: without one site map, each group keeps its own capture leader and the
    # image goes unflagged.  This pins why run_attribution must pass site_of.
    g1, g2 = _image_groups(None)
    assert list(g1["stations"]) == ["A"] and list(g2["stations"]) == ["B"]
    flag_images([g1, g2])
    assert not any(c.get("image") for c in g2["stations"]["B"])


def test_site_map_prefers_gpsdo_then_total_captures():
    rows = _cluster_rows(40.0, 10, 50)
    rx = {"RX-A": (37.0, -122.0, "grid"), "RX-B": (37.01, -122.0, "gpsdo"),
          "RX-C": (37.02, -122.0, "gpsdo")}
    by_group = {"g1": {"RX-A": rows, "RX-B": rows[:10], "RX-C": rows[:20]},
                "g2": {"RX-B": rows[:20], "RX-C": rows[:5]}}
    assert site_map(by_group, rx, None) == {"RX-A": "RX-B", "RX-B": "RX-B", "RX-C": "RX-B"}


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


# ---------------------------------------------------------------- the driver

def _arrivals(rows_by_group):
    """Arrival dicts from {group: {reporter: [(t, d_ms, level, live)]}}."""
    return [{"time": t, "reporter_id": st, "group_key": gk, "delay_ns": int(round(d * 1e6)),
             "level_db": lv, "live": live}
            for gk, by_st in rows_by_group.items() for st, rows in by_st.items()
            for t, d, lv, live in rows]


def test_run_attribution_chooses_sites_once_so_the_cross_group_image_is_flagged():
    # The shape of _image_groups, through the top-level driver.  Per-group site choice would
    # keep B in the 13.5000 group and miss the image silently.
    rx = {"A": (37.0, -122.0, "gpsdo"), "B": (37.01, -122.0, "gpsdo")}
    shift = -(13.4750e6 - 13.5000e6) / -99_260.0 * 1e3
    d2 = (100.0 + shift) % 500.0
    by_group = {"13.4750 -99.26 0.5s": {"A": _cluster_rows(100.0, 4, 200, True, 25.0),
                                        "B": _cluster_rows(100.0, 4, 100, True, 25.0)},
                "13.5000 -99.26 0.5s": {"A": _cluster_rows(d2, 4, 50, True, 15.0),
                                        "B": _cluster_rows(d2, 4, 80, True, 15.0)}}
    gs = {g["key"]: g for g in run_attribution(_arrivals(by_group), [], rx, DEFAULT, n_fake=0,
                                               coherence={"A": "s", "B": "s"})}
    g2 = gs["13.5000 -99.26 0.5s"]
    assert list(g2["stations"]) == ["A"]
    assert [c.get("image", False) for c in g2["stations"]["A"]] == [True]
    assert any(v.startswith("image of 13.4750") for v in g2["set_aside"].values())


def test_run_attribution_skips_groups_above_the_policy_centre():
    by_group = {"13.4750 -99.26 0.5s": {"A": _cluster_rows(100.0, 4, 20)},
                "4.4630 -25.73": {"A": _cluster_rows(100.0, 4, 20)}}
    rx = {"A": (37.0, -122.0, "gpsdo")}
    assert [g["key"] for g in run_attribution(_arrivals(by_group), [], rx, GW900, n_fake=0)] \
        == ["4.4630 -25.73"]
    gs = run_attribution(_arrivals(by_group), [], rx, DEFAULT, n_fake=0)
    assert sorted(g["key"] for g in gs) == ["13.4750 -99.26 0.5s", "4.4630 -25.73"]
    assert all(g["policy"] == "default" for g in gs)


def test_typed_grid_receiver_never_gets_ground_wave_in_a_fit():
    # Site 50 km from both receivers, tight clusters: G would apply at a GPSDO position only.
    site = (40.0, -74.0)
    rx = {"GPS": (40.0, -74.6, "gpsdo"), "GRID": (40.45, -74.0, "typed_grid")}
    cl = {st: [{"D": 100.0 + gc_km(site, rx[st][:2]) / C_KM_S * 1e3, "mad_us": 5.0, "n": 50,
                "level": 20.0, "flags": []}] for st in rx}
    ents = entries(site, cl, rx, 1000.0, DEFAULT)
    assert {e[2] for e in ents if e[0] == "GPS"} == {"G", "1F"}
    assert {e[2] for e in ents if e[0] == "GRID"} == {"1F"}
    fit = tuple_fit(site, (("GPS", 0), ("GRID", 0)), cl, rx, 1000.0, DEFAULT)
    assert fit[3][1] == "1F"
    # Under the ground-wave-only pass, the typed grid allows no mode at all.
    assert not [e for e in entries(site, cl, rx, 1000.0, GW900) if e[0] == "GRID"]
    assert tuple_fit(site, (("GPS", 0), ("GRID", 0)), cl, rx, 1000.0, GW900) is None


def test_fake_sites_keep_their_distance_and_repeat_for_a_seed():
    cands = [{"lat": 40.0, "lon": -74.0}, {"lat": 38.0, "lon": -75.0}]
    a = fake_sites(cands, 50, 7)
    assert a == fake_sites(cands, 50, 7) and len(a) == 50
    assert all(gc_km(p, (c["lat"], c["lon"])) >= 100.0 for p in a for c in cands)
    assert all(33.0 <= p[0] <= 45.0 and -80.0 <= p[1] <= -69.0 for p in a)


def test_one_station_gets_a_verdict_not_a_name():
    g = analyse_group("4.4630 -25.73", {"K3LR/D": _cluster_rows(40.0, 10, 50)}, SITES,
                      RX, None)
    attribute_group(g, RX, DEFAULT, 0, 1)
    assert g["named"] == [] and g["verdict"].startswith("only 1 T6 station")


# ---------------------------------------------------------------- the study's mutation cases

def _sites_4463():
    return [json.loads(l) for l in (FX / "attr_sites_4463.jsonl").read_text().splitlines() if l]


# Receivers at their typed-grid centres (sigmondb/seed/receivers.toml), labelled gpsdo here so
# ground wave applies.  The synthetic radar uses the same positions: self-consistent.
RXPOS = {st: (*grid_to_latlon(gr), "gpsdo") for st, gr in
         (("AC0G/ND", "EN16ov"), ("K3LR/D", "EN91se"), ("W2NAF", "FN21ei"), ("UACNJ", "FN20mv"))}

# attribution-result.md, "Group 4.4630 -25.73": (reporter, D ms, p10-90 us, captures, level dB).
# Each row also carries the table's level: with one shared level, flag_twins and flag_rungs
# would judge other pairs than the study did; with it, the background gets the study's flags.
BACKGROUND_4463 = [
    ("AC0G/ND", 534.644, 217, 20, 24.2), ("AC0G/ND", 535.797, 242, 25, 24.4),
    ("AC0G/ND", 542.204, 176, 37, 25.2), ("AC0G/ND", 542.686, 197, 14, 23.9),
    ("AC0G/ND", 545.300, 88, 24, 26.5), ("AC0G/ND", 10.366, 688, 52, 25.8),
    ("AC0G/ND", 17.613, 70, 21, 27.8), ("AC0G/ND", 21.549, 310, 33, 26.9),
    ("AC0G/ND", 22.250, 238, 36, 25.7), ("AC0G/ND", 26.340, 76, 14, 25.6),
    ("AC0G/ND", 27.080, 139, 15, 26.3), ("AC0G/ND", 30.773, 140, 22, 25.2),
    ("AC0G/ND", 39.429, 800, 43, 25.5), ("AC0G/ND", 51.892, 875, 34, 20.8),
    ("AC0G/ND", 56.303, 162, 18, 27.0), ("AC0G/ND", 56.986, 279, 16, 24.6),
    ("K3LR/D", 5.549, 442, 110, 41.5), ("K3LR/D", 9.173, 437, 35, 37.8),
    ("K3LR/D", 13.127, 732, 123, 36.5), ("K3LR/D", 16.706, 41, 64, 38.7),
    ("K3LR/D", 17.311, 413, 121, 43.5), ("K3LR/D", 21.460, 39, 77, 40.0),
    ("K3LR/D", 22.111, 408, 90, 41.6), ("K3LR/D", 25.161, 36, 69, 39.2),
    ("K3LR/D", 25.734, 413, 80, 41.1), ("K3LR/D", 29.495, 41, 63, 35.6),
    ("K3LR/D", 30.131, 387, 76, 40.0), ("K3LR/D", 33.886, 37, 83, 40.2),
    ("K3LR/D", 34.519, 439, 125, 43.0), ("K3LR/D", 42.556, 33, 65, 38.1),
    ("K3LR/D", 43.172, 413, 67, 39.2), ("K3LR/D", 46.852, 21, 71, 47.7),
    ("K3LR/D", 47.583, 807, 111, 42.3), ("K3LR/D", 51.317, 37, 73, 37.4),
    ("K3LR/D", 51.933, 437, 107, 41.1), ("K3LR/D", 61.316, 493, 44, 36.5),
    ("W2NAF", 522.230, 150, 25, 34.4), ("W2NAF", 526.398, 168, 30, 35.8),
    ("W2NAF", 3.753, 44, 56, 29.4), ("W2NAF", 4.413, 477, 95, 38.1),
    ("W2NAF", 12.572, 32, 43, 32.8), ("W2NAF", 13.085, 279, 63, 37.0),
    ("W2NAF", 15.496, 75, 29, 24.1), ("W2NAF", 16.207, 558, 88, 32.8),
    ("W2NAF", 24.846, 114, 36, 26.8), ("W2NAF", 32.963, 84, 30, 25.7),
    ("W2NAF", 33.667, 579, 86, 31.4), ("W2NAF", 41.898, 39, 51, 30.5),
    ("W2NAF", 42.508, 589, 78, 32.5), ("W2NAF", 46.336, 27, 56, 40.2),
    ("W2NAF", 46.849, 175, 50, 36.3), ("W2NAF", 50.939, 39, 52, 32.5),
    ("W2NAF", 51.560, 417, 84, 38.1), ("W2NAF", 60.144, 452, 74, 33.6),
]
CAPTURES_4463 = {"AC0G/ND": 88, "K3LR/D": 201, "W2NAF": 141, "UACNJ": 4}
GROUP = "4.4630 -25.73"
INJ = {}


def _background():
    """One row per capture per cluster, evenly spread over D +- p10-90/2, then one stray row per
    capture up to each station's total, at delays 100-500 ms apart from every cluster, where no
    cluster forms.

    Evenly, not randomly: a random draw can leave a gap over GAP_MS inside a sparse wide cluster
    and split it.  The choice does not drive the results: five random backgrounds gave the same
    statuses, with the two-station ASSA p at 0.076-0.085."""
    rows = {}
    for si, st in enumerate(sorted(CAPTURES_4463)):
        t0 = 1_761_000_000 + si
        mine = [b for b in BACKGROUND_4463 if b[0] == st]
        out = []
        for _, D, w_us, caps, lv in mine:
            w = w_us / 1e3
            out += [(t0 + 600 * i, (D - w / 2 + w * (i + 0.5) / caps) % 1000.0, lv, False)
                    for i in range(caps)]
        used = max((b[3] for b in mine), default=0)
        pad = CAPTURES_4463[st] - used
        out += [(t0 + 600 * (used + i), 100.0 + 400.0 * (i + 0.5) / pad, 10.0, False)
                for i in range(pad)]
        rows[st] = out
    return rows


def _run(site, mode, h_km, stations, policy):
    """Background plus a synthetic radar, as tasks/t3-augment-study/attribute_mutation.py."""
    sites = _sites_4463()
    s = next(x for x in sites if x["id"] == site)
    rows = _background()
    rnd = random.Random(1)
    INJ.clear()
    O = 300.0
    for i, st in enumerate(stations):
        d = gc_km((s["lat"], s["lon"]), RXPOS[st][:2])
        p = d / C_KM_S * 1e3 if mode == "G" else sky_km(d, h_km, 1) / C_KM_S * 1e3
        INJ[st] = (O + p) % 1000.0
        for k in range(80):
            dd = O + p + rnd.gauss(0, 0.01 if mode == "G" else 0.08)
            rows[st].append((1_762_500_000 + 600 * k + 7 * i, int(dd * 1e6) / 1e6, 30.0, False))
    return run_attribution(_arrivals({GROUP: rows}), sites, RXPOS, policy)


def _holding(groups, injected):
    """Results whose tuple uses an injected cluster.  Members at a station that got no
    synthetic radar (ND in the ASSA cases) are skipped, not looked up: injected has no D there."""
    g = next(g for g in groups if g["key"] == GROUP)
    return [nm for nm in g["named"]
            if any(abs(cdiff(g["stations"][m[0]][m[1]]["D"], injected[m[0]], g["P"])) < 0.2
                   for m in nm["mem"] if m[0] in injected)]


def test_background_reproduces_the_study_clusters():
    g = analyse_group(GROUP, _background(), _sites_4463(), RXPOS, None)
    assert len(g["cands"]) == 23
    assert {st: len(cl) for st, cl in g["stations"].items()} == \
        {"AC0G/ND": 16, "K3LR/D": 20, "W2NAF": 18, "UACNJ": 0}
    flagged = {(st, round(c["D"], 3)) for st, cl in g["stations"].items() for c in cl if c["flags"]}
    assert flagged == {("K3LR/D", 61.316), ("W2NAF", 32.963)}


def test_mutation_assa_ground_wave_read_as_skywave_is_never_named():
    hits = _holding(_run("hfrnet:ASSA", "G", 0, ["K3LR/D", "W2NAF"], DEFAULT), INJ)
    assert hits and all(nm["status"] == "ambiguous" for nm in hits)


# The two ground-wave ASSA cases below use repo-safe inputs: receivers at their typed-grid
# centres and the CRC-32 seed.  The study used GPSDO positions and seed 1009; with those, this
# port reproduces it (2 stations named at p 0.049, 3 stations p 0.000; task-4-report.md,
# "Reproduction with the study's inputs").  The grid inputs move the 2-station p over the
# threshold, so these tests state what the method gives for those inputs.

def test_mutation_assa_two_ground_wave_stations_grid_positions_stays_ambiguous():
    # Measured with grid positions: p 0.076 at the CRC-32 seed (17 of 223 eligible fakes);
    # 0.054-0.122 over seeds 1000-1019.  No rival fits (CEDR's best is 157 us, outside
    # tolerance), so only the fake-site guard keeps this from a name.  This is the test that
    # fails when that guard is loosened (pn > 0.5).
    hits = _holding(_run("hfrnet:ASSA", "G", 0, ["K3LR/D", "W2NAF"], GW900), INJ)
    nm = next(nm for nm in hits if nm["site"]["id"] == "hfrnet:ASSA")
    assert nm["status"] == "ambiguous"
    assert abs(cdiff(nm["O_ground"], 300.0, 1000.0)) <= GW_TOL_MS
    assert nm["runner"][0]                                  # the best rival fails tolerance
    assert P_NULL_MAX < nm["p_null"] <= 0.13


def test_mutation_assa_three_ground_wave_stations_grid_positions_is_named():
    # Measured with grid positions: p 0.007 at the CRC-32 seed, 0.000-0.004 over seeds 1000-1019.
    hits = _holding(_run("hfrnet:ASSA", "G", 0, ["K3LR/D", "W2NAF", "UACNJ"], GW900), INJ)
    nm = next(nm for nm in hits if nm["site"]["id"] == "hfrnet:ASSA")
    assert nm["status"] == "named" and nm["n"] == 3
    assert abs(cdiff(nm["O_ground"], 300.0, 1000.0)) <= GW_TOL_MS
    assert nm["p_null"] <= P_NULL_MAX


def test_mutation_nant_skywave_is_found_but_stays_ambiguous():
    hits = _holding(_run("hfrnet:NANT", "1F", 320.0, ["K3LR/D", "W2NAF", "AC0G/ND"], DEFAULT), INJ)
    nm = next(nm for nm in hits if nm["site"]["id"] in ("hfrnet:NANT", "hfrnet:NAUS"))
    assert nm["status"] == "ambiguous"
    assert abs(cdiff(nm["O"], 300.0, 1000.0)) <= 0.15            # study: 79 us off


# ---------------------------------------------------------------- rows

from datetime import datetime  # noqa: E402

from sigmondb.attribution import ATTRIB_VERSION, attribution_rows, inputs_digest  # noqa: E402

RUN_AT = datetime(2026, 11, 13, 12, 0, 0)
W0, W1 = datetime(2026, 10, 14, 12, 0, 0), datetime(2026, 11, 13, 11, 50, 0)


def _kfs_only_rows():
    """Today's KFS shape: one station, a cluster and its 1.632 ms twin, live."""
    rows = _cluster_rows(499.444, 5, 100, live=True) + _cluster_rows(501.076, 5, 100, live=True)
    return _arrivals({"4.7850 -25.73": {"KFS-OMNI": rows}})


def test_verdict_groups_become_unattributable_rows():   # one-station group, today's KFS shape
    gs = run_attribution(_kfs_only_rows(), SITES, RX, DEFAULT, n_fake=20)
    rows = attribution_rows(gs, DEFAULT, RUN_AT, W0, W1, "0" * 64)
    assert [r["status"] for r in rows] == ["unattributable"]
    assert rows[0]["flags"][0].startswith("only 1 T6 station")
    r = rows[0]
    assert r["tx_id"] == "" and r["run_id"] == "attr:%s:%s" % (ATTRIB_VERSION, "0" * 12)
    assert r["mode_policy"] == "default" and r["period_ns"] == 1_000_000_000
    assert r["offset_ns"] is None and r["p_null"] is None and r["n_fake"] == 0
    assert any(v.startswith("twin of") for v in r["set_aside"].values())
    assert (r["run_at"], r["window_from"], r["window_to"]) == (RUN_AT, W0, W1)


def test_inputs_digest_is_sha256_of_sorted_keys():
    import hashlib
    t = datetime(2026, 11, 7, 3, 0, 0, 250000)
    a = [{"reporter_id": "B", "group_key": "g", "time": t, "delay_ns": 5},
         {"reporter_id": "A", "group_key": "g", "time": t, "delay_ns": -2}]
    want = hashlib.sha256(("A|g|%s|-2\nB|g|%s|5" % (t.isoformat(), t.isoformat())).encode()).hexdigest()
    assert inputs_digest(a) == want == inputs_digest(list(reversed(a)))


def _fit(status, n, o_ground, pn=0.01, runner=None):
    site = {"id": "hfrnet:TSTA", "lat": 39.5, "lon": -74.0}
    mem = [("K3LR/D", 0, "G", 300.0, GW_TOL_MS, 500.0), ("W2NAF", 0, "G", 300.01, GW_TOL_MS, 400.0)]
    if n == 3:
        mem.append(("UACNJ", 0, "G", 299.99, GW_TOL_MS, 90.0))
    return {"site": site, "alias": ["hfrnet:TSTZ"], "n": n, "spread": 20.0, "O": 300.004,
            "O_ground": o_ground, "mem": mem, "runner": runner, "p_null": pn, "flags": [],
            "status": status}


def _group(named, policy):
    cl = {"D": 302.0, "mad_us": 8.0, "n": 50, "level": 30.0, "flags": []}
    return {"key": "4.4630 -25.73", "P": 1000.0, "policy": policy.name, "verdict": None,
            "n_fake": 223, "set_aside": {"N8UR": "no receiver position"},
            "stations": {"K3LR/D": [cl], "W2NAF": [cl], "UACNJ": [cl]}, "named": named}


def test_gw900_rows_never_carry_the_word_named():
    # Michael, 11-13: the ground-wave-only 900 km pass records its fits and never attaches them.
    named = [_fit("named", 3, 300.0), _fit("named_two_stations", 2, 300.0),
             _fit("ambiguous", 2, 300.0)]
    gw = attribution_rows([_group(named, GW900)], GW900, RUN_AT, W0, W1, "a" * 64)
    assert [r["status"] for r in gw] == ["fits_gw900", "fits_gw900_two_stations", "ambiguous"]
    assert not any("named" in r["status"] for r in gw)
    df = attribution_rows([_group(named, DEFAULT)], DEFAULT, RUN_AT, W0, W1, "a" * 64)
    assert [r["status"] for r in df] == ["named", "named_two_stations", "ambiguous"]


def test_rows_refuse_a_policy_other_than_the_groups():
    with pytest.raises(ValueError):
        attribution_rows([_group([_fit("named", 3, 300.0)], GW900)], DEFAULT, RUN_AT, W0, W1,
                         "a" * 64)


def test_named_row_fields():
    runner = (True, 157.0, {"id": "hfrnet:CEDR"}, None)
    r = attribution_rows([_group([_fit("named", 3, 300.0021, pn=float("nan"), runner=runner)],
                                 DEFAULT)], DEFAULT, RUN_AT, W0, W1, "b" * 64)[0]
    assert r["tx_id"] == "hfrnet:TSTA" and r["alias_tx_ids"] == ["hfrnet:TSTZ"]
    assert r["n_stations"] == 3 and r["reporter_ids"] == ["K3LR/D", "W2NAF", "UACNJ"]
    assert r["modes"] == ["G", "G", "G"]
    assert r["cluster_delays_ns"] == [302_000_000] * 3 and r["cluster_mad_ns"] == [8000.0] * 3
    assert (r["offset_ns"], r["offset_from"]) == (300_002_100, "ground_wave")
    assert r["spread_ns"] == 20_000.0 and r["p_null"] is None and r["n_fake"] == 223
    assert (r["runner_up_tx_id"], r["runner_up_spread_ns"], r["runner_up_fits"]) == \
        ("hfrnet:CEDR", 157_000.0, 0)
    assert r["set_aside"] == {"N8UR": "no receiver position"}
    assert r["solver_version"] == ATTRIB_VERSION and r["inputs_digest"] == "b" * 64
    sky = attribution_rows([_group([_fit("ambiguous", 2, None)], DEFAULT)], DEFAULT, RUN_AT,
                           W0, W1, "b" * 64)[0]
    assert (sky["offset_ns"], sky["offset_from"]) == (300_004_000, "skywave_model")
    assert sky["runner_up_tx_id"] == "" and sky["runner_up_spread_ns"] is None


def test_group_with_no_fit_gets_an_unattributable_row():
    g = _group([], DEFAULT)
    rows = attribution_rows([g], DEFAULT, RUN_AT, W0, W1, "c" * 64)
    assert [(r["status"], r["tx_id"]) for r in rows] == [("unattributable", "")]
    assert rows[0]["flags"] and rows[0]["n_stations"] == 3


def test_a_candidate_without_period_stays_a_candidate():
    sites = SITES + [dict(SITES[1], id="hfrnet:NOPER", lat=36.0, lon=-121.0, period=None)]
    g = analyse_group("4.7850 -25.73", {"KFS-OMNI": _cluster_rows(40.0, 10, 50)}, sites, RX, None)
    assert [s["id"] for s in g["cands"]] == ["hfrnet:TSTB", "hfrnet:NOPER"]
    assert g["P"] == 1000.0
    only = [dict(SITES[1], period=None)]
    g = analyse_group("4.7850 -25.73", {"KFS-OMNI": _cluster_rows(40.0, 10, 50)}, only, RX, 2.0)
    assert g["P"] == 500.0                              # no candidate period: engineering's


# --- apply_attribution: names reach the ground-wave solver's fixed clusters -----------------
from sigmondb.attribution import NAMED_STATUSES, apply_attribution  # noqa: E402
from sigmondb.groundwave import C_M_PER_S, fixed_clusters, ground_wave_solutions, great_circle_m  # noqa: E402

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from test_sigmondb_groundwave import _arrivals as _gw_base  # noqa: E402
from datetime import datetime as _dt, timedelta as _td, timezone as _tz  # noqa: E402

# UACNJ at its typed-grid centre (FN20mv, from the seed), labelled gpsdo so ground wave applies.
# hfrnet:HOOK here is a synthetic site about 95 km east of it, not the real radar's position.
_UACNJ = grid_to_latlon("FN20mv")
_HOOK = (_UACNJ[0], _UACNJ[1] + 1.13)
RX_ROWS = [dict(reporter_id="UACNJ", lat=_UACNJ[0], lon=_UACNJ[1], pos_source="gpsdo",
                pos_sigma_m=10.0, version=1)]
TX_ROWS = [dict(tx_id="hfrnet:HOOK", lat=_HOOK[0], lon=_HOOK[1], pos_sigma_m=100.0, version=1)]
HOOK_D_NS = round(great_circle_m(*_UACNJ, *_HOOK) / C_M_PER_S * 1e9) + 7_000   # offset 7 us


def _gw_arrivals(rid="UACNJ", centre_ns=HOOK_D_NS, seed=1):
    """Fixed-delay arrivals at UACNJ, 11-07: 03-09 Z is night there and 15-20 Z day (sun
    below -12 and above +12 degrees at FN20mv)."""
    return _gw_base(centre_ns, seed=seed, reporter_id=rid)


def _attr(tx, status, rid, d_ns, run_at=None, p_null=0.01):
    """One attribution row; run_at defaults to now (naive UTC), so it passes the age rule."""
    return dict(tx_id=tx, status=status, mode_policy="default", group_key="4.4630 -25.73",
                period_ns=1_000_000_000, alias_tx_ids=[], reporter_ids=[rid],
                cluster_delays_ns=[d_ns], p_null=p_null,
                run_at=run_at or _dt.now(_tz.utc).replace(tzinfo=None))


def test_named_statuses_are_exactly_the_default_policys_names():
    assert NAMED_STATUSES == ("named", "named_two_stations")


def test_a_named_cluster_reaches_the_solution_with_offset():
    arr = _gw_arrivals(rid="UACNJ", centre_ns=HOOK_D_NS)    # fixed day and night, 95 km from HOOK
    cl = fixed_clusters(arr, RX_ROWS)
    reasons = apply_attribution(cl, [_attr("hfrnet:HOOK", "named", "UACNJ", HOOK_D_NS)])
    sol = ground_wave_solutions(cl, TX_ROWS, RX_ROWS, "groundwave-3")
    assert reasons == {} and sol[0]["tx_id"] == "hfrnet:HOOK" and sol[0]["offset_ns"] is not None
    assert abs(sol[0]["offset_ns"] - 7_000) < 2_000


def test_ambiguous_and_gw900_rows_never_name():
    for a in (_attr("hfrnet:HOOK", "ambiguous", "UACNJ", HOOK_D_NS),
              _attr("hfrnet:HOOK", "unattributable", "UACNJ", HOOK_D_NS),
              dict(_attr("hfrnet:HOOK", "fits_gw900", "UACNJ", HOOK_D_NS),
                   mode_policy="gw_only_900km"),
              # Even a GW900 row that said "named" must not attach: the policy decides.
              dict(_attr("hfrnet:HOOK", "named", "UACNJ", HOOK_D_NS),
                   mode_policy="gw_only_900km")):
        cl = fixed_clusters(_gw_arrivals(rid="UACNJ", centre_ns=HOOK_D_NS), RX_ROWS)
        apply_attribution(cl, [a])
        assert cl[0]["tx_ids"] == [""], a
        sol = ground_wave_solutions(cl, TX_ROWS, RX_ROWS, "groundwave-3")
        assert sol[0]["tx_id"] == "" and sol[0]["offset_ns"] is None


def test_a_name_must_match_reporter_group_and_delay():
    for a in (_attr("hfrnet:HOOK", "named", "K3LR/D", HOOK_D_NS),
              dict(_attr("hfrnet:HOOK", "named", "UACNJ", HOOK_D_NS), group_key="4.7850 -25.73"),
              _attr("hfrnet:HOOK", "named", "UACNJ", HOOK_D_NS + 60_000),
              _attr("hfrnet:HOOK", "named", "UACNJ", HOOK_D_NS + 1_500_000_000)):
        cl = fixed_clusters(_gw_arrivals(), RX_ROWS)
        assert apply_attribution(cl, [a]) == {}
        assert cl[0]["tx_ids"] == [""], a
    # Modulo the period: a delay whole periods away, or wrapped just below one period, names
    # the same cluster.
    for d in (HOOK_D_NS + 2_000_000_000, HOOK_D_NS - 1_000_000_000 + 20_000):
        cl = fixed_clusters(_gw_arrivals(), RX_ROWS)
        apply_attribution(cl, [_attr("hfrnet:HOOK", "named", "UACNJ", d)])
        assert cl[0]["tx_ids"] == ["hfrnet:HOOK"], d


def test_aliased_or_doubly_named_cluster_stays_unnamed():
    cl = fixed_clusters(_gw_arrivals(rid="UACNJ", centre_ns=HOOK_D_NS), RX_ROWS)
    r = apply_attribution(cl, [dict(_attr("hfrnet:HOOK", "named", "UACNJ", HOOK_D_NS),
                                    alias_tx_ids=["hfrnet:HEMP"])])
    assert cl[0]["tx_ids"] == [""] and "co-sited with hfrnet:HEMP" in r[cl[0]["cluster_key"]]
    cl = fixed_clusters(_gw_arrivals(rid="UACNJ", centre_ns=HOOK_D_NS), RX_ROWS)
    r = apply_attribution(cl, [_attr("hfrnet:HOOK", "named", "UACNJ", HOOK_D_NS),
                               _attr("hfrnet:LOVE", "named", "UACNJ", HOOK_D_NS + 10_000)])
    assert cl[0]["tx_ids"] == [""] and r[cl[0]["cluster_key"]].startswith("two names")


def test_a_twin_cluster_is_never_named():
    # A named row lists the cluster at D; its 1.632 ms twin sits outside match_tol_ns.
    arr = _gw_arrivals(rid="UACNJ", centre_ns=HOOK_D_NS) + \
        _gw_arrivals(rid="UACNJ", centre_ns=HOOK_D_NS + 1_632_000, seed=2)
    cl = fixed_clusters(arr, RX_ROWS)
    apply_attribution(cl, [_attr("hfrnet:HOOK", "named", "UACNJ", HOOK_D_NS)])
    assert sorted(c["tx_ids"][0] for c in cl) == ["", "hfrnet:HOOK"]


def test_a_cluster_the_station_named_differently_keeps_its_name():
    cl = fixed_clusters(_gw_base(HOOK_D_NS, reporter_id="UACNJ", tx_id="hfrnet:OTHER"), RX_ROWS)
    r = apply_attribution(cl, [_attr("hfrnet:HOOK", "named", "UACNJ", HOOK_D_NS)])
    assert cl[0]["tx_ids"] == ["hfrnet:OTHER"] and "station names hfrnet:OTHER" in r[cl[0]["cluster_key"]]


def test_a_name_older_than_the_age_limit_attaches_nothing():
    from sigmondb.attribution import ATTRIB_MAX_AGE_H
    assert ATTRIB_MAX_AGE_H == 3
    now = _dt(2026, 11, 13, 12, 0)
    cl = fixed_clusters(_gw_arrivals(), RX_ROWS)
    apply_attribution(cl, [_attr("hfrnet:HOOK", "named", "UACNJ", HOOK_D_NS,
                                 run_at=now - _td(hours=4))], now=now)
    assert cl[0]["tx_ids"] == [""]
    cl = fixed_clusters(_gw_arrivals(), RX_ROWS)
    apply_attribution(cl, [_attr("hfrnet:HOOK", "named", "UACNJ", HOOK_D_NS,
                                 run_at=now - _td(hours=2))], now=now)
    assert cl[0]["tx_ids"] == ["hfrnet:HOOK"]
    # A tz-aware run_at (as a database read may give) is compared in UTC, never relabelled.
    cl = fixed_clusters(_gw_arrivals(), RX_ROWS)
    cdt = _tz(_td(hours=-5))
    apply_attribution(cl, [_attr("hfrnet:HOOK", "named", "UACNJ", HOOK_D_NS,
                                 run_at=_dt(2026, 11, 13, 5, 0, tzinfo=cdt))], now=now)
    assert cl[0]["tx_ids"] == ["hfrnet:HOOK"]                    # 10:00Z: 2 h old
    # A row with no run_at cannot show its age: it never names.
    cl = fixed_clusters(_gw_arrivals(), RX_ROWS)
    apply_attribution(cl, [dict(_attr("hfrnet:HOOK", "named", "UACNJ", HOOK_D_NS),
                                run_at=None)], now=now)
    assert cl[0]["tx_ids"] == [""]


def test_a_fit_without_a_passing_null_test_never_names():
    for pn in (None, float("nan"), 0.06):
        cl = fixed_clusters(_gw_arrivals(), RX_ROWS)
        apply_attribution(cl, [_attr("hfrnet:HOOK", "named_two_stations", "UACNJ", HOOK_D_NS,
                                     p_null=pn)])
        assert cl[0]["tx_ids"] == [""], pn
    cl = fixed_clusters(_gw_arrivals(), RX_ROWS)
    apply_attribution(cl, [_attr("hfrnet:HOOK", "named_two_stations", "UACNJ", HOOK_D_NS,
                                 p_null=0.05)])
    assert cl[0]["tx_ids"] == ["hfrnet:HOOK"]


# ---------------------------------------------------------------- ruling A10: the two-station bound

def test_null_upper_bound_matches_the_closed_form_and_brackets_the_limit():
    assert null_upper_bound(0, 400) == pytest.approx(1 - 0.05 ** (1 / 400))      # about 0.0075
    assert 0.025 < null_upper_bound(10, 400) < P_NULL_MAX
    assert null_upper_bound(14, 400) > P_NULL_MAX            # point 0.035 passes, bound fails
    assert null_upper_bound(400, 400) == 1.0


def test_two_station_fit_needs_the_upper_bound_under_the_limit():
    f = null_flag(2, 14, 400)
    assert f == "two-station null bound 5.4% > 5 %"         # ambiguous through the flags path
    assert null_flag(3, 14, 400) is None                    # three stations keep the point rule
    assert null_flag(2, 0, 400) is None                     # stays named_two_stations
    assert null_flag(2, 10, 400) is None
    assert null_flag(2, 21, 400).startswith("fake sites match as well")   # point estimate still fails
    assert null_flag(2, 0, 0) is None                       # no fakes: no test, as before
