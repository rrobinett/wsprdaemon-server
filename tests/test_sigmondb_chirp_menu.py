"""The channel menu: per station and UTC hour, a timing ranking and an exploration ranking,
kept apart.  Made-up sites at made-up positions; every station sits at a grid6 centre."""
import dataclasses
import math
import os
import sys
from datetime import datetime, timedelta

import pytest

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from sigmondb import chirp, chirp_menu, cli, ddl  # noqa: E402
from sigmondb.publish import latlon_to_grid6  # noqa: E402
from sigmondb.receivers import grid_to_latlon  # noqa: E402

RULES = chirp.load_rules()
NOW = datetime(2026, 11, 27, 12, 0, 0)
BASE = grid_to_latlon("EM55")          # a made-up place: the centre of a 4-character square
RATE = -25_730.0


def dest(p, bearing_deg, km):
    """The point km from p along bearing_deg (spherical earth)."""
    R = 6371.0
    la, lo, b, d = math.radians(p[0]), math.radians(p[1]), math.radians(bearing_deg), km / R
    la2 = math.asin(math.sin(la) * math.cos(d) + math.cos(la) * math.sin(d) * math.cos(b))
    lo2 = lo + math.atan2(math.sin(b) * math.sin(d) * math.cos(la),
                          math.cos(d) - math.sin(la) * math.sin(la2))
    return (math.degrees(la2), math.degrees(lo2))


def g6(p):
    """p moved to its grid6 centre, as every station position is."""
    return grid_to_latlon(latlon_to_grid6(*p))


def station(p, rid="ME", is_t6=False):
    lat, lon = g6(p)
    return dict(reporter_id=rid, lat=lat, lon=lon, is_t6=is_t6)


def crow(cid, state, pos, centre=4_537_000.0, tx_id=None, **kw):
    r = {c: None for c in ddl.COLUMNS["chirp"]}
    r.update(chirp_id=cid, group_key=chirp.group_key(centre, RATE, 1.0), kind="codar",
             state=state, centre_hz=centre, rate_hz_per_s=RATE, period_s=1.0,
             lat=pos[0] if pos else None, lon=pos[1] if pos else None,
             heard_hours=0, tx_id=cid if tx_id is None else tx_id, reporter_ids=[])
    r.update(kw)
    return r


def sweep(rid, centre, hour, snr):
    return dict(reporter_id=rid, centre_hz=centre, rate_hz_per_s=RATE, period_s=1.0,
                span_hz=25_000.0, duration_s=None, clipped=0, hour=hour, n=3, snr_med=snr,
                n_dwells=2)


def at_hour(h, days_ago=1):
    return (NOW - timedelta(days=days_ago)).replace(hour=h)


def menu(st, hour, chirps, sweeps=(), positions=None, t6=(), captures=(), rules=RULES):
    hearing = chirp_menu.prepare_hearing(chirps, list(sweeps), rules, NOW,
                                         positions=positions or {}, captures=captures)
    return chirp_menu.channel_menu(st, hour, chirps, hearing, set(t6), rules)


def by_key(rows):
    return {r["channel_key"]: r for r in rows}


def timing_order(rows):
    return [r["channel_key"] for r in sorted((r for r in rows if r["rank_timing"]),
                                             key=lambda r: r["rank_timing"])]


def explore_order(rows):
    return [r["channel_key"] for r in sorted((r for r in rows if r["rank_explore"]),
                                             key=lambda r: r["rank_explore"])]


def with_menu(**kw):
    return dataclasses.replace(RULES, menu=dataclasses.replace(RULES.menu, **kw))


# --- two established channels, A stronger, B weaker, both near the station --------------------

FA, FB = 4_537_000.0, 4_800_000.0
NEAR = dest(BASE, 90, 30)                # a nearby independent station, NB
FAR_T6 = dest(BASE, 270, 1200)           # a T6 station far from both chirps (skywave)


def pair(a_kw=None, b_kw=None, b_shared=True):
    chirps = [crow("codar:A", "established", dest(BASE, 0, 60), FA, **(a_kw or {})),
              crow("codar:B", "established", dest(BASE, 0, 60), FB, **(b_kw or {}))]
    sweeps = [sweep("NB", FA, at_hour(3), 30.0),      # A: snr_f 1
              sweep("NB", FB, at_hour(3), 8.0)]       # B: snr_f 0.25
    if b_shared:
        sweeps.append(sweep("T6X", FB, at_hour(5, days_ago=2), 5.0))
    pos = {"NB": NEAR, "T6X": FAR_T6}
    return chirps, sweeps, pos


KA, KB = chirp.group_key(FA, RATE, 1.0), chirp.group_key(FB, RATE, 1.0)


def test_timing_and_exploration_kept_apart():
    est = crow("codar:E", "established", dest(BASE, 0, 50), FA, offset_ns=1200,
               offset_sigma_ns=20_000.0)
    cpos = grid_to_latlon("EM55")[0], grid_to_latlon("EM55")[1] - 40   # ~3000 km west
    cpos = grid_to_latlon(latlon_to_grid6(*cpos)[:4])
    cand = crow("cand:20261120-abcdef", "candidate", cpos, 5_000_000.0)
    sweeps = [sweep("NB", FA, at_hour(3), 20.0),
              sweep("FAR1", 5_000_000.0, at_hour(3), 15.0),
              sweep("FAR2", 5_000_000.0, at_hour(4), 15.0)]
    pos = {"NB": NEAR, "FAR1": dest(cpos, 0, 40), "FAR2": dest(cpos, 180, 40)}
    rows = by_key(menu(station(BASE), 3, [est, cand], sweeps, pos))
    ke, kc = est["group_key"], cand["group_key"]
    assert timing_order(rows.values())[0] == ke
    assert explore_order(rows.values())[0] == kc
    assert rows[kc]["timing_value"] == 0 and rows[kc]["rank_timing"] == 0
    assert rows[ke]["path"] == "ground_range" and rows[kc]["path"] == "skywave"
    assert rows[ke]["expected_snr_db"] == 20.0
    assert rows[kc]["exploration_value"] > rows[ke]["exploration_value"]


def test_shared_t6_first_while_offsets_open():
    chirps, sweeps, pos = pair()
    rows = by_key(menu(station(BASE), 3, chirps, sweeps, pos, t6={"T6X"}))
    assert rows[KA]["timing_value"] > rows[KB]["timing_value"] > 0
    assert rows[KB]["shared_t6"] == 1 and rows[KA]["shared_t6"] == 0
    assert timing_order(rows.values()) == [KB, KA]


def test_shared_rule_lapses_per_channel():
    chirps, sweeps, pos = pair(b_kw=dict(offset_ns=500, offset_sigma_ns=50_000.0))
    assert RULES.menu.shared_t6_override is True
    rows = by_key(menu(station(BASE), 3, chirps, sweeps, pos, t6={"T6X"}))
    assert rows[KB]["shared_t6"] == 1
    assert rows[KA]["timing_value"] > rows[KB]["timing_value"]
    assert timing_order(rows.values()) == [KA, KB]


def test_override_off_switch():
    chirps, sweeps, pos = pair()
    rows = by_key(menu(station(BASE), 3, chirps, sweeps, pos, t6={"T6X"},
                       rules=with_menu(shared_t6_override=False)))
    assert rows[KB]["shared_t6"] == 1
    assert timing_order(rows.values()) == [KA, KB]


def test_trusted_offset_sorts_by_value():
    # Both hold trusted offsets, so the shared rule has lapsed for B: the stronger, unshared
    # A still outranks the weaker shared B.
    trusted = dict(offset_ns=500, offset_sigma_ns=50_000.0)
    chirps, sweeps, pos = pair(a_kw=trusted, b_kw=trusted)
    rows = by_key(menu(station(BASE), 3, chirps, sweeps, pos, t6={"T6X"}))
    assert rows[KB]["shared_t6"] == 1 and rows[KA]["shared_t6"] == 0
    assert timing_order(rows.values()) == [KA, KB]


def test_distant_station_more_exploration():
    cpos = grid_to_latlon("EM55")
    cand = crow("cand:20261120-123456", "candidate", cpos, 5_000_000.0)
    sweeps = [sweep("H1", 5_000_000.0, at_hour(3), 15.0),
              sweep("H2", 5_000_000.0, at_hour(3), 15.0)]
    pos = {"H1": dest(cpos, 0, 30), "H2": dest(cpos, 90, 30)}
    near = menu(station(dest(cpos, 180, 50)), 3, [cand], sweeps, pos)
    far = menu(station(dest(cpos, 180, 2000)), 3, [cand], sweeps, pos)
    assert far[0]["exploration_value"] > near[0]["exploration_value"] > 0
    assert far[0]["exploration_value"] == pytest.approx(1.0)


def test_dormant_off_hours_scaled():
    dorm = crow("codar:D", "dormant", dest(BASE, 0, 60), FA, heard_hours=1 << 14)
    at3 = menu(station(BASE), 3, [dorm])[0]
    at14 = menu(station(BASE), 14, [dorm])[0]
    assert at14["timing_value"] > 0
    assert at3["timing_value"] == pytest.approx(0.2 * at14["timing_value"])
    always = crow("codar:D", "dormant", dest(BASE, 0, 60), FA, heard_hours=0)
    assert menu(station(BASE), 3, [always])[0]["timing_value"] == at14["timing_value"]


def test_shared_signature_one_channel():
    a = crow("codar:S1", "established", dest(BASE, 0, 60), FA)
    b = crow("cand:20261121-0a0b0c", "candidate", grid_to_latlon("EM56"), FA)
    assert a["group_key"] == b["group_key"]
    rows = menu(station(BASE), 3, [a, b])
    assert len(rows) == 1
    assert rows[0]["chirp_ids"] == ["cand:20261121-0a0b0c", "codar:S1"]
    assert rows[0]["states"] == ["candidate", "established"]
    assert rows[0]["rank_timing"] == 1 and rows[0]["rank_explore"] == 1


def test_station_without_position_gets_no_menu():
    st = dict(reporter_id="ME", lat=None, lon=None, is_t6=False)
    with pytest.raises(ValueError):
        menu(st, 3, [crow("codar:A", "established", dest(BASE, 0, 60), FA)])


def test_ties_break_by_key():
    # Same position, same evidence (none), same cost: only the key tells them apart.
    p = dest(BASE, 0, 60)
    chirps = [crow("codar:Z", "established", p, 4_900_000.0),
              crow("codar:Y", "established", p, 4_600_000.0),
              crow("cand:20261121-111111", "candidate", grid_to_latlon("EN55"), 5_300_000.0),
              crow("cand:20261121-222222", "candidate", grid_to_latlon("EN55"), 5_200_000.0)]
    rows = menu(station(BASE), 3, chirps)
    ky, kz = chirps[1]["group_key"], chirps[0]["group_key"]
    assert timing_order(rows) == sorted([ky, kz])
    k1, k2 = chirps[2]["group_key"], chirps[3]["group_key"]
    assert explore_order(rows)[:2] == sorted([k1, k2])


def test_rows_carry_the_contract_fields():
    rows = menu(station(BASE), 3, [crow("codar:A", "established", dest(BASE, 0, 60), FA)])
    assert set(rows[0]) == {"channel_key", "centre_hz", "rate_hz_per_s", "period_s",
                            "chirp_ids", "states", "timing_value", "exploration_value",
                            "cost", "shared_t6", "expected_snr_db", "path", "rank_timing",
                            "rank_explore"}


def test_retired_rejected_and_hfdl_stay_off():
    chirps = [crow("codar:R", "retired", dest(BASE, 0, 60), FA),
              crow("cand:20261121-333333", "rejected", grid_to_latlon("EM56"), FB),
              crow("seen:1", "seen", grid_to_latlon("EM56"), 4_700_000.0),
              crow("hfdl:1", "established", dest(BASE, 0, 60), 5_100_000.0, kind="hfdl")]
    assert menu(station(BASE), 3, chirps) == []


def test_top_n_cuts_each_ranking():
    chirps = [crow("codar:%02d" % i, "established", dest(BASE, 0, 60), 4.0e6 + i * 10_000)
              for i in range(5)]
    rows = menu(station(BASE), 3, chirps, rules=with_menu(top_n=2))
    assert sorted(r["rank_timing"] for r in rows if r["rank_timing"]) == [1, 2]
    assert sorted(r["rank_explore"] for r in rows if r["rank_explore"]) == [1, 2]
    assert all(r["rank_timing"] or r["rank_explore"] for r in rows)


# --- the verb ---------------------------------------------------------------------------------

def test_menu_verb_prints_both_rankings(monkeypatch, capsys):
    from test_sigmondb_chirp_job import JobClient
    est = crow("codar:E", "established", dest(BASE, 0, 50), FA, offset_ns=1200,
               offset_sigma_ns=20_000.0)
    cand = crow("cand:20261120-abcdef", "candidate", grid_to_latlon("EM15"), 5_000_000.0)
    log = [dict(at=NOW - timedelta(days=2), chirp_id=c["chirp_id"], from_state="",
                to_state=c["state"], actor="seed", why="", evidence={}, reporter_ids=[],
                rules_version=RULES.rules_version) for c in (est, cand)]
    rxs = [dict(reporter_id="NB", lat=NEAR[0], lon=NEAR[1], pos_source="gpsdo",
                coherence_group="", valid_from=datetime(2026, 1, 1), version=1)]
    client = JobClient({"chirp": [dict(c, updated_at=NOW) for c in (est, cand)],
                        "chirp_log": log, "receiver": rxs},
                       aggs={"sweep_hourly": [sweep("NB", FA, at_hour(3), 20.0),
                                              sweep("FAR1", 5_000_000.0, at_hour(3), 9.0)]})
    monkeypatch.setattr(cli, "connect", lambda conf: client)
    monkeypatch.setattr(cli, "_now_s", lambda: NOW)
    grid = latlon_to_grid6(*BASE)
    assert cli.main(["menu", "--grid", grid, "--hour", "3"]) == 0
    out = capsys.readouterr().out
    assert "timing" in out and "exploration" in out
    assert est["group_key"] in out and cand["group_key"] in out
    lat, lon = g6(BASE)
    for coord in ("%.2f" % lat, "%.2f" % lon, "%.3f" % lat, "%.3f" % lon):
        assert coord not in out
    assert client.inserted == [] and client.commands == []


def test_menu_verb_refuses_bad_grid_or_hour(capsys):
    with pytest.raises(SystemExit):
        cli.main(["menu", "--grid", "EM55", "--hour", "3"])
    with pytest.raises(SystemExit):
        cli.main(["menu", "--grid", "EM55aa", "--hour", "24"])


def test_probers_and_history_window():
    cpos = grid_to_latlon("EM15")
    cand = crow("cand:20261120-654321", "candidate", cpos, 5_000_000.0)
    cap = dict(reporter_id="P1", centre_hz=5_000_000.0, rate_hz_per_s=RATE, n=2, n_found=1)
    alone = menu(station(BASE), 3, [cand])[0]["exploration_value"]
    probed = menu(station(BASE), 3, [cand], captures=[cap])[0]["exploration_value"]
    assert probed == pytest.approx(alone / 2)
    est = crow("codar:E", "established", dest(BASE, 0, 50), FA)
    old = sweep("NB", FA, at_hour(3, days_ago=RULES.menu.history_days + 1), 20.0)
    assert menu(station(BASE), 3, [est], [old], {"NB": NEAR})[0]["expected_snr_db"] is None
    new = sweep("NB", FA, at_hour(3, days_ago=2), 20.0)
    assert menu(station(BASE), 3, [est], [new], {"NB": NEAR})[0]["expected_snr_db"] == 20.0


def test_own_hearing_is_not_another_station():
    cpos = grid_to_latlon("EM15")
    cand = crow("cand:20261120-777777", "candidate", cpos, 5_000_000.0)
    s = sweep("ME", 5_000_000.0, at_hour(3), 12.0)
    row = menu(station(BASE), 3, [cand], [s], {"ME": BASE})[0]
    assert row["exploration_value"] == pytest.approx(RULES.menu.state_weight["candidate"])


def test_t6_in_reach_doubles_timing_value():
    est = crow("codar:E", "established", dest(BASE, 0, 50), FA)
    base = menu(station(BASE), 3, [est])[0]["timing_value"]
    assert menu(station(BASE, is_t6=True), 3, [est])[0]["timing_value"] == pytest.approx(2 * base)
    s = sweep("T6N", FA, at_hour(5), 12.0)         # heard at 05Z: no expected SNR at 03Z
    far = {"T6N": dest(BASE, 180, 900)}             # a T6 hearer, skywave from the chirp
    near = {"T6N": dest(BASE, 0, 80)}               # a T6 hearer within ground range
    v_far = menu(station(BASE), 3, [est], [s], far, t6={"T6N"})[0]["timing_value"]
    v_near = menu(station(BASE), 3, [est], [s], near, t6={"T6N"})[0]["timing_value"]
    assert v_near == pytest.approx(2 * v_far) and v_far > 0
