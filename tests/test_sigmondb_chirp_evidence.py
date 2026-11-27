"""Evidence per chirp: pure functions over already-fetched rows.  No ClickHouse.
Positions are made-up sites, never a real station's."""
import dataclasses
import os
import random
import sys
from datetime import datetime, timedelta

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from sigmondb import chirp_evidence as ce  # noqa: E402
from sigmondb.chirp import group_key, load_rules  # noqa: E402
from sigmondb.set_aside import not_set_aside  # noqa: E402

RULES = load_rules()
NOW = datetime(2026, 11, 20, 12, 0, 0)
CENTRE, RATE, PERIOD = 4_537_000.0, -25_730.0, 1.0
GK = group_key(CENTRE, RATE, PERIOD)


def a_chirp(cid="C1", centre=CENTRE, rate=RATE, period=PERIOD, tx_id=""):
    return dict(chirp_id=cid, group_key=group_key(centre, rate, period), centre_hz=centre,
                rate_hz_per_s=rate, period_s=period, span_hz=None, duration_s=None,
                tx_id=tx_id)


def sweep(rid, hour, centre=CENTRE, rate=RATE, period=PERIOD, clipped=0):
    return dict(reporter_id=rid, centre_hz=centre, rate_hz_per_s=rate, period_s=period,
                span_hz=25_000.0, duration_s=None, clipped=clipped, hour=hour, n=3,
                snr_med=12.0, n_dwells=2)


def dwell(rid, hour, centre=CENTRE, span=200_000.0, n=4):
    return dict(reporter_id=rid, centre_hz=centre, span_hz=span, hour=hour, n=n)


def rx(rid, lat, lon, coherence="", src="gpsdo"):
    return dict(reporter_id=rid, lat=lat, lon=lon, pos_source=src, coherence_group=coherence,
                valid_from=datetime(2026, 1, 1), version=1)


def aggs(sweeps=(), dwells=(), captures=(), arrivals=()):
    return {"sweep_hourly": list(sweeps), "dwell_hourly": list(dwells),
            "capture_recent": list(captures), "arrival_hourly": list(arrivals)}


H = [NOW.replace(minute=0) - timedelta(hours=k) for k in range(48)]


def test_shared_signature_credits_all_members():
    chirps = [a_chirp("A"), a_chirp("B"), a_chirp("Z", centre=13_450_000.0, rate=33_000.0)]
    ev = ce.gather(chirps, aggs(sweeps=[sweep("R1", H[2])]), [], [rx("R1", 10.0, 20.0)], [],
                   RULES, NOW)
    assert set(ev) == {"A", "B", "Z"}
    assert ev["A"].hearers == {"R1": [H[2]]}
    assert ev["B"].hearers == {"R1": [H[2]]}
    assert ev["A"].shared_signature == ["B"]
    assert ev["B"].shared_signature == ["A"]
    assert ev["Z"].hearers == {} and ev["Z"].shared_signature == []


def test_uncovered_station_is_not_absence():
    dwells = [dwell("FAR", H[1], centre=10_000_000.0), dwell("FAR", H[2], centre=10_000_000.0),
              dwell("NEAR", H[1]), dwell("NEAR", H[2], centre=CENTRE + 90_000.0),
              dwell("HEARS", H[1])]
    receivers = [rx("FAR", 10.0, 20.0), rx("NEAR", 15.0, 20.0), rx("HEARS", 20.0, 20.0)]
    ev = ce.gather([a_chirp()], aggs(sweeps=[sweep("HEARS", H[1])], dwells=dwells), [],
                   receivers, [], RULES, NOW)["C1"]
    assert "FAR" not in ev.covered_silent
    assert "HEARS" not in ev.covered_silent      # it dwelled and heard: not silent
    assert ev.covered_silent == {"NEAR": 8}      # two covering dwell hours, 4 dwells each
    assert list(ev.hearers) == ["HEARS"]


def test_colocated_count_once():
    receivers = [rx("KFS-OMNI", 10.0, 20.0, coherence="KFS"),
                 rx("KFS-MAIN", 10.5, 20.5, coherence="KFS"),
                 rx("OTHER", 30.0, 40.0)]
    sw = [sweep("KFS-OMNI", H[1]), sweep("KFS-MAIN", H[2]), sweep("OTHER", H[3])]
    ev = ce.gather([a_chirp()], aggs(sweeps=sw), [], receivers, [], RULES, NOW)["C1"]
    assert len(ev.hearers) == 2
    kfs = [k for k in ev.hearers if k.startswith("KFS")]
    assert len(kfs) == 1
    assert ev.hearers[kfs[0]] == [H[2], H[1]]


def test_independent_uses_rules_radius():
    rxpos = {"A": (10.0, 20.0, "gpsdo"), "B": (10.0, 20.7, "grid")}   # about 77 km apart
    assert ce.independent(["A", "B"], rxpos, {}, RULES) == {"A": "A", "B": "B"}
    wide = dataclasses.replace(RULES, independence=dataclasses.replace(
        RULES.independence, co_site_km=100.0))
    assert ce.independent(["A", "B"], rxpos, {}, wide) == {"A": "A", "B": "A"}
    assert ce.independent(["A", "B"], rxpos, {"A": "g", "B": "g"}, RULES) == {"A": "A", "B": "A"}


def test_local_reject_ignored_for_that_reporter():
    rejects = [dict(reporter_id="R1", centre_hz=CENTRE + 500.0, rate_hz_per_s=RATE,
                    period_s=PERIOD, span_hz=None, chirp_id="", active=1),
               dict(reporter_id="R2", centre_hz=CENTRE, rate_hz_per_s=RATE,
                    period_s=PERIOD, span_hz=None, chirp_id="", active=0)]   # withdrawn
    receivers = [rx("R1", 10.0, 20.0), rx("R2", 30.0, 40.0)]
    sw = [sweep("R1", H[1]), sweep("R2", H[1])]
    ev = ce.gather([a_chirp()], aggs(sweeps=sw, dwells=[dwell("R1", H[1])]), [], receivers, [],
                   RULES, NOW, rejects=rejects)["C1"]
    assert list(ev.hearers) == ["R2"]
    assert "R1" not in ev.covered_silent      # a rejected streak is not silence either


def t6_rows(rid, hours, base_ms, sd_us, rng, step_min=10):
    out = []
    t0 = NOW - timedelta(hours=hours)
    for k in range(int(hours * 60 / step_min)):
        d_ms = base_ms + rng.gauss(0.0, sd_us / 1e3)
        out.append(dict(time=t0 + timedelta(minutes=step_min * k), reporter_id=rid,
                        group_key=GK, delay_ns=int(round(d_ms * 1e6)), level_db=20.0))
    return out


def test_steady_t6_cluster_hours():
    rng = random.Random(7)
    steady = t6_rows("S1", 30, 12.345, 40.0, rng)
    scatter = [dict(r, reporter_id="S2", delay_ns=int(rng.uniform(0, 1e9)))
               for r in t6_rows("S1", 30, 0.0, 0.0, rng)]
    loose = [dict(r, reporter_id="S3") for r in t6_rows("S1", 30, 40.0, 400.0, rng)]
    receivers = [rx("S1", 10.0, 20.0), rx("S2", 30.0, 40.0), rx("S3", 50.0, 60.0)]
    ev = ce.gather([a_chirp()], aggs(), steady + scatter + loose, receivers, [], RULES,
                   NOW)["C1"]
    assert ev.steady_t6["S1"] >= 24
    assert "S2" not in ev.steady_t6          # no cluster at all
    assert "S3" not in ev.steady_t6          # a cluster, but wider than max_cluster_mad_us
    assert ev.t6_hearers == {"S1", "S2", "S3"}


def test_arrival_counts_as_heard_and_sets_hours():
    arr = [dict(reporter_id="R9", centre_hz=CENTRE, rate_hz_per_s=RATE, hour=H[0], n=40,
                n_t6=0),
           dict(reporter_id="R9", centre_hz=CENTRE, rate_hz_per_s=RATE,
                hour=NOW.replace(minute=0) - timedelta(days=16, hours=3), n=4, n_t6=0)]
    ev = ce.gather([a_chirp()], aggs(arrivals=arr, sweeps=[sweep("R1", H[5])]), [],
                   [rx("R9", 10.0, 20.0), rx("R1", 30.0, 40.0)], [], RULES, NOW)["C1"]
    assert set(ev.hearers) == {"R9", "R1"}
    assert ev.last_heard == H[0]
    # bits only within dormant.history_days: 12Z and 07Z, not the 16-day-old 09Z hour
    assert ev.heard_hours_bits == (1 << 12) | (1 << 7)


def test_probes_waveform_and_attribution():
    caps = [dict(reporter_id="P1", centre_hz=CENTRE, rate_hz_per_s=RATE, first_t=H[3],
                 last_t=H[1], n=5, n_found=2),
            dict(reporter_id="P2", centre_hz=CENTRE, rate_hz_per_s=RATE, first_t=H[3],
                 last_t=H[1], n=5, n_found=0),
            dict(reporter_id="P3", centre_hz=9e6, rate_hz_per_s=RATE, first_t=H[3],
                 last_t=H[1], n=5, n_found=5)]
    sw = [sweep("P1", H[1]), sweep("P2", H[1], centre=CENTRE + 2500.0),
          sweep("P2", H[2], centre=CENTRE + 2500.0), sweep("P1", H[2], clipped=1),
          sweep("P3", H[1], clipped=1)]   # P3 heard it clipped only: no waveform vote
    att = [dict(run_at=NOW - timedelta(hours=1), mode_policy="default", status="named",
                p_null=0.0, tx_id="SITE", group_key=GK),
           dict(run_at=NOW - timedelta(hours=1), mode_policy="default", status="named",
                p_null=0.0, tx_id="ELSE", group_key=group_key(9e6, RATE, 1.0))]
    receivers = [rx("P1", 10.0, 20.0), rx("P2", 30.0, 40.0), rx("P3", 50.0, 60.0)]
    ev = ce.gather([a_chirp()], aggs(sweeps=sw, captures=caps), [], receivers, att, RULES,
                   NOW)["C1"]
    assert ev.probe_stations == {"P1", "P2"}
    assert ev.probe_found == {"P1"}
    assert ev.waveform_agree == 2
    assert set(ev.hearers) == {"P1", "P2", "P3"}
    assert ev.named_by_attribution == "SITE"
    assert ev.position is None


def test_no_data_gives_empty_evidence():
    ev = ce.gather([a_chirp()], aggs(), [], [], [], RULES, NOW)["C1"]
    assert ev == ce.Evidence()
    assert ev.heard_hours_bits == 0 and ev.last_heard is None and ev.hearers == {}
    assert ce.gather([a_chirp()], {}, [], [], [], RULES, NOW)["C1"] == ce.Evidence()


def test_agg_sql_excludes_hfdl_and_set_aside():
    assert ce.AGG_SQL["sweep_hourly"][0] == (
        "reporter_id", "centre_hz", "rate_hz_per_s", "period_s", "span_hz", "duration_s",
        "clipped", "hour", "n", "snr_med", "n_dwells")
    assert ce.AGG_SQL["dwell_hourly"][0] == ("reporter_id", "centre_hz", "span_hz", "hour", "n")
    assert ce.AGG_SQL["capture_recent"][0] == (
        "reporter_id", "centre_hz", "rate_hz_per_s", "first_t", "last_t", "n", "n_found")
    assert ce.AGG_SQL["arrival_hourly"][0] == (
        "reporter_id", "centre_hz", "rate_hz_per_s", "hour", "n", "n_t6")
    for name, (_cols, sql) in ce.AGG_SQL.items():
        assert not_set_aside("codar") in sql, name
        assert "{hours:UInt32}" in sql, name
        assert "/* agg:%s */" % name in sql
    assert "startsWith(group_key, 'hfdl')" in ce.AGG_SQL["arrival_hourly"][1]
    assert "startsWith(matched_key, 'hfdl')" in ce.AGG_SQL["sweep_hourly"][1]
    assert "rate_hz_per_s != 0" in ce.AGG_SQL["capture_recent"][1]   # capture has no group_key
    assert "FROM sigmondb.capture" in ce.AGG_SQL["capture_recent"][1]
    assert "FROM sigmondb.arrival" in ce.AGG_SQL["arrival_hourly"][1]
    assert ce.agg_params(RULES) == {"hours": 21 * 24}


def test_two_names_on_one_signature():
    att = [dict(run_at=NOW - timedelta(hours=1), mode_policy="default", status="named",
                p_null=0.0, tx_id=t, group_key=GK) for t in ("SITE-A", "SITE-B")]
    att.append(dict(att[0], tx_id="SITE-C", run_at=NOW - timedelta(hours=9)))   # too old
    ev = ce.gather([a_chirp("A", tx_id="SITE-B"), a_chirp("X")], aggs(), [], [], att, RULES,
                   NOW)
    assert ev["A"].named_by_attribution == "SITE-B"     # its own name stands among two
    assert ev["X"].named_by_attribution is None         # two names, none its own: no name


def test_arrival_hour_is_not_silence():
    arr = [dict(reporter_id="R1", centre_hz=CENTRE, rate_hz_per_s=RATE, hour=H[1], n=30,
                n_t6=0)]
    ev = ce.gather([a_chirp()], aggs(arrivals=arr, dwells=[dwell("R1", H[1]), dwell("R1", H[2])]),
                   [], [rx("R1", 10.0, 20.0)], [], RULES, NOW)["C1"]
    assert ev.hearers == {"R1": [H[1]]}
    assert ev.covered_silent == {"R1": 4}        # H[2] only: in H[1] chirp-recorder heard it


def test_reject_blocks_its_reporter_only():
    receivers = [rx("KFS-OMNI", 10.0, 20.0, coherence="KFS"),
                 rx("KFS-MAIN", 10.5, 20.5, coherence="KFS")]
    rejects = [dict(reporter_id="KFS-MAIN", centre_hz=CENTRE, rate_hz_per_s=RATE,
                    period_s=PERIOD, span_hz=None, chirp_id="", active=1)]
    ev = ce.gather([a_chirp()], aggs(sweeps=[sweep("KFS-MAIN", H[1])],
                                     dwells=[dwell("KFS-MAIN", H[1], n=3),
                                             dwell("KFS-OMNI", H[1], n=4)]),
                   [], receivers, [], RULES, NOW, rejects=rejects)["C1"]
    assert ev.hearers == {}
    assert ev.covered_silent == {"KFS-OMNI": 4}  # the partner's own silent dwell counts


def test_named_from_latest_run_per_group():
    old = dict(run_at=NOW - timedelta(hours=2), run_id="r1", mode_policy="default",
               status="named", p_null=0.0, tx_id="SITE-A", group_key=GK)
    new = dict(old, run_at=NOW - timedelta(hours=1), run_id="r2", tx_id="SITE-B")
    ev = ce.gather([a_chirp("X")], aggs(), [], [], [old, new], RULES, NOW)
    assert ev["X"].named_by_attribution == "SITE-B"
    tie = dict(new, run_id="r3", tx_id="SITE-C")          # same run_at: the larger run_id wins
    ev = ce.gather([a_chirp("X")], aggs(), [], [], [old, new, tie], RULES, NOW)
    assert ev["X"].named_by_attribution == "SITE-C"


def test_sweep_span_from_whole_streaks_only():
    sql = ce.AGG_SQL["sweep_hourly"][1]
    assert "medianIf(span_hz, clipped = 0) AS span_hz" in sql
    assert "medianIf(duration_s, clipped = 0) AS duration_s" in sql


def test_wide_sweep_matches_far_centre():
    wide = dict(a_chirp("W", centre=16e6, rate=100_000.0), span_hz=28e6)
    sw = dict(sweep("R1", H[1], centre=15e6, rate=100_000.0), span_hz=26e6)   # 1 MHz off
    ev = ce.gather([wide], aggs(sweeps=[sw]), [], [rx("R1", 10.0, 20.0)], [], RULES, NOW)
    assert ev["W"].hearers == {"R1": [H[1]]}


def test_centre_buckets_change_nothing(monkeypatch):
    rng = random.Random(11)
    tol = RULES.match.centre_tol_hz
    chirps = [a_chirp("C%d" % k, centre=CENTRE + k * tol * 0.7) for k in range(12)]
    chirps.append(dict(a_chirp("W", centre=16e6, rate=100_000.0), span_hz=28e6))
    hrs = H[:6]
    sw = [dict(sweep("R%d" % rng.randrange(5), rng.choice(hrs),
                     centre=CENTRE + rng.uniform(-2, 10) * tol),
               clipped=rng.randrange(2)) for _ in range(150)]
    sw += [dict(sweep("R1", H[1], centre=15e6, rate=100_000.0), span_hz=26e6)]
    arr = [dict(reporter_id="R%d" % rng.randrange(5), centre_hz=CENTRE + rng.uniform(-2, 10) * tol,
                rate_hz_per_s=RATE, hour=rng.choice(hrs), n=3, n_t6=0) for _ in range(80)]
    caps = [dict(reporter_id="R%d" % rng.randrange(5), centre_hz=CENTRE + rng.uniform(-2, 10) * tol,
                 rate_hz_per_s=RATE, first_t=H[3], last_t=H[1], n=2, n_found=rng.randrange(2))
            for _ in range(40)]
    dw = [dwell("R%d" % k, h) for k in range(5) for h in hrs]
    receivers = [rx("R%d" % k, 10.0 * k, 20.0) for k in range(5)]
    args = ([*chirps], aggs(sweeps=sw, arrivals=arr, captures=caps, dwells=dw), [], receivers,
            [], RULES, NOW)
    fast = ce.gather(*args)
    monkeypatch.setattr(ce._ByCentre, "candidates", lambda self, sig: list(range(self.n)))
    monkeypatch.setattr(ce._ByCentre, "__init__",
                        lambda self, sigs, rules: setattr(self, "n", len(sigs)))
    assert ce.gather(*args) == fast
    assert any(ev.hearers for ev in fast.values())
