"""The chirp lifecycle's moves: a pure function over one chirp and its Evidence.  No ClickHouse.
Stations are made-up sites; positions are made-up grid centres."""
import os
import sys
from datetime import datetime, timedelta

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from sigmondb.chirp import load_rules  # noqa: E402
from sigmondb.chirp_evidence import Evidence  # noqa: E402
from sigmondb.chirp_lifecycle import STATES, Move, next_state  # noqa: E402

RULES = load_rules()
NOW = datetime(2026, 11, 20, 12, 0, 0)
H = timedelta(hours=1)
D = timedelta(days=1)

# Made-up grid6 centres: S1 and two neighbours a few hundred km apart, FAR well beyond 800 km.
POS = {"S1": (40.0208, -90.0417), "N1": (41.0208, -89.0417), "N2": (39.0208, -91.0417),
       "FAR": (30.0208, -70.0417)}


def a_chirp(state, since=NOW - 2 * D, pin="", first=NOW - 3 * D):
    return dict(chirp_id="C1", state=state, state_since=since, first_heard=first,
                last_heard=None, pin=pin, tx_id="")


def hours_ago(*ns):
    """Hour starts n hours before NOW, oldest first."""
    return sorted(NOW - n * H for n in ns)


def heard(hearers, **kw):
    """Evidence whose last_heard is the latest hour any hearer heard."""
    all_h = [h for hs in hearers.values() for h in hs]
    return Evidence(hearers=hearers, last_heard=max(all_h) if all_h else None, **kw)


def established_ev(**over):
    kw = dict(waveform_agree=3, steady_t6={"S1": 30.0, "N1": 26.0},
              position={"status": "located", "lat": 40.0, "lon": -80.0, "pos_sigma_m": 20_000.0},
              probe_stations={"S1", "N1"})
    kw.update(over)
    return heard({"S1": hours_ago(1, 2), "N1": hours_ago(1)}, **kw)


# --- seen -------------------------------------------------------------------------------------

def test_seen_to_candidate_by_hours():
    # Six distinct hours at one station: three dwells at least 1 h apart, and then some.
    m = next_state(a_chirp("seen"), heard({"S1": hours_ago(1, 3, 5, 7, 9, 11)}), RULES, NOW)
    assert m.to_state == "candidate" and m.why == "persists_hours"
    assert m.evidence["candidate.min_hours"] == "6"
    # Five hours is short of min_hours: no move (and heard an hour ago, so not dropped).
    assert next_state(a_chirp("seen"), heard({"S1": hours_ago(1, 3, 5, 7, 9)}), RULES, NOW) is None


def test_seen_to_candidate_by_stations():
    m = next_state(a_chirp("seen"), heard({"S1": hours_ago(2), "N1": hours_ago(20)}), RULES, NOW)
    assert m.to_state == "candidate" and m.why == "persists_stations"
    assert m.evidence["n_hearers"] == "2" and m.evidence["hearers"] == "N1,S1"
    # The second station heard it 30 h ago, outside window_h: one station only.
    assert next_state(a_chirp("seen"), heard({"S1": hours_ago(2), "N1": hours_ago(30)}),
                      RULES, NOW) is None


def test_seen_to_dropped():
    m = next_state(a_chirp("seen"), heard({"S1": hours_ago(49)}), RULES, NOW)
    assert m.to_state == "dropped" and m.why == "unheard"
    assert m.evidence["seen.drop_after_h"] == "48"
    # Never heard in the window: unheard counts from first_heard.
    m = next_state(a_chirp("seen", first=NOW - 50 * H), Evidence(), RULES, NOW)
    assert m.to_state == "dropped"
    assert next_state(a_chirp("seen", first=NOW - 40 * H), Evidence(), RULES, NOW) is None


# --- candidate --------------------------------------------------------------------------------

def test_candidate_to_probing():
    ev = heard({"S1": hours_ago(1)}, probe_stations={"S1"})
    m = next_state(a_chirp("candidate"), ev, RULES, NOW)
    assert m.to_state == "probing" and m.why == "probes_began"
    assert m.evidence["probe_stations"] == "S1"
    assert next_state(a_chirp("candidate"), heard({"S1": hours_ago(1)}), RULES, NOW) is None


def test_candidate_to_dropped():
    m = next_state(a_chirp("candidate", since=NOW - 10 * D), heard({"S1": [NOW - 8 * D]}),
                   RULES, NOW)
    assert m.to_state == "dropped" and m.why == "unheard" and m.local_reject is None
    assert next_state(a_chirp("candidate", since=NOW - 10 * D), heard({"S1": [NOW - 6 * D]}),
                      RULES, NOW) is None


def test_local_only_goes_local_reject():
    ev = heard({"S1": hours_ago(1, 2)}, silent_hours={"N1": 6, "N2": 9, "S1": 3})
    m = next_state(a_chirp("candidate", since=NOW - 25 * H), ev, RULES, NOW, station_pos=POS)
    assert m.to_state == "dropped" and m.why == "local_only"
    assert m.local_reject == "S1"
    assert m.evidence["local_reject"] == "S1" and m.evidence["neighbours"] == "N1,N2"
    assert m.evidence["silent_hours"] == "N1:6,N2:9"
    # Neighbours count silent hours over the whole window, not only after the last hearing
    # (the one station hears a local spur every hour): covered_silent plays no part.
    assert next_state(a_chirp("candidate", since=NOW - 25 * H),
                      heard({"S1": hours_ago(1, 2)}, covered_silent={"N1": 6, "N2": 9}),
                      RULES, NOW, station_pos=POS) is None
    # Without station positions the move cannot fire.
    assert next_state(a_chirp("candidate", since=NOW - 25 * H), ev, RULES, NOW) is None
    # Too soon after becoming a candidate.
    assert next_state(a_chirp("candidate", since=NOW - 20 * H), ev, RULES, NOW,
                      station_pos=POS) is None
    # One neighbour short of its dwells, the other too far away: not enough neighbours.
    for silent in ({"N1": 6, "N2": 5}, {"N1": 6, "FAR": 20}):
        ev2 = heard({"S1": hours_ago(1)}, silent_hours=silent)
        assert next_state(a_chirp("candidate", since=NOW - 25 * H), ev2, RULES, NOW,
                          station_pos=POS) is None
    # Two stations hear it: not local.
    ev3 = heard({"S1": hours_ago(1), "N1": hours_ago(1)}, silent_hours={"N2": 9, "FAR": 9})
    assert next_state(a_chirp("candidate", since=NOW - 25 * H), ev3, RULES, NOW,
                      station_pos=POS) is None


# --- probing ----------------------------------------------------------------------------------

def test_probing_to_established():
    m = next_state(a_chirp("probing"), established_ev(), RULES, NOW)
    assert m.to_state == "established" and m.why == "settled"
    assert m.evidence["position"] == "located"
    # A listed site's name stands in for a triangulated position.
    ev = established_ev(position=None, named_by_attribution="codar:LISL")
    m = next_state(a_chirp("probing"), ev, RULES, NOW)
    assert m.to_state == "established" and m.evidence["position"] == "named:codar:LISL"


def test_probing_needs_all_three():
    missing = [
        dict(waveform_agree=2),
        dict(steady_t6={"S1": 30.0, "N1": 20.0}),
        dict(position={"status": "located", "lat": 40.0, "lon": -80.0, "pos_sigma_m": 60_000.0}),
        dict(position={"status": "ambiguous", "lat": None, "lon": None, "pos_sigma_m": None}),
        dict(position=None, named_by_attribution="cand:20261119-abcdef"),
        dict(position=None),
    ]
    for over in missing:
        assert next_state(a_chirp("probing"), established_ev(**over), RULES, NOW) is None, over


def test_probing_to_rejected_one_station():
    ev = heard({"S1": hours_ago(1, 5)}, probe_stations={"S1", "N1", "N2"}, probe_found={"S1"})
    m = next_state(a_chirp("probing", since=NOW - 73 * H), ev, RULES, NOW)
    assert m.to_state == "rejected" and m.why == "one_station"
    assert m.evidence["rejected.min_probe_stations"] == "3"
    # Not yet min_probing_h in probing.
    assert next_state(a_chirp("probing", since=NOW - 70 * H), ev, RULES, NOW) is None
    # A second station found it.
    ev2 = heard({"S1": hours_ago(1)}, probe_stations={"S1", "N1", "N2"},
                probe_found={"S1", "N1"})
    assert next_state(a_chirp("probing", since=NOW - 73 * H), ev2, RULES, NOW) is None


def test_probing_to_candidate_after_max_days():
    ev = heard({"S1": hours_ago(1)}, probe_stations={"S1"})
    m = next_state(a_chirp("probing", since=NOW - 29 * D), ev, RULES, NOW)
    assert m == Move("candidate", "probing_timeout", m.evidence)
    assert m.evidence["probing_days"] == "29" and m.evidence["probing.max_days"] == "28"
    assert next_state(a_chirp("probing", since=NOW - 27 * D), ev, RULES, NOW) is None
    # Evidence that settles it on its last day still counts.
    m = next_state(a_chirp("probing", since=NOW - 29 * D), established_ev(), RULES, NOW)
    assert m.to_state == "established"


# --- established, dormant, retired ------------------------------------------------------------

def test_established_to_dormant():
    ev = heard({"S1": [NOW - 2 * D]}, covered_silent={"S1": 6})
    m = next_state(a_chirp("established", since=NOW - 9 * D), ev, RULES, NOW)
    assert m.to_state == "dormant" and m.why == "unheard_where_covered"
    assert m.evidence["covered_silent"] == "S1:6"
    # Heard 20 h ago: not yet dormant.after_h.
    ev2 = heard({"S1": [NOW - 20 * H]}, covered_silent={"S1": 6})
    assert next_state(a_chirp("established", since=NOW - 9 * D), ev2, RULES, NOW) is None
    # The silent station never heard it: no past hearer covered.
    ev3 = heard({"S1": [NOW - 2 * D]}, covered_silent={"N1": 20})
    assert next_state(a_chirp("established", since=NOW - 9 * D), ev3, RULES, NOW) is None
    # A T6 hearer counts as a past hearer.
    ev4 = heard({"S1": [NOW - 2 * D]}, covered_silent={"N1": 20}, t6_hearers={"N1"})
    assert next_state(a_chirp("established", since=NOW - 9 * D), ev4, RULES,
                      NOW).to_state == "dormant"


def test_no_coverage_no_dormant():
    ev = heard({"S1": [NOW - 10 * D]}, covered_silent={})
    assert next_state(a_chirp("established", since=NOW - 30 * D), ev, RULES, NOW) is None


def test_shared_signature_absence_needs_no_arrivals():
    # The scout no longer hears it at S1 (covered-silent there), and a sibling shares its
    # signature, but T6 arrivals continue: last_heard counts arrivals of any origin.
    ev = heard({"S1": [NOW - 3 * D, NOW - 2 * H]}, covered_silent={"S1": 12},
               t6_hearers={"S1"}, shared_signature=["C2"])
    assert next_state(a_chirp("established", since=NOW - 9 * D), ev, RULES, NOW) is None
    # Once the arrivals stop too, it goes dormant.
    ev2 = heard({"S1": [NOW - 3 * D, NOW - 30 * H]}, covered_silent={"S1": 12},
                t6_hearers={"S1"}, shared_signature=["C2"])
    assert next_state(a_chirp("established", since=NOW - 9 * D), ev2, RULES,
                      NOW).to_state == "dormant"


def test_dormant_to_established():
    ev = heard({"S1": [NOW - 1 * H]})
    m = next_state(a_chirp("dormant", since=NOW - 5 * D), ev, RULES, NOW)
    assert m.to_state == "established" and m.why == "heard_again"
    # Last heard before it went dormant: stays.
    ev2 = heard({"S1": [NOW - 6 * D]})
    assert next_state(a_chirp("dormant", since=NOW - 5 * D), ev2, RULES, NOW) is None


def test_dormant_to_retired():
    ev = heard({"S1": [NOW - 22 * D]}, covered_silent={"S1": 40})
    m = next_state(a_chirp("dormant", since=NOW - 21 * D), ev, RULES, NOW)
    assert m.to_state == "retired" and m.why == "unheard_where_covered"
    assert m.evidence["retired.after_days"] == "21"
    # Unheard as long, but nobody listened: stays dormant.
    ev2 = heard({"S1": [NOW - 22 * D]})
    assert next_state(a_chirp("dormant", since=NOW - 21 * D), ev2, RULES, NOW) is None
    # Unheard only 20 days.
    ev3 = heard({"S1": [NOW - 20 * D]}, covered_silent={"S1": 40})
    assert next_state(a_chirp("dormant", since=NOW - 19 * D), ev3, RULES, NOW) is None


def test_retired_to_candidate():
    m = next_state(a_chirp("retired", since=NOW - 3 * D), heard({"N1": [NOW - 2 * H]}),
                   RULES, NOW)
    assert m.to_state == "candidate" and m.why == "heard_again"
    assert next_state(a_chirp("retired", since=NOW - 3 * D), Evidence(), RULES, NOW) is None


def test_dropped_to_seen():
    m = next_state(a_chirp("dropped", since=NOW - 3 * D), heard({"N1": [NOW - 2 * H]}),
                   RULES, NOW)
    assert m.to_state == "seen" and m.why == "heard_again"
    assert next_state(a_chirp("dropped", since=NOW - 3 * D), heard({"N1": [NOW - 4 * D]}),
                      RULES, NOW) is None


# --- people and rejected ----------------------------------------------------------------------

def test_pin_and_veto_hold():
    pinned = a_chirp("established", since=NOW - 40 * D, pin="pin", first=NOW - 60 * D)
    ev = Evidence(covered_silent={"S1": 50, "N1": 50}, t6_hearers={"S1"})
    assert next_state(pinned, ev, RULES, NOW) is None
    five = {s: hours_ago(1, 2, 3, 4, 5, 6) for s in ("S1", "S2", "S3", "S4", "S5")}
    vetoed = a_chirp("rejected", since=NOW - 5 * D, pin="veto")
    assert next_state(vetoed, heard(five, probe_stations=set(five)), RULES, NOW) is None


def test_rejected_never_moves():
    five = {s: hours_ago(1, 2, 3, 4, 5, 6) for s in ("S1", "S2", "S3", "S4", "S5")}
    assert next_state(a_chirp("rejected"), heard(five), RULES, NOW) is None


def test_evidence_strings():
    cases = [
        (a_chirp("seen"), heard({"S1": hours_ago(1, 3, 5, 7, 9, 11)}), {}),
        (a_chirp("seen"), heard({"S1": hours_ago(49)}), {}),
        (a_chirp("candidate", since=NOW - 25 * H),
         heard({"S1": hours_ago(1)}, silent_hours={"N1": 6, "N2": 7}), {"station_pos": POS}),
        (a_chirp("probing"), established_ev(), {}),
        (a_chirp("probing", since=NOW - 29 * D), heard({"S1": hours_ago(1)}), {}),
        (a_chirp("established", since=NOW - 9 * D),
         heard({"S1": [NOW - 2 * D]}, covered_silent={"S1": 6, "N1": 7}), {}),
        (a_chirp("dormant", since=NOW - 5 * D), heard({"S1": [NOW - H]}), {}),
    ]
    for chirp, ev, kw in cases:
        m = next_state(chirp, ev, RULES, NOW, **kw)
        assert m is not None and m.to_state in STATES
        assert all(isinstance(k, str) and isinstance(v, str) for k, v in m.evidence.items())
        for key in ("n_hearers", "hearers", "hours", "covered_silent", "probe_stations"):
            assert key in m.evidence, (m, key)
    m = next_state(*cases[5][:2], RULES, NOW)
    assert m.evidence["covered_silent"] == "N1:7,S1:6"
