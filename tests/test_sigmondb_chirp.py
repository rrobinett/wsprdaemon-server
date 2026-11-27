"""Chirp rules file and signature matching.  No ClickHouse."""
import os
import sys

import pytest

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from sigmondb import chirp  # noqa: E402
from sigmondb.chirp import Signature, cluster_sweeps, group_key, load_rules, matches  # noqa: E402


def sig(centre=4.5e6, rate=-25730.0, period=1.0, span=None, dur=None):
    return Signature(centre, rate, period, span, dur)


def test_rules_load_starting_values():
    r = load_rules()
    assert r.rules_version == "chirp-rules-1"
    assert r.candidate.min_hours == 6
    assert r.probing.max_days == 28
    assert r.people.allowed == ("michael", "rob")
    assert r.menu.state_weight["probing"] == 0.8
    with pytest.raises(TypeError):
        r.menu.cost["codar"] = 2.0
    with pytest.raises(Exception):
        r.match.centre_tol_hz = 1


def test_rules_missing_key_names_it(tmp_path):
    text = chirp.RULES_PATH.read_text()
    p = tmp_path / "r.toml"
    p.write_text(text.replace("[retired]\nafter_days = 21\n", ""))
    with pytest.raises(ValueError, match="retired"):
        load_rules(p)
    p.write_text(text.replace("max_days = 28", ""))
    with pytest.raises(ValueError, match="probing.max_days"):
        load_rules(p)
    p.write_text(text.replace("after_days = 21", "after_days = 21\nbogus = 1"))
    with pytest.raises(ValueError, match="retired.bogus"):
        load_rules(p)


def test_rules_digest_follows_file_text(tmp_path):
    import hashlib
    r = load_rules()
    assert chirp.rules_digest(r) == hashlib.sha256(chirp.RULES_PATH.read_bytes()).hexdigest()
    p = tmp_path / "r.toml"
    p.write_text(chirp.RULES_PATH.read_text() + "\n# edited\n")
    assert chirp.rules_digest(load_rules(p)) != chirp.rules_digest(r)


def test_group_key_matches_hamsci_dsp():
    assert group_key(4.5372e6, -25730) == "4.5372 -25.73"
    assert group_key(16e6, 100000, 300, 280) == "16.0000 +100.00 300s sweep280s"


def test_match_tolerances():
    r = load_rules()
    base = sig()
    assert matches(base, sig(centre=4.5e6 + 2900), r)
    assert not matches(base, sig(centre=4.5e6 + 3100), r)
    assert not matches(base, sig(rate=+25730.0), r)
    assert not matches(base, sig(period=0.5), r)
    assert matches(base, sig(period=1.005), r)
    assert not matches(base, sig(period=1.02), r)
    assert matches(base, sig(rate=-25730.0 - 100), r)       # within the 500 Hz/s floor
    assert not matches(base, sig(rate=-25730.0 - 600), r)
    assert matches(base, sig(rate=-25730.0 * 1.004), r) is True


def test_zero_rate_rule():
    r = load_rules()
    assert matches(sig(rate=0.0), sig(rate=0.0), r)
    assert matches(sig(rate=0.0), sig(rate=300.0), r)       # zero agrees with either sign
    assert matches(sig(rate=0.0), sig(rate=-300.0), r)
    assert not matches(sig(rate=0.0), sig(rate=600.0), r)   # beyond the floor
    assert matches(sig(rate=300.0), sig(rate=0.0), r)       # symmetric


def test_wide_sweeps_match_by_overlap():
    r = load_rules()
    a = sig(centre=16e6, rate=100000.0, period=300, span=28e6)
    b = sig(centre=19e6, rate=100000.0, period=300, span=28e6)
    assert matches(a, b, r)
    far = sig(centre=40e6, rate=100000.0, period=300, span=28e6)
    assert not matches(a, far, r)
    # span None: the centre test applies
    assert not matches(sig(centre=16e6, rate=1e5, period=300, span=None),
                       sig(centre=19e6, rate=1e5, period=300, span=28e6), r)
    # a narrow sweep never takes the overlap rule
    assert not matches(sig(span=50e3), sig(centre=4.5e6 + 10e3, span=50e3), r)


def sweep(centre, rate=-25730.0, period=1.0, **kw):
    d = dict(centre_hz=centre, rate_hz_per_s=rate, period_s=period, span_hz=None, duration_s=None)
    d.update(kw)
    return d


def test_cluster_sweeps_single_linkage_and_order():
    r = load_rules()
    # 2.5 kHz steps chain: ends are 5 kHz apart but link through the middle one.
    s = [sweep(4.5e6 + 5000, id="c"), sweep(9e6, id="z"), sweep(4.5e6, id="a"),
         sweep(4.5e6 + 2500, id="b"), sweep(4.5e6, rate=+25730.0, id="n")]
    out = cluster_sweeps(s, r)
    assert [[m["id"] for m in c] for c in out] == [["a", "b", "c"], ["n"], ["z"]]
    # deterministic regardless of input order
    assert cluster_sweeps(list(reversed(s)), r) == out
    assert cluster_sweeps([], r) == []
