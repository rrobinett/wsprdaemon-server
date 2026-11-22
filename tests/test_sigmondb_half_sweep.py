"""codar-sounder rows older than 0.8.0 carry delay_ns = tau + S/2; the readers take S/2 off.
No ClickHouse: the readers run against test_sigmondb_cli's QueryClient."""
import os
import sys
from datetime import datetime, timedelta, timezone

import pytest

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from sigmondb import cli  # noqa: E402
from sigmondb import half_sweep as hs  # noqa: E402
from test_sigmondb_cli import RX_ROWS, TX, WF, _db, _ns  # noqa: E402
from test_sigmondb_load import valid_row  # noqa: E402

MS = 1_000_000
_T = datetime(2026, 11, 10, 3, tzinfo=timezone.utc)


def _row(gkey, dv, delay, t=_T, rid="W2NAF"):
    return dict(time=t, reporter_id=rid, group_key=gkey, detector_version=dv, delay_ns=delay)


# --- the group key --------------------------------------------------------------------------

@pytest.mark.parametrize("gkey, want", [
    ("4.4630 -25.73", (1.0, 1.0)),                      # continuous 1 s: no token, S = 1 s
    ("13.4750 -99.26 0.5s", (0.5, 0.5)),                # continuous, period token
    ("4.4630 -25.73 sweep0.9s", (1.0, 0.9)),            # gated in a 1 s period
    ("4.4630 -25.73 2s sweep1.5s", (2.0, 1.5)),         # gated, period token too
])
def test_period_and_sweep_follow_the_group_key(gkey, want):
    assert hs.period_and_sweep(gkey) == want


@pytest.mark.parametrize("gkey", ["", "g", "4.4630", "x -25.73", "4.4630 -25.73 xyz",
                                  "4.4630 -25.73 sweepXs", "4.4630 -25.73 abcs",
                                  "4.4630 -25.73 0s", "4.4630 -25.73 sweep0s"])
def test_a_key_that_cannot_give_s_gives_none(gkey):
    assert hs.period_and_sweep(gkey) is None


# --- the version gate -----------------------------------------------------------------------

@pytest.mark.parametrize("dv", ["codar-sounder/0.7.0", "codar-sounder/0.1.3",
                                "codar-sounder/0.7.9+dirty", "codar-sounder/0.7"])
def test_older_codar_sounder_needs_the_correction(dv):
    assert hs.needs_correction(dv)


@pytest.mark.parametrize("dv", ["codar-sounder/0.8.0", "codar-sounder/0.8.0+g1234",
                                "codar-sounder/0.8.1.dev3", "codar-sounder/1.0.0",
                                "offline-2026-11", "chirp-recorder/0.1.0", "", None,
                                "codar-sounder/", "codar-sounder/dev", "v"])
def test_everything_else_passes(dv):
    assert not hs.needs_correction(dv)


# --- the helper on rows ---------------------------------------------------------------------

@pytest.mark.parametrize("gkey, delay, want", [
    ("4.4630 -25.73", 700 * MS, 200 * MS),              # continuous 1 s: S/2 = 500 ms
    ("4.4630 -25.73", 100 * MS, 600 * MS),              # folds into [0, period)
    ("13.4750 -99.26 0.5s", 300 * MS, 50 * MS),         # 0.5 s period: S/2 = 250 ms
    ("13.4750 -99.26 0.5s", 100 * MS, 350 * MS),
    ("4.4630 -25.73 sweep0.9s", 600 * MS, 150 * MS),    # gated: S/2 = 450 ms, not P/2
    ("4.4630 -25.73 2s sweep1.5s", 1000 * MS, 250 * MS),
])
def test_an_old_row_comes_back_half_a_sweep_earlier(gkey, delay, want):
    assert hs.sweep_start_delay(_row(gkey, "codar-sounder/0.7.0", delay)) == want


@pytest.mark.parametrize("dv", ["codar-sounder/0.8.0", "codar-sounder/0.8.0+g1234",
                                "offline-2026-11", "chirp-recorder/0.1.0"])
def test_fixed_backfill_and_other_rows_pass_unchanged(dv):
    for gkey in ("4.4630 -25.73", "13.4750 -99.26 0.5s", "4.4630 -25.73 sweep0.9s", "g"):
        assert hs.sweep_start_delay(_row(gkey, dv, 700 * MS)) == 700 * MS


def test_an_old_row_whose_key_gives_no_s_has_no_delay():
    assert hs.sweep_start_delay(_row("4.4630 -25.73 xyz", "codar-sounder/0.7.0", 700 * MS)) is None


def test_to_sweep_start_leaves_out_rows_it_cannot_correct_and_says_so_once(capsys):
    rows = [_row("4.4630 -25.73", "codar-sounder/0.7.0", 700 * MS),
            _row("4.4630 -25.73 xyz", "codar-sounder/0.7.0", 1 * MS),
            _row("4.4630 -25.73 xyz", "codar-sounder/0.7.0", 2 * MS),
            _row("", "codar-sounder/0.6.1", 3 * MS),
            _row("4.4630 -25.73 xyz", "codar-sounder/0.8.0", 4 * MS),     # fixed: no S needed
            _row("4.4630 -25.73", "offline-2026-11", 5 * MS)]
    before = [dict(r) for r in rows]
    out = hs.to_sweep_start(rows, "solve")
    assert [r["delay_ns"] for r in out] == [200 * MS, 4 * MS, 5 * MS]
    assert rows == before                                   # inputs untouched
    err = capsys.readouterr().err
    assert err.count("\n") == 1                             # one line per call, not per row
    assert "solve: 3 codar-sounder" in err and "'4.4630 -25.73 xyz'" in err and "''" in err


def test_to_sweep_start_is_silent_when_every_row_corrects(capsys):
    hs.to_sweep_start([_row("4.4630 -25.73", "codar-sounder/0.7.0", 700 * MS)], "solve")
    assert capsys.readouterr().err == ""


# --- both readers ---------------------------------------------------------------------------

def _live(gkey, dv, delay, i, rid="W2NAF"):
    centre, rate = (float(x) for x in gkey.split()[:2])
    return valid_row("arrival", time=_T + timedelta(minutes=10 * i), reporter_id=rid,
                     group_key=gkey, centre_hz=centre * 1e6, rate_hz_per_s=rate * 1e3,
                     delay_ns=delay, level_db=20.0, clean_tag="A", u_epoch_ns=230.0,
                     detector_version=dv, engineering={"sweep_repetition_hz": "1"})


MIXED = [_live("4.4630 -25.73", "codar-sounder/0.7.0", 700 * MS, 0),
         _live("13.4750 -99.26 0.5s", "codar-sounder/0.7.0", 300 * MS, 1),
         _live("4.4630 -25.73 sweep0.9s", "codar-sounder/0.7.0", 600 * MS, 2),
         _live("4.4630 -25.73", "codar-sounder/0.8.0", 700 * MS, 3),
         _live("4.4630 -25.73", "codar-sounder/0.8.0+g1234", 710 * MS, 4),
         _live("4.4630 -25.73", "offline-2026-11", 720 * MS, 5),
         _live("4.4630 -25.73 xyz", "codar-sounder/0.7.0", 730 * MS, 6)]
WANT = [200 * MS, 50 * MS, 150 * MS, 700 * MS, 710 * MS, 720 * MS]


def test_from_db_corrects_old_rows(monkeypatch):
    _db(monkeypatch, {"arrival": MIXED, "receiver": RX_ROWS})
    arr, _, _ = cli._from_db(_ns())
    assert sorted(r["delay_ns"] for r in arr) == sorted(WANT)


def test_from_db_corrects_under_a_chosen_detector_version(monkeypatch):
    _db(monkeypatch, {"arrival": MIXED[:1], "receiver": RX_ROWS})
    arr, _, _ = cli._from_db(_ns("--detector-version", "codar-sounder/0.7.0"))
    assert [r["delay_ns"] for r in arr] == [200 * MS]


def test_attribution_inputs_correct_old_rows(monkeypatch):
    c = _db(monkeypatch, {"arrival": MIXED, "transmitter": TX, "waveform": WF,
                          "receiver": RX_ROWS})
    arr, _, _, _ = cli._attribution_inputs(c, 30.0)
    assert sorted(r["delay_ns"] for r in arr) == sorted(WANT)


def test_the_two_readers_agree(monkeypatch, capsys):
    c = _db(monkeypatch, {"arrival": MIXED, "transmitter": TX, "waveform": WF,
                          "receiver": RX_ROWS})
    solver, _, _ = cli._from_db(_ns())
    attr, _, _, _ = cli._attribution_inputs(c, 30.0)
    key = lambda r: (r["reporter_id"], r["group_key"], r["time"], r["delay_ns"])  # noqa: E731
    assert sorted(map(key, solver)) == sorted(map(key, attr))
    out = capsys.readouterr()
    assert "solve: 1 codar-sounder" in out.err and "attribute: 1 codar-sounder" in out.err

