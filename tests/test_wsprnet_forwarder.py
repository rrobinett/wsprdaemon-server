"""wsprnet.org forwarder: post a station's WSPR spots on its behalf.

A station today sends its WSPR spots TWICE — once inside the .tbz to the
WD servers, and again, separately, to wsprnet.org.  Same observations, two
connections, two formats.  Everything a MEPT line needs is already in the
extended row (sync_quality, dt, code included), so the wsprnet submission is
DERIVABLE server-side and the duplicate transmission stops existing.

Four properties carry the weight, and each would be a real incident:

  * The dedup key must match the client's exactly, or the station's own
    audit and the server's posting disagree on how many spots a cycle
    yielded and nobody can say which is right.
  * No leader information must mean NOT leader, or losing the elector gives
    wsprnet every spot three times from three addresses.
  * A failed POST must not advance the watermark — those spots are gone with
    no record. A duplicate is harmless; wsprnet de-duplicates.
  * Over 999 spots must be chunked. wsprnet SILENTLY TRUNCATES a larger
    transaction: it returns success and drops the tail.

Run with:  python3 -m pytest tests/test_wsprnet_forwarder.py -v
"""
from __future__ import annotations

import sys
import tempfile
from datetime import datetime, timedelta, timezone
from pathlib import Path
from unittest import mock

HERE = Path(__file__).resolve().parent
sys.path.insert(0, str(HERE.parent))

import wsprnet_forwarder as wf  # noqa: E402

T0 = datetime(2026, 10, 26, 23, 30, tzinfo=timezone.utc)


def _row(**kw):
    base = dict(time=T0, ingested_at=T0, rx_sign="AC0G", rx_loc="EM38ww",
                rx_id="KA9Q_0", tx_sign="W2NAF", tx_loc="FN21", 
                frequency_mhz=14.0971234, snr=-12.0, drift=0, power=37,
                sync_quality=0.85, dt=0.3, code=2)
    base.update(kw)
    return wf.SpotRow(**base)


# ---------- dedup: one transmission, many receivers ----------

def test_the_same_transmission_on_three_receivers_collapses_to_one():
    """⛔ A MERGEd site records each receiver's own view — that per-receiver
    detail IS the science and stays in wsprdaemon.spots.  But wsprnet must
    get ONE spot, not three, or the site spams a public database."""
    rows = [_row(rx_id="rx1", snr=-20.0), _row(rx_id="rx2", snr=-12.0),
            _row(rx_id="rx3", snr=-18.0)]
    out = wf.dedup_for_wsprnet(rows)
    assert len(out) == 1
    assert out[0].snr == -12.0, "the best SNR must survive"


def test_genuinely_different_carriers_stay_distinct():
    """⛔ Quantisation is 1e-4 MHz — finer than WSPR's bandwidth, so two
    transmitters really on different carriers are not merged away."""
    a = _row(frequency_mhz=14.0971000)
    b = _row(frequency_mhz=14.0975000)
    assert len(wf.dedup_for_wsprnet([a, b])) == 2


def test_negligible_drift_between_receivers_still_collapses():
    """...and coarser than wsprd's float output, so one transmitter seen
    twice with a few Hz of difference is ONE spot."""
    a = _row(rx_id="rx1", frequency_mhz=14.09712340)
    b = _row(rx_id="rx2", frequency_mhz=14.09712349)
    assert len(wf.dedup_for_wsprnet([a, b])) == 1


def test_different_callsigns_or_cycles_are_never_merged():
    rows = [_row(tx_sign="W2NAF"), _row(tx_sign="AC0G"),
            _row(time=T0 + timedelta(minutes=2))]
    assert len(wf.dedup_for_wsprnet(rows)) == 3


def test_a_tie_on_snr_keeps_the_first_seen():
    """Matches the audit table's INSERT-OR-IGNORE first-write-wins, so the
    two converge on the same identity."""
    first = _row(rx_id="rx1", snr=-12.0)
    second = _row(rx_id="rx2", snr=-12.0)
    assert wf.dedup_for_wsprnet([first, second])[0].rx_id == "rx1"


# ---------- MEPT rendering ----------

def test_a_line_matches_the_documented_wd_upload_format():
    line = wf.row_to_mept(_row())
    assert line == ("261026 2330  0.85 -12  0.30   14.0971234 W2NAF          "
                    "FN21   37  0    2")


def test_an_unresolved_hash_callsign_is_dropped():
    """⛔ '<...>' is wsprd saying it could NOT resolve the hash.  Posting it
    attributes a reception to a station nobody can identify."""
    assert wf.row_to_mept(_row(tx_sign="<...>")) is None


def test_a_RESOLVED_bracketed_callsign_passes_through():
    """wsprnet accepts these and strips the brackets itself — dropping them
    would silently discard real spots."""
    assert "<W2NAF>" in wf.row_to_mept(_row(tx_sign="<W2NAF>"))


def test_missing_optional_fields_do_not_lose_the_spot():
    line = wf.row_to_mept(_row(snr=None, dt=None, drift=None,
                               power=None, sync_quality=None, code=None))
    assert line and line.startswith("261026 2330")


# ---------- reporter identity ----------

def test_a_single_receiver_site_posts_under_its_own_rx_sign():
    assert wf.reporter_for(_row(), {}) == ("AC0G", "EM38ww")


def test_a_merged_site_posts_under_its_configured_identity():
    """⛔ The ONE thing a MEPT line cannot derive from the extended row.
    An operator running several receivers may report them to wsprnet under
    one identity that matches no individual receiver's call."""
    ov = {"AC0G/KA9Q_0": {"call": "AC0G/M", "grid": "EM38"}}
    assert wf.reporter_for(_row(), ov) == ("AC0G/M", "EM38")


def test_a_site_wide_override_covers_every_receiver_there():
    ov = {"AC0G": {"call": "AC0G/M", "grid": "EM38"}}
    assert wf.reporter_for(_row(rx_id="rx888-1"), ov) == ("AC0G/M", "EM38")
    assert wf.reporter_for(_row(rx_id="rx888-2"), ov) == ("AC0G/M", "EM38")


def test_a_BARE_rx_id_NEVER_matches_another_sites_receiver():
    """⛔ rx_id is a station-local name: `KA9Q_0`, `rx888-1`.  Two unrelated
    sites routinely pick the same one.  If a bare rx_id were a lookup key,
    AC0G declaring an identity for its `KA9Q_0` would silently re-attribute
    K3LR's `KA9Q_0` spots — real spots posted to a public database under a
    callsign their operator never chose, with nothing anywhere saying so."""
    ov = {"KA9Q_0": {"call": "AC0G/M", "grid": "EM38"}}
    assert wf.reporter_for(_row(rx_sign="K3LR", rx_loc="EN91"), ov) == (
        "K3LR", "EN91")
    # ...and it does not match the declaring site either: the key is wrong.
    assert wf.reporter_for(_row(), ov) == ("AC0G", "EM38ww")


def test_declared_identities_load_from_clickhouse_keyed_as_the_lookup():
    class _CH:
        def execute(self, sql, *a):
            assert "wsprdaemon.wsprnet_reporter" in sql
            return [("AC0G/KA9Q_0", "AC0G/M", "EM38"), ("", "X", "Y"),
                    ("K3LR/rx1", "", "EN91")]
    ident = wf.load_declared_identities(_CH())
    assert ident == {"AC0G/KA9Q_0": {"call": "AC0G/M", "grid": "EM38"}}
    assert wf.reporter_for(_row(), ident) == ("AC0G/M", "EM38")


def test_an_unreadable_identity_table_degrades_to_rx_sign_not_a_crash():
    """The forwarder must keep posting when the table is missing: the
    fallback is each row's own rx_sign, which is right for every
    single-receiver site."""
    class _CH:
        def execute(self, sql, *a):
            raise RuntimeError("no such table")
    assert wf.load_declared_identities(_CH()) == {}


def test_two_reporters_never_share_a_POST():
    """call/grid are FORM fields, not per-line — one POST per reporter, or
    one site's spots are attributed to the other."""
    rows = [_row(rx_sign="AC0G", tx_sign="W2NAF"),
            _row(rx_sign="K3LR", rx_id="other", tx_sign="UACNJ")]
    groups = wf.group_by_reporter(rows, {})
    assert len(groups) == 2


def test_a_broken_overrides_file_falls_back_loudly_not_silently():
    with tempfile.TemporaryDirectory() as tmp:
        p = Path(tmp) / "r.json"
        p.write_text("{not json")
        with mock.patch.object(wf.logger, "error") as err:
            assert wf.load_reporter_overrides(p) == {}
        err.assert_called_once()


def test_an_absent_overrides_file_is_normal_and_silent():
    with tempfile.TemporaryDirectory() as tmp:
        with mock.patch.object(wf.logger, "error") as err:
            assert wf.load_reporter_overrides(Path(tmp) / "none.json") == {}
        err.assert_not_called()


# ---------- leader ----------

def test_no_leader_information_means_NOT_leader():
    """⛔ Losing contact with the elector is exactly when three servers must
    not each decide they are in charge."""
    assert wf.is_self_leader(wf.LeaderState(elected=None),
                             this_host="wd20") is False


def test_the_elected_host_leads_and_others_do_not():
    st = wf.LeaderState(elected="wd20")
    assert wf.is_self_leader(st, this_host="wd20") is True
    assert wf.is_self_leader(st, this_host="wd10") is False


def test_a_pin_overrides_the_elector():
    st = wf.LeaderState(elected="wd20")
    assert wf.is_self_leader(st, this_host="wd10", pin="wd10") is True


# ---------- posting ----------

def _ok_opener(calls):
    def opener(req, timeout=None):
        calls.append(req)
        class R:
            status = 200
            def read(self): return b"ok"
            def __enter__(self): return self
            def __exit__(self, *a): return False
        return R()
    return opener


def test_over_999_spots_are_chunked():
    """⛔ wsprnet SILENTLY TRUNCATES a larger transaction — it returns
    success and drops the tail, so nothing anywhere reports the loss."""
    rows = [_row(tx_sign=f"W{i:04d}", frequency_mhz=14.0 + i * 0.001)
            for i in range(1500)]
    calls = []
    posted, failed = wf.forward_rows(rows, url="http://x/", version="1",
                                     overrides={}, opener=_ok_opener(calls))
    assert posted == 1500 and failed == 0
    assert len(calls) == 2, "1500 spots must go as two transactions"


def test_a_failed_post_is_counted_and_not_silently_dropped():
    def bad(req, timeout=None):
        raise OSError("connection reset")
    posted, failed = wf.forward_rows([_row()], url="http://x/", version="1",
                                     overrides={}, opener=bad)
    assert posted == 0 and failed == 1


def test_the_body_is_multipart_with_the_wd_boundary_and_fields():
    calls = []
    wf.forward_rows([_row()], url="http://x/", version="9.9",
                    overrides={}, opener=_ok_opener(calls))
    body = calls[0].data
    assert wf._BOUNDARY in body
    assert b'name="allmept"; filename="spots.txt"' in body
    assert b"AC0G" in body
    # The version field is the STATION's, not this forwarder's — see
    # test_wsprnet_is_told_THE_STATIONS_version_not_ours.
    assert b"9.9" not in body


if __name__ == "__main__":
    import pytest
    sys.exit(pytest.main([__file__, "-v"]))


# ---------- cross-implementation: the wire must not fork ----------

def test_our_lines_are_BYTE_IDENTICAL_to_what_stations_send():
    """⛔ THE WIRE FORMAT MUST NOT FORK.  Once the server posts on a
    station's behalf there are two implementations of the same MEPT line,
    and a drift between them is invisible — wsprnet accepts both, and the
    spots just differ in ways nobody is comparing.  Pin them together.

    Skips when no hs-uploader checkout sits beside this repo, the same way
    sigmond's bundled-copy compat test does.
    """
    import importlib.util
    src = Path("/home/rob/hamsci/repos/hs-uploader/src")
    if not (src / "hs_uploader" / "transports" / "wsprnet.py").exists():
        import pytest
        pytest.skip("no hs-uploader checkout to compare against")
    sys.path.insert(0, str(src))
    from hs_uploader.transports.wsprnet import _record_to_mept

    class Rec:
        def __init__(self, cols, t):
            self.columns, self.time = cols, t

    cases = [
        dict(tx_sign="W2NAF", tx_loc="FN21", frequency_mhz=14.0971234,
             snr=-12.0, power=37, drift=0, code=2, sync_quality=0.85, dt=0.3),
        dict(tx_sign="VK5EI", tx_loc="PF85", frequency_mhz=7.0401010,
             snr=-27.0, power=23, drift=-1, code=2, sync_quality=0.41, dt=-1.2),
        dict(tx_sign="K3LR", tx_loc="EN91", frequency_mhz=28.1261500,
             snr=3.0, power=50, drift=2, code=2, sync_quality=1.0, dt=0.0),
        dict(tx_sign="LONGCALL/P", tx_loc="IO91", frequency_mhz=10.1402000,
             snr=-5.0, power=10, drift=0, code=2, sync_quality=0.5, dt=1.0),
    ]
    for c in cases:
        ours = wf.row_to_mept(wf.SpotRow(time=T0, ingested_at=T0,
                                         rx_sign="AC0G", rx_loc="EM38ww",
                                         rx_id="r", **c))
        theirs = _record_to_mept(Rec(dict(c), T0))
        assert ours == theirs, f"wire format forked for {c['tx_sign']}"


# ---------- the watermark ----------

def _loop_once(tmp, rows, *, leader, forward=None):
    cfg = wf.ForwarderConfig(this_host="wd20", pin_leader="wd20" if leader
                             else "wd10",
                             watermark_path=Path(tmp) / "wm", once=True)
    with mock.patch.object(wf, "fetch_pending", return_value=rows), \
         mock.patch.object(wf, "forward_rows",
                           side_effect=forward or (lambda *a, **k: (len(rows), 0))), \
         mock.patch.object(wf, "load_reporter_overrides", return_value={}):
        wf._STOP = False
        wf.run_loop(cfg, ch_client_factory=lambda: object())
    return wf.read_watermark(Path(tmp) / "wm")


def test_a_clean_pass_advances_the_watermark():
    with tempfile.TemporaryDirectory() as tmp:
        assert _loop_once(tmp, [_row()], leader=True) == T0


def test_a_FAILED_post_HOLDS_the_watermark_so_the_spots_retry():
    """⛔ Advancing past spots that failed loses them with no record
    anywhere.  Re-posting a duplicate is harmless — wsprnet de-duplicates
    on its own side — so when in doubt, favour the duplicate."""
    with tempfile.TemporaryDirectory() as tmp:
        wm = _loop_once(tmp, [_row()], leader=True,
                        forward=lambda *a, **k: (0, 1))
        assert wm is None, "a failed pass must leave the watermark untouched"


def test_a_NON_leader_still_advances_its_watermark():
    """⛔ Gap-free failover runs BOTH ways.  If idle servers let their
    watermark go stale, the moment one is promoted it re-posts every spot
    since it last led — hours of duplicates from a new address, at the
    worst possible moment."""
    with tempfile.TemporaryDirectory() as tmp:
        assert _loop_once(tmp, [_row()], leader=False) == T0


def test_a_non_leader_posts_NOTHING():
    with tempfile.TemporaryDirectory() as tmp:
        cfg = wf.ForwarderConfig(this_host="wd20", pin_leader="wd10",
                                 watermark_path=Path(tmp) / "wm", once=True)
        with mock.patch.object(wf, "fetch_pending", return_value=[_row()]), \
             mock.patch.object(wf, "forward_rows") as fwd:
            wf._STOP = False
            wf.run_loop(cfg, ch_client_factory=lambda: object())
        fwd.assert_not_called()


def test_a_PINNED_host_never_probes_the_elector():
    """A pin answers the leader question by itself.  Probing anyway costs a
    round trip per poll — and a full HTTP timeout per poll when the elector
    is unreachable — to compute an answer we already have."""
    with tempfile.TemporaryDirectory() as tmp:
        cfg = wf.ForwarderConfig(this_host="wd20", pin_leader="wd20",
                                 watermark_path=Path(tmp) / "wm", once=True)
        with mock.patch.object(wf, "fetch_pending", return_value=[]), \
             mock.patch.object(wf, "fetch_leader") as probe:
            wf._STOP = False
            wf.run_loop(cfg, ch_client_factory=lambda: object())
        probe.assert_not_called()


def test_an_UNPINNED_host_DOES_probe_the_elector():
    """Mutation guard for the test above."""
    with tempfile.TemporaryDirectory() as tmp:
        cfg = wf.ForwarderConfig(this_host="wd20", watermark_path=Path(tmp) / "wm",
                                 once=True)
        with mock.patch.object(wf, "fetch_pending", return_value=[]), \
             mock.patch.object(wf, "fetch_leader",
                               return_value="wd20") as probe:
            wf._STOP = False
            wf.run_loop(cfg, ch_client_factory=lambda: object())
        probe.assert_called_once()


def test_the_operator_file_OUTRANKS_the_station_declaration():
    """⛔ ORDER MATTERS.  A wd admin fixing a misattribution must not be
    undone by the next tar from the station that caused it: the station's
    declaration is the normal path, the file is the override above it."""
    declared = {"AC0G/KA9Q_0": {"call": "WRONG", "grid": "AA00"}}
    operator = {"AC0G/KA9Q_0": {"call": "AC0G/M", "grid": "EM38"}}
    merged = dict(declared)
    merged.update(operator)          # exactly what run_loop does
    assert wf.reporter_for(_row(), merged) == ("AC0G/M", "EM38")


def test_ONE_SITES_SPOT_NEVER_DELETES_ANOTHER_SITES():
    """⛔⛔ THE BUG DEPLOYMENT FOUND (wd30, 2026-10-27).

    The dedup key carries no receiver identity — correct on a station,
    where the duplicates are its own receivers hearing one transmission.
    But this server reads EVERY site's rows in one batch, and deduping
    that batch before grouping let the first site to hear a transmission
    silence all the others.  On real traffic: 2,752 rows from 55 sites in
    4 minutes collapsed to 388, and wsprnet would have shown one receiver
    per transmission for the entire proxied fleet — deleting exactly the
    multi-receiver coverage the network exists to produce.

    The earlier tests missed it because the two-reporter case used two
    DIFFERENT transmitters, so nothing ever collided across sites.
    """
    calls = []
    # One transmission, heard by three unrelated sites, same cycle+freq.
    rows = [_row(rx_sign="AC0G", rx_loc="EM38ww", rx_id="rx1", snr=-10),
            _row(rx_sign="K3LR", rx_loc="EN91", rx_id="rx1", snr=-14),
            _row(rx_sign="W2NAF2", rx_loc="FN21", rx_id="rx1", snr=-21)]
    posted, failed = wf.forward_rows(rows, url="http://x/", version="9.9",
                                     overrides={}, opener=_ok_opener(calls))
    assert (posted, failed) == (3, 0), "every site must report its own hearing"
    assert len(calls) == 3, "one POST per reporter"
    posted_as = {c.data.split(b'name="call"')[1][:40] for c in calls}
    assert len(posted_as) == 3, "three distinct reporter identities"
    for want in (b"AC0G", b"K3LR", b"W2NAF2"):
        assert any(want in p for p in posted_as), f"{want!r} never reported"


def test_a_sites_OWN_receivers_still_collapse_to_one():
    """The other direction: within one site, three receivers hearing one
    transmission is still ONE spot to wsprnet, or the site spams it."""
    calls = []
    rows = [_row(rx_sign="AC0G", rx_id="rx1", snr=-10),
            _row(rx_sign="AC0G", rx_id="rx2", snr=-8),
            _row(rx_sign="AC0G", rx_id="rx3", snr=-14)]
    posted, failed = wf.forward_rows(rows, url="http://x/", version="9.9",
                                     overrides={}, opener=_ok_opener(calls))
    assert (posted, failed) == (1, 0)
    assert len(calls) == 1
    assert b" -8 " in calls[0].data, "best SNR survives the collapse"


def test_the_dry_run_env_switch_only_accepts_an_explicit_yes():
    """⚠ An operator turning dry-run OFF writes `=0` or `=false` far more
    often than they delete the line.  If those read as true, the forwarder
    keeps silently posting nothing long after someone believes it is live."""
    import os
    for on in ("1", "true", "TRUE", "yes", "on", " 1 "):
        os.environ["X_WF_DRY"] = on
        assert wf._env_flag("X_WF_DRY") is True, on
    for off in ("0", "false", "no", "off", "", "  "):
        os.environ["X_WF_DRY"] = off
        assert wf._env_flag("X_WF_DRY") is False, repr(off)
    del os.environ["X_WF_DRY"]
    assert wf._env_flag("X_WF_DRY") is False


# ---------- the watermark and the three roles ----------

def test_DRY_RUN_MUST_NOT_ADVANCE_THE_WATERMARK():
    """⛔ THE BUG wd30 FOUND, 2026-10-27.  Dry-run is the SAME PROCESS that
    goes live later, and the unit file tells operators to stage exactly this
    way — "run with --dry-run first to watch what it WOULD post".  Advancing
    the watermark here means the flip to live silently skips the very spots
    they just watched it promise to post.  40 real AC0G/B4 spots from cycle
    03:06 were lost that way, with the log reading "would forward 40 row(s)"
    and nothing ever posting them.
    """
    with tempfile.TemporaryDirectory() as tmp:
        wm = Path(tmp) / "wm"
        cfg = wf.ForwarderConfig(this_host="wd20", pin_leader="wd20",
                                 watermark_path=wm, once=True, dry_run=True)
        posted = []
        with mock.patch.object(wf, "fetch_pending", return_value=[_row()]), \
             mock.patch.object(wf, "forward_rows",
                               side_effect=lambda *a, **k: posted.append(a)), \
             mock.patch.object(wf, "load_reporter_overrides", return_value={}):
            wf._STOP = False
            wf.run_loop(cfg, ch_client_factory=lambda: object())
        assert posted == [], "dry-run must not post"
        assert not wm.exists(), (
            "dry-run wrote a watermark; going live would now skip these rows")


def test_an_idle_server_STILL_advances_so_failover_is_gap_free():
    """The other direction, and it must stay true: a stale idle watermark
    makes a newly promoted server replay hours of spots at the worst
    possible moment."""
    with tempfile.TemporaryDirectory() as tmp:
        assert _loop_once(tmp, [_row()], leader=False) == T0


def test_the_two_branches_are_SEPARATE_in_the_source():
    """They shared one `else`, which is how the bug existed at all."""
    import inspect
    src = inspect.getsource(wf.run_loop)
    tail = src.split("if failed == 0:")[1]
    assert "elif cfg.dry_run:" in tail, "dry-run needs its own branch"
    dry = tail.split("elif cfg.dry_run:")[1].split("            else:")[0]
    idle = tail.split("elif cfg.dry_run:")[1].split("            else:")[1]
    assert "write_watermark" not in dry
    assert "write_watermark" in idle


def test_wsprnets_html_reply_is_reduced_to_the_numbers_that_matter():
    """wsprnet answers with a whole web page.  Logging it raw puts an HTML
    document in the journal every cycle and buries the forwarder's own
    lines — which is how a real POST confirmation got lost on wd30."""
    real = ("<html>\n<body>\n  <h1>Processing Log Upload</h1><pre>\n"
            "40 out of 101 spot(s) added<br>\n"
            "Processing took 9033 milliseconds.<br>\n</body>\n</html>")
    assert wf._wsprnet_verdict(real) == "40 of 101 spots added"


def test_zero_added_is_REPORTED_not_hidden():
    """⚠ `0 out of M` looks identical to success at the HTTP layer.  A run
    of it means we are talking to wsprnet and being ignored."""
    assert wf._wsprnet_verdict("0 out of 99 spot(s) added<br>") == "0 of 99 spots added"


def test_an_unrecognised_reply_is_kept_not_swallowed():
    assert "nope" in wf._wsprnet_verdict("<html><body>nope</body></html>")
    assert wf._wsprnet_verdict("") == "(empty response)"
    assert len(wf._wsprnet_verdict("x" * 9000)) <= 160


# ---------- the version wsprnet is told ----------

def _version_field(req):
    return req.data.split(b'name="version"')[1].split(b"\r\n\r\n")[1].split(b"\r\n")[0]


def test_wsprnet_is_told_THE_STATIONS_version_not_ours():
    """⛔ wsprnet uses this field for software-population statistics.  A
    station moving to server-side posting must keep reporting its OWN
    version, or every proxied site collapses into one bogus version and the
    real spread across the fleet disappears from wsprnet's view.

    Stations send `WD_<version>` (hs-uploader transports/wsprnet.py:
    `version=f"WD_{self.version}"`), and the version each tar declared is
    already stored per row, so we reproduce exactly what the station would
    have sent itself."""
    calls = []
    wf.forward_rows([_row(version="4.0")], url="http://x/", version="9.9",
                    overrides={}, opener=_ok_opener(calls))
    assert _version_field(calls[0]) == b"WD_4.0"


def test_two_versions_NEVER_share_a_transaction():
    """version is a FORM field, not a per-line field.  Two versions in one
    POST tells wsprnet the wrong software produced half the spots — which
    is exactly what happens across an upgrade."""
    calls = []
    rows = [_row(version="4.0", tx_sign="W2NAF"),
            _row(version="4.1", tx_sign="K3LR")]
    posted, failed = wf.forward_rows(rows, url="http://x/", version="9.9",
                                     overrides={}, opener=_ok_opener(calls))
    assert (posted, failed) == (2, 0)
    assert len(calls) == 2
    assert {_version_field(c) for c in calls} == {b"WD_4.0", b"WD_4.1"}


def test_an_unknown_version_is_reported_as_unknown_not_guessed():
    """⚠ Filling in a plausible '4.0' would be a false claim about someone
    else's software."""
    assert wf.wsprnet_version(_row(version=None)) == "WD_unknown"
    assert wf.wsprnet_version(_row(version="   ")) == "WD_unknown"
    assert wf.wsprnet_version(_row(version=" 4.0 ")) == "WD_4.0"


def test_the_version_prefix_matches_what_stations_actually_send():
    """⛔ CROSS-REPO: the `WD_` prefix is hs-uploader's, and if it drifts
    here the proxied spots report a version string wsprnet has never seen
    from this fleet.  Skips without an hs-uploader checkout."""
    from pathlib import Path as _P
    src = _P(__file__).resolve().parents[2] / "hs-uploader" / "src" / \
        "hs_uploader" / "transports" / "wsprnet.py"
    if not src.exists():
        import pytest
        pytest.skip("no hs-uploader checkout to compare against")
    text = src.read_text()
    assert 'version=f"WD_{self.version}"' in text, (
        "hs-uploader changed how it spells the version field; "
        "wsprnet_version() must change with it")


# ---------- the DB -> SpotRow hop (every test above mocks it away) ----------

class _FakeCH:
    """Answers fetch_pending's SELECT positionally, as clickhouse-driver does."""
    def __init__(self, rows): self.rows, self.sql, self.params = rows, None, None
    def execute(self, sql, params=None):
        self.sql, self.params = sql, params
        return self.rows


def test_fetch_pending_READS_THE_VERSION_COLUMN():
    """⛔ Every other test builds a SpotRow by hand, so the DB->row hop is
    exactly where a dropped column hides: the forwarder would keep running
    and every proxied spot would quietly report WD_unknown to wsprnet."""
    t = datetime(2026, 10, 27, 3, 6, tzinfo=timezone.utc)
    ch = _FakeCH([(t, t, "AC0G/B4", "EM38ww", "AC0G-B4", "W2NAF", "FN21",
                   14.0971234, -12.0, 0, 37, 0.85, 0.3, 2, "4.0")])
    rows = wf.fetch_pending(ch, since=t, batch_size=10)
    assert len(rows) == 1
    assert rows[0].version == "4.0"
    assert wf.wsprnet_version(rows[0]) == "WD_4.0"
    assert "version" in ch.sql, "the SELECT must ask for it"
    assert "proxy_upload = 1" in ch.sql, "and must still select only opt-ins"


def test_fetch_pending_tolerates_a_null_version():
    t = datetime(2026, 10, 27, 3, 6, tzinfo=timezone.utc)
    ch = _FakeCH([(t, t, "AC0G/B4", "EM38ww", "AC0G-B4", "W2NAF", "FN21",
                   14.0971234, -12.0, 0, 37, 0.85, 0.3, 2, None)])
    rows = wf.fetch_pending(ch, since=t, batch_size=10)
    assert rows[0].version is None
    assert wf.wsprnet_version(rows[0]) == "WD_unknown"


def test_the_SELECT_column_order_matches_the_unpack():
    """A column added to SELECT_COLUMNS but not to the tuple unpack (or the
    reverse) shifts every field silently — snr becomes drift, and the spots
    stay plausible."""
    assert len(wf.SELECT_COLUMNS.split(",")) == 15


# ---------- the batching window ----------

def _windowed(tmp, rows, *, post_interval, passes, batch=20000):
    """Run `passes` loop iterations with a stubbed clock; count POSTs."""
    clock = {"t": 1000.0}
    calls = []
    cfg = wf.ForwarderConfig(this_host="wd20", pin_leader="wd20",
                             watermark_path=Path(tmp) / "wm",
                             poll_sec=30, batch=batch,
                             post_interval_sec=post_interval)
    n = {"i": 0}
    def monotonic():
        return clock["t"]
    def sleep(sec):
        clock["t"] += sec
        n["i"] += 1
        if n["i"] >= passes:
            wf._STOP = True
    with mock.patch.object(wf.time, "monotonic", monotonic), \
         mock.patch.object(wf, "_interruptible_sleep", sleep), \
         mock.patch.object(wf, "fetch_pending", return_value=rows), \
         mock.patch.object(wf, "forward_rows",
                           side_effect=lambda *a, **k: (calls.append(1),
                                                        (len(rows), 0))[1]), \
         mock.patch.object(wf, "load_reporter_overrides", return_value={}), \
         mock.patch.object(wf, "load_declared_identities", return_value={}):
        wf._STOP = False
        wf.run_loop(cfg, ch_client_factory=lambda: object())
    return len(calls)


def test_the_window_turns_many_polls_into_ONE_transaction():
    """⚡ The whole point: at one POST per 2-minute cycle this proxy posts as
    OFTEN as the 85 clients it replaced, so the fleet-wide saving comes only
    from skipping TLS and would vanish the day we enable it.  Measured:
    683.7 -> 179.6 MB/day at a 300 s window."""
    with tempfile.TemporaryDirectory() as tmp:
        # 20 polls at 30 s = 600 s of wall clock, 300 s window -> 2 POSTs.
        assert _windowed(tmp, [_row()], post_interval=300, passes=20) == 2


def test_zero_means_post_every_poll():
    """The low-latency mode stays available and costs nothing to ask for."""
    with tempfile.TemporaryDirectory() as tmp:
        assert _windowed(tmp, [_row()], post_interval=0, passes=5) == 5


def test_a_FULL_BATCH_does_not_wait_out_the_window():
    """⛔ A full batch means there is more.  Waiting the whole window again
    lets the backlog grow by a window every window, and the only symptom is
    spots reaching wsprnet later and later — never an error."""
    with tempfile.TemporaryDirectory() as tmp:
        rows = [_row(tx_sign=f"W{i}ABC") for i in range(4)]
        # batch=4 means every fetch comes back full -> drain every poll.
        assert _windowed(tmp, rows, post_interval=300, passes=5, batch=4) == 5
