"""sigmondb CODAR back-fill: tsv and res readers.  No ClickHouse, no tasks/ imports."""
import copy
import os
import sys
from datetime import datetime
from pathlib import Path

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from sigmondb import SCHEMA_VERSION, ddl  # noqa: E402
from sigmondb.backfill_codar import (  # noqa: E402
    SETS, read_res_jsonl, read_t6_tsv, reporter_for)
from sigmondb.provenance import (  # noqa: E402
    assumed_provenance, load_minutes, null_provenance, provenance_at)

FX = Path(__file__).parent / "fixtures" / "sigmondb" / "codar"
HEADER = "time\tlabel_minus_t6_ms\tanchor_age_s\tphase_t6_ms\tphase_label_ms\ttag\tdb\n"


def _tsv(tmp_path, rows, stamps):
    d = tmp_path / "cap"
    d.mkdir()
    for s in stamps:
        (d / f"K3LR-4463000-{s}.npy").write_bytes(b"")
    p = tmp_path / "k3lr-t6.tsv"
    p.write_text(HEADER + "".join("\t".join(map(str, r)) + "\n" for r in rows))
    return p, d


def _read(tsv, cap, minutes):
    return read_t6_tsv(tsv, cap, "K3LR/D", 4_463_000.0, -25_733.913, 1.0, minutes)


def test_midnight_rows_take_their_files_date(tmp_path):
    tsv, cap = _tsv(tmp_path,
                    [("23:59:51", 1.0, -3, 10.5, 11.5, "A", 40.0),
                     ("00:10:03", 1.0, -3, 20.5, 21.5, "A", 41.0)],
                    ["20261107T235951Z", "20261108T001003Z"])
    caps, arrs = _read(tsv, cap, [])
    assert [c["time"] for c in caps] == [datetime(2026, 11, 7, 23, 59, 51),
                                          datetime(2026, 11, 8, 0, 10, 3)]
    assert [a["time"] for a in arrs] == [datetime(2026, 11, 7, 23, 59, 51),
                                          datetime(2026, 11, 8, 0, 10, 3)]


def test_delay_is_integer_ns_mod_period(tmp_path):
    tsv, cap = _tsv(tmp_path, [("02:19:40", 1.9, -9, 1000.25, 1.0, "A", 46.0)],
                    ["20261107T021939Z"])
    _, arrs = _read(tsv, cap, [])
    assert arrs[0]["delay_ns"] == 250_000
    assert isinstance(arrs[0]["delay_ns"], int)


def test_no_anchor_gives_null_provenance(tmp_path):
    minutes = load_minutes([FX / "minutes-k3lr-1108.jsonl"])
    for m in minutes:
        m["t3_shadow"]["t6_anchor_utc_ns"] = None
    tsv, cap = _tsv(tmp_path, [("17:52:30", 1.0, -3, 10.5, 11.5, "A", 40.0)],
                    ["20261108T175229Z"])
    caps, arrs = _read(tsv, cap, minutes)
    assert len(arrs) == 1
    for r in caps + arrs:
        assert r["u_epoch_ns"] is None
        assert r["origin"] == ""
        assert r["counter_epoch_id"] == ""


def test_anchor_gives_assumed_provenance(tmp_path):
    minutes = load_minutes([FX / "minutes-k3lr-1107.jsonl"])
    tsv, cap = _tsv(tmp_path, [("00:00:01", 1.0, -3, 10.5, 11.5, "A", 40.0)],
                    ["20261107T000001Z"])
    caps, arrs = _read(tsv, cap, minutes)
    for r in caps + arrs:
        assert r["u_epoch_ns"] == 230.0
        assert r["k"] == 2.0
        assert r["a_level_provenance"] == "assumed"
        assert r["counter_epoch_id"] == "ep-1478032778"
        assert r["origin"] == "t6_anchor"


def test_provenance_at_respects_max_age():
    minutes = load_minutes([FX / "minutes-k3lr-1107.jsonl"])
    anchor = minutes[0]["t3_shadow"]["t6_anchor_utc_ns"] * 1e-9
    assert provenance_at(minutes, anchor + 100)["u_epoch_ns"] == 230.0
    assert provenance_at(minutes, anchor + 100_000) == null_provenance()
    assert provenance_at([], anchor) == null_provenance()


def test_eleven_oh_four_tsv_uses_anchor_age(tmp_path):
    tsv, cap = _tsv(tmp_path,
                    [("01:09:33", -7.0, 22, 17.1, 10.0, "A", 48.1),
                     ("01:19:45", -7.0, 3440, 17.1, 10.0, "A", 48.1)],
                    ["20261104T010932Z", "20261104T011944Z"])
    caps, arrs = _read(tsv, cap, [])
    assert arrs[0]["u_epoch_ns"] == 230.0 and arrs[0]["counter_epoch_id"] == ""
    assert arrs[0]["origin"] == "t6_anchor"
    assert arrs[1]["u_epoch_ns"] is None and arrs[1]["origin"] == ""
    assert [c["u_epoch_ns"] for c in caps] == [230.0, None]


def test_tsv_rows_carry_exactly_the_ddl_columns(tmp_path):
    tsv, cap = _tsv(tmp_path, [("02:19:40", 1.9, -9, 34.579, 36.48, "m", 46.0)],
                    ["20261107T021939Z"])
    caps, arrs = _read(tsv, cap, [])
    assert tuple(caps[0]) == ddl.COLUMNS["capture"]
    assert tuple(arrs[0]) == ddl.COLUMNS["arrival"]
    a = arrs[0]
    assert a["tx_id"] == "" and a["schema_version"] == SCHEMA_VERSION
    assert a["detector_version"] == "offline-2026-11"
    assert a["group_key"] == "4.4630 -25.73"
    assert a["clean_tag"] == "m" and a["level_db"] == 46.0
    assert caps[0]["n_found"] == 1 and caps[0]["n_sweeps"] == 30
    assert caps[0]["ring_ref"] == "K3LR-4463000-20261107T021939Z.npy"
    assert all(isinstance(v, str) for v in a["engineering"].values())


def test_tsv_distinct_times_make_distinct_captures(tmp_path):
    tsv, cap = _tsv(tmp_path,
                    [("02:19:40", 1.9, -9, 34.5, 36.4, "A", 46.0),
                     ("02:19:40", 1.9, -9, 47.5, 49.4, "A", 44.0),
                     ("02:29:52", 1.9, -9, 21.4, 23.3, "R", 44.0)],
                    ["20261107T021939Z", "20261107T022951Z"])
    caps, arrs = _read(tsv, cap, [])
    assert len(caps) == 2 and len(arrs) == 3
    assert caps[0]["n_found"] == 2


def test_res_row_fills_capture_expected():
    minutes = load_minutes([FX / "minutes-k3lr-1108.jsonl"])
    caps, arrs = read_res_jsonl(FX / "res-k3lr-1752.jsonl", "K3LR/D", minutes)
    assert len(caps) == 4
    fau = next(c for c in caps if c["centre_hz"] == 13_449_900.0)
    assert fau["rate_hz_per_s"] == -99_260.0
    assert abs(fau["expected"]["FAU 13.4499 -99.26"] - (14.01 - 9.69)) < 1e-4
    assert fau["null_max_db"] == 9.69 and fau["null_median_db"] == 5.12
    assert fau["n_found"] == 2 and fau["n_sweeps"] == 60
    fau_arr = [a for a in arrs if a["centre_hz"] == 13_449_900.0]
    assert len(fau_arr) == 2
    assert fau_arr[0]["delay_ns"] == 12_950_900
    assert fau_arr[0]["half_band_db"] == 19.3 - 9.0
    assert fau_arr[0]["group_key"] == "13.4499 -99.26"
    assert fau_arr[0]["time"] == datetime(2026, 11, 8, 17, 52, 30)
    assert tuple(caps[0]) == ddl.COLUMNS["capture"]
    assert tuple(arrs[0]) == ddl.COLUMNS["arrival"]
    assert fau_arr[0]["u_epoch_ns"] == 230.0
    assert fau_arr[0]["counter_epoch_id"] == "ep-1478032776"


def test_res_4463_group_uses_exact_rate():
    caps, arrs = read_res_jsonl(FX / "res-k3lr-4463.jsonl", "K3LR/D", [])
    rut = next(c for c in caps if c["rate_hz_per_s"] == -25_733.913)
    assert rut["centre_hz"] == 4_463_000.0 and rut["n_sweeps"] == 30
    assert any(c["rate_hz_per_s"] == -47_791.553 for c in caps)


def test_res_without_t6_keeps_arrivals_on_label_phase_with_null_provenance():
    minutes = load_minutes([FX / "minutes-k3lr-1108.jsonl"])
    caps, arrs = read_res_jsonl(FX / "res-uacnj-first.jsonl", "UACNJ", minutes)
    assert len(caps) == 4 and len(arrs) == 25
    assert all(c["u_epoch_ns"] is None and c["origin"] == "" for c in caps)
    for a in arrs:
        assert a["u_epoch_ns"] is None and a["origin"] == "" and a["counter_epoch_id"] == ""
        assert a["engineering"]["phase_basis"] == "label"
    assert sum(c["n_found"] for c in caps) == 25
    first = next(a for a in arrs if a["engineering"]["tau_ms"])
    assert first["delay_ns"] == round(float(first["engineering"]["phase_label_ms"]) * 1e6)


def test_res_t6_arrivals_carry_assumed_block_and_t6_basis():
    minutes = load_minutes([FX / "minutes-k3lr-1108.jsonl"])
    _, arrs = read_res_jsonl(FX / "res-k3lr-1752.jsonl", "K3LR/D", minutes)
    assert arrs and all(a["engineering"]["phase_basis"] == "t6" for a in arrs)
    assert all(a["u_epoch_ns"] == 230.0 and a["origin"] == "t6_anchor" for a in arrs)


def test_res_t6_row_arrival_without_t6_phase_is_label_basis(tmp_path):
    import json
    r = json.loads((FX / "res-k3lr-1752.jsonl").read_text())
    for a in r["groups"]["FAU 13.4499 -99.26"]["arrivals"]:
        del a["phase_t6_ms"]
    p = tmp_path / "r.jsonl"
    p.write_text(json.dumps(r) + "\n")
    minutes = load_minutes([FX / "minutes-k3lr-1108.jsonl"])
    _, arrs = read_res_jsonl(p, "K3LR/D", minutes)
    fau = [a for a in arrs if a["centre_hz"] == 13_449_900.0]
    assert fau and all(a["engineering"]["phase_basis"] == "label" and a["u_epoch_ns"] is None
                       for a in fau)


def test_tsv_same_clock_time_on_two_dates_locates_to_one_capture(tmp_path):
    # A tsv row carries a clock time and no date.  When capture files on two dates match it
    # equally, the first file in date order wins for every such row, so both rows land on
    # one capture.  (Known limit: the 11-08 row is filed under 11-07.)
    tsv, cap = _tsv(tmp_path,
                    [("02:19:40", 1.0, -3, 10.5, 11.5, "A", 40.0),
                     ("02:19:40", 1.0, -3, 20.5, 21.5, "A", 41.0)],
                    ["20261107T021939Z", "20261108T021939Z"])
    caps, arrs = _read(tsv, cap, [])
    assert len(caps) == 1 and caps[0]["n_found"] == 2
    assert caps[0]["time"] == datetime(2026, 11, 7, 2, 19, 40)
    assert caps[0]["ring_ref"] == "K3LR-4463000-20261107T021939Z.npy"
    assert [a["time"] for a in arrs] == [caps[0]["time"]] * 2


def test_clean_tags_map_to_the_brief_set():
    from sigmondb.backfill_codar import _clean_tag
    assert _clean_tag("") == "A"
    assert _clean_tag("on rung +2 of 12.3") == "R" and _clean_tag("rung -1") == "R"
    assert _clean_tag("main-lobe") == "m" and _clean_tag("main-lobe residue of 5.0") == "m"


def test_provenance_helpers():
    a = assumed_provenance("ep-9")
    assert (a["u_epoch_ns"], a["k"], a["a_level_provenance"], a["counter_epoch_id"], a["origin"]) \
        == (230.0, 2.0, "assumed", "ep-9", "t6_anchor")
    n = null_provenance()
    assert n == dict(u_epoch_ns=None, k=None, p=None, a_level="", a_level_provenance="",
                     counter_epoch_id="", origin="")


def test_sets_name_every_input_with_a_known_receiver():
    assert SETS
    for s in SETS:
        assert reporter_for(s["station"])
        assert s["kind"] in ("tsv", "res")
    assert {s["station"] for s in SETS if s["kind"] == "res"} == \
        {"k3lr", "uacnj", "w2naf", "ac0g-nd", "wb6cxc"}
    assert not any("day1107/" in s["path"] and s["kind"] == "tsv" for s in SETS)
