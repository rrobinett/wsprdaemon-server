"""sigmondb HFDL back-fill: squitter-part1.csv reader.  No ClickHouse, no tasks/ imports."""
import csv
import json
import os
import sys
from datetime import datetime, timedelta
from pathlib import Path

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from sigmondb import SCHEMA_VERSION, ddl  # noqa: E402
from sigmondb.backfill_hfdl import SETS, read_squitters  # noqa: E402
from sigmondb.provenance import assumed_provenance, null_provenance  # noqa: E402

FX = Path(__file__).parent / "fixtures" / "sigmondb" / "hfdl"
REP = "AC0G-B4"


def _rows(name):
    with open(FX / name, newline="") as fh:
        return list(csv.DictReader(fh))


def _write(tmp_path, rows):
    p = tmp_path / "squitter-part1.csv"
    with open(p, "w", newline="") as fh:
        w = csv.DictWriter(fh, list(rows[0].keys()))
        w.writeheader()
        w.writerows(rows)
    return p


def test_found_rows_become_arrivals():
    caps, arrs = read_squitters(FX / "t6-group.csv", REP)
    src = [r for r in _rows("t6-group.csv") if r["found"] == "True" and not r["excluded"]]
    assert len(arrs) == len(src) == 2
    a, r = arrs[0], src[0]
    assert set(a) == {c for c in ddl.COLUMNS["arrival"]}
    assert a["tx_id"] == f"hfdl:{r['gs_id']}"
    assert a["group_key"] == f"hfdl {r['freq_khz']} kHz"
    assert a["centre_hz"] == float(r["freq_khz"]) * 1e3 and a["rate_hz_per_s"] == 0
    assert abs(a["time"] - datetime(1970, 1, 1) - timedelta(seconds=float(r["nominal_utc"]))) < timedelta(microseconds=2)
    assert a["delay_ns"] == round(float(r["delay_counter_s"]) * 1e9)
    assert a["level_db"] == float(r["snr_db"])
    assert a["clean_tag"] == "squitter"
    assert a["detector_version"] == f"hfdl-analyzer-{r['analyzer_version']}"
    assert a["schema_version"] == SCHEMA_VERSION and a["reporter_id"] == REP
    for k in ("frame_index", "flags", "second_peak_ratio", "t_level_active"):
        assert a["engineering"][k] == r[k]
    assert all(isinstance(v, str) for v in a["engineering"].values())
    cm = json.loads(r["counter_map"])
    for k, v in assumed_provenance(f"ep-{int(cm['input_epoch_gps_s'])}").items():
        assert a[k] == v
    assert a["engineering"]["delay_basis"] == "counter"


def test_unfound_and_excluded_count_but_write_nothing():
    caps, arrs = read_squitters(FX / "t6-group.csv", REP)
    assert len(_rows("t6-group.csv")) == 5
    assert len(arrs) == 2 and len(caps) == 1
    c = caps[0]
    assert set(c) == {k for k in ddl.COLUMNS["capture"]}
    assert c["n_expected"] == 5 and c["n_found"] == 2
    assert c["expected"] == {"hfdl:" + _rows("t6-group.csv")[0]["gs_id"]: 0.4}
    gs = _rows("t6-group.csv")[0]["gs_id"]
    assert c["engineering"][f"n_rows:hfdl:{gs}"] == "5" and c["engineering"][f"n_found:hfdl:{gs}"] == "2"
    assert c["duration_s"] == 3600 and c["rate_hz_per_s"] == 0
    assert c["time"].minute == 0 and c["time"].second == 0 and c["time"].microsecond == 0
    assert c["time"] <= arrs[0]["time"]


def test_t5_rows_carry_null_provenance():
    caps, arrs = read_squitters(FX / "t5-group.csv", "AC0G-UACNJ")
    assert arrs and all(a[k] == v for a in arrs for k, v in null_provenance().items())
    assert all(c[k] == v for c in caps for k, v in null_provenance().items())


def test_label_only_row_lands_on_delay_s_with_null_provenance(tmp_path):
    rows = _rows("t6-group.csv")
    kept = next(r for r in rows if r["found"] == "True" and not r["excluded"])
    kept["delay_counter_s"] = ""
    kept["delay_s"] = "0.0123"
    caps, arrs = read_squitters(_write(tmp_path, rows), REP)
    assert len(arrs) == 2 and caps[0]["n_found"] == 2
    lab = [a for a in arrs if a["engineering"]["delay_basis"] == "label"]
    assert len(lab) == 1 and lab[0]["delay_ns"] == 12_300_000
    assert all(lab[0][k] == v for k, v in null_provenance().items())
    (cnt,) = [a for a in arrs if a["engineering"]["delay_basis"] == "counter"]
    assert cnt["origin"] == "t6_anchor" and cnt["a_level_provenance"] == "assumed"


def test_row_with_no_delay_at_all_writes_nothing(tmp_path, caplog):
    rows = _rows("t6-group.csv")
    kept = next(r for r in rows if r["found"] == "True" and not r["excluded"])
    kept["delay_counter_s"] = kept["delay_s"] = ""
    with caplog.at_level("WARNING"):
        caps, arrs = read_squitters(_write(tmp_path, rows), REP)
    assert len(arrs) == 1 and caps[0]["n_found"] == 1 and caps[0]["n_expected"] == 5
    assert "neither delay_counter_s nor delay_s" in caplog.text


def test_counter_epoch_id_derives_from_input_epoch(tmp_path):
    rows = _rows("t6-group.csv")
    cm = json.loads(next(r for r in rows if r["counter_map"])["counter_map"])
    want = f"ep-{int(cm['input_epoch_gps_s'])}"
    _, arrs = read_squitters(_write(tmp_path, rows), REP)
    assert arrs and all(a["counter_epoch_id"] == want for a in arrs)
    for r in rows:
        r["counter_map"] = '{"olen": 2000}'
    _, arrs = read_squitters(_write(tmp_path, rows), REP)
    assert all(a["counter_epoch_id"] == "" and a["origin"] == "t6_anchor" for a in arrs)


def test_capture_engineering_counts_rows_and_anchored():
    caps, _ = read_squitters(FX / "t6-group.csv", REP)
    assert caps[0]["engineering"]["n_rows"] == "5" and caps[0]["engineering"]["n_anchored"] == "5"
    caps, _ = read_squitters(FX / "t5-group.csv", "AC0G-UACNJ")
    assert caps[0]["engineering"]["n_anchored"] == "0"


def test_missing_counter_map_is_null_even_on_t6(tmp_path):
    rows = _rows("t6-group.csv")
    for r in rows:
        r["counter_map"] = ""
    _, arrs = read_squitters(_write(tmp_path, rows), REP)
    assert arrs and all(a["origin"] == "" and a["u_epoch_ns"] is None for a in arrs)


def test_row_without_time_is_skipped_and_logged(tmp_path, caplog):
    rows = _rows("t6-group.csv")
    rows.append(dict(rows[0], nominal_utc="", found="False", window_file="bad-1.npz"))
    with caplog.at_level("WARNING"):
        caps, _ = read_squitters(_write(tmp_path, rows), REP)
    assert caps[0]["n_expected"] == 5
    assert "1 rows without nominal_utc" in caplog.text and "bad-1.npz" in caplog.text


def test_sets_name_five_stations():
    assert sorted(s["station"] for s in SETS) == ["ac0g-nd", "b4", "k3lr", "uacnj", "wb6cxc"]
    assert all(s["reporter_id"] for s in SETS)


def _two_gs(tmp_path):
    rows = _rows("t6-group.csv")
    for r in rows:
        r["freq_khz"] = "13276"
    for r in rows[:2]:
        r["gs_id"], r["gs_name"] = "1", "San Francisco, California"
    for r in rows[2:]:
        r["gs_id"], r["gs_name"] = "4", "Riverhead, New York"
    return rows, _write(tmp_path, rows)


def test_two_ground_stations_on_one_frequency_make_one_capture(tmp_path):
    rows, p = _two_gs(tmp_path)
    caps, arrs = read_squitters(p, REP)
    assert len(caps) == 1
    c = caps[0]
    found = lambda rs: sum(r["found"] == "True" and not r["excluded"] for r in rs)
    assert c["expected"] == {"hfdl:1": found(rows[:2]) / 2, "hfdl:4": found(rows[2:]) / 3}
    assert c["n_expected"] == 5 and c["n_found"] == found(rows) == len(arrs)
    e = c["engineering"]
    assert e["n_rows"] == "5" and e["n_anchored"] == "5"
    assert (e["n_rows:hfdl:1"], e["n_rows:hfdl:4"]) == ("2", "3")
    assert int(e["n_found:hfdl:1"]) + int(e["n_found:hfdl:4"]) == c["n_found"]
    assert c["origin"] == "t6_anchor"
    assert {a["tx_id"] for a in arrs} <= {"hfdl:1", "hfdl:4"}


def test_real_nd_captures_have_unique_table_keys():
    root = Path(__file__).resolve().parents[3]
    path = next(s["path"] for s in SETS if s["station"] == "ac0g-nd")
    csv_path = root / path
    if not csv_path.exists():
        import pytest
        pytest.skip("no AC0G/ND squitter file")
    caps, _ = read_squitters(csv_path, "AC0G-ND")
    keys = [(c["reporter_id"], c["centre_hz"], c["rate_hz_per_s"], c["time"], c["detector_version"])
            for c in caps]
    assert caps and len(keys) == len(set(keys))
