"""sigmondb HFDL back-fill: squitter-part1.csv reader.  No ClickHouse, no tasks/ imports."""
import csv
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
    for k, v in assumed_provenance("").items():
        assert a[k] == v


def test_unfound_and_excluded_count_but_write_nothing():
    caps, arrs = read_squitters(FX / "t6-group.csv", REP)
    assert len(_rows("t6-group.csv")) == 5
    assert len(arrs) == 2 and len(caps) == 1
    c = caps[0]
    assert set(c) == {k for k in ddl.COLUMNS["capture"]}
    assert c["n_expected"] == 5 and c["n_found"] == 2
    assert c["expected"] == {"hfdl:" + _rows("t6-group.csv")[0]["gs_id"]: 0.4}
    assert c["duration_s"] == 3600 and c["rate_hz_per_s"] == 0
    assert c["time"].minute == 0 and c["time"].second == 0 and c["time"].microsecond == 0
    assert c["time"] <= arrs[0]["time"]


def test_t5_rows_carry_null_provenance():
    caps, arrs = read_squitters(FX / "t5-group.csv", "AC0G-UACNJ")
    assert arrs and all(a[k] == v for a in arrs for k, v in null_provenance().items())
    assert all(c[k] == v for c in caps for k, v in null_provenance().items())


def test_empty_delay_counts_but_writes_no_arrival(tmp_path):
    rows = _rows("t6-group.csv")
    rows[0]["delay_counter_s"] = ""
    caps, arrs = read_squitters(_write(tmp_path, rows), REP)
    assert len(arrs) == 1
    assert caps[0]["n_expected"] == 5 and caps[0]["n_found"] == 1


def test_counter_epoch_id_from_counter_map(tmp_path):
    rows = _rows("t6-group.csv")
    rows[0]["counter_map"] = '{"counter_epoch_id": "ep7"}'
    _, arrs = read_squitters(_write(tmp_path, rows), REP)
    kept = [a for a in arrs if a["counter_epoch_id"] == "ep7"]
    assert len(kept) == 1


def test_missing_counter_map_is_null_even_on_t6(tmp_path):
    rows = _rows("t6-group.csv")
    for r in rows:
        r["counter_map"] = ""
    _, arrs = read_squitters(_write(tmp_path, rows), REP)
    assert arrs and all(a["origin"] == "" and a["u_epoch_ns"] is None for a in arrs)


def test_row_without_time_is_skipped(tmp_path):
    rows = _rows("t6-group.csv")
    rows.append(dict(rows[0], nominal_utc="", found="False"))
    caps, _ = read_squitters(_write(tmp_path, rows), REP)
    assert caps[0]["n_expected"] == 5


def test_sets_name_five_stations():
    assert sorted(s["station"] for s in SETS) == ["ac0g-nd", "b4", "k3lr", "uacnj", "wb6cxc"]
    assert all(s["reporter_id"] for s in SETS)
