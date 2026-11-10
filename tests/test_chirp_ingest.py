"""wd30's tar ingester parses chirp/ into sigmondb rows, behind a gate."""
import io
import json
import os
import sys
import tarfile
import types as _types
from datetime import datetime
from pathlib import Path

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
for _mod in ('clickhouse_connect', 'clickhouse_driver'):
    if _mod not in sys.modules:
        _stub = _types.ModuleType(_mod)
        _stub.Client = object
        _stub.get_client = lambda **kw: None
        sys.modules[_mod] = _stub

import wsprdaemon_server as ws  # noqa: E402
from sigmondb import ddl  # noqa: E402
from sigmondb.ingest import insert_sql  # noqa: E402

MTIME = 1794340800
A = "chirp/AC0G=B4_EM38ww/AC0G_B4/arrival.jsonl"
C = "chirp/AC0G=B4_EM38ww/AC0G_B4/capture.jsonl"
SPOT_PATH = "wspr/spots/AC0G=ND_EN16ov/KA9Q_DXE/20/260518_1230_spots.txt"
SPOT_LINE = (
    b'260518 1230 5 -12 0.3 14.097150 K1ABC FN42 33 0 '
    b'1 50 4500 100 0 1 0 1 -125.5 -124.0 '
    b'20 EN16ov AC0G/ND 2000 270.5 41.0 -71.0 90.5 41.0 -73.0 '
    b'0.0 0.0 0 1\n')
ARR = dict(
    time="2026-11-10T17:46:58.461539Z", reporter_id="AC0G/B4", tx_id="hfdl:4", group_key="g",
    centre_hz=8.9e6, rate_hz_per_s=0.0, delay_ns=12345, delay_sigma_ns=None, level_db=-90.0,
    margin_db=None, half_band_db=None, clean_tag="", doppler_hz=None, phase_rate=None,
    carrier_phase_rad=None, s4=None, sigma_phi_rad=None, detector_version="v1",
    schema_version=1, engineering={"delay_basis": "counter"})
ARR2 = dict(ARR, time="2026-11-10T17:47:58Z", delay_ns=999)
CAP = dict(
    time="2026-11-10T17:00:00.000Z", reporter_id="AC0G/B4", centre_hz=8.9e6, rate_hz_per_s=0.0,
    duration_s=3600.0, n_sweeps=None, expected={"hfdl:4": 1}, null_max_db=None,
    null_median_db=None, n_expected=1, n_found=1, detector_version="v1", ring_ref="",
    schema_version=1, engineering={})


def idx(c, t="arrival"):
    return ddl.COLUMNS[t].index(c)


def _jsonl(items):
    return b"".join(json.dumps(i).encode() + b"\n" for i in items)


def _tar(tmp, members):
    p = tmp / "t.tbz"
    with tarfile.open(p, "w:bz2") as tf:
        for name, data in members.items():
            ti = tarfile.TarInfo(name)
            ti.size = len(data)
            tf.addfile(ti, io.BytesIO(data))
    os.utime(p, (MTIME, MTIME))
    return p


def test_hand_made_tar_yields_rows_with_tar_mtime(tmp_path):
    r = ws.extract_and_parse_tbz(_tar(tmp_path, {A: _jsonl([ARR, ARR2]), C: _jsonl([CAP])}),
                                 tmp_path / "x", None, None, "chirp")
    assert len(r) == 6 and (len(r[5]["arrival"]), len(r[5]["capture"])) == (2, 1)
    assert r[5]["arrival"][0][idx("received_at")] == datetime.utcfromtimestamp(MTIME)


def test_gate_off_reads_no_chirp(tmp_path):
    r = ws.extract_and_parse_tbz(_tar(tmp_path, {A: _jsonl([ARR])}), tmp_path / "x", None, None, None)
    assert len(r) == 6 and r[5] == {}


def test_bad_chirp_never_costs_spots(tmp_path):
    r = ws.extract_and_parse_tbz(_tar(tmp_path, {SPOT_PATH: SPOT_LINE, A: b"\x00garbage\n"}),
                                 tmp_path / "x", None, None, "chirp")
    assert len(r[1]) == 1 and r[5].get("arrival", []) == []


def test_chirp_parse_exception_never_costs_spots(tmp_path, monkeypatch):
    def boom(*a, **k):
        raise RuntimeError("boom")
    monkeypatch.setattr(ws, "process_chirp_files", boom)
    r = ws.extract_and_parse_tbz(_tar(tmp_path, {SPOT_PATH: SPOT_LINE, A: _jsonl([ARR])}),
                                 tmp_path / "x", None, None, "chirp")
    assert len(r[1]) == 1 and r[5] == {}


def test_unknown_table_and_wrong_depth_skipped(tmp_path):
    r = ws.extract_and_parse_tbz(
        _tar(tmp_path, {"chirp/S/R/path.jsonl": _jsonl([ARR]), "chirp/S/arrival.jsonl": _jsonl([ARR])}),
        tmp_path / "x", None, None, "chirp")
    assert not any(r[5].values())


def test_reporter_falls_back_to_the_tar_site(tmp_path):
    r = ws.extract_and_parse_tbz(_tar(tmp_path, {A: _jsonl([dict(ARR, reporter_id="")])}),
                                 tmp_path / "x", None, None, "chirp")
    assert r[5]["arrival"][0][idx("reporter_id")] == "AC0G/B4"


def test_insert_chirp_rows_per_table_and_false_on_error():
    calls = []

    class Fake:
        def execute(self, sql, rows):
            calls.append((sql, len(rows)))

    rows = {"arrival": [(1,)] * 3, "capture": [(2,)]}
    assert ws.insert_chirp_rows(Fake(), rows, 2) is True
    assert [c[0] for c in calls].count(insert_sql("arrival")) == 2   # 3 rows, batch 2
    assert calls[-1] == (insert_sql("capture"), 1)

    class Bad:
        def execute(self, sql, rows):
            raise RuntimeError("down")
    assert ws.insert_chirp_rows(Bad(), rows, 10) is False


def test_gate_defaults_off():
    assert ws.DEFAULT_CONFIG['ingest_chirp'] is False
    assert ws.DEFAULT_CONFIG['chirp_subdir'] == 'chirp'
