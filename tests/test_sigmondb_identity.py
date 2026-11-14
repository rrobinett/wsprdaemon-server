"""sigmondb identity: HFRNet, HFDL systable, receivers seed.  No ClickHouse needed."""
import json
import math
import struct
import logging
import os
import sys
import shutil

import pytest
from datetime import date, datetime
from pathlib import Path

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from sigmondb import ddl  # noqa: E402
from sigmondb.hfdl_gs import parse_systable  # noqa: E402
from sigmondb.hfrnet import parse_hfrnet  # noqa: E402
from sigmondb.groundwave import _latest  # noqa: E402
from sigmondb.receivers import (ROUNDING_SIGMA_M, grid_to_latlon, load_positions,  # noqa: E402
                                load_receivers, position_rows)

FX = Path(__file__).parent / "fixtures" / "sigmondb"
SEED = Path(__file__).parent.parent / "sigmondb" / "seed" / "receivers.toml"
SNAP = date(2026, 11, 9)


def _hfrnet():
    return parse_hfrnet(FX / "stationList.csv", SNAP)


def _by(rows, key, val):
    return next(r for r in rows if r[key] == val)


def test_hfrnet_up_sweep():
    _, wf = _hfrnet()
    w = _by(wf, "tx_id", "hfrnet:ACI1")
    assert w["centre_hz"] == 13_475_000.0
    assert abs(w["rate_hz_per_s"] - 95_583.1) <= 1  # brief said 95_582; 47.791553 kHz x 2 Hz = 95_583.106
    assert w["period_s"] == 0.5


def test_hfrnet_down_sweep():
    _, wf = _hfrnet()
    w = _by(wf, "tx_id", "hfrnet:AGL1")
    assert abs(w["rate_hz_per_s"] - (-25_733.9)) <= 1
    assert w["period_s"] == 1.0


def test_hfrnet_odd_rows_never_abort(caplog):
    with caplog.at_level(logging.WARNING):
        tx, wf = _hfrnet()
    assert len(tx) == 3 and len(wf) == 3
    ids = {r["tx_id"] for r in tx}
    assert "hfrnet:BLNK" not in ids
    assert {"hfrnet:ACI1", "hfrnet:AGL1", "hfrnet:ANGL"} <= ids
    warns = [r for r in caplog.records if r.levelno == logging.WARNING]
    assert len(warns) == 1 and "BLNK" in warns[0].getMessage()


def test_hfrnet_retired_by_age(tmp_path):
    csv = tmp_path / "s.csv"
    csv.write_text(
        'Station,"Station Name",Network,Frequency,Bandwidth,SweepRate,"Date Added",'
        '"Last Radial File",Latitude,Longitude,Manufacturer\n'
        'OLD1,"Old",TEST,5.00000,30.0,2.0,2010-01-01,2026-10-01,35.0,-120.0,"x"\n')
    tx, _ = parse_hfrnet(csv, SNAP)
    assert _by(tx, "tx_id", "hfrnet:OLD1")["status"] == "retired"
    tx, _ = _hfrnet()
    assert _by(tx, "tx_id", "hfrnet:ACI1")["status"] == "listed"


def test_hfrnet_rows_carry_every_column():
    tx, wf = _hfrnet()
    assert all(set(r) == set(ddl.COLUMNS["transmitter"]) for r in tx)
    assert all(set(r) == set(ddl.COLUMNS["waveform"]) for r in wf)


def test_systable_riverhead():
    tx, wf = parse_systable(FX / "systable.conf", SNAP)
    t = _by(tx, "tx_id", "hfdl:4")
    assert t["lat"] == 40.881922 and t["kind"] == "hfdl" and t["network"] == "hfdl"
    rv = [w for w in wf if w["tx_id"] == "hfdl:4"]
    assert len(rv) == 7
    w = _by(rv, "centre_hz", 6_661_000.0)
    assert w["keying_step_ns"] == 8_408_600.0
    assert w["keying_modulus_ns"] == 65_992_600.0
    assert w["period_s"] == 32.0 and w["frame_s"] == 32.0 and w["rate_hz_per_s"] == 0
    assert all(set(r) == set(ddl.COLUMNS["waveform"]) for r in wf)


def test_systable_unkeyed_station_has_null_keying():
    _, wf = parse_systable(FX / "systable.conf", SNAP, keying={})
    assert all(w["keying_step_ns"] is None for w in wf)


def test_grid_centre():
    lat, lon = grid_to_latlon("EM38ww")
    assert abs(lat - 38.9375) < 0.01 and abs(lon - (-92.125)) < 0.01


def test_receivers_seed_has_nine():
    rows = load_receivers(SEED)
    assert len(rows) == 9
    assert len({r["reporter_id"] for r in rows}) == 9
    assert all(r["lat"] is not None and r["lon"] is not None for r in rows)
    assert all(set(r) == set(ddl.COLUMNS["receiver"]) for r in rows)
    assert all(r["pos_source"] == "typed_grid" for r in rows)


def test_systable_valid_from_is_snapshot_not_mtime(tmp_path):
    f = tmp_path / "systable.conf"
    shutil.copy(FX / "systable.conf", f)
    a = parse_systable(f, SNAP)
    os.utime(f, (1_000_000_000, 1_000_000_000))
    b = parse_systable(f, SNAP)
    assert a == b
    assert all(r["valid_from"] == datetime(2026, 11, 9) for r in a[0] + a[1])


def test_systable_transmitter_keys_match_columns():
    tx, _ = parse_systable(FX / "systable.conf", SNAP)
    assert tx and all(set(r) == set(ddl.COLUMNS["transmitter"]) for r in tx)


def test_systable_incomplete_block_warns_once(tmp_path, caplog):
    f = tmp_path / "s.conf"
    f.write_text('stations = ( { id = 9; lat = 1.0; name = "Nolon"; frequencies = ( 5000.0 ); }, '
                 '{ id = 8; lat = 1.0; lon = 2.0; name = "Ok"; frequencies = ( 5000.0 ); } );')
    with caplog.at_level(logging.WARNING):
        tx, _ = parse_systable(f, SNAP)
    assert [r["tx_id"] for r in tx] == ["hfdl:8"]
    w = [r for r in caplog.records if r.levelno == logging.WARNING]
    assert len(w) == 1 and "Nolon" in w[0].getMessage()


def test_grid_cell_half_diagonal():
    # half the cell diagonal on a sphere of radius 6371008.8 m, east-west width at the
    # cell centre's latitude: 0.5 * hypot(R*dlat, R*cos(lat)*dlon)
    from sigmondb.receivers import grid_pos_sigma_m
    assert abs(grid_pos_sigma_m("EN91se") - 4186.11) < 0.01     # 1/24 x 2/24 deg at 41.1875 N
    assert abs(grid_pos_sigma_m("EN91") - 100133.29) < 0.01     # 1 x 2 deg at 41.5 N
    assert abs(grid_pos_sigma_m("EN") - 962977.64) < 0.01       # 10 x 20 deg at 45 N
    assert abs(grid_pos_sigma_m("EN91se11") - 418.68) < 0.01     # 1/240 x 1/120 deg at 41.1729 N


def test_receivers_seed_carries_grid_sigma():
    rows = {r["reporter_id"]: r for r in load_receivers(SEED)}
    assert abs(rows["K3LR/D"]["pos_sigma_m"] - 4186.11) < 0.01
    assert all(r["pos_sigma_m"] and r["pos_sigma_m"] > 0 for r in rows.values())


def test_seed_names_the_radiods_not_the_transporter():
    ids = {r["reporter_id"] for r in load_receivers(SEED)}
    assert {"N8UR", "KFS-OMNI", "AC0G/B1A"} <= ids
    assert not ids & {"KFS-MAIN", "KFS/MAIN"}


def _pos(tmp_path, doc, mode=0o600):
    f = tmp_path / "pos.json"
    f.write_text(json.dumps(doc))
    f.chmod(mode)
    return f


def test_position_rows_version_up_and_say_gpsdo(tmp_path):
    f = _pos(tmp_path, {"K3LR/D": {"lat": 41.0, "lon": -80.0, "pos_sigma_m": 10.0},
                        "NOPE": {"lat": 1.0, "lon": 1.0, "pos_sigma_m": 10.0}})
    rows, unknown = position_rows(load_receivers(SEED), load_positions(f), datetime(2026, 11, 14))
    assert unknown == ["NOPE"] and len(rows) == 1
    r = rows[0]
    assert (r["pos_source"], r["version"], r["lat"]) == ("gpsdo", 2, 41.0)
    assert r["valid_from"] == datetime(2026, 11, 14)
    assert _latest(load_receivers(SEED) + rows, "reporter_id")["K3LR/D"] is r


def test_position_rows_round_to_a_hundredth_of_a_degree(tmp_path):
    # wd30 answers anonymous queries, so sigmondb.receiver never holds a house-level position
    # (Michael, 2026-11-14: 0.01 degree, about 1 km).
    f = _pos(tmp_path, {"K3LR/D": {"lat": 41.123456, "lon": -80.987654, "pos_sigma_m": 5.0}})
    rows, _ = position_rows(load_receivers(SEED), load_positions(f), datetime(2026, 11, 14))
    r = rows[0]
    assert (r["lat"], r["lon"]) == (41.12, -80.99)
    assert r["pos_sigma_m"] == pytest.approx(math.hypot(5.0, ROUNDING_SIGMA_M))


def test_position_rows_write_nothing_finer_than_a_hundredth(tmp_path):
    doc = {"K3LR/D": {"lat": 41.0049999, "lon": -80.0050001, "pos_sigma_m": 1.0},
           "W2NAF": {"lat": -12.3456789, "lon": 179.999999, "pos_sigma_m": 1.0}}
    rows, _ = position_rows(load_receivers(SEED), load_positions(_pos(tmp_path, doc)),
                            datetime(2026, 11, 14))
    assert len(rows) == 2
    for r in rows:
        for k in ("lat", "lon"):
            assert r[k] * 100 == pytest.approx(round(r[k] * 100), abs=1e-9)


def test_position_rows_a_rounded_repeat_adds_no_version(tmp_path):
    doc = {"K3LR/D": {"lat": 41.123456, "lon": -80.987654, "pos_sigma_m": 5.0}}
    first, _ = position_rows(load_receivers(SEED), load_positions(_pos(tmp_path, doc)),
                             datetime(2026, 11, 14))
    doc["K3LR/D"]["lat"] = 41.1234   # moves inside the same 0.01-degree cell
    again, _ = position_rows(load_receivers(SEED) + first,
                             load_positions(_pos(tmp_path, doc)), datetime(2026, 11, 15))
    assert again == []


def test_a_world_readable_positions_file_is_refused(tmp_path):
    f = _pos(tmp_path, {}, 0o644)
    with pytest.raises(ValueError, match="mode"):
        load_positions(f)


def test_a_group_readable_positions_file_is_refused(tmp_path):
    with pytest.raises(ValueError, match="mode"):
        load_positions(_pos(tmp_path, {}, 0o640))


def test_a_position_entry_missing_a_key_is_refused_without_its_values(tmp_path):
    f = _pos(tmp_path, {"K3LR/D": {"lat": 41.5551, "lon": -80.0}})
    with pytest.raises(ValueError) as e:
        load_positions(f)
    assert "K3LR/D" in str(e.value) and "pos_sigma_m" in str(e.value)
    assert "41.5551" not in str(e.value) and "80.0" not in str(e.value)


def test_a_position_equal_to_the_latest_row_gets_no_new_version(tmp_path):
    base = load_receivers(SEED)
    f = _pos(tmp_path, {"K3LR/D": {"lat": 41.0, "lon": -80.0, "pos_sigma_m": 10.0}})
    first, _ = position_rows(base, load_positions(f), datetime(2026, 11, 14))
    again, unknown = position_rows(base + first, load_positions(f), datetime(2026, 11, 15))
    assert again == [] and unknown == []


def test_a_sigma_read_back_as_float32_is_unchanged(tmp_path):
    base = load_receivers(SEED)
    # What the database hands back for the widened sigma: hypot(12.3, ROUNDING_SIGMA_M), float32.
    stored = struct.unpack("f", struct.pack("f", math.hypot(12.3, ROUNDING_SIGMA_M)))[0]
    old = dict(_latest(base, "reporter_id")["K3LR/D"], lat=41.0, lon=-80.0,
               pos_sigma_m=stored, pos_source="gpsdo", version=2)
    f = _pos(tmp_path, {"K3LR/D": {"lat": 41.0, "lon": -80.0, "pos_sigma_m": 12.3}})
    rows, _ = position_rows(base + [old], load_positions(f), datetime(2026, 11, 15))
    assert rows == []


@pytest.mark.parametrize("entry", [
    {"lat": "41.5551", "lon": -80.0, "pos_sigma_m": 10.0},
    {"lat": float("nan"), "lon": -80.0, "pos_sigma_m": 10.0},
    {"lat": float("inf"), "lon": -80.0, "pos_sigma_m": 10.0},
    {"lat": 41.0, "lon": -80.0, "pos_sigma_m": float("inf")},
    {"lat": 90.5, "lon": -80.0, "pos_sigma_m": 10.0},
    {"lat": 41.0, "lon": -180.5, "pos_sigma_m": 10.0},
    {"lat": 41.0, "lon": -80.0, "pos_sigma_m": -1.0},
])
def test_bad_position_values_are_refused_without_the_value(tmp_path, entry):
    f = tmp_path / "pos.json"
    f.write_text(json.dumps({"K3LR/D": entry}))   # json.dumps writes NaN/Infinity, load accepts
    f.chmod(0o600)
    with pytest.raises(ValueError) as e:
        load_positions(f)
    msg = str(e.value)
    assert "K3LR/D" in msg and "41.5551" not in msg and "90.5" not in msg and "180.5" not in msg


def test_overlay_positions_replaces_listed_reporters_and_copies_the_rest():
    from sigmondb.receivers import overlay_positions
    rows = [dict(reporter_id="A", lat=41.12, lon=-80.46, pos_sigma_m=400.0, pos_source="gpsdo",
                 version=2),
            dict(reporter_id="B", lat=1.0, lon=2.0, pos_sigma_m=3.0, pos_source="typed_grid")]
    keep = [dict(r) for r in rows]
    out = overlay_positions(rows, {"A": dict(lat=41.123457, lon=-80.456789, pos_sigma_m=5.5),
                                   "Z": dict(lat=9.0, lon=9.0, pos_sigma_m=9.0)})
    assert rows == keep                                     # input untouched
    assert out[0] is not rows[0] and out[1] is not rows[1]
    assert (out[0]["lat"], out[0]["lon"], out[0]["pos_sigma_m"]) == (41.123457, -80.456789, 5.5)
    assert out[0]["pos_source"] == "gpsdo" and out[0]["version"] == 2
    assert out[1] == rows[1]
