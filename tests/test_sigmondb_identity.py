"""sigmondb identity: HFRNet, HFDL systable, receivers seed.  No ClickHouse needed."""
import logging
import os
import sys
import shutil
from datetime import date, datetime
from pathlib import Path

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from sigmondb import ddl  # noqa: E402
from sigmondb.hfdl_gs import parse_systable  # noqa: E402
from sigmondb.hfrnet import parse_hfrnet  # noqa: E402
from sigmondb.receivers import grid_to_latlon, load_receivers  # noqa: E402

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


def test_receivers_seed_has_six():
    rows = load_receivers(SEED)
    assert len(rows) == 6
    assert len({r["reporter_id"] for r in rows}) == 6
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
