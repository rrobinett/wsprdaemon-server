import os
import sys

from pytest import approx

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from sigmondb.paths import (coast_of, gc_midpoint, ground_range_km,  # noqa: E402
                            match_group, path_kind)

STATION = (38.94, -92.12)


def _tx(tx_id, lat, lon, rate=-25734.0, centre=4.4505e6):
    return {"tx_id": tx_id, "lat": lat, "lon": lon,
            "centre_hz": centre, "rate_hz_per_s": rate}


def test_match_group_picks_nearer_coast():
    txs = [_tx("TX1", 28.0, -97.0), _tx("TX2", 27.5, -97.2),
           _tx("CA1", 34.0, -120.0), _tx("CA2", 36.0, -121.9)]
    m = match_group("4.4505 -25.73", 4.4505e6, -25734.0, STATION, txs)
    assert m["group_id"] == "4.4505 -25.73 E"
    assert m["coast"] == "E"
    assert m["members"] == ["TX1", "TX2"]
    assert m["both_coasts"] is True
    assert m["nearest_km"] > 0


def test_match_group_none_without_candidate():
    txs = [_tx("TX1", 28.0, -97.0)]
    assert match_group("7.0 -25.73", 7.0e6, -25734.0, STATION, txs) is None


def test_match_group_rate_tolerance():
    near = _tx("A", 28.0, -97.0, rate=-25734.0)
    far = _tx("B", 28.0, -97.0, rate=-27000.0)
    m = match_group("k", 4.4505e6, -25700.0, STATION, [near, far])
    assert m["members"] == ["A"]
    assert m["both_coasts"] is False


def test_coast_of():
    assert coast_of(-99.9) == "E"
    assert coast_of(-100.0) == "W"


def test_ground_range_bands():
    assert ground_range_km(4.785e6) == 250
    assert ground_range_km(9.0e6) == 200
    assert ground_range_km(13.5e6) == 150


def test_path_kind():
    assert path_kind(240, 4.785e6) == "ground_range"
    assert path_kind(240, 13.5e6) == "skywave"


def test_gc_midpoint_equator():
    assert gc_midpoint(0, 0, 0, 90) == approx((0, 45))
    assert gc_midpoint(10, 20, 10, 20) == approx((10, 20))
