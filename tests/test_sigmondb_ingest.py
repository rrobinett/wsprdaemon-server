"""sigmondb.ingest: one chirp/ JSON line to one row tuple."""
import json
import os
import sys
from datetime import datetime

import pytest

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from sigmondb import ddl  # noqa: E402
from sigmondb.ingest import chirp_row, insert_sql, read_chirp_file  # noqa: E402

RA = datetime(2026, 11, 10, 18, 0)
ARR = dict(
    time="2026-11-10T17:46:58.461539Z", reporter_id="AC0G/B4", tx_id="hfdl:4", group_key="g",
    centre_hz=8.9e6, rate_hz_per_s=0.0, delay_ns=12345, delay_sigma_ns=None, level_db=-90.0,
    margin_db=None, half_band_db=None, clean_tag="", doppler_hz=None, phase_rate=None,
    carrier_phase_rad=None, s4=None, sigma_phi_rad=None, detector_version="v1",
    schema_version=1, engineering={"delay_basis": "counter"})
CAP = dict(
    time="2026-11-10T17:00:00.000Z", reporter_id="AC0G/B4", centre_hz=8.9e6, rate_hz_per_s=0.0,
    duration_s=3600.0, n_sweeps=None, expected={"hfdl:4": 1}, null_max_db=None,
    null_median_db=None, n_expected=1, n_found=1, detector_version="v1", ring_ref="",
    schema_version=1, engineering={})


def idx(c, t="arrival"):
    return ddl.COLUMNS[t].index(c)


def _lines(tmp_path, items):
    p = tmp_path / "arrival.jsonl"
    p.write_text("".join((i if isinstance(i, str) else json.dumps(i)) + "\n" for i in items))
    return p


def test_received_at_comes_from_the_tar_not_the_row():
    t = chirp_row("arrival", dict(ARR, received_at="2000-01-01T00:00:00Z"), received_at=RA,
                  fallback_reporter="X")
    assert t[idx("received_at")] == RA and t[idx("time")] == datetime(2026, 11, 10, 17, 46, 58, 461539)
    assert len(t) == len(ddl.COLUMNS["arrival"])


def test_offset_datetime_becomes_naive_utc():
    t = chirp_row("arrival", dict(ARR, time="2026-11-10T12:46:58+00:00"), received_at=RA,
                  fallback_reporter="X")
    assert t[idx("time")] == datetime(2026, 11, 10, 12, 46, 58)


def test_empty_reporter_takes_the_tar_site():
    assert chirp_row("arrival", dict(ARR, reporter_id=""), received_at=RA,
                     fallback_reporter="AC0G/B4")[idx("reporter_id")] == "AC0G/B4"


def test_unknown_key_and_numeric_engineering_survive():
    t = chirp_row("arrival", dict(ARR, new_key=1, engineering={"peak_width_s": 0.00012, "x": None}),
                  received_at=RA, fallback_reporter="X")
    assert t[idx("engineering")] == {"peak_width_s": "0.00012", "x": ""}


def test_none_in_non_nullable_string_becomes_empty():
    t = chirp_row("arrival", dict(ARR, clean_tag=None), received_at=RA, fallback_reporter="X")
    assert t[idx("clean_tag")] == ""


@pytest.mark.parametrize("bad", [dict(delay_ns=None), dict(schema_version=2), dict(time=None),
                                 dict(time="garbage")])
def test_unusable_line_raises(bad):
    with pytest.raises(ValueError):
        chirp_row("arrival", dict(ARR, **bad), received_at=RA, fallback_reporter="X")


def test_a_bad_line_costs_only_that_line(tmp_path):
    rows, bad = read_chirp_file(_lines(tmp_path, [ARR, "{x", dict(ARR, delay_ns=None), ARR]),
                                "arrival", received_at=RA, fallback_reporter="X")
    assert (len(rows), bad) == (2, 2)


def test_capture_expected_values_become_float():
    t = chirp_row("capture", CAP, received_at=RA, fallback_reporter="X")
    exp = t[idx("expected", "capture")]
    assert exp == {"hfdl:4": 1.0} and isinstance(exp["hfdl:4"], float)


def test_insert_sql_names_columns_in_order():
    assert insert_sql("capture") == (
        "INSERT INTO sigmondb.capture (%s) VALUES" % ", ".join(ddl.COLUMNS["capture"]))
