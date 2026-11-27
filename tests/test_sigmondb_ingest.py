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
                  fallback_reporter="AC0G/B4")
    assert t[idx("received_at")] == RA and t[idx("time")] == datetime(2026, 11, 10, 17, 46, 58, 461539)
    assert len(t) == len(ddl.COLUMNS["arrival"])


def test_offset_datetime_becomes_naive_utc():
    t = chirp_row("arrival", dict(ARR, time="2026-11-10T12:46:58+00:00"), received_at=RA,
                  fallback_reporter="AC0G/B4")
    assert t[idx("time")] == datetime(2026, 11, 10, 12, 46, 58)


def test_empty_reporter_takes_the_tar_site():
    assert chirp_row("arrival", dict(ARR, reporter_id=""), received_at=RA,
                     fallback_reporter="AC0G/B4")[idx("reporter_id")] == "AC0G/B4"


def test_unknown_key_and_numeric_engineering_survive():
    t = chirp_row("arrival", dict(ARR, new_key=1, engineering={"peak_width_s": 0.00012, "x": None}),
                  received_at=RA, fallback_reporter="AC0G/B4")
    assert t[idx("engineering")] == {"peak_width_s": "0.00012", "x": ""}


def test_none_in_non_nullable_string_becomes_empty():
    t = chirp_row("arrival", dict(ARR, clean_tag=None), received_at=RA, fallback_reporter="AC0G/B4")
    assert t[idx("clean_tag")] == ""


@pytest.mark.parametrize("bad", [dict(delay_ns=None), dict(schema_version=2), dict(time=None),
                                 dict(time="garbage")])
def test_unusable_line_raises(bad):
    with pytest.raises(ValueError):
        chirp_row("arrival", dict(ARR, **bad), received_at=RA, fallback_reporter="AC0G/B4")


def test_a_bad_line_costs_only_that_line(tmp_path):
    rows, bad = read_chirp_file(_lines(tmp_path, [ARR, "{x", dict(ARR, delay_ns=None), ARR]),
                                "arrival", received_at=RA, fallback_reporter="AC0G/B4")
    assert (len(rows), bad) == (2, 2)


def test_capture_expected_values_become_float():
    t = chirp_row("capture", CAP, received_at=RA, fallback_reporter="AC0G/B4")
    exp = t[idx("expected", "capture")]
    assert exp == {"hfdl:4": 1.0} and isinstance(exp["hfdl:4"], float)


def test_insert_sql_names_columns_in_order():
    assert insert_sql("capture") == (
        "INSERT INTO sigmondb.capture (%s) VALUES" % ", ".join(ddl.COLUMNS["capture"]))


def test_unknown_key_warns_once_per_file(tmp_path, caplog):
    import logging
    p = _lines(tmp_path, [dict(ARR, new_key=1) for _ in range(5)])
    with caplog.at_level(logging.WARNING):
        rows, bad = read_chirp_file(p, "arrival", received_at=RA, fallback_reporter="AC0G/B4")
    assert len(rows) == 5 and bad == 0
    assert len([r for r in caplog.records if "new_key" in r.getMessage()]) == 1


@pytest.mark.parametrize("bad", [
    dict(delay_ns="abc"), dict(delay_ns=1.5), dict(delay_ns=2 ** 63), dict(delay_ns=True),
    dict(schema_version=-1), dict(level_db=float("nan")), dict(level_db=float("inf")),
    dict(delay_sigma_ns=float("inf")), dict(tx_id=5), dict(group_key=[1])])
def test_type_and_range_mismatch_raises(bad):
    with pytest.raises(ValueError):
        chirp_row("arrival", dict(ARR, **bad), received_at=RA, fallback_reporter="AC0G/B4")


def test_out_of_range_uint_raises():
    with pytest.raises(ValueError):
        chirp_row("capture", dict(CAP, n_expected=-1), received_at=RA, fallback_reporter="AC0G/B4")
    with pytest.raises(ValueError):
        chirp_row("capture", dict(CAP, n_found=2 ** 32), received_at=RA, fallback_reporter="AC0G/B4")


def test_numeric_string_and_integral_float_coerce():
    t = chirp_row("arrival", dict(ARR, delay_ns="42", centre_hz=7), received_at=RA,
                  fallback_reporter="AC0G/B4")
    assert t[idx("delay_ns")] == 42 and t[idx("centre_hz")] == 7.0


def test_mixed_file_returns_good_rows(tmp_path):
    rows, bad = read_chirp_file(_lines(tmp_path, [ARR, dict(ARR, delay_ns="abc"), ARR]),
                                "arrival", received_at=RA, fallback_reporter="AC0G/B4")
    assert (len(rows), bad) == (2, 1)


def test_reporter_suffix_may_differ_callsign_may_not():
    t = chirp_row("arrival", dict(ARR, reporter_id="ac0g/B5"), received_at=RA,
                  fallback_reporter="AC0G/B4")
    assert t[idx("reporter_id")] == "ac0g/B5"
    with pytest.raises(ValueError, match="does not match the upload site"):
        chirp_row("arrival", dict(ARR, reporter_id="W1AW/B4"), received_at=RA,
                  fallback_reporter="AC0G/B4")


def test_invalid_utf8_costs_only_that_line(tmp_path):
    p = tmp_path / "arrival.jsonl"
    good = json.dumps(ARR).encode()
    p.write_bytes(good + b"\n" + b'{"a": "\xff\xfe"}\n' + good + b"\n")
    rows, bad = read_chirp_file(p, "arrival", received_at=RA, fallback_reporter="AC0G/B4")
    assert (len(rows), bad) == (2, 1)


def test_huge_integer_in_float_column_raises_valueerror(tmp_path):
    huge = int("9" * 400)
    with pytest.raises(ValueError):
        chirp_row("arrival", dict(ARR, level_db=huge), received_at=RA, fallback_reporter="AC0G/B4")
    p = tmp_path / "arrival.jsonl"
    g = json.dumps(ARR)
    p.write_text(g + "\n" + g.replace('"level_db": -90.0', '"level_db": ' + "9" * 400) + "\n" + g + "\n")
    rows, bad = read_chirp_file(p, "arrival", received_at=RA, fallback_reporter="AC0G/B4")
    assert (len(rows), bad) == (2, 1)


def test_deeply_nested_line_costs_only_that_line(tmp_path):
    p = tmp_path / "arrival.jsonl"
    g = json.dumps(ARR)
    p.write_text(g + "\n" + "[" * 100000 + "\n" + g + "\n")
    rows, bad = read_chirp_file(p, "arrival", received_at=RA, fallback_reporter="AC0G/B4")
    assert (len(rows), bad) == (2, 1)


DRIFT = dict(
    time="2026-11-19T06:10:00Z", reporter_id="AC0G/B4", host_key="a1b2c3d4e5f6",
    hostname="b4-vm", release="v3.94", baseline="v3.93", n_fault=1, n_drift=2,
    n_accepted=3, counts_json='{"fault": 1}', faults_json='[{"kind": "x"}]',
    reminders_json="[]", schema_version=1,
    receivers_json=json.dumps([{"reporter_id": "AC0G/B4", "radiod_id": "r0",
                                "grid6": "EN16ab", "pos_source": "gpsdo"}]))


def test_ingest_station_drift_row():
    t = chirp_row("station_drift", DRIFT, received_at=RA, fallback_reporter="AC0G/B4")
    i = lambda c: idx(c, "station_drift")
    assert len(t) == len(ddl.COLUMNS["station_drift"])
    assert t[i("time")] == datetime(2026, 11, 19, 6, 10) and t[i("received_at")] == RA
    assert (t[i("n_fault")], t[i("n_drift")], t[i("n_accepted")]) == (1, 2, 3)
    assert t[i("faults_json")] == '[{"kind": "x"}]' and t[i("release")] == "v3.94"


def test_station_drift_identity_columns_round_trip():
    t = chirp_row("station_drift", DRIFT, received_at=RA, fallback_reporter="AC0G/B4")
    i = lambda c: idx(c, "station_drift")
    assert (t[i("host_key")], t[i("hostname")]) == ("a1b2c3d4e5f6", "b4-vm")
    assert json.loads(t[i("receivers_json")]) == json.loads(DRIFT["receivers_json"])
    # an older row without the identity columns still lands, with empty strings
    old = {k: v for k, v in DRIFT.items() if k not in ("host_key", "hostname", "receivers_json")}
    t = chirp_row("station_drift", old, received_at=RA, fallback_reporter="AC0G/B4")
    assert (t[i("host_key")], t[i("hostname")], t[i("receivers_json")]) == ("", "", "")


def test_ingest_station_drift_rejects_unknown_schema(tmp_path):
    p = _lines(tmp_path, [DRIFT, dict(DRIFT, schema_version=2), DRIFT])
    rows, bad = read_chirp_file(p, "station_drift", received_at=RA,
                                fallback_reporter="AC0G/B4")
    assert (len(rows), bad) == (2, 1)


@pytest.mark.skip(reason="the chirp reporter rule lives in held-back commit 40f4deb, not on origin/main")
def test_station_drift_reporter_rule_applies(tmp_path):
    with pytest.raises(RefusedReporter):
        chirp_row("station_drift", dict(DRIFT, reporter_id="W1AW/B4"), received_at=RA,
                  fallback_reporter="AC0G/B4")
    refused = {}
    rows, bad = read_chirp_file(_lines(tmp_path, [dict(DRIFT, reporter_id="W1AW/B4")]),
                                "station_drift", received_at=RA,
                                fallback_reporter="AC0G/B4", refused=refused)
    assert (rows, bad, refused) == ([], 0, {"W1AW/B4": 1})


def test_station_drift_row_carries_sender():
    t = chirp_row("station_drift", DRIFT, received_at=RA, fallback_reporter="AC0G/B2")
    assert t[list(ddl.COLUMN_TYPES["station_drift"]).index("sender")] == "AC0G/B2"


SWEEP = dict(
    time="2026-11-10T17:46:58.461Z", reporter_id="AC0G/B4", view="wide", centre_hz=8.9e6,
    span_hz=3000.0, rate_hz_per_s=-125.5, period_s=10.0, duration_s=None, snr_db=14.5,
    n_sweeps=3, fit_score=0.97, clipped=True, matched_key="", bin_hz=11.7, frame_s=0.08,
    detector_version="v1", schema_version=1, engineering={"n_streaks": 4})
DWELL = dict(
    time="2026-11-10T17:46:00Z", reporter_id="", view="wide", centre_hz=8.9e6, span_hz=3000.0,
    bin_hz=11.7, frame_s=0.08, duration_s=60.0, n_frames=750, noise_db=None, n_streaks=2,
    detector_version="v1", schema_version=1, engineering={})


def test_sweep_row_round_trip():
    t = chirp_row("sweep", SWEEP, received_at=RA, fallback_reporter="AC0G/B4")
    i = lambda c: idx(c, "sweep")
    assert len(t) == len(ddl.COLUMNS["sweep"])
    assert t[i("received_at")] == RA and t[i("clipped")] == 1 and t[i("view")] == "wide"
    assert t[i("time")] == datetime(2026, 11, 10, 17, 46, 58, 461000)
    assert t[i("rate_hz_per_s")] == -125.5 and t[i("duration_s")] is None
    assert t[i("n_sweeps")] == 3 and t[i("engineering")] == {"n_streaks": "4"}


def test_sweep_rejects_bad_rate():
    with pytest.raises(ValueError):
        chirp_row("sweep", dict(SWEEP, rate_hz_per_s="fast"), received_at=RA,
                  fallback_reporter="AC0G/B4")


def test_dwell_empty_reporter_files_under_sender():
    t = chirp_row("scout_dwell", DWELL, received_at=RA, fallback_reporter="AC0G/B4")
    assert t[idx("reporter_id", "scout_dwell")] == "AC0G/B4"
    assert len(t) == len(ddl.COLUMNS["scout_dwell"])
