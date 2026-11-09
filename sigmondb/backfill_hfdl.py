"""HFDL back-fill: tasks/hfdl-timing/<station>/squitter-part1.csv -> capture and arrival rows.

One arrival per kept, found squitter with a delay.  One capture per (ground station, frequency,
UTC hour, analyzer version): n_expected counts every row, n_found the arrivals written.  A found
row with an empty delay_counter_s writes no arrival and counts in n_expected only; a row with no
nominal_utc (a batch error) has no time and is skipped.  Rows are dicts with exactly the keys of
ddl.COLUMNS[table].  Datetimes are naive UTC.  The package imports nothing from tasks/.
"""
import csv
import json
import logging
from datetime import datetime, timedelta
from pathlib import Path

from sigmondb import SCHEMA_VERSION
from sigmondb.ddl import COLUMNS
from sigmondb.provenance import assumed_provenance, null_provenance
from sigmondb.receivers import load_receivers

log = logging.getLogger(__name__)

SEED = Path(__file__).parent / "seed" / "receivers.toml"
ROOT = "tasks/hfdl-timing"
STATIONS = ("ac0g-nd", "b4", "k3lr", "uacnj", "wb6cxc")
EPOCH = datetime(1970, 1, 1)
HOUR_S = 3600.0


def _build_sets():
    by_host = {r["host_id"]: r["reporter_id"] for r in load_receivers(SEED)}
    return [dict(station=st, path=f"{ROOT}/{st}/squitter-part1.csv", reporter_id=by_host[st])
            for st in STATIONS]


SETS = _build_sets()


def _utc(epoch_s):
    return EPOCH + timedelta(microseconds=round(epoch_s * 1e6))


def _float(s):
    return float(s) if s not in ("", None) else None


def _counter_map(text):
    """The row's counter_map as a dict, or None when absent or unparsable."""
    if not text:
        return None
    try:
        d = json.loads(text)
    except ValueError:
        return None
    return d if isinstance(d, dict) else None


def _provenance(row):
    """Assumed block for a T6 row with a counter_map; the epoch id comes from the map."""
    cm = _counter_map(row["counter_map"])
    if row["t_level_active"] == "T6" and cm is not None:
        return assumed_provenance(str(cm.get("counter_epoch_id") or ""))
    return null_provenance()


def _capture(**kw):
    row = dict(time=None, reporter_id="", centre_hz=0.0, rate_hz_per_s=0.0, duration_s=HOUR_S,
               n_sweeps=None, expected={}, null_max_db=None, null_median_db=None, n_expected=0,
               n_found=0, detector_version="", ring_ref="", schema_version=SCHEMA_VERSION,
               engineering={})
    row.update(null_provenance())
    row.update(kw)
    return {c: row[c] for c in COLUMNS["capture"]}


def _arrival(**kw):
    row = dict(time=None, reporter_id="", tx_id="", group_key="", centre_hz=0.0,
               rate_hz_per_s=0.0, delay_ns=0, delay_sigma_ns=None, level_db=0.0, margin_db=None,
               half_band_db=None, clean_tag="", doppler_hz=None, phase_rate=None,
               carrier_phase_rad=None, s4=None, sigma_phi_rad=None, detector_version="",
               schema_version=SCHEMA_VERSION, engineering={})
    row.update(null_provenance())
    row.update(kw)
    return {c: row[c] for c in COLUMNS["arrival"]}


def read_squitters(csv_path, reporter_id):
    """(captures, arrivals) from one station's squitter-part1.csv.

    A capture takes the assumed provenance block when any of its rows is T6 with a counter_map
    (the first such row names the counter epoch); each arrival takes its own row's block.
    """
    caps, arrs, by_key = [], [], {}
    with open(Path(csv_path), newline="") as fh:
        for r in csv.DictReader(fh):
            if not r["nominal_utc"]:
                continue
            t = float(r["nominal_utc"])
            hour = _utc(t // HOUR_S * HOUR_S)
            version = f"hfdl-analyzer-{r['analyzer_version']}"
            centre = float(r["freq_khz"]) * 1e3
            key = (r["gs_id"], r["freq_khz"], hour, version)
            cap = by_key.get(key)
            if cap is None:
                cap = _capture(time=hour, reporter_id=reporter_id, centre_hz=centre,
                               detector_version=version,
                               engineering={"gs_id": r["gs_id"], "gs_name": r["gs_name"],
                                            "source": "squitter-part1"})
                by_key[key] = cap
                caps.append(cap)
            cap["n_expected"] += 1
            prov = _provenance(r)
            if prov["origin"] and not cap["origin"]:
                cap.update(prov)
            if r["found"] != "True" or r["excluded"] or not r["delay_counter_s"]:
                continue
            cap["n_found"] += 1
            arrs.append(_arrival(
                time=_utc(t), reporter_id=reporter_id, tx_id=f"hfdl:{r['gs_id']}",
                group_key=f"hfdl {r['freq_khz']} kHz", centre_hz=centre, rate_hz_per_s=0,
                delay_ns=round(float(r["delay_counter_s"]) * 1e9), level_db=float(r["snr_db"]),
                clean_tag="squitter", doppler_hz=_float(r["doppler_hz"]),
                detector_version=version,
                engineering={"frame_index": r["frame_index"], "flags": r["flags"],
                             "second_peak_ratio": r["second_peak_ratio"],
                             "t_level_active": r["t_level_active"]},
                **prov))
    for cap in caps:
        gs = cap["engineering"]["gs_id"]
        cap["expected"] = {f"hfdl:{gs}": cap["n_found"] / cap["n_expected"]}
    return caps, arrs


def read_all():
    """{station: (captures, arrivals)} over SETS, paths resolved against the hamsci root."""
    root = Path(__file__).resolve().parents[3]
    return {s["station"]: read_squitters(root / s["path"], s["reporter_id"]) for s in SETS}
