"""HFDL back-fill: tasks/hfdl-timing/<station>/squitter-part1.csv -> capture and arrival rows.

One arrival per kept, found squitter with a delay: delay_counter_s when present (basis
"counter"), else delay_s (basis "label", null provenance); a row with neither writes nothing and
is logged.  One capture per (frequency, UTC hour, analyzer version): two ground stations can share a
frequency, and the table key carries no ground station.  expected maps "hfdl:<gs>" to that
station's found fraction; n_expected counts every row, n_found the arrivals written, both
summed over the ground stations.  A row with no nominal_utc (a batch error) has
no time; it is skipped and logged once per file.  Rows are dicts with exactly the keys of
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
    """Assumed block for a T6 row with a counter_map, else null.

    The epoch id derives from the map's input_epoch_gps_s, as hf-timestd's minute rows name it.
    """
    cm = _counter_map(row["counter_map"])
    if row["t_level_active"] == "T6" and cm is not None:
        g = cm.get("input_epoch_gps_s")
        return assumed_provenance(f"ep-{int(float(g))}" if g not in (None, "") else "")
    return null_provenance()


def _capture(**kw):
    row = dict(time=None, reporter_id="", centre_hz=0.0, rate_hz_per_s=0.0, duration_s=HOUR_S,
               n_sweeps=None, expected={}, null_max_db=None, null_median_db=None, n_expected=0,
               n_found=0, detector_version="", ring_ref="", schema_version=SCHEMA_VERSION,
               engineering={}, received_at=None)
    row.update(null_provenance())
    row.update(kw)
    return {c: row[c] for c in COLUMNS["capture"]}


def _arrival(**kw):
    row = dict(time=None, reporter_id="", tx_id="", group_key="", centre_hz=0.0,
               rate_hz_per_s=0.0, delay_ns=0, delay_sigma_ns=None, level_db=0.0, margin_db=None,
               half_band_db=None, clean_tag="", doppler_hz=None, phase_rate=None,
               carrier_phase_rad=None, s4=None, sigma_phi_rad=None, detector_version="",
               schema_version=SCHEMA_VERSION, engineering={}, received_at=None)
    row.update(null_provenance())
    row.update(kw)
    return {c: row[c] for c in COLUMNS["arrival"]}


def read_squitters(csv_path, reporter_id):
    """(captures, arrivals) from one station's squitter-part1.csv.

    A capture takes the assumed provenance block when any of its rows is T6 with a counter_map
    (the first such row names the counter epoch); each arrival takes its own row's block.
    """
    caps, arrs, by_key, per_gs = [], [], {}, {}
    no_time, no_delay = [], 0
    with open(Path(csv_path), newline="") as fh:
        for r in csv.DictReader(fh):
            if not r["nominal_utc"]:
                no_time.append(r["window_file"])
                continue
            t = float(r["nominal_utc"])
            hour = _utc(t // HOUR_S * HOUR_S)
            version = f"hfdl-analyzer-{r['analyzer_version']}"
            centre = float(r["freq_khz"]) * 1e3
            key = (r["freq_khz"], hour, version)
            cap = by_key.get(key)
            if cap is None:
                cap = _capture(time=hour, reporter_id=reporter_id, centre_hz=centre,
                               detector_version=version,
                               engineering={"source": "squitter-part1", "n_rows": "0",
                                            "n_anchored": "0"})
                by_key[key] = cap
                per_gs[id(cap)] = {}
                caps.append(cap)
            gs = r["gs_id"]
            counts = per_gs[id(cap)].setdefault(gs, [0, 0])
            counts[0] += 1
            cap["n_expected"] += 1
            cap["engineering"]["n_rows"] = str(cap["n_expected"])
            cap["engineering"][f"n_rows:hfdl:{gs}"] = str(counts[0])
            prov = _provenance(r)
            if prov["origin"]:
                cap["engineering"]["n_anchored"] = str(int(cap["engineering"]["n_anchored"]) + 1)
                if not cap["origin"]:
                    cap.update(prov)
            if r["found"] != "True" or r["excluded"]:
                continue
            counter = bool(r["delay_counter_s"])
            delay_s = r["delay_counter_s"] if counter else r["delay_s"]
            if not delay_s:
                no_delay += 1
                continue
            if not counter:
                prov = null_provenance()
            cap["n_found"] += 1
            counts[1] += 1
            arrs.append(_arrival(
                time=_utc(t), reporter_id=reporter_id, tx_id=f"hfdl:{r['gs_id']}",
                group_key=f"hfdl {r['freq_khz']} kHz", centre_hz=centre, rate_hz_per_s=0,
                delay_ns=round(float(delay_s) * 1e9), level_db=float(r["snr_db"]),
                clean_tag="squitter", doppler_hz=_float(r["doppler_hz"]),
                detector_version=version,
                engineering={"frame_index": r["frame_index"], "flags": r["flags"],
                             "second_peak_ratio": r["second_peak_ratio"],
                             "t_level_active": r["t_level_active"],
                             "delay_basis": "counter" if counter else "label"},
                **prov))
    if no_time:
        log.warning("%s: %d rows without nominal_utc skipped (e.g. %s)", csv_path, len(no_time),
                    no_time[0])
    if no_delay:
        log.warning("%s: %d found rows with neither delay_counter_s nor delay_s skipped",
                    csv_path, no_delay)
    for cap in caps:
        cap["expected"] = {}
        for gs, (n, found) in per_gs[id(cap)].items():
            cap["expected"][f"hfdl:{gs}"] = found / n
            cap["engineering"][f"n_found:hfdl:{gs}"] = str(found)
    return caps, arrs


def read_all():
    """{station: (captures, arrivals)} over SETS, paths resolved against the hamsci root."""
    root = Path(__file__).resolve().parents[3]
    return {s["station"]: read_squitters(root / s["path"], s["reporter_id"]) for s in SETS}
