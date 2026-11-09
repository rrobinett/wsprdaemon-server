"""CODAR back-fill: the 11-04, 11-07 and 11-08 sets under tasks/codar-t6/ -> capture and arrival rows.

Inputs, as relabel.py and day1108-13mhz/analyze.py made them:
  tsv   one row per CLEANed arrival; phase_t6_ms is the arrival's phase after the UTC second
        on T6's ruler (11-04 and 11-07, 4.463 MHz Rutgers long-range group).
  res   one JSON line per capture with every sweep group's detection (11-08, 13.4875 and
        4.463 MHz).
Rows are dicts with exactly the keys of ddl.COLUMNS[table].  Datetimes are naive UTC.
The package imports nothing from tasks/; the constants below carry what it needs.
"""
import json
import logging
import re
from calendar import timegm
from datetime import datetime, timedelta
from pathlib import Path

from sigmondb import SCHEMA_VERSION
from sigmondb.ddl import COLUMNS
from sigmondb.provenance import (assumed_provenance, null_provenance, provenance_at)
from sigmondb.receivers import load_receivers

log = logging.getLogger(__name__)

DETECTOR_VERSION = "offline-2026-11"
SEED = Path(__file__).parent / "seed" / "receivers.toml"

# 4.463 MHz long-range group (codar-mf/clean.py K) and the OSU sweep (analyze.py G4).
K_4463 = -25_733.913
CENTRE_4463 = 4_463_000.0
PERIOD_4463_S = 1.0
EXACT_GROUPS = {
    "Rutgers 4.463 -25.73": (CENTRE_4463, K_4463),
    "OSU 4.463 -47.79": (CENTRE_4463, -47_791.553),
}
PERIOD_13MHZ_S = 0.5
CAPTURE_S = 30.0          # every capture holds 30 s of samples
STAMP_TOLERANCE_S = 5.0
ANCHOR_AGE_MAX_S = 120.0  # 11-04 sets: no minute rows, trust the tsv's anchor_age_s

_STAMP = re.compile(r"-(\d{8}T\d{6})Z\.npy$")
_receivers = {}


def reporter_for(station):
    """reporter_id of a station directory name (a host_id in receivers.toml)."""
    if not _receivers:
        for r in load_receivers(SEED):
            _receivers[r["host_id"]] = r["reporter_id"]
    return _receivers[station]


def _set(label, kind, station, path, **kw):
    return dict(label=label, kind=kind, station=station, path=path, **kw)


def _build_sets():
    root = "tasks/codar-t6"
    out = []
    for st in ("k3lr", "uacnj"):
        out.append(_set("11-04", "tsv", st, f"{root}/{st}-t6.tsv", capture_dir=f"{root}/{st}",
                        minutes=[], group="Rutgers 4.463 -25.73"))
    for st in ("ac0g-nd", "k3lr", "uacnj", "w2naf"):
        d = f"{root}/day1107-night"
        out.append(_set("11-07", "tsv", st, f"{d}/{st}-t6.tsv", capture_dir=f"{d}/{st}",
                        minutes=[f"{d}/{st}/minutes-20261107.jsonl"],
                        group="Rutgers 4.463 -25.73"))
    for st in ("ac0g-nd", "k3lr", "uacnj", "w2naf", "wb6cxc"):
        d = f"{root}/day1108-13mhz"
        out.append(_set("11-08", "res", st, f"{d}/res-{st}.jsonl", capture_dir=f"{d}/{st}",
                        minutes=[f"{d}/{st}/minutes-20261108.jsonl"], group="all"))
    for s in out:
        s["reporter_id"] = reporter_for(s["station"])
    return out


SETS = _build_sets()


def _group_key(centre_hz, rate_hz_per_s):
    return f"{centre_hz / 1e6:.4f} {rate_hz_per_s / 1e3:+.2f}"


def _epoch(dt):
    return float(timegm(dt.timetuple())) + dt.microsecond * 1e-6


def _clean_tag(tag):
    if not tag:
        return "A"
    return "R" if "rung" in tag else "m"


def _capture(**kw):
    row = dict(time=None, reporter_id="", centre_hz=0.0, rate_hz_per_s=0.0, duration_s=CAPTURE_S,
               n_sweeps=None, expected={}, null_max_db=None, null_median_db=None, n_expected=None,
               n_found=None, detector_version=DETECTOR_VERSION, ring_ref="",
               schema_version=SCHEMA_VERSION, engineering={})
    row.update(null_provenance())
    row.update(kw)
    return {c: row[c] for c in COLUMNS["capture"]}


def _arrival(**kw):
    row = dict(time=None, reporter_id="", tx_id="", group_key="", centre_hz=0.0,
               rate_hz_per_s=0.0, delay_ns=0, delay_sigma_ns=None, level_db=0.0, margin_db=None,
               half_band_db=None, clean_tag="", doppler_hz=None, phase_rate=None,
               carrier_phase_rad=None, s4=None, sigma_phi_rad=None,
               detector_version=DETECTOR_VERSION, schema_version=SCHEMA_VERSION,
               engineering={})
    row.update(null_provenance())
    row.update(kw)
    return {c: row[c] for c in COLUMNS["arrival"]}


def _stamps(capture_dir):
    out = []
    for p in Path(capture_dir).glob("*.npy"):
        m = _STAMP.search(p.name)
        if m:
            out.append((datetime.strptime(m.group(1), "%Y%m%dT%H%M%S"), p.name))
    return sorted(out)


def _locate(stamps, hms):
    """(naive UTC datetime, file name) of the capture whose stamp lies within 5 s of HH:MM:SS.

    The date comes from the file, so a capture across midnight keeps its own day.
    """
    h, m, s = (int(x) for x in hms.split(":"))
    want = h * 3600 + m * 60 + s
    best = None
    for dt, name in stamps:
        have = dt.hour * 3600 + dt.minute * 60 + dt.second
        d = (want - have + 43200) % 86400 - 43200   # wrapped to [-12 h, 12 h)
        if abs(d) <= STAMP_TOLERANCE_S and (best is None or abs(d) < abs(best[0])):
            best = (d, dt, name)
    if best is None:
        return None
    return best[1] + timedelta(seconds=best[0]), best[2]


def read_t6_tsv(tsv, capture_dir, reporter_id, centre_hz, rate_hz_per_s, period_s, minutes):
    """(captures, arrivals) from a relabel.py tsv.

    Provenance: from `minutes` when given (nearest anchored minute within 120 s), else from the
    tsv's anchor_age_s (|age| <= 120 s -> assumed block with no epoch id, else null block).
    """
    stamps = _stamps(capture_dir)
    period_ns = round(period_s * 1e9)
    gkey = _group_key(centre_hz, rate_hz_per_s)
    n_sweeps = int(CAPTURE_S / period_s)
    caps, arrs, by_time = [], [], {}
    with open(Path(tsv)) as fh:
        header = fh.readline().rstrip("\n").split("\t")
        for line in fh:
            f = dict(zip(header, line.rstrip("\n").split("\t")))
            if not f.get("time") or f["time"] == "time":
                continue
            if f["time"] not in by_time:
                loc = _locate(stamps, f["time"])
                if loc is None:
                    log.warning("%s: no capture file within %s s of %s", tsv, STAMP_TOLERANCE_S,
                                f["time"])
                    by_time[f["time"]] = None
                    continue
                t, name = loc
                age = float(f["anchor_age_s"])
                if minutes:
                    prov = provenance_at(minutes, _epoch(t))
                elif abs(age) <= ANCHOR_AGE_MAX_S:
                    prov = assumed_provenance("")
                else:
                    prov = null_provenance()
                cap = _capture(
                    time=t, reporter_id=reporter_id, centre_hz=centre_hz,
                    rate_hz_per_s=rate_hz_per_s, n_sweeps=n_sweeps, n_found=0, ring_ref=name,
                    engineering={"label_minus_t6_ms": f["label_minus_t6_ms"],
                                 "anchor_age_s": f["anchor_age_s"], "source": "relabel-tsv"},
                    **prov)
                by_time[f["time"]] = cap
                caps.append(cap)
            cap = by_time[f["time"]]
            if cap is None:
                continue
            cap["n_found"] += 1
            arrs.append(_arrival(
                time=cap["time"], reporter_id=reporter_id, group_key=gkey, centre_hz=centre_hz,
                rate_hz_per_s=rate_hz_per_s,
                delay_ns=round(float(f["phase_t6_ms"]) * 1e6) % period_ns,
                level_db=float(f["db"]), clean_tag=_clean_tag_tsv(f["tag"]),
                engineering={"phase_label_ms": f["phase_label_ms"], "ring_ref": cap["ring_ref"],
                             "anchor_age_s": f["anchor_age_s"]},
                **{k: cap[k] for k in null_provenance()}))
    return caps, arrs


def _clean_tag_tsv(tag):
    return tag if tag in ("A", "R", "m", "M") else _clean_tag(tag)


def _parse_group(name):
    if name in EXACT_GROUPS:
        return EXACT_GROUPS[name]
    parts = name.split()
    return round(float(parts[-2]) * 1e6, 3), round(float(parts[-1]) * 1e3, 3)


def read_res_jsonl(path, reporter_id, minutes):
    """(captures, arrivals) from an analyze.py results file: one capture per row per group.

    Only arrivals with phase_t6_ms become rows (a capture with no T6 label has none).  Provenance
    comes from `minutes` for rows whose T6 label held, the null block otherwise.
    """
    caps, arrs = [], []
    with open(Path(path)) as fh:
        for line in fh:
            line = line.strip()
            if not line:
                continue
            r = json.loads(line)
            on_t6 = r.get("t6") == "T6"
            lab = float(r.get("label_minus_t6_ms") or 0.0) if on_t6 else 0.0
            t_ms = round((r["t0"] - lab * 1e-3) * 1000)
            start = datetime(1970, 1, 1) + timedelta(milliseconds=t_ms)
            second = start.replace(microsecond=0)
            prov = provenance_at(minutes, r["t0"] - lab * 1e-3) if on_t6 else null_provenance()
            for gname, g in r["groups"].items():
                centre, rate = _parse_group(gname)
                period = PERIOD_13MHZ_S if centre > 1e7 else PERIOD_4463_S
                period_ns = round(period * 1e9)
                margin = g["peak_db"] - g["null_max"]
                found = [a for a in g.get("arrivals", []) if "phase_t6_ms" in a]
                eng = {"tier": str(r.get("tier_label", "")), "t6": str(r.get("t6", "")),
                       "band": str(r.get("band", "")), "group": gname,
                       "anchor_age_s": str(r.get("anchor_age_s", "")),
                       "label_minus_t6_ms": str(r.get("label_minus_t6_ms", "")),
                       "peak_lag_ms": str(g.get("peak_lag_ms", "")),
                       "raw_detected": str(g.get("raw_detected", "")),
                       "detected": str(g.get("detected", ""))}
                caps.append(_capture(
                    time=start, reporter_id=reporter_id, centre_hz=centre, rate_hz_per_s=rate,
                    n_sweeps=int(CAPTURE_S / period), expected={gname: margin},
                    null_max_db=g["null_max"], null_median_db=g["null_med"], n_expected=1,
                    n_found=len(found), ring_ref=r["file"], engineering=eng, **prov))
                for a in found:
                    arrs.append(_arrival(
                        time=second, reporter_id=reporter_id, group_key=_group_key(centre, rate),
                        centre_hz=centre, rate_hz_per_s=rate,
                        delay_ns=round(a["phase_t6_ms"] * 1e6) % period_ns,
                        level_db=a["db"], margin_db=a["db"] - g["null_max"],
                        half_band_db=(a["halves"][0] - a["halves"][1]) if a.get("halves") else None,
                        clean_tag=_clean_tag(a.get("tag", "")),
                        engineering={"tau_ms": str(a.get("tau_ms", "")),
                                     "phase_label_ms": str(a.get("phase_label_ms", "")),
                                     "full": str(a.get("full", "")), "tier": eng["tier"],
                                     "ring_ref": r["file"], "tag": a.get("tag", "")},
                        **prov))
    return caps, arrs
