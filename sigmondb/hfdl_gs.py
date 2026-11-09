"""HFDL ground stations from ka9q-radio's systable.conf -> transmitter and waveform rows.

Only id, lat, lon, frequencies (kHz) and name inside the stations list are read.
"""
import re
import logging
from datetime import datetime
from pathlib import Path

from sigmondb.ddl import COLUMNS

log = logging.getLogger(__name__)

# Riverhead (id 4) and Molokai (id 2): (step ns, modulus ns), hfdl-timing/keying/README.md
KEYING = {4: (8_408_600.0, 65_992_600.0), 2: (-408_200.0, 51_569_000.0)}
FRAME_S = 32.0

_BLOCK = re.compile(r"\{([^{}]*)\}", re.S)


def _field(body, name):
    m = re.search(rf"\b{name}\s*=\s*([^;]+);", body)
    return m.group(1).strip() if m else None


def parse_systable(path, snapshot, keying=KEYING):
    """Return (transmitter rows, waveform rows).  valid_from = snapshot midnight (UTC, naive)."""
    p = Path(path)
    text = p.read_text(encoding="utf-8")
    valid_from = datetime(snapshot.year, snapshot.month, snapshot.day)
    tx_rows, wf_rows, skipped = [], [], []
    for body in _BLOCK.findall(text):
        gid, lat, lon = _field(body, "id"), _field(body, "lat"), _field(body, "lon")
        freqs, name = _field(body, "frequencies"), _field(body, "name")
        if None in (gid, lat, lon, freqs):
            skipped.append((name or gid or "?").strip('"'))
            continue
        gid = int(gid)
        tx_id = f"hfdl:{gid}"
        t = {c: None for c in COLUMNS["transmitter"]}
        t.update(tx_id=tx_id, kind="hfdl", network="hfdl", name=(name or "").strip('"'),
                 status="listed", lat=float(lat), lon=float(lon), pos_source="systable",
                 valid_from=valid_from, version=1, source="systable")
        tx_rows.append(t)
        step, mod = keying.get(gid, (None, None))
        for khz in re.findall(r"[-+]?\d+(?:\.\d+)?", freqs.strip("() ")):
            w = {c: None for c in COLUMNS["waveform"]}
            w.update(tx_id=tx_id, valid_from=valid_from, centre_hz=float(khz) * 1e3,
                     rate_hz_per_s=0.0, period_s=FRAME_S, frame_s=FRAME_S,
                     keying_step_ns=step, keying_modulus_ns=mod, source="systable")
            wf_rows.append(w)
    if skipped:
        log.warning("systable: skipped %d incomplete station block(s): %s",
                    len(skipped), ", ".join(skipped))
    return tx_rows, wf_rows
