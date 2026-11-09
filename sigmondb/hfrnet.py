"""HFRNet station list -> sigmondb transmitter and waveform rows.

Frequency is MHz, Bandwidth is kHz (signed: negative sweeps down), SweepRate is Hz.
A row that cannot be read is skipped and named in one WARNING; no row aborts the run.
"""
import csv
import logging
from datetime import date, datetime, timedelta
from pathlib import Path

from sigmondb.ddl import COLUMNS

log = logging.getLogger(__name__)


def _blank(table):
    return {c: None for c in COLUMNS[table]}


def parse_hfrnet(path, snapshot, active_days=14):
    """Return (transmitter rows, waveform rows).  valid_from = snapshot midnight (UTC, naive)."""
    valid_from = datetime(snapshot.year, snapshot.month, snapshot.day)
    tx_rows, wf_rows, skipped = [], [], []
    with open(Path(path), newline="", encoding="utf-8") as fh:
        for rec in csv.DictReader(fh):
            sta = (rec.get("Station") or "").strip()
            try:
                lat = float(rec["Latitude"])
                lon = float(rec["Longitude"])
                freq_hz = float(rec["Frequency"]) * 1e6
                bw = float(rec["Bandwidth"])
                rate = float(rec["SweepRate"])
                if rate <= 0 or not sta:
                    raise ValueError("bad sweep rate or station")
                last = date.fromisoformat(rec["Last Radial File"].strip())
            except (ValueError, KeyError, TypeError, AttributeError):
                skipped.append(sta or "?")
                continue
            tx_id = f"hfrnet:{sta}"
            listed = (snapshot - last) <= timedelta(days=active_days)
            t = _blank("transmitter")
            t.update(tx_id=tx_id, kind="codar", network=(rec.get("Network") or "").strip(),
                     name=(rec.get("Station Name") or "").strip(),
                     status="listed" if listed else "retired", lat=lat, lon=lon,
                     pos_source="hfrnet", valid_from=valid_from, version=1, source="hfrnet")
            w = _blank("waveform")
            period = 1.0 / rate
            w.update(tx_id=tx_id, valid_from=valid_from, centre_hz=freq_hz,
                     rate_hz_per_s=bw * 1e3 * rate, period_s=period, duration_s=period,
                     bandwidth_hz=abs(bw) * 1e3, source="hfrnet")
            tx_rows.append(t)
            wf_rows.append(w)
    if skipped:
        log.warning("hfrnet: skipped %d unreadable row(s): %s", len(skipped), ", ".join(skipped))
    return tx_rows, wf_rows
