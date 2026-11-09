"""Receiver identity from sigmondb/seed/receivers.toml."""
import tomllib
from datetime import datetime
from pathlib import Path

from sigmondb.ddl import COLUMNS

SEED_VALID_FROM = datetime(2026, 11, 9)  # UTC, naive


def grid_to_latlon(grid):
    """Centre of a Maidenhead square; length 4, 6, 8 or 10."""
    g = grid.strip()
    if len(g) not in (4, 6, 8, 10):
        raise ValueError(f"bad grid length: {grid!r}")
    g = g[:2].upper() + g[2:4] + g[4:].lower()
    lon = (ord(g[0]) - 65) * 20.0 - 180.0
    lat = (ord(g[1]) - 65) * 10.0 - 90.0
    lon_w, lat_w = 20.0, 10.0
    for i in range(2, len(g), 2):
        a, b = g[i], g[i + 1]
        if i % 4 == 2:  # digit pair
            lon_w, lat_w = lon_w / 10.0, lat_w / 10.0
            lon += int(a) * lon_w
            lat += int(b) * lat_w
        else:           # letter pair
            lon_w, lat_w = lon_w / 24.0, lat_w / 24.0
            lon += (ord(a) - 97) * lon_w
            lat += (ord(b) - 97) * lat_w
    return lat + lat_w / 2.0, lon + lon_w / 2.0


def load_receivers(path):
    """Return receiver rows, one per [[receiver]] table, every COLUMNS key present."""
    with open(Path(path), "rb") as fh:
        doc = tomllib.load(fh)
    rows = []
    for r in doc.get("receiver", []):
        lat, lon = grid_to_latlon(r["grid"])
        row = {c: None for c in COLUMNS["receiver"]}
        row.update(reporter_id=r["reporter_id"], host_id=r["host_id"], radiod_id=r["radiod_id"],
                   lat=lat, lon=lon, pos_source="typed_grid",
                   antenna=r.get("antenna", ""), receiver_chain=r.get("receiver_chain", ""),
                   coherence_group=r.get("coherence_group", ""), a_level="",
                   valid_from=SEED_VALID_FROM, version=1)
        rows.append(row)
    return rows
