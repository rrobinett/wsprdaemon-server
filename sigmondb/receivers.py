"""Receiver identity from sigmondb/seed/receivers.toml."""
import json
import math
import os
import struct
import tomllib
from datetime import datetime
from pathlib import Path

from sigmondb.ddl import COLUMNS

SEED_VALID_FROM = datetime(2026, 11, 9)  # UTC, naive
EARTH_R_M = 6_371_008.8


def _grid_cell(grid):
    """(centre lat, centre lon, cell height deg, cell width deg) of a Maidenhead locator."""
    g = grid.strip()
    if len(g) not in (2, 4, 6, 8, 10):
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
    return lat + lat_w / 2.0, lon + lon_w / 2.0, lat_w, lon_w


def grid_to_latlon(grid):
    """Centre of a Maidenhead square; length 2, 4, 6, 8 or 10."""
    lat, lon, _, _ = _grid_cell(grid)
    return lat, lon


def grid_pos_sigma_m(grid):
    """Half the diagonal of the grid cell, in metres: the farthest a station can sit from the
    centre.  North-south side R*dlat; east-west side R*cos(lat)*dlon at the centre's latitude."""
    lat, _, lat_w, lon_w = _grid_cell(grid)
    ns = EARTH_R_M * math.radians(lat_w)
    ew = EARTH_R_M * math.cos(math.radians(lat)) * math.radians(lon_w)
    return 0.5 * math.hypot(ns, ew)


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
                   pos_sigma_m=grid_pos_sigma_m(r["grid"]),
                   antenna=r.get("antenna", ""), receiver_chain=r.get("receiver_chain", ""),
                   coherence_group=r.get("coherence_group", ""), a_level="",
                   valid_from=SEED_VALID_FROM, version=1)
        rows.append(row)
    return rows


_POS_KEYS = ("lat", "lon", "pos_sigma_m")
# wd30 answers anonymous queries, so sigmondb.receiver holds no house-level position: a GPSDO
# fix enters rounded to POS_DECIMALS (0.01 degree, about 1 km; Michael, 2026-11-14), and the
# precise value stays only in the private positions file.  Rounding to that cell leaves an
# error of about ROUNDING_SIGMA_M rms (0.01/sqrt(12) degree per axis, both axes), which
# pos_sigma_m carries so the solvers weigh it.
POS_DECIMALS = 2
ROUNDING_SIGMA_M = 400.0
_POS_LIMITS = {"lat": (-90.0, 90.0), "lon": (-180.0, 180.0), "pos_sigma_m": (0.0, math.inf)}


def _f32(x):
    """x rounded to float32: pos_sigma_m is Nullable(Float32) in the database."""
    return None if x is None else struct.unpack("f", struct.pack("f", x))[0]


def load_positions(path):
    """GPSDO positions from a private JSON file: {reporter_id: {lat, lon, pos_sigma_m}}.
    The file must grant nothing to group or other.  Errors name the reporter and the key,
    never a value."""
    path = Path(path)
    mode = os.stat(path).st_mode & 0o777
    if mode & 0o077:
        raise ValueError("positions file %s has mode %03o; it must grant nothing to group "
                         "or other (chmod 600)" % (path, mode))
    with open(path) as fh:
        doc = json.load(fh)
    if not isinstance(doc, dict):
        raise ValueError("positions file %s: expected a JSON object keyed by reporter id" % path)
    out = {}
    for rid, p in doc.items():
        for k in _POS_KEYS:
            if not isinstance(p, dict) or p.get(k) is None:
                raise ValueError("positions entry %s lacks %s" % (rid, k))
        v = {}
        for k in _POS_KEYS:
            try:
                if isinstance(p[k], (str, bool)):
                    raise TypeError
                v[k] = float(p[k])
            except (TypeError, ValueError):
                raise ValueError("positions entry %s: %s must be a number" % (rid, k)) from None
            lim = _POS_LIMITS[k]
            if not math.isfinite(v[k]) or not lim[0] <= v[k] <= lim[1]:
                raise ValueError("positions entry %s: %s is out of range" % (rid, k))
        out[rid] = v
    return out


DEFAULT_POSITIONS = "~/.config/sigmondb-receiver-positions.json"


def overlay_positions(rows, positions):
    """Copies of `rows`; each row whose reporter_id is in `positions` takes that entry's lat,
    lon and pos_sigma_m, unrounded, and pos_source gpsdo.  The input stays untouched.  The
    caller keeps the result in memory: precise values never go to the database or a file."""
    out = []
    for r in rows:
        r = dict(r)
        p = positions.get(r.get("reporter_id"))
        if p is not None:
            r.update(lat=p["lat"], lon=p["lon"], pos_sigma_m=p["pos_sigma_m"],
                     pos_source="gpsdo")
        out.append(r)
    return out


def position_rows(latest_rows, positions, now):
    """(new receiver rows, unknown reporter ids).  Each reporter in `positions` that has a row
    gets a copy of its latest row at the new position, rounded to POS_DECIMALS, with
    pos_sigma_m widened by ROUNDING_SIGMA_M, pos_source gpsdo, version + 1.  A reporter without
    a row goes to the second list and creates none.  An equal rounded position adds no
    version."""
    from sigmondb.groundwave import _latest
    latest = _latest(latest_rows, "reporter_id")
    now = now.replace(microsecond=0)
    rows, unknown = [], []
    for rid, precise in positions.items():
        old = latest.get(rid)
        if old is None:
            unknown.append(rid)
            continue
        p = dict(lat=round(precise["lat"], POS_DECIMALS), lon=round(precise["lon"], POS_DECIMALS),
                 pos_sigma_m=math.hypot(precise["pos_sigma_m"], ROUNDING_SIGMA_M))
        if (old.get("lat"), old.get("lon"), _f32(old.get("pos_sigma_m"))) == (
                p["lat"], p["lon"], _f32(p["pos_sigma_m"])):
            continue
        new = dict(old)
        new.update(lat=p["lat"], lon=p["lon"], pos_sigma_m=p["pos_sigma_m"],
                   pos_source="gpsdo", valid_from=now, version=(old.get("version") or 0) + 1)
        rows.append(new)
    return rows, unknown
