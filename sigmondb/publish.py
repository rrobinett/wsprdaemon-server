"""Build the sigmondb_public tables and replace each one WHOLE on every public host.

wd30 runs this hourly (sigmondb-publish.timer).  It reads wd30's own sigmondb, builds the
small tables, and swaps each into sigmondb_public on WD10 and WD20, where stations read them
anonymously.  A station never sees half an update: each table is built under <t>__new and
then exchanged with <t> in one step.  hf-timestd's T3 Phase B consumes them
(hf-timestd/docs/superpowers/plans/2026-11-13-t3-automatic-admission.md, Tasks B1, B4, B5).

Every row carries built_at_utc, unix seconds, one value for the whole build.

transmitter_offsets: the latest solution per tx_id (by window_to) that has an offset.
  confirming_reporters is that solution's reporter_ids, sorted and deduplicated, so a
  consumer can join trust to the stations that confirmed the offset.
  SIGN.  The solver (groundwave.ground_wave_solutions) writes
  offset_ns = fixed_delay_ns - distance/c, and an arrival's delay_ns is its phase AFTER
  the sweep start on UTC.  So offset_ns is the time the transmitter emitted minus the UTC
  instant it should have: a transmitter keying 2.5 ms late gives +2_500_000.  The published
  offset_ms keeps that sign: EMISSION MINUS UTC, positive when the transmitter runs late.
  RANGE.  An offset is a phase modulo the sweep period P: a transmitter 41 ms early and one
  959 ms late look alike on a 1 s sweep.  The solver gives the value nearest zero, in
  [-P/2, P/2) (groundwave-4 on; earlier solver versions gave [-d/c, P - d/c)).  period_s
  carries P, read from the solution's group key (half_sweep.period_and_sweep); it is NULL
  when the key gives no period, and that offset is unfolded.  THE FOLD a consumer must apply:
  pick the sweep start nearest the arrival, i.e. fold its residual modulo P into [-P/2, P/2).
  A consumer that instead labels each arrival with the sweep start before it (the floor, as a
  recorder counts delay_ns) misplaces it by exactly P whenever the offset is negative.

trust: one row per reporter, from its latest receiver version.
  RULE.  trusted = 1 when a_level is "A1", else 0.  a_level names the state of the
  station's ruler (hf-timestd MEASUREMENT_MODEL.md section 2; gpsdo_probe.poll()):
  "A1" means a GPSDO governs the ADC's sample rate, "A0" means it runs free.  Only a
  governed ruler holds a registration to the microsecond between anchors, so only an A1
  station can confirm a transmitter offset.  An empty a_level (the seed writes "") is an
  unstated ruler, and the model counts an unstated ruler as undisciplined: 0.
  A1 is necessary, not sufficient: the solver still admits only arrivals whose
  u_epoch_ns <= 1000 (groundwave.U_MAX_NS).

receivers: PRIVACY.  Stations sit at operators' homes.  Position goes out only as the
  6-character Maidenhead square and that square's centre, never the stored lat/lon.
  A receiver with no position is left out.

transmitters: the latest version per tx_id, at full precision (public infrastructure),
  except a row the chirp-state job discovered (source = 'discovered'): its position goes out
  only as the centre of its 4-character grid square, as for an unnamed chirp, so this table
  cannot leak what chirps hides.  groups, heard and ionosphere_hourly place such a
  transmitter at that same centre.

chirps: the established and dormant chirps, never HFDL.  A named chirp (tx_id present and not
  starting "cand:") publishes its position at full precision, pos_precision "full".  An
  unnamed one publishes the centre of its 4-character grid square, pos_precision "grid4", and
  a pos_sigma_m no smaller than that square's half-diagonal.  offset_ms keeps the sign of
  transmitter_offsets: emission minus UTC.

chirp_reject_local: the active local rejects; who set one stays on wd30.

chirp_menu: each receiver's channel menu (chirp_menu.channel_menu) for every UTC hour 0-23,
  built from the receiver's grid6 centre, never its stored position.  The rows carry no
  position and no distance.

The three chirp tables build through _optional: a failure leaves the public hosts' last copy
in place and never stops the core tables.

t3_calibration: no source yet; published EMPTY with its schema so clients see a valid table.

The publisher's credentials come from the environment (CH_PUB_USER, CH_PUB_PASSWORD,
CH_PUB_HOSTS); the password goes only into the Basic auth header and is scrubbed from
every message this module prints or returns.
"""
import base64
import json
import math
import urllib.error
import urllib.parse
import urllib.request
from datetime import datetime, timezone

from sigmondb import chirp_evidence as ce
from sigmondb.cli import _ATTR_ARRIVAL_COLS, _ATTR_WHERE
from sigmondb.groundwave import _latest, great_circle_m
from sigmondb.half_sweep import period_and_sweep
from sigmondb.paths import gc_midpoint, match_group, path_kind
from sigmondb.receivers import grid_pos_sigma_m, grid_to_latlon
from sigmondb.set_aside import not_set_aside, solution_not_set_aside

DATABASE = "sigmondb_public"
TABLES = ("transmitter_offsets", "trust", "receivers", "transmitters", "groups", "heard",
          "t3_calibration", "ionosphere_hourly", "chirps", "chirp_reject_local", "chirp_menu")
NF64 = "Nullable(Float64)"
SCHEMAS = {
    "transmitter_offsets": [
        ("tx_id", "String"), ("kind", "String"), ("offset_ms", "Float64"),
        ("sigma_ms", "Float64"), ("last_confirmed_utc", "Float64"),
        ("n_confirming", "UInt16"), ("confirming_reporters", "Array(String)"),
        ("period_s", NF64), ("built_at_utc", "Float64")],
    "trust": [
        ("reporter_id", "String"), ("a_level", "String"), ("trusted", "UInt8"),
        ("built_at_utc", "Float64")],
    "receivers": [
        ("reporter_id", "String"), ("grid6", "String"), ("lat", "Float64"),
        ("lon", "Float64"), ("a_level", "String"), ("built_at_utc", "Float64")],
    "transmitters": [
        ("tx_id", "String"), ("kind", "String"), ("network", "String"), ("name", "String"),
        ("status", "String"), ("lat", NF64), ("lon", NF64), ("pos_sigma_m", NF64),
        ("class", "String"), ("family", "String"), ("centre_mhz", NF64),
        ("rate_khz_s", NF64), ("built_at_utc", "Float64")],
    "groups": [
        ("group_id", "String"), ("class", "String"), ("family", "String"),
        ("name", "String"), ("centre_mhz", "Float64"), ("rate_khz_s", "Float64"),
        ("coast", "String"), ("both_coasts", "UInt8"), ("members", "Array(String)"),
        ("built_at_utc", "Float64")],
    "heard": [
        ("reporter_id", "String"), ("target_id", "String"), ("target_type", "String"),
        ("path", "String"), ("distance_km", "Float64"), ("centre_mhz", "Float64"),
        ("n_24h", "UInt32"), ("t6_24h", "UInt32"), ("n_7d", "UInt32"), ("t6_7d", "UInt32"),
        ("first_utc", "Float64"), ("last_utc", "Float64"), ("built_at_utc", "Float64")],
    "t3_calibration": [
        ("station", "String"), ("reference", "String"), ("hour_utc", "UInt8"),
        ("n", "UInt32"), ("rms_z", "Float64"), ("floor_fraction", "Float64"),
        ("honest", "UInt8"), ("built_at_utc", "Float64")],
    "ionosphere_hourly": [
        ("reporter_id", "String"), ("target_id", "String"), ("target_type", "String"),
        ("hour_utc", "Float64"), ("n", "UInt32"),
        ("midpoints", "Array(Tuple(Float64, Float64))"),
        ("doppler_med", NF64), ("doppler_iqr", NF64), ("s4_med", NF64), ("s4_iqr", NF64),
        ("sphi_med", NF64), ("sphi_iqr", NF64), ("level_med", NF64), ("level_iqr", NF64),
        ("built_at_utc", "Float64")],
    "chirps": [
        ("chirp_id", "String"), ("group_key", "String"), ("kind", "String"),
        ("class", "String"), ("family", "String"), ("state", "String"),
        ("centre_mhz", "Float64"), ("span_khz", NF64), ("rate_khz_s", "Float64"),
        ("period_s", NF64), ("duration_s", NF64), ("lat", NF64), ("lon", NF64),
        ("pos_sigma_m", NF64), ("pos_source", "String"), ("pos_precision", "String"),
        ("offset_ms", NF64), ("sigma_ms", NF64), ("last_heard_utc", NF64),
        ("n_stations", "UInt16"), ("heard_hours", "UInt32"), ("tx_id", "String"),
        ("built_at_utc", "Float64")],
    "chirp_reject_local": [
        ("reporter_id", "String"), ("centre_mhz", "Float64"), ("rate_khz_s", "Float64"),
        ("period_s", "Float64"), ("span_khz", NF64), ("chirp_id", "String"),
        ("reason", "String"), ("built_at_utc", "Float64")],
    "chirp_menu": [
        ("reporter_id", "String"), ("hour_utc", "UInt8"), ("channel_key", "String"),
        ("centre_mhz", "Float64"), ("rate_khz_s", "Float64"), ("period_s", NF64),
        ("chirp_ids", "Array(String)"), ("states", "Array(String)"),
        ("timing_value", "Float64"), ("exploration_value", "Float64"), ("cost", "Float64"),
        ("shared_t6", "UInt16"), ("expected_snr_db", NF64), ("path", "Nullable(String)"),
        ("rank_timing", "UInt16"), ("rank_explore", "UInt16"), ("built_at_utc", "Float64")],
}

# What read_source() takes from wd30's sigmondb, each table read FINAL.
SOURCE_COLUMNS = {
    "solution": ("tx_id", "solve_id", "window_to", "offset_ns", "offset_sigma_ns",
                 "n_stations", "reporter_ids", "cluster_key"),
    "transmitter": ("tx_id", "kind", "network", "name", "status", "lat", "lon",
                    "pos_sigma_m", "source", "valid_from", "version"),
    "receiver": ("reporter_id", "lat", "lon", "a_level", "radiod_id", "coherence_group",
                 "valid_from", "version"),
    "waveform": ("tx_id", "centre_hz", "rate_hz_per_s", "valid_from"),
    "chirp": ("chirp_id", "group_key", "kind", "state", "centre_hz", "span_hz",
              "rate_hz_per_s", "period_s", "duration_s", "gate_period_s", "lat", "lon",
              "pos_sigma_m", "pos_source", "offset_ns", "offset_sigma_ns", "last_heard",
              "n_stations", "heard_hours", "tx_id"),
    "chirp_reject_local": ("reporter_id", "centre_hz", "rate_hz_per_s", "period_s", "span_hz",
                           "chirp_id", "reason", "active"),
}
# A failed read of one of these leaves only the chirp tables out of the run.
OPTIONAL_SOURCES = ("chirp", "chirp_reject_local")
# A solution any of whose reporters holds an active 'codar' or 'all' set-aside window over the
# solution's span never reaches transmitter_offsets (sigmondb/set_aside.py).
SOURCE_WHERE = {"solution": "WHERE offset_ns IS NOT NULL AND tx_id != '' AND "
                            + solution_not_set_aside("codar"),
                "chirp": "WHERE state IN ('candidate', 'probing', 'established', 'dormant') "
                         "AND kind != 'hfdl' AND NOT startsWith(group_key, 'hfdl') "
                         "AND tx_id NOT LIKE 'hfdl:%'",
                "chirp_reject_local": "WHERE active = 1"}

# Aggregates read_source runs on wd30: name -> (columns, SQL).  HFDL stays out of every one,
# and so does every arrival inside an active 'codar' or 'all' set-aside window.
_CODAR_KEPT = ("NOT startsWith(group_key, 'hfdl') AND tx_id NOT LIKE 'hfdl:%' AND "
             + not_set_aside("codar"))
AGG_QUERIES = {
    "heard": (
        ("reporter_id", "tx_id", "group_key", "centre_hz", "rate_hz_per_s",
         "n_24h", "t6_24h", "n_7d", "t6_7d", "first_t", "last_t"),
        "/* agg:heard */ SELECT reporter_id, tx_id, group_key, "
        "any(centre_hz) AS centre_hz, any(rate_hz_per_s) AS rate_hz_per_s, "
        "countIf(time > now() - INTERVAL 24 HOUR) AS n_24h, "
        "countIf(time > now() - INTERVAL 24 HOUR AND origin = 't6_anchor') AS t6_24h, "
        "count() AS n_7d, countIf(origin = 't6_anchor') AS t6_7d, "
        "min(time) AS first_t, max(time) AS last_t "
        "FROM sigmondb.arrival FINAL WHERE time > now() - INTERVAL 7 DAY AND " + _CODAR_KEPT +
        " GROUP BY reporter_id, tx_id, group_key"),
    "ionosphere": (
        ("reporter_id", "tx_id", "group_key", "centre_hz", "rate_hz_per_s", "hour", "n",
         "dq", "sq", "pq", "lq"),
        "/* agg:ionosphere */ SELECT reporter_id, tx_id, group_key, "
        "any(centre_hz) AS centre_hz, any(rate_hz_per_s) AS rate_hz_per_s, "
        "toStartOfHour(time) AS hour, count() AS n, "
        "quantilesIf(0.25, 0.5, 0.75)(doppler_hz, isNotNull(doppler_hz)) AS dq, "
        "quantilesIf(0.25, 0.5, 0.75)(s4, isNotNull(s4)) AS sq, "
        "quantilesIf(0.25, 0.5, 0.75)(sigma_phi_rad, isNotNull(sigma_phi_rad)) AS pq, "
        "quantilesIf(0.25, 0.5, 0.75)(level_db, isNotNull(level_db)) AS lq "
        "FROM sigmondb.arrival FINAL WHERE time > now() - INTERVAL 24 HOUR AND " + _CODAR_KEPT +
        " GROUP BY reporter_id, tx_id, group_key, hour"),
}

# The channel menu's aggregates, read in the same loop with chirp_evidence.agg_params(rules)
# as their {hours:UInt32}: the sweep and capture aggregates the chirp-state job reads, and the
# T6 arrivals it reads (cli._ATTR_WHERE), HFDL kept out.
CHIRP_AGG_QUERIES = {
    "sweep_hourly": ce.AGG_SQL["sweep_hourly"],
    "capture_recent": ce.AGG_SQL["capture_recent"],
    "t6_arrival": (
        _ATTR_ARRIVAL_COLS,
        "/* agg:t6_arrival */ SELECT %s FROM sigmondb.arrival FINAL %s AND "
        "NOT startsWith(group_key, 'hfdl') AND tx_id NOT LIKE 'hfdl:%%'"
        % (", ".join(_ATTR_ARRIVAL_COLS), _ATTR_WHERE)),
}


# --- conversions ----------------------------------------------------------------------------

def emission_minus_utc_ms(offset_ns):
    """solution.offset_ns (emission minus UTC, ns) in ms, sign kept."""
    return offset_ns / 1e6


def sweep_period_s(cluster_key):
    """The sweep period, in s, of a solution's cluster_key "<reporter>|<group key>|<us>"
    (groundwave.fixed_clusters), or None when the key gives none."""
    parts = str(cluster_key or "").rsplit("|", 2)
    if len(parts) != 3:
        return None
    ps = period_and_sweep(parts[1])
    return ps[0] if ps else None


def _unix(t):
    """A datetime as unix seconds; a naive one counts as UTC (the registry stores UTC)."""
    if t.tzinfo is None:
        t = t.replace(tzinfo=timezone.utc)
    return t.timestamp()


def latlon_to_grid6(lat, lon):
    """The 6-character Maidenhead square holding (lat, lon)."""
    lat = min(max(lat, -90.0), 90.0 - 1e-9)
    lon = ((lon + 180.0) % 360.0) - 180.0 if lon != 180.0 else 180.0 - 1e-9
    x, y = lon + 180.0, lat + 90.0
    fx, fy = int(x // 20), int(y // 10)
    sx, sy = int((x % 20) // 2), int(y % 10)
    ux, uy = int((x % 2) * 12), int((y % 1) * 24)
    return "%s%s%d%d%s%s" % (chr(65 + fx), chr(65 + fy), sx, sy, chr(97 + ux), chr(97 + uy))


def latlon_to_grid4(lat, lon):
    """The 4-character Maidenhead square holding (lat, lon)."""
    return latlon_to_grid6(lat, lon)[:4]


def named(tx_id):
    """A chirp or transmitter is named when its tx_id exists and does not start "cand:"."""
    return bool(tx_id) and not str(tx_id).startswith("cand:")


def grid4_position(lat, lon, pos_sigma_m):
    """(lat, lon, pos_sigma_m) as an unnamed position leaves wd30: the centre of its grid4
    square, and a sigma no smaller than that square's half-diagonal at its centre latitude."""
    g = latlon_to_grid4(lat, lon)
    clat, clon = grid_to_latlon(g)
    return clat, clon, max(float(pos_sigma_m or 0.0), grid_pos_sigma_m(g))


# --- builders -----------------------------------------------------------------------------

# A solution counts only when its window_to lies within this many hours before the build
# (ruling A11).  A withdrawn or aged-out name therefore leaves transmitter_offsets within 3 h.
# A transmitter that nothing has confirmed for 3 h drops out of the table: nothing vouches for
# its offset any more.
PUBLISH_MAX_AGE_H = 3


def build_transmitter_offsets(solutions, transmitters, built_at):
    txs = _latest(transmitters or [], "tx_id")
    oldest = built_at - PUBLISH_MAX_AGE_H * 3600.0
    best = {}
    for s in solutions:
        if not s.get("tx_id") or s.get("offset_ns") is None or s.get("offset_sigma_ns") is None:
            continue
        if _unix(s["window_to"]) < oldest:
            continue
        k = (_unix(s["window_to"]), s.get("solve_id") or "")
        if s["tx_id"] not in best or k > best[s["tx_id"]][0]:
            best[s["tx_id"]] = (k, s)
    out = []
    for tx_id, (k, s) in sorted(best.items()):
        out.append(dict(
            tx_id=tx_id, kind=(txs.get(tx_id) or {}).get("kind") or "unknown",
            offset_ms=emission_minus_utc_ms(s["offset_ns"]),
            sigma_ms=s["offset_sigma_ns"] / 1e6, last_confirmed_utc=k[0],
            n_confirming=int(s.get("n_stations") or 0),
            confirming_reporters=sorted(set(s.get("reporter_ids") or [])),
            period_s=sweep_period_s(s.get("cluster_key")), built_at_utc=built_at))
    return out


def is_trusted(a_level):
    return 1 if (a_level or "").strip().upper() == "A1" else 0


def build_trust(receivers, built_at):
    return [dict(reporter_id=rid, a_level=r.get("a_level") or "",
                 trusted=is_trusted(r.get("a_level")), built_at_utc=built_at)
            for rid, r in sorted(_latest(receivers or [], "reporter_id").items())]


def build_receivers(receivers, built_at):
    out = []
    for rid, r in sorted(_latest(receivers or [], "reporter_id").items()):
        if r.get("lat") is None or r.get("lon") is None:
            continue
        g = latlon_to_grid6(r["lat"], r["lon"])
        lat, lon = grid_to_latlon(g)
        out.append(dict(reporter_id=rid, grid6=g, lat=lat, lon=lon,
                        a_level=r.get("a_level") or "", built_at_utc=built_at))
    return out


CLASS_OF = {"codar": "ocean radar", "oth": "over-the-horizon radar",
            "ionosonde": "chirp ionosonde", "unknown": "unknown",
            "hfdl": "HFDL ground station"}


def class_family(kind, network):
    """(class, family) for a transmitter kind.  Every codar row in sigmondb comes from a
    SeaSonde line, so that family is named; other kinds stay unspecified."""
    if kind not in CLASS_OF:
        return "unknown", "unspecified"
    if kind == "codar":
        return CLASS_OF[kind], "CODAR SeaSonde"
    return CLASS_OF[kind], "unspecified"


def _latest_waveforms(waveforms):
    latest_wf = {}
    for w in waveforms or ():
        cur = latest_wf.get(w["tx_id"])
        if cur is None or w["valid_from"] > cur["valid_from"]:
            latest_wf[w["tx_id"]] = w
    return latest_wf


def public_position(t):
    """A transmitter row's (lat, lon, pos_sigma_m) as published: a discovered row's at its
    grid4 centre (grid4_position), any other at full precision."""
    lat, lon, sig = t.get("lat"), t.get("lon"), t.get("pos_sigma_m")
    if t.get("source") == "discovered" and lat is not None and lon is not None:
        return grid4_position(lat, lon, sig)
    return lat, lon, sig


def build_transmitters(transmitters, built_at, waveforms=()):
    latest_wf = _latest_waveforms(waveforms)
    out = []
    for tx_id, t in sorted(_latest(transmitters or [], "tx_id").items()):
        lat, lon, sig = public_position(t)
        kind = t.get("kind") or ""
        cls, family = class_family(kind, t.get("network") or "")
        wf = latest_wf.get(tx_id) or {}
        centre, rate = wf.get("centre_hz"), wf.get("rate_hz_per_s")
        out.append({"tx_id": tx_id, "kind": kind, "network": t.get("network") or "",
                    "name": t.get("name") or "", "status": t.get("status") or "",
                    "lat": lat, "lon": lon,
                    "pos_sigma_m": None if sig is None else float(sig),
                    "class": cls, "family": family,
                    "centre_mhz": None if centre is None else float(centre) / 1e6,
                    "rate_khz_s": None if rate is None else float(rate) / 1e3,
                    "built_at_utc": built_at})
    return out


COAST_NAME = {"E": "east coast", "W": "west coast"}


def build_index(source):
    """What resolve_target needs: station grid6 centres by reporter, codar transmitters joined
    to their latest waveform, and every transmitter by id."""
    stations = {}
    for rid, r in _latest(source.get("receiver") or [], "reporter_id").items():
        if r.get("lat") is None or r.get("lon") is None:
            continue
        stations[rid] = grid_to_latlon(latlon_to_grid6(r["lat"], r["lon"]))
    txs = {}
    for tx_id, t in _latest(source.get("transmitter") or [], "tx_id").items():
        lat, lon, _sig = public_position(t)
        txs[tx_id] = dict(t, lat=lat, lon=lon)
    wfs = _latest_waveforms(source.get("waveform"))
    codar = []
    for tx_id, t in sorted(txs.items()):
        w = wfs.get(tx_id)
        if t.get("kind") != "codar" or w is None or t.get("lat") is None or t.get("lon") is None:
            continue
        codar.append({"tx_id": tx_id, "network": t.get("network") or "",
                      "lat": t["lat"], "lon": t["lon"],
                      "centre_hz": float(w["centre_hz"]),
                      "rate_hz_per_s": float(w["rate_hz_per_s"])})
    return {"stations": stations, "codar": codar, "txs": txs}


def resolve_target(row, index):
    """Where one aggregate row points, or None to skip it.  A named tx_id targets that tx;
    an empty one goes through match_group and targets a group, never a site.  The station
    position is its grid6 centre.  Extra key "group" holds the match_group result (or None)."""
    station = index["stations"].get(row.get("reporter_id"))
    if station is None:
        return None
    centre = row.get("centre_hz")
    if row.get("tx_id"):
        t = index["txs"].get(row["tx_id"])
        if t is None or t.get("lat") is None or t.get("lon") is None:
            return None
        pos = (t["lat"], t["lon"])
        return {"target_id": row["tx_id"], "target_type": "tx",
                "distance_km": great_circle_m(station[0], station[1], *pos) / 1000.0,
                "centre_hz": centre, "station": station, "member_positions": [pos],
                "group": None}
    rate = row.get("rate_hz_per_s")
    if centre is None or rate is None:
        return None
    m = match_group(row.get("group_key") or "", centre, rate, station, index["codar"])
    if m is None:
        return None
    by_id = {s["tx_id"]: s for s in index["codar"]}
    return {"target_id": m["group_id"], "target_type": "group",
            "distance_km": m["nearest_km"], "centre_hz": centre, "station": station,
            "member_positions": [(by_id[i]["lat"], by_id[i]["lon"]) for i in m["members"]],
            "group": m}


def build_groups_heard(source, built_at, out=print):
    """(groups rows, heard rows) from the heard aggregate."""
    index = build_index(source)
    by_id = {s["tx_id"]: s for s in index["codar"]}
    groups, heard, skipped = {}, {}, 0
    for row in source.get("agg:heard") or []:
        tgt = resolve_target(row, index)
        if tgt is None:
            skipped += 1
            continue
        m = tgt["group"]
        if m is not None and tgt["target_id"] not in groups:
            rate = float(row["rate_hz_per_s"])
            cls, family = class_family("codar", by_id[m["members"][0]]["network"])
            groups[tgt["target_id"]] = {
                "group_id": tgt["target_id"], "class": cls, "family": family,
                "name": "%.4f MHz, %+.2f kHz/s (%s)" % (
                    row["centre_hz"] / 1e6, rate / 1e3, COAST_NAME[m["coast"]]),
                "centre_mhz": row["centre_hz"] / 1e6, "rate_khz_s": rate / 1e3,
                "coast": m["coast"], "both_coasts": 1 if m["both_coasts"] else 0,
                "members": list(m["members"]), "built_at_utc": built_at}
        first, last = _unix(row["first_t"]), _unix(row["last_t"])
        key = (row["reporter_id"], tgt["target_id"])
        h = heard.get(key)
        if h is None:
            heard[key] = {
                "reporter_id": key[0], "target_id": key[1],
                "target_type": tgt["target_type"],
                "path": path_kind(tgt["distance_km"], tgt["centre_hz"]),
                "distance_km": tgt["distance_km"], "centre_mhz": tgt["centre_hz"] / 1e6,
                "n_24h": int(row["n_24h"]), "t6_24h": int(row["t6_24h"]),
                "n_7d": int(row["n_7d"]), "t6_7d": int(row["t6_7d"]),
                "first_utc": first, "last_utc": last, "built_at_utc": built_at}
        else:   # one tx heard under several group_keys
            for c in ("n_24h", "t6_24h", "n_7d", "t6_7d"):
                h[c] += int(row[c])
            h["first_utc"], h["last_utc"] = min(h["first_utc"], first), max(h["last_utc"], last)
    if skipped:
        out("groups/heard: skipped %d path(s) with no station position, no candidate "
            "or no transmitter position" % skipped)
    return ([groups[k] for k in sorted(groups)], [heard[k] for k in sorted(heard)])


def _med_iqr(q):
    """(median, q75 - q25) from a quantiles array; None, None when it is empty or NaN."""
    if not q or len(q) < 3 or any(v is None or math.isnan(v) for v in q[:3]):
        return None, None
    return float(q[1]), float(q[2]) - float(q[0])


def build_ionosphere(source, built_at, out=print):
    """ionosphere_hourly rows: skywave paths only, one midpoint per member."""
    index = build_index(source)
    rows, skipped = {}, 0
    for row in source.get("agg:ionosphere") or []:
        tgt = resolve_target(row, index)
        if tgt is None:
            skipped += 1
            continue
        if path_kind(tgt["distance_km"], tgt["centre_hz"]) != "skywave":
            continue
        st = tgt["station"]
        r = {"reporter_id": row["reporter_id"], "target_id": tgt["target_id"],
             "target_type": tgt["target_type"], "hour_utc": _unix(row["hour"]),
             "n": int(row["n"]),
             "midpoints": [list(gc_midpoint(st[0], st[1], lat, lon))
                           for lat, lon in tgt["member_positions"]]}
        for pre, col in (("doppler", "dq"), ("s4", "sq"), ("sphi", "pq"), ("level", "lq")):
            r[pre + "_med"], r[pre + "_iqr"] = _med_iqr(row.get(col))
        r["built_at_utc"] = built_at
        key = (r["reporter_id"], r["target_id"], r["hour_utc"])
        if key not in rows or r["n"] > rows[key]["n"]:   # one tx under several group_keys
            rows[key] = r
    if skipped:
        out("ionosphere: skipped %d path(s) with no station position, no candidate "
            "or no transmitter position" % skipped)
    return [rows[k] for k in sorted(rows)]


CHIRP_PUBLISHED_STATES = ("established", "dormant")


def _hfdl_chirp(c):
    return (c.get("kind") == "hfdl" or str(c.get("group_key") or "").startswith("hfdl")
            or str(c.get("tx_id") or "").startswith("hfdl:"))


def _scaled(v, k):
    return None if v is None else float(v) / k


def build_chirps(chirps, built_at):
    """chirps rows: established and dormant chirps, never HFDL, by chirp_id."""
    out = []
    for c in sorted(chirps or (), key=lambda c: c["chirp_id"]):
        if c.get("state") not in CHIRP_PUBLISHED_STATES or _hfdl_chirp(c):
            continue
        kind = c.get("kind") or ""
        cls, family = class_family(kind, "")
        lat, lon, sig = c.get("lat"), c.get("lon"), c.get("pos_sigma_m")
        if named(c.get("tx_id")):
            precision = "full"
        else:
            precision = "grid4"
            if lat is not None and lon is not None:
                lat, lon, sig = grid4_position(lat, lon, sig)
            else:
                lat = lon = None
        off = c.get("offset_ns")
        last = c.get("last_heard")
        out.append({
            "chirp_id": c["chirp_id"], "group_key": c.get("group_key") or "", "kind": kind,
            "class": cls, "family": family, "state": c["state"],
            "centre_mhz": float(c["centre_hz"]) / 1e6, "span_khz": _scaled(c.get("span_hz"), 1e3),
            "rate_khz_s": float(c["rate_hz_per_s"]) / 1e3, "period_s": c.get("period_s"),
            "duration_s": c.get("duration_s"), "lat": lat, "lon": lon,
            "pos_sigma_m": None if sig is None else float(sig),
            "pos_source": c.get("pos_source") or "", "pos_precision": precision,
            "offset_ms": None if off is None else emission_minus_utc_ms(off),
            "sigma_ms": _scaled(c.get("offset_sigma_ns"), 1e6),
            "last_heard_utc": None if last is None else _unix(last),
            "n_stations": int(c.get("n_stations") or 0),
            "heard_hours": int(c.get("heard_hours") or 0),
            "tx_id": c.get("tx_id") or "", "built_at_utc": built_at})
    return out


def build_chirp_reject_local(rejects, built_at):
    """chirp_reject_local rows: the active rejects; who set one is not published."""
    out = []
    for r in rejects or ():
        if not r.get("active", 1):
            continue
        out.append({"reporter_id": r["reporter_id"],
                    "centre_mhz": float(r["centre_hz"]) / 1e6,
                    "rate_khz_s": float(r["rate_hz_per_s"]) / 1e3,
                    "period_s": float(r["period_s"]), "span_khz": _scaled(r.get("span_hz"), 1e3),
                    "chirp_id": r.get("chirp_id") or "", "reason": r.get("reason") or "",
                    "built_at_utc": built_at})
    return sorted(out, key=lambda r: (r["reporter_id"], r["centre_mhz"], r["rate_khz_s"],
                                      r["period_s"]))


def build_chirp_menu(source, built_at, out=print):
    """chirp_menu rows: each published receiver's menu (build_receivers: its grid6 centre) for
    UTC hours 0-23.  One Hearing per run, gathered as the menu verb gathers it."""
    from sigmondb import chirp, chirp_job, chirp_menu
    rules = chirp.load_rules()
    now = datetime.fromtimestamp(built_at, timezone.utc).replace(tzinfo=None)
    naive = chirp_job._naive_row
    chirps = [naive(c) for c in source.get("chirp") or ()]
    rx = list(_latest(source.get("receiver") or [], "reporter_id").values())
    hearing, t6_stations = chirp_menu.fleet_hearing(
        chirps, [naive(r) for r in source.get("agg:sweep_hourly") or ()],
        [naive(r) for r in source.get("agg:capture_recent") or ()],
        chirp_job.t6_evidence(source.get("agg:t6_arrival") or (), rx), rx, rules, now)
    if hearing.unparsed_t6:
        out("chirp_menu: %d T6 arrival(s) with a group_key that did not parse count for no "
            "chirp" % hearing.unparsed_t6)
    rows = []
    for st in build_receivers(source.get("receiver"), built_at):
        rid = st["reporter_id"]
        station = dict(reporter_id=rid, lat=st["lat"], lon=st["lon"], is_t6=rid in t6_stations)
        for hour in range(24):
            for m in chirp_menu.channel_menu(station, hour, chirps, hearing, t6_stations, rules):
                rows.append({
                    "reporter_id": rid, "hour_utc": hour, "channel_key": m["channel_key"],
                    "centre_mhz": float(m["centre_hz"]) / 1e6,
                    "rate_khz_s": float(m["rate_hz_per_s"]) / 1e3, "period_s": m["period_s"],
                    "chirp_ids": list(m["chirp_ids"]), "states": list(m["states"]),
                    "timing_value": m["timing_value"],
                    "exploration_value": m["exploration_value"], "cost": m["cost"],
                    "shared_t6": m["shared_t6"], "expected_snr_db": m["expected_snr_db"],
                    "path": m["path"], "rank_timing": m["rank_timing"],
                    "rank_explore": m["rank_explore"], "built_at_utc": built_at})
    return rows


def _optional(tables, names, build, out):
    """Run a map builder on its own.  A failure logs one line and leaves its tables OUT of
    `tables`, so publish() skips them and the public hosts keep their last copy."""
    try:
        got = build()
    except Exception as e:
        out("%s: build FAILED, not published this run: %s: %s" % (
            "/".join(names), type(e).__name__, e))
        return
    for n, rows in zip(names, got):
        tables[n] = rows


def build_all(source, built_at, out=print):
    """{table: rows} in TABLES order.  groups, heard, ionosphere_hourly and the three chirp
    tables are left out when their source or aggregate was not read or their builder fails;
    the core tables always build."""
    core = {
        "transmitter_offsets": build_transmitter_offsets(
            source["solution"], source["transmitter"], built_at),
        "trust": build_trust(source["receiver"], built_at),
        "receivers": build_receivers(source["receiver"], built_at),
        "transmitters": build_transmitters(
            source["transmitter"], built_at, source.get("waveform", ())),
        "t3_calibration": [],
    }
    extra = {}
    if "agg:heard" in source:
        _optional(extra, ("groups", "heard"),
                  lambda: build_groups_heard(source, built_at, out), out)
    if "agg:ionosphere" in source:
        _optional(extra, ("ionosphere_hourly",),
                  lambda: (build_ionosphere(source, built_at, out),), out)
    if "chirp" in source:
        _optional(extra, ("chirps",), lambda: (build_chirps(source["chirp"], built_at),), out)
    if "chirp_reject_local" in source:
        _optional(extra, ("chirp_reject_local",),
                  lambda: (build_chirp_reject_local(source["chirp_reject_local"], built_at),),
                  out)
    if "chirp" in source and all("agg:" + n in source for n in CHIRP_AGG_QUERIES):
        _optional(extra, ("chirp_menu",),
                  lambda: (build_chirp_menu(source, built_at, out),), out)
    core.update(extra)
    return {t: core[t] for t in TABLES if t in core}


def read_source(client, out=print):
    """The SOURCE_COLUMNS tables from wd30's sigmondb, every table FINAL, and the aggregates
    as "agg:<name>".  A failed aggregate, or a failed read of an OPTIONAL_SOURCES table, is
    logged and left out; the chirp aggregates take chirp_evidence.agg_params(rules)."""
    log, out = out, {}
    hours = {}

    def run(sql, params=None):
        kw = {} if params is None else {"parameters": params}
        try:
            return client.query(sql, query_tz="UTC", **kw)
        except TypeError:   # clickhouse-connect without query_tz; _unix() treats naive as UTC
            return client.query(sql, **kw)

    def params_for(sql):
        if "{hours:UInt32}" not in sql:
            return None
        if not hours:
            from sigmondb.chirp import load_rules
            hours.update(ce.agg_params(load_rules()))
        return hours

    for table, cols in SOURCE_COLUMNS.items():
        sql = "SELECT %s FROM sigmondb.%s FINAL %s" % (
            ", ".join(cols), table, SOURCE_WHERE.get(table, ""))
        try:
            out[table] = [dict(zip(cols, r)) for r in run(sql).result_rows]
        except Exception as e:
            if table not in OPTIONAL_SOURCES:
                raise
            log("%s: read FAILED, table(s) built from it not published this run: %s: %s" % (
                table, type(e).__name__, e))
    for name, (cols, sql) in list(AGG_QUERIES.items()) + list(CHIRP_AGG_QUERIES.items()):
        try:
            out_rows = [dict(zip(cols, r)) for r in run(sql, params_for(sql)).result_rows]
        except Exception as e:   # the map must not stop the core tables
            log("agg:%s: read FAILED, table(s) not published this run: %s: %s" % (
                name, type(e).__name__, e))
            continue
        out["agg:" + name] = out_rows
    return out


# --- the public hosts ---------------------------------------------------------------------------

def _default_urlopen(req, timeout=None):
    return urllib.request.urlopen(req, timeout=timeout)


class PublishError(Exception):
    pass


class ChHttp:
    """POST statements to one ClickHouse HTTP interface, Basic auth from the arguments.
    The password never enters a URL or a message."""

    def __init__(self, base_url, user, password, timeout=30.0, urlopen=None):
        self.base = base_url.rstrip("/")
        self.password = password
        self.timeout = timeout
        self.urlopen = urlopen or _default_urlopen
        self._auth = "Basic " + base64.b64encode(
            ("%s:%s" % (user, password)).encode()).decode()

    def scrub(self, text):
        return text.replace(self.password, "***") if self.password else text

    def run(self, sql, data=None):
        if data is None:
            url, body = self.base + "/", sql.encode()
        else:
            url = self.base + "/?" + urllib.parse.urlencode({"query": sql})
            body = data.encode()
        req = urllib.request.Request(url, data=body, method="POST",
                                     headers={"Authorization": self._auth})
        try:
            with self.urlopen(req, timeout=self.timeout) as resp:
                return resp.read().decode()
        except urllib.error.HTTPError as e:
            try:
                detail = e.read().decode(errors="replace").strip() if e.fp else ""
            except Exception:
                detail = ""
            raise PublishError(self.scrub("HTTP %s %s %s" % (e.code, e.reason, detail)[:500])) \
                from None
        except Exception as e:
            raise PublishError(self.scrub("%s: %s" % (type(e).__name__, e))[:500]) from None


def _create_sql(name, schema, database=DATABASE):
    cols = ", ".join("`%s` %s" % (c, t) for c, t in schema)
    return "CREATE TABLE %s.%s (%s) ENGINE = MergeTree ORDER BY tuple()" % (database, name, cols)


def _jsonl(rows):
    return "".join(json.dumps(r, allow_nan=False, ensure_ascii=False) + "\n" for r in rows)


def replace_table(ch, table, rows, schema=None, database=DATABASE):
    """Build <table>__new, then swap it in: EXCHANGE when <table> exists, else RENAME.

    schema and database default to this module's own; fleetboard passes its own."""
    if schema is None:
        schema = SCHEMAS[table]
    new, live = "%s.%s__new" % (database, table), "%s.%s" % (database, table)
    data = _jsonl(rows)   # before any statement: unsendable rows touch nothing
    ch.run("DROP TABLE IF EXISTS %s" % new)
    ch.run(_create_sql(table + "__new", schema, database))
    if rows:
        ch.run("INSERT INTO %s FORMAT JSONEachRow" % new, data)
    if ch.run("EXISTS TABLE %s" % live).strip() == "1":
        ch.run("EXCHANGE TABLES %s AND %s" % (new, live))
        ch.run("DROP TABLE %s" % new)
    else:
        ch.run("RENAME TABLE %s TO %s" % (new, live))
    return len(rows)


def publish(tables, hosts, user, password, urlopen=None, out=print, timeout=30.0,
            names=None, schemas=None, database=DATABASE):
    """Replace every table on every host.  One line per host+table.  Returns the failures
    as (host, table, message); a failure never stops the next table or host.
    names, schemas and database default to this module's own; fleetboard passes its."""
    names = TABLES if names is None else names
    fails = []
    for host in hosts:
        ch = ChHttp(host, user, password, timeout=timeout, urlopen=urlopen)
        for table in names:
            if table not in tables:   # not built this run: the host keeps its last copy
                continue
            try:
                n = replace_table(ch, table, tables[table],
                                  schema=None if schemas is None else schemas[table],
                                  database=database)
                out("%s %s: %d rows" % (host, table, n))
            except Exception as e:
                msg = ch.scrub(str(e) if isinstance(e, PublishError)
                               else "%s: %s" % (type(e).__name__, e))
                fails.append((host, table, msg))
                out("%s %s: FAILED: %s" % (host, table, msg))
    return fails


def now_utc():
    return datetime.now(timezone.utc).timestamp()
