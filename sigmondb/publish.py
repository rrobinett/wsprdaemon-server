"""Build the five sigmondb_public tables and replace each one WHOLE on every public host.

wd30 runs this hourly (sigmondb-publish.timer).  It reads wd30's own sigmondb, builds five
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

transmitters: the latest version per tx_id, at full precision (public infrastructure).

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

from sigmondb.groundwave import _latest, great_circle_m
from sigmondb.paths import gc_midpoint, match_group, path_kind
from sigmondb.receivers import grid_to_latlon

DATABASE = "sigmondb_public"
TABLES = ("transmitter_offsets", "trust", "receivers", "transmitters", "groups", "heard",
          "t3_calibration", "ionosphere_hourly")
NF64 = "Nullable(Float64)"
SCHEMAS = {
    "transmitter_offsets": [
        ("tx_id", "String"), ("kind", "String"), ("offset_ms", "Float64"),
        ("sigma_ms", "Float64"), ("last_confirmed_utc", "Float64"),
        ("n_confirming", "UInt16"), ("confirming_reporters", "Array(String)"),
        ("built_at_utc", "Float64")],
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
}

# What read_source() takes from wd30's sigmondb, each table read FINAL.
SOURCE_COLUMNS = {
    "solution": ("tx_id", "solve_id", "window_to", "offset_ns", "offset_sigma_ns",
                 "n_stations", "reporter_ids"),
    "transmitter": ("tx_id", "kind", "network", "name", "status", "lat", "lon",
                    "pos_sigma_m", "valid_from", "version"),
    "receiver": ("reporter_id", "lat", "lon", "a_level", "valid_from", "version"),
    "waveform": ("tx_id", "centre_hz", "rate_hz_per_s", "valid_from"),
}
SOURCE_WHERE = {"solution": "WHERE offset_ns IS NOT NULL AND tx_id != ''"}

# Aggregates read_source runs on wd30: name -> (columns, SQL).  HFDL stays out of every one.
_NOT_HFDL = "NOT startsWith(group_key, 'hfdl') AND tx_id NOT LIKE 'hfdl:%'"
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
        "FROM sigmondb.arrival FINAL WHERE time > now() - INTERVAL 7 DAY AND " + _NOT_HFDL +
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
        "FROM sigmondb.arrival FINAL WHERE time > now() - INTERVAL 24 HOUR AND " + _NOT_HFDL +
        " GROUP BY reporter_id, tx_id, group_key, hour"),
}


# --- conversions ----------------------------------------------------------------------------

def emission_minus_utc_ms(offset_ns):
    """solution.offset_ns (emission minus UTC, ns) in ms, sign kept."""
    return offset_ns / 1e6


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
            built_at_utc=built_at))
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


def build_transmitters(transmitters, built_at, waveforms=()):
    latest_wf = _latest_waveforms(waveforms)
    out = []
    for tx_id, t in sorted(_latest(transmitters or [], "tx_id").items()):
        sig = t.get("pos_sigma_m")
        kind = t.get("kind") or ""
        cls, family = class_family(kind, t.get("network") or "")
        wf = latest_wf.get(tx_id) or {}
        centre, rate = wf.get("centre_hz"), wf.get("rate_hz_per_s")
        out.append({"tx_id": tx_id, "kind": kind, "network": t.get("network") or "",
                    "name": t.get("name") or "", "status": t.get("status") or "",
                    "lat": t.get("lat"), "lon": t.get("lon"),
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
    txs = _latest(source.get("transmitter") or [], "tx_id")
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
    """{table: rows} in TABLES order.  groups, heard and ionosphere_hourly are left out when
    their aggregate was not read or their builder fails; the core tables always build."""
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
    core.update(extra)
    return {t: core[t] for t in TABLES if t in core}


def read_source(client, out=print):
    """{solution, transmitter, receiver} rows from wd30's sigmondb, every table FINAL."""
    log, out = out, {}

    def run(sql):
        try:
            return client.query(sql, query_tz="UTC")
        except TypeError:   # clickhouse-connect without query_tz; _unix() treats naive as UTC
            return client.query(sql)

    for table, cols in SOURCE_COLUMNS.items():
        sql = "SELECT %s FROM sigmondb.%s FINAL %s" % (
            ", ".join(cols), table, SOURCE_WHERE.get(table, ""))
        out[table] = [dict(zip(cols, r)) for r in run(sql).result_rows]
    for name, (cols, sql) in AGG_QUERIES.items():
        try:
            out_rows = [dict(zip(cols, r)) for r in run(sql).result_rows]
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


def _create_sql(name, schema):
    cols = ", ".join("`%s` %s" % (c, t) for c, t in schema)
    return "CREATE TABLE %s.%s (%s) ENGINE = MergeTree ORDER BY tuple()" % (DATABASE, name, cols)


def _jsonl(rows):
    return "".join(json.dumps(r, allow_nan=False, ensure_ascii=False) + "\n" for r in rows)


def replace_table(ch, table, rows):
    """Build <table>__new, then swap it in: EXCHANGE when <table> exists, else RENAME."""
    new, live = "%s.%s__new" % (DATABASE, table), "%s.%s" % (DATABASE, table)
    data = _jsonl(rows)   # before any statement: unsendable rows touch nothing
    ch.run("DROP TABLE IF EXISTS %s" % new)
    ch.run(_create_sql(table + "__new", SCHEMAS[table]))
    if rows:
        ch.run("INSERT INTO %s FORMAT JSONEachRow" % new, data)
    if ch.run("EXISTS TABLE %s" % live).strip() == "1":
        ch.run("EXCHANGE TABLES %s AND %s" % (new, live))
        ch.run("DROP TABLE %s" % new)
    else:
        ch.run("RENAME TABLE %s TO %s" % (new, live))
    return len(rows)


def publish(tables, hosts, user, password, urlopen=None, out=print, timeout=30.0):
    """Replace every table on every host.  One line per host+table.  Returns the failures
    as (host, table, message); a failure never stops the next table or host."""
    fails = []
    for host in hosts:
        ch = ChHttp(host, user, password, timeout=timeout, urlopen=urlopen)
        for table in TABLES:
            if table not in tables:   # not built this run: the host keeps its last copy
                continue
            try:
                n = replace_table(ch, table, tables[table])
                out("%s %s: %d rows" % (host, table, n))
            except Exception as e:
                msg = ch.scrub(str(e) if isinstance(e, PublishError)
                               else "%s: %s" % (type(e).__name__, e))
                fails.append((host, table, msg))
                out("%s %s: FAILED: %s" % (host, table, msg))
    return fails


def now_utc():
    return datetime.now(timezone.utc).timestamp()
