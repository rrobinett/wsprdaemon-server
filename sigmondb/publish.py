"""Build the five sigmondb_public tables and replace each one WHOLE on every public host.

wd30 runs this hourly (sigmondb-publish.timer).  It reads wd30's own sigmondb, builds five
small tables, and swaps each into sigmondb_public on WD10 and WD20, where stations read them
anonymously.  A station never sees half an update: each table is built under <t>__new and
then exchanged with <t> in one step.  hf-timestd's T3 Phase B consumes them
(hf-timestd/docs/superpowers/plans/2026-11-13-t3-automatic-admission.md, Tasks B1, B4, B5).

Every row carries built_at_utc, unix seconds, one value for the whole build.

transmitter_offsets: the latest solution per tx_id (by window_to) that has an offset.
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
import urllib.error
import urllib.parse
import urllib.request
from datetime import datetime, timezone

from sigmondb.groundwave import _latest
from sigmondb.receivers import grid_to_latlon

DATABASE = "sigmondb_public"
TABLES = ("transmitter_offsets", "trust", "receivers", "transmitters", "t3_calibration")
NF64 = "Nullable(Float64)"
SCHEMAS = {
    "transmitter_offsets": [
        ("tx_id", "String"), ("kind", "String"), ("offset_ms", "Float64"),
        ("sigma_ms", "Float64"), ("last_confirmed_utc", "Float64"),
        ("n_confirming", "UInt16"), ("built_at_utc", "Float64")],
    "trust": [
        ("reporter_id", "String"), ("a_level", "String"), ("trusted", "UInt8"),
        ("built_at_utc", "Float64")],
    "receivers": [
        ("reporter_id", "String"), ("grid6", "String"), ("lat", "Float64"),
        ("lon", "Float64"), ("a_level", "String"), ("built_at_utc", "Float64")],
    "transmitters": [
        ("tx_id", "String"), ("kind", "String"), ("network", "String"), ("name", "String"),
        ("status", "String"), ("lat", NF64), ("lon", NF64), ("pos_sigma_m", NF64),
        ("built_at_utc", "Float64")],
    "t3_calibration": [
        ("station", "String"), ("reference", "String"), ("hour_utc", "UInt8"),
        ("n", "UInt32"), ("rms_z", "Float64"), ("floor_fraction", "Float64"),
        ("honest", "UInt8"), ("built_at_utc", "Float64")],
}

# What read_source() takes from wd30's sigmondb, each table read FINAL.
SOURCE_COLUMNS = {
    "solution": ("tx_id", "solve_id", "window_to", "offset_ns", "offset_sigma_ns",
                 "n_stations"),
    "transmitter": ("tx_id", "kind", "network", "name", "status", "lat", "lon",
                    "pos_sigma_m", "valid_from", "version"),
    "receiver": ("reporter_id", "lat", "lon", "a_level", "valid_from", "version"),
}
SOURCE_WHERE = {"solution": "WHERE offset_ns IS NOT NULL AND tx_id != ''"}


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

def build_transmitter_offsets(solutions, transmitters, built_at):
    txs = _latest(transmitters or [], "tx_id")
    best = {}
    for s in solutions:
        if not s.get("tx_id") or s.get("offset_ns") is None or s.get("offset_sigma_ns") is None:
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
            n_confirming=int(s.get("n_stations") or 0), built_at_utc=built_at))
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


def build_transmitters(transmitters, built_at):
    out = []
    for tx_id, t in sorted(_latest(transmitters or [], "tx_id").items()):
        sig = t.get("pos_sigma_m")
        out.append(dict(tx_id=tx_id, kind=t.get("kind") or "", network=t.get("network") or "",
                        name=t.get("name") or "", status=t.get("status") or "",
                        lat=t.get("lat"), lon=t.get("lon"),
                        pos_sigma_m=None if sig is None else float(sig),
                        built_at_utc=built_at))
    return out


def build_all(source, built_at):
    """{table: rows} for every table in TABLES order."""
    return {
        "transmitter_offsets": build_transmitter_offsets(
            source["solution"], source["transmitter"], built_at),
        "trust": build_trust(source["receiver"], built_at),
        "receivers": build_receivers(source["receiver"], built_at),
        "transmitters": build_transmitters(source["transmitter"], built_at),
        "t3_calibration": [],
    }


def read_source(client):
    """{solution, transmitter, receiver} rows from wd30's sigmondb, every table FINAL."""
    out = {}
    for table, cols in SOURCE_COLUMNS.items():
        sql = "SELECT %s FROM sigmondb.%s FINAL %s" % (
            ", ".join(cols), table, SOURCE_WHERE.get(table, ""))
        try:
            res = client.query(sql, query_tz="UTC")
        except TypeError:   # clickhouse-connect without query_tz; _unix() treats naive as UTC
            res = client.query(sql)
        out[table] = [dict(zip(cols, r)) for r in res.result_rows]
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
            try:
                n = replace_table(ch, table, tables.get(table, []))
                out("%s %s: %d rows" % (host, table, n))
            except Exception as e:
                msg = ch.scrub(str(e) if isinstance(e, PublishError)
                               else "%s: %s" % (type(e).__name__, e))
                fails.append((host, table, msg))
                out("%s %s: FAILED: %s" % (host, table, msg))
    return fails


def now_utc():
    return datetime.now(timezone.utc).timestamp()
