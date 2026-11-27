"""sigmondb publish: build the five sigmondb_public tables and replace each WHOLE on every
public host.  No ClickHouse: a fake source client stands for wd30's sigmondb, and a fake
urlopen stands for each public server's HTTP interface."""
import base64
import json
import math
import os
import re
import sys
import urllib.error
import urllib.parse
from datetime import datetime, timezone

import pytest

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from sigmondb import cli, publish  # noqa: E402
from sigmondb.groundwave import C_M_PER_S, ground_wave_solutions  # noqa: E402

BUILT = 1_794_000_000.0
PASSWORD = "s3cret-Pa55word"


# --- fakes -------------------------------------------------------------------------------

class _Result:
    def __init__(self, rows):
        self.result_rows = rows


class FakeSource:
    """wd30's sigmondb: answers `SELECT a, b FROM sigmondb.t FINAL ...` from dict rows."""

    def __init__(self, **tables):
        self.tables = tables
        self.sql = []
        self.params = []

    def query(self, sql, parameters=None, query_tz=None):
        self.sql.append(sql)
        self.params.append(parameters)
        m = re.match(r"\s*/\* agg:(\w+) \*/", sql)
        if m:
            cols = dict(publish.AGG_QUERIES, **publish.CHIRP_AGG_QUERIES)[m.group(1)][0]
            return _Result([tuple(r.get(c) for c in cols)
                            for r in self.tables.get("agg:" + m.group(1), [])])
        m = re.match(r"\s*SELECT (.+?) FROM sigmondb\.(\w+) FINAL", sql)
        cols = [c.strip() for c in m.group(1).split(",")]
        return _Result([tuple(r.get(c) for c in cols) for r in self.tables.get(m.group(2), [])])


# What each statement needs, after ClickHouse's InterpreterRenameQuery / DROP / CREATE.
PUBLISHER_GRANTS = {"CREATE TABLE", "DROP TABLE", "INSERT", "SELECT", "ALTER", "TRUNCATE"}


class FakeServer:
    """One public ClickHouse over HTTP.  Holds tables in sigmondb_public; checks Basic auth
    and the grants each statement needs; down=True refuses every connection."""

    def __init__(self, user="sigmondb_publisher", password=PASSWORD, grants=PUBLISHER_GRANTS,
                 tables=None, down=False):
        self.auth = "Basic " + base64.b64encode(("%s:%s" % (user, password)).encode()).decode()
        self.grants = set(grants)
        self.tables = dict(tables or {})   # name -> list of row dicts
        self.down = down
        self.log = []
        self.urls = []

    def _need(self, *privs):
        missing = [p for p in privs if p not in self.grants]
        if missing:
            raise PermissionError("ACCESS_DENIED: need %s" % ", ".join(missing))

    def _run(self, sql, body):
        s = " ".join(sql.split())
        self.log.append(s)
        db = "sigmondb_public."
        m = re.match(r"DROP TABLE IF EXISTS %s(\w+)$" % re.escape(db), s)
        if m:
            self._need("DROP TABLE")
            self.tables.pop(m.group(1), None)
            return ""
        m = re.match(r"DROP TABLE %s(\w+)$" % re.escape(db), s)
        if m:
            self._need("DROP TABLE")
            del self.tables[m.group(1)]
            return ""
        m = re.match(r"CREATE TABLE %s(\w+) \(.*\) ENGINE = MergeTree ORDER BY tuple\(\)$"
                     % re.escape(db), s)
        if m:
            self._need("CREATE TABLE")
            assert m.group(1) not in self.tables, "TABLE_ALREADY_EXISTS"
            self.tables[m.group(1)] = []
            return ""
        m = re.match(r"INSERT INTO %s(\w+) FORMAT JSONEachRow$" % re.escape(db), s)
        if m:
            self._need("INSERT")
            self.tables[m.group(1)] += [json.loads(ln) for ln in body.splitlines() if ln.strip()]
            return ""
        m = re.match(r"EXISTS TABLE %s(\w+)$" % re.escape(db), s)
        if m:
            return "1\n" if m.group(1) in self.tables else "0\n"
        m = re.match(r"EXCHANGE TABLES %s(\w+) AND %s(\w+)$" % (re.escape(db), re.escape(db)), s)
        if m:
            self._need("SELECT", "DROP TABLE", "CREATE TABLE", "INSERT")
            a, b = m.groups()
            self.tables[a], self.tables[b] = self.tables[b], self.tables[a]
            return ""
        m = re.match(r"RENAME TABLE %s(\w+) TO %s(\w+)$" % (re.escape(db), re.escape(db)), s)
        if m:
            self._need("SELECT", "DROP TABLE", "CREATE TABLE", "INSERT")
            a, b = m.groups()
            assert b not in self.tables, "TABLE_ALREADY_EXISTS"
            self.tables[b] = self.tables.pop(a)
            return ""
        raise AssertionError("fake does not know: %s" % s)

    def urlopen(self, req, timeout=None):
        self.urls.append(req.full_url)
        if self.down:
            raise urllib.error.URLError("connection refused")
        assert req.get_method() == "POST"
        if req.get_header("Authorization") != self.auth:
            raise urllib.error.HTTPError(req.full_url, 516, "AUTHENTICATION_FAILED", {}, None)
        q = urllib.parse.parse_qs(urllib.parse.urlsplit(req.full_url).query).get("query")
        body = (req.data or b"").decode()
        sql, data = (q[0], body) if q else (body, "")
        try:
            out = self._run(sql, data)
        except (PermissionError, AssertionError) as e:
            raise urllib.error.HTTPError(req.full_url, 500, str(e), {}, None)
        return _Resp(out)


class _Resp:
    def __init__(self, text):
        self.text = text.encode()

    def read(self):
        return self.text

    def __enter__(self):
        return self

    def __exit__(self, *a):
        return False


class Router:
    """urlopen that dispatches on the base URL to one FakeServer each."""

    def __init__(self, servers):
        self.servers = servers

    def __call__(self, req, timeout=None):
        for base, srv in self.servers.items():
            if req.full_url.startswith(base):
                return srv.urlopen(req, timeout)
        raise AssertionError(req.full_url)


def _utc(*a):
    return datetime(*a, tzinfo=timezone.utc)


def _sol(tx_id, window_to, offset_ns, sigma_ns=100_000.0, n=1, solve_id="gw:x"):
    return dict(tx_id=tx_id, solve_id=solve_id, window_to=window_to, offset_ns=offset_ns,
                offset_sigma_ns=sigma_ns, n_stations=n)


def _tx(tx_id, kind="codar", version=1, valid_from=datetime(2026, 11, 9), **kw):
    r = dict(tx_id=tx_id, kind=kind, network="hfrnet", name=tx_id.split(":")[-1],
             status="listed", lat=40.0, lon=-74.0, pos_sigma_m=30.0,
             valid_from=valid_from, version=version)
    r.update(kw)
    return r


def _rx(rid, lat=41.714775, lon=-72.727260, a_level="A1", version=1,
        valid_from=datetime(2026, 11, 9)):
    return dict(reporter_id=rid, lat=lat, lon=lon, a_level=a_level, version=version,
                valid_from=valid_from)


# --- the offset sign ----------------------------------------------------------------------

def test_offset_sign_follows_the_solvers_convention():
    """The solver writes offset_ns = fixed_delay_ns - distance/c, where delay_ns is the
    arrival's phase AFTER the sweep start on UTC (groundwave.py, ground_wave_solutions).
    A transmitter keying 2.5 ms late lands 2.5 ms later than distance/c: offset_ns =
    +2_500_000, published as +2.5 ms, emission minus UTC."""
    rx = [_rx("RX", lat=40.0, lon=-74.0)]
    tx = [_tx("hfrnet:LATE", lat=40.0, lon=-73.0)]
    from sigmondb.groundwave import great_circle_m
    path_ns = round(great_circle_m(40.0, -73.0, 40.0, -74.0) / C_M_PER_S * 1e9)
    cluster = dict(cluster_key="k", fixed_delay_ns=path_ns + 2_500_000, delay_sd_ns=1000.0,
                   n_arrivals=10, window_from=datetime(2026, 11, 10),
                   window_to=datetime(2026, 11, 11), reporter_ids=["RX"], group_key="g",
                   tx_ids=["hfrnet:LATE"], arrival_keys=["a"])
    sol = ground_wave_solutions([cluster], tx, rx, "groundwave-2")[0]
    assert sol["offset_ns"] == 2_500_000
    assert publish.emission_minus_utc_ms(sol["offset_ns"]) == pytest.approx(2.5)
    assert publish.emission_minus_utc_ms(-1_250_000) == pytest.approx(-1.25)


def test_transmitter_offsets_take_the_latest_solution_per_tx():
    sols = [
        _sol("hfrnet:A", _utc(2026, 11, 10), 1_000_000),
        _sol("hfrnet:A", _utc(2026, 11, 12), 2_500_000, sigma_ns=150_000.0, n=2),
        _sol("hfrnet:A", _utc(2026, 11, 11), 9_000_000),
        _sol("hfrnet:B", _utc(2026, 11, 12), None),          # no offset: never published
        _sol("", _utc(2026, 11, 12), 3_000_000),             # unnamed: never published
        _sol("hfrnet:C", _utc(2026, 11, 9), -500_000),
    ]
    txs = [_tx("hfrnet:A", kind="codar"), _tx("hfrnet:C", kind="ionosonde", version=1),
           _tx("hfrnet:C", kind="oth", version=2)]
    got = {r["tx_id"]: r for r in publish.build_transmitter_offsets(sols, txs, BUILT)}
    assert sorted(got) == ["hfrnet:A", "hfrnet:C"]
    a = got["hfrnet:A"]
    assert a == dict(tx_id="hfrnet:A", kind="codar", offset_ms=2.5, sigma_ms=0.15,
                     last_confirmed_utc=_utc(2026, 11, 12).timestamp(), n_confirming=2,
                     confirming_reporters=[], period_s=None, built_at_utc=BUILT)
    assert got["hfrnet:C"]["kind"] == "oth" and got["hfrnet:C"]["offset_ms"] == -0.5


def test_transmitter_offsets_carry_the_sweep_period_from_the_cluster_key():
    """offset_ms is a phase modulo the sweep period P, in [-P/2, P/2).  A consumer folds its
    residual modulo P, so the row carries P, read from the solution's cluster_key
    ("<reporter>|<group key>|<us>") by the parser the readers use."""
    sols = [dict(_sol("hfrnet:A", _utc(2026, 11, 12), -41_060_000),
                 cluster_key="K3LR/D|4.4750 -25.73|958940"),
            dict(_sol("hfrnet:B", _utc(2026, 11, 12), 1_000),
                 cluster_key="AC0G/B4|4.4630 -25.73 0.5s|12"),
            dict(_sol("hfrnet:C", _utc(2026, 11, 12), 1_000),
                 cluster_key="AC0G/B4|4.4630 -25.73 2s sweep0.5s|12"),
            dict(_sol("hfrnet:D", _utc(2026, 11, 12), 1_000), cluster_key="X|KFS-OMNI|0"),
            _sol("hfrnet:E", _utc(2026, 11, 12), 1_000)]
    got = {r["tx_id"]: r for r in publish.build_transmitter_offsets(sols, [], BUILT)}
    assert got["hfrnet:A"]["period_s"] == 1.0 and got["hfrnet:A"]["offset_ms"] == -41.06
    assert got["hfrnet:B"]["period_s"] == 0.5
    assert got["hfrnet:C"]["period_s"] == 2.0
    assert got["hfrnet:D"]["period_s"] is None          # the key gives no period
    assert got["hfrnet:E"]["period_s"] is None          # no cluster_key at all
    assert ("period_s", "Nullable(Float64)") in publish.SCHEMAS["transmitter_offsets"]
    assert "cluster_key" in publish.SOURCE_COLUMNS["solution"]
    for r in got.values():
        assert set(r) == {c for c, _ in publish.SCHEMAS["transmitter_offsets"]}


def test_the_offset_docstring_states_the_range_and_the_fold():
    doc = publish.__doc__
    assert "[-P/2, P/2)" in doc and "period_s" in doc and "modulo P" in doc


def test_confirming_reporters_are_the_chosen_solutions_reporter_ids():
    sols = [dict(_sol("hfrnet:A", _utc(2026, 11, 10), 1_000_000), reporter_ids=["OLD"]),
            dict(_sol("hfrnet:A", _utc(2026, 11, 12), 2_500_000, n=3),
                 reporter_ids=["W1AW", "AC0G/B4", "W1AW"])]
    r = publish.build_transmitter_offsets(sols, [_tx("hfrnet:A")], BUILT)[0]
    assert r["confirming_reporters"] == ["AC0G/B4", "W1AW"]
    assert r["n_confirming"] == 3
    assert ("confirming_reporters", "Array(String)") in publish.SCHEMAS["transmitter_offsets"]
    assert "reporter_ids" in publish.SOURCE_COLUMNS["solution"]


def test_a_naive_window_to_counts_as_utc():
    r = publish.build_transmitter_offsets(
        [_sol("hfrnet:A", datetime(2026, 11, 12), 0)], [_tx("hfrnet:A")], BUILT)[0]
    assert r["last_confirmed_utc"] == _utc(2026, 11, 12).timestamp()


def test_an_offset_without_a_transmitter_row_has_kind_unknown():
    r = publish.build_transmitter_offsets([_sol("cand:x", _utc(2026, 11, 12), 0)], [], BUILT)
    assert r[0]["kind"] == "unknown"


# --- trust ------------------------------------------------------------------------------

def test_trust_is_a1_only_from_the_latest_receiver_version():
    rxs = [_rx("GOOD", a_level="A1"), _rx("FREE", a_level="A0"), _rx("UNSTATED", a_level=""),
           _rx("WAS", a_level="A1", version=1), _rx("WAS", a_level="A0", version=2),
           _rx("lower", a_level=" a1 ")]
    got = {r["reporter_id"]: r for r in publish.build_trust(rxs, BUILT)}
    assert {k: v["trusted"] for k, v in got.items()} == dict(
        GOOD=1, FREE=0, UNSTATED=0, WAS=0, lower=1)
    assert got["WAS"] == dict(reporter_id="WAS", a_level="A0", trusted=0, built_at_utc=BUILT)


# --- receivers: privacy -----------------------------------------------------------------

def test_grid6_of_a_known_point():
    assert publish.latlon_to_grid6(41.714775, -72.727260) == "FN31pr"
    assert publish.latlon_to_grid6(-33.8688, 151.2093) == "QF56od"
    assert publish.latlon_to_grid6(90.0, 180.0) == "RR99xx"


def test_receivers_publish_the_grid6_centre_never_the_stored_position():
    rows = publish.build_receivers([_rx("W1AW"), _rx("NOPOS", lat=None, lon=None)], BUILT)
    assert len(rows) == 1
    r = rows[0]
    # FN31pr: lon -180 + 5*20 + 3*2 + 15/12 + 1/24, lat -90 + 13*10 + 1 + 17/24 + 1/48
    assert r == dict(reporter_id="W1AW", grid6="FN31pr", lat=pytest.approx(41.7291667),
                     lon=pytest.approx(-72.7083333), a_level="A1", built_at_utc=BUILT)
    assert r["lat"] != 41.714775 and r["lon"] != -72.727260
    assert set(r) == {c for c, _ in publish.SCHEMAS["receivers"]}


# --- transmitters ------------------------------------------------------------------------

def test_transmitters_latest_version_full_precision():
    rows = publish.build_transmitters(
        [_tx("hfrnet:A", version=1, lat=1.0), _tx("hfrnet:A", version=2, lat=40.123456789),
         _tx("cand:1", kind="unknown", lat=None, lon=None, pos_sigma_m=None)], BUILT)
    got = {r["tx_id"]: r for r in rows}
    assert got["hfrnet:A"]["lat"] == 40.123456789
    assert got["cand:1"]["pos_sigma_m"] is None
    assert set(got["hfrnet:A"]) == {c for c, _ in publish.SCHEMAS["transmitters"]}


def _wf(tx_id, valid_from, centre_hz, rate):
    return dict(tx_id=tx_id, valid_from=valid_from, centre_hz=centre_hz, rate_hz_per_s=rate)


def test_class_family_codar():
    assert publish.class_family("codar", "BML") == ("ocean radar", "CODAR SeaSonde")


def test_class_family_unknown_kind():
    assert publish.class_family("zzz", "") == ("unknown", "unspecified")


def test_class_family_other_kinds():
    assert publish.class_family("oth", "") == ("over-the-horizon radar", "unspecified")


def test_transmitters_carry_waveform():
    rows = publish.build_transmitters(
        [_tx("hfrnet:A"), _tx("hfrnet:B")], BUILT,
        waveforms=[_wf("hfrnet:A", _utc(2026, 11, 1), 12_000_000.0, -50_000.0),
                   _wf("hfrnet:A", _utc(2026, 11, 5), 13_500_000.0, -102940.0)])
    got = {r["tx_id"]: r for r in rows}
    assert got["hfrnet:A"]["centre_mhz"] == pytest.approx(13.5, abs=0.005)
    assert got["hfrnet:A"]["rate_khz_s"] == pytest.approx(-102.94, abs=0.005)
    assert got["hfrnet:A"]["class"] == "ocean radar"
    assert got["hfrnet:A"]["family"] == "CODAR SeaSonde"
    assert got["hfrnet:B"]["centre_mhz"] is None and got["hfrnet:B"]["rate_khz_s"] is None


def test_transmitters_schema_columns():
    cols = [c for c, _ in publish.SCHEMAS["transmitters"]]
    assert cols[-5:] == ["class", "family", "centre_mhz", "rate_khz_s", "built_at_utc"]


# --- ionosphere_hourly -----------------------------------------------------------------------

NAN = float("nan")


def _ion_row(reporter="K0X", tx_id="", key="4.4505 -25.73", centre=4_450_500.0,
             rate=-25_730.0, dq=(-1.0, 0.5, 2.0), sq=(0.1, 0.2, 0.4), pq=(NAN,) * 3,
             lq=(-80.0, -70.0, -60.0), n=7, hour=None):
    return dict(reporter_id=reporter, tx_id=tx_id, group_key=key, centre_hz=centre,
                rate_hz_per_s=rate, hour=hour or _utc(2026, 11, 14, 5), n=n,
                dq=list(dq), sq=list(sq), pq=list(pq), lq=list(lq))


def _ion_src(rows, **kw):
    lat, lon = publish.grid_to_latlon("EM38ww")
    return _src([], [_rx("K0X", lat=lat, lon=lon)], **{"agg:ionosphere": rows}, **kw)


def test_ionosphere_skywave_only():
    lat, lon = publish.grid_to_latlon("EL29ho")
    src = _src([], [_rx("K0X", lat=lat, lon=lon)],
               transmitter=[_tx("hfrnet:PREY", lat=lat + 0.36, lon=lon)],
               **{"agg:ionosphere": [_ion_row(tx_id="hfrnet:PREY", centre=13_500_000.0)]})
    assert publish.build_ionosphere(src, BUILT) == []


def test_ionosphere_group_midpoints():
    from sigmondb.paths import gc_midpoint
    src = _ion_src([_ion_row()],
                   transmitter=[_tx("hfrnet:PREY", lat=29.0, lon=-95.0),
                                _tx("hfrnet:P2", lat=30.0, lon=-94.0),
                                _tx("hfrnet:P3", lat=31.0, lon=-93.0)],
                   waveform=[_wf(t, _utc(2026, 11, 1), 4_450_500.0, -25_730.0)
                             for t in ("hfrnet:PREY", "hfrnet:P2", "hfrnet:P3")])
    rows = publish.build_ionosphere(src, BUILT)
    assert len(rows) == 1
    r = rows[0]
    st = publish.grid_to_latlon("EM38ww")
    assert len(r["midpoints"]) == 3
    # members come in tx_id order: P2, P3, PREY
    for mp, pos in zip(r["midpoints"], [(30.0, -94.0), (31.0, -93.0), (29.0, -95.0)]):
        assert isinstance(mp, list) and len(mp) == 2
        assert mp == pytest.approx(list(gc_midpoint(st[0], st[1], *pos)))
    assert r["target_type"] == "group" and r["n"] == 7 and r["built_at_utc"] == BUILT
    assert r["hour_utc"] == _utc(2026, 11, 14, 5).timestamp()


def test_ionosphere_medians():
    r = publish.build_ionosphere(_ion_src([_ion_row()]), BUILT)[0]
    assert r["doppler_med"] == 0.5 and r["doppler_iqr"] == 3.0
    assert r["s4_med"] == 0.2 and r["s4_iqr"] == pytest.approx(0.3)
    assert r["level_med"] == -70.0 and r["level_iqr"] == 20.0


def test_ionosphere_nulls_stay_null():
    r = publish.build_ionosphere(_ion_src([_ion_row(sq=[], pq=(NAN, NAN, NAN))]), BUILT)[0]
    assert r["s4_med"] is None and r["s4_iqr"] is None
    assert r["sphi_med"] is None and r["sphi_iqr"] is None
    assert r["doppler_med"] == 0.5


def test_ionosphere_schema_and_table_order():
    assert publish.TABLES[7] == "ionosphere_hourly"
    assert ("midpoints", "Array(Tuple(Float64, Float64))") in publish.SCHEMAS["ionosphere_hourly"]
    assert "agg:ionosphere" in publish.AGG_QUERIES["ionosphere"][1] or \
        publish.AGG_QUERIES["ionosphere"][1].startswith("/* agg:ionosphere */")


# --- build_all and the source read -----------------------------------------------------------

def test_build_all_reads_final_and_t3_calibration_is_empty():
    src = FakeSource(solution=[_sol("hfrnet:A", _utc(2026, 11, 12), 1_000_000)],
                     transmitter=[_tx("hfrnet:A")], receiver=[_rx("W1AW")])
    tables = publish.build_all(publish.read_source(src), BUILT)
    assert list(tables) == list(publish.TABLES)
    assert tables["t3_calibration"] == []
    assert [len(tables[t]) for t in publish.TABLES] == [1, 1, 1, 1, 0, 0, 0, 0, 0, 0, 0]
    assert all(" FINAL" in s for s in src.sql if not s.startswith("/* agg:"))
    for t in publish.TABLES:
        for r in tables[t]:
            assert r["built_at_utc"] == BUILT


def test_every_schema_carries_built_at_utc():
    for t in publish.TABLES:
        assert ("built_at_utc", "Float64") in publish.SCHEMAS[t]
    assert publish.SCHEMAS["t3_calibration"] == [
        ("station", "String"), ("reference", "String"), ("hour_utc", "UInt8"),
        ("n", "UInt32"), ("rms_z", "Float64"), ("floor_fraction", "Float64"),
        ("honest", "UInt8"), ("built_at_utc", "Float64")]


# --- replacement on the public hosts -------------------------------------------------------

H1, H2 = "http://10.112.0.10:8123", "http://10.112.0.20:8123"


def _core_tables():
    return dict(transmitter_offsets=[dict(tx_id="hfrnet:A", built_at_utc=BUILT)],
                trust=[], receivers=[], transmitters=[], t3_calibration=[])


def _tables():
    return dict(_core_tables(), groups=[], heard=[], ionosphere_hourly=[], chirps=[],
                chirp_reject_local=[], chirp_menu=[])


def test_first_run_creates_then_renames():
    srv = FakeServer()
    fails = publish.publish(_tables(), [H1], "sigmondb_publisher", PASSWORD,
                            urlopen=Router({H1: srv}), out=lambda s: None)
    assert fails == []
    assert sorted(srv.tables) == sorted(publish.TABLES)
    assert srv.tables["transmitter_offsets"] == [dict(tx_id="hfrnet:A", built_at_utc=BUILT)]
    assert srv.tables["t3_calibration"] == []
    seq = [s.split(" (")[0] for s in srv.log if "transmitter_offsets" in s]
    assert seq == [
        "DROP TABLE IF EXISTS sigmondb_public.transmitter_offsets__new",
        "CREATE TABLE sigmondb_public.transmitter_offsets__new",
        "INSERT INTO sigmondb_public.transmitter_offsets__new FORMAT JSONEachRow",
        "EXISTS TABLE sigmondb_public.transmitter_offsets",
        "RENAME TABLE sigmondb_public.transmitter_offsets__new TO "
        "sigmondb_public.transmitter_offsets"]
    assert not any("CREATE DATABASE" in s for s in srv.log)


def test_existing_table_is_exchanged_and_the_old_copy_dropped():
    old = [dict(tx_id="old", built_at_utc=1.0)]
    srv = FakeServer(tables=dict(transmitter_offsets=old))
    fails = publish.publish(_tables(), [H1], "sigmondb_publisher", PASSWORD,
                            urlopen=Router({H1: srv}), out=lambda s: None)
    assert fails == []
    assert srv.tables["transmitter_offsets"] == [dict(tx_id="hfrnet:A", built_at_utc=BUILT)]
    assert "transmitter_offsets__new" not in srv.tables
    seq = [s for s in srv.log if "transmitter_offsets" in s][-2:]
    assert seq == ["EXCHANGE TABLES sigmondb_public.transmitter_offsets__new AND "
                   "sigmondb_public.transmitter_offsets",
                   "DROP TABLE sigmondb_public.transmitter_offsets__new"]


def test_a_leftover_new_from_a_crash_is_dropped_first():
    srv = FakeServer(tables=dict(trust__new=[dict(stale=1)], trust=[dict(old=1)]))
    fails = publish.publish(_tables(), [H1], "sigmondb_publisher", PASSWORD,
                            urlopen=Router({H1: srv}), out=lambda s: None)
    assert fails == []
    assert srv.tables["trust"] == [] and "trust__new" not in srv.tables
    first = [s for s in srv.log if "trust" in s][0]
    assert first == "DROP TABLE IF EXISTS sigmondb_public.trust__new"


def test_the_publisher_grants_suffice_and_a_missing_one_fails_loudly():
    """EXCHANGE and RENAME need SELECT, DROP TABLE, CREATE TABLE and INSERT on both names
    (ClickHouse InterpreterRenameQuery); the publisher holds all four on sigmondb_public.*."""
    ok = FakeServer(tables=dict(trust=[]))
    assert publish.publish(_tables(), [H1], "sigmondb_publisher", PASSWORD,
                           urlopen=Router({H1: ok}), out=lambda s: None) == []
    short = FakeServer(tables=dict(trust=[]), grants=PUBLISHER_GRANTS - {"DROP TABLE"})
    lines = []
    fails = publish.publish(_tables(), [H1], "sigmondb_publisher", PASSWORD,
                            urlopen=Router({H1: short}), out=lines.append)
    assert len(fails) == len(publish.TABLES)


def test_one_host_down_the_other_still_updated_and_one_line_per_host_table(capsys):
    up, down = FakeServer(), FakeServer(down=True)
    lines = []
    fails = publish.publish(_tables(), [H1, H2], "sigmondb_publisher", PASSWORD,
                            urlopen=Router({H1: down, H2: up}), out=lines.append)
    assert sorted(up.tables) == sorted(publish.TABLES)
    assert {(h, t) for h, t, _ in fails} == {(H1, t) for t in publish.TABLES}
    assert len(lines) == 2 * len(publish.TABLES)
    assert "%s transmitter_offsets: 1 rows" % H2 in lines
    assert any(ln.startswith("%s trust: FAILED" % H1) for ln in lines)


def test_one_table_failing_does_not_block_the_others():
    srv = FakeServer()
    tables = _tables()
    tables["trust"] = [dict(x=float("nan"))]   # cannot be sent as JSON
    fails = publish.publish(tables, [H1], "sigmondb_publisher", PASSWORD,
                            urlopen=Router({H1: srv}), out=lambda s: None)
    assert [t for _, t, _ in fails] == ["trust"]
    assert "receivers" in srv.tables and "t3_calibration" in srv.tables


def test_the_password_never_appears_in_urls_logs_or_errors():
    class Leaky(FakeServer):
        def urlopen(self, req, timeout=None):
            self.urls.append(req.full_url)
            raise urllib.error.URLError("refused for sigmondb_publisher:%s" % PASSWORD)

    srv, leaky = FakeServer(), Leaky()
    lines = []
    fails = publish.publish(_tables(), [H1, H2], "sigmondb_publisher", PASSWORD,
                            urlopen=Router({H1: srv, H2: leaky}), out=lines.append)
    assert fails
    text = "\n".join(lines) + repr(fails) + "\n".join(srv.urls + leaky.urls)
    assert PASSWORD not in text
    assert "***" in text


# --- the CLI verb -----------------------------------------------------------------------------

def test_cli_publish_reads_env_and_exits_nonzero_on_any_failure(monkeypatch, capsys):
    src = FakeSource(solution=[], transmitter=[], receiver=[_rx("W1AW")])
    monkeypatch.setattr(cli, "connect", lambda conf: src)
    up, down = FakeServer(), FakeServer(down=True)
    monkeypatch.setattr(publish, "_default_urlopen", Router({H1: up, H2: down}))
    monkeypatch.setenv("CH_PUB_USER", "sigmondb_publisher")
    monkeypatch.setenv("CH_PUB_PASSWORD", PASSWORD)
    monkeypatch.setenv("CH_PUB_HOSTS", "%s %s" % (H1, H2))
    assert cli.main(["publish"]) == 1
    assert up.tables["receivers"][0]["grid6"] == "FN31pr"
    out = capsys.readouterr()
    assert PASSWORD not in out.out + out.err
    monkeypatch.setenv("CH_PUB_HOSTS", H1)
    assert cli.main(["publish"]) == 0


def test_cli_publish_without_credentials_fails(monkeypatch, capsys):
    for k in ("CH_PUB_USER", "CH_PUB_PASSWORD", "CH_PUB_HOSTS"):
        monkeypatch.delenv(k, raising=False)
    monkeypatch.setattr(cli, "connect", lambda conf: pytest.fail("must not connect"))
    assert cli.main(["publish"]) == 2
    assert "CH_PUB_" in capsys.readouterr().err


def test_transmitter_offsets_drop_solutions_older_than_the_publish_age():
    assert publish.PUBLISH_MAX_AGE_H == 3
    built = _utc(2026, 11, 12, 12).timestamp()
    # The name was withdrawn: newer solutions for that cluster carry no tx_id.  The older
    # named row must leave the public table within PUBLISH_MAX_AGE_H.
    sols = [_sol("hfrnet:A", _utc(2026, 11, 12, 8), 1_000_000),
            _sol("", _utc(2026, 11, 12, 11), 1_000_000)]
    assert publish.build_transmitter_offsets(sols, [_tx("hfrnet:A")], built) == []
    sols = [_sol("hfrnet:A", _utc(2026, 11, 12, 10), 1_000_000)]           # 2 h old
    assert [r["tx_id"] for r in publish.build_transmitter_offsets(sols, [], built)] == ["hfrnet:A"]


def test_a_gpsdo_receiver_still_publishes_only_grid6():
    from sigmondb.receivers import grid_to_latlon
    rx = dict(reporter_id="K3LR/D", lat=41.0123, lon=-80.0456, a_level="A1",
              pos_source="gpsdo", pos_sigma_m=10.0, valid_from=datetime(2026, 11, 14), version=2)
    out = publish.build_receivers([rx], 1.0)[0]
    assert out["grid6"] == publish.latlon_to_grid6(41.0123, -80.0456)
    assert (out["lat"], out["lon"]) == grid_to_latlon(out["grid6"])
    assert set(out) == {"reporter_id", "grid6", "lat", "lon", "a_level", "built_at_utc"}


# --- groups and heard -------------------------------------------------------------------------

def _codar_fixture():
    return dict(
        transmitter=[_tx("hfrnet:PREY", lat=29.0, lon=-95.0),     # Texas: East by lon > -100
                     _tx("hfrnet:CALI", lat=34.0, lon=-120.0)],   # California: West
        waveform=[_wf("hfrnet:PREY", _utc(2026, 11, 1), 4_450_500.0, -25_730.0),
                  _wf("hfrnet:CALI", _utc(2026, 11, 1), 4_450_500.0, -25_730.0)])


def _heard_row(reporter="W1AW", tx_id="", key="4.4505 -25.73", centre=4_450_500.0,
               rate=-25_730.0, **kw):
    r = dict(reporter_id=reporter, tx_id=tx_id, group_key=key, centre_hz=centre,
             rate_hz_per_s=rate, n_24h=5, t6_24h=1, n_7d=30, t6_7d=4,
             first_t=_utc(2026, 11, 8), last_t=_utc(2026, 11, 14))
    r.update(kw)
    return r


def _src(heard, receivers, **extra):
    base = _codar_fixture()
    base.update(extra)
    return dict(solution=[], receiver=receivers, **{"agg:heard": heard}, **base)


def test_heard_group_path():
    lat, lon = publish.grid_to_latlon("EM38ww")
    src = _src([_heard_row()], [_rx("K0X", lat=lat, lon=lon)])
    src["agg:heard"][0]["reporter_id"] = "K0X"
    groups, heard = publish.build_groups_heard(src, BUILT)
    assert len(groups) == 1
    g = groups[0]
    assert g["group_id"] == "4.4505 -25.73 E" and g["class"] == "ocean radar"
    assert g["family"] == "CODAR SeaSonde" and g["members"] == ["hfrnet:PREY"]
    assert g["name"] == "4.4505 MHz, -25.73 kHz/s (east coast)"
    assert g["coast"] == "E" and g["both_coasts"] == 1 and g["built_at_utc"] == BUILT
    assert len(heard) == 1
    h = heard[0]
    assert h["target_type"] == "group" and h["target_id"] == g["group_id"]
    assert h["path"] == "skywave" and h["n_7d"] == 30 and h["t6_7d"] == 4
    assert h["n_24h"] == 5 and h["t6_24h"] == 1
    assert h["first_utc"] == _utc(2026, 11, 8).timestamp()
    assert h["centre_mhz"] == pytest.approx(4.4505)


def test_heard_named_tx_path():
    # station ~40 km from PREY, 13.5 MHz
    lat, lon = publish.grid_to_latlon("EL29ho")
    src = _src([_heard_row(reporter="K0X", tx_id="hfrnet:PREY", key="13.5000 -50",
                           centre=13_500_000.0, rate=-50_000.0)],
               [_rx("K0X", lat=lat, lon=lon)],
               transmitter=[_tx("hfrnet:PREY", lat=lat + 0.36, lon=lon)])
    groups, heard = publish.build_groups_heard(src, BUILT)
    assert groups == []
    h = heard[0]
    assert h["target_type"] == "tx" and h["target_id"] == "hfrnet:PREY"
    assert h["path"] == "ground_range" and 30 < h["distance_km"] < 50


def test_heard_skips_reporter_without_position(capsys):
    src = _src([_heard_row(reporter="NOPOS")], [_rx("OTHER")])
    groups, heard = publish.build_groups_heard(src, BUILT)
    assert heard == []
    assert "skipped" in capsys.readouterr().out


def test_heard_skips_tx_without_position_and_unmatched_group():
    lat, lon = publish.grid_to_latlon("EM38ww")
    src = _src([_heard_row(reporter="K0X", tx_id="hfrnet:NOPOS"),
                _heard_row(reporter="K0X", key="9.0 -1", centre=9e6, rate=-1.0)],
               [_rx("K0X", lat=lat, lon=lon)],
               transmitter=[_tx("hfrnet:NOPOS", lat=None, lon=None)])
    assert publish.build_groups_heard(src, BUILT) == ([], [])


def test_heard_distance_uses_grid6_centre():
    from sigmondb.groundwave import great_circle_m
    true_lat, true_lon = 38.9101, -94.3333
    g = publish.latlon_to_grid6(true_lat, true_lon)
    clat, clon = publish.grid_to_latlon(g)
    src = _src([_heard_row(reporter="K0X")], [_rx("K0X", lat=true_lat, lon=true_lon)])
    _, heard = publish.build_groups_heard(src, BUILT)
    want = great_circle_m(clat, clon, 29.0, -95.0) / 1000.0
    true = great_circle_m(true_lat, true_lon, 29.0, -95.0) / 1000.0
    assert heard[0]["distance_km"] == pytest.approx(want)
    assert abs(want - true) > 0.5


def test_heard_merges_rows_of_one_named_tx():
    lat, lon = publish.grid_to_latlon("EM38ww")
    rows = [_heard_row(reporter="K0X", tx_id="hfrnet:PREY", key="a", n_7d=10, t6_7d=1),
            _heard_row(reporter="K0X", tx_id="hfrnet:PREY", key="b", n_7d=5, t6_7d=2)]
    _, heard = publish.build_groups_heard(_src(rows, [_rx("K0X", lat=lat, lon=lon)]), BUILT)
    assert len(heard) == 1 and heard[0]["n_7d"] == 15 and heard[0]["t6_7d"] == 3


def test_agg_queries_exclude_hfdl():
    assert publish.AGG_QUERIES
    for name, (cols, sql) in publish.AGG_QUERIES.items():
        assert sql.startswith("/* agg:%s */" % name)
        assert "startsWith(group_key, 'hfdl')" in sql and "'hfdl:%'" in sql


def test_read_source_runs_agg_queries():
    src = FakeSource(**{"agg:heard": [_heard_row()]})
    out = publish.read_source(src)
    assert out["agg:heard"][0]["n_7d"] == 30


def test_publish_creates_new_tables():
    srv = FakeServer()
    tables = dict(_tables(), groups=[], heard=[])
    fails = publish.publish(tables, [H1], "sigmondb_publisher", PASSWORD,
                            urlopen=Router({H1: srv}), out=lambda s: None)
    assert fails == [] and "groups" in srv.tables and "heard" in srv.tables


# --- the map must not stop the core tables -------------------------------------------------

CORE = ("transmitter_offsets", "trust", "receivers", "transmitters", "t3_calibration")
CHIRP = {"chirps", "chirp_reject_local", "chirp_menu"}


class _AggFails(FakeSource):
    def __init__(self, failing, **tables):
        super().__init__(**tables)
        self.failing = failing

    def query(self, sql, parameters=None, query_tz=None):
        if any("/* agg:%s */" % f in sql for f in self.failing):
            raise RuntimeError("query timed out")
        return super().query(sql, parameters, query_tz)


def _core_src(cls=FakeSource, *a):
    kw = dict(solution=[_sol("hfrnet:A", _utc(2026, 11, 12), 1_000_000)],
              transmitter=[_tx("hfrnet:A")], receiver=[_rx("W1AW")])
    return cls(*a, **kw)


def test_failing_aggregate_read_still_yields_core_tables():
    logs = []
    src = _core_src(_AggFails, ("heard", "ionosphere"))
    source = publish.read_source(src, out=logs.append)
    assert "agg:heard" not in source and "agg:ionosphere" not in source
    assert len(logs) == 2 and "agg:heard" in logs[0] and "RuntimeError" in logs[0]
    tables = publish.build_all(source, BUILT, out=logs.append)
    assert set(tables) == set(CORE) | CHIRP


def test_one_failing_aggregate_leaves_only_its_tables_out():
    src = _core_src(_AggFails, ("ionosphere",))
    tables = publish.build_all(publish.read_source(src, out=lambda s: None), BUILT)
    assert set(tables) == set(CORE) | {"groups", "heard"} | CHIRP


def test_a_failing_builder_leaves_only_its_tables_out(monkeypatch):
    def boom(*a, **k):
        raise ValueError("odd row")
    source = publish.read_source(_core_src(), out=lambda s: None)
    logs = []
    monkeypatch.setattr(publish, "build_groups_heard", boom)
    t = publish.build_all(source, BUILT, out=logs.append)
    assert set(t) == set(CORE) | {"ionosphere_hourly"} | CHIRP
    assert len(logs) == 1 and "groups/heard" in logs[0] and "ValueError: odd row" in logs[0]
    monkeypatch.undo()
    monkeypatch.setattr(publish, "build_ionosphere", boom)
    t = publish.build_all(source, BUILT, out=lambda s: None)
    assert set(t) == set(CORE) | {"groups", "heard"} | CHIRP


def test_publish_skips_absent_tables_and_leaves_their_copy():
    srv = FakeServer(tables={"groups": [{"group_id": "old"}]})
    out = []
    fails = publish.publish(_core_tables(), [H1], "sigmondb_publisher", PASSWORD,
                            urlopen=Router({H1: srv}), out=out.append)
    assert fails == []
    assert len(out) == len(CORE)
    assert srv.tables["groups"] == [{"group_id": "old"}]
    assert "heard" not in srv.tables and "ionosphere_hourly" not in srv.tables


def test_agg_queries_read_final():
    for name, (cols, sql) in publish.AGG_QUERIES.items():
        assert "FROM sigmondb.arrival FINAL WHERE" in sql


# --- chirps, local rejects and the channel menu ----------------------------------------------

from sigmondb import chirp as chirp_rules  # noqa: E402
from sigmondb.receivers import grid_pos_sigma_m, grid_to_latlon  # noqa: E402

CHIRP_TABLES = ("chirps", "chirp_reject_local", "chirp_menu")


def _chirp(cid, state="established", tx_id="", lat=None, lon=None, centre=4_450_500.0,
           rate=-25_730.0, **kw):
    r = dict(chirp_id=cid, group_key="%.4f %+.2f" % (centre / 1e6, rate / 1e3), kind="codar",
             state=state, centre_hz=centre, span_hz=25_730.0, rate_hz_per_s=rate,
             period_s=1.0, duration_s=1.0, gate_period_s=None, lat=lat, lon=lon,
             pos_sigma_m=500.0, pos_source="locate", offset_ns=None, offset_sigma_ns=None,
             last_heard=datetime(2026, 11, 12, 3), n_stations=2, heard_hours=0,
             tx_id=tx_id, updated_at=datetime(2026, 11, 12, 4))
    r.update(kw)
    return r


def _chirp_src(chirps=(), rejects=(), receivers=(), **kw):
    return FakeSource(solution=kw.pop("solution", []), transmitter=kw.pop("transmitter", []),
                      receiver=list(receivers), chirp=list(chirps),
                      chirp_reject_local=list(rejects), **kw)


def _built(src):
    return publish.build_all(publish.read_source(src, out=lambda s: None), BUILT,
                             out=lambda s: None)


def test_chirps_only_established_and_dormant():
    states = ("seen", "candidate", "probing", "established", "dormant", "retired",
              "rejected", "dropped")
    src = _chirp_src([_chirp("c-%s" % s, state=s, lat=30.25, lon=-80.5) for s in states])
    rows = _built(src)["chirps"]
    assert sorted(r["state"] for r in rows) == ["dormant", "established"]
    assert set(rows[0]) == {c for c, _ in publish.SCHEMAS["chirps"]}


def test_chirps_offset_sign_matches_transmitter_offsets():
    sol = _sol("hfrnet:LATE", _utc(2026, 11, 12, 3), 2_500_000)
    src = _chirp_src([_chirp("c1", tx_id="hfrnet:LATE", lat=30.25, lon=-80.5,
                             offset_ns=2_500_000, offset_sigma_ns=100_000.0)],
                     solution=[sol], transmitter=[_tx("hfrnet:LATE")])
    t = publish.build_all(publish.read_source(src, out=lambda s: None),
                          _utc(2026, 11, 12, 4).timestamp(), out=lambda s: None)
    assert t["transmitter_offsets"][0]["offset_ms"] == pytest.approx(2.5)
    assert t["chirps"][0]["offset_ms"] == t["transmitter_offsets"][0]["offset_ms"]
    assert t["chirps"][0]["sigma_ms"] == pytest.approx(0.1)


def test_reject_local_active_only():
    rej = dict(reporter_id="W1XYZ", centre_hz=4_450_500.0, rate_hz_per_s=-25_730.0,
               period_s=1.0, span_hz=25_730.0, chirp_id="c1", reason="local QRM",
               set_by="someone", set_at=datetime(2026, 11, 10))
    src = _chirp_src(rejects=[dict(rej, active=1),
                              dict(rej, reporter_id="K0ABC", active=0)])
    rows = _built(src)["chirp_reject_local"]
    assert rows == [dict(reporter_id="W1XYZ", centre_mhz=pytest.approx(4.4505),
                         rate_khz_s=pytest.approx(-25.73), period_s=1.0,
                         span_khz=pytest.approx(25.73), chirp_id="c1", reason="local QRM",
                         built_at_utc=BUILT)]
    assert "set_by" not in {c for c, _ in publish.SCHEMAS["chirp_reject_local"]}


GRID = "FN20kk"   # a made-up site: tests use grid centres only


def _menu_rows(rows, rid):
    return sorted(({k: v for k, v in r.items() if k != "reporter_id"}
                   for r in rows if r["reporter_id"] == rid),
                  key=lambda r: (r["hour_utc"], r["channel_key"]))


def test_menu_from_grid_centre_only():
    lat, lon = grid_to_latlon(GRID)
    off = lon + 3.0 / (111.32 * math.cos(math.radians(lat)))   # 3 km east, same grid6
    assert publish.latlon_to_grid6(lat, off) == GRID
    chirps = [_chirp("c1", lat=40.5, lon=-73.0), _chirp("c2", state="candidate",
                                                        centre=13_450_000.0, rate=-50_000.0)]
    sweep = [dict(reporter_id="W1XYZ", centre_hz=4_450_500.0, rate_hz_per_s=-25_730.0,
                  period_s=1.0, span_hz=25_730.0, duration_s=1.0, clipped=0,
                  hour=datetime(2026, 11, 12, 3), n=10, snr_med=12.0, n_dwells=10)]
    at_centre = _built(_chirp_src(chirps, receivers=[_rx("W1XYZ", lat=lat, lon=lon)],
                                  **{"agg:sweep_hourly": sweep}))["chirp_menu"]
    stored = _built(_chirp_src(chirps, receivers=[_rx("W1XYZ", lat=lat, lon=off)],
                               **{"agg:sweep_hourly": sweep}))["chirp_menu"]
    assert at_centre and _menu_rows(at_centre, "W1XYZ") == _menu_rows(stored, "W1XYZ")
    assert {r["hour_utc"] for r in at_centre} == set(range(24))
    assert set(at_centre[0]) == {c for c, _ in publish.SCHEMAS["chirp_menu"]}


def test_no_station_coordinate_published():
    for t in CHIRP_TABLES:
        assert t in publish.TABLES
        cols = {c for c, _ in publish.SCHEMAS[t]}
        bad = cols & {"lat", "lon", "distance_km"}
        assert bad == ({"lat", "lon"} if t == "chirps" else set()), t


def test_chirp_tables_optional(monkeypatch):
    def boom(*a, **k):
        raise ValueError("odd chirp")
    src = _chirp_src([_chirp("c1", lat=30.25, lon=-80.5)], receivers=[_rx("W1XYZ")],
                     solution=[_sol("hfrnet:A", _utc(2026, 11, 12), 1_000_000)],
                     transmitter=[_tx("hfrnet:A")])
    source = publish.read_source(src, out=lambda s: None)
    logs = []
    monkeypatch.setattr(publish, "build_chirps", boom)
    t = publish.build_all(source, _utc(2026, 11, 12, 1).timestamp(), out=logs.append)
    assert "chirps" not in t and len(t["transmitter_offsets"]) == 1
    assert "chirp_reject_local" in t and "chirp_menu" in t
    assert any("chirps" in s and "ValueError: odd chirp" in s for s in logs)
    monkeypatch.undo()
    monkeypatch.setattr(publish, "build_chirp_menu", boom)
    t = publish.build_all(source, BUILT, out=lambda s: None)
    assert "chirp_menu" not in t and "chirps" in t


def test_chirp_source_read_failing_leaves_only_chirp_tables_out():
    class _ChirpFails(FakeSource):
        def query(self, sql, parameters=None, query_tz=None):
            if "FROM sigmondb.chirp FINAL" in sql:
                raise RuntimeError("no such table")
            return super().query(sql, parameters, query_tz)
    logs = []
    source = publish.read_source(_core_src(_ChirpFails), out=logs.append)
    t = publish.build_all(source, BUILT, out=logs.append)
    assert set(t) == set(CORE) | {"groups", "heard", "ionosphere_hourly",
                                  "chirp_reject_local"}
    assert any("chirp" in s and "RuntimeError" in s for s in logs)


def test_chirp_aggregates_take_the_rules_hours():
    src = _chirp_src()
    publish.read_source(src, out=lambda s: None)
    want = __import__("sigmondb.chirp_evidence", fromlist=["x"]).agg_params(
        chirp_rules.load_rules())
    for name in publish.CHIRP_AGG_QUERIES:
        sql = [s for s in src.sql if s.startswith("/* agg:%s */" % name)]
        assert sql and "{hours:UInt32}" in sql[0]
    assert src.params[[i for i, s in enumerate(src.sql)
                       if s.startswith("/* agg:sweep_hourly */")][0]] == want


def test_menu_rows_bounded():
    rules = chirp_rules.load_rules()
    assert rules.menu.top_n == 20
    chirps = [_chirp("c%02d" % i, state=("established", "candidate")[i % 2],
                     centre=3_000_000.0 + i * 100_000.0, lat=30.0 + i * 0.1, lon=-80.0)
              for i in range(60)]
    rxs = [_rx("W1XYZ", *grid_to_latlon("FN20kk")), _rx("K0ABC", *grid_to_latlon("EM29qa"))]
    rows = _built(_chirp_src(chirps, receivers=rxs))["chirp_menu"]
    assert len(rows) <= 2 * 24 * 40
    per = {}
    for r in rows:
        per[(r["reporter_id"], r["hour_utc"])] = per.get((r["reporter_id"], r["hour_utc"]), 0) + 1
    assert len(per) == 48 and max(per.values()) <= 40 and min(per.values()) > 20


def test_unnamed_chirp_position_grid4_only():
    assert publish.latlon_to_grid4(38.123, -123.456) == "CM88"
    src = _chirp_src(
        [_chirp("c-cand", tx_id="cand:c-cand", lat=38.123, lon=-123.456, pos_sigma_m=800.0),
         _chirp("c-named", tx_id="hfrnet:PREY", lat=38.123, lon=-123.456, pos_sigma_m=800.0,
                centre=13_450_000.0)],
        transmitter=[_tx("cand:c-cand", kind="unknown", source="discovered",
                         lat=38.123, lon=-123.456, pos_sigma_m=800.0),
                     _tx("hfrnet:PREY", source="hfrnet", lat=38.123, lon=-123.456)])
    t = _built(src)
    by = {r["chirp_id"]: r for r in t["chirps"]}
    cand, named = by["c-cand"], by["c-named"]
    assert (cand["lat"], cand["lon"]) == (pytest.approx(38.5), pytest.approx(-123.0))
    assert cand["pos_precision"] == "grid4"
    assert cand["pos_sigma_m"] >= grid_pos_sigma_m("CM88") > 800.0
    assert (named["lat"], named["lon"]) == (38.123, -123.456)
    assert named["pos_precision"] == "full" and named["pos_sigma_m"] == 800.0
    tx = {r["tx_id"]: r for r in t["transmitters"]}
    assert (tx["cand:c-cand"]["lat"], tx["cand:c-cand"]["lon"]) == (pytest.approx(38.5),
                                                                    pytest.approx(-123.0))
    assert tx["cand:c-cand"]["pos_sigma_m"] >= grid_pos_sigma_m("CM88")
    assert (tx["hfrnet:PREY"]["lat"], tx["hfrnet:PREY"]["lon"]) == (38.123, -123.456)


def test_heard_places_a_discovered_transmitter_at_its_grid4_centre():
    """heard.distance_km from several grid6 centres would locate a discovered transmitter as
    well as its position: it too counts from the grid4 centre."""
    rx = _rx("W1XYZ", *grid_to_latlon("CM87xx"))
    src = _src([_heard_row(reporter="W1XYZ", tx_id="cand:c-cand")], [rx],
               transmitter=[_tx("cand:c-cand", kind="unknown", source="discovered",
                                lat=38.123, lon=-123.456)])
    h = publish.build_all(src, BUILT, out=lambda s: None)["heard"][0]
    st = grid_to_latlon("CM87xx")
    assert h["distance_km"] == pytest.approx(
        publish.great_circle_m(st[0], st[1], 38.5, -123.0) / 1000.0)


def test_chirp_queries_keep_hfdl_out():
    src = _chirp_src([_chirp("h1", kind="hfdl", lat=30.0, lon=-80.0),
                      _chirp("h2", tx_id="hfdl:13", lat=30.0, lon=-80.0)])
    assert _built(src)["chirps"] == []
    chirp_sql = [s for s in src.sql if "FROM sigmondb.chirp FINAL" in s][0]
    assert "kind != 'hfdl'" in chirp_sql and "'hfdl:%'" in chirp_sql
    t6 = publish.CHIRP_AGG_QUERIES["t6_arrival"][1]
    assert "startsWith(group_key, 'hfdl')" in t6 and "origin = 't6_anchor'" in t6
