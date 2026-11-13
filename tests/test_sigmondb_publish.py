"""sigmondb publish: build the five sigmondb_public tables and replace each WHOLE on every
public host.  No ClickHouse: a fake source client stands for wd30's sigmondb, and a fake
urlopen stands for each public server's HTTP interface."""
import base64
import json
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

    def query(self, sql, parameters=None, query_tz=None):
        self.sql.append(sql)
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
                     confirming_reporters=[], built_at_utc=BUILT)
    assert got["hfrnet:C"]["kind"] == "oth" and got["hfrnet:C"]["offset_ms"] == -0.5


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


# --- build_all and the source read -----------------------------------------------------------

def test_build_all_reads_final_and_t3_calibration_is_empty():
    src = FakeSource(solution=[_sol("hfrnet:A", _utc(2026, 11, 12), 1_000_000)],
                     transmitter=[_tx("hfrnet:A")], receiver=[_rx("W1AW")])
    tables = publish.build_all(publish.read_source(src), BUILT)
    assert list(tables) == list(publish.TABLES)
    assert tables["t3_calibration"] == []
    assert [len(tables[t]) for t in publish.TABLES] == [1, 1, 1, 1, 0]
    assert all(" FINAL" in s for s in src.sql)
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


def _tables():
    return dict(transmitter_offsets=[dict(tx_id="hfrnet:A", built_at_utc=BUILT)],
                trust=[], receivers=[], transmitters=[], t3_calibration=[])


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
