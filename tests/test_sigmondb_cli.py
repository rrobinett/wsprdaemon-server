"""python -m sigmondb verbs.  No ClickHouse: clickhouse_connect is stubbed."""
import math
import os
import sys
import types

import pytest
from datetime import datetime

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from sigmondb import cli, ddl, rows  # noqa: E402
from sigmondb.receivers import ROUNDING_SIGMA_M  # noqa: E402
from test_sigmondb_load import valid_row  # noqa: E402


class FakeClient:
    def __init__(self):
        self.commands, self.inserts = [], []

    def command(self, sql):
        self.commands.append(sql)

    def insert(self, table, data, column_names=None):
        self.inserts.append((table, len(data)))


@pytest.fixture(autouse=True)
def _no_real_positions_file(tmp_path, monkeypatch):
    """The default positions file lives under HOME; keep a real one out of every test."""
    monkeypatch.setenv("HOME", str(tmp_path / "home"))


def _stub(monkeypatch):
    seen = {}
    mod = types.ModuleType("clickhouse_connect")

    def get_client(**kw):
        seen.update(kw)
        seen["client"] = FakeClient()
        return seen["client"]

    mod.get_client = get_client
    monkeypatch.setitem(sys.modules, "clickhouse_connect", mod)
    return seen


def test_cli_reads_credentials_from_conf_only(tmp_path, monkeypatch):
    conf = tmp_path / "clickhouse.conf"
    conf.write_text('# c\nCLICKHOUSE_HOST="db.example"\nCLICKHOUSE_PORT="8123"\n'
                    'CLICKHOUSE_ROOT_ADMIN_USER="admin"\n'
                    'CLICKHOUSE_ROOT_ADMIN_PASSWORD="s3cret pw"\n')
    seen = _stub(monkeypatch)
    d = tmp_path / "out"
    d.mkdir()
    rows.write_rows(d / "arrival.jsonl", [valid_row("arrival", time=datetime(2026, 11, 8), delay_ns=1)])
    rows.write_rows(d / "transmitter.jsonl", [])
    assert cli.main(["load", "--dir", str(d), "--conf", str(conf)]) == 0
    assert (seen["host"], seen["port"], seen["username"], seen["password"]) == (
        "db.example", 8123, "admin", "s3cret pw")
    assert ("sigmondb.arrival", 1) in seen["client"].inserts
    # no verb takes a password argument
    for verb in ("load", "solve"):
        for flag in ("--password", "--pw", "--passwd"):
            try:
                cli.main([verb, flag, "x"])
            except SystemExit as e:
                assert e.code == 2
            else:
                raise AssertionError("accepted " + flag)


def test_load_order_follows_tables(tmp_path, monkeypatch):
    conf = tmp_path / "c.conf"
    conf.write_text('CLICKHOUSE_HOST="h"\nCLICKHOUSE_PORT="1"\nCLICKHOUSE_ROOT_ADMIN_USER="u"\n'
                    'CLICKHOUSE_ROOT_ADMIN_PASSWORD="p"\n')
    seen = _stub(monkeypatch)
    for t in ("solution", "arrival", "receiver"):
        rows.write_rows(tmp_path / f"{t}.jsonl", [valid_row(t)])
    cli.main(["load", "--dir", str(tmp_path), "--conf", str(conf)])
    assert [t for t, _ in seen["client"].inserts] == [
        "sigmondb.receiver", "sigmondb.arrival", "sigmondb.solution"]


def test_solve_from_files(tmp_path):
    from test_sigmondb_groundwave import _arrivals, K3LR
    rows.write_rows(tmp_path / "a.jsonl", _arrivals(1_000_000))
    rows.write_rows(tmp_path / "r.jsonl", [valid_row("receiver", **K3LR)])
    out = tmp_path / "sol.jsonl"
    assert cli.main(["solve", "--arrivals", str(tmp_path / "a.jsonl"),
                     "--receivers", str(tmp_path / "r.jsonl"), "--out", str(out)]) == 0
    got = rows.read_rows(out)
    assert len(got) == 1 and got[0]["method"] == "ground_wave"


def test_seed_writes_row_files(tmp_path):
    here = os.path.dirname(os.path.abspath(__file__))
    rx = os.path.join(os.path.dirname(here), "sigmondb", "seed", "receivers.toml")
    out = tmp_path / "o"
    assert cli.main(["seed", "--receivers", rx, "--snapshot", "2026-11-09",
                     "--out", str(out)]) == 0
    assert len(rows.read_rows(out / "receiver.jsonl")) > 0


def test_ddl_help_says_load_creates_schema(capsys):
    try:
        cli.main(["ddl", "--help"])
    except SystemExit:
        pass
    out = capsys.readouterr().out
    assert "print" in out.lower() and "`load` creates" in out.replace("\n", " ")


def test_solve_max_sd_us(tmp_path):
    from test_sigmondb_groundwave import _arrivals
    seen = []
    real = cli.fixed_clusters

    def spy(arr, rx, **kw):
        seen.append(kw)
        return real(arr, rx, **kw)

    cli.fixed_clusters = spy
    try:
        rows.write_rows(tmp_path / "a.jsonl", _arrivals(1_000_000))
        for extra in ([], ["--max-sd-us", "40"]):
            cli.main(["solve", "--arrivals", str(tmp_path / "a.jsonl"),
                      "--out", str(tmp_path / "s.jsonl")] + extra)
    finally:
        cli.fixed_clusters = real
    assert [k["max_sd_ns"] for k in seen] == [20_000.0, 40_000.0]


def test_two_backfills_into_one_dir_both_load(tmp_path, monkeypatch):
    from sigmondb import backfill_codar as bc, backfill_hfdl as bh, provenance
    cap = lambda rid: valid_row("capture", reporter_id=rid)
    arr = lambda rid: valid_row("arrival", reporter_id=rid)
    monkeypatch.setattr(provenance, "load_minutes", lambda paths: {})
    monkeypatch.setattr(bc, "SETS", [dict(kind="res", path="p", reporter_id="CODAR1",
                                          minutes=[])])
    monkeypatch.setattr(bc, "read_res_jsonl", lambda p, rid, m: ([cap(rid)], [arr(rid)] * 2))
    monkeypatch.setattr(bh, "SETS", [dict(path="q", reporter_id="HFDL1")])
    monkeypatch.setattr(bh, "read_squitters", lambda p, rid: ([cap(rid)] * 3, [arr(rid)] * 4))
    out = tmp_path / "o"
    assert cli.main(["backfill-codar", "--root", str(tmp_path), "--out", str(out)]) == 0
    assert cli.main(["backfill-hfdl", "--root", str(tmp_path), "--out", str(out)]) == 0
    assert sorted(p.name for p in out.iterdir()) == [
        "arrival.codar.jsonl", "arrival.hfdl.jsonl", "capture.codar.jsonl", "capture.hfdl.jsonl"]
    rows.write_rows(out / "arrival.jsonl", [valid_row("arrival", reporter_id="SEED")])
    conf = tmp_path / "c.conf"
    conf.write_text('CLICKHOUSE_HOST="h"\nCLICKHOUSE_PORT="1"\nCLICKHOUSE_ROOT_ADMIN_USER="u"\n'
                    'CLICKHOUSE_ROOT_ADMIN_PASSWORD="p"\n')
    seen = _stub(monkeypatch)
    assert cli.main(["load", "--dir", str(out), "--conf", str(conf)]) == 0
    # tables in ddl order; within a table, sorted by file name: arrival.codar, arrival.hfdl, arrival.jsonl.
    assert seen["client"].inserts == [
        ("sigmondb.capture", 1), ("sigmondb.capture", 3),
        ("sigmondb.arrival", 2), ("sigmondb.arrival", 4), ("sigmondb.arrival", 1)]


# --- solve --from-db --------------------------------------------------------------------------
from datetime import timedelta, timezone  # noqa: E402


class _Result:
    def __init__(self, rows):
        self.result_rows = rows


class QueryClient:
    """Records each query; returns `tables[name]` (dicts) as rows of the SELECTed columns.
    It applies no WHERE: fixtures hold only rows the query would return.  command() is a
    no-op; insert() records (table, row dict) pairs in `inserted`."""

    def __init__(self, tables, tz_kw=True):
        self.tables, self.calls, self.tz_kw = tables, [], tz_kw
        self.commands, self.inserted = [], []

    def query(self, sql, parameters=None, **kw):
        if "query_tz" in kw and not self.tz_kw:
            raise TypeError("query() got an unexpected keyword argument 'query_tz'")
        self.calls.append(dict(sql=sql, parameters=parameters, **kw))
        head, tail = sql.split(" FROM %s." % ddl.DATABASE, 1)
        name = tail.split()[0]
        cols = [c.strip() for c in head.split("SELECT", 1)[1].split(",")]
        assert set(cols) <= set(ddl.COLUMNS[name]), cols
        return _Result([tuple(r.get(c) for c in cols) for r in self.tables.get(name, [])])

    def command(self, sql):
        self.commands.append(sql)

    def insert(self, table, data, column_names=None):
        self.inserted += [(table, dict(zip(column_names, row))) for row in data]


def _db(monkeypatch, tables, tz_kw=True):
    client = QueryClient(tables, tz_kw)
    monkeypatch.setattr(cli, "connect", lambda conf: client)
    return client


def _arr(rid, gkey, dv, t=datetime(2026, 11, 7, 3, tzinfo=timezone.utc), delay=1):
    return dict(time=t, reporter_id=rid, group_key=gkey, detector_version=dv, delay_ns=delay,
                u_epoch_ns=230.0, rate_hz_per_s=-25_733.913, tx_id="")


def _ns(*extra):
    return cli.build_parser().parse_args(["solve", "--from-db", "--out", "x"] + list(extra))


def test_from_db_reads_final_and_keeps_latest_detector_version(monkeypatch):
    c = _db(monkeypatch, {"arrival": [
        _arr("K3LR/D", "4.4630 -25.73", "offline-2026-11", delay=1),
        _arr("K3LR/D", "4.4630 -25.73", "offline-2026-12", delay=2),
        _arr("K3LR/D", "13.4750 -99.26", "offline-2026-11", delay=3),
        _arr("W2NAF", "4.4630 -25.73", "offline-2026-10", delay=4)]})
    arr, _, _ = cli._from_db(_ns())
    assert sorted(r["delay_ns"] for r in arr) == [2, 3, 4]
    for call in c.calls:
        assert " FINAL" in call["sql"]
    asql = [k["sql"] for k in c.calls if ".arrival" in k["sql"]][0]
    assert "FROM sigmondb.arrival FINAL" in asql and "detector_version =" not in asql


def test_from_db_detector_version_overrides(monkeypatch):
    c = _db(monkeypatch, {"arrival": [
        _arr("K3LR/D", "4.4630 -25.73", "offline-2026-11", delay=1),
        _arr("W2NAF", "4.4630 -25.73", "offline-2026-11", delay=4)]})
    arr, _, _ = cli._from_db(_ns("--detector-version", "offline-2026-11"))
    assert sorted(r["delay_ns"] for r in arr) == [1, 4]
    call = [k for k in c.calls if ".arrival" in k["sql"]][0]
    assert "detector_version = {dv:String}" in call["sql"]
    assert call["parameters"] == {"dv": "offline-2026-11", "hours": 720}
    assert "INTERVAL {hours:UInt32} HOUR" in call["sql"] and "Float" not in call["sql"]


def test_from_db_converts_times_to_naive_utc(monkeypatch):
    cdt = timezone(timedelta(hours=-5))                  # 22:00 CDT on 11-06 = 03:00Z 11-07
    c = _db(monkeypatch, {"arrival": [
        _arr("K3LR/D", "g", "v", t=datetime(2026, 11, 6, 22, 0, tzinfo=cdt))]})
    arr, _, _ = cli._from_db(_ns())
    assert arr[0]["time"] == datetime(2026, 11, 7, 3, 0) and arr[0]["time"].tzinfo is None
    assert all(k.get("query_tz") == "UTC" for k in c.calls)


def test_from_db_works_without_query_tz_support(monkeypatch):
    cdt = timezone(timedelta(hours=-5))
    _db(monkeypatch, {"arrival": [
        _arr("K3LR/D", "g", "v", t=datetime(2026, 11, 6, 22, 0, tzinfo=cdt))]}, tz_kw=False)
    arr, _, _ = cli._from_db(_ns())
    assert arr[0]["time"] == datetime(2026, 11, 7, 3, 0)


def test_solve_max_ground_km_and_toml_receivers(tmp_path, monkeypatch):
    from test_sigmondb_groundwave import _arrivals
    seen = {}
    real = cli.ground_wave_solutions

    def spy(cl, tx, rx, v, **kw):
        seen["kw"], seen["rx"] = kw, rx
        return real(cl, tx, rx, v, **kw)

    monkeypatch.setattr(cli, "ground_wave_solutions", spy)
    rows.write_rows(tmp_path / "a.jsonl", _arrivals(1_000_000))
    here = os.path.dirname(os.path.abspath(__file__))
    toml = os.path.join(os.path.dirname(here), "sigmondb", "seed", "receivers.toml")
    assert cli.main(["solve", "--arrivals", str(tmp_path / "a.jsonl"), "--receivers", toml,
                     "--max-ground-km", "150", "--out", str(tmp_path / "s.jsonl")]) == 0
    assert seen["kw"] == {"max_ground_km": 150.0}
    assert {r["reporter_id"] for r in seen["rx"]} >= {"K3LR/D", "WB6CXC/7"}
    assert len(rows.read_rows(tmp_path / "s.jsonl")) == 1


def test_live_and_backfill_rows_of_one_group_both_survive(monkeypatch):
    t1, t2 = datetime(2026, 11, 7, 3, tzinfo=timezone.utc), datetime(2026, 11, 13, 3, tzinfo=timezone.utc)
    _db(monkeypatch, {"arrival": [
        _arr("W2NAF", "4.4630 -25.73", "offline-2026-11", t=t1, delay=1),
        _arr("W2NAF", "4.4630 -25.73", "codar-sounder/0.7.0", t=t2, delay=2),
        _arr("W2NAF", "4.4630 -25.73", "codar-sounder/0.7.0", t=t1, delay=3)]})
    arr, _, _ = cli._from_db(_ns())
    assert sorted(r["delay_ns"] for r in arr) == [1, 2]   # t1: offline wins; t2: live kept


# --- attribute ------------------------------------------------------------------------------
import random  # noqa: E402

_T0 = datetime(2026, 11, 10, tzinfo=timezone.utc)
KFS_ENG = {"radiod": "KFS-OMNI-status.local", "sweep_repetition_hz": "1", "tier": "T6"}


def _codar_rows(rid, gkey, D_ms, n, eng=None, seed=1, label=None):
    """n captures, 10 min apart, one arrival each at D +- 2.5 us, in arrival-table shape."""
    rnd = random.Random(seed)
    centre, rate = (float(x) for x in gkey.split()[:2])
    return [valid_row("arrival", time=_T0 + timedelta(minutes=10 * i), reporter_id=label or rid,
                      group_key=gkey, centre_hz=centre * 1e6, rate_hz_per_s=rate * 1e3,
                      delay_ns=int(round((D_ms + rnd.uniform(-0.0025, 0.0025)) * 1e6)),
                      level_db=20.0, clean_tag="A", u_epoch_ns=230.0,
                      detector_version="codar-sounder/0.7.0", engineering=dict(eng or {}))
            for i in range(n)]


# KFS-MAIN carries KFS-OMNI's receptions: a cluster and its 1.632 ms twin, as today.
KFS_LIVE_ROWS = (_codar_rows("KFS", "4.7850 -25.73", 499.444, 100, KFS_ENG, label="KFS-MAIN")
                 + _codar_rows("KFS", "4.7850 -25.73", 501.076, 100, KFS_ENG, seed=2,
                               label="KFS/MAIN"))
N8UR_ROWS = _codar_rows("N8UR", "4.4630 -25.73", 40.0, 50)
K3LR_ROWS = _codar_rows("K3LR/D", "4.4630 -25.73", 41.0, 50, seed=3)
_VF = datetime(2026, 11, 9)
TX = [valid_row("transmitter", tx_id=t, kind="codar", lat=la, lon=lo, pos_source="hfrnet",
                valid_from=_VF, version=1)
      for t, la, lo in (("hfrnet:TSTB", 37.0, -123.0), ("hfrnet:TSTA", 39.5, -74.0))]
WF = [valid_row("waveform", tx_id=t, valid_from=_VF, centre_hz=f, rate_hz_per_s=-25_733.913,
                period_s=1.0)
      for t, f in (("hfrnet:TSTB", 4.785e6), ("hfrnet:TSTA", 4.463e6))]
RX_ROWS = [valid_row("receiver", reporter_id="KFS-OMNI", radiod_id="KFS-OMNI-status.local",
                     lat=37.5, lon=-122.5, pos_source="gpsdo", valid_from=_VF, version=1)]
K3LR_RX = valid_row("receiver", reporter_id="K3LR/D", radiod_id="K3LR-status.local",
                    lat=41.1875, lon=-80.458333, pos_source="typed_grid", valid_from=_VF,
                    version=1)


def test_cli_attribute_writes_both_policies_and_exits_zero_with_nothing_named(monkeypatch, capsys):
    c = _db(monkeypatch, {"arrival": KFS_LIVE_ROWS, "transmitter": TX, "waveform": WF, "receiver": RX_ROWS})
    assert cli.main(["attribute", "--n-fake", "20"]) == 0
    ins = [r for t, r in c.inserted if t == "sigmondb.attribution"]
    assert {r["mode_policy"] for r in ins} == {"default", "gw_only_900km"}   # 4.785 MHz < 6 MHz
    assert all(r["status"] == "unattributable" for r in ins)
    assert "0 named" in capsys.readouterr().out
    assert any("CREATE TABLE IF NOT EXISTS sigmondb.attribution" in s for s in c.commands)
    asql = [k for k in c.calls if ".arrival" in k["sql"]][0]
    for frag in ("FROM sigmondb.arrival FINAL", "u_epoch_ns IS NOT NULL", "u_epoch_ns <= 1000",
                 "clean_tag = 'A'", "rate_hz_per_s != 0", "INTERVAL {hours:UInt32} HOUR"):
        assert frag in asql["sql"], frag
    assert asql["parameters"] == {"hours": 720}
    tsql = [k["sql"] for k in c.calls if ".transmitter" in k["sql"]][0]
    assert "kind = 'codar'" in tsql and "ORDER BY tx_id" in tsql


def test_cli_attribute_sets_aside_a_reporter_without_position(monkeypatch, tmp_path):
    c = _db(monkeypatch, {"arrival": N8UR_ROWS + K3LR_ROWS, "transmitter": TX, "waveform": WF,
                          "receiver": [K3LR_RX]})
    assert cli.main(["attribute", "--n-fake", "20", "--out", str(tmp_path / "a.jsonl")]) == 0
    out = rows.read_rows(tmp_path / "a.jsonl")
    assert out and all(r["set_aside"]["N8UR"] == "no receiver position" for r in out)
    assert c.inserted == [] and c.commands == []          # --out leaves the database alone


KFS_TRANSPORT = {"KFS-OMNI-status.local": ["KFS-MAIN", "KFS/MAIN"]}
_KFS_RX = dict(valid_row("receiver"), reporter_id="KFS-OMNI", radiod_id="KFS-OMNI-status.local")


def test_kfs_main_rows_count_as_the_kfs_omni_radiod():
    eng = {"radiod": "KFS-OMNI-status.local", "sweep_repetition_hz": "1"}
    arr = [dict(_arr("KFS-MAIN", "g", "v"), engineering=eng), dict(_arr("KFS/MAIN", "g", "v"), engineering=eng)]
    out = cli._at_radiod(arr, [_KFS_RX], KFS_TRANSPORT)
    assert {r["reporter_id"] for r in out} == {"KFS-OMNI"}
    assert {r["carried_by"] for r in out} == {"KFS-MAIN", "KFS/MAIN"}
    assert {r["reporter_id"] for r in arr} == {"KFS-MAIN", "KFS/MAIN"}    # inputs untouched


def test_at_radiod_leaves_an_unmatched_arrival_alone():
    arr = [dict(_arr("W2NAF", "g", "v"), engineering={"radiod": "elsewhere.local"}),
           _arr("K3LR/D", "g", "v")]                       # engineering absent (None)
    out = cli._at_radiod(arr, [_KFS_RX], KFS_TRANSPORT)
    assert [r["reporter_id"] for r in out] == ["W2NAF", "K3LR/D"]
    assert all("carried_by" not in r for r in out)


def test_at_radiod_ignores_a_reporter_not_listed_as_that_radiods_transporter():
    # A station that names another station's radiod must not become that station: it would
    # fake a second station and hand any site a false two-station fit.
    arr = [dict(_arr("W2NAF", "g", "v"), engineering={"radiod": "KFS-OMNI-status.local"})]
    out = cli._at_radiod(arr, [_KFS_RX], KFS_TRANSPORT)
    assert out[0]["reporter_id"] == "W2NAF" and "carried_by" not in out[0]
    assert cli._at_radiod(arr, [_KFS_RX], {})[0]["reporter_id"] == "W2NAF"


def test_at_radiod_refuses_a_radiod_id_two_receivers_share(capsys):
    twin = dict(_KFS_RX, reporter_id="KFS-OTHER")
    arr = [dict(_arr("KFS-MAIN", "g", "v"), engineering={"radiod": "KFS-OMNI-status.local"})]
    out = cli._at_radiod(arr, [_KFS_RX, twin], KFS_TRANSPORT)
    assert out[0]["reporter_id"] == "KFS-MAIN"
    assert "KFS-OMNI-status.local" in capsys.readouterr().err


def test_seed_transporters_list_kfs_main():
    assert cli.load_transporters() == KFS_TRANSPORT


def test_one_capture_under_both_kfs_labels_counts_once():
    eng = {"radiod": "KFS-OMNI-status.local"}
    a = dict(_arr("KFS-MAIN", "g", "v", delay=1_234_567), engineering=eng)
    b = dict(_arr("KFS/MAIN", "g", "v", delay=1_234_890), engineering=eng)   # same 1 us bin
    c = dict(_arr("KFS/MAIN", "g", "v", delay=1_236_000), engineering=eng)   # another bin
    kept, dropped = cli._one_per_capture(cli._at_radiod([a, b, c], [_KFS_RX], KFS_TRANSPORT))
    assert [r["delay_ns"] for r in kept] == [1_234_567, 1_236_000] and dropped == 1
    # the arrival table's intDiv truncates toward zero: -500 and 500 share bin 0 (a floor
    # would split them into -1 and 0); -1500 lands in bin -1
    neg = [dict(_arr("X", "g", "v", delay=d)) for d in (-500, 500, -1500)]
    assert [r["delay_ns"] for r in cli._one_per_capture(neg)[0]] == [-500, -1500]


def test_solve_from_db_counts_kfs_main_at_its_radiod(monkeypatch, capsys):
    dup = dict(KFS_LIVE_ROWS[0], reporter_id="KFS/MAIN")             # one capture, two labels
    _db(monkeypatch, {"arrival": KFS_LIVE_ROWS[:3] + [dup], "receiver": RX_ROWS})
    arr, _, _ = cli._from_db(_ns())
    assert {r["reporter_id"] for r in arr} == {"KFS-OMNI"} and len(arr) == 3
    assert "1 duplicate" in capsys.readouterr().err


def test_cli_attribute_with_no_arrivals_writes_nothing_and_exits_zero(monkeypatch):
    c = _db(monkeypatch, {"transmitter": TX, "waveform": WF, "receiver": RX_ROWS})
    assert cli.main(["attribute", "--n-fake", "20"]) == 0
    assert [t for t, _ in c.inserted if t == "sigmondb.attribution"] == []


def test_cli_attribute_database_error_propagates(monkeypatch):
    c = _db(monkeypatch, {"arrival": KFS_LIVE_ROWS, "transmitter": TX, "waveform": WF,
                          "receiver": RX_ROWS})

    def boom(*a, **k):
        raise RuntimeError("insert failed")

    c.insert = boom
    try:
        cli.main(["attribute", "--n-fake", "20"])
    except RuntimeError:
        pass
    else:
        raise AssertionError("a database error must reach systemd")


def test_attribution_inputs_dedups_a_capture_carried_twice(monkeypatch, capsys):
    dup = dict(KFS_LIVE_ROWS[0], reporter_id="KFS/MAIN")
    c = _db(monkeypatch, {"arrival": KFS_LIVE_ROWS[:2] + [dup], "transmitter": TX,
                          "waveform": WF, "receiver": RX_ROWS})
    arr, _, _, _ = cli._attribution_inputs(c, 30.0)
    assert len(arr) == 2
    assert "1 duplicate" in capsys.readouterr().out


def test_waveform_without_period_never_hides_a_rival(monkeypatch, capsys):
    newer = datetime(2026, 11, 12)
    wf = WF + [dict(WF[0], valid_from=newer, period_s=None),           # newest TSTB row: no period
               valid_row("waveform", tx_id="hfrnet:NOPER", valid_from=_VF, centre_hz=4.785e6,
                         rate_hz_per_s=-25_733.913, period_s=None)]
    noper = valid_row("transmitter", tx_id="hfrnet:NOPER", kind="codar", lat=36.0, lon=-121.0,
                      valid_from=_VF, version=1)
    c = _db(monkeypatch, {"arrival": KFS_LIVE_ROWS[:2], "transmitter": TX + [noper],
                          "waveform": wf, "receiver": RX_ROWS})
    _, sites, _, _ = cli._attribution_inputs(c, 30.0)
    by = {s["id"]: s for s in sites}
    assert by["hfrnet:TSTB"]["period"] == 1.0          # the older row with a period stays
    assert by["hfrnet:NOPER"]["period"] is None        # kept as a rival; analyse_group copes
    assert "hfrnet:NOPER" in capsys.readouterr().out


def test_attribution_inputs_sites_and_receivers(monkeypatch):
    old_tx = dict(TX[0], lat=0.0, lon=0.0, valid_from=datetime(2026, 1, 1))
    nowf = valid_row("transmitter", tx_id="hfrnet:NOWF", kind="codar", lat=1.0, lon=1.0,
                     valid_from=_VF, version=1)
    nopos = valid_row("receiver", reporter_id="NOPOS", valid_from=_VF, version=1)
    c = _db(monkeypatch, {"arrival": KFS_LIVE_ROWS[:2], "transmitter": [TX[1], nowf, old_tx, TX[0]],
                          "waveform": WF, "receiver": RX_ROWS + [K3LR_RX, nopos]})
    arr, sites, rxpos, coh = cli._attribution_inputs(c, 30.0)
    assert [s["id"] for s in sites] == ["hfrnet:TSTA", "hfrnet:TSTB"]     # stable order; NOWF out
    assert sites[1] == {"id": "hfrnet:TSTB", "lat": 37.0, "lon": -123.0, "freq": 4.785e6,
                        "rate": -25_733.913, "period": 1.0}
    assert rxpos == {"KFS-OMNI": (37.5, -122.5, "gpsdo"),
                     "K3LR/D": (41.1875, -80.458333, "typed_grid")}
    assert set(coh) == {"KFS-OMNI", "K3LR/D"}
    assert {a["reporter_id"] for a in arr} == {"KFS-OMNI"}
    assert all(a["live"] for a in arr) and arr[0]["sweep_repetition_hz"] == "1"


def test_summary_counts_named_by_exact_status():
    rows_ = [{"status": s} for s in ("named", "named_two_stations", "named_by_mistake",
                                    "ambiguous", "unattributable", "fits_gw900")]
    assert cli._summary("gw_only_900km", 4, rows_) == (
        "attribute gw_only_900km: 4 groups, 2 named, 1 ambiguous, 1 unattributable, "
        "1 fits_gw900 (never named)")
    assert cli._summary("default", 4, rows_[:2]) == \
        "attribute default: 4 groups, 2 named, 0 ambiguous, 0 unattributable"


# --- solve: attribution names, --write-db -----------------------------------------------------
from test_sigmondb_attribution import (  # noqa: E402
    HOOK_D_NS, RX_ROWS as GW_RX, TX_ROWS as GW_TX, _attr, _gw_arrivals)


def _sols(c):
    return [r for t, r in c.inserted if t == "sigmondb.solution"]


def test_solve_write_db_inserts_solutions_and_tolerates_an_empty_attribution_table(monkeypatch):
    c = _db(monkeypatch, {"arrival": _gw_arrivals(rid="UACNJ", centre_ns=HOOK_D_NS),
                          "receiver": GW_RX, "attribution": []})
    assert cli.main(["solve", "--from-db", "--write-db"]) == 0
    assert any(t == "sigmondb.solution" for t, _ in c.inserted)
    assert any("CREATE TABLE IF NOT EXISTS sigmondb.solution" in s for s in c.commands)
    assert all(r["tx_id"] == "" and r["solver_version"] == "groundwave-3" for r in _sols(c))


class _NoAttrTableUntilEnsured(QueryClient):
    """Reading sigmondb.attribution raises, as ClickHouse does for a missing table, until a
    CREATE TABLE for it arrives."""
    made = False

    def query(self, sql, parameters=None, **kw):
        if ".attribution" in sql and not self.made:
            raise RuntimeError("UNKNOWN_TABLE sigmondb.attribution")
        return super().query(sql, parameters, **kw)

    def command(self, sql):
        super().command(sql)
        if "CREATE TABLE IF NOT EXISTS sigmondb.attribution" in sql:
            self.made = True


def test_solve_from_db_survives_a_server_with_no_attribution_table_yet(monkeypatch):
    c = _NoAttrTableUntilEnsured({"arrival": _gw_arrivals(rid="UACNJ", centre_ns=HOOK_D_NS),
                                  "receiver": GW_RX})
    monkeypatch.setattr(cli, "connect", lambda conf: c)
    assert cli.main(["solve", "--from-db", "--write-db"]) == 0
    assert c.made and any(t == "sigmondb.solution" for t, _ in c.inserted)


def test_attribute_ensures_the_schema_even_when_it_finds_no_arrivals(monkeypatch):
    c = _db(monkeypatch, {})
    assert cli.main(["attribute"]) == 0
    assert any("CREATE TABLE IF NOT EXISTS sigmondb.attribution" in s for s in c.commands)


def test_the_attribution_table_keeps_30_days_and_solution_is_untouched():
    assert ddl.TABLES["attribution"].endswith("TTL run_at + INTERVAL 30 DAY")
    assert "TTL" not in ddl.TABLES["solution"]
    # no MODIFY TTL migration: ensure_schema runs hourly and a MODIFY TTL rematerialises the table each time
    assert "MODIFY TTL" not in " ".join(ddl.MIGRATIONS)
    sql = open(os.path.join(os.path.dirname(__file__), "..", "schema", "sigmondb.sql")).read()
    att = sql.split("CREATE TABLE IF NOT EXISTS sigmondb.attribution")[1].split(";")[0]
    assert "TTL run_at + INTERVAL 30 DAY" in att


def test_intervals_are_whole_hours_never_float(monkeypatch):
    assert cli._hours(30) == 720 and cli._hours(0.5) == 12 and cli._hours(1 / 24 * 1.4) == 1


def test_attribute_prints_the_duplicate_line_only_when_something_dropped(capsys):
    c = QueryClient({"arrival": [], "transmitter": [], "waveform": [], "receiver": []})
    cli._attribution_inputs(c, 1)
    assert "duplicate carried" not in capsys.readouterr().out


def test_solve_needs_out_or_write_db():
    with pytest.raises(SystemExit):
        cli.main(["solve", "--from-db"])
    with pytest.raises(SystemExit):
        cli.main(["solve", "--from-db", "--write-db", "--out", "x"])


def test_solve_from_db_attaches_a_default_name_and_reads_the_latest_run(monkeypatch):
    c = _db(monkeypatch, {"arrival": _gw_arrivals(), "receiver": GW_RX, "transmitter": GW_TX,
                          "attribution": [_attr("hfrnet:HOOK", "named", "UACNJ", HOOK_D_NS)]})
    assert cli.main(["solve", "--from-db", "--write-db", "--days", "7"]) == 0
    sols = _sols(c)
    assert [s["tx_id"] for s in sols] == ["hfrnet:HOOK"] and sols[0]["offset_ns"] is not None
    asql = [k for k in c.calls if ".attribution" in k["sql"]][0]["sql"]
    assert "FROM sigmondb.attribution FINAL" in asql
    # the latest run per (group_key, mode_policy): run_id hashes the inputs, so FINAL alone
    # would keep an older run's rows beside the newer ones
    assert "max(run_at)" in asql and "GROUP BY group_key, mode_policy" in asql
    # names older than ATTRIB_MAX_AGE_H drop out in the database too
    assert "run_at > now() - INTERVAL {max_age_h:UInt32} HOUR" in asql
    acall = [k for k in c.calls if ".attribution" in k["sql"]][0]
    assert acall["parameters"] == {"max_age_h": 3}
    arr_call = [k for k in c.calls if ".arrival" in k["sql"]][0]
    assert arr_call["parameters"] == {"hours": 168}


def test_solve_from_db_never_attaches_a_gw900_fit(monkeypatch, capsys):
    gw = dict(_attr("hfrnet:HOOK", "fits_gw900", "UACNJ", HOOK_D_NS), mode_policy="gw_only_900km")
    c = _db(monkeypatch, {"arrival": _gw_arrivals(), "receiver": GW_RX, "transmitter": GW_TX,
                          "attribution": [gw]})
    assert cli.main(["solve", "--from-db", "--write-db"]) == 0
    assert [s["tx_id"] for s in _sols(c)] == [""] and _sols(c)[0]["offset_ns"] is None


def test_solve_no_attribution_reads_no_names(monkeypatch):
    c = _db(monkeypatch, {"arrival": _gw_arrivals(), "receiver": GW_RX, "transmitter": GW_TX,
                          "attribution": [_attr("hfrnet:HOOK", "named", "UACNJ", HOOK_D_NS)]})
    assert cli.main(["solve", "--from-db", "--write-db", "--no-attribution"]) == 0
    assert [s["tx_id"] for s in _sols(c)] == [""]
    assert not any(".attribution" in k["sql"] for k in c.calls)


def test_solve_from_files_with_attributions_prints_reasons(tmp_path, capsys):
    rows.write_rows(tmp_path / "a.jsonl", _gw_arrivals())
    rows.write_rows(tmp_path / "r.jsonl", GW_RX)
    rows.write_rows(tmp_path / "t.jsonl", GW_TX)
    rows.write_rows(tmp_path / "at.jsonl",
                    [dict(_attr("hfrnet:HOOK", "named", "UACNJ", HOOK_D_NS),
                          alias_tx_ids=["hfrnet:HEMP"])])
    out = tmp_path / "s.jsonl"
    assert cli.main(["solve", "--arrivals", str(tmp_path / "a.jsonl"),
                     "--receivers", str(tmp_path / "r.jsonl"),
                     "--transmitters", str(tmp_path / "t.jsonl"),
                     "--attributions", str(tmp_path / "at.jsonl"), "--out", str(out)]) == 0
    assert [s["tx_id"] for s in rows.read_rows(out)] == [""]
    err = capsys.readouterr().err
    assert "attribution not applied UACNJ|4.4630 -25.73|" in err and "co-sited with hfrnet:HEMP" in err


def test_from_db_picks_the_detector_after_relabelling_at_the_radiod(monkeypatch):
    # One KFS capture carried under both labels, by two detector versions, at delays in two
    # different microsecond bins.  Relabelled first, the latest detector keeps one row; judged
    # on the old labels, both rows would reach KFS-OMNI.
    a = dict(KFS_LIVE_ROWS[0], reporter_id="KFS-MAIN", detector_version="codar-sounder/0.7.0")
    b = dict(KFS_LIVE_ROWS[0], reporter_id="KFS/MAIN", detector_version="codar-sounder/0.8.0",
             delay_ns=KFS_LIVE_ROWS[0]["delay_ns"] + 3_000)
    _db(monkeypatch, {"arrival": [a, b], "receiver": RX_ROWS})
    arr, _, _ = cli._from_db(_ns())
    assert [(r["reporter_id"], r["detector_version"]) for r in arr] == \
        [("KFS-OMNI", "codar-sounder/0.8.0")]


def test_attribution_inputs_pick_the_detector_after_relabelling(monkeypatch):
    a = dict(KFS_LIVE_ROWS[0], reporter_id="KFS-MAIN", detector_version="codar-sounder/0.7.0")
    b = dict(KFS_LIVE_ROWS[0], reporter_id="KFS/MAIN", detector_version="codar-sounder/0.8.0",
             delay_ns=KFS_LIVE_ROWS[0]["delay_ns"] + 3_000)
    c = _db(monkeypatch, {"arrival": [a, b], "transmitter": TX, "waveform": WF,
                          "receiver": RX_ROWS})
    arr, _, _, _ = cli._attribution_inputs(c, 30.0)
    assert [r["detector_version"] for r in arr] == ["codar-sounder/0.8.0"]


def test_from_db_run_at_comes_back_naive_utc(monkeypatch):
    cdt = timezone(timedelta(hours=-5))
    row = dict(_attr("hfrnet:HOOK", "named", "UACNJ", HOOK_D_NS),
               run_at=datetime(2026, 11, 13, 5, 0, tzinfo=cdt))
    c = _db(monkeypatch, {"attribution": [row]})
    got = cli._latest_attributions(c)
    assert got[0]["run_at"] == datetime(2026, 11, 13, 10, 0) and got[0]["run_at"].tzinfo is None


def test_solve_refuses_attribution_flags_that_would_be_ignored(capsys):
    with pytest.raises(SystemExit) as e:
        cli.main(["solve", "--arrivals", "a.jsonl", "--no-attribution", "--out", "x"])
    assert e.value.code == 2 and "--no-attribution" in capsys.readouterr().err
    with pytest.raises(SystemExit) as e:
        cli.main(["solve", "--from-db", "--attributions", "a.jsonl", "--write-db"])
    assert e.value.code == 2 and "--attributions" in capsys.readouterr().err


def test_solve_summary_separates_station_named_from_attributed(monkeypatch, capsys):
    station = _gw_arrivals(centre_ns=HOOK_D_NS + 5_000_000, seed=3)
    for r in station:
        r["tx_id"] = "hfrnet:OWN"
    arr = [dict(r, detector_version="v") for r in _gw_arrivals() + station]
    c = _db(monkeypatch, {"arrival": arr, "receiver": GW_RX,
                          "transmitter": GW_TX,
                          "attribution": [_attr("hfrnet:HOOK", "named", "UACNJ", HOOK_D_NS)]})
    assert cli.main(["solve", "--from-db", "--write-db"]) == 0
    assert "solve: 2 clusters, 1 station-named, 1 attributed" in capsys.readouterr().out


# --- receiver-positions ----------------------------------------------------------------------
def _posfile(tmp_path, doc, mode=0o600):
    import json
    f = tmp_path / "pos.json"
    f.write_text(json.dumps(doc))
    f.chmod(mode)
    return f


def test_cli_receiver_positions_inserts_new_versions_and_prints_no_coordinate(
        monkeypatch, tmp_path, capsys):
    c = _db(monkeypatch, {"receiver": [K3LR_RX]})
    f = _posfile(tmp_path, {"K3LR/D": {"lat": 41.5551, "lon": -80.7777, "pos_sigma_m": 10.0},
                            "NOPE": {"lat": 2.0, "lon": 3.0, "pos_sigma_m": 10.0}})
    assert cli.main(["receiver-positions", "--file", str(f)]) == 0
    out = capsys.readouterr()
    assert "receiver-positions: 1 updated, 0 unchanged, 1 unknown: NOPE" in out.out
    for s in (out.out, out.err):
        assert "41.5551" not in s and "80.7777" not in s
    ins = [r for t, r in c.inserted if t == "sigmondb.receiver"]
    assert len(ins) == 1 and ins[0]["version"] == 2 and ins[0]["pos_source"] == "gpsdo"
    assert any("FROM sigmondb.receiver FINAL" in k["sql"] for k in c.calls)


def test_cli_receiver_positions_counts_unchanged_and_inserts_nothing(monkeypatch, tmp_path, capsys):
    same = dict(K3LR_RX, lat=41.0, lon=-80.0, pos_sigma_m=math.hypot(10.0, ROUNDING_SIGMA_M),
                pos_source="gpsdo", version=2)
    c = _db(monkeypatch, {"receiver": [K3LR_RX, same]})
    f = _posfile(tmp_path, {"K3LR/D": {"lat": 41.0, "lon": -80.0, "pos_sigma_m": 10.0}})
    assert cli.main(["receiver-positions", "--file", str(f)]) == 0
    assert "0 updated, 1 unchanged, 0 unknown" in capsys.readouterr().out
    assert [r for t, r in c.inserted if t == "sigmondb.receiver"] == []


def test_cli_receiver_positions_refuses_a_readable_file(monkeypatch, tmp_path):
    c = _db(monkeypatch, {"receiver": [K3LR_RX]})
    f = _posfile(tmp_path, {}, 0o644)
    with pytest.raises(ValueError, match="mode"):
        cli.main(["receiver-positions", "--file", str(f)])
    assert c.inserted == []


# --- the private positions overlay (attribute, solve) -----------------------------------------
PRECISE = {"K3LR/D": {"lat": 41.123457, "lon": -80.456789, "pos_sigma_m": 5.5}}
STORED_RX = dict(K3LR_RX, lat=41.12, lon=-80.46, pos_sigma_m=math.hypot(5.5, ROUNDING_SIGMA_M),
                 pos_source="gpsdo")


def _digits_absent(capsys, c=None):
    out = capsys.readouterr()
    for s in (out.out, out.err):
        assert "123457" not in s and "456789" not in s
    if c is not None:
        assert "123457" not in repr(c.inserted) and "456789" not in repr(c.inserted)
    return out.out


def _spy_attribution(monkeypatch):
    from sigmondb import attribution as at
    seen = {}
    real = at.run_attribution

    def spy(arr, sites, rxpos, policy, **kw):
        seen["rxpos"] = rxpos
        return real(arr, sites, rxpos, policy, **kw)

    monkeypatch.setattr(at, "run_attribution", spy)
    return seen


def _attr_tables():
    return {"arrival": K3LR_ROWS, "transmitter": TX, "waveform": WF, "receiver": [STORED_RX]}


def test_attribute_uses_the_precise_positions_and_prints_no_coordinate(
        monkeypatch, tmp_path, capsys):
    c = _db(monkeypatch, _attr_tables())
    seen = _spy_attribution(monkeypatch)
    f = _posfile(tmp_path, PRECISE)
    assert cli.main(["attribute", "--n-fake", "20", "--positions", str(f)]) == 0
    assert seen["rxpos"]["K3LR/D"] == (41.123457, -80.456789, "gpsdo")
    out = _digits_absent(capsys, c)
    assert "positions: 1 receiver(s) at full precision from the private file" in out


def test_attribute_without_a_file_uses_the_stored_rows(monkeypatch, tmp_path, capsys):
    c = _db(monkeypatch, _attr_tables())
    seen = _spy_attribution(monkeypatch)
    assert cli.main(["attribute", "--n-fake", "20",
                     "--positions", str(tmp_path / "absent.json")]) == 0
    assert seen["rxpos"]["K3LR/D"] == (41.12, -80.46, "gpsdo")
    assert "positions: no private file; using sigmondb.receiver as stored" in capsys.readouterr().out


def test_attribute_default_positions_path_is_under_home(monkeypatch, tmp_path):
    import json
    _db(monkeypatch, _attr_tables())
    seen = _spy_attribution(monkeypatch)
    f = tmp_path / "home" / ".config" / "sigmondb-receiver-positions.json"
    f.parent.mkdir(parents=True)
    f.write_text(json.dumps(PRECISE))
    f.chmod(0o600)
    assert cli.main(["attribute", "--n-fake", "20"]) == 0
    assert seen["rxpos"]["K3LR/D"][:2] == (41.123457, -80.456789)


def test_attribute_out_file_holds_no_precise_coordinate(monkeypatch, tmp_path, capsys):
    _db(monkeypatch, _attr_tables())
    f = _posfile(tmp_path, PRECISE)
    o = tmp_path / "a.jsonl"
    assert cli.main(["attribute", "--n-fake", "20", "--positions", str(f), "--out", str(o)]) == 0
    assert "123457" not in o.read_text() and "456789" not in o.read_text()
    _digits_absent(capsys)


@pytest.mark.parametrize("argv", [["attribute"], ["solve", "--from-db", "--write-db"]])
def test_attribute_and_solve_refuse_a_readable_positions_file(monkeypatch, tmp_path, argv):
    c = _db(monkeypatch, _attr_tables())
    f = _posfile(tmp_path, PRECISE, 0o644)
    with pytest.raises(ValueError, match="mode"):
        cli.main(argv + ["--positions", str(f)])
    assert c.calls == [] and c.commands == [] and c.inserted == []


def _spy_solve(monkeypatch):
    seen = {}
    real = cli.ground_wave_solutions

    def spy(cl, tx, rx, v, **kw):
        seen["rx"] = rx
        return real(cl, tx, rx, v, **kw)

    monkeypatch.setattr(cli, "ground_wave_solutions", spy)
    return seen


def _rx_of(seen, rid):
    return [(r["lat"], r["lon"], r["pos_sigma_m"]) for r in seen["rx"] if r["reporter_id"] == rid]


def test_solve_from_db_uses_the_precise_positions(monkeypatch, tmp_path, capsys):
    c = _db(monkeypatch, {"arrival": [_arr("K3LR/D", "g", "v1")], "receiver": [STORED_RX],
                          "transmitter": [], "attribution": []})
    seen = _spy_solve(monkeypatch)
    f = _posfile(tmp_path, PRECISE)
    assert cli.main(["solve", "--from-db", "--write-db", "--positions", str(f)]) == 0
    assert _rx_of(seen, "K3LR/D") == [(41.123457, -80.456789, 5.5)]
    out = _digits_absent(capsys, c)
    assert "positions: 1 receiver(s) at full precision from the private file" in out


def test_solve_from_db_without_a_file_uses_the_stored_rows(monkeypatch, tmp_path, capsys):
    _db(monkeypatch, {"arrival": [_arr("K3LR/D", "g", "v1")], "receiver": [STORED_RX],
                      "transmitter": [], "attribution": []})
    seen = _spy_solve(monkeypatch)
    assert cli.main(["solve", "--from-db", "--write-db",
                     "--positions", str(tmp_path / "absent.json")]) == 0
    assert _rx_of(seen, "K3LR/D") == [(41.12, -80.46, STORED_RX["pos_sigma_m"])]
    assert "positions: no private file; using sigmondb.receiver as stored" in capsys.readouterr().out


def test_solve_receivers_file_uses_the_precise_positions(monkeypatch, tmp_path, capsys):
    from test_sigmondb_groundwave import _arrivals
    seen = _spy_solve(monkeypatch)
    rows.write_rows(tmp_path / "a.jsonl", _arrivals(1_000_000))
    rows.write_rows(tmp_path / "r.jsonl", [STORED_RX])
    f = _posfile(tmp_path, PRECISE)
    o = tmp_path / "s.jsonl"
    assert cli.main(["solve", "--arrivals", str(tmp_path / "a.jsonl"),
                     "--receivers", str(tmp_path / "r.jsonl"), "--positions", str(f),
                     "--out", str(o)]) == 0
    assert _rx_of(seen, "K3LR/D") == [(41.123457, -80.456789, 5.5)]
    assert "123457" not in o.read_text()
    _digits_absent(capsys)


def test_publish_ignores_the_positions_file(monkeypatch, tmp_path):
    f = _posfile(tmp_path, PRECISE, 0o644)       # unreadable by the overlay's rule: publish never looks
    monkeypatch.setenv("HOME", str(tmp_path))
    monkeypatch.delenv("CH_PUB_USER", raising=False)
    with pytest.raises(SystemExit):              # publish takes no --positions
        cli.main(["publish", "--positions", str(f)])
    assert cli.main(["publish"]) == 2            # reaches its own credentials check, no mode error
