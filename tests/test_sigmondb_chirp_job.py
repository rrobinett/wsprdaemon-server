"""The chirp-state job: reads the fleet's evidence, moves chirps, writes the log first.

No ClickHouse: JobClient serves canned rows per table and per aggregate and records inserts.
Stations are made-up sites at made-up positions."""
import configparser
import json
import os
import sys
from datetime import datetime, timedelta, timezone

import pytest

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from sigmondb import chirp, chirp_evidence, chirp_job, cli, ddl  # noqa: E402
from test_sigmondb_cli import QueryClient, _Result  # noqa: E402

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
RULES = chirp.load_rules()
NOW = datetime(2026, 11, 27, 12, 0, 0)
CENTRE, RATE = 4_537_000.0, -25_730.0
GK = chirp.group_key(CENTRE, RATE, 1.0)


def H(k):
    """The hour start k hours before NOW."""
    return NOW - timedelta(hours=k)


class JobClient(QueryClient):
    """QueryClient that also answers the system.tables probe and the AGG_SQL aggregates, and
    refuses (as ClickHouse does) a query on a table that does not exist."""

    def __init__(self, tables, aggs=None, have=None):
        super().__init__(tables)
        self.aggs = aggs or {}
        self.have = set(have) if have is not None else set(ddl.TABLES) - {"heard"}

    def query(self, sql, parameters=None, **kw):
        if "system.tables" in sql:
            self.calls.append(dict(sql=sql, parameters=parameters))
            return _Result([(n,) for n in sorted(self.have)])
        table = sql.split(" FROM %s." % ddl.DATABASE, 1)[1].split()[0]
        if table not in self.have:
            raise RuntimeError("UNKNOWN_TABLE sigmondb.%s" % table)
        if sql.startswith("/* agg:"):
            name = sql.split("agg:", 1)[1].split()[0]
            self.calls.append(dict(sql=sql, parameters=parameters, **kw))
            cols = chirp_evidence.AGG_SQL[name][0]
            return _Result([tuple(r.get(c) for c in cols) for r in self.aggs.get(name, [])])
        return super().query(sql, parameters, **kw)


def chirp_row(cid, state="seen", centre=CENTRE, rate=RATE, tx_id="", kind="unknown",
              first=NOW - timedelta(days=3), **kw):
    r = {c: None for c in ddl.COLUMNS["chirp"]}
    r.update(chirp_id=cid, group_key=chirp.group_key(centre, rate, 1.0), kind=kind, state=state,
             state_since=first, centre_hz=centre, span_hz=None, rate_hz_per_s=rate, period_s=1.0,
             duration_s=None, gate_period_s=None, pos_source="", first_heard=first,
             last_heard=None, reporter_ids=[], n_stations=0, heard_hours=0, tx_id=tx_id,
             prior_ids=[], suggested_name="", pin="", pin_by="", pin_why="",
             rules_version=RULES.rules_version, updated_at=first)
    r.update(kw)
    return r


def log_row(cid, to_state, at, from_state=""):
    return dict(at=at, chirp_id=cid, from_state=from_state, to_state=to_state, actor="seed",
                why="", evidence={}, reporter_ids=[], rules_version=RULES.rules_version)


def sweep(rid, hour, centre=CENTRE, rate=RATE, clipped=0):
    return dict(reporter_id=rid, centre_hz=centre, rate_hz_per_s=rate, period_s=1.0,
                span_hz=25_000.0, duration_s=None, clipped=clipped, hour=hour, n=3,
                snr_med=12.0, n_dwells=2)


def rx(rid, lat, lon):
    return dict(reporter_id=rid, lat=lat, lon=lon, pos_source="gpsdo", coherence_group="",
                valid_from=datetime(2026, 1, 1), version=1)


RX = [rx("S1", 40.02, -90.04), rx("S2", 42.02, -86.04), rx("S3", 38.02, -94.04),
      rx("R", 30.02, -100.04)]


def t6_rows(rid, d_ms=123.456):
    """16 T6 captures, 2 h apart over 30 h, within 1 us of d_ms: a steady cluster."""
    return [dict(time=H(2 + 2 * k), reporter_id=rid, group_key=GK,
                 delay_ns=int(round((d_ms + (k % 3 - 1) * 0.0005) * 1e6)), level_db=10.0,
                 detector_version="chirp-recorder-0.3", engineering={})
            for k in range(16)]


def prior_tx(tx_id="hfrnet:TSTA", centre=CENTRE, lat=37.0, lon=-123.0, kind="codar"):
    t = {c: None for c in ddl.COLUMNS["transmitter"]}
    t.update(tx_id=tx_id, kind=kind, network="", name="Test A", status="listed", lat=lat,
             lon=lon, pos_source="hfrnet", valid_from=datetime(2026, 1, 1), version=1,
             source="hfrnet", note="")
    w = {c: None for c in ddl.COLUMNS["waveform"]}
    w.update(tx_id=tx_id, valid_from=datetime(2026, 1, 1), centre_hz=centre,
             rate_hz_per_s=RATE, period_s=1.0, duration_s=1.0, bandwidth_hz=25_000.0,
             source="hfrnet")
    return t, w


def run(client, out=None, now=NOW):
    lines = []
    res = chirp_job.run(client, RULES, now, out=(out or lines.append))
    res.lines = lines
    return res


def located(monkeypatch, sigma_m=20_000.0):
    """Stub locate (its own tests cover the search); record what the job hands it."""
    seen = []

    def fake(st_clusters, rxpos, period_ms, rules, **kw):
        seen.append((sorted(st_clusters), period_ms))
        return {"status": "located", "lat": 39.0, "lon": -91.0, "pos_sigma_m": sigma_m,
                "spread_us": 1.0, "n_stations": len(st_clusters),
                "stations": sorted(st_clusters)}

    monkeypatch.setattr(chirp_job, "locate", fake)
    return seen


def probing_scene(cid="cand:20261101-abcdef"):
    """A probing cand: chirp three independent stations hear, whole sweeps and steady T6."""
    tables = {"chirp": [chirp_row(cid, state="probing")],
              "chirp_log": [log_row(cid, "probing", NOW - timedelta(days=2), "candidate")],
              "receiver": list(RX),
              "arrival": t6_rows("S1") + t6_rows("S2", 200.0) + t6_rows("S3", 300.0)}
    sweeps = [sweep(st, H(k)) for st in ("S1", "S2", "S3") for k in (1, 2)]
    return tables, {"sweep_hourly": sweeps}


def apply(tables, res):
    """The database after a run's writes: chirp replaced per chirp_id, the rest appended."""
    by = {r["chirp_id"]: r for r in tables.get("chirp", [])}
    by.update((r["chirp_id"], r) for r in res.chirp_rows)
    tables["chirp"] = list(by.values())
    for t, rows in (("chirp_log", res.log_rows), ("transmitter", res.transmitter_rows),
                    ("waveform", res.waveform_rows), ("chirp_reject_local", res.reject_rows)):
        tables[t] = list(tables.get(t, [])) + list(rows)


def _conf(tmp_path):
    p = tmp_path / "c.conf"
    p.write_text('CLICKHOUSE_HOST="h"\nCLICKHOUSE_PORT="1"\nCLICKHOUSE_ROOT_ADMIN_USER="u"\n'
                 'CLICKHOUSE_ROOT_ADMIN_PASSWORD="p"\n')
    return str(p)


# --- the brief's tests ------------------------------------------------------------------------

def test_log_written_before_chirp(monkeypatch, tmp_path):
    located(monkeypatch)
    tables, aggs = probing_scene()
    c = JobClient(tables, aggs)
    monkeypatch.setattr(cli, "connect", lambda conf: c)
    monkeypatch.setattr(cli, "_now_s", lambda: NOW)
    assert cli.main(["chirp-state", "--conf", _conf(tmp_path)]) == 0
    order = [t for t, _ in c.inserted]
    assert "sigmondb.chirp_log" in order and "sigmondb.chirp" in order
    first = {t: order.index(t) for t in set(order)}
    assert (first["sigmondb.chirp_log"] < first["sigmondb.transmitter"]
            < first["sigmondb.waveform"] < first["sigmondb.chirp"])
    assert any("CREATE TABLE IF NOT EXISTS sigmondb.chirp_log" in s for s in c.commands)


def test_rerun_writes_no_new_log_rows(monkeypatch):
    located(monkeypatch)
    tables, aggs = probing_scene()
    # A seen chirp at 5 MHz that one station heard in six hours: it becomes a candidate.
    tables["chirp"].append(chirp_row("A", centre=5_000_000.0))
    tables["chirp_log"].append(log_row("A", "seen", NOW - timedelta(days=3)))
    aggs["sweep_hourly"] += [sweep("S1", H(k), centre=5_000_000.0) for k in (1, 3, 5, 7, 9, 11)]
    # Unmatched sweeps at 7 MHz found a new seen chirp.
    aggs["sweep_hourly"] += [sweep("S2", H(1), centre=7_000_000.0)]
    t, w = prior_tx(centre=5_000_000.0)
    tables["transmitter"], tables["waveform"] = [t], [w]
    first = run(JobClient(tables, aggs))
    assert {(r["chirp_id"], r["to_state"]) for r in first.log_rows} >= {
        ("A", "candidate"), ("cand:20261101-abcdef", "established")}
    assert sum(1 for r in first.log_rows if r["to_state"] == "seen") == 1
    assert first.chirp_rows
    apply(tables, first)
    second = run(JobClient(tables, aggs))
    assert second.log_rows == []
    assert second.chirp_rows == []
    assert second.transmitter_rows == [] and second.waveform_rows == []


def test_state_read_from_log_not_chirp():
    tables = {"chirp": [chirp_row("C1", state="seen")],
              "chirp_log": [log_row("C1", "seen", NOW - timedelta(days=3)),
                            log_row("C1", "candidate", NOW - timedelta(days=2), "seen")],
              "receiver": list(RX)}
    aggs = {"sweep_hourly": [sweep("S1", H(k)) for k in (1, 3, 5, 7, 9, 11)],
            "capture_recent": [dict(reporter_id="S2", centre_hz=CENTRE, rate_hz_per_s=RATE,
                                    first_t=H(3), last_t=H(1), n=4, n_found=2)]}
    res = run(JobClient(tables, aggs))
    assert [(r["from_state"], r["to_state"]) for r in res.log_rows] == [("candidate", "probing")]
    assert res.chirp_rows[0]["state"] == "probing" and res.chirp_rows[0]["state_since"] == NOW


def test_new_chirps_capped_per_reporter():
    sweeps = [sweep("R", H(1), centre=5_000_000.0 + 50_000.0 * k) for k in range(12)]
    res = run(JobClient({"receiver": list(RX)}, {"sweep_hourly": sweeps}))
    assert len(res.chirp_rows) == 5 and all(r["state"] == "seen" for r in res.chirp_rows)
    assert len(res.log_rows) == 5 and all(r["to_state"] == "seen" for r in res.log_rows)
    assert res.skipped_new == {"R": 7}
    assert sum(1 for ln in res.lines if "skipped_new" in ln) == 1


def test_sweep_matching_rejected_founds_nothing():
    since = NOW - timedelta(days=2)
    # Y shares X's signature; the sweep on X counts for X alone, never for Y.
    tables = {"chirp": [chirp_row("X", state="rejected"),
                        chirp_row("Y", first=NOW - timedelta(hours=3))],
              "chirp_log": [log_row("X", "rejected", since, "probing"),
                            log_row("Y", "seen", NOW - timedelta(hours=3))],
              "receiver": list(RX)}
    res = run(JobClient(tables, {"sweep_hourly": [sweep("S1", H(1))]}))
    assert res.log_rows == [] and res.founded == 0
    assert [r["chirp_id"] for r in res.chirp_rows] == ["X"]
    assert res.chirp_rows[0]["reporter_ids"] == ["S1"]
    assert res.heard_while_rejected == 1
    assert any("heard_while_rejected 1" in ln for ln in res.lines)
    assert "chirp-state: heard_while_rejected X" in res.lines      # ids only


def test_established_discovered_writes_identity(monkeypatch):
    seen = located(monkeypatch)
    tables, aggs = probing_scene()
    res = run(JobClient(tables, aggs))
    cid = "cand:20261101-abcdef"
    assert [(r["from_state"], r["to_state"]) for r in res.log_rows] == [("probing", "established")]
    assert res.log_rows[0]["actor"] == "chirp-state"
    assert res.log_rows[0]["rules_version"] == RULES.rules_version
    assert len(res.transmitter_rows) == 1 and len(res.waveform_rows) == 1
    t, w = res.transmitter_rows[0], res.waveform_rows[0]
    assert (t["tx_id"], t["source"], t["status"], t["kind"]) == (cid, "discovered",
                                                                 "established", "unknown")
    assert (w["tx_id"], w["source"], w["measured_by"]) == (cid, "measured", "S1,S2,S3")
    row = res.chirp_rows[0]
    assert row["tx_id"] == cid and row["state"] == "established"
    assert (row["lat"], row["lon"], row["pos_source"]) == (39.0, -91.0, "triangulated")
    assert seen == [(["S1", "S2", "S3"], 1000.0)]


def test_dry_run_touches_nothing(monkeypatch, tmp_path):
    tables = {"receiver": list(RX)}
    c = JobClient(tables, {"sweep_hourly": [sweep("S1", H(1))]})
    monkeypatch.setattr(cli, "connect", lambda conf: c)
    out = tmp_path / "out"
    assert cli.main(["chirp-state", "--dry-run", "--out", str(out),
                     "--conf", _conf(tmp_path)]) == 0
    assert c.inserted == [] and c.commands == []
    assert not any("CREATE" in k["sql"] or "INSERT" in k["sql"] for k in c.calls)
    rows = [json.loads(ln) for ln in (out / "chirp.jsonl").read_text().splitlines()]
    assert len(rows) == 1 and rows[0]["state"] == "seen"
    for t in ("chirp_log", "chirp_reject_local", "transmitter", "waveform"):
        assert (out / (t + ".jsonl")).exists()


def test_prior_names_but_never_moves():
    tables = {"chirp": [chirp_row("C1", first=NOW - timedelta(hours=3))],
              "chirp_log": [log_row("C1", "seen", NOW - timedelta(hours=3))],
              "receiver": list(RX)}
    t, w = prior_tx()
    far_t, far_w = prior_tx("giro:FAR", lat=-30.0, lon=20.0, kind="ionosonde")
    far_t["source"] = far_w["source"] = "giro"
    tables["transmitter"], tables["waveform"] = [t, far_t], [w, far_w]
    res = run(JobClient(tables, {"sweep_hourly": [sweep("S1", H(1))]}))
    assert res.log_rows == []
    (row,) = res.chirp_rows
    assert row["state"] == "seen" and row["heard_hours"] == 1 << H(1).hour
    assert row["prior_ids"] == ["giro:FAR", "hfrnet:TSTA"]
    assert row["suggested_name"] and row["kind"] in ("codar", "ionosonde")


# --- the controller's rulings -----------------------------------------------------------------

def test_new_id_collision_gets_a_suffix(monkeypatch):
    base = "cand:20261127-aaaaaa"
    monkeypatch.setattr(chirp_job, "new_chirp_id", lambda cluster, day: base)
    tables = {"chirp": [chirp_row(base, centre=9_000_000.0)],
              "chirp_log": [log_row(base, "seen", NOW - timedelta(days=1))],
              "receiver": list(RX)}
    sweeps = [sweep("S1", H(1), centre=5_000_000.0), sweep("S1", H(1), centre=6_000_000.0)]
    res = run(JobClient(tables, {"sweep_hourly": sweeps}))
    founded = [r["chirp_id"] for r in res.log_rows if r["to_state"] == "seen"]
    assert founded == [base + "-2", base + "-3"]
    by = {r["chirp_id"]: r["centre_hz"] for r in res.chirp_rows}
    assert by[base + "-2"] == 5_000_000.0 and by[base + "-3"] == 6_000_000.0


def test_unknown_state_is_skipped_and_named():
    tables = {"chirp": [chirp_row("BAD"), chirp_row("C1", centre=5_000_000.0)],
              "chirp_log": [log_row("BAD", "bogus", NOW - timedelta(days=1)),
                            log_row("C1", "seen", NOW - timedelta(days=3))],
              "receiver": list(RX)}
    sweeps = [sweep("S1", H(k), centre=5_000_000.0) for k in (1, 3, 5, 7, 9, 11)]
    res = run(JobClient(tables, {"sweep_hourly": sweeps}))
    assert [(r["chirp_id"], r["to_state"]) for r in res.log_rows] == [("C1", "candidate")]
    assert res.errors == 1
    assert [ln for ln in res.lines if "BAD" in ln and "bogus" in ln]
    assert all(r["chirp_id"] != "BAD" for r in res.chirp_rows)


def test_tz_aware_times_are_normalised():
    utc = timezone.utc
    tables = {"chirp": [chirp_row("C1", centre=5_000_000.0)],
              "chirp_log": [log_row("C1", "seen", (NOW - timedelta(days=3)).replace(tzinfo=utc))],
              "receiver": list(RX)}
    sweeps = [sweep("S1", H(k).replace(tzinfo=utc), centre=5_000_000.0)
              for k in (1, 3, 5, 7, 9, 11)]
    res = run(JobClient(tables, {"sweep_hourly": sweeps}))
    assert [r["to_state"] for r in res.log_rows] == ["candidate"]
    assert res.chirp_rows[0]["last_heard"] == H(1) and res.chirp_rows[0]["last_heard"].tzinfo is None


def test_reads_aggregates_with_agg_params_and_active_rejects_only():
    tables = {"chirp": [chirp_row("C1", state="candidate")],
              "chirp_log": [log_row("C1", "candidate", NOW - timedelta(days=2))],
              "receiver": list(RX),
              "chirp_reject_local": [
                  dict(reporter_id="S1", centre_hz=CENTRE, rate_hz_per_s=RATE, period_s=1.0,
                       span_hz=None, chirp_id="C1", reason="spur", set_by="t",
                       set_at=NOW - timedelta(days=1), active=1),
                  dict(reporter_id="S2", centre_hz=CENTRE, rate_hz_per_s=RATE, period_s=1.0,
                       span_hz=None, chirp_id="C1", reason="spur", set_by="t",
                       set_at=NOW - timedelta(days=1), active=0)]}
    c = JobClient(tables, {"sweep_hourly": [sweep("S1", H(1)), sweep("S2", H(1))]})
    res = run(c)
    aggs = [k for k in c.calls if k["sql"].startswith("/* agg:")]
    assert len(aggs) == 4 and all(k["parameters"] == chirp_evidence.agg_params(RULES) for k in aggs)
    assert res.chirp_rows[0]["reporter_ids"] == ["S2"]       # S1's sweep sits on its reject


def test_station_pos_is_the_grid6_centre(monkeypatch):
    got = {}
    real = chirp_job.next_state

    def spy(c, ev, rules, now, *, station_pos=None):
        got.update(station_pos or {})
        return real(c, ev, rules, now, station_pos=station_pos)

    monkeypatch.setattr(chirp_job, "next_state", spy)
    tables = {"chirp": [chirp_row("C1")], "chirp_log": [log_row("C1", "seen", NOW)],
              "receiver": list(RX)}
    run(JobClient(tables, {"sweep_hourly": [sweep("S1", H(1))]}))
    from sigmondb.publish import latlon_to_grid6
    from sigmondb.receivers import grid_to_latlon
    assert got["S1"] == grid_to_latlon(latlon_to_grid6(40.02, -90.04)) != (40.02, -90.04)


def test_output_carries_no_coordinate(monkeypatch):
    located(monkeypatch)
    tables, aggs = probing_scene()
    res = run(JobClient(tables, aggs))
    text = "\n".join(res.lines)
    for v in ("40.02", "42.02", "38.02", "39.0", "-91.0"):
        assert v not in text
    assert any("moves" in ln and "probing->established=1" in ln for ln in res.lines)


def test_fresh_host_dry_run_reads_no_missing_table(monkeypatch, tmp_path):
    core = {"transmitter", "waveform", "receiver", "capture", "arrival", "solution",
            "attribution"}
    c = JobClient({"receiver": list(RX)}, {}, have=core)
    monkeypatch.setattr(cli, "connect", lambda conf: c)
    assert cli.main(["chirp-state", "--dry-run", "--out", str(tmp_path / "o"),
                     "--conf", _conf(tmp_path)]) == 0
    assert c.commands == [] and c.inserted == []


def test_dry_run_needs_out(tmp_path):
    with pytest.raises(SystemExit):
        cli.main(["chirp-state", "--dry-run", "--conf", _conf(tmp_path)])


def _unit(name):
    cp = configparser.ConfigParser(strict=False, interpolation=None)
    cp.optionxform = str
    with open(os.path.join(ROOT, name)) as f:
        cp.read_file(f)
    return cp


def test_unit_and_publish_chain():
    u = _unit("sigmondb-chirp-state.service")
    s = u["Service"]
    assert s["Type"] == "oneshot" and s["User"] == "wsprdaemon" and s["Group"] == "wsprdaemon"
    assert s["ExecStart"] == "/opt/wsprdaemon-server/venv/bin/python -m sigmondb chirp-state"
    assert s["Nice"] == "10" and s["IOSchedulingClass"] == "idle"
    assert s["TimeoutStartSec"] == "15min" and s["SyslogIdentifier"] == "sigmondb-chirp-state"
    assert s["NoNewPrivileges"] == "true"
    assert u["Unit"]["After"].split() == ["sigmondb-solve.service", "clickhouse-server.service"]
    pub = _unit("sigmondb-publish.service")["Unit"]
    assert "sigmondb-chirp-state.service" in pub["Wants"].split()
    assert "sigmondb-chirp-state.service" in pub["After"].split()


# --- fix round 1: reconcile from state, ties, R23 ---------------------------------------------

CID = "cand:20261101-abcdef"


def log_only(tables, res):
    """A run that crashed right after its chirp_log insert."""
    tables["chirp_log"] = list(tables.get("chirp_log", [])) + list(res.log_rows)


def test_crash_after_log_heals_identity(monkeypatch):
    located(monkeypatch)
    tables, aggs = probing_scene()
    first = run(JobClient(tables, aggs))
    assert first.transmitter_rows and first.waveform_rows
    log_only(tables, first)
    second = run(JobClient(tables, aggs))
    assert second.log_rows == []
    assert [t["tx_id"] for t in second.transmitter_rows] == [CID]
    assert [w["tx_id"] for w in second.waveform_rows] == [CID]
    (row,) = second.chirp_rows
    assert (row["state"], row["tx_id"], row["pos_source"]) == ("established", CID, "triangulated")
    apply(tables, second)
    third = run(JobClient(tables, aggs))
    assert third.rows() == {t: [] for t in third.rows()}


def test_crash_after_transmitter_points_the_chirp_at_it(monkeypatch):
    located(monkeypatch)
    tables, aggs = probing_scene()
    first = run(JobClient(tables, aggs))
    log_only(tables, first)
    tables["transmitter"] = list(first.transmitter_rows)      # the waveform never landed
    second = run(JobClient(tables, aggs))
    assert second.log_rows == [] and second.transmitter_rows == []
    assert [w["tx_id"] for w in second.waveform_rows] == [CID]
    assert second.chirp_rows[0]["tx_id"] == CID
    tables["waveform"] = list(first.waveform_rows)            # both landed, the chirp row not
    third = run(JobClient(tables, aggs))
    assert third.transmitter_rows == [] and third.waveform_rows == []
    assert third.chirp_rows[0]["tx_id"] == CID


def local_scene():
    """A candidate only S1 hears, while S2 and S3, a few hundred km off, dwell over it six
    times each and hear nothing: local_only, a reject at S1."""
    tables = {"chirp": [chirp_row("L", state="candidate")],
              "chirp_log": [log_row("L", "candidate", NOW - timedelta(days=2), "seen")],
              "receiver": list(RX)}
    dw = [dict(reporter_id=st, centre_hz=CENTRE, span_hz=200_000.0, hour=H(k), n=1)
          for st in ("S2", "S3") for k in range(1, 7)]
    return tables, {"sweep_hourly": [sweep("S1", H(k)) for k in (1, 2)], "dwell_hourly": dw}


def test_crash_after_log_heals_the_local_reject():
    tables, aggs = local_scene()
    first = run(JobClient(tables, aggs))
    assert [(r["to_state"], r["why"]) for r in first.log_rows] == [("dropped", "local_only")]
    assert [(r["reporter_id"], r["chirp_id"]) for r in first.reject_rows] == [("S1", "L")]
    log_only(tables, first)
    second = run(JobClient(tables, aggs))
    assert second.log_rows == []
    assert [(r["reporter_id"], r["chirp_id"], r["set_by"]) for r in second.reject_rows] == [
        ("S1", "L", "chirp-state")]
    assert second.chirp_rows[0]["state"] == "dropped"


def test_a_withdrawn_local_reject_stays_withdrawn():
    tables, aggs = local_scene()
    first = run(JobClient(tables, aggs))
    log_only(tables, first)
    tables["chirp_reject_local"] = [dict(first.reject_rows[0], active=0, set_by="michael")]
    assert run(JobClient(tables, aggs)).reject_rows == []


def test_log_only_chirp_rebuilt_under_its_logged_id():
    sweeps = [sweep("S2", H(1), centre=7_000_000.0)]
    tables = {"receiver": list(RX)}
    first = run(JobClient(tables, {"sweep_hourly": sweeps}))
    (cid,) = [r["chirp_id"] for r in first.log_rows]
    log_only(tables, first)
    # A day later new_chirp_id would hash the cluster to another day's id.
    second = run(JobClient(tables, {"sweep_hourly": sweeps}), now=NOW + timedelta(days=1))
    assert second.log_rows == [] and second.founded == 0 and second.rebuilt == 1
    (row,) = second.chirp_rows
    built = first.chirp_rows[0]
    assert row["chirp_id"] == cid and row["state"] == "seen" and row["state_since"] == NOW
    for k in ("group_key", "centre_hz", "rate_hz_per_s", "period_s", "span_hz", "duration_s",
              "first_heard"):
        assert row[k] == built[k], k


def test_prior_blocks_discovered_identity(monkeypatch):
    """R23: established by location, but a listed site matches: no discovered identity."""
    located(monkeypatch)
    tables, aggs = probing_scene()
    t, w = prior_tx()
    tables["transmitter"], tables["waveform"] = [t], [w]
    first = run(JobClient(tables, aggs))
    assert [r["to_state"] for r in first.log_rows] == ["established"]
    assert first.transmitter_rows == [] and first.waveform_rows == []
    assert first.chirp_rows[0]["tx_id"] == "" and first.chirp_rows[0]["prior_ids"] == [
        "hfrnet:TSTA"]
    apply(tables, first)
    second = run(JobClient(tables, aggs))
    assert second.rows() == {k: [] for k in second.rows()}


def _tie_rows():
    at = NOW - timedelta(hours=1)
    job = dict(log_row("C1", "established", at, "probing"), actor="chirp-state")
    veto = dict(log_row("C1", "rejected", at, "probing"), actor="michael")
    seed = dict(log_row("C2", "established", at), actor="seed")
    job2 = dict(log_row("C2", "dormant", at, "established"), actor="chirp-state")
    # The job rejects C3 in the same second a person pins it: the pin sorts first by to_state,
    # so only the person rank can make it win.
    reject = dict(log_row("C3", "rejected", at, "probing"), actor="chirp-state")
    pin = dict(log_row("C3", "established", at, "probing"), actor="rob")
    return [job, veto, seed, job2, reject, pin]


def test_tie_on_at_never_follows_row_order():
    rows = _tie_rows()
    for order in (rows, rows[::-1], [rows[1], rows[0], rows[3], rows[2], rows[5], rows[4]]):
        st = chirp_job.current_states(order, RULES.people.allowed)
        assert st["C1"] == ("rejected", NOW - timedelta(hours=1))     # the person's veto wins
        assert st["C2"][0] == "established"                             # then to_state
        assert st["C3"][0] == "established"                             # the person's pin wins


def test_log_read_in_order():
    c = JobClient({"receiver": list(RX)})
    run(c)
    sql = next(k["sql"] for k in c.calls if "FROM sigmondb.chirp_log" in k["sql"])
    assert sql.rstrip().endswith("ORDER BY chirp_id, at")


def test_locate_gets_only_stations_with_a_usable_cluster(monkeypatch):
    seen = located(monkeypatch)
    real = chirp_job.attribution.analyse_group

    def flagged(*a, **kw):
        g = real(*a, **kw)
        for cl in g["stations"].get("S3", []):
            cl["twin"] = True                  # S3's only cluster: set aside
        return g

    monkeypatch.setattr(chirp_job.attribution, "analyse_group", flagged)
    tables, aggs = probing_scene()
    run(JobClient(tables, aggs))
    assert seen == [(["S1", "S2"], 1000.0)]


def test_nearest_prior_names_and_kinds_a_placed_chirp():
    tables = {"chirp": [chirp_row("C1", first=NOW - timedelta(hours=3), lat=37.1, lon=-122.9,
                                  pos_source="triangulated")],
              "chirp_log": [log_row("C1", "seen", NOW - timedelta(hours=3))],
              "receiver": list(RX)}
    near_t, near_w = prior_tx("hfrnet:NEAR", lat=37.0, lon=-123.0, kind="codar")
    far_t, far_w = prior_tx("giro:AAFAR", lat=10.0, lon=-60.0, kind="ionosonde")
    far_t["source"] = far_w["source"] = "giro"
    tables["transmitter"], tables["waveform"] = [far_t, near_t], [far_w, near_w]
    res = run(JobClient(tables, {"sweep_hourly": [sweep("S1", H(1))]}))
    (row,) = res.chirp_rows
    assert row["prior_ids"] == ["giro:AAFAR", "hfrnet:NEAR"]
    assert (row["suggested_name"], row["kind"], row["state"]) == ("hfrnet:NEAR", "codar", "seen")
    assert res.log_rows == []


# --- fix round 2: a discovered identity needs a position --------------------------------------

def test_pinned_chirp_without_position_waits_then_gets_identity_once(monkeypatch):
    """A person pinned P established; it has no position and no prior.  With no locate fix the
    job writes no identity and counts it; once locate fixes it, identity goes out once."""
    tables, aggs = probing_scene("P")
    tables["chirp"] = [chirp_row("P", state="established", pin="pin", pin_by="rob")]
    tables["chirp_log"] = [dict(log_row("P", "established", NOW - timedelta(days=1), "probing"),
                                actor="rob")]

    def no_fix(st_clusters, rxpos, period_ms, rules, **kw):
        return {"status": "no_fit", "lat": None, "lon": None, "pos_sigma_m": None,
                "spread_us": None, "n_stations": len(st_clusters), "stations": []}

    monkeypatch.setattr(chirp_job, "locate", no_fix)
    first = run(JobClient(tables, aggs))
    assert first.transmitter_rows == [] and first.waveform_rows == []
    assert first.identity_waiting_position == 1 and first.relocated == 0
    assert any("identity_waiting_position 1" in ln for ln in first.lines)
    assert all(r["tx_id"] == "" for r in first.chirp_rows)
    apply(tables, first)

    located(monkeypatch)
    second = run(JobClient(tables, aggs))
    assert [t["tx_id"] for t in second.transmitter_rows] == ["P"]
    assert second.transmitter_rows[0]["lat"] is not None
    assert [w["tx_id"] for w in second.waveform_rows] == ["P"]
    assert (second.relocated, second.located, second.identity_waiting_position) == (1, 0, 0)
    assert second.log_rows == []
    apply(tables, second)
    third = run(JobClient(tables, aggs))
    assert third.transmitter_rows == [] and third.waveform_rows == []


def test_located_counts_fresh_locates_and_relocated_recovery(monkeypatch):
    located(monkeypatch)
    tables, aggs = probing_scene()
    first = run(JobClient(tables, aggs))
    assert (first.located, first.relocated) == (1, 0)
    log_only(tables, first)                       # crash: the position never reached the row
    second = run(JobClient(tables, aggs))
    assert (second.located, second.relocated) == (0, 1)
    assert [t["tx_id"] for t in second.transmitter_rows] == [CID]


# --- final review -----------------------------------------------------------------------------

def test_vetoed_chirp_never_turns_a_siblings_dwell_into_silence():
    """Important 5: X (vetoed) and Y (established, named) share a signature.  Scout-only S1
    heard Y by an arrival 40 h ago, then its sweeps matched X while it dwelled over the
    band.  Those (S1, hour) pairs are neither hearing nor silence for Y, so Y stays."""
    since = NOW - timedelta(days=5)
    tables = {"chirp": [chirp_row("X", state="rejected", pin="veto"),
                        chirp_row("Y", state="established", tx_id="SITE-Y")],
              "chirp_log": [log_row("X", "rejected", since, "probing"),
                            log_row("Y", "established", since, "probing")],
              "receiver": list(RX)}
    aggs = {"sweep_hourly": [sweep("S1", H(k)) for k in range(1, 8)],
            "dwell_hourly": [dict(reporter_id="S1", centre_hz=CENTRE, span_hz=200_000.0,
                                  hour=H(k), n=2) for k in range(1, 8)],
            "arrival_hourly": [dict(reporter_id="S1", centre_hz=CENTRE, rate_hz_per_s=RATE,
                                    hour=H(40), n=5, n_t6=0)]}
    res = run(JobClient(tables, aggs))
    assert res.log_rows == []
    assert res.states == {"established": 1, "rejected": 1}


def solution(tx_id, window_to, offset_ns, sigma_ns=40.0, solve_id="s1"):
    return dict(tx_id=tx_id, solve_id=solve_id, window_to=window_to, offset_ns=offset_ns,
                offset_sigma_ns=sigma_ns)


def test_offsets_follow_the_latest_recent_solution():
    """Important 2: each run gives a chirp with a tx_id its transmitter's latest recent
    solution offset and clears a stale one; a rerun on the same solutions writes nothing."""
    from sigmondb.publish import PUBLISH_MAX_AGE_H
    est = NOW - timedelta(days=5)
    tables = {"chirp": [chirp_row("SITE-A", state="established", tx_id="SITE-A",
                                  offset_ns=111, offset_sigma_ns=9.0),
                        chirp_row("SITE-B", state="established", tx_id="SITE-B",
                                  centre=5_000_000.0, offset_ns=222, offset_sigma_ns=9.0),
                        chirp_row("C3", centre=7_000_000.0, state="candidate",
                                  state_since=NOW - timedelta(hours=2))],
              "chirp_log": [log_row("SITE-A", "established", est),
                            log_row("SITE-B", "established", est),
                            log_row("C3", "candidate", NOW - timedelta(hours=2))],
              "receiver": list(RX),
              "solution": [solution("SITE-A", NOW - timedelta(hours=2), 500),
                           solution("SITE-A", NOW - timedelta(hours=1), 600, 30.0, "s2"),
                           solution("SITE-B", NOW - timedelta(hours=PUBLISH_MAX_AGE_H + 1), 7),
                           solution("C3", NOW - timedelta(hours=1), 9)]}
    c = JobClient(tables, {})
    res = run(c)
    assert res.log_rows == []
    by = {r["chirp_id"]: (r["offset_ns"], r["offset_sigma_ns"]) for r in res.chirp_rows}
    assert by == {"SITE-A": (600, 30.0), "SITE-B": (None, None)}     # C3 has no tx_id
    (q,) = [k for k in c.calls if "FROM sigmondb.solution" in k["sql"]]
    assert "offset_ns IS NOT NULL" in q["sql"]
    assert chirp_job.solution_not_set_aside("codar") in q["sql"]
    assert q["parameters"] == {"max_age_h": PUBLISH_MAX_AGE_H}
    apply(tables, res)
    assert run(JobClient(tables, {})).chirp_rows == []


def test_attribution_name_takes_the_listed_position(monkeypatch):
    """Minor 9: a located chirp that attribution names takes the named site's listed
    position, pos_source 'listed', and gets no discovered identity."""
    located(monkeypatch)
    tables, aggs = probing_scene()
    site = {c: None for c in ddl.COLUMNS["transmitter"]}
    site.update(tx_id="SITE-A", kind="codar", status="established", lat=37.25, lon=-122.75,
                pos_sigma_m=30.0, pos_source="listed", valid_from=datetime(2026, 1, 1),
                version=1, source="seed")
    tables["transmitter"] = [site]
    tables["attribution"] = [dict(run_at=NOW - timedelta(hours=1), run_id="r1",
                                  mode_policy="default", status="named", p_null=0.0,
                                  tx_id="SITE-A", group_key=GK)]
    res = run(JobClient(tables, aggs))
    (row,) = res.chirp_rows
    assert (row["state"], row["tx_id"]) == ("established", "SITE-A")
    assert (row["lat"], row["lon"], row["pos_sigma_m"], row["pos_source"]) == (
        37.25, -122.75, 30.0, "listed")
    assert res.transmitter_rows == [] and res.waveform_rows == []


def test_one_chirps_fault_skips_that_chirp_alone(monkeypatch):
    """Minor 10: a fault in one chirp's turn logs one line, counts an error, drops what that
    chirp's turn added (its move too) and leaves the other chirps alone."""
    real = chirp_job._reconcile

    def flaky(c, *a, **kw):
        if c["chirp_id"] == "A":
            raise RuntimeError("boom at 40.02 -90.04")
        return real(c, *a, **kw)

    monkeypatch.setattr(chirp_job, "_reconcile", flaky)
    tables = {"chirp": [chirp_row("A", centre=5_000_000.0), chirp_row("B", centre=7_000_000.0)],
              "chirp_log": [log_row("A", "seen", NOW - timedelta(days=3)),
                            log_row("B", "seen", NOW - timedelta(days=3))],
              "receiver": list(RX)}
    sweeps = [sweep("S1", H(k), centre=f) for f in (5_000_000.0, 7_000_000.0)
              for k in (1, 3, 5, 7, 9, 11)]
    res = run(JobClient(tables, {"sweep_hourly": sweeps}))
    assert [(r["chirp_id"], r["to_state"]) for r in res.log_rows] == [("B", "candidate")]
    assert [r["chirp_id"] for r in res.chirp_rows] == ["B"]
    assert res.errors == 1 and res.moves == {"seen->candidate": 1}
    assert "chirp-state: A: RuntimeError; skipped" in res.lines
    assert not any("boom" in ln for ln in res.lines)
