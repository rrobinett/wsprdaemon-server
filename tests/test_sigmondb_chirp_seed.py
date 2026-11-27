"""Seeding chirp rows from established transmitters.  No ClickHouse: QueryClient stands in."""
import os
import sys
from datetime import datetime, timedelta

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from sigmondb import chirp, chirp_seed, cli, ddl  # noqa: E402
from test_sigmondb_cli import QueryClient  # noqa: E402

NOW = datetime(2026, 11, 27, 12, 0, 0)
RULES = chirp.load_rules()


def tx(tx_id, kind="codar", lat=41.0, lon=-75.0):
    return dict(tx_id=tx_id, kind=kind, lat=lat, lon=lon, pos_sigma_m=500.0,
                valid_from=datetime(2026, 1, 1), version=1)


def wf(tx_id, centre=4.46e6, valid_from=datetime(2026, 1, 1)):
    return dict(tx_id=tx_id, valid_from=valid_from, centre_hz=centre, rate_hz_per_s=-25733.9,
                period_s=1.0, duration_s=1.0, bandwidth_hz=50e3, gate_period_s=None)


def sol(tx_id, offset=120, age_days=1, solve_id="s1"):
    return dict(tx_id=tx_id, solve_id=solve_id, offset_ns=offset,
                window_to=NOW - timedelta(days=age_days))


def named(tx_id, run_id="r1", status="named"):
    return dict(tx_id=tx_id, run_id=run_id, status=status, mode_policy="default")


def seed(txs, wfs, sols=(), attrs=(), existing=()):
    return chirp_seed.seed_rows(txs, wfs, list(sols), list(attrs), set(existing), NOW, RULES)


def test_seed_takes_solved_and_named():
    txs = [tx("A"), tx("B"), tx("C")]
    wfs = [wf("A"), wf("B", 5e6), wf("C", 6e6)]
    rows, logs = seed(txs, wfs, [sol("A")], [named("B")])
    assert {r["chirp_id"] for r in rows} == {"A", "B"}
    assert all(r["state"] == "established" and r["pos_source"] == "listed" for r in rows)
    a = next(r for r in rows if r["chirp_id"] == "A")
    assert a["offset_ns"] == 120 and a["tx_id"] == "A" and a["centre_hz"] == 4.46e6
    assert a["rules_version"] == RULES.rules_version and a["reporter_ids"] == []
    assert next(r for r in rows if r["chirp_id"] == "B")["offset_ns"] is None
    assert {l["chirp_id"] for l in logs} == {"A", "B"}
    la = next(l for l in logs if l["chirp_id"] == "A")
    assert (la["from_state"], la["to_state"], la["actor"]) == ("", "established", "seed")
    assert la["evidence"] == {"solve_id": "s1"}
    assert next(l for l in logs if l["chirp_id"] == "B")["evidence"] == {"run_id": "r1"}
    assert set(rows[0]) == set(ddl.COLUMNS["chirp"]) and set(logs[0]) == set(ddl.COLUMNS["chirp_log"])


def test_listed_only_stays_prior():
    rows, logs = seed([tx("hfrnet:X", kind="ionosonde")], [wf("hfrnet:X")])
    assert rows == [] and logs == []


def test_seed_skips_hfdl_and_existing():
    txs = [tx("hfdl:1", kind="hfdl"), tx("hfdl:2", kind="codar"), tx("D"), tx("E")]
    wfs = [wf(t["tx_id"]) for t in txs]
    sols = [sol(t["tx_id"]) for t in txs]
    rows, _ = seed(txs, wfs, sols, existing={"D"})
    assert [r["chirp_id"] for r in rows] == ["E"]


def test_old_or_offsetless_solution_does_not_name():
    rows, _ = seed([tx("A"), tx("B")], [wf("A"), wf("B", 5e6)],
                   [sol("A", age_days=31), sol("B", offset=None)])
    assert rows == []


def test_seed_log_before_chirp(monkeypatch, capsys):
    client = QueryClient({"transmitter": [tx("A"), tx("L")], "waveform": [wf("A"), wf("L", 5e6)],
                          "solution": [sol("A")], "attribution": []})
    monkeypatch.setattr(cli, "connect", lambda conf: client)
    monkeypatch.setattr(chirp_seed, "_now", lambda: NOW)
    assert cli.main(["chirp-seed"]) == 0
    tables = [t for t, _ in client.inserted]
    assert tables == ["sigmondb.chirp_log", "sigmondb.chirp"]
    out = capsys.readouterr().out
    assert "seeded 1" in out and "41.0" not in out and "-75.0" not in out


def test_seed_is_idempotent():
    txs, wfs, sols = [tx("A")], [wf("A")], [sol("A")]
    rows, _ = seed(txs, wfs, sols)
    again = seed(txs, wfs, sols, existing={r["chirp_id"] for r in rows})
    assert again == ([], [])


def test_out_writes_files_and_no_inserts(monkeypatch, tmp_path):
    client = QueryClient({"transmitter": [tx("A")], "waveform": [wf("A")],
                          "solution": [sol("A")], "attribution": []})
    monkeypatch.setattr(cli, "connect", lambda conf: client)
    monkeypatch.setattr(chirp_seed, "_now", lambda: NOW)
    out = tmp_path / "chirp.jsonl"
    assert cli.main(["chirp-seed", "--out", str(out)]) == 0
    assert client.inserted == [] and client.commands == []
    assert len(out.read_text().splitlines()) == 1
    assert len((tmp_path / "chirp.jsonl.log").read_text().splitlines()) == 1
