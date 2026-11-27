"""The chirp verb: pins, vetoes and local rejects written by hand, and the history view."""
from datetime import datetime, timedelta

import pytest

from sigmondb import chirp_job, cli
from tests.test_sigmondb_cli import QueryClient

T0 = datetime(2026, 11, 20, 12, 0, 0)


def _chirp(state="candidate", cid="cand:1", **kw):
    row = dict(chirp_id=cid, group_key="g", kind="codar", state=state, state_since=T0,
               centre_hz=4.463e6, span_hz=50e3, rate_hz_per_s=-25733.9, period_s=0.5,
               duration_s=None, gate_period_s=None, lat=41.4, lon=-75.6, pos_sigma_m=None,
               pos_source="", offset_ns=None, offset_sigma_ns=None, first_heard=T0,
               last_heard=T0, reporter_ids=["R1"], n_stations=1, heard_hours=3, tx_id="cand:1",
               prior_ids=[], suggested_name="", pin="", pin_by="", pin_why="",
               rules_version="rv1", updated_at=T0)
    row.update(kw)
    return row


def _log(at, to, frm="", actor="chirp-state", why="w", cid="cand:1", ev=None):
    return dict(at=at, chirp_id=cid, from_state=frm, to_state=to, actor=actor, why=why,
                evidence=ev or {}, reporter_ids=["R1"], rules_version="rv1")


def _run(monkeypatch, tables, *argv):
    client = QueryClient(tables)
    monkeypatch.setattr(cli, "connect", lambda conf: client)
    monkeypatch.setattr(cli, "_now_s", lambda: datetime(2026, 11, 21, 8, 0, 0))
    rc = cli.main(["chirp", *argv])
    return rc, client


def _tables(state="candidate"):
    return {"chirp": [_chirp(state)], "chirp_log": [_log(T0, state)]}


def test_pin_logs_who_and_why(monkeypatch):
    rc, c = _run(monkeypatch, _tables(), "pin", "--chirp", "cand:1", "--by", "rob",
                 "--why", "known CODAR")
    assert rc == 0
    assert [t.split(".")[-1] for t, _ in c.inserted] == ["chirp_log", "chirp"]
    log, row = c.inserted[0][1], c.inserted[1][1]
    assert (log["from_state"], log["to_state"], log["actor"], log["why"]) == (
        "candidate", "established", "rob", "known CODAR")
    assert log["rules_version"]
    assert (row["state"], row["pin"], row["pin_by"], row["pin_why"]) == (
        "established", "pin", "rob", "known CODAR")
    naive = [x.replace(tzinfo=None) for x in (row["state_since"], row["updated_at"], log["at"])]
    assert naive == [datetime(2026, 11, 21, 8)] * 3
    assert row["centre_hz"] == 4.463e6 and row["lat"] == 41.4 and row["reporter_ids"] == ["R1"]


def test_veto_logs_who_and_why(monkeypatch):
    rc, c = _run(monkeypatch, _tables("established"), "veto", "--chirp", "cand:1",
                 "--by", "michael", "--why", "ionosonde")
    assert rc == 0
    log, row = c.inserted[0][1], c.inserted[1][1]
    assert (log["from_state"], log["to_state"], log["actor"]) == (
        "established", "rejected", "michael")
    assert (row["state"], row["pin"], row["pin_by"], row["pin_why"]) == (
        "rejected", "veto", "michael", "ionosonde")


def test_by_required_and_listed(monkeypatch):
    with pytest.raises(SystemExit) as e:
        _run(monkeypatch, _tables(), "pin", "--chirp", "cand:1", "--why", "x")
    assert e.value.code == 2
    rc, c = _run(monkeypatch, _tables(), "pin", "--chirp", "cand:1", "--by", "mallory",
                 "--why", "x")
    assert rc == 2 and c.inserted == []
    rc, c = _run(monkeypatch, _tables(), "reject-local", "--reporter", "R1", "--chirp",
                 "cand:1", "--by", "mallory", "--why", "x")
    assert rc == 2 and c.inserted == []


@pytest.mark.parametrize("argv", [
    ["pin", "--why", "x"], ["veto", "--why", "x"], ["unpin"],
    ["reject-local", "--reporter", "R1", "--why", "x"],
    ["withdraw-local", "--reporter", "R1"]])
def test_unknown_chirp_refused(monkeypatch, argv):
    rc, c = _run(monkeypatch, {"chirp": [_chirp()], "chirp_log": []},
                 argv[0], "--chirp", "cand:nope", "--by", "rob", *argv[1:])
    assert rc == 1 and c.inserted == []


def test_empty_reporter_refused(monkeypatch):
    rc, c = _run(monkeypatch, _tables(), "reject-local", "--reporter", " ", "--chirp",
                 "cand:1", "--by", "rob", "--why", "x")
    assert rc == 2 and c.inserted == []


def test_unpin_returns_control_to_job(monkeypatch):
    t = {"chirp": [_chirp("rejected", pin="veto", pin_by="rob", pin_why="no")],
         "chirp_log": [_log(T0, "candidate"),
                       _log(T0 + timedelta(hours=1), "rejected", "candidate", "rob", "no")]}
    rc, c = _run(monkeypatch, t, "unpin", "--chirp", "cand:1", "--by", "rob")
    assert rc == 0
    log, row = c.inserted[0][1], c.inserted[1][1]
    assert (log["from_state"], log["to_state"], log["actor"], log["why"]) == (
        "rejected", "rejected", "rob", "unpin")
    assert (row["state"], row["pin"], row["pin_by"], row["pin_why"]) == ("rejected", "", "", "")
    rows = t["chirp_log"] + [log]
    assert chirp_job.current_states(rows)["cand:1"][0] == "rejected"


def test_reject_local_copies_key(monkeypatch):
    rc, c = _run(monkeypatch, _tables(), "reject-local", "--reporter", "R1", "--chirp",
                 "cand:1", "--by", "rob", "--why", "local birdie")
    assert rc == 0 and [t.split(".")[-1] for t, _ in c.inserted] == ["chirp_reject_local"]
    r = c.inserted[0][1]
    assert (r["reporter_id"], r["centre_hz"], r["rate_hz_per_s"], r["period_s"], r["span_hz"],
            r["chirp_id"], r["reason"], r["set_by"], r["active"]) == (
        "R1", 4.463e6, -25733.9, 0.5, 50e3, "cand:1", "local birdie", "rob", 1)


def test_withdraw_local_inserts_inactive_row(monkeypatch):
    first = dict(reporter_id="R1", centre_hz=4.463e6, rate_hz_per_s=-25733.9, period_s=0.5,
                 span_hz=50e3, chirp_id="cand:1", reason="local birdie", set_by="rob",
                 set_at=datetime(2026, 11, 21, 8, 0, 0), active=1)
    t = dict(_tables(), chirp_reject_local=[first])
    rc, c = _run(monkeypatch, t, "withdraw-local", "--reporter", "R1", "--chirp", "cand:1",
                 "--by", "michael")
    assert rc == 0 and [t_.split(".")[-1] for t_, _ in c.inserted] == ["chirp_reject_local"]
    r = c.inserted[0][1]
    assert r["active"] == 0 and r["set_by"] == "michael"
    assert r["set_at"].replace(tzinfo=None) > first["set_at"]
    assert all(r[k] == first[k] for k in ("reporter_id", "centre_hz", "rate_hz_per_s",
                                           "period_s", "chirp_id"))


def test_withdraw_local_without_reject_refused(monkeypatch):
    rc, c = _run(monkeypatch, _tables(), "withdraw-local", "--reporter", "R1", "--chirp",
                 "cand:1", "--by", "rob")
    assert rc == 1 and c.inserted == []


def test_show_prints_log_in_order(monkeypatch, capsys):
    t = {"chirp": [_chirp("rejected")],
         "chirp_log": [_log(T0 + timedelta(hours=2), "rejected", "candidate", "rob", "no",
                            ev={"b": "2", "a": "1"}),
                       _log(T0, "candidate", ev={"heard_while_rejected": "3"})]}
    rc, c = _run(monkeypatch, t, "show", "--chirp", "cand:1")
    out = capsys.readouterr().out
    assert rc == 0 and c.inserted == []
    assert out.index("heard_while_rejected=3") < out.index("candidate -> rejected")
    assert "rob" in out and "a=1 b=2" in out


def test_show_hides_unnamed_position(monkeypatch, capsys):
    _run(monkeypatch, _tables(), "show", "--chirp", "cand:1")
    out = capsys.readouterr().out
    assert "41.4" not in out and "-75.6" not in out and "FN21" in out
    t = {"chirp": [_chirp("established", cid="wwv", tx_id="CODAR-X")], "chirp_log": []}
    _run(monkeypatch, t, "show", "--chirp", "wwv")
    out = capsys.readouterr().out
    assert "41.4" in out and "-75.6" in out
