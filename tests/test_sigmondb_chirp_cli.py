"""The chirp verb: pins, vetoes and local rejects written by hand, and the history view."""
import os
import sys
from datetime import datetime, timedelta

import pytest

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from sigmondb import chirp_job, cli  # noqa: E402
from test_sigmondb_chirp_job import JobClient  # noqa: E402

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


def _run(monkeypatch, tables, *argv, have=None):
    client = JobClient(tables, have=have)
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
    assert rc == 2 and c.inserted == [] and c.commands == []
    rc, c = _run(monkeypatch, _tables(), "reject-local", "--reporter", "R1", "--chirp",
                 "cand:1", "--by", "mallory", "--why", "x")
    assert rc == 2 and c.inserted == [] and c.commands == []


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
    assert rc == 2 and c.inserted == [] and c.commands == []


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
    assert row["state_since"].replace(tzinfo=None) == T0
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


NOW = datetime(2026, 11, 21, 8, 0, 0)


def _at(c):
    return [r["at"].replace(tzinfo=None) for t, r in c.inserted if t.endswith("chirp_log")][0]


def test_at_one_second_later_when_chirp_updated_this_second(monkeypatch):
    t = {"chirp": [_chirp(updated_at=NOW)], "chirp_log": [_log(T0, "candidate")]}
    _, c = _run(monkeypatch, t, "pin", "--chirp", "cand:1", "--by", "rob", "--why", "x")
    assert _at(c) == NOW + timedelta(seconds=1)
    assert c.inserted[1][1]["updated_at"].replace(tzinfo=None) == NOW + timedelta(seconds=1)


def test_at_one_second_later_when_person_logged_this_second(monkeypatch):
    t = {"chirp": [_chirp()], "chirp_log": [_log(NOW, "established", "candidate", "michael")]}
    _, c = _run(monkeypatch, t, "veto", "--chirp", "cand:1", "--by", "rob", "--why", "x")
    assert _at(c) == NOW + timedelta(seconds=1)


def test_at_not_bumped_when_now_is_later(monkeypatch):
    t = {"chirp": [_chirp()], "chirp_log": [_log(T0, "candidate", actor="rob")]}
    _, c = _run(monkeypatch, t, "pin", "--chirp", "cand:1", "--by", "rob", "--why", "x")
    assert _at(c) == NOW


# --- final review -----------------------------------------------------------------------------

def test_reject_local_with_no_period_blocks_that_reporters_sweeps(monkeypatch):
    """Important 6: a chirp with no period counts as a 1 s sweep (group_key, gather), so a
    hand reject stores period 1.0 and then blocks the reporter's 1 s sweeps."""
    from sigmondb import chirp, chirp_evidence as ce
    t = {"chirp": [_chirp(period_s=None)], "chirp_log": [_log(T0, "candidate")]}
    rc, c = _run(monkeypatch, t, "reject-local", "--reporter", "R1", "--chirp", "cand:1",
                 "--by", "rob", "--why", "local birdie")
    assert rc == 0
    rej = c.inserted[0][1]
    assert rej["period_s"] == 1.0
    sw = dict(reporter_id="R1", centre_hz=4.463e6, rate_hz_per_s=-25733.9, period_s=1.0,
              span_hz=50e3, duration_s=None, clipped=0, hour=T0, n=3, snr_med=10.0, n_dwells=1)
    ev = ce.gather([_chirp(period_s=None)], {"sweep_hourly": [sw]}, [], [], [],
                   chirp.load_rules(), T0 + timedelta(hours=1), rejects=[rej])["cand:1"]
    assert ev.hearers == {}


def _rejected_tables(last_heard):
    since = T0 + timedelta(hours=1)
    return {"chirp": [_chirp("rejected", state_since=since, last_heard=last_heard,
                             pin="veto", pin_by="rob", pin_why="no")],
            "chirp_log": [_log(T0, "candidate"),
                          _log(since, "rejected", "candidate", "rob", "no")]}


def test_person_row_records_heard_while_rejected(monkeypatch):
    """Important 7: the next log row a person writes on a rejected chirp heard since its
    rejection carries heard_while_rejected=<last_heard>."""
    heard = T0 + timedelta(hours=5)
    rc, c = _run(monkeypatch, _rejected_tables(heard), "unpin", "--chirp", "cand:1",
                 "--by", "rob")
    assert rc == 0
    assert c.inserted[0][1]["evidence"] == {"heard_while_rejected": "2026-11-20 17:00:00"}
    # Last heard before the rejection: nothing to record.
    rc, c = _run(monkeypatch, _rejected_tables(T0), "unpin", "--chirp", "cand:1", "--by", "rob")
    assert rc == 0 and c.inserted[0][1]["evidence"] == {}


def test_show_on_a_host_without_the_tables(monkeypatch, capsys):
    """Minor 14: before chirp-state's tables exist, show says unknown chirp and exits 1."""
    rc, c = _run(monkeypatch, {}, "show", "--chirp", "cand:1",
                 have={"transmitter", "waveform", "receiver", "arrival", "solution"})
    err = capsys.readouterr().err
    assert rc == 1 and c.inserted == [] and c.commands == []
    assert err.strip() == "chirp show: unknown chirp cand:1"


def test_chirp_state_refuses_while_another_run_holds_the_lock(monkeypatch, tmp_path, capsys):
    """Important 3: a second writing run prints one line, writes nothing and exits 1; a dry
    run never takes the lock."""
    import fcntl
    lock = tmp_path / "chirp-state.lock"
    monkeypatch.setattr(cli, "CHIRP_STATE_LOCK", str(lock))
    conf = tmp_path / "c.conf"
    conf.write_text('CLICKHOUSE_HOST="h"\nCLICKHOUSE_PORT="1"\nCLICKHOUSE_ROOT_ADMIN_USER="u"\n'
                    'CLICKHOUSE_ROOT_ADMIN_PASSWORD="p"\n')
    client = JobClient({})
    monkeypatch.setattr(cli, "connect", lambda c: client)
    monkeypatch.setattr(cli, "_now_s", lambda: T0)
    with open(lock, "a") as held:
        fcntl.flock(held, fcntl.LOCK_EX | fcntl.LOCK_NB)
        assert cli.main(["chirp-state", "--conf", str(conf)]) == 1
        err = capsys.readouterr().err
        assert err.count("\n") == 1 and "another run holds" in err
        assert client.calls == [] and client.commands == [] and client.inserted == []
        assert cli.main(["chirp-state", "--dry-run", "--out", str(tmp_path / "o"),
                         "--conf", str(conf)]) == 0
    capsys.readouterr()
    assert cli.main(["chirp-state", "--conf", str(conf)]) == 0     # released: it runs
    assert client.commands
