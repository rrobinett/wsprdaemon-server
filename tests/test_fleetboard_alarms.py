import json
from datetime import datetime, timedelta

from fleetboard.alarms import CLEAR_AFTER, Ack, Alarm, alarm_hours, step
from fleetboard.checks import Finding

T0 = datetime(2026, 11, 16, 12, 0, 0)


def f(station="AC0G-B4", check="off_air", subject="", **n):
    return Finding(station, check, subject, n or {"gap_s": 4000})


def alarm(state="open", streak=0, first=T0, last=None, **kw):
    return Alarm(kw.get("station", "AC0G-B4"), kw.get("check", "off_air"),
                 kw.get("subject", ""), first, last or first, state, streak,
                 "{}", last or first)


def test_finding_opens_alarm():
    now = T0 + timedelta(microseconds=7)
    (a,) = step([], [f()], [], now)
    assert (a.state, a.false_streak) == ("open", 0)
    assert a.first_seen == a.last_seen == a.updated_at == T0
    assert json.loads(a.numbers) == {"gap_s": 4000}


def test_clears_after_three_false_checks_not_two():
    prev = [alarm()]
    for i in range(CLEAR_AFTER - 1):
        (a,) = step(prev, [], [], T0 + timedelta(minutes=i + 1))
        assert a.state == "open" and a.false_streak == i + 1
        prev = [a]
    (a,) = step(prev, [], [], T0 + timedelta(minutes=10))
    assert a.state == "cleared" and a.false_streak == CLEAR_AFTER
    assert a.last_seen == T0


def test_ack_marks_acknowledged():
    ack = Ack("AC0G-B4", "off_air", "", T0, "rob", "looking", T0)
    now = T0 + timedelta(minutes=5)
    (a,) = step([alarm()], [f()], [ack], now)
    assert a.state == "acknowledged" and a.updated_at == now
    (b,) = step([a], [f()], [], now + timedelta(minutes=5))
    assert b.state == "acknowledged" and b.last_seen == now + timedelta(minutes=5)


def test_ack_does_not_survive_a_reopen():
    ack = Ack("AC0G-B4", "off_air", "", T0, "rob", "", T0)
    cleared = alarm(state="cleared", streak=3)
    now = T0 + timedelta(hours=2)
    out = step([cleared], [f()], [ack], now)
    assert [(a.state, a.first_seen) for a in out] == [("open", now)]


def test_unchanged_open_alarm_still_writes_last_seen():
    now = T0 + timedelta(minutes=5)
    (a,) = step([alarm(streak=2)], [f(gap_s=5000)], [], now)
    assert a.last_seen == now and a.false_streak == 0 and a.state == "open"
    assert json.loads(a.numbers) == {"gap_s": 5000}


def test_alarm_hours_cover_first_to_last_seen():
    a = alarm(first=datetime(2026, 11, 16, 10, 40), last=datetime(2026, 11, 16, 12, 5),
              state="cleared")
    s = Alarm("wd30", "ingest_stalled", "", T0, T0, "open", 0,
              json.dumps({"silent": ["X1", "X2"]}), T0)
    got = alarm_hours([a, s], datetime(2026, 11, 16, 11, 30))
    h = datetime
    assert got["AC0G-B4"] == {h(2026, 11, 16, 11), h(2026, 11, 16, 12)}
    assert got["X1"] == got["X2"] == {h(2026, 11, 16, 12)}
    assert "wd30" not in got


def test_clear_after_is_three():
    assert CLEAR_AFTER == 3


def test_streak_two_with_ack_clears_not_acknowledged():
    ack = Ack("AC0G-B4", "off_air", "", T0, "rob", "", T0)
    (a,) = step([alarm(streak=2)], [], [ack], T0 + timedelta(minutes=9))
    assert a.state == "cleared"


def test_ack_naming_another_instance_changes_nothing():
    now = T0 + timedelta(minutes=5)
    acks = [
        Ack("AC0G-B4", "off_air", "", T0 + timedelta(hours=1), "rob", "", T0),
        Ack("OTHER", "off_air", "", T0, "rob", "", T0),
        Ack("AC0G-B4", "client_quiet", "", T0, "rob", "", T0),
        Ack("AC0G-B4", "off_air", "src", T0, "rob", "", T0),
    ]
    (a,) = step([alarm()], [f()], acks, now)
    assert a.state == "open"


def test_numbers_stored_sorted_raw_string():
    fd = Finding("AC0G-B4", "off_air", "", {"zeta": 1, "alpha": 2})
    (a,) = step([], [fd], [], T0)
    assert a.numbers == json.dumps({"zeta": 1, "alpha": 2}, sort_keys=True)
    assert a.numbers == '{"alpha": 2, "zeta": 1}'


def test_cleared_alarm_without_finding_is_not_returned():
    assert step([alarm(state="cleared", streak=3)], [], [], T0) == []


def test_false_streak_sets_updated_at():
    now = T0 + timedelta(minutes=3)
    (a,) = step([alarm()], [], [], now)
    assert a.updated_at == now and a.false_streak == 1


def test_hold_freezes_a_silent_stations_alarm_through_an_ingest_stall():
    """UACNJ off the air and acknowledged; wd30's ingest then stalls with UACNJ
    among the silent for 5 runs.  Its alarm must come out the same instance,
    still acknowledged: a stall must neither clear it nor re-open it unacked."""
    ack = Ack("UACNJ", "off_air", "", T0, "rob", "site visit", T0)
    (a,) = step([alarm(station="UACNJ")], [f(station="UACNJ")], [ack], T0 + timedelta(minutes=5))
    assert a.state == "acknowledged"
    stall = Finding("wd30", "ingest_stalled", "", {"silent": ["UACNJ"]})
    prev = [a]
    for i in range(5):
        now = T0 + timedelta(minutes=10 + 5 * i)
        out = step(prev, [stall], [ack], now, hold={"UACNJ"})
        assert [(x.station, x.check) for x in out] == [("wd30", "ingest_stalled")]
        prev = [a] + out
    assert (a.state, a.first_seen, a.false_streak) == ("acknowledged", T0, 0)


def test_hold_leaves_other_stations_alone():
    out = step([alarm(station="B")], [], [], T0 + timedelta(minutes=5), hold={"UACNJ"})
    assert [(x.station, x.false_streak) for x in out] == [("B", 1)]
