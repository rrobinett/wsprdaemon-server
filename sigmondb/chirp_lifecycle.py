"""The chirp lifecycle's moves: one pure function that decides a chirp's next state from
fleet evidence (chirp_evidence.Evidence) and the rules (chirp_rules.toml).

The hourly chirp-state job calls next_state for every chirp and logs each move it returns,
with the move's evidence.  next_state does no I/O and keeps no state: the chirp dict carries
the current state and state_since (naive UTC), as the job reads them from the log.

Spec: tasks/chirp-registry/2026-11-13-chirp-scout-design.md, "The lifecycle".

Terms, as every move uses them:
- "unheard for X": now - (ev.last_heard, else the chirp's first_heard) >= X.  ev.last_heard
  counts matching sweeps and matching arrivals of any origin, so a chirp whose T6 arrivals
  continue never reads as unheard because the scout stopped hearing it.
- "heard again": ev.last_heard is later than state_since.
- "covered": a station sits in ev.covered_silent with at least the rule's dwell count: that
  many distinct hours it dwelled over the chirp and heard nothing, all after the chirp's last
  hearing.  A candidate's local neighbours read ev.silent_hours (the whole window) instead:
  the one station that hears a local spur hears it every hour.
- Stations are the independent site representatives Evidence already keys by.
"""
from datetime import datetime, timedelta
from typing import NamedTuple, Optional

from sigmondb import attribution

STATES = ("seen", "candidate", "probing", "established", "dormant", "retired", "rejected",
          "dropped")


class Move(NamedTuple):
    to_state: str
    why: str
    evidence: dict
    local_reject: Optional[str] = None


def _s(x) -> str:
    """A number as the log stores it: 6, 28, 0.5."""
    if isinstance(x, float) and x.is_integer():
        return str(int(x))
    if isinstance(x, float):
        return f"{x:g}"
    return str(x)


def _base(ev) -> dict:
    """The evidence every move carries."""
    hours = {h for hs in ev.hearers.values() for h in hs}
    return {
        "n_hearers": str(len(ev.hearers)),
        "hearers": ",".join(sorted(ev.hearers)),
        "hours": str(len(hours)),
        "covered_silent": ",".join(f"{st}:{n}" for st, n in sorted(ev.covered_silent.items())),
        "probe_stations": ",".join(sorted(ev.probe_stations)),
        "last_heard": ev.last_heard.isoformat(sep=" ") if ev.last_heard else "",
    }


def _move(to_state, why, ev, thresholds, extra=None, local_reject=None) -> Move:
    evidence = _base(ev)
    evidence.update({k: _s(v) for k, v in thresholds.items()})
    evidence.update({k: str(v) for k, v in (extra or {}).items()})
    return Move(to_state, why, evidence, local_reject)


def _unheard(chirp, ev, now) -> timedelta:
    since = ev.last_heard if ev.last_heard is not None else chirp["first_heard"]
    return now - since


def _heard_again(chirp, ev) -> bool:
    return ev.last_heard is not None and ev.last_heard > chirp["state_since"]


def _covered_past_hearers(ev, min_dwells) -> list:
    """Past hearers (scout or T6) that dwelled over the chirp in at least min_dwells hours
    after its last hearing and heard nothing."""
    past = set(ev.hearers) | set(ev.t6_hearers)
    return sorted(st for st, n in ev.covered_silent.items() if st in past and n >= min_dwells)


def _local_neighbours(station, ev, rules, station_pos) -> list:
    """Independent stations within local.neighbour_km of station's grid centre that dwelled
    over the chirp in at least local.min_covered_dwells hours and never heard it."""
    lr = rules.local
    here = station_pos.get(station)
    if here is None:
        return []
    out = []
    for st, n in ev.silent_hours.items():
        if st == station or st in ev.hearers or n < lr.min_covered_dwells:
            continue
        there = station_pos.get(st)
        if there is not None and attribution.gc_km(here, there) <= lr.neighbour_km:
            out.append(st)
    return sorted(out)


def next_state(chirp: dict, ev, rules, now: datetime, *,
               station_pos: Optional[dict] = None) -> Optional[Move]:
    """The chirp's next state, or None when it stays.  The first move that applies wins.

    station_pos: independent station -> (lat, lon) of its grid6 centre.  Without it the
    candidate's local reject cannot fire."""
    state = chirp["state"]
    if state not in STATES:
        raise ValueError(f"chirp {chirp.get('chirp_id')}: unknown state {state!r}")
    pin = chirp.get("pin") or ""
    if pin in ("veto", "pin"):
        return None                      # a person decided; the job never moves it
    in_state = now - chirp["state_since"]

    if state == "seen":
        c = rules.candidate
        # Distinct UTC hours sit at least 1 h apart, so min_dwells distinct hours give
        # min_dwells dwells at least 1 h apart; the test is the larger of the two counts.
        need = max(c.min_hours, c.min_dwells)
        for st in sorted(ev.hearers):
            if len(set(ev.hearers[st])) >= need:
                return _move("candidate", "persists_hours", ev,
                             {"candidate.min_hours": c.min_hours,
                              "candidate.min_dwells": c.min_dwells}, {"station": st})
        # An hour bucket counts when any part of it lies inside the window.
        window_from = now - timedelta(hours=c.window_h)
        recent = sorted(st for st, hs in ev.hearers.items()
                        if any(h + timedelta(hours=1) > window_from for h in hs))
        if len(recent) >= c.min_stations:
            return _move("candidate", "persists_stations", ev,
                         {"candidate.min_stations": c.min_stations,
                          "candidate.window_h": c.window_h}, {"recent": ",".join(recent)})
        if _unheard(chirp, ev, now) >= timedelta(hours=rules.seen.drop_after_h):
            return _move("dropped", "unheard", ev, {"seen.drop_after_h": rules.seen.drop_after_h})
        return None

    if state == "candidate":
        lr = rules.local
        if (station_pos and len(ev.hearers) == 1
                and in_state >= timedelta(hours=lr.after_h)):
            station = next(iter(ev.hearers))
            near = _local_neighbours(station, ev, rules, station_pos)
            if len(near) >= lr.min_neighbours:
                return _move("dropped", "local_only", ev,
                             {"local.min_neighbours": lr.min_neighbours,
                              "local.neighbour_km": lr.neighbour_km,
                              "local.min_covered_dwells": lr.min_covered_dwells,
                              "local.after_h": lr.after_h},
                             {"local_reject": station, "neighbours": ",".join(near),
                              "silent_hours": ",".join(f"{st}:{ev.silent_hours[st]}"
                                                       for st in near)},
                             local_reject=station)
        if len(ev.probe_stations) >= rules.probing.min_capture_stations:
            return _move("probing", "probes_began", ev,
                         {"probing.min_capture_stations": rules.probing.min_capture_stations})
        if _unheard(chirp, ev, now) >= timedelta(days=rules.candidate.drop_after_days):
            return _move("dropped", "unheard", ev,
                         {"candidate.drop_after_days": rules.candidate.drop_after_days})
        return None

    if state == "probing":
        rj, est = rules.rejected, rules.established
        finders = set(ev.probe_found) | set(ev.hearers)
        if (len(ev.probe_stations) >= rj.min_probe_stations
                and in_state >= timedelta(hours=rj.min_probing_h) and len(finders) == 1):
            return _move("rejected", "one_station", ev,
                         {"rejected.min_probe_stations": rj.min_probe_stations,
                          "rejected.min_probing_h": rj.min_probing_h},
                         {"probe_found": ",".join(sorted(ev.probe_found))})
        steady = sorted(st for st, h in ev.steady_t6.items() if h >= est.min_steady_h)
        pos = ev.position
        named = ev.named_by_attribution
        if pos and pos.get("status") == "located" and pos.get("pos_sigma_m") is not None \
                and pos["pos_sigma_m"] <= est.max_pos_sigma_km * 1000:
            where = "located"
        elif named and not named.startswith("cand:"):
            where = "named:" + named
        else:
            where = None
        if (ev.waveform_agree >= est.min_waveform_stations
                and len(steady) >= est.min_steady_stations and where):
            extra = {"waveform_agree": ev.waveform_agree, "steady_t6": ",".join(steady),
                     "position": where}
            if where == "located":
                extra["pos_sigma_m"] = _s(float(pos["pos_sigma_m"]))
            return _move("established", "settled", ev,
                         {"established.min_waveform_stations": est.min_waveform_stations,
                          "established.min_steady_stations": est.min_steady_stations,
                          "established.min_steady_h": est.min_steady_h,
                          "established.max_pos_sigma_km": est.max_pos_sigma_km}, extra)
        if in_state >= timedelta(days=rules.probing.max_days):
            return _move("candidate", "probing_timeout", ev,
                         {"probing.max_days": rules.probing.max_days},
                         {"probing_days": int(in_state / timedelta(days=1))})
        return None

    if state == "established":
        d = rules.dormant
        covered = _covered_past_hearers(ev, d.min_covered_dwells)
        if _unheard(chirp, ev, now) >= timedelta(hours=d.after_h) and covered:
            return _move("dormant", "unheard_where_covered", ev,
                         {"dormant.after_h": d.after_h,
                          "dormant.min_covered_dwells": d.min_covered_dwells},
                         {"covered_hearers": ",".join(covered)})
        return None

    if state == "dormant":
        if _heard_again(chirp, ev):
            return _move("established", "heard_again", ev, {})
        d = rules.dormant
        covered = _covered_past_hearers(ev, d.min_covered_dwells)
        if _unheard(chirp, ev, now) >= timedelta(days=rules.retired.after_days) and covered:
            return _move("retired", "unheard_where_covered", ev,
                         {"retired.after_days": rules.retired.after_days,
                          "dormant.min_covered_dwells": d.min_covered_dwells},
                         {"covered_hearers": ",".join(covered)})
        return None

    if state == "retired":
        return _move("candidate", "heard_again", ev, {}) if _heard_again(chirp, ev) else None

    if state == "dropped":
        return _move("seen", "heard_again", ev, {}) if _heard_again(chirp, ev) else None

    # rejected never moves on evidence.  A fresh hearing goes into the next log row a
    # person writes (heard_while_rejected); the job records it, not this function.
    return None
