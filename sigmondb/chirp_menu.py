"""The channel menu: for one station and one UTC hour, which chirp channels to record.

Two rankings, kept apart (spec: tasks/chirp-registry/2026-11-13-chirp-scout-design.md, "The
channel menu").  Timing value ranks established and dormant chirps by what they give a
station's clock: ground wave or skywave, expected SNR, a trusted offset, a T6 station in
reach, and whether the chirp runs at this hour.  Exploration value ranks every live chirp by
what a new look from here would add: its state, how new this station's path, distance and
bearing are next to the stations that already hear it, and how many stations probe it already.

prepare_hearing() indexes the fleet's evidence once; channel_menu() then runs per station and
hour against that index and does no I/O.

Positions:
- A station counts at the centre of its grid6 square.  channel_menu snaps the station it is
  given, and prepare_hearing snaps every hearing station, so no stored position enters a score.
- A named chirp (tx_id present and not "cand:") counts at its stored position.  An unnamed
  chirp counts at the centre of its 4-character grid square, the only form in which its
  position leaves wd30; the menu is published per receiver.
- A chirp with no position: path_w takes the skywave weight, reach_f looks only at whether a
  T6 station hears it, and novelty uses distance novelty alone (path and bearing need a
  position).
"""
import math
import statistics
from datetime import timedelta
from typing import Optional, TypedDict

from sigmondb import chirp_evidence as ce
from sigmondb.attribution import gc_km
from sigmondb.chirp import matches
from sigmondb.paths import path_kind
from sigmondb.publish import is_hfdl_chirp, latlon_to_grid4, latlon_to_grid6, named
from sigmondb.receivers import grid_to_latlon

LIVE_STATES = ("candidate", "probing", "established", "dormant")
TIMING_STATES = ("established", "dormant")

class MenuRow(TypedDict):
    channel_key: str
    centre_hz: float
    rate_hz_per_s: float
    period_s: Optional[float]
    chirp_ids: list
    states: list
    timing_value: float
    exploration_value: float
    cost: float
    shared_t6: int
    expected_snr_db: Optional[float]
    path: Optional[str]          # "ground_range", "skywave", or None for a chirp with no position
    rank_timing: int             # 1-based; 0 where the channel did not place
    rank_explore: int


def grid6_centre(lat, lon):
    """(lat, lon) moved to the centre of its grid6 square."""
    return grid_to_latlon(latlon_to_grid6(lat, lon))


def chirp_position(c):
    """The position a score may use: the stored one when named, else its grid4 centre."""
    if c.get("lat") is None or c.get("lon") is None:
        return None
    if named(c.get("tx_id")):
        return (c["lat"], c["lon"])
    return grid_to_latlon(latlon_to_grid4(c["lat"], c["lon"]))


class Hearing:
    """What the fleet heard of each chirp, indexed once for many channel_menu calls.

    positions: reporter_id -> (lat, lon), grid6 centres.
    site_snr: chirp_id -> {UTC hour 0-23: [((lat, lon), median snr)]}, one entry per
      independent station with a position: the median of that station's hourly median sweep
      SNRs in that UTC hour over menu.history_days.
    hearers: chirp_id -> set of reporters that heard it: a matching sweep within
      menu.history_days, or a matching T6 arrival.
    heard_recent: chirp_id -> set of reporters that heard it in the last menu.t6_recent_days
      days.
    n_probers: chirp_id -> distinct reporters with matching capture_recent rows.
    geometry: chirp_id -> _Geometry, what does not depend on the station: the chirp's scoring
      position, and per located hearer its grid6 position, path kind to the chirp and bearing
      from it.
    unparsed_t6: T6 arrival rows whose group_key did not parse; they count for no chirp."""

    def __init__(self):
        self.positions, self.site_snr, self.hearers = {}, {}, {}
        self.heard_recent, self.n_probers, self.geometry = {}, {}, {}
        self.unparsed_t6 = 0


class _Geometry:
    """One chirp's station-independent geometry.  pos: chirp_position(); located: [(reporter,
    (lat, lon), path kind to the chirp or None, bearing from the chirp or None)], sorted by
    reporter, for every located hearer; ground: the located hearers within ground range."""

    def __init__(self, c, hearers, positions):
        self.pos = pos = chirp_position(c)
        centre = c["centre_hz"]
        self.located, self.ground = [], set()
        for r in sorted(hearers):
            p = positions.get(r)
            if p is None:
                continue
            if pos is None:
                self.located.append((r, p, None, None))
                continue
            kind = path_kind(gc_km(p, pos), centre)
            self.located.append((r, p, kind, _bearing(pos, p)))
            if kind == "ground_range":
                self.ground.add(r)


def prepare_hearing(chirps, sweep_rows, rules, now, *, positions=None, coherence=None,
                    captures=(), t6_arrivals=()):
    """A Hearing from already-fetched rows.

    chirps: chirp rows (ddl.COLUMNS["chirp"]).  sweep_rows: chirp_evidence.AGG_SQL
    ["sweep_hourly"] rows, `hour` a naive UTC datetime.  positions: reporter_id -> (lat, lon)
    at any precision; each is snapped here to its grid6 centre.  coherence: reporter_id ->
    coherence_group, for independence.  captures: AGG_SQL["capture_recent"] rows.
    t6_arrivals: T6 arrival rows (time, reporter_id, group_key), as cli._ATTR_WHERE gives
    them.  now: naive UTC."""
    h = Hearing()
    h.positions = {r: grid6_centre(*p[:2]) for r, p in (positions or {}).items()
                   if p is not None and p[0] is not None and p[1] is not None}
    since = now - timedelta(days=rules.menu.history_days)
    recent = now - timedelta(days=rules.menu.t6_recent_days)
    sweeps = [s for s in sweep_rows or () if s["hour"] >= min(since, recent)]
    t6_last = {}                                  # (reporter, group_key) -> latest time
    for a in t6_arrivals or ():
        k = (a["reporter_id"], a["group_key"])
        if k not in t6_last or a["time"] > t6_last[k]:
            t6_last[k] = a["time"]
    reps = sorted({s["reporter_id"] for s in sweeps} | {k[0] for k in t6_last})
    rxpos = {r: p + ("grid",) for r, p in h.positions.items()}
    site = ce.independent(reps, rxpos, coherence or {}, rules)

    sweep_sigs = [ce._row_sig(s) for s in sweeps]
    sweep_ix = ce._ByCentre(sweep_sigs, rules)
    caps = list(captures or ())
    cap_ix = ce._ByCentre([ce.Signature(p["centre_hz"], p["rate_hz_per_s"], 1.0) for p in caps],
                          rules)
    t6_keys, parsed = [], {}
    for (rid, gk), t in sorted(t6_last.items(), key=lambda kv: (kv[0][0], str(kv[0][1]))):
        if gk not in parsed:
            try:
                parsed[gk] = ce._key_centre_rate(gk)
            except (ValueError, IndexError, TypeError):
                parsed[gk] = None
        if parsed[gk] is not None:
            t6_keys.append((rid, parsed[gk], t))
    h.unparsed_t6 = sum(1 for a in t6_arrivals or () if parsed.get(a["group_key"]) is None)

    for c in chirps:
        cid, sig = c["chirp_id"], ce.chirp_signature(c)
        hearers, heard_recent, snrs = set(), set(), {}
        for i in sweep_ix.candidates(sig):
            s = sweeps[i]
            if not matches(sig, sweep_sigs[i], rules):
                continue
            if s["hour"] >= recent:
                heard_recent.add(s["reporter_id"])
            if s["hour"] < since:
                continue
            hearers.add(s["reporter_id"])
            snr = ce._known(s.get("snr_med"))
            if snr is not None:
                st = site.get(s["reporter_id"], s["reporter_id"])
                snrs.setdefault(s["hour"].hour, {}).setdefault(st, []).append(snr)
        for rid, (centre, rate), t in t6_keys:
            if matches(sig, ce._sig_like(centre, rate, sig), rules):
                if t >= since:
                    hearers.add(rid)
                if t >= recent:
                    heard_recent.add(rid)
        h.site_snr[cid] = {hr: [(h.positions[st], statistics.median(v))
                                for st, v in sorted(by.items()) if st in h.positions]
                           for hr, by in snrs.items()}
        h.hearers[cid] = hearers
        h.geometry[cid] = _Geometry(c, hearers, h.positions)
        h.heard_recent[cid] = heard_recent
        h.n_probers[cid] = len({caps[i]["reporter_id"] for i in cap_ix.candidates(sig)
                                if matches(sig, ce._sig_like(caps[i]["centre_hz"],
                                                             caps[i]["rate_hz_per_s"], sig),
                                           rules)})
    return h


def fleet_hearing(chirps, sweep_rows, captures, t6_arrivals, receivers, rules, now):
    """(Hearing, T6 stations) for menus run at `now` (naive UTC): the one gathering the menu
    verb and the publisher share.  T6 stations: the reporters with a T6 arrival in the last
    menu.t6_recent_days days; only those arrivals reach prepare_hearing.  receivers: the
    latest receiver row per reporter; their positions are snapped to grid6 centres in
    prepare_hearing, and their coherence groups judge independence."""
    recent = now - timedelta(days=rules.menu.t6_recent_days)
    t6 = [r for r in t6_arrivals or () if r["time"] >= recent]
    positions = {r["reporter_id"]: (r["lat"], r["lon"]) for r in receivers
                 if r.get("lat") is not None and r.get("lon") is not None}
    coherence = {r["reporter_id"]: r.get("coherence_group") or "" for r in receivers}
    hearing = prepare_hearing(chirps, sweep_rows or [], rules, now, positions=positions,
                              coherence=coherence, captures=captures or [], t6_arrivals=t6)
    return hearing, {r["reporter_id"] for r in t6}


def _clamp(x, lo=0.0, hi=1.0):
    return max(lo, min(hi, x))


def _bearing(a, b):
    """Initial great-circle bearing, degrees, from a to b."""
    p1, l1, p2, l2 = map(math.radians, (a[0], a[1], b[0], b[1]))
    y = math.sin(l2 - l1) * math.cos(p2)
    x = math.cos(p1) * math.sin(p2) - math.sin(p1) * math.cos(p2) * math.cos(l2 - l1)
    return math.degrees(math.atan2(y, x))


def _angle(b1, b2):
    return abs((b1 - b2 + 180.0) % 360.0 - 180.0)


def _member(c, st, me, is_t6, hour_utc, hearing, t6_stations, rules, dist):
    """One chirp's values for this station and hour.  dist(p): km from the station to a
    hearer's grid6 position, memoised per call (hearers recur across chirps)."""
    m = rules.menu
    cid, state, centre = c["chirp_id"], c["state"], c["centre_hz"]
    geo = hearing.geometry.get(cid) or _Geometry(c, hearing.hearers.get(cid, ()),
                                                 hearing.positions)
    pos = geo.pos
    path = None if pos is None else path_kind(gc_km(st, pos), centre)
    near = [snr for p, snr in hearing.site_snr.get(cid, {}).get(hour_utc, ())
            if dist(p) <= m.near_km]
    snr = statistics.median(near) if near else None
    hearers = hearing.hearers.get(cid, set())

    timing, trusted = 0.0, False
    if state in TIMING_STATES:
        path_w = 1.0 if path == "ground_range" else m.skywave_timing_weight
        snr_f = (m.unknown_snr_factor if snr is None
                 else _clamp((snr - m.snr_floor_db) / (m.snr_full_db - m.snr_floor_db)))
        sigma = c.get("offset_sigma_ns")
        if c.get("offset_ns") is None:
            offset_f = 0.5
        elif sigma is not None and sigma <= m.offset_trusted_sigma_us * 1000:
            offset_f = 1.0
        else:
            offset_f = 0.75
        trusted = offset_f == 1.0
        t6_hearers = [r for r in hearers if r in t6_stations]
        if pos is None:
            reach = bool(t6_hearers)
        else:
            reach = any(r in geo.ground for r in t6_hearers)
            reach = reach or (is_t6 and path == "ground_range")
        reach_f = 1.0 if reach else 0.5
        bits = int(c.get("heard_hours") or 0)
        sched_f = 1.0 if bits == 0 or (bits >> hour_utc) & 1 else 0.2
        timing = path_w * snr_f * offset_f * reach_f * sched_f

    others = [g for g in geo.located if g[0] != me]
    if not others:
        novelty = 1.0          # no other located hearer: any look from here is new
    else:
        novelty = _clamp(min(dist(p) for _r, p, _k, _b in others) / 1000.0)
        if pos is not None:
            if all(kind != path for _r, _p, kind, _b in others):
                novelty = 1.0  # path novelty: no other hearer shares this station's path
            mine = _bearing(pos, st)
            novelty = max(novelty, _clamp(min(_angle(mine, b) for _r, _p, _k, b in others)
                                          / 90.0))
    explore = (m.state_weight.get(state, 0.0) * novelty
               / (1 + hearing.n_probers.get(cid, 0)))

    shared = {r for r in hearing.heard_recent.get(cid, ()) if r in t6_stations and r != me}
    cost = m.cost.get(c.get("kind") or "unknown", m.cost["unknown"])
    return dict(timing=timing, trusted=trusted, explore=explore, snr=snr, path=path,
                shared=shared, cost=cost)


def channel_menu(station: dict, hour_utc: int, chirps, hearing: Hearing, t6_stations: set,
                 rules) -> list:
    """The station's channel menu for one UTC hour: [MenuRow].

    station: {"reporter_id": str or None, "lat", "lon": a grid6 centre (snapped here again),
      "is_t6": bool}.  A station with no lat or lon gets no menu: ValueError.
    chirps: chirp rows; only candidate, probing, established and dormant ones, never HFDL,
      count.  One channel per group_key, listing every such chirp with that key; each value
      takes its best member (largest timing and exploration value, largest expected SNR,
      lowest cost, any ground-range path).  The channel counts as trusted when ANY member
      holds a trusted offset (ruling R28): the shared-T6-first rule lapses once the channel
      holds one.
    hearing: prepare_hearing() over the same chirps.
    t6_stations: reporters with T6 evidence in the last menu.t6_recent_days days.
    is_t6: a T6 station on a ground-range path to the chirp is itself the T6 reference in
      reach (reach_f = 1).

    Returns the channels that placed in either ranking: the timing ranking's in its order,
    then the exploration-only ones in theirs.  Each ranking keeps its top menu.top_n."""
    if station.get("lat") is None or station.get("lon") is None:
        raise ValueError("channel_menu: the station has no position")
    if not 0 <= int(hour_utc) <= 23:
        raise ValueError("channel_menu: hour_utc must be 0-23")
    hour_utc = int(hour_utc)
    st = grid6_centre(station["lat"], station["lon"])
    me = station.get("reporter_id")
    is_t6 = bool(station.get("is_t6"))
    m = rules.menu

    memo = {}

    def dist(p):
        d = memo.get(p)
        if d is None:
            d = memo[p] = gc_km(st, p)
        return d

    groups = {}
    for c in chirps:
        if c.get("state") in LIVE_STATES and not is_hfdl_chirp(c):
            groups.setdefault(c["group_key"], []).append(c)
    rows = []
    for key, members in groups.items():         # input order: the rankings' keys decide
        members = sorted(members, key=lambda c: c["chirp_id"])
        vals = [_member(c, st, me, is_t6, hour_utc, hearing, t6_stations, rules, dist)
                for c in members]
        best_t = max(range(len(vals)), key=lambda i: (vals[i]["timing"], -i))
        snrs = [v["snr"] for v in vals if v["snr"] is not None]
        paths = {v["path"] for v in vals} - {None}
        first = members[0]
        rows.append(dict(
            channel_key=key, centre_hz=first["centre_hz"],
            rate_hz_per_s=first["rate_hz_per_s"], period_s=first.get("period_s"),
            chirp_ids=[c["chirp_id"] for c in members], states=[c["state"] for c in members],
            timing_value=vals[best_t]["timing"],
            exploration_value=max(v["explore"] for v in vals),
            cost=min(v["cost"] for v in vals),
            shared_t6=len(set().union(*(v["shared"] for v in vals))),
            expected_snr_db=max(snrs) if snrs else None,
            path=("ground_range" if "ground_range" in paths else "skywave" if paths else None),
            rank_timing=0, rank_explore=0,
            _trusted=any(v["trusted"] for v in vals)))

    timing = sorted((r for r in rows if r["timing_value"] > 0),
                    key=lambda r: (0 if m.shared_t6_override and r["shared_t6"] > 0
                                   and not r["_trusted"] else 1,
                                   -r["timing_value"], r["cost"], r["channel_key"]))[:m.top_n]
    explore = sorted((r for r in rows if r["exploration_value"] > 0),
                     key=lambda r: (-r["exploration_value"], r["cost"],
                                    r["channel_key"]))[:m.top_n]
    for i, r in enumerate(timing, 1):
        r["rank_timing"] = i
    for i, r in enumerate(explore, 1):
        r["rank_explore"] = i
    out = timing + [r for r in explore if not r["rank_timing"]]
    for r in out:
        del r["_trusted"]
    return out
