"""Seed the chirp table from transmitters the fleet already solves or names.

A transmitter seeds as `established` when it has a waveform, holds no chirp row yet, and
either a recent solution with an offset or the latest attribution names it.  Listed-only
transmitters (an online list's site with neither) stay weak priors and get no row.
"""
from datetime import datetime, timedelta, timezone

from sigmondb import chirp
from sigmondb.attribution import NAMED_STATUSES

SOLUTION_MAX_AGE = timedelta(days=30)


def _now():
    return datetime.now(timezone.utc).replace(tzinfo=None, microsecond=0)


def _latest(rows, rank):
    """The row with the greatest rank(row) per tx_id."""
    best = {}
    for r in rows:
        k = r["tx_id"]
        if k not in best or rank(r) > rank(best[k]):
            best[k] = r
    return best


def _hfdl(t):
    return t.get("kind") == "hfdl" or str(t["tx_id"]).startswith("hfdl:")


def seed_with_counts(transmitters, waveforms, solutions, attributions, existing_ids, now, rules):
    """(chirp rows, chirp_log rows, counts); counts hold seeded, skipped_existing,
    skipped_no_waveform and skipped_listed_only.  HFDL transmitters count nowhere."""
    txs = _latest(transmitters, lambda r: (r.get("valid_from") or datetime.min, r.get("version") or 0))
    wfs = _latest(waveforms, lambda r: r.get("valid_from") or datetime.min)
    sols = _latest([s for s in solutions
                    if s.get("offset_ns") is not None and s.get("window_to") is not None
                    and now - s["window_to"] <= SOLUTION_MAX_AGE],
                   lambda r: r["window_to"])
    names = {}
    for a in attributions:
        if (a.get("status") in NAMED_STATUSES and a.get("mode_policy") == "default"
                and a.get("tx_id") and not str(a["tx_id"]).startswith("cand:")):
            names.setdefault(a["tx_id"], a)
    counts = dict(seeded=0, skipped_existing=0, skipped_no_waveform=0, skipped_listed_only=0)
    rows, logs = [], []
    for tx_id, t in sorted(txs.items()):
        if _hfdl(t):
            continue
        if tx_id in existing_ids:
            counts["skipped_existing"] += 1
            continue
        w = wfs.get(tx_id)
        if w is None:
            counts["skipped_no_waveform"] += 1
            continue
        s, a = sols.get(tx_id), names.get(tx_id)
        if s is None and a is None:
            counts["skipped_listed_only"] += 1
            continue
        period = w.get("period_s")
        rows.append(dict(
            chirp_id=tx_id,
            group_key=chirp.group_key(w["centre_hz"], w["rate_hz_per_s"],
                                      period if period is not None else 1.0, w.get("duration_s")),
            kind=t.get("kind") or "", state="established", state_since=now,
            centre_hz=w["centre_hz"], span_hz=w.get("bandwidth_hz"),
            rate_hz_per_s=w["rate_hz_per_s"], period_s=period, duration_s=w.get("duration_s"),
            gate_period_s=w.get("gate_period_s"),
            lat=t.get("lat"), lon=t.get("lon"), pos_sigma_m=t.get("pos_sigma_m"),
            pos_source="listed",
            offset_ns=s["offset_ns"] if s else None, offset_sigma_ns=s.get("offset_sigma_ns") if s else None,
            first_heard=now, last_heard=None, reporter_ids=[], n_stations=0, heard_hours=0,
            tx_id=tx_id, prior_ids=[], suggested_name="", pin="", pin_by="", pin_why="",
            rules_version=rules.rules_version, updated_at=now))
        evidence = {"solve_id": s["solve_id"]} if s else {"run_id": a["run_id"]}
        logs.append(dict(at=now, chirp_id=tx_id, from_state="", to_state="established",
                         actor="seed", why="seeded from an established transmitter",
                         evidence=evidence, reporter_ids=[], rules_version=rules.rules_version))
        counts["seeded"] += 1
    return rows, logs, counts


def seed_rows(transmitters, waveforms, solutions, attributions, existing_ids, now, rules):
    """(chirp rows, chirp_log rows) for the transmitters that seed."""
    rows, logs, _ = seed_with_counts(transmitters, waveforms, solutions, attributions,
                                     existing_ids, now, rules)
    return rows, logs
