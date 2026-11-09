"""Back-fill timing provenance from hf-timestd t3_shadow minute rows.

A capture that had a T6 anchor gets the assumed block: u_epoch_ns from T6's stated scatter
(50-230 ns, the top of the range), k = 2, a_level_provenance 'assumed', the minute row's
counter epoch.  A capture with no anchor gets the null block.  Task 4 (HFDL) reuses both
builders, so each has one definition, here.
"""
import json
from pathlib import Path

U_EPOCH_NS = 230.0
K = 2.0
ORIGIN = "t6_anchor"


def null_provenance():
    return dict(u_epoch_ns=None, k=None, p=None, a_level="", a_level_provenance="",
                counter_epoch_id="", origin="")


def assumed_provenance(counter_epoch_id):
    return dict(u_epoch_ns=U_EPOCH_NS, k=K, p=None, a_level="", a_level_provenance="assumed",
                counter_epoch_id=counter_epoch_id or "", origin=ORIGIN)


def load_minutes(paths):
    """Minute rows (kind 'minute') from hf-timestd JSON Lines files, in file order."""
    rows = []
    for p in paths:
        with open(Path(p)) as fh:
            for line in fh:
                line = line.strip()
                if not line:
                    continue
                r = json.loads(line)
                if r.get("kind", "minute") == "minute":
                    rows.append(r)
    return rows


def _anchor_s(row):
    ns = (row.get("t3_shadow") or {}).get("t6_anchor_utc_ns")
    return float(ns) * 1e-9 if ns else None


def provenance_at(minutes, utc, max_age_s=120.0):
    """Provenance columns from the minute row whose T6 anchor lies nearest `utc` (epoch s).

    The null block when no anchored row lies within max_age_s.
    """
    best, best_age = None, None
    for r in minutes:
        a = _anchor_s(r)
        if a is None:
            continue
        age = abs(a - utc)
        if best_age is None or age < best_age:
            best, best_age = r, age
    if best is None or best_age > max_age_s:
        return null_provenance()
    return assumed_provenance((best.get("t3") or {}).get("counter_epoch_id") or "")
