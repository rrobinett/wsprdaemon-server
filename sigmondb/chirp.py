"""Chirp rules and signature matching.

Every threshold lives in chirp_rules.toml.  This module loads the file into
frozen dataclasses and decides whether two sweeps belong to one transmitter.
"""
import dataclasses
import hashlib
import json
import statistics
import tomllib
import types
from dataclasses import dataclass
from datetime import date
from pathlib import Path
from typing import Mapping, Optional

RULES_PATH = Path(__file__).with_name("chirp_rules.toml")


@dataclass(frozen=True)
class MatchRules:
    centre_tol_hz: float
    rate_tol_frac: float
    rate_tol_min_hz_s: float
    period_tol_frac: float
    span_overlap_min: float
    wide_span_hz: float


@dataclass(frozen=True)
class IndependenceRules:
    co_site_km: float


@dataclass(frozen=True)
class SeenRules:
    drop_after_h: float
    max_new_per_reporter_per_run: int


@dataclass(frozen=True)
class CandidateRules:
    min_hours: int
    min_dwells: int
    min_stations: int
    window_h: float
    drop_after_days: float


@dataclass(frozen=True)
class LocalRules:
    min_neighbours: int
    neighbour_km: float
    min_covered_dwells: int
    after_h: float


@dataclass(frozen=True)
class ProbingRules:
    min_capture_stations: int
    max_days: float


@dataclass(frozen=True)
class EstablishedRules:
    min_waveform_stations: int
    min_t6_stations: int
    min_steady_stations: int
    min_steady_h: float
    max_cluster_mad_us: float
    max_pos_sigma_km: float


@dataclass(frozen=True)
class RejectedRules:
    min_probe_stations: int
    min_probing_h: float


@dataclass(frozen=True)
class DormantRules:
    after_h: float
    min_covered_dwells: int
    history_days: float


@dataclass(frozen=True)
class RetiredRules:
    after_days: float


@dataclass(frozen=True)
class PeopleRules:
    allowed: tuple


@dataclass(frozen=True)
class MenuRules:
    top_n: int
    near_km: float
    history_days: float
    snr_floor_db: float
    snr_full_db: float
    unknown_snr_factor: float
    skywave_timing_weight: float
    offset_trusted_sigma_us: float
    shared_t6_override: bool
    state_weight: Mapping[str, float]
    cost: Mapping[str, float]


_SECTIONS = {
    "match": MatchRules, "independence": IndependenceRules, "seen": SeenRules,
    "candidate": CandidateRules, "local": LocalRules, "probing": ProbingRules,
    "established": EstablishedRules, "rejected": RejectedRules,
    "dormant": DormantRules, "retired": RetiredRules, "people": PeopleRules,
    "menu": MenuRules,
}


@dataclass(frozen=True)
class Rules:
    rules_version: str
    match: MatchRules
    independence: IndependenceRules
    seen: SeenRules
    candidate: CandidateRules
    local: LocalRules
    probing: ProbingRules
    established: EstablishedRules
    rejected: RejectedRules
    dormant: DormantRules
    retired: RetiredRules
    people: PeopleRules
    menu: MenuRules
    text: str = ""  # the file's text, so rules_digest needs no second argument


def _freeze(value):
    if isinstance(value, list):
        return tuple(_freeze(v) for v in value)
    if isinstance(value, dict):
        return types.MappingProxyType({k: _freeze(v) for k, v in value.items()})
    return value


def _section(raw: dict, name: str, cls):
    if name not in raw:
        raise ValueError(f"chirp rules: missing section [{name}]")
    body = raw[name]
    want = [f.name for f in dataclasses.fields(cls)]
    for key in want:
        if key not in body:
            raise ValueError(f"chirp rules: missing key {name}.{key}")
    for key in body:
        if key not in want:
            raise ValueError(f"chirp rules: unknown key {name}.{key}")
    return cls(**{k: _freeze(body[k]) for k in want})


def load_rules(path: Path = RULES_PATH) -> Rules:
    """Parse the rules file.  A missing or unknown section or key raises
    ValueError naming it."""
    text = Path(path).read_text(encoding="utf-8")
    raw = tomllib.loads(text)
    if "rules_version" not in raw:
        raise ValueError("chirp rules: missing key rules_version")
    unknown = set(raw) - set(_SECTIONS) - {"rules_version"}
    if unknown:
        raise ValueError(f"chirp rules: unknown section [{sorted(unknown)[0]}]")
    sections = {n: _section(raw, n, c) for n, c in _SECTIONS.items()}
    return Rules(rules_version=raw["rules_version"], text=text, **sections)


def rules_digest(rules: Rules) -> str:
    """sha256 hex of the rules file text."""
    return hashlib.sha256(rules.text.encode("utf-8")).hexdigest()


@dataclass(frozen=True)
class Signature:
    centre_hz: float
    rate_hz_per_s: float
    period_s: float
    span_hz: Optional[float] = None
    duration_s: Optional[float] = None


# Copied from hamsci_dsp.sigmondb_rows.group_key (the server does not depend on
# hamsci-dsp).  The two must stay in step: same format, same rounding.
def group_key(centre_hz: float, rate_hz_per_s: float, period_s: float = 1.0,
              sweep_s: Optional[float] = None) -> str:
    """A sweep group's name, from what a receiver can measure of any chirp:
    the sweep's centre (the middle of its span), its rate, its repetition
    period unless 1 s, and its sweep duration when shorter than the period.

    The rate alone does not separate sources: 49.63 kHz swept 4 times a
    second and 99.26 kHz swept twice give the same Hz/s but fold delays into
    0.25 s and 0.5 s periods (Troyer et al. 2025, Table 1).  A gated sweep,
    such as an ionosonde sweeping 2-30 MHz for 280 s every 300 s, needs its
    duration too.  A continuous 1 s sweep keeps the back-fill's form, so the
    4.46 MHz history runs on unchanged."""
    key = f"{centre_hz / 1e6:.4f} {rate_hz_per_s / 1e3:+.2f}"
    if abs(period_s - 1.0) > 1e-9:
        key += f" {period_s:g}s"
    if sweep_s is not None and sweep_s < period_s - 1e-9:
        key += f" sweep{sweep_s:g}s"
    return key


def _sign(x: float) -> int:
    return (x > 0) - (x < 0)


def matches(a: Signature, b: Signature, rules: Rules) -> bool:
    """True when two signatures describe one transmitter.

    Rates must not have opposite signs; a zero rate agrees with either sign
    when the difference is within tolerance.  The rate tolerance scales with
    the larger magnitude, so the test is symmetric.  Periods compare relative
    to the longer one.  When both spans are known and wider than wide_span_hz,
    span overlap replaces the centre test; a span of None means the centre
    test applies."""
    m = rules.match
    if _sign(a.rate_hz_per_s) * _sign(b.rate_hz_per_s) < 0:
        return False
    tol = max(m.rate_tol_min_hz_s,
              m.rate_tol_frac * max(abs(a.rate_hz_per_s), abs(b.rate_hz_per_s)))
    if abs(a.rate_hz_per_s - b.rate_hz_per_s) > tol:
        return False
    if abs(a.period_s - b.period_s) > m.period_tol_frac * max(a.period_s, b.period_s):
        return False
    if (a.span_hz is not None and b.span_hz is not None
            and a.span_hz > m.wide_span_hz and b.span_hz > m.wide_span_hz):
        lo = max(a.centre_hz - a.span_hz / 2, b.centre_hz - b.span_hz / 2)
        hi = min(a.centre_hz + a.span_hz / 2, b.centre_hz + b.span_hz / 2)
        return max(0.0, hi - lo) >= m.span_overlap_min * min(a.span_hz, b.span_hz)
    return abs(a.centre_hz - b.centre_hz) <= m.centre_tol_hz


def _need(sweep: dict, key: str):
    if key not in sweep:
        raise ValueError(f"sweep is missing key {key!r}")
    return sweep[key]


def _sig(sweep: dict) -> Signature:
    return Signature(_need(sweep, "centre_hz"), _need(sweep, "rate_hz_per_s"),
                     _need(sweep, "period_s"), sweep.get("span_hz"), sweep.get("duration_s"))


def _order_key(sweep: dict):
    """A total order: the three matching keys, then every tie-break, ending
    in a canonical dump of the dict, so input order never shows in the output."""
    span, dur = sweep.get("span_hz"), sweep.get("duration_s")
    return (_need(sweep, "centre_hz"), _need(sweep, "rate_hz_per_s"), _need(sweep, "period_s"),
            -1 if span is None else span, -1 if dur is None else dur,
            sweep.get("reporter_id") or "", str(sweep.get("time") or ""),
            json.dumps(sweep, sort_keys=True, default=str))


def cluster_sweeps(sweeps: list, rules: Rules) -> list:
    """Single-linkage clusters of sweep dicts under ``matches``.  Input is
    sorted by (centre_hz, rate_hz_per_s, period_s) and then a total tie-break (see _order_key); clusters come out ordered
    by their first member, members in that sorted order."""
    ordered = sorted(sweeps, key=_order_key)
    sigs = [_sig(s) for s in ordered]
    parent = list(range(len(ordered)))

    def find(i):
        while parent[i] != i:
            parent[i] = parent[parent[i]]
            i = parent[i]
        return i

    for i in range(len(ordered)):
        for j in range(i + 1, len(ordered)):
            if matches(sigs[i], sigs[j], rules):
                ri, rj = find(i), find(j)
                if ri != rj:
                    parent[max(ri, rj)] = min(ri, rj)
    groups: dict = {}
    for i, s in enumerate(ordered):
        groups.setdefault(find(i), []).append(s)
    return [groups[k] for k in sorted(groups)]


def founding_clusters(unmatched: list, rules: Rules) -> list:
    """Clusters of whole sweeps only.  A clipped sweep (clipped = 1 or True;
    absent means not clipped) never founds a chirp."""
    whole = [s for s in unmatched if not s.get("clipped")]
    return cluster_sweeps(whole, rules)


def _median_known(values: list) -> Optional[float]:
    known = [v for v in values if v is not None]
    return statistics.median(known) if known else None


def median_signature(cluster: list) -> Signature:
    """Per-field medians over a cluster.  span_hz and duration_s take the
    median of the members that carry them, or None when none does."""
    return Signature(
        statistics.median(_need(s, "centre_hz") for s in cluster),
        statistics.median(_need(s, "rate_hz_per_s") for s in cluster),
        statistics.median(_need(s, "period_s") for s in cluster),
        _median_known([s.get("span_hz") for s in cluster]),
        _median_known([s.get("duration_s") for s in cluster]))


def new_chirp_id(cluster: list, day: date) -> str:
    """A candidate's name: the day plus six hex characters of the median
    signature's group key, so a rerun on the same day names the same chirp
    whatever the member order."""
    m = median_signature(cluster)
    key = group_key(m.centre_hz, m.rate_hz_per_s, m.period_s, m.duration_s)
    return "cand:" + day.strftime("%Y%m%d") + "-" + hashlib.sha256(key.encode("utf-8")).hexdigest()[:6]
