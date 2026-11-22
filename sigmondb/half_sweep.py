"""codar-sounder rows older than 0.8.0 count delay_ns half a sweep late; the readers move them back.

The schema counts delay_ns from the sweep start.  codar-sounder's replica sweeps from its channel
centre, which the transmitter crosses at mid-sweep, so releases before 0.8.0 wrote tau + S/2,
S the sweep duration (0.5 s at 4.463 MHz).  Release 0.8.0 (codar-sounder 99371d6) counts from the
sweep start itself.  Those older rows stay in sigmondb.arrival as they came: delay_ns sits in the
table's sort key, so ClickHouse refuses an UPDATE, and a corrected copy would not replace the
original.  So both readers of arrival delay_ns, cli._from_db (the solver) and
cli._attribution_inputs (attribution), pass their rows through to_sweep_start.  Both must, or
apply_attribution's match of names to solver clusters (by delay modulo the period) fails silently.

S and the period come from the row's group key, written by hamsci-dsp's sigmondb_rows.group_key:
"<centre MHz> <rate kHz/s>", then "<P>s" unless the period is 1 s, then "sweep<S>s" when the sweep
runs shorter than the period.  sweep_of and period_of in chirp-recorder's compare.py read the same
tokens, so the two programs agree on S.  Nothing here writes a corrected value back.
"""
import math
import re
import sys

FIXED_FROM = (0, 8, 0)          # the first codar-sounder release that counts from the sweep start
_PREFIX = "codar-sounder/"
_VERSION_RE = re.compile(r"(\d+)\.(\d+)(?:\.(\d+))?")
# The bounds a group key's S and P must hold, in seconds: 1e-3 <= S <= P <= 3600.  Real sweeps sit
# far inside them (CODAR 0.25-2 s, ionosondes a few hundred s).  A key outside them gives no S, so
# a crafted key cannot reach the arithmetic with a zero, sub-ns, infinite or NaN period.
MIN_S, MAX_S = 1e-3, 3600.0


def codar_version(detector_version):
    """(major, minor, patch) of "codar-sounder/X.Y[.Z]...", else None.

    Only the leading X.Y[.Z] counts, as in chirp-recorder's compare._version: a local or dev
    build such as 0.8.0+g1234 or 0.8.1.dev3 parses as its release, and a missing patch reads 0."""
    text = str(detector_version or "")
    if not text.startswith(_PREFIX):
        return None
    m = _VERSION_RE.match(text[len(_PREFIX):].strip())
    if m is None:
        return None
    return int(m.group(1)), int(m.group(2)), int(m.group(3) or 0)


def needs_correction(detector_version):
    """True for a codar-sounder release before 0.8.0.  Every other row passes: back-fill
    (offline-2026-11), chirp-recorder, 0.8.0 and later, and a codar-sounder name with no
    readable version."""
    v = codar_version(detector_version)
    return v is not None and v < FIXED_FROM


def period_and_sweep(group_key):
    """(period_s, sweep_s) from a group key, or None when the key cannot give them.

    The period comes from the "<P>s" token, 1 s if none; the sweep from "sweep<S>s", the period
    if none (a continuous sweep).  A key whose centre or rate is not a number, or that carries
    any other token, gives None: this function never guesses S.  So does a key whose S and P fall
    outside MIN_S <= S <= P <= MAX_S, or are not finite."""
    parts = str(group_key or "").split()
    if len(parts) < 2:
        return None
    try:
        float(parts[0])
        float(parts[1])
    except ValueError:
        return None
    period, sweep = 1.0, None
    for tok in parts[2:]:
        if not tok.endswith("s"):
            return None
        try:
            if tok.startswith("sweep"):
                sweep = float(tok[5:-1])
            else:
                period = float(tok[:-1])
        except ValueError:
            return None
    if sweep is None:
        sweep = period
    if not (math.isfinite(period) and math.isfinite(sweep)
            and MIN_S <= sweep <= period <= MAX_S
            and round(period * 1e9) >= 1 and round(sweep * 1e9) >= 1):
        return None
    return period, sweep


def sweep_start_delay(row):
    """The row's delay from the sweep start, in ns.

    A codar-sounder row before 0.8.0 comes back S/2 earlier, folded into [0, period), as
    chirp-recorder's compare.from_codar_convention does.  Any other row keeps its delay_ns.
    None when the row needs the correction and its group key cannot give S."""
    if not needs_correction(row.get("detector_version")):
        return row["delay_ns"]
    ps = period_and_sweep(row.get("group_key"))
    if ps is None:
        return None
    period, sweep = ps
    return (int(row["delay_ns"]) - round(sweep * 1e9 / 2)) % round(period * 1e9)


def to_sweep_start(rows, who):
    """New dicts with delay_ns counted from the sweep start (sweep_start_delay).

    An old codar-sounder row whose group key cannot give S is left out of the result, and so is
    one whose arithmetic raises (a delay_ns of None, say); one line on stderr, per call, names
    how many and which keys."""
    out, lost = [], {}
    for r in rows:
        try:
            d = sweep_start_delay(r)
        except (OverflowError, ZeroDivisionError, ValueError, TypeError):
            d = None            # a second guard: one bad row must never abort the read
        if d is None:
            gk = r.get("group_key")
            lost[gk] = lost.get(gk, 0) + 1
            continue
        out.append(dict(r, delay_ns=d))
    if lost:
        print("%s: %d codar-sounder arrivals before %s left out: their group key gives no sweep "
              "duration (%s)" % (who, sum(lost.values()), ".".join(map(str, FIXED_FROM)),
                                 ", ".join("%r" % k for k in sorted(lost, key=str))),
              file=sys.stderr)
    return out
