"""Set-aside windows: rows that must never feed a sigmondb product, kept in the database.

Why it exists (2026-11-18): codar-sounder dropped sample chunks and so shifted every later
CPI label (fixed in codar-sounder 7a06713).  The CODAR arrivals, captures and ground-wave
solutions built on those labels stay wrong for some reporters up to 2026-11-18 02:09Z.
Nothing gets deleted.  A row in sigmondb.set_aside names a reporter and a half-open UTC
window [from_time, to_time); every reader of a product filters through the conditions below.

Withdrawing a window inserts the same key (scope, reporter_id, from_time) with active = 0
and a later set_at.  ReplacingMergeTree(set_at) keeps the latest row per key, and every
read goes through FINAL, so the latest set_at wins.

scope names the readers a window applies to: 'codar' (CODAR arrivals, captures and
solutions), 'hfdl' (HFDL readers) or 'all'.  A reader asks with its own scope, 'codar' or
'hfdl'; it then honours its own windows and the 'all' windows.

reporter_id matches the label stored in the row.  solve counts a carried arrival at its
radiod in memory (cli._at_radiod), so a solution names KFS-OMNI where the arrival table
says KFS/MAIN or KFS-MAIN: set aside both labels.

The set_aside table must exist.  Without it every filtered read fails with ClickHouse's
"Table sigmondb.set_aside does not exist" error; no reader passes a row it could not
check.  ensure_schema() creates the table (solve --from-db, attribute and load run it).

This module imports no ClickHouse code.
"""
import re
from datetime import datetime, timedelta, timezone

from sigmondb import ddl

SCOPES = ("codar", "hfdl", "all")
READER_SCOPES = ("codar", "hfdl")
TABLE = "%s.set_aside" % ddl.DATABASE

_IDENT = re.compile(r"^[A-Za-z_][A-Za-z0-9_.]*$")


def _ident(name):
    if not _IDENT.match(name):
        raise ValueError("not a column name: %r" % name)
    return name


def windows_sql(scope):
    """The scalar subquery: an array of (reporter_id, from_time, to_time), one per active
    window of `scope` or 'all'."""
    if scope not in READER_SCOPES:
        raise ValueError("reader scope must be one of %s, not %r" % (READER_SCOPES, scope))
    return ("(SELECT groupArray((reporter_id, from_time, to_time)) FROM %s FINAL "
            "WHERE active = 1 AND scope IN ('%s', 'all'))" % (TABLE, scope))


def not_set_aside(scope, reporter="reporter_id", time="time"):
    """WHERE fragment for a table with a reporter and a time column: true when no active
    window of `scope` (or 'all') holds the row, windows half-open [from_time, to_time)."""
    return ("NOT arrayExists(w -> w.1 = %s AND %s >= w.2 AND %s < w.3, %s)"
            % (_ident(reporter), _ident(time), _ident(time), windows_sql(scope)))


def solution_not_set_aside(scope, reporter_ids="reporter_ids", start="window_from",
                           end="window_to"):
    """WHERE fragment for sigmondb.solution: true when none of the solution's reporter_ids
    has an active window that overlaps [window_from, window_to].  A window [f, t) overlaps
    it when f <= window_to AND t > window_from."""
    return ("NOT arrayExists(w -> has(%s, w.1) AND w.2 <= %s AND w.3 > %s, %s)"
            % (_ident(reporter_ids), _ident(end), _ident(start), windows_sql(scope)))


# --- rows ---------------------------------------------------------------------------------

def parse_utc(text):
    """An ISO 8601 time as naive UTC.  A time with no zone counts as UTC; a zoned one is
    converted, never relabelled.  'Z' is accepted."""
    t = datetime.fromisoformat(text.strip().replace("Z", "+00:00").replace("z", "+00:00"))
    if t.tzinfo is not None:
        t = t.astimezone(timezone.utc).replace(tzinfo=None)
    return t.replace(microsecond=t.microsecond // 1000 * 1000)   # DateTime64(3) holds ms


def ch_time(t):
    """A naive UTC datetime as the string a {x:String} parameter carries into
    toDateTime64(x, 3, 'UTC'): no driver or server time zone can shift it."""
    return t.strftime("%Y-%m-%d %H:%M:%S.") + "%03d" % (t.microsecond // 1000)


def _t(name):
    return "toDateTime64({%s:String}, 3, 'UTC')" % name


def window_row(scope, reporter_id, from_time, to_time, reason, set_by, set_at, active=1):
    """One sigmondb.set_aside row.  Raises ValueError on an unknown scope, an empty reporter,
    reason or set_by, or a window whose end does not follow its start."""
    if scope not in SCOPES:
        raise ValueError("unknown scope %r; use one of %s" % (scope, ", ".join(SCOPES)))
    if not (reporter_id or "").strip():
        raise ValueError("empty reporter")
    if not (reason or "").strip():
        raise ValueError("empty reason")
    if not (set_by or "").strip():
        raise ValueError("empty set_by")
    if to_time <= from_time:
        raise ValueError("to (%s) must follow from (%s)" % (to_time, from_time))
    return dict(scope=scope, reporter_id=reporter_id, from_time=from_time, to_time=to_time,
                reason=reason.strip(), set_by=set_by.strip(), set_at=set_at,
                active=1 if active else 0)


def withdrawal_row(current, set_by, now):
    """The row that withdraws `current`: the same key, active = 0, set_at one second past
    current's when `now` does not already follow it (set_at counts whole seconds, and a tie
    would leave the winner to merge order)."""
    last = current["set_at"].replace(microsecond=0)
    set_at = now.replace(microsecond=0)
    if set_at <= last:
        set_at = last + timedelta(seconds=1)
    return dict(current, set_by=set_by, set_at=set_at, active=0)


def list_where(all_rows):
    return ("" if all_rows else "WHERE active = 1 ") + "ORDER BY scope, reporter_id, from_time"


def key_where():
    """Parameters scope, rid and from_time (ch_time)."""
    return ("WHERE scope = {scope:String} AND reporter_id = {rid:String} "
            "AND from_time = " + _t("from_time"))


# Read-only counts for add --dry-run.  The arrival count takes the scope's own rows:
# CODAR arrivals are chirped (rate_hz_per_s != 0), HFDL ones are not.
_KIND = {"codar": " AND rate_hz_per_s != 0", "hfdl": " AND rate_hz_per_s = 0", "all": ""}


def dry_run_queries(scope):
    """(arrival count SQL, solution count SQL or None) for one window, parameters rid, f, t
    (f and t through ch_time).
    Solutions come from CODAR arrivals only, so an 'hfdl' window sets aside none."""
    arr = ("SELECT count() FROM %s.arrival FINAL WHERE reporter_id = {rid:String} "
           "AND time >= %s AND time < %s%s" % (ddl.DATABASE, _t("f"), _t("t"), _KIND[scope]))
    sol = None
    if scope in ("codar", "all"):
        sol = ("SELECT count() FROM %s.solution FINAL WHERE has(reporter_ids, {rid:String}) "
               "AND window_to >= %s AND window_from < %s" % (ddl.DATABASE, _t("f"), _t("t")))
    return arr, sol
