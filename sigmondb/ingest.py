"""Turn one `chirp/<site>/<receiver>/<table>.jsonl` line into a sigmondb row tuple.

Station tars carry the lines.  The ingester on wd30 calls chirp_row() per line and
inserts the tuples with insert_sql().  received_at comes from the tar, never from the
line, so no station clock reaches that column.  Datetimes become naive UTC.
"""
import json
import logging
import math
import re
from datetime import datetime, timezone
from pathlib import Path

from sigmondb import SCHEMA_VERSION, ddl
from sigmondb.load import whitelist
from sigmondb.rows import DATETIME_KEYS

log = logging.getLogger(__name__)

CHIRP_TABLES = ("arrival", "capture")


def _naive_utc(v):
    if isinstance(v, str):
        v = datetime.fromisoformat(v[:-1] + "+00:00" if v.endswith("Z") else v)
    if not isinstance(v, datetime):
        raise ValueError("not a datetime: %r" % (v,))
    if v.tzinfo is not None:
        v = v.astimezone(timezone.utc).replace(tzinfo=None)
    return v


def _callsign(reporter):
    return re.split(r"[/=]", reporter, maxsplit=1)[0].strip().upper()


_INT = re.compile(r"U?Int(8|16|32|64)$")


def _check(typ, v):
    """Coerce v to the Python value ClickHouse takes for typ; ValueError on a mismatch."""
    if ddl.is_nullable(typ):
        if v is None:
            return None
        typ = typ[len("Nullable("):-1]
    m = _INT.match(typ)
    if m:
        if isinstance(v, bool):
            raise ValueError("bool where %s expected" % typ)
        if isinstance(v, float):
            if not v.is_integer():
                raise ValueError("non-integral %r for %s" % (v, typ))
            v = int(v)
        elif isinstance(v, str):
            v = int(v)  # ValueError on junk
        elif not isinstance(v, int):
            raise ValueError("%r is not an integer" % (v,))
        bits = int(m.group(1))
        lo, hi = (0, 2 ** bits - 1) if typ[0] == "U" else (-2 ** (bits - 1), 2 ** (bits - 1) - 1)
        if not lo <= v <= hi:
            raise ValueError("%d out of range for %s" % (v, typ))
        return v
    if typ.startswith("Float"):
        if isinstance(v, bool):
            raise ValueError("bool where %s expected" % typ)
        v = float(v)
        if not math.isfinite(v):
            raise ValueError("non-finite %r" % v)
        return v
    if ddl.is_string(typ):
        if not isinstance(v, str):
            raise ValueError("%r is not a string" % (v,))
        return v
    return v


def chirp_row(table, row, *, received_at, fallback_reporter, warned=None):
    """Values in ddl.COLUMNS[table] order.  Raises ValueError on an unusable line.

    The reporter_id callsign must match fallback_reporter's (the upload site); the
    suffix after '/' or '=' may differ.  `warned` is a set shared across lines so an
    unknown key logs once.
    """
    if table not in CHIRP_TABLES:
        raise ValueError("not a chirp table: %r" % (table,))
    if not isinstance(row, dict):
        raise ValueError("line is not a JSON object")
    if row.get("schema_version") != SCHEMA_VERSION:
        raise ValueError("schema_version %r, want %d" % (row.get("schema_version"), SCHEMA_VERSION))
    row = whitelist(table, row, warned)
    row["received_at"] = received_at
    if not row.get("reporter_id"):
        row["reporter_id"] = fallback_reporter
    elif not isinstance(row["reporter_id"], str) or (
            _callsign(row["reporter_id"]) != _callsign(fallback_reporter)):
        raise ValueError("reporter_id %s does not match the upload site %s"
                         % (row["reporter_id"], fallback_reporter))
    out = []
    for c, typ in ddl.COLUMN_TYPES[table].items():
        v = row.get(c)
        try:
            if c == "engineering":
                v = ({str(k): "" if x is None else str(x) for k, x in v.items()}
                     if isinstance(v, dict) else {})
            elif c == "expected":
                v = ({str(k): float(x) for k, x in v.items()} if isinstance(v, dict) else {})
            elif c in DATETIME_KEYS and v is not None:
                v = _naive_utc(v)
        except (TypeError, ValueError, AttributeError) as e:
            raise ValueError("%s: %s" % (c, e)) from e
        if v is None and not ddl.is_nullable(typ):
            if ddl.is_string(typ):
                v = ""
            else:
                raise ValueError("%s is None" % c)
        if v is not None and c not in ("engineering", "expected") and c not in DATETIME_KEYS:
            try:
                v = _check(typ, v)
            except (TypeError, ValueError) as e:
                raise ValueError("%s: %s" % (c, e)) from e
        out.append(v)
    return tuple(out)


def read_chirp_file(path, table, *, received_at, fallback_reporter):
    """(rows, n_bad).  A bad line costs that line; one WARNING per file summarises."""
    rows, n_bad, first, warned = [], 0, None, set()
    with Path(path).open("rb") as f:
        for n, raw in enumerate(f, 1):
            if not raw.strip():
                continue
            try:
                rows.append(chirp_row(table, json.loads(raw.decode("utf-8")),
                                      received_at=received_at,
                                      fallback_reporter=fallback_reporter, warned=warned))
            except ValueError as e:  # JSONDecodeError is a ValueError
                n_bad += 1
                if first is None:
                    first = "line %d: %s" % (n, e)
    if n_bad:
        log.warning("%s: %d bad line(s); first: %s", path, n_bad, first)
    return rows, n_bad


def insert_sql(table):
    return "INSERT INTO %s.%s (%s) VALUES" % (ddl.DATABASE, table, ", ".join(ddl.COLUMNS[table]))
