"""Turn one `chirp/<site>/<receiver>/<table>.jsonl` line into a sigmondb row tuple.

Station tars carry the lines.  The ingester on wd30 calls chirp_row() per line and
inserts the tuples with insert_sql().  received_at comes from the tar, never from the
line, so no station clock reaches that column.  Datetimes become naive UTC.
"""
import json
import logging
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


def chirp_row(table, row, *, received_at, fallback_reporter):
    """Values in ddl.COLUMNS[table] order.  Raises ValueError on an unusable line."""
    if table not in CHIRP_TABLES:
        raise ValueError("not a chirp table: %r" % (table,))
    if not isinstance(row, dict):
        raise ValueError("line is not a JSON object")
    if row.get("schema_version") != SCHEMA_VERSION:
        raise ValueError("schema_version %r, want %d" % (row.get("schema_version"), SCHEMA_VERSION))
    row = whitelist(table, row)
    row["received_at"] = received_at
    if not row.get("reporter_id"):
        row["reporter_id"] = fallback_reporter
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
        out.append(v)
    return tuple(out)


def read_chirp_file(path, table, *, received_at, fallback_reporter):
    """(rows, n_bad).  A bad line costs that line; one WARNING per file summarises."""
    rows, n_bad, first = [], 0, None
    with Path(path).open(encoding="utf-8") as f:
        for n, line in enumerate(f, 1):
            if not line.strip():
                continue
            try:
                rows.append(chirp_row(table, json.loads(line), received_at=received_at,
                                      fallback_reporter=fallback_reporter))
            except ValueError as e:  # JSONDecodeError is a ValueError
                n_bad += 1
                if first is None:
                    first = "line %d: %s" % (n, e)
    if n_bad:
        log.warning("%s: %d bad line(s); first: %s", path, n_bad, first)
    return rows, n_bad


def insert_sql(table):
    return "INSERT INTO %s.%s (%s) VALUES" % (ddl.DATABASE, table, ", ".join(ddl.COLUMNS[table]))
