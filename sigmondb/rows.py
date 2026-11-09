"""JSON Lines row files for sigmondb.

One JSON object per line.  Datetimes (naive UTC, microsecond resolution, which covers every
back-fill reader) are written as ISO-8601 strings ending in Z and converted back on read.
Integers stay integers: Python's json keeps arbitrary-size ints exact.  Only keys that are
datetime columns in some table are converted back, so a String column never changes type.
"""
import json
from datetime import datetime, timezone
from pathlib import Path

from sigmondb import ddl

DATETIME_KEYS = frozenset(
    n for s in ddl._SPEC.values() for n, t in s["cols"] if t.startswith("DateTime"))


def _enc(o):
    if isinstance(o, datetime):
        if o.tzinfo is not None:
            o = o.astimezone(timezone.utc).replace(tzinfo=None)
        return o.isoformat(timespec="microseconds") + "Z"
    raise TypeError("cannot serialise %r" % type(o))


def _dec(row):
    for k in DATETIME_KEYS:
        v = row.get(k)
        if isinstance(v, str) and v.endswith("Z"):
            row[k] = datetime.fromisoformat(v[:-1])
    return row


def write_rows(path, rows):
    n = 0
    with Path(path).open("w", encoding="utf-8") as f:
        for r in rows:
            f.write(json.dumps(r, default=_enc, ensure_ascii=False, allow_nan=True) + "\n")
            n += 1
    return n


def iter_rows(path):
    with Path(path).open(encoding="utf-8") as f:
        for line in f:
            if line.strip():
                yield _dec(json.loads(line))


def read_rows(path):
    return list(iter_rows(path))
