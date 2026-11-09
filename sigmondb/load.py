"""Insert sigmondb JSON Lines files into ClickHouse.

Unknown keys are dropped with one warning per key per call; they never fail a batch.
Datetimes go to the client as tz-aware UTC, so clickhouse-connect never guesses a zone.
Two loads of one file send equal tuples; ReplacingMergeTree collapses them on its key.
"""
import logging
from datetime import timezone

from sigmondb import ddl
from sigmondb.rows import iter_rows

log = logging.getLogger(__name__)


def whitelist(table, row, _warned=None):
    cols = ddl.COLUMNS[table]
    warned = set() if _warned is None else _warned
    out = {}
    for k, v in row.items():
        if k in cols:
            out[k] = v
        elif k not in warned:
            warned.add(k)
            log.warning("sigmondb.%s: dropping unknown key %r", table, k)
    return out


def _utc(v):
    if hasattr(v, "tzinfo") and v.tzinfo is None:
        return v.replace(tzinfo=timezone.utc)
    return v


def insert_file(client, table, path, batch=50_000):
    cols = list(ddl.COLUMNS[table])
    target = "%s.%s" % (ddl.DATABASE, table)
    warned, buf, total = set(), [], 0

    def flush():
        nonlocal buf, total
        if buf:
            client.insert(target, buf, column_names=cols)
            total += len(buf)
            buf = []

    for row in iter_rows(path):
        row = whitelist(table, row, warned)
        buf.append(tuple(_utc(row.get(c)) for c in cols))
        if len(buf) >= batch:
            flush()
    flush()
    return total
