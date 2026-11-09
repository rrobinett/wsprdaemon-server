"""Insert sigmondb JSON Lines files into ClickHouse.

Unknown keys are dropped with one warning per key per call; they never fail a batch.
Datetimes go to the client as tz-aware UTC, so clickhouse-connect never guesses a zone.
None in a non-Nullable String column goes as ''.  None in any other non-Nullable
column raises ValueError before the first insert; clickhouse-connect would reject it.
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


def _fillers(table):
    """Per column: (name, fill) where fill is '' for a non-Nullable String, else None;
    plus the columns that admit no None at all."""
    fill, strict = {}, []
    for c, typ in ddl.COLUMN_TYPES[table].items():
        if ddl.is_nullable(typ):
            continue
        if ddl.is_string(typ):
            fill[c] = ""
        else:
            strict.append(c)
    return fill, strict


def insert_file(client, table, path, batch=50_000):
    cols = list(ddl.COLUMNS[table])
    fill, strict = _fillers(table)
    fill_at = [(i, fill[c]) for i, c in enumerate(cols) if c in fill]
    # Check the whole file first so an error leaves nothing half-sent.
    bad = {}
    for row in iter_rows(path):
        for c in strict:
            if row.get(c) is None:
                bad[c] = bad.get(c, 0) + 1
    if bad:
        raise ValueError("sigmondb.%s: None in non-Nullable column(s): %s" % (
            table, ", ".join("%s (%d rows)" % kv for kv in bad.items())))
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
        t = [_utc(row.get(c)) for c in cols]
        for i, f in fill_at:
            if t[i] is None:
                t[i] = f
        buf.append(tuple(t))
        if len(buf) >= batch:
            flush()
    flush()
    return total
