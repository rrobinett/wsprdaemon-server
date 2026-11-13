"""DDL for the sigmondb registry.

Spec:  /home/mjh/hamsci/tasks/chirp-registry/2026-11-09-chirp-registry-design.md
       (Tables section).
Plan:  .superpowers/sdd/2026-11-09-sigmondb-first-build/ (Task 1).
Mirror: schema/sigmondb.sql holds the same statements; a test keeps them equal.
        Regenerate it with `python3 -m sigmondb.ddl > schema/sigmondb.sql`.

COLUMNS is the contract for row readers and the loader: the insertable
columns per table, in DDL order, without `ingested_at`.

This module imports no ClickHouse code.  ensure_schema() takes a client
object with a `command(sql)` method.
"""
from sigmondb import SCHEMA_VERSION  # noqa: F401  (re-exported for callers)

DATABASE = "sigmondb"

_Z = "ZSTD(3)"

# attribution.status: named, named_two_stations, ambiguous, unattributable.
# attribution.offset_from: ground_wave, skywave_model, ''.


def _codec(typ):
    if typ.startswith("DateTime64"):
        return "CODEC(Delta(8), %s)" % _Z
    if typ.startswith("DateTime"):
        return "CODEC(Delta(4), %s)" % _Z
    if typ.startswith(("Map", "Array")):
        return ""
    return "CODEC(%s)" % _Z


def _cols(pairs):
    return [(n, t) for n, t in pairs]


LC = "LowCardinality(String)"
NF64 = "Nullable(Float64)"
NF32 = "Nullable(Float32)"

# Provenance block, on capture and arrival.
PROVENANCE = [
    ("u_epoch_ns", NF64),
    ("k", NF32),
    ("p", NF32),
    ("a_level", LC),
    ("a_level_provenance", LC),
    ("counter_epoch_id", "String"),
    ("origin", LC),
]
ENGINEERING = [("engineering", "Map(String, String)")]
# When the ingester received the line, from the upload tar; never a station clock value.
RECEIVED = [("received_at", "Nullable(DateTime)")]

_SPEC = {
    "transmitter": dict(
        cols=[
            ("tx_id", "String"), ("kind", LC), ("network", LC), ("name", "String"),
            ("status", LC), ("lat", NF64), ("lon", NF64), ("pos_source", LC),
            ("pos_sigma_m", NF32), ("valid_from", "DateTime"), ("version", "UInt32"),
            ("source", "String"), ("note", "String"),
        ],
        part="toYYYYMM(valid_from)",
        order="tx_id, valid_from, version",
    ),
    "waveform": dict(
        cols=[
            ("tx_id", "String"), ("valid_from", "DateTime"),
            ("centre_hz", "Float64"), ("rate_hz_per_s", "Float64"),
            ("period_s", NF64), ("duration_s", NF64), ("bandwidth_hz", NF64),
            ("gate_period_s", NF64), ("gate_duty", NF64), ("gate_phase_s", NF64),
            ("frame_s", NF64), ("keying_step_ns", NF64), ("keying_modulus_ns", NF64),
            ("source", "String"), ("measured_by", "String"),
        ],
        part=None,
        order="tx_id, centre_hz, valid_from",
    ),
    "receiver": dict(
        cols=[
            ("reporter_id", "String"), ("host_id", "String"), ("radiod_id", "String"),
            ("lat", NF64), ("lon", NF64), ("alt_m", NF64), ("pos_source", LC),
            ("pos_sigma_m", NF32), ("antenna", "String"), ("receiver_chain", "String"),
            ("coherence_group", "String"), ("a_level", LC),
            ("valid_from", "DateTime"), ("version", "UInt32"),
        ],
        part=None,
        order="reporter_id, valid_from, version",
    ),
    "capture": dict(
        cols=[
            ("time", "DateTime64(3)"), ("reporter_id", "String"),
            ("centre_hz", "Float64"), ("rate_hz_per_s", "Float64"),
            ("duration_s", "Float32"), ("n_sweeps", "Nullable(UInt32)"),
            ("expected", "Map(String, Float32)"),
            ("null_max_db", NF32), ("null_median_db", NF32),
            ("n_expected", "Nullable(UInt32)"), ("n_found", "Nullable(UInt32)"),
            ("detector_version", LC), ("ring_ref", "String"),
            ("schema_version", "UInt16"),
        ] + PROVENANCE + ENGINEERING + RECEIVED,
        part="toYYYYMM(time)",
        order="reporter_id, centre_hz, rate_hz_per_s, time, detector_version",
    ),
    "arrival": dict(
        cols=[
            ("time", "DateTime64(9)"), ("reporter_id", "String"), ("tx_id", "String"),
            ("group_key", LC), ("centre_hz", "Float64"), ("rate_hz_per_s", "Float64"),
            ("delay_ns", "Int64"), ("delay_sigma_ns", NF32), ("freq_hz", NF64),
            ("level_db", "Float32"), ("margin_db", NF32), ("half_band_db", NF32),
            ("clean_tag", LC),
            ("doppler_hz", NF32), ("phase_rate", NF32), ("carrier_phase_rad", NF32),
            ("s4", NF32), ("sigma_phi_rad", NF32),
            ("detector_version", LC), ("schema_version", "UInt16"),
        ] + PROVENANCE + ENGINEERING + RECEIVED,
        part="toYYYYMM(time)",
        order=("reporter_id, centre_hz, rate_hz_per_s, time, detector_version, "
               "intDiv(delay_ns, 1000)"),
    ),
    "solution": dict(
        cols=[
            ("tx_id", "String"), ("cluster_key", "String"), ("solve_id", "String"),
            ("window_from", "DateTime64(3)"), ("window_to", "DateTime64(3)"),
            ("reporter_ids", "Array(String)"),
            ("fixed_delay_ns", "Int64"), ("delay_sd_ns", "Float64"),
            ("offset_ns", "Nullable(Int64)"), ("offset_sigma_ns", NF64),
            ("lat", NF64), ("lon", NF64), ("pos_sigma_m", NF32),
            ("method", LC), ("n_stations", "UInt16"), ("n_arrivals", "UInt32"),
            ("set_aside", "Map(String, String)"),
            ("solver_version", LC), ("inputs_digest", "FixedString(64)"),
        ],
        part="toYYYYMM(window_from)",
        order="cluster_key, solve_id",
    ),
    "attribution": dict(
        cols=[
            ("run_at", "DateTime"), ("run_id", "String"),
            ("window_from", "DateTime64(3)"), ("window_to", "DateTime64(3)"),
            ("group_key", LC), ("period_ns", "Int64"), ("mode_policy", LC),
            ("tx_id", "String"), ("alias_tx_ids", "Array(String)"),
            ("status", LC), ("n_stations", "UInt16"),
            ("reporter_ids", "Array(String)"), ("modes", "Array(String)"),
            ("cluster_delays_ns", "Array(Int64)"), ("cluster_mad_ns", "Array(Float64)"),
            ("offset_ns", "Nullable(Int64)"), ("offset_from", LC),
            ("spread_ns", NF64), ("p_null", NF64), ("n_fake", "UInt32"),
            ("runner_up_tx_id", "String"), ("runner_up_spread_ns", NF64),
            ("runner_up_fits", "UInt8"), ("flags", "Array(String)"),
            ("set_aside", "Map(String, String)"),
            ("solver_version", LC), ("inputs_digest", "FixedString(64)"),
        ],
        part="toYYYYMM(run_at)",
        order="group_key, mode_policy, tx_id, run_id",
    ),
    "path": dict(
        cols=[
            ("time", "DateTime"), ("tx_id", "String"), ("reporter_id", "String"),
            ("excess_delay_ns", NF64), ("mode", LC),
            ("virtual_height_km", NF32), ("solver_version", LC),
        ],
        part="toYYYYMM(time)",
        order="tx_id, reporter_id, time",
    ),
}


def _create_table(name, cols, part, order, engine=None):
    lines = ["    `%s` %s %s" % (n, t, _codec(t)) for n, t in cols]
    lines = [ln.rstrip() for ln in lines]
    lines.append("    `ingested_at` DateTime DEFAULT now() CODEC(Delta(4), %s)" % _Z)
    sql = "CREATE TABLE IF NOT EXISTS %s.%s\n(\n%s\n)\n" % (
        DATABASE, name, ",\n".join(lines))
    sql += "ENGINE = %s\n" % (engine or "ReplacingMergeTree(ingested_at)")
    if part:
        sql += "PARTITION BY %s\n" % part
    sql += "ORDER BY (%s)" % order
    return sql


TABLES = {}
COLUMNS = {}
COLUMN_TYPES = {}  # table -> {column: ClickHouse type}, insertable columns only
TABLE_ORDER = tuple(_SPEC)  # the insert order: every row table, in DDL order
for _name, _s in _SPEC.items():
    COLUMN_TYPES[_name] = dict(_s["cols"])
    TABLES[_name] = _create_table(_name, _s["cols"], _s["part"], _s["order"])
    COLUMNS[_name] = tuple(n for n, _ in _s["cols"])

# heard: last arrival time and level per (receiver, transmitter, group).
# The target table keeps aggregate states; the materialized view feeds it
# from arrival.  Read with maxMerge / argMaxMerge.
TABLES["heard_state"] = """CREATE TABLE IF NOT EXISTS sigmondb.heard_state
(
    `reporter_id` String,
    `tx_id` String,
    `group_key` LowCardinality(String),
    `last_time` AggregateFunction(max, DateTime64(9)),
    `last_level_db` AggregateFunction(argMax, Float32, DateTime64(9))
)
ENGINE = AggregatingMergeTree
ORDER BY (reporter_id, tx_id, group_key)"""

TABLES["heard"] = """CREATE MATERIALIZED VIEW IF NOT EXISTS sigmondb.heard
TO sigmondb.heard_state
AS SELECT
    reporter_id,
    tx_id,
    group_key,
    maxState(time) AS last_time,
    argMaxState(level_db, time) AS last_level_db
FROM sigmondb.arrival
GROUP BY reporter_id, tx_id, group_key"""

COLUMNS["heard_state"] = ("reporter_id", "tx_id", "group_key", "last_time", "last_level_db")


# Adds columns to tables created before they existed.  Run after ddl_statements().
# freq_hz (2026-11-12): the frequency an arrival's delay belongs to -- the centre
# for a narrow sweep, the reflection's instantaneous frequency for a wideband
# one (an ionogram trace), so the tables serve any chirp, not CODAR alone.
MIGRATIONS = tuple(
    "ALTER TABLE sigmondb.%s ADD COLUMN IF NOT EXISTS received_at Nullable(DateTime) "
    "CODEC(ZSTD(3)) AFTER engineering" % t
    for t in ("capture", "arrival")) + (
    "ALTER TABLE sigmondb.arrival ADD COLUMN IF NOT EXISTS freq_hz Nullable(Float64) "
    "CODEC(ZSTD(3)) AFTER delay_sigma_ns",)


def is_nullable(typ):
    return typ.startswith("Nullable(")


def is_string(typ):
    """True for String and LowCardinality(String); false for Nullable and the rest."""
    return typ in ("String", LC)


def ddl_statements():
    """CREATE DATABASE first, then each table in TABLES order (heard last)."""
    return ["CREATE DATABASE IF NOT EXISTS %s" % DATABASE] + list(TABLES.values())


def ensure_schema(client):
    """Create the database and every table.  Idempotent."""
    for stmt in ddl_statements() + list(MIGRATIONS):
        client.command(stmt)


_HEADER = """-- sigmondb: registry of chirp (CODAR) and HFDL receptions.
--
-- Provenance: /home/mjh/hamsci/tasks/chirp-registry/2026-11-09-chirp-registry-design.md
-- (Tables section); plan 2026-11-09-sigmondb-first-build, Task 1.
-- Mirrors sigmondb/ddl.py; tests/test_sigmondb_ddl.py keeps the two equal.
-- Regenerate: python3 -m sigmondb.ddl > schema/sigmondb.sql
--
-- Dedup keys: ReplacingMergeTree(ingested_at) collapses a re-ingested row
-- onto its key.  arrival keys on delay to the microsecond (intDiv(delay_ns, 1000)),
-- so a re-run of the detector at the same version replaces rather than doubles
-- only while each delay lands in the same 1 us bin; a delay that moves across a
-- bin edge between runs leaves two rows.
-- `heard` is a materialized view into heard_state; read it with
-- maxMerge(last_time) and argMaxMerge(last_level_db).
"""


def sql_text():
    return _HEADER + "\n" + ";\n\n".join(ddl_statements() + list(MIGRATIONS)) + ";\n"


if __name__ == "__main__":
    print(sql_text(), end="")
