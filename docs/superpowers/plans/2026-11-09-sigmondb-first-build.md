# sigmondb First Build Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Stand up the `sigmondb` registry on wd30: its tables, the transmitter and receiver
identity, a back-fill of the CODAR and HFDL receptions we already hold, and the ground-wave
solver, and prove it reproduces the 11-07 fixed-path finding.

**Architecture:** A new `sigmondb/` package inside wsprdaemon-server.  Pure-Python readers turn
source files into row dicts (JSON Lines on disk between steps).  One loader inserts row files
into ClickHouse with a column whitelist.  The solver reads arrival rows and writes solution rows.
The DDL lives once in `sigmondb/ddl.py`, with `schema/sigmondb.sql` as its checked mirror, in the
house style of `schema/station.heartbeat.sql`.  The upload path, the parser on the public hosts,
the signed file and the joint fit are a later stage and are out of scope here.

**Tech Stack:** Python 3.11+ (stdlib `tomllib`, `csv`, `json`), numpy, clickhouse-connect 2.x;
ClickHouse 25.11 on wd30; pytest.

**Spec:** `/home/mjh/hamsci/tasks/chirp-registry/2026-11-09-chirp-registry-design.md` (approved by
Michael 2026-11-09), plus step 3 of
`/home/mjh/hamsci/tasks/hfdl-timing/2026-11-09-hfdl-registry-design.md` (HFDL transmitters,
signals and arrivals).

## Global Constraints

- Database name: `sigmondb`.  Tables: `transmitter`, `waveform`, `receiver`, `capture`, `arrival`,
  `solution`, `path`; materialized view `heard`.
- House style: `ReplacingMergeTree(ingested_at)`, `PARTITION BY toYYYYMM(<time column>)`, ORDER BY
  = dedup key, `Delta`/`T64` + `ZSTD(3)` codecs, `LowCardinality(String)` for repeated strings,
  `ingested_at DateTime DEFAULT now()`, a provenance header comment in the `.sql` file.
- Dedup keys (spec, "Calls made after the builder check"):
  - `arrival`: `(reporter_id, centre_hz, rate_hz_per_s, time, detector_version, intDiv(delay_ns, 1000))`
  - `capture`: `(reporter_id, centre_hz, rate_hz_per_s, time, detector_version)`
- `delay_ns` = the arrival's phase after the sweep (or frame) start, modulo the period, as Int64 ns.
- Timing provenance columns on `capture` and `arrival`: `u_epoch_ns` Nullable(Float64), `k`
  Nullable(Float32), `p` Nullable(Float32), `a_level` LowCardinality(String),
  `a_level_provenance` LowCardinality(String), `counter_epoch_id` String, `origin`
  LowCardinality(String).  The tier shorthand goes only into `engineering` Map(String, String).
- Offsets feed only from rows with `u_epoch_ns` ≤ 1000 at `k` = 2.
- Back-fill provenance: `u_epoch_ns = 230`, `k = 2`, `a_level_provenance = 'assumed'`,
  `counter_epoch_id` from the minute row's `t3.counter_epoch_id`, `origin = 't6_anchor'` when the
  capture had a T6 anchor; all provenance columns null/empty otherwise (UACNJ on 11-08).
- `detector_version` for every back-filled CODAR row: `offline-2026-11`; for HFDL rows:
  `hfdl-analyzer-<analyzer_version>` from the CSV.
- `tx_id` forms: `hfrnet:<Station>`, `hfdl:<gs_id>`, `giro:<ursi>`, `cand:<yyyymmdd>-<n>`.  An
  arrival whose source no one has named carries `tx_id = ''`.
- Identity is declared, never inferred: no code merges two transmitters or two receivers.
- Nothing here touches a station, gw1/gw2, wd10 or wd20.  wd30 only.  No push; Michael pushes.
- Tests never need ClickHouse: stub `clickhouse_connect` as `tests/test_status_ingest.py` does,
  and give the loader a fake client.

## Review Focus

1. A tab-separated arrival file holds clock times without dates, and the 11-07 night crosses
   midnight: each row must take its date from the capture file it came from, never from the
   first file's date.  Test in Task 3.
2. Loading the same row file twice must leave every table's `count() FINAL` unchanged, including
   when `delay_ns` came from a float in ms.  Rows carry integer ns, rounded once, at read time.
   Test in Task 5.
3. A capture with no T6 anchor still produces capture and arrival rows, with null provenance; the
   solver must never use them.  Tests in Tasks 3 and 6.
4. HFRNet rows with odd values (ANGL at 41.34 MHz with a −600 kHz sweep, blank coordinates) must
   become rows or be skipped with one logged reason each, never abort the seed.  Test in Task 2.
5. HFDL squitters marked not found, or excluded, count in the capture summary but write no
   arrival.  Test in Task 4.

---

## File Structure

| Path (in wsprdaemon-server) | Responsibility |
|---|---|
| `schema/sigmondb.sql` | The DDL mirror, with provenance header |
| `sigmondb/__init__.py` | Package marker, `SCHEMA_VERSION = 1` |
| `sigmondb/ddl.py` | DDL strings and `ensure_schema` |
| `sigmondb/hfrnet.py` | HFRNet CSV → transmitter + waveform rows |
| `sigmondb/hfdl_gs.py` | dumphfdl `systable.conf` → HFDL transmitter + waveform rows |
| `sigmondb/seed/receivers.toml` | The back-fill's receivers, typed by an analyst |
| `sigmondb/receivers.py` | receivers.toml → receiver rows (Maidenhead grid → lat/lon) |
| `sigmondb/provenance.py` | Back-fill provenance from t3_shadow minute rows |
| `sigmondb/backfill_codar.py` | 11-04, 11-07 and 11-08 CODAR sets → capture + arrival rows |
| `sigmondb/backfill_hfdl.py` | `squitter-part1.csv` → capture + arrival rows |
| `sigmondb/rows.py` | JSON Lines read/write; column whitelist per table |
| `sigmondb/load.py` | Insert row files into ClickHouse |
| `sigmondb/groundwave.py` | Fixed-path clusters and ground-wave offsets |
| `sigmondb/cli.py`, `sigmondb/__main__.py` | `python -m sigmondb <verb>` |
| `tests/test_sigmondb_*.py` | One test file per module |

---

### Task 1: The tables

**Files:**
- Create: `sigmondb/__init__.py`, `sigmondb/ddl.py`, `schema/sigmondb.sql`
- Test: `tests/test_sigmondb_ddl.py`

**Interfaces:**
- Produces: `DATABASE = "sigmondb"`; `TABLES: dict[str, str]` (table name → CREATE statement, in
  creation order, `heard` last); `COLUMNS: dict[str, tuple[str, ...]]` (insertable columns per
  table, excluding `ingested_at`); `ddl_statements() -> list[str]` (CREATE DATABASE first);
  `ensure_schema(client) -> None` (runs each statement with `client.command`).

Columns, beyond the provenance block and `ingested_at` on every measurement and product table:

- `transmitter`: `tx_id`, `kind` (codar, ionosonde, oth, hfdl, unknown), `network`, `name`, `status`
  (candidate, listed, heard, solved, retired), `lat`, `lon` Nullable(Float64), `pos_source`,
  `pos_sigma_m` Nullable(Float32), `valid_from` DateTime, `version` UInt32, `source`, `note`.
  ORDER BY `(tx_id, valid_from, version)`; partition by `toYYYYMM(valid_from)`.
- `waveform`: `tx_id`, `valid_from`, `centre_hz` Float64, `rate_hz_per_s` Float64, `period_s`,
  `duration_s`, `bandwidth_hz`, `gate_period_s`, `gate_duty`, `gate_phase_s` (Nullable Float64),
  HFDL: `frame_s` Nullable(Float64), `keying_step_ns`, `keying_modulus_ns` Nullable(Float64);
  `source`, `measured_by` String.  ORDER BY `(tx_id, centre_hz, valid_from)`.
- `receiver`: `reporter_id`, `host_id`, `radiod_id`, `lat`, `lon`, `alt_m` Nullable, `pos_source`,
  `pos_sigma_m`, `antenna`, `receiver_chain`, `coherence_group`, `a_level`, `valid_from`,
  `version`.  ORDER BY `(reporter_id, valid_from, version)`.
- `capture`: `time` DateTime64(3), `reporter_id`, `centre_hz`, `rate_hz_per_s`, `duration_s`,
  `n_sweeps` Nullable(UInt32), `expected` Map(String, Float32) (tx or group key → margin dB, NaN
  when not detected), `null_max_db`, `null_median_db` Nullable(Float32), `n_expected`,
  `n_found` Nullable(UInt32), `detector_version`, `ring_ref` String, `schema_version` UInt16,
  provenance, `engineering`.
- `arrival`: `time` DateTime64(9), `reporter_id`, `tx_id`, `group_key` LowCardinality(String),
  `centre_hz`, `rate_hz_per_s`, `delay_ns` Int64, `delay_sigma_ns` Nullable(Float32), `level_db`,
  `margin_db`, `half_band_db` Nullable(Float32), `clean_tag` LowCardinality(String), physics
  Nullable(Float32): `doppler_hz`, `phase_rate`, `carrier_phase_rad`, `s4`, `sigma_phi_rad`;
  `detector_version`, `schema_version`, provenance, `engineering`.
- `solution`: `tx_id`, `cluster_key` String, `solve_id` String, `window_from`, `window_to`
  DateTime64(3), `reporter_ids` Array(String), `fixed_delay_ns` Int64, `delay_sd_ns` Float64,
  `offset_ns` Nullable(Int64), `offset_sigma_ns` Nullable(Float64), `lat`, `lon`, `pos_sigma_m`
  Nullable, `method` LowCardinality(String), `n_stations` UInt16, `n_arrivals` UInt32,
  `set_aside` Map(String, String), `solver_version`, `inputs_digest` FixedString(64).
  ORDER BY `(cluster_key, solve_id)`; partition by `toYYYYMM(window_from)`.
- `path`: `time` DateTime, `tx_id`, `reporter_id`, `excess_delay_ns` Nullable(Float64), `mode`,
  `virtual_height_km` Nullable(Float32), `solver_version`.  Defined, not filled.
- `heard`: `AggregatingMergeTree` target plus materialized view from `arrival`, keyed
  `(reporter_id, tx_id, group_key)`, holding `maxState(time)` and `argMaxState(level_db, time)`.
  Form left to the builder.

- [ ] **Step 1: Write the failing tests.**
  `test_every_table_is_created_in_order` (`ddl_statements()[0]` starts with
  `CREATE DATABASE IF NOT EXISTS sigmondb`; the names follow `TABLES`' order; `heard` last).
  `test_dedup_keys_match_the_spec` (the ORDER BY clauses of `arrival` and `capture` contain the
  exact tuples in Global Constraints).
  `test_sql_file_mirrors_ddl` (the statements in `schema/sigmondb.sql`, split on `;` with comments
  stripped and whitespace collapsed, equal `ddl_statements()` under the same normalisation).
  `test_ensure_schema_runs_each_statement_once` (a fake client records `command` calls; count =
  `len(ddl_statements())`).
  `test_columns_whitelist_names_real_columns` (every name in `COLUMNS[t]` appears in `TABLES[t]`).
- [ ] **Step 2:** `python3 -m pytest tests/test_sigmondb_ddl.py -v` → FAIL (no module).
- [ ] **Step 3:** Write `ddl.py` and the `.sql` mirror.  Header comment cites the spec path and
  this plan.  `SCHEMA_VERSION = 1` in `__init__.py`.
- [ ] **Step 4:** Rerun → PASS.
- [ ] **Step 5:** Commit `sigmondb: tables, DDL mirror, ensure_schema`.

### Task 2: Transmitter and receiver identity

**Files:**
- Create: `sigmondb/hfrnet.py`, `sigmondb/hfdl_gs.py`, `sigmondb/receivers.py`,
  `sigmondb/seed/receivers.toml`
- Test: `tests/test_sigmondb_identity.py`, fixtures under `tests/fixtures/sigmondb/`

**Interfaces:**
- Consumes: `COLUMNS` (Task 1).
- Produces:
  - `parse_hfrnet(path: Path, snapshot: date, active_days: int = 14) -> tuple[list[dict], list[dict]]`
    (transmitter rows, waveform rows).
  - `parse_systable(path: Path, keying: dict[int, tuple[float, float]] = KEYING) -> tuple[list[dict], list[dict]]`.
  - `KEYING = {4: (8_408_600.0, 65_992_600.0), 2: (-408_200.0, 51_569_000.0)}` (Riverhead id 4,
    Molokai id 2: step and modulus in ns, from `hfdl-timing/keying/README.md`).
  - `load_receivers(path: Path) -> list[dict]`; `grid_to_latlon(grid: str) -> tuple[float, float]`
    (centre of the square, any even length 4-10).

Rules: HFRNet `Frequency` is MHz → `centre_hz`; `Bandwidth` is kHz (signed: negative sweeps down)
→ `bandwidth_hz = abs(bw)*1e3`; `period_s = 1/SweepRate`; `rate_hz_per_s = bw*1e3*SweepRate`;
`duration_s = period_s`; `status = 'listed'` when the last radial file lies within `active_days`
of `snapshot`, else `'retired'`; `source = 'hfrnet'`; `pos_source = 'hfrnet'`.  HFDL: one
transmitter per ground station (`kind = 'hfdl'`, `network = 'hfdl'`, `name` from the systable),
one waveform row per frequency (`centre_hz = kHz*1e3`, `rate_hz_per_s = 0`, `period_s = frame_s =
32.0`, keying columns from `KEYING`, null otherwise).  receivers.toml holds, per station:
`reporter_id`, `host_id`, `radiod_id`, `grid`, `antenna`, `receiver_chain`, `coherence_group`.
Seed it with AC0G-ND EN16ov, AC0G-B4 EM38ww, K3LR EN91se, W2NAF FN21ei, UACNJ FN20mv, WB6CXC-7
CN88ll (`pos_source = 'typed_grid'`; reporter and host ids from `ops/fleet.toml` heartbeat names).

- [ ] **Step 1: Write the failing tests** with a 4-row CSV fixture (ACI1, AGL1, ANGL, a row with
  blank Latitude) and a 2-station systable fixture copied from `ka9q-radio/aux/systable.conf`:
  - `test_hfrnet_up_sweep`: ACI1 → `centre_hz == 13_475_000.0`, `rate_hz_per_s ≈ 95_582.0`
    (±1), `period_s == 0.5`, `tx_id == 'hfrnet:ACI1'`.
  - `test_hfrnet_down_sweep`: AGL1 → `rate_hz_per_s ≈ -25_733.9`, `period_s == 1.0`.
  - `test_hfrnet_odd_rows_never_abort`: 3 transmitters + 3 waveforms out of 4 rows; the blank
    row is skipped and named in one WARNING (caplog).
  - `test_hfrnet_retired_by_age`: snapshot 2026-11-09, a row last radial 2026-10-01 → `'retired'`.
  - `test_systable_riverhead`: `tx_id == 'hfdl:4'`, `lat == 40.881922`, 7 waveform rows, the
    6661 kHz row has `keying_step_ns == 8_408_600.0` and `keying_modulus_ns == 65_992_600.0`.
  - `test_grid_centre`: `grid_to_latlon('EM38ww')` within 0.01° of `(38.9375, -92.125)`.
  - `test_receivers_seed_has_six`: six rows, unique `reporter_id`s, every lat/lon set.
- [ ] **Step 2:** Run → FAIL.  **Step 3:** Implement.  **Step 4:** Run → PASS.
- [ ] **Step 5:** Commit `sigmondb: transmitter and receiver identity from HFRNet, systable, seed`.

### Task 3: CODAR back-fill

**Files:**
- Create: `sigmondb/provenance.py`, `sigmondb/backfill_codar.py`
- Test: `tests/test_sigmondb_backfill_codar.py`, small fixtures cut from the real files

**Interfaces:**
- Consumes: `COLUMNS` (Task 1).
- Produces:
  - `load_minutes(paths: list[Path]) -> list[dict]`;
    `provenance_at(minutes: list[dict], utc: float, max_age_s: float = 120.0) -> dict` returning
    the provenance columns (Global Constraints) from the nearest minute row with a
    `t3_shadow.t6_anchor_utc_ns`, or the null block when none lies within `max_age_s`.
  - `read_t6_tsv(tsv: Path, capture_dir: Path, reporter_id: str, centre_hz: float,
    rate_hz_per_s: float, period_s: float, minutes: list[dict]) -> tuple[list[dict], list[dict]]`
    (captures, arrivals) for the 11-04 and 11-07 sets.
  - `read_res_jsonl(path: Path, reporter_id: str, minutes: list[dict]) -> tuple[list[dict], list[dict]]`
    for the 11-08 set.
  - `SETS: list[dict]` naming every input under `tasks/codar-t6/` with its reporter_id and group.

Rules:
- tsv rows (`time, label_minus_t6_ms, anchor_age_s, phase_t6_ms, phase_label_ms, tag, db`): one
  capture per distinct `time`; its date and exact start come from the capture file in
  `capture_dir` whose name stamp (`...-YYYYMMDDTHHMMSSZ.npy`) lies within 5 s of `time`.
- Arrival `time` = the UTC second the phase counts from (capture start's second, on T6);
  `delay_ns = round(phase_t6_ms * 1e6) % round(period_s * 1e9)`; `level_db = db`; `clean_tag` from
  `tag` (`A` arrival, `R` rung, `m`/`M` merged); `group_key = f"{centre_hz/1e6:.4f} {rate/1e3:+.2f}"`.
- res jsonl: one capture per row per group (`expected` = group name → `peak_db - null_max`,
  `null_max_db`, `null_median_db` from the group); one arrival per `arrivals[]` entry with
  `phase_t6_ms`; `half_band_db = halves[0] - halves[1]`; group name gives centre and rate
  (`"FAU 13.4499 -99.26"` → 13_449_900.0 Hz, −99_260.0 Hz/s).
- 4.463 MHz long-range group: `centre_hz = 4_463_000.0`, `rate_hz_per_s = -25_733.913`
  (`codar-mf/clean.py`'s `K`), `period_s = 1.0`.  Constants live in `backfill_codar.py`; the
  package never imports from `tasks/`.

- [ ] **Step 1: Write the failing tests:**
  - `test_midnight_rows_take_their_files_date`: a tsv with `23:59:51` and `00:10:03`, capture
    files `...20261107T235951Z.npy` and `...20261108T001003Z.npy` → arrival times on 11-07 and
    11-08.
  - `test_delay_is_integer_ns_mod_period`: `phase_t6_ms = 1000.25`, period 1 s →
    `delay_ns == 250_000`.
  - `test_no_anchor_gives_null_provenance`: minutes without `t6_anchor_utc_ns` → `u_epoch_ns is None`,
    `origin == ''`; arrival rows still emitted.
  - `test_anchor_gives_assumed_provenance`: → `u_epoch_ns == 230.0`, `k == 2.0`,
    `a_level_provenance == 'assumed'`, `counter_epoch_id == 'ep-1478032778'`.
  - `test_res_row_fills_capture_expected`: the K3LR 17:52:30 fixture row → 4 captures (one per
    group), FAU margin `≈ 14.01 - 9.69`, two FAU arrivals.
- [ ] **Step 2:** Run → FAIL.  **Step 3:** Implement.  **Step 4:** Run → PASS.
- [ ] **Step 5:** Commit `sigmondb: CODAR back-fill from the 11-04, 11-07 and 11-08 sets`.

### Task 4: HFDL back-fill

**Files:**
- Create: `sigmondb/backfill_hfdl.py`
- Test: `tests/test_sigmondb_backfill_hfdl.py`

**Interfaces:**
- Consumes: provenance column names (Task 3).
- Produces: `read_squitters(csv_path: Path, reporter_id: str) -> tuple[list[dict], list[dict]]`.

Rules: an arrival per row with `found` true and `excluded` empty: `tx_id = f"hfdl:{gs_id}"`,
`time` = `nominal_utc` (frame start), `delay_ns = round(delay_counter_s * 1e9)`, `centre_hz =
freq_khz*1e3`, `rate_hz_per_s = 0`, `level_db = snr_db`, `doppler_hz`, `clean_tag = 'squitter'`,
`detector_version = f"hfdl-analyzer-{analyzer_version}"`, `engineering` carries `frame_index`,
`flags`, `second_peak_ratio` and `t_level_active`.  Provenance: `u_epoch_ns = 230` etc. when
`t_level_active == 'T6'` and `counter_map` is present, else null.  A capture row per (gs_id,
freq_khz, UTC hour): `n_expected` = rows, `n_found` = arrivals written, `duration_s = 3600`.

- [ ] **Step 1: Write the failing tests:** `test_found_rows_become_arrivals`,
  `test_unfound_and_excluded_count_but_write_nothing` (5 rows, 2 not found, 1 excluded → 2
  arrivals, capture `n_expected == 5`, `n_found == 2`), `test_t5_rows_carry_null_provenance`.
- [ ] **Step 2:** Run → FAIL.  **Step 3:** Implement.  **Step 4:** Run → PASS.
- [ ] **Step 5:** Commit `sigmondb: HFDL back-fill from squitter-part1.csv`.

### Task 5: Row files and the loader

**Files:**
- Create: `sigmondb/rows.py`, `sigmondb/load.py`
- Test: `tests/test_sigmondb_load.py`

**Interfaces:**
- Consumes: `COLUMNS`, `DATABASE` (Task 1).
- Produces: `write_rows(path: Path, rows: list[dict]) -> int`; `read_rows(path: Path) -> list[dict]`;
  `whitelist(table: str, row: dict) -> dict` (drops unknown keys, one WARNING per unknown key per
  call); `insert_file(client, table: str, path: Path, batch: int = 50_000) -> int` (rows inserted).
  JSON Lines; datetimes as ISO-8601 strings with `Z`, converted back on read.

- [ ] **Step 1: Write the failing tests:** `test_round_trip_keeps_integer_ns`,
  `test_unknown_keys_dropped_not_fatal`, `test_insert_batches` (fake client; 120_001 rows at batch
  50_000 → 3 inserts), `test_second_load_sends_identical_rows` (two loads of one file send
  equal tuples, so the ReplacingMergeTree key collapses them).
- [ ] **Step 2:** Run → FAIL.  **Step 3:** Implement with `client.insert(f"{DATABASE}.{table}",
  data, column_names=...)`.  **Step 4:** Run → PASS.
- [ ] **Step 5:** Commit `sigmondb: JSON Lines rows and the whitelisting loader`.

### Task 6: The ground-wave solver and the CLI

**Files:**
- Create: `sigmondb/groundwave.py`, `sigmondb/cli.py`, `sigmondb/__main__.py`
- Test: `tests/test_sigmondb_groundwave.py`, `tests/test_sigmondb_cli.py`

**Interfaces:**
- Consumes: arrival rows (Tasks 3-4), transmitter and receiver rows (Task 2), `insert_file`
  (Task 5).
- Produces:
  - `fixed_clusters(arrivals: list[dict], gap_ns: int = 120_000, min_captures: int = 6,
    max_sd_ns: float = 20_000.0, night: tuple[float, float] = (22.5, 11.5),
    day: tuple[float, float] = (12.5, 21.5)) -> list[dict]`: per (reporter_id, group_key),
    cluster delays (sorted, split at gaps > `gap_ns`, keep clusters seen in ≥ `min_captures`
    captures), and keep those present in both day and night whose day and night medians differ
    by ≤ `max_sd_ns` and whose sd ≤ `max_sd_ns`.  Uses only rows with `u_epoch_ns` not null and
    ≤ 1000.  Returns dicts with `cluster_key`, `fixed_delay_ns`, `delay_sd_ns`, `n_arrivals`,
    `window_from`, `window_to`, `reporter_ids`.
  - `ground_wave_solutions(clusters, transmitters, receivers, solver_version: str) -> list[dict]`:
    solution rows; when the cluster's arrivals carry one `tx_id` and both positions are known,
    `offset_ns = fixed_delay_ns - round(distance_m / 299_792_458 * 1e9)` (great-circle distance,
    WGS-84 mean radius 6_371_008.8 m), else `offset_ns` null and `tx_id = ''`.  `method =
    'ground_wave'`; `inputs_digest` = sha256 of the sorted arrival keys used.
  - CLI verbs: `ddl`, `seed --hfrnet CSV --systable CONF --receivers TOML --snapshot DATE --out DIR`,
    `backfill-codar --root DIR --out DIR`, `backfill-hfdl --root DIR --out DIR`,
    `load --dir DIR`, `solve --out FILE [--from-db | --arrivals FILE]`.  `--from-db` and `load`
    read host, port and the admin user from `/etc/wsprdaemon/clickhouse.conf`
    (`CLICKHOUSE_HOST`, `CLICKHOUSE_PORT`, `CLICKHOUSE_ROOT_ADMIN_USER`,
    `CLICKHOUSE_ROOT_ADMIN_PASSWORD`), never from arguments.

- [ ] **Step 1: Write the failing tests:**
  - `test_steady_cluster_through_day_and_night_is_fixed`: 40 arrivals at 5_047_000 ns ± 2_000,
    half by night and half by day → one cluster, `abs(fixed_delay_ns - 5_047_000) < 2_000`.
  - `test_night_only_cluster_is_not_fixed`: a cluster seen only 02-11Z → none.
  - `test_drifting_cluster_is_not_fixed`: day median 12_475_000, night median 12_520_000 → none.
  - `test_unanchored_rows_never_count`: same steady data with `u_epoch_ns = None` → none.
  - `test_named_cluster_gets_an_offset`: tx at 40.0 N 74.0 W, receiver at 40.0 N 75.0 W
    (85_166 m ± 50) and fixed delay 1_000_000 ns → `offset_ns ≈ 1_000_000 - 284_085` (± 200).
  - `test_cli_reads_credentials_from_conf_only` (the `load` verb parses a temp conf file; no
    argument names a password).
- [ ] **Step 2:** Run → FAIL.  **Step 3:** Implement.  **Step 4:** Run → PASS.
- [ ] **Step 5:** Commit `sigmondb: ground-wave solver and python -m sigmondb`.

### Task 7: The first proof on wd30

No new code unless the run finds a fault; a fault goes back to the owning task.

**Files:**
- Create: `docs/sigmondb-first-proof-2026-11.md` (results), run scripts in
  `/home/mjh/hamsci/tasks/chirp-registry/proof/`

- [ ] **Step 1:** On the devbox, build the row files: `python -m sigmondb seed ...`,
  `backfill-codar --root /home/mjh/hamsci/tasks/codar-t6`, `backfill-hfdl --root
  /home/mjh/hamsci/tasks/hfdl-timing` into `/home/mjh/hamsci/tasks/chirp-registry/proof/rows/`.
  Record row counts per table.
- [ ] **Step 2:** Copy the package and the row files to wd30 (`scp` to `~/sigmondb-trial/`), and
  run with `/opt/wsprdaemon-server/venv/bin/python -m sigmondb ddl` then `load --dir`.  Michael
  runs any command the permission check refuses.
- [ ] **Step 3:** Pass checks, each recorded in the results note with its query and output:
  - every HFRNet site active on the snapshot date has a transmitter and a waveform row
    (`SELECT count() FROM sigmondb.transmitter FINAL WHERE network != 'hfdl' AND status = 'listed'`
    equals the CSV's active count);
  - `load` a second time; `count() FINAL` per table unchanged;
  - `heard` names, per station, the groups `codar-t6/day1108-13mhz/README.md` reports;
  - `solve --from-db` finds K3LR's 11-07 fixed-path clusters (README: 5.047 ms day and night)
    within 20 µs of the README's medians, and W2NAF's likewise.
- [ ] **Step 4:** Commit the results note; tell Rob on the bus that `sigmondb` exists on wd30 only.

---

## Execution note

Tasks 1-6 run on the devbox.  Task 7 touches wd30 only, and creates a database there; nothing
else on wd30 changes.
