# sigmondb CODAR Attribution Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Every hour on wd30, name the CODAR radar behind each cluster of T6-timed arrivals where
the evidence allows it, keep every weaker fit as a recorded candidate, and let the named clusters
carry a `tx_id` and an offset into `sigmondb.solution` and on to `sigmondb_public`.

**Architecture:** A new pure-Python module, `sigmondb/attribution.py`, ports the study's method:
cross-station consistency of `O = D - p` with a fake-site null test.  A new verb,
`python -m sigmondb attribute`, reads arrivals, transmitters, waveforms and receivers from
wd30's `sigmondb`, runs the method under two path policies, and writes one row per fit (named,
ambiguous or unattributable) to a new L3 table, `sigmondb.attribution`.  `solve` then reads the
latest attribution run, attaches names to the ground-wave solver's fixed clusters, and writes
`sigmondb.solution` to the database.  systemd chains the three: attribute, then solve, then the
existing publish, all from the one hourly `sigmondb-publish.timer`.

**Tech Stack:** Python 3.11+ stdlib only (the prototype uses no numpy), clickhouse-connect via
`cli.connect()`, ClickHouse on wd30, systemd, pytest via
`uvx --with numpy --with requests pytest -q -p no:cacheprovider`.

**Spec:** `/home/mjh/hamsci/tasks/chirp-registry/2026-11-09-chirp-registry-design.md` ("The
solver", "Joint fit"; "Calls made after the builder check", "Unnamed arrivals"; "Anti-goals"),
with the method and its evidence in `/home/mjh/hamsci/tasks/t3-augment-study/attribute.py`,
`attribute_mutation.py` and `attribution-result.md`.  Executors read all four.

## Global Constraints

- All code lives in `/home/mjh/hamsci/repos/wsprdaemon-server/sigmondb`; tests in
  `tests/test_sigmondb_*.py`.  Stdlib only.  Nothing imports from `~/hamsci/tasks`: port the logic.
- Run tests from the repo root: `uvx --with numpy --with requests pytest -q -p no:cacheprovider tests/test_sigmondb_*.py`.
  Baseline before Task 1: 139 passed.
- Follow house patterns: `connect(conf_path)` for ClickHouse, `ddl._SPEC` for tables,
  `schema/sigmondb.sql` regenerated with `python3 -m sigmondb.ddl > schema/sigmondb.sql`,
  `rows.write_rows` / `read_rows` for JSON Lines, fake clients in tests (no ClickHouse).
- Port these constants verbatim from `attribute.py`: `C_KM_S = 299_792.458`, `RE = 6371.0`,
  `H_NOM, H_LO, H_HI = 300.0, 250.0, 350.0`, `IONO_TOL_MS = 0.15`, `GW_TOL_MS = 0.03`,
  `GAP_MS = 0.12`, `TIGHT_US = 50.0`, `SAME_SITE_KM = 50.0`, `RUNG_TOL_MS = 0.15`,
  `IMAGE_TOL_MS = 0.4`, ground-wave reach 300 km coastal / 100 km inland, 1F up to 3800 km,
  2F from 3000 km, cluster minimum `max(4, ceil(0.15 * captures))`, 400 fake sites at least
  100 km from every candidate, fake box = candidates' extent ± 5 degrees.
- The naming rule, from the study's Method section: a fit stands only when no rival site fits
  the same clusters within tolerance AND `p_null <= 0.05` (`P_NULL_MAX = 0.05`).  Otherwise
  the status reads `ambiguous`.  This rule applies to skywave fits too: a skywave fit gets a
  name only when it passes it.
- Published offsets never enter the method (Michael's rule of 11-07).
- `sigmondb.arrival.tx_id` never changes.  Arrivals stay the station's measurement (L2);
  names live in the L3 product tables.
- Identity is declared, never merged by guess.  Counting co-located receivers once for the
  consistency test changes no identity row.
- Operators' precise home positions never enter the repo: not in code, seeds, fixtures, tests
  or docs.  They live only in a wd30 file (`~wsprdaemon/.config/sigmondb-receiver-positions.json`,
  mode 600) and in `sigmondb.receiver` on wd30.  The repo seed keeps typed grids as the fallback,
  and `publish` keeps sending only the grid6 square and its centre.
- Work on `main`.  No push.  End every commit message with exactly:

  ```
  Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>
  Claude-Session: https://claude.ai/code/session_01BUMH4psju42aywNgiJv98Z
  ```
- On wd30, only SELECT queries until Task 9, and Task 9 runs only on Michael's go.

## Review Focus

1. **A transporter's label on a radiod's receptions.**  KFS-MAIN runs no radiod; it only
   carries data.  Yet wd30 holds 38,447 rows labelled `KFS-MAIN` and 2,565 labelled `KFS/MAIN`,
   all with `engineering['radiod'] = 'KFS-OMNI-status.local'`.  Expect both to count as the one
   receiver that heard them, KFS-OMNI, never as two stations: two labels on one path would hand
   any site a free "two-station" fit.  Tests in Task 3 (`test_colocated_reporters_count_once`)
   and Task 5 (`test_kfs_main_rows_count_as_the_kfs_omni_radiod`).
2. **Live rows dropped by the detector-version rule.**  `cli._latest_detector` keeps the
   lexically largest `detector_version` per (reporter, group).  `offline-2026-11` sorts above
   `codar-sounder/0.7.0`, so W2NAF's 5,595 live 4.463 MHz rows vanish behind its 1,235 back-fill
   rows.  Expect both kept when they cover different captures.  Test in Task 1.
3. **One fixed cluster, two names; or a name with co-sited aliases** (LOVE/BRIG, HOOK/HEMP).
   Expect no `tx_id` attached and a stated reason.  Tests in Task 6.
4. **Holes in identity.**  `sigmondb.receiver` lacks N8UR, KFS-OMNI and AC0G/B1A today; a
   receiver may still sit on a typed grid; a transmitter may lack a waveform row.  Expect those arrivals or sites set aside with a reason,
   never a crash.  Tests in Tasks 5 and 7.
5. **First run and empty runs.**  No attribution rows yet, or no arrivals in the window.
   Expect `attribute` to write only unattributable rows (or none) and exit 0, and `solve` to
   run and write unnamed solutions as before.  Tests in Tasks 5 and 6.

---

## Decisions this plan makes, and why

- **Candidates go in a new table, `sigmondb.attribution`, not in `solution.set_aside`.**  The
  design makes `solution` "one row per transmitter per solve", with `set_aside` naming the
  *stations* a solve left out and why.  An ambiguous fit has no single transmitter and no offset;
  it names a site, its rival and its fake-site rate.  `publish` reads `solution`; a guess stored
  there sits one filter away from a public offset, which the first anti-goal forbids ("a bad or
  mislabelled reception reaches a published offset").  The design's "Products" section already
  holds L3 tables beside `solution` (`path`, `heard`); `attribution` joins them.  Each row keeps
  the evidence that lets anyone re-judge it, as Goal 3 asks.
- **Names reach the solver at the cluster, not the arrival.**  `apply_attribution` sets a fixed
  cluster's `tx_ids` before `ground_wave_solutions` runs.  Arrival rows keep `tx_id = ''`, as the
  design says for an arrival "the station cannot name".  So `heard` keeps grouping by waveform key.
- **Only the default path policy attaches names.**  The study ran a second pass: ground wave
  only, tight clusters only, reach 900 km.  It called that pass's five pairings "suggestive, not
  established".  This plan records that pass's fits under `mode_policy = 'gw_only_900km'`, and
  `apply_attribution` ignores them.  **Michael decided this on 11-13: the 900 km pass records
  its fits and never attaches them.**  (Even if it attached them, `ground_wave_solutions` would
  set aside K3LR and W2NAF as out of range at 300 km and write no offset.)
- **Receivers: the radiod that heard the signal, at its GPSDO position.**
  - *Which receiver.*  Live rows carry `engineering['radiod']`.  An arrival counts at the
    receiver whose `radiod_id` matches it, and falls back to its `reporter_id` when no receiver
    row matches.  Michael, 11-13: "MAIN at KFS has no radiod so cannot be a reporter, it is a
    transporter only."  So KFS-MAIN and KFS/MAIN rows count at the KFS-OMNI receiver.  The rows
    in the database keep their labels; the rewrite happens in memory, in `attribute` and `solve`
    alike.  The one-site rule (Task 3) stays as a second guard.
  - *Where it stands.*  Michael, 11-13: use the precise GPSDO positions, kept off the repo.
    A new verb, `receiver-positions`, reads a wd30-only JSON file and writes a new version of
    each named receiver row with `pos_source = 'gpsdo'` and its `pos_sigma_m`.  The repo seed
    keeps typed grids as the fallback.  `publish.build_receivers` already sends only the grid6
    square and its centre; a new test pins that for a GPSDO-sourced row.
  - *Tolerances.*  Ground wave counts as a possible mode only at a receiver with
    `pos_source = 'gpsdo'`.  A typed grid can sit 4 km off (about 14 us), half the ground-wave
    tolerance; such a receiver gets skywave modes only, whose 0.2-0.4 ms height freedom dwarfs
    the grid error.  `GW_TOL_MS` stays at 30 us.  The study already used GPSDO receivers and
    chose 30 us for "site position (~100 m) + cluster scatter"; the HFRNet site positions and
    the clusters' own scatter (MAD 5-17 us on the tight 4.463 MHz tracks) set it, not the
    receiver.  Tightening it would also move the fake-site rates the mutation tests pin to the
    study.  Skywave tolerances stay as the study set them.
- **Today's data.**  The live codar-sounder rows carry no CLEAN, and KFS repeats each arrival
  1.632 ms later.  The port keeps the prototype's three guards: `flag_rungs` sets aside a cluster
  m rungs from a stronger one at the same station; `flag_twins` sets aside a cluster 0-5 ms after
  one with the same rows, level and spread; `flag_images` sets aside a cluster where a stronger
  one 3-30 kHz away lands after the `-(c_true - c_filter)/k` shift.  The study found eight of
  KFS's sixteen 4.785 MHz clusters on a rung and four twins, and named nothing.  Set-aside
  clusters never enter a tuple; the row records each one in `set_aside`.
- **Window: 30 days** (Michael, 11-13), the study's `--days` default.  K3LR has only back-fill
  rows (11-04 to 11-08), so a shorter window drops it from the 4.463 MHz group within days.
  Task 9 times a dry run; Michael revisits the window with that measurement in hand.

---

## File Structure

| Path (in wsprdaemon-server) | Responsibility |
|---|---|
| `sigmondb/attribution.py` (new) | The ported method: geometry, clusters, flags, matching, null test, rows, `apply_attribution` |
| `sigmondb/ddl.py`, `schema/sigmondb.sql` | `attribution` table |
| `sigmondb/load.py` | `insert_rows(client, table, rows)` beside `insert_file` |
| `sigmondb/cli.py` | `attribute` and `receiver-positions` verbs; `solve --write-db`, `--days`, attribution read; per-capture detector rule; arrivals counted at their radiod |
| `sigmondb/seed/receivers.toml` | N8UR, KFS-OMNI, AC0G/B1A (typed grids, the fallback) |
| `sigmondb/receivers.py` | `load_positions`, `position_rows`: GPSDO positions from a wd30-only file |
| `sigmondb-attribute.service`, `sigmondb-solve.service` (new), `sigmondb-publish.service` | The hourly chain |
| `tests/test_sigmondb_attribution.py` (new) | Method, guards, mutation tests |
| `tests/fixtures/sigmondb/attr_sites_4463.jsonl` (new) | The 23 candidate sites of group 4.4630 -25.73 |
| `tests/test_sigmondb_cli.py`, `test_sigmondb_ddl.py`, `test_sigmondb_load.py` | Verb, table, insert tests |
| `tests/test_sigmondb_units.py` (new) | Unit ordering |

---

### Task 1: Keep the latest detector per capture, not per group

**Files:**
- Modify: `sigmondb/cli.py` (`_latest_detector`)
- Test: `tests/test_sigmondb_cli.py`

**Interfaces:**
- Produces: `_latest_detector(arr: list[dict]) -> list[dict]`, keyed on
  `(reporter_id, group_key, time)`.  Task 5 calls it on attribution arrivals.

- [ ] **Step 1: Write the failing test**

```python
def test_live_and_backfill_rows_of_one_group_both_survive(monkeypatch):
    t1, t2 = datetime(2026, 11, 7, 3, tzinfo=timezone.utc), datetime(2026, 11, 13, 3, tzinfo=timezone.utc)
    _db(monkeypatch, {"arrival": [
        _arr("W2NAF", "4.4630 -25.73", "offline-2026-11", t=t1, delay=1),
        _arr("W2NAF", "4.4630 -25.73", "codar-sounder/0.7.0", t=t2, delay=2),
        _arr("W2NAF", "4.4630 -25.73", "codar-sounder/0.7.0", t=t1, delay=3)]})
    arr, _, _ = cli._from_db(_ns())
    assert sorted(r["delay_ns"] for r in arr) == [1, 2]   # t1: offline wins; t2: live kept
```

- [ ] **Step 2: Run it and see it fail**

Run: `uvx --with numpy --with requests pytest -q -p no:cacheprovider tests/test_sigmondb_cli.py -k both_survive`
Expected: FAIL, `[1] != [1, 2]`.

- [ ] **Step 3: Key `_latest_detector` on `(reporter_id, group_key, time)`**

Keep the lexical rule inside one capture.  Update the docstring: different captures keep their
own version, so live and back-fill rows of one group both count.

- [ ] **Step 4: Run the whole sigmondb suite**

Expected: all pass, including `test_from_db_reads_final_and_keeps_latest_detector_version`
(its rows share one time).

- [ ] **Step 5: Commit** — `sigmondb solve: latest detector per capture, so live rows survive back-fill`

---

### Task 2: The `attribution` table and `insert_rows`

**Files:**
- Modify: `sigmondb/ddl.py`, `schema/sigmondb.sql`, `sigmondb/load.py`
- Test: `tests/test_sigmondb_ddl.py`, `tests/test_sigmondb_load.py`

**Interfaces:**
- Produces: `ddl.COLUMNS["attribution"]` in this order:
  `run_at` DateTime, `run_id` String, `window_from` DateTime64(3), `window_to` DateTime64(3),
  `group_key` LC, `period_ns` Int64, `mode_policy` LC, `tx_id` String,
  `alias_tx_ids` Array(String), `status` LC (`named`, `named_two_stations`, `ambiguous`,
  `unattributable`), `n_stations` UInt16, `reporter_ids` Array(String), `modes` Array(String),
  `cluster_delays_ns` Array(Int64), `cluster_mad_ns` Array(Float64), `offset_ns`
  Nullable(Int64), `offset_from` LC (`ground_wave`, `skywave_model`, ''), `spread_ns` NF64,
  `p_null` NF64, `n_fake` UInt32, `runner_up_tx_id` String, `runner_up_spread_ns` NF64,
  `runner_up_fits` UInt8, `flags` Array(String), `set_aside` Map(String, String),
  `solver_version` LC, `inputs_digest` FixedString(64).
  Partition `toYYYYMM(run_at)`; ORDER BY `(group_key, mode_policy, tx_id, run_id)`.
  Place it after `solution` in `_SPEC`.
- Produces: `load.insert_rows(client, table, rows: list[dict]) -> int` — the same whitelist,
  `''` fill and None check as `insert_file`, all rows checked before the first insert.
  `insert_file` keeps its behaviour.

- [ ] **Step 1: Write the failing tests**

```python
def test_attribution_key_and_place():
    a = _norm(ddl.TABLES["attribution"])
    assert "ORDER BY (group_key, mode_policy, tx_id, run_id)" in a
    assert "PARTITION BY toYYYYMM(run_at)" in a
    names = list(ddl.TABLES)
    assert names.index("solution") < names.index("attribution") < names.index("path")
```

```python
def test_insert_rows_checks_everything_before_sending():
    c = FakeClient()
    good = valid_row("attribution")
    bad = dict(good, n_stations=None)
    with pytest.raises(ValueError, match="n_stations"):
        load.insert_rows(c, "attribution", [good, bad])
    assert c.calls == []
    assert load.insert_rows(c, "attribution", [dict(good, extra="x")]) == 1
```

- [ ] **Step 2: Run them and see them fail** (KeyError `attribution`).
- [ ] **Step 3: Add the table to `_SPEC`; add `insert_rows`; regenerate `schema/sigmondb.sql`.**
  `valid_row` in `test_sigmondb_load.py` may need defaults for `Array(Int64)`, `Array(Float64)`
  and `FixedString(64)`; add them there.
- [ ] **Step 4: Run the whole suite.**  `test_sql_file_mirrors_ddl` must pass.
- [ ] **Step 5: Commit** — `sigmondb: attribution table (L3) and insert_rows`

---

### Task 3: Port the method, part 1: geometry, clusters and the three guards

**Files:**
- Create: `sigmondb/attribution.py`
- Test: `tests/test_sigmondb_attribution.py`

**Interfaces:**
- Produces (ports of `attribute.py`, same arithmetic):
  - `gc_km(a, b) -> float`, `sky_km(d, h, hops) -> float`, `cdiff(a, b, P) -> float` (L147-181).
  - `@dataclass(frozen=True) class Policy: name: str; gw_reach_km: float | None; gw_only: bool; max_centre_hz: float | None`.
    `DEFAULT = Policy("default", None, False, None)`; `GW900 = Policy("gw_only_900km", 900.0, True, 6e6)`.
  - `modes(d, rx, policy, tight=True, gw_ok=True) -> list[tuple[str, float, float]]`
    (L159-175).  The global `GW_ONLY` becomes `policy.gw_only`; `gw_reach` becomes
    `policy.gw_reach_km`.  `gw_ok` False drops the ground-wave mode.
  - `rxpos` maps reporter -> `(lat, lon, pos_source)`; callers pass
    `gw_ok = rxpos[st][2] == "gpsdo"` wherever the prototype calls `modes` (`entries`,
    `tuple_fit`).
  - `COASTAL = frozenset({"KFS-OMNI", "WB6CXC/7", "UACNJ"})`.
  - `mad_us(xs)`, `clusters_of(rows, P) -> (list[dict], int)` (L186-221), rows `(t, d_ms, level)`.
    Drop the `hours` key: only the report used it.
  - `rung_spacing_ms(centre_hz, rate)`, `flag_rungs(cl, R, P, live)`, `flag_twins(cl, P)`,
    `flag_images(groups)` (L224-239, L382-417).
  - `parse_key(gk) -> (centre_hz, rate_hz_per_s, period_s | None)` (L339-344).
  - `CO_SITE_KM = 50.0`; `collapse_colocated(rows_by_st, rxpos, coherence) -> (dict, dict[str, str])`:
    reporters that share a non-empty `coherence_group`, or sit within `CO_SITE_KM` of each other,
    count as one.  Keep the one with the most captures in the group; the others go to the
    returned set-aside map as `"co-located with <kept>; counted once"`.
  - `analyse_group(gk, rows_by_st, sites, rxpos, eng_period, coherence=None) -> dict`
    (L354-379, plus `collapse_colocated` first).  Rows `(t, d_ms, level, live)`.  The group
    dict gains `"set_aside": {}`; each flagged cluster adds
    `set_aside["<reporter>|<D in us, int>"] = "<first flag>"`, and each reporter with no
    position adds `set_aside[reporter] = "no receiver position"`.

- [ ] **Step 1: Write the failing tests**

```python
def test_sky_and_ground_delays_match_the_prototype():
    assert abs(gc_km((41.0, -75.0), (41.0, -76.0)) - 83.9) < 0.1
    assert abs(sky_km(564.0, 300.0, 1) - 832.48) < 0.01        # K3LR-ASSA 1F at h = 300 km
    assert [m[0] for m in modes(90.0, "UACNJ", DEFAULT)] == ["G", "1F"]
    assert [m[0] for m in modes(150.0, "K3LR/D", DEFAULT)] == ["1F"]
    assert [m[0] for m in modes(600.0, "K3LR/D", GW900)] == ["G"]
    assert [m[0] for m in modes(90.0, "UACNJ", DEFAULT, gw_ok=False)] == ["1F"]   # typed grid

def test_kfs_twin_is_set_aside():
    # 100 captures; cluster at 499.444 ms and a twin 1.632 ms later, same rows and level.
    g = analyse_group("4.7850 -25.73", {"KFS-OMNI": _cluster_rows(499.444, 5, 100, live=True)
                                        + _cluster_rows(501.076, 5, 100, live=True)}, SITES, RX, None)
    cl = g["stations"]["KFS-OMNI"]
    assert [c.get("twin", False) for c in sorted(cl, key=lambda c: c["D"])] == [False, True]
    assert any(v.startswith("twin of") for v in g["set_aside"].values())

def test_live_rung_is_set_aside():
    R = rung_spacing_ms(4.785e6, -25_733.9)                      # 19.98 ms
    rows = _cluster_rows(517.121, 20, 200, live=True, level=21.0) + \
           _cluster_rows(517.121 + R, 20, 120, live=True, level=19.0)
    g = analyse_group("4.7850 -25.73", {"KFS-OMNI": rows}, SITES, RX, None)
    weak = min(g["stations"]["KFS-OMNI"], key=lambda c: c["n"])
    assert weak.get("rung") and weak["flags"][0].startswith("rung +1")

def test_colocated_reporters_count_once():
    rows = _cluster_rows(40.0, 10, 50)
    here = (37.0, -122.0, "gpsdo")                       # synthetic, not anyone's home
    g = analyse_group("4.4630 -25.73", {"RX-A": rows, "RX-B": rows[:30]},
                      SITES, {"RX-A": here, "RX-B": (37.01, -122.0, "gpsdo")}, None)
    assert list(g["stations"]) == ["RX-A"]
    assert "co-located with RX-A" in g["set_aside"]["RX-B"]
```

`_cluster_rows(D_ms, width_us, n_captures, live=False, level=20.0, t0=0, seed=1)` lives in the
test file: one row per capture, delays uniform over `D ± width/2`, capture times `t0 + 600*i`.
`SITES` and `RX` hold a few hand-made sites and receivers.

- [ ] **Step 2: Run them and see them fail** (ImportError).
- [ ] **Step 3: Port the listed functions into `sigmondb/attribution.py`.**  Module docstring:
  the physics paragraph and "Choices" list from `attribute.py` L1-40, with the input lines
  rewritten for sigmondb (waveform rows replace `stationList.csv`; `sigmondb.receiver` replaces
  `stations.json`).
- [ ] **Step 4: Run the tests; all pass.**
- [ ] **Step 5: Commit** — `sigmondb attribution: geometry, clusters, rung/twin/image guards`

---

### Task 4: Port the method, part 2: matching, the null test, and the mutation tests

**Files:**
- Modify: `sigmondb/attribution.py`
- Create: `tests/fixtures/sigmondb/attr_sites_4463.jsonl`
- Test: `tests/test_sigmondb_attribution.py`

**Interfaces:**
- Consumes: Task 3's functions and `Policy`.
- Produces:
  - `entries(site_pos, st_clusters, rxpos, P, policy)`, `best_sets(ents, P, min_st=2)`,
    `tuple_fit(site_pos, tup, st_clusters, rxpos, P, policy)`,
    `fake_sites(cands, n, seed, avoid_km=100.0, avoid_from=None)`,
    `candidates_for(centre, rate, sites)` — ports of L244-334 and L347-351.
  - `P_NULL_MAX = 0.05`; `N_FAKE = 400`.
  - `attribute_group(g, rxpos, policy, n_fake, seed) -> None` — port of L420-516.  Status
    strings become `named`, `named_two_stations`, `ambiguous`.  A fit gets a name only when no
    rival fits and `p_null <= P_NULL_MAX` (prototype L487-491).  Drop `partners`, `sky_check`,
    `fake_rate`, `real_rate`: only the report read them.  A group verdict (L424-430) stays in
    `g["verdict"]`.
  - `run_attribution(arrivals, sites, rxpos, policy, n_fake=N_FAKE, coherence=None) -> list[dict]`:
    groups arrivals by `group_key`, then reporter; skips groups with
    `centre >= policy.max_centre_hz`; runs `analyse_group` on each, then `flag_images` across
    them, then `attribute_group` with `seed = zlib.crc32(group_key.encode())`, so hourly runs
    on the same data agree.
    Arrival dicts carry `time`, `reporter_id`, `group_key`, `delay_ns`, `level_db`, `live`.
    Site dicts carry `id` (the full `tx_id`, e.g. `hfrnet:ASSA`), `lat`, `lon`, `freq`, `rate`, `period`.

**The fixture.**  Export the candidate sites once from wd30 (SELECT only):

```
cd /home/mjh/hamsci/ops && bin/fleet-ssh wd30 'clickhouse-client -q "SELECT t.tx_id AS id, t.lat, t.lon, w.centre_hz AS freq, w.rate_hz_per_s AS rate, w.period_s AS period FROM (SELECT tx_id, argMax(lat, version) AS lat, argMax(lon, version) AS lon FROM sigmondb.transmitter FINAL WHERE kind = '"'"'codar'"'"' GROUP BY tx_id) t INNER JOIN sigmondb.waveform w FINAL USING tx_id WHERE abs(w.centre_hz - 4463000) <= 3000 ORDER BY id FORMAT JSONEachRow"' </dev/null > tests/fixtures/sigmondb/attr_sites_4463.jsonl
```

Expect the 23 candidates the study lists for 4.4630 -25.73 (BMLR ... WAIN) among the rows,
plus the -47.79 kHz/s sites, which `candidates_for` filters out.

**The background.**  The study injected its synthetic radar into 30 days of real clusters, and
the background shapes the fake-site rate.  Rebuild it in the test as `BACKGROUND_4463`: copy the
AC0G/ND, K3LR/D and W2NAF tables of `attribution-result.md`, section "Group 4.4630 -25.73",
as `(reporter, D_ms, p10_90_us, captures)`, and the station capture totals (ND 88, K3LR 201,
W2NAF 141, UACNJ 4).  `_background()` makes one row per capture for each cluster, uniform over
`D ± p10_90/2` (which keeps every neighbouring pair apart by more than `GAP_MS` and every tight
cluster under `TIGHT_US`), and pads each station with empty-cluster captures up to its total.
Receivers: grid centres from `sigmondb/seed/receivers.toml`, labelled `pos_source = "gpsdo"`
in the test so ground wave applies.  The synthetic radar uses the same positions, so the test
stays self-consistent, and no home position enters the repo.

**The injection** follows `attribute_mutation.py`: 80 rows per station, times
`1762500000 + 600*k + 7*station_index`, delay `O + p + gauss(0, 0.01 if G else 0.08)` ms,
`random.Random(1)`, group `4.4630 -25.73`, `O = 300.0` ms, `p` from the test's own receiver
positions.

- [ ] **Step 1: Write the four mutation tests (the study's findings, as assertions)**

```python
def _holding(groups, injected):          # results whose tuple uses an injected cluster
    g = next(g for g in groups if g["key"] == "4.4630 -25.73")
    return [nm for nm in g["named"] if any(abs(cdiff(g["stations"][m[0]][m[1]]["D"], injected[m[0]], g["P"])) < 0.2
                                           for m in nm["mem"])]

def test_mutation_assa_ground_wave_read_as_skywave_is_never_named():
    hits = _holding(_run("hfrnet:ASSA", "G", 0, ["K3LR/D", "W2NAF"], DEFAULT), INJ)
    assert hits and all(nm["status"] == "ambiguous" for nm in hits)

def test_mutation_assa_ground_wave_two_stations_is_named():
    hits = _holding(_run("hfrnet:ASSA", "G", 0, ["K3LR/D", "W2NAF"], GW900), INJ)
    nm = next(nm for nm in hits if nm["site"]["id"] == "hfrnet:ASSA")
    assert nm["status"] == "named_two_stations"
    assert abs(cdiff(nm["O_ground"], 300.0, 1000.0)) <= GW_TOL_MS
    assert nm["p_null"] <= P_NULL_MAX

def test_mutation_assa_three_ground_wave_stations_is_named():
    hits = _holding(_run("hfrnet:ASSA", "G", 0, ["K3LR/D", "W2NAF", "UACNJ"], GW900), INJ)
    nm = next(nm for nm in hits if nm["site"]["id"] == "hfrnet:ASSA")
    assert nm["status"] == "named" and nm["n"] == 3 and nm["p_null"] == 0.0

def test_mutation_nant_skywave_is_found_but_stays_ambiguous():
    hits = _holding(_run("hfrnet:NANT", "1F", 320.0, ["K3LR/D", "W2NAF", "AC0G/ND"], DEFAULT), INJ)
    nm = next(nm for nm in hits if nm["site"]["id"] in ("hfrnet:NANT", "hfrnet:NAUS"))
    assert nm["status"] == "ambiguous"
    assert abs(cdiff(nm["O"], 300.0, 1000.0)) <= 0.15            # study: 79 us off
```

`_run(site, mode, h_km, stations, policy)` builds background + injection and returns
`run_attribution(...)`; it records the injected D per station in the module-level `INJ`.

- [ ] **Step 2: Run them and see them fail** (ImportError on `run_attribution`).
- [ ] **Step 3: Port the listed functions.**
- [ ] **Step 4: Run the four tests.**  All four must pass as written.  If one fails, do not move
  a threshold or a seed to make it pass.  The study's 2-station ASSA case sat exactly at
  p = 0.05, and a new seed or a rebuilt background can tip it.  Stop and report the measured
  `status`, `p_null`, rival and offset for each case to Michael.
- [ ] **Step 5: Mutate the guard, then restore it.**  Change `pn > 0.05` to `pn > 0.5`: the
  default-policy ASSA and NANT tests must fail.  Restore.
- [ ] **Step 6: Commit** — `sigmondb attribution: consistency fit, fake-site null, mutation tests from the study`

---

### Task 5: Rows, and the `attribute` verb

**Files:**
- Modify: `sigmondb/attribution.py`, `sigmondb/cli.py`
- Test: `tests/test_sigmondb_attribution.py`, `tests/test_sigmondb_cli.py`

**Interfaces:**
- Consumes: `run_attribution`, `Policy`, `DEFAULT`, `GW900`; `load.insert_rows`; `_latest_detector`.
- Produces:
  - `ATTRIB_VERSION = "attribute-1"`.
  - `inputs_digest(arrivals) -> str` — sha256 hex over the sorted
    `"%s|%s|%s|%d" % (reporter_id, group_key, time.isoformat(), delay_ns)` lines, as
    `groundwave._key` builds them.
  - `attribution_rows(groups, policy, run_at, window_from, window_to, digest) -> list[dict]`.
    One row per entry of `g["named"]`; one `unattributable` row (`tx_id = ''`,
    `flags = [verdict]`) per group with a verdict or with no fit.  `run_id = "attr:%s:%s" %
    (ATTRIB_VERSION, digest[:12])`.  `offset_ns = round(O_ground or O, ms -> ns)`,
    `offset_from = "ground_wave"` when `O_ground` exists, else `"skywave_model"`.
    `p_null` NaN becomes None.  Every row carries its group's `set_aside`.
  - `cli.cmd_attribute(a) -> int`; options `--days` (float, 30), `--n-fake` (int, 400),
    `--conf`, `--out PATH` (write JSON Lines and leave the database alone).
  - `cli._attribution_inputs(client, days) -> (arrivals, sites, rxpos, coherence)`:
    - arrivals: `time, reporter_id, group_key, delay_ns, level_db, detector_version, engineering`
      from `sigmondb.arrival FINAL WHERE u_epoch_ns IS NOT NULL AND u_epoch_ns <= 1000 AND
      clean_tag = 'A' AND rate_hz_per_s != 0 AND time > now() - INTERVAL {days:Float64} DAY`,
      then `_latest_detector`; `live = "sweep_repetition_hz" in engineering`.
    - sites: latest `transmitter` row per `tx_id` with `kind = 'codar'` and a position, joined in
      Python to every `waveform` row of that `tx_id`; a transmitter with no waveform drops out.
    - rxpos / coherence: latest `receiver` row per `reporter_id` with lat and lon;
      `rxpos[rid] = (lat, lon, pos_source)`.
    - each arrival then passes through `_at_radiod(arrivals, receivers)`.
  - `cli._at_radiod(arrivals, receivers) -> list[dict]`: when `engineering['radiod']` equals the
    `radiod_id` of a latest receiver row, set the arrival's `reporter_id` to that row's
    `reporter_id` and keep the old label as `carried_by`.  Otherwise leave it.  In memory only.
    `_from_db` (solve) applies it too, so both stages count a reception at one receiver.
  - Factor `_from_db`'s inner `fetch` into module-level `_fetch(client, table, cols, where="",
    params=None)` so both readers share the FINAL, `query_tz="UTC"` and naive-UTC handling.
    Teach the tests' `QueryClient` to return the selected columns, parsed from `SELECT ... FROM`.

The verb runs `DEFAULT` and then `GW900` on the same inputs, calls `ddl.ensure_schema` and
`insert_rows(client, "attribution", rows)` (or writes `--out`), and prints one line per policy:
`attribute <policy>: <n> groups, <n> named, <n> ambiguous, <n> unattributable`, then one line per
named row.  It exits 0 when nothing gets named.  A database error propagates, so systemd records
a failure.

- [ ] **Step 1: Write the failing tests**

```python
def test_verdict_groups_become_unattributable_rows():   # one-station group, today's KFS shape
    gs = run_attribution(_kfs_only_rows(), SITES, RX, DEFAULT, n_fake=20)
    rows = attribution_rows(gs, DEFAULT, RUN_AT, W0, W1, "0" * 64)
    assert [r["status"] for r in rows] == ["unattributable"]
    assert rows[0]["flags"][0].startswith("only 1 T6 station")

def test_cli_attribute_writes_both_policies_and_exits_zero_with_nothing_named(monkeypatch, capsys):
    c = _db(monkeypatch, {"arrival": KFS_LIVE_ROWS, "transmitter": TX, "waveform": WF, "receiver": RX_ROWS})
    assert cli.main(["attribute", "--n-fake", "20"]) == 0
    ins = [r for t, r in c.inserted if t == "sigmondb.attribution"]
    assert {r["mode_policy"] for r in ins} == {"default", "gw_only_900km"}   # 4.785 MHz < 6 MHz
    assert all(r["status"] == "unattributable" for r in ins)
    assert "0 named" in capsys.readouterr().out

def test_cli_attribute_sets_aside_a_reporter_without_position(monkeypatch, tmp_path):
    _db(monkeypatch, {"arrival": N8UR_ROWS + K3LR_ROWS, "transmitter": TX, "waveform": WF,
                      "receiver": [K3LR_RX]})
    assert cli.main(["attribute", "--n-fake", "20", "--out", str(tmp_path / "a.jsonl")]) == 0
    out = rows.read_rows(tmp_path / "a.jsonl")
    assert all(r["set_aside"]["N8UR"] == "no receiver position" for r in out)

def test_kfs_main_rows_count_as_the_kfs_omni_radiod():
    eng = {"radiod": "KFS-OMNI-status.local", "sweep_repetition_hz": "1"}
    arr = [dict(_arr("KFS-MAIN", "g", "v"), engineering=eng), dict(_arr("KFS/MAIN", "g", "v"), engineering=eng)]
    out = cli._at_radiod(arr, [dict(valid_row("receiver"), reporter_id="KFS-OMNI",
                                    radiod_id="KFS-OMNI-status.local")])
    assert {r["reporter_id"] for r in out} == {"KFS-OMNI"}
    assert {r["carried_by"] for r in out} == {"KFS-MAIN", "KFS/MAIN"}

def test_cli_attribute_with_no_arrivals_writes_nothing_and_exits_zero(monkeypatch):
    c = _db(monkeypatch, {"transmitter": TX, "waveform": WF, "receiver": RX_ROWS})
    assert cli.main(["attribute", "--n-fake", "20"]) == 0
    assert [t for t, _ in c.inserted if t == "sigmondb.attribution"] == []
```

The fixtures (`KFS_LIVE_ROWS`, `N8UR_ROWS`, `K3LR_ROWS`, `TX`, `WF`, `RX_ROWS`, `K3LR_RX`) hold a
few rows each in `ddl.COLUMNS` shape.  Extend `QueryClient` with `command(sql)` (no-op) and
`insert(table, data, column_names)`, recording `c.inserted` as `(table, dict row)` pairs.

- [ ] **Step 2: Run them and see them fail.**
- [ ] **Step 3: Implement `inputs_digest`, `attribution_rows`, `_fetch`, `_attribution_inputs`, `cmd_attribute`; register the verb in `build_parser`.**
- [ ] **Step 4: Run the whole suite.**
- [ ] **Step 5: Commit** — `sigmondb attribute: hourly verb writes named, ambiguous and unattributable fits`

---

### Task 6: `solve` reads the names and writes to the database

**Files:**
- Modify: `sigmondb/attribution.py`, `sigmondb/cli.py`
- Test: `tests/test_sigmondb_attribution.py`, `tests/test_sigmondb_cli.py`

**Interfaces:**
- Consumes: Task 2's table and `insert_rows`; `groundwave.fixed_clusters`, `ground_wave_solutions`.
- Produces:
  - `NAMED_STATUSES = ("named", "named_two_stations")`.
  - `apply_attribution(clusters, attributions, match_tol_ns=50_000) -> dict[str, str]`.  For each
    fixed cluster (one reporter, one group), find rows with `mode_policy == "default"`, status in
    `NAMED_STATUSES`, the same `group_key`, and a member at the same reporter whose
    `cluster_delays_ns` lies within `match_tol_ns` of `fixed_delay_ns`, modulo `period_ns`.
    One match with empty `alias_tx_ids`: set `c["tx_ids"] = [tx_id]`.  Otherwise leave the
    cluster alone and return `{cluster_key: reason}`, reason one of
    `"named <tx> but co-sited with <aliases>"`, `"two names: <tx1>, <tx2>"`.
  - `SOLVER_VERSION = "groundwave-3"`.
  - `solve` options: `--write-db` (insert into `sigmondb.solution` after `ensure_schema`);
    `--out` no longer required, but exactly one of `--out` / `--write-db` must be given;
    `--days` (float, default 30, from-db only: `AND time > now() - INTERVAL {days:Float64} DAY`);
    `--no-attribution` (from-db); `--attributions PATH` (file mode).
  - From the database, the latest run:
    `SELECT <cols> FROM sigmondb.attribution FINAL WHERE run_at = (SELECT max(run_at) FROM sigmondb.attribution)`.
    An empty table yields no names and no error.

- [ ] **Step 1: Write the failing tests**

```python
def test_a_named_cluster_reaches_the_solution_with_offset():
    arr = _gw_arrivals(rid="UACNJ", centre_ns=HOOK_D_NS)           # fixed day and night, 95 km from HOOK
    cl = fixed_clusters(arr, RX_ROWS)
    reasons = apply_attribution(cl, [_attr("hfrnet:HOOK", "named", "UACNJ", HOOK_D_NS)])
    sol = ground_wave_solutions(cl, TX_ROWS, RX_ROWS, "groundwave-3")
    assert reasons == {} and sol[0]["tx_id"] == "hfrnet:HOOK" and sol[0]["offset_ns"] is not None

def test_ambiguous_and_gw900_rows_never_name():
    for a in (_attr("hfrnet:HOOK", "ambiguous", "UACNJ", HOOK_D_NS),
              dict(_attr("hfrnet:HOOK", "named", "UACNJ", HOOK_D_NS), mode_policy="gw_only_900km")):
        cl = fixed_clusters(_gw_arrivals(rid="UACNJ", centre_ns=HOOK_D_NS), RX_ROWS)
        apply_attribution(cl, [a])
        assert cl[0]["tx_ids"] == [""]

def test_aliased_or_doubly_named_cluster_stays_unnamed():
    cl = fixed_clusters(_gw_arrivals(rid="UACNJ", centre_ns=HOOK_D_NS), RX_ROWS)
    r = apply_attribution(cl, [dict(_attr("hfrnet:HOOK", "named", "UACNJ", HOOK_D_NS),
                                    alias_tx_ids=["hfrnet:HEMP"])])
    assert cl[0]["tx_ids"] == [""] and "co-sited with hfrnet:HEMP" in r[cl[0]["cluster_key"]]
    cl = fixed_clusters(_gw_arrivals(rid="UACNJ", centre_ns=HOOK_D_NS), RX_ROWS)
    r = apply_attribution(cl, [_attr("hfrnet:HOOK", "named", "UACNJ", HOOK_D_NS),
                               _attr("hfrnet:LOVE", "named", "UACNJ", HOOK_D_NS + 10_000)])
    assert cl[0]["tx_ids"] == [""] and r[cl[0]["cluster_key"]].startswith("two names")

def test_a_twin_cluster_is_never_named():
    # A named row lists the cluster at D; its 1.632 ms twin sits outside match_tol_ns.
    arr = _gw_arrivals(rid="UACNJ", centre_ns=HOOK_D_NS) + \
          _gw_arrivals(rid="UACNJ", centre_ns=HOOK_D_NS + 1_632_000, seed=2)
    cl = fixed_clusters(arr, RX_ROWS)
    apply_attribution(cl, [_attr("hfrnet:HOOK", "named", "UACNJ", HOOK_D_NS)])
    assert sorted(c["tx_ids"][0] for c in cl) == ["", "hfrnet:HOOK"]
```

`_gw_arrivals` wraps `test_sigmondb_groundwave._arrivals` with UACNJ's grid position and a
night/day split that holds at UACNJ.  `_attr(tx, status, rid, d_ns)` builds one attribution row
(`mode_policy="default"`, `group_key="4.4630 -25.73"`, `period_ns=1_000_000_000`,
`alias_tx_ids=[]`, `reporter_ids=[rid]`, `cluster_delays_ns=[d_ns]`).

def test_solve_write_db_inserts_solutions_and_tolerates_an_empty_attribution_table(monkeypatch):
    c = _db(monkeypatch, {"arrival": _gw_arrivals(rid="UACNJ", centre_ns=HOOK_D_NS), "receiver": RX_ROWS, "attribution": []})
    assert cli.main(["solve", "--from-db", "--write-db"]) == 0
    assert any(t == "sigmondb.solution" for t, _ in c.inserted)

def test_solve_needs_out_or_write_db():
    with pytest.raises(SystemExit):
        cli.main(["solve", "--from-db"])
```

- [ ] **Step 2: Run them and see them fail.**
- [ ] **Step 3: Implement `apply_attribution` and the `solve` changes.**  `cmd_solve` calls
  `apply_attribution` between `fixed_clusters` and `ground_wave_solutions` and prints each reason
  to stderr as `attribution not applied <cluster_key>: <reason>`.
- [ ] **Step 4: Run the whole suite.**
- [ ] **Step 5: Mutate, then restore.**  Drop the `mode_policy == "default"` check: the GW900
  test must fail.  Drop the alias check: the co-sited test must fail.
- [ ] **Step 6: Commit** — `sigmondb solve: attach attributed names, write solutions to the database`

---

### Task 7: Receivers: seed the radiods, load GPSDO positions from a wd30-only file

**Files:**
- Modify: `sigmondb/seed/receivers.toml`, `sigmondb/receivers.py`, `sigmondb/cli.py`
- Test: `tests/test_sigmondb_identity.py` (or wherever `load_receivers` gets tested),
  `tests/test_sigmondb_publish.py`, `tests/test_sigmondb_cli.py`

**Interfaces:**
- Produces:
  - `receivers.load_positions(path) -> dict[str, dict]`: JSON object
    `{reporter_id: {"lat": float, "lon": float, "pos_sigma_m": float}}`.  Raises `ValueError`
    when the file grants any group or other permission (`st_mode & 0o077`), or when an entry
    lacks a key.
  - `receivers.position_rows(latest_rows, positions, now) -> (list[dict], list[str])`: for each
    reporter in `positions` with a receiver row, a copy of its latest row with the new lat, lon
    and `pos_sigma_m`, `pos_source = "gpsdo"`, `valid_from = now` (naive UTC, whole seconds),
    `version = old + 1`.  A reporter with no row goes to the second list; it never creates one.
    A row already equal in lat, lon and sigma gets no new version.
  - `python -m sigmondb receiver-positions --file PATH [--conf]`: reads the latest receiver
    rows FINAL, inserts `position_rows` with `insert_rows`, prints
    `receiver-positions: <n> updated, <n> unchanged, <n> unknown: <ids>`.  It never prints a
    coordinate.

Add three `[[receiver]]` tables to the seed, typed grids as `codar-map/stations.json` and
`ops/fleet.toml` give them:

| reporter_id | host_id | radiod_id | grid |
|---|---|---|---|
| `N8UR` | `n8ur` | `N8UR-status.local` | `EM79vr` |
| `KFS-OMNI` | `kfs-omni` | `KFS-OMNI-status.local` | `CM87tj` |
| `AC0G/B1A` | `b2` | `ac0g-b1-b-status.local` | `EM38ww` |

No KFS-MAIN and no KFS/MAIN: KFS-MAIN transports, it receives nothing.  `KFS-OMNI` serves as a
provisional reporter id for the KFS radiod until the follow-on below settles the real one.
AC0G/B1A rows from `ac0g-b1-a-status.local` find no receiver by radiod and fall back to the
AC0G/B1A row: same site.

- [ ] **Step 1: Write the failing tests**

```python
def test_seed_names_the_radiods_not_the_transporter():
    ids = {r["reporter_id"] for r in load_receivers(SEED)}
    assert {"N8UR", "KFS-OMNI", "AC0G/B1A"} <= ids
    assert not ids & {"KFS-MAIN", "KFS/MAIN"}

def test_position_rows_version_up_and_say_gpsdo(tmp_path):
    f = tmp_path / "pos.json"
    f.write_text(json.dumps({"K3LR/D": {"lat": 41.0, "lon": -80.0, "pos_sigma_m": 10.0},
                             "NOPE": {"lat": 1.0, "lon": 1.0, "pos_sigma_m": 10.0}}))
    f.chmod(0o600)
    rows, unknown = position_rows(load_receivers(SEED), load_positions(f), datetime(2026, 11, 14))
    assert unknown == ["NOPE"] and len(rows) == 1
    r = rows[0]
    assert (r["pos_source"], r["version"], r["lat"]) == ("gpsdo", 2, 41.0)
    assert _latest(load_receivers(SEED) + rows, "reporter_id")["K3LR/D"] is r

def test_a_world_readable_positions_file_is_refused(tmp_path):
    f = tmp_path / "pos.json"; f.write_text("{}"); f.chmod(0o644)
    with pytest.raises(ValueError, match="mode"):
        load_positions(f)

def test_a_gpsdo_receiver_still_publishes_only_grid6():          # in test_sigmondb_publish.py
    rx = dict(reporter_id="K3LR/D", lat=41.0123, lon=-80.0456, a_level="A1",
              pos_source="gpsdo", pos_sigma_m=10.0, valid_from=datetime(2026, 11, 14), version=2)
    out = build_receivers([rx], 1.0)[0]
    assert out["grid6"] == latlon_to_grid6(41.0123, -80.0456)
    assert (out["lat"], out["lon"]) == grid_to_latlon(out["grid6"])
    assert set(out) == {"reporter_id", "grid6", "lat", "lon", "a_level", "built_at_utc"}
```

(The coordinates above are made up and match no station.)

- [ ] **Step 2: Run them and see them fail.**
- [ ] **Step 3: Add the seed rows, `load_positions`, `position_rows` and the verb.**
- [ ] **Step 4: Run the suite.**
- [ ] **Step 5: Commit** — `sigmondb: KFS-OMNI, N8UR, AC0G/B1A receivers; receiver-positions loads GPSDO positions from a private file`

---

### Task 8: The hourly chain

**Files:**
- Create: `sigmondb-attribute.service`, `sigmondb-solve.service`
- Modify: `sigmondb-publish.service`
- Test: `tests/test_sigmondb_units.py`

The one trigger stays `sigmondb-publish.timer` (hourly, `RandomizedDelaySec=300`,
`Persistent=true`), unchanged.  The services order themselves:

- `sigmondb-attribute.service`: `Type=oneshot`, `User=wsprdaemon`, `Group=wsprdaemon`,
  `WorkingDirectory=/home/wsprdaemon/wsprdaemon-server`,
  `ExecStart=/opt/wsprdaemon-server/venv/bin/python -m sigmondb attribute`,
  `After=clickhouse-server.service`, `TimeoutStartSec=30min`, `NoNewPrivileges=true`,
  `SyslogIdentifier=sigmondb-attribute`.  No timer of its own.
- `sigmondb-solve.service`: the same shape; `ExecStart=... -m sigmondb solve --from-db --write-db`;
  `Wants=sigmondb-attribute.service`; `After=sigmondb-attribute.service clickhouse-server.service`;
  `TimeoutStartSec=15min`; `SyslogIdentifier=sigmondb-solve`.
- `sigmondb-publish.service`: add `Wants=sigmondb-solve.service` and
  `After=sigmondb-solve.service` beside its existing lines.

Why this shape: when the timer starts publish, `Wants=` pulls solve and attribute into the same
transaction, and `After=` on oneshot units makes each wait for the previous one to exit.  So the
order runs attribute, solve, publish.  `Wants=`, not `Requires=`: a failed attribute run still
lets solve run on the previous run's names, and a failed solve still lets publish refresh the
public tables from the solutions already stored.  This also gives the ground-wave solver the
schedule it has lacked since 11-08.

- [ ] **Step 1: Write the failing test**

```python
def test_publish_pulls_solve_pulls_attribute_in_order():
    pub, sol, att = (_unit(n) for n in ("sigmondb-publish", "sigmondb-solve", "sigmondb-attribute"))
    assert "sigmondb-solve.service" in pub["Unit"]["Wants"] and "sigmondb-solve.service" in pub["Unit"]["After"]
    assert "sigmondb-attribute.service" in sol["Unit"]["Wants"] and "sigmondb-attribute.service" in sol["Unit"]["After"]
    assert "Requires" not in pub["Unit"] and "Requires" not in sol["Unit"]
    assert sol["Service"]["ExecStart"].endswith("solve --from-db --write-db")
    assert att["Service"]["ExecStart"].endswith("-m sigmondb attribute")
    assert not os.path.exists(os.path.join(ROOT, "sigmondb-attribute.timer"))
```

`_unit(name)` reads `<repo>/<name>.service` with `configparser.ConfigParser(strict=False, interpolation=None)`,
case kept (`optionxform = str`).

- [ ] **Step 2: Run it and see it fail.**  **Step 3: Write the units.**  **Step 4: Run the suite.**
- [ ] **Step 5: Commit** — `sigmondb: hourly chain attribute -> solve -> publish on the publish timer`

---

### Task 9: First run on wd30 (only on Michael's go)

Nothing in this task runs until Michael says so.  It needs the commits on wd30's checkout
(`/home/wsprdaemon/wsprdaemon-server`), which means a push, and the brief forbids one now.

- [ ] **Step 1: Tell Rob.**  He shares this repo.  Post one claude-bus message (format per
  `/srv/hamsci/claude-bus/README.md`) naming the new table, verb and units on wd30.
- [ ] **Step 2: Bring the checkout up to date** on wd30 (`git pull --ff-only` as `wsprdaemon`).
- [ ] **Step 3: Load the receiver seed.**
  `python -m sigmondb seed --receivers sigmondb/seed/receivers.toml --snapshot 2026-11-13 --out /tmp/sigmondb-seed && python -m sigmondb load --dir /tmp/sigmondb-seed`.
  Expect `receiver.jsonl: 9 rows`; `SELECT uniqExact(reporter_id) FROM sigmondb.receiver FINAL`
  gives 9.  (`load` also creates the `attribution` table.)
- [ ] **Step 3a: The controller builds and copies the positions file.**  On the devbox, from
  `/home/mjh/hamsci/tasks/codar-map/stations.json`, write
  `{reporter_id: {lat, lon, pos_sigma_m: 10.0}}` for every station that has a lat and lon, with
  this map: `AC0G-B4 -> AC0G/B4`, `AC0G-ND -> AC0G/ND`, `K3LR -> K3LR/D`, `W2NAF -> W2NAF`,
  `UACNJ -> UACNJ`, `WB6CXC-7 -> WB6CXC/7`, `KFS -> KFS-OMNI`.  Write it to the session
  scratchpad, never into a repo.  Copy it to wd30 as
  `~wsprdaemon/.config/sigmondb-receiver-positions.json`, owner `wsprdaemon`, mode 600, then
  delete the scratch copy.  (10 m: a GPSDO fix with margin; stations.json gives no sigma.)
- [ ] **Step 3b: Load the positions.**  As `wsprdaemon`:
  `python -m sigmondb receiver-positions --file ~/.config/sigmondb-receiver-positions.json`.
  Expect `7 updated, 0 unchanged, 0 unknown`.  Check with
  `SELECT reporter_id, argMax(pos_source, (valid_from, version)) FROM sigmondb.receiver FINAL GROUP BY 1`:
  those seven read `gpsdo`, N8UR and AC0G/B1A read `typed_grid`.  Then run the publish chain
  once (Step 6) and confirm `sigmondb_public.receivers` still shows grid6 centres only.
- [ ] **Step 4: Dry run, timed.**
  `/usr/bin/time -v /opt/wsprdaemon-server/venv/bin/python -m sigmondb attribute --out /tmp/attr.jsonl`.
  Record wall time and maximum resident size.  If wall time passes 20 min, stop and report: the
  window or `--n-fake` needs a decision.  Then read `/tmp/attr.jsonl`:
  - Any `mode_policy = default` row with status in `NAMED_STATUSES`?  The study, on the same
    method and data to 11-13 00:46Z, named nothing.  Since then live rows from N8UR and
    AC0G/B1A have arrived.  A name may now be real, or may come from something the guards miss.
    Either way, stop and show Michael the row (sites, stations, modes, spread, rival, `p_null`)
    before Step 5.
  - The KFS 4.785 MHz rows, now counted at `KFS-OMNI`, must list twin and rung set-asides and
    read `unattributable`.  No row may name `KFS-MAIN` or `KFS/MAIN` as a station.
- [ ] **Step 5: Install the units.**  Copy the three `.service` files to `/etc/systemd/system/`,
  run `systemctl daemon-reload` and `systemd-analyze verify` on each.
  `systemctl list-dependencies sigmondb-publish.service` must show solve and attribute.
- [ ] **Step 6: Run the chain once by hand.**  `systemctl start sigmondb-publish.service`.  Then
  `journalctl -u sigmondb-attribute -u sigmondb-solve -u sigmondb-publish --since -1h` shows the
  three in that order, each exiting 0.
- [ ] **Step 7: Check the tables.**
  - `SELECT mode_policy, status, count() FROM sigmondb.attribution FINAL WHERE run_at = (SELECT max(run_at) FROM sigmondb.attribution) GROUP BY 1, 2`
  - `SELECT solver_version, count(), countIf(tx_id != '') FROM sigmondb.solution FINAL GROUP BY 1`
    shows `groundwave-3` rows; its `tx_id` count equals the number of attached names.
  - `sigmondb_public.transmitter_offsets` on WD10/WD20 holds no CODAR row unless Step 4 found a
    name that Michael accepted.
- [ ] **Step 8: Watch the next timer run** (top of the hour plus up to 5 min) and repeat Step 7.

---

## Follow-on (not a task here)

- **KFS's clients must report under the radiod's own reporter id.**  KFS-MAIN transports data
  for the KFS radiods and receives nothing, yet its codar-sounder rows arrive labelled `KFS-MAIN`
  and `KFS/MAIN`.  The clients should send the reporter id of the radiod that heard the signal.
  The controller raises this with Rob.  When the real id exists, rename the provisional
  `KFS-OMNI` receiver row to it (a new identity row, declared), and `_at_radiod` keeps old rows
  counting at the same receiver.
