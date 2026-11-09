# sigmondb first proof on wd30, 2026-11-09

Michael's own server, wd30. Only the database `sigmondb` was created or written. Code under test: main at 4013d4f. This note's commit is HEAD of main after that (see `git log -1 -- docs/sigmondb-first-proof-2026-11.md`).
Proof scripts (outside the repo): /home/mjh/hamsci/tasks/chirp-registry/proof/
(`build_rows.sh`, `q.py`, `q.sh`, `counts.sh`, `solve.sh`; row files in `rows/`).

## What ran

1. `build_rows.sh` on the devbox: `seed` (HFRNet list, ka9q systable, receivers.toml, snapshot 2026-11-09),
   `backfill-codar`, `backfill-hfdl`, all into one `rows/` directory.
2. wd30: dropped the empty `sigmondb` database that a first, failed load had left (ruled by the controller),
   copied the package and rows to ~/sigmondb-trial, ran `python -m sigmondb load --dir rows`.
3. The four checks below, then `solve --from-db` at 20 and 40 us, then loaded the 20 us rows into `solution`.

The first attempt stopped on two faults (null in non-Nullable columns; the two backfill verbs overwriting
each other). Commit 4013d4f fixed both. This note covers the second run.

Row files: transmitter 196, waveform 311, receiver 6, capture 1310 codar + 1528 hfdl, arrival 10275 codar + 81336 hfdl.
The seed skipped 4 unreadable HFRNet rows (CDN, NKL, STF, VIR). The hfdl backfill skipped 5 rows with no nominal_utc.

Row counts, `count() FINAL`, after load:

| table | rows |
|---|---|
| transmitter | 196 |
| waveform | 311 |
| receiver | 6 |
| capture | 2829 |
| arrival | 91611 |
| solution | 19 (after the solver load) |

Capture shows 2829, not 2838: nine file rows share a dedup key and collapsed.

## Check 1: HFRNet listed transmitters. PASS

```
SELECT count() FROM sigmondb.transmitter FINAL WHERE network != 'hfdl' AND status = 'listed'
-> 145
SELECT count() FROM sigmondb.waveform FINAL WHERE tx_id IN (<those 145>)
-> 145
```
The seed's listed count is 145. Every one has a waveform row. (The table also holds 35 retired and 16 listed hfdl transmitters.)

## Check 2: idempotence. PASS

`count() FINAL` per table before and after a second `load --dir rows`:
transmitter 196/196, waveform 311/311, receiver 6/6, capture 2829/2829, arrival 91611/91611, solution 0/0
(solution shows 0 because the solver ran after this check).

## Check 3: heard versus day1108-13mhz/README.md. PASS, with a caveat

Query (from /home/mjh/hamsci/tasks/chirp-registry/proof/heard.tsv):
```
SELECT reporter_id, group_key, ... FROM (SELECT reporter_id, tx_id, group_key,
  maxMerge(last_time), argMaxMerge(last_level_db) FROM sigmondb.heard_state
  GROUP BY reporter_id, tx_id, group_key) GROUP BY reporter_id, group_key
```
Every group the README reports as detected appears under its station:
- ND: Rutgers 13.475 -99.26, WHOI +198.52, California, UCSB, BML, FAU 13.4499.
- K3LR: WHOI, FAU, UPR (13.500 -99.26), California, UCSB, BML.
- W2NAF: FAU, UPR.
- UACNJ: FAU, UPR, California, UCSB, -198.52, BML. No +198.52 row at UACNJ, as in the README.
- WB6CXC: California, UCSB, BML, UAF (13.500 -99.26).

Caveat: `heard` lists every group that has an arrival row, not only the README's 3 dB detections. So it holds
more groups (for example 13.475 -99.26 at K3LR, UACNJ, W2NAF; -198.52 at ND, K3LR, W2NAF; +198.52 at W2NAF and WB6CXC).
The README itself calls several of these images or unexplained. `heard` has no detection threshold. The groups are keyed
by centre and rate, with `tx_id = ''` since no one has named the radars. HFDL groups (6-13 MHz) appear for ND, B4, K3LR, UACNJ and WB6CXC.

## Check 4: ground-wave solver versus stage2 tracks. PASS at 40 us, partial at 20 us

Solver: `solve --from-db` (default 20 us) gave 19 rows; `--max-sd-us 40` gave 49 rows. Rows with both day and
night counts and both SD <= 20 us in k3lr-tracks.txt and w2naf-tracks.txt (4.463 MHz, 11-07):

| station | track median (ms) | solver 20 us (ms, SD us) | solver 40 us (ms, SD us) |
|---|---|---|---|
| K3LR | 16.707 | 16.706 (17.1) | 16.706 (17.1) |
| K3LR | 21.461 | 21.460 (14.1) | 21.460 (14.1) |
| K3LR | 25.161 | not found | 25.161 (25.3) |
| K3LR | 29.496 | 29.495 (14.9) | 29.495 (14.9) |
| K3LR | 33.887 | 33.886 (13.9) | 33.886 (13.9) |
| K3LR | 42.557 | not found | 42.556 (13.1)* |
| K3LR | 46.852 | 46.852 (19.6) | 46.852 (19.6) |
| K3LR | 51.317 | 51.317 (12.5) | 51.317 (12.5) |
| W2NAF | 12.573 | 12.572 (13.0) | 12.572 (13.0) |
| W2NAF | 46.336 | not found | 46.337 (21.7) |
| W2NAF | 50.939 | 50.939 (19.9) | 50.939 (19.9) |

(*K3LR 42.556 has SD 13.1 us, under the 20 us gate, yet appears only at 40 us. See below.)

Summary: at 20 us the solver finds K3LR 6 of 8 and W2NAF 2 of 3 (8 of 11); at 40 us it finds all 11. Every cluster it
reports sits within 1 us of the track median. The cause of the misses is in Open items (a).

The solver also reports extra clusters the tables do not list as fixed (for example K3LR 60.8 ms, a day-only row in the
table; W2NAF 3.753, 66.306 at 40 us) and, at 40 us, many 13 MHz clusters for ND, K3LR, W2NAF and WB6CXC. The 20 us run
holds 13 MHz clusters for ND (4) and WB6CXC (5) from the single 11-08 afternoon. Stage2 has no 13 MHz tables to compare.

The 20 us solution rows loaded into `sigmondb.solution`: 19 rows, method ground_wave, delay SD 9.6-19.9 us.

## Surprising

- `heard` is detection-free (see check 3), so it reads as "has any arrival", not "was detected".
- Nine capture rows collapsed on their dedup key (2838 to 2829); see Open items (b).
- 4.463 MHz clusters pool several days, which changes which clusters pass a 20 us SD gate.
- The ND 13.4750 -99.26 cluster at 497.341 ms (40 us) lies near the 500 ms sweep period; see Open items (c).

## Commands

Solver runs, on wd30 in ~/sigmondb-trial with PYTHONPATH=~/sigmondb-trial and the venv python:
```
python -m sigmondb solve --from-db --out solution.jsonl                    # 20 us (default)
python -m sigmondb solve --from-db --max-sd-us 40 --out s40/solution.jsonl  # 40 us
```
Only the 20 us rows (19) were loaded into `sigmondb.solution`. The 40 us rows (49) sit in
/home/mjh/hamsci/tasks/chirp-registry/proof/solution40.jsonl, not in the database.

## Open items

a. K3LR 42.557 ms missing at 20 us. Cause found. The solver's gate has two tests: cluster SD, and the gap between the
   night median and the day median, both against `max_sd_ns`. For K3LR 42.557 the SD is 13.1 us, but the night median
   (4 arrivals) sits 24.3 us from the day median (47 arrivals), so the median test rejects it at 20 us and passes at 40 us.
   The same test explains the rest: K3LR 29.496 (median gap 20.8 us) and W2NAF 46.337 (25.3 us); K3LR 25.161 fails the
   SD test (27.0 us). I also ran the solver on 11-07 arrivals only (the tables' window; `solve --arrivals
   proof/arrival-1107.jsonl ...`, outputs proof/sol1107-20.jsonl and sol1107-40.jsonl). At 20 us that gives K3LR 5 clusters
   (16.707, 21.461, 33.887, 46.853, 51.317) and W2NAF 2 (12.573, 50.939), so pooling days did not cause the misses
   (it differs only at the edges; 11-07 alone loses K3LR 29.496 too). At 40 us the 11-07 run finds all 11 again.
   Next step: decide whether the median-gap test should use a tolerance of its own, scaled to the night count (four night
   arrivals give a noisy median), instead of reusing `max_sd_ns`. That is a solver design call for Michael and Rob.
b. Nine capture rows collapse on the dedup key (2838 to 2829). A pass over proof/rows/capture.*.jsonl found 9 key pairs.
   All are HFDL rows for AC0G/ND at 13276 kHz, 2026-11-03 to 11-07 at 15:00Z and 19:00Z. Each pair holds two distinct
   summaries: ground station 1 (San Francisco) and ground station 4 (Riverhead) heard in the same hour on the same
   frequency, with different `expected`, `n_expected`, `n_found` and `engineering`. They are not duplicates. The capture key
   (reporter_id, centre_hz, rate_hz_per_s, time, detector_version) carries no ground station, so ReplacingMergeTree keeps one
   and drops the other. The 9 dropped summaries are lost data. Next step: add the ground station to the capture key
   (or fold it into `detector_version` or `rate_hz_per_s`), reload, and expect 2838 capture rows. This goes back to
   Task 1 (DDL) and Task 6 (HFDL rows). The arrival rows are unaffected, since each carries its own `tx_id` and delay.
c. ND 13.4750 -99.26 cluster at 497.341 ms (40 us run, 8 arrivals). A delay of 497 ms sits 2.7 ms below the 500 ms sweep
   period, and the clustering sorts delays as plain numbers with no wrap. An arrival near 0 or near 500 ms then splits
   into two clusters that never merge. Next step: cluster on a circular delay (delay modulo the sweep period, split at the
   largest empty gap), and check whether a matching cluster near 0-3 ms exists for the same group; then merge or
   discard. Only the 13 MHz clusters are at risk, since their period is 500 ms; 4.463 MHz clusters sit well inside theirs.
