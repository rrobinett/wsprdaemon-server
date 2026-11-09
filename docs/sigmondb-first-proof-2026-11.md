# sigmondb first proof on wd30, 2026-11-09

Michael's own server, wd30. Only the database `sigmondb` was created or written. Code: main at 4013d4f.
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
transmitter 196/196, waveform 311/311, receiver 6/6, capture 2829/2829, arrival 91611/91611, solution 0/0.

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

Every cluster the solver reports sits within 1 us of the track median. At 40 us it finds all 11. At 20 us it finds 8 of 11.
Two of the three misses (25.161, 46.337) carry SD 25.3 and 21.7 us in the solver, against 10-13 us in the track tables. The solver pools 11-04 to 11-08
for K3LR and 11-07 to 11-08 for W2NAF; the tables cover 11-07 only. Extra days widen the SD. K3LR 42.556, with SD 13.1 us at 40 us but absent at 20 us, points at a different cause. Here the SD of the 40 us row may be one fragment of a cluster that the 20 us run split or rejected on the day/night rule: the solver also requires day and night medians to agree
(and min 3 per part), which the extra days may break. I did not investigate further.

The solver also reports extra clusters the tables do not list as fixed (for example K3LR 60.8 ms, a day-only row in the
table; W2NAF 3.753, 66.306 at 40 us) and, at 40 us, many 13 MHz clusters for ND, K3LR, W2NAF and WB6CXC. The 20 us run
holds 13 MHz clusters for ND (4) and WB6CXC (5) from the single 11-08 afternoon. Stage2 has no 13 MHz tables to compare.

The 20 us solution rows loaded into `sigmondb.solution`: 19 rows, method ground_wave, delay SD 9.6-19.9 us.

## Surprising

- `heard` is detection-free (see check 3), so it reads as "has any arrival", not "was detected".
- Nine capture rows collapsed on their dedup key (2838 to 2829).
- 4.463 MHz clusters pool several days, which changes which clusters pass a 20 us SD gate.
- The ND 13.4750 -99.26 cluster at 497.341 ms (40 us) lies near the 500 ms sweep period, a wrap candidate.
