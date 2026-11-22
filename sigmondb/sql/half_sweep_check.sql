-- half_sweep_check.sql: did any ground-wave solution, or any published transmitter offset,
-- draw on live codar-sounder arrivals older than 0.8.0?  SELECT only; it changes nothing.
--
-- Why: codar-sounder releases before 0.8.0 wrote delay_ns = tau + S/2, half a sweep late
-- (0.5 s at 4.463 MHz).  The solver (cli.py _from_db) and attribution now take S/2 off those
-- rows as they read them (sigmondb/half_sweep.py).  Solutions written before that change used
-- the late delays, so their fixed_delay_ns and offset_ns sit half a sweep late too.
--
-- How to run:
--   Queries 1-3 on wd30, against sigmondb:
--     clickhouse-client --multiquery < sigmondb/sql/half_sweep_check.sql
--   (query 4 then fails on wd30, which holds no sigmondb_public; that is expected).
--   Query 4 on WD10 and on WD20, where publish.py writes sigmondb_public.  Run it alone there.
--
-- "Old" below means: detector_version "codar-sounder/X.Y[.Z]..." with X.Y.Z < 0.8.0, read the
-- way half_sweep.codar_version reads it (a suffix such as +g1234 parses to its base, a missing
-- patch reads 0).  Back-fill ("offline-2026-11"), chirp-recorder and 0.8.0 or later never count.
-- Only arrivals the solver reads count: origin = 't6_anchor' and u_epoch_ns <= 1000.


-- 1. wd30.  The old live arrivals themselves, per reporter, group and version.
SELECT
    reporter_id,
    group_key,
    detector_version,
    count()   AS n_old_arrivals,
    min(time) AS first_time,
    max(time) AS last_time
FROM sigmondb.arrival FINAL
WHERE match(detector_version, '^codar-sounder/[0-9]+\\.[0-9]+')
  AND toUInt32OrZero(extract(detector_version, '^codar-sounder/([0-9]+)\\.')) * 1000000
    + toUInt32OrZero(extract(detector_version, '^codar-sounder/[0-9]+\\.([0-9]+)')) * 1000
    + toUInt32OrZero(extract(detector_version, '^codar-sounder/[0-9]+\\.[0-9]+\\.([0-9]+)'))
    < 8000
  AND origin = 't6_anchor' AND u_epoch_ns IS NOT NULL AND u_epoch_ns <= 1000
GROUP BY reporter_id, group_key, detector_version
ORDER BY reporter_id, group_key, detector_version;


-- 2. wd30.  sigmondb.solution rows that drew on old live arrivals.
--
-- A solution keeps only a digest of its inputs, so this query matches arrivals to solutions:
-- the group named in cluster_key ("<reporter>|<group_key>|<delay in us>"), a time inside the
-- solution's window, and a delay within 500 us of its fixed_delay_ns.  An old solution's
-- fixed_delay_ns came from the late delays, so the arrival's stored delay_ns is the right one
-- to compare.  The reporter is NOT required to match: the solver counts a carried arrival
-- (KFS-MAIN) at the receiver whose radiod heard it (KFS-OMNI), so a solution's reporter_ids
-- can differ from the arrival's reporter_id.  n_old_same_reporter counts the direct matches;
-- a row with n_old_same_reporter = 0 deserves a look at arrival_reporters.
-- No output alias repeats a column name (tx, w_from, w_to, fixed_ns, ...), so the s.-qualified
-- names in WHERE and countIf resolve to columns under the old analyzer as well as the new.
WITH old_arrivals AS (
    SELECT reporter_id, group_key, time, delay_ns, detector_version
    FROM sigmondb.arrival FINAL
    WHERE match(detector_version, '^codar-sounder/[0-9]+\\.[0-9]+')
      AND toUInt32OrZero(extract(detector_version, '^codar-sounder/([0-9]+)\\.')) * 1000000
        + toUInt32OrZero(extract(detector_version, '^codar-sounder/[0-9]+\\.([0-9]+)')) * 1000
        + toUInt32OrZero(extract(detector_version, '^codar-sounder/[0-9]+\\.[0-9]+\\.([0-9]+)'))
        < 8000
      AND origin = 't6_anchor' AND u_epoch_ns IS NOT NULL AND u_epoch_ns <= 1000
),
solutions AS (
    SELECT solve_id, cluster_key, tx_id, window_from, window_to, reporter_ids,
           fixed_delay_ns, offset_ns, offset_sigma_ns, n_arrivals, solver_version,
           splitByChar('|', cluster_key)[2] AS gkey
    FROM sigmondb.solution FINAL
    WHERE method = 'ground_wave'
)
SELECT
    s.solve_id,
    s.cluster_key,
    any(s.tx_id)                              AS tx,
    any(s.window_from)                        AS w_from,
    any(s.window_to)                          AS w_to,
    any(s.reporter_ids)                       AS solution_reporters,
    any(s.fixed_delay_ns)                     AS fixed_ns,
    any(s.offset_ns) / 1e6                    AS offset_ms,
    any(s.offset_sigma_ns) / 1e6              AS sigma_ms,
    any(s.n_arrivals)                         AS n_solution_arrivals,
    count()                                   AS n_old_arrivals,
    countIf(has(s.reporter_ids, a.reporter_id)) AS n_old_same_reporter,
    groupUniqArray(a.reporter_id)             AS arrival_reporters,
    groupUniqArray(a.detector_version)        AS old_versions,
    any(s.solver_version)                     AS solver
FROM solutions AS s
INNER JOIN old_arrivals AS a ON a.group_key = s.gkey
WHERE a.time >= s.window_from AND a.time <= s.window_to
  AND abs(a.delay_ns - s.fixed_delay_ns) <= 500000
GROUP BY s.solve_id, s.cluster_key
ORDER BY w_to, s.cluster_key;


-- 3. wd30.  Of those solutions, the ones publish.py could have put in transmitter_offsets:
-- a named tx_id with an offset and a sigma (publish.build_transmitter_offsets).  publish.py
-- takes the latest such solution per tx_id, by window_to, within 3 h of each hourly build, and
-- drops solutions inside an active set-aside window; this query does not replay those rules,
-- so it lists candidates, an upper bound.  last_confirmed_utc is the value a published row
-- would carry; query 4 matches on (tx_id, last_confirmed_utc).  latest_for_tx = 1 when no later
-- solution with an offset exists for that tx_id, so the current build may still hold it.
WITH old_arrivals AS (
    SELECT group_key, time, delay_ns
    FROM sigmondb.arrival FINAL
    WHERE match(detector_version, '^codar-sounder/[0-9]+\\.[0-9]+')
      AND toUInt32OrZero(extract(detector_version, '^codar-sounder/([0-9]+)\\.')) * 1000000
        + toUInt32OrZero(extract(detector_version, '^codar-sounder/[0-9]+\\.([0-9]+)')) * 1000
        + toUInt32OrZero(extract(detector_version, '^codar-sounder/[0-9]+\\.[0-9]+\\.([0-9]+)'))
        < 8000
      AND origin = 't6_anchor' AND u_epoch_ns IS NOT NULL AND u_epoch_ns <= 1000
),
offsets AS (
    SELECT solve_id, cluster_key, tx_id, window_from, window_to, fixed_delay_ns, offset_ns,
           offset_sigma_ns, n_stations, reporter_ids,
           splitByChar('|', cluster_key)[2] AS gkey
    FROM sigmondb.solution FINAL
    WHERE method = 'ground_wave' AND tx_id != ''
      AND offset_ns IS NOT NULL AND offset_sigma_ns IS NOT NULL
),
newest AS (
    SELECT tx_id, max(window_to) AS newest_window_to FROM offsets GROUP BY tx_id
),
hit AS (
    SELECT s.solve_id AS solve_id, s.cluster_key AS cluster_key, count() AS n_old_arrivals
    FROM offsets AS s
    INNER JOIN old_arrivals AS a ON a.group_key = s.gkey
    WHERE a.time >= s.window_from AND a.time <= s.window_to
      AND abs(a.delay_ns - s.fixed_delay_ns) <= 500000
    GROUP BY s.solve_id, s.cluster_key
)
SELECT
    o.tx_id,
    o.solve_id,
    o.window_to,
    toUnixTimestamp64Milli(o.window_to) / 1000.0 AS last_confirmed_utc,
    o.offset_ns / 1e6                            AS offset_ms,
    o.offset_sigma_ns / 1e6                      AS sigma_ms,
    o.n_stations,
    arraySort(arrayDistinct(o.reporter_ids))     AS confirming_reporters,
    h.n_old_arrivals,
    o.window_to = n.newest_window_to             AS latest_for_tx
FROM offsets AS o
INNER JOIN hit AS h ON h.solve_id = o.solve_id AND h.cluster_key = o.cluster_key
INNER JOIN newest AS n ON n.tx_id = o.tx_id
ORDER BY o.tx_id, o.window_to;


-- 4. WD10 and WD20 (not wd30).  What sigmondb_public.transmitter_offsets holds now.
-- publish.py replaces the table whole every hour, so it shows the latest build only: a row
-- published and then superseded leaves no trace here.  A row whose (tx_id, last_confirmed_utc)
-- equals one from query 3 came from a solution that drew on old live arrivals; its offset_ms
-- then sits about half a sweep (0.5 s at 4.463 MHz) from the true value.
SELECT
    tx_id,
    kind,
    offset_ms,
    sigma_ms,
    last_confirmed_utc,
    toDateTime(toUInt32(last_confirmed_utc), 'UTC') AS last_confirmed,
    n_confirming,
    confirming_reporters,
    toDateTime(toUInt32(built_at_utc), 'UTC')       AS built_at
FROM sigmondb_public.transmitter_offsets
ORDER BY tx_id;
