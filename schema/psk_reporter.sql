-- psk.reporter
--
-- The PSKReporter identity a receiver reports UNDER when the WD servers
-- forward its FT8/FT4/MSK144 spots on its behalf
-- (`pskreporter_forwarder.py`).
--
-- Written at ingest from the station's own `routing.json`
-- (`psk_reporter: {call, grid}`); mirrored inline in
-- `setup_psk_reporter_table()` so a fresh wd self-bootstraps.
--
-- `key` is spelled EXACTLY as `pskreporter_forwarder.reporter_for()`
-- looks it up — the bare `rx_sign`, which in `psk.spots` already names
-- ONE receiver (`AC0G/B4`, `W3USR/02`) rather than a site.  That differs
-- from `wsprdaemon.wsprnet_reporter`, where the receiver is a separate
-- `rx_id` column and the key is `<rx_sign>/<rx_id>`.
--
-- ⛔ THIS TABLE EXISTS TO MAKE MERGING EXPLICIT.  The forwarder's default
-- is each row's own rx_sign and grid — the identity the station uses
-- posting directly — so moving onto the proxy cannot move anyone's
-- credit.  Two receivers are reported as ONE station only when their
-- operator declares the SAME (call, grid) for both.  That is the whole
-- mechanism: "merge, and merge with what?", answered as data.
--
-- ⛔ MERGING IS NEVER INFERRED FROM A SHARED CALLSIGN PREFIX.  Until
-- 2026-10-27 the forwarder stripped the `/suffix` fleet-wide and keyed on
-- (base call, grid).  `AC0G/B4` is in Missouri and `AC0G/ND` is in North
-- Dakota, 1,100 km apart — and psk.spots additionally carried ND at B4's
-- grid — so both resolved to one instance: one map pin, and one ftlib
-- spotEquals dedup pool in which each site deleted the other's spots.
--
-- Empty on every server where no station has declared an identity.

CREATE TABLE IF NOT EXISTS psk.reporter
(
    `key`        String    CODEC(ZSTD(3)),
    `call`       String    CODEC(ZSTD(3)),
    `grid`       String    CODEC(ZSTD(3)),
    `updated_at` DateTime  CODEC(Delta(4), ZSTD(3))
)
ENGINE = ReplacingMergeTree(updated_at)
ORDER BY key
SETTINGS index_granularity = 8192;
