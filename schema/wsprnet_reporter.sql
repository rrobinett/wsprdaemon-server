-- wsprdaemon.wsprnet_reporter
--
-- The wsprnet.org identity a receiver posts UNDER when the WD servers
-- forward its WSPR spots on its behalf (`wsprnet_forwarder.py`).
--
-- Written at ingest from the station's own `routing.json`
-- (`wsprnet_reporter: {call, grid}`); mirrored inline in
-- `setup_wsprnet_reporter_table()` so a fresh wd self-bootstraps.
--
-- `key` is spelled EXACTLY as `wsprnet_forwarder.reporter_for()` looks it
-- up — `<rx_sign>/<rx_id>` for one receiver, or `<rx_sign>` for every
-- receiver at a site (the latter only ever written by hand, in the
-- operator's overrides file).
--
-- ⛔ rx_id ALONE IS NOT A KEY.  It is a station-local receiver name
-- (`KA9Q_DXE`, `rx888-1`), so two unrelated sites routinely carry the
-- same one.  An identity filed under a bare rx_id would silently
-- re-attribute another operator's spots to this site's callsign — real
-- spots in a public database under a call its operator never chose.
--
-- Empty on every server where no station has declared an identity: the
-- forwarder's default is each row's own rx_sign, which is correct for
-- every single-receiver site.

CREATE TABLE IF NOT EXISTS wsprdaemon.wsprnet_reporter
(
    `key`        String    CODEC(ZSTD(3)),
    `call`       String    CODEC(ZSTD(3)),
    `grid`       String    CODEC(ZSTD(3)),
    `updated_at` DateTime  CODEC(Delta(4), ZSTD(3))
)
ENGINE = ReplacingMergeTree(updated_at)
ORDER BY key
SETTINGS index_granularity = 8192;
