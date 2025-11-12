clickhouse-client --user default --password default --query "
WITH sample AS (
    SELECT Reporter, Date, CallSign
    FROM wsprdaemon.spots
    WHERE Date IS NOT NULL
    ORDER BY time DESC
    LIMIT 1
)
SELECT
    'wsprdaemon' as source,
    Reporter, Date, CallSign, time, dB, MHz, Band, band, distance, azimuth, rx_id, Spotnum
FROM wsprdaemon.spots
WHERE (Reporter, Date, CallSign) IN (SELECT Reporter, Date, CallSign FROM sample)

UNION ALL

SELECT
    'wsprnet' as source,
    Reporter, Date, CallSign, time, dB, MHz, Band, band, distance, azimuth, NULL as rx_id, Spotnum
FROM wsprnet.spots
WHERE (Reporter, Date, CallSign) IN (SELECT Reporter, Date, CallSign FROM sample)

FORMAT PrettyCompact
"
