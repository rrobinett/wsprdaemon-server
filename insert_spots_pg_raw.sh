#!/bin/bash
# Insert wsprdaemon.spots_pg_raw -> wsprdaemon.spots_new with column translation
# Monthly chunks from 2020-03 through 2025-04

HOST="localhost"
USER="chadmin"
PASS="ch2025wd"
SRC="wsprdaemon.spots_pg_raw"
DST="wsprdaemon.spots_new"
CHECKPOINT_DIR="/tmp/insert_spots_pg_raw_checkpoints"
LOGFILE="/tmp/insert_spots_pg_raw_$(date +%Y%m%d_%H%M%S).log"

mkdir -p "$CHECKPOINT_DIR"

log() { echo "[$(date '+%Y-%m-%d %H:%M:%S')] $1" | tee -a "$LOGFILE"; }

log "Starting $SRC -> $DST"
log "Checkpoints: $CHECKPOINT_DIR"

START_YEAR=2020; START_MONTH=3
END_YEAR=2025;   END_MONTH=4

year=$START_YEAR
month=$START_MONTH

while [[ $year -lt $END_YEAR || ( $year -eq $END_YEAR && $month -le $END_MONTH ) ]]; do
    YYYYMM="${year}$(printf '%02d' $month)"
    CHECKPOINT="$CHECKPOINT_DIR/${YYYYMM}.done"

    if [[ -f "$CHECKPOINT" ]]; then
        log "SKIP $YYYYMM (already done)"
    else
        START="${year}-$(printf '%02d' $month)-01"
        if [[ $month -eq 12 ]]; then
            END="$((year+1))-01-01"
        else
            END="${year}-$(printf '%02d' $((month+1)))-01"
        fi

        log "Inserting $START to $END ..."

        clickhouse-client -h $HOST --user $USER --password $PASS --query "
INSERT INTO $DST
(time, band, rx_sign, rx_lat, rx_lon, rx_loc, tx_sign, tx_lat, tx_lon, tx_loc,
 distance, azimuth, rx_azimuth, frequency, power, snr, drift, version, code,
 frequency_mhz, rx_id, v_lat, v_lon, c2_noise, sync_quality, dt, decode_cycles,
 jitter, rms_noise, blocksize, metric, osd_decode, nhardmin, ipass,
 proxy_upload, ov_count, rx_status, band_m)
SELECT
    toDateTime(time), toInt16(band),
    rx_id, toFloat32(rx_lat), toFloat32(rx_lon), rx_grid, tx_call,
    toFloat32(tx_lat), toFloat32(tx_lon), tx_grid,
    toInt32(km), toFloat32(tx_az), toFloat32(rx_az),
    toUInt64(freq * 1000000), toInt8(tx_dBm), toInt8(SNR), toInt8(drift),
    CAST(NULL AS Nullable(String)),
    CASE mode WHEN 2 THEN 1 WHEN 15 THEN 2 WHEN 3 THEN 3
              WHEN 6 THEN 4 WHEN 16 THEN 5 WHEN 31 THEN 8 ELSE 0 END,
    freq, receiver, toFloat32(v_lat), toFloat32(v_lon), toFloat32(c2_noise),
    toUInt16(ifNull(sync_quality,0)), toFloat32(dt), toUInt32(ifNull(decode_cycles,0)),
    toInt16(ifNull(jitter,0)), toFloat32(rms_noise), toUInt16(ifNull(blocksize,0)),
    toInt16(ifNull(metric,0)), toUInt8(ifNull(osd_decode,0)), toUInt16(ifNull(nhardmin,0)),
    toUInt8(ifNull(ipass,0)), toUInt8(0), toUInt32(ifNull(ov_count,0)),
    'No Info', toInt16(band)
FROM $SRC
WHERE time >= '$START' AND time < '$END'" 2>&1 | tee -a "$LOGFILE"

        if [[ ${PIPESTATUS[0]} -eq 0 ]]; then
            touch "$CHECKPOINT"
            log "OK $YYYYMM"
        else
            log "FAILED $YYYYMM - stopping"
            exit 1
        fi
    fi

    # Advance month
    if [[ $month -eq 12 ]]; then
        month=1; year=$((year+1))
    else
        month=$((month+1))
    fi
done

log "Done. Row counts:"
clickhouse-client -h $HOST --user $USER --password $PASS --query "
SELECT '$SRC' AS tbl, count() FROM $SRC
UNION ALL
SELECT '$DST', count() FROM $DST
FORMAT TabSeparated"
