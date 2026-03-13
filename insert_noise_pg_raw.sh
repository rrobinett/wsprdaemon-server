#!/bin/bash
# Insert wsprdaemon.noise_pg_raw -> wsprdaemon.noise_new with column translation
# Monthly chunks from 2020-06 through 2025-04

HOST="localhost"
USER="chadmin"
PASS="ch2025wd"
SRC="wsprdaemon.noise_pg_raw"
DST="wsprdaemon.noise_new"
CHECKPOINT_DIR="/tmp/insert_noise_pg_raw_checkpoints"
LOGFILE="/tmp/insert_noise_pg_raw_$(date +%Y%m%d_%H%M%S).log"

mkdir -p "$CHECKPOINT_DIR"

log() { echo "[$(date '+%Y-%m-%d %H:%M:%S')] $1" | tee -a "$LOGFILE"; }

log "Starting $SRC -> $DST"

START_YEAR=2020; START_MONTH=6
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
(time, site, receiver, rx_loc, band, rms_level, c2_level, ov)
SELECT
    toDateTime(time),
    site,
    receiver,
    rx_grid                         AS rx_loc,
    band,
    toFloat32(rms_level),
    toFloat32(c2_level),
    toInt32(ifNull(ov, 0))
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
