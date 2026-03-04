#!/bin/bash
# Insert wsprdaemon.noise_2025 -> wsprdaemon.noise_new in monthly chunks

HOST="localhost"
USER="chadmin"
PASS="ch2025wd"
SRC="wsprdaemon.noise_2025"
DST="wsprdaemon.noise_new"
CHECKPOINT_DIR="/tmp/insert_noise_2025_checkpoints"
LOGFILE="/tmp/insert_noise_2025_$(date +%Y%m%d_%H%M%S).log"

mkdir -p "$CHECKPOINT_DIR"

log() { echo "[$(date '+%Y-%m-%d %H:%M:%S')] $1" | tee -a "$LOGFILE"; }

log "Starting $SRC -> $DST"

# noise_2025 spans 2025-01 through 2025-11
for month in $(seq 1 11); do
    YYYYMM="2025$(printf '%02d' $month)"
    CHECKPOINT="$CHECKPOINT_DIR/${YYYYMM}.done"

    if [[ -f "$CHECKPOINT" ]]; then
        log "SKIP $YYYYMM (already done)"
        continue
    fi

    START="2025-$(printf '%02d' $month)-01"
    if [[ $month -eq 12 ]]; then
        END="2026-01-01"
    else
        END="2025-$(printf '%02d' $((month+1)))-01"
    fi

    log "Inserting $START to $END ..."

    clickhouse-client -h $HOST --user $USER --password $PASS --query "
INSERT INTO $DST
SELECT * FROM $SRC
WHERE time >= '$START' AND time < '$END'" 2>&1 | tee -a "$LOGFILE"

    if [[ ${PIPESTATUS[0]} -eq 0 ]]; then
        touch "$CHECKPOINT"
        log "OK $YYYYMM"
    else
        log "FAILED $YYYYMM - stopping"
        exit 1
    fi
done

log "Done. Row counts:"
clickhouse-client -h $HOST --user $USER --password $PASS --query "
SELECT '$SRC' AS tbl, count() FROM $SRC
UNION ALL
SELECT '$DST', count() FROM $DST
FORMAT TabSeparated"
