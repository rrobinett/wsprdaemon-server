#!/bin/bash
#
# ch-sync-peers.sh - Reconcile ClickHouse tables with the peer WD servers.
#
# Every WD server ingests independently (wsprnet scraper, gateway uploads, and on
# WD10 the pskreporter loader).  Outages and scraper gaps leave each host with a
# slightly different set of rows.  This script makes the tables converge:
#
#   For each table and each peer, compare the number of distinct rows per hour
#   over a lookback window.  For every hour where the counts differ, pull from the
#   peer only the rows this host does not have (matched by a hash of the row's
#   identity), using ClickHouse's remote() table function.
#
# Row identity:
#   * ReplacingMergeTree tables on the peer: hash of the peer's ORDER BY key, i.e.
#     exactly what ReplacingMergeTree dedups on.  Rows whose non-key columns differ
#     between hosts (e.g. psk.spots.ingested_at) are the same row.
#   * Other engines (pskreporter.rx on WD10 is a plain MergeTree): hash of the full
#     row, so distinct rows with equal keys are all kept.
#
# Copies are therefore idempotent and safe to repeat; the local tables are all
# ReplacingMergeTree, so anything copied twice collapses on merge.
#
# Usage:
#   ch-sync-peers [--dry-run] [--lookback HOURS] [--since 'YYYY-MM-DD [HH:MM:SS]']
#                 [--tables db.t1,db.t2] [--peers host1,host2] [--verbose]
#   ch-sync-peers --version | --help
#
# Config: /etc/wsprdaemon/ch-sync-peers.conf (bash syntax, all optional):
#   PEERS="wd10 wd20"             default: wd10 wd20 wd30 minus this host
#   TABLES="wspr.rx ..."          default: see below
#   LOOKBACK_HOURS=48             default window for a regular run
#   CUTOFF_MINUTES=20             ignore the most recent N minutes (still arriving)
#   CHUNK_HOURS=48                process long windows in slices of this many hours
#   declare -A TABLE_PEERS; TABLE_PEERS[pskreporter.rx]="wd10"
#                                 restrict a table to specific source peers
#
# Credentials come from /etc/wsprdaemon/clickhouse.conf (CLICKHOUSE_ROOT_ADMIN_*);
# the same admin account must exist on every peer.

set -uo pipefail

VERSION="1.0.1"
CONF="/etc/wsprdaemon/ch-sync-peers.conf"
CH_CONF="/etc/wsprdaemon/clickhouse.conf"
LOG="/var/log/wsprdaemon/ch-sync-peers.log"
LOCK_DIR="/run/lock"

ALL_HOSTS="wd10 wd20 wd30"
PEERS=""
TABLES="wspr.rx wsprdaemon.spots wsprdaemon.noise psk.spots pskreporter.rx"
LOOKBACK_HOURS=48
CUTOFF_MINUTES=20
CHUNK_HOURS=48           # compare/copy at most this many hours per query batch (bounds memory on big tables)
declare -A TABLE_PEERS
TABLE_PEERS[pskreporter.rx]="wd10"

DRY_RUN=0
VERBOSE=0
SINCE=""
OPT_TABLES=""
OPT_PEERS=""

usage() {
    sed -n '2,/^$/p' "$0" | sed 's/^# \{0,1\}//'
}

while (( $# > 0 )); do
    case "$1" in
        --dry-run)  DRY_RUN=1; shift ;;
        --verbose|-v) VERBOSE=1; shift ;;
        --lookback) LOOKBACK_HOURS="${2:?--lookback needs HOURS}"; shift 2 ;;
        --since)    SINCE="${2:?--since needs a date}"; shift 2 ;;
        --tables)   OPT_TABLES="${2:?--tables needs a list}"; shift 2 ;;
        --peers)    OPT_PEERS="${2:?--peers needs a list}"; shift 2 ;;
        --version)  echo "ch-sync-peers.sh v${VERSION}"; exit 0 ;;
        --help|-h)  usage; exit 0 ;;
        *) echo "ERROR: unknown option $1" >&2; usage; exit 1 ;;
    esac
done

[[ -f "$CH_CONF" ]] || { echo "ERROR: $CH_CONF not found" >&2; exit 1; }
# shellcheck disable=SC1090
source "$CH_CONF"
CH_USER="${CLICKHOUSE_ROOT_ADMIN_USER:?}"
CH_PASS="${CLICKHOUSE_ROOT_ADMIN_PASSWORD:?}"
# shellcheck disable=SC1090
[[ -f "$CONF" ]] && source "$CONF"

[[ "$LOOKBACK_HOURS" =~ ^[0-9]+$ ]] || { echo "ERROR: LOOKBACK_HOURS must be an integer" >&2; exit 1; }
[[ "$CUTOFF_MINUTES" =~ ^[0-9]+$ ]] || { echo "ERROR: CUTOFF_MINUTES must be an integer" >&2; exit 1; }
[[ "$CHUNK_HOURS" =~ ^[1-9][0-9]*$ ]] || { echo "ERROR: CHUNK_HOURS must be a positive integer" >&2; exit 1; }

SELF=$(hostname | tr 'A-Z' 'a-z')
if [[ -n "$OPT_PEERS" ]]; then
    PEERS="${OPT_PEERS//,/ }"
elif [[ -z "$PEERS" ]]; then
    PEERS=""
    for h in $ALL_HOSTS; do [[ "$h" == "$SELF" ]] || PEERS+="$h "; done
fi
[[ -n "$OPT_TABLES" ]] && TABLES="${OPT_TABLES//,/ }"

mkdir -p "$(dirname "$LOG")" 2>/dev/null
log() {
    local line
    line="[$(date -u '+%Y-%m-%d %H:%M:%S')] $*"
    echo "${line//$CH_PASS/<pw>}" | tee -a "$LOG"
}
vlog() { (( VERBOSE )) && log "$@"; return 0; }

# One lock per distinct table set, so a long one-off history copy of a single
# table does not block the regular timer runs for the other tables.
LOCK_FILE="${LOCK_DIR}/ch-sync-peers-$(echo "$TABLES" | md5sum | cut -c1-8).lock"
exec 9>"$LOCK_FILE" || { echo "ERROR: cannot open $LOCK_FILE" >&2; exit 1; }
if ! flock -n 9; then
    log "SKIP: another ch-sync-peers run holds $LOCK_FILE"
    exit 0
fi

# ---------------------------------------------------------------------------
ch() {  # run a query locally; stdout = result, non-zero on error (stderr masked)
    clickhouse-client --user "$CH_USER" --password "$CH_PASS" \
        --max_execution_time 0 --receive_timeout 7200 --send_timeout 7200 \
        --query "$1" </dev/null 2> >(sed "s/$CH_PASS/<pw>/g" >&2)
}
rem() {  # remote(peer, db.table) expression with credentials
    echo "remote('$1', $2, '$CH_USER', '$CH_PASS')"
}
sql_str() { echo "${1//\'/\\\'}"; }   # escape a string for a SQL literal

# Epoch bounds of the window, all in UTC seconds (timezone independent).
NOW_EPOCH=$(date -u +%s)
CUTOFF_EPOCH=$(( NOW_EPOCH - CUTOFF_MINUTES * 60 ))
CUTOFF_EPOCH=$(( CUTOFF_EPOCH - CUTOFF_EPOCH % 60 ))
if [[ -n "$SINCE" ]]; then
    SINCE_EPOCH=$(date -u -d "$SINCE" +%s 2>/dev/null) || { echo "ERROR: cannot parse --since '$SINCE'" >&2; exit 1; }
else
    SINCE_EPOCH=$(( CUTOFF_EPOCH - LOOKBACK_HOURS * 3600 ))
fi
SINCE_EPOCH=$(( SINCE_EPOCH - SINCE_EPOCH % 3600 ))
(( SINCE_EPOCH < CUTOFF_EPOCH )) || { echo "ERROR: empty window" >&2; exit 1; }

log "=== ch-sync-peers v${VERSION} on ${SELF}: peers=[${PEERS% }] tables=[${TABLES}] window=$(date -u -d @$SINCE_EPOCH '+%F %T')..$(date -u -d @$CUTOFF_EPOCH '+%F %T') UTC$( (( DRY_RUN )) && echo ' DRY-RUN')"

T0=$(date +%s)
TOTAL_TABLES=0; TOTAL_HOURS=0; TOTAL_COPIES=0; TOTAL_ROWS=0; TOTAL_ERRORS=0

CHUNK_SECS=$(( CHUNK_HOURS * 3600 ))
N_CHUNKS=$(( (CUTOFF_EPOCH - SINCE_EPOCH + CHUNK_SECS - 1) / CHUNK_SECS ))
CHUNK_NO=0
for (( CH_SINCE = SINCE_EPOCH; CH_SINCE < CUTOFF_EPOCH; CH_SINCE += CHUNK_SECS )); do
CH_UNTIL=$(( CH_SINCE + CHUNK_SECS )); (( CH_UNTIL > CUTOFF_EPOCH )) && CH_UNTIL=$CUTOFF_EPOCH
CHUNK_NO=$((CHUNK_NO+1))
(( N_CHUNKS > 1 )) && vlog "--- chunk ${CHUNK_NO}/${N_CHUNKS}: $(date -u -d @$CH_SINCE '+%F %H:00')..$(date -u -d @$CH_UNTIL '+%F %H:%M') UTC"
for table in $TABLES; do
    db="${table%%.*}"; tbl="${table#*.}"
    [[ "$db" != "$table" && -n "$tbl" ]] || { log "ERROR: table must be db.name, got '$table'"; TOTAL_ERRORS=$((TOTAL_ERRORS+1)); continue; }

    # Local table: engine and insertable columns.
    local_engine=$(ch "SELECT engine FROM system.tables WHERE database = '$(sql_str "$db")' AND name = '$(sql_str "$tbl")'") || { TOTAL_ERRORS=$((TOTAL_ERRORS+1)); continue; }
    if [[ -z "$local_engine" ]]; then
        vlog "SKIP $table: not present locally"; continue
    fi
    cols=$(ch "SELECT arrayStringConcat(groupArray(name), ', ') FROM (SELECT name FROM system.columns WHERE database = '$(sql_str "$db")' AND table = '$(sql_str "$tbl")' AND default_kind NOT IN ('ALIAS','MATERIALIZED') ORDER BY position)") || { TOTAL_ERRORS=$((TOTAL_ERRORS+1)); continue; }
    (( CHUNK_NO == 1 )) && TOTAL_TABLES=$((TOTAL_TABLES+1))

    peers_for_table="${TABLE_PEERS[$table]:-$PEERS}"
    for peer in $peers_for_table; do
        [[ "$peer" == "$SELF" ]] && continue
        case " $PEERS " in *" $peer "*) ;; *) continue ;; esac   # honour --peers / PEERS

        # Peer table: engine + sorting key -> identity expression.
        meta=$(ch "SELECT engine, sorting_key FROM $(rem "$peer" system.tables) WHERE database = '$(sql_str "$db")' AND name = '$(sql_str "$tbl")' FORMAT TSV" 2>/tmp/ch-sync-peers.err.$$) || {
            log "ERROR $table <- $peer: cannot read peer metadata: $(head -c 300 /tmp/ch-sync-peers.err.$$ | tr '\n' ' ')"
            rm -f /tmp/ch-sync-peers.err.$$; TOTAL_ERRORS=$((TOTAL_ERRORS+1)); continue
        }
        rm -f /tmp/ch-sync-peers.err.$$
        if [[ -z "$meta" ]]; then vlog "SKIP $table <- $peer: not present on peer"; continue; fi
        peer_engine="${meta%%$'\t'*}"; peer_key="${meta#*$'\t'}"
        if [[ "$peer_engine" == Replacing* && -n "$peer_key" ]]; then
            ident="cityHash64(${peer_key})"
        else
            ident="cityHash64(*)"
        fi
        where_win="time >= toDateTime(${CH_SINCE}) AND time < toDateTime(${CH_UNTIL})"
        hour_expr="toUnixTimestamp(toStartOfHour(time, 'UTC'))"

        declare -A lc=() pc=()
        while IFS=$'\t' read -r h c; do [[ -n "$h" ]] && lc[$h]=$c; done < <(ch "SELECT ${hour_expr} AS h, uniqExact(${ident}) AS c FROM ${table} WHERE ${where_win} GROUP BY h FORMAT TSV") || { log "ERROR $table: local hour counts failed"; TOTAL_ERRORS=$((TOTAL_ERRORS+1)); continue; }
        while IFS=$'\t' read -r h c; do [[ -n "$h" ]] && pc[$h]=$c; done < <(ch "SELECT ${hour_expr} AS h, uniqExact(${ident}) AS c FROM $(rem "$peer" "$table") WHERE ${where_win} GROUP BY h FORMAT TSV") || { log "ERROR $table <- $peer: peer hour counts failed"; TOTAL_ERRORS=$((TOTAL_ERRORS+1)); continue; }

        hours=$(printf '%s\n' "${!lc[@]}" "${!pc[@]}" | sort -un)
        n_hours=0; n_copy=0; n_rows=0
        for h in $hours; do
            n_hours=$((n_hours+1))
            l=${lc[$h]:-0}; p=${pc[$h]:-0}
            (( p == 0 || p == l )) && continue
            h_end=$(( h + 3600 )); (( h_end > CH_UNTIL )) && h_end=$CH_UNTIL
            hour_txt=$(date -u -d @$h '+%F %H:00')
            where_hour="time >= toDateTime(${h}) AND time < toDateTime(${h_end})"
            if (( DRY_RUN )); then
                if (( p > l )); then
                    log "WOULD COPY $table <- $peer $hour_txt local=$l peer=$p"
                    n_copy=$((n_copy+1)); n_rows=$((n_rows + p - l))
                else
                    vlog "WOULD CHECK $table <- $peer $hour_txt local=$l peer=$p (counts differ, peer smaller)"
                fi
                continue
            fi
            if (( l == 0 )); then
                q="INSERT INTO ${table} (${cols}) SELECT ${cols} FROM $(rem "$peer" "$table") WHERE ${where_hour}"
            else
                q="INSERT INTO ${table} (${cols}) SELECT ${cols} FROM $(rem "$peer" "$table") WHERE ${where_hour} AND ${ident} GLOBAL NOT IN (SELECT ${ident} FROM ${table} WHERE ${where_hour})"
            fi
            t1=$(date +%s)
            if ch "$q" >/dev/null 2>/tmp/ch-sync-peers.err.$$; then
                after=$(ch "SELECT uniqExact(${ident}) FROM ${table} WHERE ${where_hour}" 2>/dev/null || echo "?")
                if [[ "$after" =~ ^[0-9]+$ ]] && (( after > l )); then
                    log "COPY $table <- $peer $hour_txt local=$l peer=$p now=$after ($(( $(date +%s) - t1 ))s)"
                    n_copy=$((n_copy+1)); n_rows=$((n_rows + after - l))
                else
                    vlog "NOOP $table <- $peer $hour_txt local=$l peer=$p: peer had nothing we lack"
                fi
            else
                log "ERROR $table <- $peer $hour_txt copy failed: $(head -c 300 /tmp/ch-sync-peers.err.$$ | tr '\n' ' ')"
                TOTAL_ERRORS=$((TOTAL_ERRORS+1))
            fi
            rm -f /tmp/ch-sync-peers.err.$$
        done
        if (( n_copy > 0 || (VERBOSE && N_CHUNKS == 1) )); then
            log "$table <- $peer: $n_hours hours compared, $n_copy hours $( (( DRY_RUN )) && echo 'would be ' )copied, ~$n_rows rows added"
        fi
        TOTAL_HOURS=$((TOTAL_HOURS + n_hours)); TOTAL_COPIES=$((TOTAL_COPIES + n_copy)); TOTAL_ROWS=$((TOTAL_ROWS + n_rows))
        unset lc pc
    done
done
done

log "=== done in $(( $(date +%s) - T0 ))s: $TOTAL_TABLES tables, $TOTAL_HOURS hour-comparisons, $TOTAL_COPIES hours copied, ~$TOTAL_ROWS rows added, $TOTAL_ERRORS errors"
(( TOTAL_ERRORS == 0 ))
