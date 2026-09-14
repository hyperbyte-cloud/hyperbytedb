#!/bin/sh
# Splits DURING writes: does a region hand-over lose acknowledged writes?
#
# Every existing suite misses this intersection. The churn suite writes
# continuously but only ever touches 10 series against a split threshold of 25,
# so it NEVER splits. The scaledown suite splits 127 times but seeds once and
# then goes quiet. Nothing has exercised a split while writes are landing.
#
# That matters because of how a hand-over is sequenced. A split publishes its
# child `Staging`; the donor keeps serving that range, so writes for it keep
# landing on the DONOR. The bulk copy runs, `ClearVerified` commits, and cutover
# hands the range to the child. Any write that arrived at the donor after the
# copy read its rows is on the donor and not in the child -- and after cutover
# nothing reads the donor for that range, because every region query carries a
# series_id predicate. Reconciliation will not move it either: that is driven by
# `transfer_outstanding`, which cutover has already cleared.
#
# So the hypothesis is silent loss, proportional to write rate during a split.
# This suite is deliberately topology-STABLE -- no scaling, no kills -- so a
# failure is attributable to the split hand-over and nothing else.
#
# The dataset is built so the arithmetic is exact: every point has a unique host
# AND a unique timestamp, so the row count must equal the number of points the
# cluster acknowledged with 204. No fewer (loss) and no more (a region staged
# twice; apply_transfer_push is not idempotent).
#
# Prerequisites: deploy/kind/setup.sh hdb-reset --sharded  (6 replicas)
set -eu

SCRIPT_DIR=$(CDPATH= cd -- "$(dirname "$0")" && pwd)
PROJECT_ROOT=$(CDPATH= cd -- "$SCRIPT_DIR/../.." && pwd)

KUBE_CTX=${KUBE_CTX:-kind-hyperbytedb}
NS=${NS:-hyperbytedb}
DB=${E2E_DB:-splitwrite}
RP=${E2E_RP:-autogen}
HEARTBEAT_INTERVAL_SECS=${HEARTBEAT_INTERVAL_SECS:-10}
SPLIT_MERGE_INTERVAL_SECS=${SPLIT_MERGE_INTERVAL_SECS:-30}
HEADLESS_SVC=${HEADLESS_SVC:-hyperbytedb-headless}
CLUSTER_DOMAIN=${CLUSTER_DOMAIN:-svc.cluster.local}
HDB_PORT=${HDB_PORT:-8086}
IN_CLUSTER=${IN_CLUSTER:-0}

MEAS=${MEAS:-sp}
WRITER_BATCH=${WRITER_BATCH:-20}
WRITE_SECS=${WRITE_SECS:-180}
SETTLE_SECS=${SETTLE_SECS:-600}
FULL_REPLICAS=6

REPORT_DIR=${REPORT_DIR:-$PROJECT_ROOT/../.scratch/linear-scaling-p1}
REPORT_FILE=$REPORT_DIR/split-write-e2e-report.md
E2E_REPORT_TITLE="Sharding Split-Under-Write E2E Report"
E2E_CLUSTER_LABEL="6 nodes, topology stable, splits forced under continuous writes, RF=2"

. "$SCRIPT_DIR/lib/e2e-common-posix.sh"

WRITER_PID=""
ACK_FILE=$REPORT_DIR/.splitwrite-acked.$$
STOP_FILE=$REPORT_DIR/.splitwrite-stop.$$

cleanup() {
  [ -n "$WRITER_PID" ] && kill "$WRITER_PID" 2>/dev/null || true
  rm -f "$ACK_FILE" "$STOP_FILE"
  teardown_port_forwards
}
trap cleanup EXIT

acked_total() {
  if [ -s "$ACK_FILE" ]; then
    awk '{s+=$1} END {print s+0}' "$ACK_FILE"
  else
    echo 0
  fi
}

# Every point is a NEW series, so cardinality grows monotonically and the
# measurement keeps crossing region_split_series. That is what forces splits to
# happen WHILE writes are in flight, which is the whole point of this suite.
#
# Unique host and unique timestamp per point: no two writes can collide and
# silently overwrite, which would look like loss.
run_writer() {
  : > "$ACK_FILE"
  base=$(write_ts)
  n=0
  while [ ! -f "$STOP_FILE" ]; do
    body=$(python3 -c "
base=$base; n=$n; b=$WRITER_BATCH; m='$MEAS'
print('\n'.join('{},host=s{} value=1 {}'.format(m, n+i, base + n + i) for i in range(b)))
" 2>/dev/null) || body=""
    [ -z "$body" ] && continue
    code=$(curl_write 0 "$body" 2>/dev/null || echo 000)
    if [ "$code" = "204" ]; then
      echo "$WRITER_BATCH" >> "$ACK_FILE"
    fi
    n=$((n + WRITER_BATCH))
  done
}

splits_now() {
  v=$(metric_max_across_pods 'hyperbytedb_shard_splits_total' "$FULL_REPLICAS")
  case "$v" in
    ''|*[!0-9]*) echo 0 ;;
    *) echo "$v" ;;
  esac
}

main() {
  init_goals
  log "Split-under-write E2E — forcing splits while a writer runs, topology stable"
  setup_port_forwards

  if wait_for_full_cluster "$FULL_REPLICAS"; then
    pass P0 "cluster ${FULL_REPLICAS}/${FULL_REPLICAS} healthy"
  else
    fail P0 "cluster not healthy (run: ./deploy/kind/setup.sh hdb-reset --sharded)"
    write_report; exit 1
  fi

  curl -sf -X POST "$(api_url 0)/query" --data-urlencode "q=DROP DATABASE ${DB}" >/dev/null 2>&1 || true
  sleep 3
  curl -sf -X POST "$(api_url 0)/query" --data-urlencode "q=CREATE DATABASE ${DB}" >/dev/null 2>&1 || true

  splits_before=$(splits_now)
  rm -f "$STOP_FILE"
  run_writer &
  WRITER_PID=$!
  log "writer running for ${WRITE_SECS}s (batch=${WRITER_BATCH}, every point a new series)"

  waited=0
  while [ "$waited" -lt "$WRITE_SECS" ]; do
    sleep 15
    waited=$((waited + 15))
    log "  t=${waited}s acked=$(acked_total) splits=$(splits_now)"
  done

  acked_at_stop=$(acked_total)
  touch "$STOP_FILE"
  wait "$WRITER_PID" 2>/dev/null || true
  WRITER_PID=""

  if [ "$acked_at_stop" -gt 0 ]; then
    pass P1 "writer acknowledged ${acked_at_stop} points"
  else
    fail P1 "writer acknowledged nothing — window not exercised"
    write_report; exit 1
  fi

  splits_after=$(splits_now)
  gained=$((splits_after - splits_before))
  # Vacuity guard: without splits during the write this suite proves nothing.
  if [ "$gained" -gt 0 ]; then
    pass P2 "${gained} splits occurred while writing (${splits_before} → ${splits_after})"
  else
    fail P2 "no splits occurred — the hand-over window was never exercised, so P4 is vacuous"
  fi

  # Settle: the point is whether the writes SURVIVE the hand-over, not how fast
  # they become visible, so poll to a stable value before judging.
  log "settling up to ${SETTLE_SECS}s"
  rows=$(wait_for_query_count 0 "SELECT COUNT(value) FROM ${MEAS}" "$acked_at_stop" "$SETTLE_SECS")

  case "$rows" in
    ''|*[!0-9]*)
      fail P3 "UNREADABLE: count query returned [${rows}]"
      ;;
    *)
      if [ "$rows" -eq "$acked_at_stop" ]; then
        pass P3 "exactly-once under splits: ${rows} rows == ${acked_at_stop} acknowledged"
      elif [ "$rows" -lt "$acked_at_stop" ]; then
        fail P3 "LOST $((acked_at_stop - rows)) of ${acked_at_stop} acknowledged writes (rows=${rows})"
      else
        fail P3 "DUPLICATED $((rows - acked_at_stop)) rows (rows=${rows} acked=${acked_at_stop})"
      fi
      ;;
  esac

  rf=$(wait_for_regions_rf_ok 2 "$((HEARTBEAT_INTERVAL_SECS * 12))")
  if [ "$rf" = "OK" ]; then
    pass P4 "all regions RF>=2"
  else
    fail P4 "RF violation: $rf"
  fi

  staging=$(curl_api 0 "/internal/shard/map" | python3 -c "
import sys,json
try: d=json.load(sys.stdin)
except Exception: print('ERR'); raise SystemExit
regs=[r for sp in d.get('spaces',[]) for r in sp['regions']]
bad=[(r['region_id'], r.get('staged_from')) for r in regs if r.get('state')=='staging']
print('OK' if not bad else 'STAGING:'+repr(bad[:8]))
" 2>/dev/null || echo ERR)
  if [ "$staging" = "OK" ]; then
    pass P5 "no region left staging"
  else
    fail P5 "regions stuck staging after settle: $staging"
  fi

  write_report
  if [ "$FAILURES" -eq 0 ]; then
    log "SPLIT-UNDER-WRITE E2E PASSED"
    exit 0
  fi
  err "SPLIT-UNDER-WRITE E2E FAILED ($FAILURES)"
  exit 1
}

main "$@"
