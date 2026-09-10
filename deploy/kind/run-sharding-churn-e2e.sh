#!/bin/sh
# Writes DURING topology change: exactly-once under a live scale-down.
#
# Every other e2e seeds data and then moves it. Real clusters write
# continuously, so this keeps a writer running across a decommission and
# asserts exactly-once: the final row count equals the seed plus every write
# the cluster acknowledged with 204 -- no fewer, and no more.
#
# "No more" matters as much as "no fewer": apply_transfer_push is not
# idempotent, so a region staged twice duplicates rows rather than losing them.
#
# Prerequisites: deploy/kind/setup.sh hdb-reset --sharded  (6 replicas)
set -eu

SCRIPT_DIR=$(CDPATH= cd -- "$(dirname "$0")" && pwd)
PROJECT_ROOT=$(CDPATH= cd -- "$SCRIPT_DIR/../.." && pwd)

KUBE_CTX=${KUBE_CTX:-kind-hyperbytedb}
NS=${NS:-hyperbytedb}
DB=${E2E_DB:-churn}
RP=${E2E_RP:-autogen}
HEARTBEAT_INTERVAL_SECS=${HEARTBEAT_INTERVAL_SECS:-10}
SPLIT_MERGE_INTERVAL_SECS=${SPLIT_MERGE_INTERVAL_SECS:-30}
CONVERGE_WAIT_SECS=${CONVERGE_WAIT_SECS:-$((HEARTBEAT_INTERVAL_SECS * 60 + SPLIT_MERGE_INTERVAL_SECS * 2))}
HEADLESS_SVC=${HEADLESS_SVC:-hyperbytedb-headless}
CLUSTER_DOMAIN=${CLUSTER_DOMAIN:-svc.cluster.local}
HDB_PORT=${HDB_PORT:-8086}
IN_CLUSTER=${IN_CLUSTER:-0}

SEED_HOSTS=${SEED_HOSTS:-20}
SEED_POINTS=${SEED_POINTS:-25}
WRITER_BATCH=${WRITER_BATCH:-10}

FULL_REPLICAS=6
DOWN_REPLICAS=5

REPORT_DIR=${REPORT_DIR:-$PROJECT_ROOT/../.scratch/linear-scaling-p1}
REPORT_FILE=$REPORT_DIR/churn-e2e-report.md
E2E_REPORT_TITLE="Sharding Write-Churn E2E Report"
E2E_CLUSTER_LABEL="6→5 under continuous writes, RF=2"

. "$SCRIPT_DIR/lib/e2e-common-posix.sh"

WRITER_PID=""
ACK_FILE=$REPORT_DIR/.churn-acked.$$
STOP_FILE=$REPORT_DIR/.churn-stop.$$

cleanup() {
  [ -n "$WRITER_PID" ] && kill "$WRITER_PID" 2>/dev/null || true
  rm -f "$ACK_FILE" "$STOP_FILE"
  teardown_port_forwards
}
trap cleanup EXIT

EXP_SEED=$((SEED_HOSTS * SEED_POINTS))

seed_dataset() {
  curl -sf -X POST "$(api_url 0)/query" --data-urlencode "q=DROP DATABASE ${DB}" >/dev/null 2>&1 || true
  sleep 3
  curl -sf -X POST "$(api_url 0)/query" --data-urlencode "q=CREATE DATABASE ${DB}" >/dev/null 2>&1 || true
  base=$(write_ts)
  errors=0
  h=0
  while [ "$h" -lt "$SEED_HOSTS" ]; do
    body=$(python3 -c "
h=$h; p=$SEED_POINTS; base=$base
print('\n'.join('cpu,host=h%d value=1 %d' % (h, base+i) for i in range(p)))
")
    code=$(curl_write 0 "$body")
    [ "$code" = "204" ] || errors=$((errors + 1))
    h=$((h + 1))
  done
  echo "$errors"
}

# Appends to a distinct series space so it can never collide with the seed.
# Only writes the cluster ACKNOWLEDGED are counted -- a rejected write must not
# appear, and an accepted one must.
run_writer() {
  : > "$ACK_FILE"
  WRITER_BASE_TS=$(write_ts)
  n=0
  while [ ! -f "$STOP_FILE" ]; do
    # One timestamp per batch, one host per point. curl_write uses
    # precision=s, and the batch counter -- not the wall clock -- drives the
    # timestamp, so two batches inside the same second cannot collide and
    # silently overwrite each other, which would look like write loss.
    body=$(python3 -c "
base=$WRITER_BASE_TS; n=$n; b=$WRITER_BATCH
print('\n'.join('churn,host=w{} value=1 {}'.format(i, base + n) for i in range(b)))
" 2>/dev/null) || body=""
    [ -z "$body" ] && continue
    code=$(curl_write 0 "$body" 2>/dev/null || echo 000)
    if [ "$code" = "204" ]; then
      echo "$WRITER_BATCH" >> "$ACK_FILE"
    fi
    n=$((n + WRITER_BATCH))
    sleep 1
  done
}

acked_total() {
  if [ -s "$ACK_FILE" ]; then
    awk '{s+=$1} END {print s+0}' "$ACK_FILE"
  else
    echo 0
  fi
}

main() {
  init_goals
  log "Write-churn E2E — ${FULL_REPLICAS}→${DOWN_REPLICAS} with a live writer"
  setup_port_forwards

  if wait_for_full_cluster "$FULL_REPLICAS"; then
    pass W0 "cluster ${FULL_REPLICAS}/${FULL_REPLICAS} healthy"
  else
    fail W0 "cluster not healthy (run: ./deploy/kind/setup.sh hdb-reset --sharded)"
    write_report; exit 1
  fi

  errs=$(seed_dataset)
  if [ "$errs" = "0" ]; then
    pass W1 "seeded ${EXP_SEED} points, 0 write errors"
  else
    fail W1 "seed had $errs failed batches"
  fi

  rm -f "$STOP_FILE"
  run_writer &
  WRITER_PID=$!
  sleep 10
  early=$(acked_total)
  if [ "$early" -gt 0 ]; then
    pass W2 "writer live before the move (acked=${early})"
  else
    fail W2 "writer acknowledged nothing before the move"
  fi

  log "Scaling ${FULL_REPLICAS}→${DOWN_REPLICAS} with writes in flight"
  kubectl_ctx patch hyperbytedbcluster hyperbytedb --type merge \
    -p "{\"spec\":{\"replicas\":${DOWN_REPLICAS}}}" >/dev/null

  deadline=$(( $(date +%s) + CONVERGE_WAIT_SECS ))
  moved=0
  while [ "$(date +%s)" -lt "$deadline" ]; do
    if [ "$(sts_replicas)" = "$DOWN_REPLICAS" ] && [ "$(running_pod_count)" = "$DOWN_REPLICAS" ]; then
      moved=1; break
    fi
    sleep 3
  done
  if [ "$moved" = "1" ]; then
    pass W3 "scaled to ${DOWN_REPLICAS} while writing"
  else
    fail W3 "still sts=$(sts_replicas) pods=$(running_pod_count)"
  fi

  during=$(acked_total)
  if [ "$during" -gt "$early" ]; then
    pass W4 "writes were acknowledged during the move (${early} → ${during})"
  else
    fail W4 "no writes acknowledged during the move (still ${early}) — the window was not exercised"
  fi

  touch "$STOP_FILE"
  wait "$WRITER_PID" 2>/dev/null || true
  WRITER_PID=""
  acked=$(acked_total)
  wait_flush_boundary
  wait_flush_boundary

  seed_now=$(count_from_query_checked 0 "SELECT COUNT(value) FROM cpu")
  churn_now=$(count_from_query_checked 0 "SELECT COUNT(value) FROM churn")

  if [ "$seed_now" = "$EXP_SEED" ]; then
    pass W5 "seed data intact across the move (${seed_now})"
  else
    fail W5 "seed count=${seed_now} want ${EXP_SEED}"
  fi

  # The exactly-once assertion. Fewer means acknowledged writes were lost;
  # more means a region was staged twice, since apply_transfer_push is not
  # idempotent. Both are corruption.
  if [ "$churn_now" = "$acked" ]; then
    pass W6 "exactly-once: ${churn_now} rows for ${acked} acknowledged writes"
  elif [ "$churn_now" -lt "$acked" ] 2>/dev/null; then
    fail W6 "LOST: ${churn_now} rows for ${acked} acknowledged writes (missing $((acked - churn_now)))"
  else
    fail W6 "DUPLICATED: ${churn_now} rows for ${acked} acknowledged writes (extra $((churn_now - acked)))"
  fi

  rf=$(wait_for_regions_rf_ok 2 "$((HEARTBEAT_INTERVAL_SECS * 12))")
  if [ "$rf" = "OK" ]; then
    pass W7 "all regions RF>=2 after churn"
  else
    fail W7 "RF violation: $rf"
  fi

  write_report
  if [ "$FAILURES" -eq 0 ]; then
    log "CHURN E2E PASSED"; exit 0
  fi
  err "CHURN E2E FAILED ($FAILURES)"; exit 1
}

main "$@"
