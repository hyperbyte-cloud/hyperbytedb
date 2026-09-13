#!/bin/sh
# Writes DURING topology change: exactly-once across a full lifecycle.
#
# Every other e2e seeds data and then moves it. Real clusters write
# continuously, so this keeps ONE writer running across three consecutive
# topology changes -- scale down, kill a pod, scale back up -- and asserts
# exactly-once after each: rows equal every write the cluster acknowledged
# with 204, no fewer and no more.
#
# One change proves much less than a sequence. The windows that matter are the
# ones a single scale-down never reaches: a write landing on a JOINING node
# whose region is still staging, and a write landing during a RESTART when the
# primary is briefly unreachable and resolve_region_peers yields no target.
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
KILL_ORDINAL=2

REPORT_DIR=${REPORT_DIR:-$PROJECT_ROOT/../.scratch/linear-scaling-p1}
REPORT_FILE=$REPORT_DIR/churn-e2e-report.md
E2E_REPORT_TITLE="Sharding Write-Churn E2E Report"
E2E_CLUSTER_LABEL="6→5→kill→6 under continuous writes, RF=2"

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

wait_for_pods() {
  want=$1
  deadline=$(( $(date +%s) + CONVERGE_WAIT_SECS ))
  while [ "$(date +%s)" -lt "$deadline" ]; do
    if [ "$(sts_replicas)" = "$want" ] && [ "$(running_pod_count)" = "$want" ]; then
      return 0
    fi
    sleep 3
  done
  return 1
}

# Exactly-once, checked at every checkpoint rather than only at the end. Fewer
# rows means acknowledged writes were lost; more means a region was staged
# twice, since apply_transfer_push is not idempotent. Both are corruption, and
# checking per phase says WHICH change broke it.
check_exactly_once() {
  gate=$1
  when=$2
  # The writer keeps running through this check, so a point comparison is
  # racy: reading `acked` before the flush wait and `rows` after it counts
  # every write acknowledged in between as a duplicate. Bracket instead --
  # sample acked either side and require rows to land within. Anything below
  # the low mark is a genuinely lost write; anything above the high mark is a
  # genuine duplicate, because apply_transfer_push is not idempotent.
  before=$(acked_total)
  wait_flush_boundary
  rows=$(count_from_query_checked 0 "SELECT COUNT(value) FROM churn")
  after=$(acked_total)
  # An unreadable count is not a passing count. Both numeric tests below error
  # on a non-numeric value, and with stderr suppressed the chain falls through
  # to `pass` -- which is how "ERR:peer unreachable: no active peers for
  # region 2" was reported as exactly-once.
  case "$rows" in
    ''|*[!0-9]*)
      fail "$gate" "UNREADABLE ${when}: count query returned [${rows}]"
      return
      ;;
  esac
  if [ "$rows" -lt "$before" ] 2>/dev/null; then
    fail "$gate" "LOST ${when}: ${rows} rows, ${before} acked before the check (missing $((before - rows)))"
  elif [ "$rows" -gt "$after" ] 2>/dev/null; then
    fail "$gate" "DUPLICATED ${when}: ${rows} rows, only ${after} acked (extra $((rows - after)))"
  else
    pass "$gate" "exactly-once ${when}: ${rows} rows within acked bracket [${before}, ${after}]"
  fi
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

  # Restore the starting topology rather than demanding a hand-reset. An
  # aborted earlier run leaves the cluster mid-lifecycle, and a test that needs
  # manual cleanup before every attempt is one people stop running.
  if [ "$(cr_spec_replicas)" != "$FULL_REPLICAS" ]; then
    log "Cluster at $(cr_spec_replicas); restoring to ${FULL_REPLICAS} before starting"
    kubectl_ctx patch hyperbytedbcluster hyperbytedb --type merge \
      -p "{\"spec\":{\"replicas\":${FULL_REPLICAS}}}" >/dev/null 2>&1 || true
  fi
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
  if wait_for_pods "$DOWN_REPLICAS"; then
    pass W3 "scaled to ${DOWN_REPLICAS} while writing"
  else
    fail W3 "still sts=$(sts_replicas) pods=$(running_pod_count)"
  fi
  during=$(acked_total)
  if [ "$during" -gt "$early" ]; then
    pass W4 "writes acknowledged during scale-down (${early} → ${during})"
  else
    fail W4 "no writes acknowledged during scale-down — window not exercised"
  fi
  check_exactly_once W5 "after scale-down"

  log "Killing pod ${KILL_ORDINAL} with writes in flight"
  before_kill=$(acked_total)
  kubectl_ctx delete pod "hyperbytedb-${KILL_ORDINAL}" --wait=false >/dev/null 2>&1 || true
  if wait_for_pods "$DOWN_REPLICAS"; then
    pass W6 "pod ${KILL_ORDINAL} recovered while writing"
  else
    fail W6 "only $(running_pod_count)/${DOWN_REPLICAS} running"
  fi
  after_kill=$(acked_total)
  if [ "$after_kill" -gt "$before_kill" ]; then
    pass W7 "writes acknowledged across the restart (${before_kill} → ${after_kill})"
  else
    fail W7 "no writes acknowledged across the restart — window not exercised"
  fi
  check_exactly_once W8 "after node restart"

  log "Scaling ${DOWN_REPLICAS}→${FULL_REPLICAS} with writes in flight"
  before_up=$(acked_total)
  kubectl_ctx patch hyperbytedbcluster hyperbytedb --type merge \
    -p "{\"spec\":{\"replicas\":${FULL_REPLICAS}}}" >/dev/null
  if wait_for_pods "$FULL_REPLICAS"; then
    pass W9 "scaled back to ${FULL_REPLICAS} while writing"
  else
    fail W9 "still sts=$(sts_replicas) pods=$(running_pod_count)"
  fi
  after_up=$(acked_total)
  if [ "$after_up" -gt "$before_up" ]; then
    pass W10 "writes acknowledged during scale-up (${before_up} → ${after_up})"
  else
    fail W10 "no writes acknowledged during scale-up — window not exercised"
  fi

  touch "$STOP_FILE"
  wait "$WRITER_PID" 2>/dev/null || true
  WRITER_PID=""
  check_exactly_once W11 "after the full lifecycle"

  seed_now=$(count_from_query_checked 0 "SELECT COUNT(value) FROM cpu")
  if [ "$seed_now" = "$EXP_SEED" ]; then
    pass W12 "seed data intact across all three changes (${seed_now})"
  else
    fail W12 "seed count=${seed_now} want ${EXP_SEED}"
  fi

  rf=$(wait_for_regions_rf_ok 2 "$((HEARTBEAT_INTERVAL_SECS * 12))")
  if [ "$rf" = "OK" ]; then
    pass W13 "all regions RF>=2 after churn"
  else
    fail W13 "RF violation: $rf"
  fi

  write_report
  if [ "$FAILURES" -eq 0 ]; then
    log "CHURN E2E PASSED"; exit 0
  fi
  err "CHURN E2E FAILED ($FAILURES)"; exit 1
}

main "$@"
