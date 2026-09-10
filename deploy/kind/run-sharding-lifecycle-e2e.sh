#!/bin/sh
# Full sharding lifecycle e2e: scale down, node failure, scale up — with the
# SAME query battery verified after every topology change.
#
# The point is not that each operation completes. It is that a fixed, known
# dataset stays exactly readable across all of them: same count, same sum, same
# extremes, same cardinality, same per-series values. Two data-loss bugs on this
# branch passed every in-process test and were only visible as wrong query
# answers after churn, which is what this asserts.
#
# Prerequisites: deploy/kind/setup.sh hdb-reset --sharded  (6 replicas)
set -eu

SCRIPT_DIR=$(CDPATH= cd -- "$(dirname "$0")" && pwd)
PROJECT_ROOT=$(CDPATH= cd -- "$SCRIPT_DIR/../.." && pwd)

KUBE_CTX=${KUBE_CTX:-kind-hyperbytedb}
NS=${NS:-hyperbytedb}
DB=${E2E_DB:-lifecycle}
RP=${E2E_RP:-autogen}
HEARTBEAT_INTERVAL_SECS=${HEARTBEAT_INTERVAL_SECS:-10}
SPLIT_MERGE_INTERVAL_SECS=${SPLIT_MERGE_INTERVAL_SECS:-30}
CONVERGE_WAIT_SECS=${CONVERGE_WAIT_SECS:-$((HEARTBEAT_INTERVAL_SECS * 60 + SPLIT_MERGE_INTERVAL_SECS * 2))}
FAILOVER_WAIT_SECS=${FAILOVER_WAIT_SECS:-180}
HEADLESS_SVC=${HEADLESS_SVC:-hyperbytedb-headless}
CLUSTER_DOMAIN=${CLUSTER_DOMAIN:-svc.cluster.local}
HDB_PORT=${HDB_PORT:-8086}
IN_CLUSTER=${IN_CLUSTER:-0}
USE_LOCAL=0

# Dataset shape. Values are computable so every aggregate has an exact expected
# answer rather than a "looks plausible" one.
HOSTS=${HOSTS:-40}
POINTS_PER_HOST=${POINTS_PER_HOST:-25}

FULL_REPLICAS=6
DOWN_REPLICAS=5
DEPARTING_ORDINAL=5
KILL_ORDINAL=2

for arg in "$@"; do
  case "$arg" in
    --local) USE_LOCAL=1 ;;
    -h|--help) sed -n '1,12p' "$0"; exit 0 ;;
    *) echo "Unknown option: $arg" >&2; exit 2 ;;
  esac
done

REPORT_DIR=${REPORT_DIR:-$PROJECT_ROOT/../.scratch/linear-scaling-p1}
REPORT_FILE=$REPORT_DIR/lifecycle-e2e-report.md
E2E_REPORT_TITLE="Sharding Lifecycle E2E Report"
E2E_CLUSTER_LABEL="6→5→kill→6, RF=2"

. "$SCRIPT_DIR/lib/e2e-common-posix.sh"

trap teardown_port_forwards EXIT

# ── expected values, derived not hardcoded ────────────────────────────────────
EXP_COUNT=$((HOSTS * POINTS_PER_HOST))
EXP_SUM=$(python3 -c "
h=$HOSTS; p=$POINTS_PER_HOST
print(p*100*(h*(h-1)//2) + h*(p*(p-1)//2))
")
EXP_MIN=0
EXP_MAX=$(( (HOSTS - 1) * 100 + (POINTS_PER_HOST - 1) ))
PROBE_HOST=7
EXP_PROBE_SUM=$(python3 -c "
p=$POINTS_PER_HOST; h=$PROBE_HOST
print(p*100*h + (p*(p-1)//2))
")

seed_dataset() {
  # Drop first so the run is idempotent. Each seed uses a fresh timestamp base,
  # so re-running against a surviving database would double every count and the
  # expected values would be wrong for a reason that has nothing to do with the
  # code under test.
  curl -sf -X POST "$(api_url 0)/query" --data-urlencode "q=DROP DATABASE ${DB}" >/dev/null 2>&1 || true
  sleep 3
  curl -sf -X POST "$(api_url 0)/query" --data-urlencode "q=CREATE DATABASE ${DB}" >/dev/null 2>&1 || true
  base=$(write_ts)
  errors=0
  h=0
  while [ "$h" -lt "$HOSTS" ]; do
    body=$(python3 -c "
h=$h; p=$POINTS_PER_HOST; base=$base
print('\n'.join('cpu,host=h%d value=%d %d' % (h, h*100+i, base+i) for i in range(p)))
")
    code=$(curl_write 0 "$body")
    [ "$code" = "204" ] || errors=$((errors + 1))
    h=$((h + 1))
  done
  echo "$errors"
}

# The battery. Every assertion has an exact expected answer computed from the
# dataset shape, so a wrong answer is unambiguous rather than a judgement call.
verify_data() {
  phase=$1
  wait_flush_boundary
  problems=""

  c=$(count_from_query_checked 0 "SELECT COUNT(value) FROM cpu")
  [ "$c" = "$EXP_COUNT" ] || problems="$problems count=$c(want $EXP_COUNT)"

  s=$(query_scalar 0 "SELECT SUM(value) FROM cpu")
  [ "$s" = "$EXP_SUM" ] || problems="$problems sum=$s(want $EXP_SUM)"

  mn=$(query_scalar 0 "SELECT MIN(value) FROM cpu")
  [ "$mn" = "$EXP_MIN" ] || problems="$problems min=$mn(want $EXP_MIN)"

  mx=$(query_scalar 0 "SELECT MAX(value) FROM cpu")
  [ "$mx" = "$EXP_MAX" ] || problems="$problems max=$mx(want $EXP_MAX)"

  g=$(query_group_count 0 "SELECT COUNT(value) FROM cpu GROUP BY host")
  [ "$g" = "$HOSTS" ] || problems="$problems groups=$g(want $HOSTS)"

  ps=$(query_scalar 0 "SELECT SUM(value) FROM cpu WHERE host='h${PROBE_HOST}'")
  [ "$ps" = "$EXP_PROBE_SUM" ] || problems="$problems probe_sum=$ps(want $EXP_PROBE_SUM)"

  if [ -z "$problems" ]; then
    pass "$phase" "all queries exact (count=$c sum=$s min=$mn max=$mx groups=$g probe=$ps)"
  else
    fail "$phase" "WRONG ANSWERS:$problems"
  fi
}

wait_for_replicas() {
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

main() {
  init_goals
  log "Lifecycle E2E — ${FULL_REPLICAS}→${DOWN_REPLICAS}→kill→${FULL_REPLICAS}, dataset ${HOSTS}x${POINTS_PER_HOST}"
  setup_port_forwards

  # ── L0 baseline ─────────────────────────────────────────────────────────────
  if wait_for_full_cluster "$FULL_REPLICAS"; then
    pass L0 "cluster ${FULL_REPLICAS}/${FULL_REPLICAS} healthy"
  else
    fail L0 "cluster not healthy at ${FULL_REPLICAS} (run: ./deploy/kind/setup.sh hdb-reset --sharded)"
    write_report; exit 1
  fi

  errs=$(seed_dataset)
  if [ "$errs" = "0" ]; then
    pass L1 "seeded ${EXP_COUNT} points across ${HOSTS} series, 0 write errors"
  else
    fail L1 "seed had $errs failed batches"
  fi
  verify_data L2

  # ── L3 scale down: decommission + evacuation ────────────────────────────────
  log "Scaling down ${FULL_REPLICAS}→${DOWN_REPLICAS}"
  kubectl_ctx patch hyperbytedbcluster hyperbytedb --type merge \
    -p "{\"spec\":{\"replicas\":${DOWN_REPLICAS}}}" >/dev/null
  if wait_for_replicas "$DOWN_REPLICAS"; then
    pass L3 "scaled to ${DOWN_REPLICAS} within ${CONVERGE_WAIT_SECS}s"
  else
    fail L3 "still at sts=$(sts_replicas) pods=$(running_pod_count) after ${CONVERGE_WAIT_SECS}s"
  fi
  verify_data L4

  # ── L5 node failure ─────────────────────────────────────────────────────────
  log "Killing pod ${KILL_ORDINAL} to force failover"
  kubectl_ctx delete pod "hyperbytedb-${KILL_ORDINAL}" --wait=false >/dev/null 2>&1 || true
  deadline=$(( $(date +%s) + FAILOVER_WAIT_SECS ))
  recovered=0
  while [ "$(date +%s)" -lt "$deadline" ]; do
    if [ "$(running_pod_count)" = "$DOWN_REPLICAS" ]; then recovered=1; break; fi
    sleep 3
  done
  if [ "$recovered" = "1" ]; then
    pass L5 "pod ${KILL_ORDINAL} recovered; ${DOWN_REPLICAS} running"
  else
    fail L5 "only $(running_pod_count)/${DOWN_REPLICAS} running after ${FAILOVER_WAIT_SECS}s"
  fi
  verify_data L6

  # ── L7 scale up: joiner placement ───────────────────────────────────────────
  log "Scaling up ${DOWN_REPLICAS}→${FULL_REPLICAS}"
  kubectl_ctx patch hyperbytedbcluster hyperbytedb --type merge \
    -p "{\"spec\":{\"replicas\":${FULL_REPLICAS}}}" >/dev/null
  if wait_for_replicas "$FULL_REPLICAS"; then
    pass L7 "scaled back to ${FULL_REPLICAS}"
  else
    fail L7 "still at sts=$(sts_replicas) pods=$(running_pod_count)"
  fi
  verify_data L8

  # ── L9 final invariants ─────────────────────────────────────────────────────
  rf=$(wait_for_regions_rf_ok 2 "$((HEARTBEAT_INTERVAL_SECS * 12))")
  if [ "$rf" = "OK" ]; then
    pass L9 "all regions RF>=2 after the full lifecycle"
  else
    fail L9 "RF violation after lifecycle: $rf"
  fi

  stalled=$(metric_max_across_pods 'hyperbytedb_shard_placement_stalled_total' || echo 0)
  if [ "${stalled:-0}" -eq 0 ] 2>/dev/null; then
    pass L10 "placement_stalled_total=0"
  else
    fail L10 "placement stalled: $stalled"
  fi

  write_report
  if [ "$FAILURES" -eq 0 ]; then
    log "LIFECYCLE E2E PASSED"
    exit 0
  fi
  err "LIFECYCLE E2E FAILED ($FAILURES)"
  exit 1
}

main "$@"
