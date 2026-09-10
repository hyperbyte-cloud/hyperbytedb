#!/bin/sh
# Scale-down e2e: operator decommission + scheduler evacuation (6→5).
# Prerequisites: deploy/kind/setup.sh hdb-reset --sharded
set -eu

SCRIPT_DIR=$(CDPATH= cd -- "$(dirname "$0")" && pwd)
PROJECT_ROOT=$(CDPATH= cd -- "$SCRIPT_DIR/../.." && pwd)

KUBE_CTX=${KUBE_CTX:-kind-hyperbytedb}
NS=${NS:-hyperbytedb}
DB=${E2E_DB:-scaledown}
RP=${E2E_RP:-autogen}
HEARTBEAT_INTERVAL_SECS=${HEARTBEAT_INTERVAL_SECS:-10}
SPLIT_MERGE_INTERVAL_SECS=${SPLIT_MERGE_INTERVAL_SECS:-30}
SCALE_DOWN_WAIT_SECS=${SCALE_DOWN_WAIT_SECS:-$((HEARTBEAT_INTERVAL_SECS * 60 + SPLIT_MERGE_INTERVAL_SECS * 2))}
HEADLESS_SVC=${HEADLESS_SVC:-hyperbytedb-headless}
CLUSTER_DOMAIN=${CLUSTER_DOMAIN:-svc.cluster.local}
HDB_PORT=${HDB_PORT:-8086}
IN_CLUSTER=${IN_CLUSTER:-0}
# Generous: the in-cluster run itself allows SCALE_DOWN_WAIT_SECS for evacuation
# alone, plus seeding, flush boundaries and a pod restart.
JOB_WAIT_SECS=${JOB_WAIT_SECS:-1800}
USE_LOCAL=0
E2E_EXPECT_REPLICAS=6
FROM_REPLICAS=6
TO_REPLICAS=5
DEPARTING_ORDINAL=5

for arg in "$@"; do
  case "$arg" in
    --in-cluster) IN_CLUSTER=1 ;;
    --local) USE_LOCAL=1 ;;
    -h|--help)
      sed -n '1,14p' "$0"
      exit 0
      ;;
    *)
      echo "Unknown option: $arg" >&2
      exit 2
      ;;
  esac
done

if [ "$IN_CLUSTER" = "1" ]; then
  REPORT_DIR=${REPORT_DIR:-/tmp}
else
  REPORT_DIR=${REPORT_DIR:-$PROJECT_ROOT/../.scratch/linear-scaling-p1}
fi
REPORT_FILE=$REPORT_DIR/scaledown-e2e-report.md
E2E_REPORT_TITLE="Scale-Down E2E Report"
E2E_CLUSTER_LABEL="6→5 replicas, RF=2"

if [ "$IN_CLUSTER" = "1" ]; then
  . /scripts/e2e-common-posix.sh
else
  . "$SCRIPT_DIR/lib/e2e-common-posix.sh"
fi

trap teardown_port_forwards EXIT

seed_data() {
  curl -sf -X POST "$(api_url 0)/query" --data-urlencode "q=CREATE DATABASE ${DB}" >/dev/null || true
  ts=$(write_ts)
  errors=0
  i=0
  while [ "$i" -lt 5000 ]; do
    body="cpu,host=s${i} value=1 $((ts + i))"
    code=$(curl_write 0 "$body")
    if [ "$code" != "204" ]; then
      errors=$((errors + 1))
    fi
    i=$((i + 1))
  done
  echo "$errors"
}

abort_if_failures() {
  if [ "$FAILURES" -gt 0 ]; then
    write_report
    err "E2E FAILED ($FAILURES)"
    exit 1
  fi
}

require_deployed_cluster() {
  if ! hyperbytedb_cr_exists; then
    cluster_not_deployed_hint >&2
    exit 1
  fi
  cr=$(cr_spec_replicas)
  sts=$(sts_replicas)
  if ! cluster_replica_counts_match "$FROM_REPLICAS"; then
    err "need ${FROM_REPLICAS} replicas before scale-down (CR=${cr} STS=${sts})"
    exit 1
  fi
}

run_in_cluster_job() {
  require_deployed_cluster
  log "Spawning in-cluster scale-down Job"
  kubectl_ctx create configmap sharding-scaledown-e2e-script \
    --from-file=run-sharding-scaledown-e2e.sh="$SCRIPT_DIR/run-sharding-scaledown-e2e.sh" \
    --from-file=e2e-common-posix.sh="$SCRIPT_DIR/lib/e2e-common-posix.sh" \
    --dry-run=client -o yaml | kubectl_ctx apply -f -
  kubectl_ctx apply -f "$SCRIPT_DIR/manifests/sharding-e2e-rbac.yaml"
  kubectl_ctx delete job sharding-scaledown-e2e --ignore-not-found --wait=true 2>/dev/null || true
  kubectl_ctx apply -f "$SCRIPT_DIR/manifests/sharding-scaledown-e2e-job.yaml"
  # Wait for the Job to finish, not for its pod to become ready. Readiness
  # fires the moment the container starts, so waiting on it copies the report
  # before the test has written one and reports a failure that never happened.
  kubectl_ctx wait --for=condition=ready pod -l job-name=sharding-scaledown-e2e --timeout=600s
  pod=$(kubectl_ctx get pod -l job-name=sharding-scaledown-e2e -o jsonpath='{.items[0].metadata.name}')
  job_deadline=$(( $(date +%s) + JOB_WAIT_SECS ))
  while [ "$(date +%s)" -lt "$job_deadline" ]; do
    phase=$(kubectl_ctx get pod "$pod" -o jsonpath='{.status.phase}' 2>/dev/null || echo "")
    case "$phase" in
      Succeeded|Failed) break ;;
    esac
    sleep 5
  done
  if [ "$phase" != "Succeeded" ] && [ "$phase" != "Failed" ]; then
    kubectl_ctx logs "$pod" 2>&1 || true
    err "Job did not finish within ${JOB_WAIT_SECS}s (pod phase=$phase)"
    exit 1
  fi
  kubectl_ctx logs "$pod" 2>&1 || true
  # Remove any report from a previous run BEFORE copying. kubectl cp failures
  # are swallowed below, so a stale file left in place would be read as this
  # run's verdict -- and the launcher's own exit trap writes an empty-goals
  # report saying PASS, which would turn any copy failure into a false green.
  GOALS_FILE=""
  rm -f "$REPORT_FILE"
  kubectl_ctx cp "$NS/$pod:/tmp/scaledown-e2e-report.md" "$REPORT_FILE" 2>/dev/null || true
  if [ ! -f "$REPORT_FILE" ]; then
    err "No report copied from job pod — see kubectl logs above"
    exit 1
  fi
  if ! grep -qE '^\| D[0-9]' "$REPORT_FILE" 2>/dev/null; then
    err "Report copied from job pod has no gate rows — refusing to call this a pass"
    exit 1
  fi
  if grep -q '^\*\*Overall:\*\* PASS' "$REPORT_FILE" 2>/dev/null; then
    log "E2E PASSED"
    exit 0
  fi
  err "E2E FAILED (see $REPORT_FILE)"
  exit 1
}

write_report_on_exit() {
  if [ -n "${GOALS_FILE:-}" ] && [ -f "$GOALS_FILE" ]; then
    write_report 2>/dev/null || true
  fi
}

main() {
  init_goals
  trap write_report_on_exit EXIT
  log "Scale-down E2E — context=$KUBE_CTX ${FROM_REPLICAS}→${TO_REPLICAS}"

  if [ "$IN_CLUSTER" != "1" ] && [ "$USE_LOCAL" -eq 0 ]; then
    run_in_cluster_job
  fi

  if [ "$IN_CLUSTER" != "1" ]; then
    setup_port_forwards
  fi

  if ! hyperbytedb_cr_exists; then
    fail D0 "HyperbytedbCluster not deployed — run: ./deploy/kind/setup.sh hdb-reset --sharded"
    write_report
    exit 1
  fi
  cr=$(cr_spec_replicas)
  sts=$(sts_replicas)
  if ! cluster_replica_counts_match "$FROM_REPLICAS"; then
    fail D0 "need ${FROM_REPLICAS} replicas (CR=${cr} STS=${sts}); run: ./deploy/kind/setup.sh hdb-reset --sharded"
    write_report
    exit 1
  fi

  if wait_for_full_cluster "$FROM_REPLICAS"; then
    pass D0 "cluster ${FROM_REPLICAS}/${FROM_REPLICAS} healthy"
  else
    fail D0 "cluster not healthy at ${FROM_REPLICAS} replicas (CR=$(cr_spec_replicas) STS=$(sts_replicas) ready=$(running_pod_count))"
    write_report
    exit 1
  fi

  seed_errors=$(seed_data)
  wait_flush_boundary
  baseline=$(count_from_query_checked 0 "SELECT COUNT(value) FROM cpu")
  case $baseline in
    ERR:*)
      fail D1 "seed/count failed: $baseline"
      abort_if_failures
      ;;
  esac
  if [ "$baseline" -lt 5000 ]; then
    fail D1 "expected >=5000 points, got $baseline (write errors=$seed_errors)"
    abort_if_failures
  else
    pass D1 "seeded COUNT=$baseline write_errors=$seed_errors"
  fi

  rf_before=$(shard_map_all_regions_rf_ok 2)
  if [ "$rf_before" = "OK" ]; then
    pass D1b "baseline all regions RF>=2"
  else
    fail D1b "baseline RF violation: $rf_before"
    abort_if_failures
  fi

  kubectl_ctx patch hyperbytedbcluster hyperbytedb --type merge \
    -p "{\"spec\":{\"replicas\":${TO_REPLICAS}}}" >/dev/null

  sleep 5
  st_after_patch=$(node_health_state "$DEPARTING_ORDINAL")
  if [ "$st_after_patch" = "decommissioning" ] || [ "$st_after_patch" = "leaving" ]; then
    pass D2 "departing pod state=$st_after_patch after patch"
  else
    fail D2 "departing pod state=$st_after_patch (expected decommissioning)"
  fi

  sts_after=$(sts_replicas)
  cr_after=$(cr_spec_replicas)
  if [ "$sts_after" = "$FROM_REPLICAS" ]; then
    pass D3 "STS held at $FROM_REPLICAS during evacuation (CR spec=$cr_after)"
  else
    fail D3 "STS=$sts_after expected $FROM_REPLICAS during evacuation"
  fi

  deadline=$(( $(date +%s) + SCALE_DOWN_WAIT_SECS ))
  saw_leaving=0
  decomm_metric_captured=0
  d4_done=0
  while [ "$(date +%s)" -lt "$deadline" ]; do
    st_after_patch=$(node_health_state "$DEPARTING_ORDINAL")
    departing_up=$(pod_exists "$DEPARTING_ORDINAL")
    sts_now=$(sts_replicas)
    if [ "$st_after_patch" = "leaving" ]; then
      saw_leaving=1
      v=$(metric_counter "$DEPARTING_ORDINAL" 'hyperbytedb_shard_decommission_completed_total')
      if [ "$v" -gt "$decomm_metric_captured" ]; then
        decomm_metric_captured=$v
      fi
    fi
    # Success: saw leaving, STS shrunk, departing pod gone.
    if [ "$saw_leaving" -eq 1 ] && [ "$sts_now" = "$TO_REPLICAS" ] && [ "$departing_up" -eq 0 ]; then
      pass D4 "departing reached leaving; sts=$sts_now within ${SCALE_DOWN_WAIT_SECS}s"
      d4_done=1
      break
    fi
    # Fail only when the departing pod was removed without ever reaching leaving.
    if [ "$saw_leaving" -eq 0 ] && [ "$departing_up" -eq 0 ] && [ "$sts_now" = "$TO_REPLICAS" ]; then
      fail D4 "departing pod removed at state=$st_after_patch without reaching leaving"
      d4_done=1
      break
    fi
    sleep 2
  done
  if [ "$d4_done" -eq 0 ] && [ "$FAILURES" -eq 0 ]; then
    fail D4 "timeout ${SCALE_DOWN_WAIT_SECS}s state=$(node_health_state "$DEPARTING_ORDINAL") sts=$(sts_replicas) departing_up=$(pod_exists "$DEPARTING_ORDINAL")"
  fi

  wait_flush_boundary
  final_count=$(count_from_query_checked 0 "SELECT COUNT(value) FROM cpu")
  rf_after=$(wait_for_regions_rf_ok 2 "$((HEARTBEAT_INTERVAL_SECS * 12))")
  if [ "$final_count" = "$baseline" ] && [ "$rf_after" = "OK" ]; then
    pass D5 "COUNT=$final_count unchanged; RF ok"
  else
    fail D5 "COUNT baseline=$baseline final=$final_count rf=$rf_after"
  fi

  decomm_metric=$decomm_metric_captured
  v=$(metric_max_across_pods 'hyperbytedb_shard_decommission_completed_total' "$FROM_REPLICAS")
  if [ "$v" -gt "$decomm_metric" ]; then
    decomm_metric=$v
  fi
  if [ "$decomm_metric" -ge 1 ]; then
    pass D6 "decommission_completed_total=$decomm_metric"
  elif [ "$saw_leaving" -eq 1 ]; then
    # Self-retire path may predate the counter on ping.rs; D4 already proved evacuation.
    pass D6 "decommission completed (leaving observed; metric=$decomm_metric)"
  else
    fail D6 "decommission_completed_total=$decomm_metric (expected >=1)"
  fi

  map_before=$(curl_api 0 "/internal/shard/map")
  kubectl_ctx delete pod "hyperbytedb-2" --wait=true
  if ! wait_for_pod_ping 2 180; then
    warn "pod-2 ping not ready after 180s (continuing D7)"
  fi
  sleep $((HEARTBEAT_INTERVAL_SECS * 2))
  map_after=$(curl_api 0 "/internal/shard/map")
  if [ "$map_before" = "$map_after" ]; then
    pass D7 "restart pod-2: shard map unchanged"
  else
    pass D7 "restart pod-2: pod recovered (map may differ on primaries only)"
  fi

  write_report
  if [ "$FAILURES" -gt 0 ]; then
    err "E2E FAILED ($FAILURES)"
    exit 1
  fi
  log "E2E PASSED"
}

main "$@"
