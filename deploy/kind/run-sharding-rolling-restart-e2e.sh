#!/bin/sh
# Rolling restart under load: every pod restarts once; placement must not
# RemovePeer healthy replicas or trigger full region re-copies.
#
# Prerequisites: deploy/kind/setup.sh hdb-reset --sharded
set -eu

SCRIPT_DIR=$(CDPATH= cd -- "$(dirname "$0")" && pwd)
PROJECT_ROOT=$(CDPATH= cd -- "$SCRIPT_DIR/../.." && pwd)

KUBE_CTX=${KUBE_CTX:-kind-hyperbytedb}
NS=${NS:-hyperbytedb}
DB=${E2E_DB:-rolling}
FULL_REPLICAS=6
REPORT_DIR=${REPORT_DIR:-$PROJECT_ROOT/../.scratch/linear-scaling-p1}
REPORT_FILE=$REPORT_DIR/rolling-restart-e2e-report.md
E2E_REPORT_TITLE="Sharding Rolling-Restart E2E Report"
E2E_CLUSTER_LABEL="6 nodes, sequential pod delete, RF=2"

. "$SCRIPT_DIR/lib/e2e-common-posix.sh"

main() {
  init_goals
  log "Rolling-restart E2E — delete each pod once, assert no RemovePeer storms"
  setup_port_forwards

  if wait_for_full_cluster "$FULL_REPLICAS"; then
    pass R0 "cluster ${FULL_REPLICAS}/${FULL_REPLICAS} healthy"
  else
    fail R0 "cluster not healthy"
    write_report
    exit 1
  fi

  curl -sf -X POST "$(api_url 0)/query" --data-urlencode "q=CREATE DATABASE ${DB}" >/dev/null 2>&1 || true
  baseline_remove=$(metric_max_across_pods 'hyperbytedb_shard_remove_peer_proposals_total' "$FULL_REPLICAS" 2>/dev/null || echo 0)
  case "$baseline_remove" in ''|*[!0-9]*) baseline_remove=0 ;; esac

  i=0
  while [ "$i" -lt "$FULL_REPLICAS" ]; do
    log "Deleting pod hyperbytedb-${i}"
    kubectl_ctx delete pod "hyperbytedb-${i}" --wait=false >/dev/null 2>&1 || true
    sleep 15
    setup_port_forwards
    if ! wait_for_full_cluster "$FULL_REPLICAS"; then
      fail "R$((i + 1))" "cluster not healthy after pod ${i} restart"
      write_report
      exit 1
    fi
    pass "R$((i + 1))" "pod ${i} restarted; cluster healthy"
    i=$((i + 1))
  done

  after_remove=$(metric_max_across_pods 'hyperbytedb_shard_remove_peer_proposals_total' "$FULL_REPLICAS" 2>/dev/null || echo 0)
  case "$after_remove" in ''|*[!0-9]*) after_remove=0 ;; esac
  delta=$((after_remove - baseline_remove))
  if [ "$delta" -le 2 ]; then
    pass R7 "RemovePeer proposals delta=${delta} (no mass eviction)"
  else
    fail R7 "RemovePeer proposals increased by ${delta} during rolling restart"
  fi

  write_report
  if [ "$FAILURES" -gt 0 ]; then
    exit 1
  fi
  log "ROLLING-RESTART E2E PASSED"
}

main "$@"
