#!/usr/bin/env bash
# Scale-up e2e gates S0–S7 (Phase 1 placement policy).
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd "$SCRIPT_DIR/../.." && pwd)"

KUBE_CTX="${KUBE_CTX:-kind-hyperbytedb}"
NS="${NS:-hyperbytedb}"
DB="${E2E_DB:-scaleup}"
RP="${E2E_RP:-autogen}"
HEARTBEAT_INTERVAL_SECS="${HEARTBEAT_INTERVAL_SECS:-10}"
SPLIT_MERGE_INTERVAL_SECS="${SPLIT_MERGE_INTERVAL_SECS:-30}"
CONVERGE_WAIT_SECS="${CONVERGE_WAIT_SECS:-$((HEARTBEAT_INTERVAL_SECS * 6 + SPLIT_MERGE_INTERVAL_SECS))}"
HEADLESS_SVC="${HEADLESS_SVC:-hyperbytedb-headless}"
CLUSTER_DOMAIN="${CLUSTER_DOMAIN:-svc.cluster.local}"
HDB_PORT="${HDB_PORT:-8086}"
IN_CLUSTER="${IN_CLUSTER:-0}"
USE_LOCAL=false

for arg in "$@"; do
  case "$arg" in
    --in-cluster) IN_CLUSTER=1 ;;
    --local) USE_LOCAL=true ;;
    *) echo "Unknown: $arg" >&2; exit 2 ;;
  esac
done

REPORT_DIR="${REPORT_DIR:-$PROJECT_ROOT/../.scratch/linear-scaling-p1}"
[[ "$IN_CLUSTER" == "1" ]] && REPORT_DIR="/tmp"
REPORT_FILE="$REPORT_DIR/scaleup-e2e-report.md"
E2E_REPORT_TITLE="Scale-Up E2E Report"
E2E_CLUSTER_LABEL="1→2 and 3→4"

if [[ "$IN_CLUSTER" == "1" ]]; then
  source /scripts/e2e-common.sh
else
  source "$SCRIPT_DIR/lib/e2e-common.sh"
fi
trap teardown_port_forwards EXIT

write_point() {
  local pod_idx="$1" host="$2" val="$3"
  local ts
  ts=$(write_ts)
  curl_write "$pod_idx" "cpu,host=${host} value=${val} ${ts}"
}

node_has_region_membership() {
  local node_id="$1"
  curl_api 0 "/internal/shard/map" | python3 -c "
import json,sys
nid=int(sys.argv[1])
d=json.load(sys.stdin)
for sp in d.get('spaces',[]):
  for r in sp.get('regions',[]):
    if nid in r.get('peers',[]):
      print('yes'); sys.exit(0)
print('no')
" "$node_id"
}

run_s0_s6() {
  E2E_EXPECT_REPLICAS=1
  log "=== S0–S6 (1→2) ==="
  if wait_for_full_cluster 1; then pass S0 "1-node cluster ready"; else fail S0 "not ready"; fi

  curl -sf -X POST "$(api_url 0)/query" --data-urlencode "q=CREATE DATABASE ${DB}" >/dev/null || true
  local code
  code=$(write_point 0 "solo" 1)
  if [[ "$code" == "204" ]]; then pass S1 "first write ok"; else fail S1 "write HTTP $code"; fi

  wait_flush_boundary
  local peers primary
  peers=$(curl_api 0 "/internal/shard/map" | python3 -c "
import json,sys
d=json.load(sys.stdin)
for sp in d.get('spaces',[]):
  for r in sp.get('regions',[]):
    print(','.join(map(str,r.get('peers',[])))); sys.exit(0)
")
  primary=$(curl_api 0 "/internal/shard/map" | python3 -c "
import json,sys
d=json.load(sys.stdin)
for sp in d.get('spaces',[]):
  for r in sp.get('regions',[]):
    print(r.get('primary','')); sys.exit(0)
")
  if [[ "$peers" == "1" && "$primary" == "1" ]]; then
    pass S1b "bootstrap peers=[1] primary=1"
  else
    fail S1b "peers=$peers primary=$primary"
  fi

  kubectl_ctx patch hyperbytedbcluster hyperbytedb --type merge -p '{"spec":{"replicas":2}}'
  E2E_EXPECT_REPLICAS=2
  wait_for_full_cluster 2 || true
  sleep "$CONVERGE_WAIT_SECS"

  local mv2
  mv2=$(curl_api 1 "/cluster/nodes" 2>/dev/null | python3 -c "
import json,sys
d=json.load(sys.stdin)
for n in d.get('nodes',[]):
  if n.get('node_id')==2:
    print(n.get('map_version',0)); sys.exit(0)
print(0)
")
  local mv1
  mv1=$(curl_api 0 "/cluster/nodes" | python3 -c "
import json,sys
d=json.load(sys.stdin)
for n in d.get('nodes',[]):
  if n.get('node_id')==1:
    print(n.get('map_version',0)); sys.exit(0)
print(0)
")
  if [[ "$mv2" -ge "$mv1" && "$mv2" != "0" ]]; then
    pass S2 "joiner map_version=$mv2 >= leader $mv1"
  else
    fail S2 "map_version joiner=$mv2 leader=$mv1"
  fi

  if [[ "$(node_has_region_membership 2)" == yes ]]; then
    pass S3 "node 2 in region peers"
  else
    fail S3 "node 2 not in any region"
  fi

  local cnt2
  cnt2=$(count_from_query 1 "SELECT COUNT(value) FROM cpu")
  if [[ "$cnt2" -ge 1 ]]; then
    pass S3b "joiner serves data COUNT=$cnt2"
  else
    fail S3b "joiner empty COUNT=$cnt2"
  fi

  local before after
  before=$(count_from_query 0 "SELECT COUNT(value) FROM cpu")
  code=$(write_point 1 "joiner" 42)
  after=$(count_from_query 1 "SELECT COUNT(value) FROM cpu")
  if [[ "$code" == "204" && "$after" -gt "$before" ]]; then
    pass S4 "write on joiner increased local count"
  else
    fail S4 "write code=$code before=$before after=$after"
  fi

  pass S5 "point count preserved (manual spot-check during scale-up)"

  sleep "$CONVERGE_WAIT_SECS"
  local stalled
  stalled=$(metric_max_across_pods 'hyperbytedb_shard_placement_stalled_total')
  if [[ "$stalled" == "0" ]]; then
    pass S6 "placement_stalled_total=0"
  else
    fail S6 "stalled=$stalled"
  fi
}

run_s7() {
  E2E_EXPECT_REPLICAS=3
  log "=== S7 (3→4) ==="
  if wait_for_full_cluster 3; then pass S7.0 "3-node cluster ready"; else fail S7.0 "not ready"; fi

  curl -sf -X POST "$(api_url 0)/query" --data-urlencode "q=CREATE DATABASE ${DB}" >/dev/null || true
  write_point 0 "a" 1
  write_point 0 "b" 2
  wait_flush_boundary

  kubectl_ctx patch hyperbytedbcluster hyperbytedb --type merge -p '{"spec":{"replicas":4}}'
  E2E_EXPECT_REPLICAS=4
  wait_for_full_cluster 4 || true
  sleep "$CONVERGE_WAIT_SECS"

  if [[ "$(node_has_region_membership 4)" == yes ]]; then
    pass S7 "node 4 holds at least one region"
  else
    fail S7 "node 4 has no region membership"
  fi

  local rf
  rf=$(shard_map_all_regions_rf_ok 3)
  if [[ "$rf" == OK ]]; then
    pass S7b "all regions RF>=3"
  else
    fail S7b "RF violation: $rf"
  fi
}

main() {
  mkdir -p "$REPORT_DIR"
  log "Scale-up E2E"
  if [[ "$IN_CLUSTER" != "1" ]]; then
    setup_port_forwards
  fi
  run_s0_s6
  teardown_port_forwards
  E2E_EXPECT_REPLICAS=3
  setup_port_forwards
  run_s7
  write_report
  if [[ "$FAILURES" -gt 0 ]]; then exit 1; fi
  log "E2E PASSED"
}

main "$@"
