# Shared helpers for kind sharding e2e scripts.
# Source after setting SCRIPT_DIR, KUBE_CTX, NS, DB, REPORT_DIR, etc.
# Optional: E2E_EXPECT_REPLICAS (default 6), E2E_REPORT_TITLE, E2E_CLUSTER_LABEL

: "${E2E_EXPECT_REPLICAS:=6}"
: "${E2E_REPORT_TITLE:=Sharding E2E Test Report}"
: "${E2E_CLUSTER_LABEL:=6 replicas, RF=2}"

RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m'

declare -a GOAL_IDS=()
declare -a GOAL_RESULTS=()
declare -a GOAL_EVIDENCE=()
FAILURES=0
PF_PIDS=()

log() { echo -e "${GREEN}[e2e]${NC} $*"; }
warn() { echo -e "${YELLOW}[e2e]${NC} $*"; }
err() { echo -e "${RED}[e2e]${NC} $*" >&2; }

record() {
  local id="$1" result="$2" evidence="$3"
  GOAL_IDS+=("$id")
  GOAL_RESULTS+=("$result")
  GOAL_EVIDENCE+=("$evidence")
  if [[ "$result" == "FAIL" ]]; then
    FAILURES=$((FAILURES + 1))
    err "$id FAIL — $evidence"
  else
    log "$id $result — $evidence"
  fi
}

pass() { record "$1" "PASS" "$2"; }
fail() { record "$1" "FAIL" "$2"; }
skip() { record "$1" "SKIP" "$2"; }

wait_flush_boundary() {
  sleep "${FLUSH_WAIT_SECS:-6}"
}

kubectl_ctx() {
  if [[ "${IN_CLUSTER:-0}" == "1" ]]; then
    kubectl -n "$NS" "$@"
  else
    kubectl --context "$KUBE_CTX" -n "$NS" "$@"
  fi
}

api_port() {
  local pod_idx="$1"
  echo $((18080 + pod_idx))
}

api_url() {
  local pod_idx="$1"
  if [[ "${IN_CLUSTER:-0}" == "1" ]]; then
    echo "http://hyperbytedb-${pod_idx}.${HEADLESS_SVC:-hyperbytedb-headless}.${NS}.${CLUSTER_DOMAIN:-svc.cluster.local}:${HDB_PORT:-8086}"
  else
    echo "http://127.0.0.1:$(api_port "$pod_idx")"
  fi
}

setup_port_forwards() {
  [[ "${IN_CLUSTER:-0}" == "1" ]] && return 0
  local max=$((E2E_EXPECT_REPLICAS - 1))
  log "Starting port-forwards hyperbytedb-0..${max}"
  local i
  for ((i = 0; i <= max; i++)); do
    local port
    port=$(api_port "$i")
    fuser -k "${port}/tcp" 2>/dev/null || true
    kubectl_ctx port-forward "pod/hyperbytedb-$i" "${port}:8086" >/tmp/pf-hdb-"$i".log 2>&1 &
    PF_PIDS+=("$!")
  done
  local deadline=$((SECONDS + 45))
  while (( SECONDS < deadline )); do
    local all_ok=true
    for ((i = 0; i <= max; i++)); do
      local code
      code=$(curl -sS -o /dev/null -w '%{http_code}' -m 3 "$(api_url "$i")/ping" 2>/dev/null || echo 000)
      if [[ "$code" != "204" ]]; then
        all_ok=false
        break
      fi
    done
    if [[ "$all_ok" == true ]]; then
      log "All port-forwards responding"
      return 0
    fi
    sleep 2
  done
  warn "Not all port-forwards ready after 45s (continuing)"
}

teardown_port_forwards() {
  [[ "${IN_CLUSTER:-0}" == "1" ]] && return 0
  [[ "${KEEP_PF:-false}" == true ]] && return 0
  local pid
  for pid in "${PF_PIDS[@]:-}"; do
    kill "$pid" 2>/dev/null || true
  done
  PF_PIDS=()
  local i
  for ((i = 0; i < E2E_EXPECT_REPLICAS; i++)); do
    fuser -k "$(api_port "$i")/tcp" 2>/dev/null || true
  done
}

curl_api() {
  local pod_idx="$1"
  shift
  curl -sS -m "${CURL_TIMEOUT:-15}" "$(api_url "$pod_idx")$*" || true
}

curl_write() {
  local pod_idx="$1"
  local body="$2"
  curl -sS -m 15 -o /tmp/e2e-write.out -w '%{http_code}' \
    -X POST "$(api_url "$pod_idx")/write?db=${DB}&rp=${RP:-autogen}&precision=s" \
    --data-binary "$body" 2>/dev/null || echo 000
}

write_ts() {
  echo $(($(date +%s) + ${1:-0}))
}

query_post() {
  local pod_idx="$1"
  local q="$2"
  curl -sS -m "${CURL_TIMEOUT:-15}" -G "$(api_url "$pod_idx")/query" \
    --data-urlencode "db=${DB}" \
    --data-urlencode "q=${q}" 2>/dev/null || echo '{"results":[]}'
}

count_from_query() {
  local pod_idx="$1"
  local q="$2"
  query_post "$pod_idx" "$q" | python3 -c "
import json,sys
try:
  d=json.load(sys.stdin)
  results=d.get('results') or []
  if not results:
    print(0); sys.exit(0)
  s=results[0].get('series')
  if not s:
    print(0); sys.exit(0)
  vals=s[0].get('values') or []
  if not vals or not vals[0]:
    print(0); sys.exit(0)
  v=vals[0][0]
  print(int(v) if v is not None else 0)
except Exception:
  print(0)
" 2>/dev/null | head -n 1 || echo 0
}

count_from_query_checked() {
  local pod_idx="$1"
  local q="$2"
  query_post "$pod_idx" "$q" | python3 -c "
import json,sys
raw=sys.stdin.read()
try:
  d=json.loads(raw)
except Exception:
  print('ERR:unparseable response'); sys.exit(0)
if isinstance(d, dict) and d.get('error'):
  print('ERR:'+str(d['error'])[:160]); sys.exit(0)
results=d.get('results') or []
if not results:
  print('ERR:no results'); sys.exit(0)
s=results[0].get('series')
if not s:
  print('ERR:no series'); sys.exit(0)
vals=s[0].get('values') or []
if not vals or not vals[0]:
  print('ERR:empty values'); sys.exit(0)
v=vals[0][0]
print(v if v is not None else 'ERR:null value')
" 2>/dev/null || echo "ERR:curl failed"
}

# Scrape one pod's /metrics. Prints the body; returns non-zero if the scrape
# itself failed.
#
# An ABSENT counter is legitimately zero -- the metrics crate emits one only
# after its first increment -- but an unreachable pod yields nothing either.
# Collapsing the two lets a gate asserting "counter == 0" pass because nobody
# could be asked. See the matching note in e2e-common-posix.sh.
metrics_scrape() {
  local pod_idx="$1" body
  body=$(curl -sS -m "${CURL_TIMEOUT:-15}" "$(api_url "$pod_idx")/metrics" 2>/dev/null) || return 1
  [[ -n "$body" ]] || return 1
  printf '%s\n' "$body"
}

metric_value() {
  local pod_idx="$1"
  local name="$2"
  metrics_scrape "$pod_idx" | grep "^${name}" | awk '{print $2}' | head -1
}

# A pod's counter as an integer, or the literal string ERR when the pod could
# not be scraped. Callers must treat ERR as unknown, never as zero.
metric_counter() {
  local pod_idx="$1" name="$2" body v
  body=$(metrics_scrape "$pod_idx") || { echo ERR; return 0; }
  v=$(printf '%s\n' "$body" | grep "^${name}" | awk '{print $2}' | head -1)
  if [[ -n "$v" ]]; then
    # The exposition format renders counters as floats; gates compare integers.
    printf '%s\n' "${v%%.*}"
  else
    echo 0
  fi
}

# Highest value of `name` across pods, or ERR:unreachable=<list> when any pod
# could not be scraped.
metric_max_across_pods() {
  local name="$1"
  local max=0 v unreachable=""
  local i
  for ((i = 0; i < E2E_EXPECT_REPLICAS; i++)); do
    v=$(metric_counter "$i" "$name")
    if [[ "$v" == "ERR" ]]; then
      unreachable="${unreachable}${i},"
    elif [[ "$v" -gt "$max" ]] 2>/dev/null; then
      max=$v
    fi
  done
  if [[ -n "$unreachable" ]]; then
    echo "ERR:unreachable=${unreachable%,}"
  else
    echo "$max"
  fi
}

wait_for_count() {
  local pod_idx="$1"
  local q="$2"
  local min="$3"
  local timeout="${4:-45}"
  local deadline=$((SECONDS + timeout))
  local c=0
  while (( SECONDS < deadline )); do
    c=$(count_from_query "$pod_idx" "$q")
    if [[ "$c" -ge "$min" ]]; then
      echo "$c"
      return 0
    fi
    sleep 2
  done
  echo "$c"
  return 1
}

wait_for_pod_ping() {
  local pod_idx="$1"
  local timeout_secs="${2:-120}"
  local deadline=$((SECONDS + timeout_secs))
  while (( SECONDS < deadline )); do
    local code
    code=$(curl -sS -o /dev/null -w '%{http_code}' -m 3 "$(api_url "$pod_idx")/ping" || echo 000)
    if [[ "$code" == "204" ]]; then
      return 0
    fi
    sleep 3
  done
  return 1
}

node_health_state() {
  local pod_idx="$1"
  curl -sS -m 5 "$(api_url "$pod_idx")/health" 2>/dev/null | python3 -c "
import json,sys
try:
  d=json.load(sys.stdin)
  print(d.get('state',''))
except Exception:
  print('')
" 2>/dev/null || echo ""
}

hyperbytedb_cr_exists() {
  kubectl_ctx get hyperbytedbcluster hyperbytedb &>/dev/null
}

cr_spec_replicas() {
  if ! hyperbytedb_cr_exists; then
    echo 0
    return 0
  fi
  kubectl_ctx get hyperbytedbcluster hyperbytedb -o jsonpath='{.spec.replicas}' 2>/dev/null || echo 0
}

sts_replicas() {
  kubectl_ctx get statefulset -l app.kubernetes.io/name=hyperbytedb \
    -o jsonpath='{.items[0].spec.replicas}' 2>/dev/null || echo 0
}

running_pod_count() {
  kubectl_ctx get pods -l app.kubernetes.io/name=hyperbytedb --field-selector=status.phase=Running \
    -o json 2>/dev/null | python3 -c "
import json,sys
d=json.load(sys.stdin)
print(sum(1 for p in d.get('items',[]) if all(c.get('ready') for c in p['status'].get('containerStatuses',[]))))
" 2>/dev/null || echo 0
}

existing_pod_count() {
  kubectl_ctx get pods -l app.kubernetes.io/name=hyperbytedb --field-selector=status.phase=Running \
    -o json 2>/dev/null | python3 -c "
import json,sys
d=json.load(sys.stdin)
print(len(d.get('items',[])))
" 2>/dev/null || echo 0
}

pod_exists() {
  local ordinal="$1"
  if kubectl_ctx get pod "hyperbytedb-${ordinal}" &>/dev/null; then
    echo 1
  else
    echo 0
  fi
}

cluster_replica_counts_match() {
  local expect="$1"
  local cr sts
  if ! hyperbytedb_cr_exists; then
    warn "HyperbytedbCluster 'hyperbytedb' not found in namespace ${NS} — run: ./deploy/kind/setup.sh hdb-reset --sharded"
    return 1
  fi
  cr=$(cr_spec_replicas)
  sts=$(sts_replicas)
  if [[ "$cr" == "$expect" && "$sts" == "$expect" ]]; then
    return 0
  fi
  warn "Cluster at CR=${cr} STS=${sts}, expected ${expect} (run: ./deploy/kind/setup.sh hdb-reset --sharded)"
  return 1
}

wait_for_full_cluster() {
  local expect="${1:-$E2E_EXPECT_REPLICAS}"
  if ! cluster_replica_counts_match "$expect"; then
    return 1
  fi
  log "Waiting for ${expect}/${expect} pods and ${expect} active raft nodes..."
  kubectl_ctx wait --for=condition=ready pod -l app.kubernetes.io/name=hyperbytedb --timeout=600s || true
  local deadline=$((SECONDS + 300))
  while (( SECONDS < deadline )); do
    local ready cr sts
    ready=$(running_pod_count)
    if [[ "$ready" != "$expect" ]]; then
      cr=$(cr_spec_replicas)
      sts=$(sts_replicas)
      if [[ "$cr" != "$expect" || "$sts" != "$expect" ]]; then
        warn "Replica counts changed to CR=${cr} STS=${sts} (expected ${expect})"
        return 1
      fi
    fi
    if [[ "${IN_CLUSTER:-0}" != "1" ]]; then
      teardown_port_forwards
      setup_port_forwards
    fi
    local nodes active leader
    nodes=$(curl_api 0 "/cluster/nodes" 2>/dev/null | python3 -c "
import json,sys
try:
  d=json.load(sys.stdin)
  ns=d.get('nodes',[])
  print(len(ns), sum(1 for n in ns if n.get('state')=='active'))
except Exception:
  print('0 0')
" 2>/dev/null || echo "0 0")
    active=$(echo "$nodes" | awk '{print $2}')
    leader=$(curl_api 0 "/cluster/leader" 2>/dev/null | python3 -c "import json,sys; print(json.load(sys.stdin).get('leader_id') or '')" 2>/dev/null || true)
    if [[ "$ready" == "$expect" && "$active" == "$expect" && -n "$leader" && "$leader" != "None" ]]; then
      log "Cluster stable: ${ready} pods, ${active} active nodes, leader=$leader"
      return 0
    fi
    log "  ... ready=${ready}/${expect} active=${active}/${expect} leader=${leader:-none}"
    sleep 10
  done
  warn "Cluster not fully stable after 300s"
  return 1
}

wait_for_regions_rf_ok() {
  local min_rf="${1:-2}"
  local timeout_secs="${2:-120}"
  local deadline=$((SECONDS + timeout_secs))
  local rf
  while (( SECONDS < deadline )); do
    rf=$(shard_map_all_regions_rf_ok "$min_rf")
    if [[ "$rf" == "OK" ]]; then
      echo OK
      return 0
    fi
    sleep 5
  done
  shard_map_all_regions_rf_ok "$min_rf"
  return 0
}

shard_map_all_regions_rf_ok() {
  local min_rf="${1:-2}"
  curl_api 0 "/internal/shard/map" | python3 -c "
import json,sys
min_rf=int(sys.argv[1])
d=json.load(sys.stdin)
bad=[]
for sp in d.get('spaces',[]):
  for r in sp.get('regions',[]):
    if len(r.get('peers',[])) < min_rf:
      bad.append(r['region_id'])
if bad:
  print('FAIL:'+','.join(map(str,bad)))
else:
  print('OK')
" "$min_rf"
}

primary_distribution() {
  curl_api 0 "/internal/shard/map" | python3 -c "
import json,sys
from collections import Counter
d=json.load(sys.stdin)
primaries=[]
peer_sets=set()
for sp in d.get('spaces',[]):
  for r in sp.get('regions',[]):
    primaries.append(r.get('primary'))
    peer_sets.add(tuple(sorted(r.get('peers',[]))))
c=Counter(primaries)
print('primaries='+str(dict(c)))
print('distinct_peer_sets='+str(len(peer_sets)))
print('unique_primaries='+str(len(c)))
"
}

write_report() {
  local report_file="${REPORT_FILE:?REPORT_FILE must be set}"
  mkdir -p "$(dirname "$report_file")"
  {
    echo "# ${E2E_REPORT_TITLE}"
    echo ""
    echo "**Date:** $(date -u +%Y-%m-%dT%H:%M:%SZ)"
    local run_mode="port-forward"
    [[ "${IN_CLUSTER:-0}" == "1" ]] && run_mode="in-cluster"
    echo "**Cluster:** ${KUBE_CTX} (${E2E_CLUSTER_LABEL}, mode=${run_mode})"
    echo "**Database:** ${DB}"
    echo ""
    echo "| Goal | Result | Evidence |"
    echo "|------|--------|----------|"
    local i
    for i in "${!GOAL_IDS[@]}"; do
      echo "| ${GOAL_IDS[$i]} | ${GOAL_RESULTS[$i]} | ${GOAL_EVIDENCE[$i]} |"
    done
    echo ""
    if [[ "$FAILURES" -eq 0 ]]; then
      echo "**Overall:** PASS"
    else
      echo "**Overall:** FAIL ($FAILURES failures)"
    fi
  } >"$report_file"
  log "Report: $report_file"
}

launch_in_cluster_job() {
  local script_path="$1"
  local job_name="$2"
  local manifest_path="$3"
  local report_filename="$4"
  local manifests="$SCRIPT_DIR/manifests"
  log "Spawning in-cluster Job ${job_name}"
  kubectl_ctx create configmap "${job_name}-script" \
    --from-file="$(basename "$script_path")=$script_path" \
    --dry-run=client -o yaml | kubectl_ctx apply -f -
  kubectl_ctx apply -f "$manifests/sharding-e2e-rbac.yaml"
  kubectl_ctx delete job "$job_name" --ignore-not-found --wait=true 2>/dev/null || true
  kubectl_ctx apply -f "$manifest_path"
  kubectl_ctx wait --for=condition=ready pod -l "job-name=${job_name}" --timeout=300s
  local pod
  pod=$(kubectl_ctx get pod -l "job-name=${job_name}" -o jsonpath='{.items[0].metadata.name}')
  kubectl_ctx logs -f "$pod" || true
  local cond
  cond=$(kubectl_ctx get job "$job_name" -o jsonpath='{range .status.conditions[*]}{.type}={.status} {end}' 2>/dev/null || true)
  mkdir -p "$REPORT_DIR"
  kubectl_ctx cp "$NS/$pod:/tmp/${report_filename}" "$REPORT_DIR/$report_filename" 2>/dev/null || true
  if [[ "$cond" == *"Complete=True"* ]]; then
    return 0
  fi
  return 1
}
