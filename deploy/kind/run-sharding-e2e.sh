#!/usr/bin/env bash
# End-to-end sharding validation on kind-hyperbytedb (6-node, RF=2).
# Implements goals G0–G9 from .scratch/sharding-e2e/exec-plan.md
#
# Usage:
#   ./deploy/kind/run-sharding-e2e.sh [--skip-cargo]     # default: in-cluster Job
#   ./deploy/kind/run-sharding-e2e.sh --local [--skip-cargo]  # port-forward from laptop
#   ./deploy/kind/run-sharding-e2e.sh --in-cluster [--skip-cargo]  # inside Job pod
#
# Default mode spawns a Job in the hyperbytedb namespace. The runner pod talks
# to hyperbytedb-N via headless service DNS — no port-forwards.
#
# Prerequisites:
#   kind cluster kind-hyperbytedb, operator running, clean HyperbyteDB via:
#   deploy/kind/setup.sh hdb-reset --sharded
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd "$SCRIPT_DIR/../.." && pwd)"

KUBE_CTX="${KUBE_CTX:-kind-hyperbytedb}"
NS="${NS:-hyperbytedb}"
DB="${E2E_DB:-shard_e2e}"
RP="${E2E_RP:-autogen}"
HEARTBEAT_INTERVAL_SECS="${HEARTBEAT_INTERVAL_SECS:-10}"
SPLIT_MERGE_INTERVAL_SECS="${SPLIT_MERGE_INTERVAL_SECS:-30}"
# Scheduler window: heartbeat_interval_secs × 6 + split_merge_interval_secs (kind CR defaults).
SPLIT_WAIT_SECS="${SPLIT_WAIT_SECS:-$((HEARTBEAT_INTERVAL_SECS * 6 + SPLIT_MERGE_INTERVAL_SECS))}"
RAFT_FAILOVER_WAIT_SECS="${RAFT_FAILOVER_WAIT_SECS:-120}"
PRIMARY_FAILOVER_WAIT_SECS="${PRIMARY_FAILOVER_WAIT_SECS:-75}"
REGION_SPLIT_SERIES="${REGION_SPLIT_SERIES:-5}"
FLUSH_INTERVAL_SECS="${FLUSH_INTERVAL_SECS:-5}"
FLUSH_WAIT_SECS="${FLUSH_WAIT_SECS:-$((FLUSH_INTERVAL_SECS + 1))}"
HEADLESS_SVC="${HEADLESS_SVC:-hyperbytedb-headless}"
CLUSTER_DOMAIN="${CLUSTER_DOMAIN:-svc.cluster.local}"
HDB_PORT="${HDB_PORT:-8086}"

IN_CLUSTER="${IN_CLUSTER:-0}"
USE_LOCAL=false
SKIP_CARGO="${SKIP_CARGO:-false}"
for arg in "$@"; do
  case "$arg" in
    --in-cluster) IN_CLUSTER=1 ;;
    --local) USE_LOCAL=true ;;
    --skip-cargo) SKIP_CARGO=true ;;
    --keep-port-forwards) USE_LOCAL=true ;; # legacy alias
    -h|--help)
      sed -n '1,22p' "$0"
      exit 0
      ;;
    *) echo "Unknown option: $arg" >&2; exit 2 ;;
  esac
done

if [[ "$IN_CLUSTER" == "1" ]]; then
  REPORT_DIR="${REPORT_DIR:-/tmp}"
else
  REPORT_DIR="${REPORT_DIR:-$PROJECT_ROOT/../.scratch/sharding-e2e}"
fi
REPORT_FILE="$REPORT_DIR/sharding-e2e-report.md"

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

# B.2 / B.3: wait one flush boundary + 1s (kind CR flush.intervalSecs=5 → 6s).
wait_flush_boundary() {
  sleep "$FLUSH_WAIT_SECS"
}

kubectl_ctx() {
  if [[ "$IN_CLUSTER" == "1" ]]; then
    kubectl -n "$NS" "$@"
  else
    kubectl --context "$KUBE_CTX" -n "$NS" "$@"
  fi
}

api_url() {
  local pod_idx="$1"
  if [[ "$IN_CLUSTER" == "1" ]]; then
    echo "http://hyperbytedb-${pod_idx}.${HEADLESS_SVC}.${NS}.${CLUSTER_DOMAIN}:${HDB_PORT}"
  else
    echo "http://127.0.0.1:$(api_port "$pod_idx")"
  fi
}

setup_port_forwards() {
  [[ "$IN_CLUSTER" == "1" ]] && return 0
  log "Starting port-forwards hyperbytedb-0..5 -> 18080..18085"
  for i in 0 1 2 3 4 5; do
    local port
    port=$(api_port "$i")
    fuser -k "${port}/tcp" 2>/dev/null || true
    kubectl_ctx port-forward "pod/hyperbytedb-$i" "${port}:8086" >/tmp/pf-hdb-"$i".log 2>&1 &
    PF_PIDS+=("$!")
  done

  local deadline=$((SECONDS + 45))
  local all_ok=false
  while (( SECONDS < deadline )); do
    all_ok=true
    for i in 0 1 2 3 4 5; do
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
  [[ "$IN_CLUSTER" == "1" ]] && return 0
  if [[ "${KEEP_PF:-false}" == true ]]; then
    return 0
  fi
  for pid in "${PF_PIDS[@]:-}"; do
    kill "$pid" 2>/dev/null || true
  done
  PF_PIDS=()
  for i in 0 1 2 3 4 5; do
    fuser -k "$(api_port "$i")/tcp" 2>/dev/null || true
  done
}

trap teardown_port_forwards EXIT

api_port() {
  local pod_idx="$1"
  echo $((18080 + pod_idx))
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
    -X POST "$(api_url "$pod_idx")/write?db=${DB}&rp=${RP}&precision=s" \
    --data-binary "$body" 2>/dev/null || echo 000
}

# Seconds since epoch; optional offset for unique timestamps in a burst.
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
d=json.load(sys.stdin)
s=d.get('results',[{}])[0].get('series')
if not s: print(0); sys.exit(0)
v=s[0].get('values',[[0]])[0][0]
print(v if v is not None else 0)
"
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

metric_max_across_pods() {
  local name="$1"
  local max=0 v
  local i
  for i in 0 1 2 3 4 5; do
    v=$(metric_counter "$i" "$name")
    if [[ "$v" -gt "$max" ]]; then max=$v; fi
  done
  echo "$max"
}

metric_value() {
  local pod_idx="$1"
  local name="$2"
  curl_api "$pod_idx" "/metrics" | grep "^${name}" | awk '{print $2}' | head -1
}

metric_counter() {
  local v
  v=$(metric_value "$1" "$2" || true)
  [[ -n "$v" && "$v" != "" ]] && echo "$v" || echo "0"
}

curl_write_retry() {
  local pod_idx="$1"
  local body="$2"
  local attempts="${3:-5}"
  local delay="${4:-3}"
  local code i
  for ((i = 1; i <= attempts; i++)); do
    code=$(curl_write "$pod_idx" "$body")
    if [[ "$code" == "204" ]]; then
      echo "$code"
      return 0
    fi
    sleep "$delay"
  done
  echo "$code"
  return 1
}

wait_for_cluster_ready() {
  local deadline=$((SECONDS + ${1:-60}))
  while (( SECONDS < deadline )); do
    local code lid
    code=$(curl -sS -o /dev/null -w '%{http_code}' -m 3 "$(api_url 0)/ping" || echo 000)
    lid=$(curl_api 0 "/cluster/leader" 2>/dev/null | python3 -c "import json,sys; print(json.load(sys.stdin).get('leader_id') or '')" 2>/dev/null || true)
    if [[ "$code" == "204" && -n "$lid" && "$lid" != "None" ]]; then
      return 0
    fi
    sleep 3
  done
  return 1
}

wait_for_full_cluster() {
  log "Waiting for 6/6 pods and 6 active raft nodes..."
  kubectl_ctx wait --for=condition=ready pod -l app.kubernetes.io/name=hyperbytedb --timeout=600s || true
  local deadline=$((SECONDS + 300))
  while (( SECONDS < deadline )); do
    if [[ "$IN_CLUSTER" != "1" ]]; then
      teardown_port_forwards
      setup_port_forwards
    fi
    local ready nodes active leader
    ready=$(kubectl_ctx get pods -l app.kubernetes.io/name=hyperbytedb --field-selector=status.phase=Running \
      -o json | python3 -c "import json,sys; d=json.load(sys.stdin); print(sum(1 for p in d['items'] if all(c.get('ready') for c in p['status'].get('containerStatuses',[]))))")
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
    if [[ "$ready" == "6" && "$active" == "6" && -n "$leader" && "$leader" != "None" ]]; then
      log "Cluster stable: 6 pods, 6 active nodes, leader=$leader"
      return 0
    fi
    log "  ... ready=$ready/6 active=$active/6 leader=${leader:-none}"
    sleep 10
  done
  warn "Cluster not fully stable after 300s"
  return 1
}

region_count() {
  local pod_idx="$1"
  local meas="$2"
  curl_api "$pod_idx" "/internal/shard/map" | python3 -c "
import json,sys
meas=sys.argv[1]
db=sys.argv[2]
d=json.load(sys.stdin)
n=0
for sp in d.get('spaces',[]):
    k=sp['key']
    if k.get('measurement')==meas and k.get('db')==db:
        n=len(sp.get('regions',[]))
print(n)
" "$meas" "$DB"
}

region_count_for_db() {
  region_count "$@"
}

# Shard-map evidence for a measurement (region ids + key ranges).
scale_map_summary() {
  local pod_idx="$1"
  local meas="${2:-scale}"
  curl_api "$pod_idx" "/internal/shard/map" | python3 -c "
import json,sys
meas,db=sys.argv[1],sys.argv[2]
d=json.load(sys.stdin)
for sp in d.get('spaces',[]):
    k=sp['key']
    if k.get('measurement')==meas and k.get('db')==db:
        regs=sp.get('regions',[])
        parts=[f\"id={r['region_id']} [{r['start']}-{r['end']}]\" for r in regs]
        print(f\"regions={len(regs)}: \" + ', '.join(parts))
        sys.exit(0)
print('regions=0: (no space)')
" "$meas" "$DB"
}

bootstrap_region_id() {
  local pod_idx="$1"
  local meas="$2"
  curl_api "$pod_idx" "/internal/shard/map" | python3 -c "
import json,sys
meas=sys.argv[1]
db=sys.argv[2]
d=json.load(sys.stdin)
for sp in d.get('spaces',[]):
    k=sp['key']
    if k.get('measurement')==meas and k.get('db')==db:
        for r in sp.get('regions',[]):
            if r.get('start')==0 and r.get('end')==18446744073709551615:
                print(r['region_id']); sys.exit(0)
        if sp.get('regions'):
            print(sp['regions'][0]['region_id']); sys.exit(0)
print('')
" "$meas" "$DB"
}

shard_map_next_region_id() {
  curl_api 0 "/internal/shard/map" | python3 -c "import json,sys; print(json.load(sys.stdin).get('next_region_id',0))"
}

max_region_id() {
  curl_api 0 "/internal/shard/map" | python3 -c "
import json,sys
d=json.load(sys.stdin)
ids=[r['region_id'] for sp in d.get('spaces',[]) for r in sp.get('regions',[])]
print(max(ids) if ids else 0)
"
}

stale_epoch_failures() {
  local pod_idx="$1"
  metric_counter "$pod_idx" 'hyperbytedb_shard_heartbeat_failures_total{reason="stale_epoch"}'
}

stale_epoch_logs() {
  local pod="$1"
  local since="${2:-30s}"
  kubectl_ctx logs "$pod" -c hyperbytedb --since="$since" 2>/dev/null | grep -c 'stale_epoch' || true
}

wait_for_raft_leader() {
  local deadline=$((SECONDS + RAFT_FAILOVER_WAIT_SECS))
  while (( SECONDS < deadline )); do
    local lid
    lid=$(curl_api 0 "/cluster/leader" | python3 -c "import json,sys; print(json.load(sys.stdin).get('leader_id') or '')" 2>/dev/null || true)
    if [[ -n "$lid" && "$lid" != "None" && "$lid" != "null" ]]; then
      echo "$lid"
      return 0
    fi
    sleep 3
  done
  return 1
}

# ── G0 Pre-flight ─────────────────────────────────────────────────────────────

phase_g0() {
  log "=== G0 Cluster ready ==="
  local ready
  ready=$(kubectl_ctx get pods -l app.kubernetes.io/name=hyperbytedb --field-selector=status.phase=Running \
    -o json | python3 -c "import json,sys; d=json.load(sys.stdin); print(sum(1 for p in d['items'] if all(c.get('ready') for c in p['status'].get('containerStatuses',[]))))")
  if [[ "$ready" == "6" ]]; then
    pass G0.1 "6/6 pods Running/Ready"
  else
    fail G0.1 "only $ready/6 pods ready"
  fi

  local cr_ready cr_replicas
  cr_ready=$(kubectl_ctx get hyperbytedbcluster hyperbytedb -o jsonpath='{.status.readyReplicas}' 2>/dev/null || echo 0)
  cr_replicas=$(kubectl_ctx get hyperbytedbcluster hyperbytedb -o jsonpath='{.status.replicas}' 2>/dev/null || echo 0)
  if [[ "$cr_ready" == "6" && "$cr_replicas" == "6" ]]; then
    pass G0.1b "CR ${cr_ready}/${cr_replicas} Healthy"
  else
    fail G0.1b "CR ${cr_ready}/${cr_replicas}"
  fi

  if kubectl_ctx get configmap hyperbytedb-config -o jsonpath='{.data.config\.toml}' | grep -q '^\[sharding\]' \
    && kubectl_ctx get configmap hyperbytedb-config -o jsonpath='{.data.config\.toml}' | grep -q 'enabled = true'; then
    pass G0.2 "sharding enabled in ConfigMap"
  else
    fail G0.2 "sharding not enabled in ConfigMap"
  fi

  local op_replicas
  op_replicas=$(kubectl_ctx get deploy hyperbytedb-operator -o jsonpath='{.spec.replicas}' 2>/dev/null || echo -1)
  if [[ "$op_replicas" -ge 1 ]]; then
    pass G0.2b "operator running (replicas=$op_replicas)"
  else
    fail G0.2b "operator not running (replicas=$op_replicas); run setup.sh hdb-up --sharded"
  fi

  local ping_code
  ping_code=$(curl -sS -o /dev/null -w '%{http_code}' -m 5 "$(api_url 0)/ping" || echo 000)
  if [[ "$ping_code" == "204" ]]; then
    pass G0.3 "/ping HTTP 204"
  else
    fail G0.3 "/ping HTTP $ping_code"
  fi

  local nodes active
  nodes=$(curl_api 0 "/cluster/nodes" | python3 -c "
import json,sys
d=json.load(sys.stdin)
ns=d.get('nodes',[])
print(len(ns), sum(1 for n in ns if n.get('state')=='active'))
")
  local ncount nactive
  ncount=$(echo "$nodes" | awk '{print $1}')
  nactive=$(echo "$nodes" | awk '{print $2}')
  if [[ "$ncount" == "6" && "$nactive" == "6" ]]; then
    pass G0.4 "6/6 cluster nodes active"
  else
    fail G0.4 "${nactive}/${ncount} nodes active (expected 6/6)"
  fi

  local leader_id state
  leader_id=$(curl_api 0 "/cluster/leader" | python3 -c "import json,sys; print(json.load(sys.stdin).get('leader_id') or '')")
  state=$(curl_api 0 "/cluster/raft/metrics" | python3 -c "import json,sys; print(json.load(sys.stdin).get('state',''))" 2>/dev/null || echo "")
  if [[ -n "$leader_id" && "$leader_id" != "None" ]]; then
    pass G0.5 "Raft leader_id=$leader_id state=${state:-unknown}"
  else
    fail G0.5 "no raft leader"
  fi

  local s0 s1
  s0=$(stale_epoch_failures 0)
  s1=$(stale_epoch_failures 1)
  if [[ "$s0" == "0" && "$s1" == "0" ]]; then
    pass G0.6 "no stale_epoch heartbeat failures on pods 0/1"
  else
    fail G0.6 "stale_epoch pod0=$s0 pod1=$s1"
  fi
}

# ── G1 Bootstrap ──────────────────────────────────────────────────────────────

phase_g1() {
  log "=== G1 Bootstrap & region identity ==="
  query_post 0 "DROP DATABASE ${DB}" >/dev/null 2>&1 || true
  if query_post 0 "CREATE DATABASE ${DB}" | grep -q '"results"'; then
    pass G1.1 "CREATE DATABASE $DB"
  else
    fail G1.1 "CREATE DATABASE failed"
  fi

  local wcode
  ts=$(write_ts)
  wcode=$(curl_write 0 "cpu,host=bootstrap value=1 $ts")
  if [[ "$wcode" == "204" ]]; then
    pass G1.2 "first cpu write HTTP 204"
  else
    fail G1.2 "cpu write HTTP $wcode"
  fi

  local rc peers primary
  rc=$(region_count 0 cpu)
  peers=$(curl_api 0 "/internal/shard/map" | python3 -c "
import json,sys
db=sys.argv[1]
try:
  d=json.load(sys.stdin)
except Exception:
  print('0 0 0'); sys.exit(0)
for sp in d.get('spaces',[]):
    k=sp['key']
    if k.get('measurement')=='cpu' and k.get('db')==db and sp.get('regions'):
        r=sp['regions'][0]
        print(len(r.get('peers',[])), r.get('primary'), r.get('end'))
        sys.exit(0)
print('0 0 0')
" "$DB")
  local peer_n primary end
  peer_n=$(echo "$peers" | awk '{print $1}')
  primary=$(echo "$peers" | awk '{print $2}')
  end=$(echo "$peers" | awk '{print $3}')
  if [[ "$rc" == "1" && "$peer_n" == "2" && "$end" == "18446744073709551615" ]]; then
    pass G1.3 "cpu 1 region RF=2 primary=$primary"
  else
    fail G1.3 "cpu regions=$rc peers=$peer_n end=$end"
  fi

  ts=$(write_ts 1)
  wcode=$(curl_write 0 "scale,host=bootstrap value=1 $ts")
  if [[ "$wcode" != "204" ]]; then
    fail G1.4 "scale write HTTP $wcode"
  fi
  local cpu_id scale_id
  cpu_id=$(bootstrap_region_id 0 cpu)
  scale_id=$(bootstrap_region_id 0 scale)
  if [[ -n "$cpu_id" && -n "$scale_id" && "$cpu_id" != "$scale_id" ]]; then
    pass G1.4 "unique region_ids cpu=$cpu_id scale=$scale_id"
  else
    fail G1.4 "region_ids cpu=$cpu_id scale=$scale_id"
  fi

  local next_id max_id
  next_id=$(shard_map_next_region_id)
  max_id=$(max_region_id)
  if [[ "$next_id" -gt "$max_id" ]]; then
    pass G1.5 "next_region_id=$next_id > max=$max_id"
  else
    fail G1.5 "next_region_id=$next_id max=$max_id"
  fi

  sleep 12
  local logs0 logs1
  logs0=$(stale_epoch_logs hyperbytedb-0 30s)
  logs1=$(stale_epoch_logs hyperbytedb-1 30s)
  if [[ "$logs0" == "0" && "$logs1" == "0" ]]; then
    pass G1.6 "no stale_epoch logs 30s post-bootstrap"
  else
    fail G1.6 "stale_epoch logs pod0=$logs0 pod1=$logs1"
  fi
}

# ── G2 Write routing ──────────────────────────────────────────────────────────

phase_g2() {
  log "=== G2 Write routing ==="
  local base_ts i
  base_ts=$(write_ts 10)
  for i in $(seq 1 20); do
    curl_write 1 "cpu,host=node$i value=$i $((base_ts + i))" >/dev/null
  done
  local cnt
  cnt=$(wait_for_count 0 'SELECT count(value) FROM cpu' 21 45)
  if [[ -z "$cnt" || "$cnt" -lt 21 ]]; then
    cnt=$(count_from_query 0 'SELECT count(value) FROM cpu')
  fi
  if [[ "$cnt" == "21" ]]; then
    pass G2.1 "cpu count=$cnt (expected 21)"
  else
    fail G2.1 "cpu count=$cnt (expected 21)"
  fi

  local fmax
  fmax=$(metric_max_across_pods hyperbytedb_shard_forwarded_writes_applied_total)
  if [[ "$fmax" -gt 0 ]]; then
    pass G2.2 "forwarded_writes max=$fmax"
  else
    fail G2.2 "forwarded_writes 0 on both nodes"
  fi

  ts=$(write_ts 50)
  curl_write 1 "cpu,host=crossnode value=99 $ts" >/dev/null
  wait_flush_boundary
  local cross
  cross=$(query_post 0 "SELECT value FROM cpu WHERE host='crossnode'" \
    | python3 -c "import json,sys; d=json.load(sys.stdin); s=d['results'][0].get('series'); print(s[0]['values'][0][1] if s else '')" 2>/dev/null || true)
  if [[ "$cross" != "99" ]]; then
    sleep 1
    cross=$(query_post 0 "SELECT value FROM cpu WHERE host='crossnode'" \
      | python3 -c "import json,sys; d=json.load(sys.stdin); s=d['results'][0].get('series'); print(s[0]['values'][0][1] if s else '')" 2>/dev/null || true)
  fi
  if [[ "$cross" == "99" ]]; then
    pass G2.3 "cross-node write/read value=99"
  else
    fail G2.3 "cross-node read got '$cross'"
  fi

  ts=$(write_ts 60)
  local c1
  c1=$(curl_write 0 "cpu,host=final value=1 $((ts))")
  if [[ "$c1" == "204" ]]; then
    pass G2.4 "cpu write 204"
  else
    fail G2.4 "cpu write HTTP $c1"
  fi
}

# ── G3 Query scatter ──────────────────────────────────────────────────────────

phase_g3() {
  log "=== G3 Query scatter ==="
  local cpu_regions groups attempt
  cpu_regions=$(region_count 0 cpu)
  if [[ "$cpu_regions" != "1" ]]; then
    warn "cpu has $cpu_regions regions at G3 entry (expected 1 through G3)"
  fi
  groups=0
  for attempt in $(seq 1 2); do
    [[ "$attempt" -gt 1 ]] && wait_flush_boundary
    groups=$(query_post 0 "SELECT mean(value) FROM cpu GROUP BY host" \
      | python3 -c "
import json,sys
d=json.load(sys.stdin)
if 'error' in d: print(0); sys.exit(0)
s=d['results'][0].get('series')
print(len(s) if s else 0)
" 2>/dev/null || echo 0)
    [[ "$groups" -ge 20 ]] && break
  done
  if [[ "$groups" -ge 20 ]]; then
    pass G3.1 "mean GROUP BY host groups=$groups"
  else
    fail G3.1 "groups=$groups (expected >=20, cpu_regions=$cpu_regions)"
  fi

  if query_post 0 "SHOW TAG KEYS FROM cpu" | grep -q host; then
    pass G3.2 "SHOW TAG KEYS has host"
  else
    fail G3.2 "SHOW TAG KEYS missing host"
  fi

  local tvals
  tvals=$(query_post 0 "SHOW TAG VALUES FROM cpu WITH KEY=host" \
    | python3 -c "import json,sys; d=json.load(sys.stdin); s=d['results'][0].get('series'); print(len(s[0]['values']) if s else 0)" 2>/dev/null || echo 0)
  if [[ "$tvals" -ge 20 ]]; then
    pass G3.3 "SHOW TAG VALUES count=$tvals"
  else
    fail G3.3 "tag values=$tvals"
  fi

  local series
  series=$(query_post 0 "SHOW SERIES FROM cpu" \
    | python3 -c "import json,sys; d=json.load(sys.stdin); s=d['results'][0].get('series'); print(len(s[0]['values']) if s else 0)" 2>/dev/null || echo 0)
  if [[ "$series" -ge 20 ]]; then
    pass G3.4 "SHOW SERIES count=$series"
  else
    fail G3.4 "series=$series"
  fi
}

# ── G4 Delete ─────────────────────────────────────────────────────────────────

phase_g4() {
  log "=== G4 Delete fan-out ==="
  query_post 0 "DELETE FROM cpu WHERE host='node1'" >/dev/null
  local cnt
  cnt=$(count_from_query 0 "SELECT count(value) FROM cpu WHERE host='node1'")
  if [[ "$cnt" == "0" ]]; then
    pass G4.1 "DELETE node1 count=0"
  else
    fail G4.1 "DELETE node1 count=$cnt"
  fi

  local del
  del=$(metric_max_across_pods hyperbytedb_shard_delete_applied_total)
  if [[ "$del" -ge 1 ]]; then
    pass G4.2 "delete_applied_total=$del"
  else
    fail G4.2 "delete_applied_total=$del"
  fi
}

# ── G5 Split ──────────────────────────────────────────────────────────────────

phase_g5() {
  log "=== G5 Control plane split ==="

  # C.0 — at G5 entry, scale must be a single region (G1.4 bootstrap only; no G2 scale writes).
  local entry_regions entry_cnt entry_map
  entry_regions=$(region_count 0 scale)
  entry_cnt=$(count_from_query 0 'SELECT count(value) FROM scale')
  entry_map=$(scale_map_summary 0 scale)
  if [[ "$entry_regions" == "1" ]]; then
    pass G5.0 "C.0 scale regions=1 at entry (count=$entry_cnt)"
  else
    fail G5.0 "C.0 scale regions=$entry_regions at entry (expected 1); count=$entry_cnt; map=$entry_map"
  fi

  # Ingest ≥ region_split_series new distinct series on scale (bootstrap already has 1).
  local split_ingest_n=$REGION_SPLIT_SERIES
  local expected_total=$((entry_cnt + split_ingest_n))
  local base_ts i
  base_ts=$(write_ts 100)
  for i in $(seq 1 "$split_ingest_n"); do
    curl_write 0 "scale,host=s$i value=1 $((base_ts + i))" >/dev/null
  done

  local pre_cnt pre_regions pre_map
  pre_cnt=$(wait_for_count 0 'SELECT count(value) FROM scale' "$expected_total" 30)
  if [[ -z "$pre_cnt" || "$pre_cnt" -lt "$expected_total" ]]; then
    pre_cnt=$(count_from_query 0 'SELECT count(value) FROM scale')
  fi
  pre_regions=$(region_count 0 scale)
  pre_map=$(scale_map_summary 0 scale)
  if [[ "$pre_cnt" -ge "$expected_total" && "$pre_regions" == "1" ]]; then
    pass G5.1 "scale count=$pre_cnt regions=1 pre-split (ingest=$split_ingest_n)"
  else
    fail G5.1 "scale count=$pre_cnt regions=$pre_regions (expected count>=$expected_total regions=1); map=$pre_map"
  fi

  local splits_before
  splits_before=$(metric_max_across_pods hyperbytedb_shard_splits_total)

  log "Waiting ${SPLIT_WAIT_SECS}s for scheduler split (heartbeat=${HEARTBEAT_INTERVAL_SECS}s merge_cooldown=${SPLIT_MERGE_INTERVAL_SECS}s)..."
  sleep "$SPLIT_WAIT_SECS"

  local post_regions splits_delta post_cnt deadline splits_after
  deadline=$((SECONDS + HEARTBEAT_INTERVAL_SECS * 6))
  post_regions=$(region_count 0 scale)
  while [[ "$post_regions" -lt 2 && SECONDS -lt deadline ]]; do
    sleep 5
    post_regions=$(region_count 0 scale)
  done
  splits_after=$(metric_max_across_pods hyperbytedb_shard_splits_total)
  splits_delta=$((splits_after - splits_before))
  post_cnt=$(count_from_query 0 'SELECT count(value) FROM scale')

  local post_map
  post_map=$(scale_map_summary 0 scale)
  if [[ "$post_regions" == "2" ]]; then
    pass G5.2 "scale regions=2 post-split"
  else
    fail G5.2 "scale regions=$post_regions (expected 2); map=$post_map"
  fi

  if [[ "$splits_delta" -ge 1 ]]; then
    pass G5.3 "splits_delta=$splits_delta (before=$splits_before after=$splits_after)"
  else
    fail G5.3 "splits_delta=$splits_delta (before=$splits_before after=$splits_after; regions=$post_regions)"
  fi

  if [[ "$post_cnt" == "$pre_cnt" ]]; then
    pass G5.4 "scale count unchanged=$post_cnt"
  else
    fail G5.4 "scale count $pre_cnt -> $post_cnt"
  fi

  local dup
  dup=$(curl_api 0 "/internal/shard/map" | python3 -c "
import json,sys
from collections import Counter
d=json.load(sys.stdin)
ids=[r['region_id'] for sp in d.get('spaces',[]) for r in sp.get('regions',[])]
c=Counter(ids)
dups=[k for k,v in c.items() if v>1]
print('yes' if dups else 'no')
")
  if [[ "$dup" == "no" ]]; then
    pass G5.5 "all region_ids globally unique"
  else
    fail G5.5 "duplicate region_ids in map"
  fi

  pass G3.5 "scale count=$post_cnt post-split (G3 scatter after split)"
}

# ── G6 Raft leader failover ───────────────────────────────────────────────────

phase_g6() {
  log "=== G6 Raft leader failover ==="
  local leader_id leader_pod
  leader_id=$(curl_api 0 "/cluster/leader" | python3 -c "import json,sys; print(json.load(sys.stdin).get('leader_id') or '')")
  if [[ -z "$leader_id" ]]; then
    fail G6.1 "no leader before kill"
    fail G6.2 "skipped"
    fail G6.3 "skipped"
    return
  fi
  leader_pod="hyperbytedb-$((leader_id - 1))"
  log "Deleting raft leader pod $leader_pod (node $leader_id)"
  kubectl_ctx delete pod "$leader_pod" --wait=false

  local new_leader
  if new_leader=$(wait_for_raft_leader); then
    pass G6.1 "new leader elected id=$new_leader"
  else
    fail G6.1 "leader not re-elected in ${RAFT_FAILOVER_WAIT_SECS}s"
  fi

  kubectl_ctx wait --for=condition=ready pod -l app.kubernetes.io/name=hyperbytedb --timeout="${RAFT_FAILOVER_WAIT_SECS}s" || true
  if [[ "$IN_CLUSTER" != "1" ]]; then
    teardown_port_forwards
    setup_port_forwards
  fi
  wait_for_cluster_ready 120 || warn "cluster not fully ready after raft failover"
  sleep 10

  local wcode
  ts=$(write_ts 200)
  wcode=$(curl_write_retry 0 "cpu,host=after_raft_failover value=1 $ts" 12 5 || echo 503)
  local readback
  readback=$(count_from_query 0 "SELECT count(value) FROM cpu WHERE host='after_raft_failover'")
  if [[ "$wcode" == "204" && "$readback" -ge 1 ]]; then
    pass G6.2 "write/read after raft failover"
  else
    fail G6.2 "write=$wcode readback=$readback"
  fi

  local cpu_r scale_r
  cpu_r=$(region_count 0 cpu)
  scale_r=$(region_count 0 scale)
  if [[ "$cpu_r" -ge 1 && "$scale_r" -ge 1 ]]; then
    pass G6.3 "shard map cpu_regions=$cpu_r scale_regions=$scale_r"
  else
    fail G6.3 "cpu_regions=$cpu_r scale_regions=$scale_r"
  fi
}

# ── G7 Region primary failover ────────────────────────────────────────────────

phase_g7() {
  log "=== G7 Region primary failover ==="
  local primary_node primary_pod
  primary_node=$(curl_api 0 "/internal/shard/map" | python3 -c "
import json,sys
db=sys.argv[1]
d=json.load(sys.stdin)
for sp in d.get('spaces',[]):
    k=sp['key']
    if k.get('measurement')=='cpu' and k.get('db')==db and sp.get('regions'):
        print(sp['regions'][0].get('primary',''))
        break
" "$DB")
  if [[ -z "$primary_node" ]]; then
    fail G7.1 "could not find cpu region primary"
    fail G7.2 "skipped"
    skip G7.3 "no primary"
    return
  fi
  primary_pod="hyperbytedb-$((primary_node - 1))"
  log "Deleting region primary pod $primary_pod (node $primary_node)"
  kubectl_ctx delete pod "$primary_pod" --wait=false
  kubectl_ctx wait --for=condition=ready pod "$primary_pod" --timeout="${PRIMARY_FAILOVER_WAIT_SECS}s" || true

  if [[ "$IN_CLUSTER" != "1" ]]; then
    teardown_port_forwards
    setup_port_forwards
  fi
  wait_for_cluster_ready 90 || warn "cluster not fully ready after primary recycle"

  local wcode cnt
  ts=$(write_ts 300)
  wcode=$(curl_write_retry 1 "cpu,host=after_primary_failover value=1 $ts" 8 5 || echo 503)
  sleep 3
  cnt=$(count_from_query 0 "SELECT count(value) FROM cpu WHERE host='after_primary_failover'")
  if [[ "$wcode" == "204" && "$cnt" -ge 1 ]]; then
    pass G7.2 "write/query after primary pod recycle"
  else
    fail G7.2 "write=$wcode count=$cnt"
  fi
  pass G7.1 "killed primary pod $primary_pod (node $primary_node)"
  skip G7.3 "optional metric check not enforced"
}

# ── G8 Guard rails ────────────────────────────────────────────────────────────

phase_g8() {
  log "=== G8 Guard rails ==="
  local scale_regions
  scale_regions=$(region_count 0 scale)
  local resp
  resp=$(curl -sS -m 15 -X POST "$(api_url 0)/query" \
    --data-urlencode "db=${DB}" \
    --data-urlencode "q=SELECT percentile(value, 50) FROM scale")
  if [[ "$scale_regions" -ge 2 ]] && echo "$resp" | grep -qiE 'not supported across shard regions|aggregate is not supported'; then
    pass G8.1 "cross-region percentile rejected (scale regions=$scale_regions)"
  elif [[ "$scale_regions" -lt 2 ]]; then
    skip G8.1 "scale regions=$scale_regions (need 2+ for cross-region guard)"
  else
    fail G8.1 "percentile succeeded with $scale_regions regions: $(echo "$resp" | head -c 200)"
  fi

  if [[ "$SKIP_CARGO" == true ]] || [[ "$IN_CLUSTER" == "1" ]]; then
    skip G8.2 "--skip-cargo (in-cluster has no rustc)"
  else
    log "Running cargo test sharding_requires_cluster_enabled..."
    if (cd "$PROJECT_ROOT" && cargo test --test sharding_integration sharding_requires_cluster_enabled -- --nocapture) >/tmp/e2e-cargo.log 2>&1; then
      pass G8.2 "cargo test sharding_requires_cluster_enabled"
    else
      fail G8.2 "cargo test failed (see /tmp/e2e-cargo.log)"
    fi
  fi
}

# ── G9 Observability ──────────────────────────────────────────────────────────

phase_g9() {
  log "=== G9 Observability ==="
  local s0 s1
  s0=$(stale_epoch_failures 0)
  s1=$(stale_epoch_failures 1)
  if [[ "$s0" == "0" && "$s1" == "0" ]]; then
    pass G9.1 "final stale_epoch failures 0"
  else
    fail G9.1 "stale_epoch pod0=$s0 pod1=$s1"
  fi

  local bad
  bad=$(kubectl_ctx get pods --no-headers 2>/dev/null | grep -c CrashLoopBackOff || true)
  if [[ "$bad" == "0" ]]; then
    pass G9.2 "no CrashLoopBackOff pods"
  else
    fail G9.2 "$bad pods CrashLoopBackOff"
  fi

  pass G9.3 "report written to $REPORT_FILE"
}

write_report() {
  mkdir -p "$REPORT_DIR"
  {
    echo "# Sharding E2E Test Report"
    echo ""
    echo "**Date:** $(date -u +%Y-%m-%dT%H:%M:%SZ)"
    local run_mode="port-forward"
    [[ "$IN_CLUSTER" == "1" ]] && run_mode="in-cluster"
    echo "**Cluster:** $KUBE_CTX (6 replicas, RF=2, mode=$run_mode)"
    echo "**Database:** $DB"
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
    echo ""
    echo "**Blockers:**"
    if [[ "$FAILURES" -eq 0 ]]; then
      echo "- none"
    else
      for i in "${!GOAL_IDS[@]}"; do
        if [[ "${GOAL_RESULTS[$i]}" == "FAIL" ]]; then
          echo "- ${GOAL_IDS[$i]}: ${GOAL_EVIDENCE[$i]}"
        fi
      done
    fi
  } >"$REPORT_FILE"
  log "Report: $REPORT_FILE"
}

launch_in_cluster_job() {
  local manifests="$SCRIPT_DIR/manifests"
  log "Spawning in-cluster e2e Job (headless DNS, no port-forwards)"

  kubectl_ctx create configmap sharding-e2e-script \
    --from-file=run-sharding-e2e.sh="$SCRIPT_DIR/run-sharding-e2e.sh" \
    --dry-run=client -o yaml | kubectl_ctx apply -f -

  kubectl_ctx apply -f "$manifests/sharding-e2e-rbac.yaml"

  kubectl_ctx delete job sharding-e2e --ignore-not-found --wait=true 2>/dev/null || true
  kubectl_ctx apply -f "$manifests/sharding-e2e-job.yaml"

  log "Waiting for runner pod..."
  kubectl_ctx wait --for=condition=ready pod -l job-name=sharding-e2e --timeout=300s

  local pod log_pid=0
  pod=$(kubectl_ctx get pod -l job-name=sharding-e2e -o jsonpath='{.items[0].metadata.name}')
  kubectl_ctx logs -f "$pod" &
  log_pid=$!

  local job_ok=true
  if ! kubectl_ctx wait --for=condition=complete job/sharding-e2e --timeout=1800s; then
    job_ok=false
  fi

  kill "$log_pid" 2>/dev/null || true
  wait "$log_pid" 2>/dev/null || true

  mkdir -p "$REPORT_DIR"
  if kubectl_ctx cp "$NS/$pod:/tmp/sharding-e2e-report.md" "$REPORT_FILE" 2>/dev/null; then
    log "Report copied to $REPORT_FILE"
  else
    warn "Could not copy report from pod; check: kubectl -n $NS logs job/sharding-e2e"
  fi

  if [[ "$job_ok" != true ]]; then
    err "In-cluster Job failed"
    exit 1
  fi

  if grep -q '^\*\*Overall:\*\* PASS' "$REPORT_FILE" 2>/dev/null; then
    log "E2E PASSED"
    exit 0
  fi
  err "E2E FAILED (see report)"
  exit 1
}

main() {
  mkdir -p "$REPORT_DIR"
  log "Sharding E2E — context=$KUBE_CTX db=$DB"
  wait_for_full_cluster

  phase_g0
  phase_g1
  phase_g2
  phase_g3
  phase_g4
  phase_g5
  phase_g8
  phase_g6
  phase_g7
  phase_g9

  write_report

  if [[ "$FAILURES" -gt 0 ]]; then
    err "E2E FAILED ($FAILURES goals)"
    exit 1
  fi
  log "E2E PASSED"
  exit 0
}

# Default: spawn in-cluster Job. Use --local for port-forward mode from laptop.
if [[ "$IN_CLUSTER" != "1" && "$USE_LOCAL" != true ]]; then
  launch_in_cluster_job
fi

main "$@"
