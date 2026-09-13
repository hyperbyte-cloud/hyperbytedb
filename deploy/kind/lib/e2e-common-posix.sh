# POSIX helpers for kind sharding e2e scripts (scaledown).
# Source after setting SCRIPT_DIR, KUBE_CTX, NS, DB, REPORT_DIR, etc.

: "${E2E_EXPECT_REPLICAS:=6}"
: "${E2E_REPORT_TITLE:=Sharding E2E Test Report}"
: "${E2E_CLUSTER_LABEL:=6 replicas, RF=2}"
: "${FAILURES:=0}"

GOALS_FILE="${REPORT_DIR}/.e2e-goals.$$"

log() { printf '%s\n' "[e2e] $*"; }
warn() { printf '%s\n' "[e2e] WARN: $*" >&2; }
err() { printf '%s\n' "[e2e] ERROR: $*" >&2; }

record() {
  goal_id=$1
  result=$2
  evidence=$3
  printf '%s\n' "${goal_id}|${result}|${evidence}" >> "$GOALS_FILE"
  if [ "$result" = "FAIL" ]; then
    FAILURES=$((FAILURES + 1))
    err "${goal_id} FAIL — ${evidence}"
  else
    log "${goal_id} ${result} — ${evidence}"
  fi
}

pass() { record "$1" "PASS" "$2"; }
fail() { record "$1" "FAIL" "$2"; }

init_goals() {
  mkdir -p "$REPORT_DIR"
  : > "$GOALS_FILE"
}

wait_flush_boundary() {
  sleep "${FLUSH_WAIT_SECS:-6}"
}

# Poll until a count query reaches `want`, echoing the last value seen.
#
# A flush boundary is not a convergence boundary. Seeding 5000 series splits the
# measurement into ~128 regions, and while those are being created, staged and
# placed a scatter-gather query silently under-reports -- no error, just a
# smaller number. Reading once after a 6s sleep therefore fails a gate whose
# data is entirely intact; observed directly as 4918-4997 of 5000, settling to
# exactly 5000 on every node once convergence finished.
#
# This keeps the assertion -- the caller still fails if `want` never arrives --
# and only stops the gate from firing on a read taken too early.
wait_for_query_count() {
  pod_idx=$1
  q=$2
  want=$3
  timeout_secs=${4:-300}
  deadline=$(( $(date +%s) + timeout_secs ))
  last=""
  while [ "$(date +%s)" -lt "$deadline" ]; do
    last=$(count_from_query_checked "$pod_idx" "$q")
    case $last in
      ERR:*) : ;;
      *)
        if [ "$last" -ge "$want" ] 2>/dev/null; then
          echo "$last"
          return 0
        fi
        ;;
    esac
    sleep 5
  done
  echo "$last"
}

kubectl_ctx() {
  if [ "${IN_CLUSTER:-0}" = "1" ]; then
    kubectl -n "$NS" "$@"
  else
    kubectl --context "$KUBE_CTX" -n "$NS" "$@"
  fi
}

api_port() {
  pod_idx=$1
  echo $((18080 + pod_idx))
}

api_url() {
  pod_idx=$1
  if [ "${IN_CLUSTER:-0}" = "1" ]; then
    printf 'http://hyperbytedb-%s.%s.%s.%s:%s' \
      "$pod_idx" "${HEADLESS_SVC:-hyperbytedb-headless}" "$NS" \
      "${CLUSTER_DOMAIN:-svc.cluster.local}" "${HDB_PORT:-8086}"
  else
    printf 'http://127.0.0.1:%s' "$(api_port "$pod_idx")"
  fi
}

PF_PIDS=""

# True if something is listening on `port` locally.
port_is_bound() {
  if command -v ss >/dev/null 2>&1; then
    ss -ltn "sport = :$1" 2>/dev/null | grep -q ":$1\b"
  else
    # No ss: a successful connect proves a listener; anything else assumes free.
    curl -sS -o /dev/null -m 1 "http://127.0.0.1:$1/" >/dev/null 2>&1
  fi
}

# Free a forwarded port before rebinding it.
#
# This used to call `fuser -k`, which is NOT installed here -- and the
# `|| true` swallowed that silently. A stale forward therefore kept its port,
# the replacement `kubectl port-forward` died with "address already in use",
# and that pod dropped out of every later read while the suite carried on. It
# surfaced as L10 reporting ERR:unreachable=5 long after the real failure.
#
# The [k] bracket keeps the pattern from matching this shell's own command
# line, which would make pkill kill its own parent.
free_forward_port() {
  port=$1
  pod_idx=$2
  pkill -f "[k]ubectl.*port-forward pod/hyperbytedb-${pod_idx} ${port}:" 2>/dev/null || true
  n=0
  while [ "$n" -lt 10 ]; do
    port_is_bound "$port" || return 0
    sleep 1
    n=$((n + 1))
  done
  warn "port ${port} still bound after kill; forward for pod ${pod_idx} may fail to bind"
}

# Start (or restart) one port-forward per pod. Safe to call repeatedly -- each
# port is freed first -- so call it again after any pod is killed or recreated,
# which leaves its forward pointing at a pod that no longer exists.
#
# MUST be called at top level, never inside `$(...)`. It backgrounds
# long-lived processes that inherit the command substitution's stdout pipe,
# and the substitution then blocks on an EOF that never comes.
setup_port_forwards() {
  [ "${IN_CLUSTER:-0}" = "1" ] && return 0
  max=$((E2E_EXPECT_REPLICAS - 1))
  log "Starting port-forwards hyperbytedb-0..${max}"
  i=0
  while [ "$i" -le "$max" ]; do
    port=$(api_port "$i")
    free_forward_port "$port" "$i"
    kubectl_ctx port-forward "pod/hyperbytedb-$i" "${port}:8086" \
      >/tmp/pf-hdb-"$i".log 2>&1 &
    PF_PIDS="${PF_PIDS} $!"
    i=$((i + 1))
  done
  deadline=$(( $(date +%s) + 45 ))
  while [ "$(date +%s)" -lt "$deadline" ]; do
    all_ok=1
    i=0
    while [ "$i" -le "$max" ]; do
      code=$(curl -sS -o /dev/null -w '%{http_code}' -m 3 "$(api_url "$i")/ping" 2>/dev/null || echo 000)
      if [ "$code" != "204" ]; then
        all_ok=0
        break
      fi
      i=$((i + 1))
    done
    if [ "$all_ok" -eq 1 ]; then
      log "All port-forwards responding"
      return 0
    fi
    sleep 2
  done
  warn "Not all port-forwards ready after 45s (continuing)"
}

teardown_port_forwards() {
  [ "${IN_CLUSTER:-0}" = "1" ] && return 0
  [ "${KEEP_PF:-false}" = "true" ] && return 0
  for pid in $PF_PIDS; do
    kill "$pid" 2>/dev/null || true
  done
  PF_PIDS=""
  i=0
  while [ "$i" -lt "$E2E_EXPECT_REPLICAS" ]; do
    fuser -k "$(api_port "$i")/tcp" 2>/dev/null || true
    i=$((i + 1))
  done
}

curl_api() {
  pod_idx=$1
  shift
  curl -sS -m "${CURL_TIMEOUT:-15}" "$(api_url "$pod_idx")$*" || true
}

curl_write() {
  pod_idx=$1
  body=$2
  curl -sS -m 15 -o /tmp/e2e-write.out -w '%{http_code}' \
    -X POST "$(api_url "$pod_idx")/write?db=${DB}&rp=${RP:-autogen}&precision=s" \
    --data-binary "$body" 2>/dev/null || echo 000
}

write_ts() {
  echo $(($(date +%s) + ${1:-0}))
}

query_post() {
  pod_idx=$1
  q=$2
  curl -sS -m "${CURL_TIMEOUT:-15}" -G "$(api_url "$pod_idx")/query" \
    --data-urlencode "db=${DB}" \
    --data-urlencode "q=${q}" 2>/dev/null || echo '{"results":[]}'
}

count_from_query_checked() {
  pod_idx=$1
  q=$2
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

# Extract a single numeric value from the first series/first row of a query.
# Prints "" when the query errored or returned no rows, so callers can tell
# "no answer" from a legitimate 0.
query_scalar() {
  pod_idx=$1
  q=$2
  query_post "$pod_idx" "$q" | python3 -c "
import json,sys
raw=sys.stdin.read()
try:
  d=json.loads(raw)
except Exception:
  print('') ; raise SystemExit
res=(d.get('results') or [{}])[0]
if res.get('error'):
  print('') ; raise SystemExit
ser=(res.get('series') or [])
if not ser or not ser[0].get('values'):
  print('') ; raise SystemExit
cols=ser[0].get('columns') or []
row=ser[0]['values'][0]
# Pick the value column by NAME, not position. An aggregate with no GROUP BY
# returns a single-element row like [1962000] with columns ['sum_value'];
# assuming a leading 'time' column and taking row[1] silently yields nothing.
idx=next((i for i,c in enumerate(cols) if c != 'time'), None)
if idx is None or idx >= len(row):
  idx = len(row) - 1
val=row[idx] if row else None
if val is None:
  print('')
else:
  # Normalise numeric type. The engine returns SUM as an int but MIN/MAX as
  # floats, so a shell string compare against an expected 0 fails on 0.0.
  try:
    f=float(val)
    print(int(f) if f == int(f) else f)
  except (TypeError, ValueError):
    print(val)
" 2>/dev/null || echo ""
}

# Number of distinct groups a GROUP BY returned.
query_group_count() {
  pod_idx=$1
  q=$2
  query_post "$pod_idx" "$q" | python3 -c "
import json,sys
try:
  d=json.load(sys.stdin)
except Exception:
  print(-1) ; raise SystemExit
res=(d.get('results') or [{}])[0]
if res.get('error'):
  print(-1) ; raise SystemExit
print(len(res.get('series') or []))
" 2>/dev/null || echo -1
}

# Scrape one pod's /metrics. Prints the body; returns non-zero if the scrape
# itself failed.
#
# The distinction is the whole point. The metrics crate emits a counter only
# after its first increment, so an ABSENT counter is legitimately zero -- but
# an unreachable pod yields nothing either. Collapsing the two is how a gate
# asserting "counter == 0" passes because nobody could be asked. Not
# hypothetical: a killed pod's port-forward stays dead for the rest of the run,
# and the lifecycle suite reported placement_stalled_total=0 having reached
# only two of six pods.
metrics_scrape() {
  pod_idx=$1
  body=$(curl -sS -m "${CURL_TIMEOUT:-15}" "$(api_url "$pod_idx")/metrics" 2>/dev/null) || return 1
  [ -n "$body" ] || return 1
  printf '%s\n' "$body"
}

metric_value() {
  pod_idx=$1
  name=$2
  metrics_scrape "$pod_idx" | grep "^${name}" | awk '{print $2}' | head -1
}

# A pod's counter as an integer, or the literal string ERR when the pod could
# not be scraped. Callers must treat ERR as unknown, never as zero.
metric_counter() {
  pod_idx=$1
  name=$2
  body=$(metrics_scrape "$pod_idx") || { echo ERR; return 0; }
  v=$(printf '%s\n' "$body" | grep "^${name}" | awk '{print $2}' | head -1)
  if [ -n "$v" ]; then
    # The exposition format renders counters as floats; gates compare integers.
    printf '%s\n' "$v" | cut -d. -f1
  else
    echo 0
  fi
}

# Highest value of `name` across pods, or ERR:unreachable=<list> naming the
# pods that could not be scraped.
#
# A pure reader on purpose. It must NOT rebuild port-forwards itself: callers
# read it as `v=$(metric_max_across_pods ...)`, and a background process
# started inside a command substitution inherits the substitution's stdout
# pipe, so the shell waits for EOF that a long-lived `kubectl port-forward`
# never delivers -- the read hangs forever. Refresh forwards at top level
# BEFORE calling this (see setup_port_forwards).
metric_max_across_pods() {
  name=$1
  max_pods=${2:-$E2E_EXPECT_REPLICAS}
  max=0
  unreachable=""
  i=0
  while [ "$i" -lt "$max_pods" ]; do
    v=$(metric_counter "$i" "$name")
    if [ "$v" = "ERR" ]; then
      unreachable="${unreachable}${i},"
    elif [ "$v" -gt "$max" ] 2>/dev/null; then
      max=$v
    fi
    i=$((i + 1))
  done
  if [ -n "$unreachable" ]; then
    echo "ERR:unreachable=${unreachable%,}"
  else
    echo "$max"
  fi
}

wait_for_pod_ping() {
  pod_idx=$1
  timeout_secs=${2:-120}
  deadline=$(( $(date +%s) + timeout_secs ))
  while [ "$(date +%s)" -lt "$deadline" ]; do
    code=$(curl -sS -o /dev/null -w '%{http_code}' -m 3 "$(api_url "$pod_idx")/ping" 2>/dev/null || echo 000)
    if [ "$code" = "204" ]; then
      return 0
    fi
    sleep 3
  done
  return 1
}

node_health_state() {
  pod_idx=$1
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
  kubectl_ctx get hyperbytedbcluster hyperbytedb >/dev/null 2>&1
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

cluster_not_deployed_hint() {
  printf '%s\n' \
    "HyperbytedbCluster 'hyperbytedb' is not running in namespace '${NS}'." \
    "Deploy first: ./deploy/kind/setup.sh hdb-reset --sharded"
}

running_pod_count() {
  kubectl_ctx get pods -l app.kubernetes.io/name=hyperbytedb --field-selector=status.phase=Running \
    -o json 2>/dev/null | python3 -c "
import json,sys
d=json.load(sys.stdin)
print(sum(1 for p in d.get('items',[]) if all(c.get('ready') for c in p['status'].get('containerStatuses',[]))))
" 2>/dev/null || echo 0
}

# Running pods regardless of Ready. A decommissioning node serves 503 on /health
# and becomes NotReady while still evacuating — do not treat that as scale-down.
existing_pod_count() {
  kubectl_ctx get pods -l app.kubernetes.io/name=hyperbytedb --field-selector=status.phase=Running \
    -o json 2>/dev/null | python3 -c "
import json,sys
d=json.load(sys.stdin)
print(len(d.get('items',[])))
" 2>/dev/null || echo 0
}

pod_exists() {
  ordinal=$1
  if kubectl_ctx get pod "hyperbytedb-${ordinal}" >/dev/null 2>&1; then
    echo 1
  else
    echo 0
  fi
}

# Fail fast when CR/STS are not at the expected size (avoids waiting for pods
# that will never exist, e.g. cluster already scaled to 5 from a prior run).
cluster_replica_counts_match() {
  expect=$1
  if ! hyperbytedb_cr_exists; then
    cluster_not_deployed_hint >&2
    return 1
  fi
  cr=$(cr_spec_replicas)
  sts=$(sts_replicas)
  if [ "$cr" = "$expect" ] && [ "$sts" = "$expect" ]; then
    return 0
  fi
  warn "Cluster at CR=${cr} STS=${sts}, expected ${expect} (run: ./deploy/kind/setup.sh hdb-reset --sharded)"
  return 1
}

wait_for_full_cluster() {
  expect=${1:-$E2E_EXPECT_REPLICAS}
  if ! cluster_replica_counts_match "$expect"; then
    return 1
  fi
  log "Waiting for ${expect}/${expect} ready pods and ${expect} active raft nodes..."
  kubectl_ctx wait --for=condition=ready pod -l app.kubernetes.io/name=hyperbytedb --timeout=600s || true
  deadline=$(( $(date +%s) + 300 ))
  while [ "$(date +%s)" -lt "$deadline" ]; do
    if [ "${IN_CLUSTER:-0}" != "1" ]; then
      teardown_port_forwards
      setup_port_forwards
    fi
    ready=$(running_pod_count)
    if [ "$ready" != "$expect" ]; then
      cr=$(cr_spec_replicas)
      sts=$(sts_replicas)
      if [ "$cr" != "$expect" ] || [ "$sts" != "$expect" ]; then
        warn "Replica counts changed to CR=${cr} STS=${sts} (expected ${expect})"
        return 1
      fi
    fi
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
    leader=$(curl_api 0 "/cluster/leader" 2>/dev/null | python3 -c \
      "import json,sys; print(json.load(sys.stdin).get('leader_id') or '')" 2>/dev/null || true)
    if [ "$ready" = "$expect" ] && [ "$active" = "$expect" ] && \
       [ -n "$leader" ] && [ "$leader" != "None" ]; then
      log "Cluster stable: ${ready} pods, ${active} active nodes, leader=${leader}"
      return 0
    fi
    log "  ... ready=${ready}/${expect} active=${active}/${expect} leader=${leader:-none}"
    sleep 10
  done
  warn "Cluster not fully stable after 300s"
  return 1
}

wait_for_regions_rf_ok() {
  min_rf=${1:-2}
  timeout_secs=${2:-120}
  deadline=$(( $(date +%s) + timeout_secs ))
  while [ "$(date +%s)" -lt "$deadline" ]; do
    rf=$(shard_map_all_regions_rf_ok "$min_rf")
    if [ "$rf" = "OK" ]; then
      echo OK
      return 0
    fi
    sleep 5
  done
  # Always print the final state; callers decide pass/fail. Must not return
  # non-zero here — set -e would abort the script before recording D5.
  shard_map_all_regions_rf_ok "$min_rf"
  return 0
}

shard_map_all_regions_rf_ok() {
  min_rf=${1:-2}
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

E2E_REPORT_BEGIN_MARKER="-----8<----- E2E REPORT BEGIN -----8<-----"
E2E_REPORT_END_MARKER="-----8<----- E2E REPORT END -----8<-----"

write_report() {
  report_file=${REPORT_FILE:?REPORT_FILE must be set}
  mkdir -p "$(dirname "$report_file")"
  {
    printf '# %s\n\n' "$E2E_REPORT_TITLE"
    printf '**Date:** %s\n' "$(date -u +%Y-%m-%dT%H:%M:%SZ)"
    run_mode="port-forward"
    [ "${IN_CLUSTER:-0}" = "1" ] && run_mode="in-cluster"
    printf '**Cluster:** %s (%s, mode=%s)\n' "$KUBE_CTX" "$E2E_CLUSTER_LABEL" "$run_mode"
    printf '**Database:** %s\n\n' "$DB"
    printf '| Goal | Result | Evidence |\n'
    printf '|------|--------|----------|\n'
    while IFS='|' read -r goal_id result evidence; do
      printf '| %s | %s | %s |\n' "$goal_id" "$result" "$evidence"
    done < "$GOALS_FILE"
    printf '\n'
    if [ "$FAILURES" -eq 0 ]; then
      printf '**Overall:** PASS\n'
    else
      printf '**Overall:** FAIL (%s failures)\n' "$FAILURES"
    fi
  } >"$report_file"
  log "Report: $report_file"
  # Also emit it to stdout between markers. `kubectl cp` execs tar inside the
  # container, so it cannot retrieve anything from a pod that has already
  # Completed -- which a finished Job pod always has. The logs survive.
  printf '%s\n' "$E2E_REPORT_BEGIN_MARKER"
  cat "$report_file"
  printf '%s\n' "$E2E_REPORT_END_MARKER"
}
