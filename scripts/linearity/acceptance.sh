#!/usr/bin/env bash
# Linear-scaling acceptance test.
#
# Every run starts from a fresh cluster (CR and PVCs deleted, CR re-applied at
# 3 replicas), scales to N and measures one sweep point. Each N in NODES_LIST
# runs RUNS times; the median is the result and min-max is the noise band.
#
# Equal host load (MATCH_LOAD=1, the default): every N below the largest is
# measured while a twin cluster of (largest - N) nodes runs its own write load
# on the same host, so each size keeps the host equally busy. On one host a
# busier machine costs every point more CPU (n3: >=13% at N=3 with 9 more
# cores busy), which would otherwise be charged to the larger N.
#
# PASS when, at every N:
#   - median(N) / median(first N) >= PASS_RATIO * N / first N
#   - every measured point reads back (count == MEASURE_POINTS)
#   - no DB pod restarted
#
# DESTRUCTIVE: deletes the HyperbytedbCluster (and the twin) and all their
# data before every run. Refuses unless CONFIRM_RESET=<namespace>/<cluster name>.
set -euo pipefail
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
KUBE_CTX="${KUBE_CTX:-n3}"
NS="${NS:-hyperbytedb-e2e}"
CR_NAME="${CR_NAME:-hyperbytedb-e2e}"
RUNS="${RUNS:-3}"
MATCH_LOAD="${MATCH_LOAD:-1}"
TWIN_CR="${TWIN_CR:-${CR_NAME}-twin}"
NODES_LIST="${NODES_LIST:-3 6}"
# 0.9: N=6 must reach 1.8x N=3.
PASS_RATIO="${PASS_RATIO:-0.9}"
export SERIES_PER_NODE="${SERIES_PER_NODE:-400000}"
export MEASURE_POINTS="${MEASURE_POINTS:-90000000}"
export CONCURRENCY="${CONCURRENCY:-64}"
export CLIENTS_PER_NODE="${CLIENTS_PER_NODE:-2}"
export BENCH_CPU="${BENCH_CPU:-1}"
export BATCH="${BATCH:-5000}"
export SPLIT_TIMEOUT_SECS="${SPLIT_TIMEOUT_SECS:-2400}"
BENCH_IMAGE="${BENCH_IMAGE:-docker.io/library/hyperbytedb:e2e}"
OUT_DIR="${OUT_DIR:-/tmp/linearity-acceptance-$(date +%Y%m%d-%H%M%S)}"
# Counters summed over DB pods after each run, for the per-run diagnosis.
COUNTERS='hinted_handoff_enqueued_total|replication_queue_full_total|write_admission_rejected_total|shard_forward_capacity_retries_total|shard_forward_failed_total|write_requests_total'
# How long the read-back may wait for flushes and hints to land.
READBACK_TIMEOUT_SECS="${READBACK_TIMEOUT_SECS:-900}"

if [[ "${CONFIRM_RESET:-}" != "$NS/$CR_NAME" ]]; then
  echo "refusing: this deletes $NS/$CR_NAME and its data before every run;" \
    "set CONFIRM_RESET=$NS/$CR_NAME" >&2
  exit 2
fi
mkdir -p "$OUT_DIR"
k() { kubectl --context "$KUBE_CTX" -n "$NS" "$@"; }
# DB pods of one cluster; both clusters share app.kubernetes.io/name.
sel() { echo "app.kubernetes.io/name=hyperbytedb,app.kubernetes.io/instance=$1"; }
NMAX=$(tr ' ' '\n' <<<"$NODES_LIST" | sort -n | tail -1)
log() { echo "[acceptance] $(date -u +%H:%M:%S) $*"; }

# The spec every run starts from, captured once.
k get hyperbytedbcluster "$CR_NAME" -o json | python3 -c '
import json,sys
cr=json.load(sys.stdin)
m=cr["metadata"]
cr["metadata"]={k:m[k] for k in ("name","namespace","annotations","labels") if k in m}
cr.pop("status",None)
cr["spec"]["replicas"]=3
json.dump(cr,sys.stdout)' >"$OUT_DIR/cr.json"

# $1 = cluster name, $2 = replicas: the captured spec under another name/size.
cr_for() {
  python3 -c '
import json,sys
cr=json.load(open(sys.argv[1])); cr["metadata"]["name"]=sys.argv[2]; cr["spec"]["replicas"]=int(sys.argv[3])
json.dump(cr,sys.stdout)' "$OUT_DIR/cr.json" "$1" "$2"
}

leader() {
  k exec "${1:-$CR_NAME}-0" -c hyperbytedb -- sh -c \
    'curl -sS -m 5 http://127.0.0.1:8086/cluster/leader' 2>/dev/null \
    | python3 -c "import sys,json; print(json.load(sys.stdin).get('leader_id') or '')" 2>/dev/null || true
}

# Delete cluster $1 and its data.
delete_cluster() {
  local name=$1 pvcs
  k delete hyperbytedbcluster "$name" --ignore-not-found --wait=true --timeout=300s >/dev/null
  for _ in $(seq 1 60); do
    [[ -z "$(k get pods -l "$(sel "$name")" -o name 2>/dev/null)" ]] && break
    sleep 5
  done
  pvcs=$(k get pvc -o name | grep "/data-${name}-[0-9]" || true)
  [[ -n "$pvcs" ]] && k delete $pvcs --wait=true --timeout=300s >/dev/null
  return 0
}

# Recreate cluster $1 empty at $2 replicas and wait for a leader.
reset_cluster() {
  local name=$1 replicas=$2 ready
  delete_cluster "$name"
  cr_for "$name" "$replicas" | k apply -f - >/dev/null
  for _ in $(seq 1 90); do
    ready=$(k get pods -l "$(sel "$name")" --no-headers 2>/dev/null | grep -c '1/1 *Running' || true)
    (( ready == replicas )) && [[ -n "$(leader "$name")" ]] && return 0
    sleep 10
  done
  log "$name did not come back"; return 1
}

# Constant write load on the twin ($1 nodes) until stop_twin_load.
start_twin_load() {
  local n=$1 eps="" workers
  workers=$(( CLIENTS_PER_NODE * n ))
  for ((i = 0; i < n; i++)); do
    eps="${eps}${eps:+,}http://${TWIN_CR}-${i}.${TWIN_CR}-headless.${NS}.svc.cluster.local:8086"
  done
  k delete job linearity-twin-load --ignore-not-found >/dev/null
  cat <<JOB | k apply -f - >/dev/null
apiVersion: batch/v1
kind: Job
metadata:
  name: linearity-twin-load
  namespace: ${NS}
spec:
  backoffLimit: 0
  completionMode: Indexed
  completions: ${workers}
  parallelism: ${workers}
  template:
    spec:
      restartPolicy: Never
      containers:
        - name: bench
          image: ${BENCH_IMAGE}
          imagePullPolicy: Never
          command: ["/usr/local/bin/hyperbytedb-cli", "bench-write"]
          resources:
            limits:
              cpu: "${BENCH_CPU}"
          args:
            - "--endpoints=${eps}"
            - "--series=$(( SERIES_PER_NODE * n ))"
            - "--measure-points=4000000000"
            - "--concurrency=${CONCURRENCY}"
            - "--batch=${BATCH}"
            - "--phase=all"
            - "--db=twin_load"
            - "--workers=${workers}"
JOB
}
stop_twin_load() { k delete job linearity-twin-load --ignore-not-found --wait=false >/dev/null; }

# Count of measured points in $1, polled until it reaches MEASURE_POINTS or
# stops changing for three polls. Under-reading mid-flush is not loss.
readback() {
  local db=$1 last="" same=0 n="" deadline=$(( SECONDS + READBACK_TIMEOUT_SECS ))
  while (( SECONDS < deadline )); do
    n=$(k exec "${CR_NAME}-0" -c hyperbytedb -- sh -c \
      "curl -sS -m 120 -G http://127.0.0.1:8086/query --data-urlencode 'db=$db' \
        --data-urlencode 'q=SELECT count(value) FROM cpu WHERE time >= 1800000000s'" 2>/dev/null \
      | python3 -c "
import sys,json
try: print(int(json.load(sys.stdin)['results'][0]['series'][0]['values'][0][1]))
except Exception: print('')" 2>/dev/null || true)
    [[ "$n" == "$MEASURE_POINTS" ]] && break
    if [[ -n "$n" && "$n" == "$last" ]]; then
      same=$(( same + 1 )); (( same >= 3 )) && break
    else same=0; fi
    last=$n
    sleep 30
  done
  echo "${n:-0}"
}

# Every 10 s: DB-pod millicores and pod count, for the cluster and its twin.
sample_cpu() {
  while true; do
    echo "$(date +%s) $(k top pods -l app.kubernetes.io/name=hyperbytedb --no-headers 2>/dev/null \
      | awk -v twin="$TWIN_CR-" '{gsub(/m/,"",$2); if (index($1, twin) == 1) {t+=$2; tn++} else {s+=$2; n++}}
             END{print s+0, n+0, t+0, tn+0}')"
    sleep 10
  done
}

echo "N run points_per_sec errors unrecovered read_back restarts db_cores us_per_point" >"$OUT_DIR/runs.txt"
for N in $NODES_LIST; do
  twin=0
  [[ "$MATCH_LOAD" == 1 ]] && twin=$(( NMAX - N ))
  for r in $(seq 1 "$RUNS"); do
    log "N=$N run $r/$RUNS: reset (twin=$twin)"
    k delete job linearity-bench --ignore-not-found >/dev/null
    stop_twin_load
    reset_cluster "$CR_NAME" 3 &
    primary=$!
    if (( twin > 0 )); then
      reset_cluster "$TWIN_CR" "$twin" &
      twin_reset=$!
      wait "$twin_reset"
      start_twin_load "$twin"
    else
      delete_cluster "$TWIN_CR"
    fi
    wait "$primary"
    log "N=$N run $r/$RUNS: sweep"
    sample_cpu >"$OUT_DIR/cpu-$N-$r.txt" 2>/dev/null &
    sampler=$!
    NODES_LIST="$N" RESULTS="$OUT_DIR/sweep-$N-$r.md" KUBE_CTX="$KUBE_CTX" NS="$NS" CR_NAME="$CR_NAME" \
      CPU_STAT_FILE="$OUT_DIR/cpustat-$N-$r.txt" \
      "$SCRIPT_DIR/run-incluster-sweep.sh" >"$OUT_DIR/sweep-$N-$r.log" 2>&1 || true
    kill "$sampler" 2>/dev/null || true
    stop_twin_load
    for ((i = 0; i < N; i++)); do
      k exec "${CR_NAME}-$i" -c hyperbytedb -- sh -c 'curl -sS -m 10 http://127.0.0.1:8086/metrics' 2>/dev/null || true
    done | grep -E "^hyperbytedb_($COUNTERS)" \
      | awk '{sub(/\{peer_id="[0-9]+"\}/,"",$1); s[$1]+=$2} END{for(m in s) printf "%14.0f %s\n", s[m], m}' \
      | sort -k2 >"$OUT_DIR/counters-$N-$r.txt" || true
    # Exact DB CPU over the measure phase, from cgroup usage before and after:
    # mean cores, and CPU microseconds per measured point.
    read -r cores upp < <(awk -v pts="$MEASURE_POINTS" '
      $1=="start"{t0=$2; u0=$3} $1=="end"{t1=$2; u1=$3}
      END{ if (t1>t0 && u1>u0) printf "%.1f %.2f\n", (u1-u0)/((t1-t0)*1000), (u1-u0)/pts; else print "- -" }' \
      "$OUT_DIR/cpustat-$N-$r.txt" 2>/dev/null || echo "- -")
    db=$(grep -oE '^\[sweep\] db=\S+' "$OUT_DIR/sweep-$N-$r.log" | tail -1 | cut -d= -f2)
    read -r pps errs < <(grep -E "^\| $N \|" "$OUT_DIR/sweep-$N-$r.md" | tail -1 \
      | awk -F'|' '{gsub(/ /,"",$6); gsub(/ /,"",$7); print ($6==""?"-":$6), ($7==""?"-":$7)}')
    unrec=$(grep -oE 'replay worker=[0-9]+ failed=[0-9]+ unrecovered=[0-9]+' "$OUT_DIR/sweep-$N-$r.log" \
      | awk -F'unrecovered=' '{s+=$2} END{print s+0}')
    got=$(readback "${db:-missing}")
    restarts=$(k get pods -l "$(sel "$CR_NAME")" \
      -o jsonpath='{range .items[*]}{.status.containerStatuses[0].restartCount}{"\n"}{end}' | awk '{s+=$1} END{print s+0}')
    echo "$N $r ${pps:--} ${errs:--} $unrec $got $restarts $cores $upp" >>"$OUT_DIR/runs.txt"
    # Twin cores over the same busiest quartile of the cluster's own samples.
    twin_cores=$(awk -v n="$N" '$3==n {print $2, $4}' "$OUT_DIR/cpu-$N-$r.txt" | sort -n \
      | awk '{v[NR]=$2} END{if(!NR){print "-"; exit} s=0; c=0; for(i=int(NR*0.75)+1;i<=NR;i++){s+=v[i]; c++} printf "%.1f", s/c/1000}')
    log "N=$N run $r: pps=${pps:--} errors=${errs:--} unrecovered=$unrec read_back=$got/$MEASURE_POINTS restarts=$restarts db_cores=$cores us_per_point=$upp twin_cores=$twin_cores"
  done
done

PASS_RATIO="$PASS_RATIO" MEASURE_POINTS="$MEASURE_POINTS" python3 - "$OUT_DIR/runs.txt" <<'PY' | tee "$OUT_DIR/summary.md"
import os,statistics,sys
rows=[l.split() for l in open(sys.argv[1]).read().splitlines()[1:] if l.strip()]
want=int(os.environ["MEASURE_POINTS"]); bar=float(os.environ["PASS_RATIO"])
by={}
for n,r,pps,errs,unrec,got,rst,cores,upp in rows: by.setdefault(int(n),[]).append((pps,errs,int(unrec),int(got),int(rst),cores,upp))
ok=True; base=None
print("| N | median pts/s | min-max | vs first N | needed | lost points | restarts | DB cores | CPU us/point | pass |")
print("|---|---|---|---|---|---|---|---|---|---|")
for n in sorted(by):
    runs=by[n]; vals=[int(p) for p,*_ in runs if p not in ("-","")]
    lost=sum(max(0,want-x[3]) for x in runs); rst=sum(x[4] for x in runs)
    med_of=lambda i: (f"{statistics.median([float(x[i]) for x in runs if x[i] not in ('-','')]):.{1 if i==5 else 2}f}"
                      if any(x[i] not in ('-','') for x in runs) else "-")
    cores, upp = med_of(5), med_of(6)
    if not vals:
        print(f"| {n} | - | - | - | - | {lost} | {rst} | {cores} | {upp} | NO (no numbers) |"); ok=False; continue
    med=statistics.median(vals)
    if base is None: base=(n,med)
    ratio=med/base[1]; need=bar*n/base[0] if n!=base[0] else 1.0
    good=len(vals)==len(runs) and ratio>=need-1e-9 and lost==0 and rst==0
    ok&=good
    print(f"| {n} | {med:,.0f} | {min(vals):,}-{max(vals):,} | {ratio:.2f}x | {need:.2f}x | {lost} | {rst} | {cores} | {upp} | {'yes' if good else 'NO'} |")
print()
print("PASS" if ok else "FAIL")
PY
stop_twin_load
delete_cluster "$TWIN_CR"
log "results in $OUT_DIR"
