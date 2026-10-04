#!/usr/bin/env bash
# Linear-scaling acceptance test.
#
# Every run starts from a fresh cluster (CR and PVCs deleted, CR re-applied at
# 3 replicas), scales to N and measures one sweep point. Each N in NODES_LIST
# runs RUNS times; the median is the result and min-max is the noise band.
#
# PASS when, at every N:
#   - median(N) / median(first N) >= PASS_RATIO * N / first N
#   - every measured point reads back (count == MEASURE_POINTS)
#   - no DB pod restarted
#
# DESTRUCTIVE: deletes the HyperbytedbCluster and all its data before every
# run. Refuses unless CONFIRM_RESET=<namespace>/<cluster name>.
set -euo pipefail
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
KUBE_CTX="${KUBE_CTX:-n3}"
NS="${NS:-hyperbytedb-e2e}"
CR_NAME="${CR_NAME:-hyperbytedb-e2e}"
RUNS="${RUNS:-3}"
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
OUT_DIR="${OUT_DIR:-/tmp/linearity-acceptance-$(date +%Y%m%d-%H%M%S)}"
# How long the read-back may wait for flushes and hints to land.
READBACK_TIMEOUT_SECS="${READBACK_TIMEOUT_SECS:-900}"

if [[ "${CONFIRM_RESET:-}" != "$NS/$CR_NAME" ]]; then
  echo "refusing: this deletes $NS/$CR_NAME and its data before every run;" \
    "set CONFIRM_RESET=$NS/$CR_NAME" >&2
  exit 2
fi
mkdir -p "$OUT_DIR"
k() { kubectl --context "$KUBE_CTX" -n "$NS" "$@"; }
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

leader() {
  k exec "${CR_NAME}-0" -c hyperbytedb -- sh -c \
    'curl -sS -m 5 http://127.0.0.1:8086/cluster/leader' 2>/dev/null \
    | python3 -c "import sys,json; print(json.load(sys.stdin).get('leader_id') or '')" 2>/dev/null || true
}

reset_cluster() {
  k delete job linearity-bench --ignore-not-found >/dev/null
  k delete hyperbytedbcluster "$CR_NAME" --ignore-not-found --wait=true --timeout=300s >/dev/null
  for _ in $(seq 1 60); do
    [[ -z "$(k get pods -l app.kubernetes.io/name=hyperbytedb -o name 2>/dev/null)" ]] && break
    sleep 5
  done
  local pvcs
  pvcs=$(k get pvc -o name | grep "/data-${CR_NAME}-" || true)
  [[ -n "$pvcs" ]] && k delete $pvcs --wait=true --timeout=300s >/dev/null
  k apply -f "$OUT_DIR/cr.json" >/dev/null
  for _ in $(seq 1 90); do
    local ready
    ready=$(k get pods -l app.kubernetes.io/name=hyperbytedb --no-headers 2>/dev/null | grep -c '1/1 *Running' || true)
    (( ready == 3 )) && [[ -n "$(leader)" ]] && return 0
    sleep 10
  done
  log "cluster did not come back"; return 1
}

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

echo "N run points_per_sec errors unrecovered read_back restarts" >"$OUT_DIR/runs.txt"
for N in $NODES_LIST; do
  for r in $(seq 1 "$RUNS"); do
    log "N=$N run $r/$RUNS: reset"
    reset_cluster
    log "N=$N run $r/$RUNS: sweep"
    NODES_LIST="$N" RESULTS="$OUT_DIR/sweep-$N-$r.md" KUBE_CTX="$KUBE_CTX" NS="$NS" CR_NAME="$CR_NAME" \
      "$SCRIPT_DIR/run-incluster-sweep.sh" >"$OUT_DIR/sweep-$N-$r.log" 2>&1 || true
    db=$(grep -oE '^\[sweep\] db=\S+' "$OUT_DIR/sweep-$N-$r.log" | tail -1 | cut -d= -f2)
    read -r pps errs < <(grep -E "^\| $N \|" "$OUT_DIR/sweep-$N-$r.md" | tail -1 \
      | awk -F'|' '{gsub(/ /,"",$6); gsub(/ /,"",$7); print ($6==""?"-":$6), ($7==""?"-":$7)}')
    unrec=$(grep -oE 'replay worker=[0-9]+ failed=[0-9]+ unrecovered=[0-9]+' "$OUT_DIR/sweep-$N-$r.log" \
      | awk -F'unrecovered=' '{s+=$2} END{print s+0}')
    got=$(readback "${db:-missing}")
    restarts=$(k get pods -l app.kubernetes.io/name=hyperbytedb \
      -o jsonpath='{range .items[*]}{.status.containerStatuses[0].restartCount}{"\n"}{end}' | awk '{s+=$1} END{print s+0}')
    echo "$N $r ${pps:--} ${errs:--} $unrec $got $restarts" >>"$OUT_DIR/runs.txt"
    log "N=$N run $r: pps=${pps:--} errors=${errs:--} unrecovered=$unrec read_back=$got/$MEASURE_POINTS restarts=$restarts"
  done
done

PASS_RATIO="$PASS_RATIO" MEASURE_POINTS="$MEASURE_POINTS" python3 - "$OUT_DIR/runs.txt" <<'PY' | tee "$OUT_DIR/summary.md"
import os,statistics,sys
rows=[l.split() for l in open(sys.argv[1]).read().splitlines()[1:] if l.strip()]
want=int(os.environ["MEASURE_POINTS"]); bar=float(os.environ["PASS_RATIO"])
by={}
for n,r,pps,errs,unrec,got,rst in rows: by.setdefault(int(n),[]).append((pps,errs,int(unrec),int(got),int(rst)))
ok=True; base=None
print("| N | median pts/s | min-max | vs first N | needed | lost points | restarts | pass |")
print("|---|---|---|---|---|---|---|---|")
for n in sorted(by):
    runs=by[n]; vals=[int(p) for p,*_ in runs if p not in ("-","")]
    lost=sum(max(0,want-g) for _,_,_,g,_ in runs); rst=sum(x[4] for x in runs)
    if not vals:
        print(f"| {n} | - | - | - | - | {lost} | {rst} | NO (no numbers) |"); ok=False; continue
    med=statistics.median(vals)
    if base is None: base=(n,med)
    ratio=med/base[1]; need=bar*n/base[0] if n!=base[0] else 1.0
    good=len(vals)==len(runs) and ratio>=need-1e-9 and lost==0 and rst==0
    ok&=good
    print(f"| {n} | {med:,.0f} | {min(vals):,}-{max(vals):,} | {ratio:.2f}x | {need:.2f}x | {lost} | {rst} | {'yes' if good else 'NO'} |")
print()
print("PASS" if ok else "FAIL")
PY
log "results in $OUT_DIR"
