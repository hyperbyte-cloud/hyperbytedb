#!/usr/bin/env bash
# Full e2e run on one cluster: write and read latency at each size, and what
# scaling costs while a steady writer keeps writing.
#
#   1. Reset to a fresh FIRST_N (default 3).
#   2. Write bench (throughput + client latency), read bench, read-back.
#   3. For each N in NODES_UP (default "6 8"), with every earlier database in
#      place: start a steady writer, scale to N, time pods ready and the region
#      layout settling, count region moves, then read back everything written
#      so far, and repeat the write and read benches at N.
#   4. Scale down to FIRST_N the same way, and read everything back again.
#
# Writes OUT_DIR/results.json and OUT_DIR/report.md.
#
# DESTRUCTIVE: deletes the HyperbytedbCluster and its data first. Refuses
# unless CONFIRM_RESET=<namespace>/<cluster name>.
# No -e: one failed probe must not end a two-hour run; every step's outcome
# is recorded instead.
set -uo pipefail
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
KUBE_CTX="${KUBE_CTX:-n3}"
NS="${NS:-hyperbytedb-e2e}"
CR_NAME="${CR_NAME:-hyperbytedb-e2e}"
HEADLESS="${HEADLESS:-${CR_NAME}-headless}"
FIRST_N="${FIRST_N:-3}"
NODES_UP="${NODES_UP:-6 8}"
export SERIES_PER_NODE="${SERIES_PER_NODE:-400000}"
export MEASURE_POINTS="${MEASURE_POINTS:-90000000}"
export CONCURRENCY="${CONCURRENCY:-64}"
export CLIENTS_PER_NODE="${CLIENTS_PER_NODE:-2}"
export BENCH_CPU="${BENCH_CPU:-1}"
export BATCH="${BATCH:-5000}"
export SPLIT_TIMEOUT_SECS="${SPLIT_TIMEOUT_SECS:-2400}"
BENCH_IMAGE="${BENCH_IMAGE:-docker.io/library/hyperbytedb:e2e}"
READ_SECS="${READ_SECS:-30}"
READ_CONC="${READ_CONC:-4}"
READ_QUERIES="${READ_QUERIES:-last-host,raw-host,window-host,last-hosts,regex-hosts,count-all}"
# The steady writer: one client, a few requests in flight, for as long as a
# scaling step may take.
STEADY_SECS="${STEADY_SECS:-600}"
STEADY_SERIES="${STEADY_SERIES:-50000}"
STEADY_CONC="${STEADY_CONC:-4}"
# The layout counts as settled once it has not changed for this long.
SETTLE_SECS="${SETTLE_SECS:-120}"
OUT_DIR="${OUT_DIR:-/tmp/e2e-report-$(date +%Y%m%d-%H%M%S)}"

if [[ "${CONFIRM_RESET:-}" != "$NS/$CR_NAME" ]]; then
  echo "refusing: this deletes $NS/$CR_NAME and its data; set CONFIRM_RESET=$NS/$CR_NAME" >&2
  exit 2
fi
mkdir -p "$OUT_DIR"
k() { kubectl --context "$KUBE_CTX" -n "$NS" "$@"; }
log() { echo "[e2e] $(date -u +%H:%M:%S) $*" | tee -a "$OUT_DIR/progress.log"; }
sel="app.kubernetes.io/name=hyperbytedb,app.kubernetes.io/instance=${CR_NAME}"
# One JSON object per line: {"step":..., "key":..., "value":...}.
RESULTS="$OUT_DIR/results.jsonl"
: >"$RESULTS"
record() { python3 -c 'import json,sys; print(json.dumps({"step":sys.argv[1],"key":sys.argv[2],"value":sys.argv[3]}))' "$1" "$2" "$3" >>"$RESULTS"; }

k get hyperbytedbcluster "$CR_NAME" -o json | python3 -c '
import json,sys
cr=json.load(sys.stdin); m=cr["metadata"]
cr["metadata"]={k:m[k] for k in ("name","namespace","annotations","labels") if k in m}
cr.pop("status",None); cr["spec"]["replicas"]='"$FIRST_N"'
json.dump(cr,sys.stdout)' >"$OUT_DIR/cr.json"

leader() {
  k exec "${CR_NAME}-0" -c hyperbytedb -- sh -c 'curl -sS -m 5 http://127.0.0.1:8086/cluster/leader' 2>/dev/null \
    | python3 -c "import sys,json; print(json.load(sys.stdin).get('leader_id') or '')" 2>/dev/null || true
}
endpoints() {
  local n=$1 eps=""
  for ((i = 0; i < n; i++)); do eps="${eps}${eps:+,}http://${CR_NAME}-${i}.${HEADLESS}.${NS}.svc.cluster.local:8086"; done
  echo "$eps"
}
ready_pods() { k get pods -l "$sel" --no-headers 2>/dev/null | grep -c '1/1 *Running' || true; }
all_pods() { k get pods -l "$sel" --no-headers 2>/dev/null | grep -c . || true; }

# Seconds until exactly $1 pods are ready and a leader answers.
wait_ready() {
  local n=$1 start=$SECONDS
  for _ in $(seq 1 240); do
    if (( $(ready_pods) == n && $(all_pods) == n )) && [[ -n "$(leader)" ]]; then
      echo $(( SECONDS - start )); return 0
    fi
    sleep 5
  done
  echo "-"
}

# Every region of every space: id, primary, peers. Changes on any split,
# merge or move.
# "<regions> <hash> <ok>". ok=1 once no learner is staging and exactly $1
# nodes hold region copies: before moves start the layout is just as stable,
# and calling that settled timed a 3 -> 6 scale-up at 46 s. An empty map (a
# fresh cluster) is settled.
layout() {
  k exec "${CR_NAME}-0" -c hyperbytedb -- sh -c 'curl -sS -m 20 http://127.0.0.1:8086/internal/shard/map' 2>/dev/null \
    | python3 -c "
import sys,json
want=int(sys.argv[1])
try: d=json.load(sys.stdin)
except Exception: print('unavailable'); raise SystemExit
regs=[r for sp in d.get('spaces',[]) for r in sp['regions']]
rs=sorted((r['region_id'],r['primary'],tuple(sorted(r['peers'])),tuple(sorted(r.get('staging_peers',[])))) for r in regs)
nodes={p for r in regs for p in r['peers']}
ok=int(not regs or (not any(r.get('staging_peers') for r in regs) and len(nodes)==want))
print(len(rs), hash(tuple(rs)), ok)" "$1" 2>/dev/null || echo unavailable
}
# Seconds from $1 (SECONDS at the change) until the layout last changed, once
# it is complete for $2 nodes and has then held for SETTLE_SECS.
wait_settled() {
  local since=$1 nodes=$2 last="" changed=$SECONDS deadline=$(( SECONDS + 3600 ))
  while (( SECONDS < deadline )); do
    local now; now=$(layout "$nodes")
    if [[ "$now" != "$last" ]]; then last=$now; changed=$SECONDS; fi
    if (( SECONDS - changed >= SETTLE_SECS )) && [[ "$now" == *" 1" ]]; then
      echo $(( changed - since )); return 0
    fi
    sleep 10
  done
  echo "-"
}
placements() {
  local lid; lid=$(leader)
  [[ -n "$lid" ]] || { echo 0; return; }
  k exec "${CR_NAME}-$(( lid - 1 ))" -c hyperbytedb -- sh -c 'curl -sS -m 10 http://127.0.0.1:8086/metrics' 2>/dev/null \
    | awk '/^hyperbytedb_shard_placements_total /{print $2; f=1} END{if(!f) print 0}'
}

reset_cluster() {
  k delete job --ignore-not-found linearity-bench e2e-steady e2e-read >/dev/null
  k delete hyperbytedbcluster "$CR_NAME" --ignore-not-found --wait=true --timeout=300s >/dev/null
  for _ in $(seq 1 60); do [[ -z "$(k get pods -l "$sel" -o name 2>/dev/null)" ]] && break; sleep 5; done
  local pvcs; pvcs=$(k get pvc -o name | grep "/data-${CR_NAME}-[0-9]" || true)
  [[ -n "$pvcs" ]] && k delete $pvcs --wait=true --timeout=300s >/dev/null
  k apply -f "$OUT_DIR/cr.json" >/dev/null
}

# Measured points of $1 that read back, polled until it reaches $2 or stops
# changing for three polls.
readback() {
  local db=$1 want=$2 last="" same=0 n="" deadline=$(( SECONDS + 900 ))
  while (( SECONDS < deadline )); do
    n=$(k exec "${CR_NAME}-0" -c hyperbytedb -- sh -c \
      "curl -sS -m 120 -G http://127.0.0.1:8086/query --data-urlencode 'db=$db' \
        --data-urlencode 'q=SELECT count(value) FROM cpu WHERE time >= 1800000000s'" 2>/dev/null \
      | python3 -c "
import sys,json
try: print(int(json.load(sys.stdin)['results'][0]['series'][0]['values'][0][1]))
except Exception: print('')" 2>/dev/null || true)
    [[ "$n" == "$want" ]] && break
    if [[ -n "$n" && "$n" == "$last" ]]; then same=$(( same + 1 )); (( same >= 3 )) && break; else same=0; fi
    last=$n; sleep 30
  done
  echo "${n:-0}"
}
declare -A EXPECT=()
# Read back every database written so far; record lost points per database.
readback_all() {
  local step=$1 lost=0
  for db in "${!EXPECT[@]}"; do
    local got; got=$(readback "$db" "${EXPECT[$db]}")
    local miss=$(( ${EXPECT[$db]} - ${got:-0} ))
    record "$step" "readback.$db" "$got/${EXPECT[$db]}"
    (( miss > 0 )) && lost=$(( lost + miss ))
  done
  record "$step" "lost_points" "$lost"
  log "$step: read-back of ${#EXPECT[@]} databases, lost points: $lost"
}

# Every log line of a Job's pods.
job_logs() { k logs -l "job-name=$1" --tail=-1 --max-log-requests=64 2>/dev/null || true; }

start_steady() {
  local label=$1 db="steady_$1"
  k delete job e2e-steady --ignore-not-found >/dev/null
  cat <<JOB | k apply -f - >/dev/null
apiVersion: batch/v1
kind: Job
metadata: {name: e2e-steady, namespace: ${NS}}
spec:
  backoffLimit: 0
  template:
    spec:
      restartPolicy: Never
      containers:
        - name: bench
          image: ${BENCH_IMAGE}
          imagePullPolicy: Never
          command: ["/usr/local/bin/hyperbytedb-cli", "bench-write"]
          resources: {limits: {cpu: "1"}}
          args: ["--endpoints=$(endpoints "$FIRST_N")", "--db=${db}", "--series=${STEADY_SERIES}",
                 "--measure-points=100000000000", "--max-secs=${STEADY_SECS}", "--concurrency=${STEADY_CONC}",
                 "--batch=${BATCH}", "--phase=all", "--workers=1"]
JOB
  # Let it seed and start measuring before anything changes.
  for _ in $(seq 1 60); do
    job_logs e2e-steady | grep -q "seed complete" && break
    sleep 2
  done
}
finish_steady() {
  local step=$1 db="steady_$2"
  k wait --for=condition=complete job/e2e-steady --timeout=$(( STEADY_SECS + 900 ))s >/dev/null 2>&1 || true
  local out; out=$(job_logs e2e-steady)
  echo "$out" >"$OUT_DIR/steady-$2.log"
  latency_record "$step" steady "$out"
  local acked; acked=$(grep -oE '^\[bench\] acked worker=[0-9]+ points=[0-9]+' <<<"$out" | awk -F'points=' '{s+=$2} END{print s+0}')
  local errs; errs=$(grep -oE 'errors=[0-9]+ retries' <<<"$out" | head -1 | grep -oE '[0-9]+' || echo -)
  record "$step" steady.errors "$errs"
  record "$step" steady.points "$acked"
  EXPECT[$db]=$acked
}

# Merge the workers' latency histograms in $3 and record p50/p90/p99/max.
latency_record() { python3 "$OUT_DIR/latency.py" "$1" "$2" <<<"$3" >>"$RESULTS"; }
cat >"$OUT_DIR/latency.py" <<'PY'
import sys,json,re
step,what=sys.argv[1],sys.argv[2]
tot={}
for m in re.finditer(r'latency_ms_hist=(\S+)', sys.stdin.read()):
    for kv in m.group(1).split(','):
        b,c=kv.split(':'); tot[b]=tot.get(b,0)+int(c)
order=sorted(tot, key=lambda b: float('inf') if b=='inf' else float(b))
n=sum(tot.values())
def pct(p):
    if not n: return '-'
    seen=0
    for b in order:
        seen+=tot[b]
        if seen>=p*n: return '>30000' if b=='inf' else f'<={b}'
    return '-'
for p,name in [(0.5,'p50'),(0.9,'p90'),(0.99,'p99'),(1.0,'max')]:
    print(json.dumps({"step":step,"key":f"{what}.latency_ms.{name}","value":pct(p)}))
print(json.dumps({"step":step,"key":f"{what}.requests","value":str(n)}))
PY

cat >"$OUT_DIR/reads.py" <<'PY'
import sys,json,re
for m in re.finditer(r'query=(\S+) queries=(\d+) qps=([\d.]+) p50_ms=([\d.]+) p99_ms=([\d.]+) errors=(\d+)', sys.stdin.read()):
    q=m.group(1)
    for key,v in [('qps',m.group(3)),('p50_ms',m.group(4)),('p99_ms',m.group(5)),('errors',m.group(6))]:
        print(json.dumps({"step":sys.argv[1],"key":f"read.{q}.{key}","value":v}))
PY

write_bench() {
  local n=$1 step="n$1"
  local log="$OUT_DIR/write-$n.log" md="$OUT_DIR/write-$n.md"
  NODES_LIST="$n" RESULTS="$md" KUBE_CTX="$KUBE_CTX" NS="$NS" CR_NAME="$CR_NAME" \
    "$SCRIPT_DIR/run-incluster-sweep.sh" >"$log" 2>&1 || true
  local db; db=$(grep -oE '^\[sweep\] db=\S+' "$log" | tail -1 | cut -d= -f2)
  local row; row=$(grep -E "^\| $n \|" "$md" | tail -1)
  record "$step" write.points_per_sec "$(awk -F'|' '{gsub(/ /,"",$6); print $6}' <<<"$row")"
  record "$step" write.errors "$(awk -F'|' '{gsub(/ /,"",$7); print $7}' <<<"$row")"
  record "$step" write.regions "$(awk -F'|' '{gsub(/ /,"",$4); print $4}' <<<"$row")"
  latency_record "$step" write "$(cat "$log")"
  local acked; acked=$(grep -oE '^\[bench\] acked worker=[0-9]+ points=[0-9]+' "$log" | awk -F'points=' '{s+=$2} END{print s+0}')
  EXPECT[$db]=$acked
  WRITE_DB=$db
  log "N=$n write: $(awk -F'|' '{gsub(/ /,"",$6); print $6}' <<<"$row") points/s into $db"
}

read_bench() {
  local n=$1 db=$2 step="n$1"
  k delete job e2e-read --ignore-not-found >/dev/null
  cat <<JOB | k apply -f - >/dev/null
apiVersion: batch/v1
kind: Job
metadata: {name: e2e-read, namespace: ${NS}}
spec:
  backoffLimit: 0
  template:
    spec:
      restartPolicy: Never
      containers:
        - name: bench
          image: ${BENCH_IMAGE}
          imagePullPolicy: Never
          command: ["/usr/local/bin/hyperbytedb-cli", "bench-read"]
          resources: {limits: {cpu: "2"}}
          args: ["--endpoints=$(endpoints "$n")", "--db=$db", "--series=$(( SERIES_PER_NODE * n ))",
                 "--queries=${READ_QUERIES}", "--duration-secs=${READ_SECS}", "--concurrency=${READ_CONC}"]
JOB
  local kinds; kinds=$(tr ',' '\n' <<<"$READ_QUERIES" | grep -c .)
  k wait --for=condition=complete job/e2e-read --timeout=$(( kinds * (READ_SECS + 150) ))s >/dev/null 2>&1 || true
  local out; out=$(job_logs e2e-read)
  echo "$out" >"$OUT_DIR/read-$n.log"
  python3 "$OUT_DIR/reads.py" "$step" <<<"$out" >>"$RESULTS"
  log "N=$n read bench done on $db"
}

# Scale to $1 with the steady writer running; record ready / settled times
# and region moves.
scale_to() {
  local n=$1 step=$2
  start_steady "$step"
  local p0; p0=$(placements)
  local t0=$SECONDS
  log "$step: scaling to $n"
  k patch hyperbytedbcluster "$CR_NAME" --type merge -p "{\"spec\":{\"replicas\":${n}}}" >/dev/null
  local ready; ready=$(wait_ready "$n")
  local settled; settled=$(wait_settled "$t0" "$n")
  record "$step" scale.pods_ready_secs "$ready"
  record "$step" scale.layout_settled_secs "$settled"
  record "$step" scale.region_moves "$(( $(placements) - p0 ))"
  log "$step: pods ready after ${ready}s, layout settled after ${settled}s"
  finish_steady "$step" "$step"
}

log "reset to $FIRST_N"
t0=$SECONDS
reset_cluster
record "reset" "pods_ready_secs" "$(wait_ready "$FIRST_N")"
record "reset" "layout_settled_secs" "$(wait_settled "$t0" "$FIRST_N")"

write_bench "$FIRST_N"
read_bench "$FIRST_N" "$WRITE_DB"
readback_all "n$FIRST_N"

prev=$FIRST_N
for n in $NODES_UP; do
  scale_to "$n" "up${prev}to${n}"
  readback_all "up${prev}to${n}"
  write_bench "$n"
  read_bench "$n" "$WRITE_DB"
  readback_all "n$n"
  prev=$n
done

scale_to "$FIRST_N" "down${prev}to${FIRST_N}"
readback_all "down${prev}to${FIRST_N}"
record "final" "restarts" "$(k get pods -l "$sel" -o jsonpath='{range .items[*]}{.status.containerStatuses[0].restartCount}{"\n"}{end}' | awk '{s+=$1} END{print s+0}')"

python3 - "$RESULTS" "$OUT_DIR" <<'PY'
import json,sys,collections
rows=[json.loads(l) for l in open(sys.argv[1]) if l.strip()]
by=collections.OrderedDict()
for r in rows: by.setdefault(r["step"],{})[r["key"]]=r["value"]
json.dump(by, open(f"{sys.argv[2]}/results.json","w"), indent=1)
out=["# E2E report",""]
for step,kv in by.items():
    out.append(f"## {step}")
    for k,v in kv.items(): out.append(f"- {k}: {v}")
    out.append("")
open(f"{sys.argv[2]}/report.md","w").write("\n".join(out))
print("\n".join(out))
PY
log "done: $OUT_DIR"
