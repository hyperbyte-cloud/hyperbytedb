#!/usr/bin/env bash
# Run the in-cluster linearity bench at each N by scaling the CR and launching
# a Job on the cluster itself. See incluster_bench.py for why the client must
# live inside the cluster.
set -euo pipefail
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
KUBE_CTX="${KUBE_CTX:-n3}"
NS="${NS:-hyperbytedb-e2e}"
CR_NAME="${CR_NAME:-hyperbytedb-e2e}"
HEADLESS="${HEADLESS:-${CR_NAME}-headless}"
NODES_LIST="${NODES_LIST:-3 6}"
SERIES_PER_NODE="${SERIES_PER_NODE:-125000}"
MEASURE_POINTS="${MEASURE_POINTS:-2000000}"
CONCURRENCY="${CONCURRENCY:-32}"
# Client pods per cluster node. One Python client tops out near 900k
# lines/s, so a single pod measured the client at N=3, not the cluster.
CLIENTS_PER_NODE="${CLIENTS_PER_NODE:-2}"
RESULTS="${RESULTS:-/tmp/linearity-incluster.md}"
BASELINE=""

kubectl_ctx() { kubectl --context "$KUBE_CTX" "$@"; }

# "regions primaries" for the linearity space, from node 0's shard map.
map_stats() {
  kubectl_ctx -n "$NS" exec "${CR_NAME}-0" -c hyperbytedb -- \
    sh -c 'curl -sS -m 20 http://127.0.0.1:8086/internal/shard/map' 2>/dev/null \
    | python3 -c "
import sys,json
try: d=json.load(sys.stdin)
except Exception: print(0, 0); raise SystemExit
rs=[r for sp in d.get('spaces',[]) for r in sp.get('regions',[])]
print(len(rs), len({r.get('primary') for r in rs}))" 2>/dev/null || echo "0 0"
}

# Block until the space has at least N regions and the region and primary
# counts have held for SPLIT_STABLE_SECS, or SPLIT_TIMEOUT_SECS passes.
wait_for_split() {
  local n=$1 deadline=$(( SECONDS + ${SPLIT_TIMEOUT_SECS:-900} ))
  local last="" since=$SECONDS started=$SECONDS regions primaries
  while (( SECONDS < deadline )); do
    read -r regions primaries < <(map_stats)
    # Primaries too: placement moves them after the split settles, and
    # measuring before that timed one primary node at N=6.
    if [[ "$regions/$primaries" != "$last" ]]; then last="$regions/$primaries"; since=$SECONDS; fi
    echo "[sweep] N=$n regions=$regions primaries=$primaries stable=$(( SECONDS - since ))s"
    # Primaries must actually be spread: children of a split inherit the
    # parent's primary, and moving primacy (learner, copy, promote) takes
    # several scheduler ticks per region. A count that merely held still
    # measured one node holding 7 of 8 primaries.
    local want=$(( regions < n ? regions : n ))
    if (( regions >= n && primaries >= want && SECONDS - since >= ${SPLIT_STABLE_SECS:-60} )); then
      echo "[sweep] N=$n converged in $(( SECONDS - started ))s"
      return 0
    fi
    sleep 15
  done
  echo "[sweep] N=$n split wait timed out at regions=$regions"
}

# Drop every database and wait until the shard map has no spaces left.
# A drop issued straight after a saturating run can time out; a leftover
# space then doubles the next point's regions and keeps its transfers
# running underneath the measurement.
drop_all_spaces() {
  local deadline=$(( SECONDS + 300 )) dbs regions
  while (( SECONDS < deadline )); do
    dbs=$(kubectl_ctx -n "$NS" exec "${CR_NAME}-0" -c hyperbytedb -- \
      sh -c "curl -sS -m 20 -G http://127.0.0.1:8086/query --data-urlencode 'q=SHOW DATABASES'" 2>/dev/null \
      | python3 -c "
import sys,json
try: d=json.load(sys.stdin)
except Exception: raise SystemExit
for s in d['results'][0].get('series',[]):
  for v in s.get('values',[]): print(v[0])" 2>/dev/null)
    for db in $dbs; do
      kubectl_ctx -n "$NS" exec "${CR_NAME}-0" -c hyperbytedb -- \
        sh -c "curl -sS -m 60 -X POST http://127.0.0.1:8086/query --data-urlencode 'q=DROP DATABASE ${db}'" >/dev/null 2>&1 || true
    done
    read -r regions _ < <(map_stats)
    if [[ -z "$dbs" && "$regions" == 0 ]]; then return 0; fi
    sleep 10
  done
  echo "[sweep] WARNING: spaces still present after drop (regions=$regions)"
}

# Run one bench phase as a Job and print its log.
run_phase() {
  local phase=$1 eps=$2 series=$3 workers=$4
  kubectl_ctx -n "$NS" delete job linearity-bench --ignore-not-found >/dev/null 2>&1 || true
  cat <<JOB | kubectl_ctx apply -f - >/dev/null
apiVersion: batch/v1
kind: Job
metadata:
  name: linearity-bench
  namespace: ${NS}
spec:
  backoffLimit: 0
  ttlSecondsAfterFinished: 600
  completionMode: Indexed
  completions: ${workers}
  parallelism: ${workers}
  template:
    spec:
      restartPolicy: Never
      containers:
        - name: bench
          image: python:3.12-slim
          command: ["python3", "/bench/incluster_bench.py"]
          args:
            - "--endpoints=${eps}"
            - "--series=${series}"
            - "--measure-points=${MEASURE_POINTS}"
            - "--concurrency=${CONCURRENCY}"
            - "--phase=${phase}"
            - "--db=${DB}"
            - "--workers=${workers}"
          volumeMounts:
            - name: bench
              mountPath: /bench
      volumes:
        - name: bench
          configMap:
            name: linearity-bench
JOB
  kubectl_ctx -n "$NS" wait --for=condition=complete job/linearity-bench --timeout=1800s >/dev/null 2>&1 || true
  kubectl_ctx -n "$NS" logs -l job-name=linearity-bench --tail=-1 --max-log-requests=64 2>/dev/null || true
}

{
  echo "# Linearity sweep (in-cluster client)"
  echo ""
  echo "**Date:** $(date -u +%Y-%m-%dT%H:%M:%SZ)"
  echo "**Series per node:** $SERIES_PER_NODE   **Measure points:** $MEASURE_POINTS   **Concurrency:** $CONCURRENCY per client, $CLIENTS_PER_NODE clients per node"
  echo ""
  echo "| N | series | regions | primaries | points/sec | errors | retries | vs baseline | valid |"
  echo "|---|--------|---------|-----------|------------|--------|---------|-------------|-------|"
} >"$RESULTS"

kubectl_ctx -n "$NS" create configmap linearity-bench \
  --from-file=incluster_bench.py="$SCRIPT_DIR/incluster_bench.py" \
  --dry-run=client -o yaml | kubectl_ctx apply -f - >/dev/null

drop_all_spaces
for N in $NODES_LIST; do
  echo "[sweep] scaling to N=$N"
  kubectl_ctx -n "$NS" patch hyperbytedbcluster "$CR_NAME" --type merge \
    -p "{\"spec\":{\"replicas\":${N}}}" >/dev/null
  deadline=$(( SECONDS + 900 ))
  ready=0; total=0
  while (( SECONDS < deadline )); do
    pods=$(kubectl_ctx -n "$NS" get pods -l app.kubernetes.io/name=hyperbytedb \
      --no-headers 2>/dev/null || true)
    total=$(grep -c . <<<"$pods" || true)
    ready=$(grep -c '1/1 *Running' <<<"$pods" || true)
    # Exactly N, not at least N: going 6 -> 3, six pods are still ready while
    # the scale-down evacuates, and the old ">=" check benched that.
    (( ready == N && total == N )) && break
    sleep 10
  done
  echo "[sweep] N=$N ready=$ready total=$total"
  # Refuse to measure a cluster that is not the size we asked for. The first
  # run reported "N=6 ready=3" and benched anyway against six endpoints, three
  # of which did not resolve -- producing a throughput number from failures.
  if (( ready != N || total != N )); then
    echo "| $N | - | - | - | - | - | - | — | NO ($ready ready of $total pods, want $N) |" >>"$RESULTS"
    continue
  fi
  sleep 45

  # Fresh dataset per point: without this, N=6 measures N=3's series as well.
  eps=""
  for ((i=0; i<N; i++)); do
    eps="${eps}${eps:+,}http://${CR_NAME}-${i}.${HEADLESS}.${NS}.svc.cluster.local:8086"
  done
  series=$(( SERIES_PER_NODE * N ))

  workers=$(( CLIENTS_PER_NODE * N ))
  # A fresh database per point. Dropping and re-creating the same one while
  # flushes are pending leaves the chDB series table and the schema cache out
  # of step; the flush then fails forever ("No such column host"), the WAL
  # backs up, and the nodes starve until liveness kills them. Plan item B1.
  DB="linearity_${N}_$(date +%s)"
  run_phase "seed" "$eps" "$series" "$workers" | grep -E 'FATAL|seed complete' | sort | uniq -c
  # Wait for the space to split before timing anything. With DROP DATABASE
  # now really dropping the shard space, every point starts from ONE region;
  # measuring straight after the seed timed one primary at every N.
  wait_for_split "$N"
  out=$(run_phase "measure" "$eps" "$series" "$workers")
  echo "$out" | grep -E '^\[bench\] (worker=|error x)'
  # Total points over the slowest worker's time: the cluster absorbed all of
  # it within that window. Blank unless every worker reported.
  read -r pps errs retries < <(echo "$out" | python3 -c "
import re,sys
rows=[tuple(float(g or 0) for g in m.groups()) for m in re.finditer(r'worker=\d+/\d+ points=(\d+) duration=([0-9.]+)s errors=(\d+)(?: retries=(\d+))?', sys.stdin.read())]
if len(rows) != $workers or not rows: print('-', '-', '-'); raise SystemExit
pts=sum(r[0] for r in rows); dur=max(r[1] for r in rows)
print(int(pts/dur) if dur>0 else '-', int(sum(r[2] for r in rows)), int(sum(r[3] for r in rows)))")
  [[ "$pps" == "-" ]] && pps=""
  read -r regions primaries < <(map_stats)
  # Layout at measure time: per node, regions it is primary of, regions it
  # holds a replica of, and the share of the hash space it is primary for
  # (series are uniform in hash space, so that is its share of the load).
  kubectl_ctx -n "$NS" exec "${CR_NAME}-0" -c hyperbytedb -- \
    sh -c 'curl -sS -m 20 http://127.0.0.1:8086/internal/shard/map' 2>/dev/null \
    | python3 -c "
import sys,json,collections
d=json.load(sys.stdin); P=collections.Counter(); R=collections.Counter(); W=collections.Counter()
for sp in d.get('spaces',[]):
  for r in sp['regions']:
    P[r['primary']]+=1; W[r['primary']]+=(r['end']-r['start'])/2**64
    for p in r['peers']:
      if p!=r['primary']: R[p]+=1
for n in sorted(set(P)|set(R)): print(f'[layout] node={n} primary={P[n]} replica={R[n]} load_share={W[n]:.2f}')" || true

  valid="yes"
  if [[ -z "${pps:-}" ]]; then valid="NO (no numbers)"
  elif (( regions < N )); then valid="NO (regions=$regions < N=$N)"
  elif [[ -n "${errs:-}" ]] && (( errs * 100 > MEASURE_POINTS / 1000 )); then valid="NO (errors=$errs)"
  fi

  if [[ "$valid" == "yes" && -z "$BASELINE" ]]; then
    BASELINE="$pps"; ratio="1.00 (baseline)"
  elif [[ "$valid" == "yes" ]]; then
    ratio=$(python3 -c "print(f'{$pps/$BASELINE:.2f}x')")
  else ratio="—"; fi
  echo "| $N | $series | ${regions:-?} | ${primaries:-?} | ${pps:-?} | ${errs:-?} | ${retries:-?} | $ratio | $valid |" >>"$RESULTS"
  # Drop the dataset so the next N starts clean.
  drop_all_spaces
  sleep 20
done
echo "[sweep] results: $RESULTS"
