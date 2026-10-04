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
# Points per client write. Telegraf's default metric_batch_size is 1000.
BATCH="${BATCH:-1000}"
# Client pods per cluster node. One Python client tops out near 900k
# lines/s, so a single pod measured the client at N=3, not the cluster.
CLIENTS_PER_NODE="${CLIENTS_PER_NODE:-2}"
# rust: `hyperbytedb-cli bench-write` from the DB image (a quarter of the
# Python client's CPU per point, keep-alive connections). python: the
# original incluster_bench.py.
BENCH_CLIENT="${BENCH_CLIENT:-rust}"
BENCH_IMAGE="${BENCH_IMAGE:-docker.io/library/hyperbytedb:e2e}"
# CPU limit per client pod, so clients cannot crowd the DB on a shared host.
BENCH_CPU="${BENCH_CPU:-1}"
RESULTS="${RESULTS:-/tmp/linearity-incluster.md}"
# 1: drop every database before the sweep and after each N (the old
# behaviour). 0 (default): keep them. Dropping right after a measure applied
# DROP DATABASE under load: on n3 a node applying it OOM-crashed and replayed
# the drop on every restart. Each N writes its own database, and the region
# and layout checks look only at that database's spaces.
SWEEP_DROP="${SWEEP_DROP:-0}"
export SPACE_DB=""
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
import os
want=os.environ.get('SPACE_DB','')
rs=[r for sp in d.get('spaces',[]) if not want or sp.get('key',{}).get('db')==want for r in sp.get('regions',[])]
import collections
lay=collections.Counter()
for r in rs:
  lay[(r.get('primary'),'p')]+=1
  for p in r.get('peers',[]): lay[(p,'c')]+=1
sig='-'.join(f'{k[0]}{k[1]}{v}' for k,v in sorted(lay.items()))
print(len(rs), len({r.get('primary') for r in rs}), sig or 'none')" 2>/dev/null || echo "0 0 none"
}

# Over this many series a region must split (the CR's regionMaxSeries).
REGION_MAX_SERIES="${REGION_MAX_SERIES:-150000}"

# "known over" for the linearity space, from the raft leader's scheduler:
# regions whose heartbeat series count has reached the leader, and regions
# still over REGION_MAX_SERIES. A new database's counts take ~100 s to reach
# the leader when another database's regions are reported ahead of it; until
# then its regions look stable at the pre-split count and r8 measured N=6 and
# N=8 on 12 and 16 unsplit regions.
region_series_stats() {
  local lid
  lid=$(kubectl_ctx -n "$NS" exec "${CR_NAME}-0" -c hyperbytedb -- \
    sh -c 'curl -sS -m 5 http://127.0.0.1:8086/cluster/leader' 2>/dev/null \
    | python3 -c "import sys,json; print(json.load(sys.stdin).get('leader_id') or '')" 2>/dev/null) || true
  [[ -n "$lid" ]] || { echo "0 0"; return; }
  kubectl_ctx -n "$NS" exec "${CR_NAME}-$(( lid - 1 ))" -c hyperbytedb -- \
    sh -c 'curl -sS -m 20 http://127.0.0.1:8086/internal/shard/map; echo; curl -sS -m 20 http://127.0.0.1:8086/metrics' 2>/dev/null \
    | python3 -c "
import sys,json,os,re
lines=sys.stdin.read().split(chr(10),1)
try: d=json.loads(lines[0])
except Exception: print(0, 0); raise SystemExit
sc={int(m.group(1)): float(m.group(2)) for m in re.finditer(r'^hyperbytedb_shard_heartbeat_series_count\{region_id=\"(\d+)\"\} (\S+)', lines[1] if len(lines)>1 else '', re.M)}
want=os.environ.get('SPACE_DB','')
ids=[r['region_id'] for sp in d.get('spaces',[]) if not want or sp.get('key',{}).get('db')==want for r in sp.get('regions',[])]
known=sum(1 for i in ids if sc.get(i,0)>0)
over=sum(1 for i in ids if sc.get(i,0)>$REGION_MAX_SERIES)
print(known, over)" 2>/dev/null || echo "0 0"
}

# Block until the space has at least N regions, every region's series count
# has reached the leader and none is over REGION_MAX_SERIES, and the region
# and primary counts have held for SPLIT_STABLE_SECS, or SPLIT_TIMEOUT_SECS
# passes.
wait_for_split() {
  local n=$1 deadline=$(( SECONDS + ${SPLIT_TIMEOUT_SECS:-900} ))
  local last="" since=$SECONDS started=$SECONDS regions primaries layout known over
  while (( SECONDS < deadline )); do
    read -r regions primaries layout < <(map_stats)
    read -r known over < <(region_series_stats)
    # The whole layout -- every node's copies and primaries -- must hold
    # still, not just the counts: copy moves continue after primaries have
    # spread, and measuring mid-move timed one node at 46% of the load.
    if [[ "$regions/$primaries/$layout" != "$last" ]]; then last="$regions/$primaries/$layout"; since=$SECONDS; fi
    echo "[sweep] N=$n regions=$regions primaries=$primaries series_known=$known over_max=$over stable=$(( SECONDS - since ))s"
    # Primaries must actually be spread: children of a split inherit the
    # parent's primary, and moving primacy (learner, copy, promote) takes
    # several scheduler ticks per region. A count that merely held still
    # measured one node holding 7 of 8 primaries.
    local want=$(( regions < n ? regions : n ))
    if (( regions >= n && primaries >= want && known == regions && over == 0 \
          && SECONDS - since >= ${SPLIT_STABLE_SECS:-60} )); then
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
    read -r regions _ _ < <(map_stats)
    if [[ -z "$dbs" && "$regions" == 0 ]]; then return 0; fi
    sleep 10
  done
  echo "[sweep] WARNING: spaces still present after drop (regions=$regions)"
}

# Image and command lines of the bench container, per BENCH_CLIENT.
bench_container() {
  if [[ "$BENCH_CLIENT" == "python" ]]; then
    printf '%s\n' '          image: python:3.12-slim' \
      '          command: ["python3", "/bench/incluster_bench.py"]'
  else
    printf '%s\n' "          image: ${BENCH_IMAGE}" '          imagePullPolicy: Never' \
      '          command: ["/usr/local/bin/hyperbytedb-cli", "bench-write"]'
  fi
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
$(bench_container)
          resources:
            limits:
              cpu: "${BENCH_CPU}"
          args:
            - "--endpoints=${eps}"
            - "--series=${series}"
            - "--measure-points=${MEASURE_POINTS}"
            - "--concurrency=${CONCURRENCY}"
            - "--batch=${BATCH}"
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
  echo "**Series per node:** $SERIES_PER_NODE   **Measure points:** $MEASURE_POINTS   **Concurrency:** $CONCURRENCY per client, $CLIENTS_PER_NODE clients per node   **Batch:** $BATCH points"
  echo ""
  echo "| N | series | regions | primaries | points/sec | errors | retries | vs baseline | valid |"
  echo "|---|--------|---------|-----------|------------|--------|---------|-------------|-------|"
} >"$RESULTS"

kubectl_ctx -n "$NS" create configmap linearity-bench \
  --from-file=incluster_bench.py="$SCRIPT_DIR/incluster_bench.py" \
  --dry-run=client -o yaml | kubectl_ctx apply -f - >/dev/null

[[ "$SWEEP_DROP" == 1 ]] && drop_all_spaces
for N in $NODES_LIST; do
  echo "[sweep] scaling to N=$N"
  kubectl_ctx -n "$NS" patch hyperbytedbcluster "$CR_NAME" --type merge \
    -p "{\"spec\":{\"replicas\":${N}}}" >/dev/null
  deadline=$(( SECONDS + ${READY_TIMEOUT_SECS:-2400} ))
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
  export SPACE_DB="$DB"
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
  read -r regions primaries _ < <(map_stats)
  # Layout at measure time: per node, regions it is primary of, regions it
  # holds a replica of, and the share of the hash space it is primary for
  # (series are uniform in hash space, so that is its share of the load).
  kubectl_ctx -n "$NS" exec "${CR_NAME}-0" -c hyperbytedb -- \
    sh -c 'curl -sS -m 20 http://127.0.0.1:8086/internal/shard/map' 2>/dev/null \
    | python3 -c "
import sys,json,collections,os
want=os.environ.get('SPACE_DB','')
d=json.load(sys.stdin); P=collections.Counter(); R=collections.Counter(); W=collections.Counter()
for sp in d.get('spaces',[]):
  if want and sp.get('key',{}).get('db')!=want: continue
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
  # Drop the dataset so the next N starts clean (SWEEP_DROP=1 only).
  if [[ "$SWEEP_DROP" == 1 ]]; then
    drop_all_spaces
    sleep 20
  fi
done
echo "[sweep] results: $RESULTS"
