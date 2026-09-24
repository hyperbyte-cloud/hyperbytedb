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
CONCURRENCY="${CONCURRENCY:-64}"
RESULTS="${RESULTS:-/tmp/linearity-incluster.md}"
BASELINE=""

kubectl_ctx() { kubectl --context "$KUBE_CTX" "$@"; }

{
  echo "# Linearity sweep (in-cluster client)"
  echo ""
  echo "**Date:** $(date -u +%Y-%m-%dT%H:%M:%SZ)"
  echo "**Series per node:** $SERIES_PER_NODE   **Measure points:** $MEASURE_POINTS   **Concurrency:** $CONCURRENCY"
  echo ""
  echo "| N | series | regions | points/sec | errors | vs baseline | valid |"
  echo "|---|--------|---------|------------|--------|-------------|-------|"
} >"$RESULTS"

kubectl_ctx -n "$NS" create configmap linearity-bench \
  --from-file=incluster_bench.py="$SCRIPT_DIR/incluster_bench.py" \
  --dry-run=client -o yaml | kubectl_ctx apply -f - >/dev/null

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
    echo "| $N | - | - | - | - | — | NO ($ready ready of $total pods, want $N) |" >>"$RESULTS"
    continue
  fi
  sleep 45

  # Fresh dataset per point: without this, N=6 measures N=3's series as well.
  first="http://${CR_NAME}-0.${HEADLESS}.${NS}.svc.cluster.local:8086"
  kubectl_ctx -n "$NS" delete job linearity-bench --ignore-not-found >/dev/null 2>&1 || true

  eps=""
  for ((i=0; i<N; i++)); do
    eps="${eps}${eps:+,}http://${CR_NAME}-${i}.${HEADLESS}.${NS}.svc.cluster.local:8086"
  done
  series=$(( SERIES_PER_NODE * N ))

  cat <<JOB | kubectl_ctx apply -f - >/dev/null
apiVersion: batch/v1
kind: Job
metadata:
  name: linearity-bench
  namespace: ${NS}
spec:
  backoffLimit: 0
  ttlSecondsAfterFinished: 600
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
          volumeMounts:
            - name: bench
              mountPath: /bench
      volumes:
        - name: bench
          configMap:
            name: linearity-bench
JOB

  kubectl_ctx -n "$NS" wait --for=condition=complete job/linearity-bench --timeout=1800s >/dev/null 2>&1 || true
  out=$(kubectl_ctx -n "$NS" logs job/linearity-bench 2>/dev/null || true)
  echo "$out"

  pps=$(echo "$out" | sed -n 's/.*points_per_sec=\([0-9]*\).*/\1/p' | tail -1)
  errs=$(echo "$out" | sed -n 's/.*duration=[0-9.]*s errors=\([0-9]*\).*/\1/p' | tail -1)
  regions=$(kubectl_ctx -n "$NS" exec "${CR_NAME}-0" -c hyperbytedb -- \
    sh -c 'curl -sS -m 20 http://127.0.0.1:8086/internal/shard/map' 2>/dev/null \
    | python3 -c "
import sys,json
try: d=json.load(sys.stdin)
except Exception: print(0); raise SystemExit
print(sum(len(sp.get('regions',[])) for sp in d.get('spaces',[])))" 2>/dev/null || echo 0)

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
  echo "| $N | $series | ${regions:-?} | ${pps:-?} | ${errs:-?} | $ratio | $valid |" >>"$RESULTS"
  # Drop the dataset so the next N starts clean.
  kubectl_ctx -n "$NS" exec "${CR_NAME}-0" -c hyperbytedb -- \
    sh -c 'curl -sS -m 30 -X POST "http://127.0.0.1:8086/query" --data-urlencode "q=DROP DATABASE linearity"' >/dev/null 2>&1 || true
  sleep 20
done
echo "[sweep] results: $RESULTS"
