#!/usr/bin/env bash
# Run sharding-linearity-bench.sh at multiple cluster sizes.
# Requires kind cluster with --linearity profile and CR patched per N.
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"
RESULTS="${RESULTS:-$REPO_ROOT/../.scratch/linear-scaling-p1/linearity-results.md}"
KUBE_CTX="${KUBE_CTX:-kind-hyperbytedb}"
NS="${NS:-hyperbytedb}"
CR_NAME="${CR_NAME:-hyperbytedb}"
POD_PREFIX="${POD_PREFIX:-$CR_NAME}"
API="${API:-http://localhost:18086}"
# Series count must grow WITH the cluster, or the sweep measures nothing.
#
# The old defaults were a fixed 100k series over "3 6". With
# regionSplitSeries=100000 that is about ONE region, so every node but one sat
# idle and throughput was flat by construction — the sweep could not have
# detected linear scaling even if it were present. Each node needs enough
# series to own several regions before "points/sec vs N" means anything.
SERIES_PER_NODE="${SERIES_PER_NODE:-125000}"
NODES_LIST="${NODES_LIST:-3 6 12}"
BASELINE_PPS=""
INVALID_ROWS=0

mkdir -p "$(dirname "$RESULTS")"
{
  echo "# Linearity sweep results"
  echo ""
  echo "**Date:** $(date -u +%Y-%m-%dT%H:%M:%SZ)"
  echo "**SERIES per node:** $SERIES_PER_NODE (total = per-node x N)"
  echo ""
  echo "| N | series | regions | points/sec | errors | vs baseline | valid |"
  echo "|---|--------|---------|------------|--------|-------------|-------|"
} >"$RESULTS"

# One port-forward PER POD, not one to the Service.
#
# `kubectl port-forward svc/...` picks a SINGLE backing pod and sends every
# connection there — it does not load-balance. Driving the whole sweep through
# one entry node measures that node's forwarding capacity, not the cluster's
# ingest capacity: as N rises a larger share of regions is remote to the entry
# node, so throughput FALLS with cluster size. That is what produced a
# measured 33k points/sec at N=3 and 26k at N=6 — an artifact of the harness,
# not of the system. A write-scaling benchmark has to enter through every node.
PF_PIDS=""
kubectl_port_forward() {
  local n=$1
  local i=0
  PF_PIDS=""
  API_LIST=""
  while (( i < n )); do
    local port=$(( 18086 + i ))
    fuser -k "${port}/tcp" 2>/dev/null || true
    kubectl --context "$KUBE_CTX" port-forward -n "$NS" "pod/${POD_PREFIX}-${i}" \
      "${port}:8086" >>/tmp/pf-linearity.log 2>&1 &
    PF_PIDS="$PF_PIDS $!"
    API_LIST="${API_LIST}${API_LIST:+,}http://localhost:${port}"
    i=$(( i + 1 ))
  done
  export API_LIST
  sleep 5
}

kubectl_port_forward_stop() {
  for pid in $PF_PIDS; do kill "$pid" 2>/dev/null || true; done
  PF_PIDS=""
}

for N in $NODES_LIST; do
  echo "[sweep] scaling to N=$N"
  kubectl --context "$KUBE_CTX" patch hyperbytedbcluster "$CR_NAME" -n "$NS" \
    --type merge -p "{\"spec\":{\"replicas\":${N}}}" >/dev/null
  deadline=$((SECONDS + 600))
  while (( SECONDS < deadline )); do
    ready=$(kubectl --context "$KUBE_CTX" get pods -n "$NS" -l app.kubernetes.io/name=hyperbytedb \
      --field-selector=status.phase=Running -o json | python3 -c "
import json,sys
d=json.load(sys.stdin)
print(sum(1 for p in d.get('items',[]) if all(c.get('ready') for c in p['status'].get('containerStatuses',[]))))
")
    if [[ "$ready" == "$N" ]]; then break; fi
    sleep 10
  done
  sleep 60
  kubectl_port_forward "$N"

  # Clear the dataset between sweep points.
  #
  # The sweep patches replicas and re-seeds into the SAME database, so without
  # this each N inherits every earlier N's series: the N=6 point carried N=3's
  # 375k on top of its own 750k. That made throughput across N incomparable
  # (each point measures a bigger dataset than the last) and pushed node 0 over
  # its memory limit — an isolated N=6 run on a fresh cluster survives where
  # the same point inside a sweep does not.
  #
  # DROP DATABASE now removes the shard spaces too, so region counts start from
  # zero rather than inheriting the previous point's.
  first_api="${API_LIST%%,*}"
  curl -sf -X POST "${first_api}/query" \
    --data-urlencode "q=DROP DATABASE linearity" >/dev/null 2>&1 || true
  sleep 10

  # Total series scales with N so each node can own multiple regions.
  series=$(( SERIES_PER_NODE * N ))
  echo "[sweep] N=$N series=$series"
  out=$(NODES="$N" SERIES="$series" API="$API" API_LIST="$API_LIST" \
    "$SCRIPT_DIR/sharding-linearity-bench.sh" 2>&1) || true
  kubectl_port_forward_stop
  regions=$(echo "$out" | sed -n 's/.*regions=\([0-9]*\).*/\1/p' | tail -1)
  pps=$(echo "$out" | sed -n 's/.*points_per_sec=\([0-9]*\).*/\1/p' | tail -1)
  errors=$(echo "$out" | sed -n 's/.*errors=\([0-9]*\).*/\1/p' | tail -1)
  # A run with fewer regions than nodes cannot possibly scale: at least one
  # node owns no primary, so its capacity is unused and the number is not a
  # linearity measurement. Record it, but never as a valid data point and
  # never as the baseline.
  # Batch count the bench used for its measurement pass, for the error RATE.
  measure_batches_hint=$(( ${MEASURE_POINTS:-1000000} / ${BATCH:-1000} ))
  (( measure_batches_hint > 0 )) || measure_batches_hint=1
  valid="yes"
  if [[ -z "${regions:-}" || -z "${pps:-}" ]]; then
    valid="NO (bench produced no numbers)"
  elif (( regions < N )); then
    valid="NO (regions=$regions < N=$N)"
  elif [[ -n "${errors:-}" ]] && (( errors * 100 > measure_batches_hint )); then
    # Rate, not count. A point where writes were REJECTED is not a throughput
    # measurement and must never become the baseline — the first version of
    # this gate checked only regions>=N, so an N=6 run that failed 750 of 750
    # batches was recorded as valid and used as the baseline. But requiring
    # ZERO errors is the opposite failure: a converging cluster produces the
    # occasional partial write, and discarding an otherwise-sound measurement
    # for 1 batch in 1000 means the sweep can never record anything.
    # Threshold: >1% of batches.
    valid="NO (error rate ${errors}/${measure_batches_hint} > 1%)"
  fi

  if [[ "$valid" == "yes" && -z "$BASELINE_PPS" ]]; then
    BASELINE_PPS="$pps"
    ratio="1.00 (baseline)"
  elif [[ "$valid" == "yes" ]]; then
    ratio=$(python3 -c "print(f'{$pps / $BASELINE_PPS:.2f}x')" 2>/dev/null || echo "?")
  else
    ratio="—"
    INVALID_ROWS=$(( INVALID_ROWS + 1 ))
  fi
  echo "| $N | $series | ${regions:-?} | ${pps:-?} | ${errors:-?} | $ratio | $valid |" >>"$RESULTS"
  echo "$out"
done

{
  echo ""
  if (( INVALID_ROWS > 0 )); then
    echo "**$INVALID_ROWS row(s) invalid** — a sweep point with fewer regions than"
    echo "nodes leaves nodes idle and is not a linearity measurement."
  else
    echo "All sweep points had regions >= N."
  fi
} >>"$RESULTS"

echo "[sweep] results: $RESULTS"
if (( INVALID_ROWS > 0 )); then
  echo "[sweep] WARNING: $INVALID_ROWS invalid row(s); linearity not established"
fi
