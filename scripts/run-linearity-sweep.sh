#!/usr/bin/env bash
# Run sharding-linearity-bench.sh at multiple cluster sizes.
# Requires kind cluster with --linearity profile and CR patched per N.
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"
RESULTS="${RESULTS:-$REPO_ROOT/../.scratch/linear-scaling-p1/linearity-results.md}"
KUBE_CTX="${KUBE_CTX:-kind-hyperbytedb}"
NS="${NS:-hyperbytedb}"
API="${API:-http://localhost:18086}"
SERIES="${SERIES:-100000}"
NODES_LIST="${NODES_LIST:-3 6}"
BASELINE_PPS=""

mkdir -p "$(dirname "$RESULTS")"
{
  echo "# Linearity sweep results"
  echo ""
  echo "**Date:** $(date -u +%Y-%m-%dT%H:%M:%SZ)"
  echo "**SERIES:** $SERIES"
  echo ""
  echo "| N | regions | points/sec | errors | vs baseline |"
  echo "|---|---------|------------|--------|-------------|"
} >"$RESULTS"

kubectl_port_forward() {
  fuser -k 18086/tcp 2>/dev/null || true
  kubectl --context "$KUBE_CTX" port-forward -n "$NS" svc/hyperbytedb 18086:8086 >/tmp/pf-linearity.log 2>&1 &
  PF=$!
  sleep 3
}

for N in $NODES_LIST; do
  echo "[sweep] scaling to N=$N"
  kubectl --context "$KUBE_CTX" patch hyperbytedbcluster hyperbytedb -n "$NS" \
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
  kubectl_port_forward
  out=$(NODES="$N" SERIES="$SERIES" API="$API" "$SCRIPT_DIR/sharding-linearity-bench.sh" 2>&1) || true
  kill "$PF" 2>/dev/null || true
  regions=$(echo "$out" | sed -n 's/.*regions=\([0-9]*\).*/\1/p' | tail -1)
  pps=$(echo "$out" | sed -n 's/.*points_per_sec=\([0-9]*\).*/\1/p' | tail -1)
  errors=$(echo "$out" | sed -n 's/.*errors=\([0-9]*\).*/\1/p' | tail -1)
  if [[ -z "$BASELINE_PPS" ]]; then
    BASELINE_PPS="$pps"
    ratio="1.00 (baseline)"
  else
    ratio=$(python3 -c "print(f'{$pps / $BASELINE_PPS:.2f}x')" 2>/dev/null || echo "?")
  fi
  echo "| $N | ${regions:-?} | ${pps:-?} | ${errors:-?} | $ratio |" >>"$RESULTS"
  echo "$out"
done

echo "[sweep] results: $RESULTS"
