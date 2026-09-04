#!/usr/bin/env bash
# Run sharding perf write/query benchmarks against port-forwarded cluster node.
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
TARGET_HOST="${TARGET_HOST:-127.0.0.1}"
TARGET_PORT="${TARGET_PORT:-18080}"
LOG_DIR="${LOG_DIR:-$SCRIPT_DIR/../log/sharding-perf/$(date +%Y-%m-%d_%H-%M-%S)}"
mkdir -p "$LOG_DIR"

run_write() {
  local name="$1" rps="$2" duration="$3" points="$4"
  echo "=== write: $name (rps=$rps duration=$duration points=$points) ==="
  RPS="$rps" DURATION="$duration" POINTS_PER_REQUEST="$points" RUN_BENCHES=0 \
    TARGET_HOST="$TARGET_HOST" TARGET_PORT="$TARGET_PORT" \
    "$SCRIPT_DIR/load.sh" cluster "$TARGET_HOST" "$TARGET_PORT" "$rps" "$duration" "$points" \
    2>&1 | tee "$LOG_DIR/write-${name}.log" | tail -20
}

run_query() {
  echo "=== query: sharded metrics ==="
  QUERY_ITERATIONS="${QUERY_ITERATIONS:-50}" TARGET_HOST="$TARGET_HOST" TARGET_PORT="$TARGET_PORT" \
    k6 run "$SCRIPT_DIR/sharding-perf-query.js" 2>&1 | tee "$LOG_DIR/query.log"
}

echo "Collecting shard metrics from leader..."
curl -sf "http://${TARGET_HOST}:${TARGET_PORT}/metrics" \
  | grep -E '^hyperbytedb_shard_|^hyperbytedb_raft_|^hyperbytedb_replication_' \
  | tee "$LOG_DIR/shard-metrics.txt" || true

run_write steady 200 60s 100
run_write burst 1000 30s 50
run_write cardinality 100 60s 500
run_query

echo "Results in $LOG_DIR"
