#!/usr/bin/env bash
# Acceptance benchmark for linear scaling.
#
# One measurement, fixed load, sustained points/sec. Run at N = 3, 6, 12, 24,
# 48 and compare against the N=3 baseline. Target is >=80% of ideal linear
# scaling at N=48 (>=12.8x throughput at 16x the nodes), with no configuration
# slower than a smaller one.
#
# Deliberately does NOT delegate to scripts/sharding-perf-ingest.sh: that
# script hardcodes 100k series into measurement "metrics" and honours only API
# and DB. Driving it with SERIES=5000000 would silently ingest 100k series,
# produce roughly one region, and report a flat curve at every node count --
# "proving" the design failed when nothing was measured.
#
# SERIES defaults to 5M for a reason. A measurement occupies at most as many
# nodes as it has regions; at region_split_series=100000 /
# region_max_series=150000 the region count is roughly series/125k, so fewer
# than ~5M series cannot saturate 48 nodes however good placement is.
#
# BEFORE TRUSTING A FLAT CURVE: check regions >= nodes. A flat curve with an
# even distribution over too few regions is the cardinality floor, not a
# placement defect.
set -euo pipefail

API="${API:-http://localhost:18086}"
NODES="${NODES:?set NODES to the cluster size under test}"
SERIES="${SERIES:-5000000}"
BATCH="${BATCH:-1000}"
DB="${DB:-linearity}"
RP="${RP:-autogen}"
MEASUREMENT="${MEASUREMENT:-cpu}"

# Comma-separated entry points, one per node. Writes rotate across them so the
# measurement is of the CLUSTER's ingest capacity rather than one node's
# forwarding capacity. Falls back to the single $API when unset.
API_LIST="${API_LIST:-$API}"
IFS=',' read -r -a API_ENDPOINTS <<< "$API_LIST"
(( ${#API_ENDPOINTS[@]} > 0 )) || API_ENDPOINTS=("$API")
api_for() { echo "${API_ENDPOINTS[$(( $1 % ${#API_ENDPOINTS[@]} ))]}"; }
echo "[bench] entry points: ${#API_ENDPOINTS[@]}"

echo "[bench] nodes=$NODES series=$SERIES batch=$BATCH db=$DB api=$API"
curl -sf -X POST "${API}/query" --data-urlencode "q=CREATE DATABASE ${DB}" >/dev/null

batches=$(( (SERIES + BATCH - 1) / BATCH ))
errors=0
start_ts=$(date +%s)

for b in $(seq 0 $((batches - 1))); do
  base=$((b * BATCH))
  body=""
  for i in $(seq "$base" $((base + BATCH - 1))); do
    body+="${MEASUREMENT},host=s${i} value=1 $((1700000000 + i))000000000\n"
  done
  code=$(curl -s -o /dev/null -w "%{http_code}" \
    -X POST "$(api_for "$b")/write?db=${DB}&rp=${RP}&precision=ns" \
    --data-binary "$(printf "%b" "$body")")
  if [[ "$code" != "204" ]]; then
    echo "batch $b failed: HTTP $code" >&2
    errors=$((errors + 1))
  fi
  if (( b % 100 == 0 )); then
    echo "  batch $b/$((batches - 1)) (HTTP $code)"
  fi
done

seed_duration=$(( $(date +%s) - start_ts ))
(( seed_duration > 0 )) || seed_duration=1
echo "[bench] seed complete in ${seed_duration}s (errors=${errors})"

# Settle before measuring.
#
# Splitting is driven by the heartbeat (series counts, every
# heartbeatIntervalSecs) and then a scheduler tick, so NO split can occur
# inside a seed window of a few seconds. Timing the seed therefore measured a
# single region on a single primary at every node count — a flat curve by
# construction, which is exactly the trap this script's own header warns
# about. Wait for the regions to exist and their primaries to spread, THEN
# measure.
SETTLE_SECS="${SETTLE_SECS:-300}"
settle_deadline=$(( $(date +%s) + SETTLE_SECS ))
while (( $(date +%s) < settle_deadline )); do
  r=$(curl -sf "${API}/internal/shard/map" 2>/dev/null \
    | python3 -c "
import json,sys
try: d=json.load(sys.stdin)
except Exception: print(0); raise SystemExit
print(sum(len(sp.get('regions',[])) for sp in d.get('spaces',[])))
" 2>/dev/null || echo 0)
  case "$r" in ''|*[!0-9]*) r=0 ;; esac
  if (( r >= NODES )); then
    echo "[bench] settled: regions=${r} >= nodes=${NODES}"
    break
  fi
  sleep 10
done

# Measurement pass: rewrite a slice of the existing series, so cardinality is
# already established and this times steady-state ingest against the converged
# region layout rather than first-touch series creation.
MEASURE_POINTS="${MEASURE_POINTS:-1000000}"
measure_batches=$(( (MEASURE_POINTS + BATCH - 1) / BATCH ))
errors=0
start_ms=$(date +%s%3N)
for b in $(seq 0 $((measure_batches - 1))); do
  base=$((b * BATCH))
  body=""
  for i in $(seq "$base" $((base + BATCH - 1))); do
    body+="${MEASUREMENT},host=s${i} value=2 $((1800000000 + i))000000000\n"
  done
  code=$(curl -s -o /dev/null -w "%{http_code}" \
    -X POST "$(api_for "$b")/write?db=${DB}&rp=${RP}&precision=ns" \
    --data-binary "$(printf "%b" "$body")")
  if [[ "$code" != "204" ]]; then
    echo "measure batch $b failed: HTTP $code" >&2
    errors=$((errors + 1))
  fi
done
SERIES="$MEASURE_POINTS"

duration_ms=$(( $(date +%s%3N) - start_ms ))
(( duration_ms > 0 )) || duration_ms=1
duration=$(( duration_ms / 1000 ))
echo "[bench] nodes=$NODES series=$SERIES duration=${duration_ms}ms errors=${errors}"
# Millisecond-based so the result is not rounded to whole-second buckets.
echo "[bench] points_per_sec=$(( SERIES * 1000 / duration_ms ))"

regions=$(curl -sf "${API}/internal/shard/map" 2>/dev/null \
  | grep -o '"region_id"' | wc -l | tr -d ' ' || echo 0)
echo "[bench] regions=${regions} nodes=${NODES}"
if (( regions < NODES )); then
  echo "[bench] WARNING: regions ($regions) < nodes ($NODES). The measurement"
  echo "[bench] cannot occupy every node. A flat curve here is the cardinality"
  echo "[bench] floor, NOT a placement defect. Raise SERIES before concluding."
fi
