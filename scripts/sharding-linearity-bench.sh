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
    -X POST "${API}/write?db=${DB}&rp=${RP}&precision=ns" \
    --data-binary "$(printf "%b" "$body")")
  if [[ "$code" != "204" ]]; then
    echo "batch $b failed: HTTP $code" >&2
    errors=$((errors + 1))
  fi
  if (( b % 100 == 0 )); then
    echo "  batch $b/$((batches - 1)) (HTTP $code)"
  fi
done

duration=$(( $(date +%s) - start_ts ))
(( duration > 0 )) || duration=1
echo "[bench] nodes=$NODES series=$SERIES duration=${duration}s errors=${errors}"
echo "[bench] points_per_sec=$(( SERIES / duration ))"

regions=$(curl -sf "${API}/internal/shard/map" 2>/dev/null \
  | grep -o '"region_id"' | wc -l | tr -d ' ' || echo 0)
echo "[bench] regions=${regions} nodes=${NODES}"
if (( regions < NODES )); then
  echo "[bench] WARNING: regions ($regions) < nodes ($NODES). The measurement"
  echo "[bench] cannot occupy every node. A flat curve here is the cardinality"
  echo "[bench] floor, NOT a placement defect. Raise SERIES before concluding."
fi
