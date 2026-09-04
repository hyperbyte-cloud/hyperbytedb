#!/usr/bin/env bash
# Bulk ingest 100k distinct series into bench.autogen.metrics (1 point each).
set -euo pipefail

API="${API:-http://localhost:18080}"
DB="${DB:-bench}"
ERRORS=0
START_TS=$(date +%s)

echo "Creating database ${DB}..."
curl -sf -X POST "${API}/query" --data-urlencode "q=CREATE DATABASE ${DB}" >/dev/null

echo "Ingesting 100k series (100 batches x 1000)..."
for batch in $(seq 0 99); do
  start=$((batch * 1000))
  body=""
  for i in $(seq "$start" $((start + 999))); do
    body+="metrics,host=s${i} value=1 $((1700000000 + i))000000000\n"
  done
  code=$(curl -s -o /dev/null -w "%{http_code}" \
    -X POST "${API}/write?db=${DB}&rp=autogen&precision=ns" \
    --data-binary "$(printf "%b" "$body")")
  if [[ "$code" != "204" ]]; then
    echo "batch $batch failed: HTTP $code" >&2
    ERRORS=$((ERRORS + 1))
  fi
  if (( batch % 10 == 0 )); then
    echo "  batch $batch/99 done (HTTP $code)"
  fi
done

END_TS=$(date +%s)
DURATION=$((END_TS - START_TS))
echo "Ingest complete: duration=${DURATION}s errors=${ERRORS}"
