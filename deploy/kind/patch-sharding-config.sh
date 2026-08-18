#!/usr/bin/env bash
# Patch hyperbytedb-config with sharding settings for Kind split/perf tests.
# The HyperbytedbCluster CRD does not yet expose a sharding spec; patch the
# operator-generated ConfigMap so [sharding].enabled survives without scaling
# the operator to 0 permanently.
#
# When using low split thresholds (e.g. region_split_series=5), you must also set
# region_merge_series < region_split_series or hyperbytedb fails config validation.
set -euo pipefail

KUBE_CTX="${KUBE_CTX:-kind-hyperbytedb}"
NS="${NS:-hyperbytedb}"
# Low thresholds for split testing; override via env for perf runs.
REGION_SPLIT_SERIES="${REGION_SPLIT_SERIES:-5}"
REGION_MAX_SERIES="${REGION_MAX_SERIES:-10}"
REGION_MERGE_SERIES="${REGION_MERGE_SERIES:-2}"

kubectl --context "$KUBE_CTX" -n "$NS" scale deployment/hyperbytedb-operator --replicas=0 2>/dev/null || true

kubectl --context "$KUBE_CTX" -n "$NS" get configmap hyperbytedb-config -o yaml > /tmp/hbd-config.yaml
export REGION_SPLIT_SERIES REGION_MAX_SERIES REGION_MERGE_SERIES
python3 << PY
import os
import yaml

region_split = int(os.environ["REGION_SPLIT_SERIES"])
region_max = int(os.environ["REGION_MAX_SERIES"])
region_merge = int(os.environ["REGION_MERGE_SERIES"])

with open("/tmp/hbd-config.yaml") as f:
    cm = yaml.safe_load(f)

toml = cm["data"]["config.toml"]
sharding_block = f"""
[sharding]
enabled = true
replication_factor = 2
region_split_series = {region_split}
region_max_series = {region_max}
region_merge_series = {region_merge}
split_merge_interval_secs = 30
schedule_limit = 4
heartbeat_interval_secs = 10
load_split_qps_threshold = 0
bootstrap_timeout_ms = 5000
"""

if "[sharding]" in toml:
    start = toml.index("[sharding]")
    end = toml.find("\n[", start + 1)
    if end == -1:
        toml = toml[:start] + sharding_block.strip() + "\n"
    else:
        toml = toml[:start] + sharding_block.strip() + "\n" + toml[end + 1 :]
else:
    toml = toml.rstrip() + "\n" + sharding_block

cm["data"]["config.toml"] = toml
with open("/tmp/hbd-config.yaml", "w") as f:
    yaml.dump(cm, f, default_flow_style=False)
print("sharding config patched")
PY

kubectl --context "$KUBE_CTX" apply -f /tmp/hbd-config.yaml
kubectl --context "$KUBE_CTX" -n "$NS" rollout restart statefulset/hyperbytedb
kubectl --context "$KUBE_CTX" -n "$NS" wait --for=condition=ready pod -l app.kubernetes.io/name=hyperbytedb --timeout=600s

echo "Sharding enabled; waiting for Raft leader..."
for i in $(seq 1 60); do
  if kubectl --context "$KUBE_CTX" -n "$NS" port-forward pod/hyperbytedb-0 18080:8086 >/tmp/pf-leader.log 2>&1 & then
    PF_PID=$!
    sleep 2
    if curl -sf http://localhost:18080/cluster/leader | grep -q current_leader; then
      kill "$PF_PID" 2>/dev/null || true
      echo "Raft leader elected"
      exit 0
    fi
    kill "$PF_PID" 2>/dev/null || true
  fi
  sleep 5
done
echo "Warning: could not confirm Raft leader within timeout"
