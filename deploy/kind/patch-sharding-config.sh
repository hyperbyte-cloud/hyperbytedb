#!/usr/bin/env bash
# Deprecated: sharding is configured via HyperbytedbCluster.spec.sharding.
# The operator keeps config.toml in sync — no manual ConfigMap patch or
# scaling the operator to 0.
#
# Clean redeploy (delete CR → operator GC → fresh CR):
#   deploy/kind/setup.sh hdb-reset --sharded
#
# To tweak split thresholds, edit deploy/kind/manifests/hyperbytedb-cr-6node-sharded.yaml
# and run: deploy/kind/setup.sh hdb-up --sharded
set -euo pipefail

echo "[patch-sharding-config] DEPRECATED — use HyperbytedbCluster.spec.sharding instead." >&2
echo "[patch-sharding-config] Run: deploy/kind/setup.sh hdb-reset --sharded" >&2
exit 1
