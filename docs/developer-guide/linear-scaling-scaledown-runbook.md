# Linear scaling scale-down runbook

Manual validation of operator decommission + scheduler evacuation on kind.

**Branches:** `hyperbytedb` `feat/linear-scaling-p1`, `hyperbytedb-operator` `feat/decommission-scaledown`.

## Prerequisites

```bash
cd hyperbytedb
./deploy/kind/setup.sh hdb-reset --sharded
./deploy/kind/setup.sh status   # 6/6 pods, CR Healthy
```

## Procedure (6 → 5)

### 1. Seed data

```bash
kubectl --context kind-hyperbytedb port-forward -n hyperbytedb svc/hyperbytedb 18086:8086 &
API=http://localhost:18086

curl -sf -X POST "$API/query" --data-urlencode "q=CREATE DATABASE scaledown"
for i in $(seq 0 4999); do
  curl -sf -X POST "$API/write?db=scaledown&rp=autogen&precision=s" \
    --data-binary "cpu,host=s${i} value=1 $((1700000000 + i))"
done
```

### 2. Baseline

```bash
curl -sf -X POST "$API/query" --data-urlencode "db=scaledown" \
  --data-urlencode "q=SELECT COUNT(value) FROM cpu"
curl -s "$API/internal/shard/map" | jq '.spaces[].regions[] | {region_id, peers, primary}'
```

Every region must have `len(peers) >= 2`.

### 3. Scale down

```bash
kubectl --context kind-hyperbytedb patch hyperbytedbcluster hyperbytedb -n hyperbytedb \
  --type merge -p '{"spec":{"replicas":5}}'
```

### 4. Watch departing node (ordinal 5, node id 6)

```bash
watch -n5 'curl -s http://localhost:18086/health 2>/dev/null | jq .state'   # via pf to pod-5
# Or headless from a debug pod:
# curl -s http://hyperbytedb-5.hyperbytedb-headless.hyperbytedb.svc.cluster.local:8086/health
```

Expected: `decommissioning` → `leaving`.

Operator gates StatefulSet shrink until `leaving`. CR `spec.replicas` becomes 5 immediately (desired); STS stays at 6 until evacuation completes.

### 5. Verify after pod removal

- Point count unchanged
- All regions still RF ≥ 2
- Five running pods

### 6. Restart control

```bash
kubectl --context kind-hyperbytedb delete pod hyperbytedb-2 -n hyperbytedb
```

After pod returns: no mass `RemovePeer`; region memberships for surviving nodes unchanged.

## Automated gate

Requires a **fresh 6-node** cluster. If a prior scale-down left the CR at 5
replicas, reset first or D0 fails immediately:

```bash
./deploy/kind/setup.sh hdb-reset --sharded
./deploy/kind/run-sharding-scaledown-e2e.sh
```

Report: `.scratch/linear-scaling-p1/scaledown-e2e-report.md`

## Pass criteria

- `/internal/decommission` used (not `/internal/drain`) for scale-down
- Departing node reaches `leaving` before pod/PVC removal
- Zero data loss; RF preserved throughout
- Rolling restart does not evacuate the cluster
