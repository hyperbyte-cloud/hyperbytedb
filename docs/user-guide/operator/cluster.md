# HyperbytedbCluster

The `HyperbytedbCluster` custom resource declares a HyperbyteDB database deployment. The operator reconciles this into a StatefulSet, headless Service, client Service, PVCs, ConfigMaps, and optional monitoring resources.

---

## Quick Start

Deploy the operator first, then apply one of the examples below.

### Single Node

A minimal single-node deployment for development or testing:

```bash
kubectl apply -f https://raw.githubusercontent.com/hyperbyte-cloud/hyperbytedb/main/deploy/examples/single-node.yaml
```

### Three-Node Cluster

A production-ready cluster with replication, monitoring, and failover.
Requires at least 3 worker nodes or a `topologySpreadConstraints` override.

```bash
kubectl apply -f https://raw.githubusercontent.com/hyperbyte-cloud/hyperbytedb/main/deploy/examples/three-node.yaml
```

### High-Availability with Autoscaling

A full-featured deployment with sync-quorum replication, autoscaling, and zone-aware scheduling:

```yaml
apiVersion: hyperbytedb.hyperbyte.cloud/v1alpha1
kind: HyperbytedbCluster
metadata:
  name: hyperbytedb-ha
  namespace: default
spec:
  replicas: 5
  image: hyperbytedb:latest
  version: "0.8.3"
  server:
    port: 8086
    requestTimeoutSecs: 60
    queryTimeoutSecs: 60
  storage:
    volumeClaimTemplate:
      size: 50Gi
      storageClassName: fast-ssd
  flush:
    intervalSecs: 5
    walSizeThresholdMb: 128
    timeBucketDuration: "1h"
  chdb:
    sessionDataPath: /var/lib/hyperbytedb/chdb
  auth:
    enabled: true
    credentialsSecretName: hyperbytedb-auth
  logging:
    level: info
    format: json
  cardinality:
    maxTagValuesPerMeasurement: 100000
    maxMeasurementsPerDatabase: 10000
  statementSummary:
    enabled: true
    maxEntries: 1000
  hintedHandoff:
    enabled: true
    maxHintsPerPeer: 100000
    maxHintAgeSecs: 3600
  rateLimit:
    enabled: true
    maxRequestsPerSecond: 1000
  retention:
    enabled: true
    interval: 5m
  resources:
    requests:
      cpu: "2"
      memory: 4Gi
    limits:
      cpu: "4"
      memory: 8Gi
  cluster:
    heartbeatIntervalSecs: 1
    heartbeatMissThreshold: 3
    replicationMaxRetries: 10
    raftHeartbeatIntervalMs: 200
    raftElectionTimeoutMs: 800
    raftSnapshotThreshold: 500
    replication:
      mode: sync_quorum
      ackTimeoutMs: 5000
      syncQuorum:
        minAcks: majority
  monitoring:
    enabled: true
    serviceMonitor: true
  failover:
    enabled: true
    maxFailoverCount: 2
    failoverTimeoutSecs: 180
  autoscaling:
    enabled: true
    minReplicas: 3
    maxReplicas: 10
    targetCPUUtilizationPercentage: 70
  topologySpreadConstraints:
    - maxSkew: 1
      topologyKey: topology.kubernetes.io/zone
      whenUnsatisfiable: DoNotSchedule
      labelSelector:
        matchLabels:
          app.kubernetes.io/name: hyperbytedb
          app.kubernetes.io/instance: hyperbytedb-ha
  tolerations:
    - key: dedicated
      operator: Equal
      value: hyperbytedb
      effect: NoSchedule
```

### TLS-Enabled Cluster

Enable TLS with operator-managed self-signed certificates:

```yaml
apiVersion: hyperbytedb.hyperbyte.cloud/v1alpha1
kind: HyperbytedbCluster
metadata:
  name: hyperbytedb-tls
  namespace: default
spec:
  replicas: 3
  image: hyperbytedb:latest
  version: "0.8.3"
  server:
    port: 8086
    tls:
      enabled: true
      # Omit secretName to let the operator generate a self-signed certificate.
      # Provide secretName to use a pre-existing TLS Secret:
      # secretName: hyperbytedb-tls-cert
  storage:
    volumeClaimTemplate:
      size: 10Gi
  resources:
    requests:
      cpu: 500m
      memory: 1Gi
    limits:
      cpu: "2"
      memory: 4Gi
  failover:
    enabled: true
  monitoring:
    enabled: true
    serviceMonitor: true
```

To use cert-manager instead of self-signed certificates:

```yaml
server:
  tls:
    enabled: true
    certManagerIssuerRef:
      name: letsencrypt-prod
      kind: ClusterIssuer
```

---

## Spec Reference

### Top-Level Fields

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| `replicas` | int32 | `1` | Number of cluster members (min: 1) |
| `image` | string | `hyperbytedb:latest` | Container image for HyperbyteDB |
| `version` | string | | Version tag for upgrade orchestration; changing this triggers a rolling upgrade |
| `imagePullPolicy` | string | | Kubernetes image pull policy (`Always`, `IfNotPresent`, `Never`) |
| `imagePullSecrets` | list | | References to Secrets for pulling from private registries |
| `paused` | bool | `false` | When true, the operator skips reconciliation for manual maintenance |
| `resources` | ResourceRequirements | | CPU and memory requests/limits for each pod |
| `podAnnotations` | map | | Additional annotations applied to each pod |
| `podLabels` | map | | Additional labels applied to each pod |
| `affinity` | Affinity | | Kubernetes pod affinity/anti-affinity rules |
| `topologySpreadConstraints` | list | | Constraints for spreading pods across topology domains |
| `tolerations` | list | | Kubernetes tolerations for tainted nodes |
| `additionalVolumes` | list | | Extra volumes to mount in pods |
| `additionalVolumeMounts` | list | | Mount points for additional volumes |

### `server`

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| `port` | int32 | `8086` | HTTP API port (1--65535) |
| `maxBodySizeBytes` | int64 | `26214400` | Maximum request body size (25 MiB) |
| `requestTimeoutSecs` | int32 | `30` | HTTP request timeout |
| `queryTimeoutSecs` | int32 | `30` | Query execution timeout |
| `maxConcurrentQueries` | int32 | `0` | Cap on concurrent `/query` requests (`0` = unlimited) |
| `tls` | object | | TLS configuration (see below) |

### `server.tls`

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| `enabled` | bool | | Enable TLS for the HTTP API |
| `secretName` | string | | Name of a `kubernetes.io/tls` Secret. Omit to let the operator generate self-signed certs |
| `certManagerIssuerRef` | object | | Reference to a cert-manager Issuer or ClusterIssuer (`name`, `kind`, optional `group`) |

### `storage`

HyperbyteDB stores WAL, metadata, Raft state, and chDB session data on the per-replica PVC. The operator mounts fixed paths under `/var/lib/hyperbytedb/` and writes them into `config.toml`.

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| `volumeClaimTemplate.storageClassName` | string | | StorageClass for dynamically provisioned PVCs |
| `volumeClaimTemplate.size` | Quantity | `10Gi` | PVC size per replica |

### `flush`

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| `intervalSecs` | int32 | `10` | How often the WAL is flushed to chDB |
| `walSizeThresholdMb` | int32 | `64` | WAL size threshold that triggers an early flush |
| `timeBucketDuration` | string | `1h` | Parquet time-bucket width (`1h` or `1d`) |
| `maxPointsPerBatch` | int32 | `50000` | Max points per chDB insert batch (written to ConfigMap as `max_points_per_batch`) |
| `walBatchSize` | int32 | `64` | WAL group-commit batch size (`0` disables) |
| `walBatchDelayUs` | int64 | `200` | WAL group-commit delay in microseconds |

### `chdb`

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| `sessionDataPath` | string | | chDB session data directory (embedded MergeTree storage) |
| `poolSize` | int32 | `1` | Number of chDB connections to the same session data path. Clamped to 1–32. Set `server.maxConcurrentQueries` ≥ `poolSize` for best overlap. |

### `auth`

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| `enabled` | bool | `false` | Enable authentication for the HTTP API |
| `credentialsSecretName` | string | | Secret containing auth credentials |

### `logging`

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| `level` | string | `info` | Log level: `trace`, `debug`, `info`, `warn`, `error` |
| `format` | string | `text` | Log format: `text` or `json` |


### `cardinality`

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| `maxTagValuesPerMeasurement` | int64 | `100000` | Max distinct tag values per measurement |
| `maxMeasurementsPerDatabase` | int64 | `10000` | Max measurements per database |

### `statementSummary`

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| `enabled` | bool | `true` | Collect per-statement stats for `/debug/statement_summary` |
| `maxEntries` | int32 | `1000` | Max distinct statements tracked |

### `hintedHandoff`

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| `enabled` | bool | `true` | Retry writes against temporarily unreachable peers |
| `maxHintsPerPeer` | int64 | `100000` | Max queued hints per peer before eviction |
| `maxHintAgeSecs` | int64 | `3600` | Drop hints older than this many seconds |

### `rateLimit`

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| `enabled` | bool | `false` | Enable per-endpoint rate limiting |
| `maxRequestsPerSecond` | int64 | `0` | Max requests per second per endpoint (`0` = unlimited) |

### `retention`

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| `enabled` | bool | `true` | Run the background retention enforcement loop |
| `interval` | string | `12h` | How often retention scans run (humantime duration) |

### `cluster`

Tuning parameters for multi-replica cluster behavior. These only take effect when `replicas > 1`.

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| `heartbeatIntervalSecs` | int32 | `2` | Interval between peer heartbeats |
| `heartbeatMissThreshold` | int32 | `5` | Missed heartbeats before marking a peer unhealthy |
| `replicationMaxRetries` | int32 | `5` | Max retries for write replication |
| `replicationQueueDepth` | int32 | `8192` | Bounded outbound replication queue depth |
| `replicationMaxInflightBatches` | int32 | `8` | Max concurrent outbound replication fan-out rounds |
| `replicationMaxCoalesceBodyBytes` | int64 | `8388608` | Max bytes for coalescing consecutive WAL batches |
| `replicateReceiverQueueDepth` | int32 | `1024` | Bounded apply queue on the replicate receiver |
| `replicationTruncateStalePeerMultiplier` | int64 | `2` | Omit stale peers from WAL truncate barrier |
| `raftHeartbeatIntervalMs` | int32 | `300` | Raft leader heartbeat interval |
| `raftElectionTimeoutMs` | int32 | `1000` | Raft election timeout |
| `raftSnapshotThreshold` | int32 | `1000` | Log entries before Raft snapshot |
| `replication.mode` | string | `async` | `async` or `sync_quorum` |
| `replication.ackTimeoutMs` | int64 | `5000` | Latency budget for `sync_quorum` writes |
| `replication.syncQuorum.minAcks` | int or string | | Peer acks required: integer or `"majority"` |
| `tls` | object | | TLS for inter-node replication traffic (same schema as `server.tls`) |

In cluster mode, the Raft leader compares `/internal/sync/manifest` responses from peers and triggers sync when needed. See [Deep Dive: Clustering](../../deep-dive/deep-dive-clustering.md).

### `monitoring`

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| `enabled` | bool | `true` | Expose Prometheus metrics |
| `serviceMonitor` | bool | `true` | Create a Prometheus ServiceMonitor resource |

### `autoscaling`

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| `enabled` | bool | | Enable HorizontalPodAutoscaler |
| `minReplicas` | int32 | | Minimum replica count (min: 1) |
| `maxReplicas` | int32 | | Maximum replica count (required, min: 1) |
| `targetCPUUtilizationPercentage` | int32 | `80` | Target average CPU utilization |

### `failover`

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| `enabled` | bool | `true` | Enable automatic failover |
| `maxFailoverCount` | int32 | `1` | Maximum simultaneous failovers (min: 1) |
| `failoverTimeoutSecs` | int32 | `300` | Seconds a member must be unhealthy before failover (min: 60) |

---

## Status

The operator maintains a `.status` subresource with the following fields:

| Field | Type | Description |
|-------|------|-------------|
| `phase` | string | Current lifecycle phase: `Pending`, `Initializing`, `Running`, `Scaling`, `Upgrading`, `Failed` |
| `replicas` | int32 | Desired replica count |
| `readyReplicas` | int32 | Number of replicas passing readiness checks |
| `clusterState` | string | High-level health: `Healthy`, `Degraded`, `Recovering`, `Unknown` |
| `replicationState` | string | Replication convergence: `Healthy`, `Lagging`, `Diverged`, `Unknown` |
| `members` | list | Per-member status (name, nodeID, podName, state, health, WAL sequence, peer count) |
| `failoverCount` | int32 | Number of failovers in the current generation |
| `configHash` | string | Hash of the current `config.toml` (used for rolling update detection) |
| `conditions` | list | Standard Kubernetes conditions |

Check cluster status:

```bash
kubectl get hyperbytedbcluster hyperbytedb-cluster -o wide
```

```
NAME               REPLICAS   READY   PHASE     CLUSTER   AGE
hyperbytedb-cluster   3          3       Running   Healthy   5m
```

Inspect detailed status:

```bash
kubectl get hyperbytedbcluster hyperbytedb-cluster -o jsonpath='{.status}' | jq
```

---

## Operations

### Rolling Upgrade

Change the `version` field to trigger a rolling upgrade:

```bash
kubectl patch hyperbytedbcluster hyperbytedb-cluster \
  --type merge -p '{"spec":{"version":"1.1.0"}}'
```

The operator upgrades one pod at a time, waiting for readiness before proceeding. The cluster phase transitions to `Upgrading` during the process.

### Scaling

```bash
kubectl scale hyperbytedbcluster hyperbytedb-cluster --replicas=5
```

Or patch the spec directly:

```bash
kubectl patch hyperbytedbcluster hyperbytedb-cluster \
  --type merge -p '{"spec":{"replicas":5}}'
```

### Pause Reconciliation

```bash
kubectl patch hyperbytedbcluster hyperbytedb-cluster \
  --type merge -p '{"spec":{"paused":true}}'
```

Resume with `"paused":false`.

---

## See Also

- [Installation](installation.md) -- Installing the operator
- [Backup & Restore](backup-restore.md) -- Automated backups and restores
- [Administration](../administration.md) -- HyperbyteDB operational guide (monitoring, backups, cluster ops)
