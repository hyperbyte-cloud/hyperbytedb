# Kubernetes Operator

The HyperbyteDB Operator is a Kubernetes operator that automates deploying and managing HyperbyteDB database clusters. It extends the Kubernetes API with three custom resources:

| Custom Resource | Purpose |
|-----------------|---------|
| **HyperbytedbCluster** | Declares a HyperbyteDB cluster (single-node or multi-replica) with storage, networking, TLS, monitoring, autoscaling, and failover configuration |
| **HyperbytedbBackup** | Defines one-shot or scheduled S3 backups of a cluster with configurable retention |
| **HyperbytedbRestore** | Restores a cluster from a HyperbytedbBackup snapshot |

## How It Works

The operator watches for these custom resources and reconciles the underlying Kubernetes objects (StatefulSets, Services, PVCs, ConfigMaps, Secrets) to match the desired state. Key capabilities include:

- **Rolling upgrades** -- Change the `version` field and the operator upgrades pods one at a time, waiting for each to become healthy before proceeding.
- **Automatic TLS** -- Enable `server.tls.enabled` and the operator generates a self-signed CA and per-node certificates, or integrates with cert-manager.
- **Cluster topology** -- For multi-replica clusters, the operator configures Raft consensus, peer discovery, heartbeat, and anti-entropy settings automatically from the StatefulSet ordinals.
- **Monitoring integration** -- Optionally creates ServiceMonitor resources for Prometheus and Grafana dashboard ConfigMaps.
- **Autoscaling** -- Configures HorizontalPodAutoscaler based on CPU utilization targets.
- **Failover** -- Detects unhealthy members and triggers automatic recovery within configurable thresholds.
- **Pause/resume** -- Set `spec.paused: true` to freeze reconciliation for manual maintenance windows.

## Cluster Lifecycle Phases

The operator tracks cluster state through these phases:

| Phase | Description |
|-------|-------------|
| `Pending` | Resource created, reconciliation not yet started |
| `Initializing` | StatefulSet and services being created |
| `Running` | All replicas healthy and serving traffic |
| `Scaling` | Replica count change in progress |
| `Upgrading` | Rolling version upgrade in progress |
| `Failed` | Reconciliation encountered an unrecoverable error |

## Next Steps

- [Installation](installation.md) -- Install the operator via Helm
- [HyperbytedbCluster](cluster.md) -- Full CRD reference and example configurations
- [Backup & Restore](backup-restore.md) -- Set up automated backups and perform restores
- [hyperbytedb-proxy](hyperbytedb-proxy.md) -- Optional health-aware HTTP reverse proxy in front of the database Service
