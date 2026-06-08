# Backup & Restore

The operator provides two custom resources for data protection: `HyperbytedbBackup` for creating backups and `HyperbytedbRestore` for restoring from them. Both target S3-compatible storage.

---

## HyperbytedbBackup

### One-Shot Backup

Create a single immediate backup:

```yaml
apiVersion: hyperbytedb.hyperbyte.cloud/v1alpha1
kind: HyperbytedbBackup
metadata:
  name: hyperbytedb-manual-backup
  namespace: default
spec:
  clusterName: hyperbytedb-cluster
  destination:
    s3:
      bucket: hyperbytedb-backups
      prefix: "manual/"
      region: us-east-1
      credentialsSecretName: s3-backup-credentials
  retentionDays: 30
  backupType: full
```

### Scheduled Backup

Use a cron expression to run backups on a schedule:

```yaml
apiVersion: hyperbytedb.hyperbyte.cloud/v1alpha1
kind: HyperbytedbBackup
metadata:
  name: hyperbytedb-daily-backup
  namespace: default
spec:
  clusterName: hyperbytedb-cluster
  schedule: "0 2 * * *"
  destination:
    s3:
      bucket: hyperbytedb-backups
      prefix: "daily/"
      region: us-east-1
      credentialsSecretName: s3-backup-credentials
  retentionDays: 7
  backupType: full
```

This runs a full backup every day at 02:00 UTC and retains backups for 7 days.

### Selective Database Backup

Back up specific databases instead of the entire cluster:

```yaml
spec:
  clusterName: hyperbytedb-cluster
  databases:
    - production_metrics
    - application_logs
  destination:
    s3:
      bucket: hyperbytedb-backups
      prefix: "selective/"
      region: us-east-1
      credentialsSecretName: s3-backup-credentials
```

### S3 Credentials Secret

The credentials Secret must contain `access_key_id` and `secret_access_key` keys:

```yaml
apiVersion: v1
kind: Secret
metadata:
  name: s3-backup-credentials
  namespace: default
type: Opaque
stringData:
  access_key_id: "AKIAIOSFODNN7EXAMPLE"
  secret_access_key: "wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY"
```

### Spec Reference

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| `clusterName` | string | | Name of the HyperbytedbCluster to back up (required, min length: 1) |
| `schedule` | string | | Cron expression for scheduled backups. Omit for a one-shot backup |
| `destination.s3.bucket` | string | | S3 bucket name (required) |
| `destination.s3.prefix` | string | | Key prefix within the bucket |
| `destination.s3.region` | string | | AWS region |
| `destination.s3.endpoint` | string | | Custom S3-compatible endpoint (e.g., MinIO) |
| `destination.s3.credentialsSecretName` | string | | Secret with S3 credentials |
| `retentionDays` | int32 | `7` | Days to retain backup artifacts before cleanup (min: 1) |
| `databases` | list | | Restrict backup to specific databases. Empty means all databases |
| `backupType` | string | `full` | Backup type: `full` or `incremental` |

### Status

| Field | Type | Description |
|-------|------|-------------|
| `phase` | string | `Pending`, `Running`, `Completed`, `Failed` |
| `startTime` | Time | When the backup started |
| `completionTime` | Time | When the backup finished |
| `backupSize` | string | Human-readable size (e.g., `1.2 GiB`) |
| `backupPath` | string | S3 path where the backup was stored |
| `lastCleanupTime` | Time | Last time expired backups were cleaned up |
| `conditions` | list | Standard Kubernetes conditions |

Monitor backup progress:

```bash
kubectl get hyperbytedbbackup hyperbytedb-daily-backup -o wide
```

```
NAME                    CLUSTER            SCHEDULE    PHASE       SIZE       AGE
hyperbytedb-daily-backup   hyperbytedb-cluster   0 2 * * *  Completed   1.2 GiB   1d
```

---

## HyperbytedbRestore

### Restore from a Backup

The simplest restore references a HyperbytedbBackup by name. The operator resolves the S3 location from the backup's status:

```yaml
apiVersion: hyperbytedb.hyperbyte.cloud/v1alpha1
kind: HyperbytedbRestore
metadata:
  name: hyperbytedb-restore-latest
  namespace: default
spec:
  clusterName: hyperbytedb-cluster
  backupName: hyperbytedb-daily-backup
```

### Restore from a Specific S3 Path

Override the source location to restore from a specific backup snapshot:

```yaml
apiVersion: hyperbytedb.hyperbyte.cloud/v1alpha1
kind: HyperbytedbRestore
metadata:
  name: hyperbytedb-restore-specific
  namespace: default
spec:
  clusterName: hyperbytedb-cluster
  backupName: hyperbytedb-daily-backup
  source:
    s3:
      bucket: hyperbytedb-backups
      prefix: "daily/20260301-020000/"
      region: us-east-1
      credentialsSecretName: s3-backup-credentials
```

### Restore Workflow

When a HyperbytedbRestore resource is created, the operator performs these steps:

1. **ScalingDown** -- Scales the target cluster's StatefulSet to 0 replicas
2. **Restoring** -- Downloads the backup from S3 and writes it to each PVC
3. **ScalingUp** -- Scales the StatefulSet back to the original replica count
4. **Completed** -- All replicas are healthy and serving traffic

!!! warning
    A restore **overwrites** all data in the target cluster. The cluster is unavailable during the restore process. Plan accordingly for production restores.

### Spec Reference

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| `clusterName` | string | | Name of the HyperbytedbCluster to restore into (required, min length: 1) |
| `backupName` | string | | Name of the HyperbytedbBackup to restore from (required, min length: 1) |
| `source` | object | | Override the backup source location (optional; resolved from the backup CR if omitted) |
| `source.s3.bucket` | string | | S3 bucket name |
| `source.s3.prefix` | string | | Key prefix for the specific backup snapshot |
| `source.s3.region` | string | | AWS region |
| `source.s3.endpoint` | string | | Custom S3-compatible endpoint |
| `source.s3.credentialsSecretName` | string | | Secret with S3 credentials |
| `restoreTimestamp` | Time | | RFC 3339 timestamp for point-in-time restore (reserved for future use) |

### Status

| Field | Type | Description |
|-------|------|-------------|
| `phase` | string | `Pending`, `ScalingDown`, `Restoring`, `ScalingUp`, `Completed`, `Failed` |
| `startTime` | Time | When the restore started |
| `completionTime` | Time | When the restore finished |
| `restoredPVCs` | int32 | Number of PVCs restored |
| `conditions` | list | Standard Kubernetes conditions |

Monitor restore progress:

```bash
kubectl get hyperbytedbrestore hyperbytedb-restore-latest -o wide
```

```
NAME                      CLUSTER            BACKUP                  PHASE       AGE
hyperbytedb-restore-latest   hyperbytedb-cluster   hyperbytedb-daily-backup   Completed   5m
```

---

## See Also

- [HyperbytedbCluster](cluster.md) -- Cluster configuration reference
- [Administration](../administration.md) -- Manual backup/restore via the CLI
