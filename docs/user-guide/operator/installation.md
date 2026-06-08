# Installation

Install the HyperbyteDB operator into a Kubernetes cluster with Helm.

---

## Prerequisites

| Requirement | Minimum Version |
|-------------|-----------------|
| Kubernetes | 1.26+ |
| Helm | 3.12+ |
| kubectl | 1.26+ |

Optional:

- **cert-manager** -- Required only if you want to use cert-manager-issued TLS certificates instead of operator-generated self-signed certs.
- **Prometheus Operator** -- Required if you enable `monitoring.serviceMonitor` to have the operator create ServiceMonitor resources.

---

## Install via Helm (OCI Registry)

The operator chart is published to GitHub Packages as an OCI artifact.

### 1. Create a namespace

```bash
kubectl create namespace hyperbytedb-system
```

### 2. Install the chart

Install the latest release:

```bash
helm install hyperbytedb-operator \
  oci://ghcr.io/hyperbyte-cloud/hyperbytedb-operator \
  --namespace hyperbytedb-system
```

Install a specific **Helm chart** version. The value passed to `--version` is the **operator chart** release (not necessarily the `hyperbytedb` server crate version in this repo’s `Cargo.toml`). Use the tag published to `oci://ghcr.io/hyperbyte-cloud/hyperbytedb-operator` for your target release.

```bash
helm install hyperbytedb-operator \
  oci://ghcr.io/hyperbyte-cloud/hyperbytedb-operator \
  --version 0.6.0 \
  --namespace hyperbytedb-system
```

Replace `0.6.0` with the chart version you intend to deploy.

### 3. Verify the installation

```bash
kubectl get pods -n hyperbytedb-system
```

You should see the operator controller manager pod running:

```
NAME                                                  READY   STATUS    RESTARTS   AGE
hyperbytedb-operator-controller-manager-xxxxx-yyyyy      1/1     Running   0          30s
```

Verify the CRDs are installed:

```bash
kubectl get crds | grep hyperbytedb
```

Expected output:

```
hyperbytedbbackups.hyperbytedb.hyperbyte.cloud     2026-01-01T00:00:00Z
hyperbytedbclusters.hyperbytedb.hyperbyte.cloud    2026-01-01T00:00:00Z
hyperbytedbrestores.hyperbytedb.hyperbyte.cloud    2026-01-01T00:00:00Z
```

---

## Customizing the Installation

Override default values with `--set` or a values file:

```bash
helm install hyperbytedb-operator \
  oci://ghcr.io/hyperbyte-cloud/hyperbytedb-operator \
  --namespace hyperbytedb-system \
  --values custom-values.yaml
```

See the chart's `values.yaml` for all available configuration options. Key settings include:

| Value | Default | Description |
|-------|---------|-------------|
| `controllerManager.manager.image.repository` | `controller` | Operator container image |
| `controllerManager.manager.image.tag` | Chart appVersion | Image tag |
| `controllerManager.manager.resources.limits.cpu` | `500m` | CPU limit |
| `controllerManager.manager.resources.limits.memory` | `128Mi` | Memory limit |
| `controllerManager.manager.resources.requests.cpu` | `10m` | CPU request |
| `controllerManager.manager.resources.requests.memory` | `64Mi` | Memory request |
| `controllerManager.replicas` | `1` | Number of operator replicas |

---

## Upgrade

```bash
helm upgrade hyperbytedb-operator \
  oci://ghcr.io/hyperbyte-cloud/hyperbytedb-operator \
  --namespace hyperbytedb-system \
  --version <new-version>
```

---

## Uninstall

!!! warning
    Uninstalling the operator does **not** delete HyperbytedbCluster, HyperbytedbBackup, or HyperbytedbRestore resources. The database StatefulSets and PVCs remain intact. To fully clean up, delete all custom resources before uninstalling the operator.

```bash
# Delete all custom resources first
kubectl delete hyperbytedbclusters,hyperbytedbbackups,hyperbytedbrestores --all -A

# Uninstall the operator
helm uninstall hyperbytedb-operator --namespace hyperbytedb-system

# Optionally remove the namespace
kubectl delete namespace hyperbytedb-system
```

---

## Alternative: YAML Bundle

If you prefer not to use Helm, you can install the operator with a single YAML manifest:

```bash
kubectl apply -f https://raw.githubusercontent.com/hyperbyte-cloud/hyperbytedb-operator/main/dist/install.yaml
```

This installs the CRDs, RBAC, and controller manager deployment. Uninstall with:

```bash
kubectl delete -f https://raw.githubusercontent.com/hyperbyte-cloud/hyperbytedb-operator/main/dist/install.yaml
```

---

## Next Steps

- [HyperbytedbCluster](cluster.md) -- Deploy your first HyperbyteDB cluster
- [Backup & Restore](backup-restore.md) -- Configure automated backups
