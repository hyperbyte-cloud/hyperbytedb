# Installation

Deploy HyperbyteDB using pre-built images and charts — no compiler required.

| Path | Best for |
|------|----------|
| [Docker](#docker) | Single node, quick evaluation, simple production |
| [Docker Compose](#docker-compose) | Local or small deployments with observability pre-wired |
| [Kubernetes operator](#kubernetes-hyperbytedb-operator) | Production clusters on EKS, AKS, GKE, or self-managed Kubernetes |
| [Linux release tarballs](#linux-release-tarballs-optional) | Bare-metal or VM installs without containers |

For building from source, local kind clusters, and Compose stacks that compile HyperbyteDB, see [Development setup](../developer-guide/development-setup.md).

---

## Supported platforms

| Platform | Docker image | Release tarball |
|----------|:------------:|:---------------:|
| Linux x86_64 | yes | yes |
| Linux aarch64 (ARM) | yes | yes |
| macOS | — | — |

Docker images are multi-arch manifests — `docker pull ghcr.io/hyperbyte-cloud/hyperbytedb:latest` selects `linux/amd64` or `linux/arm64` automatically. Pre-built server tarballs and `hyperbytedb-cli` install scripts target **Linux only**. macOS is supported for development builds only; see [Development setup](../developer-guide/development-setup.md).

---

## Docker

The fastest way to run a single HyperbyteDB node. Images are published to GitHub Container Registry (`ghcr.io/hyperbyte-cloud/hyperbytedb`).

```bash
docker pull ghcr.io/hyperbyte-cloud/hyperbytedb:latest

docker run -d \
  --name hyperbytedb \
  -p 8086:8086 \
  -v hyperbytedb-data:/var/lib/hyperbytedb \
  -e HYPERBYTEDB__SERVER__BIND_ADDRESS=0.0.0.0 \
  -e HYPERBYTEDB__SERVER__PORT=8086 \
  ghcr.io/hyperbyte-cloud/hyperbytedb:latest
```

Verify the server is up:

```bash
curl -sSf http://localhost:8086/health
# {"status":"pass","message":"ready for queries and writes"}
```

Data is stored under `/var/lib/hyperbytedb` inside the container. Mount a volume (as above) to persist across restarts.

> **Note:** `HYPERBYTEDB__SERVER__BIND_ADDRESS=0.0.0.0` is required so the process accepts connections from outside the container.

The image includes `hyperbytedb-cli`:

```bash
docker exec -it hyperbytedb hyperbytedb-cli -host http://127.0.0.1:8086 ping
```

To install the CLI on the host instead, see [CLI (hyperbytedb-cli)](cli.md).

---

## Docker Compose

### Getting started stack

The getting-started compose file starts HyperbyteDB plus Telegraf, Prometheus, Loki, and Grafana from **pre-built images** — no local compilation.

```bash
docker compose -f deploy/compose/docker-compose.getting-started.yml up -d
```

Host metrics flow into HyperbyteDB via Telegraf. Prometheus scrapes `/metrics`. Grafana ships with pre-provisioned dashboards.

Open Grafana at http://localhost:3000 (`admin` / `admin`).

| Service | Port | Description |
|---------|------|-------------|
| HyperbyteDB | 8086 | Time-series database |
| Grafana | 3000 | Dashboards |
| Prometheus | 9090 | Metrics |
| Loki | 3100 | Logs |

### Smoke test

```bash
# Create a database
curl -sS -XPOST 'http://localhost:8086/query' --data-urlencode 'q=CREATE DATABASE mydb'

# Write a point
curl -sS -XPOST 'http://localhost:8086/write?db=mydb' \
  --data-binary 'cpu,host=server01,region=us-west usage_idle=95.2,usage_user=4.8'

# Wait for flush (~10s), then query
curl -sS -G 'http://localhost:8086/query' \
  --data-urlencode 'db=mydb' \
  --data-urlencode 'q=SELECT * FROM cpu'
```

### Multi-node cluster (Compose)

For a three-node active-active cluster on Docker networks, run one service per node with unique `HYPERBYTEDB__CLUSTER__NODE_ID` and peer lists. Example pattern:

```yaml
services:
  db1:
    image: ghcr.io/hyperbyte-cloud/hyperbytedb:latest
    hostname: db1
    ports: ["8086:8086"]
    volumes: [db1-data:/var/lib/hyperbytedb]
    environment:
      HYPERBYTEDB__SERVER__BIND_ADDRESS: "0.0.0.0"
      HYPERBYTEDB__CLUSTER__ENABLED: "true"
      HYPERBYTEDB__CLUSTER__NODE_ID: "1"
      HYPERBYTEDB__CLUSTER__CLUSTER_ADDR: "db1:8086"
      HYPERBYTEDB__CLUSTER__PEERS: "db2:8086,db3:8086"
      HYPERBYTEDB__CLUSTER__REPLICATION_LOG_DIR: "/var/lib/hyperbytedb/replication_log"
    networks: [cluster]

  db2:
    image: ghcr.io/hyperbyte-cloud/hyperbytedb:latest
    hostname: db2
    ports: ["8087:8086"]
    volumes: [db2-data:/var/lib/hyperbytedb]
    environment:
      HYPERBYTEDB__SERVER__BIND_ADDRESS: "0.0.0.0"
      HYPERBYTEDB__CLUSTER__ENABLED: "true"
      HYPERBYTEDB__CLUSTER__NODE_ID: "2"
      HYPERBYTEDB__CLUSTER__CLUSTER_ADDR: "db2:8086"
      HYPERBYTEDB__CLUSTER__PEERS: "db1:8086,db3:8086"
      HYPERBYTEDB__CLUSTER__REPLICATION_LOG_DIR: "/var/lib/hyperbytedb/replication_log"
    networks: [cluster]

  db3:
    image: ghcr.io/hyperbyte-cloud/hyperbytedb:latest
    hostname: db3
    ports: ["8088:8086"]
    volumes: [db3-data:/var/lib/hyperbytedb]
    environment:
      HYPERBYTEDB__SERVER__BIND_ADDRESS: "0.0.0.0"
      HYPERBYTEDB__CLUSTER__ENABLED: "true"
      HYPERBYTEDB__CLUSTER__NODE_ID: "3"
      HYPERBYTEDB__CLUSTER__CLUSTER_ADDR: "db3:8086"
      HYPERBYTEDB__CLUSTER__PEERS: "db1:8086,db2:8086"
      HYPERBYTEDB__CLUSTER__REPLICATION_LOG_DIR: "/var/lib/hyperbytedb/replication_log"
    networks: [cluster]

volumes:
  db1-data:
  db2-data:
  db3-data:

networks:
  cluster:
    driver: bridge
```

Write to any node; data replicates to peers asynchronously by default. See [Advanced features](advanced-features.md) for sync-quorum mode and [Administration](administration.md) for cluster operations.

---

## Kubernetes (HyperbyteDB operator)

For production on Kubernetes, install the [HyperbyteDB operator](operator/index.md). It manages `HyperbytedbCluster`, `HyperbytedbBackup`, and `HyperbytedbRestore` custom resources and reconciles StatefulSets, Services, TLS, and optional monitoring.

| Doc | What it covers |
|-----|----------------|
| [Operator overview](operator/index.md) | Custom resources, lifecycle phases, capabilities |
| [Operator installation (Helm)](operator/installation.md) | OCI chart install, upgrade, uninstall, raw YAML |
| [HyperbytedbCluster](operator/cluster.md) | CRD fields, examples, TLS, autoscaling |
| [Backup and restore (operator)](operator/backup-restore.md) | S3 backups and restores via custom resources |
| [hyperbytedb-proxy](operator/hyperbytedb-proxy.md) | Optional health-aware HTTP proxy in front of the database Service |

Tested Kubernetes distros include EKS, AKS, GKE, and kind (kind is for local development — see [Development setup](../developer-guide/development-setup.md)).

---

## Linux release tarballs (optional)

Each `v*` GitHub Release includes self-contained Linux tarballs (server + `libchdb.so`, synced with the Docker image):

- `hyperbytedb-vX.Y.Z-linux-x86_64.tar.gz`
- `hyperbytedb-vX.Y.Z-linux-aarch64.tar.gz`

```bash
arch=$(uname -m)              # x86_64 or aarch64
tag=vX.Y.Z                    # match the GitHub release you want
url="https://github.com/hyperbyte-cloud/hyperbytedb/releases/download/${tag}/hyperbytedb-${tag}-linux-${arch}.tar.gz"

curl -fsSL "$url" -o hyperbytedb.tar.gz
sudo tar -xzf hyperbytedb.tar.gz -C /usr/local/bin hyperbytedb
sudo tar -xzf hyperbytedb.tar.gz -C /usr/local/lib libchdb.so
sudo ldconfig

hyperbytedb serve
```

A matching `*.sha256` file is attached to every release for integrity verification. Downloads are at [github.com/hyperbyte-cloud/hyperbytedb/releases](https://github.com/hyperbyte-cloud/hyperbytedb/releases).

---

## Next Steps

- [Configuration](configuration.md) — Tune settings for your workload
- [Basic operations](basic-operations.md) — Start writing and querying data
- [CLI (hyperbytedb-cli)](cli.md) — Interactive shell and batch commands
- [Resource sizing](resource-sizing.md) — CPU, memory, and disk guidelines
