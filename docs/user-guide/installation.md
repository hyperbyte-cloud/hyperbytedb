# Installation

Deploy HyperbyteDB with Docker images, Docker Compose, release tarballs, a source build, or Kubernetes ([kind](#kubernetes-kind) for local dev, [operator](operator/index.md) for production).

## Supported platforms

| Platform           | Docker image | Release tarball | Build from source |
|--------------------|:------------:|:---------------:|:-----------------:|
| Linux x86_64       | yes          | yes             | yes               |
| Linux aarch64 (ARM)| yes          | yes             | yes               |
| macOS arm64 (Apple Silicon) | —   | —               | yes               |
| macOS x86_64       | —            | —               | yes               |

The Docker images are multi-arch manifests, so `docker pull ghcr.io/hyperbyte-cloud/hyperbytedb:latest` automatically picks `linux/amd64` or `linux/arm64` based on the host. macOS is supported as a from-source target only — pre-built macOS binaries are not currently published.

---

## Pre-built Docker Image (Recommended)

The fastest way to run HyperbyteDB. Images are published to GitHub Container Registry as a multi-arch manifest covering `linux/amd64` and `linux/arm64`.

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

To force a specific architecture (e.g. when running emulated builds for testing):

```bash
docker pull --platform=linux/arm64 ghcr.io/hyperbyte-cloud/hyperbytedb:latest
```

Verify it started:

```bash
curl -sSf http://localhost:8086/health
# {"status":"pass","message":"ready for queries and writes"}
```

The container stores all data under `/var/lib/hyperbytedb`. Mount a volume to persist data across container restarts.

> **Note:** `HYPERBYTEDB__SERVER__BIND_ADDRESS=0.0.0.0` is required so the process accepts connections from outside the container.

---

## Docker Compose (Full Stack)

The included `docker-compose.yml` starts HyperbyteDB with a full local observability stack. Config files live under `deploy/compose/`.

```bash
git clone https://github.com/hyperbyte-cloud/hyperbytedb.git
cd hyperbytedb
docker compose up --build -d
```

| Service | Port | Description |
|---------|------|-------------|
| HyperbyteDB | 8086 | Time-series database (JSON logs + OTLP traces enabled) |
| Alloy | 4318 | Docker log shipping → Loki; OTLP ingress → Tempo |
| Loki | 3100 | Log aggregation |
| Tempo | 3200 | Distributed tracing (OTLP from HyperbyteDB) |
| Telegraf | — | Collects host metrics and writes to HyperbyteDB |
| Prometheus | 9090 | Scrapes HyperbyteDB `/metrics` |
| Grafana | 3000 | Pre-provisioned dashboards (login: `admin`/`admin`) |

Validate observability end-to-end:

1. Open Grafana at http://localhost:3000 (admin/admin).
2. **Metrics** — dashboard *HyperbyteDB Cluster* or Prometheus → `hyperbytedb_*` counters.
3. **Logs** — Explore → Loki, query `{container=~".*hyperbytedb.*"}` or dashboard *HyperbyteDB Logs (Compose)*.
4. **Traces** — Explore → Tempo, search `service.name=hyperbytedb` after running writes/queries below.
5. **Statement summary** — `curl -s http://localhost:8086/api/v1/statements | jq .` (enabled in compose).

Quick smoke test:

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

---

## Pre-built Binary Tarballs

For each `v*` tag, CI publishes a GitHub Release with self-contained tarballs for Linux:

- `hyperbytedb-vX.Y.Z-linux-x86_64.tar.gz`
- `hyperbytedb-vX.Y.Z-linux-aarch64.tar.gz`

Each tarball contains the `hyperbytedb` binary plus the matching `libchdb.so` (extracted from the same Docker image that gets published to GHCR, so they're guaranteed to be in sync).

```bash
# Pick the right arch
arch=$(uname -m)              # x86_64 or aarch64
tag=vX.Y.Z                    # match the GitHub release you want
url="https://github.com/hyperbyte-cloud/hyperbytedb/releases/download/${tag}/hyperbytedb-${tag}-linux-${arch}.tar.gz"

curl -fsSL "$url" -o hyperbytedb.tar.gz
sudo tar -xzf hyperbytedb.tar.gz -C /usr/local/bin hyperbytedb
sudo tar -xzf hyperbytedb.tar.gz -C /usr/local/lib  libchdb.so
sudo ldconfig

hyperbytedb serve
```

A matching `*.sha256` file is attached to every release for integrity verification.

---

## Building from Source

### Prerequisites

| Requirement | Details |
|-------------|---------|
| **Rust** | Latest stable toolchain (`rustup update stable`) |
| **libchdb** | Embedded ClickHouse library (`https://lib.chdb.io` ships builds for Linux x86_64/aarch64 and macOS x86_64/arm64) |
| **System packages** | `clang`, `llvm-dev`, `libclang-dev`, `pkg-config`, `libssl-dev` |
| **Platforms** | Linux x86_64, Linux aarch64, macOS x86_64, macOS arm64 |

### Install system dependencies

```bash
# Debian/Ubuntu (works for both amd64 and arm64)
sudo apt-get update && sudo apt-get install -y \
  clang llvm-dev libclang-dev pkg-config libssl-dev build-essential

# Fedora/RHEL
sudo dnf install -y clang llvm-devel clang-devel pkgconfig openssl-devel

# macOS (Homebrew) — needed for both Apple Silicon and Intel
brew install llvm pkg-config openssl@3
export LIBCLANG_PATH="$(brew --prefix llvm)/lib"
```

### Install libchdb

```bash
curl -sL https://lib.chdb.io | bash
# Linux only — refresh the dynamic linker cache after the install
sudo ldconfig 2>/dev/null || true
```

The installer auto-detects your platform and pulls the right artifact (Linux x86_64/aarch64, macOS x86_64/arm64). It places `libchdb.so` in `/usr/local/lib/` and `chdb.h` in `/usr/local/include/`.

> **macOS note:** the chdb installer writes `libchdb.so` even on macOS (not `.dylib`). The Rust crate links against it by name, so leave the file as-is. If `cargo build` later complains about `libchdb` not being found at runtime, ensure `/usr/local/lib` is on `DYLD_LIBRARY_PATH` (Apple Silicon Homebrew users sometimes need this).

Verify:

```bash
ls /usr/local/lib/libchdb.so
ls /usr/local/include/chdb.h
```

If the build links successfully but the binary exits with `libchdb.so: cannot open shared object file`, the library may be present under `/usr/local/lib` but not in the system loader’s search path. Add that directory to the linker configuration and run `ldconfig` again; see the **libchdb.so** entry in [Troubleshooting](troubleshooting.md).

### Build

```bash
# Debug build (faster compilation, slower runtime)
cargo build

# Release build (optimized, recommended for production)
cargo build --release
```

The release build uses LTO, single codegen unit, and strip for maximum performance and minimum binary size.

### Run

```bash
# Start with default config (./config.toml)
./target/release/hyperbytedb serve

# Start with a custom config file
./target/release/hyperbytedb -c /etc/hyperbytedb/config.toml serve
```

### CLI Commands

| Command | Description |
|---------|-------------|
| `hyperbytedb serve` | Start the HTTP server |
| `hyperbytedb backup --output <path>` | Create a full backup |
| `hyperbytedb restore --input <path>` | Restore from a backup |

| Flag | Default | Description |
|------|---------|-------------|
| `-c`, `--config` | `config.toml` | Path to TOML config file |

---

## Docker Compose Cluster (3-Node)

For a clustered deployment with Docker Compose, create a compose file with three HyperbyteDB services. Each node needs a unique `NODE_ID` and must list the other nodes as peers:

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

```bash
docker compose up -d
```

Write to any node; all nodes see the same data after replication.

---

## Kubernetes (kind)

The `deploy/kind/` directory contains a setup script and manifests for a local Kubernetes cluster using kind:

```bash
./deploy/kind/setup.sh
```

This creates a kind cluster with:
- HyperbyteDB operator and a multi-node `HyperbytedbCluster` CR
- Observability stack (Prometheus, Grafana, Loki, Tempo, Telegraf)
- NodePort services mapped to localhost ports

For a single-node stack with full observability, use the root `docker-compose.yml` and configs under `deploy/compose/`.

See [Administration](administration.md) for cluster operations.

---

## Kubernetes (HyperbyteDB operator)

For production-style deployments on a **real Kubernetes cluster**, use the [HyperbyteDB Kubernetes operator](operator/index.md). It extends the API with `HyperbytedbCluster`, `HyperbytedbBackup`, and `HyperbytedbRestore`, and reconciles StatefulSets, services, and optional monitoring resources.

| Doc | What it covers |
|-----|----------------|
| [Operator overview](operator/index.md) | Custom resources, lifecycle phases, capabilities |
| [Operator installation (Helm)](operator/installation.md) | OCI chart install, upgrade, uninstall, raw YAML |
| [HyperbytedbCluster](operator/cluster.md) | CRD fields, examples, TLS, autoscaling |
| [Backup and restore (operator)](operator/backup-restore.md) | S3 backups and restores via custom resources |
| [hyperbytedb-proxy](operator/hyperbytedb-proxy.md) | Optional health-aware HTTP proxy in front of the database Service |

The [kind-based setup](#kubernetes-kind) above is aimed at local development. Treat the operator path as the supported way to run managed HyperbyteDB on Kubernetes in production.

---

## Next Steps

- [Configuration](configuration.md) — Tune settings for your workload
- [Basic operations](basic-operations.md) — Start writing and querying data
