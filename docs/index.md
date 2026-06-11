<div align="center" markdown="0">
<img src="img/hyperbyte-db.png" alt="HyperbyteDB">
</div>

# Documentation

HyperbyteDB is a time-series database written in Rust. It speaks the InfluxDB v1 HTTP API, stores data in embedded chDB MergeTree tables, and uses RocksDB for the write-ahead log and metadata. Clustered deployments use Raft for schema consensus and asynchronous replication for writes.

## Quick start

```bash
docker run -d \
  --name hyperbytedb \
  -p 8086:8086 \
  ghcr.io/hyperbyte-cloud/hyperbytedb:latest

docker exec -it hyperbytedb \
  hyperbytedb-cli create database mydb

docker exec -it hyperbytedb \
  hyperbytedb-cli write -database mydb \
  --data-binary 'cpu,host=srv01 value=42'

  docker exec -it hyperbytedb \
  hyperbytedb-cli query -database mydb \
  --data-urlencode 'q=SELECT * FROM cpu'
```

## User guide

For operators and application developers deploying HyperbyteDB.

| Topic | Description |
|-------|-------------|
| [Installation](user-guide/installation.md) | Docker, Compose, binaries, kind, Kubernetes operator |
| [Configuration](user-guide/configuration.md) | `config.toml` and `HYPERBYTEDB__*` environment variables |
| [Basic operations](user-guide/basic-operations.md) | Databases, writes, queries, retention |
| [Authentication](user-guide/authentication.md) | Credentials, public routes, admin APIs |
| [Advanced features](user-guide/advanced-features.md) | Clustering, continuous queries, TLS, tracing |
| [Common workflows](user-guide/common-workflows.md) | InfluxDB migration, Telegraf, Grafana, monitoring |
| [Administration](user-guide/administration.md) | Metrics, logs, traces, backup, cluster ops |
| [Troubleshooting](user-guide/troubleshooting.md) | Common problems |
| [API reference](user-guide/reference.md) | HTTP endpoints and TimeseriesQL compatibility |
| [Kubernetes operator](user-guide/operator/index.md) | Helm install, CRDs, backups |

Start with the [user guide index](user-guide/index.md) for a recommended reading order.

## Developer guide

For contributors working on the codebase.

| Topic | Description |
|-------|-------------|
| [Architecture](developer-guide/architecture.md) | Hexagonal design and data flow |
| [Development setup](developer-guide/development-setup.md) | Build, run, and debug locally |
| [Testing](developer-guide/testing.md) | Test suites and CI |
| [Contributing](developer-guide/contributing.md) | PR process and review checklist |

See the [developer guide index](developer-guide/index.md) for internals and deep dives.

## Other references

- [Glossary](glossary.md) — shared terminology
- [Benchmarks](benchmarks.md) — Criterion ingestion benchmarks
- [Deep dives](deep-dive/README.md) — write path, read path, clustering, compaction
- **Container image:** `ghcr.io/hyperbyte-cloud/hyperbytedb:latest`
