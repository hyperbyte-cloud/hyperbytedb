
|     |
| --- |
|     |


[Documentation](https://docs.hyperbyte.cloud) · [Website](https://hyperbyte.cloud/hyperbytedb)

HyperbyteDB is a time-series database written in Rust that provides InfluxDB v1 API compatibility, uses embedded ClickHouse (chDB) for queries and native MergeTree storage, and RocksDB for WAL/metadata. It supports master-master clustering for replication.

## Key Features

- **InfluxDB v1 API compatible** — Line protocol and HTTP API for Telegraf, Grafana, and other 1.x clients
- **TimeseriesQL** — Time-series analytics query language
- **Columnar MergeTree storage** — Per-measurement `ReplacingMergeTree`
- **WAL** — Durable write-ahead log and metadata
- **Active-active clustering** — Raft for schema consensus; every node accepts writes with async or sync-quorum replication
- **Built-in observability** — Prometheus metrics and structured logs

## Quick Start

### Docker

```bash
docker pull ghcr.io/hyperbyte-cloud/hyperbytedb:latest

docker run -d \
  --name hyperbytedb \
  -p 8086:8086 \
  ghcr.io/hyperbyte-cloud/hyperbytedb:latest
```

### Docker Compose (quick start)

```bash
docker compose -f deploy/compose/docker-compose.getting-started.yml up -d
```

This starts HyperbyteDB, Telegraf, Prometheus, Loki, and Grafana — pre-configured with host-metrics collection and dashboards.

Open Grafana at [http://localhost:3000](http://localhost:3000) (`admin` / `admin`) and check the **HyperbyteDB Cluster** and **Machine Monitoring** dashboards.

For a Compose stack that builds from source (contributors), see [docs/developer-guide/development-setup.md](docs/developer-guide/development-setup.md).

### CLI client

`hyperbytedb-cli` provides an interactive TimeseriesQL shell, batch queries, and write/import over the HTTP API (InfluxDB v1 `influx`-compatible). See [docs/user-guide/cli.md](docs/user-guide/cli.md).

```bash
curl -fsSL https://raw.githubusercontent.com/hyperbyte-cloud/hyperbytedb/main/scripts/install-cli.sh | sudo bash
```

Installs the latest Linux release from [GitHub Releases](https://github.com/hyperbyte-cloud/hyperbytedb/releases) to `/usr/local/bin`. See [cli.md](docs/user-guide/cli.md) for version pinning and user-local installs.

## Documentation


| Doc                                                                                                          | Description                                  |
| ------------------------------------------------------------------------------------------------------------ | -------------------------------------------- |
| [docs/index.md](docs/index.md)                                                                               | Documentation home and navigation            |
| [docs/user-guide/index.md](docs/user-guide/index.md)                                                         | **User guide** (install, configure, operate) |
| [docs/user-guide/configuration.md](docs/user-guide/configuration.md)                                         | Config file and environment variables        |
| [docs/user-guide/administration.md](docs/user-guide/administration.md)                                       | Backups, metrics, cluster ops                |
| [docs/developer-guide/system-architecture.md](docs/developer-guide/system-architecture.md)                   | Internal design overview                     |
| [docs/deep-dive/deep-dive-clustering.md](docs/deep-dive/deep-dive-clustering.md)                             | Replication, Raft, sync APIs                 |
| [docs/developer-guide/internals/replication-design.md](docs/developer-guide/internals/replication-design.md) | Replication wire format and evolution        |
| [docs/developer-guide/index.md](docs/developer-guide/index.md)                                               | Building and contributing                    |
| [docs/engineering/code-review-rubric.md](docs/engineering/code-review-rubric.md)                             | PR review checklist and test ownership       |


