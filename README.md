<div align="center">
  <table>
    <tr>
      <td align="center" bgcolor="#000000">
        <img src="docs/img/hyperbytedb_white.png" alt="HyperbyteDB" width="450">
      </td>
    </tr>
  </table>
<p align="center">
  <a href="https://docs.hyperbyte.cloud">Documentation</a> · <a href="https://hyperbyte.cloud/hyperbytedb">Website</a>
</p>
</div>



HyperbyteDB is a time-series database written in Rust that provides InfluxDB v1 API compatibility, uses embedded ClickHouse (chDB) for queries and native MergeTree storage, and RocksDB for WAL/metadata. It supports master-master clustering for replication.

## Key Features

- **InfluxDB v1 API compatible** — Line protocol and HTTP API for Telegraf, Grafana, and other 1.x clients
- **TimeseriesQL** — Time-series analytics query language
- **Embedded chDB engine** — ClickHouse in-process datastore
- **Columnar MergeTree storage** — Per-measurement `ReplacingMergeTree`
- **RocksDB WAL** — Durable write-ahead log and metadata
- **Active-active clustering** — Raft for schema consensus; every node accepts writes with async or sync-quorum replication
- **Built-in observability** — Prometheus metrics, structured logs, and OTLP trace export

## Supported Platforms

| Platform                    | Docker image | Release tarball | Build from source |
|-----------------------------|:------------:|:---------------:|:-----------------:|
| Linux x86_64                | yes          | yes             | yes               |
| Linux aarch64 (ARM)         | yes          | yes             | yes               |
| macOS arm64 (Apple Silicon) | —            | —               | yes               |
| macOS x86_64                | —            | —               | yes               |

Docker images at `ghcr.io/hyperbyte-cloud/hyperbytedb` are multi-arch manifests, so `docker pull` automatically resolves to `linux/amd64` or `linux/arm64`. Each `v*` GitHub Release also ships standalone `linux-x86_64` and `linux-aarch64` tarballs (binaries + matching `libchdb.so`, plus `sha256` files).

## Quick Start

### Pre-built Docker image (GHCR)

For Docker, Compose, kind, and the Kubernetes operator, see [docs/user-guide/installation.md](docs/user-guide/installation.md).

### Docker Compose

```bash
docker compose up --build -d
```

This starts:

- **HyperbyteDB** on port 8086 (JSON logs + OTLP traces)
- **Alloy** on port 4318 (log shipping and trace collection)
- **Loki** on port 3100
- **Tempo** on ports 3200 and 4317
- **Telegraf** (host metrics into HyperbyteDB)
- **Prometheus** on port 9090
- **Grafana** on port 3000 (pre-provisioned dashboards)

Open Grafana at http://localhost:3000 (`admin` / `admin`) to validate metrics, logs, and traces end-to-end.

### CLI client

`hyperbytedb-cli` provides an interactive TimeseriesQL shell, batch queries, and write/import over the HTTP API (InfluxDB v1 `influx`-compatible). See [docs/user-guide/cli.md](docs/user-guide/cli.md).

```bash
cargo build --release -p hyperbytedb-cli
./target/release/hyperbytedb-cli -host http://localhost:8086 -execute 'SHOW DATABASES'
```

### Manual Build

1. **Prerequisites**: Rust (latest stable), libchdb
2. **Install libchdb**:
  ```bash
   curl -sL https://lib.chdb.io | bash
  ```
3. **Build**:
  ```bash
   cargo build --release
  ```
4. **Run**:
  ```bash
   ./target/release/hyperbytedb serve
  ```

## Configuration

HyperbyteDB is configured via `config.toml` or environment variables. Environment variables use the `HYPERBYTEDB__` prefix with double underscores separating sections and keys (e.g., `HYPERBYTEDB__SERVER__PORT=9090`). See [docs/user-guide/configuration.md](docs/user-guide/configuration.md) for the full reference.

## Architecture

```
┌─────────────────────────────────────────────────────────────────────────┐
│                         HTTP Layer (axum)                               │
│              /write  /query  /ping  /health  /metrics                   │
└─────────────────────────────────────────────────────────────────────────┘
                                     │
                                     ▼
┌─────────────────────────────────────────────────────────────────────────┐
│                      Application Services                               │
│         Write Service │ Query Service │ Auth │ Flush │ Retention        │
└─────────────────────────────────────────────────────────────────────────┘
                                     │
                                     ▼
┌─────────────────────────────────────────────────────────────────────────┐
│                          Ports (traits)                                 │
│         WalPort │ QueryPort │ MetadataPort │ PointsSinkPort             │
└─────────────────────────────────────────────────────────────────────────┘
                                     │
                                     ▼
┌─────────────────────────────────────────────────────────────────────────┐
│                         Adapters                                        │
│       RocksDB (WAL, metadata) │ chDB (queries + native MergeTree)       │
└─────────────────────────────────────────────────────────────────────────┘
```

### Data Flow

**Write path**: Line protocol → parse → RocksDB WAL → background flush → chDB `INSERT` into MergeTree tables

**Query path**: TimeseriesQL → transpile to ClickHouse SQL → chDB `SELECT` from native tables

## Supported TimeseriesQL

- **SELECT** with aggregates: `mean`, `median`, `count`, `sum`, `min`, `max`, `first`, `last`, `percentile`, `spread`, `stddev`, `mode`, `distinct`
- **Transforms**: `derivative`, `non_negative_derivative`, `difference`, `moving_average`, `cumulative_sum`, `elapsed`
- **GROUP BY**: `time()` + tags
- **Fill modes**: `null`, `none`, `previous`, `linear`, `0`
- **Regex measurements** (e.g., `/^cpu.*/`)
- **Subqueries**
- **Arithmetic expressions**

See [docs/user-guide/reference.md#influxdb-v1-compatibility-matrix](docs/user-guide/reference.md#influxdb-v1-compatibility-matrix) for the compatibility matrix.

If the server crashes with `std::bad_function_call` on startup, see [docs/user-guide/troubleshooting.md](docs/user-guide/troubleshooting.md).

## Documentation


| Doc                                                                                                          | Description                                                                                                                                                                                                |
| ------------------------------------------------------------------------------------------------------------ | ---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| [docs/index.md](docs/index.md)                                                                               | Documentation home and navigation                                                                                                                                                                          |
| [docs/user-guide/index.md](docs/user-guide/index.md)                                                         | **User guide** (install, configure, operate)                                                                                                                                                               |
| [docs/user-guide/configuration.md](docs/user-guide/configuration.md)                                         | Config file and environment variables                                                                                                                                                                      |
| [docs/user-guide/administration.md](docs/user-guide/administration.md)                                       | Backups, metrics, cluster ops |
| [docs/developer-guide/system-architecture.md](docs/developer-guide/system-architecture.md)                   | Internal design overview                                                                                                                                                                                   |
| [docs/deep-dive/deep-dive-clustering.md](docs/deep-dive/deep-dive-clustering.md)                             | Replication, Raft, sync APIs                                                                                                                                                                               |
| [docs/developer-guide/internals/replication-design.md](docs/developer-guide/internals/replication-design.md) | Replication wire format and evolution                                                                                                                                                                      |
| [docs/developer-guide/index.md](docs/developer-guide/index.md)                                               | Building and contributing                                                                                                                                                                                  |
| [docs/engineering/code-review-rubric.md](docs/engineering/code-review-rubric.md)                             | PR review checklist and test ownership                                                                                                                                                                     |

## License

This project is licensed under the Apache License 2.0 - see the [LICENSE](LICENSE) file for details.