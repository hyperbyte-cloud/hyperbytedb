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

For a Compose stack that builds from source (contributors), see the [Development Setup](https://docs.hyperbyte.cloud/developer-guide/development-setup/) guide.

### CLI client

`hyperbytedb-cli` provides an interactive TimeseriesQL shell, batch queries, and write/import over the HTTP API (InfluxDB v1 `influx`-compatible). See the [CLI guide](https://docs.hyperbyte.cloud/user-guide/cli/).

```bash
curl -fsSL https://raw.githubusercontent.com/hyperbyte-cloud/hyperbytedb/main/scripts/install-cli.sh | sudo bash
```

Installs the latest Linux release from [GitHub Releases](https://github.com/hyperbyte-cloud/hyperbytedb/releases) to `/usr/local/bin`. See the [CLI guide](https://docs.hyperbyte.cloud/user-guide/cli/) for version pinning and user-local installs.

## Documentation


| Doc                                                                                                          | Description                                  |
| ------------------------------------------------------------------------------------------------------------ | -------------------------------------------- |
| [Documentation home](https://docs.hyperbyte.cloud/)                                                          | Documentation home and navigation            |
| [User Guide](https://docs.hyperbyte.cloud/user-guide/)                                                       | Install, configure, and operate              |
| [Configuration](https://docs.hyperbyte.cloud/user-guide/configuration/)                                      | Config file and environment variables        |
| [Administration](https://docs.hyperbyte.cloud/user-guide/administration/)                                    | Backups, metrics, cluster ops                |
| [System Architecture](https://docs.hyperbyte.cloud/developer-guide/system-architecture/)                     | Internal design overview                     |
| [Clustering Deep Dive](https://docs.hyperbyte.cloud/deep-dive/deep-dive-clustering/)                         | Replication, Raft, sync APIs                 |
| [Replication Design](https://docs.hyperbyte.cloud/developer-guide/internals/replication-design/)             | Replication wire format and evolution        |
| [Developer Guide](https://docs.hyperbyte.cloud/developer-guide/)                                             | Building and contributing                    |
| [Code Review Rubric](https://docs.hyperbyte.cloud/engineering/code-review-rubric/)                           | PR review checklist and test ownership       |


