# V1 Stable Scope & Policy

## Supported Topologies


| Topology             | Method                                               | Status        |
| -------------------- | ---------------------------------------------------- | ------------- |
| Single node          | Docker, Docker Compose, binary tarball, source build | **Supported** |
| Multi-node cluster   | Docker Compose (3-node example)                      | **Supported** |
| Multi-node cluster   | Kubernetes operator (Helm)                           | **Supported** |
| Multi-node cluster   | kind (local dev)                                     | **Supported** |
| macOS (any topology) | Source build only                                    | **Supported** |


A **supported** topology means we will fix bugs and accept patches to keep it working.
It does not mean every deployment size has been tested — see [Resource Sizing](resource-sizing.md) for guidance.

## Availability Model


| Deployment                  | Write semantics                                           | Read semantics               | Notes                                                        |
| --------------------------- | --------------------------------------------------------- | ---------------------------- | ------------------------------------------------------------ |
| Single-node                 | Durable after WAL fsync                                   | Reads from chDB after flush  | Restart required for upgrades; drain before shutdown         |
| Cluster (async replication) | Acknowledged after local WAL append; eventual consistency | Each node reads its own chDB | Peer unreachable → hinted handoff; schema mutations via Raft |
| Cluster (sync_quorum)       | Acknowledged after local WAL append + W-of-N peer acks    | Each node reads its own chDB | Configured via `[cluster.replication] mode = "sync_quorum"`  |


There is no distributed query fan-out. Each node queries its own embedded chDB tables.
Clients should balance reads across nodes or use the hyperbytedb-proxy for HTTP load balancing.

## What We Commit To Fixing

- **Data loss or corruption** — WAL durability, flush correctness, compaction safety
- **Query correctness bugs** — Wrong results, missing data, incorrect aggregations
- **API incompatibility** — Documented InfluxDB v1 endpoints and response shapes
- **Security vulnerabilities** — Auth bypass, privilege escalation, credential disclosure
- **Crash or hang** — Startup failure, panic under normal operation, resource exhaustion
- **Replication divergence** — Data that should be identical across peers diverges without explanation

## Experimental Features

The following are **not** covered by V1 stable guarantees. They may change, break, or be removed
without a major version bump:

- Materialized views (beyond the basic documented syntax)
- Extended InfluxQL functions beyond the [compatibility matrix](reference.md#influxdb-v1-compatibility-matrix)
- Non-S3 backup destinations
- Custom TLS certificate authority integration beyond cert-manager or operator-generated certs
- Metrics label schema (new labels may be added)

## Breaking vs Non-Breaking Changes

### Breaking (major version)

- Removal or rename of a documented HTTP endpoint
- Removal or rename of a config key or environment variable
- Wire format changes (line protocol, MessagePack columnar, replication protocol)
- Storage format changes that require manual migration
- Removal of a CLI subcommand or flag

### Non-breaking (minor / patch)

- Adding a new HTTP endpoint
- Adding a new config key or environment variable
- Adding new metrics or labels
- New features behind feature flags
- Performance improvements
- Bug fixes that do not change documented behavior
- Documentation updates

### Semver policy

HyperbyteDB follows semver for the server binary, Docker images, and Helm chart
appVersion. CRDs follow a separate apiVersion (`v1alpha1` → `v1beta1` → `v1`).


| Artifact           | Version source             | Breaking change rule            |
| ------------------ | -------------------------- | ------------------------------- |
| Server binary      | `Cargo.toml` version field | Major bump = breaking           |
| Docker image tag   | Git tag (`v0.8.4`)         | Major bump = breaking           |
| Helm chart version | Chart.yaml version         | Major bump = breaking           |
| CRD apiVersion     | CRD spec (`v1alpha1`)      | Promoted separately from server |


## How to Escalate

1. **GitHub Issues** — Report bugs at [https://github.com/hyperbyte-cloud/hyperbytedb/issues](https://github.com/hyperbyte-cloud/hyperbytedb/issues)
2. **Security** — Report vulnerabilities privately (see SECURITY.md in the repository)
3. **Support SLA** — Best-effort for community users; defined separately for commercial agreements

Escalations should include: HyperbyteDB version, deployment topology (single-node vs cluster),
libchdb version, and steps to reproduce.

## Supported Exclusions

The following are **outside** V1 stable scope:

- **Kubernetes distros:** Only EKS, AKS, GKE, and kind are tested. Other distributions
(OpenShift, Rancher, DIY Kubernetes) are on a best-effort basis.
- **Object storage:** Only S3-compatible storage is supported for operator backups.
Other backends (GCS, Azure Blob) are not tested.
- **macOS:** Supported for development and testing only. Not recommended for production workloads.
- **Ingest above ~1M points/sec/node:** Deployment-dependent (disk, query load, concurrency). Not a formally tested sizing tier — see [Resource Sizing](resource-sizing.md) for query-driven CPU/RAM guidance.
- **Custom chDB configurations:** The chDB engine is managed internally. Manual queries
via `/api/v1/chdb` are for debugging only and not covered by compatibility guarantees.
- **Third-party plugins:** Telegraf, Grafana, and Chronograf are tested with the documented
InfluxDB v1 API path. Other tools that speak the InfluxDB v1 API may work but are not tested.

