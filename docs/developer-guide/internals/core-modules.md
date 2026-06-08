# Core Modules

A module-by-module guide to the HyperbyteDB source code under `src/`. The tree follows strict hexagonal layering: `domain/` → `ports/` → `application/` → `adapters/`.

---

## Root Files

| File | Purpose |
|------|---------|
| `main.rs` | CLI entry point (clap). Subcommands: `serve`, `backup`, `restore`. Initializes tracing, calls `build_services`, starts Axum server, spawns background tasks, handles graceful shutdown. |
| `lib.rs` | Library root. Re-exports: `adapters`, `application`, `bootstrap`, `config`, `domain`, `error`, `timeseriesql`, `ports`. |
| `bootstrap.rs` | Composition root. `build_services()` wires WAL, metadata, chDB, auth, cluster, and all services into `AppState`. Returns `BootstrappedApp`. |
| `config.rs` | `HyperbytedbConfig` and nested structs. Loaded via Figment (TOML + `HYPERBYTEDB__` env vars). |
| `error.rs` | `HyperbytedbError` enum with variants for every error category. `From` impls for common error types. |

---

## `domain/` — Core Data Model

Pure types with no adapter dependencies.

| File / dir | Key Types | Description |
|------------|-----------|-------------|
| `point.rs` | `Point`, `FieldValue` | Core data point: measurement, tags, fields, timestamp (nanoseconds). |
| `series.rs` | `SeriesKey` | Measurement + sorted tags forming a canonical series identity. |
| `database.rs` | `Database`, `RetentionPolicy`, `Precision` | Database definition with retention policies. |
| `measurement.rs` | `MeasurementMeta` | Per-measurement schema: field types, tag keys. |
| `wal.rs` | `WalEntry` | WAL record: database, retention_policy, points, origin_node_id. |
| `query_result.rs` | `QueryResponse`, `StatementResult`, `SeriesResult` | InfluxDB v1-compatible JSON response structure. |
| `column_mapping.rs` | `ColumnMapping`, `tag_column_name` | Tag/field column collision rules shared by query translation and storage. |
| `chdb_naming.rs` | `quoted_table_name`, `tag_column_name`, … | ClickHouse identifier sanitisation and table naming (shared by application + chDB adapter). |
| `user.rs` | `StoredUser` | Username, password hash (Argon2 PHC format), admin flag. |
| `continuous_query.rs` | `ContinuousQueryDef` | CQ metadata. |
| `cluster/` | `NodeState`, `MutationRequest`, `SyncManifest`, … | Cluster domain DTOs and membership types (no HTTP/RocksDB I/O). |

---

## `ports/` — Trait Boundaries

Hexagonal architecture interfaces. Each trait is `Send + Sync` with `#[async_trait]` where async.

| File | Trait / type | Key Methods |
|------|--------------|-------------|
| `ingestion.rs` | `IngestionPort` | `ingest(db, rp, precision, body, format)` |
| `query.rs` | `QueryPort`, `QueryService` | `execute_sql(sql)`, `execute_query(db, query, epoch)` |
| `wal.rs` | `WalPort` | `append`, `read_from`, `read_range`, `truncate_before`, `last_sequence` |
| `metadata.rs` | `MetadataPort` | Database/measurement/tag/field catalog, users, tombstones, CQ CRUD. |
| `auth.rs` | `AuthPort` | `authenticate(username, password)` |
| `points_sink.rs` | `PointsSinkPort` | Native MergeTree INSERT / DDL for flush and retention. |
| `replication.rs` | `ReplicationPort`, `OutboundReplicationBatch` | Outbound write/mutation fan-out to peers. |
| `flush.rs` | `FlushPort` | `drain()` for graceful cluster shutdown. |

---

## `application/` — Business Logic

| File / dir | Key Types | Description |
|------------|-----------|-------------|
| `ingestion_service.rs` | `IngestionServiceImpl` | Single-node ingest: parse → validate metadata → WAL append. |
| `peer_ingestion_service.rs` | `PeerIngestionService` | Cluster ingest: local WAL + `ReplicationPort` fan-out. |
| `line_protocol.rs` | parse/encode helpers | Line protocol parsing and encoding. |
| `msgpack_ingest.rs` | `parse_msgpack_body_to_points` | MessagePack array-of-points ingest. |
| `columnar_msgpack.rs` | `ColumnarMsgpackBatch` | Feature-gated columnar MessagePack ingest. |
| `ingest_metadata.rs` | `IngestCardinalityLimits`, `IngestSchemaCache` | Hot-path schema/tag caching and cardinality enforcement. |
| `query_service.rs` | `QueryServiceImpl` | Full TimeseriesQL execution: parse → dispatch → translate → chDB → format. |
| `peer_query_service.rs` | `PeerQueryService` | Cluster query: schema mutations via Raft or `ReplicationPort`. |
| `flush_service.rs` | `FlushServiceImpl` | Background WAL → chDB flush via `PointsSinkPort`. Implements `FlushPort`. |
| `retention_service.rs` | `RetentionService` | Runs `ALTER TABLE … DELETE` for expired rows. |
| `continuous_query_service.rs` | `ContinuousQueryService` | Background CQ scheduler. |
| `replication_apply.rs` | `ReplicationApplyQueue` | Bounded parallel apply of replicated line protocol to WAL. |
| `replication_dispatch.rs` | `dispatch_outbound_replication` | Async vs sync-quorum replication dispatch via `ReplicationPort`. |
| `statement_summary.rs` | `StatementSummary` | Ring buffer of recent query digests. |
| `cluster/bootstrap.rs` | `ClusterBootstrap` | Cluster init: replication log, membership, peer client, startup sync, Raft. |
| `cluster/drain.rs` | `DrainService` | Graceful drain via `FlushPort` + replication ack wait. |
| `cluster/heartbeat.rs` | `run_heartbeat_updater` | `/ping` probes → membership heartbeat updates. |
| `cluster/sync_manifest.rs` | `build_manifest` | Build metadata + WAL sync manifest from ports. |

---

## `timeseriesql/` — Query Language Engine

| File | Description |
|------|-------------|
| `ast.rs` | Full AST: `Statement`, `SelectStatement`, `Expr`, aggregates, transforms, etc. |
| `parser.rs` | Hand-rolled recursive descent parser (Influx-compatible TimeseriesQL). |
| `to_clickhouse.rs` | AST → ClickHouse SQL against native MergeTree tables. |
| `digest.rs` | Query fingerprinting for statement summary. |

---

## `adapters/` — Infrastructure Implementations

### `adapters/http/`

Axum handlers, router (`AppState`), middleware, auth, internal cluster/Raft endpoints.

### `adapters/chdb/`

| File | Description |
|------|-------------|
| `query_adapter.rs` | `ChdbQueryAdapter` — `QueryPort` via shared `Session`. |
| `native_adapter.rs` | `ChdbNativeAdapter` — `PointsSinkPort`; auto-creates/alters MergeTree tables and INSERTs flush batches. |
| `session.rs` | Shared libchdb session wrapper. |

### `adapters/wal/`

`RocksDbWal` and `BatchingWal` (group-commit decorator).

### `adapters/metadata/`

`RocksDbMetadata` — JSON-serialized metadata in RocksDB.

### `adapters/auth.rs`

`MetadataAuthAdapter` — `AuthPort` via Argon2 + metadata lookup.

### `adapters/cluster/`

| File / dir | Description |
|------------|-------------|
| `peer_client.rs` | Outbound replication HTTP client. Implements `ReplicationPort`. |
| `sync_client.rs` | Join/reconnect sync: metadata + WAL catch-up. |
| `replication_log.rs` | RocksDB-backed per-peer WAL/mutation ack tracking. |
| `hinted_handoff.rs` | Per-peer queued hints for unreachable peers. |
| `transport.rs` | Shared HTTP transport abstraction for cluster I/O. |
| `raft/` | OpenRaft log store, network transport, state machine. |

---

## See Also

- [Architecture](../architecture.md) — High-level design
- [Key Design Decisions](key-design-decisions.md) — Deep dives into subsystems
- [Replication Design](replication-design.md) — Wire format and sync-quorum semantics
