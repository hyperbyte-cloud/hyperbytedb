# Architecture

HyperbyteDB uses embedded chDB MergeTree tables as the sole time-series storage backend. The source tree follows strict hexagonal layering under `domain/`, `ports/`, `application/`, and `adapters/`.

HyperbyteDB is a time-series database that combines RocksDB (WAL/metadata) and embedded ClickHouse (chDB) for queries and native storage.

---

## System Overview

```mermaid
graph TD
    Client["Client (Telegraf, Grafana, curl)"]
    HTTP["HTTP Layer (Axum)<br>/write /query /ping /health /metrics"]
    AppSvc["Application Services<br>Ingestion | Query | Flush | Retention | CQ"]
    Ports["Port Traits<br>WalPort | QueryPort | MetadataPort<br>PointsSinkPort | ReplicationPort | FlushPort"]
    RocksDB["RocksDB<br>(WAL + Metadata)"]
    ChDB["chDB<br>(MergeTree storage + queries)"]

    Client --> HTTP
    HTTP --> AppSvc
    AppSvc --> Ports
    Ports --> RocksDB
    Ports --> ChDB
```

**RocksDB** provides the WAL (durable, ordered write log) and metadata store (databases, measurements, schemas, users, tombstones, CQ definitions).

**chDB** (embedded ClickHouse) is both the query engine and the storage backend. Influx-compatible TimeseriesQL is transpiled to ClickHouse SQL; flushed WAL batches are inserted into per-measurement `ReplacingMergeTree` tables under `chdb.session_data_path`.

---

## Hexagonal Architecture (Ports and Adapters)

HyperbyteDB uses the hexagonal pattern. Business logic depends only on port traits, never on concrete implementations.

```mermaid
graph TB
    subgraph Domain["Domain Layer"]
        Point["Point, FieldValue, WalEntry"]
        DB["Database, RetentionPolicy"]
        ClusterDTO["cluster/ membership, sync DTOs, wire types"]
        Naming["chdb_naming"]
    end

    subgraph App["Application Services"]
        Ingest["IngestionService"]
        Query["QueryService"]
        Flush["FlushService"]
        ClusterApp["cluster/ bootstrap, drain, sync_manifest"]
    end

    subgraph PortLayer["Port Traits"]
        WalPort["WalPort"]
        QueryPort["QueryPort"]
        MetaPort["MetadataPort"]
        SinkPort["PointsSinkPort"]
        ReplPort["ReplicationPort"]
        FlushPort["FlushPort"]
    end

    subgraph InboundAdapters["Inbound Adapters"]
        HTTPHandlers["HTTP Handlers"]
        PeerHandlers["Peer Handlers"]
    end

    subgraph OutboundAdapters["Outbound Adapters"]
        RocksWAL["RocksDB WAL"]
        RocksMeta["RocksDB Metadata"]
        ChDBAdapter["chDB query + native sink"]
        ClusterIO["cluster/ peer_client, sync_client, raft"]
    end

    InboundAdapters --> App
    App --> PortLayer
    PortLayer --> OutboundAdapters
    App --> Domain
```

This means:
- Swapping RocksDB for another WAL requires only implementing `WalPort`.
- Swapping chDB for another SQL engine requires only implementing `QueryPort` and `PointsSinkPort`.
- Cluster outbound I/O is isolated in `adapters/cluster/`; application services use `ReplicationPort` and `FlushPort` rather than concrete HTTP clients.
- The HTTP layer can be replaced without touching business logic.

---

## Data Flow

### Write Path

```mermaid
sequenceDiagram
    participant C as Client
    participant H as HTTP Handler
    participant I as IngestionService
    participant M as Metadata
    participant W as WAL (RocksDB)
    participant F as FlushService
    participant S as chDB Native Adapter

    C->>H: POST /write?db=mydb
    H->>I: ingest(db, rp, precision, body)
    I->>I: Parse line protocol → Vec<Point>
    I->>M: Register field types, tag keys
    I->>M: Check cardinality limits
    I->>W: append(WalEntry)
    W-->>I: sequence number
    I-->>H: Ok
    H-->>C: 204 No Content

    Note over F: Background (every flush.interval_secs)
    F->>W: read_range(cursor, 5000)
    F->>F: Group by (db, rp, measurement)
    F->>S: INSERT batch into MergeTree tables
    F->>W: truncate_before(seq)
```

In cluster mode, `PeerIngestionService` fans out replicated line protocol via `ReplicationPort` after the local WAL append.

### Read Path

```mermaid
sequenceDiagram
    participant C as Client
    participant H as HTTP Handler
    participant Q as QueryService
    participant P as TimeseriesQL Parser
    participant T as Translator
    participant M as Metadata
    participant Ch as chDB

    C->>H: GET /query?db=mydb&q=SELECT...
    H->>Q: execute_query(db, q, epoch)
    Q->>P: parse(timeseriesql_string)
    P-->>Q: Vec<Statement>
    Q->>M: metadata lookups (SHOW/DDL) or measurement schema (SELECT)
    Q->>T: translate(AST, table names, tombstones)
    T-->>Q: ClickHouse SQL
    Q->>Ch: execute_sql(sql) [spawn_blocking]
    Ch-->>Q: JSONEachRow results
    Q->>Q: Parse → SeriesResult[]
    Q-->>H: QueryResponse
    H-->>C: JSON response
```

---

## Component Summary

| Component | Location | Purpose |
|-----------|----------|---------|
| **CLI / Main** | `src/main.rs` | Entry point, clap CLI, server lifecycle, graceful shutdown |
| **Bootstrap** | `src/bootstrap.rs` | Composition root: wires all adapters and services |
| **Config** | `src/config.rs` | Figment-based config loading (TOML + env vars) |
| **Domain** | `src/domain/` | Pure types: Point, Database, WalEntry, cluster DTOs, `chdb_naming` |
| **Ports** | `src/ports/` | Trait boundaries: WAL, metadata, query, ingestion, auth, replication, flush |
| **Application** | `src/application/` | Business logic: ingestion, query, flush, retention, replication apply |
| **Application / cluster** | `src/application/cluster/` | Cluster orchestration: bootstrap, drain, heartbeat, sync manifest builder |
| **TimeseriesQL** | `src/timeseriesql/` | Influx-compatible parser, AST, ClickHouse translator |
| **HTTP Adapters** | `src/adapters/http/` | Axum handlers, router, middleware, auth |
| **chDB Adapters** | `src/adapters/chdb/` | Query adapter, native flush adapter (`PointsSinkPort`), shared session |
| **Cluster Adapters** | `src/adapters/cluster/` | Peer client, sync client, replication log, hinted handoff, Raft I/O |

---

## Key Design Patterns

| Pattern | Where Used |
|---------|------------|
| **Ports and adapters** | All business logic depends on port traits, not concrete implementations |
| **Composition root** | `bootstrap::build_services` wires everything together |
| **Decorator / wrapper** | `BatchingWal` wraps `RocksDbWal` for group commit |
| **Strategy** | `IngestionServiceImpl` vs `PeerIngestionService` |
| **Facade** | `QueryServiceImpl` over parser + transpiler + metadata + chDB |
| **Worker pool + channel** | `ReplicationApplyQueue`, `PeerClient` outbound loop |
| **Cache** | Ingest schema cache, metadata measurement cache, auth verification cache |
| **Consensus** | OpenRaft for schema/coordination only; HTTP transport |

---

## See Also

- [Core Modules](internals/core-modules.md) — Detailed source code walkthrough
- [Key Design Decisions](internals/key-design-decisions.md) — Deep dives into subsystems
- [Extension Points](internals/extension-points.md) — How to add new functionality
