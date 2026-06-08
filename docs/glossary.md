# Glossary

Shared terminology used across HyperbyteDB documentation.

---

| Term | Definition |
|------|------------|
| **Anti-entropy** | Deprecated. The `cluster.anti_entropy_*` config keys have no effect. |
| **Arrow** | Apache Arrow — in-memory columnar format used by optional columnar MessagePack ingest (`columnar-ingest` feature). |
| **Batching WAL** | Optional decorator over the RocksDB WAL that groups multiple append operations into a single write batch (group commit) for higher throughput. |
| **Cardinality** | The number of unique values for a tag key or the number of unique measurements. High cardinality can degrade performance. |
| **chDB** | Embedded ClickHouse — query engine and native storage backend via `chdb-rust`. Data lives in per-measurement `ReplacingMergeTree` tables under `chdb.session_data_path`. |
| **Column Family (CF)** | A RocksDB concept. HyperbyteDB uses separate column families for WAL entries (`wal`), WAL metadata (`wal_meta`), general metadata (`metadata`), and replication state. |
| **Composition Root** | `bootstrap.rs` — the single location where all adapters and services are wired together using dependency injection via `Arc<dyn Trait>`. |
| **Continuous Query (CQ)** | A named query that runs automatically on a schedule, typically used for downsampling raw data into summary measurements. |
| **Drain** | Graceful node removal procedure: stop accepting writes, flush WAL, wait for replication acks, notify peers, shut down. |
| **Field** | A key-value pair in a data point that holds the actual measured value. Fields are not indexed. Field types are enforced after first write (Float, Integer, UInteger, String, Boolean). |
| **Figment** | Configuration loading library used by HyperbyteDB. Merges defaults, TOML config file, and environment variables. |
| **Flush** | Background process that reads WAL entries and writes them into chDB MergeTree tables via `ChdbNativeAdapter`. Runs every `flush.interval_secs`. |
| **Hexagonal Architecture** | Design pattern where business logic depends only on abstract port traits, with concrete adapters plugged in at the composition root. Also called ports and adapters. |
| **Hinted Handoff** | Mechanism that stores writes destined for unreachable peers in a local queue and replays them when the peer recovers. |
| **InfluxQL** | Query language compatible with InfluxDB 1.x. Supports SELECT with aggregates/transforms, SHOW commands, DDL, DELETE, and continuous queries. |
| **Line Protocol** | InfluxDB's text-based wire format for writing time-series data: `measurement,tag=val field=val timestamp`. |
| **Master-Master Replication** | Clustering model where every node independently accepts writes and replicates them to all peers asynchronously. |
| **Measurement** | Analogous to a table in a relational database. Contains a set of tag keys and field keys. |
| **Merkle Tree** | Not used in the current cluster sync model. Peers align via WAL replication and metadata/WAL sync. |
| **Metadata** | Database definitions, measurement schemas (field types, tag keys), user accounts, tombstones, and CQ definitions. Stored in RocksDB. |
| **OpenRaft** | Rust implementation of the Raft consensus protocol. Used by HyperbyteDB for schema mutation ordering in cluster mode. |
| **Point** | A single data observation: measurement name, tag set, field set, and timestamp (nanoseconds since Unix epoch). |
| **Port** | An abstract trait in the hexagonal architecture that defines a boundary between business logic and infrastructure (e.g., `WalPort`, `QueryPort`, `PointsSinkPort`). |
| **Precision** | Timestamp unit for line protocol writes: `ns` (nanoseconds), `us`/`u` (microseconds), `ms` (milliseconds), `s` (seconds). |
| **Raft** | Consensus algorithm used for schema mutations in cluster mode. Ensures all nodes apply CREATE/DROP/DELETE operations in the same order. |
| **RecordBatch** | Arrow's in-memory columnar data container. Used by optional columnar MessagePack ingest. |
| **Replication Log** | RocksDB-backed store tracking WAL and mutation acknowledgements from peers. Used for safe WAL truncation in cluster mode. |
| **Retention Policy (RP)** | Configuration that controls how long data is kept. Each database has one or more RPs. The default RP is `autogen` with infinite duration. |
| **RocksDB** | Embedded key-value store. Used for the WAL (durable write log), metadata store, replication log, and Raft state. |
| **Series** | A unique combination of measurement name and tag set. Each series has its own time-ordered sequence of field values. |
| **Statement Summary** | Ring buffer tracking recently executed InfluxQL statements with query digest, latency, and error status. Exposed via `GET /api/v1/statements`. |
| **Tag** | A key-value pair in a data point used for indexing and grouping. Tags are always strings. Stored in metadata for SHOW TAG queries. |
| **Tombstone** | A metadata record created by DELETE statements. Marks data for exclusion at query time. |
| **WAL (Write-Ahead Log)** | Durable, ordered log where incoming writes are persisted before the client receives a response. Data in the WAL is flushed to chDB by the background flush service. |
