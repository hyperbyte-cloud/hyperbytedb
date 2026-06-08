# Deep dives

Subsystem walkthroughs with pointers to source files. When a deep dive disagrees with the code, trust `src/`.

| Document | Topic | Key source files |
|----------|--------|------------------|
| [Write path](deep-dive-write-path.md) | HTTP → WAL → flush → chDB MergeTree | `adapters/http/write.rs`, `application/flush_service.rs`, `adapters/chdb/native_adapter.rs` |
| [Read path](deep-dive-read-path.md) | InfluxQL → ClickHouse SQL → chDB | `timeseriesql/`, `application/query_service.rs`, `adapters/chdb/` |
| [Clustering](deep-dive-clustering.md) | Replication, Raft, sync, hinted handoff | `domain/cluster/`, `application/cluster/`, `adapters/cluster/` |
| [Compaction](deep-dive-compaction.md) | MergeTree background merges | chDB / ClickHouse engine |
| [Self-repair](deep-dive-self-repair.md) | Peer convergence via replication and sync | `adapters/cluster/sync_client.rs`, `replication_log.rs` |

Replication peer sends use exponential backoff starting at 1s, capped at 30s, up to `replication_max_retries` attempts.
