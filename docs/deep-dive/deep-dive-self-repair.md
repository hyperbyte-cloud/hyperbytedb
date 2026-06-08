# Deep Dive: Cluster Self-Repair

In clustered deployments, replicas converge through:

1. **Write replication** — line protocol fan-out after each local WAL append.
2. **Metadata + WAL sync** — startup and reconnect paths in `SyncClient` (`/internal/sync/manifest`, `/internal/sync/metadata`, `/internal/sync/wal`).
3. **Local flush** — each node replays WAL entries (including replicated writes) into its own MergeTree tables.

Peer ack tracking in `ReplicationLog` informs safe WAL truncation so lagging nodes can catch up.

See [Clustering](deep-dive-clustering.md) for bootstrap, sync, replication modes, and drain.
