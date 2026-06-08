// `StorageError<u64>` is openraft-defined (~224 bytes), so every fn here that
// returns `Result<_, StorageError<u64>>` trips `clippy::result_large_err`.
// We can't change the error type without forking openraft; boxing it at every
// call site would be invasive and would only paper over a third-party shape.
#![allow(clippy::result_large_err)]
use std::fmt::Debug;
use std::io::Cursor;
use std::ops::RangeBounds;
use std::path::Path;
use std::sync::Arc;

use openraft::storage::RaftStorage;
use openraft::{
    Entry, EntryPayload, LogId, LogState, RaftLogReader, RaftSnapshotBuilder, Snapshot,
    SnapshotMeta, StorageError, StoredMembership, Vote,
};
use rocksdb::{
    BoundColumnFamily, ColumnFamilyDescriptor, DB, Direction, IteratorMode, Options, WriteBatch,
};

use crate::domain::cluster::membership::{NodeInfo, NodeState, SharedMembership};
use crate::ports::metadata::MetadataPort;

use super::TypeConfig;
use super::state_machine::{StateMachineData, apply_schema_mutation};
use super::types::{ClusterRequest, ClusterResponse};

const CF_META: &str = "meta";
const CF_LOGS: &str = "logs";
const CF_STATE: &str = "state";

const KEY_VOTE: &[u8] = b"vote";
const KEY_LAST_PURGED: &[u8] = b"last_purged";
const KEY_SM_DATA: &[u8] = b"sm_data";
const KEY_SNAPSHOT: &[u8] = b"snapshot";

fn u64_to_be(n: u64) -> [u8; 8] {
    n.to_be_bytes()
}

fn be_to_u64(bytes: &[u8]) -> u64 {
    let mut arr = [0u8; 8];
    arr.copy_from_slice(&bytes[..8]);
    u64::from_be_bytes(arr)
}

fn io_err(e: impl std::fmt::Display) -> std::io::Error {
    std::io::Error::other(e.to_string())
}

fn storage_io_err(e: impl std::fmt::Display) -> StorageError<u64> {
    StorageError::from_io_error(
        openraft::ErrorSubject::Logs,
        openraft::ErrorVerb::Write,
        io_err(e),
    )
}

fn raft_cf<'a>(
    db: &'a DB,
    name: &'static str,
) -> Result<std::sync::Arc<BoundColumnFamily<'a>>, StorageError<u64>> {
    db.cf_handle(name).ok_or_else(|| {
        storage_io_err(format!(
            "raft store: missing RocksDB column family {name:?}"
        ))
    })
}

/// Persisted snapshot envelope (meta + raw SM bytes).
#[derive(serde::Serialize, serde::Deserialize)]
struct StoredSnapshot {
    meta: SnapshotMeta<u64, openraft::BasicNode>,
    data: Vec<u8>,
}

/// RocksDB-backed Raft storage (log + state machine).
///
/// Persists vote, log entries, state machine, and snapshots to disk so that
/// Raft state survives pod/process restarts without invariant violations.
pub struct RaftStore {
    db: Arc<DB>,

    // Cached in memory for fast reads; written through to RocksDB on mutation.
    vote: Option<Vote<u64>>,
    last_purged_log_id: Option<LogId<u64>>,
    sm: StateMachineData,

    // Runtime-only state (not persisted in Raft storage).
    shared_membership: SharedMembership,
    metadata: Option<Arc<dyn MetadataPort>>,
}

impl RaftStore {
    /// Open (or create) the RocksDB-backed Raft store, recovering any
    /// previously persisted state.
    pub fn open<P: AsRef<Path>>(
        path: P,
        shared_membership: SharedMembership,
    ) -> Result<Self, StorageError<u64>> {
        std::fs::create_dir_all(path.as_ref()).map_err(storage_io_err)?;

        let mut db_opts = Options::default();
        db_opts.create_if_missing(true);
        db_opts.create_missing_column_families(true);

        let cfs = vec![
            ColumnFamilyDescriptor::new(CF_META, Options::default()),
            ColumnFamilyDescriptor::new(CF_LOGS, Options::default()),
            ColumnFamilyDescriptor::new(CF_STATE, Options::default()),
        ];

        let db = DB::open_cf_descriptors(&db_opts, path, cfs).map_err(storage_io_err)?;
        let db = Arc::new(db);

        // Recover vote
        let vote: Option<Vote<u64>> = {
            let cf = raft_cf(db.as_ref(), CF_META)?;
            db.get_cf(&cf, KEY_VOTE)
                .map_err(storage_io_err)?
                .map(|v| serde_json::from_slice(&v))
                .transpose()
                .map_err(storage_io_err)?
        };

        // Recover last_purged_log_id
        let last_purged_log_id: Option<LogId<u64>> = {
            let cf = raft_cf(db.as_ref(), CF_META)?;
            db.get_cf(&cf, KEY_LAST_PURGED)
                .map_err(storage_io_err)?
                .map(|v| serde_json::from_slice(&v))
                .transpose()
                .map_err(storage_io_err)?
        };

        // Recover state machine
        let sm: StateMachineData = {
            let cf = raft_cf(db.as_ref(), CF_STATE)?;
            db.get_cf(&cf, KEY_SM_DATA)
                .map_err(storage_io_err)?
                .map(|v| serde_json::from_slice(&v))
                .transpose()
                .map_err(storage_io_err)?
                .unwrap_or_default()
        };

        if vote.is_some() || last_purged_log_id.is_some() || sm.last_applied_log.is_some() {
            tracing::info!(
                vote = ?vote,
                last_purged = ?last_purged_log_id,
                last_applied = ?sm.last_applied_log,
                "raft store recovered from disk"
            );
        }

        Ok(Self {
            db,
            vote,
            last_purged_log_id,
            sm,
            shared_membership,
            metadata: None,
        })
    }

    /// Hydrate the live data-plane `SharedMembership` from the openraft
    /// membership that was just loaded from disk in `open`.
    ///
    /// Without this, a restarted node (especially a leader, whose log is
    /// fully applied so no `Membership` entries are re-applied during catch-up
    /// and no snapshot is installed since it already owns the snapshot) comes
    /// up with an empty `SharedMembership`. The data-plane uses
    /// `SharedMembership` to decide which peers to fan out writes to, so an
    /// empty value means `/write` requests on a restarted leader silently
    /// skip replication to peers — divergence at the live edge.
    ///
    /// Two sources are merged into the live state:
    ///   1. `sm.last_membership`: the openraft `Membership` (which voters are
    ///      in the cluster + their addresses). This is the authoritative
    ///      topology and drives the live `cluster_peers` gauge.
    ///   2. `sm.cluster_membership`: the app-level `NodeState` map (for
    ///      Joining/Draining/Leaving etc.). Preserved verbatim if present so
    ///      operator-initiated drains survive restarts.
    pub async fn hydrate_shared_membership_from_state_machine(&self) {
        let raft_mem = self.sm.last_membership.membership();
        let raft_nodes: std::collections::HashMap<u64, String> = raft_mem
            .nodes()
            .map(|(id, n)| (*id, n.addr.clone()))
            .collect();

        if raft_nodes.is_empty() && self.sm.cluster_membership.nodes.is_empty() {
            return;
        }

        let voter_ids: std::collections::HashSet<u64> = raft_mem
            .get_joint_config()
            .iter()
            .flat_map(|set| set.iter())
            .copied()
            .collect();

        let mut shared = self.shared_membership.write().await;

        // Start from the persisted app-level state to retain operator-set
        // states (Draining/Leaving/Disconnected), then overlay openraft
        // topology on top so any voter we forgot about gets re-added.
        if !self.sm.cluster_membership.nodes.is_empty() {
            *shared = self.sm.cluster_membership.clone();
        }

        let now = chrono::Utc::now().timestamp();
        for (nid, addr) in &raft_nodes {
            let raft_state = if voter_ids.contains(nid) {
                NodeState::Active
            } else {
                NodeState::Syncing
            };
            if let Some(existing) = shared.nodes.get_mut(nid) {
                existing.addr = addr.clone();
                if existing.state != NodeState::Draining && existing.state != NodeState::Leaving {
                    existing.state = raft_state;
                }
                existing.last_heartbeat = now;
            } else {
                shared.add_node(NodeInfo {
                    node_id: *nid,
                    addr: addr.clone(),
                    state: raft_state,
                    joined_at: now,
                    last_heartbeat: now,
                    needs_sync: false,
                });
            }
        }

        let total = shared.nodes.len();
        let version = shared.version;
        drop(shared);

        tracing::info!(
            nodes = total,
            voters = voter_ids.len(),
            version,
            "raft store hydrated SharedMembership from persisted state machine"
        );
        metrics::gauge!("hyperbytedb_cluster_peers").set(total.saturating_sub(1) as f64);
        metrics::gauge!("hyperbytedb_cluster_nodes_total").set(total as f64);
        metrics::gauge!("hyperbytedb_cluster_membership_version").set(version as f64);
    }

    pub fn with_metadata(mut self, metadata: Arc<dyn MetadataPort>) -> Self {
        self.metadata = Some(metadata);
        self
    }

    pub fn shared_membership(&self) -> &SharedMembership {
        &self.shared_membership
    }

    // ── RocksDB helpers ─────────────────────────────────────────────────

    async fn persist_vote_async(&self, vote: &Vote<u64>) -> Result<(), StorageError<u64>> {
        let start = std::time::Instant::now();
        let db = self.db.clone();
        let bytes = serde_json::to_vec(vote).map_err(storage_io_err)?;
        tokio::task::spawn_blocking(move || {
            let cf = db.cf_handle(CF_META).ok_or_else(|| {
                storage_io_err(format!(
                    "raft store: missing RocksDB column family {CF_META:?}"
                ))
            })?;
            db.put_cf(&cf, KEY_VOTE, &bytes).map_err(storage_io_err)?;
            Ok::<(), StorageError<u64>>(())
        })
        .await
        .map_err(storage_io_err)??;
        metrics::histogram!("hyperbytedb_raft_save_vote_seconds")
            .record(start.elapsed().as_secs_f64());
        metrics::counter!("hyperbytedb_raft_save_vote_total").increment(1);
        metrics::gauge!("hyperbytedb_raft_election_term").set(vote.leader_id().term as f64);
        Ok(())
    }

    async fn persist_last_purged_async(
        &self,
        log_id: &LogId<u64>,
    ) -> Result<(), StorageError<u64>> {
        let db = self.db.clone();
        let bytes = serde_json::to_vec(log_id).map_err(storage_io_err)?;
        tokio::task::spawn_blocking(move || {
            let cf = db.cf_handle(CF_META).ok_or_else(|| {
                storage_io_err(format!(
                    "raft store: missing RocksDB column family {CF_META:?}"
                ))
            })?;
            db.put_cf(&cf, KEY_LAST_PURGED, &bytes)
                .map_err(storage_io_err)?;
            Ok(())
        })
        .await
        .map_err(storage_io_err)?
    }
}

impl RaftLogReader<TypeConfig> for RaftStore {
    async fn try_get_log_entries<RB: RangeBounds<u64> + Clone + Debug + Send>(
        &mut self,
        range: RB,
    ) -> Result<Vec<Entry<TypeConfig>>, StorageError<u64>> {
        use std::ops::Bound;
        let start = std::time::Instant::now();

        let start_index = match range.start_bound() {
            Bound::Included(&v) => v,
            Bound::Excluded(&v) => v + 1,
            Bound::Unbounded => 0,
        };
        let end_index = match range.end_bound() {
            Bound::Included(&v) => Some(v + 1),
            Bound::Excluded(&v) => Some(v),
            Bound::Unbounded => None,
        };

        let db = self.db.clone();
        let entries = tokio::task::spawn_blocking(move || {
            let cf = db.cf_handle(CF_LOGS).ok_or_else(|| {
                storage_io_err(format!(
                    "raft store: missing RocksDB column family {CF_LOGS:?}"
                ))
            })?;
            let start_key = u64_to_be(start_index);
            let mode = IteratorMode::From(&start_key, Direction::Forward);
            let iter = db.iterator_cf(&cf, mode);

            let mut entries = Vec::new();
            for item in iter {
                let (key, val) = item.map_err(storage_io_err)?;
                let idx = be_to_u64(&key);
                if let Some(end) = end_index
                    && idx >= end
                {
                    break;
                }
                let entry: Entry<TypeConfig> =
                    serde_json::from_slice(&val).map_err(storage_io_err)?;
                entries.push(entry);
            }
            Ok::<Vec<Entry<TypeConfig>>, StorageError<u64>>(entries)
        })
        .await
        .map_err(storage_io_err)??;

        metrics::histogram!("hyperbytedb_raft_read_log_seconds")
            .record(start.elapsed().as_secs_f64());
        Ok(entries)
    }
}

impl RaftSnapshotBuilder<TypeConfig> for RaftStore {
    async fn build_snapshot(&mut self) -> Result<Snapshot<TypeConfig>, StorageError<u64>> {
        let start = std::time::Instant::now();
        let snapshot = self.serialize_snapshot()?;

        let stored = StoredSnapshot {
            meta: snapshot.meta.clone(),
            data: serde_json::to_vec(&self.sm).map_err(storage_io_err)?,
        };
        let snap_bytes = serde_json::to_vec(&stored).map_err(storage_io_err)?;
        let db = self.db.clone();
        tokio::task::spawn_blocking(move || {
            let cf = db.cf_handle(CF_STATE).ok_or_else(|| {
                storage_io_err(format!(
                    "raft store: missing RocksDB column family {CF_STATE:?}"
                ))
            })?;
            db.put_cf(&cf, KEY_SNAPSHOT, &snap_bytes)
                .map_err(storage_io_err)?;
            Ok::<(), StorageError<u64>>(())
        })
        .await
        .map_err(storage_io_err)??;

        metrics::histogram!("hyperbytedb_raft_build_snapshot_seconds")
            .record(start.elapsed().as_secs_f64());
        metrics::counter!("hyperbytedb_raft_build_snapshot_total").increment(1);
        Ok(snapshot)
    }
}

impl RaftStorage<TypeConfig> for RaftStore {
    type LogReader = Self;
    type SnapshotBuilder = Self;

    async fn save_vote(&mut self, vote: &Vote<u64>) -> Result<(), StorageError<u64>> {
        self.persist_vote_async(vote).await?;
        self.vote = Some(*vote);
        Ok(())
    }

    async fn read_vote(&mut self) -> Result<Option<Vote<u64>>, StorageError<u64>> {
        Ok(self.vote)
    }

    async fn get_log_state(&mut self) -> Result<LogState<TypeConfig>, StorageError<u64>> {
        let start = std::time::Instant::now();
        let db = self.db.clone();
        let last_log_id = tokio::task::spawn_blocking(move || {
            let cf = db.cf_handle(CF_LOGS).ok_or_else(|| {
                storage_io_err(format!(
                    "raft store: missing RocksDB column family {CF_LOGS:?}"
                ))
            })?;
            db.iterator_cf(&cf, IteratorMode::End)
                .next()
                .transpose()
                .map_err(storage_io_err)?
                .map(|(_, val)| -> Result<LogId<u64>, StorageError<u64>> {
                    let entry: Entry<TypeConfig> =
                        serde_json::from_slice(&val).map_err(storage_io_err)?;
                    Ok(entry.log_id)
                })
                .transpose()
        })
        .await
        .map_err(storage_io_err)??
        .or(self.last_purged_log_id);

        metrics::histogram!("hyperbytedb_raft_get_log_state_seconds")
            .record(start.elapsed().as_secs_f64());
        Ok(LogState {
            last_purged_log_id: self.last_purged_log_id,
            last_log_id,
        })
    }

    async fn get_log_reader(&mut self) -> Self::LogReader {
        RaftStore {
            db: self.db.clone(),
            vote: self.vote,
            last_purged_log_id: self.last_purged_log_id,
            sm: self.sm.clone(),
            shared_membership: self.shared_membership.clone(),
            metadata: self.metadata.clone(),
        }
    }

    async fn append_to_log<I>(&mut self, entries: I) -> Result<(), StorageError<u64>>
    where
        I: IntoIterator<Item = Entry<TypeConfig>> + Send,
    {
        let start = std::time::Instant::now();
        let serialized: Vec<(u64, Vec<u8>)> = entries
            .into_iter()
            .map(|entry| {
                let key = entry.log_id.index;
                let val = serde_json::to_vec(&entry).map_err(storage_io_err)?;
                Ok((key, val))
            })
            .collect::<Result<Vec<_>, StorageError<u64>>>()?;

        let count = serialized.len() as u64;
        let db = self.db.clone();
        tokio::task::spawn_blocking(move || {
            let cf = db.cf_handle(CF_LOGS).ok_or_else(|| {
                storage_io_err(format!(
                    "raft store: missing RocksDB column family {CF_LOGS:?}"
                ))
            })?;
            let mut batch = WriteBatch::default();
            for (index, val) in serialized {
                batch.put_cf(&cf, u64_to_be(index), val);
            }
            db.write(batch).map_err(storage_io_err)?;
            Ok::<(), StorageError<u64>>(())
        })
        .await
        .map_err(storage_io_err)??;
        metrics::histogram!("hyperbytedb_raft_append_log_seconds")
            .record(start.elapsed().as_secs_f64());
        metrics::counter!("hyperbytedb_raft_append_log_entries_total").increment(count);
        Ok(())
    }

    async fn delete_conflict_logs_since(
        &mut self,
        log_id: LogId<u64>,
    ) -> Result<(), StorageError<u64>> {
        let db = self.db.clone();
        let index = log_id.index;
        tokio::task::spawn_blocking(move || {
            let cf = db.cf_handle(CF_LOGS).ok_or_else(|| {
                storage_io_err(format!(
                    "raft store: missing RocksDB column family {CF_LOGS:?}"
                ))
            })?;
            let from = u64_to_be(index);
            let to = u64_to_be(u64::MAX);
            db.delete_range_cf(&cf, &from, &to)
                .map_err(storage_io_err)?;
            if let Err(e) = db.delete_cf(&cf, u64_to_be(u64::MAX)) {
                tracing::warn!(error = %e, "raft store: failed to delete sentinel log key");
            }
            Ok::<(), StorageError<u64>>(())
        })
        .await
        .map_err(storage_io_err)?
    }

    async fn purge_logs_upto(&mut self, log_id: LogId<u64>) -> Result<(), StorageError<u64>> {
        let start = std::time::Instant::now();
        self.persist_last_purged_async(&log_id).await?;

        let db = self.db.clone();
        let index = log_id.index;
        tokio::task::spawn_blocking(move || {
            let cf = db.cf_handle(CF_LOGS).ok_or_else(|| {
                storage_io_err(format!(
                    "raft store: missing RocksDB column family {CF_LOGS:?}"
                ))
            })?;
            let from = u64_to_be(0);
            let to = u64_to_be(index + 1);
            db.delete_range_cf(&cf, &from, &to)
                .map_err(storage_io_err)?;
            Ok::<(), StorageError<u64>>(())
        })
        .await
        .map_err(storage_io_err)??;

        self.last_purged_log_id = Some(log_id);
        metrics::histogram!("hyperbytedb_raft_purge_logs_seconds")
            .record(start.elapsed().as_secs_f64());
        Ok(())
    }

    async fn last_applied_state(
        &mut self,
    ) -> Result<
        (
            Option<LogId<u64>>,
            StoredMembership<u64, openraft::BasicNode>,
        ),
        StorageError<u64>,
    > {
        Ok((self.sm.last_applied_log, self.sm.last_membership.clone()))
    }

    async fn apply_to_state_machine(
        &mut self,
        entries: &[Entry<TypeConfig>],
    ) -> Result<Vec<ClusterResponse>, StorageError<u64>> {
        let start = std::time::Instant::now();
        let entry_count = entries.len() as u64;
        let mut responses = Vec::new();

        for entry in entries {
            self.sm.last_applied_log = Some(entry.log_id);

            match &entry.payload {
                EntryPayload::Blank => {
                    responses.push(ClusterResponse::success());
                }
                EntryPayload::Normal(req) => {
                    let resp = self.apply_request(req.clone()).await;
                    responses.push(resp);
                }
                EntryPayload::Membership(mem) => {
                    self.sm.last_membership =
                        StoredMembership::new(Some(entry.log_id), mem.clone());

                    self.sync_raft_membership_to_shared(mem).await;

                    responses.push(ClusterResponse::success());
                }
            }
        }

        let db = self.db.clone();
        let sm_bytes = serde_json::to_vec(&self.sm).map_err(storage_io_err)?;
        tokio::task::spawn_blocking(move || {
            let cf = db.cf_handle(CF_STATE).ok_or_else(|| {
                storage_io_err(format!(
                    "raft store: missing RocksDB column family {CF_STATE:?}"
                ))
            })?;
            db.put_cf(&cf, KEY_SM_DATA, &sm_bytes)
                .map_err(storage_io_err)?;
            Ok::<(), StorageError<u64>>(())
        })
        .await
        .map_err(storage_io_err)??;

        metrics::histogram!("hyperbytedb_raft_apply_sm_seconds")
            .record(start.elapsed().as_secs_f64());
        metrics::counter!("hyperbytedb_raft_apply_sm_entries_total").increment(entry_count);
        Ok(responses)
    }

    async fn get_snapshot_builder(&mut self) -> Self::SnapshotBuilder {
        RaftStore {
            db: self.db.clone(),
            vote: self.vote,
            last_purged_log_id: self.last_purged_log_id,
            sm: self.sm.clone(),
            shared_membership: self.shared_membership.clone(),
            metadata: self.metadata.clone(),
        }
    }

    async fn begin_receiving_snapshot(
        &mut self,
    ) -> Result<Box<Cursor<Vec<u8>>>, StorageError<u64>> {
        Ok(Box::new(Cursor::new(Vec::new())))
    }

    async fn install_snapshot(
        &mut self,
        meta: &SnapshotMeta<u64, openraft::BasicNode>,
        snapshot: Box<Cursor<Vec<u8>>>,
    ) -> Result<(), StorageError<u64>> {
        let start = std::time::Instant::now();
        let data = snapshot.into_inner();
        let sm_data: StateMachineData = serde_json::from_slice(&data).map_err(|e| {
            StorageError::from_io_error(
                openraft::ErrorSubject::Snapshot(Some(meta.signature())),
                openraft::ErrorVerb::Read,
                io_err(e),
            )
        })?;

        let sm_bytes = serde_json::to_vec(&sm_data).map_err(storage_io_err)?;
        let stored = StoredSnapshot {
            meta: meta.clone(),
            data: data.clone(),
        };
        let snap_bytes = serde_json::to_vec(&stored).map_err(storage_io_err)?;
        let last_log_id = meta.last_log_id;
        let purged_bytes = last_log_id
            .map(|lid| serde_json::to_vec(&lid).map_err(storage_io_err))
            .transpose()?;

        let db = self.db.clone();
        tokio::task::spawn_blocking(move || {
            let cf_state = db.cf_handle(CF_STATE).ok_or_else(|| {
                storage_io_err(format!(
                    "raft store: missing RocksDB column family {CF_STATE:?}"
                ))
            })?;
            let cf_meta = db.cf_handle(CF_META).ok_or_else(|| {
                storage_io_err(format!(
                    "raft store: missing RocksDB column family {CF_META:?}"
                ))
            })?;
            let cf_logs = db.cf_handle(CF_LOGS).ok_or_else(|| {
                storage_io_err(format!(
                    "raft store: missing RocksDB column family {CF_LOGS:?}"
                ))
            })?;

            let mut batch = WriteBatch::default();
            batch.put_cf(&cf_state, KEY_SM_DATA, &sm_bytes);
            batch.put_cf(&cf_state, KEY_SNAPSHOT, &snap_bytes);

            if let Some(lid) = last_log_id {
                if let Some(ref pb) = purged_bytes {
                    batch.put_cf(&cf_meta, KEY_LAST_PURGED, pb);
                }
                batch.delete_range_cf(&cf_logs, u64_to_be(0), u64_to_be(lid.index + 1));
            }

            db.write(batch).map_err(storage_io_err)?;
            db.flush_wal(true).map_err(storage_io_err)?;
            Ok::<(), StorageError<u64>>(())
        })
        .await
        .map_err(storage_io_err)??;

        self.sm = sm_data.clone();
        self.last_purged_log_id = meta.last_log_id;

        let mut shared = self.shared_membership.write().await;
        *shared = sm_data.cluster_membership;

        metrics::histogram!("hyperbytedb_raft_install_snapshot_seconds")
            .record(start.elapsed().as_secs_f64());
        metrics::counter!("hyperbytedb_raft_install_snapshot_total").increment(1);
        Ok(())
    }

    async fn get_current_snapshot(
        &mut self,
    ) -> Result<Option<Snapshot<TypeConfig>>, StorageError<u64>> {
        let db = self.db.clone();
        let raw = tokio::task::spawn_blocking(move || {
            let cf = db.cf_handle(CF_STATE).ok_or_else(|| {
                storage_io_err(format!(
                    "raft store: missing RocksDB column family {CF_STATE:?}"
                ))
            })?;
            db.get_cf(&cf, KEY_SNAPSHOT).map_err(storage_io_err)
        })
        .await
        .map_err(storage_io_err)??;

        match raw {
            Some(bytes) => {
                let stored: StoredSnapshot =
                    serde_json::from_slice(&bytes).map_err(storage_io_err)?;
                Ok(Some(Snapshot {
                    meta: stored.meta,
                    snapshot: Box::new(Cursor::new(stored.data)),
                }))
            }
            None => {
                if self.sm.last_applied_log.is_some() {
                    self.serialize_snapshot().map(Some)
                } else {
                    Ok(None)
                }
            }
        }
    }
}

impl RaftStore {
    fn serialize_snapshot(&self) -> Result<Snapshot<TypeConfig>, StorageError<u64>> {
        let bytes = serde_json::to_vec(&self.sm).map_err(|e| {
            StorageError::from_io_error(
                openraft::ErrorSubject::StateMachine,
                openraft::ErrorVerb::Read,
                io_err(e),
            )
        })?;

        Ok(Snapshot {
            meta: SnapshotMeta {
                last_log_id: self.sm.last_applied_log,
                last_membership: self.sm.last_membership.clone(),
                snapshot_id: self
                    .sm
                    .last_applied_log
                    .map(|id| format!("{}-{}", id.leader_id, id.index))
                    .unwrap_or_default(),
            },
            snapshot: Box::new(Cursor::new(bytes)),
        })
    }

    /// Propagate a Raft Membership change into the SharedMembership struct
    /// that the data-plane (PeerClient / fan-out replication) relies on.
    async fn sync_raft_membership_to_shared(
        &self,
        mem: &openraft::Membership<u64, openraft::BasicNode>,
    ) {
        let mut shared = self.shared_membership.write().await;
        let now = chrono::Utc::now().timestamp();

        let voter_ids: std::collections::HashSet<u64> = mem
            .get_joint_config()
            .iter()
            .flat_map(|set| set.iter())
            .copied()
            .collect();

        let all_nodes: std::collections::HashMap<u64, String> = mem
            .nodes()
            .map(|(id, node)| (*id, node.addr.clone()))
            .collect();

        for (nid, addr) in &all_nodes {
            let raft_state = if voter_ids.contains(nid) {
                NodeState::Active
            } else {
                NodeState::Syncing
            };

            if let Some(existing) = shared.nodes.get_mut(nid) {
                existing.addr = addr.clone();
                let should_update = if raft_state == NodeState::Active {
                    existing.state != NodeState::Draining && existing.state != NodeState::Leaving
                } else {
                    existing.state == NodeState::Joining
                        || existing.state == NodeState::Disconnected
                        || existing.state == NodeState::Syncing
                };
                if should_update {
                    existing.state = raft_state;
                }
                existing.last_heartbeat = now;
            } else {
                shared.add_node(NodeInfo {
                    node_id: *nid,
                    addr: addr.clone(),
                    state: raft_state,
                    joined_at: now,
                    last_heartbeat: now,
                    needs_sync: false,
                });
            }
        }

        let stale: Vec<u64> = shared
            .nodes
            .keys()
            .filter(|id| !all_nodes.contains_key(id))
            .copied()
            .collect();
        for id in stale {
            shared.remove_node(id);
        }

        let total = shared.nodes.len();
        let peers = total.saturating_sub(1);
        metrics::gauge!("hyperbytedb_cluster_peers").set(peers as f64);
        metrics::gauge!("hyperbytedb_cluster_nodes_total").set(total as f64);
        metrics::gauge!("hyperbytedb_cluster_membership_version").set(shared.version as f64);
    }

    async fn apply_request(&self, req: ClusterRequest) -> ClusterResponse {
        match req {
            ClusterRequest::SetNodeState { node_id, state } => {
                let mut shared = self.shared_membership.write().await;
                shared.set_state(node_id, state);
                ClusterResponse::success()
            }
            ClusterRequest::SchemaMutation(mutation) => {
                if let Some(ref metadata) = self.metadata {
                    match apply_schema_mutation(metadata, mutation).await {
                        Ok(()) => ClusterResponse::success(),
                        Err(e) => ClusterResponse::error(e.to_string()),
                    }
                } else {
                    ClusterResponse::error("metadata port not available")
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use std::collections::{BTreeMap, BTreeSet, HashMap};

    use openraft::{BasicNode, LeaderId, LogId, Membership, StoredMembership};
    use rocksdb::{DB, Options};
    use tempfile::TempDir;

    use crate::domain::cluster::membership::{ClusterMembership, NodeInfo, NodeState, new_shared};

    use crate::adapters::cluster::raft::state_machine::StateMachineData;

    use super::{CF_STATE, KEY_SM_DATA, RaftStore};

    fn make_app_membership() -> ClusterMembership {
        let mut nodes = HashMap::new();
        nodes.insert(
            1,
            NodeInfo {
                node_id: 1,
                addr: "10.0.0.1:8086".into(),
                state: NodeState::Active,
                joined_at: 100,
                last_heartbeat: 100,
                needs_sync: false,
            },
        );
        nodes.insert(
            2,
            NodeInfo {
                node_id: 2,
                addr: "10.0.0.2:8086".into(),
                state: NodeState::Active,
                joined_at: 100,
                last_heartbeat: 100,
                needs_sync: false,
            },
        );
        ClusterMembership { version: 7, nodes }
    }

    fn make_raft_membership(nodes: &[(u64, &str)]) -> StoredMembership<u64, BasicNode> {
        let mut node_map: BTreeMap<u64, BasicNode> = BTreeMap::new();
        let mut voters: BTreeSet<u64> = BTreeSet::new();
        for (id, addr) in nodes {
            node_map.insert(*id, BasicNode::new(*addr));
            voters.insert(*id);
        }
        let mem = Membership::new(vec![voters], node_map);
        StoredMembership::new(Some(LogId::new(LeaderId::new(8, 1), 21)), mem)
    }

    fn seed_state_machine(raft_dir: &std::path::Path, sm: &StateMachineData) {
        let mut db_opts = Options::default();
        db_opts.create_if_missing(true);
        db_opts.create_missing_column_families(true);

        let cfs = vec![
            rocksdb::ColumnFamilyDescriptor::new("meta", Options::default()),
            rocksdb::ColumnFamilyDescriptor::new("logs", Options::default()),
            rocksdb::ColumnFamilyDescriptor::new(CF_STATE, Options::default()),
        ];
        let db = DB::open_cf_descriptors(&db_opts, raft_dir, cfs).unwrap();
        {
            let cf = db.cf_handle(CF_STATE).unwrap();
            let bytes = serde_json::to_vec(sm).unwrap();
            db.put_cf(&cf, KEY_SM_DATA, &bytes).unwrap();
        }
        drop(db);
    }

    #[tokio::test]
    async fn open_then_hydrate_from_raft_membership() {
        let tmp = TempDir::new().unwrap();
        let raft_dir = tmp.path().join("raft");

        let sm = StateMachineData {
            last_applied_log: Some(LogId::new(LeaderId::new(8, 1), 21)),
            last_membership: make_raft_membership(&[(1, "10.0.0.1:8086"), (2, "10.0.0.2:8086")]),
            cluster_membership: ClusterMembership::new(),
        };
        seed_state_machine(&raft_dir, &sm);

        let shared = new_shared(ClusterMembership::new());
        let store = RaftStore::open(&raft_dir, shared.clone()).expect("open");

        {
            let g = shared.read().await;
            assert_eq!(
                g.nodes.len(),
                0,
                "open alone must NOT populate shared until hydrate is called"
            );
        }

        store.hydrate_shared_membership_from_state_machine().await;

        let g = shared.read().await;
        assert_eq!(
            g.nodes.len(),
            2,
            "hydrate must populate from openraft last_membership"
        );
        assert_eq!(g.nodes.get(&1).unwrap().addr, "10.0.0.1:8086");
        assert_eq!(g.nodes.get(&2).unwrap().addr, "10.0.0.2:8086");
        assert_eq!(g.nodes.get(&1).unwrap().state, NodeState::Active);
        assert_eq!(g.nodes.get(&2).unwrap().state, NodeState::Active);
    }

    #[tokio::test]
    async fn hydrate_preserves_app_drain_state() {
        let tmp = TempDir::new().unwrap();
        let raft_dir = tmp.path().join("raft");

        let mut app_mem = make_app_membership();
        app_mem.nodes.get_mut(&2).unwrap().state = NodeState::Draining;
        let sm = StateMachineData {
            last_applied_log: Some(LogId::new(LeaderId::new(8, 1), 21)),
            last_membership: make_raft_membership(&[(1, "10.0.0.1:8086"), (2, "10.0.0.2:8086")]),
            cluster_membership: app_mem,
        };
        seed_state_machine(&raft_dir, &sm);

        let shared = new_shared(ClusterMembership::new());
        let store = RaftStore::open(&raft_dir, shared.clone()).expect("open");
        store.hydrate_shared_membership_from_state_machine().await;

        let g = shared.read().await;
        assert_eq!(
            g.nodes.get(&2).unwrap().state,
            NodeState::Draining,
            "hydrate must preserve Draining state"
        );
        assert_eq!(g.nodes.get(&1).unwrap().state, NodeState::Active);
    }

    #[tokio::test]
    async fn hydrate_is_a_noop_for_empty_state_machine() {
        let tmp = TempDir::new().unwrap();
        let raft_dir = tmp.path().join("raft");

        let shared = new_shared(ClusterMembership::new());
        let store = RaftStore::open(&raft_dir, shared.clone()).expect("open fresh");
        store.hydrate_shared_membership_from_state_machine().await;
        let g = shared.read().await;
        assert_eq!(g.nodes.len(), 0);
    }
}
