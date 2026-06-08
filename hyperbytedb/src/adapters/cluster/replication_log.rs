use rocksdb::{ColumnFamilyDescriptor, DB, IteratorMode, Options};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::path::Path;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};

use crate::domain::cluster::types::MutationRequest;
use crate::error::HyperbytedbError;

const REPL_CF: &str = "replication";
const HINTED_HANDOFF_CF: &str = "hinted_handoff";

fn ack_key(peer_id: u64) -> Vec<u8> {
    format!("repl_ack:{}", peer_id).into_bytes()
}

fn mutation_log_key(seq: u64) -> Vec<u8> {
    format!("mutation_log:{:016x}", seq).into_bytes()
}

fn mutation_ack_key(peer_id: u64) -> Vec<u8> {
    format!("mutation_ack:{}", peer_id).into_bytes()
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MutationLogEntry {
    pub seq: u64,
    pub request: MutationRequest,
}

pub struct ReplicationLog {
    db: Arc<DB>,
    mutation_seq: AtomicU64,
    /// Tracks the last applied mutation seq per origin node for deduplication.
    applied_mutation_seqs: Mutex<HashMap<u64, u64>>,
}

impl ReplicationLog {
    pub fn open<P: AsRef<Path>>(path: P) -> Result<Self, HyperbytedbError> {
        let mut opts = Options::default();
        opts.create_if_missing(true);
        opts.create_missing_column_families(true);

        let cf_opts = Options::default();
        let hh_opts = Options::default();
        let cfs = vec![
            ColumnFamilyDescriptor::new(REPL_CF, cf_opts),
            ColumnFamilyDescriptor::new(HINTED_HANDOFF_CF, hh_opts),
        ];

        let db = Arc::new(
            DB::open_cf_descriptors(&opts, path, cfs)
                .map_err(|e| HyperbytedbError::Internal(format!("replication log open: {e}")))?,
        );

        let mutation_seq = {
            let cf = db
                .cf_handle(REPL_CF)
                .ok_or_else(|| HyperbytedbError::Internal("replication CF not found".into()))?;
            let prefix = b"mutation_log:";
            let iter = db.iterator_cf_opt(
                &cf,
                rocksdb::ReadOptions::default(),
                IteratorMode::From(prefix, rocksdb::Direction::Forward),
            );
            let mut max_seq = 0u64;
            for (key, _) in iter.flatten() {
                if !key.starts_with(prefix) {
                    break;
                }
                if let Ok(k) = std::str::from_utf8(&key)
                    && let Some(hex) = k.strip_prefix("mutation_log:")
                    && let Ok(s) = u64::from_str_radix(hex, 16)
                {
                    max_seq = max_seq.max(s);
                }
            }
            max_seq
        };

        Ok(Self {
            db,
            mutation_seq: AtomicU64::new(mutation_seq),
            applied_mutation_seqs: Mutex::new(HashMap::new()),
        })
    }

    /// Shared RocksDB handle, used by [`HintedHandoff`] to share the same
    /// database instance.
    pub fn db(&self) -> &Arc<DB> {
        &self.db
    }

    /// Record WAL ack for a peer.  Only advances the ack forward; if `seq`
    /// is <= the current ack it is ignored so that out-of-order concurrent
    /// replication tasks cannot regress the watermark.
    pub fn set_wal_ack(&self, peer_id: u64, seq: u64) -> Result<(), HyperbytedbError> {
        let cf = self
            .db
            .cf_handle(REPL_CF)
            .ok_or_else(|| HyperbytedbError::Internal("replication CF not found".into()))?;
        let current = match self.db.get_cf(&cf, ack_key(peer_id)) {
            Ok(Some(v)) => {
                let mut arr = [0u8; 8];
                arr.copy_from_slice(&v);
                u64::from_be_bytes(arr)
            }
            _ => 0,
        };
        if seq <= current {
            return Ok(());
        }
        self.db
            .put_cf(&cf, ack_key(peer_id), seq.to_be_bytes())
            .map_err(|e| HyperbytedbError::Internal(format!("set_wal_ack: {e}")))?;
        Ok(())
    }

    /// Get last WAL ack for a peer.
    pub fn get_wal_ack(&self, peer_id: u64) -> Result<u64, HyperbytedbError> {
        let cf = self
            .db
            .cf_handle(REPL_CF)
            .ok_or_else(|| HyperbytedbError::Internal("replication CF not found".into()))?;
        match self.db.get_cf(&cf, ack_key(peer_id)) {
            Ok(Some(v)) => {
                let mut arr = [0u8; 8];
                arr.copy_from_slice(&v);
                Ok(u64::from_be_bytes(arr))
            }
            Ok(None) => Ok(0),
            Err(e) => Err(HyperbytedbError::Internal(format!("get_wal_ack: {e}"))),
        }
    }

    /// Get the minimum WAL ack across all tracked peers.
    ///
    /// **Note:** Only considers peers that already have a `repl_ack` key. Peers that
    /// have never successfully acked are omitted, which can overstate the minimum.
    /// Prefer [`Self::min_max_wal_ack_for_peers`] for flush truncation decisions.
    pub fn min_wal_ack(&self) -> Result<Option<u64>, HyperbytedbError> {
        let cf = self
            .db
            .cf_handle(REPL_CF)
            .ok_or_else(|| HyperbytedbError::Internal("replication CF not found".into()))?;
        let prefix = b"repl_ack:";
        let iter = self.db.iterator_cf_opt(
            &cf,
            rocksdb::ReadOptions::default(),
            IteratorMode::From(prefix, rocksdb::Direction::Forward),
        );
        let mut min_val: Option<u64> = None;
        for (key, value) in iter.flatten() {
            if !key.starts_with(prefix) {
                break;
            }
            let mut arr = [0u8; 8];
            arr.copy_from_slice(&value);
            let seq = u64::from_be_bytes(arr);
            min_val = Some(min_val.map_or(seq, |m: u64| m.min(seq)));
        }
        Ok(min_val)
    }

    /// Per-peer WAL acks (missing keys count as 0). Returns `(min, max)` over `peer_ids`.
    /// Empty `peer_ids` yields `(u64::MAX, 0)` — caller should treat as “no peers”.
    pub fn min_max_wal_ack_for_peers(
        &self,
        peer_ids: &[u64],
    ) -> Result<(u64, u64), HyperbytedbError> {
        if peer_ids.is_empty() {
            return Ok((u64::MAX, 0));
        }
        let mut min_a = u64::MAX;
        let mut max_a = 0u64;
        for &pid in peer_ids {
            let a = self.get_wal_ack(pid)?;
            min_a = min_a.min(a);
            max_a = max_a.max(a);
        }
        Ok((min_a, max_a))
    }

    /// Append a mutation to the log and return its sequence number.
    pub fn append_mutation(&self, request: &MutationRequest) -> Result<u64, HyperbytedbError> {
        let seq = self.mutation_seq.fetch_add(1, Ordering::SeqCst) + 1;
        let cf = self
            .db
            .cf_handle(REPL_CF)
            .ok_or_else(|| HyperbytedbError::Internal("replication CF not found".into()))?;
        let entry = MutationLogEntry {
            seq,
            request: request.clone(),
        };
        let value = serde_json::to_vec(&entry)
            .map_err(|e| HyperbytedbError::Internal(format!("serialize mutation: {e}")))?;
        self.db
            .put_cf(&cf, mutation_log_key(seq), value)
            .map_err(|e| HyperbytedbError::Internal(format!("append_mutation: {e}")))?;
        Ok(seq)
    }

    /// Read mutation log entries from a given sequence.
    pub fn read_mutations_from(
        &self,
        from_seq: u64,
        max_entries: usize,
    ) -> Result<Vec<MutationLogEntry>, HyperbytedbError> {
        let cf = self
            .db
            .cf_handle(REPL_CF)
            .ok_or_else(|| HyperbytedbError::Internal("replication CF not found".into()))?;
        let start = mutation_log_key(from_seq);
        let iter = self.db.iterator_cf_opt(
            &cf,
            rocksdb::ReadOptions::default(),
            IteratorMode::From(&start, rocksdb::Direction::Forward),
        );
        let prefix = b"mutation_log:";
        let mut results = Vec::new();
        for item in iter {
            if results.len() >= max_entries {
                break;
            }
            if let Ok((key, value)) = item {
                if !key.starts_with(prefix) {
                    break;
                }
                let entry: MutationLogEntry = serde_json::from_slice(&value).map_err(|e| {
                    HyperbytedbError::Internal(format!("deserialize mutation: {e}"))
                })?;
                results.push(entry);
            }
        }
        Ok(results)
    }

    /// Set mutation ack for a peer.
    pub fn set_mutation_ack(&self, peer_id: u64, seq: u64) -> Result<(), HyperbytedbError> {
        let cf = self
            .db
            .cf_handle(REPL_CF)
            .ok_or_else(|| HyperbytedbError::Internal("replication CF not found".into()))?;
        self.db
            .put_cf(&cf, mutation_ack_key(peer_id), seq.to_be_bytes())
            .map_err(|e| HyperbytedbError::Internal(format!("set_mutation_ack: {e}")))?;
        Ok(())
    }

    /// Get mutation ack for a peer.
    pub fn get_mutation_ack(&self, peer_id: u64) -> Result<u64, HyperbytedbError> {
        let cf = self
            .db
            .cf_handle(REPL_CF)
            .ok_or_else(|| HyperbytedbError::Internal("replication CF not found".into()))?;
        match self.db.get_cf(&cf, mutation_ack_key(peer_id)) {
            Ok(Some(v)) => {
                let mut arr = [0u8; 8];
                arr.copy_from_slice(&v);
                Ok(u64::from_be_bytes(arr))
            }
            Ok(None) => Ok(0),
            Err(e) => Err(HyperbytedbError::Internal(format!("get_mutation_ack: {e}"))),
        }
    }

    pub fn last_mutation_seq(&self) -> u64 {
        self.mutation_seq.load(Ordering::SeqCst)
    }

    /// Get the minimum mutation ack across all tracked peers.
    pub fn min_mutation_ack(&self) -> Result<Option<u64>, HyperbytedbError> {
        let cf = self
            .db
            .cf_handle(REPL_CF)
            .ok_or_else(|| HyperbytedbError::Internal("replication CF not found".into()))?;
        let prefix = b"mutation_ack:";
        let iter = self.db.iterator_cf_opt(
            &cf,
            rocksdb::ReadOptions::default(),
            IteratorMode::From(prefix, rocksdb::Direction::Forward),
        );
        let mut min_val: Option<u64> = None;
        for (key, value) in iter.flatten() {
            if !key.starts_with(prefix) {
                break;
            }
            let mut arr = [0u8; 8];
            arr.copy_from_slice(&value);
            let seq = u64::from_be_bytes(arr);
            min_val = Some(min_val.map_or(seq, |m: u64| m.min(seq)));
        }
        Ok(min_val)
    }

    /// Truncate mutation log entries before the given sequence.
    pub fn truncate_mutations_before(&self, seq: u64) -> Result<(), HyperbytedbError> {
        let cf = self
            .db
            .cf_handle(REPL_CF)
            .ok_or_else(|| HyperbytedbError::Internal("replication CF not found".into()))?;
        let from = mutation_log_key(0);
        let to = mutation_log_key(seq);
        self.db
            .delete_range_cf(&cf, &from, &to)
            .map_err(|e| HyperbytedbError::Internal(format!("truncate_mutations: {e}")))?;
        Ok(())
    }

    /// Returns true if this mutation should be applied (not a duplicate).
    /// Returns false if it has already been applied (seq <= last seen for this origin).
    pub fn check_and_record_mutation(&self, origin_node_id: u64, seq: u64) -> bool {
        let mut map = self
            .applied_mutation_seqs
            .lock()
            .unwrap_or_else(|e| e.into_inner());
        let last = map.entry(origin_node_id).or_insert(0);
        if seq > *last {
            *last = seq;
            true
        } else {
            false
        }
    }

    /// Remove ack tracking for a peer that has left the cluster.
    pub fn remove_peer(&self, peer_id: u64) -> Result<(), HyperbytedbError> {
        let cf = self
            .db
            .cf_handle(REPL_CF)
            .ok_or_else(|| HyperbytedbError::Internal("replication CF not found".into()))?;
        if let Err(e) = self.db.delete_cf(&cf, ack_key(peer_id)) {
            tracing::warn!(peer_id = peer_id, error = %e, "failed to delete ack key");
        }
        if let Err(e) = self.db.delete_cf(&cf, mutation_ack_key(peer_id)) {
            tracing::warn!(peer_id = peer_id, error = %e, "failed to delete mutation ack key");
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn tmp_log() -> ReplicationLog {
        let dir = tempfile::tempdir().unwrap();
        ReplicationLog::open(dir.path()).unwrap()
    }

    #[test]
    fn test_wal_ack() {
        let log = tmp_log();
        assert_eq!(log.get_wal_ack(1).unwrap(), 0);

        log.set_wal_ack(1, 42).unwrap();
        assert_eq!(log.get_wal_ack(1).unwrap(), 42);

        log.set_wal_ack(1, 100).unwrap();
        assert_eq!(log.get_wal_ack(1).unwrap(), 100);
    }

    #[test]
    fn test_wal_ack_monotonic() {
        let log = tmp_log();
        log.set_wal_ack(1, 50).unwrap();
        assert_eq!(log.get_wal_ack(1).unwrap(), 50);

        // Setting a lower value must be a no-op
        log.set_wal_ack(1, 30).unwrap();
        assert_eq!(log.get_wal_ack(1).unwrap(), 50);

        // Equal value is also a no-op
        log.set_wal_ack(1, 50).unwrap();
        assert_eq!(log.get_wal_ack(1).unwrap(), 50);

        // Higher value advances the ack
        log.set_wal_ack(1, 60).unwrap();
        assert_eq!(log.get_wal_ack(1).unwrap(), 60);
    }

    #[test]
    fn test_min_wal_ack() {
        let log = tmp_log();
        assert_eq!(log.min_wal_ack().unwrap(), None);

        log.set_wal_ack(1, 10).unwrap();
        assert_eq!(log.min_wal_ack().unwrap(), Some(10));

        log.set_wal_ack(2, 5).unwrap();
        assert_eq!(log.min_wal_ack().unwrap(), Some(5));

        log.set_wal_ack(3, 20).unwrap();
        assert_eq!(log.min_wal_ack().unwrap(), Some(5));
    }

    #[test]
    fn test_min_max_wal_ack_for_peers_treats_missing_as_zero() {
        let log = tmp_log();
        log.set_wal_ack(1, 100).unwrap();
        // Peer 2 never acked — must not be ignored (old min_wal_ack iterator bug).
        let (min_a, max_a) = log.min_max_wal_ack_for_peers(&[1, 2]).unwrap();
        assert_eq!(min_a, 0);
        assert_eq!(max_a, 100);
    }

    #[test]
    fn test_append_and_read_mutations() {
        let log = tmp_log();
        assert_eq!(log.last_mutation_seq(), 0);

        let seq1 = log
            .append_mutation(&MutationRequest::CreateDatabase("db1".into()))
            .unwrap();
        assert_eq!(seq1, 1);
        assert_eq!(log.last_mutation_seq(), 1);

        let seq2 = log
            .append_mutation(&MutationRequest::DropDatabase("db1".into()))
            .unwrap();
        assert_eq!(seq2, 2);

        let entries = log.read_mutations_from(1, 10).unwrap();
        assert_eq!(entries.len(), 2);
        assert_eq!(entries[0].seq, 1);
        assert_eq!(entries[1].seq, 2);
    }

    #[test]
    fn test_mutation_ack() {
        let log = tmp_log();
        assert_eq!(log.get_mutation_ack(1).unwrap(), 0);

        log.set_mutation_ack(1, 5).unwrap();
        assert_eq!(log.get_mutation_ack(1).unwrap(), 5);
    }

    #[test]
    fn test_truncate_mutations() {
        let log = tmp_log();
        log.append_mutation(&MutationRequest::CreateDatabase("a".into()))
            .unwrap();
        log.append_mutation(&MutationRequest::CreateDatabase("b".into()))
            .unwrap();
        log.append_mutation(&MutationRequest::CreateDatabase("c".into()))
            .unwrap();

        log.truncate_mutations_before(3).unwrap();

        let entries = log.read_mutations_from(1, 10).unwrap();
        assert_eq!(entries.len(), 1);
        assert_eq!(entries[0].seq, 3);
    }

    #[test]
    fn test_remove_peer() {
        let log = tmp_log();
        log.set_wal_ack(1, 10).unwrap();
        log.set_mutation_ack(1, 5).unwrap();

        log.remove_peer(1).unwrap();

        assert_eq!(log.get_wal_ack(1).unwrap(), 0);
        assert_eq!(log.get_mutation_ack(1).unwrap(), 0);
    }
}
