use rocksdb::{DB, IteratorMode};
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

use crate::domain::cluster::replication_wire::ReplicationHintPayload;
use crate::error::HyperbytedbError;

const HH_CF: &str = "hinted_handoff";

fn hh_key(peer_id: u64, seq: u64) -> Vec<u8> {
    format!("hh:{:016x}:{:016x}", peer_id, seq).into_bytes()
}

fn hh_peer_prefix(peer_id: u64) -> Vec<u8> {
    format!("hh:{:016x}:", peer_id).into_bytes()
}

pub struct HintedHandoff {
    db: Arc<DB>,
    seq: AtomicU64,
    max_hints_per_peer: u64,
    /// Queued hints per peer, kept in memory: rebuilt by the startup scan,
    /// reserved before each put, released by `drain`.
    ///
    /// Counting by iterating the column family on every enqueue was
    /// quadratic in the backlog. Under saturation replication fails, hints
    /// pile up, and each new hint scanned (and snappy-decompressed) all the
    /// old ones: a CPU profile of a saturated node put 97% of its time in
    /// `pending_count`, and six nodes delivered ~6k points/s.
    counts: std::sync::Mutex<std::collections::HashMap<u64, u64>>,
}

fn peer_of_key(key: &[u8]) -> Option<u64> {
    let k = std::str::from_utf8(key).ok()?;
    let mut parts = k.split(':');
    (parts.next()? == "hh").then_some(())?;
    u64::from_str_radix(parts.next()?, 16).ok()
}

impl HintedHandoff {
    /// Create a HintedHandoff backed by an existing RocksDB instance that
    /// already has the `hinted_handoff` column family.
    pub fn new(db: Arc<DB>, max_hints_per_peer: u64) -> Result<Self, HyperbytedbError> {
        let max_seq = {
            let cf = db
                .cf_handle(HH_CF)
                .ok_or_else(|| HyperbytedbError::Internal("hinted_handoff CF not found".into()))?;

            let mut max = 0u64;
            let mut counts: std::collections::HashMap<u64, u64> = Default::default();
            let prefix = b"hh:";
            let iter = db.iterator_cf_opt(
                &cf,
                rocksdb::ReadOptions::default(),
                IteratorMode::From(prefix, rocksdb::Direction::Forward),
            );
            for item in iter.flatten() {
                let (key, _) = item;
                if !key.starts_with(prefix) {
                    break;
                }
                if let Some(peer) = peer_of_key(&key) {
                    *counts.entry(peer).or_default() += 1;
                }
                if let Ok(k) = std::str::from_utf8(&key)
                    && let Some(seq_hex) = k.rsplit(':').next()
                    && let Ok(s) = u64::from_str_radix(seq_hex, 16)
                {
                    max = max.max(s);
                }
            }
            (max, counts)
        };
        let (max_seq, counts) = max_seq;

        Ok(Self {
            db,
            seq: AtomicU64::new(max_seq),
            max_hints_per_peer,
            counts: std::sync::Mutex::new(counts),
        })
    }

    /// Queue a write that failed to replicate to `peer_id`.
    pub fn enqueue_hint(
        &self,
        peer_id: u64,
        payload: &ReplicationHintPayload,
    ) -> Result<(), HyperbytedbError> {
        let cf = self
            .db
            .cf_handle(HH_CF)
            .ok_or_else(|| HyperbytedbError::Internal("hinted_handoff CF not found".into()))?;

        // Reserve a slot under the lock, so concurrent enqueues cannot all
        // pass the cap check before any of them lands.
        {
            let mut counts = self.counts.lock().unwrap_or_else(|e| e.into_inner());
            let count = counts.entry(peer_id).or_default();
            if self.max_hints_per_peer > 0 && *count >= self.max_hints_per_peer {
                let full = *count;
                drop(counts);
                metrics::counter!(
                    "hyperbytedb_hinted_handoff_rejected_total",
                    "peer_id" => peer_id.to_string()
                )
                .increment(1);
                return Err(HyperbytedbError::Internal(
                    format!(
                        "hinted handoff queue full for peer {peer_id} ({full}/{})",
                        self.max_hints_per_peer
                    )
                    .into(),
                ));
            }
            *count += 1;
        }

        let seq = self.seq.fetch_add(1, Ordering::SeqCst) + 1;
        let key = hh_key(peer_id, seq);
        let put = payload.encode_hint_value().and_then(|value| {
            self.db
                .put_cf(&cf, key, value)
                .map_err(|e| HyperbytedbError::Internal(format!("enqueue hint: {e}").into()))
        });
        if let Err(e) = put {
            self.release(peer_id, 1);
            return Err(e);
        }

        metrics::counter!("hyperbytedb_hinted_handoff_enqueued_total", "peer_id" => peer_id.to_string())
            .increment(1);

        Ok(())
    }

    /// Read and delete up to `batch_size` hints for `peer_id`, FIFO order.
    pub fn drain(
        &self,
        peer_id: u64,
        batch_size: usize,
    ) -> Result<Vec<ReplicationHintPayload>, HyperbytedbError> {
        let cf = self
            .db
            .cf_handle(HH_CF)
            .ok_or_else(|| HyperbytedbError::Internal("hinted_handoff CF not found".into()))?;

        let prefix = hh_peer_prefix(peer_id);
        let iter = self.db.iterator_cf_opt(
            &cf,
            rocksdb::ReadOptions::default(),
            IteratorMode::From(&prefix, rocksdb::Direction::Forward),
        );

        let mut results = Vec::new();
        let mut keys_to_delete = Vec::new();

        for item in iter {
            if results.len() >= batch_size {
                break;
            }
            if let Ok((key, value)) = item {
                if !key.starts_with(&prefix) {
                    break;
                }
                match ReplicationHintPayload::decode_hint_value(&value) {
                    Ok(rec) => {
                        results.push(rec);
                        keys_to_delete.push(key.to_vec());
                    }
                    Err(e) => {
                        tracing::warn!(
                            error = %e,
                            "removing invalid or legacy hinted handoff entry"
                        );
                        keys_to_delete.push(key.to_vec());
                    }
                }
            }
        }

        let mut deleted = 0u64;
        for key in &keys_to_delete {
            match self.db.delete_cf(&cf, key) {
                Ok(()) => deleted += 1,
                Err(e) => tracing::warn!(error = %e, "failed to delete drained hint"),
            }
        }
        self.release(peer_id, deleted);

        if !results.is_empty() {
            metrics::counter!(
                "hyperbytedb_hinted_handoff_drained_total",
                "peer_id" => peer_id.to_string()
            )
            .increment(results.len() as u64);
        }

        Ok(results)
    }

    fn release(&self, peer_id: u64, n: u64) {
        if n == 0 {
            return;
        }
        let mut counts = self.counts.lock().unwrap_or_else(|e| e.into_inner());
        if let Some(c) = counts.get_mut(&peer_id) {
            *c = c.saturating_sub(n);
        }
    }

    /// Number of queued hints for a peer. O(1); see `counts`.
    pub fn pending_count(&self, peer_id: u64) -> Result<u64, HyperbytedbError> {
        let counts = self.counts.lock().unwrap_or_else(|e| e.into_inner());
        Ok(counts.get(&peer_id).copied().unwrap_or(0))
    }

    /// Total pending hints across all peers. O(peers).
    pub fn total_pending(&self) -> Result<u64, HyperbytedbError> {
        let counts = self.counts.lock().unwrap_or_else(|e| e.into_inner());
        Ok(counts.values().sum())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::adapters::cluster::replication_log::ReplicationLog;

    fn tmp_hh() -> (HintedHandoff, tempfile::TempDir) {
        let dir = tempfile::tempdir().unwrap();
        let repl = ReplicationLog::open(dir.path()).unwrap();
        let hh = HintedHandoff::new(repl.db().clone(), 100).unwrap();
        (hh, dir)
    }

    fn make_payload(db: &str) -> ReplicationHintPayload {
        ReplicationHintPayload {
            database: db.to_string(),
            retention_policy: "autogen".to_string(),
            precision: None,
            line_body: b"cpu,host=h v=1".to_vec(),
        }
    }

    #[test]
    fn counts_are_rebuilt_per_peer_on_reopen() {
        // The in-memory counts replace a per-enqueue scan; after a restart
        // they must come back from disk, per peer, including the cap.
        let dir = tempfile::tempdir().unwrap();
        let repl = ReplicationLog::open(dir.path()).unwrap();
        {
            let hh = HintedHandoff::new(repl.db().clone(), 3).unwrap();
            for _ in 0..3 {
                hh.enqueue_hint(7, &make_payload("db")).unwrap();
            }
            hh.enqueue_hint(9, &make_payload("db")).unwrap();
            hh.drain(9, 10).unwrap();
            hh.enqueue_hint(9, &make_payload("db")).unwrap();
        }
        let hh = HintedHandoff::new(repl.db().clone(), 3).unwrap();
        assert_eq!(hh.pending_count(7).unwrap(), 3);
        assert_eq!(hh.pending_count(9).unwrap(), 1);
        assert_eq!(hh.total_pending().unwrap(), 4);
        assert!(
            hh.enqueue_hint(7, &make_payload("db")).is_err(),
            "the cap still holds after reopen"
        );
        hh.drain(7, 2).unwrap();
        assert_eq!(hh.pending_count(7).unwrap(), 1);
        hh.enqueue_hint(7, &make_payload("db")).unwrap();
    }

    #[test]
    fn enqueue_and_drain() {
        let (hh, _dir) = tmp_hh();
        hh.enqueue_hint(2, &make_payload("db1")).unwrap();
        hh.enqueue_hint(2, &make_payload("db2")).unwrap();
        assert_eq!(hh.pending_count(2).unwrap(), 2);
        assert_eq!(hh.total_pending().unwrap(), 2);

        let drained = hh.drain(2, 10).unwrap();
        assert_eq!(drained.len(), 2);
        assert_eq!(drained[0].database, "db1");
        assert_eq!(drained[1].database, "db2");

        assert_eq!(hh.pending_count(2).unwrap(), 0);
    }

    #[test]
    fn drain_respects_batch_size() {
        let (hh, _dir) = tmp_hh();
        for i in 0..5 {
            hh.enqueue_hint(3, &make_payload(&format!("db{i}")))
                .unwrap();
        }
        let batch = hh.drain(3, 2).unwrap();
        assert_eq!(batch.len(), 2);
        assert_eq!(hh.pending_count(3).unwrap(), 3);
    }

    #[test]
    fn per_peer_isolation() {
        let (hh, _dir) = tmp_hh();
        hh.enqueue_hint(1, &make_payload("a")).unwrap();
        hh.enqueue_hint(2, &make_payload("b")).unwrap();
        assert_eq!(hh.pending_count(1).unwrap(), 1);
        assert_eq!(hh.pending_count(2).unwrap(), 1);

        let d1 = hh.drain(1, 10).unwrap();
        assert_eq!(d1.len(), 1);
        assert_eq!(hh.pending_count(2).unwrap(), 1);
    }

    #[test]
    fn max_hints_rejects_when_full() {
        let dir = tempfile::tempdir().unwrap();
        let repl = ReplicationLog::open(dir.path()).unwrap();
        let hh = HintedHandoff::new(repl.db().clone(), 3).unwrap();

        for i in 0..3 {
            hh.enqueue_hint(1, &make_payload(&format!("db{i}")))
                .unwrap();
        }
        assert_eq!(hh.pending_count(1).unwrap(), 3);
        let err = hh
            .enqueue_hint(1, &make_payload("db_overflow"))
            .unwrap_err();
        assert!(
            err.to_string().contains("hinted handoff queue full"),
            "unexpected error: {err}"
        );
        assert_eq!(hh.pending_count(1).unwrap(), 3);
    }
}
