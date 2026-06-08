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
                if let Ok(k) = std::str::from_utf8(&key)
                    && let Some(seq_hex) = k.rsplit(':').next()
                    && let Ok(s) = u64::from_str_radix(seq_hex, 16)
                {
                    max = max.max(s);
                }
            }
            max
        };

        Ok(Self {
            db,
            seq: AtomicU64::new(max_seq),
            max_hints_per_peer,
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

        if self.max_hints_per_peer > 0 {
            let count = self.pending_count(peer_id)?;
            if count >= self.max_hints_per_peer {
                tracing::warn!(
                    peer_id = peer_id,
                    count = count,
                    limit = self.max_hints_per_peer,
                    "hinted handoff queue full for peer, dropping oldest hint"
                );
                self.drop_oldest(peer_id)?;
            }
        }

        let seq = self.seq.fetch_add(1, Ordering::SeqCst) + 1;
        let key = hh_key(peer_id, seq);
        let value = payload.encode_hint_value()?;
        self.db
            .put_cf(&cf, key, value)
            .map_err(|e| HyperbytedbError::Internal(format!("enqueue hint: {e}")))?;

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

        for key in &keys_to_delete {
            if let Err(e) = self.db.delete_cf(&cf, key) {
                tracing::warn!(error = %e, "failed to delete drained hint");
            }
        }

        if !results.is_empty() {
            metrics::counter!(
                "hyperbytedb_hinted_handoff_drained_total",
                "peer_id" => peer_id.to_string()
            )
            .increment(results.len() as u64);
        }

        Ok(results)
    }

    /// Number of queued hints for a peer.
    pub fn pending_count(&self, peer_id: u64) -> Result<u64, HyperbytedbError> {
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

        let mut count = 0u64;
        for (key, _) in iter.flatten() {
            if !key.starts_with(&prefix) {
                break;
            }
            count += 1;
        }
        Ok(count)
    }

    /// Total pending hints across all peers.
    pub fn total_pending(&self) -> Result<u64, HyperbytedbError> {
        let cf = self
            .db
            .cf_handle(HH_CF)
            .ok_or_else(|| HyperbytedbError::Internal("hinted_handoff CF not found".into()))?;

        let prefix = b"hh:";
        let iter = self.db.iterator_cf_opt(
            &cf,
            rocksdb::ReadOptions::default(),
            IteratorMode::From(prefix, rocksdb::Direction::Forward),
        );

        let mut count = 0u64;
        for (key, _) in iter.flatten() {
            if !key.starts_with(prefix) {
                break;
            }
            count += 1;
        }
        Ok(count)
    }

    fn drop_oldest(&self, peer_id: u64) -> Result<(), HyperbytedbError> {
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

        if let Some(Ok((key, _))) = iter.into_iter().next()
            && key.starts_with(&prefix)
        {
            self.db
                .delete_cf(&cf, &key)
                .map_err(|e| HyperbytedbError::Internal(format!("drop oldest hint: {e}")))?;
        }
        Ok(())
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
    fn max_hints_drops_oldest() {
        let dir = tempfile::tempdir().unwrap();
        let repl = ReplicationLog::open(dir.path()).unwrap();
        let hh = HintedHandoff::new(repl.db().clone(), 3).unwrap();

        for i in 0..5 {
            hh.enqueue_hint(1, &make_payload(&format!("db{i}")))
                .unwrap();
        }
        assert_eq!(hh.pending_count(1).unwrap(), 3);
        let drained = hh.drain(1, 10).unwrap();
        assert_eq!(drained[0].database, "db2");
    }
}
