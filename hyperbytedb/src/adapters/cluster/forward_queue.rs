//! Durable queue of writes a coordinator owes to other region primaries.
//!
//! A sharded write used to be acknowledged only after every forward to every
//! other primary returned, so a client write waited for the slowest of N-1
//! nodes: at N=12 on n3 that was ~260 ms against ~100 ms for one forward, and
//! with a fixed number of writes in flight per node it capped throughput.
//! Replication is already asynchronous, so the coordinator now acknowledges
//! once the forwarded share is here, durable, and a background drainer
//! forwards it. The coordinator's callers bound how far the queue may lag.

use std::collections::{BTreeMap, HashSet};
use std::sync::Arc;
use std::sync::Mutex;
use std::sync::atomic::{AtomicU64, Ordering};

use rocksdb::{DB, IteratorMode};
use serde::{Deserialize, Serialize};

use crate::error::HyperbytedbError;

pub const FORWARD_QUEUE_CF: &str = "forward_queue";

/// One queued forward: points (binary frames) for one database and
/// retention policy, re-partitioned against the current shard map when sent.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ForwardEntry {
    pub db: String,
    pub rp: String,
    pub precision: Option<String>,
    /// When the client write was accepted. Kept across retries so the queue's
    /// age reports how far primaries trail, not how recently a retry landed.
    pub enqueued_ms: u64,
    /// Not handed out before this time (retry backoff).
    pub not_before_ms: u64,
    pub attempts: u32,
    pub body: Vec<u8>,
}

struct Pending {
    enqueued_ms: u64,
    not_before_ms: u64,
    bytes: u64,
}

struct State {
    /// Every entry on disk, by sequence.
    pending: BTreeMap<u64, Pending>,
    /// Entries handed to a drainer and not yet completed.
    claimed: HashSet<u64>,
}

pub struct ForwardQueue {
    db: Arc<DB>,
    seq: AtomicU64,
    bytes: AtomicU64,
    state: Mutex<State>,
    notify: tokio::sync::Notify,
}

fn key(seq: u64) -> [u8; 8] {
    seq.to_be_bytes()
}

fn internal(context: &str, e: impl std::fmt::Display) -> HyperbytedbError {
    HyperbytedbError::Internal(format!("forward queue {context}: {e}").into())
}

impl ForwardQueue {
    /// Open the queue in `db`, which must have the [`FORWARD_QUEUE_CF`]
    /// column family, and index whatever a previous run left queued.
    pub fn new(db: Arc<DB>) -> Result<Self, HyperbytedbError> {
        let mut pending = BTreeMap::new();
        let mut bytes = 0u64;
        let mut max_seq = 0u64;
        {
            let cf = db
                .cf_handle(FORWARD_QUEUE_CF)
                .ok_or_else(|| internal("open", "column family missing"))?;
            for item in db.iterator_cf(&cf, IteratorMode::Start) {
                let (k, v) = item.map_err(|e| internal("scan", e))?;
                let Ok(raw) = <[u8; 8]>::try_from(k.as_ref()) else {
                    continue;
                };
                let seq = u64::from_be_bytes(raw);
                let Ok(entry) = bincode::deserialize::<ForwardEntry>(&v) else {
                    continue;
                };
                max_seq = max_seq.max(seq);
                bytes += v.len() as u64;
                pending.insert(
                    seq,
                    Pending {
                        enqueued_ms: entry.enqueued_ms,
                        not_before_ms: entry.not_before_ms,
                        bytes: v.len() as u64,
                    },
                );
            }
        }
        Ok(Self {
            db,
            seq: AtomicU64::new(max_seq),
            bytes: AtomicU64::new(bytes),
            state: Mutex::new(State {
                pending,
                claimed: HashSet::new(),
            }),
            notify: tokio::sync::Notify::new(),
        })
    }

    fn lock(&self) -> std::sync::MutexGuard<'_, State> {
        self.state.lock().unwrap_or_else(|e| e.into_inner())
    }

    /// Store `entry` durably (same durability as the WAL) and wake a drainer.
    pub fn enqueue(&self, entry: &ForwardEntry) -> Result<u64, HyperbytedbError> {
        let value = bincode::serialize(entry).map_err(|e| internal("encode", e))?;
        let seq = self.seq.fetch_add(1, Ordering::SeqCst) + 1;
        let cf = self
            .db
            .cf_handle(FORWARD_QUEUE_CF)
            .ok_or_else(|| internal("enqueue", "column family missing"))?;
        self.db
            .put_cf(&cf, key(seq), &value)
            .map_err(|e| internal("put", e))?;
        self.bytes.fetch_add(value.len() as u64, Ordering::Relaxed);
        self.lock().pending.insert(
            seq,
            Pending {
                enqueued_ms: entry.enqueued_ms,
                not_before_ms: entry.not_before_ms,
                bytes: value.len() as u64,
            },
        );
        self.notify.notify_one();
        Ok(seq)
    }

    /// Hand out, oldest first, up to `max_entries` due entries (and at least
    /// one, whatever its size) totalling at most `max_bytes`. They stay on
    /// disk until [`Self::complete`].
    pub fn claim(
        &self,
        now_ms: u64,
        max_entries: usize,
        max_bytes: u64,
    ) -> Result<Vec<(u64, ForwardEntry)>, HyperbytedbError> {
        let seqs: Vec<u64> = {
            let mut st = self.lock();
            let mut picked = Vec::new();
            let mut total = 0u64;
            for (seq, p) in &st.pending {
                if picked.len() >= max_entries {
                    break;
                }
                if st.claimed.contains(seq) || p.not_before_ms > now_ms {
                    continue;
                }
                if !picked.is_empty() && total + p.bytes > max_bytes {
                    break;
                }
                total += p.bytes;
                picked.push(*seq);
            }
            st.claimed.extend(picked.iter().copied());
            picked
        };
        let cf = self
            .db
            .cf_handle(FORWARD_QUEUE_CF)
            .ok_or_else(|| internal("claim", "column family missing"))?;
        let mut out = Vec::with_capacity(seqs.len());
        for seq in seqs {
            match self.db.get_cf(&cf, key(seq)) {
                Ok(Some(v)) => match bincode::deserialize::<ForwardEntry>(&v) {
                    Ok(e) => out.push((seq, e)),
                    Err(e) => {
                        tracing::error!(seq, error = %e, "undecodable forward queue entry; dropping");
                        self.complete(&[seq])?;
                    }
                },
                Ok(None) => {
                    self.lock().claimed.remove(&seq);
                }
                Err(e) => {
                    self.lock().claimed.remove(&seq);
                    return Err(internal("get", e));
                }
            }
        }
        Ok(out)
    }

    /// Remove finished entries.
    pub fn complete(&self, seqs: &[u64]) -> Result<(), HyperbytedbError> {
        let cf = self
            .db
            .cf_handle(FORWARD_QUEUE_CF)
            .ok_or_else(|| internal("complete", "column family missing"))?;
        let mut batch = rocksdb::WriteBatch::default();
        for seq in seqs {
            batch.delete_cf(&cf, key(*seq));
        }
        self.db.write(batch).map_err(|e| internal("delete", e))?;
        let mut st = self.lock();
        for seq in seqs {
            st.claimed.remove(seq);
            if let Some(p) = st.pending.remove(seq) {
                self.bytes.fetch_sub(p.bytes, Ordering::Relaxed);
            }
        }
        Ok(())
    }

    /// Hand claimed entries back, not to be handed out before
    /// `not_before_ms`. They were never removed from disk.
    pub fn release(&self, seqs: &[u64], not_before_ms: u64) {
        let mut st = self.lock();
        for seq in seqs {
            st.claimed.remove(seq);
            if let Some(p) = st.pending.get_mut(seq) {
                p.not_before_ms = not_before_ms;
            }
        }
    }

    /// How long the oldest queued write has waited, in ms (0 when empty).
    pub fn oldest_age_ms(&self, now_ms: u64) -> u64 {
        self.lock()
            .pending
            .values()
            .map(|p| p.enqueued_ms)
            .min()
            .map_or(0, |t| now_ms.saturating_sub(t))
    }

    pub fn len(&self) -> usize {
        self.lock().pending.len()
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    pub fn bytes(&self) -> u64 {
        self.bytes.load(Ordering::Relaxed)
    }

    /// Resolves after the next enqueue (or at once if one is pending).
    pub async fn notified(&self) {
        self.notify.notified().await;
    }
}

pub fn now_ms() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_millis() as u64)
        .unwrap_or(0)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn open(dir: &std::path::Path) -> ForwardQueue {
        let repl = crate::adapters::cluster::replication_log::ReplicationLog::open(dir).unwrap();
        ForwardQueue::new(repl.db().clone()).unwrap()
    }

    fn entry(enqueued_ms: u64, not_before_ms: u64, body: &[u8]) -> ForwardEntry {
        ForwardEntry {
            db: "db".into(),
            rp: "autogen".into(),
            precision: None,
            enqueued_ms,
            not_before_ms,
            attempts: 0,
            body: body.to_vec(),
        }
    }

    #[test]
    fn entries_are_claimed_in_order_once_and_survive_until_completed() {
        let dir = tempfile::tempdir().unwrap();
        let q = open(dir.path());
        let a = q.enqueue(&entry(100, 0, b"a")).unwrap();
        let b = q.enqueue(&entry(200, 0, b"b")).unwrap();
        let c = q.enqueue(&entry(300, 10_000, b"c")).unwrap();

        let first = q.claim(1_000, 10, u64::MAX).unwrap();
        assert_eq!(
            first.iter().map(|(s, _)| *s).collect::<Vec<_>>(),
            vec![a, b],
            "c is not due"
        );
        assert!(
            q.claim(1_000, 10, u64::MAX).unwrap().is_empty(),
            "claimed entries are not handed out twice"
        );
        assert_eq!(q.oldest_age_ms(1_000), 900);

        q.complete(&[a]).unwrap();
        assert_eq!(q.oldest_age_ms(1_000), 800);
        assert_eq!(q.len(), 2);
        assert_eq!(
            q.claim(20_000, 10, u64::MAX)
                .unwrap()
                .iter()
                .map(|(s, _)| *s)
                .collect::<Vec<_>>(),
            vec![c]
        );
    }

    #[test]
    fn a_reopened_queue_keeps_what_was_not_completed() {
        let dir = tempfile::tempdir().unwrap();
        {
            let q = open(dir.path());
            let a = q.enqueue(&entry(1, 0, b"a")).unwrap();
            q.enqueue(&entry(2, 0, b"b")).unwrap();
            q.complete(&[a]).unwrap();
        }
        let q = open(dir.path());
        let got = q.claim(10, 10, u64::MAX).unwrap();
        assert_eq!(got.len(), 1);
        assert_eq!(got[0].1.body, b"b".to_vec());
        let next = q.enqueue(&entry(3, 0, b"c")).unwrap();
        assert!(next > got[0].0, "sequence resumes past what is on disk");
    }

    #[test]
    fn a_claim_stops_at_the_byte_budget_but_takes_at_least_one() {
        let dir = tempfile::tempdir().unwrap();
        let q = open(dir.path());
        q.enqueue(&entry(1, 0, &[0u8; 100])).unwrap();
        q.enqueue(&entry(2, 0, &[0u8; 100])).unwrap();
        assert_eq!(q.claim(10, 10, 10).unwrap().len(), 1);
    }
}
