//! Durable queue of writes a coordinator owes to other region primaries.
//!
//! A sharded write used to be acknowledged only after every forward to every
//! other primary returned, so a client write waited for the slowest of N-1
//! nodes: at N=12 on n3 that was ~260 ms against ~100 ms for one forward, and
//! with a fixed number of writes in flight per node it capped throughput.
//! Replication is already asynchronous, so the coordinator now acknowledges
//! once the forwarded share is here, durable, and a background drainer
//! forwards it. The coordinator's callers bound how far the queue may lag.

//!
//! RocksDB calls run on the blocking pool, as the WAL's do. In memory the
//! queue keeps indexes by readiness and by age, so a claim and the per-write
//! lag check cost O(log n) however many entries are claimed, backed off or
//! queued: those scans used to walk every entry, under a lock every write
//! takes, and the backlog is largest exactly when a peer is down.

use std::collections::{BTreeMap, BTreeSet};
use std::sync::Arc;
use std::sync::Mutex;
use std::sync::atomic::{AtomicU64, Ordering};

use async_trait::async_trait;
use rocksdb::{DB, IteratorMode};

pub use crate::domain::cluster::forward::ForwardEntry;
use crate::error::HyperbytedbError;
use crate::ports::forward_queue::ForwardQueuePort;

pub const FORWARD_QUEUE_CF: &str = "forward_queue";

struct Pending {
    enqueued_ms: u64,
    not_before_ms: u64,
    bytes: u64,
}

/// Every stored entry is in `pending` and `by_age`. An unclaimed one is also
/// in exactly one of `ready` (due) or `delayed` (backing off); a claimed one
/// is in neither.
#[derive(Default)]
struct State {
    /// Every entry stored, by sequence.
    pending: BTreeMap<u64, Pending>,
    /// Unclaimed entries that are due, in sequence order.
    ready: BTreeSet<u64>,
    /// Unclaimed entries backing off: (not before, sequence).
    delayed: BTreeSet<(u64, u64)>,
    /// Every entry stored: (enqueued, sequence), for the queue's age.
    by_age: BTreeSet<(u64, u64)>,
}

impl State {
    fn insert(&mut self, seq: u64, p: Pending) {
        self.by_age.insert((p.enqueued_ms, seq));
        if p.not_before_ms == 0 {
            self.ready.insert(seq);
        } else {
            self.delayed.insert((p.not_before_ms, seq));
        }
        self.pending.insert(seq, p);
    }

    /// Forget `seq`; returns its size if it was stored.
    fn remove(&mut self, seq: u64) -> Option<u64> {
        let p = self.pending.remove(&seq)?;
        self.by_age.remove(&(p.enqueued_ms, seq));
        self.ready.remove(&seq);
        self.delayed.remove(&(p.not_before_ms, seq));
        Some(p.bytes)
    }

    /// Mark `seq` unclaimed again, due at `not_before_ms`.
    fn unclaim(&mut self, seq: u64, not_before_ms: u64) {
        let Some(p) = self.pending.get_mut(&seq) else {
            return;
        };
        self.delayed.remove(&(p.not_before_ms, seq));
        p.not_before_ms = not_before_ms;
        self.ready.remove(&seq);
        self.delayed.insert((not_before_ms, seq));
    }

    fn claim(&mut self, now_ms: u64, max_entries: usize, max_bytes: u64) -> Vec<u64> {
        while let Some(&(not_before, seq)) = self.delayed.first() {
            if not_before > now_ms {
                break;
            }
            self.delayed.pop_first();
            self.ready.insert(seq);
        }
        let mut picked = Vec::new();
        let mut total = 0u64;
        for &seq in &self.ready {
            if picked.len() >= max_entries {
                break;
            }
            let bytes = self.pending.get(&seq).map_or(0, |p| p.bytes);
            if !picked.is_empty() && total + bytes > max_bytes {
                break;
            }
            total += bytes;
            picked.push(seq);
        }
        for seq in &picked {
            self.ready.remove(seq);
        }
        picked
    }
}

/// The RocksDB-backed [`ForwardQueuePort`].
pub struct ForwardQueue {
    inner: Arc<Inner>,
}

struct Inner {
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
        let mut state = State::default();
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
                state.insert(
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
            inner: Arc::new(Inner {
                db,
                seq: AtomicU64::new(max_seq),
                bytes: AtomicU64::new(bytes),
                state: Mutex::new(state),
                notify: tokio::sync::Notify::new(),
            }),
        })
    }

    /// Run `f` on the blocking pool: it does RocksDB I/O.
    async fn blocking<T: Send + 'static>(
        &self,
        f: impl FnOnce(&Inner) -> Result<T, HyperbytedbError> + Send + 'static,
    ) -> Result<T, HyperbytedbError> {
        let inner = Arc::clone(&self.inner);
        tokio::task::spawn_blocking(move || f(&inner))
            .await
            .map_err(|e| internal("task", e))?
    }
}

impl Inner {
    fn lock(&self) -> std::sync::MutexGuard<'_, State> {
        self.state.lock().unwrap_or_else(|e| e.into_inner())
    }

    fn enqueue(&self, entry: &ForwardEntry) -> Result<u64, HyperbytedbError> {
        self.write(&[], Some(entry))
            .map(|seq| seq.unwrap_or_default())
    }

    /// Delete `done` and store `entry`, in one RocksDB write; then update the
    /// indexes under one lock. Returns the new entry's sequence.
    fn write(
        &self,
        done: &[u64],
        entry: Option<&ForwardEntry>,
    ) -> Result<Option<u64>, HyperbytedbError> {
        let cf = self
            .db
            .cf_handle(FORWARD_QUEUE_CF)
            .ok_or_else(|| internal("write", "column family missing"))?;
        let mut batch = rocksdb::WriteBatch::default();
        for seq in done {
            batch.delete_cf(&cf, key(*seq));
        }
        let added = match entry {
            Some(entry) => {
                let value = bincode::serialize(entry).map_err(|e| internal("encode", e))?;
                let seq = self.seq.fetch_add(1, Ordering::SeqCst) + 1;
                batch.put_cf(&cf, key(seq), &value);
                Some((seq, entry, value.len() as u64))
            }
            None => None,
        };
        self.db.write(batch).map_err(|e| internal("write", e))?;
        let mut st = self.lock();
        for &seq in done {
            if let Some(bytes) = st.remove(seq) {
                self.bytes.fetch_sub(bytes, Ordering::Relaxed);
            }
        }
        let Some((seq, entry, bytes)) = added else {
            return Ok(None);
        };
        self.bytes.fetch_add(bytes, Ordering::Relaxed);
        st.insert(
            seq,
            Pending {
                enqueued_ms: entry.enqueued_ms,
                not_before_ms: entry.not_before_ms,
                bytes,
            },
        );
        drop(st);
        self.notify.notify_one();
        Ok(Some(seq))
    }

    fn claim(
        &self,
        now_ms: u64,
        max_entries: usize,
        max_bytes: u64,
    ) -> Result<Vec<(u64, ForwardEntry)>, HyperbytedbError> {
        let seqs = self.lock().claim(now_ms, max_entries, max_bytes);
        let cf = self
            .db
            .cf_handle(FORWARD_QUEUE_CF)
            .ok_or_else(|| internal("claim", "column family missing"))?;
        let mut out = Vec::with_capacity(seqs.len());
        for &seq in &seqs {
            match self.db.get_cf(&cf, key(seq)) {
                Ok(Some(v)) => match bincode::deserialize::<ForwardEntry>(&v) {
                    Ok(e) => out.push((seq, e)),
                    Err(e) => {
                        tracing::error!(seq, error = %e, "undecodable forward queue entry; dropping");
                        self.write(&[seq], None)?;
                    }
                },
                // Gone from disk: nothing left to send.
                Ok(None) => self.forget(seq),
                Err(e) => {
                    // The caller gets none of this claim, so all of it goes
                    // back (entries already dropped above are no-ops).
                    let mut st = self.lock();
                    for &s in &seqs {
                        st.unclaim(s, 0);
                    }
                    return Err(internal("get", e));
                }
            }
        }
        Ok(out)
    }

    fn forget(&self, seq: u64) {
        if let Some(bytes) = self.lock().remove(seq) {
            self.bytes.fetch_sub(bytes, Ordering::Relaxed);
        }
    }
}

#[async_trait]
impl ForwardQueuePort for ForwardQueue {
    async fn enqueue(&self, entry: ForwardEntry) -> Result<u64, HyperbytedbError> {
        self.blocking(move |q| q.enqueue(&entry)).await
    }

    async fn claim(
        &self,
        now_ms: u64,
        max_entries: usize,
        max_bytes: u64,
    ) -> Result<Vec<(u64, ForwardEntry)>, HyperbytedbError> {
        self.blocking(move |q| q.claim(now_ms, max_entries, max_bytes))
            .await
    }

    async fn complete(
        &self,
        seqs: Vec<u64>,
        retry: Option<ForwardEntry>,
    ) -> Result<(), HyperbytedbError> {
        self.blocking(move |q| q.write(&seqs, retry.as_ref()).map(|_| ()))
            .await
    }

    fn release(&self, seqs: &[u64], not_before_ms: u64) {
        let mut st = self.inner.lock();
        for &seq in seqs {
            st.unclaim(seq, not_before_ms);
        }
    }

    fn oldest_age_ms(&self, now_ms: u64) -> u64 {
        self.inner
            .lock()
            .by_age
            .first()
            .map_or(0, |&(t, _)| now_ms.saturating_sub(t))
    }

    fn len(&self) -> usize {
        self.inner.lock().pending.len()
    }

    fn bytes(&self) -> u64 {
        self.inner.bytes.load(Ordering::Relaxed)
    }

    async fn notified(&self) {
        self.inner.notify.notified().await;
    }
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

    fn seqs(claimed: &[(u64, ForwardEntry)]) -> Vec<u64> {
        claimed.iter().map(|(s, _)| *s).collect()
    }

    #[tokio::test]
    async fn entries_are_claimed_in_order_once_and_survive_until_completed() {
        let dir = tempfile::tempdir().unwrap();
        let q = open(dir.path());
        let a = q.enqueue(entry(100, 0, b"a")).await.unwrap();
        let b = q.enqueue(entry(200, 0, b"b")).await.unwrap();
        let c = q.enqueue(entry(300, 10_000, b"c")).await.unwrap();

        let first = q.claim(1_000, 10, u64::MAX).await.unwrap();
        assert_eq!(seqs(&first), vec![a, b], "c is not due");
        assert!(
            q.claim(1_000, 10, u64::MAX).await.unwrap().is_empty(),
            "claimed entries are not handed out twice"
        );
        assert_eq!(q.oldest_age_ms(1_000), 900);

        q.complete(vec![a], None).await.unwrap();
        assert_eq!(q.oldest_age_ms(1_000), 800);
        assert_eq!(q.len(), 2);
        assert_eq!(seqs(&q.claim(20_000, 10, u64::MAX).await.unwrap()), vec![c]);
    }

    /// Due entries come out in sequence order, whether they were due all
    /// along or only once their backoff ended, as when the queue scanned.
    #[tokio::test]
    async fn due_entries_come_out_in_sequence_order() {
        let dir = tempfile::tempdir().unwrap();
        let q = open(dir.path());
        let a = q.enqueue(entry(1, 0, b"a")).await.unwrap();
        let b = q.enqueue(entry(2, 500, b"b")).await.unwrap();
        let c = q.enqueue(entry(3, 0, b"c")).await.unwrap();
        assert_eq!(
            seqs(&q.claim(600, 10, u64::MAX).await.unwrap()),
            vec![a, b, c]
        );
    }

    #[tokio::test]
    async fn a_released_entry_waits_out_its_backoff() {
        let dir = tempfile::tempdir().unwrap();
        let q = open(dir.path());
        let a = q.enqueue(entry(1, 0, b"a")).await.unwrap();
        let b = q.enqueue(entry(2, 0, b"b")).await.unwrap();
        assert_eq!(seqs(&q.claim(10, 1, u64::MAX).await.unwrap()), vec![a]);
        q.release(&[a], 1_000);
        assert_eq!(seqs(&q.claim(10, 10, u64::MAX).await.unwrap()), vec![b]);
        assert!(q.claim(999, 10, u64::MAX).await.unwrap().is_empty());
        assert_eq!(seqs(&q.claim(1_000, 10, u64::MAX).await.unwrap()), vec![a]);
        assert_eq!(q.len(), 2, "released and claimed entries stay stored");
    }

    /// A retry keeps the write's original time, so the queue's age is how
    /// far primaries trail, not how recently a retry was queued.
    #[tokio::test]
    async fn the_age_is_the_oldest_write_not_the_oldest_sequence() {
        let dir = tempfile::tempdir().unwrap();
        let q = open(dir.path());
        let fresh = q.enqueue(entry(5_000, 0, b"fresh")).await.unwrap();
        q.enqueue(entry(1_000, 9_000, b"retry")).await.unwrap();
        assert_eq!(q.oldest_age_ms(6_000), 5_000);
        q.complete(vec![fresh], None).await.unwrap();
        assert_eq!(
            q.oldest_age_ms(6_000),
            5_000,
            "the retry is still the oldest"
        );
        assert_eq!(q.bytes() > 0, !q.is_empty());
    }

    /// Completing a batch and queueing its retry is one write: the queue
    /// never holds both, and the retry keeps the write's original time.
    #[tokio::test]
    async fn completing_with_a_retry_replaces_the_entries() {
        let dir = tempfile::tempdir().unwrap();
        let q = open(dir.path());
        q.enqueue(entry(100, 0, b"a")).await.unwrap();
        q.enqueue(entry(200, 0, b"b")).await.unwrap();
        let batch = seqs(&q.claim(1_000, 10, u64::MAX).await.unwrap());
        q.complete(batch, Some(entry(100, 5_000, b"ab")))
            .await
            .unwrap();
        assert_eq!(q.len(), 1);
        assert_eq!(q.oldest_age_ms(1_000), 900);
        assert!(q.claim(4_999, 10, u64::MAX).await.unwrap().is_empty());
        let retry = q.claim(5_000, 10, u64::MAX).await.unwrap();
        assert_eq!(retry[0].1.body, b"ab".to_vec());
        drop(q);
        let q = open(dir.path());
        assert_eq!(q.len(), 1, "on disk too: the originals are gone");
    }

    #[tokio::test]
    async fn a_reopened_queue_keeps_what_was_not_completed() {
        let dir = tempfile::tempdir().unwrap();
        {
            let q = open(dir.path());
            let a = q.enqueue(entry(1, 0, b"a")).await.unwrap();
            q.enqueue(entry(2, 0, b"b")).await.unwrap();
            q.complete(vec![a], None).await.unwrap();
        }
        let q = open(dir.path());
        let got = q.claim(10, 10, u64::MAX).await.unwrap();
        assert_eq!(got.len(), 1);
        assert_eq!(got[0].1.body, b"b".to_vec());
        let next = q.enqueue(entry(3, 0, b"c")).await.unwrap();
        assert!(next > got[0].0, "sequence resumes past what is on disk");
    }

    #[tokio::test]
    async fn a_claim_stops_at_the_byte_budget_but_takes_at_least_one() {
        let dir = tempfile::tempdir().unwrap();
        let q = open(dir.path());
        q.enqueue(entry(1, 0, &[0u8; 100])).await.unwrap();
        q.enqueue(entry(2, 0, &[0u8; 100])).await.unwrap();
        assert_eq!(q.claim(10, 10, 10).await.unwrap().len(), 1);
    }
}
