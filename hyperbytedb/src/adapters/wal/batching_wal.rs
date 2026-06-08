//! Group-commit WAL wrapper that coalesces multiple [`WalPort::append`] calls
//! into a single RocksDB `WriteBatch`, dramatically improving throughput under
//! concurrent write load.
//!
//! The caller sees the same `WalPort` interface; internally a bounded channel
//! feeds a background task that, on each pass, blocks until at least one
//! request arrives and then non-blockingly drains everything else already
//! queued (up to `max_batch`) before issuing one combined write.
//!
//! # Why no timer-based coalesce window
//!
//! An earlier version waited up to `max_delay` (e.g. 200 µs) for additional
//! requests after the first arrived, using `tokio::time::timeout_at`. In
//! practice this added **1.5–2.5 ms** to every WAL append because Tokio's
//! timer wheel runs at ~1 ms granularity and fires only when the runtime
//! services the timer task — sub-millisecond deadlines are routinely missed
//! by 1–3 ms under load.
//!
//! The drain-only approach gives equivalent batching under sustained load
//! (requests pile up while the writer is busy) without paying the timer tax
//! when the queue is empty. `max_delay` is retained in the constructor only
//! for backward compatibility with the existing `[flush]` config knobs and
//! is now logged as ignored.

use std::sync::Arc;
use std::time::Instant;

use async_trait::async_trait;
use metrics::histogram;
use tokio::sync::{mpsc, oneshot};

use crate::adapters::wal::rocksdb_wal::RocksDbWal;
use crate::application::system_trace;
use crate::error::HyperbytedbError;
use crate::ports::wal::{WalEntry, WalPort};

struct BatchRequest {
    entry: WalEntry,
    enqueued_at: Instant,
    tx: oneshot::Sender<Result<u64, HyperbytedbError>>,
}

pub struct BatchingWal {
    sender: mpsc::Sender<BatchRequest>,
    inner: Arc<RocksDbWal>,
}

impl BatchingWal {
    /// Create a new `BatchingWal` wrapping `inner`.
    ///
    /// * `channel_depth` – bounded channel size (back-pressure threshold).
    /// * `max_batch`     – max entries to coalesce into one `WriteBatch`.
    /// * `max_delay`     – **ignored**. Retained for API compatibility; see the
    ///   module-level docs for why a timer-based coalesce window is harmful
    ///   on this hot path.
    pub fn new(
        inner: Arc<RocksDbWal>,
        channel_depth: usize,
        max_batch: usize,
        max_delay: std::time::Duration,
    ) -> Arc<Self> {
        if !max_delay.is_zero() {
            tracing::debug!(
                ?max_delay,
                "BatchingWal: max_delay is ignored — drain-only coalescing in use"
            );
        }
        let (tx, rx) = mpsc::channel(channel_depth);
        let wal = Arc::new(Self {
            sender: tx,
            inner: inner.clone(),
        });

        tokio::spawn(Self::batcher_loop(rx, inner, max_batch));

        wal
    }

    async fn batcher_loop(
        mut rx: mpsc::Receiver<BatchRequest>,
        wal: Arc<RocksDbWal>,
        max_batch: usize,
    ) {
        let mut batch: Vec<BatchRequest> = Vec::with_capacity(max_batch);

        loop {
            batch.clear();

            let first = match rx.recv().await {
                Some(r) => r,
                None => break,
            };

            // Time the first request waited in the mpsc channel before the
            // batcher picked it up. This isolates "queue depth" cost from
            // batching cost in the metric set.
            let t_first = Instant::now();
            histogram!("hyperbytedb_wal_batcher_queue_wait_seconds")
                .record(t_first.duration_since(first.enqueued_at).as_secs_f64());
            batch.push(first);

            // Greedily drain everything already in the channel — non-blocking.
            // Under sustained load this naturally forms batches of up to
            // `max_batch` (requests pile up while the previous write was in
            // flight). When the queue is empty we proceed immediately rather
            // than waiting on a timer, because Tokio cannot honour
            // sub-millisecond deadlines reliably.
            while batch.len() < max_batch {
                match rx.try_recv() {
                    Ok(req) => batch.push(req),
                    Err(_) => break,
                }
            }

            let t_coalesce_end = Instant::now();
            let coalesce_elapsed = t_coalesce_end.duration_since(t_first);
            histogram!("hyperbytedb_wal_batcher_coalesce_seconds")
                .record(coalesce_elapsed.as_secs_f64());
            histogram!("hyperbytedb_wal_batcher_batch_size").record(batch.len() as f64);

            let queue_wait_us = t_first.duration_since(batch[0].enqueued_at).as_micros() as u64;

            // Move entries out of each request instead of cloning. Saves a
            // full `Vec<Point>` clone per request — at batch_size=64 with
            // hundreds of points each, that's the difference between
            // hundreds of µs of memcpy and effectively zero.
            let entries: Vec<WalEntry> = batch
                .iter_mut()
                .map(|r| std::mem::take(&mut r.entry))
                .collect();
            let result = wal.append_batch(entries).await;

            let t_write_end = Instant::now();
            let write_elapsed = t_write_end.duration_since(t_coalesce_end);
            histogram!("hyperbytedb_wal_batcher_write_seconds").record(write_elapsed.as_secs_f64());

            match &result {
                Ok(seqs) if !seqs.is_empty() => {
                    system_trace::log_wal_batch(
                        batch.len(),
                        queue_wait_us,
                        coalesce_elapsed.as_micros() as u64,
                        write_elapsed.as_micros() as u64,
                        seqs[0],
                        *seqs.last().unwrap_or(&seqs[0]),
                    );
                }
                _ => {}
            }

            match result {
                Ok(seqs) => {
                    for (req, seq) in batch.drain(..).zip(seqs) {
                        let _ = req.tx.send(Ok(seq));
                    }
                }
                Err(e) => {
                    let msg = e.to_string();
                    for req in batch.drain(..) {
                        let _ = req.tx.send(Err(HyperbytedbError::Wal(msg.clone())));
                    }
                }
            }
        }
    }
}

#[async_trait]
impl WalPort for BatchingWal {
    async fn append(&self, entry: WalEntry) -> Result<u64, HyperbytedbError> {
        let (tx, rx) = oneshot::channel();
        self.sender
            .send(BatchRequest {
                entry,
                enqueued_at: Instant::now(),
                tx,
            })
            .await
            .map_err(|_| HyperbytedbError::Wal("WAL batcher channel closed".into()))?;
        rx.await
            .map_err(|_| HyperbytedbError::Wal("WAL batcher dropped response".into()))?
    }

    async fn read_from(&self, sequence: u64) -> Result<Vec<(u64, WalEntry)>, HyperbytedbError> {
        self.inner.read_from(sequence).await
    }

    async fn read_range(
        &self,
        from: u64,
        max_entries: usize,
    ) -> Result<Vec<(u64, WalEntry)>, HyperbytedbError> {
        self.inner.read_range(from, max_entries).await
    }

    async fn truncate_before(&self, sequence: u64) -> Result<(), HyperbytedbError> {
        self.inner.truncate_before(sequence).await
    }

    async fn last_sequence(&self) -> Result<u64, HyperbytedbError> {
        self.inner.last_sequence().await
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;
    use std::time::Duration;

    use tempfile::TempDir;

    use crate::adapters::wal::rocksdb_wal::RocksDbWal;
    use crate::domain::point::{FieldValue, Point};
    use crate::domain::wal::WalEntry;
    use crate::ports::wal::WalPort;

    use super::BatchingWal;

    fn marker_entry() -> WalEntry {
        let mut tags = std::collections::BTreeMap::new();
        tags.insert("host".to_string(), "manual".to_string());
        let mut fields = std::collections::BTreeMap::new();
        fields.insert("v".to_string(), FieldValue::Float(99.0));
        WalEntry {
            database: "replica_check".into(),
            retention_policy: "autogen".into(),
            points: vec![Point {
                measurement: "marker".into(),
                tags,
                fields,
                timestamp: 1_700_000_000_000_000_000,
            }],
            origin_node_id: 99,
        }
    }

    #[tokio::test]
    async fn batching_wal_round_trip_preserves_points_single() {
        let tmp = TempDir::new().unwrap();
        let raw = Arc::new(RocksDbWal::open(tmp.path()).unwrap());
        let wal = BatchingWal::new(raw, 256, 64, Duration::from_micros(0));

        let seq = wal.append(marker_entry()).await.unwrap();
        let read_back = wal.read_range(seq, 16).await.unwrap();

        assert_eq!(read_back.len(), 1, "exactly one entry written");
        let (got_seq, got_entry) = &read_back[0];
        assert_eq!(*got_seq, seq);
        assert_eq!(
            got_entry.points.len(),
            1,
            "round-trip dropped the points: got entry={got_entry:?}"
        );
        assert_eq!(got_entry.database, "replica_check");
        assert_eq!(got_entry.origin_node_id, 99);
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn batching_wal_round_trip_preserves_points_under_concurrency() {
        let tmp = TempDir::new().unwrap();
        let raw = Arc::new(RocksDbWal::open(tmp.path()).unwrap());
        let wal = BatchingWal::new(raw, 256, 64, Duration::from_micros(0));

        let mut handles = Vec::new();
        for _ in 0..32 {
            let wal = wal.clone();
            handles.push(tokio::spawn(async move {
                wal.append(marker_entry()).await.unwrap()
            }));
        }
        let mut seqs = Vec::new();
        for h in handles {
            seqs.push(h.await.unwrap());
        }
        seqs.sort_unstable();

        let read_back = wal.read_range(0, 1024).await.unwrap();
        assert_eq!(read_back.len(), seqs.len());
        for (s, e) in &read_back {
            assert_eq!(
                e.points.len(),
                1,
                "concurrent batched append lost points at seq={s}: entry={e:?}"
            );
        }
    }
}
