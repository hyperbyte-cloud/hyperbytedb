//! Group-commit WAL wrapper that coalesces multiple [`WalPort::append`] calls
//! into a single RocksDB `WriteBatch`, dramatically improving throughput under
//! concurrent write load.

use std::sync::Arc;
use std::sync::mpsc::{self, SyncSender, TrySendError};
use std::thread::{self, JoinHandle};
use std::time::{Duration, Instant};

use async_trait::async_trait;
use metrics::histogram;
use tokio::sync::oneshot;

use crate::adapters::wal::rocksdb_wal::RocksDbWal;
use crate::application::system_trace;
use crate::domain::prepared_wal::PreparedWalSlot;
use crate::error::HyperbytedbError;
use crate::ports::wal::{WalAppendBundle, WalEntry, WalPort};

struct BatchRequest {
    bundle: WalAppendBundle,
    enqueued_at: Instant,
    tx: oneshot::Sender<Result<u64, HyperbytedbError>>,
}

pub struct BatchingWal {
    sender: SyncSender<BatchRequest>,
    _writer: JoinHandle<()>,
    inner: Arc<RocksDbWal>,
}

impl BatchingWal {
    pub fn new(
        inner: Arc<RocksDbWal>,
        channel_depth: usize,
        max_batch: usize,
        max_delay: Duration,
    ) -> Arc<Self> {
        if !max_delay.is_zero() {
            tracing::debug!(
                ?max_delay,
                "BatchingWal: max_delay is ignored — drain-only coalescing in use"
            );
        }
        let (sync_tx, sync_rx) = mpsc::sync_channel(channel_depth.max(1));
        let wal = inner.clone();
        let writer = thread::Builder::new()
            .name("hyperbytedb-wal-writer".into())
            .spawn(move || Self::writer_loop(sync_rx, wal, max_batch))
            .unwrap_or_else(|e| panic!("spawn hyperbytedb-wal-writer thread: {e}"));

        Arc::new(Self {
            sender: sync_tx,
            _writer: writer,
            inner,
        })
    }

    fn writer_loop(rx: mpsc::Receiver<BatchRequest>, wal: Arc<RocksDbWal>, max_batch: usize) {
        let mut batch: Vec<BatchRequest> = Vec::with_capacity(max_batch);

        loop {
            batch.clear();

            let first = match rx.recv() {
                Ok(r) => r,
                Err(_) => break,
            };

            let t_first = Instant::now();
            histogram!("hyperbytedb_wal_batcher_queue_wait_seconds")
                .record(t_first.duration_since(first.enqueued_at).as_secs_f64());
            batch.push(first);

            while batch.len() < max_batch {
                match rx.try_recv() {
                    Ok(req) => batch.push(req),
                    Err(mpsc::TryRecvError::Empty) => break,
                    Err(mpsc::TryRecvError::Disconnected) => break,
                }
            }

            let t_coalesce_end = Instant::now();
            let coalesce_elapsed = t_coalesce_end.duration_since(t_first);
            histogram!("hyperbytedb_wal_batcher_coalesce_seconds")
                .record(coalesce_elapsed.as_secs_f64());
            histogram!("hyperbytedb_wal_batcher_batch_size").record(batch.len() as f64);

            let queue_wait_us = t_first.duration_since(batch[0].enqueued_at).as_micros() as u64;
            let batch_len = batch.len();
            let mut bundles = Vec::with_capacity(batch_len);
            let mut responses =
                Vec::<(Instant, oneshot::Sender<Result<u64, HyperbytedbError>>)>::with_capacity(
                    batch_len,
                );
            for req in batch.drain(..) {
                bundles.push(req.bundle);
                responses.push((req.enqueued_at, req.tx));
            }
            let result = wal.append_bundle_batch_sync(bundles);

            let t_write_end = Instant::now();
            let write_elapsed = t_write_end.duration_since(t_coalesce_end);
            histogram!("hyperbytedb_wal_batcher_write_seconds").record(write_elapsed.as_secs_f64());

            match &result {
                Ok(seqs) if !seqs.is_empty() => {
                    system_trace::log_wal_batch(
                        batch_len,
                        queue_wait_us,
                        coalesce_elapsed.as_micros() as u64,
                        write_elapsed.as_micros() as u64,
                        seqs[0],
                        *seqs.last().unwrap_or(&seqs[0]),
                    );
                }
                _ => {}
            }

            let now = Instant::now();
            match result {
                Ok(seqs) => {
                    for ((enqueued_at, tx), seq) in responses.into_iter().zip(seqs) {
                        histogram!("hyperbytedb_wal_batcher_response_seconds")
                            .record(now.duration_since(enqueued_at).as_secs_f64());
                        let _ = tx.send(Ok(seq));
                    }
                }
                Err(e) => {
                    let msg = e.to_string();
                    for (enqueued_at, tx) in responses {
                        histogram!("hyperbytedb_wal_batcher_response_seconds")
                            .record(now.duration_since(enqueued_at).as_secs_f64());
                        let _ = tx.send(Err(HyperbytedbError::Wal(msg.clone())));
                    }
                }
            }
        }
    }

    async fn enqueue(&self, bundle: WalAppendBundle) -> Result<u64, HyperbytedbError> {
        let (tx, rx) = oneshot::channel();
        let mut req = BatchRequest {
            bundle,
            enqueued_at: Instant::now(),
            tx,
        };

        loop {
            match self.sender.try_send(req) {
                Ok(()) => break,
                Err(TrySendError::Full(pending)) => {
                    tokio::task::yield_now().await;
                    req = pending;
                }
                Err(TrySendError::Disconnected(_)) => {
                    return Err(HyperbytedbError::Wal("WAL batcher channel closed".into()));
                }
            }
        }

        rx.await
            .map_err(|_| HyperbytedbError::Wal("WAL batcher dropped response".into()))?
    }
}

#[async_trait]
impl WalPort for BatchingWal {
    async fn append(&self, entry: WalEntry) -> Result<u64, HyperbytedbError> {
        self.append_bundle(WalAppendBundle {
            entry,
            prepared: None,
        })
        .await
    }

    async fn append_bundle(&self, bundle: WalAppendBundle) -> Result<u64, HyperbytedbError> {
        self.enqueue(bundle).await
    }

    fn arrow_wal_enabled(&self) -> bool {
        self.inner.arrow_wal_enabled()
    }

    async fn take_prepared_range(
        &self,
        from: u64,
        to_inclusive: u64,
        max_entries: usize,
    ) -> Result<Option<Vec<(u64, PreparedWalSlot)>>, HyperbytedbError> {
        self.inner
            .take_prepared_range(from, to_inclusive, max_entries)
            .await
    }

    async fn next_prepared_seq(&self, from: u64) -> Result<Option<u64>, HyperbytedbError> {
        self.inner.next_prepared_seq(from).await
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
        assert_eq!(got_entry.points.len(), 1);
        assert_eq!(got_entry.database, "replica_check");
        assert_eq!(got_entry.origin_node_id, 99);
    }
}
