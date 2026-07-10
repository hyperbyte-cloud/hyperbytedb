//! Bounded queue with parallel workers for applying replicated line-protocol
//! batches to the WAL. WAL ordering is guaranteed by the atomic sequence in the
//! WAL implementation, so multiple workers can safely call `wal.append`
//! concurrently.

use bytes::Bytes;
use metrics::{counter, histogram};
use std::sync::Arc;
use tokio::sync::{Semaphore, mpsc, oneshot};

use crate::application::ingest_metadata::{
    IngestCardinalityLimits, IngestSchemaCache, prepare_batch_metadata,
};
use crate::application::line_protocol::parse_line_body_to_points_limited;
use crate::application::wal_append::append_points_with_prepared;
use crate::error::HyperbytedbError;
use crate::ports::points_sink::PointsSinkPort;
use crate::ports::wal::WalPort;

const DEFAULT_QUEUE_DEPTH: usize = 1024;
const DEFAULT_WORKERS: usize = 8;

pub use crate::application::line_protocol::parse_line_body_to_points;

pub enum ReplicationApplyError {
    QueueFull,
    ApplyFailed(String),
    WorkerGone,
}

struct ApplyJob {
    database: String,
    retention_policy: String,
    precision: Option<String>,
    body: Bytes,
    origin_node_id: u64,
    done: oneshot::Sender<Result<u64, String>>,
}

/// Bounded queue with a pool of parallel workers for applying replicated
/// writes. Each worker independently parses, prepares metadata, and appends
/// to the WAL. The WAL's `AtomicU64` sequence ensures monotonic ordering
/// regardless of which worker runs first.
pub struct ReplicationApplyQueue {
    tx: mpsc::Sender<ApplyJob>,
}

impl ReplicationApplyQueue {
    pub fn new(
        depth: usize,
        metadata: Arc<dyn crate::ports::metadata::MetadataPort>,
        wal: Arc<dyn WalPort>,
        limits: IngestCardinalityLimits,
    ) -> Arc<Self> {
        Self::with_sink(depth, metadata, wal, None, limits, 0)
    }

    pub fn with_sink(
        depth: usize,
        metadata: Arc<dyn crate::ports::metadata::MetadataPort>,
        wal: Arc<dyn WalPort>,
        sink: Option<Arc<dyn PointsSinkPort>>,
        limits: IngestCardinalityLimits,
        max_points_per_request: usize,
    ) -> Arc<Self> {
        Self::with_workers_and_sink(
            depth,
            DEFAULT_WORKERS,
            metadata,
            wal,
            sink,
            limits,
            max_points_per_request,
        )
    }

    pub fn with_workers(
        depth: usize,
        num_workers: usize,
        metadata: Arc<dyn crate::ports::metadata::MetadataPort>,
        wal: Arc<dyn WalPort>,
        limits: IngestCardinalityLimits,
    ) -> Arc<Self> {
        Self::with_workers_and_sink(depth, num_workers, metadata, wal, None, limits, 0)
    }

    pub fn with_workers_and_sink(
        depth: usize,
        num_workers: usize,
        metadata: Arc<dyn crate::ports::metadata::MetadataPort>,
        wal: Arc<dyn WalPort>,
        sink: Option<Arc<dyn PointsSinkPort>>,
        limits: IngestCardinalityLimits,
        max_points_per_request: usize,
    ) -> Arc<Self> {
        let depth = depth.max(1);
        let num_workers = num_workers.max(1);
        let (tx, rx) = mpsc::channel::<ApplyJob>(depth);
        let rx = Arc::new(tokio::sync::Mutex::new(rx));
        let sem = Arc::new(Semaphore::new(num_workers));
        let schema_cache = Arc::new(IngestSchemaCache::new());

        let dispatch_rx = rx.clone();
        let dispatch_sem = sem.clone();
        tokio::spawn(async move {
            loop {
                let job = {
                    let mut guard = dispatch_rx.lock().await;
                    match guard.recv().await {
                        Some(j) => j,
                        None => break,
                    }
                };

                let permit = match dispatch_sem.clone().acquire_owned().await {
                    Ok(p) => p,
                    Err(_) => break,
                };
                let meta = metadata.clone();
                let w = wal.clone();
                let sink = sink.clone();
                let sc = schema_cache.clone();
                tokio::spawn(async move {
                    let r = apply_batch(
                        &meta,
                        &w,
                        sink.as_ref(),
                        &job.database,
                        &job.retention_policy,
                        job.precision.as_deref(),
                        &job.body,
                        job.origin_node_id,
                        limits,
                        max_points_per_request,
                        &sc,
                    )
                    .await;
                    let _ = job.done.send(r.map_err(|e| e.to_string()));
                    drop(permit);
                });
            }
        });

        Arc::new(Self { tx })
    }

    pub fn with_defaults(
        metadata: Arc<dyn crate::ports::metadata::MetadataPort>,
        wal: Arc<dyn WalPort>,
    ) -> Arc<Self> {
        Self::new(
            DEFAULT_QUEUE_DEPTH,
            metadata,
            wal,
            IngestCardinalityLimits::default(),
        )
    }

    /// Non-blocking enqueue; await the returned receiver for WAL seq or error string.
    pub fn try_enqueue(
        &self,
        database: String,
        retention_policy: String,
        precision: Option<String>,
        body: Bytes,
        origin_node_id: u64,
    ) -> Result<oneshot::Receiver<Result<u64, String>>, ReplicationApplyError> {
        let (done, out) = oneshot::channel();
        let job = ApplyJob {
            database,
            retention_policy,
            precision,
            body,
            origin_node_id,
            done,
        };
        match self.tx.try_send(job) {
            Ok(()) => Ok(out),
            Err(mpsc::error::TrySendError::Full(_)) => Err(ReplicationApplyError::QueueFull),
            Err(mpsc::error::TrySendError::Closed(_)) => Err(ReplicationApplyError::WorkerGone),
        }
    }
}

#[allow(clippy::too_many_arguments)]
async fn apply_batch(
    metadata: &Arc<dyn crate::ports::metadata::MetadataPort>,
    wal: &Arc<dyn WalPort>,
    sink: Option<&Arc<dyn PointsSinkPort>>,
    database: &str,
    retention_policy: &str,
    precision: Option<&str>,
    body: &[u8],
    origin_node_id: u64,
    limits: IngestCardinalityLimits,
    max_points_per_request: usize,
    schema_cache: &IngestSchemaCache,
) -> Result<u64, HyperbytedbError> {
    let metrics_start = std::time::Instant::now();

    if body.is_empty() {
        return wal.last_sequence().await;
    }

    let t0 = std::time::Instant::now();
    let points = parse_line_body_to_points_limited(body, precision, max_points_per_request)?;
    histogram!("hyperbytedb_replication_apply_parse_seconds").record(t0.elapsed().as_secs_f64());
    if points.is_empty() {
        return wal.last_sequence().await;
    }

    let t1 = std::time::Instant::now();
    if let Err(e) = prepare_batch_metadata(
        metadata,
        database,
        retention_policy,
        &points,
        limits,
        Some(schema_cache),
    )
    .await
    {
        if let HyperbytedbError::FieldTypeConflict {
            ref field,
            ref measurement,
            ref got,
            ref expected,
        } = e
        {
            tracing::warn!(
                measurement = %measurement,
                field = %field,
                got = %got,
                expected = %expected,
                origin_node_id,
                "replicate apply field type conflict"
            );
        }
        counter!("hyperbytedb_replication_apply_errors_total").increment(1);
        return Err(e);
    }
    histogram!("hyperbytedb_replication_apply_metadata_seconds").record(t1.elapsed().as_secs_f64());

    let t2 = std::time::Instant::now();
    let result = append_points_with_prepared(
        wal.as_ref(),
        sink,
        database,
        retention_policy,
        points,
        origin_node_id,
        max_points_per_request,
    )
    .await;
    histogram!("hyperbytedb_replication_apply_wal_seconds").record(t2.elapsed().as_secs_f64());

    histogram!("hyperbytedb_replication_apply_seconds")
        .record(metrics_start.elapsed().as_secs_f64());
    match &result {
        Ok(_) => {
            counter!("hyperbytedb_replication_apply_total").increment(1);
        }
        Err(_) => {
            counter!("hyperbytedb_replication_apply_errors_total").increment(1);
        }
    }
    result
}
