use std::sync::Arc;

use crate::adapters::cluster::forward_queue::{self, ForwardEntry, ForwardQueue};
use crate::application::stage_timer::StageTimer;
use async_trait::async_trait;
use metrics::{counter, histogram};

#[cfg(feature = "columnar-ingest")]
use crate::application::ingest_metadata::prepare_columnar_metadata;
use crate::application::ingest_metadata::{
    IngestCardinalityLimits, IngestSchemaCache, prepare_batch_metadata, validate_point_count,
};
use crate::application::line_protocol::{
    encode_points_to_line_protocol, parse_line_body_to_points_limited,
};
use crate::application::msgpack_ingest::parse_msgpack_body_to_points_limited;
use crate::application::replication_dispatch::dispatch_outbound_replication;
use crate::application::shard_routing::{
    ShardRoutingContext, ensure_measurement_bootstrapped, forward_shard_writes_by_node,
    partition_points, region_replication_targets,
};
use crate::application::wal_append::{
    ColumnarWalAppend, append_columnar_with_prepared, append_points_with_prepared,
};
use crate::config::{ReplicationConfig, ReplicationMode};
use crate::domain::database::Precision;
use crate::error::HyperbytedbError;
use crate::ports::ingestion::{IngestionPort, WritePayloadFormat};
use crate::ports::points_sink::PointsSinkPort;
use crate::ports::replication::{OutboundReplicationBatch, ReplicationPort};
use crate::ports::sharding::ShardMapPort;
use crate::ports::wal::WalPort;

/// Ingestion service for clustered (master-master) mode. Writes are always
/// appended to the local WAL first; replication then dispatches based on the
/// per-node `ReplicationMode`:
///
/// - [`ReplicationMode::Async`]: fire-and-forget HTTP fan-out (current
///   default behavior). Returns to the client immediately after the local WAL
///   append.
/// - [`ReplicationMode::SyncQuorum`]: HTTP fan-out + await W-of-N peer acks
///   before returning to the client. `W = min_acks.resolve(active_peers)` —
///   self is never counted toward the quorum.
pub struct PeerIngestionService {
    wal: Arc<dyn WalPort>,
    sink: Option<Arc<dyn PointsSinkPort>>,
    metadata: Arc<dyn crate::ports::metadata::MetadataPort>,
    replication_port: Arc<dyn ReplicationPort>,
    node_id: u64,
    limits: IngestCardinalityLimits,
    max_points_per_request: usize,
    schema_cache: IngestSchemaCache,
    replication: ReplicationConfig,
    shard_routing: Option<Arc<ShardRoutingContext>>,
    is_raft_leader: Arc<dyn Fn() -> bool + Send + Sync>,
    raft_leader_addr: Arc<dyn Fn() -> Option<String> + Send + Sync>,
    /// Durable queue of forwards owed to other primaries; `None` forwards
    /// every write synchronously. See [`ForwardQueue`].
    forward_queue: Option<Arc<ForwardQueue>>,
}

/// Times a coordinator resends regions a primary refused at capacity before
/// giving up on the client write.
const CAPACITY_RETRIES: u32 = 4;

/// Wait before capacity retry `attempt` (0-based): 25 ms doubling, with
/// jitter so coordinators refused together do not come back together.
fn capacity_backoff(attempt: u32) -> std::time::Duration {
    let base = 25u64 << attempt.min(4);
    let jitter = u64::from(
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|d| d.subsec_nanos())
            .unwrap_or(0),
    ) % (base / 2 + 1);
    std::time::Duration::from_millis(base / 2 + jitter)
}

/// Queued forward batches delivered at once by the drainer.
const FORWARD_DRAIN_CONCURRENCY: usize = 8;
/// Most queue entries (client writes' forwarded shares) per drained batch.
///
/// Kept small on purpose. A batch goes out as one forward per primary, and
/// primaries admit forwarded writes by size (64 KiB per write slot). At 256
/// entries a forward to one node was ~2 MB, costing most of that node's
/// admission budget, so it was refused unless the node was idle: 822k
/// regions refused "at capacity" in one run on n3, the queue lagged by tens
/// of seconds, and most writes fell back to synchronous forwarding.
const FORWARD_DRAIN_MAX_ENTRIES: usize = 16;
const FORWARD_DRAIN_MAX_BYTES: u64 = 1024 * 1024;
/// Delivery attempts before a queued forward is dropped.
const FORWARD_MAX_ATTEMPTS: u32 = 100;

/// Retry delay. Capped well under the lag limit: a retry keeps its original
/// timestamp, so a long backoff alone would hold the queue's age over the
/// limit and push every new write back to synchronous forwarding.
fn forward_backoff_ms(attempts: u32) -> u64 {
    (50u64 << attempts.min(5)).min(1_000)
}

impl PeerIngestionService {
    pub fn new(
        wal: Arc<dyn WalPort>,
        metadata: Arc<dyn crate::ports::metadata::MetadataPort>,
        replication_port: Arc<dyn ReplicationPort>,
        node_id: u64,
        limits: IngestCardinalityLimits,
    ) -> Self {
        Self::with_replication(
            wal,
            metadata,
            replication_port,
            node_id,
            limits,
            0,
            ReplicationConfig::default(),
        )
    }

    pub fn with_replication(
        wal: Arc<dyn WalPort>,
        metadata: Arc<dyn crate::ports::metadata::MetadataPort>,
        replication_port: Arc<dyn ReplicationPort>,
        node_id: u64,
        limits: IngestCardinalityLimits,
        max_points_per_request: usize,
        replication: ReplicationConfig,
    ) -> Self {
        Self::with_replication_and_sink(
            wal,
            None,
            metadata,
            replication_port,
            node_id,
            limits,
            max_points_per_request,
            replication,
        )
    }

    #[allow(clippy::too_many_arguments)]
    pub fn with_replication_and_sink(
        wal: Arc<dyn WalPort>,
        sink: Option<Arc<dyn PointsSinkPort>>,
        metadata: Arc<dyn crate::ports::metadata::MetadataPort>,
        replication_port: Arc<dyn ReplicationPort>,
        node_id: u64,
        limits: IngestCardinalityLimits,
        max_points_per_request: usize,
        replication: ReplicationConfig,
    ) -> Self {
        // Surface the resolved coordinator mode for dashboards. We set ALL
        // mode-labeled gauges so a dashboard query like `sum by (mode)` sees
        // a 0 for the inactive modes after a flip rather than a stale 1.
        for mode in [ReplicationMode::Async, ReplicationMode::SyncQuorum] {
            let v = if replication.mode == mode { 1.0 } else { 0.0 };
            metrics::gauge!("hyperbytedb_replication_mode", "mode" => mode.as_str()).set(v);
        }

        Self {
            wal,
            sink,
            metadata,
            replication_port,
            node_id,
            limits,
            max_points_per_request,
            schema_cache: IngestSchemaCache::new(),
            replication,
            shard_routing: None,
            is_raft_leader: Arc::new(|| false),
            raft_leader_addr: Arc::new(|| None),
            forward_queue: None,
        }
    }

    /// Acknowledge writes once their forwarded share is in `queue`; see
    /// `ShardingConfig::async_forward`. Call [`Self::spawn_forward_drainer`]
    /// to deliver it.
    pub fn with_forward_queue(mut self, queue: Arc<ForwardQueue>) -> Self {
        self.forward_queue = Some(queue);
        self
    }

    pub fn with_sharding(
        mut self,
        ctx: Arc<ShardRoutingContext>,
        is_leader: Arc<dyn Fn() -> bool + Send + Sync>,
        leader_addr: Arc<dyn Fn() -> Option<String> + Send + Sync>,
    ) -> Self {
        self.shard_routing = Some(ctx);
        self.is_raft_leader = is_leader;
        self.raft_leader_addr = leader_addr;
        self
    }

    async fn ingest_sharded_points(
        &self,
        db: &str,
        rp: &str,
        precision: Option<&str>,
        points: Vec<crate::domain::point::Point>,
    ) -> Result<(), HyperbytedbError> {
        let ctx = self.shard_routing.as_ref().ok_or_else(|| {
            HyperbytedbError::Internal("sharding context missing on ingest path".into())
        })?;
        let mut stage = StageTimer::new("hyperbytedb_sharded_write_stage_seconds");
        let mut measurements = std::collections::HashSet::new();
        for p in &points {
            // contains-then-insert avoids a String allocation per point.
            if !measurements.contains(p.measurement.as_str()) {
                measurements.insert(p.measurement.clone());
            }
        }

        // Reject direct writes to materialized view destinations before any
        // WAL append — same guard as the non-sharded path. This was
        // previously skipped on the sharded path, allowing writes that the
        // flush pipeline cannot serve (stuck-flush liveness gaps).
        for meas in &measurements {
            if let Some(meta) = self.metadata.get_measurement(db, rp, meas).await?
                && meta.materialized
                && meta.materialized_rp.as_deref().is_none_or(|mr| mr == rp)
            {
                return Err(HyperbytedbError::QueryParse(format!(
                    "cannot write directly to materialized view destination \"{meas}\""
                )));
            }
        }

        let leader = (self.raft_leader_addr)();
        for meas in &measurements {
            ensure_measurement_bootstrapped(
                ctx,
                db,
                rp,
                meas,
                (self.is_raft_leader)(),
                leader.as_deref(),
            )
            .await?;
        }

        stage.lap("validate");
        let buckets = tracing::Instrument::instrument(
            partition_points(ctx, db, rp, points),
            tracing::debug_span!(target: crate::telemetry::TARGET, "partition"),
        )
        .await?;
        let precision_val = Precision::from_str_opt(precision);
        stage.lap("partition");

        if self.queue_forwards(ctx, db, rp, precision, &buckets.forward) {
            // Acknowledged once the forwarded share is durable here; the
            // drainer delivers it to the primaries.
            stage.lap("queue_forwards");
            return self
                .append_local_buckets(ctx, db, rp, precision, precision_val, buckets.local)
                .await;
        }

        // Forward every non-local region bucket concurrently with bounded
        // parallelism: a batch spanning many regions must not pay serial
        // HTTP round-trips.
        //
        // Every bucket is attempted, and the local append below runs whatever
        // the forwards did. This used to abort on the first error, which made a
        // multi-region write fail in the worst possible way: the buckets that
        // had already been forwarded stayed durable, the local bucket was
        // discarded even though it would have succeeded, and the client was
        // told the whole write failed. A write reported as failed that left
        // rows behind is what produced the 24,538 "duplicated rows" reading in
        // the split-write suite — 24,538 points stored whose batches returned
        // non-204. There is no distributed transaction available here, so a
        // partial write is still possible; what changes is that it is as small
        // as we can make it and the error says so.
        // One request per primary node, not per region; see
        // `forward_shard_writes_by_node`.
        //
        // The local bucket is appended while the forwards are in flight, not
        // after them: a write used to pay for its slowest forward and then for
        // its own WAL append in series (259 ms + 26 ms at N=12 on n3).
        let forward_region_count = buckets.forward.len();
        let forwards = async {
            let start = std::time::Instant::now();
            let results = tracing::Instrument::instrument(
                forward_shard_writes_by_node(ctx, db, rp, precision, &buckets.forward),
                tracing::debug_span!(
                    target: crate::telemetry::TARGET,
                    "forward_fanout",
                    regions = buckets.forward.len()
                ),
            )
            .await;
            metrics::histogram!("hyperbytedb_sharded_write_stage_seconds", "stage" => "forward_fanout")
                .record(start.elapsed().as_secs_f64());
            results
        };
        let local = tracing::Instrument::instrument(
            self.append_local_buckets(ctx, db, rp, precision, precision_val, buckets.local),
            tracing::debug_span!(target: crate::telemetry::TARGET, "local_append"),
        );
        let (mut forward_results, local_result) = tokio::join!(forwards, local);
        self.retry_refused_regions(
            ctx,
            db,
            rp,
            precision,
            &buckets.forward,
            &mut forward_results,
        )
        .await;
        let forward_results: Vec<Result<(), HyperbytedbError>> =
            forward_results.into_iter().map(|(_, r)| r).collect();
        let mut forward_failures = 0usize;
        let mut first_forward_error: Option<HyperbytedbError> = None;
        // Every failure was a primary shedding load: the client should back
        // off and retry, not treat the batch as lost.
        let mut all_capacity = true;
        for result in forward_results {
            if let Err(e) = result {
                forward_failures += 1;
                all_capacity &= matches!(e, HyperbytedbError::ClusterUnavailable(_));
                // Classify before deciding anything about retries. A forward
                // targets the region's primary only (`RegionTargetRole::Write`
                // resolves to it alone), so the interesting question is whether
                // these are "primary momentarily unreachable mid-failover" —
                // which a bounded retry would recover — or rejections, which it
                // would not. Guessing would just amplify load during exactly
                // the window convergence is busiest.
                let reason = match &e {
                    HyperbytedbError::PeerUnreachable(_) => "peer_unreachable",
                    HyperbytedbError::StaleShardEpoch { .. } => "stale_epoch",
                    HyperbytedbError::TransferRejected { .. } => "rejected",
                    HyperbytedbError::ShardMap(_) => "shard_map",
                    HyperbytedbError::ClusterUnavailable(_) => "capacity",
                    _ => "other",
                };
                counter!(
                    "hyperbytedb_shard_forward_failed_total",
                    "reason" => reason
                )
                .increment(1);
                if first_forward_error.is_none() {
                    first_forward_error = Some(e);
                }
            }
        }

        local_result?;

        // Reported after the local append, so a remote region's failure never
        // costs the local one its write. Still an error: some of the batch did
        // not land, and the caller has to know that. Naming the counts is what
        // lets a reader tell a partial write from a total one — the split-write
        // suite could not, and read the difference as duplication.
        if let Some(err) = first_forward_error {
            counter!("hyperbytedb_shard_ingest_partial_total").increment(1);
            // A capacity-only failure makes the client resend the whole
            // batch, including every region that did land.
            if all_capacity {
                counter!("hyperbytedb_shard_ingest_capacity_retry_total").increment(1);
                counter!("hyperbytedb_shard_ingest_capacity_retry_regions_landed_total")
                    .increment((forward_region_count - forward_failures) as u64);
            }
            metrics::counter!("hyperbytedb_shard_ingest_forward_region_failures_total")
                .increment(forward_failures as u64);
            return Err(partial_write_error(
                forward_failures,
                forward_region_count,
                all_capacity,
                &err,
            ));
        }

        Ok(())
    }

    /// Queue the regions this write forwards to other primaries, when async
    /// forwarding is on and the queue is keeping up. `false` means the caller
    /// forwards synchronously, which also slows clients while the queue
    /// catches up.
    fn queue_forwards(
        &self,
        ctx: &ShardRoutingContext,
        db: &str,
        rp: &str,
        precision: Option<&str>,
        forward: &std::collections::HashMap<u64, Vec<crate::domain::point::Point>>,
    ) -> bool {
        let Some(queue) = self.forward_queue.as_ref() else {
            return false;
        };
        if forward.is_empty() || !ctx.config.async_forward {
            return false;
        }
        let now = forward_queue::now_ms();
        let behind = if queue.oldest_age_ms(now) > ctx.config.async_forward_max_lag_ms {
            Some("lag")
        } else if queue.bytes() > ctx.config.async_forward_max_bytes {
            Some("bytes")
        } else {
            None
        };
        if let Some(reason) = behind {
            counter!("hyperbytedb_forward_queue_sync_fallback_total", "reason" => reason)
                .increment(1);
            return false;
        }
        // Frames concatenate, so each region's points encode on their own.
        let mut body = Vec::new();
        for points in forward.values() {
            match crate::application::line_protocol::encode_points_binary(points) {
                Ok(frame) => body.extend_from_slice(&frame),
                Err(e) => {
                    tracing::warn!(error = %e, "could not encode forwards for the queue");
                    return false;
                }
            }
        }
        let entry = ForwardEntry {
            db: db.to_string(),
            rp: rp.to_string(),
            precision: precision.map(str::to_string),
            enqueued_ms: now,
            not_before_ms: 0,
            attempts: 0,
            body,
        };
        match queue.enqueue(&entry) {
            Ok(_) => {
                counter!("hyperbytedb_forward_queue_enqueued_total").increment(1);
                true
            }
            Err(e) => {
                tracing::warn!(error = %e, "forward queue enqueue failed; forwarding synchronously");
                counter!("hyperbytedb_forward_queue_sync_fallback_total", "reason" => "error")
                    .increment(1);
                false
            }
        }
    }

    /// Resend only the regions a primary refused at capacity, a few times
    /// with backoff, replacing their results in `results`.
    ///
    /// A refused region used to fail the whole client write, and the client
    /// then resent every point, including the regions that had landed. At
    /// 1,000-point batches under 1% of writes hit this, but the rework grows
    /// with batch size and node count: 5,000- and 10,000-point batches at
    /// N=12 on n3 fell to 387k and 248k points/s with thousands of errors.
    async fn retry_refused_regions(
        &self,
        ctx: &ShardRoutingContext,
        db: &str,
        rp: &str,
        precision: Option<&str>,
        forward: &std::collections::HashMap<u64, Vec<crate::domain::point::Point>>,
        results: &mut Vec<(u64, Result<(), HyperbytedbError>)>,
    ) {
        for attempt in 0..CAPACITY_RETRIES {
            let refused: Vec<u64> = results
                .iter()
                .filter(|(_, r)| matches!(r, Err(HyperbytedbError::ClusterUnavailable(_))))
                .map(|(id, _)| *id)
                .collect();
            if refused.is_empty() {
                return;
            }
            tokio::time::sleep(capacity_backoff(attempt)).await;
            let subset: std::collections::HashMap<u64, Vec<crate::domain::point::Point>> = refused
                .iter()
                .filter_map(|id| forward.get(id).map(|p| (*id, p.clone())))
                .collect();
            counter!("hyperbytedb_shard_forward_capacity_retries_total")
                .increment(refused.len() as u64);
            let retried = forward_shard_writes_by_node(ctx, db, rp, precision, &subset).await;
            results.retain(|(id, _)| !refused.contains(id));
            results.extend(retried);
        }
    }

    /// Deliver the forward queue in the background. Does nothing without a
    /// queue and sharding.
    pub fn spawn_forward_drainer(self: &Arc<Self>) {
        let Some(queue) = self.forward_queue.clone() else {
            return;
        };
        if self.shard_routing.is_none() {
            return;
        }
        let this = Arc::clone(self);
        tokio::spawn(async move {
            let permits = Arc::new(tokio::sync::Semaphore::new(FORWARD_DRAIN_CONCURRENCY));
            loop {
                let now = forward_queue::now_ms();
                metrics::gauge!("hyperbytedb_forward_queue_entries").set(queue.len() as f64);
                metrics::gauge!("hyperbytedb_forward_queue_bytes").set(queue.bytes() as f64);
                metrics::gauge!("hyperbytedb_forward_queue_oldest_age_seconds")
                    .set(queue.oldest_age_ms(now) as f64 / 1000.0);
                let Ok(permit) = Arc::clone(&permits).acquire_owned().await else {
                    return;
                };
                let batch =
                    match queue.claim(now, FORWARD_DRAIN_MAX_ENTRIES, FORWARD_DRAIN_MAX_BYTES) {
                        Ok(b) => b,
                        Err(e) => {
                            tracing::warn!(error = %e, "forward queue claim failed");
                            Vec::new()
                        }
                    };
                if batch.is_empty() {
                    drop(permit);
                    tokio::select! {
                        () = queue.notified() => {}
                        () = tokio::time::sleep(std::time::Duration::from_millis(50)) => {}
                    }
                    continue;
                }
                let this = Arc::clone(&this);
                let queue = Arc::clone(&queue);
                tokio::spawn(async move {
                    this.drain_forward_batch(&queue, batch).await;
                    drop(permit);
                });
            }
        });
    }

    /// Deliver one claimed batch. Regions that fail go back on the queue with
    /// their original timestamp; the batch's entries are then removed.
    async fn drain_forward_batch(&self, queue: &ForwardQueue, batch: Vec<(u64, ForwardEntry)>) {
        let Some(ctx) = self.shard_routing.as_ref() else {
            return;
        };
        let mut groups: Vec<(ForwardEntry, Vec<u64>, Vec<crate::domain::point::Point>)> =
            Vec::new();
        for (seq, entry) in batch {
            let points = match crate::application::line_protocol::parse_line_body_to_points_limited(
                &entry.body,
                None,
                usize::MAX,
            ) {
                Ok(p) => p,
                Err(e) => {
                    tracing::error!(seq, error = %e, "undecodable queued forward; dropping");
                    counter!("hyperbytedb_forward_queue_dropped_total", "reason" => "decode")
                        .increment(1);
                    Vec::new()
                }
            };
            match groups.iter_mut().find(|(head, ..)| {
                head.db == entry.db && head.rp == entry.rp && head.precision == entry.precision
            }) {
                Some((head, seqs, pts)) => {
                    head.enqueued_ms = head.enqueued_ms.min(entry.enqueued_ms);
                    head.attempts = head.attempts.max(entry.attempts);
                    seqs.push(seq);
                    pts.extend(points);
                }
                None => groups.push((
                    ForwardEntry {
                        body: Vec::new(),
                        ..entry
                    },
                    vec![seq],
                    points,
                )),
            }
        }

        for (head, seqs, points) in groups {
            let now = forward_queue::now_ms();
            match self
                .deliver_forwards(ctx, &head.db, &head.rp, head.precision.as_deref(), points)
                .await
            {
                Ok(failed) => {
                    if failed.is_empty() {
                        metrics::histogram!("hyperbytedb_forward_queue_delivery_lag_seconds")
                            .record(now.saturating_sub(head.enqueued_ms) as f64 / 1000.0);
                    } else {
                        self.requeue_forwards(queue, &head, failed, now);
                    }
                    if let Err(e) = queue.complete(&seqs) {
                        tracing::warn!(error = %e, "forward queue complete failed");
                    }
                }
                Err(e) => {
                    // Not even partitioned (e.g. the shard map could not be
                    // read): leave the entries on disk and try again later.
                    tracing::warn!(error = %e, "could not deliver queued forwards; will retry");
                    queue.release(&seqs, now + forward_backoff_ms(head.attempts));
                }
            }
        }
    }

    /// Route `points` against the current shard map and send them: this
    /// node's own regions (its primaries may have moved here since) are
    /// appended locally, the rest forwarded. Returns the points that did not
    /// land.
    async fn deliver_forwards(
        &self,
        ctx: &ShardRoutingContext,
        db: &str,
        rp: &str,
        precision: Option<&str>,
        points: Vec<crate::domain::point::Point>,
    ) -> Result<Vec<crate::domain::point::Point>, HyperbytedbError> {
        if points.is_empty() {
            return Ok(Vec::new());
        }
        let buckets = partition_points(ctx, db, rp, points).await?;
        let precision_val = Precision::from_str_opt(precision);
        let local_points: Vec<crate::domain::point::Point> =
            buckets.local.values().flatten().cloned().collect();
        let forwards = forward_shard_writes_by_node(ctx, db, rp, precision, &buckets.forward);
        let local = self.append_local_buckets(ctx, db, rp, precision, precision_val, buckets.local);
        let (results, local_result) = tokio::join!(forwards, local);

        let mut failed = Vec::new();
        if let Err(e) = local_result {
            tracing::warn!(error = %e, "queued forward's local share failed");
            failed.extend(local_points);
        }
        let mut delivered = 0u64;
        for (region_id, result) in results {
            match result {
                Ok(()) => delivered += 1,
                Err(_) => {
                    if let Some(p) = buckets.forward.get(&region_id) {
                        failed.extend(p.iter().cloned());
                    }
                }
            }
        }
        counter!("hyperbytedb_forward_queue_delivered_regions_total").increment(delivered);
        Ok(failed)
    }

    fn requeue_forwards(
        &self,
        queue: &ForwardQueue,
        head: &ForwardEntry,
        failed: Vec<crate::domain::point::Point>,
        now: u64,
    ) {
        let attempts = head.attempts + 1;
        if attempts >= FORWARD_MAX_ATTEMPTS {
            tracing::error!(
                points = failed.len(),
                attempts,
                db = %head.db,
                "dropping queued forward after repeated delivery failures"
            );
            counter!("hyperbytedb_forward_queue_dropped_total", "reason" => "attempts")
                .increment(1);
            metrics::counter!("hyperbytedb_forward_queue_dropped_points_total")
                .increment(failed.len() as u64);
            return;
        }
        let body = match crate::application::line_protocol::encode_points_binary(&failed) {
            Ok(b) => b,
            Err(e) => {
                tracing::error!(error = %e, "could not re-encode failed forwards; dropping");
                counter!("hyperbytedb_forward_queue_dropped_total", "reason" => "encode")
                    .increment(1);
                return;
            }
        };
        let entry = ForwardEntry {
            db: head.db.clone(),
            rp: head.rp.clone(),
            precision: head.precision.clone(),
            enqueued_ms: head.enqueued_ms,
            not_before_ms: now + forward_backoff_ms(attempts),
            attempts,
            body,
        };
        match queue.enqueue(&entry) {
            Ok(_) => counter!("hyperbytedb_forward_queue_retried_total").increment(1),
            Err(e) => {
                tracing::error!(error = %e, "could not requeue failed forwards; dropping");
                counter!("hyperbytedb_forward_queue_dropped_total", "reason" => "requeue")
                    .increment(1);
            }
        }
    }

    /// Append this node's own regions of a write to the WAL and replicate
    /// them. Runs alongside the forwards to other primaries.
    async fn append_local_buckets(
        &self,
        ctx: &ShardRoutingContext,
        db: &str,
        rp: &str,
        precision: Option<&str>,
        precision_val: Precision,
        local: std::collections::HashMap<u64, Vec<crate::domain::point::Point>>,
    ) -> Result<(), HyperbytedbError> {
        let mut stage = StageTimer::new("hyperbytedb_sharded_write_stage_seconds");
        if !local.is_empty() {
            let mut all_local: Vec<crate::domain::point::Point> = Vec::new();
            for pts in local.values() {
                all_local.extend(pts.iter().cloned());
            }

            prepare_batch_metadata(
                &self.metadata,
                db,
                rp,
                &all_local,
                self.limits,
                Some(&self.schema_cache),
            )
            .await?;
            stage.lap("local_metadata");
            let wal_seq = append_points_with_prepared(
                self.wal.as_ref(),
                self.sink.as_ref(),
                db,
                rp,
                all_local,
                self.node_id,
                self.max_points_per_request,
            )
            .await?;
            stage.lap("local_wal");

            let map = ctx.shard_map.snapshot().await?;
            for (region_id, region_points) in local {
                let Some(region) = crate::application::shard_routing::region_by_id(&map, region_id)
                else {
                    return Err(HyperbytedbError::ShardMap(
                        format!("no region {region_id}").into(),
                    ));
                };
                let local_body = if ctx.config.binary_points_wire {
                    crate::application::line_protocol::encode_points_binary(&region_points)?
                } else {
                    encode_points_to_line_protocol(&region_points, precision_val)?
                };
                let targets = region_replication_targets(ctx, &region).await;
                self.dispatch_replication(OutboundReplicationBatch {
                    database: db.to_string(),
                    retention_policy: rp.to_string(),
                    precision: precision.map(|s| s.to_string()),
                    body: local_body,
                    wal_seq,
                    target_node_ids: Some(targets),
                    region_id: Some(region_id),
                })
                .await?;
            }
            stage.lap("local_replicate");
        }

        Ok(())
    }

    /// Dispatch replication based on the configured mode. Local WAL append
    /// has already happened in the caller — `wal_seq` is its result.
    async fn dispatch_replication(
        &self,
        batch: OutboundReplicationBatch,
    ) -> Result<(), HyperbytedbError> {
        dispatch_outbound_replication(
            Arc::clone(&self.replication_port),
            self.node_id,
            &self.replication,
            batch,
        )
        .await
    }
}

#[async_trait]
impl IngestionPort for PeerIngestionService {
    fn pending_forwards(&self) -> usize {
        self.forward_queue.as_ref().map_or(0, |q| q.len())
    }

    async fn ingest(
        &self,
        db: &str,
        rp: Option<&str>,
        precision: Option<&str>,
        body: &[u8],
        format: WritePayloadFormat,
    ) -> Result<(), HyperbytedbError> {
        let t0 = std::time::Instant::now();

        let retention_policy = match rp {
            Some(s) => s.to_string(),
            None => self.metadata.get_default_rp(db).await?,
        };

        self.metadata
            .get_database(db)
            .await?
            .ok_or_else(|| HyperbytedbError::DatabaseNotFound(db.to_string()))?;

        let t1 = std::time::Instant::now();
        histogram!("hyperbytedb_ingest_metadata_lookup_seconds").record((t1 - t0).as_secs_f64());
        // Columnar fast path: decode once, metadata from batch, then expand for WAL/replication
        #[cfg(feature = "columnar-ingest")]
        if matches!(format, WritePayloadFormat::ColumnarMsgpack) {
            let wire = crate::application::columnar_msgpack::decode_columnar_batch_limited(
                body,
                self.max_points_per_request,
            )?;
            if wire.values.is_empty() {
                return Ok(());
            }
            validate_point_count(wire.values.len(), self.max_points_per_request)?;

            let t2 = std::time::Instant::now();
            histogram!("hyperbytedb_ingest_parse_seconds").record((t2 - t1).as_secs_f64());

            prepare_columnar_metadata(
                &self.metadata,
                db,
                &retention_policy,
                &wire,
                self.limits,
                Some(&self.schema_cache),
            )
            .await?;

            // Reject writes to materialized view destinations at ingestion time
            // (before the WAL entry is created) to avoid stuck-flush liveness gaps.
            if let Some(meta) = self
                .metadata
                .get_measurement(db, &retention_policy, &wire.measurement)
                .await?
                && meta.materialized
                && meta
                    .materialized_rp
                    .as_deref()
                    .is_none_or(|mr| mr == retention_policy)
            {
                return Err(HyperbytedbError::QueryParse(format!(
                    "cannot write directly to materialized view destination \"{0}\"",
                    wire.measurement,
                )));
            }

            let t3 = std::time::Instant::now();
            histogram!("hyperbytedb_ingest_metadata_register_seconds")
                .record((t3 - t2).as_secs_f64());

            let point_count = wire.values.len() as u64;
            let precision_val = Precision::from_str_opt(precision);

            // Sharded clusters: region routing needs per-row series_id, so
            // expand the wire batch once and take the standard
            // partition/forward path. The prepared-WAL slot optimization
            // stays exclusive to the non-sharded single-writer flow below.
            if self.shard_routing.is_some() {
                let points = crate::application::columnar_msgpack::columnar_batch_to_points(
                    &wire, precision,
                )?;
                return self
                    .ingest_sharded_points(db, &retention_policy, precision, points)
                    .await;
            }

            let replication_body =
                crate::application::columnar_msgpack::columnar_wire_to_line_protocol(
                    &wire,
                    precision_val,
                )?;

            let wal_seq = append_columnar_with_prepared(
                self.wal.as_ref(),
                self.sink.as_ref(),
                &ColumnarWalAppend {
                    db,
                    rp: &retention_policy,
                    wire: &wire,
                    precision,
                    origin_node_id: self.node_id,
                    max_points_per_request: self.max_points_per_request,
                },
            )
            .await?;

            let t4 = std::time::Instant::now();
            histogram!("hyperbytedb_ingest_wal_append_seconds").record((t4 - t3).as_secs_f64());

            counter!("hyperbytedb_ingestion_points_total", "db" => db.to_string())
                .increment(point_count);
            counter!("hyperbytedb_wal_appends_total").increment(1);

            let result = self
                .dispatch_replication(OutboundReplicationBatch {
                    database: db.to_string(),
                    retention_policy,
                    precision: precision.map(|s| s.to_string()),
                    body: replication_body,
                    wal_seq,
                    target_node_ids: None,
                    region_id: None,
                })
                .await;
            return result;
        }

        let points = match format {
            WritePayloadFormat::LineProtocol => {
                parse_line_body_to_points_limited(body, precision, self.max_points_per_request)?
            }
            WritePayloadFormat::Msgpack => {
                parse_msgpack_body_to_points_limited(body, precision, self.max_points_per_request)?
            }
            #[cfg(feature = "columnar-ingest")]
            WritePayloadFormat::ColumnarMsgpack => {
                return Err(HyperbytedbError::Internal(
                    "columnar ingest fell through fast path".into(),
                ));
            }
        };
        if points.is_empty() {
            return Ok(());
        }
        if !matches!(format, WritePayloadFormat::LineProtocol) {
            validate_point_count(points.len(), self.max_points_per_request)?;
        }

        let precision_val = Precision::from_str_opt(precision);
        let replication_body = match format {
            WritePayloadFormat::LineProtocol => body.to_vec(),
            WritePayloadFormat::Msgpack => encode_points_to_line_protocol(&points, precision_val)?,
            #[cfg(feature = "columnar-ingest")]
            WritePayloadFormat::ColumnarMsgpack => {
                return Err(HyperbytedbError::Internal(
                    "columnar ingest fell through fast path".into(),
                ));
            }
        };

        let t2 = std::time::Instant::now();
        histogram!("hyperbytedb_ingest_parse_seconds").record((t2 - t1).as_secs_f64());

        if self.shard_routing.is_some() {
            return self
                .ingest_sharded_points(db, &retention_policy, precision, points)
                .await;
        }

        prepare_batch_metadata(
            &self.metadata,
            db,
            &retention_policy,
            &points,
            self.limits,
            Some(&self.schema_cache),
        )
        .await?;

        // Reject writes to materialized view destinations at ingestion time
        // (before the WAL entry is created) to avoid stuck-flush liveness gaps.
        let mut checked_meas = std::collections::HashSet::new();
        for point in &points {
            if checked_meas.insert(&point.measurement)
                && let Some(meta) = self
                    .metadata
                    .get_measurement(db, &retention_policy, &point.measurement)
                    .await?
                && meta.materialized
                && meta
                    .materialized_rp
                    .as_deref()
                    .is_none_or(|mr| mr == retention_policy)
            {
                return Err(HyperbytedbError::QueryParse(format!(
                    "cannot write directly to materialized view destination \"{0}\"",
                    point.measurement,
                )));
            }
        }

        let t3 = std::time::Instant::now();
        histogram!("hyperbytedb_ingest_metadata_register_seconds").record((t3 - t2).as_secs_f64());

        let point_count = points.len() as u64;
        let wal_seq = append_points_with_prepared(
            self.wal.as_ref(),
            self.sink.as_ref(),
            db,
            &retention_policy,
            points,
            self.node_id,
            self.max_points_per_request,
        )
        .await?;

        let t4 = std::time::Instant::now();
        histogram!("hyperbytedb_ingest_wal_append_seconds").record((t4 - t3).as_secs_f64());

        counter!("hyperbytedb_ingestion_points_total", "db" => db.to_string())
            .increment(point_count);
        counter!("hyperbytedb_wal_appends_total").increment(1);

        self.dispatch_replication(OutboundReplicationBatch {
            database: db.to_string(),
            retention_policy,
            precision: precision.map(|s| s.to_string()),
            body: replication_body,
            wal_seq,
            target_node_ids: None,
            region_id: None,
        })
        .await
    }
}

/// The error for a write whose forwards partly failed.
///
/// A 503 when every failure was a primary at capacity: retrying the whole
/// batch is safe (re-applying the local part rewrites identical rows, which
/// the coalescing view collapses) and is what the client should do. Anything
/// else stays a 500 that names the counts.
fn partial_write_error(
    forward_failures: usize,
    forward_region_count: usize,
    all_capacity: bool,
    first: &HyperbytedbError,
) -> HyperbytedbError {
    if all_capacity {
        return HyperbytedbError::ClusterUnavailable(format!(
            "{forward_failures} of {forward_region_count} region primaries at write \
             capacity; retry the batch"
        ));
    }
    HyperbytedbError::ShardMap(
        format!(
            "write partially applied: {forward_failures} of \
             {forward_region_count} forwarded regions failed \
             (local bucket applied); first error: {first}"
        )
        .into(),
    )
}

#[cfg(test)]
mod partial_write_error_tests {
    use super::partial_write_error;
    use crate::error::HyperbytedbError;

    #[test]
    fn capacity_only_failures_are_retryable_everything_else_is_not() {
        let cap = HyperbytedbError::ClusterUnavailable("at capacity".into());
        assert!(matches!(
            partial_write_error(3, 8, true, &cap),
            HyperbytedbError::ClusterUnavailable(_)
        ));
        let down = HyperbytedbError::PeerUnreachable("refused".into());
        assert!(matches!(
            partial_write_error(3, 8, false, &down),
            HyperbytedbError::ShardMap(_)
        ));
    }
}
