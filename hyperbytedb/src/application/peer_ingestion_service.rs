use std::sync::Arc;

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
        }
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
        let buckets = partition_points(ctx, db, rp, points).await?;
        let precision_val = Precision::from_str_opt(precision);
        stage.lap("partition");

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
            let results =
                forward_shard_writes_by_node(ctx, db, rp, precision, buckets.forward).await;
            metrics::histogram!("hyperbytedb_sharded_write_stage_seconds", "stage" => "forward_fanout")
                .record(start.elapsed().as_secs_f64());
            results
        };
        let local = self.append_local_buckets(ctx, db, rp, precision, precision_val, buckets.local);
        let (forward_results, local_result) = tokio::join!(forwards, local);
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
