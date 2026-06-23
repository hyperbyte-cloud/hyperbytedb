use std::sync::Arc;

use async_trait::async_trait;
use metrics::{counter, histogram};

#[cfg(feature = "columnar-ingest")]
use crate::application::ingest_metadata::prepare_columnar_metadata;
use crate::application::ingest_metadata::{
    IngestCardinalityLimits, IngestSchemaCache, prepare_batch_metadata,
};
use crate::application::line_protocol::{
    encode_points_to_line_protocol, parse_line_body_to_points,
};
use crate::application::msgpack_ingest::parse_msgpack_body_to_points;
use crate::application::replication_dispatch::dispatch_outbound_replication;
use crate::application::wal_append::append_points_with_prepared;
use crate::config::{ReplicationConfig, ReplicationMode};
use crate::domain::database::Precision;
use crate::error::HyperbytedbError;
use crate::ports::ingestion::{IngestionPort, WritePayloadFormat};
use crate::ports::points_sink::PointsSinkPort;
use crate::ports::replication::{OutboundReplicationBatch, ReplicationPort};
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
    schema_cache: IngestSchemaCache,
    replication: ReplicationConfig,
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
            ReplicationConfig::default(),
        )
    }

    pub fn with_replication(
        wal: Arc<dyn WalPort>,
        metadata: Arc<dyn crate::ports::metadata::MetadataPort>,
        replication_port: Arc<dyn ReplicationPort>,
        node_id: u64,
        limits: IngestCardinalityLimits,
        replication: ReplicationConfig,
    ) -> Self {
        Self::with_replication_and_sink(
            wal,
            None,
            metadata,
            replication_port,
            node_id,
            limits,
            replication,
        )
    }

    pub fn with_replication_and_sink(
        wal: Arc<dyn WalPort>,
        sink: Option<Arc<dyn PointsSinkPort>>,
        metadata: Arc<dyn crate::ports::metadata::MetadataPort>,
        replication_port: Arc<dyn ReplicationPort>,
        node_id: u64,
        limits: IngestCardinalityLimits,
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
            schema_cache: IngestSchemaCache::new(),
            replication,
        }
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
            None => self
                .metadata
                .get_default_rp(db)
                .await
                .unwrap_or_else(|_| "autogen".to_string()),
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
            let wire = crate::application::columnar_msgpack::decode_columnar_batch(body)?;
            if wire.values.is_empty() {
                return Ok(());
            }

            let t2 = std::time::Instant::now();
            histogram!("hyperbytedb_ingest_parse_seconds").record((t2 - t1).as_secs_f64());

            prepare_columnar_metadata(
                &self.metadata,
                db,
                &wire,
                self.limits,
                Some(&self.schema_cache),
            )
            .await?;

            let t3 = std::time::Instant::now();
            histogram!("hyperbytedb_ingest_metadata_register_seconds")
                .record((t3 - t2).as_secs_f64());

            let point_count = wire.values.len() as u64;
            let points =
                crate::application::columnar_msgpack::columnar_batch_to_points(&wire, precision)?;
            let precision_val = Precision::from_str_opt(precision);
            let replication_body = encode_points_to_line_protocol(&points, precision_val)?;

            let wal_seq = append_points_with_prepared(
                self.wal.as_ref(),
                self.sink.as_ref(),
                db,
                &retention_policy,
                points,
                self.node_id,
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
                })
                .await;
            return result;
        }

        let points = match format {
            WritePayloadFormat::LineProtocol => parse_line_body_to_points(body, precision)?,
            WritePayloadFormat::Msgpack => parse_msgpack_body_to_points(body, precision)?,
            #[cfg(feature = "columnar-ingest")]
            WritePayloadFormat::ColumnarMsgpack => {
                unreachable!("handled by fast path above")
            }
        };
        if points.is_empty() {
            return Ok(());
        }

        let precision_val = Precision::from_str_opt(precision);
        let replication_body = match format {
            WritePayloadFormat::LineProtocol => body.to_vec(),
            WritePayloadFormat::Msgpack => encode_points_to_line_protocol(&points, precision_val)?,
            #[cfg(feature = "columnar-ingest")]
            WritePayloadFormat::ColumnarMsgpack => {
                unreachable!("handled by fast path above")
            }
        };

        let t2 = std::time::Instant::now();
        histogram!("hyperbytedb_ingest_parse_seconds").record((t2 - t1).as_secs_f64());

        prepare_batch_metadata(
            &self.metadata,
            db,
            &retention_policy,
            &points,
            self.limits,
            Some(&self.schema_cache),
        )
        .await?;

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
        })
        .await
    }
}
