use async_trait::async_trait;
use metrics::{counter, histogram};
use std::sync::Arc;

#[cfg(feature = "columnar-ingest")]
use crate::application::ingest_metadata::prepare_columnar_metadata;
use crate::application::ingest_metadata::{
    IngestCardinalityLimits, IngestSchemaCache, prepare_batch_metadata,
};
use crate::application::line_protocol::parse_line_body_to_points;
use crate::application::msgpack_ingest::parse_msgpack_body_to_points;
use crate::application::system_trace;
use crate::error::HyperbytedbError;
use crate::ports::ingestion::{IngestionPort, WritePayloadFormat};
use crate::ports::wal::{WalEntry, WalPort};

pub struct IngestionServiceImpl {
    wal: Arc<dyn WalPort>,
    metadata: Arc<dyn crate::ports::metadata::MetadataPort>,
    limits: IngestCardinalityLimits,
    schema_cache: IngestSchemaCache,
}

impl IngestionServiceImpl {
    pub fn new(
        wal: Arc<dyn WalPort>,
        metadata: Arc<dyn crate::ports::metadata::MetadataPort>,
        max_tag_values: usize,
        max_measurements: usize,
    ) -> Self {
        Self {
            wal,
            metadata,
            limits: IngestCardinalityLimits {
                max_tag_values_per_measurement: max_tag_values,
                max_measurements_per_database: max_measurements,
            },
            schema_cache: IngestSchemaCache::new(),
        }
    }
}

#[async_trait]
impl IngestionPort for IngestionServiceImpl {
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
        system_trace::record_phase("metadata_lookup_us", t1 - t0);

        // Columnar fast path: prepare metadata directly from the wire batch
        // then expand to Points only for WAL serialization.
        #[cfg(feature = "columnar-ingest")]
        if matches!(format, WritePayloadFormat::ColumnarMsgpack) {
            let wire = crate::application::columnar_msgpack::decode_columnar_batch(body)?;
            if wire.values.is_empty() {
                return Ok(());
            }

            let t2 = std::time::Instant::now();
            histogram!("hyperbytedb_ingest_parse_seconds").record((t2 - t1).as_secs_f64());
            system_trace::record_phase("parse_us", t2 - t1);

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
            system_trace::record_phase("metadata_register_us", t3 - t2);

            let point_count = wire.values.len() as u64;
            system_trace::record_u64("point_count", point_count);
            let points =
                crate::application::columnar_msgpack::columnar_batch_to_points(&wire, precision)?;
            let entry = WalEntry {
                database: db.to_string(),
                retention_policy: retention_policy.clone(),
                points,
                origin_node_id: 0,
            };
            let wal_seq = self.wal.append(entry).await?;

            let t4 = std::time::Instant::now();
            histogram!("hyperbytedb_ingest_wal_append_seconds").record((t4 - t3).as_secs_f64());
            system_trace::record_phase("wal_append_us", t4 - t3);
            system_trace::record_u64("wal_seq", wal_seq);

            counter!("hyperbytedb_ingestion_points_total", "db" => db.to_string())
                .increment(point_count);
            counter!("hyperbytedb_wal_appends_total").increment(1);

            return Ok(());
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

        let t2 = std::time::Instant::now();
        histogram!("hyperbytedb_ingest_parse_seconds").record((t2 - t1).as_secs_f64());
        system_trace::record_phase("parse_us", t2 - t1);

        prepare_batch_metadata(
            &self.metadata,
            db,
            &points,
            self.limits,
            Some(&self.schema_cache),
        )
        .await?;

        let t3 = std::time::Instant::now();
        histogram!("hyperbytedb_ingest_metadata_register_seconds").record((t3 - t2).as_secs_f64());
        system_trace::record_phase("metadata_register_us", t3 - t2);

        let point_count = points.len() as u64;
        system_trace::record_u64("point_count", point_count);
        let entry = WalEntry {
            database: db.to_string(),
            retention_policy: retention_policy.clone(),
            points,
            origin_node_id: 0,
        };
        let wal_seq = self.wal.append(entry).await?;

        let t4 = std::time::Instant::now();
        histogram!("hyperbytedb_ingest_wal_append_seconds").record((t4 - t3).as_secs_f64());
        system_trace::record_phase("wal_append_us", t4 - t3);
        system_trace::record_u64("wal_seq", wal_seq);

        counter!("hyperbytedb_ingestion_points_total", "db" => db.to_string())
            .increment(point_count);
        counter!("hyperbytedb_wal_appends_total").increment(1);

        Ok(())
    }
}
