//! Apply and initiate region transfer payloads on store nodes.

use std::collections::BTreeMap;
use std::sync::Arc;

use metrics::counter;

use crate::application::ingest_metadata::{IngestCardinalityLimits, prepare_batch_metadata};
use crate::application::line_protocol::{
    encode_points_to_line_protocol, parse_line_body_to_points_limited,
};
use crate::application::wal_append::append_points_with_prepared;
use crate::domain::column_mapping::ColumnMapping;
use crate::domain::database::Precision;
use crate::domain::point::{FieldValue, Point};
use crate::domain::series::series_id_for_point;
use crate::domain::sharding::{MeasurementKey, ShardRegion, ShardTransferPayload, TransferPhase};
use crate::error::HyperbytedbError;
use crate::ports::metadata::MetadataPort;
use crate::ports::points_sink::PointsSinkPort;
use crate::ports::query::QueryPort;
use crate::ports::wal::WalPort;

pub async fn apply_transfer_push(
    metadata: &Arc<dyn MetadataPort>,
    wal: &Arc<dyn WalPort>,
    sink: Option<&Arc<dyn PointsSinkPort>>,
    _node_id: u64,
    req: &ShardTransferPayload,
    max_points: usize,
) -> Result<u64, HyperbytedbError> {
    let body = req
        .body
        .as_ref()
        .filter(|b| !b.is_empty())
        .ok_or_else(|| HyperbytedbError::ShardMap("transfer push missing body".into()))?;

    let points = parse_line_body_to_points_limited(body, None, max_points)?;
    let filtered: Vec<_> = points
        .into_iter()
        .filter(|p| {
            let sid = series_id_for_point(p);
            sid >= req.start && sid < req.end
        })
        .collect();

    if filtered.is_empty() {
        return wal.last_sequence().await;
    }

    let limits = IngestCardinalityLimits {
        max_tag_values_per_measurement: 0,
        max_measurements_per_database: 0,
    };
    prepare_batch_metadata(metadata, &req.db, &req.rp, &filtered, limits, None).await?;

    append_points_with_prepared(
        wal.as_ref(),
        sink,
        &req.db,
        &req.rp,
        filtered,
        req.source_node_id,
        max_points,
    )
    .await
}

pub async fn apply_transfer(
    metadata: &Arc<dyn MetadataPort>,
    wal: &Arc<dyn WalPort>,
    sink: Option<&Arc<dyn PointsSinkPort>>,
    mv_service: Option<&crate::application::materialized_view_service::MaterializedViewService>,
    node_id: u64,
    req: &ShardTransferPayload,
    max_points: usize,
) -> Result<(), HyperbytedbError> {
    match req.phase {
        TransferPhase::Push => {
            apply_transfer_push(metadata, wal, sink, node_id, req, max_points).await?;
        }
        TransferPhase::Ack => {
            if let Some(mv) = mv_service
                && let Err(e) = mv
                    .purge_dest_partials_after_source_transfer(
                        &req.db,
                        &req.rp,
                        &req.measurement,
                        req.start,
                        req.end,
                    )
                    .await
            {
                tracing::warn!(
                    db = %req.db,
                    measurement = %req.measurement,
                    error = %e,
                    "MV dest purge after transfer failed"
                );
            }
        }
    }
    Ok(())
}

pub async fn drop_region_data(
    metadata: &Arc<dyn MetadataPort>,
    sink: Option<&Arc<dyn PointsSinkPort>>,
    db: &str,
    rp: &str,
    measurement: &str,
    start: u64,
    end: u64,
) -> Result<(), HyperbytedbError> {
    metadata
        .delete_series_in_range(db, rp, measurement, start, end)
        .await?;
    if let Some(sink) = sink {
        sink.delete_series_id_range(db, rp, measurement, start, end)
            .await?;
    }
    Ok(())
}

pub async fn export_region_points_from_wal(
    wal: &Arc<dyn WalPort>,
    key: &MeasurementKey,
    region: &ShardRegion,
    max_points: usize,
) -> Result<Vec<u8>, HyperbytedbError> {
    let entries = wal.read_from(0).await?;
    let mut points = Vec::new();
    for (_, entry) in entries {
        if entry.database != key.db || entry.retention_policy != key.rp {
            continue;
        }
        for p in entry.points {
            if p.measurement != key.measurement {
                continue;
            }
            let sid = series_id_for_point(&p);
            if sid >= region.start && sid < region.end {
                points.push(p);
                if points.len() >= max_points {
                    return Err(HyperbytedbError::ShardMap(
                        "transfer export exceeds max points".into(),
                    ));
                }
            }
        }
    }
    if points.is_empty() {
        return Ok(Vec::new());
    }
    encode_points_to_line_protocol(&points, Precision::Nanosecond)
}

/// Export flushed chDB rows for `[region.start, region.end)` as line protocol.
pub async fn export_region_points_from_chdb(
    query: &dyn QueryPort,
    metadata: &dyn MetadataPort,
    key: &MeasurementKey,
    region: &ShardRegion,
    offset: u64,
    max_points: usize,
) -> Result<(Vec<u8>, usize), HyperbytedbError> {
    use crate::domain::chdb_naming::{
        field_column_name, quote_backticks, quoted_series_table_name, quoted_table_name,
    };

    let meta = metadata
        .get_measurement(&key.db, &key.rp, &key.measurement)
        .await?
        .ok_or_else(|| {
            HyperbytedbError::ShardMap(format!(
                "measurement {} not found for transfer export",
                key.measurement
            ))
        })?;

    let mapping = ColumnMapping::from_measurement_meta(&meta);
    let fact = quoted_table_name(&key.db, &key.rp, &key.measurement);
    let series = quoted_series_table_name(&key.db, &key.rp, &key.measurement);

    let mut select_cols = vec!["f.`time`".to_string(), "f.`series_id`".to_string()];
    for field in mapping.field_names.iter() {
        let phys = field_column_name(field);
        select_cols.push(format!("f.{}", quote_backticks(&phys)));
    }
    for tag in mapping.tag_keys.iter() {
        let phys = mapping.physical_tag_column_name(tag);
        select_cols.push(format!("s.{}", quote_backticks(&phys)));
    }

    let range = if region.end == u64::MAX {
        format!("f.`series_id` >= {}", region.start)
    } else {
        format!(
            "f.`series_id` >= {} AND f.`series_id` < {}",
            region.start, region.end
        )
    };

    let sql = format!(
        "SELECT {} FROM {fact} AS f \
         ANY LEFT JOIN {series} AS s ON f.`series_id` = s.`series_id` \
         WHERE {range} \
         ORDER BY f.`series_id`, f.`time` \
         LIMIT {max_points} OFFSET {offset} \
         FORMAT JSONEachRow",
        select_cols.join(", ")
    );

    let raw = query.execute_sql(&sql).await?;
    if raw.trim().is_empty() {
        return Ok((Vec::new(), 0));
    }

    let mut points = Vec::new();
    for line in raw.lines() {
        let line = line.trim();
        if line.is_empty() {
            continue;
        }
        let row: serde_json::Value = serde_json::from_str(line).map_err(|e| {
            HyperbytedbError::Internal(format!("transfer export JSON parse: {e}").into())
        })?;
        let Some(obj) = row.as_object() else {
            continue;
        };

        let mut tags = BTreeMap::new();
        for tag in &mapping.tag_keys {
            let phys = mapping.physical_tag_column_name(tag);
            if let Some(serde_json::Value::String(v)) = obj.get(&phys) {
                tags.insert(tag.clone(), v.clone());
            }
        }

        let mut fields = BTreeMap::new();
        for field in &mapping.field_names {
            let phys = field_column_name(field);
            if let Some(val) = obj.get(&phys)
                && let Some(fv) = json_field_to_value(val, meta.field_types.get(field).copied())
            {
                fields.insert(field.clone(), fv);
            }
        }

        let timestamp = obj.get("time").and_then(json_timestamp_nanos).unwrap_or(0);
        if fields.is_empty() {
            continue;
        }

        points.push(Point {
            measurement: key.measurement.clone(),
            tags,
            fields,
            timestamp,
        });
    }

    let count = points.len();
    if count == 0 {
        return Ok((Vec::new(), 0));
    }
    let body = encode_points_to_line_protocol(&points, Precision::Nanosecond)?;
    Ok((body, count))
}

fn json_timestamp_nanos(v: &serde_json::Value) -> Option<i64> {
    match v {
        serde_json::Value::Number(n) => n.as_i64(),
        serde_json::Value::String(s) => chrono::DateTime::parse_from_rfc3339(s)
            .ok()
            .and_then(|dt| dt.timestamp_nanos_opt()),
        _ => None,
    }
}

fn json_field_to_value(v: &serde_json::Value, disc: Option<u8>) -> Option<FieldValue> {
    match v {
        serde_json::Value::Number(n) => {
            if disc == Some(1) {
                n.as_i64().map(FieldValue::Integer)
            } else if disc == Some(2) {
                n.as_u64().map(FieldValue::UInteger)
            } else {
                n.as_f64().map(FieldValue::Float)
            }
        }
        serde_json::Value::String(s) => Some(FieldValue::String(s.clone())),
        serde_json::Value::Bool(b) => Some(FieldValue::Boolean(*b)),
        _ => None,
    }
}

#[allow(clippy::too_many_arguments)]
pub async fn push_region_transfer(
    peer_client: &crate::adapters::cluster::peer_client::PeerClient,
    source_node_id: u64,
    key: &MeasurementKey,
    region: &ShardRegion,
    body: Vec<u8>,
    dest_primary: u64,
    transfer_id: u64,
    seq: u64,
    done: bool,
) -> Result<(), HyperbytedbError> {
    let membership = peer_client.membership().read().await;
    let addr = membership
        .get_node(dest_primary)
        .map(|n| n.addr.clone())
        .ok_or_else(|| HyperbytedbError::PeerUnreachable("unknown transfer destination".into()))?;
    drop(membership);

    let body_len = body.len();
    let payload = ShardTransferPayload::push(
        &key.db,
        &key.rp,
        &key.measurement,
        region.region_id,
        region.start,
        region.end,
        region.epoch,
        body,
        source_node_id,
        transfer_id,
        seq,
        done,
    );
    let url = format!("http://{addr}/internal/shard/transfer");
    let resp = peer_client
        .http_client()
        .post(&url)
        .json(&payload)
        .send()
        .await
        .map_err(|e| HyperbytedbError::PeerUnreachable(e.to_string()))?;
    if !resp.status().is_success() {
        return Err(HyperbytedbError::ShardMap(format!(
            "transfer push failed: {}",
            resp.status()
        )));
    }
    counter!("hyperbytedb_shard_transfer_bytes").increment(body_len as u64);
    counter!("hyperbytedb_shard_transfers_total").increment(1);
    Ok(())
}

pub async fn ack_region_transfer(
    peer_client: &crate::adapters::cluster::peer_client::PeerClient,
    source_node_id: u64,
    key: &MeasurementKey,
    region: &ShardRegion,
    ack_node: u64,
    transfer_id: u64,
) -> Result<(), HyperbytedbError> {
    let membership = peer_client.membership().read().await;
    let addr = membership
        .get_node(ack_node)
        .map(|n| n.addr.clone())
        .ok_or_else(|| HyperbytedbError::PeerUnreachable("unknown transfer ack target".into()))?;
    drop(membership);

    let payload = ShardTransferPayload::ack(
        &key.db,
        &key.rp,
        &key.measurement,
        region.region_id,
        region.start,
        region.end,
        region.epoch,
        source_node_id,
        transfer_id,
    );
    let url = format!("http://{addr}/internal/shard/transfer");
    let resp = peer_client
        .http_client()
        .post(&url)
        .json(&payload)
        .send()
        .await
        .map_err(|e| HyperbytedbError::PeerUnreachable(e.to_string()))?;
    if !resp.status().is_success() {
        return Err(HyperbytedbError::ShardMap(format!(
            "transfer ack failed: {}",
            resp.status()
        )));
    }
    Ok(())
}

fn new_transfer_id() -> u64 {
    use std::time::{SystemTime, UNIX_EPOCH};
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_nanos() as u64)
        .unwrap_or(1)
}

#[allow(clippy::too_many_arguments)]
pub async fn push_region_transfer_data(
    peer_client: &Arc<crate::adapters::cluster::peer_client::PeerClient>,
    metadata: &Arc<dyn MetadataPort>,
    wal: &Arc<dyn WalPort>,
    query_port: Option<&Arc<dyn QueryPort>>,
    source_node_id: u64,
    key: &MeasurementKey,
    region: &ShardRegion,
    dest_primary: u64,
    max_points: usize,
) -> Result<u64, HyperbytedbError> {
    if dest_primary == source_node_id {
        return Ok(0);
    }
    let max_points = max_points.max(1);
    let transfer_id = new_transfer_id();
    let mut seq = 0u64;

    let wal_body = export_region_points_from_wal(wal, key, region, max_points).await?;
    if !wal_body.is_empty() {
        push_region_transfer(
            peer_client.as_ref(),
            source_node_id,
            key,
            region,
            wal_body,
            dest_primary,
            transfer_id,
            seq,
            false,
        )
        .await?;
        seq = seq.saturating_add(1);
    }

    if let Some(query) = query_port {
        let mut offset = 0u64;
        loop {
            let (body, count) = export_region_points_from_chdb(
                query.as_ref(),
                metadata.as_ref(),
                key,
                region,
                offset,
                max_points,
            )
            .await?;
            if body.is_empty() {
                break;
            }
            let done = count < max_points;
            push_region_transfer(
                peer_client.as_ref(),
                source_node_id,
                key,
                region,
                body,
                dest_primary,
                transfer_id,
                seq,
                done,
            )
            .await?;
            seq = seq.saturating_add(1);
            if done {
                break;
            }
            offset = offset.saturating_add(count as u64);
        }
    }

    Ok(transfer_id)
}

#[allow(clippy::too_many_arguments)]
pub async fn complete_region_transfer(
    peer_client: &Arc<crate::adapters::cluster::peer_client::PeerClient>,
    metadata: &Arc<dyn MetadataPort>,
    sink: Option<&Arc<dyn PointsSinkPort>>,
    source_node_id: u64,
    key: &MeasurementKey,
    region: &ShardRegion,
    dest_primary: u64,
    transfer_id: u64,
) -> Result<(), HyperbytedbError> {
    if dest_primary == source_node_id || transfer_id == 0 {
        return Ok(());
    }
    ack_region_transfer(
        peer_client.as_ref(),
        source_node_id,
        key,
        region,
        dest_primary,
        transfer_id,
    )
    .await?;
    drop_region_data(
        metadata,
        sink,
        &key.db,
        &key.rp,
        &key.measurement,
        region.start,
        region.end,
    )
    .await
}

#[allow(clippy::too_many_arguments)]
pub async fn run_region_transfer(
    peer_client: &Arc<crate::adapters::cluster::peer_client::PeerClient>,
    metadata: &Arc<dyn MetadataPort>,
    wal: &Arc<dyn WalPort>,
    query_port: Option<&Arc<dyn QueryPort>>,
    sink: Option<&Arc<dyn PointsSinkPort>>,
    source_node_id: u64,
    key: &MeasurementKey,
    region: &ShardRegion,
    dest_primary: u64,
    max_points: usize,
) -> Result<(), HyperbytedbError> {
    let transfer_id = push_region_transfer_data(
        peer_client,
        metadata,
        wal,
        query_port,
        source_node_id,
        key,
        region,
        dest_primary,
        max_points,
    )
    .await?;
    complete_region_transfer(
        peer_client,
        metadata,
        sink,
        source_node_id,
        key,
        region,
        dest_primary,
        transfer_id,
    )
    .await
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::adapters::metadata::rocksdb_meta::RocksDbMetadata;
    use crate::domain::measurement::MeasurementMeta;
    use crate::domain::series::series_id;
    use std::collections::BTreeMap;

    fn host_tag(v: &str) -> BTreeMap<String, String> {
        [("host".into(), v.into())].into()
    }

    #[tokio::test]
    async fn delete_series_in_range_preserves_left_half() {
        let dir = tempfile::tempdir().unwrap();
        let meta = RocksDbMetadata::open(dir.path()).unwrap();

        let left_id = series_id("cpu", &host_tag("left"));
        let right_id = series_id("cpu", &host_tag("right"));
        let low = left_id.min(right_id);
        let high = left_id.max(right_id);

        meta.register_measurement(
            "db",
            "autogen",
            &MeasurementMeta {
                name: "cpu".into(),
                tag_keys: vec!["host".into()],
                field_types: [("value".into(), 0_u8)].into(),
                ..Default::default()
            },
        )
        .await
        .unwrap();

        meta.register_series_batch(
            "db",
            "autogen",
            "cpu",
            &[(left_id, host_tag("left")), (right_id, host_tag("right"))],
        )
        .await
        .unwrap();

        let removed = meta
            .delete_series_in_range("db", "autogen", "cpu", low + 1, u64::MAX)
            .await
            .unwrap();
        assert_eq!(removed, 1);

        let remaining = meta.list_series_ids("db", "autogen", "cpu").await.unwrap();
        assert!(remaining.contains(&low));
        assert!(!remaining.contains(&high));
    }
}
