//! Apply and initiate region transfer payloads on store nodes.

use std::sync::Arc;

use metrics::counter;

use crate::application::ingest_metadata::{prepare_batch_metadata, IngestCardinalityLimits};
use crate::application::line_protocol::parse_line_body_to_points_limited;
use crate::application::wal_append::append_points_with_prepared;
use crate::domain::series::series_id_for_point;
use crate::domain::sharding::{MeasurementKey, ShardRegion, ShardTransferPayload, TransferPhase};
use crate::error::HyperbytedbError;
use crate::ports::metadata::MetadataPort;
use crate::ports::points_sink::PointsSinkPort;
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
    prepare_batch_metadata(
        metadata,
        &req.db,
        &req.rp,
        &filtered,
        limits,
        None,
    )
    .await?;

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
    node_id: u64,
    req: &ShardTransferPayload,
    max_points: usize,
) -> Result<(), HyperbytedbError> {
    match req.phase {
        TransferPhase::Push => {
            apply_transfer_push(metadata, wal, sink, node_id, req, max_points).await?;
        }
        TransferPhase::Ack => {
            drop_region_data(metadata, sink, &req.db, &req.rp, &req.measurement, req.start, req.end)
                .await?;
        }
    }
    Ok(())
}

pub async fn drop_region_data(
    metadata: &Arc<dyn MetadataPort>,
    _sink: Option<&Arc<dyn PointsSinkPort>>,
    db: &str,
    rp: &str,
    measurement: &str,
    start: u64,
    end: u64,
) -> Result<(), HyperbytedbError> {
    let predicate = if end == u64::MAX {
        format!("series_id >= {start}")
    } else {
        format!("series_id >= {start} AND series_id < {end}")
    };
    metadata
        .delete_series_matching(db, rp, Some(measurement), &predicate)
        .await?;
    Ok(())
}

pub async fn export_region_points_from_wal(
    wal: &Arc<dyn WalPort>,
    key: &MeasurementKey,
    region: &ShardRegion,
    max_points: usize,
) -> Result<Vec<u8>, HyperbytedbError> {
    use crate::application::line_protocol::encode_points_to_line_protocol;
    use crate::domain::database::Precision;

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

pub async fn push_region_transfer(
    peer_client: &crate::adapters::cluster::peer_client::PeerClient,
    source_node_id: u64,
    key: &MeasurementKey,
    region: &ShardRegion,
    body: Vec<u8>,
    dest_primary: u64,
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

#[allow(clippy::too_many_arguments)]
pub async fn run_region_transfer(
    peer_client: &Arc<crate::adapters::cluster::peer_client::PeerClient>,
    metadata: &Arc<dyn MetadataPort>,
    wal: &Arc<dyn WalPort>,
    sink: Option<&Arc<dyn PointsSinkPort>>,
    source_node_id: u64,
    key: &MeasurementKey,
    region: &ShardRegion,
    dest_primary: u64,
    max_points: usize,
) -> Result<(), HyperbytedbError> {
    if dest_primary == source_node_id {
        return Ok(());
    }
    let body = export_region_points_from_wal(wal, key, region, max_points).await?;
    push_region_transfer(
        peer_client.as_ref(),
        source_node_id,
        key,
        region,
        body.clone(),
        dest_primary,
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
    .await?;
    Ok(())
}
