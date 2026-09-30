//! Coordinator scatter-gather for SHOW metadata commands under sharding.

use std::collections::{BTreeMap, BTreeSet};

use futures::future::try_join_all;

const METADATA_SCATTER_CONCURRENCY: usize = 32;

use crate::application::shard_peer_resolution::{RegionTargetRole, ScatterKind};
use crate::application::shard_routing::{
    ShardRoutingContext, reload_region, scatter_to_region_peers,
};
use crate::domain::point::FieldValue;
use crate::domain::sharding::{ShardMetadataKind, ShardMetadataRequest, ShardRegion};
use crate::error::HyperbytedbError;
use crate::ports::metadata::{MeasurementMeta, MetadataPort};
use crate::ports::sharding::ShardMapPort;

async fn fetch_remote_metadata(
    ctx: &ShardRoutingContext,
    db: &str,
    rp: &str,
    measurement: &str,
    region: &ShardRegion,
    kind: ShardMetadataKind,
) -> Result<serde_json::Value, HyperbytedbError> {
    let mut current = region.clone();
    for retry in 0..2 {
        let req = ShardMetadataRequest {
            db: db.to_string(),
            rp: rp.to_string(),
            measurement: measurement.to_string(),
            epoch: current.epoch,
            region_id: current.region_id,
            kind: kind.clone(),
        };
        let region_id = current.region_id;

        match scatter_to_region_peers(
            ctx,
            &current,
            RegionTargetRole::Read,
            ScatterKind::Metadata,
            |peer_id, addr, timeout| {
                let req = req.clone();
                let addr = addr.to_string();
                async move {
                    let url = format!("http://{addr}/internal/shard/metadata");
                    let resp = ctx
                        .peer_client
                        .http_client()
                        .post(&url)
                        .json(&req)
                        .timeout(timeout)
                        .send()
                        .await
                        .map_err(|e| HyperbytedbError::PeerUnreachable(e.to_string()))?;
                    if resp.status() == reqwest::StatusCode::CONFLICT {
                        return Err(HyperbytedbError::StaleShardEpoch { region_id });
                    }
                    if !resp.status().is_success() {
                        return Err(HyperbytedbError::PeerUnreachable(format!(
                            "metadata scatter to peer {peer_id} failed: {}",
                            resp.status()
                        )));
                    }
                    resp.json()
                        .await
                        .map_err(|e| HyperbytedbError::Internal(e.to_string().into()))
                }
            },
        )
        .await
        {
            Ok(v) => return Ok(v),
            Err(HyperbytedbError::StaleShardEpoch { region_id }) if retry == 0 => {
                current = reload_region(ctx, db, rp, measurement, region_id).await?;
            }
            Err(e) => return Err(e),
        }
    }
    Err(HyperbytedbError::ShardMap(
        "metadata scatter stale epoch after retry".into(),
    ))
}

async fn fetch_remote_metadata_bounded(
    ctx: &ShardRoutingContext,
    db: &str,
    rp: &str,
    measurement: &str,
    regions: Vec<ShardRegion>,
    kind: ShardMetadataKind,
) -> Result<Vec<serde_json::Value>, HyperbytedbError> {
    let mut parts = Vec::with_capacity(regions.len());
    for chunk in regions.chunks(METADATA_SCATTER_CONCURRENCY) {
        let chunk_futures: Vec<_> = chunk
            .iter()
            .map(|region| {
                let region = region.clone();
                let kind = kind.clone();
                async move { fetch_remote_metadata(ctx, db, rp, measurement, &region, kind).await }
            })
            .collect();
        parts.extend(try_join_all(chunk_futures).await?);
    }
    Ok(parts)
}

pub async fn scatter_tag_keys(
    ctx: &ShardRoutingContext,
    metadata: &dyn MetadataPort,
    db: &str,
    rp: &str,
    measurement: &str,
) -> Result<Vec<String>, HyperbytedbError> {
    let map = ctx.shard_map.snapshot().await?;
    // The serving view, not the raw snapshot: a snapshot contains staging
    // regions, which are in the map but must not be routed to -- the donor
    // still serves their range and this view reflects that.
    let serving = ctx
        .location_cache
        .serving_regions(&map, db, rp, measurement);
    if serving.is_empty() {
        return metadata.list_tag_keys(db, rp, Some(measurement)).await;
    }

    let mut keys = BTreeSet::new();
    let has_local_peer = serving
        .iter()
        .any(|region| region.peers.contains(&ctx.node_id));
    if has_local_peer {
        for k in metadata.list_tag_keys(db, rp, Some(measurement)).await? {
            keys.insert(k);
        }
    }

    let remote_regions: Vec<ShardRegion> = serving
        .iter()
        .filter(|region| !region.peers.contains(&ctx.node_id))
        .cloned()
        .collect();
    let remote_parts = fetch_remote_metadata_bounded(
        ctx,
        db,
        rp,
        measurement,
        remote_regions,
        ShardMetadataKind::TagKeys,
    )
    .await?;
    for part in remote_parts {
        if let Some(arr) = part.get("tag_keys").and_then(|v| v.as_array()) {
            for k in arr {
                if let Some(s) = k.as_str() {
                    keys.insert(s.to_string());
                }
            }
        }
    }
    Ok(keys.into_iter().collect())
}

/// `(field, type name)` pairs from local measurement metadata.
pub fn local_field_keys(meta: Option<&MeasurementMeta>) -> Vec<(String, String)> {
    let mut fields: Vec<(String, String)> = meta
        .map(|m| {
            m.field_types_as_tuples()
                .into_iter()
                .map(|(name, disc)| {
                    (
                        name,
                        FieldValue::type_name_from_discriminant(disc).to_string(),
                    )
                })
                .collect()
        })
        .unwrap_or_default();
    fields.sort();
    fields
}

/// Field keys of a measurement across every region. A node records a field
/// only when it stores the field's rows, so a node holding no copy of the
/// measurement knows none of them.
pub async fn scatter_field_keys(
    ctx: &ShardRoutingContext,
    metadata: &dyn MetadataPort,
    db: &str,
    rp: &str,
    measurement: &str,
) -> Result<Vec<(String, String)>, HyperbytedbError> {
    let local = local_field_keys(
        metadata
            .get_measurement(db, rp, measurement)
            .await?
            .as_ref(),
    );
    let map = ctx.shard_map.snapshot().await?;
    let serving = ctx
        .location_cache
        .serving_regions(&map, db, rp, measurement);
    if serving.is_empty() {
        return Ok(local);
    }

    let mut fields: BTreeMap<String, String> = BTreeMap::new();
    if serving
        .iter()
        .any(|region| region.peers.contains(&ctx.node_id))
    {
        fields.extend(local);
    }
    let remote_regions: Vec<ShardRegion> = serving
        .iter()
        .filter(|region| !region.peers.contains(&ctx.node_id))
        .cloned()
        .collect();
    let remote_parts = fetch_remote_metadata_bounded(
        ctx,
        db,
        rp,
        measurement,
        remote_regions,
        ShardMetadataKind::FieldKeys,
    )
    .await?;
    for part in remote_parts {
        let Some(arr) = part.get("field_keys").and_then(|v| v.as_array()) else {
            continue;
        };
        for pair in arr {
            if let (Some(name), Some(typ)) = (
                pair.get(0).and_then(|v| v.as_str()),
                pair.get(1).and_then(|v| v.as_str()),
            ) {
                fields
                    .entry(name.to_string())
                    .or_insert_with(|| typ.to_string());
            }
        }
    }
    Ok(fields.into_iter().collect())
}

pub async fn scatter_tag_values(
    ctx: &ShardRoutingContext,
    metadata: &dyn MetadataPort,
    db: &str,
    rp: &str,
    measurement: &str,
    tag_key: &str,
) -> Result<Vec<String>, HyperbytedbError> {
    let map = ctx.shard_map.snapshot().await?;
    // The serving view, not the raw snapshot: a snapshot contains staging
    // regions, which are in the map but must not be routed to -- the donor
    // still serves their range and this view reflects that.
    let serving = ctx
        .location_cache
        .serving_regions(&map, db, rp, measurement);
    if serving.is_empty() {
        return metadata
            .list_tag_values(db, rp, tag_key, Some(measurement))
            .await;
    }

    let mut values = BTreeSet::new();
    let has_local_peer = serving
        .iter()
        .any(|region| region.peers.contains(&ctx.node_id));
    if has_local_peer {
        for v in metadata
            .list_tag_values(db, rp, tag_key, Some(measurement))
            .await?
        {
            values.insert(v);
        }
    }

    let remote_regions: Vec<ShardRegion> = serving
        .iter()
        .filter(|region| !region.peers.contains(&ctx.node_id))
        .cloned()
        .collect();
    let remote_parts = fetch_remote_metadata_bounded(
        ctx,
        db,
        rp,
        measurement,
        remote_regions,
        ShardMetadataKind::TagValues {
            tag_key: tag_key.to_string(),
        },
    )
    .await?;
    for part in remote_parts {
        if let Some(arr) = part.get("values").and_then(|v| v.as_array()) {
            for v in arr {
                if let Some(s) = v.as_str() {
                    values.insert(s.to_string());
                }
            }
        }
    }
    Ok(values.into_iter().collect())
}

pub async fn scatter_series_keys(
    ctx: &ShardRoutingContext,
    metadata: &dyn MetadataPort,
    db: &str,
    rp: &str,
    measurement: &str,
) -> Result<Vec<String>, HyperbytedbError> {
    let map = ctx.shard_map.snapshot().await?;
    // The serving view, not the raw snapshot: a snapshot contains staging
    // regions, which are in the map but must not be routed to -- the donor
    // still serves their range and this view reflects that.
    let serving = ctx
        .location_cache
        .serving_regions(&map, db, rp, measurement);
    if serving.is_empty() {
        let series = metadata.list_series(db, rp, measurement).await?;
        return Ok(series
            .into_iter()
            .map(|(id, tags)| format!("{id}:{tags:?}"))
            .collect());
    }

    let mut keys = BTreeSet::new();
    let has_local_peer = serving
        .iter()
        .any(|region| region.peers.contains(&ctx.node_id));
    if has_local_peer {
        for (id, tags) in metadata.list_series(db, rp, measurement).await? {
            keys.insert(format!("{id}:{tags:?}"));
        }
    }

    let remote_regions: Vec<ShardRegion> = serving
        .iter()
        .filter(|region| !region.peers.contains(&ctx.node_id))
        .cloned()
        .collect();
    let remote_parts = fetch_remote_metadata_bounded(
        ctx,
        db,
        rp,
        measurement,
        remote_regions,
        ShardMetadataKind::Series,
    )
    .await?;
    for part in remote_parts {
        if let Some(arr) = part.get("series").and_then(|v| v.as_array()) {
            for s in arr {
                if let Some(k) = s.as_str() {
                    keys.insert(k.to_string());
                }
            }
        }
    }
    Ok(keys.into_iter().collect())
}
