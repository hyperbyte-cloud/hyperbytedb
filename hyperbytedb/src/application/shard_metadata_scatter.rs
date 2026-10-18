//! Coordinator scatter-gather for SHOW metadata commands under sharding.

use std::collections::{BTreeMap, BTreeSet};

use futures::future::try_join_all;

const METADATA_SCATTER_CONCURRENCY: usize = 32;

use crate::application::shard_peer_resolution::{RegionTargetRole, ScatterKind};
use crate::application::shard_routing::{
    ShardRoutingContext, reload_region, scatter_to_region_peers,
};
use crate::domain::point::FieldValue;
use crate::domain::series::SeriesKey;
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

/// This node's schema for the measurement or, when it has none, the schemas
/// of the serving regions' owners merged. With sharding a node has a
/// measurement only once rows for it land there, and CREATE MATERIALIZED
/// VIEW needs the schema on the node that takes the statement.
pub async fn scatter_measurement_meta(
    ctx: &ShardRoutingContext,
    metadata: &dyn MetadataPort,
    db: &str,
    rp: &str,
    measurement: &str,
) -> Result<Option<MeasurementMeta>, HyperbytedbError> {
    if let Some(local) = metadata.get_measurement(db, rp, measurement).await? {
        return Ok(Some(local));
    }
    let map = ctx.shard_map.snapshot().await?;
    let remote_regions: Vec<ShardRegion> = ctx
        .location_cache
        .serving_regions(&map, db, rp, measurement)
        .iter()
        .filter(|region| !region.peers.contains(&ctx.node_id))
        .cloned()
        .collect();
    let parts = fetch_remote_metadata_bounded(
        ctx,
        db,
        rp,
        measurement,
        remote_regions,
        ShardMetadataKind::Measurement,
    )
    .await?;
    let mut merged: Option<MeasurementMeta> = None;
    for part in parts {
        let Some(meta) = part
            .get("measurement")
            .cloned()
            .and_then(|v| serde_json::from_value::<Option<MeasurementMeta>>(v).ok())
            .flatten()
        else {
            continue;
        };
        match merged.as_mut() {
            None => merged = Some(meta),
            Some(acc) => merge_measurement_meta(acc, meta),
        }
    }
    Ok(merged)
}

/// Union of two nodes' schemas for one measurement; `acc` wins on conflicts.
fn merge_measurement_meta(acc: &mut MeasurementMeta, other: MeasurementMeta) {
    for (k, v) in other.field_types {
        acc.field_types.entry(k).or_insert(v);
    }
    for k in other.tag_keys {
        if !acc.tag_keys.contains(&k) {
            acc.tag_keys.push(k);
        }
    }
    for (k, v) in other.field_rollups {
        acc.field_rollups.entry(k).or_insert(v);
    }
    for (k, v) in other.mean_fields {
        acc.mean_fields.entry(k).or_insert(v);
    }
    acc.materialized |= other.materialized;
    if acc.materialized_rp.is_none() {
        acc.materialized_rp = other.materialized_rp;
    }
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

/// Every series' key (`measurement,tag=value,...`, as SHOW SERIES prints it)
/// across the cluster, each once: this node's own, and one peer's for each
/// serving region it does not hold. A peer on an older build sends no tags;
/// its keys are passed through as it formats them.
pub async fn scatter_series_keys(
    ctx: &ShardRoutingContext,
    metadata: &dyn MetadataPort,
    db: &str,
    rp: &str,
    measurement: &str,
) -> Result<Vec<String>, HyperbytedbError> {
    let canonical =
        |tags: &BTreeMap<String, String>| SeriesKey::new(measurement, tags).to_canonical();
    let map = ctx.shard_map.snapshot().await?;
    // The serving view, not the raw snapshot: a snapshot contains staging
    // regions, which are in the map but must not be routed to -- the donor
    // still serves their range and this view reflects that.
    let serving = ctx
        .location_cache
        .serving_regions(&map, db, rp, measurement);
    let mut keys = BTreeSet::new();
    if serving.is_empty() || serving.iter().any(|r| r.peers.contains(&ctx.node_id)) {
        for (_, tags) in metadata.list_series(db, rp, measurement).await? {
            keys.insert(canonical(&tags));
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
        if let Some(tagged) = part.get("tagged").and_then(|v| v.as_array()) {
            for s in tagged {
                if let Ok(tags) =
                    serde_json::from_value::<BTreeMap<String, String>>(s["tags"].clone())
                {
                    keys.insert(canonical(&tags));
                }
            }
        } else if let Some(arr) = part.get("series").and_then(|v| v.as_array()) {
            keys.extend(arr.iter().filter_map(|k| k.as_str().map(str::to_string)));
        }
    }
    Ok(keys.into_iter().collect())
}

/// How many series the measurement has across the cluster: the sum, over
/// its serving regions, of each region's series in its own range. Ranges do
/// not overlap, so a series counts once however many nodes hold it (replicas,
/// or a copy a node kept after handing a region over). This node counts its
/// own regions from its series index; each other region's peer is asked for
/// its count, and a peer on an older build for its series, counted here.
pub async fn scatter_series_count(
    ctx: &ShardRoutingContext,
    metadata: &dyn MetadataPort,
    db: &str,
    rp: &str,
    measurement: &str,
) -> Result<u64, HyperbytedbError> {
    let map = ctx.shard_map.snapshot().await?;
    let serving = ctx
        .location_cache
        .serving_regions(&map, db, rp, measurement);
    if serving.is_empty() {
        return metadata
            .count_series_ids_in_range(db, rp, measurement, 0, u64::MAX)
            .await;
    }
    let mut total = 0u64;
    let mut remote = Vec::new();
    for region in serving {
        if region.peers.contains(&ctx.node_id) {
            total += metadata
                .count_series_ids_in_range(db, rp, measurement, region.start, region.end)
                .await?;
        } else {
            remote.push(region.clone());
        }
    }
    for chunk in remote.chunks(METADATA_SCATTER_CONCURRENCY) {
        let counts = try_join_all(chunk.iter().map(|region| async move {
            match fetch_remote_metadata(
                ctx,
                db,
                rp,
                measurement,
                region,
                ShardMetadataKind::SeriesCount,
            )
            .await
            {
                Ok(part) if part.get("count").is_some() => {
                    part["count"].as_u64().ok_or_else(|| {
                        HyperbytedbError::Internal("series count is not a number".into())
                    })
                }
                // An older peer: count its series in the region's range.
                _ => {
                    let part = fetch_remote_metadata(
                        ctx,
                        db,
                        rp,
                        measurement,
                        region,
                        ShardMetadataKind::Series,
                    )
                    .await?;
                    Ok(series_ids_of(&part)
                        .into_iter()
                        .filter(|id| *id >= region.start && *id < region.end)
                        .count() as u64)
                }
            }
        }))
        .await?;
        total += counts.into_iter().sum::<u64>();
    }
    Ok(total)
}

/// The series ids in a `Series` metadata reply: from `tagged`, else from the
/// `<id>:<tags>` strings of a build without it.
fn series_ids_of(part: &serde_json::Value) -> Vec<u64> {
    if let Some(tagged) = part.get("tagged").and_then(|v| v.as_array()) {
        return tagged
            .iter()
            .filter_map(|t| t.get("id")?.as_u64())
            .collect();
    }
    part.get("series")
        .and_then(|v| v.as_array())
        .map(|arr| {
            arr.iter()
                .filter_map(|s| s.as_str()?.split(':').next()?.parse().ok())
                .collect()
        })
        .unwrap_or_default()
}

/// Every series' tags for the measurement across the cluster, each series
/// once: this node's own, and one peer's for each serving region it does not
/// hold. For SHOW ... WHERE. A peer on an older build returns no `tagged`
/// list and is reported, not read as having no series.
pub async fn scatter_series_tags(
    ctx: &ShardRoutingContext,
    metadata: &dyn MetadataPort,
    db: &str,
    rp: &str,
    measurement: &str,
) -> Result<Vec<BTreeMap<String, String>>, HyperbytedbError> {
    let mut by_id: std::collections::HashMap<u64, BTreeMap<String, String>> =
        std::collections::HashMap::new();
    let map = ctx.shard_map.snapshot().await?;
    let serving = ctx
        .location_cache
        .serving_regions(&map, db, rp, measurement);
    if serving.is_empty() || serving.iter().any(|r| r.peers.contains(&ctx.node_id)) {
        by_id.extend(metadata.list_series(db, rp, measurement).await?);
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
        let Some(tagged) = part.get("tagged").and_then(|v| v.as_array()) else {
            return Err(HyperbytedbError::PeerUnreachable(
                "a peer returned series without tags (older build); retry when all nodes run this one"
                    .into(),
            ));
        };
        for s in tagged {
            if let (Some(id), Ok(tags)) = (
                s.get("id").and_then(serde_json::Value::as_u64),
                serde_json::from_value::<BTreeMap<String, String>>(s["tags"].clone()),
            ) {
                by_id.insert(id, tags);
            }
        }
    }
    Ok(by_id.into_values().collect())
}
