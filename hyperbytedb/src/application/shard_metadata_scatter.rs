//! Coordinator scatter-gather for SHOW metadata commands under sharding.

use std::collections::BTreeSet;

use futures::future::try_join_all;

use crate::application::shard_peer_resolution::{RegionTargetRole, ScatterKind};
use crate::application::shard_routing::{reload_region, scatter_to_region_peers, ShardRoutingContext};
use crate::domain::sharding::{ShardMetadataKind, ShardMetadataRequest, ShardRegion};
use crate::error::HyperbytedbError;
use crate::ports::metadata::MetadataPort;
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

pub async fn scatter_tag_keys(
    ctx: &ShardRoutingContext,
    metadata: &dyn MetadataPort,
    db: &str,
    rp: &str,
    measurement: &str,
) -> Result<Vec<String>, HyperbytedbError> {
    let map = ctx.shard_map.snapshot().await?;
    let Some(space) = map.space(db, rp, measurement) else {
        return metadata
            .list_tag_keys(db, rp, Some(measurement))
            .await;
    };

    let mut keys = BTreeSet::new();
    let has_local_peer = space
        .regions
        .iter()
        .any(|region| region.peers.contains(&ctx.node_id));
    if has_local_peer {
        for k in metadata
            .list_tag_keys(db, rp, Some(measurement))
            .await?
        {
            keys.insert(k);
        }
    }

    let remote_regions: Vec<_> = space
        .regions
        .iter()
        .filter(|region| !region.peers.contains(&ctx.node_id))
        .collect();
    let remote_parts = try_join_all(remote_regions.iter().map(|region| {
        fetch_remote_metadata(
            ctx,
            db,
            rp,
            measurement,
            region,
            ShardMetadataKind::TagKeys,
        )
    }))
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

pub async fn scatter_tag_values(
    ctx: &ShardRoutingContext,
    metadata: &dyn MetadataPort,
    db: &str,
    rp: &str,
    measurement: &str,
    tag_key: &str,
) -> Result<Vec<String>, HyperbytedbError> {
    let map = ctx.shard_map.snapshot().await?;
    let Some(space) = map.space(db, rp, measurement) else {
        return metadata
            .list_tag_values(db, rp, tag_key, Some(measurement))
            .await;
    };

    let mut values = BTreeSet::new();
    let has_local_peer = space
        .regions
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

    let remote_regions: Vec<_> = space
        .regions
        .iter()
        .filter(|region| !region.peers.contains(&ctx.node_id))
        .collect();
    let remote_parts = try_join_all(remote_regions.iter().map(|region| {
        fetch_remote_metadata(
            ctx,
            db,
            rp,
            measurement,
            region,
            ShardMetadataKind::TagValues {
                tag_key: tag_key.to_string(),
            },
        )
    }))
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
    let Some(space) = map.space(db, rp, measurement) else {
        let series = metadata.list_series(db, rp, measurement).await?;
        return Ok(series
            .into_iter()
            .map(|(id, tags)| format!("{id}:{tags:?}"))
            .collect());
    };

    let mut keys = BTreeSet::new();
    let has_local_peer = space
        .regions
        .iter()
        .any(|region| region.peers.contains(&ctx.node_id));
    if has_local_peer {
        for (id, tags) in metadata.list_series(db, rp, measurement).await? {
            keys.insert(format!("{id}:{tags:?}"));
        }
    }

    let remote_regions: Vec<_> = space
        .regions
        .iter()
        .filter(|region| !region.peers.contains(&ctx.node_id))
        .collect();
    let remote_parts = try_join_all(remote_regions.iter().map(|region| {
        fetch_remote_metadata(
            ctx,
            db,
            rp,
            measurement,
            region,
            ShardMetadataKind::Series,
        )
    }))
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
