//! Coordinator scatter-gather for SHOW metadata commands under sharding.

use std::collections::BTreeSet;

use crate::application::shard_routing::ShardRoutingContext;
use crate::domain::sharding::{ShardMetadataKind, ShardMetadataRequest};
use crate::error::HyperbytedbError;
use crate::ports::metadata::MetadataPort;
use crate::ports::sharding::ShardMapPort;

async fn fetch_remote_metadata(
    ctx: &ShardRoutingContext,
    db: &str,
    rp: &str,
    measurement: &str,
    region_id: u64,
    epoch: crate::domain::sharding::ShardEpoch,
    kind: ShardMetadataKind,
) -> Result<serde_json::Value, HyperbytedbError> {
    let map = ctx.shard_map.snapshot().await?;
    let region = map
        .space(db, rp, measurement)
        .and_then(|s| s.regions.iter().find(|r| r.region_id == region_id))
        .ok_or_else(|| HyperbytedbError::ShardMap("region missing".into()))?;

    let membership = ctx.peer_client.membership().read().await;
    let addr = membership
        .get_node(region.primary)
        .map(|n| n.addr.clone())
        .ok_or_else(|| HyperbytedbError::PeerUnreachable("unknown primary".into()))?;
    drop(membership);

    let req = ShardMetadataRequest {
        db: db.to_string(),
        rp: rp.to_string(),
        measurement: measurement.to_string(),
        epoch,
        region_id,
        kind,
    };
    let url = format!("http://{addr}/internal/shard/metadata");
    let resp = ctx
        .peer_client
        .http_client()
        .post(&url)
        .json(&req)
        .send()
        .await
        .map_err(|e| HyperbytedbError::PeerUnreachable(e.to_string()))?;
    if !resp.status().is_success() {
        return Err(HyperbytedbError::Internal(
            format!("metadata scatter failed: {}", resp.status()).into(),
        ));
    }
    resp.json()
        .await
        .map_err(|e| HyperbytedbError::Internal(e.to_string().into()))
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
    for region in &space.regions {
        if region.peers.contains(&ctx.node_id) {
            for k in metadata.list_tag_keys(db, rp, Some(measurement)).await? {
                keys.insert(k);
            }
        } else {
            let part = fetch_remote_metadata(
                ctx,
                db,
                rp,
                measurement,
                region.region_id,
                region.epoch,
                ShardMetadataKind::TagKeys,
            )
            .await?;
            if let Some(arr) = part.get("tag_keys").and_then(|v| v.as_array()) {
                for k in arr {
                    if let Some(s) = k.as_str() {
                        keys.insert(s.to_string());
                    }
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
    for region in &space.regions {
        if region.peers.contains(&ctx.node_id) {
            for v in metadata
                .list_tag_values(db, rp, tag_key, Some(measurement))
                .await?
            {
                values.insert(v);
            }
        } else {
            let part = fetch_remote_metadata(
                ctx,
                db,
                rp,
                measurement,
                region.region_id,
                region.epoch,
                ShardMetadataKind::TagValues {
                    tag_key: tag_key.to_string(),
                },
            )
            .await?;
            if let Some(arr) = part.get("values").and_then(|v| v.as_array()) {
                for v in arr {
                    if let Some(s) = v.as_str() {
                        values.insert(s.to_string());
                    }
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
    for region in &space.regions {
        if region.peers.contains(&ctx.node_id) {
            for (id, tags) in metadata.list_series(db, rp, measurement).await? {
                keys.insert(format!("{id}:{tags:?}"));
            }
        } else {
            let part = fetch_remote_metadata(
                ctx,
                db,
                rp,
                measurement,
                region.region_id,
                region.epoch,
                ShardMetadataKind::Series,
            )
            .await?;
            if let Some(arr) = part.get("series").and_then(|v| v.as_array()) {
                for s in arr {
                    if let Some(k) = s.as_str() {
                        keys.insert(k.to_string());
                    }
                }
            }
        }
    }
    Ok(keys.into_iter().collect())
}
