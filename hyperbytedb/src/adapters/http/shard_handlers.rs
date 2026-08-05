use std::sync::Arc;

use axum::extract::State;
use axum::http::StatusCode;
use axum::response::IntoResponse;
use axum::Json;
use metrics::counter;

use crate::adapters::cluster::raft::types::ClusterRequest;
use crate::application::ingest_metadata::prepare_batch_metadata;
use crate::application::line_protocol::parse_line_body_to_points_limited;
use crate::application::replication_dispatch::dispatch_outbound_replication;
use crate::application::shard_peer_resolution::active_region_peer_targets;
use crate::application::shard_routing::{bootstrap_measurement_local, build_bootstrap_op};
use crate::application::wal_append::append_points_with_prepared;
use crate::application::shard_query::inject_region_series_id_predicate;
use crate::application::shard_transfer::apply_transfer;
use crate::domain::sharding::{
    RegionHeartbeat, ShardBootstrapRequest, ShardDeleteRequest, ShardMapJson, ShardMetadataKind,
    ShardMetadataRequest, ShardQueryRequest, ShardTransferPayload, ShardWriteRequest,
};
use crate::ports::replication::OutboundReplicationBatch;
use crate::ports::sharding::ShardMapPort;

use super::router::AppState;

pub async fn handle_shard_bootstrap(
    State(state): State<Arc<AppState>>,
    Json(req): Json<ShardBootstrapRequest>,
) -> impl IntoResponse {
    let Some(ctx) = state.shard_routing.as_ref() else {
        return (
            StatusCode::BAD_REQUEST,
            Json(serde_json::json!({"error": "sharding disabled"})),
        )
            .into_response();
    };

    if let Ok(map) = ctx.shard_map.snapshot().await
        && map.space(&req.db, &req.rp, &req.measurement).is_some()
    {
        return (StatusCode::OK, Json(serde_json::json!({"ok": true}))).into_response();
    }

    if let Some(ref raft) = state.raft {
        let metrics = raft.metrics().borrow().clone();
        if metrics.current_leader != Some(state.node_id) {
            if let Some(leader) = metrics.current_leader
                && let Some(m) = state.membership.as_ref()
            {
                let membership = m.read().await;
                if let Some(node) = membership.get_node(leader) {
                    let url = format!("http://{}/internal/shard/bootstrap", node.addr);
                    let Some(pc) = state.peer_client.as_ref() else {
                        return (
                            StatusCode::SERVICE_UNAVAILABLE,
                            Json(serde_json::json!({"error": "peer client unavailable"})),
                        )
                            .into_response();
                    };
                    match pc
                        .http_client()
                        .post(&url)
                        .json(&req)
                        .send()
                        .await
                    {
                            Ok(resp) if resp.status().is_success() => {
                                return (StatusCode::OK, Json(serde_json::json!({"ok": true})))
                                    .into_response();
                            }
                            Ok(resp) => {
                                return (
                                    StatusCode::BAD_GATEWAY,
                                    Json(serde_json::json!({"error": resp.status().to_string()})),
                                )
                                    .into_response();
                            }
                            Err(e) => {
                                return (
                                    StatusCode::BAD_GATEWAY,
                                    Json(serde_json::json!({"error": e.to_string()})),
                                )
                                    .into_response();
                            }
                        }
                }
            }
            return (
                StatusCode::SERVICE_UNAVAILABLE,
                Json(serde_json::json!({"error": "not raft leader"})),
            )
                .into_response();
        }

        match build_bootstrap_op(ctx, &req.db, &req.rp, &req.measurement).await {
            Ok(op) => match raft
                .client_write(ClusterRequest::ShardMapMutation(Box::new(op)))
                .await
            {
                Ok(_) => {
                    if let Ok(map) = ctx.shard_map.snapshot().await {
                        state.shard_location_cache.refresh_from_map(&map);
                    }
                    return (StatusCode::OK, Json(serde_json::json!({"ok": true}))).into_response();
                }
                Err(e) => {
                    if let Ok(map) = ctx.shard_map.snapshot().await
                        && map.space(&req.db, &req.rp, &req.measurement).is_some()
                    {
                        state.shard_location_cache.refresh_from_map(&map);
                        return (StatusCode::OK, Json(serde_json::json!({"ok": true})))
                            .into_response();
                    }
                    return (
                        StatusCode::INTERNAL_SERVER_ERROR,
                        Json(serde_json::json!({"error": e.to_string()})),
                    )
                        .into_response();
                }
            },
            Err(e) => {
                return (
                    StatusCode::INTERNAL_SERVER_ERROR,
                    Json(serde_json::json!({"error": e.to_string()})),
                )
                    .into_response();
            }
        }
    }

    match bootstrap_measurement_local(ctx, &req.db, &req.rp, &req.measurement).await {
        Ok(()) => {
            if let Ok(map) = ctx.shard_map.snapshot().await {
                state.shard_location_cache.refresh_from_map(&map);
            }
            (StatusCode::OK, Json(serde_json::json!({"ok": true}))).into_response()
        }
        Err(e) => (
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(serde_json::json!({"error": e.to_string()})),
        )
            .into_response(),
    }
}

pub async fn handle_shard_map(State(state): State<Arc<AppState>>) -> impl IntoResponse {
    let Some(shard_map) = state.shard_map.as_ref() else {
        return Json(ShardMapJson {
            map_version: 0,
            spaces: Vec::new(),
        })
        .into_response();
    };
    match shard_map.snapshot().await {
        Ok(map) => Json(ShardMapJson::from(&map)).into_response(),
        Err(e) => (
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(serde_json::json!({"error": e.to_string()})),
        )
            .into_response(),
    }
}

pub async fn handle_shard_write(
    State(state): State<Arc<AppState>>,
    Json(req): Json<ShardWriteRequest>,
) -> impl IntoResponse {
    let Some(ctx) = state.shard_routing.as_ref() else {
        return (
            StatusCode::BAD_REQUEST,
            Json(serde_json::json!({"error": "sharding disabled"})),
        )
            .into_response();
    };

    let map = match ctx.shard_map.snapshot().await {
        Ok(m) => m,
        Err(e) => {
            return (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(serde_json::json!({"error": e.to_string()})),
            )
                .into_response();
        }
    };

    let region = map.spaces.values().find_map(|s| {
        s.regions
            .iter()
            .find(|r| r.region_id == req.region_id)
            .cloned()
    });

    let Some(region) = region else {
        return (
            StatusCode::NOT_FOUND,
            Json(serde_json::json!({"error": "region not found"})),
        )
            .into_response();
    };

    if region.epoch != req.epoch {
        return (
            StatusCode::CONFLICT,
            Json(serde_json::json!({"error": "stale epoch"})),
        )
            .into_response();
    }
    if !region.peers.contains(&state.node_id) {
        return (
            StatusCode::FORBIDDEN,
            Json(serde_json::json!({"error": "not owner"})),
        )
            .into_response();
    }

    let points = match parse_line_body_to_points_limited(
        &req.body,
        req.precision.as_deref(),
        state.max_points_per_request,
    ) {
        Ok(p) => p,
        Err(e) => {
            return (
                StatusCode::BAD_REQUEST,
                Json(serde_json::json!({"error": e.to_string()})),
            )
                .into_response();
        }
    };

    if let Err(e) = prepare_batch_metadata(
        &state.metadata,
        &req.db,
        &req.rp,
        &points,
        state.ingest_cardinality,
        None,
    )
    .await
    {
        return (
            StatusCode::UNPROCESSABLE_ENTITY,
            Json(serde_json::json!({"error": e.to_string()})),
        )
            .into_response();
    }

    let wal_seq = match append_points_with_prepared(
        state.wal.as_ref(),
        Some(&state.points_sink),
        &req.db,
        &req.rp,
        points,
        state.node_id,
        state.max_points_per_request,
    )
    .await
    {
        Ok(s) => s,
        Err(e) => {
            return (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(serde_json::json!({"error": e.to_string()})),
            )
                .into_response();
        }
    };

    if let Some(pc) = state.peer_client.as_ref() {
        let membership = pc.membership().read().await;
        let targets = active_region_peer_targets(&region, state.node_id, &membership);
        drop(membership);
        let batch = OutboundReplicationBatch {
            database: req.db,
            retention_policy: req.rp,
            precision: req.precision,
            body: req.body,
            wal_seq,
            target_node_ids: Some(targets),
        };
        let _ = dispatch_outbound_replication(
            pc.clone(),
            state.node_id,
            &state.cluster_replication,
            batch,
        )
        .await;
    }

    counter!("hyperbytedb_shard_forwarded_writes_applied_total").increment(1);
    (StatusCode::NO_CONTENT, ()).into_response()
}

pub async fn handle_shard_query(
    State(state): State<Arc<AppState>>,
    Json(req): Json<ShardQueryRequest>,
) -> impl IntoResponse {
    if let Some(ctx) = state.shard_routing.as_ref() {
        let map = match ctx.shard_map.snapshot().await {
            Ok(m) => m,
            Err(e) => {
                return (
                    StatusCode::INTERNAL_SERVER_ERROR,
                    Json(serde_json::json!({"error": e.to_string()})),
                )
                    .into_response();
            }
        };
        let region = map.spaces.values().find_map(|s| {
            s.regions
                .iter()
                .find(|r| r.region_id == req.region_id)
                .cloned()
        });
        let Some(region) = region else {
            return (
                StatusCode::NOT_FOUND,
                Json(serde_json::json!({"error": "region not found"})),
            )
                .into_response();
        };
        if region.epoch != req.epoch {
            return (
                StatusCode::CONFLICT,
                Json(serde_json::json!({"error": "stale epoch"})),
            )
                .into_response();
        }
        if !region.peers.contains(&state.node_id) {
            return (
                StatusCode::FORBIDDEN,
                Json(serde_json::json!({"error": "not owner"})),
            )
                .into_response();
        }
    }

    let sql = if req.series_id_end > req.series_id_start {
        inject_region_series_id_predicate(req.select_sql, req.series_id_start, req.series_id_end)
    } else {
        req.select_sql
    };

    match state.query_port.execute_sql(&sql).await {
        Ok(raw) => (StatusCode::OK, raw).into_response(),
        Err(e) => (
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(serde_json::json!({"error": e.to_string()})),
        )
            .into_response(),
    }
}

pub async fn handle_shard_heartbeat(
    State(state): State<Arc<AppState>>,
    Json(req): Json<RegionHeartbeat>,
) -> impl IntoResponse {
    if let Some(scheduler) = state.shard_scheduler.as_ref() {
        scheduler
            .record_heartbeat(
                req.region_id,
                state.node_id,
                req.series_count,
                req.approx_bytes,
                req.write_qps,
            )
            .await;
    }
    (StatusCode::OK, Json(serde_json::json!({"ok": true}))).into_response()
}

pub async fn handle_shard_transfer(
    State(state): State<Arc<AppState>>,
    Json(req): Json<ShardTransferPayload>,
) -> impl IntoResponse {
    if let Some(ctx) = state.shard_routing.as_ref() {
        let map = match ctx.shard_map.snapshot().await {
            Ok(m) => m,
            Err(e) => {
                return (
                    StatusCode::INTERNAL_SERVER_ERROR,
                    Json(serde_json::json!({"error": e.to_string()})),
                )
                    .into_response();
            }
        };
        let region = map.spaces.values().find_map(|s| {
            s.regions
                .iter()
                .find(|r| r.region_id == req.region_id)
                .cloned()
        });
        let Some(region) = region else {
            return (
                StatusCode::NOT_FOUND,
                Json(serde_json::json!({"error": "region not found"})),
            )
                .into_response();
        };
        if region.epoch != req.epoch {
            return (
                StatusCode::CONFLICT,
                Json(serde_json::json!({"error": "stale epoch"})),
            )
                .into_response();
        }
        if !region.peers.contains(&state.node_id) {
            return (
                StatusCode::FORBIDDEN,
                Json(serde_json::json!({"error": "not owner"})),
            )
                .into_response();
        }
    }

    match apply_transfer(
        &state.metadata,
        &state.wal,
        Some(&state.points_sink),
        state.node_id,
        &req,
        state.max_points_per_request,
    )
    .await
    {
        Ok(()) => {
            counter!("hyperbytedb_shard_transfers_total").increment(1);
            (StatusCode::OK, Json(serde_json::json!({"ok": true}))).into_response()
        }
        Err(e) => (
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(serde_json::json!({"error": e.to_string()})),
        )
            .into_response(),
    }
}

pub async fn handle_shard_metadata(
    State(state): State<Arc<AppState>>,
    Json(req): Json<ShardMetadataRequest>,
) -> impl IntoResponse {
    if let Some(ctx) = state.shard_routing.as_ref() {
        let map = match ctx.shard_map.snapshot().await {
            Ok(m) => m,
            Err(e) => {
                return (
                    StatusCode::INTERNAL_SERVER_ERROR,
                    Json(serde_json::json!({"error": e.to_string()})),
                )
                    .into_response();
            }
        };
        let region = map.spaces.values().find_map(|s| {
            s.regions
                .iter()
                .find(|r| r.region_id == req.region_id)
                .cloned()
        });
        let Some(region) = region else {
            return (
                StatusCode::NOT_FOUND,
                Json(serde_json::json!({"error": "region not found"})),
            )
                .into_response();
        };
        if region.epoch != req.epoch {
            return (
                StatusCode::CONFLICT,
                Json(serde_json::json!({"error": "stale epoch"})),
            )
                .into_response();
        }
        if !region.peers.contains(&state.node_id) {
            return (
                StatusCode::FORBIDDEN,
                Json(serde_json::json!({"error": "not owner"})),
            )
                .into_response();
        }
    }

    match req.kind {
        ShardMetadataKind::TagKeys => {
            match state
                .metadata
                .list_tag_keys(&req.db, &req.rp, Some(&req.measurement))
                .await
            {
                Ok(keys) => Json(serde_json::json!({"tag_keys": keys})).into_response(),
                Err(e) => (
                    StatusCode::INTERNAL_SERVER_ERROR,
                    Json(serde_json::json!({"error": e.to_string()})),
                )
                    .into_response(),
            }
        }
        ShardMetadataKind::TagValues { tag_key } => {
            match state
                .metadata
                .list_tag_values(&req.db, &req.rp, &tag_key, Some(&req.measurement))
                .await
            {
                Ok(values) => Json(serde_json::json!({"values": values})).into_response(),
                Err(e) => (
                    StatusCode::INTERNAL_SERVER_ERROR,
                    Json(serde_json::json!({"error": e.to_string()})),
                )
                    .into_response(),
            }
        }
        ShardMetadataKind::Series => {
            match state
                .metadata
                .list_series(&req.db, &req.rp, &req.measurement)
                .await
            {
                Ok(series) => {
                    let keys: Vec<String> = series
                        .into_iter()
                        .map(|(id, tags)| format!("{id}:{tags:?}"))
                        .collect();
                    Json(serde_json::json!({"series": keys})).into_response()
                }
                Err(e) => (
                    StatusCode::INTERNAL_SERVER_ERROR,
                    Json(serde_json::json!({"error": e.to_string()})),
                )
                    .into_response(),
            }
        }
    }
}

pub async fn handle_shard_delete(
    State(state): State<Arc<AppState>>,
    Json(req): Json<ShardDeleteRequest>,
) -> impl IntoResponse {
    if let Some(ctx) = state.shard_routing.as_ref() {
        let map = match ctx.shard_map.snapshot().await {
            Ok(m) => m,
            Err(e) => {
                return (
                    StatusCode::INTERNAL_SERVER_ERROR,
                    Json(serde_json::json!({"error": e.to_string()})),
                )
                    .into_response();
            }
        };
        let region = map.spaces.values().find_map(|s| {
            s.regions
                .iter()
                .find(|r| r.region_id == req.region_id)
                .cloned()
        });
        let Some(region) = region else {
            return (
                StatusCode::NOT_FOUND,
                Json(serde_json::json!({"error": "region not found"})),
            )
                .into_response();
        };
        if region.epoch != req.epoch {
            return (
                StatusCode::CONFLICT,
                Json(serde_json::json!({"error": "stale epoch"})),
            )
                .into_response();
        }
        if region.primary != state.node_id {
            return (
                StatusCode::FORBIDDEN,
                Json(serde_json::json!({"error": "not primary"})),
            )
                .into_response();
        }
    }

    let predicate = req.predicate.as_deref().unwrap_or("");
    if let Err(e) = state
        .metadata
        .delete_series_matching(&req.db, &req.rp, Some(&req.measurement), predicate)
        .await
    {
        return (
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(serde_json::json!({"error": e.to_string()})),
        )
            .into_response();
    }
    counter!("hyperbytedb_shard_delete_applied_total").increment(1);
    (StatusCode::OK, Json(serde_json::json!({"ok": true}))).into_response()
}
