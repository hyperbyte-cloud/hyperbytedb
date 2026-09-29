use std::sync::Arc;

use axum::Json;
use axum::body::{Body, Bytes};
use axum::extract::State;
use axum::http::{Request, StatusCode, header};
use axum::response::IntoResponse;
use metrics::counter;

use crate::adapters::cluster::raft::types::ClusterRequest;
use crate::application::ingest_metadata::prepare_batch_metadata;
use crate::application::line_protocol::parse_line_body_to_points_limited;
use crate::application::replication_dispatch::dispatch_outbound_replication;
use crate::application::shard_peer_resolution::active_region_peer_targets;
use crate::application::shard_query::inject_region_series_id_predicate;
use crate::application::shard_routing::{bootstrap_measurement_local, build_bootstrap_op};
use crate::application::shard_transfer::apply_transfer;
use crate::application::sharded_mv_backfill::apply_mv_backfill_sql;
use crate::application::wal_append::append_points_with_prepared;
use crate::domain::point::Point;
use crate::domain::sharding::{
    MeasurementKey, RegionHeartbeat, ShardBootstrapRequest, ShardDeleteRequest, ShardEpoch,
    ShardMap, ShardMapJson, ShardMetadataKind, ShardMetadataRequest, ShardMvBackfillRequest,
    ShardQueryRequest, ShardRegion, ShardRehomeRequest, ShardTransferPayload, ShardWriteBatch,
    ShardWriteBatchResponse, ShardWritePartResult, ShardWriteRequest,
};
use crate::error::HyperbytedbError;
use crate::ports::replication::OutboundReplicationBatch;
use crate::ports::sharding::ShardMapPort;

use super::router::AppState;

fn sharding_disabled() -> axum::response::Response {
    (
        StatusCode::BAD_REQUEST,
        Json(serde_json::json!({"error": "sharding disabled"})),
    )
        .into_response()
}

fn lookup_region(
    map: &ShardMap,
    db: &str,
    rp: &str,
    measurement: &str,
    region_id: u64,
) -> Option<ShardRegion> {
    map.space(db, rp, measurement)
        .and_then(|space| space.regions.iter().find(|r| r.region_id == region_id))
        .cloned()
}

fn lookup_region_by_id(map: &ShardMap, region_id: u64) -> Option<ShardRegion> {
    map.spaces.values().find_map(|space| {
        space
            .regions
            .iter()
            .find(|r| r.region_id == region_id)
            .cloned()
    })
}

fn lookup_region_in_db_rp(
    map: &ShardMap,
    db: &str,
    rp: &str,
    region_id: u64,
) -> Option<ShardRegion> {
    map.spaces.values().find_map(|space| {
        if space.key.db != db || space.key.rp != rp {
            return None;
        }
        space
            .regions
            .iter()
            .find(|r| r.region_id == region_id)
            .cloned()
    })
}

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
                    match pc.http_client().post(&url).json(&req).send().await {
                        Ok(resp) if resp.status().is_success() => {
                            return (StatusCode::OK, Json(serde_json::json!({"ok": true})))
                                .into_response();
                        }
                        Ok(resp) => {
                            let status = resp.status();
                            let body = resp.text().await.unwrap_or_default();
                            return (
                                StatusCode::BAD_GATEWAY,
                                Json(serde_json::json!({
                                    "error": format!("leader bootstrap failed: {status}"),
                                    "leader_error": body,
                                })),
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
            next_region_id: 1,
            spaces: Vec::new(),
        })
        .into_response();
    };
    match shard_map.snapshot().await {
        Ok(map) => Json(ShardMapJson::from(map.as_ref())).into_response(),
        Err(e) => (
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(serde_json::json!({"error": e.to_string()})),
        )
            .into_response(),
    }
}

fn header_value(headers: &axum::http::HeaderMap, name: &str) -> Result<String, String> {
    headers
        .get(name)
        .and_then(|v| v.to_str().ok())
        .map(str::to_string)
        .ok_or_else(|| format!("missing header {name}"))
}

fn shard_write_from_line_protocol_request(
    headers: &axum::http::HeaderMap,
    body: Bytes,
) -> Result<ShardWriteRequest, String> {
    let db = header_value(headers, "x-hyperbytedb-shard-db")?;
    let rp = header_value(headers, "x-hyperbytedb-shard-rp")?;
    let region_id = header_value(headers, "x-hyperbytedb-shard-region-id")?
        .parse::<u64>()
        .map_err(|_| "invalid x-hyperbytedb-shard-region-id".to_string())?;
    let epoch: ShardEpoch =
        serde_json::from_str(&header_value(headers, "x-hyperbytedb-shard-epoch")?)
            .map_err(|e| format!("invalid x-hyperbytedb-shard-epoch: {e}"))?;
    let precision = headers
        .get("x-hyperbytedb-shard-precision")
        .and_then(|v| v.to_str().ok())
        .map(str::to_string);
    Ok(ShardWriteRequest {
        db,
        rp,
        precision,
        epoch,
        region_id,
        body: body.to_vec(),
    })
}

pub async fn handle_shard_write(
    State(state): State<Arc<AppState>>,
    request: Request<Body>,
) -> impl IntoResponse {
    let (parts, body) = request.into_parts();
    let body_bytes = match axum::body::to_bytes(body, state.replicate_body_limit_bytes.max(1)).await
    {
        Ok(b) => b,
        Err(e) => {
            return (
                StatusCode::BAD_REQUEST,
                Json(serde_json::json!({"error": e.to_string()})),
            )
                .into_response();
        }
    };
    let req = if parts
        .headers
        .get(header::CONTENT_TYPE)
        .and_then(|v| v.to_str().ok())
        .is_some_and(|ct| ct.starts_with("application/octet-stream"))
    {
        match shard_write_from_line_protocol_request(&parts.headers, body_bytes) {
            Ok(r) => r,
            Err(e) => {
                return (
                    StatusCode::BAD_REQUEST,
                    Json(serde_json::json!({"error": e})),
                )
                    .into_response();
            }
        }
    } else {
        match serde_json::from_slice::<ShardWriteRequest>(&body_bytes) {
            Ok(r) => r,
            Err(e) => {
                return (
                    StatusCode::BAD_REQUEST,
                    Json(serde_json::json!({"error": e.to_string()})),
                )
                    .into_response();
            }
        }
    };

    apply_shard_write(state, req).await
}

/// POST /internal/shard/write-batch: several regions' forwarded writes for
/// this node as primary, bincode-encoded [`ShardWriteBatch`].
///
/// Each part is checked (epoch, ownership, primary) and parsed on its own,
/// and its status is reported per part: one stale region must not fail the
/// others. Parts that pass and share a database and retention policy are
/// then prepared and appended to the WAL together.
///
/// Appending each part on its own made a client batch cost one metadata
/// pass, one Arrow build and one WAL append per region: at N=12 on n3 a
/// 1,000-point batch split into ~61 parts of ~16 points. The WAL does not
/// need an entry per region -- catch-up copies the whole WAL, and region
/// watermarks are the node's last sequence -- so the parts share one entry.
/// Replication stays per part, because each region has its own peers; the
/// replication sender merges them per peer.
pub async fn handle_shard_write_batch(
    State(state): State<Arc<AppState>>,
    headers: axum::http::HeaderMap,
    body: Bytes,
) -> impl IntoResponse {
    let batch: ShardWriteBatch = match bincode::deserialize(&body) {
        Ok(b) => b,
        Err(e) => {
            return (
                StatusCode::BAD_REQUEST,
                Json(serde_json::json!({"error": format!("bad write batch: {e}")})),
            )
                .into_response();
        }
    };
    counter!("hyperbytedb_shard_write_batches_total").increment(1);
    counter!("hyperbytedb_shard_write_batch_parts_total").increment(batch.parts.len() as u64);
    let span = tracing::debug_span!(
        target: crate::telemetry::TARGET,
        "shard_write_batch",
        node = state.node_id,
        parts = batch.parts.len()
    );
    crate::telemetry::set_parent(&span, &headers);
    let results =
        tracing::Instrument::instrument(apply_shard_write_batch(&state, batch.parts), span).await;
    (StatusCode::OK, Json(ShardWriteBatchResponse { results })).into_response()
}

fn part_result(region_id: u64, status: StatusCode, error: Option<String>) -> ShardWritePartResult {
    ShardWritePartResult {
        region_id,
        status: status.as_u16(),
        error: error.map(|e| serde_json::json!({ "error": e }).to_string()),
    }
}

/// A forwarded part that passed its checks, parsed and waiting for its
/// group's WAL append.
struct ReadyPart {
    index: usize,
    req: ShardWriteRequest,
    region: ShardRegion,
    point_count: u64,
}

/// Forwarded parts for one (db, rp) that will share a WAL append.
struct ForwardGroup {
    db: String,
    rp: String,
    members: Vec<ReadyPart>,
    points: Vec<Point>,
}

async fn apply_shard_write_batch(
    state: &Arc<AppState>,
    parts: Vec<ShardWriteRequest>,
) -> Vec<ShardWritePartResult> {
    let mut results: Vec<Option<ShardWritePartResult>> = vec![None; parts.len()];
    let Some(ctx) = state.shard_routing.as_ref() else {
        return parts
            .iter()
            .map(|p| {
                part_result(
                    p.region_id,
                    StatusCode::BAD_REQUEST,
                    Some("sharding disabled".into()),
                )
            })
            .collect();
    };
    let map = match ctx.shard_map.snapshot().await {
        Ok(m) => m,
        Err(e) => {
            return parts
                .iter()
                .map(|p| {
                    part_result(
                        p.region_id,
                        StatusCode::INTERNAL_SERVER_ERROR,
                        Some(e.to_string()),
                    )
                })
                .collect();
        }
    };

    let mut stage =
        crate::application::stage_timer::StageTimer::new("hyperbytedb_shard_primary_stage_seconds");
    // Check and parse each part; group the good ones by (db, rp).
    let mut groups: Vec<ForwardGroup> = Vec::new();
    for (index, req) in parts.into_iter().enumerate() {
        let region = match forwarded_write_region(&map, state.node_id, &req) {
            Ok(r) => r,
            Err((status, msg)) => {
                results[index] = Some(part_result(req.region_id, status, Some(msg.into())));
                continue;
            }
        };
        let points = match parse_line_body_to_points_limited(
            &req.body,
            req.precision.as_deref(),
            state.max_points_per_request,
        ) {
            Ok(p) => p,
            Err(e) => {
                results[index] = Some(part_result(
                    req.region_id,
                    StatusCode::BAD_REQUEST,
                    Some(e.to_string()),
                ));
                continue;
            }
        };
        let point_count = points.len() as u64;
        let ready = ReadyPart {
            index,
            req,
            region,
            point_count,
        };
        // Keep a group within one request's point limit (0 means the
        // default, as in `validate_point_count`).
        let limit = match state.max_points_per_request {
            0 => crate::config::default_max_points_per_request(),
            n => n,
        };
        match groups.iter_mut().find(|g| {
            g.db == ready.req.db
                && g.rp == ready.req.rp
                && g.points.len() + point_count as usize <= limit
        }) {
            Some(g) => {
                g.members.push(ready);
                g.points.extend(points);
            }
            None => groups.push(ForwardGroup {
                db: ready.req.db.clone(),
                rp: ready.req.rp.clone(),
                members: vec![ready],
                points,
            }),
        }
    }

    stage.lap("check_parse");
    for ForwardGroup {
        db,
        rp,
        members,
        points,
    } in groups
    {
        let outcome = tracing::Instrument::instrument(
            append_forwarded_group(state, &db, &rp, points),
            tracing::debug_span!(target: crate::telemetry::TARGET, "metadata_wal"),
        )
        .await;
        stage.lap("metadata_wal");
        let wal_seq = match outcome {
            Ok(seq) => seq,
            Err((status, msg)) => {
                for m in members {
                    results[m.index] =
                        Some(part_result(m.req.region_id, status, Some(msg.clone())));
                }
                continue;
            }
        };
        let replicated = members.into_iter().map(|m| {
            let state = state.clone();
            async move {
                let region_id = m.req.region_id;
                let result = replicate_forwarded_part(&state, m.req, &m.region, wal_seq).await;
                if result.is_ok() {
                    if let Some(ctx) = state.shard_routing.as_ref() {
                        ctx.region_write_stats.record(region_id, m.point_count);
                    }
                    counter!("hyperbytedb_shard_forwarded_writes_applied_total").increment(1);
                }
                (m.index, region_id, result)
            }
        });
        let replicated = tracing::Instrument::instrument(
            futures::future::join_all(replicated),
            tracing::debug_span!(target: crate::telemetry::TARGET, "replicate"),
        )
        .await;
        stage.lap("replicate");
        for (index, region_id, result) in replicated {
            results[index] = Some(match result {
                Ok(()) => part_result(region_id, StatusCode::NO_CONTENT, None),
                Err(e) => part_result(
                    region_id,
                    StatusCode::INTERNAL_SERVER_ERROR,
                    Some(e.to_string()),
                ),
            });
        }
    }

    results
        .into_iter()
        .map(|r| {
            r.unwrap_or_else(|| {
                part_result(
                    0,
                    StatusCode::INTERNAL_SERVER_ERROR,
                    Some("part not processed".into()),
                )
            })
        })
        .collect()
}

/// Metadata pass and WAL append for one group of forwarded parts.
async fn append_forwarded_group(
    state: &AppState,
    db: &str,
    rp: &str,
    points: Vec<Point>,
) -> Result<u64, (StatusCode, String)> {
    prepare_batch_metadata(
        &state.metadata,
        db,
        rp,
        &points,
        state.ingest_cardinality,
        Some(&state.ingest_schema_cache),
    )
    .await
    .map_err(|e| (StatusCode::UNPROCESSABLE_ENTITY, e.to_string()))?;
    append_points_with_prepared(
        state.wal.as_ref(),
        Some(&state.points_sink),
        db,
        rp,
        points,
        state.node_id,
        state.max_points_per_request,
    )
    .await
    .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()))
}

/// Replicate one forwarded part to its region's peers.
async fn replicate_forwarded_part(
    state: &AppState,
    req: ShardWriteRequest,
    region: &ShardRegion,
    wal_seq: u64,
) -> Result<(), HyperbytedbError> {
    let Some(pc) = state.peer_client.as_ref() else {
        return Ok(());
    };
    let membership = pc.membership().read().await;
    let targets = active_region_peer_targets(region, state.node_id, &membership);
    drop(membership);
    dispatch_outbound_replication(
        pc.clone(),
        state.node_id,
        &state.cluster_replication,
        OutboundReplicationBatch {
            database: req.db,
            retention_policy: req.rp,
            precision: req.precision,
            body: req.body,
            wal_seq,
            target_node_ids: Some(targets),
            region_id: Some(region.region_id),
        },
    )
    .await
}

/// The region a forwarded write targets, if this node may accept it: the
/// region exists at the sender's epoch, this node holds it, and is its
/// primary.
fn forwarded_write_region(
    map: &ShardMap,
    node_id: u64,
    req: &ShardWriteRequest,
) -> Result<ShardRegion, (StatusCode, &'static str)> {
    let Some(region) = lookup_region_in_db_rp(map, &req.db, &req.rp, req.region_id) else {
        return Err((StatusCode::NOT_FOUND, "region not found"));
    };
    if region.epoch != req.epoch {
        return Err((StatusCode::CONFLICT, "stale epoch"));
    }
    if !region.peers.contains(&node_id) {
        return Err((StatusCode::FORBIDDEN, "not owner"));
    }
    // Single-writer invariant: only the region primary may accept a forwarded
    // write. A replica accepting one would append to its own WAL concurrently
    // with the primary, diverging watermarks and replication. 409 (not 403)
    // so the coordinator's forward path treats it as stale ownership, refreshes
    // the region from the shard map, and re-routes to the current primary.
    if region.primary != node_id {
        return Err((StatusCode::CONFLICT, "not region primary"));
    }
    Ok(region)
}

async fn apply_shard_write(
    state: Arc<AppState>,
    req: ShardWriteRequest,
) -> axum::response::Response {
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

    let region = match forwarded_write_region(&map, state.node_id, &req) {
        Ok(r) => r,
        Err((status, msg)) => {
            return (status, Json(serde_json::json!({"error": msg}))).into_response();
        }
    };

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
        Some(&state.ingest_schema_cache),
    )
    .await
    {
        return (
            StatusCode::UNPROCESSABLE_ENTITY,
            Json(serde_json::json!({"error": e.to_string()})),
        )
            .into_response();
    }

    let point_count = points.len() as u64;

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
            region_id: Some(region.region_id),
        };
        if let Err(e) = dispatch_outbound_replication(
            pc.clone(),
            state.node_id,
            &state.cluster_replication,
            batch,
        )
        .await
        {
            return (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(serde_json::json!({"error": e.to_string()})),
            )
                .into_response();
        }
    }

    if let Some(ctx) = state.shard_routing.as_ref() {
        ctx.region_write_stats.record(req.region_id, point_count);
    }

    counter!("hyperbytedb_shard_forwarded_writes_applied_total").increment(1);
    (StatusCode::NO_CONTENT, ()).into_response()
}

pub async fn handle_shard_query(
    State(state): State<Arc<AppState>>,
    Json(req): Json<ShardQueryRequest>,
) -> impl IntoResponse {
    let Some(ctx) = state.shard_routing.as_ref() else {
        return sharding_disabled();
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
    let region = lookup_region(&map, &req.db, &req.rp, &req.measurement, req.region_id);
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

    let sql = if req.series_id_end > req.series_id_start {
        inject_region_series_id_predicate(req.select_sql, req.series_id_start, req.series_id_end)
    } else {
        req.select_sql
    };

    match state.query_port.execute_sql(&sql).await {
        Ok(raw) => (StatusCode::OK, raw).into_response(),
        Err(e) => {
            tracing::error!(error = %e, sql = %sql, "shard query execute_sql failed");
            (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(serde_json::json!({"error": e.to_string()})),
            )
                .into_response()
        }
    }
}

pub async fn handle_shard_mv_backfill(
    State(state): State<Arc<AppState>>,
    Json(req): Json<ShardMvBackfillRequest>,
) -> impl IntoResponse {
    let Some(ctx) = state.shard_routing.as_ref() else {
        return sharding_disabled();
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
    let region = lookup_region_in_db_rp(&map, &req.db, &req.rp, req.region_id);
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

    match apply_mv_backfill_sql(&state.points_sink, &state.metadata, &state.query_port, &req).await
    {
        Ok(()) => (StatusCode::NO_CONTENT, ()).into_response(),
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
    if req.node_id == 0 {
        return (
            StatusCode::BAD_REQUEST,
            Json(serde_json::json!({"error": "node_id required"})),
        )
            .into_response();
    }
    let mut stale_epoch = false;
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
        let region = lookup_region_by_id(&map, req.region_id);
        let Some(region) = region else {
            return (
                StatusCode::NOT_FOUND,
                Json(serde_json::json!({"error": "region not found"})),
            )
                .into_response();
        };
        // Membership is what authorises the report; epoch only says whose view
        // is newer. Checked in that order on purpose.
        let reporter = req.node_id;
        if !region.peers.contains(&reporter) {
            return (
                StatusCode::FORBIDDEN,
                Json(serde_json::json!({"error": "reporter not a region peer"})),
            )
                .into_response();
        }
        // A peer's series count is true whatever epoch it thinks it is at, and
        // the epoch moves on every placement step — so dropping these reports
        // blinded the scheduler to exactly the regions it was busy moving. The
        // response is still 409 so the reporter resyncs its map; the row is
        // recorded first.
        stale_epoch = region.epoch != req.epoch;
    }

    if let Some(scheduler) = state.shard_scheduler.as_ref() {
        let reporter = req.node_id;
        scheduler
            .record_heartbeat(
                req.region_id,
                reporter,
                req.series_count,
                req.approx_bytes,
                req.write_qps,
            )
            .await;
        scheduler
            .record_split_hint(req.region_id, reporter, req.split_key_hint)
            .await;
    }
    if stale_epoch {
        counter!("hyperbytedb_shard_heartbeat_stale_epoch_total").increment(1);
        return (
            StatusCode::CONFLICT,
            Json(serde_json::json!({"error": "stale epoch", "recorded": true})),
        )
            .into_response();
    }
    (StatusCode::OK, Json(serde_json::json!({"ok": true}))).into_response()
}

pub async fn handle_shard_transfer(
    State(state): State<Arc<AppState>>,
    Json(req): Json<ShardTransferPayload>,
) -> impl IntoResponse {
    if req.stage {
        // Pre-commit staging (split optimization, and replica placement onto a
        // joiner): the range is not owned by the destination in the committed
        // map yet, so region/epoch checks are skipped. The only guard rail is
        // that the sender must be a known member; rows outside `[start, end)`
        // are filtered during apply.
        //
        // There is deliberately no local-measurement check. A joiner receiving
        // a measurement it has never seen has no catalog row yet, and
        // `apply_transfer_push` creates one via `prepare_batch_metadata`.
        let Some(membership) = state.membership.as_ref() else {
            return (
                StatusCode::SERVICE_UNAVAILABLE,
                Json(serde_json::json!({"error": "membership unavailable"})),
            )
                .into_response();
        };
        let m = membership.read().await;
        if m.get_node(req.source_node_id).is_none() {
            return (
                StatusCode::FORBIDDEN,
                Json(serde_json::json!({"error": "unknown source node"})),
            )
                .into_response();
        }
        drop(m);
    } else {
        let Some(ctx) = state.shard_routing.as_ref() else {
            return sharding_disabled();
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
        let region = lookup_region(&map, &req.db, &req.rp, &req.measurement, req.region_id);
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
        Some(state.mv_service.as_ref()),
        state.node_id,
        &req,
        state.max_points_per_request,
    )
    .await
    {
        Ok(applied) => {
            counter!("hyperbytedb_shard_transfers_total").increment(1);
            (
                StatusCode::OK,
                Json(serde_json::json!({"ok": true, "applied": applied})),
            )
                .into_response()
        }
        Err(e) => (
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(serde_json::json!({"error": e.to_string()})),
        )
            .into_response(),
    }
}

/// POST /internal/shard/rehome — push this node's local rows for the committed
/// `[start, end)` child range to `dest_primary`.
///
/// Used after a Split when the Raft leader is neither the old primary (the only
/// node that can export historical rows) nor the new child primary.
pub async fn handle_shard_rehome(
    State(state): State<Arc<AppState>>,
    Json(req): Json<ShardRehomeRequest>,
) -> impl IntoResponse {
    use crate::application::shard_transfer::{complete_region_transfer, push_region_transfer_data};

    let Some(ctx) = state.shard_routing.as_ref() else {
        return (
            StatusCode::SERVICE_UNAVAILABLE,
            Json(serde_json::json!({"error": "sharding disabled"})),
        )
            .into_response();
    };
    let Some(peer_client) = state.peer_client.as_ref() else {
        return (
            StatusCode::SERVICE_UNAVAILABLE,
            Json(serde_json::json!({"error": "no peer client"})),
        )
            .into_response();
    };
    if req.dest_primary == state.node_id {
        return (
            StatusCode::BAD_REQUEST,
            Json(serde_json::json!({"error": "destination is this node"})),
        )
            .into_response();
    }

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
    // Validate against the committed map: exact child range + epoch. This also
    // pins the real apply-allocated region_id into the transfer payloads.
    let region = map
        .space(&req.db, &req.rp, &req.measurement)
        .and_then(|space| resolve_rehome_region(space, req.region_id, req.start, req.end));
    let Some(region) = region else {
        return (
            StatusCode::NOT_FOUND,
            Json(serde_json::json!({
                "error": format!(
                    "region not found (region_id={}, range=[{}, {}))",
                    req.region_id, req.start, req.end
                )
            })),
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

    let outcome = match if req.stage {
        crate::application::shard_transfer::stage_region_transfer_data(
            peer_client,
            &state.metadata,
            &state.wal,
            Some(&state.query_port),
            state.node_id,
            &ctx_key(&req),
            region,
            req.dest_primary,
            state.max_points_per_request,
        )
        .await
    } else {
        push_region_transfer_data(
            peer_client,
            &state.metadata,
            &state.wal,
            Some(&state.query_port),
            state.node_id,
            &ctx_key(&req),
            region,
            req.dest_primary,
            state.max_points_per_request,
        )
        .await
    } {
        Ok(o) => o,
        Err(e) => {
            counter!("hyperbytedb_shard_transfer_failures_total").increment(1);
            // The serving side used to answer 500 and log nothing, so the only
            // trace of a failed rehome was the caller's bare status code.
            tracing::warn!(
                error = %e,
                region_id = region.region_id,
                dest_primary = req.dest_primary,
                stage = req.stage,
                "shard rehome rejected: transfer failed"
            );
            return (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(serde_json::json!({"error": e.to_string()})),
            )
                .into_response();
        }
    };

    if !req.stage
        && let Err(e) = complete_region_transfer(
            peer_client,
            &state.metadata,
            Some(&state.points_sink),
            state.node_id,
            &ctx_key(&req),
            region,
            req.dest_primary,
            outcome.transfer_id,
            req.drop_source,
        )
        .await
    {
        counter!("hyperbytedb_shard_transfer_failures_total").increment(1);
        tracing::warn!(
            error = %e,
            region_id = region.region_id,
            dest_primary = req.dest_primary,
            "shard rehome rejected: completing the transfer failed"
        );
        return (
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(serde_json::json!({"error": e.to_string()})),
        )
            .into_response();
    }

    counter!("hyperbytedb_shard_rehome_total").increment(1);
    (
        StatusCode::OK,
        Json(serde_json::json!({
            "ok": true,
            "exported": outcome.exported,
            "applied": outcome.applied,
            "bytes": outcome.bytes,
        })),
    )
        .into_response()
}

fn ctx_key(req: &ShardRehomeRequest) -> MeasurementKey {
    MeasurementKey::new(&req.db, &req.rp, &req.measurement)
}

pub async fn handle_shard_metadata(
    State(state): State<Arc<AppState>>,
    Json(req): Json<ShardMetadataRequest>,
) -> impl IntoResponse {
    let Some(ctx) = state.shard_routing.as_ref() else {
        return sharding_disabled();
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
    let region = lookup_region(&map, &req.db, &req.rp, &req.measurement, req.region_id);
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
    let Some(ctx) = state.shard_routing.as_ref() else {
        return sharding_disabled();
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
    let region = lookup_region(&map, &req.db, &req.rp, &req.measurement, req.region_id);
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

    let predicate = req.predicate.as_deref().unwrap_or("");
    if !predicate.is_empty()
        && let Err(e) = state
            .metadata
            .store_tombstone(&req.db, &req.rp, &req.measurement, predicate)
            .await
    {
        return (
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(serde_json::json!({"error": e.to_string()})),
        )
            .into_response();
    }
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

/// Resolve the region a rehome request names.
///
/// Prefers `region_id`. Addressing by exact range alone fails under load: a
/// child that splits again before the request lands no longer has a region
/// spanning that range, so the receiver answered 404 and the movement was
/// abandoned -- 40 of 88 staging failures on a run that split 70 times while
/// writing. `Split` keeps the parent's id on the LEFT child, so the id survives
/// a re-split and names a region carrying whatever range it now spans; the rest
/// belongs to the grandchild's own transfer.
///
/// `region_id == 0` means the caller did not supply one, and the range is used.
fn resolve_rehome_region(
    space: &crate::domain::sharding::types::MeasurementShardSpace,
    region_id: u64,
    start: u64,
    end: u64,
) -> Option<&crate::domain::sharding::types::ShardRegion> {
    if region_id != 0
        && let Some(by_id) = space.regions.iter().find(|r| r.region_id == region_id)
    {
        return Some(by_id);
    }
    space.region_with_range(start, end)
}

#[cfg(test)]
mod rehome_resolution_tests {
    use super::resolve_rehome_region;
    use crate::domain::sharding::types::{
        MeasurementKey, MeasurementShardSpace, ShardEpoch, ShardRegion,
    };

    fn region(id: u64, start: u64, end: u64) -> ShardRegion {
        ShardRegion {
            region_id: id,
            start,
            end,
            epoch: ShardEpoch::default(),
            peers: vec![1, 2],
            primary: 1,
            last_split_at: 0,
            transfer_verified: None,
            transfer_first_seen: None,
            state: Default::default(),
            staged_from: None,
            staging_peers: Vec::new(),
        }
    }

    fn space(regions: Vec<ShardRegion>) -> MeasurementShardSpace {
        MeasurementShardSpace {
            key: MeasurementKey::new("db", "rp", "cpu"),
            regions,
        }
    }

    /// The whole point: after the named region splits again, its range no
    /// longer matches the request, but the id still finds it.
    #[test]
    fn a_region_that_split_again_is_still_found_by_id() {
        let s = space(vec![region(7, 0, 100), region(9, 100, 200)]);

        let got = resolve_rehome_region(&s, 7, 0, 200).expect("id must resolve");
        assert_eq!(got.region_id, 7);
        assert_eq!(
            (got.start, got.end),
            (0, 100),
            "the mover must use the range the region NOW spans"
        );

        assert!(
            resolve_rehome_region(&s, 0, 0, 200).is_none(),
            "range-only addressing is what used to 404 here"
        );
    }

    #[test]
    fn range_is_used_when_no_id_is_supplied() {
        let s = space(vec![region(7, 0, 100), region(9, 100, 200)]);
        let got = resolve_rehome_region(&s, 0, 100, 200).expect("range must resolve");
        assert_eq!(got.region_id, 9);
    }

    /// An id naming a region that is gone falls back to the range rather than
    /// failing outright -- the range may still match after a merge.
    #[test]
    fn an_unknown_id_falls_back_to_the_range() {
        let s = space(vec![region(7, 0, 100)]);
        let got = resolve_rehome_region(&s, 999, 0, 100).expect("fallback must resolve");
        assert_eq!(got.region_id, 7);
    }

    #[test]
    fn neither_id_nor_range_resolves_to_nothing() {
        let s = space(vec![region(7, 0, 100)]);
        assert!(resolve_rehome_region(&s, 999, 500, 600).is_none());
    }
}
