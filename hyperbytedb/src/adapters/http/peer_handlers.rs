use std::sync::Arc;

use axum::Json;
use axum::body::Bytes;
use axum::extract::{Query, State};
use axum::http::StatusCode;
use axum::http::header::CONTENT_TYPE;
use axum::response::IntoResponse;
use metrics::{counter, gauge};

use crate::application::replication_apply::ReplicationApplyError;
use crate::domain::cluster::membership::{ClusterMembership, NodeInfo, NodeState};
use crate::domain::cluster::replication_wire::{
    HTTP_HEADER_DATABASE, HTTP_HEADER_ORIGIN_NODE, HTTP_HEADER_PRECISION, HTTP_HEADER_REGION,
    HTTP_HEADER_RETENTION_POLICY, HTTP_HEADER_SYNC, LINE_PROTOCOL_MEDIA_TYPE_V1,
    REPLICATE_BATCH_MEDIA_TYPE_V1, ReplicateBatchPart, ReplicateBatchRequest,
    ReplicateBatchResponse, ReplicatePartOutcome,
};
use crate::domain::cluster::sync::{
    JoinRequest, JoinResponse, LeaveRequest, LeaveResponse, MetadataEntry, MetadataSnapshot,
    WalSyncEntry, WalSyncRequest, WalSyncResponse,
};
use crate::domain::cluster::types::{MutationReplicateRequest, MutationRequest};
use crate::ports::metadata::MetadataPort;
use crate::ports::sharding::ShardMapPort as _;

use super::router::AppState;

/// Reject spoofed origin node IDs not present in cluster membership.
async fn validate_origin_node_id(state: &AppState, origin_node_id: u64) -> Result<(), StatusCode> {
    if origin_node_id == 0 {
        return Ok(());
    }
    let Some(ref membership) = state.membership else {
        return Err(StatusCode::BAD_REQUEST);
    };
    let m = membership.read().await;
    if m.get_node(origin_node_id).is_some() {
        Ok(())
    } else {
        Err(StatusCode::BAD_REQUEST)
    }
}

/// Receives replicated line-protocol writes from a peer (`POST /internal/replicate`).
pub async fn handle_replicate_write(
    State(state): State<Arc<AppState>>,
    headers: axum::http::HeaderMap,
    body: Bytes,
) -> impl IntoResponse {
    if let Some(ref membership) = state.membership {
        let m = membership.read().await;
        if let Some(node) = m.get_node(state.node_id)
            && (node.state == NodeState::Draining || node.state == NodeState::Leaving)
        {
            return (
                StatusCode::SERVICE_UNAVAILABLE,
                Json(serde_json::json!({"error": "node is draining"})),
            );
        }
    }

    let origin_node_id = headers
        .get(HTTP_HEADER_ORIGIN_NODE)
        .and_then(|v| v.to_str().ok())
        .and_then(|s| s.parse::<u64>().ok())
        .unwrap_or(0);

    if let Err(status) = validate_origin_node_id(&state, origin_node_id).await {
        return (
            status,
            Json(serde_json::json!({"error": "invalid origin node id"})),
        );
    }

    // B.4 — region check, deliberately narrow.
    //
    // Absent header means "no region scoping available": unsharded writes, and
    // replayed hinted-handoff records, whose on-disk format predates the
    // region being carried. Those apply as before. Rejecting them would turn
    // a permanent 4xx into permanent unavailability, because the hint drain
    // re-enqueues on ANY non-success and retries the same head-of-queue record
    // forever, wedging every hint behind it.
    //
    // Epoch is NOT checked here. The sender is authoritative about which
    // region a row belongs to; the receiver's shard map merely lags, and every
    // AddPeer/Split/PromotePeer moves the epoch. Rejecting on skew would drop
    // legitimate writes during exactly the window convergence is busiest —
    // and those writes then burn the retry budget into the hint queue. Only a
    // sender that is not a member of the region at all is refused.
    if let Some(region_id) = headers
        .get(HTTP_HEADER_REGION)
        .and_then(|v| v.to_str().ok())
        .and_then(|s| s.parse::<u64>().ok())
        && let Some(ctx) = state.shard_routing.as_ref()
        && let Ok(map) = ctx.shard_map.snapshot().await
        && let Some(region) = crate::application::shard_routing::region_by_id(&map, region_id)
        && !region.peers.contains(&state.node_id)
    {
        counter!(
            "hyperbytedb_replicate_rejected_total",
            "reason" => "not_a_region_peer"
        )
        .increment(1);
        return (
            StatusCode::CONFLICT,
            Json(serde_json::json!({
                "error": "not a peer of this region",
                "region_id": region_id,
            })),
        );
    }

    let ct = headers
        .get(CONTENT_TYPE)
        .and_then(|v| v.to_str().ok())
        .unwrap_or("")
        .split(';')
        .next()
        .unwrap_or("")
        .trim();

    if ct != LINE_PROTOCOL_MEDIA_TYPE_V1 {
        return (
            StatusCode::UNSUPPORTED_MEDIA_TYPE,
            Json(serde_json::json!({
                "error": format!(
                    "Content-Type must be {}",
                    LINE_PROTOCOL_MEDIA_TYPE_V1
                )
            })),
        );
    }

    let Some(queue) = state.replication_apply.as_ref() else {
        return (
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(serde_json::json!({"error": "replication apply queue not configured"})),
        );
    };

    let database = match headers
        .get(HTTP_HEADER_DATABASE)
        .and_then(|v| v.to_str().ok())
    {
        Some(d) if !d.is_empty() => d.to_string(),
        _ => {
            return (
                StatusCode::BAD_REQUEST,
                Json(serde_json::json!({"error": "missing X-Hyperbytedb-DB"})),
            );
        }
    };
    let retention_policy = match headers
        .get(HTTP_HEADER_RETENTION_POLICY)
        .and_then(|v| v.to_str().ok())
    {
        Some(r) if !r.is_empty() => r.to_string(),
        _ => {
            return (
                StatusCode::BAD_REQUEST,
                Json(serde_json::json!({"error": "missing X-Hyperbytedb-RP"})),
            );
        }
    };
    let precision = headers
        .get(HTTP_HEADER_PRECISION)
        .and_then(|v| v.to_str().ok())
        .filter(|s| !s.is_empty())
        .map(|s| s.to_string());

    let sync_requested = headers
        .get(HTTP_HEADER_SYNC)
        .and_then(|v| v.to_str().ok())
        .map(|s| matches!(s.to_ascii_lowercase().as_str(), "1" | "true" | "yes"))
        .unwrap_or(false);

    let rx = match queue.try_enqueue(database, retention_policy, precision, body, origin_node_id) {
        Ok(rx) => rx,
        Err(ReplicationApplyError::QueueFull) => {
            counter!("hyperbytedb_replication_apply_queue_full_total").increment(1);
            return (
                StatusCode::SERVICE_UNAVAILABLE,
                Json(serde_json::json!({"error": "replication apply queue full"})),
            );
        }
        Err(_) => {
            return (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(serde_json::json!({"error": "replication apply worker unavailable"})),
            );
        }
    };

    if !sync_requested {
        // Fire-and-forget (async coordinator path). Behavior preserved
        // byte-for-byte: ack immediately after enqueue and let the apply
        // workers process in the background. Holding the HTTP connection open
        // while the single-threaded apply worker serializes batches caused
        // 500ms+ latencies and retry cascades under load.
        return (
            StatusCode::OK,
            Json(serde_json::json!({"ok": true, "ack_seq": 0})),
        );
    }

    // Sync coordinator path: await the oneshot for the WAL seq before
    // responding. Caller has guaranteed bounded latency via its own
    // `ack_timeout_ms` — no extra timeout here.
    counter!("hyperbytedb_replication_sync_apply_received_total").increment(1);
    match rx.await {
        Ok(Ok(wal_seq)) => (
            StatusCode::OK,
            Json(serde_json::json!({"ok": true, "ack_seq": wal_seq})),
        ),
        Ok(Err(err)) => {
            counter!("hyperbytedb_replication_sync_apply_errors_total").increment(1);
            (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(serde_json::json!({"error": err})),
            )
        }
        Err(_recv_err) => {
            counter!("hyperbytedb_replication_sync_apply_errors_total").increment(1);
            (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(serde_json::json!({"error": "apply worker dropped before completing"})),
            )
        }
    }
}

/// Apply-queue groups for one [`ReplicateBatchRequest`]: parts that share a
/// database, retention policy, precision and encoding are applied as one job,
/// so a batch of many small region parts costs one WAL append per group
/// rather than one per part.
struct ReplicateApplyGroup {
    database: String,
    retention_policy: String,
    precision: Option<String>,
    parts: Vec<usize>,
    body: Vec<u8>,
}

/// Sort the parts of a replicate batch into apply groups. `is_peer(region)`
/// says whether this node may accept a part for `region`; a part that fails
/// it is answered `NotARegionPeer` and joins no group.
fn group_replicate_parts(
    parts: Vec<ReplicateBatchPart>,
    is_peer: impl Fn(u64) -> bool,
) -> (Vec<ReplicatePartOutcome>, Vec<ReplicateApplyGroup>) {
    let mut outcomes = vec![ReplicatePartOutcome::Accepted; parts.len()];
    let mut groups: Vec<ReplicateApplyGroup> = Vec::new();
    for (i, part) in parts.into_iter().enumerate() {
        if part.database.is_empty() || part.retention_policy.is_empty() {
            outcomes[i] =
                ReplicatePartOutcome::Failed("missing database or retention policy".into());
            continue;
        }
        if let Some(region) = part.region_id
            && !is_peer(region)
        {
            outcomes[i] = ReplicatePartOutcome::NotARegionPeer;
            continue;
        }
        let binary = crate::application::line_protocol::is_binary_points(&part.body);
        let group = groups.iter_mut().find(|g| {
            g.database == part.database
                && g.retention_policy == part.retention_policy
                && g.precision == part.precision
                && crate::application::line_protocol::is_binary_points(&g.body) == binary
        });
        match group {
            Some(g) => {
                // Frames skip whitespace between them, so one separator
                // serves both encodings.
                g.body.push(b'\n');
                g.body.extend_from_slice(&part.body);
                g.parts.push(i);
            }
            None => groups.push(ReplicateApplyGroup {
                database: part.database,
                retention_policy: part.retention_policy,
                precision: part.precision,
                parts: vec![i],
                body: part.body,
            }),
        }
    }
    (outcomes, groups)
}

/// Receives every replication part a peer had queued for this node, in one
/// request. Same contract as [`handle_replicate_write`] per part (async: a
/// part is acknowledged once queued for apply), with one outcome per part.
pub async fn handle_replicate_batch(
    State(state): State<Arc<AppState>>,
    body: Bytes,
) -> impl IntoResponse {
    let req: ReplicateBatchRequest = match bincode::deserialize(&body) {
        Ok(r) => r,
        Err(e) => {
            return (
                StatusCode::BAD_REQUEST,
                Json(serde_json::json!({"error": format!("bad replicate batch: {e}")})),
            )
                .into_response();
        }
    };
    if let Err(status) = validate_origin_node_id(&state, req.origin_node).await {
        return (
            status,
            Json(serde_json::json!({"error": "invalid origin node id"})),
        )
            .into_response();
    }
    let Some(queue) = state.replication_apply.as_ref() else {
        return (
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(serde_json::json!({"error": "replication apply queue not configured"})),
        )
            .into_response();
    };

    // Same narrow region check as the single-part path: refuse only a part
    // for a region this node is known not to hold.
    let map = match state.shard_routing.as_ref() {
        Some(ctx) => ctx.shard_map.snapshot().await.ok(),
        None => None,
    };
    let node_id = state.node_id;
    let (mut outcomes, groups) = group_replicate_parts(req.parts, |region_id| {
        map.as_ref()
            .and_then(|m| crate::application::shard_routing::region_by_id(m, region_id))
            .is_none_or(|r| r.peers.contains(&node_id))
    });
    let refused = outcomes
        .iter()
        .filter(|o| **o == ReplicatePartOutcome::NotARegionPeer)
        .count();
    if refused > 0 {
        counter!(
            "hyperbytedb_replicate_rejected_total",
            "reason" => "not_a_region_peer"
        )
        .increment(refused as u64);
    }

    for g in groups {
        let outcome = match queue.try_enqueue(
            g.database,
            g.retention_policy,
            g.precision,
            Bytes::from(g.body),
            req.origin_node,
        ) {
            Ok(_) => continue,
            Err(ReplicationApplyError::QueueFull) => {
                counter!("hyperbytedb_replication_apply_queue_full_total").increment(1);
                ReplicatePartOutcome::QueueFull
            }
            Err(_) => ReplicatePartOutcome::Failed("replication apply worker unavailable".into()),
        };
        for i in g.parts {
            outcomes[i] = outcome.clone();
        }
    }

    match bincode::serialize(&ReplicateBatchResponse { results: outcomes }) {
        Ok(bytes) => (
            StatusCode::OK,
            [(CONTENT_TYPE, REPLICATE_BATCH_MEDIA_TYPE_V1)],
            bytes,
        )
            .into_response(),
        Err(e) => (
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(serde_json::json!({"error": format!("encode response: {e}")})),
        )
            .into_response(),
    }
}

/// Receives a replicated mutation from a peer node and applies it locally.
pub async fn handle_replicate_mutation(
    State(state): State<Arc<AppState>>,
    Json(req): Json<MutationReplicateRequest>,
) -> impl IntoResponse {
    let sender_seq = req.seq;
    let origin = req.origin_node_id;

    if let Err(status) = validate_origin_node_id(&state, origin).await {
        return (
            status,
            Json(serde_json::json!({"error": "invalid origin node id"})),
        );
    }

    if origin != 0
        && let Some(ref rl) = state.replication_log
    {
        match rl.check_and_record_mutation(origin, sender_seq) {
            Ok(false) => {
                tracing::debug!(
                    origin_node_id = origin,
                    seq = sender_seq,
                    "skipping duplicate mutation"
                );
                return (
                    StatusCode::OK,
                    Json(serde_json::json!({"ok": true, "ack_seq": sender_seq})),
                );
            }
            Err(e) => {
                tracing::error!(error = %e, "failed to record mutation dedup state");
                return (
                    StatusCode::INTERNAL_SERVER_ERROR,
                    Json(serde_json::json!({"error": "internal error recording mutation"})),
                );
            }
            Ok(true) => {}
        }
    }

    let result = apply_mutation(&state, req.mutation).await;

    match result {
        Ok(()) => (
            StatusCode::OK,
            Json(serde_json::json!({"ok": true, "ack_seq": sender_seq})),
        ),
        Err(e) => {
            tracing::error!(error = %e, "replicate-mutation failed");
            (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(serde_json::json!({"error": e.to_string()})),
            )
        }
    }
}

/// Return the current cluster membership.
pub async fn handle_get_membership(State(state): State<Arc<AppState>>) -> impl IntoResponse {
    let membership = match &state.membership {
        Some(m) => m,
        None => {
            return (
                StatusCode::BAD_REQUEST,
                Json(serde_json::json!({"error": "cluster not enabled"})),
            );
        }
    };

    let m = membership.read().await;
    (StatusCode::OK, Json(serde_json::json!(*m)))
}

/// Handle a join request from a new node.
///
/// When Raft is enabled, the node is added via `add_learner` which can only
/// succeed on the Raft leader. If this node is not the leader the request
/// is rejected with 503 so the caller can retry on another peer.
/// Raft's membership change propagates to all nodes automatically via
/// `sync_raft_membership_to_shared`, so we do NOT touch `ClusterMembership`
/// directly when Raft is active.
pub async fn handle_join(
    State(state): State<Arc<AppState>>,
    Json(req): Json<JoinRequest>,
) -> impl IntoResponse {
    let membership = match &state.membership {
        Some(m) => m,
        None => {
            return (
                StatusCode::BAD_REQUEST,
                Json(serde_json::json!({"error": "cluster not enabled"})),
            );
        }
    };

    counter!("hyperbytedb_membership_transitions_total", "transition" => "join").increment(1);

    let assigned_id = {
        let m = membership.read().await;
        req.node_id.unwrap_or_else(|| m.next_node_id())
    };

    if let Some(ref raft) = state.raft {
        // Raft-managed path: propose adding the learner without blocking.
        // blocking=false returns as soon as the membership change is committed
        // (instant with single-node quorum) without waiting for the learner's
        // Raft endpoint to catch up. The learner might not have its HTTP server
        // up yet during startup sync — blocking would deadlock.
        let node = openraft::BasicNode::new(req.addr.clone());
        match raft.add_learner(assigned_id, node, false).await {
            Ok(_) => {
                tracing::info!(
                    assigned_id = assigned_id,
                    addr = %req.addr,
                    "proposed raft learner via join request"
                );
                let m = membership.read().await;
                let resp = JoinResponse {
                    assigned_node_id: assigned_id,
                    membership: m.clone(),
                };
                (StatusCode::OK, Json(serde_json::json!(resp)))
            }
            Err(e) => {
                tracing::debug!(
                    error = %e,
                    "join rejected: not the raft leader or change in progress"
                );
                (
                    StatusCode::SERVICE_UNAVAILABLE,
                    Json(serde_json::json!({"error": "not raft leader, retry on another node"})),
                )
            }
        }
    } else {
        // Non-Raft path: direct local membership update
        let now = chrono::Utc::now().timestamp();
        let mut m = membership.write().await;

        m.add_node(NodeInfo {
            node_id: assigned_id,
            addr: req.addr.clone(),
            state: NodeState::Joining,
            joined_at: now,
            last_heartbeat: now,
            needs_sync: false,
            consecutive_misses: 0,
        });

        gauge!("hyperbytedb_cluster_peers").set(m.active_peers(state.node_id).len() as f64);
        gauge!("hyperbytedb_cluster_nodes_total").set(m.nodes.len() as f64);
        gauge!("hyperbytedb_cluster_membership_version").set(m.version as f64);

        tracing::info!(
            assigned_id = assigned_id,
            addr = %req.addr,
            version = m.version,
            "node joined cluster"
        );

        let resp = JoinResponse {
            assigned_node_id: assigned_id,
            membership: m.clone(),
        };

        (StatusCode::OK, Json(serde_json::json!(resp)))
    }
}

/// Move `node_id` into `Draining` for a self-reported leave, refusing to
/// downgrade a terminal lifecycle state.
///
/// Pulled out of [`handle_leave`] so the guard is directly unit-testable:
/// `/internal/membership/leave` self-calls used to `set_state` unconditionally,
/// which let this call clobber a `Decommissioning` node back into a
/// placement-eligible state. Returns whether the transition happened.
fn apply_self_leave(m: &mut ClusterMembership, node_id: u64) -> bool {
    m.transition(node_id, NodeState::Draining)
}

/// Handle a leave request from a node.
pub async fn handle_leave(
    State(state): State<Arc<AppState>>,
    Json(req): Json<LeaveRequest>,
) -> impl IntoResponse {
    let membership = match &state.membership {
        Some(m) => m,
        None => {
            return (
                StatusCode::BAD_REQUEST,
                Json(serde_json::json!({"error": "cluster not enabled"})),
            );
        }
    };

    counter!("hyperbytedb_membership_transitions_total", "transition" => "leave").increment(1);

    if req.node_id == state.node_id {
        let mut m = membership.write().await;
        if apply_self_leave(&mut m, req.node_id) {
            gauge!("hyperbytedb_cluster_node_state").set(4.0); // Draining
            tracing::info!(node_id = req.node_id, "local node entering drain mode");
        } else {
            tracing::info!(
                node_id = req.node_id,
                current = ?m.get_node(req.node_id).map(|n| n.state),
                "self-leave ignored: node already in a terminal lifecycle state"
            );
        }
    } else {
        let mut m = membership.write().await;
        m.transition(req.node_id, NodeState::Leaving);
        m.remove_node(req.node_id);
        if let Some(ref rl) = state.replication_log {
            let _ = rl.remove_peer(req.node_id);
        }
        gauge!("hyperbytedb_cluster_peers").set(m.active_peers(state.node_id).len() as f64);
        gauge!("hyperbytedb_cluster_nodes_total").set(m.nodes.len() as f64);
        gauge!("hyperbytedb_cluster_membership_version").set(m.version as f64);
        tracing::info!(
            node_id = req.node_id,
            version = m.version,
            "node removed from cluster"
        );
    }

    let resp = LeaveResponse { ok: true };
    (StatusCode::OK, Json(serde_json::json!(resp)))
}

/// Return a sync manifest containing metadata and WAL position.
pub async fn handle_sync_manifest(State(state): State<Arc<AppState>>) -> impl IntoResponse {
    match crate::application::cluster::sync_manifest::build_manifest(
        state.node_id,
        &state.metadata,
        &state.wal,
        state
            .shard_map
            .as_ref()
            .map(|m| m.as_ref() as &dyn crate::ports::sharding::ShardMapPort),
    )
    .await
    {
        Ok(manifest) => (StatusCode::OK, Json(serde_json::json!(manifest))),
        Err(e) => (
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(serde_json::json!({"error": e.to_string()})),
        ),
    }
}

/// Stream the full metadata snapshot as JSON.
pub async fn handle_sync_metadata(State(state): State<Arc<AppState>>) -> impl IntoResponse {
    match build_metadata_snapshot(&state.metadata).await {
        Ok(snapshot) => (StatusCode::OK, Json(serde_json::json!(snapshot))),
        Err(e) => (
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(serde_json::json!({"error": e.to_string()})),
        ),
    }
}

/// Stream WAL entries from a given sequence.
pub async fn handle_sync_wal(
    State(state): State<Arc<AppState>>,
    Query(params): Query<WalSyncRequest>,
) -> impl IntoResponse {
    let max = if params.max_entries == 0 {
        5000
    } else {
        params.max_entries
    };

    match state.wal.read_range(params.from_seq, max).await {
        Ok(entries) => {
            let last_seq = entries
                .iter()
                .map(|(s, _)| *s)
                .max()
                .unwrap_or(params.from_seq);
            let sync_entries: Vec<WalSyncEntry> = entries
                .into_iter()
                .map(|(seq, entry)| WalSyncEntry {
                    seq,
                    database: entry.database,
                    retention_policy: entry.retention_policy,
                    points: entry.points,
                    origin_node_id: entry.origin_node_id,
                })
                .collect();

            let resp = WalSyncResponse {
                entries: sync_entries,
                last_seq,
            };
            (StatusCode::OK, Json(serde_json::json!(resp)))
        }
        Err(e) => (
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(serde_json::json!({"error": e.to_string()})),
        ),
    }
}

/// Execute the full drain procedure (flush WAL, wait for acks, verify, notify leave).
pub async fn handle_drain(State(state): State<Arc<AppState>>) -> impl IntoResponse {
    let drain_service = match &state.drain_service {
        Some(ds) => ds.clone(),
        None => {
            return (
                StatusCode::BAD_REQUEST,
                Json(serde_json::json!({"error": "drain service not available"})),
            );
        }
    };

    tokio::spawn(async move {
        if let Err(e) = drain_service.drain().await {
            tracing::error!(error = %e, "drain procedure failed");
        }
    });

    (
        StatusCode::ACCEPTED,
        Json(serde_json::json!({"ok": true, "message": "drain initiated"})),
    )
}

/// Execute the full decommission procedure (mark decommissioning, hand off
/// primaries, flush, wait for acks). Region evacuation and the eventual
/// `Leaving` transition are the shard scheduler's job, not this request's.
pub async fn handle_decommission(State(state): State<Arc<AppState>>) -> impl IntoResponse {
    let drain_service = match &state.drain_service {
        Some(ds) => ds.clone(),
        None => {
            return (
                StatusCode::BAD_REQUEST,
                Json(serde_json::json!({"error": "drain service not available"})),
            );
        }
    };

    tokio::spawn(async move {
        if let Err(e) = drain_service.decommission().await {
            tracing::error!(error = %e, "decommission procedure failed");
        }
    });

    (
        StatusCode::ACCEPTED,
        Json(serde_json::json!({"ok": true, "message": "decommission initiated"})),
    )
}

async fn apply_mutation(
    state: &Arc<crate::adapters::http::router::AppState>,
    req: MutationRequest,
) -> Result<(), crate::error::HyperbytedbError> {
    crate::application::schema_mutation_apply::apply_schema_mutation(
        crate::application::schema_mutation_apply::SchemaMutationDeps {
            metadata: &state.metadata,
            mv_service: Some(state.mv_service.as_ref()),
            points_sink: Some(&state.points_sink),
            wal: Some(&state.wal),
            shard_routing: state.shard_routing.as_ref(),
            runtime_settings: Some(&state.runtime_settings),
        },
        req,
    )
    .await
}

async fn build_metadata_snapshot(
    metadata: &Arc<dyn MetadataPort>,
) -> Result<MetadataSnapshot, crate::error::HyperbytedbError> {
    let mut entries = Vec::new();

    let databases = metadata.list_databases().await?;
    for db in &databases {
        entries.push(MetadataEntry {
            key: format!("db:{}", db.name),
            value: serde_json::to_vec(db).map_err(|e| {
                crate::error::HyperbytedbError::Metadata(crate::error::ChainedError::from_error(e))
            })?,
        });

        let rps = metadata.list_retention_policies(&db.name).await?;
        for rp in &rps {
            let measurements = metadata
                .list_measurements_for_rp(&db.name, &rp.name)
                .await?;
            for meas in &measurements {
                if let Some(meta) = metadata.get_measurement(&db.name, &rp.name, meas).await? {
                    entries.push(MetadataEntry {
                        key: format!("meas:{}:{}:{}", db.name, rp.name, meas),
                        value: serde_json::to_vec(&meta).map_err(|e| {
                            crate::error::HyperbytedbError::Metadata(
                                crate::error::ChainedError::from_error(e),
                            )
                        })?,
                    });
                }

                let tombstones = metadata.list_tombstones(&db.name, &rp.name, meas).await?;
                for (id, predicate) in tombstones {
                    entries.push(MetadataEntry {
                        key: format!("tombstone:{}:{}:{}:{}", db.name, rp.name, meas, id),
                        value: predicate.into_bytes(),
                    });
                }
            }
        }

        let cqs = metadata.list_continuous_queries(&db.name).await?;
        for cq in cqs {
            entries.push(MetadataEntry {
                key: format!("cq:{}:{}", db.name, cq.name),
                value: serde_json::to_vec(&cq).map_err(|e| {
                    crate::error::HyperbytedbError::Metadata(
                        crate::error::ChainedError::from_error(e),
                    )
                })?,
            });
        }

        let mvs = metadata.list_materialized_views(&db.name).await?;
        for mv in mvs {
            entries.push(MetadataEntry {
                key: format!("mv:{}:{}", db.name, mv.name),
                value: serde_json::to_vec(&mv).map_err(|e| {
                    crate::error::HyperbytedbError::Metadata(
                        crate::error::ChainedError::from_error(e),
                    )
                })?,
            });
        }
    }

    let users = metadata.list_users().await?;
    for user in users {
        if let Some(u) = metadata.get_user(&user).await? {
            entries.push(MetadataEntry {
                key: format!("user:{}", user),
                value: serde_json::to_vec(&u).map_err(|e| {
                    crate::error::HyperbytedbError::Metadata(
                        crate::error::ChainedError::from_error(e),
                    )
                })?,
            });
        }
    }

    for name in crate::application::runtime_settings::SETTINGS
        .iter()
        .map(|d| d.name)
    {
        if let Some(value) = metadata.get_setting(name).await? {
            entries.push(MetadataEntry {
                key: format!("setting:{name}"),
                value: serde_json::to_vec(&value).map_err(|e| {
                    crate::error::HyperbytedbError::Metadata(
                        crate::error::ChainedError::from_error(e),
                    )
                })?,
            });
        }
    }

    Ok(MetadataSnapshot { entries })
}

/// Triggers a background re-sync on this node. Idempotent: returns 200 if
/// a sync is already running, 202 if a new sync was started.
pub async fn handle_sync_trigger(State(state): State<Arc<AppState>>) -> impl IntoResponse {
    use std::sync::atomic::{AtomicBool, Ordering};

    static SYNC_IN_PROGRESS: AtomicBool = AtomicBool::new(false);

    if SYNC_IN_PROGRESS
        .compare_exchange(false, true, Ordering::SeqCst, Ordering::SeqCst)
        .is_err()
    {
        return (
            StatusCode::OK,
            Json(serde_json::json!({
                "status": "already_syncing",
                "message": "A sync is already in progress"
            })),
        );
    }

    let membership = match &state.membership {
        Some(m) => m.clone(),
        None => {
            SYNC_IN_PROGRESS.store(false, Ordering::SeqCst);
            return (
                StatusCode::BAD_REQUEST,
                Json(serde_json::json!({
                    "status": "error",
                    "message": "Clustering is not enabled"
                })),
            );
        }
    };

    let node_id = state.node_id;

    let metadata = state.metadata.clone();
    let wal = state.wal.clone();
    let points_sink = state.points_sink.clone();
    let mv_service = state.mv_service.clone();
    let max_points_per_request = state.max_points_per_request;
    let shard_map = state.shard_map.clone();
    let sharded = state.shard_routing.is_some();
    let cluster_auth = state.cluster_auth.clone();
    let runtime_settings = state.runtime_settings.clone();

    let membership_clone = membership.clone();

    let fallback_peers: Vec<String> = {
        let m = membership.read().await;
        m.all_peers(node_id)
            .iter()
            .map(|p| p.addr.clone())
            .collect()
    };

    tokio::spawn(async move {
        let mut sync_client = crate::adapters::cluster::sync_client::SyncClient::with_points_sink(
            node_id,
            {
                let m = membership_clone.read().await;
                m.get_node(node_id)
                    .map(|n| n.addr.clone())
                    .unwrap_or_default()
            },
            membership_clone.clone(),
            metadata.clone(),
            wal.clone(),
            Some(points_sink),
            max_points_per_request,
            fallback_peers,
            &cluster_auth,
        );
        sync_client = sync_client.with_runtime_settings(runtime_settings);
        // Only a sharded node syncs a shard map. `state.shard_map` is a
        // `DisabledShardMap` otherwise, never `None`: attaching it made a
        // replicated node take the sharded reconnect path, which skips WAL
        // catch-up and fetches a shard map its peers answer with 404. The sync
        // then always failed, `needs_sync` stayed set, peers never readmitted
        // the node, and writes stopped reaching it.
        if sharded && let Some(ref map) = shard_map {
            let sm: Arc<dyn crate::ports::sharding::ShardMapPort> = map.clone();
            sync_client = sync_client.with_shard_map(sm);
        }

        let has_data = metadata
            .list_databases()
            .await
            .map(|dbs| !dbs.is_empty())
            .unwrap_or(false);
        let wal_seq = wal.last_sequence().await.unwrap_or(0);

        // A node that already synced (Active, nothing flagged) holds no data
        // only because the cluster has none yet. Re-joining it flipped it to
        // Syncing, so the first writes after `/health` turned 200 got 503s;
        // a reconnect stays Active for a small gap. A node the operator
        // brought up knowing no peers starts flagged `needs_sync` and still
        // joins.
        let synced_before = {
            let m = membership_clone.read().await;
            m.get_node(node_id)
                .is_some_and(|n| n.state == NodeState::Active && !n.needs_sync)
        };
        let result = if !has_data && wal_seq == 0 && !synced_before {
            tracing::info!("sync trigger: new node, running join_and_sync");
            {
                let mut m = membership_clone.write().await;
                m.begin_sync_to_syncing(node_id);
            }
            gauge!("hyperbytedb_cluster_node_state").set(3.0);
            sync_client.join_and_sync().await
        } else {
            tracing::info!("sync trigger: existing node, running reconnect_sync");
            sync_client.reconnect_sync().await
        };

        let sync_failed = result.is_err();
        match result {
            Ok(_) => {
                if let Err(e) = mv_service.reconcile_all().await {
                    tracing::warn!(
                        error = %e,
                        "sync trigger: materialized view reconcile failed"
                    );
                }
                tracing::info!("sync trigger: completed successfully");
            }
            Err(ref e) => tracing::error!(error = %e, "sync trigger: sync failed"),
        }

        {
            let mut m = membership_clone.write().await;
            // Readiness stays Active either way (a stuck Syncing would block
            // the leader from reaching us), but a failed sync must keep the
            // `needs_sync` flag set: peers parse it to avoid promoting this
            // node as caught-up, and the leader keeps triggering re-syncs.
            // Never clobber Decommissioning/Leaving — that would undo scale-down.
            m.complete_sync_to_active(node_id);
            m.set_needs_sync(node_id, sync_failed);
        }
        gauge!("hyperbytedb_cluster_node_state").set(1.0);
        SYNC_IN_PROGRESS.store(false, Ordering::SeqCst);
    });

    (
        StatusCode::ACCEPTED,
        Json(serde_json::json!({
            "status": "sync_started",
            "message": "Background sync has been triggered"
        })),
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    fn make_node(id: u64, state: NodeState) -> NodeInfo {
        NodeInfo {
            node_id: id,
            addr: format!("127.0.0.1:{}", 8080 + id),
            state,
            joined_at: 0,
            last_heartbeat: 0,
            needs_sync: false,
            consecutive_misses: 0,
        }
    }

    #[test]
    fn self_leave_transitions_active_to_draining() {
        let mut m = ClusterMembership::new();
        m.add_node(make_node(1, NodeState::Active));

        assert!(apply_self_leave(&mut m, 1));
        assert_eq!(m.get_node(1).map(|n| n.state), Some(NodeState::Draining));
    }

    #[test]
    fn self_leave_does_not_downgrade_a_decommissioning_node() {
        // /internal/membership/leave{node_id: self} used to `set_state`
        // unconditionally, so this self-call could clobber Decommissioning
        // back into Draining -- re-admitting the node as a placement
        // candidate mid-evacuation.
        let mut m = ClusterMembership::new();
        m.add_node(make_node(1, NodeState::Decommissioning));

        assert!(!apply_self_leave(&mut m, 1));
        assert_eq!(
            m.get_node(1).map(|n| n.state),
            Some(NodeState::Decommissioning)
        );
    }

    #[test]
    fn self_leave_does_not_downgrade_a_leaving_node() {
        let mut m = ClusterMembership::new();
        m.add_node(make_node(1, NodeState::Leaving));

        assert!(!apply_self_leave(&mut m, 1));
        assert_eq!(m.get_node(1).map(|n| n.state), Some(NodeState::Leaving));
    }

    #[test]
    fn self_leave_on_unknown_node_is_false() {
        let mut m = ClusterMembership::new();
        assert!(!apply_self_leave(&mut m, 99));
    }
}

#[cfg(test)]
mod replicate_batch_tests {
    use super::*;

    fn part(db: &str, region: Option<u64>, body: &[u8]) -> ReplicateBatchPart {
        ReplicateBatchPart {
            database: db.into(),
            retention_policy: "autogen".into(),
            precision: None,
            region_id: region,
            body: body.to_vec(),
        }
    }

    #[test]
    fn parts_are_grouped_per_database_and_encoding_and_foreign_regions_refused() {
        let binary = crate::application::line_protocol::encode_points_binary(&[]).unwrap();
        let parts = vec![
            part("a", Some(1), b"cpu v=1"),
            part("a", Some(7), b"cpu v=2"),
            part("b", Some(2), b"cpu v=3"),
            part("a", Some(3), b"cpu v=4"),
            part("a", Some(4), &binary),
            part("a", None, b"cpu v=5"),
            part("", Some(5), b"cpu v=6"),
        ];
        let (outcomes, groups) = group_replicate_parts(parts, |region| region != 7);
        assert_eq!(outcomes[1], ReplicatePartOutcome::NotARegionPeer);
        assert!(matches!(outcomes[6], ReplicatePartOutcome::Failed(_)));
        let shape: Vec<(&str, Vec<usize>)> = groups
            .iter()
            .map(|g| (g.database.as_str(), g.parts.clone()))
            .collect();
        assert_eq!(
            shape,
            vec![("a", vec![0, 3, 5]), ("b", vec![2]), ("a", vec![4])]
        );
        assert_eq!(groups[0].body, b"cpu v=1\ncpu v=4\ncpu v=5".to_vec());
    }
}
