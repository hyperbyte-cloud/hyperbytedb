use std::sync::Arc;

use axum::Json;
use axum::body::Bytes;
use axum::extract::{Query, State};
use axum::http::StatusCode;
use axum::http::header::CONTENT_TYPE;
use axum::response::IntoResponse;
use metrics::{counter, gauge};

use crate::application::replication_apply::ReplicationApplyError;
use crate::domain::cluster::membership::{NodeInfo, NodeState};
use crate::domain::cluster::replication_wire::{
    HTTP_HEADER_DATABASE, HTTP_HEADER_ORIGIN_NODE, HTTP_HEADER_PRECISION,
    HTTP_HEADER_RETENTION_POLICY, HTTP_HEADER_SYNC, LINE_PROTOCOL_MEDIA_TYPE_V1,
};
use crate::domain::cluster::sync::{
    JoinRequest, JoinResponse, LeaveRequest, LeaveResponse, MetadataEntry, MetadataSnapshot,
    WalSyncEntry, WalSyncRequest, WalSyncResponse,
};
use crate::domain::cluster::types::{MutationReplicateRequest, MutationRequest};
use crate::ports::metadata::MetadataPort;

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
        && !rl.check_and_record_mutation(origin, sender_seq)
    {
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

    let result = apply_mutation(
        &state.metadata,
        Some(state.mv_service.as_ref()),
        req.mutation,
    )
    .await;

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
        m.set_state(req.node_id, NodeState::Draining);
        gauge!("hyperbytedb_cluster_node_state").set(4.0); // Draining
        tracing::info!(node_id = req.node_id, "local node entering drain mode");
    } else {
        let mut m = membership.write().await;
        m.set_state(req.node_id, NodeState::Leaving);
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

async fn apply_mutation(
    metadata: &Arc<dyn MetadataPort>,
    mv_service: Option<&crate::application::materialized_view_service::MaterializedViewService>,
    req: MutationRequest,
) -> Result<(), crate::error::HyperbytedbError> {
    crate::application::schema_mutation_apply::apply_schema_mutation(metadata, mv_service, req)
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
            value: serde_json::to_vec(db)
                .map_err(|e| crate::error::HyperbytedbError::Metadata(e.to_string()))?,
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
                        value: serde_json::to_vec(&meta)
                            .map_err(|e| crate::error::HyperbytedbError::Metadata(e.to_string()))?,
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
                value: serde_json::to_vec(&cq)
                    .map_err(|e| crate::error::HyperbytedbError::Metadata(e.to_string()))?,
            });
        }

        let mvs = metadata.list_materialized_views(&db.name).await?;
        for mv in mvs {
            entries.push(MetadataEntry {
                key: format!("mv:{}:{}", db.name, mv.name),
                value: serde_json::to_vec(&mv)
                    .map_err(|e| crate::error::HyperbytedbError::Metadata(e.to_string()))?,
            });
        }
    }

    let users = metadata.list_users().await?;
    for user in users {
        if let Some(u) = metadata.get_user(&user).await? {
            entries.push(MetadataEntry {
                key: format!("user:{}", user),
                value: serde_json::to_vec(&u)
                    .map_err(|e| crate::error::HyperbytedbError::Metadata(e.to_string()))?,
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

    let membership_clone = membership.clone();

    let fallback_peers: Vec<String> = {
        let m = membership.read().await;
        m.all_peers(node_id)
            .iter()
            .map(|p| p.addr.clone())
            .collect()
    };

    tokio::spawn(async move {
        let sync_client = crate::adapters::cluster::sync_client::SyncClient::with_points_sink(
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
        );

        let has_data = metadata
            .list_databases()
            .await
            .map(|dbs| !dbs.is_empty())
            .unwrap_or(false);
        let wal_seq = wal.last_sequence().await.unwrap_or(0);

        let result = if !has_data && wal_seq == 0 {
            tracing::info!("sync trigger: new node, running join_and_sync");
            {
                let mut m = membership_clone.write().await;
                m.set_state(node_id, NodeState::Syncing);
            }
            gauge!("hyperbytedb_cluster_node_state").set(3.0);
            sync_client.join_and_sync().await
        } else {
            tracing::info!("sync trigger: existing node, running reconnect_sync");
            sync_client.reconnect_sync().await
        };

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
            Err(e) => tracing::error!(error = %e, "sync trigger: sync failed"),
        }

        {
            let mut m = membership_clone.write().await;
            m.set_state(node_id, NodeState::Active);
            m.set_needs_sync(node_id, false);
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
