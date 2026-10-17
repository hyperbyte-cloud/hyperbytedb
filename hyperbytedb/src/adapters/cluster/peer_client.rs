use bytes::Bytes;
use metrics::{counter, histogram};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;
use tokio::sync::{Semaphore, oneshot};

use crate::adapters::cluster::cluster_auth::ClusterAuth;
use crate::adapters::cluster::hinted_handoff::HintedHandoff;
use crate::adapters::cluster::replication_log::ReplicationLog;
use crate::domain::cluster::membership::{ClusterMembership, SharedMembership};
use crate::domain::cluster::replication_wire::{
    HTTP_HEADER_DATABASE, HTTP_HEADER_ORIGIN_NODE, HTTP_HEADER_PRECISION, HTTP_HEADER_REGION,
    HTTP_HEADER_REPLICATED, HTTP_HEADER_RETENTION_POLICY, HTTP_HEADER_SYNC,
    LINE_PROTOCOL_MEDIA_TYPE_V1, REPLICATE_BATCH_MEDIA_TYPE_V1, ReplicateBatchPart,
    ReplicateBatchResponse, ReplicatePartOutcome, ReplicationHintPayload,
};
use crate::domain::cluster::types::{
    MutationReplicateRequest, MutationReplicateResponse, MutationRequest,
};
use crate::error::HyperbytedbError;
use crate::ports::peer_http::PeerHttp;
use crate::ports::replication::{OutboundReplicationBatch, ReplicationPort};

pub struct PeerClient {
    membership: SharedMembership,
    replication_log: Arc<ReplicationLog>,
    hinted_handoff: Option<Arc<HintedHandoff>>,
    /// One client per HTTP/2 connection to keep to each peer: each has its
    /// own pool, so each holds its own connection.
    clients: Vec<reqwest::Client>,
    /// What every client sends to peers; kept to build more of them.
    cluster_auth: ClusterAuth,
    next_client: std::sync::atomic::AtomicUsize,
    node_id: u64,
    node_addr: String,
    max_retries: u32,
    outbound_tx: tokio::sync::mpsc::Sender<OutboundReplicationBatch>,
    outbound_rx: Mutex<Option<tokio::sync::mpsc::Receiver<OutboundReplicationBatch>>>,
    outbound_started: AtomicBool,
    batch_semaphore: Arc<Semaphore>,
    max_coalesce_body_bytes: usize,
}

/// The HTTP client for peer traffic.
///
/// connect_timeout bounds the worst-case tail for dead/unreachable peers
/// (black-holed SYNs would otherwise hang until the full per-request
/// timeout); keepalive keeps long-lived pool sockets from silently expiring
/// behind NAT/firewalls. Operation timeouts remain the caller's
/// responsibility via per-request `.timeout(...)`. `builder.build()` only
/// fails on broken TLS init, and `Client::new()` would also be unusable then.
fn peer_http_client(auth: &ClusterAuth) -> reqwest::Client {
    auth.build(
        auth.client_builder()
            .timeout(Duration::from_secs(10))
            .connect_timeout(Duration::from_secs(3))
            .tcp_keepalive(Duration::from_secs(60))
            .dns_resolver(crate::adapters::cluster::dns_cache::CachingResolver::shared())
            .http2_prior_knowledge()
            .http2_adaptive_window(true)
            .http2_keep_alive_interval(Duration::from_secs(30))
            .http2_keep_alive_while_idle(true),
    )
}

impl PeerClient {
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        node_id: u64,
        node_addr: String,
        membership: SharedMembership,
        replication_log: Arc<ReplicationLog>,
        max_retries: u32,
        outbound_queue_depth: usize,
        max_inflight_batches: usize,
        max_coalesce_body_bytes: usize,
        cluster_auth: ClusterAuth,
    ) -> Self {
        let depth = outbound_queue_depth.max(1);
        let client = peer_http_client(&cluster_auth);
        let (outbound_tx, outbound_rx) =
            tokio::sync::mpsc::channel::<OutboundReplicationBatch>(depth);
        let inflight = max_inflight_batches.max(1);

        Self {
            membership,
            replication_log,
            hinted_handoff: None,
            clients: vec![client],
            cluster_auth,
            next_client: std::sync::atomic::AtomicUsize::new(0),
            node_id,
            node_addr,
            max_retries: if max_retries == 0 { 5 } else { max_retries },
            outbound_tx,
            outbound_rx: Mutex::new(Some(outbound_rx)),
            outbound_started: AtomicBool::new(false),
            batch_semaphore: Arc::new(Semaphore::new(inflight)),
            max_coalesce_body_bytes: max_coalesce_body_bytes.max(1024),
        }
    }

    pub fn with_hinted_handoff(mut self, hh: Arc<HintedHandoff>) -> Self {
        self.hinted_handoff = Some(hh);
        self
    }

    pub fn membership(&self) -> &SharedMembership {
        &self.membership
    }

    /// The cluster credential this client sends, for building other clients
    /// to peers.
    pub fn cluster_auth(&self) -> &ClusterAuth {
        &self.cluster_auth
    }

    /// Keep `connections` HTTP/2 connections to each peer; requests take
    /// them in turn. One connection carried a peer's whole load on one TCP
    /// stream; TiKV keeps 4 per store. See `cluster.internal_http2_connections`.
    #[must_use]
    pub fn with_http2_connections(mut self, connections: usize) -> Self {
        self.clients = (0..connections.max(1))
            .map(|_| peer_http_client(&self.cluster_auth))
            .collect();
        self
    }

    /// A peer's committed `map_version`, read from `/internal/shard/map`.
    pub async fn fetch_map_version(&self, peer_addr: &str) -> Result<u64, HyperbytedbError> {
        let map =
            super::sync_client::fetch_shard_map(self.http_client().clone(), peer_addr).await?;
        Ok(map.map_version)
    }

    /// The client for the next peer request (round-robin over connections).
    pub fn http_client(&self) -> &reqwest::Client {
        let i = self
            .next_client
            .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        &self.clients[i % self.clients.len()]
    }

    async fn resolve_replication_peers_async(
        &self,
        target_node_ids: Option<&[u64]>,
    ) -> Vec<(u64, String)> {
        let m = self.membership.read().await;
        resolve_replication_peers(&m, self.node_id, target_node_ids)
    }

    pub fn start_outbound_processor(self: &Arc<Self>) {
        if self
            .outbound_started
            .compare_exchange(false, true, Ordering::SeqCst, Ordering::SeqCst)
            .is_err()
        {
            return;
        }
        let rx = self
            .outbound_rx
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .take();
        let Some(rx) = rx else {
            return;
        };
        let this = Arc::clone(self);
        tokio::spawn(async move {
            this.outbound_loop(rx).await;
        });
    }

    async fn outbound_loop(
        self: Arc<Self>,
        mut rx: tokio::sync::mpsc::Receiver<OutboundReplicationBatch>,
    ) {
        while let Some(first) = rx.recv().await {
            // Take everything already queued, up to one request's worth.
            let mut bytes = first.body.len();
            let mut jobs = vec![first];
            while bytes < self.max_coalesce_body_bytes && jobs.len() < MAX_PARTS_PER_ROUND {
                match rx.try_recv() {
                    Ok(next) => {
                        bytes = bytes.saturating_add(next.body.len());
                        jobs.push(next);
                    }
                    Err(_) => break,
                }
            }
            let jobs = coalesce_jobs(jobs, self.max_coalesce_body_bytes);

            let permit = match self.batch_semaphore.clone().acquire_owned().await {
                Ok(p) => p,
                Err(_) => return,
            };
            let this = Arc::clone(&self);
            tokio::spawn(async move {
                this.replicate_round(jobs).await;
                drop(permit);
            });
        }
    }

    /// Send one round of queued parts: every part bound for a peer goes to it
    /// in a single `/internal/replicate-batch` request.
    async fn replicate_round(&self, jobs: Vec<OutboundReplicationBatch>) {
        let start = std::time::Instant::now();
        let mut per_peer: Vec<(u64, String, Vec<ReplicateBatchPart>)> = Vec::new();
        {
            let m = self.membership.read().await;
            for job in &jobs {
                for (peer_id, addr) in
                    resolve_replication_peers(&m, self.node_id, job.target_node_ids.as_deref())
                {
                    let part = ReplicateBatchPart {
                        database: job.database.clone(),
                        retention_policy: job.retention_policy.clone(),
                        precision: job.precision.clone(),
                        region_id: job.region_id,
                        body: job.body.clone(),
                    };
                    match per_peer.iter_mut().find(|(id, ..)| *id == peer_id) {
                        Some((.., parts)) => parts.push(part),
                        None => per_peer.push((peer_id, addr, vec![part])),
                    }
                }
            }
        }
        futures::future::join_all(
            per_peer
                .into_iter()
                .map(|(peer_id, addr, parts)| self.send_parts_to_peer(peer_id, addr, parts)),
        )
        .await;
        histogram!("hyperbytedb_replication_duration_seconds")
            .record(start.elapsed().as_secs_f64());
    }

    /// Deliver `parts` to one peer, retrying with backoff the parts it has not
    /// accepted. Parts still refused after the last attempt go to hinted
    /// handoff one by one, as a single-part send would.
    async fn send_parts_to_peer(
        &self,
        peer_id: u64,
        addr: String,
        mut parts: Vec<ReplicateBatchPart>,
    ) {
        let url = format!("http://{addr}/internal/replicate-batch");
        let mut attempts = 0u32;
        let mut delay = Duration::from_secs(1);
        loop {
            attempts += 1;
            // Wire-identical to `ReplicateBatchRequest` (bincode writes a
            // struct as its fields in order) without cloning every body.
            let body = match bincode::serialize(&(self.node_id, &parts)) {
                Ok(b) => b,
                Err(e) => {
                    tracing::error!(peer_id, error = %e, "encode replicate batch");
                    break;
                }
            };
            histogram!("hyperbytedb_replication_batch_bytes").record(body.len() as f64);
            histogram!("hyperbytedb_replication_batch_parts").record(parts.len() as f64);
            let http_start = std::time::Instant::now();
            let result = self
                .http_client()
                .post(&url)
                .header(reqwest::header::CONTENT_TYPE, REPLICATE_BATCH_MEDIA_TYPE_V1)
                .header(HTTP_HEADER_REPLICATED, "true")
                .header(HTTP_HEADER_ORIGIN_NODE, self.node_id.to_string())
                .body(body)
                .send()
                .await;
            histogram!("hyperbytedb_replication_http_seconds", "peer" => url.clone())
                .record(http_start.elapsed().as_secs_f64());

            match result {
                Ok(resp)
                    if resp.status() == reqwest::StatusCode::NOT_FOUND
                        || resp.status() == reqwest::StatusCode::METHOD_NOT_ALLOWED =>
                {
                    // A peer from before this endpoint: one request per part.
                    self.send_parts_one_by_one(peer_id, addr, parts).await;
                    return;
                }
                Ok(resp) if resp.status().is_success() => {
                    let decoded = resp
                        .bytes()
                        .await
                        .ok()
                        .and_then(|b| bincode::deserialize::<ReplicateBatchResponse>(&b).ok());
                    match decoded {
                        Some(r) if r.results.len() == parts.len() => {
                            counter!("hyperbytedb_replication_writes_total", "peer" => url.clone())
                                .increment(1);
                            parts = parts
                                .into_iter()
                                .zip(r.results)
                                .filter(|(_, outcome)| *outcome != ReplicatePartOutcome::Accepted)
                                .map(|(part, _)| part)
                                .collect();
                            if parts.is_empty() {
                                return;
                            }
                            counter!("hyperbytedb_replication_parts_refused_total", "peer" => url.clone())
                                .increment(parts.len() as u64);
                        }
                        _ => {
                            tracing::warn!(peer = %url, attempt = attempts, "bad replicate batch response");
                            counter!("hyperbytedb_replication_errors_total", "peer" => url.clone())
                                .increment(1);
                        }
                    }
                }
                Ok(resp) => {
                    tracing::warn!(
                        peer = %url,
                        status = %resp.status(),
                        attempt = attempts,
                        "peer rejected replicate batch"
                    );
                    counter!("hyperbytedb_replication_errors_total", "peer" => url.clone())
                        .increment(1);
                }
                Err(e) => {
                    tracing::warn!(
                        peer = %url,
                        error = %e,
                        attempt = attempts,
                        "failed to send replicate batch"
                    );
                    counter!("hyperbytedb_replication_errors_total", "peer" => url.clone())
                        .increment(1);
                }
            }
            if attempts >= self.max_retries {
                break;
            }
            tokio::time::sleep(delay).await;
            delay = (delay * 2).min(Duration::from_secs(30));
        }

        let Some(hh) = self.hinted_handoff.as_ref() else {
            tracing::error!(
                peer = %url,
                parts = parts.len(),
                "giving up replication after {} attempts (no hinted handoff)",
                self.max_retries
            );
            return;
        };
        for part in parts {
            let hint = ReplicationHintPayload {
                database: part.database,
                retention_policy: part.retention_policy,
                precision: part.precision,
                line_body: part.body,
            };
            if let Err(e) = hh.enqueue_hint(peer_id, &hint) {
                tracing::error!(peer_id, error = %e, "failed to enqueue hinted handoff");
            }
        }
    }

    /// The single-part `/internal/replicate` path, for a peer that predates
    /// `/internal/replicate-batch`.
    async fn send_parts_one_by_one(
        &self,
        peer_id: u64,
        addr: String,
        parts: Vec<ReplicateBatchPart>,
    ) {
        let sends: Vec<_> = parts
            .into_iter()
            .map(|part| {
                let hint = ReplicationHintPayload {
                    database: part.database.clone(),
                    retention_policy: part.retention_policy.clone(),
                    precision: part.precision.clone(),
                    line_body: part.body.clone(),
                };
                let ctx = self.peer_replicate_ctx(
                    peer_id,
                    addr.clone(),
                    Bytes::from(part.body),
                    part.database,
                    part.retention_policy,
                    part.precision,
                    0,
                    hint,
                    /* sync = */ false,
                    part.region_id,
                );
                async move {
                    let _ = ctx.run().await;
                }
            })
            .collect();
        futures::future::join_all(sends).await;
    }

    pub fn node_addr(&self) -> &str {
        &self.node_addr
    }

    pub fn node_id(&self) -> u64 {
        self.node_id
    }

    pub fn replication_log(&self) -> &Arc<ReplicationLog> {
        &self.replication_log
    }

    /// Fan out a line-protocol batch to all active peers (bounded queue + coalescing worker).
    /// Non-blocking: if the outbound queue is full, batches are enqueued to hinted-handoff
    /// for each replication peer instead of being dropped.
    pub fn replicate_write(
        self: &Arc<Self>,
        batch: OutboundReplicationBatch,
    ) -> Result<(), HyperbytedbError> {
        self.start_outbound_processor();
        match self.outbound_tx.try_send(batch) {
            Ok(()) => Ok(()),
            Err(tokio::sync::mpsc::error::TrySendError::Full(batch)) => {
                counter!("hyperbytedb_replication_queue_full_total").increment(1);
                if self.hinted_handoff.is_none() {
                    return Err(HyperbytedbError::Internal(
                        "replication outbound queue full and hinted handoff is not configured"
                            .into(),
                    ));
                }
                let this = Arc::clone(self);
                tokio::spawn(async move {
                    if let Err(e) = this.enqueue_batch_to_hinted_handoff(batch).await {
                        tracing::error!(
                            error = %e,
                            "replication outbound queue full and hinted handoff enqueue failed"
                        );
                        counter!("hyperbytedb_replication_queue_drops_total").increment(1);
                    }
                });
                Ok(())
            }
            Err(tokio::sync::mpsc::error::TrySendError::Closed(_)) => {
                tracing::error!("replication outbound queue closed; dropping batch");
                counter!("hyperbytedb_replication_queue_drops_total").increment(1);
                Err(HyperbytedbError::Internal(
                    "replication outbound queue closed".into(),
                ))
            }
        }
    }

    async fn enqueue_batch_to_hinted_handoff(
        self: &Arc<Self>,
        batch: OutboundReplicationBatch,
    ) -> Result<(), HyperbytedbError> {
        let hh = self
            .hinted_handoff
            .as_ref()
            .ok_or_else(|| {
                HyperbytedbError::Internal(
                    "replication outbound queue full and hinted handoff is not configured".into(),
                )
            })?
            .clone();

        // The batch's own targets: a region's replicas, not every member.
        // Hinting every member made each spilled batch replay onto N-1 nodes
        // (573k hints from 115k spills at N=6 on n3), most of which do not
        // hold the region and parsed, indexed and logged it for nothing.
        let peers: Vec<u64> = self
            .resolve_replication_peers_async(batch.target_node_ids.as_deref())
            .await
            .into_iter()
            .map(|(id, _)| id)
            .collect();

        let hint = ReplicationHintPayload {
            database: batch.database.clone(),
            retention_policy: batch.retention_policy.clone(),
            precision: batch.precision.clone(),
            line_body: batch.body.clone(),
        };

        for peer_id in peers {
            hh.enqueue_hint(peer_id, &hint)?;
        }
        Ok(())
    }

    /// Synchronously fan a single batch out to all active peers and wait for
    /// at least `min_acks` of them to ack within `ack_timeout`. The local WAL
    /// append is assumed to have happened already; self is never counted.
    ///
    /// Behavior on partial success / timeout:
    /// - Once `min_acks` peers ack, this returns `Ok(())` immediately.
    /// - Remaining in-flight tasks are NOT cancelled — they continue to retry
    ///   and, on exhaustion, push to hinted-handoff. This preserves background
    ///   convergence even when the foreground call returns early.
    /// - On timeout: returns
    ///   [`HyperbytedbError::ReplicationQuorumTimeout`]; the in-flight tasks
    ///   keep running for the same reason.
    /// - When there are no active peers (single-node cluster) or `min_acks`
    ///   resolves to 0, returns `Ok(())` immediately.
    pub async fn replicate_write_sync(
        self: &Arc<Self>,
        batch: OutboundReplicationBatch,
        min_acks: usize,
        ack_timeout: Duration,
    ) -> Result<(), HyperbytedbError> {
        let start = std::time::Instant::now();
        let peers = self
            .resolve_replication_peers_async(batch.target_node_ids.as_deref())
            .await;

        let required = min_acks.min(peers.len());
        metrics::gauge!("hyperbytedb_replication_sync_required_acks").set(required as f64);
        if required == 0 {
            counter!("hyperbytedb_replication_sync_acks_total", "outcome" => "ok").increment(1);
            histogram!("hyperbytedb_replication_sync_duration_seconds")
                .record(start.elapsed().as_secs_f64());
            return Ok(());
        }

        let body = Bytes::copy_from_slice(&batch.body);
        histogram!("hyperbytedb_replication_batch_bytes").record(body.len() as f64);
        let hint_payload = ReplicationHintPayload {
            database: batch.database.clone(),
            retention_policy: batch.retention_policy.clone(),
            precision: batch.precision.clone(),
            line_body: batch.body.clone(),
        };

        // Acquire one shared inflight permit for this fan-out round so sync
        // and async share the same backpressure budget. Released when this
        // function returns; the per-peer background tasks themselves do NOT
        // hold the permit so a slow peer does not starve other rounds.
        let _permit = match self.batch_semaphore.clone().acquire_owned().await {
            Ok(p) => p,
            Err(_) => {
                counter!("hyperbytedb_replication_sync_acks_total", "outcome" => "error")
                    .increment(1);
                return Err(HyperbytedbError::ClusterUnavailable(
                    "replication semaphore closed".into(),
                ));
            }
        };

        // Spawn one per-peer task; each gets its own oneshot to deliver an ack.
        // We collect successes via a select_all loop so we can return as soon
        // as `required` peers ack without cancelling the rest.
        let mut waiters: Vec<oneshot::Receiver<Result<u64, String>>> =
            Vec::with_capacity(peers.len());
        for (peer_id, peer_addr) in peers.iter() {
            let (tx, rx) = oneshot::channel::<Result<u64, String>>();
            waiters.push(rx);
            let ctx = self.peer_replicate_ctx(
                *peer_id,
                peer_addr.clone(),
                body.clone(),
                batch.database.clone(),
                batch.retention_policy.clone(),
                batch.precision.clone(),
                batch.wal_seq,
                hint_payload.clone(),
                /* sync = */ true,
                batch.region_id,
            );
            tokio::spawn(async move {
                let res = ctx.run().await;
                let _ = tx.send(res);
            });
        }

        let mut acks = 0usize;
        let deadline = tokio::time::Instant::now() + ack_timeout;

        // Poll the waiters round-robin; on each iteration we await the SOONEST
        // resolving oneshot via `select_all` (drives all of them concurrently).
        let mut pending: Vec<_> = waiters.into_iter().map(Box::pin).collect();
        while acks < required && !pending.is_empty() {
            let timeout_fut = tokio::time::sleep_until(deadline);
            tokio::pin!(timeout_fut);

            let select = futures::future::select(
                futures::future::select_all(pending.iter_mut().map(|p| p.as_mut())),
                timeout_fut,
            );
            match select.await {
                futures::future::Either::Left(((res, idx, _), _)) => {
                    pending.remove(idx);
                    match res {
                        Ok(Ok(_wal_seq)) => {
                            acks += 1;
                        }
                        Ok(Err(_e)) => {
                            // peer-level failure (post-retries); already counted in metrics
                        }
                        Err(_recv) => {
                            // task panicked / dropped; treat as failure
                        }
                    }
                }
                futures::future::Either::Right(_) => {
                    counter!("hyperbytedb_replication_sync_acks_total", "outcome" => "timeout")
                        .increment(1);
                    histogram!("hyperbytedb_replication_sync_duration_seconds")
                        .record(start.elapsed().as_secs_f64());
                    return Err(HyperbytedbError::ReplicationQuorumTimeout {
                        acks_received: acks,
                        required,
                        timeout_ms: ack_timeout.as_millis() as u64,
                    });
                }
            }
        }

        if acks >= required {
            counter!("hyperbytedb_replication_sync_acks_total", "outcome" => "ok").increment(1);
            histogram!("hyperbytedb_replication_sync_duration_seconds")
                .record(start.elapsed().as_secs_f64());
            Ok(())
        } else {
            // All peers resolved but quorum not met (every peer errored).
            counter!("hyperbytedb_replication_sync_acks_total", "outcome" => "error").increment(1);
            histogram!("hyperbytedb_replication_sync_duration_seconds")
                .record(start.elapsed().as_secs_f64());
            Err(HyperbytedbError::ReplicationQuorumTimeout {
                acks_received: acks,
                required,
                timeout_ms: ack_timeout.as_millis() as u64,
            })
        }
    }

    #[allow(clippy::too_many_arguments)]
    fn peer_replicate_ctx(
        &self,
        peer_id: u64,
        peer_addr: String,
        body: Bytes,
        database: String,
        retention_policy: String,
        precision: Option<String>,
        wal_seq: u64,
        hint_payload: ReplicationHintPayload,
        sync: bool,
        region_id: Option<u64>,
    ) -> PeerReplicateCtx {
        PeerReplicateCtx {
            url: format!("http://{}/internal/replicate", peer_addr),
            client: self.http_client().clone(),
            replication_log: self.replication_log.clone(),
            hinted_handoff: self.hinted_handoff.clone(),
            origin_node: self.node_id,
            peer_id,
            body,
            database,
            retention_policy,
            precision,
            wal_seq,
            hint_payload,
            sync,
            region_id,
            max_attempts: self.max_retries,
        }
    }

    /// Hinted-handoff writes queued for `peer_id` (0 without a queue).
    #[must_use]
    pub fn pending_hints(&self, peer_id: u64) -> u64 {
        self.hinted_handoff
            .as_ref()
            .and_then(|hh| hh.pending_count(peer_id).ok())
            .unwrap_or(0)
    }

    /// Drain all queued hinted-handoff writes for `peer_id`, replaying
    /// them in FIFO order. Writes that still fail are re-enqueued.
    pub async fn drain_hints_for_peer(&self, peer_id: u64) {
        let hh = match &self.hinted_handoff {
            Some(hh) => hh.clone(),
            None => return,
        };

        let peer_addr = {
            let m = self.membership.read().await;
            match m.get_node(peer_id) {
                Some(n) => n.addr.clone(),
                None => return,
            }
        };

        tracing::debug!(peer_id = peer_id, "draining hinted handoff queue");
        let mut total_replayed = 0u64;

        loop {
            let batch = match hh.drain(peer_id, 100) {
                Ok(b) if b.is_empty() => break,
                Ok(b) => b,
                Err(e) => {
                    tracing::error!(error = %e, "failed to drain hinted handoff");
                    break;
                }
            };

            let url = format!("http://{}/internal/replicate", peer_addr);
            for i in 0..batch.len() {
                let p = &batch[i];
                let body = Bytes::copy_from_slice(&p.line_body);
                let mut req = self
                    .http_client()
                    .post(&url)
                    .header("Content-Type", LINE_PROTOCOL_MEDIA_TYPE_V1)
                    .header(HTTP_HEADER_REPLICATED, "true")
                    .header(HTTP_HEADER_DATABASE, &p.database)
                    .header(HTTP_HEADER_RETENTION_POLICY, &p.retention_policy)
                    .header(HTTP_HEADER_ORIGIN_NODE, self.node_id.to_string())
                    .body(body);
                if let Some(ref prec) = p.precision {
                    req = req.header(HTTP_HEADER_PRECISION, prec);
                }
                let result = req.send().await;

                match result {
                    Ok(resp) if resp.status().is_success() => {
                        total_replayed += 1;
                    }
                    _ => {
                        for hint in batch.iter().skip(i) {
                            if let Err(e) = hh.enqueue_hint(peer_id, hint) {
                                tracing::error!(error = %e, "failed to re-enqueue hint");
                            }
                        }
                        tracing::warn!(
                            peer_id = peer_id,
                            replayed = total_replayed,
                            "peer unreachable during drain, re-enqueued remaining"
                        );
                        return;
                    }
                }
            }
        }

        if total_replayed > 0 {
            tracing::debug!(
                peer_id = peer_id,
                replayed = total_replayed,
                "hinted handoff drain complete"
            );
        }
    }

    async fn do_replicate_mutation(
        &self,
        req: &MutationRequest,
        target_node_ids: Option<&[u64]>,
    ) -> Result<(), HyperbytedbError> {
        let mutation_seq = self.replication_log.append_mutation(req).map_err(|e| {
            tracing::error!(error = %e, "failed to log mutation");
            HyperbytedbError::Internal(format!("failed to log mutation: {e}").into())
        })?;

        let peers = self.resolve_replication_peers_async(target_node_ids).await;

        if peers.is_empty() {
            return Ok(());
        }

        let wire_req = MutationReplicateRequest {
            seq: mutation_seq,
            origin_node_id: self.node_id,
            mutation: req.clone(),
            target_node_ids: target_node_ids.map(|ids| ids.to_vec()),
        };

        let futures: Vec<_> = peers
            .iter()
            .map(|(peer_id, peer_addr)| {
                let url = format!("http://{}/internal/replicate-mutation", peer_addr);
                let client = self.http_client().clone();
                // `MutationWireRequest` is a plain owned struct (no maps with
                // non-string keys, no NaN floats), so serialization is
                // infallible in practice. Keep an explicit panic rather than
                // silently sending an empty body that the peer would reject
                // with an opaque error.
                #[allow(clippy::expect_used)]
                let body = serde_json::to_vec(&wire_req).expect("serialize mutation request");
                let pid = *peer_id;
                let repl_log = self.replication_log.clone();
                let max_attempts = self.max_retries;
                async move {
                    let mut attempts = 0u32;
                    let mut delay = Duration::from_secs(1);

                    loop {
                        attempts += 1;
                        let result = client
                            .post(&url)
                            .header("Content-Type", "application/json")
                            .header(HTTP_HEADER_REPLICATED, "true")
                            .body(body.clone())
                            .send()
                            .await;

                        match result {
                            Ok(resp) if resp.status().is_success() => {
                                tracing::debug!(peer = %url, "mutation replicated to peer");
                                if let Ok(ack) = resp.json::<MutationReplicateResponse>().await {
                                    let _ = repl_log.set_mutation_ack(pid, ack.ack_seq);
                                }
                                return true;
                            }
                            Ok(resp) => {
                                tracing::warn!(
                                    peer = %url,
                                    status = %resp.status(),
                                    attempt = attempts,
                                    "peer rejected replicated mutation"
                                );
                            }
                            Err(e) => {
                                tracing::warn!(
                                    peer = %url,
                                    error = %e,
                                    attempt = attempts,
                                    "failed to replicate mutation to peer"
                                );
                            }
                        }

                        if attempts >= max_attempts {
                            tracing::error!(
                                peer = %url,
                                "giving up mutation replication after {} attempts",
                                max_attempts
                            );
                            return false;
                        }
                        tokio::time::sleep(delay).await;
                        delay = (delay * 2).min(Duration::from_secs(30));
                    }
                }
            })
            .collect();

        let outcomes = futures::future::join_all(futures).await;
        if outcomes.iter().any(|ok| !ok) {
            return Err(HyperbytedbError::Internal(
                "mutation replication failed for one or more peers".into(),
            ));
        }
        Ok(())
    }

    /// Fan out a mutation to cluster peers (optionally scoped by node id).
    pub fn replicate_mutation(
        self: &Arc<Self>,
        req: MutationRequest,
        target_node_ids: Option<Vec<u64>>,
    ) {
        let this = Arc::clone(self);
        tokio::spawn(async move {
            if let Err(e) = this
                .do_replicate_mutation(&req, target_node_ids.as_deref())
                .await
            {
                tracing::error!(error = %e, "background mutation replication failed");
            }
        });
    }

    pub async fn replicate_mutation_sync(
        self: &Arc<Self>,
        req: MutationRequest,
    ) -> Result<(), HyperbytedbError> {
        self.do_replicate_mutation(&req, None).await
    }
}

#[async_trait::async_trait]
impl ReplicationPort for PeerClient {
    fn replicate_write(
        self: Arc<Self>,
        batch: OutboundReplicationBatch,
    ) -> Result<(), HyperbytedbError> {
        PeerClient::replicate_write(&self, batch)
    }

    async fn replicate_write_sync(
        self: Arc<Self>,
        batch: OutboundReplicationBatch,
        required_acks: usize,
        timeout: Duration,
    ) -> Result<(), HyperbytedbError> {
        PeerClient::replicate_write_sync(&self, batch, required_acks, timeout).await
    }

    fn replicate_mutation(
        self: Arc<Self>,
        req: MutationRequest,
        target_node_ids: Option<Vec<u64>>,
    ) {
        PeerClient::replicate_mutation(&self, req, target_node_ids);
    }

    async fn replicate_mutation_sync(
        self: Arc<Self>,
        req: MutationRequest,
    ) -> Result<(), HyperbytedbError> {
        PeerClient::replicate_mutation_sync(&self, req).await
    }

    async fn active_peer_count(&self, self_node_id: u64) -> usize {
        let m = self.membership.read().await;
        m.active_peers(self_node_id).len()
    }
}

/// Most queued parts one send round takes.
const MAX_PARTS_PER_ROUND: usize = 4096;

/// Replication targets as `(node id, addr)`: `targets` when the batch names
/// them (region peers), otherwise every replication peer but `self_id`.
fn resolve_replication_peers(
    m: &ClusterMembership,
    self_id: u64,
    targets: Option<&[u64]>,
) -> Vec<(u64, String)> {
    match targets {
        Some(targets) => targets
            .iter()
            .filter_map(|id| m.get_node(*id).map(|n| (n.node_id, n.addr.clone())))
            .collect(),
        None => m
            .replication_peers(self_id)
            .into_iter()
            .map(|n| (n.node_id, n.addr.clone()))
            .collect(),
    }
}

/// Merge consecutive jobs that [`coalesce_ok`] allows into one body.
fn coalesce_jobs(
    jobs: Vec<OutboundReplicationBatch>,
    max_body: usize,
) -> Vec<OutboundReplicationBatch> {
    let mut out: Vec<OutboundReplicationBatch> = Vec::with_capacity(jobs.len());
    for next in jobs {
        if let Some(job) = out.last_mut()
            && coalesce_ok(job, &next, max_body)
        {
            if !job.body.is_empty() && !next.body.is_empty() {
                job.body.push(b'\n');
            }
            job.body.extend_from_slice(&next.body);
            job.wal_seq = next.wal_seq;
            continue;
        }
        out.push(next);
    }
    out
}

fn coalesce_ok(
    a: &OutboundReplicationBatch,
    b: &OutboundReplicationBatch,
    max_body: usize,
) -> bool {
    if a.database != b.database
        || a.retention_policy != b.retention_policy
        || a.precision != b.precision
    {
        return false;
    }
    // Two regions can legitimately share a peer set, so matching targets is
    // not enough to make a merged body single-region — and the region travels
    // on the wire as a header describing the WHOLE body. Merging across
    // regions would make that header a lie about half the rows.
    if a.region_id != b.region_id {
        return false;
    }
    // A merged body is one encoding or the other: frames and line protocol
    // cannot share a body.
    if crate::application::line_protocol::is_binary_points(&a.body)
        != crate::application::line_protocol::is_binary_points(&b.body)
    {
        return false;
    }
    // Targets are part of the identity of a batch, not incidental to it.
    // Under sharding each region gets its own batch with its own peer set, and
    // the merge keeps the FIRST batch's targets — so coalescing two regions'
    // batches sent one WAL sequence apart delivered region B's rows to region
    // A's peers and never to B's own. That is an RF hole, not a duplicate:
    // it reads as loss the moment B fails over. Batches from one request share
    // a WAL sequence and so were never at risk; consecutive sequences from
    // concurrent requests were.
    if a.target_node_ids != b.target_node_ids {
        return false;
    }
    if b.wal_seq != a.wal_seq + 1 {
        return false;
    }
    let combined = a.body.len().saturating_add(1).saturating_add(b.body.len());
    combined <= max_body
}

/// Per-peer replication context shared by the async fan-out loop and the
/// sync W-of-N path. The retry/backoff policy is identical in both modes;
/// the only differences are (a) whether we set `X-Hyperbytedb-Sync: true`
/// and (b) what we do with the result after retries are exhausted.
struct PeerReplicateCtx {
    url: String,
    client: reqwest::Client,
    replication_log: Arc<ReplicationLog>,
    hinted_handoff: Option<Arc<HintedHandoff>>,
    origin_node: u64,
    peer_id: u64,
    body: Bytes,
    database: String,
    retention_policy: String,
    precision: Option<String>,
    wal_seq: u64,
    hint_payload: ReplicationHintPayload,
    sync: bool,
    /// Region this body belongs to, when sharding is on. `None` for
    /// unsharded writes; a replayed hint also arrives without one, since the
    /// on-disk hint record predates the region being carried.
    region_id: Option<u64>,
    max_attempts: u32,
}

impl PeerReplicateCtx {
    /// Send the batch to a single peer, retrying with exponential backoff. On
    /// success returns the WAL sequence the peer acked at (0 in async mode
    /// where the peer 204s without a body). On exhaustion returns the last
    /// error after pushing to hinted-handoff (when configured).
    async fn run(self) -> Result<u64, String> {
        let mut attempts = 0u32;
        let mut delay = Duration::from_secs(1);
        // `last_error` is overwritten on each retry but only read after the
        // final attempt, so each intermediate assignment is intentionally
        // dropped on the next iteration.
        #[allow(unused_assignments)]
        let mut last_error: Option<String> = None;

        loop {
            attempts += 1;
            let mut req = self
                .client
                .post(&self.url)
                .header("Content-Type", LINE_PROTOCOL_MEDIA_TYPE_V1)
                .header(HTTP_HEADER_REPLICATED, "true")
                .header(HTTP_HEADER_DATABASE, &self.database)
                .header(HTTP_HEADER_RETENTION_POLICY, &self.retention_policy)
                .header(HTTP_HEADER_ORIGIN_NODE, self.origin_node.to_string())
                .body(self.body.clone());
            if let Some(region_id) = self.region_id {
                req = req.header(HTTP_HEADER_REGION, region_id.to_string());
            }
            if let Some(ref p) = self.precision {
                req = req.header(HTTP_HEADER_PRECISION, p);
            }
            if self.sync {
                req = req.header(HTTP_HEADER_SYNC, "true");
            }
            let http_start = std::time::Instant::now();
            let result = req.send().await;
            let elapsed = http_start.elapsed();
            histogram!("hyperbytedb_replication_http_seconds", "peer" => self.url.clone())
                .record(elapsed.as_secs_f64());
            if self.sync {
                histogram!(
                    "hyperbytedb_replication_sync_peer_ack_seconds",
                    "peer" => self.url.clone()
                )
                .record(elapsed.as_secs_f64());
            }

            match result {
                Ok(resp) if resp.status().is_success() => {
                    tracing::debug!(peer = %self.url, "write replicated to peer");
                    counter!("hyperbytedb_replication_writes_total", "peer" => self.url.clone())
                        .increment(1);
                    // Async mode returns before the peer applies to WAL; only
                    // record ack watermarks when sync mode confirms persistence.
                    let ack_seq = if self.sync {
                        let seq = resp
                            .json::<serde_json::Value>()
                            .await
                            .ok()
                            .and_then(|v| v.get("ack_seq").and_then(|n| n.as_u64()))
                            .unwrap_or(self.wal_seq);
                        let _ = self.replication_log.set_wal_ack(self.peer_id, seq);
                        seq
                    } else {
                        0
                    };
                    return Ok(ack_seq);
                }
                Ok(resp) => {
                    let status = resp.status();
                    last_error = Some(format!("peer returned {}", status));
                    tracing::warn!(
                        peer = %self.url,
                        status = %status,
                        attempt = attempts,
                        "peer rejected replicated write"
                    );
                    counter!("hyperbytedb_replication_errors_total", "peer" => self.url.clone())
                        .increment(1);
                }
                Err(e) => {
                    last_error = Some(e.to_string());
                    tracing::warn!(
                        peer = %self.url,
                        error = %e,
                        attempt = attempts,
                        "failed to replicate write to peer"
                    );
                    counter!("hyperbytedb_replication_errors_total", "peer" => self.url.clone())
                        .increment(1);
                }
            }

            if attempts >= self.max_attempts {
                if let Some(ref hh) = self.hinted_handoff {
                    if let Err(e) = hh.enqueue_hint(self.peer_id, &self.hint_payload) {
                        tracing::error!(
                            peer_id = self.peer_id,
                            error = %e,
                            "failed to enqueue hinted handoff"
                        );
                    } else {
                        tracing::warn!(
                            peer_id = self.peer_id,
                            "replication failed, write queued for hinted handoff"
                        );
                    }
                } else {
                    tracing::error!(
                        peer = %self.url,
                        "giving up replication after {} attempts (no hinted handoff)",
                        self.max_attempts
                    );
                }
                return Err(last_error.unwrap_or_else(|| "unknown error".to_string()));
            }
            tokio::time::sleep(delay).await;
            delay = (delay * 2).min(Duration::from_secs(30));
        }
    }
}

#[cfg(test)]
mod coalesce_tests {
    use super::coalesce_ok;
    use crate::ports::replication::OutboundReplicationBatch;

    fn batch(wal_seq: u64, targets: Option<Vec<u64>>) -> OutboundReplicationBatch {
        OutboundReplicationBatch {
            database: "db".into(),
            retention_policy: "autogen".into(),
            precision: None,
            body: b"cpu,host=h v=1".to_vec(),
            wal_seq,
            target_node_ids: targets,
            region_id: None,
        }
    }

    /// Two regions' batches must never merge: the merge keeps the first
    /// batch's targets, so region B's rows would be delivered to region A's
    /// peer set and never to B's own. That is an RF hole that surfaces as loss
    /// on failover, not a duplicate.
    #[test]
    fn binary_and_text_bodies_do_not_coalesce() {
        let mut a = batch(10, None);
        let b = batch(11, None);
        a.body = crate::application::line_protocol::encode_points_binary(&[]).unwrap();
        assert!(!coalesce_ok(&a, &b, 1 << 20));
    }

    #[test]
    fn batches_for_different_region_peer_sets_do_not_coalesce() {
        let a = batch(10, Some(vec![2, 3]));
        let b = batch(11, Some(vec![4, 5]));
        assert!(
            !coalesce_ok(&a, &b, 1 << 20),
            "batches with different targets were merged; the merged body would \
             go only to {:?}",
            a.target_node_ids
        );
    }

    /// The unsharded case (no targets) and the same-peer-set case must still
    /// coalesce — this fix must not cost the batching it exists for.
    #[test]
    fn batches_sharing_a_peer_set_still_coalesce() {
        assert!(
            coalesce_ok(
                &batch(10, Some(vec![2, 3])),
                &batch(11, Some(vec![2, 3])),
                1 << 20
            ),
            "identical target sets must still merge"
        );
        assert!(
            coalesce_ok(&batch(10, None), &batch(11, None), 1 << 20),
            "unsharded batches must still merge"
        );
    }

    fn region_batch(
        wal_seq: u64,
        targets: Option<Vec<u64>>,
        region: Option<u64>,
    ) -> OutboundReplicationBatch {
        OutboundReplicationBatch {
            region_id: region,
            ..batch(wal_seq, targets)
        }
    }

    /// Two regions can share a peer set, so matching targets is NOT enough to
    /// make a merged body single-region. The region travels as a header
    /// describing the whole body, so merging across regions would make that
    /// header wrong about half the rows — and the receiver's membership check
    /// would then judge them against the wrong region.
    #[test]
    fn batches_for_different_regions_do_not_coalesce_even_with_equal_targets() {
        let a = region_batch(10, Some(vec![2, 3]), Some(7));
        let b = region_batch(11, Some(vec![2, 3]), Some(8));
        assert_eq!(a.target_node_ids, b.target_node_ids, "same peer set");
        assert!(
            !coalesce_ok(&a, &b, 1 << 20),
            "two regions sharing a peer set must still not merge"
        );
        // Same region, same targets: merging is the whole point of coalescing.
        assert!(coalesce_ok(
            &region_batch(10, Some(vec![2, 3]), Some(7)),
            &region_batch(11, Some(vec![2, 3]), Some(7)),
            1 << 20
        ));
    }

    /// Targets differing only by presence are still different targets.
    #[test]
    fn a_targeted_batch_never_merges_with_an_untargeted_one() {
        assert!(!coalesce_ok(
            &batch(10, None),
            &batch(11, Some(vec![2])),
            1 << 20
        ));
        assert!(!coalesce_ok(
            &batch(10, Some(vec![2])),
            &batch(11, None),
            1 << 20
        ));
    }
}

#[cfg(test)]
mod replicate_batch_tests {
    use super::*;
    use crate::domain::cluster::membership::{NodeInfo, NodeState};
    use crate::domain::cluster::replication_wire::ReplicateBatchRequest;
    use std::sync::atomic::AtomicU64;

    fn part(region: u64) -> ReplicateBatchPart {
        ReplicateBatchPart {
            database: "db".into(),
            retention_policy: "autogen".into(),
            precision: None,
            region_id: Some(region),
            body: format!("cpu,host=r{region} v=1").into_bytes(),
        }
    }

    fn job(region: u64, targets: Vec<u64>) -> OutboundReplicationBatch {
        let p = part(region);
        OutboundReplicationBatch {
            database: p.database,
            retention_policy: p.retention_policy,
            precision: None,
            body: p.body,
            wal_seq: region,
            target_node_ids: Some(targets),
            region_id: Some(region),
        }
    }

    #[test]
    fn the_sent_encoding_is_a_replicate_batch_request() {
        let parts = vec![part(1), part(2)];
        let sent = bincode::serialize(&(7u64, &parts)).unwrap();
        let decoded: ReplicateBatchRequest = bincode::deserialize(&sent).unwrap();
        assert_eq!(
            decoded,
            ReplicateBatchRequest {
                origin_node: 7,
                parts
            }
        );
    }

    async fn serve(app: axum::Router) -> String {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap().to_string();
        tokio::spawn(async move {
            let _ = axum::serve(listener, app).await;
        });
        addr
    }

    fn node(id: u64, addr: &str) -> NodeInfo {
        NodeInfo {
            node_id: id,
            addr: addr.to_string(),
            state: NodeState::Active,
            joined_at: 0,
            last_heartbeat: 0,
            needs_sync: false,
            consecutive_misses: 0,
        }
    }

    /// Node 2 speaks replicate-batch and refuses region 99 once (queue
    /// full); node 3 predates the endpoint. Each gets every part meant for
    /// it: node 2 in one request plus a retry of the refused part only,
    /// node 3 one request per part.
    #[tokio::test]
    async fn a_round_sends_each_peer_one_request_and_retries_only_refused_parts() {
        let batch_calls = Arc::new(AtomicU64::new(0));
        let parts_seen = Arc::new(Mutex::new(Vec::<Vec<Option<u64>>>::new()));
        let (calls, seen) = (batch_calls.clone(), parts_seen.clone());
        let batch_peer = serve(axum::Router::new().route(
            "/internal/replicate-batch",
            axum::routing::post(move |body: axum::body::Bytes| {
                let (calls, seen) = (calls.clone(), seen.clone());
                async move {
                    let first = calls.fetch_add(1, Ordering::SeqCst) == 0;
                    let req: ReplicateBatchRequest = bincode::deserialize(&body).unwrap();
                    seen.lock()
                        .unwrap()
                        .push(req.parts.iter().map(|p| p.region_id).collect());
                    let results = req
                        .parts
                        .iter()
                        .map(|p| {
                            if first && p.region_id == Some(99) {
                                ReplicatePartOutcome::QueueFull
                            } else {
                                ReplicatePartOutcome::Accepted
                            }
                        })
                        .collect();
                    bincode::serialize(&ReplicateBatchResponse { results }).unwrap()
                }
            }),
        ))
        .await;
        let legacy_calls = Arc::new(AtomicU64::new(0));
        let lc = legacy_calls.clone();
        let legacy_peer = serve(axum::Router::new().route(
            "/internal/replicate",
            axum::routing::post(move || {
                let lc = lc.clone();
                async move {
                    lc.fetch_add(1, Ordering::SeqCst);
                    axum::http::StatusCode::OK
                }
            }),
        ))
        .await;

        let mut m = ClusterMembership::new();
        m.add_node(node(2, &batch_peer));
        m.add_node(node(3, &legacy_peer));
        let dir = tempfile::tempdir().unwrap();
        let pc = PeerClient::new(
            1,
            "127.0.0.1:1".into(),
            crate::domain::cluster::membership::new_shared(m),
            Arc::new(ReplicationLog::open(dir.path()).unwrap()),
            3,
            64,
            2,
            1 << 20,
            Default::default(),
        );

        pc.replicate_round(vec![
            job(1, vec![2]),
            job(2, vec![2, 3]),
            job(99, vec![2]),
            job(4, vec![2, 3]),
            job(5, vec![3]),
        ])
        .await;

        let seen = parts_seen.lock().unwrap().clone();
        assert_eq!(
            seen,
            vec![vec![Some(1), Some(2), Some(99), Some(4)], vec![Some(99)],],
            "one request for node 2's parts, then only the refused part"
        );
        assert_eq!(
            legacy_calls.load(Ordering::SeqCst),
            3,
            "node 3: parts 2, 4, 5"
        );
    }

    /// A batch that spills to hinted handoff because the outbound queue is
    /// full is hinted to its region's replicas only, not to every member.
    #[tokio::test]
    async fn a_spilled_batch_is_hinted_to_its_targets_only() {
        let mut m = ClusterMembership::new();
        for id in 2..=4 {
            m.add_node(node(id, "127.0.0.1:1"));
        }
        let dir = tempfile::tempdir().unwrap();
        let log = Arc::new(ReplicationLog::open(dir.path()).unwrap());
        let hh = Arc::new(HintedHandoff::new(log.db().clone(), 100).unwrap());
        let pc = Arc::new(
            PeerClient::new(
                1,
                "127.0.0.1:1".into(),
                crate::domain::cluster::membership::new_shared(m),
                log,
                3,
                1,
                2,
                1 << 20,
                Default::default(),
            )
            .with_hinted_handoff(hh.clone()),
        );
        // No consumer: the one-slot queue fills and the next batch spills.
        pc.outbound_started.store(true, Ordering::SeqCst);
        PeerClient::replicate_write(&pc, job(1, vec![3])).unwrap();
        PeerClient::replicate_write(&pc, job(2, vec![3])).unwrap();

        for _ in 0..100 {
            if hh.total_pending().unwrap() > 0 {
                break;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
        assert_eq!(hh.pending_count(3).unwrap(), 1, "the region's replica");
        assert_eq!(hh.pending_count(2).unwrap(), 0, "not a replica of region 2");
        assert_eq!(hh.pending_count(4).unwrap(), 0, "not a replica of region 2");
    }
}

#[cfg(test)]
mod http2_tests {
    use super::peer_http_client;

    async fn version_echo() -> String {
        let app = axum::Router::new().route(
            "/v",
            axum::routing::get(|req: axum::extract::Request| async move {
                format!("{:?}", req.version())
            }),
        );
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });
        format!("http://{addr}/v")
    }

    /// `internal_http2_connections` spreads a peer's requests over that many
    /// HTTP/2 connections, each request taking the next in turn.
    #[tokio::test]
    async fn requests_spread_over_the_configured_connections() {
        use std::collections::HashSet;
        use std::net::SocketAddr;
        use std::sync::{Arc, Mutex};

        let seen: Arc<Mutex<HashSet<SocketAddr>>> = Arc::default();
        let record = seen.clone();
        let app = axum::Router::new().route(
            "/v",
            axum::routing::get(
                move |axum::extract::ConnectInfo(from): axum::extract::ConnectInfo<SocketAddr>| {
                    let record = record.clone();
                    async move {
                        record.lock().unwrap().insert(from);
                        "ok"
                    }
                },
            ),
        );
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        tokio::spawn(async move {
            axum::serve(
                listener,
                app.into_make_service_with_connect_info::<SocketAddr>(),
            )
            .await
            .unwrap()
        });

        let dir = tempfile::tempdir().unwrap();
        let log = Arc::new(
            crate::adapters::cluster::replication_log::ReplicationLog::open(dir.path()).unwrap(),
        );
        let membership = crate::domain::cluster::membership::new_shared(
            crate::domain::cluster::membership::ClusterMembership::new(),
        );
        let pc = super::PeerClient::new(
            1,
            "127.0.0.1:9".into(),
            membership,
            log,
            1,
            16,
            1,
            1024,
            Default::default(),
        )
        .with_http2_connections(4);
        for _ in 0..32 {
            let r = pc
                .http_client()
                .get(format!("http://{addr}/v"))
                .send()
                .await
                .unwrap();
            assert!(r.status().is_success());
        }
        assert_eq!(
            seen.lock().unwrap().len(),
            4,
            "one connection per configured slot"
        );
    }

    /// Every connection of a PeerClient, extra HTTP/2 ones included, sends
    /// the credential it was built with, and it is kept for other clients.
    #[tokio::test]
    async fn a_peer_client_sends_its_cluster_token() {
        use crate::adapters::cluster::cluster_auth::{ClusterAuth, HEADER};
        let app = axum::Router::new().route(
            "/t",
            axum::routing::get(|headers: axum::http::HeaderMap| async move {
                headers
                    .get(HEADER)
                    .map(|v| v.to_str().unwrap().to_string())
                    .unwrap_or_default()
            }),
        );
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });

        let dir = tempfile::tempdir().unwrap();
        let log = std::sync::Arc::new(
            crate::adapters::cluster::replication_log::ReplicationLog::open(dir.path()).unwrap(),
        );
        let membership = crate::domain::cluster::membership::new_shared(
            crate::domain::cluster::membership::ClusterMembership::new(),
        );
        let pc = super::PeerClient::new(
            1,
            "127.0.0.1:9".into(),
            membership,
            log,
            1,
            16,
            1,
            1024,
            ClusterAuth::new(Some("t0ken")).unwrap(),
        )
        .with_http2_connections(3);
        for _ in 0..3 {
            let got = pc
                .http_client()
                .get(format!("http://{addr}/t"))
                .send()
                .await
                .unwrap()
                .text()
                .await
                .unwrap();
            assert_eq!(got, "t0ken");
        }
        assert!(pc.cluster_auth().token_matches(b"t0ken"));
    }

    /// The peer client speaks HTTP/2 to the axum server (cleartext, no
    /// upgrade round trip).
    #[tokio::test]
    async fn peer_client_speaks_http2() {
        let url = version_echo().await;
        let resp = peer_http_client(&super::ClusterAuth::default())
            .get(&url)
            .send()
            .await
            .unwrap();
        assert_eq!(resp.text().await.unwrap(), "HTTP/2.0");
    }
}
