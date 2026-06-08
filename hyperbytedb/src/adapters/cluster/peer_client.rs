use bytes::Bytes;
use metrics::{counter, histogram};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;
use tokio::sync::{Semaphore, oneshot};

use crate::adapters::cluster::hinted_handoff::HintedHandoff;
use crate::adapters::cluster::replication_log::ReplicationLog;
use crate::domain::cluster::membership::SharedMembership;
use crate::domain::cluster::replication_wire::{
    HTTP_HEADER_DATABASE, HTTP_HEADER_ORIGIN_NODE, HTTP_HEADER_PRECISION, HTTP_HEADER_REPLICATED,
    HTTP_HEADER_RETENTION_POLICY, HTTP_HEADER_SYNC, LINE_PROTOCOL_MEDIA_TYPE_V1,
    ReplicationHintPayload,
};
use crate::domain::cluster::types::{
    MutationReplicateRequest, MutationReplicateResponse, MutationRequest,
};
use crate::error::HyperbytedbError;
use crate::ports::replication::{OutboundReplicationBatch, ReplicationPort};

pub struct PeerClient {
    membership: SharedMembership,
    replication_log: Arc<ReplicationLog>,
    hinted_handoff: Option<Arc<HintedHandoff>>,
    client: reqwest::Client,
    node_id: u64,
    node_addr: String,
    max_retries: u32,
    outbound_tx: tokio::sync::mpsc::Sender<OutboundReplicationBatch>,
    outbound_rx: Mutex<Option<tokio::sync::mpsc::Receiver<OutboundReplicationBatch>>>,
    outbound_started: AtomicBool,
    batch_semaphore: Arc<Semaphore>,
    max_coalesce_body_bytes: usize,
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
    ) -> Self {
        let depth = outbound_queue_depth.max(1);
        // See raft/network.rs: builder.build() only fails on broken TLS init,
        // and `Client::new()` would also be unusable in that case.
        let client = reqwest::Client::builder()
            .timeout(Duration::from_secs(10))
            .build()
            .unwrap_or_else(|_| reqwest::Client::new());
        let (outbound_tx, outbound_rx) =
            tokio::sync::mpsc::channel::<OutboundReplicationBatch>(depth);
        let inflight = max_inflight_batches.max(1);

        Self {
            membership,
            replication_log,
            hinted_handoff: None,
            client,
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
        let mut pending: Option<OutboundReplicationBatch> = None;
        loop {
            let mut job = if let Some(j) = pending.take() {
                j
            } else {
                match rx.recv().await {
                    Some(j) => j,
                    None => break,
                }
            };

            loop {
                match rx.try_recv() {
                    Ok(next) if coalesce_ok(&job, &next, self.max_coalesce_body_bytes) => {
                        if !job.body.is_empty() && !next.body.is_empty() {
                            job.body.push(b'\n');
                        }
                        job.body.extend_from_slice(&next.body);
                        job.wal_seq = next.wal_seq;
                    }
                    Ok(next) => {
                        pending = Some(next);
                        break;
                    }
                    Err(tokio::sync::mpsc::error::TryRecvError::Empty) => break,
                    Err(tokio::sync::mpsc::error::TryRecvError::Disconnected) => break,
                }
            }

            let permit = match self.batch_semaphore.clone().acquire_owned().await {
                Ok(p) => p,
                Err(_) => return,
            };
            let this = Arc::clone(&self);
            let j = job;
            tokio::spawn(async move {
                this.do_replicate_write(&j).await;
                drop(permit);
            });
        }

        if let Some(j) = pending {
            let permit = match self.batch_semaphore.clone().acquire_owned().await {
                Ok(p) => p,
                Err(_) => return,
            };
            let this = Arc::clone(&self);
            tokio::spawn(async move {
                this.do_replicate_write(&j).await;
                drop(permit);
            });
        }
    }

    pub fn node_addr(&self) -> &str {
        &self.node_addr
    }

    pub fn node_id(&self) -> u64 {
        self.node_id
    }

    pub fn membership(&self) -> &SharedMembership {
        &self.membership
    }

    pub fn replication_log(&self) -> &Arc<ReplicationLog> {
        &self.replication_log
    }

    /// Fan out a line-protocol batch to all active peers (bounded queue + coalescing worker).
    pub fn replicate_write(self: &Arc<Self>, batch: OutboundReplicationBatch) {
        self.start_outbound_processor();
        if let Err(e) = self.outbound_tx.try_send(batch) {
            tracing::error!(error = %e, "replication outbound queue full or closed; dropping batch");
            counter!("hyperbytedb_replication_queue_drops_total").increment(1);
        }
    }

    async fn do_replicate_write(&self, job: &OutboundReplicationBatch) {
        let start = std::time::Instant::now();
        let peers = {
            let m = self.membership.read().await;
            m.replication_peers(self.node_id)
                .into_iter()
                .map(|n| (n.node_id, n.addr.clone()))
                .collect::<Vec<_>>()
        };

        if peers.is_empty() {
            return;
        }

        let body = Bytes::copy_from_slice(&job.body);
        histogram!("hyperbytedb_replication_batch_bytes").record(body.len() as f64);
        let hint_payload = ReplicationHintPayload {
            database: job.database.clone(),
            retention_policy: job.retention_policy.clone(),
            precision: job.precision.clone(),
            line_body: job.body.clone(),
        };

        let futures: Vec<_> = peers
            .iter()
            .map(|(peer_id, peer_addr)| {
                let ctx = self.peer_replicate_ctx(
                    *peer_id,
                    peer_addr.clone(),
                    body.clone(),
                    job.database.clone(),
                    job.retention_policy.clone(),
                    job.precision.clone(),
                    job.wal_seq,
                    hint_payload.clone(),
                    /* sync = */ false,
                );
                async move {
                    let _ = ctx.run().await;
                }
            })
            .collect();

        futures::future::join_all(futures).await;
        histogram!("hyperbytedb_replication_duration_seconds")
            .record(start.elapsed().as_secs_f64());
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
        let peers = {
            let m = self.membership.read().await;
            m.replication_peers(self.node_id)
                .into_iter()
                .map(|n| (n.node_id, n.addr.clone()))
                .collect::<Vec<_>>()
        };

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
    ) -> PeerReplicateCtx {
        PeerReplicateCtx {
            url: format!("http://{}/internal/replicate", peer_addr),
            client: self.client.clone(),
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
            max_attempts: self.max_retries,
        }
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

        tracing::info!(peer_id = peer_id, "draining hinted handoff queue");
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
                    .client
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
            tracing::info!(
                peer_id = peer_id,
                replayed = total_replayed,
                "hinted handoff drain complete"
            );
        }
    }

    async fn do_replicate_mutation(&self, req: &MutationRequest) {
        let mutation_seq = match self.replication_log.append_mutation(req) {
            Ok(seq) => seq,
            Err(e) => {
                tracing::error!(error = %e, "failed to log mutation");
                return;
            }
        };

        let peers = {
            let m = self.membership.read().await;
            m.replication_peers(self.node_id)
                .into_iter()
                .map(|n| (n.node_id, n.addr.clone()))
                .collect::<Vec<_>>()
        };

        if peers.is_empty() {
            return;
        }

        let wire_req = MutationReplicateRequest {
            seq: mutation_seq,
            origin_node_id: self.node_id,
            mutation: req.clone(),
        };

        let futures: Vec<_> = peers
            .iter()
            .map(|(peer_id, peer_addr)| {
                let url = format!("http://{}/internal/replicate-mutation", peer_addr);
                let client = self.client.clone();
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
                                break;
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
                            break;
                        }
                        tokio::time::sleep(delay).await;
                        delay = (delay * 2).min(Duration::from_secs(30));
                    }
                }
            })
            .collect();

        futures::future::join_all(futures).await;
    }

    /// Fan out a mutation to all active peers.
    pub fn replicate_mutation(self: &Arc<Self>, req: MutationRequest) {
        let this = Arc::clone(self);
        tokio::spawn(async move {
            this.do_replicate_mutation(&req).await;
        });
    }
}

#[async_trait::async_trait]
impl ReplicationPort for PeerClient {
    fn replicate_write(self: Arc<Self>, batch: OutboundReplicationBatch) {
        PeerClient::replicate_write(&self, batch);
    }

    async fn replicate_write_sync(
        self: Arc<Self>,
        batch: OutboundReplicationBatch,
        required_acks: usize,
        timeout: Duration,
    ) -> Result<(), HyperbytedbError> {
        PeerClient::replicate_write_sync(&self, batch, required_acks, timeout).await
    }

    fn replicate_mutation(self: Arc<Self>, req: MutationRequest) {
        PeerClient::replicate_mutation(&self, req);
    }

    async fn active_peer_count(&self, self_node_id: u64) -> usize {
        let m = self.membership.read().await;
        m.active_peers(self_node_id).len()
    }
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
