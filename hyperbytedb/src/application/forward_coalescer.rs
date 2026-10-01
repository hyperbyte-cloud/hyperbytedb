//! One request per destination node for many concurrent writes' forwards.
//!
//! Every client write is split by region primary, so at N nodes it forwards
//! to N-1 of them, and each forward used to be its own HTTP request. Per
//! point, that request overhead grew with N: per DB core, throughput fell
//! from 85k points/s at N=3 to 71k at N=8 on n3.
//!
//! As TiKV's client-go `batchConn` does per store, each destination gets a
//! lane: a queue and a loop that sends at most `max_inflight` requests at a
//! time. While those are out, new forwards queue, and the next request takes
//! everything queued (up to `max_bytes` / `max_parts`). An idle node sends a
//! forward at once; a busy one sends fewer, larger requests. The receiver
//! answers per part, so each write gets its own parts' results back.

use std::collections::HashMap;
use std::sync::Arc;
use std::time::{Duration, Instant};

use metrics::{counter, histogram};
use tokio::sync::{Semaphore, mpsc, oneshot};

use crate::domain::sharding::{
    ShardWriteBatch, ShardWriteBatchResponse, ShardWritePartResult, ShardWriteRequest,
};

/// What a destination said about one write's parts.
#[derive(Debug, Clone)]
pub enum NodeSendOutcome {
    /// One result per part, in the order sent.
    Results(Vec<ShardWritePartResult>),
    /// The node has no batch endpoint (an older build).
    Unsupported,
    /// The node refused the request at write capacity.
    Capacity,
    /// Transport failure or an unusable response.
    Failed(String),
}

struct Job {
    parts: Vec<ShardWriteRequest>,
    bytes: usize,
    reply: oneshot::Sender<NodeSendOutcome>,
}

#[derive(Clone, Copy)]
struct Limits {
    max_inflight: usize,
    max_bytes: usize,
    max_parts: usize,
}

pub struct ForwardCoalescer {
    lanes: parking_lot::Mutex<HashMap<String, mpsc::UnboundedSender<Job>>>,
    limits: Limits,
}

impl Default for ForwardCoalescer {
    fn default() -> Self {
        // 512 KiB stays a sixth of a 3-CPU node's 3 MiB shard admission
        // budget, so one merged request is never refused for its size alone.
        Self::new(4, 512 * 1024, 256)
    }
}

impl ForwardCoalescer {
    #[must_use]
    pub fn new(max_inflight: usize, max_bytes: usize, max_parts: usize) -> Self {
        Self {
            lanes: parking_lot::Mutex::new(HashMap::new()),
            limits: Limits {
                max_inflight: max_inflight.max(1),
                max_bytes: max_bytes.max(1),
                max_parts: max_parts.max(1),
            },
        }
    }

    /// Destinations with a lane (one per node forwarded to so far).
    #[must_use]
    pub fn lane_count(&self) -> usize {
        self.lanes.lock().len()
    }

    /// Send `parts` to the node at `addr`, in a request shared with other
    /// writes' parts for that node.
    pub async fn send(
        &self,
        client: &reqwest::Client,
        addr: &str,
        timeout: Duration,
        parts: Vec<ShardWriteRequest>,
    ) -> NodeSendOutcome {
        let bytes = parts.iter().map(|p| p.body.len() + 64).sum();
        let (reply, rx) = oneshot::channel();
        let job = Job {
            parts,
            bytes,
            reply,
        };
        if let Err(mpsc::error::SendError(job)) = self.lane(client, addr, timeout).send(job) {
            // The lane's loop has gone; start a new one.
            self.lanes.lock().remove(addr);
            if self.lane(client, addr, timeout).send(job).is_err() {
                return NodeSendOutcome::Failed(format!("forward lane to {addr} closed"));
            }
        }
        rx.await
            .unwrap_or_else(|_| NodeSendOutcome::Failed(format!("forward lane to {addr} dropped")))
    }

    fn lane(
        &self,
        client: &reqwest::Client,
        addr: &str,
        timeout: Duration,
    ) -> mpsc::UnboundedSender<Job> {
        let mut lanes = self.lanes.lock();
        if let Some(tx) = lanes.get(addr)
            && !tx.is_closed()
        {
            return tx.clone();
        }
        let (tx, rx) = mpsc::unbounded_channel();
        tokio::spawn(lane_loop(
            rx,
            client.clone(),
            addr.to_string(),
            timeout,
            self.limits,
        ));
        lanes.insert(addr.to_string(), tx.clone());
        tx
    }
}

async fn lane_loop(
    mut rx: mpsc::UnboundedReceiver<Job>,
    client: reqwest::Client,
    addr: String,
    timeout: Duration,
    limits: Limits,
) {
    let inflight = Arc::new(Semaphore::new(limits.max_inflight));
    let mut carried: Option<Job> = None;
    loop {
        // Wait for a free slot first: while every slot is busy, forwards
        // queue up and the next request carries them all.
        let Ok(permit) = inflight.clone().acquire_owned().await else {
            return;
        };
        let first = match carried.take() {
            Some(job) => job,
            None => match rx.recv().await {
                Some(job) => job,
                None => return,
            },
        };
        let (mut bytes, mut nparts) = (first.bytes, first.parts.len());
        let mut jobs = vec![first];
        while bytes < limits.max_bytes && nparts < limits.max_parts {
            match rx.try_recv() {
                Ok(job) => {
                    if bytes + job.bytes > limits.max_bytes
                        || nparts + job.parts.len() > limits.max_parts
                    {
                        carried = Some(job);
                        break;
                    }
                    bytes += job.bytes;
                    nparts += job.parts.len();
                    jobs.push(job);
                }
                Err(_) => break,
            }
        }
        let client = client.clone();
        let addr = addr.clone();
        tokio::spawn(async move {
            send_jobs(&client, &addr, timeout, jobs).await;
            drop(permit);
        });
    }
}

async fn send_jobs(client: &reqwest::Client, addr: &str, timeout: Duration, jobs: Vec<Job>) {
    let counts: Vec<usize> = jobs.iter().map(|j| j.parts.len()).collect();
    let mut replies = Vec::with_capacity(jobs.len());
    let mut parts = Vec::with_capacity(counts.iter().sum());
    for job in jobs {
        parts.extend(job.parts);
        replies.push(job.reply);
    }
    histogram!("hyperbytedb_shard_forward_writes_per_request").record(replies.len() as f64);
    counter!("hyperbytedb_shard_forward_requests_total").increment(1);

    let outcome = post_batch(client, addr, timeout, parts).await;
    match outcome {
        NodeSendOutcome::Results(results) if results.len() == counts.iter().sum::<usize>() => {
            let mut results = results.into_iter();
            for (reply, n) in replies.into_iter().zip(counts) {
                let _ = reply.send(NodeSendOutcome::Results(results.by_ref().take(n).collect()));
            }
        }
        NodeSendOutcome::Results(results) => {
            let msg = format!(
                "forward batch to {addr}: {} results for {} parts",
                results.len(),
                counts.iter().sum::<usize>()
            );
            for reply in replies {
                let _ = reply.send(NodeSendOutcome::Failed(msg.clone()));
            }
        }
        other => {
            for reply in replies {
                let _ = reply.send(other.clone());
            }
        }
    }
}

/// Send `parts` to the node at `addr` in a request of their own: the path
/// when `sharding.forward_coalesce` is off.
pub async fn send_direct(
    client: &reqwest::Client,
    addr: &str,
    timeout: Duration,
    parts: Vec<ShardWriteRequest>,
) -> NodeSendOutcome {
    let n = parts.len();
    match post_batch(client, addr, timeout, parts).await {
        NodeSendOutcome::Results(results) if results.len() != n => {
            NodeSendOutcome::Failed(format!(
                "forward batch to {addr}: {} results for {n} parts",
                results.len()
            ))
        }
        other => other,
    }
}

async fn post_batch(
    client: &reqwest::Client,
    addr: &str,
    timeout: Duration,
    parts: Vec<ShardWriteRequest>,
) -> NodeSendOutcome {
    let body = match bincode::serialize(&ShardWriteBatch { parts }) {
        Ok(b) => b,
        Err(e) => return NodeSendOutcome::Failed(format!("encode forward batch: {e}")),
    };
    let start = Instant::now();
    let resp = crate::telemetry::inject(
        client
            .post(format!("http://{addr}/internal/shard/write-batch"))
            .header("Content-Type", "application/octet-stream"),
    )
    .body(body)
    .timeout(timeout)
    .send()
    .await;
    let outcome = match resp {
        Err(e) => NodeSendOutcome::Failed(crate::application::shard_routing::error_chain(&e)),
        Ok(r) => {
            let status = r.status();
            if status == reqwest::StatusCode::NOT_FOUND
                || status == reqwest::StatusCode::METHOD_NOT_ALLOWED
            {
                NodeSendOutcome::Unsupported
            } else if status == reqwest::StatusCode::SERVICE_UNAVAILABLE
                || status == reqwest::StatusCode::TOO_MANY_REQUESTS
            {
                NodeSendOutcome::Capacity
            } else if status.is_success() {
                match r.json::<ShardWriteBatchResponse>().await {
                    Ok(parsed) => NodeSendOutcome::Results(parsed.results),
                    Err(e) => NodeSendOutcome::Failed(format!("{status}: {e}")),
                }
            } else {
                NodeSendOutcome::Failed(status.to_string())
            }
        }
    };
    let label = match &outcome {
        NodeSendOutcome::Results(_) => "ok",
        NodeSendOutcome::Capacity => "capacity",
        _ => "error",
    };
    histogram!("hyperbytedb_shard_forward_request_seconds", "outcome" => label)
        .record(start.elapsed().as_secs_f64());
    outcome
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::domain::sharding::ShardEpoch;
    use std::sync::atomic::{AtomicUsize, Ordering};

    fn part(region_id: u64) -> ShardWriteRequest {
        ShardWriteRequest {
            db: "db".into(),
            rp: "autogen".into(),
            precision: None,
            epoch: ShardEpoch::default(),
            region_id,
            body: b"cpu v=1 1".to_vec(),
        }
    }

    /// A node that holds every request `delay` and answers each part 204, or
    /// the whole request 503 when `refuse`.
    async fn mock_node(delay: Duration, refuse: bool) -> (String, Arc<AtomicUsize>) {
        let requests = Arc::new(AtomicUsize::new(0));
        let seen = requests.clone();
        let app = axum::Router::new().route(
            "/internal/shard/write-batch",
            axum::routing::post(move |body: axum::body::Bytes| {
                let seen = seen.clone();
                async move {
                    use axum::response::IntoResponse;
                    seen.fetch_add(1, Ordering::SeqCst);
                    tokio::time::sleep(delay).await;
                    if refuse {
                        return axum::http::StatusCode::SERVICE_UNAVAILABLE.into_response();
                    }
                    let batch: ShardWriteBatch = bincode::deserialize(&body).unwrap();
                    axum::Json(ShardWriteBatchResponse {
                        results: batch
                            .parts
                            .iter()
                            .map(|p| ShardWritePartResult {
                                region_id: p.region_id,
                                status: 204,
                                error: None,
                            })
                            .collect(),
                    })
                    .into_response()
                }
            }),
        );
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap().to_string();
        tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });
        (addr, requests)
    }

    /// Concurrent writes to one busy node share requests, and each gets back
    /// exactly its own parts' results.
    #[tokio::test]
    async fn concurrent_writes_share_requests_and_get_their_own_results() {
        let (addr, requests) = mock_node(Duration::from_millis(100), false).await;
        let coalescer = Arc::new(ForwardCoalescer::new(2, 512 * 1024, 256));
        let client = reqwest::Client::new();
        let writes = (0..40u64).map(|w| {
            let coalescer = coalescer.clone();
            let client = client.clone();
            let addr = addr.clone();
            async move {
                let mine = vec![part(w * 10), part(w * 10 + 1)];
                let out = coalescer
                    .send(&client, &addr, Duration::from_secs(5), mine)
                    .await;
                (w, out)
            }
        });
        for (w, out) in futures::future::join_all(writes).await {
            let NodeSendOutcome::Results(results) = out else {
                panic!("write {w}: {out:?}");
            };
            let regions: Vec<u64> = results.iter().map(|r| r.region_id).collect();
            assert_eq!(
                regions,
                [w * 10, w * 10 + 1],
                "write {w} got another's results"
            );
        }
        let sent = requests.load(Ordering::SeqCst);
        assert!(
            sent <= 10,
            "40 concurrent writes to a busy node took {sent} requests"
        );
    }

    /// A refusal at capacity reaches every write in the request.
    #[tokio::test]
    async fn a_refused_request_is_capacity_for_every_write_in_it() {
        let (addr, _) = mock_node(Duration::from_millis(50), true).await;
        let coalescer = Arc::new(ForwardCoalescer::new(1, 512 * 1024, 256));
        let client = reqwest::Client::new();
        let writes = (0..8u64).map(|w| {
            let coalescer = coalescer.clone();
            let client = client.clone();
            let addr = addr.clone();
            async move {
                coalescer
                    .send(&client, &addr, Duration::from_secs(5), vec![part(w)])
                    .await
            }
        });
        for out in futures::future::join_all(writes).await {
            assert!(matches!(out, NodeSendOutcome::Capacity), "{out:?}");
        }
    }
}
