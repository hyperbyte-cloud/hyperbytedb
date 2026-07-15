//! `BackendPool` — owns the live set of backends, the discovery loop, and the
//! health-probe loop.
//!
//! The hot path (`pick`) is lock-free: it grabs an `arc_swap`-style snapshot
//! by reading an `RwLock<Arc<Vec<Arc<Backend>>>>` once, drops the lock
//! immediately, and walks the snapshot. Discovery and health probing are
//! background tasks that mutate the pool atomically (publish-via-replace).

use std::collections::{HashMap, HashSet};
use std::net::IpAddr;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::{Duration, Instant};

use anyhow::{Context, Result};
use serde::Serialize;
use tokio::sync::{Notify, RwLock};
use tokio::time::{MissedTickBehavior, interval, timeout};

use crate::backend::{Backend, Health};
use crate::config::ProxyConfig;

/// JSON-serializable backend status for the admin pool endpoint.
#[derive(Serialize)]
pub struct BackendStatus {
    pub addr: String,
    pub port: u16,
    pub health: String,
    pub excluded: bool,
    pub inflight: usize,
    pub consecutive_failures: usize,
    pub last_probe_unix: i64,
}

pub struct BackendPool {
    cfg: ProxyConfig,

    /// Current snapshot. Replaced wholesale when discovery sees a new set of
    /// IPs; mutated in place (atomically per-backend) when health changes.
    backends: RwLock<Arc<Vec<Arc<Backend>>>>,

    /// Operator-driven exclusion set. IPs in this set are never routed to
    /// even when their health is `Active`. The operator populates this via
    /// `POST /admin/backends/{ip}/exclude` before killing a pod during
    /// rolling upgrades, and clears it via `POST /admin/backends/{ip}/include`
    /// once the replacement pod is healthy.
    excluded: RwLock<HashSet<IpAddr>>,

    /// Round-robin cursor. Wraps via modulo at pick-time.
    cursor: AtomicUsize,

    /// HTTP client used by health probes. Separate from the request-forwarding
    /// client so probe storms cannot exhaust the request connection pool.
    probe_client: reqwest::Client,

    /// Signalled whenever ANY backend transitions to `Active`. The forwarder
    /// uses this to wake from `hold_timeout` early when a new backend arrives.
    pub on_active: Notify,
}

impl BackendPool {
    pub fn new(cfg: ProxyConfig) -> Result<Arc<Self>> {
        let probe_client = reqwest::Client::builder()
            .timeout(cfg.health_timeout)
            .pool_max_idle_per_host(2)
            .build()
            .context("building probe http client")?;

        Ok(Arc::new(Self {
            cfg,
            backends: RwLock::new(Arc::new(Vec::new())),
            excluded: RwLock::new(HashSet::new()),
            cursor: AtomicUsize::new(0),
            probe_client,
            on_active: Notify::new(),
        }))
    }

    pub fn config(&self) -> &ProxyConfig {
        &self.cfg
    }

    /// Replace the backend snapshot (tests and benchmarks only).
    #[doc(hidden)]
    pub async fn set_backends_for_test(&self, backends: Vec<Arc<Backend>>) {
        let mut guard = self.backends.write().await;
        *guard = Arc::new(backends);
    }

    /// Cheap snapshot. The returned `Arc<Vec<...>>` is stable for the
    /// caller's lifetime; pool mutations create a new `Arc`.
    pub async fn snapshot(&self) -> Arc<Vec<Arc<Backend>>> {
        Arc::clone(&*self.backends.read().await)
    }

    /// Operator-driven exclusion: mark a backend IP so `pick_active` never
    /// routes to it. Returns `true` if newly excluded, or `Err` if the IP
    /// is not in the pool.
    pub async fn exclude_backend(&self, ip: IpAddr) -> anyhow::Result<bool> {
        let snap = self.snapshot().await;
        let backend = snap
            .iter()
            .find(|b| b.addr == ip)
            .ok_or_else(|| anyhow::anyhow!("backend {ip} not found in pool"))?;
        backend.set_excluded(true);
        let mut guard = self.excluded.write().await;
        Ok(guard.insert(ip))
    }

    /// Operator-driven inclusion: clear the exclusion flag so `pick_active`
    /// may route to this backend again. Returns `true` if it was previously
    /// excluded.
    pub async fn include_backend(&self, ip: IpAddr) -> bool {
        let snap = self.snapshot().await;
        if let Some(backend) = snap.iter().find(|b| b.addr == ip) {
            backend.set_excluded(false);
        }
        let mut guard = self.excluded.write().await;
        guard.remove(&ip)
    }

    /// Returns true if the given IP is currently excluded.
    pub async fn is_excluded(&self, ip: &IpAddr) -> bool {
        self.excluded.read().await.contains(ip)
    }

    /// JSON-serializable snapshot of the pool for `GET /admin/pool`.
    pub async fn pool_status(&self) -> Vec<BackendStatus> {
        let snap = self.snapshot().await;
        let excluded = self.excluded.read().await;
        snap.iter()
            .map(|b| BackendStatus {
                addr: b.addr.to_string(),
                port: b.port,
                health: b.health().as_str().to_string(),
                excluded: excluded.contains(&b.addr),
                inflight: b.inflight(),
                consecutive_failures: b.consecutive_failures(),
                last_probe_unix: b.last_probe_unix(),
            })
            .collect()
    }

    /// Round-robin pick over backends in `Active` state that are not
    /// excluded. Returns `None` if the pool currently has zero routable
    /// backends — the caller decides whether to hold-and-retry or fail fast.
    pub async fn pick_active(&self) -> Option<Arc<Backend>> {
        let snap = self.snapshot().await;
        if snap.is_empty() {
            return None;
        }
        // Filter by health and exclusion into a tight Vec; small (cluster size <= dozens).
        let active: Vec<&Arc<Backend>> = snap
            .iter()
            .filter(|b| b.health() == Health::Active && !b.is_excluded())
            .collect();
        if active.is_empty() {
            return None;
        }
        let idx = self.cursor.fetch_add(1, Ordering::Relaxed) % active.len();
        Some(Arc::clone(active[idx]))
    }

    /// Pick an active backend that isn't `exclude` and isn't excluded by the
    /// operator. Used by the retry loop so we don't re-try the same broken
    /// backend twice in a row.
    pub async fn pick_active_excluding(&self, exclude: &Arc<Backend>) -> Option<Arc<Backend>> {
        let snap = self.snapshot().await;
        let active: Vec<&Arc<Backend>> = snap
            .iter()
            .filter(|b| {
                b.health() == Health::Active && !Arc::ptr_eq(b, exclude) && !b.is_excluded()
            })
            .collect();
        if active.is_empty() {
            return None;
        }
        let idx = self.cursor.fetch_add(1, Ordering::Relaxed) % active.len();
        Some(Arc::clone(active[idx]))
    }

    /// Block (async) until at least one backend is `Active` or `deadline`
    /// elapses. Returns whichever comes first.
    pub async fn wait_for_active(&self, max_wait: Duration) -> Option<Arc<Backend>> {
        let start = Instant::now();
        loop {
            if let Some(b) = self.pick_active().await {
                return Some(b);
            }
            let remaining = match max_wait.checked_sub(start.elapsed()) {
                Some(r) if !r.is_zero() => r,
                _ => return None,
            };
            // Poll-with-notify: woken either by `on_active` or by a 100ms
            // safety tick. The safety tick guards against any
            // discovery/probe race that would otherwise leave us asleep.
            let _ = timeout(
                remaining.min(Duration::from_millis(100)),
                self.on_active.notified(),
            )
            .await;
        }
    }

    /// Background loop: re-resolve `cfg.backend_service` and reconcile the
    /// pool. Adds new backends, removes vanished ones, leaves health state
    /// alone for IPs we already know about.
    pub async fn run_discovery(self: Arc<Self>) {
        let mut tick = interval(self.cfg.discovery_interval);
        tick.set_missed_tick_behavior(MissedTickBehavior::Delay);

        loop {
            tick.tick().await;
            match self.resolve().await {
                Ok(ips) => self.reconcile(ips).await,
                Err(e) => {
                    tracing::warn!(error = %e, service = %self.cfg.backend_service, "backend dns resolve failed")
                }
            }
        }
    }

    /// Background loop: probe every known backend on a schedule.
    pub async fn run_health(self: Arc<Self>) {
        let mut tick = interval(self.cfg.health_interval);
        tick.set_missed_tick_behavior(MissedTickBehavior::Delay);

        loop {
            tick.tick().await;
            let snap = self.snapshot().await;
            let pool = Arc::clone(&self);
            // Probe all backends concurrently so a slow one can't starve the
            // others. We don't await joinset completion explicitly — each
            // probe self-contains its work.
            for backend in snap.iter() {
                let pool = Arc::clone(&pool);
                let backend = Arc::clone(backend);
                tokio::spawn(async move {
                    pool.probe_one(&backend).await;
                });
            }
        }
    }

    async fn resolve(&self) -> Result<Vec<IpAddr>> {
        let host_port = format!("{}:{}", self.cfg.backend_service, self.cfg.backend_port);
        let addrs = tokio::net::lookup_host(&host_port)
            .await
            .with_context(|| format!("lookup_host({host_port})"))?;
        // Dedupe by IP — we only ever dial one port.
        let mut seen = std::collections::BTreeSet::new();
        for sa in addrs {
            let ip = sa.ip();
            // Defense-in-depth: never proxy to ourselves. If the headless
            // Service selector accidentally matched proxy pods we'd
            // otherwise infinitely recurse and OOM.
            if Some(ip) == self.cfg.self_ip {
                tracing::debug!(self_ip = %ip, "skipping self IP in backend pool");
                continue;
            }
            seen.insert(ip);
        }
        Ok(seen.into_iter().collect())
    }

    async fn reconcile(&self, fresh_ips: Vec<IpAddr>) {
        let cur = self.snapshot().await;

        // Build map IP → existing backend for O(N) reconciliation.
        let mut existing: HashMap<IpAddr, Arc<Backend>> =
            cur.iter().map(|b| (b.addr, Arc::clone(b))).collect();

        let mut next: Vec<Arc<Backend>> = Vec::with_capacity(fresh_ips.len());
        let mut added = 0usize;
        for ip in &fresh_ips {
            if let Some(b) = existing.remove(ip) {
                next.push(b);
            } else {
                added += 1;
                next.push(Arc::new(Backend::new(*ip, self.cfg.backend_port)));
            }
        }
        let removed = existing.len();

        if added > 0 || removed > 0 {
            tracing::info!(
                added,
                removed,
                total = next.len(),
                "backend pool reconciled"
            );
            metrics::gauge!("hyperbytedb_proxy_backends_total").set(next.len() as f64);
        }

        // Atomic publish.
        let new_snap = Arc::new(next);
        {
            let mut guard = self.backends.write().await;
            *guard = new_snap;
        }

        // Garbage-collect exclusion entries for IPs that are no longer in the pool.
        if removed > 0 {
            let mut excl = self.excluded.write().await;
            let fresh_set: HashSet<IpAddr> = fresh_ips.into_iter().collect();
            excl.retain(|ip| fresh_set.contains(ip));
        }
    }

    async fn probe_one(&self, backend: &Arc<Backend>) {
        let url = format!("{}{}", backend.origin, self.cfg.health_path);
        let result = self.probe_client.get(&url).send().await;

        let new_health = match result {
            Ok(resp) => {
                let status = resp.status();
                if status.is_success() {
                    Health::Active
                } else if status == http::StatusCode::SERVICE_UNAVAILABLE {
                    // hyperbytedb returns 503 with a body like
                    // {"status":"warn","message":"node is Draining, ..."}
                    // We don't bother parsing — any 503 means "don't route".
                    Health::Draining
                } else {
                    // 4xx/5xx that isn't 503 = misconfigured proxy/auth: still
                    // treat as Down to be safe.
                    Health::Down
                }
            }
            Err(_) => Health::Down,
        };

        backend.record_probe_outcome(matches!(new_health, Health::Active | Health::Draining));

        if let Some(prev) = backend.set_health(new_health) {
            tracing::info!(
                backend = %backend.addr,
                from = prev.as_str(),
                to = new_health.as_str(),
                "backend health changed"
            );
            metrics::counter!(
                "hyperbytedb_proxy_health_transitions_total",
                "from" => prev.as_str(),
                "to" => new_health.as_str()
            )
            .increment(1);

            if new_health == Health::Active {
                // Wake any forwarder waiting in `wait_for_active`.
                self.on_active.notify_waiters();
            }
        }
    }
}
