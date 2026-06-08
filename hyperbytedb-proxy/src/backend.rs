//! Backend = one hyperbytedb pod we know about. Health-state and in-flight
//! counters are atomic so the routing hot path is lock-free.

use std::net::IpAddr;
use std::sync::atomic::{AtomicI64, AtomicU8, AtomicUsize, Ordering};
use std::time::SystemTime;

/// Health classification observed by the most recent probe.
///
/// Mapped from the hyperbytedb `/health` response:
///
/// | HTTP status | body `status` | mapped to        |
/// |-------------|---------------|------------------|
/// | 200         | `pass`        | [`Health::Active`]   |
/// | 503         | `warn`/`drain`| [`Health::Draining`] |
/// | error/timeout                | [`Health::Down`]     |
///
/// `Unknown` is the bootstrap value used after discovery but before the first
/// probe completes. Routing treats `Unknown` as not-routable, so we never
/// blindly forward to a brand-new IP.
#[repr(u8)]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Health {
    Unknown = 0,
    Active = 1,
    Draining = 2,
    Down = 3,
}

impl Health {
    fn from_u8(v: u8) -> Self {
        match v {
            1 => Health::Active,
            2 => Health::Draining,
            3 => Health::Down,
            _ => Health::Unknown,
        }
    }

    pub fn as_str(self) -> &'static str {
        match self {
            Health::Unknown => "unknown",
            Health::Active => "active",
            Health::Draining => "draining",
            Health::Down => "down",
        }
    }
}

/// One backend pod. Cheap to clone via `Arc<Backend>` from the pool.
#[derive(Debug)]
pub struct Backend {
    pub addr: IpAddr,
    pub port: u16,
    /// Cached `http://addr:port` string used to build per-request URLs without
    /// reformatting on every dispatch.
    pub origin: String,

    health: AtomicU8,
    inflight: AtomicUsize,
    last_probe_unix: AtomicI64,
    /// Consecutive probe failures. Used to log transitions cleanly and (in
    /// future) to back-off probes for backends that are deeply broken.
    consecutive_failures: AtomicUsize,
}

impl Backend {
    pub fn new(addr: IpAddr, port: u16) -> Self {
        let origin = format!("http://{addr}:{port}");
        Self {
            addr,
            port,
            origin,
            health: AtomicU8::new(Health::Unknown as u8),
            inflight: AtomicUsize::new(0),
            last_probe_unix: AtomicI64::new(0),
            consecutive_failures: AtomicUsize::new(0),
        }
    }

    pub fn health(&self) -> Health {
        Health::from_u8(self.health.load(Ordering::Acquire))
    }

    /// Returns the previous health if it changed, so callers can log
    /// transitions exactly once.
    pub fn set_health(&self, new: Health) -> Option<Health> {
        let prev = Health::from_u8(self.health.swap(new as u8, Ordering::AcqRel));
        if prev != new { Some(prev) } else { None }
    }

    pub fn record_probe_outcome(&self, ok: bool) {
        self.last_probe_unix.store(unix_now(), Ordering::Relaxed);
        if ok {
            self.consecutive_failures.store(0, Ordering::Relaxed);
        } else {
            self.consecutive_failures.fetch_add(1, Ordering::Relaxed);
        }
    }

    pub fn consecutive_failures(&self) -> usize {
        self.consecutive_failures.load(Ordering::Relaxed)
    }

    pub fn last_probe_unix(&self) -> i64 {
        self.last_probe_unix.load(Ordering::Relaxed)
    }

    pub fn inflight(&self) -> usize {
        self.inflight.load(Ordering::Relaxed)
    }

    /// RAII guard that increments `inflight` on construction and decrements
    /// on drop, even on panic. Use for the lifetime of one proxied request.
    pub fn enter(self: &std::sync::Arc<Self>) -> InflightGuard {
        self.inflight.fetch_add(1, Ordering::AcqRel);
        InflightGuard {
            backend: std::sync::Arc::clone(self),
        }
    }
}

pub struct InflightGuard {
    backend: std::sync::Arc<Backend>,
}

impl Drop for InflightGuard {
    fn drop(&mut self) {
        self.backend.inflight.fetch_sub(1, Ordering::AcqRel);
    }
}

fn unix_now() -> i64 {
    SystemTime::now()
        .duration_since(SystemTime::UNIX_EPOCH)
        .map(|d| d.as_secs() as i64)
        .unwrap_or(0)
}
