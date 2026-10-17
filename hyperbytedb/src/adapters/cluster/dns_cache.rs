//! Caching DNS resolver for node-to-node HTTP clients.
//!
//! Every new pooled connection resolves the peer's name again. Under write
//! load the peer client opens hundreds of connections, and on a real cluster
//! the resulting lookup flood made cluster DNS fail: forwards to healthy
//! primaries died with `dns error` in milliseconds, and those writes were
//! reported as failed. Peer names (StatefulSet pod DNS) change rarely, so a
//! short cache removes the flood, and serving the last good answer when a
//! refresh fails keeps a DNS blip from failing writes.

use std::collections::HashMap;
use std::net::SocketAddr;
use std::sync::{Arc, Mutex, OnceLock};
use std::time::{Duration, Instant};

use reqwest::dns::{Addrs, Name, Resolve, Resolving};

/// How long a successful lookup is reused before it is refreshed.
const TTL: Duration = Duration::from_secs(30);

#[derive(Default)]
pub struct CachingResolver {
    entries: Mutex<HashMap<String, (Vec<SocketAddr>, Instant)>>,
}

impl CachingResolver {
    /// The process-wide resolver, so every peer client shares one cache.
    pub fn shared() -> Arc<Self> {
        static SHARED: OnceLock<Arc<CachingResolver>> = OnceLock::new();
        SHARED.get_or_init(|| Arc::new(Self::default())).clone()
    }

    fn cached(&self, host: &str) -> Option<(Vec<SocketAddr>, bool)> {
        let entries = self.entries.lock().unwrap_or_else(|e| e.into_inner());
        entries
            .get(host)
            .map(|(addrs, at)| (addrs.clone(), at.elapsed() < TTL))
    }

    fn store(&self, host: &str, addrs: Vec<SocketAddr>) {
        let mut entries = self.entries.lock().unwrap_or_else(|e| e.into_inner());
        entries.insert(host.to_string(), (addrs, Instant::now()));
    }

    /// Resolve `host` with `lookup`, using and filling the cache.
    ///
    /// Fresh entry: returned without a lookup. Stale or missing: looked up;
    /// on failure a stale entry is served rather than the error.
    async fn resolve_with<F, Fut>(
        &self,
        host: &str,
        lookup: F,
    ) -> Result<Vec<SocketAddr>, Box<dyn std::error::Error + Send + Sync>>
    where
        F: FnOnce(String) -> Fut,
        Fut: std::future::Future<Output = std::io::Result<Vec<SocketAddr>>>,
    {
        let cached = self.cached(host);
        if let Some((addrs, true)) = &cached {
            return Ok(addrs.clone());
        }
        match lookup(host.to_string()).await {
            Ok(addrs) if !addrs.is_empty() => {
                self.store(host, addrs.clone());
                Ok(addrs)
            }
            Ok(_) | Err(_) if cached.is_some() => {
                metrics::counter!("hyperbytedb_peer_dns_stale_served_total").increment(1);
                Ok(cached.map(|(a, _)| a).unwrap_or_default())
            }
            Ok(_) => Err(format!("no addresses for {host}").into()),
            Err(e) => Err(Box::new(e)),
        }
    }
}

async fn system_lookup(host: String) -> std::io::Result<Vec<SocketAddr>> {
    // Port 0: reqwest substitutes the URL's port.
    Ok(tokio::net::lookup_host((host.as_str(), 0)).await?.collect())
}

impl Resolve for CachingResolver {
    fn resolve(&self, name: Name) -> Resolving {
        let this = Self::shared();
        let host = name.as_str().to_string();
        Box::pin(async move {
            let addrs = this.resolve_with(&host, system_lookup).await?;
            let addrs: Addrs = Box::new(addrs.into_iter());
            Ok(addrs)
        })
    }
}

#[cfg(test)]
mod tests {
    use super::CachingResolver;
    use std::net::SocketAddr;
    use std::sync::atomic::{AtomicUsize, Ordering};

    fn addr(s: &str) -> SocketAddr {
        s.parse().unwrap()
    }

    #[tokio::test]
    async fn a_fresh_entry_is_reused_without_a_lookup() {
        let r = CachingResolver::default();
        let calls = AtomicUsize::new(0);
        for _ in 0..3 {
            let got = r
                .resolve_with("peer", |_| async {
                    calls.fetch_add(1, Ordering::SeqCst);
                    Ok(vec![addr("10.0.0.1:0")])
                })
                .await
                .unwrap();
            assert_eq!(got, vec![addr("10.0.0.1:0")]);
        }
        assert_eq!(
            calls.load(Ordering::SeqCst),
            1,
            "one lookup, then cache hits"
        );
    }

    #[tokio::test]
    async fn a_failed_refresh_serves_the_last_good_answer() {
        let r = CachingResolver::default();
        r.store("peer", vec![addr("10.0.0.1:0")]);
        // Age it past the TTL.
        {
            let mut e = r.entries.lock().unwrap();
            e.get_mut("peer").unwrap().1 -= super::TTL * 2;
        }
        let got = r
            .resolve_with("peer", |_| async {
                Err(std::io::Error::other("dns error"))
            })
            .await
            .unwrap();
        assert_eq!(got, vec![addr("10.0.0.1:0")]);
    }

    #[tokio::test]
    async fn an_unknown_name_that_fails_is_an_error() {
        let r = CachingResolver::default();
        let got = r
            .resolve_with("nobody", |_| async {
                Err(std::io::Error::other("dns error"))
            })
            .await;
        assert!(got.is_err());
    }
}
