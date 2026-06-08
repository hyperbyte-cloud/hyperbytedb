//! Shared helpers for hyperbytedb-proxy Criterion benchmarks.

use std::net::IpAddr;
use std::sync::Arc;
use std::time::Duration;

use hyperbytedb_proxy::backend::{Backend, Health};
use hyperbytedb_proxy::config::ProxyConfig;
use hyperbytedb_proxy::pool::BackendPool;
use tokio::runtime::Runtime;

pub fn test_config() -> ProxyConfig {
    ProxyConfig {
        listen_addr: "127.0.0.1:0".into(),
        backend_service: "bench.local".into(),
        backend_port: 8086,
        discovery_interval: Duration::from_secs(5),
        health_interval: Duration::from_secs(2),
        health_path: "/health".into(),
        health_timeout: Duration::from_millis(1500),
        request_timeout: Duration::from_secs(60),
        hold_timeout: Duration::from_secs(10),
        max_retries: 2,
        shutdown_grace: Duration::from_secs(30),
        self_ip: None,
    }
}

fn bench_ip(n: usize) -> IpAddr {
    // 10.0.0.0/8 synthetic addresses — unique per bench backend index.
    let octet = (n % 254 + 1) as u8;
    let third = (n / 254) as u8;
    IpAddr::from([10, third, octet, 1])
}

pub async fn pool_with_active_backends(n: usize) -> Arc<BackendPool> {
    let pool = BackendPool::new(test_config()).expect("pool");
    let backends: Vec<Arc<Backend>> = (0..n)
        .map(|i| {
            let b = Arc::new(Backend::new(bench_ip(i), 8086));
            b.set_health(Health::Active);
            b
        })
        .collect();
    pool.set_backends_for_test(backends).await;
    pool
}

pub fn runtime() -> Runtime {
    Runtime::new().expect("runtime")
}
