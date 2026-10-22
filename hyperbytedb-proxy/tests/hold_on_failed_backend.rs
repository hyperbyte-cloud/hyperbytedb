//! A request whose backend has just gone down is held until a backend is
//! back, not failed with 503.

use std::sync::Arc;
use std::time::Duration;

use axum::Router;
use axum::http::StatusCode;
use axum::routing::{any, get};
use hyperbytedb_proxy::backend::{Backend, Health};
use hyperbytedb_proxy::config::ProxyConfig;
use hyperbytedb_proxy::pool::BackendPool;
use hyperbytedb_proxy::proxy::{ProxyState, handle};

fn config(port: u16) -> ProxyConfig {
    ProxyConfig {
        listen_addr: "127.0.0.1:0".into(),
        admin_listen_addr: "127.0.0.1:0".into(),
        backend_service: "127.0.0.1".into(),
        backend_port: port,
        discovery_interval: Duration::from_secs(3600),
        health_interval: Duration::from_millis(100),
        health_path: "/health".into(),
        health_timeout: Duration::from_millis(200),
        request_timeout: Duration::from_secs(5),
        hold_timeout: Duration::from_secs(5),
        max_retries: 3,
        shutdown_grace: Duration::from_secs(1),
        self_ip: None,
        http2_prior_knowledge: false,
    }
}

/// The backend went down a moment ago and its last probe still says Active
/// (probes run about every second): a pool of one, as for a single-node pool
/// in a rolling restart. The request used to fail at once with 503, every
/// retry handed the same dead backend; now the failed forward takes it out
/// of rotation and the request waits for it to come back.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_request_to_a_backend_that_just_went_down_is_held() {
    // A port with nothing listening yet.
    let port = {
        let l = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
        l.local_addr().unwrap().port()
    };
    let pool = BackendPool::new(config(port)).unwrap();
    let backend = Arc::new(Backend::new("127.0.0.1".parse().unwrap(), port));
    backend.set_health(Health::Active);
    pool.set_backends_for_test(vec![Arc::clone(&backend)]).await;

    let proxy = Router::new()
        .route("/write", any(handle))
        .with_state(ProxyState::new(Arc::clone(&pool)).unwrap());
    let proxy_listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let proxy_url = format!("http://{}", proxy_listener.local_addr().unwrap());
    tokio::spawn(async move { axum::serve(proxy_listener, proxy).await.unwrap() });

    let request = tokio::spawn(async move {
        reqwest::Client::new()
            .post(format!("{proxy_url}/write?db=d"))
            .body("m v=1")
            .send()
            .await
            .unwrap()
            .status()
    });

    // The backend comes back 500 ms later, and its probes start.
    tokio::time::sleep(Duration::from_millis(500)).await;
    assert!(
        !request.is_finished(),
        "held while the backend is down, not failed"
    );
    let app = Router::new()
        .route("/write", any(|| async { StatusCode::NO_CONTENT }))
        .route("/health", get(|| async { StatusCode::OK }));
    let listener = tokio::net::TcpListener::bind(("127.0.0.1", port))
        .await
        .unwrap();
    tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });
    tokio::spawn(Arc::clone(&pool).run_health());

    let status = tokio::time::timeout(Duration::from_secs(5), request)
        .await
        .expect("answered once the backend is back")
        .unwrap();
    assert_eq!(status, reqwest::StatusCode::NO_CONTENT);
}
