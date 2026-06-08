//! Production-path e2e harness: [`build_services`], background flush, real HTTP.

use std::path::{Path, PathBuf};
use std::process::{Command, Stdio};
use std::sync::Arc;
use std::time::{Duration, Instant};

use hyperbytedb::adapters::http::router::build_router;
use hyperbytedb::bootstrap::build_services;
use hyperbytedb::config::HyperbytedbConfig;
use tokio::sync::watch;
use tokio::task::JoinHandle;

const FLUSH_INTERVAL_SECS: u64 = 1;

pub struct E2eFixture {
    pub config: HyperbytedbConfig,
    tmpdir: tempfile::TempDir,
    client: reqwest::Client,
}

pub struct E2eServer {
    url: String,
    config: HyperbytedbConfig,
    client: reqwest::Client,
    shutdown_tx: watch::Sender<bool>,
    http_shutdown_tx: Option<tokio::sync::oneshot::Sender<()>>,
    http_handle: JoinHandle<()>,
    flush_handle: JoinHandle<()>,
    _tmpdir: tempfile::TempDir,
}

impl E2eFixture {
    pub fn new() -> Self {
        let tmpdir = tempfile::tempdir().expect("tempdir");
        let root = tmpdir.path();

        let mut config = HyperbytedbConfig::load(None).expect("default config");
        config.server.bind_address = "127.0.0.1".into();
        config.server.port = 0;
        config.storage.wal_dir = root.join("wal").to_string_lossy().into_owned();
        config.storage.meta_dir = root.join("meta").to_string_lossy().into_owned();
        config.chdb.session_data_path = root.join("chdb").to_string_lossy().into_owned();
        config.flush.interval_secs = FLUSH_INTERVAL_SECS;
        config.flush.wal_batch_size = 0;
        config.retention.enabled = false;
        config.cluster.enabled = false;

        Self {
            config,
            tmpdir,
            client: reqwest::Client::new(),
        }
    }

    pub fn backup_dir(&self) -> PathBuf {
        self.tmpdir.path().join("backup")
    }

    /// Query restored data via a child `hyperbytedb serve` process.
    ///
    /// libchdb allows only one session per process, so post-restore reads must
    /// run in a subprocess after the in-process server has shut down.
    pub async fn query_via_subprocess(
        &self,
        config: &HyperbytedbConfig,
        db: &str,
        q: &str,
        min_rows: usize,
    ) -> serde_json::Value {
        ensure_hyperbytedb_bin();
        let port = free_port();
        let config_path = self.write_subprocess_config(config, port);
        let bin = hyperbytedb_bin();

        let mut child = Command::new(&bin)
            .args(["--config", config_path.to_str().expect("config path"), "serve"])
            .stdout(Stdio::null())
            .stderr(Stdio::piped())
            .spawn()
            .expect("spawn hyperbytedb");

        let base = format!("http://127.0.0.1:{port}");
        wait_for_http(&base, &mut child).await;

        let deadline = Instant::now() + Duration::from_secs(30);
        loop {
            let resp = self
                .client
                .get(format!("{base}/query"))
                .query(&[("db", db), ("q", q)])
                .send()
                .await
                .expect("subprocess query request");
            let status = resp.status();
            let body = resp.text().await.expect("subprocess query body");
            if status.is_success() {
                let parsed: serde_json::Value =
                    serde_json::from_str(&body).expect("subprocess query json");
                if query_row_count(&parsed) >= min_rows {
                    let _ = child.kill();
                    let _ = child.wait();
                    return parsed;
                }
            }
            if Instant::now() >= deadline {
                let _ = child.kill();
                let _ = child.wait();
                panic!("subprocess query timed out (status={status}): {body}");
            }
            tokio::time::sleep(Duration::from_millis(500)).await;
        }
    }

    fn write_subprocess_config(&self, config: &HyperbytedbConfig, port: u16) -> PathBuf {
        let path = self.tmpdir.path().join("subprocess-config.toml");
        let contents = format!(
            r#"[server]
bind_address = "127.0.0.1"
port = {port}

[storage]
wal_dir = "{wal}"
meta_dir = "{meta}"

[chdb]
session_data_path = "{chdb}"

[flush]
interval_secs = 1
wal_batch_size = 0

[retention]
enabled = false
"#,
            wal = config.storage.wal_dir,
            meta = config.storage.meta_dir,
            chdb = config.chdb.session_data_path,
        );
        std::fs::write(&path, contents).expect("write subprocess config");
        path
    }

    pub async fn start(self) -> E2eServer {
        let E2eFixture {
            config,
            tmpdir,
            client,
        } = self;

        let boot = build_services(&config).await.expect("build_services");

        let flush_interval = Duration::from_secs(FLUSH_INTERVAL_SECS);
        let (shutdown_tx, shutdown_rx) = watch::channel(false);
        let flush = boot.flush_service.clone();
        let app_state = Arc::new(boot.app_state);
        let app = build_router(app_state);

        let flush_handle = tokio::spawn(async move {
            flush.run(flush_interval, shutdown_rx).await;
        });

        let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
            .await
            .expect("bind");
        let addr = listener.local_addr().expect("local_addr");
        let url = format!("http://{addr}");
        let (http_shutdown_tx, http_shutdown_rx) = tokio::sync::oneshot::channel::<()>();
        let http_handle = tokio::spawn(async move {
            axum::serve(listener, app)
                .with_graceful_shutdown(async {
                    let _ = http_shutdown_rx.await;
                })
                .await
                .expect("serve");
        });

        E2eServer {
            url,
            config,
            client,
            shutdown_tx,
            http_shutdown_tx: Some(http_shutdown_tx),
            http_handle,
            flush_handle,
            _tmpdir: tmpdir,
        }
    }
}

impl E2eServer {
    pub fn config(&self) -> &HyperbytedbConfig {
        &self.config
    }

    pub async fn create_db(&self, name: &str) {
        let resp = self
            .client
            .get(format!("{}/query", self.url))
            .query(&[("q", format!("CREATE DATABASE {name}"))])
            .send()
            .await
            .expect("create_db request");
        assert!(
            resp.status().is_success(),
            "CREATE DATABASE failed: {}",
            resp.status()
        );
    }

    pub async fn write(&self, db: &str, body: &str) -> reqwest::Response {
        self.client
            .post(format!("{}/write", self.url))
            .query(&[("db", db)])
            .body(body.to_string())
            .send()
            .await
            .expect("write request")
    }

    pub async fn query(&self, db: &str, q: &str) -> serde_json::Value {
        let resp = self
            .client
            .get(format!("{}/query", self.url))
            .query(&[("db", db), ("q", q)])
            .send()
            .await
            .expect("query request");
        let status = resp.status();
        let body = resp.text().await.expect("query body");
        assert!(
            status.is_success(),
            "query failed: {status} body={body}"
        );
        serde_json::from_str(&body).expect("query json")
    }

    pub async fn wait_for_rows(
        &self,
        db: &str,
        q: &str,
        min_rows: usize,
        timeout: Duration,
    ) -> serde_json::Value {
        let deadline = Instant::now() + timeout;
        loop {
            let parsed = self.query(db, q).await;
            if query_row_count(&parsed) >= min_rows {
                return parsed;
            }
            if Instant::now() >= deadline {
                panic!("timed out waiting for {min_rows} rows from {q:?}: {parsed}");
            }
            tokio::time::sleep(Duration::from_millis(200)).await;
        }
    }

    pub async fn stop(self) -> E2eFixture {
        let E2eServer {
            config,
            client,
            shutdown_tx,
            http_shutdown_tx,
            http_handle,
            flush_handle,
            _tmpdir,
            ..
        } = self;

        let _ = shutdown_tx.send(true);
        let _ = flush_handle.await;
        if let Some(tx) = http_shutdown_tx {
            let _ = tx.send(());
        }
        let _ = http_handle.await;
        tokio::time::sleep(Duration::from_millis(500)).await;

        E2eFixture {
            config,
            tmpdir: _tmpdir,
            client,
        }
    }
}

pub fn query_row_count(parsed: &serde_json::Value) -> usize {
    parsed["results"]
        .as_array()
        .and_then(|results| results.first())
        .and_then(|r| r["series"].as_array())
        .map(|series| {
            series
                .iter()
                .filter_map(|s| s["values"].as_array())
                .map(|v| v.len())
                .sum::<usize>()
        })
        .unwrap_or(0)
}

pub fn files_under(dir: &Path) -> usize {
    walkdir_recursive(dir).len()
}

fn walkdir_recursive(dir: &Path) -> Vec<PathBuf> {
    let mut result = Vec::new();
    if !dir.exists() {
        return result;
    }
    for entry in std::fs::read_dir(dir).expect("read_dir") {
        let entry = entry.expect("dir entry");
        let path = entry.path();
        if path.is_dir() {
            result.extend(walkdir_recursive(&path));
        } else {
            result.push(path);
        }
    }
    result
}

fn free_port() -> u16 {
    std::net::TcpListener::bind("127.0.0.1:0")
        .expect("bind free port")
        .local_addr()
        .expect("local addr")
        .port()
}

async fn wait_for_http(base: &str, child: &mut std::process::Child) {
    let client = reqwest::Client::new();
    let deadline = Instant::now() + Duration::from_secs(15);
    loop {
        if let Ok(resp) = client.get(format!("{base}/ping")).send().await
            && resp.status().is_success()
        {
            return;
        }
        if let Some(status) = child.try_wait().expect("try_wait") {
            let stderr = read_child_stderr(child);
            panic!("subprocess server exited early with status {status:?}: {stderr}");
        }
        if Instant::now() >= deadline {
            let stderr = read_child_stderr(child);
            panic!("subprocess server did not become ready at {base}: {stderr}");
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
}

fn read_child_stderr(child: &mut std::process::Child) -> String {
    child
        .stderr
        .take()
        .map(|mut s| {
            use std::io::Read;
            let mut buf = String::new();
            s.read_to_string(&mut buf).ok();
            buf
        })
        .unwrap_or_default()
}

fn hyperbytedb_bin() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../target/debug/hyperbytedb")
}

fn ensure_hyperbytedb_bin() {
    let bin = hyperbytedb_bin();
    if bin.exists() {
        return;
    }
    let status = std::process::Command::new("cargo")
        .args(["build", "--bin", "hyperbytedb", "-q"])
        .current_dir(
            PathBuf::from(env!("CARGO_MANIFEST_DIR"))
                .join("..")
                .canonicalize()
                .unwrap_or_else(|_| PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("..")),
        )
        .status()
        .expect("cargo build hyperbytedb");
    assert!(
        status.success(),
        "failed to build hyperbytedb binary for subprocess e2e"
    );
    assert!(
        bin.exists(),
        "hyperbytedb binary missing at {}",
        bin.display()
    );
}
