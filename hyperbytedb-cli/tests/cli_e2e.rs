//! End-to-end tests: in-process HyperbyteDB server + hyperbytedb-cli library/subprocess.
//!
//! libchdb allows one session per process; tests run serially.

use std::process::Command;
use std::sync::Arc;
use std::time::Duration;

use tokio::sync::Mutex;

static SERVER_LOCK: Mutex<()> = Mutex::const_new(());

use hyperbytedb::adapters::http::router::build_router;
use hyperbytedb::bootstrap::build_services;
use hyperbytedb::config::HyperbytedbConfig;
use hyperbytedb_cli::{
    ConnectionConfig, HyperbytedbClient, Session,
    client::{QueryOptions, WriteOptions},
    repl,
    session::OutputFormat,
};
use tokio::sync::watch;

struct TestServer {
    url: String,
    _tmpdir: tempfile::TempDir,
    shutdown_tx: watch::Sender<bool>,
    http_shutdown_tx: Option<tokio::sync::oneshot::Sender<()>>,
    http_handle: tokio::task::JoinHandle<()>,
    flush_handle: tokio::task::JoinHandle<()>,
}

async fn start_server() -> TestServer {
    let tmpdir = tempfile::tempdir().expect("tempdir");
    let root = tmpdir.path();

    let mut config = HyperbytedbConfig::load(None).expect("default config");
    config.server.bind_address = "127.0.0.1".into();
    config.server.port = 0;
    config.storage.wal_dir = root.join("wal").to_string_lossy().into_owned();
    config.storage.meta_dir = root.join("meta").to_string_lossy().into_owned();
    config.chdb.session_data_path = root.join("chdb").to_string_lossy().into_owned();
    config.flush.interval_secs = 1;
    config.flush.wal_batch_size = 0;
    config.retention.enabled = false;
    config.cluster.enabled = false;

    let boot = build_services(&config).await.expect("build_services");
    let flush = boot.flush_service.clone();
    let app = build_router(Arc::new(boot.app_state));

    let (shutdown_tx, shutdown_rx) = watch::channel(false);
    let flush_handle = tokio::spawn(async move {
        flush.run(Duration::from_secs(1), shutdown_rx).await;
    });

    let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
        .await
        .expect("bind");
    let addr = listener.local_addr().expect("addr");
    let url = format!("http://{addr}");

    let (http_shutdown_tx, http_shutdown_rx) = tokio::sync::oneshot::channel();
    let http_handle = tokio::spawn(async move {
        axum::serve(listener, app)
            .with_graceful_shutdown(async {
                let _ = http_shutdown_rx.await;
            })
            .await
            .expect("serve");
    });

    TestServer {
        url,
        _tmpdir: tmpdir,
        shutdown_tx,
        http_shutdown_tx: Some(http_shutdown_tx),
        http_handle,
        flush_handle,
    }
}

impl TestServer {
    async fn stop(self) {
        let _ = self.shutdown_tx.send(true);
        if let Some(tx) = self.http_shutdown_tx {
            let _ = tx.send(());
        }
        let _ = self.http_handle.await;
        let _ = self.flush_handle.await;
    }
}

fn client_config(url: &str) -> ConnectionConfig {
    ConnectionConfig {
        host: url.to_string(),
        database: None,
        username: None,
        password: None,
        ssl: false,
        unsafe_ssl: false,
    }
}

async fn with_server<F, Fut>(f: F)
where
    F: FnOnce(TestServer) -> Fut,
    Fut: std::future::Future<Output = ()>,
{
    let _guard = SERVER_LOCK.lock().await;
    let server = start_server().await;
    f(server).await;
}

#[tokio::test]
async fn execute_show_databases() {
    with_server(|server| async move {
        let conn = client_config(&server.url);
        let client = HyperbytedbClient::new(&conn).expect("client");

        client
            .query(
                "CREATE DATABASE testdb",
                &QueryOptions {
                    db: None,
                    epoch: None,
                    pretty: false,
                    chunked: false,
                    format: OutputFormat::Json,
                    params: None,
                },
            )
            .await
            .expect("create db");

        let session = Session::new(conn);
        repl::execute_query(&session, &client, "SHOW DATABASES")
            .await
            .expect("show databases");

        server.stop().await;
    })
    .await;
}

#[tokio::test]
async fn write_and_query_roundtrip() {
    with_server(|server| async move {
        let conn = client_config(&server.url);
        let client = HyperbytedbClient::new(&conn).expect("client");

        client
            .query(
                "CREATE DATABASE metrics",
                &QueryOptions {
                    db: None,
                    epoch: None,
                    pretty: false,
                    chunked: false,
                    format: OutputFormat::Json,
                    params: None,
                },
            )
            .await
            .expect("create db");

        client
            .write(
                b"cpu,host=a usage=42",
                &WriteOptions {
                    db: "metrics".to_string(),
                    rp: None,
                    precision: None,
                    gzip: false,
                },
            )
            .await
            .expect("write");

        let deadline = std::time::Instant::now() + Duration::from_secs(30);
        loop {
            let resp = client
                .query(
                    "SHOW MEASUREMENTS",
                    &QueryOptions {
                        db: Some("metrics".to_string()),
                        epoch: None,
                        pretty: false,
                        chunked: false,
                        format: OutputFormat::Json,
                        params: None,
                    },
                )
                .await
                .expect("show measurements");
            let has_cpu = resp.results.iter().any(|r| {
                r.series.as_ref().is_some_and(|s| {
                    s.iter().any(|ser| {
                        ser.values.iter().any(|row| {
                            row.first()
                                .and_then(|v| v.as_str())
                                .is_some_and(|n| n == "cpu")
                        })
                    })
                })
            });
            if has_cpu {
                break;
            }
            if std::time::Instant::now() >= deadline {
                panic!("timed out waiting for measurement cpu");
            }
            tokio::time::sleep(Duration::from_millis(300)).await;
        }

        server.stop().await;
    })
    .await;
}

#[tokio::test]
async fn ping_returns_version_header() {
    with_server(|server| async move {
        let client = HyperbytedbClient::new(&client_config(&server.url)).expect("client");
        let ping = client.ping().await.expect("ping");
        assert!(ping.version.is_some());
        server.stop().await;
    })
    .await;
}

#[tokio::test]
async fn materialized_view_via_execute() {
    with_server(|server| async move {
        let conn = client_config(&server.url);
        let client = HyperbytedbClient::new(&conn).expect("client");

        client
            .query(
                "CREATE DATABASE mvcli",
                &QueryOptions {
                    db: None,
                    epoch: None,
                    pretty: false,
                    chunked: false,
                    format: OutputFormat::Json,
                    params: None,
                },
            )
            .await
            .expect("create db");

        client
            .write(
                b"cpu,host=a value=10 1700000000000000000\ncpu,host=a value=20 1700000060000000000",
                &WriteOptions {
                    db: "mvcli".to_string(),
                    rp: None,
                    precision: None,
                    gzip: false,
                },
            )
            .await
            .expect("write");

        let deadline = std::time::Instant::now() + Duration::from_secs(30);
        loop {
            let resp = client
                .query(
                    "SHOW MEASUREMENTS",
                    &QueryOptions {
                        db: Some("mvcli".to_string()),
                        epoch: None,
                        pretty: false,
                        chunked: false,
                        format: OutputFormat::Json,
                        params: None,
                    },
                )
                .await
                .expect("show measurements");
            if resp.results.iter().any(|r| {
                r.series.as_ref().is_some_and(|s| {
                    s.iter().any(|ser| {
                        ser.values.iter().any(|row| {
                            row.first()
                                .and_then(|v| v.as_str())
                                .is_some_and(|n| n == "cpu")
                        })
                    })
                })
            }) {
                break;
            }
            if std::time::Instant::now() >= deadline {
                panic!("timed out waiting for cpu measurement");
            }
            tokio::time::sleep(Duration::from_millis(300)).await;
        }

        let session = Session::new(conn);
        repl::execute_query(
            &session,
            &client,
            r#"CREATE MATERIALIZED VIEW "mv_5m" ON "mvcli" AS SELECT mean("value") INTO "cpu_5m" FROM "cpu" GROUP BY time(5m), *"#,
        )
        .await
        .expect("create mv");

        repl::execute_query(&session, &client, "SHOW MATERIALIZED VIEWS")
            .await
            .expect("show mvs");

        let err = repl::execute_query(
            &session,
            &client,
            r#"CREATE MATERIALIZED VIEW "mv_5m" ON "mvcli" AS SELECT mean("value") INTO "cpu_5m" FROM "cpu" GROUP BY time(5m), *"#,
        )
        .await
        .expect_err("duplicate mv should fail");
        let err_msg = err.to_string();
        assert!(
            !err_msg.contains(r#"{"error""#),
            "error should be parsed, got: {err_msg}"
        );
        assert!(
            err_msg.contains("already exists") || err_msg.contains("query error"),
            "expected readable duplicate MV error, got: {err_msg}"
        );

        server.stop().await;
    })
    .await;
}

#[tokio::test]
async fn subprocess_execute_show_databases() {
    with_server(|server| async move {
        let bin = env!("CARGO_BIN_EXE_hyperbytedb-cli");

        let output = Command::new(bin)
            .args([
                "-host",
                &server.url,
                "-execute",
                "SHOW DATABASES",
                "-format",
                "column",
            ])
            .output()
            .expect("spawn cli");

        assert!(
            output.status.success(),
            "stderr={}",
            String::from_utf8_lossy(&output.stderr)
        );

        server.stop().await;
    })
    .await;
}

#[tokio::test]
async fn subprocess_write_data_binary_and_query() {
    with_server(|server| async move {
        let bin = env!("CARGO_BIN_EXE_hyperbytedb-cli");

        let create = Command::new(bin)
            .args(["-host", &server.url, "create", "database", "mydb"])
            .output()
            .expect("spawn cli");
        assert!(
            create.status.success(),
            "create database failed: stderr={}",
            String::from_utf8_lossy(&create.stderr)
        );

        let write = Command::new(bin)
            .args([
                "-host",
                &server.url,
                "write",
                "-database",
                "mydb",
                "--data-binary",
                "cpu,host=srv01 value=42",
            ])
            .output()
            .expect("spawn cli");
        assert!(
            write.status.success(),
            "write --data-binary failed: stderr={}",
            String::from_utf8_lossy(&write.stderr)
        );

        let deadline = std::time::Instant::now() + Duration::from_secs(30);
        let query_out = loop {
            let query = Command::new(bin)
                .args([
                    "-host",
                    &server.url,
                    "query",
                    "-database",
                    "mydb",
                    "--data-urlencode",
                    "q=SHOW MEASUREMENTS",
                ])
                .output()
                .expect("spawn cli");
            assert!(
                query.status.success(),
                "query --data-urlencode failed: stderr={}",
                String::from_utf8_lossy(&query.stderr)
            );
            let stdout = String::from_utf8_lossy(&query.stdout).into_owned();
            if stdout.contains("cpu") {
                break stdout;
            }
            if std::time::Instant::now() >= deadline {
                panic!("timed out waiting for measurement cpu; stdout={stdout}");
            }
            tokio::time::sleep(Duration::from_millis(300)).await;
        };

        assert!(query_out.contains("cpu"), "stdout={query_out}");

        server.stop().await;
    })
    .await;
}

#[tokio::test]
async fn subprocess_create_database() {
    with_server(|server| async move {
        let bin = env!("CARGO_BIN_EXE_hyperbytedb-cli");

        let create = Command::new(bin)
            .args(["-host", &server.url, "create", "database", "mydb"])
            .output()
            .expect("spawn cli");

        assert!(
            create.status.success(),
            "create database failed: stderr={}",
            String::from_utf8_lossy(&create.stderr)
        );

        let show = Command::new(bin)
            .args([
                "-host",
                &server.url,
                "-execute",
                "SHOW DATABASES",
                "-format",
                "column",
            ])
            .output()
            .expect("spawn cli");

        assert!(
            show.status.success(),
            "show databases failed: stderr={}",
            String::from_utf8_lossy(&show.stderr)
        );
        assert!(
            String::from_utf8_lossy(&show.stdout).contains("mydb"),
            "stdout={}",
            String::from_utf8_lossy(&show.stdout)
        );

        server.stop().await;
    })
    .await;
}
