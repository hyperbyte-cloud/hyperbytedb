//! End-to-end tests: in-process HyperbyteDB server + hyperbytedb-cli library/subprocess.
//!
//! libchdb allows one session per process; tests run serially via `#[serial(chdb)]`.
//! Subprocess CLI invocations must not block the Tokio runtime (that would freeze the
//! in-process HTTP server they talk to).

use std::process::{Command, Output};
use std::sync::Arc;
use std::time::Duration;

use hyperbytedb::application::flush_service::FlushServiceImpl;
use serial_test::serial;
use tokio::sync::Mutex;

static SERVER_LOCK: Mutex<()> = Mutex::const_new(());

async fn run_cli(args: &[&str]) -> Output {
    let bin = env!("CARGO_BIN_EXE_hyperbytedb-cli").to_string();
    let args: Vec<String> = args.iter().map(|a| (*a).to_string()).collect();
    tokio::task::spawn_blocking(move || Command::new(bin).args(args).output())
        .await
        .expect("spawn_blocking cli")
        .expect("spawn cli")
}

fn assert_cli_success(output: &Output, label: &str) {
    let stdout = String::from_utf8_lossy(&output.stdout);
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(
        output.status.success(),
        "{label} failed: stderr={stderr} stdout={stdout}"
    );
    assert!(
        !stdout.contains("Usage: hyperbytedb-cli"),
        "{label} printed help instead of executing: stdout={stdout}"
    );
}

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
    flush_service: Arc<FlushServiceImpl>,
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
    let flush_for_task = flush.clone();
    let flush_handle = tokio::spawn(async move {
        flush_for_task
            .run(Duration::from_secs(1), shutdown_rx)
            .await;
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
        flush_service: flush,
        shutdown_tx,
        http_shutdown_tx: Some(http_shutdown_tx),
        http_handle,
        flush_handle,
    }
}

impl TestServer {
    async fn flush(&self) {
        self.flush_service
            .flush()
            .await
            .expect("manual flush in e2e test");
    }

    async fn stop(self) {
        let _ = self.shutdown_tx.send(true);
        let _ = self.flush_handle.await;
        if let Some(tx) = self.http_shutdown_tx {
            let _ = tx.send(());
        }
        let _ = self.http_handle.await;
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
        url_prefix: None,
        socket: None,
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
#[serial(chdb)]
async fn execute_show_databases() {
    with_server(|server| async move {
        let conn = client_config(&server.url);
        let client = HyperbytedbClient::new(&conn, false).expect("client");

        client
            .query(
                "CREATE DATABASE testdb",
                &QueryOptions {
                    db: None,
                    retention_policy: None,
                    epoch: None,
                    pretty: false,
                    chunked: false,
                    chunk_size: None,
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
#[serial(chdb)]
async fn write_and_query_roundtrip() {
    with_server(|server| async move {
        let conn = client_config(&server.url);
        let client = HyperbytedbClient::new(&conn, false).expect("client");

        client
            .query(
                "CREATE DATABASE metrics",
                &QueryOptions {
                    db: None,
                    retention_policy: None,
                    epoch: None,
                    pretty: false,
                    chunked: false,
                    chunk_size: None,
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
                    consistency: None,
                },
            )
            .await
            .expect("write");

        server.flush().await;

        let resp = client
            .query(
                "SHOW MEASUREMENTS",
                &QueryOptions {
                    db: Some("metrics".to_string()),
                    retention_policy: None,
                    epoch: None,
                    pretty: false,
                    chunked: false,
                    chunk_size: None,
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
        assert!(has_cpu, "expected measurement cpu after flush");

        server.stop().await;
    })
    .await;
}

#[tokio::test]
#[serial(chdb)]
async fn ping_returns_version_header() {
    with_server(|server| async move {
        let client = HyperbytedbClient::new(&client_config(&server.url), false).expect("client");
        let ping = client.ping().await.expect("ping");
        assert!(ping.version.is_some());
        server.stop().await;
    })
    .await;
}

#[tokio::test]
#[serial(chdb)]
async fn materialized_view_via_execute() {
    with_server(|server| async move {
        let conn = client_config(&server.url);
        let client = HyperbytedbClient::new(&conn, false).expect("client");

        client
            .query(
                "CREATE DATABASE mvcli",
                &QueryOptions {
                    db: None,
                    retention_policy: None,
                    epoch: None,
                    pretty: false,
                    chunked: false,
                    chunk_size: None,
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
                    consistency: None,
                },
            )
            .await
            .expect("write");

        server.flush().await;

        let resp = client
            .query(
                "SHOW MEASUREMENTS",
                &QueryOptions {
                    db: Some("mvcli".to_string()),
                    retention_policy: None,
                    epoch: None,
                    pretty: false,
                    chunked: false,
                    chunk_size: None,
                    format: OutputFormat::Json,
                    params: None,
                },
            )
            .await
            .expect("show measurements");
        assert!(
            resp.results.iter().any(|r| {
                r.series.as_ref().is_some_and(|s| {
                    s.iter().any(|ser| {
                        ser.values.iter().any(|row| {
                            row.first()
                                .and_then(|v| v.as_str())
                                .is_some_and(|n| n == "cpu")
                        })
                    })
                })
            }),
            "expected measurement cpu after flush"
        );

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
#[serial(chdb)]
async fn subprocess_execute_show_databases() {
    with_server(|server| async move {
        let output = run_cli(&[
            "-host",
            &server.url,
            "-execute",
            "SHOW DATABASES",
            "-format",
            "column",
        ])
        .await;

        assert_cli_success(&output, "execute SHOW DATABASES");

        server.stop().await;
    })
    .await;
}

#[tokio::test]
#[serial(chdb)]
async fn subprocess_write_data_binary_and_query() {
    with_server(|server| async move {
        let create = run_cli(&["-host", &server.url, "create", "database", "mydb"]).await;
        assert_cli_success(&create, "create database");

        let write = run_cli(&[
            "-host",
            &server.url,
            "write",
            "-database",
            "mydb",
            "--data-binary",
            "cpu,host=srv01 value=42",
        ])
        .await;
        assert_cli_success(&write, "write --data-binary");

        server.flush().await;

        let query = run_cli(&[
            "-host",
            &server.url,
            "query",
            "-database",
            "mydb",
            "--data-urlencode",
            "q=SHOW MEASUREMENTS",
        ])
        .await;
        assert_cli_success(&query, "query --data-urlencode");
        let stdout = String::from_utf8_lossy(&query.stdout);
        assert!(stdout.contains("cpu"), "stdout={stdout}");

        server.stop().await;
    })
    .await;
}

#[tokio::test]
#[serial(chdb)]
async fn subprocess_global_flags_after_subcommand() {
    with_server(|server| async move {
        let output = run_cli(&[
            "query",
            "-host",
            &server.url,
            "-database",
            "mydb",
            "--data-urlencode",
            "q=SHOW DATABASES",
        ])
        .await;

        assert_cli_success(&output, "query with global -host after subcommand");

        server.stop().await;
    })
    .await;
}

#[tokio::test]
#[serial(chdb)]
async fn subprocess_drop_database() {
    with_server(|server| async move {
        let create = run_cli(&["-host", &server.url, "create", "database", "dropme"]).await;
        assert_cli_success(&create, "create database");

        let drop = run_cli(&["-host", &server.url, "drop", "database", "dropme"]).await;
        assert_cli_success(&drop, "drop database");

        server.stop().await;
    })
    .await;
}

#[tokio::test]
#[serial(chdb)]
async fn subprocess_create_database() {
    with_server(|server| async move {
        let create = run_cli(&["-host", &server.url, "create", "database", "mydb"]).await;
        assert_cli_success(&create, "create database");

        let show = run_cli(&[
            "-host",
            &server.url,
            "-execute",
            "SHOW DATABASES",
            "-format",
            "column",
        ])
        .await;

        assert_cli_success(&show, "execute SHOW DATABASES");
        assert!(
            String::from_utf8_lossy(&show.stdout).contains("mydb"),
            "stdout={}",
            String::from_utf8_lossy(&show.stdout)
        );

        server.stop().await;
    })
    .await;
}
