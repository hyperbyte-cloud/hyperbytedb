//! Client behaviour against a small fake server.

use std::collections::{HashMap, VecDeque};
use std::io::Read;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use axum::body::Bytes;
use axum::extract::{Query, State};
use axum::http::{HeaderMap, Method, StatusCode, Uri};
use axum::response::IntoResponse;
use hyperbytedb_client::{
    Client, Epoch, Error, Options, Point, Precision, QueryOptions, WriteOptions,
};

#[derive(Clone)]
struct Reply(u16, Vec<(&'static str, &'static str)>, &'static str);

#[derive(Debug, Clone)]
#[allow(dead_code)]
struct Request {
    method: Method,
    path: String,
    query: HashMap<String, String>,
    headers: HeaderMap,
    body: String,
}

#[derive(Default)]
struct Fake {
    script: Mutex<VecDeque<Reply>>,
    requests: Mutex<Vec<Request>>,
}

impl Fake {
    fn requests(&self) -> Vec<Request> {
        self.requests.lock().unwrap().clone()
    }
}

async fn handle(
    State(fake): State<Arc<Fake>>,
    method: Method,
    uri: Uri,
    Query(query): Query<HashMap<String, String>>,
    headers: HeaderMap,
    body: Bytes,
) -> impl IntoResponse {
    let body = if headers.get("content-encoding").is_some_and(|v| v == "gzip") {
        let mut s = String::new();
        flate2::read::GzDecoder::new(&body[..])
            .read_to_string(&mut s)
            .unwrap();
        s
    } else {
        String::from_utf8_lossy(&body).into_owned()
    };
    fake.requests.lock().unwrap().push(Request {
        method,
        path: uri.path().to_owned(),
        query,
        headers,
        body,
    });
    let Reply(status, hdrs, payload) =
        fake.script
            .lock()
            .unwrap()
            .pop_front()
            .unwrap_or(Reply(204, vec![], ""));
    let mut h = HeaderMap::new();
    for (k, v) in hdrs {
        h.insert(k, v.parse().unwrap());
    }
    (StatusCode::from_u16(status).unwrap(), h, payload)
}

async fn fake(script: Vec<Reply>) -> (Arc<Fake>, String) {
    let fake = Arc::new(Fake {
        script: Mutex::new(script.into()),
        ..Default::default()
    });
    let app = axum::Router::new()
        .fallback(handle)
        .with_state(fake.clone());
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let url = format!("http://{}", listener.local_addr().unwrap());
    tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });
    (fake, url)
}

fn client(url: &str, opts: Options) -> Client {
    Client::with_options(
        url,
        Options {
            backoff: Duration::from_millis(10),
            ..opts
        },
    )
    .unwrap()
}

#[tokio::test]
async fn writes_are_batched_gzipped_and_authenticated() {
    let (f, url) = fake(vec![]).await;
    let points: Vec<Point> = (0..5)
        .map(|i| {
            Point::new("m")
                .tag("h", "a")
                .field("v", i as f64 + 0.5)
                .timestamp(i)
        })
        .collect();
    let opts = Options {
        username: Some("u".into()),
        password: Some("p".into()),
        batch_points: 2,
        ..Default::default()
    };
    let n = client(&url, opts)
        .write(
            "mydb",
            &points,
            &WriteOptions {
                precision: Precision::Seconds,
                ..Default::default()
            },
        )
        .await
        .unwrap();
    assert_eq!(n, 5);
    let reqs = f.requests();
    assert_eq!(reqs.len(), 3, "5 points in batches of 2");
    let r = &reqs[0];
    assert_eq!((r.method.as_str(), r.path.as_str()), ("POST", "/write"));
    assert_eq!(
        r.query,
        HashMap::from([
            ("db".into(), "mydb".into()),
            ("precision".into(), "s".into())
        ])
    );
    assert_eq!(r.headers["authorization"], "Basic dTpw");
    assert_eq!(r.headers["content-encoding"], "gzip");
    assert!(r.headers["user-agent"]
        .to_str()
        .unwrap()
        .starts_with("hyperbytedb-rust/"));
    assert_eq!(r.body, "m,h=a v=0.5 0\nm,h=a v=1.5 1");
}

#[tokio::test]
async fn retries_503_honouring_retry_after() {
    let (f, url) = fake(vec![
        Reply(503, vec![("retry-after", "0")], "busy"),
        Reply(503, vec![], "busy"),
    ])
    .await;
    assert_eq!(
        client(&url, Options::default())
            .write_lines("db", ["m v=1"], &WriteOptions::default())
            .await
            .unwrap(),
        1
    );
    assert_eq!(f.requests().len(), 3);
}

#[tokio::test]
async fn gives_up_after_the_last_retry() {
    let busy = Reply(503, vec![], r#"{"error":"write capacity exhausted"}"#);
    let (_, url) = fake(vec![busy.clone(), busy.clone(), busy]).await;
    let err = client(
        &url,
        Options {
            retries: 2,
            ..Default::default()
        },
    )
    .write_lines("db", ["m v=1"], &WriteOptions::default())
    .await
    .unwrap_err();
    assert!(
        matches!(&err, Error::Server { status: 503, message } if message.contains("capacity")),
        "{err}"
    );
    assert!(err.is_retryable());
}

#[tokio::test]
async fn a_413_splits_the_batch() {
    let (f, url) = fake(vec![Reply(413, vec![], "too big")]).await;
    let n = client(&url, Options::default())
        .write_lines(
            "db",
            ["m v=1", "m v=2", "m v=3", "m v=4"],
            &WriteOptions::default(),
        )
        .await
        .unwrap();
    assert_eq!(n, 4);
    let bodies: Vec<String> = f.requests().into_iter().map(|r| r.body).collect();
    assert_eq!(
        bodies,
        ["m v=1\nm v=2\nm v=3\nm v=4", "m v=1\nm v=2", "m v=3\nm v=4"]
    );
}

#[tokio::test]
async fn bad_writes_and_auth_failures_are_not_retried() {
    let (f, url) = fake(vec![
        Reply(400, vec![], r#"{"error":"field type conflict"}"#),
        Reply(401, vec![], "authorization failed"),
    ])
    .await;
    let c = client(&url, Options::default());
    let err = c
        .write_lines("db", ["m v=1"], &WriteOptions::default())
        .await
        .unwrap_err();
    assert!(
        matches!(&err, Error::Write { status: 400, message } if message == "field type conflict"),
        "{err}"
    );
    assert!(!err.is_retryable());
    let err = c
        .write_lines("db", ["m v=1"], &WriteOptions::default())
        .await
        .unwrap_err();
    assert!(matches!(err, Error::Auth { status: 401, .. }), "{err}");
    assert_eq!(f.requests().len(), 2);
}

#[tokio::test]
async fn query_results_and_errors() {
    let doc = r#"{"results":[{"statement_id":0,"series":[{"name":"cpu","tags":{"host":"a"},"columns":["time","mean"],"values":[[1791806400500000001,2.5]]}]}]}"#;
    let (f, url) = fake(vec![
        Reply(200, vec![("content-type", "application/json")], doc),
        Reply(
            200,
            vec![],
            r#"{"results":[{"statement_id":0,"error":"measurement not found"}]}"#,
        ),
        Reply(400, vec![], r#"{"error":"error parsing query: bad"}"#),
    ])
    .await;
    let c = client(&url, Options::default());
    let mut opts = QueryOptions::database("db");
    opts.params = Some(serde_json::json!({"h": "a"}).as_object().unwrap().clone());
    let res = c
        .query("SELECT mean(v) FROM cpu GROUP BY host", &opts)
        .await
        .unwrap();
    let rows = res[0].rows();
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0]["host"], "a");
    // A nanosecond time keeps every digit.
    assert_eq!(rows[0]["time"].as_u64(), Some(1_791_806_400_500_000_001));
    assert_eq!(rows[0]["mean"], 2.5);
    let r = &f.requests()[0];
    assert_eq!(r.method, Method::POST);
    assert_eq!(
        r.body,
        "q=SELECT+mean%28v%29+FROM+cpu+GROUP+BY+host&db=db&epoch=ms&params=%7B%22h%22%3A%22a%22%7D"
    );

    let err = c
        .query("SELECT * FROM nope", &QueryOptions::database("db"))
        .await
        .unwrap_err();
    assert!(
        matches!(&err, Error::Query(m) if m.contains("not found")),
        "{err}"
    );
    let err = c
        .query("SELEC", &QueryOptions::database("db"))
        .await
        .unwrap_err();
    assert!(
        matches!(&err, Error::Query(m) if m.contains("parsing")),
        "{err}"
    );
}

#[tokio::test]
async fn keep_errors_and_rfc3339() {
    let (f, url) = fake(vec![Reply(
        200,
        vec![],
        r#"{"results":[{"statement_id":0,"error":"boom"}]}"#,
    )])
    .await;
    let opts = QueryOptions {
        epoch: Epoch::Rfc3339,
        keep_errors: true,
        ..Default::default()
    };
    let res = client(&url, Options::default())
        .query("SELECT 1", &opts)
        .await
        .unwrap();
    assert_eq!(res[0].error.as_deref(), Some("boom"));
    assert!(!f.requests()[0].body.contains("epoch"));
}

#[tokio::test]
async fn health_answers_503_without_retrying() {
    let (f, url) = fake(vec![Reply(
        503,
        vec![("content-type", "application/json")],
        r#"{"status":"draining"}"#,
    )])
    .await;
    let h = client(
        &url,
        Options {
            backoff: Duration::from_secs(5),
            ..Default::default()
        },
    )
    .health()
    .await
    .unwrap();
    assert_eq!(h, serde_json::json!({"status": "draining"}));
    assert_eq!(f.requests().len(), 1);
}

#[tokio::test]
async fn ping_prefers_the_hyperbytedb_version_header() {
    let (_, url) = fake(vec![
        Reply(
            204,
            vec![
                ("x-hyperbytedb-version", "0.9.3-beta"),
                ("x-influxdb-version", "HyperbyteDB-0.9.3-beta"),
            ],
            "",
        ),
        Reply(
            204,
            vec![("x-influxdb-version", "HyperbyteDB-0.9.2-beta")],
            "",
        ),
    ])
    .await;
    let c = client(&url, Options::default());
    assert_eq!(c.ping().await.unwrap(), "0.9.3-beta");
    assert_eq!(
        c.ping().await.unwrap(),
        "0.9.2-beta",
        "a server before 0.9.3"
    );
}

#[tokio::test]
async fn an_unreachable_server_fails_after_retries() {
    let err = client(
        "http://127.0.0.1:1",
        Options {
            retries: 1,
            ..Default::default()
        },
    )
    .ping()
    .await
    .unwrap_err();
    assert!(
        matches!(err, Error::Connection { .. }) && err.is_retryable(),
        "{err}"
    );
}
