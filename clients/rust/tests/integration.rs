//! Against a real HyperbyteDB: `HYPERBYTEDB_URL` (and, with auth on,
//! `HYPERBYTEDB_USER` / `HYPERBYTEDB_PASSWORD` for an admin). Skipped
//! without it.

use std::collections::BTreeMap;
use std::time::Duration;

use hyperbytedb_client::{
    Client, Epoch, Error, Options, Point, Precision, QueryOptions, WriteOptions,
};
use serde_json::Value;

fn url() -> Option<String> {
    std::env::var("HYPERBYTEDB_URL")
        .ok()
        .filter(|u| !u.is_empty())
}

fn options() -> Options {
    Options {
        username: std::env::var("HYPERBYTEDB_USER").ok(),
        password: std::env::var("HYPERBYTEDB_PASSWORD").ok(),
        ..Default::default()
    }
}

/// A fresh database, dropped at the end.
struct Db {
    client: Client,
    name: String,
}

impl Db {
    async fn new(url: &str) -> Db {
        let client = Client::with_options(url, options()).unwrap();
        let nanos = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .subsec_nanos();
        let name = format!("rsclient_{nanos:08x}");
        client.create_database(&name).await.unwrap();
        Db { client, name }
    }

    async fn drop(self) {
        self.client.drop_database(&self.name).await.unwrap();
    }

    /// Rows reach queries at the next flush; wait for them.
    async fn wait_rows(&self, q: &str, n: usize) -> Vec<BTreeMap<String, Value>> {
        let deadline = std::time::Instant::now() + Duration::from_secs(15);
        loop {
            let rows = self
                .client
                .query(q, &QueryOptions::database(&self.name))
                .await
                .unwrap()[0]
                .rows();
            if rows.len() >= n || std::time::Instant::now() > deadline {
                return rows;
            }
            tokio::time::sleep(Duration::from_millis(500)).await;
        }
    }
}

#[tokio::test]
async fn ping_and_health() {
    let Some(url) = url() else { return };
    let c = Client::with_options(&url, options()).unwrap();
    let v = c.ping().await.unwrap();
    assert!(v.starts_with(|ch: char| ch.is_ascii_digit()), "{v}");
    c.health().await.unwrap();
}

#[tokio::test]
async fn round_trip_with_awkward_names_and_every_type() {
    let Some(url) = url() else { return };
    let db = Db::new(&url).await;
    let base = 1_700_000_000_000i64;
    let points: Vec<Point> = (0..3)
        .map(|i| {
            Point::new("disk usage")
                .tag("host", "web 1,a=b")
                .tag("service", "Filesystem /")
                .field("used", 12.5 + i as f64)
                .field("inodes", 100 + i)
                .field("mount", r#"C:\ "data""#)
                .timestamp(base + i * 1000)
        })
        .collect();
    let opts = WriteOptions {
        precision: Precision::Milliseconds,
        ..Default::default()
    };
    assert_eq!(db.client.write(&db.name, &points, &opts).await.unwrap(), 3);
    let rows = db
        .wait_rows(r#"SELECT * FROM "disk usage" ORDER BY time"#, 3)
        .await;
    assert_eq!(rows.len(), 3);
    assert_eq!(rows[0]["time"].as_i64(), Some(base));
    assert_eq!(rows[0]["host"], "web 1,a=b");
    assert_eq!(rows[0]["service"], "Filesystem /");
    assert_eq!(rows[0]["used"], 12.5);
    assert_eq!(rows[2]["inodes"], 102);
    assert_eq!(rows[0]["mount"], r#"C:\ "data""#);
    db.drop().await;
}

#[tokio::test]
async fn queries_by_tag_with_epoch_and_bound_params() {
    let Some(url) = url() else { return };
    let db = Db::new(&url).await;
    let lines: Vec<String> = (0..3)
        .flat_map(|h| {
            (0..4)
                .map(move |i| format!("cpu,host=h{h} usage={}.0 {}", h * 10 + i, 1_700_000_000 + i))
        })
        .collect();
    let opts = WriteOptions {
        precision: Precision::Seconds,
        ..Default::default()
    };
    assert_eq!(
        db.client
            .write_lines(&db.name, &lines, &opts)
            .await
            .unwrap(),
        12
    );
    db.wait_rows("SELECT * FROM cpu", 12).await;
    let q = QueryOptions {
        database: Some(db.name.clone()),
        epoch: Epoch::Seconds,
        ..Default::default()
    };
    let res = db
        .client
        .query("SELECT max(usage) FROM cpu GROUP BY host", &q)
        .await
        .unwrap();
    // Compared as numbers: the server writes a whole float as `3`.
    let got: BTreeMap<_, _> = res[0]
        .series
        .iter()
        .map(|s| (s.tags["host"].as_str(), s.values[0][1].as_f64()))
        .collect();
    let want = BTreeMap::from([("h0", Some(3.0)), ("h1", Some(13.0)), ("h2", Some(23.0))]);
    assert_eq!(got, want);
    let mut q = QueryOptions::database(&db.name);
    q.params = Some(serde_json::json!({"h": "h1"}).as_object().unwrap().clone());
    let res = db
        .client
        .query("SELECT count(usage) FROM cpu WHERE host = $h", &q)
        .await
        .unwrap();
    assert_eq!(res[0].series[0].values[0][1], 4);
    db.drop().await;
}

#[tokio::test]
async fn errors() {
    let Some(url) = url() else { return };
    let db = Db::new(&url).await;
    let err = db
        .client
        .query("SELEKT nonsense", &QueryOptions::database(&db.name))
        .await
        .unwrap_err();
    assert!(matches!(err, Error::Query(_)), "{err}");
    let err = db
        .client
        .write_lines(
            &db.name,
            ["m v=1", r#"m v="text""#],
            &WriteOptions::default(),
        )
        .await;
    assert!(
        matches!(err, Err(Error::Write { .. })),
        "a field type conflict: {err:?}"
    );
    if let Ok(user) = std::env::var("HYPERBYTEDB_USER") {
        let opts = Options {
            username: Some(user),
            password: Some("wrong".into()),
            retries: 0,
            ..Default::default()
        };
        let err = Client::with_options(&url, opts)
            .unwrap()
            .databases()
            .await
            .unwrap_err();
        assert!(matches!(err, Error::Auth { .. }), "{err}");
    }
    db.drop().await;
}
