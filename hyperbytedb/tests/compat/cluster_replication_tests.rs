//! Golden record of what each cluster statement replicates (non-Raft
//! cluster): the mutation `PeerQueryService` sends for every statement
//! kind, and the statement's own result. Regenerate the fixture with
//! `UPDATE_GOLDEN=1` only when a change to it is intended.

use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use hyperbytedb::adapters::http::router::QueryService;
use hyperbytedb::application::peer_query_service::PeerQueryService;
use hyperbytedb::domain::cluster::types::MutationRequest;
use hyperbytedb::error::HyperbytedbError;
use hyperbytedb::ports::replication::{OutboundReplicationBatch, ReplicationPort};
use serial_test::serial;

use super::TestContext;

/// Records every mutation it is asked to replicate.
#[derive(Default)]
struct RecordingReplication {
    seen: parking_lot::Mutex<Vec<MutationRequest>>,
}

#[async_trait]
impl ReplicationPort for RecordingReplication {
    fn replicate_write(
        self: Arc<Self>,
        _batch: OutboundReplicationBatch,
    ) -> Result<(), HyperbytedbError> {
        Ok(())
    }

    async fn replicate_write_sync(
        self: Arc<Self>,
        _batch: OutboundReplicationBatch,
        _required_acks: usize,
        _timeout: Duration,
    ) -> Result<(), HyperbytedbError> {
        Ok(())
    }

    fn replicate_mutation(self: Arc<Self>, req: MutationRequest, _targets: Option<Vec<u64>>) {
        self.seen.lock().push(req);
    }

    async fn replicate_mutation_sync(
        self: Arc<Self>,
        req: MutationRequest,
    ) -> Result<(), HyperbytedbError> {
        self.seen.lock().push(req);
        Ok(())
    }

    async fn replication_peer_count(&self, _self_node_id: u64) -> usize {
        0
    }
}

/// One statement per replicating arm, plus the cross-database CREATE
/// RETENTION POLICY and an invalid SET. Each is run against `mydb`.
const STATEMENTS: &[&str] = &[
    "CREATE DATABASE mydb",
    "CREATE DATABASE otherdb",
    "CREATE RETENTION POLICY week ON mydb DURATION 7d REPLICATION 1",
    "CREATE RETENTION POLICY r ON otherdb DURATION 1d REPLICATION 1",
    "ALTER RETENTION POLICY week ON mydb DURATION 14d",
    "CREATE USER bob WITH PASSWORD 'secret-one'",
    "SET PASSWORD FOR bob = 'secret-two'",
    "GRANT READ ON mydb TO bob",
    "REVOKE READ ON mydb FROM bob",
    "CREATE CONTINUOUS QUERY cq1 ON mydb BEGIN SELECT mean(v) INTO cpu_1m FROM cpu GROUP BY time(1m) END",
    "DROP CONTINUOUS QUERY cq1 ON mydb",
    r#"CREATE MATERIALIZED VIEW "mv1" ON "mydb" AS SELECT mean("v") AS "mean_v" INTO "cpu_5m" FROM "cpu" GROUP BY time(5m), *"#,
    r#"DROP MATERIALIZED VIEW "mv1" ON "mydb""#,
    "DELETE FROM cpu WHERE host = 'a'",
    "DROP SERIES FROM cpu WHERE host = 'b'",
    "DROP MEASUREMENT cpu",
    "SET query_pool_size = 5",
    "SET query_pool_size = 0",
    "DROP USER bob",
    "DROP RETENTION POLICY week ON mydb",
    "DROP DATABASE otherdb",
];

/// Replace each `password_hash` with a placeholder, after checking it is an
/// argon2 hash and not the plain password, and each `created_at` (wall
/// clock) with another.
fn mask_hashes(v: &mut serde_json::Value) {
    match v {
        serde_json::Value::Object(map) => {
            for (k, val) in map.iter_mut() {
                if k == "password_hash" {
                    let h = val.as_str().expect("password_hash is a string");
                    assert!(h.starts_with("$argon2"), "not an argon2 hash: {h}");
                    assert!(!h.contains("secret"), "plain password replicated");
                    *val = serde_json::Value::String("<hash>".into());
                } else if k == "created_at" {
                    *val = serde_json::Value::String("<time>".into());
                } else {
                    mask_hashes(val);
                }
            }
        }
        serde_json::Value::Array(items) => items.iter_mut().for_each(mask_hashes),
        _ => {}
    }
}

#[tokio::test]
#[serial]
async fn every_cluster_mutation_replicates_as_before() {
    let ctx = TestContext::new_no_chdb().unwrap();
    let replication = Arc::new(RecordingReplication::default());
    // With runtime settings, so a valid SET applies and replicates.
    let inner = Arc::new(
        hyperbytedb::application::query_service::QueryServiceImpl::new(
            ctx.query_port.clone(),
            ctx.metadata.clone(),
            ctx.wal.clone(),
            30,
            Arc::new(super::NoopPointsSink),
        )
        .with_runtime_settings(Arc::new(
            hyperbytedb::application::runtime_settings::RuntimeSettings::default(),
        )),
    );
    let peer = PeerQueryService::new(inner, ctx.metadata.clone(), replication.clone());

    let mut record = Vec::new();
    for (i, stmt) in STATEMENTS.iter().enumerate() {
        if i == 2 {
            // Rows for the DELETE / DROP SERIES / DROP MEASUREMENT arms.
            ctx.write_and_flush(
                "mydb",
                "cpu,host=a v=1 1000000000\ncpu,host=b v=2 1000000000",
            )
            .await
            .unwrap();
        }
        let resp = peer.execute_query("mydb", stmt, None, None, None).await;
        let outcome = match resp {
            Ok(r) => serde_json::json!(
                r.results
                    .iter()
                    .map(|s| s.error.clone())
                    .collect::<Vec<_>>()
            ),
            Err(e) => serde_json::json!({ "err": e.to_string() }),
        };
        let replicated: Vec<MutationRequest> = std::mem::take(&mut *replication.seen.lock());
        let mut replicated = serde_json::to_value(replicated).unwrap();
        mask_hashes(&mut replicated);
        record.push(serde_json::json!({
            "statement": stmt,
            "errors": outcome,
            "replicated": replicated,
        }));
    }
    let got = serde_json::to_string_pretty(&record).unwrap() + "\n";

    let fixture = std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("tests/compat/testdata/replicated_mutations.json");
    if std::env::var_os("UPDATE_GOLDEN").is_some() {
        std::fs::create_dir_all(fixture.parent().unwrap()).unwrap();
        std::fs::write(&fixture, &got).unwrap();
    }
    let want =
        std::fs::read_to_string(&fixture).expect("fixture missing; run with UPDATE_GOLDEN=1");
    assert_eq!(got, want, "replicated mutations changed");
}

/// Number of rows `q` returns on `ctx`.
async fn row_count(ctx: &TestContext, q: &str) -> usize {
    let resp = ctx.query("testdb", q).await.unwrap();
    let stmt = &resp.results[0];
    assert!(stmt.error.is_none(), "{q}: {:?}", stmt.error);
    stmt.series.iter().flatten().map(|s| s.values.len()).sum()
}

/// A replica applying a replicated DROP SERIES with no FROM deletes the
/// matching rows of every measurement that has the tag, as the
/// coordinator does on its own node.
#[tokio::test]
#[serial(chdb)]
async fn a_replica_applies_drop_series_without_from_to_its_rows() {
    let Ok(replica) = TestContext::new() else {
        eprintln!("skipping: chDB not available");
        return;
    };
    let rows = "cpu,host=a value=1.0 1000000000\ncpu,host=b value=2.0 2000000000\n\
                mem,host=a value=3.0 1000000000\ndisk,dev=x value=4.0 1000000000";
    replica.metadata.create_database("testdb").await.unwrap();
    replica.write_and_flush("testdb", rows).await.unwrap();

    // The coordinator: build the mutation it replicates.
    let coord = TestContext::new_no_chdb().unwrap();
    coord.metadata.create_database("testdb").await.unwrap();
    coord.write_and_flush("testdb", rows).await.unwrap();
    let replication = Arc::new(RecordingReplication::default());
    let peer = PeerQueryService::new(
        coord.query_service.clone(),
        coord.metadata.clone(),
        replication.clone(),
    );
    let resp = peer
        .execute_query(
            "testdb",
            r#"DROP SERIES WHERE "host" = 'a'"#,
            None,
            None,
            None,
        )
        .await
        .unwrap();
    assert!(
        resp.results[0].error.is_none(),
        "{:?}",
        resp.results[0].error
    );
    let mutation = replication
        .seen
        .lock()
        .pop()
        .expect("DROP SERIES replicates");

    hyperbytedb::application::schema_mutation_apply::apply_schema_mutation(
        hyperbytedb::application::schema_mutation_apply::SchemaMutationDeps {
            metadata: &replica.metadata,
            mv_service: None,
            points_sink: Some(&replica.points_sink),
            wal: Some(&replica.wal),
            shard_routing: None,
            runtime_settings: None,
        },
        mutation,
    )
    .await
    .unwrap();

    assert_eq!(row_count(&replica, "SELECT value FROM cpu").await, 1);
    assert_eq!(row_count(&replica, "SELECT value FROM mem").await, 0);
    assert_eq!(row_count(&replica, "SELECT value FROM disk").await, 1);
}

/// A WAL that counts how often a delete asks where it ends (once per flush
/// wait) and otherwise passes through.
struct CountingWal {
    inner: Arc<dyn hyperbytedb::ports::wal::WalPort>,
    last_sequence_calls: std::sync::atomic::AtomicUsize,
}

#[async_trait]
impl hyperbytedb::ports::wal::WalPort for CountingWal {
    async fn append(
        &self,
        entry: hyperbytedb::domain::wal::WalEntry,
    ) -> Result<u64, HyperbytedbError> {
        self.inner.append(entry).await
    }
    async fn read_from(
        &self,
        sequence: u64,
    ) -> Result<Vec<(u64, hyperbytedb::domain::wal::WalEntry)>, HyperbytedbError> {
        self.inner.read_from(sequence).await
    }
    async fn read_range(
        &self,
        from: u64,
        max_entries: usize,
    ) -> Result<Vec<(u64, hyperbytedb::domain::wal::WalEntry)>, HyperbytedbError> {
        self.inner.read_range(from, max_entries).await
    }
    async fn truncate_before(&self, sequence: u64) -> Result<(), HyperbytedbError> {
        self.inner.truncate_before(sequence).await
    }
    async fn last_sequence(&self) -> Result<u64, HyperbytedbError> {
        self.last_sequence_calls
            .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        self.inner.last_sequence().await
    }
    fn flushed_through(&self) -> u64 {
        self.inner.flushed_through()
    }
    fn set_flushed_through(&self, sequence: u64) {
        self.inner.set_flushed_through(sequence)
    }
    fn flushed_margin_start(&self, through: u64, max_bytes: u64) -> u64 {
        self.inner.flushed_margin_start(through, max_bytes)
    }
}

/// The replicated DROP SERIES with no FROM runs inside the Raft apply: it
/// waits for the WAL to flush once, not once per measurement.
#[tokio::test]
#[serial(chdb)]
async fn a_replica_drop_series_without_from_waits_for_the_flush_once() {
    let Ok(replica) = TestContext::new() else {
        eprintln!("skipping: chDB not available");
        return;
    };
    replica.metadata.create_database("testdb").await.unwrap();
    replica
        .write_and_flush(
            "testdb",
            "cpu,host=a value=1.0 1000000000\nmem,host=a value=3.0 1000000000\n\
             disk,host=a value=4.0 1000000000",
        )
        .await
        .unwrap();
    let counting = Arc::new(CountingWal {
        inner: replica.wal.clone(),
        last_sequence_calls: Default::default(),
    });
    let wal: Arc<dyn hyperbytedb::ports::wal::WalPort> = counting.clone();

    hyperbytedb::application::schema_mutation_apply::apply_schema_mutation(
        hyperbytedb::application::schema_mutation_apply::SchemaMutationDeps {
            metadata: &replica.metadata,
            mv_service: None,
            points_sink: Some(&replica.points_sink),
            wal: Some(&wal),
            shard_routing: None,
            runtime_settings: None,
        },
        MutationRequest::DropSeries {
            database: "testdb".into(),
            rp: "autogen".into(),
            measurement: None,
            predicate_sql: "`host` = 'a'".into(),
        },
    )
    .await
    .unwrap();

    let calls = counting
        .last_sequence_calls
        .load(std::sync::atomic::Ordering::Relaxed);
    assert_eq!(calls, 1, "flush waits");
    assert_eq!(row_count(&replica, "SELECT value FROM cpu").await, 0);
}
