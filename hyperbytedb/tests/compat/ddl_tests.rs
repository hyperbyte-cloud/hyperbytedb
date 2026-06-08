//! DDL statement compatibility tests.
//!
//! Verifies CREATE/DROP DATABASE, CREATE/DROP RETENTION POLICY,
//! DROP MEASUREMENT, DELETE, and continuous query DDL behave
//! consistently with InfluxDB 1.8.x.

use hyperbytedb::ports::ingestion::{IngestionPort, WritePayloadFormat};

use super::TestContext;

// ---------------------------------------------------------------------------
// CREATE / DROP DATABASE
// ---------------------------------------------------------------------------

#[tokio::test]
async fn create_database() {
    let ctx = TestContext::new_no_chdb().unwrap();
    let resp = ctx.query("", "CREATE DATABASE mydb").await.unwrap();
    assert!(resp.results[0].error.is_none());

    let dbs = ctx.metadata.list_databases().await.unwrap();
    let names: Vec<_> = dbs.iter().map(|d| d.name.as_str()).collect();
    assert!(names.contains(&"mydb"));
}

#[tokio::test]
async fn create_database_idempotent() {
    let ctx = TestContext::new_no_chdb().unwrap();
    let resp1 = ctx.query("", "CREATE DATABASE mydb").await.unwrap();
    assert!(resp1.results[0].error.is_none());

    let resp2 = ctx.query("", "CREATE DATABASE mydb").await.unwrap();
    assert!(
        resp2.results[0].error.is_none(),
        "CREATE DATABASE should be idempotent (InfluxDB ignores duplicate creates)"
    );

    let dbs = ctx.metadata.list_databases().await.unwrap();
    let count = dbs.iter().filter(|d| d.name == "mydb").count();
    assert_eq!(count, 1, "Database should exist exactly once");
}

#[tokio::test]
async fn drop_database() {
    let ctx = TestContext::new_no_chdb().unwrap();
    ctx.metadata.create_database("todrop").await.unwrap();

    let resp = ctx.query("", "DROP DATABASE todrop").await.unwrap();
    assert!(resp.results[0].error.is_none());

    let dbs = ctx.metadata.list_databases().await.unwrap();
    let names: Vec<_> = dbs.iter().map(|d| d.name.as_str()).collect();
    assert!(!names.contains(&"todrop"));
}

#[tokio::test]
async fn drop_nonexistent_database_no_error() {
    let ctx = TestContext::new_no_chdb().unwrap();

    let resp = ctx.query("", "DROP DATABASE nonexistent").await.unwrap();
    assert!(
        resp.results[0].error.is_none(),
        "DROP DATABASE on nonexistent DB should succeed silently (InfluxDB behavior)"
    );
}

#[tokio::test]
async fn create_database_with_autogen_rp() {
    let ctx = TestContext::new_no_chdb().unwrap();
    ctx.query("", "CREATE DATABASE mydb").await.unwrap();

    let rps = ctx.metadata.list_retention_policies("mydb").await.unwrap();
    assert!(
        rps.iter().any(|rp| rp.name == "autogen" && rp.is_default),
        "New database should have default 'autogen' retention policy"
    );
}

// ---------------------------------------------------------------------------
// RETENTION POLICIES
// ---------------------------------------------------------------------------

#[tokio::test]
async fn create_retention_policy() {
    let ctx = TestContext::new_no_chdb().unwrap();
    ctx.metadata.create_database("rptest").await.unwrap();

    let resp = ctx
        .query(
            "rptest",
            "CREATE RETENTION POLICY \"oneweek\" ON \"rptest\" DURATION 7d REPLICATION 1",
        )
        .await
        .unwrap();
    assert!(resp.results[0].error.is_none());

    let rps = ctx
        .metadata
        .list_retention_policies("rptest")
        .await
        .unwrap();
    let names: Vec<_> = rps.iter().map(|rp| rp.name.as_str()).collect();
    assert!(names.contains(&"oneweek"));
}

#[tokio::test]
async fn create_retention_policy_default() {
    let ctx = TestContext::new_no_chdb().unwrap();
    ctx.metadata.create_database("rptest").await.unwrap();

    let resp = ctx
        .query(
            "rptest",
            "CREATE RETENTION POLICY \"myrp\" ON \"rptest\" DURATION 30d REPLICATION 1 DEFAULT",
        )
        .await
        .unwrap();
    assert!(resp.results[0].error.is_none());

    let rps = ctx
        .metadata
        .list_retention_policies("rptest")
        .await
        .unwrap();
    let myrp = rps.iter().find(|rp| rp.name == "myrp");
    assert!(myrp.is_some(), "Created RP should exist");
}

#[tokio::test]
async fn drop_retention_policy() {
    let ctx = TestContext::new_no_chdb().unwrap();
    ctx.metadata.create_database("rptest").await.unwrap();
    ctx.metadata
        .create_retention_policy(
            "rptest",
            hyperbytedb::domain::database::RetentionPolicy {
                name: "temporary".to_string(),
                duration: Some(std::time::Duration::from_secs(3600)),
                shard_group_duration: std::time::Duration::from_secs(3600),
                replication_factor: 1,
                is_default: false,
            },
        )
        .await
        .unwrap();

    let resp = ctx
        .query(
            "rptest",
            "DROP RETENTION POLICY \"temporary\" ON \"rptest\"",
        )
        .await
        .unwrap();
    assert!(resp.results[0].error.is_none());

    let rps = ctx
        .metadata
        .list_retention_policies("rptest")
        .await
        .unwrap();
    let names: Vec<_> = rps.iter().map(|rp| rp.name.as_str()).collect();
    assert!(!names.contains(&"temporary"), "Dropped RP should be gone");
    assert!(names.contains(&"autogen"), "Default RP should remain");
}

// ---------------------------------------------------------------------------
// DROP MEASUREMENT
// ---------------------------------------------------------------------------

#[tokio::test]
async fn drop_measurement() {
    let ctx = TestContext::new_no_chdb().unwrap();
    ctx.metadata.create_database("testdb").await.unwrap();
    ctx.ingestion
        .ingest(
            "testdb",
            None,
            None,
            b"todrop value=1.0 1000000000\nkeep value=2.0 2000000000",
            WritePayloadFormat::LineProtocol,
        )
        .await
        .unwrap();

    let resp = ctx
        .query("testdb", "DROP MEASUREMENT todrop")
        .await
        .unwrap();
    assert!(resp.results[0].error.is_none());

    let measurements = ctx.metadata.list_measurements("testdb").await.unwrap();
    assert!(!measurements.contains(&"todrop".to_string()));
    assert!(measurements.contains(&"keep".to_string()));
}

#[tokio::test]
async fn drop_measurement_removes_associated_metadata() {
    let ctx = TestContext::new_no_chdb().unwrap();
    ctx.metadata.create_database("testdb").await.unwrap();
    ctx.ingestion
        .ingest(
            "testdb",
            None,
            None,
            b"todrop,host=srv1 value=1.0 1000000000",
            WritePayloadFormat::LineProtocol,
        )
        .await
        .unwrap();

    ctx.query("testdb", "DROP MEASUREMENT todrop")
        .await
        .unwrap();

    let tag_keys = ctx
        .metadata
        .list_tag_keys("testdb", Some("todrop"))
        .await
        .unwrap();
    assert!(
        tag_keys.is_empty(),
        "Tag keys for dropped measurement should be removed"
    );
}

// ---------------------------------------------------------------------------
// DELETE
// ---------------------------------------------------------------------------

#[tokio::test]
async fn delete_from_measurement_where_time() {
    let ctx = TestContext::new_no_chdb().unwrap();
    ctx.metadata.create_database("testdb").await.unwrap();
    ctx.ingestion
        .ingest(
            "testdb",
            None,
            None,
            b"cpu value=1.0 1000000000\ncpu value=2.0 2000000000",
            WritePayloadFormat::LineProtocol,
        )
        .await
        .unwrap();

    let resp = ctx
        .query("testdb", "DELETE FROM cpu WHERE time < 1500000000")
        .await
        .unwrap();
    assert!(resp.results[0].error.is_none());

    let tombstones = ctx.metadata.list_tombstones("testdb", "cpu").await.unwrap();
    assert!(!tombstones.is_empty(), "DELETE should create a tombstone");
}

#[tokio::test]
async fn delete_from_measurement_where_tag() {
    let ctx = TestContext::new_no_chdb().unwrap();
    ctx.metadata.create_database("testdb").await.unwrap();
    ctx.ingestion
        .ingest(
            "testdb",
            None,
            None,
            b"cpu,host=a value=1.0 1000000000\ncpu,host=b value=2.0 2000000000",
            WritePayloadFormat::LineProtocol,
        )
        .await
        .unwrap();

    let resp = ctx
        .query(
            "testdb",
            "DELETE FROM cpu WHERE \"host\" = 'a' AND time < 2000000000",
        )
        .await
        .unwrap();
    assert!(resp.results[0].error.is_none());

    let tombstones = ctx.metadata.list_tombstones("testdb", "cpu").await.unwrap();
    assert!(!tombstones.is_empty());
}

// ---------------------------------------------------------------------------
// CONTINUOUS QUERIES
// ---------------------------------------------------------------------------

#[tokio::test]
async fn create_continuous_query() {
    let ctx = TestContext::new_no_chdb().unwrap();
    ctx.metadata.create_database("testdb").await.unwrap();

    let resp = ctx
        .query(
            "testdb",
            "CREATE CONTINUOUS QUERY \"cq1\" ON \"testdb\" BEGIN SELECT mean(value) INTO \"downsampled\".\":MEASUREMENT\" FROM /.*/ GROUP BY time(1m), * END",
        )
        .await
        .unwrap();
    assert!(resp.results[0].error.is_none());

    let cqs = ctx
        .metadata
        .list_continuous_queries("testdb")
        .await
        .unwrap();
    assert!(!cqs.is_empty());
    assert_eq!(cqs[0].name, "cq1");
    assert_eq!(cqs[0].database, "testdb");
}

#[tokio::test]
async fn drop_continuous_query() {
    let ctx = TestContext::new_no_chdb().unwrap();
    ctx.metadata.create_database("testdb").await.unwrap();
    ctx.metadata
        .store_continuous_query(
            "testdb",
            "cq_drop",
            &hyperbytedb::ports::metadata::ContinuousQueryDef {
                name: "cq_drop".to_string(),
                database: "testdb".to_string(),
                query_text: "SELECT ... INTO ...".to_string(),
                resample_every_secs: None,
                resample_for_secs: None,
                created_at: chrono::Utc::now().to_rfc3339(),
            },
        )
        .await
        .unwrap();

    let resp = ctx
        .query("testdb", "DROP CONTINUOUS QUERY \"cq_drop\" ON \"testdb\"")
        .await
        .unwrap();
    assert!(resp.results[0].error.is_none());

    let cqs = ctx
        .metadata
        .list_continuous_queries("testdb")
        .await
        .unwrap();
    assert!(cqs.iter().all(|cq| cq.name != "cq_drop"));
}
