//! DDL statement compatibility tests.
//!
//! Verifies CREATE/DROP DATABASE, CREATE/DROP RETENTION POLICY,
//! DROP MEASUREMENT, DELETE, and continuous query DDL behave
//! consistently with InfluxDB 1.8.x.

use hyperbytedb::error::HyperbytedbError;
use hyperbytedb::ports::ingestion::{IngestionPort, WritePayloadFormat};
use serial_test::serial;

use super::TestContext;

/// Epoch nanoseconds on a 1-minute boundary (matches MV `toStartOfInterval` bucket keys).
const MV_MINUTE_ALIGNED_NS: i64 = 1_700_000_040_000_000_000;

async fn mv_fact_row_count(ctx: &TestContext, table: &str) -> u64 {
    ctx.query_port
        .execute_sql(&format!(
            "SELECT count() AS c FROM `{table}` FORMAT JSONEachRow"
        ))
        .await
        .unwrap()
        .lines()
        .next()
        .and_then(|l| serde_json::from_str::<serde_json::Value>(l).ok())
        .and_then(|v| v.get("c").and_then(|c| c.as_u64()))
        .unwrap_or(0)
}

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
async fn drop_nonexistent_database_errors() {
    let ctx = TestContext::new_no_chdb().unwrap();

    let err = ctx
        .query("", "DROP DATABASE nonexistent")
        .await
        .unwrap_err();
    assert!(
        matches!(err, HyperbytedbError::DatabaseNotFound(_)),
        "DROP DATABASE on nonexistent DB should error: {err}"
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
    let oneweek = rps.iter().find(|rp| rp.name == "oneweek").unwrap();
    assert!(
        !oneweek.is_default,
        "RP without DEFAULT keyword must not become the default"
    );
    let autogen = rps.iter().find(|rp| rp.name == "autogen").unwrap();
    assert!(autogen.is_default, "autogen should remain default");
}

#[tokio::test]
async fn create_retention_policy_name_containing_default_substring() {
    let ctx = TestContext::new_no_chdb().unwrap();
    ctx.metadata.create_database("rptest").await.unwrap();

    let resp = ctx
        .query(
            "rptest",
            r#"CREATE RETENTION POLICY "default_high" ON "rptest" DURATION 52w REPLICATION 1"#,
        )
        .await
        .unwrap();
    assert!(
        resp.results[0].error.is_none(),
        "{:?}",
        resp.results[0].error
    );

    let rps = ctx
        .metadata
        .list_retention_policies("rptest")
        .await
        .unwrap();
    let rp = rps.iter().find(|rp| rp.name == "default_high").unwrap();
    assert!(
        !rp.is_default,
        "RP name containing 'default' must not be treated as DEFAULT modifier"
    );
    let autogen = rps.iter().find(|rp| rp.name == "autogen").unwrap();
    assert!(autogen.is_default);
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
    assert!(
        myrp.unwrap().is_default,
        "DEFAULT keyword should mark RP as default"
    );
    let autogen = rps.iter().find(|rp| rp.name == "autogen").unwrap();
    assert!(
        !autogen.is_default,
        "previous default should be cleared when a new DEFAULT RP is created"
    );
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
        .list_tag_keys("testdb", "autogen", Some("todrop"))
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

    let tombstones = ctx
        .metadata
        .list_tombstones("testdb", "autogen", "cpu")
        .await
        .unwrap();
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

    let err = ctx
        .query(
            "testdb",
            r#"DELETE FROM cpu WHERE "host" = 'a' AND time < 2000000000"#,
        )
        .await
        .unwrap_err();
    assert!(
        matches!(err, HyperbytedbError::QueryParse(_)),
        "DELETE with tag predicate should fail at parse: {err:?}"
    );
    assert!(
        err.to_string()
            .contains("only time predicates are supported"),
        "unexpected error: {err:?}"
    );
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
                group_by_interval_secs: 60,
                group_by_offset_secs: 0,
                execution_interval_secs: 60,
                coverage_interval_secs: 60,
                is_advanced: false,
                last_run_at: None,
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

// ---------------------------------------------------------------------------
// MATERIALIZED VIEWS
// ---------------------------------------------------------------------------

#[tokio::test]
#[serial(chdb)]
async fn create_and_query_materialized_view() {
    let ctx = match TestContext::new() {
        Ok(c) => c,
        Err(_) => {
            eprintln!("skipping MV test: chDB not available");
            return;
        }
    };

    ctx.metadata.create_database("mvdb").await.unwrap();

    let t1 = 1_700_000_000_000_000_000i64;
    let t2 = t1 + 60_000_000_000;
    let t3 = t1 + 120_000_000_000;
    ctx.write_and_flush(
        "mvdb",
        &format!("cpu,host=h1 value=10 {t1}\ncpu,host=h1 value=20 {t2}\ncpu,host=h1 value=30 {t3}"),
    )
    .await
    .unwrap();

    let create_resp = ctx
        .query(
            "mvdb",
            r#"CREATE MATERIALIZED VIEW "mv_cpu_5m" ON "mvdb" AS SELECT mean("value") INTO "cpu_5m" FROM "cpu" GROUP BY time(5m), *"#,
        )
        .await
        .unwrap();
    assert!(
        create_resp.results[0].error.is_none(),
        "create MV failed: {:?}",
        create_resp.results[0].error
    );

    let mvs = ctx.metadata.list_materialized_views("mvdb").await.unwrap();
    assert_eq!(mvs.len(), 1);
    assert_eq!(mvs[0].name, "mv_cpu_5m");
    assert_eq!(mvs[0].dest_measurement, "cpu_5m");

    let t4 = t1 + 180_000_000_000;
    ctx.write_and_flush("mvdb", &format!("cpu,host=h1 value=40 {t4}"))
        .await
        .unwrap();

    let query_resp = ctx
        .query(
            "mvdb",
            r#"SELECT mean("value") FROM "cpu_5m" GROUP BY time(5m), "host""#,
        )
        .await
        .unwrap();
    assert!(
        query_resp.results[0].error.is_none(),
        "query dest failed: {:?}",
        query_resp.results[0].error
    );
    let series = query_resp.results[0].series.as_ref().unwrap();
    assert!(!series.is_empty());
    assert!(!series[0].values.is_empty());
}

#[tokio::test]
#[serial(chdb)]
async fn materialized_view_sum_survives_multiple_flushes() {
    let ctx = match TestContext::new() {
        Ok(c) => c,
        Err(_) => {
            eprintln!("skipping MV multi-flush test: chDB not available");
            return;
        }
    };

    ctx.metadata.create_database("mvdb").await.unwrap();

    let t = MV_MINUTE_ALIGNED_NS;
    ctx.write_and_flush(
        "mvdb",
        &format!("metrics,host=h1 value=10 {t}\nmetrics,host=h2 value=20 {t}"),
    )
    .await
    .unwrap();

    let create_resp = ctx
        .query(
            "mvdb",
            r#"CREATE MATERIALIZED VIEW "mv_metrics_sum" ON "mvdb" WITH BACKFILL AS SELECT sum("value") AS "value" INTO "metrics_1m" FROM "metrics" GROUP BY time(1m), "host""#,
        )
        .await
        .unwrap();
    assert!(
        create_resp.results[0].error.is_none(),
        "create MV failed: {:?}",
        create_resp.results[0].error
    );

    let dest_meta = ctx
        .metadata
        .get_measurement("mvdb", "autogen", "metrics_1m")
        .await
        .unwrap()
        .expect("dest measurement metadata");
    assert_eq!(
        dest_meta.field_rollups.get("value"),
        Some(&hyperbytedb::domain::rollup::RollupCombine::Sum)
    );

    // Second flush lands a distinct point in the *same* minute bucket (t + 1s),
    // so it accumulates with the first flush rather than overwriting it (reusing
    // the same (series, timestamp) would be an InfluxDB last-write-wins overwrite).
    let t2 = t + 1_000_000_000;
    ctx.write_and_flush("mvdb", &format!("metrics,host=h1 value=5 {t2}"))
        .await
        .unwrap();

    let dest_resp = ctx
        .query(
            "mvdb",
            &format!(r#"SELECT sum("value") FROM "metrics_1m" WHERE time = {t}"#),
        )
        .await
        .unwrap();
    assert!(
        dest_resp.results[0].error.is_none(),
        "query dest failed: {:?}",
        dest_resp.results[0].error
    );
    // The aggregate is the last column (a `GROUP BY time` result prepends `time`).
    let dest_sum: f64 = dest_resp.results[0]
        .series
        .as_ref()
        .unwrap()
        .first()
        .and_then(|s| s.values.first())
        .and_then(|row| row.last())
        .and_then(|v| v.as_f64())
        .unwrap_or(0.0);

    let raw_resp = ctx
        .query(
            "mvdb",
            &format!(
                r#"SELECT sum("value") FROM "metrics" WHERE time >= {t} AND time <= {t2} GROUP BY time(1m), "host""#
            ),
        )
        .await
        .unwrap();
    assert!(raw_resp.results[0].error.is_none());
    let raw_total: f64 = raw_resp.results[0]
        .series
        .as_ref()
        .unwrap()
        .iter()
        .filter_map(|s| s.values.first()?.last()?.as_f64())
        .sum();

    assert!(
        (dest_sum - raw_total).abs() < 0.01,
        "MV dest sum {dest_sum} should match raw grouped sum {raw_total}"
    );
    assert!(
        (dest_sum - 35.0).abs() < 0.01,
        "expected 10+20+5=35 after two flushes, got {dest_sum}"
    );
}

#[tokio::test]
#[serial(chdb)]
async fn materialized_view_dest_uses_summing_merge_tree() {
    let ctx = match TestContext::new() {
        Ok(c) => c,
        Err(_) => {
            eprintln!("skipping MV SummingMergeTree test: chDB not available");
            return;
        }
    };

    ctx.metadata.create_database("mvdb").await.unwrap();
    ctx.write_and_flush(
        "mvdb",
        &format!("metrics,host=h1 value=1 {MV_MINUTE_ALIGNED_NS}"),
    )
    .await
    .unwrap();

    let create_resp = ctx
        .query(
            "mvdb",
            r#"CREATE MATERIALIZED VIEW "mv_metrics_sum" ON "mvdb" WITH BACKFILL AS SELECT sum("value") AS "value" INTO "metrics_1m" FROM "metrics" GROUP BY time(1m), "host""#,
        )
        .await
        .unwrap();
    assert!(
        create_resp.results[0].error.is_none(),
        "create MV failed: {:?}",
        create_resp.results[0].error
    );

    let ddl = ctx
        .query_port
        .execute_sql("SHOW CREATE TABLE `mvdb_autogen_metrics_1m`")
        .await
        .unwrap();
    assert!(
        ddl.contains("SummingMergeTree"),
        "MV dest should use SummingMergeTree for additive rollups, got: {ddl}"
    );
    assert!(
        ddl.contains("`value`"),
        "SummingMergeTree should list rollup columns, got: {ddl}"
    );
}

#[tokio::test]
#[serial(chdb)]
async fn materialized_view_sum_matches_raw_after_many_single_point_flushes() {
    let ctx = match TestContext::new() {
        Ok(c) => c,
        Err(_) => {
            eprintln!("skipping MV many-flush test: chDB not available");
            return;
        }
    };

    ctx.metadata.create_database("mvdb").await.unwrap();

    let t = MV_MINUTE_ALIGNED_NS;
    for (host, value) in [("h1", 10), ("h2", 20), ("h3", 30), ("h4", 40), ("h5", 50)] {
        ctx.write_and_flush("mvdb", &format!("metrics,host={host} value={value} {t}"))
            .await
            .unwrap();
    }

    let create_resp = ctx
        .query(
            "mvdb",
            r#"CREATE MATERIALIZED VIEW "mv_metrics_many" ON "mvdb" WITH BACKFILL AS SELECT sum("value") AS "value" INTO "metrics_many" FROM "metrics" GROUP BY time(1m), "host""#,
        )
        .await
        .unwrap();
    assert!(
        create_resp.results[0].error.is_none(),
        "create MV failed: {:?}",
        create_resp.results[0].error
    );

    // Second batch lands distinct points in the same minute bucket (t + 1s) so
    // they accumulate with the first batch instead of overwriting it.
    let t2 = t + 1_000_000_000;
    for (host, value) in [("h1", 5), ("h2", 5), ("h3", 5), ("h4", 5), ("h5", 5)] {
        ctx.write_and_flush("mvdb", &format!("metrics,host={host} value={value} {t2}"))
            .await
            .unwrap();
    }

    let dest_resp = ctx
        .query(
            "mvdb",
            &format!(r#"SELECT sum("value") FROM "metrics_many" WHERE time = {t}"#),
        )
        .await
        .unwrap();
    assert!(
        dest_resp.results[0].error.is_none(),
        "query dest failed: {:?}",
        dest_resp.results[0].error
    );
    let dest_total: f64 = dest_resp.results[0]
        .series
        .as_ref()
        .unwrap()
        .first()
        .and_then(|s| s.values.first())
        .and_then(|row| row.first())
        .and_then(|v| v.as_f64())
        .unwrap_or(0.0);

    let raw_resp = ctx
        .query(
            "mvdb",
            &format!(r#"SELECT sum("value") FROM "metrics" WHERE time >= {t} AND time <= {t2}"#),
        )
        .await
        .unwrap();
    assert!(raw_resp.results[0].error.is_none());
    let raw_total: f64 = raw_resp.results[0]
        .series
        .as_ref()
        .unwrap()
        .first()
        .and_then(|s| s.values.first())
        .and_then(|row| row.last())
        .and_then(|v| v.as_f64())
        .unwrap_or(0.0);

    assert!(
        (dest_total - raw_total).abs() < 0.01,
        "MV dest total {dest_total} should match raw total {raw_total} after many single-point flushes"
    );
    assert!(
        (dest_total - 175.0).abs() < 0.01,
        "expected 15+25+35+45+55=175 after ten single-point flushes, got {dest_total}"
    );
}

#[tokio::test]
#[serial(chdb)]
async fn materialized_view_mean_survives_multiple_flushes() {
    let ctx = match TestContext::new() {
        Ok(c) => c,
        Err(_) => {
            eprintln!("skipping MV mean multi-flush test: chDB not available");
            return;
        }
    };

    ctx.metadata.create_database("mvdb").await.unwrap();

    let t = 1_700_000_000_000_000_000i64;
    let t2 = t + 10_000_000_000;
    ctx.write_and_flush("mvdb", &format!("cpu,host=h1 value=10 {t}"))
        .await
        .unwrap();

    let create_resp = ctx
        .query(
            "mvdb",
            r#"CREATE MATERIALIZED VIEW "mv_cpu_mean" ON "mvdb" WITH BACKFILL AS SELECT mean("value") INTO "cpu_1m" FROM "cpu" GROUP BY time(1m), "host""#,
        )
        .await
        .unwrap();
    assert!(
        create_resp.results[0].error.is_none(),
        "create MV failed: {:?}",
        create_resp.results[0].error
    );

    let dest_meta = ctx
        .metadata
        .get_measurement("mvdb", "autogen", "cpu_1m")
        .await
        .unwrap()
        .expect("dest measurement metadata");
    assert!(
        dest_meta.mean_fields.contains_key("value"),
        "mean MV should register sum/count columns for value"
    );

    ctx.write_and_flush("mvdb", &format!("cpu,host=h1 value=30 {t2}"))
        .await
        .unwrap();

    let dest_resp = ctx
        .query(
            "mvdb",
            r#"SELECT mean("value") FROM "cpu_1m" GROUP BY time(1m), "host""#,
        )
        .await
        .unwrap();
    assert!(
        dest_resp.results[0].error.is_none(),
        "query dest failed: {:?}",
        dest_resp.results[0].error
    );
    // The aggregate is the last column (`GROUP BY time` prepends a `time` column).
    let dest_mean: f64 = dest_resp.results[0]
        .series
        .as_ref()
        .unwrap()
        .first()
        .and_then(|s| s.values.first())
        .and_then(|row| row.last())
        .and_then(|v| v.as_f64())
        .unwrap_or(0.0);

    let raw_resp = ctx
        .query(
            "mvdb",
            r#"SELECT mean("value") FROM "cpu" GROUP BY time(1m), "host""#,
        )
        .await
        .unwrap();
    assert!(raw_resp.results[0].error.is_none());
    let raw_mean: f64 = raw_resp.results[0]
        .series
        .as_ref()
        .unwrap()
        .first()
        .and_then(|s| s.values.first())
        .and_then(|row| row.last())
        .and_then(|v| v.as_f64())
        .unwrap_or(0.0);

    assert!(
        (dest_mean - raw_mean).abs() < 0.01,
        "MV dest mean {dest_mean} should match raw grouped mean {raw_mean}"
    );
    assert!(
        (dest_mean - 20.0).abs() < 0.01,
        "expected mean(10,30)=20 after two flushes, got {dest_mean}"
    );
}

#[tokio::test]
#[serial(chdb)]
async fn materialized_view_sum_dedupes_duplicate_source_rows() {
    let ctx = match TestContext::new() {
        Ok(c) => c,
        Err(_) => {
            eprintln!("skipping MV source dedup test: chDB not available");
            return;
        }
    };

    ctx.metadata.create_database("mvdb").await.unwrap();

    let t = MV_MINUTE_ALIGNED_NS;
    // Same point flushed twice mimics replication / re-ingest duplicate rows.
    ctx.write_and_flush("mvdb", &format!("metrics,host=h1 value=10 {t}"))
        .await
        .unwrap();
    ctx.write_and_flush("mvdb", &format!("metrics,host=h1 value=10 {t}"))
        .await
        .unwrap();

    let create_resp = ctx
        .query(
            "mvdb",
            r#"CREATE MATERIALIZED VIEW "mv_metrics_dedup" ON "mvdb" WITH BACKFILL AS SELECT sum("value") AS "value" INTO "metrics_dedup" FROM "metrics" GROUP BY time(1m), "host""#,
        )
        .await
        .unwrap();
    assert!(
        create_resp.results[0].error.is_none(),
        "create MV failed: {:?}",
        create_resp.results[0].error
    );

    let dest_resp = ctx
        .query(
            "mvdb",
            &format!(r#"SELECT sum("value") FROM "metrics_dedup" WHERE time = {t}"#),
        )
        .await
        .unwrap();
    assert!(
        dest_resp.results[0].error.is_none(),
        "query dest failed: {:?}",
        dest_resp.results[0].error
    );
    let dest_sum: f64 = dest_resp.results[0]
        .series
        .as_ref()
        .unwrap()
        .first()
        .and_then(|s| s.values.first())
        .and_then(|row| row.first())
        .and_then(|v| v.as_f64())
        .unwrap_or(0.0);

    let raw_resp = ctx
        .query(
            "mvdb",
            &format!(r#"SELECT sum("value") FROM "metrics" WHERE time = {t}"#),
        )
        .await
        .unwrap();
    assert!(raw_resp.results[0].error.is_none());
    let raw_sum: f64 = raw_resp.results[0]
        .series
        .as_ref()
        .unwrap()
        .first()
        .and_then(|s| s.values.first())
        .and_then(|row| row.first())
        .and_then(|v| v.as_f64())
        .unwrap_or(0.0);

    assert!(
        (dest_sum - raw_sum).abs() < 0.01,
        "MV dest sum {dest_sum} should match raw coalesced sum {raw_sum}"
    );
    assert!(
        (dest_sum - 10.0).abs() < 0.01,
        "duplicate source rows must not inflate MV sum, got {dest_sum}"
    );
}

#[tokio::test]
#[serial(chdb)]
async fn materialized_view_dest_in_different_rp_poisons_same_name_in_autogen() {
    let ctx = match TestContext::new() {
        Ok(c) => c,
        Err(_) => {
            eprintln!("skipping: chDB not available");
            return;
        }
    };

    ctx.metadata.create_database("reprodb").await.unwrap();

    // Create a non-default retention policy.
    ctx.query(
        "reprodb",
        r#"CREATE RETENTION POLICY "high" ON "reprodb" DURATION 30d REPLICATION 1"#,
    )
    .await
    .unwrap();

    // Write to `svstats` in autogen (default RP). Should succeed.
    ctx.write_and_flush("reprodb", "svstats,host=h1 value=1.0 1700000000000000000")
        .await
        .expect("first write to autogen.svstats should succeed");

    // Create an MV that writes *to the same measurement name* but in the "high" RP.
    let create_resp = ctx
        .query(
            "reprodb",
            r#"CREATE MATERIALIZED VIEW "mv_bug" ON "reprodb" AS SELECT mean("value") INTO "high"."svstats" FROM "svstats" GROUP BY time(1m), *"#,
        )
        .await
        .unwrap();
    assert!(
        create_resp.results[0].error.is_none(),
        "create MV failed: {:?}",
        create_resp.results[0].error
    );

    // Write to `svstats` in autogen again. This SHOULD still succeed because
    // autogen.svstats and high.svstats are different measurements.
    // Bug: it currently fails with "cannot write directly to materialized view destination".
    let result = ctx
        .write_and_flush("reprodb", "svstats,host=h1 value=2.0 1700000000000000000")
        .await;
    assert!(
        result.is_ok(),
        "BUG REPRODUCED: write to autogen.svstats failed after creating MV dest in high.svstats: {:?}",
        result
    );
}

#[tokio::test]
#[serial(chdb)]
async fn create_materialized_view_without_backfill_leaves_dest_empty_until_new_writes() {
    let ctx = match TestContext::new() {
        Ok(c) => c,
        Err(_) => {
            eprintln!("skipping MV no-backfill test: chDB not available");
            return;
        }
    };

    ctx.metadata.create_database("mvdb").await.unwrap();

    let t = MV_MINUTE_ALIGNED_NS;
    ctx.write_and_flush("mvdb", &format!("metrics,host=h1 value=10 {t}"))
        .await
        .unwrap();

    let create_resp = ctx
        .query(
            "mvdb",
            r#"CREATE MATERIALIZED VIEW "mv_no_backfill" ON "mvdb" AS SELECT sum("value") AS "value" INTO "metrics_nobf" FROM "metrics" GROUP BY time(1m), "host""#,
        )
        .await
        .unwrap();
    assert!(
        create_resp.results[0].error.is_none(),
        "create MV failed: {:?}",
        create_resp.results[0].error
    );

    let dest_count: u64 = mv_fact_row_count(&ctx, "mvdb_autogen_metrics_nobf").await;
    assert_eq!(
        dest_count, 0,
        "dest fact table should be empty before any post-create writes"
    );

    let dest_series_count: u64 = mv_fact_row_count(&ctx, "mvdb_autogen_metrics_nobf_series").await;
    assert!(
        dest_series_count > 0,
        "dest series table should be seeded at create even without fact backfill"
    );

    let t2 = t + 1_000_000_000;
    ctx.write_and_flush("mvdb", &format!("metrics,host=h1 value=5 {t2}"))
        .await
        .unwrap();

    let dest_resp = ctx
        .query(
            "mvdb",
            &format!(r#"SELECT sum("value") FROM "metrics_nobf" WHERE time = {t}"#),
        )
        .await
        .unwrap();
    assert!(
        dest_resp.results[0].error.is_none(),
        "query dest failed: {:?}",
        dest_resp.results[0].error
    );
    let dest_sum: f64 = dest_resp.results[0]
        .series
        .as_ref()
        .unwrap()
        .first()
        .and_then(|s| s.values.first())
        .and_then(|row| row.last())
        .and_then(|v| v.as_f64())
        .unwrap_or(0.0);
    assert!(
        (dest_sum - 5.0).abs() < 0.01,
        "only post-create writes should appear in dest, got {dest_sum}"
    );
}

#[tokio::test]
#[serial(chdb)]
async fn create_materialized_view_without_backfill_seeds_dest_series_metadata() {
    use chrono::{TimeZone, Utc};

    let ctx = match TestContext::new() {
        Ok(c) => c,
        Err(_) => {
            eprintln!("skipping MV dest series metadata test: chDB not available");
            return;
        }
    };

    fn ts(h: u32, m: u32) -> i64 {
        Utc.with_ymd_and_hms(2016, 8, 28, h, m, 0)
            .unwrap()
            .timestamp_nanos_opt()
            .unwrap()
    }

    ctx.metadata.create_database("gameservers").await.unwrap();
    ctx.metadata
        .create_retention_policy(
            "gameservers",
            hyperbytedb::domain::database::RetentionPolicy {
                name: "default_high".to_string(),
                duration: Some(std::time::Duration::from_secs(7 * 24 * 3600)),
                shard_group_duration: std::time::Duration::from_secs(3600),
                replication_factor: 1,
                is_default: false,
            },
        )
        .await
        .unwrap();

    let minute = ts(8, 0);
    let lines = format!(
        "server_stats,region_id=us,server_id=s1 cpu=1i {minute}\n\
         server_stats,region_id=eu,server_id=s2 cpu=1i {minute}\n\
         server_stats,region_id=ap,server_id=s3 cpu=1i {minute}\n\
         server_stats,region_id=us,server_id=s4 cpu=1i {minute}\n\
         server_stats,region_id=eu,server_id=s5 cpu=1i {minute}",
    );
    ctx.write_and_flush("gameservers", &lines).await.unwrap();

    let create_resp = ctx
        .query(
            "gameservers",
            r#"CREATE MATERIALIZED VIEW "mv_server_stats" ON "gameservers" AS SELECT count("cpu") AS "num_servers" INTO "default_high"."server_stats" FROM "server_stats" GROUP BY time(1m), "region_id""#,
        )
        .await
        .unwrap();
    assert!(
        create_resp.results[0].error.is_none(),
        "create MV failed: {:?}",
        create_resp.results[0].error
    );

    let dest_series_count: u64 =
        mv_fact_row_count(&ctx, "gameservers_default_high_server_stats_series").await;
    assert_eq!(
        dest_series_count, 3,
        "dest series should collapse to one row per region_id"
    );

    let tag_keys = ctx
        .query(
            "gameservers",
            r#"SHOW TAG KEYS FROM "default_high"."server_stats""#,
        )
        .await
        .unwrap();
    assert!(
        tag_keys.results[0].error.is_none(),
        "{:?}",
        tag_keys.results[0].error
    );
    let keys: Vec<&str> = tag_keys.results[0].series.as_ref().unwrap()[0]
        .values
        .iter()
        .filter_map(|row| row.first().and_then(|v| v.as_str()))
        .collect();
    assert!(keys.contains(&"region_id"));
    assert!(!keys.contains(&"server_id"));

    let tag_values = ctx
        .query(
            "gameservers",
            r#"SHOW TAG VALUES FROM "default_high"."server_stats" WITH KEY = "region_id""#,
        )
        .await
        .unwrap();
    assert!(
        tag_values.results[0].error.is_none(),
        "{:?}",
        tag_values.results[0].error
    );
    let regions: Vec<&str> = tag_values.results[0].series.as_ref().unwrap()[0]
        .values
        .iter()
        .filter_map(|row| row.get(1).and_then(|v| v.as_str()))
        .collect();
    assert_eq!(regions.len(), 3);
    assert!(regions.contains(&"us"));
    assert!(regions.contains(&"eu"));
    assert!(regions.contains(&"ap"));

    let show_series = ctx
        .query(
            "gameservers",
            r#"SHOW SERIES FROM "default_high"."server_stats""#,
        )
        .await
        .unwrap();
    assert!(
        show_series.results[0].error.is_none(),
        "{:?}",
        show_series.results[0].error
    );
    let series_keys = &show_series.results[0].series.as_ref().unwrap()[0].values;
    assert_eq!(
        series_keys.len(),
        3,
        "SHOW SERIES should list collapsed dest series"
    );
}

#[tokio::test]
#[serial(chdb)]
async fn create_materialized_view_with_backfill_populates_history() {
    let ctx = match TestContext::new() {
        Ok(c) => c,
        Err(_) => {
            eprintln!("skipping MV with-backfill test: chDB not available");
            return;
        }
    };

    ctx.metadata.create_database("mvdb").await.unwrap();

    let t = MV_MINUTE_ALIGNED_NS;
    ctx.write_and_flush("mvdb", &format!("metrics,host=h1 value=10 {t}"))
        .await
        .unwrap();

    let create_resp = ctx
        .query(
            "mvdb",
            r#"CREATE MATERIALIZED VIEW "mv_with_backfill" ON "mvdb" WITH BACKFILL AS SELECT sum("value") AS "value" INTO "metrics_bf" FROM "metrics" GROUP BY time(1m), "host""#,
        )
        .await
        .unwrap();
    assert!(
        create_resp.results[0].error.is_none(),
        "create MV failed: {:?}",
        create_resp.results[0].error
    );

    let mvs = ctx.metadata.list_materialized_views("mvdb").await.unwrap();
    let mv_def = mvs.iter().find(|m| m.name == "mv_with_backfill").unwrap();
    assert!(mv_def.backfill_on_create);

    let dest_resp = ctx
        .query(
            "mvdb",
            &format!(r#"SELECT sum("value") FROM "metrics_bf" WHERE time = {t}"#),
        )
        .await
        .unwrap();
    assert!(
        dest_resp.results[0].error.is_none(),
        "query dest failed: {:?}",
        dest_resp.results[0].error
    );
    let dest_sum: f64 = dest_resp.results[0]
        .series
        .as_ref()
        .unwrap()
        .first()
        .and_then(|s| s.values.first())
        .and_then(|row| row.last())
        .and_then(|v| v.as_f64())
        .unwrap_or(0.0);
    assert!(
        (dest_sum - 10.0).abs() < 0.01,
        "WITH BACKFILL should populate historical data, got {dest_sum}"
    );
}

#[tokio::test]
#[serial(chdb)]
async fn create_materialized_view_without_backfill_records_metadata_flag() {
    let ctx = match TestContext::new() {
        Ok(c) => c,
        Err(_) => {
            eprintln!("skipping MV metadata flag test: chDB not available");
            return;
        }
    };

    ctx.metadata.create_database("mvdb").await.unwrap();

    let t = MV_MINUTE_ALIGNED_NS;
    ctx.write_and_flush("mvdb", &format!("metrics,host=h1 value=1 {t}"))
        .await
        .unwrap();

    let create_resp = ctx
        .query(
            "mvdb",
            r#"CREATE MATERIALIZED VIEW "mv_flag_off" ON "mvdb" AS SELECT sum("value") AS "value" INTO "metrics_flag" FROM "metrics" GROUP BY time(1m), "host""#,
        )
        .await
        .unwrap();
    assert!(
        create_resp.results[0].error.is_none(),
        "create MV failed: {:?}",
        create_resp.results[0].error
    );

    let mvs = ctx.metadata.list_materialized_views("mvdb").await.unwrap();
    let mv_def = mvs.iter().find(|m| m.name == "mv_flag_off").unwrap();
    assert!(
        !mv_def.backfill_on_create,
        "default CREATE should persist backfill_on_create=false"
    );
}

#[tokio::test]
#[serial(chdb)]
async fn create_materialized_view_without_backfill_on_empty_source() {
    let ctx = match TestContext::new() {
        Ok(c) => c,
        Err(_) => {
            eprintln!("skipping MV empty-source no-backfill test: chDB not available");
            return;
        }
    };

    ctx.metadata.create_database("mvdb").await.unwrap();

    let t = MV_MINUTE_ALIGNED_NS;
    ctx.write_and_flush("mvdb", &format!("metrics,host=h1 value=1 {t}"))
        .await
        .unwrap();
    let delete_resp = ctx
        .query("mvdb", &format!("DELETE FROM metrics WHERE time <= {t}"))
        .await
        .unwrap();
    assert!(
        delete_resp.results[0].error.is_none(),
        "delete failed: {:?}",
        delete_resp.results[0].error
    );

    let create_resp = ctx
        .query(
            "mvdb",
            r#"CREATE MATERIALIZED VIEW "mv_empty_src" ON "mvdb" AS SELECT sum("value") AS "value" INTO "metrics_empty" FROM "metrics" GROUP BY time(1m), "host""#,
        )
        .await
        .unwrap();
    assert!(
        create_resp.results[0].error.is_none(),
        "create MV failed: {:?}",
        create_resp.results[0].error
    );

    assert_eq!(
        mv_fact_row_count(&ctx, "mvdb_autogen_metrics_empty").await,
        0,
        "dest should stay empty when source has no data and backfill is off"
    );
}

#[tokio::test]
#[serial(chdb)]
async fn create_materialized_view_without_vs_with_backfill_on_same_source() {
    let ctx = match TestContext::new() {
        Ok(c) => c,
        Err(_) => {
            eprintln!("skipping MV without-vs-with test: chDB not available");
            return;
        }
    };

    ctx.metadata.create_database("mvdb").await.unwrap();

    let t = MV_MINUTE_ALIGNED_NS;
    ctx.write_and_flush(
        "mvdb",
        &format!("metrics,host=h1 value=10 {t}\nmetrics,host=h2 value=20 {t}"),
    )
    .await
    .unwrap();

    let no_bf = ctx
        .query(
            "mvdb",
            r#"CREATE MATERIALIZED VIEW "mv_cmp_no_bf" ON "mvdb" AS SELECT sum("value") AS "value" INTO "metrics_cmp_nobf" FROM "metrics" GROUP BY time(1m), "host""#,
        )
        .await
        .unwrap();
    assert!(
        no_bf.results[0].error.is_none(),
        "create without backfill failed: {:?}",
        no_bf.results[0].error
    );

    let with_bf = ctx
        .query(
            "mvdb",
            r#"CREATE MATERIALIZED VIEW "mv_cmp_with_bf" ON "mvdb" WITH BACKFILL AS SELECT sum("value") AS "value" INTO "metrics_cmp_bf" FROM "metrics" GROUP BY time(1m), "host""#,
        )
        .await
        .unwrap();
    assert!(
        with_bf.results[0].error.is_none(),
        "create with backfill failed: {:?}",
        with_bf.results[0].error
    );

    assert_eq!(
        mv_fact_row_count(&ctx, "mvdb_autogen_metrics_cmp_nobf").await,
        0,
        "without-backfill dest should be empty before new writes"
    );
    assert!(
        mv_fact_row_count(&ctx, "mvdb_autogen_metrics_cmp_bf").await >= 2,
        "with-backfill dest should contain rolled-up rows for each host"
    );

    let bf_total_resp = ctx
        .query(
            "mvdb",
            &format!(r#"SELECT sum("value") FROM "metrics_cmp_bf" WHERE time = {t}"#),
        )
        .await
        .unwrap();
    assert!(bf_total_resp.results[0].error.is_none());
    let bf_total: f64 = bf_total_resp.results[0]
        .series
        .as_ref()
        .unwrap()
        .iter()
        .filter_map(|s| s.values.first()?.last()?.as_f64())
        .sum();
    assert!(
        (bf_total - 30.0).abs() < 0.01,
        "with-backfill dest should include all pre-create source data, got {bf_total}"
    );
}

#[tokio::test]
#[serial(chdb)]
async fn create_materialized_view_with_backfill_respects_where_time() {
    let ctx = match TestContext::new() {
        Ok(c) => c,
        Err(_) => {
            eprintln!("skipping MV bounded backfill test: chDB not available");
            return;
        }
    };

    ctx.metadata.create_database("mvdb").await.unwrap();

    let t_old = MV_MINUTE_ALIGNED_NS;
    let t_new = t_old + 3_600_000_000_000; // +1h, distinct minute bucket
    ctx.write_and_flush("mvdb", &format!("metrics,host=h1 value=100 {t_old}"))
        .await
        .unwrap();
    ctx.write_and_flush("mvdb", &format!("metrics,host=h1 value=5 {t_new}"))
        .await
        .unwrap();

    let create_resp = ctx
        .query(
            "mvdb",
            &format!(
                r#"CREATE MATERIALIZED VIEW "mv_bounded_bf" ON "mvdb" WITH BACKFILL AS SELECT sum("value") AS "value" INTO "metrics_bounded" FROM "metrics" WHERE time >= {t_new} GROUP BY time(1m), "host""#
            ),
        )
        .await
        .unwrap();
    assert!(
        create_resp.results[0].error.is_none(),
        "create MV failed: {:?}",
        create_resp.results[0].error
    );

    let old_count: u64 = ctx
        .query_port
        .execute_sql("SELECT count() AS c FROM `mvdb_autogen_metrics_bounded` FORMAT JSONEachRow")
        .await
        .unwrap()
        .lines()
        .next()
        .and_then(|l| serde_json::from_str::<serde_json::Value>(l).ok())
        .and_then(|v| v.get("c").and_then(|c| c.as_u64()))
        .unwrap_or(0);
    assert_eq!(
        old_count, 1,
        "bounded backfill should only materialize the recent bucket"
    );

    let new_resp = ctx
        .query(
            "mvdb",
            &format!(r#"SELECT sum("value") FROM "metrics_bounded" WHERE time = {t_new}"#),
        )
        .await
        .unwrap();
    assert!(new_resp.results[0].error.is_none());
    let new_sum: f64 = new_resp.results[0]
        .series
        .as_ref()
        .unwrap()
        .first()
        .and_then(|s| s.values.first())
        .and_then(|row| row.last())
        .and_then(|v| v.as_f64())
        .unwrap_or(0.0);
    assert!(
        (new_sum - 5.0).abs() < 0.01,
        "bounded backfill should include matching recent bucket, got {new_sum}"
    );
}

#[tokio::test]
#[serial(chdb)]
async fn create_materialized_view_with_backfill_then_incremental_write() {
    let ctx = match TestContext::new() {
        Ok(c) => c,
        Err(_) => {
            eprintln!("skipping MV backfill plus incremental test: chDB not available");
            return;
        }
    };

    ctx.metadata.create_database("mvdb").await.unwrap();

    let t = MV_MINUTE_ALIGNED_NS;
    ctx.write_and_flush("mvdb", &format!("metrics,host=h1 value=10 {t}"))
        .await
        .unwrap();

    let create_resp = ctx
        .query(
            "mvdb",
            r#"CREATE MATERIALIZED VIEW "mv_bf_incr" ON "mvdb" WITH BACKFILL AS SELECT sum("value") AS "value" INTO "metrics_bf_incr" FROM "metrics" GROUP BY time(1m), "host""#,
        )
        .await
        .unwrap();
    assert!(
        create_resp.results[0].error.is_none(),
        "create MV failed: {:?}",
        create_resp.results[0].error
    );

    let t2 = t + 1_000_000_000;
    ctx.write_and_flush("mvdb", &format!("metrics,host=h1 value=5 {t2}"))
        .await
        .unwrap();

    let dest_resp = ctx
        .query(
            "mvdb",
            &format!(r#"SELECT sum("value") FROM "metrics_bf_incr" WHERE time = {t}"#),
        )
        .await
        .unwrap();
    assert!(dest_resp.results[0].error.is_none());
    let dest_sum: f64 = dest_resp.results[0]
        .series
        .as_ref()
        .unwrap()
        .first()
        .and_then(|s| s.values.first())
        .and_then(|row| row.last())
        .and_then(|v| v.as_f64())
        .unwrap_or(0.0);
    assert!(
        (dest_sum - 15.0).abs() < 0.01,
        "backfill plus incremental flush should accumulate in the same bucket, got {dest_sum}"
    );
}

#[tokio::test]
async fn drop_materialized_view() {
    let ctx = TestContext::new_no_chdb().unwrap();
    ctx.metadata.create_database("testdb").await.unwrap();
    ctx.metadata
        .store_materialized_view(
            "testdb",
            "mv_drop",
            &hyperbytedb::domain::materialized_view::MaterializedViewDef {
                name: "mv_drop".to_string(),
                database: "testdb".to_string(),
                query_text: r#"SELECT mean("value") INTO "cpu_5m" FROM "cpu" GROUP BY time(5m), *"#
                    .to_string(),
                source_db: "testdb".to_string(),
                source_rp: "autogen".to_string(),
                source_measurement: "cpu".to_string(),
                dest_db: "testdb".to_string(),
                dest_rp: "autogen".to_string(),
                dest_measurement: "cpu_5m".to_string(),
                ch_fact_mv_name: "testdb_autogen_mv_drop_mv".to_string(),
                ch_series_mv_name: "testdb_autogen_mv_drop_series_mv".to_string(),
                created_at: chrono::Utc::now().to_rfc3339(),
                backfill_on_create: false,
            },
        )
        .await
        .unwrap();

    let resp = ctx
        .query("testdb", r#"DROP MATERIALIZED VIEW "mv_drop" ON "testdb""#)
        .await
        .unwrap();
    assert!(
        resp.results[0].error.is_none(),
        "drop failed: {:?}",
        resp.results[0].error
    );

    let mvs = ctx
        .metadata
        .list_materialized_views("testdb")
        .await
        .unwrap();
    assert!(mvs.iter().all(|mv| mv.name != "mv_drop"));
}
