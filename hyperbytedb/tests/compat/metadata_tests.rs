//! Metadata query compatibility tests.
//!
//! Verifies response shapes and content for SHOW DATABASES, SHOW MEASUREMENTS,
//! SHOW TAG KEYS, SHOW TAG VALUES, SHOW FIELD KEYS, SHOW SERIES,
//! SHOW RETENTION POLICIES, and SHOW CONTINUOUS QUERIES.

use hyperbytedb::ports::ingestion::{IngestionPort, WritePayloadFormat};
use serial_test::serial;

use super::TestContext;

// ---------------------------------------------------------------------------
// SHOW DATABASES
// ---------------------------------------------------------------------------

#[tokio::test]
async fn show_databases_response_shape() {
    let ctx = TestContext::new_no_chdb().unwrap();
    ctx.metadata.create_database("db1").await.unwrap();
    ctx.metadata.create_database("db2").await.unwrap();

    let resp = ctx.query("", "SHOW DATABASES").await.unwrap();

    assert_eq!(resp.results.len(), 1);
    assert_eq!(resp.results[0].statement_id, 0);
    assert!(resp.results[0].error.is_none());

    let series = resp.results[0].series.as_ref().unwrap();
    assert_eq!(series.len(), 1);
    assert_eq!(
        series[0].name, "databases",
        "InfluxDB names this series 'databases'"
    );
    assert_eq!(
        series[0].columns,
        vec!["name".to_string()],
        "SHOW DATABASES columns should be ['name']"
    );
    assert!(
        series[0].tags.is_none(),
        "SHOW DATABASES should have no tags"
    );

    let names: Vec<&str> = series[0]
        .values
        .iter()
        .filter_map(|row| row.first().and_then(|v| v.as_str()))
        .collect();
    assert!(names.contains(&"db1"));
    assert!(names.contains(&"db2"));
}

#[tokio::test]
async fn show_databases_empty() {
    let ctx = TestContext::new_no_chdb().unwrap();

    let resp = ctx.query("", "SHOW DATABASES").await.unwrap();
    assert!(resp.results[0].error.is_none());
    let series = resp.results[0].series.as_ref().unwrap();
    assert_eq!(
        series[0].values.len(),
        0,
        "No databases should return empty values"
    );
}

// ---------------------------------------------------------------------------
// SHOW MEASUREMENTS
// ---------------------------------------------------------------------------

#[tokio::test]
async fn show_measurements_response_shape() {
    let ctx = TestContext::new_no_chdb().unwrap();
    ctx.metadata.create_database("testdb").await.unwrap();
    ctx.ingestion
        .ingest(
            "testdb",
            None,
            None,
            b"cpu value=1.0 1000000000\nmemory value=2.0 2000000000",
            WritePayloadFormat::LineProtocol,
        )
        .await
        .unwrap();

    let resp = ctx.query("testdb", "SHOW MEASUREMENTS").await.unwrap();
    assert!(resp.results[0].error.is_none());

    let series = resp.results[0].series.as_ref().unwrap();
    assert_eq!(series.len(), 1);
    assert_eq!(
        series[0].name, "measurements",
        "InfluxDB names this series 'measurements'"
    );
    assert_eq!(
        series[0].columns,
        vec!["name".to_string()],
        "SHOW MEASUREMENTS columns should be ['name']"
    );

    let values: Vec<&str> = series[0]
        .values
        .iter()
        .filter_map(|row| row.first().and_then(|v| v.as_str()))
        .collect();
    assert!(values.contains(&"cpu"));
    assert!(values.contains(&"memory"));
}

#[tokio::test]
async fn show_measurements_sorted_alphabetically() {
    let ctx = TestContext::new_no_chdb().unwrap();
    ctx.metadata.create_database("testdb").await.unwrap();
    ctx.ingestion
        .ingest(
            "testdb",
            None,
            None,
            b"zebra v=1.0 1000000000\nalpha v=2.0 2000000000\nmiddle v=3.0 3000000000",
            WritePayloadFormat::LineProtocol,
        )
        .await
        .unwrap();

    let resp = ctx.query("testdb", "SHOW MEASUREMENTS").await.unwrap();
    let series = &resp.results[0].series.as_ref().unwrap()[0];
    let names: Vec<&str> = series
        .values
        .iter()
        .filter_map(|row| row.first().and_then(|v| v.as_str()))
        .collect();
    let mut sorted = names.clone();
    sorted.sort();
    assert_eq!(
        names, sorted,
        "SHOW MEASUREMENTS should return results in alphabetical order"
    );
}

#[tokio::test]
async fn show_measurements_empty_database() {
    let ctx = TestContext::new_no_chdb().unwrap();
    ctx.metadata.create_database("testdb").await.unwrap();

    let resp = ctx.query("testdb", "SHOW MEASUREMENTS").await.unwrap();
    assert!(resp.results[0].error.is_none());
    let series = resp.results[0].series.as_ref().unwrap();
    assert!(
        series.is_empty() || series[0].values.is_empty(),
        "Empty database should return no measurements"
    );
}

#[tokio::test]
#[serial(chdb)]
async fn show_measurements_filters_by_retention_policy() {
    let ctx = match TestContext::new() {
        Ok(c) => c,
        Err(_) => {
            eprintln!("skipping RP filter test: chDB not available");
            return;
        }
    };

    ctx.metadata.create_database("rpdb").await.unwrap();
    ctx.metadata
        .create_retention_policy(
            "rpdb",
            hyperbytedb::domain::database::RetentionPolicy {
                name: "high".to_string(),
                duration: Some(std::time::Duration::from_secs(7 * 24 * 3600)),
                shard_group_duration: std::time::Duration::from_secs(3600),
                replication_factor: 1,
                is_default: false,
            },
        )
        .await
        .unwrap();

    ctx.write_and_flush("rpdb", "cpu,host=h1 value=1 1700000000000000000")
        .await
        .unwrap();
    ctx.ingestion
        .ingest(
            "rpdb",
            Some("high"),
            None,
            b"rollup,host=h1 value=1 1700000000000000000",
            WritePayloadFormat::LineProtocol,
        )
        .await
        .unwrap();
    ctx.flush_service.flush().await.unwrap();

    let default_names = measurement_names(
        &ctx.query_with_rp("rpdb", "SHOW MEASUREMENTS", None)
            .await
            .unwrap(),
    );
    assert!(default_names.contains(&"cpu".to_string()));
    assert!(!default_names.contains(&"rollup".to_string()));

    let high_names = measurement_names(
        &ctx.query_with_rp("rpdb", "SHOW MEASUREMENTS", Some("high"))
            .await
            .unwrap(),
    );
    assert!(high_names.contains(&"rollup".to_string()));
    assert!(!high_names.contains(&"cpu".to_string()));
}

fn measurement_names(resp: &hyperbytedb::domain::query_result::QueryResponse) -> Vec<String> {
    resp.results
        .first()
        .and_then(|r| r.series.as_ref())
        .map(|series| {
            series
                .iter()
                .flat_map(|s| &s.values)
                .filter_map(|row| row.first().and_then(|v| v.as_str()).map(str::to_string))
                .collect()
        })
        .unwrap_or_default()
}

// ---------------------------------------------------------------------------
// SHOW TAG KEYS
// ---------------------------------------------------------------------------

#[tokio::test]
async fn show_tag_keys_response_shape() {
    let ctx = TestContext::new_no_chdb().unwrap();
    ctx.metadata.create_database("testdb").await.unwrap();
    ctx.ingestion
        .ingest(
            "testdb",
            None,
            None,
            b"cpu,host=srv1,region=us value=42.0 1000000000",
            WritePayloadFormat::LineProtocol,
        )
        .await
        .unwrap();

    let resp = ctx.query("testdb", "SHOW TAG KEYS FROM cpu").await.unwrap();
    assert!(resp.results[0].error.is_none());
    let series = &resp.results[0].series.as_ref().unwrap()[0];
    assert_eq!(
        series.name, "cpu",
        "SHOW TAG KEYS series name should match measurement"
    );
    assert_eq!(
        series.columns,
        vec!["tagKey".to_string()],
        "SHOW TAG KEYS columns should be ['tagKey']"
    );

    let values: Vec<&str> = series
        .values
        .iter()
        .filter_map(|row| row.first().and_then(|v| v.as_str()))
        .collect();
    assert!(values.contains(&"host"));
    assert!(values.contains(&"region"));
}

#[tokio::test]
async fn show_tag_keys_multiple_measurements() {
    let ctx = TestContext::new_no_chdb().unwrap();
    ctx.metadata.create_database("testdb").await.unwrap();
    ctx.ingestion
        .ingest(
            "testdb",
            None,
            None,
            b"cpu,host=srv1 value=1.0 1000000000\nmemory,host=srv1,zone=a value=2.0 2000000000",
            WritePayloadFormat::LineProtocol,
        )
        .await
        .unwrap();

    let resp = ctx.query("testdb", "SHOW TAG KEYS").await.unwrap();
    assert!(resp.results[0].error.is_none());
    let series = resp.results[0].series.as_ref().unwrap();
    assert!(
        !series.is_empty(),
        "SHOW TAG KEYS without FROM should return results"
    );

    let all_keys: Vec<&str> = series
        .iter()
        .flat_map(|s| s.values.iter())
        .filter_map(|row| row.first().and_then(|v| v.as_str()))
        .collect();
    assert!(
        all_keys.contains(&"host"),
        "Should contain 'host' tag key: {:?}",
        all_keys
    );
    assert!(
        all_keys.contains(&"zone"),
        "Should contain 'zone' tag key: {:?}",
        all_keys
    );
}

// ---------------------------------------------------------------------------
// SHOW TAG VALUES
// ---------------------------------------------------------------------------

#[tokio::test]
async fn show_tag_values_response_shape() {
    let ctx = TestContext::new_no_chdb().unwrap();
    ctx.metadata.create_database("testdb").await.unwrap();
    ctx.ingestion
        .ingest(
            "testdb",
            None,
            None,
            b"cpu,host=server01 value=1.0 1000000000\ncpu,host=server02 value=2.0 2000000000",
            WritePayloadFormat::LineProtocol,
        )
        .await
        .unwrap();

    let resp = ctx
        .query("testdb", "SHOW TAG VALUES WITH KEY = \"host\"")
        .await
        .unwrap();
    assert!(resp.results[0].error.is_none());
    let series = &resp.results[0].series.as_ref().unwrap()[0];
    assert_eq!(
        series.columns,
        vec!["key".to_string(), "value".to_string()],
        "SHOW TAG VALUES columns should be ['key', 'value']"
    );

    let values: Vec<&str> = series
        .values
        .iter()
        .filter_map(|row| row.get(1).and_then(|v| v.as_str()))
        .collect();
    assert!(values.contains(&"server01"));
    assert!(values.contains(&"server02"));
}

#[tokio::test]
async fn show_tag_values_visible_after_steady_state_ingest() {
    let ctx = TestContext::new_no_chdb().unwrap();
    ctx.metadata.create_database("testdb").await.unwrap();
    ctx.ingestion
        .ingest(
            "testdb",
            None,
            None,
            b"cpu,host=server01 value=1.0 1000000000",
            WritePayloadFormat::LineProtocol,
        )
        .await
        .unwrap();
    ctx.ingestion
        .ingest(
            "testdb",
            None,
            None,
            b"cpu,host=server02 value=2.0 2000000000",
            WritePayloadFormat::LineProtocol,
        )
        .await
        .unwrap();

    let resp = ctx
        .query("testdb", "SHOW TAG VALUES FROM cpu WITH KEY = \"host\"")
        .await
        .unwrap();
    assert!(resp.results[0].error.is_none());
    let values: Vec<&str> = resp.results[0].series.as_ref().unwrap()[0]
        .values
        .iter()
        .filter_map(|row| row.get(1).and_then(|v| v.as_str()))
        .collect();
    assert!(values.contains(&"server01"));
    assert!(values.contains(&"server02"));
}

#[tokio::test]
async fn show_tag_keys_not_cleared_by_tagless_batch() {
    let ctx = TestContext::new_no_chdb().unwrap();
    ctx.metadata.create_database("testdb").await.unwrap();
    ctx.ingestion
        .ingest(
            "testdb",
            None,
            None,
            b"cpu,host=server01 value=1.0 1000000000",
            WritePayloadFormat::LineProtocol,
        )
        .await
        .unwrap();
    ctx.ingestion
        .ingest(
            "testdb",
            None,
            None,
            b"cpu value=2.0 2000000000",
            WritePayloadFormat::LineProtocol,
        )
        .await
        .unwrap();

    let resp = ctx.query("testdb", "SHOW TAG KEYS FROM cpu").await.unwrap();
    assert!(resp.results[0].error.is_none());
    let keys: Vec<&str> = resp.results[0].series.as_ref().unwrap()[0]
        .values
        .iter()
        .filter_map(|row| row.first().and_then(|v| v.as_str()))
        .collect();
    assert!(
        keys.contains(&"host"),
        "tag keys should survive a tag-less batch: {:?}",
        keys
    );
}

#[tokio::test]
async fn show_tag_values_from_specific_measurement() {
    let ctx = TestContext::new_no_chdb().unwrap();
    ctx.metadata.create_database("testdb").await.unwrap();
    ctx.ingestion
        .ingest(
            "testdb",
            None,
            None,
            b"cpu,host=srv1 value=1.0 1000000000\nmemory,host=srv2 value=2.0 2000000000",
            WritePayloadFormat::LineProtocol,
        )
        .await
        .unwrap();

    let resp = ctx
        .query("testdb", "SHOW TAG VALUES FROM cpu WITH KEY = \"host\"")
        .await
        .unwrap();
    assert!(resp.results[0].error.is_none());
    let series = &resp.results[0].series.as_ref().unwrap()[0];
    assert_eq!(series.name, "cpu");

    let values: Vec<&str> = series
        .values
        .iter()
        .filter_map(|row| row.get(1).and_then(|v| v.as_str()))
        .collect();
    assert!(
        values.contains(&"srv1"),
        "Should contain tag value from cpu"
    );
    assert!(
        !values.contains(&"srv2"),
        "Should not contain tag value from memory measurement"
    );
}

// ---------------------------------------------------------------------------
// SHOW FIELD KEYS
// ---------------------------------------------------------------------------

#[tokio::test]
async fn show_field_keys_response_shape() {
    let ctx = TestContext::new_no_chdb().unwrap();
    ctx.metadata.create_database("testdb").await.unwrap();
    ctx.ingestion
        .ingest(
            "testdb",
            None,
            None,
            b"cpu value=42.0,count=10i 1000000000",
            WritePayloadFormat::LineProtocol,
        )
        .await
        .unwrap();

    let resp = ctx
        .query("testdb", "SHOW FIELD KEYS FROM cpu")
        .await
        .unwrap();
    assert!(resp.results[0].error.is_none());
    let series = &resp.results[0].series.as_ref().unwrap()[0];
    assert_eq!(series.name, "cpu");
    assert_eq!(
        series.columns,
        vec!["fieldKey".to_string(), "fieldType".to_string()],
        "SHOW FIELD KEYS columns should be ['fieldKey', 'fieldType']"
    );

    let fields: Vec<&str> = series
        .values
        .iter()
        .filter_map(|row| row.first().and_then(|v| v.as_str()))
        .collect();
    assert!(fields.contains(&"value"));
    assert!(fields.contains(&"count"));

    let types: Vec<&str> = series
        .values
        .iter()
        .filter_map(|row| row.get(1).and_then(|v| v.as_str()))
        .collect();
    assert!(
        types.contains(&"float"),
        "value field should be type 'float', got: {:?}",
        types
    );
    assert!(
        types.contains(&"integer"),
        "count field should be type 'integer', got: {:?}",
        types
    );
}

// ---------------------------------------------------------------------------
// SHOW SERIES
// ---------------------------------------------------------------------------

#[tokio::test]
async fn show_series_response_shape() {
    let ctx = TestContext::new_no_chdb().unwrap();
    ctx.metadata.create_database("testdb").await.unwrap();
    ctx.ingestion
        .ingest(
            "testdb",
            None,
            None,
            b"cpu,host=srv1 value=1.0 1000000000\nmemory,zone=us value=2.0 2000000000",
            WritePayloadFormat::LineProtocol,
        )
        .await
        .unwrap();

    let resp = ctx.query("testdb", "SHOW SERIES").await.unwrap();
    assert!(resp.results[0].error.is_none());
    let series = &resp.results[0].series.as_ref().unwrap()[0];
    assert_eq!(
        series.columns,
        vec!["key".to_string()],
        "SHOW SERIES columns should be ['key']"
    );

    let keys: Vec<&str> = series
        .values
        .iter()
        .filter_map(|row| row.first().and_then(|v| v.as_str()))
        .collect();
    assert!(!keys.is_empty(), "Should have series keys");
    assert!(
        keys.iter().any(|k| k.contains("cpu")),
        "Should have series key for cpu: {:?}",
        keys
    );
    assert!(
        keys.iter().any(|k| k.contains("memory")),
        "Should have series key for memory: {:?}",
        keys
    );
}

#[tokio::test]
async fn show_series_canonical_format() {
    let ctx = TestContext::new_no_chdb().unwrap();
    ctx.metadata.create_database("testdb").await.unwrap();
    ctx.ingestion
        .ingest(
            "testdb",
            None,
            None,
            b"cpu,host=srv1,region=us value=1.0 1000000000",
            WritePayloadFormat::LineProtocol,
        )
        .await
        .unwrap();

    let resp = ctx.query("testdb", "SHOW SERIES").await.unwrap();
    let series = &resp.results[0].series.as_ref().unwrap()[0];
    let key = series.values[0][0].as_str().unwrap();
    assert!(
        key.starts_with("cpu,"),
        "Series key should start with measurement name: got '{}'",
        key
    );
    assert!(
        key.contains("host") && key.contains("region"),
        "Series key should contain tag keys: got '{}'",
        key
    );
}

// ---------------------------------------------------------------------------
// SHOW ... CARDINALITY
// ---------------------------------------------------------------------------

/// Two measurements, Telegraf-shaped: cpu has 3 hosts x 2 cpus, mem 2 hosts.
async fn cardinality_ctx() -> TestContext {
    let ctx = TestContext::new_no_chdb().unwrap();
    ctx.metadata.create_database("testdb").await.unwrap();
    // Counts come from the series index once it is warmed, as at startup;
    // the writes below then go through it.
    ctx.metadata.warm_series().await.unwrap();
    let mut lp = String::new();
    for host in ["a", "b", "c"] {
        for cpu in ["cpu0", "cpu1"] {
            lp.push_str(&format!(
                "cpu,host={host},cpu={cpu},dc=x usage_idle=1,usage_user=2 1000000000\n"
            ));
        }
    }
    lp.push_str("mem,host=a used=1 1000000000\nmem,host=b used=1 1000000000\n");
    // A second write of a series must not count it twice.
    lp.push_str("cpu,host=a,cpu=cpu0,dc=x usage_idle=3,usage_user=4 2000000000\n");
    ctx.ingestion
        .ingest(
            "testdb",
            None,
            None,
            lp.as_bytes(),
            WritePayloadFormat::LineProtocol,
        )
        .await
        .unwrap();
    ctx
}

/// `(series name, columns, single value)` of each result series.
async fn cardinality_rows(ctx: &TestContext, q: &str) -> Vec<(String, Vec<String>, u64)> {
    let resp = ctx.query("testdb", q).await.unwrap();
    let result = &resp.results[0];
    assert!(result.error.is_none(), "{q}: {:?}", result.error);
    result
        .series
        .as_ref()
        .unwrap()
        .iter()
        .map(|s| {
            assert_eq!(s.values.len(), 1, "{q}: one row per series");
            (
                s.name.clone(),
                s.columns.clone(),
                s.values[0][0].as_u64().unwrap(),
            )
        })
        .collect()
}

fn count(name: &str, n: u64) -> (String, Vec<String>, u64) {
    (name.to_string(), vec!["count".to_string()], n)
}

fn estimate(n: u64) -> (String, Vec<String>, u64) {
    (String::new(), vec!["cardinality estimation".to_string()], n)
}

#[tokio::test]
async fn show_series_cardinality() {
    let ctx = cardinality_ctx().await;
    assert_eq!(
        cardinality_rows(&ctx, "SHOW SERIES CARDINALITY").await,
        vec![estimate(8)]
    );
    assert_eq!(
        cardinality_rows(&ctx, "SHOW SERIES CARDINALITY ON testdb").await,
        vec![estimate(8)]
    );
    assert_eq!(
        cardinality_rows(&ctx, "SHOW SERIES EXACT CARDINALITY").await,
        vec![count("cpu", 6), count("mem", 2)]
    );
    assert_eq!(
        cardinality_rows(&ctx, "SHOW SERIES CARDINALITY FROM cpu").await,
        vec![count("cpu", 6)]
    );
    assert_eq!(
        cardinality_rows(&ctx, r#"SHOW SERIES CARDINALITY WHERE "host" = 'a'"#).await,
        vec![count("cpu", 2), count("mem", 1)]
    );
    // A measurement with no matching series has no row, as in InfluxDB.
    assert_eq!(
        cardinality_rows(&ctx, r#"SHOW SERIES CARDINALITY WHERE "cpu" = 'cpu1'"#).await,
        vec![count("cpu", 3)]
    );
    assert_eq!(
        cardinality_rows(&ctx, "SHOW SERIES EXACT CARDINALITY LIMIT 1 OFFSET 1").await,
        vec![count("mem", 2)]
    );

    // The same numbers as counting SHOW SERIES.
    let resp = ctx.query("testdb", "SHOW SERIES").await.unwrap();
    let listed = resp.results[0].series.as_ref().unwrap()[0].values.len();
    assert_eq!(listed, 8);

    ctx.query("testdb", r#"DROP SERIES FROM "mem""#)
        .await
        .unwrap();
    assert_eq!(
        cardinality_rows(&ctx, "SHOW SERIES CARDINALITY").await,
        vec![estimate(6)]
    );
}

#[tokio::test]
async fn show_measurement_cardinality() {
    let ctx = cardinality_ctx().await;
    assert_eq!(
        cardinality_rows(&ctx, "SHOW MEASUREMENT CARDINALITY").await,
        vec![estimate(2)]
    );
    assert_eq!(
        cardinality_rows(&ctx, "SHOW MEASUREMENT EXACT CARDINALITY").await,
        vec![count("", 2)]
    );
    assert_eq!(
        cardinality_rows(&ctx, "SHOW MEASUREMENT CARDINALITY WITH MEASUREMENT = cpu").await,
        vec![count("", 1)]
    );
    // FROM filters, so the count is no longer the database-wide estimate.
    assert_eq!(
        cardinality_rows(&ctx, "SHOW MEASUREMENT CARDINALITY FROM cpu").await,
        vec![count("", 1)]
    );
    assert_eq!(
        cardinality_rows(&ctx, "SHOW MEASUREMENT CARDINALITY FROM nope").await,
        vec![count("", 0)]
    );
    assert_eq!(
        cardinality_rows(&ctx, r#"SHOW MEASUREMENT CARDINALITY WHERE "cpu" = 'cpu0'"#).await,
        vec![count("", 1)]
    );
}

#[tokio::test]
async fn show_tag_and_field_key_cardinality() {
    let ctx = cardinality_ctx().await;
    assert_eq!(
        cardinality_rows(&ctx, "SHOW TAG KEY CARDINALITY").await,
        vec![count("cpu", 3), count("mem", 1)]
    );
    assert_eq!(
        cardinality_rows(&ctx, "SHOW TAG KEY EXACT CARDINALITY FROM cpu").await,
        vec![count("cpu", 3)]
    );
    assert_eq!(
        cardinality_rows(&ctx, r#"SHOW TAG KEY CARDINALITY WHERE "host" = 'b'"#).await,
        vec![count("cpu", 3), count("mem", 1)]
    );
    assert_eq!(
        cardinality_rows(&ctx, "SHOW FIELD KEY CARDINALITY").await,
        vec![count("cpu", 2), count("mem", 1)]
    );
    let resp = ctx
        .query("testdb", r#"SHOW FIELD KEY CARDINALITY WHERE "host" = 'a'"#)
        .await;
    let rejected = match resp {
        Err(_) => true,
        Ok(r) => r.results[0].error.is_some(),
    };
    assert!(
        rejected,
        "a WHERE on field key cardinality is refused, not ignored"
    );
}

#[tokio::test]
async fn show_tag_values_cardinality() {
    let ctx = cardinality_ctx().await;
    assert_eq!(
        cardinality_rows(&ctx, r#"SHOW TAG VALUES CARDINALITY WITH KEY = "host""#).await,
        vec![count("cpu", 3), count("mem", 2)]
    );
    assert_eq!(
        cardinality_rows(
            &ctx,
            r#"SHOW TAG VALUES EXACT CARDINALITY FROM cpu WITH KEY IN ("host", "cpu")"#
        )
        .await,
        vec![count("cpu", 5)]
    );
    assert_eq!(
        cardinality_rows(
            &ctx,
            r#"SHOW TAG VALUES CARDINALITY FROM cpu WITH KEY = "cpu" WHERE "host" = 'a'"#
        )
        .await,
        vec![count("cpu", 2)]
    );
    // The same number as counting SHOW TAG VALUES.
    let resp = ctx
        .query(
            "testdb",
            r#"SHOW TAG VALUES FROM cpu WITH KEY IN ("host", "cpu")"#,
        )
        .await
        .unwrap();
    assert_eq!(resp.results[0].series.as_ref().unwrap()[0].values.len(), 5);
}

// ---------------------------------------------------------------------------
// SHOW RETENTION POLICIES
// ---------------------------------------------------------------------------

#[tokio::test]
async fn show_retention_policies_response_shape() {
    let ctx = TestContext::new_no_chdb().unwrap();
    ctx.metadata.create_database("testdb").await.unwrap();

    let resp = ctx
        .query("testdb", "SHOW RETENTION POLICIES ON testdb")
        .await
        .unwrap();
    assert!(resp.results[0].error.is_none());
    let series = &resp.results[0].series.as_ref().unwrap()[0];

    assert!(
        series.columns.contains(&"name".to_string()),
        "SHOW RETENTION POLICIES should have 'name' column: {:?}",
        series.columns
    );
    assert!(
        series.columns.contains(&"duration".to_string()),
        "SHOW RETENTION POLICIES should have 'duration' column: {:?}",
        series.columns
    );
    assert!(
        series.columns.contains(&"default".to_string()),
        "SHOW RETENTION POLICIES should have 'default' column: {:?}",
        series.columns
    );

    let values: Vec<&str> = series
        .values
        .iter()
        .filter_map(|row| row.first().and_then(|v| v.as_str()))
        .collect();
    assert!(
        values.contains(&"autogen"),
        "Default 'autogen' RP should exist"
    );
}

#[tokio::test]
async fn show_retention_policies_includes_custom_rp() {
    let ctx = TestContext::new_no_chdb().unwrap();
    ctx.metadata.create_database("testdb").await.unwrap();
    ctx.metadata
        .create_retention_policy(
            "testdb",
            hyperbytedb::domain::database::RetentionPolicy {
                name: "oneweek".to_string(),
                duration: Some(std::time::Duration::from_secs(7 * 24 * 3600)),
                shard_group_duration: std::time::Duration::from_secs(3600),
                replication_factor: 1,
                is_default: false,
            },
        )
        .await
        .unwrap();

    let resp = ctx
        .query("testdb", "SHOW RETENTION POLICIES ON testdb")
        .await
        .unwrap();
    let series = &resp.results[0].series.as_ref().unwrap()[0];
    let names: Vec<&str> = series
        .values
        .iter()
        .filter_map(|row| row.first().and_then(|v| v.as_str()))
        .collect();
    assert!(names.contains(&"autogen"));
    assert!(names.contains(&"oneweek"));
}

// ---------------------------------------------------------------------------
// SHOW CONTINUOUS QUERIES
// ---------------------------------------------------------------------------

#[tokio::test]
async fn show_continuous_queries_no_error() {
    let ctx = TestContext::new_no_chdb().unwrap();
    ctx.metadata.create_database("testdb").await.unwrap();

    let resp = ctx
        .query("testdb", "SHOW CONTINUOUS QUERIES")
        .await
        .unwrap();
    assert!(
        resp.results[0].error.is_none(),
        "SHOW CONTINUOUS QUERIES should succeed even with none defined"
    );
}

#[tokio::test]
async fn show_continuous_queries_after_create() {
    let ctx = TestContext::new_no_chdb().unwrap();
    ctx.metadata.create_database("testdb").await.unwrap();
    ctx.metadata
        .store_continuous_query(
            "testdb",
            "cq_test",
            &hyperbytedb::ports::metadata::ContinuousQueryDef {
                name: "cq_test".to_string(),
                database: "testdb".to_string(),
                query_text: "SELECT mean(value) INTO downsampled FROM cpu GROUP BY time(1h)"
                    .to_string(),
                resample_every_secs: None,
                resample_for_secs: None,
                created_at: chrono::Utc::now().to_rfc3339(),
                group_by_interval_secs: 3600,
                group_by_offset_secs: 0,
                execution_interval_secs: 3600,
                coverage_interval_secs: 3600,
                is_advanced: false,
                last_run_at: None,
                influxql_names: false,
            },
        )
        .await
        .unwrap();

    let resp = ctx
        .query("testdb", "SHOW CONTINUOUS QUERIES")
        .await
        .unwrap();
    assert!(resp.results[0].error.is_none());
}
