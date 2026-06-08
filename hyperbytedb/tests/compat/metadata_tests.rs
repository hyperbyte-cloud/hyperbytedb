//! Metadata query compatibility tests.
//!
//! Verifies response shapes and content for SHOW DATABASES, SHOW MEASUREMENTS,
//! SHOW TAG KEYS, SHOW TAG VALUES, SHOW FIELD KEYS, SHOW SERIES,
//! SHOW RETENTION POLICIES, and SHOW CONTINUOUS QUERIES.

use hyperbytedb::ports::ingestion::{IngestionPort, WritePayloadFormat};

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
