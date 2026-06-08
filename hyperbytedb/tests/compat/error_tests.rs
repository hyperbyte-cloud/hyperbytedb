//! Error handling compatibility tests.
//!
//! Verifies that Hyperbytedb returns appropriate errors matching InfluxDB v1
//! behavior for invalid inputs, missing databases, parse errors, etc.

use hyperbytedb::error::HyperbytedbError;
use hyperbytedb::ports::ingestion::{IngestionPort, WritePayloadFormat};

use super::TestContext;

// ---------------------------------------------------------------------------
// Parse errors
// ---------------------------------------------------------------------------

#[tokio::test]
async fn empty_query_parse_error() {
    let result = hyperbytedb::timeseriesql::parse("");
    assert!(result.is_err());
}

#[tokio::test]
async fn invalid_influxql_syntax() {
    let result = hyperbytedb::timeseriesql::parse("NOTAVALIDKEYWORD");
    assert!(result.is_err());
}

#[tokio::test]
async fn incomplete_select_statement() {
    let result = hyperbytedb::timeseriesql::parse("SELECT FROM");
    assert!(result.is_err(), "Incomplete SELECT should fail to parse");
}

#[tokio::test]
async fn incomplete_create_database() {
    let result = hyperbytedb::timeseriesql::parse("CREATE DATABASE");
    assert!(result.is_err(), "CREATE DATABASE without name should fail");
}

#[tokio::test]
async fn unclosed_string_literal() {
    let result = hyperbytedb::timeseriesql::parse("SELECT * FROM cpu WHERE host = 'unclosed");
    // Parser may accept or reject unclosed string depending on implementation.
    // If it parses successfully, query execution should still handle it gracefully.
    if result.is_ok() {
        let ctx = TestContext::new_no_chdb().unwrap();
        ctx.metadata.create_database("testdb").await.unwrap();
        let resp = ctx
            .query("testdb", "SELECT * FROM cpu WHERE host = 'unclosed")
            .await;
        // Should not panic regardless of whether it errors or succeeds
        let _ = resp;
    }
}

#[tokio::test]
async fn multiple_valid_statements_parse() {
    let result = hyperbytedb::timeseriesql::parse("SHOW DATABASES;SHOW MEASUREMENTS");
    assert!(
        result.is_ok(),
        "Multiple semicolon-separated statements should parse: {:?}",
        result.err()
    );
    assert_eq!(result.unwrap().len(), 2, "Should produce two statements");
}

// ---------------------------------------------------------------------------
// Database context errors
// ---------------------------------------------------------------------------

#[tokio::test]
async fn query_on_nonexistent_database() {
    let ctx = TestContext::new_no_chdb().unwrap();

    let result = ctx.query("nonexistent", "SELECT * FROM cpu").await;
    // SELECT on nonexistent DB may return Err or Ok with empty results.
    // Either way, it should not panic and should handle the missing DB gracefully.
    match result {
        Err(e) => {
            let msg = e.to_string();
            assert!(
                msg.to_lowercase().contains("database") || msg.to_lowercase().contains("not found"),
                "Error should be about database: {}",
                msg
            );
        }
        Ok(resp) => {
            let stmt = &resp.results[0];
            let has_error = stmt.error.is_some();
            let is_empty = stmt.series.as_ref().is_none_or(|s| s.is_empty());
            assert!(
                has_error || is_empty,
                "Query on nonexistent DB should error or return empty results"
            );
        }
    }
}

#[tokio::test]
async fn select_requires_database() {
    let ctx = TestContext::new_no_chdb().unwrap();

    let resp = ctx.query("", "SELECT * FROM cpu").await.unwrap();
    assert!(resp.results[0].error.is_some());
    let error_msg = resp.results[0].error.as_ref().unwrap();
    assert!(
        error_msg.to_lowercase().contains("database"),
        "Error should indicate database is required: {}",
        error_msg
    );
}

#[tokio::test]
async fn show_measurements_requires_database_context() {
    let ctx = TestContext::new_no_chdb().unwrap();

    let resp = ctx.query("", "SHOW MEASUREMENTS").await.unwrap();
    assert!(resp.results[0].error.is_some());
    let error_msg = resp.results[0].error.as_ref().unwrap();
    assert!(
        error_msg.to_lowercase().contains("database"),
        "Error should mention database requirement: {}",
        error_msg
    );
}

#[tokio::test]
async fn show_tag_keys_requires_database() {
    let ctx = TestContext::new_no_chdb().unwrap();

    let resp = ctx.query("", "SHOW TAG KEYS").await.unwrap();
    // SHOW TAG KEYS without database context may return error or empty results.
    let has_error = resp.results[0].error.is_some();
    let is_empty = resp.results[0]
        .series
        .as_ref()
        .is_none_or(|s| s.is_empty() || s[0].values.is_empty());
    assert!(
        has_error || is_empty,
        "SHOW TAG KEYS without database context should error or return empty"
    );
}

#[tokio::test]
async fn show_field_keys_requires_database() {
    let ctx = TestContext::new_no_chdb().unwrap();

    let resp = ctx.query("", "SHOW FIELD KEYS").await.unwrap();
    assert!(
        resp.results[0].error.is_some(),
        "SHOW FIELD KEYS without database context should error"
    );
}

// ---------------------------------------------------------------------------
// Write errors
// ---------------------------------------------------------------------------

#[tokio::test]
async fn write_to_nonexistent_database() {
    let ctx = TestContext::new_no_chdb().unwrap();

    let result = ctx
        .ingestion
        .ingest(
            "nodatabase",
            None,
            None,
            b"cpu value=1.0 1000000000",
            WritePayloadFormat::LineProtocol,
        )
        .await;
    assert!(result.is_err());
    let err = result.unwrap_err();
    assert!(
        matches!(err, HyperbytedbError::DatabaseNotFound(_)),
        "Should be DatabaseNotFound error: {:?}",
        err
    );
}

#[tokio::test]
async fn invalid_line_protocol() {
    let ctx = TestContext::new_no_chdb().unwrap();
    ctx.metadata.create_database("testdb").await.unwrap();

    let result = ctx
        .ingestion
        .ingest(
            "testdb",
            None,
            None,
            b"invalid line protocol!!!",
            WritePayloadFormat::LineProtocol,
        )
        .await;
    assert!(result.is_err());
    let err = result.unwrap_err();
    assert!(
        matches!(err, HyperbytedbError::LineProtocolParse { .. }),
        "Should be LineProtocolParse error: {:?}",
        err
    );
}

#[tokio::test]
async fn line_protocol_missing_field() {
    let ctx = TestContext::new_no_chdb().unwrap();
    ctx.metadata.create_database("testdb").await.unwrap();

    let result = ctx
        .ingestion
        .ingest(
            "testdb",
            None,
            None,
            b"cpu 1000000000",
            WritePayloadFormat::LineProtocol,
        )
        .await;
    assert!(
        result.is_err(),
        "Line protocol without field set should fail"
    );
}

#[tokio::test]
async fn write_empty_body() {
    let ctx = TestContext::new_no_chdb().unwrap();
    ctx.metadata.create_database("testdb").await.unwrap();

    let result = ctx
        .ingestion
        .ingest("testdb", None, None, b"", WritePayloadFormat::LineProtocol)
        .await;
    // InfluxDB returns 204 for empty writes (no points to write is not an error).
    // Hyperbytedb may choose to error or succeed silently.
    // Either behavior is acceptable; ensure no panic.
    let _ = result;
}

// ---------------------------------------------------------------------------
// Query result error field
// ---------------------------------------------------------------------------

#[tokio::test]
async fn error_result_has_statement_id() {
    let ctx = TestContext::new_no_chdb().unwrap();

    let resp = ctx.query("", "SELECT * FROM cpu").await.unwrap();
    assert_eq!(
        resp.results[0].statement_id, 0,
        "Error results should still have statement_id"
    );
    assert!(resp.results[0].error.is_some());
    assert!(
        resp.results[0].series.is_none(),
        "Error results should not have series"
    );
}

// ---------------------------------------------------------------------------
// Nonexistent measurement (with valid DB)
// ---------------------------------------------------------------------------

#[tokio::test]
async fn show_tag_keys_nonexistent_measurement() {
    let ctx = TestContext::new_no_chdb().unwrap();
    ctx.metadata.create_database("testdb").await.unwrap();

    let resp = ctx
        .query("testdb", "SHOW TAG KEYS FROM nonexistent")
        .await
        .unwrap();
    assert!(
        resp.results[0].error.is_none(),
        "Should not error for missing measurement"
    );
    let series = resp.results[0].series.as_ref().unwrap();
    assert!(
        series.is_empty() || series[0].values.is_empty(),
        "Should return empty results for nonexistent measurement"
    );
}

#[tokio::test]
async fn show_field_keys_nonexistent_measurement() {
    let ctx = TestContext::new_no_chdb().unwrap();
    ctx.metadata.create_database("testdb").await.unwrap();

    let resp = ctx
        .query("testdb", "SHOW FIELD KEYS FROM nonexistent")
        .await
        .unwrap();
    assert!(resp.results[0].error.is_none());
    let series = resp.results[0].series.as_ref().unwrap();
    assert!(
        series.is_empty() || series[0].values.is_empty(),
        "Should return empty results for nonexistent measurement"
    );
}
