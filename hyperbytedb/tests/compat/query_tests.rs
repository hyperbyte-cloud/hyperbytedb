//! Query response compatibility tests.
//!
//! Verifies that SELECT queries return results matching the InfluxDB v1 JSON
//! response format: `{ "results": [{ "statement_id": 0, "series": [...] }] }`.
//!
//! Most SELECT tests require chDB and are marked `#[serial(chdb)]` (chDB exposes
//! one process-global server, so concurrent sessions must be serialized).

use hyperbytedb::adapters::http::router::QueryService;
use serial_test::serial;

use super::TestContext;

// ---------------------------------------------------------------------------
// Response shape
// ---------------------------------------------------------------------------

#[tokio::test]
#[serial(chdb)]
async fn response_has_correct_top_level_shape() {
    let ctx = TestContext::new().unwrap();
    ctx.metadata.create_database("testdb").await.unwrap();
    ctx.write_and_flush("testdb", "cpu value=1.0 1000000000")
        .await
        .unwrap();

    let resp = ctx.query("testdb", "SELECT * FROM cpu").await.unwrap();

    assert_eq!(
        resp.results.len(),
        1,
        "Single statement should produce one result"
    );
    assert_eq!(resp.results[0].statement_id, 0);
    assert!(resp.results[0].error.is_none());
    assert!(resp.results[0].series.is_some());
}

#[tokio::test]
#[serial(chdb)]
async fn series_result_has_name_columns_values() {
    let ctx = TestContext::new().unwrap();
    ctx.metadata.create_database("testdb").await.unwrap();
    ctx.write_and_flush("testdb", "cpu,host=srv1 value=42.0 1000000000")
        .await
        .unwrap();

    let resp = ctx.query("testdb", "SELECT * FROM cpu").await.unwrap();
    let series = &resp.results[0].series.as_ref().unwrap()[0];

    assert_eq!(series.name, "cpu", "Series name should match measurement");
    assert!(
        series.columns.contains(&"time".to_string()),
        "Columns should include 'time'"
    );
    assert!(
        series.columns.contains(&"value".to_string()),
        "Columns should include field 'value'"
    );
    assert!(!series.values.is_empty(), "Values should not be empty");
    assert_eq!(
        series.values[0].len(),
        series.columns.len(),
        "Each row should have same length as columns"
    );
}

// ---------------------------------------------------------------------------
// SELECT basics
// ---------------------------------------------------------------------------

#[tokio::test]
#[serial(chdb)]
async fn select_star_from_measurement() {
    let ctx = TestContext::new().unwrap();
    ctx.metadata.create_database("testdb").await.unwrap();
    ctx.write_and_flush(
        "testdb",
        "cpu,host=srv1 value=42.0 1000000000\ncpu,host=srv1 value=43.0 2000000000",
    )
    .await
    .unwrap();

    let resp = ctx.query("testdb", "SELECT * FROM cpu").await.unwrap();
    assert!(!resp.results.is_empty());
    let stmt = &resp.results[0];
    assert!(stmt.error.is_none());
    let series = stmt.series.as_ref().unwrap();
    assert!(!series.is_empty());
    assert!(series[0].columns.contains(&"time".to_string()));
    assert!(series[0].columns.contains(&"value".to_string()));
    assert_eq!(series[0].values.len(), 2);
}

#[tokio::test]
#[serial(chdb)]
async fn select_specific_fields() {
    let ctx = TestContext::new().unwrap();
    ctx.metadata.create_database("testdb").await.unwrap();
    ctx.write_and_flush(
        "testdb",
        "cpu value=42.0,count=10i 1000000000\ncpu value=43.0,count=11i 2000000000",
    )
    .await
    .unwrap();

    let resp = ctx.query("testdb", "SELECT value FROM cpu").await.unwrap();
    let series = &resp.results[0].series.as_ref().unwrap()[0];
    assert!(
        series.columns.contains(&"time".to_string()),
        "SELECT should always include time"
    );
    assert!(
        series.columns.contains(&"value".to_string()),
        "Selected field should be in columns"
    );
    assert!(
        !series.columns.contains(&"count".to_string()),
        "Non-selected field should not appear"
    );
}

// ---------------------------------------------------------------------------
// Aggregate functions
// ---------------------------------------------------------------------------

#[tokio::test]
#[serial(chdb)]
async fn aggregate_mean() {
    let ctx = TestContext::new().unwrap();
    ctx.metadata.create_database("testdb").await.unwrap();
    ctx.write_and_flush(
        "testdb",
        "cpu value=10.0 1000000000\ncpu value=20.0 2000000000\ncpu value=30.0 3000000000",
    )
    .await
    .unwrap();

    let resp = ctx
        .query("testdb", "SELECT mean(value) FROM cpu")
        .await
        .unwrap();
    let series = &resp.results[0].series.as_ref().unwrap()[0];
    assert!(!series.values.is_empty());
    let mean_val = series.values[0]
        .iter()
        .find_map(|v| v.as_f64())
        .expect("mean should return a float");
    assert!(
        (mean_val - 20.0).abs() < 0.01,
        "mean(10, 20, 30) should be 20.0, got {}",
        mean_val
    );
}

#[tokio::test]
#[serial(chdb)]
async fn aggregate_count() {
    let ctx = TestContext::new().unwrap();
    ctx.metadata.create_database("testdb").await.unwrap();
    ctx.write_and_flush(
        "testdb",
        "cpu value=1.0 1000000000\ncpu value=2.0 2000000000",
    )
    .await
    .unwrap();

    let resp = ctx
        .query("testdb", "SELECT count(value) FROM cpu")
        .await
        .unwrap();
    let series = &resp.results[0].series.as_ref().unwrap()[0];
    let count_val = series.values[0]
        .iter()
        .find_map(|v| v.as_i64().or_else(|| v.as_f64().map(|f| f as i64)))
        .expect("count should return a number");
    assert_eq!(count_val, 2, "count of 2 points should be 2");
}

#[tokio::test]
#[serial(chdb)]
async fn aggregate_sum_min_max() {
    let ctx = TestContext::new().unwrap();
    ctx.metadata.create_database("testdb").await.unwrap();
    ctx.write_and_flush(
        "testdb",
        "cpu value=1.0 1000000000\ncpu value=2.0 2000000000\ncpu value=3.0 3000000000",
    )
    .await
    .unwrap();

    let resp = ctx
        .query(
            "testdb",
            "SELECT sum(value), min(value), max(value) FROM cpu",
        )
        .await
        .unwrap();
    let series = &resp.results[0].series.as_ref().unwrap()[0];
    assert!(!series.values.is_empty());

    let row = &series.values[0];
    let floats: Vec<f64> = row.iter().filter_map(|v| v.as_f64()).collect();
    assert!(
        floats.len() >= 3,
        "Expected at least 3 numeric values: {:?}",
        row
    );
    assert!(floats.contains(&6.0), "sum(1,2,3) should be 6.0");
    assert!(floats.contains(&1.0), "min(1,2,3) should be 1.0");
    assert!(floats.contains(&3.0), "max(1,2,3) should be 3.0");
}

#[tokio::test]
#[serial(chdb)]
async fn aggregate_first_last() {
    let ctx = TestContext::new().unwrap();
    ctx.metadata.create_database("testdb").await.unwrap();
    ctx.write_and_flush(
        "testdb",
        "cpu value=1.0 1000000000\ncpu value=2.0 2000000000\ncpu value=3.0 3000000000",
    )
    .await
    .unwrap();

    let resp = ctx
        .query("testdb", "SELECT first(value), last(value) FROM cpu")
        .await
        .unwrap();
    let series = &resp.results[0].series.as_ref().unwrap()[0];
    let row = &series.values[0];
    let floats: Vec<f64> = row.iter().filter_map(|v| v.as_f64()).collect();
    assert!(floats.contains(&1.0), "first(value) should be 1.0");
    assert!(floats.contains(&3.0), "last(value) should be 3.0");
}

#[tokio::test]
#[serial(chdb)]
async fn aggregate_median() {
    let ctx = TestContext::new().unwrap();
    ctx.metadata.create_database("testdb").await.unwrap();
    ctx.write_and_flush(
        "testdb",
        "cpu value=1.0 1000000000\ncpu value=2.0 2000000000\ncpu value=3.0 3000000000",
    )
    .await
    .unwrap();

    let resp = ctx
        .query("testdb", "SELECT median(value) FROM cpu")
        .await
        .unwrap();
    assert!(resp.results[0].error.is_none());
    let series = &resp.results[0].series.as_ref().unwrap()[0];
    let median = series.values[0]
        .iter()
        .find_map(|v| v.as_f64())
        .expect("median should return a float");
    assert!(
        (median - 2.0).abs() < 0.01,
        "median(1, 2, 3) should be 2.0, got {}",
        median
    );
}

#[tokio::test]
#[serial(chdb)]
async fn aggregate_spread() {
    let ctx = TestContext::new().unwrap();
    ctx.metadata.create_database("testdb").await.unwrap();
    ctx.write_and_flush(
        "testdb",
        "cpu value=10.0 1000000000\ncpu value=30.0 2000000000",
    )
    .await
    .unwrap();

    let resp = ctx
        .query("testdb", "SELECT spread(value) FROM cpu")
        .await
        .unwrap();
    assert!(resp.results[0].error.is_none());
    let series = &resp.results[0].series.as_ref().unwrap()[0];
    let spread = series.values[0]
        .iter()
        .find_map(|v| v.as_f64())
        .expect("spread should return a float");
    assert!(
        (spread - 20.0).abs() < 0.01,
        "spread(10, 30) = max-min = 20.0, got {}",
        spread
    );
}

// ---------------------------------------------------------------------------
// GROUP BY
// ---------------------------------------------------------------------------

#[tokio::test]
#[serial(chdb)]
async fn group_by_time() {
    let ctx = TestContext::new().unwrap();
    ctx.metadata.create_database("testdb").await.unwrap();
    ctx.write_and_flush(
        "testdb",
        "cpu value=1.0 1000000000\ncpu value=2.0 2000000000\ncpu value=3.0 3000000000",
    )
    .await
    .unwrap();

    let resp = ctx
        .query("testdb", "SELECT mean(value) FROM cpu GROUP BY time(1s)")
        .await
        .unwrap();
    let series = &resp.results[0].series.as_ref().unwrap();
    assert!(!series.is_empty());
    assert!(
        series[0].columns.contains(&"time".to_string()),
        "GROUP BY time should include time column"
    );
    assert!(
        series[0].values.len() >= 3,
        "Each second bucket should produce a row"
    );
}

#[tokio::test]
#[serial(chdb)]
async fn group_by_time_and_tag() {
    let ctx = TestContext::new().unwrap();
    ctx.metadata.create_database("testdb").await.unwrap();
    ctx.write_and_flush(
        "testdb",
        "cpu,host=a value=1.0 1000000000\ncpu,host=b value=2.0 1000000000\ncpu,host=a value=3.0 2000000000",
    )
    .await
    .unwrap();

    let resp = ctx
        .query(
            "testdb",
            "SELECT mean(value) FROM cpu GROUP BY time(1s), host",
        )
        .await
        .unwrap();
    let series_list = resp.results[0].series.as_ref().unwrap();
    assert!(
        series_list.len() >= 2,
        "GROUP BY tag should split into separate series per tag value, got {} series",
        series_list.len()
    );

    let tags_present: Vec<_> = series_list
        .iter()
        .filter_map(|s| s.tags.as_ref().and_then(|t| t.get("host")))
        .collect();
    assert!(
        tags_present.iter().any(|v| v.as_str() == "a"),
        "Should have series for host=a"
    );
    assert!(
        tags_present.iter().any(|v| v.as_str() == "b"),
        "Should have series for host=b"
    );
}

#[tokio::test]
#[serial(chdb)]
async fn group_by_tag_only() {
    let ctx = TestContext::new().unwrap();
    ctx.metadata.create_database("testdb").await.unwrap();
    ctx.write_and_flush(
        "testdb",
        "cpu,host=a value=10.0 1000000000\ncpu,host=a value=20.0 2000000000\ncpu,host=b value=30.0 1000000000",
    )
    .await
    .unwrap();

    let resp = ctx
        .query("testdb", "SELECT mean(value) FROM cpu GROUP BY host")
        .await
        .unwrap();
    let series_list = resp.results[0].series.as_ref().unwrap();
    assert!(
        series_list.len() >= 2,
        "GROUP BY tag alone should still split series"
    );
}

// ---------------------------------------------------------------------------
// FILL modes
// ---------------------------------------------------------------------------

#[tokio::test]
#[serial(chdb)]
async fn fill_null() {
    let ctx = TestContext::new().unwrap();
    ctx.metadata.create_database("testdb").await.unwrap();
    ctx.write_and_flush("testdb", "cpu value=1.0 1000000000")
        .await
        .unwrap();

    let resp = ctx
        .query(
            "testdb",
            "SELECT mean(value) FROM cpu GROUP BY time(1s) FILL(null)",
        )
        .await
        .unwrap();
    assert!(resp.results[0].error.is_none());
}

#[tokio::test]
#[serial(chdb)]
async fn fill_none() {
    let ctx = TestContext::new().unwrap();
    ctx.metadata.create_database("testdb").await.unwrap();
    ctx.write_and_flush("testdb", "cpu value=1.0 1000000000")
        .await
        .unwrap();

    let resp = ctx
        .query(
            "testdb",
            "SELECT mean(value) FROM cpu GROUP BY time(1s) FILL(none)",
        )
        .await
        .unwrap();
    assert!(resp.results[0].error.is_none());
}

#[tokio::test]
#[serial(chdb)]
async fn fill_zero() {
    let ctx = TestContext::new().unwrap();
    ctx.metadata.create_database("testdb").await.unwrap();
    ctx.write_and_flush("testdb", "cpu value=1.0 1000000000")
        .await
        .unwrap();

    let resp = ctx
        .query(
            "testdb",
            "SELECT mean(value) FROM cpu GROUP BY time(1s) FILL(0)",
        )
        .await
        .unwrap();
    assert!(resp.results[0].error.is_none());
}

#[tokio::test]
#[serial(chdb)]
async fn fill_previous() {
    let ctx = TestContext::new().unwrap();
    ctx.metadata.create_database("testdb").await.unwrap();
    ctx.write_and_flush("testdb", "cpu value=1.0 1000000000")
        .await
        .unwrap();

    let resp = ctx
        .query(
            "testdb",
            "SELECT mean(value) FROM cpu GROUP BY time(1s) FILL(previous)",
        )
        .await
        .unwrap();
    assert!(resp.results[0].error.is_none());
}

#[tokio::test]
#[serial(chdb)]
async fn fill_linear() {
    let ctx = TestContext::new().unwrap();
    ctx.metadata.create_database("testdb").await.unwrap();
    ctx.write_and_flush(
        "testdb",
        "cpu value=10.0 1000000000\ncpu value=30.0 3000000000",
    )
    .await
    .unwrap();

    let resp = ctx
        .query(
            "testdb",
            "SELECT mean(value) FROM cpu WHERE time >= 1000000000 AND time <= 3000000000 GROUP BY time(1s) FILL(linear)",
        )
        .await
        .unwrap();
    assert!(
        resp.results[0].error.is_none(),
        "FILL(linear) should be supported: {:?}",
        resp.results[0].error
    );
}

// ---------------------------------------------------------------------------
// WHERE clauses
// ---------------------------------------------------------------------------

#[tokio::test]
#[serial(chdb)]
async fn where_time_range() {
    let ctx = TestContext::new().unwrap();
    ctx.metadata.create_database("testdb").await.unwrap();
    ctx.write_and_flush(
        "testdb",
        "cpu value=1.0 1000000000\ncpu value=2.0 2000000000\ncpu value=3.0 3000000000",
    )
    .await
    .unwrap();

    let resp = ctx
        .query(
            "testdb",
            "SELECT * FROM cpu WHERE time >= 1000000000 AND time < 3000000000",
        )
        .await
        .unwrap();
    assert!(resp.results[0].error.is_none());
    let series = resp.results[0].series.as_ref().unwrap();
    assert!(!series.is_empty());
    assert!(
        series[0].values.len() <= 2,
        "WHERE time filter should restrict results"
    );
}

#[tokio::test]
#[serial(chdb)]
async fn where_tag_condition() {
    let ctx = TestContext::new().unwrap();
    ctx.metadata.create_database("testdb").await.unwrap();
    ctx.write_and_flush(
        "testdb",
        "cpu,host=a value=1.0 1000000000\ncpu,host=b value=2.0 2000000000",
    )
    .await
    .unwrap();

    let resp = ctx
        .query("testdb", "SELECT * FROM cpu WHERE host = 'a'")
        .await
        .unwrap();
    assert!(resp.results[0].error.is_none());
    let series = resp.results[0].series.as_ref().unwrap();
    assert_eq!(
        series[0].values.len(),
        1,
        "WHERE host='a' should return only 1 row"
    );
}

#[tokio::test]
#[serial(chdb)]
async fn where_tag_regex() {
    let ctx = TestContext::new().unwrap();
    ctx.metadata.create_database("testdb").await.unwrap();
    ctx.write_and_flush(
        "testdb",
        "cpu,host=server01 value=1.0 1000000000\ncpu,host=server02 value=2.0 2000000000\ncpu,host=client01 value=3.0 3000000000",
    )
    .await
    .unwrap();

    let resp = ctx
        .query("testdb", "SELECT * FROM cpu WHERE host =~ /^server/")
        .await
        .unwrap();
    assert!(resp.results[0].error.is_none());
    let series = resp.results[0].series.as_ref().unwrap();
    assert_eq!(
        series[0].values.len(),
        2,
        "Regex filter should match only server01/server02"
    );
}

// ---------------------------------------------------------------------------
// Regex measurement matching
// ---------------------------------------------------------------------------

#[tokio::test]
#[serial(chdb)]
async fn regex_measurement_matching() {
    let ctx = TestContext::new().unwrap();
    ctx.metadata.create_database("testdb").await.unwrap();
    ctx.write_and_flush(
        "testdb",
        "cpu_a value=1.0 1000000000\ncpu_b value=2.0 2000000000",
    )
    .await
    .unwrap();

    let resp = ctx.query("testdb", "SELECT * FROM /^cpu_/").await.unwrap();
    assert!(resp.results[0].error.is_none());
    let series = resp.results[0].series.as_ref().unwrap();
    let names: Vec<&str> = series.iter().map(|s| s.name.as_str()).collect();
    assert!(
        names.contains(&"cpu_a") || names.contains(&"cpu_b"),
        "Regex measurement should match cpu_a and/or cpu_b, got: {:?}",
        names
    );
}

// ---------------------------------------------------------------------------
// Arithmetic expressions
// ---------------------------------------------------------------------------

#[tokio::test]
#[serial(chdb)]
async fn arithmetic_expression() {
    let ctx = TestContext::new().unwrap();
    ctx.metadata.create_database("testdb").await.unwrap();
    ctx.write_and_flush("testdb", "cpu x=10.0 1000000000\ncpu x=20.0 2000000000")
        .await
        .unwrap();

    let resp = ctx
        .query("testdb", "SELECT mean(\"x\") * -1 + 100 FROM cpu")
        .await
        .unwrap();
    assert!(resp.results[0].error.is_none());
    let series = &resp.results[0].series.as_ref().unwrap()[0];
    let val = series.values[0]
        .iter()
        .find_map(|v| v.as_f64())
        .expect("arithmetic expression should return a float");
    assert!(
        (val - 85.0).abs() < 0.01,
        "mean(10, 20) * -1 + 100 = -15 + 100 = 85.0, got {}",
        val
    );
}

// ---------------------------------------------------------------------------
// Nested aggregates / transforms
// ---------------------------------------------------------------------------

#[tokio::test]
#[serial(chdb)]
async fn nested_aggregate_non_negative_derivative() {
    let ctx = TestContext::new().unwrap();
    ctx.metadata.create_database("testdb").await.unwrap();
    ctx.write_and_flush(
        "testdb",
        "cpu x=100 1000000000\ncpu x=200 2000000000\ncpu x=350 3000000000",
    )
    .await
    .unwrap();

    let resp = ctx
        .query(
            "testdb",
            "SELECT non_negative_derivative(mean(\"x\"), 1s) FROM cpu GROUP BY time(1s)",
        )
        .await
        .unwrap();
    assert!(
        resp.results[0].error.is_none(),
        "non_negative_derivative(mean(...)) should work: {:?}",
        resp.results[0].error
    );
}

// ---------------------------------------------------------------------------
// LIMIT / OFFSET
// ---------------------------------------------------------------------------

#[tokio::test]
#[serial(chdb)]
async fn limit_offset() {
    let ctx = TestContext::new().unwrap();
    ctx.metadata.create_database("testdb").await.unwrap();
    ctx.write_and_flush(
        "testdb",
        "cpu value=1.0 1000000000\ncpu value=2.0 2000000000\ncpu value=3.0 3000000000",
    )
    .await
    .unwrap();

    let resp = ctx
        .query("testdb", "SELECT * FROM cpu LIMIT 2 OFFSET 1")
        .await
        .unwrap();
    assert!(resp.results[0].error.is_none());
    let series = resp.results[0].series.as_ref().unwrap();
    assert!(
        series[0].values.len() <= 2,
        "LIMIT 2 should return at most 2 rows"
    );
}

#[tokio::test]
#[serial(chdb)]
async fn limit_only() {
    let ctx = TestContext::new().unwrap();
    ctx.metadata.create_database("testdb").await.unwrap();
    ctx.write_and_flush(
        "testdb",
        "cpu value=1.0 1000000000\ncpu value=2.0 2000000000\ncpu value=3.0 3000000000\ncpu value=4.0 4000000000\ncpu value=5.0 5000000000",
    )
    .await
    .unwrap();

    let resp = ctx
        .query("testdb", "SELECT * FROM cpu LIMIT 3")
        .await
        .unwrap();
    assert!(resp.results[0].error.is_none());
    let series = resp.results[0].series.as_ref().unwrap();
    assert_eq!(
        series[0].values.len(),
        3,
        "LIMIT 3 should return exactly 3 rows"
    );
}

// ---------------------------------------------------------------------------
// ORDER BY
// ---------------------------------------------------------------------------

#[tokio::test]
#[serial(chdb)]
async fn order_by_time_desc() {
    let ctx = TestContext::new().unwrap();
    ctx.metadata.create_database("testdb").await.unwrap();
    ctx.write_and_flush(
        "testdb",
        "cpu value=1.0 1000000000\ncpu value=2.0 2000000000",
    )
    .await
    .unwrap();

    let resp = ctx
        .query("testdb", "SELECT * FROM cpu ORDER BY time DESC")
        .await
        .unwrap();
    assert!(resp.results[0].error.is_none());
    let series = resp.results[0].series.as_ref().unwrap();
    assert_eq!(series[0].values.len(), 2);
}

// ---------------------------------------------------------------------------
// Subquery
// ---------------------------------------------------------------------------

#[tokio::test]
#[serial(chdb)]
async fn subquery() {
    let ctx = TestContext::new().unwrap();
    ctx.metadata.create_database("testdb").await.unwrap();
    ctx.write_and_flush(
        "testdb",
        "cpu value=1.0 1000000000\ncpu value=2.0 2000000000",
    )
    .await
    .unwrap();

    let resp = ctx
        .query("testdb", "SELECT mean(*) FROM (SELECT * FROM cpu)")
        .await
        .unwrap();
    assert!(
        resp.results[0].error.is_none(),
        "Subquery should be supported: {:?}",
        resp.results[0].error
    );
}

// ---------------------------------------------------------------------------
// Epoch time format
// ---------------------------------------------------------------------------

#[tokio::test]
#[serial(chdb)]
async fn epoch_s_returns_unix_seconds() {
    let ctx = TestContext::new().unwrap();
    ctx.metadata.create_database("testdb").await.unwrap();
    ctx.write_and_flush("testdb", "cpu value=1.0 1000000000")
        .await
        .unwrap();

    let resp = ctx
        .query_service
        .execute_query("testdb", "SELECT * FROM cpu", Some("s"), None, None)
        .await
        .unwrap();

    let series = resp.results[0].series.as_ref().unwrap();
    let time_idx = series[0].columns.iter().position(|c| c == "time").unwrap();
    let time_val = &series[0].values[0][time_idx];
    assert!(
        time_val.is_number(),
        "epoch=s should return numeric timestamp, got: {}",
        time_val
    );
    let ts = time_val
        .as_i64()
        .or_else(|| time_val.as_f64().map(|f| f as i64))
        .unwrap();
    assert_eq!(ts, 1, "1000000000ns = 1s");
}

#[tokio::test]
#[serial(chdb)]
async fn epoch_ms_returns_unix_millis() {
    let ctx = TestContext::new().unwrap();
    ctx.metadata.create_database("testdb").await.unwrap();
    ctx.write_and_flush("testdb", "cpu value=1.0 1000000000")
        .await
        .unwrap();

    let resp = ctx
        .query_service
        .execute_query("testdb", "SELECT * FROM cpu", Some("ms"), None, None)
        .await
        .unwrap();

    let series = resp.results[0].series.as_ref().unwrap();
    let time_idx = series[0].columns.iter().position(|c| c == "time").unwrap();
    let time_val = &series[0].values[0][time_idx];
    let ts = time_val
        .as_i64()
        .or_else(|| time_val.as_f64().map(|f| f as i64))
        .unwrap();
    assert_eq!(ts, 1000, "1000000000ns = 1000ms");
}

#[tokio::test]
#[serial(chdb)]
async fn epoch_ns_returns_unix_nanos() {
    let ctx = TestContext::new().unwrap();
    ctx.metadata.create_database("testdb").await.unwrap();
    ctx.write_and_flush("testdb", "cpu value=1.0 1000000000")
        .await
        .unwrap();

    let resp = ctx
        .query_service
        .execute_query("testdb", "SELECT * FROM cpu", Some("ns"), None, None)
        .await
        .unwrap();

    let series = resp.results[0].series.as_ref().unwrap();
    let time_idx = series[0].columns.iter().position(|c| c == "time").unwrap();
    let time_val = &series[0].values[0][time_idx];
    let ts = time_val
        .as_i64()
        .or_else(|| time_val.as_f64().map(|f| f as i64))
        .unwrap();
    assert_eq!(ts, 1000000000, "epoch=ns should return nanoseconds");
}

#[tokio::test]
#[serial(chdb)]
async fn no_epoch_returns_rfc3339() {
    let ctx = TestContext::new().unwrap();
    ctx.metadata.create_database("testdb").await.unwrap();
    ctx.write_and_flush("testdb", "cpu value=1.0 1000000000")
        .await
        .unwrap();

    let resp = ctx
        .query_service
        .execute_query("testdb", "SELECT * FROM cpu", None, None, None)
        .await
        .unwrap();

    let series = resp.results[0].series.as_ref().unwrap();
    let time_idx = series[0].columns.iter().position(|c| c == "time").unwrap();
    let time_val = &series[0].values[0][time_idx];
    assert!(
        time_val.is_string(),
        "No epoch param should return RFC3339 string, got: {}",
        time_val
    );
    let ts_str = time_val.as_str().unwrap();
    assert!(
        ts_str.contains("1970-01-01") || ts_str.contains("T"),
        "Should be RFC3339 format: {}",
        ts_str
    );
}

// ---------------------------------------------------------------------------
// Multiple aggregates with default aliases
// ---------------------------------------------------------------------------

#[tokio::test]
#[serial(chdb)]
async fn multiple_aggregates_have_distinct_columns() {
    let ctx = TestContext::new().unwrap();
    ctx.metadata.create_database("testdb").await.unwrap();
    ctx.write_and_flush(
        "testdb",
        "cpu value=1.0 1000000000\ncpu value=2.0 2000000000\ncpu value=3.0 3000000000",
    )
    .await
    .unwrap();

    let resp = ctx
        .query(
            "testdb",
            "SELECT mean(value), max(value), min(value) FROM cpu",
        )
        .await
        .unwrap();
    let series = &resp.results[0].series.as_ref().unwrap()[0];
    let non_time_cols: Vec<&str> = series
        .columns
        .iter()
        .filter(|c| c.as_str() != "time")
        .map(|c| c.as_str())
        .collect();
    assert_eq!(
        non_time_cols.len(),
        3,
        "Three aggregates should produce three columns (excluding time): {:?}",
        series.columns
    );
    let unique: std::collections::HashSet<&&str> = non_time_cols.iter().collect();
    assert_eq!(
        unique.len(),
        non_time_cols.len(),
        "Column names should be unique (no alias collisions): {:?}",
        series.columns
    );
}

#[tokio::test]
#[serial(chdb)]
async fn fill_null_with_group_by_tag_does_not_split_into_phantom_series() {
    // Grafana-style query: regex host filter + aliased means + GROUP BY time, tag
    // + fill(null) over a wide range. WITH FILL must fill the *per-tag* series, not
    // emit gap rows with an empty tag value (which produced a phantom all-NULL
    // series and surfaced as "no data" in Grafana).
    let ctx = TestContext::new().unwrap();
    ctx.metadata.create_database("testdb").await.unwrap();

    // WHERE range: time >= 1781683756774ms AND time <= 1781687356774ms (1h window).
    // Write sparse points (with gaps) so WITH FILL must synthesise gap buckets.
    let min_ms = 1_781_683_756_774i64;
    let mut lp = String::new();
    for i in 0..5 {
        let ts_ns = (min_ms + 100_000 * i) * 1_000_000; // 100s apart
        lp.push_str(&format!(
            "system,host=telegraf-7c974c458c-44fqq load1={}.0,load5={}.0,load15={}.0 {}\n",
            i + 1,
            i + 2,
            i + 3,
            ts_ns
        ));
    }
    ctx.write_and_flush("testdb", &lp).await.unwrap();

    let q = r#"SELECT mean("load1") AS "Load (short)", mean("load5") AS "Load (medium)", mean("load15") AS "Load (long)" FROM "system" WHERE "host" =~ /^(telegraf-7c974c458c-44fqq)$/ AND time >= 1781683756774ms and time <= 1781687356774ms GROUP BY time(10s), "host" fill(null) ORDER BY time ASC"#;
    let resp = ctx.query("testdb", q).await.unwrap();
    assert!(
        resp.results[0].error.is_none(),
        "{:?}",
        resp.results[0].error
    );

    let series = resp.results[0]
        .series
        .as_ref()
        .expect("expected series back, got none");
    // Exactly one series — the real host — not a real series plus a phantom
    // empty-tag fill series.
    assert_eq!(
        series.len(),
        1,
        "expected exactly one series (no phantom empty-tag series), got {}: {:?}",
        series.len(),
        series.iter().map(|s| &s.tags).collect::<Vec<_>>()
    );
    let s = &series[0];
    assert_eq!(
        s.tags
            .as_ref()
            .and_then(|t| t.get("host"))
            .map(String::as_str),
        Some("telegraf-7c974c458c-44fqq"),
        "the single series must carry the real host tag, got tags: {:?}",
        s.tags
    );
    // Wide range filled: many buckets, but only the 5 written points are non-null.
    assert!(
        s.values.len() > 100,
        "fill(null) should produce a filled bucket grid, got {} rows",
        s.values.len()
    );
    let non_null = s
        .values
        .iter()
        .filter(|row| row.iter().skip(1).any(|v| !v.is_null()))
        .count();
    assert_eq!(non_null, 5, "exactly the 5 written points carry data");
}

#[tokio::test]
#[serial(chdb)]
async fn partial_line_writes_coalesce_before_query() {
    let ctx = TestContext::new().unwrap();
    ctx.metadata.create_database("testdb").await.unwrap();
    // Simulate Telegraf / columnar partial lines: same series-instant, disjoint fields.
    let ts = 1_780_922_276_152_000_000i64;
    ctx.write_and_flush(
        "testdb",
        &format!(
            "cpu,cpu=cpu-total,host=d2ddee27a9f4 usage_idle=95.0 {ts}\n\
             cpu,cpu=cpu-total,host=d2ddee27a9f4 usage_user=4.0 {ts}\n\
             cpu,cpu=cpu-total,host=d2ddee27a9f4 usage_system=1.0 {ts}"
        ),
    )
    .await
    .unwrap();

    let resp = ctx
        .query(
            "testdb",
            &format!(
                "SELECT mean(\"usage_idle\") AS \"Usage Idle\", mean(\"usage_user\") AS \"Usage User\", mean(\"usage_system\") AS \"Usage System\" \
                 FROM cpu WHERE \"host\" =~ /^(d2ddee27a9f4)$/ AND \"cpu\" = 'cpu-total' \
                 AND time >= {ts} AND time <= {ts} GROUP BY time(2s), \"host\" fill(null)"
            ),
        )
        .await
        .unwrap();
    assert!(
        resp.results[0].error.is_none(),
        "{:?}",
        resp.results[0].error
    );
    let series = &resp.results[0].series.as_ref().unwrap()[0];
    let row = &series.values[0];
    let cols: Vec<&str> = series.columns.iter().map(|s| s.as_str()).collect();
    let idle = row[cols.iter().position(|c| *c == "Usage Idle").unwrap()]
        .as_f64()
        .unwrap();
    let user = row[cols.iter().position(|c| *c == "Usage User").unwrap()]
        .as_f64()
        .unwrap();
    let system = row[cols.iter().position(|c| *c == "Usage System").unwrap()]
        .as_f64()
        .unwrap();
    assert!(
        (idle - 95.0).abs() < 0.01,
        "usage_idle should survive partial-line coalesce, got {idle}"
    );
    assert!(
        (user - 4.0).abs() < 0.01,
        "usage_user should survive partial-line coalesce, got {user}"
    );
    assert!(
        (system - 1.0).abs() < 0.01,
        "usage_system should survive partial-line coalesce, got {system}"
    );
}
