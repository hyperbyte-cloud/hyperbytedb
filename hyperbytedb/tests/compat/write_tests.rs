//! Write path compatibility tests.
//!
//! Verifies line protocol edge cases: escaped characters, field type variants,
//! precision handling, batch writes, gzip decompression, and timestamp assignment.

use hyperbytedb::domain::point::FieldValue;
use hyperbytedb::ports::ingestion::{IngestionPort, WritePayloadFormat};
use serial_test::serial;

use super::TestContext;

// ---------------------------------------------------------------------------
// Basic line protocol
// ---------------------------------------------------------------------------

#[tokio::test]
async fn write_basic_line_protocol_wal_contains_data() {
    let ctx = TestContext::new_no_chdb().unwrap();
    ctx.metadata.create_database("testdb").await.unwrap();
    ctx.ingestion
        .ingest(
            "testdb",
            None,
            None,
            b"cpu value=42.5 1000000000",
            WritePayloadFormat::LineProtocol,
        )
        .await
        .unwrap();

    let entries = ctx.wal.read_from(1).await.unwrap();
    assert!(!entries.is_empty(), "WAL should contain entries");
    let (_, entry) = &entries[0];
    assert_eq!(entry.database, "testdb");
    assert_eq!(entry.retention_policy, "autogen");
    assert_eq!(entry.points.len(), 1);
    assert_eq!(entry.points[0].measurement, "cpu");
    assert_eq!(entry.points[0].fields.len(), 1);
}

#[tokio::test]
async fn write_multiple_fields() {
    let ctx = TestContext::new_no_chdb().unwrap();
    ctx.metadata.create_database("testdb").await.unwrap();
    ctx.ingestion
        .ingest(
            "testdb",
            None,
            None,
            b"cpu value=42.5,count=10i,host=\"srv1\" 1000000000",
            WritePayloadFormat::LineProtocol,
        )
        .await
        .unwrap();

    let entries = ctx.wal.read_from(1).await.unwrap();
    assert_eq!(entries[0].1.points.len(), 1);
    let point = &entries[0].1.points[0];
    assert_eq!(point.fields.len(), 3);
    assert!(
        matches!(point.fields.get("value"), Some(FieldValue::Float(v)) if (*v - 42.5).abs() < f64::EPSILON)
    );
    assert!(matches!(
        point.fields.get("count"),
        Some(FieldValue::Integer(10))
    ));
    assert!(matches!(point.fields.get("host"), Some(FieldValue::String(s)) if s == "srv1"));
}

#[tokio::test]
async fn write_with_tags() {
    let ctx = TestContext::new_no_chdb().unwrap();
    ctx.metadata.create_database("testdb").await.unwrap();
    ctx.ingestion
        .ingest(
            "testdb",
            None,
            None,
            b"cpu,host=server01,region=us-west value=42.5 1000000000",
            WritePayloadFormat::LineProtocol,
        )
        .await
        .unwrap();

    let entries = ctx.wal.read_from(1).await.unwrap();
    let point = &entries[0].1.points[0];
    assert_eq!(point.tags.get("host"), Some(&"server01".to_string()));
    assert_eq!(point.tags.get("region"), Some(&"us-west".to_string()));
}

#[tokio::test]
async fn write_with_explicit_timestamp() {
    let ctx = TestContext::new_no_chdb().unwrap();
    ctx.metadata.create_database("testdb").await.unwrap();
    let ts_ns = 1234567890000000000i64;
    ctx.ingestion
        .ingest(
            "testdb",
            None,
            Some("ns"),
            format!("cpu value=1.0 {}", ts_ns).as_bytes(),
            WritePayloadFormat::LineProtocol,
        )
        .await
        .unwrap();

    let entries = ctx.wal.read_from(1).await.unwrap();
    assert_eq!(entries[0].1.points[0].timestamp, ts_ns);
}

// ---------------------------------------------------------------------------
// Precision handling
// ---------------------------------------------------------------------------

#[tokio::test]
async fn write_precision_ms() {
    let ctx = TestContext::new_no_chdb().unwrap();
    ctx.metadata.create_database("testdb").await.unwrap();
    let ts_ms = 1234567890000i64;
    ctx.ingestion
        .ingest(
            "testdb",
            None,
            Some("ms"),
            format!("cpu value=1.0 {}", ts_ms).as_bytes(),
            WritePayloadFormat::LineProtocol,
        )
        .await
        .unwrap();

    let entries = ctx.wal.read_from(1).await.unwrap();
    let expected_ns = ts_ms * 1_000_000;
    assert_eq!(entries[0].1.points[0].timestamp, expected_ns);
}

#[tokio::test]
async fn write_precision_s() {
    let ctx = TestContext::new_no_chdb().unwrap();
    ctx.metadata.create_database("testdb").await.unwrap();
    let ts_s = 1234567890i64;
    ctx.ingestion
        .ingest(
            "testdb",
            None,
            Some("s"),
            format!("cpu value=1.0 {}", ts_s).as_bytes(),
            WritePayloadFormat::LineProtocol,
        )
        .await
        .unwrap();

    let entries = ctx.wal.read_from(1).await.unwrap();
    let expected_ns = ts_s * 1_000_000_000;
    assert_eq!(entries[0].1.points[0].timestamp, expected_ns);
}

#[tokio::test]
async fn write_precision_us() {
    let ctx = TestContext::new_no_chdb().unwrap();
    ctx.metadata.create_database("testdb").await.unwrap();
    let ts_us = 1234567890000000i64;
    ctx.ingestion
        .ingest(
            "testdb",
            None,
            Some("us"),
            format!("cpu value=1.0 {}", ts_us).as_bytes(),
            WritePayloadFormat::LineProtocol,
        )
        .await
        .unwrap();

    let entries = ctx.wal.read_from(1).await.unwrap();
    let expected_ns = ts_us * 1_000;
    assert_eq!(entries[0].1.points[0].timestamp, expected_ns);
}

#[tokio::test]
async fn write_precision_u_alias() {
    let ctx = TestContext::new_no_chdb().unwrap();
    ctx.metadata.create_database("testdb").await.unwrap();
    let ts_us = 1234567890000000i64;
    ctx.ingestion
        .ingest(
            "testdb",
            None,
            Some("u"),
            format!("cpu value=1.0 {}", ts_us).as_bytes(),
            WritePayloadFormat::LineProtocol,
        )
        .await
        .unwrap();

    let entries = ctx.wal.read_from(1).await.unwrap();
    let expected_ns = ts_us * 1_000;
    assert_eq!(
        entries[0].1.points[0].timestamp, expected_ns,
        "precision='u' should be treated as microseconds"
    );
}

// ---------------------------------------------------------------------------
// Field type variants
// ---------------------------------------------------------------------------

#[tokio::test]
async fn write_boolean_field() {
    let ctx = TestContext::new_no_chdb().unwrap();
    ctx.metadata.create_database("testdb").await.unwrap();
    ctx.ingestion
        .ingest(
            "testdb",
            None,
            None,
            b"sensor active=true,enabled=false 1000000000",
            WritePayloadFormat::LineProtocol,
        )
        .await
        .unwrap();

    let entries = ctx.wal.read_from(1).await.unwrap();
    let point = &entries[0].1.points[0];
    assert!(matches!(
        point.fields.get("active"),
        Some(FieldValue::Boolean(true))
    ));
    assert!(matches!(
        point.fields.get("enabled"),
        Some(FieldValue::Boolean(false))
    ));
}

#[tokio::test]
async fn write_integer_field() {
    let ctx = TestContext::new_no_chdb().unwrap();
    ctx.metadata.create_database("testdb").await.unwrap();
    ctx.ingestion
        .ingest(
            "testdb",
            None,
            None,
            b"cpu count=42i 1000000000",
            WritePayloadFormat::LineProtocol,
        )
        .await
        .unwrap();

    let entries = ctx.wal.read_from(1).await.unwrap();
    let point = &entries[0].1.points[0];
    assert!(
        matches!(point.fields.get("count"), Some(FieldValue::Integer(42))),
        "Integer field should use 'i' suffix: got {:?}",
        point.fields.get("count")
    );
}

#[tokio::test]
async fn write_string_field() {
    let ctx = TestContext::new_no_chdb().unwrap();
    ctx.metadata.create_database("testdb").await.unwrap();
    ctx.ingestion
        .ingest(
            "testdb",
            None,
            None,
            b"events message=\"server started\" 1000000000",
            WritePayloadFormat::LineProtocol,
        )
        .await
        .unwrap();

    let entries = ctx.wal.read_from(1).await.unwrap();
    let point = &entries[0].1.points[0];
    assert!(
        matches!(point.fields.get("message"), Some(FieldValue::String(s)) if s == "server started"),
        "String field value mismatch: {:?}",
        point.fields.get("message")
    );
}

// ---------------------------------------------------------------------------
// Batch writes
// ---------------------------------------------------------------------------

#[tokio::test]
async fn write_batch_multiple_lines() {
    let ctx = TestContext::new_no_chdb().unwrap();
    ctx.metadata.create_database("testdb").await.unwrap();
    let lines = "cpu value=1.0 1000000000\ncpu value=2.0 2000000000\ncpu value=3.0 3000000000";
    ctx.ingestion
        .ingest(
            "testdb",
            None,
            None,
            lines.as_bytes(),
            WritePayloadFormat::LineProtocol,
        )
        .await
        .unwrap();

    let entries = ctx.wal.read_from(1).await.unwrap();
    assert_eq!(entries[0].1.points.len(), 3);
}

#[tokio::test]
async fn write_batch_multiple_measurements() {
    let ctx = TestContext::new_no_chdb().unwrap();
    ctx.metadata.create_database("testdb").await.unwrap();
    let lines =
        "cpu value=1.0 1000000000\nmemory value=2048i 1000000000\ndisk free=50.5 1000000000";
    ctx.ingestion
        .ingest(
            "testdb",
            None,
            None,
            lines.as_bytes(),
            WritePayloadFormat::LineProtocol,
        )
        .await
        .unwrap();

    let entries = ctx.wal.read_from(1).await.unwrap();
    let points = &entries[0].1.points;
    assert_eq!(points.len(), 3, "All three points should be in WAL");

    let measurements: Vec<&str> = points.iter().map(|p| p.measurement.as_str()).collect();
    assert!(measurements.contains(&"cpu"));
    assert!(measurements.contains(&"memory"));
    assert!(measurements.contains(&"disk"));
}

#[tokio::test]
async fn write_batch_with_trailing_newline() {
    let ctx = TestContext::new_no_chdb().unwrap();
    ctx.metadata.create_database("testdb").await.unwrap();
    let lines = "cpu value=1.0 1000000000\ncpu value=2.0 2000000000\n";
    ctx.ingestion
        .ingest(
            "testdb",
            None,
            None,
            lines.as_bytes(),
            WritePayloadFormat::LineProtocol,
        )
        .await
        .unwrap();

    let entries = ctx.wal.read_from(1).await.unwrap();
    assert_eq!(
        entries[0].1.points.len(),
        2,
        "Trailing newline should not produce extra point"
    );
}

#[tokio::test]
async fn write_without_explicit_timestamp() {
    let ctx = TestContext::new_no_chdb().unwrap();
    ctx.metadata.create_database("testdb").await.unwrap();
    let before = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_nanos() as i64;

    ctx.ingestion
        .ingest(
            "testdb",
            None,
            None,
            b"cpu value=1.0",
            WritePayloadFormat::LineProtocol,
        )
        .await
        .unwrap();

    let after = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_nanos() as i64;

    let entries = ctx.wal.read_from(1).await.unwrap();
    let ts = entries[0].1.points[0].timestamp;
    assert!(
        ts >= before && ts <= after,
        "Auto-assigned timestamp {ts} should be between {before} and {after}"
    );
}

// ---------------------------------------------------------------------------
// Escaped characters
// ---------------------------------------------------------------------------

#[tokio::test]
async fn write_escaped_spaces_in_measurement() {
    let ctx = TestContext::new_no_chdb().unwrap();
    ctx.metadata.create_database("testdb").await.unwrap();
    ctx.ingestion
        .ingest(
            "testdb",
            None,
            None,
            b"cpu\\ usage value=42.0 1000000000",
            WritePayloadFormat::LineProtocol,
        )
        .await
        .unwrap();

    let entries = ctx.wal.read_from(1).await.unwrap();
    assert_eq!(
        entries[0].1.points[0].measurement, "cpu usage",
        "Escaped space in measurement should be unescaped"
    );
}

#[tokio::test]
async fn write_escaped_commas_in_tag_value() {
    let ctx = TestContext::new_no_chdb().unwrap();
    ctx.metadata.create_database("testdb").await.unwrap();
    ctx.ingestion
        .ingest(
            "testdb",
            None,
            None,
            b"cpu,location=us\\,east value=1.0 1000000000",
            WritePayloadFormat::LineProtocol,
        )
        .await
        .unwrap();

    let entries = ctx.wal.read_from(1).await.unwrap();
    let point = &entries[0].1.points[0];
    assert_eq!(
        point.tags.get("location"),
        Some(&"us,east".to_string()),
        "Escaped comma in tag value should be unescaped"
    );
}

#[tokio::test]
async fn write_escaped_equals_in_tag_value() {
    let ctx = TestContext::new_no_chdb().unwrap();
    ctx.metadata.create_database("testdb").await.unwrap();
    ctx.ingestion
        .ingest(
            "testdb",
            None,
            None,
            b"cpu,equation=a\\=b value=1.0 1000000000",
            WritePayloadFormat::LineProtocol,
        )
        .await
        .unwrap();

    let entries = ctx.wal.read_from(1).await.unwrap();
    let point = &entries[0].1.points[0];
    assert_eq!(
        point.tags.get("equation"),
        Some(&"a=b".to_string()),
        "Escaped equals in tag value should be unescaped"
    );
}

// ---------------------------------------------------------------------------
// Gzip decompression
// ---------------------------------------------------------------------------

#[tokio::test]
async fn ingest_gzip_decompressed_body() {
    use flate2::read::GzDecoder;
    use std::io::Read;

    let ctx = TestContext::new_no_chdb().unwrap();
    ctx.metadata.create_database("gztest").await.unwrap();

    let line = "cpu,host=server01 value=42.5 1000000000\n";
    let mut encoder = flate2::write::GzEncoder::new(Vec::new(), flate2::Compression::default());
    std::io::Write::write_all(&mut encoder, line.as_bytes()).unwrap();
    let compressed = encoder.finish().unwrap();

    let mut decoder = GzDecoder::new(compressed.as_slice());
    let mut decompressed = Vec::new();
    decoder.read_to_end(&mut decompressed).unwrap();

    ctx.ingestion
        .ingest(
            "gztest",
            None,
            None,
            &decompressed,
            WritePayloadFormat::LineProtocol,
        )
        .await
        .unwrap();

    let entries = ctx.wal.read_from(1).await.unwrap();
    assert!(!entries.is_empty());
    assert_eq!(entries[0].1.points[0].measurement, "cpu");
}

// ---------------------------------------------------------------------------
// Retention policy routing
// ---------------------------------------------------------------------------

#[tokio::test]
async fn write_to_specific_retention_policy() {
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

    ctx.ingestion
        .ingest(
            "testdb",
            Some("oneweek"),
            None,
            b"cpu value=1.0 1000000000",
            WritePayloadFormat::LineProtocol,
        )
        .await
        .unwrap();

    let entries = ctx.wal.read_from(1).await.unwrap();
    assert_eq!(
        entries[0].1.retention_policy, "oneweek",
        "WAL entry should reference the explicit retention policy"
    );
}

// ---------------------------------------------------------------------------
// chDB round-trip
// ---------------------------------------------------------------------------

#[tokio::test]
#[serial(chdb)]
async fn create_db_write_flush_query_select_star() {
    let ctx = TestContext::new().unwrap();
    ctx.metadata.create_database("testdb").await.unwrap();
    ctx.write_and_flush(
        "testdb",
        "temperature,location=office value=22.5 1000000000\ntemperature,location=office value=23.0 2000000000",
    )
    .await
    .unwrap();

    let resp = ctx
        .query("testdb", "SELECT * FROM temperature")
        .await
        .unwrap();
    assert!(!resp.results.is_empty());
    let stmt = &resp.results[0];
    assert!(
        stmt.error.is_none(),
        "Query should not error: {:?}",
        stmt.error
    );
    let series = stmt.series.as_ref().unwrap();
    assert!(!series.is_empty());
    assert_eq!(series[0].name, "temperature");
    assert!(series[0].columns.contains(&"time".to_string()));
    assert!(series[0].columns.contains(&"value".to_string()));
    assert!(series[0].values.len() >= 2);
}
