//! Prepared WAL slot construction must preserve every measurement in a batch.
//!
//! Regression tests for cross-measurement coalesce in `build_prepared_wal_slot`.

use hyperbytedb::adapters::chdb::native_adapter::ChdbNativeAdapter;
use hyperbytedb::adapters::chdb::session::SharedSession;
use hyperbytedb::adapters::metadata::rocksdb_meta::RocksDbMetadata;
use hyperbytedb::application::line_protocol::parse_line_body_to_points;
use hyperbytedb::ports::metadata::MetadataPort;
use serial_test::serial;

fn telegraf_like_batch() -> Vec<u8> {
    let ts = 1_780_922_276_152_000_000i64;
    format!(
        "cpu,cpu=cpu0,host=node1 usage_idle=95.0 {ts}\n\
         cpu,cpu=cpu1,host=node1 usage_idle=90.0 {ts}\n\
         mem,host=node1 used=100i {ts}\n\
         system,host=node1 load1=0.5 {ts}\n\
         swap,host=node1 free=200i {ts}\n\
         processes,host=node1 running=3i {ts}\n\
         kernel,host=node1 boot_time=100i {ts}\n\
         netstat,host=node1 tcp_established=1i {ts}\n\
         disk,device=sda,host=node1 free=1000i {ts}\n\
         diskio,name=sda,host=node1 reads=10i {ts}\n"
    )
    .into_bytes()
}

#[tokio::test]
#[serial(chdb)]
async fn build_prepared_wal_slot_keeps_all_measurements_in_telegraf_batch() {
    let tmpdir = tempfile::tempdir().unwrap();
    let meta_path = tmpdir.path().join("meta");
    let chdb_path = tmpdir.path().join("chdb");
    std::fs::create_dir_all(&meta_path).unwrap();
    std::fs::create_dir_all(&chdb_path).unwrap();

    let metadata = std::sync::Arc::new(RocksDbMetadata::open(&meta_path).unwrap());
    metadata.create_database("telegraf").await.unwrap();

    let shared = SharedSession::new_eager(chdb_path.to_str().unwrap(), 1).unwrap();
    let sink = ChdbNativeAdapter::with_metadata(shared, Some(metadata));

    let points = parse_line_body_to_points(&telegraf_like_batch(), None).unwrap();
    assert_eq!(
        points.len(),
        10,
        "fixture should have one line per measurement row"
    );

    let slot = sink
        .build_prepared_wal_slot("telegraf", "autogen", 1, &points)
        .await
        .unwrap();

    assert_eq!(
        slot.measurements.len(),
        9,
        "prepared slot must include every distinct measurement, not collapse host-only rows"
    );

    let mut names: Vec<_> = slot
        .measurements
        .iter()
        .map(|m| m.measurement.as_str())
        .collect();
    names.sort_unstable();
    assert_eq!(
        names,
        vec![
            "cpu",
            "disk",
            "diskio",
            "kernel",
            "mem",
            "netstat",
            "processes",
            "swap",
            "system"
        ]
    );

    let cpu_batch = slot
        .measurements
        .iter()
        .find(|m| m.measurement == "cpu")
        .expect("cpu batch");
    assert_eq!(cpu_batch.row_count, 2);
    assert_eq!(slot.total_rows(), points.len());
}

/// The engine's columns are probed (`DESCRIBE TABLE`) only while this
/// process has not yet materialized the table. Probing on every batch queued
/// each write behind flush inserts on the shared chDB pool: ~0.5s per Arrow
/// build at scale.
#[tokio::test]
#[serial(chdb)]
async fn engine_columns_are_probed_once_per_table_not_per_batch() {
    let tmpdir = tempfile::tempdir().unwrap();
    let meta_path = tmpdir.path().join("meta");
    let chdb_path = tmpdir.path().join("chdb");
    std::fs::create_dir_all(&meta_path).unwrap();
    std::fs::create_dir_all(&chdb_path).unwrap();
    let metadata = std::sync::Arc::new(RocksDbMetadata::open(&meta_path).unwrap());
    metadata.create_database("db").await.unwrap();
    let shared = SharedSession::new_eager(chdb_path.to_str().unwrap(), 1).unwrap();

    let sink = ChdbNativeAdapter::with_metadata(shared.clone(), Some(metadata.clone()));
    for i in 0..5 {
        let line = format!("cpu,host=h{i} v=1 {}", 1_780_000_000_000_000_000i64 + i);
        let points = parse_line_body_to_points(line.as_bytes(), None).unwrap();
        sink.build_prepared_wal_slot("db", "autogen", 1, &points)
            .await
            .unwrap();
    }
    assert_eq!(sink.engine_probe_count(), 1, "one probe, then the cache");

    // A new adapter (a restart) starts cold and must probe again, once.
    let restarted = ChdbNativeAdapter::with_metadata(shared, Some(metadata));
    for i in 0..3 {
        let line = format!("cpu,host=h{i} v=2 {}", 1_780_000_000_000_000_100i64 + i);
        let points = parse_line_body_to_points(line.as_bytes(), None).unwrap();
        restarted
            .build_prepared_wal_slot("db", "autogen", 1, &points)
            .await
            .unwrap();
    }
    assert_eq!(restarted.engine_probe_count(), 1);
}

/// Two writers can both plan a table CREATE before either records it: the
/// plan runs under the DDL lock, the CREATE after it. The second CREATE is a
/// no-op (`IF NOT EXISTS`), but its writer still cached ITS columns. On n3
/// the series table was created without `host`, the cache said it had it,
/// and every flush on two nodes failed with NO_SUCH_COLUMN_IN_TABLE while
/// their WAL grew until they were OOM-killed.
///
/// Reproduced deterministically: one adapter creates the tables without the
/// tag column, then a second adapter (cold cache, same engine) writes points
/// that carry it.
#[tokio::test]
#[serial(chdb)]
async fn a_table_created_by_another_writer_still_gets_this_writers_columns() {
    use hyperbytedb::domain::measurement::MeasurementMeta;
    use hyperbytedb::ports::points_sink::PointsSinkPort;

    let tmpdir = tempfile::tempdir().unwrap();
    let chdb_path = tmpdir.path().join("chdb");
    std::fs::create_dir_all(&chdb_path).unwrap();
    let shared = SharedSession::new_eager(chdb_path.to_str().unwrap(), 1).unwrap();

    let first = ChdbNativeAdapter::new(shared.clone());
    let mut meta = MeasurementMeta {
        name: "cpu".into(),
        ..Default::default()
    };
    meta.field_types.insert("v".into(), 0);
    first
        .ensure_measurement_schema("db", "autogen", &meta)
        .await
        .unwrap();

    let second = ChdbNativeAdapter::new(shared);
    let points = parse_line_body_to_points(b"cpu,host=a v=1 1780000000000000000", None).unwrap();
    second
        .write_points("db", "autogen", "cpu", &[1], 1, &points)
        .await
        .expect("the write must add the missing tag column, not trust its cache");
}

/// If the cache and the table disagree anyway, the failed insert makes the
/// adapter re-plan the table instead of failing the same way forever: a
/// flush retries until it succeeds, and each failure grows the WAL.
#[tokio::test]
#[serial(chdb)]
async fn an_insert_missing_a_column_repairs_the_table_on_retry() {
    use hyperbytedb::adapters::chdb::query_adapter::ChdbQueryAdapter;
    use hyperbytedb::ports::points_sink::PointsSinkPort;
    use hyperbytedb::ports::query::QueryPort;

    let tmpdir = tempfile::tempdir().unwrap();
    let chdb_path = tmpdir.path().join("chdb");
    std::fs::create_dir_all(&chdb_path).unwrap();
    let shared = SharedSession::new_eager(chdb_path.to_str().unwrap(), 1).unwrap();
    let sink = ChdbNativeAdapter::new(shared.clone());
    let write = |ts: i64, host: &str| {
        parse_line_body_to_points(format!("cpu,host={host} v=1 {ts}").as_bytes(), None).unwrap()
    };
    sink.write_points(
        "db",
        "autogen",
        "cpu",
        &[1],
        1,
        &write(1_780_000_000_000_000_000, "a"),
    )
    .await
    .unwrap();

    ChdbQueryAdapter::from_shared(shared, 0)
        .execute_sql("ALTER TABLE default.db_autogen_cpu_series DROP COLUMN host")
        .await
        .unwrap();

    let again = write(1_780_000_000_000_000_001, "b");
    assert!(
        sink.write_points("db", "autogen", "cpu", &[1], 2, &again)
            .await
            .is_err(),
        "the cache still believes in the dropped column"
    );
    sink.write_points("db", "autogen", "cpu", &[1], 3, &again)
        .await
        .expect("the retry re-plans the table and adds the column back");
}

/// Writes to a new measurement with different fields and tags, flushed
/// together through prepared Arrow batches: every value stays under its own
/// field and tag. The batches were stacked by position, so `free`'s values
/// were stored under `used` (and `region`'s under `host`).
#[tokio::test]
#[serial(chdb)]
async fn writes_with_different_fields_flushed_together_keep_their_columns() {
    use hyperbytedb::adapters::chdb::query_adapter::ChdbQueryAdapter;
    use hyperbytedb::adapters::wal::rocksdb_wal::RocksDbWal;
    use hyperbytedb::application::flush_service::FlushServiceImpl;
    use hyperbytedb::application::ingest_metadata::IngestCardinalityLimits;
    use hyperbytedb::application::ingestion_service::IngestionServiceImpl;
    use hyperbytedb::application::query_service::QueryServiceImpl;
    use hyperbytedb::ports::ingestion::{IngestionPort, WritePayloadFormat};
    use hyperbytedb::ports::points_sink::PointsSinkPort;
    use hyperbytedb::ports::query::{QueryPort, QueryService};
    use std::sync::Arc;

    let tmpdir = tempfile::tempdir().unwrap();
    let dir = |d: &str| {
        let p = tmpdir.path().join(d);
        std::fs::create_dir_all(&p).unwrap();
        p
    };
    let wal = Arc::new(RocksDbWal::open(dir("wal")).unwrap());
    let metadata = Arc::new(RocksDbMetadata::open(dir("meta")).unwrap());
    metadata.create_database("db").await.unwrap();
    let shared = SharedSession::new_eager(dir("chdb").to_str().unwrap(), 1).unwrap();
    let sink: Arc<dyn PointsSinkPort> = Arc::new(ChdbNativeAdapter::with_metadata(
        shared.clone(),
        Some(metadata.clone()),
    ));
    let query_port: Arc<dyn QueryPort> = Arc::new(ChdbQueryAdapter::from_shared(shared, 0));
    let ingestion = IngestionServiceImpl::with_sink(
        wal.clone(),
        Some(sink.clone()),
        metadata.clone(),
        IngestCardinalityLimits::default(),
        0,
    );
    let flush = FlushServiceImpl::new(wal.clone(), 0, sink.clone());
    let queries = QueryServiceImpl::new(query_port, metadata, wal, 30, sink);

    for body in [
        "m,host=a used=1 1000000000",
        "m,host=a free=2 2000000000",
        "m,region=x free=3 3000000000",
    ] {
        ingestion
            .ingest(
                "db",
                None,
                None,
                body.as_bytes(),
                WritePayloadFormat::LineProtocol,
            )
            .await
            .unwrap();
    }
    flush.flush().await.unwrap();

    let resp = queries
        .execute_query("db", "SELECT * FROM m", Some("s"), None, None)
        .await
        .unwrap();
    let series = &resp.results[0].series.as_ref().unwrap()[0];
    let rows: Vec<serde_json::Value> = series
        .values
        .iter()
        .map(|v| {
            serde_json::Value::Object(
                series
                    .columns
                    .iter()
                    .cloned()
                    .zip(v.iter().cloned())
                    .collect(),
            )
        })
        .collect();
    assert_eq!(
        serde_json::Value::Array(rows),
        // A series without a tag reads as ""; a whole float is written `1`.
        serde_json::json!([
            {"time": 1, "free": null, "host": "a", "region": "", "used": 1},
            {"time": 2, "free": 2, "host": "a", "region": "", "used": null},
            {"time": 3, "free": 3, "host": "", "region": "x", "used": null},
        ])
    );
}
