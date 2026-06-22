//! Prepared WAL slot construction must preserve every measurement in a batch.
//!
//! Regression tests for cross-measurement coalesce in `build_prepared_wal_slot`.

use hyperbytedb::adapters::chdb::native_adapter::ChdbNativeAdapter;
use hyperbytedb::adapters::chdb::session::SharedSession;
use hyperbytedb::adapters::metadata::rocksdb_meta::RocksDbMetadata;
use hyperbytedb::application::line_protocol::parse_line_body_to_points;
use hyperbytedb::ports::metadata::MetadataPort;

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
