//! End-to-end tests using the production bootstrap path and background flush loop.

mod support;

use std::time::Duration;

use hyperbytedb::adapters::metadata::rocksdb_meta::RocksDbMetadata;
use hyperbytedb::application::backup::{backup, restore};
use hyperbytedb::ports::metadata::MetadataPort;
use serial_test::serial;

use support::{E2eFixture, files_under, query_row_count};

const DB: &str = "e2edb";
const WRITE_LINES: &str = "cpu,host=a value=1.0 1000000000\ncpu,host=a value=2.0 2000000000";

#[tokio::test]
#[serial(chdb)]
async fn write_auto_flush_then_query() {
    let fixture = E2eFixture::new();
    let server = fixture.start().await;

    server.create_db(DB).await;

    let write_resp = server.write(DB, WRITE_LINES).await;
    assert_eq!(
        write_resp.status(),
        reqwest::StatusCode::NO_CONTENT,
        "write should return 204"
    );

    let parsed = server
        .wait_for_rows(DB, "SELECT * FROM cpu", 2, Duration::from_secs(5))
        .await;

    let series = parsed["results"][0]["series"]
        .as_array()
        .expect("series array");
    assert_eq!(series.len(), 1);
    assert_eq!(series[0]["name"].as_str(), Some("cpu"));

    let columns = series[0]["columns"]
        .as_array()
        .expect("columns")
        .iter()
        .filter_map(|c| c.as_str())
        .collect::<Vec<_>>();
    assert!(columns.contains(&"time"));
    assert!(columns.contains(&"value"));
    assert_eq!(series[0]["values"].as_array().unwrap().len(), 2);

    let _fixture = server.stop().await;
}

#[tokio::test]
#[serial(chdb)]
async fn backup_restore_roundtrip() {
    let fixture = E2eFixture::new();
    let backup_path = fixture.backup_dir();
    let server = fixture.start().await;

    server.create_db(DB).await;

    let write_resp = server.write(DB, WRITE_LINES).await;
    assert_eq!(write_resp.status(), reqwest::StatusCode::NO_CONTENT);

    let before = server
        .wait_for_rows(DB, "SELECT * FROM cpu", 2, Duration::from_secs(5))
        .await;
    assert_eq!(query_row_count(&before), 2);

    let show = server.query(DB, "SHOW DATABASES").await;
    let names = show["results"][0]["series"][0]["values"]
        .as_array()
        .expect("SHOW DATABASES values");
    assert!(
        names.iter().any(|row| row[0].as_str() == Some(DB)),
        "SHOW DATABASES should list {DB}: {show}"
    );

    let config = server.config().clone();
    let default_sql =
        std::path::Path::new(&config.chdb.session_data_path).join("metadata/default.sql");
    assert!(
        default_sql.exists(),
        "chDB session should persist metadata/default.sql after first table flush: {}",
        default_sql.display()
    );
    let fixture = server.stop().await;

    let metadata =
        RocksDbMetadata::open(&config.storage.meta_dir).expect("open meta before backup");
    assert!(
        metadata
            .get_database(DB)
            .await
            .expect("get database before backup")
            .is_some(),
        "metadata on disk after stop should contain {DB}"
    );
    let measurements = metadata
        .list_measurements(DB)
        .await
        .expect("list measurements before backup");
    assert!(
        measurements.iter().any(|m| m == "cpu"),
        "metadata on disk after stop should contain cpu measurement: {measurements:?}"
    );
    drop(metadata);

    backup(config.clone(), backup_path.to_str().unwrap())
        .await
        .expect("backup");

    assert!(
        backup_path.join("manifest.json").exists(),
        "backup should contain manifest.json"
    );
    assert!(
        backup_path.join("wal").exists(),
        "backup should contain wal/"
    );
    assert!(
        backup_path.join("meta").exists(),
        "backup should contain meta/"
    );
    assert!(
        files_under(&backup_path.join("meta")) > 0,
        "backup meta/ should be non-empty"
    );
    assert!(
        files_under(&backup_path.join("data")) > 0,
        "backup should contain non-empty data/"
    );
    assert!(
        backup_path.join("data/metadata/default.sql").exists(),
        "backup should contain default database metadata for cold start"
    );

    for dir in [
        &config.storage.wal_dir,
        &config.storage.meta_dir,
        &config.chdb.session_data_path,
    ] {
        if std::path::Path::new(dir).exists() {
            std::fs::remove_dir_all(dir).expect("remove data dir");
        }
    }

    restore(config.clone(), backup_path.to_str().unwrap())
        .await
        .expect("restore");

    assert!(
        std::path::Path::new(&config.storage.wal_dir).exists(),
        "restore should recreate wal/"
    );
    assert!(
        std::path::Path::new(&config.storage.meta_dir).exists(),
        "restore should recreate meta/"
    );
    assert!(
        files_under(std::path::Path::new(&config.chdb.session_data_path)) > 0,
        "restore should recreate non-empty chDB data/"
    );

    let backup_data_files = files_under(&backup_path.join("data"));
    let restored_data_files = files_under(std::path::Path::new(&config.chdb.session_data_path));
    assert_eq!(
        backup_data_files, restored_data_files,
        "restored chDB file count should match backup"
    );

    let metadata =
        RocksDbMetadata::open(&config.storage.meta_dir).expect("open meta after restore");
    assert!(
        metadata
            .get_database(DB)
            .await
            .expect("get database after restore")
            .is_some(),
        "restored metadata should contain {DB}"
    );
    let restored_measurements = metadata
        .list_measurements(DB)
        .await
        .expect("list measurements after restore");
    assert!(
        restored_measurements.iter().any(|m| m == "cpu"),
        "restored metadata should contain cpu measurement: {restored_measurements:?}"
    );
    drop(metadata);

    let after = fixture
        .query_via_subprocess(&config, DB, "SELECT * FROM cpu", 2)
        .await;
    assert_eq!(
        query_row_count(&after),
        2,
        "restored server should return cpu rows: {after}"
    );
}
