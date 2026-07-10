//! Concurrent access tests.
//!
//! Verifies Hyperbytedb handles multiple simultaneous writers and readers
//! without data corruption or panics.

use std::sync::Arc;

use hyperbytedb::ports::ingestion::{IngestionPort, WritePayloadFormat};

use super::TestContext;

/// Wrap ingestion as a trait object Arc for sharing across tasks.
fn shared_ingestion(ctx: &TestContext) -> Arc<dyn IngestionPort> {
    Arc::new(
        hyperbytedb::application::ingestion_service::IngestionServiceImpl::new(
            ctx.wal.clone(),
            ctx.metadata.clone(),
            100_000,
            10_000,
            0,
        ),
    )
}

#[tokio::test]
async fn concurrent_writers_no_data_loss() {
    let ctx = TestContext::new_no_chdb().unwrap();
    ctx.metadata.create_database("concdb").await.unwrap();

    let ingestion = shared_ingestion(&ctx);
    let num_writers = 10usize;
    let points_per_writer = 50usize;
    let mut handles = Vec::new();

    for writer_id in 0..num_writers {
        let ingestion = ingestion.clone();
        let handle = tokio::spawn(async move {
            for i in 0..points_per_writer {
                let ts = (writer_id * 1_000_000 + i) as i64 * 1_000_000_000i64;
                let line = format!(
                    "cpu,writer={} value={}.0 {}",
                    writer_id,
                    writer_id * 100 + i,
                    ts
                );
                ingestion
                    .ingest(
                        "concdb",
                        None,
                        None,
                        line.as_bytes(),
                        WritePayloadFormat::LineProtocol,
                    )
                    .await
                    .unwrap();
            }
        });
        handles.push(handle);
    }

    for handle in handles {
        handle.await.unwrap();
    }

    let entries = ctx.wal.read_from(1).await.unwrap();
    let total_points: usize = entries.iter().map(|(_, e)| e.points.len()).sum();
    assert_eq!(
        total_points,
        num_writers * points_per_writer,
        "All points from all writers should be in WAL: expected {}, got {}",
        num_writers * points_per_writer,
        total_points
    );
}

#[tokio::test]
async fn concurrent_writers_different_measurements() {
    let ctx = TestContext::new_no_chdb().unwrap();
    ctx.metadata.create_database("concdb").await.unwrap();

    let ingestion = shared_ingestion(&ctx);
    let measurements = vec!["cpu", "memory", "disk", "network"];
    let mut handles = Vec::new();

    for (idx, meas) in measurements.iter().enumerate() {
        let ingestion = ingestion.clone();
        let meas = meas.to_string();
        let handle = tokio::spawn(async move {
            for i in 0..20usize {
                let ts = (idx * 1_000_000 + i) as i64 * 1_000_000_000i64;
                let line = format!("{} value={}.0 {}", meas, i, ts);
                ingestion
                    .ingest(
                        "concdb",
                        None,
                        None,
                        line.as_bytes(),
                        WritePayloadFormat::LineProtocol,
                    )
                    .await
                    .unwrap();
            }
        });
        handles.push(handle);
    }

    for handle in handles {
        handle.await.unwrap();
    }

    let stored_measurements = ctx.metadata.list_measurements("concdb").await.unwrap();
    for meas in &measurements {
        assert!(
            stored_measurements.contains(&meas.to_string()),
            "Measurement '{}' should be registered after concurrent writes",
            meas
        );
    }
}

#[tokio::test]
async fn concurrent_readers_and_writers() {
    let ctx = TestContext::new_no_chdb().unwrap();
    ctx.metadata.create_database("rwdb").await.unwrap();

    let ingestion = shared_ingestion(&ctx);

    // Seed some initial data
    ingestion
        .ingest(
            "rwdb",
            None,
            None,
            b"cpu value=1.0 1000000000\nmemory value=2.0 2000000000",
            WritePayloadFormat::LineProtocol,
        )
        .await
        .unwrap();

    let mut handles = Vec::new();

    // Spawn writers
    for writer_id in 0..5u64 {
        let ingestion = ingestion.clone();
        let handle = tokio::spawn(async move {
            for i in 0..10u64 {
                let ts = (writer_id * 1_000_000 + i + 100) as i64 * 1_000_000_000i64;
                let line = format!("cpu,writer={} value={}.0 {}", writer_id, i, ts);
                ingestion
                    .ingest(
                        "rwdb",
                        None,
                        None,
                        line.as_bytes(),
                        WritePayloadFormat::LineProtocol,
                    )
                    .await
                    .unwrap();
            }
        });
        handles.push(handle);
    }

    // Spawn readers (metadata queries that don't need chDB)
    for _ in 0..5 {
        let metadata = ctx.metadata.clone();
        let handle = tokio::spawn(async move {
            for _ in 0..10 {
                let _measurements = metadata.list_measurements("rwdb").await.unwrap();
                let _dbs = metadata.list_databases().await.unwrap();
            }
        });
        handles.push(handle);
    }

    for handle in handles {
        handle
            .await
            .expect("Concurrent reader/writer should not panic");
    }

    let entries = ctx.wal.read_from(1).await.unwrap();
    let total_points: usize = entries.iter().map(|(_, e)| e.points.len()).sum();
    assert!(
        total_points >= 52,
        "Should have at least 2 seed + 50 concurrent = 52 points, got {}",
        total_points
    );
}

#[tokio::test]
async fn concurrent_database_operations() {
    let ctx = TestContext::new_no_chdb().unwrap();

    let mut handles = Vec::new();

    for i in 0..10u32 {
        let metadata = ctx.metadata.clone();
        let handle = tokio::spawn(async move {
            let db_name = format!("db_{}", i);
            metadata.create_database(&db_name).await.unwrap();
            let dbs = metadata.list_databases().await.unwrap();
            assert!(
                dbs.iter().any(|d| d.name == db_name),
                "Database '{}' should exist after create",
                db_name
            );
        });
        handles.push(handle);
    }

    for handle in handles {
        handle.await.unwrap();
    }

    let dbs = ctx.metadata.list_databases().await.unwrap();
    assert_eq!(
        dbs.len(),
        10,
        "All 10 concurrently-created databases should exist"
    );
}

#[tokio::test]
async fn concurrent_write_same_measurement() {
    let ctx = TestContext::new_no_chdb().unwrap();
    ctx.metadata.create_database("concdb").await.unwrap();

    let ingestion = shared_ingestion(&ctx);
    let num_tasks = 20usize;
    let mut handles = Vec::new();

    for i in 0..num_tasks {
        let ingestion = ingestion.clone();
        let handle = tokio::spawn(async move {
            let ts = (i as i64 + 1) * 1_000_000_000i64;
            let line = format!("cpu,host=same value={}.0 {}", i, ts);
            ingestion
                .ingest(
                    "concdb",
                    None,
                    None,
                    line.as_bytes(),
                    WritePayloadFormat::LineProtocol,
                )
                .await
                .unwrap();
        });
        handles.push(handle);
    }

    for handle in handles {
        handle.await.unwrap();
    }

    let entries = ctx.wal.read_from(1).await.unwrap();
    let total_points: usize = entries.iter().map(|(_, e)| e.points.len()).sum();
    assert_eq!(
        total_points, num_tasks,
        "All concurrent writes to same measurement should succeed"
    );
}
