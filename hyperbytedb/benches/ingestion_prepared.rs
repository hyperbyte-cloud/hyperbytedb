//! Prepared-slot (Arrow WAL) ingest path benchmarks (`cargo bench --bench ingestion_prepared`).
//!
//! Covers the production write path that `ingestion_line_protocol` skips:
//! `IngestionServiceImpl::with_sink` → `build_prepared_wal_slot` (coalesce +
//! Arrow build) → `BatchingWal::append_bundle` (group-commit writer thread,
//! including the per-row `ingest_seq` patch).

use std::fmt::Write as _;
use std::sync::Arc;
use std::time::{Duration, Instant};

use arrow::array::{Float64Array, TimestampNanosecondArray, UInt64Array};
use arrow::datatypes::{DataType, Field, Schema, TimeUnit};
use arrow::record_batch::RecordBatch;
use criterion::{Criterion, Throughput, black_box, criterion_group, criterion_main};
use hyperbytedb::adapters::chdb::native_adapter::ChdbNativeAdapter;
use hyperbytedb::adapters::chdb::session::SharedSession;
use hyperbytedb::adapters::metadata::rocksdb_meta::RocksDbMetadata;
use hyperbytedb::adapters::wal::batching_wal::BatchingWal;
use hyperbytedb::adapters::wal::rocksdb_wal::RocksDbWal;
use hyperbytedb::application::ingestion_service::IngestionServiceImpl;
use hyperbytedb::application::line_protocol::parse_line_body_to_points;
use hyperbytedb::domain::point::{FieldValue, Point};
use hyperbytedb::domain::point_coalesce::group_and_coalesce_by_measurement;
use hyperbytedb::domain::prepared_wal::{PreparedMeasurementBatch, PreparedWalSlot};
use hyperbytedb::domain::wal::WalEntry;
use hyperbytedb::ports::ingestion::{IngestionPort, WritePayloadFormat};
use hyperbytedb::ports::metadata::MetadataPort;
use hyperbytedb::ports::points_sink::PointsSinkPort;
use hyperbytedb::ports::wal::{WalAppendBundle, WalPort};
use tokio::runtime::Runtime;
use tokio::task::JoinSet;

const BATCH: u64 = 1000;
const BASE_NS: i64 = 1_700_000_000_000_000_000;

fn concurrency_levels() -> Vec<usize> {
    let max = std::thread::available_parallelism()
        .map(|n| n.get())
        .unwrap_or(8);
    let mut levels: Vec<usize> = vec![1, 4, 16, max];
    levels.retain(|&n| n <= max);
    levels.sort_unstable();
    levels.dedup();
    levels
}

fn build_concurrent_runtime() -> Runtime {
    let workers = std::thread::available_parallelism()
        .map(|n| n.get())
        .unwrap_or(8);
    tokio::runtime::Builder::new_multi_thread()
        .worker_threads(workers)
        .enable_all()
        .build()
        .expect("runtime")
}

/// Telegraf-style body: two measurements, 50 hosts, strictly increasing
/// timestamps (no partial-line duplicates — the common case).
fn build_two_measurement_body(n: usize) -> Vec<u8> {
    let mut buf = String::with_capacity(n * 72);
    for i in 0..n {
        let host = i % 50;
        let ts = BASE_NS + i as i64 * 1_000_000;
        if i % 2 == 0 {
            writeln!(
                buf,
                "cpu,host=host{host},region=us-west idle={:.2},usage_user={:.2} {ts}",
                90.0 + (i % 10) as f64,
                (i % 10) as f64,
            )
            .unwrap();
        } else {
            writeln!(
                buf,
                "mem,host=host{host} used={:.0},free={:.0} {ts}",
                4_000_000_000.0 + (i % 1000) as f64,
                8_000_000_000.0 - (i % 1000) as f64,
            )
            .unwrap();
        }
    }
    buf.into_bytes()
}

// ---------- End-to-end prepared ingest (parse → metadata → Arrow build → BatchingWal) ----------

struct PreparedIngestEnv {
    rt: Runtime,
    ingestion: Arc<IngestionServiceImpl>,
    wal: Arc<BatchingWal>,
    _tmpdir: tempfile::TempDir,
}

fn setup_prepared_ingest() -> PreparedIngestEnv {
    let rt = build_concurrent_runtime();
    let tmpdir = tempfile::tempdir().expect("tmpdir");
    let root = tmpdir.path();
    let wal_dir = root.join("wal");
    let meta_dir = root.join("meta");
    let chdb_dir = root.join("chdb");
    std::fs::create_dir_all(&wal_dir).unwrap();
    std::fs::create_dir_all(&meta_dir).unwrap();
    std::fs::create_dir_all(&chdb_dir).unwrap();

    let raw_wal = Arc::new(RocksDbWal::open(&wal_dir).unwrap());
    let wal = BatchingWal::new(raw_wal, 2048, 512, Duration::ZERO);
    let metadata = Arc::new(RocksDbMetadata::open(&meta_dir).unwrap());
    rt.block_on(metadata.create_database("benchdb")).unwrap();

    let shared = SharedSession::new_eager(chdb_dir.to_str().unwrap(), 1).unwrap();
    let sink: Arc<dyn PointsSinkPort> = Arc::new(ChdbNativeAdapter::new(shared));

    let wal_port: Arc<dyn WalPort> = wal.clone();
    let ingestion = Arc::new(IngestionServiceImpl::with_sink(
        wal_port,
        Some(sink),
        metadata,
        100_000,
        10_000,
    ));

    PreparedIngestEnv {
        rt,
        ingestion,
        wal,
        _tmpdir: tmpdir,
    }
}

/// Drain WAL + arrow cache (untimed between iterations) so cached prepared
/// slots don't accumulate across the benchmark run.
fn drain_wal(rt: &Runtime, wal: &Arc<BatchingWal>) {
    rt.block_on(async {
        let last = wal.last_sequence().await.expect("last_sequence");
        wal.truncate_before(last + 1).await.expect("truncate");
    });
}

fn bench_ingest_prepared_concurrent(c: &mut Criterion) {
    let env = setup_prepared_ingest();
    let body: Arc<Vec<u8>> = Arc::new(build_two_measurement_body(BATCH as usize));

    let mut group = c.benchmark_group("ingest_prepared_concurrent");
    group.sample_size(10);

    for &n in &concurrency_levels() {
        group.throughput(Throughput::Elements(BATCH * n as u64));
        group.bench_function(format!("c{n}_batch{BATCH}"), |b| {
            b.iter_custom(|iters| {
                let mut total = Duration::ZERO;
                for _ in 0..iters {
                    let start = Instant::now();
                    env.rt.block_on(async {
                        let mut set = JoinSet::new();
                        for _ in 0..n {
                            let body = Arc::clone(&body);
                            let ingestion = Arc::clone(&env.ingestion);
                            set.spawn(async move {
                                ingestion
                                    .ingest(
                                        "benchdb",
                                        None,
                                        None,
                                        body.as_slice(),
                                        WritePayloadFormat::LineProtocol,
                                    )
                                    .await
                                    .expect("ingest");
                            });
                        }
                        while set.join_next().await.is_some() {}
                    });
                    total += start.elapsed();
                    drain_wal(&env.rt, &env.wal);
                }
                total
            });
        });
    }
    group.finish();
}

// ---------- Writer-thread isolation: prepared bundles through BatchingWal ----------

fn build_fact_batch(n: usize) -> Arc<RecordBatch> {
    let schema = Arc::new(Schema::new(vec![
        Field::new(
            "time",
            DataType::Timestamp(TimeUnit::Nanosecond, Some("UTC".into())),
            false,
        ),
        Field::new("origin_node_id", DataType::UInt64, false),
        Field::new("ingest_seq", DataType::UInt64, false),
        Field::new("series_id", DataType::UInt64, false),
        Field::new("v", DataType::Float64, true),
    ]));
    let times: Vec<i64> = (0..n as i64).map(|i| BASE_NS + i * 1_000_000).collect();
    Arc::new(
        RecordBatch::try_new(
            schema,
            vec![
                Arc::new(TimestampNanosecondArray::from(times).with_timezone("UTC")),
                Arc::new(UInt64Array::from(vec![0_u64; n])),
                Arc::new(UInt64Array::from((0..n as u64).collect::<Vec<_>>())),
                Arc::new(UInt64Array::from(
                    (0..n as u64).map(|i| i % 50).collect::<Vec<_>>(),
                )),
                Arc::new(Float64Array::from(
                    (0..n).map(|i| i as f64).collect::<Vec<_>>(),
                )),
            ],
        )
        .unwrap(),
    )
}

fn synthetic_points(n: usize) -> Vec<Point> {
    (0..n)
        .map(|i| {
            let mut tags = std::collections::BTreeMap::new();
            tags.insert("host".to_string(), format!("host{}", i % 50));
            let mut fields = std::collections::BTreeMap::new();
            fields.insert("v".to_string(), FieldValue::Float(i as f64));
            Point {
                measurement: "cpu".to_string(),
                tags,
                fields,
                timestamp: BASE_NS + i as i64 * 1_000_000,
            }
        })
        .collect()
}

fn synthetic_bundle(points: &[Point], batch: &Arc<RecordBatch>) -> WalAppendBundle {
    let n = points.len();
    WalAppendBundle {
        entry: WalEntry {
            database: "benchdb".to_string(),
            retention_policy: "autogen".to_string(),
            points: points.to_vec(),
            origin_node_id: 0,
        },
        prepared: Some(PreparedWalSlot {
            database: "benchdb".to_string(),
            retention_policy: "autogen".to_string(),
            origin_node_id: 0,
            measurements: vec![PreparedMeasurementBatch {
                measurement: "cpu".to_string(),
                table_name: "`benchdb_autogen_cpu`".to_string(),
                series_table_name: "`benchdb_autogen_cpu_series`".to_string(),
                batch: Arc::clone(batch),
                row_count: n,
                min_time: BASE_NS,
                max_time: BASE_NS + (n as i64 - 1) * 1_000_000,
                new_series_batch: None,
            }],
        }),
    }
}

fn bench_wal_prepared_append_concurrent(c: &mut Criterion) {
    const APPENDS_PER_TASK: usize = 16;

    let rt = build_concurrent_runtime();
    let tmpdir = tempfile::tempdir().expect("tmpdir");
    let wal_dir = tmpdir.path().join("wal");
    std::fs::create_dir_all(&wal_dir).unwrap();
    let raw_wal = Arc::new(RocksDbWal::open(&wal_dir).unwrap());
    let wal = BatchingWal::new(raw_wal, 2048, 512, Duration::ZERO);

    let points: Arc<Vec<Point>> = Arc::new(synthetic_points(BATCH as usize));
    let batch = build_fact_batch(BATCH as usize);

    let mut group = c.benchmark_group("wal_prepared_append_concurrent");
    group.sample_size(10);

    for &n in &concurrency_levels() {
        group.throughput(Throughput::Elements(BATCH * (n * APPENDS_PER_TASK) as u64));
        group.bench_function(format!("c{n}_batch{BATCH}"), |b| {
            b.iter_custom(|iters| {
                let mut total = Duration::ZERO;
                for _ in 0..iters {
                    let start = Instant::now();
                    rt.block_on(async {
                        let mut set = JoinSet::new();
                        for _ in 0..n {
                            let points = Arc::clone(&points);
                            let batch = Arc::clone(&batch);
                            let wal = Arc::clone(&wal);
                            set.spawn(async move {
                                for _ in 0..APPENDS_PER_TASK {
                                    wal.append_bundle(synthetic_bundle(&points, &batch))
                                        .await
                                        .expect("append_bundle");
                                }
                            });
                        }
                        while set.join_next().await.is_some() {}
                    });
                    total += start.elapsed();
                    drain_wal(&rt, &wal);
                }
                total
            });
        });
    }
    group.finish();
}

// ---------- Coalesce / group-by-measurement micro ----------

fn bench_coalesce_group(c: &mut Criterion) {
    let body = build_two_measurement_body(BATCH as usize);
    let points = parse_line_body_to_points(&body, None).expect("parse");

    let mut group = c.benchmark_group("coalesce_group");
    group.throughput(Throughput::Elements(BATCH));
    group.bench_function(format!("group_coalesce_{BATCH}"), |b| {
        b.iter(|| {
            black_box(group_and_coalesce_by_measurement(black_box(&points)));
        });
    });
    group.finish();
}

criterion_group!(
    benches,
    bench_coalesce_group,
    bench_wal_prepared_append_concurrent,
    bench_ingest_prepared_concurrent,
);
criterion_main!(benches);
