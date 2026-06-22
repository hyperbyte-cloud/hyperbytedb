//! Columnar msgpack ingestion benchmarks (`cargo bench --bench ingestion_columnar`).
//!
//! Measures decode-only, decode + metadata registration, and full WAL append on a temp RocksDB.
//! Includes both the original Point-expansion path and the new fast-path.

use std::sync::Arc;
use std::time::{Duration, Instant};

use criterion::{Criterion, Throughput, black_box, criterion_group, criterion_main};
use hyperbytedb::adapters::metadata::rocksdb_meta::RocksDbMetadata;
use hyperbytedb::adapters::wal::batching_wal::BatchingWal;
use hyperbytedb::adapters::wal::rocksdb_wal::RocksDbWal;
use hyperbytedb::application::columnar_msgpack::{
    ColumnarMsgpackBatch, columnar_batch_to_points, columnar_batch_to_record_batch,
    decode_columnar_batch, parse_columnar_msgpack_to_points,
};
use hyperbytedb::application::ingest_metadata::{
    IngestCardinalityLimits, IngestSchemaCache, prepare_batch_metadata, prepare_columnar_metadata,
};
use hyperbytedb::domain::wal::WalEntry;
use hyperbytedb::ports::metadata::MetadataPort;
use hyperbytedb::ports::wal::WalPort;
use tokio::runtime::Runtime;
use tokio::task::JoinSet;

const BATCH: u64 = 1000;

/// Concurrency levels swept by the `*_concurrent` benches. Always includes 1
/// (so we can compare the parallel-fanout overhead vs the sequential bench),
/// 4, 16, and the host's reported parallelism (typically num CPU cores).
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

/// Multi-thread tokio runtime sized to the host's parallelism. Used by all
/// concurrent benches so we don't pay rebuild cost per benchmark group.
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

fn build_batch_bytes(n: usize) -> Vec<u8> {
    let mut tags = std::collections::BTreeMap::new();
    tags.insert("host".into(), "bench".into());
    let values: Vec<f64> = (0..n).map(|i| i as f64 * 0.001).collect();
    let timestamps: Vec<i64> = (0..n).map(|i| 1_700_000_000_000_i64 + i as i64).collect();
    let batch = ColumnarMsgpackBatch {
        measurement: "bench".into(),
        tags,
        field: "v".into(),
        values,
        timestamps: Some(timestamps),
    };
    rmp_serde::to_vec_named(&batch).expect("encode columnar batch")
}

fn build_batch(n: usize) -> ColumnarMsgpackBatch {
    let mut tags = std::collections::BTreeMap::new();
    tags.insert("host".into(), "bench".into());
    let values: Vec<f64> = (0..n).map(|i| i as f64 * 0.001).collect();
    let timestamps: Vec<i64> = (0..n).map(|i| 1_700_000_000_000_i64 + i as i64).collect();
    ColumnarMsgpackBatch {
        measurement: "bench".into(),
        tags,
        field: "v".into(),
        values,
        timestamps: Some(timestamps),
    }
}

// ---------- Original path benchmarks ----------

fn bench_decode(c: &mut Criterion) {
    let body = build_batch_bytes(BATCH as usize);
    let mut group = c.benchmark_group("columnar_decode");
    group.throughput(Throughput::Elements(BATCH));
    group.bench_function(format!("parse_{BATCH}"), |b| {
        b.iter(|| {
            parse_columnar_msgpack_to_points(black_box(&body), Some("ms")).unwrap();
        });
    });
    group.finish();
}

fn bench_metadata(c: &mut Criterion) {
    let body = build_batch_bytes(BATCH as usize);
    let rt = Runtime::new().expect("runtime");
    let tmpdir = tempfile::tempdir().expect("tmpdir");
    let meta_dir = tmpdir.path().join("meta");
    std::fs::create_dir_all(&meta_dir).unwrap();
    let metadata: Arc<RocksDbMetadata> = Arc::new(RocksDbMetadata::open(&meta_dir).unwrap());
    rt.block_on(metadata.create_database("benchdb")).unwrap();
    let meta_port: Arc<dyn MetadataPort> = metadata.clone();

    let limits = IngestCardinalityLimits::default();
    let cache = IngestSchemaCache::new();

    let mut group = c.benchmark_group("columnar_metadata");
    group.throughput(Throughput::Elements(BATCH));
    group.bench_function(format!("prepare_batch_metadata_{BATCH}"), |b| {
        b.iter(|| {
            let points =
                parse_columnar_msgpack_to_points(black_box(&body), Some("ms")).expect("parse");
            rt.block_on(prepare_batch_metadata(
                &meta_port,
                "benchdb",
                "autogen",
                &points,
                limits,
                Some(&cache),
            ))
            .expect("metadata");
        });
    });
    group.finish();
}

fn bench_wal(c: &mut Criterion) {
    let body = build_batch_bytes(BATCH as usize);
    let rt = Runtime::new().expect("runtime");
    let tmpdir = tempfile::tempdir().expect("tmpdir");
    let meta_dir = tmpdir.path().join("meta");
    let wal_dir = tmpdir.path().join("wal");
    std::fs::create_dir_all(&meta_dir).unwrap();
    std::fs::create_dir_all(&wal_dir).unwrap();
    let metadata: Arc<RocksDbMetadata> = Arc::new(RocksDbMetadata::open(&meta_dir).unwrap());
    let wal: Arc<RocksDbWal> = Arc::new(RocksDbWal::open(&wal_dir).unwrap());
    rt.block_on(metadata.create_database("benchdb")).unwrap();
    let meta_port: Arc<dyn MetadataPort> = metadata.clone();

    let limits = IngestCardinalityLimits::default();
    let cache = IngestSchemaCache::new();

    let mut group = c.benchmark_group("columnar_wal");
    group.throughput(Throughput::Elements(BATCH));
    group.sample_size(20);
    group.bench_function(format!("metadata_plus_wal_append_{BATCH}"), |b| {
        b.iter(|| {
            let points =
                parse_columnar_msgpack_to_points(black_box(&body), Some("ms")).expect("parse");
            rt.block_on(prepare_batch_metadata(
                &meta_port,
                "benchdb",
                "autogen",
                &points,
                limits,
                Some(&cache),
            ))
            .expect("metadata");
            let entry = WalEntry {
                database: "benchdb".into(),
                retention_policy: "autogen".into(),
                points,
                origin_node_id: 0,
            };
            rt.block_on(wal.append(entry)).expect("wal");
        });
    });
    group.finish();
}

// ---------- Fast-path benchmarks ----------

fn bench_decode_fast(c: &mut Criterion) {
    let body = build_batch_bytes(BATCH as usize);
    let mut group = c.benchmark_group("columnar_decode_fast");
    group.throughput(Throughput::Elements(BATCH));

    group.bench_function(format!("decode_only_{BATCH}"), |b| {
        b.iter(|| {
            decode_columnar_batch(black_box(&body)).unwrap();
        });
    });

    group.bench_function(format!("decode_to_points_{BATCH}"), |b| {
        b.iter(|| {
            let wire = decode_columnar_batch(black_box(&body)).unwrap();
            columnar_batch_to_points(&wire, Some("ms")).unwrap();
        });
    });

    group.bench_function(format!("decode_to_record_batch_{BATCH}"), |b| {
        b.iter(|| {
            let wire = decode_columnar_batch(black_box(&body)).unwrap();
            columnar_batch_to_record_batch(black_box(&wire), Some("ms")).unwrap();
        });
    });

    group.finish();
}

fn bench_metadata_fast(c: &mut Criterion) {
    let batch = build_batch(BATCH as usize);
    let rt = Runtime::new().expect("runtime");
    let tmpdir = tempfile::tempdir().expect("tmpdir");
    let meta_dir = tmpdir.path().join("meta");
    std::fs::create_dir_all(&meta_dir).unwrap();
    let metadata: Arc<RocksDbMetadata> = Arc::new(RocksDbMetadata::open(&meta_dir).unwrap());
    rt.block_on(metadata.create_database("benchdb")).unwrap();
    let meta_port: Arc<dyn MetadataPort> = metadata.clone();

    let limits = IngestCardinalityLimits::default();
    let cache = IngestSchemaCache::new();

    let mut group = c.benchmark_group("columnar_metadata_fast");
    group.throughput(Throughput::Elements(BATCH));
    group.bench_function(format!("prepare_columnar_metadata_{BATCH}"), |b| {
        b.iter(|| {
            rt.block_on(prepare_columnar_metadata(
                &meta_port,
                "benchdb",
                black_box(&batch),
                limits,
                Some(&cache),
            ))
            .expect("metadata");
        });
    });
    group.finish();
}

fn bench_wal_fast(c: &mut Criterion) {
    let body = build_batch_bytes(BATCH as usize);
    let rt = Runtime::new().expect("runtime");
    let tmpdir = tempfile::tempdir().expect("tmpdir");
    let meta_dir = tmpdir.path().join("meta_fast");
    let wal_dir = tmpdir.path().join("wal_fast");
    std::fs::create_dir_all(&meta_dir).unwrap();
    std::fs::create_dir_all(&wal_dir).unwrap();
    let metadata: Arc<RocksDbMetadata> = Arc::new(RocksDbMetadata::open(&meta_dir).unwrap());
    let wal: Arc<RocksDbWal> = Arc::new(RocksDbWal::open(&wal_dir).unwrap());
    rt.block_on(metadata.create_database("benchdb")).unwrap();
    let meta_port: Arc<dyn MetadataPort> = metadata.clone();

    let limits = IngestCardinalityLimits::default();
    let cache = IngestSchemaCache::new();

    let mut group = c.benchmark_group("columnar_wal_fast");
    group.throughput(Throughput::Elements(BATCH));
    group.sample_size(20);
    group.bench_function(format!("fast_metadata_plus_wal_append_{BATCH}"), |b| {
        b.iter(|| {
            let wire = decode_columnar_batch(black_box(&body)).expect("decode");
            rt.block_on(prepare_columnar_metadata(
                &meta_port,
                "benchdb",
                &wire,
                limits,
                Some(&cache),
            ))
            .expect("metadata");
            let points = columnar_batch_to_points(&wire, Some("ms")).expect("expand");
            let entry = WalEntry {
                database: "benchdb".into(),
                retention_policy: "autogen".into(),
                points,
                origin_node_id: 0,
            };
            rt.block_on(wal.append(entry)).expect("wal");
        });
    });
    group.finish();
}

// ---------- Concurrent throughput benchmarks ----------
//
// Each sweep runs `n` independent operations in parallel per Criterion
// iteration and reports aggregate throughput (`Throughput::Elements(BATCH * n)`).
// Compare the `c1_*` baseline against the highest concurrency level to see
// the effective scaling factor on the host. A perfectly scaling pipeline
// would show throughput ≈ `n × (c1 throughput)`; sub-linear scaling reveals
// internal contention (RwLocks in the schema cache, RocksDB write stall,
// fsync queue, etc).

fn bench_decode_concurrent(c: &mut Criterion) {
    let body: Arc<Vec<u8>> = Arc::new(build_batch_bytes(BATCH as usize));
    let mut group = c.benchmark_group("columnar_decode_concurrent");
    group.sample_size(20);

    for &n in &concurrency_levels() {
        group.throughput(Throughput::Elements(BATCH * n as u64));
        group.bench_function(format!("c{n}_batch{BATCH}"), |b| {
            b.iter_custom(|iters| {
                let start = Instant::now();
                for _ in 0..iters {
                    std::thread::scope(|scope| {
                        for _ in 0..n {
                            let body = Arc::clone(&body);
                            scope.spawn(move || {
                                let _ = decode_columnar_batch(black_box(body.as_slice()))
                                    .expect("decode");
                            });
                        }
                    });
                }
                start.elapsed()
            });
        });
    }
    group.finish();
}

fn bench_metadata_concurrent(c: &mut Criterion) {
    let batch = Arc::new(build_batch(BATCH as usize));
    let rt = build_concurrent_runtime();
    let tmpdir = tempfile::tempdir().expect("tmpdir");
    let meta_dir = tmpdir.path().join("meta_concurrent");
    std::fs::create_dir_all(&meta_dir).unwrap();
    let metadata: Arc<RocksDbMetadata> = Arc::new(RocksDbMetadata::open(&meta_dir).unwrap());
    rt.block_on(metadata.create_database("benchdb")).unwrap();
    let meta_port: Arc<dyn MetadataPort> = metadata.clone();
    let cache = Arc::new(IngestSchemaCache::new());
    let limits = IngestCardinalityLimits::default();

    let mut group = c.benchmark_group("columnar_metadata_concurrent");
    group.sample_size(20);

    for &n in &concurrency_levels() {
        group.throughput(Throughput::Elements(BATCH * n as u64));
        group.bench_function(format!("c{n}_batch{BATCH}"), |b| {
            b.iter_custom(|iters| {
                let start = Instant::now();
                rt.block_on(async {
                    for _ in 0..iters {
                        let mut set = JoinSet::new();
                        for _ in 0..n {
                            let batch = Arc::clone(&batch);
                            let meta_port = Arc::clone(&meta_port);
                            let cache = Arc::clone(&cache);
                            set.spawn(async move {
                                prepare_columnar_metadata(
                                    &meta_port,
                                    "benchdb",
                                    batch.as_ref(),
                                    limits,
                                    Some(cache.as_ref()),
                                )
                                .await
                                .expect("metadata");
                            });
                        }
                        while set.join_next().await.is_some() {}
                    }
                });
                start.elapsed()
            });
        });
    }
    group.finish();
}

fn bench_wal_concurrent(c: &mut Criterion) {
    let body: Arc<Vec<u8>> = Arc::new(build_batch_bytes(BATCH as usize));
    let rt = build_concurrent_runtime();
    let tmpdir = tempfile::tempdir().expect("tmpdir");
    let meta_dir = tmpdir.path().join("meta_wal_concurrent");
    let wal_dir = tmpdir.path().join("wal_concurrent");
    std::fs::create_dir_all(&meta_dir).unwrap();
    std::fs::create_dir_all(&wal_dir).unwrap();
    let metadata: Arc<RocksDbMetadata> = Arc::new(RocksDbMetadata::open(&meta_dir).unwrap());
    let wal: Arc<RocksDbWal> = Arc::new(RocksDbWal::open(&wal_dir).unwrap());
    rt.block_on(metadata.create_database("benchdb")).unwrap();
    let meta_port: Arc<dyn MetadataPort> = metadata.clone();
    let cache = Arc::new(IngestSchemaCache::new());
    let limits = IngestCardinalityLimits::default();

    let mut group = c.benchmark_group("columnar_wal_concurrent");
    // WAL append touches RocksDB write path + fsync — keep sample size modest
    // so the bench finishes in reasonable wall time even at high concurrency.
    group.sample_size(10);

    for &n in &concurrency_levels() {
        group.throughput(Throughput::Elements(BATCH * n as u64));
        group.bench_function(format!("c{n}_batch{BATCH}"), |b| {
            b.iter_custom(|iters| {
                let start = Instant::now();
                rt.block_on(async {
                    for _ in 0..iters {
                        let mut set = JoinSet::new();
                        for _ in 0..n {
                            let body = Arc::clone(&body);
                            let meta_port = Arc::clone(&meta_port);
                            let wal = Arc::clone(&wal);
                            let cache = Arc::clone(&cache);
                            set.spawn(async move {
                                let wire = decode_columnar_batch(body.as_slice()).expect("decode");
                                prepare_columnar_metadata(
                                    &meta_port,
                                    "benchdb",
                                    &wire,
                                    limits,
                                    Some(cache.as_ref()),
                                )
                                .await
                                .expect("metadata");
                                let points =
                                    columnar_batch_to_points(&wire, Some("ms")).expect("expand");
                                wal.append(WalEntry {
                                    database: "benchdb".into(),
                                    retention_policy: "autogen".into(),
                                    points,
                                    origin_node_id: 0,
                                })
                                .await
                                .expect("wal");
                            });
                        }
                        while set.join_next().await.is_some() {}
                    }
                });
                start.elapsed()
            });
        });
    }
    group.finish();
}

/// Concurrent WAL append benchmarks routed through `BatchingWal`, which is
/// what the production HTTP write path actually uses. Compare against the
/// `columnar_wal_concurrent` group: that bench calls `RocksDbWal::append`
/// directly (one `db.put_cf` per request) and is bound by RocksDB's write
/// thread. This group fans in through the group-commit batcher so the
/// many concurrent appenders coalesce into a single `db.write(WriteBatch)`,
/// which is what unlocks scaling across cores in production.
fn bench_wal_batched_concurrent(c: &mut Criterion) {
    let body: Arc<Vec<u8>> = Arc::new(build_batch_bytes(BATCH as usize));
    let rt = build_concurrent_runtime();
    let tmpdir = tempfile::tempdir().expect("tmpdir");
    let meta_dir = tmpdir.path().join("meta_wal_batched_concurrent");
    let wal_dir = tmpdir.path().join("wal_batched_concurrent");
    std::fs::create_dir_all(&meta_dir).unwrap();
    std::fs::create_dir_all(&wal_dir).unwrap();
    let metadata: Arc<RocksDbMetadata> = Arc::new(RocksDbMetadata::open(&meta_dir).unwrap());
    let raw_wal: Arc<RocksDbWal> = Arc::new(RocksDbWal::open(&wal_dir).unwrap());
    rt.block_on(metadata.create_database("benchdb")).unwrap();
    let meta_port: Arc<dyn MetadataPort> = metadata.clone();
    // `BatchingWal::new` spawns a background `batcher_loop` task; it
    // must be created inside a tokio runtime context.
    let wal: Arc<BatchingWal> =
        rt.block_on(async { BatchingWal::new(raw_wal.clone(), 2048, 512, Duration::ZERO) });
    let cache = Arc::new(IngestSchemaCache::new());
    let limits = IngestCardinalityLimits::default();

    let mut group = c.benchmark_group("columnar_wal_batched_concurrent");
    group.sample_size(10);

    for &n in &concurrency_levels() {
        group.throughput(Throughput::Elements(BATCH * n as u64));
        group.bench_function(format!("c{n}_batch{BATCH}"), |b| {
            b.iter_custom(|iters| {
                let start = Instant::now();
                rt.block_on(async {
                    for _ in 0..iters {
                        let mut set = JoinSet::new();
                        for _ in 0..n {
                            let body = Arc::clone(&body);
                            let meta_port = Arc::clone(&meta_port);
                            let wal: Arc<dyn hyperbytedb::ports::wal::WalPort> = wal.clone();
                            let cache = Arc::clone(&cache);
                            set.spawn(async move {
                                let wire = decode_columnar_batch(body.as_slice()).expect("decode");
                                prepare_columnar_metadata(
                                    &meta_port,
                                    "benchdb",
                                    &wire,
                                    limits,
                                    Some(cache.as_ref()),
                                )
                                .await
                                .expect("metadata");
                                let points =
                                    columnar_batch_to_points(&wire, Some("ms")).expect("expand");
                                wal.append(WalEntry {
                                    database: "benchdb".into(),
                                    retention_policy: "autogen".into(),
                                    points,
                                    origin_node_id: 0,
                                })
                                .await
                                .expect("wal");
                            });
                        }
                        while set.join_next().await.is_some() {}
                    }
                });
                start.elapsed()
            });
        });
    }
    group.finish();
}

criterion_group!(
    benches,
    bench_decode,
    bench_metadata,
    bench_wal,
    bench_decode_fast,
    bench_metadata_fast,
    bench_wal_fast,
    bench_decode_concurrent,
    bench_metadata_concurrent,
    bench_wal_concurrent,
    bench_wal_batched_concurrent,
);
criterion_main!(benches);
