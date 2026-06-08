//! InfluxDB line protocol ingestion benchmarks (`cargo bench --bench ingestion_line_protocol`).
//!
//! Measures parse-only, parse + metadata registration, and full WAL append on a temp RocksDB.

use std::sync::Arc;
use std::time::{Duration, Instant};

use criterion::{Criterion, Throughput, black_box, criterion_group, criterion_main};
use hyperbytedb::adapters::metadata::rocksdb_meta::RocksDbMetadata;
use hyperbytedb::adapters::wal::batching_wal::BatchingWal;
use hyperbytedb::adapters::wal::rocksdb_wal::RocksDbWal;
use hyperbytedb::application::ingest_metadata::{
    IngestCardinalityLimits, IngestSchemaCache, prepare_batch_metadata,
};
use hyperbytedb::application::line_protocol::parse_line_body_to_points;
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

fn build_line_protocol_body(n: usize) -> Vec<u8> {
    let mut buf = String::with_capacity(n * 64);
    for i in 0..n {
        use std::fmt::Write;
        writeln!(
            buf,
            "bench,host=bench v={:.3} {}",
            i as f64 * 0.001,
            1_700_000_000_000_i64 + i as i64,
        )
        .unwrap();
    }
    buf.into_bytes()
}

fn bench_parse(c: &mut Criterion) {
    let body = build_line_protocol_body(BATCH as usize);
    let mut group = c.benchmark_group("line_protocol_parse");
    group.throughput(Throughput::Elements(BATCH));
    group.bench_function(format!("parse_{BATCH}"), |b| {
        b.iter(|| {
            parse_line_body_to_points(black_box(&body), Some("ms")).unwrap();
        });
    });
    group.finish();
}

fn bench_metadata(c: &mut Criterion) {
    let body = build_line_protocol_body(BATCH as usize);
    let rt = Runtime::new().expect("runtime");
    let tmpdir = tempfile::tempdir().expect("tmpdir");
    let meta_dir = tmpdir.path().join("meta");
    std::fs::create_dir_all(&meta_dir).unwrap();
    let metadata: Arc<RocksDbMetadata> = Arc::new(RocksDbMetadata::open(&meta_dir).unwrap());
    rt.block_on(metadata.create_database("benchdb")).unwrap();
    let meta_port: Arc<dyn MetadataPort> = metadata.clone();

    let limits = IngestCardinalityLimits::default();
    let cache = IngestSchemaCache::new();

    let mut group = c.benchmark_group("line_protocol_metadata");
    group.throughput(Throughput::Elements(BATCH));
    group.bench_function(format!("parse_plus_metadata_{BATCH}"), |b| {
        b.iter(|| {
            let points = parse_line_body_to_points(black_box(&body), Some("ms")).expect("parse");
            rt.block_on(prepare_batch_metadata(
                &meta_port,
                "benchdb",
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
    let body = build_line_protocol_body(BATCH as usize);
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

    let mut group = c.benchmark_group("line_protocol_wal");
    group.throughput(Throughput::Elements(BATCH));
    group.sample_size(20);
    group.bench_function(format!("metadata_plus_wal_append_{BATCH}"), |b| {
        b.iter(|| {
            let points = parse_line_body_to_points(black_box(&body), Some("ms")).expect("parse");
            rt.block_on(prepare_batch_metadata(
                &meta_port,
                "benchdb",
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

// ---------- Concurrent throughput benchmarks ----------
//
// Each sweep runs `n` independent operations in parallel per Criterion
// iteration and reports aggregate throughput (`Throughput::Elements(BATCH * n)`).
// Compare the `c1_*` baseline against the highest concurrency level to see
// the effective scaling factor on the host. Sub-linear scaling exposes
// internal contention (RwLocks in the schema cache, RocksDB write stall,
// fsync queue, etc).

fn bench_parse_concurrent(c: &mut Criterion) {
    let body: Arc<Vec<u8>> = Arc::new(build_line_protocol_body(BATCH as usize));
    let mut group = c.benchmark_group("line_protocol_parse_concurrent");
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
                                let _ = parse_line_body_to_points(
                                    black_box(body.as_slice()),
                                    Some("ms"),
                                )
                                .expect("parse");
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
    let body: Arc<Vec<u8>> = Arc::new(build_line_protocol_body(BATCH as usize));
    let rt = build_concurrent_runtime();
    let tmpdir = tempfile::tempdir().expect("tmpdir");
    let meta_dir = tmpdir.path().join("meta_concurrent");
    std::fs::create_dir_all(&meta_dir).unwrap();
    let metadata: Arc<RocksDbMetadata> = Arc::new(RocksDbMetadata::open(&meta_dir).unwrap());
    rt.block_on(metadata.create_database("benchdb")).unwrap();
    let meta_port: Arc<dyn MetadataPort> = metadata.clone();
    let cache = Arc::new(IngestSchemaCache::new());
    let limits = IngestCardinalityLimits::default();

    let mut group = c.benchmark_group("line_protocol_metadata_concurrent");
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
                            let body = Arc::clone(&body);
                            let meta_port = Arc::clone(&meta_port);
                            let cache = Arc::clone(&cache);
                            set.spawn(async move {
                                let points = parse_line_body_to_points(body.as_slice(), Some("ms"))
                                    .expect("parse");
                                prepare_batch_metadata(
                                    &meta_port,
                                    "benchdb",
                                    &points,
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
    let body: Arc<Vec<u8>> = Arc::new(build_line_protocol_body(BATCH as usize));
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

    let mut group = c.benchmark_group("line_protocol_wal_concurrent");
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
                                let points = parse_line_body_to_points(body.as_slice(), Some("ms"))
                                    .expect("parse");
                                prepare_batch_metadata(
                                    &meta_port,
                                    "benchdb",
                                    &points,
                                    limits,
                                    Some(cache.as_ref()),
                                )
                                .await
                                .expect("metadata");
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

/// Concurrent WAL append benchmarks routed through `BatchingWal`, the
/// production write path. See the equivalent comment in
/// `ingestion_columnar.rs::bench_wal_batched_concurrent` for why this
/// exists alongside `bench_wal_concurrent` (which calls
/// `RocksDbWal::append` directly and is bound by the RocksDB write
/// thread). The aggregate throughput here should scale near-linearly
/// with concurrency until the batcher's `db.write(WriteBatch)` becomes
/// the bottleneck.
fn bench_wal_batched_concurrent(c: &mut Criterion) {
    let body: Arc<Vec<u8>> = Arc::new(build_line_protocol_body(BATCH as usize));
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
    let wal: Arc<BatchingWal> =
        rt.block_on(async { BatchingWal::new(raw_wal.clone(), 2048, 512, Duration::ZERO) });
    let cache = Arc::new(IngestSchemaCache::new());
    let limits = IngestCardinalityLimits::default();

    let mut group = c.benchmark_group("line_protocol_wal_batched_concurrent");
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
                                let points = parse_line_body_to_points(body.as_slice(), Some("ms"))
                                    .expect("parse");
                                prepare_batch_metadata(
                                    &meta_port,
                                    "benchdb",
                                    &points,
                                    limits,
                                    Some(cache.as_ref()),
                                )
                                .await
                                .expect("metadata");
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
    bench_parse,
    bench_metadata,
    bench_wal,
    bench_parse_concurrent,
    bench_metadata_concurrent,
    bench_wal_concurrent,
    bench_wal_batched_concurrent,
);
criterion_main!(benches);
