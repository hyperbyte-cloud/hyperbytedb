//! Flush service benchmarks (`cargo bench --bench flush_service`).
//!
//! Dataset profile via `BENCH_DATASET`:
//!   - `small`  (default) — 10k points
//!   - `medium` — 1M points
//!   - `large`  — 10M points
//!
//! `flush_drain_large` is skipped unless `BENCH_FLUSH_DRAIN_LARGE=1`.

mod support;

use std::sync::{Arc, OnceLock};

use criterion::{BatchSize, Criterion, Throughput, criterion_group};
use hyperbytedb::application::flush_service::FlushServiceImpl;
use support::{DatasetProfile, FlushBenchEnv, ingest_points, seed_wal_only, setup_flush};

const INCREMENTAL_BATCH: u64 = 1000;

fn shared_env() -> &'static FlushBenchEnv {
    static ENV: OnceLock<FlushBenchEnv> = OnceLock::new();
    ENV.get_or_init(|| setup_flush(DatasetProfile::from_env()))
}

fn sample_size(profile: DatasetProfile) -> usize {
    match profile {
        DatasetProfile::Small => 20,
        DatasetProfile::Medium | DatasetProfile::Large => 10,
    }
}

fn bench_flush_full(c: &mut Criterion) {
    let env = shared_env();
    let profile = env.profile;
    let label = profile.label();
    let points = profile.point_count() as u64;

    let mut group = c.benchmark_group(format!("flush_full_{label}"));
    group.throughput(Throughput::Elements(points));
    group.sample_size(sample_size(profile));
    group.bench_function(format!("flush_all_{label}"), |b| {
        let rt = &env.rt;
        let flush = Arc::clone(&env.flush_service);
        let ingestion = &env.ingestion;
        b.iter_batched(
            || seed_wal_only(rt, ingestion, profile),
            |()| {
                rt.block_on(flush.flush()).expect("flush");
            },
            BatchSize::SmallInput,
        );
    });
    group.finish();
}

fn bench_flush_incremental(c: &mut Criterion) {
    let env = shared_env();
    let profile = env.profile;
    let label = profile.label();

    // Baseline: seed full dataset and flush once so chDB schema exists and WAL is empty.
    seed_wal_only(&env.rt, &env.ingestion, profile);
    env.rt
        .block_on(env.flush_service.flush())
        .expect("initial flush");

    let mut group = c.benchmark_group(format!("flush_incremental_{label}"));
    group.throughput(Throughput::Elements(INCREMENTAL_BATCH));
    group.sample_size(sample_size(profile));
    group.bench_function(format!("flush_{INCREMENTAL_BATCH}_{label}"), |b| {
        let rt = &env.rt;
        let flush = Arc::clone(&env.flush_service);
        let ingestion = &env.ingestion;
        b.iter_batched(
            || ingest_points(rt, ingestion, INCREMENTAL_BATCH as usize),
            |()| {
                rt.block_on(flush.flush()).expect("flush");
            },
            BatchSize::SmallInput,
        );
    });
    group.finish();
}

fn bench_flush_drain(c: &mut Criterion) {
    let env = shared_env();
    let profile = env.profile;

    if profile == DatasetProfile::Large {
        let enabled = std::env::var("BENCH_FLUSH_DRAIN_LARGE")
            .ok()
            .map(|v| v == "1" || v.eq_ignore_ascii_case("true"))
            .unwrap_or(false);
        if !enabled {
            eprintln!("skipping flush_drain_large (set BENCH_FLUSH_DRAIN_LARGE=1 to enable)");
            return;
        }
    }

    let label = profile.label();
    let points = profile.point_count() as u64;

    let mut group = c.benchmark_group(format!("flush_drain_{label}"));
    group.throughput(Throughput::Elements(points));
    group.sample_size(sample_size(profile));
    group.bench_function(format!("drain_{label}"), |b| {
        let rt = &env.rt;
        let flush: Arc<FlushServiceImpl> = Arc::clone(&env.flush_service);
        let ingestion = &env.ingestion;
        b.iter_batched(
            || seed_wal_only(rt, ingestion, profile),
            |()| {
                rt.block_on(flush.drain()).expect("drain");
            },
            BatchSize::SmallInput,
        );
    });
    group.finish();
}

criterion_group!(
    benches,
    bench_flush_full,
    bench_flush_incremental,
    bench_flush_drain
);

unsafe extern "C" {
    fn _exit(status: i32) -> !;
}

fn main() {
    benches();
    // Skip libc++/chDB atexit handlers that abort in short-lived bench binaries.
    unsafe { _exit(0) };
}
