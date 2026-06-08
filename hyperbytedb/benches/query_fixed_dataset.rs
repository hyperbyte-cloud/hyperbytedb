//! Fixed-dataset TimeseriesQL query benchmarks (`cargo bench --bench query_fixed_dataset`).
//!
//! Dataset profile via `BENCH_DATASET`:
//!   - `small`  (default) — 10k points, 10 hosts, `cpu`
//!   - `medium` — 1M points, 100 hosts, `cpu` + `mem` + `disk`
//!   - `large`  — 10M points, 1000 hosts, `cpu`

mod support;

use std::sync::{Arc, OnceLock};

use criterion::{Criterion, Throughput, black_box, criterion_group};
use hyperbytedb::ports::query::QueryService;
use hyperbytedb::timeseriesql;
use support::{BenchEnv, DB, DatasetProfile, setup};

fn shared_env() -> &'static BenchEnv {
    static ENV: OnceLock<BenchEnv> = OnceLock::new();
    ENV.get_or_init(|| setup(DatasetProfile::from_env()))
}

fn bench_parse(c: &mut Criterion) {
    let env = shared_env();
    let q = "SELECT mean(idle) FROM cpu WHERE host = 'host1' GROUP BY time(1h)";
    let mut group = c.benchmark_group(format!("query_parse_{}", env.profile.label()));
    group.throughput(Throughput::Elements(1));
    group.bench_function("select_aggregate", |b| {
        b.iter(|| {
            timeseriesql::parse(black_box(q)).unwrap();
        });
    });
    group.finish();
}

fn bench_execute(c: &mut Criterion, group_name: &str, query: &str, throughput: Throughput) {
    let env = shared_env();
    let svc = Arc::new(env.query_service.clone());
    let rt = &env.rt;
    let q = query.to_string();

    let mut group = c.benchmark_group(format!("{group_name}_{}", env.profile.label()));
    group.throughput(throughput);
    group.bench_function("execute", |b| {
        b.iter(|| {
            rt.block_on(svc.execute_query(DB, black_box(&q), None, None))
                .unwrap();
        });
    });
    group.finish();
}

fn bench_metadata(c: &mut Criterion) {
    bench_execute(
        c,
        "query_metadata_show_measurements",
        "SHOW MEASUREMENTS",
        Throughput::Elements(1),
    );
    bench_execute(
        c,
        "query_metadata_show_tag_keys",
        "SHOW TAG KEYS FROM cpu",
        Throughput::Elements(1),
    );
}

fn bench_point_reads(c: &mut Criterion) {
    bench_execute(
        c,
        "query_point_limit10",
        "SELECT * FROM cpu LIMIT 10",
        Throughput::Elements(10),
    );
    bench_execute(
        c,
        "query_point_limit1000",
        "SELECT * FROM cpu LIMIT 1000",
        Throughput::Elements(1000),
    );
}

fn bench_aggregates(c: &mut Criterion) {
    bench_execute(
        c,
        "query_aggregate_mean",
        "SELECT mean(idle) FROM cpu",
        Throughput::Elements(1),
    );
    bench_execute(
        c,
        "query_aggregate_group_by_time",
        "SELECT mean(idle) FROM cpu GROUP BY time(1h)",
        Throughput::Elements(1),
    );
    bench_execute(
        c,
        "query_aggregate_group_by_tag",
        "SELECT mean(idle) FROM cpu GROUP BY host",
        Throughput::Elements(1),
    );
}

fn bench_filtered(c: &mut Criterion) {
    bench_execute(
        c,
        "query_filtered_host",
        "SELECT * FROM cpu WHERE host = 'host1' LIMIT 100",
        Throughput::Elements(100),
    );
    bench_execute(
        c,
        "query_time_range",
        "SELECT * FROM cpu WHERE time >= 1700000000000000000 AND time < 1700003600000000000 LIMIT 100",
        Throughput::Elements(100),
    );
}

criterion_group!(
    benches,
    bench_parse,
    bench_metadata,
    bench_point_reads,
    bench_aggregates,
    bench_filtered,
);

unsafe extern "C" {
    fn _exit(status: i32) -> !;
}

fn main() {
    benches();
    // Skip libc++/chDB atexit handlers that abort in short-lived bench binaries.
    unsafe { _exit(0) };
}
