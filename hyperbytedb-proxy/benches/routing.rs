//! Backend routing benchmarks (`cargo bench --bench routing`).
//!
//! Measures round-robin backend selection and drain-response detection.

mod support;

use std::sync::Arc;

use bytes::Bytes;
use criterion::{Criterion, black_box, criterion_group, criterion_main};
use hyperbytedb_proxy::proxy::looks_like_drain;
use support::{pool_with_active_backends, runtime};

const BACKEND_COUNTS: &[usize] = &[1, 4, 8, 16, 32];

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

fn bench_pick_active(c: &mut Criterion) {
    let rt = runtime();
    let mut group = c.benchmark_group("proxy_pick_active");
    for &n in BACKEND_COUNTS {
        let pool = rt.block_on(pool_with_active_backends(n));
        group.bench_function(format!("pick_{n}_backends"), |b| {
            b.iter(|| {
                let _ = black_box(rt.block_on(pool.pick_active()));
            });
        });
    }
    group.finish();
}

fn bench_pick_active_excluding(c: &mut Criterion) {
    let rt = runtime();
    let mut group = c.benchmark_group("proxy_pick_active_excluding");
    for &n in BACKEND_COUNTS {
        let pool = rt.block_on(pool_with_active_backends(n));
        let snap = rt.block_on(pool.snapshot());
        let exclude = Arc::clone(&snap[0]);
        group.bench_function(format!("pick_excluding_{n}_backends"), |b| {
            b.iter(|| {
                let _ = black_box(rt.block_on(pool.pick_active_excluding(&exclude)));
            });
        });
    }
    group.finish();
}

fn bench_pick_active_concurrent(c: &mut Criterion) {
    let rt = runtime();
    let n = 8;
    let pool = rt.block_on(pool_with_active_backends(n));
    let mut group = c.benchmark_group("proxy_pick_active_concurrent");
    for concurrency in concurrency_levels() {
        group.bench_function(format!("pick_{n}_backends_c{concurrency}"), |b| {
            b.iter(|| {
                rt.block_on(async {
                    let mut handles = Vec::with_capacity(concurrency);
                    for _ in 0..concurrency {
                        let pool = Arc::clone(&pool);
                        handles.push(tokio::spawn(async move { pool.pick_active().await }));
                    }
                    for h in handles {
                        let _ = black_box(h.await.unwrap());
                    }
                });
            });
        });
    }
    group.finish();
}

fn bench_looks_like_drain(c: &mut Criterion) {
    let drain_body = Bytes::from_static(
        br#"{"status":"warn","message":"node is Draining, not accepting writes"}"#,
    );
    let success_body = Bytes::from_static(br#"{"status":"pass","message":"ok"}"#);
    let unrelated_body = Bytes::from_static(br#"{"status":"fail","message":"bad request"}"#);
    let binary_body = Bytes::from_static(&[0xff, 0xfe, 0xfd]);

    let mut group = c.benchmark_group("proxy_looks_like_drain");
    group.bench_function("drain_json_pass", |b| {
        b.iter(|| black_box(looks_like_drain(black_box(&drain_body))));
    });
    group.bench_function("drain_json_fail", |b| {
        b.iter(|| black_box(looks_like_drain(black_box(&unrelated_body))));
    });
    group.bench_function("success_body", |b| {
        b.iter(|| black_box(looks_like_drain(black_box(&success_body))));
    });
    group.bench_function("binary_body", |b| {
        b.iter(|| black_box(looks_like_drain(black_box(&binary_body))));
    });
    group.finish();
}

criterion_group!(
    benches,
    bench_pick_active,
    bench_pick_active_excluding,
    bench_pick_active_concurrent,
    bench_looks_like_drain,
);
criterion_main!(benches);
