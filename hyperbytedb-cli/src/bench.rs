//! In-cluster ingest benchmark (`hyperbytedb-cli bench-write`), used by
//! `scripts/linearity/run-incluster-sweep.sh`.
//!
//! Same workload, arguments and `[bench]` output as the Python client it
//! replaces (`scripts/linearity/incluster_bench.py`). That client built every
//! line with an f-string and opened a connection per request; at N=12 its 24
//! pods competed with the database for the host's 24 cores. This one writes
//! lines straight into a byte buffer and reuses connections.

use std::collections::HashMap;
use std::io::Write as _;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::{Duration, Instant};

use clap::{Args, ValueEnum};
use parking_lot::Mutex;

#[derive(Clone, Copy, PartialEq, Eq, ValueEnum)]
pub enum Phase {
    Seed,
    Measure,
    All,
}

#[derive(Args, Clone)]
pub struct BenchArgs {
    /// Comma-separated base URLs, one per node.
    #[arg(long)]
    pub endpoints: String,
    #[arg(long, default_value = "linearity")]
    pub db: String,
    #[arg(long, default_value = "cpu")]
    pub measurement: String,
    /// Series to seed; the measure phase rewrites them.
    #[arg(long)]
    pub series: u64,
    #[arg(long, default_value_t = 2_000_000)]
    pub measure_points: u64,
    #[arg(long, default_value_t = 1000)]
    pub batch: u64,
    #[arg(long, default_value_t = 32)]
    pub concurrency: usize,
    #[arg(long, value_enum, default_value_t = Phase::All)]
    pub phase: Phase,
    /// Clients in the Job; each takes every `workers`-th batch.
    #[arg(long, default_value_t = 1)]
    pub workers: u64,
    /// Defaults to the Job's completion index (`JOB_COMPLETION_INDEX`).
    #[arg(long)]
    pub worker_index: Option<u64>,
    /// Stop the measure phase taking new batches after this many seconds
    /// (0: write every point). For a steady writer during scaling.
    #[arg(long, default_value_t = 0)]
    pub max_secs: u64,
}

impl BenchArgs {
    fn worker_index(&self) -> u64 {
        self.worker_index.unwrap_or(0)
    }
}

/// Not an HTTP status: the request failed in transport (refused, reset).
const TRANSPORT_ERROR: u16 = 599;
const MAX_RETRIES: u32 = 12;
/// How long the untimed replay of failed batches may take.
const REPLAY_DEADLINE: Duration = Duration::from_secs(600);

struct Bench {
    http: reqwest::Client,
    error_kinds: Mutex<HashMap<String, u64>>,
}

impl Bench {
    fn note_error(&self, kind: String) {
        *self.error_kinds.lock().entry(kind).or_insert(0) += 1;
    }

    async fn post(&self, url: &str, body: bytes::Bytes, content_type: &str) -> u16 {
        let resp = self
            .http
            .post(url)
            .header("Content-Type", content_type)
            .body(body)
            .send()
            .await;
        match resp {
            Ok(r) => {
                let status = r.status().as_u16();
                if !matches!(status, 200 | 204 | 429 | 503) {
                    let text = r.text().await.unwrap_or_default();
                    let detail: String = text.chars().take(600).collect();
                    self.note_error(format!("{status} {}", detail.replace('\n', " ")));
                }
                status
            }
            Err(e) => {
                // Refused, reset or timed out: "not now", retried like a 503.
                if !(e.is_connect() || e.is_request() || e.is_timeout()) {
                    self.note_error(format!("0 {e}"));
                }
                TRANSPORT_ERROR
            }
        }
    }
}

fn retryable(code: u16) -> bool {
    matches!(code, 429 | 503 | TRANSPORT_ERROR)
}

/// Cheap jitter in [0.5, 1.5) without a rand dependency.
fn jitter(seed: &mut u64) -> f64 {
    *seed ^= *seed << 13;
    *seed ^= *seed >> 7;
    *seed ^= *seed << 17;
    0.5 + (*seed % 1_000_000) as f64 / 1_000_000.0
}

/// Lines for points `first..last`. With `series`, point i goes to series
/// `i % series` and the timestamp advances one second per full pass, so a
/// measure phase rewrites the seeded series instead of creating new ones.
fn build_body(
    measurement: &str,
    start: u64,
    first: u64,
    last: u64,
    ts_base: u64,
    series: Option<u64>,
) -> Vec<u8> {
    let mut body = Vec::with_capacity(((last - first) as usize) * 48);
    for i in first..last {
        let sid = start + series.map_or(i, |s| i % s);
        let ts = ts_base + series.map_or(i, |s| i / s);
        // `ts` seconds followed by nine nanosecond digits.
        let _ = writeln!(
            body,
            "{measurement},host=s{sid} value=1 {ts}{:09}",
            sid % 1_000_000_000
        );
    }
    body.pop();
    body
}

struct PhaseResult {
    duration: Duration,
    errors: u64,
    retries: u64,
    /// First point index of each batch that never got a 204.
    failed: Vec<u64>,
    /// Points in the batches this worker took (all of its share, unless
    /// `--max-secs` stopped it early).
    points: u64,
    latency: Arc<LatencyHist>,
}

/// Upper bounds (ms) of the write-latency buckets; one more counts the rest.
const LATENCY_BUCKETS_MS: [u64; 24] = [
    1, 2, 3, 5, 7, 10, 15, 20, 30, 50, 70, 100, 150, 200, 300, 500, 700, 1_000, 1_500, 2_000,
    3_000, 5_000, 10_000, 30_000,
];

/// Batch write latency as a client sees it, retries included, in fixed
/// buckets so workers' histograms merge by adding counts.
#[derive(Default)]
struct LatencyHist {
    counts: [std::sync::atomic::AtomicU64; LATENCY_BUCKETS_MS.len() + 1],
}

impl LatencyHist {
    fn record(&self, d: Duration) {
        let ms = d.as_secs_f64() * 1000.0;
        let i = LATENCY_BUCKETS_MS
            .iter()
            .position(|&b| ms <= b as f64)
            .unwrap_or(LATENCY_BUCKETS_MS.len());
        self.counts[i].fetch_add(1, Ordering::Relaxed);
    }

    /// `1:<n>,2:<n>,...,30000:<n>,inf:<n>`.
    fn render(&self) -> String {
        LATENCY_BUCKETS_MS
            .iter()
            .map(|b| b.to_string())
            .chain(std::iter::once("inf".to_string()))
            .zip(self.counts.iter())
            .map(|(b, c)| format!("{b}:{}", c.load(Ordering::Relaxed)))
            .collect::<Vec<_>>()
            .join(",")
    }
}

/// Rewrite each failed batch until it is accepted or `deadline` passes, and
/// return how many were not. Run after the timed phase, so a read-back can
/// expect every point: rewriting a batch is idempotent, because a point is
/// its (series, time) and duplicates collapse on read.
#[allow(clippy::too_many_arguments)]
async fn replay_failed(
    bench: &Bench,
    url: &str,
    measurement: &str,
    failed: &[u64],
    batch: u64,
    count: u64,
    ts_base: u64,
    series: Option<u64>,
    deadline: Instant,
) -> usize {
    let mut seed = 0x2545_F491_4F6C_DD1Du64;
    let mut unrecovered = 0;
    for &base in failed {
        let body = bytes::Bytes::from(build_body(
            measurement,
            0,
            base,
            (base + batch).min(count),
            ts_base,
            series,
        ));
        let mut attempt = 0u32;
        loop {
            if bench.post(url, body.clone(), "text/plain").await == 204 {
                break;
            }
            if Instant::now() >= deadline {
                unrecovered += 1;
                break;
            }
            let wait = (0.05 * 2f64.powi(attempt.min(5) as i32)).min(1.6) * jitter(&mut seed);
            tokio::time::sleep(Duration::from_secs_f64(wait)).await;
            attempt += 1;
        }
    }
    unrecovered
}

#[allow(clippy::too_many_arguments)]
async fn run_phase(
    bench: Arc<Bench>,
    a: &BenchArgs,
    endpoints: &[String],
    count: u64,
    ts_base: u64,
    series: Option<u64>,
    deadline: Option<Instant>,
) -> PhaseResult {
    let latency = Arc::new(LatencyHist::default());
    let batches: Vec<u64> = (0..count)
        .step_by(a.batch as usize)
        .enumerate()
        .filter(|(n, _)| (*n as u64) % a.workers == a.worker_index())
        .map(|(_, b)| b)
        .collect();
    let batches = Arc::new(batches);
    let next = Arc::new(AtomicUsize::new(0));
    let started = Instant::now();
    let mut tasks = Vec::with_capacity(a.concurrency);
    for idx in 0..a.concurrency {
        let bench = bench.clone();
        let batches = batches.clone();
        let next = next.clone();
        let ep = &endpoints[(idx + a.worker_index() as usize) % endpoints.len()];
        let url = format!("{ep}/write?db={}&rp=autogen&precision=ns", a.db);
        let measurement = a.measurement.clone();
        let batch = a.batch;
        let latency = latency.clone();
        let mut seed = 0x9E37_79B9_7F4A_7C15u64 ^ ((idx as u64 + 1) << 17) ^ a.worker_index();
        tasks.push(tokio::spawn(async move {
            let (mut errors, mut retries, mut points) = (0u64, 0u64, 0u64);
            let mut failed = Vec::new();
            loop {
                if deadline.is_some_and(|d| Instant::now() >= d) {
                    break;
                }
                let i = next.fetch_add(1, Ordering::Relaxed);
                let Some(&base) = batches.get(i) else { break };
                let started = Instant::now();
                points += (base + batch).min(count) - base;
                let body = bytes::Bytes::from(build_body(
                    &measurement,
                    0,
                    base,
                    (base + batch).min(count),
                    ts_base,
                    series,
                ));
                let mut code = bench.post(&url, body.clone(), "text/plain").await;
                let mut attempt = 0u32;
                while retryable(code) && attempt < MAX_RETRIES {
                    let wait =
                        (0.05 * 2f64.powi(attempt.min(5) as i32)).min(1.6) * jitter(&mut seed);
                    tokio::time::sleep(Duration::from_secs_f64(wait)).await;
                    attempt += 1;
                    retries += 1;
                    code = bench.post(&url, body.clone(), "text/plain").await;
                }
                latency.record(started.elapsed());
                if retryable(code) {
                    bench.note_error(format!("{code} still rejected after {attempt} retries"));
                }
                if code != 204 {
                    errors += 1;
                    failed.push(base);
                }
            }
            (errors, retries, failed, points)
        }));
    }
    let (mut errors, mut retries, mut failed, mut points) = (0, 0, Vec::new(), 0);
    for t in tasks {
        if let Ok((e, r, f, p)) = t.await {
            errors += e;
            retries += r;
            failed.extend(f);
            points += p;
        }
    }
    PhaseResult {
        duration: started.elapsed(),
        errors,
        retries,
        failed,
        points,
        latency,
    }
}

/// Run the benchmark; the return value is the process exit code.
pub async fn run(mut a: BenchArgs) -> u8 {
    if a.worker_index.is_none() {
        a.worker_index = Some(
            std::env::var("JOB_COMPLETION_INDEX")
                .ok()
                .and_then(|v| v.parse().ok())
                .unwrap_or(0),
        );
    }
    let endpoints: Vec<String> = a
        .endpoints
        .split(',')
        .map(|e| e.trim().trim_end_matches('/').to_string())
        .filter(|e| !e.is_empty())
        .collect();
    if endpoints.is_empty() || a.batch == 0 || a.workers == 0 {
        eprintln!("[bench] FATAL: need endpoints, batch > 0 and workers > 0");
        return 2;
    }
    let http = match reqwest::Client::builder()
        .timeout(Duration::from_secs(60))
        .pool_max_idle_per_host(a.concurrency)
        .build()
    {
        Ok(c) => c,
        Err(e) => {
            eprintln!("[bench] FATAL: http client: {e}");
            return 2;
        }
    };
    let bench = Arc::new(Bench {
        http,
        error_kinds: Mutex::new(HashMap::new()),
    });
    println!(
        "[bench] endpoints={} concurrency={} series={} measure_points={}",
        endpoints.len(),
        a.concurrency,
        a.series,
        a.measure_points
    );

    if matches!(a.phase, Phase::Seed | Phase::All) && a.worker_index() == 0 {
        let form = format!("q=CREATE+DATABASE+{}", a.db);
        let created = bench
            .post(
                &format!("{}/query", endpoints[0]),
                bytes::Bytes::from(form),
                "application/x-www-form-urlencoded",
            )
            .await;
        println!("[bench] create database -> HTTP {created}");
        if !matches!(created, 200 | 204) {
            println!("[bench] FATAL: could not create database; aborting");
            return 2;
        }
    }

    // Prove one write lands before timing millions of them. Retried: with
    // several workers, only worker 0 creates the database.
    let deadline = Instant::now() + Duration::from_secs(60);
    let probe_url = format!("{}/write?db={}&rp=autogen&precision=ns", endpoints[0], a.db);
    let probe = loop {
        let line = format!("{},host=probe value=1 1700000000000000000", a.measurement);
        let code = bench
            .post(&probe_url, bytes::Bytes::from(line), "text/plain")
            .await;
        if code == 204 || Instant::now() > deadline {
            break code;
        }
        tokio::time::sleep(Duration::from_secs(1)).await;
    };
    println!("[bench] probe write -> HTTP {probe}");
    if probe != 204 {
        println!("[bench] FATAL: probe write rejected; aborting");
        return 3;
    }

    if matches!(a.phase, Phase::Seed | Phase::All) {
        let r = run_phase(
            bench.clone(),
            &a,
            &endpoints,
            a.series,
            1_700_000_000,
            None,
            None,
        )
        .await;
        println!(
            "[bench] seed complete in {:.1}s (errors={})",
            r.duration.as_secs_f64(),
            r.errors
        );
    }

    if matches!(a.phase, Phase::Measure | Phase::All) {
        let r = run_phase(
            bench.clone(),
            &a,
            &endpoints,
            a.measure_points,
            1_800_000_000,
            Some(a.series),
            (a.max_secs > 0).then(|| Instant::now() + Duration::from_secs(a.max_secs)),
        )
        .await;
        let mine = r.points;
        let secs = r.duration.as_secs_f64();
        let pps = if secs > 0.0 {
            (mine as f64 / secs) as u64
        } else {
            0
        };
        println!(
            "[bench] worker={}/{} points={mine} duration={secs:.3}s errors={} retries={}",
            a.worker_index(),
            a.workers,
            r.errors,
            r.retries
        );
        println!(
            "[bench] nodes={} points={mine} duration={secs:.1}s errors={}",
            endpoints.len(),
            r.errors
        );
        println!("[bench] points_per_sec={pps}");
        let mut kinds: Vec<(String, u64)> = bench.error_kinds.lock().drain().collect();
        kinds.sort_by(|x, y| y.1.cmp(&x.1));
        for (k, n) in kinds.into_iter().take(3) {
            println!("[bench] error x{n}: {k}");
        }
        // Untimed: make the written set complete for a read-back check.
        let url = format!("{}/write?db={}&rp=autogen&precision=ns", endpoints[0], a.db);
        let unrecovered = replay_failed(
            &bench,
            &url,
            &a.measurement,
            &r.failed,
            a.batch,
            a.measure_points,
            1_800_000_000,
            Some(a.series),
            Instant::now() + REPLAY_DEADLINE,
        )
        .await;
        println!(
            "[bench] replay worker={} failed={} unrecovered={unrecovered}",
            a.worker_index(),
            r.failed.len()
        );
        println!("[bench] latency_ms_hist={}", r.latency.render());
        // Every point of every batch taken, less any still failing: what a
        // read-back of this database must find.
        println!(
            "[bench] acked worker={} points={}",
            a.worker_index(),
            mine.saturating_sub(unrecovered as u64 * a.batch)
        );
    }
    0
}

#[cfg(test)]
mod tests {
    use super::build_body;

    /// A batch that failed during the timed phase is rewritten until accepted,
    /// so the read-back can expect every point.
    #[tokio::test]
    async fn failed_batches_are_replayed_until_accepted() {
        use std::sync::Arc;
        use std::sync::atomic::{AtomicUsize, Ordering};
        let posts = Arc::new(AtomicUsize::new(0));
        let seen = posts.clone();
        // Refuse the first two writes, then accept.
        let app = axum::Router::new().route(
            "/write",
            axum::routing::post(move || {
                let n = seen.fetch_add(1, Ordering::SeqCst);
                async move {
                    if n < 2 {
                        axum::http::StatusCode::INTERNAL_SERVER_ERROR
                    } else {
                        axum::http::StatusCode::NO_CONTENT
                    }
                }
            }),
        );
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });

        let bench = super::Bench {
            http: reqwest::Client::new(),
            error_kinds: parking_lot::Mutex::new(Default::default()),
        };
        let unrecovered = super::replay_failed(
            &bench,
            &format!("http://{addr}/write"),
            "cpu",
            &[0, 5],
            5,
            10,
            1_800_000_000,
            Some(4),
            std::time::Instant::now() + std::time::Duration::from_secs(30),
        )
        .await;
        assert_eq!(unrecovered, 0);
        assert_eq!(posts.load(Ordering::SeqCst), 4, "2 refusals + 2 batches");
    }

    /// Byte-for-byte the lines the Python client sent.
    #[test]
    fn latency_lands_in_the_first_bucket_that_holds_it() {
        use std::time::Duration;
        let h = super::LatencyHist::default();
        h.record(Duration::from_micros(500));
        h.record(Duration::from_millis(5));
        h.record(Duration::from_millis(6));
        h.record(Duration::from_secs(60));
        let r = h.render();
        assert!(r.starts_with("1:1,2:0,3:0,5:1,7:1,"), "{r}");
        assert!(r.ends_with(",30000:0,inf:1"), "{r}");
    }

    #[test]
    fn lines_match_the_python_client() {
        let seed = String::from_utf8(build_body("cpu", 0, 0, 2, 1_700_000_000, None)).unwrap();
        assert_eq!(
            seed,
            "cpu,host=s0 value=1 1700000000000000000\ncpu,host=s1 value=1 1700000001000000001"
        );
        let measure =
            String::from_utf8(build_body("cpu", 0, 3, 5, 1_800_000_000, Some(4))).unwrap();
        assert_eq!(
            measure,
            "cpu,host=s3 value=1 1800000000000000003\ncpu,host=s0 value=1 1800000001000000000"
        );
    }
}
