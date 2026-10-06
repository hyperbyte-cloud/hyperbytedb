//! In-cluster query benchmark (`hyperbytedb-cli bench-read`).
//!
//! Runs against the dataset `bench-write` leaves behind: series `host=s0` ..
//! `host=s{series-1}` of `--measurement`, measured points from
//! 1_800_000_000 s. Each query kind runs for `--duration-secs` at
//! `--concurrency`, spread over the endpoints, and prints one line:
//!
//! `[bench-read] query=<kind> queries=<n> qps=<n> p50_ms=<f> p99_ms=<f> errors=<n>`

use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::{Duration, Instant};

use clap::{Args, ValueEnum};

/// Start of the measured points `bench-write` writes, in seconds.
const MEASURE_START_SECS: u64 = 1_800_000_000;

#[derive(Clone, Copy, PartialEq, Eq, ValueEnum, Debug)]
pub enum QueryKind {
    /// `count` over every measured point: a full scan.
    CountAll,
    /// `mean` per 10 s window over the measured range: a windowed scan.
    Mean10s,
    /// `last` of one random series: a point lookup.
    LastHost,
    /// Every measured value of one random series.
    RawHost,
    /// `mean` per host over 10 random hosts picked Grafana-style
    /// (`host =~ /^(sA|sB|...)$/`), grouped by host.
    RegexHosts,
    /// `last` per host over 10 random hosts, grouped by host.
    LastHosts,
    /// `mean` per 10 s window of one random series.
    WindowHost,
}

impl QueryKind {
    fn name(self) -> &'static str {
        match self {
            Self::CountAll => "count_all",
            Self::Mean10s => "mean_10s",
            Self::LastHost => "last_host",
            Self::RawHost => "raw_host",
            Self::RegexHosts => "regex_hosts",
            Self::LastHosts => "last_hosts",
            Self::WindowHost => "window_host",
        }
    }

    /// The InfluxQL for this kind; `host` picks the series for per-host kinds
    /// (and the first of ten, 7919 apart modulo `series`, for the multi-host
    /// kinds).
    pub fn sql(self, measurement: &str, host: u64, series: u64) -> String {
        let from = format!("time >= {MEASURE_START_SECS}s");
        let ten = || {
            (0..10u64)
                .map(|i| format!("s{}", (host + i * 7919) % series.max(1)))
                .collect::<Vec<_>>()
                .join("|")
        };
        match self {
            Self::CountAll => format!("SELECT count(value) FROM \"{measurement}\" WHERE {from}"),
            Self::Mean10s => format!(
                "SELECT mean(value) FROM \"{measurement}\" WHERE {from} AND time < {}s GROUP BY time(10s)",
                MEASURE_START_SECS + 100
            ),
            Self::LastHost => format!(
                "SELECT last(value) FROM \"{measurement}\" WHERE host = 's{host}' AND {from}"
            ),
            Self::RawHost => {
                format!("SELECT value FROM \"{measurement}\" WHERE host = 's{host}' AND {from}")
            }
            Self::RegexHosts => format!(
                "SELECT mean(value) FROM \"{measurement}\" WHERE host =~ /^({})$/ AND {from} GROUP BY host",
                ten()
            ),
            Self::LastHosts => format!(
                "SELECT last(value) FROM \"{measurement}\" WHERE host =~ /^({})$/ AND {from} GROUP BY host",
                ten()
            ),
            Self::WindowHost => format!(
                "SELECT mean(value) FROM \"{measurement}\" WHERE host = 's{host}' AND {from} AND time < {}s GROUP BY time(10s)",
                MEASURE_START_SECS + 100
            ),
        }
    }
}

#[derive(Args, Clone)]
pub struct BenchReadArgs {
    /// Comma-separated base URLs, one per node.
    #[arg(long)]
    pub endpoints: String,
    #[arg(long)]
    pub db: String,
    #[arg(long, default_value = "cpu")]
    pub measurement: String,
    /// Series written by bench-write (hosts s0 .. s{series-1}).
    #[arg(long)]
    pub series: u64,
    /// Query kinds to run, in order.
    #[arg(long, value_enum, value_delimiter = ',', default_values_t = [QueryKind::CountAll, QueryKind::Mean10s, QueryKind::LastHost, QueryKind::RawHost])]
    pub queries: Vec<QueryKind>,
    #[arg(long, default_value_t = 30)]
    pub duration_secs: u64,
    #[arg(long, default_value_t = 16)]
    pub concurrency: usize,
}

/// Outcome of one query: `Ok(latency)` or `Err(reason)`.
async fn run_query(
    http: &reqwest::Client,
    endpoint: &str,
    db: &str,
    q: &str,
) -> Result<Duration, String> {
    let started = Instant::now();
    let resp = http
        .get(format!("{endpoint}/query"))
        .query(&[("db", db), ("q", q), ("epoch", "ns")])
        .send()
        .await
        .map_err(|e| format!("transport: {e}"))?;
    let status = resp.status();
    let body = resp.bytes().await.map_err(|e| format!("body: {e}"))?;
    let elapsed = started.elapsed();
    if !status.is_success() {
        return Err(format!("HTTP {status}"));
    }
    // InfluxQL reports statement errors with HTTP 200 and an "error" key.
    if response_has_error(&body) {
        return Err(String::from_utf8_lossy(&body).chars().take(200).collect());
    }
    Ok(elapsed)
}

fn response_has_error(body: &[u8]) -> bool {
    serde_json::from_slice::<serde_json::Value>(body)
        .map(|v| {
            v.get("error").is_some()
                || v.get("results")
                    .and_then(|r| r.as_array())
                    .is_some_and(|rs| rs.iter().any(|r| r.get("error").is_some()))
        })
        .unwrap_or(true)
}

/// `p`-th percentile (0..=100) of sorted latencies, in milliseconds.
fn percentile_ms(sorted: &[Duration], p: f64) -> f64 {
    if sorted.is_empty() {
        return 0.0;
    }
    let idx = ((p / 100.0) * (sorted.len() - 1) as f64).round() as usize;
    sorted[idx.min(sorted.len() - 1)].as_secs_f64() * 1000.0
}

/// Run the benchmark; the return value is the process exit code.
pub async fn run(a: BenchReadArgs) -> u8 {
    let endpoints: Vec<String> = a
        .endpoints
        .split(',')
        .map(|e| e.trim().trim_end_matches('/').to_string())
        .filter(|e| !e.is_empty())
        .collect();
    if endpoints.is_empty() || a.series == 0 || a.concurrency == 0 {
        eprintln!("[bench-read] FATAL: need endpoints, series > 0 and concurrency > 0");
        return 2;
    }
    let http = match reqwest::Client::builder()
        .timeout(Duration::from_secs(120))
        .pool_max_idle_per_host(a.concurrency)
        .build()
    {
        Ok(c) => c,
        Err(e) => {
            eprintln!("[bench-read] FATAL: http client: {e}");
            return 2;
        }
    };
    let endpoints = Arc::new(endpoints);
    for kind in &a.queries {
        let kind = *kind;
        let stop = Arc::new(AtomicBool::new(false));
        let started = Instant::now();
        let mut tasks = Vec::with_capacity(a.concurrency);
        for idx in 0..a.concurrency {
            let (http, endpoints, stop) = (http.clone(), endpoints.clone(), stop.clone());
            let (db, measurement, series) = (a.db.clone(), a.measurement.clone(), a.series);
            let mut seed = 0x9E37_79B9_7F4A_7C15u64 ^ ((idx as u64 + 1) << 21);
            tasks.push(tokio::spawn(async move {
                let mut lat = Vec::new();
                let mut errors: Vec<String> = Vec::new();
                let mut n = 0usize;
                while !stop.load(Ordering::Relaxed) {
                    seed ^= seed << 13;
                    seed ^= seed >> 7;
                    seed ^= seed << 17;
                    let q = kind.sql(&measurement, seed % series, series);
                    let ep = &endpoints[(idx + n) % endpoints.len()];
                    n += 1;
                    match run_query(&http, ep, &db, &q).await {
                        Ok(d) => lat.push(d),
                        Err(e) => errors.push(e),
                    }
                }
                (lat, errors)
            }));
        }
        tokio::time::sleep(Duration::from_secs(a.duration_secs)).await;
        stop.store(true, Ordering::Relaxed);
        let mut lat = Vec::new();
        let mut errors = Vec::new();
        for t in tasks {
            if let Ok((l, e)) = t.await {
                lat.extend(l);
                errors.extend(e);
            }
        }
        let secs = started.elapsed().as_secs_f64();
        lat.sort_unstable();
        println!(
            "[bench-read] query={} queries={} qps={:.1} p50_ms={:.2} p99_ms={:.2} errors={}",
            kind.name(),
            lat.len(),
            lat.len() as f64 / secs,
            percentile_ms(&lat, 50.0),
            percentile_ms(&lat, 99.0),
            errors.len()
        );
        if let Some(e) = errors.first() {
            println!("[bench-read] first error for {}: {e}", kind.name());
        }
    }
    0
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn queries_target_the_bench_write_dataset() {
        assert_eq!(
            QueryKind::LastHost.sql("cpu", 42, 100),
            "SELECT last(value) FROM \"cpu\" WHERE host = 's42' AND time >= 1800000000s"
        );
        assert!(
            QueryKind::Mean10s
                .sql("cpu", 0, 100)
                .ends_with("GROUP BY time(10s)")
        );
        let multi = QueryKind::RegexHosts.sql("cpu", 1, 100);
        assert!(multi.contains("host =~ /^(s1|s20|s39|"), "{multi}");
        assert!(multi.ends_with("GROUP BY host"));
    }

    #[test]
    fn statement_errors_in_a_200_count_as_errors() {
        assert!(response_has_error(
            br#"{"results":[{"statement_id":0,"error":"boom"}]}"#
        ));
        assert!(response_has_error(br#"{"error":"bad"}"#));
        assert!(response_has_error(b"not json"));
        assert!(!response_has_error(
            br#"{"results":[{"statement_id":0,"series":[]}]}"#
        ));
    }

    #[test]
    fn percentiles_pick_from_sorted_latencies() {
        let ms = |v: u64| Duration::from_millis(v);
        let sorted = [ms(1), ms(2), ms(3), ms(4), ms(100)];
        assert_eq!(percentile_ms(&sorted, 50.0), 3.0);
        assert_eq!(percentile_ms(&sorted, 99.0), 100.0);
        assert_eq!(percentile_ms(&[], 50.0), 0.0);
    }
}
