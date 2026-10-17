//! `GET /debug/pprof/profile?seconds=N` (feature `profiling`): sample this
//! process's CPU for N seconds and return folded stacks, one
//! `frame;frame;... count` line per distinct stack, hottest first.
//!
//! Built for finding write-path hot spots on a live node; see the
//! `profiling` feature in Cargo.toml and the Dockerfile build args.

use std::fmt::Write as _;

use axum::extract::Query;
use axum::http::StatusCode;
use axum::response::IntoResponse;

#[derive(serde::Deserialize)]
pub struct ProfileParams {
    #[serde(default)]
    seconds: Option<u64>,
}

pub async fn profile(Query(params): Query<ProfileParams>) -> impl IntoResponse {
    let seconds = params.seconds.unwrap_or(15).clamp(1, 120);
    let guard = match pprof::ProfilerGuardBuilder::default()
        .frequency(99)
        .blocklist(&["libc", "libgcc", "pthread", "vdso"])
        .build()
    {
        Ok(g) => g,
        Err(e) => return (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()).into_response(),
    };
    tokio::time::sleep(std::time::Duration::from_secs(seconds)).await;
    let folded = tokio::task::spawn_blocking(move || {
        let report = guard.report().build().map_err(|e| e.to_string())?;
        let mut rows: Vec<(String, isize)> = report
            .data
            .iter()
            .map(|(frames, count)| {
                let stack = frames
                    .frames
                    .iter()
                    .rev()
                    .map(|syms| syms.iter().map(|s| s.name()).collect::<Vec<_>>().join("+"))
                    .collect::<Vec<_>>()
                    .join(";");
                (format!("{};{}", frames.thread_name, stack), *count)
            })
            .collect();
        rows.sort_by(|a, b| b.1.cmp(&a.1));
        let mut out = String::new();
        for (stack, count) in rows {
            let _ = writeln!(out, "{stack} {count}");
        }
        Ok::<String, String>(out)
    })
    .await;
    match folded {
        Ok(Ok(text)) => (StatusCode::OK, text).into_response(),
        Ok(Err(e)) => (StatusCode::INTERNAL_SERVER_ERROR, e).into_response(),
        Err(e) => (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()).into_response(),
    }
}
