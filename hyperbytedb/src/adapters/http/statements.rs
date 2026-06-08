use axum::{
    extract::{Query, State},
    http::StatusCode,
    response::IntoResponse,
};
use serde::{Deserialize, Serialize};
use std::sync::Arc;

use crate::application::statement_summary::{SortBy, SummaryEntry};

use super::router::AppState;

#[derive(Debug, Deserialize)]
pub struct ListParams {
    #[serde(default = "default_sort_by")]
    pub sort_by: String,
    #[serde(default)]
    pub order: Option<String>,
    #[serde(default = "default_limit")]
    pub limit: usize,
    #[serde(default)]
    pub db: Option<String>,
    #[serde(default)]
    pub stmt_type: Option<String>,
}

fn default_sort_by() -> String {
    "latency_total".to_string()
}

fn default_limit() -> usize {
    100
}

#[derive(Serialize)]
struct StatementsResponse {
    statements: Vec<SummaryEntry>,
}

pub async fn handle_list(
    State(state): State<Arc<AppState>>,
    Query(params): Query<ListParams>,
) -> impl IntoResponse {
    let summary = match &state.statement_summary {
        Some(s) => s,
        None => {
            let resp = StatementsResponse { statements: vec![] };
            return (
                StatusCode::OK,
                [("Content-Type", "application/json")],
                serde_json::to_string(&resp).unwrap_or_default(),
            );
        }
    };

    let sort_by = match params.sort_by.as_str() {
        "latency_avg" | "avg_latency" => SortBy::AvgLatency,
        "latency_max" | "max_latency" => SortBy::MaxLatency,
        "count" | "exec_count" => SortBy::Count,
        _ => SortBy::TotalLatency,
    };

    let ascending = params
        .order
        .as_deref()
        .is_some_and(|o| o.eq_ignore_ascii_case("asc"));

    let entries = summary.list(
        sort_by,
        ascending,
        params.limit,
        params.db.as_deref(),
        params.stmt_type.as_deref(),
    );

    let resp = StatementsResponse {
        statements: entries,
    };

    (
        StatusCode::OK,
        [("Content-Type", "application/json")],
        serde_json::to_string(&resp).unwrap_or_default(),
    )
}

pub async fn handle_reset(State(state): State<Arc<AppState>>) -> impl IntoResponse {
    if let Some(ref summary) = state.statement_summary {
        summary.reset();
    }
    (StatusCode::OK, "reset")
}
