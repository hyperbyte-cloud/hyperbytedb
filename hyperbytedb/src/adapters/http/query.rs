use axum::{
    body::Body,
    extract::{Query, State},
    http::{HeaderMap, StatusCode},
    response::{IntoResponse, Response},
};
use serde::Deserialize;

use crate::error::HyperbytedbError;
use metrics::{counter, histogram};

use super::router::AppState;

#[derive(Debug, Default, Deserialize)]
pub struct QueryParams {
    pub q: Option<String>,
    #[serde(default)]
    pub db: Option<String>,
    #[serde(default)]
    pub epoch: Option<String>,
    #[serde(default)]
    pub pretty: Option<bool>,
    #[serde(default)]
    pub chunked: Option<bool>,
    #[serde(default)]
    pub params: Option<String>,
    #[serde(default)]
    pub rp: Option<String>,
}

pub async fn handle_query_get(
    State(state): State<std::sync::Arc<AppState>>,
    auth_user: Option<axum::Extension<super::auth_middleware::AuthenticatedUser>>,
    headers: HeaderMap,
    Query(params): Query<QueryParams>,
) -> Result<Response, HyperbytedbError> {
    let caller = auth_user.as_ref().map(|axum::Extension(u)| &u.user);
    handle_query_impl(state, headers, params, caller).await
}

pub async fn handle_query_post(
    State(state): State<std::sync::Arc<AppState>>,
    auth_user: Option<axum::Extension<super::auth_middleware::AuthenticatedUser>>,
    headers: HeaderMap,
    Query(query_params): Query<QueryParams>,
    body: axum::body::Bytes,
) -> Result<Response, HyperbytedbError> {
    let caller = auth_user.as_ref().map(|axum::Extension(u)| &u.user);
    let params = merge_form_body(query_params, &body);
    handle_query_impl(state, headers, params, caller).await
}

fn merge_form_body(query: QueryParams, body: &[u8]) -> QueryParams {
    let body_str = match std::str::from_utf8(body) {
        Ok(s) if !s.trim().is_empty() => s,
        _ => return query,
    };

    let form: QueryParams = serde_urlencoded::from_str(body_str).unwrap_or_default();

    QueryParams {
        q: form
            .q
            .filter(|s| !s.is_empty())
            .or_else(|| query.q.filter(|s| !s.is_empty())),
        db: form.db.filter(|s| !s.is_empty()).or(query.db),
        epoch: form.epoch.or(query.epoch),
        pretty: form.pretty.or(query.pretty),
        chunked: form.chunked.or(query.chunked),
        params: form.params.or(query.params),
        rp: form.rp.filter(|s| !s.is_empty()).or(query.rp),
    }
}

fn escape_bind_string(s: &str) -> String {
    let mut out = String::with_capacity(s.len());
    for c in s.chars() {
        match c {
            '\\' => out.push_str("\\\\"),
            '\'' => out.push_str("\\'"),
            _ => out.push(c),
        }
    }
    out
}

fn bind_param_replacement(value: &serde_json::Value) -> String {
    match value {
        serde_json::Value::String(s) => format!("'{}'", escape_bind_string(s)),
        serde_json::Value::Number(n) => n.to_string(),
        serde_json::Value::Bool(b) => b.to_string(),
        serde_json::Value::Null => "NULL".to_string(),
        _ => value.to_string(),
    }
}

fn is_bind_ident_char(c: char) -> bool {
    c.is_ascii_alphanumeric() || c == '_'
}

/// Replace `$name` only on token boundaries so `$host` does not match inside
/// `$hostname`.
fn replace_bind_param(query: &str, key: &str, replacement: &str) -> String {
    let placeholder = format!("${key}");
    let mut out = String::with_capacity(query.len());
    let mut i = 0usize;
    while i < query.len() {
        if query[i..].starts_with(&placeholder) {
            let end = i + placeholder.len();
            let boundary_ok =
                end >= query.len() || !query[end..].chars().next().is_some_and(is_bind_ident_char);
            if boundary_ok {
                out.push_str(replacement);
                i = end;
                continue;
            }
        }
        let ch = match query[i..].chars().next() {
            Some(c) => c,
            None => break,
        };
        out.push(ch);
        i += ch.len_utf8();
    }
    out
}

fn substitute_bind_params(query: &str, params_json: &str) -> Result<String, HyperbytedbError> {
    let params: serde_json::Map<String, serde_json::Value> = serde_json::from_str(params_json)
        .map_err(|e| HyperbytedbError::QueryParse(format!("invalid params JSON: {e}")))?;

    // Longest keys first so `$hostname` is substituted before `$host`.
    let mut keys: Vec<&String> = params.keys().collect();
    keys.sort_by_key(|b| std::cmp::Reverse(b.len()));

    let mut result = query.to_string();
    for key in keys {
        let replacement = bind_param_replacement(&params[key]);
        result = replace_bind_param(&result, key, &replacement);
    }
    Ok(result)
}

fn extract_stmt_type(query: &str) -> &'static str {
    let first = query.split_whitespace().next().unwrap_or("");
    match first.to_ascii_uppercase().as_str() {
        "SELECT" => "SELECT",
        "SHOW" => "SHOW",
        "CREATE" => "CREATE",
        "DROP" => "DROP",
        "ALTER" => "ALTER",
        "DELETE" => "DELETE",
        "GRANT" => "GRANT",
        "REVOKE" => "REVOKE",
        "SET" => "SET",
        _ => "OTHER",
    }
}

fn wants_csv(headers: &HeaderMap) -> bool {
    headers
        .get("accept")
        .and_then(|v| v.to_str().ok())
        .is_some_and(|v| v.contains("text/csv") || v.contains("application/csv"))
}

fn result_to_csv(result: &crate::domain::query_result::QueryResponse) -> String {
    let mut out = String::new();
    for stmt_result in &result.results {
        if let Some(ref series) = stmt_result.series {
            for sr in series {
                if !sr.name.is_empty() {
                    out.push_str(&format!("name: {}\n", sr.name));
                }
                out.push_str(&sr.columns.join(","));
                out.push('\n');
                for row in &sr.values {
                    let cells: Vec<String> = row
                        .iter()
                        .map(|v| match v {
                            serde_json::Value::String(s) => s.clone(),
                            serde_json::Value::Null => String::new(),
                            other => other.to_string(),
                        })
                        .collect();
                    out.push_str(&cells.join(","));
                    out.push('\n');
                }
            }
        }
    }
    out
}

async fn handle_query_impl(
    state: std::sync::Arc<AppState>,
    headers: HeaderMap,
    params: QueryParams,
    caller: Option<&crate::domain::user::StoredUser>,
) -> Result<Response, HyperbytedbError> {
    let mut q = params
        .q
        .filter(|s| !s.is_empty())
        .ok_or_else(|| HyperbytedbError::MissingParameter("q".to_string()))?;

    let db = params.db.as_deref().unwrap_or("");
    let stmt_type_label = extract_stmt_type(&q);
    async {
        if let Some(ref params_json) = params.params {
            q = substitute_bind_params(&q, params_json)?;
        }

        let epoch = params.epoch.as_deref();
        let pretty = params.pretty.unwrap_or(false);
        let chunked = params.chunked.unwrap_or(false);

        let (digest_hex, normalized_query) = match crate::timeseriesql::parse(&q) {
            Ok(stmts) if !stmts.is_empty() => crate::timeseriesql::digest::fingerprint(&stmts[0]),
            _ => (String::new(), String::new()),
        };

        let metrics_start = std::time::Instant::now();
        let rp = params.rp.as_deref().filter(|s| !s.is_empty());
        let result = state
            .query
            .execute_query(db, &q, epoch, rp, caller)
            .await
            .map_err(|e| {
            tracing::error!(query = %q, db = db, error = %e, "query execution failed");
            counter!("hyperbytedb_query_errors_total", "db" => db.to_string(), "stmt_type" => stmt_type_label, "stmt_normalized" => normalized_query.to_string(), "stmt_digest" => digest_hex.to_string()).increment(1);
            e
        })?;
        let elapsed = metrics_start.elapsed();

        counter!("hyperbytedb_query_requests_total", "db" => db.to_string(), "stmt_type" => stmt_type_label, "stmt_normalized" => normalized_query.to_string(), "stmt_digest" => digest_hex.to_string()).increment(1);
        histogram!("hyperbytedb_query_duration_seconds", "db" => db.to_string(), "stmt_type" => stmt_type_label, "stmt_normalized" => normalized_query.to_string(), "stmt_digest" => digest_hex.to_string()).record(elapsed.as_secs_f64());

        if let Some(ref summary) = state.statement_summary {
            let latency_us = elapsed.as_micros() as u64;
            let sample_query =
                crate::timeseriesql::digest::redact_credentials(&q);
            summary.record(
                &digest_hex,
                &normalized_query,
                &sample_query,
                db,
                stmt_type_label,
                latency_us,
            );
        }

        let response = if wants_csv(&headers) {
            let csv = result_to_csv(&result);
            (StatusCode::OK, [("Content-Type", "text/csv")], csv).into_response()
        } else if chunked {
            let chunks: Vec<Result<String, std::io::Error>> = result
                .results
                .iter()
                .enumerate()
                .map(|(i, stmt_result)| {
                    let partial = crate::domain::query_result::QueryResponse {
                        results: vec![crate::domain::query_result::StatementResult {
                            statement_id: i as u32,
                            series: stmt_result.series.clone(),
                            error: stmt_result.error.clone(),
                        }],
                    };
                    let mut json = serde_json::to_string(&partial).unwrap_or_default();
                    json.push('\n');
                    Ok(json)
                })
                .collect();

            let stream = futures::stream::iter(chunks);
            let body = Body::from_stream(stream);
            Response::builder()
                .status(StatusCode::OK)
                .header("Content-Type", "application/json")
                .header("Transfer-Encoding", "chunked")
                .body(body)
                .unwrap_or_else(|_| {
                    let mut resp = Response::new(Body::empty());
                    *resp.status_mut() = StatusCode::INTERNAL_SERVER_ERROR;
                    resp
                })
        } else {
            let json = if pretty {
                serde_json::to_string_pretty(&result)
                    .map_err(|e| HyperbytedbError::Internal(e.to_string()))?
            } else {
                serde_json::to_string(&result).map_err(|e| HyperbytedbError::Internal(e.to_string()))?
            };
            (StatusCode::OK, [("Content-Type", "application/json")], json).into_response()
        };
        Ok(response)
    }
    .await
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn bind_param_escapes_backslash_and_quote() {
        let out = substitute_bind_params(
            r#"SELECT * FROM cpu WHERE host = $host"#,
            r#"{"host":"a\\b"}"#,
        )
        .unwrap();
        assert_eq!(out, r#"SELECT * FROM cpu WHERE host = 'a\\b'"#);
    }

    #[test]
    fn bind_param_does_not_replace_prefix_of_longer_name() {
        let out = substitute_bind_params(
            r#"SELECT * FROM cpu WHERE host = $hostname AND region = $host"#,
            r#"{"host":"east"}"#,
        )
        .unwrap();
        assert_eq!(
            out,
            r#"SELECT * FROM cpu WHERE host = $hostname AND region = 'east'"#
        );
    }

    #[test]
    fn bind_param_longest_key_wins() {
        let out = substitute_bind_params(
            r#"SELECT * FROM cpu WHERE host = $hostname"#,
            r#"{"host":"short","hostname":"long"}"#,
        )
        .unwrap();
        assert_eq!(out, r#"SELECT * FROM cpu WHERE host = 'long'"#);
    }
}
