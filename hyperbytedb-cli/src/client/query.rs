use serde::{Deserialize, Serialize};
use serde_json::Value;

use crate::error::{CliError, Result, parse_json_error};
use crate::session::OutputFormat;

use super::HyperbytedbClient;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct QueryResponse {
    pub results: Vec<StatementResult>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StatementResult {
    pub statement_id: u32,
    pub series: Option<Vec<SeriesResult>>,
    pub error: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SeriesResult {
    pub name: String,
    pub tags: Option<std::collections::HashMap<String, String>>,
    pub columns: Vec<String>,
    pub values: Vec<Vec<Value>>,
    pub partial: Option<bool>,
}

#[derive(Debug, Clone)]
pub struct QueryOptions {
    pub db: Option<String>,
    pub epoch: Option<String>,
    pub pretty: bool,
    pub chunked: bool,
    pub chunk_size: Option<usize>,
    pub format: OutputFormat,
    pub params: Option<String>,
}

impl HyperbytedbClient {
    pub async fn query(&self, q: &str, opts: &QueryOptions) -> Result<QueryResponse> {
        let use_post = q.len() > 2048;
        if use_post {
            self.query_post(q, opts).await
        } else {
            self.query_get(q, opts).await
        }
    }

    async fn query_get(&self, q: &str, opts: &QueryOptions) -> Result<QueryResponse> {
        let query = self.build_query_string(q, opts)?;
        let mut headers = self.accept_header(opts.format);
        for (k, v) in self.auth_headers() {
            headers.push((k, v));
        }
        let header_refs: Vec<(&str, &str)> = headers
            .iter()
            .map(|(k, v)| (k.as_str(), v.as_str()))
            .collect();
        let resp = self
            .request("GET", "/query", &query, &header_refs, None)
            .await?;
        self.parse_query_response(resp, opts.format).await
    }

    async fn query_post(&self, q: &str, opts: &QueryOptions) -> Result<QueryResponse> {
        let body_pairs = self.build_body_pairs(q, opts)?;
        let encoded =
            serde_urlencoded::to_string(&body_pairs).map_err(|e| CliError::Query(e.to_string()))?;
        let mut headers = vec![(
            "Content-Type".to_string(),
            "application/x-www-form-urlencoded".to_string(),
        )];
        headers.extend(self.accept_header(opts.format));
        for (k, v) in self.auth_headers() {
            headers.push((k, v));
        }
        let header_refs: Vec<(&str, &str)> = headers
            .iter()
            .map(|(k, v)| (k.as_str(), v.as_str()))
            .collect();

        let resp = self
            .request(
                "POST",
                "/query",
                "",
                &header_refs,
                Some(encoded.into_bytes()),
            )
            .await?;
        self.parse_query_response(resp, opts.format).await
    }

    fn build_query_string(&self, q: &str, opts: &QueryOptions) -> Result<String> {
        let pairs = self.build_body_pairs(q, opts)?;
        serde_urlencoded::to_string(&pairs).map_err(|e| CliError::Query(e.to_string()))
    }

    fn build_body_pairs(&self, q: &str, opts: &QueryOptions) -> Result<Vec<(&str, String)>> {
        let mut pairs: Vec<(&str, String)> = vec![("q", q.to_string())];
        if let Some(ref db) = opts.db {
            pairs.push(("db", db.clone()));
        }
        if let Some(ref epoch) = opts.epoch {
            pairs.push(("epoch", epoch.clone()));
        }
        if opts.pretty {
            pairs.push(("pretty", "true".to_string()));
        }
        if opts.chunked {
            pairs.push(("chunked", "true".to_string()));
        }
        if let Some(size) = opts.chunk_size {
            pairs.push(("chunk_size", size.to_string()));
        }
        if let Some(ref params) = opts.params {
            pairs.push(("params", params.clone()));
        }
        self.credentials.apply_query_auth(&mut pairs);
        Ok(pairs)
    }

    fn accept_header(&self, format: OutputFormat) -> Vec<(String, String)> {
        match format {
            OutputFormat::Csv => vec![("Accept".to_string(), "text/csv".to_string())],
            _ => vec![],
        }
    }

    async fn parse_query_response(
        &self,
        resp: super::RawResponse,
        format: OutputFormat,
    ) -> Result<QueryResponse> {
        if !(200..300).contains(&resp.status) {
            let body = String::from_utf8_lossy(&resp.body);
            return Err(CliError::from_status(
                reqwest::StatusCode::from_u16(resp.status)
                    .unwrap_or(reqwest::StatusCode::BAD_REQUEST),
                &body,
            ));
        }

        let body = String::from_utf8_lossy(&resp.body);
        if format == OutputFormat::Csv {
            return Ok(QueryResponse {
                results: vec![StatementResult {
                    statement_id: 1,
                    series: None,
                    error: None,
                }],
            });
        }

        serde_json::from_str(&body).map_err(|e| {
            let detail = parse_json_error(&body);
            CliError::Query(format!("invalid JSON ({e}): {detail}"))
        })
    }

    pub async fn query_raw(&self, q: &str, opts: &QueryOptions) -> Result<String> {
        let query = self.build_query_string(q, opts)?;
        let mut headers = self.accept_header(opts.format);
        for (k, v) in self.auth_headers() {
            headers.push((k, v));
        }
        let header_refs: Vec<(&str, &str)> = headers
            .iter()
            .map(|(k, v)| (k.as_str(), v.as_str()))
            .collect();

        let resp = self
            .request("GET", "/query", &query, &header_refs, None)
            .await?;
        if !(200..300).contains(&resp.status) {
            let body = String::from_utf8_lossy(&resp.body);
            return Err(CliError::from_status(
                reqwest::StatusCode::from_u16(resp.status)
                    .unwrap_or(reqwest::StatusCode::BAD_REQUEST),
                &body,
            ));
        }
        Ok(String::from_utf8_lossy(&resp.body).into_owned())
    }
}

impl QueryResponse {
    pub fn has_errors(&self) -> bool {
        self.results.iter().any(|r| r.error.is_some())
    }

    pub fn first_error(&self) -> Option<&str> {
        self.results.iter().find_map(|r| r.error.as_deref())
    }

    pub fn format_errors(&self) -> String {
        let errors: Vec<String> = self
            .results
            .iter()
            .filter_map(|r| {
                let err = r.error.as_deref()?;
                if self.results.len() > 1 {
                    Some(format!("statement {}: {err}", r.statement_id))
                } else {
                    Some(err.to_string())
                }
            })
            .collect();
        if errors.is_empty() {
            "query failed".to_string()
        } else {
            errors.join("; ")
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn format_errors_single_statement() {
        let resp = QueryResponse {
            results: vec![StatementResult {
                statement_id: 1,
                series: None,
                error: Some("syntax error".to_string()),
            }],
        };
        assert_eq!(resp.format_errors(), "syntax error");
    }

    #[test]
    fn format_errors_multiple_statements() {
        let resp = QueryResponse {
            results: vec![
                StatementResult {
                    statement_id: 0,
                    series: None,
                    error: Some("first".to_string()),
                },
                StatementResult {
                    statement_id: 1,
                    series: None,
                    error: Some("second".to_string()),
                },
            ],
        };
        assert_eq!(
            resp.format_errors(),
            "statement 0: first; statement 1: second"
        );
    }
}
