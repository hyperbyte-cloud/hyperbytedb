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
        let url = format!("{}/query", self.base_url());
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
        if let Some(ref params) = opts.params {
            pairs.push(("params", params.clone()));
        }
        self.credentials.apply_query_auth(&mut pairs);

        let mut req = self.http.get(&url).query(&pairs);
        req = self.credentials.apply_basic_auth(req);
        req = self.apply_accept(req, opts.format);

        let resp = req
            .send()
            .await
            .map_err(|e| CliError::Connection(e.to_string()))?;
        self.parse_query_response(resp, opts.format).await
    }

    async fn query_post(&self, q: &str, opts: &QueryOptions) -> Result<QueryResponse> {
        let url = format!("{}/query", self.base_url());
        let mut body = vec![("q", q.to_string())];
        if let Some(ref db) = opts.db {
            body.push(("db", db.clone()));
        }
        if let Some(ref epoch) = opts.epoch {
            body.push(("epoch", epoch.clone()));
        }
        if opts.pretty {
            body.push(("pretty", "true".to_string()));
        }
        if opts.chunked {
            body.push(("chunked", "true".to_string()));
        }
        if let Some(ref params) = opts.params {
            body.push(("params", params.clone()));
        }

        let encoded =
            serde_urlencoded::to_string(&body).map_err(|e| CliError::Query(e.to_string()))?;

        let mut req = self
            .http
            .post(&url)
            .header("Content-Type", "application/x-www-form-urlencoded")
            .body(encoded);
        req = self.credentials.apply_basic_auth(req);
        req = self.apply_accept(req, opts.format);

        let resp = req
            .send()
            .await
            .map_err(|e| CliError::Connection(e.to_string()))?;
        self.parse_query_response(resp, opts.format).await
    }

    fn apply_accept(
        &self,
        req: reqwest::RequestBuilder,
        format: OutputFormat,
    ) -> reqwest::RequestBuilder {
        match format {
            OutputFormat::Csv => req.header("Accept", "text/csv"),
            _ => req,
        }
    }

    async fn parse_query_response(
        &self,
        resp: reqwest::Response,
        format: OutputFormat,
    ) -> Result<QueryResponse> {
        let status = resp.status();
        let body = resp
            .text()
            .await
            .map_err(|e| CliError::Connection(e.to_string()))?;

        if !status.is_success() {
            return Err(CliError::from_status(status, &body));
        }

        if format == OutputFormat::Csv {
            // Wrap CSV in a synthetic response for uniform handling
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
        let url = format!("{}/query", self.base_url());
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
        self.credentials.apply_query_auth(&mut pairs);

        let mut req = self.http.get(&url).query(&pairs);
        req = self.credentials.apply_basic_auth(req);
        req = self.apply_accept(req, opts.format);

        let resp = req
            .send()
            .await
            .map_err(|e| CliError::Connection(e.to_string()))?;
        let status = resp.status();
        let body = resp
            .text()
            .await
            .map_err(|e| CliError::Connection(e.to_string()))?;
        if !status.is_success() {
            return Err(CliError::from_status(status, &body));
        }
        Ok(body)
    }
}

impl QueryResponse {
    pub fn has_errors(&self) -> bool {
        self.results.iter().any(|r| r.error.is_some())
    }

    pub fn first_error(&self) -> Option<&str> {
        self.results.iter().find_map(|r| r.error.as_deref())
    }

    /// Format all per-statement errors into a single human-readable message.
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
