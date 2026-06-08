use serde::{Deserialize, Serialize};
use std::collections::HashMap;

/// Top-level query response matching InfluxDB v1 JSON format exactly.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct QueryResponse {
    pub results: Vec<StatementResult>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StatementResult {
    pub statement_id: u32,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub series: Option<Vec<SeriesResult>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub error: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SeriesResult {
    pub name: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub tags: Option<HashMap<String, String>>,
    pub columns: Vec<String>,
    pub values: Vec<Vec<serde_json::Value>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub partial: Option<bool>,
}

impl QueryResponse {
    pub fn error(statement_id: u32, msg: impl Into<String>) -> Self {
        Self {
            results: vec![StatementResult {
                statement_id,
                series: None,
                error: Some(msg.into()),
            }],
        }
    }

    pub fn empty(statement_id: u32) -> Self {
        Self {
            results: vec![StatementResult {
                statement_id,
                series: Some(vec![]),
                error: None,
            }],
        }
    }

    pub fn single(statement_id: u32, series: Vec<SeriesResult>) -> Self {
        Self {
            results: vec![StatementResult {
                statement_id,
                series: Some(series),
                error: None,
            }],
        }
    }
}
