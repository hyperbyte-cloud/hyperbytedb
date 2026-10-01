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

/// The time column's value: an integer in the requested epoch unit, or
/// RFC3339 with trailing fractional zeros removed.
pub fn time_value_from_nanos(nanos: i64, epoch: Option<&str>) -> serde_json::Value {
    use serde_json::Value;
    match epoch {
        Some("ns") | Some("n") => Value::from(nanos),
        Some("u") | Some("us") => Value::from(nanos / 1_000),
        Some("ms") => Value::from(nanos / 1_000_000),
        Some("s") => Value::from(nanos / 1_000_000_000),
        _ => {
            let dt = chrono::DateTime::from_timestamp_nanos(nanos);
            let mut out = dt.format("%Y-%m-%dT%H:%M:%S").to_string();
            let frac = format!("{:09}", dt.timestamp_subsec_nanos());
            let trimmed = frac.trim_end_matches('0');
            if !trimmed.is_empty() {
                out.push('.');
                out.push_str(trimmed);
            }
            out.push('Z');
            Value::String(out)
        }
    }
}
