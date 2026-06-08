use comfy_table::{Cell, Table};
use serde_json::Value;

use crate::client::QueryResponse;
use crate::session::OutputFormat;

pub fn format_response(response: &QueryResponse, format: OutputFormat, pretty: bool) -> String {
    match format {
        OutputFormat::Json => format_json(response, pretty),
        OutputFormat::Column => format_column(response),
        OutputFormat::Csv => String::new(), // CSV handled via raw response path
    }
}

pub fn format_json(response: &QueryResponse, pretty: bool) -> String {
    if pretty {
        serde_json::to_string_pretty(response).unwrap_or_default()
    } else {
        serde_json::to_string(response).unwrap_or_default()
    }
}

pub fn format_column(response: &QueryResponse) -> String {
    let mut out = String::new();
    for result in &response.results {
        if let Some(ref err) = result.error {
            out.push_str(&format!("ERR: {err}\n"));
            continue;
        }
        let Some(ref series_list) = result.series else {
            continue;
        };
        if series_list.is_empty() {
            out.push('\n');
            continue;
        }
        for series in series_list {
            if !series.name.is_empty() {
                out.push_str(&format!("name: {}\n", series.name));
            }
            if let Some(ref tags) = series.tags
                && !tags.is_empty()
            {
                let tag_str: Vec<String> = tags.iter().map(|(k, v)| format!("{k}={v}")).collect();
                out.push_str(&format!("tags: {}\n", tag_str.join(", ")));
            }
            let mut table = Table::new();
            table.set_header(series.columns.iter().map(|c| c.as_str()));
            for row in &series.values {
                table.add_row(row.iter().map(value_cell));
            }
            out.push_str(&format!("{table}\n"));
        }
    }
    out
}

fn value_cell(v: &Value) -> Cell {
    match v {
        Value::Null => Cell::new(""),
        Value::String(s) => Cell::new(s),
        Value::Number(n) => Cell::new(n.to_string()),
        Value::Bool(b) => Cell::new(b.to_string()),
        other => Cell::new(other.to_string()),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::client::{SeriesResult, StatementResult};

    #[test]
    fn column_format_single_series() {
        let resp = QueryResponse {
            results: vec![StatementResult {
                statement_id: 1,
                series: Some(vec![SeriesResult {
                    name: "databases".to_string(),
                    tags: None,
                    columns: vec!["name".to_string()],
                    values: vec![vec![Value::String("mydb".to_string())]],
                    partial: None,
                }]),
                error: None,
            }],
        };
        let out = format_column(&resp);
        assert!(out.contains("mydb"));
        assert!(out.contains("name"));
    }

    #[test]
    fn json_pretty() {
        let resp = QueryResponse {
            results: vec![StatementResult {
                statement_id: 1,
                series: Some(vec![]),
                error: None,
            }],
        };
        let out = format_json(&resp, true);
        assert!(out.contains("results"));
    }
}
