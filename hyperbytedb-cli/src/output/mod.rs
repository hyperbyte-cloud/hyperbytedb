use std::collections::HashMap;
use std::io::IsTerminal;

use comfy_table::presets::UTF8_NO_BORDERS;
use comfy_table::{Attribute, Cell, Color, ContentArrangement, Table};
use serde_json::Value;

use crate::client::QueryResponse;
use crate::session::OutputFormat;

struct DisplayStyle {
    color: bool,
}

impl DisplayStyle {
    fn detect() -> Self {
        Self {
            color: stdout_supports_color(),
        }
    }
}

fn stdout_supports_color() -> bool {
    std::io::stdout().is_terminal()
        && std::env::var("NO_COLOR").is_err()
        && std::env::var("HYPERBYTEDB_NO_COLOR").is_err()
}

pub fn format_response(response: &QueryResponse, format: OutputFormat, pretty: bool) -> String {
    match format {
        OutputFormat::Json => format_json(response, pretty),
        OutputFormat::Column => format_column(response),
        OutputFormat::Csv => {
            // CSV is streamed straight from the raw HTTP body (see `query_raw`) and
            // must never reach this formatter. Catch any future miswiring in debug.
            debug_assert!(false, "CSV must be rendered via the raw response path");
            String::new()
        }
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
    let style = DisplayStyle::detect();
    let mut out = String::new();

    for (result_idx, result) in response.results.iter().enumerate() {
        if result_idx > 0 {
            out.push('\n');
        }

        if let Some(ref err) = result.error {
            out.push_str(&format_error(err, &style));
            continue;
        }

        let Some(ref series_list) = result.series else {
            continue;
        };

        if series_list.is_empty() {
            out.push_str(&format_empty_result(&style));
            continue;
        }

        for (series_idx, series) in series_list.iter().enumerate() {
            if series_idx > 0 {
                out.push('\n');
            }
            out.push_str(&format_series_header(
                &series.name,
                series.tags.as_ref(),
                &style,
            ));
            out.push('\n');

            let mut table = Table::new();
            table
                .load_preset(UTF8_NO_BORDERS)
                .set_content_arrangement(ContentArrangement::Dynamic);
            table.set_header(
                series
                    .columns
                    .iter()
                    .map(|column| styled_column_header(column, &style)),
            );
            for row in &series.values {
                table.add_row(
                    series
                        .columns
                        .iter()
                        .zip(row.iter())
                        .map(|(column, value)| value_cell(value, column, &style)),
                );
            }
            out.push_str(&format!("{table}"));

            if series.partial == Some(true) {
                out.push('\n');
                out.push_str(&format_partial_notice(&style));
            }
        }
    }

    if !out.is_empty() && !out.ends_with('\n') {
        out.push('\n');
    }
    out
}

fn format_series_header(
    name: &str,
    tags: Option<&HashMap<String, String>>,
    style: &DisplayStyle,
) -> String {
    let mut out = String::new();
    if style.color {
        out.push_str("\x1b[1;34m");
    }
    if name.is_empty() {
        out.push('·');
    } else {
        out.push_str(name);
    }
    if style.color {
        out.push_str("\x1b[0m");
    }

    if let Some(tags) = tags
        && !tags.is_empty()
    {
        let mut pairs: Vec<_> = tags.iter().collect();
        pairs.sort_by(|(a, _), (b, _)| a.cmp(b));
        if style.color {
            out.push_str("\x1b[2;36m  ");
        } else {
            out.push_str("  ");
        }
        let rendered: Vec<String> = pairs
            .into_iter()
            .map(|(key, value)| {
                if style.color {
                    format!("\x1b[1;36m{key}\x1b[0m\x1b[2;36m={value}\x1b[0m")
                } else {
                    format!("{key}={value}")
                }
            })
            .collect();
        out.push_str(&rendered.join(", "));
        if style.color {
            out.push_str("\x1b[0m");
        }
    }
    out
}

fn styled_column_header(column: &str, style: &DisplayStyle) -> Cell {
    let mut cell = Cell::new(column);
    if style.color {
        cell = cell.fg(Color::Cyan).add_attribute(Attribute::Bold);
    }
    cell
}

fn value_cell(value: &Value, column: &str, style: &DisplayStyle) -> Cell {
    let text = value_text(value);
    let mut cell = Cell::new(&text);

    if !style.color {
        return cell;
    }

    match value {
        Value::Null => cell = cell.fg(Color::DarkGrey),
        Value::Bool(_) => cell = cell.fg(Color::Yellow),
        Value::Number(_) => cell = cell.fg(Color::Green),
        Value::String(_) if is_time_column(column) => cell = cell.fg(Color::Magenta),
        Value::String(_) => {}
        _ => cell = cell.fg(Color::Grey),
    }

    cell
}

fn is_time_column(column: &str) -> bool {
    column.eq_ignore_ascii_case("time") || column.ends_with("_time")
}

fn value_text(value: &Value) -> String {
    match value {
        Value::Null => String::new(),
        Value::String(s) => s.clone(),
        Value::Number(n) => n.to_string(),
        Value::Bool(b) => b.to_string(),
        other => other.to_string(),
    }
}

fn format_error(err: &str, style: &DisplayStyle) -> String {
    if style.color {
        format!("\x1b[1;31m✗ {err}\x1b[0m\n")
    } else {
        format!("ERR: {err}\n")
    }
}

fn format_empty_result(style: &DisplayStyle) -> String {
    if style.color {
        "\x1b[2m(no results)\x1b[0m\n".to_string()
    } else {
        "(no results)\n".to_string()
    }
}

fn format_partial_notice(style: &DisplayStyle) -> String {
    let message =
        "(partial results: response was truncated — increase chunk size or narrow the query)";
    if style.color {
        format!("\x1b[33m{message}\x1b[0m\n")
    } else {
        format!("{message}\n")
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
        assert!(!out.contains("││"));
    }

    #[test]
    fn column_format_includes_series_header() {
        let mut tags = HashMap::new();
        tags.insert("host".to_string(), "srv1".to_string());
        let resp = QueryResponse {
            results: vec![StatementResult {
                statement_id: 1,
                series: Some(vec![SeriesResult {
                    name: "cpu".to_string(),
                    tags: Some(tags),
                    columns: vec!["time".to_string(), "value".to_string()],
                    values: vec![vec![
                        Value::String("2024-01-01T00:00:00Z".to_string()),
                        Value::Number(42.into()),
                    ]],
                    partial: None,
                }]),
                error: None,
            }],
        };
        let out = format_column(&resp);
        assert!(out.contains("cpu"));
        assert!(out.contains("host=srv1"));
        assert!(out.contains("42"));
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
