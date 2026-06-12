use std::collections::{HashMap, HashSet};
use std::fs::File;
use std::io::Write;

use flate2::Compression;
use flate2::write::GzEncoder;
use serde_json::Value;

use crate::client::{HyperbytedbClient, QueryOptions, SeriesResult};
use crate::error::{CliError, Result};
use crate::session::OutputFormat;

pub struct ExportOptions {
    pub database: String,
    pub retention_policy: Option<String>,
    pub start: Option<String>,
    pub end: Option<String>,
    pub output: Option<String>,
    pub compress: bool,
}

enum ExportWriter {
    File(File),
    GzFile(GzEncoder<File>),
    Vec(Vec<u8>),
    GzVec(GzEncoder<Vec<u8>>),
}

impl Write for ExportWriter {
    fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
        match self {
            Self::File(f) => f.write(buf),
            Self::GzFile(g) => g.write(buf),
            Self::Vec(v) => v.write(buf),
            Self::GzVec(g) => g.write(buf),
        }
    }

    fn flush(&mut self) -> std::io::Result<()> {
        match self {
            Self::File(f) => f.flush(),
            Self::GzFile(g) => g.flush(),
            Self::Vec(v) => v.flush(),
            Self::GzVec(g) => g.flush(),
        }
    }
}

pub async fn run_export(client: &HyperbytedbClient, opts: &ExportOptions) -> Result<u64> {
    let mut writer = if let Some(ref path) = opts.output {
        if opts.compress {
            let f = File::create(path)
                .map_err(|e| CliError::Export(format!("create {}: {e}", path)))?;
            ExportWriter::GzFile(GzEncoder::new(f, Compression::default()))
        } else {
            ExportWriter::File(
                File::create(path)
                    .map_err(|e| CliError::Export(format!("create {}: {e}", path)))?,
            )
        }
    } else if opts.compress {
        ExportWriter::GzVec(GzEncoder::new(Vec::new(), Compression::default()))
    } else {
        ExportWriter::Vec(Vec::new())
    };

    writeln!(writer, "# DDL").map_err(|e| CliError::Export(e.to_string()))?;
    writeln!(writer, "CREATE DATABASE \"{}\"", opts.database)
        .map_err(|e| CliError::Export(e.to_string()))?;

    let qopts = QueryOptions {
        db: Some(opts.database.clone()),
        epoch: None,
        pretty: false,
        chunked: false,
        chunk_size: None,
        format: OutputFormat::Json,
        params: None,
    };

    let rp_query = format!("SHOW RETENTION POLICIES ON \"{}\"", opts.database);
    if let Ok(resp) = client.query(&rp_query, &qopts).await {
        emit_retention_policies(&mut writer, &resp, &opts.database)?;
    }

    writeln!(writer, "# DML").map_err(|e| CliError::Export(e.to_string()))?;
    writeln!(writer, "# CONTEXT-DATABASE: {}", opts.database)
        .map_err(|e| CliError::Export(e.to_string()))?;
    if let Some(ref rp) = opts.retention_policy {
        writeln!(writer, "# CONTEXT-RETENTION-POLICY: {rp}")
            .map_err(|e| CliError::Export(e.to_string()))?;
    }

    let measurements = list_measurements(client, &opts.database).await?;
    let tag_keys = list_tag_keys(client, &opts.database).await?;
    let time_filter = build_time_filter(&opts.start, &opts.end);
    let mut point_count = 0u64;

    for m in measurements {
        let q = if time_filter.is_empty() {
            format!(r#"SELECT * FROM "{m}""#)
        } else {
            format!(r#"SELECT * FROM "{m}" WHERE {time_filter}"#)
        };

        let resp = client.query(&q, &qopts).await?;
        if resp.has_errors() {
            continue;
        }
        let empty = HashSet::new();
        let measurement_tags = tag_keys.get(&m).unwrap_or(&empty);
        for result in &resp.results {
            if let Some(ref series_list) = result.series {
                for series in series_list {
                    let lines = series_to_line_protocol(series, measurement_tags);
                    for line in lines {
                        writeln!(writer, "{line}").map_err(|e| CliError::Export(e.to_string()))?;
                        point_count += 1;
                    }
                }
            }
        }
    }

    if opts.output.is_none() {
        match writer {
            ExportWriter::Vec(buf) => {
                let s = String::from_utf8_lossy(&buf);
                print!("{s}");
            }
            ExportWriter::GzVec(enc) => {
                let finished = enc.finish().map_err(|e| CliError::Export(e.to_string()))?;
                std::io::stdout()
                    .write_all(&finished)
                    .map_err(|e| CliError::Export(e.to_string()))?;
            }
            _ => {}
        }
    }

    eprintln!("export complete: {point_count} points");
    Ok(point_count)
}

async fn list_measurements(client: &HyperbytedbClient, db: &str) -> Result<Vec<String>> {
    let qopts = QueryOptions {
        db: Some(db.to_string()),
        epoch: None,
        pretty: false,
        chunked: false,
        chunk_size: None,
        format: OutputFormat::Json,
        params: None,
    };
    let resp = client.query("SHOW MEASUREMENTS", &qopts).await?;
    let mut out = Vec::new();
    for result in &resp.results {
        if let Some(ref series) = result.series {
            for s in series {
                for row in &s.values {
                    if let Some(Value::String(name)) = row.first() {
                        out.push(name.clone());
                    }
                }
            }
        }
    }
    Ok(out)
}

/// Build a `measurement -> {tag keys}` map from `SHOW TAG KEYS`, so export can
/// classify columns as tags vs. fields from the actual schema rather than guessing.
async fn list_tag_keys(
    client: &HyperbytedbClient,
    db: &str,
) -> Result<HashMap<String, HashSet<String>>> {
    let qopts = QueryOptions {
        db: Some(db.to_string()),
        epoch: None,
        pretty: false,
        chunked: false,
        chunk_size: None,
        format: OutputFormat::Json,
        params: None,
    };
    let resp = client
        .query(&format!(r#"SHOW TAG KEYS ON "{db}""#), &qopts)
        .await?;
    let mut out: HashMap<String, HashSet<String>> = HashMap::new();
    for result in &resp.results {
        if let Some(ref series) = result.series {
            for s in series {
                let keys = out.entry(s.name.clone()).or_default();
                for row in &s.values {
                    if let Some(Value::String(key)) = row.first() {
                        keys.insert(key.clone());
                    }
                }
            }
        }
    }
    Ok(out)
}

fn emit_retention_policies(
    writer: &mut ExportWriter,
    resp: &crate::client::QueryResponse,
    db: &str,
) -> Result<()> {
    for result in &resp.results {
        if let Some(ref series) = result.series {
            for s in series {
                for row in &s.values {
                    if row.len() >= 2 {
                        let rp = row[0].as_str().unwrap_or("autogen");
                        let duration = row[1].as_str().unwrap_or("INF");
                        writeln!(
                            writer,
                            r#"CREATE RETENTION POLICY "{rp}" ON "{db}" DURATION {duration} REPLICATION 1"#
                        )
                        .map_err(|e| CliError::Export(e.to_string()))?;
                    }
                }
            }
        }
    }
    Ok(())
}

fn build_time_filter(start: &Option<String>, end: &Option<String>) -> String {
    let mut parts = Vec::new();
    if let Some(s) = start {
        parts.push(format!("time >= '{s}'"));
    }
    if let Some(e) = end {
        parts.push(format!("time < '{e}'"));
    }
    parts.join(" AND ")
}

fn series_to_line_protocol(series: &SeriesResult, tag_keys: &HashSet<String>) -> Vec<String> {
    let mut lines = Vec::new();
    let time_idx = series.columns.iter().position(|c| c == "time");
    let is_tag = |c: &str| c != "time" && tag_keys.contains(c.strip_prefix("tag_").unwrap_or(c));

    let tag_cols: Vec<(usize, &String)> = series
        .columns
        .iter()
        .enumerate()
        .filter(|(_, c)| is_tag(c))
        .collect();

    let field_cols: Vec<(usize, &String)> = series
        .columns
        .iter()
        .enumerate()
        .filter(|(_, c)| *c != "time" && !is_tag(c))
        .collect();

    for row in &series.values {
        let mut tags = String::new();
        for (idx, col) in &tag_cols {
            if let Some(v) = row.get(*idx)
                && !v.is_null()
            {
                let key = col.strip_prefix("tag_").unwrap_or(col.as_str());
                tags.push_str(&format!(",{key}={}", json_to_lp_value(v)));
            }
        }

        let mut fields = Vec::new();
        for (idx, col) in &field_cols {
            if let Some(v) = row.get(*idx)
                && !v.is_null()
            {
                fields.push(format!("{}={}", col, json_to_lp_field(v)));
            }
        }
        if fields.is_empty() {
            continue;
        }

        let mut line = format!("{}{} {}", series.name, tags, fields.join(","));
        if let Some(ti) = time_idx
            && let Some(ts) = row.get(ti)
            && let Some(ns) = timestamp_to_ns(ts)
        {
            line.push(' ');
            line.push_str(&ns.to_string());
        }
        lines.push(line);
    }
    lines
}

fn json_to_lp_value(v: &Value) -> String {
    match v {
        Value::String(s) => s.replace(' ', r"\ "),
        other => other.to_string(),
    }
}

fn json_to_lp_field(v: &Value) -> String {
    match v {
        Value::String(s) => format!(r#""{}""#, s.replace('"', r#"\""#)),
        Value::Number(n) => n.to_string(),
        Value::Bool(b) => b.to_string(),
        _ => v.to_string(),
    }
}

fn timestamp_to_ns(v: &Value) -> Option<u64> {
    match v {
        Value::Number(n) => n.as_u64(),
        Value::String(s) => chrono::DateTime::parse_from_rfc3339(s)
            .ok()
            .map(|dt| dt.timestamp_nanos_opt().unwrap_or(0) as u64),
        _ => None,
    }
}
