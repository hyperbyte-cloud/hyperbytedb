use std::fs::File;
use std::io::{BufRead, BufReader, Read};
use std::time::Duration;

use crate::client::{HyperbytedbClient, QueryOptions, WriteOptions};
use crate::error::{CliError, Result};
use crate::session::OutputFormat;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Section {
    Ddl,
    Dml,
}

pub struct ImportOptions {
    pub path: String,
    pub compressed: bool,
    pub pps: u64,
    pub precision: Option<String>,
}

pub async fn run_import(client: &HyperbytedbClient, opts: &ImportOptions) -> Result<u64> {
    let reader: Box<dyn Read> = if opts.compressed {
        let f = File::open(&opts.path)
            .map_err(|e| CliError::Import(format!("open {}: {e}", opts.path)))?;
        Box::new(flate2::read::GzDecoder::new(f))
    } else {
        let f = File::open(&opts.path)
            .map_err(|e| CliError::Import(format!("open {}: {e}", opts.path)))?;
        Box::new(f)
    };

    let mut lines = BufReader::new(reader).lines();
    let mut section = Section::Ddl;
    let mut context_db: Option<String> = None;
    let mut context_rp: Option<String> = None;
    let mut batch = String::new();
    let mut point_count: u64 = 0;
    let throttle = if opts.pps == 0 {
        None
    } else {
        Some(Duration::from_nanos(1_000_000_000 / opts.pps))
    };

    while let Some(line) = lines
        .next()
        .transpose()
        .map_err(|e| CliError::Import(e.to_string()))?
    {
        let trimmed = line.trim();
        if trimmed.is_empty() {
            continue;
        }

        if trimmed == "# DDL" {
            section = Section::Ddl;
            continue;
        }
        if trimmed == "# DML" {
            section = Section::Dml;
            continue;
        }

        if let Some((kind, value)) = parse_section_headers(trimmed) {
            match kind {
                "database" => context_db = Some(value.to_string()),
                "rp" => context_rp = Some(value.to_string()),
                _ => {}
            }
            continue;
        }

        match section {
            Section::Ddl => {
                let qopts = QueryOptions {
                    db: None,
                    retention_policy: None,
                    epoch: None,
                    pretty: false,
                    chunked: false,
                    chunk_size: None,
                    format: OutputFormat::Json,
                    params: None,
                };
                let resp = client.query(trimmed, &qopts).await?;
                if resp.has_errors() {
                    return Err(CliError::Import(resp.format_errors()));
                }
            }
            Section::Dml => {
                let db = context_db.clone().ok_or_else(|| {
                    CliError::Import("DML line without CONTEXT-DATABASE".to_string())
                })?;
                batch.push_str(trimmed);
                batch.push('\n');
                point_count += 1;

                if point_count.is_multiple_of(100_000) {
                    flush_batch(client, &db, &context_rp, &opts.precision, &batch).await?;
                    eprintln!("imported {point_count} points...");
                    batch.clear();
                    if let Some(d) = throttle {
                        tokio::time::sleep(d).await;
                    }
                }
            }
        }
    }

    if !batch.is_empty() {
        let db = context_db
            .ok_or_else(|| CliError::Import("DML data without CONTEXT-DATABASE".to_string()))?;
        flush_batch(client, &db, &context_rp, &opts.precision, &batch).await?;
    }

    eprintln!("import complete: {point_count} points");
    Ok(point_count)
}

async fn flush_batch(
    client: &HyperbytedbClient,
    db: &str,
    rp: &Option<String>,
    precision: &Option<String>,
    batch: &str,
) -> Result<()> {
    let wopts = WriteOptions {
        db: db.to_string(),
        rp: rp.clone(),
        precision: precision.clone(),
        gzip: false,
        consistency: None,
    };
    client.write(batch.as_bytes(), &wopts).await
}

pub fn parse_section_headers(line: &str) -> Option<(&str, &str)> {
    if let Some(db) = line.strip_prefix("# CONTEXT-DATABASE:") {
        Some(("database", db.trim()))
    } else if let Some(rp) = line.strip_prefix("# CONTEXT-RETENTION-POLICY:") {
        Some(("rp", rp.trim()))
    } else {
        None
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parse_context_database() {
        let (kind, val) = parse_section_headers("# CONTEXT-DATABASE: mydb").unwrap();
        assert_eq!(kind, "database");
        assert_eq!(val, "mydb");
    }

    #[test]
    fn parse_context_rp() {
        let (kind, val) = parse_section_headers("# CONTEXT-RETENTION-POLICY: autogen").unwrap();
        assert_eq!(kind, "rp");
        assert_eq!(val, "autogen");
    }
}
