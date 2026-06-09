mod meta;

use std::time::Instant;

use rustyline::DefaultEditor;
use rustyline::error::ReadlineError;

use crate::client::{HyperbytedbClient, QueryOptions};
use crate::config::history_file_path;
use crate::error::{CliError, Result};
use crate::output::format_response;
use crate::session::Session;

use meta::{MetaAction, handle_meta, is_meta_command};

pub async fn run_repl(mut session: Session) -> Result<()> {
    let mut client = HyperbytedbClient::new(&session.connection)?;

    match client.ping().await {
        Ok(ping) => {
            session.server_version = ping.version.clone();
            println!(
                "Connected to {} ({})",
                client.base_url(),
                ping.version.unwrap_or_else(|| "unknown".to_string())
            );
        }
        Err(e) => {
            eprintln!("warning: ping failed: {e}");
            println!("Connected to {} (ping failed)", client.base_url());
        }
    }

    let history_path = history_file_path();
    let mut rl = DefaultEditor::new().map_err(|e| CliError::Other(e.to_string()))?;
    let _ = rl.load_history(&history_path);

    loop {
        let prompt = if let Some(db) = session.effective_database() {
            format!("{db}> ")
        } else {
            "hyperbytedb> ".to_string()
        };

        let read_result = rl.readline(&prompt);
        match read_result {
            Ok(line) => {
                let trimmed = line.trim();
                if trimmed.is_empty() {
                    continue;
                }
                let _ = rl.add_history_entry(trimmed);

                if is_meta_command(trimmed) {
                    match handle_meta(&mut session, &mut client, trimmed).await? {
                        MetaAction::Exit => break,
                        MetaAction::Continue | MetaAction::Executed => continue,
                    }
                }

                if let Err(e) = execute_query(&session, &client, trimmed).await {
                    eprintln!("{e}");
                }
            }
            Err(ReadlineError::Interrupted) => {
                println!("^C");
                continue;
            }
            Err(ReadlineError::Eof) => break,
            Err(e) => return Err(CliError::Other(e.to_string())),
        }
    }

    let _ = rl.save_history(&history_path);
    Ok(())
}

pub async fn execute_query(session: &Session, client: &HyperbytedbClient, q: &str) -> Result<()> {
    let statements: Vec<&str> = q
        .split(';')
        .map(str::trim)
        .filter(|s| !s.is_empty())
        .collect();
    let start = Instant::now();

    for stmt in statements {
        let opts = QueryOptions {
            db: session.effective_database().map(|s| s.to_string()),
            epoch: session.epoch.clone(),
            pretty: session.pretty,
            chunked: session.chunked,
            format: session.format,
            params: None,
        };

        if session.format == crate::session::OutputFormat::Csv {
            let raw = client.query_raw(stmt, &opts).await?;
            print!("{raw}");
            continue;
        }

        let resp = client.query(stmt, &opts).await?;
        if resp.has_errors() {
            return Err(CliError::Query(resp.format_errors()));
        }
        let out = format_response(&resp, session.format, session.pretty);
        print!("{out}");
    }

    if session.timing {
        eprintln!("Time: {:.3}ms", start.elapsed().as_secs_f64() * 1000.0);
    }
    Ok(())
}
