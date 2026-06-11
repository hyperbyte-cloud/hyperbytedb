use crate::client::{HyperbytedbClient, WriteOptions};
use crate::config::resolve_host;
use crate::error::{CliError, Result};
use crate::session::{OutputFormat, Session};

pub enum MetaAction {
    Continue,
    Exit,
    Executed,
}

pub async fn handle_meta(
    session: &mut Session,
    client: &mut HyperbytedbClient,
    line: &str,
) -> Result<MetaAction> {
    let lower = line.to_ascii_lowercase();
    let parts: Vec<&str> = line.split_whitespace().collect();

    if lower == "help" || lower == "?" {
        print_help();
        return Ok(MetaAction::Continue);
    }
    if lower == "exit" || lower == "quit" {
        return Ok(MetaAction::Exit);
    }
    if lower == "settings" {
        print_settings(session);
        return Ok(MetaAction::Continue);
    }
    if lower == "auth" {
        prompt_auth(session)?;
        *client = HyperbytedbClient::new(&session.connection, session.verbose)?;
        return Ok(MetaAction::Continue);
    }
    if lower == "pretty" {
        session.pretty = !session.pretty;
        println!("pretty: {}", session.pretty);
        return Ok(MetaAction::Continue);
    }
    if lower == "timing" {
        session.timing = !session.timing;
        println!("timing: {}", session.timing);
        return Ok(MetaAction::Continue);
    }
    if lower == "chunked" {
        session.chunked = !session.chunked;
        println!("chunked: {}", session.chunked);
        return Ok(MetaAction::Continue);
    }
    if parts.len() >= 3
        && parts[0].eq_ignore_ascii_case("chunk")
        && parts[1].eq_ignore_ascii_case("size")
    {
        let n: usize = parts[2]
            .parse()
            .map_err(|_| CliError::Other("invalid chunk size".to_string()))?;
        session.chunk_size = n;
        println!("chunk size: {n}");
        return Ok(MetaAction::Continue);
    }
    if parts.len() >= 2 && parts[0].eq_ignore_ascii_case("connect") {
        let host = resolve_host(Some(parts[1]), None, session.connection.ssl);
        session.connection.host = host.clone();
        client.set_base_url(host);
        let ping = client.ping().await?;
        session.server_version = ping.version.clone();
        println!(
            "Connected to {} ({})",
            client.base_url(),
            ping.version.unwrap_or_else(|| "unknown".to_string())
        );
        return Ok(MetaAction::Continue);
    }
    if parts.len() >= 2 && parts[0].eq_ignore_ascii_case("use") {
        let target = parts[1];
        if let Some((db, rp)) = target.split_once('.') {
            session.set_use(db, Some(rp));
        } else {
            session.set_use(target, None);
        }
        println!(
            "Using database {}",
            session.database.as_deref().unwrap_or("")
        );
        return Ok(MetaAction::Continue);
    }
    if parts.len() >= 2 && parts[0].eq_ignore_ascii_case("clear") {
        match parts[1].to_ascii_lowercase().as_str() {
            "database" | "db" => {
                session.clear_database();
                println!("database cleared");
            }
            "retention" | "rp" | "retention policy" => {
                session.clear_retention_policy();
                println!("retention policy cleared");
            }
            _ => println!("usage: clear database|db|rp"),
        }
        return Ok(MetaAction::Continue);
    }
    if parts.len() >= 2 && parts[0].eq_ignore_ascii_case("format") {
        if let Some(fmt) = OutputFormat::parse(parts[1]) {
            session.format = fmt;
            println!("format: {}", fmt.as_str());
        } else {
            println!("format must be json, csv, or column");
        }
        return Ok(MetaAction::Continue);
    }
    if parts.len() >= 2 && parts[0].eq_ignore_ascii_case("precision") {
        session.epoch = Some(parts[1].to_string());
        println!("precision: {}", parts[1]);
        return Ok(MetaAction::Continue);
    }
    if parts.len() >= 2 && parts[0].eq_ignore_ascii_case("consistency") {
        session.consistency = Some(parts[1].to_string());
        println!("consistency: {}", parts[1]);
        return Ok(MetaAction::Continue);
    }
    if parts.len() >= 2 && parts[0].eq_ignore_ascii_case("insert") {
        let lp = if parts[1].eq_ignore_ascii_case("into") && parts.len() >= 4 {
            let rp = parts[2].to_string();
            let body = parts[3..].join(" ");
            (Some(rp), body)
        } else {
            (session.retention_policy.clone(), parts[1..].join(" "))
        };
        let db = session
            .effective_database()
            .ok_or_else(|| CliError::Other("no database selected; use USE <db>".to_string()))?
            .to_string();
        let wopts = WriteOptions {
            db,
            rp: lp.0,
            precision: session.epoch.clone(),
            gzip: false,
            consistency: session.consistency.clone(),
        };
        client.write(lp.1.as_bytes(), &wopts).await?;
        println!("ok");
        return Ok(MetaAction::Executed);
    }
    if lower == "history" {
        println!("history is managed by the line editor (up-arrow)");
        return Ok(MetaAction::Continue);
    }

    Ok(MetaAction::Continue)
}

pub fn is_meta_command(line: &str) -> bool {
    let lower = line.trim().to_ascii_lowercase();
    lower.starts_with("help")
        || lower.starts_with("connect ")
        || lower.starts_with("use ")
        || lower.starts_with("clear ")
        || lower.starts_with("auth")
        || lower.starts_with("format ")
        || lower.starts_with("precision ")
        || lower.starts_with("consistency ")
        || lower.starts_with("insert ")
        || lower == "settings"
        || lower == "pretty"
        || lower == "timing"
        || lower == "chunked"
        || lower.starts_with("chunk size ")
        || lower == "history"
        || lower == "exit"
        || lower == "quit"
        || lower == "?"
}

fn print_help() {
    println!(
        r#"Meta-commands (not sent to the server):
  help                     Show this help
  connect <host[:port]>    Connect to a server
  use <db>[.<rp>]          Set database / retention policy
  clear database|db|rp     Clear session context
  auth                     Prompt for username/password
  insert <line_protocol>   Write a point via line protocol
  insert into <rp> ...     Write to a specific retention policy
  format json|csv|column   Set output format
  precision <unit>         Set timestamp precision (epoch param)
  consistency <level>      Set write consistency (any, one, quorum, all)
  pretty                   Toggle JSON pretty-print
  chunked                  Toggle chunked query responses
  chunk size <n>           Set chunk size
  settings                 Show session settings
  timing                   Toggle query duration display
  history                  Show history hint
  exit, quit               Exit the shell

DDL examples (sent to /query):
  CREATE MATERIALIZED VIEW "mv_5m" ON "db"
    AS SELECT mean("value") INTO "cpu_5m" FROM "cpu" GROUP BY time(5m), *
  SHOW MATERIALIZED VIEWS

Any other input is sent as TimeseriesQL to /query."#
    );
}

fn print_settings(session: &Session) {
    println!("host:       {}", session.connection.base_url());
    println!(
        "user:       {}",
        session.connection.username.as_deref().unwrap_or("(none)")
    );
    println!(
        "database:   {}",
        session.database.as_deref().unwrap_or("(none)")
    );
    println!(
        "rp:         {}",
        session.retention_policy.as_deref().unwrap_or("(none)")
    );
    println!("format:     {}", session.format.as_str());
    println!(
        "precision:  {}",
        session.epoch.as_deref().unwrap_or("rfc3339")
    );
    println!("pretty:     {}", session.pretty);
    println!("chunked:    {}", session.chunked);
    println!("chunk size: {}", session.chunk_size);
    println!("timing:     {}", session.timing);
    println!(
        "consistency:{}",
        session.consistency.as_deref().unwrap_or("(none)")
    );
    println!(
        "server:     {}",
        session.server_version.as_deref().unwrap_or("(unknown)")
    );
}

fn prompt_auth(session: &mut Session) -> Result<()> {
    use std::io::{self, Write};
    print!("username: ");
    io::stdout()
        .flush()
        .map_err(|e| CliError::Other(e.to_string()))?;
    let mut username = String::new();
    io::stdin()
        .read_line(&mut username)
        .map_err(|e| CliError::Other(e.to_string()))?;
    let username = username.trim().to_string();
    print!("password: ");
    io::stdout()
        .flush()
        .map_err(|e| CliError::Other(e.to_string()))?;
    let password = rpassword::read_password().map_err(|e| CliError::Other(e.to_string()))?;
    session.connection.username = Some(username);
    session.connection.password = Some(password);
    Ok(())
}
