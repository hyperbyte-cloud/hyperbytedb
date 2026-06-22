use std::sync::Arc;

use parking_lot::RwLock;
use rustyline::completion::{Completer, Pair};
use rustyline::highlight::Highlighter;
use rustyline::hint::Hinter;
use rustyline::validate::Validator;
use rustyline::{Context, Helper, Result as RustyResult};

use crate::client::{HyperbytedbClient, QueryOptions, QueryResponse};
use crate::session::OutputFormat;

const META_COMMANDS: &[&str] = &[
    "auth",
    "chunk",
    "clear",
    "connect",
    "consistency",
    "exit",
    "format",
    "help",
    "history",
    "insert",
    "precision",
    "pretty",
    "quit",
    "settings",
    "timing",
    "use",
];

const SQL_VERBS: &[&str] = &[
    "ALTER", "CREATE", "DELETE", "DROP", "EXPLAIN", "GRANT", "REVOKE", "SELECT", "SHOW",
];

const FORMAT_VALUES: &[&str] = &["column", "csv", "json"];
const CLEAR_TARGETS: &[&str] = &["database", "db", "retention", "rp"];
const CONSISTENCY_VALUES: &[&str] = &["all", "any", "one", "quorum"];
const PRECISION_VALUES: &[&str] = &["h", "m", "ms", "ns", "s", "u"];
const SHOW_OBJECTS: &[&str] = &[
    "CONTINUOUS",
    "DATABASES",
    "FIELD",
    "MATERIALIZED",
    "MEASUREMENTS",
    "RETENTION",
    "SERIES",
    "TAG",
    "USERS",
];
const TAG_SUBCOMMANDS: &[&str] = &["KEYS", "VALUES"];
const SHOW_MODIFIERS: &[&str] = &["FROM", "ON", "WITH"];
const CREATE_OBJECTS: &[&str] = &[
    "CONTINUOUS",
    "DATABASE",
    "MATERIALIZED",
    "RETENTION",
    "USER",
];
const CREATE_TAIL: &[&str] = &["POLICY", "QUERY", "VIEW"];
const DROP_OBJECTS: &[&str] = &[
    "CONTINUOUS",
    "DATABASE",
    "MATERIALIZED",
    "MEASUREMENT",
    "RETENTION",
    "USER",
];
const SELECT_KEYWORDS: &[&str] = &["FROM", "GROUP", "INTO", "LIMIT", "ORDER", "SELECT", "WHERE"];
const SQL_KEYWORDS: &[&str] = &[
    "AS", "BY", "FROM", "GROUP", "INTO", "LIMIT", "ON", "ORDER", "WHERE", "WITH",
];

#[derive(Default)]
pub struct CompletionCache {
    databases: Vec<String>,
    measurements: Vec<String>,
}

impl CompletionCache {
    pub fn clear_measurements(&mut self) {
        self.measurements.clear();
    }
}

pub async fn refresh_databases_cache(
    cache: &Arc<RwLock<CompletionCache>>,
    client: &HyperbytedbClient,
) -> crate::error::Result<()> {
    let databases = fetch_databases(client).await?;
    cache.write().databases = databases;
    Ok(())
}

pub async fn refresh_measurements_cache(
    cache: &Arc<RwLock<CompletionCache>>,
    client: &HyperbytedbClient,
    db: &str,
) -> crate::error::Result<()> {
    let measurements = fetch_measurements(client, db).await?;
    cache.write().measurements = measurements;
    Ok(())
}

pub fn clear_measurements_cache(cache: &Arc<RwLock<CompletionCache>>) {
    cache.write().clear_measurements();
}

async fn fetch_databases(client: &HyperbytedbClient) -> crate::error::Result<Vec<String>> {
    let resp = client
        .query(
            "SHOW DATABASES",
            &QueryOptions {
                db: None,
                retention_policy: None,
                epoch: None,
                pretty: false,
                chunked: false,
                chunk_size: None,
                format: OutputFormat::Json,
                params: None,
            },
        )
        .await?;
    Ok(first_column_values(&resp))
}

async fn fetch_measurements(
    client: &HyperbytedbClient,
    db: &str,
) -> crate::error::Result<Vec<String>> {
    let resp = client
        .query(
            "SHOW MEASUREMENTS",
            &QueryOptions {
                db: Some(db.to_string()),
                retention_policy: None,
                epoch: None,
                pretty: false,
                chunked: false,
                chunk_size: None,
                format: OutputFormat::Json,
                params: None,
            },
        )
        .await?;
    Ok(first_column_values(&resp))
}

pub struct CliHelper {
    cache: Arc<RwLock<CompletionCache>>,
}

impl CliHelper {
    pub fn new(cache: Arc<RwLock<CompletionCache>>) -> Self {
        Self { cache }
    }
}

impl Helper for CliHelper {}
impl Highlighter for CliHelper {}
impl Hinter for CliHelper {
    type Hint = String;
}
impl Validator for CliHelper {}

impl Completer for CliHelper {
    type Candidate = Pair;

    fn complete(
        &self,
        line: &str,
        pos: usize,
        _ctx: &Context<'_>,
    ) -> RustyResult<(usize, Vec<Pair>)> {
        let cache = self.cache.read();
        Ok(complete_line(line, pos, &cache))
    }
}

fn complete_line(line: &str, pos: usize, cache: &CompletionCache) -> (usize, Vec<Pair>) {
    let (start, tokens, word) = parse_context(line, pos);
    let candidates = if tokens.is_empty() {
        complete_first_word(&word)
    } else if is_meta_command_context(&tokens) {
        complete_meta(&tokens, &word, cache)
    } else {
        complete_sql(&tokens, &word, cache)
    };
    (start, candidates)
}

fn parse_context(line: &str, pos: usize) -> (usize, Vec<String>, String) {
    let safe_pos = pos.min(line.len());
    let before = &line[..safe_pos];
    let start = before
        .char_indices()
        .rfind(|(_, c)| c.is_whitespace())
        .map(|(idx, c)| idx + c.len_utf8())
        .unwrap_or(0);
    let word = line[start..safe_pos].to_string();
    let prefix = line[..start].trim();
    let tokens = prefix
        .split_whitespace()
        .map(ToString::to_string)
        .collect::<Vec<_>>();
    (start, tokens, word)
}

fn complete_first_word(word: &str) -> Vec<Pair> {
    let mut out = filter_prefix(META_COMMANDS, word, false);
    out.extend(filter_prefix(SQL_VERBS, word, true));
    out.sort_by(|a, b| {
        a.display
            .to_ascii_lowercase()
            .cmp(&b.display.to_ascii_lowercase())
    });
    out
}

fn is_meta_command_context(tokens: &[String]) -> bool {
    matches!(
        tokens[0].to_ascii_lowercase().as_str(),
        "auth"
            | "chunk"
            | "clear"
            | "connect"
            | "consistency"
            | "exit"
            | "format"
            | "help"
            | "history"
            | "insert"
            | "precision"
            | "pretty"
            | "quit"
            | "settings"
            | "timing"
            | "use"
    )
}

fn complete_meta(tokens: &[String], word: &str, cache: &CompletionCache) -> Vec<Pair> {
    match tokens[0].to_ascii_lowercase().as_str() {
        "format" if tokens.len() == 1 => filter_prefix(FORMAT_VALUES, word, false),
        "clear" if tokens.len() == 1 => filter_prefix(CLEAR_TARGETS, word, false),
        "consistency" if tokens.len() == 1 => filter_prefix(CONSISTENCY_VALUES, word, false),
        "precision" if tokens.len() == 1 => filter_prefix(PRECISION_VALUES, word, false),
        "use" if tokens.len() == 1 => filter_owned(&cache.databases, word),
        "chunk" if tokens.len() == 1 => filter_prefix(&["size"], word, false),
        "connect" | "insert" | "auth" | "help" | "history" | "settings" | "pretty" | "timing"
        | "exit" | "quit" => Vec::new(),
        _ if tokens.len() == 1 => complete_first_word(word),
        _ => Vec::new(),
    }
}

fn complete_sql(tokens: &[String], word: &str, cache: &CompletionCache) -> Vec<Pair> {
    match tokens[0].to_ascii_uppercase().as_str() {
        "SHOW" => complete_show(&tokens[1..], word, cache),
        "CREATE" => complete_create(&tokens[1..], word),
        "DROP" => complete_drop(&tokens[1..], word),
        "SELECT" | "DELETE" => complete_from_context(&tokens[1..], word, cache),
        _ => filter_prefix(SQL_KEYWORDS, word, true),
    }
}

fn complete_show(rest: &[String], word: &str, cache: &CompletionCache) -> Vec<Pair> {
    if rest.is_empty() {
        return filter_prefix(SHOW_OBJECTS, word, true);
    }
    if rest.len() == 1 && rest[0].eq_ignore_ascii_case("TAG") {
        return filter_prefix(TAG_SUBCOMMANDS, word, true);
    }
    if rest.len() >= 2 && rest[0].eq_ignore_ascii_case("TAG") {
        if rest.len() == 2 {
            return filter_prefix(TAG_SUBCOMMANDS, word, true);
        }
        return complete_name_context(rest, word, cache);
    }
    if rest.len() == 1 && rest[0].eq_ignore_ascii_case("FIELD") {
        return filter_prefix(&["KEYS"], word, true);
    }
    if rest.len() == 1
        && (rest[0].eq_ignore_ascii_case("CONTINUOUS")
            || rest[0].eq_ignore_ascii_case("MATERIALIZED"))
    {
        return filter_prefix(&["QUERIES", "VIEWS"], word, true);
    }
    if rest.len() == 1 && rest[0].eq_ignore_ascii_case("RETENTION") {
        return filter_prefix(&["POLICIES"], word, true);
    }
    complete_name_context(rest, word, cache)
}

fn complete_create(rest: &[String], word: &str) -> Vec<Pair> {
    if rest.is_empty() {
        return filter_prefix(CREATE_OBJECTS, word, true);
    }
    if rest.len() == 1 && rest[0].eq_ignore_ascii_case("RETENTION") {
        return filter_prefix(&["POLICY"], word, true);
    }
    if rest.len() == 1 && rest[0].eq_ignore_ascii_case("CONTINUOUS") {
        return filter_prefix(&["QUERY"], word, true);
    }
    if rest.len() == 1 && rest[0].eq_ignore_ascii_case("MATERIALIZED") {
        return filter_prefix(&["VIEW"], word, true);
    }
    if rest.len() >= 2
        && rest[0].eq_ignore_ascii_case("RETENTION")
        && rest[1].eq_ignore_ascii_case("POLICY")
    {
        return filter_prefix(&["ON"], word, true);
    }
    filter_prefix(CREATE_TAIL, word, true)
}

fn complete_drop(rest: &[String], word: &str) -> Vec<Pair> {
    if rest.is_empty() {
        return filter_prefix(DROP_OBJECTS, word, true);
    }
    if rest.len() == 1 && rest[0].eq_ignore_ascii_case("RETENTION") {
        return filter_prefix(&["POLICY"], word, true);
    }
    if rest.len() == 1 && rest[0].eq_ignore_ascii_case("CONTINUOUS") {
        return filter_prefix(&["QUERY"], word, true);
    }
    if rest.len() == 1 && rest[0].eq_ignore_ascii_case("MATERIALIZED") {
        return filter_prefix(&["VIEW"], word, true);
    }
    filter_prefix(SQL_KEYWORDS, word, true)
}

fn complete_from_context(rest: &[String], word: &str, cache: &CompletionCache) -> Vec<Pair> {
    if rest.last().is_some_and(|t| t.eq_ignore_ascii_case("FROM")) {
        return filter_owned(&cache.measurements, word);
    }
    let mut out = filter_prefix(SELECT_KEYWORDS, word, true);
    if rest.is_empty() {
        out.extend(filter_prefix(&["*"], word, true));
    }
    out
}

fn complete_name_context(rest: &[String], word: &str, cache: &CompletionCache) -> Vec<Pair> {
    if rest
        .last()
        .is_some_and(|t| t.eq_ignore_ascii_case("FROM") || t.eq_ignore_ascii_case("ON"))
    {
        return filter_owned(&cache.measurements, word);
    }
    filter_prefix(SHOW_MODIFIERS, word, true)
}

fn filter_prefix(candidates: &[&str], word: &str, uppercase: bool) -> Vec<Pair> {
    let needle = word.to_ascii_lowercase();
    candidates
        .iter()
        .filter(|candidate| candidate.to_ascii_lowercase().starts_with(&needle))
        .map(|candidate| pair(candidate, uppercase))
        .collect()
}

fn filter_owned(candidates: &[String], word: &str) -> Vec<Pair> {
    let needle = word.to_ascii_lowercase();
    candidates
        .iter()
        .filter(|candidate| candidate.to_ascii_lowercase().starts_with(&needle))
        .map(|candidate| pair(candidate, false))
        .collect()
}

fn pair(text: &str, uppercase: bool) -> Pair {
    let replacement = if uppercase {
        text.to_ascii_uppercase()
    } else {
        text.to_string()
    };
    Pair {
        display: replacement.clone(),
        replacement,
    }
}

fn first_column_values(response: &QueryResponse) -> Vec<String> {
    let mut out = Vec::new();
    for result in &response.results {
        let Some(series_list) = &result.series else {
            continue;
        };
        for series in series_list {
            for row in &series.values {
                if let Some(value) = row.first().and_then(|v| v.as_str()) {
                    out.push(value.to_string());
                }
            }
        }
    }
    out.sort();
    out.dedup();
    out
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn completes_meta_command_prefix() {
        let cache = CompletionCache::default();
        let (_, candidates) = complete_line("fo", 2, &cache);
        assert!(candidates.iter().any(|c| c.replacement == "format"));
    }

    #[test]
    fn completes_format_values() {
        let cache = CompletionCache::default();
        let (_, candidates) = complete_line("format j", 8, &cache);
        assert!(candidates.iter().any(|c| c.replacement == "json"));
    }

    #[test]
    fn completes_show_keywords() {
        let cache = CompletionCache::default();
        let (_, candidates) = complete_line("SHOW M", 6, &cache);
        assert!(candidates.iter().any(|c| c.replacement == "MEASUREMENTS"));
    }

    #[test]
    fn completes_show_tag_subcommands() {
        let cache = CompletionCache::default();
        let (_, candidates) = complete_line("SHOW TAG K", 10, &cache);
        assert!(candidates.iter().any(|c| c.replacement == "KEYS"));
    }

    #[test]
    fn completes_database_names_for_use() {
        let cache = CompletionCache {
            databases: vec!["metrics".to_string(), "telemetry".to_string()],
            ..Default::default()
        };
        let (_, candidates) = complete_line("use te", 6, &cache);
        assert!(candidates.iter().any(|c| c.replacement == "telemetry"));
    }
}
