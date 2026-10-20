use std::collections::BTreeMap;
use std::io::Write as _;
use std::time::Duration;

use reqwest::header::{HeaderMap, HeaderValue, CONTENT_ENCODING, CONTENT_TYPE, RETRY_AFTER};
use reqwest::Method;
use serde::Deserialize;
use serde_json::Value;

use crate::error::{Error, Result};
use crate::line_protocol::{Point, Precision};

/// This client's version.
pub const VERSION: &str = env!("CARGO_PKG_VERSION");

/// Statuses worth another attempt: rate limited, overloaded (HyperbyteDB
/// answers 503 with Retry-After when write admission is full), or a server
/// or gateway error that may pass.
pub(crate) fn retryable(status: u16) -> bool {
    matches!(status, 429 | 500 | 502 | 503 | 504)
}

/// Connection settings.
#[derive(Debug, Clone)]
pub struct Options {
    /// Sent as `Authorization: Basic`, never in the URL.
    pub username: Option<String>,
    pub password: Option<String>,
    /// Per request (default 30 s).
    pub timeout: Duration,
    /// Retries on connection errors, 429 and 5xx (default 5).
    pub retries: u32,
    /// First retry delay, doubled up to `max_backoff`, with jitter (0.5 s /
    /// 30 s). A `Retry-After` header wins.
    pub backoff: Duration,
    pub max_backoff: Duration,
    /// Gzip write bodies (default true).
    pub gzip_writes: bool,
    /// Each write request holds at most this many points / bytes (5,000 / 4 MiB).
    pub batch_points: usize,
    pub batch_bytes: usize,
}

impl Default for Options {
    fn default() -> Self {
        Options {
            username: None,
            password: None,
            timeout: Duration::from_secs(30),
            retries: 5,
            backoff: Duration::from_millis(500),
            max_backoff: Duration::from_secs(30),
            gzip_writes: true,
            batch_points: 5_000,
            batch_bytes: 4 * 1024 * 1024,
        }
    }
}

/// A write's retention policy (default: the database's) and timestamp
/// precision (default ns).
#[derive(Debug, Clone, Default)]
pub struct WriteOptions {
    pub retention_policy: Option<String>,
    pub precision: Precision,
}

/// The unit query times come back in.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum Epoch {
    Nanoseconds,
    Microseconds,
    #[default]
    Milliseconds,
    Seconds,
    /// RFC 3339 strings.
    Rfc3339,
}

/// A query's database, time unit (default ms) and bound parameters (`$name`).
/// A statement's error is returned as [`Error::Query`] unless `keep_errors`.
#[derive(Debug, Clone, Default)]
pub struct QueryOptions {
    pub database: Option<String>,
    pub epoch: Epoch,
    pub params: Option<serde_json::Map<String, Value>>,
    pub keep_errors: bool,
}

impl QueryOptions {
    pub fn database(db: impl Into<String>) -> Self {
        QueryOptions {
            database: Some(db.into()),
            ..Default::default()
        }
    }
}

/// One series of a result.
#[derive(Debug, Clone, Deserialize, PartialEq)]
pub struct Series {
    #[serde(default)]
    pub name: String,
    #[serde(default)]
    pub tags: BTreeMap<String, String>,
    #[serde(default)]
    pub columns: Vec<String>,
    #[serde(default)]
    pub values: Vec<Vec<Value>>,
}

impl Series {
    /// Each row as a map of column to value, with the series' tags.
    pub fn rows(&self) -> Vec<BTreeMap<String, Value>> {
        self.values
            .iter()
            .map(|v| {
                let mut row: BTreeMap<String, Value> = self
                    .tags
                    .iter()
                    .map(|(k, t)| (k.clone(), Value::String(t.clone())))
                    .collect();
                row.extend(self.columns.iter().cloned().zip(v.iter().cloned()));
                row
            })
            .collect()
    }
}

/// One statement's result.
#[derive(Debug, Clone, Deserialize, PartialEq)]
pub struct QueryResult {
    #[serde(default)]
    pub statement_id: u64,
    #[serde(default)]
    pub series: Vec<Series>,
    pub error: Option<String>,
}

impl QueryResult {
    pub fn rows(&self) -> Vec<BTreeMap<String, Value>> {
        self.series.iter().flat_map(Series::rows).collect()
    }
}

struct Reply {
    status: u16,
    headers: HeaderMap,
    body: Vec<u8>,
}

/// A HyperbyteDB connection. `url` is a node, or a load balancer or
/// Kubernetes Service in front of the cluster: any node accepts any write or
/// query. Cheap to clone; clones share a connection pool.
///
/// Writes are batched, gzipped and retried with backoff on connection
/// errors, 429 and 5xx, honouring `Retry-After`. A batch refused as too
/// large (413) is split in two and retried.
#[derive(Debug, Clone)]
pub struct Client {
    url: String,
    http: reqwest::Client,
    opts: Options,
}

impl Client {
    pub fn new(url: impl Into<String>) -> Result<Self> {
        Self::with_options(url, Options::default())
    }

    pub fn with_options(url: impl Into<String>, opts: Options) -> Result<Self> {
        let http = reqwest::Client::builder()
            .user_agent(format!("hyperbytedb-rust/{VERSION}"))
            .timeout(opts.timeout)
            .build()
            .map_err(|source| Error::Connection {
                method: "BUILD",
                path: "",
                source,
            })?;
        Ok(Self::with_http_client(url, opts, http))
    }

    /// With your own `reqwest::Client` (TLS roots, proxies, ...). Its timeout
    /// applies, not `opts.timeout`.
    pub fn with_http_client(
        url: impl Into<String>,
        mut opts: Options,
        http: reqwest::Client,
    ) -> Self {
        opts.batch_points = opts.batch_points.max(1);
        opts.batch_bytes = opts.batch_bytes.max(1);
        Client {
            url: url.into().trim_end_matches('/').to_owned(),
            http,
            opts,
        }
    }

    async fn request(
        &self,
        method: Method,
        path: &'static str,
        query: &[(&str, &str)],
        body: Option<Vec<u8>>,
        headers: HeaderMap,
        answers: &[u16],
    ) -> Result<Reply> {
        let url = format!("{}{path}", self.url);
        let mut attempt: u32 = 0;
        loop {
            let mut req = self
                .http
                .request(method.clone(), &url)
                .query(query)
                .headers(headers.clone());
            if let Some(user) = &self.opts.username {
                req = req.basic_auth(user, Some(self.opts.password.as_deref().unwrap_or("")));
            }
            if let Some(b) = &body {
                req = req.body(b.clone());
            }
            let wait = match req.send().await {
                Ok(resp) => {
                    let status = resp.status().as_u16();
                    let headers = resp.headers().clone();
                    let body = resp.bytes().await.map(|b| b.to_vec());
                    match body {
                        Ok(body) => {
                            if !retryable(status)
                                || answers.contains(&status)
                                || attempt >= self.opts.retries
                            {
                                return Ok(Reply {
                                    status,
                                    headers,
                                    body,
                                });
                            }
                            retry_after(&headers)
                        }
                        Err(source) if attempt >= self.opts.retries => {
                            return Err(Error::Connection {
                                method: method_name(&method),
                                path,
                                source,
                            });
                        }
                        Err(_) => None,
                    }
                }
                Err(source) if attempt >= self.opts.retries => {
                    return Err(Error::Connection {
                        method: method_name(&method),
                        path,
                        source,
                    });
                }
                Err(_) => None,
            };
            attempt += 1;
            tokio::time::sleep(wait.unwrap_or_else(|| self.backoff(attempt))).await;
        }
    }

    fn backoff(&self, attempt: u32) -> Duration {
        let base = self
            .opts
            .backoff
            .checked_mul(1u32.checked_shl(attempt - 1).unwrap_or(u32::MAX))
            .unwrap_or(self.opts.max_backoff)
            .min(self.opts.max_backoff);
        base.mul_f64(0.5 + jitter() / 2.0)
    }

    /// The server's version, e.g. `0.9.3-beta`; an error if it is down.
    pub async fn ping(&self) -> Result<String> {
        let r = self
            .request(Method::GET, "/ping", &[], None, HeaderMap::new(), &[])
            .await?;
        if r.status >= 300 {
            return Err(Error::Server {
                status: r.status,
                message: error_text(&r.body),
            });
        }
        let header = |name| {
            r.headers
                .get(name)
                .and_then(|v: &HeaderValue| v.to_str().ok())
        };
        if let Some(v) = header("x-hyperbytedb-version") {
            return Ok(v.to_owned());
        }
        // Servers before 0.9.3: X-Influxdb-Version: HyperbyteDB-<version>.
        let legacy = header("x-influxdb-version").unwrap_or("");
        Ok(legacy
            .strip_prefix("HyperbyteDB-")
            .unwrap_or(legacy)
            .to_owned())
    }

    /// `/health`: the node's state. A 503 (draining, syncing) is its answer,
    /// returned, not retried.
    pub async fn health(&self) -> Result<Value> {
        let r = self
            .request(Method::GET, "/health", &[], None, HeaderMap::new(), &[503])
            .await?;
        if r.status >= 300 && r.status != 503 {
            return Err(Error::Server {
                status: r.status,
                message: error_text(&r.body),
            });
        }
        if r.body.is_empty() {
            return Ok(Value::Object(Default::default()));
        }
        serde_json::from_slice(&r.body).map_err(|e| Error::Decode(e.to_string()))
    }

    /// Writes points and returns how many lines were sent. Points with no
    /// field left are skipped.
    pub async fn write(
        &self,
        database: &str,
        points: &[Point],
        opts: &WriteOptions,
    ) -> Result<usize> {
        let mut lines = Vec::with_capacity(points.len());
        for p in points {
            if let Some(line) = p.to_line(opts.precision)? {
                lines.push(line);
            }
        }
        self.write_lines(database, lines, opts).await
    }

    /// Writes ready-made line protocol, one line per item (blank ones
    /// skipped), batched like [`Client::write`].
    pub async fn write_lines<I, S>(
        &self,
        database: &str,
        lines: I,
        opts: &WriteOptions,
    ) -> Result<usize>
    where
        I: IntoIterator<Item = S>,
        S: AsRef<str>,
    {
        let mut query = vec![("db", database), ("precision", opts.precision.as_str())];
        if let Some(rp) = opts.retention_policy.as_deref() {
            query.push(("rp", rp));
        }
        let mut sent = 0;
        let mut batch: Vec<String> = Vec::new();
        let mut size = 0;
        for line in lines {
            let line = line.as_ref();
            if line.trim().is_empty() {
                continue;
            }
            if !batch.is_empty()
                && (batch.len() >= self.opts.batch_points
                    || size + line.len() + 1 > self.opts.batch_bytes)
            {
                sent += self
                    .write_batch(&batch, &query)
                    .await
                    .map_err(|e| interrupted(e, sent))?;
                batch.clear();
                size = 0;
            }
            size += line.len() + 1;
            batch.push(line.to_owned());
        }
        if !batch.is_empty() {
            sent += self
                .write_batch(&batch, &query)
                .await
                .map_err(|e| interrupted(e, sent))?;
        }
        Ok(sent)
    }

    /// How many of `batch`'s lines were written; an error that comes after
    /// some of them (a 413 split) is [`Error::WriteInterrupted`].
    async fn write_batch(&self, batch: &[String], query: &[(&str, &str)]) -> Result<usize> {
        let mut written = 0;
        let mut stack = vec![batch];
        while let Some(part) = stack.pop() {
            let mut body = part.join("\n").into_bytes();
            let mut headers = HeaderMap::new();
            headers.insert(
                CONTENT_TYPE,
                HeaderValue::from_static("text/plain; charset=utf-8"),
            );
            if self.opts.gzip_writes {
                let mut gz = flate2::write::GzEncoder::new(Vec::new(), flate2::Compression::fast());
                gz.write_all(&body)
                    .and_then(|_| gz.finish().map(|b| body = b))
                    .map_err(|e| Error::Decode(e.to_string()))?;
                headers.insert(CONTENT_ENCODING, HeaderValue::from_static("gzip"));
            }
            let r = self
                .request(Method::POST, "/write", query, Some(body), headers, &[])
                .await
                .map_err(|e| interrupted(e, written))?;
            match r.status {
                200 | 204 => written += part.len(),
                413 if part.len() > 1 => {
                    let (a, b) = part.split_at(part.len() / 2);
                    // Pushed in reverse, so the first half is written first.
                    stack.push(b);
                    stack.push(a);
                }
                status => {
                    let message = error_text(&r.body);
                    let e = match status {
                        401 | 403 => Error::Auth { status, message },
                        s if s >= 500 || s == 429 => Error::Server { status, message },
                        _ => Error::Write { status, message },
                    };
                    return Err(interrupted(e, written));
                }
            }
        }
        Ok(written)
    }

    /// Runs InfluxQL; one [`QueryResult`] per statement.
    pub async fn query(&self, q: &str, opts: &QueryOptions) -> Result<Vec<QueryResult>> {
        let mut form = vec![("q", q.to_owned())];
        if let Some(db) = &opts.database {
            form.push(("db", db.clone()));
        }
        let epoch = match opts.epoch {
            Epoch::Nanoseconds => Some("ns"),
            Epoch::Microseconds => Some("u"),
            Epoch::Milliseconds => Some("ms"),
            Epoch::Seconds => Some("s"),
            Epoch::Rfc3339 => None,
        };
        if let Some(e) = epoch {
            form.push(("epoch", e.to_owned()));
        }
        if let Some(p) = &opts.params {
            form.push(("params", Value::Object(p.clone()).to_string()));
        }
        let body = form_urlencode(&form);
        let mut headers = HeaderMap::new();
        headers.insert(
            CONTENT_TYPE,
            HeaderValue::from_static("application/x-www-form-urlencoded"),
        );
        let r = self
            .request(
                Method::POST,
                "/query",
                &[],
                Some(body.into_bytes()),
                headers,
                &[],
            )
            .await?;
        if r.status == 401 || r.status == 403 {
            return Err(Error::Auth {
                status: r.status,
                message: error_text(&r.body),
            });
        }
        if r.status >= 300 && r.body.is_empty() {
            return Err(Error::Server {
                status: r.status,
                message: String::new(),
            });
        }
        #[derive(Deserialize)]
        struct Doc {
            results: Option<Vec<QueryResult>>,
            error: Option<String>,
        }
        let doc: Doc = match serde_json::from_slice(&r.body) {
            Ok(d) => d,
            Err(_) if r.status >= 300 => {
                return Err(Error::Server {
                    status: r.status,
                    message: error_text(&r.body),
                })
            }
            Err(e) => return Err(Error::Decode(e.to_string())),
        };
        let results = match (doc.results, doc.error) {
            (Some(results), _) => results,
            (None, Some(error)) if r.status >= 500 => {
                return Err(Error::Server {
                    status: r.status,
                    message: error,
                })
            }
            (None, Some(error)) => return Err(Error::Query(error)),
            (None, None) => Vec::new(),
        };
        if !opts.keep_errors {
            if let Some(e) = results.iter().find_map(|r| r.error.clone()) {
                return Err(Error::Query(e));
            }
        }
        Ok(results)
    }

    pub async fn create_database(&self, name: &str) -> Result<()> {
        let opts = QueryOptions {
            epoch: Epoch::Rfc3339,
            ..Default::default()
        };
        self.query(&format!("CREATE DATABASE \"{}\"", quote_ident(name)), &opts)
            .await
            .map(drop)
    }

    pub async fn drop_database(&self, name: &str) -> Result<()> {
        let opts = QueryOptions {
            epoch: Epoch::Rfc3339,
            ..Default::default()
        };
        self.query(&format!("DROP DATABASE \"{}\"", quote_ident(name)), &opts)
            .await
            .map(drop)
    }

    pub async fn databases(&self) -> Result<Vec<String>> {
        let opts = QueryOptions {
            epoch: Epoch::Rfc3339,
            ..Default::default()
        };
        let results = self.query("SHOW DATABASES", &opts).await?;
        Ok(results
            .iter()
            .flat_map(QueryResult::rows)
            .filter_map(|row| row.get("name").and_then(Value::as_str).map(str::to_owned))
            .collect())
    }
}

/// `e` after `before` lines were written: [`Error::WriteInterrupted`] when
/// any were, counting those it already carries.
fn interrupted(e: Error, before: usize) -> Error {
    match e {
        _ if before == 0 => e,
        Error::WriteInterrupted { written, source } => Error::WriteInterrupted {
            written: before + written,
            source,
        },
        e => Error::WriteInterrupted {
            written: before,
            source: Box::new(e),
        },
    }
}

fn method_name(m: &Method) -> &'static str {
    match *m {
        Method::GET => "GET",
        Method::POST => "POST",
        _ => "HTTP",
    }
}

fn quote_ident(name: &str) -> String {
    name.replace('\\', "\\\\").replace('"', "\\\"")
}

fn retry_after(headers: &HeaderMap) -> Option<Duration> {
    let s: f64 = headers
        .get(RETRY_AFTER)?
        .to_str()
        .ok()?
        .trim()
        .parse()
        .ok()?;
    (s.is_finite() && s >= 0.0).then(|| Duration::from_secs_f64(s))
}

fn error_text(body: &[u8]) -> String {
    #[derive(Deserialize)]
    struct E {
        error: Value,
    }
    match serde_json::from_slice::<E>(body) {
        Ok(E {
            error: Value::String(s),
        }) => s,
        Ok(E { error }) => error.to_string(),
        Err(_) => String::from_utf8_lossy(body).trim().to_owned(),
    }
}

/// `application/x-www-form-urlencoded`, without another dependency.
fn form_urlencode(pairs: &[(&str, String)]) -> String {
    fn enc(out: &mut String, s: &str) {
        for b in s.bytes() {
            match b {
                b'A'..=b'Z' | b'a'..=b'z' | b'0'..=b'9' | b'-' | b'.' | b'_' | b'*' => {
                    out.push(b as char)
                }
                b' ' => out.push('+'),
                _ => out.push_str(&format!("%{b:02X}")),
            }
        }
    }
    let mut out = String::new();
    for (i, (k, v)) in pairs.iter().enumerate() {
        if i > 0 {
            out.push('&');
        }
        enc(&mut out, k);
        out.push('=');
        enc(&mut out, v);
    }
    out
}

/// A number in [0, 1) for backoff jitter; not for anything that needs
/// randomness.
fn jitter() -> f64 {
    use std::hash::{BuildHasher, Hasher};
    let mut h = std::collections::hash_map::RandomState::new().build_hasher();
    h.write_u64(
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map_or(0, |d| d.as_nanos() as u64),
    );
    (h.finish() >> 11) as f64 / (1u64 << 53) as f64
}
