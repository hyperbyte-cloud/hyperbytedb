# hyperbytedb-client (Rust client)

An async client for HyperbyteDB's InfluxDB v1 API, on reqwest (rustls) and
tokio.

```rust
use hyperbytedb_client::{Client, Options, Point, QueryOptions, WriteOptions};

let db = Client::with_options("http://hyperbytedb:8086", Options {
    username: Some("writer".into()),
    password: Some("...".into()),
    ..Default::default()
})?;
db.create_database("app").await?;
db.write("app", &[Point::new("cpu").tag("host", "web1").field("usage", 12.5)], &WriteOptions::default())
    .await?;
let results = db
    .query(r#"SELECT mean("usage") FROM "cpu" WHERE time > now() - 1h GROUP BY "host""#, &QueryOptions::database("app"))
    .await?;
for row in results[0].rows() {
    println!("{row:?}"); // {"host": String("web1"), "mean": Number(12.5), "time": Number(1791830000000)}
}
```

`Client` is cheap to clone, and clones share a connection pool. Use
`Client::with_http_client` to supply your own `reqwest::Client`, for example
for TLS roots or proxies.

## Writes

`write` takes `Point`s, and `write_lines` takes ready-made line protocol.

- **Batching, compression, retries.** As described in [the clients overview](../README.md).
  The fields are `batch_points`, `batch_bytes`, `gzip_writes`, `retries`,
  `backoff` and `max_backoff` on `Options`.
- **Errors.** A refused write is `Error::Write`, and a 401/403 is
  `Error::Auth`. Anything else after the last retry is `Error::Server`, or
  `Error::Connection` when the server can't be reached.
  `Error::is_retryable()` reports whether trying later may help. Batches
  before the failed one have been written.
- **Fields.** `f64`/`f32` become floats, and signed and unsigned integers
  become integers. `bool` and `&str`/`String` are supported too. A field
  keeps its first type, so write `1.0`, not `1`, for a float field. NaN and
  infinite floats are dropped.
- **Time.** `Point::timestamp(i64)` takes a time in the write's
  `precision`, and `Point::at(SystemTime)` takes a wall-clock time. Without
  either, the server stamps the point.
- **Tags** with an empty value are dropped. A newline in a name, tag or
  string field is `Error::InvalidPoint`.

## Queries

`query(q, &QueryOptions { database, epoch, params, keep_errors })` returns one
`QueryResult` per statement. `rows()` returns maps that carry the series'
tags. Values are `serde_json::Value`, so integers, including nanosecond
times, keep every digit.

- **Times** are integers in `epoch` units (`Epoch::Milliseconds` by
  default), or RFC 3339 strings with `Epoch::Rfc3339`.
- **Errors.** A statement error is `Error::Query` unless `keep_errors` is
  set.

`ping()` returns the server version, and `health()` returns `/health`.
There are also `create_database`, `drop_database` and `databases`.

## Tests

```sh
cargo test                                                   # unit tests, with a fake server
HYPERBYTEDB_URL=http://localhost:8086 cargo test --test integration
```
