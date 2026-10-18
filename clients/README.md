# HyperbyteDB clients

Clients for HyperbyteDB's InfluxDB v1 API (`/write`, `/query`). They behave
the same way:

- **Writes** are batched (5,000 points or 4 MiB per request), gzipped, and
  retried with backoff and jitter on connection errors, 429 and 5xx,
  honouring `Retry-After`. A batch refused as too large (413) is split in
  half. A refused write (400) is an error and is not retried.
- **Queries** return one result per statement. Rows carry their series' tags.
  Times are integers in `ms` by default. `$name` parameters can be bound.
- **`ping()`** returns the server version (`X-Hyperbytedb-Version`, else
  `X-Influxdb-Version` from servers before 0.9.3). `health()` returns
  `/health`.

| Language | Directory | Dependencies |
|----------|-----------|--------------|
| Python 3.9+ | [`python`](python/README.md) | none |
| Go 1.21+ | [`go`](go/README.md) | none |
| JavaScript / TypeScript (Node.js 18+ and browsers) | [`js`](js/README.md) | none |
| Rust | [`rust`](rust/README.md) | reqwest, serde, flate2 |

Each has unit tests against a fake server, and integration tests that run
against a real server when `HYPERBYTEDB_URL` is set (with
`HYPERBYTEDB_USER` / `HYPERBYTEDB_PASSWORD` when auth is on). The
integration tests create and drop a randomly named database.
