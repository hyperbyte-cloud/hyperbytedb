# hyperbytedb (JavaScript / TypeScript client)

A client for HyperbyteDB's InfluxDB v1 API, written in TypeScript with no
dependencies. It runs on Node.js 18+ and in browsers, using `fetch` and
`CompressionStream`.

```ts
import { HyperbyteDBClient } from "hyperbytedb";

const db = new HyperbyteDBClient("http://hyperbytedb:8086", { username: "writer", password: "..." });
await db.createDatabase("app");
await db.write([{ measurement: "cpu", tags: { host: "web1" }, fields: { usage: 12.5, cores: 8n } }], "app");
const [result] = await db.query('SELECT mean("usage") FROM "cpu" WHERE time > now() - 1h GROUP BY "host"', { database: "app" });
for (const row of result.rows()) console.log(row); // { host: 'web1', time: 1791830000000, mean: 12.5 }
```

Build with `npm install && npm run build`. The package is ESM, with type
declarations in `dist/`.

## Writes

`write(data, database, { retentionPolicy, precision, signal })` takes Points,
a line-protocol string, or a list of lines.

- **Batching, compression, retries.** As described in [the clients overview](../README.md).
  The options are `batchPoints`, `batchBytes`, `gzipWrites`, `retries`,
  `backoff` and `maxBackoff` (in ms).
- **Errors.** A refused write throws `WriteError`, and a 401/403 throws
  `AuthError`. Anything else after the last retry throws `ServerError`. All
  of them extend `HyperbyteDBError`. `error.written` is how many lines were
  stored before the error: earlier batches, or half of a batch split after
  a 413.
- **Fields.** A `number` is written as a float and a `bigint` as an integer
  (`8n` → `8i`). A field keeps the type it was first written with.
  `null`, `undefined`, NaN and ±Infinity are dropped.
- **Time** is a `Date`, a `number` or `bigint` in `precision` units (`ns` by
  default), or absent for the server's clock. Use a `bigint` for
  nanoseconds, because a `number` cannot hold them exactly.
- **Tags** with an empty value are dropped. A newline in a name, tag or
  string field throws.
- **Cancelling.** An `AbortSignal` stops the request and its retries.

## Queries

`query(q, { database, epoch, params, keepErrors, signal })` returns one
`Result` per statement. `result.rows()` returns objects that carry the
series' tags.

- **Times** are numbers in `epoch` units (`ms` by default), or RFC 3339
  strings with `epoch: "rfc3339"`. Nanosecond times do not fit a JS number
  exactly, so use `ms` or `u`.
- **Errors.** A statement error throws `QueryError` unless `keepErrors` is
  set.

`ping()` returns the server version, and `health()` returns `/health`. There
are also `createDatabase()`, `dropDatabase()` and `databases()`.

## In a browser

HyperbyteDB sends no CORS headers yet, so a page can reach it only from
the same origin, for example through a reverse proxy that serves both.
Credentials in browser code are visible to the user.

## Tests

```sh
npm install
npm test                                                  # unit tests, with a fake server
HYPERBYTEDB_URL=http://localhost:8086 npm run test:integration
```
