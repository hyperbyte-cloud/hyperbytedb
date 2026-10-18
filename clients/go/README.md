# hyperbytedb (Go client)

A Go client for HyperbyteDB's InfluxDB v1 API. Standard library only, Go 1.21+.

```go
import hyperbytedb "github.com/hyperbyte-cloud/hyperbytedb/clients/go"

db := hyperbytedb.NewClient("http://hyperbytedb:8086", hyperbytedb.Options{Username: "writer", Password: "..."})
_ = db.CreateDatabase(ctx, "app")
_, err := db.Write(ctx, "app", []hyperbytedb.Point{{
	Measurement: "cpu",
	Tags:        map[string]string{"host": "web1"},
	Fields:      map[string]any{"usage": 12.5},
}}, hyperbytedb.WriteOptions{})
results, err := db.Query(ctx, `SELECT mean("usage") FROM "cpu" WHERE time > now() - 1h GROUP BY "host"`,
	hyperbytedb.QueryOptions{Database: "app"})
for _, row := range results[0].Rows() {
	fmt.Println(row) // map[host:web1 mean:12.5 time:1791830000000]
}
```

A `Client` is safe for concurrent use. Every call takes a `context.Context`,
which also stops retries.

## Writes

`Write` takes `Point`s; `WriteLines` takes ready-made line protocol.

- **Batching, compression, retries.** As described in [the clients overview](../README.md).
  The options are `BatchPoints`, `BatchBytes`, `DisableGzip`, `Retries`
  (`-1` for none), `Backoff` and `MaxBackoff`.
- **Errors.** A refused write is a `*WriteError`, and a 401/403 is an
  `*AuthError`. Anything else after the last retry is a `*ServerError`.
  Use `errors.As` to tell them apart. `IsRetryable(err)` reports whether
  trying later may help. Batches before the failed one have been written.
- **Fields** may be any float, signed or unsigned integer (sent as an
  integer), bool or string. Nil, NaN and ±Inf fields are dropped. A point
  with no fields left is skipped. A field keeps its first type, so write
  `1.0`, not `1`, for a float field.
- **Time.** A zero `Time` leaves the time to the server. `Precision`
  (`Nanosecond` by default, `Microsecond`, `Millisecond`, `Second`) sets the
  unit written.
- **Tags** with an empty value are dropped. A newline in a name, tag or
  string field is an error.

## Queries

`Query(ctx, q, QueryOptions{Database, Epoch, Params, KeepErrors})` returns
one `Result` per statement. `Result.Rows()` returns maps that carry the
series' tags.

- **Times** are integers in `Epoch` units (`EpochMillisecond` by default), or
  RFC 3339 strings with `EpochRFC3339`.
- **Numbers.** Integers come back as `int64`, so nanosecond times survive.
  Other numbers come back as `float64`.
- **Errors.** A statement error is a `*QueryError` unless `KeepErrors` is
  set, in which case it stays in `Result.Error`.

`Ping` returns the server version, and `Health` returns `/health`. There
are also `CreateDatabase`, `DropDatabase` and `Databases`.

## Tests

```sh
go test ./...                                          # unit tests, with a fake server
HYPERBYTEDB_URL=http://localhost:8086 go test -run Integration ./...
```
