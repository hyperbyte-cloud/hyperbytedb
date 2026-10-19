# hyperbytedb (Python client)

A small Python client for HyperbyteDB's InfluxDB v1 API (`/write`, `/query`).
Standard library only, Python 3.9+.

```python
from hyperbytedb import HyperbyteDBClient, Point

db = HyperbyteDBClient("http://hyperbytedb:8086", username="writer", password="...")
db.create_database("app")
db.write([Point("cpu", {"usage": 12.5}, {"host": "web1"})], "app")
for row in db.query('SELECT mean("usage") FROM "cpu" WHERE time > now() - 1h GROUP BY "host"', "app")[0].rows():
    print(row)            # {'host': 'web1', 'time': 1791830000000, 'mean': 12.5}
```

Install from this directory with `pip install .`, or copy `src/hyperbytedb`
next to your code: it has no dependencies.

## Writes

`write()` takes `Point`s, a line-protocol string, or a list of lines.

- **Batching.** Writes go in batches of `batch_points` (5,000) points or
  `batch_bytes` (4 MiB), whichever comes first.
- **Compression.** Each batch is gzipped (`gzip_writes=False` to turn this off).
- **Retries.** A batch is retried with exponential backoff and jitter
  (`retries=5`, `backoff=0.5` s, `max_backoff=30` s) on connection errors,
  429 and 5xx. The client honours `Retry-After`, which HyperbyteDB sends
  with a 503 when write admission is full.
- **Too large.** A batch refused with 413 is split in half until it fits.
- **Errors.** A 400 (bad line protocol, field type conflict) raises
  `WriteError` with the server's message and is not retried. 401/403 raise
  `AuthError`. The error's `written` is how many lines were stored before
  it: earlier batches, or half of a batch split after a 413.

`Point(measurement, fields, tags, time)` details:

- **Time** is a `datetime` (naive means UTC), a number in the write's
  `precision` units (`ns` default, `u`, `ms`, `s`), or `None` for the
  server's clock.
- **Fields** may be float, int (sent as `i`), bool or str. Fields set to
  `None`, NaN or ±inf are dropped, and a point with no fields left is
  skipped.
- **Tags.** Empty tag values are dropped, because line protocol has no way to
  send them.
- **Escaping** follows line protocol. A newline in a name or tag raises
  `ValueError`.

## Queries

`query(q, database, epoch="ms", params=None)` returns one `Result` per
statement. `Result.rows()` yields dicts that carry the series' tags.

- **Times** are integers in `epoch` units, or RFC 3339 strings with
  `epoch=None`.
- **Bound parameters.** `params={"host": "web1"}` binds `$host`.
- **Errors.** A statement error raises `QueryError` unless you pass
  `raise_errors=False`.

`ping()` returns the server version, `health()` returns `/health` as JSON, and
there are `create_database()`, `drop_database()` and `databases()`.

## Known differences from InfluxDB

- Boolean fields come back as `1`/`0`, not `true`/`false`. See the
  strict `xfail` in `tests/test_integration.py`.

## Tests

```sh
pip install -e '.[test]'
pytest                                   # unit tests, with a fake server
HYPERBYTEDB_URL=http://localhost:8086 HYPERBYTEDB_USER=admin HYPERBYTEDB_PASSWORD=... pytest
```

The integration tests create and drop a randomly named database.
