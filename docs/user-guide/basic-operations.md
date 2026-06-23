# Basic operations

Create databases, ingest line protocol data, and query with TimeseriesQL.


## Creating a Database

Before writing data, create a database:

```bash
curl -sS -XPOST 'http://localhost:8086/query' \
  --data-urlencode 'q=CREATE DATABASE mydb'
```

Every database gets a default retention policy named `autogen` with infinite duration.

### List databases

```bash
curl -sS -G 'http://localhost:8086/query' \
  --data-urlencode 'q=SHOW DATABASES'
```

### Drop a database

```bash
curl -sS -XPOST 'http://localhost:8086/query' \
  --data-urlencode 'q=DROP DATABASE mydb'
```

---

## Writing Data

HyperbyteDB accepts data via the InfluxDB line protocol on the `/write` endpoint.

### Line Protocol Format

```
measurement,tag1=val1,tag2=val2 field1=1.0,field2="str",field3=true 1609459200000000000
```

| Component | Description |
|-----------|-------------|
| `measurement` | Name of the measurement (like a table) |
| `tag1=val1,...` | Optional comma-separated tag key-value pairs (indexed, string-only) |
| `field1=1.0,...` | Required comma-separated field key-value pairs (the actual data) |
| `1609459200000000000` | Optional timestamp (nanoseconds since Unix epoch by default) |

### Write a single point

```bash
curl -sS -XPOST 'http://localhost:8086/write?db=mydb' \
  --data-binary 'cpu,host=server01,region=us-west usage_idle=95.2,usage_user=4.8'
```

If no timestamp is provided, HyperbyteDB uses the current server time.

### Write multiple points

Send multiple lines separated by newlines:

```bash
curl -sS -XPOST 'http://localhost:8086/write?db=mydb&precision=s' \
  --data-binary 'cpu,host=server01 usage_idle=95.2 1609459200
cpu,host=server02 usage_idle=87.1 1609459200
mem,host=server01 used_percent=42.3 1609459200'
```

### Precision parameter

The `precision` query parameter controls timestamp interpretation:

| Value | Unit | Example Timestamp |
|-------|------|-------------------|
| `ns` (default) | Nanoseconds | `1609459200000000000` |
| `us` or `u` | Microseconds | `1609459200000000` |
| `ms` | Milliseconds | `1609459200000` |
| `s` | Seconds | `1609459200` |

### Gzip compression

HyperbyteDB accepts gzip-compressed payloads for efficient network transfer:

```bash
gzip -c data.txt | curl -sS -XPOST 'http://localhost:8086/write?db=mydb' \
  -H 'Content-Encoding: gzip' --data-binary @-
```

### Response codes

| Code | Meaning |
|------|---------|
| 204 | Success (no body) |
| 400 | Parse error or field type conflict |
| 404 | Database not found |
| 422 | Cardinality limit exceeded |

---

## Querying Data

HyperbyteDB supports TimeseriesQL queries via the `/query` endpoint.

> **Important:** Data must be flushed from the WAL into chDB MergeTree tables before it becomes queryable. The default flush interval is 10 seconds. Wait briefly after writing before querying.

### Basic SELECT

```bash
curl -sS -G 'http://localhost:8086/query' \
  --data-urlencode 'db=mydb' \
  --data-urlencode 'q=SELECT * FROM cpu'
```

### SELECT with time range

```bash
curl -sS -G 'http://localhost:8086/query' \
  --data-urlencode 'db=mydb' \
  --data-urlencode 'q=SELECT * FROM cpu WHERE time > now() - 1h'
```

### Aggregations with GROUP BY time

```bash
curl -sS -G 'http://localhost:8086/query' \
  --data-urlencode 'db=mydb' \
  --data-urlencode 'q=SELECT mean("usage_idle") FROM cpu WHERE time > now() - 24h GROUP BY time(1h), "host"'
```

### Timestamp format

By default, timestamps are returned as RFC3339 strings. Use the `epoch` parameter for numeric timestamps:

```bash
# Timestamps as nanosecond integers
curl -sS -G 'http://localhost:8086/query' \
  --data-urlencode 'db=mydb' \
  --data-urlencode 'q=SELECT * FROM cpu' \
  --data-urlencode 'epoch=ns'
```

| `epoch` value | Format |
|---------------|--------|
| _(empty)_ | RFC3339 (`"2024-01-15T10:00:00Z"`) |
| `ns` | Nanosecond integer |
| `us` or `u` | Microsecond integer |
| `ms` | Millisecond integer |
| `s` | Second integer |

### Multiple statements

Separate multiple statements with semicolons. Each gets its own `statement_id` in the response:

```bash
curl -sS -G 'http://localhost:8086/query' \
  --data-urlencode 'db=mydb' \
  --data-urlencode 'q=SELECT * FROM cpu LIMIT 5; SELECT * FROM mem LIMIT 5'
```

### Bind parameters

Substitute `$param` placeholders with a JSON `params` object:

```bash
curl -sS -G 'http://localhost:8086/query' \
  --data-urlencode 'db=mydb' \
  --data-urlencode "q=SELECT mean(\"value\") FROM \"cpu\" WHERE \"host\" = \$host AND time > now() - \$interval GROUP BY time(\$bucket)" \
  --data-urlencode 'params={"host":"server01","interval":"1h","bucket":"5m"}'
```

### POST queries

Queries can also be sent as POST with form-encoded body:

```bash
curl -sS -XPOST 'http://localhost:8086/query' \
  -d 'db=mydb' \
  -d 'q=SELECT * FROM cpu WHERE time > now() - 1h'
```

### Response format

```json
{
  "results": [
    {
      "statement_id": 0,
      "series": [
        {
          "name": "cpu",
          "tags": {"host": "server01"},
          "columns": ["time", "usage_idle"],
          "values": [
            ["2024-01-15T10:00:00Z", 95.2],
            ["2024-01-15T10:05:00Z", 93.8]
          ]
        }
      ]
    }
  ]
}
```

### CSV output

Request CSV format via the `Accept` header:

```bash
curl -sS -G 'http://localhost:8086/query' \
  -H 'Accept: text/csv' \
  --data-urlencode 'db=mydb' \
  --data-urlencode 'q=SELECT * FROM cpu LIMIT 5'
```

---

## Exploring Your Data

### List measurements

```sql
SHOW MEASUREMENTS [ON mydb]
```

### List tag keys for a measurement

```sql
SHOW TAG KEYS FROM cpu
```

### List tag values

```sql
SHOW TAG VALUES FROM cpu WITH KEY = "host"
SHOW TAG VALUES FROM cpu WITH KEY IN ("host", "region")
SHOW TAG VALUES FROM cpu WITH KEY =~ /^h.*/
```

### List field keys and types

```sql
SHOW FIELD KEYS FROM cpu
```

### List all series

```sql
SHOW SERIES FROM cpu
```

---

## Retention Policies

Retention policies control how long data is kept before automatic deletion.

### Create a retention policy

```sql
CREATE RETENTION POLICY "30_days" ON "mydb" DURATION 30d REPLICATION 1 DEFAULT
```

- `DURATION` — how long data is retained. Use `INF` for infinite retention.
- `REPLICATION` — replication factor (informational in HyperbyteDB).
- `DEFAULT` — makes this the default RP for writes that don't specify one.

### Alter a retention policy

```sql
ALTER RETENTION POLICY "30_days" ON "mydb" DURATION 60d DEFAULT
```

### View retention policies

```sql
SHOW RETENTION POLICIES ON "mydb"
```

A background service runs every 12 hours (by default) and issues `ALTER TABLE … DELETE` for rows outside each retention policy's duration window.

---

## Deleting Data

HyperbyteDB supports `DELETE` statements that mark data with tombstones. Tombstoned data is excluded from query results at read time; physical row removal happens via retention enforcement or MergeTree background merges.

```sql
-- Delete all data older than 30 days
DELETE FROM "cpu" WHERE time < now() - 30d

-- Delete data for a specific host
DELETE FROM "cpu" WHERE "host" = 'decommissioned-01' AND time < '2024-06-01'
```

### Drop a measurement

To remove an entire measurement and all its metadata:

```sql
DROP MEASUREMENT "cpu"
```

---

## See Also

- [Advanced features](advanced-features.md) — Clustering, continuous queries, TLS, auth
- [API & TimeseriesQL Reference](reference.md) — Complete syntax reference
