# Rate Limiting

HyperbyteDB can limit how many HTTP requests each client-facing data endpoint accepts per second. Rate limiting protects the server from ingest or query storms without requiring authentication.

---

## When to use it

Enable rate limiting when:

- A HyperbyteDB node is exposed on a shared or untrusted network
- You want a safety valve against runaway clients (Telegraf misconfiguration, retry loops, load tests)
- You need predictable back-pressure before work reaches chDB or the WAL

Rate limiting is **optional** and **disabled by default**.

---

## How it works

HyperbyteDB uses a **token bucket** per endpoint:

| Endpoint | Rate-limited? |
|----------|---------------|
| `POST /write` | Yes (when enabled) |
| `GET` / `POST /query` | Yes (when enabled) |
| `/ping`, `/health`, `/metrics`, cluster/internal routes | No |

Key behavior:

1. **Independent budgets** — `/write` and `/query` each get their own bucket. A query storm does not consume write capacity and vice versa.
2. **Per-second refill** — Each bucket holds up to `max_requests_per_second` tokens and refills at that rate every wall-clock second. After a burst exhausts the bucket, clients must wait for tokens to return (typically within one second).
3. **429 before auth** — When the bucket is empty, HyperbyteDB returns **429 Too Many Requests** with body `rate limit exceeded, try again later`. This happens **before** the [authentication](authentication.md) middleware runs, so rejected requests do not count as failed logins.
4. **No global limit** — The limit applies per HyperbyteDB process. Multiple clients share the same bucket for each endpoint on that node.

---

## Configuration

### TOML

```toml
[rate_limit]
enabled = true
max_requests_per_second = 100
```

| Key | Type | Default | Description |
|-----|------|---------|-------------|
| `enabled` | boolean | `false` | Turn rate limiting on for `/write` and `/query` |
| `max_requests_per_second` | integer | `0` | Token refill rate and burst capacity **per endpoint**. Set a positive value when `enabled = true`. `0` means unlimited (same as disabled in practice) |

See the full [`[rate_limit]`](configuration.md#rate_limit) section in [Configuration](configuration.md).

### Environment variables

```bash
export HYPERBYTEDB__RATE_LIMIT__ENABLED=true
export HYPERBYTEDB__RATE_LIMIT__MAX_REQUESTS_PER_SECOND=100
```

### Kubernetes operator

When deploying with the operator, set the `rateLimit` block on `HyperbytedbCluster`. See [HyperbytedbCluster — rateLimit](operator/cluster.md#ratelimit).

---

## Example

With `max_requests_per_second = 5`:

1. Five rapid `POST /write` requests succeed (burst capacity).
2. A sixth write in the same second receives **429**.
3. After ~1 second, tokens refill and writes succeed again.

The same pattern applies independently to `/query`.

---

## Client handling

Clients should treat **429** as transient back-pressure:

- **Backoff and retry** — Wait at least one second before retrying; use exponential backoff for sustained overload.
- **Do not disable rate limiting** to fix client bugs — tune `max_requests_per_second` or fix the client instead.
- **Telegraf / Grafana** — Ensure batch sizes and flush intervals match your configured limit. A single Telegraf agent usually stays well under 100 req/s; many agents behind one node may need a higher limit or a [hyperbytedb-proxy](operator/hyperbytedb-proxy.md).

Example response:

```
HTTP/1.1 429 Too Many Requests
rate limit exceeded, try again later
```

---

## Monitoring

Rejected requests increment the Prometheus counter:

| Metric | Type | Description |
|--------|------|-------------|
| `hyperbytedb_rate_limit_denied_total` | counter | Requests rejected with 429 because the endpoint bucket was empty |

Example PromQL:

```promql
# Denials per second (all endpoints on this node)
rate(hyperbytedb_rate_limit_denied_total[5m])

# Alert when denials are sustained
rate(hyperbytedb_rate_limit_denied_total[1m]) > 0
```

See [Administration — Monitoring](administration.md#monitoring) for scrape setup.

---

## Tuning

| Symptom | Likely cause | Action |
|---------|--------------|--------|
| Frequent 429 from one client | Limit too low for legitimate traffic | Raise `max_requests_per_second` |
| Sustained `hyperbytedb_rate_limit_denied_total` growth | Aggregate client load exceeds budget | Raise limit, add nodes/proxy, or throttle upstream |
| 429 even with limit disabled | Misconfiguration (`enabled = true` but `max_requests_per_second = 0` behaves as unlimited — check another layer) | Verify `config.toml`, env vars, and operator spec |
| Writes limited but queries fine | Expected — separate buckets | Tune each endpoint's workload independently (same config value applies to both) |

Start conservative (for example `50–100` req/s per endpoint) and increase based on `hyperbytedb_rate_limit_denied_total` and client error rates.

---

## See Also

- [Configuration](configuration.md) — Full `[rate_limit]` reference
- [Authentication](authentication.md) — Runs after rate limiting on `/write` and `/query`
- [Administration](administration.md) — Metrics and operations
- [Troubleshooting](troubleshooting.md) — HTTP 429 diagnostics
