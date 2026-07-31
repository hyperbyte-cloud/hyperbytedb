# hyperbytedb-proxy

`hyperbytedb-proxy` is a **health-aware HTTP reverse proxy** for HyperbyteDB. It sits between clients (Grafana, Telegraf, anything that speaks the InfluxDB v1 HTTP API) and the database pods—typically a Kubernetes headless Service that resolves to one A record per StatefulSet replica.

**Design goals:** round-robin across **healthy** backends only, **hold-and-wait** when no backend is temporarily routable (rolling restarts), **retry** transient failures on another backend, and **graceful shutdown** on SIGTERM. Configuration is **environment variables only** (no TOML) so the proxy fits cleanly into a plain Deployment manifest.

**Implementation:** [`hyperbytedb-proxy/src/`](../../../hyperbytedb-proxy/src/) (crate `hyperbytedb-proxy` in this workspace).

---

## Build and run

From the repository root:

```bash
cargo build --release -p hyperbytedb-proxy
./target/release/hyperbytedb-proxy
```

A multi-stage [`Dockerfile`](../../../hyperbytedb-proxy/Dockerfile) is provided under `hyperbytedb-proxy/`; build context should include the workspace `rust-toolchain.toml` as in that file.

---

## Architecture (summary)

1. **DNS discovery** — Periodically resolves `HYPERBYTEDB_PROXY_BACKEND_SERVICE` (e.g. `mydb-headless.myns.svc.cluster.local`) and reconciles the backend IP set.
2. **Health probes** — Each known backend is probed on `HYPERBYTEDB_PROXY_HEALTH_PATH` (default `/health`) on a fixed interval. Responses are mapped to `Active`, `Draining`, or `Down` (see below).
3. **Routing** — New requests use **round-robin** among backends in `Active` only.
4. **No active backend** — The proxy **waits** up to `HYPERBYTEDB_PROXY_HOLD_TIMEOUT_SECS` for an `Active` backend (wakes early when a backend becomes active). If the hold elapses, the client receives **503**.
5. **Retries** — On retryable failures (transport error, upstream 502/504, or 503 whose body looks like drain/sync), the proxy tries another backend. The inner loop advances `attempt` until it reaches `max_retries` (see [Retry semantics](#retry-semantics)).
6. **Request body** — The incoming body is buffered once (needed for safe retries). This is bounded by HyperbyteDB’s own `server.max_body_size_bytes` (default 25 MiB).
7. **Shutdown** — SIGTERM/SIGINT triggers graceful drain; in-flight requests may run for up to `HYPERBYTEDB_PROXY_SHUTDOWN_GRACE_SECS` before a watchdog exits the process.

---

## Health mapping (backend)

Probes use a **separate** HTTP client from request forwarding so probe load cannot starve user traffic.

| Upstream | Body (typical) | Proxy state |
|----------|----------------|-------------|
| HTTP 200 | `status: pass` | `Active` — included in round-robin |
| HTTP 503 | `warn` / drain-style JSON | `Draining` — not selected; may trigger client-side retry to another node |
| Timeout / connection error | — | `Down` |
| Before first probe completes | — | `Unknown` — **not** routable |

The forward path treats a **503** whose body matches drain/lifecycle substrings (e.g. `"status":"warn"`, `"Draining"`, `"Syncing"`) as **retryable** on another backend—see `looks_like_drain` in [`proxy.rs`](../../../hyperbytedb-proxy/src/proxy.rs).

---

## Admin endpoints (admin listener)

These run on **`HYPERBYTEDB_PROXY_ADMIN_LISTEN`** (default `0.0.0.0:8087`), separate from client traffic. The client-facing Service should expose only the public port so ingress never routes to admin paths.

| Path | Method | Purpose |
|------|--------|---------|
| `/healthz` | GET | **Liveness** — always 200 once the process is up |
| `/readyz` | GET | **Readiness** — 200 only when **≥1** backend is `Active`; 503 otherwise |
| `/metrics` | GET | Prometheus exposition (if the recorder is installed) |
| `/admin/backends` | GET | JSON snapshot of pool: address, health, inflight, probe stats |
| `/admin/backends/{ip}/exclude` | POST | Operator: stop routing to a backend before pod delete |
| `/admin/backends/{ip}/include` | POST | Operator: resume routing after replacement is healthy |
| `/admin/pool` | GET | Full pool status including exclusion flags |

Configure Kubernetes probes against the **admin** port (`8087` by default), not the public Service port.

## Public listener (client traffic)

Only **`/write`** and **`/query`** are accepted on **`HYPERBYTEDB_PROXY_LISTEN`** (default `0.0.0.0:8086`). Any other path returns **404** — cluster/internal hyperbytedb routes (`/cluster/*`, `/internal/*`, `/ping`, `/metrics`, …) are not reachable through ingress aimed at the proxy Service.

| Path | Methods | Purpose |
|------|---------|---------|
| `/write` | any | Proxied to a healthy backend (InfluxDB v1 write) |
| `/query` | any | Proxied to a healthy backend (InfluxDB v1 query) |

Hop-by-hop headers (e.g. `Connection`, `Transfer-Encoding`) are stripped on forward; see `HOP_BY_HOP` in [`proxy.rs`](../../../hyperbytedb-proxy/src/proxy.rs).

---

## Environment variables

All settings use the `HYPERBYTEDB_PROXY_` prefix. **Required:** backend service DNS name.

| Variable | Default | Description |
|----------|---------|-------------|
| `HYPERBYTEDB_PROXY_LISTEN` | `0.0.0.0:8086` | Public bind: **`/write`** and **`/query`** only |
| `HYPERBYTEDB_PROXY_ADMIN_LISTEN` | `0.0.0.0:8087` | Admin bind: probes, metrics, `/admin/*` (not on client Service) |
| `HYPERBYTEDB_PROXY_BACKEND_SERVICE` | *(required)* | Hostname resolving to backend pod IPs (headless Service) |
| `HYPERBYTEDB_PROXY_BACKEND_PORT` | `8086` | Port on each backend |
| `HYPERBYTEDB_PROXY_DISCOVERY_INTERVAL_SECS` | `5` | DNS refresh / pool reconcile period |
| `HYPERBYTEDB_PROXY_HEALTH_INTERVAL_SECS` | `2` | Time between probe ticks (all backends probed each tick) |
| `HYPERBYTEDB_PROXY_HEALTH_PATH` | `/health` | HTTP path for probes; use `/health/ready` if you need chDB-aware readiness at the DB |
| `HYPERBYTEDB_PROXY_HEALTH_TIMEOUT_MS` | `1500` | Per-probe deadline; slower → `Down` |
| `HYPERBYTEDB_PROXY_REQUEST_TIMEOUT_SECS` | `60` | Upstream round-trip timeout for proxied requests (large queries) |
| `HYPERBYTEDB_PROXY_HOLD_TIMEOUT_SECS` | `30` | Max wait when **no** `Active` backend before 503 to client |
| `HYPERBYTEDB_PROXY_MAX_RETRIES` | `2` | See [Retry semantics](#retry-semantics) |
| `HYPERBYTEDB_PROXY_SHUTDOWN_GRACE_SECS` | `30` | After SIGTERM, max time before forced exit watchdog |
| `HYPERBYTEDB_PROXY_SELF_IP` | *(unset)* | Optional pod IP (Downward API); that IP is **never** added as a backend (prevents accidental self-proxy loops) |
| `HYPERBYTEDB_PROXY_HTTP2_PRIOR_KNOWLEDGE` | `false` | When `true`, upstream `reqwest` uses cleartext HTTP/2 prior knowledge. HyperbyteDB pods use HTTP/1.1 via `axum::serve` by default — leave this `false` unless every backend is h2-capable |

**Logging:** `RUST_LOG` / standard tracing; `LOG_FORMAT=json` enables JSON logs.

**Source of truth:** [`config.rs`](../../../hyperbytedb-proxy/src/config.rs) (`ProxyConfig::from_env`).

### Upstream HTTP version

The proxy forwards to hyperbytedb pods over plain HTTP. HyperbyteDB serves **HTTP/1.1** (`axum::serve` in `runtime/mod.rs`). The upstream client therefore defaults to HTTP/1.1 with ALPN negotiation. Setting `HYPERBYTEDB_PROXY_HTTP2_PRIOR_KNOWLEDGE=true` skips the upgrade and sends an HTTP/2 connection preface immediately — use only when all backends are known to accept cleartext h2; otherwise connections fail at the transport layer and surface as retryable errors.

### Retry semantics

After each **retryable** failure, `attempt` is incremented; the loop continues while `attempt < max_retries`. With the default `max_retries = 2`, a single request can therefore be forwarded up to **three** times (initial try plus two more backends). See the `handle` loop in [`proxy.rs`](../../../hyperbytedb-proxy/src/proxy.rs).

---

## Metrics (Prometheus)

Examples (labels may vary by build):

- `hyperbytedb_proxy_requests_total` — outcomes: `ok`, `fatal`, `exhausted`
- `hyperbytedb_proxy_request_duration_seconds`
- `hyperbytedb_proxy_no_backend_total` — hold window expired with no `Active` backend

---

## Relationship to HyperbyteDB

- Clients use **InfluxDB v1** URLs on the public listener: **`/write`** and **`/query`** only (path and query string forwarded unchanged).
- HyperbyteDB configuration (`HYPERBYTEDB__…`) applies to **database pods**, not to the proxy.
- Cluster/admin routes on database pods (`/cluster/*`, `/internal/*`, …) are **not** exposed through the proxy Service; reach them via headless Service or port-forward when needed.
- For **TLS** termination at the proxy, terminate TLS on the proxy’s Service and use `http` to backends, or extend the proxy to support outgoing TLS if needed (not in the default crate).

---

## See also

- [Kubernetes Operator](index.md) — cluster installation
- [HyperbytedbCluster](cluster.md) — CRD and Services
- [Deep dive: Clustering](../../deep-dive/deep-dive-clustering.md) — how the database nodes interact
