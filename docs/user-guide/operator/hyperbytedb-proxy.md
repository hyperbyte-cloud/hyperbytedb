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

## Admin endpoints (proxy process)

These are served on the **same listen address** as client traffic, registered **before** the catch-all proxy so they never get forwarded upstream:

| Path | Method | Purpose |
|------|--------|---------|
| `/healthz` | GET | **Liveness** — always 200 once the process is up |
| `/readyz` | GET | **Readiness** — 200 only when **≥1** backend is `Active`; 503 otherwise |
| `/metrics` | GET | Prometheus exposition (if the recorder is installed) |
| `/admin/backends` | GET | JSON snapshot of pool: address, health, inflight, probe stats |

Configure Kubernetes probes to use **`/healthz`** for liveness and **`/readyz`** for readiness so the proxy is not marked ready until at least one HyperbyteDB pod is healthy.

Hop-by-hop headers (e.g. `Connection`, `Transfer-Encoding`) are stripped on forward; see `HOP_BY_HOP` in [`proxy.rs`](../../../hyperbytedb-proxy/src/proxy.rs).

---

## Environment variables

All settings use the `HYPERBYTEDB_PROXY_` prefix. **Required:** backend service DNS name.

| Variable | Default | Description |
|----------|---------|-------------|
| `HYPERBYTEDB_PROXY_LISTEN` | `0.0.0.0:8086` | Bind address for client + admin traffic |
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

**Logging:** `RUST_LOG` / standard tracing; `LOG_FORMAT=json` enables JSON logs.

**Source of truth:** [`config.rs`](../../../hyperbytedb-proxy/src/config.rs) (`ProxyConfig::from_env`).

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

- Clients keep using **InfluxDB v1** URLs (`/write`, `/query`, `/ping`, …); the proxy forwards them **unchanged** in path and query string.
- HyperbyteDB configuration (`HYPERBYTEDB__…`) applies to **database pods**, not to the proxy.
- For **TLS** termination at the proxy, terminate TLS on the proxy’s Service and use `http` to backends, or extend the proxy to support outgoing TLS if needed (not in the default crate).

---

## See also

- [Kubernetes Operator](index.md) — cluster installation
- [HyperbytedbCluster](cluster.md) — CRD and Services
- [Deep dive: Clustering](../../deep-dive/deep-dive-clustering.md) — how the database nodes interact
