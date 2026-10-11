# Authentication

HyperbyteDB can require credentials on data-plane HTTP methods while leaving health and monitoring endpoints open (by default). Passwords are verified with **Argon2** and stored in metadata as **PHC-format** strings.

**Config:** set `[auth] enabled = true` or `HYPERBYTEDB__AUTH__ENABLED=true`. See the [`[auth]`](configuration.md#auth) section in [Configuration](configuration.md).

---

## What requires a login (any valid user)

When authentication is **enabled**, these routes require a successful check using one of the [methods below](#sending-credentials):

| Route | Notes |
|-------|--------|
| `POST /write` | Line protocol, MessagePack, or columnar bodies |
| `GET` / `POST /query` | TimeseriesQL |

These layers run after optional **[rate limiting](rate-limiting.md)** (if `[rate_limit]` is enabled): a client may get **429** before auth runs.

---

## Public endpoints (no credentials)

The following are **not** wrapped in the user auth middleware when `auth.enabled` is `true`:

| Route | Purpose |
|-------|--------|
| `GET` / `HEAD` `/ping` | Liveness / version headers |
| `GET` / `HEAD` `/health` | JSON health |
| `GET` / `HEAD` `/health/ready` | Readiness (chDB probe) |
| `GET` `/metrics` | Prometheus text |
| `GET` / `DELETE` `/api/v1/statements` | Only when `statement_summary.require_auth = false` (default: **requires auth**) |

When `statement_summary.require_auth` is `true` (the default) and `[auth] enabled = true`, statement summary requires the same credentials as `/query`.

---

## Cluster / internal / Raft routes (admin only)

When **cluster** (or Raft) routes are present and **`auth.enabled` is true**, the stack uses a **separate** middleware: `internal_auth_layer` in [`auth_middleware.rs`](../../src/adapters/http/auth_middleware.rs).

- Valid credentials for a user with **`admin: true`** → request proceeds.
- Valid non-admin user → **403** `admin privileges required for internal routes`.
- Missing/invalid credentials → **401** `authorization failed`.
- If **`auth.enabled` is false** → internal routes are **not** checked at the HTTP layer (rely on network policy / mTLS in production).

Uses the same credential delivery as [Sending credentials](#sending-credentials) (`u`/`p`, `Authorization: Basic`, or `Authorization: Token`).

Affects paths such as `/internal/*`, `/cluster/*` (including replication and Raft) when those routes are mounted; exact set depends on cluster/Raft being enabled. See [Cluster, replication, and internal routes](reference.md#cluster-replication-and-internal-routes) in the API reference.

**Operational note:** create at least one **admin** user (below) before turning on auth in a clustered deployment if operators or automation must call internal APIs.

---

## Sending credentials (order of precedence)

The server examines sources in this order; the first successful extraction wins.

1. **Query parameters:** `u` and `p` (e.g. `?u=admin&p=secretpassword` on `GET /query` or `POST /write`).
2. **HTTP Basic:** `Authorization: Basic <base64("username:password")>`.
3. **InfluxDB-style token header:** `Authorization: Token username:password` (literal `Token ` prefix, then `user:pass` with a single colon separator).

**Implementation:** `extract_credentials` in [`auth_middleware.rs`](../../src/adapters/http/auth_middleware.rs) (Custom Base64 decode for Basic, no extra crate).

---

## Enabling auth in config

```toml
[auth]
enabled = true
admin_username = "admin"   # created at startup if missing
admin_password = "..."     # use HYPERBYTEDB__AUTH__ADMIN_PASSWORD from a Secret
cluster_token = "..."      # clusters only; HYPERBYTEDB__AUTH__CLUSTER_TOKEN
```

Each node creates `admin_username` as an admin at startup if no user has that
name, so the first `CREATE USER` can run with auth already on. It never
changes an existing user, so a later `SET PASSWORD` survives restarts.

**Clusters need `cluster_token`.** Nodes call each other's internal routes
(Raft, replication, forwarded writes and queries) and carry no user; they
send the token in the `x-hyperbytedb-cluster-token` header instead. Every
node must have the same token. A wrong token is refused even alongside an
admin login; the token does not work on `/write` or `/query`. Without it a
node logs a warning at startup and its peers' internal calls fail with 401.

On Kubernetes the operator does all of this when `spec.auth.enabled` is true:
it keeps the admin login and the token in a Secret and passes them to the
pods (see [operator: cluster](operator/cluster.md)).

---

## User management (TimeseriesQL)

Run through `/query` (or `POST` with form `q=...`).

| Action | Example |
|--------|--------|
| Create user (non-admin) | `CREATE USER "reader" WITH PASSWORD 'secret'` |
| Create **admin** | `CREATE USER "admin" WITH PASSWORD 'secret' WITH ALL PRIVILEGES` (or include `ADMIN` in the statement—see parser) |
| List users | `SHOW USERS` |
| Change password | `SET PASSWORD FOR "reader" = 'newsecret'` |
| Remove user | `DROP USER "reader"` |
| Grant on a database | `GRANT READ ON "db" TO "reader"`, `GRANT WRITE ON "db" TO "telegraf"`, `GRANT ALL ON "db" TO "app"` |
| Revoke | `REVOKE READ ON "db" FROM "app"` (ALL becomes WRITE), `REVOKE ALL ON "db" FROM "app"` |
| Show a user's grants | `SHOW GRANTS FOR "app"` (admin only) |

**Admin flag:** the parser sets `admin` if the `CREATE USER` text contains `ALL PRIVILEGES` or `ADMIN` (case-insensitive). Only admin users can access **internal** cluster routes when auth is on.

**Authorization model:** per-database `GRANT READ | WRITE | ALL ON <db> TO <user>` control query (READ) and write (WRITE) access for non-admin users; ALL is both. As in InfluxDB v1, a `GRANT` sets the privilege (GRANT WRITE on a READ user leaves WRITE only), and `REVOKE READ` or `REVOKE WRITE` clears just that one; `REVOKE ALL ON <db>` clears both. Non-admin users without a grant on a database cannot read or write it. Admin users bypass per-database checks. `GRANT ALL PRIVILEGES TO <user>` (no `ON` clause) promotes an existing user to admin; `REVOKE ALL PRIVILEGES FROM <user>` removes admin.

---

## Design reference

- Password hashing: `hash_password` (Argon2) in `auth_middleware.rs`; verification and short TTL cache in [`adapters/auth.rs`](../../src/adapters/auth.rs) (`MetadataAuthAdapter`).
- [Key design decisions: Authentication](../developer-guide/internals/key-design-decisions.md#authentication) (developer guide) — Argon2id, cache, extraction order.

---

## See also

- [Configuration](configuration.md) — `[auth]`
- [API reference](reference.md) — status codes, HTTP params
- [Advanced features](advanced-features.md) — TLS, clustering (same document family)
