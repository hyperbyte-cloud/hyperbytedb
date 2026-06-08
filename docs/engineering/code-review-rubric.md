# Code review rubric

Checklist for reviewing changes in this repository. Every gate needs cited evidence: a green CI job, a test name, or a code trace.

---

## Tool gates

These match [.github/workflows/ci.yml](../../.github/workflows/ci.yml).

| Gate | Command | When required |
|------|---------|---------------|
| Format | `cargo fmt --all --check` | Every Rust PR |
| Lint | `cargo clippy --all-targets -- -D warnings` | Every Rust PR |
| Unit tests | `cargo test --lib` | Every Rust PR |
| Integration tests | `cargo test --test '*'` | Every Rust PR |
| Feature tests | `cargo test --all-features` | When touching `columnar-ingest` or other feature-gated paths |
| Release build | `cargo build --release` | Every Rust PR |
| Container build | [.github/workflows/container.yml](../../.github/workflows/container.yml) | When changing `Dockerfile` or image entrypoints |

---

## Correctness

### Rust fundamentals

- No new `unwrap`/`expect` on fallible paths in production code unless justified (startup invariant).
- Errors propagate with `?` or explicit mapping; library boundaries use typed errors in [`src/error.rs`](../../src/error.rs).
- No `Mutex`/`RwLock` held across `.await` without review.
- Public API changes are intentional.

### Data path

Scope: WAL → metadata → flush → chDB MergeTree.

- WAL ordering and sequence semantics are preserved; flush does not drop acknowledged writes.
- Replication apply is idempotent where the protocol requires it.
- Storage layout matches [`src/domain/storage_layout.rs`](../../src/domain/storage_layout.rs).

### Cluster

Scope: Raft, peer sync, hinted handoff, drain, membership.

- Network failures use configured timeouts, retries, and backoff.
- Raft changes preserve safety (no committed log loss).

Evidence: `tests/raft_integration.rs`, `tests/sync_quorum_integration.rs` where applicable.

### HTTP / InfluxDB v1 compatibility

- Status codes and bodies match [API reference](../user-guide/reference.md#influxdb-v1-compatibility-matrix) for touched endpoints.
- Auth paths stay consistent with [`src/adapters/http/auth_middleware.rs`](../../src/adapters/http/auth_middleware.rs).

### Query translation

- Identifiers and literals are quoted/escaped; user input cannot break SQL structure.
- Query timeouts and limits remain enforced for untrusted clients.

### Security and observability

- Secrets are not logged.
- Password hashing uses Argon2 ([`src/adapters/auth.rs`](../../src/adapters/auth.rs)).
- Prometheus metrics do not add unbounded cardinality labels.

---

## Maintainability

- `ports` traits remain the boundary between application and adapters.
- Domain types do not depend on HTTP or RocksDB directly.
- New or renamed config keys are documented in [`docs/user-guide/configuration.md`](../user-guide/configuration.md) and [`config.toml.example`](../../config.toml.example).

---

## Test ownership

Avoid duplicating coverage across suites.

| Suite | Owns |
|-------|------|
| `cargo test --lib` | Parsers, domain logic, WAL batching, Raft log store hydration |
| `tests/compat/` | InfluxDB v1 HTTP, DDL, query, and error compatibility |
| `tests/integration.rs` | Auth, cardinality, users, metrics, backup manifests, chDB layout |
| `tests/raft_integration.rs` | Multi-node cluster, membership, replication log |
| `tests/sync_quorum_integration.rs` | `sync_quorum` replication mode and ack semantics |
| `scripts/load.sh` + `load.js` / `query.js` | Load and ad-hoc performance smoke (kind setup) |

Before deleting a test, confirm another suite covers the same behavior.

---

## Removing code

Allowed when you can show:

1. **Unused dependencies** — `cargo machete` clean + `cargo check --all-targets` + relevant tests pass.
2. **Dead code** — not part of stable `pub` API; no remaining callers; tests pass.
3. **Duplicate docs** — consolidate into one canonical page and update links.
4. **Obsolete scripts** — no references in CI, README, or `deploy/`.

Breaking removals (HTTP shape, config keys, features) need an explicit note in the PR description.

---

## Coding standards

Follow [Coding standards](../developer-guide/coding-standards.md). The project uses conventions from [rust-skills](https://github.com/leonardomso/rust-skills), summarized in [`.cursorrules`](../../.cursorrules) for editor hints.
