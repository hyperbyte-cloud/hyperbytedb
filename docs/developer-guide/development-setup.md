# Development Setup

Set up a local environment for building, running, and debugging HyperbyteDB.

## Prerequisites

| Requirement | Details |
|-------------|---------|
| **Rust** | Latest stable toolchain (pinned in `rust-toolchain.toml`) |
| **libchdb** | Embedded ClickHouse library |
| **System packages** | `clang`, `llvm-dev`, `libclang-dev`, `pkg-config`, `libssl-dev` |
| **Docker** | Optional; required for Compose and kind integration tests |

Install everything in one step:

```bash
sh scripts/install-dev-deps.sh
```

Or install manually:

### Rust

```bash
curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh -s -- -y
source ~/.cargo/env
rustup update stable
```

### System dependencies

```bash
# Debian/Ubuntu
sudo apt-get update && sudo apt-get install -y --no-install-recommends \
  clang llvm-dev libclang-dev pkg-config libssl-dev build-essential

# Fedora/RHEL
sudo dnf install -y clang llvm-devel clang-devel pkgconfig openssl-devel
```

### libchdb

```bash
curl -sL https://lib.chdb.io | bash
sudo ldconfig
```

Verify:

```bash
ls /usr/local/lib/libchdb.so
ls /usr/local/include/chdb.h
```

## Building

```bash
# Debug (faster compile)
cargo build

# Release (production-like)
cargo build --release

# Columnar MessagePack ingest is enabled by default; disable with:
cargo build --no-default-features
```

Debug builds optimize dependency crates at level 2 via `[profile.dev.package."*"]`, which keeps chDB and RocksDB usable during development.

## Running

```bash
# Debug with default config
cargo run -- serve

# Custom config
cargo run -- -c /path/to/config.toml serve

# Release binary
./target/release/hyperbytedb serve
```

Quick smoke test:

```bash
curl -s http://localhost:8086/health
curl -sS -XPOST 'http://localhost:8086/query' --data-urlencode 'q=CREATE DATABASE dev'
curl -sS -XPOST 'http://localhost:8086/write?db=dev' --data-binary 'test,env=dev value=42'
# Wait ~10s for flush, then:
curl -sS -G 'http://localhost:8086/query' \
  --data-urlencode 'db=dev' --data-urlencode 'q=SELECT * FROM test'
```

### Docker Compose (full stack)

```bash
docker compose up --build -d
```

| Service | URL |
|---------|-----|
| HyperbyteDB | http://localhost:8086 |
| Grafana | http://localhost:3000 (`admin` / `admin`) |
| Prometheus | http://localhost:9090 |
| Loki | http://localhost:3100/ready |
| Tempo | http://localhost:3200/ready |

```bash
docker compose logs -f hyperbytedb
```

### kind (Kubernetes)

```bash
./deploy/kind/setup.sh
```

Creates a local cluster with the operator, a multi-node `HyperbytedbCluster`, and the same observability stack as Compose.

## Project layout

```
hyperbytedb/
├── Cargo.toml              # Dependencies, features, profiles
├── config.toml.example     # Example server configuration
├── Dockerfile              # Multi-stage image build
├── docker-compose.yml      # HyperbyteDB + observability stack
├── deploy/
│   ├── compose/            # Prometheus, Grafana, Loki, Tempo, Alloy, Telegraf
│   └── kind/               # Local Kubernetes dev cluster
├── src/
│   ├── main.rs             # CLI entry point
│   ├── bootstrap.rs        # Composition root
│   ├── domain/             # Domain types
│   ├── ports/              # Trait definitions
│   ├── adapters/           # RocksDB, chDB, HTTP, cluster
│   ├── application/        # Business logic services
│   └── timeseriesql/       # TimeseriesQL parser and translator
├── tests/
│   ├── integration.rs      # Auth, metrics, backup, layout
│   ├── raft_integration.rs # Multi-node cluster
│   ├── sync_quorum_integration.rs
│   └── compat/             # InfluxDB v1 compatibility
├── scripts/                # load.sh, install-dev-deps.sh, benchmarks
└── docs/                   # Documentation (MkDocs)
```

## Environment variables

Override any config setting:

```bash
HYPERBYTEDB__LOGGING__LEVEL=debug cargo run -- serve
```

Fine-grained tracing filters:

```bash
RUST_LOG=hyperbytedb=debug,tower_http=debug cargo run -- serve
```

## Common commands

| Task | Command |
|------|---------|
| Check compilation | `cargo check` |
| Format | `cargo fmt` |
| Lint | `cargo clippy --all-targets -- -D warnings` |
| Unit tests | `cargo test --lib` |
| Integration tests | `cargo test --test '*'` |
| All tests | `cargo test` |
| All benchmarks | `cargo bench` or `./scripts/bench-all.sh` |
| API docs | `cargo doc --open` |

Build the published docs site locally:

```bash
pip install -r docs/requirements.txt
mkdocs serve
```

## See also

- [Architecture](architecture.md)
- [Testing](testing.md)
- [Building & CI](building-and-ci.md)
