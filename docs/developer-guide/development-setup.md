# Development Setup

Set up a local environment for building, running, and debugging HyperbyteDB. For production deployment paths (Docker, Compose, Kubernetes operator), see [Installation](../user-guide/installation.md).

## Prerequisites

| Requirement | Details |
|-------------|---------|
| **Rust** | Latest stable toolchain (pinned in `rust-toolchain.toml`) |
| **libchdb** | Embedded ClickHouse library |
| **System packages** | `clang`, `llvm-dev`, `libclang-dev`, `pkg-config`, `libssl-dev` |
| **Docker** | Optional; required for Compose and kind integration tests |
| **Platforms** | Linux x86_64, Linux aarch64, macOS x86_64, macOS arm64 |

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
# Debian/Ubuntu (amd64 and arm64)
sudo apt-get update && sudo apt-get install -y --no-install-recommends \
  clang llvm-dev libclang-dev pkg-config libssl-dev build-essential

# Fedora/RHEL
sudo dnf install -y clang llvm-devel clang-devel pkgconfig openssl-devel

# macOS (Homebrew) — Apple Silicon and Intel
brew install llvm pkg-config openssl@3
export LIBCLANG_PATH="$(brew --prefix llvm)/lib"
```

### libchdb

```bash
curl -sL https://lib.chdb.io | bash
sudo ldconfig 2>/dev/null || true
```

The installer auto-detects your platform (Linux x86_64/aarch64, macOS x86_64/arm64) and places `libchdb.so` in `/usr/local/lib/` and `chdb.h` in `/usr/local/include/`.

> **macOS note:** the chdb installer writes `libchdb.so` even on macOS (not `.dylib`). The Rust crate links against it by name — leave the file as-is. If the binary fails at runtime with `libchdb.so: cannot open shared object file`, ensure `/usr/local/lib` is on `DYLD_LIBRARY_PATH`.

Verify:

```bash
ls /usr/local/lib/libchdb.so
ls /usr/local/include/chdb.h
```

If the build links but the binary cannot load `libchdb.so` at runtime on Linux, add `/usr/local/lib` to the linker path and run `ldconfig` again. See [Troubleshooting](../user-guide/troubleshooting.md).

## Building

```bash
# Debug (faster compile)
cargo build

# Release (production-like)
cargo build --release

# Server only
cargo build --release -p hyperbytedb

# CLI client
cargo build --release -p hyperbytedb-cli

# Columnar MessagePack ingest is enabled by default; disable with:
cargo build --no-default-features
```

Debug builds optimize dependency crates at level 2 via `[profile.dev.package."*"]`, which keeps chDB and RocksDB usable during development.

The release profile uses LTO, single codegen unit, and strip for maximum performance and minimum binary size.

## Running

```bash
# Debug with default config
cargo run -- serve

# Custom config
cargo run -- -c /path/to/config.toml serve

# Release binary
./target/release/hyperbytedb serve
```

### Server CLI commands

| Command | Description |
|---------|-------------|
| `hyperbytedb serve` | Start the HTTP server |
| `hyperbytedb backup --output <path>` | Create a full backup |
| `hyperbytedb restore --input <path>` | Restore from a backup |

| Flag | Default | Description |
|------|---------|-------------|
| `-c`, `--config` | `config.toml` | Path to TOML config file |

Quick smoke test:

```bash
curl -s http://localhost:8086/health
curl -sS -XPOST 'http://localhost:8086/query' --data-urlencode 'q=CREATE DATABASE dev'
curl -sS -XPOST 'http://localhost:8086/write?db=dev' --data-binary 'test,env=dev value=42'
# Wait ~10s for flush, then:
curl -sS -G 'http://localhost:8086/query' \
  --data-urlencode 'db=dev' --data-urlencode 'q=SELECT * FROM test'
```

## Docker Compose (local build)

The root `docker-compose.yml` **builds HyperbyteDB from source** and starts the full observability stack (Alloy log shipping, Tempo tracing, etc.):

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

For a pre-built image stack without compiling, use the [getting-started Compose file](../user-guide/installation.md#docker-compose) instead.

## kind (local Kubernetes)

The `deploy/kind/` directory contains a setup script and manifests for a local Kubernetes cluster:

```bash
./deploy/kind/setup.sh
```

This creates a kind cluster with:

- HyperbyteDB operator and a multi-node `HyperbytedbCluster` CR
- Observability stack (Prometheus, Grafana, Loki, Telegraf)
- NodePort services mapped to localhost ports

Use this to develop and test operator changes without a cloud cluster. Production Kubernetes deployment is documented in [Operator installation](../user-guide/operator/installation.md).

## Project layout

```
hyperbytedb/
├── Cargo.toml              # Dependencies, features, profiles
├── config.toml.example     # Example server configuration
├── Dockerfile              # Multi-stage image build
├── docker-compose.yml      # HyperbyteDB + observability stack (local build)
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

- [Installation](../user-guide/installation.md) — Customer deployment paths
- [Architecture](architecture.md)
- [Testing](testing.md)
- [Building & CI](building-and-ci.md)
