# Building & CI

Build system, CI pipeline, Docker image builds, and release process.

---

## Build System

HyperbyteDB uses Cargo as its sole build system. There is no `build.rs`, `Makefile`, or custom build script.

### Profiles

| Profile | Settings | Use case |
|---------|----------|----------|
| `dev` | Default Cargo dev settings; dependency crates at `opt-level = 2` | Local development |
| `release` | `opt-level = 3`, `lto = "fat"`, `codegen-units = 1`, `panic = "abort"`, `strip = true` | Production binaries |
| `bench` | Inherits `release` but with `debug = true`, `strip = false` | Benchmarking with profiling |

### Binary targets

| Name | Source | Description |
|------|--------|-------------|
| `hyperbytedb` | `hyperbytedb/src/main.rs` (implicit) | Main database server |
| `hyperbytedb-cli` | `hyperbytedb-cli/src/main.rs` | Interactive CLI client (REPL, query, write, import) |

### Features

| Feature | Default | Description |
|---------|---------|-------------|
| `columnar-ingest` | No | Columnar MessagePack ingest support |

### Build commands

```bash
cargo build                              # Debug build (all binaries)
cargo build --release                    # Release build (all workspace binaries)
cargo build --release --bin hyperbytedb     # Release build, main binary only
cargo build --release -p hyperbytedb-cli   # CLI only
cargo build --no-default-features        # Without columnar ingest
```

---

## CI Pipeline

### `ci.yml` — Rust CI

Triggers on `push` and `pull_request` to `main`. Uses concurrency groups with cancel-in-progress.

**Jobs (in dependency order):**

1. **Format** — `cargo fmt --all --check`
2. **Clippy** (depends on Format) — `cargo clippy --all-targets -- -D warnings`
3. **Test** (depends on Format) — `cargo test --lib` + `cargo test --test '*'` + `cargo test -p hyperbytedb-cli`
4. **Build** (depends on Clippy + Test) — `cargo build --release` for `hyperbytedb` and `hyperbytedb-cli`

All jobs with Rust compilation install:
- System packages: `clang`, `llvm-dev`, `libclang-dev`, `pkg-config`, `libssl-dev`
- libchdb: `curl -sL https://lib.chdb.io | bash`
- Rust cache: `Swatinem/rust-cache@v2`

**Environment:** `RUSTFLAGS="-D warnings"`, `CARGO_INCREMENTAL=0`.

### `container.yml` — Docker Image

Triggers on `push` to `main`, version tags (`v*`), and pull requests.

**Jobs:**

1. **Build & Publish**
   - Sets up Docker Buildx.
   - Logs in to GHCR (skipped on PRs).
   - Extracts tags: branch, semver, SHA.
   - Builds with `docker/build-push-action@v6` + GHA cache.
   - Pushes to `ghcr.io` on `main` and version tags; build-only on PRs.

---

## Docker Build

### Dockerfile (multi-stage)

**Builder stage** (Debian bookworm):
1. Install Rust via rustup + system build dependencies.
2. Install libchdb.
3. Copy `Cargo.toml` + `Cargo.lock`, create stub files for bench entries, run `cargo fetch`.
4. Copy `src/` and `tests/`, build release binaries with BuildKit cache mounts.
5. Copy binaries to `/artifacts/`.

**Runtime stage** (Debian bookworm-slim):
1. Install runtime dependencies: `ca-certificates`, `libstdc++6`, `curl`.
2. Copy `libchdb.so` and `chdb.h` from builder.
3. Copy the `hyperbytedb` and `hyperbytedb-cli` binaries and `libchdb.so`.
4. Create data directories under `/var/lib/hyperbytedb/`.
5. Set environment defaults for data paths.
6. `ENTRYPOINT ["hyperbytedb"]`, `CMD ["serve"]`.

### Building locally

```bash
docker build -t hyperbytedb:dev .
docker run -d -p 8086:8086 hyperbytedb:dev
```

### Optimizations

- **Layer caching:** Cargo.toml/lock copied first so dependency resolution is cached until manifest changes.
- **BuildKit cache mounts:** `~/.cargo/registry`, `~/.cargo/git`, and `target/` persist across builds.
- **Incremental disabled:** `CARGO_INCREMENTAL=0` in Docker to avoid storing incremental artifacts in the image.

---

## Release Process

1. Ensure CI is green on `main`.
2. Update `version` in `Cargo.toml`.
3. Add a dated entry to [`CHANGELOG.md`](../../CHANGELOG.md) under the new version (Keep a Changelog format: Added, Changed, Fixed, Security).
4. Tag the commit: `git tag vX.Y.Z` (must match `version` in `Cargo.toml`).
5. Push the tag: `git push origin vX.Y.Z`.
6. The release workflow (`release.yml`) builds and pushes the multi-arch Docker image, packages `hyperbytedb` + `hyperbytedb-cli` + `libchdb.so` tarballs per platform, and publishes a GitHub Release.

---

## Supply Chain

- **Rust dependencies:** `Cargo.lock` is committed for reproducible builds.
- **Go dependencies:** `go.sum` committed under `hyperbytedb-operator/`.
- **GitHub Actions:** Pinned to major versions (`@v4`, `@v3`, etc.).

---

## See Also

- [Development Setup](development-setup.md) — Local build instructions
- [Testing](testing.md) — Test suites and CI test matrix
- [Contributing](contributing.md) — PR process
