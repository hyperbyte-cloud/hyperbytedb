# Developer Guide

Technical documentation for contributors and engineers extending HyperbyteDB.

## Contents

| # | Topic | Description |
|---|-------|-------------|
| 1 | [Architecture](architecture.md) | Hexagonal design, components, data flow |
| 2 | [System architecture](system-architecture.md) | Module layout, storage, services |
| 3 | [Development setup](development-setup.md) | Prerequisites, build, run, Compose, kind |
| 4 | **Internals** | |
| | [Core modules](internals/core-modules.md) | Source tree guide |
| | [Key design decisions](internals/key-design-decisions.md) | Write path, read path, compaction, replication |
| | [Replication design](internals/replication-design.md) | Wire protocol, sync quorum, hinted handoff |
| | [Extension points](internals/extension-points.md) | Adding statements, services, endpoints |
| 5 | [Coding standards](coding-standards.md) | Errors, async, naming, metrics |
| 6 | [Testing](testing.md) | Suites, running tests, CI |
| 7 | [Building & CI](building-and-ci.md) | Pipeline, Docker, releases |
| 8 | [Contributing](contributing.md) | PR process and review checklist |

## Quick reference

| Task | Command |
|------|---------|
| Build (debug) | `cargo build` |
| Build (release) | `cargo build --release` |
| Run server | `cargo run -- serve` |
| All tests | `cargo test` |
| Unit tests | `cargo test --lib` |
| Integration tests | `cargo test --test '*'` |
| Format | `cargo fmt --all --check` |
| Lint | `cargo clippy --all-targets -- -D warnings` |
| All benchmarks | `cargo bench` or `./scripts/bench-all.sh` |
| Full stack | `docker compose up --build -d` |
| kind cluster | `./deploy/kind/setup.sh` |
