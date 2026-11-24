# DST seams (`dst` branch)

The `dst` branch builds HyperbyteDB inside the deterministic simulation
testing rig (`hyperbytedb-sim`) with `RUSTFLAGS="--cfg madsim"`. Every change
the rig needs in this repo is a `#[cfg(madsim)]` environment seam or a
zero-cost trait seam; without `--cfg madsim` the build is the production
build. Replication, sharding, flush, Raft and startup logic are never forked
for the sim.

One line per seam: file — what — why.

- `hyperbytedb/Cargo.toml` — `tokio` is `madsim-tokio` (renamed to `tokio`) — re-exports real tokio normally; under `cfg(madsim)` swaps in madsim's deterministic runtime, time and network.
- `hyperbytedb/Cargo.toml` — `tikv-jemallocator` moved to `[target.'cfg(not(madsim))'.dependencies]` — jemalloc's background purge threads are a nondeterminism source; the sim uses the system allocator.
- `hyperbytedb/Cargo.toml` — `[lints.rust] unexpected_cfgs` declares `cfg(madsim)` — keeps `-D warnings` clean for the new cfg.
- `hyperbytedb/src/main.rs` — `#[global_allocator]` jemalloc only under `not(madsim)` — pairs with the dependency gate above.
- `hyperbytedb/src/lib.rs` — `#![cfg_attr(madsim, allow(deprecated))]` — madsim-tokio deprecates `spawn_blocking` (it runs the closure as a simulated task); silences ~40 warnings at RocksDB/chDB call sites.
- `hyperbytedb/src/application/runtime/control.rs` — `install()`/`shutdown()` are no-ops under `cfg(madsim)`; `RUNTIME`/`CONTROL_WORKERS` gated out — no second runtime can exist in the sim, so control work runs on the caller's runtime (the documented no-runtime path).
- `hyperbytedb/src/application/runtime/control.rs` — `spawn_on` re-panics with `panic!` under `cfg(madsim)` — madsim's `JoinError` has no `into_panic`.
- `hyperbytedb/src/adapters/cluster/peer_client.rs` — `wait_for_inflight` uses `timeout(deadline - now)` under `cfg(madsim)` — madsim-tokio has no `time::timeout_at`.
- `hyperbytedb/src/server.rs` — control listener not bound under `cfg(madsim)` (and `build_control_router` import gated) — the sim has no TCP listeners; the rig serves routers over the simulated network.
- `hyperbytedb/src/server.rs` — API listener phase (TLS/plain `axum::serve` + SIGTERM handling) replaced under `cfg(madsim)` by "wait for the node's ctrl-c, then signal API shutdown"; `nodelay` gated out — no TCP listeners or unix signals in the sim; shutdown sequence after the API phase is unchanged.
- `Cargo.lock` — only additions over `main`: `madsim`, `madsim-tokio`, `madsim-macros` and their transitive deps, plus `hyperbytedb`'s `tokio` edge -> `madsim-tokio` — resolved against chdb-rust `origin/main` like `main`'s lock; local working-copy lock drift from a stale `../chdb-rust` checkout (e.g. reqwest 0.11 / hyper 0.14 / openssl appearing) must never be committed.
