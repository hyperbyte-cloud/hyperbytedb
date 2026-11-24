//! Hyperbytedb library: config, domain model, HTTP/query adapters, ingestion, WAL, metadata, and cluster/raft.
#![cfg_attr(not(test), warn(clippy::unwrap_used, clippy::expect_used))]
// Simulation (`cfg(madsim)`): madsim-tokio marks `spawn_blocking` deprecated
// ("blocking function is not allowed in simulation"); it still runs the
// closure as a simulated task, which is what the sim wants for RocksDB calls.
#![cfg_attr(madsim, allow(deprecated))]

pub mod adapters;
pub mod application;
pub mod bootstrap;
pub mod config;
pub mod domain;
pub mod error;
pub mod ports;
pub mod server;
pub mod telemetry;
pub mod timeseriesql;
