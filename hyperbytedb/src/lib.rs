//! Hyperbytedb library: config, domain model, HTTP/query adapters, ingestion, WAL, metadata, and cluster/raft.
#![cfg_attr(not(test), warn(clippy::unwrap_used, clippy::expect_used))]

pub mod adapters;
pub mod application;
pub mod bootstrap;
pub mod config;
pub mod domain;
pub mod error;
pub mod ports;
pub mod timeseriesql;
