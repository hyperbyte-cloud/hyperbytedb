//! Composition: builds the concrete adapters and wires them into the
//! application. Outside the hexagon's layers, like `bootstrap.rs` and
//! `server.rs`, so it may name any of them.

pub mod bootstrap;
pub mod leader_monitor;
pub mod raft_formation;
