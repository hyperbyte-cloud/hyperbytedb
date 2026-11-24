// Real chDB, TCP listeners or reqwest: not built under `cfg(madsim)`;
// simulation tests live in `madsim_*` test crates.
#![cfg(not(madsim))]

pub mod recording_ports;
pub mod sharding_cluster;
