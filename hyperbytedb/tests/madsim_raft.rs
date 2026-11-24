//! openraft on madsim's deterministic runtime (simulation rig only).
//!
//! Run with `RUSTFLAGS="--cfg madsim" cargo test --test madsim_raft
//! --target-dir target/sim`.
#![cfg(madsim)]

use std::collections::BTreeMap;
use std::sync::Arc;
use std::time::Duration;

use hyperbytedb::adapters::cluster::cluster_auth::ClusterAuth;
use hyperbytedb::adapters::cluster::raft::log_store::RaftStore;
use hyperbytedb::adapters::cluster::raft::network::Network;
use hyperbytedb::adapters::cluster::raft::{HyperbytedbRaft, TypeConfig};
use hyperbytedb::domain::cluster::membership::{ClusterMembership, new_shared};
use openraft::BasicNode;
use openraft::storage::Adaptor;

#[madsim::test]
async fn single_node_raft_elects_itself_leader() {
    let dir = tempfile::tempdir().unwrap();
    let store = RaftStore::open(
        dir.path().join("raft"),
        new_shared(ClusterMembership::new()),
    )
    .unwrap();
    let (log_store, state_machine) = Adaptor::<TypeConfig, RaftStore>::new(store);
    let auth = ClusterAuth::new(None).unwrap();
    let network = Network::new(5, 0, &auth);
    let config = Arc::new(
        openraft::Config {
            heartbeat_interval: 100,
            election_timeout_min: 300,
            election_timeout_max: 600,
            ..Default::default()
        }
        .validate()
        .unwrap(),
    );

    let raft = HyperbytedbRaft::new(1, config, network, log_store, state_machine)
        .await
        .unwrap();

    let mut members = BTreeMap::new();
    members.insert(1u64, BasicNode::new("node1:8086"));
    raft.initialize(members).await.unwrap();

    raft.wait(Some(Duration::from_secs(10)))
        .current_leader(1, "")
        .await
        .unwrap();
    assert_eq!(raft.metrics().borrow().current_leader, Some(1));
}
