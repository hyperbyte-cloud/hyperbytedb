//! Shared Raft leader state for shard bootstrap and ingestion routing.

use std::sync::Arc;

use parking_lot::RwLock;

use crate::adapters::cluster::raft::HyperbytedbRaft;
use crate::domain::cluster::membership::SharedMembership;

/// Thread-safe leader callbacks wired after Raft starts in runtime.
pub struct RaftLeaderCallbacks {
    raft: RwLock<Option<HyperbytedbRaft>>,
    leader_addr: RwLock<Option<String>>,
    membership: Option<SharedMembership>,
    node_id: u64,
}

impl RaftLeaderCallbacks {
    pub fn new(node_id: u64, membership: Option<SharedMembership>) -> Self {
        Self {
            raft: RwLock::new(None),
            leader_addr: RwLock::new(None),
            membership,
            node_id,
        }
    }

    pub fn set_raft(&self, raft: HyperbytedbRaft) {
        *self.raft.write() = Some(raft);
        self.refresh_leader_addr();
    }

    pub fn is_leader(&self) -> bool {
        self.raft
            .read()
            .as_ref()
            .map(|r| r.metrics().borrow().current_leader == Some(self.node_id))
            .unwrap_or(false)
    }

    pub fn refresh_leader_addr(&self) {
        let guard = self.raft.read();
        let Some(raft) = guard.as_ref() else {
            return;
        };
        let Some(leader_id) = raft.metrics().borrow().current_leader else {
            return;
        };
        drop(guard);
        let Some(ref membership) = self.membership else {
            return;
        };
        if let Ok(guard) = membership.try_read()
            && let Some(node) = guard.get_node(leader_id)
        {
            *self.leader_addr.write() = Some(node.addr.clone());
        }
    }

    pub fn leader_addr(&self) -> Option<String> {
        self.refresh_leader_addr();
        self.leader_addr.read().clone()
    }
}

pub type SharedRaftLeaderCallbacks = Arc<RaftLeaderCallbacks>;
