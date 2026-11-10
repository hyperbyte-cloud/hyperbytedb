//! Shared Raft leader state for shard bootstrap and ingestion routing.

use std::sync::Arc;

use parking_lot::RwLock;

use crate::domain::cluster::membership::SharedMembership;
use crate::ports::leadership::LeadershipPort;

/// Thread-safe leader callbacks wired after Raft starts in runtime.
pub struct RaftLeaderCallbacks {
    leadership: RwLock<Option<Arc<dyn LeadershipPort>>>,
    leader_addr: RwLock<Option<String>>,
    membership: Option<SharedMembership>,
    node_id: u64,
    /// This node's cluster address. When leadership is held locally but the
    /// membership lookup cannot resolve an address (self not yet registered,
    /// lock contention), proposals are posted to our own client-write
    /// endpoint — identical to what a successful membership lookup would
    /// return — so shard bootstrap never silently bypasses Raft.
    self_addr: Option<String>,
}

impl RaftLeaderCallbacks {
    pub fn new(
        node_id: u64,
        membership: Option<SharedMembership>,
        self_addr: Option<String>,
    ) -> Self {
        Self {
            leadership: RwLock::new(None),
            leader_addr: RwLock::new(None),
            membership,
            node_id,
            self_addr,
        }
    }

    pub fn set_leadership(&self, leadership: Arc<dyn LeadershipPort>) {
        *self.leadership.write() = Some(leadership);
        self.refresh_leader_addr();
    }

    pub fn is_leader(&self) -> bool {
        self.leadership
            .read()
            .as_ref()
            .is_some_and(|l| l.is_leader(self.node_id))
    }

    pub fn refresh_leader_addr(&self) {
        let guard = self.leadership.read();
        let Some(leadership) = guard.as_ref() else {
            return;
        };
        let Some(leader_id) = leadership.current_leader() else {
            return;
        };
        if leader_id == self.node_id
            && let Some(addr) = self.self_addr.clone()
        {
            *self.leader_addr.write() = Some(addr);
            return;
        }
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
