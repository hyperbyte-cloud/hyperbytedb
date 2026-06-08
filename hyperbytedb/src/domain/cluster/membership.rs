use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::RwLock;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum NodeState {
    Joining,
    Syncing,
    Active,
    Disconnected,
    Draining,
    Leaving,
}

impl std::fmt::Display for NodeState {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            NodeState::Joining => write!(f, "joining"),
            NodeState::Syncing => write!(f, "syncing"),
            NodeState::Active => write!(f, "active"),
            NodeState::Disconnected => write!(f, "disconnected"),
            NodeState::Draining => write!(f, "draining"),
            NodeState::Leaving => write!(f, "leaving"),
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct NodeInfo {
    pub node_id: u64,
    pub addr: String,
    pub state: NodeState,
    pub joined_at: i64,
    pub last_heartbeat: i64,
    /// Set when startup sync failed and the leader should trigger a re-sync.
    #[serde(default)]
    pub needs_sync: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct ClusterMembership {
    pub version: u64,
    pub nodes: HashMap<u64, NodeInfo>,
}

impl ClusterMembership {
    pub fn new() -> Self {
        Self {
            version: 0,
            nodes: HashMap::new(),
        }
    }

    pub fn add_node(&mut self, info: NodeInfo) {
        self.nodes.insert(info.node_id, info);
        self.version += 1;
    }

    pub fn remove_node(&mut self, node_id: u64) {
        self.nodes.remove(&node_id);
        self.version += 1;
    }

    pub fn set_state(&mut self, node_id: u64, state: NodeState) -> bool {
        if let Some(node) = self.nodes.get_mut(&node_id) {
            node.state = state;
            self.version += 1;
            true
        } else {
            false
        }
    }

    pub fn update_heartbeat(&mut self, node_id: u64, ts: i64) {
        if let Some(node) = self.nodes.get_mut(&node_id) {
            node.last_heartbeat = ts;
        }
    }

    pub fn set_needs_sync(&mut self, node_id: u64, needs: bool) {
        if let Some(node) = self.nodes.get_mut(&node_id) {
            node.needs_sync = needs;
        }
    }

    pub fn active_peers(&self, exclude_id: u64) -> Vec<&NodeInfo> {
        self.nodes
            .values()
            .filter(|n| n.node_id != exclude_id && n.state == NodeState::Active)
            .collect()
    }

    /// Peers that should receive outbound write replication. Includes
    /// `Disconnected` nodes so writes are attempted (and queued in hinted
    /// handoff on failure) during rolling restarts instead of being skipped.
    pub fn replication_peers(&self, exclude_id: u64) -> Vec<&NodeInfo> {
        self.nodes
            .values()
            .filter(|n| {
                n.node_id != exclude_id
                    && matches!(n.state, NodeState::Active | NodeState::Disconnected)
            })
            .collect()
    }

    pub fn all_peers(&self, exclude_id: u64) -> Vec<&NodeInfo> {
        self.nodes
            .values()
            .filter(|n| n.node_id != exclude_id)
            .collect()
    }

    pub fn get_node(&self, node_id: u64) -> Option<&NodeInfo> {
        self.nodes.get(&node_id)
    }

    /// Find the next available node_id (for assigning to joining nodes).
    pub fn next_node_id(&self) -> u64 {
        self.nodes.keys().max().map_or(1, |m| m + 1)
    }
}

/// Thread-safe handle shared across the application.
pub type SharedMembership = Arc<RwLock<ClusterMembership>>;

pub fn new_shared(membership: ClusterMembership) -> SharedMembership {
    Arc::new(RwLock::new(membership))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn make_node(id: u64, state: NodeState) -> NodeInfo {
        NodeInfo {
            node_id: id,
            addr: format!("127.0.0.1:{}", 8080 + id),
            state,
            joined_at: 1000,
            last_heartbeat: 1000,
            needs_sync: false,
        }
    }

    #[test]
    fn test_add_and_remove_node() {
        let mut m = ClusterMembership::new();
        assert_eq!(m.version, 0);

        m.add_node(make_node(1, NodeState::Active));
        assert_eq!(m.version, 1);
        assert!(m.get_node(1).is_some());

        m.add_node(make_node(2, NodeState::Joining));
        assert_eq!(m.version, 2);
        assert_eq!(m.nodes.len(), 2);

        m.remove_node(1);
        assert_eq!(m.version, 3);
        assert!(m.get_node(1).is_none());
        assert_eq!(m.nodes.len(), 1);
    }

    #[test]
    fn test_set_state() {
        let mut m = ClusterMembership::new();
        m.add_node(make_node(1, NodeState::Joining));

        assert!(m.set_state(1, NodeState::Active));
        assert_eq!(m.get_node(1).unwrap().state, NodeState::Active);

        assert!(!m.set_state(99, NodeState::Draining));
    }

    #[test]
    fn test_active_peers() {
        let mut m = ClusterMembership::new();
        m.add_node(make_node(1, NodeState::Active));
        m.add_node(make_node(2, NodeState::Active));
        m.add_node(make_node(3, NodeState::Disconnected));
        m.add_node(make_node(4, NodeState::Draining));

        let peers = m.active_peers(1);
        assert_eq!(peers.len(), 1);
        assert_eq!(peers[0].node_id, 2);

        let repl = m.replication_peers(1);
        assert_eq!(repl.len(), 2);
        assert!(repl.iter().any(|n| n.node_id == 2));
        assert!(repl.iter().any(|n| n.node_id == 3));

        let all = m.all_peers(1);
        assert_eq!(all.len(), 3);
    }

    #[test]
    fn test_update_heartbeat() {
        let mut m = ClusterMembership::new();
        m.add_node(make_node(1, NodeState::Active));
        m.update_heartbeat(1, 2000);
        assert_eq!(m.get_node(1).unwrap().last_heartbeat, 2000);
    }

    #[test]
    fn test_next_node_id() {
        let mut m = ClusterMembership::new();
        assert_eq!(m.next_node_id(), 1);
        m.add_node(make_node(1, NodeState::Active));
        assert_eq!(m.next_node_id(), 2);
        m.add_node(make_node(5, NodeState::Active));
        assert_eq!(m.next_node_id(), 6);
    }

    #[test]
    fn test_state_transitions() {
        let mut m = ClusterMembership::new();
        m.add_node(make_node(1, NodeState::Joining));

        m.set_state(1, NodeState::Syncing);
        assert_eq!(m.get_node(1).unwrap().state, NodeState::Syncing);

        m.set_state(1, NodeState::Active);
        assert_eq!(m.get_node(1).unwrap().state, NodeState::Active);

        m.set_state(1, NodeState::Draining);
        assert_eq!(m.get_node(1).unwrap().state, NodeState::Draining);

        m.set_state(1, NodeState::Leaving);
        assert_eq!(m.get_node(1).unwrap().state, NodeState::Leaving);
    }
}
