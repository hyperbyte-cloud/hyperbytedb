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
    /// Permanently leaving. Regions are evacuated and the node is removed.
    /// Distinct from [`NodeState::Draining`], which is a restart that keeps
    /// its seat in every region it belongs to.
    Decommissioning,
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
            NodeState::Decommissioning => write!(f, "decommissioning"),
            NodeState::Leaving => write!(f, "leaving"),
        }
    }
}

impl std::str::FromStr for NodeState {
    type Err = ();

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s {
            "joining" => Ok(NodeState::Joining),
            "syncing" => Ok(NodeState::Syncing),
            "active" => Ok(NodeState::Active),
            "disconnected" => Ok(NodeState::Disconnected),
            "draining" => Ok(NodeState::Draining),
            "decommissioning" => Ok(NodeState::Decommissioning),
            "leaving" => Ok(NodeState::Leaving),
            _ => Err(()),
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
    /// Consecutive failed probes, for demotion hysteresis.
    ///
    /// Lives here rather than in the prober because `probe_peers` is stateless
    /// and called fresh each tick — a counter local to it resets every tick and
    /// pins the value at 1, which compiles and passes unit tests while
    /// demoting on the first miss exactly as before.
    #[serde(default)]
    pub consecutive_misses: u32,
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
            if node.state != state {
                tracing::debug!(
                    node_id,
                    from = %node.state,
                    to = %state,
                    "membership state transition"
                );
            }
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

    /// Advance a node's lifecycle, refusing moves out of a terminal state.
    ///
    /// [`ClusterMembership::set_state`] overwrites unconditionally, which lets
    /// the preStop hook's `/internal/drain` clobber `Decommissioning` back to
    /// `Draining` mid-evacuation — re-admitting the node as a placement
    /// candidate while its regions are still moving off. Callers that mean
    /// "advance the lifecycle" use this instead.
    pub fn transition(&mut self, node_id: u64, to: NodeState) -> bool {
        let Some(node) = self.nodes.get(&node_id) else {
            return false;
        };
        let legal = match (node.state, to) {
            (NodeState::Decommissioning, NodeState::Leaving) => true,
            (NodeState::Decommissioning | NodeState::Leaving, _) => false,
            _ => true,
        };
        legal && self.set_state(node_id, to)
    }

    /// Count a failed probe. Returns the new consecutive-miss total.
    pub fn record_probe_miss(&mut self, node_id: u64) -> u32 {
        match self.nodes.get_mut(&node_id) {
            Some(node) => {
                node.consecutive_misses = node.consecutive_misses.saturating_add(1);
                node.consecutive_misses
            }
            None => 0,
        }
    }

    /// Count a successful probe.
    ///
    /// Decrements rather than resetting. A hard reset never demotes a node
    /// alternating four misses and one success — unreachable 80% of the time
    /// yet permanently `Active`, which is worse than demoting on the first
    /// miss. Decrementing lets sustained flapping converge on demotion while a
    /// single blip still recovers.
    pub fn record_probe_success(&mut self, node_id: u64) {
        if let Some(node) = self.nodes.get_mut(&node_id) {
            node.consecutive_misses = node.consecutive_misses.saturating_sub(1);
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

/// May take traffic now. `Active` only.
///
/// One of three independent questions `NodeState` answers; the others are
/// [`holds_placement`] and [`is_departing`]. Testing `== Active` for all three
/// is what produced two rounds of placement defects: a draining node serves
/// nothing yet must keep its regions.
#[must_use]
pub fn is_serving(node: &NodeInfo) -> bool {
    node.state == NodeState::Active
}

/// Should be assigned regions, and keep the ones it has.
///
/// `Active | Draining | Disconnected`. A restarting node keeps its regions —
/// excluding it would make every rolling upgrade evacuate and refill the
/// cluster. A briefly unreachable node keeps them too; the dead-node timeout
/// handles real death.
#[must_use]
pub fn holds_placement(node: &NodeInfo) -> bool {
    matches!(
        node.state,
        NodeState::Active | NodeState::Draining | NodeState::Disconnected
    )
}

/// Regions must be evacuated off this node.
#[must_use]
pub fn is_departing(node: &NodeInfo) -> bool {
    matches!(node.state, NodeState::Decommissioning | NodeState::Leaving)
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
            consecutive_misses: 0,
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

    #[test]
    fn the_three_predicates_disagree_and_that_is_the_point() {
        // One enum, three independent questions. A draining node holds its
        // regions but serves nothing; a decommissioning node is departing.
        let draining = make_node(1, NodeState::Draining);
        assert!(!is_serving(&draining));
        assert!(holds_placement(&draining), "a restart keeps its regions");
        assert!(!is_departing(&draining));

        let decomm = make_node(2, NodeState::Decommissioning);
        assert!(!is_serving(&decomm));
        assert!(
            !holds_placement(&decomm),
            "a departing node is not a candidate"
        );
        assert!(is_departing(&decomm));

        // A blip keeps its regions; the dead-node timeout handles real death.
        let blip = make_node(3, NodeState::Disconnected);
        assert!(!is_serving(&blip));
        assert!(holds_placement(&blip));
        assert!(!is_departing(&blip));

        let active = make_node(4, NodeState::Active);
        assert!(is_serving(&active));
        assert!(holds_placement(&active));
        assert!(!is_departing(&active));
    }

    #[test]
    fn departing_states_are_terminal_and_cannot_be_downgraded() {
        // The preStop hook fires /internal/drain on a pod the operator has
        // already decommissioned. Without this guard that call re-admits the
        // node as a placement candidate mid-evacuation.
        let mut m = ClusterMembership::new();
        m.add_node(make_node(1, NodeState::Active));
        assert!(m.transition(1, NodeState::Decommissioning));
        assert!(
            !m.transition(1, NodeState::Draining),
            "decommission was downgraded to a restart"
        );
        assert!(
            !m.transition(1, NodeState::Active),
            "decommission was undone"
        );
        assert_eq!(m.get_node(1).unwrap().state, NodeState::Decommissioning);

        assert!(
            m.transition(1, NodeState::Leaving),
            "evacuation completing must be allowed"
        );
        assert!(!m.transition(1, NodeState::Draining));
        assert!(!m.transition(1, NodeState::Active));
    }

    #[test]
    fn draining_returns_to_active() {
        let mut m = ClusterMembership::new();
        m.add_node(make_node(1, NodeState::Active));
        assert!(m.transition(1, NodeState::Draining));
        assert!(
            m.transition(1, NodeState::Active),
            "a restarted node must be able to rejoin"
        );
    }

    #[test]
    fn transition_on_an_unknown_node_is_false() {
        let mut m = ClusterMembership::new();
        assert!(!m.transition(99, NodeState::Draining));
    }
}
