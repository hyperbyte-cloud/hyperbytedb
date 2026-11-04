//! Who leads the cluster, for services that only act on the leader.

/// The cluster's current leader, as this node sees it.
pub trait LeadershipPort: Send + Sync {
    /// The leader's node id, if one is known.
    fn current_leader(&self) -> Option<u64>;

    /// Whether `node_id` is the leader.
    fn is_leader(&self, node_id: u64) -> bool {
        self.current_leader() == Some(node_id)
    }
}

/// Whether a leader-only job (retention, continuous queries) may run on
/// `node_id`. With no consensus (`None`, a single node) it always may.
#[must_use]
pub fn may_run(leadership: Option<&dyn LeadershipPort>, node_id: u64) -> bool {
    leadership.is_none_or(|l| l.is_leader(node_id))
}

#[cfg(test)]
mod tests {
    use super::*;

    struct LeaderIs(u64);
    impl LeadershipPort for LeaderIs {
        fn current_leader(&self) -> Option<u64> {
            Some(self.0)
        }
    }

    #[test]
    fn retention_and_cq_gate_on_leadership() {
        let stub = LeaderIs(2);
        assert!(!may_run(Some(&stub), 1));
        assert!(may_run(Some(&stub), 2));
        // Single node: no consensus, always runs.
        assert!(may_run(None, 1));
    }
}
