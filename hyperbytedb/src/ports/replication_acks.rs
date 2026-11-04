//! Peers' replication acknowledgements, as the flush and drain paths read
//! them: how far each peer has acknowledged this node's WAL and mutation log.

use crate::error::HyperbytedbError;

/// What the flush and drain paths read from the replication log.
pub trait ReplicationAcks: Send + Sync {
    /// How far `peer_id` has acknowledged this node's WAL.
    fn get_wal_ack(&self, peer_id: u64) -> Result<u64, HyperbytedbError>;
    /// The lowest and highest WAL acknowledgement among `peer_ids`.
    fn min_max_wal_ack_for_peers(&self, peer_ids: &[u64]) -> Result<(u64, u64), HyperbytedbError>;
    /// How far `peer_id` has acknowledged this node's mutation log.
    fn get_mutation_ack(&self, peer_id: u64) -> Result<u64, HyperbytedbError>;
    /// The sequence of the last mutation this node logged.
    fn last_mutation_seq(&self) -> u64;
    /// The lowest mutation acknowledgement across peers, if any.
    fn min_mutation_ack(&self) -> Result<Option<u64>, HyperbytedbError>;
    /// Drop logged mutations before `seq`.
    fn truncate_mutations_before(&self, seq: u64) -> Result<(), HyperbytedbError>;
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::adapters::cluster::replication_log::ReplicationLog;
    use std::sync::Arc;

    #[test]
    fn replication_log_answers_through_the_port() {
        let dir = tempfile::tempdir().unwrap();
        let log: Arc<dyn ReplicationAcks> =
            Arc::new(ReplicationLog::open(dir.path().join("repl")).unwrap());
        assert_eq!(log.last_mutation_seq(), 0);
        assert_eq!(log.get_wal_ack(7).unwrap(), 0);
        assert_eq!(log.min_mutation_ack().unwrap(), None);
    }
}
