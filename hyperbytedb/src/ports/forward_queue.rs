use async_trait::async_trait;

pub use crate::domain::cluster::forward::ForwardEntry;
use crate::error::HyperbytedbError;

/// Durable queue of writes a coordinator owes to other region primaries.
///
/// A sharded write is acknowledged once the forwarded share is queued here,
/// as durably as the WAL; a background drainer sends it. Entries stay stored
/// from [`Self::enqueue`] until [`Self::complete`], so a restart resends
/// whatever was not delivered.
#[async_trait]
pub trait ForwardQueuePort: Send + Sync {
    /// Store `entry` and wake a drainer. Returns its sequence number.
    async fn enqueue(&self, entry: ForwardEntry) -> Result<u64, HyperbytedbError>;

    /// Hand out, in sequence order, up to `max_entries` entries due by
    /// `now_ms` (at least one, whatever its size) totalling at most
    /// `max_bytes`. A claimed entry is not handed out again until it is
    /// released.
    async fn claim(
        &self,
        now_ms: u64,
        max_entries: usize,
        max_bytes: u64,
    ) -> Result<Vec<(u64, ForwardEntry)>, HyperbytedbError>;

    /// Remove delivered entries and, in the same write, queue `retry` (what
    /// of them did not land). One write, so no reader or crash ever sees
    /// both the entries and their retry.
    async fn complete(
        &self,
        seqs: Vec<u64>,
        retry: Option<ForwardEntry>,
    ) -> Result<(), HyperbytedbError>;

    /// Hand claimed entries back, not to be handed out before
    /// `not_before_ms`. They were never removed from storage.
    fn release(&self, seqs: &[u64], not_before_ms: u64);

    /// How long the oldest queued write has waited, in ms (0 when empty).
    fn oldest_age_ms(&self, now_ms: u64) -> u64;

    /// Entries stored, claimed or not.
    fn len(&self) -> usize;

    fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// Bytes stored.
    fn bytes(&self) -> u64;

    /// Resolves after the next enqueue (or at once if one is pending).
    async fn notified(&self);
}
