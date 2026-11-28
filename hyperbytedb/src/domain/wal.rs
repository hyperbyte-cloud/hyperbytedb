use serde::{Deserialize, Serialize};

use crate::domain::point::Point;

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct WalEntry {
    pub database: String,
    pub retention_policy: String,
    pub points: Vec<Point>,
    /// Node that originated this write. 0 means the local node.
    pub origin_node_id: u64,
    /// Row version the originating node gave this write: row `i` is stored
    /// with `ingest_seq = version + i`, on every replica. 0 means none (an
    /// entry or sender from before versions); the local WAL sequence is used.
    /// Kept out of the serde layout: see [`encode_entry_into`].
    #[serde(skip)]
    pub version: u64,
}

/// Marks a durable entry that carries a version. An unversioned entry is bare
/// bincode, which starts with the database name's length, never these bytes.
const VERSIONED_MAGIC: &[u8; 4] = b"HBW2";

/// Durable encoding of a [`WalEntry`]: magic, version, then the bincode body.
pub fn encode_entry_into(out: &mut Vec<u8>, entry: &WalEntry) -> Result<(), bincode::Error> {
    if entry.version != 0 {
        out.extend_from_slice(VERSIONED_MAGIC);
        out.extend_from_slice(&entry.version.to_le_bytes());
    }
    bincode::serialize_into(out, entry)
}

/// Inverse of [`encode_entry_into`]; also reads entries written before
/// versions.
pub fn decode_entry(bytes: &[u8]) -> Result<WalEntry, bincode::Error> {
    if let Some(rest) = bytes.strip_prefix(VERSIONED_MAGIC)
        && let Some((version, body)) = rest.split_first_chunk::<8>()
    {
        let mut entry: WalEntry = bincode::deserialize(body)?;
        entry.version = u64::from_le_bytes(*version);
        return Ok(entry);
    }
    bincode::deserialize(bytes)
}

/// Hands out row versions: nanoseconds since the epoch, pushed forward so
/// that each reservation is above every earlier one and every version seen
/// from a peer. Replicas store the origin's version, so they all order two
/// writes of a point the same way.
#[derive(Debug, Default)]
pub struct VersionClock {
    last: std::sync::atomic::AtomicU64,
}

impl VersionClock {
    /// Reserve `rows` consecutive versions and return the first.
    pub fn reserve(&self, rows: u64) -> u64 {
        use std::sync::atomic::Ordering;
        let rows = rows.max(1);
        let now = chrono::Utc::now()
            .timestamp_nanos_opt()
            .and_then(|n| u64::try_from(n).ok())
            .unwrap_or(0);
        let mut last = self.last.load(Ordering::Relaxed);
        loop {
            let first = now.max(last.saturating_add(1));
            let end = first.saturating_add(rows - 1);
            match self
                .last
                .compare_exchange_weak(last, end, Ordering::Relaxed, Ordering::Relaxed)
            {
                Ok(_) => return first,
                Err(seen) => last = seen,
            }
        }
    }

    /// Never reserve at or below `version` again.
    pub fn observe(&self, version: u64) {
        self.last
            .fetch_max(version, std::sync::atomic::Ordering::Relaxed);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn reservations_never_overlap_and_follow_observed_versions() {
        let clock = VersionClock::default();
        let a = clock.reserve(10);
        let b = clock.reserve(1);
        assert!(b >= a + 10);
        clock.observe(b + 1_000_000_000_000_000_000);
        assert!(clock.reserve(1) > b + 1_000_000_000_000_000_000);
    }

    #[test]
    fn durable_encoding_keeps_the_version_and_reads_unversioned_entries() {
        let mut entry = WalEntry {
            database: "db".into(),
            retention_policy: "autogen".into(),
            points: Vec::new(),
            origin_node_id: 2,
            version: 42,
        };
        let mut bytes = Vec::new();
        encode_entry_into(&mut bytes, &entry).unwrap();
        assert_eq!(decode_entry(&bytes).unwrap().version, 42);

        entry.version = 0;
        let old = bincode::serialize(&entry).unwrap();
        let read = decode_entry(&old).unwrap();
        assert_eq!((read.version, read.origin_node_id), (0, 2));
    }
}
