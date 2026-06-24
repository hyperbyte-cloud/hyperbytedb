//! In-memory index of unflushed prepared Arrow WAL slots.

use std::collections::BTreeMap;

use metrics::{counter, gauge};
use parking_lot::RwLock;

use crate::domain::prepared_wal::PreparedWalSlot;

/// Number of unflushed prepared slots currently held in RAM. Watch this for
/// unbounded growth: the cache only drains on flush (`take_range`) or WAL
/// truncation, so a stalled flush or a held truncation barrier shows up here
/// before it shows up as an OOM.
const ENTRIES_GAUGE: &str = "hyperbytedb_wal_arrow_cache_entries";

pub struct WalArrowCache {
    entries: RwLock<BTreeMap<u64, PreparedWalSlot>>,
}

impl WalArrowCache {
    pub fn new() -> Self {
        Self {
            entries: RwLock::new(BTreeMap::new()),
        }
    }

    pub fn insert(&self, seq: u64, slot: PreparedWalSlot) {
        let mut map = self.entries.write();
        map.insert(seq, slot);
        gauge!(ENTRIES_GAUGE).set(map.len() as f64);
    }

    pub fn insert_batch(&self, seqs: &[u64], slots: Vec<PreparedWalSlot>) {
        debug_assert_eq!(seqs.len(), slots.len());
        let mut map = self.entries.write();
        for (seq, slot) in seqs.iter().zip(slots) {
            map.insert(*seq, slot);
        }
        gauge!(ENTRIES_GAUGE).set(map.len() as f64);
    }

    /// Move up to `max_entries` contiguous prepared slots from `from` through
    /// `to_inclusive`.
    ///
    /// Bounding by `to_inclusive` (the flush snapshot sequence) is essential:
    /// without it, a flush would take the entire contiguous run — including
    /// slots for writes that landed *after* the snapshot — then discard the
    /// out-of-window tail unflushed. The next flush would then miss those
    /// evicted sequences and fall back to the native path, which is what drives
    /// the steady ~50% cache-miss rate under continuous load.
    pub fn take_range(
        &self,
        from: u64,
        to_inclusive: u64,
        max_entries: usize,
    ) -> Option<Vec<(u64, PreparedWalSlot)>> {
        if max_entries == 0 || from > to_inclusive {
            return None;
        }

        let mut map = self.entries.write();
        if !map.contains_key(&from) {
            counter!("hyperbytedb_wal_arrow_cache_misses_total").increment(1);
            return None;
        }

        let mut out = Vec::with_capacity(max_entries.min(map.len()));
        let mut seq = from;
        while out.len() < max_entries && seq <= to_inclusive {
            match map.remove(&seq) {
                Some(slot) => {
                    out.push((seq, slot));
                    seq += 1;
                }
                None => break,
            }
        }

        if out.is_empty() {
            counter!("hyperbytedb_wal_arrow_cache_misses_total").increment(1);
            return None;
        }

        counter!("hyperbytedb_wal_arrow_cache_hits_total").increment(1);
        gauge!(ENTRIES_GAUGE).set(map.len() as f64);
        Some(out)
    }

    /// Smallest cached prepared sequence at or after `from`, if any. Lets the
    /// flush bound a native read so a gap (an entry with no prepared slot, e.g.
    /// a peer WAL catch-up append) does not swallow the prepared slots that
    /// follow it on the native path.
    pub fn next_seq_at_or_after(&self, from: u64) -> Option<u64> {
        self.entries
            .read()
            .range(from..)
            .next()
            .map(|(seq, _)| *seq)
    }

    pub fn truncate_before(&self, sequence: u64) {
        let mut map = self.entries.write();
        map.retain(|seq, _| *seq >= sequence);
        gauge!(ENTRIES_GAUGE).set(map.len() as f64);
    }

    pub fn len(&self) -> usize {
        self.entries.read().len()
    }

    pub fn is_empty(&self) -> bool {
        self.entries.read().is_empty()
    }
}

impl Default for WalArrowCache {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::domain::prepared_wal::PreparedMeasurementBatch;
    use arrow::array::Int64Array;
    use arrow::datatypes::{DataType, Field, Schema};
    use arrow::record_batch::RecordBatch;
    use std::sync::Arc;

    fn empty_slot(db: &str) -> PreparedWalSlot {
        let batch = Arc::new(
            RecordBatch::try_new(
                Arc::new(Schema::new(vec![Field::new(
                    "time",
                    DataType::Int64,
                    false,
                )])),
                vec![Arc::new(Int64Array::from(vec![1_i64]))],
            )
            .unwrap(),
        );
        PreparedWalSlot {
            database: db.into(),
            retention_policy: "autogen".into(),
            origin_node_id: 0,
            measurements: vec![PreparedMeasurementBatch {
                measurement: "m".into(),
                table_name: "t".into(),
                series_table_name: "t_series".into(),
                batch,
                row_count: 1,
                min_time: 1,
                max_time: 1,
                new_series_batch: None,
            }],
        }
    }

    #[test]
    fn take_contiguous_moves_slots() {
        let cache = WalArrowCache::new();
        cache.insert(1, empty_slot("a"));
        cache.insert(2, empty_slot("b"));

        let got = cache.take_range(1, u64::MAX, 2).expect("hit");
        assert_eq!(got.len(), 2);
        assert_eq!(cache.len(), 0);
    }

    #[test]
    fn take_range_respects_upper_bound() {
        let cache = WalArrowCache::new();
        for s in 1..=5 {
            cache.insert(s, empty_slot("x"));
        }

        // A flush whose snapshot only covers seq 3 must leave 4 and 5 cached.
        let got = cache.take_range(1, 3, 100).expect("hit");
        assert_eq!(
            got.iter().map(|(s, _)| *s).collect::<Vec<_>>(),
            vec![1, 2, 3]
        );
        assert_eq!(cache.len(), 2, "out-of-window slots must stay cached");
        assert_eq!(cache.next_seq_at_or_after(1), Some(4));

        // The next flush (larger snapshot) now hits the retained slots.
        let got = cache.take_range(4, u64::MAX, 100).expect("hit");
        assert_eq!(got.iter().map(|(s, _)| *s).collect::<Vec<_>>(), vec![4, 5]);
        assert!(cache.is_empty());
    }

    #[test]
    fn miss_returns_none_and_reports_next_seq() {
        let cache = WalArrowCache::new();
        cache.insert(7, empty_slot("a"));
        // from=5 is a gap; the next prepared slot is at 7.
        assert!(cache.take_range(5, u64::MAX, 100).is_none());
        assert_eq!(cache.next_seq_at_or_after(5), Some(7));
    }
}
