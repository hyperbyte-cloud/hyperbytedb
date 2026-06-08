use async_trait::async_trait;
use rocksdb::{
    BlockBasedOptions, Cache, ColumnFamilyDescriptor, DB, Direction, IteratorMode, Options,
    WriteBatch,
};
use std::path::Path;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

use crate::application::system_trace;
use crate::domain::point::Point;
use crate::error::HyperbytedbError;
use crate::ports::wal::{WalEntry, WalPort};

const WAL_CF: &str = "wal";
const WAL_META_CF: &str = "wal_meta";
const LAST_SEQ_KEY: &[u8] = b"last_seq";

fn u64_to_be_bytes(n: u64) -> [u8; 8] {
    n.to_be_bytes()
}

fn be_bytes_to_u64(bytes: &[u8]) -> u64 {
    let mut arr = [0u8; 8];
    arr.copy_from_slice(bytes);
    u64::from_be_bytes(arr)
}

pub struct RocksDbWal {
    db: Arc<DB>,
    seq: AtomicU64,
}

impl RocksDbWal {
    pub fn open<P: AsRef<Path>>(path: P) -> Result<Self, HyperbytedbError> {
        // The WAL is a sequential write/scan workload — point lookups are
        // rare and the data is short-lived (truncated as soon as it lands
        // in a parquet flush). A small block cache is sufficient because
        // the OS page cache covers warm reads. The dominant cost is
        // memtable footprint, which we bound at 4 × 64 MiB = 256 MiB per
        // CF — large enough to absorb tens of seconds of bursty ingest at
        // our peak rates without forcing a flush, and to give the
        // group-commit batcher in `BatchingWal` enough headroom to
        // coalesce many concurrent appenders into a single
        // `db.write(WriteBatch)`.
        let cache = Cache::new_lru_cache(16 * 1024 * 1024);
        let mut block_opts = BlockBasedOptions::default();
        block_opts.set_block_cache(&cache);
        block_opts.set_block_size(16 * 1024);

        // `available_parallelism` reports logical CPUs (e.g. 48 on a
        // many-core ingest box). We previously capped background-jobs at
        // 8, which floored compaction/flush throughput on large hosts and
        // produced write stalls under sustained ingest. The cap is gone
        // — RocksDB only spawns background work when there's actually a
        // job to run, so giving it the full parallelism budget costs
        // nothing at idle and unblocks tail latency at peak.
        let parallelism = std::thread::available_parallelism()
            .map(|n| n.get() as i32)
            .unwrap_or(4)
            .max(2);

        let mut opts = Options::default();
        opts.create_if_missing(true);
        opts.create_missing_column_families(true);
        opts.set_compression_type(rocksdb::DBCompressionType::Lz4);
        // Bottommost level is rewritten by leveled compaction once L0/L1
        // pressure clears; using zstd there gives us ~2x the compression
        // ratio of LZ4 at the cost of CPU we have available off the hot
        // write path. WAL data is short-lived, so this only matters
        // during sustained backlog scenarios where compaction has time
        // to rewrite SSTs into the bottommost level.
        opts.set_bottommost_compression_type(rocksdb::DBCompressionType::Zstd);
        opts.set_block_based_table_factory(&block_opts);
        // 64 MiB memtable × 4 = 256 MiB worst-case footprint. The bigger
        // memtable lets concurrent appenders accumulate writes for longer
        // before a flush, and 4 buffers (vs the old 2) means we don't
        // stall the writer when one memtable is being flushed.
        opts.set_write_buffer_size(64 * 1024 * 1024);
        opts.set_max_write_buffer_number(4);
        // Wait until at least 2 memtables are full before merging+flushing,
        // which produces fewer, larger SSTs and reduces L0 file count.
        opts.set_min_write_buffer_number_to_merge(2);
        // Group-commit + concurrent memtable writers. Lets RocksDB overlap WAL
        // append with memtable insert under load — biggest single-write win on
        // the hot WAL path. Concurrent memtable write is required for
        // pipelined write to actually parallelize writers.
        opts.set_enable_pipelined_write(true);
        opts.set_allow_concurrent_memtable_write(true);
        opts.set_enable_write_thread_adaptive_yield(true);
        // Background compaction / flush threads. We expose the full host
        // parallelism here; previously this was clamped at 8 which choked
        // compaction on 48-core ingest hosts and produced write stalls.
        opts.increase_parallelism(parallelism);
        opts.set_max_background_jobs(parallelism);
        // Leveled compaction with dynamic level sizes self-tunes the
        // L1..Ln byte budgets based on the current bottom-level size,
        // which keeps space-amplification in the ~1.1x range without
        // manual retuning as ingest rate changes.
        opts.set_level_compaction_dynamic_level_bytes(true);
        // 64 MiB SSTs strike a balance between merge cost (smaller is
        // cheaper to rewrite) and metadata overhead (larger means fewer
        // open file handles + faster scans).
        opts.set_target_file_size_base(64 * 1024 * 1024);
        opts.set_max_bytes_for_level_base(512 * 1024 * 1024);
        // Hint the OS to fsync in 1 MiB chunks instead of dumping a whole
        // SST/WAL at once, which smooths write latency tails.
        opts.set_bytes_per_sync(1024 * 1024);
        opts.set_wal_bytes_per_sync(1024 * 1024);
        // Cap RocksDB's own WAL footprint and rotation noise. Bumped
        // alongside the bigger memtable budget so a single rotation
        // covers a full memtable's worth of writes.
        opts.set_max_total_wal_size(512 * 1024 * 1024);
        opts.set_keep_log_file_num(10);
        // Bound open SSTs explicitly so file-descriptor pressure doesn't
        // surprise ops on hosts with low ulimits. -1 (the default) is
        // unbounded.
        opts.set_max_open_files(1024);

        let mut wal_cf_opts = Options::default();
        wal_cf_opts.set_compression_type(rocksdb::DBCompressionType::Lz4);
        wal_cf_opts.set_bottommost_compression_type(rocksdb::DBCompressionType::Zstd);
        wal_cf_opts.set_block_based_table_factory(&block_opts);
        wal_cf_opts.set_write_buffer_size(64 * 1024 * 1024);
        wal_cf_opts.set_max_write_buffer_number(4);
        wal_cf_opts.set_min_write_buffer_number_to_merge(2);
        wal_cf_opts.set_level_compaction_dynamic_level_bytes(true);
        wal_cf_opts.set_target_file_size_base(64 * 1024 * 1024);
        wal_cf_opts.set_max_bytes_for_level_base(512 * 1024 * 1024);
        let wal_meta_cf_opts = Options::default();

        let cfs = vec![
            ColumnFamilyDescriptor::new(WAL_CF, wal_cf_opts),
            ColumnFamilyDescriptor::new(WAL_META_CF, wal_meta_cf_opts),
        ];

        let db = DB::open_cf_descriptors(&opts, path, cfs)
            .map_err(|e| HyperbytedbError::Wal(e.to_string()))?;

        Self::migrate_legacy_entries(&db)?;

        let db = Arc::new(db);

        // Recover the last assigned sequence. Prefer the actual tail of the
        // WAL CF (cheap: one reverse-iterator step) so we no longer need to
        // write `last_seq` on every append. Fall back to the persisted meta
        // value, which is updated on truncate, for the case where the WAL CF
        // is empty after a `truncate_before` of every entry.
        let seq = {
            let wal_cf = db
                .cf_handle(WAL_CF)
                .ok_or_else(|| HyperbytedbError::Wal("wal column family not found".to_string()))?;
            let mut iter = db.iterator_cf(&wal_cf, IteratorMode::End);
            match iter.next() {
                Some(Ok((key, _))) if key.len() == 8 => be_bytes_to_u64(&key),
                Some(Err(e)) => return Err(HyperbytedbError::Wal(e.to_string())),
                _ => {
                    let wal_meta_cf = db.cf_handle(WAL_META_CF).ok_or_else(|| {
                        HyperbytedbError::Wal("wal_meta column family not found".to_string())
                    })?;
                    match db.get_cf(&wal_meta_cf, LAST_SEQ_KEY) {
                        Ok(Some(v)) if v.len() == 8 => be_bytes_to_u64(&v),
                        Ok(_) => 0,
                        Err(e) => return Err(HyperbytedbError::Wal(e.to_string())),
                    }
                }
            }
        };

        Ok(Self {
            db,
            seq: AtomicU64::new(seq),
        })
    }

    /// Rewrite any WAL entries that predate the `origin_node_id` field.
    ///
    /// Bincode is positional, so old 3-field entries cannot be deserialized as
    /// the current 4-field `WalEntry`. We detect them by trying the current
    /// layout first; on failure we decode the legacy 3-field shape, append
    /// `origin_node_id: 0`, re-serialize, and overwrite the key in-place.
    /// After this runs every entry matches the current schema.
    fn migrate_legacy_entries(db: &DB) -> Result<(), HyperbytedbError> {
        let wal_cf = db
            .cf_handle(WAL_CF)
            .ok_or_else(|| HyperbytedbError::Wal("wal column family not found".to_string()))?;

        #[derive(serde::Deserialize)]
        struct LegacyWalEntry {
            database: String,
            retention_policy: String,
            points: Vec<Point>,
        }

        let mut batch = WriteBatch::default();
        let mut migrated = 0u64;

        let iter = db.iterator_cf_opt(
            &wal_cf,
            rocksdb::ReadOptions::default(),
            IteratorMode::Start,
        );

        for item in iter {
            let (key, value) = item.map_err(|e| HyperbytedbError::Wal(e.to_string()))?;

            if bincode::deserialize::<WalEntry>(&value).is_ok() {
                continue;
            }

            let legacy: LegacyWalEntry = bincode::deserialize(&value)
                .map_err(|e| HyperbytedbError::Wal(format!("corrupt WAL entry: {e}")))?;

            let upgraded = WalEntry {
                database: legacy.database,
                retention_policy: legacy.retention_policy,
                points: legacy.points,
                origin_node_id: 0,
            };

            let new_value = bincode::serialize(&upgraded)
                .map_err(|e| HyperbytedbError::Wal(format!("re-serialize WAL entry: {e}")))?;

            batch.put_cf(&wal_cf, &key, &new_value);
            migrated += 1;
        }

        if migrated > 0 {
            db.write(batch)
                .map_err(|e| HyperbytedbError::Wal(e.to_string()))?;
            tracing::info!(migrated, "migrated legacy WAL entries to current schema");
        }

        Ok(())
    }

    /// Append multiple entries in a single RocksDB `WriteBatch` (group commit).
    ///
    /// Returns one sequence number per entry, in order.
    pub async fn append_batch(&self, entries: Vec<WalEntry>) -> Result<Vec<u64>, HyperbytedbError> {
        if entries.is_empty() {
            return Ok(Vec::new());
        }

        let count = entries.len() as u64;
        let first_seq = self.seq.fetch_add(count, Ordering::Relaxed) + 1;
        let db = self.db.clone();

        // NOTE: we intentionally no longer write `last_seq` to the wal_meta
        // CF on every batch. On restart we recover the last assigned sequence
        // by reading the tail of the WAL CF directly. The meta key is only
        // refreshed by `truncate_before`, which covers the edge case of a
        // fully-truncated WAL.
        tokio::task::spawn_blocking(move || {
            let wal_cf = db
                .cf_handle(WAL_CF)
                .ok_or_else(|| HyperbytedbError::Wal("wal column family not found".into()))?;

            let mut wb = WriteBatch::default();
            let mut seqs = Vec::with_capacity(entries.len());

            for (i, entry) in entries.iter().enumerate() {
                let seq = first_seq + i as u64;
                let key = u64_to_be_bytes(seq);
                let value = bincode::serialize(entry)?;
                wb.put_cf(&wal_cf, key, value);
                seqs.push(seq);
            }

            db.write(wb)
                .map_err(|e| HyperbytedbError::Wal(e.to_string()))?;

            Ok(seqs)
        })
        .await
        .map_err(|e| HyperbytedbError::Wal(format!("WAL batch append task panicked: {e}")))?
    }
}

#[async_trait]
impl WalPort for RocksDbWal {
    async fn append(&self, entry: WalEntry) -> Result<u64, HyperbytedbError> {
        let db = self.db.clone();
        let seq = self.seq.fetch_add(1, Ordering::Relaxed) + 1;
        let point_count = entry.points.len();

        let result = tokio::task::spawn_blocking(move || {
            let serialize_start = std::time::Instant::now();
            let wal_cf = db
                .cf_handle(WAL_CF)
                .ok_or_else(|| HyperbytedbError::Wal("wal column family not found".to_string()))?;

            let key = u64_to_be_bytes(seq);
            let value = bincode::serialize(&entry)?;
            let serialize_us = serialize_start.elapsed().as_micros() as u64;

            let write_start = std::time::Instant::now();
            // Single-CF put — `last_seq` is now derived from the WAL CF tail
            // on recovery (see `RocksDbWal::open`). Halves the puts per
            // append on the hot path.
            db.put_cf(&wal_cf, key, value)
                .map_err(|e| HyperbytedbError::Wal(e.to_string()))?;
            let write_us = write_start.elapsed().as_micros() as u64;

            Ok::<_, HyperbytedbError>((seq, serialize_us, write_us))
        })
        .await
        .map_err(|e| HyperbytedbError::Wal(format!("WAL append task panicked: {e}")))?;

        match result {
            Ok((seq, serialize_us, write_us)) => {
                system_trace::log_wal_append(seq, point_count, serialize_us, write_us);
                Ok(seq)
            }
            Err(e) => Err(e),
        }
    }

    async fn read_from(&self, sequence: u64) -> Result<Vec<(u64, WalEntry)>, HyperbytedbError> {
        self.read_range(sequence, usize::MAX).await
    }

    async fn read_range(
        &self,
        from: u64,
        max_entries: usize,
    ) -> Result<Vec<(u64, WalEntry)>, HyperbytedbError> {
        let db = self.db.clone();

        tokio::task::spawn_blocking(move || {
            let wal_cf = db
                .cf_handle(WAL_CF)
                .ok_or_else(|| HyperbytedbError::Wal("wal column family not found".to_string()))?;

            let mut results = Vec::new();
            let start_key = u64_to_be_bytes(from);
            let mode = IteratorMode::From(&start_key, Direction::Forward);

            let iter = db.iterator_cf_opt(&wal_cf, rocksdb::ReadOptions::default(), mode);

            for item in iter {
                if results.len() >= max_entries {
                    break;
                }
                let (key, value) = item.map_err(|e| HyperbytedbError::Wal(e.to_string()))?;
                let seq = be_bytes_to_u64(&key);
                let entry: WalEntry = bincode::deserialize(&value)?;
                results.push((seq, entry));
            }

            Ok(results)
        })
        .await
        .map_err(|e| HyperbytedbError::Wal(format!("WAL read task panicked: {e}")))?
    }

    async fn truncate_before(&self, sequence: u64) -> Result<(), HyperbytedbError> {
        let db = self.db.clone();
        // Snapshot the high-water-mark *before* the delete so a fully-
        // truncated WAL CF can still recover the last assigned seq from
        // wal_meta on the next open.
        let last_seq = self.seq.load(Ordering::Relaxed);

        tokio::task::spawn_blocking(move || {
            let wal_cf = db
                .cf_handle(WAL_CF)
                .ok_or_else(|| HyperbytedbError::Wal("wal column family not found".to_string()))?;
            let wal_meta_cf = db.cf_handle(WAL_META_CF).ok_or_else(|| {
                HyperbytedbError::Wal("wal_meta column family not found".to_string())
            })?;

            let from = u64_to_be_bytes(0);
            let to = u64_to_be_bytes(sequence);
            let mut batch = WriteBatch::default();
            batch.delete_range_cf(&wal_cf, &from, &to);
            batch.put_cf(&wal_meta_cf, LAST_SEQ_KEY, u64_to_be_bytes(last_seq));
            db.write(batch)
                .map_err(|e| HyperbytedbError::Wal(e.to_string()))?;
            Ok(())
        })
        .await
        .map_err(|e| HyperbytedbError::Wal(format!("WAL truncate task panicked: {e}")))?
    }

    async fn last_sequence(&self) -> Result<u64, HyperbytedbError> {
        Ok(self.seq.load(Ordering::Relaxed))
    }
}
