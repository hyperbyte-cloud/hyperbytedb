use metrics::{counter, gauge, histogram};
use std::collections::BTreeMap;
use std::sync::Arc;
use tokio::sync::Mutex;
use tokio::sync::watch;

use crate::adapters::cluster::replication_log::ReplicationLog;

use crate::domain::arrow_coalesce::coalesce_prepared_batches;
use crate::domain::cluster::membership::SharedMembership;
use crate::domain::point::Point;
use crate::domain::point_coalesce::coalesce_points_and_origins;
use crate::domain::prepared_wal::{PreparedMeasurementBatch, PreparedWalSlot};
use crate::error::HyperbytedbError;
use crate::ports::flush::FlushPort;
use crate::ports::points_sink::PointsSinkPort;
use crate::ports::sharding::ShardMapPort;
use crate::ports::wal::WalPort;

const WAL_READ_CHUNK: usize = 5_000;
/// chDB inserts one flush chunk runs at once. The node has one chDB write
/// connection, so more only queue, each holding its decoded rows: with a
/// backlog every 50k-point slice of a 5,000-entry chunk was spawned at once,
/// and nodes restarting after a crash were OOM-killed at 16-24 GiB within
/// seconds, every restart.
const FLUSH_INSERT_CONCURRENCY: usize = 4;
/// A native chunk stops taking WAL entries at this many batches' worth of
/// points: an entry is a whole client write (thousands of points), so 5,000
/// entries could decode tens of millions of points at once.
const NATIVE_CHUNK_BATCHES: usize = 4;
const DEFAULT_MAX_POINTS_PER_BATCH: usize = 50_000;
const MIN_BATCH_POINTS: usize = 10_000;
const MAX_BATCH_POINTS: usize = 500_000;
/// Maximum number of unflushed WAL entries to retain when peers are lagging.
/// Beyond this limit we truncate anyway to prevent unbounded WAL growth.
const MAX_WAL_RETENTION_ENTRIES: u64 = 500_000;
/// Bytes of WAL kept behind the flush in a sharded cluster: tens of seconds
/// of peak ingest, re-flushed (idempotently) after a crash.
const SHARDED_WAL_RETENTION_BYTES: u64 = 1 << 30;

type MeasurementBatchKey = (String, String, String);
type MeasurementBatch = (Vec<Point>, Vec<u64>);
type MeasurementBatchMap = BTreeMap<MeasurementBatchKey, MeasurementBatch>;

pub struct FlushServiceImpl {
    wal: Arc<dyn WalPort>,
    last_flushed: Arc<Mutex<u64>>,
    max_points_per_batch: usize,
    replication_log: Option<Arc<ReplicationLog>>,
    membership: Option<SharedMembership>,
    node_id: u64,
    /// Highest WAL sequence with `origin_node_id == self.node_id` (i.e. a
    /// direct client write, not a replication apply).  Stays 0 when this node
    /// has never received a direct write — used by `compute_safe_truncate_seq`
    /// to skip the peer-ack barrier for pure-replica nodes.
    last_local_wal_seq: std::sync::atomic::AtomicU64,
    /// When `truncate_stale_peer_multiplier` > 0 with interval/threshold, peers with ack 0 and
    /// heartbeats older than `interval * miss * multiplier` are omitted from the truncate barrier.
    heartbeat_interval_secs: u64,
    heartbeat_miss_threshold: u64,
    truncate_stale_peer_multiplier: u64,
    sink: Arc<dyn PointsSinkPort>,
    /// When true (default), merge partial-field writes sharing a series-instant
    /// before insert. Set `HYPERBYTEDB_DISABLE_COALESCE=1` to skip.
    coalesce: bool,
    sharding_enabled: bool,
    shard_map: Option<Arc<dyn ShardMapPort>>,
    /// Bytes of WAL kept behind the flush in a sharded cluster.
    sharded_wal_retention_bytes: u64,
}

struct FlushWork {
    db: String,
    rp: String,
    measurement: String,
    points: Vec<Point>,
    /// Per-row `origin_node_id`, parallel to `points`.
    origins: Vec<u64>,
}

struct PreparedFlushWork {
    db: String,
    rp: String,
    batch: PreparedMeasurementBatch,
}

impl FlushServiceImpl {
    pub fn new(
        wal: Arc<dyn WalPort>,
        max_points_per_batch: usize,
        sink: Arc<dyn PointsSinkPort>,
    ) -> Self {
        let effective_limit = if max_points_per_batch == 0 {
            DEFAULT_MAX_POINTS_PER_BATCH
        } else {
            max_points_per_batch.clamp(MIN_BATCH_POINTS, MAX_BATCH_POINTS)
        };

        tracing::info!(
            max_points_per_batch = effective_limit,
            "flush service batch size configured"
        );

        Self {
            wal,
            last_flushed: Arc::new(Mutex::new(0)),
            max_points_per_batch: effective_limit,
            replication_log: None,
            membership: None,
            node_id: 0,
            last_local_wal_seq: std::sync::atomic::AtomicU64::new(0),
            heartbeat_interval_secs: 0,
            heartbeat_miss_threshold: 0,
            truncate_stale_peer_multiplier: 0,
            sink,
            coalesce: std::env::var("HYPERBYTEDB_DISABLE_COALESCE").is_err(),
            sharding_enabled: false,
            shard_map: None,
            sharded_wal_retention_bytes: SHARDED_WAL_RETENTION_BYTES,
        }
    }

    pub fn with_sharding(
        mut self,
        enabled: bool,
        shard_map: Option<Arc<dyn ShardMapPort>>,
        _node_id: u64,
    ) -> Self {
        self.sharding_enabled = enabled;
        self.shard_map = shard_map;
        self
    }

    pub fn with_truncate_heartbeat_policy(
        mut self,
        heartbeat_interval_secs: u64,
        heartbeat_miss_threshold: u64,
        truncate_stale_peer_multiplier: u64,
    ) -> Self {
        self.heartbeat_interval_secs = heartbeat_interval_secs;
        self.heartbeat_miss_threshold = heartbeat_miss_threshold;
        self.truncate_stale_peer_multiplier = truncate_stale_peer_multiplier;
        self
    }

    pub fn with_sharded_wal_retention_bytes(mut self, bytes: u64) -> Self {
        self.sharded_wal_retention_bytes = bytes;
        self
    }

    pub fn with_replication_log(mut self, repl_log: Arc<ReplicationLog>) -> Self {
        self.replication_log = Some(repl_log);
        self
    }

    pub fn with_membership(mut self, node_id: u64, membership: SharedMembership) -> Self {
        self.node_id = node_id;
        self.membership = Some(membership);
        self
    }

    pub async fn run(&self, interval: std::time::Duration, mut shutdown_rx: watch::Receiver<bool>) {
        let mut ticker = tokio::time::interval(interval);
        ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);

        tracing::info!("flush service started, interval = {:?}", interval);
        loop {
            tokio::select! {
                _ = ticker.tick() => {
                    tracing::debug!("flush tick");
                    if let Err(e) = self.flush().await {
                        tracing::error!("flush error: {}", e);
                        counter!("hyperbytedb_flush_errors_total").increment(1);
                    }
                }
                _ = shutdown_rx.changed() => {
                    if *shutdown_rx.borrow() {
                        tracing::info!("flush service received shutdown");
                        break;
                    }
                }
            }
        }
    }

    /// Determine the highest WAL sequence that is safe to truncate.
    ///
    /// In cluster mode we must not truncate past what all peers have acked,
    /// otherwise replicated writes that haven't reached a peer are permanently
    /// lost.  Uses **every** active peer's ack (`get_wal_ack`, missing = 0), not
    /// only peers that already have a `repl_ack` row (see `ReplicationLog::min_wal_ack`).
    ///
    /// When every peer is still at ack 0, apply `MAX_WAL_RETENTION_ENTRIES` as a
    /// safety valve so a totally broken replication path cannot grow WAL forever.
    ///
    /// When some peers have acked and some are still at 0, return 0 so we do not
    /// truncate — lagging peers may still need the WAL for catch-up.
    ///
    /// **Stale peers:** If configured, active peers with `ack == 0` and
    /// `now - last_heartbeat` greater than `heartbeat_interval * miss_threshold * multiplier`
    /// are omitted (they need full sync / ops intervention); truncation then follows the
    /// remaining peers only. Multiplier `0` disables this (strict legacy behavior).
    async fn compute_safe_truncate_seq(&self, chunk_max_seq: u64) -> u64 {
        // Sharded: nothing reads a flushed entry back. Replicas get writes
        // from the replication queue and hinted handoff, and a moving region
        // from chDB plus the unflushed WAL; no peer ever acks the WAL, so the
        // barrier below kept 500k entries, 18-72 GB a pod on n3, and every
        // restart re-flushed all of it. A margin stays behind the flush for
        // the chDB parts the OS has not written back yet, bounded in bytes:
        // a received region chunk is one ~4.5 MB entry, so a margin of 10k
        // entries still held up to 45 GB.
        if self.sharding_enabled {
            return self
                .wal
                .flushed_margin_start(chunk_max_seq, self.sharded_wal_retention_bytes);
        }
        let rl = match self.replication_log {
            Some(ref rl) => rl,
            None => return chunk_max_seq,
        };

        let peer_ids: Vec<u64> = if self.sharding_enabled {
            if let Some(ref map) = self.shard_map {
                match map.snapshot().await {
                    Ok(shard_map) => {
                        let mut peers = std::collections::HashSet::new();
                        for space in shard_map.spaces.values() {
                            for region in &space.regions {
                                if region.peers.contains(&self.node_id) {
                                    for p in &region.peers {
                                        if *p != self.node_id {
                                            peers.insert(*p);
                                        }
                                    }
                                }
                            }
                        }
                        peers.into_iter().collect()
                    }
                    Err(e) => {
                        tracing::warn!(error = %e, "shard map snapshot failed for truncate barrier");
                        Vec::new()
                    }
                }
            } else {
                Vec::new()
            }
        } else if let Some(ref m) = self.membership {
            let membership = m.read().await;
            membership
                .active_peers(self.node_id)
                .into_iter()
                .map(|n| n.node_id)
                .collect()
        } else {
            Vec::new()
        };

        if peer_ids.is_empty() {
            // Cluster replication is enabled but membership has not yet listed
            // active peers (common during bootstrap). Truncating here would delete
            // WAL entries before a joining node can pull them via /internal/sync/wal.
            tracing::debug!(
                chunk_max_seq = chunk_max_seq,
                "replication enabled but no active peers in membership; holding WAL"
            );
            return chunk_max_seq.saturating_sub(MAX_WAL_RETENTION_ENTRIES);
        }

        // If this node has never originated any WAL entries (all data arrived
        // via replication from peers), those peers already have their data and
        // there is nothing to replicate back.  The ack barrier is irrelevant.
        let last_local = self
            .last_local_wal_seq
            .load(std::sync::atomic::Ordering::Relaxed);
        if last_local == 0 {
            tracing::debug!(
                chunk_max_seq = chunk_max_seq,
                "no locally-originated WAL entries; skipping peer ack barrier"
            );
            return chunk_max_seq;
        }

        let effective_peers: Vec<u64> =
            if self.truncate_stale_peer_multiplier > 0 && self.heartbeat_interval_secs > 0 {
                if let Some(ref m) = self.membership {
                    let now = chrono::Utc::now().timestamp();
                    let stale_after = (self.heartbeat_interval_secs as i64)
                        .saturating_mul(self.heartbeat_miss_threshold.max(1) as i64)
                        .saturating_mul(self.truncate_stale_peer_multiplier as i64);
                    let membership = m.read().await;
                    let mut eff = Vec::new();
                    for &pid in &peer_ids {
                        let ack = rl.get_wal_ack(pid).unwrap_or(0);
                        if ack > 0 {
                            eff.push(pid);
                            continue;
                        }
                        if let Some(node) = membership.get_node(pid)
                            && now - node.last_heartbeat > stale_after
                        {
                            tracing::warn!(
                                peer_id = pid,
                                last_heartbeat = node.last_heartbeat,
                                "excluding stale peer (wal ack=0) from truncate barrier"
                            );
                            continue;
                        }
                        eff.push(pid);
                    }
                    eff
                } else {
                    peer_ids
                }
            } else {
                peer_ids
            };

        if effective_peers.is_empty() {
            tracing::debug!(
                chunk_max_seq = chunk_max_seq,
                "no effective replication peers for truncate barrier (all stale or filtered)"
            );
            return chunk_max_seq;
        }

        let (min_ack, max_ack) = match rl.min_max_wal_ack_for_peers(&effective_peers) {
            Ok(v) => v,
            Err(e) => {
                tracing::warn!(error = %e, "failed to read per-peer wal acks, not truncating");
                return 0;
            }
        };

        if max_ack > 0 && min_ack == 0 {
            tracing::debug!(
                chunk_max_seq = chunk_max_seq,
                "replication lag (some peers acked, some still at 0); holding WAL"
            );
            return 0;
        }

        if min_ack > 0 {
            return chunk_max_seq.min(min_ack);
        }

        // All peers at ack 0: bounded retention so WAL cannot grow without bound.
        let oldest_kept = chunk_max_seq.saturating_sub(MAX_WAL_RETENTION_ENTRIES);
        tracing::warn!(
            chunk_max_seq = chunk_max_seq,
            oldest_kept = oldest_kept,
            "peers exist but none have acked; bounded WAL retention for catch-up"
        );
        oldest_kept
    }

    #[allow(clippy::too_many_arguments)]
    async fn flush_prepared_chunk(
        &self,
        prepared: Vec<(u64, PreparedWalSlot)>,
        from_seq: u64,
        cursor: &mut u64,
        total_points_flushed: &mut u64,
        total_entries_processed: &mut u64,
    ) -> Result<(), HyperbytedbError> {
        let chunk_entry_count = prepared.len();
        let chunk_max_seq = prepared.iter().map(|(s, _)| *s).max().unwrap_or(*cursor);
        for (seq, slot) in &prepared {
            let origin = slot.origin_node_id;
            if origin == self.node_id || (self.node_id > 0 && origin == 0) {
                self.last_local_wal_seq
                    .fetch_max(*seq, std::sync::atomic::Ordering::Relaxed);
            }
        }

        let prepare_start = std::time::Instant::now();
        let mut by_meas: BTreeMap<(String, String, String), Vec<PreparedMeasurementBatch>> =
            BTreeMap::new();
        let mut chunk_point_count = 0usize;

        for (_, slot) in prepared {
            for m in slot.measurements {
                chunk_point_count += m.row_count;
                by_meas
                    .entry((
                        slot.database.clone(),
                        slot.retention_policy.clone(),
                        m.measurement.clone(),
                    ))
                    .or_default()
                    .push(m);
            }
        }

        tracing::debug!(
            entries = chunk_entry_count,
            points = chunk_point_count,
            measurements = by_meas.len(),
            from_seq = from_seq,
            to_seq = chunk_max_seq,
            path = "prepared",
            "flushing prepared WAL chunk to chDB"
        );

        let mut work_items: Vec<PreparedFlushWork> = Vec::new();
        for ((db, rp, measurement), batches) in by_meas {
            let merged = if self.coalesce && batches.len() > 1 {
                let coalesce_start = std::time::Instant::now();
                let out = coalesce_prepared_batches(batches)?;
                histogram!("hyperbytedb_flush_arrow_coalesce_seconds")
                    .record(coalesce_start.elapsed().as_secs_f64());
                out.into_iter().collect::<Vec<_>>()
            } else if batches.len() == 1 {
                batches
            } else {
                coalesce_prepared_batches(batches)?
                    .into_iter()
                    .collect::<Vec<_>>()
            };

            for batch in merged {
                let mut offset = 0usize;
                while offset < batch.row_count {
                    let take = (batch.row_count - offset).min(self.max_points_per_batch);
                    let slice = slice_prepared_batch(&batch, offset, take);
                    work_items.push(PreparedFlushWork {
                        db: db.clone(),
                        rp: rp.clone(),
                        batch: slice,
                    });
                    offset += take;
                }
            }
            let _ = measurement;
        }

        histogram!("hyperbytedb_flush_prepare_seconds", "path" => "prepared")
            .record(prepare_start.elapsed().as_secs_f64());

        let sink = self.sink.clone();
        let mut handles = Vec::with_capacity(work_items.len());
        let inserts = Arc::new(tokio::sync::Semaphore::new(FLUSH_INSERT_CONCURRENCY));
        for work in work_items {
            let sink = sink.clone();
            let inserts = inserts.clone();
            handles.push(tokio::spawn(async move {
                let _slot = inserts.acquire_owned().await;
                let count = work.batch.row_count;
                let written = sink
                    .write_prepared_batch(&work.db, &work.rp, &work.batch)
                    .await;
                if unwritable_rows(&written, count) {
                    return Ok(0);
                }
                written?;
                counter!("hyperbytedb_native_rows_written_total", "path" => "prepared")
                    .increment(count as u64);
                Ok::<usize, HyperbytedbError>(count)
            }));
        }

        let sink_start = std::time::Instant::now();
        let mut chunk_points_written = 0u64;
        let mut any_failed = false;
        for handle in handles {
            match handle.await {
                Ok(Ok(count)) => chunk_points_written += count as u64,
                Ok(Err(e)) => {
                    any_failed = true;
                    tracing::error!(error = %e, "prepared flush work item failed");
                }
                Err(e) => {
                    any_failed = true;
                    tracing::error!(error = %e, "prepared sink task panicked");
                }
            }
        }
        histogram!("hyperbytedb_flush_sink_write_seconds", "path" => "prepared")
            .record(sink_start.elapsed().as_secs_f64());

        *total_points_flushed += chunk_points_written;

        // If any sink write failed, hold the WAL at the start of this chunk so the
        // failed entries are retried on the next flush instead of being truncated
        // away (silent data loss). Coalescing merges multiple WAL sequences into a
        // single work item, so the chunk is the finest safe retry granularity.
        let flushed_through = if any_failed {
            from_seq.saturating_sub(1)
        } else {
            chunk_max_seq
        };

        let safe_truncate_seq = self.compute_safe_truncate_seq(flushed_through).await;
        self.wal.truncate_before(safe_truncate_seq + 1).await?;
        if let Some(ref rl) = self.replication_log
            && let Ok(Some(min_mut_ack)) = rl.min_mutation_ack()
            && let Err(e) = rl.truncate_mutations_before(min_mut_ack)
        {
            tracing::warn!(error = %e, "failed to truncate mutation log");
        }

        *self.last_flushed.lock().await = flushed_through;
        self.wal.set_flushed_through(flushed_through);
        *cursor = flushed_through;
        *total_entries_processed += chunk_entry_count as u64;

        if any_failed {
            // Stop this flush cycle (retry next tick). Returning Err also prevents
            // drain() from looping forever on a persistently-failing chunk.
            return Err(HyperbytedbError::Internal(crate::error::ChainedError::new(
                "prepared flush sink write failed; holding WAL for retry",
            )));
        }

        Ok(())
    }

    /// Force an immediate full WAL flush for graceful shutdown / drain.
    /// Blocks until all WAL entries are flushed.
    pub async fn drain(&self) -> Result<(), HyperbytedbError> {
        tracing::info!("draining WAL: forcing full flush");
        loop {
            self.flush().await?;
            let cursor = *self.last_flushed.lock().await;
            let remaining = self.wal.read_range(cursor + 1, 1).await?;
            if remaining.is_empty() {
                tracing::info!("drain complete: all WAL entries flushed");
                break;
            }
        }
        Ok(())
    }

    pub async fn flush(&self) -> Result<(), HyperbytedbError> {
        let mut cursor = *self.last_flushed.lock().await;
        let mut total_points_flushed = 0u64;
        let mut total_entries_processed = 0u64;
        let start = std::time::Instant::now();

        let snapshot_seq = self.wal.last_sequence().await?;
        if snapshot_seq <= cursor {
            return Ok(());
        }

        // Only read sequences captured at flush start (`snapshot_seq`). Entries
        // appended mid-flush stay for the next tick; without this bound the
        // prepared path would `continue` past the snapshot and fall through to a
        // native `read_range` that always returns empty but still seeks RocksDB.
        while cursor < snapshot_seq {
            let from_seq = cursor + 1;

            let wal_read_start = std::time::Instant::now();
            // Bound the native read so a gap in the prepared cache (an entry
            // with no prepared slot — e.g. a peer WAL catch-up append) does not
            // swallow the prepared slots that follow it onto the native path.
            let mut native_limit = WAL_READ_CHUNK;
            if self.wal.arrow_wal_enabled() {
                // `take_prepared_range` is bounded by `snapshot_seq`, so the run
                // it returns is already within the flush window and slots for
                // writes that arrived mid-flush stay cached for the next flush.
                if let Some(prepared) = self
                    .wal
                    .take_prepared_range(from_seq, snapshot_seq, WAL_READ_CHUNK)
                    .await?
                {
                    if !prepared.is_empty() {
                        let wal_read_elapsed = wal_read_start.elapsed();
                        histogram!("hyperbytedb_flush_wal_read_seconds", "path" => "prepared")
                            .record(wal_read_elapsed.as_secs_f64());
                        self.flush_prepared_chunk(
                            prepared,
                            from_seq,
                            &mut cursor,
                            &mut total_points_flushed,
                            &mut total_entries_processed,
                        )
                        .await?;
                        continue;
                    }
                } else if let Some(next) = self.wal.next_prepared_seq(from_seq).await?
                    && next > from_seq
                {
                    native_limit = ((next - from_seq) as usize).min(WAL_READ_CHUNK);
                }
            }

            let entries = self.wal.read_range(from_seq, native_limit).await?;
            let wal_read_elapsed = wal_read_start.elapsed();
            histogram!("hyperbytedb_flush_wal_read_seconds", "path" => "native")
                .record(wal_read_elapsed.as_secs_f64());
            if entries.is_empty() {
                break;
            }

            let budget = self.max_points_per_batch * NATIVE_CHUNK_BATCHES;
            let mut taken_points = 0usize;
            let entries: Vec<_> = entries
                .into_iter()
                .filter(|(seq, _)| *seq <= snapshot_seq)
                .take_while(|(_, e)| {
                    // Always take one entry, then stop at the budget.
                    let take = taken_points == 0 || taken_points < budget;
                    taken_points += e.points.len();
                    take
                })
                .collect();
            if entries.is_empty() {
                break;
            }

            let chunk_entry_count = entries.len();
            let chunk_max_seq = entries.iter().map(|(s, _)| *s).max().unwrap_or(cursor);
            let prepare_start = std::time::Instant::now();
            // Group by (db, rp, measurement) only. `origin_node_id` is carried
            // per-row (parallel to points) rather than being part of the key,
            // so rows from different cluster nodes land in ONE insert per
            // measurement instead of fanning out one insert per origin.
            let mut by_meas: MeasurementBatchMap = BTreeMap::new();
            let mut chunk_point_count = 0usize;

            for (seq, entry) in entries {
                let origin = entry.origin_node_id;
                if origin == self.node_id || (self.node_id > 0 && origin == 0) {
                    self.last_local_wal_seq
                        .fetch_max(seq, std::sync::atomic::Ordering::Relaxed);
                }
                // Direct client writes carry origin 0; record them as this node.
                let eff_origin = if origin == 0 { self.node_id } else { origin };
                for point in entry.points {
                    let key = (
                        entry.database.clone(),
                        entry.retention_policy.clone(),
                        point.measurement.clone(),
                    );
                    let slot = by_meas.entry(key).or_default();
                    slot.0.push(point);
                    slot.1.push(eff_origin);
                    chunk_point_count += 1;
                }
            }

            tracing::debug!(
                entries = chunk_entry_count,
                points = chunk_point_count,
                measurements = by_meas.len(),
                from_seq = from_seq,
                to_seq = chunk_max_seq,
                "flushing WAL chunk to chDB"
            );

            let mut work_items: Vec<FlushWork> = Vec::new();
            for ((db, rp, measurement), (mut points, mut origins)) in by_meas {
                // Telegraf / columnar writers often emit several lines that share
                // the same tag set and timestamp but carry disjoint field sets.
                // Coalesce over the full measurement batch *before* splitting into
                // max_points_per_batch chunks so partial rows are never separated.
                if self.coalesce {
                    let coalesced_start = std::time::Instant::now();
                    if let Some((merged_points, merged_origins)) =
                        coalesce_points_and_origins(&points, &origins)
                    {
                        points = merged_points;
                        origins = merged_origins;
                    }
                    histogram!("hyperbytedb_flush_coalesce_points_seconds")
                        .record(coalesced_start.elapsed().as_secs_f64());
                }
                // No Rust-side sort: chDB re-sorts every inserted block by the
                // table's ORDER BY (tags, time) key, so a time-only pre-sort here
                // is redundant work. Slice points/origins into
                // max_points_per_batch chunks in parallel by moving (no clone).
                while !points.is_empty() {
                    let take = points.len().min(self.max_points_per_batch);
                    let rest_points = points.split_off(take);
                    let rest_origins = origins.split_off(take);
                    work_items.push(FlushWork {
                        db: db.clone(),
                        rp: rp.clone(),
                        measurement: measurement.clone(),
                        points,
                        origins,
                    });
                    points = rest_points;
                    origins = rest_origins;
                }
            }

            histogram!("hyperbytedb_flush_prepare_seconds")
                .record(prepare_start.elapsed().as_secs_f64());
            let sink = self.sink.clone();
            let mut handles = Vec::with_capacity(work_items.len());
            let chunk_min_seq = from_seq;
            let inserts = Arc::new(tokio::sync::Semaphore::new(FLUSH_INSERT_CONCURRENCY));
            for (idx, work) in work_items.into_iter().enumerate() {
                let sink = sink.clone();
                let ingest_seq_base = chunk_min_seq.saturating_add(idx as u64);
                let inserts = inserts.clone();
                handles.push(tokio::spawn(async move {
                    let _slot = inserts.acquire_owned().await;
                    let count = work.points.len();
                    tracing::debug!(
                        db = %work.db,
                        rp = %work.rp,
                        measurement = %work.measurement,
                        points = count,
                        "writing native chDB rows"
                    );
                    let written = sink
                        .write_points(
                            &work.db,
                            &work.rp,
                            &work.measurement,
                            &work.origins,
                            ingest_seq_base,
                            &work.points,
                        )
                        .await;
                    if unwritable_rows(&written, count) {
                        return Ok(0);
                    }
                    written?;
                    counter!("hyperbytedb_native_rows_written_total", "path" => "native")
                        .increment(count as u64);
                    Ok::<usize, HyperbytedbError>(count)
                }));
            }
            let sink_start = std::time::Instant::now();
            let mut chunk_points_written = 0u64;
            let mut any_failed = false;
            for handle in handles {
                match handle.await {
                    Ok(Ok(count)) => chunk_points_written += count as u64,
                    Ok(Err(e)) => {
                        any_failed = true;
                        tracing::error!(error = %e, "native flush work item failed");
                    }
                    Err(e) => {
                        any_failed = true;
                        tracing::error!(error = %e, "native flush task panicked");
                    }
                }
            }
            histogram!("hyperbytedb_flush_sink_write_seconds")
                .record(sink_start.elapsed().as_secs_f64());
            total_points_flushed += chunk_points_written;

            // If any sink write failed, hold the WAL at the start of this chunk so the
            // failed entries are retried on the next flush instead of being truncated
            // away (silent data loss). Coalescing merges multiple WAL sequences into a
            // single work item, so the chunk is the finest safe retry granularity.
            let flushed_through = if any_failed {
                from_seq.saturating_sub(1)
            } else {
                chunk_max_seq
            };

            let safe_truncate_seq = self.compute_safe_truncate_seq(flushed_through).await;

            self.wal.truncate_before(safe_truncate_seq + 1).await?;
            if let Some(ref rl) = self.replication_log
                && let Ok(Some(min_mut_ack)) = rl.min_mutation_ack()
                && let Err(e) = rl.truncate_mutations_before(min_mut_ack)
            {
                tracing::warn!(error = %e, "failed to truncate mutation log");
            }

            *self.last_flushed.lock().await = flushed_through;
            self.wal.set_flushed_through(flushed_through);
            cursor = flushed_through;
            total_entries_processed += chunk_entry_count as u64;

            if any_failed {
                // Stop this flush cycle (retry next tick). Returning Err also prevents
                // drain() from looping forever on a persistently-failing chunk.
                return Err(HyperbytedbError::Internal(crate::error::ChainedError::new(
                    "native flush sink write failed; holding WAL for retry",
                )));
            }
        }

        if total_entries_processed > 0 {
            let elapsed = start.elapsed();
            histogram!("hyperbytedb_flush_duration_seconds").record(elapsed.as_secs_f64());
            counter!("hyperbytedb_flush_points_total").increment(total_points_flushed);
            counter!("hyperbytedb_flush_runs_total").increment(1);
            tracing::debug!(
                entries = total_entries_processed,
                points = total_points_flushed,
                elapsed_ms = elapsed.as_millis() as u64,
                "flush complete"
            );
        }
        gauge!("hyperbytedb_wal_last_sequence").set(*self.last_flushed.lock().await as f64);

        Ok(())
    }
}

fn slice_prepared_batch(
    batch: &PreparedMeasurementBatch,
    offset: usize,
    len: usize,
) -> PreparedMeasurementBatch {
    use std::sync::Arc;

    let sliced = batch.batch.slice(offset, len);
    PreparedMeasurementBatch {
        measurement: batch.measurement.clone(),
        table_name: batch.table_name.clone(),
        series_table_name: batch.series_table_name.clone(),
        batch: Arc::new(sliced),
        row_count: len,
        min_time: batch.min_time,
        max_time: batch.max_time,
        new_series_batch: if offset == 0 {
            batch.new_series_batch.clone()
        } else {
            None
        },
    }
}

#[async_trait::async_trait]
impl FlushPort for FlushServiceImpl {
    async fn drain(&self) -> Result<(), HyperbytedbError> {
        FlushServiceImpl::drain(self).await
    }
}

/// Rows the sink refuses for good: writes to a materialized view
/// destination. Held for retry they would hold the WAL forever, so no later
/// write on the node would flush either (n3: moved destination regions put
/// such rows in the WAL). Logged, counted and dropped instead.
fn unwritable_rows<T>(written: &Result<T, HyperbytedbError>, rows: usize) -> bool {
    let Err(HyperbytedbError::WriteToMaterializedDestination(measurement)) = written else {
        return false;
    };
    tracing::error!(
        %measurement,
        rows,
        "dropping rows a materialized view destination refuses; retrying cannot succeed"
    );
    counter!("hyperbytedb_flush_rows_dropped_total", "reason" => "materialized_destination")
        .increment(rows as u64);
    true
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use async_trait::async_trait;
    use tempfile::TempDir;

    use crate::adapters::wal::rocksdb_wal::{RocksDbWal, RocksDbWalOptions};
    use crate::domain::point::{FieldValue, Point};
    use crate::domain::wal::WalEntry;
    use crate::error::HyperbytedbError;
    use crate::ports::points_sink::{PointsSinkPort, WriteAck};
    use crate::ports::wal::{WalFormat, WalPort};

    use super::FlushServiceImpl;

    /// Native sink whose write always fails, to exercise the flush error path.
    struct FailingSink;

    #[async_trait]
    impl PointsSinkPort for FailingSink {
        async fn write_points(
            &self,
            _db: &str,
            _rp: &str,
            _measurement: &str,
            _origins: &[u64],
            _ingest_seq_base: u64,
            _points: &[Point],
        ) -> Result<WriteAck, HyperbytedbError> {
            Err(HyperbytedbError::Internal(crate::error::ChainedError::new(
                "simulated chDB write failure",
            )))
        }
    }

    fn entry() -> WalEntry {
        let mut tags = std::collections::BTreeMap::new();
        tags.insert("host".to_string(), "h1".to_string());
        let mut fields = std::collections::BTreeMap::new();
        fields.insert("v".to_string(), FieldValue::Float(1.0));
        WalEntry {
            database: "db".into(),
            retention_policy: "autogen".into(),
            points: vec![Point {
                measurement: "cpu".into(),
                tags,
                fields,
                timestamp: 1_700_000_000_000_000_000,
                series_id_cache: Default::default(),
            }],
            origin_node_id: 0,
        }
    }

    /// Regression: a failed sink write must hold the WAL (retry next flush)
    /// rather than truncating the failed chunk away — otherwise acknowledged
    /// writes are silently lost on any transient chDB error.
    #[tokio::test]
    async fn failed_sink_write_holds_wal_instead_of_dropping_data() {
        let tmp = TempDir::new().unwrap();
        let wal: Arc<dyn WalPort> = Arc::new(
            RocksDbWal::open_with_options(
                tmp.path(),
                RocksDbWalOptions {
                    wal_format: WalFormat::Bincode,
                    arrow_wal_enabled: false,
                },
            )
            .unwrap(),
        );
        let seq = wal.append(entry()).await.unwrap();

        let flush = FlushServiceImpl::new(wal.clone(), 0, Arc::new(FailingSink));
        let result = flush.flush().await;

        assert!(
            result.is_err(),
            "flush must surface the sink failure, not swallow it"
        );
        let remaining = wal.read_range(seq, 16).await.unwrap();
        assert_eq!(
            remaining.len(),
            1,
            "failed chunk must stay in the WAL for retry, not be truncated"
        );
        assert!(
            wal.flushed_through() < seq,
            "a failed chunk must not be recorded as flushed"
        );
    }

    /// Native sink that refuses `cpu_1m` as a materialized view destination
    /// and counts the points it writes for anything else.
    #[derive(Default)]
    struct DestinationRefusingSink {
        written: std::sync::atomic::AtomicUsize,
    }

    #[async_trait]
    impl PointsSinkPort for DestinationRefusingSink {
        async fn write_points(
            &self,
            _db: &str,
            _rp: &str,
            measurement: &str,
            _origins: &[u64],
            _ingest_seq_base: u64,
            points: &[Point],
        ) -> Result<WriteAck, HyperbytedbError> {
            if measurement == "cpu_1m" {
                return Err(HyperbytedbError::WriteToMaterializedDestination(
                    measurement.into(),
                ));
            }
            self.written
                .fetch_add(points.len(), std::sync::atomic::Ordering::SeqCst);
            Ok(WriteAck {
                min_time: 0,
                max_time: 0,
                row_count: points.len(),
            })
        }
    }

    /// Rows a materialized view destination refuses can never be written, so
    /// they must not hold the WAL: on n3 they stopped every later write on
    /// the node from flushing.
    #[tokio::test]
    async fn rows_a_view_destination_refuses_do_not_hold_the_wal() {
        let tmp = TempDir::new().unwrap();
        let wal: Arc<dyn WalPort> = Arc::new(
            RocksDbWal::open_with_options(
                tmp.path(),
                RocksDbWalOptions {
                    wal_format: WalFormat::Bincode,
                    arrow_wal_enabled: false,
                },
            )
            .unwrap(),
        );
        let mut refused = entry();
        refused.points[0].measurement = "cpu_1m".into();
        wal.append(refused).await.unwrap();
        let last = wal.append(entry()).await.unwrap();

        let sink = Arc::new(DestinationRefusingSink::default());
        let flush = FlushServiceImpl::new(wal.clone(), 0, sink.clone());
        flush.flush().await.unwrap();

        assert_eq!(
            sink.written.load(std::sync::atomic::Ordering::SeqCst),
            1,
            "the row after the refused one must be written"
        );
        assert!(
            wal.flushed_through() >= last,
            "the WAL must move past the refused row"
        );
    }

    /// In a sharded cluster the WAL keeps only a margin behind the flush,
    /// bounded in bytes.
    ///
    /// It waited for peer acks that nothing sends, so it kept 500k entries:
    /// 18 GB on most n3 pods and 72 GB on one. A margin of 10k entries then
    /// still held 32-45 GB, since a received region chunk is one big entry.
    #[tokio::test]
    async fn a_sharded_wal_keeps_only_a_margin_behind_the_flush() {
        use crate::adapters::cluster::replication_log::ReplicationLog;
        use crate::adapters::sharding::rocksdb_shard_map::RocksDbShardMap;
        use crate::domain::cluster::membership::{ClusterMembership, new_shared};
        use crate::domain::sharding::{MeasurementKey, ShardMapOp, ShardRegion};
        use crate::ports::sharding::ShardMapPort;

        let tmp = TempDir::new().unwrap();
        let wal: Arc<dyn WalPort> = Arc::new(
            RocksDbWal::open_with_options(
                tmp.path().join("wal"),
                RocksDbWalOptions {
                    wal_format: WalFormat::Bincode,
                    arrow_wal_enabled: false,
                },
            )
            .unwrap(),
        );
        std::fs::create_dir_all(tmp.path().join("meta")).unwrap();
        let map = Arc::new(RocksDbShardMap::open(&tmp.path().join("meta"), true).unwrap());
        map.apply_op(ShardMapOp::BootstrapMeasurement {
            key: MeasurementKey::new("db", "autogen", "cpu"),
            region: ShardRegion {
                region_id: 1,
                start: 0,
                end: u64::MAX,
                epoch: Default::default(),
                peers: vec![1, 2],
                primary: 1,
                last_split_at: 0,
                transfer_verified: None,
                transfer_first_seen: None,
                state: Default::default(),
                staged_from: None,
                staging_peers: Vec::new(),
            },
        })
        .await
        .unwrap();
        // Node 2 shares a region with this node and has never acked.
        let flush = FlushServiceImpl::new(wal.clone(), 0, Arc::new(CountingSink::default()))
            .with_replication_log(Arc::new(
                ReplicationLog::open(tmp.path().join("repl")).unwrap(),
            ))
            .with_membership(1, new_shared(ClusterMembership::new()))
            .with_sharding(true, Some(map as Arc<dyn ShardMapPort>), 1);
        // A budget of two and a half entries keeps the newest two.
        let one =
            crate::adapters::wal::wal_ipc::encode_wal_value(WalFormat::Bincode, None, &entry())
                .unwrap()
                .len() as u64;
        let flush = flush.with_sharded_wal_retention_bytes(one * 5 / 2);
        for _ in 0..5 {
            wal.append(entry()).await.unwrap();
        }

        flush.flush().await.unwrap();

        let kept: Vec<u64> = wal
            .read_range(1, 16)
            .await
            .unwrap()
            .into_iter()
            .map(|(seq, _)| seq)
            .collect();
        assert_eq!(kept, vec![4, 5]);
    }

    /// A region copy reads only WAL entries past this watermark and takes the
    /// rest from chDB, so a flush must record it.
    #[tokio::test]
    async fn a_flush_records_how_far_the_wal_is_flushed() {
        let tmp = TempDir::new().unwrap();
        let wal: Arc<dyn WalPort> = Arc::new(
            RocksDbWal::open_with_options(
                tmp.path(),
                RocksDbWalOptions {
                    wal_format: WalFormat::Bincode,
                    arrow_wal_enabled: false,
                },
            )
            .unwrap(),
        );
        wal.append(entry()).await.unwrap();
        let seq = wal.append(entry()).await.unwrap();

        let flush = FlushServiceImpl::new(wal.clone(), 0, Arc::new(CountingSink::default()));
        flush.flush().await.unwrap();

        assert_eq!(wal.flushed_through(), seq);
    }

    /// Sink that records how many writes run at once and how many points are
    /// in flight, slowly enough for concurrent writes to overlap.
    #[derive(Default)]
    struct CountingSink {
        active: std::sync::atomic::AtomicUsize,
        peak_active: std::sync::atomic::AtomicUsize,
        points: std::sync::atomic::AtomicUsize,
        written: std::sync::atomic::AtomicUsize,
    }

    #[async_trait]
    impl PointsSinkPort for CountingSink {
        async fn write_points(
            &self,
            _db: &str,
            _rp: &str,
            _measurement: &str,
            _origins: &[u64],
            _ingest_seq_base: u64,
            points: &[Point],
        ) -> Result<WriteAck, HyperbytedbError> {
            use std::sync::atomic::Ordering::SeqCst;
            let now = self.active.fetch_add(1, SeqCst) + 1;
            self.peak_active.fetch_max(now, SeqCst);
            tokio::time::sleep(std::time::Duration::from_millis(5)).await;
            self.active.fetch_sub(1, SeqCst);
            self.points.fetch_add(points.len(), SeqCst);
            self.written.fetch_add(1, SeqCst);
            Ok(WriteAck {
                min_time: 0,
                max_time: 0,
                row_count: points.len(),
            })
        }
    }

    fn big_entry(n: usize, offset: usize) -> WalEntry {
        let mut e = entry();
        e.points = (0..n)
            .map(|i| {
                let mut p = e.points[0].clone();
                p.tags.insert("host".into(), format!("h{}", offset + i));
                p
            })
            .collect();
        e
    }

    /// A backlog flushes in bounded steps: chunks of a few batches' points and
    /// at most FLUSH_INSERT_CONCURRENCY inserts at once. Unbounded, a node
    /// restarting with a backlog spawned every slice of a 5,000-entry chunk at
    /// once and was OOM-killed within seconds, on every restart.
    #[tokio::test]
    async fn a_backlog_flushes_in_bounded_steps() {
        let tmp = TempDir::new().unwrap();
        let wal: Arc<dyn WalPort> = Arc::new(
            RocksDbWal::open_with_options(
                tmp.path(),
                RocksDbWalOptions {
                    wal_format: WalFormat::Bincode,
                    arrow_wal_enabled: false,
                },
            )
            .unwrap(),
        );
        for i in 0..40 {
            wal.append(big_entry(10_000, i * 10_000)).await.unwrap();
        }
        let sink = Arc::new(CountingSink::default());
        let flush = FlushServiceImpl::new(wal.clone(), 10_000, sink.clone());
        flush.flush().await.unwrap();

        use std::sync::atomic::Ordering::SeqCst;
        assert_eq!(sink.points.load(SeqCst), 400_000, "every point flushed");
        assert!(
            sink.peak_active.load(SeqCst) <= super::FLUSH_INSERT_CONCURRENCY,
            "peak concurrent inserts {}",
            sink.peak_active.load(SeqCst)
        );
        assert!(
            wal.read_range(1, 16).await.unwrap().is_empty(),
            "WAL truncated"
        );
    }

    /// Many measurements in one chunk still insert at most
    /// FLUSH_INSERT_CONCURRENCY at a time.
    #[tokio::test]
    async fn many_measurements_insert_a_few_at_a_time() {
        let tmp = TempDir::new().unwrap();
        let wal: Arc<dyn WalPort> = Arc::new(
            RocksDbWal::open_with_options(
                tmp.path(),
                RocksDbWalOptions {
                    wal_format: WalFormat::Bincode,
                    arrow_wal_enabled: false,
                },
            )
            .unwrap(),
        );
        for i in 0..20 {
            let mut e = big_entry(500, i * 500);
            for p in &mut e.points {
                p.measurement = format!("m{i}");
            }
            wal.append(e).await.unwrap();
        }
        let sink = Arc::new(CountingSink::default());
        let flush = FlushServiceImpl::new(wal.clone(), 10_000, sink.clone());
        flush.flush().await.unwrap();

        use std::sync::atomic::Ordering::SeqCst;
        assert_eq!(sink.written.load(SeqCst), 20, "one insert per measurement");
        assert!(
            sink.peak_active.load(SeqCst) <= super::FLUSH_INSERT_CONCURRENCY,
            "peak concurrent inserts {}",
            sink.peak_active.load(SeqCst)
        );
    }
}
