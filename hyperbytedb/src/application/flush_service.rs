use metrics::{counter, gauge, histogram};
use std::collections::BTreeMap;
use std::sync::Arc;
use tokio::sync::Mutex;
use tokio::sync::watch;

use crate::adapters::cluster::replication_log::ReplicationLog;
use crate::application::system_trace;
use crate::domain::cluster::membership::SharedMembership;
use crate::domain::point::Point;
use crate::error::HyperbytedbError;
use crate::ports::flush::FlushPort;
use crate::ports::points_sink::PointsSinkPort;
use crate::ports::wal::WalPort;

const WAL_READ_CHUNK: usize = 5_000;
const DEFAULT_MAX_POINTS_PER_BATCH: usize = 50_000;
const MIN_BATCH_POINTS: usize = 10_000;
const MAX_BATCH_POINTS: usize = 500_000;
/// Maximum number of unflushed WAL entries to retain when peers are lagging.
/// Beyond this limit we truncate anyway to prevent unbounded WAL growth.
const MAX_WAL_RETENTION_ENTRIES: u64 = 500_000;

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
}

struct FlushWork {
    db: String,
    rp: String,
    measurement: String,
    points: Vec<Point>,
    /// Per-row `origin_node_id`, parallel to `points`.
    origins: Vec<u64>,
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
        }
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
        let rl = match self.replication_log {
            Some(ref rl) => rl,
            None => return chunk_max_seq,
        };

        let peer_ids: Vec<u64> = if let Some(ref m) = self.membership {
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
            return chunk_max_seq;
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

        let run_span = system_trace::flush_run_span(snapshot_seq, cursor);
        let run_start = system_trace::start_timer();

        loop {
            let chunk_start = system_trace::start_timer();
            let from_seq = cursor + 1;

            let wal_read_start = std::time::Instant::now();
            let entries = self.wal.read_range(from_seq, WAL_READ_CHUNK).await?;
            let wal_read_elapsed = wal_read_start.elapsed();
            histogram!("hyperbytedb_flush_wal_read_seconds").record(wal_read_elapsed.as_secs_f64());
            if entries.is_empty() {
                break;
            }

            let entries: Vec<_> = entries
                .into_iter()
                .filter(|(seq, _)| *seq <= snapshot_seq)
                .collect();
            if entries.is_empty() {
                break;
            }

            let chunk_entry_count = entries.len();
            let chunk_max_seq = entries.iter().map(|(s, _)| *s).max().unwrap_or(cursor);
            let chunk_span = system_trace::flush_chunk_span(from_seq, chunk_max_seq);
            let _chunk_guard = chunk_span.enter();

            system_trace::record_phase("wal_read_us", wal_read_elapsed);

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

            tracing::info!(
                entries = chunk_entry_count,
                points = chunk_point_count,
                measurements = by_meas.len(),
                from_seq = from_seq,
                to_seq = chunk_max_seq,
                "flushing WAL chunk to chDB"
            );

            let measurement_count = by_meas.len();
            let mut work_items: Vec<FlushWork> = Vec::new();
            for ((db, rp, measurement), (mut points, mut origins)) in by_meas {
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
            system_trace::record_phase("prepare_us", prepare_start.elapsed());
            system_trace::record_u64("entries", chunk_entry_count as u64);
            system_trace::record_u64("points", chunk_point_count as u64);
            system_trace::record_u64("measurements", measurement_count as u64);
            system_trace::record_u64("batches", work_items.len() as u64);

            let sink = self.sink.clone();
            let mut handles = Vec::with_capacity(work_items.len());
            let chunk_min_seq = from_seq;
            for (idx, work) in work_items.into_iter().enumerate() {
                let sink = sink.clone();
                let ingest_seq_base = chunk_min_seq.saturating_add(idx as u64);
                handles.push(tokio::spawn(async move {
                    let count = work.points.len();
                    tracing::info!(
                        db = %work.db,
                        rp = %work.rp,
                        measurement = %work.measurement,
                        points = count,
                        "writing native chDB rows"
                    );
                    let _ack = sink
                        .write_points(
                            &work.db,
                            &work.rp,
                            &work.measurement,
                            &work.origins,
                            ingest_seq_base,
                            &work.points,
                        )
                        .await?;
                    counter!("hyperbytedb_native_rows_written_total").increment(count as u64);
                    Ok::<usize, HyperbytedbError>(count)
                }));
            }
            let sink_start = std::time::Instant::now();
            let mut chunk_points_written = 0u64;
            for handle in handles {
                let count = handle.await.map_err(|e| {
                    HyperbytedbError::Internal(format!("native sink task panicked: {e}"))
                })?;
                chunk_points_written += count? as u64;
            }
            histogram!("hyperbytedb_flush_sink_write_seconds")
                .record(sink_start.elapsed().as_secs_f64());
            system_trace::record_phase("sink_write_us", sink_start.elapsed());

            total_points_flushed += chunk_points_written;

            let truncate_start = std::time::Instant::now();
            let safe_truncate_seq = self.compute_safe_truncate_seq(chunk_max_seq).await;

            self.wal.truncate_before(safe_truncate_seq + 1).await?;
            system_trace::record_phase("truncate_us", truncate_start.elapsed());
            system_trace::record_u64("safe_truncate_seq", safe_truncate_seq);

            if let Some(ref rl) = self.replication_log
                && let Ok(Some(min_mut_ack)) = rl.min_mutation_ack()
                && let Err(e) = rl.truncate_mutations_before(min_mut_ack)
            {
                tracing::warn!(error = %e, "failed to truncate mutation log");
            }

            *self.last_flushed.lock().await = chunk_max_seq;
            cursor = chunk_max_seq;
            total_entries_processed += chunk_entry_count as u64;

            system_trace::finish_span(&chunk_span, chunk_start, "flush chunk complete");
        }

        if total_entries_processed > 0 {
            let elapsed = start.elapsed();
            histogram!("hyperbytedb_flush_duration_seconds").record(elapsed.as_secs_f64());
            counter!("hyperbytedb_flush_points_total").increment(total_points_flushed);
            counter!("hyperbytedb_flush_runs_total").increment(1);
            if system_trace::is_enabled() {
                run_span.record("entries", total_entries_processed);
                run_span.record("points", total_points_flushed);
            }
            system_trace::finish_span(&run_span, run_start, "flush run complete");
            tracing::info!(
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

#[async_trait::async_trait]
impl FlushPort for FlushServiceImpl {
    async fn drain(&self) -> Result<(), HyperbytedbError> {
        FlushServiceImpl::drain(self).await
    }
}
