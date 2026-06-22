//! Opt-in detailed performance tracing for Loki logs and Tempo spans.
//!
//! **Hot-path contract:** when disabled (the default), this module must not
//! call [`Instant::now`], allocate spans, or emit log lines. Every entry point
//! checks a single relaxed atomic first (inlined).
//!
//! When enabled, phase summary lines are emitted at **debug** level so the
//! default `info` filter stays quiet under write load. Enable via
//! `[logging].detailed_trace = true` or `HYPERBYTEDB__LOGGING__DETAILED_TRACE=true`,
//! and set `logging.level=debug` (or `RUST_LOG=debug`) to see the lines.
//!
//! ```logql
//! {namespace="hyperbytedb"} | json | system_trace="true"
//! ```

use std::sync::atomic::{AtomicBool, Ordering};
use std::time::{Duration, Instant};

static ENABLED: AtomicBool = AtomicBool::new(false);

/// Install the global on/off switch (called once from [`crate::application::runtime::tracing_init`]).
pub fn set_enabled(enabled: bool) {
    ENABLED.store(enabled, Ordering::Relaxed);
}

/// Fast gate for hot paths. When `false`, callers must skip timers and spans.
#[inline(always)]
pub fn is_enabled() -> bool {
    ENABLED.load(Ordering::Relaxed)
}

/// Captures elapsed time only when [`is_enabled`] is true.
pub struct PhaseTimer(Option<Instant>);

impl PhaseTimer {
    #[inline(always)]
    pub fn start() -> Self {
        if is_enabled() {
            Self(Some(Instant::now()))
        } else {
            Self(None)
        }
    }

    /// Record a phase on the current span and emit a debug line, then restart.
    #[inline]
    pub fn record_phase(&mut self, phase: &'static str) {
        if let Some(start) = self.0.take() {
            record_phase(phase, start.elapsed());
        }
        self.0 = is_enabled().then(Instant::now);
    }

    /// Record a phase without restarting (e.g. final phase before [`finish_span`]).
    #[inline]
    pub fn record_phase_final(&mut self, phase: &'static str) {
        if let Some(start) = self.0.take() {
            record_phase(phase, start.elapsed());
        }
    }

    #[inline(always)]
    pub fn restart(&mut self) {
        self.0 = is_enabled().then(Instant::now);
    }
}

/// Record a phase duration on the active span and emit a per-phase debug line.
#[inline]
pub fn record_phase(phase: &'static str, elapsed: Duration) {
    if !is_enabled() {
        return;
    }
    let us = elapsed.as_micros() as u64;
    tracing::Span::current().record(phase, us);
    tracing::debug!(
        system_trace = true,
        phase = phase,
        duration_us = us,
        "system trace phase"
    );
}

#[inline]
pub fn record_u64(field: &'static str, value: u64) {
    if !is_enabled() {
        return;
    }
    tracing::Span::current().record(field, value);
}

#[inline]
pub fn record_i64(field: &'static str, value: i64) {
    if !is_enabled() {
        return;
    }
    tracing::Span::current().record(field, value);
}

#[inline]
pub fn record_usize(field: &'static str, value: usize) {
    record_u64(field, value as u64);
}

#[inline]
pub fn record_str(field: &'static str, value: &str) {
    if !is_enabled() {
        return;
    }
    tracing::Span::current().record(field, tracing::field::display(value));
}

#[inline]
pub fn record_bool(field: &'static str, value: bool) {
    if !is_enabled() {
        return;
    }
    tracing::Span::current().record(field, value);
}

#[inline]
fn disabled_span() -> tracing::Span {
    tracing::Span::none()
}

/// Top-level span for `POST /write` (client-facing ingest).
#[inline]
pub fn client_write_span(db: &str, format: &str, payload_bytes: usize) -> tracing::Span {
    if !is_enabled() {
        return disabled_span();
    }
    tracing::info_span!(
        "write_path",
        system_trace = true,
        write_path = true,
        event = "client_write",
        db = %db,
        format = format,
        payload_bytes = payload_bytes,
        point_count = tracing::field::Empty,
        wal_seq = tracing::field::Empty,
        cluster_check_us = tracing::field::Empty,
        auth_us = tracing::field::Empty,
        gzip_us = tracing::field::Empty,
        ingest_us = tracing::field::Empty,
        metadata_lookup_us = tracing::field::Empty,
        parse_us = tracing::field::Empty,
        metadata_register_us = tracing::field::Empty,
        wal_append_us = tracing::field::Empty,
        replication_us = tracing::field::Empty,
        total_us = tracing::field::Empty,
    )
}

/// Top-level span for `/query` (GET or POST).
#[inline]
pub fn client_query_span(db: &str, query_len: usize, stmt_type: &str) -> tracing::Span {
    if !is_enabled() {
        return disabled_span();
    }
    tracing::info_span!(
        "query_path",
        system_trace = true,
        event = "client_query",
        db = %db,
        stmt_type = stmt_type,
        query_len = query_len,
        parse_us = tracing::field::Empty,
        execute_us = tracing::field::Empty,
        format_us = tracing::field::Empty,
        total_us = tracing::field::Empty,
    )
}

/// Span for query execution inside the service layer.
#[inline]
pub fn query_execute_span(db: &str, stmt_count: usize) -> tracing::Span {
    if !is_enabled() {
        return disabled_span();
    }
    tracing::info_span!(
        "query_execute",
        system_trace = true,
        event = "query_execute",
        db = %db,
        stmt_count = stmt_count,
        parse_us = tracing::field::Empty,
        authorize_us = tracing::field::Empty,
        statement_us = tracing::field::Empty,
        total_us = tracing::field::Empty,
    )
}

/// Span for a single chDB SQL execution.
#[inline]
pub fn chdb_sql_span(sql_len: usize) -> tracing::Span {
    if !is_enabled() {
        return disabled_span();
    }
    tracing::info_span!(
        "chdb_sql",
        system_trace = true,
        event = "chdb_sql",
        sql_len = sql_len,
        semaphore_wait_us = tracing::field::Empty,
        chdb_execute_us = tracing::field::Empty,
        result_bytes = tracing::field::Empty,
        total_us = tracing::field::Empty,
    )
}

/// Span for applying a replicated write batch on a peer.
#[inline]
pub fn replicate_apply_span(db: &str, origin_node_id: u64, payload_bytes: usize) -> tracing::Span {
    if !is_enabled() {
        return disabled_span();
    }
    tracing::info_span!(
        "write_path",
        system_trace = true,
        write_path = true,
        event = "replicate_apply",
        db = %db,
        origin_node_id = origin_node_id,
        payload_bytes = payload_bytes,
        point_count = tracing::field::Empty,
        wal_seq = tracing::field::Empty,
        parse_us = tracing::field::Empty,
        metadata_register_us = tracing::field::Empty,
        wal_append_us = tracing::field::Empty,
        total_us = tracing::field::Empty,
    )
}

/// Span for outbound replication dispatch (cluster coordinator).
#[inline]
pub fn replication_dispatch_span(wal_seq: u64, mode: &str) -> tracing::Span {
    if !is_enabled() {
        return disabled_span();
    }
    tracing::info_span!(
        "replication_dispatch",
        system_trace = true,
        event = "replication_dispatch",
        wal_seq = wal_seq,
        mode = mode,
        dispatch_us = tracing::field::Empty,
        total_us = tracing::field::Empty,
    )
}

/// Span for one flush-service tick (may process multiple WAL chunks).
#[inline]
pub fn flush_run_span(snapshot_seq: u64, cursor: u64) -> tracing::Span {
    if !is_enabled() {
        return disabled_span();
    }
    tracing::info_span!(
        "write_path",
        system_trace = true,
        write_path = true,
        event = "flush_run",
        snapshot_seq = snapshot_seq,
        from_seq = cursor + 1,
        entries = tracing::field::Empty,
        points = tracing::field::Empty,
        total_us = tracing::field::Empty,
    )
}

/// Span for one WAL chunk inside a flush run.
#[inline]
pub fn flush_chunk_span(from_seq: u64, to_seq: u64) -> tracing::Span {
    if !is_enabled() {
        return disabled_span();
    }
    tracing::info_span!(
        "write_path",
        system_trace = true,
        write_path = true,
        event = "flush_chunk",
        from_seq = from_seq,
        to_seq = to_seq,
        entries = tracing::field::Empty,
        points = tracing::field::Empty,
        measurements = tracing::field::Empty,
        batches = tracing::field::Empty,
        wal_read_us = tracing::field::Empty,
        prepare_us = tracing::field::Empty,
        sink_write_us = tracing::field::Empty,
        truncate_us = tracing::field::Empty,
        safe_truncate_seq = tracing::field::Empty,
        total_us = tracing::field::Empty,
    )
}

/// Span for one chDB native sink insert batch.
#[inline]
pub fn sink_write_span(db: &str, rp: &str, measurement: &str, rows: usize) -> tracing::Span {
    if !is_enabled() {
        return disabled_span();
    }
    tracing::info_span!(
        "write_path",
        system_trace = true,
        write_path = true,
        event = "sink_write",
        db = %db,
        rp = %rp,
        measurement = %measurement,
        rows = rows,
        use_arrow = tracing::field::Empty,
        ensure_table_us = tracing::field::Empty,
        coalesce_us = tracing::field::Empty,
        register_series_us = tracing::field::Empty,
        build_batch_us = tracing::field::Empty,
        chdb_insert_us = tracing::field::Empty,
        min_time = tracing::field::Empty,
        max_time = tracing::field::Empty,
        total_us = tracing::field::Empty,
    )
}

/// Standalone log for WAL group-commit batches (no request span context).
#[inline]
pub fn log_wal_batch(
    batch_size: usize,
    queue_wait_us: u64,
    coalesce_us: u64,
    write_us: u64,
    first_seq: u64,
    last_seq: u64,
) {
    if !is_enabled() {
        return;
    }
    tracing::debug!(
        system_trace = true,
        write_path = true,
        event = "wal_batch",
        batch_size = batch_size,
        queue_wait_us = queue_wait_us,
        coalesce_us = coalesce_us,
        write_us = write_us,
        first_seq = first_seq,
        last_seq = last_seq,
        total_us = queue_wait_us
            .saturating_add(coalesce_us)
            .saturating_add(write_us),
        "WAL group-commit batch"
    );
}

/// Standalone log for a single RocksDB WAL append (non-batched path).
#[inline]
pub fn log_wal_append(seq: u64, point_count: usize, serialize_us: u64, write_us: u64) {
    if !is_enabled() {
        return;
    }
    tracing::debug!(
        system_trace = true,
        write_path = true,
        event = "wal_append",
        wal_seq = seq,
        point_count = point_count,
        serialize_us = serialize_us,
        write_us = write_us,
        total_us = serialize_us.saturating_add(write_us),
        "WAL append"
    );
}

/// Emit the summary line for a span, recording total elapsed time.
#[inline]
pub fn finish_span(span: &tracing::Span, start: Option<Instant>, message: &'static str) {
    if !is_enabled() {
        return;
    }
    let total_us = start.map(|t| t.elapsed().as_micros() as u64).unwrap_or(0);
    span.record("total_us", total_us);
    tracing::debug!(
        parent: span,
        system_trace = true,
        total_us = total_us,
        "{message}"
    );
}

/// Start a total-duration timer only when tracing is enabled.
#[inline(always)]
pub fn start_timer() -> Option<Instant> {
    is_enabled().then(Instant::now)
}
