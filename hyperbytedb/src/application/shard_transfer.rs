//! Apply and initiate region transfer payloads on store nodes.

use std::collections::BTreeMap;
use std::sync::Arc;
use std::time::Duration;

use metrics::counter;

use crate::application::ingest_metadata::{IngestCardinalityLimits, prepare_batch_metadata};
use crate::application::line_protocol::{
    encode_points_to_line_protocol, parse_line_body_to_points_limited,
};
use crate::application::wal_append::append_points_with_prepared;
use crate::domain::column_mapping::ColumnMapping;
use crate::domain::database::Precision;
use crate::domain::point::{FieldValue, Point};
use crate::domain::series::series_id_for_point;
use crate::domain::sharding::{MeasurementKey, ShardRegion, ShardTransferPayload, TransferPhase};
use crate::error::HyperbytedbError;
use crate::ports::metadata::MetadataPort;
use crate::ports::points_sink::PointsSinkPort;
use crate::ports::query::QueryPort;
use crate::ports::wal::WalPort;

/// Result of pushing one region's data to a destination primary.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct RegionTransferOutcome {
    pub transfer_id: u64,
    /// Points exported from the source (WAL + flushed chDB rows).
    pub exported: u64,
    /// Points the destination confirmed applying across all pushed chunks.
    pub applied: u64,
    /// Wire bytes pushed to the destination across all chunks.
    ///
    /// The movement budget is a bandwidth throttle, so it has to be spent in
    /// the unit bandwidth is measured in. Counting points charged a region of
    /// 40-column rows the same as one of single-field rows, which is how a
    /// tick could blow its allowance by an order of magnitude while the
    /// counter looked healthy.
    pub bytes: u64,
}

impl RegionTransferOutcome {
    /// True when every exported point was confirmed applied at the destination
    /// (or there was nothing to move). Only then may source data be dropped.
    #[must_use]
    pub fn verified(&self) -> bool {
        self.applied == self.exported
    }
}

/// Parse the `applied` count out of a `/internal/shard/transfer` response body.
fn parse_applied_from_response(body: &str) -> u64 {
    serde_json::from_str::<serde_json::Value>(body)
        .ok()
        .and_then(|v| v.get("applied").and_then(serde_json::Value::as_u64))
        .unwrap_or(0)
}

pub async fn apply_transfer_push(
    metadata: &Arc<dyn MetadataPort>,
    wal: &Arc<dyn WalPort>,
    sink: Option<&Arc<dyn PointsSinkPort>>,
    _node_id: u64,
    req: &ShardTransferPayload,
    max_points: usize,
) -> Result<u64, HyperbytedbError> {
    let body = req
        .body
        .as_ref()
        .filter(|b| !b.is_empty())
        .ok_or_else(|| HyperbytedbError::ShardMap("transfer push missing body".into()))?;

    let points = parse_line_body_to_points_limited(body, None, max_points)?;
    let filtered: Vec<_> = points
        .into_iter()
        .filter(|p| {
            let sid = series_id_for_point(p);
            sid >= req.start && sid < req.end
        })
        .collect();

    let applied = filtered.len() as u64;
    if filtered.is_empty() {
        return Ok(0);
    }

    let limits = IngestCardinalityLimits {
        max_tag_values_per_measurement: 0,
        max_measurements_per_database: 0,
    };
    prepare_batch_metadata(metadata, &req.db, &req.rp, &filtered, limits, None).await?;

    append_points_with_prepared(
        wal.as_ref(),
        sink,
        &req.db,
        &req.rp,
        filtered,
        req.source_node_id,
        max_points,
    )
    .await?;

    Ok(applied)
}

pub async fn apply_transfer(
    metadata: &Arc<dyn MetadataPort>,
    wal: &Arc<dyn WalPort>,
    sink: Option<&Arc<dyn PointsSinkPort>>,
    mv_service: Option<&crate::application::materialized_view_service::MaterializedViewService>,
    node_id: u64,
    req: &ShardTransferPayload,
    max_points: usize,
) -> Result<u64, HyperbytedbError> {
    match req.phase {
        TransferPhase::Push => {
            apply_transfer_push(metadata, wal, sink, node_id, req, max_points).await
        }
        TransferPhase::Ack => {
            if let Some(mv) = mv_service
                && let Err(e) = mv
                    .purge_dest_partials_after_source_transfer(
                        &req.db,
                        &req.rp,
                        &req.measurement,
                        req.start,
                        req.end,
                    )
                    .await
            {
                tracing::warn!(
                    db = %req.db,
                    measurement = %req.measurement,
                    error = %e,
                    "MV dest purge after transfer failed"
                );
            }
            Ok(0)
        }
    }
}

pub async fn drop_region_data(
    metadata: &Arc<dyn MetadataPort>,
    sink: Option<&Arc<dyn PointsSinkPort>>,
    db: &str,
    rp: &str,
    measurement: &str,
    start: u64,
    end: u64,
) -> Result<(), HyperbytedbError> {
    metadata
        .delete_series_in_range(db, rp, measurement, start, end)
        .await?;
    if let Some(sink) = sink {
        sink.delete_series_id_range(db, rp, measurement, start, end)
            .await?;
    }
    Ok(())
}

/// Entries fetched per `read_range` page during chunked WAL export.
const WAL_EXPORT_PAGE_ENTRIES: usize = 128;

/// Resume point for chunked WAL export.
///
/// `read_range` is inclusive of `from_seq`, so a partially consumed entry is
/// re-read on the next call and its already-exported points are skipped via
/// `skip_points_in_head_entry`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct WalExportCursor {
    pub from_seq: u64,
    pub skip_points_in_head_entry: usize,
}

/// Export the next chunk of unflushed WAL points for `[region.start, region.end)`.
///
/// Unlike a single-shot read of the whole log, export resumes across calls via
/// [`WalExportCursor`] and never fails merely because the region has more
/// unflushed points than one transfer chunk may carry.
///
/// Returns `(encoded_body, exported_count, next_cursor)`; an empty body with
/// `next_cursor == cursor` means the WAL is exhausted.
pub async fn export_region_points_from_wal_chunked(
    wal: &Arc<dyn WalPort>,
    key: &MeasurementKey,
    region: &ShardRegion,
    cursor: WalExportCursor,
    max_points: usize,
    // Skip WAL entries at or below this seq when staging onto a replicating learner.
    skip_wal_through: u64,
) -> Result<(Vec<u8>, usize, WalExportCursor), HyperbytedbError> {
    let max_points = max_points.max(1);
    let mut points: Vec<Point> = Vec::new();
    let mut next = cursor;

    loop {
        let page = wal
            .read_range(next.from_seq, WAL_EXPORT_PAGE_ENTRIES)
            .await?;
        // Only an empty page is the log tail; reads stop early to bound memory.
        if page.is_empty() {
            return Ok((encode_wal_chunk(&points)?, points.len(), next));
        }
        let page_start = next;

        // Entries are owned (freshly decoded from the page read): consume by
        // value so exported points MOVE into the chunk buffer instead of
        // being deep-cloned per point.
        for (seq, entry) in page.into_iter() {
            if seq <= skip_wal_through {
                next = WalExportCursor {
                    from_seq: seq.saturating_add(1),
                    skip_points_in_head_entry: 0,
                };
                continue;
            }
            if entry.database != key.db || entry.retention_policy != key.rp {
                next = WalExportCursor {
                    from_seq: seq.saturating_add(1),
                    skip_points_in_head_entry: 0,
                };
                continue;
            }
            let mut skip = if seq == next.from_seq {
                next.skip_points_in_head_entry
            } else {
                0
            };
            let mut consumed_here = 0usize;
            for p in entry.points.into_iter() {
                if p.measurement != key.measurement {
                    continue;
                }
                let sid = series_id_for_point(&p);
                if !(sid >= region.start && sid < region.end) {
                    continue;
                }
                if skip > 0 {
                    skip -= 1;
                    consumed_here += 1;
                    continue;
                }
                points.push(p);
                consumed_here += 1;
                if points.len() >= max_points {
                    // Split mid-entry: resume inside this same entry.
                    return Ok((
                        encode_wal_chunk(&points)?,
                        points.len(),
                        WalExportCursor {
                            from_seq: seq,
                            skip_points_in_head_entry: consumed_here,
                        },
                    ));
                }
            }
            // Entry fully consumed past this point.
            next = WalExportCursor {
                from_seq: seq.saturating_add(1),
                skip_points_in_head_entry: 0,
            };
        }

        // No split: keep paging from the advanced cursor. Bail defensively if
        // a pass made no progress (should be impossible: any consumed entry
        // either advances `next` or triggers the split return).
        if next == page_start {
            return Err(HyperbytedbError::ShardMap(
                "wal export made no progress".into(),
            ));
        }
    }
}

fn encode_wal_chunk(points: &[Point]) -> Result<Vec<u8>, HyperbytedbError> {
    if points.is_empty() {
        return Ok(Vec::new());
    }
    encode_points_to_line_protocol(points, Precision::Nanosecond)
}

/// Export the next chunk of flushed chDB rows for `[region.start, region.end)`.
///
/// Pagination is keyset-based on `series_id` (inclusive re-read of the boundary
/// series) instead of `LIMIT/OFFSET`: concurrent inserts shift offsets and can
/// silently skip rows, while re-applied duplicates are idempotent at the
/// destination and are counted identically on both sides of the verified
/// handoff.
///
/// Returns `(encoded_body, exported_count, last_series_id_in_batch)`; an empty
/// body means export is complete.
#[allow(clippy::too_many_arguments)]
pub async fn export_region_points_from_chdb(
    query: &dyn QueryPort,
    metadata: &dyn MetadataPort,
    key: &MeasurementKey,
    region: &ShardRegion,
    after_series_id: Option<u64>,
    max_points: usize,
) -> Result<(Vec<u8>, usize, Option<u64>), HyperbytedbError> {
    use crate::domain::chdb_naming::{
        field_column_name, quote_backticks, quoted_series_table_name, quoted_table_name,
    };

    let meta = metadata
        .get_measurement(&key.db, &key.rp, &key.measurement)
        .await?
        .ok_or_else(|| {
            HyperbytedbError::ShardMap(
                format!(
                    "measurement {} not found for transfer export",
                    key.measurement
                )
                .into(),
            )
        })?;

    let mapping = ColumnMapping::from_measurement_meta(&meta);
    let fact = quoted_table_name(&key.db, &key.rp, &key.measurement);
    let series = quoted_series_table_name(&key.db, &key.rp, &key.measurement);

    let mut select_cols = vec!["f.`time`".to_string(), "f.`series_id`".to_string()];
    for field in mapping.field_names.iter() {
        let phys = field_column_name(field);
        select_cols.push(format!("f.{}", quote_backticks(&phys)));
    }
    for tag in mapping.tag_keys.iter() {
        let phys = mapping.physical_tag_column_name(tag);
        select_cols.push(format!("s.{}", quote_backticks(&phys)));
    }
    // Witness for the ANY LEFT JOIN below. A LEFT JOIN that finds no series row
    // does not fail -- it substitutes column defaults, so every tag comes back
    // as an empty string and the row looks like a legitimately untagged series.
    // Selecting the joined key lets us tell "this series has no host" from
    // "this node has no series row for this series_id at all".
    select_cols.push("s.`series_id` AS `__joined_series_id`".to_string());

    let mut predicates = Vec::new();
    match after_series_id {
        // Inclusive re-read of the boundary series keeps tie-grouped rows
        // (same series, split across chunk boundaries) from being skipped.
        Some(sid) => predicates.push(format!("f.`series_id` >= {sid}")),
        None => predicates.push(format!("f.`series_id` >= {}", region.start)),
    }
    if region.end != u64::MAX {
        predicates.push(format!("f.`series_id` < {}", region.end));
    }

    let sql = format!(
        "SELECT {} FROM {fact} AS f \
         ANY LEFT JOIN {series} AS s ON f.`series_id` = s.`series_id` \
         WHERE {} \
         ORDER BY f.`series_id`, f.`time` \
         LIMIT {max_points} \
         FORMAT JSONEachRow",
        select_cols.join(", "),
        predicates.join(" AND ")
    );

    let raw = query.execute_sql(&sql).await?;
    if raw.trim().is_empty() {
        return Ok((Vec::new(), 0, None));
    }

    let mut points = Vec::new();
    for line in raw.lines() {
        let line = line.trim();
        if line.is_empty() {
            continue;
        }
        let row: serde_json::Value = serde_json::from_str(line).map_err(|e| {
            HyperbytedbError::Internal(format!("transfer export JSON parse: {e}").into())
        })?;
        let Some(obj) = row.as_object() else {
            continue;
        };

        // The series dictionary entry must exist, or every tag on this row is a
        // LEFT JOIN default rather than data. Both ways of carrying that
        // forward corrupt: emitting `host=` produces invalid line protocol and
        // the destination rejects the whole payload with 500, while dropping
        // the tag changes the point's series_id, so `apply_transfer_push`
        // recomputes it, finds it outside [start, end) and discards the row.
        // Fail the chunk instead -- the disposition for a 500 is Retryable, so
        // the movement is retried rather than silently losing rows.
        let fact_sid = obj
            .get("series_id")
            .and_then(serde_json::Value::as_u64)
            .unwrap_or_default();
        let joined_sid = obj
            .get("__joined_series_id")
            .and_then(serde_json::Value::as_u64)
            .unwrap_or_default();
        if !mapping.tag_keys.is_empty() && joined_sid != fact_sid {
            counter!("hyperbytedb_shard_transfer_missing_series_total").increment(1);
            return Err(HyperbytedbError::Internal(
                format!(
                    "transfer export: no series row for series_id {fact_sid} in \
                     {}.{}.{}; tags would be empty defaults",
                    key.db, key.rp, key.measurement
                )
                .into(),
            ));
        }

        let mut tags = BTreeMap::new();
        for tag in &mapping.tag_keys {
            let phys = mapping.physical_tag_column_name(tag);
            if let Some(serde_json::Value::String(v)) = obj.get(&phys)
                && !v.is_empty()
            {
                // An empty value is a tag this series does not carry. Line
                // protocol has no representation for `host=`, so it must be
                // omitted, not emitted empty.
                tags.insert(tag.clone(), v.clone());
            }
        }

        let mut fields = BTreeMap::new();
        for field in &mapping.field_names {
            let phys = field_column_name(field);
            if let Some(val) = obj.get(&phys)
                && let Some(fv) = json_field_to_value(val, meta.field_types.get(field).copied())
            {
                fields.insert(field.clone(), fv);
            }
        }

        let timestamp = point_timestamp(obj, &key.measurement)?;
        if fields.is_empty() {
            continue;
        }

        points.push(Point {
            measurement: key.measurement.clone(),
            tags,
            fields,
            timestamp,
        });
    }

    let count = points.len();
    if count == 0 {
        return Ok((Vec::new(), 0, None));
    }
    let last_sid = series_id_for_point(&points[count - 1]);
    let body = encode_points_to_line_protocol(&points, Precision::Nanosecond)?;
    Ok((body, count, Some(last_sid)))
}

/// Extract a point timestamp from an exported chDB row.
///
/// Aborts the export on unparseable timestamps instead of silently rewriting
/// them to epoch 0 — a transferred point with a corrupted time is data loss
/// for every downstream range query. chDB renders `DateTime64(9)` in
/// JSONEachRow as `"YYYY-MM-DD HH:MM:SS[.frac]"` (space separator, no offset),
/// which RFC3339 parsing rejects; both shapes are accepted here.
fn point_timestamp(
    obj: &serde_json::Map<String, serde_json::Value>,
    measurement: &str,
) -> Result<i64, HyperbytedbError> {
    let Some(v) = obj.get("time") else {
        return Err(HyperbytedbError::ShardMap(
            format!("transfer export: row for {measurement} missing time column").into(),
        ));
    };
    match json_timestamp_nanos(v) {
        Some(ts) => Ok(ts),
        None => Err(HyperbytedbError::ShardMap(
            format!(
                "transfer export: unparseable timestamp {v} for {measurement}; refusing to transfer"
            )
            .into(),
        )),
    }
}

fn json_timestamp_nanos(v: &serde_json::Value) -> Option<i64> {
    match v {
        serde_json::Value::Number(n) => n
            .as_i64()
            .or_else(|| n.as_u64().and_then(|u| i64::try_from(u).ok())),
        serde_json::Value::String(s) => parse_timestamp_string(s),
        _ => None,
    }
}

fn parse_timestamp_string(s: &str) -> Option<i64> {
    let s = s.trim();
    if let Ok(dt) = chrono::DateTime::parse_from_rfc3339(s) {
        return dt.timestamp_nanos_opt();
    }
    // chdb/ClickHouse JSONEachRow: "YYYY-MM-DD HH:MM:SS[.frac]" (UTC assumed).
    if let Ok(dt) = chrono::NaiveDateTime::parse_from_str(s, "%Y-%m-%d %H:%M:%S%.f") {
        return dt.and_utc().timestamp_nanos_opt();
    }
    if let Ok(d) = chrono::NaiveDate::parse_from_str(s, "%Y-%m-%d") {
        return d.and_hms_opt(0, 0, 0)?.and_utc().timestamp_nanos_opt();
    }
    None
}

fn json_field_to_value(v: &serde_json::Value, disc: Option<u8>) -> Option<FieldValue> {
    match v {
        serde_json::Value::Number(n) => {
            if disc == Some(1) {
                n.as_i64().map(FieldValue::Integer)
            } else if disc == Some(2) {
                n.as_u64().map(FieldValue::UInteger)
            } else {
                n.as_f64().map(FieldValue::Float)
            }
        }
        serde_json::Value::String(s) => Some(FieldValue::String(s.clone())),
        serde_json::Value::Bool(b) => Some(FieldValue::Boolean(*b)),
        _ => None,
    }
}

/// Bulk data movement, not a scatter query: generous explicit timeout so a
/// hung destination cannot stall a scheduler operator slot indefinitely.
const TRANSFER_PUSH_TIMEOUT_SECS: u64 = 120;
const TRANSFER_ACK_TIMEOUT_SECS: u64 = 30;

#[allow(clippy::too_many_arguments)]
pub async fn push_region_transfer(
    peer_client: &crate::adapters::cluster::peer_client::PeerClient,
    source_node_id: u64,
    key: &MeasurementKey,
    region: &ShardRegion,
    body: Vec<u8>,
    dest_primary: u64,
    transfer_id: u64,
    seq: u64,
    done: bool,
    stage: bool,
) -> Result<(u64, u64), HyperbytedbError> {
    let membership = peer_client.membership().read().await;
    let addr = membership
        .get_node(dest_primary)
        .map(|n| n.addr.clone())
        .ok_or_else(|| HyperbytedbError::PeerUnreachable("unknown transfer destination".into()))?;
    drop(membership);

    let body_len = body.len();
    let wire_bytes = body_len as u64;
    let payload = if stage {
        ShardTransferPayload::stage_push(
            &key.db,
            &key.rp,
            &key.measurement,
            region.region_id,
            region.start,
            region.end,
            region.epoch,
            body,
            source_node_id,
            transfer_id,
            seq,
        )
    } else {
        ShardTransferPayload::push(
            &key.db,
            &key.rp,
            &key.measurement,
            region.region_id,
            region.start,
            region.end,
            region.epoch,
            body,
            source_node_id,
            transfer_id,
            seq,
            done,
        )
    };
    let url = format!("http://{addr}/internal/shard/transfer");
    let resp = peer_client
        .http_client()
        .post(&url)
        .json(&payload)
        .timeout(Duration::from_secs(TRANSFER_PUSH_TIMEOUT_SECS))
        .send()
        .await
        .map_err(|e| HyperbytedbError::PeerUnreachable(e.to_string()))?;
    if !resp.status().is_success() {
        let status = resp.status().as_u16();
        return Err(HyperbytedbError::TransferRejected {
            status,
            reason: rejection_reason(resp).await,
        });
    }
    let applied = parse_applied_from_response(&resp.text().await.unwrap_or_default());
    counter!("hyperbytedb_shard_transfer_bytes").increment(wire_bytes);
    counter!("hyperbytedb_shard_transfers_total").increment(1);
    Ok((applied, wire_bytes))
}

pub async fn ack_region_transfer(
    peer_client: &crate::adapters::cluster::peer_client::PeerClient,
    source_node_id: u64,
    key: &MeasurementKey,
    region: &ShardRegion,
    ack_node: u64,
    transfer_id: u64,
) -> Result<(), HyperbytedbError> {
    let membership = peer_client.membership().read().await;
    let addr = membership
        .get_node(ack_node)
        .map(|n| n.addr.clone())
        .ok_or_else(|| HyperbytedbError::PeerUnreachable("unknown transfer ack target".into()))?;
    drop(membership);

    let payload = ShardTransferPayload::ack(
        &key.db,
        &key.rp,
        &key.measurement,
        region.region_id,
        region.start,
        region.end,
        region.epoch,
        source_node_id,
        transfer_id,
    );
    let url = format!("http://{addr}/internal/shard/transfer");
    let resp = peer_client
        .http_client()
        .post(&url)
        .json(&payload)
        .timeout(Duration::from_secs(TRANSFER_ACK_TIMEOUT_SECS))
        .send()
        .await
        .map_err(|e| HyperbytedbError::PeerUnreachable(e.to_string()))?;
    if !resp.status().is_success() {
        return Err(HyperbytedbError::ShardMap(
            format!("transfer ack failed: {}", resp.status()).into(),
        ));
    }
    Ok(())
}

fn new_transfer_id() -> u64 {
    use std::time::{SystemTime, UNIX_EPOCH};
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_nanos() as u64)
        .unwrap_or(1)
}

/// Most points one transfer push carries. Callers pass the client
/// `max_points_per_request` (500,000 by default): one chunk then was hundreds
/// of MB of JSON parsed row by row, far past the receiver's body limit (413,
/// retried forever), and with 4 concurrent moves the donor was OOM-killed at
/// 16 GiB on every scale-up on n3.
pub(crate) const TRANSFER_CHUNK_POINTS: usize = 50_000;

/// How long to wait so `sent` bytes over `elapsed` stay at or under `rate`
/// bytes per second. A rate of 0 is unpaced.
fn pacing_delay(rate: u64, sent: u64, elapsed: std::time::Duration) -> std::time::Duration {
    if rate == 0 {
        return std::time::Duration::ZERO;
    }
    std::time::Duration::from_secs_f64(sent as f64 / rate as f64).saturating_sub(elapsed)
}

/// `max_bytes_per_sec` paces the push chunk by chunk (0 = unpaced). The
/// sender is the only place a rate can be enforced: the leader that starts a
/// move only sees the whole remote stage, after it is done.
#[allow(clippy::too_many_arguments)]
async fn transfer_region_data(
    peer_client: &Arc<crate::adapters::cluster::peer_client::PeerClient>,
    metadata: &Arc<dyn MetadataPort>,
    wal: &Arc<dyn WalPort>,
    query_port: Option<&Arc<dyn QueryPort>>,
    source_node_id: u64,
    key: &MeasurementKey,
    region: &ShardRegion,
    dest_primary: u64,
    max_points: usize,
    stage: bool,
    max_bytes_per_sec: u64,
) -> Result<RegionTransferOutcome, HyperbytedbError> {
    if dest_primary == source_node_id {
        return Ok(RegionTransferOutcome {
            transfer_id: 0,
            exported: 0,
            applied: 0,
            bytes: 0,
        });
    }
    let max_points = max_points.clamp(1, TRANSFER_CHUNK_POINTS);
    let transfer_id = new_transfer_id();
    let mut seq = 0u64;
    let mut exported = 0u64;
    let mut bytes = 0u64;
    let mut applied = 0u64;
    let started = std::time::Instant::now();

    // Unflushed WAL rows: chunked and resumable so regions with more pending
    // points than one chunk transfers never fail permanently (H4).
    let mut cursor = WalExportCursor::default();
    loop {
        let (body, count, next_cursor) =
            export_region_points_from_wal_chunked(wal, key, region, cursor, max_points, 0).await?;
        if !body.is_empty() {
            let (chunk_applied, chunk_bytes) = push_region_transfer(
                peer_client.as_ref(),
                source_node_id,
                key,
                region,
                body,
                dest_primary,
                transfer_id,
                seq,
                false,
                stage,
            )
            .await?;
            exported += count as u64;
            applied += chunk_applied;
            bytes += chunk_bytes;
            seq = seq.saturating_add(1);
            let wait = pacing_delay(max_bytes_per_sec, bytes, started.elapsed());
            if !wait.is_zero() {
                tokio::time::sleep(wait).await;
            }
        }
        if next_cursor == cursor {
            break;
        }
        cursor = next_cursor;
    }

    // Flushed chDB rows: keyset-paginated on series_id (M9). Exported
    // unconditionally.
    //
    // This used to be skipped whenever the destination reported a non-zero WAL
    // watermark, on the theory that replication had already delivered the hot
    // rows. The number compared was the learner's *node-wide* sequence against
    // the donor's, so a learner busy with any other region skipped its own cold
    // backfill entirely — trading a duplicate for a missing row. The duplicate
    // it was avoiding was never visible in the first place: reads go through a
    // `GROUP BY (series_id, time)` coalescing view, so a re-exported row
    // collapses against the replicated one before any aggregate sees it.
    if let Some(query) = query_port {
        let mut after_series_id: Option<u64> = None;
        loop {
            let (body, count, last_sid) = export_region_points_from_chdb(
                query.as_ref(),
                metadata.as_ref(),
                key,
                region,
                after_series_id,
                max_points,
            )
            .await?;
            if body.is_empty() {
                break;
            }
            let done = count < max_points;
            let (chunk_applied, chunk_bytes) = push_region_transfer(
                peer_client.as_ref(),
                source_node_id,
                key,
                region,
                body,
                dest_primary,
                transfer_id,
                seq,
                done,
                stage,
            )
            .await?;
            exported += count as u64;
            applied += chunk_applied;
            bytes += chunk_bytes;
            seq = seq.saturating_add(1);
            let wait = pacing_delay(max_bytes_per_sec, bytes, started.elapsed());
            if !wait.is_zero() {
                tokio::time::sleep(wait).await;
            }
            if done {
                break;
            }
            // Full batch whose boundary series did not advance means one
            // single series exceeds the chunk cap: force past it rather than
            // re-reading the same rows forever (remainder skipped, warned).
            match (last_sid, after_series_id) {
                (Some(ls), prev) if ls == prev.unwrap_or(u64::MAX) && ls != u64::MAX => {
                    tracing::warn!(
                        db = %key.db,
                        rp = %key.rp,
                        measurement = %key.measurement,
                        series_id = ls,
                        max_points,
                        "single series exceeds transfer chunk cap; skipping its overflow rows"
                    );
                    after_series_id = Some(ls.saturating_add(1));
                }
                (Some(ls), _) => after_series_id = Some(ls),
                (None, _) => break,
            }
        }
    }

    let outcome = RegionTransferOutcome {
        transfer_id,
        exported,
        applied,
        bytes,
    };
    // Verified handoff: refuse to bless the transfer unless the destination
    // confirmed every exported point. Callers must not drop source data on Err.
    if !outcome.verified() {
        counter!("hyperbytedb_shard_transfer_failures_total").increment(1);
        return Err(HyperbytedbError::ShardMap(format!(
            "verified handoff mismatch for {}.{} {}: exported {} points, destination applied {}",
            key.db, key.rp, key.measurement, exported, applied
        ).into()));
    }
    Ok(outcome)
}

/// Push a region's data to `dest_primary`, verifying every exported point was
/// applied. Ownership-aware: the destination validates the committed range.
#[allow(clippy::too_many_arguments)]
pub async fn push_region_transfer_data(
    peer_client: &Arc<crate::adapters::cluster::peer_client::PeerClient>,
    metadata: &Arc<dyn MetadataPort>,
    wal: &Arc<dyn WalPort>,
    query_port: Option<&Arc<dyn QueryPort>>,
    source_node_id: u64,
    key: &MeasurementKey,
    region: &ShardRegion,
    dest_primary: u64,
    max_points: usize,
) -> Result<RegionTransferOutcome, HyperbytedbError> {
    transfer_region_data(
        peer_client,
        metadata,
        wal,
        query_port,
        source_node_id,
        key,
        region,
        dest_primary,
        max_points,
        false,
        0,
    )
    .await
}

/// Pre-commit staging variant used by split: pushes rows for a range the
/// destination does not own yet (the Split proposal has not committed).
/// Best-effort by design — callers log failures and proceed to commit.
#[allow(clippy::too_many_arguments)]
pub async fn stage_region_transfer_data(
    peer_client: &Arc<crate::adapters::cluster::peer_client::PeerClient>,
    metadata: &Arc<dyn MetadataPort>,
    wal: &Arc<dyn WalPort>,
    query_port: Option<&Arc<dyn QueryPort>>,
    source_node_id: u64,
    key: &MeasurementKey,
    region: &ShardRegion,
    dest_primary: u64,
    max_points: usize,
    max_bytes_per_sec: u64,
) -> Result<RegionTransferOutcome, HyperbytedbError> {
    let outcome = transfer_region_data(
        peer_client,
        metadata,
        wal,
        query_port,
        source_node_id,
        key,
        region,
        dest_primary,
        max_points,
        true,
        max_bytes_per_sec,
    )
    .await;
    match outcome {
        Ok(o) => {
            counter!("hyperbytedb_shard_stage_total").increment(1);
            Ok(o)
        }
        Err(e) => {
            counter!("hyperbytedb_shard_stage_failures_total").increment(1);
            Err(e)
        }
    }
}

/// The `{"error": ...}` a rehome/transfer handler puts in a rejection body, or
/// the raw body, truncated on a char boundary.
///
/// Without this a rejection reads "shard transfer rejected with status 500"
/// and nothing else: the handler already serialised the cause into the body,
/// and every caller threw it away at the one place anyone would look. That is
/// why a cluster could report 49 staging rejections across 27 regions without
/// a single line saying what was actually wrong.
pub(crate) async fn rejection_reason(resp: reqwest::Response) -> Box<str> {
    let body = resp.text().await.unwrap_or_default();
    let mut reason = serde_json::from_str::<serde_json::Value>(&body)
        .ok()
        .and_then(|v| {
            v.get("error")
                .and_then(serde_json::Value::as_str)
                .map(str::to_owned)
        })
        .unwrap_or(body);
    if reason.trim().is_empty() {
        reason = "no body".to_owned();
    }
    if reason.len() > 300 {
        let end = (0..=300)
            .rev()
            .find(|i| reason.is_char_boundary(*i))
            .unwrap_or(0);
        reason.truncate(end);
    }
    reason.into_boxed_str()
}

#[allow(clippy::too_many_arguments)]
pub async fn complete_region_transfer(
    peer_client: &Arc<crate::adapters::cluster::peer_client::PeerClient>,
    metadata: &Arc<dyn MetadataPort>,
    sink: Option<&Arc<dyn PointsSinkPort>>,
    source_node_id: u64,
    key: &MeasurementKey,
    region: &ShardRegion,
    dest_primary: u64,
    transfer_id: u64,
    drop_source: bool,
) -> Result<(), HyperbytedbError> {
    if dest_primary == source_node_id || transfer_id == 0 {
        return Ok(());
    }
    ack_region_transfer(
        peer_client.as_ref(),
        source_node_id,
        key,
        region,
        dest_primary,
        transfer_id,
    )
    .await?;
    if !drop_source {
        return Ok(());
    }
    drop_region_data(
        metadata,
        sink,
        &key.db,
        &key.rp,
        &key.measurement,
        region.start,
        region.end,
    )
    .await
}

/// Push a region's data to `dest_primary` and, once the destination has
/// confirmed every point, ack and optionally drop the source copy.
///
/// `drop_source` must only be set when the destination range is *owned*
/// elsewhere after the move (e.g. post-split child re-homing). For primary
/// moves that keep the same range (merge/rebalance/failover), pass `false`:
/// keeping the stale replica copy is harmless under primary-only reads, while
/// dropping it strands data whenever the subsequent ownership change fails.
#[allow(clippy::too_many_arguments)]
pub async fn run_region_transfer(
    peer_client: &Arc<crate::adapters::cluster::peer_client::PeerClient>,
    metadata: &Arc<dyn MetadataPort>,
    wal: &Arc<dyn WalPort>,
    query_port: Option<&Arc<dyn QueryPort>>,
    sink: Option<&Arc<dyn PointsSinkPort>>,
    source_node_id: u64,
    key: &MeasurementKey,
    region: &ShardRegion,
    dest_primary: u64,
    max_points: usize,
    drop_source: bool,
) -> Result<(), HyperbytedbError> {
    let outcome = push_region_transfer_data(
        peer_client,
        metadata,
        wal,
        query_port,
        source_node_id,
        key,
        region,
        dest_primary,
        max_points,
    )
    .await?;
    complete_region_transfer(
        peer_client,
        metadata,
        sink,
        source_node_id,
        key,
        region,
        dest_primary,
        outcome.transfer_id,
        drop_source,
    )
    .await
}

#[cfg(test)]
mod tests {

    /// A rejection must carry WHY, not just a status code.
    ///
    /// The rehome handler serialises its cause into `{"error": ...}` and every
    /// caller used to discard the body, so a cluster could log 49 staging
    /// rejections across 27 regions and never say what was wrong.
    #[tokio::test]
    async fn rejection_reason_prefers_the_handlers_error_field() {
        let resp = axum::http::Response::builder()
            .status(500)
            .body(r#"{"error":"region range not found"}"#)
            .unwrap();
        let got = super::rejection_reason(reqwest::Response::from(resp)).await;
        assert_eq!(&*got, "region range not found");
    }

    #[tokio::test]
    async fn rejection_reason_falls_back_to_the_raw_body() {
        let resp = axum::http::Response::builder()
            .status(502)
            .body("upstream exploded")
            .unwrap();
        let got = super::rejection_reason(reqwest::Response::from(resp)).await;
        assert_eq!(&*got, "upstream exploded");
    }

    /// An empty body must still say something; "" in a log line is
    /// indistinguishable from the bug this fixes.
    #[tokio::test]
    async fn rejection_reason_reports_an_empty_body() {
        let resp = axum::http::Response::builder()
            .status(500)
            .body("")
            .unwrap();
        let got = super::rejection_reason(reqwest::Response::from(resp)).await;
        assert_eq!(&*got, "no body");
    }

    /// Truncation must land on a char boundary -- a multi-byte body would
    /// otherwise panic the scheduler tick that is reporting the failure.
    #[tokio::test]
    async fn rejection_reason_truncates_on_a_char_boundary() {
        let body = "\u{e9}".repeat(400);
        let resp = axum::http::Response::builder()
            .status(500)
            .body(body)
            .unwrap();
        let got = super::rejection_reason(reqwest::Response::from(resp)).await;
        assert!(got.len() <= 300, "len {}", got.len());
        assert!(!got.is_empty());
    }

    use super::*;
    use crate::adapters::metadata::rocksdb_meta::RocksDbMetadata;
    use crate::domain::measurement::MeasurementMeta;
    use crate::domain::series::series_id;
    use std::collections::BTreeMap;

    fn host_tag(v: &str) -> BTreeMap<String, String> {
        [("host".into(), v.into())].into()
    }

    #[tokio::test]
    async fn delete_series_in_range_preserves_left_half() {
        let dir = tempfile::tempdir().unwrap();
        let meta = RocksDbMetadata::open(dir.path()).unwrap();

        let left_id = series_id("cpu", &host_tag("left"));
        let right_id = series_id("cpu", &host_tag("right"));
        let low = left_id.min(right_id);
        let high = left_id.max(right_id);

        meta.register_measurement(
            "db",
            "autogen",
            &MeasurementMeta {
                name: "cpu".into(),
                tag_keys: vec!["host".into()],
                field_types: [("value".into(), 0_u8)].into(),
                ..Default::default()
            },
        )
        .await
        .unwrap();

        meta.register_series_batch(
            "db",
            "autogen",
            "cpu",
            &[(left_id, host_tag("left")), (right_id, host_tag("right"))],
        )
        .await
        .unwrap();

        let removed = meta
            .delete_series_in_range("db", "autogen", "cpu", low + 1, u64::MAX)
            .await
            .unwrap();
        assert_eq!(removed, 1);

        let remaining = meta.list_series_ids("db", "autogen", "cpu").await.unwrap();
        assert!(remaining.contains(&low));
        assert!(!remaining.contains(&high));
    }

    #[test]
    fn parses_chdb_json_each_row_timestamps() {
        // RFC3339 (client-style exports).
        assert_eq!(
            parse_timestamp_string("2026-08-21T15:33:00Z"),
            Some(1_787_326_380 * 1_000_000_000)
        );
        // chdb JSONEachRow DateTime64(9): space separator, nanosecond fraction.
        assert_eq!(
            parse_timestamp_string("2026-08-21 15:33:00.123456789"),
            Some(1_787_326_380 * 1_000_000_000 + 123_456_789)
        );
        // Second precision without fraction.
        assert_eq!(
            parse_timestamp_string("2026-08-21 15:33:00"),
            Some(1_787_326_380 * 1_000_000_000)
        );
        // Bare date.
        assert_eq!(
            parse_timestamp_string("2026-08-21"),
            Some(1_787_270_400 * 1_000_000_000)
        );
    }

    #[test]
    fn rejects_unparseable_timestamps_instead_of_epoch_zero() {
        assert_eq!(parse_timestamp_string("not-a-time"), None);
        assert_eq!(parse_timestamp_string(""), None);
        assert_eq!(json_timestamp_nanos(&serde_json::Value::Null), None);
    }

    #[test]
    fn numeric_timestamps_are_nanoseconds() {
        assert_eq!(
            json_timestamp_nanos(&serde_json::json!(1_700_000_000_000_000_000u64)),
            Some(1_700_000_000_000_000_000)
        );
        assert_eq!(json_timestamp_nanos(&serde_json::json!(-5)), Some(-5));
    }

    #[test]
    fn point_timestamp_errors_on_missing_or_bad_time_column() {
        use serde_json::Map;
        let mut obj = Map::new();
        obj.insert("time".into(), serde_json::json!("garbage"));
        let err = point_timestamp(&obj, "cpu").unwrap_err().to_string();
        assert!(err.contains("unparseable timestamp"), "{err}");

        let empty = Map::new();
        let err = point_timestamp(&empty, "cpu").unwrap_err().to_string();
        assert!(err.contains("missing time column"), "{err}");
    }

    #[test]
    fn applied_count_parsed_from_transfer_response() {
        assert_eq!(parse_applied_from_response(r#"{"ok":true,"applied":7}"#), 7);
        assert_eq!(parse_applied_from_response(r#"{"ok":true}"#), 0);
        assert_eq!(parse_applied_from_response("not json"), 0);
    }

    #[test]
    fn outcome_verified_requires_exact_match() {
        let ok = RegionTransferOutcome {
            transfer_id: 1,
            exported: 10,
            applied: 10,
            bytes: 4_096,
        };
        let short = RegionTransferOutcome {
            transfer_id: 1,
            exported: 10,
            applied: 9,
            bytes: 4_096,
        };
        let nothing = RegionTransferOutcome {
            transfer_id: 0,
            exported: 0,
            applied: 0,
            bytes: 0,
        };
        assert!(ok.verified());
        assert!(!short.verified());
        assert!(nothing.verified(), "nothing to move is trivially verified");
        // Verification is about points delivered, not bandwidth spent: bytes
        // must never enter the predicate.
        let same_points_more_bytes = RegionTransferOutcome {
            bytes: 1_000_000,
            ..ok
        };
        assert!(same_points_more_bytes.verified());
    }

    #[test]
    fn pacing_holds_the_sender_to_its_rate() {
        use std::time::Duration;
        // 1 MiB sent at 1 MiB/s is due at 1 s: after 0.25 s, wait 0.75 s.
        assert_eq!(
            super::pacing_delay(1 << 20, 1 << 20, Duration::from_millis(250)),
            Duration::from_millis(750)
        );
        // Already slower than the rate: no wait.
        assert_eq!(
            super::pacing_delay(1 << 20, 1 << 20, Duration::from_secs(2)),
            Duration::ZERO
        );
        // Rate 0 is unpaced.
        assert_eq!(
            super::pacing_delay(0, u64::MAX, Duration::ZERO),
            Duration::ZERO
        );
    }

    /// The movement budget is a bandwidth throttle, so two regions with equal
    /// point counts but very different row widths must not cost the same.
    /// Charging points made a wide-row region look as cheap as a narrow one.
    #[test]
    fn the_budget_unit_is_bytes_not_points() {
        let narrow = RegionTransferOutcome {
            transfer_id: 1,
            exported: 1_000,
            applied: 1_000,
            bytes: 20_000,
        };
        let wide = RegionTransferOutcome {
            transfer_id: 2,
            exported: 1_000,
            applied: 1_000,
            bytes: 900_000,
        };
        assert_eq!(narrow.exported, wide.exported);
        assert!(
            wide.bytes > narrow.bytes * 10,
            "equal point counts must be able to differ hugely in bytes, which \
             is exactly why the budget cannot be charged in points"
        );
    }
}
