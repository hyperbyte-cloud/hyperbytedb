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

    let limits = IngestCardinalityLimits::unlimited();
    prepare_batch_metadata(metadata, &req.db, &req.rp, &filtered, &limits, None).await?;

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
/// Where a region export resumes: strictly after this `(series_id, time)`,
/// the fact table's sort key.
///
/// The export paged on `series_id` alone, re-reading the boundary series
/// from its start. A series with more rows than one chunk then never
/// advanced, and its remaining rows were skipped with a warning: any series
/// older than about 6 days at 10 s resolution lost rows on every move.
/// Every version of one point (same series and time, another `ingest_seq`)
/// lands in the same chunk (`LIMIT ... WITH TIES`), so resuming after the
/// time cannot lose one.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ExportCursor {
    pub series_id: u64,
    pub time_nanos: i64,
}

/// One chunk of a region export: the line-protocol body, the points in it,
/// the rows read (a row without fields sends nothing but still counts), and
/// where the next chunk starts (`None` when nothing was read).
pub struct ExportChunk {
    pub body: Vec<u8>,
    pub points: usize,
    pub rows: usize,
    pub next: Option<ExportCursor>,
}

pub async fn export_region_points_from_chdb(
    query: &dyn QueryPort,
    metadata: &dyn MetadataPort,
    key: &MeasurementKey,
    region: &ShardRegion,
    after: Option<ExportCursor>,
    max_points: usize,
) -> Result<ExportChunk, HyperbytedbError> {
    let empty = ExportChunk {
        body: Vec::new(),
        points: 0,
        rows: 0,
        next: None,
    };
    use crate::domain::chdb_naming::{
        field_column_name, quote_backticks, quoted_series_table_name, quoted_table_name,
    };

    // No metadata here means no rows here: a node registers a measurement
    // before any of its points reach the WAL. That is nothing to copy, not an
    // error. Failing it made every move of such a region fail forever: on n3
    // a materialized view whose CREATE failed left its destination's shard
    // space behind with no measurement, its learners never promoted, and they
    // used up the new nodes' learner slots, so no other region moved there.
    let Some(meta) = metadata
        .get_measurement(&key.db, &key.rp, &key.measurement)
        .await?
    else {
        return Ok(empty);
    };

    let mapping = ColumnMapping::from_measurement_meta(&meta);
    let fact = quoted_table_name(&key.db, &key.rp, &key.measurement);
    let series = quoted_series_table_name(&key.db, &key.rp, &key.measurement);

    let mut fact_cols = vec![
        "`time`".to_string(),
        "`series_id`".to_string(),
        "`ingest_seq`".to_string(),
    ];
    let mut select_cols = vec![
        "f.`time`".to_string(),
        "f.`series_id`".to_string(),
        "f.`ingest_seq`".to_string(),
    ];
    for field in mapping.field_names.iter() {
        let phys = quote_backticks(&field_column_name(field));
        select_cols.push(format!("f.{phys}"));
        fact_cols.push(phys);
    }
    let mut series_cols = vec!["`series_id`".to_string()];
    for tag in mapping.tag_keys.iter() {
        let phys = quote_backticks(&mapping.physical_tag_column_name(tag));
        select_cols.push(format!("s.{phys}"));
        series_cols.push(phys);
    }
    // Witness for the ANY LEFT JOIN below. A LEFT JOIN that finds no series row
    // does not fail -- it substitutes column defaults, so every tag comes back
    // as an empty string and the row looks like a legitimately untagged series.
    // Selecting the joined key lets us tell "this series has no host" from
    // "this node has no series row for this series_id at all".
    select_cols.push("s.`series_id` AS `__joined_series_id`".to_string());

    // Series range for both tables; the fact table also resumes after the
    // cursor row.
    let mut predicates = vec![format!(
        "`series_id` >= {}",
        after.map_or(region.start, |c| c.series_id)
    )];
    if region.end != u64::MAX {
        predicates.push(format!("`series_id` < {}", region.end));
    }
    let range = predicates.join(" AND ");
    // Spelled out rather than as a tuple comparison, which the primary key
    // index does not narrow: 0.10 s against 0.62 s a chunk on n3.
    let fact_range = match after {
        Some(c) => format!(
            "{range} AND (`series_id` > {sid} OR (`series_id` = {sid} \
             AND `time` > fromUnixTimestamp64Nano({t}, 'UTC')))",
            sid = c.series_id,
            t = c.time_nanos
        ),
        None => range.clone(),
    };

    // The chunk is cut from the fact table before the join, in its sort
    // order, so it reads about one chunk; and only the range's series are
    // joined. Joining first sorted every remaining row of the region and
    // hashed every series on the node for each chunk: on n3 0.24 s a chunk
    // against 0.06 s, for the same 50,000 rows.
    //
    // The cut orders by the sort key alone so ClickHouse reads in index
    // order and stops after one chunk. Adding `ingest_seq` made it sort
    // every remaining row of the region for each chunk: 3.7 s a chunk on a
    // 280M-row region on n3 (0.13 s now), and copies ran at 0.3-0.5 MB/s.
    let sql = format!(
        "SELECT {} FROM (SELECT {} FROM {fact} WHERE {fact_range} \
         ORDER BY `series_id`, `time` LIMIT {max_points} WITH TIES) AS f \
         ANY LEFT JOIN (SELECT {} FROM {series} WHERE {range}) AS s \
         ON f.`series_id` = s.`series_id` \
         ORDER BY f.`series_id`, f.`time`, f.`ingest_seq` \
         FORMAT JSONEachRow",
        select_cols.join(", "),
        fact_cols.join(", "),
        series_cols.join(", "),
    );

    let raw = query.execute_sql(&sql).await?;
    if raw.trim().is_empty() {
        return Ok(empty);
    }

    let mut points = Vec::new();
    let mut rows = 0usize;
    let mut next: Option<ExportCursor> = None;
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

        let timestamp = point_timestamp(obj, &key.measurement)?;
        rows += 1;
        next = Some(ExportCursor {
            series_id: fact_sid,
            time_nanos: timestamp,
        });

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

        if fields.is_empty() {
            continue;
        }

        points.push(Point {
            measurement: key.measurement.clone(),
            tags,
            fields,
            timestamp,
            series_id_cache: Default::default(),
        });
    }

    let body = if points.is_empty() {
        Vec::new()
    } else {
        encode_points_to_line_protocol(&points, Precision::Nanosecond)?
    };
    Ok(ExportChunk {
        body,
        points: points.len(),
        rows,
        next,
    })
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
    let resp = send_transfer_push(peer_client.http_client(), &addr, payload).await?;
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

/// Send one push: the line protocol as the raw body, the rest of the payload
/// in a header. A node without the raw route (an older build) answers 404,
/// and gets the JSON push instead.
async fn send_transfer_push(
    client: &reqwest::Client,
    addr: &str,
    mut payload: ShardTransferPayload,
) -> Result<reqwest::Response, HyperbytedbError> {
    use base64::Engine as _;
    let unreachable = |e: reqwest::Error| HyperbytedbError::PeerUnreachable(e.to_string());
    let timeout = Duration::from_secs(TRANSFER_PUSH_TIMEOUT_SECS);
    // Bytes: the fallback below resends it without a copy.
    let body = bytes::Bytes::from(payload.body.take().unwrap_or_default());
    let header = serde_json::to_vec(&payload)
        .map(|json| base64::engine::general_purpose::STANDARD.encode(json))
        .map_err(|e| HyperbytedbError::Internal(format!("transfer header: {e}").into()))?;
    let resp = client
        .post(format!("http://{addr}/internal/shard/transfer/raw"))
        .header(
            crate::domain::sharding::transfer::TRANSFER_PAYLOAD_HEADER,
            header,
        )
        .header(reqwest::header::CONTENT_TYPE, "text/plain; charset=utf-8")
        .body(body.clone())
        .timeout(timeout)
        .send()
        .await
        .map_err(unreachable)?;
    if resp.status() != reqwest::StatusCode::NOT_FOUND {
        return Ok(resp);
    }
    // An older node, or a region the receiver does not know: the JSON route
    // answers the second the same way.
    payload.body = Some(body.to_vec());
    client
        .post(format!("http://{addr}/internal/shard/transfer"))
        .json(&payload)
        .timeout(timeout)
        .send()
        .await
        .map_err(unreachable)
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

/// Fewest points a chunk is cut down to after 413s.
const MIN_TRANSFER_CHUNK_POINTS: usize = 100;

/// Whether a push was refused as too large and the chunk can still shrink;
/// if so `max_points` is halved for the rest of the transfer.
fn smaller_chunks_after(
    pushed: &Result<(u64, u64), HyperbytedbError>,
    max_points: &mut usize,
) -> bool {
    if matches!(
        pushed,
        Err(HyperbytedbError::TransferRejected { status: 413, .. })
    ) && *max_points > MIN_TRANSFER_CHUNK_POINTS
    {
        *max_points = (*max_points / 2).max(MIN_TRANSFER_CHUNK_POINTS);
        counter!("hyperbytedb_shard_transfer_chunk_shrunk_total").increment(1);
        tracing::info!(
            max_points = *max_points,
            "transfer chunk too large for the receiver; halved"
        );
        true
    } else {
        false
    }
}

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
    // Halved when the receiver refuses a chunk as too large (413): the cap is
    // in points, the receiver's limit in bytes, and a point of a wide
    // measurement (Telegraf's cpu: 10 fields plus tags) is many times one of
    // a single field. The move otherwise retried the same chunk forever.
    let mut max_points = max_points.clamp(1, TRANSFER_CHUNK_POINTS);
    let transfer_id = new_transfer_id();
    let mut seq = 0u64;
    let mut exported = 0u64;
    let mut bytes = 0u64;
    let mut applied = 0u64;
    let started = std::time::Instant::now();

    // Unflushed WAL rows: chunked and resumable so regions with more pending
    // points than one chunk transfers never fail permanently (H4).
    //
    // Only unflushed: rows the flush has written are exported from chDB below.
    // Starting at 0 decoded the node's whole retained WAL for every copy, 18 GB
    // on n3, to keep the ~1/18 in this region and send it all again from chDB;
    // a copy then pushed nothing for minutes. Without a chDB pass there is no
    // other source, so the WAL is read from the start.
    let flushed = wal.flushed_through();
    let mut cursor = WalExportCursor {
        from_seq: if query_port.is_some() && flushed > 0 {
            flushed + 1
        } else {
            0
        },
        skip_points_in_head_entry: 0,
    };
    loop {
        let (body, count, next_cursor) =
            export_region_points_from_wal_chunked(wal, key, region, cursor, max_points, 0).await?;
        if !body.is_empty() {
            let pushed = push_region_transfer(
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
            .await;
            if smaller_chunks_after(&pushed, &mut max_points) {
                continue; // the same rows again, in a smaller chunk
            }
            let (chunk_applied, chunk_bytes) = pushed?;
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
        let export = |after: Option<ExportCursor>, max_points: usize| {
            export_region_points_from_chdb(
                query.as_ref(),
                metadata.as_ref(),
                key,
                region,
                after,
                max_points,
            )
        };
        let mut after: Option<ExportCursor> = None;
        let mut chunk = export(after, max_points).await?;
        loop {
            if chunk.rows == 0 {
                break;
            }
            let done = chunk.rows < max_points;
            if chunk.body.is_empty() {
                after = chunk.next;
                if done {
                    break;
                }
                chunk = export(after, max_points).await?;
                continue;
            }
            let (body, count, chunk_end) = (chunk.body, chunk.points, chunk.next);
            let push = push_region_transfer(
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
            );
            // Read the next chunk while this one is sent and applied: done in
            // turn, the sender sat idle through every apply. A chunk the
            // receiver refuses as too large discards the read-ahead.
            let (pushed, read_ahead) = if done {
                (push.await, None)
            } else {
                let (pushed, next) = tokio::join!(push, export(chunk_end, max_points));
                (pushed, Some(next))
            };
            if smaller_chunks_after(&pushed, &mut max_points) {
                // `after` unchanged: the same rows, smaller chunks.
                chunk = export(after, max_points).await?;
                continue;
            }
            let (chunk_applied, chunk_bytes) = pushed?;
            after = chunk_end;
            exported += count as u64;
            applied += chunk_applied;
            bytes += chunk_bytes;
            seq = seq.saturating_add(1);
            let wait = pacing_delay(max_bytes_per_sec, bytes, started.elapsed());
            if !wait.is_zero() {
                tokio::time::sleep(wait).await;
            }
            match read_ahead {
                Some(next) => chunk = next?,
                None => break,
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

    /// A region of a measurement this node has no metadata for holds no rows
    /// here: exporting it is empty, not an error. The error failed every move
    /// of an empty space forever, and its learners blocked other moves.
    #[tokio::test]
    async fn a_measurement_unknown_here_exports_nothing() {
        use crate::adapters::chdb::query_adapter::ChdbQueryAdapter;
        use crate::adapters::chdb::session::SharedSession;

        let dir = tempfile::tempdir().unwrap();
        let metadata: Arc<dyn MetadataPort> = Arc::new(RocksDbMetadata::open(dir.path()).unwrap());
        let query: Arc<dyn QueryPort> = Arc::new(ChdbQueryAdapter::from_shared(
            SharedSession::new("/nonexistent"),
            0,
        ));
        let region = ShardRegion {
            region_id: 1,
            start: 0,
            end: u64::MAX,
            epoch: Default::default(),
            peers: vec![1],
            primary: 1,
            last_split_at: 0,
            transfer_verified: None,
            transfer_first_seen: None,
            state: Default::default(),
            staged_from: None,
            staging_peers: Vec::new(),
        };
        let chunk = export_region_points_from_chdb(
            query.as_ref(),
            metadata.as_ref(),
            &MeasurementKey::new("db", "autogen", "cpu_1m"),
            &region,
            None,
            100,
        )
        .await
        .unwrap();
        assert!(chunk.body.is_empty());
        assert_eq!(chunk.points, 0);
        assert_eq!(chunk.next, None);
    }

    /// A copy sends every row of a series longer than a chunk, and of a wide
    /// measurement whose chunks are too big for the receiver. Paging on
    /// series_id alone never got past a long series and skipped its rest; a
    /// chunk over the receiver's byte limit was refused (413) forever.
    #[tokio::test]
    #[serial_test::serial(chdb)]
    async fn a_copy_sends_long_series_whole_and_shrinks_chunks_on_413() {
        long_series_copy(true).await;
    }

    /// A receiver on an older build has only the JSON route: its 404 on the
    /// raw one sends the same chunk as JSON.
    #[tokio::test]
    #[serial_test::serial(chdb)]
    async fn a_copy_to_an_older_node_falls_back_to_json() {
        long_series_copy(false).await;
    }

    async fn long_series_copy(raw_route: bool) {
        use crate::adapters::chdb::native_adapter::ChdbNativeAdapter;
        use crate::adapters::chdb::query_adapter::ChdbQueryAdapter;
        use crate::adapters::chdb::session::SharedSession;
        use crate::adapters::cluster::peer_client::PeerClient;
        use crate::adapters::cluster::replication_log::ReplicationLog;
        use crate::adapters::wal::rocksdb_wal::RocksDbWal;
        use crate::application::ingest_metadata::{
            IngestCardinalityLimits, prepare_batch_metadata,
        };
        use crate::domain::cluster::membership::{
            ClusterMembership, NodeInfo, NodeState, new_shared,
        };
        use crate::domain::point::{FieldValue, Point};
        use crate::ports::points_sink::PointsSinkPort;

        // A raw chunk is a third of its JSON form: a lower limit still
        // makes the receiver refuse the first, larger chunks.
        let limit: usize = if raw_route { 5_000 } else { 15_000 };
        let received: Arc<std::sync::Mutex<Vec<String>>> = Default::default();
        let refused = Arc::new(std::sync::atomic::AtomicU64::new(0));
        let raw_pushes = Arc::new(std::sync::atomic::AtomicU64::new(0));
        // Applies one push's line protocol: refuses it over LIMIT bytes (on
        // the wire), else records its lines.
        let apply = {
            let (seen, nope) = (received.clone(), refused.clone());
            move |wire_len: usize, raw: Vec<u8>| {
                if wire_len > limit {
                    nope.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
                    return (axum::http::StatusCode::PAYLOAD_TOO_LARGE, "too big").into_response();
                }
                let lines: Vec<String> = String::from_utf8_lossy(&raw)
                    .lines()
                    .filter(|l| !l.trim().is_empty())
                    .map(str::to_string)
                    .collect();
                let n = lines.len();
                seen.lock().unwrap().extend(lines);
                axum::Json(serde_json::json!({ "applied": n })).into_response()
            }
        };
        let json_apply = apply.clone();
        let mut app = axum::Router::new().route(
            "/internal/shard/transfer",
            axum::routing::post(move |body: axum::body::Bytes| {
                let apply = json_apply.clone();
                async move {
                    let payload: serde_json::Value = serde_json::from_slice(&body).unwrap();
                    let raw: Vec<u8> = payload["body"]
                        .as_array()
                        .map(|b| {
                            b.iter()
                                .filter_map(|v| v.as_u64())
                                .map(|n| n as u8)
                                .collect()
                        })
                        .unwrap_or_default();
                    apply(body.len(), raw)
                }
            }),
        );
        if raw_route {
            let raw_seen = raw_pushes.clone();
            app = app.route(
                "/internal/shard/transfer/raw",
                axum::routing::post(
                    move |headers: axum::http::HeaderMap, body: axum::body::Bytes| {
                        let (apply, raw_seen) = (apply.clone(), raw_seen.clone());
                        async move {
                            use base64::Engine as _;
                            let header = headers
                                .get(crate::domain::sharding::transfer::TRANSFER_PAYLOAD_HEADER)
                                .unwrap();
                            let json = base64::engine::general_purpose::STANDARD
                                .decode(header.as_bytes())
                                .unwrap();
                            let payload: ShardTransferPayload =
                                serde_json::from_slice(&json).unwrap();
                            assert!(payload.body.is_none(), "the body is not in the header");
                            raw_seen.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
                            apply(body.len(), body.to_vec())
                        }
                    },
                ),
            );
        }
        use axum::response::IntoResponse;
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap().to_string();
        tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });

        let dir = tempfile::tempdir().unwrap();
        let mut m = ClusterMembership::new();
        m.add_node(NodeInfo {
            node_id: 2,
            addr,
            state: NodeState::Active,
            joined_at: 0,
            last_heartbeat: 0,
            needs_sync: false,
            consecutive_misses: 0,
        });
        let pc = Arc::new(PeerClient::new(
            1,
            "127.0.0.1:1".into(),
            new_shared(m),
            Arc::new(ReplicationLog::open(dir.path().join("repl")).unwrap()),
            3,
            64,
            2,
            1 << 20,
            Default::default(),
        ));
        let chdb = SharedSession::new_eager(dir.path().join("chdb").to_str().unwrap(), 1).unwrap();
        let query: Arc<dyn QueryPort> = Arc::new(ChdbQueryAdapter::from_shared(chdb.clone(), 0));
        let sink = ChdbNativeAdapter::new(chdb);
        let metadata: Arc<dyn MetadataPort> =
            Arc::new(RocksDbMetadata::open(dir.path().join("meta")).unwrap());
        let wal: Arc<dyn WalPort> = Arc::new(RocksDbWal::open(dir.path().join("wal")).unwrap());

        let point = |host: &str, i: i64, v: f64| Point {
            measurement: "cpu".into(),
            tags: host_tag(host),
            fields: [("v".to_string(), FieldValue::Float(v))].into(),
            timestamp: (i + 1) * 1_000_000_000,
            series_id_cache: Default::default(),
        };
        // One series of 1,000 points (more than a chunk), a second short
        // one, and a second version of one point of the first.
        let long: Vec<Point> = (0..1_000).map(|i| point("h0", i, 1.0)).collect();
        let short: Vec<Point> = (0..10).map(|i| point("h1", i, 2.0)).collect();
        let again = vec![point("h0", 400, 9.0)];
        let limits = IngestCardinalityLimits::unlimited();
        let all: Vec<Point> = long.iter().chain(&short).chain(&again).cloned().collect();
        prepare_batch_metadata(&metadata, "db", "autogen", &all, &limits, None)
            .await
            .unwrap();
        for (seq, pts) in [(1u64, &long), (10_000, &short), (20_000, &again)] {
            sink.write_points("db", "autogen", "cpu", &vec![0; pts.len()], seq, pts)
                .await
                .unwrap();
        }
        wal.set_flushed_through(u64::MAX / 2);

        let region = ShardRegion {
            region_id: 1,
            start: 0,
            end: u64::MAX,
            epoch: Default::default(),
            peers: vec![1],
            primary: 1,
            last_split_at: 0,
            transfer_verified: None,
            transfer_first_seen: None,
            state: Default::default(),
            staged_from: None,
            staging_peers: Vec::new(),
        };
        let outcome = transfer_region_data(
            &pc,
            &metadata,
            &wal,
            Some(&query),
            1,
            &MeasurementKey::new("db", "autogen", "cpu"),
            &region,
            2,
            400,
            true,
            0,
        )
        .await
        .unwrap();

        let lines = received.lock().unwrap().clone();
        assert!(
            refused.load(std::sync::atomic::Ordering::SeqCst) > 0,
            "the receiver refused some"
        );
        assert_eq!(
            lines.len(),
            1_011,
            "every row once, both versions of the doubled point"
        );
        assert_eq!(outcome.exported, 1_011);
        let h0: std::collections::BTreeSet<&str> = lines
            .iter()
            .filter(|l| l.contains("host=h0"))
            .map(|l| l.rsplit(' ').next().unwrap())
            .collect();
        assert_eq!(h0.len(), 1_000, "every timestamp of the long series");
        assert_eq!(
            lines.iter().filter(|l| l.contains("v=9")).count(),
            1,
            "the second version"
        );
        assert_eq!(
            raw_pushes.load(std::sync::atomic::Ordering::SeqCst) > 0,
            raw_route,
            "pushes go raw where the route exists"
        );
    }

    /// Two versions of one point at a chunk boundary both go: a chunk ends
    /// only after every version of its last (series, time).
    #[tokio::test]
    #[serial_test::serial(chdb)]
    async fn a_chunk_never_splits_the_versions_of_a_point() {
        use crate::adapters::chdb::native_adapter::ChdbNativeAdapter;
        use crate::adapters::chdb::query_adapter::ChdbQueryAdapter;
        use crate::adapters::chdb::session::SharedSession;
        use crate::application::ingest_metadata::{
            IngestCardinalityLimits, prepare_batch_metadata,
        };
        use crate::domain::point::{FieldValue, Point};
        use crate::ports::points_sink::PointsSinkPort;

        let dir = tempfile::tempdir().unwrap();
        let chdb = SharedSession::new_eager(dir.path().join("chdb").to_str().unwrap(), 1).unwrap();
        let query = ChdbQueryAdapter::from_shared(chdb.clone(), 0);
        let sink = ChdbNativeAdapter::new(chdb);
        let metadata: Arc<dyn MetadataPort> =
            Arc::new(RocksDbMetadata::open(dir.path().join("meta")).unwrap());
        let point = |secs: i64, v: f64| Point {
            measurement: "cpu".into(),
            tags: host_tag("h0"),
            fields: [("v".to_string(), FieldValue::Float(v))].into(),
            timestamp: secs * 1_000_000_000,
            series_id_cache: Default::default(),
        };
        let first = vec![point(1, 1.0), point(2, 1.0), point(3, 1.0)];
        let second = vec![point(2, 2.0)];
        let all: Vec<Point> = first.iter().chain(&second).cloned().collect();
        prepare_batch_metadata(
            &metadata,
            "db",
            "autogen",
            &all,
            &IngestCardinalityLimits::unlimited(),
            None,
        )
        .await
        .unwrap();
        sink.write_points("db", "autogen", "cpu", &[0; 3], 1, &first)
            .await
            .unwrap();
        sink.write_points("db", "autogen", "cpu", &[0], 10, &second)
            .await
            .unwrap();

        let region = ShardRegion {
            region_id: 1,
            start: 0,
            end: u64::MAX,
            epoch: Default::default(),
            peers: vec![1],
            primary: 1,
            last_split_at: 0,
            transfer_verified: None,
            transfer_first_seen: None,
            state: Default::default(),
            staged_from: None,
            staging_peers: Vec::new(),
        };
        let key = MeasurementKey::new("db", "autogen", "cpu");
        let (mut after, mut rows) = (None, 0);
        loop {
            let chunk =
                export_region_points_from_chdb(&query, metadata.as_ref(), &key, &region, after, 2)
                    .await
                    .unwrap();
            if chunk.rows == 0 {
                break;
            }
            rows += chunk.rows;
            after = chunk.next;
        }
        assert_eq!(rows, 4, "both versions of the point at 2 s");
    }

    /// A copy sends each row once: flushed rows from chDB, the rest from the
    /// WAL. The WAL pass started at 0 and re-read every flushed entry, so it
    /// sent flushed rows twice and, on a node with an 18 GB WAL, pushed
    /// nothing for minutes while it decoded the lot.
    #[tokio::test]
    #[serial_test::serial(chdb)]
    async fn a_copy_reads_only_unflushed_wal_entries() {
        use crate::adapters::chdb::native_adapter::ChdbNativeAdapter;
        use crate::adapters::chdb::query_adapter::ChdbQueryAdapter;
        use crate::adapters::chdb::session::SharedSession;
        use crate::adapters::cluster::peer_client::PeerClient;
        use crate::adapters::cluster::replication_log::ReplicationLog;
        use crate::adapters::wal::rocksdb_wal::RocksDbWal;
        use crate::application::ingest_metadata::{
            IngestCardinalityLimits, prepare_batch_metadata,
        };
        use crate::domain::cluster::membership::{
            ClusterMembership, NodeInfo, NodeState, new_shared,
        };
        use crate::domain::point::{FieldValue, Point};
        use crate::ports::wal::WalEntry;
        use std::sync::atomic::{AtomicU64, Ordering};

        let received = Arc::new(AtomicU64::new(0));
        let seen = received.clone();
        let app = axum::Router::new().route(
            "/internal/shard/transfer",
            axum::routing::post(move |body: axum::body::Bytes| {
                let seen = seen.clone();
                async move {
                    let payload: serde_json::Value = serde_json::from_slice(&body).unwrap();
                    let raw: Vec<u8> = payload["body"]
                        .as_array()
                        .map(|b| {
                            b.iter()
                                .filter_map(|v| v.as_u64())
                                .map(|n| n as u8)
                                .collect()
                        })
                        .unwrap_or_default();
                    let lines = String::from_utf8_lossy(&raw)
                        .lines()
                        .filter(|l| !l.trim().is_empty())
                        .count() as u64;
                    seen.fetch_add(lines, Ordering::SeqCst);
                    axum::Json(serde_json::json!({ "applied": lines }))
                }
            }),
        );
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap().to_string();
        tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });

        let dir = tempfile::tempdir().unwrap();
        let mut m = ClusterMembership::new();
        m.add_node(NodeInfo {
            node_id: 2,
            addr,
            state: NodeState::Active,
            joined_at: 0,
            last_heartbeat: 0,
            needs_sync: false,
            consecutive_misses: 0,
        });
        let pc = Arc::new(PeerClient::new(
            1,
            "127.0.0.1:1".into(),
            new_shared(m),
            Arc::new(ReplicationLog::open(dir.path().join("repl")).unwrap()),
            3,
            64,
            2,
            1 << 20,
            Default::default(),
        ));
        let chdb = SharedSession::new_eager(dir.path().join("chdb").to_str().unwrap(), 1).unwrap();
        let query: Arc<dyn QueryPort> = Arc::new(ChdbQueryAdapter::from_shared(chdb.clone(), 0));
        let sink = ChdbNativeAdapter::new(chdb);
        let metadata: Arc<dyn MetadataPort> =
            Arc::new(RocksDbMetadata::open(dir.path().join("meta")).unwrap());
        let wal: Arc<dyn WalPort> = Arc::new(RocksDbWal::open(dir.path().join("wal")).unwrap());

        let points = |from: usize, n: usize| -> Vec<Point> {
            (from..from + n)
                .map(|i| Point {
                    measurement: "cpu".into(),
                    tags: host_tag(&format!("h{i}")),
                    fields: [("v".to_string(), FieldValue::Float(1.0))].into(),
                    timestamp: 1_000_000_000,
                    series_id_cache: Default::default(),
                })
                .collect()
        };
        let entry = |points: Vec<Point>| WalEntry {
            database: "db".into(),
            retention_policy: "autogen".into(),
            points,
            origin_node_id: 0,
        };
        let (flushed, unflushed) = (points(0, 100), points(100, 50));
        let all: Vec<Point> = flushed.iter().chain(&unflushed).cloned().collect();
        let limits = IngestCardinalityLimits::unlimited();
        prepare_batch_metadata(&metadata, "db", "autogen", &all, &limits, None)
            .await
            .unwrap();

        // What a flush does: the entry reaches chDB, then its watermark is set.
        let seq = wal.append(entry(flushed.clone())).await.unwrap();
        sink.write_points("db", "autogen", "cpu", &[0; 100], seq, &flushed)
            .await
            .unwrap();
        wal.set_flushed_through(seq);
        wal.append(entry(unflushed)).await.unwrap();

        let region = ShardRegion {
            region_id: 1,
            start: 0,
            end: u64::MAX,
            epoch: Default::default(),
            peers: vec![1],
            primary: 1,
            last_split_at: 0,
            transfer_verified: None,
            transfer_first_seen: None,
            state: Default::default(),
            staged_from: None,
            staging_peers: Vec::new(),
        };
        let outcome = transfer_region_data(
            &pc,
            &metadata,
            &wal,
            Some(&query),
            1,
            &MeasurementKey::new("db", "autogen", "cpu"),
            &region,
            2,
            10_000,
            true,
            0,
        )
        .await
        .unwrap();

        assert_eq!(
            received.load(Ordering::SeqCst),
            150,
            "each of the 150 rows once: 100 from chDB, 50 from the WAL"
        );
        assert_eq!(outcome.exported, 150);
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
