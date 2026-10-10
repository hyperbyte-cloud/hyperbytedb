//! Moving a departing node's materialized view rollups to a node that stays.
//!
//! A view rolls up each row on the node that first stored it, into that
//! node's own destination tables, and a destination query adds every node's
//! rollups. A node that left the cluster took its rollups with it: on n3 a
//! view counted exactly at 3 and 6 nodes held 44,700 of 60,000 points after
//! scaling back to 3. Before a departing node is let go, its destination rows
//! are now inserted into a node that stays, then removed here.
//!
//! By then the departing node holds no source region, so nothing new is
//! rolled up here and its destination tables are fixed. They are sent in
//! `series_id` ranges sized from the row count; each range is every row in
//! it, so no row is sent twice and none is skipped, and nothing needs
//! aggregating on the way (min/max columns would not survive a sum). The
//! receiver inserts each range with a deduplication token derived from the
//! range, so a retried range counts once.

use std::collections::HashSet;

use serde::{Deserialize, Serialize};

use crate::domain::chdb_naming::{quoted_series_table_name, quoted_table_name};
use crate::domain::sharding::MeasurementKey;
use crate::error::HyperbytedbError;
use crate::ports::metadata::MetadataPort;
use crate::ports::query::QueryPort;

/// Rows per range, from the table's row count.
pub const HANDOFF_CHUNK_ROWS: u64 = 50_000;

/// Inserts a destination table remembers for deduplication. Far more than a
/// handoff sends, so every retried range is recognised.
const DEDUP_WINDOW: u64 = 100_000;

/// `POST /internal/mv/handoff`, to the departing node.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct MvHandoffRequest {
    /// The node that keeps the rollups (`host:port`).
    pub to_addr: String,
}

/// `POST /internal/mv/handoff-chunk`, to the receiving node: one range of
/// one destination table.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct MvHandoffChunk {
    pub db: String,
    pub rp: String,
    pub measurement: String,
    /// The `_series` dimension table rather than the fact table.
    pub series: bool,
    pub token: String,
    /// `JSONEachRow`.
    pub rows: String,
}

/// `n` half-open `series_id` ranges covering every id; the last is open
/// ended so `u64::MAX` is included.
#[must_use]
pub fn handoff_ranges(n: u64) -> Vec<(u64, Option<u64>)> {
    let n = n.max(1);
    let span = (u128::from(u64::MAX) + 1) / u128::from(n);
    (0..n)
        .map(|i| {
            let lo = (span * u128::from(i)) as u64;
            let hi = (i + 1 < n).then(|| (span * u128::from(i + 1)) as u64);
            (lo, hi)
        })
        .collect()
}

/// The deduplication token of one range: the same range of the same table
/// from the same node always gets the same token.
#[must_use]
pub fn handoff_token(
    from_node: u64,
    key: &MeasurementKey,
    series: bool,
    index: usize,
    ranges: usize,
) -> String {
    let raw = format!(
        "hb-mv-handoff-{from_node}-{}.{}.{}-{}-{index}-of-{ranges}",
        key.db,
        key.rp,
        key.measurement,
        if series { "series" } else { "fact" }
    );
    raw.chars()
        .map(|c| {
            if c.is_ascii_alphanumeric() || c == '-' || c == '.' {
                c
            } else {
                '_'
            }
        })
        .collect()
}

fn table_for(key: &MeasurementKey, series: bool) -> String {
    if series {
        quoted_series_table_name(&key.db, &key.rp, &key.measurement)
            .as_str()
            .to_string()
    } else {
        quoted_table_name(&key.db, &key.rp, &key.measurement)
            .as_str()
            .to_string()
    }
}

fn range_predicate(lo: u64, hi: Option<u64>) -> String {
    match hi {
        Some(hi) => format!("`series_id` >= {lo} AND `series_id` < {hi}"),
        None => format!("`series_id` >= {lo}"),
    }
}

async fn row_count(query: &dyn QueryPort, table: &str) -> Result<u64, HyperbytedbError> {
    let out = query
        .execute_sql(&format!(
            "SELECT count() AS c FROM {table} FORMAT JSONEachRow"
        ))
        .await?;
    Ok(out
        .lines()
        .next()
        .and_then(|l| serde_json::from_str::<serde_json::Value>(l).ok())
        .and_then(|v| {
            v.get("c")
                .and_then(|c| c.as_u64().or_else(|| c.as_str()?.parse().ok()))
        })
        .unwrap_or(0))
}

/// Every row of `table` in one range, as `JSONEachRow`.
pub async fn export_range(
    query: &dyn QueryPort,
    table: &str,
    lo: u64,
    hi: Option<u64>,
) -> Result<String, HyperbytedbError> {
    query
        .execute_sql(&format!(
            "SELECT * FROM {table} WHERE {} FORMAT JSONEachRow",
            range_predicate(lo, hi)
        ))
        .await
}

/// Insert one exported range into `table`; a range already inserted under
/// `token` is ignored.
pub async fn import_range(
    query: &dyn QueryPort,
    table: &str,
    token: &str,
    rows: &str,
) -> Result<(), HyperbytedbError> {
    if rows.trim().is_empty() {
        return Ok(());
    }
    if !token
        .chars()
        .all(|c| c.is_ascii_alphanumeric() || c == '-' || c == '.' || c == '_')
    {
        return Err(HyperbytedbError::QueryParse(format!(
            "invalid handoff token {token:?}"
        )));
    }
    query
        .execute_sql(&format!(
            "ALTER TABLE {table} MODIFY SETTING non_replicated_deduplication_window = {DEDUP_WINDOW}"
        ))
        .await?;
    query
        .execute_sql(&format!(
            "INSERT INTO {table} SETTINGS insert_deduplication_token = '{token}', \
             input_format_skip_unknown_fields = 1 FORMAT JSONEachRow\n{rows}"
        ))
        .await?;
    Ok(())
}

/// The destinations of every view this node knows.
async fn destinations(
    metadata: &dyn MetadataPort,
) -> Result<Vec<MeasurementKey>, HyperbytedbError> {
    let mut seen = HashSet::new();
    Ok(metadata
        .list_all_materialized_views()
        .await?
        .into_iter()
        .map(|d| MeasurementKey::new(d.dest_db, d.dest_rp, d.dest_measurement))
        .filter(|k| seen.insert(k.clone()))
        .collect())
}

/// Whether every view destination on this node is empty: nothing left to
/// hand off (it was handed off, or nothing was ever rolled up here).
pub async fn local_rollups_empty(
    metadata: &dyn MetadataPort,
    query: &dyn QueryPort,
) -> Result<bool, HyperbytedbError> {
    for key in destinations(metadata).await? {
        if row_count(query, &table_for(&key, false)).await? > 0 {
            return Ok(false);
        }
    }
    Ok(true)
}

/// Whether `node_id` is still a peer of a region it must give up before it
/// may leave. View destinations do not count: their regions never move, and
/// their rows leave by the rollup handoff instead.
#[must_use]
pub fn holds_regions_to_evacuate(
    map: &crate::domain::sharding::ShardMap,
    node_id: u64,
    dests: &HashSet<MeasurementKey>,
) -> bool {
    map.spaces
        .values()
        .filter(|s| !dests.contains(&s.key))
        .flat_map(|s| s.regions.iter())
        .any(|r| r.peers.contains(&node_id))
}

/// The destinations of every view this node knows, as a set.
pub async fn destination_set(
    metadata: &dyn MetadataPort,
) -> Result<HashSet<MeasurementKey>, HyperbytedbError> {
    Ok(destinations(metadata).await?.into_iter().collect())
}

/// Send every destination row on this node to `send`, one range at a time,
/// then empty the destination tables here. Returns the rows sent.
///
/// `send` delivers one chunk and returns once the receiver has inserted it.
/// A destination is emptied only after all of its ranges arrived; a failure
/// leaves it whole for the next attempt, whose ranges and tokens are the same.
pub async fn hand_off_rollups<F, Fut>(
    node_id: u64,
    metadata: &dyn MetadataPort,
    query: &dyn QueryPort,
    mut send: F,
) -> Result<u64, HyperbytedbError>
where
    F: FnMut(MvHandoffChunk) -> Fut,
    Fut: std::future::Future<Output = Result<(), HyperbytedbError>>,
{
    let mut sent = 0u64;
    for key in destinations(metadata).await? {
        for series in [false, true] {
            let table = table_for(&key, series);
            let count = row_count(query, &table).await?;
            if count == 0 {
                continue;
            }
            let ranges = handoff_ranges(count.div_ceil(HANDOFF_CHUNK_ROWS));
            for (index, (lo, hi)) in ranges.iter().enumerate() {
                let rows = export_range(query, &table, *lo, *hi).await?;
                if rows.trim().is_empty() {
                    continue;
                }
                let n = rows.lines().filter(|l| !l.trim().is_empty()).count() as u64;
                send(MvHandoffChunk {
                    db: key.db.clone(),
                    rp: key.rp.clone(),
                    measurement: key.measurement.clone(),
                    series,
                    token: handoff_token(node_id, &key, series, index, ranges.len()),
                    rows,
                })
                .await?;
                sent += n;
            }
        }
        for series in [false, true] {
            query
                .execute_sql(&format!(
                    "TRUNCATE TABLE IF EXISTS {}",
                    table_for(&key, series)
                ))
                .await?;
        }
        tracing::info!(
            db = %key.db,
            rp = %key.rp,
            measurement = %key.measurement,
            "materialized view rollups handed off"
        );
    }
    Ok(sent)
}

/// Receive one chunk: only into a destination of a view this node knows.
pub async fn receive_chunk(
    metadata: &dyn MetadataPort,
    query: &dyn QueryPort,
    points_sink: &dyn crate::ports::points_sink::PointsSinkPort,
    chunk: &MvHandoffChunk,
) -> Result<(), HyperbytedbError> {
    let key = MeasurementKey::new(&chunk.db, &chunk.rp, &chunk.measurement);
    if !destinations(metadata).await?.contains(&key) {
        return Err(HyperbytedbError::QueryParse(format!(
            "{}.{}.{} is not a materialized view destination here",
            chunk.db, chunk.rp, chunk.measurement
        )));
    }
    if let Some(meta) = metadata
        .get_measurement(&chunk.db, &chunk.rp, &chunk.measurement)
        .await?
    {
        points_sink
            .ensure_measurement_schema(&chunk.db, &chunk.rp, &meta)
            .await?;
    }
    import_range(
        query,
        &table_for(&key, chunk.series),
        &chunk.token,
        &chunk.rows,
    )
    .await
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn ranges_cover_every_id_once() {
        for n in [1, 2, 3, 7, 64] {
            let r = handoff_ranges(n);
            assert_eq!(r.len() as u64, n);
            assert_eq!(r[0].0, 0);
            assert_eq!(r.last().unwrap().1, None, "last range open ended");
            for w in r.windows(2) {
                assert_eq!(w[0].1, Some(w[1].0), "adjacent, no gap or overlap");
            }
        }
    }

    #[test]
    fn tokens_are_stable_and_safe() {
        let key = MeasurementKey::new("my db", "autogen", "cpu'1m");
        let a = handoff_token(5, &key, false, 2, 9);
        assert_eq!(a, handoff_token(5, &key, false, 2, 9));
        assert_ne!(a, handoff_token(5, &key, true, 2, 9));
        assert_ne!(a, handoff_token(6, &key, false, 2, 9));
        assert!(
            a.chars()
                .all(|c| c.is_ascii_alphanumeric() || "-._".contains(c))
        );
    }

    /// Destination regions never move, so they must not count as regions a
    /// departing node still has to give up.
    #[test]
    fn destination_regions_do_not_hold_a_node() {
        use crate::domain::sharding::{MeasurementShardSpace, ShardMap, ShardRegion};
        let region = |peers: Vec<u64>| ShardRegion {
            region_id: 1,
            start: 0,
            end: u64::MAX,
            epoch: Default::default(),
            peers,
            primary: 1,
            last_split_at: 0,
            transfer_verified: None,
            transfer_first_seen: None,
            state: Default::default(),
            staged_from: None,
            staging_peers: Vec::new(),
        };
        let raw = MeasurementKey::new("db", "autogen", "cpu");
        let dest = MeasurementKey::new("db", "autogen", "cpu_1m");
        let mut map = ShardMap::default();
        map.spaces.insert(
            dest.clone(),
            MeasurementShardSpace {
                key: dest.clone(),
                regions: vec![region(vec![1, 4])],
            },
        );
        map.spaces.insert(
            raw.clone(),
            MeasurementShardSpace {
                key: raw.clone(),
                regions: vec![region(vec![1, 2])],
            },
        );
        let dests = HashSet::from([dest]);
        assert!(!holds_regions_to_evacuate(&map, 4, &dests));
        assert!(holds_regions_to_evacuate(&map, 4, &HashSet::new()));
        assert!(holds_regions_to_evacuate(&map, 2, &dests));
    }

    /// Two tables in one chDB stand for the departing node's destination and
    /// the receiver's: sending every range twice (a retry) must land each
    /// row once.
    #[tokio::test]
    #[serial_test::serial(chdb)]
    async fn a_retried_range_is_inserted_once() {
        use crate::adapters::chdb::query_adapter::ChdbQueryAdapter;
        use crate::adapters::chdb::session::SharedSession;

        let dir = tempfile::tempdir().unwrap();
        let chdb = SharedSession::new_eager(dir.path().to_str().unwrap(), 1).unwrap();
        let q = ChdbQueryAdapter::from_shared(chdb, 0);
        for t in ["zz_from", "zz_to"] {
            q.execute_sql(&format!(
                "CREATE TABLE {t} (`series_id` UInt64, `time` DateTime64(9, 'UTC'), `v` Float64) \
                 ENGINE = SummingMergeTree((`v`)) ORDER BY (`series_id`, `time`)"
            ))
            .await
            .unwrap();
        }
        q.execute_sql(
            "INSERT INTO zz_from SELECT number * 3074457345618258602 + 7, \
             toDateTime64('2027-01-01 00:00:00', 9, 'UTC'), 1.5 FROM numbers(6)",
        )
        .await
        .unwrap();

        let ranges = handoff_ranges(3);
        for _attempt in 0..2 {
            for (i, (lo, hi)) in ranges.iter().enumerate() {
                let rows = export_range(&q, "zz_from", *lo, *hi).await.unwrap();
                let token = format!("t-{i}");
                import_range(&q, "zz_to", &token, &rows).await.unwrap();
            }
        }
        let out = q
            .execute_sql("SELECT count() AS c, sum(`v`) AS s FROM zz_to FORMAT JSONEachRow")
            .await
            .unwrap();
        let v: serde_json::Value = serde_json::from_str(out.lines().next().unwrap()).unwrap();
        assert_eq!(
            v["c"].as_u64().or_else(|| v["c"].as_str()?.parse().ok()),
            Some(6)
        );
        assert_eq!(v["s"].as_f64(), Some(9.0));
    }
}
