//! A dropped series range must not leave `known_series` claiming the series is
//! still registered.
//!
//! `series_known` gates the series dimension-row insert. `drop_measurement`
//! evicts the cache; `delete_series_id_range` -- the path a region transfer
//! takes with `drop_source: true` -- did not. So after a region moved away,
//! the next write for one of those series ids wrote a FACT row and skipped the
//! SERIES row. Reads join fact -> series and returned an empty tag; region
//! export saw a row whose series entry was missing and refused the chunk.
//!
//! Observed on a 6-node cluster: 2575 of 4932 rows reading host='' after
//! region churn, with hyperbytedb_shard_transfer_missing_series_total at 887.

use std::collections::BTreeMap;
use std::sync::Arc;

use hyperbytedb::adapters::chdb::native_adapter::ChdbNativeAdapter;
use hyperbytedb::adapters::chdb::query_adapter::ChdbQueryAdapter;
use hyperbytedb::adapters::chdb::session::SharedSession;
use hyperbytedb::domain::chdb_naming::{quoted_series_table_name, quoted_table_name};
use hyperbytedb::domain::point::{FieldValue, Point};
use hyperbytedb::ports::points_sink::PointsSinkPort;
use hyperbytedb::ports::query::QueryPort;

fn point(host: &str, ts: i64) -> Point {
    let mut tags = BTreeMap::new();
    tags.insert("host".to_string(), host.to_string());
    let mut fields = BTreeMap::new();
    fields.insert("value".to_string(), FieldValue::Float(1.0));
    Point {
        measurement: "cpu".to_string(),
        tags,
        fields,
        timestamp: ts,
    }
}

#[tokio::test(flavor = "multi_thread")]
#[serial_test::serial(chdb)]
async fn rewriting_a_series_after_its_range_was_dropped_keeps_its_tags() {
    let dir = tempfile::tempdir().unwrap();
    let chdb = SharedSession::new_eager(dir.path().to_str().unwrap(), 1).unwrap();
    let sink: Arc<dyn PointsSinkPort> = Arc::new(ChdbNativeAdapter::new(chdb.clone()));

    let db = "t";
    let rp = "autogen";

    // 1. Write the series, so it is registered in known_series.
    sink.write_points(db, rp, "cpu", &[1], 0, &[point("s1", 1_000_000_000)])
        .await
        .unwrap();

    // 2. Drop the whole series-id space, exactly as a region hand-off with
    //    drop_source: true does.
    sink.delete_series_id_range(db, rp, "cpu", 0, u64::MAX)
        .await
        .unwrap();

    // 3. Write the SAME series again. The series row must be recreated; if the
    //    cache still claims it is registered, only the fact row is written.
    sink.write_points(db, rp, "cpu", &[1], 1, &[point("s1", 2_000_000_000)])
        .await
        .unwrap();

    // 4. The tag must come back. A fact row with no series row reads as ''.
    let query = ChdbQueryAdapter::from_shared(chdb.clone(), 0);
    let fact = quoted_table_name(db, rp, "cpu");
    let series = quoted_series_table_name(db, rp, "cpu");
    let rows = query
        .execute_sql(&format!(
            "SELECT s.`host` AS h FROM {fact} AS f \
             ANY LEFT JOIN {series} AS s ON f.`series_id` = s.`series_id` \
             FORMAT JSONEachRow"
        ))
        .await
        .expect("series join query must succeed");

    assert!(
        rows.contains("s1"),
        "series row was not recreated after its range was dropped; \
         the tag reads empty. rows={rows}"
    );
}
