//! Refuse writes whose names would share storage with other names.
//!
//! Tables and columns are named from sanitised identifiers
//! ([`crate::domain::chdb_naming`]): every character outside
//! `[A-Za-z0-9_]` becomes `_`, and a table is `<db>_<rp>_<measurement>`.
//! So `cpu.load` and `cpu_load` are one table, as are database `a` with
//! retention policy `b_c` and database `a_b` with policy `c`, and the
//! fields `a.b` and `a_b` are one column. Writing the second name of such a
//! pair overwrote and read back the first one's data.
//!
//! The names stay as they are, since tables already stored under them
//! would otherwise be lost. Instead the client write path checks each
//! measurement, field and tag key it has not checked before, and refuses
//! the write (400) when a new name maps onto storage another name already
//! owns. The check runs before the WAL append, so a refused write is never
//! flushed. Replication and shard transfers do not check: they carry data
//! an entry node already accepted.

use std::collections::{BTreeMap, BTreeSet, HashMap, HashSet};
use std::hash::{Hash, Hasher};
use std::sync::Arc;

use parking_lot::RwLock;

use crate::domain::chdb_naming::{
    field_column_name, tag_column_name, unquoted_series_table_name, unquoted_table_name,
};
use crate::domain::point::Point;
use crate::error::HyperbytedbError;
use crate::ports::metadata::MetadataPort;

/// Upper bound on remembered names; past it the set is cleared and names
/// are checked again as they recur.
const MAX_CHECKED: usize = 1 << 20;

/// Names (measurement, field and tag keys) already checked by this node.
#[derive(Default)]
pub struct PhysicalNameGuard {
    checked: RwLock<HashSet<u64>>,
    /// [`MetadataPort::drop_generation`] `checked` was filled at: a dropped
    /// name no longer owns its table, and one that took it since must not
    /// be shadowed by the first being remembered as checked.
    drop_generation: std::sync::atomic::AtomicU64,
}

/// The names one batch writes to one measurement.
#[derive(Default)]
struct BatchNames<'a> {
    fields: BTreeSet<&'a str>,
    tags: BTreeSet<&'a str>,
}

fn name_hash(db: &str, rp: &str, measurement: &str, kind: u8, key: &str) -> u64 {
    let mut h = std::hash::DefaultHasher::new();
    (db, rp, measurement, kind, key).hash(&mut h);
    h.finish()
}

impl PhysicalNameGuard {
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// Refuse `points` if any of their names maps onto storage that another
    /// name already owns.
    ///
    /// # Errors
    /// [`HyperbytedbError::NameCollision`] for a colliding name, or a
    /// metadata error.
    pub async fn check_points(
        &self,
        metadata: &Arc<dyn MetadataPort>,
        db: &str,
        rp: &str,
        points: &[Point],
    ) -> Result<(), HyperbytedbError> {
        let mut by_meas: BTreeMap<&str, BatchNames<'_>> = BTreeMap::new();
        let mut last: Option<&str> = None;
        for p in points {
            // Batches are mostly runs of one measurement.
            if last != Some(p.measurement.as_str()) {
                by_meas.entry(p.measurement.as_str()).or_default();
                last = Some(p.measurement.as_str());
            }
            let Some(names) = by_meas.get_mut(p.measurement.as_str()) else {
                continue;
            };
            for k in p.fields.keys() {
                if !names.fields.contains(k.as_str()) {
                    names.fields.insert(k);
                }
            }
            for k in p.tags.keys() {
                if !names.tags.contains(k.as_str()) {
                    names.tags.insert(k);
                }
            }
        }
        for (measurement, names) in &by_meas {
            self.check_measurement(metadata, db, rp, measurement, names)
                .await?;
        }
        Ok(())
    }

    /// [`Self::check_points`] for a columnar batch: one measurement, one
    /// field, and its tags.
    ///
    /// # Errors
    /// As [`Self::check_points`].
    pub async fn check_columnar(
        &self,
        metadata: &Arc<dyn MetadataPort>,
        db: &str,
        rp: &str,
        measurement: &str,
        field: &str,
        tags: impl Iterator<Item = &str>,
    ) -> Result<(), HyperbytedbError> {
        let names = BatchNames {
            fields: BTreeSet::from([field]),
            tags: tags.collect(),
        };
        self.check_measurement(metadata, db, rp, measurement, &names)
            .await
    }

    async fn check_measurement(
        &self,
        metadata: &Arc<dyn MetadataPort>,
        db: &str,
        rp: &str,
        measurement: &str,
        names: &BatchNames<'_>,
    ) -> Result<(), HyperbytedbError> {
        let mut hashes: Vec<u64> = Vec::with_capacity(1 + names.fields.len() + names.tags.len());
        hashes.push(name_hash(db, rp, measurement, 0, ""));
        hashes.extend(
            names
                .fields
                .iter()
                .map(|k| name_hash(db, rp, measurement, 1, k)),
        );
        hashes.extend(
            names
                .tags
                .iter()
                .map(|k| name_hash(db, rp, measurement, 2, k)),
        );
        let generation = metadata.drop_generation();
        if self
            .drop_generation
            .swap(generation, std::sync::atomic::Ordering::AcqRel)
            != generation
        {
            self.checked.write().clear();
        }
        if hashes.iter().all(|h| self.checked.read().contains(h)) {
            return Ok(());
        }

        let existing = metadata.get_measurement(db, rp, measurement).await?;
        if existing.is_none() {
            check_table_names(metadata, db, rp, measurement).await?;
        }
        let (stored_fields, stored_tags) = existing
            .map(|m| (m.field_types.into_keys().collect(), m.tag_keys))
            .unwrap_or_else(|| (Vec::new(), Vec::new()));
        check_column_names(measurement, &stored_fields, &stored_tags, names)?;

        let mut checked = self.checked.write();
        if checked.len() + hashes.len() > MAX_CHECKED {
            checked.clear();
        }
        checked.extend(hashes);
        Ok(())
    }
}

/// A new measurement's fact and series tables must not be another
/// measurement's, in any database or retention policy.
async fn check_table_names(
    metadata: &Arc<dyn MetadataPort>,
    db: &str,
    rp: &str,
    measurement: &str,
) -> Result<(), HyperbytedbError> {
    let mine = [
        unquoted_table_name(db, rp, measurement).to_string(),
        unquoted_series_table_name(db, rp, measurement).to_string(),
    ];
    for database in metadata.list_databases().await? {
        let rps: BTreeSet<&str> = database
            .retention_policies
            .iter()
            .map(|r| r.name.as_str())
            .collect();
        for other_rp in rps {
            for other in metadata
                .list_measurements_for_rp(&database.name, other_rp)
                .await?
            {
                if database.name == db && other_rp == rp && other == measurement {
                    continue;
                }
                let theirs = [
                    unquoted_table_name(&database.name, other_rp, &other).to_string(),
                    unquoted_series_table_name(&database.name, other_rp, &other).to_string(),
                ];
                if mine.iter().any(|n| theirs.contains(n)) {
                    return Err(HyperbytedbError::NameCollision(format!(
                        "measurement \"{measurement}\" (database \"{db}\", retention policy \
                         \"{rp}\") would be stored with measurement \"{other}\" (database \
                         \"{}\", retention policy \"{other_rp}\"); rename it",
                        database.name
                    )));
                }
            }
        }
    }
    Ok(())
}

/// A new field key, or tag key, of one measurement must not share a column
/// with another. Stored keys that already share one (written before this
/// check) are left alone, so their measurement can still be written.
fn check_column_names(
    measurement: &str,
    stored_fields: &[String],
    stored_tags: &[String],
    names: &BatchNames<'_>,
) -> Result<(), HyperbytedbError> {
    let field_set: HashSet<&str> = stored_fields
        .iter()
        .map(String::as_str)
        .chain(names.fields.iter().copied())
        .collect();
    first_new_collision(stored_fields, &names.fields, field_column_name).map_or(
        Ok(()),
        |(new, other)| {
            Err(HyperbytedbError::NameCollision(format!(
                "field \"{new}\" of measurement \"{measurement}\" would be stored with field \
                 \"{other}\"; rename it"
            )))
        },
    )?;
    first_new_collision(stored_tags, &names.tags, |t| tag_column_name(t, &field_set)).map_or(
        Ok(()),
        |(new, other)| {
            Err(HyperbytedbError::NameCollision(format!(
                "tag key \"{new}\" of measurement \"{measurement}\" would be stored with tag key \
                 \"{other}\"; rename it"
            )))
        },
    )
}

/// The first key of `batch` not in `stored` whose column is another key's.
fn first_new_collision<'a>(
    stored: &'a [String],
    batch: &BTreeSet<&'a str>,
    column: impl Fn(&str) -> String,
) -> Option<(&'a str, &'a str)> {
    let stored_set: HashSet<&str> = stored.iter().map(String::as_str).collect();
    let mut columns: HashMap<String, &str> =
        stored.iter().map(|k| (column(k), k.as_str())).collect();
    for &k in batch {
        if stored_set.contains(k) {
            continue;
        }
        match columns.get(&column(k)) {
            Some(&other) if other != k => return Some((k, other)),
            Some(_) => {}
            None => {
                columns.insert(column(k), k);
            }
        }
    }
    None
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::adapters::metadata::rocksdb_meta::RocksDbMetadata;
    use crate::domain::database::RetentionPolicy;
    use crate::domain::measurement::MeasurementMeta;
    use crate::domain::point::FieldValue;

    fn point(meas: &str, field: &str, tag: &str) -> Point {
        Point {
            measurement: meas.to_string(),
            tags: BTreeMap::from([(tag.to_string(), "x".to_string())]),
            fields: BTreeMap::from([(field.to_string(), FieldValue::Float(1.0))]),
            timestamp: 1,
            series_id_cache: Default::default(),
        }
    }

    async fn meta_with(
        dir: &tempfile::TempDir,
        db: &str,
        rp: &str,
        meas: &str,
    ) -> Arc<dyn MetadataPort> {
        let meta: Arc<dyn MetadataPort> = Arc::new(RocksDbMetadata::open(dir.path()).unwrap());
        meta.create_database(db).await.unwrap();
        if rp != "autogen" {
            let mut policy = RetentionPolicy::autogen();
            policy.name = rp.to_string();
            policy.is_default = false;
            meta.create_retention_policy(db, policy).await.unwrap();
        }
        register(&meta, db, rp, meas, "v", "host").await;
        meta
    }

    async fn register(
        meta: &Arc<dyn MetadataPort>,
        db: &str,
        rp: &str,
        meas: &str,
        field: &str,
        tag: &str,
    ) {
        meta.register_measurement(
            db,
            rp,
            &MeasurementMeta {
                name: meas.into(),
                field_types: HashMap::from([(field.to_string(), 0u8)]),
                tag_keys: vec![tag.to_string()],
                ..Default::default()
            },
        )
        .await
        .unwrap();
    }

    fn is_collision(r: Result<(), HyperbytedbError>) -> bool {
        matches!(r, Err(HyperbytedbError::NameCollision(_)))
    }

    #[tokio::test]
    async fn measurement_sharing_a_table_is_refused() {
        let dir = tempfile::tempdir().unwrap();
        let meta = meta_with(&dir, "db", "autogen", "cpu.load").await;
        let guard = PhysicalNameGuard::new();
        for m in ["cpu_load", "cpu load", "cpu-load"] {
            let r = guard
                .check_points(&meta, "db", "autogen", &[point(m, "v", "host")])
                .await;
            assert!(is_collision(r), "{m}");
        }
        // The owner itself, and unrelated names, still write.
        guard
            .check_points(&meta, "db", "autogen", &[point("cpu.load", "v", "host")])
            .await
            .unwrap();
        guard
            .check_points(&meta, "db", "autogen", &[point("cpu", "v", "host")])
            .await
            .unwrap();
    }

    #[tokio::test]
    async fn non_ascii_names_sharing_a_table_are_refused() {
        let dir = tempfile::tempdir().unwrap();
        let meta = meta_with(&dir, "db", "autogen", "温度").await;
        let r = PhysicalNameGuard::new()
            .check_points(&meta, "db", "autogen", &[point("湿度", "v", "host")])
            .await;
        assert!(is_collision(r));
    }

    #[tokio::test]
    async fn another_database_sharing_a_table_is_refused() {
        // db `a_b` rp `c` and db `a` rp `b_c` are both table `a_b_c_m`.
        let dir = tempfile::tempdir().unwrap();
        let meta = meta_with(&dir, "a_b", "c", "m").await;
        meta.create_database("a").await.unwrap();
        let r = PhysicalNameGuard::new()
            .check_points(&meta, "a", "b_c", &[point("m", "v", "host")])
            .await;
        assert!(is_collision(r));
    }

    #[tokio::test]
    async fn measurement_named_like_a_series_table_is_refused() {
        let dir = tempfile::tempdir().unwrap();
        let meta = meta_with(&dir, "db", "autogen", "cpu").await;
        let r = PhysicalNameGuard::new()
            .check_points(&meta, "db", "autogen", &[point("cpu_series", "v", "host")])
            .await;
        assert!(is_collision(r));
    }

    #[tokio::test]
    async fn fields_or_tags_sharing_a_column_are_refused() {
        let dir = tempfile::tempdir().unwrap();
        let meta = meta_with(&dir, "db", "autogen", "cpu").await;
        let guard = PhysicalNameGuard::new();
        let r = guard
            .check_points(&meta, "db", "autogen", &[point("cpu", "v.", "host")])
            .await;
        assert!(r.is_ok(), "`v_` is not `v`");
        let r = guard
            .check_points(&meta, "db", "autogen", &[point("mem", "a.b", "t")])
            .await;
        assert!(r.is_ok());
        // Within one batch, and against the stored schema.
        let r = guard
            .check_points(
                &meta,
                "db",
                "autogen",
                &[point("disk", "a.b", "t"), point("disk", "a_b", "t")],
            )
            .await;
        assert!(is_collision(r));
        register(&meta, "db", "autogen", "net", "a.b", "host.name").await;
        let r = guard
            .check_points(&meta, "db", "autogen", &[point("net", "a_b", "host.name")])
            .await;
        assert!(is_collision(r));
        let r = guard
            .check_points(&meta, "db", "autogen", &[point("net", "a.b", "host_name")])
            .await;
        assert!(is_collision(r));
    }

    #[tokio::test]
    async fn stored_keys_that_already_collide_still_write() {
        let dir = tempfile::tempdir().unwrap();
        let meta = meta_with(&dir, "db", "autogen", "cpu").await;
        meta.register_measurement(
            "db",
            "autogen",
            &MeasurementMeta {
                name: "old".into(),
                field_types: HashMap::from([("a.b".to_string(), 0u8), ("a_b".to_string(), 0u8)]),
                tag_keys: vec!["host".to_string()],
                ..Default::default()
            },
        )
        .await
        .unwrap();
        PhysicalNameGuard::new()
            .check_points(&meta, "db", "autogen", &[point("old", "a_b", "host")])
            .await
            .unwrap();
    }

    #[tokio::test]
    async fn columnar_batches_are_checked() {
        let dir = tempfile::tempdir().unwrap();
        let meta = meta_with(&dir, "db", "autogen", "cpu.load").await;
        let r = PhysicalNameGuard::new()
            .check_columnar(
                &meta,
                "db",
                "autogen",
                "cpu_load",
                "v",
                ["host"].into_iter(),
            )
            .await;
        assert!(is_collision(r));
    }

    #[tokio::test]
    async fn a_refused_name_stays_refused() {
        let dir = tempfile::tempdir().unwrap();
        let meta = meta_with(&dir, "db", "autogen", "cpu.load").await;
        let guard = PhysicalNameGuard::new();
        for _ in 0..2 {
            let r = guard
                .check_points(&meta, "db", "autogen", &[point("cpu_load", "v", "host")])
                .await;
            assert!(is_collision(r));
        }
    }
}
