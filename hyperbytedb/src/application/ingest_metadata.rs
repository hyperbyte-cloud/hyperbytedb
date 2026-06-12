//! Shared metadata registration for ingest and replication paths (cardinality, schema, tag values).

use std::collections::{BTreeSet, HashMap, HashSet};
use std::hash::{Hash, Hasher};
use std::sync::Arc;

use parking_lot::RwLock;

use crate::domain::point::Point;
use crate::error::HyperbytedbError;
use crate::ports::metadata::{MeasurementMeta, MetadataPort};

/// Cardinality limits (0 = unlimited for that bound), matching [`crate::config::CardinalityConfig`].
#[derive(Debug, Clone, Copy, Default)]
pub struct IngestCardinalityLimits {
    pub max_tag_values_per_measurement: usize,
    pub max_measurements_per_database: usize,
}

/// Fast in-memory cache for the ingest hot path.  Tracks which measurement
/// schemas and tag values have already been persisted so that
/// `prepare_batch_metadata` can return immediately with zero I/O when
/// nothing has changed (the common steady-state case).
pub struct IngestSchemaCache {
    /// (db, measurement) → hash of (field_types, tag_keys) for schema identity.
    schema: RwLock<HashMap<u64, u64>>,
    /// Set of hashed (db, measurement, tag_key, tag_value) tuples already persisted.
    tags: RwLock<HashSet<u64>>,
}

impl Default for IngestSchemaCache {
    fn default() -> Self {
        Self::new()
    }
}

impl IngestSchemaCache {
    pub fn new() -> Self {
        Self {
            schema: RwLock::new(HashMap::with_capacity(256)),
            tags: RwLock::new(HashSet::with_capacity(4096)),
        }
    }

    fn hash_key(a: &str, b: &str) -> u64 {
        let mut h = std::collections::hash_map::DefaultHasher::new();
        a.hash(&mut h);
        b.hash(&mut h);
        h.finish()
    }

    fn hash_schema(field_types: &HashMap<String, u8>, tag_keys: &BTreeSet<String>) -> u64 {
        let mut h = std::collections::hash_map::DefaultHasher::new();
        let mut fields: Vec<(&String, &u8)> = field_types.iter().collect();
        fields.sort_by_key(|(k, _)| *k);
        for (k, v) in &fields {
            k.hash(&mut h);
            v.hash(&mut h);
        }
        for k in tag_keys {
            k.hash(&mut h);
        }
        h.finish()
    }

    pub fn hash_tag(db: &str, meas: &str, tag_key: &str, tag_value: &str) -> u64 {
        let mut h = std::collections::hash_map::DefaultHasher::new();
        db.hash(&mut h);
        meas.hash(&mut h);
        tag_key.hash(&mut h);
        tag_value.hash(&mut h);
        h.finish()
    }

    fn is_schema_known(
        &self,
        db: &str,
        meas: &str,
        field_types: &HashMap<String, u8>,
        tag_keys: &BTreeSet<String>,
    ) -> bool {
        let key = Self::hash_key(db, meas);
        let schema_hash = Self::hash_schema(field_types, tag_keys);
        let cache = self.schema.read();
        cache.get(&key) == Some(&schema_hash)
    }

    fn mark_schema(
        &self,
        db: &str,
        meas: &str,
        field_types: &HashMap<String, u8>,
        tag_keys: &BTreeSet<String>,
    ) {
        let key = Self::hash_key(db, meas);
        let schema_hash = Self::hash_schema(field_types, tag_keys);
        let mut cache = self.schema.write();
        cache.insert(key, schema_hash);
    }

    fn is_tag_known(&self, db: &str, meas: &str, tag_key: &str, tag_value: &str) -> bool {
        let h = Self::hash_tag(db, meas, tag_key, tag_value);
        self.is_tag_known_by_hash(h)
    }

    fn is_tag_known_by_hash(&self, h: u64) -> bool {
        let cache = self.tags.read();
        cache.contains(&h)
    }

    fn mark_tags(&self, entries: &[(u64,)]) {
        let mut cache = self.tags.write();
        for (h,) in entries {
            cache.insert(*h);
        }
    }
}

/// Merge tag keys from durable metadata with those seen in the current batch.
async fn merged_tag_keys(
    metadata: &Arc<dyn MetadataPort>,
    db: &str,
    measurement: &str,
    batch_tag_keys: &BTreeSet<String>,
) -> Result<BTreeSet<String>, HyperbytedbError> {
    let mut tag_keys = batch_tag_keys.clone();
    if let Some(existing) = metadata.get_measurement(db, measurement).await? {
        for k in &existing.tag_keys {
            tag_keys.insert(k.clone());
        }
    }
    Ok(tag_keys)
}

/// Ensure SHOW TAG KEYS/VALUES indexes reflect tags observed on written points.
/// Merges with existing measurement metadata rather than replacing tag keys.
pub async fn backfill_tag_metadata(
    metadata: &Arc<dyn MetadataPort>,
    db: &str,
    measurement: &str,
    tags: impl IntoIterator<Item = (String, String)>,
) -> Result<(), HyperbytedbError> {
    let mut tag_keys: BTreeSet<String> = BTreeSet::new();
    let mut seen: HashSet<(String, String)> = HashSet::new();
    let mut tag_batch: Vec<(String, String)> = Vec::new();
    for (k, v) in tags {
        tag_keys.insert(k.clone());
        if seen.insert((k.clone(), v.clone())) {
            tag_batch.push((k, v));
        }
    }
    if tag_keys.is_empty() {
        return Ok(());
    }

    let mut meas_updates: Vec<MeasurementMeta> = Vec::new();
    if let Some(mut existing) = metadata.get_measurement(db, measurement).await? {
        let before_len = existing.tag_keys.len();
        for k in &tag_keys {
            if !existing.tag_keys.contains(k) {
                existing.tag_keys.push(k.clone());
            }
        }
        existing.tag_keys.sort();
        if existing.tag_keys.len() != before_len {
            meas_updates.push(existing);
        }
    } else {
        meas_updates.push(MeasurementMeta {
            name: measurement.to_string(),
            field_types: HashMap::new(),
            tag_keys: tag_keys.into_iter().collect(),
        });
    }

    let tag_entries = vec![(measurement.to_string(), tag_batch)];
    metadata
        .register_metadata_batch(db, &meas_updates, &tag_entries)
        .await
}

/// Fast-path metadata preparation for columnar batches.
///
/// Works directly from the wire format without expanding to `Vec<Point>`,
/// avoiding O(n) clones entirely.  The schema has a single measurement,
/// single float field, and shared tags.
#[cfg(feature = "columnar-ingest")]
pub async fn prepare_columnar_metadata(
    metadata: &Arc<dyn MetadataPort>,
    db: &str,
    batch: &crate::application::columnar_msgpack::ColumnarMsgpackBatch,
    limits: IngestCardinalityLimits,
    schema_cache: Option<&IngestSchemaCache>,
) -> Result<(), HyperbytedbError> {
    let mut field_types: HashMap<String, u8> = HashMap::with_capacity(1);
    field_types.insert(batch.field.clone(), 0); // Float64

    let tag_keys: BTreeSet<String> = batch.tags.keys().cloned().collect();

    if let Some(sc) = schema_cache
        && sc.is_schema_known(db, &batch.measurement, &field_types, &tag_keys)
    {
        let all_tags_known = batch
            .tags
            .iter()
            .all(|(k, v)| sc.is_tag_known(db, &batch.measurement, k, v));
        if all_tags_known {
            metrics::counter!("hyperbytedb_ingest_schema_cache_hits_total").increment(1);
            return Ok(());
        }

        let novel_hashes: Vec<(u64,)> = batch
            .tags
            .iter()
            .filter_map(|(k, v)| {
                let h = IngestSchemaCache::hash_tag(db, &batch.measurement, k, v);
                if !sc.is_tag_known_by_hash(h) {
                    Some((h,))
                } else {
                    None
                }
            })
            .collect();

        if !novel_hashes.is_empty() {
            let tag_batch: Vec<(String, String)> = batch
                .tags
                .iter()
                .filter(|(k, v)| {
                    let h = IngestSchemaCache::hash_tag(db, &batch.measurement, k, v);
                    novel_hashes.iter().any(|(nh,)| *nh == h)
                })
                .map(|(k, v)| (k.clone(), v.clone()))
                .collect();
            if !tag_batch.is_empty() {
                metadata
                    .register_metadata_batch(db, &[], &[(batch.measurement.clone(), tag_batch)])
                    .await?;
                sc.mark_tags(&novel_hashes);
            }
        }
        metrics::counter!("hyperbytedb_ingest_schema_cache_hits_total").increment(1);
        return Ok(());
    }

    // Slow path: schema unknown
    if limits.max_measurements_per_database > 0 {
        let existing = metadata.list_measurements(db).await?;
        let is_new = !existing.contains(&batch.measurement);
        if is_new && existing.len() + 1 > limits.max_measurements_per_database {
            return Err(HyperbytedbError::CardinalityExceeded {
                measurement: db.to_string(),
                tag_key: "(measurements)".to_string(),
                current: existing.len() + 1,
                limit: limits.max_measurements_per_database,
            });
        }
    }

    if limits.max_tag_values_per_measurement > 0 {
        for (tag_key, tag_value) in &batch.tags {
            let count = metadata
                .count_tag_values(db, tag_key, Some(&batch.measurement))
                .await
                .unwrap_or(0);
            let total = if metadata
                .tag_value_is_known(db, &batch.measurement, tag_key, tag_value)
                .await
                .unwrap_or(false)
            {
                count
            } else {
                count.saturating_add(1)
            };
            if total > limits.max_tag_values_per_measurement {
                return Err(HyperbytedbError::CardinalityExceeded {
                    measurement: batch.measurement.clone(),
                    tag_key: tag_key.clone(),
                    current: total,
                    limit: limits.max_tag_values_per_measurement,
                });
            }
        }
    }

    let field_tuples: Vec<(String, u8)> =
        field_types.iter().map(|(k, v)| (k.clone(), *v)).collect();
    metadata
        .check_field_types(db, &batch.measurement, &field_tuples)
        .await?;

    let metas = vec![MeasurementMeta {
        name: batch.measurement.clone(),
        field_types: field_types.clone(),
        tag_keys: merged_tag_keys(metadata, db, &batch.measurement, &tag_keys)
            .await?
            .into_iter()
            .collect(),
    }];

    let tag_batch: Vec<(String, String)> = batch
        .tags
        .iter()
        .map(|(k, v)| (k.clone(), v.clone()))
        .collect();
    let tags = if tag_batch.is_empty() {
        vec![]
    } else {
        vec![(batch.measurement.clone(), tag_batch)]
    };

    metadata.register_metadata_batch(db, &metas, &tags).await?;

    if let Some(sc) = schema_cache {
        let merged = merged_tag_keys(metadata, db, &batch.measurement, &tag_keys).await?;
        sc.mark_schema(db, &batch.measurement, &field_types, &merged);
        let novel_hashes: Vec<(u64,)> = batch
            .tags
            .iter()
            .map(|(k, v)| (IngestSchemaCache::hash_tag(db, &batch.measurement, k, v),))
            .collect();
        sc.mark_tags(&novel_hashes);
    }

    Ok(())
}

/// Register measurements, validate field types, enforce cardinality, persist tag values (batched).
pub async fn prepare_batch_metadata(
    metadata: &Arc<dyn MetadataPort>,
    db: &str,
    points: &[Point],
    limits: IngestCardinalityLimits,
    schema_cache: Option<&IngestSchemaCache>,
) -> Result<(), HyperbytedbError> {
    let mut measurements: HashMap<String, (HashMap<String, u8>, BTreeSet<String>)> = HashMap::new();

    for point in points {
        let entry = measurements
            .entry(point.measurement.clone())
            .or_insert_with(|| (HashMap::new(), BTreeSet::new()));
        for (k, v) in &point.fields {
            entry.0.insert(k.clone(), v.type_discriminant());
        }
        for k in point.tags.keys() {
            entry.1.insert(k.clone());
        }
    }

    if let Some(sc) = schema_cache {
        let all_schemas_known = measurements
            .iter()
            .all(|(name, (fields, tags))| sc.is_schema_known(db, name, fields, tags));

        if all_schemas_known {
            // Schema is known → field types are unchanged, check_field_types is redundant.
            let all_tags_known = points.iter().all(|p| {
                p.tags
                    .iter()
                    .all(|(k, v)| sc.is_tag_known(db, &p.measurement, k, v))
            });
            if all_tags_known {
                // Full cache hit: zero I/O.
                metrics::counter!("hyperbytedb_ingest_schema_cache_hits_total").increment(1);
                return Ok(());
            }

            // Schema hit but novel tag values — persist before updating the cache.
            // Tag values are only needed for SHOW TAG VALUES queries, not write
            // correctness, but import-style bulk loads query metadata immediately
            // after the write returns.
            let novel_hashes: Vec<(u64,)> = points
                .iter()
                .flat_map(|p| {
                    p.tags.iter().filter_map(move |(k, v)| {
                        let h = IngestSchemaCache::hash_tag(db, &p.measurement, k, v);
                        if !sc.is_tag_known_by_hash(h) {
                            Some((h,))
                        } else {
                            None
                        }
                    })
                })
                .collect();

            // Collect only truly novel tag values for persistence
            let novel_set: HashSet<u64> = novel_hashes.iter().map(|(h,)| *h).collect();
            let mut tag_batch_for_bg: Vec<(String, Vec<(String, String)>)> = Vec::new();
            for meas_name in measurements.keys() {
                let mut seen: HashSet<(String, String)> = HashSet::new();
                let mut batch: Vec<(String, String)> = Vec::new();
                for point in points.iter().filter(|pt| pt.measurement == *meas_name) {
                    for (k, v) in &point.tags {
                        let h = IngestSchemaCache::hash_tag(db, &point.measurement, k, v);
                        if novel_set.contains(&h) && seen.insert((k.clone(), v.clone())) {
                            batch.push((k.clone(), v.clone()));
                        }
                    }
                }
                if !batch.is_empty() {
                    tag_batch_for_bg.push((meas_name.clone(), batch));
                }
            }
            if !tag_batch_for_bg.is_empty() {
                metadata
                    .register_metadata_batch(db, &[], &tag_batch_for_bg)
                    .await?;
                sc.mark_tags(&novel_hashes);
            }
            metrics::counter!("hyperbytedb_ingest_schema_cache_hits_total").increment(1);
            return Ok(());
        }
    }

    // Slow path: schema is unknown (first write for a measurement, or schema changed).
    // Full validation + registration.

    if limits.max_measurements_per_database > 0 {
        let existing = metadata.list_measurements(db).await?;
        let new_count = measurements
            .keys()
            .filter(|m| !existing.contains(m))
            .count();
        if existing.len() + new_count > limits.max_measurements_per_database {
            return Err(HyperbytedbError::CardinalityExceeded {
                measurement: db.to_string(),
                tag_key: "(measurements)".to_string(),
                current: existing.len() + new_count,
                limit: limits.max_measurements_per_database,
            });
        }
    }

    for (meas_name, (field_types, tag_keys)) in &measurements {
        if limits.max_tag_values_per_measurement > 0 {
            for tag_key in tag_keys.iter() {
                let count = metadata
                    .count_tag_values(db, tag_key, Some(meas_name))
                    .await
                    .unwrap_or(0);
                let new_values: std::collections::BTreeSet<&String> = points
                    .iter()
                    .filter(|p| p.measurement == *meas_name)
                    .filter_map(|p| p.tags.get(tag_key))
                    .collect();
                let mut novel: HashSet<&String> = HashSet::new();
                for v in &new_values {
                    if !metadata
                        .tag_value_is_known(db, meas_name, tag_key, v)
                        .await
                        .unwrap_or(false)
                    {
                        novel.insert(v);
                    }
                }
                let total = count + novel.len();
                if total > limits.max_tag_values_per_measurement {
                    return Err(HyperbytedbError::CardinalityExceeded {
                        measurement: meas_name.clone(),
                        tag_key: tag_key.clone(),
                        current: total,
                        limit: limits.max_tag_values_per_measurement,
                    });
                }
            }
        }

        let field_tuples: Vec<(String, u8)> =
            field_types.iter().map(|(k, v)| (k.clone(), *v)).collect();
        metadata
            .check_field_types(db, meas_name, &field_tuples)
            .await?;
    }

    let mut all_metas: Vec<MeasurementMeta> = Vec::with_capacity(measurements.len());
    let mut all_tags: Vec<(String, Vec<(String, String)>)> = Vec::with_capacity(measurements.len());

    for (meas_name, (field_types, _tag_keys)) in &measurements {
        let merged = merged_tag_keys(metadata, db, meas_name, &measurements[meas_name].1).await?;
        all_metas.push(MeasurementMeta {
            name: meas_name.clone(),
            field_types: field_types.clone(),
            tag_keys: merged.into_iter().collect(),
        });

        let mut seen: HashSet<(String, String)> = HashSet::new();
        let mut tag_batch: Vec<(String, String)> = Vec::new();
        for point in points.iter().filter(|p| p.measurement == *meas_name) {
            for (k, v) in &point.tags {
                if seen.insert((k.clone(), v.clone())) {
                    tag_batch.push((k.clone(), v.clone()));
                }
            }
        }
        if !tag_batch.is_empty() {
            all_tags.push((meas_name.clone(), tag_batch));
        }
    }

    metadata
        .register_metadata_batch(db, &all_metas, &all_tags)
        .await?;

    if let Some(sc) = schema_cache {
        for (name, (fields, tags)) in &measurements {
            let merged = merged_tag_keys(metadata, db, name, tags).await?;
            sc.mark_schema(db, name, fields, &merged);
        }
        let novel_hashes: Vec<(u64,)> = points
            .iter()
            .flat_map(|p| {
                p.tags
                    .iter()
                    .map(move |(k, v)| (IngestSchemaCache::hash_tag(db, &p.measurement, k, v),))
            })
            .collect();
        sc.mark_tags(&novel_hashes);
    }

    Ok(())
}
