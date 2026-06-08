use async_trait::async_trait;
use parking_lot::RwLock;
use rocksdb::{BlockBasedOptions, Cache, ColumnFamilyDescriptor, IteratorMode, Options};
use serde::{Deserialize, Serialize};
use std::collections::{BTreeMap, HashMap, HashSet};
use std::path::Path;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

use crate::domain::database::{Database, RetentionPolicy};
use crate::domain::point::FieldValue;
use crate::error::HyperbytedbError;
use crate::ports::metadata::{ContinuousQueryDef, MeasurementMeta, MetadataPort, StoredUser};

const META_CF: &str = "metadata";

fn db_key(name: &str) -> Vec<u8> {
    format!("db:{}", name).into_bytes()
}

fn meas_key(db: &str, name: &str) -> Vec<u8> {
    format!("meas:{}:{}", db, name).into_bytes()
}

fn meas_prefix(db: &str) -> Vec<u8> {
    format!("meas:{}:", db).into_bytes()
}

fn tag_val_prefix(db: &str, meas: Option<&str>) -> Vec<u8> {
    match meas {
        Some(m) => format!("tag_val:{}:{}:", db, m).into_bytes(),
        None => format!("tag_val:{}:", db).into_bytes(),
    }
}

fn tag_val_storage_key(db: &str, measurement: &str, tag_key: &str, tag_value: &str) -> String {
    format!("tag_val:{}:{}:{}:{}", db, measurement, tag_key, tag_value)
}

/// In-memory `(db, measurement, tag_key) → distinct value count`.
fn tag_count_cache_key(db: &str, measurement: &str, tag_key: &str) -> String {
    format!("{db}:{measurement}:{tag_key}")
}

fn parse_tag_val_storage_key(key: &str) -> Option<(String, String, String)> {
    let rest = key.strip_prefix("tag_val:")?;
    let parts: Vec<&str> = rest.splitn(4, ':').collect();
    if parts.len() == 4 {
        Some((
            parts[0].to_string(),
            parts[1].to_string(),
            parts[2].to_string(),
        ))
    } else {
        None
    }
}

fn bump_tag_count_cache(
    counts: &mut HashMap<String, usize>,
    db: &str,
    measurement: &str,
    delta: usize,
    tag_key: &str,
) {
    if delta == 0 {
        return;
    }
    let ck = tag_count_cache_key(db, measurement, tag_key);
    *counts.entry(ck).or_insert(0) += delta;
}

fn bump_tag_counts_from_storage_keys(counts: &mut HashMap<String, usize>, storage_keys: &[String]) {
    let mut per_key: HashMap<String, usize> = HashMap::new();
    for key in storage_keys {
        if let Some((db, meas, tag_key)) = parse_tag_val_storage_key(key) {
            let ck = tag_count_cache_key(&db, &meas, &tag_key);
            *per_key.entry(ck).or_insert(0) += 1;
        }
    }
    for (ck, delta) in per_key {
        *counts.entry(ck).or_insert(0) += delta;
    }
}

/// Storage key for one series dimension row. RP-scoped because the physical
/// `<db>_<rp>_<measurement>_series` table is per-retention-policy. The id is a
/// fixed-width hex suffix so it never contains `:`.
fn series_storage_key(db: &str, rp: &str, measurement: &str, series_id: u64) -> String {
    format!("series:{db}:{rp}:{measurement}:{series_id:016x}")
}

fn series_prefix(db: &str, rp: &str, measurement: &str) -> String {
    format!("series:{db}:{rp}:{measurement}:")
}

/// Parse `series:{db}:{rp}:{measurement}:{id_hex}`. Assumes db/rp/measurement are
/// `:`-free (same assumption as [`parse_tag_val_storage_key`]).
fn parse_series_storage_key(key: &str) -> Option<(String, String, String, u64)> {
    let rest = key.strip_prefix("series:")?;
    let parts: Vec<&str> = rest.splitn(4, ':').collect();
    if parts.len() == 4 {
        let id = u64::from_str_radix(parts[3], 16).ok()?;
        Some((
            parts[0].to_string(),
            parts[1].to_string(),
            parts[2].to_string(),
            id,
        ))
    } else {
        None
    }
}

fn user_key(username: &str) -> Vec<u8> {
    format!("user:{}", username).into_bytes()
}

fn user_prefix() -> Vec<u8> {
    b"user:".to_vec()
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct DbValue {
    database: Database,
}

pub struct RocksDbMetadata {
    db: Arc<rocksdb::DB>,
    /// Monotonic counter bumped on **structural** metadata changes only
    /// (measurement create/delete, db create/drop). Used by
    /// `meas_list_cache` to detect a stale snapshot.
    generation: AtomicU64,
    db_cache: RwLock<HashMap<String, Database>>,
    meas_cache: RwLock<HashMap<String, MeasurementMeta>>,
    tag_known: RwLock<HashSet<String>>,
    /// `(db, measurement, tag_key)` → distinct tag value count.
    tag_count_cache: RwLock<HashMap<String, usize>>,
    /// Storage-key strings of series already persisted, for O(1) dedup on the
    /// flush path. Mirrors `tag_known`. Warmed by `warm_series`.
    series_known: RwLock<HashSet<String>>,
    /// User record cache: `username → StoredUser`.
    user_cache: RwLock<HashMap<String, StoredUser>>,
    /// Generation-gated measurement list: `db → (generation, names)`.
    meas_list_cache: RwLock<HashMap<String, (u64, Vec<String>)>>,
    /// Tombstone cache: `"{db}:{meas}" → [(id, predicate)]`.
    tombstone_cache: RwLock<HashMap<String, Vec<(String, String)>>>,
    /// CQ list cache: all continuous queries, invalidated on CQ DDL.
    cq_cache: RwLock<Option<Vec<ContinuousQueryDef>>>,
}

impl RocksDbMetadata {
    pub fn open<P: AsRef<Path>>(path: P) -> Result<Self, HyperbytedbError> {
        // The metadata DB stores measurement schemas, catalog entries,
        // users, CQs, and tombstones — typically a few MiB up
        // to ~100 MiB at scale, with cold reads dominated by the in-process
        // `meas_cache` layer. The memtable footprint is kept
        // small (2 × 8 MiB) because writes are dominated by the in-process
        // caches, but we still enable the full set of RocksDB
        // high-concurrency knobs (concurrent memtable writes + pipelined
        // write + adaptive yield + bumped background-job budget) so that
        // bursty writes (schema migrations, retention catalog updates) don't
        // serialise on the write thread and stall every concurrent ingest call
        // sharing the process.
        let cache = Cache::new_lru_cache(16 * 1024 * 1024);
        let mut block_opts = BlockBasedOptions::default();
        block_opts.set_block_cache(&cache);
        block_opts.set_bloom_filter(10.0, false);
        block_opts.set_block_size(16 * 1024);

        let parallelism = std::thread::available_parallelism()
            .map(|n| n.get() as i32)
            .unwrap_or(4)
            .max(2);

        let mut opts = Options::default();
        opts.create_if_missing(true);
        opts.create_missing_column_families(true);
        opts.set_compression_type(rocksdb::DBCompressionType::Lz4);
        opts.set_block_based_table_factory(&block_opts);
        // 2 × 8 MiB = 16 MiB worst-case memtable footprint. Even at the
        // peak metadata-write rate (retention catalog updates, DDL mutations)
        // a few hundred parquet entries per cycle), 8 MiB holds many cycles
        // worth of writes before a flush is needed.
        opts.set_max_write_buffer_number(2);
        opts.set_write_buffer_size(8 * 1024 * 1024);
        // Concurrent memtable writers + pipelined write + adaptive yield.
        // Even though the metadata workload is much lower volume than the
        // WAL, every ingest request that misses the schema cache has to
        // serialise through this DB. Without these knobs a schema-cache
        // miss storm (e.g. a fleet rolling a new measurement) becomes a
        // process-wide write-throughput cliff.
        opts.set_enable_pipelined_write(true);
        opts.set_allow_concurrent_memtable_write(true);
        opts.set_enable_write_thread_adaptive_yield(true);
        // Give the metadata DB enough background-job budget to flush
        // and compact in parallel with ingest writes. Capped at
        // `parallelism / 2` because metadata is tiny and we don't want
        // it to compete with the WAL DB for cores.
        opts.increase_parallelism((parallelism / 2).max(2));
        opts.set_max_background_jobs((parallelism / 2).max(2));
        opts.set_bytes_per_sync(1024 * 1024);
        opts.set_wal_bytes_per_sync(1024 * 1024);
        // Metadata is tiny; 64 open SSTs is overkill but cheap enough to
        // bound the FD count predictably for ops.
        opts.set_max_open_files(64);

        let mut cf_opts = Options::default();
        cf_opts.set_compression_type(rocksdb::DBCompressionType::Lz4);
        cf_opts.set_block_based_table_factory(&block_opts);
        cf_opts.set_max_write_buffer_number(2);
        cf_opts.set_write_buffer_size(8 * 1024 * 1024);
        let cfs = vec![ColumnFamilyDescriptor::new(META_CF, cf_opts)];

        let db = Arc::new(
            rocksdb::DB::open_cf_descriptors(&opts, path, cfs)
                .map_err(|e| HyperbytedbError::Metadata(e.to_string()))?,
        );

        Ok(Self {
            db,
            generation: AtomicU64::new(0),
            db_cache: RwLock::new(HashMap::new()),
            meas_cache: RwLock::new(HashMap::new()),
            tag_known: RwLock::new(HashSet::new()),
            tag_count_cache: RwLock::new(HashMap::new()),
            series_known: RwLock::new(HashSet::new()),
            user_cache: RwLock::new(HashMap::new()),
            meas_list_cache: RwLock::new(HashMap::new()),
            tombstone_cache: RwLock::new(HashMap::new()),
            cq_cache: RwLock::new(None),
        })
    }
}

#[async_trait]
impl MetadataPort for RocksDbMetadata {
    async fn create_database(&self, name: &str) -> Result<(), HyperbytedbError> {
        let cf = self.db.cf_handle(META_CF).ok_or_else(|| {
            HyperbytedbError::Metadata("metadata column family not found".to_string())
        })?;
        let db = Database::new(name);
        let key = db_key(name);
        let value = serde_json::to_vec(&DbValue {
            database: db.clone(),
        })
        .map_err(|e| HyperbytedbError::Metadata(e.to_string()))?;
        self.db
            .put_cf(&cf, key, value)
            .map_err(|e| HyperbytedbError::Metadata(e.to_string()))?;
        self.db_cache.write().insert(name.to_string(), db);
        Ok(())
    }

    async fn drop_database(&self, name: &str) -> Result<(), HyperbytedbError> {
        let cf = self.db.cf_handle(META_CF).ok_or_else(|| {
            HyperbytedbError::Metadata("metadata column family not found".to_string())
        })?;
        let db_prefix = format!("db:{}", name);
        let meas_prefix = format!("meas:{}:", name);
        let tag_prefix = format!("tag_val:{}:", name);
        let series_db_prefix = format!("series:{}:", name);

        let iter =
            self.db
                .iterator_cf_opt(&cf, rocksdb::ReadOptions::default(), IteratorMode::Start);
        let mut to_delete = Vec::new();
        for item in iter {
            if let Ok((key, _)) = item
                && let Ok(k) = std::str::from_utf8(&key)
                && (k.starts_with(&db_prefix)
                    || k.starts_with(&meas_prefix)
                    || k.starts_with(&tag_prefix)
                    || k.starts_with(&series_db_prefix))
            {
                to_delete.push(key.to_vec());
            }
        }
        for k in to_delete {
            self.db
                .delete_cf(&cf, k)
                .map_err(|e| HyperbytedbError::Metadata(e.to_string()))?;
        }
        self.db_cache.write().remove(name);
        {
            let prefix = format!("{}:", name);
            self.meas_cache
                .write()
                .retain(|k, _| !k.starts_with(&prefix));
            self.tag_known.write().retain(|k| !k.starts_with(&prefix));
            let count_prefix = format!("{name}:");
            self.tag_count_cache
                .write()
                .retain(|k, _| !k.starts_with(&count_prefix));
            self.series_known
                .write()
                .retain(|k| !k.starts_with(&series_db_prefix));
        }
        self.meas_list_cache.write().remove(name);
        // Structural change — invalidate the `meas_list_cache`
        // snapshot for this database.
        self.generation.fetch_add(1, Ordering::Relaxed);
        Ok(())
    }

    async fn list_databases(&self) -> Result<Vec<Database>, HyperbytedbError> {
        let cf = self.db.cf_handle(META_CF).ok_or_else(|| {
            HyperbytedbError::Metadata("metadata column family not found".to_string())
        })?;
        let mut dbs = Vec::new();
        let prefix = b"db:";
        let iter = self.db.iterator_cf_opt(
            &cf,
            rocksdb::ReadOptions::default(),
            IteratorMode::From(prefix, rocksdb::Direction::Forward),
        );

        for item in iter {
            let (key, value) = item.map_err(|e| HyperbytedbError::Metadata(e.to_string()))?;
            if !key.starts_with(prefix) {
                break;
            }
            let v: DbValue = serde_json::from_slice(&value)
                .map_err(|e| HyperbytedbError::Metadata(e.to_string()))?;
            dbs.push(v.database);
        }
        Ok(dbs)
    }

    async fn get_database(&self, name: &str) -> Result<Option<Database>, HyperbytedbError> {
        {
            let cache = self.db_cache.read();
            if let Some(db) = cache.get(name) {
                return Ok(Some(db.clone()));
            }
        }
        let db = self.db.clone();
        let key = db_key(name);
        let name_owned = name.to_string();
        let result = tokio::task::spawn_blocking(move || {
            let cf = db.cf_handle(META_CF).ok_or_else(|| {
                HyperbytedbError::Metadata("metadata column family not found".to_string())
            })?;
            match db.get_cf(&cf, key) {
                Ok(Some(v)) => {
                    let dv: DbValue = serde_json::from_slice(&v)
                        .map_err(|e| HyperbytedbError::Metadata(e.to_string()))?;
                    Ok(Some(dv.database))
                }
                Ok(None) => Ok(None),
                Err(e) => Err(HyperbytedbError::Metadata(e.to_string())),
            }
        })
        .await
        .map_err(|e| HyperbytedbError::Metadata(format!("metadata task panicked: {e}")))?;
        if let Ok(Some(ref database)) = result {
            self.db_cache.write().insert(name_owned, database.clone());
        }
        result
    }

    async fn create_retention_policy(
        &self,
        db: &str,
        rp: RetentionPolicy,
    ) -> Result<(), HyperbytedbError> {
        let mut db_opt = self
            .get_database(db)
            .await?
            .ok_or_else(|| HyperbytedbError::DatabaseNotFound(db.to_string()))?;

        let exists = db_opt.retention_policies.iter().any(|x| x.name == rp.name);
        if !exists {
            db_opt.retention_policies.push(rp.clone());
            if rp.is_default {
                db_opt.default_rp = rp.name.clone();
                for r in &mut db_opt.retention_policies {
                    if r.name != rp.name {
                        r.is_default = false;
                    }
                }
            }
        }

        let cf = self.db.cf_handle(META_CF).ok_or_else(|| {
            HyperbytedbError::Metadata("metadata column family not found".to_string())
        })?;
        let key = db_key(db);
        let value = serde_json::to_vec(&DbValue {
            database: db_opt.clone(),
        })
        .map_err(|e| HyperbytedbError::Metadata(e.to_string()))?;
        self.db
            .put_cf(&cf, key, value)
            .map_err(|e| HyperbytedbError::Metadata(e.to_string()))?;
        self.db_cache.write().insert(db.to_string(), db_opt);
        Ok(())
    }

    async fn drop_retention_policy(&self, db: &str, name: &str) -> Result<(), HyperbytedbError> {
        let mut db_obj = self
            .get_database(db)
            .await?
            .ok_or_else(|| HyperbytedbError::DatabaseNotFound(db.to_string()))?;

        db_obj.retention_policies.retain(|rp| rp.name != name);

        if db_obj.default_rp == name {
            db_obj.default_rp = db_obj
                .retention_policies
                .first()
                .map(|rp| rp.name.clone())
                .unwrap_or_else(|| "autogen".to_string());
        }

        let cf = self.db.cf_handle(META_CF).ok_or_else(|| {
            HyperbytedbError::Metadata("metadata column family not found".to_string())
        })?;
        let key = db_key(db);
        let value = serde_json::to_vec(&DbValue {
            database: db_obj.clone(),
        })
        .map_err(|e| HyperbytedbError::Metadata(e.to_string()))?;
        self.db
            .put_cf(&cf, key, value)
            .map_err(|e| HyperbytedbError::Metadata(e.to_string()))?;
        self.db_cache.write().insert(db.to_string(), db_obj);
        Ok(())
    }

    async fn get_default_rp(&self, db: &str) -> Result<String, HyperbytedbError> {
        let d = self
            .get_database(db)
            .await?
            .ok_or_else(|| HyperbytedbError::DatabaseNotFound(db.to_string()))?;
        Ok(d.default_rp)
    }

    async fn register_measurement(
        &self,
        db: &str,
        measurement: &MeasurementMeta,
    ) -> Result<(), HyperbytedbError> {
        let cache_key = format!("{}:{}", db, measurement.name);
        {
            let cache = self.meas_cache.read();
            if let Some(existing) = cache.get(&cache_key)
                && existing.field_types == measurement.field_types
                && existing.tag_keys == measurement.tag_keys
            {
                return Ok(());
            }
        }
        let rdb = self.db.clone();
        let key = meas_key(db, &measurement.name);
        let value = serde_json::to_vec(measurement)
            .map_err(|e| HyperbytedbError::Metadata(e.to_string()))?;
        tokio::task::spawn_blocking(move || {
            let cf = rdb.cf_handle(META_CF).ok_or_else(|| {
                HyperbytedbError::Metadata("metadata column family not found".to_string())
            })?;
            rdb.put_cf(&cf, key, value)
                .map_err(|e| HyperbytedbError::Metadata(e.to_string()))?;
            Ok::<(), HyperbytedbError>(())
        })
        .await
        .map_err(|e| HyperbytedbError::Metadata(format!("metadata task panicked: {e}")))??;
        self.meas_cache
            .write()
            .insert(cache_key, measurement.clone());
        self.meas_list_cache.write().remove(db);
        // Structural change. Bump for the same reason as the explicit
        // `meas_list_cache.remove(db)` above.
        self.generation.fetch_add(1, Ordering::Relaxed);
        Ok(())
    }

    async fn get_measurement(
        &self,
        db: &str,
        name: &str,
    ) -> Result<Option<MeasurementMeta>, HyperbytedbError> {
        let cache_key = format!("{}:{}", db, name);
        {
            let cache = self.meas_cache.read();
            if let Some(m) = cache.get(&cache_key) {
                return Ok(Some(m.clone()));
            }
        }
        let rdb = self.db.clone();
        let key = meas_key(db, name);
        let result = tokio::task::spawn_blocking(move || {
            let cf = rdb.cf_handle(META_CF).ok_or_else(|| {
                HyperbytedbError::Metadata("metadata column family not found".to_string())
            })?;
            match rdb.get_cf(&cf, key) {
                Ok(Some(v)) => {
                    let m: MeasurementMeta = serde_json::from_slice(&v)
                        .map_err(|e| HyperbytedbError::Metadata(e.to_string()))?;
                    Ok(Some(m))
                }
                Ok(None) => Ok(None),
                Err(e) => Err(HyperbytedbError::Metadata(e.to_string())),
            }
        })
        .await
        .map_err(|e| HyperbytedbError::Metadata(format!("metadata task panicked: {e}")))?;
        if let Ok(Some(ref m)) = result {
            self.meas_cache.write().insert(cache_key, m.clone());
        }
        result
    }

    async fn list_measurements(&self, db: &str) -> Result<Vec<String>, HyperbytedbError> {
        let cur_gen = self.generation.load(Ordering::Relaxed);
        {
            let cache = self.meas_list_cache.read();
            if let Some((cached_gen, names)) = cache.get(db)
                && *cached_gen == cur_gen
            {
                return Ok(names.clone());
            }
        }

        let cf = self.db.cf_handle(META_CF).ok_or_else(|| {
            HyperbytedbError::Metadata("metadata column family not found".to_string())
        })?;
        let mut names = Vec::new();
        let prefix = meas_prefix(db);
        let iter = self.db.iterator_cf_opt(
            &cf,
            rocksdb::ReadOptions::default(),
            IteratorMode::From(prefix.as_slice(), rocksdb::Direction::Forward),
        );

        for item in iter {
            let (key, _) = item.map_err(|e| HyperbytedbError::Metadata(e.to_string()))?;
            if !key.starts_with(&prefix) {
                break;
            }
            let rest = &key[prefix.len()..];
            if let Ok(s) = std::str::from_utf8(rest) {
                let name = s.split(':').next().unwrap_or(s);
                names.push(name.to_string());
            }
        }
        names.sort();
        names.dedup();

        self.meas_list_cache
            .write()
            .insert(db.to_string(), (cur_gen, names.clone()));
        Ok(names)
    }

    async fn check_field_types(
        &self,
        db: &str,
        measurement: &str,
        fields: &[(String, u8)],
    ) -> Result<(), HyperbytedbError> {
        let meta = match self.get_measurement(db, measurement).await? {
            Some(m) => m,
            None => return Ok(()), // First write for this measurement, no conflicts possible
        };

        for (name, got) in fields {
            if let Some(&expected) = meta.field_types.get(name)
                && *got != expected
            {
                return Err(HyperbytedbError::FieldTypeConflict {
                    field: name.clone(),
                    measurement: measurement.to_string(),
                    got: FieldValue::type_name_from_discriminant(*got).to_string(),
                    expected: FieldValue::type_name_from_discriminant(expected).to_string(),
                });
            }
        }
        Ok(())
    }

    async fn list_tag_keys(
        &self,
        db: &str,
        measurement: Option<&str>,
    ) -> Result<Vec<String>, HyperbytedbError> {
        let mut keys: std::collections::HashSet<String> = std::collections::HashSet::new();

        match measurement {
            Some(m) => {
                if let Some(meta) = self.get_measurement(db, m).await? {
                    for k in &meta.tag_keys {
                        keys.insert(k.clone());
                    }
                }
            }
            None => {
                let measurements = self.list_measurements(db).await?;
                for m in &measurements {
                    if let Some(meta) = self.get_measurement(db, m).await? {
                        for k in &meta.tag_keys {
                            keys.insert(k.clone());
                        }
                    }
                }
            }
        }

        let mut result: Vec<_> = keys.into_iter().collect();
        result.sort();
        Ok(result)
    }

    async fn list_tag_values(
        &self,
        db: &str,
        tag_key: &str,
        measurement: Option<&str>,
    ) -> Result<Vec<String>, HyperbytedbError> {
        let cf = self.db.cf_handle(META_CF).ok_or_else(|| {
            HyperbytedbError::Metadata("metadata column family not found".to_string())
        })?;
        let mut values: std::collections::HashSet<String> = std::collections::HashSet::new();
        let prefix = tag_val_prefix(db, measurement);
        let iter = self.db.iterator_cf_opt(
            &cf,
            rocksdb::ReadOptions::default(),
            IteratorMode::From(prefix.as_slice(), rocksdb::Direction::Forward),
        );

        for item in iter {
            let (key, _) = item.map_err(|e| HyperbytedbError::Metadata(e.to_string()))?;
            if !key.starts_with(&prefix) {
                break;
            }
            let rest = &key[prefix.len()..];
            if let Ok(s) = std::str::from_utf8(rest) {
                // When measurement is Some: rest = "tag_key:tag_value"
                // When measurement is None: rest = "measurement:tag_key:tag_value"
                let (k, v) = if measurement.is_some() {
                    let parts: Vec<&str> = s.splitn(2, ':').collect();
                    if parts.len() == 2 {
                        (parts[0], parts[1])
                    } else {
                        continue;
                    }
                } else {
                    let parts: Vec<&str> = s.splitn(3, ':').collect();
                    if parts.len() == 3 {
                        (parts[1], parts[2])
                    } else {
                        continue;
                    }
                };
                if k == tag_key {
                    values.insert(v.to_string());
                }
            }
        }
        let mut result: Vec<_> = values.into_iter().collect();
        result.sort();
        Ok(result)
    }

    async fn count_tag_values(
        &self,
        db: &str,
        tag_key: &str,
        measurement: Option<&str>,
    ) -> Result<usize, HyperbytedbError> {
        if let Some(meas) = measurement {
            let ck = tag_count_cache_key(db, meas, tag_key);
            if let Some(&count) = self.tag_count_cache.read().get(&ck) {
                return Ok(count);
            }
            let count = self.list_tag_values(db, tag_key, Some(meas)).await?.len();
            self.tag_count_cache.write().insert(ck, count);
            return Ok(count);
        }

        let suffix = format!(":{tag_key}");
        let prefix = format!("{db}:");
        let cached: usize = self
            .tag_count_cache
            .read()
            .iter()
            .filter(|(k, _)| k.starts_with(&prefix) && k.ends_with(&suffix))
            .map(|(_, v)| *v)
            .sum();
        if cached > 0 {
            return Ok(cached);
        }
        Ok(self.list_tag_values(db, tag_key, None).await?.len())
    }

    async fn tag_value_is_known(
        &self,
        db: &str,
        measurement: &str,
        tag_key: &str,
        tag_value: &str,
    ) -> Result<bool, HyperbytedbError> {
        let k = tag_val_storage_key(db, measurement, tag_key, tag_value);
        Ok(self.tag_known.read().contains(&k))
    }

    async fn warm_tag_value_counts(&self) -> Result<usize, HyperbytedbError> {
        let rdb = self.db.clone();
        let counts = tokio::task::spawn_blocking(move || {
            let cf = rdb.cf_handle(META_CF).ok_or_else(|| {
                HyperbytedbError::Metadata("metadata column family not found".to_string())
            })?;
            let prefix = b"tag_val:";
            let iter = rdb.iterator_cf_opt(
                &cf,
                rocksdb::ReadOptions::default(),
                IteratorMode::From(prefix, rocksdb::Direction::Forward),
            );
            let mut counts: HashMap<String, usize> = HashMap::new();
            for item in iter {
                let (key, _) = item.map_err(|e| HyperbytedbError::Metadata(e.to_string()))?;
                if !key.starts_with(prefix) {
                    break;
                }
                let Ok(s) = std::str::from_utf8(&key) else {
                    continue;
                };
                if let Some((db, meas, tag_key)) = parse_tag_val_storage_key(s) {
                    bump_tag_count_cache(&mut counts, &db, &meas, 1, &tag_key);
                }
            }
            Ok::<HashMap<String, usize>, HyperbytedbError>(counts)
        })
        .await
        .map_err(|e| HyperbytedbError::Metadata(format!("metadata task panicked: {e}")))??;

        let warmed = counts.len();
        *self.tag_count_cache.write() = counts;
        Ok(warmed)
    }

    async fn register_series_batch(
        &self,
        db: &str,
        rp: &str,
        measurement: &str,
        series: &[(u64, BTreeMap<String, String>)],
    ) -> Result<(), HyperbytedbError> {
        if series.is_empty() {
            return Ok(());
        }
        // Novelty filter against the in-memory set; serialise tags only for new.
        let novel: Vec<(String, Vec<u8>)> = {
            let cache = self.series_known.read();
            let mut out = Vec::with_capacity(series.len());
            for (id, tags) in series {
                let k = series_storage_key(db, rp, measurement, *id);
                if cache.contains(&k) {
                    continue;
                }
                let value = serde_json::to_vec(tags)
                    .map_err(|e| HyperbytedbError::Metadata(e.to_string()))?;
                out.push((k, value));
            }
            out
        };
        if novel.is_empty() {
            return Ok(());
        }
        let rdb = self.db.clone();
        let entries = novel.clone();
        tokio::task::spawn_blocking(move || {
            let cf = rdb.cf_handle(META_CF).ok_or_else(|| {
                HyperbytedbError::Metadata("metadata column family not found".to_string())
            })?;
            let mut batch = rocksdb::WriteBatch::default();
            for (key, value) in &entries {
                batch.put_cf(&cf, key.as_bytes(), value);
            }
            rdb.write(batch)
                .map_err(|e| HyperbytedbError::Metadata(e.to_string()))?;
            Ok::<(), HyperbytedbError>(())
        })
        .await
        .map_err(|e| HyperbytedbError::Metadata(format!("metadata task panicked: {e}")))??;
        {
            let mut cache = self.series_known.write();
            for (k, _) in &novel {
                cache.insert(k.clone());
            }
        }
        Ok(())
    }

    async fn list_series(
        &self,
        db: &str,
        rp: &str,
        measurement: &str,
    ) -> Result<Vec<(u64, BTreeMap<String, String>)>, HyperbytedbError> {
        let rdb = self.db.clone();
        let prefix = series_prefix(db, rp, measurement);
        tokio::task::spawn_blocking(move || {
            let cf = rdb.cf_handle(META_CF).ok_or_else(|| {
                HyperbytedbError::Metadata("metadata column family not found".to_string())
            })?;
            let pbytes = prefix.as_bytes();
            let iter = rdb.iterator_cf_opt(
                &cf,
                rocksdb::ReadOptions::default(),
                IteratorMode::From(pbytes, rocksdb::Direction::Forward),
            );
            let mut out = Vec::new();
            for item in iter {
                let (key, value) = item.map_err(|e| HyperbytedbError::Metadata(e.to_string()))?;
                if !key.starts_with(pbytes) {
                    break;
                }
                let Ok(s) = std::str::from_utf8(&key) else {
                    continue;
                };
                let Ok(id) = u64::from_str_radix(&s[prefix.len()..], 16) else {
                    continue;
                };
                let tags: BTreeMap<String, String> = serde_json::from_slice(&value)
                    .map_err(|e| HyperbytedbError::Metadata(e.to_string()))?;
                out.push((id, tags));
            }
            Ok::<Vec<(u64, BTreeMap<String, String>)>, HyperbytedbError>(out)
        })
        .await
        .map_err(|e| HyperbytedbError::Metadata(format!("metadata task panicked: {e}")))?
    }

    async fn warm_series(&self) -> Result<usize, HyperbytedbError> {
        let rdb = self.db.clone();
        let keys = tokio::task::spawn_blocking(move || {
            let cf = rdb.cf_handle(META_CF).ok_or_else(|| {
                HyperbytedbError::Metadata("metadata column family not found".to_string())
            })?;
            let prefix = b"series:";
            let iter = rdb.iterator_cf_opt(
                &cf,
                rocksdb::ReadOptions::default(),
                IteratorMode::From(prefix, rocksdb::Direction::Forward),
            );
            let mut keys: HashSet<String> = HashSet::new();
            for item in iter {
                let (key, _) = item.map_err(|e| HyperbytedbError::Metadata(e.to_string()))?;
                if !key.starts_with(prefix) {
                    break;
                }
                if let Ok(s) = std::str::from_utf8(&key) {
                    keys.insert(s.to_string());
                }
            }
            Ok::<HashSet<String>, HyperbytedbError>(keys)
        })
        .await
        .map_err(|e| HyperbytedbError::Metadata(format!("metadata task panicked: {e}")))??;

        let warmed = keys.len();
        *self.series_known.write() = keys;
        Ok(warmed)
    }

    async fn store_tag_value(
        &self,
        db: &str,
        measurement: &str,
        tag_key: &str,
        tag_value: &str,
    ) -> Result<(), HyperbytedbError> {
        self.store_tag_values_batch(
            db,
            measurement,
            &[(tag_key.to_string(), tag_value.to_string())],
        )
        .await
    }

    async fn store_tag_values_batch(
        &self,
        db: &str,
        measurement: &str,
        entries: &[(String, String)],
    ) -> Result<(), HyperbytedbError> {
        if entries.is_empty() {
            return Ok(());
        }
        let novel: Vec<String> = {
            let cache = self.tag_known.read();
            entries
                .iter()
                .filter_map(|(tag_key, tag_value)| {
                    let k = tag_val_storage_key(db, measurement, tag_key, tag_value);
                    if cache.contains(&k) { None } else { Some(k) }
                })
                .collect()
        };
        if novel.is_empty() {
            return Ok(());
        }
        let rdb = self.db.clone();
        let keys = novel.clone();
        tokio::task::spawn_blocking(move || {
            let cf = rdb.cf_handle(META_CF).ok_or_else(|| {
                HyperbytedbError::Metadata("metadata column family not found".to_string())
            })?;
            let mut batch = rocksdb::WriteBatch::default();
            for key in &keys {
                batch.put_cf(&cf, key.as_bytes(), b"1");
            }
            rdb.write(batch)
                .map_err(|e| HyperbytedbError::Metadata(e.to_string()))?;
            Ok::<(), HyperbytedbError>(())
        })
        .await
        .map_err(|e| HyperbytedbError::Metadata(format!("metadata task panicked: {e}")))??;
        {
            let mut cache = self.tag_known.write();
            for k in &novel {
                cache.insert(k.clone());
            }
        }
        {
            let mut counts = self.tag_count_cache.write();
            bump_tag_counts_from_storage_keys(&mut counts, &novel);
        }
        Ok(())
    }

    async fn register_metadata_batch(
        &self,
        db: &str,
        measurements: &[MeasurementMeta],
        tag_entries: &[(String, Vec<(String, String)>)],
    ) -> Result<(), HyperbytedbError> {
        let mut meas_updates: Vec<(String, Vec<u8>, MeasurementMeta)> = Vec::new();
        let mut novel_tags: Vec<String> = Vec::new();

        // Phase 1: check caches, collect novel data (no I/O)
        {
            let meas_cache = self.meas_cache.read();
            for m in measurements {
                let cache_key = format!("{}:{}", db, m.name);
                let needs_write = !matches!(
                    meas_cache.get(&cache_key),
                    Some(existing)
                        if existing.field_types == m.field_types
                            && existing.tag_keys == m.tag_keys
                );
                if needs_write {
                    let value = serde_json::to_vec(m)
                        .map_err(|e| HyperbytedbError::Metadata(e.to_string()))?;
                    meas_updates.push((cache_key, value, m.clone()));
                }
            }
        }
        {
            let tag_cache = self.tag_known.read();
            for (meas_name, tags) in tag_entries {
                for (tag_key, tag_value) in tags {
                    let k = tag_val_storage_key(db, meas_name, tag_key, tag_value);
                    if !tag_cache.contains(&k) {
                        novel_tags.push(k);
                    }
                }
            }
        }

        if meas_updates.is_empty() && novel_tags.is_empty() {
            return Ok(());
        }

        // Phase 2: single RocksDB WriteBatch for all novel data
        let rdb = self.db.clone();
        let meas_keys: Vec<(Vec<u8>, Vec<u8>)> = meas_updates
            .iter()
            .map(|(_, value, m)| (meas_key(db, &m.name), value.clone()))
            .collect();
        let tag_keys = novel_tags.clone();

        tokio::task::spawn_blocking(move || {
            let cf = rdb.cf_handle(META_CF).ok_or_else(|| {
                HyperbytedbError::Metadata("metadata column family not found".to_string())
            })?;
            let mut batch = rocksdb::WriteBatch::default();
            for (key, value) in &meas_keys {
                batch.put_cf(&cf, key, value);
            }
            for key in &tag_keys {
                batch.put_cf(&cf, key.as_bytes(), b"1");
            }
            rdb.write(batch)
                .map_err(|e| HyperbytedbError::Metadata(e.to_string()))?;
            Ok::<(), HyperbytedbError>(())
        })
        .await
        .map_err(|e| HyperbytedbError::Metadata(format!("metadata task panicked: {e}")))??;

        // Phase 3: update caches
        if !meas_updates.is_empty() {
            {
                let mut cache = self.meas_cache.write();
                for (cache_key, _, m) in &meas_updates {
                    cache.insert(cache_key.clone(), m.clone());
                }
            }
            self.meas_list_cache.write().remove(db);
        }
        if !novel_tags.is_empty() {
            let mut cache = self.tag_known.write();
            for k in &novel_tags {
                cache.insert(k.clone());
            }
            let mut counts = self.tag_count_cache.write();
            bump_tag_counts_from_storage_keys(&mut counts, &novel_tags);
        }

        Ok(())
    }

    async fn list_retention_policies(
        &self,
        db: &str,
    ) -> Result<Vec<RetentionPolicy>, HyperbytedbError> {
        let database = self.get_database(db).await?;
        match database {
            Some(d) => Ok(d.retention_policies),
            None => Ok(vec![]),
        }
    }

    async fn create_user(
        &self,
        username: &str,
        password_hash: &str,
        admin: bool,
    ) -> Result<(), HyperbytedbError> {
        let cf = self.db.cf_handle(META_CF).ok_or_else(|| {
            HyperbytedbError::Metadata("metadata column family not found".to_string())
        })?;
        let key = user_key(username);
        let user = StoredUser {
            password_hash: password_hash.to_string(),
            admin,
            created_at: chrono::Utc::now().to_rfc3339(),
            privileges: Default::default(),
        };
        let value =
            serde_json::to_vec(&user).map_err(|e| HyperbytedbError::Metadata(e.to_string()))?;
        self.db
            .put_cf(&cf, key, value)
            .map_err(|e| HyperbytedbError::Metadata(e.to_string()))?;
        self.user_cache.write().insert(username.to_string(), user);
        Ok(())
    }

    async fn drop_user(&self, username: &str) -> Result<(), HyperbytedbError> {
        let cf = self.db.cf_handle(META_CF).ok_or_else(|| {
            HyperbytedbError::Metadata("metadata column family not found".to_string())
        })?;
        let key = user_key(username);
        self.db
            .delete_cf(&cf, key)
            .map_err(|e| HyperbytedbError::Metadata(e.to_string()))?;
        self.user_cache.write().remove(username);
        Ok(())
    }

    async fn get_user(&self, username: &str) -> Result<Option<StoredUser>, HyperbytedbError> {
        {
            let cache = self.user_cache.read();
            if let Some(user) = cache.get(username) {
                return Ok(Some(user.clone()));
            }
        }
        let cf = self.db.cf_handle(META_CF).ok_or_else(|| {
            HyperbytedbError::Metadata("metadata column family not found".to_string())
        })?;
        let key = user_key(username);
        match self.db.get_cf(&cf, key) {
            Ok(Some(v)) => {
                let user: StoredUser = serde_json::from_slice(&v)
                    .map_err(|e| HyperbytedbError::Metadata(e.to_string()))?;
                self.user_cache
                    .write()
                    .insert(username.to_string(), user.clone());
                Ok(Some(user))
            }
            Ok(None) => Ok(None),
            Err(e) => Err(HyperbytedbError::Metadata(e.to_string())),
        }
    }

    async fn list_users(&self) -> Result<Vec<String>, HyperbytedbError> {
        let cf = self.db.cf_handle(META_CF).ok_or_else(|| {
            HyperbytedbError::Metadata("metadata column family not found".to_string())
        })?;
        let prefix = user_prefix();
        let iter = self.db.iterator_cf_opt(
            &cf,
            rocksdb::ReadOptions::default(),
            IteratorMode::From(prefix.as_slice(), rocksdb::Direction::Forward),
        );
        let mut names = Vec::new();
        for item in iter {
            let (key, _) = item.map_err(|e| HyperbytedbError::Metadata(e.to_string()))?;
            if !key.starts_with(&prefix) {
                break;
            }
            if let Ok(s) = std::str::from_utf8(&key[prefix.len()..]) {
                names.push(s.to_string());
            }
        }
        Ok(names)
    }

    async fn grant_privilege(
        &self,
        username: &str,
        database: &str,
        privilege: crate::domain::user::DatabasePrivilege,
    ) -> Result<(), HyperbytedbError> {
        let mut user = self
            .get_user(username)
            .await?
            .ok_or_else(|| HyperbytedbError::Internal(format!("user not found: {username}")))?;
        user.privileges.insert(database.to_string(), privilege);
        let cf = self.db.cf_handle(META_CF).ok_or_else(|| {
            HyperbytedbError::Metadata("metadata column family not found".to_string())
        })?;
        let key = user_key(username);
        let value =
            serde_json::to_vec(&user).map_err(|e| HyperbytedbError::Metadata(e.to_string()))?;
        self.db
            .put_cf(&cf, key, value)
            .map_err(|e| HyperbytedbError::Metadata(e.to_string()))?;
        self.user_cache.write().insert(username.to_string(), user);
        Ok(())
    }

    async fn revoke_privilege(
        &self,
        username: &str,
        database: &str,
    ) -> Result<(), HyperbytedbError> {
        let mut user = self
            .get_user(username)
            .await?
            .ok_or_else(|| HyperbytedbError::Internal(format!("user not found: {username}")))?;
        user.privileges.remove(database);
        let cf = self.db.cf_handle(META_CF).ok_or_else(|| {
            HyperbytedbError::Metadata("metadata column family not found".to_string())
        })?;
        let key = user_key(username);
        let value =
            serde_json::to_vec(&user).map_err(|e| HyperbytedbError::Metadata(e.to_string()))?;
        self.db
            .put_cf(&cf, key, value)
            .map_err(|e| HyperbytedbError::Metadata(e.to_string()))?;
        self.user_cache.write().insert(username.to_string(), user);
        Ok(())
    }

    async fn delete_measurement(&self, db: &str, name: &str) -> Result<(), HyperbytedbError> {
        let cf = self.db.cf_handle(META_CF).ok_or_else(|| {
            HyperbytedbError::Metadata("metadata column family not found".to_string())
        })?;
        let key = meas_key(db, name);
        self.db
            .delete_cf(&cf, key)
            .map_err(|e| HyperbytedbError::Metadata(e.to_string()))?;

        // Sweep series rows for this measurement across all retention policies.
        // The series key is rp-scoped (`series:{db}:{rp}:{meas}:{id}`) so we
        // can't form a measurement-only prefix; scan `series:{db}:` and filter.
        let series_db_prefix = format!("series:{}:", db);
        let mut series_to_delete = Vec::new();
        let iter = self.db.iterator_cf_opt(
            &cf,
            rocksdb::ReadOptions::default(),
            IteratorMode::From(series_db_prefix.as_bytes(), rocksdb::Direction::Forward),
        );
        for item in iter {
            let (key, _) = item.map_err(|e| HyperbytedbError::Metadata(e.to_string()))?;
            if !key.starts_with(series_db_prefix.as_bytes()) {
                break;
            }
            if let Ok(s) = std::str::from_utf8(&key)
                && let Some((kdb, _rp, kmeas, _id)) = parse_series_storage_key(s)
                && kdb == db
                && kmeas == name
            {
                series_to_delete.push((key.to_vec(), s.to_string()));
            }
        }
        for (k, _) in &series_to_delete {
            self.db
                .delete_cf(&cf, k)
                .map_err(|e| HyperbytedbError::Metadata(e.to_string()))?;
        }

        let cache_key = format!("{}:{}", db, name);
        self.meas_cache.write().remove(&cache_key);
        self.meas_list_cache.write().remove(db);
        {
            let tag_prefix = format!("tag_val:{}:{}:", db, name);
            self.tag_known
                .write()
                .retain(|k| !k.starts_with(&tag_prefix));
            let count_prefix = format!("{db}:{name}:");
            self.tag_count_cache
                .write()
                .retain(|k, _| !k.starts_with(&count_prefix));
            let mut series_known = self.series_known.write();
            for (_, s) in &series_to_delete {
                series_known.remove(s);
            }
        }
        // Bump the structural generation so the `meas_list_cache`
        // snapshot for this database invalidates on next read.
        self.generation.fetch_add(1, Ordering::Relaxed);
        Ok(())
    }

    async fn store_tombstone(
        &self,
        db: &str,
        measurement: &str,
        predicate_sql: &str,
    ) -> Result<String, HyperbytedbError> {
        let cf = self.db.cf_handle(META_CF).ok_or_else(|| {
            HyperbytedbError::Metadata("metadata column family not found".to_string())
        })?;
        let id = uuid::Uuid::new_v4().to_string();
        let key = format!("tombstone:{}:{}:{}", db, measurement, id);
        self.db
            .put_cf(&cf, key.as_bytes(), predicate_sql.as_bytes())
            .map_err(|e| HyperbytedbError::Metadata(e.to_string()))?;
        let tomb_key = format!("{}:{}", db, measurement);
        self.tombstone_cache.write().remove(&tomb_key);
        Ok(id)
    }

    async fn list_tombstones(
        &self,
        db: &str,
        measurement: &str,
    ) -> Result<Vec<(String, String)>, HyperbytedbError> {
        let tomb_key = format!("{}:{}", db, measurement);
        {
            let cache = self.tombstone_cache.read();
            if let Some(entries) = cache.get(&tomb_key) {
                return Ok(entries.clone());
            }
        }

        let cf = self.db.cf_handle(META_CF).ok_or_else(|| {
            HyperbytedbError::Metadata("metadata column family not found".to_string())
        })?;
        let prefix = format!("tombstone:{}:{}:", db, measurement);
        let prefix_bytes = prefix.as_bytes();
        let iter = self.db.iterator_cf_opt(
            &cf,
            rocksdb::ReadOptions::default(),
            IteratorMode::From(prefix_bytes, rocksdb::Direction::Forward),
        );
        let mut results = Vec::new();
        for item in iter {
            let (key, value) = item.map_err(|e| HyperbytedbError::Metadata(e.to_string()))?;
            if !key.starts_with(prefix_bytes) {
                break;
            }
            let id = std::str::from_utf8(&key[prefix.len()..])
                .unwrap_or("")
                .to_string();
            let predicate = std::str::from_utf8(&value).unwrap_or("").to_string();
            results.push((id, predicate));
        }

        self.tombstone_cache
            .write()
            .insert(tomb_key, results.clone());
        Ok(results)
    }

    async fn remove_tombstone(&self, db: &str, tombstone_id: &str) -> Result<(), HyperbytedbError> {
        let cf = self.db.cf_handle(META_CF).ok_or_else(|| {
            HyperbytedbError::Metadata("metadata column family not found".to_string())
        })?;
        let prefix = format!("tombstone:{}:", db);
        let prefix_bytes = prefix.as_bytes();
        let iter = self.db.iterator_cf_opt(
            &cf,
            rocksdb::ReadOptions::default(),
            IteratorMode::From(prefix_bytes, rocksdb::Direction::Forward),
        );
        for item in iter {
            let (key, _) = item.map_err(|e| HyperbytedbError::Metadata(e.to_string()))?;
            if !key.starts_with(prefix_bytes) {
                break;
            }
            if let Ok(k) = std::str::from_utf8(&key)
                && k.ends_with(tombstone_id)
            {
                self.db
                    .delete_cf(&cf, &key)
                    .map_err(|e| HyperbytedbError::Metadata(e.to_string()))?;
                // Invalidate all tombstone cache entries for this db
                {
                    let prefix = format!("{}:", db);
                    self.tombstone_cache
                        .write()
                        .retain(|k, _| !k.starts_with(&prefix));
                }
                return Ok(());
            }
        }
        Ok(())
    }

    async fn store_continuous_query(
        &self,
        db: &str,
        name: &str,
        definition: &ContinuousQueryDef,
    ) -> Result<(), HyperbytedbError> {
        let cf = self.db.cf_handle(META_CF).ok_or_else(|| {
            HyperbytedbError::Metadata("metadata column family not found".to_string())
        })?;
        let key = format!("cq:{}:{}", db, name);
        let value = serde_json::to_vec(definition)
            .map_err(|e| HyperbytedbError::Metadata(e.to_string()))?;
        self.db
            .put_cf(&cf, key.as_bytes(), value)
            .map_err(|e| HyperbytedbError::Metadata(e.to_string()))?;
        *self.cq_cache.write() = None;
        Ok(())
    }

    async fn get_continuous_query(
        &self,
        db: &str,
        name: &str,
    ) -> Result<Option<ContinuousQueryDef>, HyperbytedbError> {
        let cf = self.db.cf_handle(META_CF).ok_or_else(|| {
            HyperbytedbError::Metadata("metadata column family not found".to_string())
        })?;
        let key = format!("cq:{}:{}", db, name);
        match self.db.get_cf(&cf, key.as_bytes()) {
            Ok(Some(v)) => {
                let def: ContinuousQueryDef = serde_json::from_slice(&v)
                    .map_err(|e| HyperbytedbError::Metadata(e.to_string()))?;
                Ok(Some(def))
            }
            Ok(None) => Ok(None),
            Err(e) => Err(HyperbytedbError::Metadata(e.to_string())),
        }
    }

    async fn list_continuous_queries(
        &self,
        db: &str,
    ) -> Result<Vec<ContinuousQueryDef>, HyperbytedbError> {
        let cf = self.db.cf_handle(META_CF).ok_or_else(|| {
            HyperbytedbError::Metadata("metadata column family not found".to_string())
        })?;
        let prefix = format!("cq:{}:", db);
        let prefix_bytes = prefix.as_bytes();
        let iter = self.db.iterator_cf_opt(
            &cf,
            rocksdb::ReadOptions::default(),
            IteratorMode::From(prefix_bytes, rocksdb::Direction::Forward),
        );
        let mut results = Vec::new();
        for item in iter {
            let (key, value) = item.map_err(|e| HyperbytedbError::Metadata(e.to_string()))?;
            if !key.starts_with(prefix_bytes) {
                break;
            }
            let def: ContinuousQueryDef = serde_json::from_slice(&value)
                .map_err(|e| HyperbytedbError::Metadata(e.to_string()))?;
            results.push(def);
        }
        Ok(results)
    }

    async fn list_all_continuous_queries(
        &self,
    ) -> Result<Vec<ContinuousQueryDef>, HyperbytedbError> {
        {
            let cache = self.cq_cache.read();
            if let Some(ref entries) = *cache {
                return Ok(entries.clone());
            }
        }

        let cf = self.db.cf_handle(META_CF).ok_or_else(|| {
            HyperbytedbError::Metadata("metadata column family not found".to_string())
        })?;
        let prefix = b"cq:";
        let iter = self.db.iterator_cf_opt(
            &cf,
            rocksdb::ReadOptions::default(),
            IteratorMode::From(prefix, rocksdb::Direction::Forward),
        );
        let mut results = Vec::new();
        for item in iter {
            let (key, value) = item.map_err(|e| HyperbytedbError::Metadata(e.to_string()))?;
            if !key.starts_with(prefix) {
                break;
            }
            let def: ContinuousQueryDef = serde_json::from_slice(&value)
                .map_err(|e| HyperbytedbError::Metadata(e.to_string()))?;
            results.push(def);
        }

        *self.cq_cache.write() = Some(results.clone());
        Ok(results)
    }

    async fn drop_continuous_query(&self, db: &str, name: &str) -> Result<(), HyperbytedbError> {
        let cf = self.db.cf_handle(META_CF).ok_or_else(|| {
            HyperbytedbError::Metadata("metadata column family not found".to_string())
        })?;
        let key = format!("cq:{}:{}", db, name);
        self.db
            .delete_cf(&cf, key.as_bytes())
            .map_err(|e| HyperbytedbError::Metadata(e.to_string()))?;
        *self.cq_cache.write() = None;
        Ok(())
    }
}
