//! chDB-native [`PointsSinkPort`] implementation.
//!
//! Writes flushed points directly into per-measurement
//! `ReplacingMergeTree` tables hosted by the same chDB session that
//! [`crate::adapters::chdb::query_adapter::ChdbQueryAdapter`] reads
//! from. Tables are auto-created on first write and auto-altered with
//! `ALTER TABLE ... ADD COLUMN IF NOT EXISTS` when a write introduces
//! a previously-unseen tag or field.
//!
//! Schema layout for `(db, rp, measurement)` table
//! `` `<db>_<rp>_<measurement>` ``:
//!
//! ```sql
//! CREATE TABLE IF NOT EXISTS `<db>_<rp>_<measurement>` (
//!     `time`           DateTime64(9, 'UTC'),
//!     `origin_node_id` UInt64,
//!     `ingest_seq`     UInt64,
//!     <tag_col>        LowCardinality(String) or String,  -- per tag key;
//!                      plain `String` when distinct values exceed 100k
//!     <field_col>      Nullable(<chtype>)            -- one per field key
//! ) ENGINE = ReplacingMergeTree(`ingest_seq`)
//! PARTITION BY toDate(`time`)
//! ORDER BY (<sorted_tag_cols...>, `time`);
//! ```
//!
//! Tag/field collisions reuse [`crate::domain::chdb_naming::tag_column_name`],
//! which delegates to the same logic as the Parquet writer
//! ([`crate::domain::column_mapping::tag_column_name`]) so
//! the TimeseriesQL→ClickHouse translator produces identical column
//! references regardless of storage format.

use std::collections::{BTreeMap, BTreeSet, HashMap, HashSet};
use std::fmt::Write as _;
use std::sync::Arc;

use arrow::array::{
    ArrayRef, DictionaryArray, Float64Builder, Int32Array, Int64Builder, RecordBatch, StringArray,
    StringBuilder, TimestampNanosecondArray, UInt8Builder, UInt64Array, UInt64Builder,
};
use arrow::datatypes::{DataType, Field, Schema, TimeUnit};
use async_trait::async_trait;
use chdb_rust::arrow_insert::{InsertOptions, insert_record_batch_direct};
use chdb_rust::format::OutputFormat;
use metrics::{counter, histogram};
use parking_lot::RwLock;

use crate::adapters::chdb::catalog;
use crate::adapters::chdb::connection_pool::ChdbConnectionPool;
use crate::adapters::chdb::session::{SharedSession, execute_connection};
use crate::application::ingest_metadata::backfill_tag_metadata;
use crate::application::system_trace;
use crate::domain::chdb_naming::{
    field_column_name, quote_backticks, quoted_series_table_name, quoted_table_name,
    tag_column_name, unquoted_series_table_name, unquoted_table_name,
};
use crate::domain::field_type::{merge_field_type_map, widen_field_disc};
use crate::domain::measurement::MeasurementMeta;
use crate::domain::point::{FieldValue, Point};
use crate::domain::series::series_id_for_point;
use crate::error::HyperbytedbError;
use crate::ports::metadata::MetadataPort;
use crate::ports::points_sink::{PointsSinkPort, WriteAck};

/// Above this many distinct values per tag key, ClickHouse
/// `LowCardinality(String)` hurts more than it helps; use plain `String`.
pub const TAG_LOW_CARDINALITY_MAX: usize = 100_000;

/// Column kind for tags and fields. Tags use `LowCardinality(String)` only
/// while distinct value count stays at or below [`TAG_LOW_CARDINALITY_MAX`].
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
enum ColumnKind {
    TagLowCardinality,
    TagString,
    Field(u8),
}

impl ColumnKind {
    fn ch_column_type(self) -> &'static str {
        match self {
            ColumnKind::TagLowCardinality => "LowCardinality(String)",
            ColumnKind::TagString => "String",
            ColumnKind::Field(0) => "Nullable(Float64)",
            ColumnKind::Field(1) => "Nullable(Int64)",
            ColumnKind::Field(2) => "Nullable(UInt64)",
            ColumnKind::Field(3) => "Nullable(String)",
            ColumnKind::Field(4) => "Nullable(UInt8)",
            // Unknown discriminant: fall back to Float64 to mirror the
            // Parquet writer's identical fallback behaviour. Should not
            // be reachable in practice (`FieldValue::type_discriminant`
            // only emits 0..=4).
            ColumnKind::Field(_) => "Nullable(Float64)",
        }
    }
}

/// Split a measurement's metadata into the FACT schema (field columns) and the
/// SERIES schema (tag columns). Both start not [`TableSchema::materialized`].
fn table_schema_from_measurement_meta(
    meas_meta: &MeasurementMeta,
    tag_kinds: &HashMap<String, ColumnKind>,
) -> (TableSchema, TableSchema) {
    let field_name_set: HashSet<&str> = meas_meta.field_types.keys().map(String::as_str).collect();
    let mut field_cols = HashMap::with_capacity(meas_meta.field_types.len());
    let mut tag_cols = HashMap::with_capacity(meas_meta.tag_keys.len());

    for tag_key in &meas_meta.tag_keys {
        let phys = tag_column_name(tag_key, &field_name_set);
        let kind = tag_kinds
            .get(tag_key)
            .copied()
            .unwrap_or(ColumnKind::TagLowCardinality);
        tag_cols.insert(phys, kind);
    }
    for (field_key, disc) in &meas_meta.field_types {
        let phys = field_column_name(field_key);
        field_cols.insert(phys, ColumnKind::Field(*disc));
    }

    (
        TableSchema {
            columns: field_cols,
            materialized: false,
        },
        TableSchema {
            columns: tag_cols,
            materialized: false,
        },
    )
}

fn tag_column_kind(distinct_values: usize) -> ColumnKind {
    if distinct_values > TAG_LOW_CARDINALITY_MAX {
        ColumnKind::TagString
    } else {
        ColumnKind::TagLowCardinality
    }
}

/// In-memory representation of a `(db, rp, measurement)` table's
/// physical schema. The native adapter keeps one of these per table
/// in [`ChdbNativeAdapter::schemas`] and consults it before every
/// `INSERT` so unseen tags/fields can be added with a single
/// `ALTER TABLE`.
#[derive(Debug, Clone, Default)]
struct TableSchema {
    /// Physical column name → kind. Includes only user-driven columns
    /// (tags + fields); the fixed `time`, `origin_node_id`, and
    /// `ingest_seq` columns are implicit.
    columns: HashMap<String, ColumnKind>,
    /// `true` once this process has run `CREATE TABLE` / `ALTER` against
    /// chDB for this table. Metadata-only warm entries leave this `false`
    /// so the first post-restart flush still issues `CREATE TABLE IF NOT
    /// EXISTS` and tag-type reconciliation `MODIFY`s.
    materialized: bool,
}

impl TableSchema {
    fn knows(&self, col: &str, kind: ColumnKind) -> bool {
        self.columns.get(col) == Some(&kind)
    }
}

#[derive(Clone, Eq, PartialEq, Hash)]
struct TableKey {
    db: String,
    rp: String,
    measurement: String,
}

/// chDB-native `PointsSinkPort`.
pub struct ChdbNativeAdapter {
    session: SharedSession,
    /// Optional metadata store for per-tag distinct value counts (DDL only).
    metadata: Option<Arc<dyn MetadataPort>>,
    /// Cached per-table FACT schemas (field columns only — tags live on the
    /// `_series` table). Hot-path reads take the read lock; schema mutations
    /// take the write lock plus the per-table mutex below to serialise
    /// concurrent ADD COLUMN attempts on the same table.
    schemas: Arc<RwLock<HashMap<TableKey, TableSchema>>>,
    /// Cached per-table SERIES (tag dimension) schemas, parallel to `schemas`.
    /// Holds the tag columns of the `<table>_series` table. New tag *keys*
    /// ALTER this table, never the fact table.
    series_schemas: Arc<RwLock<HashMap<TableKey, TableSchema>>>,
    /// Per-table set of `series_id`s already inserted into the `_series`
    /// dimension table and persisted to metadata, so steady-state flushes skip
    /// the dimension insert + metadata write. Loaded on startup by
    /// [`Self::warm_series_from_metadata`]. A lean `HashSet<u64>` — the
    /// authoritative `series_id → tags` map lives in the metadata layer.
    known_series: Arc<RwLock<HashMap<TableKey, HashSet<u64>>>>,
    /// Per-table async mutex serialising DDL. Without this two
    /// concurrent flush tasks for the same measurement could each
    /// observe the cache miss, race on `ALTER TABLE`, and one could
    /// fail loudly on a duplicate-column error. With it, the second
    /// caller waits, sees the cached schema, and is a no-op.
    ddl_locks: Arc<tokio::sync::Mutex<HashMap<TableKey, Arc<tokio::sync::Mutex<()>>>>>,
    /// When true (default), inserts go through the Arrow C Data Interface
    /// (`INSERT INTO … SELECT * FROM ArrowStream(...)`) instead of building a
    /// giant `INSERT … VALUES` SQL string. The Arrow path avoids SQL
    /// serialization + re-parsing and is several times faster. Set
    /// `HYPERBYTEDB_DISABLE_ARROW_INSERT=1` to fall back to the SQL path.
    use_arrow: bool,
}

impl ChdbNativeAdapter {
    pub fn new(session: SharedSession) -> Self {
        Self::with_metadata(session, None)
    }

    pub fn with_metadata(session: SharedSession, metadata: Option<Arc<dyn MetadataPort>>) -> Self {
        let use_arrow = std::env::var("HYPERBYTEDB_DISABLE_ARROW_INSERT").is_err();
        Self {
            session,
            metadata,
            schemas: Arc::new(RwLock::new(HashMap::new())),
            series_schemas: Arc::new(RwLock::new(HashMap::new())),
            known_series: Arc::new(RwLock::new(HashMap::new())),
            ddl_locks: Arc::new(tokio::sync::Mutex::new(HashMap::new())),
            use_arrow,
        }
    }

    /// Override the insert path (Arrow vs SQL `VALUES`). Defaults to Arrow.
    pub fn with_arrow_inserts(mut self, enabled: bool) -> Self {
        self.use_arrow = enabled;
        self
    }

    /// Re-warm schema caches from metadata (e.g. after peer metadata sync).
    pub async fn refresh_schemas_from_metadata(&self) -> Result<usize, HyperbytedbError> {
        let warmed = self.warm_schemas_from_metadata().await?;
        let _ = self.sync_materialized_from_engine().await?;
        Ok(warmed)
    }

    /// Pre-populate the schema cache from RocksDB measurement metadata so a
    /// restart does not treat known tables as empty (skipping `ALTER` / type
    /// upgrades) or re-resolve column kinds from scratch on every table.
    ///
    /// Entries are marked not [`TableSchema::materialized`] until the next
    /// successful DDL on that table in this process.
    pub async fn warm_schemas_from_metadata(&self) -> Result<usize, HyperbytedbError> {
        let Some(meta) = &self.metadata else {
            return Ok(0);
        };

        let databases = meta.list_databases().await?;
        let mut pending: Vec<(TableKey, TableSchema, TableSchema)> = Vec::new();

        for db in databases {
            let measurements = meta.list_measurements(&db.name).await?;
            for meas_name in measurements {
                let Some(meas_meta) = meta.get_measurement(&db.name, &meas_name).await? else {
                    continue;
                };
                let (fact_schema, series_schema) = self
                    .table_schema_from_measurement_meta(
                        meta.as_ref(),
                        &db.name,
                        &meas_name,
                        &meas_meta,
                    )
                    .await?;
                if fact_schema.columns.is_empty() && series_schema.columns.is_empty() {
                    continue;
                }
                for rp in &db.retention_policies {
                    let key = TableKey {
                        db: db.name.clone(),
                        rp: rp.name.clone(),
                        measurement: meas_name.clone(),
                    };
                    pending.push((key, fact_schema.clone(), series_schema.clone()));
                }
            }
        }

        let warmed = pending.len();
        let mut fact_writers = self.schemas.write();
        let mut series_writers = self.series_schemas.write();
        for (key, fact_schema, series_schema) in pending {
            fact_writers.insert(key.clone(), fact_schema);
            series_writers.insert(key, series_schema);
        }

        Ok(warmed)
    }

    /// Mark schema-cache entries as materialized when the engine catalog already
    /// contains the corresponding fact / series tables (e.g. after backup restore).
    pub async fn sync_materialized_from_engine(&self) -> Result<usize, HyperbytedbError> {
        let pool = self.session.pool()?;
        let raw = tokio::task::spawn_blocking(move || {
            pool.with_connection(|conn| {
                let sql =
                    "SELECT name FROM system.tables WHERE database = 'default' FORMAT TabSeparated";
                let result =
                    execute_connection(conn, sql, chdb_rust::format::OutputFormat::TabSeparated);
                match result {
                    Ok(qr) => qr
                        .data_utf8()
                        .map_err(|e| HyperbytedbError::Chdb(e.to_string())),
                    Err(e) => Err(HyperbytedbError::Chdb(e.to_string())),
                }
            })
        })
        .await
        .map_err(|e| {
            HyperbytedbError::Internal(format!("chDB materialization sync join error: {e}"))
        })??;

        let attached: HashSet<String> = raw
            .lines()
            .filter(|line| !line.is_empty())
            .map(str::to_string)
            .collect();
        if attached.is_empty() {
            return Ok(0);
        }

        let keys: Vec<TableKey> = self.schemas.read().keys().cloned().collect();
        let mut synced = 0usize;
        let mut fact_writers = self.schemas.write();
        let mut series_writers = self.series_schemas.write();
        for key in keys {
            let fact = unquoted_table_name(&key.db, &key.rp, &key.measurement);
            let series = unquoted_series_table_name(&key.db, &key.rp, &key.measurement);
            if attached.contains(&fact)
                && fact_writers.get_mut(&key).is_some_and(|schema| {
                    if !schema.materialized {
                        schema.materialized = true;
                        true
                    } else {
                        false
                    }
                })
            {
                synced += 1;
            }
            if attached.contains(&series)
                && let Some(schema) = series_writers.get_mut(&key)
                && !schema.materialized
            {
                schema.materialized = true;
            }
        }
        Ok(synced)
    }

    /// Warm the in-memory `known_series` set from durable metadata so the first
    /// post-restart flush of an existing series does not re-insert its dimension
    /// row or re-persist it. Mirrors [`Self::warm_schemas_from_metadata`].
    pub async fn warm_series_from_metadata(&self) -> Result<usize, HyperbytedbError> {
        let Some(meta) = &self.metadata else {
            return Ok(0);
        };
        let databases = meta.list_databases().await?;
        let mut warmed = 0usize;
        for db in databases {
            let measurements = meta.list_measurements(&db.name).await?;
            for meas_name in &measurements {
                for rp in &db.retention_policies {
                    // IDs only — decoding each series' tag map here would cost
                    // a `BTreeMap` allocation per series and dominate startup
                    // memory/time at high cardinality. The warm only needs IDs.
                    let series = meta.list_series_ids(&db.name, &rp.name, meas_name).await?;
                    if series.is_empty() {
                        continue;
                    }
                    let key = TableKey {
                        db: db.name.clone(),
                        rp: rp.name.clone(),
                        measurement: meas_name.clone(),
                    };
                    let ids: HashSet<u64> = series.into_iter().collect();
                    warmed += ids.len();
                    self.known_series.write().insert(key, ids);
                }
            }
        }
        Ok(warmed)
    }

    /// True if `series_id` is already registered (dimension row inserted +
    /// persisted) for this table.
    fn series_known(&self, key: &TableKey, sid: u64) -> bool {
        self.known_series
            .read()
            .get(key)
            .is_some_and(|s| s.contains(&sid))
    }

    /// Record `series_id`s as registered for this table.
    fn mark_series(&self, key: &TableKey, ids: impl IntoIterator<Item = u64>) {
        let mut map = self.known_series.write();
        map.entry(key.clone()).or_default().extend(ids);
    }

    async fn ddl_mutex(&self, key: &TableKey) -> Arc<tokio::sync::Mutex<()>> {
        let mut map = self.ddl_locks.lock().await;
        map.entry(key.clone())
            .or_insert_with(|| Arc::new(tokio::sync::Mutex::new(())))
            .clone()
    }

    /// Load fact-table field columns from the engine catalog (`DESCRIBE TABLE`).
    async fn engine_field_columns(
        &self,
        table: &str,
    ) -> Result<HashMap<String, u8>, HyperbytedbError> {
        let pool = self.session.pool()?;
        let table = table.to_string();
        let raw = tokio::task::spawn_blocking(move || {
            pool.with_connection(|conn| {
                let sql = format!("DESCRIBE TABLE {table}");
                let result =
                    execute_connection(conn, &sql, chdb_rust::format::OutputFormat::TabSeparated);
                match result {
                    Ok(qr) => qr
                        .data_utf8()
                        .map_err(|e| HyperbytedbError::Chdb(e.to_string())),
                    Err(e) => {
                        let msg = e.to_string();
                        if msg.contains("doesn't exist")
                            || msg.contains("UNKNOWN_TABLE")
                            || (msg.contains("Table") && msg.contains("not exist"))
                        {
                            Ok(String::new())
                        } else {
                            Err(HyperbytedbError::Chdb(msg))
                        }
                    }
                }
            })
        })
        .await
        .map_err(|e| HyperbytedbError::Internal(format!("chDB describe join error: {e}")))??;

        let skip = ["time", "origin_node_id", "ingest_seq", "series_id"];
        let mut out = HashMap::new();
        for line in raw.lines().filter(|l| !l.is_empty()) {
            let mut parts = line.split('\t');
            let name = parts.next().unwrap_or("").trim();
            let ty = parts.next().unwrap_or("").trim();
            if skip.contains(&name) {
                continue;
            }
            if let Some(disc) = ch_type_to_field_disc(ty) {
                out.insert(name.to_string(), disc);
            }
        }
        Ok(out)
    }

    /// Run a `chdb` statement on the shared session via
    /// `spawn_blocking`. Used for both DDL and `INSERT` paths.
    async fn execute(&self, sql: String) -> Result<(), HyperbytedbError> {
        let pool = self.session.pool()?;
        tokio::task::spawn_blocking(move || run_sync(&pool, &sql))
            .await
            .map_err(|e| HyperbytedbError::Internal(format!("chDB DDL join error: {e}")))?
    }

    /// Compute the union of tag keys across `points`, then ensure the
    /// table exists with at least those columns plus all fields. Returns
    /// the post-update schema snapshot used by the INSERT renderer.
    async fn ensure_table(
        &self,
        key: &TableKey,
        points: &[Point],
    ) -> Result<EnsuredTable, HyperbytedbError> {
        let table = quoted_table_name(&key.db, &key.rp, &key.measurement);
        let series_table = quoted_series_table_name(&key.db, &key.rp, &key.measurement);
        let table_unquoted = unquoted_table_name(&key.db, &key.rp, &key.measurement);

        // Discover required columns from this batch. A fixed-schema telemetry
        // stream repeats the same ~handful of tag/field keys across every one
        // of the (often tens of thousands of) points, so only clone a key
        // string when it is genuinely new — cloning unconditionally meant
        // hundreds of thousands of wasted allocations per flush. Membership
        // checks are allocation-free.
        let mut tag_keys: BTreeSet<String> = BTreeSet::new();
        let mut field_types: HashMap<String, u8> = HashMap::new();
        for p in points {
            for k in p.tags.keys() {
                if !tag_keys.contains(k) {
                    tag_keys.insert(k.clone());
                }
            }
            for (k, v) in &p.fields {
                let new_disc = v.type_discriminant();
                match field_types.get_mut(k) {
                    Some(existing) => {
                        if let Some(w) = widen_field_disc(*existing, new_disc) {
                            *existing = w;
                        }
                    }
                    None => {
                        field_types.insert(k.clone(), new_disc);
                    }
                }
            }
        }

        if let Some(meta) = &self.metadata
            && let Some(meas_meta) = meta.get_measurement(&key.db, &key.measurement).await?
        {
            field_types = merge_field_type_map(&meas_meta.field_types, &field_types);
            for k in &meas_meta.tag_keys {
                tag_keys.insert(k.clone());
            }
        }

        // Snapshot pre-existing schemas first (enables flush fast path without
        // scanning all tag values when both tables are materialized). The fact
        // schema holds only field columns; the series schema holds tag columns.
        let cached = { self.schemas.read().get(key).cloned() }.unwrap_or_default();
        let series_cached = { self.series_schemas.read().get(key).cloned() }.unwrap_or_default();

        // Union cached/materialized chDB columns so sparse batches still pad to
        // the full on-disk table width after restart (metadata may be partial).
        for (phys, kind) in &cached.columns {
            if let ColumnKind::Field(d) = kind {
                merge_cached_field_column(&mut field_types, phys, *d);
            }
        }
        // Always probe the engine when the table may already exist (e.g. evolved
        // Telegraf `netstat` with 75 columns but sparse metadata). Relying only
        // on `cached.materialized` misses tables not yet in the in-memory cache.
        if let Ok(engine_cols) = self.engine_field_columns(&table).await {
            for (phys, d) in engine_cols {
                merge_cached_field_column(&mut field_types, &phys, d);
            }
        }

        // Collision-aware physical column names. Tag wins the
        // `__tag__` prefix when a tag key matches a field name, same
        // as the Parquet path.
        let field_name_set: HashSet<&str> = field_types.keys().map(String::as_str).collect();

        // Tag kinds are resolved against the SERIES cache, since tag columns now
        // live on the `_series` table.
        let mut tag_phys: Vec<(String, String, ColumnKind)> = Vec::with_capacity(tag_keys.len());
        for k in &tag_keys {
            let phys = tag_column_name(k, &field_name_set);
            let kind = self
                .resolve_tag_column_kind(
                    &series_cached,
                    &key.db,
                    &key.measurement,
                    k,
                    &phys,
                    points,
                )
                .await?;
            tag_phys.push((k.clone(), phys, kind));
        }
        let mut field_phys: Vec<(String, String, u8)> = field_types
            .iter()
            .map(|(k, d)| (k.clone(), field_column_name(k), *d))
            .collect();
        // Must match [`build_create_table_sql`]: DDL emits field columns sorted by
        // physical name, and Arrow `INSERT … SELECT *` maps batch columns by position.
        field_phys.sort_by(|a, b| a.1.cmp(&b.1));

        // Nothing to do if both caches already know every required column. We
        // optimistically check first to avoid taking the DDL mutex on the
        // steady-state path. Tag DDL is checked against the series schema.
        let tag_ddl_needed = tag_phys
            .iter()
            .any(|(_, phys, kind)| series_cached.columns.get(phys).copied() != Some(*kind));
        let missing_fields: Vec<&(String, String, u8)> = field_phys
            .iter()
            .filter(|(_, phys, d)| !cached.knows(phys, ColumnKind::Field(*d)))
            .collect();

        if cached.materialized
            && series_cached.materialized
            && !tag_ddl_needed
            && missing_fields.is_empty()
        {
            return Ok(EnsuredTable {
                table,
                series_table,
                tag_phys,
                field_phys,
            });
        }

        // Slow path: serialise DDL on this table (both fact + series).
        let ddl_lock = self.ddl_mutex(key).await;
        let _guard = ddl_lock.lock().await;

        // Re-read caches under the DDL lock; another writer may have already
        // added the columns we needed while we were waiting.
        let cached = { self.schemas.read().get(key).cloned() }.unwrap_or_default();
        let series_cached = { self.series_schemas.read().get(key).cloned() }.unwrap_or_default();

        // Fact table: fields only. ALTERs run only against an already-existing
        // table (materialized) or a warmed-from-metadata entry; a cold CREATE
        // already includes every field, so no redundant ADD COLUMN.
        let create_fact = if !cached.materialized {
            Some(build_create_table_sql(&table, &field_phys, None))
        } else {
            None
        };
        let mut fact_alters = if cached.materialized || !cached.columns.is_empty() {
            let mut alters = build_alter_add_field_columns(&table, &cached, &field_phys);
            alters.extend(build_alter_reconcile_field_widening(
                &table,
                &cached,
                &field_phys,
            ));
            alters
        } else {
            Vec::new()
        };

        // Series (dimension) table: tag columns only.
        let create_series = if !series_cached.materialized {
            Some(build_create_series_table_sql(&series_table, &tag_phys))
        } else {
            None
        };
        let mut series_alters = if series_cached.materialized || !series_cached.columns.is_empty() {
            build_alter_add_series_columns(&series_table, &series_cached, &tag_phys)
        } else {
            Vec::new()
        };
        if !series_cached.materialized && !series_cached.columns.is_empty() {
            // Warmed-from-metadata series table may still carry LowCardinality
            // tags that have since crossed TAG_LOW_CARDINALITY_MAX.
            series_alters.extend(build_alter_reconcile_tag_strings(&series_table, &tag_phys));
        }

        if let Some(sql) = create_fact {
            tracing::debug!(table = %table_unquoted, "creating chDB native fact table");
            self.execute(sql).await?;
        }
        if let Some(sql) = create_series {
            tracing::debug!(table = %table_unquoted, "creating chDB native series table");
            self.execute(sql).await?;
        }
        fact_alters.append(&mut series_alters);
        for stmt in fact_alters {
            tracing::debug!(table = %table_unquoted, alter = %stmt, "altering chDB native table");
            self.execute(stmt).await?;
        }

        // Update the fact cache (fields) and series cache (tags).
        {
            let mut writers = self.schemas.write();
            let entry = writers.entry(key.clone()).or_default();
            for (_, phys, d) in &field_phys {
                entry.columns.insert(phys.clone(), ColumnKind::Field(*d));
            }
            entry.materialized = true;
        }
        {
            let mut writers = self.series_schemas.write();
            let entry = writers.entry(key.clone()).or_default();
            for (_, phys, kind) in &tag_phys {
                entry.columns.insert(phys.clone(), *kind);
            }
            entry.materialized = true;
        }

        if let Err(e) = catalog::persist_default_database_metadata(&self.session).await {
            tracing::warn!(
                error = %e,
                "failed to persist chDB default database metadata after table DDL"
            );
        }

        Ok(EnsuredTable {
            table,
            series_table,
            tag_phys,
            field_phys,
        })
    }

    /// Insert any brand-new series (by `series_id`) from this batch into the
    /// `_series` dimension table and persist them to the metadata layer, then
    /// mark them in the in-memory cache. Idempotent: known/duplicate ids are
    /// skipped, so steady-state flushes do no work here. Runs before the fact
    /// insert so a tag-resolving query never sees a `series_id` without its row.
    async fn register_new_series(
        &self,
        key: &TableKey,
        ensured: &EnsuredTable,
        points: &[Point],
        sids: &[u64],
    ) -> Result<(), HyperbytedbError> {
        let mut seen: HashSet<u64> = HashSet::new();
        let mut new_series: Vec<(u64, &Point)> = Vec::new();
        for (i, &sid) in sids.iter().enumerate() {
            if self.series_known(key, sid) || !seen.insert(sid) {
                continue;
            }
            new_series.push((sid, &points[i]));
        }
        if new_series.is_empty() {
            return Ok(());
        }

        if self.use_arrow {
            let batch = build_series_record_batch(ensured, &new_series)?;
            let pool = self.session.pool()?;
            let series_table = ensured.series_table.clone();
            tokio::task::spawn_blocking(move || {
                pool.with_connection(|conn| {
                    insert_record_batch_direct(
                        conn,
                        &series_table,
                        batch,
                        InsertOptions::default_bulk(),
                    )
                    .map_err(|e| HyperbytedbError::Chdb(e.to_string()))
                })
            })
            .await
            .map_err(|e| {
                HyperbytedbError::Internal(format!("chDB series insert join error: {e}"))
            })??;
        } else {
            let sql = build_series_insert_sql(ensured, &new_series);
            self.execute(sql).await?;
        }

        // Mark known first so concurrent/subsequent flushes skip the insert.
        self.mark_series(key, new_series.iter().map(|(id, _)| *id));

        // Persist to the metadata layer (local-deterministic, never via Raft).
        // Non-fatal: the dimension rows are already in chDB; on persist failure
        // a post-restart warm simply re-registers (idempotent).
        if let Some(meta) = &self.metadata {
            let entries: Vec<(u64, BTreeMap<String, String>)> = new_series
                .iter()
                .map(|(id, p)| (*id, p.tags.clone()))
                .collect();
            if let Err(e) = meta
                .register_series_batch(&key.db, &key.rp, &key.measurement, &entries)
                .await
            {
                tracing::warn!(error = %e, "failed to persist series metadata; re-registers after restart");
            } else {
                let mut tag_pairs: Vec<(String, String)> = Vec::new();
                for (_, p) in &new_series {
                    for (k, v) in &p.tags {
                        tag_pairs.push((k.clone(), v.clone()));
                    }
                }
                if let Err(e) =
                    backfill_tag_metadata(meta, &key.db, &key.measurement, tag_pairs).await
                {
                    tracing::warn!(
                        error = %e,
                        "failed to backfill tag metadata from flushed series"
                    );
                }
            }
        }

        Ok(())
    }

    async fn table_schema_from_measurement_meta(
        &self,
        meta: &dyn MetadataPort,
        db: &str,
        measurement: &str,
        meas_meta: &MeasurementMeta,
    ) -> Result<(TableSchema, TableSchema), HyperbytedbError> {
        let mut tag_kinds = HashMap::with_capacity(meas_meta.tag_keys.len());
        for tag_key in &meas_meta.tag_keys {
            let count = meta
                .count_tag_values(db, tag_key, Some(measurement))
                .await?;
            tag_kinds.insert(tag_key.clone(), tag_column_kind(count));
        }
        Ok(table_schema_from_measurement_meta(meas_meta, &tag_kinds))
    }

    /// Resolve tag column kind using the schema cache when safe, otherwise
    /// count distinct values via metadata.
    async fn resolve_tag_column_kind(
        &self,
        cached: &TableSchema,
        db: &str,
        measurement: &str,
        tag_key: &str,
        phys_col: &str,
        points: &[Point],
    ) -> Result<ColumnKind, HyperbytedbError> {
        if cached.materialized
            && let Some(kind) = cached.columns.get(phys_col).copied()
        {
            match kind {
                ColumnKind::TagString => return Ok(kind),
                ColumnKind::TagLowCardinality => {
                    // Accept the materialized LowCardinality column directly.
                    // New tag *values* never require DDL for a
                    // LowCardinality(String) column, so we skip the per-flush
                    // novelty scan that previously walked every point for this
                    // tag and did a metadata lookup per distinct value
                    // (O(rows) — the dominant flush cost for high-cardinality
                    // tags like `machine_id`). Auto LC->String promotion above
                    // TAG_LOW_CARDINALITY_MAX still happens at first
                    // materialization and whenever a new tag *key* appears; an
                    // oversized LowCardinality column degrades gracefully rather
                    // than being written incorrectly.
                    return Ok(kind);
                }
                _ => {}
            }
        }
        let count = self
            .distinct_tag_value_count(db, measurement, tag_key, points)
            .await?;
        Ok(tag_column_kind(count))
    }

    /// Distinct tag values for `(db, measurement, tag_key)` from metadata plus
    /// novel values in this batch.
    async fn distinct_tag_value_count(
        &self,
        db: &str,
        measurement: &str,
        tag_key: &str,
        points: &[Point],
    ) -> Result<usize, HyperbytedbError> {
        let Some(meta) = &self.metadata else {
            let mut values: HashSet<String> = HashSet::new();
            for p in points {
                if let Some(v) = p.tags.get(tag_key) {
                    values.insert(v.clone());
                }
            }
            return Ok(values.len());
        };

        let base = meta
            .count_tag_values(db, tag_key, Some(measurement))
            .await?;
        let mut novel: HashSet<String> = HashSet::new();
        for p in points {
            if let Some(v) = p.tags.get(tag_key)
                && !meta.tag_value_is_known(db, measurement, tag_key, v).await?
            {
                novel.insert(v.clone());
            }
        }
        Ok(base + novel.len())
    }

    /// Create fact + `_series` tables for `meta` when they do not yet exist.
    pub async fn ensure_measurement_schema_impl(
        &self,
        db: &str,
        rp: &str,
        meta: &MeasurementMeta,
    ) -> Result<(), HyperbytedbError> {
        let key = TableKey {
            db: db.to_string(),
            rp: rp.to_string(),
            measurement: meta.name.clone(),
        };
        let table = quoted_table_name(db, rp, &meta.name);
        let series_table = quoted_series_table_name(db, rp, &meta.name);

        let field_name_set: HashSet<&str> = meta.field_types.keys().map(String::as_str).collect();
        let tag_phys: Vec<(String, String, ColumnKind)> = meta
            .tag_keys
            .iter()
            .map(|k| {
                (
                    k.clone(),
                    tag_column_name(k, &field_name_set),
                    ColumnKind::TagLowCardinality,
                )
            })
            .collect();
        let mut field_phys: Vec<(String, String, u8)> = meta
            .field_types
            .iter()
            .map(|(k, d)| (k.clone(), field_column_name(k), *d))
            .collect();
        field_phys.sort_by(|a, b| a.1.cmp(&b.1));

        let cached = { self.schemas.read().get(&key).cloned() }.unwrap_or_default();
        let series_cached = { self.series_schemas.read().get(&key).cloned() }.unwrap_or_default();

        if cached.materialized && series_cached.materialized {
            return Ok(());
        }

        let ddl_lock = self.ddl_mutex(&key).await;
        let _guard = ddl_lock.lock().await;

        let cached = { self.schemas.read().get(&key).cloned() }.unwrap_or_default();
        let series_cached = { self.series_schemas.read().get(&key).cloned() }.unwrap_or_default();

        if !cached.materialized {
            let sql = build_create_table_sql(&table, &field_phys, summing_columns_from_meta(meta));
            tracing::debug!(table = %meta.name, "creating chDB native fact table for MV destination");
            self.execute(sql).await?;
        }
        if !series_cached.materialized {
            let sql = build_create_series_table_sql(&series_table, &tag_phys);
            tracing::debug!(table = %meta.name, "creating chDB native series table for MV destination");
            self.execute(sql).await?;
        }

        {
            let mut writers = self.schemas.write();
            let entry = writers.entry(key.clone()).or_default();
            for (_, phys, d) in &field_phys {
                entry.columns.insert(phys.clone(), ColumnKind::Field(*d));
            }
            entry.materialized = true;
        }
        {
            let mut writers = self.series_schemas.write();
            let entry = writers.entry(key).or_default();
            for (_, phys, kind) in &tag_phys {
                entry.columns.insert(phys.clone(), *kind);
            }
            entry.materialized = true;
        }

        if let Err(e) = catalog::persist_default_database_metadata(&self.session).await {
            tracing::warn!(
                error = %e,
                "failed to persist chDB default database metadata after MV destination DDL"
            );
        }

        Ok(())
    }

    /// Build a prepared WAL slot from points grouped by measurement (ingest-time Arrow path).
    pub async fn build_prepared_wal_slot(
        &self,
        db: &str,
        rp: &str,
        origin_node_id: u64,
        points: &[Point],
    ) -> Result<crate::domain::prepared_wal::PreparedWalSlot, HyperbytedbError> {
        use std::collections::BTreeMap;

        use crate::domain::point_coalesce::coalesce_points_within_measurements;
        use crate::domain::prepared_wal::PreparedWalSlot;

        if points.is_empty() {
            return Ok(PreparedWalSlot {
                database: db.to_string(),
                retention_policy: rp.to_string(),
                origin_node_id,
                measurements: Vec::new(),
            });
        }

        let coalesced_points = coalesce_points_within_measurements(points);

        let mut by_meas: BTreeMap<String, Vec<Point>> = BTreeMap::new();
        for p in coalesced_points {
            by_meas.entry(p.measurement.clone()).or_default().push(p);
        }

        let mut measurements = Vec::with_capacity(by_meas.len());
        for (measurement, meas_points) in by_meas {
            let key = TableKey {
                db: db.to_string(),
                rp: rp.to_string(),
                measurement: measurement.clone(),
            };
            measurements.push(
                self.prepare_measurement_batch(&key, origin_node_id, &meas_points, 0)
                    .await?,
            );
        }

        Ok(PreparedWalSlot {
            database: db.to_string(),
            retention_policy: rp.to_string(),
            origin_node_id,
            measurements,
        })
    }

    async fn prepare_measurement_batch(
        &self,
        key: &TableKey,
        origin_node_id: u64,
        points: &[Point],
        ingest_seq_base: u64,
    ) -> Result<crate::domain::prepared_wal::PreparedMeasurementBatch, HyperbytedbError> {
        use std::sync::Arc;

        use crate::domain::prepared_wal::PreparedMeasurementBatch;

        let ensured = self.ensure_table(key, points).await?;
        let origins = vec![origin_node_id; points.len()];
        let sids: Vec<u64> = points.iter().map(series_id_for_point).collect();
        let new_series_batch = self
            .build_new_series_batch(key, &ensured, points, &sids)
            .await?;

        let (batch, min_time, max_time) =
            build_record_batch(&ensured, &origins, ingest_seq_base, points, &sids)?;

        Ok(PreparedMeasurementBatch {
            measurement: key.measurement.clone(),
            table_name: ensured.table.clone(),
            series_table_name: ensured.series_table.clone(),
            batch: Arc::new(batch),
            row_count: points.len(),
            min_time,
            max_time,
            new_series_batch,
        })
    }

    async fn build_new_series_batch(
        &self,
        key: &TableKey,
        ensured: &EnsuredTable,
        points: &[Point],
        sids: &[u64],
    ) -> Result<Option<Arc<RecordBatch>>, HyperbytedbError> {
        let mut seen: HashSet<u64> = HashSet::new();
        let mut new_series: Vec<(u64, &Point)> = Vec::new();
        for (i, &sid) in sids.iter().enumerate() {
            if self.series_known(key, sid) || !seen.insert(sid) {
                continue;
            }
            new_series.push((sid, &points[i]));
        }
        if new_series.is_empty() {
            return Ok(None);
        }
        if !self.use_arrow {
            return Ok(None);
        }
        let batch = build_series_record_batch(ensured, &new_series)?;
        Ok(Some(Arc::new(batch)))
    }

    async fn insert_prepared_series(
        &self,
        key: &TableKey,
        ensured: &EnsuredTable,
        series_batch: &RecordBatch,
    ) -> Result<(), HyperbytedbError> {
        let pool = self.session.pool()?;
        let series_table = ensured.series_table.clone();
        let batch = series_batch.clone();
        tokio::task::spawn_blocking(move || {
            pool.with_connection(|conn| {
                insert_record_batch_direct(
                    conn,
                    &series_table,
                    batch,
                    InsertOptions::default_bulk(),
                )
                .map_err(|e| HyperbytedbError::Chdb(e.to_string()))
            })
        })
        .await
        .map_err(|e| HyperbytedbError::Internal(format!("chDB series insert join error: {e}")))??;

        if series_batch.num_rows() > 0 {
            let sid_col = series_batch
                .column(0)
                .as_any()
                .downcast_ref::<UInt64Array>()
                .ok_or_else(|| HyperbytedbError::Internal("series batch sid col".into()))?;
            let ids: Vec<u64> = (0..series_batch.num_rows())
                .map(|i| sid_col.value(i))
                .collect();
            self.mark_series(key, ids.iter().copied());
        }
        Ok(())
    }
}

struct EnsuredTable {
    /// Backtick-quoted physical fact table name for use in SQL.
    table: String,
    /// Backtick-quoted `<table>_series` dimension table name.
    series_table: String,
    /// `(logical_tag_key, physical_column_name, column_kind)` tuples. Retained
    /// even though the fact table no longer stores tags: it drives the `_series`
    /// dimension table's DDL and the per-series dimension rows.
    tag_phys: Vec<(String, String, ColumnKind)>,
    /// `(logical_field_key, physical_column_name, type_discriminant)`.
    field_phys: Vec<(String, String, u8)>,
}

#[async_trait]
impl PointsSinkPort for ChdbNativeAdapter {
    async fn write_points(
        &self,
        db: &str,
        rp: &str,
        measurement: &str,
        origins: &[u64],
        ingest_seq_base: u64,
        points: &[Point],
    ) -> Result<WriteAck, HyperbytedbError> {
        if points.is_empty() {
            return Ok(WriteAck {
                min_time: 0,
                max_time: 0,
                row_count: 0,
            });
        }
        debug_assert_eq!(
            origins.len(),
            points.len(),
            "origins must be parallel to points"
        );
        let trace_start = system_trace::start_timer();
        let span = system_trace::sink_write_span(db, rp, measurement, points.len());
        let _guard = span.enter();
        system_trace::record_bool("use_arrow", self.use_arrow);

        let key = TableKey {
            db: db.to_string(),
            rp: rp.to_string(),
            measurement: measurement.to_string(),
        };
        let ensure_start = std::time::Instant::now();
        let ensured = self.ensure_table(&key, points).await?;
        histogram!("hyperbytedb_flush_sink_ensure_table_seconds")
            .record(ensure_start.elapsed().as_secs_f64());
        system_trace::record_phase("ensure_table_us", ensure_start.elapsed());

        // Partial-line coalescing runs in the flush service over the full
        // measurement batch (before max_points_per_batch splitting).
        // Deterministic series id per (post-coalesce) point. Register any
        // brand-new series into the dimension table + metadata before inserting
        // the fact rows, so tag-resolving queries always find the series row.
        let register_series_start = std::time::Instant::now();
        let sids: Vec<u64> = points.iter().map(series_id_for_point).collect();
        self.register_new_series(&key, &ensured, points, &sids)
            .await?;
        system_trace::record_phase("register_series_us", register_series_start.elapsed());

        let row_count = points.len();
        let (min_time, max_time) = if self.use_arrow {
            // Arrow path: build the RecordBatch on this (async) task — it only
            // reads points by reference, no deep Point clone — then move the
            // Arc-backed batch into `spawn_blocking` for the FFI insert.
            let build_start = std::time::Instant::now();
            let (batch, min_time, max_time) =
                build_record_batch(&ensured, origins, ingest_seq_base, points, &sids)?;
            histogram!("hyperbytedb_flush_sink_build_insert_sql_seconds")
                .record(build_start.elapsed().as_secs_f64());
            system_trace::record_phase("build_batch_us", build_start.elapsed());

            let pool = self.session.pool()?;
            let table = ensured.table.clone();
            let insert_start = std::time::Instant::now();
            tokio::task::spawn_blocking(move || {
                pool.with_connection(|conn| {
                    insert_record_batch_direct(conn, &table, batch, InsertOptions::default_bulk())
                        .map_err(|e| HyperbytedbError::Chdb(e.to_string()))
                })
            })
            .await
            .map_err(|e| {
                HyperbytedbError::Internal(format!("chDB arrow insert join error: {e}"))
            })??;
            histogram!("hyperbytedb_flush_sink_chdb_insert_seconds")
                .record(insert_start.elapsed().as_secs_f64());
            system_trace::record_phase("chdb_insert_us", insert_start.elapsed());
            (min_time, max_time)
        } else {
            // Legacy SQL VALUES path (HYPERBYTEDB_DISABLE_ARROW_INSERT=1).
            let build_insert_start = std::time::Instant::now();
            let (sql, min_time, max_time) =
                build_insert_sql(&ensured, origins, ingest_seq_base, points, &sids)?;
            histogram!("hyperbytedb_flush_sink_build_insert_sql_seconds")
                .record(build_insert_start.elapsed().as_secs_f64());
            system_trace::record_phase("build_batch_us", build_insert_start.elapsed());

            let insert_start = std::time::Instant::now();
            self.execute(sql).await?;
            histogram!("hyperbytedb_flush_sink_chdb_insert_seconds")
                .record(insert_start.elapsed().as_secs_f64());
            system_trace::record_phase("chdb_insert_us", insert_start.elapsed());
            (min_time, max_time)
        };

        system_trace::record_i64("min_time", min_time);
        system_trace::record_i64("max_time", max_time);
        system_trace::finish_span(&span, trace_start, "sink write complete");

        Ok(WriteAck {
            min_time,
            max_time,
            row_count,
        })
    }

    async fn write_prepared_batch(
        &self,
        db: &str,
        rp: &str,
        batch: &crate::domain::prepared_wal::PreparedMeasurementBatch,
    ) -> Result<WriteAck, HyperbytedbError> {
        if batch.row_count == 0 {
            return Ok(WriteAck {
                min_time: 0,
                max_time: 0,
                row_count: 0,
            });
        }
        system_trace::record_bool("use_arrow", self.use_arrow);
        system_trace::record_bool("prepared_path", true);
        counter!("hyperbytedb_flush_sink_writes_total", "path" => "prepared").increment(1);

        let key = TableKey {
            db: db.to_string(),
            rp: rp.to_string(),
            measurement: batch.measurement.clone(),
        };

        if let Some(ref series_batch) = batch.new_series_batch {
            let ensured = EnsuredTable {
                table: batch.table_name.clone(),
                series_table: batch.series_table_name.clone(),
                tag_phys: Vec::new(),
                field_phys: Vec::new(),
            };
            self.insert_prepared_series(&key, &ensured, series_batch)
                .await?;
        }

        if !self.use_arrow {
            return Err(HyperbytedbError::Internal(
                "prepared batch path requires Arrow inserts".into(),
            ));
        }

        // Re-align legacy sparse prepared batches (pre-coalesce WAL entries)
        // to the current metadata-driven table schema before insert.
        let ensured = self.ensure_table(&key, &[]).await?;
        let padded = pad_record_batch_to_ensured(&batch.batch, &ensured)?;

        let pool = self.session.pool()?;
        let table = batch.table_name.clone();
        let fact = Arc::new(padded);
        let insert_start = std::time::Instant::now();
        tokio::task::spawn_blocking(move || {
            let batch = (*fact).clone();
            pool.with_connection(|conn| {
                insert_record_batch_direct(conn, &table, batch, InsertOptions::default_bulk())
                    .map_err(|e| HyperbytedbError::Chdb(e.to_string()))
            })
        })
        .await
        .map_err(|e| HyperbytedbError::Internal(format!("chDB prepared insert join: {e}")))??;
        histogram!("hyperbytedb_flush_sink_chdb_insert_seconds", "path" => "prepared")
            .record(insert_start.elapsed().as_secs_f64());

        Ok(WriteAck {
            min_time: batch.min_time,
            max_time: batch.max_time,
            row_count: batch.row_count,
        })
    }

    async fn build_prepared_wal_slot(
        &self,
        db: &str,
        rp: &str,
        origin_node_id: u64,
        points: &[Point],
    ) -> Result<crate::domain::prepared_wal::PreparedWalSlot, HyperbytedbError> {
        self.build_prepared_wal_slot(db, rp, origin_node_id, points)
            .await
    }

    async fn ensure_measurement_schema(
        &self,
        db: &str,
        rp: &str,
        meta: &MeasurementMeta,
    ) -> Result<(), HyperbytedbError> {
        self.ensure_measurement_schema_impl(db, rp, meta).await
    }

    async fn refresh_schema_cache(&self) -> Result<(), HyperbytedbError> {
        self.refresh_schemas_from_metadata().await?;
        Ok(())
    }

    async fn drop_measurement(
        &self,
        db: &str,
        rp: &str,
        measurement: &str,
    ) -> Result<(), HyperbytedbError> {
        let key = TableKey {
            db: db.to_string(),
            rp: rp.to_string(),
            measurement: measurement.to_string(),
        };
        let table = quoted_table_name(db, rp, measurement);
        let series_table = quoted_series_table_name(db, rp, measurement);
        self.execute(format!("DROP TABLE IF EXISTS {table}"))
            .await?;
        self.execute(format!("DROP TABLE IF EXISTS {series_table}"))
            .await?;
        self.schemas.write().remove(&key);
        self.series_schemas.write().remove(&key);
        self.known_series.write().remove(&key);
        Ok(())
    }
}

fn run_sync(pool: &ChdbConnectionPool, sql: &str) -> Result<(), HyperbytedbError> {
    pool.with_connection(|conn| {
        execute_connection(conn, sql, OutputFormat::JSONEachRow)
            .map(|_| ())
            .map_err(|e| HyperbytedbError::Chdb(e.to_string()))
    })
}

/// Build the FACT table DDL. Tags no longer live here — every row carries a
/// single `series_id UInt64` that keys the `_series` dimension table. The sort
/// key is `(series_id, time)`.
///
/// Raw measurements use `ReplacingMergeTree(ingest_seq)`. MV / rollup destinations
/// with additive partial aggregates use `SummingMergeTree` on rollup sum columns
/// so incremental flush partials merge correctly on disk.
fn build_create_table_sql(
    table: &str,
    field_phys: &[(String, String, u8)],
    summing_columns: Option<Vec<String>>,
) -> String {
    let mut sql = String::new();
    sql.push_str("CREATE TABLE IF NOT EXISTS ");
    sql.push_str(table);
    sql.push_str(" (\n");
    sql.push_str("    `time` DateTime64(9, 'UTC') CODEC(Delta(4), ZSTD(1)),\n");
    sql.push_str("    `origin_node_id` UInt64,\n");
    sql.push_str("    `ingest_seq` UInt64,\n");
    sql.push_str("    `series_id` UInt64");
    let mut sorted_fields = field_phys.to_vec();
    sorted_fields.sort_by(|a, b| a.1.cmp(&b.1));
    for (_, phys, disc) in &sorted_fields {
        sql.push_str(",\n    ");
        sql.push_str(&quote_backticks(phys));
        sql.push(' ');
        sql.push_str(ColumnKind::Field(*disc).ch_column_type());
    }
    if let Some(cols) = summing_columns.filter(|c| !c.is_empty()) {
        let col_list = cols
            .iter()
            .map(|c| quote_backticks(c))
            .collect::<Vec<_>>()
            .join(", ");
        sql.push_str(&format!("\n) ENGINE = SummingMergeTree(({col_list}))\n"));
    } else {
        sql.push_str("\n) ENGINE = ReplacingMergeTree(`ingest_seq`)\n");
    }
    sql.push_str("PARTITION BY toDate(`time`)\n");
    sql.push_str("ORDER BY (`series_id`, `time`)");
    sql
}

fn summing_columns_from_meta(meta: &MeasurementMeta) -> Option<Vec<String>> {
    let names = crate::domain::rollup::summing_field_names(meta);
    if names.is_empty() {
        return None;
    }
    let mut cols: Vec<String> = names.iter().map(|n| field_column_name(n)).collect();
    cols.sort();
    cols.dedup();
    Some(cols)
}

/// Build the SERIES (tag dimension) table DDL: `series_id` plus one column per
/// tag key. One row per distinct series; `ReplacingMergeTree()` collapses any
/// re-inserted duplicate (same id ⇒ same tags by construction).
fn build_create_series_table_sql(
    series_table: &str,
    tag_phys: &[(String, String, ColumnKind)],
) -> String {
    let mut sql = String::new();
    sql.push_str("CREATE TABLE IF NOT EXISTS ");
    sql.push_str(series_table);
    sql.push_str(" (\n");
    sql.push_str("    `series_id` UInt64");
    let mut sorted_tags = tag_phys.to_vec();
    sorted_tags.sort_by(|a, b| a.1.cmp(&b.1));
    for (_, phys, kind) in &sorted_tags {
        sql.push_str(",\n    ");
        sql.push_str(&quote_backticks(phys));
        sql.push(' ');
        sql.push_str(kind.ch_column_type());
    }
    sql.push_str("\n) ENGINE = ReplacingMergeTree()\n");
    sql.push_str("ORDER BY (`series_id`)");
    sql
}

/// Emit `ALTER TABLE ... MODIFY COLUMN` when cached field types are narrower
/// than the metadata/batch union (e.g. Int64 → UInt64).
fn build_alter_reconcile_field_widening(
    table: &str,
    cached: &TableSchema,
    field_phys: &[(String, String, u8)],
) -> Vec<String> {
    let mut out = Vec::new();
    for (_, phys, disc) in field_phys {
        if let Some(ColumnKind::Field(cached_disc)) = cached.columns.get(phys)
            && *cached_disc != *disc
            && widen_field_disc(*cached_disc, *disc) == Some(*disc)
        {
            out.push(format!(
                "ALTER TABLE {} MODIFY COLUMN {} {}",
                table,
                quote_backticks(phys),
                ColumnKind::Field(*disc).ch_column_type()
            ));
        }
    }
    out
}

fn merge_cached_field_column(field_types: &mut HashMap<String, u8>, phys: &str, d: u8) {
    let logical = field_types
        .keys()
        .find(|k| field_column_name(k) == phys)
        .cloned()
        .unwrap_or_else(|| phys.to_string());
    match field_types.get_mut(&logical) {
        Some(existing) => {
            if let Some(w) = widen_field_disc(*existing, d) {
                *existing = w;
            }
        }
        None => {
            field_types.insert(logical, d);
        }
    }
}

fn ch_type_to_field_disc(ch_type: &str) -> Option<u8> {
    let t = ch_type
        .trim()
        .trim_start_matches("Nullable(")
        .trim_end_matches(')');
    match t {
        "Float64" => Some(0),
        "Int64" => Some(1),
        "UInt64" => Some(2),
        "String" => Some(3),
        "UInt8" => Some(4),
        _ => None,
    }
}

/// Emit `ALTER TABLE ... ADD COLUMN` for any FIELD column on the fact table not
/// already present in `cached`.
fn build_alter_add_field_columns(
    table: &str,
    cached: &TableSchema,
    field_phys: &[(String, String, u8)],
) -> Vec<String> {
    let mut out = Vec::new();
    for (_, phys, disc) in field_phys {
        if !cached.knows(phys, ColumnKind::Field(*disc)) {
            out.push(format!(
                "ALTER TABLE {} ADD COLUMN IF NOT EXISTS {} {}",
                table,
                quote_backticks(phys),
                ColumnKind::Field(*disc).ch_column_type()
            ));
        }
    }
    out
}

/// Emit `ALTER TABLE ... ADD COLUMN` (and `MODIFY` for LowCardinality→String
/// promotion) for any TAG column on the series dimension table not already
/// present in `cached`.
fn build_alter_add_series_columns(
    series_table: &str,
    cached: &TableSchema,
    tag_phys: &[(String, String, ColumnKind)],
) -> Vec<String> {
    let mut out = Vec::new();
    for (_, phys, kind) in tag_phys {
        match cached.columns.get(phys) {
            None => {
                out.push(format!(
                    "ALTER TABLE {} ADD COLUMN IF NOT EXISTS {} {}",
                    series_table,
                    quote_backticks(phys),
                    kind.ch_column_type()
                ));
            }
            Some(ColumnKind::TagLowCardinality) if *kind == ColumnKind::TagString => {
                out.push(format!(
                    "ALTER TABLE {} MODIFY COLUMN {} String",
                    series_table,
                    quote_backticks(phys)
                ));
            }
            _ => {}
        }
    }
    out
}

/// After a metadata-only cache warm, existing MergeTree tables may still use
/// `LowCardinality(String)` for tags that have since crossed
/// [`TAG_LOW_CARDINALITY_MAX`]. `MODIFY` to `String` is safe when the column
/// is already plain `String`.
fn build_alter_reconcile_tag_strings(
    table: &str,
    tag_phys: &[(String, String, ColumnKind)],
) -> Vec<String> {
    tag_phys
        .iter()
        .filter(|(_, _, kind)| *kind == ColumnKind::TagString)
        .map(|(_, phys, _)| {
            format!(
                "ALTER TABLE {} MODIFY COLUMN {} String",
                table,
                quote_backticks(phys)
            )
        })
        .collect()
}

/// Arrow logical type for a field discriminant. Mirrors
/// [`ColumnKind::ch_column_type`]; chDB stores fields as `Nullable(...)` so the
/// Arrow field is built nullable.
fn field_arrow_type(disc: u8) -> DataType {
    match disc {
        0 => DataType::Float64,
        1 => DataType::Int64,
        2 => DataType::UInt64,
        3 => DataType::Utf8,
        4 => DataType::UInt8,
        _ => DataType::Float64,
    }
}

/// Pad a prepared fact batch to include every column in `ensured`, adding NULL
/// arrays for fields absent from legacy sparse WAL entries.
fn pad_record_batch_to_ensured(
    batch: &RecordBatch,
    ensured: &EnsuredTable,
) -> Result<RecordBatch, HyperbytedbError> {
    use arrow::array::new_null_array;

    let n = batch.num_rows();
    let schema = batch.schema();
    let fixed = ["time", "origin_node_id", "ingest_seq", "series_id"];

    let mut fields: Vec<Field> = Vec::with_capacity(fixed.len() + ensured.field_phys.len());
    let mut columns: Vec<ArrayRef> = Vec::with_capacity(fields.capacity());

    for name in fixed {
        let idx = schema.index_of(name).map_err(|e| {
            HyperbytedbError::Internal(format!("prepared batch missing {name}: {e}"))
        })?;
        fields.push(schema.field(idx).clone());
        columns.push(batch.column(idx).clone());
    }

    for (_, phys, disc) in &ensured.field_phys {
        fields.push(Field::new(phys, field_arrow_type(*disc), true));
        columns.push(if let Ok(idx) = schema.index_of(phys) {
            batch.column(idx).clone()
        } else {
            new_null_array(&field_arrow_type(*disc), n)
        });
    }

    RecordBatch::try_new(Arc::new(Schema::new(fields)), columns)
        .map_err(|e| HyperbytedbError::Internal(format!("pad prepared RecordBatch: {e}")))
}

/// Arrow logical type for a tag column, aligned with [`ColumnKind::ch_column_type`].
fn tag_arrow_type(kind: ColumnKind) -> DataType {
    match kind {
        ColumnKind::TagLowCardinality => {
            DataType::Dictionary(Box::new(DataType::Int32), Box::new(DataType::Utf8))
        }
        ColumnKind::TagString => DataType::Utf8,
        ColumnKind::Field(_) => DataType::Utf8,
    }
}

/// Build a tag column array: dictionary-encoded Utf8 for low-cardinality tags.
fn build_series_tag_column(
    new_series: &[(u64, &Point)],
    logical: &str,
    kind: ColumnKind,
) -> Result<ArrayRef, HyperbytedbError> {
    let n = new_series.len();
    let values: Vec<&str> = new_series
        .iter()
        .map(|(_, p)| p.tags.get(logical).map(|s| s.as_str()).unwrap_or(""))
        .collect();

    match kind {
        ColumnKind::TagLowCardinality => {
            let mut dict_values: Vec<&str> = Vec::new();
            let mut value_to_index: HashMap<&str, i32> = HashMap::new();
            let mut keys = Vec::with_capacity(n);

            for v in &values {
                let idx = *value_to_index.entry(*v).or_insert_with(|| {
                    let i = dict_values.len() as i32;
                    dict_values.push(*v);
                    i
                });
                keys.push(idx);
            }

            let dictionary_values = StringArray::from(dict_values);
            let dict =
                DictionaryArray::try_new(Int32Array::from(keys), Arc::new(dictionary_values))
                    .map_err(|e| {
                        HyperbytedbError::Internal(format!("build dictionary tag column: {e}"))
                    })?;
            Ok(Arc::new(dict))
        }
        ColumnKind::TagString | ColumnKind::Field(_) => {
            let mut b = StringBuilder::with_capacity(n, n * 8);
            for v in values {
                b.append_value(v);
            }
            Ok(Arc::new(b.finish()))
        }
    }
}

/// Build one Arrow column for a field, nulling rows that lack the field or
/// whose value doesn't match the column's resolved type.
fn build_field_column(points: &[Point], logical: &str, disc: u8) -> ArrayRef {
    let n = points.len();
    match disc {
        1 => {
            let mut b = Int64Builder::with_capacity(n);
            for p in points {
                match p.fields.get(logical) {
                    Some(FieldValue::Integer(v)) => b.append_value(*v),
                    Some(FieldValue::UInteger(v)) if *v <= i64::MAX as u64 => {
                        b.append_value(*v as i64)
                    }
                    Some(FieldValue::Float(v)) => b.append_value(*v as i64),
                    _ => b.append_null(),
                }
            }
            Arc::new(b.finish())
        }
        2 => {
            let mut b = UInt64Builder::with_capacity(n);
            for p in points {
                match p.fields.get(logical) {
                    Some(FieldValue::UInteger(v)) => b.append_value(*v),
                    Some(FieldValue::Integer(v)) if *v >= 0 => b.append_value(*v as u64),
                    _ => b.append_null(),
                }
            }
            Arc::new(b.finish())
        }
        3 => {
            let mut b = StringBuilder::new();
            for p in points {
                match p.fields.get(logical) {
                    Some(FieldValue::String(v)) => b.append_value(v),
                    _ => b.append_null(),
                }
            }
            Arc::new(b.finish())
        }
        4 => {
            let mut b = UInt8Builder::with_capacity(n);
            for p in points {
                match p.fields.get(logical) {
                    Some(FieldValue::Boolean(v)) => b.append_value(u8::from(*v)),
                    _ => b.append_null(),
                }
            }
            Arc::new(b.finish())
        }
        // Discriminant 0 (Float) and any unknown fallback.
        _ => {
            let mut b = Float64Builder::with_capacity(n);
            for p in points {
                match p.fields.get(logical) {
                    Some(FieldValue::Float(v)) => b.append_value(*v),
                    Some(FieldValue::Integer(v)) => b.append_value(*v as f64),
                    _ => b.append_null(),
                }
            }
            Arc::new(b.finish())
        }
    }
}

/// Build an Arrow `RecordBatch` for the FACT table matching `ensured`'s physical
/// schema, with per-row `origins` and `sids` (parallel to `points`). Columns are
/// `time`, `origin_node_id`, `ingest_seq`, `series_id`, then the field columns
/// (nullable). Returns the batch plus the observed `(min_time, max_time)`.
fn build_record_batch(
    ensured: &EnsuredTable,
    origins: &[u64],
    ingest_seq_base: u64,
    points: &[Point],
    sids: &[u64],
) -> Result<(RecordBatch, i64, i64), HyperbytedbError> {
    let n = points.len();
    debug_assert_eq!(origins.len(), n, "origins must be parallel to points");
    debug_assert_eq!(sids.len(), n, "sids must be parallel to points");

    let mut min_time = i64::MAX;
    let mut max_time = i64::MIN;
    let mut times = Vec::with_capacity(n);
    let mut seqs = Vec::with_capacity(n);
    for (i, p) in points.iter().enumerate() {
        min_time = min_time.min(p.timestamp);
        max_time = max_time.max(p.timestamp);
        times.push(p.timestamp);
        seqs.push(ingest_seq_base.saturating_add(i as u64));
    }

    let mut fields: Vec<Field> = Vec::with_capacity(4 + ensured.field_phys.len());
    fields.push(Field::new(
        "time",
        DataType::Timestamp(TimeUnit::Nanosecond, Some("UTC".into())),
        false,
    ));
    fields.push(Field::new("origin_node_id", DataType::UInt64, false));
    fields.push(Field::new("ingest_seq", DataType::UInt64, false));
    fields.push(Field::new("series_id", DataType::UInt64, false));
    for (_, phys, disc) in &ensured.field_phys {
        fields.push(Field::new(phys, field_arrow_type(*disc), true));
    }

    let mut columns: Vec<ArrayRef> = Vec::with_capacity(fields.len());
    columns.push(Arc::new(
        TimestampNanosecondArray::from(times).with_timezone("UTC"),
    ));
    columns.push(Arc::new(UInt64Array::from(origins.to_vec())));
    columns.push(Arc::new(UInt64Array::from(seqs)));
    columns.push(Arc::new(UInt64Array::from(sids.to_vec())));
    for (logical, _, disc) in &ensured.field_phys {
        columns.push(build_field_column(points, logical, *disc));
    }

    let batch = RecordBatch::try_new(Arc::new(Schema::new(fields)), columns)
        .map_err(|e| HyperbytedbError::Internal(format!("build Arrow RecordBatch: {e}")))?;
    Ok((batch, min_time, max_time))
}

/// Build an Arrow `RecordBatch` for the SERIES (tag dimension) table: a
/// `series_id` column plus one Utf8 column per tag (matching the data-table
/// convention of "" for a tag absent on a given series). `new_series` carries
/// one `(series_id, representative point)` per distinct new series.
fn build_series_record_batch(
    ensured: &EnsuredTable,
    new_series: &[(u64, &Point)],
) -> Result<RecordBatch, HyperbytedbError> {
    let mut fields: Vec<Field> = Vec::with_capacity(1 + ensured.tag_phys.len());
    fields.push(Field::new("series_id", DataType::UInt64, false));
    for (_, phys, kind) in &ensured.tag_phys {
        fields.push(Field::new(phys, tag_arrow_type(*kind), false));
    }

    let mut columns: Vec<ArrayRef> = Vec::with_capacity(fields.len());
    columns.push(Arc::new(UInt64Array::from(
        new_series.iter().map(|(id, _)| *id).collect::<Vec<_>>(),
    )));
    for (logical, _, kind) in &ensured.tag_phys {
        columns.push(build_series_tag_column(new_series, logical, *kind)?);
    }

    RecordBatch::try_new(Arc::new(Schema::new(fields)), columns)
        .map_err(|e| HyperbytedbError::Internal(format!("build series Arrow RecordBatch: {e}")))
}

fn build_insert_sql(
    ensured: &EnsuredTable,
    origins: &[u64],
    ingest_seq_base: u64,
    points: &[Point],
    sids: &[u64],
) -> Result<(String, i64, i64), HyperbytedbError> {
    debug_assert_eq!(
        origins.len(),
        points.len(),
        "origins must be parallel to points"
    );
    debug_assert_eq!(sids.len(), points.len(), "sids must be parallel to points");
    let row_count_estimate = 64 + points.len() * (48 + ensured.field_phys.len() * 12);
    let mut sql = String::with_capacity(row_count_estimate);
    sql.push_str("INSERT INTO ");
    sql.push_str(&ensured.table);
    sql.push_str(" (`time`, `origin_node_id`, `ingest_seq`, `series_id`");
    for (_, phys, _) in &ensured.field_phys {
        sql.push_str(", ");
        sql.push_str(&quote_backticks(phys));
    }
    sql.push_str(") VALUES ");

    let mut min_time = i64::MAX;
    let mut max_time = i64::MIN;
    for (i, point) in points.iter().enumerate() {
        if i > 0 {
            sql.push_str(", ");
        }
        if point.timestamp < min_time {
            min_time = point.timestamp;
        }
        if point.timestamp > max_time {
            max_time = point.timestamp;
        }
        sql.push('(');
        // time
        write!(sql, "fromUnixTimestamp64Nano(toInt64({}))", point.timestamp)?;
        // origin_node_id (per-row), ingest_seq, series_id
        write!(sql, ", {}", origins[i])?;
        let seq = ingest_seq_base.saturating_add(i as u64);
        write!(sql, ", {}, {}", seq, sids[i])?;

        for (logical, _, disc) in &ensured.field_phys {
            sql.push_str(", ");
            match point.fields.get(logical) {
                Some(v) => append_field_value(&mut sql, v, *disc),
                None => sql.push_str("NULL"),
            }
        }
        sql.push(')');
    }

    if min_time == i64::MAX {
        min_time = 0;
        max_time = 0;
    }

    Ok((sql, min_time, max_time))
}

/// Legacy SQL `INSERT` for the series dimension table: `series_id` + tag columns,
/// one row per new series.
fn build_series_insert_sql(ensured: &EnsuredTable, new_series: &[(u64, &Point)]) -> String {
    let mut sql = String::with_capacity(64 + new_series.len() * (16 + ensured.tag_phys.len() * 12));
    sql.push_str("INSERT INTO ");
    sql.push_str(&ensured.series_table);
    sql.push_str(" (`series_id`");
    for (_, phys, _) in &ensured.tag_phys {
        sql.push_str(", ");
        sql.push_str(&quote_backticks(phys));
    }
    sql.push_str(") VALUES ");
    for (i, (id, point)) in new_series.iter().enumerate() {
        if i > 0 {
            sql.push_str(", ");
        }
        let _ = write!(sql, "({}", id);
        for (logical, _, _) in &ensured.tag_phys {
            sql.push_str(", ");
            match point.tags.get(logical) {
                Some(v) => append_quoted_string(&mut sql, v),
                None => sql.push_str("''"),
            }
        }
        sql.push(')');
    }
    sql
}

/// Quote a string for ClickHouse: wrap in single quotes; escape `\` and `'`.
fn append_quoted_string(out: &mut String, s: &str) {
    out.push('\'');
    for ch in s.chars() {
        match ch {
            '\\' => out.push_str("\\\\"),
            '\'' => out.push_str("\\'"),
            '\n' => out.push_str("\\n"),
            '\r' => out.push_str("\\r"),
            '\t' => out.push_str("\\t"),
            c if (c as u32) < 0x20 => {
                let _ = write!(out, "\\x{:02x}", c as u32);
            }
            c => out.push(c),
        }
    }
    out.push('\'');
}

fn append_field_value(out: &mut String, v: &FieldValue, expected_disc: u8) {
    match (v, expected_disc) {
        (FieldValue::Float(f), 0) => append_float(out, *f),
        (FieldValue::Integer(i), 1) => {
            let _ = write!(out, "{}", i);
        }
        (FieldValue::UInteger(u), 2) => {
            let _ = write!(out, "{}", u);
        }
        (FieldValue::String(s), 3) => append_quoted_string(out, s),
        (FieldValue::Boolean(b), 4) => {
            out.push_str(if *b { "1" } else { "0" });
        }
        // Type widening: an Int64 stored alongside a Float64 column
        // (we widen across the batch in `ensure_table`).
        (FieldValue::Integer(i), 0) => append_float(out, *i as f64),
        (FieldValue::Float(f), 1) => append_float(out, *f),
        // Integer ↔ unsigned widening at insert time.
        (FieldValue::Integer(i), 2) if *i >= 0 => {
            let _ = write!(out, "{}", *i as u64);
        }
        (FieldValue::UInteger(u), 1) if *u <= i64::MAX as u64 => {
            let _ = write!(out, "{}", *u as i64);
        }
        // Cross-type collisions outside what ensure_table widens are
        // surfaced as NULL rather than a hard error so an at-least-once
        // WAL replay never panics; field_type validation upstream
        // ([`crate::ports::metadata::MetadataPort::check_field_types`])
        // already rejects genuinely incompatible writes.
        _ => out.push_str("NULL"),
    }
}

fn append_float(out: &mut String, f: f64) {
    if f.is_nan() {
        out.push_str("nan");
    } else if f.is_infinite() {
        out.push_str(if f > 0.0 { "inf" } else { "-inf" });
    } else {
        let _ = write!(out, "{:?}", f);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::Float64Array;
    use std::collections::BTreeMap;

    fn make_point(ts: i64, tags: &[(&str, &str)], fields: &[(&str, FieldValue)]) -> Point {
        let mut tag_map = BTreeMap::new();
        for (k, v) in tags {
            tag_map.insert((*k).to_string(), (*v).to_string());
        }
        let mut field_map = BTreeMap::new();
        for (k, v) in fields {
            field_map.insert((*k).to_string(), v.clone());
        }
        Point {
            measurement: "m".to_string(),
            tags: tag_map,
            fields: field_map,
            timestamp: ts,
        }
    }

    #[test]
    fn tag_arrow_type_matches_column_kind() {
        assert_eq!(
            tag_arrow_type(ColumnKind::TagLowCardinality),
            DataType::Dictionary(Box::new(DataType::Int32), Box::new(DataType::Utf8))
        );
        assert_eq!(tag_arrow_type(ColumnKind::TagString), DataType::Utf8);
    }

    #[test]
    fn series_record_batch_uses_dictionary_for_low_cardinality_tags() {
        let ensured = EnsuredTable {
            table: "`db_rp_m`".to_string(),
            series_table: "`db_rp_m_series`".to_string(),
            tag_phys: vec![
                (
                    "host".to_string(),
                    "host".to_string(),
                    ColumnKind::TagLowCardinality,
                ),
                (
                    "region".to_string(),
                    "region".to_string(),
                    ColumnKind::TagString,
                ),
            ],
            field_phys: vec![],
        };
        let p1 = make_point(0, &[("host", "a"), ("region", "us")], &[]);
        let p2 = make_point(0, &[("host", "b"), ("region", "eu")], &[]);
        let new_series = vec![(1u64, &p1), (2u64, &p2)];

        let batch = build_series_record_batch(&ensured, &new_series).expect("batch");
        assert_eq!(batch.num_rows(), 2);
        assert_eq!(
            batch.schema().field(1).data_type(),
            &tag_arrow_type(ColumnKind::TagLowCardinality)
        );
        assert_eq!(
            batch.schema().field(2).data_type(),
            &tag_arrow_type(ColumnKind::TagString)
        );
        assert!(matches!(
            batch.column(1).data_type(),
            DataType::Dictionary(_, _)
        ));
        assert_eq!(batch.column(2).data_type(), &DataType::Utf8);
    }

    #[test]
    fn tag_column_kind_switches_at_100k() {
        assert_eq!(
            tag_column_kind(TAG_LOW_CARDINALITY_MAX),
            ColumnKind::TagLowCardinality
        );
        assert_eq!(
            tag_column_kind(TAG_LOW_CARDINALITY_MAX + 1),
            ColumnKind::TagString
        );
    }

    #[test]
    fn create_table_sql_uses_summing_merge_tree_for_rollups() {
        use std::collections::HashMap;

        let meta = MeasurementMeta {
            name: "server_stats_1m".to_string(),
            field_types: HashMap::from([("players".to_string(), 0), ("cpu".to_string(), 0)]),
            tag_keys: vec!["host".to_string()],
            field_rollups: HashMap::from([
                (
                    "players".to_string(),
                    crate::domain::rollup::RollupCombine::Sum,
                ),
                ("cpu".to_string(), crate::domain::rollup::RollupCombine::Sum),
            ]),
            ..Default::default()
        };
        let field_phys: Vec<(String, String, u8)> = meta
            .field_types
            .iter()
            .map(|(k, d)| (k.clone(), field_column_name(k), *d))
            .collect();
        let sql = build_create_table_sql(
            "`db_rp_server_stats_1m`",
            &field_phys,
            summing_columns_from_meta(&meta),
        );
        assert!(
            sql.contains("ENGINE = SummingMergeTree((`cpu`, `players`))"),
            "rollup dest should sum partial aggregates on disk, got: {sql}"
        );
    }

    #[test]
    fn create_table_sql_layout() {
        let fields = vec![("usage".to_string(), "usage".to_string(), 0u8)];
        let sql = build_create_table_sql("`db_rp_m`", &fields, None);
        assert!(sql.contains("CREATE TABLE IF NOT EXISTS `db_rp_m`"));
        assert!(sql.contains("`time` DateTime64(9, 'UTC')"));
        assert!(sql.contains("`origin_node_id` UInt64"));
        assert!(sql.contains("`ingest_seq` UInt64"));
        assert!(sql.contains("`series_id` UInt64"));
        // Tags no longer live on the fact table.
        assert!(!sql.contains("`host`"));
        assert!(!sql.contains("LowCardinality"));
        assert!(sql.contains("`usage` Nullable(Float64)"));
        assert!(sql.contains("ENGINE = ReplacingMergeTree(`ingest_seq`)"));
        assert!(sql.contains("PARTITION BY toDate(`time`)"));
        assert!(sql.contains("ORDER BY (`series_id`, `time`)"));
    }

    #[test]
    fn create_table_sql_always_orders_by_series_id_and_time() {
        let sql = build_create_table_sql(
            "`db_rp_m`",
            &[("v".to_string(), "v".to_string(), 0u8)],
            None,
        );
        assert!(sql.contains("ORDER BY (`series_id`, `time`)"));
    }

    #[test]
    fn create_series_table_sql_layout() {
        let tags = vec![
            (
                "host".to_string(),
                "host".to_string(),
                ColumnKind::TagLowCardinality,
            ),
            (
                "uuid".to_string(),
                "uuid".to_string(),
                ColumnKind::TagString,
            ),
        ];
        let sql = build_create_series_table_sql("`db_rp_m_series`", &tags);
        assert!(sql.contains("CREATE TABLE IF NOT EXISTS `db_rp_m_series`"));
        assert!(sql.contains("`series_id` UInt64"));
        assert!(sql.contains("`host` LowCardinality(String)"));
        assert!(sql.contains("`uuid` String"));
        assert!(sql.contains("ENGINE = ReplacingMergeTree()"));
        assert!(sql.contains("ORDER BY (`series_id`)"));
        // No time/partition on the dimension table.
        assert!(!sql.contains("PARTITION BY"));
    }

    #[test]
    fn alter_emits_only_missing_field_columns() {
        let mut cached = TableSchema::default();
        cached
            .columns
            .insert("usage".to_string(), ColumnKind::Field(0));
        let alters = build_alter_add_field_columns(
            "`db_rp_m`",
            &cached,
            &[
                ("usage".to_string(), "usage".to_string(), 0u8),
                ("count".to_string(), "count".to_string(), 1u8),
            ],
        );
        assert_eq!(alters.len(), 1);
        assert!(alters[0].contains("`count` Nullable(Int64)"));
    }

    #[test]
    fn alter_emits_only_missing_series_columns() {
        let mut cached = TableSchema::default();
        cached
            .columns
            .insert("host".to_string(), ColumnKind::TagLowCardinality);
        let alters = build_alter_add_series_columns(
            "`db_rp_m_series`",
            &cached,
            &[
                (
                    "host".to_string(),
                    "host".to_string(),
                    ColumnKind::TagLowCardinality,
                ),
                (
                    "region".to_string(),
                    "region".to_string(),
                    ColumnKind::TagLowCardinality,
                ),
            ],
        );
        assert_eq!(alters.len(), 1);
        assert!(alters[0].contains("`region` LowCardinality(String)"));
        assert!(alters[0].contains("`db_rp_m_series`"));
    }

    #[test]
    fn table_schema_from_measurement_meta_resolves_physical_columns() {
        let mut fields = HashMap::new();
        fields.insert("value".to_string(), 0u8);
        let meta = MeasurementMeta {
            name: "cpu".to_string(),
            field_types: fields,
            tag_keys: vec!["host".to_string(), "region".to_string()],
            ..Default::default()
        };
        let mut tag_kinds = HashMap::new();
        tag_kinds.insert("host".to_string(), ColumnKind::TagLowCardinality);
        tag_kinds.insert("region".to_string(), ColumnKind::TagString);
        let (fact, series) = table_schema_from_measurement_meta(&meta, &tag_kinds);
        assert!(!fact.materialized);
        assert!(!series.materialized);
        // Fields land on the fact schema, tags on the series schema.
        assert_eq!(fact.columns.get("value"), Some(&ColumnKind::Field(0)));
        assert!(!fact.columns.contains_key("host"));
        assert_eq!(
            series.columns.get("host"),
            Some(&ColumnKind::TagLowCardinality)
        );
        assert_eq!(series.columns.get("region"), Some(&ColumnKind::TagString));
    }

    #[test]
    fn alter_reconcile_widens_int_to_uint() {
        let cached = TableSchema {
            columns: HashMap::from([("uptime".to_string(), ColumnKind::Field(1))]),
            materialized: true,
        };
        let field_phys = vec![("uptime".to_string(), "uptime".to_string(), 2u8)];
        let alters = build_alter_reconcile_field_widening("`t`", &cached, &field_phys);
        assert_eq!(alters.len(), 1);
        assert!(alters[0].contains("MODIFY COLUMN"));
        assert!(alters[0].contains("UInt64"));
    }

    #[test]
    fn alter_reconcile_emits_modify_for_string_tags() {
        let alters = build_alter_reconcile_tag_strings(
            "`db_rp_m`",
            &[(
                "uuid".to_string(),
                "uuid".to_string(),
                ColumnKind::TagString,
            )],
        );
        assert_eq!(alters.len(), 1);
        assert!(alters[0].contains("MODIFY COLUMN `uuid` String"));
    }

    #[test]
    fn alter_modifies_low_cardinality_tag_to_string() {
        let mut cached = TableSchema::default();
        cached
            .columns
            .insert("uuid".to_string(), ColumnKind::TagLowCardinality);
        let alters = build_alter_add_series_columns(
            "`db_rp_m_series`",
            &cached,
            &[(
                "uuid".to_string(),
                "uuid".to_string(),
                ColumnKind::TagString,
            )],
        );
        assert_eq!(alters.len(), 1);
        assert!(alters[0].contains("MODIFY COLUMN `uuid` String"));
    }

    fn test_ensured() -> EnsuredTable {
        EnsuredTable {
            table: "`db_rp_m`".to_string(),
            series_table: "`db_rp_m_series`".to_string(),
            tag_phys: vec![(
                "host".to_string(),
                "host".to_string(),
                ColumnKind::TagLowCardinality,
            )],
            field_phys: vec![("usage".to_string(), "usage".to_string(), 0u8)],
        }
    }

    #[test]
    fn create_table_and_record_batch_field_columns_share_sort_order() {
        let field_phys = vec![
            ("usage_user".to_string(), "usage_user".to_string(), 0u8),
            ("usage_idle".to_string(), "usage_idle".to_string(), 0u8),
            ("usage_system".to_string(), "usage_system".to_string(), 0u8),
        ];
        let ddl = build_create_table_sql("`db_rp_cpu`", &field_phys, None);
        assert!(
            ddl.find("usage_idle").unwrap() < ddl.find("usage_system").unwrap()
                && ddl.find("usage_system").unwrap() < ddl.find("usage_user").unwrap(),
            "DDL must sort field columns by physical name, got: {ddl}"
        );

        let mut sorted = field_phys.clone();
        sorted.sort_by(|a, b| a.1.cmp(&b.1));
        let ensured = EnsuredTable {
            table: "`db_rp_cpu`".to_string(),
            series_table: "`db_rp_cpu_series`".to_string(),
            tag_phys: vec![],
            field_phys: sorted,
        };
        let ts = 1_780_922_276_152_000_000i64;
        let tags = &[("host", "h1")];
        let p = make_point(
            ts,
            tags,
            &[
                ("usage_idle", FieldValue::Float(95.0)),
                ("usage_user", FieldValue::Float(4.0)),
                ("usage_system", FieldValue::Float(1.0)),
            ],
        );
        let sid = series_id_for_point(&p);
        let (batch, _, _) = build_record_batch(&ensured, &[0], 1, &[p], &[sid]).unwrap();
        let schema = batch.schema();
        let idle_idx = schema.index_of("usage_idle").unwrap();
        let system_idx = schema.index_of("usage_system").unwrap();
        let user_idx = schema.index_of("usage_user").unwrap();
        assert!(idle_idx < system_idx && system_idx < user_idx);
        let idle_col = batch
            .column(idle_idx)
            .as_any()
            .downcast_ref::<Float64Array>()
            .unwrap();
        let user_col = batch
            .column(user_idx)
            .as_any()
            .downcast_ref::<Float64Array>()
            .unwrap();
        let system_col = batch
            .column(system_idx)
            .as_any()
            .downcast_ref::<Float64Array>()
            .unwrap();
        assert_eq!(idle_col.value(0), 95.0);
        assert_eq!(user_col.value(0), 4.0);
        assert_eq!(system_col.value(0), 1.0);
    }

    #[test]
    fn insert_sql_renders_values_and_tracks_time_window() {
        let ensured = test_ensured();
        let p1 = make_point(
            1_700_000_000_000_000_000,
            &[("host", "a")],
            &[("usage", FieldValue::Float(1.5))],
        );
        let p2 = make_point(
            1_700_000_000_000_000_500,
            &[("host", "b")],
            &[("usage", FieldValue::Float(2.5))],
        );
        let sids = vec![series_id_for_point(&p1), series_id_for_point(&p2)];
        let (sql, min_t, max_t) =
            build_insert_sql(&ensured, &[7, 7], 100, &[p1, p2], &sids).unwrap();
        assert!(sql.starts_with("INSERT INTO `db_rp_m`"));
        assert!(sql.contains("`time`"));
        assert!(sql.contains("`origin_node_id`"));
        assert!(sql.contains("`ingest_seq`"));
        assert!(sql.contains("`series_id`"));
        // Tags are NOT written to the fact table anymore.
        assert!(!sql.contains("'a'"));
        assert!(sql.contains("fromUnixTimestamp64Nano"));
        // origin, ingest_seq, series_id per row.
        assert!(sql.contains(&format!(", 7, 100, {}", sids[0])));
        assert!(sql.contains(&format!(", 7, 101, {}", sids[1])));
        assert_eq!(min_t, 1_700_000_000_000_000_000);
        assert_eq!(max_t, 1_700_000_000_000_000_500);
    }

    #[test]
    fn insert_sql_writes_null_for_missing_field() {
        let ensured = EnsuredTable {
            table: "`db_rp_m`".to_string(),
            series_table: "`db_rp_m_series`".to_string(),
            tag_phys: vec![],
            field_phys: vec![
                ("a".to_string(), "a".to_string(), 0u8),
                ("b".to_string(), "b".to_string(), 3u8),
            ],
        };
        let p = make_point(1, &[], &[("a", FieldValue::Float(1.0))]);
        let sids = vec![series_id_for_point(&p)];
        let (sql, _, _) = build_insert_sql(&ensured, &[0], 0, &[p], &sids).unwrap();
        assert!(sql.contains("NULL"));
    }

    #[test]
    fn series_insert_sql_renders_dimension_rows() {
        let ensured = test_ensured();
        let p1 = make_point(1, &[("host", "a")], &[("usage", FieldValue::Float(1.0))]);
        let p2 = make_point(2, &[("host", "b")], &[("usage", FieldValue::Float(2.0))]);
        let id1 = series_id_for_point(&p1);
        let id2 = series_id_for_point(&p2);
        let sql = build_series_insert_sql(&ensured, &[(id1, &p1), (id2, &p2)]);
        assert!(sql.starts_with("INSERT INTO `db_rp_m_series`"));
        assert!(sql.contains("(`series_id`, `host`)"));
        assert!(sql.contains(&format!("({}, 'a')", id1)));
        assert!(sql.contains(&format!("({}, 'b')", id2)));
    }

    #[test]
    fn quoted_string_escapes_single_quote_and_backslash() {
        let mut out = String::new();
        append_quoted_string(&mut out, "a'b\\c\nd");
        assert_eq!(out, "'a\\'b\\\\c\\nd'");
    }
}
