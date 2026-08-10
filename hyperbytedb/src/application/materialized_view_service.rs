use std::sync::Arc;

use crate::application::ingest_metadata::register_series_from_series_table;
use crate::application::shard_routing::{ShardRoutingContext, ensure_measurement_bootstrapped};
use crate::application::sharded_mv_backfill::{
    MeasurementShardRef, ShardedMvBackfillPlan, ShardedMvBackfillPorts, scatter_mv_backfill,
};
use crate::domain::chdb_naming::{
    quoted_fact_mv_name, quoted_series_mv_name, quoted_series_table_name, quoted_table_name,
    unquoted_fact_mv_name, unquoted_series_mv_name,
};
use crate::domain::materialized_view::MaterializedViewDef;
use crate::domain::measurement::MeasurementMeta;
use crate::error::HyperbytedbError;
use crate::ports::metadata::MetadataPort;
use crate::ports::points_sink::PointsSinkPort;
use crate::ports::query::QueryPort;
use crate::timeseriesql::ast::{
    CreateMaterializedViewStatement, MeasurementName, MeasurementSource,
};
use crate::timeseriesql::to_clickhouse::{
    self, build_create_fact_materialized_view, build_create_series_materialized_view,
};

pub struct MaterializedViewService {
    metadata: Arc<dyn MetadataPort>,
    query_port: Arc<dyn QueryPort>,
    points_sink: Arc<dyn PointsSinkPort>,
    shard_routing: Option<Arc<ShardRoutingContext>>,
    is_raft_leader: Option<Arc<dyn Fn() -> bool + Send + Sync>>,
    raft_leader_addr: Option<Arc<dyn Fn() -> Option<String> + Send + Sync>>,
}

impl MaterializedViewService {
    pub fn new(
        metadata: Arc<dyn MetadataPort>,
        query_port: Arc<dyn QueryPort>,
        points_sink: Arc<dyn PointsSinkPort>,
    ) -> Self {
        Self {
            metadata,
            query_port,
            points_sink,
            shard_routing: None,
            is_raft_leader: None,
            raft_leader_addr: None,
        }
    }

    #[must_use]
    pub fn with_sharding(
        mut self,
        ctx: Arc<ShardRoutingContext>,
        is_leader: Arc<dyn Fn() -> bool + Send + Sync>,
        leader_addr: Arc<dyn Fn() -> Option<String> + Send + Sync>,
    ) -> Self {
        self.shard_routing = Some(ctx);
        self.is_raft_leader = Some(is_leader);
        self.raft_leader_addr = Some(leader_addr);
        self
    }

    pub fn shard_routing(&self) -> Option<&Arc<ShardRoutingContext>> {
        self.shard_routing.as_ref()
    }

    pub fn points_sink(&self) -> &Arc<dyn PointsSinkPort> {
        &self.points_sink
    }

    pub async fn create(
        &self,
        mv: &CreateMaterializedViewStatement,
    ) -> Result<MaterializedViewDef, HyperbytedbError> {
        if self
            .metadata
            .get_materialized_view(&mv.database, &mv.name)
            .await?
            .is_some()
        {
            return Err(HyperbytedbError::QueryParse(format!(
                "materialized view \"{}\" already exists on \"{}\"",
                mv.name, mv.database
            )));
        }

        let (source_db, source_rp_opt, source_measurement) =
            extract_source(&mv.query, &mv.database)?;
        let (dest_db, dest_rp_opt, dest_measurement) = extract_dest(&mv.query, &mv.database)?;
        let dest_rp = match dest_rp_opt {
            Some(rp) => rp,
            None => self
                .metadata
                .get_default_rp(&dest_db)
                .await
                .unwrap_or_else(|_| "autogen".to_string()),
        };
        let source_rp = match source_rp_opt {
            Some(rp) => rp,
            None => self
                .metadata
                .get_default_rp(&source_db)
                .await
                .unwrap_or_else(|_| "autogen".to_string()),
        };

        self.bootstrap_sharded_measurements(
            &source_db,
            &source_rp,
            &source_measurement,
            &dest_db,
            &dest_rp,
            &dest_measurement,
        )
        .await?;

        if mv.backfill_on_create && self.shard_routing.is_some() {
            self.run_sharded_backfill(
                mv,
                MeasurementShardRef {
                    db: &source_db,
                    rp: &source_rp,
                    measurement: &source_measurement,
                },
                MeasurementShardRef {
                    db: &dest_db,
                    rp: &dest_rp,
                    measurement: &dest_measurement,
                },
            )
            .await?;
        }

        let run_local_backfill = mv.backfill_on_create && self.shard_routing.is_none();
        let sharded_backfill_done = mv.backfill_on_create && self.shard_routing.is_some();
        // Sharded scatter backfill runs before DDL; do not drop the destination it just filled.
        let reset_destination = !sharded_backfill_done;
        self.materialize_ddl(mv, reset_destination, run_local_backfill)
            .await?;

        let def = MaterializedViewDef {
            name: mv.name.clone(),
            database: mv.database.clone(),
            query_text: mv.raw_query.clone(),
            source_db,
            source_rp,
            source_measurement,
            dest_db,
            dest_rp: dest_rp.clone(),
            dest_measurement,
            ch_fact_mv_name: unquoted_fact_mv_name(&mv.database, &dest_rp, &mv.name).to_string(),
            ch_series_mv_name: unquoted_series_mv_name(&mv.database, &dest_rp, &mv.name)
                .to_string(),
            created_at: chrono::Utc::now().to_rfc3339(),
            backfill_on_create: mv.backfill_on_create,
        };

        self.metadata
            .store_materialized_view(&mv.database, &mv.name, &def)
            .await?;

        tracing::info!(
            mv = %mv.name,
            db = %mv.database,
            source = %def.source_measurement,
            dest = %def.dest_measurement,
            backfill = mv.backfill_on_create,
            "materialized view created"
        );

        Ok(def)
    }

    pub async fn drop_mv(&self, db: &str, name: &str) -> Result<(), HyperbytedbError> {
        let def = self
            .metadata
            .get_materialized_view(db, name)
            .await?
            .ok_or_else(|| {
                HyperbytedbError::QueryParse(format!(
                    "materialized view \"{}\" not found on \"{}\"",
                    name, db
                ))
            })?;

        let fact_mv = quoted_fact_mv_name(db, &def.dest_rp, name);
        let series_mv = quoted_series_mv_name(db, &def.dest_rp, name);
        self.drop_ch_mv_objects(fact_mv.as_str(), series_mv.as_str())
            .await?;

        // Drop the destination fact + series tables to avoid orphaned tables.
        if let Err(e) = self
            .points_sink
            .drop_measurement(&def.dest_db, &def.dest_rp, &def.dest_measurement)
            .await
        {
            tracing::warn!(
                mv = %name,
                db = %db,
                dest = %def.dest_measurement,
                error = %e,
                "failed to drop MV destination tables"
            );
        }

        self.metadata.drop_materialized_view(db, name).await?;

        tracing::info!(mv = %name, db = %db, "materialized view dropped");
        Ok(())
    }

    pub async fn drop_for_source_measurement(
        &self,
        db: &str,
        measurement: &str,
    ) -> Result<(), HyperbytedbError> {
        let mvs = self.metadata.list_materialized_views(db).await?;
        for mv in mvs {
            if mv.source_db == db
                && mv.source_measurement == measurement
                && let Err(e) = self.drop_mv(db, &mv.name).await
            {
                tracing::warn!(
                    mv = %mv.name,
                    db = %db,
                    error = %e,
                    "failed to cascade-drop materialized view for dropped source measurement"
                );
            }
        }
        Ok(())
    }

    pub async fn drop_all_in_database(&self, db: &str) -> Result<(), HyperbytedbError> {
        let mvs = self.metadata.list_materialized_views(db).await?;
        for mv in mvs {
            if let Err(e) = self.drop_mv(db, &mv.name).await {
                tracing::warn!(
                    mv = %mv.name,
                    db = %db,
                    error = %e,
                    "failed to cascade-drop materialized view for dropped database"
                );
            }
        }
        Ok(())
    }

    /// Ensure ClickHouse MV objects exist for every metadata definition. Used
    /// on startup and after cluster metadata sync when definitions arrive before
    /// local DDL has run.
    pub async fn reconcile_all(&self) -> Result<usize, HyperbytedbError> {
        let mvs = self.metadata.list_all_materialized_views().await?;
        let mut reconciled = 0usize;
        for def in &mvs {
            if self.reconcile_one(def).await? {
                reconciled += 1;
            }
        }
        Ok(reconciled)
    }

    /// Ensure ClickHouse MV objects exist for a metadata definition already stored
    /// locally (cluster replication / metadata sync path).
    pub async fn reconcile_from_definition(
        &self,
        def: &MaterializedViewDef,
    ) -> Result<(), HyperbytedbError> {
        let _ = self.reconcile_one(def).await?;
        Ok(())
    }

    /// Idempotent apply of a Raft-replicated materialized view definition.
    pub async fn apply_replicated_definition(
        &self,
        definition: &MaterializedViewDef,
    ) -> Result<(), HyperbytedbError> {
        if self
            .metadata
            .get_materialized_view(&definition.database, &definition.name)
            .await?
            .is_some()
        {
            return self.reconcile_from_definition(definition).await;
        }

        let stmt = CreateMaterializedViewStatement {
            name: definition.name.clone(),
            database: definition.database.clone(),
            query: parse_mv_select(&definition.query_text)?,
            raw_query: definition.query_text.clone(),
            backfill_on_create: definition.backfill_on_create,
        };
        self.create(&stmt).await?;
        Ok(())
    }

    /// Idempotent drop of a replicated materialized view (metadata + chDB objects).
    pub async fn apply_replicated_drop(
        &self,
        db: &str,
        name: &str,
    ) -> Result<(), HyperbytedbError> {
        if self
            .metadata
            .get_materialized_view(db, name)
            .await?
            .is_some()
        {
            self.drop_mv(db, name).await
        } else {
            Ok(())
        }
    }

    /// Drop ClickHouse MV objects for a definition without touching metadata.
    pub async fn drop_ch_objects_for_def(
        &self,
        def: &MaterializedViewDef,
    ) -> Result<(), HyperbytedbError> {
        let fact_mv = quoted_fact_mv_name(&def.database, &def.dest_rp, &def.name);
        let series_mv = quoted_series_mv_name(&def.database, &def.dest_rp, &def.name);
        self.drop_ch_mv_objects(fact_mv.as_str(), series_mv.as_str())
            .await
    }

    async fn reconcile_one(&self, def: &MaterializedViewDef) -> Result<bool, HyperbytedbError> {
        let exists_sql = format!(
            "SELECT count() FROM system.tables WHERE database = 'default' AND name = '{}' FORMAT TabSeparated",
            def.ch_fact_mv_name
        );
        let count = self.query_port.execute_sql(&exists_sql).await?;
        if count.trim() == "1" {
            return Ok(false);
        }

        let stmt = CreateMaterializedViewStatement {
            name: def.name.clone(),
            database: def.database.clone(),
            query: parse_mv_select(&def.query_text)?,
            raw_query: def.query_text.clone(),
            backfill_on_create: false,
        };
        self.materialize_ddl(&stmt, false, false).await?;
        tracing::info!(
            mv = %def.name,
            db = %def.database,
            "reconciled materialized view DDL from metadata"
        );
        Ok(true)
    }

    async fn materialize_ddl(
        &self,
        mv: &CreateMaterializedViewStatement,
        reset_destination: bool,
        backfill_on_create: bool,
    ) -> Result<(), HyperbytedbError> {
        let (source_db, source_rp_opt, source_measurement) =
            extract_source(&mv.query, &mv.database)?;
        let (dest_db, dest_rp_opt, dest_measurement) = extract_dest(&mv.query, &mv.database)?;

        let source_rp = match source_rp_opt {
            Some(rp) => rp,
            None => self
                .metadata
                .get_default_rp(&source_db)
                .await
                .unwrap_or_else(|_| "autogen".to_string()),
        };

        let source_meta = self
            .metadata
            .get_measurement(&source_db, &source_rp, &source_measurement)
            .await?
            .ok_or_else(|| {
                HyperbytedbError::QueryParse(format!(
                    "source measurement \"{}\" not found in database \"{}\"",
                    source_measurement, source_db
                ))
            })?;

        let mut dest_meta = dest_measurement_meta(&mv.query, &source_meta)?;

        let dest_rp = match dest_rp_opt {
            Some(rp) => rp,
            None => self
                .metadata
                .get_default_rp(&dest_db)
                .await
                .unwrap_or_else(|_| "autogen".to_string()),
        };
        dest_meta.materialized_rp = Some(dest_rp.clone());

        if reset_destination {
            // Failed CREATE retries often leave destination tables in a non-default
            // retention policy with a stale field layout (DROP MEASUREMENT only
            // removes the default RP). Clear orphans before re-creating schema.
            if let Err(e) = self
                .points_sink
                .drop_measurement(&dest_db, &dest_rp, &dest_measurement)
                .await
            {
                tracing::warn!(
                    db = %dest_db,
                    rp = %dest_rp,
                    measurement = %dest_measurement,
                    error = %e,
                    "failed to drop stale MV destination before recreate"
                );
            }
            self.metadata
                .delete_measurement(&dest_db, &dest_rp, &dest_measurement)
                .await?;
        }

        let source_mapping =
            crate::domain::column_mapping::ColumnMapping::from_measurement_meta(&source_meta);

        let mut tag_keys: Vec<String> = source_meta.tag_keys.to_vec();
        tag_keys.sort();
        let (effective_query, _grouped_tag_keys) = if let Some(ref gb) = mv.query.group_by {
            let (expanded_gb, resolved) = gb.expand_all_tags(&tag_keys);
            let mut q = mv.query.clone();
            q.group_by = Some(expanded_gb);
            (q, resolved)
        } else {
            (mv.query.clone(), Vec::new())
        };

        let source_fact = quoted_table_name(&source_db, &source_rp, &source_measurement);
        let source_series = quoted_series_table_name(&source_db, &source_rp, &source_measurement);
        let dest_fact = quoted_table_name(&dest_db, &dest_rp, &dest_measurement);
        let dest_series = quoted_series_table_name(&dest_db, &dest_rp, &dest_measurement);

        let fact_mv_quoted = quoted_fact_mv_name(&mv.database, &dest_rp, &mv.name);
        let series_mv_quoted = quoted_series_mv_name(&mv.database, &dest_rp, &mv.name);

        let select_sql = to_clickhouse::translate_materialized_view_select(
            &effective_query,
            &source_fact,
            &source_series,
            &dest_measurement,
            &source_mapping,
        )?;

        let create_fact_mv =
            build_create_fact_materialized_view(&fact_mv_quoted, &dest_fact, &select_sql);

        let dest_field_names: std::collections::HashSet<String> =
            dest_meta.field_types.keys().cloned().collect();
        let series_select = to_clickhouse::translate_materialized_view_series_select(
            &effective_query,
            &source_series,
            &dest_measurement,
            &source_mapping,
            Some(&dest_field_names),
        )?;
        let create_series_mv =
            build_create_series_materialized_view(&series_mv_quoted, &dest_series, &series_select);

        let backfill_fact = to_clickhouse::translate_materialized_view_backfill(
            &effective_query,
            &dest_fact,
            &source_fact,
            &source_series,
            &dest_measurement,
            &source_mapping,
        )?;

        let result = async {
            self.points_sink
                .ensure_measurement_schema(&dest_db, &dest_rp, &dest_meta)
                .await?;

            self.drop_ch_mv_objects(fact_mv_quoted.as_str(), series_mv_quoted.as_str())
                .await?;

            self.query_port.execute_sql(&create_fact_mv).await?;
            self.query_port.execute_sql(&create_series_mv).await?;

            if backfill_on_create {
                self.query_port.execute_sql(&backfill_fact).await?;
            } else {
                tracing::info!(
                    mv = %mv.name,
                    db = %mv.database,
                    "skipping materialized view fact backfill (use WITH BACKFILL to enable historical data)"
                );
            }

            let backfill_series = format!("INSERT INTO {dest_series}\n{series_select}");
            self.query_port.execute_sql(&backfill_series).await?;

            self.metadata
                .register_measurement(&dest_db, &dest_rp, &dest_meta)
                .await?;

            if let Err(e) = register_series_from_series_table(
                &self.metadata,
                &self.query_port,
                &dest_db,
                &dest_rp,
                &dest_measurement,
                &dest_meta,
                dest_series.as_str(),
            )
            .await
            {
                tracing::warn!(
                    mv = %mv.name,
                    db = %mv.database,
                    dest = %dest_measurement,
                    error = %e,
                    "failed to persist MV destination series metadata"
                );
            }

            Ok::<_, HyperbytedbError>(())
        }
        .await;

        if let Err(e) = result {
            let _ = self
                .drop_ch_mv_objects(fact_mv_quoted.as_str(), series_mv_quoted.as_str())
                .await;
            let _ = self
                .points_sink
                .drop_measurement(&dest_db, &dest_rp, &dest_measurement)
                .await;
            return Err(e);
        }

        Ok(())
    }

    async fn bootstrap_sharded_measurements(
        &self,
        source_db: &str,
        source_rp: &str,
        source_measurement: &str,
        dest_db: &str,
        dest_rp: &str,
        dest_measurement: &str,
    ) -> Result<(), HyperbytedbError> {
        let Some(ctx) = self.shard_routing.as_ref() else {
            return Ok(());
        };
        let is_leader = self.is_raft_leader.as_ref().map(|f| f()).unwrap_or(true);
        let leader_addr = self.raft_leader_addr.as_ref().and_then(|f| f());

        ensure_measurement_bootstrapped(
            ctx,
            source_db,
            source_rp,
            source_measurement,
            is_leader,
            leader_addr.as_deref(),
        )
        .await?;
        ensure_measurement_bootstrapped(
            ctx,
            dest_db,
            dest_rp,
            dest_measurement,
            is_leader,
            leader_addr.as_deref(),
        )
        .await
    }

    /// Remove destination rollup rows on this node that were produced from source
    /// series in the transferred `[start, end)` range.
    pub async fn purge_dest_partials_after_source_transfer(
        &self,
        db: &str,
        rp: &str,
        source_measurement: &str,
        start: u64,
        end: u64,
    ) -> Result<(), HyperbytedbError> {
        let mvs = self.metadata.list_materialized_views(db).await?;
        for def in mvs {
            if def.source_db != db
                || def.source_rp != rp
                || def.source_measurement != source_measurement
            {
                continue;
            }

            let mv = CreateMaterializedViewStatement {
                name: def.name.clone(),
                database: def.database.clone(),
                query: parse_mv_select(&def.query_text)?,
                raw_query: def.query_text.clone(),
                backfill_on_create: false,
            };

            let source_meta = match self
                .metadata
                .get_measurement(db, rp, source_measurement)
                .await?
            {
                Some(m) => m,
                None => continue,
            };

            let source_mapping =
                crate::domain::column_mapping::ColumnMapping::from_measurement_meta(&source_meta);
            let mut tag_keys: Vec<String> = source_meta.tag_keys.to_vec();
            tag_keys.sort();
            let (effective_query, _) = if let Some(ref gb) = mv.query.group_by {
                let (expanded_gb, _) = gb.expand_all_tags(&tag_keys);
                let mut q = mv.query.clone();
                q.group_by = Some(expanded_gb);
                (q, ())
            } else {
                (mv.query.clone(), ())
            };

            let source_fact = quoted_table_name(db, rp, source_measurement);
            let source_series = quoted_series_table_name(db, rp, source_measurement);
            let dest_fact = quoted_table_name(&def.dest_db, &def.dest_rp, &def.dest_measurement);

            let select_sql = to_clickhouse::translate_materialized_view_select(
                &effective_query,
                &source_fact,
                &source_series,
                &def.dest_measurement,
                &source_mapping,
            )?;
            let keys_sql = format!("SELECT time, series_id FROM (\n{select_sql}\n)");
            let keys_sql =
                crate::application::shard_query::inject_region_series_id_predicate_with_alias(
                    keys_sql,
                    start,
                    end,
                    Some("t"),
                );
            let delete_sql =
                format!("ALTER TABLE {dest_fact} DELETE WHERE (time, series_id) IN ({keys_sql})");

            if let Err(e) = self.query_port.execute_sql(&delete_sql).await {
                tracing::warn!(
                    mv = %def.name,
                    db = %db,
                    error = %e,
                    "failed to purge stale MV destination rows after source transfer"
                );
            }
        }
        Ok(())
    }

    async fn run_sharded_backfill(
        &self,
        mv: &CreateMaterializedViewStatement,
        source: MeasurementShardRef<'_>,
        dest: MeasurementShardRef<'_>,
    ) -> Result<(), HyperbytedbError> {
        let Some(ctx) = self.shard_routing.as_ref() else {
            return Ok(());
        };

        let source_meta = self
            .metadata
            .get_measurement(source.db, source.rp, source.measurement)
            .await?
            .ok_or_else(|| {
                HyperbytedbError::QueryParse(format!(
                    "source measurement \"{}\" not found in database \"{}\"",
                    source.measurement, source.db
                ))
            })?;

        let mut dest_meta = dest_measurement_meta(&mv.query, &source_meta)?;
        dest_meta.materialized_rp = Some(dest.rp.to_string());

        self.points_sink
            .ensure_measurement_schema(dest.db, dest.rp, &dest_meta)
            .await?;
        self.metadata
            .register_measurement(dest.db, dest.rp, &dest_meta)
            .await?;

        let source_mapping =
            crate::domain::column_mapping::ColumnMapping::from_measurement_meta(&source_meta);
        let mut tag_keys: Vec<String> = source_meta.tag_keys.to_vec();
        tag_keys.sort();
        let (effective_query, _) = if let Some(ref gb) = mv.query.group_by {
            let (expanded_gb, _) = gb.expand_all_tags(&tag_keys);
            let mut q = mv.query.clone();
            q.group_by = Some(expanded_gb);
            (q, ())
        } else {
            (mv.query.clone(), ())
        };

        let source_fact = quoted_table_name(source.db, source.rp, source.measurement);
        let source_series = quoted_series_table_name(source.db, source.rp, source.measurement);
        let dest_fact = quoted_table_name(dest.db, dest.rp, dest.measurement);
        let dest_series = quoted_series_table_name(dest.db, dest.rp, dest.measurement);

        let backfill_fact = to_clickhouse::translate_materialized_view_backfill(
            &effective_query,
            &dest_fact,
            &source_fact,
            &source_series,
            dest.measurement,
            &source_mapping,
        )?;

        let dest_field_names: std::collections::HashSet<String> =
            dest_meta.field_types.keys().cloned().collect();
        let series_select = to_clickhouse::translate_materialized_view_series_select(
            &effective_query,
            &source_series,
            dest.measurement,
            &source_mapping,
            Some(&dest_field_names),
        )?;
        let series_backfill = format!("INSERT INTO {dest_series}\n{series_select}");

        scatter_mv_backfill(
            ctx,
            ShardedMvBackfillPorts {
                query_port: &self.query_port,
                points_sink: &self.points_sink,
                metadata: &self.metadata,
            },
            ShardedMvBackfillPlan {
                source,
                dest,
                fact_sql: &backfill_fact,
                series_sql: &series_backfill,
            },
        )
        .await
    }

    async fn drop_ch_mv_objects(
        &self,
        fact_mv: &str,
        series_mv: &str,
    ) -> Result<(), HyperbytedbError> {
        self.query_port
            .execute_sql(&format!("DROP VIEW IF EXISTS {fact_mv}"))
            .await?;
        self.query_port
            .execute_sql(&format!("DROP VIEW IF EXISTS {series_mv}"))
            .await?;
        Ok(())
    }
}

/// Build a [`MaterializedViewDef`] from a parsed CREATE statement (for cluster
/// replication after the leader has applied local DDL).
pub fn def_from_statement(
    mv: &CreateMaterializedViewStatement,
    source_rp: &str,
    dest_rp: &str,
) -> Result<MaterializedViewDef, HyperbytedbError> {
    let (source_db, _, source_measurement) = extract_source(&mv.query, &mv.database)?;
    let (dest_db, _, dest_measurement) = extract_dest(&mv.query, &mv.database)?;
    Ok(MaterializedViewDef {
        name: mv.name.clone(),
        database: mv.database.clone(),
        query_text: mv.raw_query.clone(),
        source_db,
        source_rp: source_rp.to_string(),
        source_measurement,
        dest_db,
        dest_rp: dest_rp.to_string(),
        dest_measurement,
        ch_fact_mv_name: unquoted_fact_mv_name(&mv.database, dest_rp, &mv.name).to_string(),
        ch_series_mv_name: unquoted_series_mv_name(&mv.database, dest_rp, &mv.name).to_string(),
        created_at: chrono::Utc::now().to_rfc3339(),
        backfill_on_create: mv.backfill_on_create,
    })
}

fn parse_mv_select(
    query_text: &str,
) -> Result<crate::timeseriesql::ast::SelectStatement, HyperbytedbError> {
    let stmts = crate::timeseriesql::parse(query_text)?;
    match stmts.into_iter().next() {
        Some(crate::timeseriesql::ast::Statement::Select(s)) => Ok(s),
        _ => Err(HyperbytedbError::QueryParse(
            "MV body must be a SELECT statement".to_string(),
        )),
    }
}

fn extract_source(
    stmt: &crate::timeseriesql::ast::SelectStatement,
    default_db: &str,
) -> Result<(String, Option<String>, String), HyperbytedbError> {
    if stmt.from.len() != 1 {
        return Err(HyperbytedbError::QueryParse(
            "materialized view requires exactly one source measurement".to_string(),
        ));
    }
    let MeasurementSource::Concrete(m) = &stmt.from[0] else {
        return Err(HyperbytedbError::QueryParse(
            "materialized view does not support subquery sources".to_string(),
        ));
    };
    let MeasurementName::Name(name) = &m.name else {
        return Err(HyperbytedbError::QueryParse(
            "materialized view does not support regex source measurements".to_string(),
        ));
    };
    let db = m.database.as_deref().unwrap_or(default_db).to_string();
    Ok((db, m.retention_policy.clone(), name.clone()))
}

fn extract_dest(
    stmt: &crate::timeseriesql::ast::SelectStatement,
    default_db: &str,
) -> Result<(String, Option<String>, String), HyperbytedbError> {
    let into = stmt.into.as_ref().ok_or_else(|| {
        HyperbytedbError::QueryParse("materialized view requires SELECT INTO".to_string())
    })?;
    let MeasurementName::Name(name) = &into.name else {
        return Err(HyperbytedbError::QueryParse(
            "materialized view does not support regex destination measurements".to_string(),
        ));
    };
    let db = into.database.as_deref().unwrap_or(default_db).to_string();
    Ok((db, into.retention_policy.clone(), name.clone()))
}

fn dest_measurement_meta(
    stmt: &crate::timeseriesql::ast::SelectStatement,
    source_meta: &MeasurementMeta,
) -> Result<MeasurementMeta, HyperbytedbError> {
    let (field_types, field_rollups, mean_fields) =
        crate::domain::rollup::field_rollups_from_mv_select(&stmt.fields)?;

    let into = stmt.into.as_ref().ok_or_else(|| {
        HyperbytedbError::QueryParse("materialized view requires SELECT INTO".to_string())
    })?;
    let MeasurementName::Name(dest_name) = &into.name else {
        return Err(HyperbytedbError::QueryParse(
            "materialized view does not support regex destination measurements".to_string(),
        ));
    };

    let mut tag_keys: Vec<String> = Vec::new();
    if let Some(ref gb) = stmt.group_by {
        let source_keys: Vec<String> = source_meta.tag_keys.to_vec();
        let (expanded_gb, resolved) = gb.expand_all_tags(&source_keys);
        let _ = expanded_gb;
        for key in resolved {
            if source_meta.tag_keys.contains(&key) {
                tag_keys.push(key);
            }
        }
        tag_keys.sort();
        tag_keys.dedup();
    }

    Ok(MeasurementMeta {
        name: dest_name.clone(),
        field_types,
        tag_keys,
        field_rollups,
        mean_fields,
        materialized: true,
        materialized_rp: None,
    })
}
