use std::sync::Arc;

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
        }
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

        self.materialize_ddl(mv, true).await?;

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
            ch_fact_mv_name: unquoted_fact_mv_name(&mv.database, &dest_rp, &mv.name),
            ch_series_mv_name: unquoted_series_mv_name(&mv.database, &dest_rp, &mv.name),
            created_at: chrono::Utc::now().to_rfc3339(),
        };

        self.metadata
            .store_materialized_view(&mv.database, &mv.name, &def)
            .await?;

        tracing::info!(
            mv = %mv.name,
            db = %mv.database,
            source = %def.source_measurement,
            dest = %def.dest_measurement,
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
        self.drop_ch_mv_objects(&fact_mv, &series_mv).await?;

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
        self.drop_ch_mv_objects(&fact_mv, &series_mv).await
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
        };
        self.materialize_ddl(&stmt, false).await?;
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
    ) -> Result<(), HyperbytedbError> {
        let (source_db, source_rp, source_measurement) = extract_source(&mv.query, &mv.database)?;
        let (dest_db, dest_rp, dest_measurement) = extract_dest(&mv.query, &mv.database)?;

        let source_meta = self
            .metadata
            .get_measurement(&source_db, &source_measurement)
            .await?
            .ok_or_else(|| {
                HyperbytedbError::QueryParse(format!(
                    "source measurement \"{}\" not found in database \"{}\"",
                    source_measurement, source_db
                ))
            })?;

        let dest_meta = dest_measurement_meta(&mv.query, &source_meta)?;

        let dest_rp = match dest_rp {
            Some(rp) => rp,
            None => self
                .metadata
                .get_default_rp(&dest_db)
                .await
                .unwrap_or_else(|_| "autogen".to_string()),
        };
        let source_rp = match source_rp {
            Some(rp) => rp,
            None => self
                .metadata
                .get_default_rp(&source_db)
                .await
                .unwrap_or_else(|_| "autogen".to_string()),
        };

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
                .delete_measurement(&dest_db, &dest_measurement)
                .await?;
        }

        self.metadata
            .register_measurement(&dest_db, &dest_meta)
            .await?;

        self.points_sink
            .ensure_measurement_schema(&dest_db, &dest_rp, &dest_meta)
            .await?;

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

        // A prior failed CREATE may have installed ClickHouse MV objects without
        // recording metadata. Drop orphans so retries succeed.
        self.drop_ch_mv_objects(&fact_mv_quoted, &series_mv_quoted)
            .await?;

        let select_sql = to_clickhouse::translate_materialized_view_select(
            &effective_query,
            &source_fact,
            &source_series,
            &dest_measurement,
            &source_mapping,
        )?;

        let create_fact_mv =
            build_create_fact_materialized_view(&fact_mv_quoted, &dest_fact, &select_sql);
        self.query_port.execute_sql(&create_fact_mv).await?;

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
        self.query_port.execute_sql(&create_series_mv).await?;

        let backfill_fact = to_clickhouse::translate_materialized_view_backfill(
            &effective_query,
            &dest_fact,
            &source_fact,
            &source_series,
            &dest_measurement,
            &source_mapping,
        )?;
        self.query_port.execute_sql(&backfill_fact).await?;

        let backfill_series = format!("INSERT INTO {dest_series}\n{series_select}");
        self.query_port.execute_sql(&backfill_series).await?;
        Ok(())
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
        ch_fact_mv_name: unquoted_fact_mv_name(&mv.database, dest_rp, &mv.name),
        ch_series_mv_name: unquoted_series_mv_name(&mv.database, dest_rp, &mv.name),
        created_at: chrono::Utc::now().to_rfc3339(),
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
    })
}
