//! Shared WHERE → SQL translation for DELETE / DROP SERIES (local + replication).

use std::sync::Arc;

use crate::domain::column_mapping::ColumnMapping;
use crate::error::HyperbytedbError;
use crate::ports::metadata::MetadataPort;
use crate::timeseriesql::ast::Expr;
use crate::timeseriesql::to_clickhouse;

pub async fn build_predicate_sql(
    metadata: &Arc<dyn MetadataPort>,
    db: &str,
    rp: &str,
    measurement: &str,
    cond: &Expr,
) -> Result<String, HyperbytedbError> {
    let mapping = match metadata.get_measurement(db, rp, measurement).await? {
        Some(meta) => ColumnMapping::from_measurement_meta(&meta),
        None => {
            // Coordinators that are not region peers have no local catalog
            // entry; still translate tag predicates so sharded DELETE can fan
            // out to the owner.
            ColumnMapping::from_identifiers(
                expr_identifiers(cond).filter(|id| id != "value"),
                std::iter::once("value".to_string()),
            )
        }
    };
    let mut sql = String::new();
    to_clickhouse::translate_condition(cond, &mapping, &mut sql)?;
    Ok(sql)
}

fn expr_identifiers(expr: &Expr) -> impl Iterator<Item = String> {
    let mut ids = Vec::new();
    collect_expr_identifiers(expr, &mut ids);
    ids.into_iter()
}

pub(crate) fn collect_expr_identifiers(expr: &Expr, out: &mut Vec<String>) {
    match expr {
        Expr::Identifier(name) | Expr::FieldRef { name, .. } => out.push(name.clone()),
        Expr::Call(call) => {
            for arg in &call.args {
                collect_expr_identifiers(arg, out);
            }
        }
        Expr::BinaryExpr(bin) => {
            collect_expr_identifiers(&bin.left, out);
            collect_expr_identifiers(&bin.right, out);
        }
        Expr::UnaryExpr(_, inner) => collect_expr_identifiers(inner, out),
        _ => {}
    }
}

/// The fact-table condition that selects the rows a `DELETE` or
/// `DROP SERIES` removes, for `ALTER TABLE <fact> DELETE WHERE <it>`.
///
/// Tags live in the series table, so each comparison on a tag becomes a
/// series lookup (`series_id IN (SELECT series_id FROM <series> WHERE ..)`);
/// comparisons on `time` stay as they are. No condition selects every row.
///
/// As in InfluxDB 1.x, only tags and `time` may be used: a field is refused,
/// and so is a name the measurement does not have (it was stored and then
/// failed every later query on the measurement with "Unknown identifier").
/// A coordinator with no catalog entry for the measurement (a sharded node
/// that holds none of its regions) cannot tell fields from tags and takes
/// every name but `time` to be a tag.
///
/// # Errors
/// [`HyperbytedbError::QueryParse`] for a field or an unknown name.
pub async fn build_delete_predicate(
    metadata: &Arc<dyn MetadataPort>,
    db: &str,
    rp: &str,
    measurement: &str,
    cond: Option<&Expr>,
) -> Result<String, HyperbytedbError> {
    let Some(cond) = cond else {
        return Ok("1".to_string());
    };
    let mut ids = Vec::new();
    collect_expr_identifiers(cond, &mut ids);
    let mapping = match metadata.get_measurement(db, rp, measurement).await? {
        Some(meta) => {
            let mapping = ColumnMapping::from_measurement_meta(&meta);
            for id in &ids {
                if id == "time" || mapping.tag_keys.contains(id) {
                    continue;
                }
                return Err(HyperbytedbError::QueryParse(
                    if mapping.field_names.contains(id) {
                        format!(
                            "fields not supported in WHERE clause during deletion: \"{id}\" \
                             is a field of measurement \"{measurement}\""
                        )
                    } else {
                        format!("measurement \"{measurement}\" has no tag key \"{id}\"")
                    },
                ));
            }
            mapping
        }
        None => ColumnMapping::from_identifiers(
            ids.iter().filter(|id| *id != "time").cloned(),
            std::iter::empty(),
        ),
    };
    let series = crate::domain::chdb_naming::quoted_series_table_name(db, rp, measurement);
    let mut sql = String::new();
    delete_term(cond, &mapping, series.as_str(), &mut sql)?;
    Ok(sql)
}

fn delete_term(
    expr: &Expr,
    mapping: &ColumnMapping,
    series_table: &str,
    out: &mut String,
) -> Result<(), HyperbytedbError> {
    use crate::timeseriesql::ast::{BinaryOp, UnaryOp};
    match expr {
        Expr::BinaryExpr(b) if matches!(b.op, BinaryOp::And | BinaryOp::Or) => {
            out.push('(');
            delete_term(&b.left, mapping, series_table, out)?;
            out.push_str(if b.op == BinaryOp::And {
                " AND "
            } else {
                " OR "
            });
            delete_term(&b.right, mapping, series_table, out)?;
            out.push(')');
        }
        Expr::UnaryExpr(UnaryOp::Not, inner) => {
            out.push_str("NOT (");
            delete_term(inner, mapping, series_table, out)?;
            out.push(')');
        }
        other => {
            let mut ids = Vec::new();
            collect_expr_identifiers(other, &mut ids);
            if ids.iter().any(|id| mapping.tag_keys.contains(id)) {
                out.push_str("`series_id` IN (SELECT `series_id` FROM ");
                out.push_str(series_table);
                out.push_str(" WHERE ");
                to_clickhouse::translate_condition(other, mapping, out)?;
                out.push(')');
            } else {
                out.push('(');
                to_clickhouse::translate_condition(other, mapping, out)?;
                out.push(')');
            }
        }
    }
    Ok(())
}

/// What a node deletes its own rows with: its WAL (flushed first) and sink.
#[derive(Clone, Copy)]
pub struct LocalRows<'a> {
    pub wal: Option<&'a dyn crate::ports::wal::WalPort>,
    pub sink: &'a dyn crate::ports::points_sink::PointsSinkPort,
}

/// Delete the rows `predicate` (from [`build_delete_predicate`]) selects
/// from this node's copy of the measurement.
///
/// Rows still in the WAL are flushed first, up to the WAL's last entry when
/// the delete arrived, so a point written before the delete is removed with
/// the rest; one written after it is not touched. An empty `predicate`
/// deletes nothing.
///
/// # Errors
/// A chDB error from the delete.
pub async fn delete_rows(
    wal: Option<&dyn crate::ports::wal::WalPort>,
    sink: &dyn crate::ports::points_sink::PointsSinkPort,
    db: &str,
    rp: &str,
    measurement: &str,
    predicate: &str,
) -> Result<(), HyperbytedbError> {
    if predicate.is_empty() || !sink.deletes_rows() {
        return Ok(());
    }
    if let Some(wal) = wal {
        wait_for_flush(wal).await?;
    }
    sink.delete_rows(db, rp, measurement, predicate).await
}

/// Longest wait for the flush service to write the WAL's current entries:
/// past one flush interval (10 s by default), well inside the query
/// timeout (30 s by default).
const DELETE_FLUSH_WAIT: std::time::Duration = std::time::Duration::from_secs(15);

async fn wait_for_flush(wal: &dyn crate::ports::wal::WalPort) -> Result<(), HyperbytedbError> {
    let target = wal.last_sequence().await?;
    let deadline = std::time::Instant::now() + DELETE_FLUSH_WAIT;
    while wal.flushed_through() < target {
        if std::time::Instant::now() >= deadline {
            tracing::warn!(
                target,
                flushed_through = wal.flushed_through(),
                "delete: WAL not flushed in time; points written just before the delete may remain"
            );
            break;
        }
        tokio::time::sleep(std::time::Duration::from_millis(100)).await;
    }
    Ok(())
}
