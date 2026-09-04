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
