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
    let mapping = metadata
        .get_measurement(db, rp, measurement)
        .await?
        .map(|m| ColumnMapping::from_measurement_meta(&m));
    let mut sql = String::new();
    to_clickhouse::translate_condition_with_mapping(cond, mapping.as_ref(), &mut sql)?;
    Ok(sql)
}
