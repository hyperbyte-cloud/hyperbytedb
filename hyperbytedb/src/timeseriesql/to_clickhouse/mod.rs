use crate::domain::chdb_naming::QuotedTableName;
use crate::error::HyperbytedbError;
use crate::timeseriesql::ast::*;

mod aggregates;
mod coalesce;
mod conditions;
mod materialized_view;
mod rename;
mod select;
mod time_bounds;

#[cfg(test)]
mod tests;

pub use time_bounds::extract_time_bounds;
pub use rename::rename_time_bucket_alias;

pub use coalesce::{build_coalesced_fact_view, build_coalesced_fact_view_with_row_meta};
pub use conditions::translate_condition;
pub use materialized_view::{
    build_create_fact_materialized_view, build_create_series_materialized_view,
    cq_time_window_condition, prepare_cq_select, strip_time_predicates, translate_bounded_cq_into,
    translate_materialized_view_backfill, translate_materialized_view_select,
    translate_materialized_view_series_select, translate_select_into, translate_select_into_native,
};
pub use select::{
    select_has_true_aggregate, select_output_field_name, translate_native_table, translate_with_source,
};

/// `SELECT ... INTO` requires `GROUP BY time(<interval>)` so results are bucketed
/// before writing to the destination measurement.
pub fn validate_select_into(stmt: &SelectStatement) -> Result<(), HyperbytedbError> {
    if stmt.into.is_none() {
        return Ok(());
    }
    let Some(gb) = stmt.group_by.as_ref() else {
        return Err(HyperbytedbError::QueryParse(
            "SELECT INTO requires GROUP BY time(<interval>)".to_string(),
        ));
    };
    if gb.time_dimension().is_none() {
        return Err(HyperbytedbError::QueryParse(
            "SELECT INTO requires GROUP BY time(<interval>)".to_string(),
        ));
    }
    Ok(())
}

/// The per-measurement series (tag dimension) table to join for tag resolution.
/// In the `series_id` layout the fact table no longer stores tag columns; when a
/// query references a tag we re-attach the tag columns from this table.
#[derive(Debug, Clone, Copy)]
pub struct SeriesJoin<'a> {
    /// Backtick-quoted `<db>_<rp>_<measurement>_series` table name.
    pub table: &'a QuotedTableName,
    /// Force the inline tag-rejoin view even when the query body references no
    /// tag. Set when tombstone predicates (spliced into WHERE post-translation)
    /// reference tag columns that must be present in the FROM source.
    pub force: bool,
    /// Physical column names that actually exist in the series table.
    /// When empty, all tags from the ColumnMapping are projected (backward-compat).
    pub tag_columns: &'a [String],
}
