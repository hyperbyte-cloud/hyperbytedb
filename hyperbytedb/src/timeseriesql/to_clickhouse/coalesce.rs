use crate::domain::chdb_naming::QuotedTableName;
use crate::domain::column_mapping::ColumnMapping;
use crate::domain::rollup::RollupCombine;

use super::conditions::quote_phys_identifier;

pub fn build_coalesced_fact_view(fact_table: &QuotedTableName, mapping: &ColumnMapping) -> String {
    build_coalesced_fact_view_impl(fact_table, mapping, false, None)
}

/// Like [`build_coalesced_fact_view`], with `filter` applied to the fact rows
/// before they are grouped. Only predicates on `series_id` and `time` (the
/// grouping keys) belong here: a group is kept or dropped as a whole either
/// way, so filtering early returns the same rows and lets the
/// `(series_id, time)` sort key skip the rest of the table.
pub(super) fn build_coalesced_fact_view_filtered(
    fact_table: &QuotedTableName,
    mapping: &ColumnMapping,
    filter: Option<&str>,
) -> String {
    build_coalesced_fact_view_impl(fact_table, mapping, false, filter)
}

/// Like [`build_coalesced_fact_view`], but preserves `ingest_seq` / `origin_node_id` for
/// downstream aggregates (materialized view source dedup).
pub fn build_coalesced_fact_view_with_row_meta(
    fact_table: &QuotedTableName,
    mapping: &ColumnMapping,
) -> String {
    build_coalesced_fact_view_impl(fact_table, mapping, true, None)
}

pub(super) fn build_coalesced_fact_view_impl(
    fact_table: &QuotedTableName,
    mapping: &ColumnMapping,
    include_row_metadata: bool,
    filter: Option<&str>,
) -> String {
    let mut field_cols: Vec<&String> = mapping.field_names.iter().collect();
    field_cols.sort();
    let field_aggs: Vec<String> = field_cols
        .iter()
        .map(|f| {
            let q = quote_phys_identifier(f);
            let agg = match mapping.field_rollups.get(*f) {
                Some(RollupCombine::Sum) => format!("sum({q})"),
                Some(RollupCombine::Min) => format!("min({q})"),
                Some(RollupCombine::Max) => format!("max({q})"),
                Some(RollupCombine::First) => format!("argMin({q}, `time`)"),
                Some(RollupCombine::Last) | None => format!("argMax({q}, `ingest_seq`)"),
            };
            format!("{agg} AS {q}")
        })
        .collect();
    let select_fields = if field_aggs.is_empty() {
        String::new()
    } else {
        format!(", {}", field_aggs.join(", "))
    };
    let row_meta = if include_row_metadata {
        ", max(`ingest_seq`) AS `_mv_src_ingest_seq`, any(`origin_node_id`) AS `_mv_src_origin_node_id`"
    } else {
        ""
    };
    let filter = filter.map(|f| format!(" WHERE {f}")).unwrap_or_default();
    format!(
        "(SELECT `series_id`, `time`{row_meta}{select_fields} FROM {fact_table}{filter} GROUP BY `series_id`, `time`)"
    )
}
