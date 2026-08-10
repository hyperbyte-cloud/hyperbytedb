//! Shard-aware SQL helpers (region series_id predicates).

/// Inject `series_id` range filter `[start, end)` into translated ClickHouse SQL.
pub fn inject_region_series_id_predicate(sql: String, start: u64, end: u64) -> String {
    inject_region_series_id_predicate_with_alias(sql, start, end, None)
}

/// Like [`inject_region_series_id_predicate`], but qualify the fact-table column
/// (for example `t.`series_id`` in materialized-view SELECT bodies).
pub fn inject_region_series_id_predicate_with_alias(
    sql: String,
    start: u64,
    end: u64,
    fact_alias: Option<&str>,
) -> String {
    if start == 0 && end == u64::MAX {
        return sql;
    }
    let series_col = match fact_alias {
        Some(alias) => format!("{alias}.`series_id`"),
        None => "`series_id`".to_string(),
    };
    let predicate = if end == u64::MAX {
        format!("{series_col} >= {start}")
    } else {
        format!("{series_col} >= {start} AND {series_col} < {end}")
    };
    inject_and_predicate(sql, &predicate)
}

fn inject_and_predicate(sql: String, predicate: &str) -> String {
    if predicate.is_empty() {
        return sql;
    }
    if let Some(inner_and_tail) = sql.strip_prefix("SELECT * FROM (\n")
        && let Some(end) = inner_and_tail.rfind("\n) ")
    {
        let inner = inner_and_tail[..end].to_string();
        let tail = &inner_and_tail[end..];
        let injected = inject_and_predicate(inner, predicate);
        return format!("SELECT * FROM (\n{injected}{tail}");
    }
    let clause = format!(" AND ({predicate})");
    if sql.contains("\nWHERE ") {
        if let Some(where_end) = sql
            .find("\nGROUP BY")
            .or_else(|| sql.find("\nORDER BY"))
            .or_else(|| sql.find("\nLIMIT"))
        {
            let mut result = sql;
            result.insert_str(where_end, &clause);
            result
        } else {
            let mut result = sql;
            result.push_str(&clause);
            result
        }
    } else {
        let from_end = sql
            .find("\nGROUP BY")
            .or_else(|| sql.find("\nORDER BY"))
            .or_else(|| sql.find("\nLIMIT"))
            .unwrap_or(sql.len());
        let mut result = sql;
        result.insert_str(from_end, &format!("\nWHERE ({predicate})"));
        result
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn injects_series_id_range() {
        let sql = "SELECT a FROM t\nWHERE x = 1\nGROUP BY a".to_string();
        let out = inject_region_series_id_predicate(sql, 10, 100);
        assert!(out.contains("series_id` >= 10"));
        assert!(out.contains("series_id` < 100"));
    }

    #[test]
    fn injects_series_id_range_with_fact_alias() {
        let sql = "SELECT time FROM src AS t\nGROUP BY time".to_string();
        let out = inject_region_series_id_predicate_with_alias(sql, 10, 100, Some("t"));
        assert!(out.contains("t.`series_id` >= 10"));
        assert!(out.contains("t.`series_id` < 100"));
    }
}
