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

/// Clause keywords that terminate the projection/WHERE region of the generated
/// SQL; an injected range predicate must land before any of them.
const CLAUSE_TERMINATORS: [&str; 4] = ["\nGROUP BY", "\nORDER BY", "\nLIMIT", "\nOFFSET"];

fn find_clause_terminator(sql: &str) -> Option<usize> {
    CLAUSE_TERMINATORS
        .iter()
        .filter_map(|marker| sql.find(marker))
        .min()
}

fn inject_and_predicate(sql: String, predicate: &str) -> String {
    if predicate.is_empty() {
        return sql;
    }
    // Subquery-wrapped statements get the predicate pushed into the innermost
    // SELECT so it filters rows before aggregation. Both LF and CRLF shapes
    // are recognized.
    for wrapper_open in ["SELECT * FROM (\n", "SELECT * FROM (\r\n"] {
        if let Some(inner_and_tail) = sql.strip_prefix(wrapper_open)
            && let Some(end) = inner_and_tail
                .rfind("\n) ")
                .or_else(|| inner_and_tail.rfind("\r\n) "))
        {
            let inner = inner_and_tail[..end].to_string();
            let tail = &inner_and_tail[end..];
            let injected = inject_and_predicate(inner, predicate);
            return format!(
                "SELECT * FROM ({open}{injected}{tail}",
                open = if wrapper_open.ends_with("\r\n") {
                    "\r\n"
                } else {
                    "\n"
                }
            );
        }
    }
    let clause = format!(" AND ({predicate})");
    if sql.contains("\nWHERE ") || sql.contains("\r\nWHERE ") {
        if let Some(where_end) = find_clause_terminator(&sql) {
            let mut result = sql;
            result.insert_str(where_end, &clause);
            result
        } else {
            let mut result = sql;
            result.push_str(&clause);
            result
        }
    } else {
        let from_end = find_clause_terminator(&sql).unwrap_or(sql.len());
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

    #[test]
    fn injects_before_offset_clause() {
        let sql = "SELECT value FROM t\nWHERE x = 1\nLIMIT 5\nOFFSET 10".to_string();
        let out = inject_region_series_id_predicate(sql, 10, 100);
        let and_pos = out.find("AND (`series_id`").expect("predicate injected");
        let offset_pos = out.find("\nOFFSET").expect("OFFSET preserved");
        assert!(and_pos < offset_pos, "predicate must precede OFFSET: {out}");
    }

    #[test]
    fn injects_into_crlf_subquery_wrapper() {
        let sql = "SELECT * FROM (\r\nSELECT value FROM t\r\n) AS inner_q\nLIMIT 3".to_string();
        let out = inject_region_series_id_predicate(sql, 10, 100);
        assert!(
            out.contains("`series_id` < 100"),
            "CRLF-wrapped subquery must receive the predicate: {out}"
        );
        // The predicate belongs inside the wrapper, before the closing paren.
        let close = out.find(") AS inner_q").expect("wrapper preserved");
        let pred = out.find("`series_id` < 100").unwrap();
        assert!(pred < close);
    }
}
