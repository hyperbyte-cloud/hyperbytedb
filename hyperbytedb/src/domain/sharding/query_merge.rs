use std::collections::HashMap;

use crate::domain::query_result::{QueryResponse, SeriesResult};

/// Merge multiple partial query responses into one, deduplicating series by name+tags.
pub fn merge_query_results(mut parts: Vec<QueryResponse>) -> QueryResponse {
    if parts.is_empty() {
        return QueryResponse::empty(0);
    }
    if parts.len() == 1 {
        return parts.pop().unwrap();
    }

    let statement_id = parts[0].results.first().map(|r| r.statement_id).unwrap_or(0);
    let mut merged: HashMap<String, SeriesResult> = HashMap::new();

    for part in parts {
        for stmt in part.results {
            if let Some(series_list) = stmt.series {
                for series in series_list {
                    let key = format!(
                        "{}:{:?}",
                        series.name,
                        series.tags.as_ref().map(|t| {
                            let mut pairs: Vec<_> = t.iter().collect();
                            pairs.sort_by_key(|(k, _)| *k);
                            pairs
                        })
                    );
                    merged
                        .entry(key)
                        .and_modify(|existing| {
                            existing.values.extend(series.values.clone());
                        })
                        .or_insert(series);
                }
            }
        }
    }

    let mut series: Vec<SeriesResult> = merged.into_values().collect();
    series.sort_by(|a, b| a.name.cmp(&b.name));

    QueryResponse::single(statement_id, series)
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn merges_series_from_two_parts() {
        let a = QueryResponse::single(
            0,
            vec![SeriesResult {
                name: "cpu".into(),
                tags: Some(HashMap::from([("host".into(), "a".into())])),
                columns: vec!["time".into(), "value".into()],
                values: vec![vec![json!(1), json!(1.0)]],
                partial: None,
            }],
        );
        let b = QueryResponse::single(
            0,
            vec![SeriesResult {
                name: "cpu".into(),
                tags: Some(HashMap::from([("host".into(), "b".into())])),
                columns: vec!["time".into(), "value".into()],
                values: vec![vec![json!(2), json!(2.0)]],
                partial: None,
            }],
        );
        let merged = merge_query_results(vec![a, b]);
        assert_eq!(merged.results[0].series.as_ref().unwrap().len(), 2);
    }
}
