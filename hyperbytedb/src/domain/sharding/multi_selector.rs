//! Finishing TOP, BOTTOM and SAMPLE results.
//!
//! Each region of a sharded measurement returns its own N points per group,
//! so the coordinator ranks the union again ([`rerank`]). The top N of a
//! union of per-region top-N lists is the global top N, also with tag
//! arguments: a tag's best point overall is its best point in some region,
//! and is in that region's list unless N other tags beat it there, and so
//! overall. SAMPLE picks N at random from the regions' samples.
//!
//! Every path then drops the hidden ranking columns ([`strip_hidden`]).

use std::cmp::Ordering;
use std::collections::HashMap;
use std::hash::BuildHasher;

use indexmap::IndexMap;
use serde_json::Value;

use crate::domain::query_result::SeriesResult;
use crate::timeseriesql::ast::SelectStatement;
use crate::timeseriesql::to_clickhouse::{BUCKET_COL, MultiSelector, SelectorKind, TIME_NS_COL};

/// Remove the hidden ranking columns.
#[must_use]
pub fn strip_hidden(mut series: Vec<SeriesResult>) -> Vec<SeriesResult> {
    for s in &mut series {
        let keep: Vec<usize> = (0..s.columns.len())
            .filter(|&i| s.columns[i] != BUCKET_COL && s.columns[i] != TIME_NS_COL)
            .collect();
        if keep.len() == s.columns.len() {
            continue;
        }
        s.columns = keep.iter().map(|&i| s.columns[i].clone()).collect();
        for row in &mut s.values {
            *row = keep
                .iter()
                .map(|&i| row.get(i).cloned().unwrap_or(Value::Null))
                .collect();
        }
    }
    series
}

/// Rank the regions' rows again: at most `sel.n` per bucket and series, in
/// time order, then the statement's LIMIT and OFFSET per series. The hidden
/// columns are dropped.
#[must_use]
pub fn rerank(
    sel: &MultiSelector,
    stmt: &SelectStatement,
    series: Vec<SeriesResult>,
) -> Vec<SeriesResult> {
    // The same series from several regions becomes one.
    let mut merged: IndexMap<(String, Vec<(String, String)>), SeriesResult> = IndexMap::new();
    for s in series {
        let mut key_tags: Vec<(String, String)> = s
            .tags
            .as_ref()
            .map(|t| t.iter().map(|(k, v)| (k.clone(), v.clone())).collect())
            .unwrap_or_default();
        key_tags.sort();
        match merged.entry((s.name.clone(), key_tags)) {
            indexmap::map::Entry::Vacant(e) => {
                e.insert(s);
            }
            indexmap::map::Entry::Occupied(mut e) => {
                let into = e.get_mut();
                let at: Vec<Option<usize>> = into
                    .columns
                    .iter()
                    .map(|c| s.columns.iter().position(|x| x == c))
                    .collect();
                for row in s.values {
                    into.values.push(
                        at.iter()
                            .map(|i| i.and_then(|i| row.get(i).cloned()).unwrap_or(Value::Null))
                            .collect(),
                    );
                }
            }
        }
    }

    let desc = stmt.order_by.as_ref().is_some_and(|o| o.time_desc);
    let random = std::collections::hash_map::RandomState::new();
    let mut out = Vec::with_capacity(merged.len());
    for (_, mut s) in merged {
        let col = |name: &str| s.columns.iter().position(|c| c == name);
        let (Some(bucket), Some(tns), Some(value)) =
            (col(BUCKET_COL), col(TIME_NS_COL), col(&sel.output))
        else {
            out.push(s);
            continue;
        };
        let tag_cols: Vec<usize> = sel.tags.iter().filter_map(|t| col(t)).collect();
        let num = |row: &[Value], i: usize| row.get(i).and_then(Value::as_f64);
        let int = |row: &[Value], i: usize| row.get(i).and_then(Value::as_i64).unwrap_or(0);

        let mut groups: IndexMap<i64, Vec<Vec<Value>>> = IndexMap::new();
        for row in std::mem::take(&mut s.values) {
            groups.entry(int(&row, bucket)).or_default().push(row);
        }
        let mut kept = Vec::new();
        for (_, mut rows) in groups {
            match sel.kind {
                SelectorKind::Top | SelectorKind::Bottom => {
                    rows.sort_by(|a, b| {
                        let by_value = match (num(a, value), num(b, value)) {
                            (Some(x), Some(y)) => x.total_cmp(&y),
                            (Some(_), None) => Ordering::Greater,
                            (None, Some(_)) => Ordering::Less,
                            (None, None) => Ordering::Equal,
                        };
                        let by_value = if sel.kind == SelectorKind::Top {
                            by_value.reverse()
                        } else {
                            by_value
                        };
                        by_value.then_with(|| int(a, tns).cmp(&int(b, tns)))
                    });
                    if !tag_cols.is_empty() {
                        let mut seen = HashMap::new();
                        rows.retain(|r| {
                            let key: Vec<Value> = tag_cols
                                .iter()
                                .map(|&i| r.get(i).cloned().unwrap_or(Value::Null))
                                .collect();
                            seen.insert(serde_json::to_string(&key).unwrap_or_default(), ())
                                .is_none()
                        });
                    }
                }
                SelectorKind::Sample => {
                    rows.sort_by_key(|r| random.hash_one(int(r, tns)));
                }
            }
            rows.truncate(usize::try_from(sel.n).unwrap_or(usize::MAX));
            kept.extend(rows);
        }
        kept.sort_by(|a, b| {
            let o = int(a, tns).cmp(&int(b, tns));
            if desc { o.reverse() } else { o }
        });
        let offset = usize::try_from(stmt.offset.unwrap_or(0)).unwrap_or(usize::MAX);
        let limit = stmt
            .limit
            .map_or(usize::MAX, |l| usize::try_from(l).unwrap_or(usize::MAX));
        s.values = kept.into_iter().skip(offset).take(limit).collect();
        if !s.values.is_empty() {
            out.push(s);
        }
    }
    strip_hidden(out)
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn part(tags: Option<&str>, rows: &[(i64, i64, f64, &str)]) -> SeriesResult {
        SeriesResult {
            name: "cpu".into(),
            tags: tags.map(|h| HashMap::from([("dc".to_string(), h.to_string())])),
            columns: vec![
                "time".into(),
                TIME_NS_COL.into(),
                BUCKET_COL.into(),
                "top".into(),
                "host".into(),
            ],
            values: rows
                .iter()
                .map(|(t, b, v, h)| vec![json!(t), json!(t), json!(b), json!(v), json!(h)])
                .collect(),
            partial: None,
        }
    }

    fn sel(kind: SelectorKind, tags: &[&str], n: u64) -> MultiSelector {
        MultiSelector {
            kind,
            output: "top".into(),
            field: "v".into(),
            tags: tags.iter().map(|t| t.to_string()).collect(),
            n,
        }
    }

    fn stmt() -> SelectStatement {
        match crate::timeseriesql::parser::parse_query("SELECT top(v, 2) FROM cpu")
            .unwrap()
            .pop()
        {
            Some(crate::timeseriesql::ast::Statement::Select(s)) => s,
            other => panic!("{other:?}"),
        }
    }

    /// Two regions' top 2 per bucket: the coordinator keeps the global top 2
    /// per bucket, earliest first on ties, in time order, and drops the
    /// hidden columns.
    #[test]
    fn keeps_the_global_top_n_per_bucket() {
        let r1 = part(
            None,
            &[(1, 0, 5.0, "a"), (2, 0, 9.0, "a"), (11, 10, 1.0, "a")],
        );
        let r2 = part(
            None,
            &[(3, 0, 9.0, "b"), (4, 0, 7.0, "b"), (12, 10, 3.0, "b")],
        );
        let out = rerank(&sel(SelectorKind::Top, &[], 2), &stmt(), vec![r1, r2]);
        assert_eq!(out.len(), 1);
        assert_eq!(out[0].columns, ["time", "top", "host"]);
        let got: Vec<(i64, f64)> = out[0]
            .values
            .iter()
            .map(|r| (r[0].as_i64().unwrap(), r[1].as_f64().unwrap()))
            .collect();
        assert_eq!(got, [(2, 9.0), (3, 9.0), (11, 1.0), (12, 3.0)]);
    }

    /// With a tag argument at most one point per tag value: host a's best
    /// (9) and host b's best (8), not a's two best.
    #[test]
    fn a_tag_argument_keeps_one_point_per_value() {
        let r1 = part(None, &[(1, 0, 9.0, "a"), (2, 0, 8.5, "a")]);
        let r2 = part(None, &[(3, 0, 8.0, "b"), (4, 0, 1.0, "c")]);
        let out = rerank(&sel(SelectorKind::Top, &["host"], 2), &stmt(), vec![r1, r2]);
        let hosts: Vec<&str> = out[0]
            .values
            .iter()
            .map(|r| r[2].as_str().unwrap())
            .collect();
        assert_eq!(hosts, ["a", "b"]);
    }

    #[test]
    fn bottom_keeps_the_smallest_and_series_stay_apart() {
        let east = part(Some("east"), &[(1, 0, 5.0, "a"), (2, 0, 1.0, "a")]);
        let west = part(Some("west"), &[(3, 0, 4.0, "b"), (4, 0, 2.0, "b")]);
        let east2 = part(Some("east"), &[(5, 0, 3.0, "c")]);
        let out = rerank(
            &sel(SelectorKind::Bottom, &[], 1),
            &stmt(),
            vec![east, west, east2],
        );
        assert_eq!(out.len(), 2);
        assert_eq!(out[0].values[0][1], json!(1.0));
        assert_eq!(out[1].values[0][1], json!(2.0));
    }

    #[test]
    fn sample_keeps_n_per_bucket() {
        let r1 = part(
            None,
            &[(1, 0, 1.0, "a"), (2, 0, 2.0, "a"), (3, 0, 3.0, "a")],
        );
        let r2 = part(None, &[(4, 0, 4.0, "b"), (11, 10, 5.0, "b")]);
        let out = rerank(&sel(SelectorKind::Sample, &[], 2), &stmt(), vec![r1, r2]);
        let buckets: Vec<i64> = out[0]
            .values
            .iter()
            .map(|r| r[0].as_i64().unwrap())
            .collect();
        assert_eq!(buckets.len(), 3, "two from bucket 0, one from bucket 10");
        assert!(buckets.windows(2).all(|w| w[0] < w[1]), "time order");
    }
}
