//! Region selection for sharded SELECT queries (narrow fan-out when WHERE fixes series_id).

use std::collections::{BTreeMap, BTreeSet};

use crate::domain::series::series_id;
use crate::domain::sharding::{MeasurementShardSpace, ShardRegion};
use crate::timeseriesql::ast::{BinaryOp, Expr, SelectStatement};

/// Maximum distinct series_id routings before falling back to all regions.
pub const MAX_SERIES_IDS_FOR_ROUTING: usize = 10_000;

/// Which shard regions a coordinator should query.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum RegionSelection<'a> {
    All,
    Single(&'a ShardRegion),
    Subset(Vec<&'a ShardRegion>),
}

impl<'a> RegionSelection<'a> {
    #[must_use]
    pub fn region_count(&self, total: usize) -> usize {
        match self {
            RegionSelection::All => total,
            RegionSelection::Single(_) => 1,
            RegionSelection::Subset(v) => v.len(),
        }
    }

    #[must_use]
    pub fn regions<'b>(&'b self, space: &'b MeasurementShardSpace) -> Vec<&'b ShardRegion> {
        match self {
            RegionSelection::All => space.regions.iter().collect(),
            RegionSelection::Single(r) => vec![*r],
            RegionSelection::Subset(rs) => rs.clone(),
        }
    }
}

/// Choose regions for a sharded measurement query.
///
/// Pruning by `series_id` is only sound when the WHERE clause fixes every tag
/// key on the measurement (not a subset, and not a field column).
/// `measurement_tag_keys` is `None` when the coordinator does not know them
/// (no local metadata or series table); every region is queried then. An
/// empty list means the measurement truly has no tags.
#[must_use]
pub fn select_regions_for_query<'a>(
    space: &'a MeasurementShardSpace,
    measurement: &str,
    measurement_tag_keys: Option<&[String]>,
    stmt: &SelectStatement,
) -> RegionSelection<'a> {
    let Some(measurement_tag_keys) = measurement_tag_keys else {
        return RegionSelection::All;
    };
    let Some(series_ids) =
        routable_series_ids(measurement, measurement_tag_keys, stmt.condition.as_ref())
    else {
        return RegionSelection::All;
    };
    if series_ids.is_empty() {
        return RegionSelection::All;
    }
    if series_ids.len() > MAX_SERIES_IDS_FOR_ROUTING {
        return RegionSelection::All;
    }

    let mut regions: Vec<&ShardRegion> = Vec::new();
    let mut seen = BTreeSet::new();
    for sid in series_ids {
        let Some(region) = space.locate(sid) else {
            return RegionSelection::All;
        };
        if seen.insert(region.region_id) {
            regions.push(region);
        }
    }

    match regions.len() {
        0 => RegionSelection::All,
        1 => RegionSelection::Single(regions[0]),
        _ => RegionSelection::Subset(regions),
    }
}

/// Materialized view destinations store region-local partial rollups. Coordinators
/// must query every region and merge results.
#[must_use]
pub fn select_regions_for_materialized_dest<'a>(
    _space: &'a MeasurementShardSpace,
) -> RegionSelection<'a> {
    RegionSelection::All
}

/// Extract routable series_id values from a WHERE clause, if the predicate is exact enough.
///
/// The clause must be an AND of time bounds and, for every tag key, one set
/// of exact values: `tag = 'v'`, an OR of those, or an anchored literal
/// regex `tag =~ /^(a|b|c)$/` (how Grafana sends a multi-value variable).
/// Before regexes and ORs next to a time bound were routed, a 10-host
/// `GROUP BY host` went to every region and took seconds on n3, against
/// ~70 ms for one host.
fn routable_series_ids(
    measurement: &str,
    measurement_tag_keys: &[String],
    condition: Option<&Expr>,
) -> Option<Vec<u64>> {
    let mut sets = BTreeMap::new();
    if !collect_tag_value_sets(condition?, &mut sets) {
        return None;
    }
    let mut want: Vec<&String> = measurement_tag_keys.iter().collect();
    want.sort();
    if !want.iter().copied().eq(sets.keys()) {
        return None;
    }
    let mut combos = vec![BTreeMap::new()];
    for (tag, values) in &sets {
        if combos.len().saturating_mul(values.len()) > MAX_SERIES_IDS_FOR_ROUTING {
            return None;
        }
        combos = combos
            .iter()
            .flat_map(|c| {
                values.iter().map(move |v| {
                    let mut c = c.clone();
                    c.insert(tag.clone(), v.clone());
                    c
                })
            })
            .collect();
    }
    Some(combos.iter().map(|t| series_id(measurement, t)).collect())
}

/// AND terms: time bounds are skipped, and each other term must be an exact
/// value set for a tag not already constrained.
fn collect_tag_value_sets(expr: &Expr, sets: &mut BTreeMap<String, Vec<String>>) -> bool {
    match expr {
        Expr::BinaryExpr(be) => match be.op {
            BinaryOp::And => {
                collect_tag_value_sets(&be.left, sets) && collect_tag_value_sets(&be.right, sets)
            }
            BinaryOp::Gt | BinaryOp::Gte | BinaryOp::Lt | BinaryOp::Lte => {
                is_time_bound_expr(&be.left) || is_time_bound_expr(&be.right)
            }
            BinaryOp::Eq
                if parse_tag_identifier_eq(&be.left, &be.right)
                    .is_some_and(|(tag, _)| is_time_identifier(&tag)) =>
            {
                true
            }
            _ => match tag_value_set(expr) {
                Some((tag, values)) => sets.insert(tag, values).is_none(),
                None => false,
            },
        },
        _ => false,
    }
}

/// `tag = 'v'`, an OR of those on one tag, or `tag =~ /^(a|b)$/`.
fn tag_value_set(expr: &Expr) -> Option<(String, Vec<String>)> {
    let Expr::BinaryExpr(be) = expr else {
        return None;
    };
    match be.op {
        BinaryOp::Eq => {
            let (tag, value) = parse_tag_identifier_eq(&be.left, &be.right)?;
            (!is_time_identifier(&tag)).then(|| (tag, vec![value]))
        }
        BinaryOp::Or => {
            let (tag, mut values) = tag_value_set(&be.left)?;
            let (other, more) = tag_value_set(&be.right)?;
            (tag == other).then(|| {
                values.extend(more);
                (tag, values)
            })
        }
        BinaryOp::RegexMatch => match (&be.left, &be.right) {
            (Expr::Identifier(tag), Expr::Regex(pattern)) if !is_time_identifier(tag) => {
                Some((tag.clone(), regex_literal_alternatives(pattern)?))
            }
            _ => None,
        },
        _ => None,
    }
}

/// The values an anchored regex of literal alternatives matches exactly:
/// `^a$`, `^(a|b)$` or `^(?:a|b)$`, where a backslash escapes punctuation.
/// None for anything else, which then goes to every region.
fn regex_literal_alternatives(pattern: &str) -> Option<Vec<String>> {
    let body = pattern.strip_prefix('^')?.strip_suffix('$')?;
    let (body, grouped) = match body.strip_prefix("(?:").or_else(|| body.strip_prefix('(')) {
        Some(inner) => (inner.strip_suffix(')')?, true),
        None => (body, false),
    };
    let mut values = vec![String::new()];
    let mut chars = body.chars();
    while let Some(c) = chars.next() {
        match c {
            '\\' => match chars.next() {
                Some(e) if e.is_ascii_punctuation() => values.last_mut()?.push(e),
                _ => return None,
            },
            // Ungrouped, `^a|b$` is `(^a)|(b$)`.
            '|' if grouped => values.push(String::new()),
            '.' | '+' | '*' | '?' | '(' | ')' | '[' | ']' | '{' | '}' | '^' | '$' | '|' => {
                return None;
            }
            c => values.last_mut()?.push(c),
        }
    }
    (!values.iter().any(String::is_empty)).then_some(values)
}

fn parse_tag_identifier_eq(left: &Expr, right: &Expr) -> Option<(String, String)> {
    match (left, right) {
        (Expr::Identifier(tag), Expr::StringLiteral(val))
        | (Expr::StringLiteral(val), Expr::Identifier(tag)) => Some((tag.clone(), val.clone())),
        _ => None,
    }
}

fn is_time_identifier(name: &str) -> bool {
    name.eq_ignore_ascii_case("time")
}

fn is_time_bound_expr(expr: &Expr) -> bool {
    match expr {
        Expr::Identifier(name) | Expr::FieldRef { name, .. } => is_time_identifier(name),
        Expr::Now => true,
        Expr::BinaryExpr(be) => is_time_bound_expr(&be.left) || is_time_bound_expr(&be.right),
        Expr::UnaryExpr(_, inner) => is_time_bound_expr(inner),
        Expr::DurationLiteral(_) | Expr::TimeLiteral(_) | Expr::Call(_) => true,
        _ => false,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::domain::sharding::{MeasurementKey, MeasurementShardSpace};
    use crate::timeseriesql::parser::parse_query;

    fn four_region_space() -> MeasurementShardSpace {
        let q = u64::MAX / 4;
        MeasurementShardSpace {
            key: MeasurementKey::new("db", "autogen", "metrics"),
            regions: vec![
                ShardRegion {
                    region_id: 1,
                    start: 0,
                    end: q,
                    epoch: Default::default(),
                    peers: vec![1],
                    primary: 1,
                    last_split_at: 0,
                    transfer_verified: None,
                    transfer_first_seen: None,
                    state: Default::default(),
                    staged_from: None,
                    staging_peers: Vec::new(),
                },
                ShardRegion {
                    region_id: 2,
                    start: q,
                    end: q * 2,
                    epoch: Default::default(),
                    peers: vec![1],
                    primary: 1,
                    last_split_at: 0,
                    transfer_verified: None,
                    transfer_first_seen: None,
                    state: Default::default(),
                    staged_from: None,
                    staging_peers: Vec::new(),
                },
                ShardRegion {
                    region_id: 3,
                    start: q * 2,
                    end: q * 3,
                    epoch: Default::default(),
                    peers: vec![1],
                    primary: 1,
                    last_split_at: 0,
                    transfer_verified: None,
                    transfer_first_seen: None,
                    state: Default::default(),
                    staged_from: None,
                    staging_peers: Vec::new(),
                },
                ShardRegion {
                    region_id: 4,
                    start: q * 3,
                    end: u64::MAX,
                    epoch: Default::default(),
                    peers: vec![1],
                    primary: 1,
                    last_split_at: 0,
                    transfer_verified: None,
                    transfer_first_seen: None,
                    state: Default::default(),
                    staged_from: None,
                    staging_peers: Vec::new(),
                },
            ],
        }
    }

    fn select_stmt(q: &str) -> SelectStatement {
        match parse_query(q).unwrap().remove(0) {
            crate::timeseriesql::ast::Statement::Select(s) => s,
            _ => panic!("expected select"),
        }
    }

    fn host_only_tags() -> Vec<String> {
        vec!["host".into()]
    }

    #[test]
    fn unknown_tag_keys_query_every_region() {
        let space = four_region_space();
        let stmt = select_stmt("SELECT count(value) FROM metrics WHERE time >= 0");
        // With no tags a time-only WHERE pins the one tagless series...
        let sel = select_regions_for_query(&space, "metrics", Some(&[]), &stmt);
        assert!(matches!(sel, RegionSelection::Single(_)));
        // ...but unknown tag keys must not be read as "no tags".
        let sel = select_regions_for_query(&space, "metrics", None, &stmt);
        assert!(matches!(sel, RegionSelection::All));
    }

    #[test]
    fn equality_where_selects_single_region() {
        let space = four_region_space();
        let stmt = select_stmt(r#"SELECT value FROM metrics WHERE host = 's50000'"#);
        let sel = select_regions_for_query(&space, "metrics", Some(&host_only_tags()), &stmt);
        assert!(matches!(sel, RegionSelection::Single(_)));
        assert_eq!(sel.region_count(space.regions.len()), 1);
    }

    #[test]
    fn count_without_where_selects_all_regions() {
        let space = four_region_space();
        let stmt = select_stmt("SELECT count(value) FROM metrics");
        let sel = select_regions_for_query(&space, "metrics", Some(&host_only_tags()), &stmt);
        assert!(matches!(sel, RegionSelection::All));
        assert_eq!(sel.region_count(space.regions.len()), 4);
    }

    #[test]
    fn regex_where_selects_all_regions() {
        let space = four_region_space();
        let stmt = select_stmt(r#"SELECT value FROM metrics WHERE host =~ /^prod/"#);
        let sel = select_regions_for_query(&space, "metrics", Some(&host_only_tags()), &stmt);
        assert!(matches!(sel, RegionSelection::All));
    }

    #[test]
    fn and_with_time_and_tag_selects_single_region() {
        let space = four_region_space();
        let stmt =
            select_stmt(r#"SELECT value FROM metrics WHERE host = 's1' AND time > now() - 1h"#);
        let sel = select_regions_for_query(&space, "metrics", Some(&host_only_tags()), &stmt);
        assert!(matches!(sel, RegionSelection::Single(_)));
    }

    #[test]
    fn or_same_tag_selects_subset() {
        let space = four_region_space();
        let (h1, h2) = two_hosts_in_different_regions(&space, "metrics");
        let stmt = select_stmt(&format!(
            r#"SELECT value FROM metrics WHERE host = '{h1}' OR host = '{h2}'"#
        ));
        let sel = select_regions_for_query(&space, "metrics", Some(&host_only_tags()), &stmt);
        assert!(matches!(sel, RegionSelection::Subset(_)));
        assert_eq!(sel.region_count(space.regions.len()), 2);
    }

    fn two_hosts_in_different_regions(
        space: &MeasurementShardSpace,
        measurement: &str,
    ) -> (String, String) {
        for i in 0..50_000u64 {
            let h1 = format!("host-{i}");
            let mut t1 = BTreeMap::new();
            t1.insert("host".into(), h1.clone());
            let r1 = space
                .locate(series_id(measurement, &t1))
                .expect("region")
                .region_id;
            for j in (i + 1)..(i + 500).min(50_000) {
                let h2 = format!("host-{j}");
                let mut t2 = BTreeMap::new();
                t2.insert("host".into(), h2.clone());
                let r2 = space
                    .locate(series_id(measurement, &t2))
                    .expect("region")
                    .region_id;
                if r1 != r2 {
                    return (h1, h2);
                }
            }
        }
        panic!("no host pair in different regions");
    }

    /// Grafana's multi-value variable next to a time bound: routed to the
    /// regions of the listed hosts, not to every region.
    #[test]
    fn literal_regex_with_time_selects_subset() {
        let space = four_region_space();
        let (h1, h2) = two_hosts_in_different_regions(&space, "metrics");
        for q in [
            format!(
                r#"SELECT last(value) FROM metrics WHERE host =~ /^({h1}|{h2})$/ AND time >= 0 GROUP BY host"#
            ),
            format!(r#"SELECT value FROM metrics WHERE time >= 0 AND host =~ /^(?:{h1}|{h2})$/"#),
            format!(
                r#"SELECT value FROM metrics WHERE (host = '{h1}' OR host = '{h2}') AND time >= 0"#
            ),
        ] {
            let sel = select_regions_for_query(
                &space,
                "metrics",
                Some(&host_only_tags()),
                &select_stmt(&q),
            );
            assert_eq!(sel.region_count(space.regions.len()), 2, "{q}");
        }
        let one = format!(r#"SELECT value FROM metrics WHERE host =~ /^{h1}$/"#);
        let sel = select_regions_for_query(
            &space,
            "metrics",
            Some(&host_only_tags()),
            &select_stmt(&one),
        );
        assert!(matches!(sel, RegionSelection::Single(_)), "{one}");
    }

    #[test]
    fn regex_literal_alternatives_only_for_exact_sets() {
        let alts = |p: &str| regex_literal_alternatives(p);
        assert_eq!(alts("^(a|b)$"), Some(vec!["a".into(), "b".into()]));
        assert_eq!(alts("^(?:a|b)$"), Some(vec!["a".into(), "b".into()]));
        assert_eq!(
            alts(r"^(web\.1|web\-2)$"),
            Some(vec!["web.1".into(), "web-2".into()])
        );
        assert_eq!(alts("^s1$"), Some(vec!["s1".into()]));
        for p in [
            "^a|b$",     // (^a)|(b$)
            "^(a|b)",    // unanchored end
            "(a|b)$",    // unanchored start
            "^(a.|b)$",  // metacharacter
            "^(a|b)+$",  // repetition
            "^(a)|(b)$", // two groups
            "^(a||b)$",  // empty alternative matches ''
            r"^(a|b)\$", // escaped dollar: no end anchor
            r"^(a\d)$",  // class escape
            "^(?i)a$",   // flags
        ] {
            assert_eq!(alts(p), None, "{p}");
        }
    }

    #[test]
    fn regex_with_two_tag_keys_needs_every_key() {
        let space = four_region_space();
        let tags = vec!["host".into(), "dc".into()];
        let partial = select_stmt(r#"SELECT value FROM metrics WHERE host =~ /^(a|b)$/"#);
        let sel = select_regions_for_query(&space, "metrics", Some(&tags), &partial);
        assert!(matches!(sel, RegionSelection::All));
        let full = select_stmt(r#"SELECT value FROM metrics WHERE host =~ /^(a|b)$/ AND dc = 'x'"#);
        let sel = select_regions_for_query(&space, "metrics", Some(&tags), &full);
        assert!(!matches!(sel, RegionSelection::All));
    }

    #[test]
    fn mixed_tag_or_falls_back_to_all() {
        let space = four_region_space();
        let stmt = select_stmt(r#"SELECT value FROM metrics WHERE host = 'a' OR region = 'b'"#);
        let sel = select_regions_for_query(&space, "metrics", Some(&host_only_tags()), &stmt);
        assert!(matches!(sel, RegionSelection::All));
    }

    #[test]
    fn partial_tag_equality_falls_back_to_all_regions() {
        let space = four_region_space();
        let stmt = select_stmt(r#"SELECT value FROM metrics WHERE host = 's50000'"#);
        let tags = vec!["host".into(), "region".into()];
        let sel = select_regions_for_query(&space, "metrics", Some(&tags), &stmt);
        assert!(matches!(sel, RegionSelection::All));
    }

    #[test]
    fn materialized_dest_selects_all_regions() {
        let space = four_region_space();
        let stmt = select_stmt(r#"SELECT mean(value) FROM metrics WHERE host = 's50000'"#);
        let sel = select_regions_for_materialized_dest(&space);
        assert!(matches!(sel, RegionSelection::All));
        assert_eq!(sel.region_count(space.regions.len()), 4);
        // Narrowing WHERE on a materialized dest must not reduce fan-out.
        let _ = stmt;
    }
}
