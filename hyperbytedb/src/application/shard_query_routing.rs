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
#[must_use]
pub fn select_regions_for_query<'a>(
    space: &'a MeasurementShardSpace,
    measurement: &str,
    stmt: &SelectStatement,
) -> RegionSelection<'a> {
    let Some(series_ids) = routable_series_ids(measurement, stmt.condition.as_ref()) else {
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
fn routable_series_ids(measurement: &str, condition: Option<&Expr>) -> Option<Vec<u64>> {
    let expr = condition?;
    if let Some(tags) = tag_equalities_from_and(expr) {
        return Some(vec![series_id(measurement, &tags)]);
    }
    same_tag_or_series_ids(measurement, expr)
}

/// `tag = 'a' AND region = 'b' AND time > 1` → tag map; non-tag equalities on `time` are ignored.
fn tag_equalities_from_and(expr: &Expr) -> Option<BTreeMap<String, String>> {
    let mut tags = BTreeMap::new();
    if collect_and_tag_equalities(expr, &mut tags) {
        Some(tags)
    } else {
        None
    }
}

fn collect_and_tag_equalities(expr: &Expr, tags: &mut BTreeMap<String, String>) -> bool {
    match expr {
        Expr::BinaryExpr(be) => match be.op {
            BinaryOp::And => {
                collect_and_tag_equalities(&be.left, tags)
                    && collect_and_tag_equalities(&be.right, tags)
            }
            BinaryOp::Eq => {
                if let Some((tag, value)) = parse_tag_string_eq(&be.left, &be.right) {
                    if is_time_identifier(&tag) {
                        return true;
                    }
                    tags.insert(tag, value);
                    true
                } else {
                    false
                }
            }
            BinaryOp::Gt | BinaryOp::Gte | BinaryOp::Lt | BinaryOp::Lte => {
                is_time_bound_expr(&be.left) || is_time_bound_expr(&be.right)
            }
            _ => false,
        },
        _ => false,
    }
}

/// `(host = 'a' OR host = 'b')` → two series_ids for the same tag.
fn same_tag_or_series_ids(measurement: &str, expr: &Expr) -> Option<Vec<u64>> {
    let mut values = Vec::new();
    let mut tag_name: Option<String> = None;
    if !collect_or_tag_values(expr, &mut tag_name, &mut values) {
        return None;
    }
    let tag = tag_name?;
    if values.is_empty() || values.len() > MAX_SERIES_IDS_FOR_ROUTING {
        return None;
    }
    Some(
        values
            .into_iter()
            .map(|v| {
                let mut tags = BTreeMap::new();
                tags.insert(tag.clone(), v);
                series_id(measurement, &tags)
            })
            .collect(),
    )
}

fn collect_or_tag_values(
    expr: &Expr,
    tag_name: &mut Option<String>,
    values: &mut Vec<String>,
) -> bool {
    match expr {
        Expr::BinaryExpr(be) => match be.op {
            BinaryOp::Or => {
                collect_or_tag_values(&be.left, tag_name, values)
                    && collect_or_tag_values(&be.right, tag_name, values)
            }
            BinaryOp::Eq => {
                let Some((tag, value)) = parse_tag_string_eq(&be.left, &be.right) else {
                    return false;
                };
                if is_time_identifier(&tag) {
                    return false;
                }
                match tag_name {
                    None => *tag_name = Some(tag),
                    Some(existing) if existing == &tag => {}
                    Some(_) => return false,
                }
                values.push(value);
                true
            }
            _ => false,
        },
        _ => false,
    }
}

fn parse_tag_string_eq(left: &Expr, right: &Expr) -> Option<(String, String)> {
    match (left, right) {
        (Expr::Identifier(tag) | Expr::FieldRef { name: tag, .. }, Expr::StringLiteral(val))
        | (Expr::StringLiteral(val), Expr::Identifier(tag) | Expr::FieldRef { name: tag, .. }) => {
            Some((tag.clone(), val.clone()))
        }
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
                },
                ShardRegion {
                    region_id: 2,
                    start: q,
                    end: q * 2,
                    epoch: Default::default(),
                    peers: vec![1],
                    primary: 1,
                    last_split_at: 0,
                },
                ShardRegion {
                    region_id: 3,
                    start: q * 2,
                    end: q * 3,
                    epoch: Default::default(),
                    peers: vec![1],
                    primary: 1,
                    last_split_at: 0,
                },
                ShardRegion {
                    region_id: 4,
                    start: q * 3,
                    end: u64::MAX,
                    epoch: Default::default(),
                    peers: vec![1],
                    primary: 1,
                    last_split_at: 0,
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

    #[test]
    fn equality_where_selects_single_region() {
        let space = four_region_space();
        let stmt = select_stmt(r#"SELECT value FROM metrics WHERE host = 's50000'"#);
        let sel = select_regions_for_query(&space, "metrics", &stmt);
        assert!(matches!(sel, RegionSelection::Single(_)));
        assert_eq!(sel.region_count(space.regions.len()), 1);
    }

    #[test]
    fn count_without_where_selects_all_regions() {
        let space = four_region_space();
        let stmt = select_stmt("SELECT count(value) FROM metrics");
        let sel = select_regions_for_query(&space, "metrics", &stmt);
        assert!(matches!(sel, RegionSelection::All));
        assert_eq!(sel.region_count(space.regions.len()), 4);
    }

    #[test]
    fn regex_where_selects_all_regions() {
        let space = four_region_space();
        let stmt = select_stmt(r#"SELECT value FROM metrics WHERE host =~ /^prod/"#);
        let sel = select_regions_for_query(&space, "metrics", &stmt);
        assert!(matches!(sel, RegionSelection::All));
    }

    #[test]
    fn and_with_time_and_tag_selects_single_region() {
        let space = four_region_space();
        let stmt =
            select_stmt(r#"SELECT value FROM metrics WHERE host = 's1' AND time > now() - 1h"#);
        let sel = select_regions_for_query(&space, "metrics", &stmt);
        assert!(matches!(sel, RegionSelection::Single(_)));
    }

    #[test]
    fn or_same_tag_selects_subset() {
        let space = four_region_space();
        let (h1, h2) = two_hosts_in_different_regions(&space, "metrics");
        let stmt = select_stmt(&format!(
            r#"SELECT value FROM metrics WHERE host = '{h1}' OR host = '{h2}'"#
        ));
        let sel = select_regions_for_query(&space, "metrics", &stmt);
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

    #[test]
    fn mixed_tag_or_falls_back_to_all() {
        let space = four_region_space();
        let stmt = select_stmt(r#"SELECT value FROM metrics WHERE host = 'a' OR region = 'b'"#);
        let sel = select_regions_for_query(&space, "metrics", &stmt);
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
