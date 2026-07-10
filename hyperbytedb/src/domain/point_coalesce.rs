//! Merge line-protocol / columnar partial writes that share a `(timestamp, tags)`
//! series-instant into one logical point (InfluxDB-compatible behaviour).
//!
//! Coalesce helpers operate on `(timestamp, tags)` only — **not** measurement name.
//! Callers must group by measurement first (see [`group_and_coalesce_by_measurement`])
//! when a batch contains multiple measurements, e.g. a Telegraf write bundle.

use std::borrow::Cow;
use std::collections::{BTreeMap, HashMap};
use std::hash::{Hash, Hasher};

use crate::domain::point::Point;

/// Plan groups of input indices that share a `(timestamp, tags)` series-instant.
///
/// Returns `None` when every point has a unique series-instant — callers use
/// their inputs unchanged with zero allocation.
pub fn coalesce_plan(points: &[Point]) -> Option<Vec<Vec<u32>>> {
    let idxs: Vec<u32> = (0..points.len() as u32).collect();
    coalesce_plan_indexed(points, &idxs)
}

/// Like [`coalesce_plan`] but over the subset of `points` selected by `idxs`
/// (indices into `points`). Returned groups likewise hold indices into `points`.
pub fn coalesce_plan_indexed(points: &[Point], idxs: &[u32]) -> Option<Vec<Vec<u32>>> {
    let mut groups: Vec<Vec<u32>> = Vec::with_capacity(idxs.len());
    let mut buckets: HashMap<u64, Vec<u32>> = HashMap::with_capacity(idxs.len());

    for &i in idxs {
        let p = &points[i as usize];
        let h = series_instant_hash(p);
        let bucket = buckets.entry(h).or_default();
        let existing = bucket
            .iter()
            .copied()
            .find(|&g| same_series_instant(&points[groups[g as usize][0] as usize], p));
        match existing {
            Some(g) => groups[g as usize].push(i),
            None => {
                let g = groups.len() as u32;
                groups.push(vec![i]);
                bucket.push(g);
            }
        }
    }

    if groups.len() == idxs.len() {
        None
    } else {
        Some(groups)
    }
}

/// Merge one group of points sharing a series-instant into a single point,
/// unioning fields with last-write-wins.
pub fn merge_point_group(points: &[Point], group: &[u32]) -> Point {
    let mut base = points[group[0] as usize].clone();
    for &idx in &group[1..] {
        for (fk, fv) in &points[idx as usize].fields {
            base.fields.insert(fk.clone(), fv.clone());
        }
    }
    base
}

/// Apply [`coalesce_plan`] to a `(points, origins)` pair, keeping them parallel.
///
/// When partial-line merges happen, each merged row inherits the `origin_node_id`
/// of its last contributor (consistent with last-write-wins field merge).
pub fn coalesce_points_and_origins(
    points: &[Point],
    origins: &[u64],
) -> Option<(Vec<Point>, Vec<u64>)> {
    let groups = coalesce_plan(points)?;
    let merged_points = groups
        .iter()
        .map(|g| merge_point_group(points, g))
        .collect();
    let merged_origins = groups
        .iter()
        .map(|g| origins[*g.last().unwrap_or(&g[0]) as usize])
        .collect();
    Some((merged_points, merged_origins))
}

/// Group points by measurement, coalescing partial rows within each group.
///
/// Matches the native WAL flush path: cross-measurement points that share the
/// same `(timestamp, tags)` (common in Telegraf bundles) stay separate. The
/// common no-merge case borrows the caller's points without cloning; only
/// measurements that actually contain partial-write duplicates allocate
/// merged points.
pub fn group_and_coalesce_by_measurement(points: &[Point]) -> Vec<(&str, Vec<Cow<'_, Point>>)> {
    let mut by_meas: BTreeMap<&str, Vec<u32>> = BTreeMap::new();
    for (i, p) in points.iter().enumerate() {
        by_meas
            .entry(p.measurement.as_str())
            .or_default()
            .push(i as u32);
    }

    by_meas
        .into_iter()
        .map(|(measurement, idxs)| {
            let meas_points: Vec<Cow<'_, Point>> = match coalesce_plan_indexed(points, &idxs) {
                None => idxs
                    .iter()
                    .map(|&i| Cow::Borrowed(&points[i as usize]))
                    .collect(),
                Some(groups) => groups
                    .iter()
                    .map(|g| Cow::Owned(merge_point_group(points, g)))
                    .collect(),
            };
            (measurement, meas_points)
        })
        .collect()
}

fn series_instant_hash(p: &Point) -> u64 {
    let mut h = std::collections::hash_map::DefaultHasher::new();
    p.timestamp.hash(&mut h);
    for (k, v) in &p.tags {
        k.hash(&mut h);
        v.hash(&mut h);
    }
    h.finish()
}

fn same_series_instant(a: &Point, b: &Point) -> bool {
    a.timestamp == b.timestamp && a.tags == b.tags
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::domain::point::FieldValue;

    fn make_point(ts: i64, tags: &[(&str, &str)], fields: &[(&str, FieldValue)]) -> Point {
        Point {
            measurement: "m".into(),
            tags: tags
                .iter()
                .map(|(k, v)| (k.to_string(), v.to_string()))
                .collect(),
            fields: fields
                .iter()
                .map(|(k, v)| (k.to_string(), v.clone()))
                .collect(),
            timestamp: ts,
        }
    }

    fn make_named_point(
        measurement: &str,
        ts: i64,
        tags: &[(&str, &str)],
        fields: &[(&str, FieldValue)],
    ) -> Point {
        let mut p = make_point(ts, tags, fields);
        p.measurement = measurement.into();
        p
    }

    #[test]
    fn coalesce_merges_partial_lines_same_series_and_time() {
        let ts = 1_778_437_451_000_000_000i64;
        let tags = &[("host", "h1")];
        let p1 = make_point(ts, tags, &[("load1", FieldValue::Float(2.92))]);
        let p2 = make_point(ts, tags, &[("uptime", FieldValue::UInteger(13761777))]);
        let p3 = make_point(
            ts,
            tags,
            &[("uptime_format", FieldValue::String("159 days".into()))],
        );
        let pts = [p1, p2, p3];
        let groups = coalesce_plan(&pts).expect("partial lines should merge");
        assert_eq!(groups.len(), 1);
        let merged = merge_point_group(&pts, &groups[0]);
        assert_eq!(merged.fields.len(), 3);
        assert_eq!(merged.fields.get("load1"), Some(&FieldValue::Float(2.92)));
        assert_eq!(
            merged.fields.get("uptime"),
            Some(&FieldValue::UInteger(13761777))
        );
    }

    #[test]
    fn coalesce_returns_none_when_nothing_merges() {
        let ts = 1_778_437_451_000_000_000i64;
        let p1 = make_point(ts, &[("host", "h1")], &[("v", FieldValue::Float(1.0))]);
        let p2 = make_point(ts, &[("host", "h2")], &[("v", FieldValue::Float(2.0))]);
        let p3 = make_point(ts + 1, &[("host", "h1")], &[("v", FieldValue::Float(3.0))]);
        assert!(coalesce_plan(&[p1, p2, p3]).is_none());
    }

    #[test]
    fn global_coalesce_merges_across_measurements_with_same_host_and_time() {
        let ts = 1_780_922_276_152_000_000i64;
        let host = &[("host", "telegraf-pod")];
        let points = vec![
            make_named_point("mem", ts, host, &[("used", FieldValue::Float(1.0))]),
            make_named_point("system", ts, host, &[("load1", FieldValue::Float(0.5))]),
            make_named_point("swap", ts, host, &[("free", FieldValue::Float(2.0))]),
            make_named_point(
                "cpu",
                ts,
                &[("host", "telegraf-pod"), ("cpu", "cpu0")],
                &[("usage_idle", FieldValue::Float(95.0))],
            ),
        ];
        let (merged, _) =
            coalesce_points_and_origins(&points, &[1, 1, 1, 1]).expect("global coalesce runs");
        assert_eq!(
            merged.len(),
            2,
            "bug: global coalesce collapses mem/system/swap (same host+time) into one row"
        );
    }

    #[test]
    fn coalesce_within_measurements_preserves_telegraf_host_only_measurements() {
        let ts = 1_780_922_276_152_000_000i64;
        let host = &[("host", "telegraf-pod")];
        let points = vec![
            make_named_point("mem", ts, host, &[("used", FieldValue::Float(1.0))]),
            make_named_point("system", ts, host, &[("load1", FieldValue::Float(0.5))]),
            make_named_point("swap", ts, host, &[("free", FieldValue::Float(2.0))]),
            make_named_point(
                "processes",
                ts,
                host,
                &[("running", FieldValue::UInteger(3))],
            ),
            make_named_point(
                "kernel",
                ts,
                host,
                &[("boot_time", FieldValue::UInteger(100))],
            ),
            make_named_point(
                "netstat",
                ts,
                host,
                &[("tcp_established", FieldValue::UInteger(1))],
            ),
            make_named_point(
                "cpu",
                ts,
                &[("host", "telegraf-pod"), ("cpu", "cpu0")],
                &[("usage_idle", FieldValue::Float(95.0))],
            ),
            make_named_point(
                "cpu",
                ts,
                &[("host", "telegraf-pod"), ("cpu", "cpu1")],
                &[("usage_idle", FieldValue::Float(90.0))],
            ),
        ];
        let grouped = group_and_coalesce_by_measurement(&points);
        let merged: Vec<&Point> = grouped
            .iter()
            .flat_map(|(_, pts)| pts.iter().map(|c| c.as_ref()))
            .collect();
        assert_eq!(merged.len(), points.len());
        assert!(
            grouped
                .iter()
                .flat_map(|(_, pts)| pts.iter())
                .all(|c| matches!(c, Cow::Borrowed(_))),
            "no-merge case must borrow, not clone"
        );
        let mut names: Vec<_> = merged.iter().map(|p| p.measurement.as_str()).collect();
        names.sort_unstable();
        assert_eq!(
            names,
            vec![
                "cpu",
                "cpu",
                "kernel",
                "mem",
                "netstat",
                "processes",
                "swap",
                "system"
            ]
        );
    }

    #[test]
    fn coalesce_within_measurements_still_merges_partial_lines() {
        let ts = 1_778_437_451_000_000_000i64;
        let tags = &[("host", "h1")];
        let points = vec![
            make_named_point("system", ts, tags, &[("load1", FieldValue::Float(2.92))]),
            make_named_point(
                "system",
                ts,
                tags,
                &[("uptime", FieldValue::UInteger(13761777))],
            ),
            make_named_point("mem", ts, tags, &[("used", FieldValue::Float(100.0))]),
        ];
        let grouped = group_and_coalesce_by_measurement(&points);
        let merged: Vec<&Point> = grouped
            .iter()
            .flat_map(|(_, pts)| pts.iter().map(|c| c.as_ref()))
            .collect();
        assert_eq!(merged.len(), 2);
        let system = merged
            .iter()
            .find(|p| p.measurement == "system")
            .expect("system row");
        assert_eq!(system.fields.len(), 2);
        assert_eq!(system.fields.get("load1"), Some(&FieldValue::Float(2.92)));
    }

    #[test]
    fn coalesce_telegraf_cpu_partial_lines() {
        use crate::application::line_protocol::parse_line_body_to_points;

        let ts = 1_780_922_276_152_000_000i64;
        let body = format!(
            "cpu,cpu=cpu-total,host=d2ddee27a9f4 usage_idle=95.0 {ts}\n\
             cpu,cpu=cpu-total,host=d2ddee27a9f4 usage_user=4.0 {ts}\n\
             cpu,cpu=cpu-total,host=d2ddee27a9f4 usage_system=1.0 {ts}"
        );
        let points = parse_line_body_to_points(body.as_bytes(), None).unwrap();
        assert_eq!(points.len(), 3);
        let (merged, _) =
            coalesce_points_and_origins(&points, &[0, 0, 0]).expect("should coalesce");
        assert_eq!(merged.len(), 1);
        let p = &merged[0];
        assert_eq!(p.fields.get("usage_idle"), Some(&FieldValue::Float(95.0)));
        assert_eq!(p.fields.get("usage_user"), Some(&FieldValue::Float(4.0)));
        assert_eq!(p.fields.get("usage_system"), Some(&FieldValue::Float(1.0)));
    }
}
