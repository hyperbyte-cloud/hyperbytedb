//! InfluxDB v1 continuous query scheduling and coverage windows.
//!
//! See: https://docs.influxdata.com/influxdb/v1/query_language/continuous_queries/

use chrono::{DateTime, TimeZone, Utc};

use crate::domain::continuous_query::ContinuousQueryDef;
use crate::error::HyperbytedbError;
use crate::timeseriesql::ast::CreateContinuousQueryStatement;

/// Half-open window `[start, end)` applied to raw point timestamps.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct CqWindow {
    pub start: DateTime<Utc>,
    pub end: DateTime<Utc>,
}

/// Align `ts` down to the start of an interval bucket with optional offset.
pub fn bucket_start(unix_secs: i64, interval_secs: u64, offset_secs: u64) -> i64 {
    if interval_secs == 0 {
        return unix_secs;
    }
    let interval = interval_secs as i64;
    let offset = offset_secs as i64;
    let shifted = unix_secs - offset;
    let bucket = shifted.div_euclid(interval) * interval;
    bucket + offset
}

/// Derive schedule metadata from a parsed CREATE statement (Influx semantics).
pub fn derive_schedule(
    cq: &CreateContinuousQueryStatement,
) -> Result<ScheduleMeta, HyperbytedbError> {
    let group_by_interval_secs = group_by_interval_secs(&cq.query)?;
    let group_by_offset_secs = group_by_offset_secs(&cq.query)?;

    let resample_every_secs = cq
        .resample_every
        .as_ref()
        .map(duration_to_secs)
        .transpose()?;
    let resample_for_secs = cq.resample_for.as_ref().map(duration_to_secs).transpose()?;

    let is_advanced = resample_every_secs.is_some() || resample_for_secs.is_some();

    let execution_interval_secs = resample_every_secs.unwrap_or(group_by_interval_secs);

    if let Some(for_secs) = resample_for_secs {
        let min_for = execution_interval_secs.max(group_by_interval_secs);
        if for_secs < min_for {
            return Err(HyperbytedbError::QueryParse(format!(
                "FOR duration must be >= GROUP BY time duration: must be a minimum of {}s got {}s",
                min_for, for_secs
            )));
        }
    }

    let coverage_interval_secs = if let Some(for_secs) = resample_for_secs {
        for_secs
    } else if is_advanced
        && resample_every_secs.is_some()
        && execution_interval_secs > group_by_interval_secs
    {
        // EVERY > GROUP BY without FOR: cover the EVERY window.
        execution_interval_secs
    } else {
        group_by_interval_secs
    };

    Ok(ScheduleMeta {
        group_by_interval_secs,
        group_by_offset_secs,
        execution_interval_secs,
        coverage_interval_secs,
        resample_every_secs,
        resample_for_secs,
        is_advanced,
    })
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ScheduleMeta {
    pub group_by_interval_secs: u64,
    pub group_by_offset_secs: u64,
    pub execution_interval_secs: u64,
    pub coverage_interval_secs: u64,
    pub resample_every_secs: Option<u64>,
    pub resample_for_secs: Option<u64>,
    pub is_advanced: bool,
}

impl ScheduleMeta {
    pub fn apply_to(&self, def: &mut ContinuousQueryDef) {
        def.group_by_interval_secs = self.group_by_interval_secs;
        def.group_by_offset_secs = self.group_by_offset_secs;
        def.execution_interval_secs = self.execution_interval_secs;
        def.coverage_interval_secs = self.coverage_interval_secs;
        def.is_advanced = self.is_advanced;
    }
}

/// Whether `cq` should execute at `now` (Influx boundary-aligned scheduling).
pub fn should_run(now: DateTime<Utc>, cq: &ContinuousQueryDef) -> bool {
    let exec_secs = cq.execution_interval_secs.max(1);
    let offset = cq.group_by_offset_secs;

    let last = last_run_time(cq, now);
    let current_boundary = bucket_start(now.timestamp(), exec_secs, offset);
    let last_boundary = bucket_start(last.timestamp(), exec_secs, offset);

    now.timestamp() >= current_boundary && current_boundary > last_boundary
}

fn last_run_time(cq: &ContinuousQueryDef, fallback: DateTime<Utc>) -> DateTime<Utc> {
    cq.last_run_at
        .as_ref()
        .and_then(|s| DateTime::parse_from_rfc3339(s).ok())
        .map(|dt| dt.with_timezone(&Utc))
        .or_else(|| {
            DateTime::parse_from_rfc3339(&cq.created_at)
                .ok()
                .map(|dt| dt.with_timezone(&Utc))
        })
        .unwrap_or(fallback)
}

/// Compute the raw-data time window for a CQ execution at `now`.
pub fn coverage_window(now: DateTime<Utc>, cq: &ContinuousQueryDef) -> CqWindow {
    let meta = schedule_from_def(cq);
    coverage_window_from_meta(now, &meta)
}

fn schedule_from_def(cq: &ContinuousQueryDef) -> ScheduleMeta {
    ScheduleMeta {
        group_by_interval_secs: cq.group_by_interval_secs,
        group_by_offset_secs: cq.group_by_offset_secs,
        execution_interval_secs: cq.execution_interval_secs,
        coverage_interval_secs: cq.coverage_interval_secs,
        resample_every_secs: cq.resample_every_secs,
        resample_for_secs: cq.resample_for_secs,
        is_advanced: cq.is_advanced,
    }
}

fn coverage_window_from_meta(now: DateTime<Utc>, meta: &ScheduleMeta) -> CqWindow {
    let offset = meta.group_by_offset_secs;
    let group_secs = meta.group_by_interval_secs as i64;
    let exec_secs = meta.execution_interval_secs as i64;

    if meta.resample_for_secs.is_some() {
        let for_secs = meta
            .resample_for_secs
            .unwrap_or(meta.coverage_interval_secs) as i64;
        let end = bucket_start(now.timestamp(), meta.execution_interval_secs, offset);
        let start = end - for_secs;
        return window_from_unix(start, end);
    }

    if meta.is_advanced && meta.resample_every_secs.is_some() && exec_secs > group_secs {
        // EVERY > GROUP BY without FOR: [now - EVERY, now).
        let end = now.timestamp();
        let start = end - exec_secs;
        return window_from_unix(start, end);
    }

    if meta.is_advanced && meta.resample_every_secs.is_some() {
        // EVERY without FOR: current GROUP BY bucket containing now.
        let start = bucket_start(now.timestamp(), meta.group_by_interval_secs, offset);
        let end = start + group_secs;
        return window_from_unix(start, end);
    }

    // Basic syntax: [boundary - group_interval, boundary).
    let end = bucket_start(now.timestamp(), meta.group_by_interval_secs, offset);
    let start = end - group_secs;
    window_from_unix(start, end)
}

fn window_from_unix(start_secs: i64, end_secs: i64) -> CqWindow {
    CqWindow {
        start: Utc.timestamp_opt(start_secs, 0).unwrap(),
        end: Utc.timestamp_opt(end_secs, 0).unwrap(),
    }
}

fn group_by_interval_secs(
    query: &crate::timeseriesql::ast::SelectStatement,
) -> Result<u64, HyperbytedbError> {
    let gb = query.group_by.as_ref().ok_or_else(|| {
        HyperbytedbError::QueryParse(
            "continuous query requires GROUP BY time(<interval>)".to_string(),
        )
    })?;
    let Some(crate::timeseriesql::ast::Dimension::Time { interval, .. }) = gb.time_dimension()
    else {
        return Err(HyperbytedbError::QueryParse(
            "continuous query requires GROUP BY time(<interval>)".to_string(),
        ));
    };
    duration_to_secs(interval)
}

fn group_by_offset_secs(
    query: &crate::timeseriesql::ast::SelectStatement,
) -> Result<u64, HyperbytedbError> {
    let gb = query.group_by.as_ref().ok_or_else(|| {
        HyperbytedbError::QueryParse(
            "continuous query requires GROUP BY time(<interval>)".to_string(),
        )
    })?;
    match gb.time_dimension() {
        Some(crate::timeseriesql::ast::Dimension::Time {
            offset: Some(o), ..
        }) => duration_to_secs(o),
        _ => Ok(0),
    }
}

fn duration_to_secs(d: &crate::timeseriesql::ast::Duration) -> Result<u64, HyperbytedbError> {
    let nanos = d.to_nanos();
    if nanos <= 0 {
        return Err(HyperbytedbError::QueryParse(
            "duration must be positive".to_string(),
        ));
    }
    Ok((nanos / 1_000_000_000) as u64)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::timeseriesql::parser::parse_query;

    fn parse_cq(inner: &str) -> CreateContinuousQueryStatement {
        let q = format!(
            r#"CREATE CONTINUOUS QUERY "cq" ON "db" BEGIN {} END"#,
            inner
        );
        match parse_query(&q).unwrap().remove(0) {
            crate::timeseriesql::ast::Statement::CreateContinuousQuery(cq) => cq,
            _ => panic!("expected CQ"),
        }
    }

    fn parse_cq_resample(resample: &str, inner: &str) -> CreateContinuousQueryStatement {
        let q = format!(
            r#"CREATE CONTINUOUS QUERY "cq" ON "db" {} BEGIN {} END"#,
            resample, inner
        );
        match parse_query(&q).unwrap().remove(0) {
            crate::timeseriesql::ast::Statement::CreateContinuousQuery(cq) => cq,
            _ => panic!("expected CQ"),
        }
    }

    fn def_from_cq(cq: &CreateContinuousQueryStatement) -> ContinuousQueryDef {
        let meta = derive_schedule(cq).unwrap();
        let mut def = ContinuousQueryDef {
            name: cq.name.clone(),
            database: cq.database.clone(),
            query_text: cq.raw_query.clone(),
            resample_every_secs: meta.resample_every_secs,
            resample_for_secs: meta.resample_for_secs,
            created_at: "2016-08-28T06:00:00Z".to_string(),
            group_by_interval_secs: 0,
            group_by_offset_secs: 0,
            execution_interval_secs: 0,
            coverage_interval_secs: 0,
            is_advanced: false,
            last_run_at: None,
        };
        meta.apply_to(&mut def);
        def
    }

    fn ts(h: u32, m: u32) -> DateTime<Utc> {
        Utc.with_ymd_and_hms(2016, 8, 28, h, m, 0).unwrap()
    }

    #[test]
    fn bucket_start_hourly() {
        assert_eq!(
            bucket_start(ts(8, 0).timestamp(), 3600, 0),
            ts(8, 0).timestamp()
        );
        assert_eq!(
            bucket_start(ts(8, 30).timestamp(), 3600, 0),
            ts(8, 0).timestamp()
        );
        assert_eq!(
            bucket_start(ts(8, 15).timestamp(), 3600, 900),
            ts(8, 15).timestamp()
        );
    }

    #[test]
    fn basic_hourly_coverage_at_8am() {
        let cq = parse_cq(
            r#"SELECT mean("passengers") INTO "average_passengers" FROM "bus_data" GROUP BY time(1h)"#,
        );
        let def = def_from_cq(&cq);
        assert_eq!(def.execution_interval_secs, 3600);
        assert_eq!(def.coverage_interval_secs, 3600);

        let w = coverage_window(ts(8, 0), &def);
        assert_eq!(w.start, ts(7, 0));
        assert_eq!(w.end, ts(8, 0));
    }

    #[test]
    fn basic_offset_coverage_at_815() {
        let cq = parse_cq(
            r#"SELECT mean("passengers") INTO "average_passengers" FROM "bus_data" GROUP BY time(1h, 15m)"#,
        );
        let def = def_from_cq(&cq);
        assert_eq!(def.group_by_offset_secs, 900);

        let w = coverage_window(ts(8, 15), &def);
        assert_eq!(w.start, ts(7, 15));
        assert_eq!(w.end, ts(8, 15));
    }

    #[test]
    fn advanced_every_30m_group_1h_at_830() {
        let cq = parse_cq_resample(
            "RESAMPLE EVERY 30m",
            r#"SELECT mean("passengers") INTO "average_passengers" FROM "bus_data" GROUP BY time(1h)"#,
        );
        let def = def_from_cq(&cq);
        assert_eq!(def.execution_interval_secs, 1800);

        let w = coverage_window(ts(8, 30), &def);
        assert_eq!(w.start, ts(8, 0));
        assert_eq!(w.end, ts(9, 0));
    }

    #[test]
    fn advanced_for_1h_group_30m_at_8am() {
        let cq = parse_cq_resample(
            "RESAMPLE FOR 1h",
            r#"SELECT mean("passengers") INTO "average_passengers" FROM "bus_data" GROUP BY time(30m)"#,
        );
        let def = def_from_cq(&cq);
        assert_eq!(def.execution_interval_secs, 1800);
        assert_eq!(def.resample_for_secs, Some(3600));

        let w = coverage_window(ts(8, 0), &def);
        assert_eq!(w.start, ts(7, 0));
        assert_eq!(w.end, ts(8, 0));
    }

    #[test]
    fn advanced_every_1h_for_90m_at_9am() {
        let cq = parse_cq_resample(
            "RESAMPLE EVERY 1h FOR 90m",
            r#"SELECT mean("passengers") INTO "average_passengers" FROM "bus_data" GROUP BY time(30m)"#,
        );
        let def = def_from_cq(&cq);

        let w = coverage_window(ts(9, 0), &def);
        assert_eq!(w.start, ts(7, 30));
        assert_eq!(w.end, ts(9, 0));
    }

    #[test]
    fn every_greater_than_group_by_uses_every_window() {
        let cq = parse_cq_resample(
            "RESAMPLE EVERY 10m",
            r#"SELECT mean("value") INTO "downsampled" FROM "cpu" GROUP BY time(5m)"#,
        );
        let meta = derive_schedule(&cq).unwrap();
        assert_eq!(meta.execution_interval_secs, 600);
        assert_eq!(meta.coverage_interval_secs, 600);

        let mut def = def_from_cq(&cq);
        meta.apply_to(&mut def);
        let w = coverage_window(ts(8, 0), &def);
        assert_eq!(w.end.timestamp() - w.start.timestamp(), 600);
    }

    #[test]
    fn for_shorter_than_group_by_rejected() {
        let cq = parse_cq_resample(
            "RESAMPLE FOR 5m",
            r#"SELECT mean("value") INTO "downsampled" FROM "cpu" GROUP BY time(30m)"#,
        );
        assert!(derive_schedule(&cq).is_err());
    }

    #[test]
    fn should_run_advances_on_boundaries_only() {
        let cq = parse_cq(
            r#"SELECT mean("passengers") INTO "average_passengers" FROM "bus_data" GROUP BY time(1h)"#,
        );
        let mut def = def_from_cq(&cq);
        def.last_run_at = Some(ts(7, 0).to_rfc3339());

        assert!(!should_run(ts(7, 10), &def));
        assert!(!should_run(ts(7, 59), &def));
        assert!(should_run(ts(8, 0), &def));
        def.last_run_at = Some(ts(8, 0).to_rfc3339());
        assert!(!should_run(ts(8, 5), &def));
    }

    #[test]
    fn should_run_no_burst_in_first_minute() {
        let cq = parse_cq(
            r#"SELECT mean("passengers") INTO "average_passengers" FROM "bus_data" GROUP BY time(1h)"#,
        );
        let mut def = def_from_cq(&cq);
        def.created_at = ts(7, 5).to_rfc3339();

        assert!(!should_run(ts(7, 10), &def));
        assert!(!should_run(ts(7, 30), &def));
        assert!(should_run(ts(8, 0), &def));
    }
}
