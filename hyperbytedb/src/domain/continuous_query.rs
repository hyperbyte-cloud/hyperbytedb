use serde::{Deserialize, Serialize};

use crate::domain::cq_schedule::derive_schedule;
use crate::error::HyperbytedbError;
use crate::timeseriesql::ast::CreateContinuousQueryStatement;
use crate::timeseriesql::parser::parse_query;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ContinuousQueryDef {
    pub name: String,
    pub database: String,
    pub query_text: String,
    pub resample_every_secs: Option<u64>,
    pub resample_for_secs: Option<u64>,
    pub created_at: String,
    /// `GROUP BY time(<interval>)` in seconds.
    #[serde(default)]
    pub group_by_interval_secs: u64,
    /// `GROUP BY time(<interval>, <offset>)` offset in seconds.
    #[serde(default)]
    pub group_by_offset_secs: u64,
    /// Execution interval (`EVERY` or group-by interval).
    #[serde(default)]
    pub execution_interval_secs: u64,
    /// Default coverage span derived from `FOR` / Influx rules.
    #[serde(default)]
    pub coverage_interval_secs: u64,
    /// True when `RESAMPLE` is present.
    #[serde(default)]
    pub is_advanced: bool,
    /// RFC3339 timestamp of the last successful execution.
    #[serde(default)]
    pub last_run_at: Option<String>,
}

impl ContinuousQueryDef {
    /// Build metadata from a parsed CREATE statement, including Influx validation.
    pub fn from_create(cq: &CreateContinuousQueryStatement) -> Result<Self, HyperbytedbError> {
        if cq.query.into.is_none() {
            return Err(HyperbytedbError::QueryParse(
                "continuous query requires SELECT INTO".to_string(),
            ));
        }

        let meta = derive_schedule(cq)?;
        let mut def = Self {
            name: cq.name.clone(),
            database: cq.database.clone(),
            query_text: cq.raw_query.clone(),
            resample_every_secs: meta.resample_every_secs,
            resample_for_secs: meta.resample_for_secs,
            created_at: chrono::Utc::now().to_rfc3339(),
            group_by_interval_secs: 0,
            group_by_offset_secs: 0,
            execution_interval_secs: 0,
            coverage_interval_secs: 0,
            is_advanced: false,
            last_run_at: None,
        };
        meta.apply_to(&mut def);
        Ok(def)
    }

    /// Fill derived schedule fields for definitions stored before CQ parity.
    pub fn normalize(&mut self) -> Result<(), HyperbytedbError> {
        if self.group_by_interval_secs > 0 && self.execution_interval_secs > 0 {
            return Ok(());
        }
        let stmts = parse_query(&self.query_text)?;
        let select = match stmts.first() {
            Some(crate::timeseriesql::ast::Statement::Select(s)) => s.clone(),
            _ => {
                return Err(HyperbytedbError::QueryParse(
                    "continuous query body must be a SELECT statement".to_string(),
                ));
            }
        };
        let cq = CreateContinuousQueryStatement {
            name: self.name.clone(),
            database: self.database.clone(),
            query: select,
            raw_query: self.query_text.clone(),
            resample_every: self.resample_every_secs.map(secs_to_duration),
            resample_for: self.resample_for_secs.map(secs_to_duration),
        };
        let meta = derive_schedule(&cq)?;
        meta.apply_to(self);
        Ok(())
    }
}

fn secs_to_duration(secs: u64) -> crate::timeseriesql::ast::Duration {
    crate::timeseriesql::ast::Duration {
        value: secs as i64,
        unit: crate::timeseriesql::ast::DurationUnit::Second,
    }
}
