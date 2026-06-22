use serde::{Deserialize, Serialize};
use std::time::Duration;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Database {
    pub name: String,
    pub retention_policies: Vec<RetentionPolicy>,
    pub default_rp: String,
}

impl Database {
    pub fn new(name: &str) -> Self {
        let rp = RetentionPolicy::autogen();
        Self {
            name: name.to_string(),
            default_rp: rp.name.clone(),
            retention_policies: vec![rp],
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RetentionPolicy {
    pub name: String,
    #[serde(with = "optional_duration_serde")]
    pub duration: Option<Duration>,
    #[serde(with = "duration_serde")]
    pub shard_group_duration: Duration,
    pub replication_factor: u32,
    pub is_default: bool,
}

impl RetentionPolicy {
    pub fn autogen() -> Self {
        Self {
            name: "autogen".to_string(),
            duration: None,
            shard_group_duration: derive_shard_group_duration(None),
            replication_factor: 1,
            is_default: true,
        }
    }
}

/// Derive shard group duration from retention policy duration (InfluxDB 1.x rules).
#[must_use]
pub fn derive_shard_group_duration(duration: Option<Duration>) -> Duration {
    const H: u64 = 3600;
    const D: u64 = 86_400;
    const MO: u64 = 180 * D;
    match duration {
        None => Duration::from_secs(7 * D),
        Some(d) => {
            let secs = d.as_secs();
            if secs < 2 * D {
                Duration::from_secs(H)
            } else if secs < MO {
                Duration::from_secs(D)
            } else {
                Duration::from_secs(7 * D)
            }
        }
    }
}

/// Build a domain retention policy from CREATE DATABASE … WITH options.
#[must_use]
pub fn retention_policy_from_create(
    stmt: &crate::timeseriesql::ast::CreateDatabaseStatement,
) -> Option<RetentionPolicy> {
    use crate::timeseriesql::ast::DurationUnit;

    let has_with = stmt.duration.is_some()
        || stmt.replication.is_some()
        || stmt.shard_duration.is_some()
        || stmt.rp_name.is_some();
    if !has_with {
        return None;
    }

    let rp_name = stmt
        .rp_name
        .clone()
        .unwrap_or_else(|| "autogen".to_string());
    let std_duration = match &stmt.duration {
        None => None,
        Some(d) if d.value == 0 && d.unit == DurationUnit::Second => None,
        Some(d) => Some(Duration::from_nanos(d.to_nanos() as u64)),
    };
    let std_shard = stmt
        .shard_duration
        .as_ref()
        .map(|d| Duration::from_nanos(d.to_nanos() as u64))
        .unwrap_or_else(|| derive_shard_group_duration(std_duration));
    let replication = stmt.replication.unwrap_or(1);

    Some(RetentionPolicy {
        name: rp_name,
        duration: std_duration,
        shard_group_duration: std_shard,
        replication_factor: replication,
        is_default: true,
    })
}

/// Reconstruct CREATE DATABASE … WITH from a replicated retention policy.
#[must_use]
pub fn create_database_statement_from_rp(
    name: String,
    rp: &RetentionPolicy,
) -> crate::timeseriesql::ast::CreateDatabaseStatement {
    use crate::timeseriesql::ast::{
        CreateDatabaseStatement, Duration as AstDuration, DurationUnit,
    };
    use crate::timeseriesql::lexer::nanos_to_ast_duration;

    fn ast_duration_from_std(d: Duration) -> AstDuration {
        nanos_to_ast_duration(d.as_nanos() as i64)
    }

    CreateDatabaseStatement {
        name,
        duration: match rp.duration {
            None => Some(AstDuration {
                value: 0,
                unit: DurationUnit::Second,
            }),
            Some(d) => Some(ast_duration_from_std(d)),
        },
        replication: Some(rp.replication_factor),
        shard_duration: Some(ast_duration_from_std(rp.shard_group_duration)),
        rp_name: Some(rp.name.clone()),
    }
}

/// Format a duration for SHOW RETENTION POLICIES (Go-style: `168h0m0s`, `0s`).
#[must_use]
pub fn format_influx_duration(d: Option<Duration>) -> String {
    match d {
        None => "0s".to_string(),
        Some(dur) => {
            let mut secs = dur.as_secs();
            let weeks = secs / (7 * 86_400);
            secs %= 7 * 86_400;
            let days = secs / 86_400;
            secs %= 86_400;
            let hours = secs / 3600;
            secs %= 3600;
            let minutes = secs / 60;
            secs %= 60;
            let mut out = String::new();
            if weeks > 0 {
                out.push_str(&format!("{weeks}w"));
            }
            if days > 0 {
                out.push_str(&format!("{days}d"));
            }
            if hours > 0 {
                out.push_str(&format!("{hours}h"));
            }
            if minutes > 0 {
                out.push_str(&format!("{minutes}m"));
            }
            out.push_str(&format!("{secs}s"));
            if out.is_empty() {
                "0s".to_string()
            } else {
                out
            }
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Precision {
    Nanosecond,
    Microsecond,
    Millisecond,
    Second,
}

impl Precision {
    pub fn from_str_opt(s: Option<&str>) -> Self {
        match s {
            Some("ns") | None => Precision::Nanosecond,
            Some("us") | Some("u") => Precision::Microsecond,
            Some("ms") => Precision::Millisecond,
            Some("s") => Precision::Second,
            _ => Precision::Nanosecond,
        }
    }

    pub fn to_nanos(&self, ts: i64) -> i64 {
        match self {
            Precision::Nanosecond => ts,
            Precision::Microsecond => ts * 1_000,
            Precision::Millisecond => ts * 1_000_000,
            Precision::Second => ts * 1_000_000_000,
        }
    }

    pub fn from_nanos(&self, nanos: i64) -> i64 {
        match self {
            Precision::Nanosecond => nanos,
            Precision::Microsecond => nanos / 1_000,
            Precision::Millisecond => nanos / 1_000_000,
            Precision::Second => nanos / 1_000_000_000,
        }
    }
}

mod duration_serde {
    use serde::{Deserialize, Deserializer, Serialize, Serializer};
    use std::time::Duration;

    pub fn serialize<S: Serializer>(d: &Duration, s: S) -> Result<S::Ok, S::Error> {
        d.as_secs().serialize(s)
    }

    pub fn deserialize<'de, D: Deserializer<'de>>(d: D) -> Result<Duration, D::Error> {
        let secs = u64::deserialize(d)?;
        Ok(Duration::from_secs(secs))
    }
}

mod optional_duration_serde {
    use serde::{Deserialize, Deserializer, Serialize, Serializer};
    use std::time::Duration;

    pub fn serialize<S: Serializer>(d: &Option<Duration>, s: S) -> Result<S::Ok, S::Error> {
        d.map(|d| d.as_secs()).serialize(s)
    }

    pub fn deserialize<'de, D: Deserializer<'de>>(d: D) -> Result<Option<Duration>, D::Error> {
        let secs = Option::<u64>::deserialize(d)?;
        Ok(secs.map(Duration::from_secs))
    }
}
