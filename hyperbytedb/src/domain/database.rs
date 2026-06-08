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
            shard_group_duration: Duration::from_secs(7 * 24 * 3600),
            replication_factor: 1,
            is_default: true,
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
