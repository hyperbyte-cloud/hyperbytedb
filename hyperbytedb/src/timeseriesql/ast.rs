use serde::{Deserialize, Serialize};

#[derive(Debug, Clone)]
pub enum Statement {
    Select(SelectStatement),
    ShowDatabases,
    ShowMeasurements(ShowMeasurementsStatement),
    ShowTagKeys(ShowTagKeysStatement),
    ShowTagValues(ShowTagValuesStatement),
    ShowFieldKeys(ShowFieldKeysStatement),
    ShowSeries(ShowSeriesStatement),
    CreateDatabase(CreateDatabaseStatement),
    DropDatabase(String),
    DropMeasurement {
        name: String,
        rp: Option<String>,
    },
    DropSeries(DropSeriesStatement),
    ShowRetentionPolicies(String),
    ShowUsers,
    DropUser(String),
    CreateRetentionPolicyStmt {
        name: String,
        db: String,
        duration: Option<Duration>,
        replication: u32,
        shard_duration: Option<Duration>,
        is_default: bool,
    },
    AlterRetentionPolicyStmt {
        name: String,
        db: String,
        duration: Option<Duration>,
        replication: Option<u32>,
        shard_duration: Option<Duration>,
        is_default: Option<bool>,
    },
    DropRetentionPolicyStmt {
        name: String,
        db: String,
    },
    CreateUser {
        username: String,
        password: String,
        admin: bool,
    },
    SetPassword {
        username: String,
        password: String,
    },
    Grant {
        username: String,
        database: Option<String>,
    },
    Revoke {
        username: String,
        database: Option<String>,
    },
    Delete(DeleteStatement),
    CreateContinuousQuery(CreateContinuousQueryStatement),
    ShowContinuousQueries,
    DropContinuousQuery {
        name: String,
        db: String,
    },
    CreateMaterializedView(CreateMaterializedViewStatement),
    ShowMaterializedViews,
    DropMaterializedView {
        name: String,
        db: String,
    },
}

#[derive(Debug, Clone)]
pub struct SelectStatement {
    pub fields: Vec<Field>,
    pub into: Option<Measurement>,
    pub from: Vec<MeasurementSource>,
    pub condition: Option<Expr>,
    pub group_by: Option<GroupBy>,
    pub order_by: Option<OrderBy>,
    pub limit: Option<u64>,
    pub offset: Option<u64>,
    pub slimit: Option<u64>,
    pub soffset: Option<u64>,
    pub fill: Option<FillOption>,
    pub timezone: Option<String>,
}

#[derive(Debug, Clone)]
pub enum MeasurementSource {
    Concrete(Measurement),
    Subquery(Box<SelectStatement>),
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CreateDatabaseStatement {
    pub name: String,
    pub duration: Option<Duration>,
    pub replication: Option<u32>,
    pub shard_duration: Option<Duration>,
    pub rp_name: Option<String>,
}

#[derive(Debug, Clone)]
pub struct DropSeriesStatement {
    pub database: Option<String>,
    pub from: Option<MeasurementName>,
    pub condition: Option<Expr>,
}

/// Partial update for ALTER RETENTION POLICY.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct RetentionPolicyChange {
    pub duration: Option<Option<Duration>>,
    pub replication: Option<u32>,
    pub shard_duration: Option<Duration>,
    pub is_default: Option<bool>,
}

#[derive(Debug, Clone)]
pub struct ShowMeasurementsStatement {
    pub database: Option<String>,
    pub condition: Option<Expr>,
    pub limit: Option<u64>,
    pub offset: Option<u64>,
}

#[derive(Debug, Clone)]
pub struct ShowTagKeysStatement {
    pub database: Option<String>,
    pub from: Option<Measurement>,
    pub condition: Option<Expr>,
    pub limit: Option<u64>,
    pub offset: Option<u64>,
}

#[derive(Debug, Clone)]
pub struct ShowTagValuesStatement {
    pub database: Option<String>,
    pub from: Option<Measurement>,
    pub tag_key: TagKeySelector,
    pub condition: Option<Expr>,
    pub limit: Option<u64>,
    pub offset: Option<u64>,
}

#[derive(Debug, Clone)]
pub enum TagKeySelector {
    All,
    Eq(String),
    Neq(String),
    Regex(String),
    In(Vec<String>),
}

#[derive(Debug, Clone)]
pub struct ShowFieldKeysStatement {
    pub database: Option<String>,
    pub from: Option<Measurement>,
}

#[derive(Debug, Clone)]
pub struct ShowSeriesStatement {
    pub database: Option<String>,
    pub from: Option<Measurement>,
    pub condition: Option<Expr>,
    pub limit: Option<u64>,
    pub offset: Option<u64>,
}

#[derive(Debug, Clone)]
pub struct Field {
    pub expr: Expr,
    pub alias: Option<String>,
}

#[derive(Debug, Clone)]
pub enum Expr {
    Identifier(String),
    Star,
    StringLiteral(String),
    IntegerLiteral(i64),
    FloatLiteral(f64),
    BooleanLiteral(bool),
    DurationLiteral(Duration),
    TimeLiteral(String),
    Regex(String),
    Call(FunctionCall),
    BinaryExpr(Box<BinaryExpr>),
    UnaryExpr(UnaryOp, Box<Expr>),
    Wildcard,
    FieldRef {
        name: String,
        typ: Option<FieldType>,
    },
    Now,
}

#[derive(Debug, Clone)]
pub struct FunctionCall {
    pub name: String,
    pub args: Vec<Expr>,
}

#[derive(Debug, Clone)]
pub struct BinaryExpr {
    pub left: Expr,
    pub op: BinaryOp,
    pub right: Expr,
}

#[derive(Debug, Clone, PartialEq)]
pub enum BinaryOp {
    Add,
    Sub,
    Mul,
    Div,
    Mod,
    Eq,
    Neq,
    Lt,
    Lte,
    Gt,
    Gte,
    And,
    Or,
    RegexMatch,
    RegexNotMatch,
}

#[derive(Debug, Clone)]
pub enum UnaryOp {
    Neg,
    Not,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Duration {
    pub value: i64,
    pub unit: DurationUnit,
}

impl Duration {
    /// Convert to nanoseconds, saturating at `i64::MIN`/`i64::MAX` instead of
    /// overflowing. (The signature is infallible and widely called, so
    /// saturation is used rather than returning an error.)
    pub fn to_nanos(&self) -> i64 {
        let per_unit: i64 = match self.unit {
            DurationUnit::Nanosecond => 1,
            DurationUnit::Microsecond => 1_000,
            DurationUnit::Millisecond => 1_000_000,
            DurationUnit::Second => 1_000_000_000,
            DurationUnit::Minute => 60 * 1_000_000_000,
            DurationUnit::Hour => 3_600 * 1_000_000_000,
            DurationUnit::Day => 86_400 * 1_000_000_000,
            DurationUnit::Week => 7 * 86_400 * 1_000_000_000,
        };
        self.value.saturating_mul(per_unit)
    }

    pub fn to_clickhouse_interval(&self) -> String {
        let (val, unit) = match self.unit {
            DurationUnit::Nanosecond => (self.value, "NANOSECOND"),
            DurationUnit::Microsecond => (self.value, "MICROSECOND"),
            DurationUnit::Millisecond => (self.value, "MILLISECOND"),
            DurationUnit::Second => (self.value, "SECOND"),
            DurationUnit::Minute => (self.value, "MINUTE"),
            DurationUnit::Hour => (self.value, "HOUR"),
            DurationUnit::Day => (self.value, "DAY"),
            DurationUnit::Week => (self.value.saturating_mul(7), "DAY"),
        };
        format!("INTERVAL {val} {unit}")
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum DurationUnit {
    Nanosecond,
    Microsecond,
    Millisecond,
    Second,
    Minute,
    Hour,
    Day,
    Week,
}

#[derive(Debug, Clone)]
pub enum FillOption {
    Null,
    None,
    Previous,
    Linear,
    Value(f64),
}

#[derive(Debug, Clone)]
pub struct GroupBy {
    pub dimensions: Vec<Dimension>,
}

impl GroupBy {
    pub fn time_dimension(&self) -> Option<&Dimension> {
        self.dimensions
            .iter()
            .find(|d| matches!(d, Dimension::Time { .. }))
    }

    pub fn tag_dimensions(&self) -> Vec<&str> {
        self.dimensions
            .iter()
            .filter_map(|d| match d {
                Dimension::Tag(name) => Some(name.as_str()),
                _ => None,
            })
            .collect()
    }

    /// Whether GROUP BY references any tag dimension, including `*` (all tags).
    pub fn references_tags(&self) -> bool {
        self.dimensions.iter().any(|d| {
            matches!(
                d,
                Dimension::AllTags | Dimension::Regex(_) | Dimension::Tag(_)
            )
        })
    }

    /// Expand InfluxQL `GROUP BY *` into explicit tag keys for SQL translation.
    pub fn expand_all_tags(&self, tag_keys: &[String]) -> (Self, Vec<String>) {
        let mut expanded_dims = Vec::new();
        let mut resolved_tags = Vec::new();
        let mut sorted_keys = tag_keys.to_vec();
        sorted_keys.sort();

        for d in &self.dimensions {
            match d {
                Dimension::AllTags => {
                    for key in &sorted_keys {
                        expanded_dims.push(Dimension::Tag(key.clone()));
                        resolved_tags.push(key.clone());
                    }
                }
                Dimension::Tag(name) if name == "*" => {
                    for key in &sorted_keys {
                        expanded_dims.push(Dimension::Tag(key.clone()));
                        resolved_tags.push(key.clone());
                    }
                }
                Dimension::Tag(name) => {
                    expanded_dims.push(d.clone());
                    resolved_tags.push(name.clone());
                }
                other => expanded_dims.push(other.clone()),
            }
        }

        (
            Self {
                dimensions: expanded_dims,
            },
            resolved_tags,
        )
    }
}

#[derive(Debug, Clone)]
pub enum Dimension {
    Time {
        interval: Duration,
        offset: Option<Duration>,
    },
    Tag(String),
    /// InfluxQL `GROUP BY *` — all tag keys on the source measurement.
    AllTags,
    Regex(String),
}

#[derive(Debug, Clone)]
pub struct OrderBy {
    pub time_desc: bool,
}

#[derive(Debug, Clone)]
pub struct Measurement {
    pub database: Option<String>,
    pub retention_policy: Option<String>,
    pub name: MeasurementName,
}

impl Measurement {
    pub fn name_str(&self) -> Option<&str> {
        match &self.name {
            MeasurementName::Name(n) => Some(n.as_str()),
            MeasurementName::Regex(_) => None,
        }
    }
}

impl MeasurementSource {
    pub fn name_str(&self) -> Option<&str> {
        match self {
            MeasurementSource::Concrete(m) => m.name_str(),
            MeasurementSource::Subquery(_) => None,
        }
    }

    pub fn as_concrete(&self) -> Option<&Measurement> {
        match self {
            MeasurementSource::Concrete(m) => Some(m),
            MeasurementSource::Subquery(_) => None,
        }
    }
}

#[derive(Debug, Clone)]
pub enum MeasurementName {
    Name(String),
    Regex(String),
}

#[derive(Debug, Clone)]
pub enum FieldType {
    Field,
    Tag,
}

#[derive(Debug, Clone)]
pub struct DeleteStatement {
    pub from: String,
    pub condition: Option<Expr>,
}

#[derive(Debug, Clone)]
pub struct CreateContinuousQueryStatement {
    pub name: String,
    pub database: String,
    pub query: SelectStatement,
    pub raw_query: String,
    pub resample_every: Option<Duration>,
    pub resample_for: Option<Duration>,
}

#[derive(Debug, Clone)]
pub struct CreateMaterializedViewStatement {
    pub name: String,
    pub database: String,
    pub query: SelectStatement,
    pub raw_query: String,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_duration_to_nanos_saturates_instead_of_overflowing() {
        let d = Duration {
            value: i64::MAX,
            unit: DurationUnit::Week,
        };
        assert_eq!(d.to_nanos(), i64::MAX);

        let d = Duration {
            value: i64::MIN,
            unit: DurationUnit::Hour,
        };
        assert_eq!(d.to_nanos(), i64::MIN);

        let d = Duration {
            value: 2,
            unit: DurationUnit::Hour,
        };
        assert_eq!(d.to_nanos(), 7_200_000_000_000);
    }

    #[test]
    fn test_to_clickhouse_interval_week_saturates() {
        let d = Duration {
            value: i64::MAX,
            unit: DurationUnit::Week,
        };
        assert_eq!(
            d.to_clickhouse_interval(),
            format!("INTERVAL {} DAY", i64::MAX)
        );

        let d = Duration {
            value: 2,
            unit: DurationUnit::Week,
        };
        assert_eq!(d.to_clickhouse_interval(), "INTERVAL 14 DAY");
    }
}
