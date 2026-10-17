//! Settings an operator can change on a running cluster with
//! `SET <name> = <value> | DEFAULT` and read with `SHOW SETTINGS`.
//!
//! A `SET` is an override: stored in metadata (so it survives a restart and
//! reaches a node that joins later), replicated to every node through Raft
//! like a schema change, and in effect on each node at once. `DEFAULT`
//! removes it and the config file's value applies again.
//!
//! Two kinds of consumer. Most read [`usize_or`] / [`u64_or`] / [`bool_or`]
//! where they use the value, with their own config as the fallback, so an
//! instance built from a config (a test, say) is unaffected until a `SET`.
//! Those that hold a resource sized from the value (connection pools,
//! semaphores, the log filter) register an [`Applier`] at startup, which a
//! `SET` calls with the new value.

use std::collections::HashMap;
use std::sync::Arc;

use parking_lot::RwLock;

pub use crate::domain::setting::SettingValue;
use crate::error::HyperbytedbError;
use crate::ports::metadata::MetadataPort;

/// What a setting accepts.
#[derive(Debug, Clone, Copy)]
pub enum Kind {
    Int { min: i64, max: i64 },
    Bool,
    Text,
}

/// One setting `SET` accepts.
#[derive(Debug, Clone, Copy)]
pub struct Def {
    pub name: &'static str,
    pub kind: Kind,
    /// The config key it overrides.
    pub config: &'static str,
    pub description: &'static str,
}

const fn int(
    name: &'static str,
    min: i64,
    max: i64,
    config: &'static str,
    description: &'static str,
) -> Def {
    Def {
        name,
        kind: Kind::Int { min, max },
        config,
        description,
    }
}

const fn flag(name: &'static str, config: &'static str, description: &'static str) -> Def {
    Def {
        name,
        kind: Kind::Bool,
        config,
        description,
    }
}

pub const LOG_LEVEL: &str = "log_level";
pub const QUERY_POOL_SIZE: &str = "query_pool_size";
pub const WRITE_POOL_SIZE: &str = "write_pool_size";
pub const MAX_CONCURRENT_QUERIES: &str = "max_concurrent_queries";
pub const QUERY_TIMEOUT_SECS: &str = "query_timeout_secs";
pub const MAX_INFLIGHT_WRITES: &str = "max_inflight_writes";
pub const RATE_LIMIT_PER_SECOND: &str = "rate_limit_per_second";
pub const MAX_TAG_VALUES_PER_MEASUREMENT: &str = "max_tag_values_per_measurement";
pub const MAX_MEASUREMENTS_PER_DATABASE: &str = "max_measurements_per_database";
pub const FLUSH_INTERVAL_SECS: &str = "flush_interval_secs";
pub const ADD_PEER_PROPOSALS_ENABLED: &str = "add_peer_proposals_enabled";
pub const REMOVE_PEER_PROPOSALS_ENABLED: &str = "remove_peer_proposals_enabled";
pub const PEER_HEAL_ENABLED: &str = "peer_heal_enabled";
pub const SCHEDULE_LIMIT: &str = "schedule_limit";
pub const MOVEMENT_BUDGET_BYTES_PER_SEC: &str = "movement_budget_bytes_per_sec";
pub const REGION_SPLIT_SERIES: &str = "region_split_series";
pub const REGION_MAX_SERIES: &str = "region_max_series";
pub const REGION_MERGE_SERIES: &str = "region_merge_series";
pub const SPLIT_MERGE_INTERVAL_SECS: &str = "split_merge_interval_secs";
pub const BOOTSTRAP_REGIONS_PER_NODE: &str = "bootstrap_regions_per_node";

const BIG: i64 = 1 << 50;

/// Every setting `SET` accepts.
pub const SETTINGS: &[Def] = &[
    Def {
        name: LOG_LEVEL,
        kind: Kind::Text,
        config: "logging.level",
        description: "log filter: a level (info, debug) or directives (hyperbytedb=debug,info)",
    },
    int(
        QUERY_POOL_SIZE,
        1,
        128,
        "chdb.query_pool_size",
        "chDB connections for queries per node",
    ),
    int(
        WRITE_POOL_SIZE,
        1,
        128,
        "chdb.write_pool_size",
        "chDB connections for ingest and flush per node",
    ),
    int(
        MAX_CONCURRENT_QUERIES,
        0,
        100_000,
        "server.max_concurrent_queries",
        "queries a node runs at once (0: unlimited)",
    ),
    int(
        QUERY_TIMEOUT_SECS,
        1,
        86_400,
        "server.query_timeout_secs",
        "longest a query may run",
    ),
    int(
        MAX_INFLIGHT_WRITES,
        1,
        1_000_000,
        "server.max_inflight_writes",
        "client write requests a node admits at once; more get 503",
    ),
    int(
        RATE_LIMIT_PER_SECOND,
        0,
        BIG,
        "rate_limit.max_requests_per_second",
        "requests per second per endpoint (0: unlimited)",
    ),
    int(
        MAX_TAG_VALUES_PER_MEASUREMENT,
        0,
        BIG,
        "cardinality.max_tag_values_per_measurement",
        "distinct tag values a measurement may hold (0: unlimited)",
    ),
    int(
        MAX_MEASUREMENTS_PER_DATABASE,
        0,
        BIG,
        "cardinality.max_measurements_per_database",
        "measurements a database may hold (0: unlimited)",
    ),
    int(
        FLUSH_INTERVAL_SECS,
        1,
        3_600,
        "flush.interval_secs",
        "how often the WAL is flushed to chDB",
    ),
    flag(
        ADD_PEER_PROPOSALS_ENABLED,
        "sharding.add_peer_proposals_enabled",
        "the scheduler may add region peers (off during a rolling upgrade)",
    ),
    flag(
        REMOVE_PEER_PROPOSALS_ENABLED,
        "sharding.remove_peer_proposals_enabled",
        "the scheduler may remove region peers (off during a rolling upgrade)",
    ),
    flag(
        PEER_HEAL_ENABLED,
        "sharding.peer_heal_enabled",
        "the scheduler replaces dead region peers",
    ),
    int(
        SCHEDULE_LIMIT,
        1,
        256,
        "sharding.schedule_limit",
        "region copies that run at once",
    ),
    int(
        MOVEMENT_BUDGET_BYTES_PER_SEC,
        0,
        BIG,
        "sharding.movement_budget_bytes_per_sec",
        "cluster-wide bytes per second for balancing copies (0: unlimited)",
    ),
    int(
        REGION_SPLIT_SERIES,
        1,
        BIG,
        "sharding.region_split_series",
        "series at which a region splits",
    ),
    int(
        REGION_MAX_SERIES,
        1,
        BIG,
        "sharding.region_max_series",
        "series at which a region splits regardless of cooldown",
    ),
    int(
        REGION_MERGE_SERIES,
        0,
        BIG,
        "sharding.region_merge_series",
        "series below which neighbouring regions merge",
    ),
    int(
        SPLIT_MERGE_INTERVAL_SECS,
        1,
        86_400,
        "sharding.split_merge_interval_secs",
        "least time between a region's splits",
    ),
    int(
        BOOTSTRAP_REGIONS_PER_NODE,
        0,
        16,
        "sharding.bootstrap_regions_per_node",
        "regions per node a new measurement starts with (0: one region that splits as it grows)",
    ),
];

/// Where a setting's current value comes from.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Source {
    /// `SET`.
    Set,
    /// The config file (or its default).
    Config,
    /// Derived at startup (for the query pool: the CPUs).
    Auto,
}

impl Source {
    #[must_use]
    pub fn as_str(self) -> &'static str {
        match self {
            Source::Set => "set",
            Source::Config => "config",
            Source::Auto => "auto",
        }
    }
}

/// Makes a resource match a new value (the override, or the fallback when
/// the override is removed).
pub type Applier = Arc<dyn Fn(&SettingValue) -> Result<(), HyperbytedbError> + Send + Sync>;

#[derive(Default)]
struct Registry {
    overrides: HashMap<&'static str, SettingValue>,
    fallbacks: HashMap<&'static str, (SettingValue, Source)>,
    appliers: HashMap<&'static str, Applier>,
}

/// One node's runtime settings: the `SET` overrides, the values to fall
/// back to, and the appliers that make a resource follow a change. Built
/// at startup and handed to what reads or changes settings, so nodes run
/// in one process (tests) each have their own.
#[derive(Default)]
pub struct RuntimeSettings {
    registry: RwLock<Registry>,
}

fn def(name: &str) -> Option<&'static Def> {
    SETTINGS.iter().find(|d| d.name == name)
}

/// An applier for a count: `f` gets the value as a `usize`.
pub fn usize_applier(f: impl Fn(usize) + Send + Sync + 'static) -> Applier {
    Arc::new(move |v| {
        let n = v
            .as_i64()
            .and_then(|n| usize::try_from(n).ok())
            .ok_or_else(|| {
                HyperbytedbError::Internal(format!("not a count: {}", v.to_json()).into())
            })?;
        f(n);
        Ok(())
    })
}

/// Grow or shrink a semaphore that holds `from` permits to `to`. Permits in
/// use when it shrinks are taken back as their holders release them.
pub fn resize_semaphore(sem: &Arc<tokio::sync::Semaphore>, from: usize, to: usize) {
    if to > from {
        sem.add_permits(to - from);
    } else if from > to {
        let want = from - to;
        let forgotten = sem.forget_permits(want);
        if forgotten < want {
            let sem = Arc::clone(sem);
            let rest = u32::try_from(want - forgotten).unwrap_or(u32::MAX);
            tokio::spawn(async move {
                if let Ok(permits) = sem.acquire_many_owned(rest).await {
                    permits.forget();
                }
            });
        }
    }
}

/// Check a `SET`: a known setting, and a value of its kind and range.
pub fn validate(
    name: &str,
    value: Option<SettingValue>,
) -> Result<Option<SettingValue>, HyperbytedbError> {
    let Some(d) = def(name) else {
        let names: Vec<&str> = SETTINGS.iter().map(|d| d.name).collect();
        return Err(HyperbytedbError::QueryParse(format!(
            "unknown setting {name:?}; settings: {}",
            names.join(", ")
        )));
    };
    let Some(value) = value else {
        return Ok(None);
    };
    let ok = match (d.kind, &value) {
        (Kind::Int { min, max }, SettingValue::Int(v)) => (min..=max).contains(v),
        (Kind::Bool, SettingValue::Bool(_)) => true,
        (Kind::Text, SettingValue::Text(t)) => {
            d.name != LOG_LEVEL || tracing_subscriber::EnvFilter::try_new(t).is_ok()
        }
        _ => false,
    };
    if ok {
        return Ok(Some(value));
    }
    let want = match d.kind {
        Kind::Int { min, max } => format!("an integer from {min} to {max}"),
        Kind::Bool => "TRUE or FALSE".to_string(),
        Kind::Text => "a valid log filter in quotes".to_string(),
    };
    Err(HyperbytedbError::QueryParse(format!(
        "{name} takes {want}, got {}",
        value.to_json()
    )))
}

/// One `SHOW SETTINGS` row.
#[derive(Debug, Clone, PartialEq)]
pub struct Row {
    pub name: &'static str,
    /// `None` when this node has no value for it (a sharding setting on a
    /// node without sharding).
    pub value: Option<SettingValue>,
    pub source: Option<Source>,
    pub config: &'static str,
    pub description: &'static str,
}

/// Store a `SET` and, with this node's `settings`, apply it. Without them
/// (a component built without settings) it is only stored, and takes
/// effect when the node next starts.
pub async fn apply_or_store(
    settings: Option<&RuntimeSettings>,
    metadata: &dyn MetadataPort,
    name: &str,
    value: Option<SettingValue>,
) -> Result<(), HyperbytedbError> {
    match settings {
        Some(settings) => settings.apply(metadata, name, value).await,
        None => {
            tracing::warn!(
                setting = name,
                "SET stored but not applied here: no runtime settings; it applies at the next start"
            );
            metadata.set_setting(name, value).await
        }
    }
}

impl RuntimeSettings {
    /// The value this node uses without an override, and where it comes from,
    /// for `SHOW SETTINGS` and for appliers when an override is removed.
    pub fn set_fallback(&self, name: &'static str, value: SettingValue, source: Source) {
        self.registry
            .write()
            .fallbacks
            .insert(name, (value, source));
    }

    /// Register the applier for a setting that holds a resource; a later call
    /// replaces it.
    pub fn register_applier(&self, name: &'static str, applier: Applier) {
        self.registry.write().appliers.insert(name, applier);
    }

    /// The override for an integer setting, if one is set.
    #[must_use]
    pub fn override_i64(&self, name: &str) -> Option<i64> {
        self.registry
            .read()
            .overrides
            .get(name)
            .and_then(SettingValue::as_i64)
    }

    /// The override for a boolean setting, if one is set.
    #[must_use]
    pub fn override_bool(&self, name: &str) -> Option<bool> {
        self.registry
            .read()
            .overrides
            .get(name)
            .and_then(SettingValue::as_bool)
    }

    /// `value`, or the override for `name` if one is set.
    #[must_use]
    pub fn usize_or(&self, name: &str, value: usize) -> usize {
        self.override_i64(name)
            .map_or(value, |v| usize::try_from(v.max(0)).unwrap_or(usize::MAX))
    }

    /// `value`, or the override for `name` if one is set.
    #[must_use]
    pub fn u64_or(&self, name: &str, value: u64) -> u64 {
        self.override_i64(name)
            .map_or(value, |v| u64::try_from(v.max(0)).unwrap_or(0))
    }

    /// `value`, or the override for `name` if one is set.
    #[must_use]
    pub fn bool_or(&self, name: &str, value: bool) -> bool {
        self.override_bool(name).unwrap_or(value)
    }

    /// Make `value` (or the fallback, for `None`) this node's value, without
    /// storing it.
    pub(crate) fn apply_runtime(
        &self,
        name: &str,
        value: Option<SettingValue>,
    ) -> Result<(), HyperbytedbError> {
        let Some(d) = def(name) else {
            return Ok(());
        };
        let (effective, applier) = {
            let mut reg = self.registry.write();
            let effective = match value {
                Some(v) => {
                    reg.overrides.insert(d.name, v.clone());
                    Some(v)
                }
                None => {
                    reg.overrides.remove(d.name);
                    reg.fallbacks.get(d.name).map(|(v, _)| v.clone())
                }
            };
            (effective, reg.appliers.get(d.name).cloned())
        };
        if let (Some(v), Some(applier)) = (effective, applier) {
            applier(&v)?;
        }
        Ok(())
    }

    /// Store and apply a `SET` on this node. Every node runs this for a
    /// replicated `SET`.
    pub async fn apply(
        &self,
        metadata: &dyn MetadataPort,
        name: &str,
        value: Option<SettingValue>,
    ) -> Result<(), HyperbytedbError> {
        metadata.set_setting(name, value.clone()).await?;
        self.apply_runtime(name, value)
    }

    /// Apply the stored overrides at startup, after the appliers are registered.
    pub async fn restore(&self, metadata: &dyn MetadataPort) -> Result<(), HyperbytedbError> {
        for d in SETTINGS {
            if let Some(value) = metadata.get_setting(d.name).await? {
                tracing::info!(setting = d.name, value = ?value, "applying stored setting");
                if let Err(e) = self.apply_runtime(d.name, Some(value)) {
                    tracing::warn!(setting = d.name, error = %e, "stored setting not applied");
                }
            }
        }
        Ok(())
    }

    /// `SHOW SETTINGS`: every setting with this node's value and its source.
    pub async fn show(&self, metadata: &dyn MetadataPort) -> Result<Vec<Row>, HyperbytedbError> {
        let mut rows = Vec::with_capacity(SETTINGS.len());
        for d in SETTINGS {
            let (value, source) = match metadata.get_setting(d.name).await? {
                Some(v) => (Some(v), Some(Source::Set)),
                None => match self.registry.read().fallbacks.get(d.name) {
                    Some((v, s)) => (Some(v.clone()), Some(*s)),
                    None => (None, None),
                },
            };
            rows.push(Row {
                name: d.name,
                value,
                source,
                config: d.config,
                description: d.description,
            });
        }
        Ok(rows)
    }

    /// Record the config file's values, for `SHOW SETTINGS` and for removing
    /// an override. The pools and the log level are recorded where they are
    /// resolved.
    pub fn set_config_fallbacks(&self, config: &crate::config::HyperbytedbConfig) {
        let i = |v: u64| SettingValue::Int(i64::try_from(v).unwrap_or(i64::MAX));
        let c = Source::Config;
        self.set_fallback(
            MAX_CONCURRENT_QUERIES,
            i(config.server.max_concurrent_queries as u64),
            c,
        );
        self.set_fallback(QUERY_TIMEOUT_SECS, i(config.server.query_timeout_secs), c);
        let rate = if config.rate_limit.enabled {
            config.rate_limit.max_requests_per_second
        } else {
            0
        };
        self.set_fallback(RATE_LIMIT_PER_SECOND, i(rate), c);
        self.set_fallback(
            MAX_TAG_VALUES_PER_MEASUREMENT,
            i(config.cardinality.max_tag_values_per_measurement as u64),
            c,
        );
        self.set_fallback(
            MAX_MEASUREMENTS_PER_DATABASE,
            i(config.cardinality.max_measurements_per_database as u64),
            c,
        );
        self.set_fallback(FLUSH_INTERVAL_SECS, i(config.flush.interval_secs), c);
        if config.sharding.enabled {
            let s = &config.sharding;
            let b = SettingValue::Bool;
            self.set_fallback(
                ADD_PEER_PROPOSALS_ENABLED,
                b(s.add_peer_proposals_enabled),
                c,
            );
            self.set_fallback(
                REMOVE_PEER_PROPOSALS_ENABLED,
                b(s.remove_peer_proposals_enabled),
                c,
            );
            self.set_fallback(PEER_HEAL_ENABLED, b(s.peer_heal_enabled), c);
            self.set_fallback(SCHEDULE_LIMIT, i(s.schedule_limit as u64), c);
            self.set_fallback(
                MOVEMENT_BUDGET_BYTES_PER_SEC,
                i(s.movement_budget_bytes_per_sec),
                c,
            );
            self.set_fallback(REGION_SPLIT_SERIES, i(s.region_split_series), c);
            self.set_fallback(REGION_MAX_SERIES, i(s.region_max_series), c);
            self.set_fallback(REGION_MERGE_SERIES, i(s.region_merge_series), c);
            self.set_fallback(SPLIT_MERGE_INTERVAL_SECS, i(s.split_merge_interval_secs), c);
            self.set_fallback(
                BOOTSTRAP_REGIONS_PER_NODE,
                i(s.bootstrap_regions_per_node as u64),
                c,
            );
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// A shrink below the permits in use takes them back as they are
    /// released, so the limit holds without cancelling anyone.
    #[tokio::test]
    async fn a_semaphore_resizes_around_permits_in_use() {
        let sem = Arc::new(tokio::sync::Semaphore::new(4));
        resize_semaphore(&sem, 4, 6);
        assert_eq!(sem.available_permits(), 6);
        let held = Arc::clone(&sem).acquire_many_owned(5).await.unwrap();
        resize_semaphore(&sem, 6, 2);
        assert_eq!(sem.available_permits(), 0);
        drop(held);
        for _ in 0..100 {
            if sem.available_permits() == 2 {
                break;
            }
            tokio::task::yield_now().await;
        }
        assert_eq!(sem.available_permits(), 2);
    }

    #[test]
    fn only_known_settings_of_the_right_kind_validate() {
        let int = |v| Some(SettingValue::Int(v));
        let text = |v: &str| Some(SettingValue::Text(v.into()));
        assert!(validate(QUERY_POOL_SIZE, int(8)).is_ok());
        assert!(validate(QUERY_POOL_SIZE, None).is_ok());
        assert!(validate(QUERY_POOL_SIZE, int(0)).is_err());
        assert!(validate(QUERY_POOL_SIZE, int(129)).is_err());
        assert!(validate(QUERY_POOL_SIZE, Some(SettingValue::Bool(true))).is_err());
        assert!(validate(PEER_HEAL_ENABLED, Some(SettingValue::Bool(false))).is_ok());
        assert!(validate(PEER_HEAL_ENABLED, int(1)).is_err());
        assert!(validate(LOG_LEVEL, text("hyperbytedb=debug,info")).is_ok());
        assert!(validate(LOG_LEVEL, text("hyperbytedb=loud")).is_err());
        assert!(validate("max_threads", int(4)).is_err());
    }

    /// Two nodes in one process (the sharded test harness) each have their
    /// own settings: a SET on one resizes only its own pool. With one
    /// process-wide registry the last node to register won every applier.
    #[test]
    fn each_node_has_its_own_settings() {
        use std::sync::atomic::{AtomicUsize, Ordering};
        let (a, b) = (RuntimeSettings::default(), RuntimeSettings::default());
        let (pool_a, pool_b) = (Arc::new(AtomicUsize::new(2)), Arc::new(AtomicUsize::new(2)));
        for (settings, pool) in [(&a, &pool_a), (&b, &pool_b)] {
            let pool = Arc::clone(pool);
            settings.register_applier(
                QUERY_POOL_SIZE,
                usize_applier(move |n| pool.store(n, Ordering::SeqCst)),
            );
        }
        a.apply_runtime(QUERY_POOL_SIZE, Some(SettingValue::Int(5)))
            .unwrap();
        assert_eq!(pool_a.load(Ordering::SeqCst), 5);
        assert_eq!(pool_b.load(Ordering::SeqCst), 2);
        assert_eq!(a.usize_or(QUERY_POOL_SIZE, 2), 5);
        assert_eq!(b.usize_or(QUERY_POOL_SIZE, 2), 2);
    }

    /// Without settings a SET is stored, so the node applies it at its next
    /// start, rather than lost.
    #[tokio::test]
    async fn a_set_without_settings_is_stored() {
        let dir = tempfile::tempdir().unwrap();
        let meta =
            crate::adapters::metadata::rocksdb_meta::RocksDbMetadata::open(dir.path()).unwrap();
        apply_or_store(None, &meta, QUERY_POOL_SIZE, Some(SettingValue::Int(4)))
            .await
            .unwrap();
        assert_eq!(
            meta.get_setting(QUERY_POOL_SIZE).await.unwrap(),
            Some(SettingValue::Int(4))
        );
        let later = RuntimeSettings::default();
        later.restore(&meta).await.unwrap();
        assert_eq!(later.usize_or(QUERY_POOL_SIZE, 2), 4);
    }

    #[test]
    fn every_setting_name_is_unique_and_a_plain_identifier() {
        let mut names: Vec<&str> = SETTINGS.iter().map(|d| d.name).collect();
        names.sort_unstable();
        names.dedup();
        assert_eq!(names.len(), SETTINGS.len());
        assert!(
            names
                .iter()
                .all(|n| n.chars().all(|c| c.is_ascii_lowercase() || c == '_'))
        );
    }
}
