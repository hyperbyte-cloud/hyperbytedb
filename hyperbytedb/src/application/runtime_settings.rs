//! Settings an operator can change on a running cluster with
//! `SET <name> = <value> | DEFAULT` and read with `SHOW SETTINGS`.
//!
//! An override is stored in metadata (and so survives a restart and reaches
//! a node that joins later), replicated to every node through Raft like a
//! schema change, and applied on each node at once. `DEFAULT` removes it, and
//! the value falls back to the config file, then to the automatic default.

use std::sync::{Arc, Weak};

use parking_lot::RwLock;

use crate::adapters::chdb::connection_pool::{ChdbConnectionPool, MAX_POOL_SIZE, MIN_POOL_SIZE};
use crate::error::HyperbytedbError;
use crate::ports::metadata::MetadataPort;

/// chDB connections for queries. See [`crate::adapters::chdb::connection_pool::default_query_pool_size`].
pub const QUERY_POOL_SIZE: &str = "query_pool_size";

/// Every setting `SET` accepts.
pub const SETTINGS: &[&str] = &[QUERY_POOL_SIZE];

/// Where a setting's current value comes from.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Source {
    /// `SET`.
    Set,
    /// The config file.
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

struct QueryPool {
    /// Weak: the settings never keep a pool (and its chDB data path) alive.
    pool: Weak<ChdbConnectionPool>,
    /// The size without an override, and where it comes from.
    fallback: usize,
    fallback_source: Source,
}

static QUERY_POOL: RwLock<Option<QueryPool>> = RwLock::new(None);

/// Hand the query pool to the settings, with the size it has when no
/// override is set. Called at startup; a later call replaces it.
pub fn register_query_pool(
    pool: &Arc<ChdbConnectionPool>,
    fallback: usize,
    fallback_source: Source,
) {
    *QUERY_POOL.write() = Some(QueryPool {
        pool: Arc::downgrade(pool),
        fallback,
        fallback_source,
    });
}

/// The registered query pool, its fallback size and source, if it is alive.
fn query_pool() -> Option<(Arc<ChdbConnectionPool>, usize, Source)> {
    let guard = QUERY_POOL.read();
    let qp = guard.as_ref()?;
    Some((qp.pool.upgrade()?, qp.fallback, qp.fallback_source))
}

/// Check a `SET`: a known setting, and a value it can take.
pub fn validate(name: &str, value: Option<i64>) -> Result<Option<u64>, HyperbytedbError> {
    if !SETTINGS.contains(&name) {
        return Err(HyperbytedbError::QueryParse(format!(
            "unknown setting {name:?}; settings: {}",
            SETTINGS.join(", ")
        )));
    }
    match value {
        None => Ok(None),
        Some(v) if (MIN_POOL_SIZE as i64..=MAX_POOL_SIZE as i64).contains(&v) => Ok(Some(v as u64)),
        Some(v) => Err(HyperbytedbError::QueryParse(format!(
            "{name} must be between {MIN_POOL_SIZE} and {MAX_POOL_SIZE}, got {v}"
        ))),
    }
}

/// Make `value` this node's effective value, without storing it.
fn apply_runtime(name: &str, value: Option<u64>) -> Result<(), HyperbytedbError> {
    if name == QUERY_POOL_SIZE
        && let Some((pool, fallback, _)) = query_pool()
    {
        pool.resize(value.map_or(fallback, |v| v as usize))?;
    }
    Ok(())
}

/// Store and apply a `SET` on this node. Every node runs this for a
/// replicated `SET`.
pub async fn apply(
    metadata: &dyn MetadataPort,
    name: &str,
    value: Option<u64>,
) -> Result<(), HyperbytedbError> {
    metadata.set_setting(name, value).await?;
    apply_runtime(name, value)
}

/// Apply the stored overrides at startup.
pub async fn restore(metadata: &dyn MetadataPort) -> Result<(), HyperbytedbError> {
    for name in SETTINGS {
        if let Some(value) = metadata.get_setting(name).await? {
            tracing::info!(setting = name, value, "applying stored setting");
            apply_runtime(name, Some(value))?;
        }
    }
    Ok(())
}

/// `SHOW SETTINGS` rows: name, value, source.
pub async fn show(
    metadata: &dyn MetadataPort,
) -> Result<Vec<(String, u64, Source)>, HyperbytedbError> {
    let mut rows = Vec::new();
    for name in SETTINGS {
        let stored = metadata.get_setting(name).await?;
        if *name == QUERY_POOL_SIZE
            && let Some((pool, _, fallback_source)) = query_pool()
        {
            let source = if stored.is_some() {
                Source::Set
            } else {
                fallback_source
            };
            rows.push((name.to_string(), pool.pool_size() as u64, source));
        } else if let Some(v) = stored {
            rows.push((name.to_string(), v, Source::Set));
        }
    }
    Ok(rows)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn only_known_settings_in_range_validate() {
        assert_eq!(validate(QUERY_POOL_SIZE, Some(8)).unwrap(), Some(8));
        assert_eq!(validate(QUERY_POOL_SIZE, None).unwrap(), None);
        assert!(validate(QUERY_POOL_SIZE, Some(0)).is_err());
        assert!(validate(QUERY_POOL_SIZE, Some(129)).is_err());
        assert!(validate(QUERY_POOL_SIZE, Some(-1)).is_err());
        assert!(validate("max_threads", Some(4)).is_err());
    }
}
