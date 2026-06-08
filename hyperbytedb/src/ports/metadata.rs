use std::collections::BTreeMap;

use async_trait::async_trait;

use crate::domain::database::{Database, RetentionPolicy};
use crate::error::HyperbytedbError;

pub use crate::domain::continuous_query::ContinuousQueryDef;
pub use crate::domain::measurement::MeasurementMeta;
pub use crate::domain::user::StoredUser;

#[async_trait]
pub trait MetadataPort: Send + Sync {
    // Database operations
    async fn create_database(&self, name: &str) -> Result<(), HyperbytedbError>;
    async fn drop_database(&self, name: &str) -> Result<(), HyperbytedbError>;
    async fn list_databases(&self) -> Result<Vec<Database>, HyperbytedbError>;
    async fn get_database(&self, name: &str) -> Result<Option<Database>, HyperbytedbError>;

    // Retention policies
    async fn create_retention_policy(
        &self,
        db: &str,
        rp: RetentionPolicy,
    ) -> Result<(), HyperbytedbError>;
    async fn get_default_rp(&self, db: &str) -> Result<String, HyperbytedbError>;

    // Measurement metadata
    async fn register_measurement(
        &self,
        db: &str,
        measurement: &MeasurementMeta,
    ) -> Result<(), HyperbytedbError>;
    async fn get_measurement(
        &self,
        db: &str,
        name: &str,
    ) -> Result<Option<MeasurementMeta>, HyperbytedbError>;
    async fn list_measurements(&self, db: &str) -> Result<Vec<String>, HyperbytedbError>;

    // Field type validation
    async fn check_field_types(
        &self,
        db: &str,
        measurement: &str,
        fields: &[(String, u8)],
    ) -> Result<(), HyperbytedbError>;

    // Tag metadata
    async fn list_tag_keys(
        &self,
        db: &str,
        measurement: Option<&str>,
    ) -> Result<Vec<String>, HyperbytedbError>;
    async fn list_tag_values(
        &self,
        db: &str,
        tag_key: &str,
        measurement: Option<&str>,
    ) -> Result<Vec<String>, HyperbytedbError>;

    /// Distinct tag value count for `SHOW TAG VALUES` cardinality and DDL.
    /// Implementations should prefer an O(1) counter when available.
    async fn count_tag_values(
        &self,
        db: &str,
        tag_key: &str,
        measurement: Option<&str>,
    ) -> Result<usize, HyperbytedbError> {
        Ok(self.list_tag_values(db, tag_key, measurement).await?.len())
    }

    /// Whether `(tag_key, tag_value)` is already recorded for the measurement.
    async fn tag_value_is_known(
        &self,
        db: &str,
        measurement: &str,
        tag_key: &str,
        tag_value: &str,
    ) -> Result<bool, HyperbytedbError> {
        Ok(self
            .list_tag_values(db, tag_key, Some(measurement))
            .await?
            .iter()
            .any(|v| v == tag_value))
    }

    /// Populate in-memory tag value counters from durable metadata (startup).
    async fn warm_tag_value_counts(&self) -> Result<usize, HyperbytedbError> {
        let _ = self;
        Ok(0)
    }

    async fn store_tag_value(
        &self,
        db: &str,
        measurement: &str,
        tag_key: &str,
        tag_value: &str,
    ) -> Result<(), HyperbytedbError>;

    /// Persist multiple distinct `(tag_key, tag_value)` pairs for `SHOW TAG VALUES` (single RocksDB write batch).
    async fn store_tag_values_batch(
        &self,
        db: &str,
        measurement: &str,
        entries: &[(String, String)],
    ) -> Result<(), HyperbytedbError>;

    /// Batch-register multiple measurements and their tag values in a single write.
    /// Default falls back to individual calls; RocksDB overrides with a single WriteBatch.
    async fn register_metadata_batch(
        &self,
        db: &str,
        measurements: &[MeasurementMeta],
        tag_entries: &[(String, Vec<(String, String)>)],
    ) -> Result<(), HyperbytedbError> {
        for m in measurements {
            self.register_measurement(db, m).await?;
        }
        for (meas, tags) in tag_entries {
            self.store_tag_values_batch(db, meas, tags).await?;
        }
        Ok(())
    }

    // Series metadata (series_id -> tags dimension table).
    //
    // Registration is local-deterministic — every node computes the same
    // `series_id` hash from the tag set, so this is never routed through Raft.
    // The map mirrors the chDB `<db>_<rp>_<measurement>_series` table.

    /// Register newly-seen series for a `(db, rp, measurement)` table. Idempotent:
    /// already-known `series_id`s are skipped.
    async fn register_series_batch(
        &self,
        db: &str,
        rp: &str,
        measurement: &str,
        series: &[(u64, BTreeMap<String, String>)],
    ) -> Result<(), HyperbytedbError> {
        let _ = (db, rp, measurement, series);
        Ok(())
    }

    /// All registered series for `(db, rp, measurement)` as `(series_id, tags)`.
    async fn list_series(
        &self,
        db: &str,
        rp: &str,
        measurement: &str,
    ) -> Result<Vec<(u64, BTreeMap<String, String>)>, HyperbytedbError> {
        let _ = (db, rp, measurement);
        Ok(Vec::new())
    }

    /// Resolve a single `series_id` to its tag set.
    async fn get_series(
        &self,
        db: &str,
        rp: &str,
        measurement: &str,
        series_id: u64,
    ) -> Result<Option<BTreeMap<String, String>>, HyperbytedbError> {
        Ok(self
            .list_series(db, rp, measurement)
            .await?
            .into_iter()
            .find(|(id, _)| *id == series_id)
            .map(|(_, tags)| tags))
    }

    /// Populate the in-memory series dedup set from durable metadata (startup).
    async fn warm_series(&self) -> Result<usize, HyperbytedbError> {
        let _ = self;
        Ok(0)
    }

    async fn list_retention_policies(
        &self,
        db: &str,
    ) -> Result<Vec<RetentionPolicy>, HyperbytedbError>;

    async fn drop_retention_policy(&self, db: &str, name: &str) -> Result<(), HyperbytedbError>;

    // User management
    async fn create_user(
        &self,
        username: &str,
        password_hash: &str,
        admin: bool,
    ) -> Result<(), HyperbytedbError>;
    async fn drop_user(&self, username: &str) -> Result<(), HyperbytedbError>;
    async fn get_user(&self, username: &str) -> Result<Option<StoredUser>, HyperbytedbError>;
    async fn list_users(&self) -> Result<Vec<String>, HyperbytedbError>;

    async fn grant_privilege(
        &self,
        username: &str,
        database: &str,
        privilege: crate::domain::user::DatabasePrivilege,
    ) -> Result<(), HyperbytedbError>;
    async fn revoke_privilege(
        &self,
        username: &str,
        database: &str,
    ) -> Result<(), HyperbytedbError>;

    // Measurement deletion
    async fn delete_measurement(&self, db: &str, name: &str) -> Result<(), HyperbytedbError>;

    // Tombstone management (for DELETE statements)
    async fn store_tombstone(
        &self,
        db: &str,
        measurement: &str,
        predicate_sql: &str,
    ) -> Result<String, HyperbytedbError>;
    async fn list_tombstones(
        &self,
        db: &str,
        measurement: &str,
    ) -> Result<Vec<(String, String)>, HyperbytedbError>;
    async fn remove_tombstone(&self, db: &str, tombstone_id: &str) -> Result<(), HyperbytedbError>;

    // Continuous query management
    async fn store_continuous_query(
        &self,
        db: &str,
        name: &str,
        definition: &ContinuousQueryDef,
    ) -> Result<(), HyperbytedbError>;
    async fn get_continuous_query(
        &self,
        db: &str,
        name: &str,
    ) -> Result<Option<ContinuousQueryDef>, HyperbytedbError>;
    async fn list_continuous_queries(
        &self,
        db: &str,
    ) -> Result<Vec<ContinuousQueryDef>, HyperbytedbError>;
    async fn list_all_continuous_queries(
        &self,
    ) -> Result<Vec<ContinuousQueryDef>, HyperbytedbError>;
    async fn drop_continuous_query(&self, db: &str, name: &str) -> Result<(), HyperbytedbError>;
}
