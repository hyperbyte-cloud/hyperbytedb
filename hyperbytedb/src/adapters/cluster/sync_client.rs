use crate::adapters::cluster::cluster_auth::ClusterAuth;
use crate::ports::peer_http::PeerHttp;
use std::sync::Arc;
use std::time::Duration;

use crate::application::ingest_metadata::{IngestCardinalityLimits, prepare_batch_metadata};
use crate::application::wal_append::append_points_with_prepared;
use crate::domain::cluster::membership::{NodeState, SharedMembership};
use crate::domain::cluster::sync::{
    JoinRequest, JoinResponse, MetadataSnapshot, SyncManifest, WalSyncResponse,
};
use crate::domain::sharding::{ShardMap, ShardMapJson};
use crate::error::HyperbytedbError;
use crate::ports::metadata::MetadataPort;
use crate::ports::points_sink::PointsSinkPort;
use crate::ports::sharding::ShardMapPort;
use crate::ports::wal::WalPort;

pub struct SyncClient {
    node_id: u64,
    node_addr: String,
    membership: SharedMembership,
    metadata: Arc<dyn MetadataPort>,
    wal: Arc<dyn WalPort>,
    points_sink: Option<Arc<dyn PointsSinkPort>>,
    max_points_per_request: usize,
    client: reqwest::Client,
    /// Static peer addresses from config, used as fallback when the
    /// membership has no active peers yet (Raft hasn't formed).
    fallback_peer_addrs: Vec<String>,
    shard_map: Option<Arc<dyn ShardMapPort>>,
    runtime_settings: Option<Arc<crate::application::runtime_settings::RuntimeSettings>>,
}

impl SyncClient {
    pub fn new(
        node_id: u64,
        node_addr: String,
        membership: SharedMembership,
        metadata: Arc<dyn MetadataPort>,
        wal: Arc<dyn WalPort>,
        fallback_peer_addrs: Vec<String>,
        cluster_auth: &ClusterAuth,
    ) -> Self {
        Self::with_points_sink(
            node_id,
            node_addr,
            membership,
            metadata,
            wal,
            None,
            0,
            fallback_peer_addrs,
            cluster_auth,
        )
    }

    #[allow(clippy::too_many_arguments)]
    pub fn with_points_sink(
        node_id: u64,
        node_addr: String,
        membership: SharedMembership,
        metadata: Arc<dyn MetadataPort>,
        wal: Arc<dyn WalPort>,
        points_sink: Option<Arc<dyn PointsSinkPort>>,
        max_points_per_request: usize,
        fallback_peer_addrs: Vec<String>,
        cluster_auth: &ClusterAuth,
    ) -> Self {
        let client = cluster_auth.build(
            cluster_auth
                .client_builder()
                .timeout(Duration::from_secs(300)),
        );

        Self {
            node_id,
            node_addr,
            membership,
            metadata,
            wal,
            points_sink,
            max_points_per_request,
            client,
            fallback_peer_addrs,
            shard_map: None,
            runtime_settings: None,
        }
    }

    /// This node's runtime settings, so synced `SET`s apply at once.
    pub fn with_runtime_settings(
        mut self,
        settings: Arc<crate::application::runtime_settings::RuntimeSettings>,
    ) -> Self {
        self.runtime_settings = Some(settings);
        self
    }

    pub fn with_shard_map(mut self, map: Arc<dyn ShardMapPort>) -> Self {
        self.shard_map = Some(map);
        self
    }

    /// Reconnect after a disconnect: detect WAL gap and do WAL catch-up or
    /// a full metadata re-sync when the gap is large.
    ///
    /// Sharded: metadata and the shard map only. A node's WAL holds the writes
    /// it took, numbered by its own sequence, so "the gap" compared unrelated
    /// numbers, and the catch-up copied another node's regions into this
    /// one's store. On n3 that took ~20 minutes per reconnect with the node
    /// Syncing (not ready). A sharded replica gets missed writes from hinted
    /// handoff, and a new copy of a region from a learner stage.
    pub async fn reconnect_sync(&self) -> Result<bool, HyperbytedbError> {
        let peer_addr = self.pick_sync_peer().await;
        let peer_addr = match peer_addr {
            Some(addr) => addr,
            None => {
                tracing::info!("no peers available for reconnect sync");
                return Ok(false);
            }
        };

        tracing::debug!(peer = %peer_addr, "starting reconnect sync");

        if self.shard_map.is_some() {
            self.sync_metadata(&peer_addr).await?;
            self.sync_shard_map(&peer_addr).await?;
            tracing::info!("reconnect sync complete (metadata and shard map), node is Active");
            return Ok(true);
        }

        let manifest = self.get_manifest(&peer_addr).await?;
        let local_wal_seq = self.wal.last_sequence().await?;
        let remote_wal_seq = manifest.wal_last_seq;

        let gap = remote_wal_seq.saturating_sub(local_wal_seq);
        tracing::debug!(
            local_wal_seq = local_wal_seq,
            remote_wal_seq = remote_wal_seq,
            gap = gap,
            "reconnect gap analysis"
        );

        const SMALL_GAP_THRESHOLD: u64 = 10_000;

        if gap == 0 {
            tracing::debug!("no WAL gap, node is current");
        } else if gap <= SMALL_GAP_THRESHOLD {
            tracing::debug!(
                gap = gap,
                "small gap detected, performing WAL catch-up (staying Active)"
            );
            let applied = self.wal_catchup(&peer_addr, local_wal_seq).await?;
            let local_after = self.wal.last_sequence().await?;
            verify_catchup_progress(local_wal_seq, remote_wal_seq, applied, local_after)?;
        } else {
            {
                let mut m = self.membership.write().await;
                m.begin_sync_to_syncing(self.node_id);
            }
            tracing::info!(
                gap = gap,
                "large gap detected, performing metadata sync + WAL catch-up"
            );

            self.sync_metadata(&peer_addr).await?;
            self.sync_shard_map(&peer_addr).await?;

            let updated_wal_seq = self.wal.last_sequence().await?;
            let applied = self.wal_catchup(&peer_addr, updated_wal_seq).await?;
            let local_after = self.wal.last_sequence().await?;
            verify_catchup_progress(updated_wal_seq, remote_wal_seq, applied, local_after)?;

            {
                let mut m = self.membership.write().await;
                m.complete_sync_to_active(self.node_id);
            }
        }

        tracing::info!("reconnect sync complete, node is Active");
        Ok(true)
    }

    /// Perform the full node join / sync protocol.
    /// Returns Ok(true) if sync was needed and performed, Ok(false) if no sync needed.
    pub async fn join_and_sync(&self) -> Result<bool, HyperbytedbError> {
        let peer_addr = self.pick_sync_peer().await;
        let peer_addr = match peer_addr {
            Some(addr) => addr,
            None => {
                tracing::info!("no peers available for sync, starting as first node");
                return Ok(false);
            }
        };

        tracing::info!(peer = %peer_addr, "starting node join and sync");

        if let Err(e) = self.send_join_request(&peer_addr).await {
            tracing::debug!(error = %e, "join request failed (raft leader may add us separately), continuing sync");
        }

        let manifest = self.get_manifest(&peer_addr).await?;
        let local_wal_seq = self.wal.last_sequence().await?;

        tracing::debug!(
            databases = manifest.databases.len(),
            local_wal_seq = local_wal_seq,
            remote_wal_seq = manifest.wal_last_seq,
            "received sync manifest from peer"
        );

        self.sync_metadata(&peer_addr).await?;
        // Install the committed map before any region data movement so the
        // joiner's map_version matches the cluster at Active.
        //
        // Best-effort: `/internal/shard/map` only exists on peers that have
        // sharding enabled and run a build that serves it, so a 404 here says
        // nothing about whether this node can catch up its WAL. Failing hard
        // used to abort the join before `wal_catchup`, and after the retries
        // were exhausted the node went Active having synced nothing at all.
        // Skipping is safe: without a map there are no regions to move below,
        // and the leader's own catch-up check gates placement onto this node.
        if let Err(e) = self.sync_shard_map(&peer_addr).await {
            tracing::warn!(
                error = %e,
                peer = %peer_addr,
                "shard map install failed; continuing with WAL catch-up"
            );
        }

        // Sharded: no WAL catch-up (see `reconnect_sync`). A joiner gets a
        // region's rows when it is added as a learner and staged.
        if self.shard_map.is_none() {
            let applied = self.wal_catchup(&peer_addr, local_wal_seq).await?;
            let local_after = self.wal.last_sequence().await?;
            verify_catchup_progress(local_wal_seq, manifest.wal_last_seq, applied, local_after)?;
        }

        let mut m = self.membership.write().await;
        m.complete_sync_to_active(self.node_id);
        tracing::info!("node sync complete, transitioned to Active");
        Ok(true)
    }

    async fn pick_sync_peer(&self) -> Option<String> {
        let m = self.membership.read().await;
        if let Some(peer) = m.active_peers(self.node_id).first() {
            return Some(peer.addr.clone());
        }
        drop(m);

        for addr in &self.fallback_peer_addrs {
            if addr == &self.node_addr {
                continue;
            }
            let url = format!("http://{}/ping", addr);
            if let Ok(resp) = self.client.get(&url).send().await
                && resp.status().is_success()
            {
                tracing::debug!(peer = %addr, "using fallback peer for sync");
                return Some(addr.clone());
            }
        }
        None
    }

    async fn send_join_request(&self, peer_addr: &str) -> Result<(), HyperbytedbError> {
        let url = format!("http://{}/internal/membership/join", peer_addr);
        let req = JoinRequest {
            node_id: Some(self.node_id),
            addr: self.node_addr.clone(),
        };

        let resp = self
            .client
            .post(&url)
            .json(&req)
            .send()
            .await
            .map_err(|e| HyperbytedbError::PeerUnreachable(format!("join request failed: {e}")))?;

        if !resp.status().is_success() {
            return Err(HyperbytedbError::ClusterUnavailable(format!(
                "join request rejected: {}",
                resp.status()
            )));
        }

        let join_resp: JoinResponse = resp
            .json()
            .await
            .map_err(|e| HyperbytedbError::SyncFailed(format!("parse join response: {e}")))?;

        let mut m = self.membership.write().await;
        *m = join_resp.membership;
        m.set_state(self.node_id, NodeState::Syncing);

        tracing::debug!(
            assigned_id = join_resp.assigned_node_id,
            membership_version = m.version,
            "joined cluster, syncing"
        );

        Ok(())
    }

    async fn get_manifest(&self, peer_addr: &str) -> Result<SyncManifest, HyperbytedbError> {
        let url = format!("http://{}/internal/sync/manifest", peer_addr);
        let resp = self.client.get(&url).send().await.map_err(|e| {
            HyperbytedbError::PeerUnreachable(format!("manifest request failed: {e}"))
        })?;

        if !resp.status().is_success() {
            return Err(HyperbytedbError::SyncFailed(format!(
                "manifest request failed: {}",
                resp.status()
            )));
        }

        resp.json()
            .await
            .map_err(|e| HyperbytedbError::SyncFailed(format!("parse manifest: {e}")))
    }

    async fn sync_metadata(&self, peer_addr: &str) -> Result<(), HyperbytedbError> {
        let url = format!("http://{}/internal/sync/metadata", peer_addr);
        let resp =
            self.client.get(&url).send().await.map_err(|e| {
                HyperbytedbError::PeerUnreachable(format!("metadata sync failed: {e}"))
            })?;

        if !resp.status().is_success() {
            return Err(HyperbytedbError::SyncFailed(format!(
                "metadata sync failed: {}",
                resp.status()
            )));
        }

        let snapshot: MetadataSnapshot = resp
            .json()
            .await
            .map_err(|e| HyperbytedbError::SyncFailed(format!("parse metadata snapshot: {e}")))?;

        tracing::debug!(
            entries = snapshot.entries.len(),
            "importing metadata snapshot"
        );

        for entry in &snapshot.entries {
            self.import_metadata_entry(entry).await?;
        }

        if let Some(sink) = &self.points_sink
            && let Err(e) = sink.refresh_schema_cache().await
        {
            tracing::warn!(
                error = %e,
                "failed to refresh chDB schema cache after metadata sync"
            );
        }

        tracing::debug!("metadata sync complete");
        Ok(())
    }

    /// Copy the peer's committed shard map so `map_version` matches before
    /// region rows are transferred onto this joiner.
    pub async fn sync_shard_map(&self, peer_addr: &str) -> Result<(), HyperbytedbError> {
        let Some(shard_map) = self.shard_map.as_ref() else {
            return Ok(());
        };
        let remote = fetch_shard_map(self.client.clone(), peer_addr).await?;
        let version = remote.map_version;

        // Never move the map backwards. `map_version` counts local applies and
        // the sync peer is whichever Active node came first out of a HashMap —
        // not the leader, and not necessarily the most-applied node. Installing
        // an older snapshot would drop ops Raft will never redeliver, because
        // `replace_map` bypasses the state machine and leaves `last_applied`
        // untouched; every later op would then fail StaleEpoch/UnknownRegion
        // and the node would silently stop owning regions it holds data for.
        let local = shard_map.snapshot().await?.map_version;
        if !remote_map_is_newer(version, local) {
            tracing::info!(
                peer = %peer_addr,
                remote_map_version = version,
                local_map_version = local,
                "peer shard map is not newer; keeping local map"
            );
            return Ok(());
        }

        shard_map.replace_map(remote).await?;
        tracing::info!(
            peer = %peer_addr,
            map_version = version,
            "installed peer shard map before region movement"
        );
        Ok(())
    }

    async fn import_metadata_entry(
        &self,
        entry: &crate::domain::cluster::sync::MetadataEntry,
    ) -> Result<(), HyperbytedbError> {
        use crate::domain::database::Database;
        use crate::ports::metadata::{ContinuousQueryDef, MeasurementMeta, StoredUser};

        if entry.key.starts_with("pq:") {
            tracing::trace!(key = %entry.key, "skipping legacy pq: metadata row");
            return Ok(());
        }

        if let Some(_db_name) = entry.key.strip_prefix("db:") {
            let db: Database = serde_json::from_slice(&entry.value)
                .map_err(|e| HyperbytedbError::Metadata(format!("parse db: {e}").into()))?;
            self.metadata.create_database(&db.name).await?;
            for rp in &db.retention_policies {
                self.metadata
                    .create_retention_policy(&db.name, rp.clone())
                    .await?;
            }
        } else if entry.key.starts_with("meas:") {
            let parts: Vec<&str> = entry.key.splitn(4, ':').collect();
            if parts.len() == 4 {
                let db = parts[1];
                let rp = parts[2];
                let meta: MeasurementMeta = serde_json::from_slice(&entry.value)
                    .map_err(|e| HyperbytedbError::Metadata(format!("parse meas: {e}").into()))?;
                self.metadata.register_measurement(db, rp, &meta).await?;
            }
        } else if let Some(username) = entry.key.strip_prefix("user:") {
            let user: StoredUser = serde_json::from_slice(&entry.value)
                .map_err(|e| HyperbytedbError::Metadata(format!("parse user: {e}").into()))?;
            // Whole, so a node that syncs keeps every user's grants.
            self.metadata.put_user(username, &user).await?;
        } else if entry.key.starts_with("tombstone:") {
            let parts: Vec<&str> = entry.key.splitn(5, ':').collect();
            if parts.len() == 5 {
                let db = parts[1];
                let rp = parts[2];
                let meas = parts[3];
                let predicate = std::str::from_utf8(&entry.value).map_err(|e| {
                    HyperbytedbError::Metadata(
                        format!("invalid UTF-8 in tombstone value for {db}/{rp}/{meas}: {e}")
                            .into(),
                    )
                })?;
                self.metadata
                    .store_tombstone(db, rp, meas, predicate)
                    .await?;
            }
        } else if entry.key.starts_with("cq:") {
            let parts: Vec<&str> = entry.key.splitn(3, ':').collect();
            if parts.len() == 3 {
                let db = parts[1];
                let cq: ContinuousQueryDef = serde_json::from_slice(&entry.value)
                    .map_err(|e| HyperbytedbError::Metadata(format!("parse cq: {e}").into()))?;
                self.metadata
                    .store_continuous_query(db, &cq.name, &cq)
                    .await?;
            }
        } else if let Some(name) = entry.key.strip_prefix("setting:") {
            let value: crate::domain::setting::SettingValue = serde_json::from_slice(&entry.value)
                .map_err(|e| HyperbytedbError::Metadata(format!("parse setting: {e}").into()))?;
            crate::application::runtime_settings::apply_or_store(
                self.runtime_settings.as_deref(),
                self.metadata.as_ref(),
                name,
                Some(value),
            )
            .await?;
        } else if entry.key.starts_with("mv:") {
            let parts: Vec<&str> = entry.key.splitn(3, ':').collect();
            if parts.len() == 3 {
                let db = parts[1];
                let mv: crate::ports::metadata::MaterializedViewDef =
                    serde_json::from_slice(&entry.value)
                        .map_err(|e| HyperbytedbError::Metadata(format!("parse mv: {e}").into()))?;
                self.metadata
                    .store_materialized_view(db, &mv.name, &mv)
                    .await?;
            }
        }

        Ok(())
    }

    async fn wal_catchup(&self, peer_addr: &str, from_seq: u64) -> Result<u64, HyperbytedbError> {
        let mut cursor = from_seq;
        let mut total_entries = 0u64;

        loop {
            let url = format!(
                "http://{}/internal/sync/wal?from_seq={}&max_entries=5000",
                peer_addr,
                cursor + 1
            );

            let resp = self.client.get(&url).send().await.map_err(|e| {
                HyperbytedbError::PeerUnreachable(format!("WAL sync request failed: {e}"))
            })?;

            if !resp.status().is_success() {
                return Err(HyperbytedbError::SyncFailed(format!(
                    "WAL sync failed: {}",
                    resp.status()
                )));
            }

            let wal_resp: WalSyncResponse = resp.json().await.map_err(|e| {
                HyperbytedbError::SyncFailed(format!("parse WAL sync response: {e}"))
            })?;

            if wal_resp.entries.is_empty() {
                break;
            }

            let batch_count = wal_resp.entries.len();
            for entry in &wal_resp.entries {
                self.append_catchup_entry(entry).await?;
            }

            cursor = wal_resp.last_seq;
            total_entries += batch_count as u64;

            tracing::debug!(
                entries = batch_count,
                cursor = cursor,
                "WAL catch-up batch applied"
            );
        }

        tracing::info!(total_entries = total_entries, "WAL catch-up complete");
        Ok(total_entries)
    }

    async fn append_catchup_entry(
        &self,
        entry: &crate::domain::cluster::sync::WalSyncEntry,
    ) -> Result<(), HyperbytedbError> {
        if entry.points.is_empty() {
            return Ok(());
        }

        prepare_batch_metadata(
            &self.metadata,
            &entry.database,
            &entry.retention_policy,
            &entry.points,
            &IngestCardinalityLimits::unlimited(),
            None,
        )
        .await?;

        append_points_with_prepared(
            self.wal.as_ref(),
            self.points_sink.as_ref(),
            &entry.database,
            &entry.retention_policy,
            entry.points.clone(),
            entry.origin_node_id,
            self.max_points_per_request,
        )
        .await?;
        Ok(())
    }
}

/// GET `/internal/shard/map` from `peer_addr` and return the committed snapshot.
/// Whether a peer's shard map may replace the local one.
///
/// Strictly-newer only. Equal is a no-op, and older must be refused: a sync
/// peer is whichever Active node came first out of a `HashMap`, so it is not
/// necessarily the leader or the most-applied node.
#[must_use]
pub fn remote_map_is_newer(remote_version: u64, local_version: u64) -> bool {
    remote_version > local_version
}

pub async fn fetch_shard_map(
    client: reqwest::Client,
    peer_addr: &str,
) -> Result<ShardMap, HyperbytedbError> {
    let url = format!("http://{peer_addr}/internal/shard/map");
    let resp =
        client.get(&url).send().await.map_err(|e| {
            HyperbytedbError::PeerUnreachable(format!("shard map request failed: {e}"))
        })?;
    if !resp.status().is_success() {
        return Err(HyperbytedbError::SyncFailed(format!(
            "shard map request failed: {}",
            resp.status()
        )));
    }
    let json: ShardMapJson = resp
        .json()
        .await
        .map_err(|e| HyperbytedbError::SyncFailed(format!("parse shard map: {e}")))?;
    Ok(ShardMap::from(json))
}

/// Fail closed when the peer advertises a higher WAL watermark but returns no
/// readable entries — usually because the leader truncated past the gap.
fn verify_catchup_progress(
    local_seq_before: u64,
    remote_wal_seq: u64,
    entries_applied: u64,
    local_seq_after: u64,
) -> Result<(), HyperbytedbError> {
    if remote_wal_seq <= local_seq_before {
        return Ok(());
    }
    if entries_applied == 0 && local_seq_after <= local_seq_before {
        return Err(HyperbytedbError::SyncFailed(format!(
            "peer reports wal_last_seq={remote_wal_seq} but returned no WAL entries from seq {}",
            local_seq_before + 1
        )));
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::{remote_map_is_newer, verify_catchup_progress};
    use crate::error::HyperbytedbError;

    /// A peer that answers sync calls and counts WAL catch-up requests.
    async fn spawn_sync_peer(wal_requests: std::sync::Arc<std::sync::atomic::AtomicU64>) -> String {
        use axum::routing::{get, post};
        let app = axum::Router::new()
            .route(
                "/internal/sync/manifest",
                get(|| async {
                    axum::Json(serde_json::json!({
                        "node_id": 2, "wal_last_seq": 500_000, "databases": [],
                        "shard_map_version": 0,
                    }))
                }),
            )
            .route(
                "/internal/sync/metadata",
                get(|| async { axum::Json(serde_json::json!({ "entries": [] })) }),
            )
            .route(
                "/internal/shard/map",
                get(|| async {
                    axum::Json(serde_json::json!({
                        "map_version": 0, "next_region_id": 1, "spaces": [],
                    }))
                }),
            )
            .route(
                "/internal/sync/wal",
                get(move || {
                    let n = wal_requests.clone();
                    async move {
                        n.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
                        axum::http::StatusCode::INTERNAL_SERVER_ERROR
                    }
                }),
            )
            .route(
                "/internal/membership/join",
                post(|| async { axum::http::StatusCode::OK }),
            );
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap().to_string();
        tokio::spawn(async move {
            let _ = axum::serve(listener, app).await;
        });
        addr
    }

    /// A node that syncs gets every user whole. Importing through
    /// `create_user` dropped grants: on n3 all six UAT nodes restarted after
    /// the grants were made, and Telegraf's writes were refused from then on.
    #[tokio::test]
    async fn a_synced_user_keeps_its_grants() {
        use crate::adapters::metadata::rocksdb_meta::RocksDbMetadata;
        use crate::adapters::wal::rocksdb_wal::RocksDbWal;
        use crate::domain::cluster::membership::{ClusterMembership, new_shared};
        use crate::domain::user::{DatabasePrivilege, StoredUser};
        use crate::ports::metadata::MetadataPort;
        use std::sync::Arc;

        let dir = tempfile::tempdir().unwrap();
        let metadata = Arc::new(RocksDbMetadata::open(dir.path().join("meta")).unwrap());
        let client = super::SyncClient::new(
            1,
            "127.0.0.1:1".into(),
            new_shared(ClusterMembership::new()),
            metadata.clone(),
            Arc::new(RocksDbWal::open(dir.path().join("wal")).unwrap()),
            Vec::new(),
            &Default::default(),
        );
        let user = StoredUser {
            password_hash: "hash".into(),
            admin: false,
            created_at: "now".into(),
            privileges: [("telegraf".to_string(), DatabasePrivilege::Write)].into(),
        };
        client
            .import_metadata_entry(&crate::domain::cluster::sync::MetadataEntry {
                key: "user:telegraf".into(),
                value: serde_json::to_vec(&user).unwrap(),
            })
            .await
            .unwrap();
        let got = metadata.get_user("telegraf").await.unwrap().unwrap();
        assert_eq!(
            got.privileges.get("telegraf"),
            Some(&DatabasePrivilege::Write)
        );
        assert_eq!(got.password_hash, "hash");
    }

    /// A sharded node never pulls a peer's WAL to sync: not on reconnect and
    /// not on join. Comparing its own WAL sequence with a peer's compared
    /// unrelated numbers, and copied the peer's regions into this node (~20
    /// minutes per reconnect on n3, with the node not ready meanwhile).
    #[tokio::test]
    async fn a_sharded_node_syncs_without_pulling_a_peer_wal() {
        use crate::adapters::metadata::rocksdb_meta::RocksDbMetadata;
        use crate::adapters::sharding::rocksdb_shard_map::RocksDbShardMap;
        use crate::adapters::wal::rocksdb_wal::RocksDbWal;
        use crate::domain::cluster::membership::{
            ClusterMembership, NodeInfo, NodeState, new_shared,
        };
        use std::sync::Arc;
        use std::sync::atomic::{AtomicU64, Ordering};

        let wal_requests = Arc::new(AtomicU64::new(0));
        let peer = spawn_sync_peer(wal_requests.clone()).await;
        let dir = tempfile::tempdir().unwrap();
        let mut m = ClusterMembership::new();
        m.add_node(NodeInfo {
            node_id: 2,
            addr: peer,
            state: NodeState::Active,
            joined_at: 0,
            last_heartbeat: 0,
            needs_sync: false,
            consecutive_misses: 0,
        });
        std::fs::create_dir_all(dir.path().join("meta")).unwrap();
        let client = super::SyncClient::new(
            1,
            "127.0.0.1:1".into(),
            new_shared(m),
            Arc::new(RocksDbMetadata::open(dir.path().join("meta2")).unwrap()),
            Arc::new(RocksDbWal::open(dir.path().join("wal")).unwrap()),
            Vec::new(),
            &Default::default(),
        )
        .with_shard_map(Arc::new(
            RocksDbShardMap::open(&dir.path().join("meta"), true).unwrap(),
        ));

        assert!(client.reconnect_sync().await.unwrap());
        client.join_and_sync().await.unwrap();
        assert_eq!(wal_requests.load(Ordering::SeqCst), 0);
    }

    #[test]
    fn remote_map_installs_only_when_strictly_newer() {
        assert!(remote_map_is_newer(10, 0), "fresh joiner takes the map");
        assert!(remote_map_is_newer(10, 9));
        assert!(!remote_map_is_newer(10, 10), "equal is a no-op");
        // The case that silently strands a node: syncing from a less-applied
        // peer would drop ops Raft never redelivers.
        assert!(!remote_map_is_newer(8, 10));
    }

    #[test]
    fn verify_catchup_progress_ok_when_caught_up() {
        verify_catchup_progress(5, 5, 0, 5).unwrap();
    }

    #[test]
    fn verify_catchup_progress_ok_when_entries_applied() {
        verify_catchup_progress(0, 4, 2, 2).unwrap();
    }

    #[test]
    fn verify_catchup_progress_err_when_gap_unreadable() {
        let err = verify_catchup_progress(0, 2, 0, 0).unwrap_err();
        assert!(matches!(err, HyperbytedbError::SyncFailed(_)));
    }
}
