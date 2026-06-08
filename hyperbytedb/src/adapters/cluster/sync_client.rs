use std::sync::Arc;
use std::time::Duration;

use crate::domain::cluster::membership::{NodeState, SharedMembership};
use crate::domain::cluster::sync::{
    JoinRequest, JoinResponse, MetadataSnapshot, SyncManifest, WalSyncResponse,
};
use crate::error::HyperbytedbError;
use crate::ports::metadata::MetadataPort;
use crate::ports::wal::{WalEntry, WalPort};

pub struct SyncClient {
    node_id: u64,
    node_addr: String,
    membership: SharedMembership,
    metadata: Arc<dyn MetadataPort>,
    wal: Arc<dyn WalPort>,
    client: reqwest::Client,
    /// Static peer addresses from config, used as fallback when the
    /// membership has no active peers yet (Raft hasn't formed).
    fallback_peer_addrs: Vec<String>,
}

impl SyncClient {
    pub fn new(
        node_id: u64,
        node_addr: String,
        membership: SharedMembership,
        metadata: Arc<dyn MetadataPort>,
        wal: Arc<dyn WalPort>,
        fallback_peer_addrs: Vec<String>,
    ) -> Self {
        let client = reqwest::Client::builder()
            .timeout(Duration::from_secs(300))
            .build()
            .unwrap_or_else(|_| reqwest::Client::new());

        Self {
            node_id,
            node_addr,
            membership,
            metadata,
            wal,
            client,
            fallback_peer_addrs,
        }
    }

    /// Reconnect after a disconnect: detect WAL gap and do WAL catch-up or
    /// a full metadata re-sync when the gap is large.
    pub async fn reconnect_sync(&self) -> Result<bool, HyperbytedbError> {
        let peer_addr = self.pick_sync_peer().await;
        let peer_addr = match peer_addr {
            Some(addr) => addr,
            None => {
                tracing::info!("no peers available for reconnect sync");
                return Ok(false);
            }
        };

        tracing::info!(peer = %peer_addr, "starting reconnect sync");

        let manifest = self.get_manifest(&peer_addr).await?;
        let local_wal_seq = self.wal.last_sequence().await?;
        let remote_wal_seq = manifest.wal_last_seq;

        let gap = remote_wal_seq.saturating_sub(local_wal_seq);
        tracing::info!(
            local_wal_seq = local_wal_seq,
            remote_wal_seq = remote_wal_seq,
            gap = gap,
            "reconnect gap analysis"
        );

        const SMALL_GAP_THRESHOLD: u64 = 10_000;

        if gap == 0 {
            tracing::info!("no WAL gap, node is current");
        } else if gap <= SMALL_GAP_THRESHOLD {
            tracing::info!(
                gap = gap,
                "small gap detected, performing WAL catch-up (staying Active)"
            );
            self.wal_catchup(&peer_addr, local_wal_seq).await?;
        } else {
            {
                let mut m = self.membership.write().await;
                m.set_state(self.node_id, NodeState::Syncing);
            }
            tracing::info!(
                gap = gap,
                "large gap detected, performing metadata sync + WAL catch-up"
            );

            self.sync_metadata(&peer_addr).await?;

            let updated_wal_seq = self.wal.last_sequence().await?;
            self.wal_catchup(&peer_addr, updated_wal_seq).await?;

            {
                let mut m = self.membership.write().await;
                m.set_state(self.node_id, NodeState::Active);
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
            tracing::info!(error = %e, "join request failed (raft leader may add us separately), continuing sync");
        }

        let manifest = self.get_manifest(&peer_addr).await?;
        let local_wal_seq = self.wal.last_sequence().await?;

        tracing::info!(
            databases = manifest.databases.len(),
            local_wal_seq = local_wal_seq,
            remote_wal_seq = manifest.wal_last_seq,
            "received sync manifest from peer"
        );

        self.sync_metadata(&peer_addr).await?;

        // Pull all WAL entries this node is missing, starting from our local
        // sequence. Using the remote watermark would skip the entire history on
        // a new node (cursor starts at remote+1).
        self.wal_catchup(&peer_addr, local_wal_seq).await?;

        let mut m = self.membership.write().await;
        m.set_state(self.node_id, NodeState::Active);
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
                tracing::info!(peer = %addr, "using fallback peer for sync");
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

        tracing::info!(
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

        tracing::info!(
            entries = snapshot.entries.len(),
            "importing metadata snapshot"
        );

        for entry in &snapshot.entries {
            self.import_metadata_entry(entry).await?;
        }

        tracing::info!("metadata sync complete");
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
                .map_err(|e| HyperbytedbError::Metadata(format!("parse db: {e}")))?;
            self.metadata.create_database(&db.name).await?;
            for rp in &db.retention_policies {
                self.metadata
                    .create_retention_policy(&db.name, rp.clone())
                    .await?;
            }
        } else if entry.key.starts_with("meas:") {
            let parts: Vec<&str> = entry.key.splitn(3, ':').collect();
            if parts.len() == 3 {
                let db = parts[1];
                let meta: MeasurementMeta = serde_json::from_slice(&entry.value)
                    .map_err(|e| HyperbytedbError::Metadata(format!("parse meas: {e}")))?;
                self.metadata.register_measurement(db, &meta).await?;
            }
        } else if let Some(username) = entry.key.strip_prefix("user:") {
            let user: StoredUser = serde_json::from_slice(&entry.value)
                .map_err(|e| HyperbytedbError::Metadata(format!("parse user: {e}")))?;
            self.metadata
                .create_user(username, &user.password_hash, user.admin)
                .await?;
        } else if entry.key.starts_with("tombstone:") {
            let parts: Vec<&str> = entry.key.splitn(4, ':').collect();
            if parts.len() == 4 {
                let db = parts[1];
                let meas = parts[2];
                let predicate = std::str::from_utf8(&entry.value).unwrap_or("");
                self.metadata.store_tombstone(db, meas, predicate).await?;
            }
        } else if entry.key.starts_with("cq:") {
            let parts: Vec<&str> = entry.key.splitn(3, ':').collect();
            if parts.len() == 3 {
                let db = parts[1];
                let cq: ContinuousQueryDef = serde_json::from_slice(&entry.value)
                    .map_err(|e| HyperbytedbError::Metadata(format!("parse cq: {e}")))?;
                self.metadata
                    .store_continuous_query(db, &cq.name, &cq)
                    .await?;
            }
        }

        Ok(())
    }

    async fn wal_catchup(&self, peer_addr: &str, from_seq: u64) -> Result<(), HyperbytedbError> {
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
                let wal_entry = WalEntry {
                    database: entry.database.clone(),
                    retention_policy: entry.retention_policy.clone(),
                    points: entry.points.clone(),
                    origin_node_id: entry.origin_node_id,
                };
                self.wal.append(wal_entry).await?;
            }

            cursor = wal_resp.last_seq;
            total_entries += batch_count as u64;

            tracing::info!(
                entries = batch_count,
                cursor = cursor,
                "WAL catch-up batch applied"
            );
        }

        tracing::info!(total_entries = total_entries, "WAL catch-up complete");
        Ok(())
    }
}
