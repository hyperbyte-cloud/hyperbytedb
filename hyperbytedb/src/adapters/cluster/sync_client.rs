use std::sync::Arc;
use std::time::Duration;

use crate::application::ingest_metadata::{IngestCardinalityLimits, prepare_batch_metadata};
use crate::application::wal_append::append_points_with_prepared;
use crate::domain::cluster::membership::{NodeState, SharedMembership};
use crate::domain::cluster::sync::{
    JoinRequest, JoinResponse, MetadataSnapshot, SyncManifest, WalSyncResponse,
};
use crate::error::HyperbytedbError;
use crate::ports::metadata::MetadataPort;
use crate::ports::points_sink::PointsSinkPort;
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
        Self::with_points_sink(
            node_id,
            node_addr,
            membership,
            metadata,
            wal,
            None,
            0,
            fallback_peer_addrs,
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
            points_sink,
            max_points_per_request,
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

        tracing::debug!(peer = %peer_addr, "starting reconnect sync");

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
                m.set_state(self.node_id, NodeState::Syncing);
            }
            tracing::info!(
                gap = gap,
                "large gap detected, performing metadata sync + WAL catch-up"
            );

            self.sync_metadata(&peer_addr).await?;

            let updated_wal_seq = self.wal.last_sequence().await?;
            let applied = self.wal_catchup(&peer_addr, updated_wal_seq).await?;
            let local_after = self.wal.last_sequence().await?;
            verify_catchup_progress(updated_wal_seq, remote_wal_seq, applied, local_after)?;

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

        // Pull all WAL entries this node is missing, starting from our local
        // sequence. Using the remote watermark would skip the entire history on
        // a new node (cursor starts at remote+1).
        let applied = self.wal_catchup(&peer_addr, local_wal_seq).await?;
        let local_after = self.wal.last_sequence().await?;
        verify_catchup_progress(local_wal_seq, manifest.wal_last_seq, applied, local_after)?;

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
            let parts: Vec<&str> = entry.key.splitn(4, ':').collect();
            if parts.len() == 4 {
                let db = parts[1];
                let rp = parts[2];
                let meta: MeasurementMeta = serde_json::from_slice(&entry.value)
                    .map_err(|e| HyperbytedbError::Metadata(format!("parse meas: {e}")))?;
                self.metadata.register_measurement(db, rp, &meta).await?;
            }
        } else if let Some(username) = entry.key.strip_prefix("user:") {
            let user: StoredUser = serde_json::from_slice(&entry.value)
                .map_err(|e| HyperbytedbError::Metadata(format!("parse user: {e}")))?;
            self.metadata
                .create_user(username, &user.password_hash, user.admin)
                .await?;
        } else if entry.key.starts_with("tombstone:") {
            let parts: Vec<&str> = entry.key.splitn(5, ':').collect();
            if parts.len() == 5 {
                let db = parts[1];
                let rp = parts[2];
                let meas = parts[3];
                let predicate = std::str::from_utf8(&entry.value).map_err(|e| {
                    HyperbytedbError::Metadata(format!(
                        "invalid UTF-8 in tombstone value for {db}/{rp}/{meas}: {e}"
                    ))
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
                    .map_err(|e| HyperbytedbError::Metadata(format!("parse cq: {e}")))?;
                self.metadata
                    .store_continuous_query(db, &cq.name, &cq)
                    .await?;
            }
        } else if entry.key.starts_with("mv:") {
            let parts: Vec<&str> = entry.key.splitn(3, ':').collect();
            if parts.len() == 3 {
                let db = parts[1];
                let mv: crate::ports::metadata::MaterializedViewDef =
                    serde_json::from_slice(&entry.value)
                        .map_err(|e| HyperbytedbError::Metadata(format!("parse mv: {e}")))?;
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
            IngestCardinalityLimits::default(),
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
    use super::verify_catchup_progress;
    use crate::error::HyperbytedbError;

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
