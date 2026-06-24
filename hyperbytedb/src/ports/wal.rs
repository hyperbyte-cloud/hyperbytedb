use async_trait::async_trait;

use crate::domain::prepared_wal::PreparedWalSlot;
use crate::error::HyperbytedbError;

pub use crate::domain::wal::WalEntry;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum WalFormat {
    #[default]
    Bincode,
    ArrowIpc,
}

impl WalFormat {
    pub fn from_config(s: &str) -> Self {
        match s.trim().to_ascii_lowercase().as_str() {
            "arrow_ipc" | "arrow-ipc" | "ipc" => Self::ArrowIpc,
            _ => Self::Bincode,
        }
    }
}

/// Bundle written atomically to the durable WAL and in-memory caches.
pub struct WalAppendBundle {
    /// Legacy entry for peer sync and bincode durability.
    pub entry: WalEntry,
    /// chDB-ready slot; required when `arrow_wal_enabled` or `WalFormat::ArrowIpc`.
    pub prepared: Option<PreparedWalSlot>,
}

#[async_trait]
pub trait WalPort: Send + Sync {
    async fn append(&self, entry: WalEntry) -> Result<u64, HyperbytedbError>;

    /// Append with optional prepared Arrow slot for the in-memory fast path.
    async fn append_bundle(&self, bundle: WalAppendBundle) -> Result<u64, HyperbytedbError> {
        let _ = bundle.prepared;
        self.append(bundle.entry).await
    }

    async fn read_from(&self, sequence: u64) -> Result<Vec<(u64, WalEntry)>, HyperbytedbError>;
    async fn read_range(
        &self,
        from: u64,
        max_entries: usize,
    ) -> Result<Vec<(u64, WalEntry)>, HyperbytedbError>;

    /// Move prepared slots out of the in-memory Arrow cache for flush.
    ///
    /// Returns a contiguous run starting at `from`, never advancing past
    /// `to_inclusive` (the flush snapshot sequence) so slots for writes that
    /// arrived during the flush are left cached for the next one.
    async fn take_prepared_range(
        &self,
        _from: u64,
        _to_inclusive: u64,
        _max_entries: usize,
    ) -> Result<Option<Vec<(u64, PreparedWalSlot)>>, HyperbytedbError> {
        Ok(None)
    }

    /// Smallest cached prepared sequence at or after `from`, if any. Lets the
    /// flush bound a native read so a gap (an entry with no prepared slot) does
    /// not consume the prepared slots that follow it.
    async fn next_prepared_seq(&self, _from: u64) -> Result<Option<u64>, HyperbytedbError> {
        Ok(None)
    }

    fn arrow_wal_enabled(&self) -> bool {
        false
    }

    async fn truncate_before(&self, sequence: u64) -> Result<(), HyperbytedbError>;
    async fn last_sequence(&self) -> Result<u64, HyperbytedbError>;
}
