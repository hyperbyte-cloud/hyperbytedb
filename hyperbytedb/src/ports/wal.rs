use async_trait::async_trait;

use crate::error::HyperbytedbError;

pub use crate::domain::wal::WalEntry;

#[async_trait]
pub trait WalPort: Send + Sync {
    async fn append(&self, entry: WalEntry) -> Result<u64, HyperbytedbError>;
    async fn read_from(&self, sequence: u64) -> Result<Vec<(u64, WalEntry)>, HyperbytedbError>;
    async fn read_range(
        &self,
        from: u64,
        max_entries: usize,
    ) -> Result<Vec<(u64, WalEntry)>, HyperbytedbError>;
    async fn truncate_before(&self, sequence: u64) -> Result<(), HyperbytedbError>;
    async fn last_sequence(&self) -> Result<u64, HyperbytedbError>;
}
