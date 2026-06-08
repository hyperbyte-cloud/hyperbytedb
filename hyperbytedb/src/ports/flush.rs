use async_trait::async_trait;

use crate::error::HyperbytedbError;

/// Background flush service used during graceful cluster drain.
#[async_trait]
pub trait FlushPort: Send + Sync {
    async fn drain(&self) -> Result<(), HyperbytedbError>;
}
