use async_trait::async_trait;

use crate::error::HyperbytedbError;

/// Body encoding for [`IngestionPort::ingest`]. HTTP `/write` selects this via `Content-Type`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum WritePayloadFormat {
    /// InfluxDB line protocol (default).
    #[default]
    LineProtocol,
    /// MessagePack array of point maps (`Content-Type: application/msgpack`).
    Msgpack,
    /// Columnar MessagePack map (`Content-Type: application/vnd.hyperbytedb.columnar-msgpack.v1`).
    #[cfg(feature = "columnar-ingest")]
    ColumnarMsgpack,
}

#[async_trait]
pub trait IngestionPort: Send + Sync {
    /// Ingest write payload into the given database (`format` selects line protocol vs msgpack).
    async fn ingest(
        &self,
        db: &str,
        rp: Option<&str>,
        precision: Option<&str>,
        body: &[u8],
        format: WritePayloadFormat,
    ) -> Result<(), HyperbytedbError>;
}
