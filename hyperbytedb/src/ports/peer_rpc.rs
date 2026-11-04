//! Calls to peer nodes, one typed method per internal endpoint. The
//! application uses this instead of building HTTP requests itself.

use std::sync::Arc;

#[async_trait::async_trait]
pub trait PeerRpc: Send + Sync {}

pub type SharedPeerRpc = Arc<dyn PeerRpc>;
