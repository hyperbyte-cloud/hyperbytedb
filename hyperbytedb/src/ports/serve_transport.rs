//! How a node's HTTP routers are exposed: TCP listeners in production, the
//! simulated network in the deterministic simulation rig.

use async_trait::async_trait;
use tokio::sync::watch;

/// Which of a node's listeners a router is for.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum ListenerKind {
    /// The public API (`server.port`).
    Main,
    /// Probes and Raft RPCs (`cluster.control_port`).
    Control,
}

#[async_trait]
pub trait ServeTransport: Send + Sync {
    /// Serve `router` at `addr` until `shutdown` flips. Ok(()) once serving has begun.
    async fn serve(
        &self,
        kind: ListenerKind,
        addr: String,
        router: axum::Router,
        shutdown: watch::Receiver<bool>,
    ) -> anyhow::Result<()>;

    /// Wait until the `kind` listener has stopped serving: `shutdown`
    /// flipped and in-flight requests finished, or it failed. Transports
    /// with nothing to drain take this default, which waits for the flip.
    async fn stopped(
        &self,
        kind: ListenerKind,
        mut shutdown: watch::Receiver<bool>,
    ) -> anyhow::Result<()> {
        let _ = kind;
        // A dropped sender also means shut down.
        let _ = shutdown.wait_for(|stop| *stop).await;
        Ok(())
    }
}
