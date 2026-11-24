//! [`ServeTransport`] over real TCP (and TLS) listeners: the production
//! transport. Not built under `cfg(madsim)`, where the simulation rig
//! supplies a transport over its simulated network.

use std::time::Duration;

use async_trait::async_trait;
use tokio::sync::watch;
use tokio::task::JoinHandle;

use crate::application::runtime::control;
use crate::config::ServerConfig;
use crate::ports::serve_transport::{ListenerKind, ServeTransport};

/// Binds `server.bind_address`-style addresses with TCP, with TLS on the
/// main listener when `server.tls_enabled`.
pub struct TcpServeTransport {
    tls_enabled: bool,
    tls_cert_path: String,
    tls_key_path: String,
    /// The main listener's serve task, joined by [`ServeTransport::stopped`].
    main: parking_lot::Mutex<Option<JoinHandle<anyhow::Result<()>>>>,
}

impl TcpServeTransport {
    pub fn new(server: &ServerConfig) -> Self {
        Self {
            tls_enabled: server.tls_enabled,
            tls_cert_path: server.tls_cert_path.clone(),
            tls_key_path: server.tls_key_path.clone(),
            main: parking_lot::Mutex::new(None),
        }
    }

    async fn serve_main(
        &self,
        addr: String,
        app: axum::Router,
        shutdown: watch::Receiver<bool>,
    ) -> anyhow::Result<JoinHandle<anyhow::Result<()>>> {
        if self.tls_enabled {
            use axum_server::tls_rustls::RustlsConfig;

            if self.tls_cert_path.is_empty() || self.tls_key_path.is_empty() {
                anyhow::bail!("tls_enabled is true but tls_cert_path or tls_key_path is not set");
            }
            if !std::path::Path::new(&self.tls_cert_path).exists() {
                anyhow::bail!("TLS cert file not found: {}", self.tls_cert_path);
            }
            if !std::path::Path::new(&self.tls_key_path).exists() {
                anyhow::bail!("TLS key file not found: {}", self.tls_key_path);
            }

            let tls_config = RustlsConfig::from_pem_file(&self.tls_cert_path, &self.tls_key_path)
                .await
                .map_err(|e| anyhow::anyhow!("TLS config error: {}", e))?;

            let handle = axum_server::Handle::new();
            let shutdown_handle = handle.clone();
            tokio::spawn(async move {
                wait_for_shutdown(shutdown).await;
                shutdown_handle.graceful_shutdown(Some(Duration::from_secs(10)));
            });

            tracing::info!(addr = %addr, "Hyperbytedb listening (TLS)");
            let addr = addr.parse()?;
            Ok(tokio::spawn(async move {
                axum_server::bind_rustls(addr, tls_config)
                    .handle(handle)
                    .serve(app.into_make_service())
                    .await?;
                Ok(())
            }))
        } else {
            tracing::info!(addr = %addr, "Hyperbytedb listening");
            let listener = tokio::net::TcpListener::bind(&addr).await?;
            Ok(tokio::spawn(async move {
                axum::serve(nodelay(listener), app)
                    .with_graceful_shutdown(wait_for_shutdown(shutdown))
                    .await?;
                Ok(())
            }))
        }
    }
}

#[async_trait]
impl ServeTransport for TcpServeTransport {
    async fn serve(
        &self,
        kind: ListenerKind,
        addr: String,
        router: axum::Router,
        shutdown: watch::Receiver<bool>,
    ) -> anyhow::Result<()> {
        match kind {
            ListenerKind::Main => {
                let task = self.serve_main(addr, router, shutdown).await?;
                *self.main.lock() = Some(task);
            }
            ListenerKind::Control => {
                // On the control runtime, next to the Raft core: probes and
                // Raft RPCs are answered even while the write path is
                // saturated. It serves until the process exits.
                let _ = shutdown;
                let control_addr = addr;
                let control_app = router;
                let spawn_here = tokio::runtime::Handle::current();
                control::handle().unwrap_or(&spawn_here).spawn(async move {
                    match tokio::net::TcpListener::bind(&control_addr).await {
                        Ok(listener) => {
                            tracing::info!(addr = %control_addr, "control listener up");
                            if let Err(e) = axum::serve(nodelay(listener), control_app).await {
                                tracing::error!(error = %e, "control listener stopped");
                            }
                        }
                        Err(e) => {
                            tracing::error!(addr = %control_addr, error = %e, "control listener failed to bind");
                        }
                    }
                });
            }
        }
        Ok(())
    }

    async fn stopped(
        &self,
        kind: ListenerKind,
        _shutdown: watch::Receiver<bool>,
    ) -> anyhow::Result<()> {
        match kind {
            ListenerKind::Main => {
                let task = self.main.lock().take();
                match task {
                    Some(task) => task.await?,
                    None => Ok(()),
                }
            }
            // Never drained: it lives until the process exits.
            ListenerKind::Control => Ok(()),
        }
    }
}

async fn wait_for_shutdown(mut shutdown: watch::Receiver<bool>) {
    // A dropped sender also means shut down.
    let _ = shutdown.wait_for(|stop| *stop).await;
}

/// Accepted connections with Nagle off. Region results stream back in
/// several small writes; with Nagle each one after the first waited for the
/// peer's delayed ACK, up to 40 ms on Linux. Every query that touched a
/// remote region on the UAT cluster took ~55 ms or ~20 ms at random.
fn nodelay(
    listener: tokio::net::TcpListener,
) -> impl axum::serve::Listener<Io = tokio::net::TcpStream, Addr = std::net::SocketAddr> {
    use axum::serve::ListenerExt;
    listener.tap_io(|tcp| {
        if let Err(e) = tcp.set_nodelay(true) {
            tracing::debug!(error = %e, "TCP_NODELAY not set");
        }
    })
}
