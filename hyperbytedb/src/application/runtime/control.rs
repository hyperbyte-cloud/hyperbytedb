//! A small dedicated runtime for the control plane: the Raft core and the
//! control listener (`/ping`, `/health`, Raft RPCs).
//!
//! Under write saturation the main runtime's task queues grow long enough
//! that a Raft heartbeat or a liveness probe waits behind thousands of write
//! tasks. On n3 that cost ~70 elections per minute-long run and liveness
//! kills. Work here never queues behind the write path.
//!
//! The server installs the runtime at startup and shuts it down at the end
//! of its run. Nothing else installs it, so without it (tests, tools) control
//! work runs on the caller's runtime exactly as before. A process-global
//! runtime that was never shut down outlived RocksDB at exit and aborted
//! the process ("pthread lock: Invalid argument").

use std::future::Future;
use std::sync::{Mutex, OnceLock};

use tokio::runtime::{Handle, Runtime};

/// Worker threads for the control runtime. Its work is small and bursty.
const CONTROL_WORKERS: usize = 2;

static RUNTIME: Mutex<Option<Runtime>> = Mutex::new(None);
static HANDLE: OnceLock<Handle> = OnceLock::new();

/// Create the control runtime. Idempotent; call once at server start.
pub fn install() -> std::io::Result<()> {
    let mut slot = RUNTIME.lock().unwrap_or_else(|e| e.into_inner());
    if slot.is_some() || HANDLE.get().is_some() {
        return Ok(());
    }
    let rt = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(CONTROL_WORKERS)
        .thread_name("hb-control")
        .enable_all()
        .build()?;
    let _ = HANDLE.set(rt.handle().clone());
    *slot = Some(rt);
    Ok(())
}

/// Stop the control runtime without waiting on its tasks. Call at the end
/// of the server's run, before the process exits.
pub fn shutdown() {
    let rt = RUNTIME.lock().unwrap_or_else(|e| e.into_inner()).take();
    if let Some(rt) = rt {
        rt.shutdown_background();
    }
}

/// The control runtime, if installed.
pub fn handle() -> Option<&'static Handle> {
    HANDLE.get()
}

/// Run `fut` on `handle`'s runtime and wait for it from any runtime.
///
/// Anything `fut` spawns (for openraft: the Raft core, its state-machine
/// worker and replication tasks) lands on that runtime too.
pub async fn spawn_on<F, T>(handle: &Handle, fut: F) -> T
where
    F: Future<Output = T> + Send + 'static,
    T: Send + 'static,
{
    match handle.spawn(fut).await {
        Ok(v) => v,
        Err(e) => std::panic::resume_unwind(e.into_panic()),
    }
}

/// Run `fut` on the control runtime when installed, otherwise in place.
pub async fn on_control<F, T>(fut: F) -> T
where
    F: Future<Output = T> + Send + 'static,
    T: Send + 'static,
{
    match handle() {
        Some(h) => spawn_on(h, fut).await,
        None => fut.await,
    }
}

#[cfg(test)]
mod tests {
    use super::spawn_on;

    #[tokio::test]
    async fn work_and_its_spawns_run_on_the_given_runtime() {
        let rt = tokio::runtime::Builder::new_multi_thread()
            .worker_threads(1)
            .thread_name("hb-control-test")
            .enable_all()
            .build()
            .unwrap();
        let (outer, inner) = spawn_on(rt.handle(), async {
            let outer = std::thread::current().name().map(str::to_string);
            let inner = tokio::spawn(async { std::thread::current().name().map(str::to_string) })
                .await
                .unwrap();
            (outer, inner)
        })
        .await;
        rt.shutdown_background();
        assert_eq!(outer.as_deref(), Some("hb-control-test"));
        assert_eq!(
            inner.as_deref(),
            Some("hb-control-test"),
            "spawned work must stay there"
        );
    }
}
