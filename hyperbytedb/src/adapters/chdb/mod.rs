pub mod catalog;
pub mod connection_pool;
pub mod native_adapter;
pub mod query_adapter;
pub mod session;

use std::sync::atomic::{AtomicBool, Ordering};

/// Set once chDB reports that its engine shut down after a fatal error.
static ENGINE_FAILED: AtomicBool = AtomicBool::new(false);

/// Note a chDB error. After a fatal error (a crash its signal handler caught,
/// in its threads or ours) the embedded engine refuses every query and
/// insert until the process restarts, so `/health` must fail: it kept
/// answering 200 while the WAL grew and no query could run.
pub fn note_error(message: &str) {
    if message.contains("shutting down due to a fatal error") && !engine_failed() {
        ENGINE_FAILED.store(true, Ordering::Release);
        tracing::error!(
            error = message,
            "chDB engine failed; failing /health so the node is restarted"
        );
    }
}

/// Whether chDB's engine has failed (see [`note_error`]).
#[must_use]
pub fn engine_failed() -> bool {
    ENGINE_FAILED.load(Ordering::Acquire)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn only_the_fatal_shutdown_error_marks_the_engine_failed() {
        note_error("Code: 62. DB::Exception: Syntax error");
        assert!(!engine_failed());
        note_error(
            "Code: 236. DB::Exception: The server is shutting down due to a fatal error. (ABORTED)",
        );
        assert!(engine_failed());
        ENGINE_FAILED.store(false, Ordering::Release);
    }
}
