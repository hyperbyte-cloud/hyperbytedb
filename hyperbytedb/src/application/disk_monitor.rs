//! Periodic disk space checks for data directories.

use std::path::Path;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};

use metrics::{counter, gauge};
use tokio::sync::watch;

use crate::config::DiskConfig;

#[derive(Debug, Clone)]
pub struct DiskMonitorPaths {
    pub wal: String,
    pub meta: String,
    pub chdb: String,
}

pub fn free_bytes(path: &Path) -> std::io::Result<u64> {
    use std::ffi::CString;
    use std::os::unix::ffi::OsStrExt;

    let c_path = CString::new(path.as_os_str().as_bytes())
        .map_err(|e| std::io::Error::new(std::io::ErrorKind::InvalidInput, e))?;
    let mut stat: libc::statvfs = unsafe { std::mem::zeroed() };
    let rc = unsafe { libc::statvfs(c_path.as_ptr(), &mut stat) };
    if rc != 0 {
        return Err(std::io::Error::last_os_error());
    }
    Ok(stat.f_bavail * stat.f_frsize)
}

pub async fn run_disk_monitor(
    paths: DiskMonitorPaths,
    config: DiskConfig,
    read_only: Arc<AtomicBool>,
    mut shutdown_rx: watch::Receiver<bool>,
) {
    let interval = std::time::Duration::from_secs(config.check_interval_secs.max(1));
    let warn_bytes = config.warn_threshold_mb.saturating_mul(1024 * 1024);
    let readonly_bytes = config.readonly_threshold_mb.saturating_mul(1024 * 1024);
    let mut ticker = tokio::time::interval(interval);
    ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);

    tracing::info!(
        ?interval,
        warn_threshold_mb = config.warn_threshold_mb,
        readonly_threshold_mb = config.readonly_threshold_mb,
        "disk monitor started"
    );

    loop {
        tokio::select! {
            _ = ticker.tick() => {
                check_paths(&paths, warn_bytes, readonly_bytes, &read_only);
            }
            _ = shutdown_rx.changed() => {
                if *shutdown_rx.borrow() {
                    tracing::info!("disk monitor received shutdown");
                    break;
                }
            }
        }
    }
}

fn check_paths(
    paths: &DiskMonitorPaths,
    warn_bytes: u64,
    readonly_bytes: u64,
    read_only: &AtomicBool,
) {
    let entries = [
        ("wal", paths.wal.as_str()),
        ("meta", paths.meta.as_str()),
        ("chdb", paths.chdb.as_str()),
    ];

    let mut lowest = u64::MAX;
    for (label, path) in entries {
        match free_bytes(Path::new(path)) {
            Ok(free) => {
                gauge!("hyperbytedb_disk_free_bytes", "path" => label.to_string()).set(free as f64);
                lowest = lowest.min(free);
            }
            Err(e) => {
                tracing::warn!(path = path, error = %e, "disk space check failed");
            }
        }
    }

    if lowest == u64::MAX {
        return;
    }

    if lowest < readonly_bytes {
        if !read_only.swap(true, Ordering::SeqCst) {
            counter!("hyperbytedb_disk_readonly_events_total").increment(1);
            tracing::error!(
                free_bytes = lowest,
                readonly_threshold_bytes = readonly_bytes,
                "entering read-only mode due to low disk space"
            );
        }
    } else if read_only.swap(false, Ordering::SeqCst) {
        tracing::info!(
            free_bytes = lowest,
            "exiting read-only mode; disk space recovered"
        );
    } else if lowest < warn_bytes {
        tracing::warn!(
            free_bytes = lowest,
            warn_threshold_bytes = warn_bytes,
            "low disk space on data volume"
        );
    }
}

pub fn startup_disk_warning(paths: &DiskMonitorPaths, warn_threshold_mb: u64) {
    if warn_threshold_mb == 0 {
        return;
    }
    let warn_bytes = warn_threshold_mb.saturating_mul(1024 * 1024);
    for (label, path) in [
        ("wal", paths.wal.as_str()),
        ("meta", paths.meta.as_str()),
        ("chdb", paths.chdb.as_str()),
    ] {
        match free_bytes(Path::new(path)) {
            Ok(free) if free < warn_bytes => {
                tracing::warn!(
                    path = label,
                    free_bytes = free,
                    warn_threshold_bytes = warn_bytes,
                    "data directory below disk warn threshold at startup"
                );
            }
            Err(e) => {
                tracing::warn!(path = label, error = %e, "startup disk space check failed");
            }
            _ => {}
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn free_bytes_on_existing_dir_is_positive() {
        let dir = tempfile::tempdir().unwrap();
        let free = free_bytes(dir.path()).unwrap();
        assert!(free > 0, "expected positive free bytes");
    }
}
