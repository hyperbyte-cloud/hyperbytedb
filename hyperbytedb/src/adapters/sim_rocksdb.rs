//! DST seam, `cfg(madsim)` only: RocksDB tuned so the simulation's small
//! workloads still flush memtables and compact L0, and per-database
//! flush/compaction counts for the rig.
//!
//! Production keeps 64 MiB (WAL) / 8 MiB (metadata) memtables and the
//! default L0 trigger of 4, which a simulated run of a few MiB never
//! reaches: RocksDB's flush and compaction threads (real OS threads, outside
//! madsim's control) would never run. Here every RocksDB-backed store (WAL,
//! metadata, Raft log, replication log) uses [`WRITE_BUFFER_SIZE`]
//! memtables, merges one at a time, and compacts at [`L0_COMPACTION_TRIGGER`]
//! L0 files, so they do; the rig checks those runs stay deterministic.
//!
//! [`track`] turns RocksDB statistics on for a database's DB-wide options
//! and keeps a handle (an `Options` clone shares the statistics object), so
//! [`stats`] can report how many flushes and compactions each database
//! (by path) ran, summed over its opens.

use std::path::{Path, PathBuf};

use parking_lot::Mutex;
use rocksdb::Options;
use rocksdb::statistics::{Histogram, Ticker};

/// Memtable size for every store in the sim.
pub const WRITE_BUFFER_SIZE: usize = 256 * 1024;
/// L0 files that trigger a compaction in the sim.
pub const L0_COMPACTION_TRIGGER: i32 = 2;

static TRACKED: Mutex<Vec<(PathBuf, Options)>> = Mutex::new(Vec::new());

/// Small memtables, flushed one at a time, and an eager L0 compaction
/// trigger, on DB-wide or column-family options.
pub fn tune(opts: &mut Options) {
    opts.set_write_buffer_size(WRITE_BUFFER_SIZE);
    opts.set_min_write_buffer_number_to_merge(1);
    opts.set_level_zero_file_num_compaction_trigger(L0_COMPACTION_TRIGGER);
}

/// [`tune`] the DB-wide options of the database about to open at `path`,
/// enable statistics on them and remember them for [`stats`].
pub fn track(path: &Path, opts: &mut Options) {
    tune(opts);
    opts.enable_statistics();
    TRACKED.lock().push((path.to_path_buf(), opts.clone()));
}

/// Flush and compaction activity of one database.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct DbActivity {
    pub path: PathBuf,
    pub flushes: u64,
    pub compactions: u64,
    pub flush_bytes: u64,
    pub compaction_bytes: u64,
}

/// Every tracked database under `root`, by path (in path order), summed
/// over its opens.
pub fn stats(root: &Path) -> Vec<DbActivity> {
    let mut out: Vec<DbActivity> = Vec::new();
    for (path, opts) in TRACKED.lock().iter().filter(|(p, _)| p.starts_with(root)) {
        let i = match out.iter().position(|a| a.path == *path) {
            Some(i) => i,
            None => {
                out.push(DbActivity {
                    path: path.clone(),
                    ..DbActivity::default()
                });
                out.len() - 1
            }
        };
        let a = &mut out[i];
        a.flushes += opts.get_histogram_data(Histogram::FlushTime).count();
        a.compactions += opts.get_histogram_data(Histogram::CompactionTime).count();
        a.flush_bytes += opts.get_ticker_count(Ticker::FlushWriteBytes);
        a.compaction_bytes += opts.get_ticker_count(Ticker::CompactWriteBytes);
    }
    out.sort_by(|a, b| a.path.cmp(&b.path));
    out
}

/// Forget every tracked database under `root` (a finished run's).
pub fn forget(root: &Path) {
    TRACKED.lock().retain(|(p, _)| !p.starts_with(root));
}
