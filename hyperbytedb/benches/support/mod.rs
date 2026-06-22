//! Shared helpers for Criterion benchmarks (dataset seeding, query harness).

#![allow(dead_code)]

use std::fmt::Write as _;
use std::sync::Arc;

use hyperbytedb::adapters::chdb::native_adapter::ChdbNativeAdapter;
use hyperbytedb::adapters::chdb::query_adapter::ChdbQueryAdapter;
use hyperbytedb::adapters::chdb::session::SharedSession;
use hyperbytedb::adapters::metadata::rocksdb_meta::RocksDbMetadata;
use hyperbytedb::adapters::wal::rocksdb_wal::RocksDbWal;
use hyperbytedb::application::flush_service::FlushServiceImpl;
use hyperbytedb::application::ingestion_service::IngestionServiceImpl;
use hyperbytedb::application::query_service::QueryServiceImpl;
use hyperbytedb::ports::ingestion::{IngestionPort, WritePayloadFormat};
use hyperbytedb::ports::metadata::MetadataPort;
use hyperbytedb::ports::points_sink::PointsSinkPort;
use tokio::runtime::Runtime;

pub const DB: &str = "benchdb";

/// Fixed dataset sizes for reproducible query benchmarks.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DatasetProfile {
    /// 10k points, 1 measurement, 10 hosts.
    Small,
    /// 1M points, 3 measurements, 100 hosts.
    Medium,
    /// 10M points, 1 measurement, 1000 hosts.
    Large,
}

impl DatasetProfile {
    pub fn from_env() -> Self {
        match std::env::var("BENCH_DATASET")
            .ok()
            .map(|s| s.to_ascii_lowercase())
            .as_deref()
        {
            Some("medium") => Self::Medium,
            Some("large") => Self::Large,
            _ => Self::Small,
        }
    }

    pub fn point_count(self) -> usize {
        match self {
            Self::Small => 10_000,
            Self::Medium => 1_000_000,
            Self::Large => 10_000_000,
        }
    }

    fn host_count(self) -> usize {
        match self {
            Self::Small => 10,
            Self::Medium => 100,
            Self::Large => 1_000,
        }
    }

    fn measurements(self) -> &'static [&'static str] {
        match self {
            Self::Small => &["cpu"],
            Self::Medium => &["cpu", "mem", "disk"],
            Self::Large => &["cpu"],
        }
    }

    pub fn label(self) -> &'static str {
        match self {
            Self::Small => "small",
            Self::Medium => "medium",
            Self::Large => "large",
        }
    }
}

pub struct BenchEnv {
    pub rt: Runtime,
    pub query_service: QueryServiceImpl,
    pub profile: DatasetProfile,
    _tmpdir: tempfile::TempDir,
}

pub struct FlushBenchEnv {
    pub rt: Runtime,
    pub flush_service: Arc<FlushServiceImpl>,
    pub ingestion: IngestionServiceImpl,
    pub profile: DatasetProfile,
    _tmpdir: tempfile::TempDir,
}

pub fn setup_flush(profile: DatasetProfile) -> FlushBenchEnv {
    let rt = Runtime::new().expect("runtime");
    let tmpdir = tempfile::tempdir().expect("tmpdir");
    let root = tmpdir.path();
    let wal_dir = root.join("wal");
    let meta_dir = root.join("meta");
    let chdb_dir = root.join("chdb");
    std::fs::create_dir_all(&wal_dir).unwrap();
    std::fs::create_dir_all(&meta_dir).unwrap();
    std::fs::create_dir_all(&chdb_dir).unwrap();

    let wal = Arc::new(RocksDbWal::open(&wal_dir).unwrap());
    let metadata = Arc::new(RocksDbMetadata::open(&meta_dir).unwrap());
    rt.block_on(metadata.create_database(DB)).unwrap();

    let shared = SharedSession::new_eager(chdb_dir.to_str().unwrap(), 1).unwrap();
    let points_sink: Arc<dyn PointsSinkPort> = Arc::new(ChdbNativeAdapter::new(shared));

    let ingestion = IngestionServiceImpl::new(wal.clone(), metadata.clone(), 100_000, 10_000);
    let flush_service = Arc::new(FlushServiceImpl::new(wal, 0, points_sink));

    FlushBenchEnv {
        rt,
        flush_service,
        ingestion,
        profile,
        _tmpdir: tmpdir,
    }
}

/// Seed WAL entries without flushing — timed iterations measure flush cost.
pub fn seed_wal_only(rt: &Runtime, ingestion: &IngestionServiceImpl, profile: DatasetProfile) {
    seed_points(rt, ingestion, profile);
}

/// Append a fixed batch of line-protocol points to the WAL (for incremental flush benches).
pub fn ingest_points(rt: &Runtime, ingestion: &IngestionServiceImpl, count: usize) {
    const BASE_NS: i64 = 1_800_000_000_000_000_000;
    let mut body = String::with_capacity(count * 64);
    for i in 0..count {
        let ts = BASE_NS + (i as i64) * 1_000_000_000;
        // Match the `cpu` schema used by `seed_points` (idle + usage_user, region tag).
        writeln!(
            body,
            "cpu,host=bench,region=us-west idle={:.2},usage_user={:.2} {ts}",
            90.0 + (i % 10) as f64,
            (i % 10) as f64,
        )
        .unwrap();
    }
    rt.block_on(ingestion.ingest(
        DB,
        None,
        None,
        body.as_bytes(),
        WritePayloadFormat::LineProtocol,
    ))
    .expect("ingest");
}

pub fn setup(profile: DatasetProfile) -> BenchEnv {
    let rt = Runtime::new().expect("runtime");
    let tmpdir = tempfile::tempdir().expect("tmpdir");
    let root = tmpdir.path();
    let wal_dir = root.join("wal");
    let meta_dir = root.join("meta");
    let chdb_dir = root.join("chdb");
    std::fs::create_dir_all(&wal_dir).unwrap();
    std::fs::create_dir_all(&meta_dir).unwrap();
    std::fs::create_dir_all(&chdb_dir).unwrap();

    let wal = Arc::new(RocksDbWal::open(&wal_dir).unwrap());
    let metadata = Arc::new(RocksDbMetadata::open(&meta_dir).unwrap());
    rt.block_on(metadata.create_database(DB)).unwrap();

    let shared = SharedSession::new_eager(chdb_dir.to_str().unwrap(), 1).unwrap();
    let query_port = Arc::new(ChdbQueryAdapter::from_shared(shared.clone(), 0));
    let points_sink: Arc<dyn PointsSinkPort> = Arc::new(ChdbNativeAdapter::new(shared));

    let ingestion = IngestionServiceImpl::new(wal.clone(), metadata.clone(), 100_000, 10_000);
    let flush_service = FlushServiceImpl::new(wal.clone(), 0, points_sink.clone());
    let query_service =
        QueryServiceImpl::new(query_port, metadata.clone(), wal.clone(), 120, points_sink);

    seed_dataset(&rt, &ingestion, &flush_service, profile);

    BenchEnv {
        rt,
        query_service,
        profile,
        _tmpdir: tmpdir,
    }
}

fn seed_dataset(
    rt: &Runtime,
    ingestion: &IngestionServiceImpl,
    flush: &FlushServiceImpl,
    profile: DatasetProfile,
) {
    seed_points(rt, ingestion, profile);
    rt.block_on(flush.flush()).expect("flush");
    eprintln!("dataset {:?} ready", profile);
}

fn seed_points(rt: &Runtime, ingestion: &IngestionServiceImpl, profile: DatasetProfile) {
    let total = profile.point_count();
    let hosts = profile.host_count();
    let measurements = profile.measurements();
    let points_per_measurement = total / measurements.len();
    const CHUNK: usize = 5_000;
    const BASE_NS: i64 = 1_700_000_000_000_000_000;

    eprintln!(
        "seeding {:?} dataset: {} points across {} measurement(s), {} hosts",
        profile,
        total,
        measurements.len(),
        hosts
    );

    let mut global_idx = 0usize;
    for measurement in measurements {
        let mut written = 0usize;
        while written < points_per_measurement {
            let n = (points_per_measurement - written).min(CHUNK);
            let mut body = String::with_capacity(n * 72);
            for i in 0..n {
                let idx = global_idx + written + i;
                let host = idx % hosts;
                let ts = BASE_NS + (idx as i64) * 1_000_000_000;
                match *measurement {
                    "cpu" => writeln!(
                        body,
                        "cpu,host=host{host},region=us-west idle={:.2},usage_user={:.2} {ts}",
                        90.0 + (idx % 10) as f64,
                        (idx % 10) as f64,
                    )
                    .unwrap(),
                    "mem" => writeln!(
                        body,
                        "mem,host=host{host} used={:.0},free={:.0} {ts}",
                        4_000_000_000.0 + (idx % 1000) as f64,
                        8_000_000_000.0 - (idx % 1000) as f64,
                    )
                    .unwrap(),
                    "disk" => writeln!(
                        body,
                        "disk,host=host{host},path=/dev/sda1 used_percent={:.1} {ts}",
                        (idx % 100) as f64,
                    )
                    .unwrap(),
                    _ => unreachable!(),
                }
            }
            rt.block_on(ingestion.ingest(
                DB,
                None,
                None,
                body.as_bytes(),
                WritePayloadFormat::LineProtocol,
            ))
            .expect("ingest");
            written += n;
        }
        global_idx += points_per_measurement;
    }
}
