//! In-memory `PointsSinkPort` / `QueryPort` pair that records what reaches
#![allow(dead_code)]

//! them, for tests that build the app with `AdapterOverrides` and no chDB.

use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};

use async_trait::async_trait;
use hyperbytedb::domain::point::Point;
use hyperbytedb::error::HyperbytedbError;
use hyperbytedb::ports::points_sink::{PointsSinkPort, WriteAck};
use hyperbytedb::ports::query::{ArrowBatchStream, QueryPort, empty_arrow_stream};

/// Counts the rows written through it.
#[derive(Default)]
pub struct RecordingSink {
    rows: AtomicUsize,
}

impl RecordingSink {
    pub fn rows(&self) -> usize {
        self.rows.load(Ordering::SeqCst)
    }
}

#[async_trait]
impl PointsSinkPort for RecordingSink {
    async fn write_points(
        &self,
        _db: &str,
        _rp: &str,
        _measurement: &str,
        _origins: &[u64],
        _ingest_seq_base: u64,
        points: &[Point],
    ) -> Result<WriteAck, HyperbytedbError> {
        self.rows.fetch_add(points.len(), Ordering::SeqCst);
        let min_time = points.iter().map(|p| p.timestamp).min().unwrap_or(0);
        let max_time = points.iter().map(|p| p.timestamp).max().unwrap_or(0);
        Ok(WriteAck {
            min_time,
            max_time,
            row_count: points.len(),
        })
    }
}

/// Accepts any SQL, records it, and returns empty results.
#[derive(Default)]
pub struct RecordingQuery {
    sql: parking_lot::Mutex<Vec<String>>,
}

impl RecordingQuery {
    pub fn statements(&self) -> Vec<String> {
        self.sql.lock().clone()
    }
}

#[async_trait]
impl QueryPort for RecordingQuery {
    async fn execute_sql(&self, sql: &str) -> Result<String, HyperbytedbError> {
        self.sql.lock().push(sql.to_string());
        Ok(String::new())
    }

    async fn execute_sql_arrow(&self, sql: &str) -> Result<ArrowBatchStream, HyperbytedbError> {
        self.sql.lock().push(sql.to_string());
        Ok(empty_arrow_stream())
    }
}

/// A fresh recording pair.
pub fn recording_ports() -> (Arc<RecordingSink>, Arc<RecordingQuery>) {
    (Arc::default(), Arc::default())
}
