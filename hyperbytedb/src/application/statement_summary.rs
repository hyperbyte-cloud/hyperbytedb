use std::collections::HashMap;
use std::sync::RwLock;

use serde::Serialize;

#[derive(Debug, Clone, Serialize)]
pub struct SummaryEntry {
    pub digest: String,
    pub normalized_query: String,
    pub sample_query: String,
    pub db: String,
    pub stmt_type: String,
    pub exec_count: u64,
    pub sum_latency_us: u64,
    pub avg_latency_us: u64,
    pub min_latency_us: u64,
    pub max_latency_us: u64,
    pub first_seen: i64,
    pub last_seen: i64,
}

struct InnerEntry {
    digest: String,
    normalized_query: String,
    sample_query: String,
    db: String,
    stmt_type: String,
    exec_count: u64,
    sum_latency_us: u64,
    min_latency_us: u64,
    max_latency_us: u64,
    first_seen: i64,
    last_seen: i64,
}

impl InnerEntry {
    fn to_summary(&self) -> SummaryEntry {
        let avg = self
            .sum_latency_us
            .checked_div(self.exec_count)
            .unwrap_or(0);
        SummaryEntry {
            digest: self.digest.clone(),
            normalized_query: self.normalized_query.clone(),
            sample_query: self.sample_query.clone(),
            db: self.db.clone(),
            stmt_type: self.stmt_type.clone(),
            exec_count: self.exec_count,
            sum_latency_us: self.sum_latency_us,
            avg_latency_us: avg,
            min_latency_us: self.min_latency_us,
            max_latency_us: self.max_latency_us,
            first_seen: self.first_seen,
            last_seen: self.last_seen,
        }
    }
}

pub struct StatementSummary {
    entries: RwLock<HashMap<(String, String), InnerEntry>>,
    max_entries: usize,
}

#[derive(Debug, Clone, Copy)]
pub enum SortBy {
    TotalLatency,
    AvgLatency,
    MaxLatency,
    Count,
}

impl StatementSummary {
    pub fn new(max_entries: usize) -> Self {
        Self {
            entries: RwLock::new(HashMap::new()),
            max_entries,
        }
    }

    pub fn record(
        &self,
        digest: &str,
        normalized_query: &str,
        raw_query: &str,
        db: &str,
        stmt_type: &str,
        latency_us: u64,
    ) {
        let now = chrono::Utc::now().timestamp();
        let key = (digest.to_string(), db.to_string());

        let mut map = self.entries.write().unwrap_or_else(|e| e.into_inner());

        if let Some(entry) = map.get_mut(&key) {
            entry.exec_count += 1;
            entry.sum_latency_us += latency_us;
            if latency_us < entry.min_latency_us {
                entry.min_latency_us = latency_us;
            }
            if latency_us > entry.max_latency_us {
                entry.max_latency_us = latency_us;
            }
            entry.last_seen = now;
            entry.sample_query = raw_query.to_string();
        } else {
            if map.len() >= self.max_entries {
                Self::evict_lru(&mut map);
            }
            map.insert(
                key,
                InnerEntry {
                    digest: digest.to_string(),
                    normalized_query: normalized_query.to_string(),
                    sample_query: raw_query.to_string(),
                    db: db.to_string(),
                    stmt_type: stmt_type.to_string(),
                    exec_count: 1,
                    sum_latency_us: latency_us,
                    min_latency_us: latency_us,
                    max_latency_us: latency_us,
                    first_seen: now,
                    last_seen: now,
                },
            );
        }
    }

    pub fn list(
        &self,
        sort_by: SortBy,
        ascending: bool,
        limit: usize,
        db_filter: Option<&str>,
        stmt_type_filter: Option<&str>,
    ) -> Vec<SummaryEntry> {
        let map = self.entries.read().unwrap_or_else(|e| e.into_inner());
        let mut entries: Vec<SummaryEntry> = map
            .values()
            .filter(|e| {
                if let Some(db) = db_filter
                    && !db.is_empty()
                    && e.db != db
                {
                    return false;
                }
                if let Some(st) = stmt_type_filter
                    && !st.is_empty()
                    && e.stmt_type != st
                {
                    return false;
                }
                true
            })
            .map(|e| e.to_summary())
            .collect();

        entries.sort_by(|a, b| {
            let cmp = match sort_by {
                SortBy::TotalLatency => a.sum_latency_us.cmp(&b.sum_latency_us),
                SortBy::AvgLatency => a.avg_latency_us.cmp(&b.avg_latency_us),
                SortBy::MaxLatency => a.max_latency_us.cmp(&b.max_latency_us),
                SortBy::Count => a.exec_count.cmp(&b.exec_count),
            };
            if ascending { cmp } else { cmp.reverse() }
        });

        entries.truncate(limit);
        entries
    }

    pub fn reset(&self) {
        let mut map = self.entries.write().unwrap_or_else(|e| e.into_inner());
        map.clear();
    }

    fn evict_lru(map: &mut HashMap<(String, String), InnerEntry>) {
        if let Some(oldest_key) = map
            .iter()
            .min_by_key(|(_, v)| v.last_seen)
            .map(|(k, _)| k.clone())
        {
            map.remove(&oldest_key);
        }
    }
}
