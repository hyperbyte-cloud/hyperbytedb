use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MaterializedViewDef {
    pub name: String,
    pub database: String,
    pub query_text: String,
    pub source_db: String,
    pub source_rp: String,
    pub source_measurement: String,
    pub dest_db: String,
    pub dest_rp: String,
    pub dest_measurement: String,
    pub ch_fact_mv_name: String,
    pub ch_series_mv_name: String,
    pub created_at: String,
    /// Whether CREATE ran a one-time historical backfill (`WITH BACKFILL`).
    #[serde(default)]
    pub backfill_on_create: bool,
}
