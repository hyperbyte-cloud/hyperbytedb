use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ContinuousQueryDef {
    pub name: String,
    pub database: String,
    pub query_text: String,
    pub resample_every_secs: Option<u64>,
    pub resample_for_secs: Option<u64>,
    pub created_at: String,
}
