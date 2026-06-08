use crate::config::ConnectionConfig;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum OutputFormat {
    Column,
    Json,
    Csv,
}

impl OutputFormat {
    pub fn parse(s: &str) -> Option<Self> {
        match s.to_ascii_lowercase().as_str() {
            "column" | "table" => Some(Self::Column),
            "json" => Some(Self::Json),
            "csv" => Some(Self::Csv),
            _ => None,
        }
    }

    pub fn as_str(self) -> &'static str {
        match self {
            Self::Column => "column",
            Self::Json => "json",
            Self::Csv => "csv",
        }
    }
}

#[derive(Debug, Clone)]
pub struct Session {
    pub connection: ConnectionConfig,
    pub database: Option<String>,
    pub retention_policy: Option<String>,
    pub format: OutputFormat,
    pub epoch: Option<String>,
    pub pretty: bool,
    pub chunked: bool,
    pub chunk_size: usize,
    pub timing: bool,
    pub verbose: bool,
    pub server_version: Option<String>,
}

impl Session {
    pub fn new(connection: ConnectionConfig) -> Self {
        let database = connection.database.clone();
        Self {
            connection,
            database,
            retention_policy: None,
            format: OutputFormat::Column,
            epoch: None,
            pretty: false,
            chunked: false,
            chunk_size: 10_000,
            timing: false,
            verbose: false,
            server_version: None,
        }
    }

    pub fn effective_database(&self) -> Option<&str> {
        self.database.as_deref()
    }

    pub fn set_use(&mut self, db: &str, rp: Option<&str>) {
        self.database = Some(db.to_string());
        self.retention_policy = rp.map(|s| s.to_string());
    }

    pub fn clear_database(&mut self) {
        self.database = None;
    }

    pub fn clear_retention_policy(&mut self) {
        self.retention_policy = None;
    }
}
