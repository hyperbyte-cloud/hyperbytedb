use std::collections::HashMap;
use std::env;
use std::fs;
use std::path::PathBuf;

use serde::Deserialize;

use crate::error::{CliError, Result};

#[derive(Debug, Clone, Default, Deserialize)]
pub struct ProfileConfig {
    pub host: Option<String>,
    pub database: Option<String>,
    pub username: Option<String>,
    pub password: Option<String>,
    pub ssl: Option<bool>,
    pub unsafe_ssl: Option<bool>,
}

#[derive(Debug, Clone, Default, Deserialize)]
pub struct ConfigFile {
    #[serde(flatten)]
    pub profiles: HashMap<String, ProfileConfig>,
}

#[derive(Debug, Clone)]
pub struct ConnectionConfig {
    pub host: String,
    pub database: Option<String>,
    pub username: Option<String>,
    pub password: Option<String>,
    pub ssl: bool,
    pub unsafe_ssl: bool,
}

impl Default for ConnectionConfig {
    fn default() -> Self {
        Self {
            host: "http://localhost:8086".to_string(),
            database: None,
            username: None,
            password: None,
            ssl: false,
            unsafe_ssl: false,
        }
    }
}

impl ConnectionConfig {
    pub fn load(profile: Option<&str>) -> Result<Self> {
        let mut cfg = Self::default();
        let profile_name = profile.unwrap_or("default");

        if let Some(file) = load_config_file()? {
            if let Some(p) = file.profiles.get(profile_name) {
                cfg.apply_profile(p);
            } else if profile.is_some() {
                return Err(CliError::Config(format!(
                    "profile '{profile_name}' not found in config file"
                )));
            }
        }

        cfg.apply_env();
        Ok(cfg)
    }

    pub fn apply_profile(&mut self, p: &ProfileConfig) {
        if let Some(ref h) = p.host {
            self.host = h.clone();
        }
        if p.database.is_some() {
            self.database = p.database.clone();
        }
        if p.username.is_some() {
            self.username = p.username.clone();
        }
        if p.password.is_some() {
            self.password = p.password.clone();
        }
        if let Some(ssl) = p.ssl {
            self.ssl = ssl;
        }
        if let Some(unsafe_ssl) = p.unsafe_ssl {
            self.unsafe_ssl = unsafe_ssl;
        }
    }

    pub fn apply_env(&mut self) {
        env_override(&mut self.host, "HYPERBYTEDB_HOST");
        env_override(&mut self.host, "INFLUX_HOST");
        env_override_opt(&mut self.database, "HYPERBYTEDB_DATABASE");
        env_override_opt(&mut self.database, "INFLUX_DATABASE");
        env_override_opt(&mut self.username, "HYPERBYTEDB_USERNAME");
        env_override_opt(&mut self.username, "INFLUX_USERNAME");
        env_override_opt(&mut self.password, "HYPERBYTEDB_PASSWORD");
        env_override_opt(&mut self.password, "INFLUX_PASSWORD");
    }

    pub fn base_url(&self) -> String {
        let mut url = self.host.trim_end_matches('/').to_string();
        if self.ssl && !url.starts_with("https://") {
            url = url.replacen("http://", "https://", 1);
        }
        url
    }
}

fn env_override(target: &mut String, key: &str) {
    if let Ok(v) = env::var(key)
        && !v.is_empty()
    {
        *target = v;
    }
}

fn env_override_opt(target: &mut Option<String>, key: &str) {
    if let Ok(v) = env::var(key)
        && !v.is_empty()
    {
        *target = Some(v);
    }
}

pub fn config_file_path() -> Option<PathBuf> {
    if let Ok(p) = env::var("HYPERBYTEDB_CLI_CONFIG") {
        return Some(PathBuf::from(p));
    }
    dirs::config_dir().map(|d| d.join("hyperbytedb").join("config.toml"))
}

fn load_config_file() -> Result<Option<ConfigFile>> {
    let Some(path) = config_file_path() else {
        return Ok(None);
    };
    if !path.exists() {
        return Ok(None);
    }
    let contents = fs::read_to_string(&path)
        .map_err(|e| CliError::Config(format!("read {}: {e}", path.display())))?;
    let file: ConfigFile = toml::from_str(&contents)
        .map_err(|e| CliError::Config(format!("parse {}: {e}", path.display())))?;
    Ok(Some(file))
}

pub fn resolve_host(host: Option<&str>, port: Option<u16>, ssl: bool) -> String {
    if let Some(h) = host {
        let h = h.trim();
        if h.starts_with("http://") || h.starts_with("https://") {
            return h.trim_end_matches('/').to_string();
        }
        let scheme = if ssl { "https" } else { "http" };
        if h.contains(':') {
            return format!("{scheme}://{h}");
        }
        let p = port.unwrap_or(8086);
        return format!("{scheme}://{h}:{p}");
    }
    let scheme = if ssl { "https" } else { "http" };
    let p = port.unwrap_or(8086);
    format!("{scheme}://localhost:{p}")
}

pub fn history_file_path() -> PathBuf {
    if let Ok(p) = env::var("HYPERBYTEDB_CLI_HISTORY") {
        return PathBuf::from(p);
    }
    dirs::home_dir()
        .unwrap_or_else(|| PathBuf::from("."))
        .join(".hyperbytedb_history")
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn resolve_host_with_port() {
        assert_eq!(
            resolve_host(Some("db.example.com"), Some(9090), false),
            "http://db.example.com:9090"
        );
    }

    #[test]
    fn resolve_host_full_url() {
        assert_eq!(
            resolve_host(Some("https://db.example.com:443"), None, false),
            "https://db.example.com:443"
        );
    }
}
