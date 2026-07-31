use crate::config::ConnectionConfig;
use crate::error::{CliError, Result};

#[derive(Debug, Clone)]
pub struct Credentials {
    pub username: Option<String>,
    pub password: Option<String>,
}

impl Credentials {
    pub fn from_config(cfg: &ConnectionConfig) -> Self {
        Self {
            username: cfg.username.clone(),
            password: cfg.password.clone(),
        }
    }

    /// Require both username and password when either is set.
    ///
    /// HyperbyteDB CLI uses HTTP Basic authentication only. InfluxDB v1's
    /// `Token username:` header (username without password) is not supported.
    pub fn validate(&self) -> Result<()> {
        match (&self.username, &self.password) {
            (Some(_), None) => Err(CliError::Auth(
                "password is required when username is set \
                 (use -password, HYPERBYTEDB_PASSWORD, or the REPL `auth` command)"
                    .to_string(),
            )),
            (None, Some(_)) => Err(CliError::Auth(
                "username is required when password is set \
                 (use -username, HYPERBYTEDB_USERNAME, or the REPL `auth` command)"
                    .to_string(),
            )),
            _ => Ok(()),
        }
    }

    pub fn authorization_header(&self) -> Option<(String, String)> {
        let (u, p) = (self.username.as_ref()?, self.password.as_ref()?);
        use base64::Engine as _;
        let token = base64::engine::general_purpose::STANDARD.encode(format!("{u}:{p}"));
        Some(("Authorization".to_string(), format!("Basic {token}")))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn credentials_from_config() {
        let cfg = ConnectionConfig {
            host: "http://localhost:8086".to_string(),
            database: None,
            username: Some("admin".to_string()),
            password: Some("secret".to_string()),
            ssl: false,
            unsafe_ssl: false,
            url_prefix: None,
            socket: None,
        };
        let creds = Credentials::from_config(&cfg);
        assert_eq!(creds.username.as_deref(), Some("admin"));
        assert_eq!(creds.password.as_deref(), Some("secret"));
    }

    #[test]
    fn basic_auth_header_when_both_set() {
        let creds = Credentials {
            username: Some("admin".to_string()),
            password: Some("secret".to_string()),
        };
        creds.validate().expect("valid");
        let (k, v) = creds.authorization_header().expect("header");
        assert_eq!(k, "Authorization");
        assert!(v.starts_with("Basic "));
    }

    #[test]
    fn rejects_username_without_password() {
        let creds = Credentials {
            username: Some("admin".to_string()),
            password: None,
        };
        let err = creds.validate().expect_err("must reject");
        assert!(err.to_string().contains("password is required"));
        assert!(creds.authorization_header().is_none());
    }

    #[test]
    fn rejects_password_without_username() {
        let creds = Credentials {
            username: None,
            password: Some("secret".to_string()),
        };
        let err = creds.validate().expect_err("must reject");
        assert!(err.to_string().contains("username is required"));
    }

    #[test]
    fn no_credentials_is_valid() {
        let creds = Credentials {
            username: None,
            password: None,
        };
        creds.validate().expect("anonymous ok");
        assert!(creds.authorization_header().is_none());
    }
}
