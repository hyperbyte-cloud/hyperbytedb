use crate::config::ConnectionConfig;

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

    pub fn authorization_header(&self) -> Option<(String, String)> {
        if let (Some(u), Some(p)) = (&self.username, &self.password) {
            use base64::Engine as _;
            let token = base64::engine::general_purpose::STANDARD.encode(format!("{u}:{p}"));
            Some(("Authorization".to_string(), format!("Basic {token}")))
        } else {
            self.username
                .as_ref()
                .map(|u| ("Authorization".to_string(), format!("Token {u}:")))
        }
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
}
