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

    pub fn is_some(&self) -> bool {
        self.username.is_some()
    }

    pub fn apply_basic_auth(&self, req: reqwest::RequestBuilder) -> reqwest::RequestBuilder {
        if let (Some(u), Some(p)) = (&self.username, &self.password) {
            req.basic_auth(u, Some(p))
        } else if let Some(u) = &self.username {
            req.header("Authorization", format!("Token {u}:"))
        } else {
            req
        }
    }

    pub fn apply_query_auth(&self, pairs: &mut Vec<(&str, String)>) {
        if let (Some(u), Some(p)) = (&self.username, &self.password) {
            pairs.push(("u", u.clone()));
            pairs.push(("p", p.clone()));
        }
    }

    pub fn authorization_header(&self) -> Option<(String, String)> {
        if let (Some(u), Some(p)) = (&self.username, &self.password) {
            use base64::Engine as _;
            let token = base64::engine::general_purpose::STANDARD.encode(format!("{u}:{p}"));
            Some(("Authorization".to_string(), format!("Basic {token}")))
        } else if let Some(u) = &self.username {
            Some(("Authorization".to_string(), format!("Token {u}:")))
        } else {
            None
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
        assert!(creds.is_some());
    }
}
