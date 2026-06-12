use crate::error::{CliError, Result};

use super::HyperbytedbClient;

#[derive(Debug, Clone)]
pub struct WriteOptions {
    pub db: String,
    pub rp: Option<String>,
    pub precision: Option<String>,
    pub gzip: bool,
    pub consistency: Option<String>,
}

impl HyperbytedbClient {
    pub async fn write(&self, body: &[u8], opts: &WriteOptions) -> Result<()> {
        let mut pairs: Vec<(&str, String)> = vec![("db", opts.db.clone())];
        if let Some(ref rp) = opts.rp {
            pairs.push(("rp", rp.clone()));
        }
        if let Some(ref precision) = opts.precision {
            pairs.push(("precision", precision.clone()));
        }
        if let Some(ref consistency) = opts.consistency {
            pairs.push(("consistency", consistency.clone()));
        }
        // Credentials travel in the Authorization header (see `auth_headers`),
        // not as `u`/`p` query params, to avoid leaking them into URLs and logs.

        let payload = if opts.gzip {
            use flate2::Compression;
            use flate2::write::GzEncoder;
            use std::io::Write;
            let mut enc = GzEncoder::new(Vec::new(), Compression::default());
            enc.write_all(body)
                .map_err(|e| CliError::Write(e.to_string()))?;
            enc.finish().map_err(|e| CliError::Write(e.to_string()))?
        } else {
            body.to_vec()
        };

        let query =
            serde_urlencoded::to_string(&pairs).map_err(|e| CliError::Write(e.to_string()))?;
        let mut headers = vec![];
        if opts.gzip {
            headers.push(("Content-Encoding".to_string(), "gzip".to_string()));
        }
        for (k, v) in self.auth_headers() {
            headers.push((k, v));
        }
        let header_refs: Vec<(&str, &str)> = headers
            .iter()
            .map(|(k, v)| (k.as_str(), v.as_str()))
            .collect();

        let resp = self
            .request("POST", "/write", &query, &header_refs, Some(payload))
            .await?;
        if (200..300).contains(&resp.status) {
            return Ok(());
        }
        let body = String::from_utf8_lossy(&resp.body);
        Err(CliError::from_status(
            reqwest::StatusCode::from_u16(resp.status).unwrap_or(reqwest::StatusCode::BAD_REQUEST),
            &body,
        ))
    }
}
