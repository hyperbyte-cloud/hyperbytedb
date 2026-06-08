use crate::error::{CliError, Result};

use super::HyperbytedbClient;

#[derive(Debug, Clone)]
pub struct WriteOptions {
    pub db: String,
    pub rp: Option<String>,
    pub precision: Option<String>,
    pub gzip: bool,
}

impl HyperbytedbClient {
    pub async fn write(&self, body: &[u8], opts: &WriteOptions) -> Result<()> {
        let url = format!("{}/write", self.base_url());
        let mut pairs: Vec<(&str, String)> = vec![("db", opts.db.clone())];
        if let Some(ref rp) = opts.rp {
            pairs.push(("rp", rp.clone()));
        }
        if let Some(ref precision) = opts.precision {
            pairs.push(("precision", precision.clone()));
        }
        self.credentials.apply_query_auth(&mut pairs);

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

        let mut req = self.http.post(&url).query(&pairs).body(payload);
        if opts.gzip {
            req = req.header("Content-Encoding", "gzip");
        }
        req = self.credentials.apply_basic_auth(req);

        let resp = req
            .send()
            .await
            .map_err(|e| CliError::Connection(e.to_string()))?;
        let status = resp.status();
        if status.is_success() {
            return Ok(());
        }
        let body = resp.text().await.unwrap_or_default();
        Err(CliError::from_status(status, &body))
    }
}
