use bytes::Bytes;
use http_body_util::{BodyExt, Full};
use hyper::Request;
use hyper_util::client::legacy::Client;
use hyper_util::rt::TokioExecutor;

use crate::error::{CliError, Result};

#[cfg(unix)]
use hyperlocal::Uri;

pub struct RawResponse {
    pub status: u16,
    pub body: Vec<u8>,
    pub version: Option<String>,
    pub build: Option<String>,
}

pub enum HttpBackend {
    Reqwest(reqwest::Client),
    #[cfg(unix)]
    Unix {
        socket: std::path::PathBuf,
        client: Client<hyperlocal::UnixConnector, Full<Bytes>>,
    },
}

impl HttpBackend {
    pub fn from_config(config: &crate::config::ConnectionConfig, verbose: bool) -> Result<Self> {
        if let Some(ref socket) = config.socket {
            #[cfg(unix)]
            {
                if verbose {
                    eprintln!("[verbose] using unix socket {}", socket.display());
                }
                let client = Client::builder(TokioExecutor::new()).build(hyperlocal::UnixConnector);
                return Ok(Self::Unix {
                    socket: socket.clone(),
                    client,
                });
            }
            #[cfg(not(unix))]
            {
                let _ = socket;
                return Err(CliError::Other(
                    "unix socket connections are not supported on this platform".to_string(),
                ));
            }
        }

        let mut builder = reqwest::Client::builder();
        if config.unsafe_ssl {
            builder = builder.danger_accept_invalid_certs(true);
        }
        if verbose {
            eprintln!("[verbose] using HTTP {}", config.base_url());
        }
        Ok(Self::Reqwest(
            builder
                .build()
                .map_err(|e| CliError::Connection(e.to_string()))?,
        ))
    }

    pub async fn request(
        &self,
        method: &str,
        base: &str,
        path: &str,
        query: &str,
        headers: &[(&str, &str)],
        body: Option<Vec<u8>>,
        verbose: bool,
    ) -> Result<RawResponse> {
        let path_and_query = if query.is_empty() {
            path.to_string()
        } else {
            format!("{path}?{query}")
        };

        if verbose {
            eprintln!("[verbose] {method} {path_and_query}");
        }

        match self {
            Self::Reqwest(client) => {
                let url = format!("{base}{path_and_query}");
                let mut req = match method {
                    "GET" => client.get(&url),
                    "POST" => client.post(&url),
                    _ => return Err(CliError::Other(format!("unsupported method {method}"))),
                };
                for (k, v) in headers {
                    req = req.header(*k, *v);
                }
                if let Some(body) = body {
                    req = req.body(body);
                }
                let resp = req
                    .send()
                    .await
                    .map_err(|e| CliError::Connection(e.to_string()))?;
                let status = resp.status().as_u16();
                let version = resp
                    .headers()
                    .get("X-Influxdb-Version")
                    .and_then(|v| v.to_str().ok())
                    .map(str::to_string);
                let build = resp
                    .headers()
                    .get("X-Influxdb-Build")
                    .and_then(|v| v.to_str().ok())
                    .map(str::to_string);
                let body = resp
                    .text()
                    .await
                    .map_err(|e| CliError::Connection(e.to_string()))?
                    .into_bytes();
                Ok(RawResponse {
                    status,
                    body,
                    version,
                    build,
                })
            }
            #[cfg(unix)]
            Self::Unix { socket, client } => {
                let uri: hyper::Uri = Uri::new(socket, &path_and_query).into();
                let payload = Full::new(Bytes::from(body.unwrap_or_default()));
                let mut builder = Request::builder().method(method).uri(uri);
                for (k, v) in headers {
                    builder = builder.header(*k, *v);
                }
                let req = builder
                    .body(payload)
                    .map_err(|e| CliError::Connection(e.to_string()))?;
                let resp = client
                    .request(req)
                    .await
                    .map_err(|e| CliError::Connection(e.to_string()))?;
                let status = resp.status().as_u16();
                let version = resp
                    .headers()
                    .get("X-Influxdb-Version")
                    .and_then(|v| v.to_str().ok())
                    .map(str::to_string);
                let build = resp
                    .headers()
                    .get("X-Influxdb-Build")
                    .and_then(|v| v.to_str().ok())
                    .map(str::to_string);
                let body = resp
                    .into_body()
                    .collect()
                    .await
                    .map_err(|e| CliError::Connection(e.to_string()))?
                    .to_bytes()
                    .to_vec();
                Ok(RawResponse {
                    status,
                    body,
                    version,
                    build,
                })
            }
        }
    }
}
