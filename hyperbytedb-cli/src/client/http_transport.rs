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

pub struct HttpRequest<'a> {
    pub method: &'a str,
    pub base: &'a str,
    pub path: &'a str,
    pub query: &'a str,
    pub headers: &'a [(&'a str, &'a str)],
    pub body: Option<Vec<u8>>,
    pub verbose: bool,
}

type UnixClient = Client<hyperlocal::UnixConnector, Full<Bytes>>;

pub enum HttpBackend {
    Reqwest(reqwest::Client),
    #[cfg(unix)]
    Unix {
        socket: std::path::PathBuf,
        client: Box<UnixClient>,
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
                    client: Box::new(client),
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

    pub async fn request(&self, req: HttpRequest<'_>) -> Result<RawResponse> {
        let path_and_query = if req.query.is_empty() {
            req.path.to_string()
        } else {
            format!("{}?{}", req.path, req.query)
        };

        if req.verbose {
            eprintln!("[verbose] {} {path_and_query}", req.method);
        }

        match self {
            Self::Reqwest(client) => {
                let url = format!("{}{path_and_query}", req.base);
                let mut http_req = match req.method {
                    "GET" => client.get(&url),
                    "POST" => client.post(&url),
                    _ => {
                        return Err(CliError::Other(format!(
                            "unsupported method {}",
                            req.method
                        )));
                    }
                };
                for (k, v) in req.headers {
                    http_req = http_req.header(*k, *v);
                }
                if let Some(body) = req.body {
                    http_req = http_req.body(body);
                }
                let resp = http_req
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
                let payload = Full::new(Bytes::from(req.body.unwrap_or_default()));
                let mut builder = Request::builder().method(req.method).uri(uri);
                for (k, v) in req.headers {
                    builder = builder.header(*k, *v);
                }
                let hyper_req = builder
                    .body(payload)
                    .map_err(|e| CliError::Connection(e.to_string()))?;
                let resp = client
                    .request(hyper_req)
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
