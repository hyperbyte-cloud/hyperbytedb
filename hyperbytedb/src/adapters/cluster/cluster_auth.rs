//! The credential one node presents to another's internal routes.
//!
//! With `auth.enabled`, `/internal/*` and `/cluster/*` need an admin user.
//! Nodes and the operator carry no user, so they send the shared
//! `auth.cluster_token` in [`HEADER`] instead; every node of a cluster gets
//! the same token (the operator puts it in a Secret). [`ClusterAuth`] is
//! built once at startup and handed to everything that talks to a peer or
//! checks a peer's request, so none can forget it. Only peers get its
//! clients: the token must never reach another host.

use std::sync::Arc;

use reqwest::header::{HeaderMap, HeaderValue};

use crate::error::HyperbytedbError;
use crate::ports::peer_http::PeerHttp;

pub const HEADER: &str = "x-hyperbytedb-cluster-token";

/// This node's cluster token: sent to peers, and accepted from them. The
/// default has none, as without `auth.cluster_token`.
#[derive(Clone, Default)]
pub struct ClusterAuth {
    token: Option<Arc<str>>,
    header: Option<HeaderValue>,
}

impl std::fmt::Debug for ClusterAuth {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ClusterAuth")
            .field("token", &self.token.as_ref().map(|_| "<redacted>"))
            .finish()
    }
}

impl ClusterAuth {
    /// From `auth.cluster_token` (empty means none). Fails if the token
    /// cannot be sent as a header, or no HTTP client can be built at all,
    /// so either stops startup instead of every peer call.
    pub fn new(token: Option<&str>) -> Result<Self, HyperbytedbError> {
        let auth = match token.filter(|t| !t.is_empty()) {
            None => Self::default(),
            Some(t) => {
                let mut header = HeaderValue::from_str(t).map_err(|_| {
                    HyperbytedbError::Internal(
                        "auth.cluster_token cannot be sent as an HTTP header".into(),
                    )
                })?;
                header.set_sensitive(true);
                Self {
                    token: Some(Arc::from(t)),
                    header: Some(header),
                }
            }
        };
        auth.client_builder().build().map_err(|e| {
            HyperbytedbError::Internal(format!("cannot build an HTTP client: {e}").into())
        })?;
        Ok(auth)
    }

    /// Whether `presented` is this cluster's token. False when none is set.
    #[must_use]
    pub fn token_matches(&self, presented: &[u8]) -> bool {
        let Some(token) = self.token.as_deref() else {
            return false;
        };
        let token = token.as_bytes();
        // Constant time over the token's length.
        token.len() == presented.len()
            && token
                .iter()
                .zip(presented)
                .fold(0u8, |acc, (a, b)| acc | (a ^ b))
                == 0
    }
}

impl PeerHttp for ClusterAuth {
    fn client_builder(&self) -> reqwest::ClientBuilder {
        let builder = reqwest::Client::builder();
        match &self.header {
            Some(value) => {
                let mut headers = HeaderMap::new();
                headers.insert(HEADER, value.clone());
                builder.default_headers(headers)
            }
            None => builder,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn only_the_configured_token_matches() {
        let none = ClusterAuth::new(None).unwrap();
        assert!(!none.token_matches(b""));
        assert!(!none.token_matches(b"anything"));
        let empty = ClusterAuth::new(Some("")).unwrap();
        assert!(!empty.token_matches(b""), "an empty token is no token");
        let auth = ClusterAuth::new(Some("s3cret")).unwrap();
        assert!(auth.token_matches(b"s3cret"));
        assert!(!auth.token_matches(b"s3cre"));
        assert!(!auth.token_matches(b"s3cret!"));
        assert!(!auth.token_matches(b"S3cret"));
    }

    #[test]
    fn a_token_that_is_not_a_header_value_is_refused() {
        assert!(ClusterAuth::new(Some("s3cret\n")).is_err());
    }

    #[test]
    fn debug_does_not_show_the_token() {
        let auth = ClusterAuth::new(Some("s3cret")).unwrap();
        assert!(!format!("{auth:?}").contains("s3cret"));
    }
}
