//! The credential one node presents to another's internal routes.
//!
//! With `auth.enabled`, `/internal/*` and `/cluster/*` need an admin user.
//! Nodes and the operator carry no user, so they send the shared
//! `auth.cluster_token` in [`HEADER`] instead; every node of a cluster gets
//! the same token (the operator puts it in a Secret). Every client that talks
//! to a peer is built from [`client_builder`] so none can forget it. Only
//! peers get these clients: the token must never reach another host.

use parking_lot::RwLock;

pub const HEADER: &str = "x-hyperbytedb-cluster-token";

static TOKEN: RwLock<Option<String>> = RwLock::new(None);

/// Set at startup from `auth.cluster_token`, before any peer client is built.
pub fn set_token(token: Option<&str>) {
    *TOKEN.write() = token.filter(|t| !t.is_empty()).map(str::to_owned);
}

/// Whether `presented` is this cluster's token. False when none is set.
#[must_use]
pub fn token_matches(presented: &[u8]) -> bool {
    let token = TOKEN.read();
    let Some(token) = token.as_deref() else {
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

/// A client builder for requests to peers: it sends the cluster token.
pub fn client_builder() -> reqwest::ClientBuilder {
    let builder = reqwest::Client::builder();
    let token = TOKEN.read();
    match token
        .as_deref()
        .and_then(|t| reqwest::header::HeaderValue::from_str(t).ok())
    {
        Some(mut value) => {
            value.set_sensitive(true);
            let mut headers = reqwest::header::HeaderMap::new();
            headers.insert(HEADER, value);
            builder.default_headers(headers)
        }
        None => builder,
    }
}

/// A default client for requests to peers.
#[must_use]
pub fn client() -> reqwest::Client {
    client_builder().build().unwrap_or_default()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn only_the_set_token_matches() {
        set_token(None);
        assert!(!token_matches(b""));
        assert!(!token_matches(b"anything"));
        set_token(Some(""));
        assert!(!token_matches(b""), "an empty token is no token");
        set_token(Some("s3cret"));
        assert!(token_matches(b"s3cret"));
        assert!(!token_matches(b"s3cre"));
        assert!(!token_matches(b"s3cret!"));
        assert!(!token_matches(b"S3cret"));
        set_token(None);
    }
}
