use std::sync::Arc;

use axum::{
    extract::State,
    http::{HeaderMap, StatusCode},
    middleware::Next,
    response::{IntoResponse, Response},
};
use serde::Deserialize;

use crate::domain::user::StoredUser;

use super::router::AppState;

#[derive(Debug, Clone)]
pub struct AuthenticatedUser {
    pub username: String,
    pub user: StoredUser,
}

#[derive(Debug, Deserialize, Default)]
pub struct AuthParams {
    #[serde(default)]
    pub u: Option<String>,
    #[serde(default)]
    pub p: Option<String>,
}

pub async fn auth_layer(
    State(state): State<Arc<AppState>>,
    headers: HeaderMap,
    query: axum::extract::Query<AuthParams>,
    mut request: axum::extract::Request,
    next: Next,
) -> Response {
    if !state.auth_enabled {
        return next.run(request).await;
    }

    let credentials =
        extract_credentials(&headers, &query, state.auth_allow_query_param_credentials);

    match credentials {
        Some((user, pass)) => match state.auth.authenticate_user(&user, &pass).await {
            Ok(Some(stored_user)) => {
                request.extensions_mut().insert(AuthenticatedUser {
                    username: user,
                    user: stored_user,
                });
                next.run(request).await
            }
            _ => (StatusCode::UNAUTHORIZED, "authorization failed").into_response(),
        },
        None => (StatusCode::UNAUTHORIZED, "authorization failed").into_response(),
    }
}

fn extract_credentials(
    headers: &HeaderMap,
    query: &AuthParams,
    allow_query_params: bool,
) -> Option<(String, String)> {
    // 1. Query parameters (opt-in; see `[auth] allow_query_param_credentials`)
    if allow_query_params
        && let (Some(u), Some(p)) = (&query.u, &query.p)
        && !u.is_empty()
    {
        return Some((u.clone(), p.clone()));
    }

    // 2. Basic auth header
    if let Some(auth) = headers.get("authorization").and_then(|v| v.to_str().ok()) {
        if let Some(basic) = auth.strip_prefix("Basic ")
            && let Ok(decoded) = base64_decode(basic.trim())
            && let Some((user, pass)) = decoded.split_once(':')
        {
            return Some((user.to_string(), pass.to_string()));
        }
        // 3. Token auth header (InfluxDB v1: token = "user:pass")
        if let Some(token) = auth.strip_prefix("Token ")
            && let Some((user, pass)) = token.trim().split_once(':')
        {
            return Some((user.to_string(), pass.to_string()));
        }
    }

    None
}

fn base64_decode(input: &str) -> Result<String, ()> {
    use base64::Engine;
    let decoded = base64::engine::general_purpose::STANDARD
        .decode(input.trim())
        .map_err(|_| ())?;
    String::from_utf8(decoded).map_err(|_| ())
}

/// Auth layer for internal cluster routes (/internal/*, /cluster/*).
/// When auth is enabled, requires valid admin credentials.
/// When auth is disabled, allows all requests (assumes network isolation).
pub async fn internal_auth_layer(
    State(state): State<Arc<AppState>>,
    headers: HeaderMap,
    query: axum::extract::Query<AuthParams>,
    request: axum::extract::Request,
    next: Next,
) -> Response {
    if !state.auth_enabled {
        return next.run(request).await;
    }

    let credentials =
        extract_credentials(&headers, &query, state.auth_allow_query_param_credentials);

    match credentials {
        Some((user, pass)) => match state.auth.authenticate_user(&user, &pass).await {
            Ok(Some(stored_user)) if stored_user.admin => next.run(request).await,
            _ => (
                StatusCode::FORBIDDEN,
                "admin privileges required for internal routes",
            )
                .into_response(),
        },
        None => (StatusCode::UNAUTHORIZED, "authorization failed").into_response(),
    }
}

fn rate_limit_response() -> Response {
    metrics::counter!("hyperbytedb_rate_limit_denied_total").increment(1);
    (
        StatusCode::TOO_MANY_REQUESTS,
        "rate limit exceeded, try again later",
    )
        .into_response()
}

fn check_rate_limit(bucket: &super::rate_limit::TokenBucket) -> bool {
    bucket.try_acquire()
}

/// Rate-limiting middleware for `/write`.
pub async fn rate_limit_write_layer(
    State(state): State<Arc<AppState>>,
    request: axum::extract::Request,
    next: Next,
) -> Response {
    if let Some(ref limiters) = state.rate_limiter
        && !check_rate_limit(&limiters.write)
    {
        return rate_limit_response();
    }
    next.run(request).await
}

/// Rate-limiting middleware for `/query`.
pub async fn rate_limit_query_layer(
    State(state): State<Arc<AppState>>,
    request: axum::extract::Request,
    next: Next,
) -> Response {
    if let Some(ref limiters) = state.rate_limiter
        && !check_rate_limit(&limiters.query)
    {
        return rate_limit_response();
    }
    next.run(request).await
}

pub fn hash_password(password: &str) -> Result<String, crate::error::HyperbytedbError> {
    use argon2::Argon2;
    use argon2::password_hash::{PasswordHasher, SaltString};
    let salt = SaltString::generate(&mut argon2::password_hash::rand_core::OsRng);
    let hash = Argon2::default()
        .hash_password(password.as_bytes(), &salt)
        .map_err(|e| {
            crate::error::HyperbytedbError::Internal(format!("password hash failed: {e}").into())
        })?;
    Ok(hash.to_string())
}

#[cfg(test)]
mod tests {
    use super::*;
    use axum::http::HeaderValue;

    fn auth_params(u: &str, p: &str) -> AuthParams {
        AuthParams {
            u: Some(u.to_string()),
            p: Some(p.to_string()),
        }
    }

    #[test]
    fn query_params_ignored_when_not_allowed() {
        let headers = HeaderMap::new();
        let query = auth_params("admin", "secret");
        assert!(extract_credentials(&headers, &query, false).is_none());
    }

    #[test]
    fn query_params_used_when_allowed() {
        let headers = HeaderMap::new();
        let query = auth_params("admin", "secret");
        assert_eq!(
            extract_credentials(&headers, &query, true),
            Some(("admin".to_string(), "secret".to_string()))
        );
    }

    #[test]
    fn basic_auth_works_when_query_params_disallowed() {
        use base64::Engine;
        let encoded = base64::engine::general_purpose::STANDARD.encode("admin:secret");
        let mut headers = HeaderMap::new();
        headers.insert(
            "authorization",
            HeaderValue::from_str(&format!("Basic {encoded}")).unwrap(),
        );
        let query = AuthParams::default();
        assert_eq!(
            extract_credentials(&headers, &query, false),
            Some(("admin".to_string(), "secret".to_string()))
        );
    }
}
