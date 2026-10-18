use axum::http::{Response, header};
use uuid::Uuid;

/// `X-Influxdb-Version` and `X-Influxdb-Build` are kept for InfluxDB
/// clients and tools that read them from `/ping`; `X-Hyperbytedb-Version`
/// is HyperbyteDB's own, the bare version.
const VERSION: &str = concat!("HyperbyteDB-", env!("CARGO_PKG_VERSION"));
const BUILD: &str = "OSS";
const X_HYPERBYTEDB_VERSION_HEADER: &str = "X-Hyperbytedb-Version";
const REQUEST_ID_HEADER: &str = "Request-Id";
const X_REQUEST_ID_HEADER: &str = "X-Request-Id";
const X_INFLUXDB_VERSION_HEADER: &str = "X-Influxdb-Version";
const X_INFLUXDB_BUILD_HEADER: &str = "X-Influxdb-Build";

/// Response-mapping middleware that adds the version headers and a request ID.
/// Use with `axum::middleware::map_response(add_version_headers)`.
pub async fn add_version_headers<B>(mut response: Response<B>) -> Response<B> {
    let request_id = Uuid::new_v4().to_string();

    let headers = response.headers_mut();
    headers.insert(
        X_HYPERBYTEDB_VERSION_HEADER,
        header::HeaderValue::from_static(env!("CARGO_PKG_VERSION")),
    );
    if let Ok(v) = VERSION.parse::<header::HeaderValue>() {
        headers.insert(X_INFLUXDB_VERSION_HEADER, v);
    }
    if let Ok(v) = BUILD.parse::<header::HeaderValue>() {
        headers.insert(X_INFLUXDB_BUILD_HEADER, v);
    }
    if let Ok(v) = request_id.parse::<header::HeaderValue>() {
        headers.insert(REQUEST_ID_HEADER, v.clone());
        headers.insert(X_REQUEST_ID_HEADER, v);
    }

    response
}
