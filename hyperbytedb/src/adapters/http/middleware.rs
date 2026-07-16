use axum::http::{Response, header};
use uuid::Uuid;

const VERSION: &str = "HyperbyteDB-0.8.5";
const BUILD: &str = "OSS";
const REQUEST_ID_HEADER: &str = "Request-Id";
const X_REQUEST_ID_HEADER: &str = "X-Request-Id";
const X_INFLUXDB_VERSION_HEADER: &str = "X-Influxdb-Version";
const X_INFLUXDB_BUILD_HEADER: &str = "X-Influxdb-Build";

/// Response-mapping middleware that adds InfluxDB-style version headers and a request ID.
/// Use with `axum::middleware::map_response(add_version_headers)`.
pub async fn add_version_headers<B>(mut response: Response<B>) -> Response<B> {
    let request_id = Uuid::new_v4().to_string();

    let headers = response.headers_mut();
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
