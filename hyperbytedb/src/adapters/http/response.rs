use axum::{
    http::{HeaderValue, StatusCode, header},
    response::{IntoResponse, Response},
};

/// Build an InfluxDB v1-style JSON error response with the given status code and message.
pub fn error_response(status: StatusCode, msg: &str) -> Response {
    let body = influxdb_error_json(msg);
    (
        status,
        [(
            header::CONTENT_TYPE,
            HeaderValue::from_static("application/json"),
        )],
        body,
    )
        .into_response()
}

/// Returns the InfluxDB v1 error JSON format: `{"error":"msg"}`.
pub fn influxdb_error_json(msg: &str) -> String {
    serde_json::json!({ "error": msg }).to_string()
}
