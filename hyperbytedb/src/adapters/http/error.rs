use axum::{
    http::StatusCode,
    response::{IntoResponse, Response},
};

use crate::error::HyperbytedbError;

use super::response;

impl IntoResponse for HyperbytedbError {
    fn into_response(self) -> Response {
        let (status, msg) = error_to_status_and_message(&self);
        response::error_response(status, &msg)
    }
}

/// Map HyperbytedbError variants to HTTP status codes and error messages.
fn error_to_status_and_message(err: &HyperbytedbError) -> (StatusCode, String) {
    let msg = err.to_string();
    let status = match err {
        HyperbytedbError::DatabaseNotFound(_) => StatusCode::NOT_FOUND,
        HyperbytedbError::RetentionPolicyNotFound(_) => StatusCode::NOT_FOUND,
        HyperbytedbError::LineProtocolParse { .. }
        | HyperbytedbError::MsgpackParse { .. }
        | HyperbytedbError::ColumnarMsgpackParse { .. }
        | HyperbytedbError::FieldTypeConflict { .. }
        | HyperbytedbError::WallClockTimestampUnavailable => StatusCode::BAD_REQUEST,
        HyperbytedbError::AuthFailed => StatusCode::UNAUTHORIZED,
        HyperbytedbError::Forbidden(_) => StatusCode::FORBIDDEN,
        HyperbytedbError::DatabaseRequired
        | HyperbytedbError::MissingParameter(_)
        | HyperbytedbError::QueryParse(_) => StatusCode::BAD_REQUEST,
        HyperbytedbError::CardinalityExceeded { .. } => StatusCode::UNPROCESSABLE_ENTITY,
        HyperbytedbError::RequestPointLimitExceeded { .. } => StatusCode::PAYLOAD_TOO_LARGE,
        HyperbytedbError::WalBackpressure { .. } => StatusCode::SERVICE_UNAVAILABLE,
        HyperbytedbError::QueryTimeout => StatusCode::REQUEST_TIMEOUT,
        HyperbytedbError::ClusterUnavailable(_) => StatusCode::SERVICE_UNAVAILABLE,
        HyperbytedbError::PeerUnreachable(_) => StatusCode::BAD_GATEWAY,
        HyperbytedbError::SyncFailed(_) => StatusCode::INTERNAL_SERVER_ERROR,
        HyperbytedbError::ReplicationTimeout(_) => StatusCode::GATEWAY_TIMEOUT,
        HyperbytedbError::ReplicationQuorumTimeout { .. } => StatusCode::GATEWAY_TIMEOUT,
        HyperbytedbError::Wal(_)
        | HyperbytedbError::Storage(_)
        | HyperbytedbError::Chdb(_)
        | HyperbytedbError::Metadata(_)
        | HyperbytedbError::Internal(_) => StatusCode::INTERNAL_SERVER_ERROR,
    };
    (status, msg)
}
