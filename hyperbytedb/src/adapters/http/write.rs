use axum::{
    extract::{Query, State},
    http::{HeaderMap, StatusCode, header::CONTENT_TYPE},
    response::{IntoResponse, Response},
};
use serde::Deserialize;
use tracing::Instrument;

use crate::application::system_trace::{self, PhaseTimer};
use crate::error::HyperbytedbError;
use crate::ports::ingestion::WritePayloadFormat;
use metrics::{counter, histogram};

use super::auth_middleware::AuthenticatedUser;
use super::router::AppState;

#[cfg(not(feature = "columnar-ingest"))]
const COLUMNAR_MSGPACK_V1_CT: &str = "application/vnd.hyperbytedb.columnar-msgpack.v1";

#[derive(Debug, Deserialize)]
pub struct WriteParams {
    #[serde(default)]
    pub db: Option<String>,
    #[serde(default)]
    pub rp: Option<String>,
    #[serde(default)]
    pub precision: Option<String>,
    #[serde(default)]
    pub u: Option<String>,
    #[serde(default)]
    pub p: Option<String>,
}

pub async fn handle_write(
    State(state): State<std::sync::Arc<AppState>>,
    auth_user: Option<axum::Extension<AuthenticatedUser>>,
    headers: HeaderMap,
    Query(params): Query<WriteParams>,
    body: axum::body::Bytes,
) -> Result<Response, HyperbytedbError> {
    let mut cluster_pt = PhaseTimer::start();
    // Reject writes if this node is draining or syncing
    if let Some(ref membership) = state.membership {
        let m = membership.read().await;
        if let Some(node) = m.get_node(state.node_id) {
            use crate::domain::cluster::membership::NodeState;
            match node.state {
                NodeState::Draining | NodeState::Leaving => {
                    let active_peer = m
                        .active_peers(state.node_id)
                        .first()
                        .map(|n| n.addr.clone());
                    drop(m);
                    let mut resp = StatusCode::SERVICE_UNAVAILABLE.into_response();
                    if let Some(peer) = active_peer {
                        resp.headers_mut()
                            .insert("Retry-After", axum::http::HeaderValue::from_static("5"));
                        resp.headers_mut().insert(
                            "X-Hyperbytedb-Redirect",
                            axum::http::HeaderValue::from_str(&peer).unwrap_or_else(|_| {
                                axum::http::HeaderValue::from_static("unknown")
                            }),
                        );
                    }
                    return Ok(resp);
                }
                NodeState::Syncing | NodeState::Joining => {
                    return Ok(StatusCode::SERVICE_UNAVAILABLE.into_response());
                }
                _ => {}
            }
        }
    }

    let db = params
        .db
        .filter(|s| !s.is_empty())
        .ok_or(HyperbytedbError::DatabaseRequired)?;

    let ct_norm = normalized_content_type(&headers);
    let write_format = write_payload_format_from_headers(&ct_norm);
    let format_label = write_format_label(write_format);
    let span = system_trace::client_write_span(&db, format_label, body.len());
    cluster_pt.record_phase_final("cluster_check_us");

    let total_start = system_trace::start_timer();
    let db_owned = db.clone();
    let rp = params.rp.clone();
    let precision = params.precision.clone();
    let auth_user = auth_user.map(|u| u.0);
    let mut write_succeeded = false;

    let outcome = async {
        let mut auth_pt = PhaseTimer::start();
        if let Some(ref user) = auth_user
            && !user.user.can_write(&db_owned)
        {
            auth_pt.record_phase_final("auth_us");
            system_trace::record_bool("auth_denied", true);
            return Ok((
                StatusCode::FORBIDDEN,
                format!(
                    "user '{}' is not authorized to write to database '{}'",
                    user.username, db_owned
                ),
            )
                .into_response());
        }
        auth_pt.record_phase_final("auth_us");

        let mut gzip_pt = PhaseTimer::start();
        let decompressed = maybe_decompress_gzip(&headers, &body)?;
        let payload: &[u8] = decompressed.as_deref().unwrap_or(&body);
        gzip_pt.record_phase_final("gzip_us");
        system_trace::record_usize("payload_bytes", payload.len());
        histogram!("hyperbytedb_write_payload_bytes").record(payload.len() as f64);

        #[cfg(not(feature = "columnar-ingest"))]
        if ct_norm.eq_ignore_ascii_case(COLUMNAR_MSGPACK_V1_CT) {
            return Ok((
                StatusCode::UNSUPPORTED_MEDIA_TYPE,
                "columnar-ingest feature is not enabled for this build",
            )
                .into_response());
        }

        let mut ingest_pt = PhaseTimer::start();
        let result = state
            .ingestion
            .ingest(
                &db_owned,
                rp.as_deref(),
                precision.as_deref(),
                payload,
                write_format,
            )
            .await;
        ingest_pt.record_phase_final("ingest_us");

        if let Some(start) = total_start {
            histogram!("hyperbytedb_write_duration_seconds").record(start.elapsed().as_secs_f64());
        }

        match result {
            Ok(()) => {
                write_succeeded = true;
                counter!("hyperbytedb_write_requests_total").increment(1);
                Ok(StatusCode::NO_CONTENT.into_response())
            }
            Err(e) => {
                counter!("hyperbytedb_write_requests_total").increment(1);
                counter!("hyperbytedb_write_errors_total").increment(1);
                Err(e)
            }
        }
    }
    .instrument(span.clone())
    .await;

    match &outcome {
        Ok(_) if write_succeeded => {
            system_trace::record_bool("ok", true);
            system_trace::finish_span(&span, total_start, "client write complete");
        }
        Ok(_) => {
            system_trace::record_bool("ok", false);
            system_trace::finish_span(&span, total_start, "client write rejected");
        }
        Err(_) => {
            system_trace::record_bool("ok", false);
            system_trace::finish_span(&span, total_start, "client write failed");
        }
    }

    outcome
}

fn write_format_label(format: WritePayloadFormat) -> &'static str {
    match format {
        WritePayloadFormat::LineProtocol => "line_protocol",
        WritePayloadFormat::Msgpack => "msgpack",
        #[cfg(feature = "columnar-ingest")]
        WritePayloadFormat::ColumnarMsgpack => "columnar_msgpack",
    }
}

fn normalized_content_type(headers: &HeaderMap) -> String {
    headers
        .get(CONTENT_TYPE)
        .and_then(|v| v.to_str().ok())
        .unwrap_or("")
        .split(';')
        .next()
        .unwrap_or("")
        .trim()
        .to_ascii_lowercase()
}

fn write_payload_format_from_headers(ct_norm: &str) -> WritePayloadFormat {
    if ct_norm == "application/msgpack" {
        WritePayloadFormat::Msgpack
    } else {
        #[cfg(feature = "columnar-ingest")]
        if ct_norm == crate::application::columnar_msgpack::CONTENT_TYPE {
            return WritePayloadFormat::ColumnarMsgpack;
        }
        WritePayloadFormat::LineProtocol
    }
}

fn maybe_decompress_gzip(
    headers: &HeaderMap,
    body: &[u8],
) -> Result<Option<Vec<u8>>, HyperbytedbError> {
    let is_gzip = headers
        .get("content-encoding")
        .and_then(|v| v.to_str().ok())
        .is_some_and(|v| v.eq_ignore_ascii_case("gzip"));

    if is_gzip {
        use flate2::read::GzDecoder;
        use std::io::Read;
        let mut decoder = GzDecoder::new(body);
        let mut decompressed = Vec::with_capacity(body.len() * 2);
        decoder.read_to_end(&mut decompressed).map_err(|e| {
            HyperbytedbError::LineProtocolParse {
                line: String::new(),
                reason: format!("gzip decompression failed: {e}"),
            }
        })?;
        Ok(Some(decompressed))
    } else {
        Ok(None)
    }
}
