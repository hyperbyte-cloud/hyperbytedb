//! Tracing subscriber setup: stderr logs + optional OTLP export for Tempo.
//!
//! **Log level conventions** (default filter: `[logging].level`, usually `info`):
//! - `info` — process lifecycle, service start/stop, cluster sync/drain completion,
//!   membership changes, operator-visible milestones
//! - `debug` — per-flush/per-batch/per-sync-step detail, heartbeats, DDL, CQ runs,
//!   and [`crate::application::system_trace`] phase lines when `detailed_trace` is on
//! - `warn` / `error` — recoverable and fatal failures

use opentelemetry::trace::TracerProvider as _;
use opentelemetry_otlp::WithExportConfig;
use opentelemetry_sdk::{
    Resource,
    trace::{Sampler, SdkTracerProvider},
};
use tracing_opentelemetry::OpenTelemetryLayer;
use tracing_subscriber::{EnvFilter, Layer, fmt, layer::SubscriberExt, util::SubscriberInitExt};

use crate::config::LoggingConfig;

/// Keeps the OTLP tracer provider alive until process exit.
pub struct OtelGuard {
    tracer_provider: SdkTracerProvider,
}

impl Drop for OtelGuard {
    fn drop(&mut self) {
        if let Err(e) = self.tracer_provider.shutdown() {
            eprintln!("failed to shut down OTLP tracer provider: {e}");
        }
    }
}

fn resolve_otlp_endpoint(logging: &LoggingConfig) -> Option<String> {
    let raw = logging
        .otlp_endpoint
        .as_ref()
        .map(|s| s.trim().to_string())
        .filter(|s| !s.is_empty())
        .or_else(|| {
            std::env::var("OTEL_EXPORTER_OTLP_ENDPOINT")
                .ok()
                .map(|s| s.trim().to_string())
                .filter(|s| !s.is_empty())
        })
        .or_else(|| {
            std::env::var("HYPERBYTEDB__LOGGING__OTLP_ENDPOINT")
                .ok()
                .map(|s| s.trim().to_string())
                .filter(|s| !s.is_empty())
        })?;
    Some(normalize_otlp_http_traces_endpoint(&raw))
}

/// OTLP HTTP `.with_endpoint()` expects the full traces URL (no auto `/v1/traces`).
fn normalize_otlp_http_traces_endpoint(endpoint: &str) -> String {
    let trimmed = endpoint.trim().trim_end_matches('/');
    if trimmed.ends_with("/v1/traces") {
        trimmed.to_string()
    } else {
        format!("{trimmed}/v1/traces")
    }
}

fn service_name() -> String {
    std::env::var("OTEL_SERVICE_NAME")
        .ok()
        .filter(|s| !s.is_empty())
        .unwrap_or_else(|| "hyperbytedb".to_string())
}

fn build_tracer_provider(endpoint: &str, sample_ratio: f64) -> anyhow::Result<SdkTracerProvider> {
    let exporter = opentelemetry_otlp::SpanExporter::builder()
        .with_http()
        .with_endpoint(endpoint)
        .build()
        .map_err(|e| anyhow::anyhow!("OTLP span exporter: {e}"))?;

    let resource = Resource::builder()
        .with_service_name(service_name())
        .build();

    let ratio = sample_ratio.clamp(0.0, 1.0);
    let sampler = if ratio >= 1.0 {
        Sampler::AlwaysOn
    } else if ratio <= 0.0 {
        Sampler::AlwaysOff
    } else {
        Sampler::ParentBased(Box::new(Sampler::TraceIdRatioBased(ratio)))
    };

    Ok(SdkTracerProvider::builder()
        .with_sampler(sampler)
        .with_resource(resource)
        .with_batch_exporter(exporter)
        .build())
}

/// Install the global tracing subscriber. Returns an [`OtelGuard`] when OTLP export is enabled.
pub fn init_tracing(logging: &LoggingConfig) -> anyhow::Result<Option<OtelGuard>> {
    crate::application::system_trace::set_enabled(logging.detailed_trace);

    let filter =
        EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new(&logging.level));

    let fmt_layer = match logging.format.as_str() {
        "json" => fmt::layer().json().boxed(),
        _ => fmt::layer().boxed(),
    };

    let registry = tracing_subscriber::registry().with(filter).with(fmt_layer);

    if let Some(endpoint) = resolve_otlp_endpoint(logging) {
        let provider = build_tracer_provider(&endpoint, logging.otlp_sample_ratio)?;
        let tracer = provider.tracer("hyperbytedb");
        registry
            .with(OpenTelemetryLayer::new(tracer))
            .try_init()
            .map_err(|e| anyhow::anyhow!("tracing subscriber: {e}"))?;
        tracing::info!(
            otlp_endpoint = %endpoint,
            otlp_sample_ratio = logging.otlp_sample_ratio,
            detailed_trace = logging.detailed_trace,
            service_name = %service_name(),
            "OTLP trace export enabled"
        );
        return Ok(Some(OtelGuard {
            tracer_provider: provider,
        }));
    }

    registry
        .try_init()
        .map_err(|e| anyhow::anyhow!("tracing subscriber: {e}"))?;

    if logging.detailed_trace {
        tracing::info!(
            "detailed system tracing enabled (debug-level phase logs; enable RUST_LOG=debug or logging.level=debug to view)"
        );
    }

    Ok(None)
}

#[cfg(test)]
mod tests {
    use super::normalize_otlp_http_traces_endpoint;

    #[test]
    fn appends_v1_traces_to_base_url() {
        assert_eq!(
            normalize_otlp_http_traces_endpoint("http://alloy:4318"),
            "http://alloy:4318/v1/traces"
        );
    }

    #[test]
    fn leaves_full_traces_url_unchanged() {
        assert_eq!(
            normalize_otlp_http_traces_endpoint("http://alloy:4318/v1/traces"),
            "http://alloy:4318/v1/traces"
        );
    }
}
