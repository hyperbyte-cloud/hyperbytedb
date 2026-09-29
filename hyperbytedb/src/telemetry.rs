//! Request tracing over OpenTelemetry (the `otel` feature).
//!
//! The write path's spans live on the [`TARGET`] target at DEBUG, so the log
//! layer (INFO by default) never formats them. They are recorded only when
//! the server is built with `otel` and `OTEL_EXPORTER_OTLP_ENDPOINT` is set;
//! otherwise each costs a filter check. Trace context crosses nodes in a
//! W3C `traceparent` header, so a coordinator's write and the primaries'
//! handling of its forwards land in one trace.
//!
//! Added to see where an admitted write waits (fan-out, admission, WAL,
//! scheduling), which a CPU profile cannot show. Remove this module, its
//! call sites and the `otel` feature if it stops earning its keep.

/// Target of every tracing span meant for export.
pub const TARGET: &str = "hb_trace";

/// Add the current span's trace context to an outgoing peer request.
pub fn inject(req: reqwest::RequestBuilder) -> reqwest::RequestBuilder {
    #[cfg(feature = "otel")]
    {
        otel::inject(req)
    }
    #[cfg(not(feature = "otel"))]
    {
        req
    }
}

/// Make `span` a child of the trace context in an incoming request.
pub fn set_parent(span: &tracing::Span, headers: &axum::http::HeaderMap) {
    #[cfg(feature = "otel")]
    otel::set_parent(span, headers);
    #[cfg(not(feature = "otel"))]
    let _ = (span, headers);
}

/// Install logging, plus the OTLP layer when the `otel` feature is built in
/// and `OTEL_EXPORTER_OTLP_ENDPOINT` is set. Returns a guard that flushes
/// queued spans when dropped.
pub fn init_logging(
    filter: tracing_subscriber::EnvFilter,
    json: bool,
) -> anyhow::Result<TelemetryGuard> {
    #[cfg(feature = "otel")]
    if std::env::var_os("OTEL_EXPORTER_OTLP_ENDPOINT").is_some() {
        return otel::init(filter, json);
    }
    if json {
        tracing_subscriber::fmt()
            .with_env_filter(filter)
            .json()
            .try_init()
            .map_err(|e| anyhow::anyhow!("failed to set logging subscriber: {e}"))?;
    } else {
        tracing_subscriber::fmt()
            .with_env_filter(filter)
            .try_init()
            .map_err(|e| anyhow::anyhow!("failed to set logging subscriber: {e}"))?;
    }
    Ok(TelemetryGuard::default())
}

/// Flushes and shuts down the span exporter on drop.
#[derive(Default)]
pub struct TelemetryGuard {
    #[cfg(feature = "otel")]
    provider: Option<opentelemetry_sdk::trace::SdkTracerProvider>,
}

impl Drop for TelemetryGuard {
    fn drop(&mut self) {
        #[cfg(feature = "otel")]
        if let Some(p) = self.provider.take() {
            let _ = p.shutdown();
        }
    }
}

#[cfg(feature = "otel")]
mod otel {
    use opentelemetry::propagation::{Extractor, Injector, TextMapPropagator};
    use opentelemetry::trace::TracerProvider as _;
    use opentelemetry_sdk::propagation::TraceContextPropagator;
    use opentelemetry_sdk::trace::{Sampler, SdkTracerProvider};
    use tracing_opentelemetry::OpenTelemetrySpanExt;
    use tracing_subscriber::Layer;
    use tracing_subscriber::layer::SubscriberExt;
    use tracing_subscriber::util::SubscriberInitExt;

    use super::{TARGET, TelemetryGuard};

    /// Fraction of writes traced (`HYPERBYTEDB_TRACE_SAMPLE`, default 0.02).
    /// Forwarded work follows its parent's decision.
    fn sample_ratio() -> f64 {
        std::env::var("HYPERBYTEDB_TRACE_SAMPLE")
            .ok()
            .and_then(|v| v.parse().ok())
            .unwrap_or(0.02)
    }

    pub(super) fn init(
        filter: tracing_subscriber::EnvFilter,
        json: bool,
    ) -> anyhow::Result<TelemetryGuard> {
        let exporter = opentelemetry_otlp::SpanExporter::builder()
            .with_http()
            .build()
            .map_err(|e| anyhow::anyhow!("OTLP exporter: {e}"))?;
        let provider = SdkTracerProvider::builder()
            .with_batch_exporter(exporter)
            .with_sampler(Sampler::ParentBased(Box::new(Sampler::TraceIdRatioBased(
                sample_ratio(),
            ))))
            .with_resource(
                opentelemetry_sdk::Resource::builder()
                    .with_service_name("hyperbytedb")
                    .build(),
            )
            .build();
        let tracer = provider.tracer("hyperbytedb");
        opentelemetry::global::set_text_map_propagator(TraceContextPropagator::new());

        let spans = tracing_opentelemetry::layer()
            .with_tracer(tracer)
            .with_filter(
                tracing_subscriber::filter::Targets::new()
                    .with_target(TARGET, tracing::Level::DEBUG),
            );
        let registry = tracing_subscriber::registry().with(spans);
        if json {
            registry
                .with(tracing_subscriber::fmt::layer().json().with_filter(filter))
                .try_init()
        } else {
            registry
                .with(tracing_subscriber::fmt::layer().with_filter(filter))
                .try_init()
        }
        .map_err(|e| anyhow::anyhow!("failed to set logging subscriber: {e}"))?;
        tracing::info!(ratio = sample_ratio(), "exporting request traces over OTLP");
        Ok(TelemetryGuard {
            provider: Some(provider),
        })
    }

    struct HeaderInjector<'a>(&'a mut reqwest::header::HeaderMap);

    impl Injector for HeaderInjector<'_> {
        fn set(&mut self, key: &str, value: String) {
            if let (Ok(k), Ok(v)) = (
                reqwest::header::HeaderName::from_bytes(key.as_bytes()),
                reqwest::header::HeaderValue::from_str(&value),
            ) {
                self.0.insert(k, v);
            }
        }
    }

    struct HeaderExtractor<'a>(&'a axum::http::HeaderMap);

    impl Extractor for HeaderExtractor<'_> {
        fn get(&self, key: &str) -> Option<&str> {
            self.0.get(key).and_then(|v| v.to_str().ok())
        }

        fn keys(&self) -> Vec<&str> {
            self.0.keys().map(|k| k.as_str()).collect()
        }
    }

    pub(super) fn inject(req: reqwest::RequestBuilder) -> reqwest::RequestBuilder {
        let cx = tracing::Span::current().context();
        let mut headers = reqwest::header::HeaderMap::new();
        TraceContextPropagator::new().inject_context(&cx, &mut HeaderInjector(&mut headers));
        req.headers(headers)
    }

    pub(super) fn set_parent(span: &tracing::Span, headers: &axum::http::HeaderMap) {
        let cx = TraceContextPropagator::new().extract(&HeaderExtractor(headers));
        let _ = span.set_parent(cx);
    }
}
