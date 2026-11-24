//! Simulation-only (`cfg(madsim)`) stand-in for
//! `tower_http::timeout::TimeoutLayer`.
//!
//! tower-http's layer sleeps on the real tokio timer
//! (`tokio::time::sleep` of the `tokio` crate it depends on), which has no
//! reactor inside the simulation and panics on the first request. This is
//! the same middleware, line for line, on the crate's own `tokio` (madsim's
//! simulated timer under `cfg(madsim)`): the timer is polled first and, once
//! it fires, the request is answered with an empty body and the configured
//! status; otherwise the inner service's response is returned. Production
//! builds keep tower-http's layer.

use std::future::Future;
use std::pin::Pin;
use std::task::{Context, Poll};
use std::time::Duration;

use axum::http::{Request, Response, StatusCode};
use tower::{Layer, Service};

/// See the module docs; mirrors `tower_http::timeout::TimeoutLayer`.
#[derive(Debug, Clone, Copy)]
pub struct TimeoutLayer {
    timeout: Duration,
    status_code: StatusCode,
}

impl TimeoutLayer {
    pub fn with_status_code(status_code: StatusCode, timeout: Duration) -> Self {
        Self {
            timeout,
            status_code,
        }
    }
}

impl<S> Layer<S> for TimeoutLayer {
    type Service = Timeout<S>;

    fn layer(&self, inner: S) -> Self::Service {
        Timeout {
            inner,
            timeout: self.timeout,
            status_code: self.status_code,
        }
    }
}

/// Mirrors `tower_http::timeout::Timeout`.
#[derive(Debug, Clone, Copy)]
pub struct Timeout<S> {
    inner: S,
    timeout: Duration,
    status_code: StatusCode,
}

impl<S, ReqBody, ResBody> Service<Request<ReqBody>> for Timeout<S>
where
    S: Service<Request<ReqBody>, Response = Response<ResBody>>,
    ResBody: Default,
{
    type Response = S::Response;
    type Error = S::Error;
    type Future = ResponseFuture<S::Future>;

    fn poll_ready(&mut self, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        self.inner.poll_ready(cx)
    }

    fn call(&mut self, req: Request<ReqBody>) -> Self::Future {
        ResponseFuture {
            inner: Box::pin(self.inner.call(req)),
            sleep: Box::pin(tokio::time::sleep(self.timeout)),
            status_code: self.status_code,
        }
    }
}

/// Mirrors `tower_http::timeout::ResponseFuture` (boxed instead of
/// pin-projected; simulation only).
pub struct ResponseFuture<F> {
    inner: Pin<Box<F>>,
    sleep: Pin<Box<tokio::time::Sleep>>,
    status_code: StatusCode,
}

impl<F, B, E> Future for ResponseFuture<F>
where
    F: Future<Output = Result<Response<B>, E>>,
    B: Default,
{
    type Output = Result<Response<B>, E>;

    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        if self.sleep.as_mut().poll(cx).is_ready() {
            let mut res = Response::new(B::default());
            *res.status_mut() = self.status_code;
            return Poll::Ready(Ok(res));
        }
        self.inner.as_mut().poll(cx)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use axum::Router;
    use axum::body::Body;
    use axum::routing::get;
    use tower::ServiceExt;

    #[madsim::test]
    async fn times_out_on_the_simulated_timer() {
        let app = Router::new()
            .route(
                "/slow",
                get(|| async {
                    tokio::time::sleep(Duration::from_secs(5)).await;
                    "late"
                }),
            )
            .route("/fast", get(|| async { "ok" }))
            .layer(TimeoutLayer::with_status_code(
                StatusCode::REQUEST_TIMEOUT,
                Duration::from_secs(1),
            ));
        let get_ = |uri: &str| Request::get(uri).body(Body::empty()).unwrap();

        let start = tokio::time::Instant::now();
        let slow = app.clone().oneshot(get_("/slow")).await.unwrap();
        assert_eq!(slow.status(), StatusCode::REQUEST_TIMEOUT);
        let waited = start.elapsed();
        assert!(
            waited >= Duration::from_secs(1) && waited < Duration::from_secs(2),
            "{waited:?}"
        );

        let fast = app.oneshot(get_("/fast")).await.unwrap();
        assert_eq!(fast.status(), StatusCode::OK);
    }
}
