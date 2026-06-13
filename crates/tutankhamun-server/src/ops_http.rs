//! Operations HTTP server.
//!
//! Serves `/healthz`, `/readyz` on the ops port. `/metrics` and `/status`
//! land here as their subsystems are added. Distinct from the gRPC
//! data-plane port. Plain HTTP — protected by network policy until auth
//! lands.

use std::net::SocketAddr;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};

use axum::Router;
use axum::extract::State;
use axum::http::StatusCode;
use axum::http::header::CONTENT_TYPE;
use axum::response::IntoResponse;
use axum::routing::get;
use tokio::net::TcpListener;
use tracing::info;

use crate::metrics::Metrics;
use crate::shutdown::ShutdownHandle;

/// Liveness + readiness state plus the metrics surface, shared with handlers.
#[derive(Clone)]
pub struct OpsState {
    inner: Arc<OpsStateInner>,
}

struct OpsStateInner {
    ready: AtomicBool,
    metrics: Arc<Metrics>,
}

impl OpsState {
    #[must_use]
    pub fn new(metrics: Arc<Metrics>) -> Self {
        Self {
            inner: Arc::new(OpsStateInner {
                ready: AtomicBool::new(false),
                metrics,
            }),
        }
    }

    /// Mark the daemon as ready to serve traffic. Called once the data-plane
    /// subsystems have started. Release-ordered so writes that established
    /// readiness are visible to readers that observe `true`.
    pub fn mark_ready(&self) {
        self.inner.ready.store(true, Ordering::Release);
    }

    /// Mark the daemon as no longer ready (e.g. during shutdown drain).
    pub fn mark_not_ready(&self) {
        self.inner.ready.store(false, Ordering::Release);
    }

    fn is_ready(&self) -> bool {
        self.inner.ready.load(Ordering::Acquire)
    }
}

/// Bind the ops HTTP listener. Returns once the listener is accepting
/// connections, so callers can mark readiness without racing the server.
pub async fn bind(addr: SocketAddr) -> anyhow::Result<TcpListener> {
    let listener = TcpListener::bind(addr).await?;
    info!(bound = %listener.local_addr()?, "ops HTTP listening");
    Ok(listener)
}

/// Build the ops HTTP router. Factored out so tests can exercise the
/// routes via `tower::ServiceExt::oneshot` without binding a port.
fn router(state: OpsState) -> Router {
    Router::new()
        .route("/healthz", get(healthz))
        .route("/readyz", get(readyz))
        .route("/metrics", get(metrics))
        .with_state(state)
}

/// Serve until `shutdown` fires.
pub async fn serve(
    listener: TcpListener,
    state: OpsState,
    shutdown: ShutdownHandle,
) -> anyhow::Result<()> {
    let app = router(state);

    axum::serve(listener, app)
        .with_graceful_shutdown(async move {
            shutdown.shutdown_requested().await;
            info!("ops HTTP graceful shutdown signaled");
        })
        .await?;

    Ok(())
}

async fn healthz() -> impl IntoResponse {
    (StatusCode::OK, "ok")
}

async fn readyz(State(state): State<OpsState>) -> impl IntoResponse {
    if state.is_ready() {
        (StatusCode::OK, "ready")
    } else {
        (StatusCode::SERVICE_UNAVAILABLE, "not ready")
    }
}

/// Prometheus text exposition of the daemon's metrics (§3.4).
async fn metrics(State(state): State<OpsState>) -> impl IntoResponse {
    (
        StatusCode::OK,
        [(CONTENT_TYPE, "text/plain; version=0.0.4")],
        state.inner.metrics.render(),
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    use axum::body::Body;
    use axum::http::{Request, StatusCode};
    use tower::ServiceExt;

    use crate::bitmap_cache::BitmapCache;
    use crate::memory::{MemoryBudget, SessionMemoryHandle};

    fn test_state() -> OpsState {
        let budget = Arc::new(MemoryBudget::new(u64::MAX));
        let cache = Arc::new(BitmapCache::new(Arc::new(SessionMemoryHandle::new(
            Arc::clone(&budget),
            u64::MAX,
        ))));
        OpsState::new(Metrics::new(budget, cache))
    }

    fn req(uri: &str) -> Request<Body> {
        Request::builder()
            .uri(uri)
            .body(Body::empty())
            .expect("valid request")
    }

    #[tokio::test]
    async fn healthz_returns_ok_regardless_of_readiness() {
        let state = test_state();
        let response = router(state.clone())
            .oneshot(req("/healthz"))
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::OK);

        state.mark_ready();
        let response = router(state).oneshot(req("/healthz")).await.unwrap();
        assert_eq!(response.status(), StatusCode::OK);
    }

    #[tokio::test]
    async fn readyz_starts_not_ready() {
        let response = router(test_state()).oneshot(req("/readyz")).await.unwrap();
        assert_eq!(response.status(), StatusCode::SERVICE_UNAVAILABLE);
    }

    #[tokio::test]
    async fn readyz_reflects_mark_ready_toggles() {
        let state = test_state();

        state.mark_ready();
        let response = router(state.clone()).oneshot(req("/readyz")).await.unwrap();
        assert_eq!(response.status(), StatusCode::OK);

        state.mark_not_ready();
        let response = router(state).oneshot(req("/readyz")).await.unwrap();
        assert_eq!(response.status(), StatusCode::SERVICE_UNAVAILABLE);
    }

    #[tokio::test]
    async fn metrics_returns_prometheus_text() {
        let response = router(test_state()).oneshot(req("/metrics")).await.unwrap();
        assert_eq!(response.status(), StatusCode::OK);
        assert_eq!(
            response.headers().get(CONTENT_TYPE).unwrap(),
            "text/plain; version=0.0.4"
        );
        let body = axum::body::to_bytes(response.into_body(), usize::MAX)
            .await
            .unwrap();
        let text = String::from_utf8(body.to_vec()).unwrap();
        assert!(text.contains("tut_memory_limit_bytes"));
        assert!(text.contains("tut_bitmap_cache_hits_total"));
        assert!(text.contains("tut_query_duration_seconds_bucket"));
    }
}
