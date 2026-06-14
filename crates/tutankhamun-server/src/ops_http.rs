//! Operations HTTP server.
//!
//! Serves `/healthz`, `/readyz`, `/metrics` (Prometheus, §3.4), and the §3.5
//! status surface — `/status` (an embedded static HTML page), `/status.json`
//! (live daemon state), and `/favicon.svg` (the service mark) — on the ops port.
//! Distinct from the gRPC data-plane port. Plain HTTP — protected by network
//! policy until auth lands.

use std::net::SocketAddr;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Instant;

use axum::extract::State;
use axum::http::StatusCode;
use axum::http::header::CONTENT_TYPE;
use axum::response::IntoResponse;
use axum::routing::get;
use axum::{Json, Router};
use tokio::net::TcpListener;
use tracing::info;

use crate::metrics::Metrics;
use crate::shutdown::ShutdownHandle;
use crate::status::{StatusReport, StatusSource};

/// Liveness + readiness state plus the metrics surface, shared with handlers.
#[derive(Clone)]
pub struct OpsState {
    inner: Arc<OpsStateInner>,
}

struct OpsStateInner {
    ready: AtomicBool,
    metrics: Arc<Metrics>,
    /// Structural state (sessions / datasets) for `/status`, provided by the
    /// `FlightSQL` service without exposing its internals.
    status_source: Arc<dyn StatusSource>,
    /// Process start, for the `/status` uptime.
    started_at: Instant,
}

impl OpsState {
    #[must_use]
    pub fn new(
        metrics: Arc<Metrics>,
        status_source: Arc<dyn StatusSource>,
        started_at: Instant,
    ) -> Self {
        Self {
            inner: Arc::new(OpsStateInner {
                ready: AtomicBool::new(false),
                metrics,
                status_source,
                started_at,
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
        .route("/status", get(status_page))
        .route("/status.json", get(status_json))
        .route("/favicon.svg", get(favicon))
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

/// Live daemon state as JSON (§3.5): build/uptime/readiness, memory, bitmap
/// cache, query counters + recent queries, sessions, and dataset names.
async fn status_json(State(state): State<OpsState>) -> impl IntoResponse {
    Json(StatusReport {
        version: env!("CARGO_PKG_VERSION"),
        uptime_secs: state.inner.started_at.elapsed().as_secs(),
        ready: state.is_ready(),
        metrics: state.inner.metrics.status_snapshot(),
        structural: state.inner.status_source.structural_snapshot().await,
    })
}

/// The human-facing status page: a self-contained static document that fetches
/// `/status.json` and renders it client-side. Embedded so the binary needs no
/// asset directory.
async fn status_page() -> impl IntoResponse {
    (
        StatusCode::OK,
        [(CONTENT_TYPE, "text/html; charset=utf-8")],
        include_str!("status.html"),
    )
}

/// The service mark, embedded and served as the page favicon.
async fn favicon() -> impl IntoResponse {
    (
        StatusCode::OK,
        [(CONTENT_TYPE, "image/svg+xml")],
        include_str!("../assets/t9n.svg"),
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
    use crate::status::{DatasetLoad, DatasetsReport, SessionsSummary, StructuralReport};

    /// Stand-in for the `FlightSQL` service so the ops routes can be tested without it.
    struct StubStatusSource;

    #[async_trait::async_trait]
    impl StatusSource for StubStatusSource {
        async fn structural_snapshot(&self) -> StructuralReport {
            StructuralReport {
                sessions: SessionsSummary {
                    count: 0,
                    oldest_age_secs: 0,
                    reserved_bytes: 0,
                },
                datasets: DatasetsReport {
                    ok: true,
                    names: vec!["events".to_string()],
                },
                loaded: vec![DatasetLoad {
                    name: "events".to_string(),
                    shards: 2,
                    cached_bytes: 4096,
                }],
            }
        }
    }

    fn test_state() -> OpsState {
        let budget = Arc::new(MemoryBudget::new(u64::MAX));
        let cache = Arc::new(BitmapCache::new(Arc::new(SessionMemoryHandle::new(
            Arc::clone(&budget),
            u64::MAX,
        ))));
        OpsState::new(
            Metrics::new(budget, cache),
            Arc::new(StubStatusSource),
            Instant::now(),
        )
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

    async fn body_string(response: axum::response::Response) -> String {
        let body = axum::body::to_bytes(response.into_body(), usize::MAX)
            .await
            .unwrap();
        String::from_utf8(body.to_vec()).unwrap()
    }

    #[tokio::test]
    async fn status_json_returns_expected_shape() {
        let response = router(test_state())
            .oneshot(req("/status.json"))
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::OK);
        assert_eq!(
            response.headers().get(CONTENT_TYPE).unwrap(),
            "application/json"
        );
        let json: serde_json::Value =
            serde_json::from_str(&body_string(response).await).expect("valid JSON");
        for key in [
            "version",
            "uptime_secs",
            "ready",
            "memory",
            "bitmap_cache",
            "queries",
            "sessions",
            "datasets",
            "loaded",
        ] {
            assert!(json.get(key).is_some(), "missing {key} in {json}");
        }
        assert_eq!(json["datasets"]["names"][0], "events");
        assert_eq!(json["loaded"][0]["name"], "events");
        assert_eq!(json["loaded"][0]["cached_bytes"], 4096);
    }

    #[tokio::test]
    async fn status_page_is_html_referencing_the_json() {
        let response = router(test_state()).oneshot(req("/status")).await.unwrap();
        assert_eq!(response.status(), StatusCode::OK);
        assert_eq!(
            response.headers().get(CONTENT_TYPE).unwrap(),
            "text/html; charset=utf-8"
        );
        let html = body_string(response).await;
        assert!(html.contains("status.json"), "page must fetch the JSON");
        assert!(html.contains("/favicon.svg"), "page references the favicon");
    }

    #[tokio::test]
    async fn favicon_is_svg() {
        let response = router(test_state())
            .oneshot(req("/favicon.svg"))
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::OK);
        assert_eq!(
            response.headers().get(CONTENT_TYPE).unwrap(),
            "image/svg+xml"
        );
        assert!(body_string(response).await.contains("<svg"));
    }
}
