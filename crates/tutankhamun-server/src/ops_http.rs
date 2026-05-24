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
use axum::response::IntoResponse;
use axum::routing::get;
use tokio::net::TcpListener;
use tracing::info;

use crate::shutdown::ShutdownHandle;

/// Liveness + readiness state shared with handlers.
#[derive(Clone, Default)]
pub struct OpsState {
    inner: Arc<OpsStateInner>,
}

#[derive(Default)]
struct OpsStateInner {
    ready: AtomicBool,
}

impl OpsState {
    #[must_use]
    pub fn new() -> Self {
        Self::default()
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

/// Serve until `shutdown` fires.
pub async fn serve(
    listener: TcpListener,
    state: OpsState,
    shutdown: ShutdownHandle,
) -> anyhow::Result<()> {
    let app = Router::new()
        .route("/healthz", get(healthz))
        .route("/readyz", get(readyz))
        .with_state(state);

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
