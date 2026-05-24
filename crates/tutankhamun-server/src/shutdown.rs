//! Graceful-shutdown coordination.
//!
//! A single `ShutdownHandle` is created at startup. Long-running tasks hold a
//! clone and `.await` on `shutdown_requested()` to learn when to drain. The
//! `main` task waits for SIGTERM / SIGINT, signals shutdown, then waits up to
//! `shutdown_timeout` for all holders to drop before exiting.

use std::time::Duration;

use tokio::sync::watch;
use tokio::time::timeout;
use tracing::{info, warn};

/// Handle distributed to subsystems for cooperative drain.
///
/// Backed by `tokio::sync::watch` so that `shutdown_requested()` is race-free:
/// late callers that arrive *after* `trigger()` will see the new value
/// immediately rather than missing a wakeup.
#[derive(Clone)]
pub struct ShutdownHandle {
    tx: watch::Sender<bool>,
    rx: watch::Receiver<bool>,
}

impl ShutdownHandle {
    #[must_use]
    pub fn new() -> Self {
        let (tx, rx) = watch::channel(false);
        Self { tx, rx }
    }

    /// Resolves once shutdown has been requested. Resolves immediately if
    /// `trigger` has already fired.
    pub async fn shutdown_requested(&self) {
        let mut rx = self.rx.clone();
        // `wait_for` checks the current value before parking, so a value that
        // was already set is observed without awaiting. The sender lives for
        // the daemon's lifetime, so the `Err(_)` branch (sender dropped) is
        // not reachable in normal operation; treat it as "shutdown anyway".
        let _ = rx.wait_for(|v| *v).await;
    }

    /// Has shutdown been requested?
    #[must_use]
    pub fn is_shutting_down(&self) -> bool {
        *self.rx.borrow()
    }

    /// Trigger shutdown. Idempotent — sending the same value is a no-op
    /// from watchers' perspective, and `wait_for` already considers `true`
    /// satisfied.
    pub fn trigger(&self) {
        // `send_replace` notifies receivers even if the value is unchanged,
        // but since we only ever transition false → true, a normal `send`
        // is sufficient and naturally idempotent.
        let _ = self.tx.send(true);
    }
}

impl Default for ShutdownHandle {
    fn default() -> Self {
        Self::new()
    }
}

/// Listen for OS shutdown signals. On Unix: SIGTERM or SIGINT (Ctrl-C).
/// On other platforms: Ctrl-C only.
#[cfg(unix)]
pub async fn wait_for_signal() {
    use tokio::signal::unix::{SignalKind, signal};

    let mut sigterm = signal(SignalKind::terminate()).expect("install SIGTERM handler");
    let mut sigint = signal(SignalKind::interrupt()).expect("install SIGINT handler");

    tokio::select! {
        _ = sigterm.recv() => info!("received SIGTERM"),
        _ = sigint.recv()  => info!("received SIGINT"),
    }
}

#[cfg(not(unix))]
pub async fn wait_for_signal() {
    tokio::signal::ctrl_c()
        .await
        .expect("install Ctrl-C handler");
    info!("received Ctrl-C");
}

/// Wait for `task` to complete, but no longer than `dur`. On timeout, logs a
/// warning and returns; the task is not aborted (caller decides).
pub async fn drain_with_timeout<F>(dur: Duration, task: F)
where
    F: std::future::Future<Output = ()>,
{
    if timeout(dur, task).await.is_ok() {
        info!(?dur, "drain completed");
    } else {
        warn!(?dur, "drain timed out; proceeding with shutdown");
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Regression: late awaiters must still resolve. The previous
    /// `Notify`-based implementation lost the wakeup if `trigger()` ran
    /// between the flag check and `notified().await`.
    #[tokio::test]
    async fn trigger_before_await_does_not_hang() {
        let h = ShutdownHandle::new();
        h.trigger();
        // Hard cap of 1s on a paused clock; if this hangs, the race is back.
        timeout(Duration::from_secs(1), h.shutdown_requested())
            .await
            .expect("shutdown_requested should resolve immediately after trigger");
        assert!(h.is_shutting_down());
    }

    #[tokio::test]
    async fn await_before_trigger_resolves_when_triggered() {
        let h = ShutdownHandle::new();
        let waiter = {
            let h = h.clone();
            tokio::spawn(async move { h.shutdown_requested().await })
        };
        // Give the spawned task a chance to park.
        tokio::task::yield_now().await;
        assert!(!h.is_shutting_down());
        h.trigger();
        timeout(Duration::from_secs(1), waiter)
            .await
            .expect("waiter should resolve after trigger")
            .expect("waiter task should not panic");
    }

    #[tokio::test]
    async fn many_awaiters_all_resolve() {
        let h = ShutdownHandle::new();
        let waiters: Vec<_> = (0..16)
            .map(|_| {
                let h = h.clone();
                tokio::spawn(async move { h.shutdown_requested().await })
            })
            .collect();
        tokio::task::yield_now().await;
        h.trigger();
        for w in waiters {
            timeout(Duration::from_secs(1), w)
                .await
                .expect("waiter should resolve")
                .expect("waiter task should not panic");
        }
    }

    #[tokio::test]
    async fn trigger_is_idempotent() {
        let h = ShutdownHandle::new();
        h.trigger();
        h.trigger();
        h.trigger();
        assert!(h.is_shutting_down());
        timeout(Duration::from_secs(1), h.shutdown_requested())
            .await
            .expect("shutdown_requested should resolve");
    }
}
