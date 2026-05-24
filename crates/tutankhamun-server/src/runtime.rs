//! Runtime helpers — Rayon pool and async-to-Rayon dispatch.
//!
//! **Tokio for I/O, Rayon for CPU-bound compute.** Async tasks dispatch CPU
//! work onto Rayon via a oneshot channel and `.await` the result; the Tokio
//! thread is freed for other I/O while the Rayon worker runs.

use std::sync::OnceLock;

use rayon::ThreadPool;
use tokio::sync::oneshot;

static RAYON_POOL: OnceLock<ThreadPool> = OnceLock::new();

/// Initialise the global Rayon pool. Call once at startup, before any
/// `spawn_cpu` call.
pub fn init_rayon(workers: usize) -> anyhow::Result<()> {
    let pool = rayon::ThreadPoolBuilder::new()
        .num_threads(workers)
        .thread_name(|i| format!("tk-rayon-{i}"))
        .build()?;
    RAYON_POOL
        .set(pool)
        .map_err(|_| anyhow::anyhow!("Rayon pool already initialised"))?;
    Ok(())
}

/// Reference the global Rayon pool. Panics if [`init_rayon`] has not run.
fn pool() -> &'static ThreadPool {
    RAYON_POOL
        .get()
        .expect("Rayon pool not initialised; call init_rayon() in main()")
}

/// Dispatch a CPU-bound closure onto the Rayon pool, returning a
/// Tokio-compatible future.
///
/// Use this from async (Tokio) code whenever you need to run substantial
/// compute. The Tokio worker is freed for other I/O while the Rayon worker
/// executes; the result is delivered back through a oneshot channel.
///
/// Panics from the closure propagate as a `JoinError`-equivalent
/// (`anyhow::Error`).
pub async fn spawn_cpu<F, T>(f: F) -> anyhow::Result<T>
where
    F: FnOnce() -> T + Send + 'static,
    T: Send + 'static,
{
    let (tx, rx) = oneshot::channel();
    pool().spawn(move || {
        // We catch unwinds so a panicking job doesn't poison the Rayon worker.
        let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(f));
        // Receiver may have been dropped (e.g. caller cancelled); ignore send
        // failure.
        let _ = tx.send(result);
    });
    match rx.await {
        Ok(Ok(value)) => Ok(value),
        Ok(Err(panic)) => Err(anyhow::anyhow!(
            "Rayon task panicked: {}",
            panic_message(&panic)
        )),
        Err(_) => Err(anyhow::anyhow!(
            "Rayon task dropped before completion (Rayon pool shut down?)"
        )),
    }
}

fn panic_message(payload: &Box<dyn std::any::Any + Send>) -> String {
    if let Some(s) = payload.downcast_ref::<&'static str>() {
        return (*s).to_string();
    }
    if let Some(s) = payload.downcast_ref::<String>() {
        return s.clone();
    }
    "<non-string panic payload>".to_string()
}
