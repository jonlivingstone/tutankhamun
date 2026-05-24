//! Tutankhamun (`t9n`) daemon entry point.
//!
//! Single binary with subcommands. Currently only `t9n serve` is implemented;
//! other subcommands land as the relevant subsystems do.

use std::time::Duration;

use clap::{Parser, Subcommand};
use tracing::{error, info};

use tutankhamun_server::config::{Config, ServeArgs, env_vars};
use tutankhamun_server::ops_http::{self, OpsState};
use tutankhamun_server::runtime;
use tutankhamun_server::shutdown::{self, ShutdownHandle};

#[derive(Parser, Debug)]
#[command(name = "t9n", version, about = "Tutankhamun (t9n) — analytics engine")]
struct Cli {
    #[command(subcommand)]
    command: Command,
}

#[derive(Subcommand, Debug)]
enum Command {
    /// Start the daemon.
    Serve(ServeArgs),
}

fn main() -> anyhow::Result<()> {
    init_tracing();

    let cli = Cli::parse();

    match &cli.command {
        Command::Serve(args) => run_serve(args),
    }
}

fn run_serve(args: &ServeArgs) -> anyhow::Result<()> {
    let config = Config::resolve(args)?;
    info!(?config, "loaded configuration");

    runtime::init_rayon(config.rayon_workers)?;
    info!(workers = config.rayon_workers, "rayon pool initialised");

    let runtime = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(config.tokio_workers)
        .thread_name("tk-tokio")
        .enable_all()
        .build()?;
    info!(workers = config.tokio_workers, "tokio runtime initialised");

    runtime.block_on(serve(config))
}

async fn serve(config: Config) -> anyhow::Result<()> {
    let shutdown = ShutdownHandle::new();
    let ops_state = OpsState::new();

    // Bind synchronously-with-async so we know the listener is up before
    // marking ready. The serve() future then runs for the daemon's lifetime.
    let ops_addr = config.ops_addr.parse()?;
    let ops_listener = ops_http::bind(ops_addr).await?;
    let ops_task = tokio::spawn({
        let shutdown = shutdown.clone();
        let state = ops_state.clone();
        async move {
            if let Err(e) = ops_http::serve(ops_listener, state, shutdown).await {
                error!(error = ?e, "ops HTTP server failed");
            }
        }
    });

    // TODO: gRPC data-plane bind goes here. Once it lands, gate mark_ready
    // on both bindings succeeding.
    ops_state.mark_ready();
    info!(
        ops_addr = %config.ops_addr,
        grpc_addr = %config.grpc_addr,
        "daemon ready"
    );

    shutdown::wait_for_signal().await;

    info!("shutdown requested; draining");
    ops_state.mark_not_ready();
    shutdown.trigger();

    let timeout = Duration::from_secs(config.shutdown_timeout_secs);
    shutdown::drain_with_timeout(timeout, async {
        let _ = ops_task.await;
    })
    .await;

    info!("shutdown complete");
    Ok(())
}

fn init_tracing() {
    use tracing_subscriber::EnvFilter;
    use tracing_subscriber::fmt;

    let filter = EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("info"));

    if std::env::var(env_vars::LOG_JSON).as_deref() == Ok("1") {
        fmt().json().with_env_filter(filter).init();
    } else {
        fmt().with_env_filter(filter).init();
    }
}
