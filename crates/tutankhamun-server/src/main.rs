//! Tutankhamun (`t9n`) daemon entry point.
//!
//! Single binary with subcommands. `t9n serve` runs the daemon; `t9n storage`
//! groups operator commands for the storage backend.

use std::io;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;

use clap::{Args, Parser, Subcommand};
use tracing::{error, info, warn};

use tutankhamun_server::config::{Config, ServeArgs, env_vars};
use tutankhamun_server::ops_http::{self, OpsState};
use tutankhamun_server::runtime;
use tutankhamun_server::shard;
use tutankhamun_server::shard_source::{
    ObjectStoreShardSource, ShardManager, ShardSource, ShardSummary,
};
use tutankhamun_server::shutdown::{self, ShutdownHandle};
use tutankhamun_server::storage::{self, StorageRegistry};

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
    /// Storage backend operations (admin / verification).
    Storage(StorageArgs),
    /// Shard inspection / maintenance.
    Shard(ShardArgs),
}

#[derive(Args, Debug)]
struct StorageArgs {
    #[command(subcommand)]
    command: StorageCommand,
}

#[derive(Args, Debug)]
struct ShardArgs {
    #[command(subcommand)]
    command: ShardCommand,
}

#[derive(Subcommand, Debug)]
enum ShardCommand {
    /// Print a shard's metadata and schema.
    Inspect {
        /// Path to the shard directory (containing metadata.json + metrics.arrow).
        path: PathBuf,
    },
    /// List every shard discovered at a storage URL.
    List {
        /// Storage backend URL (e.g. `s3://bucket/prefix`,
        /// `file:///var/data`). Defaults to `TUT_STORAGE_URL`.
        #[arg(
            long,
            env = env_vars::STORAGE_URL,
            help = "Storage backend URL (e.g. s3://bucket/prefix, file:///var/data). \
                    Defaults to TUT_STORAGE_URL."
        )]
        url: String,
    },
    /// Sum a metric column over the docs in a shard, optionally
    /// restricted to those matching a single string-field term.
    Query {
        /// Path to the shard directory.
        path: PathBuf,
        /// Metric column to sum.
        #[arg(long)]
        metric: String,
        /// Optional `<field>=<term>` restriction.
        #[arg(long)]
        filter: Option<String>,
    },
}

#[derive(Subcommand, Debug)]
enum StorageCommand {
    /// Verify that a storage URL is reachable by listing a prefix.
    Check {
        /// Storage backend URL (e.g. `s3://bucket/prefix`,
        /// `file:///var/data`). Defaults to `TUT_STORAGE_URL`.
        #[arg(
            long,
            env = env_vars::STORAGE_URL,
            help = "Storage backend URL (e.g. s3://bucket/prefix, file:///var/data). \
                    Defaults to TUT_STORAGE_URL."
        )]
        url: String,
        /// Prefix to list under (default: backend root).
        #[arg(long)]
        prefix: Option<String>,
    },
}

fn main() -> anyhow::Result<()> {
    init_tracing();

    let cli = Cli::parse();

    match &cli.command {
        Command::Serve(args) => run_serve(args),
        Command::Storage(args) => run_storage(args),
        Command::Shard(args) => run_shard(args),
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
    let storage = Arc::new(StorageRegistry::from_url(&config.storage_url)?);
    info!(storage_url = %config.storage_url, "storage backend ready");

    let shutdown = ShutdownHandle::new();
    let ops_state = OpsState::new();

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

    ops_state.mark_ready();
    info!(
        ops_addr = %config.ops_addr,
        grpc_addr = %config.grpc_addr,
        "daemon ready"
    );

    // Run the startup scans as a background task so they don't block reaching
    // the signal handler — a slow LIST against a large S3 bucket would
    // otherwise widen the window in which SIGTERM bypasses the graceful
    // drain. The scans are informational; aborting mid-flight on shutdown
    // is safe.
    let scan_task = tokio::spawn({
        let storage = Arc::clone(&storage);
        async move {
            // Both scans hit the same backend independently; running them
            // concurrently bounds startup-scan wall-clock by the slower one
            // rather than their sum.
            let store = storage.store();
            tokio::join!(
                log_storage_scan(&*store),
                log_shard_scan(Arc::clone(&store)),
            );
        }
    });

    shutdown::wait_for_signal().await;

    scan_task.abort();

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

fn run_storage(args: &StorageArgs) -> anyhow::Result<()> {
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()?;
    match &args.command {
        StorageCommand::Check { url, prefix } => {
            runtime.block_on(run_storage_check(url, prefix.as_deref()))
        }
    }
}

async fn run_storage_check(url: &str, prefix: Option<&str>) -> anyhow::Result<()> {
    let registry = StorageRegistry::from_url(url)?;
    storage::check(&*registry.store(), prefix).await.map(|_| ())
}

fn run_shard(args: &ShardArgs) -> anyhow::Result<()> {
    match &args.command {
        ShardCommand::Inspect { path } => {
            let stdout = io::stdout();
            let mut out = stdout.lock();
            shard::inspect(path, &mut out)
        }
        ShardCommand::List { url } => {
            let runtime = tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()?;
            runtime.block_on(run_shard_list(url))
        }
        ShardCommand::Query {
            path,
            metric,
            filter,
        } => {
            let parsed = filter.as_deref().map(parse_filter).transpose()?;
            let stdout = io::stdout();
            let mut out = stdout.lock();
            shard::query(path, parsed, metric, &mut out)
        }
    }
}

/// Splits at the first `=`. Terms may contain further `=` characters.
/// Both halves must be non-empty.
fn parse_filter(s: &str) -> anyhow::Result<(&str, &str)> {
    let (field, term) = s
        .split_once('=')
        .ok_or_else(|| anyhow::anyhow!("filter must be `<field>=<term>` (got {s:?})"))?;
    if field.is_empty() || term.is_empty() {
        anyhow::bail!("filter must be `<field>=<term>` with both sides non-empty (got {s:?})");
    }
    Ok((field, term))
}

async fn run_shard_list(url: &str) -> anyhow::Result<()> {
    let registry = StorageRegistry::from_url(url)?;
    let source: Arc<dyn ShardSource> = Arc::new(ObjectStoreShardSource::new(registry.store()));
    let mut manager = ShardManager::new(source);
    manager.refresh().await?;

    let shards = manager.all_shards();
    if shards.is_empty() {
        println!("no shards discovered at {url}");
        return Ok(());
    }

    let widest_loc = shards
        .iter()
        .map(|s| s.location.as_ref().len())
        .max()
        .unwrap_or(0);
    for s in shards {
        println!(
            "{:<width$}  {:>10} docs  {} .. {}",
            s.location.as_ref(),
            s.metadata.num_docs,
            s.metadata.time_range_start,
            s.metadata.time_range_end,
            width = widest_loc,
        );
    }
    println!("---");
    let total: u64 = shards.iter().map(|s| s.metadata.num_docs).sum();
    let word = if shards.len() == 1 { "shard" } else { "shards" };
    println!("{} {word}, {total} total docs", shards.len());
    Ok(())
}

async fn log_shard_scan(store: Arc<dyn object_store::ObjectStore>) {
    let source: Arc<dyn ShardSource> = Arc::new(ObjectStoreShardSource::new(store));
    let mut manager = ShardManager::new(source);
    match manager.refresh().await {
        Ok(()) => log_shards(manager.all_shards()),
        Err(e) => warn!(error = ?e, "shard scan failed; daemon will continue"),
    }
}

fn log_shards(shards: &[ShardSummary]) {
    if shards.is_empty() {
        info!("shard scan complete: no shards discovered");
        return;
    }
    for s in shards {
        info!(
            location = %s.location.as_ref(),
            num_docs = s.metadata.num_docs,
            time_range_start = s.metadata.time_range_start,
            time_range_end = s.metadata.time_range_end,
            "shard"
        );
    }
    let total_docs: u64 = shards.iter().map(|s| s.metadata.num_docs).sum();
    info!(shards = shards.len(), total_docs, "shard scan complete");
}

async fn log_storage_scan(store: &dyn object_store::ObjectStore) {
    match storage::summarize(store).await {
        Ok(stats) => {
            if stats.is_empty() {
                info!("storage scan complete: empty backend");
                return;
            }
            for s in &stats {
                info!(prefix = %s.prefix, objects = s.objects, bytes = s.bytes, "dataset");
            }
            let total_objects: usize = stats.iter().map(|s| s.objects).sum();
            let total_bytes: usize = stats.iter().map(|s| s.bytes).sum();
            info!(
                prefixes = stats.len(),
                total_objects, total_bytes, "storage scan complete"
            );
        }
        Err(e) => {
            warn!(error = ?e, "storage scan failed; daemon will continue");
        }
    }
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
