//! Tutankhamun (`t9n`) daemon entry point.
//!
//! Single binary with subcommands. `t9n serve` runs the daemon; `t9n storage`
//! groups operator commands for the storage backend.

use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;

use clap::{Args, Parser, Subcommand, ValueEnum};
use tracing::{error, info, warn};

use tutankhamun_server::cache;
use tutankhamun_server::config::{Config, ServeArgs, env_vars};
use tutankhamun_server::flight_sql::{self, TutankhamunFlightSqlService};
use tutankhamun_server::ingest::ShardBy;
use tutankhamun_server::memory;
use tutankhamun_server::ops_http::{self, OpsState};
use tutankhamun_server::runtime;
use tutankhamun_server::shard::Aggregate;
use tutankhamun_server::shard_source::{
    ObjectStoreShardSource, ShardManager, ShardSource, ShardSummary,
};
use tutankhamun_server::shutdown::{self, ShutdownHandle};
use tutankhamun_server::storage::{self, StorageRegistry};

mod commands;

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
    /// Aggregate one or more metric columns across every shard
    /// discovered under a dataset source, optionally restricted by a
    /// string-field term and/or a time-range window.
    Query {
        /// Where the shards live. Accepts a local directory path or
        /// any `object_store` URL (`s3://`, `gs://`, `az://`,
        /// `file://`, `memory://`). Bare paths are treated as
        /// `file://`.
        source: String,
        /// Metric column to aggregate. Repeatable — one line per
        /// metric is returned in declaration order. At least one
        /// required.
        #[arg(long = "metric", required = true)]
        metrics: Vec<String>,
        /// Aggregate to compute per metric. Default `sum`. Accepts
        /// `sum`, `min`, `max`, `avg`. `min`/`max`/`avg` display
        /// `n/a` when no docs matched.
        #[arg(long, value_enum, default_value_t = Aggregate::Sum)]
        aggregate: Aggregate,
        /// `<field>=<value>` restriction. Repeatable — multiple
        /// `--filter`s AND together. `<value>` is either an exact
        /// term (`country=us`) or an inclusive range
        /// (`vendor_id=100..200`, with either end optionally open:
        /// `vendor_id=100..`, `vendor_id=..200`).
        #[arg(long = "filter")]
        filters: Vec<String>,
        /// Inclusive lower bound of the time window — only shards
        /// whose time range intersects `[from, to]` are scanned.
        /// Accepts `YYYY-MM-DD` (start-of-day UTC), unix epoch
        /// seconds, RFC 3339, or `YYYY-MM-DD HH:MM:SS` (UTC).
        #[arg(long)]
        from: Option<String>,
        /// Inclusive upper bound of the time window. Same formats as
        /// `--from`; a bare `YYYY-MM-DD` value is bumped to end-of-day
        /// (`23:59:59Z`) so the named date is fully included. May be
        /// omitted independently of `--from`.
        #[arg(long)]
        to: Option<String>,
        /// Directory for the local shard cache. Defaults to the
        /// platform cache dir (`~/.cache/t9n` on Linux,
        /// `~/Library/Caches/tutankhamun` on macOS) — shared with the
        /// daemon's storage cache.
        #[arg(long, env = "T9N_CACHE_DIR")]
        cache_dir: Option<PathBuf>,
        /// Hard cap on resident cache bytes. Accepts plain units
        /// (`10GB`, `1.5TB`, `512MiB`) or a percent of total disk
        /// (`50%`). Whole shards are evicted oldest-mtime-first when
        /// an insert would exceed this.
        #[arg(long, env = "T9N_CACHE_SIZE", default_value = "10GB")]
        cache_size: String,
    },
    /// Build one or more shards from a CSV/TSV input file.
    Ingest {
        /// Path to the CSV/TSV input file (must have a header row).
        input: PathBuf,
        /// Where to write the finalised shard(s). Accepts a local
        /// directory path or any `object_store` URL (`s3://`,
        /// `gs://`, `az://`, `memory://`). With `--shard-by daily`
        /// or `hourly`, one shard per time bucket is written under
        /// this root (e.g. `<root>/YYYY-MM-DD/`). For remote URLs
        /// each shard is staged in a tempdir and uploaded with
        /// `metadata.json` last, so partial uploads stay invisible
        /// to discovery.
        #[arg(long)]
        output: String,
        /// Header name of the time column. Values may be unix epoch
        /// seconds, RFC 3339, or `YYYY-MM-DD HH:MM:SS` (treated as
        /// UTC). Bare integers always parse as epoch — preprocess
        /// `YYYYMMDD` columns to one of the above formats.
        #[arg(long)]
        time: String,
        /// Header name of an int64 metric column (aggregatable
        /// only). Repeatable.
        #[arg(long = "metric")]
        metrics: Vec<String>,
        /// Header name of a string-field column (filterable only).
        /// Repeatable.
        #[arg(long = "string")]
        strings: Vec<String>,
        /// Header name of an int64 column to ingest as an `Int`
        /// field (both aggregatable and filterable — useful for
        /// numeric IDs like `vendor_id`). Repeatable.
        #[arg(long = "int")]
        ints: Vec<String>,
        /// Field delimiter (default ','). Use `$'\t'` for TSV.
        #[arg(long, default_value = ",")]
        delimiter: char,
        /// Partition rows into shards by UTC time bucket. `none`
        /// produces one shard at `--output`; `daily` / `hourly`
        /// produce one shard per day / hour under `--output`.
        #[arg(long, value_enum, default_value_t = ShardByArg::None)]
        shard_by: ShardByArg,
    },
    /// Run a SQL query over a dataset via `DataFusion` and print the
    /// result table. The dataset is registered as a table named `t`.
    Sql {
        /// Where the shards live. Accepts a local directory path or
        /// any `object_store` URL (`s3://`, `gs://`, `az://`,
        /// `file://`, `memory://`). Bare paths are treated as
        /// `file://`.
        source: String,
        /// The SQL query. Reference the dataset as table `t`, e.g.
        /// `SELECT country, sum(fare) FROM t WHERE vendor_id > 2
        /// GROUP BY country`.
        query: String,
        /// Directory for the local shard cache. Same default and
        /// semantics as `t9n query --cache-dir`.
        #[arg(long, env = "T9N_CACHE_DIR")]
        cache_dir: Option<PathBuf>,
        /// Hard cap on resident cache bytes. Same forms as
        /// `t9n query --cache-size` (`10GB`, `1.5TB`, `50%`).
        #[arg(long, env = "T9N_CACHE_SIZE", default_value = "10GB")]
        cache_size: String,
    },
}

/// CLI-facing shard-by aliases, mapped to the engine's duration-based
/// [`ShardBy`] in `commands::ingest`.
#[derive(Debug, Clone, Copy, ValueEnum, Default)]
pub(crate) enum ShardByArg {
    #[default]
    None,
    Daily,
    Hourly,
}

impl From<ShardByArg> for ShardBy {
    fn from(a: ShardByArg) -> Self {
        match a {
            ShardByArg::None => ShardBy::None,
            ShardByArg::Daily => ShardBy::Bucket { seconds: 86_400 },
            ShardByArg::Hourly => ShardBy::Bucket { seconds: 3600 },
        }
    }
}

#[derive(Args, Debug)]
pub(crate) struct StorageArgs {
    #[command(subcommand)]
    command: StorageCommand,
}

#[derive(Args, Debug)]
pub(crate) struct ShardArgs {
    #[command(subcommand)]
    command: ShardCommand,
}

#[derive(Subcommand, Debug)]
pub(crate) enum ShardCommand {
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
    /// Aggregate one or more metric columns over the docs in a shard,
    /// optionally restricted to those matching a single string-field
    /// term.
    Query {
        /// Path to the shard directory.
        path: PathBuf,
        /// Metric column to aggregate. Repeatable — one line per
        /// metric is returned in declaration order. At least one
        /// required.
        #[arg(long = "metric", required = true)]
        metrics: Vec<String>,
        /// Aggregate to compute per metric. Default `sum`. Accepts
        /// `sum`, `min`, `max`, `avg`.
        #[arg(long, value_enum, default_value_t = Aggregate::Sum)]
        aggregate: Aggregate,
        /// `<field>=<value>` restriction. Repeatable — multiple
        /// `--filter`s AND together. `<value>` is either an exact
        /// term (`country=us`) or an inclusive range
        /// (`vendor_id=100..200`, with either end optionally open).
        #[arg(long = "filter")]
        filters: Vec<String>,
    },
}

#[derive(Subcommand, Debug)]
pub(crate) enum StorageCommand {
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
        Command::Storage(args) => commands::storage::run(args),
        Command::Shard(args) => commands::shard::run(args),
        Command::Query {
            source,
            metrics,
            aggregate,
            filters,
            from,
            to,
            cache_dir,
            cache_size,
        } => commands::query::run(
            source,
            metrics,
            *aggregate,
            filters,
            from.as_deref(),
            to.as_deref(),
            cache_dir.as_deref(),
            cache_size,
        ),
        Command::Ingest {
            input,
            output,
            time,
            metrics,
            strings,
            ints,
            delimiter,
            shard_by,
        } => commands::ingest::run(
            input, output, time, metrics, strings, ints, *delimiter, *shard_by,
        ),
        Command::Sql {
            source,
            query,
            cache_dir,
            cache_size,
        } => commands::sql::run(source, query, cache_dir.as_deref(), cache_size),
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

    // FlightSQL data-plane. Per-dataset shard caches are created lazily and
    // reused across queries (see `flight_sql`); the gRPC listener is bound
    // before marking ready so /readyz only flips once both ops and gRPC are live.
    let grpc_addr = config.grpc_addr.parse()?;
    let grpc_listener = flight_sql::bind(grpc_addr).await?;
    let cache_cap = cache::size::parse_cache_size("10GB", &config.cache_dir)?;
    let mem_limit = cache::size::parse_byte_size(&config.memory_limit)?;
    // A 0% cap would make every session's baseline reservation fail, rejecting
    // all handshakes; >100% would let a session exceed the global budget.
    let pct = config.max_session_memory_pct;
    if pct == 0 || pct > 100 {
        anyhow::bail!("--max-session-memory-pct must be between 1 and 100 (got {pct})");
    }
    let budget = Arc::new(memory::MemoryBudget::new(mem_limit));
    info!(
        bytes = mem_limit,
        per_session_pct = config.max_session_memory_pct,
        "memory budget initialised"
    );
    let grpc_task = tokio::spawn({
        let shutdown = shutdown.clone();
        let svc = TutankhamunFlightSqlService::new(
            config.storage_url.clone(),
            config.cache_dir.clone(),
            cache_cap,
            budget,
            config.max_session_memory_pct,
        );
        async move {
            if let Err(e) = flight_sql::serve(grpc_listener, svc, shutdown).await {
                error!(error = ?e, "gRPC FlightSQL server failed");
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
        let _ = tokio::join!(ops_task, grpc_task);
    })
    .await;

    info!("shutdown complete");
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
            let total_bytes: u64 = stats.iter().map(|s| s.bytes).sum();
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
