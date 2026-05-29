//! Tutankhamun (`t9n`) daemon entry point.
//!
//! Single binary with subcommands. `t9n serve` runs the daemon; `t9n storage`
//! groups operator commands for the storage backend.

use std::io::{self, Write as _};
use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;

use clap::{Args, Parser, Subcommand, ValueEnum};
use tracing::{error, info, warn};

use tutankhamun_server::config::{Config, ServeArgs, env_vars};
use tutankhamun_server::ingest::{self, IngestOptions, ShardBy};
use tutankhamun_server::ops_http::{self, OpsState};
use tutankhamun_server::runtime;
use tutankhamun_server::shard;
use tutankhamun_server::shard::Aggregate;
use tutankhamun_server::shard_source::{
    self, ObjectStoreShardSource, ShardManager, ShardSource, ShardSummary,
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
        /// Optional `<field>=<term>` restriction (applied to every
        /// shard).
        #[arg(long)]
        filter: Option<String>,
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
}

/// CLI-facing shard-by aliases. Maps to the engine's duration-based
/// [`ShardBy`] in `run_ingest`; future aliases / explicit-duration
/// support are CLI-only changes.
#[derive(Debug, Clone, Copy, ValueEnum, Default)]
enum ShardByArg {
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
        Command::Query {
            source,
            metrics,
            aggregate,
            filter,
            from,
            to,
            cache_dir,
            cache_size,
        } => run_query(
            source,
            metrics,
            *aggregate,
            filter.as_deref(),
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
        } => run_ingest(
            input, output, time, metrics, strings, ints, *delimiter, *shard_by,
        ),
    }
}

// Args mirror the CLI flag count, which is the user-facing surface.
#[allow(clippy::too_many_arguments)]
fn run_ingest(
    input: &std::path::Path,
    output: &str,
    time: &str,
    metrics: &[String],
    strings: &[String],
    ints: &[String],
    delimiter: char,
    shard_by: ShardByArg,
) -> anyhow::Result<()> {
    use anyhow::Context as _;
    if !delimiter.is_ascii() {
        anyhow::bail!("delimiter must be a single ASCII byte (got {delimiter:?})");
    }
    let delimiter_byte = delimiter as u8;
    let opts = IngestOptions {
        time: time.to_string(),
        metrics: metrics.to_vec(),
        strings: strings.to_vec(),
        ints: ints.to_vec(),
        delimiter: delimiter_byte,
        shard_by: shard_by.into(),
    };
    match ingest::IngestDestination::parse(output)? {
        ingest::IngestDestination::Local(local) => {
            let n = ingest::ingest_csv(input, &local, &opts)?;
            println!("wrote {n} docs to {}", local.display());
        }
        ingest::IngestDestination::Remote(url) => {
            let staging = tempfile::tempdir().context("create ingest staging tempdir")?;
            let n = ingest::ingest_csv(input, staging.path(), &opts)?;
            let runtime = tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()?;
            runtime.block_on(ingest::upload_ingest_tree(staging.path(), &url))?;
            println!("wrote {n} docs to {url}");
        }
    }
    Ok(())
}

// Args mirror the CLI flag count, which is the user-facing surface.
#[allow(clippy::too_many_arguments)]
fn run_query(
    source: &str,
    metrics: &[String],
    aggregate: Aggregate,
    filter: Option<&str>,
    from: Option<&str>,
    to: Option<&str>,
    cache_dir: Option<&std::path::Path>,
    cache_size: &str,
) -> anyhow::Result<()> {
    use anyhow::Context as _;

    // clap's `required = true` on `metrics` guarantees non-empty.
    let metric_refs: Vec<&str> = metrics.iter().map(String::as_str).collect();
    let parsed = filter.map(parse_filter).transpose()?;
    let time_range = parse_time_range(from, to)?;
    let url = shard_source::resolve_source_url(source)?;
    let cache_dir = resolve_cache_dir(cache_dir);
    let size_cap = tutankhamun_server::cache::size::parse_cache_size(cache_size, &cache_dir)
        .context("parse --cache-size")?;
    let registry = StorageRegistry::from_url(&url)?;
    let cache =
        tutankhamun_server::cache::Cache::open(cache_dir, registry.store(), url.clone(), size_cap)?;
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()?;
    let output = runtime.block_on(shard_source::query_dataset(
        &url,
        &cache,
        parsed,
        &metric_refs,
        time_range,
    ))?;
    let stdout = io::stdout();
    let mut out = stdout.lock();
    shard_source::render_dataset_query_output(&mut out, &output, parsed, &metric_refs, aggregate)?;
    Ok(())
}

/// Turn a user-supplied `source` (bare path or `object_store` URL)
/// into an `object_store` URL. Bare paths are canonicalised against
/// the current working directory and converted to `file://`.
fn resolve_cache_dir(overridden: Option<&std::path::Path>) -> PathBuf {
    overridden.map_or_else(
        tutankhamun_server::config::default_cache_dir,
        std::path::Path::to_path_buf,
    )
}

/// Resolve `--from` / `--to` into the closed interval expected by
/// `query_dataset`. Returns `None` when both are absent (preserves
/// today's "scan every shard" behaviour); otherwise the missing
/// half is filled in with `i64::MIN` / `i64::MAX`.
fn parse_time_range(from: Option<&str>, to: Option<&str>) -> anyhow::Result<Option<(i64, i64)>> {
    use anyhow::Context as _;
    if from.is_none() && to.is_none() {
        return Ok(None);
    }
    let from = from
        .map(ingest::parse_time_str)
        .transpose()
        .context("parse --from")?
        .unwrap_or(i64::MIN);
    let to = to
        .map(parse_to_inclusive)
        .transpose()
        .context("parse --to")?
        .unwrap_or(i64::MAX);
    Ok(Some((from, to)))
}

/// `--to` accepts the same formats as `--from`, but a bare date is
/// bumped to end-of-day (`23:59:59Z`) so `--to 2023-01-14` covers
/// the whole of Jan 14 rather than stopping at midnight. Explicit
/// datetimes pass through unchanged.
fn parse_to_inclusive(s: &str) -> anyhow::Result<i64> {
    if let Some(start_of_day) = ingest::parse_date_only(s) {
        return Ok(start_of_day + 86_399);
    }
    ingest::parse_time_str(s)
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
            metrics,
            aggregate,
            filter,
        } => {
            // clap's `required = true` on `metrics` guarantees non-empty.
            let metric_refs: Vec<&str> = metrics.iter().map(String::as_str).collect();
            let parsed = filter.as_deref().map(parse_filter).transpose()?;
            let result = shard::query_shard(path, parsed, &metric_refs)?;
            let stdout = io::stdout();
            let mut out = stdout.lock();
            writeln!(out, "shard:    {}", path.display())?;
            shard::write_query_summary(&mut out, parsed, &metric_refs, *aggregate, &result)?;
            Ok(())
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
