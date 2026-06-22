//! Tutankhamun (`t9n`) daemon entry point.
//!
//! Single binary with subcommands. `t9n serve` runs the daemon; `t9n storage`
//! groups operator commands for the storage backend.

use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;

use clap::{Args, Parser, Subcommand, ValueEnum};
use tracing::{error, info, warn};

use tutankhamun_server::aggregate_cache;
use tutankhamun_server::bitmap_cache;
use tutankhamun_server::cache;
use tutankhamun_server::config::{Config, ServeArgs, env_vars};
use tutankhamun_server::flight_sql::{self, TutankhamunFlightSqlService};
use tutankhamun_server::ingest::ShardBy;
use tutankhamun_server::memory;
use tutankhamun_server::metrics;
use tutankhamun_server::ops_http::{self, OpsState};
use tutankhamun_server::runtime;
use tutankhamun_server::shard::Aggregate;
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
    /// Build one or more shards from a CSV/TSV or Parquet input file.
    Ingest {
        /// Path to the input file: CSV/TSV (with a header row) or Parquet.
        /// Format is inferred from the extension (`.parquet`/`.pq` →
        /// Parquet); override with `--format`.
        input: PathBuf,
        /// Where to write the finalised shard(s). Accepts a local
        /// directory path or any `object_store` URL (`s3://`,
        /// `gs://`, `az://`, `memory://`). With a `--shard-by`
        /// granularity, one shard per time bucket is written under
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
        /// Header name of a numeric metric column (aggregatable only).
        /// For CSV the value must be int64; for Parquet, integer /
        /// decimal / float columns are accepted (floats are scaled — see
        /// `--scale`). Repeatable.
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
        /// produces one shard at `--output`; `hourly` / `daily` /
        /// `weekly` produce one shard per fixed-width bucket and
        /// `monthly` one per calendar month, under `--output`.
        /// (`weekly` windows are 7-day, epoch-aligned.)
        #[arg(long, value_enum, default_value_t = ShardByArg::None)]
        shard_by: ShardByArg,
        /// Per-column scale for Parquet float columns: `--scale
        /// fare_amount=2` stores `round(value × 10^2)` (cents). Floats
        /// default to scale 3. Repeatable. Decimal columns use their own
        /// schema scale; not valid for CSV input.
        #[arg(long = "scale", value_parser = parse_scale)]
        scale: Vec<(String, i8)>,
        /// Input format. Inferred from the file extension when omitted.
        #[arg(long, value_enum)]
        format: Option<FormatArg>,
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
    Hourly,
    Daily,
    Weekly,
    Monthly,
}

impl From<ShardByArg> for ShardBy {
    fn from(a: ShardByArg) -> Self {
        match a {
            ShardByArg::None => ShardBy::None,
            ShardByArg::Hourly => ShardBy::Bucket { seconds: 3600 },
            ShardByArg::Daily => ShardBy::Bucket { seconds: 86_400 },
            ShardByArg::Weekly => ShardBy::Bucket { seconds: 604_800 },
            ShardByArg::Monthly => ShardBy::Month,
        }
    }
}

/// Input format for `t9n ingest`; inferred from the file extension when omitted.
#[derive(Debug, Clone, Copy, ValueEnum)]
pub(crate) enum FormatArg {
    Csv,
    Parquet,
}

/// Parse a `--scale COL=N` argument into a `(column, scale)` pair.
fn parse_scale(s: &str) -> Result<(String, i8), String> {
    let (name, n) = s
        .split_once('=')
        .ok_or_else(|| format!("expected COL=SCALE, got {s:?}"))?;
    if name.is_empty() {
        return Err(format!("empty column name in {s:?}"));
    }
    let scale: i8 = n
        .parse()
        .map_err(|_| format!("invalid scale {n:?} in {s:?}"))?;
    Ok((name.to_string(), scale))
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
    let cli = Cli::parse();

    // `serve` builds its own layered subscriber (fmt + optional OTLP) inside the
    // tokio runtime — the OTLP batch exporter needs a runtime. Every other
    // subcommand is one-shot and uses the simple fmt/json subscriber.
    if !matches!(cli.command, Command::Serve(_)) {
        init_tracing();
    }

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
            scale,
            format,
        } => commands::ingest::run(
            input, output, time, metrics, strings, ints, *delimiter, *shard_by, scale, *format,
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

    let runtime = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(config.tokio_workers)
        .thread_name("tk-tokio")
        .enable_all()
        .build()?;

    // Init the layered subscriber inside the runtime (the OTLP batch exporter
    // spawns a background task that needs it). Held across `serve` so the tracer
    // flushes on shutdown. Logs below this line are captured by it.
    let _telemetry = {
        let _enter = runtime.enter();
        init_serve_tracing(&config)?
    };
    info!(?config, "loaded configuration");

    runtime::init_rayon(config.rayon_workers)?;
    info!(workers = config.rayon_workers, "rayon pool initialised");
    info!(workers = config.tokio_workers, "tokio runtime initialised");

    runtime.block_on(serve(config))
}

/// A capped sub-budget handle off the global budget, for a daemon-shared cache.
fn sub_budget(budget: &Arc<memory::MemoryBudget>, cap: u64) -> Arc<memory::SessionMemoryHandle> {
    Arc::new(memory::SessionMemoryHandle::new(Arc::clone(budget), cap))
}

/// The global memory budget plus the daemon-shared caches and metrics that hang
/// off it. The §2.8 doc-set bitmap cache and the per-shard aggregate cache EACH
/// get their own `cache_pct`%-of-`mem_limit` sub-budget (so caching can total up
/// to `2 × cache_pct`%, still hard-capped by the global budget they both draw
/// from). Separate slices, not a shared pool: each cache only knows its own
/// entries, so it can only LRU-evict its own — a shared cap would let one cache
/// fill the pool and the other be unable to free it. See Tech debt in the roadmap.
fn build_budget_and_caches(
    mem_limit: u64,
    cache_pct: u8,
) -> (
    Arc<memory::MemoryBudget>,
    Arc<bitmap_cache::BitmapCache>,
    Arc<aggregate_cache::AggregateCache>,
    Arc<metrics::Metrics>,
) {
    let budget = Arc::new(memory::MemoryBudget::new(mem_limit));
    let cap = mem_limit.saturating_mul(u64::from(cache_pct)) / 100;
    let bitmap_cache = Arc::new(bitmap_cache::BitmapCache::new(sub_budget(&budget, cap)));
    let aggregate_cache = Arc::new(aggregate_cache::AggregateCache::new(sub_budget(
        &budget, cap,
    )));
    let metrics = metrics::Metrics::new(
        Arc::clone(&budget),
        Arc::clone(&bitmap_cache),
        Arc::clone(&aggregate_cache),
    );
    (budget, bitmap_cache, aggregate_cache, metrics)
}

async fn serve(config: Config) -> anyhow::Result<()> {
    let storage = Arc::new(StorageRegistry::from_url(&config.storage_url)?);
    info!(storage_url = %config.storage_url, "storage backend ready");

    let shutdown = ShutdownHandle::new();

    // Resolve and validate the memory/cache config up front, then build the
    // budget, the §2.8 bitmap cache, and the shared metrics surface — all needed
    // before the ops server (which renders /metrics) and the flight service.
    let cache_cap = cache::size::parse_cache_size("10GB", &config.cache_dir)?;
    let mem_limit = cache::size::parse_byte_size(&config.memory_limit)?;
    // A 0% cap would make every session's baseline reservation fail, rejecting
    // all handshakes; >100% would let a session exceed the global budget.
    let pct = config.max_session_memory_pct;
    if pct == 0 || pct > 100 {
        anyhow::bail!("--max-session-memory-pct must be between 1 and 100 (got {pct})");
    }
    // The bitmap cache may use 0% (disabled) up to 100% of the budget.
    let cache_pct = config.bitmap_cache_pct;
    if cache_pct > 100 {
        anyhow::bail!("--bitmap-cache-pct must be between 0 and 100 (got {cache_pct})");
    }
    let (budget, bitmap_cache, aggregate_cache, metrics) =
        build_budget_and_caches(mem_limit, cache_pct);
    info!(
        bytes = mem_limit,
        per_session_pct = pct,
        "memory budget initialised"
    );

    // FlightSQL data-plane service. Built before the ops server so it can be
    // handed to `OpsState` as the `/status` structural-state source (sessions,
    // datasets); it's Clone-cheap (an `Arc` handle), so the gRPC task takes its
    // own clone. Per-dataset shard caches are created lazily and reused across
    // queries (see `flight_sql`).
    let validation = if config.verify_shards {
        cache::Validation::Verify
    } else {
        cache::Validation::Trust
    };
    let svc = TutankhamunFlightSqlService::new(
        config.storage_url.clone(),
        config.cache_dir.clone(),
        cache_cap,
        validation,
        budget,
        pct,
        bitmap_cache,
        aggregate_cache,
        Arc::clone(&metrics),
    );

    let ops_state = OpsState::new(
        Arc::clone(&metrics),
        Arc::new(svc.clone()),
        std::time::Instant::now(),
    );
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

    // The gRPC listener is bound before marking ready so /readyz only flips once
    // both ops and gRPC are live.
    let grpc_addr = config.grpc_addr.parse()?;
    let grpc_listener = flight_sql::bind(grpc_addr).await?;
    // A handle for the startup warm-up below, before `svc` moves into the server.
    let scan_svc = svc.clone();
    let grpc_task = tokio::spawn({
        let shutdown = shutdown.clone();
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
    // otherwise widen the window in which SIGTERM bypasses the graceful drain.
    // `warm_cache` discovers each dataset's shard set into the query cache (and
    // logs it); aborting mid-flight on shutdown is safe (lazy resolution covers
    // anything not yet warmed). `log_storage_scan` is the per-prefix byte/object
    // summary.
    let scan_task = tokio::spawn({
        let storage = Arc::clone(&storage);
        async move {
            // Both scans hit the same backend independently; running them
            // concurrently bounds startup-scan wall-clock by the slower one
            // rather than their sum.
            let store = storage.store();
            tokio::join!(log_storage_scan(&*store), scan_svc.warm_cache());
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

/// Simple fmt/json subscriber for one-shot subcommands (no OTLP). `TUT_LOG_JSON=1`
/// selects JSON; the filter defaults to `info`.
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

/// Holds the OTLP tracer provider so spans flush on a graceful shutdown; dropping
/// it (when `run_serve` returns) shuts the batch exporter down. `None` when no
/// `--otlp-endpoint` is configured.
struct TelemetryGuard {
    provider: Option<opentelemetry_sdk::trace::SdkTracerProvider>,
}

impl Drop for TelemetryGuard {
    fn drop(&mut self) {
        if let Some(provider) = self.provider.take()
            && let Err(e) = provider.shutdown()
        {
            eprintln!("otel tracer shutdown failed: {e}");
        }
    }
}

/// Build the daemon's layered subscriber: `EnvFilter` (default `info`) + an
/// fmt/json layer (honoring `TUT_LOG_JSON`) + an optional OpenTelemetry layer that
/// exports spans to `--otlp-endpoint` over OTLP/HTTP. Must run inside the tokio
/// runtime (the batch exporter spawns a background task). One-shot: calls `.init()`.
fn init_serve_tracing(config: &Config) -> anyhow::Result<TelemetryGuard> {
    use opentelemetry::trace::TracerProvider as _;
    use tracing_subscriber::layer::SubscriberExt as _;
    use tracing_subscriber::util::SubscriberInitExt as _;
    use tracing_subscriber::{EnvFilter, Layer as _, fmt};

    let filter = EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("info"));
    let json = std::env::var(env_vars::LOG_JSON).as_deref() == Ok("1");
    let fmt_layer = if json {
        fmt::layer().json().with_current_span(true).boxed()
    } else {
        fmt::layer().boxed()
    };
    let registry = tracing_subscriber::registry().with(filter).with(fmt_layer);

    let Some(endpoint) = config.otlp_endpoint.as_deref() else {
        registry.init();
        return Ok(TelemetryGuard { provider: None });
    };

    let provider = build_otlp_provider(endpoint)?;
    let otel_layer =
        tracing_opentelemetry::layer().with_tracer(provider.tracer(env!("CARGO_PKG_NAME")));
    registry.with(otel_layer).init();
    info!(otlp_endpoint = %endpoint, "OTLP trace export enabled");
    Ok(TelemetryGuard {
        provider: Some(provider),
    })
}

/// Build an OTLP/HTTP span exporter + batch tracer provider for `endpoint`. Build
/// is offline (no connection until spans export), so a dead collector is fine —
/// exports just fail in the background. Must run inside a tokio runtime.
fn build_otlp_provider(
    endpoint: &str,
) -> anyhow::Result<opentelemetry_sdk::trace::SdkTracerProvider> {
    use opentelemetry_otlp::WithExportConfig as _;

    let exporter = opentelemetry_otlp::SpanExporter::builder()
        .with_http()
        .with_endpoint(endpoint)
        .build()?;
    let resource = opentelemetry_sdk::Resource::builder()
        .with_service_name(env!("CARGO_PKG_NAME"))
        .build();
    Ok(opentelemetry_sdk::trace::SdkTracerProvider::builder()
        .with_batch_exporter(exporter)
        .with_resource(resource)
        .build())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn otlp_provider_builds_offline() {
        // A dead/bogus endpoint must still build (export is async + best-effort);
        // this exercises the otel API wiring without a live collector.
        let provider = build_otlp_provider("http://127.0.0.1:4318").expect("build provider");
        let _ = provider.shutdown();
    }
}
