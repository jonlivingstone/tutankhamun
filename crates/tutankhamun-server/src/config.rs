//! Layered configuration (CLI > env > file > defaults).

use std::num::NonZeroUsize;
use std::path::PathBuf;

use clap::Args;
use directories::ProjectDirs;
use figment::Figment;
use figment::providers::{Env, Format, Serialized, Toml};
use serde::{Deserialize, Serialize};

/// Single source of truth for every `TUT_*` environment variable name.
/// Referenced by clap's `env` attribute and by `figment`'s env scanner.
pub mod env_vars {
    pub const CONFIG: &str = "TUT_CONFIG";
    pub const OPS_ADDR: &str = "TUT_OPS_ADDR";
    pub const GRPC_ADDR: &str = "TUT_GRPC_ADDR";
    pub const CACHE_DIR: &str = "TUT_CACHE_DIR";
    pub const TOKIO_WORKERS: &str = "TUT_TOKIO_WORKERS";
    pub const RAYON_WORKERS: &str = "TUT_RAYON_WORKERS";
    pub const SHUTDOWN_TIMEOUT_SECS: &str = "TUT_SHUTDOWN_TIMEOUT_SECS";
    pub const LOG_JSON: &str = "TUT_LOG_JSON";

    /// Prefix figment uses to scan for env-driven overrides.
    pub const PREFIX: &str = "TUT_";
}

/// Flags for the `serve` subcommand (run the daemon). Also reads
/// `TUT_*` environment variables via `clap`'s `env` feature.
///
/// `None` fields are skipped when serialized, so they don't clobber lower-
/// precedence layers (defaults / file / env) in [`Config::resolve`].
#[derive(Args, Debug, Clone, Serialize)]
pub struct ServeArgs {
    #[arg(long, env = env_vars::CONFIG)]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub config: Option<PathBuf>,

    /// Bind address for the operations HTTP port (health, readiness, metrics, status).
    #[arg(long, env = env_vars::OPS_ADDR)]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub ops_addr: Option<String>,

    /// Bind address for the gRPC data-plane (sessions, Flight, `FlightSQL`).
    #[arg(
        long,
        env = env_vars::GRPC_ADDR,
        help = "Bind address for the gRPC data-plane (sessions, Flight, FlightSQL)"
    )]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub grpc_addr: Option<String>,

    /// Local cache directory (defaults to XDG cache path).
    #[arg(long, env = env_vars::CACHE_DIR)]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub cache_dir: Option<PathBuf>,

    /// Tokio worker thread count (defaults to ~2× physical cores).
    #[arg(long, env = env_vars::TOKIO_WORKERS)]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub tokio_workers: Option<NonZeroUsize>,

    /// Rayon compute pool size (defaults to physical core count).
    #[arg(long, env = env_vars::RAYON_WORKERS)]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub rayon_workers: Option<NonZeroUsize>,

    /// Graceful shutdown timeout in seconds.
    #[arg(long, env = env_vars::SHUTDOWN_TIMEOUT_SECS)]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub shutdown_timeout_secs: Option<u64>,
}

/// Fully-resolved daemon configuration.
#[derive(Debug, Clone, Deserialize, Serialize)]
#[serde(default)]
pub struct Config {
    pub ops_addr: String,
    pub grpc_addr: String,
    pub cache_dir: PathBuf,
    pub tokio_workers: usize,
    pub rayon_workers: usize,
    pub shutdown_timeout_secs: u64,
}

impl Default for Config {
    fn default() -> Self {
        let cores = std::thread::available_parallelism().map_or(4, NonZeroUsize::get);
        Self {
            ops_addr: "0.0.0.0:8080".into(),
            grpc_addr: "0.0.0.0:50051".into(),
            cache_dir: default_cache_dir(),
            tokio_workers: cores.saturating_mul(2),
            rayon_workers: cores,
            shutdown_timeout_secs: 30,
        }
    }
}

impl Config {
    /// Resolve config from defaults < file < env < CLI.
    pub fn resolve(args: &ServeArgs) -> anyhow::Result<Self> {
        let mut fig = Figment::from(Serialized::defaults(Config::default()));

        if let Some(path) = args.config.as_ref() {
            fig = fig.merge(Toml::file(path));
        }

        fig = fig.merge(Env::prefixed(env_vars::PREFIX).split("__"));
        fig = fig.merge(Serialized::defaults(args));

        let cfg: Config = fig.extract()?;
        Ok(cfg)
    }
}

fn default_cache_dir() -> PathBuf {
    // Fallback only applies when the OS doesn't expose a home / data dir,
    // which is rare in practice but possible in minimal containers.
    ProjectDirs::from("", "", "tutankhamun").map_or_else(
        || PathBuf::from("./tutankhamun-cache"),
        |d| d.cache_dir().to_path_buf(),
    )
}
