//! Layered configuration (CLI > env > file > defaults).

use std::num::NonZeroUsize;
use std::path::PathBuf;

use clap::Args;
use directories::ProjectDirs;
use figment::Figment;
use figment::providers::{Env, Format, Serialized, Toml};
use serde::{Deserialize, Serialize};
use url::Url;

/// Single source of truth for every `TUT_*` environment variable name.
/// Referenced by clap's `env` attribute and by `figment`'s env scanner.
pub mod env_vars {
    pub const CONFIG: &str = "TUT_CONFIG";
    pub const OPS_ADDR: &str = "TUT_OPS_ADDR";
    pub const GRPC_ADDR: &str = "TUT_GRPC_ADDR";
    pub const CACHE_DIR: &str = "TUT_CACHE_DIR";
    pub const STORAGE_URL: &str = "TUT_STORAGE_URL";
    pub const TOKIO_WORKERS: &str = "TUT_TOKIO_WORKERS";
    pub const RAYON_WORKERS: &str = "TUT_RAYON_WORKERS";
    pub const SHUTDOWN_TIMEOUT_SECS: &str = "TUT_SHUTDOWN_TIMEOUT_SECS";
    pub const LOG_JSON: &str = "TUT_LOG_JSON";
    pub const MEMORY_LIMIT: &str = "TUT_MEMORY_LIMIT";
    pub const MAX_SESSION_MEMORY_PCT: &str = "TUT_MAX_SESSION_MEMORY_PCT";
    pub const BITMAP_CACHE_PCT: &str = "TUT_BITMAP_CACHE_PCT";
    pub const OTLP_ENDPOINT: &str = "TUT_OTLP_ENDPOINT";

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

    /// Object-storage backend URL (e.g. `s3://bucket/prefix`,
    /// `gs://bucket`, `file:///var/data`). Defaults to a local directory
    /// under the OS data dir.
    #[arg(long, env = env_vars::STORAGE_URL)]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub storage_url: Option<String>,

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

    /// Daemon-wide memory budget as an absolute size (`8GB`, `512MiB`).
    /// Percent-of-RAM is not supported — set it to your container/VM limit.
    #[arg(long, env = env_vars::MEMORY_LIMIT)]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub memory_limit: Option<String>,

    /// Per-session memory cap as a percent of the global budget (1..=100).
    #[arg(long, env = env_vars::MAX_SESSION_MEMORY_PCT)]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub max_session_memory_pct: Option<u8>,

    /// Doc-set bitmap cache size as a percent of the global memory budget
    /// (0..=100; 0 disables the cache).
    #[arg(long, env = env_vars::BITMAP_CACHE_PCT)]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub bitmap_cache_pct: Option<u8>,

    /// OTLP/HTTP endpoint for OpenTelemetry trace export (e.g.
    /// `http://localhost:4318`). Unset disables tracing export.
    #[arg(long, env = env_vars::OTLP_ENDPOINT)]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub otlp_endpoint: Option<String>,
}

/// Fully-resolved daemon configuration.
#[derive(Debug, Clone, Deserialize, Serialize)]
#[serde(default)]
pub struct Config {
    pub ops_addr: String,
    pub grpc_addr: String,
    pub cache_dir: PathBuf,
    pub storage_url: String,
    pub tokio_workers: usize,
    pub rayon_workers: usize,
    pub shutdown_timeout_secs: u64,
    /// Daemon-wide memory budget (absolute size string, resolved in `serve`).
    pub memory_limit: String,
    /// Per-session memory cap as a percent of the global budget.
    pub max_session_memory_pct: u8,
    /// Doc-set bitmap cache size as a percent of the global budget (0 disables).
    pub bitmap_cache_pct: u8,
    /// OTLP/HTTP trace-export endpoint; `None` disables tracing export.
    pub otlp_endpoint: Option<String>,
}

impl Default for Config {
    fn default() -> Self {
        let cores = std::thread::available_parallelism().map_or(4, NonZeroUsize::get);
        Self {
            ops_addr: "0.0.0.0:8080".into(),
            grpc_addr: "0.0.0.0:50051".into(),
            cache_dir: default_cache_dir(),
            storage_url: default_storage_url(),
            tokio_workers: cores.saturating_mul(2),
            rayon_workers: cores,
            shutdown_timeout_secs: 30,
            memory_limit: "4GB".into(),
            max_session_memory_pct: 20,
            bitmap_cache_pct: 25,
            otlp_endpoint: None,
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

fn project_dirs() -> Option<&'static ProjectDirs> {
    use std::sync::OnceLock;
    static DIRS: OnceLock<Option<ProjectDirs>> = OnceLock::new();
    DIRS.get_or_init(|| ProjectDirs::from("", "", "tutankhamun"))
        .as_ref()
}

#[must_use]
pub fn default_cache_dir() -> PathBuf {
    // Fallback only applies when the OS doesn't expose a home / data dir,
    // which is rare in practice but possible in minimal containers.
    project_dirs().map_or_else(
        || PathBuf::from("./tutankhamun-cache"),
        |d| d.cache_dir().to_path_buf(),
    )
}

fn default_storage_url() -> String {
    let path = project_dirs().map_or_else(
        || PathBuf::from("./tutankhamun-storage"),
        |d| d.data_local_dir().join("storage"),
    );
    path_to_file_url(&path)
}

/// Convert a local filesystem path to a `file://` URL. Handles percent-
/// encoding of spaces / non-ASCII characters and produces a well-formed
/// `file:///C:/...` shape on Windows. Falls back to a raw `file://<display>`
/// only when `path` is not absolute (which `Url::from_file_path` rejects);
/// callers should treat the relative-path case as already degraded.
fn path_to_file_url(path: &std::path::Path) -> String {
    Url::from_file_path(path)
        .map_or_else(|()| format!("file://{}", path.display()), |u| u.to_string())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[cfg(unix)]
    #[test]
    fn path_to_file_url_percent_encodes_spaces() {
        let url = path_to_file_url(std::path::Path::new("/tmp/a b/c"));
        let parsed = Url::parse(&url).expect("re-parseable");
        assert_eq!(parsed.scheme(), "file");
        assert_eq!(parsed.path(), "/tmp/a%20b/c");
        assert!(!url.contains(' '), "URL must not contain raw spaces: {url}");
    }

    #[test]
    fn path_to_file_url_falls_back_for_relative_paths() {
        // `Url::from_file_path` only accepts absolute paths; the fallback
        // produces a syntactically-weak URL but at least keeps the daemon
        // running in the no-home-dir degraded case.
        let url = path_to_file_url(std::path::Path::new("./relative"));
        assert!(url.starts_with("file://"), "got {url}");
    }
}
