//! Subcommand handlers for the `t9n` binary. Each verb's body lives in its
//! own module; `main` parses the CLI and dispatches here. Helpers shared by
//! more than one verb live here at the module root.

pub mod filter;
pub mod ingest;
pub mod query;
pub mod shard;
pub mod sql;
pub mod storage;

use std::path::{Path, PathBuf};

/// Resolve the cache directory: the explicit `--cache-dir` if given,
/// otherwise the platform default. Shared by `query` and `sql`.
pub(crate) fn resolve_cache_dir(overridden: Option<&Path>) -> PathBuf {
    overridden.map_or_else(
        tutankhamun_server::config::default_cache_dir,
        Path::to_path_buf,
    )
}

/// A current-thread tokio runtime for the one-shot CLI verbs — each does a
/// bounded amount of async I/O against object storage, then exits. (`sql`
/// uses a multi-thread runtime for `DataFusion`; `serve` builds its own.)
pub(crate) fn current_thread_runtime() -> anyhow::Result<tokio::runtime::Runtime> {
    Ok(tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()?)
}
