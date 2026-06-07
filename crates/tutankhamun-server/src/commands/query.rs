//! `t9n query` — aggregate metric columns across every shard under a dataset
//! source, optionally restricted by `--filter` terms and a `--from`/`--to`
//! time window.

use std::io;
use std::path::Path;

use anyhow::Context as _;

use tutankhamun_server::cache::{self, Cache};
use tutankhamun_server::ingest;
use tutankhamun_server::shard::Aggregate;
use tutankhamun_server::shard_source;
use tutankhamun_server::storage::StorageRegistry;

use super::filter::parse_filters;
use super::resolve_cache_dir;

// Args mirror the CLI flag count, which is the user-facing surface.
#[allow(clippy::too_many_arguments)]
pub(crate) fn run(
    source: &str,
    metrics: &[String],
    aggregate: Aggregate,
    filters: &[String],
    from: Option<&str>,
    to: Option<&str>,
    cache_dir: Option<&Path>,
    cache_size: &str,
) -> anyhow::Result<()> {
    // clap's `required = true` on `metrics` guarantees non-empty.
    let metric_refs: Vec<&str> = metrics.iter().map(String::as_str).collect();
    let parsed_filters = parse_filters(filters)?;
    let time_range = parse_time_range(from, to)?;
    let url = shard_source::resolve_source_url(source)?;
    let cache_dir = resolve_cache_dir(cache_dir);
    let size_cap =
        cache::size::parse_cache_size(cache_size, &cache_dir).context("parse --cache-size")?;
    let registry = StorageRegistry::from_url(&url)?;
    let cache = Cache::open(cache_dir, registry.store(), url.clone(), size_cap)?;
    let runtime = super::current_thread_runtime()?;
    let output = runtime.block_on(shard_source::query_dataset(
        &url,
        &cache,
        &parsed_filters,
        &metric_refs,
        time_range,
    ))?;
    let stdout = io::stdout();
    let mut out = stdout.lock();
    shard_source::render_dataset_query_output(
        &mut out,
        &output,
        &parsed_filters,
        &metric_refs,
        aggregate,
    )?;
    Ok(())
}

/// Resolve `--from` / `--to` into the closed interval expected by
/// `query_dataset`. Returns `None` when both are absent (preserves
/// today's "scan every shard" behaviour); otherwise the missing
/// half is filled in with `i64::MIN` / `i64::MAX`.
fn parse_time_range(from: Option<&str>, to: Option<&str>) -> anyhow::Result<Option<(i64, i64)>> {
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
