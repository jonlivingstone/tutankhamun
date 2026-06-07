//! `t9n sql` — run a SQL query over a dataset via `DataFusion`, registering it
//! as table `t`, and print the result as a table.

use std::path::Path;
use std::sync::Arc;

use anyhow::Context as _;

use tutankhamun_server::cache::{self, Cache};
use tutankhamun_server::shard_source;
use tutankhamun_server::sql::{self, TutankhamunTableProvider};
use tutankhamun_server::storage::StorageRegistry;

use super::resolve_cache_dir;

/// Run a SQL `query` over the dataset at `source` via `DataFusion`,
/// registering it as table `t`, and print the result as a table.
pub(crate) fn run(
    source: &str,
    query: &str,
    cache_dir: Option<&Path>,
    cache_size: &str,
) -> anyhow::Result<()> {
    let url = shard_source::resolve_source_url(source)?;
    let cache_dir = resolve_cache_dir(cache_dir);
    let size_cap =
        cache::size::parse_cache_size(cache_size, &cache_dir).context("parse --cache-size")?;
    let registry = StorageRegistry::from_url(&url)?;

    let runtime = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()?;
    runtime.block_on(async {
        let cache = Arc::new(Cache::open(
            cache_dir,
            registry.store(),
            url.clone(),
            size_cap,
        )?);
        let provider = TutankhamunTableProvider::try_new(url, cache).await?;
        // GROUP BY pushdown rule + planner wired in; falls back to
        // DataFusion's own aggregation for unsupported queries.
        let ctx = sql::session_context();
        ctx.register_table("t", Arc::new(provider))
            .context("register table t")?;
        let batches = ctx.sql(query).await?.collect().await?;
        let rendered = arrow::util::pretty::pretty_format_batches(&batches)?;
        println!("{rendered}");
        anyhow::Ok(())
    })
}
