//! `t9n shard` — inspect a shard's metadata, list shards at a storage URL, or
//! aggregate metric columns over a single shard.

use std::io::{self, Write as _};
use std::sync::Arc;

use tutankhamun_server::shard;
use tutankhamun_server::shard_source::{ObjectStoreShardSource, ShardManager, ShardSource};
use tutankhamun_server::storage::StorageRegistry;

use crate::{ShardArgs, ShardCommand};

use super::filter::parse_filters;

pub(crate) fn run(args: &ShardArgs) -> anyhow::Result<()> {
    match &args.command {
        ShardCommand::Inspect { path } => {
            let stdout = io::stdout();
            let mut out = stdout.lock();
            shard::inspect(path, &mut out)
        }
        ShardCommand::List { url } => {
            let runtime = super::current_thread_runtime()?;
            runtime.block_on(list(url))
        }
        ShardCommand::Query {
            path,
            metrics,
            aggregate,
            filters,
        } => {
            // clap's `required = true` on `metrics` guarantees non-empty.
            let metric_refs: Vec<&str> = metrics.iter().map(String::as_str).collect();
            let parsed_filters = parse_filters(filters)?;
            let result = shard::query_shard(path, &parsed_filters, &metric_refs)?;
            let stdout = io::stdout();
            let mut out = stdout.lock();
            writeln!(out, "shard:    {}", path.display())?;
            shard::write_query_summary(
                &mut out,
                &parsed_filters,
                &metric_refs,
                *aggregate,
                &result,
            )?;
            Ok(())
        }
    }
}

async fn list(url: &str) -> anyhow::Result<()> {
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
