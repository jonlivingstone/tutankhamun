//! `t9n ingest` — build one or more shards from a CSV/TSV input file, writing
//! locally or staging-and-uploading to an `object_store` URL.

use std::collections::BTreeMap;
use std::path::Path;

use anyhow::Context as _;

use tutankhamun_server::ingest::{self, IngestOptions};

use crate::{FormatArg, ShardByArg};

// Args mirror the CLI flag count, which is the user-facing surface.
#[allow(clippy::too_many_arguments)]
pub(crate) fn run(
    input: &Path,
    output: &str,
    time: &str,
    metrics: &[String],
    strings: &[String],
    ints: &[String],
    delimiter: char,
    shard_by: ShardByArg,
    scale: &[(String, i8)],
    format: Option<FormatArg>,
) -> anyhow::Result<()> {
    if !delimiter.is_ascii() {
        anyhow::bail!("delimiter must be a single ASCII byte (got {delimiter:?})");
    }
    let opts = IngestOptions {
        time: time.to_string(),
        metrics: metrics.to_vec(),
        strings: strings.to_vec(),
        ints: ints.to_vec(),
        delimiter: delimiter as u8,
        shard_by: shard_by.into(),
    };
    let scales: BTreeMap<String, i8> = scale.iter().cloned().collect();

    // Explicit --format wins; otherwise infer from the extension.
    let parquet = match format {
        Some(FormatArg::Parquet) => true,
        Some(FormatArg::Csv) => false,
        None => input
            .extension()
            .is_some_and(|e| e.eq_ignore_ascii_case("parquet") || e.eq_ignore_ascii_case("pq")),
    };
    if !parquet && !scales.is_empty() {
        anyhow::bail!(
            "--scale is only supported for Parquet input (cast float CSV columns upstream)"
        );
    }

    let ingest_to = |dir: &Path| -> anyhow::Result<u64> {
        if parquet {
            ingest::ingest_parquet(input, dir, &opts, &scales)
        } else {
            ingest::ingest_csv(input, dir, &opts)
        }
    };

    match ingest::IngestDestination::parse(output)? {
        ingest::IngestDestination::Local(local) => {
            let n = ingest_to(&local)?;
            println!("wrote {n} docs to {}", local.display());
        }
        ingest::IngestDestination::Remote(url) => {
            let staging = tempfile::tempdir().context("create ingest staging tempdir")?;
            let n = ingest_to(staging.path())?;
            let runtime = super::current_thread_runtime()?;
            runtime.block_on(ingest::upload_ingest_tree(staging.path(), &url))?;
            println!("wrote {n} docs to {url}");
        }
    }
    Ok(())
}
