//! `t9n ingest` — build one or more shards from a CSV/TSV input file, writing
//! locally or staging-and-uploading to an `object_store` URL.

use std::path::Path;

use anyhow::Context as _;

use tutankhamun_server::ingest::{self, IngestOptions};

use crate::ShardByArg;

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
) -> anyhow::Result<()> {
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
            let runtime = super::current_thread_runtime()?;
            runtime.block_on(ingest::upload_ingest_tree(staging.path(), &url))?;
            println!("wrote {n} docs to {url}");
        }
    }
    Ok(())
}
