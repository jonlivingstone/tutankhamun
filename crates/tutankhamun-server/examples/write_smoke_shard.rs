//! Scratch helper: writes a synthetic shard at the path given as $1.
//! Used to smoke-test `t9n shard inspect` end-to-end.

use std::path::Path;

use tutankhamun_server::shard::DiskShardWriter;

fn main() -> anyhow::Result<()> {
    let path = std::env::args()
        .nth(1)
        .ok_or_else(|| anyhow::anyhow!("usage: write_smoke_shard <path>"))?;
    let path = Path::new(&path);
    let _ = std::fs::remove_dir_all(path);
    let mut w = DiskShardWriter::new(path, (1_700_000_000, 1_700_003_600))?;
    w.add_metric("clicks", (0..1000).map(|i| i * 3).collect())?;
    w.add_metric("impressions", (0..1000).map(|i| 100 + i).collect())?;
    w.add_metric("dwell_ms", (0..1000).map(|i| 50 + (i % 100)).collect())?;
    w.finalize()?;
    println!("wrote shard at {}", path.display());
    Ok(())
}
