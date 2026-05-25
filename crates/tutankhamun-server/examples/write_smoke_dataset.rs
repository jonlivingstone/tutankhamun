//! Scratch helper: writes a synthetic 3-shard dataset under
//! `<root>/nyc_taxi/shard-NNN/` for end-to-end testing of
//! `t9n shard list` and `t9n shard inspect`.

use std::collections::BTreeMap;
use std::path::Path;

use roaring::RoaringBitmap;
use tutankhamun_server::shard::DiskShardWriter;

fn main() -> anyhow::Result<()> {
    let root = std::env::args()
        .nth(1)
        .ok_or_else(|| anyhow::anyhow!("usage: write_smoke_dataset <root>"))?;
    let root = Path::new(&root);
    let _ = std::fs::remove_dir_all(root);

    let countries = ["us", "de", "fr", "uk"];

    for i in 0..3i64 {
        let shard_dir = root.join("nyc_taxi").join(format!("shard-{i:03}"));
        let start = 1_700_000_000 + i * 3600;
        let end = start + 3600;

        let mut w = DiskShardWriter::new(&shard_dir, (start, end))?;
        w.add_metric("clicks", (0..1000).map(|x| x + i).collect())?;
        w.add_metric("impressions", (0..1000).map(|x| 100 + x + i).collect())?;

        // One bitmap per country, doc IDs round-robin'd across the 1000
        // rows. Gives the inverted-index round trip something interesting
        // to look at via `t9n shard inspect`.
        let mut postings: BTreeMap<String, RoaringBitmap> = BTreeMap::new();
        for doc in 0u32..1000 {
            let term = countries[doc as usize % countries.len()];
            postings.entry(term.to_string()).or_default().insert(doc);
        }
        w.add_string_field("country", postings)?;

        w.finalize()?;
        println!("wrote shard at {}", shard_dir.display());
    }

    Ok(())
}
