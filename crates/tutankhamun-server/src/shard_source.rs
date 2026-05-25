//! Server-side discovery of shards from a storage backend.
//!
//! A daemon learns what shards it owns by asking a [`ShardSource`]. The
//! current implementation, [`ObjectStoreShardSource`], walks the
//! configured `object_store` backend looking for directories that contain
//! a `metadata.json` file.
//!
//! **Distribution model**: each daemon owns exactly the shards visible at
//! its `--storage-url`. There is no automatic placement, hashing, or
//! cluster-aware filtering. Operators partition shards across daemons by
//! pointing each daemon at a different URL or prefix (one bucket per
//! daemon, one prefix per daemon, or one dataset per daemon).

use std::sync::Arc;

use anyhow::Result;
use async_trait::async_trait;
use object_store::ObjectStore;
use object_store::path::Path;

use crate::shard::Metadata;

const METADATA_FILE: &str = "metadata.json";

/// One discovered shard. Holds the shard's directory path within the
/// backend and a copy of its `metadata.json` contents.
#[derive(Debug, Clone)]
pub struct ShardSummary {
    /// Directory inside the backend that contains the shard's files.
    /// For a metadata file at `nyc_taxi/shard-001/metadata.json` this is
    /// `nyc_taxi/shard-001`.
    pub location: Path,
    pub metadata: Metadata,
}

impl ShardSummary {
    /// Whether the shard's `[time_range_start, time_range_end]` window
    /// intersects the closed interval `[start, end]`. Used by callers
    /// that want to skip shards outside a query's time range.
    #[must_use]
    pub fn intersects_time_range(&self, start: i64, end: i64) -> bool {
        self.metadata.time_range_start <= end && self.metadata.time_range_end >= start
    }
}

/// Source of shard discoveries. Implementations are responsible for
/// scanning a backend and returning every shard they consider this
/// daemon's responsibility.
#[async_trait]
pub trait ShardSource: Send + Sync {
    async fn discover(&self) -> Result<Vec<ShardSummary>>;
}

/// `ShardSource` backed by an `object_store` backend.
///
/// Discovery walks every object whose filename is `metadata.json`, reads
/// the JSON, and records the parent directory as the shard's location.
/// Per-shard `metadata.json` reads happen in parallel.
///
/// This source does not filter, hash, or otherwise restrict what it
/// returns — see the module docs for the distribution model.
pub struct ObjectStoreShardSource {
    store: Arc<dyn ObjectStore>,
}

impl ObjectStoreShardSource {
    #[must_use]
    pub fn new(store: Arc<dyn ObjectStore>) -> Self {
        Self { store }
    }
}

#[async_trait]
impl ShardSource for ObjectStoreShardSource {
    async fn discover(&self) -> Result<Vec<ShardSummary>> {
        use futures::StreamExt;

        let store = Arc::clone(&self.store);

        // Walk every object and collect the locations of metadata.json files.
        let mut stream = store.list(None);
        let mut metadata_paths = Vec::new();
        while let Some(meta) = stream.next().await {
            let meta = meta?;
            if meta.location.filename() == Some(METADATA_FILE) {
                metadata_paths.push(meta.location);
            }
        }

        // Fetch each metadata.json in parallel; map to ShardSummary.
        let summaries = futures::future::try_join_all(metadata_paths.into_iter().map(|path| {
            let store = Arc::clone(&store);
            async move {
                let bytes = store.get(&path).await?.bytes().await?;
                let metadata: Metadata = serde_json::from_slice(&bytes)?;
                metadata
                    .validate()
                    .map_err(|e| anyhow::anyhow!("invalid metadata at {path}: {e}"))?;
                let location = parent_path(&path);
                anyhow::Ok(ShardSummary { location, metadata })
            }
        }))
        .await?;

        Ok(summaries)
    }
}

/// Return the directory containing `path` as a new [`Path`]. Empty if
/// `path` has zero or one segments.
fn parent_path(path: &Path) -> Path {
    let parts: Vec<_> = path.parts().collect();
    let keep = parts.len().saturating_sub(1);
    let mut parent = Path::default();
    for part in parts.iter().take(keep) {
        parent = parent.child(part.as_ref());
    }
    parent
}

/// Holds the shards a daemon owns. v0 is a one-shot scan: call
/// [`refresh`](Self::refresh) once at startup to populate the list, then
/// read via [`all_shards`](Self::all_shards) or
/// [`shards_in_time_range`](Self::shards_in_time_range).
pub struct ShardManager {
    source: Arc<dyn ShardSource>,
    shards: Vec<ShardSummary>,
}

impl ShardManager {
    #[must_use]
    pub fn new(source: Arc<dyn ShardSource>) -> Self {
        Self {
            source,
            shards: Vec::new(),
        }
    }

    pub async fn refresh(&mut self) -> Result<()> {
        self.shards = self.source.discover().await?;
        Ok(())
    }

    #[must_use]
    pub fn all_shards(&self) -> &[ShardSummary] {
        &self.shards
    }

    /// Shards whose time-range intersects `[start, end]` (inclusive).
    #[must_use]
    pub fn shards_in_time_range(&self, start: i64, end: i64) -> Vec<&ShardSummary> {
        self.shards
            .iter()
            .filter(|s| s.intersects_time_range(start, end))
            .collect()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::shard::{FieldKind, FieldSchema};
    use crate::storage::StorageRegistry;
    use object_store::PutPayload;

    fn metadata(num_docs: u64, time_range: (i64, i64)) -> Metadata {
        Metadata {
            format_version: crate::shard::FORMAT_VERSION,
            num_docs,
            time_range_start: time_range.0,
            time_range_end: time_range.1,
            fields: vec![FieldSchema {
                name: "x".into(),
                kind: FieldKind::Metric,
            }],
        }
    }

    async fn put_metadata(store: &dyn ObjectStore, path: &str, m: &Metadata) {
        let bytes = serde_json::to_vec(m).unwrap();
        store
            .put(&Path::from(path), PutPayload::from_bytes(bytes.into()))
            .await
            .expect("put metadata");
    }

    fn summary(location: &str, num_docs: u64, time_range: (i64, i64)) -> ShardSummary {
        ShardSummary {
            location: Path::from(location),
            metadata: metadata(num_docs, time_range),
        }
    }

    #[test]
    fn intersects_time_range_covers_overlap_cases() {
        let s = summary("x", 1, (100, 200));
        // Fully inside the shard's range
        assert!(s.intersects_time_range(120, 180));
        // Query range starts before, ends inside
        assert!(s.intersects_time_range(50, 150));
        // Query range starts inside, ends after
        assert!(s.intersects_time_range(150, 250));
        // Query range fully contains the shard's range
        assert!(s.intersects_time_range(50, 250));
        // Edge: query touches shard start
        assert!(s.intersects_time_range(50, 100));
        // Edge: query touches shard end
        assert!(s.intersects_time_range(200, 300));
        // Strictly before the shard
        assert!(!s.intersects_time_range(0, 99));
        // Strictly after the shard
        assert!(!s.intersects_time_range(201, 300));
    }

    #[tokio::test]
    async fn discover_empty_backend_returns_empty() {
        let registry = StorageRegistry::from_url("memory:///").expect("registry");
        let source = ObjectStoreShardSource::new(registry.store());
        let summaries = source.discover().await.expect("discover");
        assert!(summaries.is_empty());
    }

    #[tokio::test]
    async fn discover_finds_shards_at_arbitrary_depth() {
        let registry = StorageRegistry::from_url("memory:///").expect("registry");
        let store = registry.store();

        put_metadata(
            &*store,
            "nyc_taxi/shard-001/metadata.json",
            &metadata(100, (1_700_000_000, 1_700_003_600)),
        )
        .await;
        put_metadata(
            &*store,
            "binance/BTCUSDT/shard-2024-01/metadata.json",
            &metadata(200, (1_704_067_200, 1_706_745_600)),
        )
        .await;
        // A non-shard file at the root should be ignored.
        put_metadata(&*store, "junk.json", &metadata(0, (0, 0))).await;

        let source = ObjectStoreShardSource::new(store);
        let mut summaries = source.discover().await.expect("discover");
        summaries.sort_by(|a, b| a.location.as_ref().cmp(b.location.as_ref()));

        assert_eq!(summaries.len(), 2);
        assert_eq!(
            summaries[0].location.as_ref(),
            "binance/BTCUSDT/shard-2024-01"
        );
        assert_eq!(summaries[0].metadata.num_docs, 200);
        assert_eq!(summaries[1].location.as_ref(), "nyc_taxi/shard-001");
        assert_eq!(summaries[1].metadata.num_docs, 100);
    }

    #[tokio::test]
    async fn discover_ignores_non_metadata_files() {
        let registry = StorageRegistry::from_url("memory:///").expect("registry");
        let store = registry.store();

        put_metadata(
            &*store,
            "nyc_taxi/shard-001/metadata.json",
            &metadata(100, (0, 0)),
        )
        .await;
        // Sibling files with different names should not be picked up.
        store
            .put(
                &Path::from("nyc_taxi/shard-001/metrics.arrow"),
                PutPayload::from_bytes(vec![0u8; 16].into()),
            )
            .await
            .unwrap();
        store
            .put(
                &Path::from("nyc_taxi/shard-001/other.json"),
                PutPayload::from_bytes(b"{}".to_vec().into()),
            )
            .await
            .unwrap();

        let source = ObjectStoreShardSource::new(store);
        let summaries = source.discover().await.expect("discover");
        assert_eq!(summaries.len(), 1);
        assert_eq!(summaries[0].location.as_ref(), "nyc_taxi/shard-001");
    }

    #[tokio::test]
    async fn discover_rejects_degenerate_time_range() {
        let registry = StorageRegistry::from_url("memory:///").expect("registry");
        let store = registry.store();
        // start > end is degenerate; discover should refuse it loudly so a
        // corrupt shard doesn't silently break time-range pruning later.
        put_metadata(
            &*store,
            "bad/shard-001/metadata.json",
            &Metadata {
                format_version: crate::shard::FORMAT_VERSION,
                num_docs: 0,
                time_range_start: 200,
                time_range_end: 100,
                fields: vec![],
            },
        )
        .await;

        let source = ObjectStoreShardSource::new(store);
        let err = source
            .discover()
            .await
            .expect_err("expected degenerate-range error");
        let msg = err.to_string();
        assert!(
            msg.to_lowercase().contains("time range") || msg.contains("invalid"),
            "got: {msg}"
        );
    }

    #[tokio::test]
    async fn manager_refresh_populates_and_replaces() {
        let registry = StorageRegistry::from_url("memory:///").expect("registry");
        let store = registry.store();
        put_metadata(
            &*store,
            "a/shard-001/metadata.json",
            &metadata(10, (0, 100)),
        )
        .await;

        let source: Arc<dyn ShardSource> = Arc::new(ObjectStoreShardSource::new(store.clone()));
        let mut manager = ShardManager::new(source);

        assert!(manager.all_shards().is_empty());

        manager.refresh().await.expect("refresh");
        assert_eq!(manager.all_shards().len(), 1);

        // Add a second shard and refresh again — should now see both.
        put_metadata(
            &*store,
            "b/shard-002/metadata.json",
            &metadata(20, (0, 100)),
        )
        .await;
        manager.refresh().await.expect("refresh");
        assert_eq!(manager.all_shards().len(), 2);
    }

    #[tokio::test]
    async fn manager_shards_in_time_range_filters() {
        // Build a manager directly with synthetic shards (no source needed).
        struct FakeSource(Vec<ShardSummary>);
        #[async_trait]
        impl ShardSource for FakeSource {
            async fn discover(&self) -> Result<Vec<ShardSummary>> {
                Ok(self.0.clone())
            }
        }

        let shards = vec![
            summary("a", 1, (100, 200)),
            summary("b", 1, (300, 400)),
            summary("c", 1, (500, 600)),
        ];
        let source: Arc<dyn ShardSource> = Arc::new(FakeSource(shards));
        let mut manager = ShardManager::new(source);
        manager.refresh().await.unwrap();

        let inside = manager.shards_in_time_range(250, 350);
        assert_eq!(inside.len(), 1);
        assert_eq!(inside[0].location.as_ref(), "b");

        let none = manager.shards_in_time_range(700, 800);
        assert!(none.is_empty());

        let spanning = manager.shards_in_time_range(150, 550);
        assert_eq!(spanning.len(), 3);
    }
}
