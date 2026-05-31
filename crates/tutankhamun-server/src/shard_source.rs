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

/// Turn a user-supplied dataset source (a bare path or an
/// `object_store` URL like `s3://bucket/dataset`) into a URL ready
/// for [`query_dataset`]. Bare paths are canonicalised and converted
/// to `file://`; the path must point at an existing directory.
pub fn resolve_source_url(source: &str) -> Result<String> {
    use anyhow::Context as _;
    if source.contains("://") {
        return Ok(source.to_string());
    }
    let canonical = std::fs::canonicalize(source).with_context(|| format!("resolve {source}"))?;
    if !canonical.is_dir() {
        anyhow::bail!("{} is not a valid directory path", canonical.display());
    }
    let url = url::Url::from_directory_path(&canonical)
        .map_err(|()| anyhow::anyhow!("{} is not a valid directory path", canonical.display()))?;
    Ok(url.to_string())
}

/// Strip any `user:pass@` userinfo from a URL before showing it to
/// the user, so a credentials-bearing source URL doesn't land on
/// stdout. Non-URL inputs pass through unchanged.
fn redact_credentials(source: &str) -> String {
    match url::Url::parse(source) {
        Ok(mut u) if !u.username().is_empty() || u.password().is_some() => {
            let _ = u.set_username("");
            let _ = u.set_password(None);
            u.to_string()
        }
        _ => source.to_string(),
    }
}

/// Render a `--from`/`--to` epoch for human display. `i64::MIN` and
/// `i64::MAX` come from the "unspecified bound" sentinel used by the
/// CLI; they'd otherwise round-trip through chrono as
/// `(out of range)`, which is honest but ugly. Display them as
/// `(unbounded)` so the user sees the actual semantic.
fn format_bound(epoch: i64) -> String {
    if epoch == i64::MIN || epoch == i64::MAX {
        "(unbounded)".to_string()
    } else {
        crate::shard::format_timestamp(epoch)
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

/// Holds the shards a daemon owns. Call
/// [`refresh`](Self::refresh) to populate the list, then read via
/// [`all_shards`](Self::all_shards) or
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

/// Structured result of a dataset-wide query — what `query_dataset`
/// returns. Callers render this however they like (CLI text, Arrow
/// batches, SQL result sets). `source` and `time_range` are always
/// present; the scan outcome lives in [`DatasetQueryOutcome`].
#[derive(Debug)]
pub struct DatasetQueryOutput {
    /// URL or filesystem path the query ran against, in the form the
    /// user supplied (or canonicalised for `file://`).
    pub source: String,
    pub time_range: Option<(i64, i64)>,
    pub outcome: DatasetQueryOutcome,
}

/// What happened during the dataset scan. New variants (e.g.
/// `PartialScanned` with per-shard errors) plug in without forcing
/// existing callers to revisit the always-present fields.
#[derive(Debug)]
pub enum DatasetQueryOutcome {
    /// No shards matched the time range, or none were discovered.
    NoShards,
    /// At least one shard was scanned and the results merged.
    Scanned {
        shards_scanned: std::num::NonZeroU64,
        result: crate::shard::QueryResult,
    },
}

/// Fan out [`crate::shard::query_shard`] across every shard
/// discovered under `url` (any `object_store` URL — `file://`,
/// `s3://`, `gs://`, `az://`, `memory://`) and return the aggregated
/// result. Shards are pulled into `cache` on miss and read via mmap
/// from the local copy.
///
/// When `time_range` is `Some((from, to))`, shards whose
/// `[time_range_start, time_range_end]` window does not intersect
/// `[from, to]` are pruned via [`ShardSummary::intersects_time_range`]
/// before any are fetched.
pub async fn query_dataset(
    url: &str,
    cache: &crate::cache::Cache,
    filters: &[crate::shard::FilterClause<'_>],
    metrics: &[&str],
    time_range: Option<(i64, i64)>,
) -> Result<DatasetQueryOutput> {
    use anyhow::Context as _;

    if let Some((from, to)) = time_range
        && from > to
    {
        // Swapped bounds quietly returns shards that *span* both
        // bounds (because the predicate is `shard.start <= end &&
        // shard.end >= start`), not the empty set a user would expect
        // — refuse the call rather than silently mislead.
        anyhow::bail!("invalid time range: from must be <= to (got from={from}, to={to})");
    }

    let registry = crate::storage::StorageRegistry::from_url(url)?;
    let discoverer: Arc<dyn ShardSource> = Arc::new(ObjectStoreShardSource::new(registry.store()));
    let all_summaries = discoverer.discover().await?;
    let summaries: Vec<&ShardSummary> = if let Some((from, to)) = time_range {
        all_summaries
            .iter()
            .filter(|s| s.intersects_time_range(from, to))
            .collect()
    } else {
        all_summaries.iter().collect()
    };

    let Some(shards_scanned) = std::num::NonZeroU64::new(summaries.len() as u64) else {
        return Ok(DatasetQueryOutput {
            source: url.to_string(),
            time_range,
            outcome: DatasetQueryOutcome::NoShards,
        });
    };

    let mut total = crate::shard::QueryResult::zeros(metrics.len());
    for summary in &summaries {
        let local_dir = cache
            .fetch_shard(summary)
            .await
            .with_context(|| format!("cache fetch {}", summary.location))?;
        let r = crate::shard::query_shard(&local_dir, filters, metrics)
            .with_context(|| format!("query {}", summary.location))?;
        total.num_docs += r.num_docs;
        total.matched += r.matched;
        for (t, s) in total.aggregates.iter_mut().zip(&r.aggregates) {
            t.absorb(s);
        }
    }

    Ok(DatasetQueryOutput {
        source: url.to_string(),
        time_range,
        outcome: DatasetQueryOutcome::Scanned {
            shards_scanned,
            result: total,
        },
    })
}

/// Render a [`DatasetQueryOutput`] to `out` in the t9n CLI text shape.
/// Single source of truth — main.rs and the test suite both call this
/// so the format can't drift between them.
pub fn render_dataset_query_output(
    out: &mut dyn std::io::Write,
    output: &DatasetQueryOutput,
    filters: &[crate::shard::FilterClause<'_>],
    metrics: &[&str],
    aggregate: crate::shard::Aggregate,
) -> std::io::Result<()> {
    writeln!(out, "dataset:  {}", redact_credentials(&output.source))?;
    if let Some((from, to)) = output.time_range {
        writeln!(
            out,
            "range:    {} .. {}",
            format_bound(from),
            format_bound(to),
        )?;
    }
    match &output.outcome {
        DatasetQueryOutcome::NoShards => writeln!(out, "no shards found"),
        DatasetQueryOutcome::Scanned {
            shards_scanned,
            result,
        } => {
            writeln!(out, "shards:   {shards_scanned} scanned")?;
            crate::shard::write_query_summary(out, filters, metrics, aggregate, result)
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::shard::{FieldKind, FieldSchema};
    use crate::storage::StorageRegistry;
    use object_store::PutPayload;

    /// Test-only: composes main.rs's CLI flow against a throwaway
    /// cache so the existing text-shape assertions stay terse.
    async fn query_dataset_cli(
        root: &std::path::Path,
        filters: &[crate::shard::FilterClause<'_>],
        metrics: &[&str],
        aggregate: crate::shard::Aggregate,
        time_range: Option<(i64, i64)>,
        out: &mut dyn std::io::Write,
    ) -> Result<()> {
        let url = resolve_source_url(root.to_str().expect("utf-8 test path"))?;
        let registry = crate::storage::StorageRegistry::from_url(&url)?;
        let cache_dir = tempfile::tempdir().expect("test cache tmpdir");
        let cache = crate::cache::Cache::open(
            cache_dir.path().to_path_buf(),
            registry.store(),
            url.clone(),
            u64::MAX,
        )?;
        let output = query_dataset(&url, &cache, filters, metrics, time_range).await?;
        render_dataset_query_output(out, &output, filters, metrics, aggregate)?;
        std::mem::forget(cache_dir);
        Ok(())
    }

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
            content_hashes: std::collections::BTreeMap::default(),
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
    fn redact_credentials_strips_userinfo() {
        assert_eq!(
            redact_credentials("s3://AKIA:SECRET@bucket/path"),
            "s3://bucket/path"
        );
        assert_eq!(
            redact_credentials("s3://AKIA@bucket/path"),
            "s3://bucket/path"
        );
        assert_eq!(redact_credentials("s3://bucket/path"), "s3://bucket/path");
        assert_eq!(redact_credentials("/tmp/local"), "/tmp/local");
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
                content_hashes: std::collections::BTreeMap::default(),
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

    use std::collections::BTreeMap;

    use crate::shard::DiskShardWriter;
    use roaring::RoaringBitmap;

    fn write_query_shard(
        dir: &std::path::Path,
        clicks: Vec<i64>,
        us_docs: &[u32],
        de_docs: &[u32],
    ) {
        let mut postings: BTreeMap<String, RoaringBitmap> = BTreeMap::new();
        if !us_docs.is_empty() {
            postings.insert("us".to_string(), us_docs.iter().copied().collect());
        }
        if !de_docs.is_empty() {
            postings.insert("de".to_string(), de_docs.iter().copied().collect());
        }
        let mut w = DiskShardWriter::new(dir, (0, 0)).expect("new writer");
        w.add_metric("clicks", clicks).expect("add_metric");
        if !postings.is_empty() {
            w.add_string_field("country", postings)
                .expect("add_string_field");
        }
        w.finalize().expect("finalize");
    }

    #[tokio::test]
    async fn query_dataset_aggregates_across_shards() {
        let tmp = tempfile::tempdir().expect("tmpdir");
        write_query_shard(
            &tmp.path().join("dataset/shard-000"),
            vec![10, 20, 30],
            &[0, 2],
            &[1],
        );
        write_query_shard(
            &tmp.path().join("dataset/shard-001"),
            vec![100, 200, 300, 400],
            &[0, 3],
            &[1, 2],
        );

        // Filtered sum: us docs in shard-000 = 10+30 = 40, in shard-001 = 100+400 = 500.
        let mut buf = Vec::new();
        query_dataset_cli(
            tmp.path(),
            &[crate::shard::FilterClause::equals("country", "us")],
            &["clicks"],
            crate::shard::Aggregate::Sum,
            None,
            &mut buf,
        )
        .await
        .expect("query dataset");
        let out = String::from_utf8(buf).expect("utf-8");
        assert!(out.contains("shards:   2 scanned"), "{out}");
        assert!(out.contains("matched:  4 / 7 docs"), "{out}");
        assert!(out.contains("clicks:   sum = 540"), "{out}");
    }

    #[tokio::test]
    async fn query_dataset_no_filter_sums_every_doc() {
        let tmp = tempfile::tempdir().expect("tmpdir");
        write_query_shard(&tmp.path().join("a/shard-000"), vec![1, 2, 3], &[0], &[]);
        write_query_shard(
            &tmp.path().join("b/shard-001"),
            vec![10, 20, 30, 40],
            &[0],
            &[],
        );

        let mut buf = Vec::new();
        query_dataset_cli(
            tmp.path(),
            &[],
            &["clicks"],
            crate::shard::Aggregate::Sum,
            None,
            &mut buf,
        )
        .await
        .expect("query dataset");
        let out = String::from_utf8(buf).expect("utf-8");
        assert!(out.contains("shards:   2 scanned"), "{out}");
        assert!(out.contains("matched:  all 7 docs"), "{out}");
        assert!(out.contains("clicks:   sum = 106"), "{out}");
        assert!(!out.contains("filter:"), "{out}");
    }

    #[tokio::test]
    async fn query_dataset_empty_dir_reports_no_shards() {
        let tmp = tempfile::tempdir().expect("tmpdir");
        let mut buf = Vec::new();
        query_dataset_cli(
            tmp.path(),
            &[],
            &["clicks"],
            crate::shard::Aggregate::Sum,
            None,
            &mut buf,
        )
        .await
        .expect("query dataset");
        let out = String::from_utf8(buf).expect("utf-8");
        assert!(out.contains("no shards found"), "{out}");
    }

    #[tokio::test]
    async fn query_dataset_rejects_file_argument() {
        let tmp = tempfile::tempdir().expect("tmpdir");
        let file_path = tmp.path().join("not-a-directory.txt");
        std::fs::write(&file_path, b"hello").expect("write");

        let mut buf = Vec::new();
        let err = query_dataset_cli(
            &file_path,
            &[],
            &["clicks"],
            crate::shard::Aggregate::Sum,
            None,
            &mut buf,
        )
        .await
        .expect_err("expected file-not-directory rejection");
        let msg = err.to_string();
        assert!(msg.contains("not a valid directory path"), "{msg}");
    }

    /// Helper for the time-range tests: writes 5 single-day shards
    /// under `<root>/day-N/` with time ranges (N*86400, N*86400 + 60)
    /// and 1 doc of `clicks=10*(N+1)` per shard, for N in 0..5.
    /// Returns the root path.
    fn write_5_day_dataset(root: &std::path::Path) {
        for n in 0u32..5 {
            let shard = root.join(format!("day-{n}"));
            let mut w = crate::shard::DiskShardWriter::new(
                &shard,
                (i64::from(n) * 86_400, i64::from(n) * 86_400 + 60),
            )
            .expect("new");
            w.add_metric("clicks", vec![10 * i64::from(n + 1)])
                .expect("add_metric");
            w.finalize().expect("finalize");
        }
    }

    #[tokio::test]
    async fn query_dataset_with_range_keeps_intersecting_shards() {
        let tmp = tempfile::tempdir().expect("tmpdir");
        write_5_day_dataset(tmp.path());
        // Days 0..5 with values 10, 20, 30, 40, 50.
        // Range covers days 1 and 2 (epoch 86400 .. 200000).
        let mut buf = Vec::new();
        query_dataset_cli(
            tmp.path(),
            &[],
            &["clicks"],
            crate::shard::Aggregate::Sum,
            Some((86_400, 200_000)),
            &mut buf,
        )
        .await
        .expect("query dataset");
        let out = String::from_utf8(buf).expect("utf-8");
        assert!(out.contains("range:"), "{out}");
        assert!(out.contains("shards:   2 scanned"), "{out}");
        // 20 + 30 = 50, summed across the 2 scanned shards.
        assert!(out.contains("clicks:   sum = 50"), "{out}");
    }

    #[tokio::test]
    async fn query_dataset_with_from_only_keeps_shards_after() {
        let tmp = tempfile::tempdir().expect("tmpdir");
        write_5_day_dataset(tmp.path());
        // From day 3 onward; i64::MAX as the implicit upper bound.
        let mut buf = Vec::new();
        query_dataset_cli(
            tmp.path(),
            &[],
            &["clicks"],
            crate::shard::Aggregate::Sum,
            Some((3 * 86_400, i64::MAX)),
            &mut buf,
        )
        .await
        .expect("query dataset");
        let out = String::from_utf8(buf).expect("utf-8");
        assert!(out.contains("shards:   2 scanned"), "{out}"); // days 3 + 4
        assert!(out.contains("clicks:   sum = 90"), "{out}"); // 40 + 50
    }

    #[tokio::test]
    async fn query_dataset_with_to_only_keeps_shards_before() {
        let tmp = tempfile::tempdir().expect("tmpdir");
        write_5_day_dataset(tmp.path());
        // i64::MIN as the implicit lower bound, up to day 1 end.
        let mut buf = Vec::new();
        query_dataset_cli(
            tmp.path(),
            &[],
            &["clicks"],
            crate::shard::Aggregate::Sum,
            Some((i64::MIN, 86_400 + 60)),
            &mut buf,
        )
        .await
        .expect("query dataset");
        let out = String::from_utf8(buf).expect("utf-8");
        assert!(out.contains("shards:   2 scanned"), "{out}"); // days 0 + 1
        assert!(out.contains("clicks:   sum = 30"), "{out}"); // 10 + 20
    }

    #[tokio::test]
    async fn query_dataset_range_excluding_all_shards_reports_no_shards() {
        let tmp = tempfile::tempdir().expect("tmpdir");
        write_5_day_dataset(tmp.path());
        // Range strictly after day 4's window.
        let mut buf = Vec::new();
        query_dataset_cli(
            tmp.path(),
            &[],
            &["clicks"],
            crate::shard::Aggregate::Sum,
            Some((10 * 86_400, 11 * 86_400)),
            &mut buf,
        )
        .await
        .expect("query dataset");
        let out = String::from_utf8(buf).expect("utf-8");
        assert!(out.contains("range:"), "{out}");
        assert!(out.contains("no shards found"), "{out}");
        assert!(
            !out.contains("shards:"),
            "shouldn't print scanned count: {out}"
        );
    }

    #[tokio::test]
    async fn query_dataset_aggregates_multi_metric_across_shards() {
        let tmp = tempfile::tempdir().expect("tmpdir");
        // Two shards, each with two metrics.
        for (sub, clicks, impressions) in [
            ("a/shard-000", vec![10, 20, 30], vec![100, 200, 300]),
            ("b/shard-001", vec![40, 50], vec![400, 500]),
        ] {
            let shard = tmp.path().join(sub);
            let mut w = crate::shard::DiskShardWriter::new(&shard, (0, 0)).expect("new");
            w.add_metric("clicks", clicks).expect("add clicks");
            w.add_metric("impressions", impressions)
                .expect("add impressions");
            w.finalize().expect("finalize");
        }

        let mut buf = Vec::new();
        query_dataset_cli(
            tmp.path(),
            &[],
            &["clicks", "impressions"],
            crate::shard::Aggregate::Sum,
            None,
            &mut buf,
        )
        .await
        .expect("query dataset");
        let out = String::from_utf8(buf).expect("utf-8");
        assert!(out.contains("shards:   2 scanned"), "{out}");
        // 10+20+30+40+50 = 150
        assert!(out.contains("clicks:        sum = 150"), "{out}");
        // 100+200+300+400+500 = 1500
        assert!(out.contains("impressions:   sum = 1500"), "{out}");
    }

    #[tokio::test]
    async fn query_dataset_rejects_inverted_range() {
        // from > to would silently match shards spanning both bounds
        // (not the empty set users expect from the closed interval),
        // so the engine refuses it with a clear error rather than
        // returning misleading data.
        let tmp = tempfile::tempdir().expect("tmpdir");
        write_5_day_dataset(tmp.path());
        let mut buf = Vec::new();
        let err = query_dataset_cli(
            tmp.path(),
            &[],
            &["clicks"],
            crate::shard::Aggregate::Sum,
            Some((200_000, 86_400)),
            &mut buf,
        )
        .await
        .expect_err("expected inverted-range rejection");
        let msg = err.to_string();
        assert!(msg.contains("from must be <= to"), "{msg}");
    }

    #[tokio::test]
    async fn query_dataset_no_range_unchanged() {
        // Regression guard: passing None preserves today's behaviour
        // (every shard scanned, no time-range line in output).
        let tmp = tempfile::tempdir().expect("tmpdir");
        write_5_day_dataset(tmp.path());
        let mut buf = Vec::new();
        query_dataset_cli(
            tmp.path(),
            &[],
            &["clicks"],
            crate::shard::Aggregate::Sum,
            None,
            &mut buf,
        )
        .await
        .expect("query dataset");
        let out = String::from_utf8(buf).expect("utf-8");
        assert!(out.contains("shards:   5 scanned"), "{out}");
        assert!(out.contains("clicks:   sum = 150"), "{out}"); // 10+20+30+40+50
        assert!(!out.contains("range:"), "{out}");
    }

    #[tokio::test]
    async fn query_dataset_surfaces_per_shard_error_with_path() {
        let tmp = tempfile::tempdir().expect("tmpdir");
        write_query_shard(&tmp.path().join("ok/shard-000"), vec![1, 2, 3], &[0], &[]);

        let mut buf = Vec::new();
        let err = query_dataset_cli(
            tmp.path(),
            &[],
            &["no_such_metric"],
            crate::shard::Aggregate::Sum,
            None,
            &mut buf,
        )
        .await
        .expect_err("expected error from missing metric in one of the shards");
        // `{:#}` renders the full anyhow chain (with_context wraps the
        // inner error, so the top-level Display only shows "query …").
        let msg = format!("{err:#}");
        assert!(msg.contains("no_such_metric"), "{msg}");
        assert!(msg.contains("shard-000"), "{msg}");
    }
}
