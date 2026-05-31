//! Local hot-storage cache for shards fetched from an
//! [`object_store`] backend.
//!
//! The cache materialises every file under a shard's remote prefix
//! into a deterministic local subdirectory and returns the local
//! path. [`crate::shard::DiskShard::open`] reads that copy via mmap.
//!
//! Eviction is LRU at shard granularity: when an insert would push
//! the cache over its size cap, whole shards are removed
//! oldest-mtime-first until back under the cap. A single shard
//! larger than the cap is admitted anyway, leaving the cache
//! temporarily over.
//!
//! Hash validation is gated on the writer's `format_version`. For
//! shards at `format_version >= 2`, every fetch (hit or miss)
//! re-hashes the local files and compares them to the per-file
//! digests in [`Metadata::content_hashes`]; a mismatch (corrupt
//! local copy or remote drift) triggers a re-download. Shards
//! written under `format_version` 1 carry no hashes and load
//! unvalidated.
//!
//! No cross-process locking. Two concurrent `t9n` invocations
//! sharing a cache dir may race on a fresh download; the second
//! writer wins. Acceptable for the single-user CLI today.

pub mod size;

use std::collections::HashMap;
use std::fs;
use std::path::{Path as StdPath, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::SystemTime;

use anyhow::{Context, Result, bail};
use futures::StreamExt;
use object_store::ObjectStore;
use object_store::path::Path;
use sha2::{Digest, Sha256};
use tokio::io::AsyncWriteExt;

use crate::shard::{Metadata, sha256_file};
use crate::shard_source::ShardSummary;

const SHARD_KEY_LEN: usize = 16;

/// Hot-storage cache backed by a local directory.
#[derive(Debug)]
pub struct Cache {
    dir: PathBuf,
    store: Arc<dyn ObjectStore>,
    store_identity: String,
    size_cap: u64,
    state: Mutex<CacheState>,
}

#[derive(Debug)]
struct CacheState {
    total_bytes: u64,
    shards: HashMap<String, ShardEntry>,
}

#[derive(Clone, Debug)]
struct ShardEntry {
    local_dir: PathBuf,
    size_bytes: u64,
    last_access: SystemTime,
}

impl Cache {
    /// Open or create a cache rooted at `dir`. `store_identity` is a
    /// stable string that disambiguates shards coming from different
    /// backends (typically the source URL). `size_cap` is a soft
    /// upper bound on resident bytes — see the module docs for the
    /// single-shard-over-cap escape.
    ///
    /// Walks `dir` on construction to register any pre-existing
    /// cached shards (left over from a previous process).
    pub fn open(
        dir: PathBuf,
        store: Arc<dyn ObjectStore>,
        store_identity: String,
        size_cap: u64,
    ) -> Result<Self> {
        fs::create_dir_all(&dir).with_context(|| format!("create cache dir {}", dir.display()))?;
        let state = scan_existing(&dir)?;
        Ok(Self {
            dir,
            store,
            store_identity,
            size_cap,
            state: Mutex::new(state),
        })
    }

    /// Materialise every file under `summary.location` into the
    /// cache and return the local shard directory. Validates per-file
    /// SHA-256 against [`Metadata::content_hashes`] when
    /// `metadata.format_version >= 2`; older shards load unvalidated.
    pub async fn fetch_shard(&self, summary: &ShardSummary) -> Result<PathBuf> {
        let key = self.shard_key(&summary.location);
        let local_dir = self.dir.join(&key);

        if validate_local(&local_dir, &summary.metadata).is_err() {
            self.download(&summary.location, &local_dir).await?;
            validate_local(&local_dir, &summary.metadata)
                .with_context(|| format!("validate {}", local_dir.display()))?;
        }

        let size = dir_size(&local_dir)?;
        let now = SystemTime::now();
        touch_dir_mtime(&local_dir, now);

        self.register(&key, &local_dir, size, now)?;

        Ok(local_dir)
    }

    fn shard_key(&self, location: &Path) -> String {
        let mut hasher = Sha256::new();
        hasher.update(self.store_identity.as_bytes());
        hasher.update(b"::");
        hasher.update(location.as_ref().as_bytes());
        let digest = hasher.finalize();
        hex::encode(&digest[..SHARD_KEY_LEN])
    }

    async fn download(&self, remote_dir: &Path, local_dir: &StdPath) -> Result<()> {
        if local_dir.exists() {
            fs::remove_dir_all(local_dir)
                .with_context(|| format!("remove stale {}", local_dir.display()))?;
        }
        fs::create_dir_all(local_dir).with_context(|| format!("create {}", local_dir.display()))?;

        let mut stream = self.store.list(Some(remote_dir));
        while let Some(meta) = stream.next().await {
            let meta = meta.context("list shard files")?;
            let rel = strip_prefix(&meta.location, remote_dir).with_context(|| {
                format!(
                    "remote object {} not under shard prefix {remote_dir}",
                    meta.location
                )
            })?;
            let dest = safe_join(local_dir, &rel)
                .with_context(|| format!("reject suspicious remote path {}", meta.location))?;
            if let Some(parent) = dest.parent() {
                fs::create_dir_all(parent)
                    .with_context(|| format!("create {}", parent.display()))?;
            }
            let get = self
                .store
                .get(&meta.location)
                .await
                .context("fetch shard file")?;
            stream_to_local(&dest, get.into_stream()).await?;
        }
        Ok(())
    }

    fn register(
        &self,
        key: &str,
        local_dir: &StdPath,
        size_bytes: u64,
        now: SystemTime,
    ) -> Result<()> {
        let mut state = self.state.lock().expect("cache state mutex");
        if let Some(prev) = state.shards.insert(
            key.to_string(),
            ShardEntry {
                local_dir: local_dir.to_path_buf(),
                size_bytes,
                last_access: now,
            },
        ) {
            state.total_bytes = state.total_bytes.saturating_sub(prev.size_bytes);
        }
        state.total_bytes = state.total_bytes.saturating_add(size_bytes);
        self.evict_if_needed(&mut state, key)?;
        Ok(())
    }

    fn evict_if_needed(&self, state: &mut CacheState, protected: &str) -> Result<()> {
        if state.total_bytes <= self.size_cap {
            return Ok(());
        }
        let mut victims: Vec<(String, SystemTime, u64, PathBuf)> = state
            .shards
            .iter()
            .filter(|(k, _)| k.as_str() != protected)
            .map(|(k, e)| (k.clone(), e.last_access, e.size_bytes, e.local_dir.clone()))
            .collect();
        victims.sort_by_key(|(_, mtime, _, _)| *mtime);

        for (key, _, bytes, path) in victims {
            if state.total_bytes <= self.size_cap {
                break;
            }
            fs::remove_dir_all(&path).with_context(|| format!("evict {}", path.display()))?;
            state.shards.remove(&key);
            state.total_bytes = state.total_bytes.saturating_sub(bytes);
        }
        Ok(())
    }

    #[cfg(test)]
    fn total_bytes(&self) -> u64 {
        self.state.lock().expect("cache state mutex").total_bytes
    }
}

fn validate_local(dir: &StdPath, metadata: &Metadata) -> Result<()> {
    if !dir.is_dir() {
        bail!("{} missing", dir.display());
    }
    let metadata_path = dir.join(crate::shard::METADATA_FILE);
    if !metadata_path.is_file() {
        bail!("{} missing", metadata_path.display());
    }
    // Hash validation is gated on the writer version, not the
    // presence of hashes: a v2 shard whose `content_hashes` map is
    // somehow empty would otherwise silently bypass validation.
    if metadata.format_version < 2 {
        return Ok(());
    }
    for (rel, expected) in &metadata.content_hashes {
        let path = safe_join(dir, rel)
            .with_context(|| format!("reject suspicious content_hashes entry {rel:?}"))?;
        if !path.is_file() {
            bail!("{} missing", path.display());
        }
        let actual = sha256_file(&path)?;
        if &actual != expected {
            bail!(
                "{} content hash mismatch (expected {expected}, got {actual})",
                path.display()
            );
        }
    }
    Ok(())
}

/// Join `rel` onto `dir`, rejecting anything that would escape the
/// shard root (path traversal). `rel` must be a forward-slash-
/// separated, relative path with no `..` segments — `metadata.json`
/// is attacker-controllable once we read it from object storage, so
/// every contributing segment is checked.
fn safe_join(dir: &StdPath, rel: &str) -> Result<PathBuf> {
    if rel.is_empty() {
        bail!("empty path");
    }
    if rel.contains('\\') {
        bail!("backslash in path");
    }
    if rel.contains('\0') {
        bail!("nul byte in path");
    }
    let mut out = dir.to_path_buf();
    for segment in rel.split('/') {
        if segment.is_empty() {
            bail!("empty path segment");
        }
        if segment == ".." || segment == "." {
            bail!("relative segment {segment:?}");
        }
        if segment.contains(':') {
            bail!("drive-letter or scheme segment {segment:?}");
        }
        out.push(segment);
    }
    Ok(out)
}

fn strip_prefix(child: &Path, prefix: &Path) -> Result<String> {
    let child_str = child.as_ref();
    let prefix_str = prefix.as_ref();
    if prefix_str.is_empty() {
        return Ok(child_str.to_string());
    }
    let prefix_with_slash = format!("{prefix_str}/");
    child_str
        .strip_prefix(&prefix_with_slash)
        .map(str::to_string)
        .ok_or_else(|| anyhow::anyhow!("{child_str} not under {prefix_str}"))
}

fn dir_size(dir: &StdPath) -> Result<u64> {
    let mut total: u64 = 0;
    crate::shard::walk_files(dir, dir, &mut |path, _rel| {
        let meta = fs::metadata(path).with_context(|| format!("stat {}", path.display()))?;
        total = total.saturating_add(meta.len());
        Ok(())
    })?;
    Ok(total)
}

/// Stream `chunks` into `dest` via a sibling
/// `.tmp.<pid>.<call>` file, then atomic-rename into place. Peak
/// memory is one chunk, not the full object — the multi-GiB OOM
/// you'd otherwise see fetching a big `metrics.arrow` from object
/// storage.
///
/// Tmp name uses `with_file_name` (not `with_extension`): the
/// latter would *replace* the extension, so `country.fst` and
/// `country.posting` would both produce `country.tmp.…`. The
/// per-call counter then disambiguates concurrent fetches of the
/// same file within one process — e.g. two `tokio::join!`'d queries
/// against an uncached shard.
async fn stream_to_local(
    dest: &StdPath,
    mut chunks: futures::stream::BoxStream<'_, object_store::Result<bytes::Bytes>>,
) -> Result<()> {
    static TMP_COUNTER: AtomicU64 = AtomicU64::new(0);
    let file_name = dest
        .file_name()
        .ok_or_else(|| anyhow::anyhow!("no filename in {}", dest.display()))?;
    let nonce = TMP_COUNTER.fetch_add(1, Ordering::Relaxed);
    let tmp_name = format!(
        "{}.tmp.{}.{}",
        file_name.to_string_lossy(),
        std::process::id(),
        nonce,
    );
    let tmp = dest.with_file_name(tmp_name);

    // No `sync_all` — fsync per posting file would barrier the
    // disk hundreds of times for a typical shard. Cache integrity
    // is recovered on the next fetch (content-hash mismatch
    // triggers re-download; `download`'s prelude `remove_dir_all`
    // wipes any half-written tree from a crash).
    let mut file = tokio::fs::File::create(&tmp)
        .await
        .with_context(|| format!("create {}", tmp.display()))?;
    while let Some(chunk) = chunks.next().await {
        let chunk = chunk.with_context(|| format!("read remote chunk for {}", dest.display()))?;
        file.write_all(&chunk)
            .await
            .with_context(|| format!("write chunk to {}", tmp.display()))?;
    }
    tokio::fs::rename(&tmp, dest)
        .await
        .with_context(|| format!("rename {} -> {}", tmp.display(), dest.display()))?;
    Ok(())
}

/// Best-effort mtime touch used to seed cross-process LRU ordering.
/// In-process LRU uses the in-memory `last_access` instead, so a
/// silent failure here only degrades eviction priority for *next*
/// invocations sharing the same cache dir.
fn touch_dir_mtime(dir: &StdPath, now: SystemTime) {
    if let Ok(f) = fs::OpenOptions::new().read(true).open(dir) {
        let _ = f.set_modified(now);
    }
}

fn scan_existing(dir: &StdPath) -> Result<CacheState> {
    let mut state = CacheState {
        total_bytes: 0,
        shards: HashMap::new(),
    };
    if !dir.is_dir() {
        return Ok(state);
    }
    for entry in fs::read_dir(dir).with_context(|| format!("read_dir {}", dir.display()))? {
        let entry = entry?;
        let path = entry.path();
        if !entry.file_type()?.is_dir() {
            continue;
        }
        let Some(key) = path
            .file_name()
            .and_then(|s| s.to_str())
            .map(str::to_string)
        else {
            continue;
        };
        let size = dir_size(&path)?;
        let mtime = entry
            .metadata()
            .and_then(|m| m.modified())
            .unwrap_or(SystemTime::UNIX_EPOCH);
        state.total_bytes = state.total_bytes.saturating_add(size);
        state.shards.insert(
            key,
            ShardEntry {
                local_dir: path,
                size_bytes: size,
                last_access: mtime,
            },
        );
    }
    Ok(state)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::shard::{DiskShardWriter, FORMAT_VERSION};
    use crate::storage::StorageRegistry;
    use object_store::PutPayload;

    async fn upload_shard(store: &dyn ObjectStore, remote_prefix: &str, local_dir: &StdPath) {
        for entry in walk_dir(local_dir) {
            let rel = entry
                .strip_prefix(local_dir)
                .unwrap()
                .components()
                .map(|c| c.as_os_str().to_string_lossy())
                .collect::<Vec<_>>()
                .join("/");
            let remote_path = Path::from(format!("{remote_prefix}/{rel}"));
            let bytes = fs::read(&entry).unwrap();
            store
                .put(&remote_path, PutPayload::from(bytes))
                .await
                .unwrap();
        }
    }

    fn walk_dir(dir: &StdPath) -> Vec<PathBuf> {
        let mut out = Vec::new();
        for entry in fs::read_dir(dir).unwrap() {
            let entry = entry.unwrap();
            let path = entry.path();
            if entry.file_type().unwrap().is_dir() {
                out.extend(walk_dir(&path));
            } else {
                out.push(path);
            }
        }
        out
    }

    fn write_local_shard(path: &StdPath, values: Vec<i64>) {
        let mut w = DiskShardWriter::new(path, (0, 0)).unwrap();
        w.add_metric("m", values).unwrap();
        w.finalize().unwrap();
    }

    #[tokio::test]
    async fn fetch_shard_downloads_on_miss_and_hits_on_repeat() {
        let local_src = tempfile::tempdir().unwrap();
        write_local_shard(local_src.path(), vec![1, 2, 3]);

        let registry = StorageRegistry::from_url("memory:///").unwrap();
        let store = registry.store();
        upload_shard(&*store, "data/shard-000", local_src.path()).await;

        let metadata: Metadata = serde_json::from_slice(
            &store
                .get(&Path::from("data/shard-000/metadata.json"))
                .await
                .unwrap()
                .bytes()
                .await
                .unwrap(),
        )
        .unwrap();
        assert_eq!(metadata.format_version, FORMAT_VERSION);
        assert!(!metadata.content_hashes.is_empty());

        let summary = ShardSummary {
            location: Path::from("data/shard-000"),
            metadata,
        };

        let cache_dir = tempfile::tempdir().unwrap();
        let cache = Cache::open(
            cache_dir.path().to_path_buf(),
            Arc::clone(&store),
            "memory:///".to_string(),
            10 * 1024 * 1024,
        )
        .unwrap();

        let local_a = cache.fetch_shard(&summary).await.expect("miss");
        assert!(local_a.join("metadata.json").is_file());
        assert!(local_a.join("metrics.arrow").is_file());

        let local_b = cache.fetch_shard(&summary).await.expect("hit");
        assert_eq!(local_a, local_b);
    }

    #[tokio::test]
    async fn fetch_shard_re_downloads_on_corrupted_local_file() {
        let local_src = tempfile::tempdir().unwrap();
        write_local_shard(local_src.path(), vec![10, 20, 30]);

        let registry = StorageRegistry::from_url("memory:///").unwrap();
        let store = registry.store();
        upload_shard(&*store, "data/shard-000", local_src.path()).await;
        let metadata: Metadata = serde_json::from_slice(
            &store
                .get(&Path::from("data/shard-000/metadata.json"))
                .await
                .unwrap()
                .bytes()
                .await
                .unwrap(),
        )
        .unwrap();

        let summary = ShardSummary {
            location: Path::from("data/shard-000"),
            metadata,
        };

        let cache_dir = tempfile::tempdir().unwrap();
        let cache = Cache::open(
            cache_dir.path().to_path_buf(),
            Arc::clone(&store),
            "memory:///".to_string(),
            10 * 1024 * 1024,
        )
        .unwrap();

        let local = cache.fetch_shard(&summary).await.unwrap();
        let metrics = local.join("metrics.arrow");
        fs::write(&metrics, b"garbage").unwrap();

        let local2 = cache.fetch_shard(&summary).await.unwrap();
        assert_eq!(local, local2);
        let restored = fs::read(&metrics).unwrap();
        assert_ne!(restored, b"garbage");
    }

    #[tokio::test]
    async fn cache_evicts_oldest_when_over_cap() {
        let registry = StorageRegistry::from_url("memory:///").unwrap();
        let store = registry.store();

        let mut summaries = Vec::new();
        for i in 0..3 {
            let local = tempfile::tempdir().unwrap();
            let values: Vec<i64> = (0..2000).map(|n| n + i * 1000).collect();
            write_local_shard(local.path(), values);
            let remote = format!("data/shard-{i:03}");
            upload_shard(&*store, &remote, local.path()).await;
            let metadata: Metadata = serde_json::from_slice(
                &store
                    .get(&Path::from(format!("{remote}/metadata.json")))
                    .await
                    .unwrap()
                    .bytes()
                    .await
                    .unwrap(),
            )
            .unwrap();
            summaries.push(ShardSummary {
                location: Path::from(remote),
                metadata,
            });
            std::mem::forget(local);
        }

        let cache_dir = tempfile::tempdir().unwrap();
        let cache = Cache::open(
            cache_dir.path().to_path_buf(),
            Arc::clone(&store),
            "memory:///".to_string(),
            32 * 1024, // small cap forces eviction after first shard
        )
        .unwrap();

        let first = cache.fetch_shard(&summaries[0]).await.unwrap();
        tokio::time::sleep(std::time::Duration::from_millis(10)).await;
        cache.fetch_shard(&summaries[1]).await.unwrap();
        tokio::time::sleep(std::time::Duration::from_millis(10)).await;
        cache.fetch_shard(&summaries[2]).await.unwrap();

        assert!(!first.join("metadata.json").is_file(), "oldest evicted");
        // After eviction the cache holds at most the two newest shards
        // (the third one is the protected just-inserted entry). The
        // protected shard alone may push us over the cap.
        assert!(cache.total_bytes() > 0, "non-empty after evictions");
    }

    #[tokio::test]
    async fn fetch_shard_rejects_path_traversal_in_content_hashes() {
        let local_src = tempfile::tempdir().unwrap();
        write_local_shard(local_src.path(), vec![1, 2, 3]);

        let registry = StorageRegistry::from_url("memory:///").unwrap();
        let store = registry.store();
        upload_shard(&*store, "data/shard-000", local_src.path()).await;

        // Pull the real metadata back, then poison content_hashes with
        // a `..` segment and re-upload — simulating a malicious or
        // corrupted remote.
        let mut metadata: Metadata = serde_json::from_slice(
            &store
                .get(&Path::from("data/shard-000/metadata.json"))
                .await
                .unwrap()
                .bytes()
                .await
                .unwrap(),
        )
        .unwrap();
        metadata.content_hashes.insert(
            "../../../etc/passwd".to_string(),
            "sha256:deadbeef".to_string(),
        );
        store
            .put(
                &Path::from("data/shard-000/metadata.json"),
                PutPayload::from(serde_json::to_vec(&metadata).unwrap()),
            )
            .await
            .unwrap();

        let summary = ShardSummary {
            location: Path::from("data/shard-000"),
            metadata,
        };

        let cache_dir = tempfile::tempdir().unwrap();
        let cache = Cache::open(
            cache_dir.path().to_path_buf(),
            Arc::clone(&store),
            "memory:///".to_string(),
            10 * 1024 * 1024,
        )
        .unwrap();

        let err = cache
            .fetch_shard(&summary)
            .await
            .expect_err("path traversal must be rejected");
        let msg = format!("{err:#}");
        assert!(
            msg.contains("relative segment") || msg.contains("suspicious"),
            "{msg}"
        );
    }

    #[test]
    fn safe_join_rejects_traversal_and_absolute_paths() {
        let tmp = tempfile::tempdir().unwrap();
        assert!(safe_join(tmp.path(), "../etc/passwd").is_err());
        assert!(safe_join(tmp.path(), "/etc/passwd").is_err());
        assert!(safe_join(tmp.path(), "foo\\bar").is_err());
        assert!(safe_join(tmp.path(), "C:/x").is_err());
        assert!(safe_join(tmp.path(), "").is_err());
        assert!(safe_join(tmp.path(), "foo/./bar").is_err());
        assert!(safe_join(tmp.path(), "foo\0bar").is_err());
        assert!(safe_join(tmp.path(), "ok/nested").is_ok());
    }

    #[tokio::test]
    async fn stream_to_local_round_trips_large_payload() {
        let size = 6 * 1024 * 1024 + 17;
        let payload: Vec<u8> = (0..size)
            .map(|i| u8::try_from(i % 251).expect("< 251"))
            .collect();

        let registry = StorageRegistry::from_url("memory:///").expect("registry");
        let store = registry.store();
        let remote_path = Path::from("big.bin");
        store
            .put(&remote_path, PutPayload::from(payload.clone()))
            .await
            .expect("seed remote");

        let dest_dir = tempfile::tempdir().expect("dest tmpdir");
        let dest = dest_dir.path().join("big.bin");
        let get = store.get(&remote_path).await.expect("get");
        stream_to_local(&dest, get.into_stream())
            .await
            .expect("stream");

        let got = fs::read(&dest).expect("read local");
        assert_eq!(got.len(), payload.len());
        assert_eq!(got, payload);
    }

    #[tokio::test]
    async fn fetch_shard_streams_payload_with_large_metric_column() {
        // End-to-end: a shard whose metrics.arrow exceeds the
        // typical buffer scratch (~5 MiB) still round-trips through
        // the cache without ever materialising the whole file in
        // memory. Covers the OOM regression we're guarding against.
        let local_src = tempfile::tempdir().expect("tmpdir");
        // 700_000 i64 values = 5.6 MiB of metric data; the Arrow IPC
        // wrapping pushes the file just past 5 MiB.
        let values: Vec<i64> = (0..700_000_i64).collect();
        write_local_shard(local_src.path(), values);
        let metrics_size = fs::metadata(local_src.path().join("metrics.arrow"))
            .unwrap()
            .len();
        assert!(
            metrics_size > 5 * 1024 * 1024,
            "metrics.arrow {metrics_size}B should exceed 5 MiB for this regression check",
        );

        let registry = StorageRegistry::from_url("memory:///").unwrap();
        let store = registry.store();
        upload_shard(&*store, "data/shard-000", local_src.path()).await;

        let metadata: Metadata = serde_json::from_slice(
            &store
                .get(&Path::from("data/shard-000/metadata.json"))
                .await
                .unwrap()
                .bytes()
                .await
                .unwrap(),
        )
        .unwrap();
        let summary = ShardSummary {
            location: Path::from("data/shard-000"),
            metadata,
        };

        let cache_dir = tempfile::tempdir().unwrap();
        let cache = Cache::open(
            cache_dir.path().to_path_buf(),
            Arc::clone(&store),
            "memory:///".to_string(),
            100 * 1024 * 1024,
        )
        .unwrap();

        let local = cache.fetch_shard(&summary).await.expect("fetch");
        let cached_size = fs::metadata(local.join("metrics.arrow")).unwrap().len();
        assert_eq!(cached_size, metrics_size, "byte-for-byte parity");
    }
}
