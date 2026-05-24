//! Object storage backend abstraction.
//!
//! Wraps the `object_store` crate behind a [`StorageRegistry`] that holds
//! the daemon's configured backend.
//!
//! Supported URL schemes (via `object_store::parse_url`):
//!
//! | Scheme | Backend |
//! |---|---|
//! | `file://` | local filesystem |
//! | `memory://` | in-memory (tests) |
//! | `s3://` | AWS S3 / S3-compatible (MinIO, R2, B2, Ceph, …) |
//! | `gs://` | Google Cloud Storage |
//! | `az://`, `abfs://`, `abfss://` | Azure Blob Storage |
//! | `http(s)://` | read-only HTTP source |

use std::sync::Arc;

use object_store::ObjectStore;
use object_store::path::Path;
use object_store::prefix::PrefixStore;
use url::Url;

/// Owns the configured object-storage backend.
pub struct StorageRegistry {
    store: Arc<dyn ObjectStore>,
}

impl StorageRegistry {
    /// Build a registry from a URL such as `s3://bucket/prefix` or
    /// `file:///var/data`. A path component in the URL is folded into a
    /// `PrefixStore` so all subsequent operations are scoped under it.
    pub fn from_url(url: &str) -> anyhow::Result<Self> {
        let parsed =
            Url::parse(url).map_err(|e| anyhow::anyhow!("invalid storage URL {url:?}: {e}"))?;
        let (store, path) = object_store::parse_url(&parsed)
            .map_err(|e| anyhow::anyhow!("unsupported storage URL {url:?}: {e}"))?;

        let store: Arc<dyn ObjectStore> = if path.parts().next().is_some() {
            Arc::new(PrefixStore::new(store, path))
        } else {
            Arc::from(store)
        };

        Ok(Self { store })
    }

    #[must_use]
    pub fn store(&self) -> Arc<dyn ObjectStore> {
        Arc::clone(&self.store)
    }
}

/// One-shot connectivity check: list objects under `prefix` (or the
/// backend root if `None`), printing a small summary. Returns the number
/// of objects observed.
pub async fn check(store: &dyn ObjectStore, prefix: Option<&str>) -> anyhow::Result<usize> {
    use futures::StreamExt;

    let prefix_path = prefix.map(Path::from);
    let mut stream = store.list(prefix_path.as_ref());

    let mut count = 0usize;
    let mut total_bytes: usize = 0;
    while let Some(meta) = stream.next().await {
        let meta = meta?;
        count += 1;
        total_bytes += meta.size;
        if count <= 10 {
            println!("{}\t{} bytes", meta.location, meta.size);
        }
    }
    if count > 10 {
        println!("… ({} more objects not shown)", count - 10);
    }
    println!("---");
    let object_word = if count == 1 { "object" } else { "objects" };
    println!("{count} {object_word}, {total_bytes} bytes total");
    Ok(count)
}

#[cfg(test)]
mod tests {
    use super::*;
    use object_store::PutPayload;

    async fn put(store: &dyn ObjectStore, path: &str, body: &[u8]) {
        store
            .put(
                &Path::from(path),
                PutPayload::from_bytes(body.to_vec().into()),
            )
            .await
            .expect("put");
    }

    #[tokio::test]
    async fn from_url_memory_roundtrip() {
        let registry = StorageRegistry::from_url("memory:///").expect("registry");
        let store = registry.store();
        put(&*store, "hello.txt", b"hi").await;

        let got = store.get(&Path::from("hello.txt")).await.expect("get");
        let bytes = got.bytes().await.expect("bytes");
        assert_eq!(&bytes[..], b"hi");
    }

    #[tokio::test]
    async fn from_url_file_roundtrip() {
        let tmp = tempfile::tempdir().expect("tmpdir");
        let url = format!("file://{}", tmp.path().display());
        let registry = StorageRegistry::from_url(&url).expect("registry");
        let store = registry.store();
        put(&*store, "hello.txt", b"hi").await;

        let got = store.get(&Path::from("hello.txt")).await.expect("get");
        let bytes = got.bytes().await.expect("bytes");
        assert_eq!(&bytes[..], b"hi");
    }

    #[tokio::test]
    async fn from_url_with_path_scopes_operations() {
        use futures::StreamExt;

        // `memory://` URLs treat the host+path as a prefix. After PrefixStore
        // wrapping, callers see paths relative to that prefix.
        let registry = StorageRegistry::from_url("memory:///pre/fix").expect("registry");
        let store = registry.store();
        put(&*store, "file.txt", b"x").await;

        let mut stream = store.list(None);
        let mut seen = Vec::new();
        while let Some(meta) = stream.next().await {
            seen.push(meta.expect("meta").location.to_string());
        }
        assert_eq!(seen, vec!["file.txt".to_string()]);
    }

    #[test]
    fn from_url_rejects_garbage() {
        // `.err().expect(...)` avoids needing `Debug` on `StorageRegistry`.
        let err = StorageRegistry::from_url("not-a-url")
            .err()
            .expect("expected error for invalid URL");
        let msg = err.to_string();
        assert!(
            msg.contains("not-a-url"),
            "error should mention bad URL: {msg}"
        );
    }

    #[tokio::test]
    async fn check_empty_backend_returns_zero() {
        let registry = StorageRegistry::from_url("memory:///").expect("registry");
        let count = check(&*registry.store(), None).await.expect("check");
        assert_eq!(count, 0);
    }

    #[tokio::test]
    async fn check_counts_objects_and_filters_by_prefix() {
        let registry = StorageRegistry::from_url("memory:///").expect("registry");
        let store = registry.store();
        put(&*store, "top.txt", b"a").await;
        put(&*store, "sub/a.txt", b"b").await;
        put(&*store, "sub/b.txt", b"c").await;

        let all = check(&*store, None).await.expect("check");
        assert_eq!(all, 3);

        let only_sub = check(&*store, Some("sub")).await.expect("check");
        assert_eq!(only_sub, 2);
    }
}
