//! Per-dataset manifest — the authoritative catalog of a dataset's shards.
//!
//! A dataset is a directory of shard subdirectories in object storage. Rather
//! than learn the shard set by walking the backend on every query (a
//! `store.list()` plus a GET of every shard's `metadata.json` — costly against
//! S3/GCS), each dataset carries one small `manifest.json` at its root listing
//! its shards and their metadata. A query resolves a single consistent
//! snapshot from one read, and the manifest's object etag is a cheap
//! invalidation signal (see [`crate::flight_sql`]'s summary cache).
//!
//! **Writer**: ingest is the only writer. After uploading a dataset's shards it
//! calls [`write_manifest`], which merges the new shards into any existing
//! manifest (by location, new wins), bumps the version, and PUTs the result.
//! A single object PUT is atomic, so a reader sees the old or new manifest,
//! never a partial one. v1 assumes one ingest at a time per dataset; the
//! atomic conditional-swap that concurrent writers need lands with the
//! re-ingest/replace work.
//!
//! **Reader**: the daemon never writes a manifest — it resolves the shard set
//! by reading one, and a dataset with no manifest has no shards.

use anyhow::Result;
use object_store::path::Path;
use object_store::{ObjectStore, PutPayload};
use serde::{Deserialize, Serialize};

use crate::shard::Metadata;
use crate::shard_source::ShardSummary;

/// Manifest object name at the dataset root.
pub const MANIFEST_FILE: &str = "manifest.json";

/// On-disk manifest format version, bumped only on an incompatible layout
/// change (distinct from the per-shard [`Metadata::format_version`]).
pub const MANIFEST_FORMAT_VERSION: u32 = 1;

/// One shard's entry in a manifest: its directory relative to the dataset
/// root (e.g. `day-2024-01-01`, or `""` for a single-shard dataset) and a
/// copy of the shard's `metadata.json`.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ManifestShard {
    pub location: String,
    pub metadata: Metadata,
}

/// A dataset's catalog snapshot. `version` increases by one on every write — the
/// atomic-commit counter the re-ingest / live-shard control-plane ops build on;
/// the daemon's read path polls the object's etag, not this field.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Manifest {
    pub format_version: u32,
    pub version: u64,
    pub shards: Vec<ManifestShard>,
}

impl Default for Manifest {
    /// An empty, never-written manifest — `version` 0 so the first
    /// [`write_manifest`] writes version 1.
    fn default() -> Self {
        Self {
            format_version: MANIFEST_FORMAT_VERSION,
            version: 0,
            shards: Vec::new(),
        }
    }
}

impl Manifest {
    /// Convert the manifest's entries to the [`ShardSummary`] currency the
    /// engine uses everywhere, validating each shard's metadata (a manifest
    /// authored elsewhere is untrusted input).
    pub fn into_summaries(self) -> Result<Vec<ShardSummary>> {
        self.shards
            .into_iter()
            .map(|s| {
                s.metadata.validate().map_err(|e| {
                    anyhow::anyhow!("invalid metadata for shard {}: {e}", s.location)
                })?;
                Ok(ShardSummary {
                    location: Path::from(s.location),
                    metadata: s.metadata,
                })
            })
            .collect()
    }
}

/// The manifest's path within a dataset-rooted store.
#[must_use]
pub fn manifest_path() -> Path {
    Path::from(MANIFEST_FILE)
}

/// Load a dataset's manifest from its store, or `None` if it has none. Rejects a
/// manifest written by a newer, incompatible build (`format_version` ahead of
/// ours) rather than misreading it — mirrors the per-shard [`Metadata`] check.
pub async fn load(store: &dyn ObjectStore) -> Result<Option<Manifest>> {
    match store.get(&manifest_path()).await {
        Ok(res) => {
            let bytes = res.bytes().await?;
            let manifest: Manifest = serde_json::from_slice(&bytes)?;
            if manifest.format_version > MANIFEST_FORMAT_VERSION {
                anyhow::bail!(
                    "manifest format version {} is newer than this build supports ({MANIFEST_FORMAT_VERSION})",
                    manifest.format_version,
                );
            }
            Ok(Some(manifest))
        }
        Err(object_store::Error::NotFound { .. }) => Ok(None),
        Err(e) => Err(e.into()),
    }
}

/// Merge `new_shards` into the dataset's manifest and write it back: the
/// shard set becomes (existing ∪ new) keyed by location with `new` winning,
/// the version is bumped, and the result is PUT atomically. Used by ingest
/// after a dataset's shards are in place.
pub async fn write_manifest(store: &dyn ObjectStore, new_shards: Vec<ManifestShard>) -> Result<()> {
    let existing = load(store).await?.unwrap_or_default();
    let manifest = Manifest {
        format_version: MANIFEST_FORMAT_VERSION,
        version: existing.version + 1,
        shards: merge(existing.shards, new_shards),
    };
    let bytes = serde_json::to_vec_pretty(&manifest)?;
    store.put(&manifest_path(), PutPayload::from(bytes)).await?;
    Ok(())
}

/// Combine two shard lists by location — `new` overwrites `existing` for a
/// shared location — and return them sorted by location for a deterministic
/// manifest.
fn merge(existing: Vec<ManifestShard>, new: Vec<ManifestShard>) -> Vec<ManifestShard> {
    let mut by_location: std::collections::BTreeMap<String, ManifestShard> = existing
        .into_iter()
        .map(|s| (s.location.clone(), s))
        .collect();
    for s in new {
        by_location.insert(s.location.clone(), s);
    }
    by_location.into_values().collect()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::shard::{FieldKind, FieldSchema};
    use crate::storage::StorageRegistry;

    fn metadata(num_docs: u64) -> Metadata {
        Metadata {
            format_version: crate::shard::FORMAT_VERSION,
            num_docs,
            time_range_start: 0,
            time_range_end: 100,
            fields: vec![FieldSchema {
                name: "x".into(),
                kind: FieldKind::Metric,
                scale: 0,
            }],
            content_hashes: std::collections::BTreeMap::default(),
            time_field: None,
        }
    }

    fn shard(location: &str, num_docs: u64) -> ManifestShard {
        ManifestShard {
            location: location.to_string(),
            metadata: metadata(num_docs),
        }
    }

    #[tokio::test]
    async fn load_returns_none_when_absent() {
        let store = StorageRegistry::from_url("memory:///").unwrap().store();
        assert!(load(&*store).await.unwrap().is_none());
    }

    #[tokio::test]
    async fn write_then_load_round_trips() {
        let store = StorageRegistry::from_url("memory:///").unwrap().store();
        write_manifest(&*store, vec![shard("s0", 3)]).await.unwrap();

        let m = load(&*store).await.unwrap().expect("manifest present");
        assert_eq!(m.version, 1);
        assert_eq!(m.shards.len(), 1);
        assert_eq!(m.shards[0].location, "s0");

        let summaries = m.into_summaries().unwrap();
        assert_eq!(summaries.len(), 1);
        assert_eq!(summaries[0].location.as_ref(), "s0");
        assert_eq!(summaries[0].metadata.num_docs, 3);
    }

    #[tokio::test]
    async fn load_rejects_newer_format_version() {
        let store = StorageRegistry::from_url("memory:///").unwrap().store();
        let manifest = Manifest {
            format_version: MANIFEST_FORMAT_VERSION + 1,
            version: 1,
            shards: vec![shard("s0", 1)],
        };
        let bytes = serde_json::to_vec(&manifest).unwrap();
        store
            .put(&manifest_path(), bytes.into())
            .await
            .expect("put manifest");

        let err = load(&*store)
            .await
            .expect_err("newer format must be rejected");
        assert!(err.to_string().contains("newer than this build"), "{err}");
    }

    #[tokio::test]
    async fn write_merges_and_bumps_version() {
        let store = StorageRegistry::from_url("memory:///").unwrap().store();
        // First ingest: two shards.
        write_manifest(&*store, vec![shard("s0", 1), shard("s1", 2)])
            .await
            .unwrap();
        // Second ingest: a new shard plus an overwrite of s0.
        write_manifest(&*store, vec![shard("s0", 9), shard("s2", 3)])
            .await
            .unwrap();

        let m = load(&*store).await.unwrap().unwrap();
        assert_eq!(m.version, 2);
        let by_loc: std::collections::BTreeMap<_, _> = m
            .shards
            .iter()
            .map(|s| (s.location.as_str(), s.metadata.num_docs))
            .collect();
        assert_eq!(by_loc.len(), 3, "s0, s1, s2");
        assert_eq!(by_loc["s0"], 9, "new shard wins on conflict");
        assert_eq!(by_loc["s1"], 2);
        assert_eq!(by_loc["s2"], 3);
        // Sorted by location for determinism.
        let locs: Vec<_> = m.shards.iter().map(|s| s.location.as_str()).collect();
        assert_eq!(locs, vec!["s0", "s1", "s2"]);
    }
}
