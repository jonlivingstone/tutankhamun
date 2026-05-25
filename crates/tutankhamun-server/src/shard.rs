//! On-disk shard format.
//!
//! A shard is a directory containing:
//!
//! - `metadata.json` — schema + shard stats (numDocs, time range,
//!   format version).
//! - `metrics.arrow` — single uncompressed Arrow IPC record batch, one
//!   `Int64Array` column per metric field. The forward-column hot path
//!   (FTGS) reads metric values from this file.
//!
//! Inverted-index files (`postings/<field>.fst` + `<field>.posting`)
//! are a separate layer not implemented here.
//!
//! # The "one batch, uncompressed" invariant
//!
//! [`DiskShardWriter`] always emits a single record batch with no
//! compression. [`DiskShard::open`] rejects files that violate this
//! invariant. Cold-tier compressed shards are a future addition that
//! goes through a different reader.

use std::fs;
use std::io;
use std::path::{Path, PathBuf};
use std::sync::Arc;

use anyhow::{Context, Result, bail};
use arrow::array::{Array, ArrayRef, Int64Array, RecordBatch};
use arrow::datatypes::{DataType, Field, Schema};
use arrow::ipc::reader::FileReader;
use arrow::ipc::writer::FileWriter;
use chrono::DateTime;
use serde::{Deserialize, Serialize};

const METADATA_FILE: &str = "metadata.json";
const METRICS_FILE: &str = "metrics.arrow";
const METADATA_TMP: &str = ".metadata.json.tmp";
const METRICS_TMP: &str = ".metrics.arrow.tmp";

/// Shard format version this build writes and refuses to read anything
/// other than.
pub(crate) const FORMAT_VERSION: u32 = 1;

/// Shard-level metadata persisted as `metadata.json`.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Metadata {
    pub format_version: u32,
    pub num_docs: u64,
    pub time_range_start: i64,
    pub time_range_end: i64,
    pub fields: Vec<FieldSchema>,
}

impl Metadata {
    /// Reject corrupt metadata. Currently checks that the time range
    /// satisfies `start <= end`; a degenerate range silently breaks
    /// time-range pruning later on, so we'd rather fail fast on read.
    pub fn validate(&self) -> Result<()> {
        if self.time_range_start > self.time_range_end {
            bail!(
                "invalid time range: start ({}) > end ({})",
                self.time_range_start,
                self.time_range_end
            );
        }
        Ok(())
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct FieldSchema {
    pub name: String,
    pub kind: FieldKind,
}

/// Field type within a shard. Only `Metric` (an `int64` forward column)
/// is implemented; `Int` and `String` (which require the inverted-index
/// layer) come later.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum FieldKind {
    Metric,
}

/// Read-only access to a shard's contents.
pub trait Shard: Send + Sync {
    fn metadata(&self) -> &Metadata;

    fn num_docs(&self) -> u64 {
        self.metadata().num_docs
    }

    fn time_range(&self) -> (i64, i64) {
        (
            self.metadata().time_range_start,
            self.metadata().time_range_end,
        )
    }

    /// Borrow a metric column by field name, or `None` if no such
    /// column exists.
    fn forward_column(&self, name: &str) -> Option<&[i64]>;
}

/// A shard backed by files on local disk.
///
/// `metrics.arrow` is read into Arrow's heap buffers at open time; the
/// hot-path `&[i64]` slice returned by [`Shard::forward_column`] is a
/// borrow into those buffers. True mmap-backed zero-copy reads are a
/// later optimisation; the trait signature does not foreclose it.
pub struct DiskShard {
    metadata: Metadata,
    batch: RecordBatch,
}

impl DiskShard {
    /// `metrics.arrow` is read into Arrow's heap buffers in full here.
    /// True mmap-backed zero-copy is a later optimisation; until then, a
    /// 1 GB shard means ~1 GB of resident heap per open.
    pub fn open(dir: &Path) -> Result<Self> {
        let metadata_path = dir.join(METADATA_FILE);
        let metrics_path = dir.join(METRICS_FILE);

        let metadata_bytes = fs::read(&metadata_path)
            .with_context(|| format!("read {}", metadata_path.display()))?;
        let metadata: Metadata = serde_json::from_slice(&metadata_bytes)
            .with_context(|| format!("parse {}", metadata_path.display()))?;
        metadata
            .validate()
            .with_context(|| format!("validate {}", metadata_path.display()))?;

        if metadata.format_version != FORMAT_VERSION {
            bail!(
                "{}: unsupported shard format_version {} (this build writes/reads {})",
                metadata_path.display(),
                metadata.format_version,
                FORMAT_VERSION,
            );
        }

        let file = fs::File::open(&metrics_path)
            .with_context(|| format!("open {}", metrics_path.display()))?;
        let mut reader = FileReader::try_new(file, None)
            .with_context(|| format!("parse Arrow IPC {}", metrics_path.display()))?;

        let batch = reader
            .next()
            .ok_or_else(|| anyhow::anyhow!("{}: no record batches", metrics_path.display()))?
            .with_context(|| format!("read record batch from {}", metrics_path.display()))?;

        if reader.next().is_some() {
            bail!(
                "{}: forward-column invariant violated: expected exactly one record batch",
                metrics_path.display(),
            );
        }

        validate_schema_matches(&metadata, &batch, &metrics_path)?;
        if batch.num_rows() as u64 != metadata.num_docs {
            bail!(
                "{}: row count {} does not match metadata num_docs {}",
                metrics_path.display(),
                batch.num_rows(),
                metadata.num_docs,
            );
        }

        Ok(Self { metadata, batch })
    }
}

impl Shard for DiskShard {
    fn metadata(&self) -> &Metadata {
        &self.metadata
    }

    fn forward_column(&self, name: &str) -> Option<&[i64]> {
        let index = self.batch.schema_ref().index_of(name).ok()?;
        let array = self.batch.column(index);
        let int64 = array.as_any().downcast_ref::<Int64Array>()?;
        Some(int64.values())
    }
}

fn validate_schema_matches(
    metadata: &Metadata,
    batch: &RecordBatch,
    metrics_path: &Path,
) -> Result<()> {
    let schema = batch.schema_ref();
    if schema.fields().len() != metadata.fields.len() {
        bail!(
            "{}: schema field count {} does not match metadata {}",
            metrics_path.display(),
            schema.fields().len(),
            metadata.fields.len(),
        );
    }
    for (decl, batch_field) in metadata.fields.iter().zip(schema.fields()) {
        if decl.name.as_str() != batch_field.name().as_str() {
            bail!(
                "{}: schema field name mismatch: metadata says {:?}, batch says {:?}",
                metrics_path.display(),
                decl.name,
                batch_field.name(),
            );
        }
        match decl.kind {
            FieldKind::Metric => {
                if batch_field.data_type() != &DataType::Int64 {
                    bail!(
                        "{}: metric field {:?} must be Int64, got {:?}",
                        metrics_path.display(),
                        decl.name,
                        batch_field.data_type(),
                    );
                }
            }
        }
    }
    Ok(())
}

/// Build a shard directory on disk.
///
/// All columns must have the same length. `finalize` writes
/// `metrics.arrow` and `metadata.json` to temp files and atomically
/// renames them into place so a crashed run leaves no partially-written
/// shard visible to readers.
pub struct DiskShardWriter {
    dir: PathBuf,
    time_range: (i64, i64),
    columns: Vec<(String, Vec<i64>)>,
    num_docs: Option<u64>,
}

impl DiskShardWriter {
    pub fn new(dir: &Path, time_range: (i64, i64)) -> Result<Self> {
        fs::create_dir_all(dir).with_context(|| format!("create shard dir {}", dir.display()))?;
        Ok(Self {
            dir: dir.to_path_buf(),
            time_range,
            columns: Vec::new(),
            num_docs: None,
        })
    }

    /// Add a metric (`Int64`) column. All columns added to one writer
    /// must have the same length; the first call fixes the row count
    /// and subsequent mismatches return an error.
    ///
    /// `values` is owned so [`Int64Array::from`] can take it without a
    /// copy on `finalize`.
    pub fn add_metric(&mut self, name: &str, values: Vec<i64>) -> Result<()> {
        let len = values.len() as u64;
        match self.num_docs {
            None => self.num_docs = Some(len),
            Some(existing) if existing != len => {
                bail!("column {name:?} has {len} rows but previous columns have {existing}")
            }
            Some(_) => {}
        }
        if self.columns.iter().any(|(n, _)| n == name) {
            bail!("column {name:?} already added");
        }
        self.columns.push((name.to_string(), values));
        Ok(())
    }

    pub fn finalize(self) -> Result<()> {
        let num_docs = self.num_docs.unwrap_or(0);

        let metadata = Metadata {
            format_version: FORMAT_VERSION,
            num_docs,
            time_range_start: self.time_range.0,
            time_range_end: self.time_range.1,
            fields: self
                .columns
                .iter()
                .map(|(name, _)| FieldSchema {
                    name: name.clone(),
                    kind: FieldKind::Metric,
                })
                .collect(),
        };

        let fields: Vec<Field> = self
            .columns
            .iter()
            .map(|(name, _)| Field::new(name, DataType::Int64, false))
            .collect();
        let schema = Arc::new(Schema::new(fields));

        let arrays: Vec<ArrayRef> = self
            .columns
            .into_iter()
            .map(|(_, values)| Arc::new(Int64Array::from(values)) as ArrayRef)
            .collect();

        let batch =
            RecordBatch::try_new(Arc::clone(&schema), arrays).context("build record batch")?;

        let metrics_tmp = self.dir.join(METRICS_TMP);
        let metadata_tmp = self.dir.join(METADATA_TMP);

        {
            let file = fs::File::create(&metrics_tmp)
                .with_context(|| format!("create {}", metrics_tmp.display()))?;
            let mut writer = FileWriter::try_new(file, &schema)
                .with_context(|| format!("init Arrow IPC writer for {}", metrics_tmp.display()))?;
            writer
                .write(&batch)
                .with_context(|| format!("write record batch to {}", metrics_tmp.display()))?;
            writer
                .finish()
                .with_context(|| format!("finalise {}", metrics_tmp.display()))?;
        }

        let metadata_json = serde_json::to_vec_pretty(&metadata).context("serialise metadata")?;
        fs::write(&metadata_tmp, &metadata_json)
            .with_context(|| format!("write {}", metadata_tmp.display()))?;

        // Rename metrics first so a reader that sees metadata.json is
        // guaranteed to also see metrics.arrow.
        fs::rename(&metrics_tmp, self.dir.join(METRICS_FILE))
            .with_context(|| format!("rename {} into place", metrics_tmp.display()))?;
        fs::rename(&metadata_tmp, self.dir.join(METADATA_FILE))
            .with_context(|| format!("rename {} into place", metadata_tmp.display()))?;

        Ok(())
    }
}

/// Open the shard at `path` and write a human-readable summary
/// (metadata, schema, per-field types) to `out`. Used by the
/// `t9n shard inspect` CLI subcommand.
pub fn inspect(path: &Path, out: &mut dyn io::Write) -> Result<()> {
    let shard = DiskShard::open(path)?;
    let metadata = shard.metadata();

    let (start, end) = shard.time_range();
    let start_iso = format_timestamp(start);
    let end_iso = format_timestamp(end);

    let widest_name = metadata
        .fields
        .iter()
        .map(|f| f.name.len())
        .max()
        .unwrap_or(0);

    writeln!(out, "path:            {}", path.display())?;
    writeln!(out, "format version:  {}", metadata.format_version)?;
    writeln!(out, "num docs:        {}", metadata.num_docs)?;
    writeln!(out, "time range:      {start} .. {end}")?;
    writeln!(out, "                 ({start_iso} .. {end_iso})")?;
    writeln!(out, "schema:")?;
    for field in &metadata.fields {
        let (kind, dtype) = match field.kind {
            FieldKind::Metric => ("metric", "int64"),
        };
        writeln!(
            out,
            "  {:<width$}  {}  {}",
            field.name,
            kind,
            dtype,
            width = widest_name
        )?;
    }

    Ok(())
}

fn format_timestamp(secs: i64) -> String {
    DateTime::from_timestamp(secs, 0)
        .map_or_else(|| "(out of range)".to_string(), |t| t.to_rfc3339())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn write_shard(dir: &Path, time_range: (i64, i64), columns: Vec<(&str, Vec<i64>)>) {
        let mut w = DiskShardWriter::new(dir, time_range).expect("new writer");
        for (name, values) in columns {
            w.add_metric(name, values).expect("add_metric");
        }
        w.finalize().expect("finalize");
    }

    #[test]
    fn metadata_roundtrips_through_serde_json() {
        let m = Metadata {
            format_version: FORMAT_VERSION,
            num_docs: 100,
            time_range_start: 1_700_000_000,
            time_range_end: 1_700_003_600,
            fields: vec![
                FieldSchema {
                    name: "a".into(),
                    kind: FieldKind::Metric,
                },
                FieldSchema {
                    name: "b".into(),
                    kind: FieldKind::Metric,
                },
            ],
        };
        let bytes = serde_json::to_vec(&m).unwrap();
        let back: Metadata = serde_json::from_slice(&bytes).unwrap();
        assert_eq!(m, back);
    }

    #[test]
    fn disk_shard_roundtrip_minimal() {
        let tmp = tempfile::tempdir().expect("tmpdir");
        let values: Vec<i64> = (0..100).collect();
        write_shard(
            tmp.path(),
            (1_700_000_000, 1_700_003_600),
            vec![("x", values.clone())],
        );

        let shard = DiskShard::open(tmp.path()).expect("open");
        assert_eq!(shard.num_docs(), 100);
        assert_eq!(shard.time_range(), (1_700_000_000, 1_700_003_600));
        assert_eq!(shard.forward_column("x").unwrap(), values.as_slice());
    }

    #[test]
    fn disk_shard_roundtrip_multi_column() {
        let tmp = tempfile::tempdir().expect("tmpdir");
        let ascending: Vec<i64> = (0..50).collect();
        let descending: Vec<i64> = (0..50).rev().collect();
        let mixed: Vec<i64> = (0..50).map(|i| if i % 2 == 0 { i } else { -i }).collect();

        write_shard(
            tmp.path(),
            (1, 2),
            vec![
                ("asc", ascending.clone()),
                ("desc", descending.clone()),
                ("mixed", mixed.clone()),
            ],
        );

        let shard = DiskShard::open(tmp.path()).expect("open");
        assert_eq!(shard.num_docs(), 50);
        assert_eq!(shard.forward_column("asc").unwrap(), ascending.as_slice());
        assert_eq!(shard.forward_column("desc").unwrap(), descending.as_slice());
        assert_eq!(shard.forward_column("mixed").unwrap(), mixed.as_slice());
    }

    #[test]
    fn disk_shard_rejects_missing_metadata() {
        let tmp = tempfile::tempdir().expect("tmpdir");
        let err = DiskShard::open(tmp.path())
            .err()
            .expect("expected error opening empty dir");
        let msg = err.to_string();
        assert!(msg.contains("metadata.json"), "got: {msg}");
    }

    #[test]
    fn disk_shard_rejects_schema_mismatch() {
        let tmp = tempfile::tempdir().expect("tmpdir");
        write_shard(tmp.path(), (0, 0), vec![("real", vec![1, 2, 3])]);

        // Replace metadata.json with a doctored copy that claims a different
        // field name. The Arrow IPC file is unchanged, so the schema check
        // should reject the inconsistency.
        let metadata_path = tmp.path().join(METADATA_FILE);
        let mut metadata: Metadata =
            serde_json::from_slice(&fs::read(&metadata_path).unwrap()).unwrap();
        metadata.fields[0].name = "ghost".into();
        fs::write(&metadata_path, serde_json::to_vec(&metadata).unwrap()).unwrap();

        let err = DiskShard::open(tmp.path())
            .err()
            .expect("expected schema-mismatch error");
        let msg = err.to_string();
        assert!(
            msg.contains("field name mismatch") || msg.contains("ghost"),
            "got: {msg}"
        );
    }

    #[test]
    fn disk_shard_rejects_degenerate_time_range() {
        let tmp = tempfile::tempdir().expect("tmpdir");
        write_shard(tmp.path(), (0, 0), vec![("x", vec![1, 2, 3])]);

        // Doctor metadata.json to invert the time range.
        let metadata_path = tmp.path().join(METADATA_FILE);
        let mut metadata: Metadata =
            serde_json::from_slice(&fs::read(&metadata_path).unwrap()).unwrap();
        metadata.time_range_start = 200;
        metadata.time_range_end = 100;
        fs::write(&metadata_path, serde_json::to_vec(&metadata).unwrap()).unwrap();

        let err = DiskShard::open(tmp.path())
            .err()
            .expect("expected degenerate-range error");
        let msg = err.to_string();
        assert!(
            msg.contains("validate") || msg.to_lowercase().contains("time range"),
            "got: {msg}"
        );
    }

    #[test]
    fn disk_shard_unknown_column_is_none() {
        let tmp = tempfile::tempdir().expect("tmpdir");
        write_shard(tmp.path(), (0, 0), vec![("x", vec![1, 2, 3])]);

        let shard = DiskShard::open(tmp.path()).expect("open");
        assert!(shard.forward_column("nope").is_none());
    }

    #[test]
    fn inspect_emits_path_metadata_and_schema() {
        let tmp = tempfile::tempdir().expect("tmpdir");
        // Pick a recognisable time so the ISO line is easy to assert on.
        write_shard(
            tmp.path(),
            (1_700_000_000, 1_700_003_600),
            vec![
                ("alpha", vec![1, 2, 3]),
                ("beta", vec![4, 5, 6]),
                ("gamma_long_name", vec![7, 8, 9]),
            ],
        );

        let mut buf = Vec::new();
        inspect(tmp.path(), &mut buf).expect("inspect");
        let out = String::from_utf8(buf).expect("utf-8");

        assert!(out.contains(&tmp.path().display().to_string()), "{out}");
        assert!(out.contains("format version:  1"), "{out}");
        assert!(out.contains("num docs:        3"), "{out}");
        assert!(out.contains("1700000000 .. 1700003600"), "{out}");
        assert!(out.contains("2023-11-14T22:13:20"), "{out}");
        assert!(out.contains("alpha"), "{out}");
        assert!(out.contains("beta"), "{out}");
        assert!(out.contains("gamma_long_name"), "{out}");
        assert!(out.contains("metric"), "{out}");
        assert!(out.contains("int64"), "{out}");
    }
}
