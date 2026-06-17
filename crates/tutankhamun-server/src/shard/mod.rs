//! On-disk shard format.
//!
//! A shard is a directory containing:
//!
//! - `metadata.json` — schema + shard stats (numDocs, time range,
//!   format version).
//! - `metrics.arrow` — single uncompressed Arrow IPC record batch, one
//!   `Int64Array` column per metric field. The forward-column hot path
//!   (FTGS) reads metric values from this file.
//! - `postings/<field>.fst` + `postings/<field>.posting` — one pair per
//!   string field. The FST is a sorted term dictionary mapping each
//!   term to a byte offset into the posting file. The posting file is
//!   a concatenation of serialized roaring bitmaps — the wire format
//!   is self-bounded, so no length prefix is needed.
//!
//! # The "one batch, uncompressed" invariant (forward columns)
//!
//! [`DiskShardWriter`] always emits a single record batch with no
//! compression. [`DiskShard::open`] rejects files that violate this
//! invariant, which is what keeps forward-column reads mmap-friendly
//! and zero-copy.
//!
//! # The "files are immutable post-rename" invariant (inverted index)
//!
//! Postings files are mmap'd. Callers must not mutate any file inside
//! a shard directory after it has been finalized; doing so undefines
//! the behavior of any concurrently-open [`DiskShard`].

use std::collections::HashMap;
use std::fs;
use std::io;
use std::path::{Path, PathBuf};
use std::ptr::NonNull;
use std::sync::Arc;

use anyhow::{Context, Result, bail};
use arrow::array::{Array, Int64Array, RecordBatch};
use arrow::buffer::Buffer;
use arrow::datatypes::DataType;
use chrono::DateTime;
use fst::{IntoStreamer, Streamer};
use memmap2::Mmap;
use roaring::RoaringBitmap;
use serde::{Deserialize, Serialize};

mod filter;
mod writer;

use filter::combine_filters;
pub use filter::{FilterClause, FilterOp, FilterResult, matched_doc_set};
pub use writer::DiskShardWriter;

pub(crate) const METADATA_FILE: &str = "metadata.json";
pub(crate) const METRICS_FILE: &str = "metrics.arrow";
const POSTINGS_DIR: &str = "postings";
const POSTING_EXT: &str = "posting";
const FST_EXT: &str = "fst";

/// Shard format version this build writes. Readers accept this version
/// and the previous one (v1 had no `content_hashes` map; it loads
/// without hash validation).
pub(crate) const FORMAT_VERSION: u32 = 2;
const MIN_SUPPORTED_FORMAT_VERSION: u32 = 1;

/// Shard-level metadata persisted as `metadata.json`.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Metadata {
    pub format_version: u32,
    pub num_docs: u64,
    pub time_range_start: i64,
    pub time_range_end: i64,
    pub fields: Vec<FieldSchema>,
    /// Per-file SHA-256 of every payload file in the shard, keyed by
    /// the file's path relative to the shard directory (e.g.
    /// `metrics.arrow`, `postings/country.fst`). Values are
    /// `"sha256:<hex>"`. Empty (and skipped during validation) for
    /// shards written with `format_version` 1.
    #[serde(default)]
    pub content_hashes: std::collections::BTreeMap<String, String>,
    /// Name of the field (an `Int` field in `fields`) that holds each
    /// doc's time as epoch seconds — the `--time` column at ingest.
    /// `None` for shards written before per-doc time was stored; those
    /// still carry the shard-level `time_range_*` but expose no
    /// per-row time column. The SQL layer presents this field as an
    /// Arrow `Timestamp`.
    #[serde(default)]
    pub time_field: Option<String>,
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
    /// Decimal scale: the stored `i64` is the value × 10^`scale` (so a
    /// `Decimal(p,2)` or a float ingested at scale 2 stores cents). 0 for plain
    /// integers and strings. Recorded so a future read path can present the
    /// scaled decimal; today aggregates return the raw integer. Optional in the
    /// JSON (omitted when 0) so existing shards deserialize unchanged.
    #[serde(default, skip_serializing_if = "is_zero_scale")]
    pub scale: i8,
}

// Signature dictated by serde's `skip_serializing_if` (takes `&T`).
#[allow(clippy::trivially_copy_pass_by_ref)]
fn is_zero_scale(scale: &i8) -> bool {
    *scale == 0
}

/// Field type within a shard.
///
/// - `Metric` — int64 forward column only. Aggregatable, not filterable.
/// - `String` — inverted index over UTF-8 terms only. Filterable, not aggregatable.
/// - `Int` — int64 forward column AND inverted index over the numeric values.
///   Both filterable and aggregatable. Index keys are stored as
///   order-preserving big-endian i64 (sign bit flipped), so bytewise lex
///   sort on the FST equals numeric sort.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum FieldKind {
    Metric,
    String,
    Int,
}

impl std::fmt::Display for FieldKind {
    /// Lowercase form matching the JSON serialisation and the labels
    /// used in `inspect` output / CLI errors.
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(match self {
            FieldKind::Metric => "metric",
            FieldKind::String => "string",
            FieldKind::Int => "int",
        })
    }
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

    /// Borrow the inverted index for a string field, or `None` if no
    /// such field exists. Default implementation returns `None` so
    /// shard impls that have no index layer (e.g. test fakes) need no
    /// extra code.
    fn inverted_index(&self, _name: &str) -> Option<&InvertedIndex> {
        None
    }
}

/// Inverted index for one string field — a sorted term dictionary
/// (FST) plus the concatenated bitmaps it points into.
///
/// Both halves are mmap'd, so lookups touch only the pages they read;
/// the per-shard resident footprint at open time is roughly the size
/// of the FST's in-memory index, not the postings file.
pub struct InvertedIndex {
    fst: fst::Map<Mmap>,
    postings: Mmap,
}

impl InvertedIndex {
    /// Number of terms in the dictionary. Cheap — the FST stores it.
    #[must_use]
    pub fn num_terms(&self) -> u64 {
        self.fst.len() as u64
    }

    /// Doc set for `term`, or `None` if the term is not in the
    /// dictionary. Thin wrapper around [`lookup_bytes`] for the
    /// common UTF-8 case (string fields).
    #[must_use]
    pub fn lookup(&self, term: &str) -> Option<RoaringBitmap> {
        self.lookup_bytes(term.as_bytes())
    }

    /// Doc set for the raw byte key, or `None`. Used by callers
    /// whose terms aren't UTF-8 strings — e.g. `Int` fields encode
    /// values via [`encode_int_key`] and look up with the resulting
    /// 8-byte slice.
    #[must_use]
    pub fn lookup_bytes(&self, key: &[u8]) -> Option<RoaringBitmap> {
        let offset = self.fst.get(key)?;
        Some(read_bitmap_at(&self.postings, offset))
    }

    /// All terms in `[start, end_inclusive]`, in lexicographic order,
    /// each paired with its doc set. UTF-8 wrapper for string fields;
    /// for byte-keyed indexes (Int) use [`range_bytes`].
    pub fn range(
        &self,
        start: &str,
        end_inclusive: &str,
    ) -> impl Iterator<Item = (String, RoaringBitmap)> + '_ {
        self.range_bytes(Some(start.as_bytes()), Some(end_inclusive.as_bytes()))
            .map(|(k, bm)| (utf8_term(&k), bm))
    }

    /// All byte-keyed terms in `[lo, hi_inclusive]`, in lexicographic
    /// byte order, each paired with its doc set. Either bound may be
    /// `None` for an open range (`None, Some(hi)` = "everything up to
    /// hi", `Some(lo), None` = "everything from lo onwards"). For
    /// `Int` fields whose FST keys are [`encode_int_key`]-encoded,
    /// byte order = numeric order, so this gives natural numeric
    /// range semantics.
    pub fn range_bytes(
        &self,
        lo: Option<&[u8]>,
        hi_inclusive: Option<&[u8]>,
    ) -> impl Iterator<Item = (Vec<u8>, RoaringBitmap)> + '_ {
        let mut range = self.fst.range();
        if let Some(lo) = lo {
            range = range.ge(lo);
        }
        if let Some(hi) = hi_inclusive {
            range = range.le(hi);
        }
        let mut stream = range.into_stream();
        let postings: &[u8] = &self.postings;
        std::iter::from_fn(move || {
            let (k, v) = stream.next()?;
            Some((k.to_vec(), read_bitmap_at(postings, v)))
        })
    }

    /// All terms in lexicographic order, without their bitmaps.
    pub fn terms(&self) -> impl Iterator<Item = String> + '_ {
        let mut stream = self.fst.stream();
        std::iter::from_fn(move || {
            let (k, _) = stream.next()?;
            Some(utf8_term(k))
        })
    }
}

/// FST stores keys as raw bytes; we only ever insert valid UTF-8 (the
/// `add_string_field` API takes `String` keys), so an invalid sequence
/// here means the shard file is corrupt.
pub(crate) fn utf8_term(bytes: &[u8]) -> String {
    std::str::from_utf8(bytes)
        .expect("FST term is not valid UTF-8 — shard file corrupt")
        .to_owned()
}

/// Order-preserving big-endian i64 encoding for FST keys on `Int`
/// fields. Flipping the sign bit makes bytewise lex order equal
/// numeric order: `encode(-1) < encode(0) < encode(1)`.
#[must_use]
pub fn encode_int_key(v: i64) -> [u8; 8] {
    (v ^ i64::MIN).to_be_bytes()
}

/// Inverse of [`encode_int_key`]. Slice must be 8 bytes.
#[must_use]
pub fn decode_int_key(bytes: &[u8]) -> i64 {
    let arr: [u8; 8] = bytes.try_into().expect("int key is exactly 8 bytes");
    i64::from_be_bytes(arr) ^ i64::MIN
}

fn read_bitmap_at(postings: &[u8], offset: u64) -> RoaringBitmap {
    let offset = usize::try_from(offset).expect("postings offset fits in usize");
    let slice = postings
        .get(offset..)
        .expect("postings offset within file — shard file corrupt");
    // RoaringBitmap's wire format is self-bounded: deserialize_from reads
    // exactly the bitmap's bytes and stops, so no length prefix is needed.
    RoaringBitmap::deserialize_from(slice).expect("bitmap bytes deserialize — shard file corrupt")
}

/// Open a file at `path` and mmap its full contents read-only.
///
/// Safety: the OS treats mmap'd files as live — if the file is
/// truncated or written to under our feet, reads through the slice
/// can race or fault. Shard files satisfy the required invariant by
/// construction: they are atomic-renamed into place once at write
/// time and never mutated afterward.
fn mmap_readonly(path: &Path) -> Result<Mmap> {
    let file = fs::File::open(path).with_context(|| format!("open {} for mmap", path.display()))?;
    #[allow(unsafe_code)]
    // SAFETY: see function-level doc.
    let mmap = unsafe { Mmap::map(&file) }.with_context(|| format!("mmap {}", path.display()))?;
    Ok(mmap)
}

/// mmap `metrics.arrow` and decode its single record batch **zero-copy**: the
/// returned batch's `Int64` buffers are views into the mapping (arrow only
/// re-copies a column if it were misaligned, which the writer's 64-byte
/// alignment prevents). `Mmap` satisfies Arrow's `Allocation` marker trait via
/// its blanket impl, so the returned `Arc<Mmap>` owns the mapping both directly
/// and behind the batch's buffers — it must outlive the batch.
///
/// Preserves the one-uncompressed-batch invariant: errors unless the IPC footer
/// lists exactly one record batch.
fn mmap_forward_batch(path: &Path) -> Result<(RecordBatch, Arc<Mmap>)> {
    use arrow::ipc::convert::fb_to_schema;
    use arrow::ipc::reader::{FileDecoder, read_footer_length};
    use arrow::ipc::root_as_footer;

    let mmap = Arc::new(mmap_readonly(path)?);
    let bytes: &[u8] = &mmap;
    if bytes.len() < 10 {
        bail!(
            "{}: truncated Arrow IPC file ({} bytes)",
            path.display(),
            bytes.len()
        );
    }
    let ptr = NonNull::new(bytes.as_ptr().cast_mut()).expect("mmap of non-empty file is non-null");
    let len = bytes.len();
    #[allow(unsafe_code)]
    // SAFETY: `ptr`/`len` describe the live, immutable mmap; the cloned `Arc`
    // owner (an `Allocation`) keeps it mapped for as long as any `Buffer`
    // derived from it lives. Shard files are immutable post-rename.
    let buffer = unsafe { Buffer::from_custom_allocation(ptr, len, mmap.clone()) };

    let trailer_start = buffer.len() - 10;
    let footer_len = read_footer_length(buffer[trailer_start..].try_into().unwrap())
        .with_context(|| format!("{}: bad IPC footer length", path.display()))?;
    // A corrupt footer length could exceed the file; reject it rather than
    // underflow-panicking on the slice (the writer's files are always valid,
    // but object storage can hand back a truncated/corrupt read in Trust mode).
    let footer_start = trailer_start.checked_sub(footer_len).ok_or_else(|| {
        anyhow::anyhow!(
            "{}: IPC footer length {footer_len} exceeds file",
            path.display()
        )
    })?;
    let footer = root_as_footer(&buffer[footer_start..trailer_start])
        .map_err(|e| anyhow::anyhow!("{}: parse IPC footer: {e}", path.display()))?;

    let ipc_schema = footer
        .schema()
        .ok_or_else(|| anyhow::anyhow!("{}: IPC footer has no schema", path.display()))?;
    let schema = Arc::new(fb_to_schema(ipc_schema));
    let decoder = FileDecoder::new(schema, footer.version());

    let batches = footer
        .recordBatches()
        .ok_or_else(|| anyhow::anyhow!("{}: IPC footer has no record batches", path.display()))?;
    if batches.len() != 1 {
        bail!(
            "{}: forward-column invariant violated: expected exactly one record batch, found {}",
            path.display(),
            batches.len(),
        );
    }

    let block = batches.get(0);
    // Footer block offsets/lengths are non-negative file positions. Reject a
    // corrupt footer (negative, or out of the file's bounds) rather than
    // wrapping on the cast or panicking inside `slice_with_length`.
    let offset = usize::try_from(block.offset()).context("negative IPC block offset")?;
    let body = usize::try_from(block.bodyLength()).context("negative IPC body length")?;
    let meta = usize::try_from(block.metaDataLength()).context("negative IPC metadata length")?;
    let block_len = body
        .checked_add(meta)
        .ok_or_else(|| anyhow::anyhow!("{}: IPC block length overflow", path.display()))?;
    let in_bounds = offset
        .checked_add(block_len)
        .is_some_and(|end| end <= buffer.len());
    if !in_bounds {
        bail!(
            "{}: IPC block [{offset}..+{block_len}) exceeds file size {}",
            path.display(),
            buffer.len(),
        );
    }
    let data = buffer.slice_with_length(offset, block_len);
    let batch = decoder
        .read_record_batch(block, &data)
        .with_context(|| format!("{}: decode record batch", path.display()))?
        .ok_or_else(|| anyhow::anyhow!("{}: empty record batch message", path.display()))?;

    Ok((batch, mmap))
}

/// A shard backed by files on local disk.
///
/// `metrics.arrow` is mmap'd zero-copy at open time: the `&[i64]` slice returned
/// by [`Shard::forward_column`] is a view directly into the mapped file (paged
/// in on demand by the OS, evictable under memory pressure — not process heap).
/// Inverted-index files (FST + postings) are mmap'd the same way. So opening a
/// shard reads no column data into the heap; it parses the IPC footer and builds
/// array views over the mapping.
pub struct DiskShard {
    metadata: Metadata,
    batch: RecordBatch,
    indexes: HashMap<String, InvertedIndex>,
    /// Keeps the `metrics.arrow` mapping alive for the shard's lifetime: the
    /// forward-column `Buffer`s are views into it via Arrow's custom allocation,
    /// so it must not drop while `batch` lives. Held explicitly (not just via the
    /// buffers' internal `Arc`) to make the contract visible and robust to any
    /// future transform that replaces `batch`. Never read in normal operation —
    /// its job is ownership.
    #[allow(dead_code)]
    mmap: Arc<Mmap>,
}

impl DiskShard {
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

        if metadata.format_version < MIN_SUPPORTED_FORMAT_VERSION
            || metadata.format_version > FORMAT_VERSION
        {
            bail!(
                "{}: unsupported shard format_version {} (this build accepts {}..={})",
                metadata_path.display(),
                metadata.format_version,
                MIN_SUPPORTED_FORMAT_VERSION,
                FORMAT_VERSION,
            );
        }

        let (batch, mmap) = mmap_forward_batch(&metrics_path)?;

        validate_schema_matches(&metadata, &batch, &metrics_path)?;
        if batch.num_rows() as u64 != metadata.num_docs {
            bail!(
                "{}: row count {} does not match metadata num_docs {}",
                metrics_path.display(),
                batch.num_rows(),
                metadata.num_docs,
            );
        }

        let indexes = load_indexes(dir, &metadata)?;

        Ok(Self {
            metadata,
            batch,
            indexes,
            mmap,
        })
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

    fn inverted_index(&self, name: &str) -> Option<&InvertedIndex> {
        self.indexes.get(name)
    }
}

fn load_indexes(dir: &Path, metadata: &Metadata) -> Result<HashMap<String, InvertedIndex>> {
    let mut indexes = HashMap::new();
    for field in &metadata.fields {
        // String and Int fields both have inverted indexes on disk;
        // their key encoding differs (UTF-8 bytes vs encode_int_key)
        // but the FST/postings layout is identical.
        if !matches!(field.kind, FieldKind::String | FieldKind::Int) {
            continue;
        }
        let fst_path = posting_path(dir, &field.name, FST_EXT);
        let posting_path = posting_path(dir, &field.name, POSTING_EXT);

        let fst_mmap = mmap_readonly(&fst_path)?;
        let postings = mmap_readonly(&posting_path)?;
        let fst =
            fst::Map::new(fst_mmap).with_context(|| format!("parse FST {}", fst_path.display()))?;

        indexes.insert(field.name.clone(), InvertedIndex { fst, postings });
    }
    Ok(indexes)
}

fn posting_path(dir: &Path, field: &str, ext: &str) -> PathBuf {
    dir.join(POSTINGS_DIR).join(format!("{field}.{ext}"))
}

fn validate_schema_matches(
    metadata: &Metadata,
    batch: &RecordBatch,
    metrics_path: &Path,
) -> Result<()> {
    // Metric and Int fields both contribute an int64 column to
    // metrics.arrow; String fields have no forward column.
    let metric_fields: Vec<&FieldSchema> = metadata
        .fields
        .iter()
        .filter(|f| matches!(f.kind, FieldKind::Metric | FieldKind::Int))
        .collect();

    let schema = batch.schema_ref();
    if schema.fields().len() != metric_fields.len() {
        bail!(
            "{}: schema metric-field count {} does not match metadata {}",
            metrics_path.display(),
            schema.fields().len(),
            metric_fields.len(),
        );
    }
    for (decl, batch_field) in metric_fields.iter().zip(schema.fields()) {
        if decl.name.as_str() != batch_field.name().as_str() {
            bail!(
                "{}: schema field name mismatch: metadata says {:?}, batch says {:?}",
                metrics_path.display(),
                decl.name,
                batch_field.name(),
            );
        }
        if batch_field.data_type() != &DataType::Int64 {
            bail!(
                "{}: metric field {:?} must be Int64, got {:?}",
                metrics_path.display(),
                decl.name,
                batch_field.data_type(),
            );
        }
    }
    Ok(())
}

/// SHA-256 of `path`'s contents, formatted as `"sha256:<hex>"` — the
/// shared form used in [`Metadata::content_hashes`] and the cache's
/// per-file validator.
pub(crate) fn sha256_file(path: &Path) -> Result<String> {
    use sha2::{Digest, Sha256};
    let bytes = fs::read(path).with_context(|| format!("hash read {}", path.display()))?;
    let mut hasher = Sha256::new();
    hasher.update(&bytes);
    Ok(format!("sha256:{:x}", hasher.finalize()))
}

/// Recursive walk over every file under `dir`. `root` is the path
/// that rel-paths are computed against; pass `dir` for the top-level
/// call. Skips leftover `.<name>.tmp` siblings from a crashed
/// [`write_atomic`] (hashing those would either pollute
/// `content_hashes` or leave dangling entries after a schema change).
pub(crate) fn walk_files(
    root: &Path,
    dir: &Path,
    f: &mut dyn FnMut(&Path, &str) -> Result<()>,
) -> Result<()> {
    for entry in fs::read_dir(dir).with_context(|| format!("read_dir {}", dir.display()))? {
        let entry = entry?;
        let path = entry.path();
        let file_type = entry.file_type()?;
        if file_type.is_dir() {
            walk_files(root, &path, f)?;
        } else if file_type.is_file() {
            let file_name = entry.file_name();
            let name_str = file_name.to_string_lossy();
            if name_str.starts_with('.') && name_str.ends_with(".tmp") {
                continue;
            }
            let rel = path
                .strip_prefix(root)
                .expect("walk_files stays under root")
                .components()
                .map(|c| c.as_os_str().to_string_lossy())
                .collect::<Vec<_>>()
                .join("/");
            f(&path, &rel)?;
        }
    }
    Ok(())
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
        let (kind_label, detail) = match field.kind {
            FieldKind::Metric => ("metric", "int64".to_string()),
            FieldKind::String => {
                let idx = shard
                    .inverted_index(&field.name)
                    .expect("string field present in metadata but not loaded");
                ("string", format!("index ({} terms)", idx.num_terms()))
            }
            FieldKind::Int => {
                let idx = shard
                    .inverted_index(&field.name)
                    .expect("int field present in metadata but not loaded");
                ("int", format!("forward+index ({} terms)", idx.num_terms()))
            }
        };
        // Pad kind_label to 6 (the longest kind name: "metric" /
        // "string") so the detail column lines up across mixed-kind
        // shards (an "int" row would otherwise shorten its detail
        // column by 3 chars vs neighbouring "metric"/"string" rows).
        writeln!(
            out,
            "  {:<width$}  {:<6}  {}",
            field.name,
            kind_label,
            detail,
            width = widest_name
        )?;
    }

    Ok(())
}

#[must_use]
pub fn format_timestamp(secs: i64) -> String {
    DateTime::from_timestamp(secs, 0)
        .map_or_else(|| "(out of range)".to_string(), |t| t.to_rfc3339())
}

/// Which aggregate to display per metric. `query_shard` always
/// computes sum/min/max during the scan; this just picks which one
/// the display layer emits (avg is derived from sum + matched count
/// at display time).
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, clap::ValueEnum)]
pub enum Aggregate {
    #[default]
    Sum,
    Min,
    Max,
    Avg,
}

/// Per-metric aggregates from one shard (or accumulated across
/// shards). `sum` adds across shards; `min`/`max` reduce; `avg` is
/// computed at display time from `sum / matched` (composes correctly
/// — averaging per-shard averages would be wrong).
///
/// `min`/`max` are `None` when no docs matched the metric (e.g.
/// filter term not in the dictionary, or empty shard).
#[derive(Debug, Clone, Copy, Default)]
pub struct MetricAggregates {
    pub sum: i128,
    pub min: Option<i64>,
    pub max: Option<i64>,
}

impl MetricAggregates {
    fn merge_min(a: Option<i64>, b: Option<i64>) -> Option<i64> {
        match (a, b) {
            (Some(x), Some(y)) => Some(x.min(y)),
            (x, None) | (None, x) => x,
        }
    }

    fn merge_max(a: Option<i64>, b: Option<i64>) -> Option<i64> {
        match (a, b) {
            (Some(x), Some(y)) => Some(x.max(y)),
            (x, None) | (None, x) => x,
        }
    }

    /// Element-wise combine — used by the cross-shard reducer.
    pub fn absorb(&mut self, other: &Self) {
        self.sum += other.sum;
        self.min = Self::merge_min(self.min, other.min);
        self.max = Self::merge_max(self.max, other.max);
    }
}

/// Per-shard result of [`query_shard`]. Aggregatable across shards:
/// `num_docs` and `matched` add as `u64`; `aggregates` combines
/// element-wise via [`MetricAggregates::absorb`] (the slot order
/// matches the `metrics` slice passed in).
#[derive(Debug, Clone, Default)]
pub struct QueryResult {
    pub num_docs: u64,
    pub matched: u64,
    pub aggregates: Vec<MetricAggregates>,
}

impl QueryResult {
    /// Zero-initialised accumulator with `num_metrics` slots — the
    /// cross-shard reducer's starting point. Centralises the
    /// `aggregates.len() == metrics.len()` invariant.
    #[must_use]
    pub fn zeros(num_metrics: usize) -> Self {
        Self {
            num_docs: 0,
            matched: 0,
            aggregates: vec![MetricAggregates::default(); num_metrics],
        }
    }
}

/// Open the shard at `path`, optionally restrict to docs matching
/// every clause in `filters` (AND semantics; empty slice = match
/// all), and compute sum/min/max for each named metric over the
/// resulting doc set. Returns one [`MetricAggregates`] per input
/// metric in declaration order. Avg is derived at display time from
/// `sum / matched` so it composes correctly across shards.
///
/// All three aggregates are computed in a single scan; picking which
/// one(s) to display is the caller's job.
pub fn query_shard(
    path: &Path,
    filters: &[FilterClause<'_>],
    metrics: &[&str],
) -> Result<QueryResult> {
    let shard = DiskShard::open(path)?;
    let metadata = shard.metadata();

    // One pass that validates kind, rejects duplicate names, and
    // collects the column slices. Duplicates would produce confusing
    // repeated output lines (same column summed twice); the caller
    // almost certainly meant something else.
    let mut seen = std::collections::HashSet::new();
    let cols: Vec<&[i64]> = metrics
        .iter()
        .map(|m| {
            // Metric or Int field — both contribute a forward column.
            require_field(metadata, m, &[FieldKind::Metric, FieldKind::Int])?;
            if !seen.insert(*m) {
                bail!("metric {m:?} declared more than once");
            }
            Ok(shard
                .forward_column(m)
                .expect("metric field kind validated above"))
        })
        .collect::<Result<_>>()?;

    let combined = combine_filters(&shard, metadata, filters)?;
    let (matched, aggregates) = match combined {
        FilterResult::All => {
            let aggs: Vec<MetricAggregates> = cols
                .iter()
                .map(|col| aggregate_iter(col.iter().copied()))
                .collect();
            (metadata.num_docs, aggs)
        }
        FilterResult::Empty => (0u64, vec![MetricAggregates::default(); cols.len()]),
        FilterResult::Bitmap(bm) => {
            let aggs: Vec<MetricAggregates> = cols
                .iter()
                .map(|col| aggregate_iter(bm.iter().map(|d| col[d as usize])))
                .collect();
            (bm.len(), aggs)
        }
    };

    Ok(QueryResult {
        num_docs: metadata.num_docs,
        matched,
        aggregates,
    })
}

/// Empty iterator yields `MetricAggregates::default()` (sum=0,
/// min/max=None) — the contract `format_aggregate` relies on to render
/// "n/a" for empty matches.
fn aggregate_iter(values: impl Iterator<Item = i64>) -> MetricAggregates {
    let mut iter = values;
    let Some(first) = iter.next() else {
        return MetricAggregates::default();
    };
    let mut sum = i128::from(first);
    let mut min = first;
    let mut max = first;
    for v in iter {
        sum += i128::from(v);
        if v < min {
            min = v;
        }
        if v > max {
            max = v;
        }
    }
    MetricAggregates {
        sum,
        min: Some(min),
        max: Some(max),
    }
}

/// Emit the shared "filter / matched / metric aggregates" block used
/// by both the single-shard and dataset-wide CLI verbs. Callers print
/// whatever header they want above it. Per-metric lines show the
/// chosen aggregate, padded to the longest metric name.
pub fn write_query_summary(
    out: &mut dyn io::Write,
    filters: &[FilterClause<'_>],
    metrics: &[&str],
    aggregate: Aggregate,
    result: &QueryResult,
) -> io::Result<()> {
    if filters.is_empty() {
        writeln!(out, "matched:  all {} docs", result.num_docs)?;
    } else {
        for clause in filters {
            writeln!(out, "filter:   {clause}")?;
        }
        writeln!(
            out,
            "matched:  {} / {} docs",
            result.matched, result.num_docs
        )?;
    }
    let widest = metrics.iter().map(|m| m.len()).max().unwrap_or(0);
    let op_name = match aggregate {
        Aggregate::Sum => "sum",
        Aggregate::Min => "min",
        Aggregate::Max => "max",
        Aggregate::Avg => "avg",
    };
    for (i, m) in metrics.iter().enumerate() {
        let label = format!("{m}:");
        let value = format_aggregate(aggregate, &result.aggregates[i], result.matched);
        writeln!(
            out,
            "{:<width$}   {op_name} = {value}",
            label,
            width = widest + 1
        )?;
    }
    Ok(())
}

/// Render a single metric's aggregate value. `min`/`max`/`avg` are
/// "n/a" when no docs matched (the underlying min/max are `None`,
/// avg has zero denominator).
fn format_aggregate(op: Aggregate, agg: &MetricAggregates, matched: u64) -> String {
    match op {
        Aggregate::Sum => agg.sum.to_string(),
        Aggregate::Min => agg.min.map_or_else(|| "n/a".to_string(), |v| v.to_string()),
        Aggregate::Max => agg.max.map_or_else(|| "n/a".to_string(), |v| v.to_string()),
        Aggregate::Avg => {
            if matched == 0 {
                "n/a".to_string()
            } else {
                #[allow(clippy::cast_precision_loss)]
                let avg = (agg.sum as f64) / (matched as f64);
                format!("{avg:.2}")
            }
        }
    }
}

/// Verify that `name` is declared in `metadata.fields` and has one
/// of the accepted `FieldKind`s. Returns the matching `FieldSchema`
/// on success; produces a uniform error message on either
/// missing-field or wrong-kind failures so callers can rely on
/// consistent CLI output.
pub(crate) fn require_field<'a>(
    metadata: &'a Metadata,
    name: &str,
    accept: &[FieldKind],
) -> Result<&'a FieldSchema> {
    let field = metadata
        .fields
        .iter()
        .find(|f| f.name == name)
        .ok_or_else(|| anyhow::anyhow!("field {name:?} not found in shard"))?;
    if !accept.contains(&field.kind) {
        let expected = accept
            .iter()
            .map(FieldKind::to_string)
            .collect::<Vec<_>>()
            .join(" or ");
        bail!(
            "field {name:?} is a {} field, expected {expected}",
            field.kind,
        );
    }
    Ok(field)
}

#[cfg(test)]
mod tests;
