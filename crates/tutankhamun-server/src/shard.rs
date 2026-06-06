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

use std::collections::{BTreeMap, HashMap};
use std::fs;
use std::io;
use std::io::Write as _;
use std::path::{Path, PathBuf};
use std::sync::Arc;

use anyhow::{Context, Result, bail};
use arrow::array::{Array, ArrayRef, Int64Array, RecordBatch};
use arrow::datatypes::{DataType, Field, Schema};
use arrow::ipc::reader::FileReader;
use arrow::ipc::writer::FileWriter;
use chrono::DateTime;
use fst::{IntoStreamer, Streamer};
use memmap2::Mmap;
use roaring::RoaringBitmap;
use serde::{Deserialize, Serialize};

pub(crate) const METADATA_FILE: &str = "metadata.json";
const METRICS_FILE: &str = "metrics.arrow";
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

/// A shard backed by files on local disk.
///
/// `metrics.arrow` is read into Arrow's heap buffers at open time —
/// a 1 GB shard means ~1 GB of resident heap per open. The hot-path
/// `&[i64]` slice returned by [`Shard::forward_column`] is a borrow
/// into those buffers. True mmap-backed zero-copy reads for forward
/// columns are a later optimisation; the trait signature does not
/// foreclose it.
///
/// Inverted-index files (FST + postings) are already mmap'd.
pub struct DiskShard {
    metadata: Metadata,
    batch: RecordBatch,
    indexes: HashMap<String, InvertedIndex>,
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

        let indexes = load_indexes(dir, &metadata)?;

        Ok(Self {
            metadata,
            batch,
            indexes,
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

/// Sibling temp path used by [`write_atomic`]: same directory as
/// `path`, filename prefixed with `.` and suffixed with `.tmp`.
fn tmp_sibling(path: &Path) -> PathBuf {
    let parent = path.parent().expect("shard files always have a parent dir");
    let name = path
        .file_name()
        .and_then(|n| n.to_str())
        .expect("shard files have UTF-8 names");
    parent.join(format!(".{name}.tmp"))
}

/// Write to a sibling `.tmp` file then atomically rename it into
/// place. A crashed writer leaves the `.tmp` behind but never a
/// half-written final file.
fn write_atomic<F>(final_path: &Path, write_fn: F) -> Result<()>
where
    F: FnOnce(&Path) -> Result<()>,
{
    let tmp = tmp_sibling(final_path);
    write_fn(&tmp)?;
    fs::rename(&tmp, final_path).with_context(|| format!("rename {} into place", tmp.display()))?;
    Ok(())
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

/// Build a shard directory on disk.
///
/// All metric columns must have the same length. `finalize` writes
/// every file to a `.tmp` sibling and atomically renames it into
/// place, so a crashed run leaves no partially-written shard visible
/// to readers.
pub struct DiskShardWriter {
    dir: PathBuf,
    time_range: (i64, i64),
    forward_cols: Vec<ForwardCol>,
    string_fields: Vec<(String, BTreeMap<String, RoaringBitmap>)>,
    num_docs: Option<u64>,
    time_field: Option<String>,
}

/// One forward-column field, used for both `Metric` and `Int` kinds.
/// `kind` is preserved so finalize can emit it in `metadata.fields`
/// and (for `Int`) also derive the inverted index from `values`.
struct ForwardCol {
    name: String,
    values: Vec<i64>,
    kind: FieldKind,
}

impl DiskShardWriter {
    pub fn new(dir: &Path, time_range: (i64, i64)) -> Result<Self> {
        fs::create_dir_all(dir).with_context(|| format!("create shard dir {}", dir.display()))?;
        Ok(Self {
            dir: dir.to_path_buf(),
            time_range,
            forward_cols: Vec::new(),
            string_fields: Vec::new(),
            num_docs: None,
            time_field: None,
        })
    }

    /// Record which field holds each doc's time (an `Int` field added
    /// separately via [`add_int_field`]). Stamped into
    /// [`Metadata::time_field`] at finalize so the query layer can
    /// present it as a timestamp and prune shards by it.
    pub fn set_time_field(&mut self, name: &str) {
        self.time_field = Some(name.to_string());
    }

    /// Add a metric (`Int64`) column — aggregatable, not filterable.
    /// All forward columns added to one writer must have the same
    /// length; the first call fixes the row count and subsequent
    /// mismatches return an error.
    ///
    /// `values` is owned so [`Int64Array::from`] can take it without a
    /// copy on `finalize`.
    pub fn add_metric(&mut self, name: &str, values: Vec<i64>) -> Result<()> {
        self.add_forward_col(name, values, FieldKind::Metric)
    }

    /// Add an `Int` field — int64 forward column AND inverted index
    /// over the values. Same row-count rules as [`add_metric`]; at
    /// finalize the values double as the source for the inverted
    /// index (one term per distinct value, keys
    /// [`encode_int_key`]-encoded so FST lex order matches numeric
    /// order).
    pub fn add_int_field(&mut self, name: &str, values: Vec<i64>) -> Result<()> {
        self.add_forward_col(name, values, FieldKind::Int)
    }

    fn add_forward_col(&mut self, name: &str, values: Vec<i64>, kind: FieldKind) -> Result<()> {
        let len = values.len() as u64;
        match self.num_docs {
            None => self.num_docs = Some(len),
            Some(existing) if existing != len => {
                bail!("column {name:?} has {len} rows but previous columns have {existing}")
            }
            Some(_) => {}
        }
        self.ensure_unused_name(name)?;
        self.forward_cols.push(ForwardCol {
            name: name.to_string(),
            values,
            kind,
        });
        Ok(())
    }

    /// Add a string field's inverted index. `postings` maps each term
    /// to its doc-ID bitmap; the `BTreeMap` ensures lex-sorted
    /// iteration, which the FST builder requires.
    ///
    /// `postings` must contain at least one term — an empty index
    /// would produce a zero-byte postings file that fails to mmap on
    /// read. Callers should simply omit the field instead.
    ///
    /// Per-term bitmaps are sparse; callers may include only docs that
    /// have a term. Doc IDs are validated against `num_docs` at
    /// [`finalize`].
    pub fn add_string_field(
        &mut self,
        name: &str,
        postings: BTreeMap<String, RoaringBitmap>,
    ) -> Result<()> {
        if postings.is_empty() {
            bail!("string field {name:?}: postings map is empty; omit the field instead");
        }
        self.ensure_unused_name(name)?;
        self.string_fields.push((name.to_string(), postings));
        Ok(())
    }

    fn ensure_unused_name(&self, name: &str) -> Result<()> {
        if self.forward_cols.iter().any(|c| c.name == name)
            || self.string_fields.iter().any(|(n, _)| n == name)
        {
            bail!("field {name:?} already added");
        }
        Ok(())
    }

    pub fn finalize(self) -> Result<()> {
        // num_docs is the doc-ID universe size. Forward columns
        // (Metric or Int) fix it explicitly via their row count;
        // string fields are sparse over that universe. When no
        // forward column declares it, derive num_docs from the
        // highest doc ID any string bitmap touches.
        let max_string_doc = self
            .string_fields
            .iter()
            .flat_map(|(_, postings)| postings.values())
            .filter_map(RoaringBitmap::max)
            .max();
        let num_docs = match (self.num_docs, max_string_doc) {
            (Some(declared), Some(max)) if u64::from(max) >= declared => {
                bail!(
                    "string-field bitmap references doc ID {max} but num_docs is {declared}; \
                     bitmap doc IDs must be < num_docs"
                );
            }
            (Some(declared), _) => declared,
            (None, Some(max)) => u64::from(max) + 1,
            (None, None) => 0,
        };

        // Build Int-field postings now — must run before
        // `self.forward_cols.into_iter()` below moves the values into
        // the Arrow arrays. For each int field, doc_id N gets a bit in
        // the bitmap for term encode_int_key(values[N]).
        //
        // `BTreeMap<[u8; 8], _>` (not `Vec<u8>`) so the per-row key
        // doesn't heap-allocate: 8-byte arrays are `Ord`
        // lexicographically, which equals numeric order via the
        // encoding, and `[u8; 8]: AsRef<[u8]>` so `write_indexed_field`
        // accepts them unchanged.
        let mut int_postings: Vec<(String, BTreeMap<[u8; 8], RoaringBitmap>)> = Vec::new();
        for col in &self.forward_cols {
            if col.kind != FieldKind::Int {
                continue;
            }
            let mut postings: BTreeMap<[u8; 8], RoaringBitmap> = BTreeMap::new();
            for (doc_id, &v) in col.values.iter().enumerate() {
                postings
                    .entry(encode_int_key(v))
                    .or_default()
                    .insert(u32::try_from(doc_id).expect("doc_id within u32 (writer enforced)"));
            }
            int_postings.push((col.name.clone(), postings));
        }

        // metadata.fields: forward_cols in insertion order (so they
        // line up with the Arrow batch's columns, which
        // validate_schema_matches relies on), then string fields.
        let mut fields: Vec<FieldSchema> =
            Vec::with_capacity(self.forward_cols.len() + self.string_fields.len());
        for col in &self.forward_cols {
            fields.push(FieldSchema {
                name: col.name.clone(),
                kind: col.kind,
            });
        }
        for (name, _) in &self.string_fields {
            fields.push(FieldSchema {
                name: name.clone(),
                kind: FieldKind::String,
            });
        }
        let mut metadata = Metadata {
            format_version: FORMAT_VERSION,
            num_docs,
            time_range_start: self.time_range.0,
            time_range_end: self.time_range.1,
            fields,
            content_hashes: BTreeMap::new(),
            time_field: self.time_field.clone(),
        };

        let arrow_fields: Vec<Field> = self
            .forward_cols
            .iter()
            .map(|c| Field::new(&c.name, DataType::Int64, false))
            .collect();
        let schema = Arc::new(Schema::new(arrow_fields));

        let arrays: Vec<ArrayRef> = self
            .forward_cols
            .into_iter()
            .map(|c| Arc::new(Int64Array::from(c.values)) as ArrayRef)
            .collect();

        // Required when there are no forward columns (string-only
        // shards): RecordBatch can't infer row count from zero arrays.
        let options = arrow::array::RecordBatchOptions::new().with_row_count(Some(
            usize::try_from(num_docs).expect("num_docs fits in usize"),
        ));
        let batch = RecordBatch::try_new_with_options(Arc::clone(&schema), arrays, &options)
            .context("build record batch")?;

        // Ordering: metrics.arrow and all postings files are renamed
        // into place before metadata.json, so a reader that sees
        // metadata.json is guaranteed to find every file it claims.
        write_atomic(&self.dir.join(METRICS_FILE), |tmp| {
            let file =
                fs::File::create(tmp).with_context(|| format!("create {}", tmp.display()))?;
            let mut writer = FileWriter::try_new(file, &schema)
                .with_context(|| format!("init Arrow IPC writer for {}", tmp.display()))?;
            writer
                .write(&batch)
                .with_context(|| format!("write record batch to {}", tmp.display()))?;
            writer
                .finish()
                .with_context(|| format!("finalise {}", tmp.display()))?;
            Ok(())
        })?;

        if !self.string_fields.is_empty() || !int_postings.is_empty() {
            let postings_dir = self.dir.join(POSTINGS_DIR);
            fs::create_dir_all(&postings_dir)
                .with_context(|| format!("create {}", postings_dir.display()))?;
            for (name, postings) in &self.string_fields {
                write_indexed_field(&self.dir, name, postings)?;
            }
            for (name, postings) in &int_postings {
                write_indexed_field(&self.dir, name, postings)?;
            }
        }

        metadata.content_hashes = hash_payload_files(&self.dir)?;
        let metadata_json = serde_json::to_vec_pretty(&metadata).context("serialise metadata")?;
        write_atomic(&self.dir.join(METADATA_FILE), |tmp| {
            fs::write(tmp, &metadata_json).with_context(|| format!("write {}", tmp.display()))
        })?;

        Ok(())
    }
}

/// Walk `dir` and hash every payload file (everything except
/// `metadata.json` itself). Returns a map of `<rel-path>` →
/// `"sha256:<hex>"`. Rel-paths use `/` separators regardless of host
/// platform so the same shard reads back the same on any OS.
fn hash_payload_files(dir: &Path) -> Result<BTreeMap<String, String>> {
    let mut out = BTreeMap::new();
    walk_files(dir, dir, &mut |abs_path, rel_path| {
        if rel_path == METADATA_FILE {
            return Ok(());
        }
        out.insert(rel_path.to_string(), sha256_file(abs_path)?);
        Ok(())
    })?;
    Ok(out)
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

/// Write `<field>.posting` (concatenated bitmaps) and `<field>.fst`
/// (term → byte offset into postings) for one indexed field. Posting
/// is renamed in first so a reader seeing the `.fst` is guaranteed
/// the offsets resolve.
///
/// Generic over key type so `String` (UTF-8) and `Vec<u8>`
/// (order-preserving int encoding) both work through one path. The
/// `BTreeMap` iteration order = byte order in both cases, which is
/// what the FST builder requires.
fn write_indexed_field<K>(
    dir: &Path,
    name: &str,
    postings: &BTreeMap<K, RoaringBitmap>,
) -> Result<()>
where
    K: AsRef<[u8]>,
{
    // Copy keys to owned Vec<u8> so the second pass (FST build) can
    // see them after the first pass (posting writes) borrows
    // `postings` to iterate. Per-key copy is tiny vs the bitmap
    // serialisation cost.
    let mut offsets: Vec<(Vec<u8>, u64)> = Vec::with_capacity(postings.len());
    write_atomic(&posting_path(dir, name, POSTING_EXT), |tmp| {
        let file = fs::File::create(tmp).with_context(|| format!("create {}", tmp.display()))?;
        let mut writer = io::BufWriter::new(file);
        let mut cursor: u64 = 0;
        for (key, bitmap) in postings {
            offsets.push((key.as_ref().to_vec(), cursor));
            bitmap
                .serialize_into(&mut writer)
                .with_context(|| format!("serialise bitmap for field {name:?}"))?;
            cursor += bitmap.serialized_size() as u64;
        }
        writer
            .flush()
            .with_context(|| format!("flush {}", tmp.display()))?;
        Ok(())
    })?;

    write_atomic(&posting_path(dir, name, FST_EXT), |tmp| {
        let file = fs::File::create(tmp).with_context(|| format!("create {}", tmp.display()))?;
        let mut builder = fst::MapBuilder::new(io::BufWriter::new(file))
            .with_context(|| format!("init FST builder for {}", tmp.display()))?;
        for (key, offset) in &offsets {
            builder
                .insert(key, *offset)
                .with_context(|| format!("insert key into FST for field {name:?}"))?;
        }
        // into_inner finalises the FST (writes the footer); the second
        // call unwraps the BufWriter to flush it to disk.
        builder
            .into_inner()
            .with_context(|| format!("finalise FST {}", tmp.display()))?
            .into_inner()
            .with_context(|| format!("flush FST {}", tmp.display()))?;
        Ok(())
    })?;
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

/// What `--filter` resolves to: a single-term equality or an
/// inclusive range with optionally open ends. The CLI parser
/// constructs these; the engine consumes them.
#[derive(Debug, Clone, Copy)]
pub struct FilterClause<'a> {
    pub field: &'a str,
    pub op: FilterOp<'a>,
}

#[derive(Debug, Clone, Copy)]
pub enum FilterOp<'a> {
    /// Exact-term match against the field's inverted index.
    Equals(&'a str),
    /// Inclusive range. `None` on either side means unbounded.
    /// `lo` and `hi` both `None` is rejected upstream — it would
    /// match every doc, which the user almost certainly didn't mean.
    Range {
        lo: Option<&'a str>,
        hi: Option<&'a str>,
    },
}

impl<'a> FilterClause<'a> {
    #[must_use]
    pub fn equals(field: &'a str, term: &'a str) -> Self {
        Self {
            field,
            op: FilterOp::Equals(term),
        }
    }

    #[must_use]
    pub fn range(field: &'a str, lo: Option<&'a str>, hi: Option<&'a str>) -> Self {
        Self {
            field,
            op: FilterOp::Range { lo, hi },
        }
    }
}

impl std::fmt::Display for FilterClause<'_> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match &self.op {
            FilterOp::Equals(term) => write!(f, "{} = {:?}", self.field, term),
            FilterOp::Range { lo, hi } => write!(
                f,
                "{} = {}..{}",
                self.field,
                lo.unwrap_or(""),
                hi.unwrap_or(""),
            ),
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

/// Outcome of intersecting every filter clause: either no filter was
/// supplied (match every doc), some clause matched no docs (whole
/// result is empty, short-circuiting the rest), or a concrete doc set.
/// Public form of the engine's per-shard filter-resolution result.
/// Used internally by [`query_shard`] for aggregate dispatch and
/// externally by the SQL `TableProvider` to drive row-set
/// materialisation.
#[derive(Debug)]
pub enum FilterResult {
    /// No filter supplied — every doc matches.
    All,
    /// Filter matched zero docs (or some clause hit zero terms in
    /// the index and short-circuited the intersection).
    Empty,
    /// Concrete matched doc set.
    Bitmap(RoaringBitmap),
}

/// Resolve every clause in `filters` against `shard`, AND-intersect
/// the per-clause bitmaps, and return the [`FilterResult`] dispatch
/// shape. Empty `filters` slice → `All`; any clause that hits zero
/// terms short-circuits the whole result to `Empty`.
pub fn matched_doc_set(shard: &DiskShard, filters: &[FilterClause<'_>]) -> Result<FilterResult> {
    let metadata = shard.metadata();
    combine_filters(shard, metadata, filters)
}

fn combine_filters(
    shard: &DiskShard,
    metadata: &Metadata,
    filters: &[FilterClause<'_>],
) -> Result<FilterResult> {
    use roaring::MultiOps;

    if filters.is_empty() {
        return Ok(FilterResult::All);
    }
    let mut resolved: Vec<RoaringBitmap> = Vec::with_capacity(filters.len());
    for clause in filters {
        match resolve_filter(shard, metadata, clause)? {
            Some(bm) => resolved.push(bm),
            None => return Ok(FilterResult::Empty),
        }
    }
    // Tree-reduced k-way intersection — beats pairwise `&=` when
    // bitmaps differ in size (smallest pair reduced first).
    let intersection: RoaringBitmap = resolved.into_iter().intersection();
    Ok(if intersection.is_empty() {
        FilterResult::Empty
    } else {
        FilterResult::Bitmap(intersection)
    })
}

/// Resolve one filter clause to its matched doc set, or `None` if no
/// docs match (a clause whose term/range hits zero terms in the
/// inverted index).
fn resolve_filter(
    shard: &DiskShard,
    metadata: &Metadata,
    clause: &FilterClause<'_>,
) -> Result<Option<RoaringBitmap>> {
    // String or Int field — both have an inverted index, but the
    // term encoding differs: String uses UTF-8 bytes directly, Int
    // parses the decimal term and encodes via encode_int_key.
    let filter_field = require_field(metadata, clause.field, &[FieldKind::String, FieldKind::Int])?;
    let idx = shard
        .inverted_index(clause.field)
        .expect("indexed field kind validated above");
    match clause.op {
        FilterOp::Equals(term) => {
            let key = encode_bound(filter_field.kind, clause.field, Some(term))?
                .expect("Some bound yields Some encoded key");
            Ok(idx.lookup_bytes(&key))
        }
        FilterOp::Range { lo, hi } => {
            use roaring::MultiOps;
            if lo.is_none() && hi.is_none() {
                // Reachable only via a programmatic
                // `FilterClause::range(_, None, None)`; the CLI
                // parser rejects `field=..` upstream. Hard-fail so
                // the misuse can't silently match every doc.
                bail!(
                    "range filter for field {field:?} must have at least one bound",
                    field = clause.field,
                );
            }
            let lo_bytes = encode_bound(filter_field.kind, clause.field, lo)?;
            let hi_bytes = encode_bound(filter_field.kind, clause.field, hi)?;
            // Encoded byte order matches the desired range semantics
            // for both kinds (UTF-8 lex for String, numeric via
            // encode_int_key for Int), so a byte compare catches
            // inverted ranges across both.
            if let (Some(l), Some(h)) = (&lo_bytes, &hi_bytes)
                && l > h
            {
                bail!(
                    "range filter for field {field:?}: lower bound {lo:?} exceeds upper bound {hi:?}",
                    field = clause.field,
                    lo = lo.expect("checked above"),
                    hi = hi.expect("checked above"),
                );
            }
            // Tree-reduced k-way union — for wide ranges over
            // high-cardinality fields this beats pairwise `|=`
            // because intermediate bitmap sizes stay smaller.
            let union: RoaringBitmap = idx
                .range_bytes(lo_bytes.as_deref(), hi_bytes.as_deref())
                .map(|(_, bm)| bm)
                .union();
            Ok(if union.is_empty() { None } else { Some(union) })
        }
    }
}

/// Encode a single filter bound (either side of a range, or the
/// equality term) for an inverted-index lookup. `None` bound →
/// `None` byte vec (the open-range case in
/// [`InvertedIndex::range_bytes`]). String fields use UTF-8 bytes
/// directly; Int fields parse the bound as i64 and encode via
/// [`encode_int_key`] so the FST's byte order matches numeric order.
fn encode_bound(kind: FieldKind, field: &str, bound: Option<&str>) -> Result<Option<Vec<u8>>> {
    let Some(bound) = bound else {
        return Ok(None);
    };
    match kind {
        FieldKind::String => Ok(Some(bound.as_bytes().to_vec())),
        FieldKind::Int => {
            let v: i64 = bound.parse().with_context(|| {
                format!("filter value {bound:?} is not a valid int64 for field {field:?}")
            })?;
            Ok(Some(encode_int_key(v).to_vec()))
        }
        FieldKind::Metric => unreachable!("require_field rejected non-indexed kind"),
    }
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
