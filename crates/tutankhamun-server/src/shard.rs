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
//! invariant. Cold-tier compressed shards are a future addition that
//! goes through a different reader.
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

const METADATA_FILE: &str = "metadata.json";
const METRICS_FILE: &str = "metrics.arrow";
const POSTINGS_DIR: &str = "postings";
const POSTING_EXT: &str = "posting";
const FST_EXT: &str = "fst";

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

/// Field type within a shard. `Metric` is an int64 forward column;
/// `String` is an inverted index over UTF-8 terms with no forward
/// column. `Int` (forward column + inverted index over the numeric
/// values) is not yet implemented.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum FieldKind {
    Metric,
    String,
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
    /// dictionary.
    #[must_use]
    pub fn lookup(&self, term: &str) -> Option<RoaringBitmap> {
        let offset = self.fst.get(term.as_bytes())?;
        Some(read_bitmap_at(&self.postings, offset))
    }

    /// All terms in `[start, end_inclusive]`, in lexicographic order,
    /// each paired with its doc set.
    pub fn range(
        &self,
        start: &str,
        end_inclusive: &str,
    ) -> impl Iterator<Item = (String, RoaringBitmap)> + '_ {
        let mut stream = self
            .fst
            .range()
            .ge(start.as_bytes())
            .le(end_inclusive.as_bytes())
            .into_stream();
        let postings: &[u8] = &self.postings;
        std::iter::from_fn(move || {
            let (k, v) = stream.next()?;
            let term = utf8_term(k);
            Some((term, read_bitmap_at(postings, v)))
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
fn utf8_term(bytes: &[u8]) -> String {
    std::str::from_utf8(bytes)
        .expect("FST term is not valid UTF-8 — shard file corrupt")
        .to_owned()
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
        if field.kind != FieldKind::String {
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
    let metric_fields: Vec<&FieldSchema> = metadata
        .fields
        .iter()
        .filter(|f| f.kind == FieldKind::Metric)
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
    columns: Vec<(String, Vec<i64>)>,
    string_fields: Vec<(String, BTreeMap<String, RoaringBitmap>)>,
    num_docs: Option<u64>,
}

impl DiskShardWriter {
    pub fn new(dir: &Path, time_range: (i64, i64)) -> Result<Self> {
        fs::create_dir_all(dir).with_context(|| format!("create shard dir {}", dir.display()))?;
        Ok(Self {
            dir: dir.to_path_buf(),
            time_range,
            columns: Vec::new(),
            string_fields: Vec::new(),
            num_docs: None,
        })
    }

    /// Add a metric (`Int64`) column. All metric columns added to one
    /// writer must have the same length; the first call fixes the row
    /// count and subsequent mismatches return an error.
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
        self.ensure_unused_name(name)?;
        self.columns.push((name.to_string(), values));
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
        if self.columns.iter().any(|(n, _)| n == name)
            || self.string_fields.iter().any(|(n, _)| n == name)
        {
            bail!("field {name:?} already added");
        }
        Ok(())
    }

    pub fn finalize(self) -> Result<()> {
        // num_docs is the doc-ID universe size. Metric columns fix it
        // explicitly via their row count; string fields are sparse over
        // that universe. When no metric column declares it, derive
        // num_docs from the highest doc ID any string bitmap touches.
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

        // metadata.fields lists metric fields before string fields,
        // regardless of add_* call order — readers that rely on the
        // metric-prefix in validate_schema_matches depend on this.
        let mut fields: Vec<FieldSchema> =
            Vec::with_capacity(self.columns.len() + self.string_fields.len());
        for (name, _) in &self.columns {
            fields.push(FieldSchema {
                name: name.clone(),
                kind: FieldKind::Metric,
            });
        }
        for (name, _) in &self.string_fields {
            fields.push(FieldSchema {
                name: name.clone(),
                kind: FieldKind::String,
            });
        }
        let metadata = Metadata {
            format_version: FORMAT_VERSION,
            num_docs,
            time_range_start: self.time_range.0,
            time_range_end: self.time_range.1,
            fields,
        };

        let arrow_fields: Vec<Field> = self
            .columns
            .iter()
            .map(|(name, _)| Field::new(name, DataType::Int64, false))
            .collect();
        let schema = Arc::new(Schema::new(arrow_fields));

        let arrays: Vec<ArrayRef> = self
            .columns
            .into_iter()
            .map(|(_, values)| Arc::new(Int64Array::from(values)) as ArrayRef)
            .collect();

        // Required when there are no metric columns (string-only
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

        if !self.string_fields.is_empty() {
            let postings_dir = self.dir.join(POSTINGS_DIR);
            fs::create_dir_all(&postings_dir)
                .with_context(|| format!("create {}", postings_dir.display()))?;
            for (name, postings) in &self.string_fields {
                write_string_field(&self.dir, name, postings)?;
            }
        }

        let metadata_json = serde_json::to_vec_pretty(&metadata).context("serialise metadata")?;
        write_atomic(&self.dir.join(METADATA_FILE), |tmp| {
            fs::write(tmp, &metadata_json).with_context(|| format!("write {}", tmp.display()))
        })?;

        Ok(())
    }
}

/// Write `<field>.posting` (concatenated bitmaps) and `<field>.fst`
/// (term → byte offset into postings) for one string field. Posting
/// is renamed in first so a reader seeing the `.fst` is guaranteed
/// the offsets resolve.
fn write_string_field(
    dir: &Path,
    name: &str,
    postings: &BTreeMap<String, RoaringBitmap>,
) -> Result<()> {
    let mut offsets: Vec<(String, u64)> = Vec::with_capacity(postings.len());
    write_atomic(&posting_path(dir, name, POSTING_EXT), |tmp| {
        let file = fs::File::create(tmp).with_context(|| format!("create {}", tmp.display()))?;
        let mut writer = io::BufWriter::new(file);
        let mut cursor: u64 = 0;
        for (term, bitmap) in postings {
            offsets.push((term.clone(), cursor));
            bitmap
                .serialize_into(&mut writer)
                .with_context(|| format!("serialise bitmap for term {term:?}"))?;
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
        for (term, offset) in &offsets {
            builder
                .insert(term.as_bytes(), *offset)
                .with_context(|| format!("insert term {term:?} into FST"))?;
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
        };
        writeln!(
            out,
            "  {:<width$}  {}  {}",
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

/// Per-shard result of [`query_shard`]. Aggregatable across shards by
/// summing each field — `num_docs` and `matched` add as `u64`, `sum`
/// as `i128`.
#[derive(Debug, Clone, Copy, Default)]
pub struct QueryResult {
    pub num_docs: u64,
    pub matched: u64,
    pub sum: i128,
}

/// Open the shard at `path`, optionally restrict to docs matching
/// `filter = Some((field, term))`, and sum the named metric over the
/// resulting doc set.
///
/// Accumulates into `i128` so the result is overflow-free for any
/// realistic shard.
pub fn query_shard(path: &Path, filter: Option<(&str, &str)>, metric: &str) -> Result<QueryResult> {
    let shard = DiskShard::open(path)?;
    let metadata = shard.metadata();

    require_field(metadata, metric, FieldKind::Metric)?;
    let col = shard
        .forward_column(metric)
        .expect("metric field kind validated above");

    let (matched, sum) = if let Some((field, term)) = filter {
        require_field(metadata, field, FieldKind::String)?;
        let idx = shard
            .inverted_index(field)
            .expect("string field kind validated above");
        idx.lookup(term).map_or((0, 0), |bm| {
            let m = bm.len();
            let s: i128 = bm.iter().map(|doc| i128::from(col[doc as usize])).sum();
            (m, s)
        })
    } else {
        let sum: i128 = col.iter().copied().map(i128::from).sum();
        (metadata.num_docs, sum)
    };

    Ok(QueryResult {
        num_docs: metadata.num_docs,
        matched,
        sum,
    })
}

/// Used by the `t9n shard query` CLI subcommand.
pub fn query(
    path: &Path,
    filter: Option<(&str, &str)>,
    metric: &str,
    out: &mut dyn io::Write,
) -> Result<()> {
    let result = query_shard(path, filter, metric)?;
    writeln!(out, "shard:    {}", path.display())?;
    write_query_summary(out, filter, metric, &result)?;
    Ok(())
}

/// Emit the shared "filter / matched / metric sum" block used by both
/// the single-shard ([`query`]) and dataset-wide (`shard_source::query_dataset`)
/// CLI verbs. Callers are responsible for printing whatever header
/// they want above it.
pub fn write_query_summary(
    out: &mut dyn io::Write,
    filter: Option<(&str, &str)>,
    metric: &str,
    result: &QueryResult,
) -> io::Result<()> {
    if let Some((field, term)) = filter {
        writeln!(out, "filter:   {field} = {term:?}")?;
        writeln!(
            out,
            "matched:  {} / {} docs",
            result.matched, result.num_docs
        )?;
    } else {
        writeln!(out, "matched:  all {} docs", result.num_docs)?;
    }
    writeln!(out, "{metric}:   sum = {}", result.sum)?;
    Ok(())
}

/// Verify that `name` is declared in `metadata.fields` and has the
/// expected `FieldKind`. Returns the matching `FieldSchema` on success;
/// produces a uniform error message on either missing-field or
/// wrong-kind failures so callers can rely on consistent CLI output.
fn require_field<'a>(
    metadata: &'a Metadata,
    name: &str,
    want: FieldKind,
) -> Result<&'a FieldSchema> {
    let field = metadata
        .fields
        .iter()
        .find(|f| f.name == name)
        .ok_or_else(|| anyhow::anyhow!("field {name:?} not found in shard"))?;
    if field.kind != want {
        bail!(
            "field {name:?} is a {:?} field, not a {:?}",
            field.kind,
            want
        );
    }
    Ok(field)
}

#[cfg(test)]
mod tests;
