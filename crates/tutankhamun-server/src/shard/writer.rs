//! Shard writer — [`DiskShardWriter`] builds a shard directory on disk
//! (forward columns + inverted indexes) and finalises it atomically: every
//! file is written to a `.tmp` sibling and renamed into place, with
//! `metadata.json` last, so a crashed run leaves no shard visible to
//! readers. The shard format types and reader live in the parent
//! [`shard`](super) module.

use std::collections::BTreeMap;
use std::fs;
use std::io::{self, Write as _};
use std::path::{Path, PathBuf};
use std::sync::Arc;

use anyhow::{Context, Result, bail};
use arrow::array::{ArrayRef, Int64Array, RecordBatch};
use arrow::buffer::{NullBuffer, ScalarBuffer};
use arrow::datatypes::{DataType, Field, Schema};
use arrow::ipc::writer::FileWriter;
use roaring::RoaringBitmap;

use super::{
    FORMAT_VERSION, FST_EXT, FieldKind, FieldSchema, METADATA_FILE, METRICS_FILE, Metadata,
    POSTING_EXT, POSTINGS_DIR, encode_int_key, posting_path, sha256_file, walk_files,
};

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
pub(crate) fn write_atomic<F>(final_path: &Path, write_fn: F) -> Result<()>
where
    F: FnOnce(&Path) -> Result<()>,
{
    let tmp = tmp_sibling(final_path);
    write_fn(&tmp)?;
    fs::rename(&tmp, final_path).with_context(|| format!("rename {} into place", tmp.display()))?;
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
    /// Decimal scale per field name; absent ⇒ 0. Stamped into each
    /// `FieldSchema` at finalize (see [`set_field_scale`]).
    scales: BTreeMap<String, i8>,
}

/// One forward-column field, used for both `Metric` and `Int` kinds.
/// `kind` is preserved so finalize can emit it in `metadata.fields`
/// and (for `Int`) also derive the inverted index from `values`.
struct ForwardCol {
    name: String,
    values: Vec<i64>,
    kind: FieldKind,
    /// Null mask, `Some` iff the field is declared nullable. A null doc holds a
    /// placeholder in `values` and a clear bit here; it also contributes no term
    /// to an `Int` field's index (so it falls into the SQL NULL group). `None`
    /// ⇒ dense/non-nullable.
    nulls: Option<NullBuffer>,
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
            scales: BTreeMap::new(),
        })
    }

    /// Record a decimal scale for a forward-column field (the stored `i64` is
    /// the value × 10^`scale`). No-op semantics for scale 0. Call after the
    /// field's `add_metric`/`add_int_field`; finalize stamps it into the field's
    /// `metadata.json` entry.
    pub fn set_field_scale(&mut self, name: &str, scale: i8) {
        if scale != 0 {
            self.scales.insert(name.to_string(), scale);
        }
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
        self.add_forward_col(name, values, FieldKind::Metric, None)
    }

    /// Add a forward column of either kind with an optional null mask — the
    /// general form behind [`add_metric`]/[`add_int_field`], used by ingest to
    /// thread nullability. `nulls` is `Some` (even all-valid) for a declared
    /// nullable field, `None` for a dense one; a `Some` mask makes the field's
    /// `metadata.json` entry and Arrow field nullable. Same row-count rules as
    /// [`add_metric`].
    pub fn add_forward_column(
        &mut self,
        name: &str,
        values: Vec<i64>,
        kind: FieldKind,
        nulls: Option<NullBuffer>,
    ) -> Result<()> {
        self.add_forward_col(name, values, kind, nulls)
    }

    /// Add an `Int` field — int64 forward column AND inverted index
    /// over the values. Same row-count rules as [`add_metric`]; at
    /// finalize the values double as the source for the inverted
    /// index (one term per distinct value, keys
    /// [`encode_int_key`]-encoded so FST lex order matches numeric
    /// order).
    pub fn add_int_field(&mut self, name: &str, values: Vec<i64>) -> Result<()> {
        self.add_forward_col(name, values, FieldKind::Int, None)
    }

    fn add_forward_col(
        &mut self,
        name: &str,
        values: Vec<i64>,
        kind: FieldKind,
        nulls: Option<NullBuffer>,
    ) -> Result<()> {
        let len = values.len() as u64;
        match self.num_docs {
            None => self.num_docs = Some(len),
            Some(existing) if existing != len => {
                bail!("column {name:?} has {len} rows but previous columns have {existing}")
            }
            Some(_) => {}
        }
        if let Some(n) = &nulls {
            if n.len() != values.len() {
                bail!(
                    "column {name:?} null mask has {} bits but {} values",
                    n.len(),
                    values.len()
                );
            }
        }
        self.ensure_unused_name(name)?;
        self.forward_cols.push(ForwardCol {
            name: name.to_string(),
            values,
            kind,
            nulls,
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

    #[allow(clippy::too_many_lines)]
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
                // A NULL doc contributes no term — it's absent from the index, so
                // the query layer's NULL-group machinery (covered_docs) picks it up.
                if col.nulls.as_ref().is_some_and(|n| n.is_null(doc_id)) {
                    continue;
                }
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
                scale: self.scales.get(&col.name).copied().unwrap_or(0),
                nullable: col.nulls.is_some(),
            });
        }
        for (name, _) in &self.string_fields {
            fields.push(FieldSchema {
                name: name.clone(),
                kind: FieldKind::String,
                scale: 0,
                nullable: false,
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
            .map(|c| Field::new(&c.name, DataType::Int64, c.nulls.is_some()))
            .collect();
        let schema = Arc::new(Schema::new(arrow_fields));

        let arrays: Vec<ArrayRef> = self
            .forward_cols
            .into_iter()
            .map(|c| {
                let array = match c.nulls {
                    Some(nulls) => Int64Array::new(ScalarBuffer::from(c.values), Some(nulls)),
                    None => Int64Array::from(c.values),
                };
                Arc::new(array) as ArrayRef
            })
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
