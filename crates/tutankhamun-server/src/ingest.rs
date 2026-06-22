//! CSV/TSV and Parquet → shard ingester.
//!
//! [`ingest_csv`] reads a delimited text file and [`ingest_parquet`] reads a
//! Parquet file (via its typed Arrow schema); both parse each row and feed the
//! declared columns into [`crate::shard::DiskShardWriter`]. Columns not declared
//! in [`IngestOptions`] are silently dropped.
//!
//! The storage format's metrics are `i64`, so numeric columns are stored as a
//! scaled integer: integers as-is (scale 0), `Decimal128(p,s)` as the unscaled
//! mantissa (scale `s`, from the schema), and floats as `round(v × 10^scale)`
//! (scale defaults to [`DEFAULT_FLOAT_SCALE`], overridable per column). The scale
//! is recorded in each field's `metadata.json` entry.
//!
//! Used by the `t9n ingest` CLI verb.

use std::collections::{BTreeMap, HashSet};
use std::path::{Path, PathBuf};
use std::sync::Arc;

use anyhow::{Context, Result, bail};
use arrow::array::{
    Array, ArrayRef, Date32Array, Date64Array, Decimal128Array, Float32Array, Float64Array,
    Int8Array, Int16Array, Int32Array, Int64Array, LargeStringArray, StringArray,
    TimestampMicrosecondArray, TimestampMillisecondArray, TimestampNanosecondArray,
    TimestampSecondArray, UInt8Array, UInt16Array, UInt32Array,
};
use arrow::datatypes::{DataType, TimeUnit};
use chrono::{DateTime, Datelike, NaiveDate, NaiveDateTime};
use object_store::path::Path as ObjPath;
use object_store::{ObjectStore, PutPayload, WriteMultipart};
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
use roaring::RoaringBitmap;
use tokio::io::AsyncReadExt;

use crate::shard::{DiskShardWriter, FieldKind, METADATA_FILE, Metadata};

/// Default decimal scale applied to float columns at ingest (`round(v × 10^3)`),
/// i.e. three fractional digits. Overridable per column via `--scale`.
pub const DEFAULT_FLOAT_SCALE: i8 = 3;

/// Files smaller than this go through a single `put`; larger files
/// use `put_multipart` so we never buffer the full payload in
/// memory. The threshold matches S3's 5 MiB minimum per non-final
/// multipart part (going multipart on smaller files would just add
/// round-trips for no gain).
const SINGLE_PUT_THRESHOLD: u64 = 5 * 1024 * 1024;

const STREAM_CHUNK_SIZE: usize = 5 * 1024 * 1024;

const DEFAULT_UPLOAD_CONCURRENCY: usize = 8;

/// Cap on concurrent multipart parts in flight for any one file.
/// `WriteMultipart::write` spawns a fresh upload task on every full
/// chunk without applying backpressure; without this cap a 5 GiB
/// shard would launch ~1024 simultaneous part PUTs.
const MAX_PARTS_IN_FLIGHT: usize = 8;

/// What to extract from each row, by column name.
#[derive(Debug, Clone)]
pub struct IngestOptions {
    /// Header name of the column to parse as the doc's time.
    pub time: String,
    /// Header names of columns to store as int64 metric forward
    /// columns (aggregatable, not filterable).
    pub metrics: Vec<String>,
    /// Header names of columns to store as string-field inverted
    /// indexes (filterable, not aggregatable).
    pub strings: Vec<String>,
    /// Header names of columns to store as `Int` fields — int64
    /// forward column plus inverted index over the numeric values
    /// (both filterable and aggregatable).
    pub ints: Vec<String>,
    /// Field delimiter — `,` for CSV, `\t` for TSV.
    pub delimiter: u8,
    /// How to partition rows into shards. Defaults to one shard per
    /// ingest (`ShardBy::None`).
    pub shard_by: ShardBy,
}

/// How rows are partitioned into shards. Most granularities are
/// duration-based (`Bucket { seconds }`); the CLI exposes named aliases
/// (`hourly` → 3600, `daily` → 86400, `weekly` → 604800) but the engine
/// accepts any positive bucket size — adding `--shard-by 6h` later is a
/// CLI-parser change only. `Month` is the one calendar-based granularity,
/// since a month is not a fixed number of seconds.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub enum ShardBy {
    /// All rows into a single shard at the `--output` path.
    #[default]
    None,
    /// One shard per `seconds`-wide UTC time bucket. Buckets are aligned
    /// to the Unix epoch (so `weekly` windows start on the epoch's
    /// weekday, Thursday) — a storage-partitioning detail; query-time
    /// pruning works regardless of alignment.
    Bucket { seconds: i64 },
    /// One shard per calendar month (UTC).
    Month,
}

impl ShardBy {
    /// Integer bucket key for an epoch-seconds time. Equal keys land
    /// in the same shard. `div_euclid` (not `/`) so pre-1970 inputs
    /// floor toward negative infinity correctly.
    fn bucket_key(self, epoch: i64) -> i64 {
        match self {
            Self::None => 0,
            Self::Bucket { seconds } => epoch.div_euclid(seconds),
            Self::Month => {
                let dt = DateTime::from_timestamp(epoch, 0).expect("epoch within chrono range");
                i64::from(dt.year()) * 12 + i64::from(dt.month0())
            }
        }
    }

    /// Where to write the shard for `key` under `root`. Dirname
    /// precision is tied to the bucket size — when a new size lands
    /// the match below must gain a matching format, otherwise two
    /// distinct buckets could collide on the same dirname (e.g.
    /// `seconds=1800` would write `YYYY-MM-DDTHH` for both
    /// half-hours of an hour). Fail fast rather than silently merge.
    fn output_dir(self, root: &Path, key: i64) -> PathBuf {
        match self {
            Self::None => root.to_path_buf(),
            Self::Bucket { seconds } => {
                let bucket_start = key.saturating_mul(seconds);
                let dt = DateTime::from_timestamp(bucket_start, 0)
                    .expect("bucket_start within chrono range");
                let fmt = match seconds {
                    // Daily and weekly both name the dir by the bucket-start
                    // date; each weekly bucket starts on a distinct date, so
                    // they never collide within a single (fixed-granularity)
                    // ingest.
                    86_400 | 604_800 => "%Y-%m-%d",
                    3600 => "%Y-%m-%dT%H",
                    _ => panic!(
                        "ShardBy::output_dir has no dirname format defined for {seconds}s \
                         buckets — extend the match before exposing this granularity on the CLI"
                    ),
                };
                root.join(dt.format(fmt).to_string())
            }
            Self::Month => {
                let year = i32::try_from(key.div_euclid(12)).expect("year within range");
                let month = u32::try_from(key.rem_euclid(12)).expect("month in 0..12") + 1;
                let dt = NaiveDate::from_ymd_opt(year, month, 1).expect("valid year-month");
                root.join(dt.format("%Y-%m").to_string())
            }
        }
    }
}

/// Where `t9n ingest` should put its output. A bare path or
/// `file://` URL routes to [`IngestDestination::Local`] (written
/// directly); any other `object_store` URL routes to
/// [`IngestDestination::Remote`] (written to a staging tempdir
/// first, then uploaded by [`upload_ingest_tree`]).
#[derive(Debug, Clone)]
pub enum IngestDestination {
    Local(PathBuf),
    Remote(String),
}

impl IngestDestination {
    /// Parse a `--output` argument. Bare paths become
    /// `Local(PathBuf)`; URLs are dispatched by scheme.
    pub fn parse(raw: &str) -> Result<Self> {
        if !raw.contains("://") {
            return Ok(Self::Local(PathBuf::from(raw)));
        }
        let url = url::Url::parse(raw).with_context(|| format!("parse output URL {raw:?}"))?;
        if url.scheme() == "file" {
            let path = url
                .to_file_path()
                .map_err(|()| anyhow::anyhow!("file URL {raw:?} has no filesystem path"))?;
            return Ok(Self::Local(path));
        }
        Ok(Self::Remote(raw.to_string()))
    }
}

/// Upload every shard under `local_root` to `url`, preserving the
/// directory layout. Shards upload concurrently (capped by
/// [`upload_concurrency`]); within each shard, every payload file
/// uploads before `metadata.json` so the shard isn't discoverable
/// until it's complete (the discovery code's "look for
/// metadata.json" check).
///
/// "Shard" means any directory under `local_root` that directly
/// contains a `metadata.json`. Handles both `--shard-by none` (one
/// shard at the root) and `--shard-by daily`/`hourly` (one shard per
/// bucket subdir).
pub async fn upload_ingest_tree(local_root: &Path, url: &str) -> Result<()> {
    use futures::stream::{StreamExt, TryStreamExt};

    let registry = crate::storage::StorageRegistry::from_url(url)?;
    let store = registry.store();
    let shard_dirs = find_shard_dirs(local_root)?;
    let concurrency = upload_concurrency();

    futures::stream::iter(shard_dirs.into_iter().map(|shard_dir| {
        let store = Arc::clone(&store);
        let rel = shard_rel_location(local_root, &shard_dir);
        async move { upload_one_shard(&*store, &shard_dir, &rel).await }
    }))
    .buffer_unordered(concurrency)
    .try_for_each(|()| async { Ok(()) })
    .await
}

/// A shard directory's location relative to the dataset `root`, as a
/// `/`-joined backend path (empty for a single-shard root) — the prefix the
/// shard's files are uploaded under in storage.
fn shard_rel_location(root: &Path, dir: &Path) -> String {
    dir.strip_prefix(root)
        .expect("find_shard_dirs returns paths under root")
        .components()
        .map(|c| c.as_os_str().to_string_lossy().into_owned())
        .collect::<Vec<_>>()
        .join("/")
}

/// Read and parse a shard directory's `metadata.json`.
fn read_shard_metadata(dir: &Path) -> Result<Metadata> {
    let path = dir.join(METADATA_FILE);
    let bytes = std::fs::read(&path).with_context(|| format!("read {}", path.display()))?;
    serde_json::from_slice(&bytes).with_context(|| format!("parse {}", path.display()))
}

fn upload_concurrency() -> usize {
    std::env::var("T9N_UPLOAD_CONCURRENCY")
        .ok()
        .and_then(|s| s.parse::<usize>().ok())
        .filter(|&n| n > 0)
        .unwrap_or(DEFAULT_UPLOAD_CONCURRENCY)
}

fn find_shard_dirs(root: &Path) -> Result<Vec<PathBuf>> {
    let mut out = Vec::new();
    walk_shard_dirs(root, &mut out)?;
    Ok(out)
}

/// Every shard written under `root`, paired with its doc count, sorted by
/// path (bucket dirs are named by date, so this reads chronologically).
/// Reads each shard's `metadata.json`; used by the `ingest` CLI to report
/// what it wrote. Mirrors discovery's "directory containing `metadata.json`"
/// rule, so it covers both `--shard-by none` and bucketed layouts.
pub fn shard_summaries(root: &Path) -> Result<Vec<(PathBuf, u64)>> {
    let mut dirs = find_shard_dirs(root)?;
    dirs.sort();
    dirs.into_iter()
        .map(|dir| {
            let num_docs = read_shard_metadata(&dir)?.num_docs;
            Ok((dir, num_docs))
        })
        .collect()
}

fn walk_shard_dirs(dir: &Path, out: &mut Vec<PathBuf>) -> Result<()> {
    if dir.join(METADATA_FILE).is_file() {
        out.push(dir.to_path_buf());
        return Ok(());
    }
    for entry in std::fs::read_dir(dir).with_context(|| format!("read_dir {}", dir.display()))? {
        let entry = entry?;
        if entry.file_type()?.is_dir() {
            walk_shard_dirs(&entry.path(), out)?;
        }
    }
    Ok(())
}

async fn upload_one_shard(
    store: &dyn ObjectStore,
    local_shard_dir: &Path,
    remote_prefix: &str,
) -> Result<()> {
    let metadata_local = local_shard_dir.join(METADATA_FILE);
    if !metadata_local.is_file() {
        bail!("missing {} in {}", METADATA_FILE, local_shard_dir.display());
    }
    let mut payload_files: Vec<(PathBuf, String)> = Vec::new();
    crate::shard::walk_files(
        local_shard_dir,
        local_shard_dir,
        &mut |abs_path, rel_path| {
            if rel_path == METADATA_FILE {
                return Ok(());
            }
            payload_files.push((abs_path.to_path_buf(), rel_path.to_string()));
            Ok(())
        },
    )?;

    for (abs_path, rel_path) in payload_files {
        upload_file(
            store,
            &abs_path,
            &join_remote_path(remote_prefix, &rel_path),
        )
        .await?;
    }
    // metadata.json LAST: discovery looks for it, so until it's
    // there the shard is invisible — partial uploads can't be seen.
    upload_file(
        store,
        &metadata_local,
        &join_remote_path(remote_prefix, METADATA_FILE),
    )
    .await
}

async fn upload_file(store: &dyn ObjectStore, local: &Path, remote: &ObjPath) -> Result<()> {
    let metadata = tokio::fs::metadata(local)
        .await
        .with_context(|| format!("stat {}", local.display()))?;
    if metadata.len() < SINGLE_PUT_THRESHOLD {
        let bytes = tokio::fs::read(local)
            .await
            .with_context(|| format!("read {}", local.display()))?;
        store
            .put(remote, PutPayload::from(bytes))
            .await
            .with_context(|| format!("upload {} -> {remote}", local.display()))?;
        return Ok(());
    }
    let upload = store
        .put_multipart(remote)
        .await
        .with_context(|| format!("start multipart {} -> {remote}", local.display()))?;
    let mut writer = WriteMultipart::new_with_chunk_size(upload, STREAM_CHUNK_SIZE);
    // Drive the read/spawn loop in an inner block: on any error we
    // must call `writer.abort()` explicitly because S3/GCS don't
    // auto-clean a multipart upload when the writer is just dropped
    // — the parts stay billed until a lifecycle rule reaps them.
    let drive: Result<()> = async {
        let mut file = tokio::fs::File::open(local)
            .await
            .with_context(|| format!("open {}", local.display()))?;
        let mut buf = vec![0u8; STREAM_CHUNK_SIZE];
        loop {
            let n = file
                .read(&mut buf)
                .await
                .with_context(|| format!("read {}", local.display()))?;
            if n == 0 {
                break;
            }
            writer
                .wait_for_capacity(MAX_PARTS_IN_FLIGHT)
                .await
                .with_context(|| format!("multipart backpressure {}", local.display()))?;
            writer.write(&buf[..n]);
        }
        Ok(())
    }
    .await;
    if let Err(err) = drive {
        let _ = writer.abort().await;
        return Err(err);
    }
    writer
        .finish()
        .await
        .with_context(|| format!("finish multipart {} -> {remote}", local.display()))?;
    Ok(())
}

fn join_remote_path(prefix: &str, rel: &str) -> ObjPath {
    if prefix.is_empty() {
        ObjPath::from(rel)
    } else {
        ObjPath::from(format!("{prefix}/{rel}"))
    }
}

/// Read `input`, parse rows according to `opts`, and write a single
/// finalised shard to `output`.
///
/// Returns the number of data rows ingested. Errors carry the input
/// line number where the problem was found so the user can locate it.
///
/// **Memory footprint:** all parsed values are held in memory until
/// `finalize` runs — roughly `N × 8 × num_metrics` bytes plus
/// per-term postings storage for string fields. A 100M-row × 20-metric
/// ingest needs ~16 GB resident before counting strings. Run separate
/// ingests over time slices to bound RSS on large inputs.
pub fn ingest_csv(input: &Path, output: &Path, opts: &IngestOptions) -> Result<u64> {
    check_no_duplicate_columns(opts)?;

    let mut reader = csv::ReaderBuilder::new()
        .delimiter(opts.delimiter)
        .has_headers(true)
        .from_path(input)
        .with_context(|| format!("open {}", input.display()))?;

    let header_row: Vec<String> = reader.headers()?.iter().map(str::to_owned).collect();
    let find = |name: &str| -> Result<usize> {
        header_row
            .iter()
            .position(|h| h == name)
            .ok_or_else(|| anyhow::anyhow!("column {name:?} not found in CSV header"))
    };

    let time_idx = find(&opts.time)?;
    // Metric and Int fields share the same numeric accumulator —
    // their only difference is which `DiskShardWriter` method
    // receives them at finalize. Metrics-then-ints preserves the
    // existing "all forward columns up front" ordering in the
    // resulting Arrow schema.
    let mut numeric_proto: Vec<NumericCol> = Vec::new();
    for name in &opts.metrics {
        numeric_proto.push(NumericCol {
            name: name.clone(),
            col_idx: find(name)?,
            values: Vec::new(),
            kind: FieldKind::Metric,
            scale: 0,
        });
    }
    for name in &opts.ints {
        numeric_proto.push(NumericCol {
            name: name.clone(),
            col_idx: find(name)?,
            values: Vec::new(),
            kind: FieldKind::Int,
            scale: 0,
        });
    }
    let strings_proto: Vec<StringCol> = opts
        .strings
        .iter()
        .map(|name| {
            Ok(StringCol {
                name: name.clone(),
                col_idx: find(name)?,
                postings: BTreeMap::new(),
            })
        })
        .collect::<Result<_>>()?;

    // BTreeMap (not HashMap) so finalize iterates buckets in
    // time-ascending order — operators following the log see
    // chronological progress, and deterministic order is easier to
    // debug on partial failures.
    let mut buckets: BTreeMap<i64, BucketBuilder> = BTreeMap::new();
    // Caches the successful time format after the first row so subsequent
    // rows skip the failing-parser attempts. Mixed-format inputs still
    // work via the full-detection fallback inside parse_time.
    let mut time_hint: Option<TimeFormat> = None;
    let mut total_rows: u64 = 0;

    for record in reader.records() {
        let row = record.with_context(|| format!("read {}", input.display()))?;
        let line = row.position().map_or(0, csv::Position::line);

        let time_str = row
            .get(time_idx)
            .with_context(|| format!("line {line}: missing time column"))?;
        let (t, fmt) = parse_time(time_str, time_hint)
            .with_context(|| format!("line {line}: time column {time_str:?}"))?;
        time_hint = Some(fmt);

        let key = opts.shard_by.bucket_key(t);
        let bucket = buckets
            .entry(key)
            .or_insert_with(|| BucketBuilder::new(&numeric_proto, &strings_proto, &opts.time));
        bucket.push_row(&row, line, t)?;
        total_rows += 1;
    }

    if total_rows == 0 {
        bail!("no data rows in {}", input.display());
    }

    let mut written: u64 = 0;
    for (key, bucket) in buckets {
        let shard_dir = opts.shard_by.output_dir(output, key);
        written += bucket.finalize(&shard_dir)?;
    }
    Ok(written)
}

/// Read `input` as Parquet (via its typed Arrow schema) and write shards,
/// mirroring [`ingest_csv`]'s bucketing + finalize. `scales` overrides the
/// per-column float scale (`--scale`) and is valid only on float columns.
pub fn ingest_parquet(
    input: &Path,
    output: &Path,
    opts: &IngestOptions,
    scales: &BTreeMap<String, i8>,
) -> Result<u64> {
    check_no_duplicate_columns(opts)?;

    let file = std::fs::File::open(input).with_context(|| format!("open {}", input.display()))?;
    let builder = ParquetRecordBatchReaderBuilder::try_new(file)
        .with_context(|| format!("open parquet {}", input.display()))?;
    let schema = builder.schema().clone();

    let find = |name: &str| -> Result<usize> {
        schema
            .index_of(name)
            .map_err(|_| anyhow::anyhow!("column {name:?} not found in parquet schema"))
    };

    let time_idx = find(&opts.time)?;

    let mut numeric_proto: Vec<NumericCol> = Vec::new();
    for (names, kind) in [
        (&opts.metrics, FieldKind::Metric),
        (&opts.ints, FieldKind::Int),
    ] {
        for name in names {
            let idx = find(name)?;
            let scale = resolve_scale(name, schema.field(idx).data_type(), scales)?;
            numeric_proto.push(NumericCol {
                name: name.clone(),
                col_idx: idx,
                values: Vec::new(),
                kind,
                scale,
            });
        }
    }
    let strings_proto: Vec<StringCol> = opts
        .strings
        .iter()
        .map(|name| {
            Ok(StringCol {
                name: name.clone(),
                col_idx: find(name)?,
                postings: BTreeMap::new(),
            })
        })
        .collect::<Result<_>>()?;

    // A `--scale` key that names no ingested metric/int column is a user error
    // (resolve_scale already rejects scale on int/decimal columns).
    for key in scales.keys() {
        if !numeric_proto.iter().any(|c| &c.name == key) {
            bail!("--scale {key}: no metric/int column named {key:?}");
        }
    }

    let reader = builder
        .build()
        .with_context(|| format!("read parquet {}", input.display()))?;

    let mut buckets: BTreeMap<i64, BucketBuilder> = BTreeMap::new();
    let mut total_rows: u64 = 0;
    for batch in reader {
        let batch =
            batch.with_context(|| format!("read parquet batch from {}", input.display()))?;
        let time_arr = batch.column(time_idx);
        let mut numeric = Vec::with_capacity(numeric_proto.len());
        let mut strings = Vec::with_capacity(strings_proto.len());
        for row in 0..batch.num_rows() {
            let t = extract_time(time_arr, row, &opts.time)?;
            numeric.clear();
            for col in &numeric_proto {
                numeric.push(extract_numeric(
                    batch.column(col.col_idx),
                    row,
                    &col.name,
                    col.scale,
                )?);
            }
            strings.clear();
            for col in &strings_proto {
                strings.push(extract_string(batch.column(col.col_idx), row, &col.name)?);
            }
            let key = opts.shard_by.bucket_key(t);
            buckets
                .entry(key)
                .or_insert_with(|| BucketBuilder::new(&numeric_proto, &strings_proto, &opts.time))
                .push_values(t, &numeric, &strings)?;
            total_rows += 1;
        }
    }

    if total_rows == 0 {
        bail!("no data rows in {}", input.display());
    }

    let mut written: u64 = 0;
    for (key, bucket) in buckets {
        let shard_dir = opts.shard_by.output_dir(output, key);
        written += bucket.finalize(&shard_dir)?;
    }
    Ok(written)
}

/// Resolve the decimal scale for a metric/int column from its Arrow type and the
/// `--scale` overrides: decimals use their schema scale, floats default to
/// [`DEFAULT_FLOAT_SCALE`] (overridable), integers are scale 0. `--scale` on a
/// non-float column is a user error.
fn resolve_scale(name: &str, dt: &DataType, scales: &BTreeMap<String, i8>) -> Result<i8> {
    match dt {
        DataType::Decimal128(_, s) => {
            if scales.contains_key(name) {
                bail!("--scale {name}: decimal columns carry their own scale; drop the override");
            }
            Ok(*s)
        }
        DataType::Float32 | DataType::Float64 => {
            Ok(scales.get(name).copied().unwrap_or(DEFAULT_FLOAT_SCALE))
        }
        DataType::Int8
        | DataType::Int16
        | DataType::Int32
        | DataType::Int64
        | DataType::UInt8
        | DataType::UInt16
        | DataType::UInt32 => {
            if scales.contains_key(name) {
                bail!("--scale {name}: only float columns can be scaled (integers are exact)");
            }
            Ok(0)
        }
        other => {
            bail!("column {name:?} has unsupported type {other:?} for a metric/int field")
        }
    }
}

/// Downcast an `ArrayRef` to a concrete Arrow array type. The type is dictated by
/// the column's `DataType`, so a failure is an internal invariant break.
fn downcast_array<'a, T: 'static>(array: &'a ArrayRef, name: &str) -> Result<&'a T> {
    array
        .as_any()
        .downcast_ref::<T>()
        .ok_or_else(|| anyhow::anyhow!("column {name:?}: unexpected Arrow array layout"))
}

/// Extract a doc's time as epoch seconds from a Parquet column.
fn extract_time(array: &ArrayRef, row: usize, name: &str) -> Result<i64> {
    if array.is_null(row) {
        bail!("row {row}: null in time column {name:?}");
    }
    let v = match array.data_type() {
        DataType::Timestamp(unit, _) => match unit {
            TimeUnit::Second => downcast_array::<TimestampSecondArray>(array, name)?.value(row),
            TimeUnit::Millisecond => {
                downcast_array::<TimestampMillisecondArray>(array, name)?.value(row) / 1_000
            }
            TimeUnit::Microsecond => {
                downcast_array::<TimestampMicrosecondArray>(array, name)?.value(row) / 1_000_000
            }
            TimeUnit::Nanosecond => {
                downcast_array::<TimestampNanosecondArray>(array, name)?.value(row) / 1_000_000_000
            }
        },
        DataType::Date32 => {
            i64::from(downcast_array::<Date32Array>(array, name)?.value(row)) * 86_400
        }
        DataType::Date64 => downcast_array::<Date64Array>(array, name)?.value(row) / 1_000,
        DataType::Int64 => downcast_array::<Int64Array>(array, name)?.value(row),
        DataType::Int32 => i64::from(downcast_array::<Int32Array>(array, name)?.value(row)),
        DataType::Utf8 => parse_time_str(downcast_array::<StringArray>(array, name)?.value(row))?,
        DataType::LargeUtf8 => {
            parse_time_str(downcast_array::<LargeStringArray>(array, name)?.value(row))?
        }
        other => bail!("time column {name:?} has unsupported type {other:?}"),
    };
    Ok(v)
}

/// Extract a doc's numeric value as a scaled `i64` (see module docs).
fn extract_numeric(array: &ArrayRef, row: usize, name: &str, scale: i8) -> Result<i64> {
    if array.is_null(row) {
        bail!("row {row}: null in numeric column {name:?}");
    }
    let v = match array.data_type() {
        DataType::Int64 => downcast_array::<Int64Array>(array, name)?.value(row),
        DataType::Int32 => i64::from(downcast_array::<Int32Array>(array, name)?.value(row)),
        DataType::Int16 => i64::from(downcast_array::<Int16Array>(array, name)?.value(row)),
        DataType::Int8 => i64::from(downcast_array::<Int8Array>(array, name)?.value(row)),
        DataType::UInt32 => i64::from(downcast_array::<UInt32Array>(array, name)?.value(row)),
        DataType::UInt16 => i64::from(downcast_array::<UInt16Array>(array, name)?.value(row)),
        DataType::UInt8 => i64::from(downcast_array::<UInt8Array>(array, name)?.value(row)),
        DataType::Decimal128(_, _) => {
            let mantissa = downcast_array::<Decimal128Array>(array, name)?.value(row);
            i64::try_from(mantissa).map_err(|_| {
                anyhow::anyhow!("row {row}: decimal column {name:?} value {mantissa} overflows i64")
            })?
        }
        DataType::Float64 => scale_float(
            downcast_array::<Float64Array>(array, name)?.value(row),
            scale,
        ),
        DataType::Float32 => scale_float(
            f64::from(downcast_array::<Float32Array>(array, name)?.value(row)),
            scale,
        ),
        other => bail!("column {name:?} has unsupported numeric type {other:?}"),
    };
    Ok(v)
}

/// Extract a doc's string term from a Parquet column.
fn extract_string<'a>(array: &'a ArrayRef, row: usize, name: &str) -> Result<&'a str> {
    if array.is_null(row) {
        bail!("row {row}: null in string column {name:?}");
    }
    match array.data_type() {
        DataType::Utf8 => Ok(downcast_array::<StringArray>(array, name)?.value(row)),
        DataType::LargeUtf8 => Ok(downcast_array::<LargeStringArray>(array, name)?.value(row)),
        other => bail!("string column {name:?} has unsupported type {other:?}"),
    }
}

/// `round(v × 10^scale)`, saturating to `i64` (Rust float→int casts saturate, so
/// pathological magnitudes clamp rather than wrap; exact for in-range values).
#[allow(clippy::cast_possible_truncation)]
fn scale_float(v: f64, scale: i8) -> i64 {
    (v * 10f64.powi(i32::from(scale))).round() as i64
}

/// Reject before opening the CSV — failing after parsing millions of
/// rows is a bad UX. `DiskShardWriter` also catches this via
/// `ensure_unused_name`, but only at finalize time.
fn check_no_duplicate_columns(opts: &IngestOptions) -> Result<()> {
    let mut seen = HashSet::new();
    for name in std::iter::once(&opts.time)
        .chain(&opts.metrics)
        .chain(&opts.strings)
        .chain(&opts.ints)
    {
        if !seen.insert(name.as_str()) {
            bail!("column {name:?} declared more than once");
        }
    }
    Ok(())
}

/// Numeric (`Metric` or `Int`) column accumulator. Both kinds parse
/// rows the same way (i64 forward column); `kind` tags them so
/// finalize knows which `DiskShardWriter` method to call.
struct NumericCol {
    name: String,
    col_idx: usize,
    values: Vec<i64>,
    kind: FieldKind,
    /// Decimal scale recorded for this column (0 for plain integers / CSV).
    scale: i8,
}

struct StringCol {
    name: String,
    col_idx: usize,
    postings: BTreeMap<String, RoaringBitmap>,
}

/// One in-progress shard's accumulators. Built from prototype
/// arrays so every bucket inherits the same names + column indexes;
/// data structures start empty.
struct BucketBuilder {
    numeric: Vec<NumericCol>,
    strings: Vec<StringCol>,
    /// Name of the time column (the `--time` header). Stored as an
    /// `Int` field at finalize so per-doc time is range-filterable.
    time_name: String,
    /// Parsed epoch-seconds time of each doc, in doc-id order. The
    /// shard's `(time_min, time_max)` is folded from this at finalize.
    time_values: Vec<i64>,
    doc_id: u32,
}

impl BucketBuilder {
    fn new(numeric_proto: &[NumericCol], strings_proto: &[StringCol], time_name: &str) -> Self {
        Self {
            numeric: numeric_proto
                .iter()
                .map(|c| NumericCol {
                    name: c.name.clone(),
                    col_idx: c.col_idx,
                    values: Vec::new(),
                    kind: c.kind,
                    scale: c.scale,
                })
                .collect(),
            strings: strings_proto
                .iter()
                .map(|s| StringCol {
                    name: s.name.clone(),
                    col_idx: s.col_idx,
                    postings: BTreeMap::new(),
                })
                .collect(),
            time_name: time_name.to_string(),
            time_values: Vec::new(),
            doc_id: 0,
        }
    }

    fn push_row(&mut self, row: &csv::StringRecord, line: u64, t: i64) -> Result<()> {
        self.time_values.push(t);

        // Accumulate directly (no per-row scratch Vec) — the Parquet path uses
        // `push_values` with already-extracted values instead.
        for col in &mut self.numeric {
            let raw = row
                .get(col.col_idx)
                .with_context(|| format!("line {line}: missing {} column", col.kind))?;
            let value: i64 = raw.parse().with_context(|| {
                format!(
                    "line {line}: {} column {:?} value {raw:?}: only int64 values are \
                     supported (multiply decimal values by 100 etc. and round)",
                    col.kind, col.name,
                )
            })?;
            col.values.push(value);
        }

        for col in &mut self.strings {
            let term = row
                .get(col.col_idx)
                .with_context(|| format!("line {line}: missing string column"))?;
            if let Some(bm) = col.postings.get_mut(term) {
                bm.insert(self.doc_id);
            } else {
                let mut bm = RoaringBitmap::new();
                bm.insert(self.doc_id);
                col.postings.insert(term.to_string(), bm);
            }
        }

        self.doc_id = self
            .doc_id
            .checked_add(1)
            .ok_or_else(|| anyhow::anyhow!("line {line}: doc ID overflow (> u32::MAX rows)"))?;
        Ok(())
    }

    /// Append one doc's already-extracted values: its time, the numeric forward
    /// columns (in proto order), then the string terms (in proto order). The
    /// accumulation step for the Parquet front-end (values arrive typed, not as
    /// text to parse).
    fn push_values(&mut self, t: i64, numeric: &[i64], strings: &[&str]) -> Result<()> {
        self.time_values.push(t);

        for (col, &value) in self.numeric.iter_mut().zip(numeric) {
            col.values.push(value);
        }

        for (col, &term) in self.strings.iter_mut().zip(strings) {
            // Look up first to avoid allocating a fresh String for terms
            // that already exist — low-cardinality columns repeat heavily.
            if let Some(bm) = col.postings.get_mut(term) {
                bm.insert(self.doc_id);
            } else {
                let mut bm = RoaringBitmap::new();
                bm.insert(self.doc_id);
                col.postings.insert(term.to_string(), bm);
            }
        }

        self.doc_id = self
            .doc_id
            .checked_add(1)
            .ok_or_else(|| anyhow::anyhow!("doc ID overflow (> u32::MAX rows)"))?;
        Ok(())
    }

    fn finalize(self, output_dir: &Path) -> Result<u64> {
        // Buckets are only created on first push_row, so doc_id is
        // always >= 1 here. Catch a refactor that breaks that
        // invariant before the (MAX, MIN) sentinel from an empty fold
        // reaches Metadata::validate.
        debug_assert!(self.doc_id > 0, "finalize called on empty BucketBuilder");
        let num_docs = u64::from(self.doc_id);
        let (time_min, time_max) = self
            .time_values
            .iter()
            .fold((i64::MAX, i64::MIN), |(lo, hi), &t| (lo.min(t), hi.max(t)));
        let mut writer = DiskShardWriter::new(output_dir, (time_min, time_max))
            .with_context(|| format!("create writer at {}", output_dir.display()))?;
        // The time column is stored as an Int field (forward column +
        // order-preserving index) so per-doc time-range filters push
        // down, and is marked as the shard's time field.
        writer
            .add_int_field(&self.time_name, self.time_values)
            .with_context(|| format!("add time field {:?}", self.time_name))?;
        writer.set_time_field(&self.time_name);
        for col in self.numeric {
            let name = col.name;
            let scale = col.scale;
            match col.kind {
                FieldKind::Metric => writer
                    .add_metric(&name, col.values)
                    .with_context(|| format!("add metric {name:?}"))?,
                FieldKind::Int => writer
                    .add_int_field(&name, col.values)
                    .with_context(|| format!("add int field {name:?}"))?,
                FieldKind::String => unreachable!("NumericCol only holds Metric or Int"),
            }
            writer.set_field_scale(&name, scale);
        }
        for col in self.strings {
            writer
                .add_string_field(&col.name, col.postings)
                .with_context(|| format!("add string field {:?}", col.name))?;
        }
        writer
            .finalize()
            .with_context(|| format!("finalize {}", output_dir.display()))?;
        Ok(num_docs)
    }
}

/// Which time parser matched. Cached across rows so the hot path skips
/// the failing alternatives.
#[derive(Debug, Clone, Copy)]
enum TimeFormat {
    Epoch,
    Rfc3339,
    Naive,
    Date,
}

/// Three-way time parsing: unix epoch seconds, RFC 3339, or
/// `YYYY-MM-DD HH:MM:SS` (NYC taxi style; many database exports).
///
/// When `hint` is `Some`, try that format first; fall through to the
/// full detection chain on miss so mixed-format inputs still work.
///
/// **Caveats** worth surfacing to users:
///
/// - Epoch is tried first. Bare integers like `"20231114"` (YYYYMMDD)
///   parse as a Unix epoch (~1970-08-22), not the intended date.
///   Preprocess such columns to epoch seconds or an ISO-8601 string.
/// - The `YYYY-MM-DD HH:MM:SS` form has no timezone and is treated as
///   UTC. Inputs in other time zones will be off by the local offset.
fn parse_time(s: &str, hint: Option<TimeFormat>) -> Result<(i64, TimeFormat)> {
    if let Some(h) = hint
        && let Some(t) = try_parse(s, h)
    {
        return Ok((t, h));
    }
    for fmt in [
        TimeFormat::Epoch,
        TimeFormat::Rfc3339,
        TimeFormat::Naive,
        TimeFormat::Date,
    ] {
        if let Some(t) = try_parse(s, fmt) {
            return Ok((t, fmt));
        }
    }
    bail!(
        "unrecognized time format (expected unix epoch seconds, RFC 3339, \
         'YYYY-MM-DD HH:MM:SS', or 'YYYY-MM-DD')"
    )
}

fn try_parse(s: &str, fmt: TimeFormat) -> Option<i64> {
    match fmt {
        TimeFormat::Epoch => s.parse::<i64>().ok(),
        TimeFormat::Rfc3339 => DateTime::parse_from_rfc3339(s)
            .ok()
            .map(|dt| dt.timestamp()),
        TimeFormat::Naive => NaiveDateTime::parse_from_str(s, "%Y-%m-%d %H:%M:%S")
            .ok()
            .map(|naive| naive.and_utc().timestamp()),
        TimeFormat::Date => NaiveDate::parse_from_str(s, "%Y-%m-%d")
            .ok()
            .and_then(|d| d.and_hms_opt(0, 0, 0))
            .map(|dt| dt.and_utc().timestamp()),
    }
}

/// One-shot CLI-friendly time parse. Wraps [`parse_time`] for callers
/// that don't need the format-hint cache (e.g. parsing `--from` /
/// `--to` arguments once per invocation).
pub fn parse_time_str(s: &str) -> Result<i64> {
    parse_time(s, None).map(|(t, _)| t)
}

/// Returns the start-of-day epoch if `s` is a bare `YYYY-MM-DD`
/// date, or `None` otherwise. Single-source the date format so
/// callers that want end-of-day semantics (e.g. `--to`'s
/// inclusive-day bump) don't have to re-parse with chrono themselves.
#[must_use]
pub fn parse_date_only(s: &str) -> Option<i64> {
    try_parse(s, TimeFormat::Date)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::shard::{DiskShard, Shard};

    #[test]
    fn ingest_parquet_maps_types_and_records_scale() {
        use arrow::array::RecordBatch;
        use arrow::datatypes::{Field, Schema};
        use parquet::arrow::ArrowWriter;

        use crate::shard::{METADATA_FILE, Metadata};

        let schema = Arc::new(Schema::new(vec![
            Field::new("ts", DataType::Timestamp(TimeUnit::Second, None), false),
            Field::new("vendor", DataType::Int64, false),
            Field::new("fare", DataType::Decimal128(10, 2), false),
            Field::new("dist", DataType::Float64, false),
            Field::new("payment", DataType::Utf8, false),
        ]));
        let batch = RecordBatch::try_new(
            Arc::clone(&schema),
            vec![
                Arc::new(TimestampSecondArray::from(vec![
                    1_700_000_000,
                    1_700_000_050,
                ])),
                Arc::new(Int64Array::from(vec![1_i64, 2])),
                Arc::new(
                    Decimal128Array::from(vec![1234_i128, 5678])
                        .with_precision_and_scale(10, 2)
                        .unwrap(),
                ),
                Arc::new(Float64Array::from(vec![1.5_f64, 2.25])),
                Arc::new(StringArray::from(vec!["card", "cash"])),
            ],
        )
        .unwrap();

        let dir = tempfile::tempdir().unwrap();
        let pq = dir.path().join("in.parquet");
        {
            let f = std::fs::File::create(&pq).unwrap();
            let mut w = ArrowWriter::try_new(f, schema, None).unwrap();
            w.write(&batch).unwrap();
            w.close().unwrap();
        }

        let mk_opts = || IngestOptions {
            time: "ts".into(),
            metrics: vec!["fare".into(), "dist".into()],
            strings: vec!["payment".into()],
            ints: vec!["vendor".into()],
            delimiter: b',',
            shard_by: ShardBy::None,
        };

        // Default float scale (3); decimal scale from the schema (2); int scale 0.
        let out = dir.path().join("out");
        let n = ingest_parquet(&pq, &out, &mk_opts(), &BTreeMap::new()).unwrap();
        assert_eq!(n, 2);
        let shard = DiskShard::open(&out).unwrap();
        assert_eq!(shard.forward_column("vendor").unwrap(), &[1, 2]);
        assert_eq!(shard.forward_column("fare").unwrap(), &[1234, 5678]); // decimal mantissa
        assert_eq!(shard.forward_column("dist").unwrap(), &[1500, 2250]); // float × 10^3
        let meta: Metadata =
            serde_json::from_reader(std::fs::File::open(out.join(METADATA_FILE)).unwrap()).unwrap();
        let scale = |name: &str| meta.fields.iter().find(|f| f.name == name).unwrap().scale;
        assert_eq!(scale("fare"), 2);
        assert_eq!(scale("dist"), 3);
        assert_eq!(scale("vendor"), 0);

        // --scale override: dist at scale 2 → × 10^2.
        let out2 = dir.path().join("out2");
        let scales = BTreeMap::from([("dist".to_string(), 2_i8)]);
        ingest_parquet(&pq, &out2, &mk_opts(), &scales).unwrap();
        let shard2 = DiskShard::open(&out2).unwrap();
        assert_eq!(shard2.forward_column("dist").unwrap(), &[150, 225]);

        // --scale on a decimal column is rejected (schema scale is authoritative).
        let bad = BTreeMap::from([("fare".to_string(), 2_i8)]);
        assert!(ingest_parquet(&pq, &dir.path().join("out3"), &mk_opts(), &bad).is_err());
    }

    fn write_csv(dir: &Path, contents: &str) -> std::path::PathBuf {
        let path = dir.join("input.csv");
        std::fs::write(&path, contents).expect("write csv");
        path
    }

    fn opts(time: &str, metrics: &[&str], strings: &[&str]) -> IngestOptions {
        IngestOptions {
            time: time.to_string(),
            metrics: metrics.iter().map(|s| (*s).to_string()).collect(),
            strings: strings.iter().map(|s| (*s).to_string()).collect(),
            ints: Vec::new(),
            delimiter: b',',
            shard_by: ShardBy::None,
        }
    }

    fn opts_daily(time: &str, metrics: &[&str], strings: &[&str]) -> IngestOptions {
        IngestOptions {
            shard_by: ShardBy::Bucket { seconds: 86_400 },
            ..opts(time, metrics, strings)
        }
    }

    fn opts_hourly(time: &str, metrics: &[&str], strings: &[&str]) -> IngestOptions {
        IngestOptions {
            shard_by: ShardBy::Bucket { seconds: 3600 },
            ..opts(time, metrics, strings)
        }
    }

    fn opts_weekly(time: &str, metrics: &[&str], strings: &[&str]) -> IngestOptions {
        IngestOptions {
            shard_by: ShardBy::Bucket { seconds: 604_800 },
            ..opts(time, metrics, strings)
        }
    }

    fn opts_monthly(time: &str, metrics: &[&str], strings: &[&str]) -> IngestOptions {
        IngestOptions {
            shard_by: ShardBy::Month,
            ..opts(time, metrics, strings)
        }
    }

    /// Epoch seconds for `YYYY-MM-DD` at UTC midnight (test helper).
    fn day_epoch(date: &str) -> i64 {
        NaiveDate::parse_from_str(date, "%Y-%m-%d")
            .unwrap()
            .and_hms_opt(0, 0, 0)
            .unwrap()
            .and_utc()
            .timestamp()
    }

    #[test]
    fn ingest_csv_round_trip_minimal() {
        let tmp = tempfile::tempdir().expect("tmpdir");
        let input = write_csv(
            tmp.path(),
            "pickup,vendor,passengers\n\
             1700000000,A,1\n\
             1700000300,B,2\n\
             1700000600,A,3\n",
        );
        let shard_dir = tmp.path().join("shard");
        let n = ingest_csv(
            &input,
            &shard_dir,
            &opts("pickup", &["passengers"], &["vendor"]),
        )
        .expect("ingest");
        assert_eq!(n, 3);

        let shard = DiskShard::open(&shard_dir).expect("open");
        assert_eq!(shard.num_docs(), 3);
        assert_eq!(shard.time_range(), (1_700_000_000, 1_700_000_600));
        assert_eq!(shard.forward_column("passengers").unwrap(), &[1, 2, 3]);
        let idx = shard.inverted_index("vendor").expect("vendor index");
        assert_eq!(idx.num_terms(), 2);
        assert_eq!(idx.lookup("A").unwrap().len(), 2);
        assert_eq!(idx.lookup("B").unwrap().len(), 1);
    }

    #[test]
    fn ingest_csv_drops_undeclared_columns() {
        let tmp = tempfile::tempdir().expect("tmpdir");
        let input = write_csv(
            tmp.path(),
            "pickup,a,b,c,d\n\
             1,10,20,30,40\n\
             2,11,21,31,41\n",
        );
        let shard_dir = tmp.path().join("shard");
        ingest_csv(&input, &shard_dir, &opts("pickup", &["a", "c"], &[])).expect("ingest");

        let shard = DiskShard::open(&shard_dir).expect("open");
        let names: Vec<&str> = shard
            .metadata()
            .fields
            .iter()
            .map(|f| f.name.as_str())
            .collect();
        // Declared metrics kept, undeclared `b`/`d` dropped, and the
        // time column `pickup` stored as a field (the new time field).
        assert!(names.contains(&"a"));
        assert!(names.contains(&"c"));
        assert!(names.contains(&"pickup"));
        assert!(!names.contains(&"b"));
        assert!(!names.contains(&"d"));
        assert_eq!(shard.metadata().time_field.as_deref(), Some("pickup"));
    }

    #[test]
    fn ingest_csv_parses_unix_epoch_time() {
        assert_eq!(parse_time("1700000000", None).unwrap().0, 1_700_000_000);
        assert_eq!(parse_time("-5", None).unwrap().0, -5);
    }

    #[test]
    fn ingest_csv_parses_rfc3339_time() {
        // 2023-11-14T22:13:20Z is 1_700_000_000.
        assert_eq!(
            parse_time("2023-11-14T22:13:20Z", None).unwrap().0,
            1_700_000_000
        );
        assert_eq!(
            parse_time("2023-11-14T22:13:20+00:00", None).unwrap().0,
            1_700_000_000
        );
    }

    #[test]
    fn ingest_csv_parses_date_only_as_start_of_day() {
        // Date-only inputs parse as start-of-day UTC. Used by `t9n
        // query --from 2023-01-08`-style CLI args.
        assert_eq!(parse_time("2023-01-01", None).unwrap().0, DAY_0);
        // 2023-11-14T00:00:00Z
        assert_eq!(parse_time("2023-11-14", None).unwrap().0, 1_699_920_000);
        // parse_time_str (the CLI-friendly wrapper) returns the
        // same value.
        assert_eq!(parse_time_str("2023-01-01").unwrap(), DAY_0);
    }

    #[test]
    fn ingest_csv_parses_yyyy_mm_dd_hh_mm_ss_time() {
        // NYC taxi / typical database export format.
        assert_eq!(
            parse_time("2023-11-14 22:13:20", None).unwrap().0,
            1_700_000_000
        );
    }

    #[test]
    fn ingest_csv_rejects_unrecognized_time_format() {
        let err = parse_time("not-a-time", None).expect_err("expected time-parse error");
        let msg = err.to_string();
        assert!(msg.contains("unrecognized time format"), "{msg}");
    }

    #[test]
    fn parse_time_cache_falls_back_on_mismatch() {
        // Cached as Epoch from prior row, but this row is RFC 3339: must
        // still parse correctly via the full-detection fallback.
        let (t, fmt) = parse_time("2023-11-14T22:13:20Z", Some(TimeFormat::Epoch)).unwrap();
        assert_eq!(t, 1_700_000_000);
        assert!(matches!(fmt, TimeFormat::Rfc3339));
    }

    #[test]
    fn ingest_csv_rejects_non_integer_metric() {
        let tmp = tempfile::tempdir().expect("tmpdir");
        let input = write_csv(
            tmp.path(),
            "pickup,fare\n\
             1,12.50\n",
        );
        let shard_dir = tmp.path().join("shard");
        let err = ingest_csv(&input, &shard_dir, &opts("pickup", &["fare"], &[]))
            .expect_err("expected non-integer rejection");
        let msg = format!("{err:#}");
        assert!(msg.contains("fare"), "{msg}");
        assert!(msg.contains("int64"), "{msg}");
    }

    #[test]
    fn ingest_csv_rejects_empty_input() {
        let tmp = tempfile::tempdir().expect("tmpdir");
        let input = write_csv(tmp.path(), "pickup,fare\n");
        let shard_dir = tmp.path().join("shard");
        let err = ingest_csv(&input, &shard_dir, &opts("pickup", &["fare"], &[]))
            .expect_err("expected empty-input rejection");
        let msg = err.to_string();
        assert!(msg.contains("no data rows"), "{msg}");
    }

    #[test]
    fn ingest_csv_rejects_duplicate_column_declarations() {
        let tmp = tempfile::tempdir().expect("tmpdir");
        // The CSV is never opened — the check fires before that.
        let input = tmp.path().join("does-not-need-to-exist.csv");
        let shard_dir = tmp.path().join("shard");

        let err = ingest_csv(
            &input,
            &shard_dir,
            &opts("pickup", &["clicks", "clicks"], &[]),
        )
        .expect_err("expected duplicate-column rejection");
        let msg = err.to_string();
        assert!(
            msg.contains("clicks") && msg.contains("more than once"),
            "{msg}"
        );

        // Overlap between metric and string flags is also rejected.
        let err = ingest_csv(&input, &shard_dir, &opts("pickup", &["x"], &["x"]))
            .expect_err("expected duplicate-column rejection across kinds");
        let msg = err.to_string();
        assert!(
            msg.contains("\"x\"") && msg.contains("more than once"),
            "{msg}"
        );

        // Same column as both time and metric is also rejected.
        let err = ingest_csv(&input, &shard_dir, &opts("pickup", &["pickup"], &[]))
            .expect_err("expected duplicate-column rejection (time vs metric)");
        let msg = err.to_string();
        assert!(
            msg.contains("pickup") && msg.contains("more than once"),
            "{msg}"
        );
    }

    #[test]
    fn ingest_csv_rejects_unknown_declared_column() {
        let tmp = tempfile::tempdir().expect("tmpdir");
        let input = write_csv(
            tmp.path(),
            "pickup,a\n\
             1,2\n",
        );
        let shard_dir = tmp.path().join("shard");
        let err = ingest_csv(&input, &shard_dir, &opts("pickup", &["not_a_column"], &[]))
            .expect_err("expected unknown-column rejection");
        let msg = err.to_string();
        assert!(msg.contains("not_a_column"), "{msg}");
    }

    #[test]
    fn ingest_csv_int_field_round_trip() {
        // CSV with an int field declared via --int. Verify that
        // both the forward column AND the inverted index land on
        // disk and behave correctly.
        let tmp = tempfile::tempdir().expect("tmpdir");
        let input = write_csv(
            tmp.path(),
            "pickup,vendor_id,fare_cents\n\
             1700000000,1,2500\n\
             1700000300,2,3700\n\
             1700000600,1,4500\n\
             1700000900,3,2200\n",
        );
        let shard_dir = tmp.path().join("shard");
        let opts = IngestOptions {
            time: "pickup".to_string(),
            metrics: vec!["fare_cents".to_string()],
            strings: Vec::new(),
            ints: vec!["vendor_id".to_string()],
            delimiter: b',',
            shard_by: ShardBy::None,
        };
        ingest_csv(&input, &shard_dir, &opts).expect("ingest");

        let shard = crate::shard::DiskShard::open(&shard_dir).expect("open");
        // Forward column carries the raw values.
        assert_eq!(shard.forward_column("vendor_id").unwrap(), &[1, 2, 1, 3]);
        // Inverted index has one bitmap per distinct value.
        let idx = shard.inverted_index("vendor_id").expect("vendor_id index");
        assert_eq!(idx.num_terms(), 3);
        let v1 = idx
            .lookup_bytes(&crate::shard::encode_int_key(1))
            .expect("vendor 1");
        assert_eq!(v1.len(), 2);
    }

    #[test]
    fn ingest_csv_tracks_min_and_max_time_across_rows() {
        let tmp = tempfile::tempdir().expect("tmpdir");
        let input = write_csv(
            tmp.path(),
            "pickup,x\n\
             500,1\n\
             100,2\n\
             900,3\n\
             300,4\n",
        );
        let shard_dir = tmp.path().join("shard");
        ingest_csv(&input, &shard_dir, &opts("pickup", &["x"], &[])).expect("ingest");
        let shard = DiskShard::open(&shard_dir).expect("open");
        assert_eq!(shard.time_range(), (100, 900));
    }

    // 2023-01-01T00:00:00Z, 2023-01-02T00:00:00Z, 2023-01-03T00:00:00Z
    // are 1672531200, 1672617600, 1672704000.
    const DAY_0: i64 = 1_672_531_200;
    const DAY_1: i64 = 1_672_617_600;
    const DAY_2: i64 = 1_672_704_000;

    #[test]
    fn ingest_csv_shard_by_daily_produces_one_shard_per_day() {
        let tmp = tempfile::tempdir().expect("tmpdir");
        let input = write_csv(
            tmp.path(),
            &format!(
                "pickup,x\n\
                 {DAY_0},10\n\
                 {d0_late},20\n\
                 {DAY_1},30\n\
                 {DAY_2},40\n\
                 {DAY_2},50\n",
                d0_late = DAY_0 + 3600,
            ),
        );
        let root = tmp.path().join("dataset");
        let n = ingest_csv(&input, &root, &opts_daily("pickup", &["x"], &[])).expect("ingest");
        assert_eq!(n, 5);

        assert!(root.join("2023-01-01").is_dir(), "{root:?}");
        assert!(root.join("2023-01-02").is_dir());
        assert!(root.join("2023-01-03").is_dir());

        let s0 = DiskShard::open(&root.join("2023-01-01")).expect("open day 0");
        assert_eq!(s0.num_docs(), 2);
        assert_eq!(s0.forward_column("x").unwrap(), &[10, 20]);
        let s1 = DiskShard::open(&root.join("2023-01-02")).expect("open day 1");
        assert_eq!(s1.num_docs(), 1);
        assert_eq!(s1.forward_column("x").unwrap(), &[30]);
        let s2 = DiskShard::open(&root.join("2023-01-03")).expect("open day 2");
        assert_eq!(s2.num_docs(), 2);
        assert_eq!(s2.forward_column("x").unwrap(), &[40, 50]);
    }

    #[test]
    fn ingest_csv_shard_by_hourly_produces_one_shard_per_hour() {
        let tmp = tempfile::tempdir().expect("tmpdir");
        let input = write_csv(
            tmp.path(),
            &format!(
                "pickup,x\n\
                 {DAY_0},1\n\
                 {h1},2\n\
                 {h2},3\n",
                h1 = DAY_0 + 3600,
                h2 = DAY_0 + 7200,
            ),
        );
        let root = tmp.path().join("dataset");
        ingest_csv(&input, &root, &opts_hourly("pickup", &["x"], &[])).expect("ingest");

        assert!(root.join("2023-01-01T00").is_dir(), "{root:?}");
        assert!(root.join("2023-01-01T01").is_dir());
        assert!(root.join("2023-01-01T02").is_dir());
    }

    #[test]
    fn ingest_csv_shard_by_weekly_produces_one_shard_per_week() {
        // Epoch weeks align to Thursday; 2023-01-05 is a Thursday and an
        // exact 604800s boundary, so it is a week start.
        let tmp = tempfile::tempdir().expect("tmpdir");
        let input = write_csv(
            tmp.path(),
            &format!(
                "pickup,x\n\
                 {w0_start},10\n\
                 {w0_mid},20\n\
                 {w1_start},30\n",
                w0_start = day_epoch("2023-01-05"),
                w0_mid = day_epoch("2023-01-10"), // same week (Thu..Wed)
                w1_start = day_epoch("2023-01-12"), // next week
            ),
        );
        let root = tmp.path().join("dataset");
        let n = ingest_csv(&input, &root, &opts_weekly("pickup", &["x"], &[])).expect("ingest");
        assert_eq!(n, 3);

        assert!(root.join("2023-01-05").is_dir(), "{root:?}");
        assert!(root.join("2023-01-12").is_dir());
        let w0 = DiskShard::open(&root.join("2023-01-05")).expect("open week 0");
        assert_eq!(w0.num_docs(), 2);
        assert_eq!(w0.forward_column("x").unwrap(), &[10, 20]);
        let w1 = DiskShard::open(&root.join("2023-01-12")).expect("open week 1");
        assert_eq!(w1.num_docs(), 1);
        assert_eq!(w1.forward_column("x").unwrap(), &[30]);
    }

    #[test]
    fn ingest_csv_shard_by_monthly_produces_one_shard_per_calendar_month() {
        // Calendar months (not fixed-width): Jan rows share a shard;
        // Feb and Dec are distinct, including across the year.
        let tmp = tempfile::tempdir().expect("tmpdir");
        let input = write_csv(
            tmp.path(),
            &format!(
                "pickup,x\n\
                 {jan_a},10\n\
                 {jan_b},20\n\
                 {feb},30\n\
                 {dec},40\n",
                jan_a = day_epoch("2023-01-05"),
                jan_b = day_epoch("2023-01-28"),
                feb = day_epoch("2023-02-03"),
                dec = day_epoch("2023-12-31"),
            ),
        );
        let root = tmp.path().join("dataset");
        let n = ingest_csv(&input, &root, &opts_monthly("pickup", &["x"], &[])).expect("ingest");
        assert_eq!(n, 4);

        assert!(root.join("2023-01").is_dir(), "{root:?}");
        assert!(root.join("2023-02").is_dir());
        assert!(root.join("2023-12").is_dir());
        let jan = DiskShard::open(&root.join("2023-01")).expect("open jan");
        assert_eq!(jan.num_docs(), 2);
        assert_eq!(jan.forward_column("x").unwrap(), &[10, 20]);
        let feb = DiskShard::open(&root.join("2023-02")).expect("open feb");
        assert_eq!(feb.num_docs(), 1);
    }

    #[test]
    fn ingest_csv_shard_by_daily_handles_midnight_boundary() {
        // A row at exactly the start of a day belongs to that day,
        // not the previous one (div_euclid semantics).
        let tmp = tempfile::tempdir().expect("tmpdir");
        let input = write_csv(
            tmp.path(),
            &format!(
                "pickup,x\n\
                 {last_sec_of_day_0},1\n\
                 {DAY_1},2\n",
                last_sec_of_day_0 = DAY_1 - 1,
            ),
        );
        let root = tmp.path().join("dataset");
        ingest_csv(&input, &root, &opts_daily("pickup", &["x"], &[])).expect("ingest");
        assert!(root.join("2023-01-01").is_dir(), "{root:?}");
        assert!(root.join("2023-01-02").is_dir());

        let s0 = DiskShard::open(&root.join("2023-01-01")).expect("open");
        assert_eq!(s0.num_docs(), 1);
        let s1 = DiskShard::open(&root.join("2023-01-02")).expect("open");
        assert_eq!(s1.num_docs(), 1);
    }

    #[test]
    fn ingest_csv_shard_by_isolates_string_postings() {
        // Term "us" appears only in day-0 rows; "de" only in day-1.
        // Each shard's inverted index should reflect only its own
        // bucket, with the other term absent.
        let tmp = tempfile::tempdir().expect("tmpdir");
        let input = write_csv(
            tmp.path(),
            &format!(
                "pickup,country,x\n\
                 {DAY_0},us,10\n\
                 {DAY_0},us,20\n\
                 {DAY_1},de,30\n\
                 {DAY_1},de,40\n",
            ),
        );
        let root = tmp.path().join("dataset");
        ingest_csv(&input, &root, &opts_daily("pickup", &["x"], &["country"])).expect("ingest");

        let s0 = DiskShard::open(&root.join("2023-01-01")).expect("open");
        let idx0 = s0.inverted_index("country").expect("country idx");
        assert!(idx0.lookup("us").is_some(), "us in day 0");
        assert!(idx0.lookup("de").is_none(), "de absent from day 0");

        let s1 = DiskShard::open(&root.join("2023-01-02")).expect("open");
        let idx1 = s1.inverted_index("country").expect("country idx");
        assert!(idx1.lookup("de").is_some(), "de in day 1");
        assert!(idx1.lookup("us").is_none(), "us absent from day 1");
    }

    #[test]
    fn ingest_csv_shard_by_none_unchanged() {
        // Round-trip: with shard_by=None (the default opts()), the
        // shard lands directly at `output`, not under a date subdir.
        let tmp = tempfile::tempdir().expect("tmpdir");
        let input = write_csv(
            tmp.path(),
            &format!(
                "pickup,x\n\
                 {DAY_0},10\n\
                 {DAY_2},20\n",
            ),
        );
        let shard_dir = tmp.path().join("shard");
        ingest_csv(&input, &shard_dir, &opts("pickup", &["x"], &[])).expect("ingest");
        let shard = DiskShard::open(&shard_dir).expect("open");
        assert_eq!(shard.num_docs(), 2);
        assert_eq!(shard.time_range(), (DAY_0, DAY_2));
    }

    #[test]
    fn ingest_destination_parse_routes_paths_and_urls() {
        assert!(matches!(
            IngestDestination::parse("/tmp/foo").unwrap(),
            IngestDestination::Local(p) if p == Path::new("/tmp/foo")
        ));
        assert!(matches!(
            IngestDestination::parse("file:///tmp/foo").unwrap(),
            IngestDestination::Local(p) if p == Path::new("/tmp/foo")
        ));
        assert!(matches!(
            IngestDestination::parse("s3://bucket/dataset").unwrap(),
            IngestDestination::Remote(u) if u == "s3://bucket/dataset"
        ));
        assert!(matches!(
            IngestDestination::parse("memory:///foo").unwrap(),
            IngestDestination::Remote(u) if u == "memory:///foo"
        ));
    }

    fn tempdir_url(dir: &tempfile::TempDir) -> String {
        url::Url::from_directory_path(dir.path())
            .expect("tempdir is an absolute path")
            .to_string()
    }

    #[tokio::test]
    async fn upload_ingest_tree_uploads_payload_and_metadata() {
        let tmp = tempfile::tempdir().expect("tmpdir");
        let input = write_csv(
            tmp.path(),
            &format!(
                "pickup,fare\n\
                 {DAY_0},100\n\
                 {DAY_0},200\n",
            ),
        );
        let staging = tempfile::tempdir().expect("staging tmpdir");
        ingest_csv(&input, staging.path(), &opts("pickup", &["fare"], &[])).expect("ingest");

        let remote = tempfile::tempdir().expect("remote tmpdir");
        let url = tempdir_url(&remote);
        upload_ingest_tree(staging.path(), &url)
            .await
            .expect("upload");

        assert!(remote.path().join("metadata.json").is_file());
        assert!(remote.path().join("metrics.arrow").is_file());
    }

    #[tokio::test]
    async fn upload_ingest_tree_handles_shard_by_daily() {
        let tmp = tempfile::tempdir().expect("tmpdir");
        let input = write_csv(
            tmp.path(),
            &format!(
                "pickup,x\n\
                 {DAY_0},10\n\
                 {DAY_1},20\n\
                 {DAY_2},30\n",
            ),
        );
        let staging = tempfile::tempdir().expect("staging tmpdir");
        ingest_csv(&input, staging.path(), &opts_daily("pickup", &["x"], &[])).expect("ingest");

        let remote = tempfile::tempdir().expect("remote tmpdir");
        let url = tempdir_url(&remote);
        upload_ingest_tree(staging.path(), &url)
            .await
            .expect("upload");

        for day in ["2023-01-01", "2023-01-02", "2023-01-03"] {
            assert!(
                remote.path().join(day).join("metadata.json").is_file(),
                "{day}/metadata.json present"
            );
        }
    }

    #[tokio::test]
    async fn upload_ingest_tree_yields_queryable_dataset() {
        // End-to-end: ingest CSV → upload → query reads back the
        // expected aggregate.
        let tmp = tempfile::tempdir().expect("tmpdir");
        let input = write_csv(
            tmp.path(),
            &format!(
                "pickup,fare\n\
                 {DAY_0},100\n\
                 {DAY_0},200\n\
                 {DAY_0},300\n",
            ),
        );

        let staging = tempfile::tempdir().expect("staging");
        ingest_csv(&input, staging.path(), &opts("pickup", &["fare"], &[])).expect("ingest");

        let remote = tempfile::tempdir().expect("remote");
        let url = tempdir_url(&remote);
        upload_ingest_tree(staging.path(), &url)
            .await
            .expect("upload");

        let registry = crate::storage::StorageRegistry::from_url(&url).unwrap();
        let cache_dir = tempfile::tempdir().expect("cache dir");
        let cache = crate::cache::Cache::open(
            cache_dir.path().to_path_buf(),
            registry.store(),
            url.clone(),
            10 * 1024 * 1024,
            crate::cache::Validation::Trust,
        )
        .expect("cache");

        let output = crate::shard_source::query_dataset(&url, &cache, &[], &["fare"], None)
            .await
            .expect("query");
        match output.outcome {
            crate::shard_source::DatasetQueryOutcome::Scanned { result, .. } => {
                assert_eq!(result.matched, 3);
                assert_eq!(result.aggregates[0].sum, 600);
            }
            crate::shard_source::DatasetQueryOutcome::NoShards => panic!("expected scan"),
        }
    }

    #[tokio::test]
    async fn upload_file_round_trip_above_multipart_threshold() {
        let tmp = tempfile::tempdir().expect("tmpdir");
        let local = tmp.path().join("big.bin");
        let size = usize::try_from(SINGLE_PUT_THRESHOLD).expect("fits") + 1024;
        let payload: Vec<u8> = (0..size)
            .map(|i| u8::try_from(i % 251).expect("< 251"))
            .collect();
        tokio::fs::write(&local, &payload).await.expect("write big");

        let remote = tempfile::tempdir().expect("remote tmpdir");
        let url = tempdir_url(&remote);
        let registry = crate::storage::StorageRegistry::from_url(&url).expect("registry");
        let store = registry.store();

        upload_file(&*store, &local, &ObjPath::from("big.bin"))
            .await
            .expect("upload");

        let got = tokio::fs::read(remote.path().join("big.bin"))
            .await
            .expect("read back");
        assert_eq!(got.len(), payload.len());
        assert_eq!(got, payload);
    }

    #[tokio::test]
    async fn upload_file_round_trip_below_multipart_threshold() {
        let tmp = tempfile::tempdir().expect("tmpdir");
        let local = tmp.path().join("small.bin");
        let payload: Vec<u8> = (0..1024_usize)
            .map(|i| u8::try_from(i % 251).expect("< 251"))
            .collect();
        tokio::fs::write(&local, &payload)
            .await
            .expect("write small");

        let remote = tempfile::tempdir().expect("remote tmpdir");
        let url = tempdir_url(&remote);
        let registry = crate::storage::StorageRegistry::from_url(&url).expect("registry");
        let store = registry.store();

        upload_file(&*store, &local, &ObjPath::from("small.bin"))
            .await
            .expect("upload");

        let got = tokio::fs::read(remote.path().join("small.bin"))
            .await
            .expect("read back");
        assert_eq!(got, payload);
    }
}
