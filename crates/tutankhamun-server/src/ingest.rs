//! CSV/TSV → shard ingester.
//!
//! [`ingest_csv`] reads a delimited text file, parses each row, and
//! feeds the declared columns into [`crate::shard::DiskShardWriter`].
//! Columns not declared in [`IngestOptions`] are silently dropped.
//!
//! Used by the `t9n ingest` CLI verb.

use std::collections::{BTreeMap, HashSet};
use std::path::{Path, PathBuf};

use anyhow::{Context, Result, bail};
use chrono::{DateTime, NaiveDate, NaiveDateTime};
use roaring::RoaringBitmap;

use crate::shard::DiskShardWriter;

/// What to extract from each row, by column name.
#[derive(Debug, Clone)]
pub struct IngestOptions {
    /// Header name of the column to parse as the doc's time.
    pub time: String,
    /// Header names of columns to store as int64 metric forward columns.
    pub metrics: Vec<String>,
    /// Header names of columns to store as string-field inverted indexes.
    pub strings: Vec<String>,
    /// Field delimiter — `,` for CSV, `\t` for TSV.
    pub delimiter: u8,
    /// How to partition rows into shards. Defaults to one shard per
    /// ingest (`ShardBy::None`).
    pub shard_by: ShardBy,
}

/// How rows are partitioned into shards. The internal model is
/// duration-based (`Bucket { seconds }`); the CLI exposes named
/// aliases (`daily` → 86400, `hourly` → 3600) but the engine accepts
/// any positive bucket size — adding `--shard-by 6h` later is a
/// CLI-parser change only.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub enum ShardBy {
    /// All rows into a single shard at the `--output` path.
    #[default]
    None,
    /// One shard per `seconds`-wide UTC time bucket.
    Bucket { seconds: i64 },
}

impl ShardBy {
    /// Integer bucket key for an epoch-seconds time. Equal keys land
    /// in the same shard. `div_euclid` (not `/`) so pre-1970 inputs
    /// floor toward negative infinity correctly.
    fn bucket_key(self, epoch: i64) -> i64 {
        match self {
            Self::None => 0,
            Self::Bucket { seconds } => epoch.div_euclid(seconds),
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
                    86_400 => "%Y-%m-%d",
                    3600 => "%Y-%m-%dT%H",
                    _ => panic!(
                        "ShardBy::output_dir has no dirname format defined for {seconds}s \
                         buckets — extend the match before exposing this granularity on the CLI"
                    ),
                };
                root.join(dt.format(fmt).to_string())
            }
        }
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
    let metrics_proto: Vec<MetricCol> = opts
        .metrics
        .iter()
        .map(|name| {
            Ok(MetricCol {
                name: name.clone(),
                col_idx: find(name)?,
                values: Vec::new(),
            })
        })
        .collect::<Result<_>>()?;
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
            .or_insert_with(|| BucketBuilder::new(&metrics_proto, &strings_proto));
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

/// Reject before opening the CSV — failing after parsing millions of
/// rows is a bad UX. `DiskShardWriter` also catches this via
/// `ensure_unused_name`, but only at finalize time.
fn check_no_duplicate_columns(opts: &IngestOptions) -> Result<()> {
    let mut seen = HashSet::new();
    for name in std::iter::once(&opts.time)
        .chain(&opts.metrics)
        .chain(&opts.strings)
    {
        if !seen.insert(name.as_str()) {
            bail!("column {name:?} declared more than once");
        }
    }
    Ok(())
}

struct MetricCol {
    name: String,
    col_idx: usize,
    values: Vec<i64>,
}

struct StringCol {
    name: String,
    col_idx: usize,
    postings: BTreeMap<String, RoaringBitmap>,
}

/// One in-progress shard's accumulators. Built from prototype
/// `MetricCol` / `StringCol` arrays so every bucket inherits the same
/// names + column indexes; data structures start empty.
struct BucketBuilder {
    metrics: Vec<MetricCol>,
    strings: Vec<StringCol>,
    time_min: i64,
    time_max: i64,
    doc_id: u32,
}

impl BucketBuilder {
    fn new(metrics_proto: &[MetricCol], strings_proto: &[StringCol]) -> Self {
        Self {
            metrics: metrics_proto
                .iter()
                .map(|m| MetricCol {
                    name: m.name.clone(),
                    col_idx: m.col_idx,
                    values: Vec::new(),
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
            time_min: i64::MAX,
            time_max: i64::MIN,
            doc_id: 0,
        }
    }

    fn push_row(&mut self, row: &csv::StringRecord, line: u64, t: i64) -> Result<()> {
        self.time_min = self.time_min.min(t);
        self.time_max = self.time_max.max(t);

        for col in &mut self.metrics {
            let raw = row
                .get(col.col_idx)
                .with_context(|| format!("line {line}: missing metric column"))?;
            let value: i64 = raw.parse().with_context(|| {
                format!(
                    "line {line}: metric column {:?} value {raw:?}: only int64 values are \
                     supported (multiply decimal values by 100 etc. and round)",
                    col.name,
                )
            })?;
            col.values.push(value);
        }

        for col in &mut self.strings {
            let term = row
                .get(col.col_idx)
                .with_context(|| format!("line {line}: missing string column"))?;
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
            .ok_or_else(|| anyhow::anyhow!("line {line}: doc ID overflow (> u32::MAX rows)"))?;
        Ok(())
    }

    fn finalize(self, output_dir: &Path) -> Result<u64> {
        // Buckets are only created on first push_row, so doc_id is
        // always >= 1 here. Catch a refactor that breaks that
        // invariant before time_min/time_max (MAX/MIN) reach
        // Metadata::validate.
        debug_assert!(self.doc_id > 0, "finalize called on empty BucketBuilder");
        let num_docs = u64::from(self.doc_id);
        let mut writer = DiskShardWriter::new(output_dir, (self.time_min, self.time_max))
            .with_context(|| format!("create writer at {}", output_dir.display()))?;
        for col in self.metrics {
            writer
                .add_metric(&col.name, col.values)
                .with_context(|| format!("add metric {:?}", col.name))?;
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
    use crate::shard::{DiskShard, FieldKind, Shard};

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
        assert_eq!(names, vec!["a", "c"]);
        assert!(matches!(shard.metadata().fields[0].kind, FieldKind::Metric));
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
}
