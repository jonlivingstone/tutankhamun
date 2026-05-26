//! CSV/TSV → shard ingester.
//!
//! [`ingest_csv`] reads a delimited text file, parses each row, and
//! feeds the declared columns into [`crate::shard::DiskShardWriter`].
//! Columns not declared in [`IngestOptions`] are silently dropped.
//!
//! Used by the `t9n ingest` CLI verb.

use std::collections::{BTreeMap, HashSet};
use std::path::Path;

use anyhow::{Context, Result, bail};
use chrono::{DateTime, NaiveDateTime};
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
    let mut metrics: Vec<MetricCol> = opts
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
    let mut strings: Vec<StringCol> = opts
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

    let mut time_min = i64::MAX;
    let mut time_max = i64::MIN;
    let mut doc_id: u32 = 0;
    // Caches the successful time format after the first row so subsequent
    // rows skip the failing-parser attempts. Mixed-format inputs still
    // work via the full-detection fallback inside parse_time.
    let mut time_hint: Option<TimeFormat> = None;

    for record in reader.records() {
        let row = record.with_context(|| format!("read {}", input.display()))?;
        let line = row.position().map_or(0, csv::Position::line);

        let time_str = row
            .get(time_idx)
            .with_context(|| format!("line {line}: missing time column"))?;
        let (t, fmt) = parse_time(time_str, time_hint)
            .with_context(|| format!("line {line}: time column {time_str:?}"))?;
        time_hint = Some(fmt);
        time_min = time_min.min(t);
        time_max = time_max.max(t);

        for col in &mut metrics {
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

        for col in &mut strings {
            let term = row
                .get(col.col_idx)
                .with_context(|| format!("line {line}: missing string column"))?;
            // Look up first to avoid allocating a fresh String for terms
            // that already exist — low-cardinality columns repeat heavily.
            if let Some(bm) = col.postings.get_mut(term) {
                bm.insert(doc_id);
            } else {
                let mut bm = RoaringBitmap::new();
                bm.insert(doc_id);
                col.postings.insert(term.to_string(), bm);
            }
        }

        doc_id = doc_id
            .checked_add(1)
            .ok_or_else(|| anyhow::anyhow!("line {line}: doc ID overflow (> u32::MAX rows)"))?;
    }

    let num_docs = u64::from(doc_id);
    if num_docs == 0 {
        bail!("no data rows in {}", input.display());
    }

    let mut writer = DiskShardWriter::new(output, (time_min, time_max))
        .with_context(|| format!("create writer at {}", output.display()))?;
    for col in metrics {
        writer
            .add_metric(&col.name, col.values)
            .with_context(|| format!("add metric {:?}", col.name))?;
    }
    for col in strings {
        writer
            .add_string_field(&col.name, col.postings)
            .with_context(|| format!("add string field {:?}", col.name))?;
    }
    writer
        .finalize()
        .with_context(|| format!("finalize {}", output.display()))?;

    Ok(num_docs)
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

/// Which time parser matched. Cached across rows so the hot path skips
/// the failing alternatives.
#[derive(Debug, Clone, Copy)]
enum TimeFormat {
    Epoch,
    Rfc3339,
    Naive,
}

/// Three-way time parsing: unix epoch seconds, RFC 3339, or
/// `YYYY-MM-DD HH:MM:SS` (NYC taxi style; many database exports).
///
/// When `hint` is `Some`, try that format first; fall through to the
/// full detection chain on miss so mixed-format inputs still work.
///
/// **Caveats** worth surfacing to users:
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
    for fmt in [TimeFormat::Epoch, TimeFormat::Rfc3339, TimeFormat::Naive] {
        if let Some(t) = try_parse(s, fmt) {
            return Ok((t, fmt));
        }
    }
    bail!(
        "unrecognized time format (expected unix epoch seconds, RFC 3339, or \
         'YYYY-MM-DD HH:MM:SS')"
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
    }
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
}
