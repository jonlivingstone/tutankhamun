use std::collections::BTreeMap;

use arrow::buffer::BooleanBuffer;

use super::*;

fn write_shard(dir: &Path, time_range: (i64, i64), columns: Vec<(&str, Vec<i64>)>) {
    let mut w = DiskShardWriter::new(dir, time_range).expect("new writer");
    for (name, values) in columns {
        w.add_metric(name, values).expect("add_metric");
    }
    w.finalize().expect("finalize");
}

fn bitmap(docs: impl IntoIterator<Item = u32>) -> RoaringBitmap {
    let mut bm = RoaringBitmap::new();
    bm.extend(docs);
    bm
}

fn write_string_shard(dir: &Path, field: &str, postings: BTreeMap<String, RoaringBitmap>) {
    let mut w = DiskShardWriter::new(dir, (0, 0)).expect("new writer");
    w.add_string_field(field, postings)
        .expect("add_string_field");
    w.finalize().expect("finalize");
}

/// Test-only: mirrors what main.rs's single-shard CLI does so the
/// existing text-shape assertions stay terse.
fn query_cli(
    path: &Path,
    filters: &[FilterClause<'_>],
    metrics: &[&str],
    aggregate: Aggregate,
    out: &mut dyn io::Write,
) -> Result<()> {
    let result = query_shard(path, filters, metrics)?;
    writeln!(out, "shard:    {}", path.display())?;
    write_query_summary(out, filters, metrics, aggregate, &result)?;
    Ok(())
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
                scale: 0,
                nullable: false,
            },
            FieldSchema {
                name: "country".into(),
                kind: FieldKind::String,
                scale: 0,
                nullable: false,
            },
        ],
        content_hashes: std::collections::BTreeMap::default(),
        time_field: None,
    };
    let bytes = serde_json::to_vec(&m).unwrap();
    let back: Metadata = serde_json::from_slice(&bytes).unwrap();
    assert_eq!(m, back);
}

/// A shard `Metadata` carrying just the schema-relevant bits — for the
/// `DatasetSchema` inference/merge tests.
fn meta(fields: Vec<(&str, FieldKind, bool)>, time_field: Option<&str>) -> Metadata {
    Metadata {
        format_version: FORMAT_VERSION,
        num_docs: 1,
        time_range_start: 0,
        time_range_end: 0,
        fields: fields
            .into_iter()
            .map(|(name, kind, nullable)| FieldSchema {
                name: name.into(),
                kind,
                scale: 0,
                nullable,
            })
            .collect(),
        content_hashes: std::collections::BTreeMap::default(),
        time_field: time_field.map(String::from),
    }
}

fn nullable_of(schema: &DatasetSchema, name: &str) -> bool {
    schema
        .fields
        .iter()
        .find(|f| f.name == name)
        .unwrap()
        .nullable
}

#[test]
fn dataset_schema_roundtrips_through_serde_json() {
    let s = DatasetSchema {
        format_version: FORMAT_VERSION,
        fields: vec![FieldSchema {
            name: "x".into(),
            kind: FieldKind::Int,
            scale: 2,
            nullable: true,
        }],
        time_field: Some("ts".into()),
    };
    let bytes = serde_json::to_vec(&s).unwrap();
    assert_eq!(s, serde_json::from_slice::<DatasetSchema>(&bytes).unwrap());
}

#[test]
fn infer_from_shards_unions_nullability() {
    // Shard A: x has a null (nullable); shard B: x dense. Union → nullable.
    let a = meta(vec![("x", FieldKind::Metric, true)], Some("ts"));
    let b = meta(vec![("x", FieldKind::Metric, false)], Some("ts"));
    let schema = DatasetSchema::infer_from_shards(&[a, b]).unwrap();
    assert!(
        nullable_of(&schema, "x"),
        "union: nullable in any shard ⇒ nullable"
    );
}

#[test]
fn infer_from_shards_bails_on_structural_mismatch() {
    // Same column name, different kind — genuinely not one table.
    let a = meta(vec![("x", FieldKind::Metric, false)], None);
    let b = meta(vec![("x", FieldKind::String, false)], None);
    assert!(DatasetSchema::infer_from_shards(&[a, b]).is_err());
}

#[test]
fn merge_is_monotonic_and_rejects_conflict() {
    let stored = DatasetSchema {
        format_version: FORMAT_VERSION,
        fields: vec![FieldSchema {
            name: "x".into(),
            kind: FieldKind::Metric,
            scale: 0,
            nullable: false,
        }],
        time_field: None,
    };
    // A later run that introduces a null flips x to nullable (monotonic).
    let run = DatasetSchema {
        fields: vec![FieldSchema {
            name: "x".into(),
            kind: FieldKind::Metric,
            scale: 0,
            nullable: true,
        }],
        ..stored.clone()
    };
    let evolved = stored.clone().merge(&run).unwrap();
    assert!(nullable_of(&evolved, "x"));

    // A run that changes the column's kind is a conflict, caught at ingest.
    let conflict = DatasetSchema {
        fields: vec![FieldSchema {
            name: "x".into(),
            kind: FieldKind::String,
            scale: 0,
            nullable: false,
        }],
        ..stored.clone()
    };
    assert!(stored.merge(&conflict).is_err());
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
fn forward_column_is_a_view_into_the_mmap() {
    let tmp = tempfile::tempdir().expect("tmpdir");
    let values: Vec<i64> = vec![10, 20, 30, 40];
    write_shard(tmp.path(), (0, 0), vec![("x", values.clone())]);

    let shard = DiskShard::open(tmp.path()).expect("open");
    let col = shard.forward_column("x").expect("column x");
    assert_eq!(col, values.as_slice());

    // The slice's bytes must lie inside the mmap'd metrics.arrow — proving the
    // forward column is a zero-copy view into the file, not a heap copy.
    let map: &[u8] = &shard.mmap;
    let map_start = map.as_ptr() as usize;
    let map_end = map_start + map.len();

    let col_start = col.as_ptr() as usize;
    let col_end = col_start + std::mem::size_of_val(col);
    assert!(
        col_start >= map_start && col_end <= map_end,
        "forward column ({col_start:#x}..{col_end:#x}) not inside mmap \
         ({map_start:#x}..{map_end:#x}) — read was not zero-copy",
    );
}

#[test]
fn nullable_forward_column_roundtrips_with_validity() {
    let tmp = tempfile::tempdir().expect("tmpdir");
    // docs 1 and 3 are NULL — they hold a 0 placeholder in the dense buffer.
    let values: Vec<i64> = vec![10, 0, 30, 0];
    let validity = NullBuffer::new(BooleanBuffer::from(vec![true, false, true, false]));
    let mut w = DiskShardWriter::new(tmp.path(), (0, 0)).expect("new writer");
    w.add_forward_column("m", values.clone(), FieldKind::Metric, Some(validity))
        .expect("add nullable column");
    w.finalize().expect("finalize");

    let shard = DiskShard::open(tmp.path()).expect("open");
    // Values stay dense (placeholders included) and zero-copy.
    let col = shard.forward_column("m").expect("column m");
    assert_eq!(col, values.as_slice());
    let map: &[u8] = &shard.mmap;
    let map_start = map.as_ptr() as usize;
    let col_start = col.as_ptr() as usize;
    assert!(
        col_start >= map_start && col_start + std::mem::size_of_val(col) <= map_start + map.len(),
        "nullable forward column was not a zero-copy view",
    );
    // The validity mask marks exactly docs 1 and 3 null.
    let nulls = shard
        .forward_column_validity("m")
        .expect("validity present");
    assert!(nulls.is_valid(0) && nulls.is_null(1) && nulls.is_valid(2) && nulls.is_null(3));
    // The field is recorded nullable in metadata.
    let field = shard
        .metadata()
        .fields
        .iter()
        .find(|f| f.name == "m")
        .expect("field m");
    assert!(field.nullable);
}

#[test]
fn query_shard_skips_null_metric_values() {
    let tmp = tempfile::tempdir().expect("tmpdir");
    // fare = [100, NULL, 300, NULL, 500] — NULL docs hold a 0 placeholder.
    let validity = NullBuffer::new(BooleanBuffer::from(vec![true, false, true, false, true]));
    let mut w = DiskShardWriter::new(tmp.path(), (0, 0)).expect("new writer");
    w.add_forward_column(
        "fare",
        vec![100, 0, 300, 0, 500],
        FieldKind::Metric,
        Some(validity),
    )
    .expect("add nullable metric");
    w.finalize().expect("finalize");

    let r = query_shard(tmp.path(), &[], &["fare"]).expect("query_shard");
    let agg = &r.aggregates[0];
    // NULLs skipped: the placeholder 0 is neither summed nor taken as the min.
    assert_eq!(agg.sum, 900);
    assert_eq!(agg.min, Some(100));
    assert_eq!(agg.max, Some(500));
    // avg divides by the 3 non-NULL values, not the 5 matched docs.
    assert_eq!(agg.count, 3);
    assert_eq!(format_aggregate(Aggregate::Avg, agg), "300.00");
    // The matched row count still covers every doc.
    assert_eq!(r.matched, 5);
}

#[test]
fn query_shard_all_null_metric_is_na() {
    let tmp = tempfile::tempdir().expect("tmpdir");
    let validity = NullBuffer::new(BooleanBuffer::from(vec![false, false, false]));
    let mut w = DiskShardWriter::new(tmp.path(), (0, 0)).expect("new writer");
    w.add_forward_column("m", vec![0, 0, 0], FieldKind::Metric, Some(validity))
        .expect("add nullable metric");
    w.finalize().expect("finalize");

    let r = query_shard(tmp.path(), &[], &["m"]).expect("query_shard");
    let agg = &r.aggregates[0];
    // No non-NULL value contributed: sum 0, min/max None, avg "n/a".
    assert_eq!(agg.sum, 0);
    assert_eq!(agg.min, None);
    assert_eq!(agg.count, 0);
    assert_eq!(format_aggregate(Aggregate::Min, agg), "n/a");
    assert_eq!(format_aggregate(Aggregate::Avg, agg), "n/a");
}

#[test]
fn all_valid_nullable_column_keeps_flag_but_has_no_null_buffer() {
    let tmp = tempfile::tempdir().expect("tmpdir");
    let validity = NullBuffer::new(BooleanBuffer::from(vec![true, true, true]));
    let mut w = DiskShardWriter::new(tmp.path(), (0, 0)).expect("new writer");
    w.add_forward_column("m", vec![1, 2, 3], FieldKind::Metric, Some(validity))
        .expect("add nullable column");
    w.finalize().expect("finalize");

    let shard = DiskShard::open(tmp.path()).expect("open");
    // The field stays nullable in metadata (uniform across a dataset's shards),
    // but Arrow drops an all-valid null buffer, so validity reads back None.
    let field = shard
        .metadata()
        .fields
        .iter()
        .find(|f| f.name == "m")
        .expect("field m");
    assert!(field.nullable);
    assert!(shard.forward_column_validity("m").is_none());
}

#[test]
fn open_rejects_corrupt_metrics_file_without_panicking() {
    let tmp = tempfile::tempdir().expect("tmpdir");
    write_shard(tmp.path(), (0, 0), vec![("x", vec![1, 2, 3])]);
    // Clobber the forward-column file with garbage — past the 10-byte truncation
    // guard, into footer parsing — so a corrupt footer length / block bounds must
    // produce a clean error rather than an underflow / `slice_with_length` panic.
    std::fs::write(tmp.path().join("metrics.arrow"), [0xABu8; 64]).expect("clobber");

    let err = DiskShard::open(tmp.path())
        .err()
        .expect("corrupt metrics.arrow must error");
    assert!(
        err.to_string().contains("metrics.arrow"),
        "error should name the file: {err}",
    );
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
fn inverted_index_roundtrip_single_term() {
    let tmp = tempfile::tempdir().expect("tmpdir");
    let mut postings = BTreeMap::new();
    postings.insert("us".to_string(), bitmap([0, 2]));
    write_string_shard(tmp.path(), "country", postings);

    let shard = DiskShard::open(tmp.path()).expect("open");
    let idx = shard.inverted_index("country").expect("country index");
    assert_eq!(idx.num_terms(), 1);
    let got = idx.lookup("us").expect("us term");
    assert_eq!(got, bitmap([0, 2]));
}

#[test]
fn inverted_index_roundtrip_many_terms() {
    let tmp = tempfile::tempdir().expect("tmpdir");
    let mut postings = BTreeMap::new();
    // 100 terms with mixed dense/sparse bitmaps over 1000 docs.
    for i in 0u32..100 {
        let term = format!("t{i:03}");
        let bm: RoaringBitmap = if i % 2 == 0 {
            bitmap((0u32..1000).filter(|d| d % (i + 2) == 0))
        } else {
            bitmap([i, i + 100, i + 500])
        };
        postings.insert(term, bm);
    }
    write_string_shard(tmp.path(), "f", postings.clone());

    let shard = DiskShard::open(tmp.path()).expect("open");
    let idx = shard.inverted_index("f").expect("f index");
    assert_eq!(idx.num_terms(), 100);

    // Sample a handful of lookups.
    for sample in ["t000", "t017", "t050", "t099"] {
        let expected = postings.get(sample).expect("expected term");
        let got = idx.lookup(sample).expect("term present");
        assert_eq!(&got, expected, "term {sample}");
    }
    assert!(idx.lookup("not-a-term").is_none());

    // terms() returns sorted.
    let listed: Vec<String> = idx.terms().collect();
    let mut sorted = listed.clone();
    sorted.sort();
    assert_eq!(listed, sorted);
    assert_eq!(listed.len(), 100);
}

#[test]
fn inverted_index_range_scan() {
    let tmp = tempfile::tempdir().expect("tmpdir");
    let mut postings = BTreeMap::new();
    postings.insert("a".to_string(), bitmap([0]));
    postings.insert("b".to_string(), bitmap([1]));
    postings.insert("c".to_string(), bitmap([2]));
    postings.insert("d".to_string(), bitmap([3]));

    write_string_shard(tmp.path(), "letters", postings);

    let shard = DiskShard::open(tmp.path()).expect("open");
    let idx = shard.inverted_index("letters").expect("index");

    let got: Vec<(String, RoaringBitmap)> = idx.range("b", "c").collect();
    assert_eq!(got.len(), 2);
    assert_eq!(got[0].0, "b");
    assert_eq!(got[0].1, bitmap([1]));
    assert_eq!(got[1].0, "c");
    assert_eq!(got[1].1, bitmap([2]));
}

#[test]
fn inverted_index_missing_term_is_none() {
    let tmp = tempfile::tempdir().expect("tmpdir");
    let mut postings = BTreeMap::new();
    postings.insert("present".to_string(), bitmap([0]));
    write_string_shard(tmp.path(), "f", postings);

    let shard = DiskShard::open(tmp.path()).expect("open");
    let idx = shard.inverted_index("f").expect("index");
    assert!(idx.lookup("absent").is_none());
}

#[test]
fn inverted_index_missing_field_is_none() {
    let tmp = tempfile::tempdir().expect("tmpdir");
    write_shard(tmp.path(), (0, 0), vec![("x", vec![1, 2, 3])]);

    let shard = DiskShard::open(tmp.path()).expect("open");
    assert!(shard.inverted_index("nope").is_none());
}

#[test]
fn mixed_metric_and_string_fields() {
    let tmp = tempfile::tempdir().expect("tmpdir");
    let mut postings = BTreeMap::new();
    postings.insert("de".to_string(), bitmap([1]));
    postings.insert("us".to_string(), bitmap([0, 2]));

    let mut w = DiskShardWriter::new(tmp.path(), (0, 0)).expect("new");
    w.add_metric("clicks", vec![10, 20, 30])
        .expect("add_metric");
    w.add_metric("impressions", vec![100, 200, 300])
        .expect("add_metric");
    w.add_string_field("country", postings)
        .expect("add_string_field");
    w.finalize().expect("finalize");

    let shard = DiskShard::open(tmp.path()).expect("open");
    assert_eq!(shard.num_docs(), 3);
    assert_eq!(shard.forward_column("clicks").unwrap(), &[10, 20, 30]);
    assert_eq!(
        shard.forward_column("impressions").unwrap(),
        &[100, 200, 300]
    );
    assert!(shard.forward_column("country").is_none());

    let idx = shard.inverted_index("country").expect("country index");
    assert_eq!(idx.lookup("us").unwrap(), bitmap([0, 2]));
    assert_eq!(idx.lookup("de").unwrap(), bitmap([1]));
    assert!(shard.inverted_index("clicks").is_none());
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
    assert!(out.contains("format version:  2"), "{out}");
    assert!(out.contains("num docs:        3"), "{out}");
    assert!(out.contains("1700000000 .. 1700003600"), "{out}");
    assert!(out.contains("2023-11-14T22:13:20"), "{out}");
    assert!(out.contains("alpha"), "{out}");
    assert!(out.contains("beta"), "{out}");
    assert!(out.contains("gamma_long_name"), "{out}");
    assert!(out.contains("metric"), "{out}");
    assert!(out.contains("int64"), "{out}");
}

#[test]
fn add_string_field_rejects_empty_postings() {
    let tmp = tempfile::tempdir().expect("tmpdir");
    let mut w = DiskShardWriter::new(tmp.path(), (0, 0)).expect("new");
    let err = w
        .add_string_field("empty", BTreeMap::new())
        .expect_err("expected empty-postings rejection");
    let msg = err.to_string();
    assert!(msg.contains("empty"), "got: {msg}");
}

#[test]
fn finalize_rejects_string_bitmap_exceeding_num_docs() {
    let tmp = tempfile::tempdir().expect("tmpdir");
    let mut postings = BTreeMap::new();
    postings.insert("oops".to_string(), bitmap([0, 99]));

    let mut w = DiskShardWriter::new(tmp.path(), (0, 0)).expect("new");
    w.add_metric("clicks", vec![1, 2, 3]).expect("add_metric");
    w.add_string_field("country", postings)
        .expect("add_string_field");
    let err = w.finalize().expect_err("expected num_docs overflow");
    let msg = err.to_string();
    assert!(msg.contains("num_docs") && msg.contains("99"), "got: {msg}");
}

#[test]
fn finalize_derives_num_docs_from_string_bitmaps_when_no_metrics() {
    let tmp = tempfile::tempdir().expect("tmpdir");
    let mut postings = BTreeMap::new();
    postings.insert("us".to_string(), bitmap([0, 2, 7]));
    write_string_shard(tmp.path(), "country", postings);

    let shard = DiskShard::open(tmp.path()).expect("open");
    // Max doc ID was 7, so num_docs is derived as 8.
    assert_eq!(shard.num_docs(), 8);
}

fn write_query_fixture(dir: &Path) {
    // 5 docs: clicks = [10, 20, 30, 40, 50], country: us=[0,2,4], de=[1,3].
    let mut postings = BTreeMap::new();
    postings.insert("de".to_string(), bitmap([1, 3]));
    postings.insert("us".to_string(), bitmap([0, 2, 4]));
    let mut w = DiskShardWriter::new(dir, (0, 0)).expect("new");
    w.add_metric("clicks", vec![10, 20, 30, 40, 50])
        .expect("add_metric");
    w.add_string_field("country", postings)
        .expect("add_string_field");
    w.finalize().expect("finalize");
}

fn write_multi_metric_fixture(dir: &Path) {
    // 5 docs:
    //   clicks      = [10, 20, 30, 40, 50]  (sum 150)
    //   impressions = [100, 200, 300, 400, 500]  (sum 1500)
    //   country     : us=[0, 2, 4], de=[1, 3]
    let mut postings = BTreeMap::new();
    postings.insert("de".to_string(), bitmap([1, 3]));
    postings.insert("us".to_string(), bitmap([0, 2, 4]));
    let mut w = DiskShardWriter::new(dir, (0, 0)).expect("new");
    w.add_metric("clicks", vec![10, 20, 30, 40, 50])
        .expect("add_metric clicks");
    w.add_metric("impressions", vec![100, 200, 300, 400, 500])
        .expect("add_metric impressions");
    w.add_string_field("country", postings)
        .expect("add_string_field");
    w.finalize().expect("finalize");
}

#[test]
fn query_shard_returns_one_sum_per_metric() {
    let tmp = tempfile::tempdir().expect("tmpdir");
    write_multi_metric_fixture(tmp.path());

    let r =
        query_shard(tmp.path(), &[], &["clicks", "impressions"]).expect("query_shard multi-metric");
    assert_eq!(r.num_docs, 5);
    assert_eq!(r.matched, 5);
    assert_eq!(r.aggregates[0].sum, 150);
    assert_eq!(r.aggregates[1].sum, 1500);

    // Slot order follows input order — flipping the metrics flips the sums.
    let r = query_shard(tmp.path(), &[], &["impressions", "clicks"])
        .expect("query_shard multi-metric reversed");
    assert_eq!(r.aggregates[0].sum, 1500);
    assert_eq!(r.aggregates[1].sum, 150);
}

#[test]
fn query_shard_multi_metric_with_filter() {
    let tmp = tempfile::tempdir().expect("tmpdir");
    write_multi_metric_fixture(tmp.path());

    // us = docs 0, 2, 4: clicks 10+30+50=90, impressions 100+300+500=900.
    let r = query_shard(
        tmp.path(),
        &[FilterClause::equals("country", "us")],
        &["clicks", "impressions"],
    )
    .expect("query_shard multi-metric filtered");
    assert_eq!(r.matched, 3);
    assert_eq!(r.aggregates[0].sum, 90);
    assert_eq!(r.aggregates[1].sum, 900);
}

#[test]
fn query_shard_rejects_duplicate_metrics() {
    let tmp = tempfile::tempdir().expect("tmpdir");
    write_multi_metric_fixture(tmp.path());

    let err = query_shard(tmp.path(), &[], &["clicks", "clicks"])
        .expect_err("expected duplicate-metric rejection");
    let msg = err.to_string();
    assert!(
        msg.contains("clicks") && msg.contains("more than once"),
        "{msg}"
    );
}

#[test]
fn query_sums_metric_filtered_by_term() {
    let tmp = tempfile::tempdir().expect("tmpdir");
    write_query_fixture(tmp.path());

    let mut buf = Vec::new();
    query_cli(
        tmp.path(),
        &[FilterClause::equals("country", "us")],
        &["clicks"],
        Aggregate::Sum,
        &mut buf,
    )
    .expect("query us");
    let out = String::from_utf8(buf).expect("utf-8");
    assert!(out.contains("matched:  3 / 5 docs"), "{out}");
    assert!(out.contains("clicks:   sum = 90"), "{out}");

    let mut buf = Vec::new();
    query_cli(
        tmp.path(),
        &[FilterClause::equals("country", "de")],
        &["clicks"],
        Aggregate::Sum,
        &mut buf,
    )
    .expect("query de");
    let out = String::from_utf8(buf).expect("utf-8");
    assert!(out.contains("matched:  2 / 5 docs"), "{out}");
    assert!(out.contains("clicks:   sum = 60"), "{out}");
}

#[test]
fn query_no_filter_sums_all_docs() {
    let tmp = tempfile::tempdir().expect("tmpdir");
    write_query_fixture(tmp.path());

    let mut buf = Vec::new();
    query_cli(tmp.path(), &[], &["clicks"], Aggregate::Sum, &mut buf).expect("query");
    let out = String::from_utf8(buf).expect("utf-8");
    assert!(out.contains("matched:  all 5 docs"), "{out}");
    assert!(out.contains("clicks:   sum = 150"), "{out}");
    assert!(!out.contains("filter:"), "{out}");
}

#[test]
fn query_missing_filter_term_is_zero() {
    let tmp = tempfile::tempdir().expect("tmpdir");
    write_query_fixture(tmp.path());

    let mut buf = Vec::new();
    query_cli(
        tmp.path(),
        &[FilterClause::equals("country", "fr")],
        &["clicks"],
        Aggregate::Sum,
        &mut buf,
    )
    .expect("query fr");
    let out = String::from_utf8(buf).expect("utf-8");
    assert!(out.contains("matched:  0 / 5 docs"), "{out}");
    assert!(out.contains("clicks:   sum = 0"), "{out}");
}

#[test]
fn query_rejects_unknown_metric() {
    let tmp = tempfile::tempdir().expect("tmpdir");
    write_query_fixture(tmp.path());

    let mut buf = Vec::new();
    let err = query_cli(
        tmp.path(),
        &[],
        &["no_such_metric"],
        Aggregate::Sum,
        &mut buf,
    )
    .expect_err("expected unknown-metric error");
    let msg = err.to_string();
    assert!(msg.contains("no_such_metric"), "{msg}");
}

#[test]
fn query_rejects_string_field_as_metric() {
    let tmp = tempfile::tempdir().expect("tmpdir");
    write_query_fixture(tmp.path());

    let mut buf = Vec::new();
    let err = query_cli(tmp.path(), &[], &["country"], Aggregate::Sum, &mut buf)
        .expect_err("expected wrong-kind error");
    let msg = err.to_string();
    // The error names the offending field and the kinds that would
    // have been acceptable for a metric (metric or int).
    assert!(
        msg.contains("country") && msg.contains("metric") && msg.contains("int"),
        "{msg}"
    );
}

#[test]
fn query_rejects_unknown_filter_field() {
    let tmp = tempfile::tempdir().expect("tmpdir");
    write_query_fixture(tmp.path());

    let mut buf = Vec::new();
    let err = query_cli(
        tmp.path(),
        &[FilterClause::equals("cuontry", "us")],
        &["clicks"],
        Aggregate::Sum,
        &mut buf,
    )
    .expect_err("expected unknown-field error");
    let msg = err.to_string();
    assert!(msg.contains("cuontry"), "{msg}");
}

#[test]
fn query_rejects_metric_field_as_filter() {
    let tmp = tempfile::tempdir().expect("tmpdir");
    write_query_fixture(tmp.path());

    let mut buf = Vec::new();
    let err = query_cli(
        tmp.path(),
        &[FilterClause::equals("clicks", "10")],
        &["clicks"],
        Aggregate::Sum,
        &mut buf,
    )
    .expect_err("expected wrong-kind error");
    let msg = err.to_string();
    // The error names the offending field and the kinds that would
    // have been acceptable for a filter (string or int).
    assert!(
        msg.contains("clicks") && msg.contains("string") && msg.contains("int"),
        "{msg}"
    );
}

#[test]
fn inspect_prints_string_field_term_count() {
    let tmp = tempfile::tempdir().expect("tmpdir");
    let mut postings = BTreeMap::new();
    postings.insert("us".to_string(), bitmap([0]));
    postings.insert("de".to_string(), bitmap([1]));
    postings.insert("fr".to_string(), bitmap([2]));

    let mut w = DiskShardWriter::new(tmp.path(), (0, 0)).expect("new");
    w.add_metric("clicks", vec![1, 2, 3]).expect("add_metric");
    w.add_string_field("country", postings)
        .expect("add_string_field");
    w.finalize().expect("finalize");

    let mut buf = Vec::new();
    inspect(tmp.path(), &mut buf).expect("inspect");
    let out = String::from_utf8(buf).expect("utf-8");

    assert!(out.contains("country"), "{out}");
    assert!(out.contains("string"), "{out}");
    assert!(out.contains("index (3 terms)"), "{out}");
}

// ===== Int field tests =====

#[test]
fn int_key_encoding_round_trips() {
    for v in [i64::MIN, -1_i64, 0_i64, 1_i64, i64::MAX, 12345, -67890] {
        assert_eq!(decode_int_key(&encode_int_key(v)), v, "value {v}");
    }
}

#[test]
fn int_key_encoding_preserves_order() {
    // Order-preserving: encode(a) < encode(b) iff a < b. Critical
    // for FST range scans to give numeric semantics.
    let pairs = [
        (i64::MIN, -1),
        (-100, -1),
        (-1, 0),
        (0, 1),
        (1, 100),
        (100, i64::MAX),
    ];
    for (a, b) in pairs {
        assert!(encode_int_key(a) < encode_int_key(b), "{a} < {b}");
    }
}

#[test]
fn add_int_field_round_trip() {
    let tmp = tempfile::tempdir().expect("tmpdir");
    let mut w = DiskShardWriter::new(tmp.path(), (0, 0)).expect("new");
    w.add_int_field("vendor_id", vec![1, 2, 1, 3])
        .expect("add_int_field");
    w.finalize().expect("finalize");

    let shard = DiskShard::open(tmp.path()).expect("open");
    // Forward column: raw values in row order.
    assert_eq!(shard.forward_column("vendor_id").unwrap(), &[1, 2, 1, 3]);
    // Inverted index: 3 distinct terms (1, 2, 3); vendor 1 appears at docs 0 and 2.
    let idx = shard
        .inverted_index("vendor_id")
        .expect("vendor_id has an index");
    assert_eq!(idx.num_terms(), 3);
    let v1 = idx.lookup_bytes(&encode_int_key(1)).expect("vendor 1");
    assert_eq!(v1, bitmap([0, 2]));
    let v2 = idx.lookup_bytes(&encode_int_key(2)).expect("vendor 2");
    assert_eq!(v2, bitmap([1]));
    let v3 = idx.lookup_bytes(&encode_int_key(3)).expect("vendor 3");
    assert_eq!(v3, bitmap([3]));
    assert!(idx.lookup_bytes(&encode_int_key(999)).is_none());
}

#[test]
fn int_field_negative_values() {
    let tmp = tempfile::tempdir().expect("tmpdir");
    let mut w = DiskShardWriter::new(tmp.path(), (0, 0)).expect("new");
    w.add_int_field("delta", vec![-100, 0, 100, -100])
        .expect("add_int_field");
    w.finalize().expect("finalize");

    let shard = DiskShard::open(tmp.path()).expect("open");
    assert_eq!(
        shard.forward_column("delta").unwrap(),
        &[-100, 0, 100, -100]
    );
    let idx = shard.inverted_index("delta").expect("delta index");
    assert_eq!(
        idx.lookup_bytes(&encode_int_key(-100)).unwrap(),
        bitmap([0, 3])
    );
    assert_eq!(idx.lookup_bytes(&encode_int_key(0)).unwrap(), bitmap([1]));
    assert_eq!(idx.lookup_bytes(&encode_int_key(100)).unwrap(), bitmap([2]));
}

#[test]
fn int_field_i64_boundary_values() {
    let tmp = tempfile::tempdir().expect("tmpdir");
    let mut w = DiskShardWriter::new(tmp.path(), (0, 0)).expect("new");
    w.add_int_field("v", vec![i64::MIN, i64::MAX, 0])
        .expect("add_int_field");
    w.finalize().expect("finalize");

    let shard = DiskShard::open(tmp.path()).expect("open");
    assert_eq!(shard.forward_column("v").unwrap(), &[i64::MIN, i64::MAX, 0]);
    let idx = shard.inverted_index("v").expect("v index");
    assert_eq!(
        idx.lookup_bytes(&encode_int_key(i64::MIN)).unwrap(),
        bitmap([0])
    );
    assert_eq!(
        idx.lookup_bytes(&encode_int_key(i64::MAX)).unwrap(),
        bitmap([1])
    );
    assert_eq!(idx.lookup_bytes(&encode_int_key(0)).unwrap(), bitmap([2]));
}

#[test]
fn int_field_range_is_numeric_order() {
    // Values include 100, 5, -100, -5: lex sort on decimal strings
    // would put "100" before "5"; numeric sort (via the
    // order-preserving encoding) puts -100 < -5 < 5 < 100.
    let tmp = tempfile::tempdir().expect("tmpdir");
    let mut w = DiskShardWriter::new(tmp.path(), (0, 0)).expect("new");
    w.add_int_field("v", vec![100, 5, -100, -5])
        .expect("add_int_field");
    w.finalize().expect("finalize");

    let shard = DiskShard::open(tmp.path()).expect("open");
    let idx = shard.inverted_index("v").expect("v index");
    // Range [-50, 50] should match -5 and 5 only — not 100 (which
    // would slip in under naive lex-on-decimal-string).
    let lo = encode_int_key(-50);
    let hi = encode_int_key(50);
    let in_range: Vec<RoaringBitmap> = idx
        .range_bytes(Some(&lo), Some(&hi))
        .map(|(_, bm)| bm)
        .collect();
    assert_eq!(in_range.len(), 2, "should match exactly -5 and 5");
    let mut union = RoaringBitmap::new();
    for bm in in_range {
        union |= bm;
    }
    // Doc IDs for -5 and 5 are 3 and 1 respectively (per the input order).
    assert_eq!(union, bitmap([1, 3]));
}

#[test]
fn query_shard_accepts_int_as_metric() {
    let tmp = tempfile::tempdir().expect("tmpdir");
    let mut w = DiskShardWriter::new(tmp.path(), (0, 0)).expect("new");
    w.add_int_field("vendor_id", vec![1, 2, 1, 3])
        .expect("add_int_field");
    w.finalize().expect("finalize");

    let r = query_shard(tmp.path(), &[], &["vendor_id"]).expect("query_shard");
    assert_eq!(r.num_docs, 4);
    assert_eq!(r.matched, 4);
    assert_eq!(r.aggregates[0].sum, i128::from(1 + 2 + 1 + 3));
}

#[test]
fn query_shard_int_field_used_as_both_filter_and_metric() {
    let tmp = tempfile::tempdir().expect("tmpdir");
    let mut w = DiskShardWriter::new(tmp.path(), (0, 0)).expect("new");
    w.add_int_field("vendor_id", vec![1, 2, 1, 3])
        .expect("add_int_field");
    w.add_metric("fare", vec![100, 200, 300, 400])
        .expect("add_metric");
    w.finalize().expect("finalize");

    // Filter on vendor_id=1 (matches docs 0 and 2), sum both
    // vendor_id (1+1=2) and fare (100+300=400) for those docs.
    let r = query_shard(
        tmp.path(),
        &[FilterClause::equals("vendor_id", "1")],
        &["vendor_id", "fare"],
    )
    .expect("query_shard");
    assert_eq!(r.matched, 2);
    assert_eq!(r.aggregates[0].sum, 2);
    assert_eq!(r.aggregates[1].sum, 400);
}

#[test]
fn query_shard_int_filter_term_must_parse_as_int() {
    let tmp = tempfile::tempdir().expect("tmpdir");
    let mut w = DiskShardWriter::new(tmp.path(), (0, 0)).expect("new");
    w.add_int_field("vendor_id", vec![1, 2, 3])
        .expect("add_int_field");
    w.finalize().expect("finalize");

    let err = query_shard(
        tmp.path(),
        &[FilterClause::equals("vendor_id", "not-a-number")],
        &["vendor_id"],
    )
    .expect_err("expected parse error for non-int filter term");
    let msg = format!("{err:#}");
    assert!(
        msg.contains("vendor_id") && msg.contains("not a valid int64"),
        "{msg}"
    );
}

#[test]
fn inspect_shows_int_field_row() {
    let tmp = tempfile::tempdir().expect("tmpdir");
    let mut w = DiskShardWriter::new(tmp.path(), (0, 0)).expect("new");
    w.add_int_field("vendor_id", vec![1, 2, 1, 3])
        .expect("add_int_field");
    w.finalize().expect("finalize");

    let mut buf = Vec::new();
    inspect(tmp.path(), &mut buf).expect("inspect");
    let out = String::from_utf8(buf).expect("utf-8");
    assert!(out.contains("vendor_id"), "{out}");
    assert!(out.contains("int"), "{out}");
    assert!(out.contains("forward+index (3 terms)"), "{out}");
}

#[test]
fn query_shard_computes_min_max_in_single_scan() {
    let tmp = tempfile::tempdir().expect("tmpdir");
    let mut w = DiskShardWriter::new(tmp.path(), (0, 0)).expect("new");
    w.add_metric("v", vec![10, -5, 30, 7, 22])
        .expect("add_metric");
    w.finalize().expect("finalize");

    let r = query_shard(tmp.path(), &[], &["v"]).expect("query_shard");
    let agg = &r.aggregates[0];
    assert_eq!(agg.sum, 64);
    assert_eq!(agg.min, Some(-5));
    assert_eq!(agg.max, Some(30));
}

#[test]
fn query_shard_min_max_under_filter() {
    let tmp = tempfile::tempdir().expect("tmpdir");
    let mut w = DiskShardWriter::new(tmp.path(), (0, 0)).expect("new");
    w.add_metric("v", vec![10, -5, 30, 7, 22]).expect("metric");
    w.add_int_field("g", vec![1, 1, 2, 1, 2]).expect("int");
    w.finalize().expect("finalize");

    // g=1 selects docs 0,1,3 -> values 10, -5, 7.
    let r =
        query_shard(tmp.path(), &[FilterClause::equals("g", "1")], &["v"]).expect("query_shard");
    assert_eq!(r.matched, 3);
    let agg = &r.aggregates[0];
    assert_eq!(agg.sum, 12);
    assert_eq!(agg.min, Some(-5));
    assert_eq!(agg.max, Some(10));
}

#[test]
fn query_shard_empty_match_yields_none_min_max() {
    let tmp = tempfile::tempdir().expect("tmpdir");
    let mut w = DiskShardWriter::new(tmp.path(), (0, 0)).expect("new");
    w.add_metric("v", vec![1, 2, 3]).expect("metric");
    w.add_string_field("c", BTreeMap::from([("us".to_string(), bitmap([0, 1, 2]))]))
        .expect("string");
    w.finalize().expect("finalize");

    let r =
        query_shard(tmp.path(), &[FilterClause::equals("c", "fr")], &["v"]).expect("query_shard");
    assert_eq!(r.matched, 0);
    let agg = &r.aggregates[0];
    assert_eq!(agg.sum, 0);
    assert!(agg.min.is_none());
    assert!(agg.max.is_none());
}

#[test]
fn query_renders_min_max_avg() {
    let tmp = tempfile::tempdir().expect("tmpdir");
    let mut w = DiskShardWriter::new(tmp.path(), (0, 0)).expect("new");
    w.add_metric("v", vec![10, 20, 30, 40]).expect("metric");
    w.finalize().expect("finalize");

    for (op, expected) in [
        (Aggregate::Min, "min = 10"),
        (Aggregate::Max, "max = 40"),
        (Aggregate::Avg, "avg = 25.00"),
    ] {
        let mut buf = Vec::new();
        query_cli(tmp.path(), &[], &["v"], op, &mut buf).expect("query");
        let out = String::from_utf8(buf).expect("utf-8");
        assert!(out.contains(expected), "op={op:?} out={out}");
    }
}

#[test]
fn query_renders_n_a_when_no_match_for_min_max_avg() {
    let tmp = tempfile::tempdir().expect("tmpdir");
    let mut w = DiskShardWriter::new(tmp.path(), (0, 0)).expect("new");
    w.add_metric("v", vec![1, 2, 3]).expect("metric");
    w.add_string_field("c", BTreeMap::from([("us".to_string(), bitmap([0, 1, 2]))]))
        .expect("string");
    w.finalize().expect("finalize");

    for op in [Aggregate::Min, Aggregate::Max, Aggregate::Avg] {
        let mut buf = Vec::new();
        query_cli(
            tmp.path(),
            &[FilterClause::equals("c", "fr")],
            &["v"],
            op,
            &mut buf,
        )
        .expect("query");
        let out = String::from_utf8(buf).expect("utf-8");
        assert!(out.contains("= n/a"), "op={op:?} out={out}");
    }
}

#[test]
fn metric_aggregates_absorb_composes_min_max_avg_across_shards() {
    let a = MetricAggregates {
        sum: 60,
        min: Some(10),
        max: Some(40),
        count: 3,
    };
    let b = MetricAggregates {
        sum: 40,
        min: Some(5),
        max: Some(25),
        count: 2,
    };
    let mut acc = a;
    acc.absorb(&b);
    assert_eq!(acc.sum, 100);
    assert_eq!(acc.min, Some(5));
    assert_eq!(acc.max, Some(40));
    // avg denominator composes: 3 + 2 non-NULL values across the two shards.
    assert_eq!(acc.count, 5);

    // None on one side is a no-op for that side (empty shard).
    let empty = MetricAggregates::default();
    let mut acc = a;
    acc.absorb(&empty);
    assert_eq!(acc.sum, 60);
    assert_eq!(acc.min, Some(10));
    assert_eq!(acc.max, Some(40));
    assert_eq!(acc.count, 3);
}

#[test]
fn query_shard_int_range_filter_includes_inclusive_bounds() {
    // vendor_id values 1, 2, 3, 4, 5; range 2..4 must hit docs whose
    // vendor_id is 2, 3, or 4 (both ends inclusive).
    let tmp = tempfile::tempdir().expect("tmpdir");
    let mut w = DiskShardWriter::new(tmp.path(), (0, 0)).expect("new");
    w.add_int_field("vendor_id", vec![1, 2, 3, 4, 5])
        .expect("add_int_field");
    w.add_metric("fare", vec![100, 200, 300, 400, 500])
        .expect("add_metric");
    w.finalize().expect("finalize");

    let r = query_shard(
        tmp.path(),
        &[FilterClause::range("vendor_id", Some("2"), Some("4"))],
        &["fare"],
    )
    .expect("query_shard");
    assert_eq!(r.matched, 3, "vendor_id in {{2, 3, 4}}");
    assert_eq!(r.aggregates[0].sum, 200 + 300 + 400);
}

#[test]
fn query_shard_int_range_open_lower_and_upper() {
    let tmp = tempfile::tempdir().expect("tmpdir");
    let mut w = DiskShardWriter::new(tmp.path(), (0, 0)).expect("new");
    w.add_int_field("v", vec![-10, 0, 5, 100, 1000])
        .expect("add_int_field");
    w.add_metric("m", vec![1, 1, 1, 1, 1]).expect("add_metric");
    w.finalize().expect("finalize");

    let r = query_shard(
        tmp.path(),
        &[FilterClause::range("v", None, Some("5"))],
        &["m"],
    )
    .expect("open lower");
    assert_eq!(r.matched, 3, "v in [-inf, 5] = -10, 0, 5");

    let r = query_shard(
        tmp.path(),
        &[FilterClause::range("v", Some("5"), None)],
        &["m"],
    )
    .expect("open upper");
    assert_eq!(r.matched, 3, "v in [5, +inf] = 5, 100, 1000");
}

#[test]
fn query_shard_int_range_with_no_matching_terms_is_zero() {
    let tmp = tempfile::tempdir().expect("tmpdir");
    let mut w = DiskShardWriter::new(tmp.path(), (0, 0)).expect("new");
    w.add_int_field("v", vec![1, 2, 3]).expect("add_int_field");
    w.add_metric("m", vec![10, 20, 30]).expect("add_metric");
    w.finalize().expect("finalize");

    let r = query_shard(
        tmp.path(),
        &[FilterClause::range("v", Some("100"), Some("200"))],
        &["m"],
    )
    .expect("query_shard");
    assert_eq!(r.matched, 0);
    assert_eq!(r.aggregates[0].sum, 0);
    assert!(r.aggregates[0].min.is_none());
}

#[test]
fn query_shard_string_lex_range_filter() {
    // Lex range "b".."d" on a string field. Terms a, b, c, d, e:
    // hits b, c, d.
    let tmp = tempfile::tempdir().expect("tmpdir");
    let mut postings = BTreeMap::new();
    for (i, term) in ["a", "b", "c", "d", "e"].iter().enumerate() {
        let mut bm = RoaringBitmap::new();
        bm.insert(u32::try_from(i).expect("doc id fits in u32"));
        postings.insert((*term).to_string(), bm);
    }
    let mut w = DiskShardWriter::new(tmp.path(), (0, 0)).expect("new");
    w.add_metric("m", vec![1, 1, 1, 1, 1]).expect("metric");
    w.add_string_field("letter", postings).expect("string");
    w.finalize().expect("finalize");

    let r = query_shard(
        tmp.path(),
        &[FilterClause::range("letter", Some("b"), Some("d"))],
        &["m"],
    )
    .expect("query_shard");
    assert_eq!(r.matched, 3, "letter in [b, d]");
}

#[test]
fn query_shard_int_range_bound_must_parse_as_int() {
    let tmp = tempfile::tempdir().expect("tmpdir");
    let mut w = DiskShardWriter::new(tmp.path(), (0, 0)).expect("new");
    w.add_int_field("vendor_id", vec![1, 2, 3])
        .expect("add_int_field");
    w.finalize().expect("finalize");

    let err = query_shard(
        tmp.path(),
        &[FilterClause::range(
            "vendor_id",
            Some("not-a-number"),
            Some("5"),
        )],
        &["vendor_id"],
    )
    .expect_err("expected parse error for non-int range bound");
    let msg = format!("{err:#}");
    assert!(
        msg.contains("vendor_id") && msg.contains("not a valid int64"),
        "{msg}"
    );
}

#[test]
fn query_shard_range_rejects_double_open_filter_clause() {
    // CLI parser blocks `field=..`, but a programmatic caller can
    // still construct `FilterClause::range(field, None, None)`.
    // Engine must hard-fail rather than silently match every doc.
    let tmp = tempfile::tempdir().expect("tmpdir");
    let mut w = DiskShardWriter::new(tmp.path(), (0, 0)).expect("new");
    w.add_int_field("v", vec![1, 2, 3]).expect("int");
    w.add_metric("m", vec![10, 20, 30]).expect("metric");
    w.finalize().expect("finalize");

    let err = query_shard(tmp.path(), &[FilterClause::range("v", None, None)], &["m"])
        .expect_err("expected at-least-one-bound rejection");
    let msg = format!("{err:#}");
    assert!(msg.contains("at least one bound"), "{msg}");
}

#[test]
fn query_shard_range_rejects_inverted_bounds() {
    // `lo > hi` would silently return 0 matches via an empty
    // FST range; surface as an error instead (matches the
    // time-range UX, which already rejects `from > to`).
    let tmp = tempfile::tempdir().expect("tmpdir");
    let mut w = DiskShardWriter::new(tmp.path(), (0, 0)).expect("new");
    w.add_int_field("v", vec![1, 2, 3, 4, 5]).expect("int");
    w.add_metric("m", vec![10, 20, 30, 40, 50]).expect("metric");
    w.finalize().expect("finalize");

    let err = query_shard(
        tmp.path(),
        &[FilterClause::range("v", Some("4"), Some("2"))],
        &["m"],
    )
    .expect_err("expected inverted-bounds rejection");
    let msg = format!("{err:#}");
    assert!(msg.contains("lower bound"), "{msg}");

    // Same check for string fields.
    let tmp = tempfile::tempdir().expect("tmpdir");
    let mut postings = BTreeMap::new();
    postings.insert("a".to_string(), bitmap([0]));
    postings.insert("b".to_string(), bitmap([1]));
    let mut w = DiskShardWriter::new(tmp.path(), (0, 0)).expect("new");
    w.add_metric("m", vec![1, 2]).expect("metric");
    w.add_string_field("c", postings).expect("string");
    w.finalize().expect("finalize");
    let err = query_shard(
        tmp.path(),
        &[FilterClause::range("c", Some("d"), Some("b"))],
        &["m"],
    )
    .expect_err("expected inverted-string-bounds rejection");
    let msg = format!("{err:#}");
    assert!(msg.contains("lower bound"), "{msg}");
}

#[test]
fn write_query_summary_renders_range_clause() {
    let tmp = tempfile::tempdir().expect("tmpdir");
    let mut w = DiskShardWriter::new(tmp.path(), (0, 0)).expect("new");
    w.add_int_field("v", vec![1, 2, 3, 4, 5]).expect("int");
    w.add_metric("m", vec![10, 20, 30, 40, 50]).expect("metric");
    w.finalize().expect("finalize");

    let mut buf = Vec::new();
    query_cli(
        tmp.path(),
        &[FilterClause::range("v", Some("2"), Some("4"))],
        &["m"],
        Aggregate::Sum,
        &mut buf,
    )
    .expect("query_cli");
    let out = String::from_utf8(buf).expect("utf-8");
    assert!(out.contains("filter:   v = 2..4"), "{out}");

    let mut buf = Vec::new();
    query_cli(
        tmp.path(),
        &[FilterClause::range("v", Some("3"), None)],
        &["m"],
        Aggregate::Sum,
        &mut buf,
    )
    .expect("query_cli");
    let out = String::from_utf8(buf).expect("utf-8");
    assert!(out.contains("filter:   v = 3.."), "{out}");
}

#[test]
fn query_shard_multi_filter_and_intersects_clauses() {
    // vendor_id=1 docs: 0, 2; country=us docs: 0, 1; AND = doc 0 (fare=100).
    let tmp = tempfile::tempdir().expect("tmpdir");
    let mut w = DiskShardWriter::new(tmp.path(), (0, 0)).expect("new");
    w.add_int_field("vendor_id", vec![1, 2, 1, 2]).expect("int");
    w.add_metric("fare", vec![100, 200, 300, 400])
        .expect("metric");
    let mut country = BTreeMap::new();
    country.insert("us".to_string(), bitmap([0, 1]));
    country.insert("de".to_string(), bitmap([2, 3]));
    w.add_string_field("country", country).expect("string");
    w.finalize().expect("finalize");

    let r = query_shard(
        tmp.path(),
        &[
            FilterClause::equals("vendor_id", "1"),
            FilterClause::equals("country", "us"),
        ],
        &["fare"],
    )
    .expect("query_shard");
    assert_eq!(r.matched, 1);
    assert_eq!(r.aggregates[0].sum, 100);
}

#[test]
fn query_shard_multi_filter_combines_equals_and_range() {
    let tmp = tempfile::tempdir().expect("tmpdir");
    let mut w = DiskShardWriter::new(tmp.path(), (0, 0)).expect("new");
    w.add_int_field("vendor_id", vec![1, 2, 3, 4, 5])
        .expect("int");
    w.add_metric("fare", vec![10, 20, 30, 40, 50])
        .expect("metric");
    let mut country = BTreeMap::new();
    country.insert("us".to_string(), bitmap([0, 2, 4]));
    country.insert("de".to_string(), bitmap([1, 3]));
    w.add_string_field("country", country).expect("string");
    w.finalize().expect("finalize");

    // country=us → {0, 2, 4}; vendor_id 2..4 → {1, 2, 3}; AND → {2}, fare=30.
    let r = query_shard(
        tmp.path(),
        &[
            FilterClause::equals("country", "us"),
            FilterClause::range("vendor_id", Some("2"), Some("4")),
        ],
        &["fare"],
    )
    .expect("query_shard");
    assert_eq!(r.matched, 1);
    assert_eq!(r.aggregates[0].sum, 30);
}

#[test]
fn query_shard_multi_filter_short_circuits_when_one_clause_matches_zero() {
    let tmp = tempfile::tempdir().expect("tmpdir");
    let mut w = DiskShardWriter::new(tmp.path(), (0, 0)).expect("new");
    w.add_int_field("v", vec![1, 2, 3]).expect("int");
    w.add_metric("m", vec![10, 20, 30]).expect("metric");
    let mut country = BTreeMap::new();
    country.insert("us".to_string(), bitmap([0, 1, 2]));
    w.add_string_field("country", country).expect("string");
    w.finalize().expect("finalize");

    let r = query_shard(
        tmp.path(),
        &[
            FilterClause::equals("country", "us"),
            FilterClause::equals("country", "fr"), // not in dict
        ],
        &["m"],
    )
    .expect("query_shard");
    assert_eq!(r.matched, 0);
    assert_eq!(r.aggregates[0].sum, 0);
    assert!(r.aggregates[0].min.is_none());
}

#[test]
fn write_query_summary_renders_one_line_per_filter() {
    let tmp = tempfile::tempdir().expect("tmpdir");
    let mut w = DiskShardWriter::new(tmp.path(), (0, 0)).expect("new");
    w.add_int_field("v", vec![1, 2, 3]).expect("int");
    w.add_metric("m", vec![10, 20, 30]).expect("metric");
    let mut country = BTreeMap::new();
    country.insert("us".to_string(), bitmap([0, 1, 2]));
    w.add_string_field("country", country).expect("string");
    w.finalize().expect("finalize");

    let mut buf = Vec::new();
    query_cli(
        tmp.path(),
        &[
            FilterClause::equals("country", "us"),
            FilterClause::range("v", Some("1"), Some("2")),
        ],
        &["m"],
        Aggregate::Sum,
        &mut buf,
    )
    .expect("query_cli");
    let out = String::from_utf8(buf).expect("utf-8");
    assert!(out.contains("filter:   country = \"us\""), "{out}");
    assert!(out.contains("filter:   v = 1..2"), "{out}");
}
