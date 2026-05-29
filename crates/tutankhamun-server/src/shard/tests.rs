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
    filter: Option<(&str, &str)>,
    metrics: &[&str],
    aggregate: Aggregate,
    out: &mut dyn io::Write,
) -> Result<()> {
    let result = query_shard(path, filter, metrics)?;
    writeln!(out, "shard:    {}", path.display())?;
    write_query_summary(out, filter, metrics, aggregate, &result)?;
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
            },
            FieldSchema {
                name: "country".into(),
                kind: FieldKind::String,
            },
        ],
        content_hashes: std::collections::BTreeMap::default(),
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

    let r = query_shard(tmp.path(), None, &["clicks", "impressions"])
        .expect("query_shard multi-metric");
    assert_eq!(r.num_docs, 5);
    assert_eq!(r.matched, 5);
    assert_eq!(r.aggregates[0].sum, 150);
    assert_eq!(r.aggregates[1].sum, 1500);

    // Slot order follows input order — flipping the metrics flips the sums.
    let r = query_shard(tmp.path(), None, &["impressions", "clicks"])
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
        Some(("country", "us")),
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

    let err = query_shard(tmp.path(), None, &["clicks", "clicks"])
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
        Some(("country", "us")),
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
        Some(("country", "de")),
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
    query_cli(tmp.path(), None, &["clicks"], Aggregate::Sum, &mut buf).expect("query");
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
        Some(("country", "fr")),
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
        None,
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
    let err = query_cli(tmp.path(), None, &["country"], Aggregate::Sum, &mut buf)
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
        Some(("cuontry", "us")),
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
        Some(("clicks", "10")),
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
    let in_range: Vec<RoaringBitmap> = idx.range_bytes(&lo, &hi).map(|(_, bm)| bm).collect();
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

    let r = query_shard(tmp.path(), None, &["vendor_id"]).expect("query_shard");
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
    let r = query_shard(tmp.path(), Some(("vendor_id", "1")), &["vendor_id", "fare"])
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
        Some(("vendor_id", "not-a-number")),
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

    let r = query_shard(tmp.path(), None, &["v"]).expect("query_shard");
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
    let r = query_shard(tmp.path(), Some(("g", "1")), &["v"]).expect("query_shard");
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

    let r = query_shard(tmp.path(), Some(("c", "fr")), &["v"]).expect("query_shard");
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
        query_cli(tmp.path(), None, &["v"], op, &mut buf).expect("query");
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
        query_cli(tmp.path(), Some(("c", "fr")), &["v"], op, &mut buf).expect("query");
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
    };
    let b = MetricAggregates {
        sum: 40,
        min: Some(5),
        max: Some(25),
    };
    let mut acc = a;
    acc.absorb(&b);
    assert_eq!(acc.sum, 100);
    assert_eq!(acc.min, Some(5));
    assert_eq!(acc.max, Some(40));

    // None on one side is a no-op for that side (empty shard).
    let empty = MetricAggregates::default();
    let mut acc = a;
    acc.absorb(&empty);
    assert_eq!(acc.sum, 60);
    assert_eq!(acc.min, Some(10));
    assert_eq!(acc.max, Some(40));
}
