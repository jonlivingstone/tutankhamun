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

#[test]
fn query_sums_metric_filtered_by_term() {
    let tmp = tempfile::tempdir().expect("tmpdir");
    write_query_fixture(tmp.path());

    let mut buf = Vec::new();
    query(tmp.path(), Some(("country", "us")), "clicks", &mut buf).expect("query us");
    let out = String::from_utf8(buf).expect("utf-8");
    assert!(out.contains("matched:  3 / 5 docs"), "{out}");
    assert!(out.contains("clicks:   sum = 90"), "{out}");

    let mut buf = Vec::new();
    query(tmp.path(), Some(("country", "de")), "clicks", &mut buf).expect("query de");
    let out = String::from_utf8(buf).expect("utf-8");
    assert!(out.contains("matched:  2 / 5 docs"), "{out}");
    assert!(out.contains("clicks:   sum = 60"), "{out}");
}

#[test]
fn query_no_filter_sums_all_docs() {
    let tmp = tempfile::tempdir().expect("tmpdir");
    write_query_fixture(tmp.path());

    let mut buf = Vec::new();
    query(tmp.path(), None, "clicks", &mut buf).expect("query");
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
    query(tmp.path(), Some(("country", "fr")), "clicks", &mut buf).expect("query fr");
    let out = String::from_utf8(buf).expect("utf-8");
    assert!(out.contains("matched:  0 / 5 docs"), "{out}");
    assert!(out.contains("clicks:   sum = 0"), "{out}");
}

#[test]
fn query_rejects_unknown_metric() {
    let tmp = tempfile::tempdir().expect("tmpdir");
    write_query_fixture(tmp.path());

    let mut buf = Vec::new();
    let err = query(tmp.path(), None, "no_such_metric", &mut buf)
        .expect_err("expected unknown-metric error");
    let msg = err.to_string();
    assert!(msg.contains("no_such_metric"), "{msg}");
}

#[test]
fn query_rejects_string_field_as_metric() {
    let tmp = tempfile::tempdir().expect("tmpdir");
    write_query_fixture(tmp.path());

    let mut buf = Vec::new();
    let err = query(tmp.path(), None, "country", &mut buf).expect_err("expected wrong-kind error");
    let msg = err.to_string();
    assert!(
        msg.contains("country") && msg.to_lowercase().contains("not a metric"),
        "{msg}"
    );
}

#[test]
fn query_rejects_unknown_filter_field() {
    let tmp = tempfile::tempdir().expect("tmpdir");
    write_query_fixture(tmp.path());

    let mut buf = Vec::new();
    let err = query(tmp.path(), Some(("cuontry", "us")), "clicks", &mut buf)
        .expect_err("expected unknown-field error");
    let msg = err.to_string();
    assert!(msg.contains("cuontry"), "{msg}");
}

#[test]
fn query_rejects_metric_field_as_filter() {
    let tmp = tempfile::tempdir().expect("tmpdir");
    write_query_fixture(tmp.path());

    let mut buf = Vec::new();
    let err = query(tmp.path(), Some(("clicks", "10")), "clicks", &mut buf)
        .expect_err("expected wrong-kind error");
    let msg = err.to_string();
    assert!(
        msg.contains("clicks") && msg.to_lowercase().contains("not a string"),
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
