use std::collections::BTreeMap;
use std::path::Path;

use arrow::buffer::{BooleanBuffer, NullBuffer};
use roaring::RoaringBitmap;
use tempfile::TempDir;

use super::{
    Finalized, FtgsRow, StatSpec, StatValue, aggregate_docs, aggregate_docs_grouped, combine_stats,
    ftgs_scan, ftgs_scan_merge, merge_ftgs, merge_into, render_term,
};
use crate::group_lookup::GroupLookup;
use crate::shard::{DiskShard, DiskShardWriter, FieldKind, Shard};

/// Finalize a scalar stat to its `i64` output — panics via
/// [`Finalized::int_opt`](super::Finalized::int_opt) if it isn't an `Int`-kind
/// stat, which the scalar tests never produce. These helpers only feed
/// non-NULL stats, so the `None` (all-NULL) case is unwrapped.
fn fin_int(v: &StatValue) -> i64 {
    v.finalize()
        .int_opt()
        .expect("scalar test stat is non-NULL")
}

/// Postings map from a doc→term assignment given in doc-id order.
fn postings(terms: &[&str]) -> BTreeMap<String, RoaringBitmap> {
    let mut map: BTreeMap<String, RoaringBitmap> = BTreeMap::new();
    for (doc, term) in terms.iter().enumerate() {
        map.entry((*term).to_string())
            .or_default()
            .insert(u32::try_from(doc).unwrap());
    }
    map
}

/// 5-doc shard: `country` (string) + `hour` (int) + `revenue` (metric).
/// Doc:        0     1     2     3     4
/// country:    US    US    UK    US    UK
/// hour:       9     9     10    11    10
/// revenue:    10    20    30    40    50
fn write_dataset(dir: &Path) -> DiskShard {
    let mut w = DiskShardWriter::new(dir, (0, 0)).expect("new");
    w.add_string_field("country", postings(&["US", "US", "UK", "US", "UK"]))
        .expect("country");
    w.add_int_field("hour", vec![9, 9, 10, 11, 10])
        .expect("hour");
    w.add_metric("revenue", vec![10, 20, 30, 40, 50])
        .expect("revenue");
    w.finalize().expect("finalize");
    DiskShard::open(dir).expect("open")
}

/// One shard with a `country` string field + `revenue` metric.
fn write_country_shard(dir: &Path, countries: &[&str], revenue: Vec<i64>) -> DiskShard {
    let mut w = DiskShardWriter::new(dir, (0, 0)).expect("new");
    w.add_string_field("country", postings(countries))
        .expect("country");
    w.add_metric("revenue", revenue).expect("revenue");
    w.finalize().expect("finalize");
    DiskShard::open(dir).expect("open")
}

/// One shard with an `hour` int field + `revenue` metric.
fn write_hour_shard(dir: &Path, hours: Vec<i64>, revenue: Vec<i64>) -> DiskShard {
    let mut w = DiskShardWriter::new(dir, (0, 0)).expect("new");
    w.add_int_field("hour", hours).expect("hour");
    w.add_metric("revenue", revenue).expect("revenue");
    w.finalize().expect("finalize");
    DiskShard::open(dir).expect("open")
}

/// Groups: doc0=1 doc1=1 doc2=2 doc3=2 doc4=1.
fn groups_5() -> GroupLookup {
    let mut g = GroupLookup::all_in_one_group(5);
    for (doc, group) in [(0, 1), (1, 1), (2, 2), (3, 2), (4, 1)] {
        g.set(doc, group);
    }
    g
}

/// Fresh temp dir + the standard dataset. The `TempDir` is returned so
/// the caller keeps it alive for the shard's lifetime.
fn setup() -> (TempDir, DiskShard) {
    let tmp = TempDir::new().unwrap();
    let shard = write_dataset(tmp.path());
    (tmp, shard)
}

/// Field kinds in the test datasets, for rendering raw term keys.
fn kind_of(field: &str) -> FieldKind {
    match field {
        "hour" => FieldKind::Int,
        _ => FieldKind::String,
    }
}

/// Render FTGS output to comparable `(field, term, group, stats)` tuples
/// — `FtgsRow::term` is the raw FST key.
fn rendered(rows: &[FtgsRow]) -> Vec<(String, String, u32, Vec<i64>)> {
    rows.iter()
        .map(|r| {
            (
                r.field.clone(),
                render_term(kind_of(&r.field), &r.term),
                r.group,
                r.stats.iter().map(fin_int).collect(),
            )
        })
        .collect()
}

fn row(field: &str, term: &str, group: u32, stats: &[i64]) -> (String, String, u32, Vec<i64>) {
    (field.to_string(), term.to_string(), group, stats.to_vec())
}

#[test]
fn sum_by_string_field_is_field_term_group_ordered() {
    let (_tmp, shard) = setup();
    let rows = ftgs_scan(
        &shard,
        &groups_5(),
        &["country"],
        &[StatSpec::Sum("revenue")],
    )
    .unwrap();

    // Terms ascending ("UK" < "US"), groups ascending within a term.
    // UK docs {2,4}: g2 doc2=30, g1 doc4=50.
    // US docs {0,1,3}: g1 docs0,1=10+20, g2 doc3=40.
    assert_eq!(
        rendered(&rows),
        vec![
            row("country", "UK", 1, &[50]),
            row("country", "UK", 2, &[30]),
            row("country", "US", 1, &[30]),
            row("country", "US", 2, &[40]),
        ]
    );
}

#[test]
fn int_group_by_renders_decimal_terms() {
    let (_tmp, shard) = setup();
    let rows = ftgs_scan(&shard, &groups_5(), &["hour"], &[StatSpec::Sum("revenue")]).unwrap();

    // hour 9 docs {0,1} both g1 -> 30; hour 10 docs {2,4}: g2=30, g1=50;
    // hour 11 doc {3} g2 -> 40. Terms numerically sorted (FST byte order).
    assert_eq!(
        rendered(&rows),
        vec![
            row("hour", "9", 1, &[30]),
            row("hour", "10", 1, &[50]),
            row("hour", "10", 2, &[30]),
            row("hour", "11", 2, &[40]),
        ]
    );
}

#[test]
fn aggregate_docs_grouped_buckets_by_group() {
    let (_tmp, shard) = setup();
    // groups_5: docs→groups [1,1,2,2,1]; revenue [10,20,30,40,50].
    let out =
        aggregate_docs_grouped(&shard, 0..5, &groups_5(), &[StatSpec::Sum("revenue")]).unwrap();
    let finalized: Vec<(u32, Vec<i64>)> = out
        .iter()
        .map(|(&g, st)| (g, st.iter().map(fin_int).collect()))
        .collect();
    // group 1 = {0,1,4} = 10+20+50 = 80; group 2 = {2,3} = 30+40 = 70.
    assert_eq!(finalized, vec![(1, vec![80]), (2, vec![70])]);
}

#[test]
fn nullable_metric_skips_nulls_and_all_null_group_is_null() {
    let tmp = TempDir::new().unwrap();
    let dir = tmp.path();
    let mut w = DiskShardWriter::new(dir, (0, 0)).expect("new");
    // revenue NULL at docs 1 and 3 (0 placeholders in the dense buffer).
    let validity = NullBuffer::new(BooleanBuffer::from(vec![true, false, true, false, true]));
    w.add_forward_column(
        "revenue",
        vec![10, 0, 30, 0, 50],
        FieldKind::Metric,
        Some(validity),
    )
    .expect("revenue");
    w.finalize().expect("finalize");
    let shard = DiskShard::open(dir).expect("open");

    // groups: {0,1,4}→g1, {2}→g2, {3}→g3 (g3's only doc is NULL).
    let mut g = GroupLookup::all_in_one_group(5);
    for (doc, group) in [(0, 1), (1, 1), (2, 2), (3, 3), (4, 1)] {
        g.set(doc, group);
    }
    let out = aggregate_docs_grouped(
        &shard,
        0..5,
        &g,
        &[
            StatSpec::Sum("revenue"),
            StatSpec::Min("revenue"),
            StatSpec::Max("revenue"),
            StatSpec::Avg("revenue"),
            StatSpec::ApproxPercentile("revenue", 0.5, 100),
        ],
    )
    .unwrap();

    // g1 = {10, NULL, 50}: nulls skipped → sum 60, min 10, max 50, avg 60/2.
    let g1 = &out[&1];
    assert!(matches!(g1[0], StatValue::Scalar(60)));
    assert!(matches!(g1[1], StatValue::Scalar(10)));
    assert!(matches!(g1[2], StatValue::Scalar(50)));
    // avg carries (sum, non-null count) un-finalized → 60/2.
    assert!(matches!(g1[3], StatValue::Avg { sum: 60, count: 2 }));
    // percentile buffered the two non-null values → a real digest, not NULL.
    assert!(matches!(g1[4], StatValue::TDigest { .. }));
    // g2 = {30}.
    assert!(matches!(out[&2][0], StatValue::Scalar(30)));
    // g3 = {NULL}: every stat — scalar AND the percentile sketch — is SQL NULL,
    // not 0 / i64::MAX / NaN / an empty-digest 0.
    for st in &out[&3] {
        assert!(matches!(st, StatValue::Null), "all-NULL group must be NULL");
    }
    assert!(matches!(out[&3][4].finalize(), Finalized::Null));
}

#[test]
fn approx_percentile_estimates_quantile() {
    let (_tmp, shard) = setup();
    // revenue {10,20,30,40,50}: median 30 (t-digest keeps all points exact
    // at this size, so within a tight band).
    let spec = [StatSpec::ApproxPercentile("revenue", 0.5, 100)];
    let out = aggregate_docs(&shard, 0..5, &spec).unwrap();
    let p50 = fin_int(&out[0]);
    assert!((25..=35).contains(&p50), "p50 = {p50}");
}

#[test]
fn approx_percentile_merges_across_shards() {
    let tmp = TempDir::new().unwrap();
    let s1 = write_hour_shard(&tmp.path().join("a"), vec![0, 0, 0], vec![10, 20, 30]);
    let s2 = write_hour_shard(&tmp.path().join("b"), vec![0, 0, 0], vec![40, 50, 60]);

    // Combine the per-shard digests, as the cross-shard merge does.
    let spec = [StatSpec::ApproxPercentile("revenue", 0.5, 100)];
    let mut a = aggregate_docs(&s1, 0..3, &spec).unwrap();
    let b = aggregate_docs(&s2, 0..3, &spec).unwrap();
    combine_stats(&mut a, &b, &spec);

    // Combined {10,20,30,40,50,60}: median 35.
    let p50 = fin_int(&a[0]);
    assert!((30..=40).contains(&p50), "merged p50 = {p50}");
}

/// Decode a `StatValue::TopK`'s value-key bytes to `(String, count)`.
fn topk_rendered(stat: &StatValue) -> Vec<(String, i64)> {
    let StatValue::TopK { items, .. } = stat else {
        panic!("expected TopK");
    };
    items
        .iter()
        .map(|(b, c)| (render_term(FieldKind::String, b), *c))
        .collect()
}

#[test]
fn approx_top_k_counts_terms() {
    let (_tmp, shard) = setup();
    // country: US 3 (docs 0,1,3), UK 2 (docs 2,4) → count desc.
    let out = aggregate_docs(&shard, 0..5, &[StatSpec::TopK("country", 2, 100)]).unwrap();
    assert_eq!(
        topk_rendered(&out[0]),
        vec![("US".to_string(), 3), ("UK".to_string(), 2)]
    );
}

#[test]
fn approx_top_k_merges_across_shards() {
    let tmp = TempDir::new().unwrap();
    let s1 = write_country_shard(&tmp.path().join("a"), &["US", "US", "UK"], vec![0, 0, 0]);
    let s2 = write_country_shard(&tmp.path().join("b"), &["UK", "UK", "US"], vec![0, 0, 0]);

    let spec = [StatSpec::TopK("country", 2, 100)];
    let mut a = aggregate_docs(&s1, 0..3, &spec).unwrap();
    let b = aggregate_docs(&s2, 0..3, &spec).unwrap();
    combine_stats(&mut a, &b, &spec);

    // US 3, UK 3 → tie broken by value asc ("UK" < "US").
    assert_eq!(
        topk_rendered(&a[0]),
        vec![("UK".to_string(), 3), ("US".to_string(), 3)]
    );
}

#[test]
fn combine_stats_treats_null_as_merge_identity() {
    let spec = [StatSpec::Sum("x"), StatSpec::Min("x")];
    // A group valued in one shard, all-NULL in the other (each direction).
    let mut acc = vec![StatValue::Scalar(10), StatValue::Null];
    let other = vec![StatValue::Null, StatValue::Scalar(7)];
    combine_stats(&mut acc, &other, &spec);
    assert!(matches!(acc[0], StatValue::Scalar(10))); // value ∘ NULL = value
    assert!(matches!(acc[1], StatValue::Scalar(7))); // NULL ∘ value = value

    // NULL ∘ NULL stays NULL.
    let mut both_null = vec![StatValue::Null];
    combine_stats(&mut both_null, &[StatValue::Null], &spec[..1]);
    assert!(matches!(both_null[0], StatValue::Null));
}

#[test]
fn theta_counts_and_merges_across_shards() {
    let tmp = TempDir::new().unwrap();
    let s1 = write_hour_shard(&tmp.path().join("a"), vec![0, 1, 2], vec![0, 0, 0]);
    let s2 = write_hour_shard(&tmp.path().join("b"), vec![2, 3, 4], vec![0, 0, 0]);

    let spec = [StatSpec::Theta("hour", 4096)];
    // hour distinct per shard: s1 {0,1,2}=3, s2 {2,3,4}=3.
    let mut a = aggregate_docs(&s1, 0..3, &spec).unwrap();
    let StatValue::Theta(sa) = &a[0] else {
        panic!()
    };
    assert_eq!(sa.estimate(), 3);

    let b = aggregate_docs(&s2, 0..3, &spec).unwrap();
    combine_stats(&mut a, &b, &spec);
    // union {0,1,2,3,4} = 5 (hour 2 shared, deduped).
    let StatValue::Theta(merged) = &a[0] else {
        panic!()
    };
    assert_eq!(merged.estimate(), 5);
}

#[test]
fn group_zero_is_excluded() {
    let (_tmp, shard) = setup();
    // Move doc1 (US, revenue 20) into the filtered-out group 0.
    let mut g = groups_5();
    g.set(1, 0);
    let rows = ftgs_scan(&shard, &g, &["country"], &[StatSpec::Sum("revenue")]).unwrap();

    // (US, g1) now holds only doc0 -> 10; doc1 contributes nowhere.
    assert_eq!(
        rendered(&rows),
        vec![
            row("country", "UK", 1, &[50]),
            row("country", "UK", 2, &[30]),
            row("country", "US", 1, &[10]),
            row("country", "US", 2, &[40]),
        ]
    );
}

#[test]
fn count_min_max_operators() {
    let (_tmp, shard) = setup();
    let stats = [
        StatSpec::Count,
        StatSpec::Sum("revenue"),
        StatSpec::Min("revenue"),
        StatSpec::Max("revenue"),
    ];
    let rows = ftgs_scan(&shard, &groups_5(), &["country"], &stats).unwrap();

    let find = |term: &str, group: u32| {
        rows.iter()
            .find(|r| render_term(kind_of(&r.field), &r.term) == term && r.group == group)
            .unwrap()
            .stats
            .iter()
            .map(fin_int)
            .collect::<Vec<i64>>()
    };

    // (US, g1) = docs {0,1}: count 2, sum 30, min 10, max 20.
    let stats = find("US", 1);
    assert_eq!(stats, vec![2, 30, 10, 20]);
    // avg derives from sum/count.
    assert_eq!(stats[1] / stats[0], 15);

    // (UK, g1) = doc {4}: count 1, sum 50, min 50, max 50.
    assert_eq!(find("UK", 1), vec![1, 50, 50, 50]);
}

#[test]
fn multiple_group_by_fields_emit_in_caller_order() {
    let (_tmp, shard) = setup();
    let rows = ftgs_scan(
        &shard,
        &groups_5(),
        &["hour", "country"],
        &[StatSpec::Count],
    )
    .unwrap();

    // All `hour` rows precede all `country` rows (caller order).
    let fields: Vec<&str> = rows.iter().map(|r| r.field.as_str()).collect();
    let first_country = fields.iter().position(|f| *f == "country").unwrap();
    assert!(fields[..first_country].iter().all(|f| *f == "hour"));
    assert!(fields[first_country..].iter().all(|f| *f == "country"));
}

#[test]
fn rejects_group_lookup_length_mismatch() {
    let (_tmp, shard) = setup();
    let groups = GroupLookup::all_in_one_group(4); // shard has 5 docs
    let err = ftgs_scan(&shard, &groups, &["country"], &[StatSpec::Count]).unwrap_err();
    assert!(err.to_string().contains("does not match shard num_docs"));
}

#[test]
fn rejects_group_by_on_a_metric_field() {
    let (_tmp, shard) = setup();
    // `revenue` is a Metric — forward column only, not a group-by field.
    let err = ftgs_scan(&shard, &groups_5(), &["revenue"], &[StatSpec::Count]).unwrap_err();
    assert!(err.to_string().contains("expected string or int"));
}

#[test]
fn rejects_unknown_stat_column() {
    let (_tmp, shard) = setup();
    let err = ftgs_scan(&shard, &groups_5(), &["country"], &[StatSpec::Sum("nope")]).unwrap_err();
    assert!(err.to_string().contains("no forward column"));
}

// ---- cross-shard merge ----

#[test]
fn int_terms_merge_in_numeric_order_across_shards() {
    let (t1, t2) = (TempDir::new().unwrap(), TempDir::new().unwrap());
    let s1 = write_hour_shard(t1.path(), vec![9, 10, 11], vec![1, 1, 1]);
    let s2 = write_hour_shard(t2.path(), vec![9, 10, 11], vec![1, 1, 1]);
    let (g1, g2) = (
        GroupLookup::all_in_one_group(3),
        GroupLookup::all_in_one_group(3),
    );
    let shards: [(&dyn Shard, &GroupLookup); 2] = [(&s1, &g1), (&s2, &g2)];
    let merged = ftgs_scan_merge(&shards, &["hour"], &[StatSpec::Sum("revenue")]).unwrap();

    // The decision's proof: terms in numeric order, NOT decimal-string
    // lexical order ("10","11","9"). Each hour's revenue sums across both.
    assert_eq!(
        rendered(&merged),
        vec![
            row("hour", "9", 1, &[2]),
            row("hour", "10", 1, &[2]),
            row("hour", "11", 1, &[2]),
        ]
    );
}

#[test]
fn overlapping_keys_combine_all_operators() {
    let (t1, t2) = (TempDir::new().unwrap(), TempDir::new().unwrap());
    // Both shards: all docs country=US, group 1.
    let s1 = write_country_shard(t1.path(), &["US", "US"], vec![10, 20]);
    let s2 = write_country_shard(t2.path(), &["US", "US"], vec![5, 40]);
    let (g1, g2) = (
        GroupLookup::all_in_one_group(2),
        GroupLookup::all_in_one_group(2),
    );
    let stats = [
        StatSpec::Count,
        StatSpec::Sum("revenue"),
        StatSpec::Min("revenue"),
        StatSpec::Max("revenue"),
    ];
    let shards: [(&dyn Shard, &GroupLookup); 2] = [(&s1, &g1), (&s2, &g2)];
    let merged = ftgs_scan_merge(&shards, &["country"], &stats).unwrap();

    // (US, g1): count 2+2=4, sum 30+45=75, min min(10,5)=5, max max(20,40)=40.
    assert_eq!(
        rendered(&merged),
        vec![row("country", "US", 1, &[4, 75, 5, 40])]
    );
}

#[test]
fn disjoint_terms_interleave_sorted() {
    let (t1, t2) = (TempDir::new().unwrap(), TempDir::new().unwrap());
    let s1 = write_country_shard(t1.path(), &["US"], vec![10]);
    let s2 = write_country_shard(t2.path(), &["UK"], vec![20]);
    let (g1, g2) = (
        GroupLookup::all_in_one_group(1),
        GroupLookup::all_in_one_group(1),
    );
    let shards: [(&dyn Shard, &GroupLookup); 2] = [(&s1, &g1), (&s2, &g2)];
    let merged = ftgs_scan_merge(&shards, &["country"], &[StatSpec::Sum("revenue")]).unwrap();

    // UK before US (sorted), no key shared so nothing combines.
    assert_eq!(
        rendered(&merged),
        vec![
            row("country", "UK", 1, &[20]),
            row("country", "US", 1, &[10]),
        ]
    );
}

#[test]
fn fans_out_and_combines_across_three_shards() {
    let (t1, t2, t3) = (
        TempDir::new().unwrap(),
        TempDir::new().unwrap(),
        TempDir::new().unwrap(),
    );
    // Partially overlapping hours so combining spans non-adjacent shards.
    let s1 = write_hour_shard(t1.path(), vec![9, 11], vec![10, 10]);
    let s2 = write_hour_shard(t2.path(), vec![10], vec![20]);
    let s3 = write_hour_shard(t3.path(), vec![9, 10, 11], vec![1, 1, 1]);
    let (g1, g2, g3) = (
        GroupLookup::all_in_one_group(2),
        GroupLookup::all_in_one_group(1),
        GroupLookup::all_in_one_group(3),
    );
    let shards: [(&dyn Shard, &GroupLookup); 3] = [(&s1, &g1), (&s2, &g2), (&s3, &g3)];
    let merged = ftgs_scan_merge(&shards, &["hour"], &[StatSpec::Sum("revenue")]).unwrap();

    // hour 9: s1 10 + s3 1 = 11; hour 10: s2 20 + s3 1 = 21;
    // hour 11: s1 10 + s3 1 = 11. Numeric order across all three.
    assert_eq!(
        rendered(&merged),
        vec![
            row("hour", "9", 1, &[11]),
            row("hour", "10", 1, &[21]),
            row("hour", "11", 1, &[11]),
        ]
    );
}

#[test]
fn single_shard_merge_equals_scan() {
    let (_tmp, shard) = setup();
    let groups = groups_5();
    let scanned = ftgs_scan(&shard, &groups, &["country"], &[StatSpec::Sum("revenue")]).unwrap();
    let merged = merge_ftgs(
        vec![scanned.clone()],
        &["country"],
        &[StatSpec::Sum("revenue")],
    );
    assert_eq!(rendered(&merged), rendered(&scanned));
}

#[test]
fn merge_into_fold_equals_merge_all_at_once() {
    // Streaming the merge batch-by-batch (`merge_into`) must equal merging
    // every partial in one shot (`merge_ftgs`) — the associativity guard the
    // bounded-memory aggregate fold relies on.
    let (_tmp, shard) = setup();
    let gb = ["country"];
    let specs = [StatSpec::Sum("revenue")];
    let scan = ftgs_scan(&shard, &groups_5(), &gb, &specs).unwrap();

    let all_at_once = merge_ftgs(vec![scan.clone(), scan.clone(), scan.clone()], &gb, &specs);
    let mut folded: Vec<FtgsRow> = Vec::new();
    for _ in 0..3 {
        folded = merge_into(folded, vec![scan.clone()], &gb, &specs);
    }
    assert_eq!(rendered(&folded), rendered(&all_at_once));
}

#[test]
fn approx_count_distinct_per_group() {
    let (_tmp, shard) = setup();
    let rows = ftgs_scan(
        &shard,
        &groups_5(),
        &["country"],
        &[StatSpec::ApproxCountDistinct("revenue")],
    )
    .unwrap();

    // revenue per (country, group); distinct counts are exact in HLL's
    // sparse regime. UK g1 = doc4 {50} = 1; UK g2 = doc2 {30} = 1;
    // US g1 = docs0,1 {10,20} = 2; US g2 = doc3 {40} = 1.
    assert_eq!(
        rendered(&rows),
        vec![
            row("country", "UK", 1, &[1]),
            row("country", "UK", 2, &[1]),
            row("country", "US", 1, &[2]),
            row("country", "US", 2, &[1]),
        ]
    );
}

#[test]
fn approx_count_distinct_unions_sketches_across_shards() {
    let (t1, t2) = (TempDir::new().unwrap(), TempDir::new().unwrap());
    // Same country term in both shards, overlapping revenue values.
    let s1 = write_country_shard(t1.path(), &["us", "us", "us"], vec![10, 20, 30]);
    let s2 = write_country_shard(t2.path(), &["us", "us", "us"], vec![20, 30, 40]);
    let (g1, g2) = (
        GroupLookup::all_in_one_group(3),
        GroupLookup::all_in_one_group(3),
    );
    let shards: [(&dyn Shard, &GroupLookup); 2] = [(&s1, &g1), (&s2, &g2)];
    let merged = crate::ftgs::ftgs_scan_merge(
        &shards,
        &["country"],
        &[StatSpec::ApproxCountDistinct("revenue")],
    )
    .unwrap();

    // Union of {10,20,30} and {20,30,40} = {10,20,30,40} = 4 distinct —
    // merging the HLL registers, not the per-shard counts (3 + 3 ≠ 4).
    assert_eq!(rendered(&merged), vec![row("country", "us", 1, &[4])]);
}
