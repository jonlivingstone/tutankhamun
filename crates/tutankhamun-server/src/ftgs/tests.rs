use std::collections::BTreeMap;
use std::path::Path;

use roaring::RoaringBitmap;
use tempfile::TempDir;

use super::{FtgsRow, StatSpec, ftgs_scan};
use crate::group_lookup::GroupLookup;
use crate::shard::{DiskShard, DiskShardWriter};

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

fn row(field: &str, term: &str, group: u32, stats: &[i64]) -> FtgsRow {
    FtgsRow {
        field: field.to_string(),
        term: term.to_string(),
        group,
        stats: stats.to_vec(),
    }
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
        rows,
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
    // hour 11 doc {3} g2 -> 40. Terms render as decimal, numerically sorted.
    assert_eq!(
        rows,
        vec![
            row("hour", "9", 1, &[30]),
            row("hour", "10", 1, &[50]),
            row("hour", "10", 2, &[30]),
            row("hour", "11", 2, &[40]),
        ]
    );
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
        rows,
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
            .find(|r| r.term == term && r.group == group)
            .unwrap()
            .stats
            .clone()
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
