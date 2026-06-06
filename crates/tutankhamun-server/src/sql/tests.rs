//! In-process `DataFusion` integration tests: register
//! [`TutankhamunTableProvider`] against a real on-disk shard
//! tree and run SQL through `DataFusion`'s planner end-to-end.

use std::collections::BTreeMap;
use std::path::Path;
use std::sync::Arc;

use arrow::array::Array;
use datafusion::catalog::TableProvider;
use datafusion::prelude::SessionContext;
use roaring::RoaringBitmap;
use tempfile::TempDir;

use super::TutankhamunTableProvider;
use crate::cache::Cache;
use crate::shard::DiskShardWriter;
use crate::storage::StorageRegistry;

/// Build a two-shard dataset at `root`:
/// - shard A: `vendor_id` [1, 2, 1, 2], fare [100, 200, 300, 400],
///   country: {us → 0,2; de → 1,3}.
/// - shard B: `vendor_id` [3, 3], fare [500, 600], country: {us → 0;
///   de → 1}.
fn write_two_shard_dataset(root: &Path) {
    let shard_a = root.join("a");
    let mut w = DiskShardWriter::new(&shard_a, (0, 0)).expect("new a");
    w.add_int_field("vendor_id", vec![1, 2, 1, 2])
        .expect("int a");
    w.add_metric("fare", vec![100, 200, 300, 400])
        .expect("metric a");
    let mut country = BTreeMap::new();
    country.insert("us".to_string(), bitmap([0, 2]));
    country.insert("de".to_string(), bitmap([1, 3]));
    w.add_string_field("country", country).expect("string a");
    w.finalize().expect("finalize a");

    let shard_b = root.join("b");
    let mut w = DiskShardWriter::new(&shard_b, (0, 0)).expect("new b");
    w.add_int_field("vendor_id", vec![3, 3]).expect("int b");
    w.add_metric("fare", vec![500, 600]).expect("metric b");
    let mut country = BTreeMap::new();
    country.insert("us".to_string(), bitmap([0]));
    country.insert("de".to_string(), bitmap([1]));
    w.add_string_field("country", country).expect("string b");
    w.finalize().expect("finalize b");
}

fn bitmap(docs: impl IntoIterator<Item = u32>) -> RoaringBitmap {
    let mut bm = RoaringBitmap::new();
    bm.extend(docs);
    bm
}

async fn provider_for(root: &Path) -> (TutankhamunTableProvider, TempDir) {
    let url = url::Url::from_directory_path(root)
        .expect("absolute")
        .to_string();
    let registry = StorageRegistry::from_url(&url).expect("registry");
    let cache_dir = tempfile::tempdir().expect("cache tmpdir");
    let cache = Arc::new(
        Cache::open(
            cache_dir.path().to_path_buf(),
            registry.store(),
            url.clone(),
            u64::MAX,
        )
        .expect("cache"),
    );
    let provider = TutankhamunTableProvider::try_new(url, cache)
        .await
        .expect("provider");
    (provider, cache_dir)
}

#[tokio::test(flavor = "multi_thread")]
async fn select_star_returns_all_forward_columns() {
    let tmp = tempfile::tempdir().expect("tmpdir");
    write_two_shard_dataset(tmp.path());
    let (provider, _cache_dir) = provider_for(tmp.path()).await;

    let ctx = SessionContext::new();
    ctx.register_table("trips", Arc::new(provider)).unwrap();

    let df = ctx.sql("SELECT vendor_id, fare FROM trips").await.unwrap();
    let batches = df.collect().await.unwrap();
    let total_rows: usize = batches
        .iter()
        .map(arrow::array::RecordBatch::num_rows)
        .sum();
    assert_eq!(total_rows, 6, "two shards × {{4, 2}} rows");
}

#[tokio::test(flavor = "multi_thread")]
async fn sum_aggregation_across_shards() {
    let tmp = tempfile::tempdir().expect("tmpdir");
    write_two_shard_dataset(tmp.path());
    let (provider, _cache_dir) = provider_for(tmp.path()).await;

    let ctx = SessionContext::new();
    ctx.register_table("trips", Arc::new(provider)).unwrap();

    let df = ctx
        .sql("SELECT sum(fare) AS total FROM trips")
        .await
        .unwrap();
    let batches = df.collect().await.unwrap();
    assert_eq!(batches.len(), 1);
    let total = batches[0]
        .column(0)
        .as_any()
        .downcast_ref::<arrow::array::Int64Array>()
        .unwrap()
        .value(0);
    // 100 + 200 + 300 + 400 + 500 + 600 = 2100
    assert_eq!(total, 2100);
}

#[tokio::test(flavor = "multi_thread")]
async fn where_country_equals_pushes_down() {
    let tmp = tempfile::tempdir().expect("tmpdir");
    write_two_shard_dataset(tmp.path());
    let (provider, _cache_dir) = provider_for(tmp.path()).await;

    let ctx = SessionContext::new();
    ctx.register_table("trips", Arc::new(provider)).unwrap();

    // country=us → shard A docs {0, 2} (fare 100, 300) +
    // shard B doc {0} (fare 500) = sum 900.
    let df = ctx
        .sql("SELECT sum(fare) FROM trips WHERE country = 'us'")
        .await
        .unwrap();
    let batches = df.collect().await.unwrap();
    let total = batches[0]
        .column(0)
        .as_any()
        .downcast_ref::<arrow::array::Int64Array>()
        .unwrap()
        .value(0);
    assert_eq!(total, 900);
}

#[tokio::test(flavor = "multi_thread")]
async fn where_int_equals_pushes_down() {
    // Int equality pushes down: `scalar_to_string` renders the
    // literal as decimal text and the engine's Int-field parser
    // resolves it against the inverted index.
    let tmp = tempfile::tempdir().expect("tmpdir");
    write_two_shard_dataset(tmp.path());
    let (provider, _cache_dir) = provider_for(tmp.path()).await;

    let ctx = SessionContext::new();
    ctx.register_table("trips", Arc::new(provider)).unwrap();

    let df = ctx
        .sql("SELECT sum(fare) FROM trips WHERE vendor_id = 1")
        .await
        .unwrap();
    let batches = df.collect().await.unwrap();
    let total = batches[0]
        .column(0)
        .as_any()
        .downcast_ref::<arrow::array::Int64Array>()
        .unwrap()
        .value(0);
    // vendor_id=1 lives only in shard A, at docs {0, 2}: fare 100+300=400.
    assert_eq!(total, 400);
}

#[tokio::test(flavor = "multi_thread")]
async fn where_combines_string_and_int_pushdown() {
    let tmp = tempfile::tempdir().expect("tmpdir");
    write_two_shard_dataset(tmp.path());
    let (provider, _cache_dir) = provider_for(tmp.path()).await;

    let ctx = SessionContext::new();
    ctx.register_table("trips", Arc::new(provider)).unwrap();

    // country=us ∩ vendor_id=2 in shard A: doc 2 hmm — country=us
    // is {0, 2}, vendor_id=2 is doc IDs where the int field has
    // value 2, i.e. {1, 3}. Intersection: empty. So:
    let df = ctx
        .sql("SELECT count(*) FROM trips WHERE country = 'us' AND vendor_id = 2")
        .await
        .unwrap();
    let batches = df.collect().await.unwrap();
    let n = batches[0]
        .column(0)
        .as_any()
        .downcast_ref::<arrow::array::Int64Array>()
        .unwrap()
        .value(0);
    assert_eq!(n, 0);
}

#[tokio::test(flavor = "multi_thread")]
async fn projection_only_requested_columns() {
    let tmp = tempfile::tempdir().expect("tmpdir");
    write_two_shard_dataset(tmp.path());
    let (provider, _cache_dir) = provider_for(tmp.path()).await;

    let ctx = SessionContext::new();
    ctx.register_table("trips", Arc::new(provider)).unwrap();

    let df = ctx.sql("SELECT fare FROM trips").await.unwrap();
    let batches = df.collect().await.unwrap();
    for batch in &batches {
        assert_eq!(batch.num_columns(), 1, "only fare projected");
        assert_eq!(batch.schema().field(0).name(), "fare");
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn string_field_selectable_via_reverse_lookup() {
    // String columns are exposed as Utf8 in the schema and
    // reconstructed per-doc from the inverted index at scan time.
    // SELECT country returns the right value for every doc.
    let tmp = tempfile::tempdir().expect("tmpdir");
    write_two_shard_dataset(tmp.path());
    let (provider, _cache_dir) = provider_for(tmp.path()).await;

    let ctx = SessionContext::new();
    ctx.register_table("trips", Arc::new(provider)).unwrap();

    let df = ctx
        .sql("SELECT country, count(*) AS n FROM trips GROUP BY country ORDER BY country")
        .await
        .unwrap();
    let batches = df.collect().await.unwrap();
    assert_eq!(batches.len(), 1);
    let countries = batches[0]
        .column(0)
        .as_any()
        .downcast_ref::<arrow::array::StringArray>()
        .unwrap();
    let counts = batches[0]
        .column(1)
        .as_any()
        .downcast_ref::<arrow::array::Int64Array>()
        .unwrap();
    assert_eq!(countries.value(0), "de");
    assert_eq!(countries.value(1), "us");
    // Shard A: us={0,2}, de={1,3} → 2 each.
    // Shard B: us={0}, de={1} → 1 each.
    assert_eq!(counts.value(0), 3); // de
    assert_eq!(counts.value(1), 3); // us
}

#[tokio::test(flavor = "multi_thread")]
async fn where_strict_greater_than_excludes_boundary() {
    // Regression: strict `>` is pushed as an inclusive `>=` range, so
    // the scan over-returns the boundary value. The provider reports
    // the filter Inexact, so DataFusion must re-apply `> N` and drop
    // the boundary row. vendor_id values: A={1,2,1,2}, B={3,3}.
    let tmp = tempfile::tempdir().expect("tmpdir");
    write_two_shard_dataset(tmp.path());
    let (provider, _cache_dir) = provider_for(tmp.path()).await;

    let ctx = SessionContext::new();
    ctx.register_table("trips", Arc::new(provider)).unwrap();

    // vendor_id > 2 → only the two docs with vendor_id=3 (shard B),
    // NOT the vendor_id=2 docs.
    let df = ctx
        .sql("SELECT count(*) FROM trips WHERE vendor_id > 2")
        .await
        .unwrap();
    let n = df.collect().await.unwrap()[0]
        .column(0)
        .as_any()
        .downcast_ref::<arrow::array::Int64Array>()
        .unwrap()
        .value(0);
    assert_eq!(n, 2, "vendor_id>2 excludes the boundary value 2");
}

#[tokio::test(flavor = "multi_thread")]
async fn where_strict_less_than_excludes_boundary() {
    let tmp = tempfile::tempdir().expect("tmpdir");
    write_two_shard_dataset(tmp.path());
    let (provider, _cache_dir) = provider_for(tmp.path()).await;

    let ctx = SessionContext::new();
    ctx.register_table("trips", Arc::new(provider)).unwrap();

    // vendor_id < 2 → only the two vendor_id=1 docs (shard A),
    // NOT the vendor_id=2 docs.
    let df = ctx
        .sql("SELECT count(*) FROM trips WHERE vendor_id < 2")
        .await
        .unwrap();
    let n = df.collect().await.unwrap()[0]
        .column(0)
        .as_any()
        .downcast_ref::<arrow::array::Int64Array>()
        .unwrap()
        .value(0);
    assert_eq!(n, 2, "vendor_id<2 excludes the boundary value 2");
}

#[tokio::test(flavor = "multi_thread")]
async fn where_gte_includes_boundary() {
    // `>=` is exact-pushed and must include the boundary.
    let tmp = tempfile::tempdir().expect("tmpdir");
    write_two_shard_dataset(tmp.path());
    let (provider, _cache_dir) = provider_for(tmp.path()).await;

    let ctx = SessionContext::new();
    ctx.register_table("trips", Arc::new(provider)).unwrap();

    // vendor_id >= 2 → A's two vendor_id=2 docs + B's two vendor_id=3
    // docs = 4.
    let df = ctx
        .sql("SELECT count(*) FROM trips WHERE vendor_id >= 2")
        .await
        .unwrap();
    let n = df.collect().await.unwrap()[0]
        .column(0)
        .as_any()
        .downcast_ref::<arrow::array::Int64Array>()
        .unwrap()
        .value(0);
    assert_eq!(n, 4, "vendor_id>=2 includes the boundary value 2");
}

#[tokio::test(flavor = "multi_thread")]
async fn where_or_not_pushed_but_correct() {
    // `OR` is a single BinaryExpr DataFusion doesn't hand us as a
    // pushable filter; it evaluates the predicate itself. Result must
    // still be correct.
    let tmp = tempfile::tempdir().expect("tmpdir");
    write_two_shard_dataset(tmp.path());
    let (provider, _cache_dir) = provider_for(tmp.path()).await;

    let ctx = SessionContext::new();
    ctx.register_table("trips", Arc::new(provider)).unwrap();

    // vendor_id=1 (A: 2 docs) OR vendor_id=3 (B: 2 docs) = 4.
    let df = ctx
        .sql("SELECT count(*) FROM trips WHERE vendor_id = 1 OR vendor_id = 3")
        .await
        .unwrap();
    let n = df.collect().await.unwrap()[0]
        .column(0)
        .as_any()
        .downcast_ref::<arrow::array::Int64Array>()
        .unwrap()
        .value(0);
    assert_eq!(n, 4);
}

#[tokio::test(flavor = "current_thread")]
async fn scan_runs_under_current_thread_runtime() {
    // Regression: `execute` must not panic when driven from a
    // current-thread runtime (the flavor the CLI builds). The scan
    // runs its async work on a dedicated thread, so this works under
    // any ambient runtime flavor.
    let tmp = tempfile::tempdir().expect("tmpdir");
    write_two_shard_dataset(tmp.path());
    let (provider, _cache_dir) = provider_for(tmp.path()).await;

    let ctx = SessionContext::new();
    ctx.register_table("trips", Arc::new(provider)).unwrap();

    let df = ctx.sql("SELECT sum(fare) FROM trips").await.unwrap();
    let total = df.collect().await.unwrap()[0]
        .column(0)
        .as_any()
        .downcast_ref::<arrow::array::Int64Array>()
        .unwrap()
        .value(0);
    assert_eq!(total, 2100);
}

/// Single shard with a `ts` time field (epoch seconds) + a `fare`
/// metric. 5 docs at 2023-01-01 00:00, 01:00, 02:00, 03:00, 04:00.
fn write_time_dataset(root: &Path) {
    // 2023-01-01T00:00:00Z = 1672531200.
    let base = 1_672_531_200_i64;
    let times: Vec<i64> = (0..5).map(|h| base + h * 3600).collect();
    let shard = root.join("day");
    let mut w = DiskShardWriter::new(&shard, (times[0], times[4])).expect("new");
    w.add_int_field("ts", times).expect("ts");
    w.add_metric("fare", vec![10, 20, 30, 40, 50])
        .expect("fare");
    w.set_time_field("ts");
    w.finalize().expect("finalize");
}

#[tokio::test(flavor = "multi_thread")]
async fn time_field_presented_as_timestamp() {
    let tmp = tempfile::tempdir().expect("tmpdir");
    write_time_dataset(tmp.path());
    let (provider, _cache_dir) = provider_for(tmp.path()).await;

    // The time field's Arrow type is Timestamp(Nanosecond), not Int64.
    let ts_field = provider
        .schema()
        .field_with_name("ts")
        .expect("ts in schema")
        .clone();
    assert!(
        matches!(
            ts_field.data_type(),
            arrow::datatypes::DataType::Timestamp(arrow::datatypes::TimeUnit::Nanosecond, _)
        ),
        "ts should be Timestamp(Nanosecond), got {:?}",
        ts_field.data_type()
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn time_range_filter_with_timestamp_literals() {
    let tmp = tempfile::tempdir().expect("tmpdir");
    write_time_dataset(tmp.path());
    let (provider, _cache_dir) = provider_for(tmp.path()).await;

    let ctx = SessionContext::new();
    ctx.register_table("t", Arc::new(provider)).unwrap();

    // [01:00, 03:00) → docs at 01:00 and 02:00 → fare 20 + 30 = 50.
    let df = ctx
        .sql(
            "SELECT sum(fare) FROM t \
             WHERE ts >= TIMESTAMP '2023-01-01T01:00:00' \
               AND ts <  TIMESTAMP '2023-01-01T03:00:00'",
        )
        .await
        .unwrap();
    let total = df.collect().await.unwrap()[0]
        .column(0)
        .as_any()
        .downcast_ref::<arrow::array::Int64Array>()
        .unwrap()
        .value(0);
    assert_eq!(total, 50);
}

#[tokio::test(flavor = "multi_thread")]
async fn select_time_field_returns_timestamps() {
    let tmp = tempfile::tempdir().expect("tmpdir");
    write_time_dataset(tmp.path());
    let (provider, _cache_dir) = provider_for(tmp.path()).await;

    let ctx = SessionContext::new();
    ctx.register_table("t", Arc::new(provider)).unwrap();

    let df = ctx
        .sql("SELECT ts FROM t ORDER BY ts LIMIT 1")
        .await
        .unwrap();
    let batches = df.collect().await.unwrap();
    let col = batches[0].column(0);
    assert!(
        matches!(
            col.data_type(),
            arrow::datatypes::DataType::Timestamp(arrow::datatypes::TimeUnit::Nanosecond, _)
        ),
        "projected ts column should be Timestamp(Nanosecond)"
    );
    let ts = col
        .as_any()
        .downcast_ref::<arrow::array::TimestampNanosecondArray>()
        .unwrap();
    // 2023-01-01T00:00:00Z = 1672531200 s = 1672531200e9 ns.
    assert_eq!(ts.value(0), 1_672_531_200_000_000_000);
}

/// Two single-doc shards in different days, each with a `ts` time
/// field. Used to exercise cross-shard time pruning: a filter that
/// selects one day must not surface the other day's row.
fn write_two_day_dataset(root: &Path) {
    // 2023-01-01T00:00:00Z and 2023-01-02T00:00:00Z.
    let day1 = 1_672_531_200_i64;
    let day2 = day1 + 86_400;
    for (name, t, fare) in [("d1", day1, 11_i64), ("d2", day2, 22_i64)] {
        let shard = root.join(name);
        let mut w = DiskShardWriter::new(&shard, (t, t)).expect("new");
        w.add_int_field("ts", vec![t]).expect("ts");
        w.add_metric("fare", vec![fare]).expect("fare");
        w.set_time_field("ts");
        w.finalize().expect("finalize");
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn time_filter_selects_one_shard_across_a_two_day_dataset() {
    let tmp = tempfile::tempdir().expect("tmpdir");
    write_two_day_dataset(tmp.path());
    let (provider, _cache_dir) = provider_for(tmp.path()).await;

    let ctx = SessionContext::new();
    ctx.register_table("t", Arc::new(provider)).unwrap();

    // Only Jan 1 is in window — the Jan 2 shard is pruned before
    // fetch, and its fare (22) must not appear in the sum.
    let df = ctx
        .sql(
            "SELECT sum(fare) FROM t \
             WHERE ts >= TIMESTAMP '2023-01-01T00:00:00' \
               AND ts <  TIMESTAMP '2023-01-02T00:00:00'",
        )
        .await
        .unwrap();
    let total = df.collect().await.unwrap()[0]
        .column(0)
        .as_any()
        .downcast_ref::<arrow::array::Int64Array>()
        .unwrap()
        .value(0);
    assert_eq!(total, 11);
}

// ---- single-column GROUP BY pushdown into FTGS ----

/// Physical plan of `sql` under `ctx`, rendered for substring checks
/// (e.g. asserting `FtgsAggExec` is or isn't present).
async fn physical_plan(ctx: &SessionContext, sql: &str) -> String {
    let plan = ctx
        .sql(sql)
        .await
        .unwrap()
        .create_physical_plan()
        .await
        .unwrap();
    format!(
        "{}",
        datafusion::physical_plan::displayable(plan.as_ref()).indent(true)
    )
}

/// Numeric result rows in batch order, reading `Int64` or `UInt64`
/// columns as `i64`.
fn int_rows(batches: &[arrow::array::RecordBatch]) -> Vec<Vec<i64>> {
    batches
        .iter()
        .flat_map(|b| {
            let cols: Vec<Vec<i64>> = (0..b.num_columns())
                .map(|i| {
                    let c = b.column(i);
                    if let Some(a) = c.as_any().downcast_ref::<arrow::array::Int64Array>() {
                        a.values().to_vec()
                    } else if let Some(a) = c.as_any().downcast_ref::<arrow::array::UInt64Array>() {
                        a.values()
                            .iter()
                            .map(|&v| i64::try_from(v).unwrap())
                            .collect()
                    } else {
                        panic!("expected an Int64/UInt64 column");
                    }
                })
                .collect();
            (0..b.num_rows()).map(move |r| cols.iter().map(|c| c[r]).collect())
        })
        .collect()
}

#[tokio::test(flavor = "multi_thread")]
async fn group_by_int_pushes_down_and_aggregates() {
    let tmp = tempfile::tempdir().expect("tmpdir");
    write_two_shard_dataset(tmp.path());
    let (provider, _cache_dir) = provider_for(tmp.path()).await;

    let ctx = super::session_context();
    ctx.register_table("trips", Arc::new(provider)).unwrap();

    let sql = "SELECT vendor_id, sum(fare), count(*), min(fare), max(fare) \
               FROM trips GROUP BY vendor_id ORDER BY vendor_id";

    // The pushdown fired: FTGS exec replaced the DataFusion aggregate.
    assert!(
        physical_plan(&ctx, sql).await.contains("FtgsAggExec"),
        "expected GROUP BY to push down to FtgsAggExec"
    );

    // vendor 1 = docs A{0,2} fare {100,300}; vendor 2 = A{1,3} {200,400};
    // vendor 3 = B{0,1} {500,600}. Columns: vendor, sum, count, min, max.
    let rows = int_rows(&ctx.sql(sql).await.unwrap().collect().await.unwrap());
    assert_eq!(
        rows,
        vec![
            vec![1, 400, 2, 100, 300],
            vec![2, 600, 2, 200, 400],
            vec![3, 1100, 2, 500, 600],
        ]
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn group_by_int_with_exact_where_filter() {
    let tmp = tempfile::tempdir().expect("tmpdir");
    write_two_shard_dataset(tmp.path());
    let (provider, _cache_dir) = provider_for(tmp.path()).await;

    let ctx = super::session_context();
    ctx.register_table("trips", Arc::new(provider)).unwrap();

    // `country = 'us'` is an exact filter → bare TableScan → still pushed.
    let sql = "SELECT vendor_id, sum(fare) FROM trips \
               WHERE country = 'us' GROUP BY vendor_id ORDER BY vendor_id";
    assert!(physical_plan(&ctx, sql).await.contains("FtgsAggExec"));

    // us docs: A{0,2} (vendor 1, fare 100+300=400), B{0} (vendor 3, 500).
    let rows = int_rows(&ctx.sql(sql).await.unwrap().collect().await.unwrap());
    assert_eq!(rows, vec![vec![1, 400], vec![3, 500]]);
}

/// Result rows of a `String`-grouped query as `(group, stats)`, with a
/// NULL group key as `None`.
fn string_keyed_rows(batches: &[arrow::array::RecordBatch]) -> Vec<(Option<String>, Vec<i64>)> {
    batches
        .iter()
        .flat_map(|b| {
            let keys = b
                .column(0)
                .as_any()
                .downcast_ref::<arrow::array::StringArray>()
                .unwrap();
            let cols: Vec<&arrow::array::Int64Array> = (1..b.num_columns())
                .map(|i| {
                    b.column(i)
                        .as_any()
                        .downcast_ref::<arrow::array::Int64Array>()
                        .unwrap()
                })
                .collect();
            (0..b.num_rows()).map(move |r| {
                let key = (!keys.is_null(r)).then(|| keys.value(r).to_string());
                (key, cols.iter().map(|c| c.value(r)).collect())
            })
        })
        .collect()
}

#[tokio::test(flavor = "multi_thread")]
async fn group_by_string_pushes_down_no_null_group_when_dense() {
    let tmp = tempfile::tempdir().expect("tmpdir");
    write_two_shard_dataset(tmp.path());
    let (provider, _cache_dir) = provider_for(tmp.path()).await;

    let ctx = super::session_context();
    ctx.register_table("trips", Arc::new(provider)).unwrap();

    // Every doc carries a country term, so the string GROUP BY pushes
    // down and emits no NULL row.
    let sql = "SELECT country, count(*) AS n FROM trips GROUP BY country ORDER BY country";
    assert!(physical_plan(&ctx, sql).await.contains("FtgsAggExec"));

    let rows = string_keyed_rows(&ctx.sql(sql).await.unwrap().collect().await.unwrap());
    // de: A{1,3}, B{1} = 3; us: A{0,2}, B{0} = 3. No NULL group.
    assert_eq!(
        rows,
        vec![
            (Some("de".to_string()), vec![3]),
            (Some("us".to_string()), vec![3]),
        ]
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn group_by_string_emits_null_group_for_no_term_docs() {
    // 3 docs, fare [10, 20, 30]; only doc 0 has a country term ("us").
    // docs 1 and 2 carry no country → SQL NULL group.
    let tmp = tempfile::tempdir().expect("tmpdir");
    let shard = tmp.path().join("s");
    let mut w = DiskShardWriter::new(&shard, (0, 0)).expect("new");
    w.add_metric("fare", vec![10, 20, 30]).expect("fare");
    let mut country = BTreeMap::new();
    country.insert("us".to_string(), bitmap([0]));
    w.add_string_field("country", country).expect("country");
    w.finalize().expect("finalize");

    let (provider, _cache_dir) = provider_for(tmp.path()).await;
    let ctx = super::session_context();
    ctx.register_table("trips", Arc::new(provider)).unwrap();

    let sql = "SELECT country, sum(fare), count(*) FROM trips GROUP BY country";
    assert!(physical_plan(&ctx, sql).await.contains("FtgsAggExec"));

    let rows = string_keyed_rows(&ctx.sql(sql).await.unwrap().collect().await.unwrap());
    // us: doc 0 → sum 10, count 1. NULL: docs {1,2} → sum 50, count 2.
    assert_eq!(rows.len(), 2);
    assert!(rows.contains(&(Some("us".to_string()), vec![10, 1])));
    assert!(rows.contains(&(None, vec![50, 2])));
}

#[tokio::test(flavor = "multi_thread")]
async fn group_by_string_with_where_filter() {
    let tmp = tempfile::tempdir().expect("tmpdir");
    write_two_shard_dataset(tmp.path());
    let (provider, _cache_dir) = provider_for(tmp.path()).await;

    let ctx = super::session_context();
    ctx.register_table("trips", Arc::new(provider)).unwrap();

    // vendor_id = 1 → docs A{0,2}, both country "us", fare 100 + 300.
    let sql = "SELECT country, sum(fare) FROM trips WHERE vendor_id = 1 GROUP BY country";
    assert!(physical_plan(&ctx, sql).await.contains("FtgsAggExec"));

    let rows = string_keyed_rows(&ctx.sql(sql).await.unwrap().collect().await.unwrap());
    assert_eq!(rows, vec![(Some("us".to_string()), vec![400])]);
}

#[tokio::test(flavor = "multi_thread")]
async fn avg_grouped_by_int_pushes_down() {
    let tmp = tempfile::tempdir().expect("tmpdir");
    write_two_shard_dataset(tmp.path());
    let (provider, _cache_dir) = provider_for(tmp.path()).await;

    let ctx = super::session_context();
    ctx.register_table("trips", Arc::new(provider)).unwrap();

    let sql = "SELECT vendor_id, avg(fare) FROM trips GROUP BY vendor_id ORDER BY vendor_id";
    assert!(physical_plan(&ctx, sql).await.contains("FtgsAggExec"));

    // vendor 1 {100,300}→200; vendor 2 {200,400}→300; vendor 3 {500,600}→550.
    let batches = ctx.sql(sql).await.unwrap().collect().await.unwrap();
    assert_eq!(
        f64_col(&batches[0], 1),
        vec![Some(200.0), Some(300.0), Some(550.0)]
    );
}

// ---- approx_count_distinct (HLL) pushdown ----

#[tokio::test(flavor = "multi_thread")]
async fn approx_distinct_pushes_down() {
    let tmp = tempfile::tempdir().expect("tmpdir");
    write_two_shard_dataset(tmp.path());
    let (provider, _cache_dir) = provider_for(tmp.path()).await;

    let ctx = super::session_context();
    ctx.register_table("trips", Arc::new(provider)).unwrap();

    let sql = "SELECT vendor_id, approx_distinct(fare) FROM trips \
               GROUP BY vendor_id ORDER BY vendor_id";
    assert!(physical_plan(&ctx, sql).await.contains("FtgsAggExec"));
    assert!(physical_plan(&ctx, sql).await.contains("FtgsAggExec"));

    // Each vendor's fares are distinct (small → HLL exact): vendor 1
    // {100,300}=2, vendor 2 {200,400}=2, vendor 3 {500,600}=2.
    let rows = int_rows(&ctx.sql(sql).await.unwrap().collect().await.unwrap());
    assert_eq!(rows, vec![vec![1, 2], vec![2, 2], vec![3, 2]]);
}

#[tokio::test(flavor = "multi_thread")]
async fn approx_distinct_mixed_with_scalar_keeps_column_order() {
    let tmp = tempfile::tempdir().expect("tmpdir");
    write_two_shard_dataset(tmp.path());
    let (provider, _cache_dir) = provider_for(tmp.path()).await;

    let ctx = super::session_context();
    ctx.register_table("trips", Arc::new(provider)).unwrap();

    let sql = "SELECT vendor_id, sum(fare), approx_distinct(fare) FROM trips \
               GROUP BY vendor_id ORDER BY vendor_id";
    assert!(physical_plan(&ctx, sql).await.contains("FtgsAggExec"));

    // Columns: vendor, sum, approx-distinct. vendor 1: 100+300=400, 2 distinct.
    let rows = int_rows(&ctx.sql(sql).await.unwrap().collect().await.unwrap());
    assert_eq!(
        rows,
        vec![vec![1, 400, 2], vec![2, 600, 2], vec![3, 1100, 2]]
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn approx_distinct_on_string_column_pushes_down() {
    let tmp = tempfile::tempdir().expect("tmpdir");
    write_two_shard_dataset(tmp.path());
    let (provider, _cache_dir) = provider_for(tmp.path()).await;

    let ctx = super::session_context();
    ctx.register_table("trips", Arc::new(provider)).unwrap();

    // A String column has no forward column; approx_distinct sources its
    // values from the inverted index terms, so it now pushes down.
    let sql = "SELECT vendor_id, approx_distinct(country) FROM trips \
               GROUP BY vendor_id ORDER BY vendor_id";
    assert!(physical_plan(&ctx, sql).await.contains("FtgsAggExec"));

    // vendor 1 docs A{0,2}=us,us → 1; vendor 2 docs A{1,3}=de,de → 1;
    // vendor 3 docs B{0,1}=us,de → 2.
    let rows = int_rows(&ctx.sql(sql).await.unwrap().collect().await.unwrap());
    assert_eq!(rows, vec![vec![1, 1], vec![2, 1], vec![3, 2]]);
}

#[tokio::test(flavor = "multi_thread")]
async fn global_approx_distinct_string_unions_across_shards() {
    let tmp = tempfile::tempdir().expect("tmpdir");
    write_two_shard_dataset(tmp.path());
    let (provider, _cache_dir) = provider_for(tmp.path()).await;

    let ctx = super::session_context();
    ctx.register_table("trips", Arc::new(provider)).unwrap();

    // "us" and "de" each appear in BOTH shards. Hashing term bytes means
    // the sketches union to the true 2 distinct — not 4, as a per-shard
    // sum of exact counts would give.
    let sql = "SELECT approx_distinct(country) FROM trips";
    assert!(physical_plan(&ctx, sql).await.contains("FtgsAggExec"));

    let rows = int_rows(&ctx.sql(sql).await.unwrap().collect().await.unwrap());
    assert_eq!(rows, vec![vec![2]]);
}

// ---- global (no GROUP BY) aggregate pushdown ----

#[tokio::test(flavor = "multi_thread")]
async fn global_aggregate_pushes_down() {
    let tmp = tempfile::tempdir().expect("tmpdir");
    write_two_shard_dataset(tmp.path());
    let (provider, _cache_dir) = provider_for(tmp.path()).await;

    let ctx = super::session_context();
    ctx.register_table("trips", Arc::new(provider)).unwrap();

    // No GROUP BY → one row over the whole dataset.
    let sql = "SELECT count(*), sum(fare), min(fare), max(fare), approx_distinct(fare) FROM trips";
    assert!(physical_plan(&ctx, sql).await.contains("FtgsAggExec"));

    // fares {100,200,300,400,500,600}: count 6, sum 2100, min 100,
    // max 600, 6 distinct.
    let rows = int_rows(&ctx.sql(sql).await.unwrap().collect().await.unwrap());
    assert_eq!(rows, vec![vec![6, 2100, 100, 600, 6]]);
}

#[tokio::test(flavor = "multi_thread")]
async fn global_aggregate_with_where_filter() {
    let tmp = tempfile::tempdir().expect("tmpdir");
    write_two_shard_dataset(tmp.path());
    let (provider, _cache_dir) = provider_for(tmp.path()).await;

    let ctx = super::session_context();
    ctx.register_table("trips", Arc::new(provider)).unwrap();

    // vendor_id = 1 → docs A{0,2}, fare {100,300}.
    let sql = "SELECT count(*), sum(fare) FROM trips WHERE vendor_id = 1";
    assert!(physical_plan(&ctx, sql).await.contains("FtgsAggExec"));

    let rows = int_rows(&ctx.sql(sql).await.unwrap().collect().await.unwrap());
    assert_eq!(rows, vec![vec![2, 400]]);
}

#[tokio::test(flavor = "multi_thread")]
async fn global_aggregate_empty_input_has_sql_null_semantics() {
    let tmp = tempfile::tempdir().expect("tmpdir");
    write_two_shard_dataset(tmp.path());
    let (provider, _cache_dir) = provider_for(tmp.path()).await;

    let ctx = super::session_context();
    ctx.register_table("trips", Arc::new(provider)).unwrap();

    // No matching docs → one row: count/approx 0, sum NULL.
    let sql = "SELECT count(*) AS n, sum(fare) AS s, approx_distinct(fare) AS d \
               FROM trips WHERE country = 'zz'";
    assert!(physical_plan(&ctx, sql).await.contains("FtgsAggExec"));

    let batches = ctx.sql(sql).await.unwrap().collect().await.unwrap();
    let b = &batches[0];
    assert_eq!(b.num_rows(), 1);
    let count = b
        .column(0)
        .as_any()
        .downcast_ref::<arrow::array::Int64Array>()
        .unwrap();
    let sum = b
        .column(1)
        .as_any()
        .downcast_ref::<arrow::array::Int64Array>()
        .unwrap();
    let distinct = b
        .column(2)
        .as_any()
        .downcast_ref::<arrow::array::UInt64Array>()
        .unwrap();
    assert_eq!(count.value(0), 0);
    assert!(sum.is_null(0), "sum over empty input is NULL");
    assert_eq!(distinct.value(0), 0);
}

// ---- AVG ----

/// The `Float64` values of `batch` column `col`, `None` for NULL.
fn f64_col(batch: &arrow::array::RecordBatch, col: usize) -> Vec<Option<f64>> {
    let a = batch
        .column(col)
        .as_any()
        .downcast_ref::<arrow::array::Float64Array>()
        .expect("expected a Float64 column");
    (0..a.len())
        .map(|i| (!a.is_null(i)).then(|| a.value(i)))
        .collect()
}

#[tokio::test(flavor = "multi_thread")]
async fn avg_global_pushes_down() {
    let tmp = tempfile::tempdir().expect("tmpdir");
    write_two_shard_dataset(tmp.path());
    let (provider, _cache_dir) = provider_for(tmp.path()).await;

    let ctx = super::session_context();
    ctx.register_table("trips", Arc::new(provider)).unwrap();

    // fares {100,200,300,400,500,600} → mean 2100/6 = 350.0.
    let sql = "SELECT avg(fare) FROM trips";
    assert!(physical_plan(&ctx, sql).await.contains("FtgsAggExec"));

    let batches = ctx.sql(sql).await.unwrap().collect().await.unwrap();
    assert_eq!(f64_col(&batches[0], 0), vec![Some(350.0)]);
}

#[tokio::test(flavor = "multi_thread")]
async fn avg_grouped_pushes_down() {
    let tmp = tempfile::tempdir().expect("tmpdir");
    write_two_shard_dataset(tmp.path());
    let (provider, _cache_dir) = provider_for(tmp.path()).await;

    let ctx = super::session_context();
    ctx.register_table("trips", Arc::new(provider)).unwrap();

    // us = {100,300,500}/3 = 300.0; de = {200,400,600}/3 = 400.0.
    let sql = "SELECT country, avg(fare) FROM trips GROUP BY country ORDER BY country";
    assert!(physical_plan(&ctx, sql).await.contains("FtgsAggExec"));

    let batches = ctx.sql(sql).await.unwrap().collect().await.unwrap();
    assert_eq!(f64_col(&batches[0], 1), vec![Some(400.0), Some(300.0)]);
}

#[tokio::test(flavor = "multi_thread")]
async fn avg_empty_input_is_null() {
    let tmp = tempfile::tempdir().expect("tmpdir");
    write_two_shard_dataset(tmp.path());
    let (provider, _cache_dir) = provider_for(tmp.path()).await;

    let ctx = super::session_context();
    ctx.register_table("trips", Arc::new(provider)).unwrap();

    // No matching docs → one row, AVG NULL (not 0).
    let sql = "SELECT avg(fare) FROM trips WHERE country = 'zz'";
    assert!(physical_plan(&ctx, sql).await.contains("FtgsAggExec"));

    let batches = ctx.sql(sql).await.unwrap().collect().await.unwrap();
    assert_eq!(f64_col(&batches[0], 0), vec![None]);
}

// ---- multi-column GROUP BY ----

/// `Int64`/`UInt64` column `col` as `i64` (ignores nulls; group/stat
/// columns in these tests are non-null).
fn col_i64(b: &arrow::array::RecordBatch, col: usize) -> Vec<i64> {
    let c = b.column(col);
    if let Some(a) = c.as_any().downcast_ref::<arrow::array::Int64Array>() {
        a.values().to_vec()
    } else if let Some(a) = c.as_any().downcast_ref::<arrow::array::UInt64Array>() {
        a.values()
            .iter()
            .map(|&v| i64::try_from(v).unwrap())
            .collect()
    } else {
        panic!("expected an Int64/UInt64 column at {col}");
    }
}

/// `Utf8` column `col`, `None` for NULL.
fn col_str(b: &arrow::array::RecordBatch, col: usize) -> Vec<Option<String>> {
    let a = b
        .column(col)
        .as_any()
        .downcast_ref::<arrow::array::StringArray>()
        .expect("expected a Utf8 column");
    (0..a.len())
        .map(|i| (!a.is_null(i)).then(|| a.value(i).to_string()))
        .collect()
}

/// Single shard with a *sparse* `String` field: `tag` covers only doc 0,
/// so docs 1 and 2 have no term (the SQL NULL group). `vendor_id`
/// [1, 1, 2], `fare` [10, 20, 30].
fn write_sparse_tag_shard(root: &Path) {
    let mut w = DiskShardWriter::new(&root.join("s"), (0, 0)).expect("new");
    w.add_int_field("vendor_id", vec![1, 1, 2]).expect("int");
    w.add_metric("fare", vec![10, 20, 30]).expect("metric");
    let mut tag = BTreeMap::new();
    tag.insert("x".to_string(), bitmap([0]));
    w.add_string_field("tag", tag).expect("string");
    w.finalize().expect("finalize");
}

#[tokio::test(flavor = "multi_thread")]
async fn multi_column_group_by_pushes_down() {
    let tmp = tempfile::tempdir().expect("tmpdir");
    write_two_shard_dataset(tmp.path());
    let (provider, _cache_dir) = provider_for(tmp.path()).await;

    let ctx = super::session_context();
    ctx.register_table("trips", Arc::new(provider)).unwrap();

    // Int prefix (vendor_id) × String cursor (country). Cells:
    // (1,us)=100+300, (2,de)=200+400, (3,us)=500, (3,de)=600.
    let sql = "SELECT vendor_id, country, sum(fare) FROM trips \
               GROUP BY vendor_id, country ORDER BY vendor_id, country";
    assert!(physical_plan(&ctx, sql).await.contains("FtgsAggExec"));

    let b = &ctx.sql(sql).await.unwrap().collect().await.unwrap()[0];
    let (v, c, s) = (col_i64(b, 0), col_str(b, 1), col_i64(b, 2));
    let rows: Vec<(i64, &str, i64)> = (0..b.num_rows())
        .map(|i| (v[i], c[i].as_deref().unwrap(), s[i]))
        .collect();
    assert_eq!(
        rows,
        vec![
            (1, "us", 400),
            (2, "de", 600),
            (3, "de", 600),
            (3, "us", 500)
        ]
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn multi_column_group_by_order_independent() {
    let tmp = tempfile::tempdir().expect("tmpdir");
    write_two_shard_dataset(tmp.path());
    let (provider, _cache_dir) = provider_for(tmp.path()).await;

    let ctx = super::session_context();
    ctx.register_table("trips", Arc::new(provider)).unwrap();

    // String prefix (country) × Int cursor (vendor_id) — same cells, cols
    // swapped.
    let sql = "SELECT country, vendor_id, sum(fare) FROM trips \
               GROUP BY country, vendor_id ORDER BY country, vendor_id";
    assert!(physical_plan(&ctx, sql).await.contains("FtgsAggExec"));

    let b = &ctx.sql(sql).await.unwrap().collect().await.unwrap()[0];
    let (c, v, s) = (col_str(b, 0), col_i64(b, 1), col_i64(b, 2));
    let rows: Vec<(&str, i64, i64)> = (0..b.num_rows())
        .map(|i| (c[i].as_deref().unwrap(), v[i], s[i]))
        .collect();
    assert_eq!(
        rows,
        vec![
            ("de", 2, 600),
            ("de", 3, 600),
            ("us", 1, 400),
            ("us", 3, 500)
        ]
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn multi_column_group_by_with_avg() {
    let tmp = tempfile::tempdir().expect("tmpdir");
    write_two_shard_dataset(tmp.path());
    let (provider, _cache_dir) = provider_for(tmp.path()).await;

    let ctx = super::session_context();
    ctx.register_table("trips", Arc::new(provider)).unwrap();

    // (1,us)=400/2, (2,de)=600/2, (3,de)=600/1, (3,us)=500/1.
    let sql = "SELECT vendor_id, country, avg(fare) FROM trips \
               GROUP BY vendor_id, country ORDER BY vendor_id, country";
    assert!(physical_plan(&ctx, sql).await.contains("FtgsAggExec"));

    let b = &ctx.sql(sql).await.unwrap().collect().await.unwrap()[0];
    assert_eq!(
        f64_col(b, 2),
        vec![Some(200.0), Some(300.0), Some(600.0), Some(500.0)]
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn multi_column_null_cursor_group() {
    let tmp = tempfile::tempdir().expect("tmpdir");
    write_sparse_tag_shard(tmp.path());
    let (provider, _cache_dir) = provider_for(tmp.path()).await;

    let ctx = super::session_context();
    ctx.register_table("trips", Arc::new(provider)).unwrap();

    // Int prefix, String cursor with NULLs: docs 1,2 have no tag.
    // (1,x)=10, (1,NULL)=20, (2,NULL)=30.
    let sql = "SELECT vendor_id, tag, sum(fare) FROM trips GROUP BY vendor_id, tag";
    assert!(physical_plan(&ctx, sql).await.contains("FtgsAggExec"));

    let b = &ctx.sql(sql).await.unwrap().collect().await.unwrap()[0];
    let (v, t, s) = (col_i64(b, 0), col_str(b, 1), col_i64(b, 2));
    let mut rows: Vec<(i64, Option<String>, i64)> = (0..b.num_rows())
        .map(|i| (v[i], t[i].clone(), s[i]))
        .collect();
    rows.sort();
    assert_eq!(
        rows,
        vec![(1, None, 20), (1, Some("x".to_string()), 10), (2, None, 30),]
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn multi_column_null_prefix_group() {
    let tmp = tempfile::tempdir().expect("tmpdir");
    write_sparse_tag_shard(tmp.path());
    let (provider, _cache_dir) = provider_for(tmp.path()).await;

    let ctx = super::session_context();
    ctx.register_table("trips", Arc::new(provider)).unwrap();

    // String prefix with NULLs (tag) × Int cursor (vendor_id): the
    // missing-tag docs form their own NULL prefix group.
    // (x,1)=10, (NULL,1)=20, (NULL,2)=30.
    let sql = "SELECT tag, vendor_id, sum(fare) FROM trips GROUP BY tag, vendor_id";
    assert!(physical_plan(&ctx, sql).await.contains("FtgsAggExec"));

    let b = &ctx.sql(sql).await.unwrap().collect().await.unwrap()[0];
    let (t, v, s) = (col_str(b, 0), col_i64(b, 1), col_i64(b, 2));
    let mut rows: Vec<(Option<String>, i64, i64)> = (0..b.num_rows())
        .map(|i| (t[i].clone(), v[i], s[i]))
        .collect();
    rows.sort();
    assert_eq!(
        rows,
        vec![(None, 1, 20), (None, 2, 30), (Some("x".to_string()), 1, 10),]
    );
}

// ---- approx_percentile (t-digest) ----

#[tokio::test(flavor = "multi_thread")]
async fn approx_percentile_global_pushes_down() {
    let tmp = tempfile::tempdir().expect("tmpdir");
    write_two_shard_dataset(tmp.path());
    let (provider, _cache_dir) = provider_for(tmp.path()).await;

    let ctx = super::session_context();
    ctx.register_table("trips", Arc::new(provider)).unwrap();

    // fares {100..600}: median ~350. Output is Int64 (the input type).
    let sql = "SELECT approx_percentile_cont(fare, 0.5) AS median FROM trips";
    assert!(physical_plan(&ctx, sql).await.contains("FtgsAggExec"));

    let rows = int_rows(&ctx.sql(sql).await.unwrap().collect().await.unwrap());
    let m = rows[0][0];
    assert!((300..=400).contains(&m), "median = {m}");
}

#[tokio::test(flavor = "multi_thread")]
async fn approx_percentile_grouped_and_merges_across_shards() {
    let tmp = tempfile::tempdir().expect("tmpdir");
    write_two_shard_dataset(tmp.path());
    let (provider, _cache_dir) = provider_for(tmp.path()).await;

    let ctx = super::session_context();
    ctx.register_table("trips", Arc::new(provider)).unwrap();

    // de = {200,400,600} median ~400; us = {100,300,500} median ~300.
    // us values span both shards, so this also exercises the digest merge.
    let sql = "SELECT country, approx_percentile_cont(fare, 0.5) AS m FROM trips \
               GROUP BY country ORDER BY country";
    assert!(physical_plan(&ctx, sql).await.contains("FtgsAggExec"));

    let b = &ctx.sql(sql).await.unwrap().collect().await.unwrap()[0];
    let m = col_i64(b, 1);
    assert!((350..=450).contains(&m[0]), "de median = {}", m[0]);
    assert!((250..=350).contains(&m[1]), "us median = {}", m[1]);
}

#[tokio::test(flavor = "multi_thread")]
async fn approx_percentile_empty_input_is_null() {
    let tmp = tempfile::tempdir().expect("tmpdir");
    write_two_shard_dataset(tmp.path());
    let (provider, _cache_dir) = provider_for(tmp.path()).await;

    let ctx = super::session_context();
    ctx.register_table("trips", Arc::new(provider)).unwrap();

    let sql = "SELECT approx_percentile_cont(fare, 0.5) AS m FROM trips WHERE country = 'zz'";
    assert!(physical_plan(&ctx, sql).await.contains("FtgsAggExec"));

    let b = &ctx.sql(sql).await.unwrap().collect().await.unwrap()[0];
    let m = b
        .column(0)
        .as_any()
        .downcast_ref::<arrow::array::Int64Array>()
        .unwrap();
    assert_eq!(b.num_rows(), 1);
    assert!(m.is_null(0), "percentile over empty input is NULL");
}

// ---- approx_top_k ----

/// The `List<Struct<value: Utf8, count>>` cell at `(col, row)` as
/// (value, count) pairs.
fn topk_str(b: &arrow::array::RecordBatch, col: usize, row: usize) -> Vec<(String, i64)> {
    let list = b
        .column(col)
        .as_any()
        .downcast_ref::<arrow::array::ListArray>()
        .expect("List column");
    let item = list.value(row);
    let s = item
        .as_any()
        .downcast_ref::<arrow::array::StructArray>()
        .unwrap();
    let values = s
        .column(0)
        .as_any()
        .downcast_ref::<arrow::array::StringArray>()
        .unwrap();
    let counts = s
        .column(1)
        .as_any()
        .downcast_ref::<arrow::array::Int64Array>()
        .unwrap();
    (0..s.len())
        .map(|i| (values.value(i).to_string(), counts.value(i)))
        .collect()
}

/// Same, for a `List<Struct<value: Int64, count>>` cell.
fn topk_int(b: &arrow::array::RecordBatch, col: usize, row: usize) -> Vec<(i64, i64)> {
    let list = b
        .column(col)
        .as_any()
        .downcast_ref::<arrow::array::ListArray>()
        .expect("List column");
    let item = list.value(row);
    let s = item
        .as_any()
        .downcast_ref::<arrow::array::StructArray>()
        .unwrap();
    let values = s
        .column(0)
        .as_any()
        .downcast_ref::<arrow::array::Int64Array>()
        .unwrap();
    let counts = s
        .column(1)
        .as_any()
        .downcast_ref::<arrow::array::Int64Array>()
        .unwrap();
    (0..s.len())
        .map(|i| (values.value(i), counts.value(i)))
        .collect()
}

#[tokio::test(flavor = "multi_thread")]
async fn approx_top_k_string_global() {
    let tmp = tempfile::tempdir().expect("tmpdir");
    write_two_shard_dataset(tmp.path());
    let (provider, _cache_dir) = provider_for(tmp.path()).await;

    let ctx = super::session_context();
    ctx.register_table("trips", Arc::new(provider)).unwrap();

    // country: us 3 (A0,A2,B0), de 3 (A1,A3,B1). Tie → value asc.
    let sql = "SELECT approx_top_k(country, 2) AS top FROM trips";
    assert!(physical_plan(&ctx, sql).await.contains("FtgsAggExec"));
    let b = &ctx.sql(sql).await.unwrap().collect().await.unwrap()[0];
    assert_eq!(
        topk_str(b, 0, 0),
        vec![("de".to_string(), 3), ("us".to_string(), 3)]
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn approx_top_k_int_global() {
    let tmp = tempfile::tempdir().expect("tmpdir");
    write_two_shard_dataset(tmp.path());
    let (provider, _cache_dir) = provider_for(tmp.path()).await;

    let ctx = super::session_context();
    ctx.register_table("trips", Arc::new(provider)).unwrap();

    // vendor_id: 1→2, 2→2, 3→2. All tie → value asc, top 2 = (1,2),(2,2).
    let sql = "SELECT approx_top_k(vendor_id, 2) AS top FROM trips";
    assert!(physical_plan(&ctx, sql).await.contains("FtgsAggExec"));
    let b = &ctx.sql(sql).await.unwrap().collect().await.unwrap()[0];
    assert_eq!(topk_int(b, 0, 0), vec![(1, 2), (2, 2)]);
}

#[tokio::test(flavor = "multi_thread")]
async fn approx_top_k_grouped() {
    let tmp = tempfile::tempdir().expect("tmpdir");
    write_two_shard_dataset(tmp.path());
    let (provider, _cache_dir) = provider_for(tmp.path()).await;

    let ctx = super::session_context();
    ctx.register_table("trips", Arc::new(provider)).unwrap();

    // vendor 1: us,us → [(us,2)]; vendor 2: de,de → [(de,2)];
    // vendor 3: us,de → tie → [(de,1),(us,1)].
    let sql = "SELECT vendor_id, approx_top_k(country, 2) AS top FROM trips \
               GROUP BY vendor_id ORDER BY vendor_id";
    let b = &ctx.sql(sql).await.unwrap().collect().await.unwrap()[0];
    assert_eq!(topk_str(b, 1, 0), vec![("us".to_string(), 2)]);
    assert_eq!(topk_str(b, 1, 1), vec![("de".to_string(), 2)]);
    assert_eq!(
        topk_str(b, 1, 2),
        vec![("de".to_string(), 1), ("us".to_string(), 1)]
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn approx_top_k_capacity_below_k_is_clamped() {
    let tmp = tempfile::tempdir().expect("tmpdir");
    write_two_shard_dataset(tmp.path());
    let (provider, _cache_dir) = provider_for(tmp.path()).await;

    let ctx = super::session_context();
    ctx.register_table("trips", Arc::new(provider)).unwrap();

    // capacity 1 < k 2: clamped to k, so both values still come back.
    let sql = "SELECT approx_top_k(country, 2, 1) AS top FROM trips";
    assert!(physical_plan(&ctx, sql).await.contains("FtgsAggExec"));
    let b = &ctx.sql(sql).await.unwrap().collect().await.unwrap()[0];
    assert_eq!(
        topk_str(b, 0, 0),
        vec![("de".to_string(), 3), ("us".to_string(), 3)]
    );
}
