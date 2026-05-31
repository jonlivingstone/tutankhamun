//! In-process `DataFusion` integration tests: register
//! [`TutankhamunTableProvider`] against a real on-disk shard
//! tree and run SQL through `DataFusion`'s planner end-to-end.

use std::collections::BTreeMap;
use std::path::Path;
use std::sync::Arc;

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
    // Tier 1: String columns are exposed as Utf8 in the schema and
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
