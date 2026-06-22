//! Scan prelude shared by both Tutankhamun execution plans — the row scan
//! ([`super::exec`]) and the pushed-down aggregate ([`super::ftgs_agg`]).
//!
//! Holds the synchronous-`execute` → async bridge ([`block_on_scan`]), the
//! bounded-concurrency shard driver ([`for_each_shard_batch`]) that discovers,
//! time-prunes, and streams shards in batches, and the bounded record-batch
//! streaming ([`chunked_stream`]). Neither exec depends on the other — both
//! depend on this neutral module.

use std::future::Future;
use std::sync::Arc;

use arrow::array::RecordBatch;
use arrow::datatypes::SchemaRef;
use datafusion::common::Result as DfResult;
use datafusion::common::error::DataFusionError;
use datafusion::execution::{SendableRecordBatchStream, TaskContext};
use datafusion::physical_plan::memory::MemoryStream;

use super::pushdown::{PushedFilter, PushedOp};
use crate::bitmap_cache::BitmapCache;
use crate::cache::Cache;
use crate::memory::SessionMemoryHandle;
use crate::runtime;
use crate::shard::{DiskShard, FilterClause, FilterResult, matched_doc_set};
use crate::shard_source::ShardSummary;

/// Run a scan's async work to completion from `DataFusion`'s
/// synchronous `execute`, on a dedicated scoped thread with its own
/// current-thread runtime. `block_on` panics if the caller is already
/// inside a runtime, and `block_in_place` only works on the multi-thread
/// scheduler (it panics on the current-thread runtime the CLI builds);
/// a separate thread sidesteps both — correct under any ambient flavor
/// or none. Single-partition, so the one-thread-per-scan cost is
/// bounded. The future is built inside the thread (`make`), so only it
/// crosses the thread boundary, not the future.
pub(crate) fn block_on_scan<T, Fut>(make: impl FnOnce() -> Fut + Send) -> DfResult<T>
where
    Fut: Future<Output = anyhow::Result<T>>,
    T: Send,
{
    std::thread::scope(|scope| {
        scope
            .spawn(|| {
                let rt = tokio::runtime::Builder::new_current_thread()
                    .enable_all()
                    .build()?;
                rt.block_on(make())
            })
            .join()
            .map_err(|_| anyhow::anyhow!("scan thread panicked"))?
    })
    .map_err(|e| DataFusionError::External(e.into()))
}

/// Per-batch shard concurrency: how many shards are opened/resident at once.
/// The Rayon pool width (so each batch's `par_iter` saturates the pool without
/// over-opening) on the daemon, else 1 (sequential — tests / non-`serve` CLI,
/// where a wider batch only grows residency).
pub(crate) fn scan_batch_width() -> usize {
    if runtime::rayon_ready() {
        runtime::cpu_width().max(1)
    } else {
        1
    }
}

/// Does the query's time window (if any) intersect this shard's time range?
/// `true` when there's no time predicate or no time field — pruning only ever
/// skips shards that provably hold no matching doc.
fn intersects_query_window(summary: &ShardSummary, pushed: &[PushedFilter]) -> bool {
    summary
        .metadata
        .time_field
        .as_deref()
        .and_then(|time_field| time_window(pushed, time_field))
        .is_none_or(|(from, to)| summary.intersects_time_range(from, to))
}

/// Per-query scan substrate threaded through the shard driver and both scan
/// paths: where the dataset lives (`url`), its shard set resolved once by the
/// caller (`summaries`), the file cache, and the optional per-session memory
/// handle + §2.8 bitmap cache. Borrowed for the duration of one query — a
/// parameter object so the scan/aggregate functions forward one reference
/// instead of five repeated arguments.
pub(crate) struct ScanCtx<'a> {
    pub url: &'a str,
    pub summaries: &'a [ShardSummary],
    pub cache: &'a Cache,
    pub mem: Option<&'a Arc<SessionMemoryHandle>>,
    pub bitmap_cache: Option<&'a Arc<BitmapCache>>,
}

/// Drive a query over its shards in **bounded-concurrency batches**, calling
/// `work` with each batch's opened shards (and their resolved matched-doc sets)
/// and dropping the batch — releasing its forward-column residency — before the
/// next one. Peak resident shards = [`scan_batch_width`], independent of how
/// many shards the query spans. The shared prelude for both the row scan
/// ([`collect_batches`]) and the FTGS aggregate ([`aggregate_batch`]).
///
/// Memory (§2.2): when `mem` is set (the daemon data plane), a single bounded
/// working-set **envelope** is reserved up front — `width × largest selected
/// shard's num_docs × per_doc_bytes` — and held for the whole query. It is
/// span-independent (it bounds the *concurrent* working set, not the total data
/// scanned), so an admitted query is guaranteed room to finish; an over-cap
/// query is refused here. `per_doc_bytes` is the caller's estimate of the heap
/// working set a doc contributes (gathered columns / group + stat buffers).
/// `None` (the `t9n sql` CLI) charges nothing.
///
/// When `ctx.bitmap_cache` is set, the matched-doc set is served through the §2.8
/// cache (exact hit or monotone-narrowing reuse); `None` resolves directly via
/// [`matched_doc_set`].
pub(crate) async fn for_each_shard_batch<F>(
    ctx: &ScanCtx<'_>,
    pushed: &[PushedFilter],
    per_doc_bytes: u64,
    mut work: F,
) -> anyhow::Result<()>
where
    F: FnMut(&[(u64, Arc<DiskShard>, FilterResult)]) -> anyhow::Result<()>,
{
    // The dataset's shard set is resolved once per query by the caller (cached in
    // the daemon); here we only time-prune it (cheap — metadata only), so the
    // envelope and batching see only the shards the query can actually touch.
    let selected: Vec<&ShardSummary> = ctx
        .summaries
        .iter()
        .filter(|s| intersects_query_window(s, pushed))
        .collect();

    let width = scan_batch_width();

    // Reserve the bounded working-set envelope once, up front, and hold it for
    // the whole query: at most `width` shards resident at a time, each no larger
    // than the biggest selected shard. Span-independent — querying all history
    // costs the same as querying one batch — so admission gates on a bounded
    // number and an admitted query is guaranteed room.
    let _envelope = match ctx.mem {
        Some(handle) => {
            let max_docs = selected
                .iter()
                .map(|s| s.metadata.num_docs)
                .max()
                .unwrap_or(0);
            let bytes = (width as u64)
                .saturating_mul(max_docs)
                .saturating_mul(per_doc_bytes);
            Some(handle.reserve(bytes).map_err(|e| anyhow::anyhow!(e))?)
        }
        None => None,
    };

    for chunk in selected.chunks(width) {
        let mut shards: Vec<(u64, Arc<DiskShard>, FilterResult)> = Vec::with_capacity(chunk.len());
        for summary in chunk {
            let id = BitmapCache::shard_id(ctx.url, summary.location.as_ref());
            // Opened once and held resident by the cache — repeat queries reuse the
            // parse (footer + mmap) instead of re-opening every shard.
            let shard = ctx.cache.fetch_parsed_shard(summary).await?;
            let selection = if let Some(bc) = ctx.bitmap_cache {
                bc.resolve(&shard, id, pushed)?
            } else {
                let clauses: Vec<FilterClause<'_>> =
                    pushed.iter().map(PushedFilter::as_clause).collect();
                matched_doc_set(&shard, &clauses)?
            };
            shards.push((id, shard, selection));
        }
        work(&shards)?;
        // `shards` drops here, releasing this batch's *extra* shard refs before
        // the next batch opens. The cache holds its own `Arc<DiskShard>` per
        // shard, so the mmap stays resident across queries (bounded by the cache
        // size cap, reclaimable page cache) — but the §2.2 heap envelope is
        // unaffected: the per-batch heap working set is built and dropped here.
    }
    Ok(())
}

/// Split a batch into `<= batch_size`-row slices for streaming. Zero-copy —
/// Arrow `slice` shares the underlying buffers. A zero-row batch yields no
/// slices.
fn chunk_batch(batch: &RecordBatch, batch_size: usize) -> Vec<RecordBatch> {
    let n = batch.num_rows();
    let size = batch_size.max(1);
    (0..n)
        .step_by(size)
        .map(|off| batch.slice(off, size.min(n - off)))
        .collect()
}

/// Emit `batches` as a bounded record-batch stream: each is chunked to the
/// session's configured `batch_size` (see [`chunk_batch`]) so downstream
/// consumers get evenly capped batches. The single seam tying every exec's
/// output batching to the session config.
pub(crate) fn chunked_stream(
    batches: Vec<RecordBatch>,
    schema: SchemaRef,
    context: &TaskContext,
) -> DfResult<SendableRecordBatchStream> {
    let batch_size = context.session_config().batch_size();
    let chunked: Vec<RecordBatch> = batches
        .into_iter()
        .flat_map(|b| chunk_batch(&b, batch_size))
        .collect();
    Ok(Box::pin(MemoryStream::try_new(chunked, schema, None)?))
}

/// Closed epoch-second window `[from, to]` that the pushed filters on
/// `time_field` constrain the time column to, or `None` when no pushed
/// filter references it. Equality pins both ends; a range narrows
/// whichever end it bounds; multiple filters AND together (they
/// intersect, since `supports_filters_pushdown` accepts each conjunct
/// independently). Terms that don't parse as integers are ignored —
/// they can't be time bounds, and leaving that end unconstrained keeps
/// pruning conservative: it only ever skips shards that provably hold
/// no matching doc.
fn time_window(pushed: &[PushedFilter], time_field: &str) -> Option<(i64, i64)> {
    let mut from = i64::MIN;
    let mut to = i64::MAX;
    let mut constrained = false;
    for f in pushed.iter().filter(|f| f.field == time_field) {
        match &f.op {
            PushedOp::Equals(t) => {
                if let Ok(v) = t.parse::<i64>() {
                    from = from.max(v);
                    to = to.min(v);
                    constrained = true;
                }
            }
            PushedOp::Range { lo, hi } => {
                if let Some(v) = lo.as_deref().and_then(|s| s.parse::<i64>().ok()) {
                    from = from.max(v);
                    constrained = true;
                }
                if let Some(v) = hi.as_deref().and_then(|s| s.parse::<i64>().ok()) {
                    to = to.min(v);
                    constrained = true;
                }
            }
        }
    }
    constrained.then_some((from, to))
}

#[cfg(test)]
mod tests {
    use super::{PushedFilter, PushedOp, time_window};

    fn eq(field: &str, term: &str) -> PushedFilter {
        PushedFilter {
            field: field.to_string(),
            op: PushedOp::Equals(term.to_string()),
            exact: true,
        }
    }

    fn range(field: &str, lo: Option<&str>, hi: Option<&str>) -> PushedFilter {
        PushedFilter {
            field: field.to_string(),
            op: PushedOp::Range {
                lo: lo.map(str::to_string),
                hi: hi.map(str::to_string),
            },
            exact: true,
        }
    }

    #[test]
    fn time_window_none_without_a_filter_on_the_time_field() {
        let pushed = vec![eq("vendor_id", "3"), range("vendor_id", Some("1"), None)];
        assert_eq!(time_window(&pushed, "ts"), None);
    }

    #[test]
    fn time_window_from_a_two_sided_range() {
        let pushed = vec![range("ts", Some("100"), Some("200"))];
        assert_eq!(time_window(&pushed, "ts"), Some((100, 200)));
    }

    #[test]
    fn time_window_intersects_multiple_conjuncts() {
        // ts >= 100 AND ts <= 500 AND ts >= 150  →  [150, 500].
        let pushed = vec![
            range("ts", Some("100"), None),
            range("ts", None, Some("500")),
            range("ts", Some("150"), None),
        ];
        assert_eq!(time_window(&pushed, "ts"), Some((150, 500)));
    }

    #[test]
    fn time_window_equality_pins_both_ends() {
        let pushed = vec![eq("ts", "42")];
        assert_eq!(time_window(&pushed, "ts"), Some((42, 42)));
    }

    #[test]
    fn time_window_open_ended_range_leaves_the_other_end_unbounded() {
        assert_eq!(
            time_window(&[range("ts", Some("100"), None)], "ts"),
            Some((100, i64::MAX))
        );
        assert_eq!(
            time_window(&[range("ts", None, Some("200"))], "ts"),
            Some((i64::MIN, 200))
        );
    }

    #[test]
    fn chunk_batch_splits_to_bounded_slices() {
        use std::sync::Arc;

        use super::chunk_batch;
        use arrow::array::{Int64Array, RecordBatch};
        use arrow::datatypes::{DataType, Field, Schema};

        let schema = Arc::new(Schema::new(vec![Field::new("x", DataType::Int64, false)]));
        let batch = RecordBatch::try_new(
            schema,
            vec![Arc::new(Int64Array::from((0..10).collect::<Vec<i64>>()))],
        )
        .unwrap();

        let chunks = chunk_batch(&batch, 4);
        assert_eq!(
            chunks.iter().map(RecordBatch::num_rows).collect::<Vec<_>>(),
            vec![4, 4, 2],
        );

        // Slices concatenate back to the original rows, in order.
        let got: Vec<i64> = chunks
            .iter()
            .flat_map(|c| {
                c.column(0)
                    .as_any()
                    .downcast_ref::<Int64Array>()
                    .unwrap()
                    .values()
                    .to_vec()
            })
            .collect();
        assert_eq!(got, (0..10).collect::<Vec<i64>>());

        // `batch_size` 0 is treated as 1 — no panic, no empty-step loop.
        assert_eq!(chunk_batch(&batch, 0).len(), 10);

        // A zero-row batch yields no slices.
        assert!(chunk_batch(&batch.slice(0, 0), 4).is_empty());
    }
}
