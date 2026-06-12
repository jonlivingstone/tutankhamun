//! Scan prelude shared by both Tutankhamun execution plans — the row scan
//! ([`super::exec`]) and the pushed-down aggregate ([`super::ftgs_agg`]).
//!
//! Holds the synchronous-`execute` → async bridge ([`block_on_scan`]), the
//! discover → time-prune → fetch → resolve-filters step
//! ([`fetch_selected_shards`]), and the bounded record-batch streaming
//! ([`chunked_stream`]). Neither exec depends on the other — both depend on
//! this neutral module.

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
use crate::memory::{SessionMemoryHandle, SessionReservation};
use crate::shard::{DiskShard, FilterClause, FilterResult, METRICS_FILE, matched_doc_set};
use crate::shard_source::{ObjectStoreShardSource, ShardSource};
use crate::storage::StorageRegistry;

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

/// Discover the dataset's shards, prune by time, fetch + open each, and
/// resolve the pushed filters to a matched-doc set. The shared prelude
/// for both the row scan ([`collect_batches`]) and the FTGS aggregate
/// ([`aggregate_batch`]).
///
/// When `mem` is set (the daemon data plane — §2.2), each opened shard's
/// forward-column working set (`metrics.arrow` byte size) is reserved against
/// the session budget; a query that would exceed it fails here rather than
/// letting resident memory blow past the cap. The reservations are returned
/// alongside the shards so the
/// charge is held exactly as long as the shards are resident and released when
/// the caller drops them. `None` (the `t9n sql` CLI) charges nothing.
///
/// When `bitmap_cache` is set, the matched-doc set is served through the §2.8
/// cache (exact hit or monotone-narrowing reuse) instead of always rescanning the
/// inverted index; `None` resolves directly via [`matched_doc_set`].
pub(crate) async fn fetch_selected_shards(
    url: &str,
    cache: &Cache,
    pushed: &[PushedFilter],
    mem: Option<&Arc<SessionMemoryHandle>>,
    bitmap_cache: Option<&Arc<BitmapCache>>,
) -> anyhow::Result<(Vec<(DiskShard, FilterResult)>, Vec<SessionReservation>)> {
    let registry = StorageRegistry::from_url(url)?;
    let source = ObjectStoreShardSource::new(registry.store());
    let summaries = source.discover().await?;

    let mut out = Vec::with_capacity(summaries.len());
    let mut reservations = Vec::new();
    for summary in &summaries {
        // Prune whole shards before fetching them: if the query
        // constrains the time field to a window that the shard's
        // `[time_range_start, time_range_end]` can't intersect, the
        // shard has no matching docs and we skip the cache fetch
        // entirely.
        if let Some(time_field) = summary.metadata.time_field.as_deref()
            && let Some((from, to)) = time_window(pushed, time_field)
            && !summary.intersects_time_range(from, to)
        {
            continue;
        }
        let local_dir = cache.fetch_shard(summary).await?;
        let shard = DiskShard::open(&local_dir)?;
        if let Some(handle) = mem {
            // Rough resident estimate: the forward-column file size (§2.2). A
            // missing file (statless edge) charges nothing rather than failing.
            let bytes = std::fs::metadata(local_dir.join(METRICS_FILE)).map_or(0, |m| m.len());
            reservations.push(handle.reserve(bytes).map_err(|e| anyhow::anyhow!(e))?);
        }
        let selection = if let Some(bc) = bitmap_cache {
            let id = BitmapCache::shard_id(url, summary.location.as_ref());
            bc.resolve(&shard, id, pushed)?
        } else {
            let clauses: Vec<FilterClause<'_>> =
                pushed.iter().map(PushedFilter::as_clause).collect();
            matched_doc_set(&shard, &clauses)?
        };
        out.push((shard, selection));
    }
    Ok((out, reservations))
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
