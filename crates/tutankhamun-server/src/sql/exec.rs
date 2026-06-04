//! [`TutankhamunExec`] — `DataFusion` `ExecutionPlan` that materialises
//! Arrow record batches from a Tutankhamun dataset.
//!
//! Single-partition, collect-all execution: we walk every discovered
//! shard, fetch through the cache, resolve the pushed filters into a
//! matched-doc set via [`crate::shard::matched_doc_set`], and assemble
//! one record batch per shard from the projected forward columns.
//! Streaming-per-shard and per-shard partitioning are Tier 2 work.

use std::any::Any;
use std::fmt;
use std::sync::Arc;

use anyhow::Context as _;

use arrow::array::{ArrayRef, Int64Array, RecordBatch, StringArray, TimestampNanosecondArray};
use arrow::datatypes::{DataType, SchemaRef, TimeUnit};
use datafusion::common::Result as DfResult;
use datafusion::common::error::DataFusionError;
use datafusion::execution::{SendableRecordBatchStream, TaskContext};
use datafusion::physical_expr::EquivalenceProperties;
use datafusion::physical_plan::execution_plan::{Boundedness, EmissionType};
use datafusion::physical_plan::memory::MemoryStream;
use datafusion::physical_plan::{
    DisplayAs, DisplayFormatType, ExecutionPlan, Partitioning, PlanProperties,
};

use super::pushdown::{PushedFilter, PushedOp};
use crate::cache::Cache;
use crate::shard::{DiskShard, FilterClause, FilterResult, Shard, matched_doc_set};
use crate::shard_source::{ObjectStoreShardSource, ShardSource};
use crate::storage::StorageRegistry;

#[derive(Debug)]
pub(crate) struct TutankhamunExec {
    url: String,
    cache: Arc<Cache>,
    /// Projected output schema; also the source of truth for which
    /// columns each shard contributes (field names, in order).
    projected_schema: SchemaRef,
    /// Owned form of pushed-down predicates so the plan can hold
    /// them past the lifetime of the originating `scan` call.
    pushed: Vec<PushedFilter>,
    plan_properties: PlanProperties,
}

impl TutankhamunExec {
    pub(crate) fn new(
        url: String,
        cache: Arc<Cache>,
        projected_schema: SchemaRef,
        pushed: Vec<PushedFilter>,
    ) -> Self {
        let plan_properties = PlanProperties::new(
            EquivalenceProperties::new(Arc::clone(&projected_schema)),
            Partitioning::UnknownPartitioning(1),
            EmissionType::Incremental,
            Boundedness::Bounded,
        );
        Self {
            url,
            cache,
            projected_schema,
            pushed,
            plan_properties,
        }
    }
}

impl DisplayAs for TutankhamunExec {
    fn fmt_as(&self, _t: DisplayFormatType, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "TutankhamunExec: url={}, pushed_filters={}",
            self.url,
            self.pushed.len()
        )
    }
}

impl ExecutionPlan for TutankhamunExec {
    fn name(&self) -> &'static str {
        "TutankhamunExec"
    }

    fn as_any(&self) -> &dyn Any {
        self
    }

    fn properties(&self) -> &PlanProperties {
        &self.plan_properties
    }

    fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
        Vec::new()
    }

    fn with_new_children(
        self: Arc<Self>,
        _children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> DfResult<Arc<dyn ExecutionPlan>> {
        // Leaf node: no children to swap.
        Ok(self)
    }

    fn execute(
        &self,
        _partition: usize,
        _context: Arc<TaskContext>,
    ) -> DfResult<SendableRecordBatchStream> {
        // DataFusion's `execute` is synchronous, but our scan work is
        // async (cache fetches, shard discovery). We run it on a
        // dedicated scoped thread with its own current-thread runtime.
        //
        // Why not `block_on` / `block_in_place` on the ambient
        // runtime: `block_on` panics if the caller is already inside a
        // runtime, and `block_in_place` only works on the
        // multi-thread scheduler (it panics on a current-thread
        // runtime, which the CLI builds). A separate thread sidesteps
        // both — correct under any ambient flavor or none. Tier 1 is
        // single-partition, so the one-thread-per-scan cost is bounded;
        // Tier 2 streaming will replace this with a proper
        // `RecordBatchStream`.
        let batches = std::thread::scope(|scope| {
            scope
                .spawn(|| {
                    let rt = tokio::runtime::Builder::new_current_thread()
                        .enable_all()
                        .build()?;
                    rt.block_on(collect_batches(
                        &self.url,
                        &self.cache,
                        &self.pushed,
                        &self.projected_schema,
                    ))
                })
                .join()
                .map_err(|_| anyhow::anyhow!("scan thread panicked"))?
        })
        .map_err(|e| DataFusionError::External(e.into()))?;

        Ok(Box::pin(MemoryStream::try_new(
            batches,
            Arc::clone(&self.projected_schema),
            None,
        )?))
    }
}

async fn collect_batches(
    url: &str,
    cache: &Cache,
    pushed: &[PushedFilter],
    projected_schema: &SchemaRef,
) -> anyhow::Result<Vec<RecordBatch>> {
    let registry = StorageRegistry::from_url(url)?;
    let source = ObjectStoreShardSource::new(registry.store());
    let summaries = source.discover().await?;

    let filter_clauses: Vec<FilterClause<'_>> =
        pushed.iter().map(PushedFilter::as_clause).collect();

    let mut batches = Vec::with_capacity(summaries.len());
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
        let selection = matched_doc_set(&shard, &filter_clauses)?;
        let batch = build_record_batch(&shard, &selection, projected_schema)?;
        if batch.num_rows() > 0 {
            batches.push(batch);
        }
    }
    Ok(batches)
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

fn build_record_batch(
    shard: &DiskShard,
    selection: &FilterResult,
    projected_schema: &SchemaRef,
) -> anyhow::Result<RecordBatch> {
    let arrays: Vec<ArrayRef> = projected_schema
        .fields()
        .iter()
        .map(|field| -> anyhow::Result<ArrayRef> {
            let col_name = field.name();
            match field.data_type() {
                DataType::Int64 => int_column(shard, col_name, selection),
                DataType::Timestamp(TimeUnit::Nanosecond, _) => {
                    timestamp_column(shard, col_name, selection)
                }
                DataType::Utf8 => string_column(shard, col_name, selection),
                other => anyhow::bail!("unsupported projected type {other:?} for {col_name:?}"),
            }
        })
        .collect::<anyhow::Result<_>>()?;
    // Required when there are no projected columns (e.g. SELECT
    // COUNT(*) FROM …): Arrow can't infer the row count from zero
    // arrays, so we tell it explicitly.
    let row_count = match selection {
        FilterResult::All => usize::try_from(shard.num_docs())?,
        FilterResult::Empty => 0,
        FilterResult::Bitmap(bm) => usize::try_from(bm.len())?,
    };
    let options = arrow::array::RecordBatchOptions::new().with_row_count(Some(row_count));
    Ok(RecordBatch::try_new_with_options(
        Arc::clone(projected_schema),
        arrays,
        &options,
    )?)
}

/// Gather the i64 values of forward column `col_name` selected by
/// `selection`, in doc order. Shared by `int_column` (Int64 output)
/// and `timestamp_column` (the time field, same i64 epoch values
/// presented as Timestamp).
fn gather_i64(
    shard: &DiskShard,
    col_name: &str,
    selection: &FilterResult,
) -> anyhow::Result<Vec<i64>> {
    let col = shard
        .forward_column(col_name)
        .ok_or_else(|| anyhow::anyhow!("no forward column for {col_name:?}"))?;
    Ok(match selection {
        FilterResult::All => col.to_vec(),
        FilterResult::Empty => Vec::new(),
        FilterResult::Bitmap(bm) => bm.iter().map(|d| col[d as usize]).collect(),
    })
}

fn int_column(
    shard: &DiskShard,
    col_name: &str,
    selection: &FilterResult,
) -> anyhow::Result<ArrayRef> {
    Ok(Arc::new(Int64Array::from(gather_i64(
        shard, col_name, selection,
    )?)))
}

/// The time field: the same epoch-seconds forward column as any Int
/// field, scaled to nanoseconds and presented to `DataFusion` as
/// `Timestamp(Nanosecond)` so SQL can filter it with date/timestamp
/// literals without an injected cast. Overflows i64 only past ~year
/// 2262 (seconds × 1e9), which is far beyond any realistic shard.
fn timestamp_column(
    shard: &DiskShard,
    col_name: &str,
    selection: &FilterResult,
) -> anyhow::Result<ArrayRef> {
    let nanos: Vec<i64> = gather_i64(shard, col_name, selection)?
        .into_iter()
        .map(|secs| {
            secs.checked_mul(1_000_000_000).ok_or_else(|| {
                anyhow::anyhow!("time value {secs} (epoch seconds) overflows nanosecond range")
            })
        })
        .collect::<anyhow::Result<_>>()?;
    Ok(Arc::new(TimestampNanosecondArray::from(nanos)))
}

/// Reconstruct per-doc string values from the inverted index by
/// walking every term in the FST once and recording, per doc-id,
/// which term covers it. Each term is allocated exactly once (held
/// in `terms`); `doc_term` stores indices into it, so a high-
/// cardinality field costs one `String` per *term*, not per *doc*.
/// `O(num_docs)` total work per shard per field.
fn string_column(
    shard: &DiskShard,
    col_name: &str,
    selection: &FilterResult,
) -> anyhow::Result<ArrayRef> {
    let num_docs = usize::try_from(shard.num_docs())?;
    if num_docs == 0 || matches!(selection, FilterResult::Empty) {
        return Ok(Arc::new(StringArray::from(Vec::<&str>::new())));
    }
    let idx = shard
        .inverted_index(col_name)
        .ok_or_else(|| anyhow::anyhow!("no inverted index for {col_name:?}"))?;
    let mut terms: Vec<String> = Vec::new();
    let mut doc_term: Vec<Option<usize>> = vec![None; num_docs];
    for (term_bytes, bitmap) in idx.range_bytes(None, None) {
        let term = std::str::from_utf8(&term_bytes)
            .with_context(|| format!("non-UTF-8 term in string field {col_name:?}"))?
            .to_string();
        let term_idx = terms.len();
        terms.push(term);
        for doc in &bitmap {
            doc_term[doc as usize] = Some(term_idx);
        }
    }
    let term_at = |doc: usize| doc_term[doc].map(|i| terms[i].as_str());
    let values: Vec<Option<&str>> = match selection {
        FilterResult::All => (0..num_docs).map(term_at).collect(),
        FilterResult::Empty => unreachable!("handled above"),
        FilterResult::Bitmap(bm) => bm.iter().map(|d| term_at(d as usize)).collect(),
    };
    Ok(Arc::new(StringArray::from(values)))
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
}
