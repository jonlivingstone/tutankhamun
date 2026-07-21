//! [`TutankhamunExec`] — `DataFusion` `ExecutionPlan` that materialises
//! Arrow record batches from a Tutankhamun dataset.
//!
//! Single-partition, collect-all execution: we walk every discovered
//! shard, fetch through the cache, resolve the pushed filters into a
//! matched-doc set via [`crate::shard::matched_doc_set`], and assemble
//! one record batch per shard from the projected forward columns.
//!
//! The shared scan prelude (discover → fetch → bounded streaming) lives in
//! [`super::scan`]; the pushed-down aggregate plan in [`super::ftgs_agg`].

use std::any::Any;
use std::fmt;
use std::sync::Arc;

use anyhow::Context as _;

use arrow::array::{ArrayRef, Int64Array, RecordBatch, StringArray, TimestampNanosecondArray};
use arrow::buffer::ScalarBuffer;
use arrow::datatypes::{DataType, SchemaRef, TimeUnit};
use datafusion::common::Result as DfResult;
use datafusion::execution::{SendableRecordBatchStream, TaskContext};
use datafusion::physical_expr::EquivalenceProperties;
use datafusion::physical_plan::execution_plan::{Boundedness, EmissionType};
use datafusion::physical_plan::{
    DisplayAs, DisplayFormatType, ExecutionPlan, Partitioning, PlanProperties,
};

use super::pushdown::PushedFilter;
use super::scan::{ScanCtx, block_on_scan, chunked_stream, for_each_shard_batch};
use crate::bitmap_cache::BitmapCache;
use crate::cache::Cache;
use crate::memory::SessionMemoryHandle;
use crate::shard::{DiskShard, FilterResult, Shard};
use crate::shard_source::ShardSummary;

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
    /// The dataset's shard set, resolved once by the provider — the scan reuses
    /// it instead of re-discovering.
    summaries: Arc<Vec<ShardSummary>>,
    plan_properties: PlanProperties,
}

impl TutankhamunExec {
    pub(crate) fn new(
        url: String,
        cache: Arc<Cache>,
        projected_schema: SchemaRef,
        pushed: Vec<PushedFilter>,
        summaries: Arc<Vec<ShardSummary>>,
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
            summaries,
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
        context: Arc<TaskContext>,
    ) -> DfResult<SendableRecordBatchStream> {
        // Per-session memory handle (§2.2), threaded in as a SessionConfig
        // extension; `None` for contexts that don't set it (the CLI).
        let mem = context
            .session_config()
            .get_extension::<SessionMemoryHandle>();
        let bitmap_cache = context.session_config().get_extension::<BitmapCache>();
        let ctx = ScanCtx {
            url: &self.url,
            summaries: &self.summaries,
            cache: &self.cache,
            mem: mem.as_ref(),
            bitmap_cache: bitmap_cache.as_ref(),
        };
        let batches =
            block_on_scan(|| collect_batches(&ctx, &self.pushed, &self.projected_schema))?;
        // Each shard contributes one (possibly large) batch; cap every emitted
        // batch to the session's configured row count for streaming.
        chunked_stream(batches, Arc::clone(&self.projected_schema), &context)
    }
}

async fn collect_batches(
    ctx: &ScanCtx<'_>,
    pushed: &[PushedFilter],
    projected_schema: &SchemaRef,
) -> anyhow::Result<Vec<RecordBatch>> {
    // Stream shards in bounded batches so forward-column *residency* stays
    // bounded to `scan_batch_width` shards regardless of span. NOTE: the
    // assembled output still accumulates into one `Vec<RecordBatch>` — an
    // unbounded `SELECT *` holds the full result in memory before streaming.
    // Bounding that (output backpressure) is a separate concern, not handled here.
    let mut batches = Vec::new();
    // Per-doc heap estimate for the bounded envelope (§2.2): each projected
    // column gathers ~8 bytes per matched doc (i64/timestamp). Strings are
    // term-deduplicated, so counting them at 8 B/doc over-estimates — keeping
    // the bound conservative.
    let per_doc = (projected_schema.fields().len().max(1) * std::mem::size_of::<i64>()) as u64;
    for_each_shard_batch(ctx, pushed, per_doc, |chunk| {
        for (_id, shard, selection) in chunk {
            let batch = build_record_batch(shard, selection, projected_schema)?;
            if batch.num_rows() > 0 {
                batches.push(batch);
            }
        }
        Ok(())
    })
    .await?;
    Ok(batches)
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
    // Non-nullable (the common case): dense fast path, no per-doc validity work.
    let Some(validity) = shard.forward_column_validity(col_name) else {
        return Ok(Arc::new(Int64Array::from(gather_i64(
            shard, col_name, selection,
        )?)));
    };
    // Nullable: carry the null mask into the output so a NULL doc projects to SQL
    // NULL (its `values` slot holds a placeholder).
    let col = shard
        .forward_column(col_name)
        .ok_or_else(|| anyhow::anyhow!("no forward column for {col_name:?}"))?;
    let array = match selection {
        // Whole column: copy the dense values (as the non-nullable path also
        // does) but carry the existing null buffer straight through — an
        // Arc-backed clone — instead of re-deriving validity bit-by-bit through
        // `Vec<Option<i64>>`.
        FilterResult::All => {
            Int64Array::new(ScalarBuffer::from(col.to_vec()), Some(validity.clone()))
        }
        FilterResult::Empty => Int64Array::from(Vec::<Option<i64>>::new()),
        // A subset selection has to gather both value and validity per doc.
        FilterResult::Bitmap(bm) => Int64Array::from(
            bm.iter()
                .map(|d| {
                    let d = d as usize;
                    validity.is_valid(d).then(|| col[d])
                })
                .collect::<Vec<Option<i64>>>(),
        ),
    };
    Ok(Arc::new(array))
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
