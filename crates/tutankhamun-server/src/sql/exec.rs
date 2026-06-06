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
use std::future::Future;
use std::sync::Arc;

use anyhow::Context as _;

use arrow::array::{
    ArrayRef, Int64Array, RecordBatch, RecordBatchOptions, StringArray, TimestampNanosecondArray,
};
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

use super::group_by::OwnedStat;
use super::pushdown::{PushedFilter, PushedOp};
use crate::cache::Cache;
use crate::ftgs::{FtgsRow, StatSpec, ftgs_scan_merge};
use crate::group_lookup::GroupLookup;
use crate::shard::{DiskShard, FilterClause, FilterResult, Shard, decode_int_key, matched_doc_set};
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
        let batches = block_on_scan(|| {
            collect_batches(&self.url, &self.cache, &self.pushed, &self.projected_schema)
        })?;
        Ok(Box::pin(MemoryStream::try_new(
            batches,
            Arc::clone(&self.projected_schema),
            None,
        )?))
    }
}

/// Run a scan's async work to completion from `DataFusion`'s
/// synchronous `execute`, on a dedicated scoped thread with its own
/// current-thread runtime. `block_on` panics if the caller is already
/// inside a runtime, and `block_in_place` only works on the multi-thread
/// scheduler (it panics on the current-thread runtime the CLI builds);
/// a separate thread sidesteps both — correct under any ambient flavor
/// or none. Single-partition, so the one-thread-per-scan cost is
/// bounded; Tier 2 streaming will replace this with a real
/// `RecordBatchStream`. The future is built inside the thread (`make`),
/// so only it crosses the thread boundary, not the future.
fn block_on_scan<T, Fut>(make: impl FnOnce() -> Fut + Send) -> DfResult<T>
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
async fn fetch_selected_shards(
    url: &str,
    cache: &Cache,
    pushed: &[PushedFilter],
) -> anyhow::Result<Vec<(DiskShard, FilterResult)>> {
    let registry = StorageRegistry::from_url(url)?;
    let source = ObjectStoreShardSource::new(registry.store());
    let summaries = source.discover().await?;

    let filter_clauses: Vec<FilterClause<'_>> =
        pushed.iter().map(PushedFilter::as_clause).collect();

    let mut out = Vec::with_capacity(summaries.len());
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
        out.push((shard, selection));
    }
    Ok(out)
}

async fn collect_batches(
    url: &str,
    cache: &Cache,
    pushed: &[PushedFilter],
    projected_schema: &SchemaRef,
) -> anyhow::Result<Vec<RecordBatch>> {
    let shards = fetch_selected_shards(url, cache, pushed).await?;
    let mut batches = Vec::with_capacity(shards.len());
    for (shard, selection) in &shards {
        let batch = build_record_batch(shard, selection, projected_schema)?;
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

/// `ExecutionPlan` for a pushed-down single-column `GROUP BY` (§3.2).
/// Runs the aggregation through FTGS per shard and merges, emitting one
/// pre-aggregated record batch instead of materializing every row.
#[derive(Debug)]
pub(crate) struct FtgsAggExec {
    url: String,
    cache: Arc<Cache>,
    group_col: String,
    stats: Vec<OwnedStat>,
    filters: Vec<PushedFilter>,
    /// Output schema: the group column, then one `Int64` column per
    /// stat — matching the `Aggregate`'s output that this replaces.
    schema: SchemaRef,
    plan_properties: PlanProperties,
}

impl FtgsAggExec {
    pub(crate) fn new(
        url: String,
        cache: Arc<Cache>,
        group_col: String,
        stats: Vec<OwnedStat>,
        filters: Vec<PushedFilter>,
        schema: SchemaRef,
    ) -> Self {
        let plan_properties = PlanProperties::new(
            EquivalenceProperties::new(Arc::clone(&schema)),
            Partitioning::UnknownPartitioning(1),
            EmissionType::Incremental,
            Boundedness::Bounded,
        );
        Self {
            url,
            cache,
            group_col,
            stats,
            filters,
            schema,
            plan_properties,
        }
    }
}

impl DisplayAs for FtgsAggExec {
    fn fmt_as(&self, _t: DisplayFormatType, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "FtgsAggExec: group_by={}, stats={}, pushed_filters={}",
            self.group_col,
            self.stats.len(),
            self.filters.len()
        )
    }
}

impl ExecutionPlan for FtgsAggExec {
    fn name(&self) -> &'static str {
        "FtgsAggExec"
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
        Ok(self)
    }

    fn execute(
        &self,
        _partition: usize,
        _context: Arc<TaskContext>,
    ) -> DfResult<SendableRecordBatchStream> {
        let batch = block_on_scan(|| {
            aggregate_batch(
                &self.url,
                &self.cache,
                &self.group_col,
                &self.stats,
                &self.filters,
                &self.schema,
            )
        })?;
        Ok(Box::pin(MemoryStream::try_new(
            vec![batch],
            Arc::clone(&self.schema),
            None,
        )?))
    }
}

async fn aggregate_batch(
    url: &str,
    cache: &Cache,
    group_col: &str,
    stats: &[OwnedStat],
    filters: &[PushedFilter],
    schema: &SchemaRef,
) -> anyhow::Result<RecordBatch> {
    let shards = fetch_selected_shards(url, cache, filters).await?;

    // Per shard, a group lookup placing WHERE-matching docs in group 1
    // and the rest in group 0 (which FTGS skips). An empty selection
    // contributes no shard.
    let mut pairs: Vec<(&DiskShard, GroupLookup)> = Vec::with_capacity(shards.len());
    for (shard, selection) in &shards {
        let num_docs = usize::try_from(shard.num_docs())?;
        let groups = match selection {
            FilterResult::Empty => continue,
            FilterResult::All => GroupLookup::all_in_one_group(num_docs),
            FilterResult::Bitmap(bm) => {
                let mut g = GroupLookup::constant(num_docs, 0);
                for doc in bm {
                    g.set(doc as usize, 1);
                }
                g
            }
        };
        pairs.push((shard, groups));
    }

    let stat_specs: Vec<StatSpec> = stats.iter().map(OwnedStat::as_spec).collect();
    let refs: Vec<(&dyn Shard, &GroupLookup)> =
        pairs.iter().map(|(s, g)| (*s as &dyn Shard, g)).collect();
    let rows = ftgs_scan_merge(&refs, &[group_col], &stat_specs)?;
    reshape_aggregate(&rows, stats.len(), schema)
}

/// Turn merged FTGS rows into one record batch: column 0 is the `Int`
/// group value decoded from the raw term key, columns 1.. are the stats
/// in `StatSpec` order. All output columns are `Int64`.
fn reshape_aggregate(
    rows: &[FtgsRow],
    num_stats: usize,
    schema: &SchemaRef,
) -> anyhow::Result<RecordBatch> {
    let mut arrays: Vec<ArrayRef> = Vec::with_capacity(1 + num_stats);
    let group: Vec<i64> = rows.iter().map(|r| decode_int_key(&r.term)).collect();
    arrays.push(Arc::new(Int64Array::from(group)));
    for s in 0..num_stats {
        let col: Vec<i64> = rows.iter().map(|r| r.stats[s]).collect();
        arrays.push(Arc::new(Int64Array::from(col)));
    }
    let options = RecordBatchOptions::new().with_row_count(Some(rows.len()));
    Ok(RecordBatch::try_new_with_options(
        Arc::clone(schema),
        arrays,
        &options,
    )?)
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
