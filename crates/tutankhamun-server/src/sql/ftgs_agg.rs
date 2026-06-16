//! [`FtgsAggExec`] — the `DataFusion` `ExecutionPlan` for a pushed-down
//! aggregate. Streams the dataset's shards in bounded batches
//! ([`for_each_shard_batch`](super::scan::for_each_shard_batch)), folding each
//! batch's per-shard FTGS partials into a running merged result, then reshapes
//! the merged stats into one Arrow record batch (the group columns followed by
//! one column per stat) instead of materialising every row. The synchronous-
//! `execute` → async bridge ([`block_on_scan`](super::scan::block_on_scan)) and
//! bounded output streaming ([`chunked_stream`](super::scan::chunked_stream))
//! live in [`super::scan`]; the row-scan exec in [`super::exec`].

use std::any::Any;
use std::collections::BTreeMap;
use std::fmt;
use std::sync::Arc;

use arrow::array::{
    ArrayRef, BinaryBuilder, Float64Array, Int64Array, ListArray, RecordBatch, RecordBatchOptions,
    StringArray, StructArray, TimestampNanosecondArray, UInt64Array,
};
use arrow::buffer::OffsetBuffer;
use arrow::datatypes::{DataType, FieldRef, SchemaRef, TimeUnit};
use chrono::{DateTime, Datelike, Duration};
use datafusion::common::Result as DfResult;
use datafusion::execution::{SendableRecordBatchStream, TaskContext};
use datafusion::physical_expr::EquivalenceProperties;
use datafusion::physical_plan::execution_plan::{Boundedness, EmissionType};
use datafusion::physical_plan::{
    DisplayAs, DisplayFormatType, ExecutionPlan, Partitioning, PlanProperties,
};

use roaring::RoaringBitmap;

use super::group_by::{BucketUnit, GroupKey, OwnedStat};
use super::pushdown::PushedFilter;
use super::scan::{block_on_scan, chunked_stream, for_each_shard_batch};
use crate::aggregate_cache::{AggregateCache, GroupTuple, ShardPartial};
use crate::bitmap_cache::BitmapCache;
use crate::cache::Cache;
use crate::ftgs::{
    OutputKind, StatSpec, StatValue, aggregate_docs, aggregate_docs_grouped, combine_stats,
    ftgs_scan, render_term, term_map,
};
use crate::group_lookup::GroupLookup;
use crate::memory::SessionMemoryHandle;
use crate::shard::{DiskShard, FieldKind, FilterResult, Shard, decode_int_key, encode_int_key};
use crate::sketches::ThetaSketch;

/// `ExecutionPlan` for a pushed-down aggregate (§3.2). Runs the
/// aggregation through FTGS per shard and merges, emitting one
/// pre-aggregated record batch instead of materializing every row.
/// `group_cols` are the `GROUP BY` keys (bare columns and/or `date_trunc`
/// time buckets, in `GROUP BY` order); empty means a global aggregate
/// (no `GROUP BY` — one row over the whole filtered set).
#[derive(Debug)]
pub(crate) struct FtgsAggExec {
    url: String,
    cache: Arc<Cache>,
    group_cols: Vec<GroupKey>,
    stats: Vec<OwnedStat>,
    filters: Vec<PushedFilter>,
    /// Output schema, matching the `Aggregate` this replaces: the group
    /// columns (in `GROUP BY` order) followed by one column per stat.
    schema: SchemaRef,
    plan_properties: PlanProperties,
}

impl FtgsAggExec {
    pub(crate) fn new(
        url: String,
        cache: Arc<Cache>,
        group_cols: Vec<GroupKey>,
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
            group_cols,
            stats,
            filters,
            schema,
            plan_properties,
        }
    }
}

impl DisplayAs for FtgsAggExec {
    fn fmt_as(&self, _t: DisplayFormatType, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let group_by = if self.group_cols.is_empty() {
            "(global)".to_string()
        } else {
            self.group_cols
                .iter()
                .map(GroupKey::to_string)
                .collect::<Vec<_>>()
                .join(",")
        };
        write!(
            f,
            "FtgsAggExec: group_by={group_by}, stats={}, pushed_filters={}",
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
        context: Arc<TaskContext>,
    ) -> DfResult<SendableRecordBatchStream> {
        let mem = context
            .session_config()
            .get_extension::<SessionMemoryHandle>();
        let bitmap_cache = context.session_config().get_extension::<BitmapCache>();
        let agg_cache = context.session_config().get_extension::<AggregateCache>();
        let batch = block_on_scan(|| {
            aggregate_batch(
                &self.url,
                &self.cache,
                &self.group_cols,
                &self.stats,
                &self.filters,
                &self.schema,
                mem.as_ref(),
                bitmap_cache.as_ref(),
                agg_cache.as_ref(),
            )
        })?;
        // One row per group; cap every emitted batch to the session's
        // configured row count for streaming.
        chunked_stream(vec![batch], Arc::clone(&self.schema), &context)
    }
}

#[allow(clippy::too_many_arguments)]
async fn aggregate_batch(
    url: &str,
    cache: &Cache,
    group_cols: &[GroupKey],
    stats: &[OwnedStat],
    filters: &[PushedFilter],
    schema: &SchemaRef,
    mem: Option<&Arc<SessionMemoryHandle>>,
    bitmap_cache: Option<&Arc<BitmapCache>>,
    agg_cache: Option<&Arc<AggregateCache>>,
) -> anyhow::Result<RecordBatch> {
    if group_cols.is_empty() {
        let acc = global_stats(url, cache, stats, filters, mem, bitmap_cache, agg_cache).await?;
        return reshape_global(acc.as_deref(), stats, schema);
    }
    // A time bucket can't be the index-walked cursor (the time field's index is
    // per-second, not per-bucket), so any bucket forces the regrouped path: fold
    // the whole group tuple into a combo id and aggregate per group — no cursor.
    if group_cols
        .iter()
        .any(|k| matches!(k, GroupKey::TimeBucket { .. }))
    {
        return aggregate_regrouped(
            url,
            cache,
            group_cols,
            stats,
            filters,
            mem,
            bitmap_cache,
            agg_cache,
            schema,
        )
        .await;
    }
    // All bare columns → the cursor-walk path (regroup the prefix, scan the last).
    aggregate_grouped(
        url,
        cache,
        group_cols,
        stats,
        filters,
        mem,
        bitmap_cache,
        agg_cache,
        schema,
    )
    .await
}

/// Conservative per-doc heap working-set estimate for the bounded scan envelope
/// (§2.2): each grouping column and stat contributes ~8 bytes of buffer per
/// doc, plus a slot for the group lookup. An upper bound, so admission stays
/// honest — over-estimating only refuses genuinely-too-big work, it never lets
/// the resident set blow past the cap.
fn per_doc_estimate(group_cols: usize, stats: usize) -> u64 {
    ((group_cols + stats + 1) * std::mem::size_of::<i64>()) as u64
}

/// `GROUP BY g1..gk` over bare columns: regroup the prefix `g1..g(k-1)` into each
/// shard's group lookup, FTGS-scan the last column `gk` (the index-walked
/// cursor), and emit one self-contained partial per shard — `(prefix values +
/// cursor term) -> stats`. A `String` cursor can leave docs term-less; those form
/// a NULL-cursor group. Partials cache per shard ([`AggregateCache`]) and combine
/// across shards by tuple ([`combine_partial`]); shards stream in bounded batches.
#[allow(clippy::too_many_arguments)]
async fn aggregate_grouped(
    url: &str,
    cache: &Cache,
    group_cols: &[GroupKey],
    stats: &[OwnedStat],
    filters: &[PushedFilter],
    mem: Option<&Arc<SessionMemoryHandle>>,
    bitmap_cache: Option<&Arc<BitmapCache>>,
    agg_cache: Option<&Arc<AggregateCache>>,
    schema: &SchemaRef,
) -> anyhow::Result<RecordBatch> {
    let stat_specs: Vec<StatSpec> = stats.iter().map(OwnedStat::as_spec).collect();
    let cols: Vec<String> = group_cols.iter().map(|k| k.col().to_string()).collect();
    let (cursor, prefix) = cols
        .split_last()
        .expect("grouped path has at least one column");
    let cursor = cursor.as_str();
    // The cursor is output column `prefix.len()`. A `String` cursor can be sparse
    // (the SQL NULL group); `Int` is dense.
    let cursor_is_string = matches!(schema.field(prefix.len()).data_type(), DataType::Utf8);

    let mut combined: BTreeMap<GroupTuple, Vec<StatValue>> = BTreeMap::new();
    let per_doc = per_doc_estimate(cols.len(), stat_specs.len());

    for_each_shard_batch(url, cache, filters, mem, bitmap_cache, per_doc, |chunk| {
        for (id, shard, selection) in chunk {
            let partial = cached_partial(agg_cache, *id, filters, group_cols, stats, || {
                cursor_walk_shard(
                    shard,
                    selection,
                    prefix,
                    cursor,
                    cursor_is_string,
                    &stat_specs,
                )
            })?;
            combine_partial(&mut combined, &partial, &stat_specs);
        }
        Ok(())
    })
    .await?;

    reshape_regrouped(&combined, cols.len(), &stat_specs, schema)
}

/// One shard's grouped aggregate via the FTGS index-walk: regroup the prefix into
/// a *local* combo, scan the cursor column's index, and return `(prefix values +
/// cursor term) -> stats` per `(group, term)`. Self-contained (group values, not
/// the cross-shard combo ids the old batch-merge used), so it caches per shard.
fn cursor_walk_shard(
    shard: &DiskShard,
    selection: &FilterResult,
    prefix: &[String],
    cursor: &str,
    cursor_is_string: bool,
    stat_specs: &[StatSpec<'_>],
) -> anyhow::Result<ShardPartial> {
    let matched: Option<&RoaringBitmap> = match selection {
        FilterResult::Empty => return Ok(Vec::new()),
        FilterResult::All => None,
        FilterResult::Bitmap(bm) => Some(bm),
    };
    let mut combo = ComboMap::default();
    let groups = build_groups(shard, matched, prefix, &mut combo)?;
    // Recover the prefix values for a group id. The single-column case (empty
    // prefix) carries no prefix — just the membership marker group 1.
    let prefix_tuple = |gid: u32| -> GroupTuple {
        if prefix.is_empty() {
            Vec::new()
        } else {
            combo.inverse[(gid - 1) as usize].clone()
        }
    };

    let mut out: ShardPartial = Vec::new();
    for row in ftgs_scan(shard, &groups, &[cursor], stat_specs)? {
        let mut tuple = prefix_tuple(row.group);
        tuple.push(Some(row.term));
        out.push((tuple, row.stats));
    }
    // String cursors can leave matched docs term-less — the SQL NULL group.
    if cursor_is_string {
        let mut null_stats: BTreeMap<u32, Vec<StatValue>> = BTreeMap::new();
        accumulate_null_cursor(shard, matched, cursor, &groups, stat_specs, &mut null_stats)?;
        for (gid, st) in null_stats {
            let mut tuple = prefix_tuple(gid);
            tuple.push(None);
            out.push((tuple, st));
        }
    }
    Ok(out)
}

/// Map each matched doc to its prefix-tuple combo id (single-column: group 1
/// for matched docs). The cross-shard `combo` assigns a stable id per tuple, so
/// the same tuple folds together across shards and batches.
fn build_groups(
    shard: &DiskShard,
    matched: Option<&RoaringBitmap>,
    prefix: &[String],
    combo: &mut ComboMap,
) -> anyhow::Result<GroupLookup> {
    let num_docs = usize::try_from(shard.num_docs())?;
    if prefix.is_empty() {
        // Single column: the lookup carries only WHERE membership (matched →
        // group 1), so the unfiltered case stays O(1).
        return Ok(match matched {
            None => GroupLookup::all_in_one_group(num_docs),
            Some(bm) => {
                let mut g = GroupLookup::constant(num_docs, 0);
                for doc in bm {
                    g.set(doc as usize, 1);
                }
                g
            }
        });
    }
    // Regroup each matched doc by its prefix tuple's combo id.
    let keys: Vec<DocKeys> = prefix
        .iter()
        .map(|c| DocKeys::build(shard, c))
        .collect::<anyhow::Result<_>>()?;
    let mut g = GroupLookup::constant(num_docs, 0);
    match matched {
        None => {
            for doc in 0..num_docs {
                let key = keys.iter().map(|k| k.key(doc)).collect();
                g.set(doc, combo.id(key));
            }
        }
        Some(bm) => {
            for doc in bm {
                let doc = doc as usize;
                let key = keys.iter().map(|k| k.key(doc)).collect();
                g.set(doc, combo.id(key));
            }
        }
    }
    Ok(g)
}

/// Fold the NULL-cursor docs (matched docs with no term for a `String` cursor)
/// into `null_stats`, one entry per combo group — the SQL NULL-keyed output row.
fn accumulate_null_cursor(
    shard: &DiskShard,
    matched: Option<&RoaringBitmap>,
    cursor: &str,
    groups: &GroupLookup,
    stat_specs: &[StatSpec<'_>],
    null_stats: &mut BTreeMap<u32, Vec<StatValue>>,
) -> anyhow::Result<()> {
    let covered = covered_docs(shard, cursor);
    let null_docs = match matched {
        None => {
            let mut all = RoaringBitmap::new();
            all.insert_range(0..u32::try_from(shard.num_docs())?);
            all - covered
        }
        Some(bm) => bm.clone() - covered,
    };
    for (gid, st) in aggregate_docs_grouped(shard, null_docs.iter(), groups, stat_specs)? {
        match null_stats.get_mut(&gid) {
            Some(acc) => combine_stats(acc, &st, stat_specs),
            None => {
                null_stats.insert(gid, st);
            }
        }
    }
    Ok(())
}

/// `GROUP BY` including a time bucket: regroup the **whole** group tuple
/// (buckets truncated, categoricals by value) into one combo id per distinct
/// tuple and aggregate per group — no index-walked cursor, because the time
/// field's inverted index is per-*second*, not per-bucket. Handles any
/// mix/order of buckets + low-cardinality categoricals uniformly. Streams
/// shards in bounded batches ([`for_each_shard_batch`]); the cross-shard `combo`
/// and the running per-group stats accumulate across batches while each batch's
/// shards drop.
#[allow(clippy::too_many_arguments)]
async fn aggregate_regrouped(
    url: &str,
    cache: &Cache,
    group_cols: &[GroupKey],
    stats: &[OwnedStat],
    filters: &[PushedFilter],
    mem: Option<&Arc<SessionMemoryHandle>>,
    bitmap_cache: Option<&Arc<BitmapCache>>,
    agg_cache: Option<&Arc<AggregateCache>>,
    schema: &SchemaRef,
) -> anyhow::Result<RecordBatch> {
    let stat_specs: Vec<StatSpec> = stats.iter().map(OwnedStat::as_spec).collect();
    // Combine per-shard partials by group-value tuple. Group values are
    // self-contained (not per-shard ids), so this folds across shards/batches
    // and lets each shard's partial be cached and reused independently.
    let mut combined: BTreeMap<GroupTuple, Vec<StatValue>> = BTreeMap::new();
    let per_doc = per_doc_estimate(group_cols.len(), stat_specs.len());

    for_each_shard_batch(url, cache, filters, mem, bitmap_cache, per_doc, |chunk| {
        for (id, shard, selection) in chunk {
            let partial = cached_partial(agg_cache, *id, filters, group_cols, stats, || {
                process_regrouped_shard(shard, selection, group_cols, &stat_specs)
            })?;
            combine_partial(&mut combined, &partial, &stat_specs);
        }
        Ok(())
    })
    .await?;

    reshape_regrouped(&combined, group_cols.len(), &stat_specs, schema)
}

/// Fetch a shard's partial from `agg_cache` (computing + storing on a miss), or
/// compute it directly when no cache is wired (the CLI). The cache makes a
/// shard's `(filter, group-keys, stats)` partial reusable across reruns and
/// across queries that touch the same immutable shard.
fn cached_partial<F>(
    agg_cache: Option<&Arc<AggregateCache>>,
    shard_id: u64,
    filters: &[PushedFilter],
    group_cols: &[GroupKey],
    stats: &[OwnedStat],
    compute: F,
) -> anyhow::Result<Arc<ShardPartial>>
where
    F: FnOnce() -> anyhow::Result<ShardPartial>,
{
    match agg_cache {
        Some(c) => c.get_or_compute(shard_id, filters, group_cols, stats, compute),
        None => Ok(Arc::new(compute()?)),
    }
}

/// Fold one shard's partial into the running cross-shard accumulator, combining
/// stats for group tuples already seen and inserting new ones.
fn combine_partial(
    combined: &mut BTreeMap<GroupTuple, Vec<StatValue>>,
    partial: &ShardPartial,
    stat_specs: &[StatSpec<'_>],
) {
    for (tuple, st) in partial {
        match combined.get_mut(tuple) {
            Some(acc) => combine_stats(acc, st, stat_specs),
            None => {
                combined.insert(tuple.clone(), st.clone());
            }
        }
    }
}

/// One shard's aggregate for a regrouped (time-bucket) query: each distinct
/// group-value tuple with its stats. Self-contained (group *values*, not global
/// ids), so it caches per shard ([`AggregateCache`]) and combines across shards.
fn process_regrouped_shard(
    shard: &DiskShard,
    selection: &FilterResult,
    group_cols: &[GroupKey],
    stat_specs: &[StatSpec<'_>],
) -> anyhow::Result<ShardPartial> {
    let num_docs = usize::try_from(shard.num_docs())?;
    let matched: Option<&RoaringBitmap> = match selection {
        FilterResult::Empty => return Ok(Vec::new()),
        FilterResult::All => None,
        FilterResult::Bitmap(bm) => Some(bm),
    };
    let sources: Vec<KeySource> = group_cols
        .iter()
        .map(|k| KeySource::build(shard, k))
        .collect::<anyhow::Result<_>>()?;

    // Shard-granular fast path: when every key is constant for this shard — pure
    // time-bucket grouping where the shard's time range falls in one bucket — the
    // whole matched set is a single group, so one `aggregate_docs` does it with no
    // per-doc truncation or combo. (Weekly shards under month/quarter/year.)
    if sources.iter().all(|s| matches!(s, KeySource::Const(_))) {
        let tuple: Vec<Option<Box<[u8]>>> = sources.iter().map(|s| s.key(0)).collect();
        let st = match matched {
            None => aggregate_docs(shard, 0..u32::try_from(num_docs)?, stat_specs)?,
            Some(bm) => aggregate_docs(shard, bm.iter(), stat_specs)?,
        };
        return Ok(vec![(tuple, st)]);
    }

    // Mixed: at least one per-doc key (a categorical column, or a time bucket on a
    // shard that straddles a boundary). Regroup matched docs into a local combo,
    // aggregate per group, then emit (group tuple, stats) by decoding that combo.
    let mut combo = ComboMap::default();
    let mut g = GroupLookup::constant(num_docs, 0);
    match matched {
        None => {
            for doc in 0..num_docs {
                let tuple = sources.iter().map(|s| s.key(doc)).collect();
                g.set(doc, combo.id(tuple));
            }
        }
        Some(bm) => {
            for doc in bm {
                let doc = doc as usize;
                let tuple = sources.iter().map(|s| s.key(doc)).collect();
                g.set(doc, combo.id(tuple));
            }
        }
    }
    // Aggregate only matched docs (the unmatched keep group 0 and are skipped).
    let per_group = match matched {
        None => aggregate_docs_grouped(shard, 0..u32::try_from(num_docs)?, &g, stat_specs)?,
        Some(bm) => aggregate_docs_grouped(shard, bm.iter(), &g, stat_specs)?,
    };
    Ok(per_group
        .into_iter()
        .map(|(gid, st)| (combo.inverse[(gid - 1) as usize].clone(), st))
        .collect())
}

/// Per-shard source for a group key's value: a constant (a single-bucket time
/// key, computed once from the shard's time range) or a per-doc lookup (a
/// categorical column, or a time bucket on a shard that straddles a boundary).
enum KeySource<'a> {
    Const(Option<Box<[u8]>>),
    PerDoc(DocKeys<'a>),
}

impl<'a> KeySource<'a> {
    fn build(shard: &'a DiskShard, key: &GroupKey) -> anyhow::Result<KeySource<'a>> {
        if let GroupKey::TimeBucket { unit, .. } = key {
            let (lo, hi) = shard.time_range();
            let (blo, bhi) = (truncate_epoch(lo, *unit), truncate_epoch(hi, *unit));
            if blo == bhi {
                // The whole shard is in one bucket — constant key, computed once.
                return Ok(KeySource::Const(Some(Box::from(
                    encode_int_key(blo).as_slice(),
                ))));
            }
        }
        Ok(KeySource::PerDoc(DocKeys::for_key(shard, key)?))
    }

    fn key(&self, doc: usize) -> Option<Box<[u8]>> {
        match self {
            KeySource::Const(v) => v.clone(),
            KeySource::PerDoc(dk) => dk.key(doc),
        }
    }
}

/// Assigns a stable group id (from 1) to each distinct prefix-column
/// tuple, shared across all shards so a tuple maps to the same id
/// everywhere — what lets [`ftgs_scan_merge`] fold matching `(cursor
/// term, group)` rows across shards. `inverse[id - 1]` recovers the tuple
/// for output; a `None` component is a SQL NULL group value.
#[derive(Default)]
struct ComboMap {
    map: BTreeMap<Vec<Option<Box<[u8]>>>, u32>,
    inverse: Vec<Vec<Option<Box<[u8]>>>>,
}

impl ComboMap {
    fn id(&mut self, key: Vec<Option<Box<[u8]>>>) -> u32 {
        if let Some(&id) = self.map.get(&key) {
            return id;
        }
        let id = u32::try_from(self.inverse.len() + 1).expect("combo group count fits u32");
        self.inverse.push(key.clone());
        self.map.insert(key, id);
        id
    }
}

/// Per-doc value source for a regroup column, yielding a doc's
/// FTGS-canonical term-key bytes — the same encoding the cursor's
/// `FtgsRow.term` carries, so reshape decodes prefix and cursor columns
/// the same way.
enum DocKeys<'a> {
    /// `Int`/`Metric`: dense forward column; key = order-preserving 8-byte.
    Int(&'a [i64]),
    /// `String`: doc→term reverse map; `None` for a doc with no term.
    Str {
        doc_term: Vec<Option<u32>>,
        terms: Vec<Box<[u8]>>,
    },
    /// Time bucket: dense epoch-seconds forward column; key = the
    /// bucket-start epoch (`encode_int_key`-encoded like any `Int`).
    TimeBucket { col: &'a [i64], unit: BucketUnit },
}

impl<'a> DocKeys<'a> {
    fn build(shard: &'a dyn Shard, name: &str) -> anyhow::Result<DocKeys<'a>> {
        if let Some(col) = shard.forward_column(name) {
            Ok(DocKeys::Int(col))
        } else {
            let (doc_term, terms) = term_map(shard, name)?;
            Ok(DocKeys::Str { doc_term, terms })
        }
    }

    /// Build the per-doc key source for a [`GroupKey`] — a plain column (via
    /// [`build`](Self::build)) or a time bucket over the time field's dense
    /// forward column.
    fn for_key(shard: &'a dyn Shard, key: &GroupKey) -> anyhow::Result<DocKeys<'a>> {
        match key {
            GroupKey::Column(name) => DocKeys::build(shard, name),
            GroupKey::TimeBucket { col, unit } => {
                let fc = shard
                    .forward_column(col)
                    .ok_or_else(|| anyhow::anyhow!("no forward column for time field {col:?}"))?;
                Ok(DocKeys::TimeBucket {
                    col: fc,
                    unit: *unit,
                })
            }
        }
    }

    fn key(&self, doc: usize) -> Option<Box<[u8]>> {
        match self {
            DocKeys::Int(col) => Some(Box::from(encode_int_key(col[doc]).as_slice())),
            DocKeys::Str { doc_term, terms } => doc_term[doc].map(|i| terms[i as usize].clone()),
            DocKeys::TimeBucket { col, unit } => Some(Box::from(
                encode_int_key(truncate_epoch(col[doc], *unit)).as_slice(),
            )),
        }
    }
}

/// The bucket-start epoch (UTC seconds) for `secs` under `unit`. Fixed-width
/// units divide on the epoch (day/hour/… align to UTC midnight/top-of-hour,
/// matching `date_trunc`); `week` (Monday) and `month`/`quarter`/`year` use
/// calendar math (the chrono pattern the ingester uses for `ShardBy::Month`).
fn truncate_epoch(secs: i64, unit: BucketUnit) -> i64 {
    let width = match unit {
        BucketUnit::Second => 1,
        BucketUnit::Minute => 60,
        BucketUnit::Hour => 3_600,
        BucketUnit::Day => 86_400,
        BucketUnit::Week | BucketUnit::Month | BucketUnit::Quarter | BucketUnit::Year => {
            let dt = DateTime::from_timestamp(secs, 0).expect("epoch within chrono range");
            let date = dt.date_naive();
            let start = match unit {
                BucketUnit::Week => {
                    date - Duration::days(i64::from(date.weekday().num_days_from_monday()))
                }
                BucketUnit::Month => date.with_day(1).expect("day 1 valid"),
                BucketUnit::Quarter => date
                    .with_day(1)
                    .and_then(|d| d.with_month((date.month0() / 3) * 3 + 1))
                    .expect("quarter-start valid"),
                BucketUnit::Year => date
                    .with_day(1)
                    .and_then(|d| d.with_month(1))
                    .expect("Jan 1 valid"),
                _ => unreachable!("fixed-width units handled above"),
            };
            return start
                .and_hms_opt(0, 0, 0)
                .expect("midnight valid")
                .and_utc()
                .timestamp();
        }
    };
    secs.div_euclid(width) * width
}

/// Global aggregate (no `GROUP BY`): aggregate each shard's whole filtered doc
/// set as one group and combine across shards via the associative
/// [`combine_stats`] fold. Streams shards in bounded batches
/// ([`for_each_shard_batch`]) — the running `acc` is all that survives between
/// batches. `None` means no shard had a matching doc (empty input).
async fn global_stats(
    url: &str,
    cache: &Cache,
    stats: &[OwnedStat],
    filters: &[PushedFilter],
    mem: Option<&Arc<SessionMemoryHandle>>,
    bitmap_cache: Option<&Arc<BitmapCache>>,
    agg_cache: Option<&Arc<AggregateCache>>,
) -> anyhow::Result<Option<Vec<StatValue>>> {
    let stat_specs: Vec<StatSpec> = stats.iter().map(OwnedStat::as_spec).collect();
    let mut acc: Option<Vec<StatValue>> = None;
    let per_doc = per_doc_estimate(0, stat_specs.len());
    for_each_shard_batch(url, cache, filters, mem, bitmap_cache, per_doc, |chunk| {
        for (id, shard, selection) in chunk {
            // No group keys: the shard's partial is a single group (empty tuple)
            // over the matched docs, cached per shard like the grouped paths.
            let partial = cached_partial(agg_cache, *id, filters, &[], stats, || {
                global_shard_partial(shard, selection, &stat_specs)
            })?;
            for (_tuple, st) in partial.iter() {
                match acc.as_mut() {
                    Some(a) => combine_stats(a, st, &stat_specs),
                    None => acc = Some(st.clone()),
                }
            }
        }
        Ok(())
    })
    .await?;
    Ok(acc)
}

/// One shard's global aggregate (no `GROUP BY`): a single empty-tuple group over
/// the matched docs, or empty when nothing matched.
fn global_shard_partial(
    shard: &DiskShard,
    selection: &FilterResult,
    stat_specs: &[StatSpec<'_>],
) -> anyhow::Result<ShardPartial> {
    let num_docs = u32::try_from(shard.num_docs())?;
    let st = match selection {
        FilterResult::Empty => return Ok(Vec::new()),
        FilterResult::All => aggregate_docs(shard, 0..num_docs, stat_specs)?,
        FilterResult::Bitmap(bm) => aggregate_docs(shard, bm.iter(), stat_specs)?,
    };
    Ok(vec![(Vec::new(), st)])
}

/// Union of all of `col`'s postings in `shard` — the docs that carry
/// some term for it. Docs absent from this set have no value, i.e. the
/// SQL NULL group.
fn covered_docs(shard: &dyn Shard, col: &str) -> RoaringBitmap {
    let mut covered = RoaringBitmap::new();
    if let Some(index) = shard.inverted_index(col) {
        for (_term, bitmap) in index.range_bytes(None, None) {
            covered |= bitmap;
        }
    }
    covered
}

/// Build the stat columns (after the `group_cols` group columns) from each
/// output row's finalized stats — dispatched by the stat's [`OutputKind`] and
/// filled in the schema's declared type. Shared by [`reshape_aggregate`] and
/// [`reshape_regrouped`].
fn stat_columns(
    rows: &[&[StatValue]],
    specs: &[StatSpec<'_>],
    schema: &SchemaRef,
    group_cols: usize,
) -> anyhow::Result<Vec<ArrayRef>> {
    let mut arrays = Vec::with_capacity(specs.len());
    for (s, spec) in specs.iter().enumerate() {
        let dtype = schema.field(group_cols + s).data_type();
        let array = match spec.output_kind() {
            // `stat_array` validates `dtype` (Int64/UInt64) itself.
            OutputKind::Int => stat_array(
                dtype,
                rows.iter().map(|st| Some(st[s].finalize().int())).collect(),
            )?,
            OutputKind::Float => float_array(
                rows.iter()
                    .map(|st| Some(st[s].finalize().float()))
                    .collect(),
            ),
            OutputKind::TopK => {
                let DataType::List(item) = dtype else {
                    anyhow::bail!("approx_top_k output column is not a List, got {dtype:?}");
                };
                topk_list_array(item, rows.iter().map(|st| st[s].finalize().topk()))?
            }
            OutputKind::Theta => {
                theta_binary_array(rows.iter().map(|st| Some(st[s].finalize().theta())))
            }
        };
        arrays.push(array);
    }
    Ok(arrays)
}

/// Turn the regrouped path's combined per-tuple stats into one record batch: one
/// row per group-value tuple, the group columns decoded from the tuple in their
/// schema types, then the stats. Group order is the tuple order (`BTreeMap`
/// ascending); any `ORDER BY` is applied by `DataFusion` downstream.
fn reshape_regrouped(
    combined: &BTreeMap<GroupTuple, Vec<StatValue>>,
    num_group_cols: usize,
    specs: &[StatSpec<'_>],
    schema: &SchemaRef,
) -> anyhow::Result<RecordBatch> {
    let entries: Vec<(&GroupTuple, &Vec<StatValue>)> = combined.iter().collect();
    let mut arrays: Vec<ArrayRef> = Vec::with_capacity(num_group_cols + specs.len());

    #[allow(clippy::needless_range_loop)]
    for j in 0..num_group_cols {
        let values: Vec<Option<&[u8]>> = entries
            .iter()
            .map(|(tuple, _)| tuple[j].as_deref())
            .collect();
        arrays.push(group_column(schema.field(j).data_type(), &values));
    }

    let stat_rows: Vec<&[StatValue]> = entries.iter().map(|(_, st)| st.as_slice()).collect();
    arrays.extend(stat_columns(&stat_rows, specs, schema, num_group_cols)?);

    let options = RecordBatchOptions::new().with_row_count(Some(entries.len()));
    Ok(RecordBatch::try_new_with_options(
        Arc::clone(schema),
        arrays,
        &options,
    )?)
}

/// Build a group-key column from FTGS term-key bytes in the column's
/// declared type: `Utf8` renders the term string, `Timestamp` decodes a
/// time-bucket start (epoch seconds → ns), `Int64` decodes the order-preserving
/// key. `None` (a `String` NULL group) is a SQL NULL.
fn group_column(dtype: &DataType, values: &[Option<&[u8]>]) -> ArrayRef {
    match dtype {
        DataType::Utf8 => Arc::new(StringArray::from(
            values
                .iter()
                .map(|v| v.map(|b| render_term(FieldKind::String, b)))
                .collect::<Vec<Option<String>>>(),
        )),
        // Time-bucket group: the key decodes to the bucket-start epoch in
        // seconds; scale to nanoseconds to match the time field's presented
        // `Timestamp(Nanosecond)` type. Dense (the time field is non-null).
        DataType::Timestamp(TimeUnit::Nanosecond, _) => Arc::new(TimestampNanosecondArray::from(
            values
                .iter()
                .map(|v| v.map(|b| decode_int_key(b).saturating_mul(1_000_000_000)))
                .collect::<Vec<Option<i64>>>(),
        )),
        // `Int` group: dense, so a `None` here never occurs.
        _ => Arc::new(Int64Array::from(
            values
                .iter()
                .map(|v| v.map(decode_int_key))
                .collect::<Vec<Option<i64>>>(),
        )),
    }
}

/// Build the `List<Struct<value, count>>` column (`approx_top_k`'s output)
/// from each row's top-k `(value-key bytes, count)` slice. The struct/list
/// `Field`s are taken from the schema's declared `item_field` so the
/// `RecordBatch` matches the aggregate's output type exactly; `value`
/// decodes like a group column (`Utf8` → term, `Int64` → key).
fn topk_list_array<'a>(
    item_field: &FieldRef,
    rows: impl Iterator<Item = &'a [(Box<[u8]>, i64)]>,
) -> anyhow::Result<ArrayRef> {
    let DataType::Struct(fields) = item_field.data_type() else {
        anyhow::bail!("approx_top_k list item is not a Struct");
    };
    let (value_field, count_field) = (&fields[0], &fields[1]);

    let mut lengths: Vec<usize> = Vec::new();
    let mut bytes: Vec<&[u8]> = Vec::new();
    let mut counts: Vec<i64> = Vec::new();
    for items in rows {
        lengths.push(items.len());
        for (b, c) in items {
            bytes.push(b);
            counts.push(*c);
        }
    }

    let value_array: ArrayRef = match value_field.data_type() {
        DataType::Utf8 => Arc::new(StringArray::from(
            bytes
                .iter()
                .map(|b| render_term(FieldKind::String, b))
                .collect::<Vec<String>>(),
        )),
        DataType::Int64 => Arc::new(Int64Array::from(
            bytes
                .iter()
                .map(|b| decode_int_key(b))
                .collect::<Vec<i64>>(),
        )),
        other => anyhow::bail!("approx_top_k value type {other:?} unsupported"),
    };
    let struct_array = StructArray::from(vec![
        (value_field.clone(), value_array),
        (
            count_field.clone(),
            Arc::new(Int64Array::from(counts)) as ArrayRef,
        ),
    ]);
    Ok(Arc::new(ListArray::new(
        item_field.clone(),
        OffsetBuffer::from_lengths(lengths),
        Arc::new(struct_array),
        None,
    )))
}

/// Build the `Binary` column (`theta`'s output) from each row's sketch,
/// `None` → SQL NULL.
fn theta_binary_array<'a>(rows: impl Iterator<Item = Option<&'a ThetaSketch>>) -> ArrayRef {
    let mut b = BinaryBuilder::new();
    for row in rows {
        match row {
            Some(sketch) => b.append_value(sketch.to_bytes()),
            None => b.append_null(),
        }
    }
    Arc::new(b.finish())
}

/// Build a stat column in the type the aggregate's output schema
/// declares, each `None` rendering a SQL NULL. Cardinalities are
/// non-negative, so the `UInt64` widening can't lose a value.
fn stat_array(dtype: &DataType, vals: Vec<Option<i64>>) -> anyhow::Result<ArrayRef> {
    Ok(match dtype {
        DataType::Int64 => Arc::new(Int64Array::from(vals)),
        DataType::UInt64 => Arc::new(UInt64Array::from(
            vals.into_iter()
                .map(|v| v.map(|x| u64::try_from(x).expect("cardinality is non-negative")))
                .collect::<Vec<Option<u64>>>(),
        )),
        other => anyhow::bail!("unsupported aggregate output type {other:?}"),
    })
}

/// A `Float64` stat column — the output type of `AVG` — each `None` a NULL.
fn float_array(vals: Vec<Option<f64>>) -> ArrayRef {
    Arc::new(Float64Array::from(vals))
}

/// The single row of a global aggregate: each stat finalized to its
/// column type, or — when the input was empty (`stats` is `None`) — the
/// SQL empty-input value, which is `0` for `COUNT`/`approx_distinct` and
/// NULL for `SUM`/`MIN`/`MAX`/`AVG`.
fn reshape_global(
    stats: Option<&[StatValue]>,
    owned: &[OwnedStat],
    schema: &SchemaRef,
) -> anyhow::Result<RecordBatch> {
    let mut arrays: Vec<ArrayRef> = Vec::with_capacity(owned.len());
    for (s, spec) in owned.iter().enumerate() {
        let dtype = schema.field(s).data_type();
        let array = match spec.output_kind() {
            OutputKind::Int => {
                let value = match stats {
                    Some(st) => Some(st[s].finalize().int()),
                    // Empty input: `0` for the counts, SQL NULL for the rest
                    // (`Sum`/`Min`/`Max`/`ApproxPercentile`).
                    None => match spec {
                        OwnedStat::Count | OwnedStat::ApproxCountDistinct(_) => Some(0),
                        _ => None,
                    },
                };
                stat_array(dtype, vec![value])?
            }
            // `AVG`: present → sum/count; empty input → NULL.
            OutputKind::Float => float_array(vec![stats.map(|st| st[s].finalize().float())]),
            // `approx_top_k`: one row holding the top-k list (or an empty
            // list when there was no input).
            OutputKind::TopK => {
                let DataType::List(item) = dtype else {
                    anyhow::bail!("approx_top_k output column is not a List, got {dtype:?}");
                };
                let empty: &[(Box<[u8]>, i64)] = &[];
                let row = stats.map_or(empty, |st| st[s].finalize().topk());
                topk_list_array(item, std::iter::once(row))?
            }
            // `theta`: one Binary sketch, or NULL when there was no input.
            OutputKind::Theta => {
                theta_binary_array(std::iter::once(stats.map(|st| st[s].finalize().theta())))
            }
        };
        arrays.push(array);
    }
    let options = RecordBatchOptions::new().with_row_count(Some(1));
    Ok(RecordBatch::try_new_with_options(
        Arc::clone(schema),
        arrays,
        &options,
    )?)
}
