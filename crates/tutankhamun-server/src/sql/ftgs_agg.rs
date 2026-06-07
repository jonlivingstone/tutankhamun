//! [`FtgsAggExec`] — the `DataFusion` `ExecutionPlan` for a pushed-down
//! aggregate. Runs the aggregation through FTGS per shard, merges across
//! shards, and reshapes the merged stats into one Arrow record batch (the
//! group columns followed by one column per stat) instead of materialising
//! every row. The row-scan exec and the shared scan prelude
//! ([`block_on_scan`](super::exec::block_on_scan),
//! [`fetch_selected_shards`](super::exec::fetch_selected_shards),
//! [`chunked_stream`](super::exec::chunked_stream)) live in [`super::exec`].

use std::any::Any;
use std::collections::BTreeMap;
use std::fmt;
use std::sync::Arc;

use arrow::array::{
    ArrayRef, BinaryBuilder, Float64Array, Int64Array, ListArray, RecordBatch, RecordBatchOptions,
    StringArray, StructArray, UInt64Array,
};
use arrow::buffer::OffsetBuffer;
use arrow::datatypes::{DataType, FieldRef, SchemaRef};
use datafusion::common::Result as DfResult;
use datafusion::execution::{SendableRecordBatchStream, TaskContext};
use datafusion::physical_expr::EquivalenceProperties;
use datafusion::physical_plan::execution_plan::{Boundedness, EmissionType};
use datafusion::physical_plan::{
    DisplayAs, DisplayFormatType, ExecutionPlan, Partitioning, PlanProperties,
};

use roaring::RoaringBitmap;

use super::group_by::OwnedStat;
use super::pushdown::PushedFilter;
use super::scan::{block_on_scan, chunked_stream, fetch_selected_shards};
use crate::cache::Cache;
use crate::ftgs::{
    FtgsRow, OutputKind, StatSpec, StatValue, aggregate_docs, aggregate_docs_grouped,
    combine_stats, ftgs_scan_merge, render_term, term_map,
};
use crate::group_lookup::GroupLookup;
use crate::shard::{DiskShard, FieldKind, FilterResult, Shard, decode_int_key, encode_int_key};
use crate::sketches::ThetaSketch;

/// `ExecutionPlan` for a pushed-down aggregate (§3.2). Runs the
/// aggregation through FTGS per shard and merges, emitting one
/// pre-aggregated record batch instead of materializing every row.
/// `group_col` is the single grouping column, or `None` for a global
/// aggregate (no `GROUP BY` — one row over the whole filtered set).
#[derive(Debug)]
pub(crate) struct FtgsAggExec {
    url: String,
    cache: Arc<Cache>,
    group_cols: Vec<String>,
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
        group_cols: Vec<String>,
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
            self.group_cols.join(",")
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
        let batch = block_on_scan(|| {
            aggregate_batch(
                &self.url,
                &self.cache,
                &self.group_cols,
                &self.stats,
                &self.filters,
                &self.schema,
            )
        })?;
        // One row per group; cap every emitted batch to the session's
        // configured row count for streaming.
        chunked_stream(vec![batch], Arc::clone(&self.schema), &context)
    }
}

async fn aggregate_batch(
    url: &str,
    cache: &Cache,
    group_cols: &[String],
    stats: &[OwnedStat],
    filters: &[PushedFilter],
    schema: &SchemaRef,
) -> anyhow::Result<RecordBatch> {
    let shards = fetch_selected_shards(url, cache, filters).await?;
    let stat_specs: Vec<StatSpec> = stats.iter().map(OwnedStat::as_spec).collect();
    if group_cols.is_empty() {
        reshape_global(
            global_stats(&shards, &stat_specs)?.as_deref(),
            stats,
            schema,
        )
    } else {
        aggregate_grouped(&shards, group_cols, &stat_specs, schema)
    }
}

/// `GROUP BY g1..gk`: regroup the prefix `g1..g(k-1)` into each shard's
/// group lookup as a combo id, scan the last column `gk` with FTGS, and
/// reshape to one row per `(prefix tuple, cursor term)`. A `String` cursor
/// can leave docs term-less — those form a per-group NULL-keyed row.
fn aggregate_grouped(
    shards: &[(DiskShard, FilterResult)],
    group_cols: &[String],
    stat_specs: &[StatSpec],
    schema: &SchemaRef,
) -> anyhow::Result<RecordBatch> {
    let (cursor, prefix) = group_cols
        .split_last()
        .expect("grouped path has at least one column");
    // The cursor column is output column `prefix.len()`. A `String` cursor
    // can be sparse (the SQL NULL group); `Int`/`Metric` are dense.
    let cursor_is_string = matches!(schema.field(prefix.len()).data_type(), DataType::Utf8);

    // Combo ids are assigned from one map shared across all shards, so a
    // given prefix tuple maps to the same group id everywhere — the
    // invariant `ftgs_scan_merge` needs to fold rows across shards.
    let mut combo = ComboMap::default();
    let mut pairs: Vec<(&DiskShard, GroupLookup)> = Vec::with_capacity(shards.len());
    // NULL-cursor docs, aggregated per combo group, merged across shards.
    let mut null_stats: BTreeMap<u32, Vec<StatValue>> = BTreeMap::new();
    for (shard, selection) in shards {
        let num_docs = usize::try_from(shard.num_docs())?;
        let matched: Option<&RoaringBitmap> = match selection {
            FilterResult::Empty => continue,
            FilterResult::All => None,
            FilterResult::Bitmap(bm) => Some(bm),
        };

        let groups = if prefix.is_empty() {
            // Single column: the lookup carries only WHERE membership
            // (matched → group 1), so the unfiltered case stays O(1).
            match matched {
                None => GroupLookup::all_in_one_group(num_docs),
                Some(bm) => {
                    let mut g = GroupLookup::constant(num_docs, 0);
                    for doc in bm {
                        g.set(doc as usize, 1);
                    }
                    g
                }
            }
        } else {
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
            g
        };

        if cursor_is_string {
            let covered = covered_docs(shard, cursor);
            let null_docs = match matched {
                None => {
                    let mut all = RoaringBitmap::new();
                    all.insert_range(0..u32::try_from(num_docs)?);
                    all - covered
                }
                Some(bm) => bm.clone() - covered,
            };
            for (gid, st) in aggregate_docs_grouped(shard, null_docs.iter(), &groups, stat_specs)? {
                match null_stats.get_mut(&gid) {
                    Some(acc) => combine_stats(acc, &st, stat_specs),
                    None => {
                        null_stats.insert(gid, st);
                    }
                }
            }
        }

        pairs.push((shard, groups));
    }

    let refs: Vec<(&dyn Shard, &GroupLookup)> =
        pairs.iter().map(|(s, g)| (*s as &dyn Shard, g)).collect();
    let rows = ftgs_scan_merge(&refs, &[cursor.as_str()], stat_specs)?;
    reshape_aggregate(
        &rows,
        prefix.len(),
        stat_specs,
        schema,
        &combo.inverse,
        &null_stats,
    )
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

    fn key(&self, doc: usize) -> Option<Box<[u8]>> {
        match self {
            DocKeys::Int(col) => Some(Box::from(encode_int_key(col[doc]).as_slice())),
            DocKeys::Str { doc_term, terms } => doc_term[doc].map(|i| terms[i as usize].clone()),
        }
    }
}

/// Global aggregate (no `GROUP BY`): aggregate each shard's whole
/// filtered doc set as one group and combine across shards. `None` means
/// no shard had a matching doc (empty input).
fn global_stats(
    shards: &[(DiskShard, FilterResult)],
    stat_specs: &[StatSpec],
) -> anyhow::Result<Option<Vec<StatValue>>> {
    let mut acc: Option<Vec<StatValue>> = None;
    for (shard, selection) in shards {
        let num_docs = u32::try_from(shard.num_docs())?;
        let shard_stats = match selection {
            FilterResult::Empty => continue,
            FilterResult::All => aggregate_docs(shard, 0..num_docs, stat_specs)?,
            FilterResult::Bitmap(bm) => aggregate_docs(shard, bm.iter(), stat_specs)?,
        };
        match acc.as_mut() {
            Some(a) => combine_stats(a, &shard_stats, stat_specs),
            None => acc = Some(shard_stats),
        }
    }
    Ok(acc)
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

/// Turn merged FTGS rows into one record batch. The first `num_prefix`
/// columns are the regrouped prefix values, decoded from each row's combo
/// group via `inverse`; column `num_prefix` is the cursor term (`None` for
/// a `String` cursor's NULL group); the rest are the stats. `null_stats`
/// holds the NULL-cursor docs, one entry per combo group.
fn reshape_aggregate(
    rows: &[FtgsRow],
    num_prefix: usize,
    specs: &[StatSpec],
    schema: &SchemaRef,
    inverse: &[Vec<Option<Box<[u8]>>>],
    null_stats: &BTreeMap<u32, Vec<StatValue>>,
) -> anyhow::Result<RecordBatch> {
    // One output row per scanned `(cursor term, group)`, plus one per
    // NULL-cursor combo group. `group` decodes to the prefix tuple.
    struct OutRow<'a> {
        group: u32,
        cursor: Option<&'a [u8]>,
        stats: &'a [StatValue],
    }
    let out: Vec<OutRow> = rows
        .iter()
        .map(|r| OutRow {
            group: r.group,
            cursor: Some(&r.term),
            stats: &r.stats,
        })
        .chain(null_stats.iter().map(|(&gid, st)| OutRow {
            group: gid,
            cursor: None,
            stats: st,
        }))
        .collect();

    let num_group_cols = num_prefix + 1;
    let mut arrays: Vec<ArrayRef> = Vec::with_capacity(num_group_cols + specs.len());

    // Group columns: prefix j from `inverse[group - 1][j]`, the cursor
    // (last group column) from the row's own term. `j` indexes the schema,
    // the prefix/cursor split, and the inverse tuple — not a single slice.
    #[allow(clippy::needless_range_loop)]
    for j in 0..num_group_cols {
        let values: Vec<Option<&[u8]>> = out
            .iter()
            .map(|o| {
                if j == num_prefix {
                    o.cursor
                } else {
                    inverse[(o.group - 1) as usize][j].as_deref()
                }
            })
            .collect();
        arrays.push(group_column(schema.field(j).data_type(), &values));
    }

    // Stat columns follow the group columns. The output column is keyed by
    // the stat's own [`OutputKind`], not the Arrow dtype — `dtype` only
    // supplies the concrete array type the builder fills (Int64 vs UInt64,
    // or the `List` item field for top-k).
    for (s, spec) in specs.iter().enumerate() {
        let dtype = schema.field(num_group_cols + s).data_type();
        let array = match spec.output_kind() {
            // `stat_array` validates `dtype` (Int64/UInt64) itself.
            OutputKind::Int => stat_array(
                dtype,
                out.iter()
                    .map(|o| Some(o.stats[s].finalize().int()))
                    .collect(),
            )?,
            OutputKind::Float => float_array(
                out.iter()
                    .map(|o| Some(o.stats[s].finalize().float()))
                    .collect(),
            ),
            OutputKind::TopK => {
                let DataType::List(item) = dtype else {
                    anyhow::bail!("approx_top_k output column is not a List, got {dtype:?}");
                };
                topk_list_array(item, out.iter().map(|o| o.stats[s].finalize().topk()))?
            }
            OutputKind::Theta => {
                theta_binary_array(out.iter().map(|o| Some(o.stats[s].finalize().theta())))
            }
        };
        arrays.push(array);
    }

    let options = RecordBatchOptions::new().with_row_count(Some(out.len()));
    Ok(RecordBatch::try_new_with_options(
        Arc::clone(schema),
        arrays,
        &options,
    )?)
}

/// Build a group-key column from FTGS term-key bytes in the column's
/// declared type: `Utf8` renders the term string, `Int64` decodes the
/// order-preserving key. `None` (a `String` NULL group) is a SQL NULL.
fn group_column(dtype: &DataType, values: &[Option<&[u8]>]) -> ArrayRef {
    match dtype {
        DataType::Utf8 => Arc::new(StringArray::from(
            values
                .iter()
                .map(|v| v.map(|b| render_term(FieldKind::String, b)))
                .collect::<Vec<Option<String>>>(),
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
