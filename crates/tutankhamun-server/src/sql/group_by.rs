//! `GROUP BY` pushdown: a `DataFusion` optimizer rule that rewrites
//! `Aggregate(TableScan(TutankhamunTableProvider))` into an
//! [`FtgsAggregate`] extension node, planned to an [`FtgsAggExec`] that
//! runs the aggregation through FTGS instead of materializing every row.
//!
//! The rewrite is purely additive: it fires only for the supported shape
//! — zero or more `String`/`Int` grouping columns, aggregates limited to
//! `COUNT(*)` / `SUM` / `MIN` / `MAX` / `AVG` / `approx_distinct`, and a
//! *bare* `TableScan` (so all `WHERE` filters were pushed exactly; an
//! inexact `<`/`>` leaves a `Filter` node and we fall back). Every other
//! query is left untouched for `DataFusion` to aggregate over the row
//! scan, so the pushdown can never change a result, only skip the
//! optimization.
//!
//! Multi-column `GROUP BY` regroups all but the last column into the
//! group lookup (the combo key) and scans the last. `String` columns can
//! be sparse, so [`FtgsAggExec`] adds a NULL pass for docs with no term:
//! a missing prefix value is its own group; a missing cursor value yields
//! a NULL-keyed row per group.

use std::cmp::Ordering;
use std::fmt;
use std::hash::{Hash, Hasher};
use std::sync::Arc;

use async_trait::async_trait;
use datafusion::common::DFSchemaRef;
use datafusion::common::Result as DfResult;
use datafusion::common::tree_node::Transformed;
use datafusion::datasource::source_as_provider;
use datafusion::execution::context::QueryPlanner;
use datafusion::execution::session_state::SessionState;
use datafusion::execution::session_state::SessionStateBuilder;
use datafusion::logical_expr::{
    Aggregate, AggregateUDF, Expr, Extension, LogicalPlan, ScalarUDF, UserDefinedLogicalNode,
    UserDefinedLogicalNodeCore,
};
use datafusion::optimizer::optimizer::ApplyOrder;
use datafusion::optimizer::{OptimizerConfig, OptimizerRule};
use datafusion::physical_plan::ExecutionPlan;
use datafusion::physical_planner::{DefaultPhysicalPlanner, ExtensionPlanner, PhysicalPlanner};
use datafusion::prelude::{SessionConfig, SessionContext};
use datafusion::scalar::ScalarValue;

use super::ftgs_agg::FtgsAggExec;
use super::provider::TutankhamunTableProvider;
use super::pushdown::{PushedFilter, expr_to_pushed_filter};
use crate::cache::Cache;
use crate::ftgs::{OutputKind, StatSpec};
use crate::shard::FieldKind;
use crate::sketches::TDigest;

/// A `SessionContext` with the `GROUP BY` pushdown rule + planner wired
/// in. Falls back to `DataFusion`'s own aggregation for any query the
/// rule doesn't rewrite.
#[must_use]
pub fn session_context() -> SessionContext {
    session_context_with(SessionConfig::new())
}

/// [`session_context`] built over a caller-supplied [`SessionConfig`] — lets
/// callers (and tests) tune knobs like `batch_size`.
#[must_use]
pub fn session_context_with(config: SessionConfig) -> SessionContext {
    let state = SessionStateBuilder::new()
        .with_config(config)
        .with_default_features()
        .with_optimizer_rule(Arc::new(FtgsAggregatePushdown))
        .with_query_planner(Arc::new(FtgsQueryPlanner))
        .build();
    let ctx = SessionContext::new_with_state(state);
    // `approx_top_k` has no `DataFusion` built-in; register it so SQL can
    // parse it (and so it has a row-scan fallback when not pushed). Done on
    // the live context — the builder's `with_aggregate_functions` would
    // *replace* the default aggregates, not add to them.
    ctx.register_udaf(AggregateUDF::from(
        super::approx_top_k::ApproxTopK::default(),
    ));
    // theta (Binary sketch aggregate) + theta_intersect (scalar over two
    // Binary sketches) — also custom, no DataFusion built-in.
    ctx.register_udaf(AggregateUDF::from(super::theta::Theta::default()));
    ctx.register_udf(ScalarUDF::from(super::theta::ThetaIntersect::default()));
    ctx
}

/// An aggregate the rule pushes down — the owned counterpart to
/// [`StatSpec`] (which borrows column names).
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub(crate) enum OwnedStat {
    Count,
    Sum(String),
    Min(String),
    Max(String),
    Avg(String),
    ApproxCountDistinct(String),
    /// `approx_percentile`: column, the quantile as raw `f64` bits (so the
    /// node stays `Eq`/`Ord`/`Hash` for plan dedup), and the digest size.
    ApproxPercentile(String, u64, usize),
    /// `approx_top_k`: column, `k`, and the per-shard merge `capacity`.
    TopK(String, usize, usize),
    /// `theta`: column and the sketch's nominal entries.
    Theta(String, usize),
}

impl OwnedStat {
    pub(crate) fn as_spec(&self) -> StatSpec<'_> {
        match self {
            OwnedStat::Count => StatSpec::Count,
            OwnedStat::Sum(c) => StatSpec::Sum(c),
            OwnedStat::Min(c) => StatSpec::Min(c),
            OwnedStat::Max(c) => StatSpec::Max(c),
            OwnedStat::Avg(c) => StatSpec::Avg(c),
            OwnedStat::ApproxCountDistinct(c) => StatSpec::ApproxCountDistinct(c),
            OwnedStat::ApproxPercentile(c, bits, max_size) => {
                StatSpec::ApproxPercentile(c, f64::from_bits(*bits), *max_size)
            }
            OwnedStat::TopK(c, k, capacity) => StatSpec::TopK(c, *k, *capacity),
            OwnedStat::Theta(c, nominal) => StatSpec::Theta(c, *nominal),
        }
    }

    #[must_use]
    pub(crate) fn output_kind(&self) -> OutputKind {
        self.as_spec().output_kind()
    }
}

/// A `GROUP BY` key the rule pushes down: a bare column, or a time bucket
/// (`date_trunc(unit, <time field>)`) whose value is the bucket-start epoch
/// computed per doc from the time column.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub(crate) enum GroupKey {
    Column(String),
    TimeBucket { col: String, unit: BucketUnit },
}

impl GroupKey {
    /// The underlying stored column name (the time field, for a bucket).
    pub(crate) fn col(&self) -> &str {
        match self {
            GroupKey::Column(c) | GroupKey::TimeBucket { col: c, .. } => c,
        }
    }
}

impl fmt::Display for GroupKey {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            GroupKey::Column(c) => f.write_str(c),
            GroupKey::TimeBucket { col, unit } => write!(f, "date_trunc({unit},{col})"),
        }
    }
}

/// Time-bucket granularity for [`GroupKey::TimeBucket`]. Fixed-width units
/// (`Second`…`Day`) truncate by integer arithmetic on epoch seconds; the rest
/// (`Week`/`Month`/`Quarter`/`Year`) need calendar math (see `truncate_epoch`).
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub(crate) enum BucketUnit {
    Second,
    Minute,
    Hour,
    Day,
    Week,
    Month,
    Quarter,
    Year,
}

impl BucketUnit {
    /// Parse a `date_trunc` unit literal (case-insensitive); `None` for units
    /// we don't support (the query then falls back to `DataFusion`).
    fn parse(s: &str) -> Option<Self> {
        Some(match s.to_ascii_lowercase().as_str() {
            "second" => Self::Second,
            "minute" => Self::Minute,
            "hour" => Self::Hour,
            "day" => Self::Day,
            "week" => Self::Week,
            "month" => Self::Month,
            "quarter" => Self::Quarter,
            "year" => Self::Year,
            _ => return None,
        })
    }
}

impl fmt::Display for BucketUnit {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(match self {
            Self::Second => "second",
            Self::Minute => "minute",
            Self::Hour => "hour",
            Self::Day => "day",
            Self::Week => "week",
            Self::Month => "month",
            Self::Quarter => "quarter",
            Self::Year => "year",
        })
    }
}

/// Logical node standing in for `Aggregate(TableScan)`. Carries
/// everything the physical exec needs. `cache` rides along but is
/// excluded from logical identity (`Eq`/`Ord`/`Hash`): a node is
/// identified by its url, group column, stats, and filters.
#[derive(Clone)]
pub(crate) struct FtgsAggregate {
    url: String,
    cache: Arc<Cache>,
    /// The grouping keys in `GROUP BY` order, or empty for a global
    /// aggregate (no `GROUP BY`) — one row over the whole filtered set.
    group_cols: Vec<GroupKey>,
    stats: Vec<OwnedStat>,
    filters: Vec<PushedFilter>,
    schema: DFSchemaRef,
}

impl FtgsAggregate {
    fn identity(&self) -> (&str, &[GroupKey], &[OwnedStat], &[PushedFilter]) {
        (&self.url, &self.group_cols, &self.stats, &self.filters)
    }
}

impl PartialEq for FtgsAggregate {
    fn eq(&self, other: &Self) -> bool {
        self.identity() == other.identity()
    }
}
impl Eq for FtgsAggregate {}
impl PartialOrd for FtgsAggregate {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}
impl Ord for FtgsAggregate {
    fn cmp(&self, other: &Self) -> Ordering {
        self.identity().cmp(&other.identity())
    }
}
impl Hash for FtgsAggregate {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.identity().hash(state);
    }
}
impl fmt::Debug for FtgsAggregate {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        UserDefinedLogicalNodeCore::fmt_for_explain(self, f)
    }
}

impl UserDefinedLogicalNodeCore for FtgsAggregate {
    // Trait signature ties the return lifetime to `&self`, so the
    // literal can't be `&'static str`.
    #[allow(clippy::unnecessary_literal_bound)]
    fn name(&self) -> &str {
        "FtgsAggregate"
    }

    fn inputs(&self) -> Vec<&LogicalPlan> {
        Vec::new()
    }

    fn schema(&self) -> &DFSchemaRef {
        &self.schema
    }

    fn expressions(&self) -> Vec<Expr> {
        Vec::new()
    }

    fn fmt_for_explain(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let group_by = self
            .group_cols
            .iter()
            .map(GroupKey::to_string)
            .collect::<Vec<_>>()
            .join(",");
        write!(
            f,
            "FtgsAggregate: group_by=[{group_by}], stats={}, filters={}",
            self.stats.len(),
            self.filters.len()
        )
    }

    fn with_exprs_and_inputs(
        &self,
        _exprs: Vec<Expr>,
        _inputs: Vec<LogicalPlan>,
    ) -> DfResult<Self> {
        // Leaf node with no expressions or inputs — nothing to rewrite.
        Ok(self.clone())
    }
}

/// Optimizer rule that rewrites the supported `Aggregate(TableScan)`
/// shape into [`FtgsAggregate`].
#[derive(Debug)]
struct FtgsAggregatePushdown;

impl OptimizerRule for FtgsAggregatePushdown {
    #[allow(clippy::unnecessary_literal_bound)]
    fn name(&self) -> &str {
        "ftgs_aggregate_pushdown"
    }

    fn apply_order(&self) -> Option<ApplyOrder> {
        // Bottom-up so the input `TableScan` already carries its pushed
        // filters by the time we see the `Aggregate`.
        Some(ApplyOrder::BottomUp)
    }

    fn rewrite(
        &self,
        plan: LogicalPlan,
        _config: &dyn OptimizerConfig,
    ) -> DfResult<Transformed<LogicalPlan>> {
        if let LogicalPlan::Aggregate(agg) = &plan
            && let Some(node) = try_build(agg)?
        {
            return Ok(Transformed::yes(LogicalPlan::Extension(Extension {
                node: Arc::new(node),
            })));
        }
        Ok(Transformed::no(plan))
    }
}

/// Build an [`FtgsAggregate`] from an `Aggregate` if it matches the
/// supported shape, else `None` (the query falls back to `DataFusion`).
fn try_build(agg: &Aggregate) -> DfResult<Option<FtgsAggregate>> {
    // Input must be a *bare* TableScan of our provider. A `Filter` above
    // the scan means inexact filters remain to be re-applied, so we
    // don't push (the FTGS scan can't re-check them).
    let LogicalPlan::TableScan(scan) = agg.input.as_ref() else {
        return Ok(None);
    };
    let provider = source_as_provider(&scan.source)?;
    let Some(prov) = provider.as_any().downcast_ref::<TutankhamunTableProvider>() else {
        return Ok(None);
    };

    // No grouping key → a global aggregate (one row over the whole filtered
    // set). Otherwise each key is a bare filterable (`String`/`Int`) column or
    // a `date_trunc(unit, <time field>)` time bucket. A bare column is
    // index-walked/regrouped; a time bucket is always regrouped (the time
    // field's index is per-second, not per-bucket) — see `aggregate_batch`.
    let mut group_cols = Vec::with_capacity(agg.group_expr.len());
    for expr in &agg.group_expr {
        match group_key(expr, prov) {
            Some(key) => group_cols.push(key),
            None => return Ok(None),
        }
    }

    // Every aggregate must map to a supported scalar stat.
    let mut stats = Vec::with_capacity(agg.aggr_expr.len());
    for expr in &agg.aggr_expr {
        match aggregate_to_stat(expr, prov) {
            Some(stat) => stats.push(stat),
            None => return Ok(None),
        }
    }
    if stats.is_empty() {
        // GROUP BY with no aggregate (e.g. SELECT DISTINCT) — leave it.
        return Ok(None);
    }

    // Bare TableScan ⇒ its filters were all pushed exactly.
    let mut filters = Vec::with_capacity(scan.filters.len());
    for expr in &scan.filters {
        match expr_to_pushed_filter(expr) {
            Some(pf) => filters.push(pf),
            None => return Ok(None),
        }
    }

    Ok(Some(FtgsAggregate {
        url: prov.url().to_string(),
        cache: Arc::clone(prov.cache()),
        group_cols,
        stats,
        filters,
        schema: Arc::clone(&agg.schema),
    }))
}

/// Map one `GROUP BY` expression to a pushable [`GroupKey`], or `None`.
fn group_key(expr: &Expr, prov: &TutankhamunTableProvider) -> Option<GroupKey> {
    match expr {
        Expr::Column(col) => matches!(
            prov.field_kind(&col.name),
            Some(FieldKind::String | FieldKind::Int)
        )
        .then(|| GroupKey::Column(col.name.clone())),
        // `date_trunc(unit, <time field>)` → a time bucket. The source column
        // must be the dataset's time field (the one presented as Timestamp).
        Expr::ScalarFunction(f) if f.func.name().eq_ignore_ascii_case("date_trunc") => {
            let [unit_expr, Expr::Column(col)] = f.args.as_slice() else {
                return None;
            };
            let unit = match unit_expr {
                Expr::Literal(
                    ScalarValue::Utf8(Some(u))
                    | ScalarValue::LargeUtf8(Some(u))
                    | ScalarValue::Utf8View(Some(u)),
                    _,
                ) => BucketUnit::parse(u)?,
                _ => return None,
            };
            prov.is_time_field(&col.name).then(|| GroupKey::TimeBucket {
                col: col.name.clone(),
                unit,
            })
        }
        _ => None,
    }
}

/// Map one aggregate expression to a pushable [`OwnedStat`], or `None`.
// One arm per supported aggregate name — long but flat.
#[allow(clippy::too_many_lines)]
fn aggregate_to_stat(expr: &Expr, prov: &TutankhamunTableProvider) -> Option<OwnedStat> {
    let Expr::AggregateFunction(af) = expr else {
        return None;
    };
    // No DISTINCT / FILTER / ORDER BY aggregates.
    if af.params.distinct || af.params.filter.is_some() || !af.params.order_by.is_empty() {
        return None;
    }
    let args = &af.params.args;
    match af.func.name().to_ascii_lowercase().as_str() {
        // COUNT(*) only — args are empty or a literal. COUNT(col) (which
        // skips NULLs) isn't equivalent to FTGS's per-group doc count.
        "count" if args.iter().all(|a| matches!(a, Expr::Literal(..))) => Some(OwnedStat::Count),
        // `sum`/`min`/`max`/`avg` read one bare `Metric`/`Int` forward
        // column; `approx_distinct` also takes a `String`, hashing its terms.
        name @ ("sum" | "min" | "max" | "avg" | "approx_distinct") => {
            let [arg] = args.as_slice() else {
                return None;
            };
            // `avg(int)` arrives as `avg(CAST(col AS Float64))`; peel the
            // cast DataFusion inserts so the i64 column underneath is summed.
            // Only for `avg` — keeping the others bare-column-only preserves
            // `Float64 output <=> AVG` for the reshape's type dispatch.
            let col_expr = match arg {
                Expr::Cast(cast) if name == "avg" => cast.expr.as_ref(),
                other => other,
            };
            let Expr::Column(c) = col_expr else {
                return None;
            };
            let kind = prov.field_kind(&c.name)?;
            let ok = match name {
                "approx_distinct" => {
                    matches!(kind, FieldKind::Metric | FieldKind::Int | FieldKind::String)
                }
                _ => matches!(kind, FieldKind::Metric | FieldKind::Int),
            };
            if !ok {
                return None;
            }
            let col = c.name.clone();
            Some(match name {
                "sum" => OwnedStat::Sum(col),
                "min" => OwnedStat::Min(col),
                "max" => OwnedStat::Max(col),
                "avg" => OwnedStat::Avg(col),
                _ => OwnedStat::ApproxCountDistinct(col),
            })
        }
        // approx_percentile_cont(col, p [, centroids]) — a t-digest quantile
        // over a `Metric`/`Int` column. The column is passed bare (no cast).
        "approx_percentile_cont" => {
            let [Expr::Column(c), p_expr, rest @ ..] = args.as_slice() else {
                return None;
            };
            if !matches!(
                prov.field_kind(&c.name)?,
                FieldKind::Metric | FieldKind::Int
            ) {
                return None;
            }
            let percentile = match p_expr {
                Expr::Literal(ScalarValue::Float64(Some(p)), _) => *p,
                Expr::Literal(ScalarValue::Float32(Some(p)), _) => f64::from(*p),
                _ => return None,
            };
            if !(0.0..=1.0).contains(&percentile) {
                return None;
            }
            // Optional third arg: t-digest size (centroids).
            let max_size = match rest {
                [] => TDigest::DEFAULT_MAX_SIZE,
                [e] => lit_usize(e)?,
                _ => return None,
            };
            Some(OwnedStat::ApproxPercentile(
                c.name.clone(),
                percentile.to_bits(),
                max_size,
            ))
        }
        // approx_top_k(col, k [, capacity]) — the k most frequent values of
        // a column. Counts per-doc, so any kind (String/Int/Metric) works.
        "approx_top_k" => {
            let [col_expr, k_expr, rest @ ..] = args.as_slice() else {
                return None;
            };
            let Expr::Column(c) = col_expr else {
                return None;
            };
            if !matches!(
                prov.field_kind(&c.name)?,
                FieldKind::String | FieldKind::Int | FieldKind::Metric
            ) {
                return None;
            }
            let k = lit_usize(k_expr)?;
            // `capacity` is at least `k` — you can't keep fewer candidates
            // than the result needs.
            let capacity = match rest {
                [] => super::approx_top_k::default_capacity(k),
                [e] => lit_usize(e)?,
                _ => return None,
            }
            .max(k);
            Some(OwnedStat::TopK(c.name.clone(), k, capacity))
        }
        // theta(col [, nominal]) — a KMV distinct-count sketch (Binary).
        // Counts per-doc, so any kind (String/Int/Metric) works.
        "theta" => {
            let [col_expr, rest @ ..] = args.as_slice() else {
                return None;
            };
            let Expr::Column(c) = col_expr else {
                return None;
            };
            if !matches!(
                prov.field_kind(&c.name)?,
                FieldKind::String | FieldKind::Int | FieldKind::Metric
            ) {
                return None;
            }
            let nominal = match rest {
                [] => crate::sketches::ThetaSketch::DEFAULT_NOMINAL,
                [e] => lit_usize(e)?.next_power_of_two(),
                _ => return None,
            };
            Some(OwnedStat::Theta(c.name.clone(), nominal))
        }
        _ => None,
    }
}

/// A positive `Int64` literal as a `usize` (`DataFusion` renders an
/// unsuffixed integer literal as `Int64`).
fn lit_usize(expr: &Expr) -> Option<usize> {
    match expr {
        Expr::Literal(ScalarValue::Int64(Some(n)), _) if *n > 0 => usize::try_from(*n).ok(),
        _ => None,
    }
}

/// Plans [`FtgsAggregate`] → [`FtgsAggExec`].
#[derive(Debug)]
struct FtgsAggregatePlanner;

#[async_trait]
impl ExtensionPlanner for FtgsAggregatePlanner {
    async fn plan_extension(
        &self,
        _planner: &dyn PhysicalPlanner,
        node: &dyn UserDefinedLogicalNode,
        _logical_inputs: &[&LogicalPlan],
        _physical_inputs: &[Arc<dyn ExecutionPlan>],
        _session_state: &SessionState,
    ) -> DfResult<Option<Arc<dyn ExecutionPlan>>> {
        let Some(node) = node.as_any().downcast_ref::<FtgsAggregate>() else {
            return Ok(None);
        };
        let exec = FtgsAggExec::new(
            node.url.clone(),
            Arc::clone(&node.cache),
            node.group_cols.clone(),
            node.stats.clone(),
            node.filters.clone(),
            Arc::clone(node.schema.inner()),
        );
        Ok(Some(Arc::new(exec)))
    }
}

/// Wraps the default physical planner with our [`FtgsAggregatePlanner`]
/// so `Extension(FtgsAggregate)` nodes get a physical plan.
#[derive(Debug)]
struct FtgsQueryPlanner;

#[async_trait]
impl QueryPlanner for FtgsQueryPlanner {
    async fn create_physical_plan(
        &self,
        logical_plan: &LogicalPlan,
        session_state: &SessionState,
    ) -> DfResult<Arc<dyn ExecutionPlan>> {
        let planner =
            DefaultPhysicalPlanner::with_extension_planners(vec![Arc::new(FtgsAggregatePlanner)]);
        planner
            .create_physical_plan(logical_plan, session_state)
            .await
    }
}
