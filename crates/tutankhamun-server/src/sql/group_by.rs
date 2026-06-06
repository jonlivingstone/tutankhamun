//! Single-column `GROUP BY` pushdown: a `DataFusion` optimizer rule
//! that rewrites `Aggregate(TableScan(TutankhamunTableProvider))` into an
//! [`FtgsAggregate`] extension node, planned to an [`FtgsAggExec`] that
//! runs the aggregation through FTGS instead of materializing every row.
//!
//! The rewrite is purely additive: it fires only for the exactly
//! supported shape — a single `Int` grouping column, aggregates limited
//! to `COUNT(*)` / `SUM` / `MIN` / `MAX` over `Metric`/`Int` columns, and
//! a *bare* `TableScan` (so all `WHERE` filters were pushed exactly; an
//! inexact `<`/`>` leaves a `Filter` node and we fall back). Every other
//! query is left untouched for `DataFusion` to aggregate over the row
//! scan, so the pushdown can never change a result, only skip the
//! optimization.
//!
//! A single `Int` or `String` grouping column. `Int`/`Metric` forward
//! columns are dense, so an `Int` group has no NULL group; a `String`
//! column can be sparse, so [`FtgsAggExec`] adds a NULL-group pass that
//! aggregates the docs with no term into a NULL-keyed row.

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
    Aggregate, Expr, Extension, LogicalPlan, UserDefinedLogicalNode, UserDefinedLogicalNodeCore,
};
use datafusion::optimizer::optimizer::ApplyOrder;
use datafusion::optimizer::{OptimizerConfig, OptimizerRule};
use datafusion::physical_plan::ExecutionPlan;
use datafusion::physical_planner::{DefaultPhysicalPlanner, ExtensionPlanner, PhysicalPlanner};
use datafusion::prelude::SessionContext;

use super::exec::FtgsAggExec;
use super::provider::TutankhamunTableProvider;
use super::pushdown::{PushedFilter, expr_to_pushed_filter};
use crate::cache::Cache;
use crate::ftgs::StatSpec;
use crate::shard::FieldKind;

/// A `SessionContext` with the `GROUP BY` pushdown rule + planner wired
/// in. Falls back to `DataFusion`'s own aggregation for any query the
/// rule doesn't rewrite.
#[must_use]
pub fn session_context() -> SessionContext {
    let state = SessionStateBuilder::new()
        .with_default_features()
        .with_optimizer_rule(Arc::new(FtgsAggregatePushdown))
        .with_query_planner(Arc::new(FtgsQueryPlanner))
        .build();
    SessionContext::new_with_state(state)
}

/// An aggregate the rule pushes down — the owned counterpart to
/// [`StatSpec`] (which borrows column names).
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub(crate) enum OwnedStat {
    Count,
    Sum(String),
    Min(String),
    Max(String),
    ApproxCountDistinct(String),
}

impl OwnedStat {
    pub(crate) fn as_spec(&self) -> StatSpec<'_> {
        match self {
            OwnedStat::Count => StatSpec::Count,
            OwnedStat::Sum(c) => StatSpec::Sum(c),
            OwnedStat::Min(c) => StatSpec::Min(c),
            OwnedStat::Max(c) => StatSpec::Max(c),
            OwnedStat::ApproxCountDistinct(c) => StatSpec::ApproxCountDistinct(c),
        }
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
    /// The single grouping column, or `None` for a global aggregate
    /// (no `GROUP BY`) — one row over the whole filtered set.
    group_col: Option<String>,
    stats: Vec<OwnedStat>,
    filters: Vec<PushedFilter>,
    schema: DFSchemaRef,
}

impl FtgsAggregate {
    fn identity(&self) -> (&str, Option<&str>, &[OwnedStat], &[PushedFilter]) {
        (
            &self.url,
            self.group_col.as_deref(),
            &self.stats,
            &self.filters,
        )
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
        write!(
            f,
            "FtgsAggregate: group_by=[{}], stats={}, filters={}",
            self.group_col.as_deref().unwrap_or(""),
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

    // No grouping column → a global aggregate (one row over the whole
    // filtered set). Exactly one column → grouped; it must be filterable
    // (`String`/`Int`, i.e. carries the inverted index the cursor walks).
    // Two or more columns fall back (needs regroups).
    let group_col = match agg.group_expr.as_slice() {
        [] => None,
        [Expr::Column(col)] => {
            if !matches!(
                prov.field_kind(&col.name),
                Some(FieldKind::String | FieldKind::Int)
            ) {
                return Ok(None);
            }
            Some(col.name.clone())
        }
        _ => return Ok(None),
    };

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
        group_col,
        stats,
        filters,
        schema: Arc::clone(&agg.schema),
    }))
}

/// Map one aggregate expression to a pushable [`OwnedStat`], or `None`.
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
        // `sum`/`min`/`max` read one bare `Metric`/`Int` forward column;
        // `approx_distinct` also takes a `String`, hashing its index terms.
        name @ ("sum" | "min" | "max" | "approx_distinct") => {
            let [Expr::Column(c)] = args.as_slice() else {
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
                _ => OwnedStat::ApproxCountDistinct(col),
            })
        }
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
            node.group_col.clone(),
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
