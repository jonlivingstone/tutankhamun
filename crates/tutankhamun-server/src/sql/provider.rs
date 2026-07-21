//! [`TutankhamunTableProvider`] — adapts a Tutankhamun dataset to
//! `DataFusion`'s [`TableProvider`] trait.

use std::any::Any;
use std::sync::Arc;

use anyhow::Context as _;
use arrow::datatypes::{DataType, Field, Schema, SchemaRef, TimeUnit};
use async_trait::async_trait;
use datafusion::catalog::{Session, TableProvider};
use datafusion::common::Result as DfResult;
use datafusion::common::error::DataFusionError;
use datafusion::logical_expr::{Expr, TableProviderFilterPushDown, TableType};
use datafusion::physical_plan::ExecutionPlan;
use object_store::ObjectStore;

use super::exec::TutankhamunExec;
use super::pushdown;
use crate::cache::Cache;
use crate::shard::{DatasetSchema, FieldKind};
use crate::shard_source::{ObjectStoreShardSource, ShardSource, ShardSummary};
use crate::storage::StorageRegistry;

/// `DataFusion` `TableProvider` over a single Tutankhamun dataset.
///
/// One instance corresponds to one URL (one logical table). The
/// schema is derived at construction from the first discovered
/// shard's metadata (every shard in a dataset shares a schema).
#[derive(Debug)]
pub struct TutankhamunTableProvider {
    url: String,
    cache: Arc<Cache>,
    schema: SchemaRef,
    /// Field name → kind, from the dataset's metadata. The Arrow
    /// `schema` collapses `Metric`/`Int` to `Int64`, so the aggregate
    /// pushdown rule consults this to tell a filterable `Int` group
    /// column from a non-filterable `Metric`.
    field_kinds: std::collections::BTreeMap<String, FieldKind>,
    /// The dataset's shard set, resolved once at construction (cached by the
    /// daemon, freshly discovered for one-shot CLI use). Handed to the execs so
    /// the scan reuses it instead of re-discovering.
    summaries: Arc<Vec<ShardSummary>>,
}

impl TutankhamunTableProvider {
    pub(crate) fn url(&self) -> &str {
        &self.url
    }

    pub(crate) fn cache(&self) -> &Arc<Cache> {
        &self.cache
    }

    /// Kind of `field` as declared in the dataset metadata, or `None`
    /// if the dataset has no such field.
    pub(crate) fn field_kind(&self, field: &str) -> Option<FieldKind> {
        self.field_kinds.get(field).copied()
    }

    /// Whether `field` is the dataset's time field — the one column the
    /// schema presents as `Timestamp` (stored as epoch-seconds `Int`). Used
    /// by the aggregate pushdown to validate `date_trunc(unit, <time>)`.
    pub(crate) fn is_time_field(&self, field: &str) -> bool {
        self.schema
            .field_with_name(field)
            .is_ok_and(|f| matches!(f.data_type(), DataType::Timestamp(_, _)))
    }
}

impl TutankhamunTableProvider {
    /// Discover the dataset under `url` (one-shot, no cache) and build a
    /// provider. The CLI path: it walks the backend for shards directly. The
    /// daemon never calls this — it walks once and caches per dataset
    /// (`ServiceInner::resolve_summaries`), building via [`Self::from_summaries`].
    pub async fn try_new(url: String, cache: Arc<Cache>) -> anyhow::Result<Self> {
        let registry = StorageRegistry::from_url(&url)?;
        let store = registry.store();
        let source = ObjectStoreShardSource::new(Arc::clone(&store));
        let summaries = source
            .discover()
            .await
            .with_context(|| format!("discover shards at {url}"))?;
        let schema = resolve_dataset_schema(&*store, &summaries).await?;
        Self::from_summaries(url, cache, Arc::new(summaries), &schema)
    }

    /// Build a provider from an already-resolved shard set and the dataset schema
    /// — no I/O. The Arrow schema and field kinds come from `schema` (the
    /// authoritative `schema.json` when present, else inferred from the shards).
    /// Each shard is checked to structurally match (a diverging shard would
    /// mis-project). Errors if the dataset is empty — `DataFusion` has no schema
    /// to register without a shard.
    pub(crate) fn from_summaries(
        url: String,
        cache: Arc<Cache>,
        summaries: Arc<Vec<ShardSummary>>,
        schema: &DatasetSchema,
    ) -> anyhow::Result<Self> {
        if summaries.is_empty() {
            anyhow::bail!("no shards at {url}");
        }
        for s in summaries.iter() {
            if !schema.matches(&s.metadata.fields) {
                anyhow::bail!(
                    "shard {} at {url} does not match the dataset schema (a column's \
                     name/kind/scale differs)",
                    s.location,
                );
            }
        }
        let arrow = arrow_schema(schema);
        let field_kinds = schema
            .fields
            .iter()
            .map(|f| (f.name.clone(), f.kind))
            .collect();
        Ok(Self {
            url,
            cache,
            schema: arrow,
            field_kinds,
            summaries,
        })
    }

    /// The dataset's resolved shard set, shared with the execs this provider
    /// builds so the scan reuses it instead of re-discovering.
    pub(crate) fn summaries(&self) -> Arc<Vec<ShardSummary>> {
        Arc::clone(&self.summaries)
    }
}

#[async_trait]
impl TableProvider for TutankhamunTableProvider {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn schema(&self) -> SchemaRef {
        Arc::clone(&self.schema)
    }

    fn table_type(&self) -> TableType {
        TableType::Base
    }

    async fn scan(
        &self,
        _state: &dyn Session,
        projection: Option<&Vec<usize>>,
        filters: &[Expr],
        _limit: Option<usize>,
    ) -> DfResult<Arc<dyn ExecutionPlan>> {
        let pushed = filters
            .iter()
            .filter_map(pushdown::expr_to_pushed_filter)
            .collect::<Vec<_>>();
        let projected_schema = match projection {
            Some(indices) => Arc::new(
                self.schema
                    .project(indices)
                    .map_err(DataFusionError::from)?,
            ),
            None => Arc::clone(&self.schema),
        };
        let exec = TutankhamunExec::new(
            self.url.clone(),
            Arc::clone(&self.cache),
            projected_schema,
            pushed,
            self.summaries(),
        );
        Ok(Arc::new(exec))
    }

    fn supports_filters_pushdown(
        &self,
        filters: &[&Expr],
    ) -> DfResult<Vec<TableProviderFilterPushDown>> {
        Ok(filters
            .iter()
            .map(|expr| match pushdown::expr_to_pushed_filter(expr) {
                Some(pf) if pf.exact => TableProviderFilterPushDown::Exact,
                // Widened (strict `<`/`>`) filters still prune at scan
                // time, but `DataFusion` must re-apply the original
                // predicate to drop the boundary rows we over-return.
                Some(_) => TableProviderFilterPushDown::Inexact,
                None => TableProviderFilterPushDown::Unsupported,
            })
            .collect())
    }
}

/// Resolve the dataset schema the reader projects against. The shards' actual
/// null flags are always inferred; a present `schema.json` stays authoritative
/// for structure (kind/scale/field-set/time) but has the observed nullability
/// OR-ed in, so a stale file can't under-report a column as non-nullable and
/// make the reader drop NULLs. When absent, the inferred schema stands alone
/// (the "inferred if not provided" path, also covering pre-`schema.json`
/// datasets) — a missing file degrades to inference, never an error.
pub(crate) async fn resolve_dataset_schema(
    store: &dyn ObjectStore,
    summaries: &[ShardSummary],
) -> anyhow::Result<DatasetSchema> {
    let observed = DatasetSchema::infer_from_shards(summaries.iter().map(|s| &s.metadata))?;
    match crate::shard_source::read_dataset_schema(store).await? {
        Some(stored) => stored.reconcile_with_shards(&observed),
        None => Ok(observed),
    }
}

fn arrow_schema(schema: &DatasetSchema) -> SchemaRef {
    // Every declared field is exposed to DataFusion: Metric/Int as
    // Int64 (read from forward columns), String as Utf8
    // (reconstructed per doc from the inverted index at scan time),
    // and the time field as Timestamp(Nanosecond) so SQL can filter it
    // with date/timestamp literals. The time field is itself an Int
    // field on disk (epoch seconds); only its presentation type
    // differs. String reconstruction is O(num_docs) per shard per field.
    let time_field = schema.time_field.as_deref();
    let fields: Vec<Field> = schema
        .fields
        .iter()
        .map(|f| {
            if Some(f.name.as_str()) == time_field {
                // Epoch-seconds Int field presented as a timestamp.
                // Nanosecond, not Second, because DataFusion's
                // `TIMESTAMP '...'` literals default to nanosecond
                // precision: matching the column unit avoids an
                // injected `CAST(col AS Timestamp(ns))` around the
                // predicate (which both blocks our column-side
                // pushdown and, in this arrow version, mis-evaluates).
                // The exec scales the stored epoch-seconds up to
                // nanoseconds when building the array. Non-nullable:
                // every doc has a time value.
                return Field::new(
                    &f.name,
                    DataType::Timestamp(TimeUnit::Nanosecond, None),
                    false,
                );
            }
            match f.kind {
                // Forward columns are dense, but a declared-nullable one may hold
                // SQL NULLs (placeholder value + a validity mask), so present it
                // to DataFusion as nullable.
                FieldKind::Metric | FieldKind::Int => {
                    Field::new(&f.name, DataType::Int64, f.nullable)
                }
                // A string field is sparse: a doc with no term for it
                // reconstructs to NULL, so the column is nullable.
                FieldKind::String => Field::new(&f.name, DataType::Utf8, true),
            }
        })
        .collect();
    Arc::new(Schema::new(fields))
}
