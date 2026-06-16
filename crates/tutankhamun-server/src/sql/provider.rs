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

use super::exec::TutankhamunExec;
use super::pushdown;
use crate::cache::Cache;
use crate::shard::{FieldKind, Metadata};
use crate::shard_source::{ObjectStoreShardSource, ShardSource};
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
    /// Discover the dataset under `url`, derive an Arrow schema from
    /// its first shard, and return a provider `DataFusion` can register.
    /// Errors if the dataset is empty (no shards) — `DataFusion` has no
    /// way to ask us for a schema if we can't read one.
    pub async fn try_new(url: String, cache: Arc<Cache>) -> anyhow::Result<Self> {
        let registry = StorageRegistry::from_url(&url)?;
        let source = ObjectStoreShardSource::new(registry.store());
        let summaries = source
            .discover()
            .await
            .with_context(|| format!("discover shards at {url}"))?;
        let first = summaries
            .first()
            .ok_or_else(|| anyhow::anyhow!("no shards at {url}"))?;
        // A dataset is one logical table, so every shard must share a
        // field set. The query path projects against this single
        // schema; a diverging shard would otherwise mis-project or
        // fail deep in the scan rather than here at registration.
        // `discover()` already loaded every shard's metadata, so this
        // check is a fold over in-memory data, no extra I/O.
        for s in &summaries[1..] {
            if s.metadata.fields != first.metadata.fields {
                anyhow::bail!(
                    "shards at {url} have inconsistent schemas: {} and {} declare different fields",
                    first.location,
                    s.location,
                );
            }
        }
        let schema = arrow_schema_from_metadata(&first.metadata);
        let field_kinds = first
            .metadata
            .fields
            .iter()
            .map(|f| (f.name.clone(), f.kind))
            .collect();
        Ok(Self {
            url,
            cache,
            schema,
            field_kinds,
        })
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

fn arrow_schema_from_metadata(metadata: &Metadata) -> SchemaRef {
    // Every declared field is exposed to DataFusion: Metric/Int as
    // Int64 (read from forward columns), String as Utf8
    // (reconstructed per doc from the inverted index at scan time),
    // and the time field as Timestamp(Nanosecond) so SQL can filter it
    // with date/timestamp literals. The time field is itself an Int
    // field on disk (epoch seconds); only its presentation type
    // differs. String reconstruction is O(num_docs) per shard per field.
    let time_field = metadata.time_field.as_deref();
    let fields: Vec<Field> = metadata
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
                // Forward columns are dense — every doc has an int
                // value, so these are non-nullable.
                FieldKind::Metric | FieldKind::Int => Field::new(&f.name, DataType::Int64, false),
                // A string field is sparse: a doc with no term for it
                // reconstructs to NULL, so the column is nullable.
                FieldKind::String => Field::new(&f.name, DataType::Utf8, true),
            }
        })
        .collect();
    Arc::new(Schema::new(fields))
}
