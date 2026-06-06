//! `approx_top_k(col, k [, capacity])` — a custom `DataFusion` aggregate
//! returning the `k` most frequent values of a column, per group, as a
//! `List<Struct<value, count: Int64>>`.
//!
//! `DataFusion` has no built-in for this, so we register this UDAF. It
//! makes the function available in SQL *and* provides the row-scan
//! fallback for when the FTGS pushdown rule (see [`super::group_by`])
//! doesn't fire. Both paths run the same algorithm — count exactly, keep
//! the top `capacity`, take the top `k` — so they agree.

use std::any::Any;
use std::cmp::Ordering;
use std::collections::HashMap;
use std::sync::Arc;

use arrow::array::{
    Array, ArrayRef, Int64Array, ListArray, RecordBatch, StructArray, new_empty_array,
};
use arrow::datatypes::{DataType, Field, FieldRef, Fields, Schema};

use datafusion::common::utils::SingleRowListArrayBuilder;
use datafusion::common::{Result, ScalarValue, internal_err, not_impl_err};
use datafusion::logical_expr::function::{AccumulatorArgs, StateFieldsArgs};
use datafusion::logical_expr::utils::format_state_name;
use datafusion::logical_expr::{
    Accumulator, AggregateUDFImpl, ColumnarValue, Signature, Volatility,
};
use datafusion::physical_expr::PhysicalExpr;

/// `approx_top_k` aggregate. Arg types are validated in
/// [`return_type`](AggregateUDFImpl::return_type) /
/// [`accumulator`](AggregateUDFImpl::accumulator) rather than the
/// signature, so the column may be `Utf8` or `Int64`.
#[derive(Debug, PartialEq, Eq, Hash)]
pub(crate) struct ApproxTopK {
    signature: Signature,
}

impl Default for ApproxTopK {
    fn default() -> Self {
        Self {
            signature: Signature::user_defined(Volatility::Immutable),
        }
    }
}

/// Default `capacity` when the optional third arg is omitted: keep well
/// more than `k` candidates so the top-`k` survives the per-shard merge.
/// Shared with the FTGS pushdown so both paths agree.
pub(crate) fn default_capacity(k: usize) -> usize {
    k.saturating_mul(8).max(128)
}

/// The `Struct<value: <col type>, count: Int64>` element type, the source
/// of truth for the field names/nullability the accumulator must emit.
fn item_field(value_type: DataType) -> Field {
    Field::new_list_field(
        DataType::Struct(Fields::from(vec![
            Field::new("value", value_type, true),
            Field::new("count", DataType::Int64, false),
        ])),
        true,
    )
}

/// A column type `approx_top_k` accepts as its value (and how `value`
/// renders): `Utf8` strings or `Int64`.
fn check_value_type(dt: &DataType) -> Result<DataType> {
    match dt {
        DataType::Utf8 | DataType::Int64 => Ok(dt.clone()),
        other => not_impl_err!("approx_top_k supports Utf8 or Int64 columns, got {other}"),
    }
}

impl AggregateUDFImpl for ApproxTopK {
    fn as_any(&self) -> &dyn Any {
        self
    }

    #[allow(clippy::unnecessary_literal_bound)]
    fn name(&self) -> &str {
        "approx_top_k"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn coerce_types(&self, arg_types: &[DataType]) -> Result<Vec<DataType>> {
        // A user-defined signature does its own coercion; pass through.
        Ok(arg_types.to_vec())
    }

    fn return_type(&self, arg_types: &[DataType]) -> Result<DataType> {
        Ok(DataType::List(Arc::new(item_field(check_value_type(
            &arg_types[0],
        )?))))
    }

    fn state_fields(&self, args: StateFieldsArgs) -> Result<Vec<FieldRef>> {
        // The partial state is the top-`capacity` list (NOT top-`k`), so
        // the cross-partition merge keeps enough tail to stay accurate.
        let value_type = check_value_type(args.input_fields[0].data_type())?;
        Ok(vec![Arc::new(Field::new(
            format_state_name(args.name, "counts"),
            DataType::List(Arc::new(item_field(value_type))),
            false,
        ))])
    }

    fn accumulator(&self, acc_args: AccumulatorArgs) -> Result<Box<dyn Accumulator>> {
        let value_type = check_value_type(&acc_args.exprs[0].data_type(acc_args.schema)?)?;
        let k = scalar_usize(&acc_args.exprs[1])?;
        // `capacity` is at least `k` — fewer candidates than `k` couldn't
        // return a full top-k.
        let capacity = match acc_args.exprs.get(2) {
            Some(e) => scalar_usize(e)?,
            None => default_capacity(k),
        }
        .max(k);
        Ok(Box::new(TopKAccumulator {
            counts: HashMap::new(),
            value_type,
            k,
            capacity,
        }))
    }
}

/// Read a positive-integer literal arg by evaluating it against an empty
/// batch — the same trick `approx_percentile_cont` uses for its percentile.
#[allow(clippy::cast_sign_loss, clippy::cast_possible_truncation)]
fn scalar_usize(expr: &Arc<dyn PhysicalExpr>) -> Result<usize> {
    let empty = RecordBatch::new_empty(Arc::new(Schema::empty()));
    let ColumnarValue::Scalar(scalar) = expr.evaluate(&empty)? else {
        return internal_err!("approx_top_k expects a literal k/capacity, got an array");
    };
    let n = match scalar {
        ScalarValue::Int64(Some(v)) if v > 0 => v as usize,
        ScalarValue::Int32(Some(v)) if v > 0 => v as usize,
        ScalarValue::UInt64(Some(v)) if v > 0 => v as usize,
        ScalarValue::UInt32(Some(v)) if v > 0 => v as usize,
        other => {
            return not_impl_err!(
                "approx_top_k k/capacity must be a positive integer literal, got {other:?}"
            );
        }
    };
    Ok(n)
}

/// Exact frequency counter keyed by the (`Utf8`/`Int64`) value. Counts
/// all distinct values within a partition (like the FTGS pushdown does
/// per shard); `state` hands off the top `capacity`, `evaluate` emits the
/// top `k`, and `merge_batch` re-bounds the merged map to `capacity`.
#[derive(Debug)]
struct TopKAccumulator {
    counts: HashMap<ScalarValue, i64>,
    value_type: DataType,
    k: usize,
    capacity: usize,
}

impl TopKAccumulator {
    /// `counts` sorted by count desc, then value asc (for determinism),
    /// truncated to `n`.
    fn top(&self, n: usize) -> Vec<(ScalarValue, i64)> {
        let mut entries: Vec<(ScalarValue, i64)> =
            self.counts.iter().map(|(v, &c)| (v.clone(), c)).collect();
        entries.sort_by(|a, b| {
            b.1.cmp(&a.1)
                .then_with(|| a.0.partial_cmp(&b.0).unwrap_or(Ordering::Equal))
        });
        entries.truncate(n);
        entries
    }

    /// Build the `List<Struct<value, count>>` scalar of the top `n`.
    fn top_list(&self, n: usize) -> Result<ScalarValue> {
        let entries = self.top(n);
        let value_array: ArrayRef = if entries.is_empty() {
            new_empty_array(&self.value_type)
        } else {
            ScalarValue::iter_to_array(entries.iter().map(|(v, _)| v.clone()))?
        };
        let count_array = Arc::new(Int64Array::from(
            entries.iter().map(|(_, c)| *c).collect::<Vec<i64>>(),
        ));
        let struct_array = StructArray::from(vec![
            (
                Arc::new(Field::new("value", self.value_type.clone(), true)),
                value_array,
            ),
            (
                Arc::new(Field::new("count", DataType::Int64, false)),
                count_array as ArrayRef,
            ),
        ]);
        Ok(SingleRowListArrayBuilder::new(Arc::new(struct_array)).build_list_scalar())
    }

    /// Drop all but the top `capacity` entries — used after a merge to
    /// re-bound the map (not during accumulation, which holds all distinct
    /// so it matches the pushdown and avoids a per-batch sort).
    fn prune(&mut self) {
        if self.counts.len() > self.capacity {
            self.counts = self.top(self.capacity).into_iter().collect();
        }
    }
}

impl Accumulator for TopKAccumulator {
    fn update_batch(&mut self, values: &[ArrayRef]) -> Result<()> {
        let array = &values[0];
        for row in 0..array.len() {
            if array.is_null(row) {
                continue;
            }
            let value = ScalarValue::try_from_array(array, row)?;
            *self.counts.entry(value).or_default() += 1;
        }
        Ok(())
    }

    fn evaluate(&mut self) -> Result<ScalarValue> {
        self.top_list(self.k)
    }

    fn state(&mut self) -> Result<Vec<ScalarValue>> {
        Ok(vec![self.top_list(self.capacity)?])
    }

    fn merge_batch(&mut self, states: &[ArrayRef]) -> Result<()> {
        // The state type is the `List<Struct<value, count>>` we declared in
        // `state_fields`, so these downcasts hold by construction.
        let lists = states[0]
            .as_any()
            .downcast_ref::<ListArray>()
            .expect("approx_top_k state is a List");
        for i in 0..lists.len() {
            if lists.is_null(i) {
                continue;
            }
            let structs = lists.value(i);
            let structs = structs
                .as_any()
                .downcast_ref::<StructArray>()
                .expect("approx_top_k list item is a Struct");
            let counts = structs
                .column(1)
                .as_any()
                .downcast_ref::<Int64Array>()
                .expect("approx_top_k count is Int64");
            let value_col = structs.column(0);
            for row in 0..structs.len() {
                let value = ScalarValue::try_from_array(value_col, row)?;
                *self.counts.entry(value).or_default() += counts.value(row);
            }
        }
        self.prune();
        Ok(())
    }

    fn size(&self) -> usize {
        std::mem::size_of_val(self)
            + self.counts.capacity()
                * (std::mem::size_of::<ScalarValue>() + std::mem::size_of::<i64>())
    }
}
