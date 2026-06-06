//! `theta(col [, nominal_entries])` and `theta_intersect(a, b)` — custom
//! `DataFusion` functions for theta sketches (distinct counting that also
//! supports intersection; see [`crate::sketches::ThetaSketch`]).
//!
//! - `theta` is an aggregate UDAF returning Arrow `Binary` (the serialized
//!   sketch). `DataFusion` has no built-in, so registering it makes the
//!   function available in SQL and provides the row-scan fallback for when
//!   the FTGS pushdown rule doesn't fire.
//! - `theta_intersect` is a *scalar* UDF over two Binary sketch columns,
//!   returning the estimated overlap size as `Int64` — the cohort-overlap
//!   question HLL can't answer.

use std::any::Any;
use std::sync::Arc;

use arrow::array::{Array, ArrayRef, Int64Array, Int64Builder, StringArray};
use arrow::datatypes::{DataType, Field, FieldRef};

use datafusion::common::cast::as_binary_array;
use datafusion::common::{DataFusionError, Result, ScalarValue, not_impl_err};
use datafusion::logical_expr::function::{AccumulatorArgs, StateFieldsArgs};
use datafusion::logical_expr::utils::format_state_name;
use datafusion::logical_expr::{
    Accumulator, AggregateUDFImpl, ColumnarValue, ScalarFunctionArgs, ScalarUDFImpl, Signature,
    Volatility,
};

use super::approx_top_k::scalar_usize;
use crate::sketches::ThetaSketch;

/// `theta` aggregate. The column may be `Utf8` or `Int64` (validated in
/// `accumulator`); an optional second arg sets the nominal entries.
#[derive(Debug, PartialEq, Eq, Hash)]
pub(crate) struct Theta {
    signature: Signature,
}

impl Default for Theta {
    fn default() -> Self {
        Self {
            signature: Signature::user_defined(Volatility::Immutable),
        }
    }
}

/// A column type `theta` accepts: `Utf8` (hash term bytes) or `Int64`.
fn check_value_type(dt: &DataType) -> Result<DataType> {
    match dt {
        DataType::Utf8 | DataType::Int64 => Ok(dt.clone()),
        other => not_impl_err!("theta supports Utf8 or Int64 columns, got {other}"),
    }
}

impl AggregateUDFImpl for Theta {
    fn as_any(&self) -> &dyn Any {
        self
    }

    #[allow(clippy::unnecessary_literal_bound)]
    fn name(&self) -> &str {
        "theta"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn coerce_types(&self, arg_types: &[DataType]) -> Result<Vec<DataType>> {
        Ok(arg_types.to_vec())
    }

    fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType> {
        Ok(DataType::Binary)
    }

    fn state_fields(&self, args: StateFieldsArgs) -> Result<Vec<FieldRef>> {
        Ok(vec![Arc::new(Field::new(
            format_state_name(args.name, "sketch"),
            DataType::Binary,
            false,
        ))])
    }

    fn accumulator(&self, acc_args: AccumulatorArgs) -> Result<Box<dyn Accumulator>> {
        let value_type = check_value_type(&acc_args.exprs[0].data_type(acc_args.schema)?)?;
        let nominal = match acc_args.exprs.get(1) {
            Some(e) => scalar_usize(e)?,
            None => ThetaSketch::DEFAULT_NOMINAL,
        };
        Ok(Box::new(ThetaAccumulator {
            sketch: ThetaSketch::with_nominal(nominal),
            value_type,
        }))
    }
}

/// Builds a [`ThetaSketch`] over the input column's values.
#[derive(Debug)]
struct ThetaAccumulator {
    sketch: ThetaSketch,
    value_type: DataType,
}

impl Accumulator for ThetaAccumulator {
    fn update_batch(&mut self, values: &[ArrayRef]) -> Result<()> {
        let array = &values[0];
        match self.value_type {
            DataType::Int64 => {
                let a = array
                    .as_any()
                    .downcast_ref::<Int64Array>()
                    .expect("theta Int64 column");
                for i in 0..a.len() {
                    if !a.is_null(i) {
                        self.sketch.insert(a.value(i));
                    }
                }
            }
            DataType::Utf8 => {
                let a = array
                    .as_any()
                    .downcast_ref::<StringArray>()
                    .expect("theta Utf8 column");
                for i in 0..a.len() {
                    if !a.is_null(i) {
                        self.sketch.insert_bytes(a.value(i).as_bytes());
                    }
                }
            }
            _ => unreachable!("theta value type validated in accumulator"),
        }
        Ok(())
    }

    fn evaluate(&mut self) -> Result<ScalarValue> {
        Ok(ScalarValue::Binary(Some(self.sketch.to_bytes())))
    }

    fn state(&mut self) -> Result<Vec<ScalarValue>> {
        Ok(vec![ScalarValue::Binary(Some(self.sketch.to_bytes()))])
    }

    fn merge_batch(&mut self, states: &[ArrayRef]) -> Result<()> {
        let arr = as_binary_array(&states[0])?;
        for i in 0..arr.len() {
            if !arr.is_null(i) {
                let other = ThetaSketch::from_bytes(arr.value(i))
                    .map_err(|e| DataFusionError::External(e.into()))?;
                self.sketch.union(&other);
            }
        }
        Ok(())
    }

    fn size(&self) -> usize {
        std::mem::size_of_val(self) + self.sketch.estimated_size()
    }
}

/// `theta_intersect(a, b)` — the estimated size of the intersection of two
/// theta sketches (`Binary` columns), as `Int64`.
#[derive(Debug, PartialEq, Eq, Hash)]
pub(crate) struct ThetaIntersect {
    signature: Signature,
}

impl Default for ThetaIntersect {
    fn default() -> Self {
        Self {
            signature: Signature::exact(
                vec![DataType::Binary, DataType::Binary],
                Volatility::Immutable,
            ),
        }
    }
}

impl ScalarUDFImpl for ThetaIntersect {
    fn as_any(&self) -> &dyn Any {
        self
    }

    #[allow(clippy::unnecessary_literal_bound)]
    fn name(&self) -> &str {
        "theta_intersect"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType> {
        Ok(DataType::Int64)
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        let arrays = ColumnarValue::values_to_arrays(&args.args)?;
        let a = as_binary_array(&arrays[0])?;
        let b = as_binary_array(&arrays[1])?;
        let mut out = Int64Builder::with_capacity(a.len());
        for i in 0..a.len() {
            if a.is_null(i) || b.is_null(i) {
                out.append_null();
                continue;
            }
            let sa = ThetaSketch::from_bytes(a.value(i))
                .map_err(|e| DataFusionError::External(e.into()))?;
            let sb = ThetaSketch::from_bytes(b.value(i))
                .map_err(|e| DataFusionError::External(e.into()))?;
            out.append_value(ThetaSketch::intersect(&sa, &sb).estimate());
        }
        Ok(ColumnarValue::Array(Arc::new(out.finish())))
    }
}
