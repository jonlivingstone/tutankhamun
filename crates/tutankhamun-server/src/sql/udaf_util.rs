//! Shared helpers for the custom aggregate UDFs (`approx_top_k`, `theta`):
//! column value-type validation and literal-integer argument evaluation.

use std::sync::Arc;

use arrow::array::RecordBatch;
use arrow::datatypes::{DataType, Schema};
use datafusion::common::{Result, ScalarValue, internal_err, not_impl_err};
use datafusion::logical_expr::ColumnarValue;
use datafusion::physical_expr::PhysicalExpr;

/// The column value-type a sketch UDF accepts: `Utf8` strings or `Int64`.
/// `func` names the calling aggregate for the error message.
pub(super) fn check_value_type(func: &str, dt: &DataType) -> Result<DataType> {
    match dt {
        DataType::Utf8 | DataType::Int64 => Ok(dt.clone()),
        other => not_impl_err!("{func} supports Utf8 or Int64 columns, got {other}"),
    }
}

/// Read a positive-integer literal arg by evaluating it against an empty
/// batch — the same trick `approx_percentile_cont` uses for its percentile.
#[allow(clippy::cast_sign_loss, clippy::cast_possible_truncation)]
pub(super) fn scalar_usize(expr: &Arc<dyn PhysicalExpr>) -> Result<usize> {
    let empty = RecordBatch::new_empty(Arc::new(Schema::empty()));
    let ColumnarValue::Scalar(scalar) = expr.evaluate(&empty)? else {
        return internal_err!("expected a literal integer argument, got an array");
    };
    let n = match scalar {
        ScalarValue::Int64(Some(v)) if v > 0 => v as usize,
        ScalarValue::Int32(Some(v)) if v > 0 => v as usize,
        ScalarValue::UInt64(Some(v)) if v > 0 => v as usize,
        ScalarValue::UInt32(Some(v)) if v > 0 => v as usize,
        other => {
            return not_impl_err!("argument must be a positive integer literal, got {other:?}");
        }
    };
    Ok(n)
}
