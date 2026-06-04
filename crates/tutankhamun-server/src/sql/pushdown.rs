//! `DataFusion` `Expr` → owned filter form that can outlive the
//! `scan` call. The exec converts these to
//! [`crate::shard::FilterClause`]s (which borrow `&str`) at the
//! point we hand them to [`crate::shard::query_shard`].
//!
//! Only expressions our inverted index can answer turn into a
//! `PushedFilter`; everything else returns `None`, which lets
//! [`super::provider::TutankhamunTableProvider::supports_filters_pushdown`]
//! report `Unsupported` so `DataFusion` evaluates them post-scan.

use datafusion::logical_expr::{BinaryExpr, Expr, Operator};
use datafusion::scalar::ScalarValue;

use crate::shard::FilterClause;

/// Owned mirror of [`FilterClause`] so the SQL execution plan can
/// hold filters past the lifetime of the `scan(&self, …)` call.
#[derive(Debug, Clone)]
pub(crate) struct PushedFilter {
    pub field: String,
    pub op: PushedOp,
    /// Whether the pushed filter exactly matches the original SQL
    /// predicate. `false` for strict `<` / `>`, which we widen to
    /// inclusive `<=` / `>=` (the index range is inclusive-only). An
    /// inexact filter still prunes at scan time, but the provider
    /// reports `Inexact` so `DataFusion` re-applies the original
    /// predicate and drops the extra boundary rows.
    pub exact: bool,
}

#[derive(Debug, Clone)]
pub(crate) enum PushedOp {
    Equals(String),
    Range {
        lo: Option<String>,
        hi: Option<String>,
    },
}

impl PushedFilter {
    pub(crate) fn as_clause(&self) -> FilterClause<'_> {
        match &self.op {
            PushedOp::Equals(term) => FilterClause::equals(&self.field, term),
            PushedOp::Range { lo, hi } => {
                FilterClause::range(&self.field, lo.as_deref(), hi.as_deref())
            }
        }
    }
}

/// Try to convert a `DataFusion` predicate into a `PushedFilter`.
/// Supports `column = literal`, `column <op> literal` for
/// `<, <=, >, >=` with string or int literals. Anything else
/// (AND splits, OR, IS NULL, IN, …) returns `None`; `DataFusion`
/// handles them post-scan.
pub(crate) fn expr_to_pushed_filter(expr: &Expr) -> Option<PushedFilter> {
    let Expr::BinaryExpr(BinaryExpr { left, op, right }) = expr else {
        return None;
    };

    let (field, literal, op) = match (left.as_ref(), right.as_ref()) {
        (Expr::Column(c), Expr::Literal(l, _)) => (c.name.clone(), l, *op),
        (Expr::Literal(l, _), Expr::Column(c)) => (c.name.clone(), l, flip(*op)?),
        _ => return None,
    };

    let Rendered { term, lossless } = render_literal(literal)?;
    // A filter is exact (DataFusion skips re-checking) only when both:
    //  - the operator maps without widening — `=`, `<=`, `>=` do;
    //    strict `<` / `>` widen to inclusive bounds (the FST range is
    //    inclusive-only) and over-return the boundary value; and
    //  - the literal rendered losslessly — a sub-second timestamp
    //    floored to whole seconds (the time field's granularity) can't
    //    be honored exactly.
    // Inexact filters still prune at scan time; DataFusion re-applies
    // the original predicate to drop the extra rows.
    match op {
        Operator::Eq => Some(PushedFilter {
            field,
            op: PushedOp::Equals(term),
            exact: lossless,
        }),
        Operator::GtEq | Operator::Gt => Some(PushedFilter {
            field,
            op: PushedOp::Range {
                lo: Some(term),
                hi: None,
            },
            exact: lossless && op == Operator::GtEq,
        }),
        Operator::LtEq | Operator::Lt => Some(PushedFilter {
            field,
            op: PushedOp::Range {
                lo: None,
                hi: Some(term),
            },
            exact: lossless && op == Operator::LtEq,
        }),
        _ => None,
    }
}

fn flip(op: Operator) -> Option<Operator> {
    Some(match op {
        Operator::Eq => Operator::Eq,
        Operator::Lt => Operator::Gt,
        Operator::LtEq => Operator::GtEq,
        Operator::Gt => Operator::Lt,
        Operator::GtEq => Operator::LtEq,
        _ => return None,
    })
}

/// A scalar literal rendered as the text the engine's per-field-kind
/// parser expects, plus whether that render was lossless.
struct Rendered {
    term: String,
    /// `true` when `term` reproduces the literal exactly. Only a
    /// sub-second timestamp floored to whole seconds sets this
    /// `false`; every other scalar renders losslessly.
    lossless: bool,
}

/// Render a scalar literal as the text the engine's per-field-kind
/// parser expects. Strings borrow their UTF-8; integers format to
/// decimal text (the same form `t9n query --filter` accepts).
/// Returns `None` for null / list / struct / non-text-renderable
/// scalars.
fn render_literal(v: &ScalarValue) -> Option<Rendered> {
    // Convert a sub-second timestamp to whole seconds, flooring with
    // `div_euclid`; `lossless` is true only when the literal already
    // sat on a second boundary. The time field has second
    // granularity, so a finer bound can't be represented exactly:
    // DataFusion re-applies the original predicate (range filters
    // from `<`/`>` are already reported inexact, and equality on a
    // finer literal simply won't match a whole-second column).
    let secs = |n: i64, per_sec: i64| Rendered {
        term: n.div_euclid(per_sec).to_string(),
        lossless: n.rem_euclid(per_sec) == 0,
    };
    let lossless = |term: String| Rendered {
        term,
        lossless: true,
    };
    match v {
        ScalarValue::Utf8(Some(s)) | ScalarValue::LargeUtf8(Some(s)) => Some(lossless(s.clone())),
        ScalarValue::Int8(Some(n)) => Some(lossless(n.to_string())),
        ScalarValue::Int16(Some(n)) => Some(lossless(n.to_string())),
        ScalarValue::Int32(Some(n)) => Some(lossless(n.to_string())),
        // Whole-second timestamps render exactly like an Int64 (the
        // time field is stored as epoch seconds); the sub-second
        // variants below floor to seconds.
        ScalarValue::Int64(Some(n)) | ScalarValue::TimestampSecond(Some(n), _) => {
            Some(lossless(n.to_string()))
        }
        ScalarValue::UInt8(Some(n)) => Some(lossless(n.to_string())),
        ScalarValue::UInt16(Some(n)) => Some(lossless(n.to_string())),
        ScalarValue::UInt32(Some(n)) => Some(lossless(n.to_string())),
        // UInt64 may exceed i64; the engine's int parser catches
        // out-of-range values with a clear error.
        ScalarValue::UInt64(Some(n)) => Some(lossless(n.to_string())),
        ScalarValue::TimestampMillisecond(Some(n), _) => Some(secs(*n, 1_000)),
        ScalarValue::TimestampMicrosecond(Some(n), _) => Some(secs(*n, 1_000_000)),
        ScalarValue::TimestampNanosecond(Some(n), _) => Some(secs(*n, 1_000_000_000)),
        _ => None,
    }
}
