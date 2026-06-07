//! Filter-clause evaluation — resolve `--filter` predicates (exact term or
//! inclusive range) against a shard's inverted indexes and AND-intersect
//! them into a [`FilterResult`] matched-doc set. The shard format types and
//! reader live in the parent [`shard`](super) module.

use anyhow::{Context, Result, bail};
use roaring::RoaringBitmap;

use super::{DiskShard, FieldKind, Metadata, Shard, encode_int_key, require_field};

/// What `--filter` resolves to: a single-term equality or an
/// inclusive range with optionally open ends. The CLI parser
/// constructs these; the engine consumes them.
#[derive(Debug, Clone, Copy)]
pub struct FilterClause<'a> {
    pub field: &'a str,
    pub op: FilterOp<'a>,
}

#[derive(Debug, Clone, Copy)]
pub enum FilterOp<'a> {
    /// Exact-term match against the field's inverted index.
    Equals(&'a str),
    /// Inclusive range. `None` on either side means unbounded.
    /// `lo` and `hi` both `None` is rejected upstream — it would
    /// match every doc, which the user almost certainly didn't mean.
    Range {
        lo: Option<&'a str>,
        hi: Option<&'a str>,
    },
}

impl<'a> FilterClause<'a> {
    #[must_use]
    pub fn equals(field: &'a str, term: &'a str) -> Self {
        Self {
            field,
            op: FilterOp::Equals(term),
        }
    }

    #[must_use]
    pub fn range(field: &'a str, lo: Option<&'a str>, hi: Option<&'a str>) -> Self {
        Self {
            field,
            op: FilterOp::Range { lo, hi },
        }
    }
}

impl std::fmt::Display for FilterClause<'_> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match &self.op {
            FilterOp::Equals(term) => write!(f, "{} = {:?}", self.field, term),
            FilterOp::Range { lo, hi } => write!(
                f,
                "{} = {}..{}",
                self.field,
                lo.unwrap_or(""),
                hi.unwrap_or(""),
            ),
        }
    }
}

/// Outcome of intersecting every filter clause: either no filter was
/// supplied (match every doc), some clause matched no docs (whole
/// result is empty, short-circuiting the rest), or a concrete doc set.
/// Public form of the engine's per-shard filter-resolution result.
/// Used internally by [`query_shard`] for aggregate dispatch and
/// externally by the SQL `TableProvider` to drive row-set
/// materialisation.
#[derive(Debug)]
pub enum FilterResult {
    /// No filter supplied — every doc matches.
    All,
    /// Filter matched zero docs (or some clause hit zero terms in
    /// the index and short-circuited the intersection).
    Empty,
    /// Concrete matched doc set.
    Bitmap(RoaringBitmap),
}

/// Resolve every clause in `filters` against `shard`, AND-intersect
/// the per-clause bitmaps, and return the [`FilterResult`] dispatch
/// shape. Empty `filters` slice → `All`; any clause that hits zero
/// terms short-circuits the whole result to `Empty`.
pub fn matched_doc_set(shard: &DiskShard, filters: &[FilterClause<'_>]) -> Result<FilterResult> {
    let metadata = shard.metadata();
    combine_filters(shard, metadata, filters)
}

pub(super) fn combine_filters(
    shard: &DiskShard,
    metadata: &Metadata,
    filters: &[FilterClause<'_>],
) -> Result<FilterResult> {
    use roaring::MultiOps;

    if filters.is_empty() {
        return Ok(FilterResult::All);
    }
    let mut resolved: Vec<RoaringBitmap> = Vec::with_capacity(filters.len());
    for clause in filters {
        match resolve_filter(shard, metadata, clause)? {
            Some(bm) => resolved.push(bm),
            None => return Ok(FilterResult::Empty),
        }
    }
    // Tree-reduced k-way intersection — beats pairwise `&=` when
    // bitmaps differ in size (smallest pair reduced first).
    let intersection: RoaringBitmap = resolved.into_iter().intersection();
    Ok(if intersection.is_empty() {
        FilterResult::Empty
    } else {
        FilterResult::Bitmap(intersection)
    })
}

/// Resolve one filter clause to its matched doc set, or `None` if no
/// docs match (a clause whose term/range hits zero terms in the
/// inverted index).
fn resolve_filter(
    shard: &DiskShard,
    metadata: &Metadata,
    clause: &FilterClause<'_>,
) -> Result<Option<RoaringBitmap>> {
    // String or Int field — both have an inverted index, but the
    // term encoding differs: String uses UTF-8 bytes directly, Int
    // parses the decimal term and encodes via encode_int_key.
    let filter_field = require_field(metadata, clause.field, &[FieldKind::String, FieldKind::Int])?;
    let idx = shard
        .inverted_index(clause.field)
        .expect("indexed field kind validated above");
    match clause.op {
        FilterOp::Equals(term) => {
            let key = encode_bound(filter_field.kind, clause.field, Some(term))?
                .expect("Some bound yields Some encoded key");
            Ok(idx.lookup_bytes(&key))
        }
        FilterOp::Range { lo, hi } => {
            use roaring::MultiOps;
            if lo.is_none() && hi.is_none() {
                // Reachable only via a programmatic
                // `FilterClause::range(_, None, None)`; the CLI
                // parser rejects `field=..` upstream. Hard-fail so
                // the misuse can't silently match every doc.
                bail!(
                    "range filter for field {field:?} must have at least one bound",
                    field = clause.field,
                );
            }
            let lo_bytes = encode_bound(filter_field.kind, clause.field, lo)?;
            let hi_bytes = encode_bound(filter_field.kind, clause.field, hi)?;
            // Encoded byte order matches the desired range semantics
            // for both kinds (UTF-8 lex for String, numeric via
            // encode_int_key for Int), so a byte compare catches
            // inverted ranges across both.
            if let (Some(l), Some(h)) = (&lo_bytes, &hi_bytes)
                && l > h
            {
                bail!(
                    "range filter for field {field:?}: lower bound {lo:?} exceeds upper bound {hi:?}",
                    field = clause.field,
                    lo = lo.expect("checked above"),
                    hi = hi.expect("checked above"),
                );
            }
            // Tree-reduced k-way union — for wide ranges over
            // high-cardinality fields this beats pairwise `|=`
            // because intermediate bitmap sizes stay smaller.
            let union: RoaringBitmap = idx
                .range_bytes(lo_bytes.as_deref(), hi_bytes.as_deref())
                .map(|(_, bm)| bm)
                .union();
            Ok(if union.is_empty() { None } else { Some(union) })
        }
    }
}

/// Encode a single filter bound (either side of a range, or the
/// equality term) for an inverted-index lookup. `None` bound →
/// `None` byte vec (the open-range case in
/// [`InvertedIndex::range_bytes`]). String fields use UTF-8 bytes
/// directly; Int fields parse the bound as i64 and encode via
/// [`encode_int_key`] so the FST's byte order matches numeric order.
fn encode_bound(kind: FieldKind, field: &str, bound: Option<&str>) -> Result<Option<Vec<u8>>> {
    let Some(bound) = bound else {
        return Ok(None);
    };
    match kind {
        FieldKind::String => Ok(Some(bound.as_bytes().to_vec())),
        FieldKind::Int => {
            let v: i64 = bound.parse().with_context(|| {
                format!("filter value {bound:?} is not a valid int64 for field {field:?}")
            })?;
            Ok(Some(encode_int_key(v).to_vec()))
        }
        FieldKind::Metric => unreachable!("require_field rejected non-indexed kind"),
    }
}
