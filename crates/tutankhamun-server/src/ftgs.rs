//! FTGS — the Field-Term-Group-Stat scan (§2.6).
//!
//! The engine's single aggregation primitive: every analytical
//! `GROUP BY` ultimately runs through this loop. Given a per-doc group
//! assignment ([`GroupLookup`], set by prior regroups) and a set of
//! group-by fields, it walks each field's term dictionary and, for
//! every `(field, term)`, emits the requested stats per group. This is
//! the four-level cursor — field → term → group → stat.
//!
//! Output ordering is deterministic, which [`merge_ftgs`] relies on:
//! fields in the caller's order, terms ascending (the FST is sorted by
//! raw key), groups ascending (the [`BitTree`] drains low-to-high). The
//! term stays in its raw order-preserving FST-key form all the way
//! through the merge — byte order is the canonical sort order for both
//! kinds (numeric for `Int` via [`encode_int_key`], lexical for
//! `String`) — and is rendered to a display string only at the output
//! edge via [`render_term`].
//!
//! ## Stats
//!
//! Stats live behind the [`Stat`] enum (accumulation) and
//! [`StatSpec::combine`] (cross-shard merge), each operator a variant.
//! The scalar operators (sum / count / min / max) hold one `i64` per
//! group. A mergeable sketch — `approx_percentile` (t-digest),
//! `approx_count_distinct` (HLL), `theta` (§3.1) — is a variant whose
//! accumulator owns its own storage (e.g. `Vec<TDigest>`); the critical
//! loop and `FtgsRow` are agnostic to which variant a stat is.

use std::cmp::Reverse;
use std::collections::BinaryHeap;

use anyhow::{Context as _, Result, bail};
use rayon::prelude::*;

use crate::bit_tree::BitTree;
use crate::group_lookup::GroupLookup;
use crate::runtime;
use crate::shard::{FieldKind, Shard, decode_int_key, require_field, utf8_term};
use crate::sketches::Hll;

/// A stat the caller wants accumulated per group, named against the
/// shard's columns. `Sum`/`Min`/`Max` read a `Metric`/`Int` forward
/// column; `ApproxCountDistinct` reads a forward column too, or a
/// `String` field's inverted-index terms; `Count` needs no column.
#[derive(Debug, Clone, Copy)]
pub enum StatSpec<'a> {
    Count,
    Sum(&'a str),
    Min(&'a str),
    Max(&'a str),
    ApproxCountDistinct(&'a str),
}

impl StatSpec<'_> {
    /// Merge two scalar stat values for the same `(field, term, group)`:
    /// additive stats sum, `Min`/`Max` fold. The sketch stat
    /// (`ApproxCountDistinct`) merges its accumulator instead — see
    /// [`combine_stats`] — so it never reaches here.
    #[must_use]
    pub fn combine(self, a: i64, b: i64) -> i64 {
        match self {
            StatSpec::Count | StatSpec::Sum(_) => a + b,
            StatSpec::Min(_) => a.min(b),
            StatSpec::Max(_) => a.max(b),
            StatSpec::ApproxCountDistinct(_) => unreachable!("sketch combines its accumulator"),
        }
    }
}

/// A stat's per-group accumulator value, carried through the cross-shard
/// merge un-finalized so mergeable sketches can be unioned (you can't
/// combine two finalized cardinalities). [`finalize`](Self::finalize)
/// produces the output `i64`.
#[derive(Debug, Clone)]
pub enum StatValue {
    /// `Count`/`Sum`/`Min`/`Max` — the value is already the result.
    Scalar(i64),
    /// `ApproxCountDistinct` — a `HyperLogLog`, finalized to an estimate.
    Hll(Hll),
}

impl StatValue {
    #[must_use]
    pub fn finalize(&self) -> i64 {
        match self {
            StatValue::Scalar(v) => *v,
            StatValue::Hll(h) => h.estimate(),
        }
    }
}

/// Combine `other`'s stats into `acc` in place — the cross-shard merge of
/// two groups' accumulators. Scalars fold via [`StatSpec::combine`];
/// sketches union their registers. Both sides come from the same `specs`,
/// so the variants always match.
pub fn combine_stats(acc: &mut [StatValue], other: &[StatValue], specs: &[StatSpec]) {
    for (i, b) in other.iter().enumerate() {
        match (&mut acc[i], b) {
            (StatValue::Scalar(a), StatValue::Scalar(b)) => *a = specs[i].combine(*a, *b),
            (StatValue::Hll(a), StatValue::Hll(b)) => a.merge(b),
            _ => unreachable!("stat variants match across shards (same specs)"),
        }
    }
}

/// One output row: the requested stats for a single `(field, term,
/// group)`. `term` is the raw order-preserving FST key — the canonical
/// sort order the merge compares by; render it for display with
/// [`render_term`] and the field's `FieldKind`. `stats[i]` is the i-th
/// [`StatSpec`]'s accumulator over the docs that carry `term` and fall in
/// `group`, carried un-finalized for the merge; call
/// [`StatValue::finalize`] at output.
#[derive(Debug, Clone)]
pub struct FtgsRow {
    pub field: String,
    pub term: Box<[u8]>,
    pub group: u32,
    pub stats: Vec<StatValue>,
}

/// Live per-group accumulator for one stat. Each variant owns its own
/// `slots` (one per group) seeded to the operator's identity, so the
/// inner loop is a flat array write and a sketch variant can pick a
/// different storage type without touching the loop.
enum Stat<'a> {
    Count {
        slots: Vec<i64>,
    },
    Sum {
        col: &'a [i64],
        slots: Vec<i64>,
    },
    Min {
        col: &'a [i64],
        slots: Vec<i64>,
    },
    Max {
        col: &'a [i64],
        slots: Vec<i64>,
    },
    Hll {
        col: &'a [i64],
        sketches: Vec<Hll>,
    },
    /// `approx_distinct` over a `String` column, which has no forward
    /// column: `doc_term[doc]` indexes the doc's term in `terms` (or
    /// `None` when the doc has no value — a SQL NULL, counted by no
    /// distinct). Hashing the term bytes (not the per-shard index)
    /// keeps the sketch mergeable across shards.
    HllBytes {
        doc_term: Vec<Option<u32>>,
        terms: Vec<Box<[u8]>>,
        sketches: Vec<Hll>,
    },
}

impl<'a> Stat<'a> {
    /// Resolve a spec against the shard and allocate `num_groups`
    /// identity-seeded slots.
    fn new(spec: StatSpec<'a>, shard: &'a dyn Shard, num_groups: usize) -> Result<Self> {
        let column = |name: &str| -> Result<&'a [i64]> {
            shard
                .forward_column(name)
                .with_context(|| format!("stat field {name:?} has no forward column"))
        };
        Ok(match spec {
            StatSpec::Count => Stat::Count {
                slots: vec![0; num_groups],
            },
            StatSpec::Sum(name) => Stat::Sum {
                col: column(name)?,
                slots: vec![0; num_groups],
            },
            StatSpec::Min(name) => Stat::Min {
                col: column(name)?,
                slots: vec![i64::MAX; num_groups],
            },
            StatSpec::Max(name) => Stat::Max {
                col: column(name)?,
                slots: vec![i64::MIN; num_groups],
            },
            // `Int`/`Metric` have a dense forward column; `String` does
            // not, so it sources term bytes from the inverted index.
            StatSpec::ApproxCountDistinct(name) => {
                if let Some(col) = shard.forward_column(name) {
                    Stat::Hll {
                        col,
                        sketches: vec![Hll::default(); num_groups],
                    }
                } else {
                    let (doc_term, terms) = term_map(shard, name)?;
                    Stat::HllBytes {
                        doc_term,
                        terms,
                        sketches: vec![Hll::default(); num_groups],
                    }
                }
            }
        })
    }

    /// Fold `doc`'s value into `group`'s slot.
    fn update(&mut self, group: usize, doc: usize) {
        match self {
            Stat::Count { slots } => slots[group] += 1,
            Stat::Sum { col, slots } => slots[group] += col[doc],
            Stat::Min { col, slots } => slots[group] = slots[group].min(col[doc]),
            Stat::Max { col, slots } => slots[group] = slots[group].max(col[doc]),
            Stat::Hll { col, sketches } => sketches[group].insert(col[doc]),
            Stat::HllBytes {
                doc_term,
                terms,
                sketches,
            } => {
                if let Some(t) = doc_term[doc] {
                    sketches[group].insert_bytes(&terms[t as usize]);
                }
            }
        }
    }

    /// Take `group`'s accumulated value for output, leaving the slot at
    /// the operator's identity so the buffer is reusable for the next
    /// term. Moving the sketch out (rather than cloning) keeps this off
    /// the per-row hot path; only the groups a term touched are taken, so
    /// the cost scales with groups seen, not the group count.
    fn take_value(&mut self, group: usize) -> StatValue {
        match self {
            Stat::Count { slots } | Stat::Sum { slots, .. } => {
                StatValue::Scalar(std::mem::take(&mut slots[group]))
            }
            Stat::Min { slots, .. } => {
                StatValue::Scalar(std::mem::replace(&mut slots[group], i64::MAX))
            }
            Stat::Max { slots, .. } => {
                StatValue::Scalar(std::mem::replace(&mut slots[group], i64::MIN))
            }
            Stat::Hll { sketches, .. } | Stat::HllBytes { sketches, .. } => {
                StatValue::Hll(std::mem::take(&mut sketches[group]))
            }
        }
    }
}

/// A `String` column's reverse map: per doc, the index into the term
/// table of the term covering it (`None` = no value), plus the term
/// bytes themselves (allocated once each, indexed by `doc_term`).
type TermMap = (Vec<Option<u32>>, Vec<Box<[u8]>>);

/// Build a doc→term reverse map for a `String` field (no forward
/// column): walk its inverted index once, allocating each term's bytes
/// exactly once in `terms` and recording, per doc, the term that covers
/// it. Docs with no term map to `None`. `O(num_docs)` work per shard.
fn term_map(shard: &dyn Shard, name: &str) -> Result<TermMap> {
    let index = shard.inverted_index(name).with_context(|| {
        format!("stat field {name:?} has neither a forward column nor an inverted index")
    })?;
    let mut doc_term: Vec<Option<u32>> = vec![None; usize::try_from(shard.num_docs())?];
    let mut terms: Vec<Box<[u8]>> = Vec::new();
    for (term_bytes, bitmap) in index.range_bytes(None, None) {
        let term_idx = u32::try_from(terms.len())?;
        terms.push(term_bytes.into_boxed_slice());
        for doc in &bitmap {
            doc_term[doc as usize] = Some(term_idx);
        }
    }
    Ok((doc_term, terms))
}

/// Run an FTGS scan over one shard.
///
/// `group_by` names the fields whose terms drive the cursor — each must
/// be filterable (`String` or `Int`, i.e. carry an inverted index).
/// `stats` are accumulated per group. `groups` must be sized to the
/// shard's doc count; group 0 is "filtered out" and contributes to no
/// row. Rows come back field-then-term-then-group ordered.
pub fn ftgs_scan(
    shard: &dyn Shard,
    groups: &GroupLookup,
    group_by: &[&str],
    stats: &[StatSpec],
) -> Result<Vec<FtgsRow>> {
    let num_docs = shard.num_docs();
    if groups.len() as u64 != num_docs {
        bail!(
            "group lookup len {} does not match shard num_docs {num_docs}",
            groups.len()
        );
    }

    let num_groups = groups.num_groups().max(1) as usize;
    let mut accumulators: Vec<Stat> = stats
        .iter()
        .map(|spec| Stat::new(*spec, shard, num_groups))
        .collect::<Result<_>>()?;

    let metadata = shard.metadata();
    let mut rows = Vec::new();
    // Both are empty after every term drains, so a single pair is reused
    // across all fields rather than reallocated per field.
    let mut groups_seen = BitTree::new(num_groups);
    let mut drained = Vec::new();

    for &field in group_by {
        // A group-by field must be filterable (String/Int → has an
        // inverted index); `require_field` rejects a bare Metric.
        require_field(metadata, field, &[FieldKind::String, FieldKind::Int])?;
        let index = shard
            .inverted_index(field)
            .with_context(|| format!("field {field:?} has no inverted index"))?;

        for (key, bitmap) in index.range_bytes(None, None) {
            for doc in &bitmap {
                let group = groups.get(doc as usize);
                if group == 0 {
                    continue;
                }
                groups_seen.set(group as usize);
                for stat in &mut accumulators {
                    stat.update(group as usize, doc as usize);
                }
            }

            // drain_into yields the touched groups ascending and empties
            // the tree, leaving it ready for the next term.
            drained.clear();
            groups_seen.drain_into(&mut drained);
            if drained.is_empty() {
                continue;
            }
            // The raw FST key is the canonical term: byte order = the
            // merge's sort order. Rendered to a string only at output.
            let term: Box<[u8]> = key.into();
            for &g in &drained {
                let row_stats = accumulators
                    .iter_mut()
                    .map(|s| s.take_value(g as usize))
                    .collect();
                rows.push(FtgsRow {
                    field: field.to_string(),
                    term: term.clone(),
                    group: g,
                    stats: row_stats,
                });
            }
        }
    }
    Ok(rows)
}

/// Aggregate a single set of docs as one group, returning one finalized
/// value per [`StatSpec`]. Used for groups the term cursor doesn't
/// emit — e.g. the SQL NULL group: docs with no term for a group-by
/// column. `docs` must be valid indices into the shard's forward
/// columns.
pub fn aggregate_docs(
    shard: &dyn Shard,
    docs: impl Iterator<Item = u32>,
    stats: &[StatSpec],
) -> Result<Vec<StatValue>> {
    let mut accumulators: Vec<Stat> = stats
        .iter()
        .map(|spec| Stat::new(*spec, shard, 1))
        .collect::<Result<_>>()?;
    for doc in docs {
        for stat in &mut accumulators {
            stat.update(0, doc as usize);
        }
    }
    Ok(accumulators.iter_mut().map(|s| s.take_value(0)).collect())
}

/// Render a raw FST key ([`FtgsRow::term`]) as its display term: `Int`
/// keys are the order-preserving 8-byte encoding, everything else a
/// UTF-8 string. The caller supplies the field's `FieldKind` (held by
/// the schema); the kind isn't stored per row.
#[must_use]
pub fn render_term(kind: FieldKind, key: &[u8]) -> String {
    match kind {
        FieldKind::Int => decode_int_key(key).to_string(),
        _ => utf8_term(key),
    }
}

/// Within-daemon fan-out: run [`ftgs_scan`] over each `(shard, groups)`
/// pair — each shard carries its own [`GroupLookup`] — and merge the
/// per-shard results. Each shard's scan is single-threaded; the shards
/// run in parallel on the bounded Rayon pool when it's initialised
/// (the daemon), else sequentially (tests / non-`serve` CLI). The
/// cross-daemon layer reuses [`merge_ftgs`] directly on each daemon's
/// already-merged rows ("distribution is recursion").
pub fn ftgs_scan_merge(
    shards: &[(&dyn Shard, &GroupLookup)],
    group_by: &[&str],
    stats: &[StatSpec],
) -> Result<Vec<FtgsRow>> {
    // par_iter preserves input order, so `per_shard` is positionally
    // identical to the sequential form — and `merge_ftgs` is order-
    // independent regardless — so the branch can't change the result.
    let per_shard: Vec<Vec<FtgsRow>> = if runtime::rayon_ready() {
        runtime::run_cpu(|| {
            shards
                .par_iter()
                .map(|(shard, groups)| ftgs_scan(*shard, groups, group_by, stats))
                .collect::<Result<Vec<_>>>()
        })?
    } else {
        shards
            .iter()
            .map(|(shard, groups)| ftgs_scan(*shard, groups, group_by, stats))
            .collect::<Result<Vec<_>>>()?
    };
    Ok(merge_ftgs(per_shard, group_by, stats))
}

/// N-way sorted merge of per-shard FTGS results into one sorted stream,
/// combining the stats of rows that share a `(field, term, group)` key
/// via [`StatSpec::combine`]. Each input vec is already sorted by the
/// canonical key (that's [`ftgs_scan`]'s emission order), so this is a
/// k-way merge, not a re-sort. The same function serves the cross-daemon
/// layer: its inputs are just another set of sorted `Vec<FtgsRow>`.
#[must_use]
pub fn merge_ftgs(
    per_shard: Vec<Vec<FtgsRow>>,
    group_by: &[&str],
    stats: &[StatSpec],
) -> Vec<FtgsRow> {
    // Fields order by their position in `group_by` (not field-name
    // lexical order); within a field, by raw term bytes; then by group.
    let rank = |field: &str| {
        group_by
            .iter()
            .position(|f| *f == field)
            .unwrap_or(usize::MAX)
    };

    let mut iters: Vec<std::vec::IntoIter<FtgsRow>> =
        per_shard.into_iter().map(IntoIterator::into_iter).collect();
    let mut heap = BinaryHeap::new();
    for (shard, it) in iters.iter_mut().enumerate() {
        if let Some(row) = it.next() {
            heap.push(Reverse(HeapEntry {
                rank: rank(&row.field),
                shard,
                row,
            }));
        }
    }

    let mut out: Vec<FtgsRow> = Vec::new();
    while let Some(Reverse(HeapEntry { shard, row, .. })) = heap.pop() {
        // Refill from the same shard before consuming `row`.
        if let Some(next) = iters[shard].next() {
            heap.push(Reverse(HeapEntry {
                rank: rank(&next.field),
                shard,
                row: next,
            }));
        }
        // Equal keys come out consecutively, so folding into the last
        // emitted row is enough to combine across all shards.
        match out.last_mut() {
            Some(last)
                if last.field == row.field && last.term == row.term && last.group == row.group =>
            {
                combine_stats(&mut last.stats, &row.stats, stats);
            }
            _ => out.push(row),
        }
    }
    out
}

/// Heap entry for [`merge_ftgs`], ordered by the canonical merge key:
/// field rank, then raw term bytes, then group (shard breaks ties so the
/// order is total). `rank` is precomputed since `Ord` can't see
/// `group_by`.
struct HeapEntry {
    rank: usize,
    shard: usize,
    row: FtgsRow,
}

impl HeapEntry {
    fn key(&self) -> (usize, &[u8], u32, usize) {
        (self.rank, &self.row.term, self.row.group, self.shard)
    }
}

impl PartialEq for HeapEntry {
    fn eq(&self, other: &Self) -> bool {
        self.key() == other.key()
    }
}
impl Eq for HeapEntry {}
impl Ord for HeapEntry {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        self.key().cmp(&other.key())
    }
}
impl PartialOrd for HeapEntry {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}

#[cfg(test)]
mod tests;
