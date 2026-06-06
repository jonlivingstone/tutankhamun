//! FTGS — the Field-Term-Group-Stat scan (§2.6).
//!
//! The engine's single aggregation primitive: every analytical
//! `GROUP BY` ultimately runs through this loop. Given a per-doc group
//! assignment ([`GroupLookup`], set by prior regroups) and a set of
//! group-by fields, it walks each field's term dictionary and, for
//! every `(field, term)`, emits the requested stats per group. This is
//! the four-level cursor — field → term → group → stat — preserved
//! from Imhotep.
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
//! ## Extending the stats
//!
//! Stats live behind the [`Stat`] enum (accumulation) and
//! [`StatSpec::combine`] (cross-shard merge) so new operators are
//! additive. This pass implements the cheap scalar operators (sum /
//! count / min / max), each one `i64` per group. The heavier mergeable
//! sketches — `approx_percentile` (t-digest), `approx_count_distinct`
//! (HLL), `theta` (§3.1) — arrive as new variants whose accumulator
//! owns its own storage (e.g. `Vec<TDigest>`); the critical loop and
//! `FtgsRow` do not change.

use std::cmp::Reverse;
use std::collections::BinaryHeap;

use anyhow::{Context as _, Result, bail};
use rayon::prelude::*;

use crate::bit_tree::BitTree;
use crate::group_lookup::GroupLookup;
use crate::runtime;
use crate::shard::{FieldKind, Shard, decode_int_key, require_field, utf8_term};

/// A stat the caller wants accumulated per group, named against the
/// shard's columns. `Sum`/`Min`/`Max` read a `Metric`/`Int` forward
/// column; `Count` needs none.
#[derive(Debug, Clone, Copy)]
pub enum StatSpec<'a> {
    Count,
    Sum(&'a str),
    Min(&'a str),
    Max(&'a str),
}

impl StatSpec<'_> {
    /// Merge two shards' finalized values of this stat for the same
    /// `(field, term, group)`. The cross-shard counterpart to
    /// [`Stat::update`]: additive stats sum, `Min`/`Max` fold. Mergeable
    /// sketch stats (§3.1) combine their own accumulators and widen the
    /// output type, as noted on [`FtgsRow`].
    #[must_use]
    pub fn combine(self, a: i64, b: i64) -> i64 {
        match self {
            StatSpec::Count | StatSpec::Sum(_) => a + b,
            StatSpec::Min(_) => a.min(b),
            StatSpec::Max(_) => a.max(b),
        }
    }
}

/// One output row: the requested stats for a single `(field, term,
/// group)`. `term` is the raw order-preserving FST key — the canonical
/// sort order the merge compares by; render it for display with
/// [`render_term`] and the field's `FieldKind`. `stats[i]` is the
/// finalized value of the i-th [`StatSpec`] over the docs that carry
/// `term` and fall in `group`. Every stat we emit today finalizes to one
/// `i64`; a binary-valued sketch (raw `theta`, §3.1) widens this to a
/// stat-value enum when it lands.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FtgsRow {
    pub field: String,
    pub term: Box<[u8]>,
    pub group: u32,
    pub stats: Vec<i64>,
}

/// Live per-group accumulator for one stat. Each variant owns its own
/// `slots` (one per group) seeded to the operator's identity, so the
/// inner loop is a flat array write and a future sketch variant can
/// pick a different storage type without touching the loop.
enum Stat<'a> {
    Count { slots: Vec<i64> },
    Sum { col: &'a [i64], slots: Vec<i64> },
    Min { col: &'a [i64], slots: Vec<i64> },
    Max { col: &'a [i64], slots: Vec<i64> },
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
        })
    }

    /// Fold `doc`'s value into `group`'s slot.
    fn update(&mut self, group: usize, doc: usize) {
        match self {
            Stat::Count { slots } => slots[group] += 1,
            Stat::Sum { col, slots } => slots[group] += col[doc],
            Stat::Min { col, slots } => slots[group] = slots[group].min(col[doc]),
            Stat::Max { col, slots } => slots[group] = slots[group].max(col[doc]),
        }
    }

    /// Finalized value of `group`'s slot for output.
    fn value(&self, group: usize) -> i64 {
        match self {
            Stat::Count { slots }
            | Stat::Sum { slots, .. }
            | Stat::Min { slots, .. }
            | Stat::Max { slots, .. } => slots[group],
        }
    }

    /// Restore `group`'s slot to the operator's identity, so the buffer
    /// is reusable for the next term. Only the groups a term touched are
    /// reset, so the cost scales with groups seen, not the group count.
    fn reset(&mut self, group: usize) {
        match self {
            Stat::Count { slots } | Stat::Sum { slots, .. } => slots[group] = 0,
            Stat::Min { slots, .. } => slots[group] = i64::MAX,
            Stat::Max { slots, .. } => slots[group] = i64::MIN,
        }
    }
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
                let row_stats = accumulators.iter().map(|s| s.value(g as usize)).collect();
                for stat in &mut accumulators {
                    stat.reset(g as usize);
                }
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
                for (s, spec) in stats.iter().enumerate() {
                    last.stats[s] = spec.combine(last.stats[s], row.stats[s]);
                }
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
