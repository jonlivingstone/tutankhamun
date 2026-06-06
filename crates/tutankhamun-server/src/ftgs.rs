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
//! Output ordering is deterministic, which the cross-shard merge will
//! rely on: fields in the caller's order, terms ascending (the FST is
//! sorted), groups ascending (the [`BitTree`] drains low-to-high).
//!
//! ## Extending the stats
//!
//! Stats live behind the [`Stat`] enum so new operators are additive.
//! This pass implements the cheap scalar operators (sum / count / min /
//! max), each one `i64` per group. The heavier mergeable sketches —
//! `approx_percentile` (t-digest), `approx_count_distinct` (HLL),
//! `theta` (§3.1) — arrive as new variants whose accumulator owns its
//! own storage (e.g. `Vec<TDigest>`); the critical loop and `FtgsRow`
//! do not change. The cross-shard merge (next §2.6 item) adds a
//! `merge` method to the same seam.

use anyhow::{Context as _, Result, bail};

use crate::bit_tree::BitTree;
use crate::group_lookup::GroupLookup;
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

/// One output row: the requested stats for a single `(field, term,
/// group)`. `stats[i]` is the finalized value of the i-th [`StatSpec`]
/// over the docs that carry `term` and fall in `group`. Every stat we
/// emit today finalizes to one `i64`; a binary-valued sketch (raw
/// `theta`, §3.1) widens this to a stat-value enum when it lands.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FtgsRow {
    pub field: String,
    pub term: String,
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
        let kind = require_field(metadata, field, &[FieldKind::String, FieldKind::Int])?.kind;
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
            let term = render_term(kind, &key);
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

/// Render a raw FST key as its display term: `Int` keys are the
/// order-preserving 8-byte encoding, everything else a UTF-8 string.
fn render_term(kind: FieldKind, key: &[u8]) -> String {
    match kind {
        FieldKind::Int => decode_int_key(key).to_string(),
        _ => utf8_term(key),
    }
}

#[cfg(test)]
mod tests;
