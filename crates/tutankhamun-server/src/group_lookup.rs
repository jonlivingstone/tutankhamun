//! Group lookup — the per-doc → group-ID map a session mutates across
//! a query (§2.5).
//!
//! Group 0 is the "filtered out" group: a doc in group 0 contributes
//! to no result. A fresh lookup over a shard puts every doc in group 1
//! (the `Constant` backing, zero bytes per doc); regroups widen the
//! backing in place as group cardinality grows. The backing is
//! specialized by the widest group ID it must hold so many concurrent
//! sessions stay within bounded memory, and is a Rust `enum` so the
//! compiler checks each backing swap.
//!
//! | Backing    | Max group ID | Bytes / doc |
//! |------------|--------------|-------------|
//! | `Constant` | n/a (one group, immutable) | 0 |
//! | `BitSet`   | 1            | ⅛ |
//! | `Byte`     | 255          | 1 |
//! | `U16`      | 65 535       | 2 |
//! | `U32`      | 4 294 967 295 | 4 |

use crate::bit_tree::BitTree;

/// Widest group ID the `BitSet` backing can store (groups 0 and 1).
const BITSET_MAX: u32 = 1;
/// Widest group ID the `Byte` backing can store.
const BYTE_MAX: u32 = u8::MAX as u32;
/// Widest group ID the `U16` backing can store.
const U16_MAX: u32 = u16::MAX as u32;

/// Per-doc group assignment, specialized by current cardinality.
#[derive(Debug)]
pub struct GroupLookup {
    backing: Backing,
    /// Number of docs — the valid `doc` range is `[0, len)`.
    len: usize,
    /// `1 + the largest group ID assigned` (group 0 counts), i.e. the
    /// number of distinct group slots. Maintained on `set`.
    num_groups: u32,
}

#[derive(Debug)]
enum Backing {
    /// Every doc in one group; immutable, so any `set` upgrades it.
    Constant(u32),
    /// One bit per doc: group 0 (clear) or 1 (set). `ceil(len / 64)`
    /// words.
    BitSet(Vec<u64>),
    Byte(Vec<u8>),
    U16(Vec<u16>),
    U32(Vec<u32>),
}

impl GroupLookup {
    /// All `len` docs in group 1 — the initial state of a session over
    /// a shard, before any regroup.
    #[must_use]
    pub fn all_in_one_group(len: usize) -> Self {
        Self::constant(len, 1)
    }

    /// All `len` docs in `group`. `Constant`: zero per-doc bytes.
    #[must_use]
    pub fn constant(len: usize, group: u32) -> Self {
        Self {
            backing: Backing::Constant(group),
            len,
            num_groups: group.saturating_add(1),
        }
    }

    /// Number of docs.
    #[must_use]
    pub fn len(&self) -> usize {
        self.len
    }

    /// Whether the lookup covers zero docs.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.len == 0
    }

    /// Number of distinct group slots, including group 0.
    #[must_use]
    pub fn num_groups(&self) -> u32 {
        self.num_groups
    }

    /// The group of `doc`.
    #[must_use]
    pub fn get(&self, doc: usize) -> u32 {
        match &self.backing {
            Backing::Constant(c) => *c,
            Backing::BitSet(words) => ((words[doc >> 6] >> (doc & 0x3F)) & 1) as u32,
            Backing::Byte(v) => u32::from(v[doc]),
            Backing::U16(v) => u32::from(v[doc]),
            Backing::U32(v) => v[doc],
        }
    }

    /// Assign `doc` to `group`, widening the backing in place first if
    /// `group` (or the immutable `Constant`) can't otherwise be held.
    pub fn set(&mut self, doc: usize, group: u32) {
        assert!(doc < self.len, "doc {doc} out of range (len {})", self.len);
        self.ensure_capacity(group);
        write_raw(&mut self.backing, doc, group);
        self.num_groups = self.num_groups.max(group.saturating_add(1));
    }

    /// Widen the backing in place so it can store group IDs up to and
    /// including `max_group_id`, preserving every doc's current group.
    /// A no-op when the current mutable backing already suffices;
    /// `Constant` always upgrades, since it can't be mutated. The
    /// backing only grows; it never shrinks back down.
    pub fn ensure_capacity(&mut self, max_group_id: u32) {
        let needs_upgrade = match &self.backing {
            Backing::Constant(_) => true,
            Backing::BitSet(_) => max_group_id > BITSET_MAX,
            Backing::Byte(_) => max_group_id > BYTE_MAX,
            Backing::U16(_) => max_group_id > U16_MAX,
            Backing::U32(_) => false,
        };
        if !needs_upgrade {
            return;
        }
        // From `Constant`, the new backing must hold both the existing
        // constant group and the requested ID.
        let required = match &self.backing {
            Backing::Constant(c) => max_group_id.max(*c),
            _ => max_group_id,
        };
        let mut next = empty_backing(required, self.len);
        for doc in 0..self.len {
            write_raw(&mut next, doc, self.get(doc));
        }
        self.backing = next;
    }

    /// FTGS dispatch (§2.6): for each doc in `doc_ids`, mark its group
    /// in `groups_seen`, so the caller can drain the sorted, deduped
    /// set of groups this term's postings touched. Docs in group 0
    /// (filtered out) are skipped. Per-group stat accumulation lives in
    /// the FTGS loop, not here. `groups_seen` must be sized to at least
    /// [`num_groups`](Self::num_groups).
    pub fn next_group_callback(&self, doc_ids: &[u32], groups_seen: &mut BitTree) {
        for &doc in doc_ids {
            let group = self.get(doc as usize);
            if group != 0 {
                groups_seen.set(group as usize);
            }
        }
    }

    /// Bytes held by the per-doc backing — what a session reports to
    /// its memory budget (§2.2). `Constant` holds none.
    #[must_use]
    pub fn memory_used(&self) -> usize {
        match &self.backing {
            Backing::Constant(_) => 0,
            Backing::BitSet(words) => words.len() * 8,
            Backing::Byte(v) => v.len(),
            Backing::U16(v) => v.len() * 2,
            Backing::U32(v) => v.len() * 4,
        }
    }

    /// The current backing's name, for `/status` and tests.
    #[must_use]
    pub fn backing_name(&self) -> &'static str {
        match &self.backing {
            Backing::Constant(_) => "constant",
            Backing::BitSet(_) => "bitset",
            Backing::Byte(_) => "byte",
            Backing::U16(_) => "u16",
            Backing::U32(_) => "u32",
        }
    }
}

/// A zeroed mutable backing wide enough for group IDs up to `max_id`,
/// over `len` docs.
fn empty_backing(max_id: u32, len: usize) -> Backing {
    if max_id <= BITSET_MAX {
        Backing::BitSet(vec![0u64; len.div_ceil(64)])
    } else if max_id <= BYTE_MAX {
        Backing::Byte(vec![0u8; len])
    } else if max_id <= U16_MAX {
        Backing::U16(vec![0u16; len])
    } else {
        Backing::U32(vec![0u32; len])
    }
}

/// Write `group` for `doc` into a mutable backing without bounds or
/// capacity checks — callers guarantee both. Panics on `Constant`,
/// which is immutable.
// The `as u8` / `as u16` casts can't truncate: a caller reaches the
// Byte/U16 arm only after `ensure_capacity` sized the backing to hold
// `group`, so `group` is already within the narrower type's range.
#[allow(clippy::cast_possible_truncation)]
fn write_raw(backing: &mut Backing, doc: usize, group: u32) {
    match backing {
        Backing::Constant(_) => unreachable!("constant backing is immutable"),
        Backing::BitSet(words) => {
            let bit = 1u64 << (doc & 0x3F);
            if group == 1 {
                words[doc >> 6] |= bit;
            } else {
                words[doc >> 6] &= !bit;
            }
        }
        Backing::Byte(v) => v[doc] = group as u8,
        Backing::U16(v) => v[doc] = group as u16,
        Backing::U32(v) => v[doc] = group,
    }
}

#[cfg(test)]
mod tests {
    use super::{BITSET_MAX, BYTE_MAX, GroupLookup, U16_MAX};
    use crate::bit_tree::BitTree;

    #[test]
    fn fresh_lookup_is_constant_group_one() {
        let gl = GroupLookup::all_in_one_group(10);
        assert_eq!(gl.backing_name(), "constant");
        assert_eq!(gl.num_groups(), 2);
        assert_eq!(gl.memory_used(), 0);
        assert!((0..10).all(|d| gl.get(d) == 1));
    }

    #[test]
    fn set_upgrades_constant_to_bitset_and_tracks_num_groups() {
        let mut gl = GroupLookup::all_in_one_group(100);
        gl.set(3, 0);
        assert_eq!(gl.backing_name(), "bitset");
        assert_eq!(gl.get(3), 0);
        // Docs not yet touched keep the original constant group 1.
        assert_eq!(gl.get(0), 1);
        assert_eq!(gl.num_groups(), 2);
    }

    #[test]
    fn backing_widens_at_each_cardinality_threshold() {
        let mut gl = GroupLookup::all_in_one_group(4);
        gl.set(0, BITSET_MAX); // still fits a bitset
        assert_eq!(gl.backing_name(), "bitset");
        gl.set(1, 2); // > 1 → byte
        assert_eq!(gl.backing_name(), "byte");
        gl.set(2, BYTE_MAX + 1); // > 255 → u16
        assert_eq!(gl.backing_name(), "u16");
        gl.set(3, U16_MAX + 1); // > 65535 → u32
        assert_eq!(gl.backing_name(), "u32");
        assert_eq!(gl.num_groups(), U16_MAX + 2);
    }

    #[test]
    fn upgrade_preserves_every_docs_group() {
        let mut gl = GroupLookup::all_in_one_group(5);
        gl.set(0, 1);
        gl.set(1, 0);
        gl.set(2, 300); // forces bitset → u16, carrying docs 0,1 and the constant
        assert_eq!(gl.backing_name(), "u16");
        assert_eq!(gl.get(0), 1);
        assert_eq!(gl.get(1), 0);
        assert_eq!(gl.get(2), 300);
        assert_eq!(gl.get(3), 1); // untouched: original constant group 1
        assert_eq!(gl.get(4), 1);
    }

    #[test]
    fn ensure_capacity_presizes_without_changing_values() {
        let mut gl = GroupLookup::all_in_one_group(64);
        gl.ensure_capacity(1000);
        assert_eq!(gl.backing_name(), "u16");
        assert!((0..64).all(|d| gl.get(d) == 1));
        // num_groups reflects assigned groups, not reserved capacity.
        assert_eq!(gl.num_groups(), 2);
    }

    #[test]
    fn memory_used_matches_backing_width() {
        let mut gl = GroupLookup::all_in_one_group(1000);
        gl.set(0, 1);
        assert_eq!(gl.memory_used(), 1000usize.div_ceil(64) * 8); // bitset words
        gl.set(0, 2);
        assert_eq!(gl.memory_used(), 1000); // byte
        gl.set(0, 1000);
        assert_eq!(gl.memory_used(), 2000); // u16
        gl.set(0, 100_000);
        assert_eq!(gl.memory_used(), 4000); // u32
    }

    #[test]
    fn next_group_callback_marks_groups_and_skips_filtered_docs() {
        let mut gl = GroupLookup::all_in_one_group(6);
        gl.set(0, 0); // filtered out
        gl.set(1, 2);
        gl.set(2, 2); // duplicate group
        gl.set(3, 5);
        gl.set(4, 0); // filtered out
        gl.set(5, 1);

        let mut seen = BitTree::new(gl.num_groups() as usize);
        gl.next_group_callback(&[0, 1, 2, 3, 4, 5], &mut seen);
        // Groups 1, 2, 5 present; group 0 never marked.
        assert_eq!(seen.drain(), vec![1, 2, 5]);
    }
}
