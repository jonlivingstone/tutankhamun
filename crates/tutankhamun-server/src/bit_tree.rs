//! `BitTree` — a hierarchical bitset for marking a sparse set of
//! non-negative integers and draining them in ascending order.
//!
//! Ported from Imhotep's `BitTree`. The FTGS group lookup (§2.5) marks
//! the groups a term's postings fall into, then the caller drains them
//! sorted to drive `group_stats`; the §2.6 shard merge reuses the same
//! structure. Each level above the leaf summarises 64 words of the
//! level below — one bit per child word — so draining costs
//! O(set bits) rather than O(universe): empty regions are skipped a
//! whole level at a time.

/// A hierarchical bitset over `[0, size)`.
#[derive(Debug)]
pub struct BitTree {
    /// One bitset per level. `levels[0]` is the leaf (one bit per
    /// representable index); each higher level holds one bit per
    /// 64-bit word of the level below. The top level is always a
    /// single word, so a drain can start from a known root.
    levels: Vec<Vec<u64>>,
}

impl BitTree {
    /// A tree that can mark indices in `[0, size)`. `size` is rounded
    /// up internally; marking an index `>= size` panics on the
    /// out-of-bounds word access.
    #[must_use]
    pub fn new(size: usize) -> Self {
        // Level count mirrors Imhotep: floor(log2(size-1))/6 + 1. The
        // `/6` is one level per 64× (2⁶) fan-out; the `size <= 1` guard
        // avoids `ilog2(0)`. This guarantees the top level collapses to
        // a single word.
        let num_levels = if size <= 1 {
            1
        } else {
            (size - 1).ilog2() as usize / 6 + 1
        };
        let mut levels = Vec::with_capacity(num_levels);
        let mut words = size.max(1);
        for _ in 0..num_levels {
            words = words.div_ceil(64);
            levels.push(vec![0u64; words]);
        }
        Self { levels }
    }

    /// Mark `index`. Setting an index more than once is idempotent.
    pub fn set(&mut self, index: usize) {
        let mut idx = index;
        for level in &mut self.levels {
            let word = idx >> 6;
            level[word] |= 1u64 << (idx & 0x3F);
            idx = word;
        }
    }

    /// Whether `index` is currently marked.
    #[must_use]
    pub fn get(&self, index: usize) -> bool {
        self.levels[0][index >> 6] & (1u64 << (index & 0x3F)) != 0
    }

    /// Append every marked index to `out` in ascending order and reset
    /// the tree to empty. The hierarchy lets this skip empty 64-index
    /// spans without visiting them.
    pub fn drain_into(&mut self, out: &mut Vec<u32>) {
        let top = self.levels.len() - 1;
        let mut depth = top;
        let mut index = 0usize;
        loop {
            // Ascend past fully-drained words; a zero word at the top
            // means the whole tree is empty.
            while self.levels[depth][index] == 0 {
                if depth == top {
                    return;
                }
                depth += 1;
                index >>= 6;
            }
            // Descend to a non-empty leaf word, clearing each summary
            // bit on the way down — the leaf it points at is drained in
            // full before we re-ascend, so the bit is correctly 0.
            while depth != 0 {
                let word = self.levels[depth][index];
                let lsb = word & word.wrapping_neg();
                self.levels[depth][index] ^= lsb;
                depth -= 1;
                index = (index << 6) + lsb.trailing_zeros() as usize;
            }
            // Emit the leaf word's indices ascending (lowest bit first).
            // The `as u32` can't truncate: every emitted value is an
            // index in `[0, size)`, and `size` is a group count that
            // fits `u32` by construction.
            #[allow(clippy::cast_possible_truncation)]
            while self.levels[0][index] != 0 {
                let word = self.levels[0][index];
                let lsb = word & word.wrapping_neg();
                self.levels[0][index] ^= lsb;
                out.push(((index << 6) + lsb.trailing_zeros() as usize) as u32);
            }
            if self.levels.len() == 1 {
                return;
            }
            // Re-examine the parent of the leaf just drained.
            depth = 1;
            index >>= 6;
        }
    }

    /// Drain every marked index, ascending, into a fresh `Vec`.
    #[must_use]
    pub fn drain(&mut self) -> Vec<u32> {
        let mut out = Vec::new();
        self.drain_into(&mut out);
        out
    }

    /// Bytes held by the level bitsets.
    #[must_use]
    pub fn memory_used(&self) -> usize {
        self.levels.iter().map(|l| l.len() * 8).sum()
    }
}

#[cfg(test)]
mod tests {
    use super::BitTree;
    use std::collections::BTreeSet;

    #[test]
    fn top_level_is_always_a_single_word() {
        for size in [1usize, 2, 63, 64, 65, 4096, 4097, 1_000_000] {
            let tree = BitTree::new(size);
            let top = tree.levels.last().unwrap();
            assert_eq!(top.len(), 1, "size {size} should collapse to one top word");
        }
    }

    #[test]
    fn set_and_get_roundtrip_across_word_boundaries() {
        let mut tree = BitTree::new(200);
        for &i in &[0usize, 1, 63, 64, 65, 127, 128, 199] {
            assert!(!tree.get(i));
            tree.set(i);
            assert!(tree.get(i));
        }
        assert!(!tree.get(2));
    }

    #[test]
    fn drain_yields_marked_indices_ascending_then_empties() {
        let mut tree = BitTree::new(5000);
        for &i in &[4096usize, 7, 7, 64, 0, 4095, 130] {
            tree.set(i);
        }
        assert_eq!(tree.drain(), vec![0, 7, 64, 130, 4095, 4096]);
        // Draining again yields nothing — the tree reset itself.
        assert_eq!(tree.drain(), Vec::<u32>::new());
    }

    #[test]
    fn drain_matches_a_reference_set_for_scattered_marks() {
        // Deterministic spread of indices across several levels.
        let size = 300_000;
        let mut tree = BitTree::new(size);
        let mut reference = BTreeSet::new();
        let mut x = 1usize;
        for _ in 0..5000 {
            x = (x.wrapping_mul(1_103_515_245).wrapping_add(12_345)) % size;
            tree.set(x);
            reference.insert(u32::try_from(x).unwrap());
        }
        let drained = tree.drain();
        let expected: Vec<u32> = reference.into_iter().collect();
        assert_eq!(drained, expected);
    }
}
