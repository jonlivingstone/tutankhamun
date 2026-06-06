//! Mergeable sketches for approximate aggregations (§3.1). Each is a
//! stat-stack value: it accumulates per-doc, merges across shards in the
//! FTGS merge path, and finalizes to an estimate at output.

use std::collections::hash_map::DefaultHasher;
use std::hash::BuildHasherDefault;

use hyperloglogplus::{HyperLogLog, HyperLogLogPlus};

/// A deterministic, fixed-seed hasher. Random per-instance seeds (e.g.
/// `RandomState`) would make two shards' sketches hash the same value
/// differently, so their registers couldn't be merged.
type FixedHasher = BuildHasherDefault<DefaultHasher>;

/// `HyperLogLog` distinct-count sketch over raw bytes — the backing for
/// `approx_count_distinct`. Both `i64` columns (hashed as their
/// little-endian bytes) and `String` index terms feed the same sketch,
/// so a value hashes identically wherever it appears. Mergeable across
/// shards (registers union) and finalized to an estimated cardinality.
/// Exact in the sparse (small-cardinality) regime.
#[derive(Clone, Debug)]
pub struct Hll(HyperLogLogPlus<Vec<u8>, FixedHasher>);

impl Hll {
    /// ~0.8 % standard error (16384 registers); the sparse regime below
    /// a few thousand distinct values is exact.
    const PRECISION: u8 = 14;

    pub fn insert(&mut self, value: i64) {
        self.0.insert(value.to_le_bytes().as_slice());
    }

    pub fn insert_bytes(&mut self, value: &[u8]) {
        self.0.insert(value);
    }

    /// Union `other`'s registers into `self`. Both use the same
    /// precision and hasher by construction, so this cannot fail.
    pub fn merge(&mut self, other: &Hll) {
        self.0
            .merge(&other.0)
            .expect("HLL merge: identical precision by construction");
    }

    /// Estimated distinct count. Estimated from a clone because the
    /// underlying `count` takes `&mut self` (it may densify the sparse
    /// representation), keeping the sketch reusable and this `&self`.
    #[must_use]
    #[allow(clippy::cast_possible_truncation, clippy::cast_sign_loss)]
    pub fn estimate(&self) -> i64 {
        // Cardinality is non-negative and far below i64::MAX.
        self.0.clone().count().round() as i64
    }
}

impl Default for Hll {
    fn default() -> Self {
        Hll(
            HyperLogLogPlus::<Vec<u8>, _>::new(Self::PRECISION, FixedHasher::default())
                .expect("HLL precision 14 is in range"),
        )
    }
}
