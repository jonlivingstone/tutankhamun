//! Mergeable sketches for approximate aggregations (§3.1). Each is a
//! stat-stack value: it accumulates per-doc, merges across shards in the
//! FTGS merge path, and finalizes to an estimate at output.

use std::collections::BTreeSet;
use std::collections::hash_map::DefaultHasher;
use std::hash::{BuildHasherDefault, Hasher};

use anyhow::{Result, ensure};
use hyperloglogplus::{HyperLogLog, HyperLogLogPlus};
use tdigest::TDigest as TDigestImpl;

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

/// A `t-digest` quantile sketch over `f64` values — the backing for
/// `approx_percentile`. Mergeable across shards (centroids merge) and
/// queried for any quantile at finalize. The crate's API is batch (build
/// from a value buffer), not per-value, so accumulation collects values
/// and constructs the digest once at the merge edge.
#[derive(Clone, Debug)]
pub struct TDigest(TDigestImpl);

impl TDigest {
    /// Centroids retained — the accuracy/size trade-off (~1 % quantile
    /// error, tighter at the tails). Matches the crate's default.
    pub const DEFAULT_MAX_SIZE: usize = 100;

    /// Build a digest from a batch of values, keeping up to `max_size`
    /// centroids.
    #[must_use]
    pub fn from_values(values: Vec<f64>, max_size: usize) -> Self {
        TDigest(TDigestImpl::new_with_size(max_size).merge_unsorted(values))
    }

    /// Merge `other`'s centroids into `self`. `self`'s digest moves into
    /// the merge (only `other` is borrowed, so it's cloned).
    pub fn merge(&mut self, other: &TDigest) {
        self.0 = TDigestImpl::merge_digests(vec![std::mem::take(&mut self.0), other.0.clone()]);
    }

    /// Estimate the value at quantile `q` (in `[0, 1]`).
    #[must_use]
    pub fn quantile(&self, q: f64) -> f64 {
        self.0.estimate_quantile(q)
    }
}

/// Hash bytes to a `u64` with the fixed-seed [`DefaultHasher`], so a value
/// hashes identically across shards (and between the int and string
/// paths) — the precondition for merging sketches.
fn theta_hash(bytes: &[u8]) -> u64 {
    let mut h = DefaultHasher::new();
    h.write(bytes);
    h.finish()
}

/// 2^64 as `f64` — the denominator that normalizes a `u64` hash / threshold
/// into `[0, 1)`.
#[allow(clippy::cast_precision_loss)]
const TWO_POW_64: f64 = (u64::MAX as f64) + 1.0;

/// A KMV (k-minimum-values) theta sketch — the backing for `theta` /
/// `theta_intersect`. Like `Hll` it estimates distinct counts and merges
/// by union, but it *also* supports intersection (cohort overlap), which
/// HLL can't. Keeps the `k` smallest distinct hashes; the threshold
/// `theta_u64` is the smallest *excluded* hash (`u64::MAX` until full), so
/// the retained hashes are exactly those `< theta_u64`. Exact in the
/// not-full regime.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ThetaSketch {
    k: usize,
    theta_u64: u64,
    hashes: BTreeSet<u64>,
}

impl ThetaSketch {
    /// Nominal entries: ~1.6 % relative error (1/√k), the HLL accuracy tier.
    pub const DEFAULT_NOMINAL: usize = 4096;
    const VERSION: u8 = 1;

    /// An empty sketch retaining up to `nominal` (rounded up to a power of
    /// two) smallest hashes.
    #[must_use]
    pub fn with_nominal(nominal: usize) -> Self {
        ThetaSketch {
            k: nominal.max(1).next_power_of_two(),
            theta_u64: u64::MAX,
            hashes: BTreeSet::new(),
        }
    }

    pub fn insert(&mut self, value: i64) {
        self.insert_hash(theta_hash(&value.to_le_bytes()));
    }

    pub fn insert_bytes(&mut self, value: &[u8]) {
        self.insert_hash(theta_hash(value));
    }

    /// Retain `hash` if below the threshold; once `k+1` distinct hashes are
    /// held, drop the largest and lower the threshold to it (so the `k`
    /// smallest remain, all strictly below the new threshold).
    fn insert_hash(&mut self, hash: u64) {
        if hash >= self.theta_u64 {
            return;
        }
        if self.hashes.insert(hash) && self.hashes.len() > self.k {
            let largest = self.hashes.pop_last().expect("non-empty after insert");
            self.theta_u64 = largest;
        }
    }

    /// Union `other` into `self` (distinct count of the combined sets):
    /// take the lower threshold, keep the hashes below it from either side,
    /// then trim to the `k` smallest.
    pub fn union(&mut self, other: &ThetaSketch) {
        self.k = self.k.min(other.k);
        self.theta_u64 = self.theta_u64.min(other.theta_u64);
        let theta = self.theta_u64;
        let mut merged: BTreeSet<u64> = std::mem::take(&mut self.hashes);
        merged.retain(|&h| h < theta);
        merged.extend(other.hashes.iter().copied().filter(|&h| h < theta));
        // Trim to the k smallest: each popped value is the unique max, so
        // the rest already satisfy `< theta` — no re-scan needed.
        while merged.len() > self.k {
            self.theta_u64 = merged.pop_last().expect("non-empty while over capacity");
        }
        self.hashes = merged;
    }

    /// The intersection sketch of `a` and `b` (cohort overlap): the hashes
    /// present in both, below the lower threshold. `estimate()` on it gives
    /// the estimated overlap size.
    #[must_use]
    pub fn intersect(a: &ThetaSketch, b: &ThetaSketch) -> ThetaSketch {
        let theta_u64 = a.theta_u64.min(b.theta_u64);
        let hashes: BTreeSet<u64> = a
            .hashes
            .intersection(&b.hashes)
            .copied()
            .filter(|&h| h < theta_u64)
            .collect();
        ThetaSketch {
            k: a.k.min(b.k),
            theta_u64,
            hashes,
        }
    }

    /// Estimated distinct count: exact while below `k` distinct (threshold
    /// still `u64::MAX`), else the KMV estimator `retained / (theta/2^64)`.
    #[must_use]
    #[allow(
        clippy::cast_precision_loss,
        clippy::cast_possible_truncation,
        clippy::cast_sign_loss,
        clippy::cast_possible_wrap
    )]
    pub fn estimate(&self) -> i64 {
        if self.theta_u64 == u64::MAX {
            return self.hashes.len() as i64;
        }
        let theta = self.theta_u64 as f64 / TWO_POW_64;
        (self.hashes.len() as f64 / theta).round() as i64
    }

    /// Heap bytes held by the retained hashes — for accumulator accounting.
    #[must_use]
    pub fn estimated_size(&self) -> usize {
        std::mem::size_of::<Self>() + self.hashes.len() * std::mem::size_of::<u64>()
    }

    /// Serialize as `VERSION | k | theta_u64 | count | hashes…` (little-
    /// endian), the wire form returned to clients as Arrow `Binary`.
    #[must_use]
    pub fn to_bytes(&self) -> Vec<u8> {
        let mut out = Vec::with_capacity(17 + self.hashes.len() * 8);
        out.push(Self::VERSION);
        out.extend_from_slice(&u32::try_from(self.k).unwrap_or(u32::MAX).to_le_bytes());
        out.extend_from_slice(&self.theta_u64.to_le_bytes());
        out.extend_from_slice(
            &u32::try_from(self.hashes.len())
                .unwrap_or(u32::MAX)
                .to_le_bytes(),
        );
        for &h in &self.hashes {
            out.extend_from_slice(&h.to_le_bytes());
        }
        out
    }

    /// Parse [`to_bytes`](Self::to_bytes), validating the version, lengths,
    /// and that the hashes are ascending and below the threshold.
    pub fn from_bytes(bytes: &[u8]) -> Result<Self> {
        ensure!(
            bytes.len() >= 17,
            "theta sketch too short: {} bytes",
            bytes.len()
        );
        ensure!(
            bytes[0] == Self::VERSION,
            "unknown theta sketch version {}",
            bytes[0]
        );
        let k = u32::from_le_bytes(bytes[1..5].try_into().unwrap()) as usize;
        let theta_u64 = u64::from_le_bytes(bytes[5..13].try_into().unwrap());
        let count = u32::from_le_bytes(bytes[13..17].try_into().unwrap()) as usize;
        ensure!(
            bytes.len() == 17 + count * 8,
            "theta sketch length mismatch"
        );
        ensure!(
            k.is_power_of_two(),
            "theta sketch k {k} is not a power of two"
        );
        let mut hashes = BTreeSet::new();
        let mut prev: Option<u64> = None;
        for chunk in bytes[17..].chunks_exact(8) {
            let h = u64::from_le_bytes(chunk.try_into().unwrap());
            ensure!(h < theta_u64, "theta sketch hash >= threshold");
            ensure!(
                prev.is_none_or(|p| p < h),
                "theta sketch hashes not ascending"
            );
            prev = Some(h);
            hashes.insert(h);
        }
        if count != 0 {
            ensure!(hashes.len() == count, "theta sketch duplicate hashes");
        }
        Ok(ThetaSketch {
            k,
            theta_u64,
            hashes,
        })
    }
}

impl Default for ThetaSketch {
    fn default() -> Self {
        Self::with_nominal(Self::DEFAULT_NOMINAL)
    }
}

#[cfg(test)]
mod theta_tests {
    use super::ThetaSketch;

    /// k from a sketch's serialized form (k is private).
    fn k_of(s: &ThetaSketch) -> usize {
        u32::from_le_bytes(s.to_bytes()[1..5].try_into().unwrap()) as usize
    }

    fn of(values: impl IntoIterator<Item = i64>) -> ThetaSketch {
        let mut s = ThetaSketch::default();
        for v in values {
            s.insert(v);
        }
        s
    }

    #[test]
    fn exact_below_k() {
        let mut s = ThetaSketch::default();
        for v in 0..100 {
            s.insert(v);
        }
        for v in 0..100 {
            s.insert(v); // duplicates: no-op
        }
        assert_eq!(s.estimate(), 100);
    }

    #[test]
    fn estimate_within_band_above_k() {
        let s = of(0..100_000);
        let e = s.estimate();
        assert!((95_000..=105_000).contains(&e), "estimate {e}");
    }

    #[test]
    fn nominal_rounds_to_power_of_two() {
        assert_eq!(k_of(&ThetaSketch::with_nominal(3000)), 4096);
        assert_eq!(k_of(&ThetaSketch::with_nominal(4096)), 4096);
    }

    #[test]
    fn union_dedups_and_is_order_independent() {
        let a = of(0..6_000);
        let b = of(4_000..10_000);
        let mut ab = a.clone();
        ab.union(&b);
        let mut ba = b.clone();
        ba.union(&a);
        // union cardinality = |0..10000| = 10000.
        assert!(
            (9_000..=11_000).contains(&ab.estimate()),
            "union {}",
            ab.estimate()
        );
        assert_eq!(ab.estimate(), ba.estimate(), "order independent");

        let mut a_empty = a.clone();
        a_empty.union(&ThetaSketch::default());
        assert_eq!(a_empty.estimate(), a.estimate(), "union with empty is self");
    }

    #[test]
    fn intersect_overlap() {
        // 0..10000 ∩ 5000..15000 → overlap 5000.
        let i = ThetaSketch::intersect(&of(0..10_000), &of(5_000..15_000));
        assert!(
            (4_000..=6_000).contains(&i.estimate()),
            "overlap {}",
            i.estimate()
        );

        // disjoint → ~0.
        let d = ThetaSketch::intersect(&of(0..5_000), &of(10_000..15_000));
        assert!(d.estimate() < 300, "disjoint {}", d.estimate());

        // with empty → 0.
        let e = ThetaSketch::intersect(&of(0..10_000), &ThetaSketch::default());
        assert_eq!(e.estimate(), 0);
    }

    #[test]
    fn serialization_roundtrip() {
        let s = of(0..50_000);
        let bytes = s.to_bytes();
        let back = ThetaSketch::from_bytes(&bytes).unwrap();
        assert_eq!(s, back);
        assert_eq!(s.estimate(), back.estimate());

        // empty round-trips.
        let empty = ThetaSketch::default();
        assert_eq!(ThetaSketch::from_bytes(&empty.to_bytes()).unwrap(), empty);

        // rejects a bad version byte and a truncated buffer.
        let mut bad = bytes.clone();
        bad[0] = 99;
        assert!(ThetaSketch::from_bytes(&bad).is_err());
        assert!(ThetaSketch::from_bytes(&bytes[..10]).is_err());
    }
}
