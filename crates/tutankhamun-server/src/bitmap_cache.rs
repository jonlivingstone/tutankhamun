//! Daemon-shared doc-set bitmap cache (§2.8).
//!
//! Every query resolves, per shard, a matched-doc set from the inverted index
//! ([`matched_doc_set`]). Shards are immutable (§2.1), so that result is a pure
//! function of `(filter clauses, shard)` — content-addressable, never stale, and
//! shareable across sessions. This cache keys the realized Roaring bitmap by
//! `(shard identity, normalized clause set, visibility)` and serves two reuse
//! patterns:
//!
//! - **Exact hit** — an identical filter on the same shard returns the cached
//!   bitmap (50 analysts who all start from `country = 'US'` compute it once).
//! - **Monotone narrowing** — a new filter that *adds* conjuncts to a cached one
//!   (`old AND extra`) reuses the cached `old` bitmap and intersects only the
//!   `extra` delta, instead of rescanning. Pushed filters are a flat conjunction,
//!   so `result(new) = result(cached) ∩ result(delta)` exactly.
//!
//! The cache is bounded by a [`SessionMemoryHandle`] sub-budget (a fraction of the
//! §2.2 global budget): each entry holds a [`SessionReservation`], so cached
//! bitmaps compete with session working sets and LRU-evict under pressure. An
//! insert that can't be charged is skipped, never failing the query.
//!
//! Range-*tightening* narrowing (`age>=5` → `age>=7`) and FTGS sub-result caching
//! are future refinements; this caches doc-set bitmaps for the clause-superset
//! case only.

use std::collections::HashMap;
use std::collections::hash_map::DefaultHasher;
use std::hash::{Hash, Hasher};
use std::sync::Arc;
use std::sync::Mutex;
use std::sync::atomic::{AtomicU64, Ordering};

use roaring::RoaringBitmap;

use crate::memory::{SessionMemoryHandle, SessionReservation};
use crate::shard::{DiskShard, FilterResult, matched_doc_set};
use crate::sql::pushdown::PushedFilter;

/// Authorization/visibility dimension of the cache key. v1 has no auth — every
/// session sees the same data, so all entries are [`Visibility::Shared`]. The
/// variant exists so v2 auth (§1.6) can key per-tenant without a shared bitmap
/// leaking rows across users; nothing threads a real identity in v1.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum Visibility {
    Shared,
}

/// One cached realization: the normalized clause set that produced it, the
/// bitmap, the memory charge (released on eviction via drop), and an LRU stamp.
struct Entry {
    clauses: Vec<PushedFilter>,
    bitmap: Arc<RoaringBitmap>,
    _charge: SessionReservation,
    last_access: u64,
}

struct CacheState {
    /// Entries bucketed by `(shard, visibility)`; within a bucket, lookup and the
    /// narrowing subset-search are linear over the (small) clause sets seen.
    buckets: HashMap<(u64, Visibility), Vec<Entry>>,
    /// Monotonic logical clock for LRU ordering (deterministic, test-friendly).
    clock: u64,
}

/// Daemon-shared cache of per-shard matched-doc bitmaps.
pub struct BitmapCache {
    budget: Arc<SessionMemoryHandle>,
    state: Mutex<CacheState>,
    hits: AtomicU64,
    narrows: AtomicU64,
    misses: AtomicU64,
}

impl BitmapCache {
    #[must_use]
    pub fn new(budget: Arc<SessionMemoryHandle>) -> Self {
        Self {
            budget,
            state: Mutex::new(CacheState {
                buckets: HashMap::new(),
                clock: 0,
            }),
            hits: AtomicU64::new(0),
            narrows: AtomicU64::new(0),
            misses: AtomicU64::new(0),
        }
    }

    /// Stable identity for a shard: a hash of its dataset URL and location string.
    /// Shards are immutable at a location, so this is a content-stable key.
    #[must_use]
    pub(crate) fn shard_id(url: &str, location: &str) -> u64 {
        let mut h = DefaultHasher::new();
        url.hash(&mut h);
        location.hash(&mut h);
        h.finish()
    }

    // Counters are incremented on the hot path always; only tests read them for
    // now. §3.4 observability will expose them on `/metrics`.
    #[cfg(test)]
    pub(crate) fn hits(&self) -> u64 {
        self.hits.load(Ordering::Relaxed)
    }

    #[cfg(test)]
    pub(crate) fn narrows(&self) -> u64 {
        self.narrows.load(Ordering::Relaxed)
    }

    #[cfg(test)]
    pub(crate) fn misses(&self) -> u64 {
        self.misses.load(Ordering::Relaxed)
    }

    /// Resolve `pushed` against `shard` to a matched-doc set, serving from cache
    /// where possible. `shard_id` is [`Self::shard_id`] for this shard. An empty
    /// filter is `All` and is never cached.
    pub(crate) fn resolve(
        &self,
        shard: &DiskShard,
        shard_id: u64,
        pushed: &[PushedFilter],
    ) -> anyhow::Result<FilterResult> {
        if pushed.is_empty() {
            return Ok(FilterResult::All);
        }
        let key = normalize(pushed);
        let bucket = (shard_id, Visibility::Shared);

        match self.probe(bucket, &key) {
            Probe::Exact(bitmap) => {
                self.hits.fetch_add(1, Ordering::Relaxed);
                Ok(to_filter_result(&bitmap))
            }
            Probe::Subset { base, base_clauses } => {
                self.narrows.fetch_add(1, Ordering::Relaxed);
                // delta = key \ base_clauses (non-empty: base is a strict subset).
                let delta: Vec<PushedFilter> = key
                    .iter()
                    .filter(|c| !base_clauses.contains(c))
                    .cloned()
                    .collect();
                let clauses: Vec<_> = delta.iter().map(PushedFilter::as_clause).collect();
                let delta_set = matched_doc_set(shard, &clauses)?;
                let result = intersect(base.as_ref(), &delta_set);
                let arc = Arc::new(result);
                self.insert(bucket, key, Arc::clone(&arc));
                Ok(to_filter_result(&arc))
            }
            Probe::Miss => {
                self.misses.fetch_add(1, Ordering::Relaxed);
                let clauses: Vec<_> = key.iter().map(PushedFilter::as_clause).collect();
                let result = matched_doc_set(shard, &clauses)?;
                // Non-empty filters never yield `All`; cache Bitmap/Empty as a
                // (possibly empty) bitmap. Return the owned result unchanged.
                let bitmap = match &result {
                    FilterResult::Bitmap(b) => b.clone(),
                    FilterResult::Empty => RoaringBitmap::new(),
                    FilterResult::All => return Ok(result),
                };
                self.insert(bucket, key, Arc::new(bitmap));
                Ok(result)
            }
        }
    }

    /// Look for an exact entry, else the largest cached subset (nearest ancestor
    /// → smallest delta), touching the LRU stamp of whatever it returns.
    fn probe(&self, bucket: (u64, Visibility), key: &[PushedFilter]) -> Probe {
        let mut st = self.state.lock().expect("bitmap cache lock");
        st.clock += 1;
        let now = st.clock;
        let Some(entries) = st.buckets.get_mut(&bucket) else {
            return Probe::Miss;
        };
        if let Some(e) = entries.iter_mut().find(|e| e.clauses == key) {
            e.last_access = now;
            return Probe::Exact(Arc::clone(&e.bitmap));
        }
        let best = entries
            .iter_mut()
            .filter(|e| is_subset(&e.clauses, key))
            .max_by_key(|e| e.clauses.len());
        match best {
            Some(e) => {
                e.last_access = now;
                Probe::Subset {
                    base: Arc::clone(&e.bitmap),
                    base_clauses: e.clauses.clone(),
                }
            }
            None => Probe::Miss,
        }
    }

    /// Charge and store an entry, evicting LRU entries to make room. If the bitmap
    /// can't be charged even after evicting everything, skip caching.
    fn insert(
        &self,
        bucket: (u64, Visibility),
        clauses: Vec<PushedFilter>,
        bitmap: Arc<RoaringBitmap>,
    ) {
        let bytes = bitmap.serialized_size() as u64;
        let Some(charge) = self.reserve_with_eviction(bytes) else {
            return;
        };
        let mut st = self.state.lock().expect("bitmap cache lock");
        st.clock += 1;
        let last_access = st.clock;
        st.buckets.entry(bucket).or_default().push(Entry {
            clauses,
            bitmap,
            _charge: charge,
            last_access,
        });
    }

    /// Reserve `bytes` from the sub-budget, evicting the oldest cached entries
    /// until it fits or the cache is empty.
    fn reserve_with_eviction(&self, bytes: u64) -> Option<SessionReservation> {
        loop {
            if let Ok(r) = self.budget.reserve(bytes) {
                return Some(r);
            }
            if !self.evict_oldest() {
                return None;
            }
        }
    }

    /// Drop the single globally-oldest entry (releasing its reservation). Returns
    /// false when the cache is empty.
    fn evict_oldest(&self) -> bool {
        let mut st = self.state.lock().expect("bitmap cache lock");
        let mut victim: Option<((u64, Visibility), usize, u64)> = None;
        for (k, entries) in &st.buckets {
            for (i, e) in entries.iter().enumerate() {
                if victim.as_ref().is_none_or(|(_, _, la)| e.last_access < *la) {
                    victim = Some((*k, i, e.last_access));
                }
            }
        }
        let Some((k, i, _)) = victim else {
            return false;
        };
        let entries = st.buckets.get_mut(&k).expect("victim bucket present");
        entries.swap_remove(i);
        if entries.is_empty() {
            st.buckets.remove(&k);
        }
        true
    }
}

enum Probe {
    Exact(Arc<RoaringBitmap>),
    Subset {
        base: Arc<RoaringBitmap>,
        base_clauses: Vec<PushedFilter>,
    },
    Miss,
}

/// Canonical clause set: sorted then deduped, so set-equality and subset checks
/// are positional merges. `PushedFilter` derives `Ord`/`Eq`.
fn normalize(pushed: &[PushedFilter]) -> Vec<PushedFilter> {
    let mut v = pushed.to_vec();
    v.sort();
    v.dedup();
    v
}

/// Whether every clause of `sub` appears in `sup`. Both must be normalized
/// (sorted, deduped); O(|sub| + |sup|).
fn is_subset(sub: &[PushedFilter], sup: &[PushedFilter]) -> bool {
    let mut j = 0;
    for s in sub {
        while j < sup.len() && &sup[j] < s {
            j += 1;
        }
        if j >= sup.len() || &sup[j] != s {
            return false;
        }
        j += 1;
    }
    true
}

/// Intersect a cached base bitmap with a freshly-resolved delta result.
fn intersect(base: &RoaringBitmap, delta: &FilterResult) -> RoaringBitmap {
    match delta {
        // Delta clauses are non-empty here, so `matched_doc_set` yields
        // Bitmap/Empty, never All; All is handled defensively as "no narrowing".
        FilterResult::All => base.clone(),
        FilterResult::Empty => RoaringBitmap::new(),
        FilterResult::Bitmap(d) => {
            let mut r = base.clone();
            r &= d;
            r
        }
    }
}

/// Present a stored bitmap as a [`FilterResult`]: an empty bitmap is `Empty`.
fn to_filter_result(bitmap: &RoaringBitmap) -> FilterResult {
    if bitmap.is_empty() {
        FilterResult::Empty
    } else {
        FilterResult::Bitmap(bitmap.clone())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::sql::pushdown::PushedOp;

    fn eq(field: &str, term: &str) -> PushedFilter {
        PushedFilter {
            field: field.to_string(),
            op: PushedOp::Equals(term.to_string()),
            exact: true,
        }
    }

    #[test]
    fn normalize_sorts_and_dedups() {
        let raw = vec![eq("b", "1"), eq("a", "2"), eq("b", "1")];
        let n = normalize(&raw);
        assert_eq!(n, vec![eq("a", "2"), eq("b", "1")]);
    }

    #[test]
    fn subset_detection() {
        let a = normalize(&[eq("country", "us")]);
        let ab = normalize(&[eq("country", "us"), eq("device", "mobile")]);
        let ac = normalize(&[eq("country", "us"), eq("os", "ios")]);
        assert!(
            is_subset(&a, &ab),
            "single clause is a subset of its superset"
        );
        assert!(is_subset(&ab, &ab), "a set is a subset of itself");
        assert!(!is_subset(&ab, &a), "superset is not a subset");
        assert!(
            !is_subset(&ab, &ac),
            "disjoint extra clause breaks containment"
        );
        assert!(is_subset(&[], &ab), "empty set is a subset of anything");
    }

    #[test]
    fn shard_id_is_stable_and_distinguishing() {
        let a = BitmapCache::shard_id("file:///data/ds1", "s0");
        let a2 = BitmapCache::shard_id("file:///data/ds1", "s0");
        let b = BitmapCache::shard_id("file:///data/ds1", "s1");
        let c = BitmapCache::shard_id("file:///data/ds2", "s0");
        assert_eq!(a, a2);
        assert_ne!(a, b);
        assert_ne!(a, c);
    }

    #[test]
    fn intersect_combines_and_empties() {
        let base: RoaringBitmap = [0u32, 1, 2, 3].into_iter().collect();
        let delta: RoaringBitmap = [1u32, 3, 5].into_iter().collect();
        let got = intersect(&base, &FilterResult::Bitmap(delta));
        assert_eq!(got, [1u32, 3].into_iter().collect());
        assert!(intersect(&base, &FilterResult::Empty).is_empty());
        assert_eq!(intersect(&base, &FilterResult::All), base);
    }
}
