//! Daemon-shared per-shard aggregate cache.
//!
//! A time-bucket / regrouped aggregate computes, per shard, a small set of
//! `(group-value tuple → stats)` partials. Shards are immutable (§2.1), so a
//! shard's partial for a given `(filter, group-keys, stats)` shape is a pure
//! function of the shard — content-addressable, never stale, shareable across
//! sessions. This cache keys each shard's partial by that shape and serves it on
//! reruns and on queries that touch the same shards (e.g. an extended time
//! window): the unchanged shards hit, only new shards compute.
//!
//! Sibling to [`crate::bitmap_cache::BitmapCache`] (which caches the per-shard
//! *filter* result): same `SessionMemoryHandle` sub-budget + LRU eviction. An
//! insert that can't be charged is skipped, never failing the query. Exact-shape
//! match only; rollup/range reuse is a future refinement.

use std::collections::HashMap;
use std::sync::Arc;
use std::sync::Mutex;
use std::sync::atomic::{AtomicU64, Ordering};

use crate::ftgs::StatValue;
use crate::memory::{SessionMemoryHandle, SessionReservation};
use crate::sql::group_by::{GroupKey, OwnedStat};
use crate::sql::pushdown::PushedFilter;

/// A group-value tuple: the FTGS-canonical key bytes per group column (`None`
/// for a SQL NULL). Group *values*, not per-shard ids, so tuples are comparable
/// across shards.
pub(crate) type GroupTuple = Vec<Option<Box<[u8]>>>;

/// One shard's aggregated groups for a `(filter, group-keys, stats)` shape: each
/// entry is a group-value tuple and its per-stat values. Self-contained (group
/// values, not global ids), so it combines across shards independently.
pub(crate) type ShardPartial = Vec<(GroupTuple, Vec<StatValue>)>;

/// Identifies a cached partial: a shard plus the aggregate shape it answers.
#[derive(Clone, PartialEq, Eq, Hash)]
struct AggKey {
    shard: u64,
    filters: Vec<PushedFilter>,
    groups: Vec<GroupKey>,
    stats: Vec<OwnedStat>,
}

struct Entry {
    partial: Arc<ShardPartial>,
    _charge: SessionReservation,
    last_access: u64,
}

struct CacheState {
    map: HashMap<AggKey, Entry>,
    clock: u64,
}

/// Daemon-shared cache of per-shard aggregate partials.
pub struct AggregateCache {
    budget: Arc<SessionMemoryHandle>,
    state: Mutex<CacheState>,
    hits: AtomicU64,
    misses: AtomicU64,
}

impl AggregateCache {
    #[must_use]
    pub fn new(budget: Arc<SessionMemoryHandle>) -> Self {
        Self {
            budget,
            state: Mutex::new(CacheState {
                map: HashMap::new(),
                clock: 0,
            }),
            hits: AtomicU64::new(0),
            misses: AtomicU64::new(0),
        }
    }

    pub(crate) fn hits(&self) -> u64 {
        self.hits.load(Ordering::Relaxed)
    }

    pub(crate) fn misses(&self) -> u64 {
        self.misses.load(Ordering::Relaxed)
    }

    /// Bytes currently held by cached partials (the cache's sub-budget usage).
    pub(crate) fn used_bytes(&self) -> u64 {
        self.budget.used()
    }

    /// The cached partial for `shard_id` under this aggregate shape, or compute it
    /// via `compute` and store it. `compute` runs only on a miss.
    pub(crate) fn get_or_compute<F>(
        &self,
        shard_id: u64,
        filters: &[PushedFilter],
        groups: &[GroupKey],
        stats: &[OwnedStat],
        compute: F,
    ) -> anyhow::Result<Arc<ShardPartial>>
    where
        F: FnOnce() -> anyhow::Result<ShardPartial>,
    {
        let mut key = AggKey {
            shard: shard_id,
            filters: filters.to_vec(),
            groups: groups.to_vec(),
            stats: stats.to_vec(),
        };
        // Canonical filter order, so set-equal filters share an entry.
        key.filters.sort();
        key.filters.dedup();

        if let Some(p) = self.probe(&key) {
            self.hits.fetch_add(1, Ordering::Relaxed);
            return Ok(p);
        }
        self.misses.fetch_add(1, Ordering::Relaxed);
        let partial = Arc::new(compute()?);
        self.insert(key, Arc::clone(&partial));
        Ok(partial)
    }

    fn probe(&self, key: &AggKey) -> Option<Arc<ShardPartial>> {
        let mut st = self.state.lock().expect("aggregate cache lock");
        st.clock += 1;
        let now = st.clock;
        let entry = st.map.get_mut(key)?;
        entry.last_access = now;
        Some(Arc::clone(&entry.partial))
    }

    fn insert(&self, key: AggKey, partial: Arc<ShardPartial>) {
        let bytes = partial_bytes(&partial);
        let Some(charge) = self.reserve_with_eviction(bytes) else {
            return;
        };
        let mut st = self.state.lock().expect("aggregate cache lock");
        st.clock += 1;
        let last_access = st.clock;
        st.map.insert(
            key,
            Entry {
                partial,
                _charge: charge,
                last_access,
            },
        );
    }

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

    /// Drop the globally-oldest entry (releasing its reservation). False when empty.
    fn evict_oldest(&self) -> bool {
        let mut st = self.state.lock().expect("aggregate cache lock");
        let victim = st
            .map
            .iter()
            .min_by_key(|(_, e)| e.last_access)
            .map(|(k, _)| k.clone());
        match victim {
            Some(k) => {
                st.map.remove(&k);
                true
            }
            None => false,
        }
    }
}

/// Rough byte size of a partial for budgeting: per group, the tuple's key bytes
/// plus a fixed allowance per stat. Bounded by group cardinality (low for the
/// time-bucket case), so a coarse estimate is fine.
fn partial_bytes(partial: &ShardPartial) -> u64 {
    const PER_STAT: u64 = 32;
    partial
        .iter()
        .map(|(tuple, stats)| {
            let key_bytes: u64 = tuple
                .iter()
                .map(|c| c.as_ref().map_or(0, |b| b.len() as u64))
                .sum();
            key_bytes + stats.len() as u64 * PER_STAT + 16
        })
        .sum()
}
