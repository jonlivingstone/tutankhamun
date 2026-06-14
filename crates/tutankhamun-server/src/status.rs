//! Status report types and the [`StatusSource`] seam for the `/status` page (§3.5).
//!
//! `/status.json` ([`crate::ops_http`]) serialises a [`StatusReport`]: numeric
//! state (memory / cache / query counters + a recent-query ring) comes from the
//! shared [`crate::metrics::Metrics`]; structural state (session summary + dataset
//! names) comes from a [`StatusSource`], implemented by the `FlightSQL` service so
//! the ops layer never sees its internals. The HTML page renders this JSON
//! client-side.
//!
//! Deferred (v1 lists dataset names only): a per-shard size-on-disk / mmap'd table
//! (needs `Cache` resident-bytes accounting; "loaded shards" isn't tracked, as
//! datasets are discovered lazily), and latency percentiles in JSON (already on
//! `/metrics`).

use async_trait::async_trait;
use serde::Serialize;

/// Structural daemon state that lives in the `FlightSQL` service, surfaced to the
/// ops layer without exposing the service's internals.
#[async_trait]
pub trait StatusSource: Send + Sync {
    /// Snapshot the live session summary and the dataset names under the storage
    /// root. Must not fail the page: a storage LIST error yields
    /// [`DatasetsReport::ok`] `= false` with no names rather than an error.
    async fn structural_snapshot(&self) -> StructuralReport;
}

/// The full `/status.json` body. `version`/`uptime_secs`/`ready` are set by the
/// ops layer; the metrics and structural sections are flattened in so the JSON is
/// a single flat object.
#[derive(Serialize)]
pub struct StatusReport {
    pub version: &'static str,
    pub uptime_secs: u64,
    pub ready: bool,
    #[serde(flatten)]
    pub metrics: MetricsSnapshot,
    #[serde(flatten)]
    pub structural: StructuralReport,
}

/// Numeric state read from [`crate::metrics::Metrics`].
#[derive(Serialize)]
pub struct MetricsSnapshot {
    pub memory: MemoryReport,
    pub bitmap_cache: CacheReport,
    pub queries: QueriesReport,
}

#[derive(Serialize)]
pub struct MemoryReport {
    pub limit_bytes: u64,
    pub used_bytes: u64,
    pub available_bytes: u64,
}

#[derive(Serialize)]
pub struct CacheReport {
    pub used_bytes: u64,
    pub hits: u64,
    pub narrows: u64,
    pub misses: u64,
}

#[derive(Serialize)]
pub struct QueriesReport {
    pub total: u64,
    pub errors: u64,
    /// Most-recent-first, bounded ring (see `metrics::RECENT_CAP`).
    pub recent: Vec<RecentQueryView>,
}

/// A recent query as rendered for the page — `age_secs` is computed at snapshot
/// time from the stored instant, so no wall-clock dependency is needed.
#[derive(Serialize)]
pub struct RecentQueryView {
    pub sql: String,
    pub rows: u64,
    pub ok: bool,
    pub duration_ms: u64,
    pub age_secs: u64,
}

/// Structural state from the [`StatusSource`].
#[derive(Serialize)]
pub struct StructuralReport {
    pub sessions: SessionsSummary,
    pub datasets: DatasetsReport,
}

#[derive(Serialize)]
pub struct SessionsSummary {
    pub count: u64,
    pub oldest_age_secs: u64,
    /// Baseline bytes reserved per live session (admission floor), not live
    /// working-set — per-session query charges are not metered here.
    pub reserved_bytes: u64,
}

#[derive(Serialize)]
pub struct DatasetsReport {
    /// `false` if the storage LIST failed; `names` is then empty.
    pub ok: bool,
    pub names: Vec<String>,
}
