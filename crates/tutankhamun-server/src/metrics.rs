//! Prometheus metrics (§3.4).
//!
//! A daemon-shared [`Metrics`] holder, rendered as Prometheus text exposition on
//! the ops port's `/metrics` ([`crate::ops_http`]). Gauges are pulled live from the
//! subsystems they reflect (the §2.2 [`MemoryBudget`], the §2.8 [`BitmapCache`]) so
//! they're never stale; the session gauge and the query histogram/counters are
//! bumped by the `FlightSQL` service as work happens.
//!
//! The text format is hand-rolled — the metric set is small and fixed, so a client
//! crate would only add a dependency and a facade. Standard cardinality discipline:
//! no per-user or per-query labels.

use std::fmt::Write as _;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;

use crate::bitmap_cache::BitmapCache;
use crate::memory::MemoryBudget;

/// Upper bounds (seconds, inclusive) of the query-latency histogram buckets.
const BUCKETS_SECS: [f64; 12] = [
    0.001, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0, 2.5, 5.0, 10.0,
];

/// A fixed-bucket latency histogram. Each observation lands in the smallest bucket
/// whose bound is `>= elapsed` (observations past the last bound count only toward
/// the implicit `+Inf` bucket, i.e. `count`). Cumulative `_bucket` values are
/// computed at render time.
#[derive(Debug)]
struct Histogram {
    buckets: [AtomicU64; BUCKETS_SECS.len()],
    sum_nanos: AtomicU64,
    count: AtomicU64,
}

impl Default for Histogram {
    fn default() -> Self {
        Self {
            buckets: std::array::from_fn(|_| AtomicU64::new(0)),
            sum_nanos: AtomicU64::new(0),
            count: AtomicU64::new(0),
        }
    }
}

impl Histogram {
    fn observe(&self, elapsed: Duration) {
        let secs = elapsed.as_secs_f64();
        if let Some(i) = BUCKETS_SECS.iter().position(|&b| secs <= b) {
            self.buckets[i].fetch_add(1, Ordering::Relaxed);
        }
        // `as_nanos` is u128; a single duration fits u64 (~584 years of ns), and
        // a pathological value saturates rather than wrapping the sum.
        let nanos = u64::try_from(elapsed.as_nanos()).unwrap_or(u64::MAX);
        self.sum_nanos.fetch_add(nanos, Ordering::Relaxed);
        self.count.fetch_add(1, Ordering::Relaxed);
    }

    /// Render `name` as a Prometheus histogram (cumulative `_bucket` + `_sum` +
    /// `_count`).
    fn render(&self, out: &mut String, name: &str, help: &str) {
        let _ = writeln!(out, "# HELP {name} {help}");
        let _ = writeln!(out, "# TYPE {name} histogram");
        let mut cumulative = 0u64;
        for (i, bound) in BUCKETS_SECS.iter().enumerate() {
            cumulative += self.buckets[i].load(Ordering::Relaxed);
            let _ = writeln!(out, "{name}_bucket{{le=\"{bound}\"}} {cumulative}");
        }
        let count = self.count.load(Ordering::Relaxed);
        let _ = writeln!(out, "{name}_bucket{{le=\"+Inf\"}} {count}");
        #[allow(clippy::cast_precision_loss)]
        let sum_secs = self.sum_nanos.load(Ordering::Relaxed) as f64 / 1e9;
        let _ = writeln!(out, "{name}_sum {sum_secs}");
        let _ = writeln!(out, "{name}_count {count}");
    }
}

/// Daemon-shared metrics surface. Cheap to clone (`Arc`); shared between the ops
/// HTTP server (which renders `/metrics`) and the `FlightSQL` service (which records
/// queries and session lifecycle).
pub struct Metrics {
    budget: Arc<MemoryBudget>,
    bitmap_cache: Arc<BitmapCache>,
    live_sessions: AtomicU64,
    query_latency: Histogram,
    queries_total: AtomicU64,
    query_errors_total: AtomicU64,
}

impl Metrics {
    #[must_use]
    pub fn new(budget: Arc<MemoryBudget>, bitmap_cache: Arc<BitmapCache>) -> Arc<Self> {
        Arc::new(Self {
            budget,
            bitmap_cache,
            live_sessions: AtomicU64::new(0),
            query_latency: Histogram::default(),
            queries_total: AtomicU64::new(0),
            query_errors_total: AtomicU64::new(0),
        })
    }

    /// A session was opened.
    pub fn session_opened(&self) {
        self.live_sessions.fetch_add(1, Ordering::Relaxed);
    }

    /// `n` sessions were reaped together.
    pub fn sessions_reaped(&self, n: usize) {
        self.dec_sessions(n as u64);
    }

    /// A session was explicitly closed.
    pub fn session_closed(&self) {
        self.dec_sessions(1);
    }

    fn dec_sessions(&self, n: u64) {
        // Saturating: never wrap below zero even if a decrement races a reset.
        let _ = self
            .live_sessions
            .fetch_update(Ordering::Relaxed, Ordering::Relaxed, |cur| {
                Some(cur.saturating_sub(n))
            });
    }

    /// Record one query execution: its latency and whether it succeeded.
    pub fn record_query(&self, elapsed: Duration, ok: bool) {
        self.query_latency.observe(elapsed);
        self.queries_total.fetch_add(1, Ordering::Relaxed);
        if !ok {
            self.query_errors_total.fetch_add(1, Ordering::Relaxed);
        }
    }

    /// Render the full Prometheus text exposition.
    #[must_use]
    pub fn render(&self) -> String {
        let mut out = String::with_capacity(1024);

        let _ = writeln!(out, "# HELP tut_build_info Build metadata.");
        let _ = writeln!(out, "# TYPE tut_build_info gauge");
        let _ = writeln!(
            out,
            "tut_build_info{{version=\"{}\"}} 1",
            env!("CARGO_PKG_VERSION")
        );

        gauge(
            &mut out,
            "tut_memory_limit_bytes",
            "Daemon-wide memory budget cap.",
            self.budget.limit(),
        );
        gauge(
            &mut out,
            "tut_memory_used_bytes",
            "Daemon-wide memory currently reserved.",
            self.budget.used(),
        );
        gauge(
            &mut out,
            "tut_sessions_live",
            "FlightSQL sessions currently open.",
            self.live_sessions.load(Ordering::Relaxed),
        );

        gauge(
            &mut out,
            "tut_bitmap_cache_used_bytes",
            "Bytes held by the §2.8 doc-set bitmap cache.",
            self.bitmap_cache.used_bytes(),
        );
        counter(
            &mut out,
            "tut_bitmap_cache_hits_total",
            "Doc-set bitmap cache exact hits.",
            self.bitmap_cache.hits(),
        );
        counter(
            &mut out,
            "tut_bitmap_cache_narrows_total",
            "Doc-set bitmap cache monotone-narrowing reuses.",
            self.bitmap_cache.narrows(),
        );
        counter(
            &mut out,
            "tut_bitmap_cache_misses_total",
            "Doc-set bitmap cache misses (fresh scans).",
            self.bitmap_cache.misses(),
        );

        self.query_latency.render(
            &mut out,
            "tut_query_duration_seconds",
            "Query execution latency in seconds.",
        );
        counter(
            &mut out,
            "tut_queries_total",
            "Queries executed.",
            self.queries_total.load(Ordering::Relaxed),
        );
        counter(
            &mut out,
            "tut_query_errors_total",
            "Query executions that failed.",
            self.query_errors_total.load(Ordering::Relaxed),
        );

        out
    }
}

fn gauge(out: &mut String, name: &str, help: &str, value: u64) {
    emit(out, name, "gauge", help, value);
}

fn counter(out: &mut String, name: &str, help: &str, value: u64) {
    emit(out, name, "counter", help, value);
}

fn emit(out: &mut String, name: &str, ty: &str, help: &str, value: u64) {
    let _ = writeln!(out, "# HELP {name} {help}");
    let _ = writeln!(out, "# TYPE {name} {ty}");
    let _ = writeln!(out, "{name} {value}");
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::memory::SessionMemoryHandle;

    fn metrics() -> Arc<Metrics> {
        let budget = Arc::new(MemoryBudget::new(1000));
        let cache = Arc::new(BitmapCache::new(Arc::new(SessionMemoryHandle::new(
            Arc::clone(&budget),
            1000,
        ))));
        Metrics::new(budget, cache)
    }

    #[test]
    fn render_has_expected_metrics() {
        let m = metrics();
        let text = m.render();
        for name in [
            "tut_build_info",
            "tut_memory_limit_bytes",
            "tut_memory_used_bytes",
            "tut_sessions_live",
            "tut_bitmap_cache_hits_total",
            "tut_query_duration_seconds_bucket",
            "tut_queries_total",
            "tut_query_errors_total",
        ] {
            assert!(text.contains(name), "missing {name} in:\n{text}");
        }
        assert!(text.contains("# TYPE tut_queries_total counter"));
        assert!(text.contains("tut_memory_limit_bytes 1000"));
    }

    #[test]
    fn session_gauge_tracks_open_and_reap_saturating() {
        let m = metrics();
        m.session_opened();
        m.session_opened();
        m.session_opened();
        assert!(m.render().contains("tut_sessions_live 3"));
        m.sessions_reaped(2);
        assert!(m.render().contains("tut_sessions_live 1"));
        // Over-decrement saturates at 0, never wraps.
        m.sessions_reaped(5);
        assert!(m.render().contains("tut_sessions_live 0"));
    }

    #[test]
    fn query_histogram_buckets_and_error_counter() {
        let m = metrics();
        m.record_query(Duration::from_millis(2), true); // ≤ 0.005 bucket
        m.record_query(Duration::from_millis(2), true);
        m.record_query(Duration::from_secs(3), false); // ≤ 2.5? no → 5.0 bucket, error
        let text = m.render();
        assert!(text.contains("tut_queries_total 3"));
        assert!(text.contains("tut_query_errors_total 1"));
        assert!(text.contains("tut_query_duration_seconds_count 3"));
        // Two fast queries are ≤ 5ms; the 3s one isn't.
        assert!(text.contains("tut_query_duration_seconds_bucket{le=\"0.005\"} 2"));
        // All three are ≤ 5s (cumulative includes the 3s one).
        assert!(text.contains("tut_query_duration_seconds_bucket{le=\"5\"} 3"));
        assert!(text.contains("tut_query_duration_seconds_bucket{le=\"+Inf\"} 3"));
    }
}
