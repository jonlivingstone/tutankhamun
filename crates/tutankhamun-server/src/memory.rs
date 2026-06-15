//! Memory budget and accounting (§2.2).
//!
//! A daemon-wide [`MemoryBudget`] holds a hard byte cap and the current charge.
//! Every accounted allocation reserves through it and receives an RAII
//! [`MemoryReservation`] whose drop returns the bytes — so the running total
//! cannot leak. Reservation is **claim-or-fail**: a request that would exceed
//! the cap returns [`BudgetExceeded`] rather than proceeding, so the daemon
//! stays under its configured limit instead of being OOM-killed.
//!
//! Each session holds a [`SessionMemoryHandle`] — a sub-budget capped at a
//! fraction of the global (default 20%, §2.2 fairness) that charges *both* the
//! session counter and the global one, so a single session cannot monopolise
//! the daemon. Admission control reserves a session's baseline through the
//! handle at open time; if that fails the daemon is at capacity.
//!
//! Reservations charge a query's **bounded working set**, not the volume of
//! data it scans. The scan ([`crate::sql::scan::for_each_shard_batch`])
//! processes shards in bounded-concurrency batches and reserves a
//! span-independent envelope up front (`batch width × largest shard's num_docs
//! × a per-doc working-set estimate`), held for the whole query — so an
//! admitted query is guaranteed room to finish and querying all of history
//! uses no more memory than querying one batch. (mmap'd forward-column pages are
//! demand-faulted and kernel-evictable; their residency is bounded by the batch
//! concurrency, not byte-charged here.)

use std::fmt;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

/// Which budget a reservation request blew past.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Scope {
    /// The per-session cap.
    Session,
    /// The daemon-wide cap.
    Global,
}

impl fmt::Display for Scope {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Scope::Session => f.write_str("session"),
            Scope::Global => f.write_str("global"),
        }
    }
}

/// A reservation was refused because it would exceed a cap.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct BudgetExceeded {
    pub scope: Scope,
    pub requested: u64,
    pub available: u64,
}

impl fmt::Display for BudgetExceeded {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "{} memory budget exceeded: requested {} bytes, {} available",
            self.scope, self.requested, self.available
        )
    }
}

impl std::error::Error for BudgetExceeded {}

/// Daemon-wide memory budget: a hard cap and the current charge.
#[derive(Debug)]
pub struct MemoryBudget {
    limit: u64,
    charged: AtomicU64,
}

impl MemoryBudget {
    #[must_use]
    pub fn new(limit: u64) -> Self {
        Self {
            limit,
            charged: AtomicU64::new(0),
        }
    }

    /// The configured cap.
    #[must_use]
    pub fn limit(&self) -> u64 {
        self.limit
    }

    /// Bytes currently reserved.
    #[must_use]
    pub fn used(&self) -> u64 {
        self.charged.load(Ordering::Acquire)
    }

    /// Headroom remaining before the cap.
    #[must_use]
    pub fn available(&self) -> u64 {
        self.limit.saturating_sub(self.used())
    }

    /// Reserve `bytes`, or fail if the cap would be exceeded. The returned
    /// guard releases the bytes on drop. Lock-free: a CAS loop that only
    /// commits when the new total still fits.
    ///
    /// # Errors
    /// [`BudgetExceeded`] if `used() + bytes > limit`.
    pub fn reserve(self: &Arc<Self>, bytes: u64) -> Result<MemoryReservation, BudgetExceeded> {
        let mut cur = self.charged.load(Ordering::Acquire);
        loop {
            // Saturating add: an overflowing request is, a fortiori, over cap.
            let new = cur.saturating_add(bytes);
            if new > self.limit {
                return Err(BudgetExceeded {
                    scope: Scope::Global,
                    requested: bytes,
                    available: self.limit.saturating_sub(cur),
                });
            }
            match self
                .charged
                .compare_exchange_weak(cur, new, Ordering::AcqRel, Ordering::Acquire)
            {
                Ok(_) => {
                    return Ok(MemoryReservation {
                        budget: Arc::clone(self),
                        bytes,
                    });
                }
                Err(actual) => cur = actual,
            }
        }
    }

    fn release(&self, bytes: u64) {
        self.charged.fetch_sub(bytes, Ordering::AcqRel);
    }
}

/// RAII guard for a global reservation. Drop returns the bytes to the budget.
#[derive(Debug)]
pub struct MemoryReservation {
    budget: Arc<MemoryBudget>,
    bytes: u64,
}

impl MemoryReservation {
    /// The number of bytes this reservation holds.
    #[must_use]
    pub fn bytes(&self) -> u64 {
        self.bytes
    }
}

impl Drop for MemoryReservation {
    fn drop(&mut self) {
        self.budget.release(self.bytes);
    }
}

/// A session's sub-budget: capped at a fraction of the global budget, and
/// charging both its own counter and the global one. Held by each live session.
#[derive(Debug)]
pub struct SessionMemoryHandle {
    global: Arc<MemoryBudget>,
    cap: u64,
    charged: AtomicU64,
}

impl SessionMemoryHandle {
    #[must_use]
    pub fn new(global: Arc<MemoryBudget>, cap: u64) -> Self {
        Self {
            global,
            cap,
            charged: AtomicU64::new(0),
        }
    }

    /// The per-session cap.
    #[must_use]
    pub fn cap(&self) -> u64 {
        self.cap
    }

    /// Bytes this session currently holds.
    #[must_use]
    pub fn used(&self) -> u64 {
        self.charged.load(Ordering::Acquire)
    }

    /// Reserve `bytes` against this session (and the global budget). Checks the
    /// session cap first, then the global; on global failure the session charge
    /// is rolled back. The returned guard releases both on drop.
    ///
    /// # Errors
    /// [`BudgetExceeded`] with [`Scope::Session`] if over the per-session cap,
    /// or [`Scope::Global`] if the daemon-wide budget is exhausted.
    pub fn reserve(self: &Arc<Self>, bytes: u64) -> Result<SessionReservation, BudgetExceeded> {
        let mut cur = self.charged.load(Ordering::Acquire);
        loop {
            let new = cur.saturating_add(bytes);
            if new > self.cap {
                return Err(BudgetExceeded {
                    scope: Scope::Session,
                    requested: bytes,
                    available: self.cap.saturating_sub(cur),
                });
            }
            match self
                .charged
                .compare_exchange_weak(cur, new, Ordering::AcqRel, Ordering::Acquire)
            {
                Ok(_) => break,
                Err(actual) => cur = actual,
            }
        }
        // Session cap satisfied; now the global budget.
        match self.global.reserve(bytes) {
            Ok(global) => Ok(SessionReservation {
                _global: global,
                handle: Arc::clone(self),
                bytes,
            }),
            Err(e) => {
                self.charged.fetch_sub(bytes, Ordering::AcqRel);
                Err(e)
            }
        }
    }

    fn release(&self, bytes: u64) {
        self.charged.fetch_sub(bytes, Ordering::AcqRel);
    }
}

/// RAII guard for a session reservation. Drop releases the session counter;
/// the held global [`MemoryReservation`] releases the global budget on its own
/// drop.
#[derive(Debug)]
pub struct SessionReservation {
    _global: MemoryReservation,
    handle: Arc<SessionMemoryHandle>,
    bytes: u64,
}

impl Drop for SessionReservation {
    fn drop(&mut self) {
        self.handle.release(self.bytes);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn reserve_up_to_limit_then_refuse() {
        let budget = Arc::new(MemoryBudget::new(100));
        let a = budget.reserve(60).expect("first fits");
        assert_eq!(budget.used(), 60);
        // 60 + 50 > 100 → refused, charge unchanged.
        let err = budget.reserve(50).expect_err("over cap");
        assert_eq!(err.scope, Scope::Global);
        assert_eq!(err.available, 40);
        assert_eq!(budget.used(), 60);
        // Exactly fills the remainder.
        let _b = budget.reserve(40).expect("exact fit");
        assert_eq!(budget.used(), 100);
        drop(a);
        assert_eq!(budget.used(), 40, "drop returns bytes");
    }

    #[test]
    fn drop_returns_bytes() {
        let budget = Arc::new(MemoryBudget::new(1000));
        {
            let _r = budget.reserve(400).expect("fits");
            assert_eq!(budget.used(), 400);
        }
        assert_eq!(budget.used(), 0);
    }

    #[test]
    fn session_cap_rejects_even_with_global_headroom() {
        let global = Arc::new(MemoryBudget::new(1000));
        let handle = Arc::new(SessionMemoryHandle::new(Arc::clone(&global), 100));
        let _a = handle.reserve(80).expect("under cap");
        // Global has 920 free, but the session cap (100) blocks this.
        let err = handle.reserve(50).expect_err("over session cap");
        assert_eq!(err.scope, Scope::Session);
        assert_eq!(handle.used(), 80);
        // Global only reflects the committed 80.
        assert_eq!(global.used(), 80);
    }

    #[test]
    fn session_reservation_charges_global() {
        let global = Arc::new(MemoryBudget::new(1000));
        let handle = Arc::new(SessionMemoryHandle::new(Arc::clone(&global), 500));
        let r = handle.reserve(300).expect("fits both");
        assert_eq!(handle.used(), 300);
        assert_eq!(global.used(), 300, "session charge flows to global");
        drop(r);
        assert_eq!(handle.used(), 0);
        assert_eq!(global.used(), 0, "drop releases both counters");
    }

    #[test]
    fn global_exhaustion_rolls_back_session_charge() {
        let global = Arc::new(MemoryBudget::new(100));
        // Session cap is generous; the global budget is the binding constraint.
        let handle = Arc::new(SessionMemoryHandle::new(Arc::clone(&global), 1000));
        let _fill = global.reserve(80).expect("reserve global directly");
        // 20 free globally; this asks for 50 (under session cap, over global).
        let err = handle.reserve(50).expect_err("global exhausted");
        assert_eq!(err.scope, Scope::Global);
        assert_eq!(
            handle.used(),
            0,
            "session charge rolled back on global failure"
        );
        assert_eq!(global.used(), 80, "global unchanged");
    }
}
