//! `DataFusion` `TableProvider` over a Tutankhamun dataset.
//!
//! Tier 1 surface (this module):
//!
//! - **Selectable columns:** every declared field. Metric/Int are
//!   read from forward columns; String values are reconstructed
//!   per-doc from the inverted index at scan time (`O(num_docs)` per
//!   shard per field — cheap relative to bitmap deserialise).
//! - **Pushed-down filters:** equality and range on String / Int
//!   fields via [`crate::shard::FilterClause`]; anything else
//!   `DataFusion` evaluates after we return rows.
//! - **No shard-time pruning yet.** Tier 1 walks every discovered
//!   shard. Time-range pushdown is a follow-up.
//! - **No per-doc `time` column.** Ingest stores the shard's time
//!   range, not each doc's time. SQL queries referencing `time`
//!   error at schema validation; `WHERE time > ...` works at the
//!   shard-pruning level (when that lands).
//! - **No aggregation pushdown.** `DataFusion` executes `SUM`,
//!   `COUNT`, `GROUP BY`, joins, windows, CTEs, etc. in memory
//!   above our scan.
//!
//! See `.docs/tutankhamun.md` §3.2 for the longer-term plan
//! (Tier 2 aggregation pushdown, Tier 3 FTGS + GROUP BY pushdown,
//! sessions, `FlightSQL` wire).

pub mod exec;
pub mod provider;
pub mod pushdown;

pub use provider::TutankhamunTableProvider;

#[cfg(test)]
mod tests;
