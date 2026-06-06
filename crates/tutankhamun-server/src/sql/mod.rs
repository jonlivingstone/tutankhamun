//! `DataFusion` `TableProvider` over a Tutankhamun dataset.
//!
//! - **Selectable columns:** every declared field. Metric/Int are read
//!   from forward columns as `Int64`; String values are reconstructed
//!   per-doc from the inverted index at scan time (`O(num_docs)` per
//!   shard per field); the time field is presented as a `Timestamp`.
//! - **Filter pushdown:** equality and range on String / Int (and the
//!   time) fields via [`crate::shard::FilterClause`]; other predicates
//!   `DataFusion` evaluates after the scan returns rows.
//! - **Time-range shard pruning:** a predicate on the time field skips
//!   shards whose `[time_range_start, time_range_end]` can't intersect
//!   the query window, before they're fetched.
//! - **Aggregation pushdown:** a single-column `Int` `GROUP BY` with
//!   `COUNT(*)` / `SUM` / `MIN` / `MAX` runs through an FTGS scan (see
//!   [`group_by`]); other aggregation `DataFusion` computes above the
//!   scan.
//!
//! See `.docs/tutankhamun.md` §3.2 for the design.

pub mod exec;
pub mod group_by;
pub mod provider;
pub mod pushdown;

pub use group_by::session_context;
pub use provider::TutankhamunTableProvider;

#[cfg(test)]
mod tests;
