//! `--filter` argument parsing — the `<field>=<value>` CLI DSL shared by the
//! `query` and `shard query` verbs.

use tutankhamun_server::shard::{self, FilterClause, FilterOp};

/// Parse every `--filter` argument in `raw` into [`FilterClause`]s in
/// the same order. Borrows from `raw` — caller keeps it alive for the
/// duration of the query.
pub(crate) fn parse_filters(raw: &[String]) -> anyhow::Result<Vec<shard::FilterClause<'_>>> {
    raw.iter().map(|s| parse_filter(s)).collect()
}

/// Parse a `--filter` argument into a structured [`FilterClause`].
///
/// Syntax:
/// - `field=term`        — exact match (the existing form).
/// - `field=lo..hi`      — inclusive range over the field's index.
/// - `field=lo..` / `field=..hi` — open-ended range.
///
/// Splits on the first `=` (so terms may contain further `=`s);
/// inclusive-range bounds are split on the first `..` in the RHS.
/// Both ends of a range being empty is rejected — it would match
/// every doc, which the user almost certainly didn't mean.
pub(crate) fn parse_filter(s: &str) -> anyhow::Result<shard::FilterClause<'_>> {
    let (field, rhs) = s
        .split_once('=')
        .ok_or_else(|| anyhow::anyhow!("filter must be `<field>=<value>` (got {s:?})"))?;
    if field.is_empty() {
        anyhow::bail!("filter field name must be non-empty (got {s:?})");
    }
    let op = if let Some((lo, hi)) = rhs.split_once("..") {
        let lo = (!lo.is_empty()).then_some(lo);
        let hi = (!hi.is_empty()).then_some(hi);
        if lo.is_none() && hi.is_none() {
            anyhow::bail!(
                "range filter must have at least one bound (got `{s}`); use `field=value` for exact match"
            );
        }
        FilterOp::Range { lo, hi }
    } else if rhs.is_empty() {
        anyhow::bail!("filter must be `<field>=<value>` with non-empty value (got {s:?})");
    } else {
        FilterOp::Equals(rhs)
    };
    Ok(FilterClause { field, op })
}

#[cfg(test)]
mod tests {
    use super::parse_filter;
    use tutankhamun_server::shard::FilterOp;

    #[test]
    fn parse_filter_equals_simple() {
        let c = parse_filter("country=us").unwrap();
        assert_eq!(c.field, "country");
        assert!(matches!(c.op, FilterOp::Equals("us")));
    }

    #[test]
    fn parse_filter_equals_allows_equals_in_value() {
        let c = parse_filter("query=a=b").unwrap();
        assert_eq!(c.field, "query");
        assert!(matches!(c.op, FilterOp::Equals("a=b")));
    }

    #[test]
    fn parse_filter_range_both_bounds() {
        let c = parse_filter("v=100..200").unwrap();
        assert_eq!(c.field, "v");
        assert!(matches!(
            c.op,
            FilterOp::Range {
                lo: Some("100"),
                hi: Some("200"),
            }
        ));
    }

    #[test]
    fn parse_filter_range_open_lower() {
        let c = parse_filter("v=..200").unwrap();
        assert!(matches!(
            c.op,
            FilterOp::Range {
                lo: None,
                hi: Some("200"),
            }
        ));
    }

    #[test]
    fn parse_filter_range_open_upper() {
        let c = parse_filter("v=100..").unwrap();
        assert!(matches!(
            c.op,
            FilterOp::Range {
                lo: Some("100"),
                hi: None,
            }
        ));
    }

    #[test]
    fn parse_filter_rejects_double_open_range() {
        let err = parse_filter("v=..").expect_err("`..` alone matches everything; rejected");
        let msg = err.to_string();
        assert!(msg.contains("at least one bound"), "{msg}");
    }

    #[test]
    fn parse_filter_rejects_empty_value() {
        assert!(parse_filter("v=").is_err());
    }

    #[test]
    fn parse_filter_rejects_missing_equals() {
        assert!(parse_filter("v100").is_err());
    }

    #[test]
    fn parse_filter_rejects_empty_field() {
        assert!(parse_filter("=us").is_err());
    }
}
