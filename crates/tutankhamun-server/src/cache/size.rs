//! Parsing the `--cache-size` CLI value.
//!
//! Accepts:
//! - bytes-with-units: `10GB`, `1.5TB`, `512MiB`. Suffixes are
//!   case-insensitive. Binary (`KiB`/`MiB`/`GiB`/`TiB`) use 1024;
//!   decimal (`KB`/`MB`/`GB`/`TB` and unsuffixed `K`/`M`/`G`/`T`)
//!   use 1000. Bare integers are bytes.
//! - percent-of-disk: `50%` resolves to that fraction of the total
//!   capacity of the filesystem containing `reference_path` at the
//!   moment of parsing.

use std::path::Path;

use anyhow::{Context, Result, anyhow, bail};

/// Resolve a `--cache-size` argument to a byte count. `reference_path`
/// is the cache directory (or any path on the same filesystem) — only
/// consulted when `raw` ends in `%`.
pub fn parse_cache_size(raw: &str, reference_path: &Path) -> Result<u64> {
    let raw = raw.trim();
    if raw.is_empty() {
        bail!("cache size must not be empty");
    }
    if let Some(pct) = raw.strip_suffix('%') {
        return parse_percent(pct, reference_path);
    }
    parse_units(raw)
}

fn parse_percent(pct: &str, reference_path: &Path) -> Result<u64> {
    let pct: f64 = pct
        .trim()
        .parse()
        .with_context(|| format!("percent value {pct:?} is not a number"))?;
    if !(0.0..=100.0).contains(&pct) {
        bail!("percent value {pct} out of range (expected 0..=100)");
    }
    let total = fs2::total_space(reference_path).with_context(|| {
        format!(
            "stat filesystem at {} (needed for percent cache size)",
            reference_path.display()
        )
    })?;
    #[allow(
        clippy::cast_precision_loss,
        clippy::cast_sign_loss,
        clippy::cast_possible_truncation
    )]
    let bytes = ((total as f64) * (pct / 100.0)) as u64;
    Ok(bytes)
}

fn parse_units(raw: &str) -> Result<u64> {
    let (number_part, multiplier) = split_unit(raw)?;
    let value: f64 = number_part
        .parse()
        .with_context(|| format!("size value {number_part:?} is not a number"))?;
    if !value.is_finite() || value < 0.0 {
        bail!("cache size must be a non-negative finite number (got {value})");
    }
    #[allow(
        clippy::cast_precision_loss,
        clippy::cast_sign_loss,
        clippy::cast_possible_truncation
    )]
    let bytes = (value * (multiplier as f64)) as u64;
    Ok(bytes)
}

fn split_unit(raw: &str) -> Result<(&str, u64)> {
    let split_at = raw
        .find(|c: char| c.is_ascii_alphabetic())
        .unwrap_or(raw.len());
    let (number, unit) = raw.split_at(split_at);
    let multiplier = match unit.trim().to_ascii_uppercase().as_str() {
        "" | "B" => 1,
        "K" | "KB" => 1_000,
        "M" | "MB" => 1_000_000,
        "G" | "GB" => 1_000_000_000,
        "T" | "TB" => 1_000_000_000_000,
        "KIB" => 1_024,
        "MIB" => 1_024 * 1_024,
        "GIB" => 1_024 * 1_024 * 1_024,
        "TIB" => 1_024_u64.pow(4),
        other => return Err(anyhow!("unknown size unit {other:?}")),
    };
    Ok((number.trim(), multiplier))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn decimal_units() {
        assert_eq!(parse_units("10").unwrap(), 10);
        assert_eq!(parse_units("10B").unwrap(), 10);
        assert_eq!(parse_units("10K").unwrap(), 10_000);
        assert_eq!(parse_units("10KB").unwrap(), 10_000);
        assert_eq!(parse_units("10GB").unwrap(), 10_000_000_000);
        assert_eq!(parse_units("1.5TB").unwrap(), 1_500_000_000_000);
    }

    #[test]
    fn binary_units() {
        assert_eq!(parse_units("1KiB").unwrap(), 1_024);
        assert_eq!(parse_units("1MiB").unwrap(), 1_048_576);
        assert_eq!(parse_units("1GiB").unwrap(), 1_073_741_824);
    }

    #[test]
    fn case_insensitive_units() {
        assert_eq!(parse_units("10gb").unwrap(), 10_000_000_000);
        assert_eq!(parse_units("10Gb").unwrap(), 10_000_000_000);
    }

    #[test]
    fn rejects_negative() {
        assert!(parse_units("-1GB").is_err());
    }

    #[test]
    fn rejects_nan_and_inf() {
        assert!(parse_units("nanGB").is_err());
        assert!(parse_units("infGB").is_err());
        assert!(parse_units("-infGB").is_err());
    }

    #[test]
    fn rejects_unknown_unit() {
        assert!(parse_units("10XB").is_err());
    }

    #[test]
    fn percent_resolves_against_disk() {
        let tmp = tempfile::tempdir().unwrap();
        let total = fs2::total_space(tmp.path()).unwrap();
        let half = parse_cache_size("50%", tmp.path()).unwrap();
        assert!(half > 0);
        assert!(half <= total / 2 + 1);
        assert!(half >= total / 2 - 1);
    }

    #[test]
    fn percent_out_of_range() {
        let tmp = tempfile::tempdir().unwrap();
        assert!(parse_cache_size("101%", tmp.path()).is_err());
        assert!(parse_cache_size("-5%", tmp.path()).is_err());
    }
}
