//! Threshold flags (RFC-0008 C3): the simple half of the flag family. A **threshold flag** is a
//! single transfer whose value reaches a configured amount (travel-rule style, e.g. a $3,000-equivalent
//! in token base units - thresholds are config, not code, and there is no currency conversion in-core).
//!
//! Unlike velocity, a threshold flag needs no aggregation: it is a pure per-transfer predicate, so it
//! becomes an append-only `threshold_flag` annotation keyed by its transfer's `{block}-{log}`. That
//! makes reorg handling free - the annotation is block-keyed, so it rolls back (retracts) with its
//! transfer. Comparison is on the shipped **i128** path, so a value past
//! i64 is flagged, not silently truncated (a threshold view on i64 would be a compliance liability).

use serde_json::{json, Value};

/// Build the `threshold_flag` annotation for a transfer, or `None` if it is below `threshold`. The
/// key is the transfer's `{block:012}-{log:06}` plus a `-thr` discriminator (distinct from the
/// transfer row at the same log), so both coexist and roll back together.
pub fn threshold_annotation(
    from: &str,
    to: &str,
    value: i128,
    block: u64,
    log_index: u64,
    tx_hash: &str,
    threshold: i128,
) -> Option<(String, Value)> {
    if value < threshold {
        return None;
    }
    let key = format!("{block:012}-{log_index:06}-thr");
    let ann = json!({
        "table": "threshold_flag",
        "kind": "threshold_flag",
        "rule": "threshold",
        "block_number": block,
        "log_index": log_index,
        "tx_hash": tx_hash,
        "from": from,
        "to": to,
        "value": value.to_string(),
        "threshold": threshold.to_string(),
    });
    Some((key, ann))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn flags_at_or_above_threshold_only() {
        let t = 1_000i128;
        assert!(threshold_annotation("0xa", "0xb", 999, 10, 0, "0xt", t).is_none());
        assert!(threshold_annotation("0xa", "0xb", 1_000, 10, 0, "0xt", t).is_some());
        let (key, ann) = threshold_annotation("0xa", "0xb", 5_000, 10, 1, "0xt", t).unwrap();
        assert_eq!(key, "000000000010-000001-thr");
        assert_eq!(ann["value"], "5000");
        assert_eq!(ann["threshold"], "1000");
        assert_eq!(ann["table"], "threshold_flag");
    }

    /// The C3 gate item: threshold comparison runs on i128 - a transfer above i64::MAX is flagged,
    /// not lost to truncation. (~100 tokens of an 18-decimal token overflows i64.)
    #[test]
    fn flags_values_beyond_i64() {
        let big: i128 = 100_000_000_000_000_000_000; // 1e20 > i64::MAX (~9.2e18)
        let threshold: i128 = 50_000_000_000_000_000_000; // 5e19, also > i64::MAX
        assert!(big > i64::MAX as i128 && threshold > i64::MAX as i128);
        let flagged = threshold_annotation("0xwhale", "0xexch", big, 1, 0, "0xt", threshold);
        assert!(
            flagged.is_some(),
            "a value beyond i64 must still be compared correctly"
        );
        assert_eq!(flagged.unwrap().1["value"], big.to_string());
        // And a large-but-below value does not flag.
        assert!(
            threshold_annotation("0xa", "0xb", threshold - 1, 1, 0, "0xt", threshold).is_none()
        );
    }
}
