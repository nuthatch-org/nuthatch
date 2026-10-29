//! What the cursor counts as it goes. The node publishes these as process-wide Prometheus series; a
//! tab has no `/metrics`, so it takes [`NoMetrics`] and loses nothing it could have served.

pub trait CursorMetrics: Send + Sync {
    fn set_last_block(&self, _block: u64) {}
    fn add_rows_decoded(&self, _n: u64) {}
    fn add_rows_sealed(&self, _n: u64) {}
    fn set_sealed_through(&self, _block: u64) {}
    /// One tip-path seal attempt's error, or `None` when it did not fail.
    fn record_seal(&self, _error: Option<String>) {}
    fn add_undecoded(&self, _contract: &str, _topic0: &str, _n: u64) {}
    fn record_reorg(&self, _depth: u64) {}
    /// Returns the running count, which paces the warning a missed checkpoint logs.
    fn inc_checkpoints_missed(&self) -> u64 {
        0
    }
    fn add_ipfs_unreadable(&self, _n: u64) {}
}

/// Counts nothing.
pub struct NoMetrics;

impl CursorMetrics for NoMetrics {}

/// Distinct (contract, topic0) pairs a nest's undecoded tally keeps before folding the rest into
/// `("other", "other")`, so a label set cannot grow without bound.
pub const UNDECODED_PAIRS_MAX: usize = 256;

/// Logs whose topic0 a declared decoder claims and whose body did not decode against it (RFC-0049
/// §2, RFC-0073 FR14), by contract alias or template name and topic0.
#[derive(Default)]
pub struct UndecodedTally {
    pairs: std::collections::BTreeMap<(String, String), u64>,
}

impl UndecodedTally {
    pub const fn new() -> Self {
        Self {
            pairs: std::collections::BTreeMap::new(),
        }
    }

    pub fn add(&mut self, contract: &str, topic0: &str, n: u64) {
        let key = (contract.to_string(), topic0.to_string());
        let key = if self.pairs.contains_key(&key) || self.pairs.len() < UNDECODED_PAIRS_MAX {
            key
        } else {
            ("other".to_string(), "other".to_string())
        };
        *self.pairs.entry(key).or_default() += n;
    }

    pub fn pairs(&self) -> &std::collections::BTreeMap<(String, String), u64> {
        &self.pairs
    }

    pub fn total(&self) -> u64 {
        self.pairs.values().sum()
    }

    /// The `undecoded` block `/ready` and `/nest` carry: the total and the five largest pairs.
    pub fn summary(&self) -> serde_json::Value {
        let mut top: Vec<_> = self.pairs.iter().collect();
        top.sort_by(|a, b| b.1.cmp(a.1).then_with(|| a.0.cmp(b.0)));
        let top: Vec<_> = top
            .into_iter()
            .take(5)
            .map(|((contract, topic0), logs)| {
                serde_json::json!({ "contract": contract, "topic0": topic0, "logs": logs })
            })
            .collect();
        serde_json::json!({ "total": self.total(), "top": top })
    }
}
