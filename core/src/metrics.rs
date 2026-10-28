//! What the cursor counts as it goes. The node publishes these as process-wide Prometheus series; a
//! tab has no `/metrics`, so it takes [`NoMetrics`] and loses nothing it could have served.

pub trait CursorMetrics: Send + Sync {
    fn set_last_block(&self, _block: u64) {}
    fn add_rows_decoded(&self, _n: u64) {}
    fn add_rows_sealed(&self, _n: u64) {}
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
