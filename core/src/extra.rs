//! What a window reads beyond its logs: pinned reads and top-level calls, which add rows, and the
//! documents rows name, which hold a seal. The node resolves them; a tab declares none of them.
//!
//! These are the data path, not hooks around it: each is called at the one point in the window where
//! it was always called, so the rows it adds and the keys they take do not move.

use crate::source::Source;
use crate::store::HotStore;
use anyhow::Result;
use nuthatch_decode::registry::DecodedRow;
use serde_json::Value;
use std::collections::HashMap;

#[async_trait::async_trait]
pub trait ExtraRows: Send + Sync {
    /// Blocks in `[next, to]` a pinned read samples. They need a timestamp though they may hold no log.
    fn sampled_blocks(&self, _next: u64, _to: u64) -> Vec<u64> {
        Vec::new()
    }

    /// Whether a pinned read needs the window's headers.
    fn keeps_headers(&self) -> bool {
        false
    }

    /// The window's top-level call rows (RFC-0038 §5).
    async fn top_level_calls(
        &self,
        _source: &dyn Source,
        _next: u64,
        _to: u64,
    ) -> Result<Vec<DecodedRow>> {
        Ok(Vec::new())
    }

    /// How many of `rows` name no document that could be resolved (RFC-0037).
    fn unreadable(&self, _rows: &[&DecodedRow]) -> usize {
        0
    }

    /// The window's pinned-read rows (RFC-0023 tier 3). A
    /// [`WindowForked`](crate::window::WindowForked) in the error refuses the window for a retry.
    async fn pinned_reads(
        &self,
        _source: &dyn Source,
        _rows: &[DecodedRow],
        _next: u64,
        _to: u64,
        _timestamps: &HashMap<u64, u64>,
        _headers: Option<&HashMap<u64, Value>>,
    ) -> Result<Vec<DecodedRow>> {
        Ok(Vec::new())
    }

    /// The window from `next` is about to commit.
    fn committing(&self, _next: u64) {}

    /// How far a seal may reach with `finalized_through` final, or `None` to seal nothing now.
    fn seal_through(&self, _store: &dyn HotStore, finalized_through: u64) -> Result<Option<u64>> {
        Ok(Some(finalized_through))
    }
}

/// Reads nothing beyond the logs.
pub struct NoExtraRows;

impl ExtraRows for NoExtraRows {}
