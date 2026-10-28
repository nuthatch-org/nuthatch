//! The ingestion `Source`'s node implementations. The trait and [`LogFilter`] live in
//! `nuthatch-core` (RFC-0075 S1), so a tab's source implements the one the cursor reads.

use anyhow::Result;
use std::collections::HashMap;

use crate::rpc::RpcClient;
pub use nuthatch_core::source::*;

#[async_trait::async_trait]
impl Source for RpcClient {
    fn forget_cached_above(&self, block: u64) {
        self.forget_timestamps_above(block);
    }

    fn refuses_topic0_only(&self, err: &anyhow::Error) -> bool {
        crate::rpc::filter_refused(err)
    }

    async fn tip(&self) -> Result<u64> {
        self.block_number().await
    }

    async fn block_hash(&self, number: u64) -> Result<Option<String>> {
        // Disambiguate from this trait method - call the inherent one.
        RpcClient::block_hash(self, number).await
    }

    async fn block_hashes(&self, blocks: &[u64]) -> Result<HashMap<u64, String>> {
        Ok(RpcClient::block_headers(self, blocks)
            .await?
            .into_iter()
            .filter_map(|(b, h)| Some((b, h.get("hash")?.as_str()?.to_string())))
            .collect())
    }

    async fn finalized(&self) -> Result<Option<u64>> {
        self.finalized_block().await
    }

    async fn block_timestamps(&self, blocks: &[u64]) -> Result<HashMap<u64, u64>> {
        RpcClient::block_timestamps(self, blocks).await
    }

    async fn block_record(&self, number: u64) -> Result<Option<(String, Option<u64>)>> {
        RpcClient::block_record(self, number).await
    }

    async fn block_headers(&self, blocks: &[u64]) -> Result<HashMap<u64, serde_json::Value>> {
        RpcClient::block_headers(self, blocks).await
    }

    async fn block_bodies(&self, blocks: &[u64]) -> Result<HashMap<u64, serde_json::Value>> {
        RpcClient::block_bodies(self, blocks).await
    }

    async fn logs(&self, filter: &LogFilter, from: u64, to: u64) -> Result<Vec<Log>> {
        self.get_logs(filter.addresses(), filter.topic0s(), from, to)
            .await
    }
}

/// A chain's RPC pool behind its cursor, replaceable while the cursor runs (RFC-0073 FR9).
///
/// Each call takes the pool current when it starts and finishes on it, so a replacement never cuts a
/// request off; the old pool is dropped once its last call returns.
pub struct LivePool {
    current: arc_swap::ArcSwap<RpcClient>,
    fallback: Vec<String>,
}

impl LivePool {
    /// `fallback` is what `--rpc-fallback` added, kept behind every pool that replaces this one.
    pub fn new(client: RpcClient, fallback: Vec<String>) -> Self {
        Self {
            current: arc_swap::ArcSwap::from_pointee(client),
            fallback,
        }
    }

    pub fn current(&self) -> std::sync::Arc<RpcClient> {
        self.current.load_full()
    }

    pub fn fallback(&self) -> &[String] {
        &self.fallback
    }

    /// Serve every call from now on through `client`.
    pub fn replace(&self, client: RpcClient) {
        self.current.store(std::sync::Arc::new(client));
    }
}

#[async_trait::async_trait]
impl Source for LivePool {
    fn forget_cached_above(&self, block: u64) {
        Source::forget_cached_above(&*self.current(), block);
    }

    fn refuses_topic0_only(&self, err: &anyhow::Error) -> bool {
        crate::rpc::filter_refused(err)
    }

    async fn tip(&self) -> Result<u64> {
        Source::tip(&*self.current()).await
    }

    async fn block_hash(&self, number: u64) -> Result<Option<String>> {
        Source::block_hash(&*self.current(), number).await
    }

    async fn block_hashes(&self, blocks: &[u64]) -> Result<HashMap<u64, String>> {
        Source::block_hashes(&*self.current(), blocks).await
    }

    async fn finalized(&self) -> Result<Option<u64>> {
        Source::finalized(&*self.current()).await
    }

    async fn block_timestamps(&self, blocks: &[u64]) -> Result<HashMap<u64, u64>> {
        Source::block_timestamps(&*self.current(), blocks).await
    }

    async fn block_record(&self, number: u64) -> Result<Option<(String, Option<u64>)>> {
        Source::block_record(&*self.current(), number).await
    }

    async fn block_headers(&self, blocks: &[u64]) -> Result<HashMap<u64, serde_json::Value>> {
        Source::block_headers(&*self.current(), blocks).await
    }

    async fn block_bodies(&self, blocks: &[u64]) -> Result<HashMap<u64, serde_json::Value>> {
        Source::block_bodies(&*self.current(), blocks).await
    }

    async fn logs(&self, filter: &LogFilter, from: u64, to: u64) -> Result<Vec<Log>> {
        Source::logs(&*self.current(), filter, from, to).await
    }
}
