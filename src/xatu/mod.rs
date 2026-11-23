//! An opt-in history source for Ethereum mainnet: logs and block timestamps from ethPandaOps' public
//! Xatu Parquet files, handed off to the normal RPC source for everything the dataset does not hold.
//!
//! It is a [`Source`] wrapper, not a second indexer. Decode, the hot store, sealing and serving see
//! the same `Log`s and the same timestamps they would have had from `eth_getLogs` and the block
//! headers; only where the bytes came from changes. Off unless the operator asks (`nuthatch dev
//! --history-source xatu`), and it never touches nest data identity, so a nest indexed this way has
//! the NID it would have had from RPC alone.
//!
//! **Shape.** The dataset is cut in 1,000-block chunks, one logs file and one block file per chunk
//! (`canonical_execution_logs/1000/<start>.parquet`, `canonical_execution_block/1000/<start>.parquet`).
//! A request is split at chunk boundaries. A chunk the dataset serves answers its part; every other
//! part (above the dataset's head, before its start, or a chunk that failed) is asked of the inner
//! RPC source, in contiguous runs. Parts are disjoint and visited in ascending order, so the handoff
//! has no gap and no duplicate by construction.
//!
//! **Trust.** A chunk is used only once it is past the chain's finality (so a cached hash can never
//! hide a reorg from the indexer's reorg check) and after three checks, each cheap and
//! deterministic: its block file has exactly one row per block, and, where the file carries parent
//! hashes, an unbroken parent-hash chain; the RPC's hash and timestamp for the chunk's *last* block (and
//! its first, in older files without parent hashes) equal the dataset's; and the logs file opens with the
//! columns this reader needs. That is one or two RPC calls per 1,000 blocks, made before the large
//! logs file is downloaded. The parent-hash chain is the file agreeing with itself, not the chain
//! vouching for it: the hashes of the blocks between the anchors are the dataset's word. Any
//! mismatch, fetch error or parse error falls back to RPC for that chunk with a warning, never an
//! empty answer. What it cannot check is that a block's logs are *complete*: the dataset carries no
//! receipts root.
//!
//! **Memory.** The compressed logs of at most `KEEP_CHUNKS` chunks (4 x 35 to 50 MB on mainnet); the
//! hashes and timestamps of the last `KEEP_HEADERS` chunks (about 100 KB each), so a wide window's
//! header lookups do not outlive its logs; and the decoded rows of the filter last asked of each held
//! chunk, within `MATCHED_BUDGET` (256 MiB) between them, least recently used dropped first. A filter
//! matching more than that of one chunk is decoded per window instead. About 0.5 GB held at most,
//! plus the decode in flight, inside the 2 GB cursor budget; a backfill's many small windows still
//! decode a chunk once.

mod chunks;
mod columns;
mod config;
mod fetch;
mod handoff;
#[cfg(test)]
mod tests;

use crate::source::Source;
use chunks::State;
use std::sync::atomic::AtomicU64;
use std::sync::{Arc, Mutex};
use std::time::Duration;

pub use config::with_history_source;
pub use fetch::{Fetch, HttpFetch};
pub use nuthatch_decode::rpc::Log;

/// Where ethPandaOps publishes the mainnet files. Overridable, so a mirror works.
pub const DEFAULT_BASE_URL: &str = "https://data.ethpandaops.io/xatu/mainnet/databases/default";

/// Blocks per file in the public dataset.
pub const DATASET_CHUNK: u64 = 1000;

/// Counters a benchmark or a log line can read.
#[derive(Debug, Default)]
pub struct Stats {
    /// Chunks downloaded, verified and used.
    pub chunks_loaded: AtomicU64,
    /// Compressed bytes downloaded.
    pub bytes: AtomicU64,
    /// RPC calls spent checking a chunk against the chain (one per chunk).
    pub anchor_calls: AtomicU64,
    /// Chunks that failed and were handed to RPC.
    pub fallbacks: AtomicU64,
    /// Block-timestamp and hash lookups answered from the dataset.
    pub header_hits: AtomicU64,
    /// Chunks the dataset had no file for (its head, or a mirror without the layout), then asked
    /// of the RPC.
    pub absent: AtomicU64,
}

pub struct XatuSource {
    inner: Arc<dyn Source>,
    fetch: Arc<dyn Fetch>,
    base: String,
    chunk_len: u64,
    state: Mutex<State>,
    load: tokio::sync::Mutex<()>,
    /// Held while a chunk is decoded whole: one such decode in flight, so the decoded-rows budget
    /// holds while it runs and concurrent windows of one chunk decode it once.
    decoding: tokio::sync::Mutex<()>,
    /// The highest block seen to be final, so old chunks need no finality call each.
    final_seen: AtomicU64,
    keep_chunks: usize,
    keep_headers: usize,
    matched_budget: usize,
    /// How long a transient failure (a stalled host, an RPC that would not answer the anchor) is
    /// remembered before the chunk is tried again.
    retry_after: Duration,
    pub stats: Stats,
}

impl XatuSource {
    pub fn new(inner: Arc<dyn Source>, fetch: Arc<dyn Fetch>, base: &str) -> Self {
        Self::with_chunk_len(inner, fetch, base, DATASET_CHUNK)
    }

    /// For fixtures, which are cut in smaller chunks than the public files.
    pub fn with_chunk_len(
        inner: Arc<dyn Source>,
        fetch: Arc<dyn Fetch>,
        base: &str,
        chunk_len: u64,
    ) -> Self {
        Self {
            inner,
            fetch,
            base: base.trim_end_matches('/').to_string(),
            chunk_len,
            state: Mutex::new(State::default()),
            load: tokio::sync::Mutex::new(()),
            decoding: tokio::sync::Mutex::new(()),
            final_seen: AtomicU64::new(0),
            keep_chunks: chunks::KEEP_CHUNKS,
            keep_headers: chunks::KEEP_HEADERS,
            matched_budget: chunks::MATCHED_BUDGET,
            retry_after: chunks::TRANSIENT_FOR,
            stats: Stats::default(),
        }
    }

    fn url(&self, table: &str, start: u64) -> String {
        format!("{}/{table}/{}/{start}.parquet", self.base, self.chunk_len)
    }

    fn start_of(&self, block: u64) -> u64 {
        block / self.chunk_len * self.chunk_len
    }
}
