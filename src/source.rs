//! The ingestion `Source` - the seam between "where blocks come from" and everything downstream.
//!
//! Decode, hot store, sealing, IVM, and serving are all oblivious to whether a block arrived by
//! RPC polling or was handed to us in-process by a colocated reth node. That obliviousness is the
//! whole point of this trait: a colocated-node source (RFC-0003, not built) would be a new `Source`
//! impl, never a fork of the indexing logic (per the standing brief - no `#[cfg]` forks of business
//! logic).
//!
//! - `RpcSource`: polls a JSON-RPC endpoint. The only source today.

use anyhow::Result;
use std::collections::HashMap;

use crate::rpc::{Log, RpcClient};

/// A `getLogs` filter that is known not to mean *every log on the chain*.
///
/// An `eth_getLogs` with an empty address list **and** an empty topic list does not mean "no logs" to
/// a node - it matches everything. A nest with no contract at all (`[extract] blocks = true` and no
/// `[[contracts]]`, OBIB case 3) produces exactly that pair, so the request succeeds and returns an
/// answer that is wrong rather than absent: every log on the chain, fetched and then discarded, since
/// no log can decode without a matching address or topic. The bench harness builds such a registry
/// directly and does reach it; `build_nest` currently refuses the config outright, so the *nest*
/// paths are guarded ahead of the configuration rather than behind it - see
/// `a_contract_free_nest_cannot_be_built_at_all_today` in `indexer.rs`.
///
/// This type exists because the per-call-site guard was written **three times and missed a path each
/// time** - #421 guarded `backfill_direct`, #429 guarded `fetch_logs_splitting`, and both left the two
/// live tip loops issuing it forever (#432). Enumerating call sites is the losing move: the set grows,
/// and a new one is a silent regression rather than a compile error.
///
/// So the guard moves into the type. [`LogFilter::new`] is the only constructor and returns `None` for
/// the empty-and-empty case, and [`Source::logs`] takes a `&LogFilter` rather than two slices. A caller
/// therefore *cannot* hold a value that means every log on the chain, and a new call site cannot ask
/// for one without first deciding what "nothing to ask for" means there. The check is at the type
/// boundary, not at each of the places that fetch.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LogFilter {
    addresses: Vec<String>,
    topic0s: Vec<String>,
}

impl LogFilter {
    /// The only way to build one. `None` when both halves are empty, which is the case that would
    /// otherwise ask a node for the entire chain's logs.
    ///
    /// Note that *one* empty half is fine and load-bearing: a factory nest deliberately drops the
    /// address filter and matches on topic0 alone, because its children are not known in advance.
    pub fn new(addresses: &[String], topic0s: &[String]) -> Option<Self> {
        (!addresses.is_empty() || !topic0s.is_empty()).then(|| Self {
            addresses: addresses.to_vec(),
            topic0s: topic0s.to_vec(),
        })
    }

    /// The addresses to match. Possibly empty - a factory nest matches on topic0 alone.
    pub fn addresses(&self) -> &[String] {
        &self.addresses
    }

    /// The topic0s to match. Possibly empty - a nest may filter by address alone.
    pub fn topic0s(&self) -> &[String] {
        &self.topic0s
    }
}

/// A source for a role that **never polls one** (#815).
///
/// `serve_role` owns no cursor - its own comment says "No `Source` is ever polled on this role; the
/// parameter exists for the ingest half we discard" - and yet it built an [`RpcClient`] from
/// `config.nest.rpc_urls` to satisfy `build_nest`, whose first parameter is named `_source` and is
/// unused. A vestigial dependency, and not a harmless one: a nest with `rpc_urls = []` could not be
/// served at all, failing with `no RPC URLs configured`.
///
/// That shape is not exotic. It is exactly a **finished, fully-sealed nest** with no chain behind it
/// any more - including the RFC-0016 eval fixture, which is why this was found. The production
/// read-only shadow on the Lodestar box only starts because it happens to carry URLs it never uses.
///
/// Every method errors rather than returning an empty success. If a future change starts polling on
/// a serve-only role, it must fail loudly at the first call rather than silently indexing nothing -
/// an empty result here would be indistinguishable from "the chain had nothing", which is the
/// asymmetry `block_headers` already documents below.
pub struct UnpolledSource;

#[async_trait::async_trait]
impl Source for UnpolledSource {
    async fn tip(&self) -> Result<u64> {
        anyhow::bail!(
            "this role serves a nest without indexing it and has no ingestion source; something \
             asked it for the chain tip, which means a cursor is running where none should be"
        )
    }

    async fn block_hash(&self, _number: u64) -> Result<Option<String>> {
        anyhow::bail!(
            "this role serves a nest without indexing it and has no ingestion source; something \
             asked it for a block hash, which means reorg detection is running where none should be"
        )
    }

    async fn logs(&self, _filter: &LogFilter, _from: u64, _to: u64) -> Result<Vec<Log>> {
        anyhow::bail!(
            "this role serves a nest without indexing it and has no ingestion source; something \
             asked it for logs, which means an ingest loop is running where none should be"
        )
    }
}

/// Everything the indexer needs from an ingestion source. Pull-shaped: the single-cursor loop asks
/// for the tip, verifies canonical hashes (reorg detection), and requests decoded logs for a range.
#[async_trait::async_trait]
pub trait Source: Send + Sync {
    /// Latest block the source can serve.
    async fn tip(&self) -> Result<u64>;

    /// Canonical block hash at `number`, or `None` if the source can't answer (retry later).
    async fn block_hash(&self, number: u64) -> Result<Option<String>>;

    /// [`Source::block_hash`] for several blocks. A block the source cannot answer is absent.
    async fn block_hashes(&self, blocks: &[u64]) -> Result<HashMap<u64, String>> {
        let mut out = HashMap::new();
        for &b in blocks {
            if let Some(h) = self.block_hash(b).await? {
                out.insert(b, h);
            }
        }
        Ok(out)
    }

    /// The source's `finalized` block number, if it exposes one (L1-aware on an L2). `None` means
    /// "no finalized signal available" - the caller falls back to a depth-based finality policy.
    async fn finalized(&self) -> Result<Option<u64>> {
        Ok(None)
    }

    /// Unix timestamps (seconds) for the given blocks (default: none). Used to populate every row's
    /// implicit `block_timestamp` column; a block omitted or answered as 0 refuses its window.
    async fn block_timestamps(&self, _blocks: &[u64]) -> Result<HashMap<u64, u64>> {
        Ok(HashMap::new())
    }

    /// Hash and timestamp of one block, for a window checkpoint. `None` when the hash is unavailable;
    /// the timestamp is best-effort. An RPC source answers from one header (#1494); the default asks
    /// the two methods above, in the order the checkpoint always has.
    async fn block_record(&self, number: u64) -> Result<Option<(String, Option<u64>)>> {
        let Some(hash) = self.block_hash(number).await? else {
            return Ok(None);
        };
        let ts = self
            .block_timestamps(&[number])
            .await
            .ok()
            .and_then(|m| m.get(&number).copied());
        Ok(Some((hash, ts)))
    }

    /// Full block headers for the given blocks (RFC-0036 §4.2), for a nest with `[extract] blocks`.
    ///
    /// Default: none, which means a source that cannot answer produces **no block rows** rather than
    /// empty ones. That asymmetry with `block_timestamps` is deliberate: a missing timestamp leaves a
    /// column unset on a row that still exists, while a missing header would be a missing *row* - and
    /// a gap in a blocks table is indistinguishable from "the chain had no block there".
    async fn block_headers(&self, _blocks: &[u64]) -> Result<HashMap<u64, serde_json::Value>> {
        Ok(HashMap::new())
    }

    /// Full blocks **with transaction bodies**, for a nest with `[extract] top_level_calls`
    /// (RFC-0038 §5).
    ///
    /// Same default and same reasoning as `block_headers`: a source that cannot answer produces no
    /// call rows rather than empty ones, because an empty table is indistinguishable from "this
    /// contract was never called".
    async fn block_bodies(&self, _blocks: &[u64]) -> Result<HashMap<u64, serde_json::Value>> {
        Ok(HashMap::new())
    }

    /// Forget any cached per-block data above `block`, because the chain reorganised there.
    ///
    /// Default: nothing to forget. A source that caches anything keyed by **block number** must
    /// implement this, because a number is not an identity - after a reorg the block at that height is
    /// a different block with a different timestamp, and `block_timestamp` is a sealed column feeding
    /// the segment's content address. Serving a pre-reorg value for a re-indexed block would seal
    /// something a re-execution against the canonical chain could not reproduce.
    ///
    /// On the trait rather than on `RpcClient` so the reorg path, which holds a `dyn Source`, can call
    /// it without downcasting - and so any future caching source is *asked* the question rather than
    /// silently getting it wrong.
    fn forget_cached_above(&self, _block: u64) {}

    /// Logs matching `filter` over the inclusive range `[from, to]`.
    ///
    /// Takes a [`LogFilter`] rather than two slices so that "match everything" is not expressible
    /// here: a caller with nothing to ask for cannot construct one, and so cannot ask (#432).
    async fn logs(&self, filter: &LogFilter, from: u64, to: u64) -> Result<Vec<Log>>;
}

#[async_trait::async_trait]
impl Source for RpcClient {
    fn forget_cached_above(&self, block: u64) {
        self.forget_timestamps_above(block);
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

#[cfg(test)]
mod log_filter_tests {
    use super::LogFilter;

    /// The whole point of the type (#432): the one filter that means *every log on the chain* cannot
    /// be built, so no call site can issue it - present or future.
    ///
    /// The three cases below are not symmetric and the asymmetry is the design. One empty half is
    /// legitimate and load-bearing: a factory nest drops the address filter deliberately, because its
    /// children are not known in advance, and a nest can equally filter by address alone. Only both
    /// halves empty is the unbounded request, so only that case is refused.
    #[test]
    fn the_empty_filter_is_unrepresentable() {
        let addr = ["0xabc".to_string()];
        let topic = ["0xddf2".to_string()];

        assert!(
            LogFilter::new(&[], &[]).is_none(),
            "an empty address AND topic filter is every log on the chain, and must not be buildable"
        );
        assert!(
            LogFilter::new(&addr, &topic).is_some(),
            "an ordinary nest's filter is fine"
        );
        assert!(
            LogFilter::new(&[], &topic).is_some(),
            "topic0-only is a factory nest's filter, not the unbounded one"
        );
        assert!(
            LogFilter::new(&addr, &[]).is_some(),
            "address-only is bounded by the address list"
        );

        // What went in comes back out: the type narrows what is representable without editing the
        // filter, so a refusal is the only behaviour it adds.
        let f = LogFilter::new(&addr, &topic).unwrap();
        assert_eq!(f.addresses(), addr);
        assert_eq!(f.topic0s(), topic);
    }
}
