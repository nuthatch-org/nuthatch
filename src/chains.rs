//! Tiny chain registry. Ships sensible public-RPC defaults with round-robin failover - the
//!
//! ## These endpoint lists have an expiry date
//!
//! They are the **zero-setup path**: `init 0xAddr --chain mainnet && dev` uses them, and the founding
//! non-negotiable is a live indexed query in under two minutes with no configuration. That makes them
//! product surface, not constants - and they rot without anyone touching this file.
//!
//! On 2026-07-31, **two of the four mainnet defaults were dead**: `ethereum-rpc.publicnode.com` had
//! moved archive requests behind a token (so it could not serve a backfill *at all*, while being
//! listed first), and `eth.llamarpc.com` was returning HTTP 521. The comment beside them said
//! "Verified to serve keyless eth_getLogs (2026-07)" - within the same month.
//!
//! **Re-measure before trusting this list**, with the smallest request a real backfill makes - a
//! 10-block address-filtered `eth_getLogs` a few thousand blocks behind tip:
//!
//! ```sh
//! nuthatch doctor --rpc <url> --address <a-busy-contract>
//! ```
//!
//! A tip-only check passes on endpoints that cannot backfill, which is exactly how this went unnoticed.
//! first-run killer is RPC friction, so out of the box you should not need to bring a key.
//! (The "no third-party" upgrade is to colocate with a reth node; that path comes later.)
//!
//! The registry also carries each chain's finality policy and `eth_getLogs` window, so an L2 like
//! Arbitrum - different finality semantics, denser blocks - is a data entry here, not a fork of the
//! indexing loop.

use anyhow::Context;

/// How a chain decides a block is final enough to seal to the immutable cold layer. The sealing
/// invariant is unchanged either way: the columnar layer never sees a reorg, so this only sets *how
/// far behind the tip* we wait before a block is beyond reorg risk.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Finality {
    /// Seal blocks at least `n` behind the tip. A conservative proxy for finality (Ethereum L1).
    Depth(u64),
    /// Prefer the node's L1-aware `finalized` block tag (correct by construction on an L2 like
    /// Arbitrum); fall back to `Depth(fallback_depth)` when the endpoint doesn't serve the tag.
    FinalizedTag { fallback_depth: u64 },
}

pub struct Chain {
    pub name: &'static str,
    pub chain_id: u64,
    /// Tried in order, then round-robin, so a single flaky endpoint doesn't stall a run.
    pub rpc_urls: &'static [&'static str],
    /// When a block is safe to seal (see `Finality`).
    pub finality: Finality,
    /// Block span per `eth_getLogs` call. Small on dense L1 (dodge result-size caps); large on a
    /// sparse L2 like Arbitrum where events are few but block heights climb fast.
    pub log_window: u64,
    /// Whether a `getLogs` with an empty address list (topic0-only, the factory flip) is accepted.
    /// `false` means the shipped default returns an error such as "Please specify an address"; a
    /// factory nest on that chain keeps asking by address past the flip threshold, and is told so
    /// at load.
    pub topic0_only_getlogs: bool,
    /// **The longest block span a finalized range may be held unsealed** (#1199).
    ///
    /// Both seal paths cut a segment once `SEAL_DIRECT_BATCH` rows have finalised. That rule alone
    /// ratchets shut on a sparse nest: a range carrying *any* rows below the threshold is held, the
    /// range only grows, and the empty-range arm that would otherwise advance the watermark becomes
    /// unreachable. Measured live on the Graph allocations nest, which sees roughly 95
    /// event-carrying blocks a day against a 20,000-row threshold: `sealed_through` had not moved in
    /// 739,192 blocks (~51 h) while the cursor followed tip perfectly.
    ///
    /// So a held range is also cut at `from + seal_span - 1`, whichever comes first. The cut stays a
    /// function of the previous watermark and a constant rather than of the clock, so two operators
    /// still produce identical segments (RFC-0028 §4) - which is the whole reason this is a block
    /// span and not a timer.
    ///
    /// **Sized per chain because one number cannot serve both ends.** Every value below is ~6 hours
    /// of chain time, from block times measured against the shipped endpoints on 2026-09-07 over a
    /// 10,000-block sample. A single constant would be six hours on Arbitrum and eleven weeks on
    /// mainnet, or six hours on mainnet and ninety seconds on Robinhood Chain.
    ///
    /// Six hours is a judgement, not a measurement: long enough that a sparse nest is not cutting
    /// near-empty segments all day (the sub-20 KB segment problem `SEAL_DIRECT_BATCH` was added to
    /// fix), short enough that a tattler receipt pins a watermark within a working morning.
    pub seal_span: u64,
    /// Measured block time, the same 2026-09-07 sample `seal_span` is sized from. An undialled
    /// cursor polls this often, floored at two seconds (#1497).
    pub block_time_ms: u64,
}

impl Chain {
    pub fn block_time(&self) -> std::time::Duration {
        std::time::Duration::from_millis(self.block_time_ms)
    }
}

/// `seal_span` for a chain not in this registry.
///
/// 10,800 is six hours at a two-second block, the modal L2 cadence and what Base, Optimism and
/// Polygon all measure near. It is deliberately not sized for a fast chain: too small a default cuts
/// near-empty segments every few minutes on a 100 ms chain, whereas too large merely leaves an
/// unknown chain with a looser bound than a known one - and any bound at all is the fix. A custom
/// chain on a cadence far from two seconds should carry its own registry entry.
pub const DEFAULT_SEAL_SPAN: u64 = 10_800;

const MAINNET: Chain = Chain {
    name: "mainnet",
    chain_id: 1,
    rpc_urls: &[
        // **Ordered by measured backfill capability, best first** - see the module note above on why
        // this list has an expiry date. Re-measured 2026-10-01 with `nuthatch doctor --rpc … --address
        // <usdc>` across twenty keyless endpoints, then with getLogs at blocks 6.5M and 12M, which
        // doctor's archive probe (state 1M behind tip) does not reach:
        //   mainnet.gateway.tenderly.co   archive YES, getLogs 160 blocks, batch 20, topic0-only YES
        //   rpc.mevblocker.io             archive YES, getLogs 80 blocks, batch 200, topic0-only YES
        // USDC from deployment on this pair: 126,362 rows in 121 s, no retries. On the pair it
        // replaces: 0 rows in 122 s (#1607).
        "https://mainnet.gateway.tenderly.co",
        "https://rpc.mevblocker.io",
        // Removed 2026-10-01 (#1607): `eth.drpc.org` keeps no history (HTTP 400 "Unknown state" below
        // its pruning horizon, while doctor still reports it archive) and `eth-pokt.nodies.app` caps
        // getLogs at 50 blocks and throttles. Removed 2026-08-23 (#761): `eth.api.onfinality.io/public`.
        // Removed 2026-07-31: `ethereum-rpc.publicnode.com` (archive token), `eth.llamarpc.com` (521).
    ],
    // ~2 epochs; real finality signals arrive with the ExEx mode. The `finalized` tag exists
    // post-merge but Depth keeps a single conservative policy until ExEx lands.
    finality: Finality::Depth(64),
    log_window: 20,
    topic0_only_getlogs: true,
    // 12.05 s measured, so 1,800 blocks is 6.0 h (#1199).
    seal_span: 1_800,
    block_time_ms: 12_050,
};

const ARBITRUM_ONE: Chain = Chain {
    name: "arbitrum-one",
    chain_id: 42161,
    rpc_urls: &[
        // Keyless Arbitrum One endpoints. Both re-measured 2026-08-06 against the RFC-0030 §4 bar
        // with a 10-block address-filtered `eth_getLogs` 5,000 behind tip, five times each: archive
        // OK, getLogs 5/5, batch-of-5 OK, `finalized` OK. The official sequencer RPC first.
        "https://arb1.arbitrum.io/rpc",
        "https://arb-pokt.nodies.app",
        // Removed 2026-08-06 (issue #267): `arbitrum.drpc.org` failed two criteria - `getLogs`
        // **0/5** with `Request timeout on the free plan, please upgrade`, and a 5-request batch
        // rejected with `Batch of more than 3 requests are not allowed`. It could not serve a
        // backfill at all, and it was listed *second*, so round-robin handed it real traffic.
        // `arbitrum-one-rpc.publicnode.com` removed 2026-07-31 - same archive-token policy as its
        // mainnet sibling; it cannot serve a backfill.
    ],
    // True finality is L1 confirmation of the batch (~10-20 min). Prefer the node's `finalized`
    // tag; else ~7.5 min at 250 ms blocks. Horizon is sparse, so the extra hot window is cheap.
    finality: Finality::FinalizedTag {
        fallback_depth: 1800,
    },
    // Arbitrum blocks are frequent but Horizon events are rare, so a wide window keeps up cheaply.
    //
    // Measured 2026-08-07, address-filtered: `arb1.arbitrum.io` sustains ~163,840 blocks, but
    // `arb-pokt.nodies.app` only ~40 - and failover can route any request to the narrower one, so
    // `doctor` recommends 20 across the pair. 2000 is kept deliberately: it is right for the sparse-L2
    // case this window exists for, and RFC-0028's `fetch_logs_splitting` narrows a refused range
    // rather than failing it. The cost of that rescue is a burst of retries at the start of a
    // backfill, which reads as slowness - so a busy contract wants `--window` set from `doctor`.
    log_window: 2000,
    topic0_only_getlogs: true,
    // 0.2509 s measured, so 86,400 blocks is 6.0 h (#1199).
    seal_span: 86_400,
    block_time_ms: 251,
};

const BASE: Chain = Chain {
    name: "base",
    chain_id: 8453,
    rpc_urls: &[
        // Keyless Base mainnet endpoints. Both re-measured 2026-08-06 against the RFC-0030 §4 bar:
        // archive OK, getLogs 5/5, batch-of-5 OK, `finalized` OK. The official RPC first.
        "https://mainnet.base.org",
        "https://base-pokt.nodies.app",
        // Removed 2026-08-06 (issue #267): `base.drpc.org` rejects a 5-request batch with `Batch of
        // more than 3 requests are not allowed`, and `getLogs` was 4/5 with the same free-plan
        // timeout its sibling endpoints return. Flaky *and* over the bar's batch floor.
        // `base-rpc.publicnode.com` removed 2026-07-31 - same archive-token policy.
    ],
    // OP-stack L2: true finality is L1 confirmation. Base exposes the L1-aware `finalized` tag, so
    // prefer it (same policy as Arbitrum); the fallback (~30 min at 2 s blocks) only bites if an
    // endpoint doesn't serve the tag.
    finality: Finality::FinalizedTag {
        fallback_depth: 900,
    },
    // ~2 s blocks and busy - a moderate window that the adaptive chunker (RFC-0004 §2) tunes further.
    // Measured 2026-08-07, address-filtered: `mainnet.base.org` ~80, `base-pokt.nodies.app` ~40.
    // Same reasoning as Arbitrum: the optimistic default is recovered by adaptive splitting, at the
    // cost of early retries.
    log_window: 1000,
    topic0_only_getlogs: true,
    // 2.000 s measured, so 10,800 blocks is 6.0 h (#1199).
    seal_span: 10_800,
    block_time_ms: 2_000,
};

/// BNB Smart Chain. **Tip-following works out of the box; any history older than about 75 minutes
/// does not.**
///
/// Re-measured 2026-08-23 (#761), confirmed 2026-08-24. `bsc-rpc.publicnode.com` is still the only
/// keyless endpoint that answers address-filtered getLogs at all. `bsc-dataseed.binance.org` fails a
/// 10-block getLogs (`limit exceeded`) and has no trie state ~1M behind tip; `bsc.drpc.org` 429s the
/// public plan; `binance.llamarpc.com` does not connect.
///
/// Re-measured 2026-10-06 for #1941, and publicnode is narrower than it was:
///
/// - **History: about 10,000 blocks.** A getLogs 9,000 blocks behind tip is served; 10,500, 12,000
///   and 20,000 are HTTP 403 `-32602 Archive requests require a personal token`, which classifies
///   as history unavailable. It used to be recorded at block 1,000,000.
/// - **At most nine addresses per getLogs.** Ten or more is HTTP 403 `-32602 Request blocked`. The
///   client splits the address list into groups the endpoint takes and remembers the size.
/// - **No address-less getLogs** (`-32701 Please specify an address`). A factory nest asks by
///   `base ∪ children` until RFC-0009 §4's flip at 500 children, and on this endpoint stays there
///   past it. `topic0_only_getlogs` is false so `build_nest` says so at load.
/// - `1rpc.io/bnb` caps a range at 50 blocks (`-32602 eth_getLogs is limited to 0 - 50 blocks
///   range`, narrowed for) and rate-limits within minutes; over its quota again on the day.
///
/// So this is shipped honestly: tip-follow on the default; a backfill needs `--rpc <your archive
/// endpoint>`.
const BSC: Chain = Chain {
    name: "bsc",
    chain_id: 56,
    rpc_urls: &["https://bsc-rpc.publicnode.com"],
    // Fast finality is ~2 blocks, and the endpoint serves the tag; the fallback is deliberately far
    // more conservative than that because a single-endpoint chain has nothing to cross-check against.
    finality: Finality::FinalizedTag {
        fallback_depth: 1000,
    },
    // Measured max 320 address-filtered. Set at the measurement rather than optimistically above it:
    // with one endpoint there is no sibling to absorb a retry storm.
    log_window: 320,
    topic0_only_getlogs: false,
    // 0.4501 s measured - not the 3 s of the old cadence - so 48,000 blocks is 6.0 h (#1199).
    seal_span: 48_000,
    block_time_ms: 450,
};

/// Polygon PoS. Archive is available but narrow; the wide endpoint is not archive.
///
/// Measured 2026-08-20 (three runs each): `polygon.drpc.org` is archive, getLogs up to 80 blocks,
/// batch 10, doctor recommends 40. `polygon-bor-rpc.publicnode.com` gives a 5,120-block window and
/// batch 200+ but **archive depth no** - tip-following only, backfills fail. `polygon-rpc.com` was
/// unreachable on 2026-08-19.
///
/// Archive depth is the binding constraint for backfill; the archive endpoint goes first so a new
/// nest does not open against a non-archive URL. The wider endpoint is kept as a secondary for
/// tip-following fallover once an initial backfill completes.
///
/// Measured 2026-10-06 (#1941): `polygon-bor-rpc.publicnode.com` now refuses an address-less
/// getLogs over a range (`-32701 Please specify an address`) and ten or more addresses (HTTP 403
/// `Request blocked`; nine served), as its BSC sibling does. `polygon.drpc.org` takes both, so
/// `topic0_only_getlogs` stays true and a refusal fails over to it.
///
/// Measured 2026-10-13 (#2040): publicnode now fails even a 10-block getLogs a few thousand blocks
/// behind tip, so it is gone. `polygon.gateway.tenderly.co` replaces it: archive state, a
/// 163,840-block window, batch 10, topic0-only getLogs served. Twelve other keyless endpoints refused.
const POLYGON: Chain = Chain {
    name: "polygon",
    chain_id: 137,
    rpc_urls: &[
        "https://polygon.drpc.org",
        "https://polygon.gateway.tenderly.co",
    ],
    // Heimdall checkpoints are the real finality signal and the tag reflects them; the fallback is
    // sized for a checkpoint interval rather than a block time.
    finality: Finality::FinalizedTag {
        fallback_depth: 1000,
    },
    // polygon.drpc.org (archive, first endpoint) caps getLogs at 80 blocks; doctor recommends 40.
    // Measured 2026-08-20.
    log_window: 40,
    topic0_only_getlogs: true,
    // 1.500 s measured, so 14,400 blocks is 6.0 h (#1199).
    seal_span: 14_400,
    block_time_ms: 1_500,
};

/// Gnosis. The best-served of the four chains added here: two keyless **archive** endpoints, both
/// with a 163,840-block getLogs ceiling and batch 200+.
///
/// Measured 2026-08-19. `gnosis-rpc.publicnode.com` also passes the bar but reports archive depth no,
/// so it is deliberately not listed - a third endpoint that cannot serve history is a trap during a
/// backfill, not redundancy.
const GNOSIS: Chain = Chain {
    name: "gnosis",
    chain_id: 100,
    rpc_urls: &[
        "https://rpc.gnosischain.com",
        "https://rpc.gnosis.gateway.fm",
    ],
    finality: Finality::FinalizedTag {
        fallback_depth: 200,
    },
    // Both endpoints measured at 163,840; 20,000 leaves headroom for the adaptive chunker to climb
    // without opening on a window neither can serve.
    log_window: 20_000,
    topic0_only_getlogs: true,
    // 5.088 s measured, so 4,250 blocks is 6.0 h (#1199).
    seal_span: 4_250,
    block_time_ms: 5_088,
};

/// Optimism. OP-stack L2, so the same finality reasoning as Base: the `finalized` tag is L1-aware.
///
/// Measured 2026-08-19: `mainnet.optimism.io` (window 640) and `optimism-rpc.publicnode.com`
/// (window 1,280), both **archive**, both batch-of-5 OK. `optimism.drpc.org` is excluded for failing
/// batch-of-5 - the identical failure that removed `arbitrum.drpc.org` and `base.drpc.org` under
/// issue #267, which is now three chains in a row.
///
/// Re-measured 2026-09-07 (#1196), and `optimism-rpc.publicnode.com` is **gone**: it now answers an
/// address-filtered `eth_getLogs` only within ~20 blocks of tip and refuses anything ~200 back with
/// `-32602 Archive requests require a personal token`. Measured at depths 20 / 200 / 1,000 / 5,000 /
/// 20,000 behind tip: served, refused, refused, refused, refused. That is the same allnodes archive
/// policy that removed `ethereum-rpc.publicnode.com` and `arbitrum-one-rpc.publicnode.com` on
/// 2026-07-31, and it was listed *second*, so round-robin handed it real backfill traffic. Its BSC
/// sibling is unaffected and stays.
///
/// `op-pokt.nodies.app` replaces it, measured the same day against the RFC-0030 §4 bar: archive
/// state 1M blocks back OK, `eth_getLogs` 5/5 at 5,000 behind tip, logs at 1M behind tip, batch-of-5
/// OK, `finalized` OK. Same family as the pokt endpoints already shipped for mainnet, Arbitrum and
/// Base. It goes second; the official sequencer RPC stays first.
///
/// This one was invisible for nineteen days because the live-endpoints probe never covered Optimism.
/// It covers every shipped chain now, and reads the list from this file rather than a copy.
const OPTIMISM: Chain = Chain {
    name: "optimism",
    chain_id: 10,
    rpc_urls: &[
        "https://mainnet.optimism.io",
        "https://op-pokt.nodies.app",
        // Removed 2026-09-07 (#1196): `optimism-rpc.publicnode.com` - archive token, tip-only.
    ],
    finality: Finality::FinalizedTag {
        fallback_depth: 900,
    },
    log_window: 600,
    topic0_only_getlogs: true,
    // 2.000 s measured, so 10,800 blocks is 6.0 h (#1199).
    seal_span: 10_800,
    block_time_ms: 2_000,
};

/// Monad. A full-EVM-bytecode L1 with MonadBFT single-slot finality, and the first chain here whose
/// `finalized` tag runs *one block* behind tip because finality genuinely is that fast, not because
/// an endpoint aliases it to `latest` (RFC-0051). Blocks every 300 ms - measured 2026-09-03 at 302 ms
/// over 100 blocks - and dense: the busiest contract that day carried 7,831 logs across 101 blocks.
///
/// Measured 2026-09-03 with `nuthatch doctor --address <that contract>` against the RFC-0030 §4 bar:
///
/// | endpoint                    | getLogs (addr) | batch | `finalized` | history              |
/// |-----------------------------|----------------|-------|-------------|----------------------|
/// | `rpc1.monad.xyz` (Alchemy)  | 640            | 200+  | yes         | logs + blocks from 1 |
/// | `rpc.monad.xyz` (QuickNode) | 80 (cap 100)   | 100+  | yes         | logs + blocks from 1 |
/// | `rpc3.monad.xyz` (Ankr)     | 320            | 10    | yes         | logs + blocks from 1 |
///
/// **`doctor` reports archive depth "no" on all three, and that verdict is right about what it
/// measures and wrong about what a backfill needs.** Its probe is a pinned `eth_getBalance` a million
/// blocks behind tip, and none of the three keeps historic *state* - Monad full nodes do not. All
/// three serve historic *logs and blocks* from block 1 (probed at 1, 1,000,000, 40,000,000 and
/// 50,000,000), which is what a from-genesis backfill reads. So backfills work on the shipped
/// defaults; a pinned `[[calls]]` at an old block (RFC-0023) does not, and wants `--rpc` at an archive
/// endpoint. `rpc-mainnet.monadinfra.com` is the one keyless endpoint that *does* keep state, and it
/// is not listed because it refuses JSON-RPC batches outright (HTTP 403 `Restricted JSON RPC method`),
/// which fails the bar's batch floor and would make every timestamp fetch a single-block request.
///
/// The three refuse an over-wide range in three different shapes, none of them the `-32602` the RFC
/// draft expected: QuickNode HTTP 413 `-32614 "eth_getLogs is limited to a 100 range"`, Alchemy the
/// RFC-0029 HTTP 400 `-32602 "Log response size exceeded"`, and Ankr HTTP 200 `-32603 "response
/// exceeds size limit"`. The first and third are in `classify_rpc_error` because of this chain; the
/// third matched nothing before and would have been retried at the same width.
///
/// **No execution-lag guard, and a depth rather than the tag, for now.** RFC-0051 proposed a guard on
/// the grounds that Monad finalises a block before executing it (execution is deferred by `k = 3`
/// blocks). Measured eight times at `latest` on 2026-09-03: every block's receipts and logs were
/// complete on the first read (receipt count equal to transaction count), identical two seconds
/// later, same hash - the RPC layer served only executed blocks. Eight samples are not an invariant
/// under provider load or a pipeline stall, and the guard as drafted cannot tell an unexecuted block
/// from an executed empty one, since both answer an empty list. So the seal boundary is a **depth of
/// 8**: 2.4 s at 300 ms, more than twice the protocol's deferral, and every sealed block is at least
/// six past the `finalized` tag, which itself is irreversible without a hard fork. That costs two
/// seconds of seal latency against the tag and buys a margin no probe has to keep proving. Moving
/// to `FinalizedTag` wants a soak that reads receipts at `finalized` continuously, not a sample.
///
/// What makes the depth safe rather than merely wide, measured 2026-09-03 on all three endpoints:
/// a height a node has **not** executed is answered with an **error**, never an empty list -
/// Alchemy `-32602 "block range extends beyond current head block"`, QuickNode and Ankr `-32602
/// "Block requested not found"` - and both classify `Transient` and are retried, so a short answer
/// cannot be mistaken for an empty block. Monad's own docs say `latest` is "backed by speculative
/// execution", i.e. a node serves a height only once it has executed it. And the header's
/// `logsBloom` is the block's **own** (52 of 52 log-emitting addresses present in header N's bloom,
/// 32 in N+3's), so the RFC-0049 §1 empty-case oracle is available from headers already fetched
/// when timestamps are on, should anyone want to wire it.
const MONAD: Chain = Chain {
    name: "monad",
    chain_id: 143,
    rpc_urls: &[
        // Widest window and widest batch first; the canonical endpoint second, because its 100-block
        // cap and 50 req/s limit make it the narrow one; Ankr third with its batch of 10.
        "https://rpc1.monad.xyz",
        "https://rpc.monad.xyz",
        "https://rpc3.monad.xyz",
    ],
    // MonadBFT: a block is final at the proposal of N+2, about 600 ms, and every shipped endpoint
    // serves the `finalized` tag one block behind tip. Eight blocks is not a reorg margin - nothing
    // past `finalized` reorgs on Monad - it is an *execution* margin over the deferred pipeline, see
    // the doc comment above. RFC-0051's body argues for the tag; its addendum item 14 says why not yet.
    finality: Finality::Depth(8),
    // At the narrowest endpoint's documented range cap (QuickNode, 100), not above it: Monad blocks
    // are dense enough that the *result* cap is what bites on a busy contract, and RFC-0028's chunker
    // narrows from here on a cap it can see. `doctor` recommends 40 across the pool.
    log_window: 100,
    topic0_only_getlogs: true,
    // 0.3020 s measured, so 72,000 blocks is 6.0 h (#1199).
    seal_span: 72_000,
    block_time_ms: 302,
};

/// Robinhood Chain. Robinhood's Arbitrum Orbit L2 on the Nitro stack - the same execution stack and
/// the same settlement shape as Arbitrum One, so it rides the generic EVM path with no special-casing
/// (RFC-0050; the addendum confirms every `42161` outside this file is a fixture). Chain id 4663,
/// mainnet since 2026-07-01, 100 ms blocks (measured 9.9 blocks/s on 2026-09-04), and busy: 148 logs
/// per block over 1,000 sampled blocks from 3,059 emitters, the top of the list being USDG, WETH and
/// the Stock Tokens (MSTR, AMC, SPY, ...) - ERC-20s, which is exactly the shape `init` was built for.
///
/// Measured 2026-09-04 with `nuthatch doctor --address <USDG>` against the RFC-0030 §4 bar. Three
/// keyless hosts answer `eth_chainId` with 4663; one of them can back a nest:
///
/// | endpoint                          | getLogs (addr) | batch | archive        | verdict |
/// |-----------------------------------|----------------|-------|----------------|---------|
/// | `rpc.mainnet.chain.robinhood.com` | 640            | 5     | refused (429)  | shipped |
/// | `robinhood.drpc.org`              | -              | -     | -              | no `eth_blockNumber` (`-32601`) |
/// | `robinhood-rpc.publicnode.com`    | 0 - 403        | 200+  | token required | the archive-token policy its siblings have |
///
/// The shipped endpoint is rate-limited, and the limit is per second rather than per day: the same
/// `doctor` run that read 640 above read **40** and `batching unusable at 2` when it ran straight
/// after a thousand-block sampling pass, and every failure was HTTP 429. A backfill on it works at
/// the shipped window; anything in anger wants `--rpc` at a keyed provider (Alchemy, QuickNode and
/// Chainstack all list the chain). The over-wide refusal is HTTP 200 `-32000 "logs matched by query
/// exceeds limit of 10000"`, which `classify_rpc_error` already narrows on.
///
/// **`FinalizedTag`, not the `safe` tag the RFC's body recommends.** Both tags are served: at
/// 15:08 UTC `safe` was 7,765 blocks (784 s) behind `latest` and `finalized` 11,921 blocks (1,207 s,
/// about twenty minutes). `Finality` has no `safe` arm, and adding one is a seal-loop change rather
/// than a registry entry, so the carve-out takes the tag the code has and pays about seven minutes
/// of seal latency for it - the same policy Arbitrum One runs. The fallback depth is sized for the
/// cadence, not copied from Arbitrum One's 1,800: 15,000 blocks is twenty-five minutes at 10 blocks/s,
/// a margin over the measured `finalized` lag, and it only runs on an endpoint that stops serving
/// the tag.
const ROBINHOOD: Chain = Chain {
    name: "robinhood",
    chain_id: 4663,
    rpc_urls: &[
        // The only keyless endpoint that passed the bar on 2026-09-04; see the table above for the
        // two that did not.
        "https://rpc.mainnet.chain.robinhood.com",
    ],
    finality: Finality::FinalizedTag {
        fallback_depth: 15_000,
    },
    // Half of what the public endpoint sustained address-filtered on the busiest token, which is
    // what `doctor` recommends. The result cap is 10,000 logs, and a busy token here carries about
    // 20 a block, so 320 sits under it with room; RFC-0028's chunker narrows from here on the cap
    // it can see.
    log_window: 320,
    // Address-less windows are served: the 2026-09-04 emitter sample was 20 unfiltered 50-block
    // windows, every one answered.
    topic0_only_getlogs: true,
    // 0.1008 s measured, so 216,000 blocks is 6.0 h (#1199).
    seal_span: 216_000,
    block_time_ms: 101,
};

/// The registry entry for a chain id, if we ship one.
///
/// A by-id lookup exists for callers that have the id but not the name - `bench`, which threads
/// `chain_id` through its harness. It matters because a benchmark that seals on a different rule
/// from production measures a cadence nobody runs (#1199).
pub fn lookup_by_id(chain_id: u64) -> Option<&'static Chain> {
    all().iter().copied().find(|c| c.chain_id == chain_id)
}

pub fn lookup(name: &str) -> Option<&'static Chain> {
    match name {
        "mainnet" | "ethereum" | "eth" => Some(&MAINNET),
        "arbitrum-one" | "arbitrum" | "arb" | "arb1" => Some(&ARBITRUM_ONE),
        "base" | "base-mainnet" | "base-one" => Some(&BASE),
        "bsc" | "bnb" | "binance" | "bnb-smart-chain" | "bsc-mainnet" => Some(&BSC),
        "polygon" | "matic" | "polygon-pos" | "polygon-mainnet" => Some(&POLYGON),
        "gnosis" | "xdai" | "gnosis-chain" => Some(&GNOSIS),
        "optimism" | "op" | "op-mainnet" | "optimism-mainnet" => Some(&OPTIMISM),
        "monad" | "monad-mainnet" | "mon" => Some(&MONAD),
        "robinhood" | "robinhood-chain" | "robinhood-mainnet" | "rh" => Some(&ROBINHOOD),
        _ => None,
    }
}

/// Registered chains whose block headers carry `l1BlockNumber`: the Arbitrum Nitro family.
pub fn reports_l1_block_number(chain_id: u64) -> bool {
    matches!(chain_id, 42161 | 4663)
}

/// What an operator should know before indexing `chain` through the endpoints actually in use, when
/// it matters (#1570): only when timestamps are being fetched, and only for `arb1.arbitrum.io` itself.
///
/// `arb1` sends `blockTimestamp: 0x0` on every log, so every block's timestamp costs a header, and it
/// takes header batches of about ten at most. The header batch width is the narrowest any endpoint in
/// the pool takes, so while `arb1` is in the pool it bounds the rest: the note says so, and says to
/// replace it rather than add to it.
pub fn keyless_caveat(chain: &str, rpc_urls: &[String], timestamps: bool) -> Option<&'static str> {
    let arbitrum = lookup(chain).is_some_and(|c| c.chain_id == 42161);
    let arb1 = rpc_urls.iter().any(|u| host_is(u, ARB1));
    (timestamps && arbitrum && arb1).then_some(
        "arb1.arbitrum.io sends no timestamp on its logs, so each block's comes from a header, and it \
         takes header batches of about ten. While it is anywhere in the pool, primary or fallback, \
         every header batch goes out at ten, keyed endpoints included: a backfill here runs at a few \
         times chain speed, and a long one may be refused. For history, use a keyed Arbitrum RPC in \
         place of arb1 (in rpc_urls, or --rpc <url>).",
    )
}

/// Arbitrum's own keyless RPC, which the registry ships first for `arbitrum-one`.
const ARB1: &str = "arb1.arbitrum.io";

fn host_is(url: &str, host: &str) -> bool {
    reqwest::Url::parse(url).is_ok_and(|u| u.host_str() == Some(host))
}

/// The most block headers an endpoint in `rpc_urls` answers in one batch, where a known keyless one
/// answers fewer than nuthatch would send (#1570).
///
/// `arb1.arbitrum.io` answers ten and refuses fifty, and refuses them with a 429 that no response can
/// tell from a throttle. Inferring the limit from refusals (or probing for it) therefore mistakes a
/// throttle for a size limit and spends quota doing it, so the fact is recorded here, beside the
/// endpoint itself, as the registry records the rest of what it knows about the endpoints it ships.
pub fn header_batch_cap(rpc_urls: &[String]) -> Option<usize> {
    rpc_urls.iter().any(|u| host_is(u, ARB1)).then_some(10)
}

/// Policy for a chain with no registry entry: the same "assume L1, wait for real depth, and a
/// narrow `eth_getLogs` window" default the indexer already falls back to for a nest whose
/// `chain` field names nothing in this file. `init`/`add` hand a custom chain the identical
/// policy, so a nest built here behaves exactly as `dev` already promised for one.
pub const UNREGISTERED_FINALITY: Finality = Finality::Depth(64);
pub const UNREGISTERED_WINDOW: u64 = 20;

/// A resolved chain, ready for `init`/`add` to act on. `Chain` stays a purely `&'static` built-in
/// registry entry; this is the owned shape a custom (non-built-in) chain needs, since its name and
/// endpoints come from the command line rather than a `const`.
///
/// `Debug` is load-bearing rather than decorative: a test under the `scaled` feature unwraps a
/// `Result` carrying this, which needs it, and the default build never compiles that test - so the
/// omission was invisible until the scaled-mode CI job ran.
#[derive(Debug)]
pub struct ResolvedChain {
    pub name: String,
    pub chain_id: u64,
    pub rpc_urls: Vec<String>,
    pub finality: Finality,
    pub log_window: u64,
}

impl From<&'static Chain> for ResolvedChain {
    fn from(c: &'static Chain) -> Self {
        ResolvedChain {
            name: c.name.to_string(),
            chain_id: c.chain_id,
            rpc_urls: c.rpc_urls.iter().map(|s| s.to_string()).collect(),
            finality: c.finality,
            log_window: c.log_window,
        }
    }
}

/// Resolve `--chain` for `init`: a name in the built-in registry always wins, on its own vetted
/// endpoints and policy. A name outside it is accepted too, but only when paired with `--rpc` -
/// nothing else could tell nuthatch where to send traffic for a chain it doesn't ship. The custom
/// chain's `chain_id` comes from the RPC itself (`eth_chainId`, one call) rather than a flag, since
/// the endpoint already knows and a wrong hand-typed id would silently corrupt the nest.
///
/// This is the fix for issue #535: the nest format (`chain` + `chain_id` + `rpc_urls` in
/// `nuthatch.toml`) and the runtime (`indexer::index` falls back to [`UNREGISTERED_FINALITY`] /
/// [`UNREGISTERED_WINDOW`] for exactly this case) already support any chain - `init`'s allow-list
/// was the only thing narrower than what it scaffolds.
pub async fn resolve(name: &str, rpc_urls: &[String]) -> anyhow::Result<ResolvedChain> {
    if let Some(c) = lookup(name) {
        return Ok(c.into());
    }
    if rpc_urls.is_empty() {
        let known = all().iter().map(|c| c.name).collect::<Vec<_>>().join(", ");
        anyhow::bail!(
            "unknown chain '{name}' (try: {known}) - or pass --rpc <url> to point nuthatch at a \
             chain it doesn't ship built-in"
        );
    }
    let chain_id = crate::rpc::RpcClient::new(rpc_urls.to_vec())?
        .chain_id()
        .await
        .with_context(|| format!("could not read the chain id for '{name}' from --rpc"))?;
    Ok(ResolvedChain {
        name: name.to_string(),
        chain_id,
        rpc_urls: rpc_urls.to_vec(),
        finality: UNREGISTERED_FINALITY,
        log_window: UNREGISTERED_WINDOW,
    })
}

/// The chain of a nest already on disk: a known chain uses its own registry policy, and a nest
/// scaffolded against a custom chain (see [`resolve`]) has its `chain_id` saved in `nuthatch.toml`
/// already, so there is nothing left to ask an RPC for. Used by `add`, which grows an existing nest
/// and must never re-detect or re-resolve the chain it already committed to at `init`.
pub fn from_config(name: &str, chain_id: u64) -> ResolvedChain {
    match lookup(name) {
        Some(c) => c.into(),
        None => ResolvedChain {
            name: name.to_string(),
            chain_id,
            rpc_urls: Vec::new(),
            finality: UNREGISTERED_FINALITY,
            log_window: UNREGISTERED_WINDOW,
        },
    }
}

/// Every registered chain, in auto-detect probe order (L1 first, then the busiest L2s). `init`
/// walks this when `--chain` is omitted: the chain a contract lives on is discoverable, not a
/// thing the user should have to know and type.
///
/// **This must list every chain [`lookup`] knows** (#696). It returned three while `lookup` knew
/// seven, so a contract on Optimism, Polygon, BSC or Gnosis could be indexed by name and not found
/// by probe - and the README stated in bold that omitting `--chain` probes all seven. Two lists of
/// the same set, one of them silent when it falls behind. `every_registered_chain_is_probed` below
/// derives the expectation from `lookup` rather than restating the literal, so the next chain added
/// fails the test until it is added here too.
pub fn all() -> &'static [&'static Chain] {
    &[
        &MAINNET,
        &ARBITRUM_ONE,
        &BASE,
        &OPTIMISM,
        &POLYGON,
        &BSC,
        &GNOSIS,
        &MONAD,
        &ROBINHOOD,
    ]
}

#[cfg(test)]
mod tests {
    use super::*;

    /// #1570: the caveat is for Arbitrum through `arb1`, under any name the chain goes by, and not
    /// for a nest that brought its own endpoint or another chain.
    #[test]
    fn the_keyless_arbitrum_caveat_names_only_the_case_it_is_about() {
        let arb1 = vec!["https://arb1.arbitrum.io/rpc".to_string()];
        let keyed = vec!["https://arb-mainnet.g.alchemy.com/v2/k".to_string()];
        assert!(keyless_caveat("arbitrum-one", &arb1, true).is_some());
        assert!(keyless_caveat("arbitrum", &arb1, true).is_some());
        assert!(keyless_caveat("arbitrum-one", &keyed, true).is_none());
        assert!(keyless_caveat("base", &arb1, true).is_none());
        assert_eq!(header_batch_cap(&arb1), Some(10));
        assert_eq!(header_batch_cap(&keyed), None);
        assert!(
            keyless_caveat("arbitrum-one", &arb1, false).is_none(),
            "no timestamps, no headers"
        );
        let lookalike = vec!["https://proxy.example/arb1.arbitrum.io/rpc".to_string()];
        assert!(
            keyless_caveat("arbitrum-one", &lookalike, true).is_none(),
            "the host, not a substring"
        );
    }

    #[test]
    fn arbitrum_is_registered_with_l2_finality() {
        let c = lookup("arbitrum-one").expect("arbitrum-one in registry");
        assert_eq!(c.chain_id, 42161);
        assert_eq!(
            c.finality,
            Finality::FinalizedTag {
                fallback_depth: 1800
            }
        );
        assert!(
            c.log_window >= 1000,
            "sparse L2 wants a wide getLogs window"
        );
        assert!(!c.rpc_urls.is_empty());
        // Aliases resolve to the same chain.
        assert_eq!(lookup("arb").unwrap().chain_id, 42161);
        assert_eq!(lookup("arbitrum").unwrap().chain_id, 42161);
    }

    #[test]
    fn mainnet_uses_depth_finality() {
        let c = lookup("mainnet").unwrap();
        assert_eq!(c.finality, Finality::Depth(64));
        assert_eq!(c.log_window, 20);
    }

    /// #761: onfinality's public URL was a third mainnet default that could not serve a backfill
    /// (no archive, and on 2026-08-23 the doctor probe did not complete). Failover across a corpse
    /// is not failover.
    #[test]
    fn mainnet_does_not_ship_onfinality_or_a_single_live_host() {
        let urls = lookup("mainnet").unwrap().rpc_urls;
        assert!(
            !urls.iter().any(|u| u.contains("onfinality")),
            "onfinality public is not archive and does not complete a doctor probe: {urls:?}"
        );
        assert!(
            urls.len() >= 2,
            "mainnet must keep a spare after pruning: {urls:?}"
        );
        assert!(
            lookup("mainnet").unwrap().topic0_only_getlogs,
            "mainnet factory flip is the ordinary case"
        );
    }

    /// #761: BSC's only keyless default refuses address-less getLogs, which is the factory flip.
    #[test]
    fn bsc_shipped_default_does_not_claim_topic0_only_getlogs() {
        let c = lookup("bsc").unwrap();
        assert!(
            !c.topic0_only_getlogs,
            "bsc-rpc.publicnode.com refuses an empty address list"
        );
        assert_eq!(c.rpc_urls, &["https://bsc-rpc.publicnode.com"]);
    }

    #[test]
    fn base_is_registered_as_op_stack_l2() {
        let c = lookup("base").expect("base in registry");
        assert_eq!(c.chain_id, 8453);
        // OP-stack L2 → same finalized-tag policy as Arbitrum.
        assert!(matches!(c.finality, Finality::FinalizedTag { .. }));
        assert!(!c.rpc_urls.is_empty());
        assert_eq!(lookup("base-mainnet").unwrap().chain_id, 8453);
    }

    /// RFC-0051: the first BFT single-slot-final chain in the registry. The seal boundary is a depth
    /// of exactly eight blocks - 2.4 s, more than twice Monad's three-block execution deferral - not
    /// the `finalized` tag, until a soak shows finalized and executed are coupled under load (RFC-0051
    /// addendum, item 14). The window sits at the narrowest shipped endpoint's documented cap rather
    /// than above it, because Monad's blocks are dense enough that the result cap, not the range cap,
    /// is what a busy contract hits.
    #[test]
    fn monad_is_registered_with_an_execution_margin_at_the_narrowest_cap() {
        let c = lookup("monad").expect("monad in registry");
        assert_eq!(c.chain_id, 143);
        assert_eq!(
            c.finality,
            Finality::Depth(8),
            "an execution margin over k=3 deferred execution; the tag is a soak away, not a sample"
        );
        assert_eq!(
            c.log_window, 100,
            "QuickNode's documented cap - see the entry's measurements before raising it"
        );
        assert!(
            c.topic0_only_getlogs,
            "measured 2026-09-03: an address-less topic0 getLogs is served by all three"
        );
        assert_eq!(lookup("monad-mainnet").unwrap().chain_id, 143);
    }

    /// RFC-0050: an Arbitrum Nitro L2, so it takes Arbitrum One's tag policy, with the fallback
    /// depth sized for 100 ms blocks rather than copied. The RFC's body asks for the `safe` tag;
    /// `Finality` has no such arm, and the carve-out took the tag the code has rather than a
    /// seal-loop change. The window is what `doctor` recommended on the busiest token, under a
    /// 10,000-log result cap the endpoint names in its refusal.
    #[test]
    fn robinhood_is_registered_on_the_nitro_tag_policy_with_a_cadence_sized_fallback() {
        let c = lookup("robinhood").expect("robinhood in registry");
        assert_eq!(c.chain_id, 4663);
        assert_eq!(
            c.finality,
            Finality::FinalizedTag {
                fallback_depth: 15_000
            },
            "finalized runs about 12,000 blocks behind tip at 10 blocks/s; 1,800 is Arbitrum One's \
             number and would fall short of the tag by ten minutes"
        );
        assert_eq!(
            c.log_window, 320,
            "doctor's recommendation on 2026-09-04 against USDG; the result cap is 10,000 logs"
        );
        assert!(
            c.topic0_only_getlogs,
            "measured 2026-09-04: address-less getLogs windows are served"
        );
        assert_eq!(
            c.rpc_urls.len(),
            1,
            "one keyless endpoint passed the RFC-0030 §4 bar; the two others are recorded in the \
             entry's doc comment with why not"
        );
        assert_eq!(lookup("robinhood-chain").unwrap().chain_id, 4663);
    }

    #[test]
    fn unknown_chain_is_none() {
        assert!(lookup("dogechain").is_none());
    }

    #[test]
    fn all_chains_are_probeable_and_lead_with_l1() {
        let all = all();
        // Every registered chain is reachable via `lookup` and carries endpoints to probe.
        assert!(!all.is_empty());
        for c in all {
            assert!(
                lookup(c.name).is_some(),
                "{} must resolve via lookup",
                c.name
            );
            assert!(
                !c.rpc_urls.is_empty(),
                "{} needs endpoints to probe",
                c.name
            );
        }
        // L1 first: mainnet is the most likely home and the least surprising default hit.
        assert_eq!(all[0].name, "mainnet");
    }

    /// RFC-0030 §4: a chain may ship only with **at least two** endpoints that independently clear
    /// the bar, because the round-robin failover in `rpc_urls` is the mitigation for a flaky host -
    /// and a list of one has nothing to fail over to.
    ///
    /// Enforced here rather than remembered, because the failure is silent: pruning a bad endpoint
    /// is exactly when this drops to one, and the person doing it is looking at what they removed
    /// rather than at what is left. Issue #267 pruned three lists in one pass.
    #[test]
    fn every_chain_ships_at_least_two_endpoints() {
        // BSC is the one exception, and it is recorded rather than waved through. No keyless public
        // archive endpoint for it has been found: probed 2026-08-21 with `nuthatch doctor`,
        // `bsc.drpc.org` and `binance.llamarpc.com` both refused the getLogs probe outright and could
        // not be asked about archive depth, and `bsc-dataseed.bnbchain.org` answered but is
        // tip-following only, so a from-deployment backfill cannot use it.
        //
        // This surfaced only when #696 put BSC into `all()`: the rule had been passing because the
        // chain that breaks it was not in the list the rule iterates. A short list hid a real RFC-0030
        // §4 violation.
        //
        // Robinhood Chain is the second, recorded the same way (RFC-0050 addendum, item 8). Seven
        // keyless hosts were tried on 2026-09-04 and one cleared the bar: `robinhood.drpc.org`
        // answers the chain id but has no `eth_blockNumber` (`-32601`), `robinhood-rpc.publicnode.com`
        // wants an archive token even for a 10-block getLogs, `rpc.ankr.com/robinhood` is 403,
        // `1rpc.io/robinhood` is `unknown network`, thirdweb's two hosts say `Invalid chain`,
        // Tenderly's gateway is 404, and the explorer's `eth-rpc` sits behind a Cloudflare challenge.
        // The chain is two months old; re-probe before the entry's next measurement.
        //
        // Each exception announces its own obsolescence. It asserts the chain has **exactly one**
        // endpoint, so the day somebody adds a second this test fails and whoever does it deletes
        // that name rather than leaving a stale carve-out behind.
        const SINGLE_ENDPOINT_EXCEPTIONS: &[&str] = &["bsc", "robinhood"];
        for c in all() {
            if SINGLE_ENDPOINT_EXCEPTIONS.contains(&c.name) {
                assert_eq!(
                    c.rpc_urls.len(),
                    1,
                    "{} now ships {} endpoints - delete the exception in this test, it has served \
                     its purpose",
                    c.name,
                    c.rpc_urls.len()
                );
                continue;
            }
            assert!(
                c.rpc_urls.len() >= 2,
                "{} ships {} endpoint(s); RFC-0030 §4 requires at least two so round-robin has \
                 somewhere to fail over. Measure a replacement before removing the last spare.",
                c.name,
                c.rpc_urls.len()
            );
        }
    }

    /// No endpoint appears twice in one chain's list - a duplicate looks like failover and is not.
    #[test]
    fn a_chains_endpoints_are_distinct() {
        for c in all() {
            let mut seen = std::collections::BTreeSet::new();
            for u in c.rpc_urls {
                assert!(seen.insert(*u), "{} lists {u} twice", c.name);
            }
        }
    }

    // ---- #535: `resolve`/`from_config` - a chain outside the built-in three ----------------------

    /// A one-endpoint fake JSON-RPC server answering only `eth_chainId`. Mirrors `rpc::tests::fake_rpc`
    /// (private to that module) - real HTTP, so `resolve`'s actual RPC round trip is exercised.
    async fn fake_chain_id_rpc(chain_id: u64) -> (String, tokio::task::JoinHandle<()>) {
        use axum::{extract::State, routing::post, Json, Router};
        use serde_json::{json, Value};

        async fn handler(State(chain_id): State<u64>, Json(_req): Json<Value>) -> Json<Value> {
            Json(json!({"jsonrpc": "2.0", "id": 1, "result": format!("0x{chain_id:x}")}))
        }

        let app = Router::new().route("/", post(handler)).with_state(chain_id);
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let handle = tokio::spawn(async move {
            let _ = axum::serve(listener, app).await;
        });
        (format!("http://{addr}"), handle)
    }

    #[tokio::test]
    async fn resolve_of_a_known_name_ignores_rpc_and_never_dials_it() {
        // A known chain never needs the network: `resolve` must not even try to dial `--rpc` for it -
        // `127.0.0.1:1` is a port nothing listens on, so a dial here would hang or error.
        let resolved = resolve("mainnet", &["http://127.0.0.1:1".into()])
            .await
            .unwrap();
        assert_eq!(resolved.chain_id, 1);
        assert_eq!(resolved.finality, Finality::Depth(64));
        assert_eq!(resolved.log_window, 20);
    }

    /// Every shipped chain resolves under each of its aliases, with the right id.
    ///
    /// Added with the four chains in the top-25 list (BNB, Polygon, Gnosis, Optimism). A typo in an
    /// alias arm is invisible until somebody types that alias, and then it reads as "nuthatch does
    /// not support this chain" rather than "we spelled it wrong".
    #[test]
    fn every_shipped_chain_resolves_under_all_of_its_aliases() {
        // Also the source of truth for `every_registered_chain_is_probed` below, so a new chain is
        // added here once and both properties are checked from it.
        for (aliases, id) in [
            (&["mainnet", "ethereum", "eth"][..], 1u64),
            (&["arbitrum-one", "arbitrum", "arb", "arb1"][..], 42161),
            (&["base", "base-mainnet", "base-one"][..], 8453),
            (
                &["bsc", "bnb", "binance", "bnb-smart-chain", "bsc-mainnet"][..],
                56,
            ),
            (
                &["polygon", "matic", "polygon-pos", "polygon-mainnet"][..],
                137,
            ),
            (&["gnosis", "xdai", "gnosis-chain"][..], 100),
            (
                &["optimism", "op", "op-mainnet", "optimism-mainnet"][..],
                10,
            ),
            (&["monad", "monad-mainnet", "mon"][..], 143),
            (
                &["robinhood", "robinhood-chain", "robinhood-mainnet", "rh"][..],
                4663,
            ),
        ] {
            for a in aliases {
                let c = lookup(a).unwrap_or_else(|| panic!("alias {a:?} resolves to nothing"));
                assert_eq!(c.chain_id, id, "alias {a:?} points at the wrong chain");
            }
        }
    }

    /// Every chain `lookup` knows must also be **probed** by `init` when `--chain` is omitted (#696).
    ///
    /// `all()` returned three while `lookup` knew seven, so a contract on Optimism, Polygon, BSC or
    /// Gnosis could be indexed by name and never found by auto-detect - and the README said in bold
    /// that omitting `--chain` probes all seven.
    ///
    /// The existing `all_chains_are_probeable_and_lead_with_l1` could not see it: it asserts every
    /// chain in `all()` resolves via `lookup`, which is trivially true of any *subset*. One direction
    /// of a two-list invariant, and the missing direction is the one that rots. Same shape as #353.
    ///
    /// The ids come from the alias table above rather than a fresh literal, so there are two lists in
    /// this file and not three: add a chain, add it there, and this fails until `all()` has it too.
    #[test]
    fn every_registered_chain_is_probed_by_auto_detect() {
        let probed: Vec<u64> = all().iter().map(|c| c.chain_id).collect();
        for id in [1u64, 42161, 8453, 56, 137, 100, 10, 143, 4663] {
            assert!(
                probed.contains(&id),
                "chain {id} resolves via `lookup` but `all()` never probes it, so `init` without \
                 `--chain` cannot find a contract that lives there: probed {probed:?}"
            );
        }
        assert_eq!(
            probed.len(),
            9,
            "a chain was added to `all()` without being added to the alias table above: {probed:?}"
        );
    }

    /// A shipped chain with no endpoint would be worse than an unshipped one: `init` would look
    /// supported and then have nowhere to ask.
    #[test]
    fn no_shipped_chain_has_an_empty_endpoint_list() {
        for name in [
            "mainnet",
            "arbitrum-one",
            "base",
            "bsc",
            "polygon",
            "gnosis",
            "optimism",
        ] {
            let c = lookup(name).unwrap();
            assert!(!c.rpc_urls.is_empty(), "{name} ships with no endpoints");
            assert!(
                c.log_window > 0,
                "{name} has a zero log window, which schedules nothing"
            );
        }
    }

    #[tokio::test]
    async fn resolve_of_an_unknown_name_without_rpc_names_the_remedy() {
        let err = resolve("avalanche", &[]).await.unwrap_err();
        let msg = format!("{err:#}");
        assert!(msg.contains("unknown chain 'avalanche'"), "{msg}");
        assert!(msg.contains("--rpc"), "{msg}");
        for c in all() {
            assert!(
                msg.contains(c.name),
                "{} missing from the remedy: {msg}",
                c.name
            );
        }
    }

    /// The fix for #535: an unregistered chain name is accepted once `--rpc` names where it lives,
    /// and its `chain_id` comes from the endpoint itself rather than a hand-typed flag.
    #[tokio::test]
    async fn resolve_of_an_unknown_name_with_rpc_reads_the_chain_id_live() {
        let (url, _rpc) = fake_chain_id_rpc(43114).await;
        let resolved = resolve("avalanche", std::slice::from_ref(&url))
            .await
            .unwrap();
        assert_eq!(resolved.name, "avalanche");
        assert_eq!(resolved.chain_id, 43114);
        assert_eq!(resolved.rpc_urls, vec![url]);
        // Same fallback policy the indexer already applies to a nest on an unregistered chain
        // (`indexer::DEFAULT_FINALITY`/`DEFAULT_WINDOW`) - a custom chain must not silently get a
        // laxer or stricter default than the runtime it will actually run under.
        assert_eq!(resolved.finality, UNREGISTERED_FINALITY);
        assert_eq!(resolved.log_window, UNREGISTERED_WINDOW);
    }

    #[test]
    fn from_config_of_a_known_chain_uses_the_registry_not_the_saved_id() {
        // A hand-edited `chain_id` that disagrees with the registry is not this function's problem to
        // catch (RPC startup's `verify_chain_ids` is) - it returns the registry's own policy either way.
        let resolved = from_config("base", 999);
        assert_eq!(resolved.chain_id, 8453);
        assert!(matches!(resolved.finality, Finality::FinalizedTag { .. }));
    }

    #[test]
    fn from_config_of_a_custom_chain_carries_the_saved_id_with_no_rpc_round_trip() {
        let resolved = from_config("avalanche", 43114);
        assert_eq!(resolved.name, "avalanche");
        assert_eq!(resolved.chain_id, 43114);
        assert_eq!(resolved.finality, UNREGISTERED_FINALITY);
        assert_eq!(resolved.log_window, UNREGISTERED_WINDOW);
    }
}
