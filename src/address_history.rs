//! Address history (RFC-0063): the transactions, internal transactions, token transfers, withdrawals
//! and produced blocks that touch a watched address, served Etherscan-shaped at `/api` so rotki can
//! use a nest as one of its indexers.
//!
//! The rows live in the nest's own `nuthatch.redb`, beside the hot store, so they are derived state
//! that the content address already excludes. Keys are fixed-width binary, `address | block | tx
//! index | position`, all big-endian, so byte order is history order and a page is one range read
//! over one address. Coverage is kept per (address, action) as block intervals: a range the nest has
//! not finished is answered `NUTHATCH_INCOMPLETE`, never as an empty list, because rotki falls
//! through to its next indexer only on a failure.
//!
//! The watched set lives in the store, seeded from `[address_history]`, so an address added while
//! the nest runs simply has no coverage until it is backfilled.

use anyhow::{anyhow, bail, Context, Result};
use redb::{ReadableTable, TableDefinition};
use serde::{Deserialize, Serialize};
use serde_json::{Map, Value};
use std::time::Duration;

use crate::store::Store;

/// Rotki reads history rather than the tip, so an idle rotki-mode nest polls rarely (Chief, 2026-10-09).
pub const DEFAULT_POLL_INTERVAL: Duration = Duration::from_secs(300);

/// Etherscan's largest page.
pub const MAX_OFFSET: u64 = 10_000;
pub const DEFAULT_OFFSET: u64 = 1_000;
/// A partition is a few MB; a smaller cache could not keep the one being verified.
pub const MIN_CACHE_MB: u64 = 64;

/// The first block a chain's `trace_filter` can be trusted from. Optimism answers it with an empty
/// success before Bedrock, which would read as an account with no history (RFC-0063 §7).
pub fn traced_from(chain_id: u64) -> u64 {
    match chain_id {
        10 => 105_235_063,
        // Arbitrum's Nitro genesis: the classic chain before it has no state a node can read.
        42161 => 22_207_817,
        _ => 0,
    }
}

/// The `[address_history]` table of `nuthatch.toml`.
#[derive(Debug, Serialize, Deserialize, Clone, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct AddressHistoryConfig {
    pub addresses: Vec<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub start_block: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub end_block: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub poll_interval: Option<String>,
    /// Block partitions to download rather than read block by block (RFC-0063 §8). Off unless named.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub mirror: Option<MirrorConfig>,
    /// Ask the MEV-Boost relays' public data API about the blocks a watched address produced or was
    /// paid for (RFC-0063 §8). Off unless set: the relays see this machine's IP and those block
    /// numbers, which name the validator.
    #[serde(default, skip_serializing_if = "std::ops::Not::not")]
    pub mev_relays: bool,
    /// How long a request for history above what is covered may wait while the nest covers it:
    /// queue, RPC work and the answer together. Default 8 s, under rotki's 30 s read timeout.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub request_deadline: Option<String>,
    /// Validator indices whose proposed blocks to prove from Dencun on (RFC-0063 plan C). A client
    /// such as rotki registers its own through `/api/watch`. Ethereum mainnet only.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub validators: Vec<u64>,
    /// A beacon node's API, to build the proposer catalogue for blocks the mirror has not published.
    /// Off unless named: the beacon sees this machine's IP and the beacon roots asked for.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub beacon_url: Option<String>,
    /// Read every block once for the whole watched set, rather than each address on its own
    /// (RFC-0064). Off unless set.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub sweep: Option<SweepConfig>,
}

/// `[address_history.sweep]`: where the sweep starts. Omitted, at the finalized head when it first
/// runs; history before it comes from per-address discovery, if asked for.
#[derive(Debug, Serialize, Deserialize, Clone, PartialEq, Eq, Default)]
#[serde(deny_unknown_fields)]
pub struct SweepConfig {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub from_block: Option<u64>,
    /// Trace each block for internal transfers (RFC-0064 S2). Needs a trace-capable RPC; off unless set.
    #[serde(default, skip_serializing_if = "std::ops::Not::not")]
    pub internal: bool,
    /// Sweep up to this many blocks under the chain head, rather than up to the finalized head
    /// (RFC-0064 S3). A reorg above finality takes back what it replaced, and the feed retracts it.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub confirmations: Option<u64>,
    /// POST the deposits feed here as it grows, at least once, oldest first (RFC-0064 S3). The
    /// feed stays the record: a delivery that fails is sent again, never skipped.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub webhook: Option<String>,
}

/// `[address_history.mirror]`: where verified block partitions come from, which chain and blocks to
/// take from it, and how much disk the downloaded copies may hold. The mirror sees this nest's IP
/// and the ranges it asks for, never an address.
#[derive(Debug, Serialize, Deserialize, Clone, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct MirrorConfig {
    pub url: String,
    pub chain_id: u64,
    pub from_block: u64,
    /// The last block to take from the mirror; omitted, as far as the mirror has published, found
    /// again every poll, the blocks above it read from the RPC.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub to_block: Option<u64>,
    pub cache_mb: u64,
}

impl AddressHistoryConfig {
    pub fn validate(&self) -> Result<()> {
        // An empty list is allowed: a client such as rotki registers its own accounts through
        // `/api/watch`.
        let mut seen = std::collections::HashSet::new();
        for a in &self.addresses {
            let key =
                parse_address(a).with_context(|| format!("[address_history] address `{a}`"))?;
            if !seen.insert(key) {
                bail!("[address_history] lists `{a}` twice");
            }
        }
        if let (Some(s), Some(e)) = (self.start_block, self.end_block) {
            if e < s {
                bail!("[address_history] end_block {e} is before start_block {s}");
            }
        }
        if let Some(m) = &self.mirror {
            if let Some(to) = m.to_block.filter(|t| *t < m.from_block) {
                bail!(
                    "[address_history.mirror] to_block {to} is before from_block {}",
                    m.from_block
                );
            }
            if m.cache_mb < MIN_CACHE_MB {
                bail!(
                    "[address_history.mirror] cache_mb {} cannot hold one partition; give it at least {MIN_CACHE_MB}",
                    m.cache_mb
                );
            }
        }
        if let Some(url) = self.sweep.as_ref().and_then(|s| s.webhook.as_deref()) {
            if !(url.starts_with("http://") || url.starts_with("https://")) {
                bail!("[address_history.sweep] webhook `{url}` is not an http(s) URL");
            }
        }
        self.poll_interval()?;
        self.request_deadline()?;
        Ok(())
    }

    pub fn request_deadline(&self) -> Result<Duration> {
        match &self.request_deadline {
            None => Ok(crate::address_mode::DEFAULT_DEADLINE),
            Some(s) => crate::freshness::parse_duration(s)
                .map_err(|e| anyhow!("[address_history] request_deadline `{s}`: {e}")),
        }
    }

    pub fn poll_interval(&self) -> Result<Duration> {
        match &self.poll_interval {
            None => Ok(DEFAULT_POLL_INTERVAL),
            Some(s) => crate::freshness::parse_duration(s)
                .map_err(|e| anyhow!("[address_history] poll_interval `{s}`: {e}")),
        }
    }
}

pub type Address = [u8; 20];

/// A mixed-case address must carry a valid EIP-55 checksum: one mistyped character would otherwise
/// watch someone else's account.
pub fn parse_address(address: &str) -> Result<Address> {
    let hex = address
        .strip_prefix("0x")
        .ok_or_else(|| anyhow!("not a 0x-prefixed address"))?;
    if hex.len() != 40 || !hex.bytes().all(|b| b.is_ascii_hexdigit()) {
        bail!("not a 20-byte hex address");
    }
    let mixed =
        hex.bytes().any(|b| b.is_ascii_lowercase()) && hex.bytes().any(|b| b.is_ascii_uppercase());
    if mixed {
        alloy_primitives::Address::parse_checksummed(address, None)
            .map_err(|_| anyhow!("fails its EIP-55 checksum"))?;
    }
    let mut out = [0u8; 20];
    for (i, pair) in hex.as_bytes().chunks(2).enumerate() {
        out[i] = u8::from_str_radix(std::str::from_utf8(pair)?, 16)?;
    }
    Ok(out)
}

fn address_hex(a: &Address) -> String {
    format!("0x{}", alloy_primitives::hex::encode(a))
}

/// The account actions a rotki-mode nest answers from its own rows.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Action {
    TxList,
    TxListInternal,
    TokenTx,
    BeaconWithdrawals,
    MinedBlocks,
    TokenNftTx,
    Token1155Tx,
    /// Nuthatch's own: the blocks an address produced or was paid for through a relay, with the
    /// relays' record of each (RFC-0063 §8).
    ProducedBlocks,
}

impl Action {
    pub const ALL: [Action; 8] = [
        Action::TxList,
        Action::TxListInternal,
        Action::TokenTx,
        Action::BeaconWithdrawals,
        Action::MinedBlocks,
        Action::TokenNftTx,
        Action::Token1155Tx,
        Action::ProducedBlocks,
    ];

    pub fn from_etherscan(action: &str) -> Option<Action> {
        Some(match action {
            "txlist" => Action::TxList,
            "txlistinternal" => Action::TxListInternal,
            "tokentx" => Action::TokenTx,
            "txsBeaconWithdrawal" => Action::BeaconWithdrawals,
            "getminedblocks" => Action::MinedBlocks,
            "tokennfttx" => Action::TokenNftTx,
            "token1155tx" => Action::Token1155Tx,
            "nuthatchProducedBlocks" => Action::ProducedBlocks,
            _ => return None,
        })
    }

    pub fn etherscan(self) -> &'static str {
        match self {
            Action::TxList => "txlist",
            Action::TxListInternal => "txlistinternal",
            Action::TokenTx => "tokentx",
            Action::BeaconWithdrawals => "txsBeaconWithdrawal",
            Action::MinedBlocks => "getminedblocks",
            Action::TokenNftTx => "tokennfttx",
            Action::Token1155Tx => "token1155tx",
            Action::ProducedBlocks => "nuthatchProducedBlocks",
        }
    }

    /// Stored in coverage keys, so these numbers are on disk and must never be reassigned.
    fn code(self) -> u8 {
        match self {
            Action::TxList => 1,
            Action::TxListInternal => 2,
            Action::TokenTx => 3,
            Action::BeaconWithdrawals => 4,
            Action::MinedBlocks => 5,
            Action::TokenNftTx => 6,
            Action::Token1155Tx => 7,
            Action::ProducedBlocks => 8,
        }
    }

    fn table(self) -> TableDefinition<'static, &'static [u8], &'static [u8]> {
        match self {
            Action::TxList => TXLIST,
            Action::TxListInternal => TXLIST_INTERNAL,
            Action::TokenTx => TOKENTX,
            Action::BeaconWithdrawals => WITHDRAWALS,
            Action::MinedBlocks => MINED_BLOCKS,
            Action::TokenNftTx => TOKENNFTTX,
            Action::Token1155Tx => TOKEN1155TX,
            Action::ProducedBlocks => PRODUCED_BLOCKS,
        }
    }
}

const TXLIST: TableDefinition<&[u8], &[u8]> = TableDefinition::new("ah_txlist");
const TXLIST_INTERNAL: TableDefinition<&[u8], &[u8]> = TableDefinition::new("ah_txlistinternal");
const TOKENTX: TableDefinition<&[u8], &[u8]> = TableDefinition::new("ah_tokentx");
const WITHDRAWALS: TableDefinition<&[u8], &[u8]> = TableDefinition::new("ah_withdrawals");
const MINED_BLOCKS: TableDefinition<&[u8], &[u8]> = TableDefinition::new("ah_minedblocks");
const TOKENNFTTX: TableDefinition<&[u8], &[u8]> = TableDefinition::new("ah_tokennfttx");
const TOKEN1155TX: TableDefinition<&[u8], &[u8]> = TableDefinition::new("ah_token1155tx");
const PRODUCED_BLOCKS: TableDefinition<&[u8], &[u8]> = TableDefinition::new("ah_producedblocks");
/// The relays' answers about a block, by block number, kept only for blocks the nest serves.
const RELAY_CACHE: TableDefinition<u64, &[u8]> = TableDefinition::new("ah_relay_cache");
/// Hydrated transactions by hash, so a hash fetched once is never fetched again.
const TX_CACHE: TableDefinition<&[u8], &[u8]> = TableDefinition::new("ah_tx_cache");
/// Block timestamps already read.
const BLOCK_TS: TableDefinition<u64, u64> = TableDefinition::new("ah_block_ts");
/// The header hash a `BLOCK_TS` row was read against. A separate table so the timestamp value
/// stays what `getblocknobytime` reads. No row means the timestamp is not tied to a fork.
const BLOCK_HASH: TableDefinition<u64, &[u8]> = TableDefinition::new("ah_block_hash");
/// A transaction's whole internal list, by hash, as `txhash` lookups answer it.
const TX_INTERNAL: TableDefinition<&[u8], &[u8]> = TableDefinition::new("ah_tx_internal");
/// The blocks whose headers came from a verified partition, as merged `[from, to]` pairs under one key.
/// Only within them does `BLOCK_TS` answer `getblocknobytime`.
const HEADERS: TableDefinition<&str, &[u8]> = TableDefinition::new("ah_headers");
const VERIFIED: &str = "verified";
/// Under `HEADERS`, the blocks whose body summaries `BODY_SUMMARY` holds, as merged spans.
const SUMMARIZED: &str = "summarized";
/// Each RPC-scanned body's summary: fee recipient, timestamp, hash, withdrawals. Kept for the most
/// recent [`SUMMARY_KEEP`] blocks, so an address watched later reads them rather than the chain;
/// older history comes from the partitions.
const BODY_SUMMARY: TableDefinition<u64, &[u8]> = TableDefinition::new("ah_body_summary");
pub const SUMMARY_KEEP: u64 = 100_000;
/// The part of each (address, action) coverage that came from verified partitions, in the same shape
/// as `COVERAGE`. With a mirror configured, only this part of its blocks may answer.
const VERIFIED_COV: TableDefinition<&[u8], &[u8]> = TableDefinition::new("ah_verified_coverage");
/// `address | action code` -> `[from, to]` pairs, inclusive, sorted and merged, 16 bytes each.
const COVERAGE: TableDefinition<&[u8], &[u8]> = TableDefinition::new("ah_coverage");
const WATCHED: TableDefinition<&[u8], u8> = TableDefinition::new("ah_watched");
/// Addresses removed at runtime, so a restart does not re-seed them from the config.
const UNWATCHED: TableDefinition<&[u8], u8> = TableDefinition::new("ah_unwatched");
/// Addresses a request has asked for withdrawals or produced blocks. Only these are body-scanned in
/// the background: a plain wallet is never asked, and the scan costs one body a block.
const BODIES_ASKED: TableDefinition<&[u8], u8> = TableDefinition::new("ah_bodies_asked");
/// RFC-0064's sweep: the span of blocks it has read, held once for every swept address, and the
/// block each address joined it at. An address's sweep coverage is that span from its joined block on.
const SWEEP: TableDefinition<&str, &[u8]> = TableDefinition::new("ah_sweep");
const SWEPT: &str = "swept";
/// The part of the swept span that was also traced, so internal transactions are claimed only where
/// they were read.
const SWEPT_INTERNAL: &str = "swept_internal";
const JOINED: TableDefinition<&[u8], u64> = TableDefinition::new("ah_joined");
/// The first block from which an address's history cannot be proved: where it had code, so another
/// account's code could spend its balance and the residual (RFC-0063, L2) proves nothing.
const UNPROVABLE: TableDefinition<&[u8], u64> = TableDefinition::new("ah_unprovable");
/// RFC-0064's deposits feed: every incoming row the sweep recorded that moved value, and every
/// retraction a reorg made of one, by sequence number. A consumer reads it from the last number it
/// saw.
const DEPOSITS: TableDefinition<u64, &[u8]> = TableDefinition::new("ah_deposits");
/// Deposits a reorg could still take back, keyed `block | action | address | tx index | position`
/// so the ones above a fork are one range read.
const LIVE_DEPOSITS: TableDefinition<&[u8], u64> = TableDefinition::new("ah_live_deposits");
const DEPOSIT_NEXT: &str = "deposit_next";
/// The last feed sequence number the webhook endpoint accepted.
const DEPOSIT_DELIVERED: &str = "deposit_delivered";
/// Block hashes above the finalized head that stored history was read against. The highest anchors
/// everything below it, so one comparison per poll finds a reorg.
const CANON: TableDefinition<u64, &[u8]> = TableDefinition::new("ah_canon");
/// Blocks whose relay answer lacked a delivery, re-asked on a slow schedule: a relay can save its
/// record after the block is published, or fail to.
const MEV_PENDING: TableDefinition<u64, &[u8]> = TableDefinition::new("ah_mev_pending");
/// The hashes of the newest finalized heads this store committed. History under them is never
/// rewritten, so each poll holds the newest and a sample against the chain: a reorg reaching under
/// finality, or an RPC that came back on another chain under the same id, stops the cursor.
const FINAL: TableDefinition<u64, &[u8]> = TableDefinition::new("ah_final");
const FINAL_KEEP: usize = 32;
/// Why finalized history stopped matching the chain, once it has: it outlives a restart, because the
/// rows it concerns do, and only removing the store clears it.
const CONFLICT: TableDefinition<&str, &str> = TableDefinition::new("ah_conflict");
const META: TableDefinition<&str, u64> = TableDefinition::new("ah_meta");
/// The finalized head: what the cursor backfills to and the line under which nothing is re-checked.
const HEAD: &str = "head";
/// The newest block served on demand above the finalized head.
const TIP: &str = "tip";
const GENERATION: &str = "generation";
/// Every account row and cached transaction above the finalized head, keyed `block | tag | id` (an
/// action's code and an address, or a cache's tag and a hash), so a reorg finds what it takes back
/// as one range read instead of visiting every watched address (#2117). Pruned as finality passes.
const REORGABLE: TableDefinition<&[u8], u8> = TableDefinition::new("ah_reorgable");
/// Set once [`REORGABLE`] holds everything above the head, which a store written before it did not.
const REORGABLE_COMPLETE: &str = "reorgable_complete";
const TX_CACHE_TAG: u8 = 0xf0;
const TX_INTERNAL_TAG: u8 = 0xf1;

const KEY_LEN: usize = 20 + 8 * 3;

fn row_key(a: &Address, block: u64, tx_index: u64, position: u64) -> [u8; KEY_LEN] {
    let mut k = [0u8; KEY_LEN];
    k[..20].copy_from_slice(a);
    k[20..28].copy_from_slice(&block.to_be_bytes());
    k[28..36].copy_from_slice(&tx_index.to_be_bytes());
    k[36..].copy_from_slice(&position.to_be_bytes());
    k
}

fn reorgable_key(block: u64, tag: u8, id: &[u8]) -> Vec<u8> {
    let mut k = Vec::with_capacity(9 + id.len());
    k.extend_from_slice(&block.to_be_bytes());
    k.push(tag);
    k.extend_from_slice(id);
    k
}

/// The block a cached transaction or trace names; one without is taken back by any reorg.
fn cached_block(record: &[u8]) -> Result<u64> {
    Ok(decode_record(record)?
        .get("blockNumber")
        .and_then(Value::as_str)
        .and_then(|b| b.parse::<u64>().ok())
        .unwrap_or(u64::MAX))
}

/// Fill [`REORGABLE`] from a store written before it, once: one pass over the rows and caches.
fn index_reorgable(wtx: &redb::WriteTransaction) -> Result<()> {
    let head = {
        let m = wtx.open_table(META)?;
        if m.get(REORGABLE_COMPLETE)?.is_some() {
            return Ok(());
        }
        let head = m.get(HEAD)?.map(|v| v.value());
        head
    };
    let above = |b: u64| head.is_none_or(|h| b > h);
    let mut index = wtx.open_table(REORGABLE)?;
    for action in Action::ALL {
        let t = wtx.open_table(action.table())?;
        for e in t.iter()? {
            let (k, _) = e?;
            let k = k.value();
            let block = u64::from_be_bytes(k[20..28].try_into().expect("row keys are 44 bytes"));
            if above(block) {
                index.insert(reorgable_key(block, action.code(), &k[..20]).as_slice(), 1)?;
            }
        }
    }
    for (table, tag) in [(TX_CACHE, TX_CACHE_TAG), (TX_INTERNAL, TX_INTERNAL_TAG)] {
        let t = wtx.open_table(table)?;
        for e in t.iter()? {
            let (k, v) = e?;
            let block = cached_block(v.value())?;
            if above(block) {
                index.insert(reorgable_key(block, tag, k.value()).as_slice(), 1)?;
            }
        }
    }
    wtx.open_table(META)?.insert(REORGABLE_COMPLETE, 1)?;
    Ok(())
}

fn coverage_key(a: &Address, action: Action) -> [u8; 21] {
    let mut k = [0u8; 21];
    k[..20].copy_from_slice(a);
    k[20] = action.code();
    k
}

/// Where a recorded span was read from: the RPC, against an anchor block when above the finalized
/// head, or a verified partition.
#[derive(Debug, Clone, Copy)]
enum Read {
    Rpc(Option<(u64, [u8; 32])>),
    Partition,
}

/// A block and its body summary.
pub type Summary = (u64, Map<String, Value>);

/// A transaction hash, its block, the block's hash as the trace named it, and its whole internal list.
pub type TxInternals = (Vec<u8>, u64, [u8; 32], Vec<Map<String, Value>>);

/// One stored record and where it sorts.
#[derive(Debug, Clone)]
pub struct Row {
    pub block: u64,
    pub tx_index: u64,
    pub position: u64,
    pub record: Map<String, Value>,
}

/// What a page request comes back as.
#[derive(Debug, PartialEq)]
pub enum Answer {
    Rows(Vec<Value>),
    Incomplete(String),
    /// Never servable here, however long one waits: the caller should ask elsewhere.
    Unsupported(String),
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Sort {
    Asc,
    Desc,
}

#[derive(Debug, Clone)]
pub struct PageRequest {
    pub address: String,
    pub start_block: Option<u64>,
    pub end_block: Option<u64>,
    pub sort: Sort,
    pub page: u64,
    pub offset: u64,
    /// The generation page 1 was cut from. Pages after the first carry it, so a reorg or unwatch
    /// between pages reads incomplete instead of silently shifting rows across a page boundary.
    pub generation: Option<u64>,
}

#[cfg(test)]
thread_local! {
    /// Rows the last `page` call on this thread read from its table, so a test can see the index
    /// was used rather than a scan.
    static ROWS_READ: std::cell::Cell<usize> = const { std::cell::Cell::new(0) };
}

#[derive(Clone)]
pub struct AddressHistory {
    store: Store,
    chain_id: u64,
}

/// Whether the store at `path` holds history from before finalized hashes were recorded (a
/// committed head and none of them), and if so the accounts it watched. Unless `repair` is set, a
/// store redb would have to repair is refused with [`redb::DatabaseError::RepairAborted`] rather
/// than repaired, so a store moved aside for it is moved as it was.
pub fn pre_finality(path: &std::path::Path, repair: bool) -> Result<Option<Accounts>> {
    if !path.exists() {
        return Ok(None);
    }
    let mut builder = redb::Builder::new();
    if !repair {
        builder.set_repair_callback(redb::RepairSession::abort);
    }
    let db = builder.open(path)?;
    let rtx = db.begin_read()?;
    let head = match rtx.open_table(META) {
        Ok(t) => t.get(HEAD)?.map(|v| v.value()),
        Err(redb::TableError::TableDoesNotExist(_)) => None,
        Err(e) => return Err(e.into()),
    };
    let finals = match rtx.open_table(FINAL) {
        Ok(t) => redb::ReadableTableMetadata::len(&t)?,
        Err(redb::TableError::TableDoesNotExist(_)) => 0,
        Err(e) => return Err(e.into()),
    };
    if head.is_none() || finals > 0 {
        return Ok(None);
    }
    let list = |def: TableDefinition<&[u8], u8>| -> Result<Vec<String>> {
        match rtx.open_table(def) {
            Ok(t) => t
                .iter()?
                .map(|e| {
                    let (k, _) = e?;
                    let a: Address = k
                        .value()
                        .try_into()
                        .context("account key is not 20 bytes")?;
                    Ok(address_hex(&a))
                })
                .collect(),
            Err(redb::TableError::TableDoesNotExist(_)) => Ok(Vec::new()),
            Err(e) => Err(e.into()),
        }
    };
    Ok(Some(Accounts {
        watched: list(WATCHED)?,
        unwatched: list(UNWATCHED)?,
    }))
}

/// The accounts a store watched, and those removed from it, which configuration must not bring back.
pub struct Accounts {
    pub watched: Vec<String>,
    pub unwatched: Vec<String>,
}

/// A finalized block the store recorded is now reported at another hash.
#[derive(Debug)]
pub struct FinalConflict(pub u64);

impl std::fmt::Display for FinalConflict {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "finalized block {} is now at another hash", self.0)
    }
}

impl std::error::Error for FinalConflict {}

fn finalize_in(wtx: &redb::WriteTransaction, block: u64) -> Result<()> {
    let mut m = wtx.open_table(META)?;
    let head = m.get(HEAD)?.map(|v| v.value());
    if head.is_none_or(|h| block > h) {
        m.insert(HEAD, block)?;
    }
    {
        let mut index = wtx.open_table(REORGABLE)?;
        let settled: Vec<Vec<u8>> = index
            .range(..reorgable_key(block.saturating_add(1), 0, &[]).as_slice())?
            .map(|e| e.map(|(k, _)| k.value().to_vec()))
            .collect::<std::result::Result<_, _>>()?;
        for k in settled {
            index.remove(k.as_slice())?;
        }
    }
    let mut c = wtx.open_table(CANON)?;
    let done: Vec<u64> = c
        .range(..=block)?
        .map(|e| e.map(|(k, _)| k.value()))
        .collect::<std::result::Result<_, _>>()?;
    for b in done {
        c.remove(b)?;
    }
    Ok(())
}

impl AddressHistory {
    /// Open the tables, and add `seed` (the configured addresses) to the watched set. Addresses
    /// already watched are left alone, none are removed, and one unwatched at runtime is not brought
    /// back by the config that first seeded it.
    pub fn open(store: Store, chain_id: u64, seed: &[String]) -> Result<AddressHistory> {
        let seed: Vec<Address> = seed
            .iter()
            .map(|a| parse_address(a))
            .collect::<Result<_>>()?;
        let db = store.database();
        let wtx = db.begin_write()?;
        for a in Action::ALL {
            wtx.open_table(a.table())?;
        }
        wtx.open_table(COVERAGE)?;
        wtx.open_table(META)?;
        wtx.open_table(TX_CACHE)?;
        wtx.open_table(BLOCK_TS)?;
        wtx.open_table(BLOCK_HASH)?;
        wtx.open_table(TX_INTERNAL)?;
        wtx.open_table(HEADERS)?;
        wtx.open_table(VERIFIED_COV)?;
        wtx.open_table(RELAY_CACHE)?;
        wtx.open_table(CANON)?;
        wtx.open_table(MEV_PENDING)?;
        wtx.open_table(BODY_SUMMARY)?;
        wtx.open_table(FINAL)?;
        wtx.open_table(CONFLICT)?;
        wtx.open_table(BODIES_ASKED)?;
        wtx.open_table(SWEEP)?;
        wtx.open_table(JOINED)?;
        wtx.open_table(UNPROVABLE)?;
        wtx.open_table(DEPOSITS)?;
        wtx.open_table(LIVE_DEPOSITS)?;
        wtx.open_table(REORGABLE)?;
        reset_produced_blocks(&wtx)?;
        index_reorgable(&wtx)?;
        {
            let dropped = wtx.open_table(UNWATCHED)?;
            let mut w = wtx.open_table(WATCHED)?;
            for a in &seed {
                if dropped.get(a.as_slice())?.is_none() {
                    w.insert(a.as_slice(), 1)?;
                }
            }
        }
        wtx.commit()?;
        Ok(AddressHistory { store, chain_id })
    }

    pub fn store(&self) -> &Store {
        &self.store
    }

    pub fn chain_id(&self) -> u64 {
        self.chain_id
    }

    /// A transaction already hydrated, by its 32-byte hash.
    pub fn cached_tx(&self, hash: &[u8]) -> Result<Option<Map<String, Value>>> {
        let rtx = self.store.database().begin_read()?;
        match rtx.open_table(TX_CACHE)?.get(hash)? {
            Some(v) => Ok(Some(decode_record(v.value())?)),
            None => Ok(None),
        }
    }

    /// Every internal transaction of a transaction already traced, by its 32-byte hash.
    pub fn tx_internals(&self, hash: &[u8]) -> Result<Option<Vec<Map<String, Value>>>> {
        let rtx = self.store.database().begin_read()?;
        let Some(v) = rtx.open_table(TX_INTERNAL)?.get(hash)? else {
            return Ok(None);
        };
        let wrapped = decode_record(v.value())?;
        let rows = wrapped
            .get("rows")
            .and_then(Value::as_array)
            .ok_or_else(|| anyhow!("a cached internal list without rows"))?
            .iter()
            .map(|r| {
                r.as_object()
                    .cloned()
                    .ok_or_else(|| anyhow!("an internal row is not an object"))
            })
            .collect::<Result<_>>()?;
        Ok(Some(rows))
    }

    /// The block a cached trace was read from, and that block's hash as the trace named it (absent
    /// for a trace kept before 5.2, which is traced again rather than trusted).
    pub fn tx_internals_block(&self, hash: &[u8]) -> Result<Option<(u64, Option<[u8; 32]>)>> {
        let rtx = self.store.database().begin_read()?;
        let Some(v) = rtx.open_table(TX_INTERNAL)?.get(hash)? else {
            return Ok(None);
        };
        let wrapped = decode_record(v.value())?;
        let Some(block) = wrapped
            .get("blockNumber")
            .and_then(Value::as_str)
            .and_then(|b| b.parse().ok())
        else {
            return Ok(None);
        };
        let named = wrapped
            .get("blockHash")
            .and_then(Value::as_str)
            .and_then(|h| alloy_primitives::hex::decode(h.trim_start_matches("0x")).ok())
            .and_then(|h| <[u8; 32]>::try_from(h).ok());
        Ok(Some((block, named)))
    }

    /// What a fetch read, in one transaction: transactions, timestamps, traces and the block hashes
    /// they were read against. Nothing from a fetch that failed its check (`trusted` false). Otherwise
    /// what lay under the finalized head when the fetch began, and what lay above it only if the
    /// generation has not moved since: a reorg handled meanwhile may have replaced its block, even if
    /// the new chain has since grown, or finalized, back past it.
    #[allow(clippy::too_many_arguments)]
    pub fn persist_pending(
        &self,
        txs: &[(Vec<u8>, Map<String, Value>)],
        timestamps: &[(u64, u64, Option<[u8; 32]>)],
        internals: &[TxInternals],
        anchors: &[(u64, [u8; 32])],
        summaries: &[Summary],
        generation: u64,
        finalized_at_start: Option<u64>,
        trusted: bool,
    ) -> Result<()> {
        let wtx = self.store.database().begin_write()?;
        {
            let (head, now) = {
                let m = wtx.open_table(META)?;
                let head = m.get(HEAD)?.map(|v| v.value());
                let now = m.get(GENERATION)?.map_or(0, |v| v.value());
                (head, now)
            };
            let current = trusted && now == generation;
            let keep =
                |block: u64| trusted && (current || finalized_at_start.is_some_and(|h| block <= h));
            let mut buf = Vec::new();
            {
                let mut t = wtx.open_table(TX_CACHE)?;
                for (hash, record) in txs {
                    let block = record
                        .get("blockNumber")
                        .and_then(Value::as_str)
                        .and_then(|b| b.parse::<u64>().ok());
                    if !block.is_some_and(keep) {
                        continue;
                    }
                    buf.clear();
                    encode_record(record, &mut buf)?;
                    t.insert(hash.as_slice(), buf.as_slice())?;
                    if let Some(b) = block.filter(|b| head.is_none_or(|h| *b > h)) {
                        wtx.open_table(REORGABLE)?
                            .insert(reorgable_key(b, TX_CACHE_TAG, hash).as_slice(), 1)?;
                    }
                }
            }
            {
                let verified = match wtx.open_table(HEADERS)?.get(VERIFIED)? {
                    Some(v) => decode_spans(v.value())?,
                    None => Vec::new(),
                };
                let mut t = wtx.open_table(BLOCK_TS)?;
                let mut hashes = wtx.open_table(BLOCK_HASH)?;
                for (b, s, hash) in timestamps {
                    if keep(*b) && !verified.iter().any(|(f, l)| f <= b && b <= l) {
                        t.insert(*b, *s)?;
                        // A timestamp written without a hash must not keep an older fork's hash.
                        match hash {
                            Some(h) => hashes.insert(*b, h.as_slice())?,
                            None => hashes.remove(*b)?,
                        };
                    }
                }
            }
            {
                let mut t = wtx.open_table(TX_INTERNAL)?;
                for (hash, block, named, rows) in internals {
                    if !keep(*block) {
                        continue;
                    }
                    let mut wrapped = Map::new();
                    wrapped.insert("blockNumber".into(), Value::String(block.to_string()));
                    wrapped.insert(
                        "blockHash".into(),
                        Value::String(format!("0x{}", alloy_primitives::hex::encode(named))),
                    );
                    wrapped.insert(
                        "rows".into(),
                        Value::Array(rows.iter().cloned().map(Value::Object).collect()),
                    );
                    buf.clear();
                    encode_record(&wrapped, &mut buf)?;
                    t.insert(hash.as_slice(), buf.as_slice())?;
                    if head.is_none_or(|h| *block > h) {
                        wtx.open_table(REORGABLE)?
                            .insert(reorgable_key(*block, TX_INTERNAL_TAG, hash).as_slice(), 1)?;
                    }
                }
            }
            {
                let kept: Vec<&(u64, Map<String, Value>)> =
                    summaries.iter().filter(|(b, _)| keep(*b)).collect();
                if !kept.is_empty() {
                    let mut t = wtx.open_table(BODY_SUMMARY)?;
                    let mut h = wtx.open_table(HEADERS)?;
                    let mut spans = match h.get(SUMMARIZED)? {
                        Some(v) => decode_spans(v.value())?,
                        None => Vec::new(),
                    };
                    for (b, sum) in kept {
                        buf.clear();
                        encode_record(sum, &mut buf)?;
                        t.insert(*b, buf.as_slice())?;
                        spans.push((*b, *b));
                    }
                    let mut spans = merge(spans);
                    // Only the newest blocks are kept.
                    if let Some(&(_, newest)) = spans.last() {
                        let floor = newest.saturating_sub(SUMMARY_KEEP - 1);
                        if spans.first().is_some_and(|(f, _)| *f < floor) {
                            let old: Vec<u64> = t
                                .range(..floor)?
                                .map(|e| e.map(|(k, _)| k.value()))
                                .collect::<std::result::Result<_, _>>()?;
                            for b in old {
                                t.remove(b)?;
                            }
                            spans = subtract(&spans, (0, floor - 1));
                        }
                    }
                    h.insert(SUMMARIZED, encode_spans(&spans).as_slice())?;
                }
            }
            if current {
                let mut c = wtx.open_table(CANON)?;
                for (block, hash) in anchors {
                    if head.is_none_or(|h| *block > h) {
                        anchor(&mut c, *block, hash)?;
                    }
                }
            }
        }
        wtx.commit()?;
        Ok(())
    }

    /// The body summaries of `[from, to]`, if every block in it has one.
    pub fn summaries(&self, from: u64, to: u64) -> Result<Option<Vec<Summary>>> {
        let rtx = self.store.database().begin_read()?;
        let spans = match rtx.open_table(HEADERS)?.get(SUMMARIZED)? {
            Some(v) => decode_spans(v.value())?,
            None => return Ok(None),
        };
        if !spans.iter().any(|(f, t)| *f <= from && to <= *t) {
            return Ok(None);
        }
        let t = rtx.open_table(BODY_SUMMARY)?;
        let rows = t
            .range(from..=to)?
            .map(|e| {
                let (k, v) = e?;
                Ok((k.value(), decode_record(v.value())?))
            })
            .collect::<Result<Vec<_>>>()?;
        Ok((rows.len() as u64 == to - from + 1).then_some(rows))
    }

    /// The tracked block hashes above the finalized head, lowest first.
    pub fn tracked(&self) -> Result<Vec<(u64, [u8; 32])>> {
        let rtx = self.store.database().begin_read()?;
        rtx.open_table(CANON)?
            .iter()?
            .map(|e| {
                let (k, v) = e?;
                let hash: [u8; 32] = v.value().try_into().map_err(|_| {
                    anyhow!("a tracked hash at block {} is not 32 bytes", k.value())
                })?;
                Ok((k.value(), hash))
            })
            .collect()
    }

    /// Move the finalized head up to `block` and stop tracking hashes at or under it. Never lowers
    /// the head: an RPC failing over to a lagging node must not unfinalize anything.
    pub fn finalize(&self, block: u64) -> Result<()> {
        let wtx = self.store.database().begin_write()?;
        finalize_in(&wtx, block)?;
        wtx.commit()?;
        Ok(())
    }

    /// [`AddressHistory::finalize`] `block`, recording `hash` as its own in the same transaction and
    /// keeping the newest [`FINAL_KEEP`], so no crash can leave a committed head without its hash.
    /// A block already recorded at another hash is a [`FinalConflict`], and nothing is written.
    pub fn finalize_at(&self, block: u64, hash: [u8; 32]) -> Result<()> {
        let wtx = self.store.database().begin_write()?;
        {
            let mut t = wtx.open_table(FINAL)?;
            if let Some(old) = t.get(block)? {
                if old.value() != hash.as_slice() {
                    return Err(FinalConflict(block).into());
                }
            }
            t.insert(block, hash.as_slice())?;
            let all: Vec<u64> = t
                .iter()?
                .map(|e| e.map(|(k, _)| k.value()))
                .collect::<std::result::Result<_, _>>()?;
            for b in all.iter().take(all.len().saturating_sub(FINAL_KEEP)) {
                t.remove(*b)?;
            }
            finalize_in(&wtx, block)?;
        }
        wtx.commit()?;
        Ok(())
    }

    /// Record the first conflict with finalized history; a later one keeps the first reason.
    pub fn set_conflict(&self, why: &str) -> Result<()> {
        let wtx = self.store.database().begin_write()?;
        {
            let mut t = wtx.open_table(CONFLICT)?;
            if t.get("why")?.is_none() {
                t.insert("why", why)?;
            }
        }
        wtx.commit()?;
        Ok(())
    }

    /// The conflict with finalized history this store recorded, if any.
    pub fn conflict(&self) -> Result<Option<String>> {
        let rtx = self.store.database().begin_read()?;
        Ok(rtx
            .open_table(CONFLICT)?
            .get("why")?
            .map(|v| v.value().to_string()))
    }

    /// The finalized heads' hashes recorded, oldest first.
    pub fn finals(&self) -> Result<Vec<(u64, [u8; 32])>> {
        let rtx = self.store.database().begin_read()?;
        rtx.open_table(FINAL)?
            .iter()?
            .map(|e| {
                let (k, v) = e?;
                let h: [u8; 32] = v.value().try_into().map_err(|_| {
                    anyhow!("a finalized hash at block {} is not 32 bytes", k.value())
                })?;
                Ok((k.value(), h))
            })
            .collect()
    }

    /// The newest block served on demand, if any was. Only ever rises, except under a reorg.
    pub fn tip(&self) -> Result<Option<u64>> {
        let rtx = self.store.database().begin_read()?;
        Ok(rtx.open_table(META)?.get(TIP)?.map(|v| v.value()))
    }

    pub fn raise_tip(&self, block: u64) -> Result<()> {
        let wtx = self.store.database().begin_write()?;
        {
            let mut m = wtx.open_table(META)?;
            if m.get(TIP)?.is_none_or(|v| block > v.value()) {
                m.insert(TIP, block)?;
            }
        }
        wtx.commit()?;
        Ok(())
    }

    /// The blocks of `[from, to]` that (address, action) has no coverage for, lowest first.
    pub fn uncovered(
        &self,
        action: Action,
        address: &str,
        (from, to): (u64, u64),
    ) -> Result<Vec<(u64, u64)>> {
        if to < from {
            return Ok(Vec::new());
        }
        Ok(subtract_all(
            &[(from, to)],
            &self.coverage(action, address)?,
        ))
    }

    pub fn block_timestamp(&self, block: u64) -> Result<Option<u64>> {
        let rtx = self.store.database().begin_read()?;
        Ok(rtx.open_table(BLOCK_TS)?.get(block)?.map(|v| v.value()))
    }

    /// A cached timestamp for `block` read against header `expected`. A row with no stored hash is
    /// trusted only inside a verified header span: elsewhere it may be the block this number had
    /// on another fork, and a same-number reorg does not drop the row at the base.
    pub fn timestamp_on_fork(&self, block: u64, expected: [u8; 32]) -> Result<Option<u64>> {
        let rtx = self.store.database().begin_read()?;
        let Some(ts) = rtx.open_table(BLOCK_TS)?.get(block)?.map(|v| v.value()) else {
            return Ok(None);
        };
        let stored = match rtx.open_table(BLOCK_HASH)?.get(block)? {
            Some(v) if v.value().len() == 32 => {
                let mut h = [0u8; 32];
                h.copy_from_slice(v.value());
                Some(h)
            }
            _ => None,
        };
        if stored == Some(expected) {
            return Ok(Some(ts));
        }
        if stored.is_some() {
            return Ok(None);
        }
        let spans = match rtx.open_table(HEADERS)?.get(VERIFIED)? {
            Some(v) => decode_spans(v.value())?,
            None => Vec::new(),
        };
        Ok(spans
            .iter()
            .any(|(from, to)| *from <= block && block <= *to)
            .then_some(ts))
    }

    /// Timestamps read over RPC. A block with a verified header keeps the verified one: a slow read
    /// landing after a partition was recorded must not replace it.
    pub fn cache_block_timestamps(&self, ts: &[(u64, u64)]) -> Result<()> {
        let wtx = self.store.database().begin_write()?;
        {
            let verified = match wtx.open_table(HEADERS)?.get(VERIFIED)? {
                Some(v) => decode_spans(v.value())?,
                None => Vec::new(),
            };
            let mut t = wtx.open_table(BLOCK_TS)?;
            for (b, s) in ts {
                if !verified.iter().any(|(f, l)| f <= b && b <= l) {
                    t.insert(*b, *s)?;
                }
            }
        }
        wtx.commit()?;
        Ok(())
    }

    /// Record that every block in `[from, to]` has a verified header, with `ts` holding each one's
    /// timestamp.
    pub fn record_headers(&self, (from, to): (u64, u64), ts: &[(u64, u64)]) -> Result<()> {
        let contiguous = ts.len() as u64 == to - from + 1
            && ts.iter().zip(from..=to).all(|((b, _), want)| *b == want);
        if !contiguous {
            bail!("header timestamps do not cover [{from}, {to}] block by block");
        }
        let wtx = self.store.database().begin_write()?;
        {
            let mut t = wtx.open_table(BLOCK_TS)?;
            let mut hashes = wtx.open_table(BLOCK_HASH)?;
            for (b, s) in ts {
                t.insert(*b, *s)?;
                // The partition's timestamp replaces a cached one. A side hash from before the
                // partition would disagree with it, and the span is the record of the chain.
                hashes.remove(*b)?;
            }
            let mut h = wtx.open_table(HEADERS)?;
            let mut spans = match h.get(VERIFIED)? {
                Some(v) => decode_spans(v.value())?,
                None => Vec::new(),
            };
            spans.push((from, to));
            h.insert(VERIFIED, encode_spans(&merge(spans)).as_slice())?;
        }
        wtx.commit()?;
        Ok(())
    }

    pub fn header_coverage(&self) -> Result<Vec<(u64, u64)>> {
        let rtx = self.store.database().begin_read()?;
        match rtx.open_table(HEADERS)?.get(VERIFIED)? {
            Some(v) => decode_spans(v.value()),
            None => Ok(Vec::new()),
        }
    }

    /// Etherscan's `getblocknobytime`: the last block at or before `t`, or the first at or after it.
    /// Answered only inside a run of verified headers, where a block's neighbour is known too;
    /// `None` when no run brackets `t`. Timestamps rise strictly from block to block, which is what
    /// lets an equal timestamp at a run's edge answer.
    pub fn block_by_time(&self, t: u64, after: bool) -> Result<Option<u64>> {
        let rtx = self.store.database().begin_read()?;
        let spans = match rtx.open_table(HEADERS)?.get(VERIFIED)? {
            Some(v) => decode_spans(v.value())?,
            None => return Ok(None),
        };
        let table = rtx.open_table(BLOCK_TS)?;
        let ts = |b: u64| -> Result<u64> {
            table
                .get(b)?
                .map(|v| v.value())
                .ok_or_else(|| anyhow!("verified block {b} has no timestamp"))
        };
        for (from, to) in spans {
            if t < ts(from)? || t > ts(to)? {
                continue;
            }
            let (mut lo, mut hi) = (from, to);
            while lo < hi {
                if after {
                    let mid = lo + (hi - lo) / 2;
                    if ts(mid)? >= t {
                        hi = mid;
                    } else {
                        lo = mid + 1;
                    }
                } else {
                    let mid = lo + (hi - lo).div_ceil(2);
                    if ts(mid)? <= t {
                        lo = mid;
                    } else {
                        hi = mid - 1;
                    }
                }
            }
            return Ok(Some(lo));
        }
        Ok(None)
    }

    /// Bumped by every change that removes rows or coverage. A fetch records the generation it
    /// started under, and [`AddressHistory::record`] refuses its result if a reorg or unwatch came
    /// in between, so rows a reorg dropped are never re-marked complete.
    pub fn generation(&self) -> Result<u64> {
        let rtx = self.store.database().begin_read()?;
        Ok(rtx
            .open_table(META)?
            .get(GENERATION)?
            .map_or(0, |v| v.value()))
    }

    pub fn watched(&self) -> Result<Vec<String>> {
        let rtx = self.store.database().begin_read()?;
        let t = rtx.open_table(WATCHED)?;
        t.iter()?
            .map(|e| {
                let (k, _) = e?;
                let a: Address = k
                    .value()
                    .try_into()
                    .context("watched key is not 20 bytes")?;
                Ok(address_hex(&a))
            })
            .collect()
    }

    /// Start watching every one of `addresses` in one transaction, all or none: a bad address refuses
    /// the batch before anything is written. Returns how many were not watched before.
    pub fn watch_many(&self, addresses: &[String]) -> Result<usize> {
        let parsed = addresses
            .iter()
            .map(|a| parse_address(a).with_context(|| format!("address `{a}`")))
            .collect::<Result<Vec<_>>>()?;
        let wtx = self.store.database().begin_write()?;
        let mut added = 0;
        {
            let mut w = wtx.open_table(WATCHED)?;
            let mut u = wtx.open_table(UNWATCHED)?;
            for a in &parsed {
                if w.insert(a.as_slice(), 1)?.is_none() {
                    added += 1;
                }
                u.remove(a.as_slice())?;
            }
        }
        wtx.commit()?;
        Ok(added)
    }

    /// Record that `address`'s history cannot be proved from `block` on; the earliest block wins.
    pub fn mark_unprovable(&self, address: &str, block: u64) -> Result<()> {
        let a = parse_address(address)?;
        let wtx = self.store.database().begin_write()?;
        {
            let mut t = wtx.open_table(UNPROVABLE)?;
            let keep = t.get(a.as_slice())?.map_or(block, |v| v.value().min(block));
            t.insert(a.as_slice(), keep)?;
        }
        wtx.commit()?;
        Ok(())
    }

    pub fn unprovable_from(&self, address: &str) -> Result<Option<u64>> {
        let a = parse_address(address)?;
        let rtx = self.store.database().begin_read()?;
        Ok(rtx
            .open_table(UNPROVABLE)?
            .get(a.as_slice())?
            .map(|v| v.value()))
    }

    pub fn watched_count(&self) -> Result<u64> {
        let rtx = self.store.database().begin_read()?;
        Ok(redb::ReadableTableMetadata::len(&rtx.open_table(WATCHED)?)?)
    }

    /// Start watching `address`. It has no coverage until a backfill records some, so `/api` reads
    /// incomplete for it meanwhile.
    pub fn watch(&self, address: &str) -> Result<()> {
        let a = parse_address(address)?;
        let wtx = self.store.database().begin_write()?;
        wtx.open_table(WATCHED)?.insert(a.as_slice(), 1)?;
        wtx.open_table(UNWATCHED)?.remove(a.as_slice())?;
        wtx.commit()?;
        Ok(())
    }

    /// Stop watching `address`, dropping its rows and coverage.
    pub fn unwatch(&self, address: &str) -> Result<()> {
        let a = parse_address(address)?;
        let wtx = self.store.database().begin_write()?;
        {
            wtx.open_table(WATCHED)?.remove(a.as_slice())?;
            wtx.open_table(UNWATCHED)?.insert(a.as_slice(), 1)?;
            wtx.open_table(BODIES_ASKED)?.remove(a.as_slice())?;
            wtx.open_table(JOINED)?.remove(a.as_slice())?;
            let lo = row_key(&a, 0, 0, 0);
            let hi = row_key(&a, u64::MAX, u64::MAX, u64::MAX);
            for action in Action::ALL {
                let mut t = wtx.open_table(action.table())?;
                drain(&mut t, &lo, &hi)?;
                wtx.open_table(COVERAGE)?
                    .remove(coverage_key(&a, action).as_slice())?;
                wtx.open_table(VERIFIED_COV)?
                    .remove(coverage_key(&a, action).as_slice())?;
            }
            // No generation change: the address's rows and coverage go with it, so its pages read
            // incomplete, and a fetch for it is refused as not watched; a window in flight for every
            // other address stays good.
        }
        wtx.commit()?;
        Ok(())
    }

    pub fn ask_bodies(&self, address: &str) -> Result<()> {
        let a = parse_address(address)?;
        // Every withdrawals request lands here; a commit each time would be a write per read.
        if self
            .store
            .database()
            .begin_read()?
            .open_table(BODIES_ASKED)?
            .get(a.as_slice())?
            .is_some()
        {
            return Ok(());
        }
        let wtx = self.store.database().begin_write()?;
        wtx.open_table(BODIES_ASKED)?.insert(a.as_slice(), 1)?;
        wtx.commit()?;
        Ok(())
    }

    /// Whether the background body scan covers `address`: it was asked for withdrawals or produced
    /// blocks, or has body coverage from before the scan waited to be asked.
    pub fn bodies_wanted(&self, address: &str) -> Result<bool> {
        let a = parse_address(address)?;
        let rtx = self.store.database().begin_read()?;
        if rtx.open_table(BODIES_ASKED)?.get(a.as_slice())?.is_some() {
            return Ok(true);
        }
        let cov = rtx.open_table(COVERAGE)?;
        for action in [Action::BeaconWithdrawals, Action::MinedBlocks] {
            if cov.get(coverage_key(&a, action).as_slice())?.is_some() {
                return Ok(true);
            }
        }
        Ok(false)
    }

    pub fn is_watched(&self, address: &str) -> Result<bool> {
        let a = parse_address(address)?;
        let rtx = self.store.database().begin_read()?;
        Ok(rtx.open_table(WATCHED)?.get(a.as_slice())?.is_some())
    }

    /// The watched accounts with coverage of their own for any of `actions`, and with `asked` those
    /// asked for bodies: the few a per-account pass has work for, found without visiting every
    /// watched address (#2275). A sweep's shared span is no account's own.
    pub fn accounts_with(&self, actions: &[Action], asked: bool) -> Result<Vec<String>> {
        let rtx = self.store.database().begin_read()?;
        let mut found = std::collections::BTreeSet::new();
        for e in rtx.open_table(COVERAGE)?.iter()? {
            let (k, _) = e?;
            let k = k.value();
            if actions.iter().any(|a| a.code() == k[20]) {
                found.insert(k[..20].to_vec());
            }
        }
        if asked {
            for e in rtx.open_table(BODIES_ASKED)?.iter()? {
                found.insert(e?.0.value().to_vec());
            }
        }
        let watched = rtx.open_table(WATCHED)?;
        let mut accounts = Vec::new();
        for a in found {
            if watched.get(a.as_slice())?.is_some() {
                accounts.push(address_hex(
                    &a.try_into()
                        .map_err(|_| anyhow!("account keys are 20 bytes"))?,
                ));
            }
        }
        Ok(accounts)
    }

    /// Store a fetch's result: `rows`, replacing what `[from, to]` held, and the claim that the span
    /// is complete for (address, action), in one transaction. Refused if the address is not watched
    /// or the generation moved since the fetch began, because either way the claim no longer
    /// describes what is stored.
    pub fn record(
        &self,
        action: Action,
        address: &str,
        rows: &[Row],
        span: (u64, u64),
        generation: u64,
    ) -> Result<()> {
        self.record_with(action, address, rows, span, generation, Read::Rpc(None))
    }

    /// [`AddressHistory::record`] for a span read against the chain as it stood at block `anchor.0`,
    /// whose hash was `anchor.1`. Above the finalized head the hash is tracked, so a reorg found
    /// later takes the span back out; a conflicting tracked hash refuses the record.
    pub fn record_anchored(
        &self,
        action: Action,
        address: &str,
        rows: &[Row],
        span: (u64, u64),
        generation: u64,
        anchor: Option<(u64, [u8; 32])>,
    ) -> Result<()> {
        self.record_with(action, address, rows, span, generation, Read::Rpc(anchor))
    }

    /// [`AddressHistory::record`] for rows read from a verified partition: the span is marked
    /// verified.
    pub fn record_verified(
        &self,
        action: Action,
        address: &str,
        rows: &[Row],
        span: (u64, u64),
        generation: u64,
    ) -> Result<()> {
        self.record_with(action, address, rows, span, generation, Read::Partition)
    }

    fn record_with(
        &self,
        action: Action,
        address: &str,
        rows: &[Row],
        (from, to): (u64, u64),
        generation: u64,
        read: Read,
    ) -> Result<()> {
        let (verified, anchored) = match read {
            Read::Partition => (true, None),
            Read::Rpc(anchor) => (false, anchor),
        };
        if to < from {
            bail!("coverage interval [{from}, {to}] is empty");
        }
        let a = parse_address(address)?;
        let wtx = self.store.database().begin_write()?;
        {
            if wtx.open_table(WATCHED)?.get(a.as_slice())?.is_none() {
                bail!("{} is not watched", address_hex(&a));
            }
            let now = wtx
                .open_table(META)?
                .get(GENERATION)?
                .map_or(0, |v| v.value());
            if now != generation {
                bail!("address history changed during the fetch (generation {generation} -> {now}); refetch");
            }
            if let Some(r) = rows.iter().find(|r| r.block < from || r.block > to) {
                bail!("a row at block {} lies outside [{from}, {to}]", r.block);
            }
            // A recorded span is complete, so its rows replace whatever the span held, from
            // either source.
            drain(
                &mut wtx.open_table(action.table())?,
                &row_key(&a, from, 0, 0),
                &row_key(&a, to, u64::MAX, u64::MAX),
            )?;
            if verified {
                add_span(
                    &mut wtx.open_table(VERIFIED_COV)?,
                    &coverage_key(&a, action),
                    (from, to),
                )?;
            } else {
                // Rows the RPC wrote are not verified, whatever was there before them.
                let key = coverage_key(&a, action);
                let mut v = wtx.open_table(VERIFIED_COV)?;
                let kept = match v.get(key.as_slice())? {
                    Some(s) => Some(subtract(&decode_spans(s.value())?, (from, to))),
                    None => None,
                };
                if let Some(kept) = kept {
                    v.insert(key.as_slice(), encode_spans(&kept).as_slice())?;
                }
            }
            if let Some((block, hash)) = anchored {
                let head = wtx.open_table(META)?.get(HEAD)?.map(|v| v.value());
                if head.is_none_or(|h| block > h) {
                    anchor(&mut wtx.open_table(CANON)?, block, &hash)?;
                }
            }
            put_rows(&wtx, action, &a, rows)?;
            cover(&wtx, action, &a, from, to)?;
        }
        wtx.commit()?;
        Ok(())
    }

    pub fn verified_coverage(&self, action: Action, address: &str) -> Result<Vec<(u64, u64)>> {
        let a = parse_address(address)?;
        let rtx = self.store.database().begin_read()?;
        match rtx
            .open_table(VERIFIED_COV)?
            .get(coverage_key(&a, action).as_slice())?
        {
            Some(v) => decode_spans(v.value()),
            None => Ok(Vec::new()),
        }
    }

    /// Within `[first, last]`, drop the coverage of `actions` that did not come from verified
    /// partitions, and its rows, so a range scanned before a mirror was configured answers
    /// incomplete until a partition replaces it rather than standing in for one.
    pub fn keep_verified_only(&self, actions: &[Action], (first, last): (u64, u64)) -> Result<()> {
        let wtx = self.store.database().begin_write()?;
        let mut changed = false;
        // Produced blocks are derived from mined blocks: where unverified mined-block coverage goes,
        // the produced-block coverage built on it goes too.
        let mut unmined: Vec<(Address, Vec<(u64, u64)>)> = Vec::new();
        {
            let mut c = wtx.open_table(COVERAGE)?;
            let v = wtx.open_table(VERIFIED_COV)?;
            let entries: Vec<(Vec<u8>, Vec<u8>)> = c
                .iter()?
                .map(|e| e.map(|(k, v)| (k.value().to_vec(), v.value().to_vec())))
                .collect::<std::result::Result<_, _>>()?;
            for (key, spans) in entries {
                let Some(&action) = actions.iter().find(|a| a.code() == key[20]) else {
                    continue;
                };
                let spans = decode_spans(&spans)?;
                let verified = match v.get(key.as_slice())? {
                    Some(s) => decode_spans(s.value())?,
                    None => Vec::new(),
                };
                let mut kept = subtract(&spans, (first, last));
                kept.extend(intersect(&spans, &intersect(&verified, &[(first, last)])));
                let kept = merge(kept);
                if kept == spans {
                    continue;
                }
                changed = true;
                let a: Address = key[..20].try_into().expect("coverage keys are 21 bytes");
                let removed = subtract_all(&intersect(&spans, &[(first, last)]), &verified);
                if action == Action::MinedBlocks && !removed.is_empty() {
                    unmined.push((a, removed.clone()));
                }
                let mut rows = wtx.open_table(action.table())?;
                for (f, t) in removed {
                    drain(
                        &mut rows,
                        &row_key(&a, f, 0, 0),
                        &row_key(&a, t, u64::MAX, u64::MAX),
                    )?;
                }
                if kept.is_empty() {
                    c.remove(key.as_slice())?;
                } else {
                    c.insert(key.as_slice(), encode_spans(&kept).as_slice())?;
                }
            }
            let mut produced = wtx.open_table(Action::ProducedBlocks.table())?;
            for (a, removed) in unmined {
                let key = coverage_key(&a, Action::ProducedBlocks);
                let Some(spans) = c.get(key.as_slice())?.map(|s| s.value().to_vec()) else {
                    continue;
                };
                let mut kept = decode_spans(&spans)?;
                for &(f, t) in &removed {
                    kept = subtract(&kept, (f, t));
                    drain(
                        &mut produced,
                        &row_key(&a, f, 0, 0),
                        &row_key(&a, t, u64::MAX, u64::MAX),
                    )?;
                }
                changed = true;
                if kept.is_empty() {
                    c.remove(key.as_slice())?;
                } else {
                    c.insert(key.as_slice(), encode_spans(&merge(kept)).as_slice())?;
                }
            }
            if changed {
                bump_generation(&wtx)?;
            }
        }
        wtx.commit()?;
        Ok(())
    }

    /// Every stored row of (address, action) in `[from, to]`, in order.
    pub fn rows_in(
        &self,
        action: Action,
        address: &str,
        (from, to): (u64, u64),
    ) -> Result<Vec<Map<String, Value>>> {
        let a = parse_address(address)?;
        let rtx = self.store.database().begin_read()?;
        let t = rtx.open_table(action.table())?;
        let lo = row_key(&a, from, 0, 0);
        let hi = row_key(&a, to, u64::MAX, u64::MAX);
        t.range(lo.as_slice()..=hi.as_slice())?
            .map(|e| decode_record(e?.1.value()))
            .collect()
    }

    /// What the relays said about `block`, if it was asked before.
    pub fn relay_record(&self, block: u64) -> Result<Option<Map<String, Value>>> {
        let rtx = self.store.database().begin_read()?;
        match rtx.open_table(RELAY_CACHE)?.get(block)? {
            Some(v) => Ok(Some(decode_record(v.value())?)),
            None => Ok(None),
        }
    }

    /// Keep what the relays said about `block`, once the nest serves through it: a relay may still
    /// learn of a block near the tip.
    pub fn cache_relay_record(&self, block: u64, record: &Map<String, Value>) -> Result<()> {
        let wtx = self.store.database().begin_write()?;
        {
            let head = wtx.open_table(META)?.get(HEAD)?.map(|v| v.value());
            if head.is_some_and(|h| block <= h) {
                let mut buf = Vec::new();
                encode_record(record, &mut buf)?;
                wtx.open_table(RELAY_CACHE)?.insert(block, buf.as_slice())?;
            }
        }
        wtx.commit()?;
        Ok(())
    }

    pub fn coverage(&self, action: Action, address: &str) -> Result<Vec<(u64, u64)>> {
        let a = parse_address(address)?;
        let rtx = self.store.database().begin_read()?;
        let t = rtx.open_table(COVERAGE)?;
        let mut spans = match t.get(coverage_key(&a, action).as_slice())? {
            Some(v) => decode_spans(v.value())?,
            None => Vec::new(),
        };
        spans.extend(swept_for(
            &rtx.open_table(SWEEP)?,
            &rtx.open_table(JOINED)?,
            &a,
            action,
        )?);
        Ok(merge(spans))
    }

    /// The last feed sequence number the webhook endpoint accepted; 0 before any.
    pub fn deposits_delivered(&self) -> Result<u64> {
        let rtx = self.store.database().begin_read()?;
        Ok(rtx
            .open_table(META)?
            .get(DEPOSIT_DELIVERED)?
            .map_or(0, |v| v.value()))
    }

    pub fn set_deposits_delivered(&self, seq: u64) -> Result<()> {
        let wtx = self.store.database().begin_write()?;
        wtx.open_table(META)?.insert(DEPOSIT_DELIVERED, seq)?;
        wtx.commit()?;
        Ok(())
    }

    /// Up to `limit` feed entries after sequence number `after`, in order (RFC-0064 S3).
    pub fn deposits_after(&self, after: u64, limit: usize) -> Result<Vec<Value>> {
        let rtx = self.store.database().begin_read()?;
        rtx.open_table(DEPOSITS)?
            .range(after.saturating_add(1)..)?
            .take(limit)
            .map(|e| Ok(serde_json::from_slice(e?.1.value())?))
            .collect()
    }

    /// [`Self::deposits_after`] and, in the same read, the last block the page is complete to: the
    /// sweep's end when the page reaches the feed's end, else none, since an entry past the page
    /// can name an earlier block than the page's last (a reorg's replacement).
    pub fn deposits_page(&self, after: u64, limit: usize) -> Result<(Vec<Value>, Option<u64>)> {
        let rtx = self.store.database().begin_read()?;
        let mut entries = rtx
            .open_table(DEPOSITS)?
            .range(after.saturating_add(1)..)?
            .take(limit.saturating_add(1))
            .map(|e| Ok(serde_json::from_slice(e?.1.value())?))
            .collect::<Result<Vec<Value>>>()?;
        if entries.len() > limit {
            entries.truncate(limit);
            return Ok((entries, None));
        }
        let through = match rtx.open_table(SWEEP)?.get(SWEPT)? {
            Some(v) => decode_spans(v.value())?.last().map(|(_, t)| *t),
            None => None,
        };
        Ok((entries, through))
    }

    /// The span the sweep has read, from the first block it read to the last.
    pub fn swept(&self) -> Result<Option<(u64, u64)>> {
        self.swept_span(SWEPT)
    }

    /// The block `address` joined the sweep at, once the sweep has read a window with it watched.
    pub fn joined(&self, address: &str) -> Result<Option<u64>> {
        let a = parse_address(address)?;
        let rtx = self.store.database().begin_read()?;
        Ok(rtx
            .open_table(JOINED)?
            .get(a.as_slice())?
            .map(|v| v.value()))
    }

    /// The span the sweep has traced for internal transfers, when it traces them.
    pub fn swept_internal(&self) -> Result<Option<(u64, u64)>> {
        self.swept_span(SWEPT_INTERNAL)
    }

    fn swept_span(&self, key: &str) -> Result<Option<(u64, u64)>> {
        let rtx = self.store.database().begin_read()?;
        let spans = match rtx.open_table(SWEEP)?.get(key)? {
            Some(v) => decode_spans(v.value())?,
            None => Vec::new(),
        };
        Ok(spans.last().map(|(_, t)| (spans[0].0, *t)))
    }

    /// Record one swept window (RFC-0064): the rows found for each matched address, the sweep's span
    /// extended over `[from, to]`, and `from` as the joined block of every address in `swept` that
    /// had none, in one transaction. No per-address coverage is written: an address's sweep
    /// coverage is the shared span from its joined block on. The span must continue the last one.
    pub fn record_sweep<S: AsRef<str>>(
        &self,
        found: &std::collections::BTreeMap<String, crate::address_discovery::Found>,
        swept: impl IntoIterator<Item = S>,
        (from, to): (u64, u64),
        generation: u64,
        anchored: Option<(u64, [u8; 32])>,
        traced: bool,
    ) -> Result<()> {
        if to < from {
            bail!("sweep interval [{from}, {to}] is empty");
        }
        let wtx = self.store.database().begin_write()?;
        {
            let now = wtx
                .open_table(META)?
                .get(GENERATION)?
                .map_or(0, |v| v.value());
            if now != generation {
                bail!("address history changed during the sweep (generation {generation} -> {now}); sweep again");
            }
            let mut sweep = wtx.open_table(SWEEP)?;
            let mut spans = match sweep.get(SWEPT)? {
                Some(v) => decode_spans(v.value())?,
                None => Vec::new(),
            };
            if let Some((_, last)) = spans.last() {
                if from != last + 1 {
                    bail!("a sweep of [{from}, {to}] does not continue the swept span ending at {last}");
                }
            }
            let watched = wtx.open_table(WATCHED)?;
            let mut joined = wtx.open_table(JOINED)?;
            // Every watched address joined already in the usual window: skip a lookup per address,
            // a million of them at a million addresses (2 s a window, measured 2026-10-14).
            use redb::ReadableTableMetadata;
            if joined.len()? < watched.len()? {
                for address in swept {
                    let a = parse_address(address.as_ref())?;
                    if watched.get(a.as_slice())?.is_some() && joined.get(a.as_slice())?.is_none() {
                        joined.insert(a.as_slice(), from)?;
                    }
                }
            }
            for (address, f) in found {
                let a = parse_address(address)?;
                // An address unwatched while the window was read is passed over, not the window.
                if watched.get(a.as_slice())?.is_none() {
                    continue;
                }
                for (action, rows) in [
                    (Action::TxList, &f.txlist),
                    (Action::TokenTx, &f.tokentx),
                    (Action::TokenNftTx, &f.tokennfttx),
                    (Action::Token1155Tx, &f.token1155tx),
                    (Action::TxListInternal, &f.txlistinternal),
                ] {
                    if action == Action::TxListInternal && !traced && !rows.is_empty() {
                        bail!("internal rows from a window that was not traced");
                    }
                    if let Some(r) = rows.iter().find(|r| r.block < from || r.block > to) {
                        bail!("a row at block {} lies outside [{from}, {to}]", r.block);
                    }
                    put_rows(&wtx, action, &a, rows)?;
                    for r in rows {
                        let field = |k: &str| r.record.get(k).and_then(Value::as_str);
                        // A creation's receiver is its contract; a send to oneself brings nothing.
                        let receiver = match field("to") {
                            Some("") | None => field("contractAddress"),
                            to => to,
                        };
                        let incoming = receiver.is_some_and(|t| t.eq_ignore_ascii_case(address))
                            && !field("from").is_some_and(|f| f.eq_ignore_ascii_case(address));
                        if !incoming || !arrived(action, r, &f.callcode) {
                            continue;
                        }
                        let seq = append_feed(
                            &wtx,
                            serde_json::json!({"event": "deposit", "address": address_hex(&a),
                                "action": action.etherscan(), "blockNumber": r.block,
                                "row": r.record}),
                        )?;
                        wtx.open_table(LIVE_DEPOSITS)?.insert(
                            live_key(r.block, action, &a, r.tx_index, r.position).as_slice(),
                            seq,
                        )?;
                    }
                }
            }
            if traced {
                // Its own span: a window swept before tracing was turned on claims no internals.
                let mut internal = match sweep.get(SWEPT_INTERNAL)? {
                    Some(v) => decode_spans(v.value())?,
                    None => Vec::new(),
                };
                internal.push((from, to));
                sweep.insert(SWEPT_INTERNAL, encode_spans(&merge(internal)).as_slice())?;
            }
            if let Some((block, hash)) = anchored {
                let head = wtx.open_table(META)?.get(HEAD)?.map(|v| v.value());
                if head.is_none_or(|h| block > h) {
                    anchor(&mut wtx.open_table(CANON)?, block, &hash)?;
                }
            }
            spans.push((from, to));
            sweep.insert(SWEPT, encode_spans(&merge(spans)).as_slice())?;
        }
        wtx.commit()?;
        Ok(())
    }

    /// Blocks awaiting a relay re-ask: (block, first asked, last asked), in unix seconds.
    pub fn mev_pending(&self) -> Result<Vec<(u64, u64, u64)>> {
        let rtx = self.store.database().begin_read()?;
        rtx.open_table(MEV_PENDING)?
            .iter()?
            .map(|e| {
                let (k, v) = e?;
                let (first, last) = decode_asked(v.value())?;
                Ok((k.value(), first, last))
            })
            .collect()
    }

    /// Note that the relays were asked about `block` at `now` and named no delivery for it; the
    /// first such ask is kept, so the re-ask schedule runs from it.
    pub fn mev_absent(&self, block: u64, now: u64) -> Result<()> {
        let wtx = self.store.database().begin_write()?;
        {
            let mut t = wtx.open_table(MEV_PENDING)?;
            let first = match t.get(block)? {
                Some(v) => decode_asked(v.value())?.0,
                None => now,
            };
            t.insert(block, encode_asked(first, now).as_slice())?;
        }
        wtx.commit()?;
        Ok(())
    }

    /// [`AddressHistory::record`] for produced blocks, with the blocks the relays named no delivery
    /// for put on the re-ask schedule in the same transaction.
    pub fn record_produced(
        &self,
        address: &str,
        rows: &[Row],
        (from, to): (u64, u64),
        generation: u64,
        unrecorded: &[u64],
        now: u64,
    ) -> Result<()> {
        if to < from {
            bail!("coverage interval [{from}, {to}] is empty");
        }
        let a = parse_address(address)?;
        let wtx = self.store.database().begin_write()?;
        {
            if wtx.open_table(WATCHED)?.get(a.as_slice())?.is_none() {
                bail!("{} is not watched", address_hex(&a));
            }
            let current = wtx
                .open_table(META)?
                .get(GENERATION)?
                .map_or(0, |v| v.value());
            if current != generation {
                bail!("address history changed during the fetch (generation {generation} -> {current}); refetch");
            }
            if let Some(r) = rows.iter().find(|r| r.block < from || r.block > to) {
                bail!("a row at block {} lies outside [{from}, {to}]", r.block);
            }
            drain(
                &mut wtx.open_table(Action::ProducedBlocks.table())?,
                &row_key(&a, from, 0, 0),
                &row_key(&a, to, u64::MAX, u64::MAX),
            )?;
            put_rows(&wtx, Action::ProducedBlocks, &a, rows)?;
            cover(&wtx, Action::ProducedBlocks, &a, from, to)?;
            let mut p = wtx.open_table(MEV_PENDING)?;
            for &b in unrecorded {
                let first = match p.get(b)? {
                    Some(v) => decode_asked(v.value())?.0,
                    None => now,
                };
                p.insert(b, encode_asked(first, now).as_slice())?;
            }
        }
        wtx.commit()?;
        Ok(())
    }

    /// A re-ask's outcome, in one transaction: the relays' answer cached (the block is finalized),
    /// each address's changed rows at the block replaced, and the block kept on the schedule or taken
    /// off it. Refused if the generation moved since the re-ask began.
    pub fn apply_reask(
        &self,
        block: u64,
        record: &Map<String, Value>,
        changes: &[(String, Vec<Row>)],
        keep_asking: bool,
        now: u64,
        generation: u64,
    ) -> Result<()> {
        let wtx = self.store.database().begin_write()?;
        {
            let current = wtx
                .open_table(META)?
                .get(GENERATION)?
                .map_or(0, |v| v.value());
            if current != generation {
                bail!("address history changed during the re-ask (generation {generation} -> {current}); ask again");
            }
            let mut buf = Vec::new();
            encode_record(record, &mut buf)?;
            wtx.open_table(RELAY_CACHE)?.insert(block, buf.as_slice())?;
            for (address, rows) in changes {
                let a = parse_address(address)?;
                if let Some(r) = rows.iter().find(|r| r.block != block) {
                    bail!("a row at block {} is not at block {block}", r.block);
                }
                drain(
                    &mut wtx.open_table(Action::ProducedBlocks.table())?,
                    &row_key(&a, block, 0, 0),
                    &row_key(&a, block, u64::MAX, u64::MAX),
                )?;
                put_rows(&wtx, Action::ProducedBlocks, &a, rows)?;
            }
            if !changes.is_empty() {
                bump_generation(&wtx)?;
            }
            let mut p = wtx.open_table(MEV_PENDING)?;
            if keep_asking {
                let first = match p.get(block)? {
                    Some(v) => decode_asked(v.value())?.0,
                    None => now,
                };
                p.insert(block, encode_asked(first, now).as_slice())?;
            } else {
                p.remove(block)?;
            }
        }
        wtx.commit()?;
        Ok(())
    }

    pub fn mev_settled(&self, block: u64) -> Result<()> {
        let wtx = self.store.database().begin_write()?;
        wtx.open_table(MEV_PENDING)?.remove(block)?;
        wtx.commit()?;
        Ok(())
    }

    /// Replace (address, action)'s rows at `block` with `rows`, in a span already covered: late relay
    /// evidence upgrading a row, or inserting one that had none. The generation moves, so a client
    /// paging across it starts again rather than reading half of each.
    pub fn replace_block_rows(
        &self,
        action: Action,
        address: &str,
        block: u64,
        rows: &[Row],
    ) -> Result<()> {
        let a = parse_address(address)?;
        if let Some(r) = rows.iter().find(|r| r.block != block) {
            bail!("a row at block {} is not at block {block}", r.block);
        }
        let wtx = self.store.database().begin_write()?;
        {
            if wtx.open_table(WATCHED)?.get(a.as_slice())?.is_none() {
                bail!("{} is not watched", address_hex(&a));
            }
            let covered = match wtx
                .open_table(COVERAGE)?
                .get(coverage_key(&a, action).as_slice())?
            {
                Some(v) => decode_spans(v.value())?,
                None => Vec::new(),
            };
            if !covered.iter().any(|(f, t)| *f <= block && block <= *t) {
                bail!("{} block {block} is not covered", action.etherscan());
            }
            drain(
                &mut wtx.open_table(action.table())?,
                &row_key(&a, block, 0, 0),
                &row_key(&a, block, u64::MAX, u64::MAX),
            )?;
            put_rows(&wtx, action, &a, rows)?;
            bump_generation(&wtx)?;
        }
        wtx.commit()?;
        Ok(())
    }

    /// Set the finalized head outright, lower included; the cursor uses
    /// [`AddressHistory::finalize`].
    pub fn set_head(&self, block: u64) -> Result<()> {
        let wtx = self.store.database().begin_write()?;
        wtx.open_table(META)?.insert(HEAD, block)?;
        wtx.commit()?;
        Ok(())
    }

    pub fn head(&self) -> Result<Option<u64>> {
        let rtx = self.store.database().begin_read()?;
        Ok(rtx.open_table(META)?.get(HEAD)?.map(|v| v.value()))
    }

    /// A reorg back to `block`: rows above it are dropped, and so is every claim of coverage above
    /// it, so the range reads incomplete until the cursor refetches it.
    pub fn invalidate_above(&self, block: u64) -> Result<()> {
        let wtx = self.store.database().begin_write()?;
        {
            // What the index lists above the fork, for a reorg at or above the finalized head, the
            // only kind the cursor takes back. Anything lower walks every holder and cache.
            let reorged: Option<Vec<Vec<u8>>> = {
                let (complete, head) = {
                    let m = wtx.open_table(META)?;
                    let complete = m.get(REORGABLE_COMPLETE)?.is_some();
                    let head = m.get(HEAD)?.map(|v| v.value());
                    (complete, head)
                };
                match block.checked_add(1) {
                    Some(above) if complete && head.is_none_or(|h| block >= h) => Some(
                        wtx.open_table(REORGABLE)?
                            .range(reorgable_key(above, 0, &[]).as_slice()..)?
                            .map(|e| e.map(|(k, _)| k.value().to_vec()))
                            .collect::<std::result::Result<_, _>>()?,
                    ),
                    _ => None,
                }
            };
            let tagged = |keys: &[Vec<u8>], tag: u8| -> Vec<Vec<u8>> {
                keys.iter()
                    .filter(|k| k[8] == tag)
                    .map(|k| k[9..].to_vec())
                    .collect()
            };
            let mut c = wtx.open_table(COVERAGE)?;
            let entries: Vec<(Vec<u8>, Vec<u8>)> = c
                .iter()?
                .map(|e| e.map(|(k, v)| (k.value().to_vec(), v.value().to_vec())))
                .collect::<std::result::Result<_, _>>()?;
            if let Some(above) = block.checked_add(1) {
                let drained: Vec<(Action, Address)> = match &reorged {
                    Some(keys) => keys
                        .iter()
                        .filter_map(|k| {
                            let action = Action::ALL.into_iter().find(|a| a.code() == k[8])?;
                            Some((action, k[9..].try_into().expect("row ids are 20 bytes")))
                        })
                        .collect(),
                    None => {
                        // Rows can outlive their coverage (a fetch stored them, then a reorg cut
                        // the span), so every watched address and every covered one is drained.
                        let mut holders: std::collections::BTreeSet<Address> = wtx
                            .open_table(WATCHED)?
                            .iter()?
                            .map(|e| {
                                e.map(|(k, _)| {
                                    k.value().try_into().expect("watched keys are 20 bytes")
                                })
                            })
                            .collect::<std::result::Result<_, _>>()?;
                        holders.extend(entries.iter().map(|(k, _)| -> Address {
                            k[..20].try_into().expect("coverage keys are 21 bytes")
                        }));
                        holders
                            .into_iter()
                            .flat_map(|a| Action::ALL.map(|action| (action, a)))
                            .collect()
                    }
                };
                for (action, a) in drained {
                    drain(
                        &mut wtx.open_table(action.table())?,
                        &row_key(&a, above, 0, 0),
                        &row_key(&a, u64::MAX, u64::MAX, u64::MAX),
                    )?;
                }
            }
            for (key, spans) in entries {
                let kept: Vec<(u64, u64)> = decode_spans(&spans)?
                    .into_iter()
                    .filter(|(from, _)| *from <= block)
                    .map(|(from, to)| (from, to.min(block)))
                    .collect();
                if kept.is_empty() {
                    c.remove(key.as_slice())?;
                } else {
                    c.insert(key.as_slice(), encode_spans(&kept).as_slice())?;
                }
            }
            {
                // Every deposit the reorg takes back is retracted in the feed, once: a consumer
                // that credited it learns the chain took it back.
                let above: Vec<(Vec<u8>, u64)> = wtx
                    .open_table(LIVE_DEPOSITS)?
                    .range(block.saturating_add(1).to_be_bytes().as_slice()..)?
                    .map(|e| e.map(|(k, v)| (k.value().to_vec(), v.value())))
                    .collect::<std::result::Result<_, _>>()?;
                for (key, seq) in above {
                    wtx.open_table(LIVE_DEPOSITS)?.remove(key.as_slice())?;
                    let original: Value = match wtx.open_table(DEPOSITS)?.get(seq)? {
                        Some(v) => serde_json::from_slice(v.value())?,
                        None => bail!("live deposit {seq} is missing from the feed"),
                    };
                    append_feed(
                        &wtx,
                        serde_json::json!({"event": "retraction", "retracts": seq,
                            "address": original["address"], "action": original["action"],
                            "blockNumber": original["blockNumber"], "row": original["row"]}),
                    )?;
                }
            }
            {
                let mut sweep = wtx.open_table(SWEEP)?;
                for key in [SWEPT, SWEPT_INTERNAL] {
                    let spans = match sweep.get(key)? {
                        Some(v) => decode_spans(v.value())?,
                        None => Vec::new(),
                    };
                    let kept: Vec<(u64, u64)> = spans
                        .into_iter()
                        .filter(|(from, _)| *from <= block)
                        .map(|(from, to)| (from, to.min(block)))
                        .collect();
                    if kept.is_empty() {
                        sweep.remove(key)?;
                    } else {
                        sweep.insert(key, encode_spans(&kept).as_slice())?;
                    }
                }
            }
            {
                let mut m = wtx.open_table(META)?;
                let head = m.get(HEAD)?.map(|v| v.value());
                if head.is_some_and(|h| h > block) {
                    m.insert(HEAD, block)?;
                }
            }
            {
                let mut relays = wtx.open_table(RELAY_CACHE)?;
                let above: Vec<u64> = relays
                    .range(block.saturating_add(1)..)?
                    .map(|e| e.map(|(k, _)| k.value()))
                    .collect::<std::result::Result<_, _>>()?;
                for b in above {
                    relays.remove(b)?;
                }
            }
            // A trace of a block the reorg replaced describes a transaction that may no longer exist
            // there, so a txhash lookup must trace it again; and a transaction re-included elsewhere
            // keeps its hash, so a cached copy would bring back its old block, index and receipt.
            for (table, tag) in [(TX_INTERNAL, TX_INTERNAL_TAG), (TX_CACHE, TX_CACHE_TAG)] {
                let mut t = wtx.open_table(table)?;
                let stale = match &reorged {
                    // An index key outlives a re-cache at a lower block, so the record decides.
                    Some(keys) => {
                        let mut stale = Vec::new();
                        for k in tagged(keys, tag) {
                            let above = match t.get(k.as_slice())? {
                                Some(v) => cached_block(v.value())? > block,
                                None => false,
                            };
                            if above {
                                stale.push(k);
                            }
                        }
                        stale
                    }
                    None => {
                        let mut stale = Vec::new();
                        for e in t.iter()? {
                            let (k, v) = e?;
                            if cached_block(v.value())? > block {
                                stale.push(k.value().to_vec());
                            }
                        }
                        stale
                    }
                };
                for k in stale {
                    t.remove(k.as_slice())?;
                }
            }
            {
                let mut index = wtx.open_table(REORGABLE)?;
                let above: Vec<Vec<u8>> = index
                    .range(reorgable_key(block.saturating_add(1), 0, &[]).as_slice()..)?
                    .map(|e| e.map(|(k, _)| k.value().to_vec()))
                    .collect::<std::result::Result<_, _>>()?;
                for k in above {
                    index.remove(k.as_slice())?;
                }
            }
            {
                let mut t = wtx.open_table(BLOCK_TS)?;
                let above: Vec<u64> = t
                    .range(block.saturating_add(1)..)?
                    .map(|e| e.map(|(k, _)| k.value()))
                    .collect::<std::result::Result<_, _>>()?;
                for b in above {
                    t.remove(b)?;
                }
            }
            {
                let mut t = wtx.open_table(BLOCK_HASH)?;
                let above: Vec<u64> = t
                    .range(block.saturating_add(1)..)?
                    .map(|e| e.map(|(k, _)| k.value()))
                    .collect::<std::result::Result<_, _>>()?;
                for b in above {
                    t.remove(b)?;
                }
            }
            {
                let mut t = wtx.open_table(BODY_SUMMARY)?;
                let above: Vec<u64> = t
                    .range(block.saturating_add(1)..)?
                    .map(|e| e.map(|(k, _)| k.value()))
                    .collect::<std::result::Result<_, _>>()?;
                for b in above {
                    t.remove(b)?;
                }
                let mut h = wtx.open_table(HEADERS)?;
                let kept = match h.get(SUMMARIZED)? {
                    Some(v) => Some(subtract(
                        &decode_spans(v.value())?,
                        (block.saturating_add(1), u64::MAX),
                    )),
                    None => None,
                };
                if let Some(kept) = kept {
                    h.insert(SUMMARIZED, encode_spans(&kept).as_slice())?;
                }
            }
            for table in [CANON, MEV_PENDING] {
                let mut t = wtx.open_table(table)?;
                let above: Vec<u64> = t
                    .range(block.saturating_add(1)..)?
                    .map(|e| e.map(|(k, _)| k.value()))
                    .collect::<std::result::Result<_, _>>()?;
                for b in above {
                    t.remove(b)?;
                }
            }
            {
                let mut m = wtx.open_table(META)?;
                if m.get(TIP)?.is_some_and(|v| v.value() > block) {
                    m.insert(TIP, block)?;
                }
            }
            {
                let mut v = wtx.open_table(VERIFIED_COV)?;
                let entries: Vec<(Vec<u8>, Vec<u8>)> = v
                    .iter()?
                    .map(|e| e.map(|(k, v)| (k.value().to_vec(), v.value().to_vec())))
                    .collect::<std::result::Result<_, _>>()?;
                for (key, spans) in entries {
                    let kept =
                        subtract(&decode_spans(&spans)?, (block.saturating_add(1), u64::MAX));
                    if kept.is_empty() {
                        v.remove(key.as_slice())?;
                    } else {
                        v.insert(key.as_slice(), encode_spans(&kept).as_slice())?;
                    }
                }
            }
            {
                let mut h = wtx.open_table(HEADERS)?;
                let kept: Option<Vec<(u64, u64)>> = match h.get(VERIFIED)? {
                    Some(v) => Some(
                        decode_spans(v.value())?
                            .into_iter()
                            .filter(|(from, _)| *from <= block)
                            .map(|(from, to)| (from, to.min(block)))
                            .collect(),
                    ),
                    None => None,
                };
                if let Some(kept) = kept {
                    h.insert(VERIFIED, encode_spans(&kept).as_slice())?;
                }
            }
            bump_generation(&wtx)?;
        }
        {
            // A marker above the fork came from code a reorg may have taken away.
            let mut t = wtx.open_table(UNPROVABLE)?;
            let marks: Vec<(Vec<u8>, u64)> = t
                .iter()?
                .map(|e| e.map(|(k, v)| (k.value().to_vec(), v.value())))
                .collect::<std::result::Result<_, _>>()?;
            for (k, from) in marks {
                if from > block {
                    t.remove(k.as_slice())?;
                }
            }
        }
        wtx.commit()?;
        Ok(())
    }

    pub fn page(&self, action: Action, req: &PageRequest) -> Result<Answer> {
        Ok(self.page_at(action, req)?.0)
    }

    /// A page, and the generation it was read at, from one read transaction.
    pub fn page_at(&self, action: Action, req: &PageRequest) -> Result<(Answer, u64)> {
        let a = parse_address(&req.address)?;
        let rtx = self.store.database().begin_read()?;
        let meta = rtx.open_table(META)?;
        let generation = meta.get(GENERATION)?.map_or(0, |v| v.value());
        let answer = |a: Answer| Ok((a, generation));
        if req.generation.is_some_and(|g| g != generation) {
            return answer(Answer::Incomplete(
                "address history changed between pages; start again from page 1".into(),
            ));
        }
        let start = req.start_block.unwrap_or(0);
        if let Some(from) = rtx
            .open_table(UNPROVABLE)?
            .get(a.as_slice())?
            .map(|v| v.value())
        {
            if req.end_block.is_none_or(|e| e >= from) {
                return answer(Answer::Unsupported(format!(
                    "{} had code from block {from}, and this chain's history is proved by its balance, \
                     which another account's code can spend; ask another source for blocks from {from}",
                    req.address
                )));
            }
        }
        let mut spans = match rtx
            .open_table(COVERAGE)?
            .get(coverage_key(&a, action).as_slice())?
        {
            Some(v) => decode_spans(v.value())?,
            None => Vec::new(),
        };
        spans.extend(swept_for(
            &rtx.open_table(SWEEP)?,
            &rtx.open_table(JOINED)?,
            &a,
            action,
        )?);
        let spans = merge(spans);
        if req.end_block.is_some_and(|e| e < start) {
            return answer(Answer::Rows(Vec::new()));
        }
        // Without an end, page 1 reads to the end of what is covered, which moves as coverage grows;
        // a later page could then repeat or skip rows.
        if req.page > 1 && req.end_block.is_none() {
            return answer(Answer::Incomplete(
                "a page past the first needs an endblock, so every page reads the same blocks"
                    .into(),
            ));
        }
        let Some(&(_, covered_to)) = spans.iter().find(|(f, t)| *f <= start && start <= *t) else {
            return answer(Answer::Incomplete(format!(
                "{} for {} is not indexed at block {start}",
                action.etherscan(),
                address_hex(&a)
            )));
        };
        // An explicit end is answered whole or refused: shortening it to what is covered would let a
        // client record history it never received. No end means everything covered.
        let end = req.end_block.unwrap_or(covered_to);
        if end > covered_to {
            return answer(Answer::Incomplete(format!(
                "{} for {} is indexed through block {covered_to}, not {}",
                action.etherscan(),
                address_hex(&a),
                req.end_block.unwrap_or(end)
            )));
        }
        let t = rtx.open_table(action.table())?;
        let lo = row_key(&a, start, 0, 0);
        let hi = row_key(&a, end, u64::MAX, u64::MAX);
        let range = t.range(lo.as_slice()..=hi.as_slice())?;
        let skip = usize::try_from(req.page.saturating_sub(1).saturating_mul(req.offset))
            .unwrap_or(usize::MAX);
        let take = usize::try_from(req.offset).unwrap_or(usize::MAX);
        let read: Box<dyn Iterator<Item = _>> = match req.sort {
            Sort::Asc => Box::new(range),
            Sort::Desc => Box::new(range.rev()),
        };
        #[cfg(test)]
        let read = read.inspect(|_| ROWS_READ.with(|n| n.set(n.get() + 1)));
        #[cfg(test)]
        ROWS_READ.with(|n| n.set(0));
        let rows = read
            .skip(skip)
            .take(take)
            .map(|e| {
                let (_, v) = e?;
                decode_record(v.value()).map(Value::Object)
            })
            .collect::<Result<Vec<_>>>()?;
        answer(Answer::Rows(rows))
    }

    /// Raw writes for tests that need a store in a state `record` would refuse to produce.
    #[cfg(test)]
    fn insert(&self, action: Action, address: &str, rows: &[Row]) -> Result<()> {
        let wtx = self.store.database().begin_write()?;
        put_rows(&wtx, action, &parse_address(address)?, rows)?;
        wtx.commit()?;
        Ok(())
    }

    #[cfg(test)]
    fn mark_covered(&self, action: Action, address: &str, from: u64, to: u64) -> Result<()> {
        let wtx = self.store.database().begin_write()?;
        cover(&wtx, action, &parse_address(address)?, from, to)?;
        wtx.commit()?;
        Ok(())
    }
}

fn put_rows(wtx: &redb::WriteTransaction, action: Action, a: &Address, rows: &[Row]) -> Result<()> {
    let mut t = wtx.open_table(action.table())?;
    let head = wtx.open_table(META)?.get(HEAD)?.map(|v| v.value());
    let mut index = wtx.open_table(REORGABLE)?;
    let mut buf = Vec::new();
    for r in rows {
        let claimed = r.record.get("blockNumber").and_then(Value::as_str);
        if claimed != Some(r.block.to_string().as_str()) {
            bail!(
                "{} row at block {} carries blockNumber {claimed:?}",
                action.etherscan(),
                r.block
            );
        }
        buf.clear();
        encode_record(&r.record, &mut buf)?;
        t.insert(
            row_key(a, r.block, r.tx_index, r.position).as_slice(),
            buf.as_slice(),
        )?;
        if head.is_none_or(|h| r.block > h) {
            index.insert(reorgable_key(r.block, action.code(), a).as_slice(), 1)?;
        }
    }
    Ok(())
}

/// The actions every sweep fills. Internal transactions are filled only where the sweep traced,
/// under their own span (RFC-0064 S2).
pub const SWEPT_ACTIONS: [Action; 4] = [
    Action::TxList,
    Action::TokenTx,
    Action::TokenNftTx,
    Action::Token1155Tx,
];

fn live_key(block: u64, action: Action, a: &Address, tx_index: u64, position: u64) -> Vec<u8> {
    let mut k = Vec::with_capacity(45);
    k.extend_from_slice(&block.to_be_bytes());
    k.push(action.code());
    k.extend_from_slice(a.as_slice());
    k.extend_from_slice(&tx_index.to_be_bytes());
    k.extend_from_slice(&position.to_be_bytes());
    k
}

/// Whether an incoming row moved value to the address. Etherscan lists failed transactions, zero
/// transfers and `callcode` frames too; the feed is for money that arrived.
fn arrived(
    action: Action,
    r: &Row,
    callcode: &std::collections::BTreeSet<(u64, u64, u64)>,
) -> bool {
    let field = |k: &str| r.record.get(k).and_then(Value::as_str);
    if field("isError") == Some("1") || field("txreceipt_status") == Some("0") {
        return false;
    }
    if field("value") == Some("0") || field("tokenValue") == Some("0") {
        return false;
    }
    !(action == Action::TxListInternal && callcode.contains(&(r.block, r.tx_index, r.position)))
}

/// Append `entry` to the deposits feed under the next sequence number, which it returns.
fn append_feed(wtx: &redb::WriteTransaction, mut entry: Value) -> Result<u64> {
    let mut meta = wtx.open_table(META)?;
    let seq = meta.get(DEPOSIT_NEXT)?.map_or(1, |v| v.value());
    entry["seq"] = Value::from(seq);
    wtx.open_table(DEPOSITS)?
        .insert(seq, serde_json::to_vec(&entry)?.as_slice())?;
    meta.insert(DEPOSIT_NEXT, seq + 1)?;
    Ok(seq)
}

/// `a`'s coverage of `action` from the sweep: the shared span from the block `a` joined it on.
fn swept_for(
    sweep: &impl ReadableTable<&'static str, &'static [u8]>,
    joined: &impl ReadableTable<&'static [u8], u64>,
    a: &Address,
    action: Action,
) -> Result<Vec<(u64, u64)>> {
    let key = if SWEPT_ACTIONS.contains(&action) {
        SWEPT
    } else if action == Action::TxListInternal {
        SWEPT_INTERNAL
    } else {
        return Ok(Vec::new());
    };
    let Some(j) = joined.get(a.as_slice())?.map(|v| v.value()) else {
        return Ok(Vec::new());
    };
    let spans = match sweep.get(key)? {
        Some(v) => decode_spans(v.value())?,
        None => Vec::new(),
    };
    Ok(spans
        .into_iter()
        .filter(|(_, t)| *t >= j)
        .map(|(f, t)| (f.max(j), t))
        .collect())
}

fn cover(
    wtx: &redb::WriteTransaction,
    action: Action,
    a: &Address,
    from: u64,
    to: u64,
) -> Result<()> {
    let mut t = wtx.open_table(COVERAGE)?;
    let key = coverage_key(a, action);
    let mut spans = match t.get(key.as_slice())? {
        Some(v) => decode_spans(v.value())?,
        None => Vec::new(),
    };
    spans.push((from, to));
    t.insert(key.as_slice(), encode_spans(&merge(spans)).as_slice())?;
    Ok(())
}

/// The produced-block contract a store's rows were read under. 5.1 stored no relay delivery as
/// `none` and kept no re-ask schedule, and skipped payout-contract payments, so its rows are dropped
/// and read again rather than carried into a contract they were not read under.
const PRODUCED_CONTRACT: &str = "produced_contract";
const PRODUCED_CONTRACT_NOW: u64 = 2;

fn reset_produced_blocks(wtx: &redb::WriteTransaction) -> Result<()> {
    use redb::ReadableTableMetadata;
    let read_under = wtx
        .open_table(META)?
        .get(PRODUCED_CONTRACT)?
        .map(|v| v.value());
    if read_under == Some(PRODUCED_CONTRACT_NOW) {
        return Ok(());
    }
    let mut rows = wtx.open_table(Action::ProducedBlocks.table())?;
    let had_rows = !rows.is_empty()?;
    rows.retain(|_, _| false)?;
    let mut c = wtx.open_table(COVERAGE)?;
    let stale: Vec<Vec<u8>> = c
        .iter()?
        .filter_map(|e| e.ok())
        .map(|(k, _)| k.value().to_vec())
        .filter(|k| k[20] == Action::ProducedBlocks.code())
        .collect();
    for k in &stale {
        c.remove(k.as_slice())?;
    }
    if had_rows || !stale.is_empty() {
        bump_generation(wtx)?;
    }
    wtx.open_table(META)?
        .insert(PRODUCED_CONTRACT, PRODUCED_CONTRACT_NOW)?;
    Ok(())
}

fn encode_asked(first: u64, last: u64) -> [u8; 16] {
    let mut out = [0u8; 16];
    out[..8].copy_from_slice(&first.to_be_bytes());
    out[8..].copy_from_slice(&last.to_be_bytes());
    out
}

fn decode_asked(raw: &[u8]) -> Result<(u64, u64)> {
    let raw: [u8; 16] = raw
        .try_into()
        .map_err(|_| anyhow!("a pending relay entry is not 16 bytes"))?;
    Ok((
        u64::from_be_bytes(raw[..8].try_into().expect("8 bytes")),
        u64::from_be_bytes(raw[8..].try_into().expect("8 bytes")),
    ))
}

/// Track `hash` at `block`; a different hash already tracked there means the chain moved under the
/// fetch, which must not be recorded as if it had not.
fn anchor(c: &mut redb::Table<u64, &'static [u8]>, block: u64, hash: &[u8; 32]) -> Result<()> {
    if let Some(v) = c.get(block)? {
        if v.value() != hash.as_slice() {
            bail!("block {block} changed hash during the fetch; refetch");
        }
        return Ok(());
    }
    c.insert(block, hash.as_slice())?;
    Ok(())
}

fn bump_generation(wtx: &redb::WriteTransaction) -> Result<()> {
    let mut m = wtx.open_table(META)?;
    let next = m.get(GENERATION)?.map_or(0, |v| v.value()) + 1;
    m.insert(GENERATION, next)?;
    Ok(())
}

fn drain(t: &mut redb::Table<&'static [u8], &'static [u8]>, lo: &[u8], hi: &[u8]) -> Result<()> {
    let keys: Vec<Vec<u8>> = t
        .range(lo..=hi)?
        .map(|e| e.map(|(k, _)| k.value().to_vec()))
        .collect::<std::result::Result<_, _>>()?;
    for k in keys {
        t.remove(k.as_slice())?;
    }
    Ok(())
}

/// `spans` without the blocks of `[from, to]`.
fn subtract(spans: &[(u64, u64)], (from, to): (u64, u64)) -> Vec<(u64, u64)> {
    let mut out = Vec::new();
    for &(f, t) in spans {
        if t < from || f > to {
            out.push((f, t));
            continue;
        }
        if f < from {
            out.push((f, from - 1));
        }
        if t > to {
            out.push((to + 1, t));
        }
    }
    out
}

fn subtract_all(spans: &[(u64, u64)], others: &[(u64, u64)]) -> Vec<(u64, u64)> {
    others
        .iter()
        .fold(spans.to_vec(), |left, &other| subtract(&left, other))
}

fn intersect(a: &[(u64, u64)], b: &[(u64, u64)]) -> Vec<(u64, u64)> {
    let mut out = Vec::new();
    for &(af, at) in a {
        for &(bf, bt) in b {
            let (f, t) = (af.max(bf), at.min(bt));
            if f <= t {
                out.push((f, t));
            }
        }
    }
    merge(out)
}

fn add_span(
    t: &mut redb::Table<&'static [u8], &'static [u8]>,
    key: &[u8],
    span: (u64, u64),
) -> Result<()> {
    let mut spans = match t.get(key)? {
        Some(v) => decode_spans(v.value())?,
        None => Vec::new(),
    };
    spans.push(span);
    t.insert(key, encode_spans(&merge(spans)).as_slice())?;
    Ok(())
}

/// Sort and join intervals that overlap or touch: `[1, 5]` and `[6, 9]` cover `[1, 9]`.
fn merge(mut spans: Vec<(u64, u64)>) -> Vec<(u64, u64)> {
    spans.sort_unstable();
    let mut out: Vec<(u64, u64)> = Vec::with_capacity(spans.len());
    for (from, to) in spans {
        match out.last_mut() {
            Some(last) if from <= last.1.saturating_add(1) => last.1 = last.1.max(to),
            _ => out.push((from, to)),
        }
    }
    out
}

fn encode_spans(spans: &[(u64, u64)]) -> Vec<u8> {
    spans
        .iter()
        .flat_map(|(f, t)| f.to_be_bytes().into_iter().chain(t.to_be_bytes()))
        .collect()
}

fn decode_spans(raw: &[u8]) -> Result<Vec<(u64, u64)>> {
    if !raw.len().is_multiple_of(16) {
        bail!(
            "address-history coverage is {} bytes, not pairs of u64",
            raw.len()
        );
    }
    Ok(raw
        .chunks_exact(16)
        .map(|c| {
            (
                u64::from_be_bytes(c[..8].try_into().expect("16-byte chunk")),
                u64::from_be_bytes(c[8..].try_into().expect("16-byte chunk")),
            )
        })
        .collect())
}

/// Etherscan's record keys, by stored index. Append only: the index is on disk.
const KNOWN_KEYS: &[&str] = &[
    "blockNumber",
    "timeStamp",
    "hash",
    "nonce",
    "blockHash",
    "transactionIndex",
    "from",
    "to",
    "value",
    "gas",
    "gasPrice",
    "isError",
    "txreceipt_status",
    "input",
    "contractAddress",
    "cumulativeGasUsed",
    "gasUsed",
    "confirmations",
    "methodId",
    "functionName",
    "traceId",
    "type",
    "errCode",
    "callType",
    "logIndex",
    "tokenName",
    "tokenSymbol",
    "tokenDecimal",
    "tokenID",
    "tokenValue",
    "withdrawalIndex",
    "validatorIndex",
    "address",
    "amount",
    "blockReward",
    "timestamp",
    "L1FeesPaid",
    "authorizationList",
    "feeRecipient",
    "mevRecipient",
    "mevReward",
    "relays",
    "builderPubkey",
    "proposerPubkey",
    "paymentTx",
    "paymentValue",
    "mev",
];
const OTHER_KEY: u8 = 0xff;

const V_TEXT: u8 = 0;
const V_HEX: u8 = 1;
const V_DECIMAL: u8 = 2;
const V_NULL: u8 = 3;
const V_FALSE: u8 = 4;
const V_TRUE: u8 = 5;
/// A JSON number, kept as its literal so it prints back exactly.
const V_NUMBER: u8 = 6;
const V_ARRAY: u8 = 7;
const V_OBJECT: u8 = 8;

/// A record as rotki reads it: Etherscan's mostly hex and decimal strings, packed so a hash costs 32
/// bytes and a block number a few. Every value round-trips to the exact string it was given; anything
/// that would not (odd-length or uppercase hex, a decimal with a leading zero) is kept as text.
fn encode_record(record: &Map<String, Value>, out: &mut Vec<u8>) -> Result<()> {
    put_varint(out, record.len() as u128);
    for (k, v) in record {
        match KNOWN_KEYS.iter().position(|known| known == k) {
            Some(i) => out.push(u8::try_from(i).expect("fewer than 255 known keys")),
            None => {
                out.push(OTHER_KEY);
                put_bytes(out, k.as_bytes());
            }
        }
        encode_value(v, out);
    }
    Ok(())
}

fn encode_value(v: &Value, out: &mut Vec<u8>) {
    match v {
        Value::String(s) => encode_string(s, out),
        Value::Null => out.push(V_NULL),
        Value::Bool(false) => out.push(V_FALSE),
        Value::Bool(true) => out.push(V_TRUE),
        Value::Number(n) => {
            out.push(V_NUMBER);
            put_bytes(out, n.to_string().as_bytes());
        }
        Value::Array(items) => {
            out.push(V_ARRAY);
            put_varint(out, items.len() as u128);
            for item in items {
                encode_value(item, out);
            }
        }
        Value::Object(fields) => {
            out.push(V_OBJECT);
            put_varint(out, fields.len() as u128);
            for (k, item) in fields {
                put_bytes(out, k.as_bytes());
                encode_value(item, out);
            }
        }
    }
}

fn decode_value(raw: &mut &[u8]) -> Result<Value> {
    Ok(match take_u8(raw)? {
        V_TEXT => Value::String(String::from_utf8(take_bytes(raw)?.to_vec())?),
        V_HEX => Value::String(format!(
            "0x{}",
            alloy_primitives::hex::encode(take_bytes(raw)?)
        )),
        V_DECIMAL => Value::String(take_varint(raw)?.to_string()),
        V_NULL => Value::Null,
        V_FALSE => Value::Bool(false),
        V_TRUE => Value::Bool(true),
        V_NUMBER => Value::Number(std::str::from_utf8(take_bytes(raw)?)?.parse()?),
        V_ARRAY => {
            let n = take_varint(raw)?;
            let mut items = Vec::new();
            for _ in 0..n {
                items.push(decode_value(raw)?);
            }
            Value::Array(items)
        }
        V_OBJECT => {
            let n = take_varint(raw)?;
            let mut fields = Map::new();
            for _ in 0..n {
                let k = String::from_utf8(take_bytes(raw)?.to_vec())?;
                fields.insert(k, decode_value(raw)?);
            }
            Value::Object(fields)
        }
        t => bail!("unknown record value tag {t}"),
    })
}

fn encode_string(s: &str, out: &mut Vec<u8>) {
    if let Some(hex) = s.strip_prefix("0x") {
        let canonical = hex.len() % 2 == 0
            && hex
                .bytes()
                .all(|b| b.is_ascii_digit() || (b'a'..=b'f').contains(&b));
        if canonical {
            if let Ok(bytes) = alloy_primitives::hex::decode(hex) {
                out.push(V_HEX);
                put_bytes(out, &bytes);
                return;
            }
        }
    }
    let canonical_decimal =
        !s.is_empty() && s.bytes().all(|b| b.is_ascii_digit()) && (s == "0" || !s.starts_with('0'));
    if canonical_decimal {
        if let Ok(n) = s.parse::<u128>() {
            out.push(V_DECIMAL);
            put_varint(out, n);
            return;
        }
    }
    out.push(V_TEXT);
    put_bytes(out, s.as_bytes());
}

/// Encode `record` into `buf` and assert it decodes back unchanged.
#[cfg(test)]
pub(crate) fn tests_encode_roundtrip(record: &Map<String, Value>, buf: &mut Vec<u8>) {
    buf.clear();
    encode_record(record, buf).unwrap();
    assert_eq!(&decode_record(buf).unwrap(), record);
}

fn decode_record(mut raw: &[u8]) -> Result<Map<String, Value>> {
    let n = take_varint(&mut raw)?;
    let mut out = Map::new();
    for _ in 0..n {
        let tag = take_u8(&mut raw)?;
        let key = if tag == OTHER_KEY {
            String::from_utf8(take_bytes(&mut raw)?.to_vec())?
        } else {
            KNOWN_KEYS
                .get(usize::from(tag))
                .ok_or_else(|| anyhow!("unknown record key {tag}"))?
                .to_string()
        };
        let value = decode_value(&mut raw)?;
        out.insert(key, value);
    }
    if !raw.is_empty() {
        bail!("{} trailing bytes after a record", raw.len());
    }
    Ok(out)
}

fn put_varint(out: &mut Vec<u8>, mut n: u128) {
    loop {
        let low = (n & 0x7f) as u8;
        n >>= 7;
        if n == 0 {
            out.push(low);
            return;
        }
        out.push(low | 0x80);
    }
}

fn put_bytes(out: &mut Vec<u8>, b: &[u8]) {
    put_varint(out, b.len() as u128);
    out.extend_from_slice(b);
}

fn take_u8(raw: &mut &[u8]) -> Result<u8> {
    let (&b, rest) = raw
        .split_first()
        .ok_or_else(|| anyhow!("record ends early"))?;
    *raw = rest;
    Ok(b)
}

fn take_varint(raw: &mut &[u8]) -> Result<u128> {
    let mut n: u128 = 0;
    for shift in (0..128).step_by(7) {
        let b = take_u8(raw)?;
        // The 19th byte has room for 2 bits; anything more would be shifted out silently.
        if shift == 126 && b > 0x03 {
            bail!("varint overflows 128 bits");
        }
        n |= u128::from(b & 0x7f) << shift;
        if b & 0x80 == 0 {
            return Ok(n);
        }
    }
    bail!("varint longer than 128 bits")
}

fn take_bytes<'a>(raw: &mut &'a [u8]) -> Result<&'a [u8]> {
    let len = usize::try_from(take_varint(raw)?)?;
    if raw.len() < len {
        bail!("record ends early");
    }
    let (b, rest) = raw.split_at(len);
    *raw = rest;
    Ok(b)
}

/// The Etherscan envelope for `/api`. `generation` is ours: a client paging past page 1 sends it
/// back, and Etherscan clients that do not know it ignore an extra key.
pub fn ok(result: Value, generation: u64) -> Value {
    serde_json::json!({
        "status": "1",
        "message": "OK",
        "result": result,
        "generation": generation.to_string(),
    })
}

pub fn unsupported(why: &str) -> Value {
    serde_json::json!({ "status": "0", "message": "NOTOK", "result": format!("NUTHATCH_UNSUPPORTED: {why}") })
}

pub fn incomplete(why: &str) -> Value {
    serde_json::json!({ "status": "0", "message": "NOTOK", "result": format!("NUTHATCH_INCOMPLETE: {why}") })
}

fn error(why: impl std::fmt::Display) -> Value {
    serde_json::json!({ "status": "0", "message": "NOTOK", "result": format!("Error! {why}") })
}

/// Answer one `/api` request. `history` is `None` on a nest with no `[address_history]`.
pub fn respond(
    history: Option<&AddressHistory>,
    params: &std::collections::HashMap<String, String>,
) -> Value {
    let get = |k: &str| params.get(k).map(String::as_str);
    let module = get("module").unwrap_or("");
    let action = get("action").unwrap_or("");
    if module == "block" && action == "getblocknobytime" {
        return block_no_by_time(history, params);
    }
    if module == "account" && action == "nuthatchDeposits" {
        return deposits_feed(history, params);
    }
    let Some(act) = Action::from_etherscan(action).filter(|_| module == "account") else {
        return unsupported(&format!("{module}/{action} is not served by this nest"));
    };
    let Some(history) = history else {
        return unsupported("this nest has no [address_history]");
    };
    if let Some(c) = get("chainid") {
        if c.parse::<u64>().ok() != Some(history.chain_id) {
            return unsupported(&format!(
                "chainid {c} is not this nest's chain {}",
                history.chain_id
            ));
        }
    }
    if let Some(hash) = get("txhash") {
        return by_txhash(history, act, hash, params);
    }
    if act == Action::MinedBlocks && get("blocktype").is_some_and(|t| t != "blocks") {
        return unsupported("getminedblocks is served for blocktype=blocks only");
    }
    match parse_request(params) {
        Err(e) => error(e),
        Ok(req) if req.start_block.unwrap_or(0) < traced_from(history.chain_id) => {
            unsupported(&format!(
                "chain {} has no trustworthy traces before block {}",
                history.chain_id,
                traced_from(history.chain_id)
            ))
        }
        Ok(req) => match history.page_at(act, &req) {
            Ok((Answer::Rows(rows), generation)) => ok(Value::Array(rows), generation),
            Ok((Answer::Incomplete(why), _)) => incomplete(&why),
            Ok((Answer::Unsupported(why), _)) => unsupported(&why),
            Err(e) => error(format!("{e:#}")),
        },
    }
}

/// Whether every block's timestamp is strictly greater than its parent's, which is what lets
/// `getblocknobytime` prove an answer from one neighbour. Arbitrum's and BNB Chain's blocks can share a
/// second, so they are refused rather than answered from a guess.
pub fn strictly_increasing_timestamps(chain_id: u64) -> bool {
    matches!(chain_id, 1 | 10 | 100 | 137 | 8453)
}

/// `account/nuthatchDeposits`: the deposits feed after sequence number `after`, at most `offset`
/// entries, and how far the sweep has read, so a consumer knows the feed is complete up to there.
fn deposits_feed(
    history: Option<&AddressHistory>,
    params: &std::collections::HashMap<String, String>,
) -> Value {
    let Some(history) = history else {
        return unsupported("this nest has no [address_history]");
    };
    let num = |k: &str, default: u64| -> std::result::Result<u64, String> {
        match params.get(k) {
            None => Ok(default),
            Some(v) => v.parse().map_err(|_| format!("{k} `{v}` is not a number")),
        }
    };
    let (after, limit) = match (num("after", 0), num("offset", DEFAULT_OFFSET)) {
        (Ok(a), Ok(l)) if (1..=MAX_OFFSET).contains(&l) => (a, l),
        (Ok(_), Ok(l)) => return error(format!("offset {l} is outside 1 to {MAX_OFFSET}")),
        (Err(e), _) | (_, Err(e)) => return error(e),
    };
    match history.deposits_page(after, limit as usize) {
        Ok((entries, through)) => serde_json::json!({
            "status": "1", "message": "OK", "result": entries,
            "sweptThrough": through,
        }),
        Err(e) => error(format!("{e:#}")),
    }
}

/// `block/getblocknobytime` from verified headers. The rotki-mode server searches its RPC when
/// none bracket the timestamp.
fn block_no_by_time(
    history: Option<&AddressHistory>,
    params: &std::collections::HashMap<String, String>,
) -> Value {
    let Some(history) = history else {
        return unsupported("this nest has no [address_history]");
    };
    let get = |k: &str| params.get(k).map(String::as_str);
    if let Some(c) = get("chainid") {
        if c.parse::<u64>().ok() != Some(history.chain_id) {
            return unsupported(&format!(
                "chainid {c} is not this nest's chain {}",
                history.chain_id
            ));
        }
    }
    if !strictly_increasing_timestamps(history.chain_id) {
        return unsupported(&format!(
            "chain {}'s blocks can share a timestamp, so a block by time cannot be proven",
            history.chain_id
        ));
    }
    let Some(t) = get("timestamp").and_then(|t| t.parse::<u64>().ok()) else {
        return error("Invalid timestamp");
    };
    let after = match get("closest") {
        Some("before") => false,
        Some("after") => true,
        _ => return error("Invalid closest value, use before or after"),
    };
    match history.block_by_time(t, after) {
        Ok(Some(b)) => ok(
            Value::String(b.to_string()),
            history.generation().unwrap_or(0),
        ),
        Ok(None) => incomplete(&format!("no verified headers bracket timestamp {t}")),
        Err(e) => error(format!("{e:#}")),
    }
}

/// A `txhash` lookup: every internal transaction of one transaction, from its cached trace. A
/// transaction not traced yet answers unsupported here; the rotki-mode server traces it first.
fn by_txhash(
    history: &AddressHistory,
    action: Action,
    hash: &str,
    params: &std::collections::HashMap<String, String>,
) -> Value {
    if action != Action::TxListInternal {
        return unsupported(&format!("{} does not take a txhash", action.etherscan()));
    }
    let key = match hash
        .strip_prefix("0x")
        .and_then(|h| alloy_primitives::hex::decode(h).ok())
        .filter(|k| k.len() == 32)
    {
        Some(k) => k,
        None => return error("Invalid txhash format"),
    };
    // Paged as Etherscan pages it: a client walks `page` at a fixed `offset` until a short page.
    let num = |k: &str, default: u64| -> Result<u64> {
        match params.get(k).map(String::as_str).filter(|s| !s.is_empty()) {
            None => Ok(default),
            Some(s) => s.parse::<u64>().map_err(|_| anyhow!("invalid {k} `{s}`")),
        }
    };
    let (page, offset) = match (num("page", 1), num("offset", MAX_OFFSET)) {
        (Ok(p), Ok(o)) if p >= 1 && (1..=MAX_OFFSET).contains(&o) => (p, o),
        (Err(e), _) | (_, Err(e)) => return error(e),
        _ => {
            return error(format!(
                "page and offset start at 1; offset is at most {MAX_OFFSET}"
            ))
        }
    };
    let descending = match params.get("sort").map(String::as_str) {
        None | Some("asc") => false,
        Some("desc") => true,
        Some(s) => return error(format!("invalid sort `{s}`")),
    };
    let unpaged = !params.contains_key("page") && !params.contains_key("offset");
    match history.tx_internals(&key) {
        // Unpaged, the whole list or nothing: cut at the offset cap, a client asking once would take
        // the first 10,000 for all of them.
        Ok(Some(rows)) if unpaged && rows.len() as u64 > MAX_OFFSET => unsupported(&format!(
            "{hash} has {} internal transactions, over the {MAX_OFFSET} one page holds; ask for it by page",
            rows.len()
        )),
        Ok(Some(mut rows)) => {
            if descending {
                rows.reverse();
            }
            let skip = usize::try_from((page - 1).saturating_mul(offset)).unwrap_or(usize::MAX);
            let take = usize::try_from(offset).unwrap_or(usize::MAX);
            ok(
                Value::Array(
                    rows.into_iter()
                        .skip(skip)
                        .take(take)
                        .map(|mut r| {
                            // Etherscan's txhash form carries no traceId; rotki reads its absence as 0.
                            r.remove("traceId");
                            Value::Object(r)
                        })
                        .collect(),
                ),
                history.generation().unwrap_or(0),
            )
        }
        Ok(None) => unsupported(&format!("{hash} has not been traced")),
        Err(e) => error(format!("{e:#}")),
    }
}

fn parse_request(params: &std::collections::HashMap<String, String>) -> Result<PageRequest> {
    let get = |k: &str| params.get(k).map(String::as_str).filter(|s| !s.is_empty());
    let address = get("address").ok_or_else(|| anyhow!("Missing Or invalid Address"))?;
    parse_address(address).map_err(|_| anyhow!("Missing Or invalid Address"))?;
    let num = |k: &str| -> Result<Option<u64>> {
        get(k)
            .map(|s| s.parse::<u64>().map_err(|_| anyhow!("invalid {k} `{s}`")))
            .transpose()
    };
    let sort = match get("sort") {
        None | Some("asc") => Sort::Asc,
        Some("desc") => Sort::Desc,
        Some(s) => bail!("invalid sort `{s}`"),
    };
    let page = num("page")?.unwrap_or(1);
    let offset = num("offset")?.unwrap_or(DEFAULT_OFFSET);
    if page == 0 || offset == 0 {
        bail!("page and offset start at 1");
    }
    if offset > MAX_OFFSET {
        bail!("offset {offset} is over the maximum of {MAX_OFFSET}");
    }
    Ok(PageRequest {
        address: address.to_string(),
        start_block: num("startblock")?,
        end_block: num("endblock")?,
        sort,
        page,
        offset,
        generation: num("generation")?,
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::{BTreeSet, HashMap};

    const ALICE: &str = "0xd8dA6BF26964aF9D7eEd9e03E53415D37aA96045";
    const BOB: &str = "0x00000000219ab540356cbb839cbe05303d7705fa";

    fn history() -> (tempfile::TempDir, AddressHistory) {
        let dir = tempfile::tempdir().unwrap();
        let store = Store::open(&dir.path().join("t.redb")).unwrap();
        (
            dir,
            AddressHistory::open(store, 1, &[ALICE.into()]).unwrap(),
        )
    }

    fn tx(block: u64, tx_index: u64) -> Row {
        let mut record = Map::new();
        record.insert("blockNumber".into(), Value::String(block.to_string()));
        record.insert(
            "hash".into(),
            Value::String(format!("0x{block:04x}{tx_index:060x}")),
        );
        record.insert(
            "timeStamp".into(),
            Value::String((1_700_000_000 + block).to_string()),
        );
        Row {
            block,
            tx_index,
            position: 0,
            record,
        }
    }

    fn params(pairs: &[(&str, &str)]) -> HashMap<String, String> {
        pairs
            .iter()
            .map(|(k, v)| (k.to_string(), v.to_string()))
            .collect()
    }

    fn hashes(v: &Value) -> Vec<String> {
        v["result"]
            .as_array()
            .unwrap_or_else(|| panic!("not a list: {v}"))
            .iter()
            .map(|r| r["hash"].as_str().unwrap().to_string())
            .collect()
    }

    fn req(start: u64, end: u64, page: u64, offset: u64) -> PageRequest {
        PageRequest {
            address: ALICE.into(),
            start_block: Some(start),
            end_block: Some(end),
            sort: Sort::Asc,
            page,
            offset,
            generation: None,
        }
    }

    #[test]
    fn a_thousand_and_one_rows_in_one_block_page_through_exactly() {
        let (_d, h) = history();
        let rows: Vec<Row> = (0..1_001).map(|i| tx(500, i)).collect();
        h.insert(Action::TxList, ALICE, &rows).unwrap();
        h.mark_covered(Action::TxList, ALICE, 0, 600).unwrap();
        let q = |page: &str| {
            respond(
                Some(&h),
                &params(&[
                    ("module", "account"),
                    ("action", "txlist"),
                    ("address", ALICE),
                    ("startblock", "500"),
                    ("endblock", "500"),
                    ("page", page),
                    ("offset", "1000"),
                ]),
            )
        };
        let first = hashes(&q("1"));
        let second = hashes(&q("2"));
        let third = hashes(&q("3"));
        assert_eq!(first.len(), 1_000, "a full first page");
        assert_eq!(second.len(), 1, "the 1,001st row on its own page");
        assert!(third.is_empty(), "nothing past the last row");
        let all: Vec<String> = first.iter().chain(&second).cloned().collect();
        let distinct: BTreeSet<&String> = all.iter().collect();
        assert_eq!(distinct.len(), 1_001, "no row twice");
        let want: Vec<String> = rows
            .iter()
            .map(|r| r.record["hash"].as_str().unwrap().to_string())
            .collect();
        assert_eq!(all, want, "every row, in transaction order");
    }

    /// A page is one range read over one address: with 50,000 of another account's rows in the same
    /// blocks, a 1,000-row page reads exactly 1,000 rows.
    #[test]
    fn a_page_reads_only_its_own_addresses_rows() {
        let (_d, h) = history();
        h.watch(BOB).unwrap();
        let mine: Vec<Row> = (0..2_000).map(|i| tx(100 + i / 10, i % 10)).collect();
        let theirs: Vec<Row> = (0..50_000).map(|i| tx(100 + i / 250, i % 250)).collect();
        h.insert(Action::TxList, ALICE, &mine).unwrap();
        h.insert(Action::TxList, BOB, &theirs).unwrap();
        h.mark_covered(Action::TxList, ALICE, 0, 400).unwrap();

        let mut times = Vec::new();
        for _ in 0..21 {
            let t = std::time::Instant::now();
            let Answer::Rows(rows) = h.page(Action::TxList, &req(0, 400, 1, 1_000)).unwrap() else {
                panic!("covered")
            };
            times.push(t.elapsed());
            assert_eq!(rows.len(), 1_000);
            assert_eq!(ROWS_READ.with(|n| n.get()), 1_000, "read past its own page");
        }
        times.sort();
        eprintln!("1,000-row page p50 {:?}", times[times.len() / 2]);

        h.page(Action::TxList, &req(0, 400, 2, 1_000)).unwrap();
        assert_eq!(
            ROWS_READ.with(|n| n.get()),
            2_000,
            "page 2 reads its own address's first two pages and nothing of the other's"
        );
    }

    #[test]
    fn descending_pages_are_the_ascending_order_reversed() {
        let (_d, h) = history();
        let rows: Vec<Row> = (0..5).map(|i| tx(10 + i, 0)).collect();
        h.insert(Action::TxList, ALICE, &rows).unwrap();
        h.mark_covered(Action::TxList, ALICE, 0, 20).unwrap();
        let q = |sort: &str, page: &str| {
            hashes(&respond(
                Some(&h),
                &params(&[
                    ("module", "account"),
                    ("action", "txlist"),
                    ("address", ALICE),
                    ("endblock", "20"),
                    ("sort", sort),
                    ("page", page),
                    ("offset", "2"),
                ]),
            ))
        };
        let asc: Vec<String> = ["1", "2", "3"].iter().flat_map(|p| q("asc", p)).collect();
        let mut desc: Vec<String> = ["1", "2", "3"].iter().flat_map(|p| q("desc", p)).collect();
        desc.reverse();
        assert_eq!(asc.len(), 5);
        assert_eq!(asc, desc);
    }

    #[test]
    fn a_range_not_wholly_covered_is_incomplete_not_empty() {
        let (_d, h) = history();
        h.mark_covered(Action::TokenTx, ALICE, 100, 200).unwrap();
        let q = |start: &str, end: &str| {
            respond(
                Some(&h),
                &params(&[
                    ("module", "account"),
                    ("action", "tokentx"),
                    ("address", ALICE),
                    ("startblock", start),
                    ("endblock", end),
                ]),
            )
        };
        let covered = q("100", "200");
        assert_eq!(covered["status"], "1", "{covered}");
        assert_eq!(
            covered["result"],
            serde_json::json!([]),
            "covered and empty is an empty list"
        );
        for (start, end) in [("99", "150"), ("150", "201"), ("0", "50"), ("201", "300")] {
            let a = q(start, end);
            assert_eq!(a["status"], "0", "[{start}, {end}]: {a}");
            assert!(
                a["result"]
                    .as_str()
                    .unwrap()
                    .starts_with("NUTHATCH_INCOMPLETE:"),
                "[{start}, {end}]: {a}"
            );
        }
        let other = respond(
            Some(&h),
            &params(&[
                ("module", "account"),
                ("action", "txlist"),
                ("address", ALICE),
                ("startblock", "100"),
                ("endblock", "200"),
            ]),
        );
        assert!(
            other["result"]
                .as_str()
                .unwrap()
                .starts_with("NUTHATCH_INCOMPLETE:"),
            "coverage is per action: {other}"
        );
    }

    #[test]
    fn a_gap_between_covered_spans_is_incomplete() {
        let (_d, h) = history();
        h.mark_covered(Action::TxList, ALICE, 0, 99).unwrap();
        h.mark_covered(Action::TxList, ALICE, 101, 200).unwrap();
        assert_eq!(
            h.coverage(Action::TxList, ALICE).unwrap(),
            vec![(0, 99), (101, 200)]
        );
        let r = req(0, 200, 1, 10);
        assert!(matches!(
            h.page(Action::TxList, &r).unwrap(),
            Answer::Incomplete(_)
        ));
        assert_eq!(
            h.page(Action::TxList, &req(150, 160, 1, 10)).unwrap(),
            Answer::Rows(vec![]),
            "a range inside the second span is covered"
        );
        h.mark_covered(Action::TxList, ALICE, 100, 100).unwrap();
        assert_eq!(h.coverage(Action::TxList, ALICE).unwrap(), vec![(0, 200)]);
        assert_eq!(h.page(Action::TxList, &r).unwrap(), Answer::Rows(vec![]));
    }

    /// An explicit end past what is covered is refused whole, never answered short: a client that
    /// asked through block N records that it has history through N (cold test B1, 2026-10-11: 17 of
    /// Etherscan's 65 rows came back as a complete answer).
    #[test]
    fn an_endblock_past_coverage_is_refused_not_shortened() {
        let (_d, h) = history();
        h.insert(Action::TxList, ALICE, &[tx(50, 0), tx(150, 0)])
            .unwrap();
        h.mark_covered(Action::TxList, ALICE, 0, 100).unwrap();
        h.set_head(200).unwrap();
        let ask = |end: &str| {
            respond(
                Some(&h),
                &params(&[
                    ("module", "account"),
                    ("action", "txlist"),
                    ("address", ALICE),
                    ("endblock", end),
                ]),
            )
        };
        for end in ["101", "200", "99999999"] {
            let a = ask(end);
            assert!(
                a["result"]
                    .as_str()
                    .unwrap()
                    .starts_with("NUTHATCH_INCOMPLETE:"),
                "{end}: {a}"
            );
        }
        assert_eq!(hashes(&ask("100")).len(), 1);
    }

    #[test]
    fn an_unknown_action_or_module_is_unsupported() {
        let (_d, h) = history();
        for (module, action) in [
            ("account", "balancemulti"),
            ("contract", "getabi"),
            ("block", "getblockreward"),
            ("logs", "txlist"),
        ] {
            let a = respond(
                Some(&h),
                &params(&[("module", module), ("action", action), ("address", ALICE)]),
            );
            assert_eq!(a["status"], "0", "{module}/{action}");
            assert!(
                a["result"]
                    .as_str()
                    .unwrap()
                    .starts_with("NUTHATCH_UNSUPPORTED:"),
                "{module}/{action}: {a}"
            );
        }
        let none = respond(
            None,
            &params(&[
                ("module", "account"),
                ("action", "txlist"),
                ("address", ALICE),
            ]),
        );
        assert!(
            none["result"]
                .as_str()
                .unwrap()
                .starts_with("NUTHATCH_UNSUPPORTED:"),
            "{none}"
        );
    }

    /// A transaction cached at 105 and re-cached at 90 keeps its 105 index key; a reorg back to 100
    /// must read the record's block, not the key's, and keep the copy.
    #[test]
    fn a_reorg_keeps_a_cached_transaction_re_cached_under_the_fork() {
        let (_d, h) = history();
        h.finalize(80).unwrap();
        for block in [105, 90] {
            let g = h.generation().unwrap();
            h.persist_pending(
                &[(vec![1; 32], tx(block, 0).record)],
                &[],
                &[],
                &[],
                &[],
                g,
                None,
                true,
            )
            .unwrap();
        }
        h.invalidate_above(100).unwrap();
        let kept = h.cached_tx(&[1; 32]).unwrap().expect("cached at 90");
        assert_eq!(kept["blockNumber"], "90");
    }

    /// #2275: the per-account passes find their accounts from coverage and the bodies asked for,
    /// never from the sweep's shared span or an account no longer watched.
    #[test]
    fn accounts_with_finds_only_watched_accounts_with_their_own_coverage() {
        let (_d, h) = history();
        let (carol, dave) = (
            "0x00000000000000000000000000000000000000c0",
            "0x00000000000000000000000000000000000000d0",
        );
        h.watch_many(&[BOB.into(), carol.into(), dave.into()])
            .unwrap();
        let g = h.generation().unwrap();
        h.record_sweep(
            &std::collections::BTreeMap::new(),
            [ALICE, BOB, carol, dave],
            (1, 10),
            g,
            None,
            false,
        )
        .unwrap();
        let g = h.generation().unwrap();
        h.record(Action::MinedBlocks, BOB, &[], (1, 10), g).unwrap();
        h.ask_bodies(carol).unwrap();
        h.record(Action::MinedBlocks, dave, &[], (1, 10), g)
            .unwrap();
        h.unwatch(dave).unwrap();
        let bob = BOB.to_lowercase();
        assert_eq!(
            h.accounts_with(&[Action::MinedBlocks], false).unwrap(),
            vec![bob.clone()]
        );
        assert_eq!(
            h.accounts_with(&[Action::MinedBlocks], true).unwrap(),
            vec![carol.to_string(), bob]
        );
        assert!(h
            .accounts_with(&[Action::TxList], false)
            .unwrap()
            .is_empty());
    }

    /// #2117: a reorg reads what it takes back from a block-first index. A store written before the
    /// index is indexed once at open, so its rows and cached transactions above the head still go.
    #[test]
    fn a_store_from_before_the_reorg_index_is_indexed_at_open() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("t.redb");
        let bob = parse_address(BOB).unwrap();
        {
            let h = AddressHistory::open(Store::open(&path).unwrap(), 1, &[ALICE.into()]).unwrap();
            h.finalize(100).unwrap();
            h.insert(Action::TxList, ALICE, &[tx(90, 0), tx(105, 0)])
                .unwrap();
            // Rows of an address neither watched nor covered still go: a fetch can outlive both.
            h.insert(Action::TokenTx, BOB, &[tx(106, 0)]).unwrap();
            let g = h.generation().unwrap();
            h.persist_pending(
                &[
                    (vec![1; 32], tx(105, 0).record),
                    (vec![2; 32], tx(90, 0).record),
                ],
                &[],
                &[],
                &[],
                &[],
                g,
                None,
                true,
            )
            .unwrap();
            let wtx = h.store.database().begin_write().unwrap();
            wtx.open_table(REORGABLE)
                .unwrap()
                .retain(|_, _| false)
                .unwrap();
            wtx.open_table(META)
                .unwrap()
                .remove(REORGABLE_COMPLETE)
                .unwrap();
            wtx.commit().unwrap();
        }
        let h = AddressHistory::open(Store::open(&path).unwrap(), 1, &[ALICE.into()]).unwrap();
        h.invalidate_above(100).unwrap();
        let blocks = |action, a: &Address| -> Vec<u64> {
            let rtx = h.store.database().begin_read().unwrap();
            let t = rtx.open_table(Action::table(action)).unwrap();
            t.range(
                row_key(a, 0, 0, 0).as_slice()
                    ..=row_key(a, u64::MAX, u64::MAX, u64::MAX).as_slice(),
            )
            .unwrap()
            .map(|e| u64::from_be_bytes(e.unwrap().0.value()[20..28].try_into().unwrap()))
            .collect()
        };
        assert_eq!(
            blocks(Action::TxList, &parse_address(ALICE).unwrap()),
            vec![90]
        );
        assert_eq!(blocks(Action::TokenTx, &bob), Vec::<u64>::new());
        assert!(h.cached_tx(&[1; 32]).unwrap().is_none());
        assert!(h.cached_tx(&[2; 32]).unwrap().is_some());
    }

    #[test]
    fn a_reorg_drops_rows_and_coverage_above_the_block() {
        let (_d, h) = history();
        let rows: Vec<Row> = [90, 100, 101, 150].iter().map(|b| tx(*b, 0)).collect();
        h.insert(Action::TxList, ALICE, &rows).unwrap();
        h.insert(Action::TokenTx, ALICE, &[tx(120, 3)]).unwrap();
        h.mark_covered(Action::TxList, ALICE, 0, 200).unwrap();
        h.mark_covered(Action::TokenTx, ALICE, 110, 200).unwrap();
        h.set_head(200).unwrap();

        h.invalidate_above(100).unwrap();

        assert_eq!(h.coverage(Action::TxList, ALICE).unwrap(), vec![(0, 100)]);
        assert!(
            h.coverage(Action::TokenTx, ALICE).unwrap().is_empty(),
            "a span wholly above goes"
        );
        assert_eq!(h.head().unwrap(), Some(100));
        let Answer::Rows(kept) = h.page(Action::TxList, &req(0, 100, 1, 100)).unwrap() else {
            panic!("covered through 100")
        };
        let blocks: Vec<&str> = kept
            .iter()
            .map(|r| r["blockNumber"].as_str().unwrap())
            .collect();
        assert_eq!(blocks, ["90", "100"]);
        h.mark_covered(Action::TxList, ALICE, 101, 200).unwrap();
        h.mark_covered(Action::TokenTx, ALICE, 0, 200).unwrap();
        let Answer::Rows(after) = h.page(Action::TxList, &req(0, 200, 1, 100)).unwrap() else {
            panic!("re-covered")
        };
        assert_eq!(
            after.len(),
            2,
            "rows above the reorg are gone, not merely hidden"
        );
        let Answer::Rows(tokens) = h.page(Action::TokenTx, &req(0, 200, 1, 100)).unwrap() else {
            panic!("re-covered")
        };
        assert!(
            tokens.is_empty(),
            "the token row above the reorg went too: {tokens:?}"
        );
    }

    #[test]
    fn an_address_watched_later_reads_incomplete_until_backfilled() {
        let (_d, h) = history();
        assert_eq!(h.watched().unwrap(), vec![ALICE.to_lowercase()]);
        h.watch(BOB).unwrap();
        let mut w = h.watched().unwrap();
        w.sort();
        assert_eq!(w, vec![BOB.to_string(), ALICE.to_lowercase()]);
        let bob = PageRequest {
            address: BOB.into(),
            ..req(0, 10, 1, 10)
        };
        assert!(matches!(
            h.page(Action::TxList, &bob).unwrap(),
            Answer::Incomplete(_)
        ));

        h.insert(Action::TxList, BOB, &[tx(5, 0)]).unwrap();
        h.mark_covered(Action::TxList, BOB, 0, 10).unwrap();
        h.unwatch(BOB).unwrap();
        assert_eq!(h.watched().unwrap(), vec![ALICE.to_lowercase()]);
        assert!(h.coverage(Action::TxList, BOB).unwrap().is_empty());
        h.mark_covered(Action::TxList, BOB, 0, 10).unwrap();
        assert_eq!(
            h.page(Action::TxList, &bob).unwrap(),
            Answer::Rows(vec![]),
            "unwatching drops the rows, not just the coverage"
        );
    }

    #[test]
    fn reopening_with_a_different_seed_keeps_the_watched_set() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("t.redb");
        {
            let h = AddressHistory::open(Store::open(&path).unwrap(), 1, &[ALICE.into()]).unwrap();
            h.watch(BOB).unwrap();
        }
        let h = AddressHistory::open(Store::open(&path).unwrap(), 1, &[]).unwrap();
        assert_eq!(h.watched().unwrap().len(), 2);

        // Unwatching a configured address survives a restart with the same config.
        h.unwatch(ALICE).unwrap();
        drop(h);
        let h = AddressHistory::open(Store::open(&path).unwrap(), 1, &[ALICE.into()]).unwrap();
        assert_eq!(h.watched().unwrap(), vec![BOB.to_string()]);
        h.watch(ALICE).unwrap();
        drop(h);
        let h = AddressHistory::open(Store::open(&path).unwrap(), 1, &[]).unwrap();
        assert_eq!(
            h.watched().unwrap().len(),
            2,
            "watching again clears the removal"
        );
    }

    #[test]
    fn records_round_trip_exactly_and_pack_small() {
        let raw = serde_json::json!({
            "blockNumber": "19000000",
            "timeStamp": "1704067211",
            "hash": "0x5c504ed432cb51138bcf09aa5e8a410dd4a1e204ef84bfed1be16dfba1b22060",
            "from": "0xd8da6bf26964af9d7eed9e03e53415d37aa96045",
            "to": "",
            "value": "0",
            "input": "0x",
            "isError": "0",
            "nonce": "007",
            "gas": "0x0",
            "functionName": "transfer(address _to, uint256 _value)",
            "tokenValue": "115792089237316195423570985008687907853269984665640564039457584007913129639935",
            "authorizationList": [{"chainId": "1", "yParity": 0, "nested": [true, false, null, 1.5, -3]}],
            "someNewField": "0xABCD",
        });
        let Value::Object(record) = raw.clone() else {
            unreachable!()
        };
        let mut buf = Vec::new();
        encode_record(&record, &mut buf).unwrap();
        assert_eq!(Value::Object(decode_record(&buf).unwrap()), raw);
        assert!(
            !buf.windows(10).any(|w| w == b"{\"chainId\""),
            "a nested value is packed, not stored as JSON text"
        );

        // An Etherscan txlist row as it arrives: hashes and addresses dominate, and pack to bytes.
        let row = serde_json::json!({
            "blockNumber": "19000000", "timeStamp": "1704067211",
            "hash": "0x5c504ed432cb51138bcf09aa5e8a410dd4a1e204ef84bfed1be16dfba1b22060",
            "nonce": "1234",
            "blockHash": "0x2e9e5d5e3b0e2a6a3e1a0c6c1c7b8b3f5f4f7d9c1d0a6b4e7e3f2a1b0c9d8e7f",
            "transactionIndex": "42", "from": "0xd8da6bf26964af9d7eed9e03e53415d37aa96045",
            "to": "0xa0b86991c6218b36c1d19d4a2e9eb0ce3606eb48", "value": "0", "gas": "65000",
            "gasPrice": "21000000000", "isError": "0", "txreceipt_status": "1",
            "input": "0xa9059cbb000000000000000000000000ab5801a7d398351b8be11c439e05c5b3259aec9b0000000000000000000000000000000000000000000000000000000005f5e100",
            "contractAddress": "", "cumulativeGasUsed": "4012345", "gasUsed": "41309",
            "confirmations": "1000000", "methodId": "0xa9059cbb",
            "functionName": "transfer(address _to, uint256 _value)",
        });
        let Value::Object(row) = row.clone() else {
            unreachable!()
        };
        buf.clear();
        encode_record(&row, &mut buf).unwrap();
        let json_len = serde_json::to_string(&row).unwrap().len();
        assert!(
            buf.len() * 2 < json_len,
            "{} bytes packed against {json_len} as JSON",
            buf.len()
        );
    }

    #[test]
    fn a_row_whose_blocknumber_disagrees_with_its_key_is_refused() {
        let (_d, h) = history();
        let mut r = tx(10, 0);
        r.block = 11;
        assert!(h.insert(Action::TxList, ALICE, &[r]).is_err());
    }

    #[test]
    fn addresses_are_matched_case_insensitively_but_a_bad_checksum_is_refused() {
        let (_d, h) = history();
        h.insert(Action::TxList, &ALICE.to_lowercase(), &[tx(5, 0)])
            .unwrap();
        h.mark_covered(Action::TxList, ALICE, 0, 10).unwrap();
        let a = respond(
            Some(&h),
            &params(&[
                ("module", "account"),
                ("action", "txlist"),
                ("address", ALICE),
                ("endblock", "10"),
            ]),
        );
        assert_eq!(hashes(&a).len(), 1, "{a}");
        assert!(parse_address("0xD8dA6BF26964aF9D7eEd9e03E53415D37aA96045").is_err());
    }

    /// Rows the RPC scan recorded in a mirror's blocks are set aside unless a verified partition
    /// replaced them, and a verified record replaces what its span held.
    /// An RPC scan over blocks a partition supplied replaces their rows: a row the scan did not
    /// produce is not left beside the rows it did.
    #[test]
    fn a_recorded_span_replaces_the_rows_it_held() {
        let (_d, h) = history();
        let at = |b: u64| Row {
            block: b,
            tx_index: 0,
            position: 0,
            record: serde_json::json!({"blockNumber": b.to_string()})
                .as_object()
                .unwrap()
                .clone(),
        };
        let g = h.generation().unwrap();
        h.record_verified(Action::MinedBlocks, ALICE, &[at(55), at(60)], (50, 70), g)
            .unwrap();
        h.record(Action::MinedBlocks, ALICE, &[at(55)], (40, 80), g)
            .unwrap();
        let Answer::Rows(rows) = h
            .page(
                Action::MinedBlocks,
                &PageRequest {
                    start_block: Some(40),
                    end_block: Some(80),
                    ..req(0, 0, 1, 10)
                },
            )
            .unwrap()
        else {
            panic!("covered")
        };
        let blocks: Vec<&Value> = rows.iter().map(|r| &r["blockNumber"]).collect();
        assert_eq!(blocks, ["55"]);
    }

    #[test]
    fn only_verified_coverage_survives_in_the_mirrors_blocks() {
        let (_d, h) = history();
        let at = |b: u64| Row {
            block: b,
            tx_index: 0,
            position: 0,
            record: serde_json::json!({"blockNumber": b.to_string()})
                .as_object()
                .unwrap()
                .clone(),
        };
        let g = h.generation().unwrap();
        h.record(
            Action::MinedBlocks,
            ALICE,
            &[at(10), at(60), at(90)],
            (0, 100),
            g,
        )
        .unwrap();
        h.record_verified(Action::MinedBlocks, ALICE, &[at(55)], (50, 70), g)
            .unwrap();
        // Produced blocks built on that mined-block coverage.
        h.record_produced(ALICE, &[at(10), at(60), at(90)], (0, 100), g, &[], 0)
            .unwrap();
        h.keep_verified_only(&[Action::MinedBlocks], (40, 80))
            .unwrap();
        // Where unverified mined-block coverage went, the produced-block coverage built on it went too;
        // over verified blocks it stays.
        assert_eq!(
            h.coverage(Action::ProducedBlocks, ALICE).unwrap(),
            vec![(0, 39), (50, 70), (81, 100)]
        );
        assert_eq!(
            h.coverage(Action::MinedBlocks, ALICE).unwrap(),
            vec![(0, 39), (50, 70), (81, 100)]
        );
        assert_eq!(
            h.verified_coverage(Action::MinedBlocks, ALICE).unwrap(),
            vec![(50, 70)]
        );
        assert!(
            h.generation().unwrap() > g,
            "a page cut before this must not continue"
        );
        let Answer::Rows(rows) = h
            .page(
                Action::MinedBlocks,
                &PageRequest {
                    start_block: Some(50),
                    end_block: Some(70),
                    ..req(0, 0, 1, 10)
                },
            )
            .unwrap()
        else {
            panic!("covered")
        };
        assert_eq!(rows.len(), 1, "block 60 from the RPC is gone, 55 verified");
        assert_eq!(rows[0]["blockNumber"], "55");
        h.invalidate_above(60).unwrap();
        assert_eq!(
            h.verified_coverage(Action::MinedBlocks, ALICE).unwrap(),
            vec![(50, 60)]
        );
        let g = h.generation().unwrap();
        h.record(Action::MinedBlocks, ALICE, &[], (55, 56), g)
            .unwrap();
        assert_eq!(
            h.verified_coverage(Action::MinedBlocks, ALICE).unwrap(),
            vec![(50, 54), (57, 60)],
            "an RPC write over verified blocks takes their mark away"
        );
    }

    /// `getblocknobytime` answers only inside verified headers, an equal timestamp naming its own
    /// block either way, and a reorg below them takes the answer away.
    #[test]
    fn getblocknobytime_answers_from_verified_headers_only() {
        let (_d, h) = history();
        let ask = |t: &str, closest: &str| {
            respond(
                Some(&h),
                &params(&[
                    ("module", "block"),
                    ("action", "getblocknobytime"),
                    ("timestamp", t),
                    ("closest", closest),
                ]),
            )
        };
        assert!(ask("1000", "before")["result"]
            .as_str()
            .unwrap()
            .starts_with("NUTHATCH_INCOMPLETE:"));
        // Blocks 100..=110 at 1000, 1012, ...; 99 and 111 are unread RPC timestamps, not verified.
        h.cache_block_timestamps(&[(99, 988), (111, 1132)]).unwrap();
        let ts: Vec<(u64, u64)> = (100..=110).map(|b| (b, 1000 + 12 * (b - 100))).collect();
        h.record_headers((100, 110), &ts).unwrap();
        // A slow RPC read landing after the partition does not replace a verified timestamp.
        h.cache_block_timestamps(&[(105, 1)]).unwrap();
        for (t, closest, want) in [
            ("1000", "before", "100"),
            ("1000", "after", "100"),
            ("1005", "before", "100"),
            ("1005", "after", "101"),
            ("1120", "before", "110"),
            ("1120", "after", "110"),
        ] {
            assert_eq!(ask(t, closest)["result"], want, "{t} {closest}");
        }
        for (t, closest) in [("999", "after"), ("1121", "before")] {
            assert!(
                ask(t, closest)["result"]
                    .as_str()
                    .unwrap()
                    .starts_with("NUTHATCH_INCOMPLETE:"),
                "{t} {closest}"
            );
        }
        assert!(ask("1005", "nearest")["result"]
            .as_str()
            .unwrap()
            .starts_with("Error!"));
        assert!(h.record_headers((100, 102), &ts[..2]).is_err());
        h.invalidate_above(105).unwrap();
        assert_eq!(h.header_coverage().unwrap(), vec![(100, 105)]);
        assert!(ask("1100", "before")["result"]
            .as_str()
            .unwrap()
            .starts_with("NUTHATCH_INCOMPLETE:"));
    }

    /// What a fetch read is kept against the generation it began in: if a reorg was handled
    /// meanwhile, nothing above the finalized head is kept, anchors included, even though the new
    /// chain may have grown back past those blocks; what is under it is kept.
    #[test]
    fn a_stale_fetch_keeps_nothing_above_the_finalized_head() {
        let (_d, h) = history();
        h.finalize(100).unwrap();
        let g = h.generation().unwrap();
        let tx = |b: u64| {
            let mut r = Map::new();
            r.insert("blockNumber".into(), Value::String(b.to_string()));
            (vec![b as u8; 32], r)
        };
        h.invalidate_above(150).unwrap();
        h.persist_pending(
            &[tx(90), tx(160)],
            &[(90, 1, None), (160, 2, None)],
            &[(vec![7; 32], 160, [3; 32], vec![])],
            &[(160, [1; 32])],
            &[],
            g,
            Some(100),
            true,
        )
        .unwrap();
        assert!(h.cached_tx(&[90; 32]).unwrap().is_some());
        assert!(h.cached_tx(&[160; 32]).unwrap().is_none());
        assert_eq!(h.block_timestamp(90).unwrap(), Some(1));
        assert_eq!(h.block_timestamp(160).unwrap(), None);
        assert!(h.tx_internals(&[7; 32]).unwrap().is_none());
        assert!(h.tracked().unwrap().is_empty());

        // The same fetch under the current generation keeps all of it.
        let g = h.generation().unwrap();
        // A fetch whose check after it failed keeps only what was finalized when it began.
        h.persist_pending(
            &[tx(160)],
            &[(160, 2, None)],
            &[],
            &[(160, [1; 32])],
            &[],
            g,
            Some(100),
            false,
        )
        .unwrap();
        assert!(h.cached_tx(&[160; 32]).unwrap().is_none());
        assert!(h.tracked().unwrap().is_empty());
        h.persist_pending(
            &[tx(160)],
            &[(160, 2, None)],
            &[],
            &[(160, [1; 32])],
            &[],
            g,
            Some(100),
            true,
        )
        .unwrap();
        assert!(h.cached_tx(&[160; 32]).unwrap().is_some());
        assert_eq!(h.tracked().unwrap(), vec![(160, [1; 32])]);
        // A record anchored at a hash other than the tracked one is refused.
        let err = h
            .record_anchored(
                Action::TxList,
                ALICE,
                &[],
                (101, 160),
                g,
                Some((160, [2; 32])),
            )
            .unwrap_err();
        assert!(err.to_string().contains("changed hash"), "{err:#}");
        // Stale data does not become acceptable because the replacement chain has since finalized
        // past its block: what matters is the head when the fetch began.
        let g_old = h.generation().unwrap();
        h.invalidate_above(150).unwrap();
        h.finalize(200).unwrap();
        h.persist_pending(
            &[tx(155)],
            &[(155, 9, None)],
            &[],
            &[],
            &[],
            g_old,
            Some(150),
            true,
        )
        .unwrap();
        assert!(h.cached_tx(&[155; 32]).unwrap().is_none());
        assert_eq!(h.block_timestamp(155).unwrap(), None);
        // Finalizing past a tracked block stops tracking it, and never lowers the head.
        h.finalize(170).unwrap();
        h.finalize(120).unwrap();
        assert!(h.tracked().unwrap().is_empty());
        assert_eq!(h.head().unwrap(), Some(200));
    }

    #[test]
    fn a_block_by_time_is_refused_where_timestamps_can_repeat() {
        let dir = tempfile::tempdir().unwrap();
        let h = AddressHistory::open(
            Store::open(&dir.path().join("t.redb")).unwrap(),
            42161,
            &[ALICE.into()],
        )
        .unwrap();
        let a = respond(
            Some(&h),
            &params(&[
                ("module", "block"),
                ("action", "getblocknobytime"),
                ("timestamp", "1"),
                ("closest", "before"),
            ]),
        );
        assert!(
            a["result"]
                .as_str()
                .unwrap()
                .starts_with("NUTHATCH_UNSUPPORTED:"),
            "{a}"
        );
    }

    /// Bor rejects a header whose time is not at least its period past its parent's, and Polygon's
    /// period has never been below one second, so its timestamps strictly increase.
    #[test]
    fn polygon_timestamps_strictly_increase() {
        assert!(strictly_increasing_timestamps(137));
        assert!(!strictly_increasing_timestamps(42161));
        assert!(!strictly_increasing_timestamps(56));
    }

    /// Page 1 without an end reads to the end of what is covered, which moves as coverage grows, so a
    /// later page without one is refused rather than repeating or skipping rows.
    #[test]
    fn a_page_past_the_first_needs_an_endblock() {
        let (_d, h) = history();
        h.mark_covered(Action::TxList, ALICE, 0, 100).unwrap();
        let mut r = req(0, 0, 2, 1);
        r.end_block = None;
        assert!(matches!(
            h.page(Action::TxList, &r).unwrap(),
            Answer::Incomplete(_)
        ));
        r.end_block = Some(100);
        assert_eq!(h.page(Action::TxList, &r).unwrap(), Answer::Rows(vec![]));
    }

    /// Produced-block coverage and the re-ask entries for blocks without relay evidence land in one
    /// transaction; a refused record writes neither.
    #[test]
    fn produced_coverage_and_its_reasks_commit_together() {
        let (_d, h) = history();
        let g = h.generation().unwrap();
        h.record_produced(ALICE, &[], (100, 199), g, &[150, 160], 7)
            .unwrap();
        assert_eq!(
            h.coverage(Action::ProducedBlocks, ALICE).unwrap(),
            vec![(100, 199)]
        );
        assert_eq!(h.mev_pending().unwrap(), vec![(150, 7, 7), (160, 7, 7)]);
        assert!(h
            .record_produced(ALICE, &[], (200, 299), g + 1, &[250], 8)
            .is_err());
        assert_eq!(
            h.coverage(Action::ProducedBlocks, ALICE).unwrap(),
            vec![(100, 199)]
        );
        assert_eq!(h.mev_pending().unwrap().len(), 2);
    }

    /// A store written under 5.1's produced-block contract (`none`, no re-ask schedule, payout
    /// contracts missed) has those rows and their coverage dropped once, at open, to be read again.
    #[test]
    fn produced_rows_from_the_old_contract_are_read_again() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("t.redb");
        {
            let h = AddressHistory::open(Store::open(&path).unwrap(), 1, &[ALICE.into()]).unwrap();
            let mut r = tx(150, 0);
            r.record.insert("mev".into(), Value::String("none".into()));
            h.insert(Action::ProducedBlocks, ALICE, &[r]).unwrap();
            h.mark_covered(Action::ProducedBlocks, ALICE, 100, 199)
                .unwrap();
            h.mark_covered(Action::TxList, ALICE, 100, 199).unwrap();
            // As 5.1 left it: no contract recorded.
            let wtx = h.store.database().begin_write().unwrap();
            wtx.open_table(META)
                .unwrap()
                .remove(PRODUCED_CONTRACT)
                .unwrap();
            wtx.commit().unwrap();
        }
        let h = AddressHistory::open(Store::open(&path).unwrap(), 1, &[ALICE.into()]).unwrap();
        assert!(h
            .coverage(Action::ProducedBlocks, ALICE)
            .unwrap()
            .is_empty());
        assert!(h
            .rows_in(Action::ProducedBlocks, ALICE, (0, 1_000))
            .unwrap()
            .is_empty());
        assert_eq!(
            h.coverage(Action::TxList, ALICE).unwrap(),
            vec![(100, 199)],
            "others untouched"
        );
        let g = h.generation().unwrap();
        h.mark_covered(Action::ProducedBlocks, ALICE, 100, 199)
            .unwrap();
        drop(h);
        let h = AddressHistory::open(Store::open(&path).unwrap(), 1, &[ALICE.into()]).unwrap();
        assert_eq!(
            h.coverage(Action::ProducedBlocks, ALICE).unwrap(),
            vec![(100, 199)],
            "only once"
        );
        assert_eq!(h.generation().unwrap(), g);
    }

    /// A re-ask's evidence, rows and schedule commit together or not at all.
    #[test]
    fn a_reask_commits_its_evidence_with_its_rows_or_not_at_all() {
        let (_d, h) = history();
        h.set_head(1_000).unwrap();
        let g = h.generation().unwrap();
        h.record_produced(ALICE, &[], (100, 199), g, &[150], 7)
            .unwrap();
        let mut record = Map::new();
        record.insert("delivered".into(), Value::Array(vec![]));
        let mut row = tx(150, 0);
        row.record
            .insert("mev".into(), Value::String("relay".into()));
        let changes = vec![(ALICE.to_string(), vec![row])];
        let stale = h.generation().unwrap() + 1;
        assert!(h
            .apply_reask(150, &record, &changes, false, 9, stale)
            .is_err());
        assert!(h.relay_record(150).unwrap().is_none());
        assert!(h
            .rows_in(Action::ProducedBlocks, ALICE, (150, 150))
            .unwrap()
            .is_empty());
        assert_eq!(h.mev_pending().unwrap().len(), 1);
        let g = h.generation().unwrap();
        h.apply_reask(150, &record, &changes, false, 9, g).unwrap();
        assert!(h.relay_record(150).unwrap().is_some());
        assert_eq!(
            h.rows_in(Action::ProducedBlocks, ALICE, (150, 150))
                .unwrap()
                .len(),
            1
        );
        assert!(h.mev_pending().unwrap().is_empty());
        assert!(h.generation().unwrap() > g);
    }

    /// A txhash lookup is paged as Etherscan pages it, reverted frames included: 1,500 rows at an
    /// offset of 1,000 come back as 1,000, then 500, then none, never a page repeated.
    #[test]
    fn a_txhash_lookup_honours_page_and_offset() {
        let (_d, h) = history();
        h.set_head(1_000).unwrap();
        let key = vec![0xab; 32];
        let rows: Vec<Map<String, Value>> = (0..1_500)
            .map(|i| {
                let mut r = Map::new();
                r.insert("blockNumber".into(), Value::String("500".into()));
                r.insert("nuthatchTraceIndex".into(), Value::String(i.to_string()));
                r.insert("isError".into(), Value::String("1".into()));
                r.insert("traceId".into(), Value::String("0_1".into()));
                r
            })
            .collect();
        h.persist_pending(
            &[],
            &[],
            &[(key, 500, [9; 32], rows)],
            &[],
            &[],
            h.generation().unwrap(),
            Some(1_000),
            true,
        )
        .unwrap();
        let ask = |page: &str| {
            respond(
                Some(&h),
                &params(&[
                    ("module", "account"),
                    ("action", "txlistinternal"),
                    ("txhash", &format!("0x{}", "ab".repeat(32))),
                    ("page", page),
                    ("offset", "1000"),
                ]),
            )
        };
        let index = |v: &Value, i: usize| {
            v["result"][i]["nuthatchTraceIndex"]
                .as_str()
                .unwrap()
                .to_string()
        };
        let first = ask("1");
        assert_eq!(first["result"].as_array().unwrap().len(), 1_000);
        assert_eq!(index(&first, 0), "0");
        assert!(first["result"][0].get("traceId").is_none());
        let second = ask("2");
        assert_eq!(second["result"].as_array().unwrap().len(), 500);
        assert_eq!(index(&second, 0), "1000");
        assert_eq!(
            second["result"][0]["isError"], "1",
            "reverted frames are served; the client filters"
        );
        assert_eq!(ask("3")["result"], serde_json::json!([]));
        // Asked once without paging, the whole list comes back.
        let whole = respond(
            Some(&h),
            &params(&[
                ("module", "account"),
                ("action", "txlistinternal"),
                ("txhash", &format!("0x{}", "ab".repeat(32))),
            ]),
        );
        assert_eq!(whole["result"].as_array().unwrap().len(), 1_500);
    }

    /// Unpaged, a list over one page's cap is refused rather than cut: asked once, a client would
    /// take the first page for all of it.
    #[test]
    fn an_unpaged_txhash_lookup_over_the_cap_is_refused() {
        let (_d, h) = history();
        h.set_head(1_000).unwrap();
        let rows: Vec<Map<String, Value>> = (0..=MAX_OFFSET)
            .map(|i| {
                let mut r = Map::new();
                r.insert("nuthatchTraceIndex".into(), Value::String(i.to_string()));
                r
            })
            .collect();
        h.persist_pending(
            &[],
            &[],
            &[(vec![0xcd; 32], 500, [9; 32], rows)],
            &[],
            &[],
            h.generation().unwrap(),
            Some(1_000),
            true,
        )
        .unwrap();
        let hash = format!("0x{}", "cd".repeat(32));
        let base = [
            ("module", "account"),
            ("action", "txlistinternal"),
            ("txhash", hash.as_str()),
        ];
        let a = respond(Some(&h), &params(&base));
        assert!(
            a["result"]
                .as_str()
                .unwrap()
                .starts_with("NUTHATCH_UNSUPPORTED:"),
            "{a}"
        );
        let mut paged = base.to_vec();
        paged.extend([("page", "2"), ("offset", "10000")]);
        assert_eq!(
            respond(Some(&h), &params(&paged))["result"]
                .as_array()
                .unwrap()
                .len(),
            1
        );
    }

    /// Unwatching one address leaves the generation alone, so a window in flight for any other
    /// address is still recorded (cold test: a watch change mid-scan discarded a whole window).
    #[test]
    fn unwatching_one_address_does_not_move_the_generation() {
        let (_d, h) = history();
        h.watch(BOB).unwrap();
        let g = h.generation().unwrap();
        h.unwatch(BOB).unwrap();
        assert_eq!(h.generation().unwrap(), g);
        h.record(Action::TxList, ALICE, &[], (0, 10), g).unwrap();
        assert!(
            h.record(Action::TxList, BOB, &[], (0, 10), g).is_err(),
            "not watched"
        );
    }

    /// Body summaries answer only for a window they wholly cover, go with a reorg above them, and
    /// keep only the newest blocks.
    #[test]
    fn body_summaries_cover_whole_windows_follow_reorgs_and_stay_bounded() {
        let (_d, h) = history();
        let sum = |b: u64| {
            let mut m = Map::new();
            m.insert("ts".into(), Value::String(b.to_string()));
            (b, m)
        };
        let g = h.generation().unwrap();
        let some: Vec<Summary> = (100..=110).map(sum).collect();
        h.persist_pending(&[], &[], &[], &[], &some, g, None, true)
            .unwrap();
        assert_eq!(h.summaries(100, 110).unwrap().unwrap().len(), 11);
        assert!(
            h.summaries(100, 111).unwrap().is_none(),
            "not wholly covered"
        );
        h.invalidate_above(105).unwrap();
        assert!(h.summaries(100, 110).unwrap().is_none());
        assert_eq!(h.summaries(100, 105).unwrap().unwrap().len(), 6);
        // A summary far above pushes the old ones out.
        let g = h.generation().unwrap();
        h.persist_pending(
            &[],
            &[],
            &[],
            &[],
            &[sum(105 + SUMMARY_KEEP)],
            g,
            None,
            true,
        )
        .unwrap();
        assert!(h.summaries(100, 105).unwrap().is_none());
        assert!(h
            .summaries(105 + SUMMARY_KEEP, 105 + SUMMARY_KEEP)
            .unwrap()
            .is_some());
    }

    #[test]
    fn config_validation() {
        let ok = AddressHistoryConfig {
            addresses: vec![ALICE.into()],
            start_block: None,
            end_block: None,
            poll_interval: None,
            mirror: None,
            mev_relays: false,
            request_deadline: None,
            validators: vec![],
            beacon_url: None,
            sweep: None,
        };
        ok.validate().unwrap();
        assert_eq!(ok.poll_interval().unwrap(), DEFAULT_POLL_INTERVAL);
        let bad = |c: AddressHistoryConfig| assert!(c.validate().is_err(), "{c:?}");
        // No addresses is valid: rotki registers its own through /api/watch.
        AddressHistoryConfig {
            addresses: vec![],
            ..ok.clone()
        }
        .validate()
        .unwrap();
        bad(AddressHistoryConfig {
            addresses: vec![ALICE.into(), ALICE.to_lowercase()],
            ..ok.clone()
        });
        bad(AddressHistoryConfig {
            start_block: Some(10),
            end_block: Some(5),
            ..ok.clone()
        });
        bad(AddressHistoryConfig {
            poll_interval: Some("0s".into()),
            ..ok.clone()
        });
        let mirror = MirrorConfig {
            url: "https://example.org".into(),
            chain_id: 1,
            from_block: 10,
            to_block: Some(20),
            cache_mb: MIN_CACHE_MB,
        };
        AddressHistoryConfig {
            mirror: Some(mirror.clone()),
            ..ok.clone()
        }
        .validate()
        .unwrap();
        bad(AddressHistoryConfig {
            mirror: Some(MirrorConfig {
                to_block: Some(9),
                ..mirror.clone()
            }),
            ..ok.clone()
        });
        bad(AddressHistoryConfig {
            mirror: Some(MirrorConfig {
                cache_mb: MIN_CACHE_MB - 1,
                ..mirror
            }),
            ..ok.clone()
        });
    }

    fn open_ended(h: &AddressHistory) -> Answer {
        h.page(
            Action::TxList,
            &PageRequest {
                end_block: None,
                ..req(0, 0, 1, 10)
            },
        )
        .unwrap()
    }

    /// An absent `endblock` answers everything covered from the start onward; a start that is not
    /// covered is still incomplete.
    #[test]
    fn an_absent_endblock_answers_everything_covered() {
        let (_d, h) = history();
        assert!(matches!(open_ended(&h), Answer::Incomplete(_)));
        h.mark_covered(Action::TxList, ALICE, 0, 99).unwrap();
        assert_eq!(open_ended(&h), Answer::Rows(vec![]));
        h.mark_covered(Action::TxList, ALICE, 101, 200).unwrap();
        assert_eq!(
            open_ended(&h),
            Answer::Rows(vec![]),
            "through 99, the run the start is in"
        );
    }

    #[test]
    fn a_range_starting_past_the_head_is_incomplete_not_empty() {
        let (_d, h) = history();
        h.mark_covered(Action::TxList, ALICE, 0, 100).unwrap();
        h.set_head(100).unwrap();
        assert!(matches!(
            h.page(Action::TxList, &req(150, 99_999_999, 1, 10))
                .unwrap(),
            Answer::Incomplete(_)
        ));
        assert_eq!(
            h.page(Action::TxList, &req(50, 40, 1, 10)).unwrap(),
            Answer::Rows(vec![]),
            "an endblock before the startblock asks for nothing"
        );
    }

    /// A row stored without coverage (a fetch interrupted, or one whose span a reorg cut) must not
    /// survive a reorg and resurface once the range is covered again.
    #[test]
    fn a_reorg_drops_rows_that_had_no_coverage() {
        let (_d, h) = history();
        h.insert(Action::TxList, ALICE, &[tx(120, 0)]).unwrap();
        h.invalidate_above(100).unwrap();
        let g = h.generation().unwrap();
        h.record(Action::TxList, ALICE, &[], (0, 150), g).unwrap();
        assert_eq!(
            h.page(Action::TxList, &req(0, 150, 1, 10)).unwrap(),
            Answer::Rows(vec![])
        );
    }

    #[test]
    fn a_fetch_that_straddles_a_reorg_or_unwatch_is_refused() {
        let (_d, h) = history();
        let g = h.generation().unwrap();
        h.invalidate_above(100).unwrap();
        let err = h
            .record(Action::TxList, ALICE, &[tx(90, 0)], (0, 100), g)
            .unwrap_err();
        assert!(
            err.to_string().contains("changed during the fetch"),
            "{err}"
        );
        assert!(h.coverage(Action::TxList, ALICE).unwrap().is_empty());

        h.watch(BOB).unwrap();
        let g = h.generation().unwrap();
        h.unwatch(BOB).unwrap();
        assert!(
            h.record(Action::TxList, BOB, &[], (0, 1), g).is_err(),
            "unwatched"
        );
        let g = h.generation().unwrap();
        h.watch(BOB).unwrap();
        assert!(
            h.record(Action::TxList, BOB, &[tx(5, 0)], (6, 9), g)
                .is_err(),
            "a row outside the span it claims"
        );
        h.record(Action::TxList, BOB, &[tx(5, 0)], (0, 9), g)
            .unwrap();
    }

    #[test]
    fn a_later_page_from_an_older_generation_is_incomplete() {
        let (_d, h) = history();
        let rows: Vec<Row> = (0..3).map(|i| tx(10 + i, 0)).collect();
        let g = h.generation().unwrap();
        h.record(Action::TxList, ALICE, &rows, (0, 20), g).unwrap();
        let q = |page: &str, generation: Option<&str>| {
            let mut p = vec![
                ("module", "account"),
                ("action", "txlist"),
                ("address", ALICE),
                ("startblock", "0"),
                ("endblock", "20"),
                ("offset", "2"),
                ("page", page),
            ];
            if let Some(g) = generation {
                p.push(("generation", g));
            }
            respond(Some(&h), &params(&p))
        };
        let first = q("1", None);
        let seen = first["generation"].as_str().unwrap().to_string();
        assert_eq!(hashes(&q("2", Some(&seen))).len(), 1);
        // The reorg is refetched before page 2, so coverage is whole again and only the generation
        // shows that page 1 was cut from rows that no longer exist.
        h.invalidate_above(10).unwrap();
        let g = h.generation().unwrap();
        let refetched: Vec<Row> = (0..3).map(|i| tx(11 + i, 1)).collect();
        h.record(Action::TxList, ALICE, &refetched, (11, 20), g)
            .unwrap();
        assert_eq!(h.coverage(Action::TxList, ALICE).unwrap(), vec![(0, 20)]);
        let second = q("2", Some(&seen));
        assert!(
            second["result"]
                .as_str()
                .unwrap()
                .starts_with("NUTHATCH_INCOMPLETE:"),
            "{second}"
        );
    }

    /// A deposit is value that arrived: a failed transaction, a frame inside a reverted call, a zero
    /// transfer and a `callcode` frame are served as Etherscan serves them, and are not deposits.
    #[test]
    fn the_feed_carries_only_value_that_arrived() {
        let (_d, h) = history();
        let g = h.generation().unwrap();
        let set = [ALICE.to_string()];
        let to_alice = |block: u64, i: u64, fields: &[(&str, &str)]| {
            let mut r = tx(block, i);
            r.record
                .insert("to".into(), Value::String(ALICE.to_ascii_lowercase()));
            for (k, v) in fields {
                r.record.insert((*k).into(), Value::String((*v).into()));
            }
            r
        };
        let callcode = to_alice(105, 4, &[("value", "7"), ("isError", "0")]);
        let mut found = std::collections::BTreeMap::new();
        found.insert(
            ALICE.to_ascii_lowercase(),
            crate::address_discovery::Found {
                txlist: vec![
                    to_alice(
                        105,
                        0,
                        &[("value", "5"), ("isError", "0"), ("txreceipt_status", "1")],
                    ),
                    to_alice(
                        105,
                        1,
                        &[("value", "5"), ("isError", "1"), ("txreceipt_status", "0")],
                    ),
                    to_alice(105, 2, &[("value", "0"), ("isError", "0")]),
                ],
                txlistinternal: vec![
                    to_alice(105, 3, &[("value", "6"), ("isError", "1")]),
                    callcode.clone(),
                ],
                tokentx: vec![
                    to_alice(105, 5, &[("value", "0")]),
                    to_alice(105, 6, &[("value", "9")]),
                ],
                callcode: [(callcode.block, callcode.tx_index, callcode.position)].into(),
                ..Default::default()
            },
        );
        h.record_sweep(&found, &set, (100, 110), g, None, true)
            .unwrap();
        let feed: Vec<(String, u64)> = h
            .deposits_after(0, 100)
            .unwrap()
            .iter()
            .map(|e| {
                // tx() writes the transaction index into the hash's last digits.
                let hash = e["row"]["hash"].as_str().unwrap();
                let i = u64::from_str_radix(&hash[hash.len() - 2..], 16).unwrap();
                (e["action"].as_str().unwrap().into(), i)
            })
            .collect();
        assert_eq!(feed, [("txlist".into(), 0), ("tokentx".into(), 6)]);
        let served = respond(
            Some(&h),
            &params(&[
                ("module", "account"),
                ("action", "txlist"),
                ("address", ALICE),
                ("startblock", "100"),
                ("endblock", "110"),
            ]),
        );
        assert_eq!(
            served["result"].as_array().unwrap().len(),
            3,
            "txlist still lists all three"
        );
    }

    /// An address marked unprovable from a block answers unsupported for any page reaching it, is
    /// served below it, keeps the earliest mark, and loses a mark a reorg took the block of.
    #[test]
    fn an_unprovable_mark_refuses_pages_from_its_block() {
        let (_d, h) = history();
        let g = h.generation().unwrap();
        h.record(Action::TxList, ALICE, &[tx(10, 0)], (0, 99), g)
            .unwrap();
        let page = |end: Option<u64>| {
            h.page(
                Action::TxList,
                &PageRequest {
                    address: ALICE.into(),
                    start_block: Some(0),
                    end_block: end,
                    sort: Sort::Asc,
                    page: 1,
                    offset: 100,
                    generation: None,
                },
            )
            .unwrap()
        };
        h.mark_unprovable(ALICE, 80).unwrap();
        h.mark_unprovable(ALICE, 90).unwrap();
        assert_eq!(
            h.unprovable_from(ALICE).unwrap(),
            Some(80),
            "the earliest mark wins"
        );
        assert!(matches!(page(None), Answer::Unsupported(_)));
        assert!(matches!(page(Some(80)), Answer::Unsupported(_)));
        assert!(matches!(page(Some(79)), Answer::Rows(r) if r.len() == 1));
        h.invalidate_above(79).unwrap();
        assert_eq!(
            h.unprovable_from(ALICE).unwrap(),
            None,
            "a reorg below the mark removes it"
        );
    }

    /// RFC-0064 S3: the feed carries each incoming row once as a deposit, a reorg that takes it back
    /// as one retraction naming it, and the replacement as a new deposit; nothing is retracted twice.
    #[test]
    fn the_feed_carries_deposits_and_retracts_each_once() {
        let (_d, h) = history();
        let g = h.generation().unwrap();
        let set = [ALICE.to_string()];
        let row = |to: &str, block: u64, i: u64| {
            let mut r = tx(block, i);
            r.record.insert("to".into(), Value::String(to.into()));
            r
        };
        let found = |rows: Vec<Row>| {
            let mut f = std::collections::BTreeMap::new();
            f.insert(
                ALICE.to_ascii_lowercase(),
                crate::address_discovery::Found {
                    txlist: rows,
                    ..Default::default()
                },
            );
            f
        };
        let deposit_and_send = |block| {
            found(vec![
                row(&ALICE.to_ascii_lowercase(), block, 0),
                row(BOB, block, 1),
            ])
        };
        h.record_sweep(&deposit_and_send(105), &set, (100, 110), g, None, false)
            .unwrap();
        let feed = h.deposits_after(0, 100).unwrap();
        assert_eq!(feed.len(), 1, "the incoming row only: {feed:?}");
        assert_eq!(feed[0]["event"], "deposit");
        assert_eq!(feed[0]["seq"], 1);

        h.invalidate_above(100).unwrap();
        let g = h.generation().unwrap();
        // The replacement lands elsewhere, as one usually does after a reorg.
        h.record_sweep(&deposit_and_send(106), &set, (101, 110), g, None, false)
            .unwrap();
        h.invalidate_above(100).unwrap();
        let events: Vec<(String, Option<u64>)> = h
            .deposits_after(0, 100)
            .unwrap()
            .iter()
            .map(|e| (e["event"].as_str().unwrap().into(), e["retracts"].as_u64()))
            .collect();
        assert_eq!(
            events,
            [
                ("deposit".into(), None),
                ("retraction".into(), Some(1)),
                ("deposit".into(), None),
                ("retraction".into(), Some(3)),
            ]
        );
        let mut q = std::collections::HashMap::new();
        for (k, v) in [
            ("module", "account"),
            ("action", "nuthatchDeposits"),
            ("after", "2"),
        ] {
            q.insert(k.to_string(), v.to_string());
        }
        let answer = respond(Some(&h), &q);
        assert_eq!(answer["status"], "1", "{answer}");
        assert_eq!(answer["result"].as_array().unwrap().len(), 2);
        assert_eq!(answer["result"][0]["seq"], 3);
    }

    /// A deposit is value that arrived: a watched address sending to itself is not one, and a
    /// creation carrying value into a watched contract is (#2121, #2115).
    #[test]
    fn a_send_to_oneself_is_no_deposit_and_a_creation_is_one() {
        let (_d, h) = history();
        let g = h.generation().unwrap();
        let alice = ALICE.to_ascii_lowercase();
        let row = |i: u64, fields: &[(&str, &str)]| {
            let mut r = tx(105, i);
            for (k, v) in fields {
                r.record.insert((*k).into(), Value::String((*v).into()));
            }
            r
        };
        let mut found = std::collections::BTreeMap::new();
        found.insert(
            alice.clone(),
            crate::address_discovery::Found {
                txlist: vec![
                    row(0, &[("from", &alice), ("to", &alice), ("value", "1")]),
                    row(
                        1,
                        &[
                            ("from", BOB),
                            ("to", ""),
                            ("contractAddress", &alice),
                            ("value", "1"),
                        ],
                    ),
                ],
                tokentx: vec![row(2, &[("from", &alice), ("to", &alice), ("value", "1")])],
                ..Default::default()
            },
        );
        h.record_sweep(&found, &[ALICE.to_string()], (100, 110), g, None, false)
            .unwrap();
        let feed = h.deposits_after(0, 100).unwrap();
        let rows: Vec<&Value> = feed.iter().map(|e| &e["row"]["hash"]).collect();
        assert_eq!(rows, [&found[&alice].txlist[1].record["hash"]], "{feed:?}");
    }

    /// `sweptThrough` is read with the page and claims nothing for a page that stops short of the
    /// feed's end, where a later entry can name an earlier block (#2122).
    #[test]
    fn swept_through_is_claimed_only_by_a_page_that_reaches_the_feed_end() {
        let (_d, h) = history();
        let g = h.generation().unwrap();
        let mut found = std::collections::BTreeMap::new();
        found.insert(
            ALICE.to_ascii_lowercase(),
            crate::address_discovery::Found {
                txlist: [0, 1, 2]
                    .map(|i| {
                        let mut r = tx(105, i);
                        r.record.insert("to".into(), Value::String(ALICE.into()));
                        r
                    })
                    .into(),
                ..Default::default()
            },
        );
        h.record_sweep(&found, &[ALICE.to_string()], (100, 110), g, None, false)
            .unwrap();
        let page = |after: &str, offset: &str| {
            respond(
                Some(&h),
                &params(&[
                    ("module", "account"),
                    ("action", "nuthatchDeposits"),
                    ("after", after),
                    ("offset", offset),
                ]),
            )
        };
        let short = page("0", "2");
        assert_eq!(short["result"].as_array().unwrap().len(), 2, "{short}");
        assert_eq!(short["sweptThrough"], Value::Null, "{short}");
        for (after, offset) in [("0", "3"), ("2", "2")] {
            let end = page(after, offset);
            assert_eq!(end["sweptThrough"], 110, "{end}");
        }
    }

    /// RFC-0064 S2: internal transactions are claimed only over windows the sweep traced, so turning
    /// tracing on later claims nothing for what was swept before it; a reorg cuts both spans.
    #[test]
    fn internal_coverage_is_claimed_only_where_the_sweep_traced() {
        let (_d, h) = history();
        let g = h.generation().unwrap();
        let set = [ALICE.to_string()];
        h.record_sweep(&Default::default(), &set, (100, 110), g, None, false)
            .unwrap();
        assert_eq!(h.coverage(Action::TxList, ALICE).unwrap(), vec![(100, 110)]);
        assert!(h
            .coverage(Action::TxListInternal, ALICE)
            .unwrap()
            .is_empty());
        h.record_sweep(&Default::default(), &set, (111, 120), g, None, true)
            .unwrap();
        assert_eq!(
            h.coverage(Action::TxListInternal, ALICE).unwrap(),
            vec![(111, 120)]
        );
        h.invalidate_above(115).unwrap();
        assert_eq!(
            h.coverage(Action::TxListInternal, ALICE).unwrap(),
            vec![(111, 115)]
        );
    }

    /// RFC-0064 S1: the sweep's span is held once and covers every swept address from the block it
    /// joined; a gap is refused, and a reorg cuts the shared span as it cuts any other.
    #[test]
    fn a_sweep_covers_every_address_from_the_block_it_joined() {
        const CAROL: &str = "0x00000000000000000000000000000000000000ca";
        let (_d, h) = history();
        h.watch(BOB).unwrap();
        let g = h.generation().unwrap();
        let mut found = std::collections::BTreeMap::new();
        found.insert(
            ALICE.to_ascii_lowercase(),
            crate::address_discovery::Found {
                txlist: vec![tx(105, 0)],
                ..Default::default()
            },
        );
        let first = [ALICE.to_string(), BOB.to_string()];
        h.record_sweep(&found, &first, (100, 110), g, None, false)
            .unwrap();
        for who in [ALICE, BOB] {
            assert_eq!(h.coverage(Action::TxList, who).unwrap(), vec![(100, 110)]);
            assert_eq!(
                h.coverage(Action::Token1155Tx, who).unwrap(),
                vec![(100, 110)]
            );
        }
        assert!(
            h.coverage(Action::TxListInternal, ALICE)
                .unwrap()
                .is_empty(),
            "internal transactions are not swept yet"
        );

        h.watch(CAROL).unwrap();
        let all = [ALICE.to_string(), BOB.to_string(), CAROL.to_string()];
        h.record_sweep(&Default::default(), &all, (111, 120), g, None, false)
            .unwrap();
        assert_eq!(h.coverage(Action::TxList, ALICE).unwrap(), vec![(100, 120)]);
        assert_eq!(
            h.coverage(Action::TxList, CAROL).unwrap(),
            vec![(111, 120)],
            "nothing before the block it joined is claimed"
        );
        assert!(h
            .record_sweep(&Default::default(), &all, (130, 140), g, None, false)
            .is_err());

        h.invalidate_above(115).unwrap();
        assert_eq!(h.swept().unwrap(), Some((100, 115)));
        assert_eq!(h.coverage(Action::TxList, CAROL).unwrap(), vec![(111, 115)]);
    }

    #[test]
    fn a_chainid_for_another_chain_is_unsupported() {
        let (_d, h) = history();
        h.mark_covered(Action::TxList, ALICE, 0, 10).unwrap();
        let q = |chain: &str| {
            respond(
                Some(&h),
                &params(&[
                    ("chainid", chain),
                    ("module", "account"),
                    ("action", "txlist"),
                    ("address", ALICE),
                    ("endblock", "10"),
                ]),
            )
        };
        assert_eq!(q("1")["status"], "1");
        for other in ["8453", "x"] {
            let a = q(other);
            assert!(
                a["result"]
                    .as_str()
                    .unwrap()
                    .starts_with("NUTHATCH_UNSUPPORTED:"),
                "{other}: {a}"
            );
        }
    }

    #[test]
    fn optimism_before_bedrock_is_unsupported_even_when_covered() {
        let dir = tempfile::tempdir().unwrap();
        let store = Store::open(&dir.path().join("t.redb")).unwrap();
        let h = AddressHistory::open(store, 10, &[ALICE.into()]).unwrap();
        let bedrock = traced_from(10);
        h.mark_covered(Action::TxList, ALICE, 0, bedrock + 10)
            .unwrap();
        let q = |start: u64| {
            respond(
                Some(&h),
                &params(&[
                    ("module", "account"),
                    ("action", "txlist"),
                    ("address", ALICE),
                    ("startblock", &start.to_string()),
                    ("endblock", &(bedrock + 10).to_string()),
                ]),
            )
        };
        assert_eq!(q(bedrock)["status"], "1");
        for before in [0, bedrock - 1] {
            let a = q(before);
            assert!(
                a["result"]
                    .as_str()
                    .unwrap()
                    .starts_with("NUTHATCH_UNSUPPORTED:"),
                "{before}: {a}"
            );
        }
    }

    #[test]
    fn a_varint_past_128_bits_is_refused() {
        let mut max = Vec::new();
        put_varint(&mut max, u128::MAX);
        assert_eq!(take_varint(&mut max.as_slice()).unwrap(), u128::MAX);
        let mut over = vec![0x80u8; 18];
        over.push(0x04);
        assert!(take_varint(&mut over.as_slice()).is_err());
    }

    /// Writes a rotki-mode nest holding about 10,000 rows into `$NUTHATCH_AH_FIXTURE`, for measuring
    /// a real process: `NUTHATCH_AH_FIXTURE=/tmp/nest cargo test --lib write_measurement_fixture -- --ignored`.
    #[test]
    #[ignore]
    fn write_measurement_fixture() {
        let dir = std::path::PathBuf::from(std::env::var("NUTHATCH_AH_FIXTURE").unwrap());
        std::fs::create_dir_all(&dir).unwrap();
        std::fs::write(
            dir.join(crate::config::CONFIG_FILE),
            format!(
                "[nest]\nname = \"rotki\"\nchain = \"mainnet\"\nchain_id = 1\n\
                 rpc_urls = [\"https://ethereum-rpc.publicnode.com\"]\n\n\
                 [address_history]\naddresses = [\"{ALICE}\", \"{BOB}\"]\n"
            ),
        )
        .unwrap();
        let store = Store::open(&dir.join(crate::config::DB_FILE)).unwrap();
        let h = AddressHistory::open(store, 1, &[ALICE.into(), BOB.into()]).unwrap();
        for (who, action, n) in [
            (ALICE, Action::TxList, 4_000u64),
            (ALICE, Action::TokenTx, 3_000),
            (ALICE, Action::TxListInternal, 1_000),
            (BOB, Action::TxList, 2_000),
        ] {
            let rows: Vec<Row> = (0..n).map(|i| tx(18_000_000 + i * 97, i % 7)).collect();
            h.insert(action, who, &rows).unwrap();
            h.mark_covered(action, who, 0, 20_000_000).unwrap();
        }
        h.set_head(20_000_000).unwrap();
    }

    /// Times a reorg and the watched-set reads at `$NUTHATCH_AH_WATCHED` addresses (default
    /// 1,000,000, RFC-0064's size), for #2117:
    /// `cargo test --release --lib measure_a_reorg_at_scale -- --ignored --nocapture`.
    #[test]
    #[ignore]
    fn measure_a_reorg_at_scale() {
        let n: usize =
            std::env::var("NUTHATCH_AH_WATCHED").map_or(1_000_000, |v| v.parse().unwrap());
        let dir = tempfile::tempdir().unwrap();
        let h =
            AddressHistory::open(Store::open(&dir.path().join("t.redb")).unwrap(), 1, &[]).unwrap();
        let addrs: Vec<String> = (1..=n).map(|i| format!("0x{i:040x}")).collect();
        h.watch_many(&addrs).unwrap();
        let head = 20_000_000;
        h.finalize(head).unwrap();
        for (i, a) in addrs.iter().take(1_000).enumerate() {
            let rows: Vec<Row> = (0..20)
                .map(|j| tx(head - 20_000 + j * 1_000, i as u64))
                .collect();
            h.insert(Action::TxList, a, &rows).unwrap();
        }
        for a in addrs.iter().take(100) {
            h.insert(Action::TokenTx, a, &[tx(head + 5, 0)]).unwrap();
        }
        let g = h.generation().unwrap();
        let txs: Vec<(Vec<u8>, Map<String, Value>)> = (0..200_000u64)
            .map(|i| (i.to_be_bytes().repeat(4), tx(head - 200_000 + i, 0).record))
            .chain((0..100u64).map(|i| {
                (
                    (u64::MAX - i).to_be_bytes().repeat(4),
                    tx(head + 5, 0).record,
                )
            }))
            .collect();
        h.persist_pending(&txs, &[], &[], &[], &[], g, None, true)
            .unwrap();
        let t = std::time::Instant::now();
        let w = h.watched().unwrap();
        println!("watched(): {} addresses in {:?}", w.len(), t.elapsed());
        let t = std::time::Instant::now();
        assert!(h.is_watched(&addrs[n / 2]).unwrap());
        println!("is_watched(): {:?}", t.elapsed());
        let t = std::time::Instant::now();
        assert_eq!(h.watched_count().unwrap(), n as u64);
        println!("watched_count(): {:?}", t.elapsed());
        let t = std::time::Instant::now();
        h.invalidate_above(head + 2).unwrap();
        println!("invalidate_above, {n} watched: {:?}", t.elapsed());
        assert!(h
            .cached_tx(&u64::MAX.to_be_bytes().repeat(4))
            .unwrap()
            .is_none());
        assert!(h
            .cached_tx(&0u64.to_be_bytes().repeat(4))
            .unwrap()
            .is_some());
        let t = std::time::Instant::now();
        let rows = h
            .rows_in(Action::TokenTx, &addrs[0], (0, u64::MAX))
            .unwrap();
        assert!(rows.is_empty(), "{rows:?}");
        println!("check: {:?}", t.elapsed());
    }
}
