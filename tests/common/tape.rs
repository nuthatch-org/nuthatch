//! `TapeSource` - a scripted, mutable chain the test drives, plus the fixtures/helpers around it.
//!
//! Why this exists: the crate's other test doubles all answer `block_hash -> None`, which makes
//! `detect_reorg` a no-op - so reorgs are untestable through them. `TapeSource` answers `block_hash`
//! with the *current canonical* hash (`Some`), supplies non-zero timestamps (a zero-timestamp window
//! is refused and retried forever), and exposes `finalized()` as the sealing trigger the test controls
//! precisely. That's the whole point: it lets an integration test drive the real `spawn_nest` /
//! `spawn_roost` loop through land → seal → reorg → halt without a network or a wall-clock race.

#![allow(dead_code)]

use std::collections::{BTreeMap, HashMap};
use std::path::Path;
use std::sync::Mutex;
use std::time::{Duration, Instant};

use anyhow::Result;
use async_trait::async_trait;
use nuthatch::config::Config;
use nuthatch::rpc::Log;
use nuthatch::source::Source;

/// The canonical ERC-20 `Transfer(address,address,uint256)` topic0.
pub const TRANSFER_TOPIC0: &str =
    "0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef";

/// The example USDC address (Arbitrum One) - reused so tests exercise real checksummed addresses.
pub const USDC: &str = "0xaf88d065e77c8cC2239327C5EDb3A432268e5831";
/// The example ARB address (Arbitrum One).
pub const ARB: &str = "0x912CE59144191C1204E64559FE8253a0e49E6548";

/// A deterministic 20-byte account address from a small id, e.g. `account(1)` → `0x0000…0001`.
pub fn account(id: u64) -> String {
    format!("0x{id:040x}")
}

/// A deterministic 32-byte block hash for `(number, variant)`. `variant` lets a reorg mint a distinct
/// hash for the same height (`variant = 0` canonical, `1` the first replacement, …).
pub fn block_hash(number: u64, variant: u64) -> String {
    format!("0x{:032x}{:032x}", variant, number)
}

// ---------------------------------------------------------------------------
// Fixtures
// ---------------------------------------------------------------------------

/// One block on the tape: its canonical hash, header timestamp (non-zero!), and emitted logs.
#[derive(Clone, Debug)]
pub struct BlockFixture {
    pub hash: String,
    pub timestamp: u64,
    pub logs: Vec<Log>,
}

/// Build an ERC-20 `Transfer` log. Topics are `[topic0, from(32B), to(32B)]`; `data` is the 32-byte
/// big-endian value - exactly the on-wire shape the decode registry parses.
pub fn transfer_log(
    address: &str,
    block: u64,
    log_index: u64,
    block_hash: &str,
    from: &str,
    to: &str,
    value: u128,
) -> Log {
    Log {
        address: address.to_string(),
        topics: vec![
            TRANSFER_TOPIC0.to_string(),
            topic_address(from),
            topic_address(to),
        ],
        data: format!("0x{value:064x}"),
        block_number: block,
        block_hash: block_hash.to_string(),
        // Deterministic from (block, log_index) - no wall clock.
        tx_hash: format!("0x{:064x}", (block << 20) | log_index),
        log_index,
    }
}

/// A 20-byte address left-padded to a 32-byte indexed-topic word.
fn topic_address(addr: &str) -> String {
    let h = addr.trim_start_matches("0x");
    format!("0x{h:0>64}")
}

/// A block carrying `transfers` (`(from, to, value)`), log-indexed `0..n`, for one emitting `address`.
pub fn transfers_block(
    number: u64,
    variant: u64,
    timestamp: u64,
    address: &str,
    transfers: &[(&str, &str, u128)],
) -> BlockFixture {
    let hash = block_hash(number, variant);
    let logs = transfers
        .iter()
        .enumerate()
        .map(|(i, (from, to, value))| {
            transfer_log(address, number, i as u64, &hash, from, to, *value)
        })
        .collect();
    BlockFixture {
        hash,
        timestamp,
        logs,
    }
}

/// topic0 of Uniswap-V2 `Sync(uint112 reserve0, uint112 reserve1)` (keccak of the canonical signature).
pub const SYNC_TOPIC0: &str = "0x1c411e9a96e071241c2f21f7726b17ae89e3cab4c78be50e062b03a9fffbbad1";

/// A Uniswap-V2 `Sync` log - no indexed params; `data` is the two reserves ABI-encoded (two 32B words).
pub fn sync_log(
    address: &str,
    block: u64,
    log_index: u64,
    block_hash: &str,
    reserve0: u128,
    reserve1: u128,
) -> Log {
    Log {
        address: address.to_string(),
        topics: vec![SYNC_TOPIC0.to_string()],
        data: format!("0x{reserve0:064x}{reserve1:064x}"),
        block_number: block,
        block_hash: block_hash.to_string(),
        tx_hash: format!("0x{:064x}", (block << 20) | log_index),
        log_index,
    }
}

/// A block emitting `syncs` (`(reserve0, reserve1)`) for one pair `address`, log-indexed `0..n`.
pub fn sync_block(
    number: u64,
    variant: u64,
    timestamp: u64,
    address: &str,
    syncs: &[(u128, u128)],
) -> BlockFixture {
    let hash = block_hash(number, variant);
    let logs = syncs
        .iter()
        .enumerate()
        .map(|(i, (r0, r1))| sync_log(address, number, i as u64, &hash, *r0, *r1))
        .collect();
    BlockFixture {
        hash,
        timestamp,
        logs,
    }
}

/// A block that emits nothing (used to advance the tip past the sealing point without adding rows).
pub fn empty_block(number: u64, variant: u64, timestamp: u64) -> BlockFixture {
    BlockFixture {
        hash: block_hash(number, variant),
        timestamp,
        logs: Vec::new(),
    }
}

/// Counterparties for the zero-value Transfer logs [`pad_address_to_seal`] appends.
/// Recipe and restart tests must not query these: the pads exist only to cross
/// [`nuthatch::indexer::SEAL_DIRECT_BATCH`].
pub fn seal_pad_from() -> String {
    account(10_000)
}
pub fn seal_pad_to() -> String {
    account(10_001)
}

/// Append zero-value Transfer logs for `address` so a nest that only indexes that
/// address will cut at this block once it is finalized, given `rows_before` already
/// indexed rows of this address in the range being sealed.
///
/// The tip path holds below [`nuthatch::indexer::SEAL_DIRECT_BATCH`] (#1067). Tests that
/// need a sealed watermark, or real Parquet, have to cross that threshold with rows the
/// nest will actually decode. Zero-value pads between [`seal_pad_from`] and [`seal_pad_to`]
/// do not move balances, supply, or holder counts.
pub fn pad_address_to_seal(fx: &mut BlockFixture, address: &str, rows_before: usize) {
    let already_here = fx
        .logs
        .iter()
        .filter(|l| l.address.eq_ignore_ascii_case(address))
        .count();
    let need = nuthatch::indexer::SEAL_DIRECT_BATCH
        .saturating_sub(rows_before.saturating_add(already_here));
    if need == 0 {
        return;
    }
    let from = seal_pad_from();
    let to = seal_pad_to();
    let block_number = fx.logs.first().map(|l| l.block_number).unwrap_or(0);
    let start = fx.logs.len() as u64;
    for i in 0..need {
        let log_index = start + i as u64;
        fx.logs.push(transfer_log(
            address,
            block_number,
            log_index,
            &fx.hash,
            &from,
            &to,
            0,
        ));
    }
}

/// Append copies of the last log so this block crosses the tip-path batch threshold.
/// Used for Sync (and anything else whose latest-row semantics must survive padding):
/// the real last log stays last if the caller pads *from a clone of it*, so we copy it
/// and the latest value does not change.
pub fn pad_clone_last_to_seal(fx: &mut BlockFixture, rows_before: usize) {
    let need = nuthatch::indexer::SEAL_DIRECT_BATCH
        .saturating_sub(rows_before.saturating_add(fx.logs.len()));
    if need == 0 {
        return;
    }
    let template = fx
        .logs
        .last()
        .cloned()
        .expect("pad_clone_last_to_seal needs an existing log");
    let start = fx.logs.len() as u64;
    for i in 0..need {
        let mut log = template.clone();
        log.log_index = start + i as u64;
        log.tx_hash = format!("0x{:064x}", (log.block_number << 20) | log.log_index);
        fx.logs.push(log);
    }
}

// ---------------------------------------------------------------------------
// TapeSource
// ---------------------------------------------------------------------------

struct Tape {
    blocks: BTreeMap<u64, BlockFixture>,
    tip: u64,
    finalized: u64,
}

/// A scripted chain with interior mutability. Test-facing mutators (`advance_tip_to`,
/// `advance_finalized_to`, `reorg`, `insert_block`) drive it; the [`Source`] impl is what the indexer
/// sees. A non-zero timestamp per block is a hard requirement - `process_window` refuses to commit a
/// zero-timestamp window and would retry forever.
pub struct TapeSource {
    inner: Mutex<Tape>,
    /// How many `logs` calls this tape has answered.
    ///
    /// The point of a shared dataset (RFC-0032 §4) is that it is fetched **once**, and the only way
    /// to prove that is to count what the source was asked for. A test that merely checks both
    /// aliases return the same rows would pass just as happily with two identical backfills running
    /// side by side - which is the bug, not the fix.
    logs_calls: std::sync::atomic::AtomicUsize,
    /// Every `logs` call's `(from, to)`, in order - see [`TapeSource::logs_ranges`].
    logs_windows: std::sync::Mutex<Vec<(u64, u64)>>,
    /// When set, every [`Source`] call fails - the tape is "dark".
    ///
    /// This models the ordinary operational fault, not an exotic one: every RPC endpoint for this
    /// chain is unreachable, which is what `escalate_stall` exists to report. It fails *all* the
    /// methods rather than only `tip`, because an endpoint that cannot answer `eth_blockNumber`
    /// cannot answer `eth_getLogs` either, and a knob that darkened only the tip poll would leave
    /// the test asserting against a chain state no provider outage produces.
    dark: std::sync::atomic::AtomicBool,
    /// `l1BlockNumber` by block hash, so a reorged block can report a different one.
    l1_by_hash: Mutex<HashMap<String, u64>>,
    /// Every block number `block_headers` was asked for.
    headers_asked: Mutex<Vec<u64>>,
}

impl Default for TapeSource {
    fn default() -> Self {
        Self::new()
    }
}

impl TapeSource {
    pub fn new() -> Self {
        Self {
            inner: Mutex::new(Tape {
                blocks: BTreeMap::new(),
                tip: 0,
                finalized: 0,
            }),
            logs_calls: std::sync::atomic::AtomicUsize::new(0),
            logs_windows: std::sync::Mutex::new(Vec::new()),
            dark: std::sync::atomic::AtomicBool::new(false),
            l1_by_hash: Mutex::new(HashMap::new()),
            headers_asked: Mutex::new(Vec::new()),
        }
    }

    /// Have the header of the block with `hash` report `l1BlockNumber = l1`, as Arbitrum's do.
    pub fn set_l1_block_number(&self, hash: &str, l1: u64) {
        self.l1_by_hash.lock().unwrap().insert(hash.to_string(), l1);
    }

    /// Every block number a `block_headers` call has asked for, in order.
    pub fn headers_asked(&self) -> Vec<u64> {
        self.headers_asked.lock().unwrap().clone()
    }

    /// How many `logs` calls have been made against this tape.
    pub fn logs_call_count(&self) -> usize {
        self.logs_calls.load(std::sync::atomic::Ordering::Relaxed)
    }

    /// The `(from, to)` of every `logs` call, in order.
    ///
    /// A bare count cannot tell a *duplicate* fetch from an extra tip-following window, and once the
    /// cursor is paced by `--poll-interval` (#1190) the number of following windows before a test's
    /// wait predicate flips is a fact about the machine's load, not about the code. The ranges say
    /// which blocks were actually asked for, which is what a "did it backfill twice" guard means.
    pub fn logs_ranges(&self) -> Vec<(u64, u64)> {
        self.logs_windows.lock().unwrap().clone()
    }

    /// Take this chain's provider offline: every subsequent [`Source`] call returns an error, exactly
    /// as a cursor whose whole endpoint set is unreachable sees. The tape keeps its blocks, so
    /// [`restore`](Self::restore) brings the same chain back rather than a different one.
    pub fn go_dark(&self) {
        self.dark.store(true, std::sync::atomic::Ordering::SeqCst);
    }

    /// Bring the provider back. `escalate_stall` promises indexing "resumes automatically when an
    /// endpoint recovers"; this is what lets a test hold it to that.
    pub fn restore(&self) {
        self.dark.store(false, std::sync::atomic::Ordering::SeqCst);
    }

    /// `Err` while this tape is dark, so each `Source` method can start with `self.online()?`.
    fn online(&self) -> Result<()> {
        if self.dark.load(std::sync::atomic::Ordering::SeqCst) {
            anyhow::bail!("all RPC endpoints unreachable (tape is dark)");
        }
        Ok(())
    }

    /// Insert/overwrite a block. Raises the tip to `number` if it was behind (does not lower it).
    pub fn insert_block(&self, number: u64, fixture: BlockFixture) {
        let mut t = self.inner.lock().unwrap();
        t.blocks.insert(number, fixture);
        if number > t.tip {
            t.tip = number;
        }
    }

    /// Set the chain tip (the highest block the source will serve).
    pub fn advance_tip_to(&self, n: u64) {
        self.inner.lock().unwrap().tip = n;
    }

    /// Set the `finalized` block - the sealing trigger. `seal_ceiling` takes the FinalizedTag branch on
    /// an L2 chain (arbitrum-one/base), so this is exactly how far the test lets sealing advance.
    pub fn advance_finalized_to(&self, n: u64) {
        self.inner.lock().unwrap().finalized = n;
    }

    /// Rewrite the chain above `fork_block`: drop every block `> fork_block`, then install
    /// `replacement` as blocks `fork_block + 1, fork_block + 2, …` (new hashes + new logs). The tip
    /// becomes the highest replacement block. Blocks `<= fork_block` are untouched (the surviving
    /// ancestry `detect_reorg` walks back to).
    pub fn reorg(&self, fork_block: u64, replacement: Vec<BlockFixture>) {
        let mut t = self.inner.lock().unwrap();
        let above: Vec<u64> = t
            .blocks
            .range((fork_block + 1)..)
            .map(|(k, _)| *k)
            .collect();
        for k in above {
            t.blocks.remove(&k);
        }
        let mut tip = fork_block;
        for (offset, fixture) in replacement.into_iter().enumerate() {
            let number = fork_block + 1 + offset as u64;
            t.blocks.insert(number, fixture);
            tip = number;
        }
        t.tip = tip;
    }
}

#[async_trait]
impl Source for TapeSource {
    async fn tip(&self) -> Result<u64> {
        self.online()?;
        Ok(self.inner.lock().unwrap().tip)
    }

    async fn block_hash(&self, number: u64) -> Result<Option<String>> {
        self.online()?;
        Ok(self
            .inner
            .lock()
            .unwrap()
            .blocks
            .get(&number)
            .map(|b| b.hash.clone()))
    }

    async fn finalized(&self) -> Result<Option<u64>> {
        self.online()?;
        Ok(Some(self.inner.lock().unwrap().finalized))
    }

    async fn block_timestamps(&self, blocks: &[u64]) -> Result<HashMap<u64, u64>> {
        self.online()?;
        let t = self.inner.lock().unwrap();
        Ok(blocks
            .iter()
            .filter_map(|b| t.blocks.get(b).map(|f| (*b, f.timestamp)))
            .collect())
    }

    async fn block_headers(&self, blocks: &[u64]) -> Result<HashMap<u64, serde_json::Value>> {
        self.online()?;
        self.headers_asked.lock().unwrap().extend_from_slice(blocks);
        let t = self.inner.lock().unwrap();
        let l1 = self.l1_by_hash.lock().unwrap();
        Ok(blocks
            .iter()
            .filter_map(|b| {
                let f = t.blocks.get(b)?;
                let mut h = serde_json::json!({
                    "number": format!("0x{b:x}"),
                    "hash": f.hash,
                    "timestamp": format!("0x{:x}", f.timestamp),
                });
                if let Some(n) = l1.get(&f.hash) {
                    h["l1BlockNumber"] = serde_json::json!(format!("0x{n:x}"));
                }
                Some((*b, h))
            })
            .collect())
    }

    async fn logs(
        &self,
        filter: &nuthatch::source::LogFilter,
        from: u64,
        to: u64,
    ) -> Result<Vec<Log>> {
        let addresses = filter.addresses();
        let topic0s = filter.topic0s();
        self.online()?;
        self.logs_calls
            .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        self.logs_windows.lock().unwrap().push((from, to));
        let t = self.inner.lock().unwrap();
        let mut out = Vec::new();
        for (_, fixture) in t.blocks.range(from..=to) {
            for log in &fixture.logs {
                // Empty address list = "any address" (a real provider's factory / topic0-only fetch).
                let addr_ok = addresses.is_empty()
                    || addresses
                        .iter()
                        .any(|a| a.eq_ignore_ascii_case(&log.address));
                // Empty topic list = "any topic" (mirror a provider; the indexer always sends topics).
                let topic_ok = topic0s.is_empty()
                    || log
                        .topics
                        .first()
                        .map(|t0| topic0s.iter().any(|s| s.eq_ignore_ascii_case(t0)))
                        .unwrap_or(false);
                if addr_ok && topic_ok {
                    out.push(log.clone());
                }
            }
        }
        Ok(out)
    }
}

// ---------------------------------------------------------------------------
// Nest scaffolding - `init`'s OUTPUT without `init`'s network dependency.
// ---------------------------------------------------------------------------

/// Write a real nest into `dir`: a `nuthatch.toml` (arbitrum-one → FinalizedTag finality, so
/// `finalized()` controls sealing) plus `abis/erc20.json` copied from the vendored example ABI. Returns
/// the loaded [`Config`]. One `Transfer`-only ERC-20 contract at `address`, backfilling from block 1.
pub fn scaffold_nest(dir: &Path, name: &str, address: &str) -> Config {
    scaffold_nest_on_chain(dir, name, address, "arbitrum-one")
}

/// [`scaffold_nest`] on a named chain rather than the default `arbitrum-one`.
///
/// A multichain test needs its cursors to differ in the field the runtime keys them by: health
/// quarantines a *cursor* by chain name (`RuntimeHealth::quarantine_cursor`), so two nests scaffolded
/// on one chain share a cursor's fate whatever the test intended. `base` is the only other registered
/// chain with the same `FinalizedTag` finality, so sealing behaves identically on both.
pub fn scaffold_nest_on_chain(dir: &Path, name: &str, address: &str, chain: &str) -> Config {
    let chain_id = nuthatch::chains::lookup(chain)
        .unwrap_or_else(|| panic!("chain '{chain}' is not in the registry"))
        .chain_id;
    let abi_dir = dir.join("abis");
    std::fs::create_dir_all(&abi_dir).expect("create abis dir");
    let example_abi = concat!(env!("CARGO_MANIFEST_DIR"), "/tests/fixtures/erc20.json");
    let abi = std::fs::read(example_abi).expect("read erc20 abi fixture");
    std::fs::write(abi_dir.join("erc20.json"), abi).expect("write erc20 abi");

    let toml = format!(
        r#"[nest]
name = "{name}"
chain = "{chain}"
chain_id = {chain_id}
rpc_urls = []

[[contracts]]
alias = "{name}"
address = "{address}"
start_block = 1
abi = "abis/erc20.json"
events = ["Transfer"]
"#
    );
    std::fs::write(dir.join("nuthatch.toml"), toml).expect("write nuthatch.toml");
    Config::load(dir).expect("load scaffolded config")
}

/// [`scaffold_nest`], but with **two** independently-aliased ERC-20 `Transfer` contracts in one nest,
/// so the fixture has two tables (`{alias1}__transfer`, `{alias2}__transfer`) sharing one cursor.
/// Every hot-scan/cold-corruption fixture up to #472/#477 scaffolds exactly one contract, so "the
/// nest is degraded" and "this query's table is degraded" are the same set in every assertion - a
/// signal that names the wrong table, or every table, passes them all. This exists so a test can
/// degrade one alias and query the other.
pub fn scaffold_two_contract_nest(
    dir: &Path,
    name: &str,
    alias1: &str,
    address1: &str,
    alias2: &str,
    address2: &str,
) -> Config {
    let chain_id = nuthatch::chains::lookup("arbitrum-one")
        .expect("arbitrum-one is in the registry")
        .chain_id;
    let abi_dir = dir.join("abis");
    std::fs::create_dir_all(&abi_dir).expect("create abis dir");
    let example_abi = concat!(env!("CARGO_MANIFEST_DIR"), "/tests/fixtures/erc20.json");
    let abi = std::fs::read(example_abi).expect("read erc20 abi fixture");
    std::fs::write(abi_dir.join("erc20.json"), abi).expect("write erc20 abi");

    let toml = format!(
        r#"[nest]
name = "{name}"
chain = "arbitrum-one"
chain_id = {chain_id}
rpc_urls = []

[[contracts]]
alias = "{alias1}"
address = "{address1}"
start_block = 1
abi = "abis/erc20.json"
events = ["Transfer"]

[[contracts]]
alias = "{alias2}"
address = "{address2}"
start_block = 1
abi = "abis/erc20.json"
events = ["Transfer"]
"#
    );
    std::fs::write(dir.join("nuthatch.toml"), toml).expect("write nuthatch.toml");
    Config::load(dir).expect("load scaffolded two-contract config")
}

/// Scaffold a nest whose contract emits Uniswap-V2 `Sync` (for the `reserves` recipe). One `Sync`-only
/// pair contract at `address`, backfilling from block 1; the decoded table is `{alias}__sync`.
pub fn scaffold_pair_nest(dir: &Path, name: &str, address: &str) -> Config {
    let abi_dir = dir.join("abis");
    std::fs::create_dir_all(&abi_dir).expect("create abis dir");
    std::fs::write(
        abi_dir.join("pair.json"),
        r#"[{"type":"event","name":"Sync","anonymous":false,"inputs":[{"name":"reserve0","type":"uint112","indexed":false},{"name":"reserve1","type":"uint112","indexed":false}]}]"#,
    )
    .expect("write pair abi");
    let toml = format!(
        r#"[nest]
name = "{name}"
chain = "arbitrum-one"
chain_id = 42161
rpc_urls = []

[[contracts]]
alias = "{name}"
address = "{address}"
start_block = 1
abi = "abis/pair.json"
events = ["Sync"]
"#
    );
    std::fs::write(dir.join("nuthatch.toml"), toml).expect("write nuthatch.toml");
    Config::load(dir).expect("load scaffolded pair config")
}

/// The event table a nest named `name` decodes its transfers into (`{alias}__transfer`).
pub fn transfer_table(name: &str) -> String {
    format!("{name}__transfer")
}

// ---------------------------------------------------------------------------
// Polling - bounded waits on observable state (never a fixed sleep driving the loop).
// ---------------------------------------------------------------------------

/// Poll `cond` every 25 ms until it holds or `timeout` elapses. Returns whether it held. Used to
/// observe pipeline progress deterministically without racing on a fixed sleep.
pub async fn wait_until<F: FnMut() -> bool>(timeout: Duration, mut cond: F) -> bool {
    let start = Instant::now();
    loop {
        if cond() {
            return true;
        }
        if start.elapsed() >= timeout {
            return false;
        }
        tokio::time::sleep(Duration::from_millis(25)).await;
    }
}

/// Wait until `cond` holds. `stall` is how long `progress` may sit still, not a cap on a run that
/// is still moving: a busy runner that keeps indexing keeps its time. `ceiling` stops a hang.
pub async fn wait_until_progress<F, P, T>(
    stall: Duration,
    ceiling: Duration,
    mut cond: F,
    mut progress: P,
) -> bool
where
    F: FnMut() -> bool,
    P: FnMut() -> T,
    T: PartialEq,
{
    let start = Instant::now();
    let mut since_move = Instant::now();
    let mut last = progress();
    loop {
        if cond() {
            return true;
        }
        let now = progress();
        if now != last {
            last = now;
            since_move = Instant::now();
        }
        if since_move.elapsed() >= stall || start.elapsed() >= ceiling {
            return false;
        }
        tokio::time::sleep(Duration::from_millis(25)).await;
    }
}

/// A generous ceiling for a bounded poll. The loop's own idle re-poll is ~2 s, so this comfortably
/// covers reorg detection + reconvergence while still failing fast on a genuine hang.
pub const POLL_TIMEOUT: Duration = Duration::from_secs(20);

/// Waiting for a tip-path seal now means decoding and writing
/// [`nuthatch::indexer::SEAL_DIRECT_BATCH`] rows, which is more than [`POLL_TIMEOUT`] on a
/// debug build. A last-block wait inside that window uses this too: `last_block` does not move
/// until the window commits (#1989, #2017).
pub const SEAL_POLL_TIMEOUT: Duration = Duration::from_secs(180);

/// Seal `from..=to` through the public seal API and prune the hot store.
///
/// Use this in fixtures that need Parquet and a watermark but do not need to
/// prove `maybe_seal` itself. The tip path will not seal a short range (#1067),
/// and stuffing [`nuthatch::indexer::SEAL_DIRECT_BATCH`] dummy events into a
/// fixture that later asserts exact row counts (or runs an entity circuit)
/// is the wrong tool.
pub fn force_seal_through(dir: &Path, to: u64) {
    let store = nuthatch::store::Store::open(&dir.join("nuthatch.redb")).expect("open store");
    let from = match store.sealed_through() {
        0 => 1,
        n => n + 1,
    };
    if to < from {
        return;
    }
    let rows = store
        .entities_in_range(from, to)
        .expect("entities_in_range");
    if rows.is_empty() {
        store
            .set_meta("sealed_through", &to.to_string())
            .expect("advance empty watermark");
        return;
    }
    nuthatch::seal::seal_range(dir, &rows, from, to)
        .unwrap()
        .expect("range holds rows");
    store
        .prune_and_set_meta(from, to, "sealed_through", &to.to_string())
        .expect("prune sealed range");
}
