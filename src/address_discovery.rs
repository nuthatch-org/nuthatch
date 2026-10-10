//! Discovery for an address-history nest (RFC-0063 §4): which normal transactions and token
//! transfers touch a watched address, fetched window by window and recorded with their coverage.
//!
//! Normal transactions come from `trace_filter`, once with `fromAddress` and once with `toAddress`
//! (both at once means both, not either), keeping root frames. Token transfers come from `eth_getLogs`
//! with no emitter and the address in a topic position. Each hash is hydrated once and cached.
//!
//! A window is recorded as covered only when every call in it succeeded and nothing looked like a
//! silent empty answer. Two checks catch the latter: an empty trace window is sampled against
//! `trace_block`, and for an externally owned account the outgoing transactions found must match the
//! nonce's movement across the window.

use anyhow::{anyhow, bail, Context, Result};
use futures::StreamExt;
use serde_json::{json, Map, Value};
use std::collections::{BTreeMap, BTreeSet};
use std::future::Future;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::Mutex;

use crate::address_history::{Action, AddressHistory, Row};

/// The actions discovery fills; the others stay incomplete until their slices land.
pub const DISCOVERED: [Action; 5] = [
    Action::TxList,
    Action::TxListInternal,
    Action::TokenTx,
    Action::TokenNftTx,
    Action::Token1155Tx,
];

/// The actions a scan of block bodies fills, once per block for every watched address.
pub const BLOCK_SCANNED: [Action; 2] = [Action::BeaconWithdrawals, Action::MinedBlocks];

/// Blocks per block-scan window: one body each, so a failure costs at most this many refetches.
pub const BLOCK_WINDOW: u64 = 2_000;
/// Bodies fetched at once. GraphOps refused hydration with 403s at eight; bodies held at eight.
const SCAN_CONCURRENCY: usize = 8;

/// Ethereum mainnet's forks that change what a block body carries or what its miner earns.
const SHANGHAI: u64 = 17_034_870;
const MERGE: u64 = 15_537_394;
const LONDON: u64 = 12_965_000;
const CONSTANTINOPLE: u64 = 7_280_000;
const BYZANTIUM: u64 = 4_370_000;

/// Blocks per recorded window. A failure costs at most this much refetching.
pub const OUTER_WINDOW: u64 = 50_000;
/// A sub-call returning this many items may have been truncated by the provider, so it is split.
const SUSPICIOUSLY_FULL: usize = 10_000;
/// The most frames one transaction's trace may have before a lookup refuses it rather than hold it.
const MAX_TRACE_FRAMES: usize = 100_000;
/// Hashes hydrated at once.
const HYDRATE_CONCURRENCY: usize = 8;

const TRANSFER: &str = "0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef";
const TRANSFER_SINGLE: &str = "0xc3d58168c5ae7397731d063d5bbf3d657854427343f4c083240f7aacaa2d0f62";
const TRANSFER_BATCH: &str = "0x4a39dc06d4c0dbc64b70af90fd698a233a518aa5d07e595d983b8c0526c8f7fb";

/// A transaction above the finalized head, whose trace a reorg could still change.
#[derive(Debug)]
pub struct NotFinalized(pub u64);

impl std::fmt::Display for NotFinalized {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "block {} is not finalized yet", self.0)
    }
}

impl std::error::Error for NotFinalized {}

/// One JSON-RPC endpoint pool.
pub trait Rpc: Send + Sync + 'static {
    fn call(&self, method: &str, params: Value) -> impl Future<Output = Result<Value>> + Send;
}

impl Rpc for crate::rpc::RpcClient {
    async fn call(&self, method: &str, params: Value) -> Result<Value> {
        crate::rpc::RpcClient::call(self, method, params).await
    }
}

/// An endpoint pool that counts what it is asked, by method.
pub struct Counted<R> {
    inner: R,
    calls: Mutex<BTreeMap<String, u64>>,
}

impl<R> Counted<R> {
    pub fn new(inner: R) -> Counted<R> {
        Counted {
            inner,
            calls: Mutex::new(BTreeMap::new()),
        }
    }

    pub fn calls(&self) -> BTreeMap<String, u64> {
        self.calls.lock().expect("calls lock").clone()
    }

    pub fn inner(&self) -> &R {
        &self.inner
    }
}

impl<R: Rpc> Rpc for Counted<R> {
    async fn call(&self, method: &str, params: Value) -> Result<Value> {
        *self
            .calls
            .lock()
            .expect("calls lock")
            .entry(method.to_string())
            .or_default() += 1;
        self.inner.call(method, params).await
    }
}

/// A block span a provider accepts per call, learned from its refusals.
///
/// A limit the provider states becomes a ceiling. A refusal that only looks range-shaped halves the
/// span, and a run of successes doubles it back towards the ceiling, so one bad minute does not
/// leave a backfill crawling for good. Any other error is not about the range and shrinks nothing.
pub struct Window {
    size: AtomicU64,
    ceiling: AtomicU64,
    streak: AtomicU64,
    /// Whether the current size has been served once. Spans go out concurrently only then, so an
    /// unlearned size costs one refusal rather than a batch of them.
    proven: AtomicBool,
    /// Calls in flight on this window's provider, across every range sharing it.
    permits: tokio::sync::Semaphore,
}

/// Successes at one span before it is tried doubled.
const REGROW_AFTER: u64 = 32;
/// Spans in flight at once on one provider. GraphOps refused hydration with 403s past eight.
const RANGE_CONCURRENCY: usize = 8;

impl Window {
    pub fn new(start: u64) -> Window {
        Window {
            size: AtomicU64::new(start.max(1)),
            ceiling: AtomicU64::new(start.max(1)),
            streak: AtomicU64::new(0),
            proven: AtomicBool::new(false),
            permits: tokio::sync::Semaphore::new(RANGE_CONCURRENCY),
        }
    }

    pub fn get(&self) -> u64 {
        self.size.load(Ordering::Relaxed)
    }

    /// Shrink after a refusal, returning the new span, or `None` when the error says nothing about
    /// the range and the call should fail as it is.
    fn shrink(&self, error: &str) -> Option<u64> {
        let now = self.get();
        self.streak.store(0, Ordering::Relaxed);
        let next = match stated_limit(error).filter(|n| *n >= 1) {
            Some(n) => {
                self.ceiling.fetch_min(n, Ordering::Relaxed);
                if n < now {
                    n
                } else {
                    now / 2
                }
            }
            None if range_shaped(error) => now / 2,
            None => return None,
        }
        .max(1);
        self.size.store(next, Ordering::Relaxed);
        self.proven.store(false, Ordering::Relaxed);
        Some(next)
    }

    fn succeeded(&self) {
        self.proven.store(true, Ordering::Relaxed);
        if self.streak.fetch_add(1, Ordering::Relaxed) + 1 < REGROW_AFTER {
            return;
        }
        self.streak.store(0, Ordering::Relaxed);
        let ceiling = self.ceiling.load(Ordering::Relaxed);
        let now = self.get();
        let next = now.saturating_mul(2).min(ceiling).max(now);
        if next != now {
            self.size.store(next, Ordering::Relaxed);
            self.proven.store(false, Ordering::Relaxed);
        }
    }
}

/// Whether an error reads like a span the provider would not serve, as opposed to one it could not
/// reach or would not authorise.
fn range_shaped(error: &str) -> bool {
    let lower = error.to_ascii_lowercase();
    [
        "range",
        "limit",
        "too many",
        "too large",
        "exceed",
        "response size",
        "timeout",
        "timed out",
        "504",
        "more than",
    ]
    .iter()
    .any(|m| lower.contains(m))
}

/// The block-span limit an error message states, in the forms providers use:
/// `limited to 100 blocks`, `"maxAllowedRange":16384`, `max range of 2000`.
fn stated_limit(error: &str) -> Option<u64> {
    let lower = error.to_ascii_lowercase();
    for marker in [
        "limited to ",
        "maxallowedrange\":",
        "max range of ",
        "maximum range of ",
    ] {
        if let Some(i) = lower.find(marker) {
            let digits: String = lower[i + marker.len()..]
                .chars()
                .skip_while(|c| c.is_whitespace())
                .take_while(char::is_ascii_digit)
                .collect();
            if let Ok(n) = digits.parse() {
                return Some(n);
            }
        }
    }
    None
}

/// Run a ranged call over `[from, to]` in spans the provider accepts, concatenating the arrays in
/// block order. Once the window's span has been served, up to `RANGE_CONCURRENCY` spans go out at
/// once and are judged in order as they arrive: clean answers are kept, a fatal one fails the call
/// at once, and a span that needs a smaller size sends the rest back to be asked again at the new
/// size, after their answers are read so that a fatal one among them is never discarded.
async fn ranged<R: Rpc>(
    rpc: &R,
    window: &Window,
    from: u64,
    to: u64,
    method: &str,
    params: impl Fn(u64, u64) -> Value,
) -> Result<Vec<Value>> {
    let mut out = Vec::new();
    let mut start = from;
    while start <= to {
        let span = window.get();
        let lanes = if window.proven.load(Ordering::Relaxed) {
            RANGE_CONCURRENCY
        } else {
            1
        };
        let mut spans = Vec::with_capacity(lanes);
        let mut s = start;
        while s <= to && spans.len() < lanes {
            let e = s.saturating_add(span - 1).min(to);
            spans.push((s, e));
            s = e + 1;
        }
        let mut answers = futures::stream::iter(spans.clone())
            .map(|(s, e)| {
                let p = params(s, e);
                async move {
                    let _permit = window.permits.acquire().await?;
                    rpc.call(method, p).await
                }
            })
            .buffered(lanes);
        let mut asked = spans.into_iter();
        while let Some(answer) = answers.next().await {
            let (s, e) = asked.next().expect("one answer per span");
            match judge(method, s, e, answer)? {
                Judged::Rows(items) => {
                    window.succeeded();
                    out.extend(items);
                    start = e + 1;
                }
                Judged::Smaller(why) => {
                    while let Some(later) = answers.next().await {
                        let (s, e) = asked.next().expect("one answer per span");
                        judge(method, s, e, later)?;
                    }
                    if let Some(next) = window.shrink(&why) {
                        tracing::debug!(
                            "{method} [{s}, {e}] refused, retrying in spans of {next}: {why}"
                        );
                    }
                    break;
                }
            }
        }
    }
    Ok(out)
}

/// One span's answer: its rows, or a refusal about its size that a smaller span may not meet.
enum Judged {
    Rows(Vec<Value>),
    Smaller(String),
}

/// Judge one span's answer by the rules a sequential scan applies; an error is fatal to the call.
fn judge(method: &str, s: u64, e: u64, answer: Result<Value>) -> Result<Judged> {
    match answer {
        Ok(Value::Array(items)) if items.len() >= SUSPICIOUSLY_FULL => {
            if e == s {
                bail!(
                    "{method} at block {s} answered {} items, which may be a truncated page",
                    items.len()
                );
            }
            Ok(Judged::Smaller("too many results".into()))
        }
        Ok(Value::Array(items)) => Ok(Judged::Rows(items)),
        Ok(other) => bail!("{method} [{s}, {e}] answered a non-list: {other}"),
        Err(err) if e > s => {
            let why = format!("{err:#}");
            if stated_limit(&why).is_some_and(|n| n >= 1) || range_shaped(&why) {
                Ok(Judged::Smaller(why))
            } else {
                Err(err).with_context(|| format!("{method} [{s}, {e}]"))
            }
        }
        Err(err) => Err(err).with_context(|| format!("{method} at block {s}")),
    }
}

pub(crate) fn hex_u64(v: &Value) -> Result<u64> {
    match v {
        Value::Number(n) => n.as_u64().ok_or_else(|| anyhow!("not a u64: {n}")),
        Value::String(s) => u64::from_str_radix(s.trim_start_matches("0x"), 16)
            .with_context(|| format!("not a hex quantity: {s}")),
        other => bail!("not a quantity: {other}"),
    }
}

/// A hex quantity or 32-byte word as the decimal string Etherscan prints.
fn decimal(hex: &str) -> Result<String> {
    let digits = hex.trim_start_matches("0x");
    if digits.is_empty() {
        return Ok("0".into());
    }
    Ok(alloy_primitives::U256::from_str_radix(digits, 16)
        .with_context(|| format!("not a 256-bit hex value: {hex}"))?
        .to_string())
}

fn str_field<'a>(v: &'a Value, k: &str) -> Result<&'a str> {
    v.get(k)
        .and_then(Value::as_str)
        .ok_or_else(|| anyhow!("missing `{k}` in {v}"))
}

fn topic_address(word: &str) -> Result<String> {
    let hex = word.trim_start_matches("0x");
    // Only the shape is checked: the counterparty topic comes from whatever contract emitted the
    // log, and refusing a dirty high half would block this address's window for good.
    if hex.len() != 64 || !hex.bytes().all(|b| b.is_ascii_hexdigit()) {
        bail!("not a 32-byte topic: {word}");
    }
    Ok(format!("0x{}", &hex[24..]))
}

fn padded(address: &str) -> String {
    format!("0x{:0>64}", address.trim_start_matches("0x"))
}

/// Whether `trace_filter` with `side` set to `a` would return this frame: `fromAddress` matches a
/// call's or creation's sender and a selfdestructing contract; `toAddress` a call's callee, a
/// creation's new contract and a selfdestruct's beneficiary.
fn frame_side_is(f: &Value, side: &str, a: &str) -> bool {
    let action = f.get("action").unwrap_or(&Value::Null);
    let result = f.get("result").unwrap_or(&Value::Null);
    let keys: &[(&Value, &str)] = if side == "fromAddress" {
        &[(action, "from"), (action, "address")]
    } else {
        &[
            (action, "to"),
            (result, "address"),
            (action, "refundAddress"),
        ]
    };
    keys.iter().any(|(v, k)| {
        v.get(*k)
            .and_then(Value::as_str)
            .is_some_and(|s| s.eq_ignore_ascii_case(a))
    })
}

/// Cache entries a window fetched, committed once at its end rather than one commit per fetch,
/// and committed whether or not the window is recorded, so a retried window refetches nothing.
type CachedTx = (Vec<u8>, Map<String, Value>);

#[derive(Default)]
pub struct Pending {
    txs: Mutex<Vec<CachedTx>>,
    timestamps: Mutex<Vec<(u64, u64)>>,
    /// A transaction's whole internal list, by hash, for `txhash` lookups.
    internals: Mutex<Vec<crate::address_history::TxInternals>>,
}

impl Pending {
    /// Commit what was fetched. Blocking: call it off the async workers.
    pub fn persist(self, history: &AddressHistory) -> Result<()> {
        history.cache_txs(&self.txs.into_inner().expect("pending lock"))?;
        history.cache_block_timestamps(&self.timestamps.into_inner().expect("pending lock"))?;
        history.cache_tx_internals(&self.internals.into_inner().expect("pending lock"))?;
        Ok(())
    }
}

/// A normal transaction a trace root names: its hash, its block, whether the watched address sent
/// it, and whether it failed, which pre-Byzantium receipts do not say.
#[derive(Debug, Clone)]
struct TxRef {
    hash: String,
    block: u64,
    outgoing: bool,
    failed: bool,
}

/// What a block scan found for one address.
#[derive(Debug, Default)]
pub struct BlockFound {
    pub withdrawals: Vec<Row>,
    pub mined: Vec<Row>,
}

/// The issuance a block paid its miner before the Merge, by fork; none after it.
pub fn static_reward(b: u64) -> alloy_primitives::U256 {
    use alloy_primitives::U256;
    let ether = U256::from(1_000_000_000_000_000_000u64);
    if b >= MERGE {
        U256::ZERO
    } else if b >= CONSTANTINOPLE {
        ether * U256::from(2)
    } else if b >= BYZANTIUM {
        ether * U256::from(3)
    } else {
        ether * U256::from(5)
    }
}

/// Record a block scan's finds for one address over `[from, to]`; `verified` when they were read from
/// a verified partition rather than the RPC.
pub fn record_blocks(
    history: &AddressHistory,
    address: &str,
    (from, to): (u64, u64),
    found: BlockFound,
    generation: u64,
    verified: bool,
) -> Result<()> {
    let record = if verified {
        AddressHistory::record_verified
    } else {
        AddressHistory::record
    };
    let within = |rows: Vec<Row>| -> Vec<Row> {
        rows.into_iter()
            .filter(|r| from <= r.block && r.block <= to)
            .collect()
    };
    record(
        history,
        Action::BeaconWithdrawals,
        address,
        &within(found.withdrawals),
        (from, to),
        generation,
    )?;
    record(
        history,
        Action::MinedBlocks,
        address,
        &within(found.mined),
        (from, to),
        generation,
    )
}

/// What one window found for one address, ready to record.
#[derive(Debug, Default)]
pub struct Found {
    pub txlist: Vec<Row>,
    pub txlistinternal: Vec<Row>,
    pub tokentx: Vec<Row>,
    pub tokennfttx: Vec<Row>,
    pub token1155tx: Vec<Row>,
}

/// Discovery over a main endpoint pool (logs, hydration, nonces) and a trace one, which may be the
/// same endpoints.
pub struct Discoverer<M, T> {
    pub main: M,
    pub trace: T,
    pub trace_window: Window,
    pub log_window: Window,
}

impl<M: Rpc, T: Rpc> Discoverer<M, T> {
    pub fn new(main: M, trace: T) -> Discoverer<M, T> {
        Discoverer {
            main,
            trace,
            trace_window: Window::new(100_000),
            log_window: Window::new(100_000),
        }
    }

    /// Discover `[from, to]` for `address` (lowercase `0x…`). An error means nothing may be recorded;
    /// what was fetched before it is in `pending` either way.
    pub async fn discover(
        &self,
        history: &AddressHistory,
        address: &str,
        from: u64,
        to: u64,
        pending: &Pending,
    ) -> Result<Found> {
        let a = address.to_ascii_lowercase();
        let ((txs, internal), logs) = futures::try_join!(
            self.normal_transactions(&a, from, to),
            self.token_logs(&a, from, to)
        )?;
        let mut found = Found {
            txlist: self.hydrate(history, &txs, pending).await?,
            ..Found::default()
        };
        // Only traces of blocks the nest serves through are cached; a window is never past them.
        let finalized = history.head()?.unwrap_or(0);
        for hash in &internal {
            let rows = self
                .internal_rows_of(history, hash, pending, finalized)
                .await?;
            found.txlistinternal.extend(
                rows.into_iter()
                    .filter(|row| touches(&row.record, &a))
                    .map(|mut row| {
                        row.record
                            .insert("hash".into(), Value::String(hash.clone()));
                        address_mode_order(row)
                    }),
            );
        }
        let timestamps = self.timestamps(history, &logs, pending).await?;
        for log in &logs {
            transfer_rows(log, &a, &timestamps, &mut found)?;
        }
        Ok(found)
    }

    /// Every internal transaction of `hash`, as Etherscan's `txhash` form lists them: from the cache,
    /// else from one `trace_transaction`.
    pub async fn internal_rows_of(
        &self,
        history: &AddressHistory,
        hash: &str,
        pending: &Pending,
        finalized: u64,
    ) -> Result<Vec<Row>> {
        let key = alloy_primitives::hex::decode(hash.trim_start_matches("0x"))?;
        let records = match history.tx_internals(&key)? {
            Some(r) => r,
            None => {
                let trace = self
                    .trace
                    .call("trace_transaction", json!([hash]))
                    .await
                    .with_context(|| format!("trace_transaction {hash}"))?;
                let frames = trace
                    .as_array()
                    .ok_or_else(|| anyhow!("trace_transaction {hash} answered a non-list"))?;
                let Some(root) = frames.first() else {
                    bail!("trace_transaction {hash} answered no frames");
                };
                if frames.len() > MAX_TRACE_FRAMES {
                    bail!(
                        "{hash} has {} trace frames, over the {MAX_TRACE_FRAMES} one lookup holds",
                        frames.len()
                    );
                }
                let block = hex_u64(root.get("blockNumber").unwrap_or(&Value::Null))?;
                // Cached traces are never revisited, so only a finalized block's may be cached.
                if block > finalized {
                    return Err(anyhow!(NotFinalized(block)));
                }
                let ts = self.block_timestamp(history, block, pending).await?;
                let records = internal_records(frames, ts)?;
                pending
                    .internals
                    .lock()
                    .expect("pending lock")
                    .push((key, block, records.clone()));
                records
            }
        };
        records
            .into_iter()
            .map(|record| {
                let num = |k: &str| -> Result<u64> {
                    record
                        .get(k)
                        .and_then(Value::as_str)
                        .ok_or_else(|| anyhow!("internal record lacks {k}"))?
                        .parse()
                        .with_context(|| format!("internal record {k}"))
                };
                Ok(Row {
                    block: num("blockNumber")?,
                    tx_index: num("transactionIndex")?,
                    position: num(TRACE_INDEX)?,
                    record,
                })
            })
            .collect()
    }

    /// Read every block body in `[from, to]` once, for all of `addresses` at once: the withdrawals
    /// paid to each (EIP-4895) and the blocks each received the fees of. A body missing, without a
    /// miner, or without its withdrawals after Shanghai fails the scan rather than reading as none.
    pub async fn scan_blocks(
        &self,
        addresses: &[String],
        from: u64,
        to: u64,
        pending: &Pending,
    ) -> Result<BTreeMap<String, BlockFound>> {
        let watched: BTreeSet<String> = addresses.iter().map(|a| a.to_ascii_lowercase()).collect();
        let bodies: Vec<Result<(u64, Value)>> = futures::stream::iter(from..=to)
            .map(|b| self.body(b))
            .buffered(SCAN_CONCURRENCY)
            .collect()
            .await;
        let mut found: BTreeMap<String, BlockFound> = watched
            .iter()
            .map(|a| (a.clone(), BlockFound::default()))
            .collect();
        let mut timestamps = Vec::new();
        for body in bodies {
            let (b, header) = body?;
            let ts = hex_u64(header.get("timestamp").unwrap_or(&Value::Null))
                .with_context(|| format!("block {b} timestamp"))?;
            timestamps.push((b, ts));
            match header.get("withdrawals").and_then(Value::as_array) {
                Some(ws) => {
                    for (i, w) in ws.iter().enumerate() {
                        let to = str_field(w, "address")?.to_ascii_lowercase();
                        let Some(f) = found.get_mut(&to) else {
                            continue;
                        };
                        let mut r = Map::new();
                        let mut put = |k: &str, v: String| {
                            r.insert(k.to_string(), Value::String(v));
                        };
                        put("withdrawalIndex", decimal(str_field(w, "index")?)?);
                        put("validatorIndex", decimal(str_field(w, "validatorIndex")?)?);
                        put("address", to);
                        put("amount", decimal(str_field(w, "amount")?)?);
                        put("blockNumber", b.to_string());
                        put("timestamp", ts.to_string());
                        f.withdrawals.push(Row {
                            block: b,
                            tx_index: 0,
                            position: i as u64,
                            record: r,
                        });
                    }
                }
                None if b >= SHANGHAI => {
                    bail!("block {b}'s body carries no withdrawals, though it is past Shanghai")
                }
                None => {}
            }
            let miner = str_field(&header, "miner")?.to_ascii_lowercase();
            if let Some(f) = found.get_mut(&miner) {
                let reward = self.block_reward(b, &header).await?;
                let mut r = Map::new();
                r.insert("blockNumber".into(), Value::String(b.to_string()));
                r.insert("timeStamp".into(), Value::String(ts.to_string()));
                r.insert("blockReward".into(), Value::String(reward));
                f.mined.push(Row {
                    block: b,
                    tx_index: 0,
                    position: 0,
                    record: r,
                });
            }
        }
        pending
            .timestamps
            .lock()
            .expect("pending lock")
            .extend(timestamps);
        Ok(found)
    }

    async fn body(&self, b: u64) -> Result<(u64, Value)> {
        let header = self
            .main
            .call("eth_getBlockByNumber", json!([format!("0x{b:x}"), false]))
            .await
            .with_context(|| format!("block {b}"))?;
        if header.is_null() {
            bail!("the main endpoint has no block {b}");
        }
        let number = hex_u64(header.get("number").unwrap_or(&Value::Null))
            .with_context(|| format!("block {b} number"))?;
        if number != b {
            bail!("asked for block {b}, the main endpoint answered with block {number}");
        }
        Ok((b, header))
    }

    /// What Etherscan's `getminedblocks` calls `blockReward`, measured against its answers: the
    /// priority fees the block paid its fee recipient, plus before the Merge the static reward and
    /// a thirty-second of it for each uncle included. Burnt base fees are not counted.
    pub(crate) async fn block_reward(&self, b: u64, header: &Value) -> Result<String> {
        use alloy_primitives::U256;
        let receipts = self
            .main
            .call("eth_getBlockReceipts", json!([format!("0x{b:x}")]))
            .await
            .with_context(|| format!("receipts of block {b}"))?;
        let receipts = receipts
            .as_array()
            .ok_or_else(|| anyhow!("receipts of block {b} are not a list"))?;
        let txs = header
            .get("transactions")
            .and_then(Value::as_array)
            .ok_or_else(|| anyhow!("block {b} lists no transactions"))?;
        if receipts.len() != txs.len() {
            bail!(
                "block {b} has {} transactions and {} receipts",
                txs.len(),
                receipts.len()
            );
        }
        let quantity = |v: &Value, field: &str| -> Result<U256> {
            let s = v
                .get(field)
                .and_then(Value::as_str)
                .ok_or_else(|| anyhow!("block {b}: no {field}"))?;
            Ok(U256::from_str_radix(s.trim_start_matches("0x"), 16)?)
        };
        let base_fee = if b >= LONDON {
            quantity(header, "baseFeePerGas")?
        } else {
            U256::ZERO
        };
        let mut fees = U256::ZERO;
        for (r, tx) in receipts.iter().zip(txs) {
            if r.get("transactionHash") != Some(tx) {
                bail!("block {b}'s receipts do not follow its transactions");
            }
            let tip = quantity(r, "effectiveGasPrice")?
                .checked_sub(base_fee)
                .ok_or_else(|| anyhow!("block {b}: a receipt priced under the base fee"))?;
            fees += quantity(r, "gasUsed")? * tip;
        }
        let static_reward = static_reward(b);
        let uncles = header
            .get("uncles")
            .and_then(Value::as_array)
            .map_or(0, Vec::len);
        let reward = static_reward + fees + static_reward / U256::from(32) * U256::from(uncles);
        Ok(reward.to_string())
    }

    /// The root frames from or to `a`, and the transactions with an internal frame touching `a`.
    async fn normal_transactions(
        &self,
        a: &str,
        from: u64,
        to: u64,
    ) -> Result<(Vec<TxRef>, Vec<String>)> {
        let sides = ["fromAddress", "toAddress"];
        let answers = futures::future::try_join_all(sides.map(|side| {
            ranged(&self.trace, &self.trace_window, from, to, "trace_filter", move |s, e| {
                json!([{ "fromBlock": format!("0x{s:x}"), "toBlock": format!("0x{e:x}"), side: [a] }])
            })
        }))
        .await?;
        let mut frames = Vec::new();
        let mut empty_sides = Vec::new();
        for (side, got) in sides.into_iter().zip(answers) {
            if got.is_empty() {
                empty_sides.push(side);
            }
            frames.extend(got);
        }
        // Each side is judged on its own: rows from one say nothing about whether the other's
        // empty answer is true.
        if !empty_sides.is_empty() {
            self.check_empty_traces(a, &empty_sides, from, to).await?;
        }
        let mut roots: BTreeMap<String, TxRef> = BTreeMap::new();
        let mut internal: BTreeSet<String> = BTreeSet::new();
        for f in &frames {
            let root = f
                .get("traceAddress")
                .and_then(Value::as_array)
                .is_some_and(Vec::is_empty);
            let Some(hash) = f.get("transactionHash").and_then(Value::as_str) else {
                continue;
            };
            if !root {
                if let Some(record) = internal_record(f)? {
                    if touches(&record, a) {
                        internal.insert(hash.to_ascii_lowercase());
                    }
                }
                continue;
            }
            let action = f.get("action").unwrap_or(&Value::Null);
            let sender = action.get("from").and_then(Value::as_str).unwrap_or("");
            let outgoing = sender.eq_ignore_ascii_case(a);
            // The toAddress side matches a creation by the address it made, and Etherscan lists the
            // creating transaction for that contract.
            if !outgoing && !frame_side_is(f, "toAddress", a) {
                continue;
            }
            let block = hex_u64(f.get("blockNumber").unwrap_or(&Value::Null))?;
            let hash = hash.to_ascii_lowercase();
            let e = roots.entry(hash.clone()).or_insert(TxRef {
                hash,
                block,
                outgoing: false,
                failed: f.get("error").is_some_and(|e| !e.is_null()),
            });
            e.outgoing |= outgoing;
        }
        let outgoing = roots.values().filter(|r| r.outgoing).count() as u64;
        self.check_nonce(a, from, to, outgoing).await?;
        Ok((
            roots.into_values().collect(),
            internal.into_iter().collect(),
        ))
    }

    /// `sides` are the `trace_filter` directions that answered nothing over `[from, to]`. Each is
    /// believed only if the trace source demonstrably has traces for the range (some providers
    /// answer `[]` for blocks they never traced) and none of a probe block's frames touches `a` on
    /// that side, which the filter would have had to return.
    async fn check_empty_traces(&self, a: &str, sides: &[&str], from: u64, to: u64) -> Result<()> {
        // Every empty side is probed, a block with transactions at a time, so a refused probe
        // cannot be skipped on the retry.
        let mid = from + (to - from) / 2;
        for probe in [mid, from, to] {
            let count = self
                .main
                .call(
                    "eth_getBlockTransactionCountByNumber",
                    json!([format!("0x{probe:x}")]),
                )
                .await?;
            if hex_u64(&count)? == 0 {
                continue;
            }
            let traced = self
                .trace
                .call("trace_block", json!([format!("0x{probe:x}")]))
                .await
                .context("trace_block, checking an empty trace_filter window")?;
            let Some(block_frames) = traced.as_array().filter(|t| !t.is_empty()) else {
                bail!(
                    "the trace source returned no traces for block {probe}, which holds \
                     transactions; not recording [{from}, {to}] as covered"
                );
            };
            for side in sides {
                if block_frames.iter().any(|f| frame_side_is(f, side, a)) {
                    bail!(
                        "trace_filter {side} answered nothing for [{from}, {to}], but block \
                         {probe} has a frame {side} {a}; not recording the window as covered"
                    );
                }
            }
            return Ok(());
        }
        Ok(())
    }

    /// For an EOA every sent transaction moves the nonce by one, so the outgoing root frames found
    /// must equal the nonce's movement. A contract's nonce counts its creations instead, and a
    /// delegated account (EIP-7702) can move without sending, so neither is checked.
    async fn check_nonce(&self, a: &str, from: u64, to: u64, outgoing: u64) -> Result<()> {
        // Code is read at the window's end, not today: an account delegated under EIP-7702 since
        // was a plain EOA through all of its earlier history.
        let code = self
            .main
            .call("eth_getCode", json!([a, format!("0x{to:x}")]))
            .await?;
        if code.as_str() != Some("0x") {
            return Ok(());
        }
        let nonce_at = |b: u64| async move {
            let n = self
                .main
                .call("eth_getTransactionCount", json!([a, format!("0x{b:x}")]))
                .await?;
            hex_u64(&n)
        };
        let before = if from == 0 {
            0
        } else {
            nonce_at(from - 1).await?
        };
        let after = nonce_at(to).await?;
        let moved = after.saturating_sub(before);
        if moved != outgoing {
            bail!(
                "{a} sent {moved} transactions in [{from}, {to}] by its nonce, and the traces show \
                 {outgoing}; not recording the window as covered"
            );
        }
        Ok(())
    }

    async fn token_logs(&self, a: &str, from: u64, to: u64) -> Result<Vec<Value>> {
        let who = padded(a);
        let filters = [
            json!([TRANSFER, who]),
            json!([TRANSFER, null, who]),
            json!([[TRANSFER_SINGLE, TRANSFER_BATCH], null, who]),
            json!([[TRANSFER_SINGLE, TRANSFER_BATCH], null, null, who]),
        ];
        let answers = futures::future::try_join_all(filters.iter().map(|topics| {
            ranged(&self.main, &self.log_window, from, to, "eth_getLogs", move |s, e| {
                json!([{ "fromBlock": format!("0x{s:x}"), "toBlock": format!("0x{e:x}"), "topics": topics }])
            })
        }))
        .await?;
        let mut seen = BTreeSet::new();
        let mut logs = Vec::new();
        let mut empty = Vec::new();
        for (topics, got) in filters.into_iter().zip(answers) {
            if got.is_empty() {
                empty.push(topics.clone());
            }
            for log in got {
                if log.get("removed").and_then(Value::as_bool) == Some(true) {
                    bail!("a removed log in finalized range [{from}, {to}]: {log}");
                }
                let key = (
                    str_field(&log, "transactionHash")?.to_ascii_lowercase(),
                    hex_u64(log.get("logIndex").unwrap_or(&Value::Null))?,
                );
                if seen.insert(key) {
                    logs.push(log);
                }
            }
        }
        if !empty.is_empty() {
            self.check_empty_logs(&who, &empty, from, to).await?;
        }
        Ok(logs)
    }

    /// Each positional filter that answered nothing is checked against the same event signatures
    /// in a probe block with no address position: a log there with the address where the filter
    /// looked means the filter's empty answer was false. Rows from the filter at the other
    /// position say nothing about this one.
    async fn check_empty_logs(&self, who: &str, empty: &[Value], from: u64, to: u64) -> Result<()> {
        let probe = from + (to - from) / 2;
        let mut probed: BTreeMap<String, Vec<Value>> = BTreeMap::new();
        for topics in empty {
            let sig = topics[0].clone();
            let position = topics.as_array().map_or(0, |t| t.len().saturating_sub(1));
            let key = sig.to_string();
            if !probed.contains_key(&key) {
                let got = self
                    .main
                    .call(
                        "eth_getLogs",
                        json!([{ "fromBlock": format!("0x{probe:x}"), "toBlock": format!("0x{probe:x}"), "topics": [sig] }]),
                    )
                    .await
                    .context("eth_getLogs, checking an empty positional filter")?;
                let got = got
                    .as_array()
                    .cloned()
                    .ok_or_else(|| anyhow!("eth_getLogs answered a non-list"))?;
                probed.insert(key.clone(), got);
            }
            let hit = probed[&key].iter().any(|log| {
                log.get("topics")
                    .and_then(|t| t.get(position))
                    .and_then(Value::as_str)
                    .is_some_and(|t| t.eq_ignore_ascii_case(who))
            });
            if hit {
                bail!(
                    "eth_getLogs {topics} answered nothing for [{from}, {to}], but block {probe} has \
                     such a log; not recording the window as covered"
                );
            }
        }
        Ok(())
    }

    /// Block timestamps for the logs' blocks: from the log when the provider includes it, else the
    /// cache, else one header read per block.
    async fn timestamps(
        &self,
        history: &AddressHistory,
        logs: &[Value],
        pending: &Pending,
    ) -> Result<BTreeMap<u64, u64>> {
        let mut ts = BTreeMap::new();
        let mut missing = BTreeSet::new();
        for log in logs {
            let block = hex_u64(log.get("blockNumber").unwrap_or(&Value::Null))?;
            match log.get("blockTimestamp") {
                Some(t) => {
                    ts.insert(block, hex_u64(t)?);
                }
                None => {
                    missing.insert(block);
                }
            }
        }
        for b in missing {
            if let std::collections::btree_map::Entry::Vacant(slot) = ts.entry(b) {
                slot.insert(self.block_timestamp(history, b, pending).await?);
            }
        }
        Ok(ts)
    }

    async fn block_timestamp(
        &self,
        history: &AddressHistory,
        block: u64,
        pending: &Pending,
    ) -> Result<u64> {
        if let Some(t) = history.block_timestamp(block)? {
            return Ok(t);
        }
        let header = self
            .main
            .call(
                "eth_getBlockByNumber",
                json!([format!("0x{block:x}"), false]),
            )
            .await?;
        let t = hex_u64(header.get("timestamp").unwrap_or(&Value::Null))
            .with_context(|| format!("block {block} header"))?;
        pending
            .timestamps
            .lock()
            .expect("pending lock")
            .push((block, t));
        Ok(t)
    }

    async fn hydrate_one(
        &self,
        history: &AddressHistory,
        r: TxRef,
        pending: &Pending,
    ) -> Result<Map<String, Value>> {
        let hash = r.hash;
        let key = alloy_primitives::hex::decode(hash.trim_start_matches("0x"))?;
        if let Some(r) = history.cached_tx(&key)? {
            return Ok(r);
        }
        let (tx, receipt, ts) = futures::try_join!(
            self.main.call("eth_getTransactionByHash", json!([hash])),
            self.main.call("eth_getTransactionReceipt", json!([hash])),
            self.block_timestamp(history, r.block, pending)
        )?;
        if tx.is_null() || receipt.is_null() {
            bail!("{hash} has no transaction or receipt at the main endpoint");
        }
        // Traces and the main endpoint are separate providers: refuse a transaction they place
        // differently rather than record a mixture.
        let tx_block = hex_u64(tx.get("blockNumber").unwrap_or(&Value::Null))?;
        let tx_block_hash = str_field(&tx, "blockHash")?;
        if tx_block != r.block
            || !str_field(&receipt, "blockHash")?.eq_ignore_ascii_case(tx_block_hash)
        {
            bail!(
                "{hash}: the trace source puts it in block {}, the main endpoint in {tx_block}, and \
                 its receipt in another block; not recording a mixture",
                r.block
            );
        }
        let record = txlist_record(&tx, &receipt, ts, r.failed)?;
        pending
            .txs
            .lock()
            .expect("pending lock")
            .push((key, record.clone()));
        Ok(record)
    }

    /// Each hash's Etherscan `txlist` record: from the cache if it was hydrated before, else from
    /// the transaction, its receipt and its block's timestamp, cached as it is built.
    async fn hydrate(
        &self,
        history: &AddressHistory,
        txs: &[TxRef],
        pending: &Pending,
    ) -> Result<Vec<Row>> {
        let records: Vec<Result<Map<String, Value>>> = futures::stream::iter(txs.iter().cloned())
            .map(|r| self.hydrate_one(history, r, pending))
            .buffer_unordered(HYDRATE_CONCURRENCY)
            .collect()
            .await;
        records
            .into_iter()
            .map(|r| {
                let record = r?;
                let num = |k: &str| -> Result<u64> {
                    record
                        .get(k)
                        .and_then(Value::as_str)
                        .ok_or_else(|| anyhow!("record lacks {k}"))?
                        .parse()
                        .with_context(|| format!("record {k}"))
                };
                Ok(Row {
                    block: num("blockNumber")?,
                    tx_index: num("transactionIndex")?,
                    position: 0,
                    record,
                })
            })
            .collect()
    }
}

/// The `txlist` fields Etherscan prints, in its order, from a transaction and its receipt.
/// `functionName` needs the callee's ABI and `confirmations` changes by the block, so neither is
/// kept. `gasPrice` is what the sender paid: the receipt's effective price. A receipt from before
/// Byzantium has no status, so whether it failed comes from its trace.
pub fn txlist_record(
    tx: &Value,
    receipt: &Value,
    timestamp: u64,
    trace_failed: bool,
) -> Result<Map<String, Value>> {
    let dec = |v: &Value, k: &str| -> Result<String> { decimal(str_field(v, k)?) };
    let status = receipt.get("status").and_then(Value::as_str);
    let input = str_field(tx, "input")?.to_string();
    let method_id = if input.len() >= 10 {
        input[..10].to_string()
    } else {
        "0x".to_string()
    };
    let to = tx
        .get("to")
        .and_then(Value::as_str)
        .unwrap_or("")
        .to_string();
    let created = receipt
        .get("contractAddress")
        .and_then(Value::as_str)
        .unwrap_or("")
        .to_string();
    let gas_price = match receipt.get("effectiveGasPrice").and_then(Value::as_str) {
        Some(p) => decimal(p)?,
        None => dec(tx, "gasPrice")?,
    };
    let mut r = Map::new();
    let mut put = |k: &str, v: String| {
        r.insert(k.to_string(), Value::String(v));
    };
    put("blockNumber", dec(tx, "blockNumber")?);
    put(
        "blockHash",
        str_field(tx, "blockHash")?.to_ascii_lowercase(),
    );
    put("timeStamp", timestamp.to_string());
    put("hash", str_field(tx, "hash")?.to_ascii_lowercase());
    put("nonce", dec(tx, "nonce")?);
    put("transactionIndex", dec(tx, "transactionIndex")?);
    put("from", str_field(tx, "from")?.to_ascii_lowercase());
    put("to", to.to_ascii_lowercase());
    put("value", dec(tx, "value")?);
    put("gas", dec(tx, "gas")?);
    put("gasPrice", gas_price);
    put("input", input);
    put("methodId", method_id);
    put("contractAddress", created.to_ascii_lowercase());
    put("cumulativeGasUsed", dec(receipt, "cumulativeGasUsed")?);
    put(
        "txreceipt_status",
        match status {
            Some(s) => decimal(s)?,
            None => String::new(),
        },
    );
    put("gasUsed", dec(receipt, "gasUsed")?);
    let failed = match status {
        Some(s) => decimal(s)? == "0",
        None => trace_failed,
    };
    put("isError", if failed { "1".into() } else { "0".into() });
    Ok(r)
}

/// Our key beside Etherscan's: the frame's position in the transaction's whole trace, in pre-order.
/// Unique within the transaction and the same however the frame is reached, which Etherscan's own
/// `traceId` is not.
pub const TRACE_INDEX: &str = "nuthatchTraceIndex";

/// A trace frame as an Etherscan internal transaction, or `None` for a frame Etherscan does not
/// list: a delegatecall, a static call, or a call that moves no value. Creations are listed at any
/// value and typed by their opcode; a selfdestruct is listed as `self-destruct`. Context the frame
/// alone cannot give (failure inherited from an ancestor, block, position) is added by
/// [`internal_records`].
fn internal_record(f: &Value) -> Result<Option<Map<String, Value>>> {
    let action = f.get("action").unwrap_or(&Value::Null);
    let result = f.get("result").unwrap_or(&Value::Null);
    let lower = |v: &Value, k: &str| -> String {
        v.get(k)
            .and_then(Value::as_str)
            .unwrap_or("")
            .to_ascii_lowercase()
    };
    let dec_or_zero = |v: &Value, k: &str| -> Result<String> {
        match v.get(k).and_then(Value::as_str) {
            Some(s) => decimal(s),
            None => Ok("0".into()),
        }
    };
    let (kind, from, to, value, created, gas, gas_used) =
        match f.get("type").and_then(Value::as_str) {
            Some("call") => {
                let call = action
                    .get("callType")
                    .and_then(Value::as_str)
                    .unwrap_or("call");
                let value = dec_or_zero(action, "value")?;
                if !matches!(call, "call" | "callcode") || value == "0" {
                    return Ok(None);
                }
                (
                    "call".to_string(),
                    lower(action, "from"),
                    lower(action, "to"),
                    value,
                    String::new(),
                    dec_or_zero(action, "gas")?,
                    dec_or_zero(result, "gasUsed")?,
                )
            }
            Some("create") => (
                action
                    .get("creationMethod")
                    .and_then(Value::as_str)
                    .unwrap_or("create")
                    .to_string(),
                lower(action, "from"),
                String::new(),
                dec_or_zero(action, "value")?,
                lower(result, "address"),
                dec_or_zero(action, "gas")?,
                dec_or_zero(result, "gasUsed")?,
            ),
            Some("suicide") => (
                "self-destruct".to_string(),
                lower(action, "address"),
                lower(action, "refundAddress"),
                dec_or_zero(action, "balance")?,
                String::new(),
                "0".to_string(),
                "0".to_string(),
            ),
            _ => return Ok(None),
        };
    let mut r = Map::new();
    let mut put = |k: &str, v: String| {
        r.insert(k.to_string(), Value::String(v));
    };
    put("from", from);
    put("to", to);
    put("value", value);
    put("contractAddress", created);
    put("input", String::new());
    put("type", kind);
    put("gas", gas);
    put("gasUsed", gas_used);
    Ok(Some(r))
}

/// Whether an internal record moves value from, to or into existence at `a`.
fn touches(record: &Map<String, Value>, a: &str) -> bool {
    ["from", "to", "contractAddress"]
        .iter()
        .any(|k| record.get(*k).and_then(Value::as_str) == Some(a))
}

/// Etherscan's error text for a trace error: geth's wording, lowercased.
fn err_code(error: &str) -> String {
    match error {
        "Reverted" => "execution reverted".into(),
        "Out of gas" => "out of gas".into(),
        "Bad instruction" => "invalid opcode".into(),
        "Bad jump destination" => "invalid jump destination".into(),
        other => other.to_ascii_lowercase(),
    }
}

/// Every internal transaction of one transaction's whole trace, in the `txhash` form Etherscan
/// gives, each with the `traceId` its address form carries and our [`TRACE_INDEX`]. A frame inside a
/// reverted call is marked failed though it succeeded itself, as Etherscan marks it; `errCode` is the
/// frame's own error only.
fn internal_records(frames: &[Value], timestamp: u64) -> Result<Vec<Map<String, Value>>> {
    let reverted: Vec<Vec<Value>> = frames
        .iter()
        .filter(|f| f.get("error").is_some_and(|e| !e.is_null()))
        .filter_map(|f| f.get("traceAddress").and_then(Value::as_array).cloned())
        .collect();
    let mut out = Vec::new();
    for (index, f) in frames.iter().enumerate() {
        let path = f
            .get("traceAddress")
            .and_then(Value::as_array)
            .ok_or_else(|| anyhow!("a trace frame without traceAddress: {f}"))?;
        if path.is_empty() {
            continue;
        }
        let Some(body) = internal_record(f)? else {
            continue;
        };
        let own = f.get("error").and_then(Value::as_str);
        let failed = own.is_some() || reverted.iter().any(|r| path.starts_with(r));
        let mut r = Map::new();
        let mut put = |k: &str, v: String| {
            r.insert(k.to_string(), Value::String(v));
        };
        put(
            "blockNumber",
            hex_u64(f.get("blockNumber").unwrap_or(&Value::Null))?.to_string(),
        );
        put(
            "transactionIndex",
            hex_u64(f.get("transactionPosition").unwrap_or(&Value::Null))?.to_string(),
        );
        put("timeStamp", timestamp.to_string());
        r.extend(body);
        let mut put = |k: &str, v: String| {
            r.insert(k.to_string(), Value::String(v));
        };
        put("traceId", format!("0{}", "_1".repeat(path.len())));
        put("isError", if failed { "1".into() } else { "0".into() });
        put("errCode", own.map(err_code).unwrap_or_default());
        put(TRACE_INDEX, index.to_string());
        out.push(r);
    }
    Ok(out)
}

/// An internal row in the order Etherscan's address form prints it, `hash` after `timeStamp`.
fn address_mode_order(mut row: Row) -> Row {
    let mut ordered = Map::new();
    for k in ["blockNumber", "transactionIndex", "timeStamp", "hash"] {
        if let Some(v) = row.record.remove(k) {
            ordered.insert(k.to_string(), v);
        }
    }
    ordered.extend(std::mem::take(&mut row.record));
    row.record = ordered;
    row
}

/// One log's Etherscan rows: an ERC-20 `Transfer` (three topics) goes to `tokentx`, an ERC-721
/// `Transfer` (four, the token id indexed) to `tokennfttx`, an ERC-1155 transfer to `token1155tx`
/// with one row per id. Rows sort by log index, then by element within a batch.
fn transfer_rows(
    log: &Value,
    a: &str,
    timestamps: &BTreeMap<u64, u64>,
    found: &mut Found,
) -> Result<()> {
    let topics: Vec<&str> = log
        .get("topics")
        .and_then(Value::as_array)
        .ok_or_else(|| anyhow!("log without topics: {log}"))?
        .iter()
        .filter_map(Value::as_str)
        .collect();
    let block = hex_u64(log.get("blockNumber").unwrap_or(&Value::Null))?;
    let tx_index = hex_u64(log.get("transactionIndex").unwrap_or(&Value::Null))?;
    let log_index = hex_u64(log.get("logIndex").unwrap_or(&Value::Null))?;
    if log_index >= 1 << 47 {
        bail!("log index {log_index} is past the row key's room");
    }
    let ts = timestamps
        .get(&block)
        .ok_or_else(|| anyhow!("no timestamp for block {block}"))?;
    let data = str_field(log, "data")?.trim_start_matches("0x");
    let word = |i: usize| -> Result<String> {
        data.get(i * 64..(i + 1) * 64)
            .map(|w| format!("0x{w}"))
            .ok_or_else(|| anyhow!("log data too short: {log}"))
    };
    let base = || -> Result<Map<String, Value>> {
        let mut r = Map::new();
        r.insert("blockNumber".into(), Value::String(block.to_string()));
        r.insert("timeStamp".into(), Value::String(ts.to_string()));
        r.insert(
            "hash".into(),
            Value::String(str_field(log, "transactionHash")?.to_ascii_lowercase()),
        );
        r.insert(
            "blockHash".into(),
            Value::String(str_field(log, "blockHash")?.to_ascii_lowercase()),
        );
        r.insert(
            "contractAddress".into(),
            Value::String(str_field(log, "address")?.to_ascii_lowercase()),
        );
        Ok(r)
    };
    let tail = |r: &mut Map<String, Value>| {
        r.insert(
            "transactionIndex".into(),
            Value::String(tx_index.to_string()),
        );
        r.insert("logIndex".into(), Value::String(log_index.to_string()));
    };
    let row = |record, element: u64| Row {
        block,
        tx_index,
        position: (log_index << 16) | element,
        record,
    };
    let party = |t: &str| topic_address(t).map(Value::String);
    match (topics.first().copied(), topics.len()) {
        (Some(TRANSFER), 3) => {
            let mut r = base()?;
            r.insert("from".into(), party(topics[1])?);
            r.insert("to".into(), party(topics[2])?);
            r.insert("value".into(), Value::String(decimal(&word(0)?)?));
            tail(&mut r);
            found.tokentx.push(row(r, 0));
        }
        (Some(TRANSFER), 4) => {
            let mut r = base()?;
            r.insert("from".into(), party(topics[1])?);
            r.insert("to".into(), party(topics[2])?);
            r.insert("tokenID".into(), Value::String(decimal(topics[3])?));
            tail(&mut r);
            found.tokennfttx.push(row(r, 0));
        }
        (Some(TRANSFER_SINGLE), 4) => {
            let mut r = base()?;
            r.insert("from".into(), party(topics[2])?);
            r.insert("to".into(), party(topics[3])?);
            r.insert("tokenID".into(), Value::String(decimal(&word(0)?)?));
            r.insert("tokenValue".into(), Value::String(decimal(&word(1)?)?));
            tail(&mut r);
            found.token1155tx.push(row(r, 0));
        }
        (Some(TRANSFER_BATCH), 4) => {
            let (ids, values) = batch_arrays(data)?;
            if ids.len() > 1 << 16 {
                bail!(
                    "a TransferBatch of {} ids is past the row key's room",
                    ids.len()
                );
            }
            for (i, (id, value)) in ids.iter().zip(&values).enumerate() {
                let mut r = base()?;
                r.insert("from".into(), party(topics[2])?);
                r.insert("to".into(), party(topics[3])?);
                r.insert("tokenID".into(), Value::String(id.clone()));
                r.insert("tokenValue".into(), Value::String(value.clone()));
                tail(&mut r);
                found.token1155tx.push(row(r, i as u64));
            }
        }
        _ => {
            tracing::debug!(
                "address history: skipping an unrecognised transfer log for {a}: {log}"
            );
        }
    }
    Ok(())
}

/// `TransferBatch` data: two dynamic `uint256[]`, ids then values, as decimal strings.
fn batch_arrays(data: &str) -> Result<(Vec<String>, Vec<String>)> {
    let word = |i: usize| -> Result<&str> {
        data.get(i * 64..(i + 1) * 64)
            .ok_or_else(|| anyhow!("TransferBatch data too short"))
    };
    let at = |i: usize| -> Result<usize> {
        usize::try_from(alloy_primitives::U256::from_str_radix(word(i)?, 16)?)
            .map_err(|_| anyhow!("TransferBatch offset out of range"))
    };
    let array = |offset_word: usize| -> Result<Vec<String>> {
        let start = at(offset_word)? / 32;
        let len = at(start)?;
        (0..len)
            .map(|k| decimal(&format!("0x{}", word(start + 1 + k)?)))
            .collect()
    };
    let (ids, values) = (array(0)?, array(1)?);
    if ids.len() != values.len() {
        bail!(
            "TransferBatch with {} ids and {} values",
            ids.len(),
            values.len()
        );
    }
    Ok((ids, values))
}

/// The first block at or after `start` that some discovered action has not covered for `address`.
pub fn next_uncovered(history: &AddressHistory, address: &str, start: u64) -> Result<u64> {
    next_uncovered_for(history, address, start, &DISCOVERED)
}

/// [`next_uncovered`] over the given actions.
pub fn next_uncovered_for(
    history: &AddressHistory,
    address: &str,
    start: u64,
    actions: &[Action],
) -> Result<u64> {
    let mut next = u64::MAX;
    for &action in actions {
        let mut at = start;
        for (from, to) in history.coverage(action, address)? {
            if from <= at && at <= to {
                at = to.saturating_add(1);
            }
        }
        next = next.min(at);
    }
    Ok(next)
}

/// Record a window's finds, every action's rows with its coverage, under the generation the
/// window's fetch began in.
pub fn record(
    history: &AddressHistory,
    address: &str,
    (from, to): (u64, u64),
    found: Found,
    generation: u64,
) -> Result<()> {
    for (action, rows) in [
        (Action::TxList, found.txlist),
        (Action::TxListInternal, found.txlistinternal),
        (Action::TokenTx, found.tokentx),
        (Action::TokenNftTx, found.tokennfttx),
        (Action::Token1155Tx, found.token1155tx),
    ] {
        history.record(action, address, &rows, (from, to), generation)?;
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::store::Store;

    #[test]
    fn a_stated_limit_is_read_from_each_providers_wording() {
        assert_eq!(
            stated_limit("-32602: Block range too large; currently limited to 100 blocks"),
            Some(100)
        );
        assert_eq!(
            stated_limit(r#"{"details":{"requestRange":100001,"maxAllowedRange":16384}}"#),
            Some(16_384)
        );
        assert_eq!(
            stated_limit("query exceeds max range of 2000 blocks"),
            Some(2_000)
        );
        assert_eq!(stated_limit("timeout"), None);
        let w = Window::new(100_000);
        assert_eq!(w.shrink("currently limited to 100 blocks"), Some(100));
        assert_eq!(w.shrink("timeout"), Some(50));
        assert_eq!(
            w.shrink("limited to 500 blocks"),
            Some(25),
            "a stated limit above now halves instead"
        );
        assert_eq!(
            w.shrink("401 Unauthorized: invalid API key"),
            None,
            "not about the range"
        );
        assert_eq!(w.get(), 25);

        // Successes grow the span back, never past the stated ceiling.
        for _ in 0..REGROW_AFTER {
            w.succeeded();
        }
        assert_eq!(w.get(), 50);
        for _ in 0..3 * REGROW_AFTER {
            w.succeeded();
        }
        assert_eq!(w.get(), 100, "capped at the stated limit");
    }

    /// A provider that silently truncates at 10,000 results: a span answering that many is split
    /// until each answer is believably whole.
    #[tokio::test]
    async fn a_suspiciously_full_answer_is_split() {
        let rpc = script(|_, p| {
            let s = hex_u64(&p[0]["fromBlock"])?;
            let e = hex_u64(&p[0]["toBlock"])?;
            let n = if e - s + 1 > 2 {
                SUSPICIOUSLY_FULL
            } else {
                (e - s + 1) as usize
            };
            Ok(Value::Array(vec![json!(1); n]))
        });
        let w = Window::new(100_000);
        let got = ranged(
            &rpc,
            &w,
            0,
            3,
            "eth_getLogs",
            |s, e| json!([{ "fromBlock": format!("0x{s:x}"), "toBlock": format!("0x{e:x}") }]),
        )
        .await
        .unwrap();
        assert_eq!(got.len(), 4, "one item per block, not a truncated page");

        // A single block that still answers a full page cannot be split further, and is refused.
        let full = script(|_, _| Ok(Value::Array(vec![json!(1); SUSPICIOUSLY_FULL])));
        let err = ranged(
            &full,
            &Window::new(1),
            7,
            7,
            "eth_getLogs",
            |s, e| json!([{ "fromBlock": format!("0x{s:x}"), "toBlock": format!("0x{e:x}") }]),
        )
        .await
        .unwrap_err();
        assert!(format!("{err:#}").contains("truncated page"), "{err:#}");
    }

    /// A provider that caps spans at 100 blocks, answers one item per block, and finishes later
    /// spans first, so the concurrent path is out of order underneath.
    struct Capped {
        in_flight: AtomicU64,
        most: AtomicU64,
        timed_out_once: AtomicBool,
        fatal_at: Option<u64>,
    }
    impl Capped {
        fn new(fatal_at: Option<u64>) -> Capped {
            Capped {
                in_flight: AtomicU64::new(0),
                most: AtomicU64::new(0),
                timed_out_once: AtomicBool::new(false),
                fatal_at,
            }
        }
    }
    impl Rpc for Capped {
        async fn call(&self, _: &str, p: Value) -> Result<Value> {
            let now = self.in_flight.fetch_add(1, Ordering::SeqCst) + 1;
            self.most.fetch_max(now, Ordering::SeqCst);
            let s = hex_u64(&p[0]["fromBlock"])?;
            let e = hex_u64(&p[0]["toBlock"])?;
            tokio::time::sleep(std::time::Duration::from_millis(8 - s / 100 % 8)).await;
            self.in_flight.fetch_sub(1, Ordering::SeqCst);
            let has = |b: u64| (s..=e).contains(&b);
            if e - s + 1 > 100 {
                bail!("-32602: Block range too large; currently limited to 100 blocks");
            }
            if self.fatal_at.is_some_and(has) {
                bail!("401 Unauthorized: invalid API key");
            }
            if has(1_250) && !self.timed_out_once.swap(true, Ordering::SeqCst) {
                bail!("query timed out");
            }
            if has(1_700) && e - s + 1 > 25 {
                return Ok(Value::Array(vec![json!(-1); SUSPICIOUSLY_FULL]));
            }
            Ok(Value::Array((s..=e).map(|b| json!(b)).collect()))
        }
    }

    fn blocks(s: u64, e: u64) -> Value {
        json!([{ "fromBlock": format!("0x{s:x}"), "toBlock": format!("0x{e:x}") }])
    }

    /// Spans run concurrently once the cap is learned, and the answer is still every block once, in
    /// order, through a stated cap, a mid-range timeout and a page that looks truncated.
    #[tokio::test]
    async fn concurrent_spans_answer_exactly_what_sequential_spans_would() {
        let rpc = Capped::new(None);
        let got = ranged(
            &rpc,
            &Window::new(100_000),
            0,
            2_999,
            "trace_filter",
            blocks,
        )
        .await
        .unwrap();
        let want: Vec<Value> = (0..=2_999u64).map(|b| json!(b)).collect();
        assert_eq!(got, want);
        let most = rpc.most.load(Ordering::SeqCst);
        assert!(
            most > 1 && most <= RANGE_CONCURRENCY as u64,
            "in flight at once: {most}"
        );
    }

    /// An error that says nothing about the range still fails the whole range, concurrent or not.
    #[tokio::test]
    async fn a_non_range_error_in_a_concurrent_batch_fails_the_range() {
        let rpc = Capped::new(Some(2_000));
        let err = ranged(&rpc, &Window::new(100), 0, 2_999, "trace_filter", blocks)
            .await
            .unwrap_err();
        assert!(format!("{err:#}").contains("401"), "{err:#}");
        assert!(
            format!("{err:#}").contains("trace_filter [2000, "),
            "{err:#}"
        );
    }

    /// Answers by span: the first time a span is asked, then every time after.
    struct Spans(
        Mutex<BTreeSet<(u64, u64)>>,
        fn(u64, u64, bool) -> Option<Result<Value>>,
    );
    impl Rpc for Spans {
        async fn call(&self, _: &str, p: Value) -> Result<Value> {
            let s = hex_u64(&p[0]["fromBlock"])?;
            let e = hex_u64(&p[0]["toBlock"])?;
            let first = self.0.lock().unwrap().insert((s, e));
            match (self.1)(s, e, first) {
                Some(answer) => answer,
                None => std::future::pending().await,
            }
        }
    }
    fn rows(s: u64, e: u64) -> Option<Result<Value>> {
        Some(Ok(Value::Array((s..=e).map(|b| json!(b)).collect())))
    }

    /// A fatal answer to a span sent alongside one that needs a smaller size fails the call, though
    /// that span is asked again and answers cleanly the second time.
    #[tokio::test]
    async fn a_fatal_answer_behind_a_refusal_is_not_discarded() {
        let rpc = Spans(Mutex::default(), |s, e, first| match (s, e) {
            (2, 3) => Some(Err(anyhow!("query timed out"))),
            (4, 4) if first => Some(Err(anyhow!("401 Unauthorized"))),
            _ => rows(s, e),
        });
        let err = ranged(&rpc, &Window::new(2), 0, 4, "trace_filter", blocks)
            .await
            .unwrap_err();
        assert!(format!("{err:#}").contains("401"), "{err:#}");
    }

    /// A fatal answer fails the call when it arrives, not when a slower span after it does.
    #[tokio::test]
    async fn a_fatal_answer_is_not_held_up_by_a_slower_span() {
        let rpc = Spans(Mutex::default(), |s, e, _| match (s, e) {
            (2, 3) => Some(Err(anyhow!("401 Unauthorized"))),
            (4, 5) => None,
            _ => rows(s, e),
        });
        let got = tokio::time::timeout(
            std::time::Duration::from_secs(5),
            ranged(&rpc, &Window::new(2), 0, 5, "trace_filter", blocks),
        )
        .await
        .expect("the fatal answer ends the call without waiting for [4, 5]");
        assert!(format!("{:#}", got.unwrap_err()).contains("401"));
    }

    #[test]
    fn a_batch_transfer_decodes_every_id() {
        // ids [1, 2], values [10, 20]
        let words = ["40", "a0", "2", "1", "2", "2", "a", "14"].map(|w| format!("{w:0>64}"));
        let (ids, values) = batch_arrays(&words.concat()).unwrap();
        assert_eq!(ids, ["1", "2"]);
        assert_eq!(values, ["10", "20"]);
    }

    #[test]
    fn a_txlist_record_matches_etherscans_fields() {
        let tx = json!({
            "blockNumber": "0x112a89e", "blockHash": "0xAB", "hash": "0xCD", "nonce": "0x47",
            "transactionIndex": "0x79", "from": "0x31148E9423e6abdcdcf4cd6b13cbebdeca29a9fe",
            "to": null, "value": "0x0", "gas": "0xa0a4", "gasPrice": "0x1", "input": "0x60806040aa",
        });
        let receipt = json!({
            "status": "0x0", "gasUsed": "0x5208", "cumulativeGasUsed": "0x10",
            "effectiveGasPrice": "0x47a9e8d5c", "contractAddress": "0xBEEF",
        });
        let r = txlist_record(&tx, &receipt, 1_693_067_255, false).unwrap();
        assert_eq!(r["blockNumber"], "18000030");
        assert_eq!(r["nonce"], "71");
        assert_eq!(r["to"], "", "a creation has an empty to");
        assert_eq!(r["contractAddress"], "0xbeef");
        assert_eq!(r["gasPrice"], "19237080412", "the price paid, not the cap");
        assert_eq!(r["isError"], "1");
        assert_eq!(r["txreceipt_status"], "0");
        assert_eq!(r["methodId"], "0x60806040");
        assert_eq!(r["timeStamp"], "1693067255");

        // Before Byzantium a receipt has no status, and a failure is the trace's to report.
        let mut old = receipt.clone();
        old.as_object_mut().unwrap().remove("status");
        let r = txlist_record(&tx, &old, 1, true).unwrap();
        assert_eq!(r["isError"], "1");
        assert_eq!(r["txreceipt_status"], "");
        assert_eq!(txlist_record(&tx, &old, 1, false).unwrap()["isError"], "0");
    }

    type Answer = dyn Fn(&str, &Value) -> Result<Value> + Send + Sync;

    /// A scripted endpoint: answers by method, recording what it was asked.
    struct Script {
        answer: Box<Answer>,
        asked: Mutex<Vec<(String, Value)>>,
    }
    impl Rpc for Script {
        async fn call(&self, method: &str, params: Value) -> Result<Value> {
            self.asked
                .lock()
                .unwrap()
                .push((method.to_string(), params.clone()));
            (self.answer)(method, &params)
        }
    }
    fn script(f: impl Fn(&str, &Value) -> Result<Value> + Send + Sync + 'static) -> Script {
        Script {
            answer: Box::new(f),
            asked: Mutex::new(Vec::new()),
        }
    }

    const A: &str = "0xd8da6bf26964af9d7eed9e03e53415d37aa96045";

    impl<M: Rpc, T: Rpc> Discoverer<M, T> {
        /// One window as the cursor runs it: discover, then keep what was fetched either way.
        async fn discover_now(
            &self,
            history: &AddressHistory,
            address: &str,
            from: u64,
            to: u64,
        ) -> Result<Found> {
            let pending = Pending::default();
            let found = self.discover(history, address, from, to, &pending).await;
            pending.persist(history)?;
            found
        }
    }

    fn history(dir: &std::path::Path) -> AddressHistory {
        AddressHistory::open(Store::open(&dir.join("t.redb")).unwrap(), 1, &[A.into()]).unwrap()
    }

    fn tx_json(hash: &str, block: u64, nonce: u64) -> Value {
        json!({
            "blockNumber": format!("0x{block:x}"), "blockHash": "0xbb", "hash": hash,
            "nonce": format!("0x{nonce:x}"), "transactionIndex": "0x0", "from": A, "to": "0x01",
            "value": "0x0", "gas": "0x5208", "gasPrice": "0x1", "input": "0x",
        })
    }

    fn receipt_json() -> Value {
        json!({"status": "0x1", "gasUsed": "0x5208", "cumulativeGasUsed": "0x5208",
               "effectiveGasPrice": "0x1", "contractAddress": null, "blockHash": "0xbb"})
    }

    /// A chain where `A` sends one transaction at block 150 (nonce 0 -> 1), with a trace source
    /// that refuses spans over 100 blocks and a log source that refuses spans over 64.
    fn chain(empty_traces_have_no_data: bool) -> (Script, Script) {
        let main = script(move |m, p| {
            Ok(match m {
                "eth_getCode" => json!("0x"),
                "eth_getTransactionCount" => {
                    let b =
                        u64::from_str_radix(p[1].as_str().unwrap().trim_start_matches("0x"), 16)?;
                    json!(if b >= 150 { "0x1" } else { "0x0" })
                }
                "eth_getLogs" => {
                    let f = &p[0];
                    let s = hex_u64(&f["fromBlock"])?;
                    let e = hex_u64(&f["toBlock"])?;
                    if e - s + 1 > 64 {
                        bail!(
                            "getLogs request exceeded max allowed range {{\"maxAllowedRange\":64}}"
                        );
                    }
                    json!([])
                }
                "eth_getTransactionByHash" => tx_json(p[0].as_str().unwrap(), 150, 0),
                "eth_getTransactionReceipt" => receipt_json(),
                "eth_getBlockByNumber" => json!({"timestamp": "0x10"}),
                "eth_getBlockTransactionCountByNumber" => json!("0x5"),
                other => bail!("unexpected {other}"),
            })
        });
        let trace = script(move |m, p| {
            Ok(match m {
                "trace_filter" => {
                    let f = &p[0];
                    let s = hex_u64(&f["fromBlock"])?;
                    let e = hex_u64(&f["toBlock"])?;
                    if e - s + 1 > 100 {
                        bail!("Block range too large; currently limited to 100 blocks");
                    }
                    if f.get("fromAddress").is_some() && s <= 150 && 150 <= e {
                        json!([{"transactionHash": "0xaa", "blockNumber": 150, "traceAddress": [],
                                "type": "call", "action": {"from": A, "to": "0x01"}}])
                    } else if f.get("toAddress").is_some() && s <= 300 && 300 <= e {
                        // An internal call to A: a txlistinternal row, never a txlist one.
                        json!([{"transactionHash": "0xbb", "blockNumber": 300, "traceAddress": [0],
                                "type": "call", "action": {"from": "0x02", "to": A}}])
                    } else {
                        json!([])
                    }
                }
                "trace_block" => {
                    if empty_traces_have_no_data {
                        json!([])
                    } else {
                        json!([{"x": 1}])
                    }
                }
                other => bail!("unexpected {other}"),
            })
        });
        (main, trace)
    }

    #[tokio::test]
    async fn a_window_finds_a_sent_transaction_and_learns_the_limits() {
        let dir = tempfile::tempdir().unwrap();
        let h = history(dir.path());
        let (main, trace) = chain(false);
        let d = Discoverer::new(main, trace);
        let found = d.discover_now(&h, A, 0, 499).await.unwrap();
        assert_eq!(
            found.txlist.len(),
            1,
            "the internal call at 300 is not a normal transaction"
        );
        assert_eq!(found.txlist[0].record["hash"], "0xaa");
        assert_eq!(
            d.trace_window.get(),
            100,
            "learned from the trace source's refusal"
        );
        assert_eq!(
            d.log_window.get(),
            64,
            "learned from the log source's refusal"
        );

        // Hydrated once: after the window is recorded, a second pass asks for no transaction again.
        record(&h, A, (0, 499), found, h.generation().unwrap()).unwrap();
        let before = d.main.asked.lock().unwrap().len();
        d.discover_now(&h, A, 0, 499).await.unwrap();
        let again: Vec<String> = d.main.asked.lock().unwrap()[before..]
            .iter()
            .map(|(m, _)| m.clone())
            .collect();
        assert!(
            !again
                .iter()
                .any(|m| m == "eth_getTransactionByHash" || m == "eth_getTransactionReceipt"),
            "{again:?}"
        );
    }

    /// A contract's txlist starts with the transaction that deployed it, a root frame with no `to`
    /// that names the contract only in `result.address`; one made by a factory is internal.
    #[tokio::test]
    async fn a_contract_created_by_a_transaction_lists_its_creation() {
        let (main, _) = chain(false);
        let main = script(move |m, p| match m {
            "eth_getCode" => Ok(json!("0x6080")),
            _ => (main.answer)(m, p),
        });
        let trace = script(|m, p| {
            Ok(match m {
                "trace_filter" if p[0].get("toAddress").is_some() => json!([
                    {"transactionHash": "0xcc", "blockNumber": 400, "traceAddress": [], "type": "create",
                     "action": {"from": "0x02", "value": "0x0", "init": "0x6080"},
                     "result": {"address": A, "code": "0x6080"}},
                    {"transactionHash": "0xdd", "blockNumber": 450, "traceAddress": [0], "type": "create",
                     "action": {"from": "0x03", "value": "0x0", "init": "0x6080"},
                     "result": {"address": A, "code": "0x6080"}},
                ]),
                "trace_filter" => json!([]),
                "trace_block" => json!([{"x": 1}]),
                other => bail!("unexpected {other}"),
            })
        });
        let d = Discoverer::new(main, trace);
        let (roots, internal) = d.normal_transactions(A, 400, 499).await.unwrap();
        let hashes: Vec<&str> = roots.iter().map(|r| r.hash.as_str()).collect();
        assert_eq!(hashes, ["0xcc"]);
        assert!(!roots[0].outgoing);
        assert_eq!(
            internal,
            ["0xdd"],
            "the factory's creation is an internal transaction"
        );
    }

    /// Two providers that disagree about where a transaction is give no row, not a mixture.
    #[tokio::test]
    async fn a_transaction_the_providers_place_differently_is_refused() {
        let dir = tempfile::tempdir().unwrap();
        let h = history(dir.path());
        let (main, trace) = chain(false);
        let main = script(move |m, p| match m {
            "eth_getTransactionByHash" => Ok(tx_json(p[0].as_str().unwrap(), 151, 0)),
            _ => (main.answer)(m, p),
        });
        let d = Discoverer::new(main, trace);
        let err = d.discover_now(&h, A, 100, 199).await.unwrap_err();
        assert!(
            format!("{err:#}").contains("not recording a mixture"),
            "{err:#}"
        );
    }

    /// A probe block that really is empty proves nothing either way, so the next one is tried.
    #[tokio::test]
    async fn an_empty_probe_block_is_passed_over() {
        let dir = tempfile::tempdir().unwrap();
        let h = history(dir.path());
        let (main, _) = chain(false);
        // Blocks 1,000 to 1,099 have no transactions except the first; only it has traces.
        let main = script(move |m, p| match m {
            "eth_getBlockTransactionCountByNumber" => Ok(json!(if hex_u64(&p[0])? == 1_000 {
                "0x3"
            } else {
                "0x0"
            })),
            _ => (main.answer)(m, p),
        });
        let trace = script(|m, p| match m {
            "trace_filter" => Ok(json!([])),
            "trace_block" => Ok(if hex_u64(&p[0])? == 1_000 {
                json!([{"x": 1}])
            } else {
                json!([])
            }),
            other => bail!("unexpected {other}"),
        });
        let d = Discoverer::new(main, trace);
        d.discover_now(&h, A, 1_000, 1_099)
            .await
            .expect("the empty midpoint is skipped and block 1,000 has traces");
    }

    /// One direction answering rows says nothing about the other's empty answer: an incoming
    /// transaction the toAddress filter dropped, while fromAddress found the outgoing one, must
    /// keep the window uncovered.
    #[tokio::test]
    async fn an_empty_side_is_checked_though_the_other_found_rows() {
        let dir = tempfile::tempdir().unwrap();
        let h = history(dir.path());
        let (main, trace) = chain(false);
        let incoming = json!({"transactionHash": "0xdd", "blockNumber": 149, "traceAddress": [],
            "type": "call", "action": {"from": "0x09", "to": A, "value": "0x1"}});
        let trace = script(move |m, p| match m {
            // toAddress silently answers nothing, though block 149 pays A.
            "trace_filter" if p[0].get("toAddress").is_some() => Ok(json!([])),
            "trace_block" if hex_u64(&p[0])? == 149 => Ok(json!([incoming, {"x": 1}])),
            _ => (trace.answer)(m, p),
        });
        let d = Discoverer::new(main, trace);
        let err = d.discover_now(&h, A, 100, 199).await.unwrap_err();
        assert!(
            format!("{err:#}").contains("trace_filter toAddress answered nothing"),
            "{err:#}"
        );
    }

    /// The same for the positional log filters: rows with the address in topic 1 say nothing about
    /// an empty answer for topic 2.
    #[tokio::test]
    async fn an_empty_log_filter_is_checked_though_another_found_rows() {
        let dir = tempfile::tempdir().unwrap();
        let h = history(dir.path());
        let who = padded(A);
        let other = padded("0x0000000000000000000000000000000000000009");
        let log = |topics: Value| {
            json!({"address": "0x7070", "topics": topics, "data": format!("0x{:0>64}", "1"),
                   "blockNumber": "0x1f", "blockTimestamp": "0x20", "transactionHash": "0xcc",
                   "transactionIndex": "0x0", "logIndex": "0x0", "blockHash": "0xbb", "removed": false})
        };
        let sent = log(json!([TRANSFER, who, other]));
        let received = log(json!([TRANSFER, other, who]));
        let (main, trace) = chain(false);
        let main = script(move |m, p| match m {
            "eth_getLogs" => {
                let f = &p[0];
                let single = f["fromBlock"] == f["toBlock"];
                let topics = f["topics"].as_array().unwrap();
                Ok(
                    if single && topics.len() == 1 && topics[0] == json!(TRANSFER) {
                        // The probe: block 31 holds a transfer to A.
                        json!([received.clone()])
                    } else if topics.len() == 2 && topics[0] == json!(TRANSFER) {
                        json!([sent.clone()])
                    } else {
                        json!([])
                    },
                )
            }
            _ => (main.answer)(m, p),
        });
        let d = Discoverer::new(main, trace);
        let err = d.discover_now(&h, A, 0, 63).await.unwrap_err();
        assert!(format!("{err:#}").contains("eth_getLogs"), "{err:#}");
        assert!(format!("{err:#}").contains("answered nothing"), "{err:#}");
    }

    #[tokio::test]
    async fn a_trace_source_with_no_traces_is_not_believed() {
        let dir = tempfile::tempdir().unwrap();
        let h = history(dir.path());
        let (main, trace) = chain(true);
        let d = Discoverer::new(main, trace);
        let err = d.discover_now(&h, A, 1_000, 1_099).await.unwrap_err();
        assert!(
            format!("{err:#}").contains("no traces for block"),
            "{err:#}"
        );
    }

    #[tokio::test]
    async fn a_nonce_that_moved_without_a_trace_fails_the_window() {
        let dir = tempfile::tempdir().unwrap();
        let h = history(dir.path());
        let (main, _) = chain(false);
        // A trace source that sees nothing at all, though the nonce moved at 150.
        let blind = script(|m, _| match m {
            "trace_filter" => Ok(json!([])),
            "trace_block" => Ok(json!([{"x": 1}])),
            other => bail!("unexpected {other}"),
        });
        let d = Discoverer::new(main, blind);
        let err = d.discover_now(&h, A, 100, 199).await.unwrap_err();
        assert!(format!("{err:#}").contains("by its nonce"), "{err:#}");
    }

    /// Code at the window's end means a contract or a delegated account, whose nonce does not count
    /// sent transactions; code only later (a delegation made since) still leaves the window checked.
    #[tokio::test]
    async fn the_nonce_check_reads_code_at_the_windows_end() {
        let dir = tempfile::tempdir().unwrap();
        let h = history(dir.path());
        let blind = || {
            script(|m, _| match m {
                "trace_filter" => Ok(json!([])),
                "trace_block" => Ok(json!([{"x": 1}])),
                other => bail!("unexpected {other}"),
            })
        };
        let code_from = |delegated_at: u64| {
            let (main, _) = chain(false);
            script(move |m, p| match m {
                "eth_getCode" => {
                    let b = hex_u64(&p[1])?;
                    Ok(json!(if b >= delegated_at {
                        "0xef0100aa"
                    } else {
                        "0x"
                    }))
                }
                _ => (main.answer)(m, p),
            })
        };
        let d = Discoverer::new(code_from(150), blind());
        d.discover_now(&h, A, 100, 199)
            .await
            .expect("delegated by the window's end, so the nonce is not held to the traces");
        let d = Discoverer::new(code_from(10_000), blind());
        let err = d.discover_now(&h, A, 100, 199).await.unwrap_err();
        assert!(format!("{err:#}").contains("by its nonce"), "{err:#}");
    }

    /// A removed log in a range past finality means the provider's view moved under us: the window
    /// fails rather than quietly losing the row.
    #[tokio::test]
    async fn a_removed_log_fails_the_window() {
        let dir = tempfile::tempdir().unwrap();
        let h = history(dir.path());
        let (main, trace) = chain(false);
        let removed = json!({"address": "0xT0KEN", "topics": [TRANSFER, padded(A), padded(A)],
            "data": format!("0x{:0>64}", "1"), "blockNumber": "0x10", "blockTimestamp": "0x20",
            "transactionHash": "0xHH", "transactionIndex": "0x1", "logIndex": "0x1",
            "blockHash": "0xBB", "removed": true});
        let main = script(move |m, p| match m {
            "eth_getLogs" => Ok(json!([removed])),
            _ => (main.answer)(m, p),
        });
        let d = Discoverer::new(main, trace);
        let err = d.discover_now(&h, A, 0, 63).await.unwrap_err();
        assert!(format!("{err:#}").contains("removed log"), "{err:#}");
    }

    /// A provider that returns a short counterparty topic fails the window with an error; slicing it
    /// panicked and took the cursor down while /ready still read ready.
    #[tokio::test]
    async fn a_short_topic_fails_the_window_without_panicking() {
        let dir = tempfile::tempdir().unwrap();
        let h = history(dir.path());
        let short = json!({"address": "0xT0KEN", "topics": [TRANSFER, padded(A), "0x1234"],
            "data": format!("0x{:0>64}", "1"), "blockNumber": "0x10", "blockTimestamp": "0x20",
            "transactionHash": "0xHH", "transactionIndex": "0x1", "logIndex": "0x1",
            "blockHash": "0xBB", "removed": false});
        let (main, trace) = chain(false);
        let main = script(move |m, p| match m {
            "eth_getLogs" => Ok(json!([short])),
            _ => (main.answer)(m, p),
        });
        let d = Discoverer::new(main, trace);
        let err = d.discover_now(&h, A, 0, 63).await.unwrap_err();
        assert!(
            format!("{err:#}").contains("not a 32-byte topic"),
            "{err:#}"
        );
    }

    #[tokio::test]
    async fn token_logs_become_rows_by_standard_and_dedupe() {
        let dir = tempfile::tempdir().unwrap();
        let h = history(dir.path());
        let who = padded(A);
        let other = padded("0x0000000000000000000000000000000000000001");
        let log = |topics: Value, data: &str, index: u64| {
            json!({"address": "0xT0KEN", "topics": topics, "data": data, "blockNumber": "0x10",
                   "blockTimestamp": "0x20", "transactionHash": "0xHH", "transactionIndex": "0x1",
                   "logIndex": format!("0x{index:x}"), "blockHash": "0xBB", "removed": false})
        };
        let erc20 = log(json!([TRANSFER, who, who]), &format!("0x{:0>64}", "64"), 1);
        let nft = log(
            json!([TRANSFER, other, who, format!("0x{:0>64}", "7")]),
            "0x",
            2,
        );
        let single = log(
            json!([TRANSFER_SINGLE, other, other, who]),
            &format!("0x{:0>64}{:0>64}", "5", "3"),
            3,
        );
        let (main, trace) = chain(false);
        let logs = vec![erc20.clone(), erc20, nft, single];
        let main = script(move |m, p| match m {
            "eth_getLogs" => Ok(json!(logs)),
            _ => (main.answer)(m, p),
        });
        let d = Discoverer::new(main, trace);
        let found = d.discover_now(&h, A, 0, 63).await.unwrap();
        assert_eq!(
            found.tokentx.len(),
            1,
            "the same log through two filters is one row"
        );
        assert_eq!(found.tokentx[0].record["value"], "100");
        assert_eq!(
            found.tokentx[0].record["timeStamp"], "32",
            "from the log's blockTimestamp"
        );
        assert_eq!(found.tokennfttx.len(), 1);
        assert_eq!(found.tokennfttx[0].record["tokenID"], "7");
        assert_eq!(found.token1155tx.len(), 1);
        assert_eq!(found.token1155tx[0].record["tokenValue"], "3");
    }

    #[test]
    fn next_uncovered_is_the_least_covered_action() {
        let dir = tempfile::tempdir().unwrap();
        let h = history(dir.path());
        let g = h.generation().unwrap();
        assert_eq!(next_uncovered(&h, A, 10).unwrap(), 10);
        record(&h, A, (10, 99), Found::default(), g).unwrap();
        assert_eq!(next_uncovered(&h, A, 10).unwrap(), 100);
        h.record(Action::TxList, A, &[], (100, 199), g).unwrap();
        assert_eq!(
            next_uncovered(&h, A, 10).unwrap(),
            100,
            "token coverage still ends at 99"
        );
    }

    fn mode(h: &AddressHistory, end: u64) -> crate::address_mode::ModeState {
        crate::address_mode::ModeState::new(h.clone(), 1, std::time::Duration::from_secs(300), true)
            .with_range(0, Some(end), 0)
    }

    fn filtered_from(asked: &Mutex<Vec<(String, Value)>>) -> Vec<u64> {
        asked
            .lock()
            .unwrap()
            .iter()
            .filter(|(m, _)| m == "trace_filter")
            .map(|(_, p)| hex_u64(&p[0]["fromBlock"]).unwrap())
            .collect()
    }

    /// The cursor's catch-up covers every window to `end_block`, and a later pass starts where
    /// coverage ends rather than refetching.
    #[tokio::test]
    async fn catch_up_covers_to_the_end_and_resumes_from_coverage() {
        use crate::address_mode::Discovery;
        let dir = tempfile::tempdir().unwrap();
        let h = history(dir.path());
        let (main, trace) = chain(false);
        let d = Discoverer::new(Counted::new(main), Counted::new(trace));

        d.catch_up(&mode(&h, 99_999), 1_000_000).await;
        for action in DISCOVERED {
            assert_eq!(
                h.coverage(action, A).unwrap(),
                vec![(0, 99_999)],
                "{action:?}"
            );
        }
        let req = crate::address_history::PageRequest {
            address: A.into(),
            start_block: Some(0),
            end_block: Some(99_999),
            sort: crate::address_history::Sort::Asc,
            page: 1,
            offset: 10,
            generation: None,
        };
        let crate::address_history::Answer::Rows(rows) = h.page(Action::TxList, &req).unwrap()
        else {
            panic!("covered")
        };
        assert_eq!(rows.len(), 1, "the transaction at block 150");

        let first_pass = filtered_from(&d.trace.inner().asked).len();
        d.catch_up(&mode(&h, 149_999), 1_000_000).await;
        let second: Vec<u64> = filtered_from(&d.trace.inner().asked)[first_pass..].to_vec();
        assert!(!second.is_empty());
        assert!(
            second.iter().all(|b| *b >= 100_000),
            "the second pass refetched below its coverage: {:?}",
            second.iter().min()
        );
        assert_eq!(h.coverage(Action::TxList, A).unwrap(), vec![(0, 149_999)]);
    }

    /// A window that hydrated a transaction and then failed keeps the hydration, so its retry does
    /// not fetch that transaction again.
    #[tokio::test]
    async fn a_failed_window_keeps_what_it_hydrated() {
        use crate::address_mode::Discovery;
        let dir = tempfile::tempdir().unwrap();
        let h = history(dir.path());
        let log = json!({"address": "0x7070", "topics": [TRANSFER, padded(A), padded(A)],
            "data": format!("0x{:0>64}", "1"), "blockNumber": "0x10", "transactionHash": "0xcc",
            "transactionIndex": "0x0", "logIndex": "0x0", "blockHash": "0xbb", "removed": false});
        // The log carries no timestamp and its block's header fails: the window fails after the
        // transaction at block 150 is hydrated.
        let failing = |headers_ok: bool| {
            let (main, trace) = chain(false);
            let log = log.clone();
            let main = script(move |m, p| match m {
                "eth_getLogs" => Ok(json!([log])),
                "eth_getBlockByNumber" if hex_u64(&p[0])? == 0x10 && !headers_ok => {
                    bail!("header unavailable")
                }
                _ => (main.answer)(m, p),
            });
            Discoverer::new(Counted::new(main), Counted::new(trace))
        };
        let first = failing(false);
        first.catch_up(&mode(&h, 99_999), 1_000_000).await;
        assert!(h.coverage(Action::TxList, A).unwrap().is_empty());
        assert_eq!(first.main.calls().get("eth_getTransactionByHash"), Some(&1));

        let second = failing(true);
        second.catch_up(&mode(&h, 99_999), 1_000_000).await;
        assert_eq!(h.coverage(Action::TxList, A).unwrap(), vec![(0, 99_999)]);
        assert_eq!(
            second.main.calls().get("eth_getTransactionByHash"),
            None,
            "the retry hydrated a transaction the failed window already had"
        );
    }

    #[tokio::test]
    async fn a_failed_window_records_nothing() {
        use crate::address_mode::Discovery;
        let dir = tempfile::tempdir().unwrap();
        let h = history(dir.path());
        let (main, _) = chain(false);
        let down = script(|_, _| bail!("connection refused"));
        let d = Discoverer::new(Counted::new(main), Counted::new(down));
        d.catch_up(&mode(&h, 99_999), 1_000_000).await;
        for action in DISCOVERED {
            assert!(h.coverage(action, A).unwrap().is_empty(), "{action:?}");
        }
    }

    fn frame(ta: &[u64], kind: &str, action: Value, result: Value, error: Option<&str>) -> Value {
        json!({"traceAddress": ta, "type": kind, "action": action, "result": result,
               "error": error, "blockNumber": 19_000_016, "transactionPosition": 5,
               "transactionHash": "0x93a7"})
    }

    /// Mainnet transaction 0x93a7cdc1…a917 (block 19,000,016), against Etherscan's answer for it:
    /// a reverted call carrying value, a delegatecall Etherscan omits, and a call that succeeded
    /// inside a reverted parent, which Etherscan lists as failed with no error code of its own.
    #[test]
    fn internal_records_follow_etherscan_on_a_failed_ancestor() {
        let call = |from: &str, to: &str, value: &str, ct: &str| json!({"from": from, "to": to, "value": value, "callType": ct, "gas": "0x10"});
        let frames = [
            frame(
                &[],
                "call",
                call("0xd857", "0x881d", "0x4e28e2290f0000", "call"),
                json!(null),
                Some("Reverted"),
            ),
            frame(
                &[0],
                "call",
                call("0x881d", "0x74de", "0x4e28e2290f0000", "call"),
                json!(null),
                Some("Reverted"),
            ),
            frame(
                &[0, 0],
                "call",
                call("0x74de", "0x7cdf", "0x4e28e2290f0000", "delegatecall"),
                json!(null),
                Some("Reverted"),
            ),
            frame(
                &[0, 0, 0],
                "call",
                call("0x74de", "0x1111", "0x4d79ce42f07800", "call"),
                json!(null),
                Some("Reverted"),
            ),
            frame(
                &[0, 0, 0, 0],
                "call",
                call("0x1111", "0xc02a", "0x4d79ce42f07800", "call"),
                json!({"gasUsed": "0x5da6"}),
                None,
            ),
            frame(
                &[0, 0, 0, 1],
                "call",
                call("0x1111", "0xc02a", "0x0", "call"),
                json!({"gasUsed": "0x0"}),
                None,
            ),
        ];
        let rows = internal_records(&frames, 1_705_173_747).unwrap();
        let got: Vec<(String, String, String, String, String, String)> = rows
            .iter()
            .map(|r| {
                let s = |k: &str| r[k].as_str().unwrap().to_string();
                (
                    s("to"),
                    s("value"),
                    s("isError"),
                    s("errCode"),
                    s("traceId"),
                    s(TRACE_INDEX),
                )
            })
            .collect();
        let want = [
            (
                "0x74de",
                "22000000000000000",
                "1",
                "execution reverted",
                "0_1",
                "1",
            ),
            (
                "0x1111",
                "21807500000000000",
                "1",
                "execution reverted",
                "0_1_1_1",
                "3",
            ),
            ("0xc02a", "21807500000000000", "1", "", "0_1_1_1_1", "4"),
        ]
        .map(|(a, b, c, d, e, f)| {
            (
                a.to_string(),
                b.to_string(),
                c.to_string(),
                d.to_string(),
                e.to_string(),
                f.to_string(),
            )
        });
        assert_eq!(got, want);
        assert_eq!(rows[2]["gasUsed"], "23974");
        assert_eq!(rows[0]["transactionIndex"], "5");
    }

    /// Mainnet transaction 0x84bde65f…4ee8 (block 19,000,010): a CREATE2 at zero value, which
    /// Etherscan lists by its opcode, and the new contract's selfdestruct paying its beneficiary.
    #[test]
    fn internal_records_list_creations_and_selfdestructs() {
        let frames = [
            frame(
                &[],
                "call",
                json!({"from": "0xa7fb", "to": "0xc77a", "value": "0x0", "callType": "call"}),
                json!({"gasUsed": "0x1"}),
                None,
            ),
            frame(
                &[0],
                "create",
                json!({"from": "0xc77a", "value": "0x0", "gas": "0x92ca", "creationMethod": "create2"}),
                json!({"address": "0x1d29", "gasUsed": "0x22e3"}),
                None,
            ),
            frame(
                &[0, 2],
                "suicide",
                json!({"address": "0x1d29", "refundAddress": "0xa7fb", "balance": "0x71afd498d0000"}),
                json!(null),
                None,
            ),
        ];
        let rows = internal_records(&frames, 1).unwrap();
        assert_eq!(rows.len(), 2);
        let (create, kill) = (&rows[0], &rows[1]);
        assert_eq!(
            (
                create["type"].as_str(),
                create["contractAddress"].as_str(),
                create["to"].as_str()
            ),
            (Some("create2"), Some("0x1d29"), Some(""))
        );
        assert_eq!(
            (create["gas"].as_str(), create["gasUsed"].as_str()),
            (Some("37578"), Some("8931"))
        );
        assert_eq!(
            (
                kill["type"].as_str(),
                kill["from"].as_str(),
                kill["to"].as_str(),
                kill["value"].as_str()
            ),
            (
                Some("self-destruct"),
                Some("0x1d29"),
                Some("0xa7fb"),
                Some("2000000000000000")
            )
        );
        assert_eq!(
            (kill["gas"].as_str(), kill["traceId"].as_str()),
            (Some("0"), Some("0_1_1"))
        );
        assert!(
            touches(create, "0x1d29"),
            "a creation touches the contract it creates"
        );
    }

    /// A window finds an internal transfer to the watched address, traces its transaction once, and
    /// answers the same frame by address and by txhash under the same position.
    #[tokio::test]
    async fn an_internal_transfer_is_found_and_answered_both_ways() {
        let dir = tempfile::tempdir().unwrap();
        let h = history(dir.path());
        let (main, _) = chain(false);
        let tx = "0x00000000000000000000000000000000000000000000000000000000000000ee";
        let whole = json!([
            {"traceAddress": [], "type": "call", "transactionHash": tx, "blockNumber": 300,
             "transactionPosition": 2, "action": {"from": "0x02", "to": "0x03", "value": "0x0", "callType": "call"}},
            {"traceAddress": [0], "type": "call", "transactionHash": tx, "blockNumber": 300,
             "transactionPosition": 2, "action": {"from": "0x03", "to": "0x04", "value": "0x0", "callType": "call"}},
            {"traceAddress": [0, 0], "type": "call", "transactionHash": tx, "blockNumber": 300,
             "transactionPosition": 2, "action": {"from": "0x03", "to": A, "value": "0x5", "callType": "call", "gas": "0x8fc"},
             "result": {"gasUsed": "0x0"}},
        ]);
        let traced = whole.clone();
        let trace = script(move |m, p| match m {
            "trace_filter" => {
                let f = &p[0];
                let (s, e) = (hex_u64(&f["fromBlock"])?, hex_u64(&f["toBlock"])?);
                Ok(if f.get("toAddress").is_some() && s <= 300 && 300 <= e {
                    json!([traced[2]])
                } else {
                    json!([])
                })
            }
            "trace_transaction" => Ok(whole.clone()),
            "trace_block" => Ok(json!([{"x": 1}])),
            other => bail!("unexpected {other}"),
        });
        let d = Discoverer::new(main, trace);
        let key = alloy_primitives::hex::decode(&tx[2..]).unwrap();
        // Served only through block 299, the trace of block 300 is neither used nor kept.
        h.set_head(299).unwrap();
        let err = d.discover_now(&h, A, 300, 399).await.unwrap_err();
        assert!(err.downcast_ref::<NotFinalized>().is_some(), "{err:#}");
        assert!(h.tx_internals(&key).unwrap().is_none());

        h.set_head(400).unwrap();
        let found = d.discover_now(&h, A, 300, 399).await.unwrap();
        assert_eq!(found.txlistinternal.len(), 1);
        let row = &found.txlistinternal[0];
        assert_eq!(row.record["hash"], tx);
        assert_eq!(row.record["traceId"], "0_1_1");
        assert_eq!(row.record["value"], "5");
        assert_eq!(row.position, 2, "pre-order position in the whole trace");
        let keys: Vec<&String> = row.record.keys().take(4).collect();
        assert_eq!(
            keys,
            ["blockNumber", "transactionIndex", "timeStamp", "hash"]
        );

        let by_hash = crate::address_history::respond(
            Some(&h),
            &[
                ("module", "account"),
                ("action", "txlistinternal"),
                ("txhash", tx),
            ]
            .iter()
            .map(|(k, v)| (k.to_string(), v.to_string()))
            .collect(),
        );
        let only = &by_hash["result"][0];
        assert_eq!(
            only[TRACE_INDEX], row.record[TRACE_INDEX],
            "the same frame either way"
        );
        assert!(
            only.get("traceId").is_none(),
            "Etherscan's txhash form has no traceId"
        );
        assert!(only.get("hash").is_none());
        let traced_calls = d
            .trace
            .asked
            .lock()
            .unwrap()
            .iter()
            .filter(|(m, _)| m == "trace_transaction")
            .count();
        assert_eq!(traced_calls, 2, "once refused as not final, once kept");

        // A reorg below block 300 takes the trace with it; one above leaves it.
        h.invalidate_above(300).unwrap();
        assert!(h.tx_internals(&key).unwrap().is_some());
        h.invalidate_above(299).unwrap();
        assert!(h.tx_internals(&key).unwrap().is_none());

        // A trace fetched before a reorg and persisted after it is not kept.
        h.set_head(400).unwrap();
        let pending = Pending::default();
        d.internal_rows_of(&h, tx, &pending, 400).await.unwrap();
        h.invalidate_above(299).unwrap();
        pending.persist(&h).unwrap();
        assert!(h.tx_internals(&key).unwrap().is_none());
    }

    /// Etherscan's getminedblocks also lists uncles; this nest serves produced blocks only, and says
    /// so rather than answering an uncle query with blocks.
    #[test]
    fn getminedblocks_answers_blocks_only() {
        let dir = tempfile::tempdir().unwrap();
        let h = history(dir.path());
        h.record(
            Action::MinedBlocks,
            A,
            &[],
            (0, 10),
            h.generation().unwrap(),
        )
        .unwrap();
        let ask = |kind: &str| {
            crate::address_history::respond(
                Some(&h),
                &[
                    ("module", "account"),
                    ("action", "getminedblocks"),
                    ("address", A),
                    ("startblock", "0"),
                    ("endblock", "10"),
                    ("blocktype", kind),
                ]
                .iter()
                .map(|(k, v)| (k.to_string(), v.to_string()))
                .collect(),
            )
        };
        assert_eq!(ask("blocks")["status"], "1");
        assert!(ask("uncles")["result"]
            .as_str()
            .unwrap()
            .starts_with("NUTHATCH_UNSUPPORTED:"));
    }

    #[test]
    fn a_txhash_not_yet_traced_is_unsupported_not_empty() {
        let dir = tempfile::tempdir().unwrap();
        let h = history(dir.path());
        let ask = |action: &str, hash: &str| {
            crate::address_history::respond(
                Some(&h),
                &[("module", "account"), ("action", action), ("txhash", hash)]
                    .iter()
                    .map(|(k, v)| (k.to_string(), v.to_string()))
                    .collect(),
            )
        };
        let unseen = format!("0x{}", "ab".repeat(32));
        assert!(ask("txlistinternal", &unseen)["result"]
            .as_str()
            .unwrap()
            .starts_with("NUTHATCH_UNSUPPORTED:"));
        assert!(ask("txlist", &unseen)["result"]
            .as_str()
            .unwrap()
            .starts_with("NUTHATCH_UNSUPPORTED:"));
        assert_eq!(ask("txlistinternal", "0x12")["status"], "0");
    }

    /// The server traces a txhash it has not seen, once, keeps it only if its block is final, and
    /// traces nothing for a request it would refuse anyway.
    #[tokio::test]
    async fn a_cold_txhash_is_traced_once_and_only_when_final() {
        use tower::ServiceExt;
        let dir = tempfile::tempdir().unwrap();
        let h = history(dir.path());
        let tx = format!("0x{}", "ee".repeat(32));
        let whole = json!([
            {"traceAddress": [], "type": "call", "blockNumber": 300, "transactionPosition": 1,
             "action": {"from": "0x02", "to": "0x03", "value": "0x0", "callType": "call"}},
            {"traceAddress": [0], "type": "call", "blockNumber": 300, "transactionPosition": 1,
             "action": {"from": "0x03", "to": "0x04", "value": "0x9", "callType": "call", "gas": "0x1"},
             "result": {"gasUsed": "0x0"}},
        ]);
        let (main, _) = chain(false);
        let trace = script(move |m, _| match m {
            "trace_transaction" => Ok(whole.clone()),
            other => bail!("unexpected {other}"),
        });
        let d = std::sync::Arc::new(Discoverer::new(Counted::new(main), Counted::new(trace)));
        let verified = std::sync::Arc::new(tokio::sync::OnceCell::new());
        verified.set(()).unwrap();
        let state = std::sync::Arc::new(
            crate::address_mode::ModeState::new(
                h.clone(),
                1,
                std::time::Duration::from_secs(300),
                true,
            )
            .with_tracer(crate::address_mode::tracer(d.clone(), h.clone(), verified)),
        );
        let get = |path: String| {
            let app = crate::address_mode::router(state.clone());
            async move {
                let resp = app
                    .oneshot(
                        axum::http::Request::get(path)
                            .body(axum::body::Body::empty())
                            .unwrap(),
                    )
                    .await
                    .unwrap();
                let bytes = axum::body::to_bytes(resp.into_body(), usize::MAX)
                    .await
                    .unwrap();
                serde_json::from_slice::<Value>(&bytes).unwrap()
            }
        };
        let traced = || {
            d.trace
                .calls()
                .get("trace_transaction")
                .copied()
                .unwrap_or(0)
        };
        let path =
            |extra: &str| format!("/api?module=account&action=txlistinternal&txhash={tx}{extra}");

        let refused = get(format!(
            "/api?module=contract&action=txlistinternal&txhash={tx}"
        ))
        .await;
        assert_eq!(refused["status"], "0");
        let wrong_chain = get(path("&chainid=8453")).await;
        assert_eq!(wrong_chain["status"], "0");
        assert_eq!(traced(), 0, "nothing traced for requests respond refuses");

        h.set_head(299).unwrap();
        let young = get(path("")).await;
        assert!(
            young["result"]
                .as_str()
                .unwrap()
                .starts_with("NUTHATCH_INCOMPLETE:"),
            "{young}"
        );
        assert!(h
            .tx_internals(&alloy_primitives::hex::decode(&tx[2..]).unwrap())
            .unwrap()
            .is_none());

        h.set_head(400).unwrap();
        let first = get(path("")).await;
        assert_eq!(first["result"][0]["value"], "9", "{first}");
        let calls = traced();
        let again = get(path("")).await;
        assert_eq!(again["result"], first["result"]);
        assert_eq!(
            traced(),
            calls,
            "the second lookup is answered from the store"
        );
    }

    const B: &str = "0x00000000219ab540356cbb839cbe05303d7705fa";

    /// Bodies for blocks past Shanghai: block x02 pays A twice and B once, block x05 is B's.
    fn bodies(missing_withdrawals_at: Option<u64>) -> Script {
        script(move |m, p| {
            Ok(match m {
                "eth_getBlockByNumber" => {
                    let b = hex_u64(&p[0])?;
                    let mut h = json!({"number": format!("0x{b:x}"), "timestamp": format!("0x{:x}", 1_000 + b),
                        "miner": if b % 10 == 5 { B } else { "0x0000000000000000000000000000000000000077" },
                        "baseFeePerGas": "0x64", "uncles": [], "withdrawals": [], "transactions": ["0x01", "0x02"]});
                    if b % 10 == 2 {
                        h["withdrawals"] = json!([
                            {"index": "0xa", "validatorIndex": "0x1", "address": A, "amount": "0x5"},
                            {"index": "0xb", "validatorIndex": "0x2", "address": "0x0000000000000000000000000000000000000099", "amount": "0x6"},
                            {"index": "0xc", "validatorIndex": "0x3", "address": B, "amount": "0x7"},
                            {"index": "0xd", "validatorIndex": "0x4", "address": A, "amount": "0x3b9aca00"},
                        ]);
                    }
                    if Some(b) == missing_withdrawals_at {
                        h.as_object_mut().unwrap().remove("withdrawals");
                    }
                    h
                }
                "eth_getBlockReceipts" => json!([
                    {"transactionHash": "0x01", "gasUsed": "0x5208", "effectiveGasPrice": "0x66"},
                    {"transactionHash": "0x02", "gasUsed": "0x2", "effectiveGasPrice": "0x64"},
                ]),
                other => bail!("unexpected {other}"),
            })
        })
    }

    #[tokio::test]
    async fn a_block_scan_finds_withdrawals_and_mined_blocks_for_every_address() {
        let (_, trace) = chain(false);
        let d = Discoverer::new(bodies(None), trace);
        let pending = Pending::default();
        let found = d
            .scan_blocks(&[A.into(), B.into()], 17_100_000, 17_100_009, &pending)
            .await
            .unwrap();
        let a = &found[A];
        assert_eq!(a.withdrawals.len(), 2);
        assert_eq!(
            a.withdrawals[1].record,
            json!({"withdrawalIndex": "13", "validatorIndex": "4", "address": A,
                   "amount": "1000000000", "blockNumber": "17100002", "timestamp": "17101002"})
            .as_object()
            .unwrap()
            .clone()
        );
        assert_eq!(
            a.withdrawals[1].position, 3,
            "the withdrawal's place in its block"
        );
        assert!(a.mined.is_empty());
        let b = &found[B];
        assert_eq!(b.withdrawals.len(), 1);
        assert_eq!(b.mined.len(), 1);
        assert_eq!(b.mined[0].record["blockNumber"], "17100005");
        // Post-Merge: priority fees only, 21000 gas at 2 over a base fee of 100, plus 2 gas at 0.
        assert_eq!(b.mined[0].record["blockReward"], "42000");
        assert_eq!(
            pending.timestamps.lock().unwrap().len(),
            10,
            "every body's timestamp kept"
        );
        assert_eq!(
            d.main
                .asked
                .lock()
                .unwrap()
                .iter()
                .filter(|(m, _)| m == "eth_getBlockByNumber")
                .count(),
            10,
            "one body per block, for both addresses"
        );
    }

    #[tokio::test]
    async fn a_body_past_shanghai_without_withdrawals_fails_the_scan() {
        let (_, trace) = chain(false);
        let d = Discoverer::new(bodies(Some(17_100_004)), trace);
        let err = d
            .scan_blocks(&[A.into()], 17_100_000, 17_100_009, &Pending::default())
            .await
            .unwrap_err();
        assert!(
            format!("{err:#}").contains("carries no withdrawals"),
            "{err:#}"
        );
        let (_, trace) = chain(false);
        let d = Discoverer::new(bodies(Some(17_000_004)), trace);
        d.scan_blocks(&[A.into()], 17_000_000, 17_000_009, &Pending::default())
            .await
            .expect("before Shanghai a body has no withdrawals to carry");
    }

    /// Etherscan's blockReward, as measured: pre-Merge it adds the static reward for the fork and a
    /// thirty-second of it per uncle (mainnet blocks 15,342,722 and 14,000,136 matched to the wei).
    #[tokio::test]
    async fn block_rewards_follow_the_forks() {
        let (_, trace) = chain(false);
        let d = Discoverer::new(bodies(None), trace);
        let reward = |b: u64, uncles: usize, base: Option<&str>| {
            let mut h =
                json!({"uncles": vec![json!("0x0"); uncles], "transactions": ["0x01", "0x02"]});
            if let Some(f) = base {
                h["baseFeePerGas"] = json!(f);
            }
            let d = &d;
            async move { d.block_reward(b, &h).await.unwrap() }
        };
        // Receipts pay 21000 x 102 + 2 x 100.
        assert_eq!(reward(20_000_000, 0, Some("0x64")).await, "42000");
        assert_eq!(
            reward(15_342_722, 1, Some("0x64")).await,
            (2_000_000_000_000_000_000u128 + 42_000 + 62_500_000_000_000_000).to_string()
        );
        assert_eq!(
            reward(5_000_000, 0, None).await,
            (3_000_000_000_000_000_000u128 + 21_000 * 102 + 200).to_string(),
            "Byzantium: 3 ETH, and every fee before London"
        );
        assert_eq!(
            reward(1_000_000, 2, None).await,
            (5_000_000_000_000_000_000u128 + 21_000 * 102 + 200 + 2 * 156_250_000_000_000_000)
                .to_string()
        );
    }

    /// An answer that does not account for the whole block fails the scan instead of recording a
    /// reward or an empty window: a body for another height, receipts that do not match the body's
    /// transactions, and a fee field that is missing.
    #[tokio::test]
    async fn an_incomplete_block_answer_fails_the_scan() {
        type Spoil = fn(&str, &mut Value);
        let cases: [(&str, Spoil); 6] = [
            ("answered with block", |m, v| {
                if m == "eth_getBlockByNumber" {
                    v["number"] = json!("0x1");
                }
            }),
            ("2 transactions and 1 receipts", |m, v| {
                if m == "eth_getBlockReceipts" {
                    v.as_array_mut().unwrap().pop();
                }
            }),
            ("no baseFeePerGas", |m, v| {
                if m == "eth_getBlockByNumber" {
                    v.as_object_mut().unwrap().remove("baseFeePerGas");
                }
            }),
            ("no gasUsed", |m, v| {
                if m == "eth_getBlockReceipts" {
                    v[0].as_object_mut().unwrap().remove("gasUsed");
                }
            }),
            ("do not follow its transactions", |m, v| {
                if m == "eth_getBlockReceipts" {
                    v[0]["transactionHash"] = json!("0x03");
                }
            }),
            ("under the base fee", |m, v| {
                if m == "eth_getBlockReceipts" {
                    v[1]["effectiveGasPrice"] = json!("0x63");
                }
            }),
        ];
        for (want, spoil) in cases {
            let good = bodies(None);
            let main = script(move |m, p| {
                let mut v = (good.answer)(m, p)?;
                spoil(m, &mut v);
                Ok(v)
            });
            let (_, trace) = chain(false);
            let d = Discoverer::new(main, trace);
            let err = d
                .scan_blocks(&[B.into()], 17_100_005, 17_100_005, &Pending::default())
                .await
                .unwrap_err();
            assert!(format!("{err:#}").contains(want), "{want}: {err:#}");
        }
    }

    /// The cursor's block pass reads each body once for every address that lacks it, and an address
    /// watched after a window was read gets only its own gap.
    #[tokio::test]
    async fn the_block_pass_shares_bodies_and_backfills_a_late_address() {
        use crate::address_mode::Discovery;
        let dir = tempfile::tempdir().unwrap();
        let h = history(dir.path());
        let (_, trace) = chain(false);
        let main = bodies(None);
        let both = script(move |m, p| match m {
            "eth_getLogs" => Ok(json!([])),
            "eth_getCode" => Ok(json!("0x")),
            "eth_getTransactionCount" => Ok(json!("0x0")),
            "eth_getBlockTransactionCountByNumber" => Ok(json!("0x1")),
            _ => (main.answer)(m, p),
        });
        let trace = script(move |m, p| match m {
            "trace_block" => Ok(json!([{"x": 1}])),
            _ => (trace.answer)(m, p),
        });
        let d = Discoverer::new(Counted::new(both), Counted::new(trace));
        let state = crate::address_mode::ModeState::new(
            h.clone(),
            1,
            std::time::Duration::from_secs(300),
            true,
        )
        .with_range(17_100_000, Some(17_100_009), 0);
        d.catch_up(&state, 17_200_000).await;
        assert_eq!(
            h.coverage(Action::BeaconWithdrawals, A).unwrap(),
            vec![(17_100_000, 17_100_009)]
        );
        let bodies_read = || {
            d.main
                .calls()
                .get("eth_getBlockByNumber")
                .copied()
                .unwrap_or(0)
        };
        let after_first = bodies_read();

        h.watch(B).unwrap();
        d.catch_up(&state, 17_200_000).await;
        assert_eq!(
            h.coverage(Action::MinedBlocks, B).unwrap(),
            vec![(17_100_000, 17_100_009)]
        );
        assert_eq!(
            bodies_read() - after_first,
            10,
            "B's gap read once, A's coverage not reread"
        );
        let req = crate::address_history::PageRequest {
            address: B.into(),
            start_block: Some(17_100_000),
            end_block: Some(17_100_009),
            sort: crate::address_history::Sort::Asc,
            page: 1,
            offset: 10,
            generation: None,
        };
        let crate::address_history::Answer::Rows(rows) = h.page(Action::MinedBlocks, &req).unwrap()
        else {
            panic!("covered")
        };
        assert_eq!(rows.len(), 1);
    }
}
