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
//! nonce's movement across the window, except a rise of one at the block where an EIP-7702
//! delegation appeared or was cleared.

use anyhow::{anyhow, bail, Context, Result};
use futures::StreamExt;
use serde_json::{json, Map, Value};
use std::collections::{BTreeMap, BTreeSet, VecDeque};
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
/// Blocks a sweep fetches at once: Arbitrum makes four a second and one takes 0.13-0.38 s to read.
const SWEEP_CONCURRENCY: usize = 8;

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

    /// The same call asked of every endpoint behind this one, for a question whose answer must hold
    /// at all of them; one endpoint answers once.
    fn call_each(
        &self,
        method: &str,
        params: Value,
    ) -> impl Future<Output = Result<Vec<Value>>> + Send {
        async move { Ok(vec![self.call(method, params).await?]) }
    }

    /// [`Rpc::call_each`], handing each endpoint's outcome to `seen` as it arrives rather than
    /// failing on the first error.
    fn visit_each(
        &self,
        method: &str,
        params: Value,
        seen: &mut (dyn FnMut(Result<Value>) + Send),
    ) -> impl Future<Output = ()> + Send {
        async move { seen(self.call(method, params).await) }
    }
}

/// The most of one `eth_getLogs` body address history reads before refusing it: about 7,000
/// transfers, so a sparse stretch that widened the span cannot hand a dense one back whole. A range is
/// narrowed; one block, which a block's gas limit keeps far under it, is refused.
pub const LOGS_BODY_CAP: usize = 4 << 20;

impl Rpc for crate::rpc::RpcClient {
    async fn call(&self, method: &str, params: Value) -> Result<Value> {
        if method == "eth_getLogs" {
            return self.call_capped(method, params, LOGS_BODY_CAP).await;
        }
        crate::rpc::RpcClient::call(self, method, params).await
    }

    async fn call_each(&self, method: &str, params: Value) -> Result<Vec<Value>> {
        crate::rpc::RpcClient::call_each(self, method, params).await
    }

    async fn visit_each(
        &self,
        method: &str,
        params: Value,
        seen: &mut (dyn FnMut(Result<Value>) + Send),
    ) {
        crate::rpc::RpcClient::visit_each(self, method, params, seen).await
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

    async fn call_each(&self, method: &str, params: Value) -> Result<Vec<Value>> {
        let answers = self.inner.call_each(method, params).await;
        if let Ok(a) = &answers {
            *self
                .calls
                .lock()
                .expect("calls lock")
                .entry(method.to_string())
                .or_default() += a.len() as u64;
        }
        answers
    }

    async fn visit_each(
        &self,
        method: &str,
        params: Value,
        seen: &mut (dyn FnMut(Result<Value>) + Send),
    ) {
        let calls = &self.calls;
        let mut counted = |a: Result<Value>| {
            *calls
                .lock()
                .expect("calls lock")
                .entry(method.to_string())
                .or_default() += 1;
            seen(a);
        };
        self.inner.visit_each(method, params, &mut counted).await
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
    /// The largest limit the provider has stated. Alchemy states 5,000 and then, for a while, 100
    /// on Optimism, so the ceiling is the largest stated, not the latest.
    stated: AtomicU64,
    start: u64,
    streak: AtomicU64,
    /// Whether the current size has been served once. Spans go out concurrently only then, so an
    /// unlearned size costs one refusal rather than a batch of them.
    proven: AtomicBool,
    /// Calls in flight on this window's provider, across every range sharing it.
    permits: tokio::sync::Semaphore,
    /// Held by the one range probing a size not yet served, so concurrent ranges do not each pay
    /// for the same refusal.
    probe: tokio::sync::Mutex<()>,
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
            stated: AtomicU64::new(0),
            start: start.max(1),
            streak: AtomicU64::new(0),
            proven: AtomicBool::new(false),
            permits: tokio::sync::Semaphore::new(RANGE_CONCURRENCY),
            probe: tokio::sync::Mutex::new(()),
        }
    }

    pub fn get(&self) -> u64 {
        self.size.load(Ordering::Relaxed)
    }

    /// A span of `refused` blocks was refused for its size: the size a retry of it may use, which
    /// the window takes too unless it is already smaller. A stated limit below the refused size is
    /// taken as given; anything else halves it. Measured from the refused span, not the window, so
    /// siblings refused at one size shrink it once, and a retry gets smaller whatever other ranges
    /// on the window have grown it to meanwhile.
    fn refused(&self, refused: u64, error: &str) -> u64 {
        self.streak.store(0, Ordering::Relaxed);
        let next = match stated_limit(error).filter(|n| *n >= 1) {
            Some(n) => {
                let most = self.stated.fetch_max(n, Ordering::Relaxed).max(n);
                self.ceiling.store(most.min(self.start), Ordering::Relaxed);
                if n < refused {
                    n
                } else {
                    refused / 2
                }
            }
            None => refused / 2,
        }
        .max(1);
        if self.size.fetch_min(next, Ordering::Relaxed) > next {
            self.proven.store(false, Ordering::Relaxed);
        }
        next
    }

    /// A span of `served` blocks was answered. Only an answer at the window's size or above proves
    /// it or counts towards growing it: a smaller one still in flight from before a regrowth says
    /// nothing about the new size.
    fn succeeded(&self, served: u64) {
        if served < self.get() {
            return;
        }
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
pub(crate) fn range_shaped(error: &str) -> bool {
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
pub(crate) fn stated_limit(error: &str) -> Option<u64> {
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
/// block order. Once the window's size has been served, up to `RANGE_CONCURRENCY` spans go out at
/// once; until then every range on the window sends one span at a time. Answers are judged in
/// order as they arrive: a fatal one fails the call at once, and a span refused for its size is
/// asked again at the smaller size while the clean answers around it are kept.
async fn ranged<R: Rpc>(
    rpc: &R,
    window: &Window,
    from: u64,
    to: u64,
    method: &str,
    params: impl Fn(u64, u64) -> Value,
) -> Result<Vec<Value>> {
    ranged_in(rpc, window, from, to, method, params, RANGE_CONCURRENCY).await
}

/// `ranged` with at most `most` spans in flight; one is the sequential scan the tests compare with.
async fn ranged_in<R: Rpc>(
    rpc: &R,
    window: &Window,
    from: u64,
    to: u64,
    method: &str,
    params: impl Fn(u64, u64) -> Value,
    most: usize,
) -> Result<Vec<Value>> {
    let mut parts: BTreeMap<u64, Vec<Value>> = BTreeMap::new();
    // Each range left to ask, with the largest span it may use: a refused span's retry is capped below
    // the size it was refused at.
    let mut todo: VecDeque<(u64, u64, u64)> = VecDeque::from([(from, to, u64::MAX)]);
    while !todo.is_empty() {
        let probing = match window.proven.load(Ordering::Relaxed) {
            true => None,
            false => Some(window.probe.lock().await),
        };
        let lanes = if window.proven.load(Ordering::Relaxed) {
            most
        } else {
            1
        };
        let probing = probing.filter(|_| lanes == 1);
        let span = window.get();
        let mut spans = Vec::with_capacity(lanes);
        while spans.len() < lanes {
            let Some((s, e, cap)) = todo.pop_front() else {
                break;
            };
            let end = s.saturating_add(span.min(cap) - 1).min(e);
            spans.push((s, end));
            if end < e {
                todo.push_front((end + 1, e, cap));
            }
        }
        // Judged as they complete, so a fatal answer is never held behind a slower span; the rows
        // are keyed by where they start, so the order they arrive in does not matter.
        let mut answers = futures::stream::iter(spans)
            .map(|(s, e)| {
                let p = params(s, e);
                async move {
                    let answer = match window.permits.acquire().await {
                        Ok(_permit) => rpc.call(method, p).await,
                        Err(closed) => Err(closed.into()),
                    };
                    (s, e, answer)
                }
            })
            .buffer_unordered(lanes);
        let mut again = Vec::new();
        while let Some((s, e, answer)) = answers.next().await {
            match judge(method, s, e, answer)? {
                Judged::Rows(items) => {
                    window.succeeded(e - s + 1);
                    parts.insert(s, items);
                }
                Judged::Smaller(why) => {
                    let next = window.refused(e - s + 1, &why);
                    // A provider naming its limit is routine; a timeout or a failure is not, and a
                    // dead RPC halving its way down to single blocks must be visible.
                    if stated_limit(&why).is_some() {
                        tracing::debug!(
                            "{method} [{s}, {e}] refused, retrying in spans of {next}: {why}"
                        );
                    } else {
                        tracing::warn!(
                            "{method} [{s}, {e}] failed, retrying in spans of {next}: {why}"
                        );
                    }
                    again.push((s, e, next));
                }
            }
        }
        drop(probing);
        again.sort_unstable();
        for range in again.into_iter().rev() {
            todo.push_front(range);
        }
    }
    Ok(parts.into_values().flatten().collect())
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

/// A 32-byte hash written as `0x` and 64 hex digits.
pub(crate) fn word32(s: &str) -> Result<[u8; 32]> {
    let raw = alloy_primitives::hex::decode(s.trim_start_matches("0x"))
        .with_context(|| format!("not hex: {s}"))?;
    raw.try_into()
        .map_err(|_| anyhow!("not a 32-byte hash: {s}"))
}

/// EIP-7702 delegation designator: `0xef0100` and a 20-byte address, nothing else.
fn delegation_designator(code: &str) -> bool {
    let hex = code.trim_start_matches("0x");
    hex.len() == 46
        && hex
            .get(..6)
            .is_some_and(|p| p.eq_ignore_ascii_case("ef0100"))
        && hex.bytes().all(|b| b.is_ascii_hexdigit())
}

/// One applied authorisation changes the designator. Two in one block change it at most once, and
/// an authorisation that leaves the same delegate in place does not change it at all.
fn explains_one_auth(before: &str, after: &str) -> bool {
    !before.eq_ignore_ascii_case(after)
        && (delegation_designator(before) || delegation_designator(after))
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
type CachedStamp = (u64, u64, Option<[u8; 32]>);

#[derive(Default)]
pub struct Pending {
    txs: Mutex<Vec<CachedTx>>,
    timestamps: Mutex<Vec<CachedStamp>>,
    /// A transaction's whole internal list, by hash, for `txhash` lookups.
    internals: Mutex<Vec<crate::address_history::TxInternals>>,
    /// Block hashes above the finalized head that what was fetched was read against, tracked once
    /// the fetch is recorded.
    anchors: Mutex<Vec<(u64, [u8; 32])>>,
    /// Block hashes another provider named (a trace's block), at any height, which the main
    /// endpoint must agree with before anything is kept.
    named: Mutex<Vec<(u64, [u8; 32])>>,
    /// What each body scanned holds for any address, so an address watched later reads it here.
    summaries: Mutex<Vec<(u64, Map<String, Value>)>>,
    /// The generation and finalized head the fetch began under.
    generation: u64,
    finalized: Option<u64>,
}

impl Pending {
    /// A fetch beginning now, against `history` as it stands.
    pub fn begin(history: &AddressHistory) -> Result<Pending> {
        Ok(Pending {
            generation: history.generation()?,
            finalized: history.head()?,
            ..Pending::default()
        })
    }

    pub fn generation(&self) -> u64 {
        self.generation
    }

    pub fn anchor(&self, block: u64, hash: [u8; 32]) {
        self.anchors
            .lock()
            .expect("pending lock")
            .push((block, hash));
    }

    pub fn anchors(&self) -> Vec<(u64, [u8; 32])> {
        self.anchors.lock().expect("pending lock").clone()
    }

    pub fn named(&self) -> Vec<(u64, [u8; 32])> {
        self.named.lock().expect("pending lock").clone()
    }

    /// Commit what was fetched; nothing if `trusted` is false (a check after the fetch failed).
    /// Blocking: call it off the async workers.
    pub fn persist(self, history: &AddressHistory, trusted: bool) -> Result<()> {
        history.persist_pending(
            &self.txs.into_inner().expect("pending lock"),
            &self.timestamps.into_inner().expect("pending lock"),
            &self.internals.into_inner().expect("pending lock"),
            &self.anchors.into_inner().expect("pending lock"),
            &self.summaries.into_inner().expect("pending lock"),
            self.generation,
            self.finalized,
            trusted,
        )
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
    /// The trace's block hash, when it named one. Absent, the cache matches on the block number.
    block_hash: Option<[u8; 32]>,
}

/// What a block body holds for any address: its fee recipient, timestamp, hash and every
/// withdrawal as (index, validator, recipient, amount), decimal.
fn summary(header: &Value, ts: u64) -> Result<Map<String, Value>> {
    let mut m = Map::new();
    m.insert("ts".into(), Value::String(ts.to_string()));
    m.insert(
        "hash".into(),
        Value::String(str_field(header, "hash")?.to_ascii_lowercase()),
    );
    m.insert(
        "miner".into(),
        Value::String(str_field(header, "miner")?.to_ascii_lowercase()),
    );
    let mut w = Vec::new();
    for x in header
        .get("withdrawals")
        .and_then(Value::as_array)
        .into_iter()
        .flatten()
    {
        w.push(json!([
            decimal(str_field(x, "index")?)?,
            decimal(str_field(x, "validatorIndex")?)?,
            str_field(x, "address")?.to_ascii_lowercase(),
            decimal(str_field(x, "amount")?)?,
        ]));
    }
    m.insert("w".into(), Value::Array(w));
    Ok(m)
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
    record_blocks_anchored(
        history,
        address,
        (from, to),
        found,
        generation,
        verified,
        None,
    )
}

/// [`record_blocks`] for bodies read against block `anchor.0` with hash `anchor.1`.
pub fn record_blocks_anchored(
    history: &AddressHistory,
    address: &str,
    (from, to): (u64, u64),
    found: BlockFound,
    generation: u64,
    verified: bool,
    anchor: Option<(u64, [u8; 32])>,
) -> Result<()> {
    let within = |rows: Vec<Row>| -> Vec<Row> {
        rows.into_iter()
            .filter(|r| from <= r.block && r.block <= to)
            .collect()
    };
    for (action, rows) in [
        (Action::BeaconWithdrawals, within(found.withdrawals)),
        (Action::MinedBlocks, within(found.mined)),
    ] {
        if verified {
            history.record_verified(action, address, &rows, (from, to), generation)?;
        } else {
            history.record_anchored(action, address, &rows, (from, to), generation, anchor)?;
        }
    }
    Ok(())
}

/// What one window found for one address, ready to record.
#[derive(Debug, Default)]
pub struct Found {
    pub txlist: Vec<Row>,
    pub txlistinternal: Vec<Row>,
    pub tokentx: Vec<Row>,
    pub tokennfttx: Vec<Row>,
    pub token1155tx: Vec<Row>,
    /// Internal rows from a `callcode` frame, by (block, tx index, position): Etherscan lists them,
    /// but the value never leaves the caller, so they are not deposits.
    pub callcode: BTreeSet<(u64, u64, u64)>,
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
        // A window above the finalized head is anchored at its last block, which covers its traces.
        let limit = history.head()?.unwrap_or(0).max(to);
        for hash in &internal {
            let rows = self.internal_rows_of(history, hash, pending, limit).await?;
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

    /// RFC-0064's sweep: read `[from, to]` once for every address in `set` (lowercase `0x…`), each
    /// block's transactions and the range's transfer logs, and with `traced` each block's trace,
    /// matched in memory, so the cost is per block rather than per address.
    pub async fn sweep(
        &self,
        history: &AddressHistory,
        set: &std::collections::HashSet<String>,
        from: u64,
        to: u64,
        pending: &Pending,
        traced: bool,
    ) -> Result<BTreeMap<String, Found>> {
        if history.chain_id() == 1 && from < BYZANTIUM {
            bail!("a sweep reads success from receipts, which carry no status before block {BYZANTIUM}");
        }
        let blocks: Vec<Result<Value>> = futures::stream::iter(from..=to)
            .map(|b| async move {
                let block = self
                    .main
                    .call("eth_getBlockByNumber", json!([format!("0x{b:x}"), true]))
                    .await
                    .with_context(|| format!("eth_getBlockByNumber {b}"))?;
                if block.is_null() {
                    bail!("block {b} is missing at the main endpoint");
                }
                Ok(block)
            })
            .buffered(SWEEP_CONCURRENCY)
            .collect()
            .await;
        let mut refs: BTreeMap<String, Vec<TxRef>> = BTreeMap::new();
        let mut timestamps = BTreeMap::new();
        let mut hashes = BTreeMap::new();
        for block in blocks {
            let block = block?;
            let number = hex_u64(block.get("number").unwrap_or(&Value::Null))?;
            let hash = word32(str_field(&block, "hash")?)?;
            hashes.insert(number, hash);
            timestamps.insert(
                number,
                hex_u64(block.get("timestamp").unwrap_or(&Value::Null))?,
            );
            let txs = block
                .get("transactions")
                .and_then(Value::as_array)
                .ok_or_else(|| anyhow!("block {number} came without its transactions"))?;
            for tx in txs {
                let sender = str_field(tx, "from")?.to_ascii_lowercase();
                let receiver = tx
                    .get("to")
                    .and_then(Value::as_str)
                    .unwrap_or("")
                    .to_ascii_lowercase();
                for (party, outgoing) in [(&sender, true), (&receiver, false)] {
                    if !set.contains(party) || (!outgoing && receiver == sender) {
                        continue;
                    }
                    refs.entry(party.clone()).or_default().push(TxRef {
                        hash: str_field(tx, "hash")?.to_ascii_lowercase(),
                        block: number,
                        outgoing,
                        failed: false,
                        block_hash: Some(hash),
                    });
                }
            }
        }
        let logs = ranged(
            &self.main,
            &self.log_window,
            from,
            to,
            "eth_getLogs",
            |s, e| {
                json!([{ "fromBlock": format!("0x{s:x}"), "toBlock": format!("0x{e:x}"),
                     "topics": [[TRANSFER, TRANSFER_SINGLE, TRANSFER_BATCH]] }])
            },
        )
        .await?;
        let mut found: BTreeMap<String, Found> = BTreeMap::new();
        for log in &logs {
            if log.get("removed").and_then(Value::as_bool) == Some(true) {
                bail!("a removed log in a swept range [{from}, {to}]: {log}");
            }
            let topics: Vec<&str> = log
                .get("topics")
                .and_then(Value::as_array)
                .map(|t| t.iter().filter_map(Value::as_str).collect())
                .unwrap_or_default();
            let parties = match topics.first().copied() {
                Some(TRANSFER) if topics.len() >= 3 => [topics[1], topics[2]],
                Some(TRANSFER_SINGLE | TRANSFER_BATCH) if topics.len() == 4 => {
                    [topics[2], topics[3]]
                }
                _ => continue,
            };
            let mut matched: Vec<String> = Vec::new();
            for word in parties {
                let Ok(p) = topic_address(word).map(|p| p.to_ascii_lowercase()) else {
                    continue;
                };
                if set.contains(&p) && !matched.contains(&p) {
                    matched.push(p);
                }
            }
            for p in matched {
                transfer_rows(log, &p, &timestamps, found.entry(p.clone()).or_default())?;
            }
        }
        if traced {
            self.sweep_traces(set, &hashes, &timestamps, &mut found)
                .await?;
        }
        for (a, r) in refs {
            found.entry(a).or_default().txlist = self.hydrate(history, &r, pending).await?;
        }
        Ok(found)
    }

    /// The sweep's internal transfers: one `trace_block` per block, each transaction's frames turned
    /// into Etherscan's internal rows and kept for every swept address a row names. A frame naming
    /// another block hash than the main endpoint's refuses the window rather than mix two forks.
    async fn sweep_traces(
        &self,
        set: &std::collections::HashSet<String>,
        hashes: &BTreeMap<u64, [u8; 32]>,
        timestamps: &BTreeMap<u64, u64>,
        found: &mut BTreeMap<String, Found>,
    ) -> Result<()> {
        let traces: Vec<Result<(u64, Value)>> = futures::stream::iter(hashes.keys().copied())
            .map(|b| async move {
                let t = self
                    .trace
                    .call("trace_block", json!([format!("0x{b:x}")]))
                    .await
                    .with_context(|| format!("trace_block {b}"))?;
                Ok((b, t))
            })
            .buffered(SWEEP_CONCURRENCY)
            .collect()
            .await;
        for t in traces {
            let (b, t) = t?;
            let frames = t
                .as_array()
                .ok_or_else(|| anyhow!("trace_block {b} answered a non-list"))?;
            let mut by_tx: Vec<(String, Vec<Value>)> = Vec::new();
            for f in frames {
                // A block reward frame belongs to no transaction.
                let Some(hash) = f.get("transactionHash").and_then(Value::as_str) else {
                    continue;
                };
                if word32(str_field(f, "blockHash")?)? != hashes[&b] {
                    bail!("trace_block {b} answered for another block than the main endpoint's");
                }
                let hash = hash.to_ascii_lowercase();
                match by_tx.last_mut() {
                    Some((h, fs)) if *h == hash => fs.push(f.clone()),
                    _ => by_tx.push((hash, vec![f.clone()])),
                }
            }
            for (hash, frames) in by_tx {
                for mut record in internal_records(&frames, timestamps[&b])? {
                    let parties: BTreeSet<String> = ["from", "to", "contractAddress"]
                        .iter()
                        .filter_map(|k| record.get(*k).and_then(Value::as_str))
                        .map(str::to_ascii_lowercase)
                        .filter(|p| set.contains(p))
                        .collect();
                    if parties.is_empty() {
                        continue;
                    }
                    let num = |k: &str| -> Result<u64> {
                        record
                            .get(k)
                            .and_then(Value::as_str)
                            .ok_or_else(|| anyhow!("internal record lacks {k}"))?
                            .parse()
                            .with_context(|| format!("internal record {k}"))
                    };
                    let (block, tx_index, position) = (
                        num("blockNumber")?,
                        num("transactionIndex")?,
                        num(TRACE_INDEX)?,
                    );
                    let callcode = frames
                        .get(position as usize)
                        .and_then(|f| f.pointer("/action/callType"))
                        .and_then(Value::as_str)
                        == Some("callcode");
                    record.insert("hash".into(), Value::String(hash.clone()));
                    let row = address_mode_order(Row {
                        block,
                        tx_index,
                        position,
                        record,
                    });
                    for p in parties {
                        let f = found.entry(p).or_default();
                        f.txlistinternal.push(row.clone());
                        if callcode {
                            f.callcode.insert((block, tx_index, position));
                        }
                    }
                }
            }
        }
        Ok(())
    }

    /// Every internal transaction of `hash`, as Etherscan's `txhash` form lists them: from the cache,
    /// else from one `trace_transaction`.
    pub async fn internal_rows_of(
        &self,
        history: &AddressHistory,
        hash: &str,
        pending: &Pending,
        limit: u64,
    ) -> Result<Vec<Row>> {
        let key = alloy_primitives::hex::decode(hash.trim_start_matches("0x"))?;
        // A kept trace is used only with the block hash its provider named, which the main endpoint
        // is asked to agree with again; one kept without it is traced afresh.
        let kept = match history.tx_internals_block(&key)? {
            Some((block, Some(named))) => {
                pending
                    .named
                    .lock()
                    .expect("pending lock")
                    .push((block, named));
                history.tx_internals(&key)?
            }
            _ => None,
        };
        let records = match kept {
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
                // A cached trace is revisited only if a reorg takes its block, so a block above the
                // finalized head is cached only with its hash tracked.
                if block > limit {
                    return Err(anyhow!(NotFinalized(block)));
                }
                // The trace endpoint names the block it traced; the main endpoint must agree, at any
                // height, and above the finalized head the hash is tracked too.
                let named = word32(str_field(root, "blockHash")?)?;
                pending
                    .named
                    .lock()
                    .expect("pending lock")
                    .push((block, named));
                if history.head()?.is_none_or(|h| block > h) {
                    pending.anchor(block, named);
                }
                let ts = self
                    .block_timestamp(history, block, Some(named), pending)
                    .await?;
                let records = internal_records(frames, ts)?;
                pending.internals.lock().expect("pending lock").push((
                    key,
                    block,
                    named,
                    records.clone(),
                ));
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
        Ok(self
            .scan_blocks_in(addresses, from, to, pending, false)
            .await?
            .0)
    }

    /// [`scan_blocks`], and the blocks themselves when `keep` is set, so a proof can use the bodies
    /// already read. A body that will not parse as a header drops the kept list; the scan stands.
    async fn scan_blocks_in(
        &self,
        addresses: &[String],
        from: u64,
        to: u64,
        pending: &Pending,
        mut keep: bool,
    ) -> Result<(
        BTreeMap<String, BlockFound>,
        Vec<crate::address_partitions::VerifiedBlock>,
    )> {
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
        let mut summaries = Vec::new();
        let mut kept = Vec::new();
        // The bodies must be one chain, each naming the one before as its parent, and the last one's
        // hash is named for the caller to hold against the main endpoint: a pool's endpoints can be
        // on different forks, and a body from another fork would otherwise pass as this chain's.
        let mut previous: Option<String> = None;
        let mut last: Option<(u64, [u8; 32])> = None;
        for body in bodies {
            let (b, header) = body?;
            let hash = str_field(&header, "hash")?.to_ascii_lowercase();
            let parent = str_field(&header, "parentHash")?.to_ascii_lowercase();
            if let Some(p) = &previous {
                if *p != parent {
                    bail!("block {b} does not name block {} as its parent; the bodies are from two chains", b - 1);
                }
            }
            let hash_bytes = word32(&hash)?;
            last = Some((b, hash_bytes));
            previous = Some(hash);
            let ts = hex_u64(header.get("timestamp").unwrap_or(&Value::Null))
                .with_context(|| format!("block {b} timestamp"))?;
            timestamps.push((b, ts, Some(hash_bytes)));
            summaries.push((b, summary(&header, ts)?));
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
            if keep {
                match crate::address_proposers::block_from_rpc_value(header.clone(), b) {
                    Ok(block) => kept.push(block),
                    Err(_) => {
                        keep = false;
                        kept.clear();
                    }
                }
            }
        }
        pending
            .timestamps
            .lock()
            .expect("pending lock")
            .extend(timestamps);
        // Kept only from a scan that read every body whole, so a retry never takes a failed body's
        // summary for a complete one.
        pending
            .summaries
            .lock()
            .expect("pending lock")
            .extend(summaries);
        if let Some(l) = last {
            pending.named.lock().expect("pending lock").push(l);
        }
        Ok((found, kept))
    }

    /// [`Discoverer::scan_blocks`], from the summaries of bodies already scanned for other addresses
    /// where they cover the window: only a block one of `addresses` received the fees of is read
    /// again, for its reward. The window's last summary hash is named for the main endpoint's check.
    pub async fn scan_with(
        &self,
        history: &AddressHistory,
        addresses: &[String],
        from: u64,
        to: u64,
        pending: &Pending,
    ) -> Result<BTreeMap<String, BlockFound>> {
        Ok(self
            .scan_with_blocks(history, addresses, from, to, pending, false)
            .await?
            .0)
    }

    /// [`scan_with`], plus the parsed blocks when `keep` is set. A window replayed from summaries
    /// has no body in hand and keeps none.
    pub(crate) async fn scan_with_blocks(
        &self,
        history: &AddressHistory,
        addresses: &[String],
        from: u64,
        to: u64,
        pending: &Pending,
        keep: bool,
    ) -> Result<(
        BTreeMap<String, BlockFound>,
        Vec<crate::address_partitions::VerifiedBlock>,
    )> {
        let Some(sums) = history.summaries(from, to)? else {
            return self
                .scan_blocks_in(addresses, from, to, pending, keep)
                .await;
        };
        let watched: BTreeSet<String> = addresses.iter().map(|a| a.to_ascii_lowercase()).collect();
        let mut found: BTreeMap<String, BlockFound> = watched
            .iter()
            .map(|a| (a.clone(), BlockFound::default()))
            .collect();
        let text = |m: &Map<String, Value>, k: &str| {
            m.get(k).and_then(Value::as_str).unwrap_or("").to_string()
        };
        for (b, sum) in &sums {
            let ts = text(sum, "ts");
            for (i, w) in sum
                .get("w")
                .and_then(Value::as_array)
                .into_iter()
                .flatten()
                .enumerate()
            {
                let field = |n: usize| w.get(n).and_then(Value::as_str).unwrap_or("").to_string();
                let Some(f) = found.get_mut(&field(2)) else {
                    continue;
                };
                let mut r = Map::new();
                for (k, v) in [
                    ("withdrawalIndex", field(0)),
                    ("validatorIndex", field(1)),
                    ("address", field(2)),
                    ("amount", field(3)),
                    ("blockNumber", b.to_string()),
                    ("timestamp", ts.clone()),
                ] {
                    r.insert(k.into(), Value::String(v));
                }
                f.withdrawals.push(Row {
                    block: *b,
                    tx_index: 0,
                    position: i as u64,
                    record: r,
                });
            }
            if let Some(f) = found.get_mut(&text(sum, "miner")) {
                let (_, header) = self.body(*b).await?;
                if str_field(&header, "hash")?.to_ascii_lowercase() != text(sum, "hash") {
                    bail!("block {b} is no longer the block its summary was read from");
                }
                let mut r = Map::new();
                r.insert("blockNumber".into(), Value::String(b.to_string()));
                r.insert("timeStamp".into(), Value::String(ts.clone()));
                r.insert(
                    "blockReward".into(),
                    Value::String(self.block_reward(*b, &header).await?),
                );
                f.mined.push(Row {
                    block: *b,
                    tx_index: 0,
                    position: 0,
                    record: r,
                });
            }
        }
        if let Some((b, sum)) = sums.last() {
            pending
                .named
                .lock()
                .expect("pending lock")
                .push((*b, word32(&text(sum, "hash"))?));
        }
        Ok((found, Vec::new()))
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
            let named = match f.get("blockHash").and_then(Value::as_str) {
                Some(h) => Some(word32(h).with_context(|| {
                    format!("trace blockHash of {}", hash.to_ascii_lowercase())
                })?),
                None => None,
            };
            let hash = hash.to_ascii_lowercase();
            let e = roots.entry(hash.clone()).or_insert(TxRef {
                hash,
                block,
                outgoing: false,
                failed: f.get("error").is_some_and(|e| !e.is_null()),
                block_hash: named,
            });
            e.outgoing |= outgoing;
            if e.block_hash.is_none() {
                e.block_hash = named;
            } else if named.is_some_and(|h| e.block_hash != Some(h)) {
                bail!("{} traces name two block hashes", e.hash);
            }
        }
        let mut outgoing_at = BTreeMap::new();
        for r in roots.values() {
            if r.outgoing {
                *outgoing_at.entry(r.block).or_default() += 1;
            }
        }
        self.check_nonce(a, from, to, &outgoing_at).await?;
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
    /// must equal the nonce's movement. A contract's nonce counts its creations instead, and an
    /// account that is still delegated at the window's end can move without sending, so neither is
    /// checked. A delegation set and then cleared inside the window leaves code empty at both ends;
    /// each rise of one is kept only when the code changed across that block.
    async fn check_nonce(
        &self,
        a: &str,
        from: u64,
        to: u64,
        outgoing_at: &BTreeMap<u64, u64>,
    ) -> Result<()> {
        // Code is read at the window's end, not today: an account delegated under EIP-7702 since
        // was a plain EOA through all of its earlier history.
        let code = self
            .main
            .call("eth_getCode", json!([a, format!("0x{to:x}")]))
            .await?;
        if code.as_str() != Some("0x") {
            return Ok(());
        }
        let before = if from == 0 {
            0
        } else {
            self.nonce_at(a, from - 1).await?
        };
        let after = self.nonce_at(a, to).await?;
        let moved = after.saturating_sub(before);
        let outgoing = outgoing_at.values().sum();
        if moved == outgoing {
            return Ok(());
        }
        // A set and a clear in the same block leave code empty on both sides and raise the nonce
        // by two. Post-state code cannot see that, so the window stays refused.
        if moved > outgoing
            && self
                .auths_explain(a, from, to, before, after, outgoing_at)
                .await?
        {
            return Ok(());
        }
        bail!(
            "{a} sent {moved} transactions in [{from}, {to}] by its nonce, and the traces show \
             {outgoing}; not recording the window as covered"
        )
    }

    async fn nonce_at(&self, a: &str, block: u64) -> Result<u64> {
        let n = self
            .main
            .call(
                "eth_getTransactionCount",
                json!([a, format!("0x{block:x}")]),
            )
            .await?;
        hex_u64(&n)
    }

    async fn code_at(&self, a: &str, block: u64) -> Result<String> {
        let code = self
            .main
            .call("eth_getCode", json!([a, format!("0x{block:x}")]))
            .await?;
        Ok(code.as_str().unwrap_or("").to_string())
    }

    /// Smallest block in `[lo, hi]` whose nonce is above `prev`. Nonce is the count after the block.
    async fn first_block_above_nonce(
        &self,
        a: &str,
        mut lo: u64,
        mut hi: u64,
        prev: u64,
    ) -> Result<u64> {
        while lo < hi {
            let mid = lo + (hi - lo) / 2;
            if self.nonce_at(a, mid).await? > prev {
                hi = mid;
            } else {
                lo = mid + 1;
            }
        }
        Ok(lo)
    }

    /// Each nonce rise above the outgoing transactions in that block is one applied authorisation,
    /// and only when the delegation designator changed across the block. Two rises in one block, or
    /// one that leaves the code unchanged, are not explained.
    async fn auths_explain(
        &self,
        a: &str,
        from: u64,
        to: u64,
        mut prev: u64,
        after: u64,
        outgoing_at: &BTreeMap<u64, u64>,
    ) -> Result<bool> {
        let mut cursor = from;
        while prev < after {
            if cursor > to {
                return Ok(false);
            }
            let b = self.first_block_above_nonce(a, cursor, to, prev).await?;
            let now = self.nonce_at(a, b).await?;
            if now <= prev {
                return Ok(false);
            }
            let sent = outgoing_at.get(&b).copied().unwrap_or(0);
            let Some(extra) = now
                .checked_sub(prev)
                .and_then(|rise| rise.checked_sub(sent))
            else {
                return Ok(false);
            };
            if extra == 1 {
                let before_code = if b == 0 {
                    "0x".to_string()
                } else {
                    self.code_at(a, b - 1).await?
                };
                if !explains_one_auth(&before_code, &self.code_at(a, b).await?) {
                    return Ok(false);
                }
            } else if extra != 0 {
                return Ok(false);
            }
            prev = now;
            cursor = b.saturating_add(1);
        }
        Ok(true)
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
    /// cache, else one header read per block. A log that names its block hash trusts a cached
    /// timestamp only when that hash was the one stored with it.
    async fn timestamps(
        &self,
        history: &AddressHistory,
        logs: &[Value],
        pending: &Pending,
    ) -> Result<BTreeMap<u64, u64>> {
        let mut ts = BTreeMap::new();
        let mut missing: BTreeMap<u64, Option<[u8; 32]>> = BTreeMap::new();
        for log in logs {
            let block = hex_u64(log.get("blockNumber").unwrap_or(&Value::Null))?;
            match log.get("blockTimestamp") {
                Some(t) => {
                    ts.insert(block, hex_u64(t)?);
                }
                None => {
                    // Not 32 bytes: it cannot name a fork, so the block number is all there is.
                    let named = log
                        .get("blockHash")
                        .and_then(Value::as_str)
                        .and_then(|h| word32(h).ok());
                    missing
                        .entry(block)
                        .and_modify(|h| {
                            if h.is_none() {
                                *h = named;
                            }
                        })
                        .or_insert(named);
                }
            }
        }
        for (b, named) in missing {
            if let std::collections::btree_map::Entry::Vacant(slot) = ts.entry(b) {
                slot.insert(self.block_timestamp(history, b, named, pending).await?);
            }
        }
        Ok(ts)
    }

    async fn block_timestamp(
        &self,
        history: &AddressHistory,
        block: u64,
        expected: Option<[u8; 32]>,
        pending: &Pending,
    ) -> Result<u64> {
        let cached = match expected {
            Some(h) => history.timestamp_on_fork(block, h)?,
            None => history.block_timestamp(block)?,
        };
        if let Some(t) = cached {
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
        let read = match header.get("hash").and_then(Value::as_str) {
            Some(h) => Some(word32(h)?),
            None => None,
        };
        pending
            .timestamps
            .lock()
            .expect("pending lock")
            .push((block, t, read));
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
        // A copy cached at another block is from before a re-inclusion; read it again. The same
        // block number on another fork is the same hole: returning the stale row fails the anchor
        // check while the cache stays, and the window retries forever.
        if let Some(cached) = history.cached_tx(&key)? {
            let same_number = cached.get("blockNumber").and_then(Value::as_str)
                == Some(r.block.to_string().as_str());
            if same_number {
                let cached_hash = cached
                    .get("blockHash")
                    .and_then(Value::as_str)
                    .and_then(|h| word32(h).ok());
                match (r.block_hash, cached_hash) {
                    (Some(want), Some(have)) if want == have => {
                        pending
                            .named
                            .lock()
                            .expect("pending lock")
                            .push((r.block, want));
                        return Ok(cached);
                    }
                    (None, _) => return Ok(cached),
                    _ => {}
                }
            }
        }
        let (tx, receipt, ts) = futures::try_join!(
            self.main.call("eth_getTransactionByHash", json!([hash])),
            self.main.call("eth_getTransactionReceipt", json!([hash])),
            self.block_timestamp(history, r.block, r.block_hash, pending)
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
    // A type-4 transaction's delegations. Etherscan's txlist leaves them out, so a client decoding
    // from it cannot tell a delegation or a revocation from any other self-call; quantities are
    // decimal, as the row's others are.
    if let Some(list) = tx.get("authorizationList").and_then(Value::as_array) {
        let entries = list
            .iter()
            .map(|a| {
                let mut e = Map::new();
                e.insert("chainId".into(), Value::String(dec(a, "chainId")?));
                e.insert(
                    "address".into(),
                    Value::String(str_field(a, "address")?.to_ascii_lowercase()),
                );
                e.insert("nonce".into(), Value::String(dec(a, "nonce")?));
                let parity = a.get("yParity").or_else(|| a.get("v"));
                if let Some(p) = parity.and_then(Value::as_str) {
                    e.insert("yParity".into(), Value::String(decimal(p)?));
                }
                for k in ["r", "s"] {
                    e.insert(
                        k.into(),
                        Value::String(str_field(a, k)?.to_ascii_lowercase()),
                    );
                }
                Ok(Value::Object(e))
            })
            .collect::<Result<Vec<_>>>()?;
        r.insert("authorizationList".into(), Value::Array(entries));
    }
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
///
/// Three topics stay `tokentx`: that is the ERC-20 log shape. Etherscan files by its token
/// tracker, which this nest does not have. A one-topic `Transfer` (CryptoKitties) is skipped.
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
    record_anchored(history, address, (from, to), found, generation, None)
}

/// [`record`] for a window read against block `anchor.0` with hash `anchor.1`.
pub fn record_anchored(
    history: &AddressHistory,
    address: &str,
    (from, to): (u64, u64),
    found: Found,
    generation: u64,
    anchor: Option<(u64, [u8; 32])>,
) -> Result<()> {
    for (action, rows) in [
        (Action::TxList, found.txlist),
        (Action::TxListInternal, found.txlistinternal),
        (Action::TokenTx, found.tokentx),
        (Action::TokenNftTx, found.tokennfttx),
        (Action::Token1155Tx, found.token1155tx),
    ] {
        history.record_anchored(action, address, &rows, (from, to), generation, anchor)?;
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
        assert_eq!(w.refused(100_000, "currently limited to 100 blocks"), 100);
        assert_eq!(w.refused(100, "timeout"), 50);
        assert_eq!(
            w.refused(50, "limited to 500 blocks"),
            25,
            "a stated limit above the refused size halves it instead"
        );
        assert_eq!(w.refused(100_000, "timeout"), 50_000);
        assert_eq!(
            w.get(),
            25,
            "a refusal of a size already left behind shrinks nothing"
        );

        // Successes grow the span back, never past the largest limit stated.
        for _ in 0..REGROW_AFTER {
            w.succeeded(w.get());
        }
        assert_eq!(w.get(), 50);
        for _ in 0..8 * REGROW_AFTER {
            w.succeeded(w.get());
        }
        assert_eq!(w.get(), 500, "capped at the largest stated limit");

        // A provider that only ever states one limit keeps it.
        let w = Window::new(100_000);
        assert_eq!(w.refused(100_000, "currently limited to 100 blocks"), 100);
        for _ in 0..8 * REGROW_AFTER {
            w.succeeded(w.get());
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
        calls: AtomicU64,
        in_flight: AtomicU64,
        most: AtomicU64,
        timed_out_once: AtomicBool,
        fatal_at: Option<u64>,
    }
    impl Capped {
        fn new(fatal_at: Option<u64>) -> Capped {
            Capped {
                calls: AtomicU64::new(0),
                in_flight: AtomicU64::new(0),
                most: AtomicU64::new(0),
                timed_out_once: AtomicBool::new(false),
                fatal_at,
            }
        }
    }
    impl Rpc for Capped {
        async fn call(&self, _: &str, p: Value) -> Result<Value> {
            self.calls.fetch_add(1, Ordering::SeqCst);
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

        // Nor by a slower span before it.
        let rpc = Spans(Mutex::default(), |s, e, _| match (s, e) {
            (2, 3) => None,
            (4, 5) => Some(Err(anyhow!("401 Unauthorized"))),
            _ => rows(s, e),
        });
        let got = tokio::time::timeout(
            std::time::Duration::from_secs(5),
            ranged(&rpc, &Window::new(2), 0, 5, "trace_filter", blocks),
        )
        .await
        .expect("the fatal answer ends the call without waiting for [2, 3]");
        assert!(format!("{:#}", got.unwrap_err()).contains("401"));
    }

    /// Refuses a span whose answer would hold more than 2,000 logs, as Alchemy does, without
    /// stating a limit. Blocks 40,000 to 41,999 hold five logs each; elsewhere every 97th holds one.
    struct Dense {
        calls: AtomicU64,
    }
    impl Rpc for Dense {
        async fn call(&self, _: &str, p: Value) -> Result<Value> {
            self.calls.fetch_add(1, Ordering::SeqCst);
            let s = hex_u64(&p[0]["fromBlock"])?;
            let e = hex_u64(&p[0]["toBlock"])?;
            tokio::time::sleep(std::time::Duration::from_millis(s % 3)).await;
            let logs = |b: u64| match b {
                40_000..42_000 => 5,
                _ if b.is_multiple_of(97) => 1,
                _ => 0,
            };
            if (s..=e).map(logs).sum::<u64>() > 2_000 {
                bail!("query returned more than 2000 results");
            }
            Ok(Value::Array(
                (s..=e)
                    .flat_map(|b| (0..logs(b)).map(move |i| json!([b, i])))
                    .collect(),
            ))
        }
    }

    /// Four filters sharing one window, all at once, make no more calls than the same four one
    /// after the other one span at a time, and answer the same.
    #[tokio::test]
    async fn concurrent_spans_make_no_more_calls_than_sequential_ones() {
        async fn four(most: usize, together: bool) -> (Vec<Vec<Value>>, u64) {
            let rpc = Dense {
                calls: AtomicU64::new(0),
            };
            let w = Window::new(100_000);
            let one = || ranged_in(&rpc, &w, 0, 199_999, "eth_getLogs", blocks, most);
            let got = if together {
                futures::future::try_join_all([one(), one(), one(), one()])
                    .await
                    .unwrap()
            } else {
                let mut got = Vec::new();
                for _ in 0..4 {
                    got.push(one().await.unwrap());
                }
                got
            };
            (got, rpc.calls.load(Ordering::SeqCst))
        }
        let (want, sequential) = four(1, false).await;
        let (got, concurrent) = four(RANGE_CONCURRENCY, true).await;
        assert_eq!(got, want);
        assert!(
            concurrent <= sequential,
            "{concurrent} calls concurrently against {sequential} one at a time"
        );

        let capped = |most| async move {
            let rpc = Capped::new(None);
            let got = ranged_in(
                &rpc,
                &Window::new(100_000),
                0,
                2_999,
                "trace_filter",
                blocks,
                most,
            )
            .await
            .unwrap();
            (got, rpc.calls.load(Ordering::SeqCst))
        };
        let (want, sequential) = capped(1).await;
        let (got, concurrent) = capped(RANGE_CONCURRENCY).await;
        assert_eq!(got, want);
        assert!(
            concurrent <= sequential,
            "{concurrent} against {sequential}"
        );

        let four = |limit, why, start, most, together| async move {
            let rpc = Stated(AtomicU64::new(0), limit, why);
            let w = Window::new(start);
            let one = || ranged_in(&rpc, &w, 0, 2_999, "eth_getLogs", blocks, most);
            let got = if together {
                futures::future::try_join_all([one(), one(), one(), one()])
                    .await
                    .unwrap()
            } else {
                let mut got = Vec::new();
                for _ in 0..4 {
                    got.push(one().await.unwrap());
                }
                got
            };
            (got, rpc.0.load(Ordering::SeqCst))
        };
        // A stated cap makes the sequential count exact: one refusal, then every span at the cap.
        // Four filters at once must not each pay for the refusal, nor shrink below the cap.
        let limited = "currently limited to 100 blocks";
        let (want, sequential) = four(100, limited, 100_000, 1, false).await;
        assert_eq!(sequential, 1 + 4 * 30);
        let (got, concurrent) = four(100, limited, 100_000, RANGE_CONCURRENCY, true).await;
        assert_eq!(got, want);
        assert_eq!(concurrent, sequential);

        // An unstated limit between the halved and the regrown size: the window keeps growing back
        // to a size that is refused, and a smaller answer still in flight must not prove it.
        let many = "query returned too many results";
        let (want, sequential) = four(60, many, 100, 1, false).await;
        let (got, concurrent) = four(60, many, 100, RANGE_CONCURRENCY, true).await;
        assert_eq!(got, want);
        assert!(
            concurrent <= sequential,
            "{concurrent} against {sequential}"
        );
    }

    /// Refuses spans over a limit, in the words given.
    struct Stated(AtomicU64, u64, &'static str);
    impl Rpc for Stated {
        async fn call(&self, _: &str, p: Value) -> Result<Value> {
            self.0.fetch_add(1, Ordering::SeqCst);
            let s = hex_u64(&p[0]["fromBlock"])?;
            let e = hex_u64(&p[0]["toBlock"])?;
            tokio::time::sleep(std::time::Duration::from_millis(s % 3)).await;
            tokio::task::yield_now().await;
            if e - s + 1 > self.1 {
                bail!("-32602: {}", self.2);
            }
            Ok(Value::Array((s..=e).map(|b| json!(b)).collect()))
        }
    }

    /// A span refused for its size is asked again smaller, even when siblings that finish after
    /// the refusal have grown the window straight back.
    #[tokio::test]
    async fn a_refused_span_retries_smaller_whatever_the_window_grew_to() {
        /// Refuses spans over 100 blocks that hold block 5,000. Every other span succeeds slowly
        /// and grows the window back to its ceiling, as busy siblings would.
        struct Regrows(std::sync::Arc<Window>, AtomicU64);
        impl Rpc for Regrows {
            async fn call(&self, _: &str, p: Value) -> Result<Value> {
                let s = hex_u64(&p[0]["fromBlock"])?;
                let e = hex_u64(&p[0]["toBlock"])?;
                if (s..=e).contains(&5_000) && e - s + 1 > 100 {
                    if self.1.fetch_add(1, Ordering::SeqCst) >= 8 {
                        bail!("401 Unauthorized: block 5,000 refused again and again");
                    }
                    bail!("query timed out");
                }
                tokio::time::sleep(std::time::Duration::from_millis(5)).await;
                for _ in 0..8 * REGROW_AFTER {
                    self.0.succeeded(self.0.get());
                }
                Ok(Value::Array((s..=e).map(|b| json!(b)).collect()))
            }
        }
        let w = std::sync::Arc::new(Window::new(1_000));
        let rpc = Regrows(w.clone(), AtomicU64::new(0));
        let got = ranged(&rpc, &w, 0, 99_999, "trace_filter", blocks)
            .await
            .unwrap();
        assert_eq!(got, (0..=99_999u64).map(|b| json!(b)).collect::<Vec<_>>());
    }

    #[test]
    fn a_batch_transfer_decodes_every_id() {
        // ids [1, 2], values [10, 20]
        let words = ["40", "a0", "2", "1", "2", "2", "a", "14"].map(|w| format!("{w:0>64}"));
        let (ids, values) = batch_arrays(&words.concat()).unwrap();
        assert_eq!(ids, ["1", "2"]);
        assert_eq!(values, ["10", "20"]);
    }

    /// A type-4 transaction's row carries its authorization list, as rotki's decoder needs to name a
    /// delegation or a revocation; others carry none. Pinned from mainnet transaction
    /// 0x63d68742…3f40 at block 23500010 (the eip7702 parity case), whose authorization the RPC
    /// returned on 2026-10-11. Etherscan's txlist omits the field.
    #[test]
    fn a_type_4_row_carries_its_authorizations() {
        let tx = json!({"blockNumber": "0x166989a", "blockHash": "0xbb", "hash": "0x63d68742e5dc553a4fed594538f17d7b33f84921a51bece08f486d1f7eb73f40",
            "nonce": "0x0", "transactionIndex": "0x7", "from": "0x689256b5008983673cbdb97a227cf32f38798555",
            "to": "0x689256b5008983673cbdb97a227cf32f38798555", "value": "0x0", "gas": "0x1", "gasPrice": "0x1",
            "input": "0x", "type": "0x4", "authorizationList": [{"chainId": "0x1",
            "address": "0x000000009B1D0aF20D8C6d0A44e162d11F9b8f00", "nonce": "0x1", "yParity": "0x0",
            "r": "0x4c7ed4bff711025b9ace5cef6df93196ab91b4687f717d0a38ee6224f133ee10",
            "s": "0x2036c3b1fbd979ac7bd5cfca2603f232974923611b866ef15a1934eaae0da0f2"}]});
        let receipt = json!({"status": "0x1", "cumulativeGasUsed": "0x1", "gasUsed": "0x1", "contractAddress": null});
        let r = txlist_record(&tx, &receipt, 1_759_527_755, false).unwrap();
        assert_eq!(
            r["authorizationList"],
            json!([{"chainId": "1", "address": "0x000000009b1d0af20d8c6d0a44e162d11f9b8f00", "nonce": "1",
                "yParity": "0", "r": "0x4c7ed4bff711025b9ace5cef6df93196ab91b4687f717d0a38ee6224f133ee10",
                "s": "0x2036c3b1fbd979ac7bd5cfca2603f232974923611b866ef15a1934eaae0da0f2"}])
        );
        let mut plain = tx.clone();
        plain.as_object_mut().unwrap().remove("authorizationList");
        assert!(txlist_record(&plain, &receipt, 1, false)
            .unwrap()
            .get("authorizationList")
            .is_none());
        // It survives the store's encoding.
        let mut buf = Vec::new();
        crate::address_history::tests_encode_roundtrip(&r, &mut buf);
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
            let pending = Pending::begin(history)?;
            let found = self.discover(history, address, from, to, &pending).await;
            pending.persist(history, true)?;
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
                "eth_getBlockByNumber" => {
                    let b = hex_u64(&p[0])?;
                    json!({"timestamp": "0x10", "number": format!("0x{b:x}"), "hash": format!("0x{b:064x}")})
                }
                "eth_getBlockTransactionCountByNumber" => json!("0x5"),
                other => bail!("unexpected {other}"),
            })
        });
        let trace = script(move |m, p| {
            Ok(match m {
                // The trace endpoint is on the main endpoint's chain.
                "eth_getBlockByNumber" => {
                    let b = hex_u64(&p[0])?;
                    json!({"number": format!("0x{b:x}"), "hash": format!("0x{b:064x}")})
                }
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

    /// A delegation set at 150 and cleared at 179 moves the nonce twice with no outgoing
    /// transaction. Each rise of one is the block where the code changed. The same rises with empty
    /// code, and both authorisations in one block, stay refused: post-state code is empty on both
    /// sides of a same-block set and clear.
    #[tokio::test]
    async fn a_delegation_set_and_cleared_inside_the_window_is_explained_by_the_code() {
        let designator = format!("0xef0100{}", "ab".repeat(20));
        let main_for = |nonce_of: fn(u64) -> u64, code_of: fn(u64) -> bool| {
            let designator = designator.clone();
            script(move |m, p| {
                Ok(match m {
                    "eth_getCode" => {
                        let b = hex_u64(&p[1])?;
                        json!(if code_of(b) {
                            designator.as_str()
                        } else {
                            "0x"
                        })
                    }
                    "eth_getTransactionCount" => {
                        json!(format!("0x{:x}", nonce_of(hex_u64(&p[1])?)))
                    }
                    "eth_getBlockTransactionCountByNumber" => json!("0x5"),
                    "eth_getLogs" => json!([]),
                    other => bail!("unexpected {other}"),
                })
            })
        };
        let trace = || {
            script(|m, _| match m {
                "trace_filter" => Ok(json!([])),
                "trace_block" => Ok(json!([{"x": 1}])),
                other => bail!("unexpected {other}"),
            })
        };
        let run = |nonce_of: fn(u64) -> u64, code_of: fn(u64) -> bool| {
            let dir = tempfile::tempdir().unwrap();
            let h = history(dir.path());
            let d = Discoverer::new(main_for(nonce_of, code_of), trace());
            async move {
                let _dir = dir;
                d.discover_now(&h, A, 100, 199).await
            }
        };
        let hump = |b: u64| {
            if b >= 179 {
                2
            } else if b >= 150 {
                1
            } else {
                0
            }
        };
        let delegated = |b: u64| (150..=178).contains(&b);
        run(hump, delegated)
            .await
            .expect("each rise is one block where the delegation changed");

        let err = run(hump, |_| false).await.unwrap_err();
        assert!(format!("{err:#}").contains("by its nonce"), "{err:#}");

        let same_block = |b: u64| u64::from(b >= 150) * 2;
        let err = run(same_block, |_| false).await.unwrap_err();
        assert!(
            format!("{err:#}").contains("by its nonce"),
            "a set and a clear in one block: {err:#}"
        );
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

    /// A traced sweep finds value a contract sends a watched address from inside a call, which no
    /// top-level read shows: how smart-wallet deposits arrive. A trace naming another block hash than
    /// the main endpoint's refuses the window (RFC-0064 S2).
    #[tokio::test]
    async fn a_traced_sweep_finds_value_sent_from_inside_a_call() {
        let dir = tempfile::tempdir().unwrap();
        let h = history(dir.path());
        const C: &str = "0x00000000000000000000000000000000000000c0";
        const OUT: &str = "0x00000000000000000000000000000000000000ee";
        let n = 17_000_000u64;
        let bh = format!("0x{:0>64}", "b1");
        let main = || {
            let bh = bh.clone();
            script(move |m, _| {
                Ok(match m {
                    "eth_getBlockByNumber" => json!({"number": format!("0x{n:x}"), "hash": bh,
                        "timestamp": "0x64", "transactions": []}),
                    "eth_getLogs" => json!([]),
                    other => bail!("unexpected {other}"),
                })
            })
        };
        let frame = |tx: &str,
                     pos: u64,
                     path: Value,
                     from: &str,
                     to: &str,
                     value: &str,
                     block: &str| {
            json!({"type": "call", "traceAddress": path, "blockHash": block, "transactionHash": tx,
                   "transactionPosition": pos, "blockNumber": n,
                   "action": {"from": from, "to": to, "value": value, "callType": "call",
                              "gas": "0x0"},
                   "result": {"gasUsed": "0x0"}})
        };
        let (h1, h2, h3) = (
            format!("0x{:0>64}", "1"),
            format!("0x{:0>64}", "2"),
            format!("0x{:0>64}", "3"),
        );
        let frames = |block: &str| {
            let mut callcode = frame(&h3, 2, json!([0]), C, A, "0x7", block);
            callcode["action"]["callType"] = json!("callcode");
            json!([
                frame(&h1, 0, json!([]), OUT, C, "0x0", block),
                frame(&h1, 0, json!([0]), C, A, "0x5", block),
                frame(&h2, 1, json!([]), OUT, C, "0x0", block),
                frame(&h2, 1, json!([0]), C, OUT, "0x1", block),
                frame(&h3, 2, json!([]), OUT, C, "0x0", block),
                callcode,
                {"type": "reward", "action": {"author": OUT, "value": "0x1"}, "blockHash": block,
                 "blockNumber": n, "traceAddress": []},
            ])
        };
        let set: std::collections::HashSet<String> = [A.to_string()].into();
        for (named, refused) in [(bh.clone(), false), (format!("0x{:0>64}", "ff"), true)] {
            let answer = frames(&named);
            let d = Discoverer::new(
                main(),
                script(move |m, _| match m {
                    "trace_block" => Ok(answer.clone()),
                    other => bail!("unexpected {other}"),
                }),
            );
            let pending = Pending::begin(&h).unwrap();
            let swept = d.sweep(&h, &set, n, n, &pending, true).await;
            if refused {
                assert!(swept.is_err(), "a trace of another block is refused");
                continue;
            }
            let found = swept.unwrap();
            assert_eq!(found.keys().collect::<Vec<_>>(), [A]);
            let rows = &found[A].txlistinternal;
            assert_eq!(
                rows.len(),
                2,
                "Etherscan lists the callcode frame too: {rows:?}"
            );
            assert_eq!(rows[0].record["hash"], h1);
            assert_eq!(rows[0].record["from"], C);
            assert_eq!(rows[0].record["value"], "5");
            assert_eq!(rows[1].record["hash"], h3);
            let marked: Vec<_> = found[A].callcode.iter().copied().collect();
            assert_eq!(
                marked,
                [(n, 2, rows[1].position)],
                "only the callcode frame is marked"
            );
        }
    }

    /// One pass over a block finds every watched address's transactions and transfers, both
    /// directions, and nothing for anyone outside the set (RFC-0064 S1).
    #[tokio::test]
    async fn a_sweep_matches_a_block_against_the_whole_set() {
        let dir = tempfile::tempdir().unwrap();
        let h = history(dir.path());
        const B: &str = "0x00000000000000000000000000000000000000b0";
        const OUT: &str = "0x00000000000000000000000000000000000000ee";
        let n = 17_000_000u64;
        let bh = format!("0x{:0>64}", "b1");
        let tx = |hash: &str, from: &str, to: &str, i: u64| {
            json!({"blockNumber": format!("0x{n:x}"), "blockHash": bh, "hash": hash,
                   "nonce": "0x0", "transactionIndex": format!("0x{i:x}"), "from": from, "to": to,
                   "value": "0x1", "gas": "0x5208", "gasPrice": "0x1", "input": "0x"})
        };
        let hashes = [1, 2, 3].map(|i| format!("0x{:0>64}", i));
        let txs = vec![
            tx(&hashes[0], A, OUT, 0),
            tx(&hashes[1], OUT, B, 1),
            tx(&hashes[2], OUT, OUT, 2),
        ];
        let log = |from: &str, to: &str, index: u64| {
            json!({"address": "0x00000000000000000000000000000000000000c0",
                   "topics": [TRANSFER, padded(from), padded(to)],
                   "data": format!("0x{:0>64}", "64"), "blockNumber": format!("0x{n:x}"),
                   "transactionHash": hashes[2], "transactionIndex": "0x2",
                   "logIndex": format!("0x{index:x}"), "blockHash": bh, "removed": false})
        };
        let logs = vec![log(OUT, A, 0), log(OUT, OUT, 1)];
        let (block_txs, by_hash, bh2) = (txs.clone(), txs, bh.clone());
        let main = script(move |m, p| {
            Ok(match m {
                "eth_getBlockByNumber" => json!({"number": format!("0x{n:x}"), "hash": bh2,
                    "timestamp": "0x64", "transactions": block_txs}),
                "eth_getLogs" => json!(logs),
                "eth_getTransactionByHash" => {
                    by_hash.iter().find(|t| t["hash"] == p[0]).cloned().unwrap()
                }
                "eth_getTransactionReceipt" => json!({"status": "0x1", "gasUsed": "0x5208",
                    "cumulativeGasUsed": "0x5208", "effectiveGasPrice": "0x1",
                    "contractAddress": null, "blockHash": bh2}),
                other => bail!("unexpected {other}"),
            })
        });
        let d = Discoverer::new(main, script(|m, _| bail!("no trace call expected: {m}")));
        let set: std::collections::HashSet<String> = [A, B].map(String::from).into();
        let pending = Pending::begin(&h).unwrap();
        let found = d.sweep(&h, &set, n, n, &pending, false).await.unwrap();
        assert_eq!(
            found.keys().collect::<Vec<_>>(),
            [B, A],
            "only watched addresses"
        );
        let a = &found[A];
        assert_eq!(a.txlist.len(), 1);
        assert_eq!(a.txlist[0].record["hash"], hashes[0]);
        assert_eq!(
            a.tokentx.len(),
            1,
            "the transfer to A, not the one between others"
        );
        assert_eq!(a.tokentx[0].record["to"], A);
        let b = &found[B];
        assert_eq!(b.txlist.len(), 1);
        assert_eq!(b.txlist[0].record["hash"], hashes[1]);
        assert!(b.tokentx.is_empty());
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

    /// A cursor's state over `[0, end]`, with the finalized head the cursor's reconcile would have
    /// set before its catch-up, so nothing it backfills needs anchoring.
    fn mode(h: &AddressHistory, end: u64) -> crate::address_mode::ModeState {
        h.finalize(1_000_000).unwrap();
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
                // Only to a request whose range holds block 0x10, as a node answers.
                "eth_getLogs" => {
                    let (from, to) = (hex_u64(&p[0]["fromBlock"])?, hex_u64(&p[0]["toBlock"])?);
                    Ok(if from <= 0x10 && 0x10 <= to {
                        json!([log])
                    } else {
                        json!([])
                    })
                }
                "eth_getBlockByNumber" if hex_u64(&p[0])? == 0x10 && !headers_ok => {
                    bail!("header unavailable")
                }
                _ => (main.answer)(m, p),
            });
            Discoverer::new(Counted::new(main), Counted::new(trace))
        };
        let first = failing(false);
        first
            .catch_up(&mode(&h, 99_999).with_first_window(OUTER_WINDOW), 1_000_000)
            .await;
        assert!(h.coverage(Action::TxList, A).unwrap().is_empty());
        assert_eq!(first.main.calls().get("eth_getTransactionByHash"), Some(&1));

        let second = failing(true);
        second
            .catch_up(&mode(&h, 99_999).with_first_window(OUTER_WINDOW), 1_000_000)
            .await;
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

    const BLOCK_300: &str = "0x3000000000000000000000000000000000000000000000000000000000000300";

    /// A window finds an internal transfer to the watched address, traces its transaction once, and
    /// answers the same frame by address and by txhash under the same position.
    #[tokio::test]
    async fn an_internal_transfer_is_found_and_answered_both_ways() {
        let dir = tempfile::tempdir().unwrap();
        let h = history(dir.path());
        let (main, _) = chain(false);
        let tx = "0x00000000000000000000000000000000000000000000000000000000000000ee";
        let whole = json!([
            {"traceAddress": [], "type": "call", "transactionHash": tx, "blockNumber": 300, "blockHash": BLOCK_300,
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
        // Finalized only through block 299, the trace of block 300 is kept with its block's hash
        // tracked, so a reorg found later takes it back out.
        h.set_head(299).unwrap();
        d.discover_now(&h, A, 300, 399).await.unwrap();
        assert!(h.tx_internals(&key).unwrap().is_some());
        assert_eq!(
            h.tracked().unwrap(),
            vec![(300, word32(BLOCK_300).unwrap())]
        );
        h.invalidate_above(299).unwrap();
        assert!(h.tx_internals(&key).unwrap().is_none());
        assert!(h.tracked().unwrap().is_empty());
        // A standalone lookup is refused past the limit it is given, and nothing is kept.
        let pending = Pending::default();
        let err = d.internal_rows_of(&h, tx, &pending, 299).await.unwrap_err();
        assert!(err.downcast_ref::<NotFinalized>().is_some(), "{err:#}");

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
        assert_eq!(
            traced_calls, 3,
            "kept with its hash tracked, refused past a standalone limit after the reorg, kept again"
        );

        // A reorg below block 300 takes the trace with it; one above leaves it.
        h.invalidate_above(300).unwrap();
        assert!(h.tx_internals(&key).unwrap().is_some());
        h.invalidate_above(299).unwrap();
        assert!(h.tx_internals(&key).unwrap().is_none());

        // A trace fetched before a reorg and persisted after it is not kept.
        h.set_head(299).unwrap();
        let pending = Pending::begin(&h).unwrap();
        d.internal_rows_of(&h, tx, &pending, 400).await.unwrap();
        h.invalidate_above(299).unwrap();
        pending.persist(&h, true).unwrap();
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

    /// The cursor traces a txhash the store has not seen, once, up to the chain head; a block above
    /// the finalized head is kept with its hash tracked. Nothing is traced for a request `respond`
    /// would refuse anyway.
    #[tokio::test]
    async fn a_cold_txhash_is_traced_once_up_to_the_head() {
        use tower::ServiceExt;
        let dir = tempfile::tempdir().unwrap();
        let h = history(dir.path());
        let tx = format!("0x{}", "ee".repeat(32));
        let block_hash = format!("0x{}", "ab".repeat(32));
        let whole = json!([
            {"traceAddress": [], "type": "call", "blockNumber": 300, "blockHash": block_hash, "transactionPosition": 1,
             "action": {"from": "0x02", "to": "0x03", "value": "0x0", "callType": "call"}},
            {"traceAddress": [0], "type": "call", "blockNumber": 300, "blockHash": block_hash, "transactionPosition": 1,
             "action": {"from": "0x03", "to": "0x04", "value": "0x9", "callType": "call", "gas": "0x1"},
             "result": {"gasUsed": "0x0"}},
        ]);
        let latest = std::sync::Arc::new(AtomicU64::new(299));
        let head = latest.clone();
        // The main endpoint's block 300; a trace naming another hash for it is of a fork.
        let canonical = std::sync::Arc::new(Mutex::new(block_hash.clone()));
        let main_300 = canonical.clone();
        let main = script(move |m, _| match m {
            "eth_blockNumber" => Ok(json!(format!("0x{:x}", head.load(Ordering::Relaxed)))),
            "eth_getBlockByNumber" => {
                Ok(json!({"timestamp": "0x10", "hash": main_300.lock().unwrap().clone()}))
            }
            other => bail!("unexpected {other}"),
        });
        let trace = script(move |m, _| match m {
            "trace_transaction" => Ok(whole.clone()),
            other => bail!("unexpected {other}"),
        });
        let d = std::sync::Arc::new(Discoverer::new(Counted::new(main), Counted::new(trace)));
        let state = std::sync::Arc::new(crate::address_mode::ModeState::new(
            h.clone(),
            1,
            std::time::Duration::from_secs(300),
            true,
        ));
        let cursor = tokio::spawn(crate::address_mode::serve_forever(state.clone(), d.clone()));
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

        // The chain head is under the transaction's block, so it is not kept.
        h.set_head(200).unwrap();
        let young = get(path("")).await;
        assert!(
            young["result"]
                .as_str()
                .unwrap()
                .starts_with("NUTHATCH_INCOMPLETE:"),
            "{young}"
        );
        let key = alloy_primitives::hex::decode(&tx[2..]).unwrap();
        assert!(h.tx_internals(&key).unwrap().is_none());

        latest.store(400, Ordering::Relaxed);
        // The trace endpoint is still on a fork the main endpoint has left.
        *canonical.lock().unwrap() = format!("0x{}", "cd".repeat(32));
        let forked = get(path("")).await;
        assert!(
            forked["result"]
                .as_str()
                .unwrap()
                .starts_with("NUTHATCH_INCOMPLETE:"),
            "{forked}"
        );
        assert!(
            h.tx_internals(&key).unwrap().is_none(),
            "a forked trace is not kept"
        );
        *canonical.lock().unwrap() = block_hash.clone();
        let first = get(path("")).await;
        let kept_first = first.clone();
        assert_eq!(first["result"][0]["value"], "9", "{first}");
        assert_eq!(
            h.tracked().unwrap(),
            vec![(300, word32(&block_hash).unwrap())],
            "above the finalized head the trace's block hash is tracked"
        );
        let calls = traced();
        let again = get(path("")).await;
        assert_eq!(again["result"], first["result"]);
        assert_eq!(
            traced(),
            calls,
            "the second lookup is answered from the store"
        );

        // A reorg since the last poll takes block 300: the kept trace is checked before it is
        // served, taken back, and the fork the trace endpoint still serves is refused.
        *canonical.lock().unwrap() = format!("0x{}", "ef".repeat(32));
        let after = get(path("")).await;
        assert!(
            after["result"]
                .as_str()
                .unwrap()
                .starts_with("NUTHATCH_INCOMPLETE:"),
            "{after}"
        );
        assert!(h.tx_internals(&key).unwrap().is_none());
        assert_ne!(after, kept_first);

        // Under the finalized head a trace naming a block the main endpoint does not have is
        // refused too: finality says which history stands, not that another provider returned it.
        h.finalize(400).unwrap();
        let finalized = get(path("")).await;
        assert!(
            finalized["result"]
                .as_str()
                .unwrap()
                .starts_with("NUTHATCH_INCOMPLETE:"),
            "{finalized}"
        );
        assert!(h.tx_internals(&key).unwrap().is_none());
        cursor.abort();
    }

    /// An explicit `endblock` above what the cursor has covered is covered on demand, by the cursor,
    /// and answered exactly; past the chain head it is refused. Cold test B1 (2026-10-11): the nest
    /// answered 17 of Etherscan's 65 rows as complete for an end above its head.
    #[tokio::test]
    async fn an_endblock_above_coverage_is_covered_on_demand_and_answered_exactly() {
        use crate::address_mode::Discovery;
        use tower::ServiceExt;
        let dir = tempfile::tempdir().unwrap();
        let h = history(dir.path());
        let (main, trace) = chain(false);
        let main = script(move |m, p| match m {
            "eth_blockNumber" => Ok(json!("0x12c")),
            "eth_getBlockByNumber" => {
                let b = hex_u64(&p[0])?;
                Ok(
                    json!({"number": format!("0x{b:x}"), "timestamp": "0x10", "hash": format!("0x{b:064x}")}),
                )
            }
            _ => (main.answer)(m, p),
        });
        let d = std::sync::Arc::new(Discoverer::new(Counted::new(main), Counted::new(trace)));
        h.finalize(100).unwrap();
        let state = std::sync::Arc::new(crate::address_mode::ModeState::new(
            h.clone(),
            1,
            std::time::Duration::from_secs(300),
            true,
        ));
        d.catch_up(&state, 100).await;
        assert_eq!(h.coverage(Action::TxList, A).unwrap(), vec![(0, 100)]);
        let cursor = tokio::spawn(crate::address_mode::serve_forever(state.clone(), d.clone()));
        let get = |end: u64| {
            let app = crate::address_mode::router(state.clone());
            async move {
                let path = format!(
                    "/api?module=account&action=txlist&address={A}&startblock=0&endblock={end}"
                );
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
        let a = get(200).await;
        assert_eq!(a["status"], "1", "{a}");
        assert_eq!(
            a["result"].as_array().unwrap().len(),
            1,
            "the transaction at 150: {a}"
        );
        assert_eq!(h.coverage(Action::TxList, A).unwrap(), vec![(0, 200)]);
        assert_eq!(
            h.tracked().unwrap(),
            vec![(200, word32(&format!("0x{:064x}", 200)).unwrap())]
        );
        assert_eq!(
            h.head().unwrap(),
            Some(100),
            "on-demand coverage finalizes nothing"
        );

        let past = get(400).await;
        let r = past["result"].as_str().unwrap();
        assert!(
            r.starts_with("NUTHATCH_INCOMPLETE:") && r.contains("past the chain head"),
            "{past}"
        );
        assert_eq!(h.coverage(Action::TxList, A).unwrap(), vec![(0, 200)]);
        cursor.abort();
    }

    /// A transaction re-included in another block keeps its hash; the copy cached at its old block
    /// is not used, and a reorg past it removes it.
    #[tokio::test]
    async fn a_reincluded_transaction_is_read_again_not_served_from_its_old_block() {
        let dir = tempfile::tempdir().unwrap();
        let h = history(dir.path());
        let (main, trace) = chain(false);
        let d = Discoverer::new(main, trace);
        let key = alloy_primitives::hex::decode("aa".repeat(32)).unwrap();
        let mut stale = Map::new();
        stale.insert("blockNumber".into(), json!("149"));
        stale.insert("hash".into(), json!(format!("0x{}", "aa".repeat(32))));
        h.persist_pending(&[(key.clone(), stale)], &[], &[], &[], &[], 0, None, true)
            .unwrap();
        let pending = Pending::default();
        let r = TxRef {
            hash: format!("0x{}", "aa".repeat(32)),
            block: 150,
            outgoing: true,
            failed: false,
            block_hash: None,
        };
        let got = d.hydrate_one(&h, r, &pending).await.unwrap();
        assert_eq!(got["blockNumber"], "150", "read again at its new block");
        let asked = d
            .main
            .asked
            .lock()
            .unwrap()
            .iter()
            .filter(|(m, _)| m == "eth_getTransactionByHash")
            .count();
        assert_eq!(asked, 1);

        h.invalidate_above(148).unwrap();
        assert!(
            h.cached_tx(&key).unwrap().is_none(),
            "a reorg under 149 drops the old copy"
        );
    }

    /// The same block number on another fork is not the cached transaction. Reading it again also
    /// re-reads the timestamp, which was stored against the old header. The same fork hits neither.
    #[tokio::test]
    async fn a_cached_transaction_from_another_fork_is_read_again() {
        let dir = tempfile::tempdir().unwrap();
        let h = history(dir.path());
        let hash_hex = format!("0x{}", "aa".repeat(32));
        let key = alloy_primitives::hex::decode("aa".repeat(32)).unwrap();
        let fork = |n: u8| {
            let mut x = [0u8; 32];
            x[31] = n;
            x
        };
        let a = fork(1);
        let b = fork(2);
        let hex_hash = |h: [u8; 32]| format!("0x{}", alloy_primitives::hex::encode(h));
        let mut stale = Map::new();
        stale.insert("blockNumber".into(), json!("150"));
        stale.insert("hash".into(), json!(hash_hex));
        stale.insert("blockHash".into(), json!(hex_hash(a)));
        stale.insert("timeStamp".into(), json!("16"));
        h.persist_pending(
            &[(key, stale)],
            &[(150, 16, Some(a))],
            &[],
            &[],
            &[],
            0,
            None,
            true,
        )
        .unwrap();

        let (main, trace) = chain(false);
        let bb = hex_hash(b);
        let main = script(move |m, p| {
            let mut v = (main.answer)(m, p)?;
            if m == "eth_getTransactionByHash" || m == "eth_getTransactionReceipt" {
                v["blockHash"] = json!(bb.clone());
            }
            if m == "eth_getBlockByNumber" {
                v["hash"] = json!(bb.clone());
            }
            Ok(v)
        });
        let d = Discoverer::new(main, trace);
        let pending = Pending::begin(&h).unwrap();
        let r = TxRef {
            hash: hash_hex,
            block: 150,
            outgoing: true,
            failed: false,
            block_hash: Some(b),
        };
        let got = d.hydrate_one(&h, r.clone(), &pending).await.unwrap();
        assert_eq!(got["blockNumber"], "150");
        let asked = |method: &str| {
            d.main
                .asked
                .lock()
                .unwrap()
                .iter()
                .filter(|(m, _)| m == method)
                .count()
        };
        assert_eq!(asked("eth_getTransactionByHash"), 1);
        assert_eq!(
            asked("eth_getBlockByNumber"),
            1,
            "the timestamp was read against the other fork"
        );
        pending.persist(&h, true).unwrap();

        let pending = Pending::begin(&h).unwrap();
        let before_tx = asked("eth_getTransactionByHash");
        let before_hdr = asked("eth_getBlockByNumber");
        let got = d.hydrate_one(&h, r, &pending).await.unwrap();
        assert_eq!(got["blockHash"], hex_hash(b));
        assert_eq!(asked("eth_getTransactionByHash"), before_tx);
        assert_eq!(asked("eth_getBlockByNumber"), before_hdr);
        assert_eq!(pending.named(), vec![(150, b)]);
    }

    const B: &str = "0x00000000219ab540356cbb839cbe05303d7705fa";

    /// Bodies for blocks past Shanghai: block x02 pays A twice and B once, block x05 is B's.
    fn bodies(missing_withdrawals_at: Option<u64>) -> Script {
        script(move |m, p| {
            Ok(match m {
                "eth_getBlockByNumber" => {
                    let b = hex_u64(&p[0])?;
                    let mut h = json!({"number": format!("0x{b:x}"), "timestamp": format!("0x{:x}", 1_000 + b),
                        "hash": format!("0x{b:064x}"), "parentHash": format!("0x{:064x}", b - 1),
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

    /// A body that does not name the previous one as its parent is from another fork (a pool's
    /// endpoints can disagree), and fails the scan; the last body's hash is named for the main check.
    #[tokio::test]
    async fn a_scan_whose_bodies_are_from_two_chains_fails() {
        let (_, trace) = chain(false);
        let good = Discoverer::new(bodies(None), trace);
        let pending = Pending::default();
        good.scan_blocks(&[A.into()], 17_100_000, 17_100_009, &pending)
            .await
            .unwrap();
        assert_eq!(
            pending.named(),
            vec![(
                17_100_009,
                word32(&format!("0x{:064x}", 17_100_009u64)).unwrap()
            )]
        );

        let (_, trace) = chain(false);
        let inner = bodies(None);
        let forked = script(move |m, p| {
            let mut v = (inner.answer)(m, p)?;
            if m == "eth_getBlockByNumber" && hex_u64(&p[0])? == 17_100_005 {
                v["parentHash"] = json!(format!("0x{}", "9".repeat(64)));
            }
            Ok(v)
        });
        let d = Discoverer::new(forked, trace);
        let err = d
            .scan_blocks(&[A.into()], 17_100_000, 17_100_009, &Pending::default())
            .await
            .unwrap_err();
        assert!(err.to_string().contains("two chains"), "{err:#}");
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

    /// A scan that fails on any body keeps no summary, so its retry reads the bodies again rather
    /// than taking a summary of a body without its withdrawals for a complete, empty one.
    #[tokio::test]
    async fn a_failed_scan_keeps_no_summary() {
        let good = bodies(None);
        let main = script(move |m, p| {
            let mut v = (good.answer)(m, p)?;
            if m == "eth_getBlockByNumber" && v["number"] == json!(format!("0x{:x}", 17_100_005)) {
                v.as_object_mut().unwrap().remove("withdrawals");
            }
            Ok(v)
        });
        let (_, trace) = chain(false);
        let d = Discoverer::new(main, trace);
        let pending = Pending::default();
        let err = d
            .scan_blocks(&[B.into()], 17_100_004, 17_100_005, &pending)
            .await
            .unwrap_err();
        assert!(format!("{err:#}").contains("no withdrawals"), "{err:#}");
        assert!(pending.summaries.lock().unwrap().is_empty());
    }

    /// A call that fails, of any kind the sweep makes, leaves its window unswept: no span, no
    /// coverage, no rows. The next poll reads the window again and covers it whole (RFC-0064 S1).
    #[tokio::test]
    async fn a_failed_call_leaves_the_window_unswept_and_retried() {
        use crate::address_mode::Discovery;
        use std::sync::atomic::{AtomicBool, Ordering::Relaxed};
        let n = 17_000_000u64;
        let sent = format!("0x{:0>64}", "a1");
        for failing in [
            "eth_getBlockByNumber",
            "eth_getLogs",
            "eth_getTransactionReceipt",
        ] {
            let dir = tempfile::tempdir().unwrap();
            let h = history(dir.path());
            let fail = std::sync::Arc::new(AtomicBool::new(true));
            let main = script({
                let (fail, sent) = (fail.clone(), sent.clone());
                move |m, p| {
                    if m == failing && fail.load(Relaxed) {
                        bail!("injected failure of {m}");
                    }
                    let tx = |b: u64| {
                        json!({"blockNumber": format!("0x{b:x}"), "blockHash": format!("0x{b:064x}"),
                               "hash": sent, "nonce": "0x0", "transactionIndex": "0x0", "from": A,
                               "to": "0x01", "value": "0x1", "gas": "0x5208", "gasPrice": "0x1",
                               "input": "0x"})
                    };
                    Ok(match m {
                        "eth_getBlockByNumber" => {
                            let b = hex_u64(&p[0])?;
                            let txs = if b == n + 5 { vec![tx(b)] } else { vec![] };
                            json!({"number": format!("0x{b:x}"), "hash": format!("0x{b:064x}"),
                                   "parentHash": format!("0x{:064x}", b - 1),
                                   "timestamp": "0x64", "transactions": txs})
                        }
                        "eth_getLogs" => json!([]),
                        "eth_getTransactionByHash" => tx(n + 5),
                        "eth_getTransactionReceipt" => json!({"status": "0x1",
                            "gasUsed": "0x5208", "cumulativeGasUsed": "0x5208",
                            "effectiveGasPrice": "0x1", "contractAddress": null,
                            "blockHash": format!("0x{:064x}", n + 5)}),
                        other => bail!("unexpected {other}"),
                    })
                }
            });
            let d = Discoverer::new(
                Counted::new(main),
                Counted::new(script(|m, _| bail!("no trace call expected: {m}"))),
            );
            let state = crate::address_mode::ModeState::new(
                h.clone(),
                1,
                std::time::Duration::from_secs(300),
                true,
            )
            .with_range(n, None, 0)
            .with_sweep(Some(crate::address_history::SweepConfig {
                from_block: Some(n),
                internal: false,
                confirmations: None,
                webhook: None,
            }));
            h.finalize(n + 100).unwrap();
            d.catch_up(&state, n + 9).await;
            assert_eq!(
                h.swept().unwrap(),
                None,
                "{failing}: nothing past a failed window"
            );
            assert!(
                h.coverage(Action::TxList, A).unwrap().is_empty(),
                "{failing}"
            );

            fail.store(false, Relaxed);
            d.catch_up(&state, n + 9).await;
            assert_eq!(
                h.swept().unwrap(),
                Some((n, n + 9)),
                "{failing}: retried whole"
            );
            let req = crate::address_history::PageRequest {
                address: A.into(),
                start_block: Some(n),
                end_block: Some(n + 9),
                sort: crate::address_history::Sort::Asc,
                page: 1,
                offset: 10,
                generation: None,
            };
            let crate::address_history::Answer::Rows(rows) = h.page(Action::TxList, &req).unwrap()
            else {
                panic!("{failing}: covered after the retry")
            };
            assert_eq!(rows.len(), 1, "{failing}: the transaction at {}", n + 5);
        }
    }

    /// A sweeping nest covers its whole set from the configured block with block reads and one log
    /// query per window, and never discovers an address on its own (RFC-0064 S1).
    #[tokio::test]
    async fn a_sweeping_nest_covers_its_set_without_per_address_discovery() {
        use crate::address_mode::Discovery;
        // Counted inside the script: `call_each` counts only calls that answered.
        let traced = std::sync::Arc::new(std::sync::atomic::AtomicUsize::new(0));
        let dir = tempfile::tempdir().unwrap();
        let h = history(dir.path());
        let n = 17_000_000u64;
        let main = script(move |m, p| {
            Ok(match m {
                "eth_getBlockByNumber" => {
                    let b = hex_u64(&p[0])?;
                    json!({"number": format!("0x{b:x}"), "hash": format!("0x{b:064x}"),
                           "parentHash": format!("0x{:064x}", b - 1),
                           "timestamp": "0x64", "transactions": []})
                }
                "eth_getLogs" => json!([]),
                other => bail!("unexpected {other}"),
            })
        });
        let d = Discoverer::new(
            Counted::new(main),
            Counted::new(script({
                let asked = traced.clone();
                move |m, _| {
                    asked.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                    bail!("no trace call expected: {m}")
                }
            })),
        );
        let state = crate::address_mode::ModeState::new(
            h.clone(),
            1,
            std::time::Duration::from_secs(300),
            true,
        )
        // History starts 50 blocks before the sweep: a per-address pass would try to fill that gap.
        .with_range(n - 50, None, 0)
        .with_sweep(Some(crate::address_history::SweepConfig {
            from_block: Some(n),
            internal: false,
            confirmations: None,
            webhook: None,
        }));
        h.finalize(n + 100).unwrap();
        d.catch_up(&state, n + 9).await;
        assert_eq!(h.swept().unwrap(), Some((n, n + 9)));
        assert_eq!(h.coverage(Action::TokenTx, A).unwrap(), vec![(n, n + 9)]);
        let calls = d.main.calls();
        assert!(
            !calls.contains_key("eth_getTransactionCount") && !calls.contains_key("eth_getCode"),
            "no per-address discovery: {calls:?}"
        );
        assert!(
            traced.load(std::sync::atomic::Ordering::Relaxed) == 0,
            "per-address discovery asks the trace endpoint; the sweep never does"
        );
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
        h.finalize(17_200_000).unwrap();
        let bodies_read = || {
            d.main
                .calls()
                .get("eth_getBlockByNumber")
                .copied()
                .unwrap_or(0)
        };
        // Nothing asked for A's withdrawals or produced blocks yet, so no body is read for it.
        d.catch_up(&state, 17_200_000).await;
        assert!(h.coverage(Action::BeaconWithdrawals, A).unwrap().is_empty());
        let unasked = bodies_read();

        h.ask_bodies(A).unwrap();
        d.catch_up(&state, 17_200_000).await;
        assert_eq!(
            h.coverage(Action::BeaconWithdrawals, A).unwrap(),
            vec![(17_100_000, 17_100_009)]
        );
        assert!(bodies_read() >= unasked + 10, "{unasked} {}", bodies_read());
        let after_first = bodies_read();

        h.watch(B).unwrap();
        h.ask_bodies(B).unwrap();
        d.catch_up(&state, 17_200_000).await;
        assert_eq!(
            h.coverage(Action::MinedBlocks, B).unwrap(),
            vec![(17_100_000, 17_100_009)]
        );
        assert_eq!(
            bodies_read() - after_first,
            5,
            "no body read again for B, its summaries kept from A's scan: one header for the reward \
             of the block B produced, two hashes to check B's discovery window's trace endpoint is on \
             the main chain, and the window's last hash checked twice (cold test M3)"
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
