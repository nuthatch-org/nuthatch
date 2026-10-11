//! The rotki-mode runtime (RFC-0063): a nest with `[address_history]` and no contracts.
//!
//! It shares nothing with the event path. No decode registry, no DBSP circuit, no Burrmill, no
//! sealing: one redb handle, one cursor that reads the chain head every `poll_interval`, and a small
//! router serving `/api`, `/health`, `/ready` and `/metrics`. After each head read the cursor
//! discovers every watched address up to the finalized head ([`Discovery`]).

use anyhow::{bail, Context, Result};
use std::collections::BTreeMap;
use std::sync::Mutex;

use crate::address_discovery::{Counted, Discoverer, Rpc};
use axum::{
    extract::{Query, State},
    http::StatusCode,
    response::IntoResponse,
    routing::{get, post},
    Json, Router,
};
use serde_json::{json, Value};
use std::collections::HashMap;
use std::path::Path;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

use crate::address_history::{AddressHistory, AddressHistoryConfig};
use crate::config::Config;
use crate::rpc::RpcClient;

/// Where the cursor reads the chain head. A trait so tests drive the loop without a network.
pub trait HeadSource: Send + Sync + 'static {
    fn head(&self) -> impl std::future::Future<Output = Result<u64>> + Send;
    /// Every request this source has sent so far, failovers and startup checks included.
    fn requests(&self) -> u64;
}

impl HeadSource for RpcClient {
    async fn head(&self) -> Result<u64> {
        self.block_number().await
    }

    fn requests(&self) -> u64 {
        self.request_count()
    }
}

pub struct ModeState {
    pub history: AddressHistory,
    pub chain_id: u64,
    pub poll_interval: Duration,
    /// Whether the bound socket is a loopback address. Watch and unwatch change what the nest asks its
    /// RPC for, so they are refused on any other bind.
    pub loopback: bool,
    started: Instant,
    /// Milliseconds after `started` of the last successful poll, plus one; 0 for none yet. Monotonic,
    /// so a wall-clock step cannot hide a stall.
    last_poll: AtomicU64,
    polls: AtomicU64,
    poll_errors: AtomicU64,
    /// Whether the last poll, its head read or its reconcile, failed: the RPC is down or hanging.
    failing: std::sync::atomic::AtomicBool,
    rpc_requests: AtomicU64,
    /// Blocks behind the chain head the nest serves through, so nothing it records can be reorged.
    pub depth: u64,
    /// `[address_history]`'s `start_block` and `end_block`: the history each address backfills.
    pub start_block: u64,
    pub end_block: Option<u64>,
    /// Rung by watch, so a new address starts backfilling now rather than at the next poll.
    pub wake: tokio::sync::Notify,
    /// Why the cursor stopped for good, e.g. an RPC on the wrong chain.
    cursor_error: Mutex<Option<String>>,
    /// A conflict with history under the finalized head: nothing is answered while it stands.
    conflict: Mutex<Option<String>>,
    /// Which recorded finalized hash the next poll samples, beside the newest.
    sample: AtomicU64,
    /// RPC calls by endpoint and method.
    calls: Mutex<BTreeMap<(String, String), u64>>,
    windows: AtomicU64,
    discovery_errors: AtomicU64,
    /// Forwards `proxy` and `logs` requests to the nest's RPC.
    pub forward: Option<crate::address_proxy::Forward>,
    /// Reads the RPC counters as they stand, so calls forwarded for a request show at once rather
    /// than when the cursor next reports.
    counter: Option<Counter>,
    /// The MEV-Boost relays, when `mev_relays` turns them on.
    pub relays: Option<crate::address_mev::Relays>,
    /// Verified block partitions, and the blocks `[address_history.mirror]` takes from them.
    pub mirror: Option<(Arc<crate::address_partitions::Source>, (u64, u64))>,
    /// Requests for history above what the cursor has covered, served by the cursor between its
    /// windows so it stays the only writer.
    requests: tokio::sync::mpsc::Sender<Request>,
    queue: tokio::sync::Mutex<tokio::sync::mpsc::Receiver<Request>>,
    /// How long a request may wait for the cursor, queue and RPC work included.
    pub deadline: Duration,
    /// The chain head as last read, 0 for none yet.
    latest: AtomicU64,
    /// `logs/getLogs` requests running at once: each can hold up to a page of logs and its receipts.
    logs: tokio::sync::Semaphore,
}

/// How many `logs/getLogs` requests may run at once; one more answers incomplete at once.
const LOGS_AT_ONCE: usize = 2;

/// Blocks one on-demand discovery window covers, and one body window: small enough that a window
/// started near a request's deadline does not hold the cursor long past it.
const COVER_WINDOW: u64 = 5_000;
const COVER_BODIES: u64 = 500;
const COVER_PRODUCED: u64 = 1_000;

/// The address-history store's page cache.
const STORE_CACHE_BYTES: usize = 32 * 1024 * 1024;

/// How many requests may wait for the cursor at once; past it a request answers incomplete at once.
const QUEUE: usize = 64;

/// What a request needs covered.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Need {
    /// The five actions discovery fills.
    Discovered,
    /// Withdrawals and mined blocks, from block bodies.
    Bodies,
    /// Produced blocks, and the mined blocks and transactions they are built from.
    Produced,
}

impl Need {
    pub fn of(action: crate::address_history::Action) -> Need {
        use crate::address_history::Action;
        match action {
            Action::BeaconWithdrawals | Action::MinedBlocks => Need::Bodies,
            Action::ProducedBlocks => Need::Produced,
            _ => Need::Discovered,
        }
    }
}

pub type Reply = tokio::sync::oneshot::Sender<std::result::Result<(), String>>;

/// Work a request hands the cursor.
pub enum Request {
    Cover {
        address: String,
        need: Need,
        from: u64,
        to: u64,
        deadline: Instant,
        reply: Reply,
    },
    Trace {
        hash: String,
        deadline: Instant,
        reply: Reply,
    },
}

/// The default for `[address_history] request_deadline`: under rotki's 30 s read timeout.
pub const DEFAULT_DEADLINE: Duration = Duration::from_secs(20);

impl ModeState {
    pub fn new(
        history: AddressHistory,
        chain_id: u64,
        poll_interval: Duration,
        loopback: bool,
    ) -> ModeState {
        let (tx, rx) = tokio::sync::mpsc::channel(QUEUE);
        // A store that is unreadable here fails its first request instead, with the reason.
        let conflict = history.conflict().ok().flatten();
        ModeState {
            history,
            chain_id,
            poll_interval,
            loopback,
            started: Instant::now(),
            last_poll: AtomicU64::new(0),
            polls: AtomicU64::new(0),
            poll_errors: AtomicU64::new(0),
            failing: std::sync::atomic::AtomicBool::new(false),
            rpc_requests: AtomicU64::new(0),
            depth: 0,
            start_block: 0,
            end_block: None,
            wake: tokio::sync::Notify::new(),
            counter: None,
            cursor_error: Mutex::new(conflict.clone()),
            conflict: Mutex::new(conflict),
            sample: AtomicU64::new(0),
            calls: Mutex::new(BTreeMap::new()),
            windows: AtomicU64::new(0),
            discovery_errors: AtomicU64::new(0),
            forward: None,
            relays: None,
            mirror: None,
            requests: tx,
            queue: tokio::sync::Mutex::new(rx),
            deadline: DEFAULT_DEADLINE,
            latest: AtomicU64::new(0),
            logs: tokio::sync::Semaphore::new(LOGS_AT_ONCE),
        }
    }

    pub fn with_deadline(mut self, deadline: Duration) -> Self {
        self.deadline = deadline;
        self
    }

    /// Hand `make(deadline, reply)` to the cursor and wait for it, all within the deadline. `Err`
    /// says why it was not done; nothing is partly answered.
    async fn ask(
        &self,
        make: impl FnOnce(Instant, Reply) -> Request,
    ) -> std::result::Result<(), String> {
        let deadline = Instant::now() + self.deadline;
        let (reply, done) = tokio::sync::oneshot::channel();
        if self.requests.try_send(make(deadline, reply)).is_err() {
            return Err("the nest is busy with other requests; try again shortly".into());
        }
        match tokio::time::timeout_at(deadline.into(), done).await {
            Ok(Ok(r)) => r,
            Ok(Err(_)) => Err("the cursor dropped the request".into()),
            Err(_) => Err(format!(
                "not covered within the {}s deadline; try again shortly",
                self.deadline.as_secs()
            )),
        }
    }

    pub fn with_relays(mut self, relays: crate::address_mev::Relays) -> Self {
        self.relays = Some(relays);
        self
    }

    pub fn with_forward(mut self, forward: crate::address_proxy::Forward) -> Self {
        self.forward = Some(forward);
        self
    }

    pub fn with_counter(mut self, counter: Counter) -> Self {
        self.counter = Some(counter);
        self
    }

    pub fn with_mirror(
        mut self,
        source: Arc<crate::address_partitions::Source>,
        blocks: (u64, u64),
    ) -> Self {
        self.mirror = Some((source, blocks));
        self
    }

    pub fn with_range(mut self, start_block: u64, end_block: Option<u64>, depth: u64) -> Self {
        self.start_block = start_block;
        self.end_block = end_block;
        self.depth = depth;
        self
    }

    /// Stop the cursor over a conflict with finalized history, and refuse every answer.
    /// The conflict with finalized history, if one was found: by this cursor, or by any check that
    /// recorded it in the store.
    fn conflicted(&self) -> Option<String> {
        let mut held = self.conflict.lock().expect("conflict lock");
        if held.is_none() {
            *held = self.history.conflict().ok().flatten();
            if let Some(why) = held.clone() {
                drop(held);
                self.stop(why.clone());
                return Some(why);
            }
        }
        held.clone()
    }

    fn conflict(&self, why: String) {
        if let Err(e) = self.history.set_conflict(&why) {
            tracing::error!("address history: could not record the conflict in the store: {e:#}");
        }
        *self.conflict.lock().expect("conflict lock") = Some(why.clone());
        self.stop(why);
    }

    fn stop(&self, why: String) {
        tracing::error!("address history: cursor stopped: {why}");
        *self.cursor_error.lock().expect("cursor_error lock") = Some(why);
    }

    fn set_calls(&self, endpoint: &str, calls: BTreeMap<String, u64>) {
        let mut all = self.calls.lock().expect("calls lock");
        for (method, n) in calls {
            all.insert((endpoint.to_string(), method), n);
        }
    }

    fn stalled(&self) -> bool {
        let since_start = self.started.elapsed();
        let last = self.last_poll.load(Ordering::Relaxed);
        let since_poll = if last == 0 {
            since_start
        } else {
            since_start.saturating_sub(Duration::from_millis(last - 1))
        };
        since_poll > 3 * self.poll_interval || self.failing.load(Ordering::Relaxed)
    }
}

impl<R: Rpc> HeadSource for Counted<R> {
    async fn head(&self) -> Result<u64> {
        let n = self.call("eth_blockNumber", json!([])).await?;
        let s = n
            .as_str()
            .context("eth_blockNumber did not answer a string")?;
        u64::from_str_radix(s.trim_start_matches("0x"), 16).context("eth_blockNumber")
    }

    fn requests(&self) -> u64 {
        self.calls().values().sum()
    }
}

/// What the cursor does after each head read. `()` discovers nothing, for a cursor without discovery.
pub trait Discovery: Send + Sync + 'static {
    /// Backfill up to `safe`, the finalized head.
    fn catch_up(
        &self,
        state: &ModeState,
        safe: u64,
    ) -> impl std::future::Future<Output = ()> + Send;
    /// Check the tracked hashes against the chain under `latest`, take back what a reorg replaced, and
    /// move the finalized head up. Returns the finalized head.
    fn reconcile(
        &self,
        state: &ModeState,
        latest: u64,
    ) -> impl std::future::Future<Output = Result<u64>> + Send;
    /// Do one request's work and answer it.
    fn serve(
        &self,
        state: &ModeState,
        request: Request,
    ) -> impl std::future::Future<Output = ()> + Send;
}

impl Discovery for () {
    async fn catch_up(&self, _: &ModeState, _: u64) {}

    async fn reconcile(&self, state: &ModeState, latest: u64) -> Result<u64> {
        let finalized = latest.saturating_sub(state.depth);
        let history = state.history.clone();
        tokio::task::spawn_blocking(move || history.finalize(finalized)).await??;
        Ok(finalized)
    }

    async fn serve(&self, _: &ModeState, request: Request) {
        let (Request::Cover { reply, .. } | Request::Trace { reply, .. }) = request;
        let _ = reply.send(Err("this nest discovers nothing".into()));
    }
}

impl<D: Discovery> Discovery for Arc<D> {
    async fn catch_up(&self, state: &ModeState, safe: u64) {
        (**self).catch_up(state, safe).await
    }

    async fn reconcile(&self, state: &ModeState, latest: u64) -> Result<u64> {
        (**self).reconcile(state, latest).await
    }

    async fn serve(&self, state: &ModeState, request: Request) {
        (**self).serve(state, request).await
    }
}

/// Serve every request already waiting, in arrival order. A second request for what the first
/// covered finds it covered and costs nothing, which is how same-address requests coalesce.
async fn serve_queued(state: &ModeState, d: &impl Discovery) {
    let waiting: Vec<Request> = {
        let mut q = state.queue.lock().await;
        std::iter::from_fn(|| q.try_recv().ok()).collect()
    };
    for r in waiting {
        d.serve(state, r).await;
    }
}

/// Serve requests as they arrive and nothing else: the cursor without its polls, for tests.
#[cfg(test)]
pub(crate) async fn serve_forever(state: Arc<ModeState>, d: impl Discovery) {
    loop {
        let r = state.queue.lock().await.recv().await;
        match r {
            Some(r) => d.serve(&state, r).await,
            None => return,
        }
    }
}

/// `block`'s hash on the main endpoint.
async fn block_hash(rpc: &impl Rpc, block: u64) -> Result<[u8; 32]> {
    let b = rpc
        .call(
            "eth_getBlockByNumber",
            json!([format!("0x{block:x}"), false]),
        )
        .await?;
    let hash = b
        .get("hash")
        .and_then(Value::as_str)
        .ok_or_else(|| anyhow::anyhow!("block {block} is not on the main endpoint yet"))?;
    crate::address_discovery::word32(hash)
}

/// The anchor for a fetch ending at `to`: its last block's hash, if `to` is above the finalized head.
pub async fn anchor_for<M: Rpc, T: Rpc>(
    d: &Discoverer<Counted<M>, Counted<T>>,
    history: &AddressHistory,
    to: u64,
) -> Result<Option<(u64, [u8; 32])>> {
    if history.head()?.is_some_and(|h| to <= h) {
        return Ok(None);
    }
    Ok(Some((to, block_hash(&d.main, to).await?)))
}

/// Fail a fetch if any block it was read against is not canonical on the main endpoint now: its
/// anchor, every block hash another provider named, and, under each of those, the newest tracked
/// hash. Checking the tracked hash under a new anchor is what makes them one chain: a window read on
/// a fork that replaced a tracked block would otherwise hide that block behind a newer anchor that
/// matches.
pub async fn check_anchor<M: Rpc, T: Rpc>(
    d: &Discoverer<Counted<M>, Counted<T>>,
    history: &AddressHistory,
    anchor: Option<(u64, [u8; 32])>,
    named: &[(u64, [u8; 32])],
) -> Result<()> {
    let tracked = history.tracked()?;
    let mut checks: std::collections::BTreeSet<(u64, [u8; 32])> = std::collections::BTreeSet::new();
    for &(block, hash) in anchor.iter().chain(named) {
        checks.insert((block, hash));
        if let Some(&under) = tracked.iter().rev().find(|(b, _)| *b < block) {
            checks.insert(under);
        }
    }
    // And the newest tracked hash of all, which anchors every block under it: a fetch recorded
    // beneath an anchor on a fork that has since been replaced would otherwise outlive it.
    if !checks.is_empty() {
        if let Some(&newest) = tracked.last() {
            checks.insert(newest);
        }
    }
    // Twice over, so a node that moves to another fork between two comparisons cannot pass one
    // block on each.
    let held = async {
        for _ in 0..2 {
            for &(block, hash) in &checks {
                if block_hash(&d.main, block).await? != hash {
                    bail!(
                        "block {block} is no longer the one this was read against; a reorg is under way"
                    );
                }
            }
        }
        Ok(())
    }
    .await;
    // And the newest finalized hash, whether or not those held, so a switch to a fork under
    // finality after the request's own check is recorded as the conflict it is, not as a reorg.
    hold_final(&d.main, history).await?;
    held
}

/// Hold the newest finalized hash the store recorded against the main endpoint, twice. A mismatch is
/// a conflict with finalized history, recorded in the store at once, so whatever path found it the
/// nest stops answering, across restarts too.
/// Every endpoint behind `main` is asked, so a pool with one endpoint on another fork cannot pass on
/// the others while that one supplies the data.
pub async fn hold_final(main: &impl Rpc, history: &AddressHistory) -> Result<()> {
    match history.finals()?.last() {
        Some(&(block, hash)) => hold_hash(main, history, block, hash).await,
        None => hold_legacy(main, history).await,
    }
}

/// A store from before finalized hashes were recorded has none to hold, but its history was still
/// read on some chain: its newest stored transaction's block hash is held instead, so a first
/// finalized hash is never recorded over history from another fork.
async fn hold_legacy(main: &impl Rpc, history: &AddressHistory) -> Result<()> {
    let Some(head) = history.head()? else {
        return Ok(());
    };
    match history.newest_hashed_row(head)? {
        Some((block, hash)) => hold_hash(main, history, block, hash).await,
        None => Ok(()),
    }
}

/// [`hold_final`] for the one `(block, hash)`.
async fn hold_hash(
    main: &impl Rpc,
    history: &AddressHistory,
    block: u64,
    hash: [u8; 32],
) -> Result<()> {
    let params = json!([format!("0x{block:x}"), false]);
    let why = final_conflict(block);
    for _ in 0..2 {
        let (mut conflict, mut missing) = (None, false);
        // Each answer is judged as it arrives: a mismatch one endpoint confirmed is the conflict,
        // recorded before the next endpoint is awaited, so neither its failure nor a deadline that
        // ends the wait can lose it.
        let mut judge = |a: Result<Value>| {
            let seen = a.ok().and_then(|b| {
                let h = b.get("hash")?.as_str()?.to_string();
                crate::address_discovery::word32(&h).ok()
            });
            match seen {
                Some(h) if h != hash && conflict.is_none() => {
                    conflict = Some(history.set_conflict(&why));
                }
                Some(_) => {}
                None => missing = true,
            }
        };
        main.visit_each("eth_getBlockByNumber", params.clone(), &mut judge)
            .await;
        if let Some(recorded) = conflict {
            recorded?;
            bail!(why);
        }
        if missing {
            bail!("block {block} could not be read from every main endpoint");
        }
    }
    Ok(())
}

/// The hash every main endpoint reports for `block`. Endpoints that disagree above the committed
/// head are a reorg still settling, not a conflict: the head is not advanced until they agree.
async fn agreed_hash(main: &impl Rpc, block: u64) -> Result<[u8; 32]> {
    let answers = main
        .call_each(
            "eth_getBlockByNumber",
            json!([format!("0x{block:x}"), false]),
        )
        .await?;
    let hashes: std::collections::BTreeSet<Option<String>> = answers
        .iter()
        .map(|b| {
            b.get("hash")
                .and_then(Value::as_str)
                .map(str::to_ascii_lowercase)
        })
        .collect();
    match &hashes.into_iter().collect::<Vec<_>>()[..] {
        [Some(h)] => crate::address_discovery::word32(h),
        _ => bail!(
            "the main endpoints do not agree on block {block}; not advancing the finalized head"
        ),
    }
}

fn final_conflict(block: u64) -> String {
    format!(
        "finalized block {block} is now at another hash: a reorg under finality, or an RPC on \
         another chain with the same id; history under the finalized head is never rewritten, so \
         the nest answers nothing; remove the address-history store to read it again"
    )
}

/// Fail a fetch unless every main and trace endpoint is on one chain at `block`: a trace source on
/// another fork, or behind, would answer an empty range the main chain does not have, and a pool
/// rotates its calls across its endpoints, so all of them must agree, not the one that was asked.
async fn same_chain_at<M: Rpc, T: Rpc>(
    d: &Discoverer<Counted<M>, Counted<T>>,
    block: u64,
) -> Result<()> {
    let params = json!([format!("0x{block:x}"), false]);
    let main = d
        .main
        .call_each("eth_getBlockByNumber", params.clone())
        .await?;
    let trace = d.trace.call_each("eth_getBlockByNumber", params).await?;
    let hashes: std::collections::BTreeSet<Option<String>> = main
        .iter()
        .chain(&trace)
        .map(|b| {
            b.get("hash")
                .and_then(Value::as_str)
                .map(str::to_ascii_lowercase)
        })
        .collect();
    if hashes.len() != 1 || hashes.contains(&None) {
        bail!("the main and trace endpoints are not all on one chain at block {block}");
    }
    Ok(())
}

/// Discover one window of `address` and record it, anchored when it ends above the finalized head.
async fn discover_window<M: Rpc, T: Rpc>(
    d: &Discoverer<Counted<M>, Counted<T>>,
    state: &ModeState,
    address: &str,
    (next, end): (u64, u64),
    deadline: Option<Instant>,
) -> Result<()> {
    let pending = crate::address_discovery::Pending::begin(&state.history)?;
    let generation = pending.generation();
    // The whole read, checks included, is one phase under the deadline: it writes nothing, so a
    // deadline that ends it discards it whole, and the cursor is free again.
    let read = within(deadline, async {
        let anchor = anchor_for(d, &state.history, end).await?;
        if let Some((b, h)) = anchor {
            pending.anchor(b, h);
        }
        // The trace endpoints must be on the main chain before the fetch as well as after it: one
        // that catches up in between would otherwise pass with the empty answers it gave from
        // another fork.
        same_chain_at(d, end).await?;
        let found = d
            .discover(&state.history, address, next, end, &pending)
            .await;
        let checked = match check_anchor(d, &state.history, anchor, &pending.named()).await {
            Ok(()) => same_chain_at(d, end).await,
            Err(e) => Err(e),
        };
        Ok((anchor, found, checked))
    })
    .await;
    state.set_calls("main", d.main.calls());
    state.set_calls("trace", d.trace.calls());
    let (anchor, found, checked) = read?;
    // What was fetched is kept even when the window fails, so its retry refetches only what it
    // still lacks; nothing is kept from a window that failed its check.
    let history = state.history.clone();
    let who = address.to_string();
    tokio::task::spawn_blocking(move || {
        pending.persist(&history, checked.is_ok())?;
        checked?;
        crate::address_discovery::record_anchored(
            &history,
            &who,
            (next, end),
            found?,
            generation,
            anchor,
        )
    })
    .await?
}

/// Read one window of bodies for `addresses` and record it for each, anchored as
/// [`discover_window`] is.
async fn body_window<M: Rpc, T: Rpc>(
    d: &Discoverer<Counted<M>, Counted<T>>,
    state: &ModeState,
    addresses: Vec<String>,
    (from, end): (u64, u64),
    deadline: Option<Instant>,
) -> Result<()> {
    use crate::address_discovery::record_blocks_anchored;
    let pending = crate::address_discovery::Pending::begin(&state.history)?;
    let generation = pending.generation();
    let read = within(deadline, async {
        let anchor = anchor_for(d, &state.history, end).await?;
        if let Some((b, h)) = anchor {
            pending.anchor(b, h);
        }
        let found = d
            .scan_with(&state.history, &addresses, from, end, &pending)
            .await;
        let checked = check_anchor(d, &state.history, anchor, &pending.named()).await;
        Ok((anchor, found, checked))
    })
    .await;
    state.set_calls("main", d.main.calls());
    state.set_calls("trace", d.trace.calls());
    let (anchor, found, checked) = read?;
    let history = state.history.clone();
    tokio::task::spawn_blocking(move || {
        pending.persist(&history, checked.is_ok())?;
        checked?;
        let mut found = found?;
        for a in addresses {
            // An address unwatched while the window was read is passed over, not the window.
            if !history.is_watched(&a)? {
                continue;
            }
            let f = found.remove(&a.to_ascii_lowercase()).unwrap_or_default();
            record_blocks_anchored(&history, &a, (from, end), f, generation, false, anchor)?;
        }
        Ok::<_, anyhow::Error>(())
    })
    .await?
}

/// A read, stopped at `deadline` if it has one. Only reads go through this: they write nothing, so
/// stopping one orphans no writer; what a window persists runs after it, to completion.
pub(crate) async fn within<F, X>(deadline: Option<Instant>, fetch: F) -> Result<X>
where
    F: std::future::Future<Output = Result<X>>,
{
    match deadline {
        None => fetch.await,
        Some(d) => tokio::time::timeout_at(d.into(), fetch)
            .await
            .unwrap_or_else(|_| {
                Err(anyhow::anyhow!(
                    "not covered within the deadline; try again shortly"
                ))
            }),
    }
}

/// The end of the uncovered run starting at `next` in `actions`' coverage of `a`, capped at `to`.
fn hole_end(
    history: &AddressHistory,
    a: &str,
    actions: &[crate::address_history::Action],
    next: u64,
    to: u64,
) -> Result<u64> {
    let mut end = to;
    for &action in actions {
        if let Some((f, _)) = history
            .coverage(action, a)?
            .into_iter()
            .find(|(f, _)| *f > next)
        {
            end = end.min(f - 1);
        }
    }
    Ok(end)
}

fn unix_now() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map_or(0, |d| d.as_secs())
}

impl<H: HeadSource> HeadSource for Arc<H> {
    async fn head(&self) -> Result<u64> {
        (**self).head().await
    }

    fn requests(&self) -> u64 {
        (**self).requests()
    }
}

/// The head as the discoverer's main endpoint reads it, so its calls are counted with the rest.
pub struct MainHead<M, T>(pub Arc<Discoverer<Counted<M>, Counted<T>>>);

impl<M: Rpc, T: Rpc> HeadSource for MainHead<M, T> {
    async fn head(&self) -> Result<u64> {
        self.0.main.head().await
    }

    fn requests(&self) -> u64 {
        self.0.main.requests() + self.0.trace.requests()
    }
}

impl<M: Rpc, T: Rpc> Discovery for Discoverer<Counted<M>, Counted<T>> {
    /// Backfill every watched address window by window up to `safe` (or `end_block`). A failed window
    /// records nothing and ends this pass for that address; the next poll resumes from coverage.
    /// Requests waiting meanwhile are served between windows, so none waits behind a backlog.
    async fn catch_up(&self, state: &ModeState, safe: u64) {
        let to = state.end_block.map_or(safe, |e| e.min(safe));
        let watched = match state.history.watched() {
            Ok(w) => w,
            Err(e) => {
                tracing::warn!("address history: reading the watched set: {e:#}");
                return;
            }
        };
        // Produced blocks first, on what is already covered: they are cheap, and a long backfill of
        // other actions would otherwise hold them back for whole polls.
        if state.chain_id == 1 {
            mev_passes(self, state, &watched, to).await;
        }
        for address in watched.clone() {
            let mut recorded_through: Option<u64> = None;
            loop {
                serve_queued(state, self).await;
                let next = match crate::address_discovery::next_uncovered(
                    &state.history,
                    &address,
                    state.start_block,
                ) {
                    Ok(n) => n,
                    Err(e) => {
                        tracing::warn!("address history: coverage for {address}: {e:#}");
                        break;
                    }
                };
                if next > to {
                    break;
                }
                // A recorded window must move coverage past it; one that did not would be fetched
                // again forever, at the provider's expense.
                if recorded_through.is_some_and(|r| next <= r) {
                    state.discovery_errors.fetch_add(1, Ordering::Relaxed);
                    tracing::error!(
                        "address history: coverage for {address} did not advance past {next}; stopping this pass"
                    );
                    break;
                }
                let end = next
                    .saturating_add(crate::address_discovery::OUTER_WINDOW - 1)
                    .min(to);
                match discover_window(self, state, &address, (next, end), None).await {
                    Ok(()) => {
                        recorded_through = Some(end);
                        state.windows.fetch_add(1, Ordering::Relaxed);
                        tracing::info!("address history: {address} covered through {end}");
                    }
                    Err(e) => {
                        state.discovery_errors.fetch_add(1, Ordering::Relaxed);
                        tracing::warn!(
                            "address history: [{next}, {end}] for {address} not recorded, retrying next poll: {e:#}"
                        );
                        break;
                    }
                }
            }
        }
        // Withdrawals and produced blocks come from bodies, and the definitions they are matched to
        // are Ethereum mainnet's, so only there is the block scan run.
        if state.chain_id == 1 {
            let skip = state.mirror.as_ref().map(|(_, blocks)| *blocks);
            if let Some((source, blocks)) = &state.mirror {
                mirror_pass(self, state, &watched, to, source, *blocks).await;
            }
            serve_queued(state, self).await;
            scan_bodies(self, state, &watched, to, skip).await;
            mev_passes(self, state, &watched, to).await;
        } else if let Some((source, blocks)) = &state.mirror {
            // Elsewhere a partition is read for its verified headers alone: timestamp to block.
            mirror_pass(self, state, &[], to, source, *blocks).await;
        }
    }

    async fn reconcile(&self, state: &ModeState, latest: u64) -> Result<u64> {
        let by_depth = latest.saturating_sub(state.depth);
        // The RPC's finalized tag where it has one; the registry's depth under the head otherwise.
        let finalized = match crate::address_partitions::finalized(&self.main).await {
            Ok(f) if f <= latest => f,
            _ => by_depth,
        };
        // The new finalized hash is read on both sides of the checks: one read on a fork the checks
        // never saw would otherwise become the record every later poll trusts.
        let hash = agreed_hash(&self.main, finalized).await;
        // The recorded finalized hash is held again after the tracked check, whatever the tag says
        // now: a tag that went backwards would otherwise bracket only blocks under it.
        let checked = match self.check_tracked(state, latest).await {
            Ok(()) => hold_final(&self.main, &state.history).await,
            Err(e) => Err(e),
        };
        let again = agreed_hash(&self.main, finalized).await;
        state.set_calls("main", self.main.calls());
        checked?;
        let hash = hash?;
        if again? != hash {
            bail!("finalized block {finalized} changed hash during the poll; trying again");
        }
        let h = state.history.clone();
        let done = tokio::task::spawn_blocking(move || h.finalize_at(finalized, hash)).await?;
        if let Err(e) = done {
            if let Some(c) = e.downcast_ref::<crate::address_history::FinalConflict>() {
                state.conflict(format!(
                    "{c}: a reorg under finality, or an RPC on another chain with the same id; \
                     history under the finalized head is never rewritten, so the nest answers \
                     nothing until an operator looks"
                ));
            }
            return Err(e);
        }
        Ok(finalized)
    }

    async fn serve(&self, state: &ModeState, request: Request) {
        match request {
            Request::Cover {
                address,
                need,
                from,
                to,
                deadline,
                reply,
            } => {
                let done = self
                    .cover(state, &address, need, (from, to), deadline)
                    .await;
                state.set_calls("main", self.main.calls());
                state.set_calls("trace", self.trace.calls());
                let _ = reply.send(done.map_err(|e| format!("{e:#}")));
            }
            Request::Trace {
                hash,
                deadline,
                reply,
            } => {
                let done = self.trace_one(state, &hash, deadline).await;
                state.set_calls("main", self.main.calls());
                state.set_calls("trace", self.trace.calls());
                let _ = reply.send(done.map_err(|e| format!("{e:#}")));
            }
        }
    }
}

impl<M: Rpc, T: Rpc> Discoverer<Counted<M>, Counted<T>> {
    /// Check the tracked hashes against the chain under `latest`, and take back everything above the
    /// newest one that still matches. A tracked block the chain no longer reaches (the head fell under
    /// it) cannot be shown to stand, so it counts as moved. Every tracked block lies above the
    /// finalized head this store committed, so it was mutable when it was read, however far the chain
    /// has finalized since; only a block under that committed head is a conflict, which stops the
    /// cursor.
    async fn check_tracked(&self, state: &ModeState, latest: u64) -> Result<()> {
        self.check_tracked_within(state, latest, None).await
    }

    /// [`Self::check_tracked`] with its RPC reads under `deadline`; taking back what a reorg replaced,
    /// once found, always runs to completion.
    async fn check_tracked_within(
        &self,
        state: &ModeState,
        latest: u64,
        deadline: Option<Instant>,
    ) -> Result<()> {
        let history = state.history.clone();
        let (tracked, committed) = {
            let h = history.clone();
            tokio::task::spawn_blocking(move || Ok::<_, anyhow::Error>((h.tracked()?, h.head()?)))
                .await??
        };
        let committed = committed.unwrap_or(0);
        self.check_finals(state, deadline).await?;
        let found = within(deadline, async {
            let mut moved: Option<u64> = tracked.iter().map(|(b, _)| *b).find(|b| *b > latest);
            let mut base = committed;
            for &(b, h) in tracked.iter().rev().filter(|(b, _)| *b <= latest) {
                if block_hash(&self.main, b).await? == h {
                    base = b;
                    break;
                }
                moved = Some(b);
            }
            Ok((moved, base))
        })
        .await;
        state.set_calls("main", self.main.calls());
        let (moved, base) = found?;
        let Some(lowest_moved) = moved else {
            return Ok(());
        };
        if lowest_moved <= committed {
            let why = format!(
                "block {lowest_moved}, under the finalized head {committed}, is now at another hash; \
                 history under the finalized head is never rewritten, so this needs an operator"
            );
            state.conflict(why.clone());
            bail!(why);
        }
        tracing::warn!(
            "address history: a reorg replaced blocks above {base}; taking back what was read there"
        );
        tokio::task::spawn_blocking(move || history.invalidate_above(base)).await??;
        Ok(())
    }

    /// Hold the newest recorded finalized hash, and one older one in turn, against the chain. A
    /// mismatch is history under the finalized head that changed, a reorg under finality or an RPC
    /// that came back on another chain with the same id, and nothing is answered while it stands.
    async fn check_finals(&self, state: &ModeState, deadline: Option<Instant>) -> Result<()> {
        let finals = state.history.finals()?;
        let Some(&newest) = finals.last() else {
            // A conflict it finds is in the store, where every answer looks for one.
            let held = within(deadline, hold_legacy(&self.main, &state.history)).await;
            state.set_calls("main", self.main.calls());
            return held;
        };
        let pick = state.sample.fetch_add(1, Ordering::Relaxed) as usize % finals.len();
        let mut checks = vec![newest];
        if finals[pick] != newest {
            checks.push(finals[pick]);
        }
        let moved = within(deadline, async {
            for (b, h) in checks {
                if block_hash(&self.main, b).await? != h {
                    return Ok(Some(b));
                }
            }
            Ok(None)
        })
        .await;
        state.set_calls("main", self.main.calls());
        if let Some(b) = moved? {
            let why = final_conflict(b);
            state.conflict(why.clone());
            bail!(why);
        }
        Ok(())
    }

    /// Cover `[from, to]` of `address` for `need`, holes included, a window at a time, within
    /// `deadline`. Never past the chain head, the configured range, or the chain's traceable start.
    /// A reorg found first is taken back before anything above the finalized head is served or
    /// extended.
    async fn cover(
        &self,
        state: &ModeState,
        address: &str,
        need: Need,
        (from, to): (u64, u64),
        deadline: Instant,
    ) -> Result<()> {
        if from < state.start_block {
            bail!("history starts at block {}", state.start_block);
        }
        if let Some(e) = state.end_block.filter(|e| to > *e) {
            bail!("history ends at block {e}");
        }
        if need != Need::Discovered && state.chain_id != 1 {
            bail!("withdrawals and produced blocks are served on Ethereum mainnet only");
        }
        if need == Need::Produced && state.relays.is_none() {
            bail!("produced blocks need mev_relays = true");
        }
        if need == Need::Produced {
            let head = state.history.head()?.unwrap_or(0);
            if to > head {
                bail!("produced blocks are served through the finalized block {head}, not {to}");
            }
        }
        use crate::address_discovery::{BLOCK_SCANNED, DISCOVERED};
        use crate::address_history::Action;
        // The deadline is checked between windows, never by cancelling one: a window's write runs
        // on a blocking thread a cancellation would orphan, and the cursor must stay the only writer.
        let late = || {
            if Instant::now() >= deadline {
                bail!("not covered within the deadline; try again shortly");
            }
            Ok(())
        };
        let latest = within(Some(deadline), self.main.head()).await?;
        state.latest.store(latest, Ordering::Relaxed);
        if to > latest {
            bail!("block {to} is past the chain head {latest}");
        }
        self.check_tracked_within(state, latest, Some(deadline))
            .await?;
        let history = &state.history;
        loop {
            let next = crate::address_discovery::next_uncovered(history, address, from)?;
            if next > to {
                break;
            }
            late()?;
            let end = hole_end(history, address, &DISCOVERED, next, to)?
                .min(next.saturating_add(COVER_WINDOW - 1));
            discover_window(self, state, address, (next, end), Some(deadline)).await?;
        }
        if need == Need::Discovered {
            history.raise_tip(to)?;
            return Ok(());
        }
        loop {
            let next = crate::address_discovery::next_uncovered_for(
                history,
                address,
                from,
                &BLOCK_SCANNED,
            )?;
            if next > to {
                break;
            }
            late()?;
            if let Some((first, last)) = state.mirror.as_ref().map(|(_, b)| *b) {
                if first <= next && next <= last {
                    bail!("blocks {first} to {last} are read from the mirror's partitions, which have not reached block {next} yet");
                }
            }
            let mut end = hole_end(history, address, &BLOCK_SCANNED, next, to)?
                .min(next.saturating_add(COVER_BODIES - 1));
            if let Some((first, _)) = state
                .mirror
                .as_ref()
                .map(|(_, b)| *b)
                .filter(|(f, _)| next < *f)
            {
                end = end.min(first - 1);
            }
            body_window(
                self,
                state,
                vec![address.to_string()],
                (next, end),
                Some(deadline),
            )
            .await?;
        }
        if need == Need::Produced {
            let relays = state.relays.as_ref().expect("checked above");
            loop {
                let next = crate::address_discovery::next_uncovered_for(
                    history,
                    address,
                    from,
                    &[Action::ProducedBlocks],
                )?;
                if next > to {
                    break;
                }
                late()?;
                let end = hole_end(history, address, &[Action::ProducedBlocks], next, to)?
                    .min(next.saturating_add(COVER_PRODUCED - 1));
                crate::address_mev::produced_window(
                    self,
                    relays,
                    history,
                    address,
                    (next, end),
                    unix_now(),
                    Some(deadline),
                )
                .await?;
            }
            let watched = history.watched()?;
            crate::address_mev::reask_range(
                self,
                relays,
                history,
                &watched,
                (from, to),
                unix_now(),
                Some(deadline),
            )
            .await?;
        }
        history.raise_tip(to)?;
        Ok(())
    }

    /// Trace one transaction for a `txhash` lookup, up to the chain head, and keep it. Its reads run
    /// under `deadline`; the write after them runs to completion.
    async fn trace_one(&self, state: &ModeState, hash: &str, deadline: Instant) -> Result<()> {
        let latest = within(Some(deadline), self.main.head()).await?;
        state.latest.store(latest, Ordering::Relaxed);
        self.check_tracked_within(state, latest, Some(deadline))
            .await?;
        let pending = crate::address_discovery::Pending::begin(&state.history)?;
        within(Some(deadline), async {
            self.internal_rows_of(&state.history, hash, &pending, latest)
                .await?;
            // The trace endpoint names the block it traced: it must be the main endpoint's, at any
            // height, and so must the tracked block under it.
            check_anchor(self, &state.history, None, &pending.named()).await
        })
        .await?;
        let history = state.history.clone();
        tokio::task::spawn_blocking(move || pending.persist(&history, true)).await?
    }
}

/// The MEV passes: each watched address's produced blocks as far as their prerequisites are covered,
/// up to `to`, then the relay re-asks that are due; requests are served between windows.
async fn mev_passes<M: Rpc, T: Rpc>(
    d: &Discoverer<Counted<M>, Counted<T>>,
    state: &ModeState,
    watched: &[String],
    to: u64,
) {
    let Some(relays) = &state.relays else {
        return;
    };
    let now = unix_now();
    let passes = async {
        for a in watched {
            loop {
                serve_queued(state, d).await;
                let more = crate::address_mev::mev_step(
                    d,
                    relays,
                    &state.history,
                    a,
                    state.start_block,
                    to,
                    now,
                )
                .await?;
                if !more {
                    break;
                }
                state.windows.fetch_add(1, Ordering::Relaxed);
            }
            if let Some(&(_, t)) = state
                .history
                .coverage(crate::address_history::Action::ProducedBlocks, a)?
                .first()
            {
                tracing::info!("address history: {a} produced blocks covered through {t}");
            }
        }
        loop {
            serve_queued(state, d).await;
            let more =
                crate::address_mev::reask_pass(d, relays, &state.history, watched, now).await?;
            if !more {
                break;
            }
        }
        Ok::<_, anyhow::Error>(())
    };
    if let Err(e) = passes.await {
        state.discovery_errors.fetch_add(1, Ordering::Relaxed);
        tracing::warn!("address history: MEV pass not recorded, retrying next poll: {e:#}");
    }
    state.set_calls("main", d.main.calls());
}

/// The partition pass: each partition in the mirror's blocks that some watched address, or the
/// verified headers, still lack is downloaded, verified against the RPC and read for all of them at
/// once. A partition that is missing or fails verification records nothing, and its blocks stay
/// incomplete: the RPC body scan does not stand in for it.
async fn mirror_pass<M: Rpc, T: Rpc>(
    d: &Discoverer<Counted<M>, Counted<T>>,
    state: &ModeState,
    watched: &[String],
    to: u64,
    source: &crate::address_partitions::Source,
    (first, last): (u64, u64),
) {
    use crate::address_discovery::{record_blocks, BLOCK_SCANNED};
    use crate::address_partitions::{finalized, ingest, span_of, SPAN};
    // Coverage the RPC scan recorded before the mirror was configured does not count here.
    let history = state.history.clone();
    let kept = tokio::task::spawn_blocking(move || {
        history.keep_verified_only(&BLOCK_SCANNED, (first, last))
    })
    .await;
    if let Err(e) = kept.map_err(anyhow::Error::from).and_then(|r| r) {
        tracing::warn!("address history: setting the mirror's blocks aside: {e:#}");
        return;
    }
    let (lo, hi) = (state.start_block.max(first), to.min(last));
    if lo > hi {
        return;
    }
    let mut ceiling: Option<u64> = None;
    let mut start = span_of(lo).0;
    while start <= hi {
        serve_queued(state, d).await;
        let end = start + SPAN - 1;
        let clip = (start.max(lo), end.min(hi));
        let verified = |a: &String| {
            BLOCK_SCANNED.iter().all(|&action| {
                state
                    .history
                    .verified_coverage(action, a)
                    .is_ok_and(|spans| spans.iter().any(|(f, t)| *f <= clip.0 && clip.1 <= *t))
            })
        };
        let lacking: Vec<String> = watched.iter().filter(|a| !verified(a)).cloned().collect();
        let headers = state
            .history
            .header_coverage()
            .is_ok_and(|spans| spans.iter().any(|(f, t)| *f <= clip.0 && clip.1 <= *t));
        if lacking.is_empty() && headers {
            start += SPAN;
            continue;
        }
        // A partition is anchored at its last block, which the RPC must call finalized: the
        // nest's serving depth is a guess at finality, not finality.
        if ceiling.is_none() {
            match finalized(&d.main).await {
                Ok(f) => ceiling = Some(f),
                Err(e) => {
                    state.discovery_errors.fetch_add(1, Ordering::Relaxed);
                    tracing::warn!("address history: the RPC's finalized block: {e:#}");
                    return;
                }
            }
        }
        if ceiling.is_some_and(|f| end > f) {
            return;
        }
        let outcome = async {
            let generation = state.history.generation()?;
            let mut got = ingest(&d.main, source, start, &lacking, clip).await?;
            let history = state.history.clone();
            tokio::task::spawn_blocking(move || {
                history.record_headers(clip, &got.timestamps)?;
                for a in lacking {
                    let f = got
                        .found
                        .remove(&a.to_ascii_lowercase())
                        .unwrap_or_default();
                    record_blocks(&history, &a, clip, f, generation, true)?;
                }
                Ok::<_, anyhow::Error>(())
            })
            .await?
        }
        .await;
        state.set_calls("main", d.main.calls());
        match outcome {
            Ok(()) => {
                state.windows.fetch_add(1, Ordering::Relaxed);
                tracing::info!("address history: partition [{start}, {end}] verified and read");
            }
            Err(e) => {
                state.discovery_errors.fetch_add(1, Ordering::Relaxed);
                tracing::warn!(
                    "address history: partition [{start}, {end}] not used, its blocks stay incomplete: {e:#}"
                );
            }
        }
        start += SPAN;
    }
}

/// The block-body pass: each window is read once for every watched address that still lacks it,
/// starting from the least covered, so an address watched later backfills without a second scan
/// of what the others already have.
/// Blocks in `skip` are the mirror's, and are never read here.
async fn scan_bodies<M: Rpc, T: Rpc>(
    d: &Discoverer<Counted<M>, Counted<T>>,
    state: &ModeState,
    watched: &[String],
    to: u64,
    skip: Option<(u64, u64)>,
) {
    use crate::address_discovery::{next_uncovered_for, BLOCK_SCANNED, BLOCK_WINDOW};
    let mut last: Option<u64> = None;
    let mut cursor = state.start_block;
    loop {
        serve_queued(state, d).await;
        let mut next = Vec::new();
        for a in watched {
            match next_uncovered_for(&state.history, a, cursor, &BLOCK_SCANNED) {
                Ok(n) => next.push((a.clone(), n)),
                Err(e) => {
                    tracing::warn!("address history: block coverage for {a}: {e:#}");
                    return;
                }
            }
        }
        let Some(from) = next.iter().map(|(_, n)| *n).min().filter(|n| *n <= to) else {
            return;
        };
        if let Some((_, mirror_end)) = skip.filter(|(f, l)| *f <= from && from <= *l) {
            cursor = mirror_end.saturating_add(1);
            continue;
        }
        if last.is_some_and(|l| from <= l) {
            state.discovery_errors.fetch_add(1, Ordering::Relaxed);
            tracing::error!(
                "address history: block coverage did not advance past {from}; stopping this pass"
            );
            return;
        }
        let mut end = from.saturating_add(BLOCK_WINDOW - 1).min(to);
        if let Some((first, _)) = skip.filter(|(f, _)| from < *f) {
            end = end.min(first - 1);
        }
        // The scan reads the whole window for every address that lacks any of it, so each is
        // recorded over the whole window.
        let addresses: Vec<String> = next
            .into_iter()
            .filter(|(_, n)| *n <= end)
            .map(|(a, _)| a)
            .collect();
        let outcome = body_window(d, state, addresses, (from, end), None).await;
        match outcome {
            Ok(()) => {
                last = Some(end);
                state.windows.fetch_add(1, Ordering::Relaxed);
                tracing::info!("address history: block bodies read through {end}");
            }
            Err(e) => {
                state.discovery_errors.fetch_add(1, Ordering::Relaxed);
                tracing::warn!(
                    "address history: bodies [{from}, {end}] not recorded, retrying next poll: {e:#}"
                );
                return;
            }
        }
    }
}

/// One poll: read the chain head. Fails on a failed read; the finalized head is moved by
/// [`Discovery::reconcile`], after the tracked hashes are checked.
pub async fn poll_once(state: &ModeState, source: &impl HeadSource) -> Result<u64> {
    state.polls.fetch_add(1, Ordering::Relaxed);
    let read = source.head().await;
    state
        .rpc_requests
        .store(source.requests(), Ordering::Relaxed);
    match read {
        Ok(head) => {
            state.latest.store(head, Ordering::Relaxed);
            let ms = u64::try_from(state.started.elapsed().as_millis()).unwrap_or(u64::MAX - 1);
            state.last_poll.store(ms + 1, Ordering::Relaxed);
            Ok(head)
        }
        Err(e) => {
            state.poll_errors.fetch_add(1, Ordering::Relaxed);
            Err(e)
        }
    }
}

/// A poll and its reconcile: the finalized head the cursor may backfill to.
pub async fn step(
    state: &ModeState,
    source: &impl HeadSource,
    discovery: &impl Discovery,
) -> Result<u64> {
    let stepped = match poll_once(state, source).await {
        Ok(latest) => discovery.reconcile(state, latest).await.inspect_err(|_| {
            state.poll_errors.fetch_add(1, Ordering::Relaxed);
        }),
        Err(e) => Err(e),
    };
    state.failing.store(stepped.is_err(), Ordering::Relaxed);
    stepped
}

/// Poll forever, the first poll at once, and catch up after each. A failed poll is logged and
/// retried at the next tick: covered history keeps serving while the RPC is down. A watch wakes it
/// early, and a request is served as soon as it arrives.
pub async fn cursor(state: Arc<ModeState>, source: impl HeadSource, discovery: impl Discovery) {
    let mut tick = tokio::time::interval(state.poll_interval);
    tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
    loop {
        if state.conflicted().is_some() {
            tracing::error!(
                "address history: the cursor stopped over a conflict with finalized history"
            );
            return;
        }
        let request = {
            let mut q = state.queue.lock().await;
            tokio::select! {
                _ = tick.tick() => None,
                () = state.wake.notified() => None,
                r = q.recv() => r,
            }
        };
        if let Some(r) = request {
            discovery.serve(&state, r).await;
            serve_queued(&state, &discovery).await;
            continue;
        }
        match step(&state, &source, &discovery).await {
            Ok(safe) => discovery.catch_up(&state, safe).await,
            Err(e) => tracing::warn!("address history: poll failed, retrying next interval: {e:#}"),
        }
    }
}

/// A panicking cursor must take /ready down with it, not leave it reading ready over stale history.
fn supervise(
    state: Arc<ModeState>,
    work: tokio::task::JoinHandle<()>,
) -> tokio::task::JoinHandle<()> {
    tokio::spawn(async move {
        match work.await {
            Err(e) if e.is_panic() => state.stop(format!("the cursor task panicked: {e}")),
            _ => {}
        }
    })
}

pub fn router(state: Arc<ModeState>) -> Router {
    Router::new()
        .route("/api", get(api))
        .route("/api/watch", get(watched).post(watch))
        .route("/api/unwatch", post(unwatch))
        .route("/health", get(|| async { "ok" }))
        .route("/ready", get(ready))
        .route("/metrics", get(metrics))
        .with_state(state)
}

/// Run a store read off the async workers.
async fn blocking<T: Send + 'static>(
    s: &Arc<ModeState>,
    f: impl FnOnce(&AddressHistory) -> Result<T> + Send + 'static,
) -> Result<T> {
    let history = s.history.clone();
    tokio::task::spawn_blocking(move || f(&history)).await?
}

fn failed(status: StatusCode, e: impl std::fmt::Display) -> (StatusCode, Json<Value>) {
    (status, Json(json!({ "error": e.to_string() })))
}

async fn api(
    State(s): State<Arc<ModeState>>,
    Query(q): Query<HashMap<String, String>>,
) -> Json<Value> {
    let conflict = |s: &ModeState| s.conflicted();
    if let Some(why) = conflict(&s) {
        return Json(crate::address_history::incomplete(&why));
    }
    let answer = answer(&s, q).await;
    // A conflict found while this request was answered may concern what it read.
    match conflict(&s) {
        Some(why) => Json(crate::address_history::incomplete(&why)),
        None => answer,
    }
}

async fn answer(s: &Arc<ModeState>, q: HashMap<String, String>) -> Json<Value> {
    let s = s.clone();
    // The RPC behind the nest is the operator's; only a loopback bind lends it out.
    if let Some(forward) = s.forward.clone().filter(|_| s.loopback) {
        let _permit = if q.get("module").map(String::as_str) == Some("logs") {
            match s.logs.try_acquire() {
                Ok(p) => Some(p),
                Err(_) => {
                    return Json(crate::address_history::incomplete(
                        "the nest is answering other getLogs requests; try again shortly",
                    ))
                }
            }
        } else {
            None
        };
        if let Some(v) = crate::address_proxy::answer(&q, s.chain_id, &forward).await {
            return Json(v);
        }
    }
    let get = |k: &str| q.get(k).map(String::as_str);
    if get("module") == Some("block") && get("action") == Some("getblocknobytime") {
        return Json(block_by_time(&s, &q).await);
    }
    // A txhash lookup needs no coverage: a transaction the store has not traced is traced now, by
    // the cursor, once the request is one `respond` would answer.
    let traceable = get("module") == Some("account")
        && get("action") == Some("txlistinternal")
        && get("chainid").is_none_or(|c| c.parse::<u64>().ok() == Some(s.chain_id));
    let key = get("txhash")
        .and_then(|h| h.strip_prefix("0x"))
        .and_then(|h| alloy_primitives::hex::decode(h).ok())
        .filter(|k| k.len() == 32);
    if let (true, Some(key), Some(hash)) = (traceable, key, q.get("txhash").cloned()) {
        // A trace not kept yet is traced; one kept from above the finalized head is checked against
        // the chain first, since a reorg since the last poll may have taken its block.
        let fresh = blocking(&s, move |h| {
            Ok(match h.tx_internals_block(&key)? {
                None => true,
                Some((b, named)) => named.is_none() || h.head()?.is_none_or(|f| b > f),
            })
        })
        .await;
        if matches!(fresh, Ok(true)) {
            if let Err(e) = s
                .ask(|deadline, reply| Request::Trace {
                    hash,
                    deadline,
                    reply,
                })
                .await
            {
                return Json(crate::address_history::incomplete(&e));
            }
        }
    }
    // An answer is served only under the generation it was read in: a reorg taken back between the
    // read and the checks below would otherwise let the replaced rows out.
    let mut answer = respond(&s, &q).await;
    let mut want = None;
    for _ in 0..3 {
        match wanted(&s, &q, &answer).await {
            Want::Serve => return Json(answer),
            Want::Cover(w) => {
                want = Some(w);
                break;
            }
            Want::Reread => answer = respond(&s, &q).await,
        }
    }
    let Some(want) = want else {
        return Json(crate::address_history::incomplete(
            "the history changed while it was being read; ask again",
        ));
    };
    match s
        .ask(|deadline, reply| Request::Cover {
            address: want.0,
            need: want.1,
            from: want.2,
            to: want.3,
            deadline,
            reply,
        })
        .await
    {
        Ok(()) => Json(respond(&s, &q).await),
        Err(e) => {
            let why = answer["result"]
                .as_str()
                .unwrap_or("")
                .trim_start_matches("NUTHATCH_INCOMPLETE: ")
                .to_string();
            Json(crate::address_history::incomplete(&if why.is_empty() {
                e
            } else {
                format!("{why}; {e}")
            }))
        }
    }
}

async fn respond(s: &Arc<ModeState>, q: &HashMap<String, String>) -> Value {
    let q = q.clone();
    blocking(s, move |h| Ok(crate::address_history::respond(Some(h), &q)))
        .await
        .unwrap_or_else(
            |e| json!({ "status": "0", "message": "NOTOK", "result": format!("Error! {e:#}") }),
        )
}

/// The coverage an account answer needs the cursor for, when the cursor could supply it: a watched
/// address and a start the configured range allows, and either an answer that is incomplete for an
/// explicit `endblock`, or one reaching above the finalized head, which is checked against the chain
/// before it is served.
/// What to do with an account answer once read.
enum Want {
    /// Serve it as it is.
    Serve,
    /// Have the cursor cover (and check) this, then read again.
    Cover((String, Need, u64, u64)),
    /// The history changed since it was read: read again.
    Reread,
}

async fn wanted(s: &Arc<ModeState>, q: &HashMap<String, String>, answer: &Value) -> Want {
    use crate::address_history::Action;
    let rows = answer["status"] == "1";
    let read_at = answer["generation"]
        .as_str()
        .and_then(|g| g.parse::<u64>().ok());
    let decided = async {
        if q.get("module").map(String::as_str) != Some("account") || q.contains_key("txhash") {
            return None;
        }
        let incomplete = answer["result"]
            .as_str()
            .is_some_and(|r| r.starts_with("NUTHATCH_INCOMPLETE:"));
        if !incomplete && !rows {
            return None;
        }
        let action = Action::from_etherscan(q.get("action")?)?;
        let address = crate::address_history::parse_address(q.get("address")?).ok()?;
        let address = format!("0x{}", alloy_primitives::hex::encode(address));
        let from = q
            .get("startblock")
            .map_or(Some(0), |f| f.parse::<u64>().ok())?
            .max(s.start_block);
        let explicit = match q.get("endblock") {
            Some(e) => Some(e.parse::<u64>().ok()?),
            None => None,
        };
        let who = address.clone();
        // One pass over the store, the generation read last: a re-ask or reorg committed while the
        // rest is read moves it, and the answer is read again.
        let (watched, head, covered, owed, generation) = blocking(s, move |h| {
            let covered = h
                .coverage(action, &who)?
                .into_iter()
                .find(|(f, t)| *f <= from && from <= *t)
                .map(|(_, t)| t);
            // A produced-blocks row whose relay check is owed is not settled.
            let owed = match explicit.or(covered) {
                Some(end) if action == Action::ProducedBlocks => {
                    (!crate::address_mev::overdue(h, unix_now(), Some((from, end)))?.is_empty())
                        .then_some(end)
                }
                _ => None,
            };
            Ok((
                h.watched()?.contains(&who),
                h.head()?.unwrap_or(0),
                covered,
                owed,
                h.generation()?,
            ))
        })
        .await
        .ok()?;
        if rows && read_at != Some(generation) {
            return Some(Want::Reread);
        }
        if let (true, true, Some(end)) = (rows, watched, owed) {
            return Some(Want::Cover((address, Need::of(action), from, end)));
        }
        if !watched {
            return None;
        }
        let to = match (explicit, incomplete) {
            (Some(e), _) => e,
            // Without an end, an incomplete answer is a start nobody has covered; the backfill gets
            // to it.
            (None, true) => return None,
            (None, false) => covered?,
        };
        (to >= from && (incomplete || to > head)).then_some(Want::Cover((
            address,
            Need::of(action),
            from,
            to,
        )))
    };
    decided.await.unwrap_or(Want::Serve)
}

/// `block/getblocknobytime`: from verified headers where they bracket the timestamp, else searched
/// on the nest's own RPC, exact or refused.
async fn block_by_time(s: &Arc<ModeState>, q: &HashMap<String, String>) -> Value {
    let first = respond(s, q).await;
    let unbracketed = first["result"]
        .as_str()
        .is_some_and(|r| r.starts_with("NUTHATCH_INCOMPLETE:"));
    let (Some(forward), true) = (s.forward.clone(), unbracketed) else {
        return first;
    };
    let t = q
        .get("timestamp")
        .and_then(|t| t.parse::<u64>().ok())
        .expect("respond validated the timestamp");
    let after = q.get("closest").map(String::as_str) == Some("after");
    let committed = s.history.head().ok().flatten();
    let search = async {
        match search_by_time(&forward, t, after).await? {
            Ok((b, latest, proof)) => {
                // Above the finalized head an answer read from one node may be on a fork another has
                // left: it stands only if parent hashes link it to the head the search began from.
                if committed.is_none_or(|c| b + 1 > c)
                    && !descends(&forward, b, latest, &proof).await?
                {
                    return Ok(Err(format!(
                        "block {b} does not link to the chain head the search began from; ask again"
                    )));
                }
                Ok(Ok(b))
            }
            Err(why) => Ok::<_, anyhow::Error>(Err(why)),
        }
    };
    match tokio::time::timeout(s.deadline, search).await {
        Ok(Ok(Ok(b))) => crate::address_history::ok(
            Value::String(b.to_string()),
            s.history.generation().unwrap_or(0),
        ),
        Ok(Ok(Err(why))) => crate::address_history::incomplete(&why),
        Ok(Err(e)) => crate::address_history::incomplete(&format!("searching the RPC: {e:#}")),
        Err(_) => crate::address_history::incomplete(&format!(
            "the RPC search did not finish within the {}s deadline",
            s.deadline.as_secs()
        )),
    }
}

/// One block's number, timestamp, hash and parent hash.
struct Header {
    number: u64,
    ts: u64,
    hash: String,
    parent: String,
}

async fn header(forward: &crate::address_proxy::Forward, at: Value) -> Result<Header> {
    let b = forward("eth_getBlockByNumber".into(), json!([at, false])).await?;
    let field = |k: &str| {
        b.get(k)
            .and_then(Value::as_str)
            .map(str::to_ascii_lowercase)
            .ok_or_else(|| anyhow::anyhow!("block {at} has no {k}"))
    };
    Ok(Header {
        number: crate::address_discovery::hex_u64(&b["number"])?,
        ts: crate::address_discovery::hex_u64(&b["timestamp"])?,
        hash: field("hash")?,
        parent: field("parentHash")?,
    })
}

/// Etherscan's `getblocknobytime` on the RPC: the greatest block with timestamp <= `t`, or the least
/// with timestamp >= `t`. The answer is proven against its neighbour, linked by parent hash, so a
/// search that straddled a reorg is refused rather than answered. The inner `Err` says why it was
/// refused. Only on a chain whose timestamps rise strictly from block to block.
/// The most headers [`descends`] reads to link an answer to the head.
const MAX_LINK: u64 = 512;

/// Whether block `b` and the block after it are on the chain ending at `latest`: every header from
/// the head down to `b` names the one before it as its parent. A chain of hashes proves ancestry
/// whichever node served each header.
async fn descends(
    forward: &crate::address_proxy::Forward,
    b: u64,
    latest: Header,
    proof: &[(u64, String)],
) -> Result<bool> {
    if latest.number < b {
        return Ok(false);
    }
    if latest.number - b > MAX_LINK {
        bail!(
            "block {b} is {} blocks under the head, too far to link",
            latest.number - b
        );
    }
    // The headers the search proved its answer with must be the ones on the head's chain, or the
    // walk could pass on headers a node served after moving to another fork.
    if proof
        .iter()
        .any(|(n, h)| *n == latest.number && *h != latest.hash)
    {
        return Ok(false);
    }
    let mut parent = latest.parent;
    for n in (b..latest.number).rev() {
        let h = header(forward, json!(format!("0x{n:x}"))).await?;
        if h.hash != parent || proof.iter().any(|(m, ph)| *m == n && *ph != h.hash) {
            return Ok(false);
        }
        parent = h.parent;
    }
    Ok(true)
}

/// The answer, the head the search began from, and the headers that proved the answer.
type Found = (u64, Header, Vec<(u64, String)>);

async fn search_by_time(
    forward: &crate::address_proxy::Forward,
    t: u64,
    after: bool,
) -> Result<std::result::Result<Found, String>> {
    let at = |n: u64| json!(format!("0x{n:x}"));
    let latest = header(forward, json!("latest")).await?;
    if t > latest.ts {
        return Ok(Err(format!(
            "timestamp {t} is newer than the chain head {} ({}); a block at or before it may still come",
            latest.number, latest.ts
        )));
    }
    let genesis = header(forward, at(0)).await?;
    if t < genesis.ts {
        return Ok(if after {
            Ok((0, latest, vec![(0, genesis.hash)]))
        } else {
            Err(format!("timestamp {t} is before the first block"))
        });
    }
    let (mut lo, mut hi) = (0, latest.number);
    while lo < hi {
        if after {
            let mid = lo + (hi - lo) / 2;
            if header(forward, at(mid)).await?.ts >= t {
                hi = mid;
            } else {
                lo = mid + 1;
            }
        } else {
            let mid = lo + (hi - lo).div_ceil(2);
            if header(forward, at(mid)).await?.ts <= t {
                lo = mid;
            } else {
                hi = mid - 1;
            }
        }
    }
    let b = header(forward, at(lo)).await?;
    let mut proof = vec![(lo, b.hash.clone())];
    let proven = if after {
        b.ts >= t
            && (lo == 0 || {
                let prev = header(forward, at(lo - 1)).await?;
                proof.push((lo - 1, prev.hash.clone()));
                prev.ts < t && b.parent == prev.hash
            })
    } else {
        b.ts <= t
            && if lo == latest.number {
                // Timestamps rise strictly, so the next block's will be past `t == b.ts`.
                b.ts == t
            } else {
                let next = header(forward, at(lo + 1)).await?;
                proof.push((lo + 1, next.hash.clone()));
                next.ts > t && next.parent == b.hash
            }
    };
    if !proven {
        bail!("the chain moved during the search; ask again");
    }
    Ok(Ok((lo, latest, proof)))
}

#[derive(serde::Deserialize)]
struct AddressBody {
    address: String,
}

async fn watched(State(s): State<Arc<ModeState>>) -> (StatusCode, Json<Value>) {
    match blocking(&s, |h| h.watched()).await {
        Ok(w) => (StatusCode::OK, Json(json!({ "watched": w }))),
        Err(e) => failed(StatusCode::INTERNAL_SERVER_ERROR, format!("{e:#}")),
    }
}

async fn watch(
    State(s): State<Arc<ModeState>>,
    Json(b): Json<AddressBody>,
) -> (StatusCode, Json<Value>) {
    change(s, b.address, true).await
}

async fn unwatch(
    State(s): State<Arc<ModeState>>,
    Json(b): Json<AddressBody>,
) -> (StatusCode, Json<Value>) {
    change(s, b.address, false).await
}

async fn change(s: Arc<ModeState>, address: String, add: bool) -> (StatusCode, Json<Value>) {
    if !s.loopback {
        return failed(
            StatusCode::FORBIDDEN,
            "the watched set changes only on a loopback bind",
        );
    }
    if let Err(e) = crate::address_history::parse_address(&address) {
        return failed(StatusCode::BAD_REQUEST, format!("address `{address}`: {e}"));
    }
    let done = blocking(&s, move |h| {
        if add {
            h.watch(&address)?;
        } else {
            h.unwatch(&address)?;
        }
        h.watched()
    })
    .await;
    match done {
        Ok(w) => {
            if add {
                s.wake.notify_one();
            }
            (StatusCode::OK, Json(json!({ "watched": w })))
        }
        Err(e) => failed(StatusCode::INTERNAL_SERVER_ERROR, format!("{e:#}")),
    }
}

async fn ready(State(s): State<Arc<ModeState>>) -> (StatusCode, Json<Value>) {
    // Offline is not unready: covered history still serves, and a cursor that has not polled for
    // three intervals is reported stalled. A store that cannot be read is unready, and so is a
    // cursor stopped for good: covered history still serves, but nothing will ever extend it.
    let stopped = s
        .conflicted()
        .or_else(|| s.cursor_error.lock().expect("cursor_error lock").clone());
    match blocking(&s, |h| Ok((h.head()?, h.watched()?.len()))).await {
        Ok((head, watched)) if stopped.is_some() => (
            StatusCode::SERVICE_UNAVAILABLE,
            Json(json!({
                "version": env!("CARGO_PKG_VERSION"),
                "mode": "address_history",
                "ready": false,
                "chain_id": s.chain_id,
                "head": head,
                "watched": watched,
                "error": stopped,
            })),
        ),
        // Before the first finalized head nothing can be answered, but the chain is known, so a
        // client can still register its addresses.
        Ok((None, watched)) => (
            StatusCode::SERVICE_UNAVAILABLE,
            Json(json!({
                "version": env!("CARGO_PKG_VERSION"),
                "mode": "address_history",
                "ready": false,
                "chain_id": s.chain_id,
                "head": Value::Null,
                "watched": watched,
                "error": "the chain head has not been read yet",
            })),
        ),
        Ok((head, watched)) => (
            StatusCode::OK,
            Json(json!({
                "version": env!("CARGO_PKG_VERSION"),
                "mode": "address_history",
                "ready": true,
                "chain_id": s.chain_id,
                "head": head,
                "tip": s.history.tip().ok().flatten(),
                "poll_interval_secs": s.poll_interval.as_secs(),
                "stalled": s.stalled(),
                "watched": watched,
            })),
        ),
        Err(e) => (
            StatusCode::SERVICE_UNAVAILABLE,
            Json(json!({
                "version": env!("CARGO_PKG_VERSION"),
                "mode": "address_history",
                "ready": false,
                "error": format!("{e:#}"),
            })),
        ),
    }
}

async fn metrics(State(s): State<Arc<ModeState>>) -> impl IntoResponse {
    if let Some(counter) = &s.counter {
        for (endpoint, calls) in counter() {
            s.set_calls(endpoint, calls);
        }
    }
    let (head, watched) = match blocking(&s, |h| Ok((h.head()?, h.watched()?.len()))).await {
        Ok((head, watched)) => (head.unwrap_or(0), watched),
        Err(e) => {
            return (
                StatusCode::SERVICE_UNAVAILABLE,
                [(axum::http::header::CONTENT_TYPE, "text/plain")],
                format!("store unreadable: {e:#}\n"),
            );
        }
    };
    let mut by_method = String::from(
        "# HELP nuthatch_address_history_rpc_calls_total RPC calls by endpoint role and method.\n\
         # TYPE nuthatch_address_history_rpc_calls_total counter\n",
    );
    for ((endpoint, method), n) in s.calls.lock().expect("calls lock").iter() {
        by_method.push_str(&format!(
            "nuthatch_address_history_rpc_calls_total{{endpoint=\"{endpoint}\",method=\"{method}\"}} {n}\n"
        ));
    }
    let body = format!(
        "{by_method}\
         # TYPE nuthatch_address_history_windows_recorded_total counter\n\
         nuthatch_address_history_windows_recorded_total {}\n\
         # TYPE nuthatch_address_history_discovery_errors_total counter\n\
         nuthatch_address_history_discovery_errors_total {}\n\
         # HELP nuthatch_address_history_rpc_requests_total RPC requests sent, startup checks and failovers included.\n\
         # TYPE nuthatch_address_history_rpc_requests_total counter\n\
         nuthatch_address_history_rpc_requests_total {}\n\
         # TYPE nuthatch_address_history_polls_total counter\n\
         nuthatch_address_history_polls_total {}\n\
         # TYPE nuthatch_address_history_poll_errors_total counter\n\
         nuthatch_address_history_poll_errors_total {}\n\
         # TYPE nuthatch_address_history_head gauge\n\
         nuthatch_address_history_head {head}\n\
         # TYPE nuthatch_address_history_watched gauge\n\
         nuthatch_address_history_watched {watched}\n",
        s.windows.load(Ordering::Relaxed),
        s.discovery_errors.load(Ordering::Relaxed),
        // Every call counted so far, not only those made by the last head read.
        s.rpc_requests
            .load(Ordering::Relaxed)
            .max(s.calls.lock().expect("calls lock").values().sum::<u64>()),
        s.polls.load(Ordering::Relaxed),
        s.poll_errors.load(Ordering::Relaxed),
    );
    (
        StatusCode::OK,
        [(
            axum::http::header::CONTENT_TYPE,
            "text/plain; version=0.0.4",
        )],
        body,
    )
}

/// Each endpoint role's calls by method, read live.
pub type Counter = Arc<dyn Fn() -> Vec<(&'static str, BTreeMap<String, u64>)> + Send + Sync>;

/// The live [`Counter`] over `discovery`'s endpoints.
pub fn counter<M: Rpc, T: Rpc>(discovery: Arc<Discoverer<Counted<M>, Counted<T>>>) -> Counter {
    Arc::new(move || {
        vec![
            ("main", discovery.main.calls()),
            ("trace", discovery.trace.calls()),
        ]
    })
}

/// Forwards a `proxy` or `logs` call to the nest's main RPC, once `verified` says it is on the
/// nest's chain. Counted with the rest of the nest's calls.
pub fn forward<M: Rpc, T: Rpc>(
    discovery: Arc<Discoverer<Counted<M>, Counted<T>>>,
    verified: Arc<tokio::sync::OnceCell<()>>,
) -> crate::address_proxy::Forward {
    Arc::new(move |method: String, params: Value| {
        let (discovery, verified) = (discovery.clone(), verified.clone());
        Box::pin(async move {
            if verified.get().is_none() {
                bail!("the RPC's chain has not been checked yet; try again shortly");
            }
            discovery.main.call(&method, params).await
        })
    })
}

/// `--poll-interval` wins over `poll_interval` in `[address_history]`, which wins over the default.
pub fn poll_interval(flag: Option<Duration>, config: &AddressHistoryConfig) -> Result<Duration> {
    match flag {
        Some(d) => Ok(d),
        None => config.poll_interval(),
    }
}

/// `dev` flags that drive the event path. Accepting one here would start a healthy-looking nest that
/// silently ignores it.
fn refuse_event_flags(args: &crate::cli::DevArgs) -> Result<()> {
    let set: Vec<&str> = [
        ("--backfill", args.backfill.is_some()),
        ("--seal-direct", args.seal_direct),
        ("--concurrency", args.concurrency != 1),
        ("--window", args.window.is_some()),
        ("--finality-only", args.finality_only),
        ("--publish-target", args.publish_target.is_some()),
        ("--audit-rpc", args.audit_rpc.is_some()),
        ("--state-rpc", !args.state_rpc.is_empty()),
        ("--ipfs", !args.ipfs.is_empty()),
        ("--registry", args.registry.is_some()),
    ]
    .into_iter()
    .filter_map(|(flag, on)| on.then_some(flag))
    .collect();
    if !set.is_empty() {
        bail!(
            "an address-history nest does not take {}: those drive an event nest's indexing",
            set.join(", ")
        );
    }
    Ok(())
}

/// `nuthatch dev` on a rotki-mode nest.
pub async fn dev(
    dir: &Path,
    config: Config,
    args: &crate::cli::DevArgs,
    cors: Option<tower_http::cors::CorsLayer>,
) -> Result<()> {
    let Some(ah) = &config.address_history else {
        bail!("not an address-history nest");
    };
    if !config.contracts.is_empty() {
        bail!(
            "{} declares both [address_history] and [[contracts]]; an address-history nest watches \
             accounts and indexes no contract, so give each its own nest",
            crate::config::CONFIG_FILE
        );
    }
    refuse_event_flags(args)?;
    let poll_interval = poll_interval(args.poll_interval, ah)?;
    let listener = tokio::net::TcpListener::bind(&args.listen)
        .await
        .with_context(|| format!("cannot bind {}", args.listen))?;
    let loopback = listener.local_addr()?.ip().is_loopback();
    if !loopback {
        tracing::warn!(
            "address history: listening on {}, not loopback: anyone who can reach it can read /api and \
             GET /api/watch, which lists the accounts watched",
            listener.local_addr()?
        );
    }
    // redb caches every page it reads or writes up to its cache size, and a backfill touches all of
    // them; an address-history nest reads by key, so a small cache costs it nothing.
    let store =
        crate::store::Store::open_with_cache(&dir.join(crate::config::DB_FILE), STORE_CACHE_BYTES)?;
    let history = AddressHistory::open(store, config.nest.chain_id, &ah.addresses)?;

    let urls = crate::rpc::select_rpcs(&args.rpc, config.nest.rpc_urls.clone());
    let trace_urls = if args.trace_rpc.is_empty() {
        urls.clone()
    } else {
        args.trace_rpc.clone()
    };
    let main = RpcClient::with_fallbacks(urls, args.rpc_fallback.clone())?;
    let trace = RpcClient::with_fallbacks(trace_urls, Vec::new())?;
    let chain_id = config.nest.chain_id;
    let depth = match crate::chains::lookup_by_id(chain_id).map(|c| c.finality) {
        Some(crate::chains::Finality::Depth(n)) => n,
        Some(crate::chains::Finality::FinalizedTag { fallback_depth }) => fallback_depth,
        None => 64,
    };
    let discovery = Arc::new(Discoverer::new(Counted::new(main), Counted::new(trace)));
    // Forwarded calls wait on the same chain check the cursor does.
    let verified = Arc::new(tokio::sync::OnceCell::<()>::new());
    let mut state = ModeState::new(history, chain_id, poll_interval, loopback)
        .with_range(
            ah.start_block
                .unwrap_or(0)
                .max(crate::address_history::traced_from(chain_id)),
            ah.end_block,
            depth,
        )
        .with_deadline(ah.request_deadline()?)
        .with_forward(forward(discovery.clone(), verified.clone()))
        .with_counter(counter(discovery.clone()));
    if ah.mev_relays {
        if chain_id != 1 {
            bail!("[address_history] mev_relays is for Ethereum mainnet's relays; this nest is on chain {chain_id}");
        }
        tracing::info!(
            "address history: asking the MEV-Boost relays about produced blocks; they see this machine's IP and those block numbers"
        );
        state = state.with_relays(crate::address_mev::relays_over_http()?);
    }
    if let Some(m) = &ah.mirror {
        if m.chain_id != chain_id {
            bail!(
                "[address_history.mirror] names chain {}, and this nest is on chain {chain_id}",
                m.chain_id
            );
        }
        // Before anything is served: RPC-scanned rows in the mirror's blocks must not answer while the
        // cursor, which may never get past its chain check, has yet to replace them.
        state.history.keep_verified_only(
            &crate::address_discovery::BLOCK_SCANNED,
            (m.from_block, m.to_block),
        )?;
        let source = crate::address_partitions::Source::open(
            &m.url,
            chain_id,
            dir.join("partitions"),
            m.cache_mb * 1024 * 1024,
        )?;
        tracing::info!(
            "address history: blocks {} to {} from verified partitions at the configured mirror, which \
             sees this machine's IP and the block ranges asked for, never an address",
            m.from_block,
            m.to_block
        );
        state = state.with_mirror(Arc::new(source), (m.from_block, m.to_block));
    }
    let state = Arc::new(state);
    tracing::info!(
        "address history: {} watched from block {}, polling every {}s",
        state.history.watched()?.len(),
        state.start_block,
        poll_interval.as_secs()
    );
    // The chain check runs in the cursor, not before serving: covered history answers at once, even
    // offline. A wrong chain stops the cursor before it fetches anything.
    let task_state = state.clone();
    let work = tokio::spawn(async move {
        for (role, rpc) in [
            ("main", discovery.main.inner()),
            ("trace", discovery.trace.inner()),
        ] {
            if let Err(e) = rpc.verify_chain_ids(chain_id).await {
                task_state.stop(format!("the {role} RPC: {e:#}"));
                return;
            }
        }
        let _ = verified.set(());
        task_state.rpc_requests.store(
            discovery.main.inner().request_count() + discovery.trace.inner().request_count(),
            Ordering::Relaxed,
        );
        cursor(task_state, MainHead(discovery.clone()), discovery).await;
    });
    let work_abort = work.abort_handle();
    let cursor = supervise(state.clone(), work);
    let served = crate::serve::serve_bound(listener, router(state), cors).await;
    cursor.abort();
    work_abort.abort();
    served
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::store::Store;
    use tower::ServiceExt;

    const ALICE: &str = "0xd8dA6BF26964aF9D7eEd9e03E53415D37aA96045";
    const BOB: &str = "0x00000000219ab540356cbb839cbe05303d7705fa";

    /// A head source: 0 means down. Each read counts as `per_read` requests, as a failover would.
    struct FixedHead {
        head: AtomicU64,
        reads: AtomicU64,
        per_read: u64,
    }
    fn fixed(head: u64, per_read: u64) -> Arc<FixedHead> {
        Arc::new(FixedHead {
            head: AtomicU64::new(head),
            reads: AtomicU64::new(0),
            per_read,
        })
    }
    impl HeadSource for FixedHead {
        async fn head(&self) -> Result<u64> {
            self.reads.fetch_add(1, Ordering::Relaxed);
            match self.head.load(Ordering::Relaxed) {
                0 => bail!("down"),
                h => Ok(h),
            }
        }
        fn requests(&self) -> u64 {
            self.reads.load(Ordering::Relaxed) * self.per_read
        }
    }

    fn state(dir: &Path, loopback: bool) -> Arc<ModeState> {
        let store = Store::open(&dir.join("t.redb")).unwrap();
        let history = AddressHistory::open(store, 1, &[ALICE.into()]).unwrap();
        Arc::new(ModeState::new(
            history,
            1,
            Duration::from_secs(300),
            loopback,
        ))
    }

    async fn call(
        app: Router,
        method: &str,
        path: &str,
        body: Option<Value>,
    ) -> (StatusCode, Value) {
        let mut req = axum::http::Request::builder().method(method).uri(path);
        let body = match body {
            Some(b) => {
                req = req.header("content-type", "application/json");
                axum::body::Body::from(b.to_string())
            }
            None => axum::body::Body::empty(),
        };
        let resp = app.oneshot(req.body(body).unwrap()).await.unwrap();
        let status = resp.status();
        let bytes = axum::body::to_bytes(resp.into_body(), usize::MAX)
            .await
            .unwrap();
        let v = serde_json::from_slice(&bytes)
            .unwrap_or(Value::String(String::from_utf8_lossy(&bytes).into()));
        (status, v)
    }

    /// A conflict found while a request is being answered refuses that answer too.
    #[tokio::test]
    async fn a_conflict_found_mid_request_refuses_its_answer() {
        let dir = tempfile::tempdir().unwrap();
        let store = Store::open(&dir.path().join("t.redb")).unwrap();
        let history = AddressHistory::open(store, 1, &[ALICE.into()]).unwrap();
        let cell: Arc<std::sync::OnceLock<Arc<ModeState>>> = Arc::default();
        let seen = cell.clone();
        let forward: crate::address_proxy::Forward = Arc::new(move |_, _| {
            let seen = seen.clone();
            Box::pin(async move {
                seen.get()
                    .unwrap()
                    .conflict("finalized block 9 is now at another hash".into());
                Ok(json!("0x10"))
            })
        });
        let state = Arc::new(
            ModeState::new(history, 1, Duration::from_secs(300), true).with_forward(forward),
        );
        let _ = cell.set(state.clone());
        let (_, v) = call(
            router(state),
            "GET",
            "/api?module=proxy&action=eth_blockNumber",
            None,
        )
        .await;
        let r = v["result"].as_str().unwrap();
        assert!(
            r.starts_with("NUTHATCH_INCOMPLETE:") && r.contains("finalized block 9"),
            "{v}"
        );
    }

    /// The nest's RPC is lent out through `proxy` only on a loopback bind.
    #[tokio::test]
    async fn the_proxy_answers_only_on_loopback() {
        for loopback in [true, false] {
            let dir = tempfile::tempdir().unwrap();
            let store = Store::open(&dir.path().join("t.redb")).unwrap();
            let history = AddressHistory::open(store, 1, &[ALICE.into()]).unwrap();
            let forward: crate::address_proxy::Forward =
                Arc::new(|_, _| Box::pin(async { Ok(json!("0x10")) }));
            let state = Arc::new(
                ModeState::new(history, 1, Duration::from_secs(300), loopback)
                    .with_forward(forward),
            );
            let (_, v) = call(
                router(state),
                "GET",
                "/api?module=proxy&action=eth_blockNumber",
                None,
            )
            .await;
            if loopback {
                assert_eq!(v["result"], "0x10");
            } else {
                assert!(
                    v["result"]
                        .as_str()
                        .unwrap()
                        .starts_with("NUTHATCH_UNSUPPORTED:"),
                    "{v}"
                );
            }
        }
    }

    #[tokio::test]
    async fn a_poll_is_one_head_read_and_lands_on_ready_and_metrics() {
        let dir = tempfile::tempdir().unwrap();
        let s = state(dir.path(), true);
        let source = fixed(1_234, 1);
        let (st, r) = call(router(s.clone()), "GET", "/ready", None).await;
        assert_eq!(st, StatusCode::SERVICE_UNAVAILABLE, "no head yet: {r}");
        assert_eq!(
            r["chain_id"], 1,
            "the chain is known before the head is: {r}"
        );
        assert_eq!(step(&s, &source, &()).await.unwrap(), 1_234);
        assert_eq!(
            source.reads.load(Ordering::Relaxed),
            1,
            "one head read per poll"
        );

        let (_, r) = call(router(s.clone()), "GET", "/ready", None).await;
        assert_eq!(r["head"], 1_234, "{r}");
        assert_eq!(r["version"], env!("CARGO_PKG_VERSION"));
        assert_eq!(r["stalled"], false);
        let (_, m) = call(router(s.clone()), "GET", "/metrics", None).await;
        let m = m.as_str().unwrap();
        assert!(
            m.contains("nuthatch_address_history_rpc_requests_total 1\n"),
            "{m}"
        );
        assert!(m.contains("nuthatch_address_history_head 1234\n"), "{m}");

        source.head.store(0, Ordering::Relaxed);
        assert!(step(&s, &source, &()).await.is_err());
        let (_, r) = call(router(s.clone()), "GET", "/ready", None).await;
        assert_eq!(r["head"], 1_234, "a failed poll keeps the last head");
        assert_eq!(r["ready"], true, "offline still serves");
        let (_, m) = call(router(s.clone()), "GET", "/metrics", None).await;
        assert!(m
            .as_str()
            .unwrap()
            .contains("nuthatch_address_history_poll_errors_total 1\n"));
    }

    /// The counter is the source's own request count, so a read that failed over to a second
    /// endpoint shows as two requests, not one poll.
    #[tokio::test]
    async fn forwarded_calls_show_in_the_metrics_at_once() {
        let dir = tempfile::tempdir().unwrap();
        let chain = Chain::new(10, 5);
        let d = Arc::new(chain.discoverer());
        let s = Arc::new(
            ModeState::new(
                state(dir.path(), true).history.clone(),
                1,
                Duration::from_secs(300),
                true,
            )
            .with_counter(counter(d.clone())),
        );
        // A forwarded request's calls, with no cursor to report them.
        for _ in 0..4 {
            d.main.call("eth_blockNumber", json!([])).await.unwrap();
        }
        let (_, m) = call(router(s), "GET", "/metrics", None).await;
        let m = m.as_str().unwrap().to_string();
        assert!(
            m.contains("nuthatch_address_history_rpc_requests_total 4\n"),
            "{m}"
        );
        assert!(
            m.contains("{endpoint=\"main\",method=\"eth_blockNumber\"} 4"),
            "{m}"
        );
    }

    #[tokio::test]
    async fn metrics_count_requests_not_polls() {
        let dir = tempfile::tempdir().unwrap();
        let s = state(dir.path(), true);
        let source = fixed(9, 2);
        poll_once(&s, &source).await.unwrap();
        let (_, m) = call(router(s.clone()), "GET", "/metrics", None).await;
        let m = m.as_str().unwrap();
        assert!(
            m.contains("nuthatch_address_history_rpc_requests_total 2\n"),
            "{m}"
        );
        assert!(
            m.contains("nuthatch_address_history_polls_total 1\n"),
            "{m}"
        );
    }

    #[tokio::test(start_paused = true)]
    async fn the_cursor_polls_once_per_interval_and_reports_a_stall() {
        let dir = tempfile::tempdir().unwrap();
        let s = state(dir.path(), true);
        let source = fixed(7, 1);
        let task = tokio::spawn(cursor(s.clone(), source.clone(), ()));
        let settle = || async {
            for _ in 0..20 {
                tokio::task::yield_now().await;
                tokio::time::sleep(Duration::from_millis(1)).await;
            }
        };
        settle().await;
        assert_eq!(source.reads.load(Ordering::Relaxed), 1, "one poll at start");
        tokio::time::advance(Duration::from_secs(299)).await;
        settle().await;
        assert_eq!(
            source.reads.load(Ordering::Relaxed),
            1,
            "none before the interval"
        );
        tokio::time::advance(Duration::from_secs(1)).await;
        settle().await;
        assert_eq!(source.reads.load(Ordering::Relaxed), 2);
        task.abort();
    }

    #[test]
    fn a_cursor_quiet_for_three_intervals_is_stalled() {
        let dir = tempfile::tempdir().unwrap();
        let s = state(dir.path(), true);
        assert!(!s.stalled());
        let fresh = ModeState::new(s.history.clone(), 1, Duration::from_millis(1), true);
        std::thread::sleep(Duration::from_millis(10));
        assert!(fresh.stalled(), "no poll in three intervals");
        let ms = u64::try_from(fresh.started.elapsed().as_millis()).unwrap();
        fresh.last_poll.store(ms + 1, Ordering::Relaxed);
        assert!(!fresh.stalled(), "a poll just now");
    }

    #[tokio::test]
    async fn a_panicking_cursor_takes_ready_down() {
        let dir = tempfile::tempdir().unwrap();
        let s = state(dir.path(), true);
        let work = tokio::spawn(async { panic!("a provider answer the decoder could not take") });
        supervise(s.clone(), work).await.unwrap();
        let (st, r) = call(router(s), "GET", "/ready", None).await;
        assert_eq!(st, StatusCode::SERVICE_UNAVAILABLE, "{r}");
        assert!(r["error"].as_str().unwrap().contains("panicked"), "{r}");
    }

    #[tokio::test]
    async fn watch_and_unwatch_take_effect_without_a_restart() {
        let dir = tempfile::tempdir().unwrap();
        let s = state(dir.path(), true);
        let (st, r) = call(
            router(s.clone()),
            "POST",
            "/api/watch",
            Some(json!({"address": BOB})),
        )
        .await;
        assert_eq!(st, StatusCode::OK, "{r}");
        assert_eq!(r["watched"].as_array().unwrap().len(), 2);
        let q = format!("/api?module=account&action=txlist&address={BOB}&startblock=0&endblock=1");
        let (_, a) = call(router(s.clone()), "GET", &q, None).await;
        assert!(
            a["result"]
                .as_str()
                .unwrap()
                .starts_with("NUTHATCH_INCOMPLETE:"),
            "{a}"
        );

        let (st, r) = call(
            router(s.clone()),
            "POST",
            "/api/unwatch",
            Some(json!({"address": BOB})),
        )
        .await;
        assert_eq!(st, StatusCode::OK);
        assert_eq!(r["watched"], json!([ALICE.to_lowercase()]));
        let (st, _) = call(
            router(s.clone()),
            "POST",
            "/api/watch",
            Some(json!({"address": "0xnope"})),
        )
        .await;
        assert_eq!(st, StatusCode::BAD_REQUEST);
    }

    #[tokio::test]
    async fn watch_is_refused_off_loopback() {
        let dir = tempfile::tempdir().unwrap();
        let s = state(dir.path(), false);
        let (st, _) = call(
            router(s.clone()),
            "POST",
            "/api/watch",
            Some(json!({"address": BOB})),
        )
        .await;
        assert_eq!(st, StatusCode::FORBIDDEN);
        assert_eq!(s.history.watched().unwrap().len(), 1);
    }

    /// Five minutes, written out: a test that compared against the constant would move with it.
    #[test]
    fn the_poll_interval_defaults_to_five_minutes_and_the_flag_wins() {
        let mut cfg = AddressHistoryConfig {
            addresses: vec![ALICE.into()],
            start_block: None,
            end_block: None,
            poll_interval: None,
            mirror: None,
            mev_relays: false,
            request_deadline: None,
        };
        assert_eq!(
            poll_interval(None, &cfg).unwrap(),
            Duration::from_secs(5 * 60)
        );
        cfg.poll_interval = Some("2m".into());
        assert_eq!(poll_interval(None, &cfg).unwrap(), Duration::from_secs(120));
        assert_eq!(
            poll_interval(Some(Duration::from_secs(7)), &cfg).unwrap(),
            Duration::from_secs(7)
        );
    }

    #[test]
    fn event_flags_are_refused_by_name() {
        use clap::Parser;
        let parse = |extra: &[&str]| {
            let mut argv = vec!["nuthatch", "dev", "--dir", "x"];
            argv.extend_from_slice(extra);
            match crate::cli::Cli::parse_from(argv).command {
                crate::cli::Command::Dev(a) => a,
                _ => unreachable!(),
            }
        };
        refuse_event_flags(&parse(&[])).unwrap();
        refuse_event_flags(&parse(&["--poll-interval", "1m", "--rpc", "https://x"])).unwrap();
        for flag in [
            &["--backfill", "10"][..],
            &["--seal-direct"],
            &["--window", "100"],
            &["--finality-only"],
            &["--publish-target", "s3://x"],
            &["--audit-rpc", "https://x"],
        ] {
            let err = refuse_event_flags(&parse(flag)).unwrap_err();
            assert!(err.to_string().contains(flag[0]), "{flag:?}: {err}");
        }
    }

    /// With a mirror, block history comes from verified partitions: no body is read over RPC in the
    /// mirror's blocks, only each partition's anchor and the bodies of blocks a watched address
    /// produced, and a partition the mirror lacks leaves its blocks incomplete.
    #[tokio::test]
    async fn the_mirror_covers_its_blocks_and_a_missing_partition_stays_incomplete() {
        use crate::address_discovery::Discoverer;
        use crate::address_history::{Action, Answer, PageRequest, Sort};
        use crate::address_partitions::tests::{chain_rpc, Fake, MINER, PAID};
        use crate::address_partitions::{build, partition_key, Source, SPAN};
        let mirror = tempfile::tempdir().unwrap();
        let chain = chain_rpc(2 * SPAN + 5);
        build(
            &chain,
            mirror.path().to_str().unwrap(),
            1,
            (0, 2 * SPAN - 1),
            4,
            false,
            |_, _, _| {},
        )
        .await
        .unwrap();
        std::fs::remove_file(mirror.path().join(partition_key(1, SPAN))).unwrap();

        // The RPC's finalized block, which the test moves.
        let finalized = Arc::new(AtomicU64::new(2 * SPAN + 5));
        let fin = finalized.clone();
        let main = Fake::new(move |m, p| match m {
            "eth_getBlockByNumber" if p[0] == "finalized" => {
                Ok(json!({"number": format!("0x{:x}", fin.load(Ordering::Relaxed))}))
            }
            "eth_getLogs" => Ok(json!([])),
            "eth_getCode" => Ok(json!("0x")),
            "eth_getTransactionCount" => Ok(json!("0x0")),
            "eth_getBlockTransactionCountByNumber" => Ok(json!("0x1")),
            _ => (chain.answer)(m, p),
        });
        let twin = chain_rpc(2 * SPAN + 5);
        let trace = Fake::new(move |m, p| match m {
            "trace_filter" => Ok(json!([])),
            "trace_block" => Ok(json!([{"x": 1}])),
            "eth_getBlockByNumber" => (twin.answer)(m, p),
            other => bail!("unexpected {other}"),
        });
        let d = Discoverer::new(Counted::new(main), Counted::new(trace));
        let (paid, miner) = (format!("{PAID:#x}"), format!("{MINER:#x}"));
        let dir = tempfile::tempdir().unwrap();
        let history = AddressHistory::open(
            Store::open(&dir.path().join("t.redb")).unwrap(),
            1,
            &[paid.clone(), miner.clone()],
        )
        .unwrap();
        let source = Source::open(
            mirror.path().to_str().unwrap(),
            1,
            dir.path().join("partitions"),
            u64::MAX,
        )
        .unwrap();
        let state = ModeState::new(history.clone(), 1, Duration::from_secs(300), true)
            .with_range(5, Some(2 * SPAN - 1), 0)
            .with_mirror(Arc::new(source), (0, 2 * SPAN - 1));
        state.history.finalize(2 * SPAN + 5).unwrap();
        d.catch_up(&state, 2 * SPAN + 5).await;

        let covered = vec![(5, SPAN - 1)];
        assert_eq!(
            history.coverage(Action::BeaconWithdrawals, &paid).unwrap(),
            covered
        );
        assert_eq!(
            history.coverage(Action::MinedBlocks, &miner).unwrap(),
            covered
        );
        assert_eq!(history.header_coverage().unwrap(), covered);
        assert_eq!(
            d.main.calls().get("eth_getBlockByNumber"),
            Some(&7),
            "the finality check, one anchor per partition, no body, and two hashes per address's \
             discovery window to check its trace endpoint is on the main chain, before and after"
        );
        assert_eq!(
            d.main.calls().get("eth_getBlockByHash"),
            Some(&99),
            "blocks 100 to 9,900"
        );

        let page = |action, address: &str, start, end| {
            history
                .page(
                    action,
                    &PageRequest {
                        address: address.into(),
                        start_block: start,
                        end_block: end,
                        sort: Sort::Asc,
                        page: 1,
                        offset: 10_000,
                        generation: None,
                    },
                )
                .unwrap()
        };
        let Answer::Rows(w) = page(Action::BeaconWithdrawals, &paid, Some(5), Some(SPAN - 1))
        else {
            panic!("covered")
        };
        assert_eq!(w.len() as u64, SPAN - 5);
        let Answer::Rows(m) = page(Action::MinedBlocks, &miner, Some(5), Some(SPAN - 1)) else {
            panic!("covered")
        };
        assert_eq!(m.len(), 99);
        for (start, end) in [(None, Some(SPAN - 1)), (Some(5), Some(SPAN + 10))] {
            assert!(
                matches!(
                    page(Action::MinedBlocks, &miner, start, end),
                    Answer::Incomplete(_)
                ),
                "{start:?}..{end:?}"
            );
        }
        assert_eq!(
            history
                .block_by_time(1_600_000_000 + 12 * 77, false)
                .unwrap(),
            Some(77)
        );
        assert_eq!(
            history
                .block_by_time(1_600_000_000 + 12 * 77 + 1, true)
                .unwrap(),
            Some(78)
        );
        assert_eq!(
            history
                .block_by_time(1_600_000_000 + 12 * SPAN, true)
                .unwrap(),
            None
        );

        // Published late, the partition is used, but only once its last block is final for the nest.
        build(
            &chain_rpc(2 * SPAN + 5),
            mirror.path().to_str().unwrap(),
            1,
            (SPAN, 2 * SPAN - 1),
            4,
            false,
            |_, _, _| {},
        )
        .await
        .unwrap();
        finalized.store(2 * SPAN - 2, Ordering::Relaxed);
        let reads = d.main.calls()["eth_getBlockByNumber"];
        state.history.finalize(2 * SPAN + 5).unwrap();
        d.catch_up(&state, 2 * SPAN + 5).await;
        assert_eq!(history.header_coverage().unwrap(), covered);
        assert_eq!(
            d.main.calls()["eth_getBlockByNumber"],
            reads + 1,
            "the finality check, and no anchor past it"
        );
        finalized.store(2 * SPAN - 1, Ordering::Relaxed);
        state.history.finalize(2 * SPAN + 5).unwrap();
        d.catch_up(&state, 2 * SPAN + 5).await;
        assert_eq!(history.header_coverage().unwrap(), vec![(5, 2 * SPAN - 1)]);
        assert_eq!(
            history.coverage(Action::MinedBlocks, &miner).unwrap(),
            vec![(5, 2 * SPAN - 1)]
        );
    }

    /// Off Ethereum mainnet a partition is read for its headers alone, so timestamp to block is
    /// answered there too, and no body is scanned for withdrawals or produced blocks.
    #[tokio::test]
    async fn off_mainnet_the_mirror_gives_headers_and_nothing_else() {
        use crate::address_discovery::Discoverer;
        use crate::address_history::Action;
        use crate::address_partitions::tests::{chain_rpc, Fake, PAID};
        use crate::address_partitions::{build, Source, SPAN};
        let mirror = tempfile::tempdir().unwrap();
        let chain = chain_rpc(SPAN + 5);
        let url = mirror.path().to_str().unwrap();
        build(&chain, url, 100, (0, SPAN - 1), 4, false, |_, _, _| {})
            .await
            .unwrap();
        let main = Fake::new(move |m, p| match m {
            "eth_getLogs" => Ok(json!([])),
            _ => (chain.answer)(m, p),
        });
        let twin = chain_rpc(SPAN + 5);
        let trace = Fake::new(move |m, p| match m {
            "trace_filter" => Ok(json!([])),
            "eth_getBlockByNumber" => (twin.answer)(m, p),
            other => bail!("unexpected {other}"),
        });
        let d = Discoverer::new(Counted::new(main), Counted::new(trace));
        let paid = format!("{PAID:#x}");
        let dir = tempfile::tempdir().unwrap();
        let history = AddressHistory::open(
            Store::open(&dir.path().join("t.redb")).unwrap(),
            100,
            std::slice::from_ref(&paid),
        )
        .unwrap();
        let source = Source::open(url, 100, dir.path().join("partitions"), u64::MAX).unwrap();
        let state = ModeState::new(history.clone(), 100, Duration::from_secs(300), true)
            .with_range(0, Some(SPAN - 1), 0)
            .with_mirror(Arc::new(source), (0, SPAN - 1));
        state.history.finalize(SPAN + 5).unwrap();
        d.catch_up(&state, SPAN + 5).await;

        assert_eq!(history.header_coverage().unwrap(), vec![(0, SPAN - 1)]);
        assert_eq!(
            history
                .block_by_time(1_600_000_000 + 12 * 77, false)
                .unwrap(),
            Some(77)
        );
        assert!(history
            .coverage(Action::BeaconWithdrawals, &paid)
            .unwrap()
            .is_empty());
        assert_eq!(d.main.calls().get("eth_getBlockByHash"), None);
    }

    /// The mirror pass runs for headers alone when nothing watched lacks the blocks, a range that
    /// ends mid-partition still takes that partition, and the RPC scan stops where the mirror starts.
    #[tokio::test]
    async fn the_mirror_meets_unaligned_ranges_and_the_rpc_scan_at_its_edges() {
        use crate::address_discovery::Discoverer;
        use crate::address_history::Action;
        use crate::address_partitions::tests::{chain_rpc, Fake, PAID};
        use crate::address_partitions::{build, Source, SPAN};
        let mirror = tempfile::tempdir().unwrap();
        build(
            &chain_rpc(2 * SPAN + 5),
            mirror.path().to_str().unwrap(),
            1,
            (0, 2 * SPAN - 1),
            4,
            false,
            |_, _, _| {},
        )
        .await
        .unwrap();
        let nest = |seed: &[String], range: (u64, u64), blocks: (u64, u64)| {
            let chain = chain_rpc(2 * SPAN + 5);
            let main = Fake::new(move |m, p| match m {
                "eth_getLogs" => Ok(json!([])),
                "eth_getCode" => Ok(json!("0x")),
                "eth_getTransactionCount" => Ok(json!("0x0")),
                "eth_getBlockTransactionCountByNumber" => Ok(json!("0x1")),
                _ => (chain.answer)(m, p),
            });
            let twin = chain_rpc(2 * SPAN + 5);
            let trace = Fake::new(move |m, p| match m {
                "trace_filter" => Ok(json!([])),
                "trace_block" => Ok(json!([{"x": 1}])),
                "eth_getBlockByNumber" => (twin.answer)(m, p),
                other => bail!("unexpected {other}"),
            });
            let dir = tempfile::tempdir().unwrap();
            let history =
                AddressHistory::open(Store::open(&dir.path().join("t.redb")).unwrap(), 1, seed)
                    .unwrap();
            let source = Source::open(
                mirror.path().to_str().unwrap(),
                1,
                dir.path().join("partitions"),
                u64::MAX,
            )
            .unwrap();
            let state = ModeState::new(history.clone(), 1, Duration::from_secs(300), true)
                .with_range(range.0, Some(range.1), 0)
                .with_mirror(Arc::new(source), blocks);
            (
                dir,
                Discoverer::new(Counted::new(main), Counted::new(trace)),
                history,
                state,
            )
        };

        let (_dir, d, history, state) = nest(&[], (5, SPAN - 10), (0, 2 * SPAN - 1));
        state.history.finalize(2 * SPAN + 5).unwrap();
        d.catch_up(&state, 2 * SPAN + 5).await;
        assert_eq!(history.header_coverage().unwrap(), vec![(5, SPAN - 10)]);

        let paid = format!("{PAID:#x}");
        let (_dir, d, history, state) = nest(
            std::slice::from_ref(&paid),
            (SPAN - 20, SPAN + 30),
            (SPAN, 2 * SPAN - 1),
        );
        state.history.finalize(2 * SPAN + 5).unwrap();
        d.catch_up(&state, 2 * SPAN + 5).await;
        assert_eq!(
            history.coverage(Action::BeaconWithdrawals, &paid).unwrap(),
            vec![(SPAN - 20, SPAN + 30)]
        );
        assert_eq!(
            d.main.calls()["eth_getBlockByNumber"],
            20 + 2 + 2 + 2,
            "twenty bodies before the mirror, its finality check, one anchor, two hashes to check the \
             discovery window's trace endpoint is on the main chain, and the body window's last hash \
             checked twice"
        );

        // Coverage the RPC scan recorded before a mirror was configured does not answer for the
        // mirror's blocks while no partition has replaced it.
        let (_dir, d, history, state) = nest(
            std::slice::from_ref(&paid),
            (2 * SPAN, 2 * SPAN + 5),
            (2 * SPAN, 3 * SPAN - 1),
        );
        let g = history.generation().unwrap();
        history
            .record(
                Action::BeaconWithdrawals,
                &paid,
                &[],
                (2 * SPAN, 2 * SPAN + 5),
                g,
            )
            .unwrap();
        state.history.finalize(2 * SPAN + 5).unwrap();
        d.catch_up(&state, 2 * SPAN + 5).await;
        assert_eq!(
            history.coverage(Action::BeaconWithdrawals, &paid).unwrap(),
            vec![]
        );
    }

    /// A chain the tests can reorganise: one hash per block, a finalized tag, and a head.
    #[derive(Default)]
    struct Chain {
        hashes: std::sync::Mutex<Vec<[u8; 32]>>,
        finalized: AtomicU64,
        seed: AtomicU64,
        /// A reorg of this depth lands as soon as the next trace_filter has answered.
        reorg_next: AtomicU64,
        /// `(block, n, depth)`: a reorg of `depth` lands once block `block` has been read `n` times.
        reorg_on_read: std::sync::Mutex<Option<(u64, u64, u64)>>,
    }

    impl Chain {
        fn new(len: u64, finalized: u64) -> Arc<Chain> {
            let c = Arc::new(Chain::default());
            c.grow(len);
            c.finalized.store(finalized, Ordering::Relaxed);
            c
        }
        fn fresh(&self) -> [u8; 32] {
            let n = self.seed.fetch_add(1, Ordering::Relaxed) + 1;
            let mut h = [0u8; 32];
            h[..8].copy_from_slice(&n.wrapping_mul(0x9e37_79b9_7f4a_7c15).to_be_bytes());
            h[31] = 1;
            h
        }
        fn grow(&self, n: u64) {
            for _ in 0..n {
                let h = self.fresh();
                self.hashes.lock().unwrap().push(h);
            }
        }
        fn latest(&self) -> u64 {
            self.hashes.lock().unwrap().len() as u64 - 1
        }
        /// Replace the newest `depth` blocks with new ones.
        fn reorg(&self, depth: u64) {
            let keep = self.hashes.lock().unwrap().len() as u64 - depth;
            self.hashes.lock().unwrap().truncate(keep as usize);
            self.grow(depth);
        }
        fn hash(&self, b: u64) -> [u8; 32] {
            self.hashes.lock().unwrap()[b as usize]
        }
        /// Whether the watched address has a row at `b` on the chain as it stands.
        fn row_at(&self, b: u64) -> bool {
            self.hash(b)[0].is_multiple_of(3)
        }
        fn discoverer(
            self: &Arc<Chain>,
        ) -> Discoverer<
            Counted<crate::address_partitions::tests::Fake>,
            Counted<crate::address_partitions::tests::Fake>,
        > {
            let c = self.clone();
            let main = crate::address_partitions::tests::Fake::new(move |m, p| match m {
                "eth_blockNumber" => Ok(json!(format!("0x{:x}", c.latest()))),
                "eth_getBlockByNumber" if p[0] == "finalized" => {
                    Ok(json!({"number": format!("0x{:x}", c.finalized.load(Ordering::Relaxed))}))
                }
                "eth_getBlockByNumber" => {
                    let b = crate::address_discovery::hex_u64(&p[0])?;
                    if b > c.latest() {
                        return Ok(Value::Null);
                    }
                    let answer = json!({"number": format!("0x{b:x}"),
                              "hash": format!("0x{}", alloy_primitives::hex::encode(c.hash(b)))});
                    c.after_read(b);
                    Ok(answer)
                }
                other => bail!("unexpected {other}"),
            });
            let trace = crate::address_partitions::tests::Fake::new(|m, _| bail!("unexpected {m}"));
            Discoverer::new(Counted::new(main), Counted::new(trace))
        }
        /// Land the reorg [`Chain::reorg_on_read`] waits for, if this read of `b` is the one.
        fn after_read(&self, b: u64) {
            let mut hook = self.reorg_on_read.lock().unwrap();
            if let Some((at, n, depth)) = *hook {
                if at == b {
                    *hook = (n > 1).then_some((at, n - 1, depth));
                    if n == 1 {
                        drop(hook);
                        self.reorg(depth);
                    }
                }
            }
        }
        fn hex(&self, b: u64) -> String {
            format!("0x{}", alloy_primitives::hex::encode(self.hash(b)))
        }
        fn block_json(&self, b: u64) -> Value {
            json!({"number": format!("0x{b:x}"), "hash": self.hex(b), "timestamp": "0x10",
                   "parentHash": if b == 0 { format!("0x{:064x}", 0) } else { self.hex(b - 1) }})
        }
        /// A discoverer that answers the production window as a node would: ALICE receives an
        /// internal call in every block [`Chain::row_at`] marks, the transaction hash being that
        /// block's, and a transaction a reorg took away is no longer found.
        fn node(
            self: &Arc<Chain>,
        ) -> Discoverer<
            Counted<crate::address_partitions::tests::Fake>,
            Counted<crate::address_partitions::tests::Fake>,
        > {
            let (main, trace) = self.node_rpcs();
            Discoverer::new(Counted::new(main), Counted::new(trace))
        }
        fn node_rpcs(
            self: &Arc<Chain>,
        ) -> (
            crate::address_partitions::tests::Fake,
            crate::address_partitions::tests::Fake,
        ) {
            let c = self.clone();
            let main = crate::address_partitions::tests::Fake::new(move |m, p| match m {
                "eth_blockNumber" => Ok(json!(format!("0x{:x}", c.latest()))),
                "eth_getBlockByNumber" if p[0] == "finalized" => {
                    Ok(json!({"number": format!("0x{:x}", c.finalized.load(Ordering::Relaxed))}))
                }
                "eth_getBlockByNumber" => {
                    let b = crate::address_discovery::hex_u64(&p[0])?;
                    let answer = if b > c.latest() {
                        Value::Null
                    } else {
                        c.block_json(b)
                    };
                    c.after_read(b);
                    Ok(answer)
                }
                "eth_getCode" => Ok(json!("0x")),
                "eth_getTransactionCount" => Ok(json!("0x0")),
                "eth_getLogs" => Ok(json!([])),
                "eth_getBlockTransactionCountByNumber" => Ok(json!("0x1")),
                other => bail!("unexpected {other}"),
            });
            let c = self.clone();
            let trace = crate::address_partitions::tests::Fake::new(move |m, p| match m {
                "eth_getBlockByNumber" => {
                    let b = crate::address_discovery::hex_u64(&p[0])?;
                    Ok(if b > c.latest() {
                        Value::Null
                    } else {
                        c.block_json(b)
                    })
                }
                "trace_filter" => {
                    let f = &p[0];
                    let s = crate::address_discovery::hex_u64(&f["fromBlock"])?;
                    let e = crate::address_discovery::hex_u64(&f["toBlock"])?.min(c.latest());
                    let frames: Vec<Value> = if f.get("toAddress").is_some() {
                        (s..=e)
                            .filter(|b| c.row_at(*b))
                            .map(|b| {
                                json!({"transactionHash": c.hex(b), "blockNumber": b,
                                       "traceAddress": [0], "type": "call",
                                       "action": {"from": "0x03", "to": ALICE.to_ascii_lowercase(),
                                                  "value": "0x5", "callType": "call", "gas": "0x0"},
                                       "result": {"gasUsed": "0x0"}})
                            })
                            .collect()
                    } else {
                        Vec::new()
                    };
                    let depth = c.reorg_next.swap(0, Ordering::Relaxed);
                    if depth > 0 {
                        c.reorg(depth);
                    }
                    Ok(json!(frames))
                }
                "trace_transaction" => {
                    let tx = p[0].as_str().unwrap().to_ascii_lowercase();
                    let Some(b) = (0..=c.latest()).find(|b| c.row_at(*b) && c.hex(*b) == tx) else {
                        bail!("transaction {tx} not found");
                    };
                    let a = ALICE.to_ascii_lowercase();
                    Ok(json!([
                        {"traceAddress": [], "type": "call", "transactionHash": tx, "blockNumber": b,
                         "blockHash": c.hex(b), "transactionPosition": 0,
                         "action": {"from": "0x02", "to": "0x03", "value": "0x0", "callType": "call"}},
                        {"traceAddress": [0], "type": "call", "transactionHash": tx, "blockNumber": b,
                         "blockHash": c.hex(b), "transactionPosition": 0,
                         "action": {"from": "0x03", "to": a, "value": "0x5", "callType": "call",
                                    "gas": "0x0"},
                         "result": {"gasUsed": "0x0"}},
                    ]))
                }
                "trace_block" => Ok(json!([{"x": 1}])),
                other => bail!("unexpected {other}"),
            });
            (main, trace)
        }
    }

    /// Record `[from, to]` for ALICE as discovery would have read it from `chain` now, anchored at
    /// `to` when it is above the finalized head.
    fn cover_from(chain: &Chain, h: &AddressHistory, from: u64, to: u64) {
        use crate::address_history::{Action, Row};
        if to < from {
            return;
        }
        let rows: Vec<Row> = (from..=to)
            .filter(|b| chain.row_at(*b))
            .map(|b| {
                let mut record = serde_json::Map::new();
                record.insert("blockNumber".into(), json!(b.to_string()));
                record.insert(
                    "hash".into(),
                    json!(format!(
                        "0x{}",
                        alloy_primitives::hex::encode(chain.hash(b))
                    )),
                );
                Row {
                    block: b,
                    tx_index: 0,
                    position: 0,
                    record,
                }
            })
            .collect();
        let anchor = h
            .head()
            .unwrap()
            .is_none_or(|f| to > f)
            .then(|| (to, chain.hash(to)));
        let g = h.generation().unwrap();
        for action in crate::address_discovery::DISCOVERED {
            let rows = if action == Action::TxList {
                rows.clone()
            } else {
                Vec::new()
            };
            h.record_anchored(action, ALICE, &rows, (from, to), g, anchor)
                .unwrap();
        }
    }

    /// What the store holds for ALICE over its covered blocks, and what the chain says it should.
    fn held_and_wanted(chain: &Chain, h: &AddressHistory) -> (Vec<String>, Vec<String>) {
        held_and_wanted_of(chain, h, crate::address_history::Action::TxList)
    }

    fn held_and_wanted_of(
        chain: &Chain,
        h: &AddressHistory,
        action: crate::address_history::Action,
    ) -> (Vec<String>, Vec<String>) {
        let mut held = Vec::new();
        let mut wanted = Vec::new();
        for (f, t) in h.coverage(action, ALICE).unwrap() {
            assert!(t <= chain.latest(), "coverage past the chain head");
            for r in h.rows_in(action, ALICE, (f, t)).unwrap() {
                held.push(r["hash"].as_str().unwrap().to_string());
            }
            for b in (f..=t).filter(|b| chain.row_at(*b)) {
                wanted.push(format!(
                    "0x{}",
                    alloy_primitives::hex::encode(chain.hash(b))
                ));
            }
        }
        (held, wanted)
    }

    fn reorg_state(dir: &Path) -> Arc<ModeState> {
        let store = Store::open(&dir.join("t.redb")).unwrap();
        let history = AddressHistory::open(store, 1, &[ALICE.into()]).unwrap();
        Arc::new(ModeState::new(history, 1, Duration::from_secs(300), true).with_range(0, None, 20))
    }

    /// [`cover_from`] as the cursor's windows run it: anchored, and refused if the check after the
    /// fetch finds the chain moved.
    async fn fetch_from<M: Rpc, T: Rpc>(
        d: &Discoverer<Counted<M>, Counted<T>>,
        chain: &Chain,
        h: &AddressHistory,
        from: u64,
        to: u64,
        reorg_during: Option<u64>,
    ) {
        if to < from {
            return;
        }
        let anchor = anchor_for(d, h, to).await.unwrap();
        let rows_chain_hashes: Vec<[u8; 32]> = (0..=to).map(|b| chain.hash(b)).collect();
        if let Some(depth) = reorg_during {
            chain.reorg(depth);
        }
        if check_anchor(d, h, anchor, &[]).await.is_err() {
            return;
        }
        use crate::address_history::{Action, Row};
        let rows: Vec<Row> = (from..=to)
            .filter(|b| rows_chain_hashes[*b as usize][0].is_multiple_of(3))
            .map(|b| {
                let mut record = serde_json::Map::new();
                record.insert("blockNumber".into(), json!(b.to_string()));
                record.insert(
                    "hash".into(),
                    json!(format!(
                        "0x{}",
                        alloy_primitives::hex::encode(rows_chain_hashes[b as usize])
                    )),
                );
                Row {
                    block: b,
                    tx_index: 0,
                    position: 0,
                    record,
                }
            })
            .collect();
        let g = h.generation().unwrap();
        for action in crate::address_discovery::DISCOVERED {
            let rows = if action == Action::TxList {
                rows.clone()
            } else {
                Vec::new()
            };
            if h.record_anchored(action, ALICE, &rows, (from, to), g, anchor)
                .is_err()
            {
                return;
            }
        }
    }

    proptest::proptest! {
        #![proptest_config(proptest::prelude::ProptestConfig::with_cases(96))]
        /// Random reorgs of random depth above the chain's finalized tag, finality moving, windows read
        /// at the tip, reorgs landing between a window's fetch and its check, and polls, in any order
        /// and without a poll after every change: after the last poll, every covered block holds
        /// exactly the rows the canonical chain has (CLAUDE.md's reorg property).
        #[test]
        fn reorgs_of_any_depth_converge_to_the_canonical_chain(
            ops in proptest::collection::vec((0u8..5, 1u64..10), 1..48)
        ) {
            let rt = tokio::runtime::Builder::new_current_thread().enable_all().build().unwrap();
            rt.block_on(async {
                let dir = tempfile::tempdir().unwrap();
                let s = reorg_state(dir.path());
                let chain = Chain::new(60, 30);
                let d = chain.discoverer();
                d.reconcile(&s, chain.latest()).await.unwrap();
                let tail = |chain: &Chain| chain.latest() - chain.finalized.load(Ordering::Relaxed);
                for (op, n) in ops {
                    let next = crate::address_discovery::next_uncovered_for(
                        &s.history, ALICE, 0, &[crate::address_history::Action::TxList],
                    ).unwrap();
                    match op {
                        // A reorg never reaches the chain's finalized tag, which trails the head by 20.
                        0 => chain.reorg(n.min(tail(&chain))),
                        1 => {
                            chain.grow(n);
                            chain.finalized.store(chain.latest() - 20, Ordering::Relaxed);
                        }
                        2 => fetch_from(&d, &chain, &s.history, next, chain.latest(), None).await,
                        3 => {
                            let depth = n.min(tail(&chain));
                            fetch_from(&d, &chain, &s.history, next, chain.latest(), Some(depth)).await
                        }
                        _ => {
                            d.reconcile(&s, chain.latest()).await.unwrap();
                        }
                    }
                }
                d.reconcile(&s, chain.latest()).await.unwrap();
                let (held, wanted) = held_and_wanted(&chain, &s.history);
                proptest::prop_assert_eq!(held, wanted);
                proptest::prop_assert!(s.cursor_error.lock().unwrap().is_none());
                Ok(())
            })?;
        }
    }

    proptest::proptest! {
        #![proptest_config(proptest::prelude::ProptestConfig::with_cases(64))]
        /// The same property through the cursor's own window, [`discover_window`], against a node
        /// whose traces follow the chain, with reorgs that may also cross finality. One that reaches
        /// a finalized head the store committed stops the cursor and refuses every answer; short of
        /// that, after the last poll every covered block holds exactly the canonical rows, and the
        /// run must have covered something, so a fetch path that records nothing cannot pass.
        #[test]
        fn reorgs_through_the_cursors_window_converge_or_stop(
            ops in proptest::collection::vec((0u8..6, 1u64..12), 1..40)
        ) {
            use crate::address_history::Action;
            let rt = tokio::runtime::Builder::new_current_thread().enable_all().build().unwrap();
            rt.block_on(async {
                let dir = tempfile::tempdir().unwrap();
                let s = reorg_state(dir.path());
                let chain = Chain::new(60, 30);
                let d = chain.node();
                d.reconcile(&s, chain.latest()).await.unwrap();
                let tail = |chain: &Chain| chain.latest() - chain.finalized.load(Ordering::Relaxed);
                let window = |s: &ModeState, chain: &Chain| {
                    let next = crate::address_discovery::next_uncovered_for(
                        &s.history, ALICE, 0, &[Action::TxListInternal],
                    ).unwrap();
                    (next, chain.latest())
                };
                let mut crossed = false;
                for (op, n) in ops {
                    match op {
                        0 => chain.reorg(n.min(tail(&chain))),
                        1 => {
                            chain.grow(n);
                            chain.finalized.store(chain.latest() - 20, Ordering::Relaxed);
                        }
                        2 => {
                            let w = window(&s, &chain);
                            if w.0 <= w.1 {
                                let _ = discover_window(&d, &s, ALICE, w, None).await;
                            }
                        }
                        3 => {
                            let w = window(&s, &chain);
                            if w.0 <= w.1 {
                                chain.reorg_next.store(n.min(tail(&chain)), Ordering::Relaxed);
                                let _ = discover_window(&d, &s, ALICE, w, None).await;
                                chain.reorg_next.store(0, Ordering::Relaxed);
                            }
                        }
                        4 => d.reconcile(&s, chain.latest()).await.map(|_| ()).unwrap(),
                        _ => {
                            // Past the chain's own finalized tag: the replaced blocks start at
                            // `point`, a finalized block the store may already have committed.
                            let depth = (tail(&chain) + n).min(chain.latest());
                            let point = chain.latest() + 1 - depth;
                            chain.reorg(depth);
                            let committed = s.history.finals().unwrap().last().map(|f| f.0);
                            if committed.is_some_and(|f| point <= f) {
                                crossed = true;
                                break;
                            }
                        }
                    }
                }
                if crossed {
                    let err = d.reconcile(&s, chain.latest()).await.unwrap_err();
                    proptest::prop_assert!(err.to_string().contains("finalized"), "{err:#}");
                    let q = format!("/api?module=account&action=txlistinternal&address={ALICE}&startblock=0&endblock=10");
                    let (_, a) = call(router(s.clone()), "GET", &q, None).await;
                    let r = a["result"].as_str().unwrap_or_default().to_string();
                    proptest::prop_assert!(r.starts_with("NUTHATCH_INCOMPLETE:") && r.contains("finalized"), "{a}");
                    let (st, _) = call(router(s.clone()), "GET", "/ready", None).await;
                    proptest::prop_assert_eq!(st, StatusCode::SERVICE_UNAVAILABLE);
                    return Ok(());
                }
                d.reconcile(&s, chain.latest()).await.unwrap();
                let w = window(&s, &chain);
                if w.0 <= w.1 {
                    discover_window(&d, &s, ALICE, w, None).await.unwrap();
                }
                d.reconcile(&s, chain.latest()).await.unwrap();
                let (held, wanted) = held_and_wanted_of(&chain, &s.history, Action::TxListInternal);
                proptest::prop_assert!(!s.history.coverage(Action::TxListInternal, ALICE).unwrap().is_empty());
                proptest::prop_assert!(!wanted.is_empty(), "the chain gave ALICE no rows to hold");
                proptest::prop_assert_eq!(held, wanted);
                proptest::prop_assert!(s.cursor_error.lock().unwrap().is_none());
                Ok(())
            })?;
        }
    }

    /// A fork switch during a poll, before the new finalized head's hash is read or after the checks
    /// that would have seen it, never records a hash the checks did not hold: the poll fails, the
    /// next one takes back what was read on the old fork, and no false conflict follows.
    #[tokio::test]
    async fn a_fork_switch_during_a_poll_never_records_an_unchecked_finalized_hash() {
        // Block 80 read once (the new head's first read), or 99 read once (the tracked check).
        for (at, n) in [(80, 1), (99, 1)] {
            let dir = tempfile::tempdir().unwrap();
            let s = reorg_state(dir.path());
            let chain = Chain::new(100, 70);
            let d = chain.discoverer();
            d.reconcile(&s, chain.latest()).await.unwrap();
            cover_from(&chain, &s.history, 0, 99);
            chain.grow(10);
            chain.finalized.store(80, Ordering::Relaxed);
            // 75 onward replaced: past the new finalized head, above the recorded one.
            *chain.reorg_on_read.lock().unwrap() = Some((at, n, 35));
            let err = d.reconcile(&s, chain.latest()).await.unwrap_err();
            assert!(s.conflict.lock().unwrap().is_none(), "({at}, {n}): {err:#}");
            d.reconcile(&s, chain.latest()).await.unwrap();
            d.reconcile(&s, chain.latest()).await.unwrap();
            let (held, wanted) = held_and_wanted(&chain, &s.history);
            assert_eq!(held, wanted, "({at}, {n})");
            assert_eq!(
                s.history.finals().unwrap().last().unwrap(),
                &(80, chain.hash(80))
            );
        }
    }

    /// A switch to a fork under finality landing after a request's own finality check, but before
    /// its fetch, cannot join that fetch to history read on the old fork: the fetch is held against
    /// the newest finalized hash as well, fails, and the next poll stops the cursor.
    #[tokio::test]
    async fn a_fork_under_finality_after_the_request_check_never_joins_two_forks() {
        let dir = tempfile::tempdir().unwrap();
        let s = reorg_state(dir.path());
        let chain = Chain::new(100, 70);
        let d = chain.node();
        d.reconcile(&s, chain.latest()).await.unwrap();
        discover_window(&d, &s, ALICE, (0, 99), None).await.unwrap();
        chain.grow(10);
        chain.finalized.store(90, Ordering::Relaxed);
        d.reconcile(&s, chain.latest()).await.unwrap();
        // The request's check reads finalized 90 on the old fork; then 75 onward is replaced.
        *chain.reorg_on_read.lock().unwrap() = Some((90, 1, 35));
        d.check_tracked(&s, chain.latest()).await.unwrap();
        assert!(
            chain.reorg_on_read.lock().unwrap().is_none(),
            "the switch landed"
        );
        let err = discover_window(&d, &s, ALICE, (91, chain.latest()), None)
            .await
            .unwrap_err();
        assert!(err.to_string().contains("block 90"), "{err:#}");
        assert_eq!(
            s.history
                .coverage(crate::address_history::Action::TxListInternal, ALICE)
                .unwrap(),
            vec![(0, 90)]
        );
        // Refused at once, before any poll.
        let q = format!(
            "/api?module=account&action=txlistinternal&address={ALICE}&startblock=0&endblock=80"
        );
        let (_, a) = call(router(s.clone()), "GET", &q, None).await;
        assert!(
            a["result"].as_str().unwrap().contains("finalized block 90"),
            "{a}"
        );
        let (st, _) = call(router(s.clone()), "GET", "/ready", None).await;
        assert_eq!(st, StatusCode::SERVICE_UNAVAILABLE);
    }

    /// A fetch whose own anchor no longer holds, because the chain switched to a fork under
    /// finality, records the conflict at once rather than failing as an ordinary reorg.
    #[tokio::test]
    async fn a_failed_anchor_over_a_fork_under_finality_is_a_conflict() {
        let dir = tempfile::tempdir().unwrap();
        let s = reorg_state(dir.path());
        let chain = Chain::new(100, 90);
        let d = chain.node();
        d.reconcile(&s, chain.latest()).await.unwrap();
        let anchor = Some((99, chain.hash(99)));
        chain.reorg(25);
        let err = check_anchor(&d, &s.history, anchor, &[]).await.unwrap_err();
        assert!(err.to_string().contains("finalized block 90"), "{err:#}");
        let (st, _) = call(router(s.clone()), "GET", "/ready", None).await;
        assert_eq!(st, StatusCode::SERVICE_UNAVAILABLE);
    }

    /// One endpoint of a pool on another fork under finality is a conflict, though the endpoint a
    /// plain call reaches agrees, and though another endpoint failed or has no such block.
    #[tokio::test]
    async fn one_pool_endpoint_on_another_fork_is_a_conflict() {
        struct Pool(u8);
        impl Rpc for Pool {
            async fn call(&self, _: &str, _: Value) -> Result<Value> {
                Ok(json!({"hash": format!("0x{}", "11".repeat(32))}))
            }
            async fn visit_each(
                &self,
                m: &str,
                p: Value,
                seen: &mut (dyn FnMut(Result<Value>) + Send),
            ) {
                let other = || Ok(json!({"hash": format!("0x{}", "22".repeat(32))}));
                match self.0 {
                    0 => {
                        seen(self.call(m, p).await);
                        seen(other());
                    }
                    1 => {
                        seen(other());
                        seen(Err(anyhow::anyhow!("timed out")));
                    }
                    2 => {
                        seen(Ok(Value::Null));
                        seen(other());
                    }
                    // The mismatch, then an endpoint that does not answer before the deadline.
                    _ => {
                        seen(other());
                        tokio::time::sleep(Duration::from_secs(3600)).await;
                    }
                }
            }
        }
        for case in 0..4 {
            let dir = tempfile::tempdir().unwrap();
            let s = reorg_state(dir.path());
            s.history.finalize_at(90, [0x11; 32]).unwrap();
            let deadline = Some(Instant::now() + Duration::from_millis(200));
            let err = within(deadline, hold_final(&Pool(case), &s.history))
                .await
                .unwrap_err();
            let want = if case == 3 {
                "deadline"
            } else {
                "finalized block 90"
            };
            assert!(err.to_string().contains(want), "{case}: {err:#}");
            drop(s);
            let again = reorg_state(dir.path());
            assert!(
                again.conflicted().is_some(),
                "{case}: kept across a restart"
            );
        }
    }

    /// Endpoints that disagree about a block the chain calls finalized, above what the store
    /// committed, hold the head back rather than commit one side or call it a conflict.
    #[tokio::test]
    async fn endpoints_split_over_a_new_finalized_head_hold_it_back() {
        struct Split(crate::address_partitions::tests::Fake, u64);
        impl Rpc for Split {
            async fn call(&self, m: &str, p: Value) -> Result<Value> {
                self.0.call(m, p).await
            }
            async fn call_each(&self, m: &str, p: Value) -> Result<Vec<Value>> {
                let a = self.0.call(m, p.clone()).await?;
                let mut b = a.clone();
                if m == "eth_getBlockByNumber" && p[0] == json!(format!("0x{:x}", self.1)) {
                    b["hash"] = json!(format!("0x{}", "ee".repeat(32)));
                }
                Ok(vec![a, b])
            }
        }
        let dir = tempfile::tempdir().unwrap();
        let s = reorg_state(dir.path());
        let chain = Chain::new(100, 70);
        let (main, trace) = chain.node_rpcs();
        let d = Discoverer::new(Counted::new(Split(main, 80)), Counted::new(trace));
        d.reconcile(&s, chain.latest()).await.unwrap();
        chain.grow(10);
        chain.finalized.store(80, Ordering::Relaxed);
        let err = d.reconcile(&s, chain.latest()).await.unwrap_err();
        assert!(
            err.to_string().contains("do not agree on block 80"),
            "{err:#}"
        );
        assert_eq!(s.history.head().unwrap(), Some(70));
        assert!(s.conflicted().is_none());
    }

    /// A store from before finalized hashes were recorded holds its newest stored transaction's block
    /// hash before recording its first: on the same chain the upgrade goes through, and over a fork
    /// under finality it is a conflict rather than a hash that would bless the old fork's rows.
    #[tokio::test]
    async fn an_upgraded_store_holds_its_stored_hashes_before_its_first_final() {
        for forked in [false, true] {
            let dir = tempfile::tempdir().unwrap();
            let s = reorg_state(dir.path());
            let chain = Chain::new(100, 70);
            let mut record = serde_json::Map::new();
            record.insert("blockNumber".into(), json!("60"));
            record.insert("blockHash".into(), json!(chain.hex(60)));
            let row = crate::address_history::Row {
                block: 60,
                tx_index: 0,
                position: 0,
                record,
            };
            let g = s.history.generation().unwrap();
            s.history
                .record(
                    crate::address_history::Action::TxList,
                    ALICE,
                    &[row],
                    (0, 70),
                    g,
                )
                .unwrap();
            s.history.finalize(70).unwrap();
            assert!(s.history.finals().unwrap().is_empty(), "a 5.1 store");
            if forked {
                chain.reorg(45);
            }
            let d = chain.node();
            let polled = d.reconcile(&s, chain.latest()).await;
            if forked {
                let err = polled.unwrap_err();
                assert!(err.to_string().contains("finalized block 60"), "{err:#}");
                assert!(
                    s.history.finals().unwrap().is_empty(),
                    "nothing recorded over it"
                );
                let (st, _) = call(router(s.clone()), "GET", "/ready", None).await;
                assert_eq!(st, StatusCode::SERVICE_UNAVAILABLE);
            } else {
                polled.unwrap();
                assert_eq!(s.history.finals().unwrap(), vec![(70, chain.hash(70))]);
            }
        }
    }

    /// A finalized tag that went backwards does not narrow the poll's check: the recorded finalized
    /// hash is held again after the tracked check, so a switch to a fork under it during the poll is
    /// a conflict rather than a rollback that keeps the old fork's blocks under the committed head.
    #[tokio::test]
    async fn a_regressing_finalized_tag_still_holds_the_recorded_head() {
        let dir = tempfile::tempdir().unwrap();
        let s = reorg_state(dir.path());
        let chain = Chain::new(130, 100);
        let d = chain.node();
        d.reconcile(&s, chain.latest()).await.unwrap();
        discover_window(&d, &s, ALICE, (0, 129), None)
            .await
            .unwrap();
        chain.finalized.store(80, Ordering::Relaxed);
        // 95 onward replaced once the poll's finality check has read 100.
        *chain.reorg_on_read.lock().unwrap() = Some((100, 1, 35));
        let err = d.reconcile(&s, chain.latest()).await.unwrap_err();
        assert!(err.to_string().contains("finalized block 100"), "{err:#}");
        assert!(s.conflicted().is_some());
    }

    /// A finalized hash already recorded is never replaced: another one is a conflict, and the head
    /// does not move.
    #[test]
    fn a_recorded_finalized_hash_is_never_replaced() {
        let dir = tempfile::tempdir().unwrap();
        let s = reorg_state(dir.path());
        s.history.finalize_at(80, [1; 32]).unwrap();
        s.history.finalize_at(80, [1; 32]).unwrap();
        let err = s
            .history
            .finalize_at(90, [2; 32])
            .and_then(|_| s.history.finalize_at(90, [3; 32]));
        assert!(err
            .unwrap_err()
            .downcast_ref::<crate::address_history::FinalConflict>()
            .is_some());
        assert_eq!(
            s.history.finals().unwrap(),
            vec![(80, [1; 32]), (90, [2; 32])]
        );
        assert_eq!(s.history.head().unwrap(), Some(90));
    }

    /// A reorg that replaces a finalized head the store committed, even with nothing tracked there,
    /// stops the cursor at the next poll and every answer is refused with the reason, as is an RPC
    /// that comes back on another chain under the same id.
    #[tokio::test]
    async fn a_reorg_under_finality_or_another_chain_stops_the_cursor() {
        for depth in [35, 100] {
            let dir = tempfile::tempdir().unwrap();
            let s = reorg_state(dir.path());
            let chain = Chain::new(100, 70);
            let d = chain.node();
            d.reconcile(&s, chain.latest()).await.unwrap();
            discover_window(&d, &s, ALICE, (0, 70), None).await.unwrap();
            let q = format!(
                "/api?module=account&action=txlistinternal&address={ALICE}&startblock=0&endblock=70"
            );
            let (_, before) = call(router(s.clone()), "GET", &q, None).await;
            assert_eq!(before["status"], "1", "{before}");
            // 65 to 99 replaced, under the finalized 70; or every block, another chain entirely.
            chain.reorg(depth);
            let err = d.reconcile(&s, chain.latest()).await.unwrap_err();
            assert!(err.to_string().contains("finalized block 70"), "{err:#}");
            let (_, after) = call(router(s.clone()), "GET", &q, None).await;
            let r = after["result"].as_str().unwrap();
            assert!(
                r.starts_with("NUTHATCH_INCOMPLETE:") && r.contains("reorg under finality"),
                "{after}"
            );
            let (st, ready) = call(router(s.clone()), "GET", "/ready", None).await;
            assert_eq!(st, StatusCode::SERVICE_UNAVAILABLE, "{ready}");
            assert!(ready.to_string().contains("finalized block 70"), "{ready}");
            // A restart over the same store, with no RPC to ask, still refuses.
            drop(s);
            let again = reorg_state(dir.path());
            let (_, after) = call(router(again.clone()), "GET", &q, None).await;
            let r = after["result"].as_str().unwrap();
            assert!(r.contains("reorg under finality"), "{after}");
            let (st, _) = call(router(again), "GET", "/ready", None).await;
            assert_eq!(st, StatusCode::SERVICE_UNAVAILABLE);
        }
    }

    /// A chain whose head fell under the newest tracked block (a reorg to a shorter fork) cannot show
    /// that block still stands, so what was read above the newest one it can show is taken back.
    #[tokio::test]
    async fn a_head_fallen_under_the_newest_anchor_takes_back_what_it_cannot_show() {
        let dir = tempfile::tempdir().unwrap();
        let s = reorg_state(dir.path());
        let chain = Chain::new(121, 100);
        let d = chain.discoverer();
        d.reconcile(&s, chain.latest()).await.unwrap();
        cover_from(&chain, &s.history, 0, 110);
        cover_from(&chain, &s.history, 111, 120);
        // A shorter fork from 112: 110 still stands, but the chain now ends at 115, under 120.
        chain.hashes.lock().unwrap().truncate(112);
        chain.grow(4);
        assert_eq!(chain.latest(), 115);
        d.check_tracked(&s, chain.latest()).await.unwrap();
        assert_eq!(
            s.history
                .coverage(crate::address_history::Action::TxList, ALICE)
                .unwrap(),
            vec![(0, 110)],
            "110 still matches; 120 is out of reach, so what was read above 110 goes"
        );
    }

    /// A read reaching above the finalized head is checked against the chain before it is served: a
    /// reorg since the last poll is taken back first, so the replaced rows are never answered, and
    /// the range is refused until it is read again.
    #[tokio::test]
    async fn a_reorg_after_serving_is_found_before_the_next_answer() {
        let dir = tempfile::tempdir().unwrap();
        let s = reorg_state(dir.path());
        let chain = Chain::new(100, 70);
        let d = Arc::new(chain.discoverer());
        d.reconcile(&s, chain.latest()).await.unwrap();
        cover_from(&chain, &s.history, 0, 99);
        let cursor = tokio::spawn(serve_forever(s.clone(), d.clone()));
        let q = |end: u64| {
            format!("/api?module=account&action=txlist&address={ALICE}&startblock=0&endblock={end}")
        };
        let (_, before) = call(router(s.clone()), "GET", &q(99), None).await;
        assert_eq!(before["status"], "1", "{before}");

        // No poll between the reorg and the next request.
        chain.reorg(5);
        let (_, during) = call(router(s.clone()), "GET", &q(99), None).await;
        assert!(
            during["result"]
                .as_str()
                .unwrap()
                .starts_with("NUTHATCH_INCOMPLETE:"),
            "{during}"
        );
        assert_eq!(
            s.history
                .coverage(crate::address_history::Action::TxList, ALICE)
                .unwrap(),
            vec![(0, 70)],
            "the window was anchored at 99 alone, so all of it above the finalized head goes"
        );

        cover_from(&chain, &s.history, 71, 99);
        let (held, wanted) = held_and_wanted(&chain, &s.history);
        assert_eq!(held, wanted);
        let (_, after) = call(router(s.clone()), "GET", &q(99), None).await;
        assert_eq!(
            after["result"].as_array().unwrap().len(),
            wanted.len(),
            "{after}"
        );
        cursor.abort();
    }

    /// A window read on a fork that replaced a tracked block must not be recorded: its own anchor
    /// would match the chain at the next poll and hide the stale block under it.
    #[tokio::test]
    async fn a_window_on_a_fork_under_its_anchor_is_refused() {
        let dir = tempfile::tempdir().unwrap();
        let s = reorg_state(dir.path());
        let chain = Chain::new(112, 100);
        let d = chain.discoverer();
        d.reconcile(&s, chain.latest()).await.unwrap();
        cover_from(&chain, &s.history, 0, 110);
        // The reorg lands before the window's anchor is read, so the anchor itself is on the fork
        // and matches; only the tracked block under it says the window is not on the chain the
        // store holds.
        chain.reorg(8);
        let anchor = anchor_for(&d, &s.history, 111).await.unwrap();
        let err = check_anchor(&d, &s.history, anchor, &[]).await.unwrap_err();
        assert!(err.to_string().contains("block 110"), "{err:#}");
    }

    /// The tracked hashes outlive the process: a nest restarted after a reorg it never saw finds it
    /// at its first poll.
    #[tokio::test]
    async fn a_reorg_during_a_restart_is_found_at_the_first_poll() {
        let dir = tempfile::tempdir().unwrap();
        let chain = Chain::new(100, 70);
        let d = chain.discoverer();
        {
            let s = reorg_state(dir.path());
            d.reconcile(&s, chain.latest()).await.unwrap();
            cover_from(&chain, &s.history, 0, 99);
            assert_eq!(s.history.tracked().unwrap(), vec![(99, chain.hash(99))]);
        }
        chain.reorg(3);
        let s = reorg_state(dir.path());
        d.reconcile(&s, chain.latest()).await.unwrap();
        assert_eq!(
            s.history
                .coverage(crate::address_history::Action::TxList, ALICE)
                .unwrap(),
            vec![(0, 70)],
            "the only anchor moved, so everything above the finalized head goes"
        );
        assert!(s.history.tracked().unwrap().is_empty());
    }

    /// A reorg while the nest was down, which the chain has since finalized past, is still a reorg of
    /// blocks that were mutable when they were read: it is taken back, not mistaken for a conflict.
    #[tokio::test]
    async fn a_reorg_finalized_during_downtime_is_taken_back() {
        let dir = tempfile::tempdir().unwrap();
        let s = reorg_state(dir.path());
        let chain = Chain::new(100, 70);
        let d = chain.discoverer();
        d.reconcile(&s, chain.latest()).await.unwrap();
        cover_from(&chain, &s.history, 0, 80);
        chain.reorg(25);
        chain.grow(20);
        chain.finalized.store(100, Ordering::Relaxed);
        assert_eq!(d.reconcile(&s, chain.latest()).await.unwrap(), 100);
        assert!(s.cursor_error.lock().unwrap().is_none());
        assert_eq!(
            s.history
                .coverage(crate::address_history::Action::TxList, ALICE)
                .unwrap(),
            vec![(0, 70)]
        );
        assert_eq!(s.history.head().unwrap(), Some(100));
    }

    /// A fetch anchored below the newest tracked block is checked against that block too: on a fork
    /// that replaced it, the new anchor would match and the old rows beneath it survive.
    #[tokio::test]
    async fn a_fetch_below_the_newest_anchor_is_checked_against_it() {
        let dir = tempfile::tempdir().unwrap();
        let s = reorg_state(dir.path());
        let chain = Chain::new(121, 100);
        let d = chain.discoverer();
        d.reconcile(&s, chain.latest()).await.unwrap();
        cover_from(&chain, &s.history, 0, 120);
        // A fork from 118: block 119 on it is what a trace now names.
        chain.reorg(3);
        let named = [(119, chain.hash(119))];
        let err = check_anchor(&d, &s.history, None, &named)
            .await
            .unwrap_err();
        assert!(err.to_string().contains("block 120"), "{err:#}");
    }

    /// A trace endpoint on another fork, or behind, than the main endpoint answers ranges the main
    /// chain does not have; a window whose last block the two disagree on is refused.
    #[tokio::test]
    async fn a_trace_endpoint_on_another_chain_is_refused() {
        let chain = Chain::new(120, 100);
        let other = Chain::new(120, 100);
        other.seed.store(1_000, Ordering::Relaxed);
        other.reorg(20);
        let main = chain.discoverer().main;
        let trace = other.discoverer().main;
        let d = Discoverer::new(main, trace);
        let err = same_chain_at(&d, 119).await.unwrap_err();
        assert!(err.to_string().contains("not all on one chain"), "{err:#}");
        assert!(
            same_chain_at(&d, 99).await.is_ok(),
            "below the fork they agree"
        );
    }

    /// A pool rotates calls across its endpoints, so every endpoint must be on one chain, not only
    /// the one a single call reached.
    #[tokio::test]
    async fn every_endpoint_in_a_pool_must_agree() {
        struct Pool(Vec<&'static str>);
        impl Rpc for Pool {
            async fn call(&self, _: &str, _: Value) -> Result<Value> {
                Ok(json!({"hash": self.0[0]}))
            }
            async fn call_each(&self, _: &str, _: Value) -> Result<Vec<Value>> {
                Ok(self.0.iter().map(|h| json!({"hash": h})).collect())
            }
        }
        let agree = Discoverer::new(
            Counted::new(Pool(vec!["0xb"])),
            Counted::new(Pool(vec!["0xb", "0xb"])),
        );
        assert!(same_chain_at(&agree, 7).await.is_ok());
        let split = Discoverer::new(
            Counted::new(Pool(vec!["0xb"])),
            Counted::new(Pool(vec!["0xb", "0xa"])),
        );
        assert!(same_chain_at(&split, 7).await.is_err());
    }

    /// An answer read before a reorg was taken back is read again, not served.
    #[tokio::test]
    async fn an_answer_from_a_moved_generation_is_read_again() {
        let dir = tempfile::tempdir().unwrap();
        let s = reorg_state(dir.path());
        let chain = Chain::new(100, 70);
        let d = chain.discoverer();
        d.reconcile(&s, chain.latest()).await.unwrap();
        cover_from(&chain, &s.history, 0, 60);
        let q: HashMap<String, String> = [
            ("module", "account"),
            ("action", "txlist"),
            ("address", ALICE),
            ("startblock", "0"),
            ("endblock", "60"),
        ]
        .into_iter()
        .map(|(k, v)| (k.to_string(), v.to_string()))
        .collect();
        let answer = respond(&s, &q).await;
        assert!(matches!(wanted(&s, &q, &answer).await, Want::Serve));
        s.history.invalidate_above(80).unwrap();
        assert!(matches!(wanted(&s, &q, &answer).await, Want::Reread));
    }

    /// A window's fetch stops at the request's deadline, writing nothing; one without a deadline runs.
    #[tokio::test]
    async fn a_fetch_stops_at_its_deadline() {
        let slow = || async {
            tokio::time::sleep(Duration::from_secs(5)).await;
            Ok::<_, anyhow::Error>(1)
        };
        let started = Instant::now();
        let err = within(Some(Instant::now() + Duration::from_millis(50)), slow())
            .await
            .unwrap_err();
        assert!(err.to_string().contains("deadline"), "{err:#}");
        assert!(started.elapsed() < Duration::from_secs(1));
        assert_eq!(
            within(None, async { Ok::<_, anyhow::Error>(2) })
                .await
                .unwrap(),
            2
        );
    }

    /// A window's whole read phase, checks included, stops at the request's deadline and leaves
    /// nothing behind, so a slow RPC cannot hold the cursor past it.
    #[tokio::test]
    async fn a_window_past_its_deadline_releases_the_cursor_and_writes_nothing() {
        struct Slow;
        impl Rpc for Slow {
            async fn call(&self, _: &str, _: Value) -> Result<Value> {
                tokio::time::sleep(Duration::from_secs(10)).await;
                Ok(Value::Null)
            }
        }
        let dir = tempfile::tempdir().unwrap();
        let s = reorg_state(dir.path());
        s.history.finalize(100).unwrap();
        let d = Discoverer::new(Counted::new(Slow), Counted::new(Slow));
        let started = Instant::now();
        let deadline = Some(Instant::now() + Duration::from_millis(100));
        let err = discover_window(&d, &s, ALICE, (101, 120), deadline)
            .await
            .unwrap_err();
        assert!(err.to_string().contains("deadline"), "{err:#}");
        let err = body_window(&d, &s, vec![ALICE.to_string()], (101, 120), deadline)
            .await
            .unwrap_err();
        assert!(err.to_string().contains("deadline"), "{err:#}");
        assert!(
            started.elapsed() < Duration::from_secs(2),
            "{:?}",
            started.elapsed()
        );
        for action in crate::address_history::Action::ALL {
            assert!(s.history.coverage(action, ALICE).unwrap().is_empty());
        }
        assert!(s.history.tracked().unwrap().is_empty());
    }

    /// A produced-blocks answer read before a re-ask committed is read again, and one whose range
    /// still has a check owed goes to the cursor: neither is served as settled.
    #[tokio::test]
    async fn a_produced_answer_is_served_only_when_settled_and_current() {
        use crate::address_history::Row;
        let dir = tempfile::tempdir().unwrap();
        let s = reorg_state(dir.path());
        s.history.finalize(1_000).unwrap();
        let g = s.history.generation().unwrap();
        let long_ago = unix_now() - 3 * 24 * 3600;
        s.history
            .record_produced(ALICE, &[], (100, 199), g, &[150], long_ago)
            .unwrap();
        let q: HashMap<String, String> = [
            ("module", "account"),
            ("action", "nuthatchProducedBlocks"),
            ("address", ALICE),
            ("startblock", "100"),
            ("endblock", "199"),
        ]
        .into_iter()
        .map(|(k, v)| (k.to_string(), v.to_string()))
        .collect();
        let answer = respond(&s, &q).await;
        assert!(
            matches!(wanted(&s, &q, &answer).await, Want::Cover(_)),
            "a check is owed"
        );

        // The re-ask commits after the answer was read: the answer is stale.
        let mut record = serde_json::Map::new();
        record.insert("delivered".into(), json!([]));
        let mut row = serde_json::Map::new();
        row.insert("blockNumber".into(), json!("150"));
        row.insert("mev".into(), json!("relay"));
        let rows = vec![Row {
            block: 150,
            tx_index: 0,
            position: 0,
            record: row,
        }];
        let g = s.history.generation().unwrap();
        s.history
            .apply_reask(
                150,
                &record,
                &[(ALICE.to_lowercase(), rows)],
                false,
                unix_now(),
                g,
            )
            .unwrap();
        assert!(matches!(wanted(&s, &q, &answer).await, Want::Reread));
        let fresh = respond(&s, &q).await;
        assert!(matches!(wanted(&s, &q, &fresh).await, Want::Serve));
        assert_eq!(fresh["result"][0]["mev"], "relay");
    }

    /// A failing RPC shows on /ready at once as stalled, though covered history still serves; the
    /// RPC calls counter is current between polls; a refusal always names its reason.
    #[tokio::test]
    async fn a_failing_rpc_shows_at_once_and_refusals_name_their_reason() {
        let dir = tempfile::tempdir().unwrap();
        let s = state(dir.path(), true);
        let source = fixed(100, 1);
        step(&s, &source, &()).await.unwrap();
        let (_, r) = call(router(s.clone()), "GET", "/ready", None).await;
        assert_eq!(r["stalled"], false, "{r}");
        source.head.store(0, Ordering::Relaxed);
        assert!(step(&s, &source, &()).await.is_err());
        let (_, r) = call(router(s.clone()), "GET", "/ready", None).await;
        assert_eq!(
            (r["ready"].clone(), r["stalled"].clone()),
            (json!(true), json!(true)),
            "{r}"
        );

        s.set_calls("main", BTreeMap::from([("eth_getLogs".to_string(), 40)]));
        let (_, m) = call(router(s.clone()), "GET", "/metrics", None).await;
        assert!(
            m.as_str()
                .unwrap()
                .contains("nuthatch_address_history_rpc_requests_total 40\n"),
            "{m}"
        );

        // A covered read above the finalized head goes to the cursor; with none running it times
        // out, and the refusal says so without an empty reason before it.
        s.history.set_head(50).unwrap();
        s.history
            .record(
                crate::address_history::Action::TxList,
                ALICE,
                &[],
                (0, 100),
                s.history.generation().unwrap(),
            )
            .unwrap();
        let s2 = Arc::new(
            ModeState::new(s.history.clone(), 1, Duration::from_secs(300), true)
                .with_deadline(Duration::from_millis(50)),
        );
        let q =
            format!("/api?module=account&action=txlist&address={ALICE}&startblock=0&endblock=100");
        let (_, a) = call(router(s2), "GET", &q, None).await;
        let r = a["result"].as_str().unwrap();
        assert!(r.starts_with("NUTHATCH_INCOMPLETE: not covered"), "{a}");
    }

    /// A tracked block under the committed finalized head, now at another hash, is not rewritten:
    /// the cursor stops and says why, and /ready reports it.
    #[tokio::test]
    async fn a_conflict_under_the_committed_finalized_head_stops_the_cursor() {
        let dir = tempfile::tempdir().unwrap();
        let s = reorg_state(dir.path());
        let chain = Chain::new(100, 70);
        let d = chain.discoverer();
        d.reconcile(&s, chain.latest()).await.unwrap();
        cover_from(&chain, &s.history, 0, 80);
        // A store whose committed head passed a block it still tracks: not a state the cursor
        // writes, but the check must hold if one is ever found.
        s.history.set_head(90).unwrap();
        chain.reorg(25);
        let err = d.reconcile(&s, chain.latest()).await.unwrap_err();
        assert!(err.to_string().contains("finalized head"), "{err:#}");
        let (st, r) = call(router(s.clone()), "GET", "/ready", None).await;
        assert_eq!(st, StatusCode::SERVICE_UNAVAILABLE, "{r}");
    }

    /// A request the cursor cannot get to within the deadline answers incomplete when the deadline
    /// passes, queue time included, and the nest does not answer half of it.
    #[tokio::test]
    async fn a_request_past_its_deadline_answers_incomplete() {
        let dir = tempfile::tempdir().unwrap();
        let store = Store::open(&dir.path().join("t.redb")).unwrap();
        let history = AddressHistory::open(store, 1, &[ALICE.into()]).unwrap();
        history.finalize(100).unwrap();
        history
            .record(
                crate::address_history::Action::TxList,
                ALICE,
                &[],
                (0, 100),
                0,
            )
            .unwrap();
        let s = Arc::new(
            ModeState::new(history, 1, Duration::from_secs(300), true)
                .with_deadline(Duration::from_millis(200)),
        );
        // No cursor is running, so nothing takes the request off the queue.
        let started = Instant::now();
        let q =
            format!("/api?module=account&action=txlist&address={ALICE}&startblock=0&endblock=150");
        let (_, a) = call(router(s.clone()), "GET", &q, None).await;
        let took = started.elapsed();
        let r = a["result"].as_str().unwrap();
        assert!(
            r.starts_with("NUTHATCH_INCOMPLETE:") && r.contains("deadline"),
            "{a}"
        );
        assert!(
            took >= Duration::from_millis(200) && took < Duration::from_secs(2),
            "{took:?}"
        );
    }

    /// A made-up chain for timestamp search: block b at timestamp 1000 + 12 b, `head` blocks long,
    /// each linked to its parent unless `broken` names a block whose parent link is wrong.
    fn timed(head: u64, broken: Option<u64>) -> crate::address_proxy::Forward {
        Arc::new(move |method: String, params: Value| {
            Box::pin(async move {
                assert_eq!(method, "eth_getBlockByNumber");
                let b = match params[0].as_str().unwrap() {
                    "latest" => head,
                    n => crate::address_discovery::hex_u64(&json!(n))?,
                };
                let hash = |n: u64| format!("0x{n:064x}");
                let parent = if Some(b) == broken {
                    hash(999_999)
                } else {
                    hash(b.saturating_sub(1))
                };
                Ok(
                    json!({"number": format!("0x{b:x}"), "timestamp": format!("0x{:x}", 1000 + 12 * b),
                          "hash": hash(b), "parentHash": parent}),
                )
            })
        })
    }

    /// An answer above the finalized head stands only if parent hashes link it to the head the
    /// search began from, so a node still on a fork cannot hand rotki a boundary on it.
    #[tokio::test]
    async fn a_timestamp_answer_above_finality_must_link_to_the_head() {
        let dir = tempfile::tempdir().unwrap();
        let store = Store::open(&dir.path().join("t.redb")).unwrap();
        let history = AddressHistory::open(store, 1, &[ALICE.into()]).unwrap();
        history.finalize(40).unwrap();
        let ask = |forward: crate::address_proxy::Forward, t: u64| {
            let s = Arc::new(
                ModeState::new(history.clone(), 1, Duration::from_secs(300), true)
                    .with_forward(forward),
            );
            async move {
                let q = format!(
                    "/api?module=block&action=getblocknobytime&timestamp={t}&closest=before"
                );
                call(router(s), "GET", &q, None).await.1
            }
        };
        let between_50_and_51 = 1000 + 12 * 50 + 5;
        assert_eq!(
            ask(timed(100, None), between_50_and_51).await["result"],
            "50"
        );
        // Block 80 does not link to 79: the head the search began from is on another chain than
        // the blocks the answer came from.
        let a = ask(timed(100, Some(80)), between_50_and_51).await;
        assert!(
            a["result"]
                .as_str()
                .unwrap()
                .starts_with("NUTHATCH_INCOMPLETE:"),
            "{a}"
        );
        // Under the finalized head the answer needs no link to the head.
        let a = ask(timed(100, Some(80)), 1000 + 12 * 20 + 5).await;
        assert_eq!(a["result"], "20", "{a}");
    }

    /// The walk to the head must pass through the very headers the search proved its answer with:
    /// a node that moves to another fork between the search and the walk cannot have the walk pass on
    /// headers the search never saw.
    #[tokio::test]
    async fn the_timestamp_walk_is_bound_to_the_searched_headers() {
        let reads = Arc::new(AtomicU64::new(0));
        let n = reads.clone();
        let inner = timed(100, None);
        // After the search, block 51 is served at another hash and block 52 names it as parent: a
        // chain as consistent as the one searched, so only the searched headers can tell them apart.
        let forward: crate::address_proxy::Forward = Arc::new(move |m: String, p: Value| {
            let (inner, n) = (inner.clone(), n.clone());
            Box::pin(async move {
                let mut h = inner(m, p).await?;
                let calls = n.fetch_add(1, Ordering::Relaxed);
                if calls > 40 && h["number"] == "0x33" {
                    h["hash"] = json!(format!("0x{:064x}", 777));
                }
                if calls > 40 && h["number"] == "0x34" {
                    h["parentHash"] = json!(format!("0x{:064x}", 777));
                }
                Ok(h)
            })
        });
        let (b, latest, proof) = search_by_time(&forward, 1000 + 12 * 50 + 5, false)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(b, 50);
        assert!(!descends(&forward, b, latest, &proof).await.unwrap());
    }

    #[tokio::test]
    async fn the_timestamp_search_is_exact_at_its_edges_and_refuses_what_it_cannot_prove() {
        let f = timed(100, None);
        let at = |b: u64| 1000 + 12 * b;
        for (t, after, want) in [
            (at(50), false, Ok(50)),
            (at(50), true, Ok(50)),
            (at(50) + 5, false, Ok(50)),
            (at(50) + 5, true, Ok(51)),
            (at(0), false, Ok(0)),
            (999, true, Ok(0)),
            (at(100), false, Ok(100)),
            (at(100), true, Ok(100)),
        ] {
            assert_eq!(
                search_by_time(&f, t, after)
                    .await
                    .unwrap()
                    .map(|(b, _, _)| b),
                want,
                "t={t} after={after}"
            );
        }
        // Newer than the head: a block at or before it may still come.
        assert!(search_by_time(&f, at(100) + 1, false)
            .await
            .unwrap()
            .is_err());
        assert!(search_by_time(&f, at(100) + 1, true)
            .await
            .unwrap()
            .is_err());
        // Before the first block nothing is at or before it.
        assert!(search_by_time(&f, 999, false).await.unwrap().is_err());
        // An answer whose neighbour does not link to it was read across a reorg.
        let moved = timed(100, Some(51));
        assert!(search_by_time(&moved, at(50) + 5, false).await.is_err());
        assert!(search_by_time(&moved, at(50) + 5, true).await.is_err());
    }
}
