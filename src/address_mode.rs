//! The rotki-mode runtime (RFC-0063): a nest with `[address_history]` and no contracts.
//!
//! It shares nothing with the event path. No decode registry, no DBSP circuit, no Burrmill, no
//! sealing: one redb handle, one cursor that reads the chain head every `poll_interval`, and a small
//! router serving `/api`, `/health`, `/ready` and `/metrics`. After each head read the cursor
//! discovers every watched address up to the finalized head ([`Discovery`]).

use anyhow::{bail, Context, Result};
use std::collections::{BTreeMap, BTreeSet};
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
    /// Set when the nest sweeps every block for the whole watched set (RFC-0064).
    pub sweep: Option<crate::address_history::SweepConfig>,
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
    /// Whether the last window read failed: a working head with a failing trace endpoint still
    /// stalls the backfill.
    backfill_failing: std::sync::atomic::AtomicBool,
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
    /// Watched validators and their proven blocks (plan C), on Ethereum mainnet.
    pub validators: Option<crate::address_validators::Validators>,
    /// Where proposer catalogue spans the mirror lacks are built from, when `beacon_url` names one.
    pub beacon: Option<Arc<crate::address_proposers::Beacon>>,
    /// The chain's hash at a height, read for a request without the cursor: one hash check lets a
    /// covered answer above the finalized head be served without waiting behind the cursor's work.
    pub hash_at: Option<HashAt>,
    /// The blocks a backfill's first discovery window covers, before [`Pace`] adjusts it.
    pub first_window: u64,
    /// The discovery window [`Pace`] last settled on, kept across polls; 0 before the first.
    backfill_span: AtomicU64,
    /// Blocks per minute the last discovery window covered, at least 1 once measured; 0 before the
    /// first. Per minute so a slow measured layer never truncates to "unmeasured".
    backfill_rate: AtomicU64,
    /// The same for the last body window and the last produced-block step.
    body_rate: AtomicU64,
    produced_rate: AtomicU64,
    /// How long the last timestamp search on the RPC took, in milliseconds; 0 before the first.
    search_ms: AtomicU64,
    /// Whether the mirror's last block is as far as it has published, found again every poll, rather
    /// than a configured `to_block`.
    mirror_open: bool,
    /// The mirror's last block as last found, when it is open.
    mirror_last: AtomicU64,
    /// The pass that currently has the cursor, and when it took it. Empty between passes.
    /// A long pass polls and serves while it runs; this is how `/ready` says which one is doing it.
    holding: Mutex<Option<(&'static str, Instant)>>,
    /// Blocks the body scan has read, held until the validator pass can hang them from the
    /// finalized tag. A window proved only against its neighbour is not recorded on its own.
    held: Mutex<Option<HeldBridge>>,
}

/// One tip block from the body scan, and the watched proposers' rows for the blocks before it.
struct HeldBridge {
    /// The first catalogue block this proof covers.
    from: Option<u64>,
    /// That block's header: the block under it hangs from this, once the tip is the finalized block.
    head: Option<alloy_consensus::Header>,
    tip: crate::address_partitions::VerifiedBlock,
    proven: Vec<crate::address_proposers::Proven>,
}

pub type HashAt =
    Arc<dyn Fn(u64) -> futures::future::BoxFuture<'static, Result<[u8; 32]>> + Send + Sync>;

/// How many `logs/getLogs` requests may run at once; one more answers incomplete at once.
const LOGS_AT_ONCE: usize = 2;

/// Blocks one on-demand discovery window covers, and one body window: small enough that a window
/// started near a request's deadline does not hold the cursor long past it.
const COVER_WINDOW: u64 = 5_000;
const COVER_BODIES: u64 = 500;
const COVER_PRODUCED: u64 = 1_000;

/// How many blocks a backfill window covers, set by how long the last one took: requests for history
/// above what is covered wait for the window in hand, so a window must stay well under their deadline.
/// It starts small, doubles while windows are quick, and halves when one runs long.
pub(crate) struct Pace {
    span: u64,
    min: u64,
    max: u64,
    target: Duration,
    deadline: Duration,
    last: Option<(u64, Duration)>,
    /// What a window costs whatever its size, from the last two windows of different sizes.
    fixed: Option<Duration>,
}

impl Pace {
    pub(crate) fn new(start: u64, min: u64, max: u64, deadline: Duration) -> Pace {
        Pace {
            span: start.clamp(min, max),
            min,
            max,
            // A quarter of the deadline: a request waits for the window in hand.
            target: deadline / 4,
            deadline,
            last: None,
            fixed: None,
        }
    }
    pub(crate) fn span(&self) -> u64 {
        self.span
    }
    pub(crate) fn took(&mut self, elapsed: Duration) {
        if let Some((s, e)) = self.last.filter(|(s, _)| *s != self.span) {
            let ((s_lo, e_lo), (s_hi, e_hi)) = if s < self.span {
                ((s, e), (self.span, elapsed))
            } else {
                ((self.span, elapsed), (s, e))
            };
            let per_block = e_hi.saturating_sub(e_lo).as_secs_f64() / (s_hi - s_lo) as f64;
            self.fixed = Some(Duration::from_secs_f64(
                (e_lo.as_secs_f64() - per_block * s_lo as f64).max(0.0),
            ));
        }
        self.last = Some((self.span, elapsed));
        // Over a remote RPC a window's fixed checks alone can outrun a quarter of the deadline, and
        // halving then only multiplies them: 64 blocks per 2.3 s turned a 16-minute backfill into 12
        // hours. The target makes room for them, never past the deadline.
        let target = self.fixed.map_or(self.target, |f| {
            (f * 3).clamp(self.target, self.deadline.max(self.target))
        });
        // Growth is judged on the per-block part alone: a fixed cost past half the target would
        // otherwise never let a window grow (GraphOps' getLogs, 4.5 s a window, 2026-10-14).
        let fixed = self.fixed.unwrap_or_default().min(target);
        let quick = elapsed.saturating_sub(fixed) < (target - fixed) / 2;
        self.span = if elapsed > target {
            if self.span == self.min && self.fixed.is_none() {
                // Slow at the floor with nothing to compare: one larger window tells fixed cost from
                // per-block cost.
                (self.span * 2).min(self.max)
            } else {
                (self.span / 2).max(self.min)
            }
        } else if quick {
            (self.span * 2).min(self.max)
        } else {
            self.span
        };
    }
}

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

/// The default for `[address_history] request_deadline`: the most any request waits, queue and
/// background work included, well under rotki's 30 s read timeout.
pub const DEFAULT_DEADLINE: Duration = Duration::from_secs(8);

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
            backfill_failing: std::sync::atomic::AtomicBool::new(false),
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
            validators: None,
            beacon: None,
            hash_at: None,
            first_window: 128,
            sweep: None,
            backfill_span: AtomicU64::new(0),
            backfill_rate: AtomicU64::new(0),
            body_rate: AtomicU64::new(0),
            produced_rate: AtomicU64::new(0),
            search_ms: AtomicU64::new(0),
            mirror_open: false,
            mirror_last: AtomicU64::new(0),
            holding: Mutex::new(None),
            held: Mutex::new(None),
        }
    }

    pub fn with_deadline(mut self, deadline: Duration) -> Self {
        self.deadline = deadline;
        self
    }

    /// Hand `make(deadline, reply)` to the cursor and wait for it, no later than `deadline`: the
    /// request's own, set when it arrived, so queue time counts against it. `Err` says why it was not
    /// done; nothing is partly answered.
    async fn ask(
        &self,
        deadline: Instant,
        make: impl FnOnce(Instant, Reply) -> Request,
    ) -> std::result::Result<(), String> {
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

    pub fn with_validators(
        mut self,
        validators: crate::address_validators::Validators,
        beacon: Option<Arc<crate::address_proposers::Beacon>>,
    ) -> Self {
        self.validators = Some(validators);
        self.beacon = beacon;
        self
    }

    /// The first block validators' proposals are proven from: the catalogue's first. It does not
    /// follow `start_block`, which bounds account history: the catalogue costs no RPC to read.
    pub fn proposals_from(&self) -> u64 {
        crate::address_proposers::CATALOGUE_FROM_BLOCK
    }

    /// A mirror without a `to_block`: its last block is as far as it has published, `published`
    /// for now (`None`: nothing yet).
    pub fn with_open_mirror(
        mut self,
        source: Arc<crate::address_partitions::Source>,
        first: u64,
        published: Option<u64>,
    ) -> Self {
        self.mirror_last.store(
            published.unwrap_or(first.saturating_sub(1)),
            Ordering::Relaxed,
        );
        self.mirror = Some((
            source,
            (first, published.unwrap_or(first.saturating_sub(1))),
        ));
        self.mirror_open = true;
        self
    }

    /// The blocks the mirror serves: its configured range, or up to what it has published when open.
    /// An empty range is `None`.
    pub fn mirror_blocks(&self) -> Option<(Arc<crate::address_partitions::Source>, (u64, u64))> {
        let (source, (first, last)) = self.mirror.as_ref()?;
        let last = if self.mirror_open {
            self.mirror_last.load(Ordering::Relaxed)
        } else {
            *last
        };
        (last >= *first).then(|| (source.clone(), (*first, last)))
    }

    /// Find how far an open mirror has published now: one request when it has not moved.
    async fn refresh_mirror(&self) {
        let Some((source, (first, _))) = self.mirror.as_ref().filter(|_| self.mirror_open) else {
            return;
        };
        let known = self.mirror_last.load(Ordering::Relaxed);
        let from = if known >= *first {
            crate::address_partitions::span_of(known).0
        } else {
            crate::address_partitions::span_of(*first).0
        };
        match source.published_end(from).await {
            Ok(Some(end)) if end > known => {
                // Unverified coverage in the newly published blocks goes before they count as the
                // mirror's: a request read meanwhile must not be answered from it.
                let (h, range) = (self.history.clone(), (*first, end));
                let revoked = tokio::task::spawn_blocking(move || {
                    h.keep_verified_only(&crate::address_discovery::BLOCK_SCANNED, range)
                })
                .await
                .map_err(anyhow::Error::from)
                .and_then(|r| r);
                match revoked {
                    Ok(()) => self.mirror_last.store(end, Ordering::Relaxed),
                    Err(e) => tracing::warn!(
                        "address history: setting the newly published blocks aside: {e:#}"
                    ),
                }
            }
            Ok(Some(_)) => {}
            Ok(None) => {}
            Err(e) => tracing::warn!("address history: how far the mirror has published: {e:#}"),
        }
    }

    pub fn with_first_window(mut self, blocks: u64) -> Self {
        self.first_window = blocks;
        self
    }

    pub fn with_sweep(mut self, sweep: Option<crate::address_history::SweepConfig>) -> Self {
        self.sweep = sweep;
        self
    }

    pub fn with_hash_at(mut self, hash_at: HashAt) -> Self {
        self.hash_at = Some(hash_at);
        self
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
        since_poll > 3 * self.poll_interval
            || self.failing.load(Ordering::Relaxed)
            || self.backfill_failing.load(Ordering::Relaxed)
    }

    fn window_done(&self) {
        self.windows.fetch_add(1, Ordering::Relaxed);
        self.backfill_failing.store(false, Ordering::Relaxed);
    }

    fn window_failed(&self) {
        self.discovery_errors.fetch_add(1, Ordering::Relaxed);
        self.backfill_failing.store(true, Ordering::Relaxed);
    }

    /// Name the pass that has the cursor, until the guard drops.
    fn begin_pass(&self, name: &'static str) -> Pass<'_> {
        *self.holding.lock().expect("holding") = Some((name, Instant::now()));
        Pass { state: self }
    }

    /// The pass holding the cursor, and how many seconds it has held it.
    fn holding(&self) -> Option<(&'static str, u64)> {
        self.holding
            .lock()
            .expect("holding")
            .as_ref()
            .map(|(name, since)| (*name, since.elapsed().as_secs()))
    }

    fn take_held(&self) -> Option<HeldBridge> {
        self.held.lock().expect("held bridge").take()
    }

    fn clear_held(&self) {
        self.held.lock().expect("held bridge").take();
    }

    fn set_held(&self, held: HeldBridge) {
        *self.held.lock().expect("held bridge") = Some(held);
    }
}

/// Clears the pass `/ready` is reporting, including when the pass returns early.
struct Pass<'a> {
    state: &'a ModeState,
}

impl Drop for Pass<'_> {
    fn drop(&mut self) {
        *self.state.holding.lock().expect("holding") = None;
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

/// Whether a pass owes the cursor a poll: the last one is a full interval behind. None yet does not
/// count. The cursor polls before a pass starts, and a pass must not take that first one itself.
fn poll_due(state: &ModeState) -> bool {
    let last = state.last_poll.load(Ordering::Relaxed);
    if last == 0 {
        return false;
    }
    let now = u64::try_from(state.started.elapsed().as_millis()).unwrap_or(u64::MAX);
    let since = now.saturating_sub(last.saturating_sub(1));
    since >= u64::try_from(state.poll_interval.as_millis()).unwrap_or(u64::MAX)
}

/// One poll and its reconcile, when one is due. A pass runs on the cursor, so without this the head
/// stays where the pass started for as long as the pass runs.
async fn resume_head(state: &ModeState, source: &impl HeadSource, discovery: &impl Discovery) {
    if !poll_due(state) {
        return;
    }
    match poll_once(state, source).await {
        Ok(latest) => match discovery.reconcile(state, latest).await {
            Ok(_) => state.failing.store(false, Ordering::Relaxed),
            Err(e) => {
                state.poll_errors.fetch_add(1, Ordering::Relaxed);
                state.failing.store(true, Ordering::Relaxed);
                tracing::warn!("address history: poll during a pass failed, retrying: {e:#}");
            }
        },
        Err(e) => {
            state.failing.store(true, Ordering::Relaxed);
            tracing::warn!("address history: poll during a pass failed, retrying: {e:#}");
        }
    }
}

/// Serve what is waiting, and poll when a poll is due. The cursor's passes call this instead of
/// running to the end with the queue behind them.
async fn yield_cursor<M: Rpc, T: Rpc>(state: &ModeState, d: &Discoverer<Counted<M>, Counted<T>>) {
    serve_queued(state, d).await;
    resume_head(state, &d.main, d).await;
}

/// Run `read` while calling `between` every 50 ms it is still pending. A beacon read or a partition
/// download that succeeds slowly otherwise holds every tip request queued behind that one chunk.
async fn while_serving<'b, T>(
    read: impl std::future::Future<Output = T>,
    between: &(dyn Fn() -> futures::future::BoxFuture<'b, ()> + Sync),
) -> T {
    tokio::pin!(read);
    loop {
        tokio::select! {
            biased;
            out = &mut read => return out,
            _ = tokio::time::sleep(Duration::from_millis(50)) => between().await,
        }
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

/// Blocks one sweep window may cover. A mainnet block with its logs is about 0.5 MB in flight.
const SWEEP_WINDOW: u64 = 256;

/// RFC-0064's pass: every block from where the sweep stopped, for the whole watched set, a window
/// at a time. A window that fails records nothing and is read again at the next poll; nothing past it
/// is swept, so coverage stays contiguous.
async fn sweep_pass<M: Rpc, T: Rpc>(
    d: &Discoverer<Counted<M>, Counted<T>>,
    state: &ModeState,
    to: u64,
    cfg: &crate::address_history::SweepConfig,
) {
    let mut next = match state.history.swept() {
        Ok(Some((_, last))) => last + 1,
        Ok(None) => cfg.from_block.unwrap_or(to),
        Err(e) => {
            tracing::warn!("address history: reading the swept span: {e:#}");
            return;
        }
    };
    let mut pace = Pace::new(16, 1, SWEEP_WINDOW, state.deadline);
    while next <= to {
        yield_cursor(state, d).await;
        let set: std::collections::HashSet<String> = match state.history.watched() {
            Ok(w) => w.into_iter().map(|a| a.to_ascii_lowercase()).collect(),
            Err(e) => {
                tracing::warn!("address history: reading the watched set: {e:#}");
                return;
            }
        };
        if set.is_empty() {
            return;
        }
        let end = next.saturating_add(pace.span() - 1).min(to);
        let started = Instant::now();
        let done = sweep_window(d, state, &set, (next, end)).await;
        pace.took(started.elapsed());
        match done {
            Ok(()) => {
                state.window_done();
                tracing::info!(
                    "address history: swept through {end} for {} addresses",
                    set.len()
                );
                next = end + 1;
            }
            Err(e) => {
                state.window_failed();
                tracing::warn!(
                    "address history: [{next}, {end}] not swept, retrying next poll: {e:#}"
                );
                return;
            }
        }
    }
}

/// Sweep one window and record it, anchored as [`body_window`] is.
async fn sweep_window<M: Rpc, T: Rpc>(
    d: &Discoverer<Counted<M>, Counted<T>>,
    state: &ModeState,
    set: &std::collections::HashSet<String>,
    (from, end): (u64, u64),
) -> Result<()> {
    let pending = crate::address_discovery::Pending::begin(&state.history)?;
    let generation = pending.generation();
    let read = async {
        let anchor = anchor_for(d, &state.history, end).await?;
        if let Some((b, h)) = anchor {
            pending.anchor(b, h);
        }
        let found = d.sweep(&state.history, set, from, end, &pending).await;
        let checked = check_anchor(d, &state.history, anchor, &pending.named()).await;
        Ok::<_, anyhow::Error>((anchor, found, checked))
    }
    .await;
    state.set_calls("main", d.main.calls());
    state.set_calls("trace", d.trace.calls());
    let (anchor, found, checked) = read?;
    let history = state.history.clone();
    let swept: Vec<String> = set.iter().cloned().collect();
    tokio::task::spawn_blocking(move || {
        pending.persist(&history, checked.is_ok())?;
        checked?;
        history.record_sweep(&found?, &swept, (from, end), generation, anchor)
    })
    .await?
}

/// Read one window of bodies for `addresses` and record it for each, anchored as
/// [`discover_window`] is. When `keep` is set, the parsed blocks come back with it so a proof can
/// use the bodies already read; a window that fails its check keeps none.
async fn body_window<M: Rpc, T: Rpc>(
    d: &Discoverer<Counted<M>, Counted<T>>,
    state: &ModeState,
    addresses: Vec<String>,
    (from, end): (u64, u64),
    deadline: Option<Instant>,
    keep: bool,
) -> Result<Vec<crate::address_partitions::VerifiedBlock>> {
    use crate::address_discovery::record_blocks_anchored;
    let pending = crate::address_discovery::Pending::begin(&state.history)?;
    let generation = pending.generation();
    let read = within(deadline, async {
        let anchor = anchor_for(d, &state.history, end).await?;
        if let Some((b, h)) = anchor {
            pending.anchor(b, h);
        }
        let scanned = d
            .scan_with_blocks(&state.history, &addresses, from, end, &pending, keep)
            .await;
        let checked = check_anchor(d, &state.history, anchor, &pending.named()).await;
        Ok((anchor, scanned, checked))
    })
    .await;
    state.set_calls("main", d.main.calls());
    state.set_calls("trace", d.trace.calls());
    let (anchor, scanned, checked) = read?;
    let (found, blocks) = match scanned {
        Ok((found, blocks)) => (Ok(found), blocks),
        Err(e) => (Err(e), Vec::new()),
    };
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
    .await??;
    Ok(blocks)
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
        let kept = state.backfill_span.load(Ordering::Relaxed);
        let mut pace = Pace::new(
            if kept == 0 { state.first_window } else { kept },
            64,
            crate::address_discovery::OUTER_WINDOW,
            state.deadline,
        );
        // One window per account per round, the watched set read again every round: an account rotki
        // adds mid-backfill gets its first window within a round, not after the others' whole backlog.
        let mut recorded_through: BTreeMap<String, u64> = BTreeMap::new();
        let mut stopped: BTreeSet<String> = BTreeSet::new();
        if let Some(cfg) = state.sweep.clone() {
            sweep_pass(self, state, to, &cfg).await;
        }
        // A sweeping nest covers its set block by block; discovering each address too would cost
        // per address what the sweep exists to avoid.
        while state.sweep.is_none() {
            let round = match state.history.watched() {
                Ok(w) => w,
                Err(e) => {
                    tracing::warn!("address history: reading the watched set: {e:#}");
                    break;
                }
            };
            let mut advanced = false;
            for address in round {
                if stopped.contains(&address) {
                    continue;
                }
                yield_cursor(state, self).await;
                let next = match crate::address_discovery::next_uncovered(
                    &state.history,
                    &address,
                    state.start_block,
                ) {
                    Ok(n) => n,
                    Err(e) => {
                        tracing::warn!("address history: coverage for {address}: {e:#}");
                        stopped.insert(address);
                        continue;
                    }
                };
                if next > to {
                    continue;
                }
                // A recorded window must move coverage past it; one that did not would be fetched
                // again forever, at the provider's expense.
                if recorded_through.get(&address).is_some_and(|r| next <= *r) {
                    state.window_failed();
                    tracing::error!(
                        "address history: coverage for {address} did not advance past {next}; stopping this pass"
                    );
                    stopped.insert(address);
                    continue;
                }
                let end = next.saturating_add(pace.span() - 1).min(to);
                let started = Instant::now();
                let done = discover_window(self, state, &address, (next, end), None).await;
                let took = started.elapsed();
                pace.took(took);
                state.backfill_span.store(pace.span(), Ordering::Relaxed);
                match done {
                    Ok(()) => {
                        state
                            .backfill_rate
                            .store(per_minute(end - next + 1, took), Ordering::Relaxed);
                        recorded_through.insert(address.clone(), end);
                        advanced = true;
                        state.window_done();
                        tracing::info!("address history: {address} covered through {end}");
                    }
                    Err(e) => {
                        state.window_failed();
                        tracing::warn!(
                            "address history: [{next}, {end}] for {address} not recorded, retrying next poll: {e:#}"
                        );
                        stopped.insert(address);
                    }
                }
            }
            if !advanced {
                break;
            }
        }
        // Withdrawals and produced blocks come from bodies, and the definitions they are matched to
        // are Ethereum mainnet's, so only there is the block scan run. An account added during the
        // backfill joins these passes too.
        let watched = state.history.watched().unwrap_or(watched);
        state.refresh_mirror().await;
        if state.chain_id == 1 {
            let skip = state.mirror_blocks().map(|(_, blocks)| blocks);
            if let Some((source, blocks)) = state.mirror_blocks() {
                mirror_pass(self, state, &watched, to, &source, blocks).await;
            }
            yield_cursor(state, self).await;
            scan_bodies(self, state, &watched, to, skip).await;
            mev_passes(self, state, &watched, to).await;
            validator_pass(self, state).await;
        } else if let Some((source, blocks)) = state.mirror_blocks() {
            // Elsewhere a partition is read for its verified headers alone: timestamp to block.
            mirror_pass(self, state, &[], to, &source, blocks).await;
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
            return Ok(());
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
        if state.sweep.is_some() {
            bail!("this nest sweeps every block for its whole set and is not there yet; ask again shortly");
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
            if let Some((first, last)) = state.mirror_blocks().map(|(_, b)| b) {
                if first <= next && next <= last {
                    bail!("blocks {first} to {last} are read from the mirror's partitions, which have not reached block {next} yet");
                }
            }
            let mut end = hole_end(history, address, &BLOCK_SCANNED, next, to)?
                .min(next.saturating_add(COVER_BODIES - 1));
            if let Some((first, _)) = state
                .mirror_blocks()
                .map(|(_, b)| b)
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
                false,
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
    let _pass = state.begin_pass("mev");
    let now = unix_now();
    let passes = async {
        for a in watched {
            loop {
                yield_cursor(state, d).await;
                // Blocks covered in all, so a step that starts the coverage or joins two spans counts
                // exactly what it added.
                let through = |h: &AddressHistory| {
                    h.coverage(crate::address_history::Action::ProducedBlocks, a)
                        .ok()
                        .map(|c| c.iter().map(|(f, t)| t - f + 1).sum::<u64>())
                };
                let (before, started) = (through(&state.history), Instant::now());
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
                if let (Some(b), Some(t)) = (before, through(&state.history)) {
                    if t > b {
                        state
                            .produced_rate
                            .store(per_minute(t - b, started.elapsed()), Ordering::Relaxed);
                    }
                }
                state.window_done();
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
            yield_cursor(state, d).await;
            let more =
                crate::address_mev::reask_pass(d, relays, &state.history, watched, now).await?;
            if !more {
                break;
            }
        }
        Ok::<_, anyhow::Error>(())
    };
    if let Err(e) = passes.await {
        state.window_failed();
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
    let _pass = state.begin_pass("mirror");
    let mut ceiling: Option<u64> = None;
    let mut start = span_of(lo).0;
    while start <= hi {
        yield_cursor(state, d).await;
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
                    state.window_failed();
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
                state.window_done();
                // `end` is the partition's last block. `end_block` can fall inside it, and the
                // nest only read the clip: logging the partition would claim blocks past its end.
                if clip.0 == start && clip.1 == end {
                    tracing::info!("address history: partition [{start}, {end}] verified and read");
                } else {
                    tracing::info!(
                        "address history: partition [{start}, {end}] verified and read through [{}, {}]",
                        clip.0, clip.1
                    );
                }
            }
            Err(e) => {
                state.window_failed();
                tracing::warn!(
                    "address history: partition [{start}, {end}] not used, its blocks stay incomplete: {e:#}"
                );
            }
        }
        start += SPAN;
    }
}

/// The validator pass (plan C). Anchored at the block the RPC calls finalized, read by that tag
/// once per pass. A beacon proves the uncovered bridge above the mirror first and the spans hang
/// from that header, so the bridge is fetched once; otherwise the newest span is linked to the
/// finalized block by parent hashes and the tail is proven after. A span that cannot be proven
/// records nothing and ends its part of the pass.
async fn validator_pass<M: Rpc, T: Rpc>(d: &Discoverer<Counted<M>, Counted<T>>, state: &ModeState) {
    let Some(validators) = state.validators.clone() else {
        return;
    };
    let watched = match validators.watched() {
        Ok(w) if w.is_empty() => return,
        Ok(w) => w,
        Err(e) => {
            tracing::warn!("address history: reading the watched validators: {e:#}");
            return;
        }
    };
    let _pass = state.begin_pass("validators");
    let anchor = match crate::address_proposers::finalized_header(&d.main).await {
        Ok((h, _)) => h,
        Err(e) => {
            state.window_failed();
            tracing::warn!(
                "address history: the RPC's finalized block, for the proposer catalogue: {e:#}"
            );
            return;
        }
    };
    // Proofs run to the finalized block whatever `end_block` says: the chain of spans must reach the
    // anchor. `end_block` bounds what is served, not what is proven.
    let ceiling = anchor.number.saturating_sub(1);
    let first = state.proposals_from();
    let gap_from = |floor: u64| {
        watched
            .iter()
            .filter_map(|v| validators.next_uncovered(*v, floor).ok())
            .min()
            .unwrap_or(floor)
    };
    let mut mirror_end = None;
    // Header of the first block above the mirror, once that whole bridge has been proven.
    let mut bridge_header = None;
    // The body scan already read these headers. Record them only when the newest is this
    // finalized block and the oldest is the uncovered one: a shorter proof is read again from
    // the hole, so it is not kept.
    if let Some(held) = state.take_held() {
        let floor = state
            .mirror_blocks()
            .map_or(first, |(_, blocks)| first.max(blocks.1 + 1));
        let from = gap_from(floor);
        if held.tip.header.number == anchor.number
            && held.tip.hash == anchor.hash_slow()
            && held.from == Some(from)
            && from <= ceiling
            && commit_scanned(d, state, &validators, &watched, &held.proven, from, ceiling).await
        {
            bridge_header = held.head;
        }
    }
    if let Some((source, blocks)) = state.mirror_blocks() {
        mirror_end = Some(blocks.1);
        if let Some(beacon) = state.beacon.clone() {
            let bridge_from = blocks.1 + 1;
            let from = gap_from(first.max(bridge_from));
            // A hole partway along the bridge is left to the link walk, which reads the whole gap.
            if bridge_from <= ceiling && from == bridge_from {
                bridge_header = prove_range(
                    d,
                    state,
                    &validators,
                    &watched,
                    &beacon,
                    from,
                    ceiling,
                    &anchor,
                )
                .await;
            }
        }
        if let Err(e) = mirror_spans(
            d,
            state,
            &validators,
            &watched,
            &source,
            blocks,
            &anchor,
            ceiling,
            bridge_header.as_ref(),
        )
        .await
        {
            state.window_failed();
            tracing::warn!(
                "address history: proposers from the mirror not proven, those validators' blocks stay incomplete: {e:#}"
            );
        }
    }
    yield_cursor(state, d).await;
    if let Some(beacon) = &state.beacon {
        // Above the mirror, from the first block any watched validator lacks there.
        let floor = mirror_end.map_or(first, |m| first.max(m + 1));
        let from = gap_from(floor);
        if from <= ceiling {
            let _ = prove_range(
                d,
                state,
                &validators,
                &watched,
                beacon,
                from,
                ceiling,
                &anchor,
            )
            .await;
        }
    }
    validator_retries(d, state, &validators).await;
}

/// Build the rows of `proven` for `lacking` and record `span` for them, under `generation`.
async fn record_proven<M: Rpc, T: Rpc>(
    d: &Discoverer<Counted<M>, Counted<T>>,
    state: &ModeState,
    validators: &crate::address_validators::Validators,
    lacking: &[u64],
    span: (u64, u64),
    proven: &[crate::address_proposers::Proven],
    generation: u64,
) -> Result<()> {
    let mut found: BTreeMap<u64, Vec<crate::address_validators::Stored>> = BTreeMap::new();
    for p in proven
        .iter()
        .filter(|p| lacking.contains(&p.proposer_index))
    {
        found
            .entry(p.proposer_index)
            .or_default()
            .push(proven_row(d, state, p).await);
    }
    let v = validators.clone();
    let lacking = lacking.to_vec();
    tokio::task::spawn_blocking(move || v.record_span(span, &lacking, &found, generation)).await?
}

/// The mirror's whole spans inside `[first, ceiling]` that some watched validator lacks, proven newest
/// first. `prelinked` is the header of the first block above the mirror when that bridge was just
/// proven: the newest span hangs from it, or is linked only up to it. Without it, the newest span
/// is linked to `anchor`. Every older span's successor is the first block of the span above it.
#[allow(clippy::too_many_arguments)]
async fn mirror_spans<M: Rpc, T: Rpc>(
    d: &Discoverer<Counted<M>, Counted<T>>,
    state: &ModeState,
    validators: &crate::address_validators::Validators,
    watched: &[u64],
    source: &crate::address_partitions::Source,
    (_, mirror_last): (u64, u64),
    anchor: &alloy_consensus::Header,
    ceiling: u64,
    prelinked: Option<&alloy_consensus::Header>,
) -> Result<()> {
    use crate::address_partitions::{span_of, SPAN};
    use crate::address_proposers as ap;
    let first = state.proposals_from();
    let lacking = |lo: u64, hi: u64| -> Vec<u64> {
        watched
            .iter()
            .filter(|v| !validators.covers(**v, (lo, hi)).unwrap_or(false))
            .copied()
            .collect()
    };
    // From the catalogue's first span whatever the mirror's `from_block`, which bounds only what
    // address history reads from partitions.
    let mut start = span_of(first).0;
    let mut needed = Vec::new();
    while start + SPAN - 1 <= mirror_last.min(ceiling) {
        if !lacking(start.max(first), start + SPAN - 1).is_empty() {
            needed.push(start);
        }
        start += SPAN;
    }
    let (Some(&lowest), Some(&top)) = (needed.first(), needed.last()) else {
        return Ok(());
    };
    // The newest needed span's successor. A proven bridge's first header already reaches the
    // finalized block, so the span hangs from it and those headers are not read again.
    let top_end = top + SPAN - 1;
    let mut successor = if top_end + 1 == anchor.number {
        anchor.clone()
    } else if let Some(h) = prelinked.filter(|h| h.number == top_end + 1) {
        h.clone()
    } else {
        let hang = prelinked
            .filter(|h| top_end + 1 < h.number)
            .unwrap_or(anchor);
        let (lo, hi) = (top_end + 1, hang.number - 1);
        if hi - lo + 1 > LOCAL_CATALOGUE_MAX {
            bail!(
                "the mirror's spans end {} blocks under the finalized block, over the {LOCAL_CATALOGUE_MAX} a nest links itself",
                hi - lo + 1
            );
        }
        ap::linked_to(&d.main, (lo, hi), hang, &|| {
            Box::pin(yield_cursor(state, d))
        })
        .await?
    };
    let mut span = top;
    loop {
        yield_cursor(state, d).await;
        let end = span + SPAN - 1;
        let generation = validators.generation()?;
        let mut verified = None;
        for fresh in [false, true] {
            let Some(raw) = source.fetch(span, fresh).await? else {
                bail!("the mirror has no block partition for [{span}, {end}]");
            };
            match crate::address_partitions::decode(&raw).and_then(|b| {
                crate::address_partitions::verify(&b, span, end, successor.parent_hash, 1)
            }) {
                Ok(b) => {
                    verified = Some(b);
                    break;
                }
                Err(e) if !fresh => {
                    tracing::warn!("address history: cached partition [{span}, {end}] refused, downloading it again: {e:#}");
                    source.discard(span);
                }
                Err(e) => return Err(e.context(format!("the partition for [{span}, {end}]"))),
            }
        }
        let blocks = verified.expect("the loop verifies or returns");
        if needed.contains(&span) {
            let lo = ap::provable(span).map_or(end + 1, |(lo, _)| lo);
            let clip = (span.max(first), end);
            let who = lacking(clip.0, clip.1);
            let mut proven = None;
            for fresh in [false, true] {
                let Some(raw) = source.fetch_catalogue(span, fresh).await? else {
                    bail!("the mirror has no proposer catalogue for [{span}, {end}]");
                };
                match ap::decode(&raw).and_then(|rows| {
                    ap::verify(
                        &rows,
                        &blocks[(lo - span) as usize..],
                        &successor,
                        (lo, end),
                    )
                }) {
                    Ok(p) => {
                        proven = Some(p);
                        break;
                    }
                    Err(e) if !fresh => {
                        tracing::warn!("address history: cached catalogue [{span}, {end}] refused, downloading it again: {e:#}");
                        source.discard_catalogue(span);
                    }
                    Err(e) => return Err(e.context(format!("the catalogue for [{span}, {end}]"))),
                }
            }
            let proven: Vec<ap::Proven> = proven
                .expect("the loop proves or returns")
                .into_iter()
                .filter(|p| p.block.header.number >= clip.0)
                .collect();
            record_proven(d, state, validators, &who, clip, &proven, generation).await?;
            state.set_calls("main", d.main.calls());
            state.window_done();
            tracing::info!(
                "address history: proposers proven over [{}, {end}] for {} validator(s)",
                clip.0,
                who.len()
            );
        }
        if span == lowest {
            return Ok(());
        }
        successor = blocks[0].header.clone();
        span -= SPAN;
    }
}

/// The most blocks behind finality a nest reads itself for the proposer catalogue: enough for the gap
/// between a mirror's last published span and the finalized block, not for history a mirror holds.
const LOCAL_CATALOGUE_MAX: u64 = 50_000;

/// How often a validator block's owed reward or payment is read again.
const RETRY_EVERY_SECS: u64 = 60 * 60;

/// Every block of `[from, to]` (`to` at most `anchor`'s parent) with its proven proposer, built here
/// from the RPC and the beacon and linked to `anchor`, the block the RPC calls finalized, through
/// every block between: a self-consistent fork the RPC returned for the range cannot reach it.
/// The header of block `from` is what the block under it hangs from.
async fn prove_tail<'b, M: Rpc, T: Rpc>(
    d: &Discoverer<Counted<M>, Counted<T>>,
    beacon: &crate::address_proposers::Beacon,
    from: u64,
    to: u64,
    anchor: &alloy_consensus::Header,
    keep: &[u64],
    between: &(dyn Fn() -> futures::future::BoxFuture<'b, ()> + Sync),
) -> Result<(
    u64,
    Vec<crate::address_proposers::Proven>,
    alloy_consensus::Header,
)> {
    use crate::address_proposers as ap;
    let last = anchor.number.saturating_sub(1);
    if to < from || to > last {
        bail!(
            "the range [{from}, {to}] is not under the finalized block {}",
            anchor.number
        );
    }
    if last - from + 1 > LOCAL_CATALOGUE_MAX {
        bail!(
            "building {} blocks here is over the {LOCAL_CATALOGUE_MAX} a nest builds itself; the mirror should hold them",
            last - from + 1
        );
    }
    // Read through to the finalized block whatever `to` is, so the range hangs from it: newest chunk
    // first, each linked to the chunk above it, the top one to the anchor, and the cursor's waiting
    // requests served between chunks.
    let mut successor = anchor.clone();
    let mut proven = Vec::new();
    let mut hi = last;
    loop {
        let lo = hi.saturating_sub(ap::CHUNK - 1).max(from);
        // Served while the chunk is in flight, not only after it: a slow beacon read that succeeds
        // holds every tip request queued behind this one chunk.
        let blocks = while_serving(ap::blocks_from_rpc(&d.main, (lo, hi), 8), between).await?;
        let rows = while_serving(
            ap::build_rows(beacon, &blocks, &successor, (lo, hi), 8),
            between,
        )
        .await?;
        let chunk = ap::verify(&rows, &blocks, &successor, (lo, hi))?;
        successor = blocks[0].header.clone();
        proven.extend(
            chunk
                .into_iter()
                .filter(|p| keep.contains(&p.proposer_index) && p.block.header.number <= to),
        );
        if lo == from {
            break;
        }
        hi = lo - 1;
        between().await;
    }
    proven.sort_by_key(|p| p.block.header.number);
    Ok((to, proven, successor))
}

/// Prove `[from, ceiling]` and record it for the watched validators who lack it. The header of
/// block `from` lets a mirror span hang from the bridge without reading it again. `None` on
/// failure: the caller links and proves the range as it did without this.
#[allow(clippy::too_many_arguments)]
async fn prove_range<M: Rpc, T: Rpc>(
    d: &Discoverer<Counted<M>, Counted<T>>,
    state: &ModeState,
    validators: &crate::address_validators::Validators,
    watched: &[u64],
    beacon: &crate::address_proposers::Beacon,
    from: u64,
    ceiling: u64,
    anchor: &alloy_consensus::Header,
) -> Option<alloy_consensus::Header> {
    let outcome = async {
        let generation = validators.generation()?;
        let (to, proven, header) = prove_tail(d, beacon, from, ceiling, anchor, watched, &|| {
            Box::pin(yield_cursor(state, d))
        })
        .await?;
        let lacking: Vec<u64> = watched
            .iter()
            .filter(|v| !validators.covers(**v, (from, to)).unwrap_or(false))
            .copied()
            .collect();
        record_proven(
            d,
            state,
            validators,
            &lacking,
            (from, to),
            &proven,
            generation,
        )
        .await?;
        Ok::<_, anyhow::Error>(header)
    }
    .await;
    state.set_calls("main", d.main.calls());
    match outcome {
        Ok(header) => {
            state.window_done();
            tracing::info!(
                "address history: proposers proven over [{from}, {ceiling}], anchored at the finalized block"
            );
            Some(header)
        }
        Err(e) => {
            state.window_failed();
            tracing::warn!("address history: proposers over [{from}, {ceiling}] not proven: {e:#}");
            None
        }
    }
}

/// Record a proof the body scan already built, the same way [`prove_range`] records one it read.
/// False when it was not recorded, so the caller reads the range itself.
async fn commit_scanned<M: Rpc, T: Rpc>(
    d: &Discoverer<Counted<M>, Counted<T>>,
    state: &ModeState,
    validators: &crate::address_validators::Validators,
    watched: &[u64],
    proven: &[crate::address_proposers::Proven],
    from: u64,
    ceiling: u64,
) -> bool {
    let lacking: Vec<u64> = watched
        .iter()
        .filter(|v| !validators.covers(**v, (from, ceiling)).unwrap_or(false))
        .copied()
        .collect();
    // An empty slice still moves the generation, and a proof read against the old one is then refused.
    if lacking.is_empty() {
        return false;
    }
    let outcome = async {
        let generation = validators.generation()?;
        record_proven(
            d,
            state,
            validators,
            &lacking,
            (from, ceiling),
            proven,
            generation,
        )
        .await?;
        Ok::<_, anyhow::Error>(())
    }
    .await;
    state.set_calls("main", d.main.calls());
    match outcome {
        Ok(()) => {
            state.window_done();
            tracing::info!(
                "address history: proposers proven over [{from}, {ceiling}], anchored at the finalized block"
            );
            true
        }
        Err(e) => {
            state.window_failed();
            tracing::warn!("address history: proposers over [{from}, {ceiling}] not proven: {e:#}");
            false
        }
    }
}

/// The served row of one proven block. Its reward comes from the block's own transactions and
/// receipts, checked against its header; when they cannot be read the reward is owed, never zero.
/// Relay provenance is asked once here and again later when it is missing.
async fn proven_row<M: Rpc, T: Rpc>(
    d: &Discoverer<Counted<M>, Counted<T>>,
    state: &ModeState,
    p: &crate::address_proposers::Proven,
) -> crate::address_validators::Stored {
    let h = &p.block.header;
    let mut row = serde_json::Map::new();
    let mut meta = serde_json::Map::new();
    let text = |v: String| Value::String(v);
    row.insert("blockNumber".into(), text(h.number.to_string()));
    row.insert("blockHash".into(), text(format!("{:#x}", p.block.hash)));
    row.insert("timeStamp".into(), text(h.timestamp.to_string()));
    row.insert("slot".into(), text(p.slot.to_string()));
    row.insert("proposerIndex".into(), text(p.proposer_index.to_string()));
    row.insert("feeRecipient".into(), text(format!("{:#x}", h.beneficiary)));
    match crate::address_partitions::verified_reward(&d.main, &p.block).await {
        Ok(r) => {
            row.insert("blockReward".into(), text(r.to_string()));
            row.insert("rewardStatus".into(), text("verified".into()));
        }
        Err(e) => {
            tracing::warn!(
                "address history: block {}'s reward owed, retried later: {e:#}",
                h.number
            );
            row.insert("rewardStatus".into(), text("unavailable".into()));
        }
    }
    let now = unix_now();
    let mev = match &state.relays {
        Some(relays) => {
            crate::address_mev::block_mev(
                &state.history,
                relays,
                h.number,
                &format!("{:#x}", p.block.hash),
                false,
                None,
            )
            .await
        }
        None => crate::address_mev::no_mev(),
    };
    if state.relays.is_some() && mev.get("mev").and_then(Value::as_str) == Some("unrecorded") {
        meta.insert("mevFirstAsked".into(), json!(now));
        meta.insert("mevLastAsked".into(), json!(now));
    }
    row.extend(mev);
    payment_into(d, h.number, &mut row, &mut meta).await;
    crate::address_validators::Stored {
        block: h.number,
        row,
        meta,
    }
}

/// The builder's payment to the relays' recipient in block `b`, as `nuthatchProducedBlocks` carries it,
/// read from the block's traces. Without a recipient there is nothing to look for; a trace that cannot
/// be read leaves the payment owed, retried later, never claimed absent.
async fn payment_into<M: Rpc, T: Rpc>(
    d: &Discoverer<Counted<M>, Counted<T>>,
    b: u64,
    row: &mut serde_json::Map<String, Value>,
    meta: &mut serde_json::Map<String, Value>,
) {
    let get = |k: &str| row.get(k).and_then(Value::as_str).unwrap_or("").to_string();
    let (recipient, hash, miner) = (get("mevRecipient"), get("blockHash"), get("feeRecipient"));
    let mut found = (String::new(), String::new());
    meta.remove("paymentOwed");
    if !recipient.is_empty() {
        let frames = d
            .trace
            .call("trace_block", json!([format!("0x{b:x}")]))
            .await;
        match frames.and_then(|f| {
            let f = f
                .as_array()
                .cloned()
                .ok_or_else(|| anyhow::anyhow!("trace_block answered no list for block {b}"))?;
            crate::address_mev::payment_legs(&f, &hash, &miner, &recipient)
        }) {
            Ok((hashes, total)) if !hashes.is_empty() => {
                found = (hashes.join(","), total.to_string())
            }
            Ok(_) => {}
            Err(e) => {
                tracing::warn!("address history: block {b}'s payment owed, retried later: {e:#}");
                meta.insert("paymentOwed".into(), Value::Bool(true));
            }
        }
    }
    row.insert("paymentTx".into(), Value::String(found.0));
    row.insert("paymentValue".into(), Value::String(found.1));
}

/// Owed rewards and payments read again, and blocks without relay evidence asked again on the re-ask
/// schedule, a bounded batch per pass. A block first written while relays were off is asked as soon
/// as they are on.
async fn validator_retries<M: Rpc, T: Rpc>(
    d: &Discoverer<Counted<M>, Counted<T>>,
    state: &ModeState,
    validators: &crate::address_validators::Validators,
) {
    use crate::address_mev::{REASK_EVERY_SECS, REASK_FOR_SECS};
    let all = match validators.all() {
        Ok(a) => a,
        Err(e) => {
            tracing::warn!("address history: reading validator blocks to retry: {e:#}");
            return;
        }
    };
    let now = unix_now();
    let num = |m: &serde_json::Map<String, Value>, k: &str| m.get(k).and_then(Value::as_u64);
    // Owed reads are tried at most hourly, least recently tried first, so rows whose data stays
    // unavailable cannot hold every batch.
    let tried = |s: &crate::address_validators::Stored| num(&s.meta, "retriedAt").unwrap_or(0);
    let reask_due = |s: &crate::address_validators::Stored| {
        state.relays.is_some()
            && s.row.get("mev").and_then(Value::as_str) == Some("unrecorded")
            && match num(&s.meta, "mevFirstAsked") {
                None => true,
                Some(f) => {
                    now < f + REASK_FOR_SECS
                        && num(&s.meta, "mevLastAsked").is_some_and(|l| now >= l + REASK_EVERY_SECS)
                }
            }
    };
    let mut due: Vec<(u64, crate::address_validators::Stored)> = all
        .into_iter()
        .filter(|(_, s)| {
            let owed = s.row.get("rewardStatus").and_then(Value::as_str) == Some("unavailable")
                || s.meta.get("paymentOwed").and_then(Value::as_bool) == Some(true);
            (owed && now >= tried(s) + RETRY_EVERY_SECS) || reask_due(s)
        })
        .collect();
    // Least recently tried or asked first.
    due.sort_by_key(|(_, s)| tried(s).max(num(&s.meta, "mevLastAsked").unwrap_or(0)));
    for (v, mut s) in due.into_iter().take(64) {
        let owed = s.row.get("rewardStatus").and_then(Value::as_str) == Some("unavailable");
        let reask = reask_due(&s);
        let payment_owed = s.meta.get("paymentOwed").and_then(Value::as_bool) == Some(true);
        let owed = owed && now >= tried(&s) + RETRY_EVERY_SECS;
        let payment_owed = payment_owed && now >= tried(&s) + RETRY_EVERY_SECS;
        if !owed && !reask && !payment_owed {
            continue;
        }
        if owed || payment_owed {
            s.meta.insert("retriedAt".into(), json!(now));
        }
        let hash = s
            .row
            .get("blockHash")
            .and_then(Value::as_str)
            .unwrap_or("")
            .to_string();
        if owed {
            let block = async {
                let (header, h) = crate::address_proposers::header_at(&d.main, s.block).await?;
                if format!("{h:#x}") != hash {
                    bail!("block {} is now at another hash", s.block);
                }
                let vb = crate::address_partitions::VerifiedBlock {
                    hash: h,
                    header,
                    withdrawals: Vec::new(),
                };
                crate::address_partitions::verified_reward(&d.main, &vb).await
            };
            if let Ok(r) = block.await {
                s.row
                    .insert("blockReward".into(), Value::String(r.to_string()));
                s.row
                    .insert("rewardStatus".into(), Value::String("verified".into()));
            }
        }
        if reask {
            if let Some(relays) = &state.relays {
                let mev = crate::address_mev::block_mev(
                    &state.history,
                    relays,
                    s.block,
                    &hash,
                    true,
                    None,
                )
                .await;
                s.meta.insert("mevLastAsked".into(), json!(now));
                s.meta.entry("mevFirstAsked").or_insert(json!(now));
                s.row.extend(mev);
            }
        }
        if reask || payment_owed {
            payment_into(d, s.block, &mut s.row, &mut s.meta).await;
        }
        let vs = validators.clone();
        let saved = tokio::task::spawn_blocking(move || vs.update(v, &s)).await;
        if let Err(e) = saved.map_err(anyhow::Error::from).and_then(|r| r) {
            tracing::warn!("address history: saving a validator block's retry: {e:#}");
        }
    }
    state.set_calls("main", d.main.calls());
}

/// Fold `blocks` into the held proof. The last stays the tip the next window must follow; the rest
/// are proven from the beacon, in chunks, and only the watched proposers' rows are kept. A gap or a
/// beacon failure drops the hold. The body record is the caller's, and is left as it is.
async fn note_scanned<M: Rpc, T: Rpc>(
    d: &Discoverer<Counted<M>, Counted<T>>,
    state: &ModeState,
    blocks: Vec<crate::address_partitions::VerifiedBlock>,
) -> Result<()> {
    use crate::address_proposers::{build_rows, verify, CHUNK};
    let prev = state.take_held();
    if blocks.is_empty() {
        return Ok(());
    }
    let follows = prev.as_ref().is_some_and(|prev| {
        blocks[0].header.number == prev.tip.header.number + 1
            && blocks[0].header.parent_hash == prev.tip.hash
    });
    let (carried, mut chain) = if follows {
        let HeldBridge {
            from,
            head,
            tip,
            proven,
        } = prev.expect("follows");
        let mut chain = blocks;
        chain.insert(0, tip);
        (Some((from, head, proven)), chain)
    } else {
        (None, blocks)
    };
    let (mut from, mut head, mut proven) = carried.unwrap_or((None, None, Vec::new()));
    if chain.len() < 2 {
        state.set_held(HeldBridge {
            from,
            head,
            tip: chain.pop().expect("one block"),
            proven,
        });
        return Ok(());
    }
    let tip = chain.pop().expect("the last block");
    let cat = state.proposals_from();
    if chain.last().is_none_or(|b| b.header.number < cat) {
        state.set_held(HeldBridge {
            from,
            head,
            tip,
            proven,
        });
        return Ok(());
    }
    let clip: Vec<_> = chain
        .into_iter()
        .filter(|b| b.header.number >= cat)
        .collect();
    let last = clip.last().expect("a block at or after the catalogue");
    if tip.header.number != last.header.number + 1 || tip.header.parent_hash != last.hash {
        bail!(
            "block {} does not follow block {}",
            tip.header.number,
            last.header.number
        );
    }
    for pair in clip.windows(2) {
        if pair[1].header.number != pair[0].header.number + 1
            || pair[1].header.parent_hash != pair[0].hash
        {
            bail!(
                "block {} does not follow block {}",
                pair[1].header.number,
                pair[0].header.number
            );
        }
    }
    let beacon = state
        .beacon
        .clone()
        .context("the proposer proof has no beacon")?;
    let watched = state
        .validators
        .as_ref()
        .context("the proposer proof has no validators")?
        .watched()?;
    let mut start = 0;
    while start < clip.len() {
        let end = (start + CHUNK as usize).min(clip.len());
        let chunk = &clip[start..end];
        let successor = if end == clip.len() {
            &tip.header
        } else {
            &clip[end].header
        };
        let lo = chunk[0].header.number;
        let hi = chunk[chunk.len() - 1].header.number;
        let rows = while_serving(build_rows(&beacon, chunk, successor, (lo, hi), 8), &|| {
            Box::pin(yield_cursor(state, d))
        })
        .await?;
        let got = verify(&rows, chunk, successor, (lo, hi))?;
        proven.extend(
            got.into_iter()
                .filter(|p| watched.contains(&p.proposer_index)),
        );
        if from.is_none() {
            from = Some(lo);
            head = Some(chunk[0].header.clone());
        }
        start = end;
        if start < clip.len() {
            yield_cursor(state, d).await;
        }
    }
    state.set_held(HeldBridge {
        from,
        head,
        tip,
        proven,
    });
    Ok(())
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
    // A hold from an earlier pass must not be committed: this pass may not reach the finalized block.
    state.clear_held();
    let mut keep = state.beacon.is_some() && state.validators.is_some();
    // rotki asks for withdrawals and produced blocks only by a tracked validator's withdrawal
    // address, so a plain wallet would pay one body a block for nothing. The proposer proof reads
    // every scanned block, so with validators watched the scan stays whole.
    let asked: Vec<String>;
    let watched = if keep {
        watched
    } else {
        asked = watched
            .iter()
            .filter(|a| state.history.bodies_wanted(a).unwrap_or(true))
            .cloned()
            .collect();
        &asked[..]
    };
    let _pass = state.begin_pass("bodies");
    let mut last: Option<u64> = None;
    let mut cursor = state.start_block;
    let mut pace = Pace::new(250, 16, BLOCK_WINDOW, state.deadline);
    loop {
        yield_cursor(state, d).await;
        let mut next = Vec::new();
        for a in watched {
            match next_uncovered_for(&state.history, a, cursor, &BLOCK_SCANNED) {
                Ok(n) => next.push((a.clone(), n)),
                Err(e) => {
                    tracing::warn!("address history: block coverage for {a}: {e:#}");
                    state.clear_held();
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
            state.window_failed();
            state.clear_held();
            tracing::error!(
                "address history: block coverage did not advance past {from}; stopping this pass"
            );
            return;
        }
        let mut end = from.saturating_add(pace.span() - 1).min(to);
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
        let started = Instant::now();
        let outcome = body_window(d, state, addresses, (from, end), None, keep).await;
        pace.took(started.elapsed());
        if outcome.is_ok() {
            state.body_rate.store(
                per_minute(end - from + 1, started.elapsed()),
                Ordering::Relaxed,
            );
        }
        match outcome {
            Ok(blocks) => {
                if keep {
                    if let Err(e) = note_scanned(d, state, blocks).await {
                        tracing::warn!(
                            "address history: scanned blocks not kept for the proposer proof: {e:#}"
                        );
                        state.clear_held();
                        keep = false;
                    }
                }
                last = Some(end);
                state.window_done();
                tracing::info!("address history: block bodies read through {end}");
            }
            Err(e) => {
                state.clear_held();
                state.window_failed();
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
    // One ceiling for the whole request, whatever it waits on: reads, the hash check, the queue, a
    // window the cursor is part way through, a trace and a cover in turn. The work inside stops a
    // little short of it, so a range covered by then is still read and served under it.
    let ceiling = Instant::now() + s.deadline;
    let work = ceiling - s.deadline / 20;
    let answer = match tokio::time::timeout_at(ceiling.into(), answer(&s, q, work)).await {
        Ok(a) => a,
        Err(_) => Json(crate::address_history::incomplete(&format!(
            "not answered within the {}s deadline; try again shortly",
            s.deadline.as_secs()
        ))),
    };
    // A conflict found while this request was answered may concern what it read.
    match conflict(&s) {
        Some(why) => Json(crate::address_history::incomplete(&why)),
        None => answer,
    }
}

async fn answer(s: &Arc<ModeState>, q: HashMap<String, String>, deadline: Instant) -> Json<Value> {
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
        // Within the request deadline, or refused: a client's read timeout is not much longer.
        match tokio::time::timeout_at(
            deadline.into(),
            crate::address_proxy::answer(&q, s.chain_id, &forward),
        )
        .await
        {
            Ok(Some(v)) => return Json(v),
            Ok(None) => {}
            Err(_) => {
                return Json(crate::address_history::incomplete(&format!(
                    "not answered within the {}s deadline; ask for a narrower range",
                    s.deadline.as_secs()
                )))
            }
        }
    }
    let get = |k: &str| q.get(k).map(String::as_str);
    if get("module") == Some("account") && get("action") == Some("nuthatchValidatorBlocks") {
        return Json(validator_blocks(&s, &q).await);
    }
    if get("module") == Some("block") && get("action") == Some("getblocknobytime") {
        return Json(block_by_time(&s, &q, deadline).await);
    }
    // A range from before the configured history is refused in the one wording a client reads to ask
    // again from the start, whatever the action and whether or not it named a start.
    if let Some(why) = before_history(&s, &q) {
        return Json(crate::address_history::incomplete(&why));
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
                .ask(deadline, |deadline, reply| Request::Trace {
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
                // Rows already cover the range: only a reorg could change them, and the lowest
                // tracked hash at or above its end standing proves nothing under it moved.
                if w.1 != Need::Produced && answer["status"] == "1" {
                    match stands(&s, w.3, &answer, deadline).await {
                        Some(true) => return Json(answer),
                        Some(false) => {
                            answer = respond(&s, &q).await;
                            continue;
                        }
                        None => {}
                    }
                }
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
    // A gap the cursor cannot cover within the deadline at its measured pace is refused at once,
    // not after holding the request for the whole deadline: the backfill reaches it in the background.
    if let Some(why) = beyond_reach(&s, &want).await {
        return Json(crate::address_history::incomplete(&why));
    }
    match s
        .ask(deadline, |deadline, reply| Request::Cover {
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
            // The cursor may have finished just as the ceiling passed: what is now covered is served,
            // anything else is refused, and nothing waits past the ceiling for either.
            let now = respond(&s, &q).await;
            if matches!(wanted(&s, &q, &now).await, Want::Serve) {
                return Json(now);
            }
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

/// Why an account request starts before the configured history, if it does: `startblock`, or 0
/// when absent, under `start_block`.
fn before_history(s: &ModeState, q: &HashMap<String, String>) -> Option<String> {
    use crate::address_history::Action;
    if q.get("module").map(String::as_str) != Some("account") || q.contains_key("txhash") {
        return None;
    }
    Action::from_etherscan(q.get("action")?)?;
    let start = match q.get("startblock").filter(|v| !v.is_empty()) {
        None => 0,
        Some(v) => v.parse::<u64>().ok()?,
    };
    if let Some(end) = s.end_block.filter(|e| start > *e) {
        return Some(format!("history ends at block {end}"));
    }
    (start < s.start_block).then(|| {
        format!(
            "history starts at block {}; ask from there and use another source before it",
            s.start_block
        )
    })
}

/// The blocks one layer can cover for a request: what it covers in half the deadline at its own
/// measured rate, or `unmeasured` before it has a rate. Layers differ by orders of magnitude: trace
/// discovery tens of thousands of blocks a minute, a block-body scan a few thousand.
fn reach(s: &ModeState, rate: &AtomicU64, unmeasured: u64) -> u64 {
    match rate.load(Ordering::Relaxed) {
        0 => unmeasured,
        per_minute => (per_minute as u128 * s.deadline.as_millis() / 2 / 60_000) as u64,
    }
}

/// `blocks` covered in `took`, per minute: at least 1, so a measured rate is never read as none.
fn per_minute(blocks: u64, took: Duration) -> u64 {
    ((blocks as u128 * 60_000) / took.as_millis().max(1)).max(1) as u64
}

/// Why `want`'s uncovered blocks are too many to cover within the deadline, if they are.
async fn beyond_reach(s: &Arc<ModeState>, want: &(String, Need, u64, u64)) -> Option<String> {
    use crate::address_discovery::{next_uncovered_for, BLOCK_SCANNED, DISCOVERED};
    use crate::address_history::Action;
    let (address, need, from, to) = want.clone();
    // Each layer the request needs, its first uncovered block, and how far it can reach in time.
    let mut layers: Vec<(&'static [Action], u64)> =
        vec![(&DISCOVERED, reach(s, &s.backfill_rate, COVER_WINDOW))];
    if need != Need::Discovered {
        layers.push((&BLOCK_SCANNED, reach(s, &s.body_rate, COVER_BODIES)));
    }
    if need == Need::Produced {
        layers.push((
            &[Action::ProducedBlocks],
            reach(s, &s.produced_rate, COVER_PRODUCED),
        ));
    }
    let sets: Vec<&'static [Action]> = layers.iter().map(|(set, _)| *set).collect();
    let who = address.clone();
    // Overdue relay re-asks run in the same request, one per block owed: they cost the produced-block
    // layer as much as blocks it has yet to cover.
    let (nexts, owed) = blocking(s, move |h| {
        let nexts = sets
            .into_iter()
            .map(|set| next_uncovered_for(h, &who, from, set))
            .collect::<Result<Vec<u64>>>()?;
        let owed = if need == Need::Produced {
            crate::address_mev::overdue(h, unix_now(), Some((from, to)))?.len() as u64
        } else {
            0
        };
        Ok((nexts, owed))
    })
    .await
    .ok()?;
    // The layers run one after another, so their times add up: each layer's share of its own
    // half-deadline reach, summed, must fit in one half-deadline.
    let load: f64 = layers
        .iter()
        .zip(&nexts)
        .enumerate()
        .map(|(i, ((_, reach), next))| {
            let owed = if i == 2 { owed } else { 0 };
            let work = to
                .saturating_sub(*next)
                .saturating_add(u64::from(*next <= to))
                .saturating_add(owed);
            match (work, *reach) {
                (0, _) => 0.0,
                // A measured rate too slow to reach one block in time reaches none.
                (_, 0) => f64::INFINITY,
                (w, r) => w as f64 / r as f64,
            }
        })
        .sum();
    // The polled chain tip, and only that. A zero means the tip has not been read yet: the
    // finalized head sits behind it by the finality depth, and a tip request is not above the chain.
    let chain = s.latest.load(Ordering::Relaxed);
    if chain > 0 && from > chain {
        return Some(format!(
            "the chain head is block {chain}; this range starts at block {from}, above it"
        ));
    }
    if load <= 1.0 {
        return None;
    }
    let next = *nexts.iter().min()?;
    let gap = to
        .saturating_sub(next)
        .saturating_add(u64::from(next <= to));
    let covered = next.saturating_sub(1);
    let covered = if chain > 0 {
        covered.min(chain)
    } else {
        covered
    };
    Some(()).map(|()| {
        format!(
            "covered through block {covered}; the {gap} blocks above it are being backfilled, more than can \
             be covered within the {}s deadline; ask again later",
            s.deadline.as_secs()
        )
    })
}

/// `account/nuthatchValidatorBlocks`, from the validators' own store: complete or refused.
async fn validator_blocks(s: &Arc<ModeState>, q: &HashMap<String, String>) -> Value {
    if let Some(c) = q.get("chainid") {
        if c.parse::<u64>().ok() != Some(s.chain_id) {
            return crate::address_history::unsupported(&format!(
                "chainid {c} is not this nest's chain {}",
                s.chain_id
            ));
        }
    }
    let Some(validators) = s.validators.clone() else {
        return crate::address_history::unsupported(
            "validator blocks are proven on Ethereum mainnet address-history nests only",
        );
    };
    let (from, until) = (s.proposals_from(), s.end_block);
    let q = q.clone();
    tokio::task::spawn_blocking(move || {
        crate::address_validators::respond(&validators, from, until, &q)
    })
    .await
    .unwrap_or_else(|e| crate::address_history::incomplete(&format!("{e}")))
}

/// Whether the covered rows of `answer`, which reach block `to` above the finalized head, still
/// stand: the lowest tracked hash at or above `to` matches the chain, and nothing was written since
/// the answer was read. `Some(false)`: written since, read it again. `None`: ask the cursor.
async fn stands(s: &Arc<ModeState>, to: u64, answer: &Value, deadline: Instant) -> Option<bool> {
    let hash_at = s.hash_at.clone()?;
    let read_at = answer["generation"].as_str()?.parse::<u64>().ok()?;
    let (b, h) = blocking(s, move |h| {
        Ok(h.tracked()?.into_iter().find(|(b, _)| *b >= to))
    })
    .await
    .ok()??;
    let now = tokio::time::timeout_at(deadline.into(), hash_at(b))
        .await
        .ok()?
        .ok()?;
    if now != h {
        return None;
    }
    let generation = blocking(s, |h| h.generation()).await.ok()?;
    Some(generation == read_at)
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
            let watched = h.watched()?.contains(&who);
            if watched && Need::of(action) != Need::Discovered {
                // Here rather than in `cover`: a request beyond reach never gets that far, and its
                // address must still join the background body scan.
                h.ask_bodies(&who)?;
            }
            Ok((
                watched,
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
async fn block_by_time(
    s: &Arc<ModeState>,
    q: &HashMap<String, String>,
    deadline: Instant,
) -> Value {
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
    // A search gets half the deadline; when the last one needed more, the RPC is too busy to finish
    // this one in time either, and it is refused at once rather than held.
    let budget = s.deadline / 2;
    let last = Duration::from_millis(s.search_ms.load(Ordering::Relaxed));
    if last > budget {
        s.search_ms.store(
            last.as_millis().saturating_sub(budget.as_millis() / 2) as u64,
            Ordering::Relaxed,
        );
        return crate::address_history::incomplete(&format!(
            "the RPC took {}s for the last timestamp search, over the {}s this one may take; ask again shortly",
            last.as_secs(),
            budget.as_secs()
        ));
    }
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
    let started = Instant::now();
    let left = deadline.saturating_duration_since(Instant::now());
    let searched = tokio::time::timeout(budget.min(left), search).await;
    s.search_ms
        .store(started.elapsed().as_millis() as u64, Ordering::Relaxed);
    match searched {
        Ok(Ok(Ok(b))) => crate::address_history::ok(
            Value::String(b.to_string()),
            s.history.generation().unwrap_or(0),
        ),
        Ok(Ok(Err(why))) => crate::address_history::incomplete(&why),
        Ok(Err(e)) => crate::address_history::incomplete(&format!("searching the RPC: {e:#}")),
        Err(_) => crate::address_history::incomplete(&format!(
            "the RPC search did not finish within {}s; ask again shortly",
            budget.as_secs()
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
    // rotki ends a sync just behind the tip. Bracket that timestamp within the last eight blocks
    // before searching the whole chain: a genesis-to-head search costs about 25 serial RPC reads
    // on mainnet, which can exhaust the search budget before reaching an otherwise nearby answer.
    // This only narrows the search; the same neighbour and ancestry checks still prove its answer.
    let recent = latest.number.saturating_sub(8);
    let lower = header(forward, at(recent)).await?;
    let (mut lo, mut hi) = if t >= lower.ts {
        (recent, latest.number)
    } else {
        let genesis = if recent == 0 {
            lower
        } else {
            header(forward, at(0)).await?
        };
        if t < genesis.ts {
            return Ok(if after {
                Ok((0, latest, vec![(0, genesis.hash)]))
            } else {
                Err(format!("timestamp {t} is before the first block"))
            });
        }
        (0, recent)
    };
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
    #[serde(default)]
    address: Option<String>,
    /// A validator index, instead of an address.
    #[serde(default)]
    validator: Option<u64>,
    /// Many addresses at once, added in one transaction (RFC-0064). Added only: a deposit set grows.
    #[serde(default)]
    addresses: Option<Vec<String>>,
}

fn watch_list(s: &ModeState, addresses: Vec<String>) -> Result<Value> {
    let validators = match &s.validators {
        Some(v) => v.watched()?,
        None => Vec::new(),
    };
    Ok(json!({ "watched": addresses, "validators": validators }))
}

async fn watched(State(s): State<Arc<ModeState>>) -> (StatusCode, Json<Value>) {
    match blocking(&s, |h| h.watched())
        .await
        .and_then(|w| watch_list(&s, w))
    {
        Ok(v) => (StatusCode::OK, Json(v)),
        Err(e) => failed(StatusCode::INTERNAL_SERVER_ERROR, format!("{e:#}")),
    }
}

async fn watch(
    State(s): State<Arc<ModeState>>,
    Json(b): Json<AddressBody>,
) -> (StatusCode, Json<Value>) {
    dispatch(s, b, true).await
}

async fn unwatch(
    State(s): State<Arc<ModeState>>,
    Json(b): Json<AddressBody>,
) -> (StatusCode, Json<Value>) {
    dispatch(s, b, false).await
}

async fn dispatch(s: Arc<ModeState>, b: AddressBody, add: bool) -> (StatusCode, Json<Value>) {
    if !s.loopback {
        return failed(
            StatusCode::FORBIDDEN,
            "the watched set changes only on a loopback bind",
        );
    }
    match (b.address, b.validator, b.addresses) {
        (Some(a), None, None) => change(s, a, add).await,
        (None, Some(v), None) => change_validator(s, v, add).await,
        (None, None, Some(many)) if add => watch_many(s, many).await,
        (None, None, Some(_)) => failed(
            StatusCode::BAD_REQUEST,
            "a list of addresses is added only; unwatch them one at a time",
        ),
        _ => failed(
            StatusCode::BAD_REQUEST,
            "give one of an address, a validator index or a list of addresses",
        ),
    }
}

async fn change_validator(
    s: Arc<ModeState>,
    validator: u64,
    add: bool,
) -> (StatusCode, Json<Value>) {
    let Some(validators) = s.validators.clone() else {
        return failed(
            StatusCode::BAD_REQUEST,
            "validators are watched on Ethereum mainnet address-history nests only",
        );
    };
    let done = tokio::task::spawn_blocking(move || {
        if add {
            validators.watch(validator)
        } else {
            validators.unwatch(validator)
        }
    })
    .await
    .map_err(anyhow::Error::from)
    .and_then(|r| r);
    if let Err(e) = done {
        return failed(StatusCode::INTERNAL_SERVER_ERROR, format!("{e:#}"));
    }
    if add {
        s.wake.notify_one();
    }
    match blocking(&s, |h| h.watched())
        .await
        .and_then(|w| watch_list(&s, w))
    {
        Ok(v) => (StatusCode::OK, Json(v)),
        Err(e) => failed(StatusCode::INTERNAL_SERVER_ERROR, format!("{e:#}")),
    }
}

/// Add a batch of addresses. The answer is counts, not the list: at a deposit set's size the list
/// would be megabytes per call.
async fn watch_many(s: Arc<ModeState>, addresses: Vec<String>) -> (StatusCode, Json<Value>) {
    let done = blocking(&s, move |h| {
        Ok((h.watch_many(&addresses)?, h.watched_count()?))
    })
    .await;
    match done {
        Ok((added, watched)) => {
            s.wake.notify_one();
            (
                StatusCode::OK,
                Json(json!({ "added": added, "watched": watched })),
            )
        }
        Err(e) => failed(StatusCode::BAD_REQUEST, format!("{e:#}")),
    }
}

async fn change(s: Arc<ModeState>, address: String, add: bool) -> (StatusCode, Json<Value>) {
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
    match done.and_then(|w| watch_list(&s, w)) {
        Ok(v) => {
            if add {
                s.wake.notify_one();
            }
            (StatusCode::OK, Json(v))
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
                "holding": match s.holding() {
                    Some((pass, secs)) => json!({"pass": pass, "secs": secs}),
                    None => Value::Null,
                },
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
    let holding = s.holding().map(|(_, secs)| secs).unwrap_or(0);
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
         # HELP nuthatch_address_history_holding_seconds Seconds the current pass has held the cursor; 0 when none does.\n\
         # TYPE nuthatch_address_history_holding_seconds gauge\n\
         nuthatch_address_history_holding_seconds {holding}\n\
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

/// A store written before finalized hashes were recorded cannot show which chain its finalized
/// history came from, so it is renamed aside, intact, and the nest reads its history again from
/// `start_block`. The accounts it watched are kept. Returns the accounts to open the new store with.
pub fn move_aside_pre_finality(
    path: &Path,
    chain_id: u64,
    configured: &[String],
) -> Result<Vec<String>> {
    if !path.exists() {
        return Ok(configured.to_vec());
    }
    // The lock redb takes, held through the move, so a nest still running on this store is refused
    // rather than left writing to the copy kept aside.
    let held = std::fs::File::open(path)?;
    held.try_lock().map_err(|_| {
        anyhow::anyhow!(
            "{} is open in another process; stop it first",
            path.display()
        )
    })?;
    let Some(accounts) = probe_copy(path)? else {
        return Ok(configured.to_vec());
    };
    // The replacement is built beside the old store, its accounts and removals committed, before
    // anything is moved: a crash at any step leaves the old store where it was, or linked at both
    // names.
    let fresh = with_suffix(path, ".new");
    if fresh.exists() {
        std::fs::remove_file(&fresh)?;
    }
    {
        let h = AddressHistory::open(
            crate::store::Store::open(&fresh)?,
            chain_id,
            &accounts.watched,
        )?;
        for a in &accounts.unwatched {
            h.unwatch(a)?;
        }
    }
    let stamp = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)?
        .as_secs();
    // A hard link never replaces an existing file, so an earlier copy kept under the same second
    // survives.
    let mut n = 0;
    let aside = loop {
        let name = match n {
            0 => with_suffix(path, &format!(".pre-5.2-{stamp}")),
            _ => with_suffix(path, &format!(".pre-5.2-{stamp}-{n}")),
        };
        match std::fs::hard_link(path, &name) {
            Ok(()) => break name,
            Err(e) if e.kind() == std::io::ErrorKind::AlreadyExists => n += 1,
            Err(e) => {
                return Err(e)
                    .with_context(|| format!("keeping {} as {}", path.display(), name.display()))
            }
        }
    };
    std::fs::rename(&fresh, path)
        .with_context(|| format!("putting the new store at {}", path.display()))?;
    drop(held);
    tracing::warn!(
        "address history: {} was written before finalized block hashes were recorded, so it cannot \
         show its history is this chain's; kept it, untouched, as {} and reading history again from \
         start_block",
        path.display(),
        aside.display()
    );
    Ok(configured.to_vec())
}

fn with_suffix(path: &Path, suffix: &str) -> std::path::PathBuf {
    let mut p = path.as_os_str().to_owned();
    p.push(suffix);
    p.into()
}

/// [`crate::address_history::pre_finality`] on a copy, so opening it, which redb may write to,
/// never touches the store itself.
fn probe_copy(path: &Path) -> Result<Option<crate::address_history::Accounts>> {
    let probe = with_suffix(path, ".probe");
    std::fs::copy(path, &probe)?;
    let found = crate::address_history::pre_finality(&probe);
    std::fs::remove_file(&probe)?;
    found
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
    let path = dir.join(crate::config::DB_FILE);
    let addresses = move_aside_pre_finality(&path, config.nest.chain_id, &ah.addresses)?;
    let store = crate::store::Store::open_with_cache(&path, STORE_CACHE_BYTES)?;
    let history = AddressHistory::open(store, config.nest.chain_id, &addresses)?;

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
        .with_sweep(ah.sweep.clone())
        .with_forward(forward(discovery.clone(), verified.clone()))
        .with_counter(counter(discovery.clone()))
        .with_hash_at({
            let d = discovery.clone();
            let verified = verified.clone();
            Arc::new(move |b: u64| {
                let (d, verified) = (d.clone(), verified.clone());
                Box::pin(async move {
                    // Not before the cursor has checked the RPC's chain.
                    if verified.get().is_none() {
                        bail!("the RPC's chain is not checked yet");
                    }
                    block_hash(&d.main, b).await
                })
            })
        });
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
        let source = Arc::new(crate::address_partitions::Source::open(
            &m.url,
            chain_id,
            dir.join("partitions"),
            m.cache_mb * 1024 * 1024,
        )?);
        // Without a `to_block`, as far as the mirror has published now; the cursor looks again
        // every poll, and the blocks above it are read from the RPC meanwhile.
        let to = match m.to_block {
            Some(t) => Some(t),
            None => source
                .published_end(crate::address_partitions::span_of(m.from_block).0)
                .await
                .unwrap_or_else(|e| {
                    tracing::warn!("address history: how far the mirror has published: {e:#}");
                    None
                }),
        };
        state.history.keep_verified_only(
            &crate::address_discovery::BLOCK_SCANNED,
            (m.from_block, to.unwrap_or(m.from_block.saturating_sub(1))),
        )?;
        tracing::info!(
            "address history: blocks {} to {} from verified partitions at the configured mirror, which \
             sees this machine's IP and the block ranges asked for, never an address",
            m.from_block,
            match (m.to_block, to) {
                (Some(t), _) => t.to_string(),
                (None, Some(t)) => format!("{t} (as far as it has published, checked every poll)"),
                (None, None) => "none yet (checked every poll)".into(),
            }
        );
        state = match m.to_block {
            Some(t) => state.with_mirror(source, (m.from_block, t)),
            None => state.with_open_mirror(source, m.from_block, to),
        };
    }
    if chain_id == 1 {
        let validators = crate::address_validators::Validators::open(
            state.history.store().clone(),
            &ah.validators,
        )?;
        // From the environment first, so a keyed URL stays out of nuthatch.toml.
        let beacon_url = std::env::var("NUTHATCH_BEACON_URL")
            .ok()
            .filter(|u| !u.is_empty())
            .or_else(|| ah.beacon_url.clone());
        let beacon = match &beacon_url {
            Some(url) => {
                tracing::info!(
                    "address history: proposer catalogue spans the mirror lacks are built from the configured beacon, which sees this machine's IP and the beacon roots asked for"
                );
                Some(Arc::new(crate::address_proposers::Beacon::new(
                    Some(url.clone()),
                    None,
                )?))
            }
            None => None,
        };
        state = state.with_validators(validators, beacon);
    } else if !ah.validators.is_empty() || ah.beacon_url.is_some() {
        bail!("[address_history] validators and beacon_url are for Ethereum mainnet; this nest is on chain {chain_id}");
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

    /// Headers by number, and the one it calls finalized.
    struct Headers {
        by_number: BTreeMap<u64, Value>,
        finalized: Value,
    }
    impl Rpc for Headers {
        async fn call(&self, method: &str, params: Value) -> Result<Value> {
            if method == "trace_block" {
                if let Some(t) = TRACES.with(|t| t.borrow().clone()) {
                    return Ok(t);
                }
            }
            if method != "eth_getBlockByNumber" {
                bail!("{method} is not scripted");
            }
            match params[0].as_str() {
                Some("finalized") => Ok(self.finalized.clone()),
                Some(n) => {
                    let n = u64::from_str_radix(n.trim_start_matches("0x"), 16)?;
                    Ok(self.by_number.get(&n).cloned().unwrap_or(Value::Null))
                }
                None => bail!("no block tag"),
            }
        }
    }
    thread_local! {
        static TRACES: std::cell::RefCell<Option<Value>> = const { std::cell::RefCell::new(None) };
    }

    #[tokio::test(flavor = "current_thread")]
    async fn a_proven_block_carries_the_builders_payment() {
        use crate::address_proposers::{tests as pt, Proven};
        let (blocks, _, _) = pt::chain(1);
        let block = blocks[0].clone();
        let hash = format!("{:#x}", block.hash);
        let builder = format!("{:#x}", block.header.beneficiary);
        let proposer = "0x00000000000000000000000000000000000000a1";
        TRACES.with(|t| {
            *t.borrow_mut() = Some(json!([{
                "blockHash": hash, "transactionPosition": 0, "transactionHash": "0x01",
                "traceAddress": [], "type": "call",
                "action": {"callType": "call", "from": builder, "to": proposer, "value": "0x9"},
            }]))
        });
        let relay_hash = hash.clone();
        let relays: crate::address_mev::Relays = Arc::new(move |name: &'static str, b: u64| {
            let h = relay_hash.clone();
            Box::pin(async move {
                Ok(if name == "ultrasound" {
                    vec![json!({"block_number": b.to_string(), "block_hash": h,
                        "proposer_fee_recipient": proposer, "value": "9",
                        "builder_pubkey": "0xb", "proposer_pubkey": "0xp"})]
                } else {
                    vec![]
                })
            })
        });
        let dir = tempfile::tempdir().unwrap();
        let store = Store::open(&dir.path().join("t.redb")).unwrap();
        let history = AddressHistory::open(store, 1, &[]).unwrap();
        let state = ModeState::new(history, 1, Duration::from_secs(300), true).with_relays(relays);
        let make = || Headers {
            by_number: BTreeMap::new(),
            finalized: Value::Null,
        };
        let d = Discoverer::new(Counted::new(make()), Counted::new(make()));
        let p = Proven {
            block,
            slot: 1,
            proposer_index: 7,
        };
        let stored = proven_row(&d, &state, &p).await;
        assert_eq!(stored.row["mev"], "relay");
        assert_eq!(stored.row["paymentTx"], "0x01");
        assert_eq!(stored.row["paymentValue"], "9");
        assert!(!stored.meta.contains_key("paymentOwed"));
        // A trace answer that is not a list is no evidence: the payment stays owed.
        TRACES.with(|t| *t.borrow_mut() = Some(Value::Null));
        let stored = proven_row(&d, &state, &p).await;
        assert_eq!(stored.row["paymentValue"], "");
        assert_eq!(stored.meta["paymentOwed"], true);
        TRACES.with(|t| *t.borrow_mut() = None);
    }

    fn header_json(h: &alloy_consensus::Header) -> Value {
        let mut v = serde_json::to_value(h).unwrap();
        v["hash"] = json!(format!("{:#x}", h.hash_slow()));
        v
    }

    #[tokio::test]
    async fn mirror_spans_hang_from_the_finalized_block() {
        use crate::address_partitions::{encode, partition_key, Published, Source, SPAN};
        use crate::address_proposers::{catalogue_key, encode as encode_catalogue, tests as pt};
        let start = 23_560_000u64;
        let end = start + SPAN - 1;
        // The span, two blocks above it, and the finalized block.
        let (blocks, rows, anchor) = pt::chain_from(start, 12_800_000, SPAN as usize + 2);
        let mirror = tempfile::tempdir().unwrap();
        let put = |key: String, bytes: Vec<u8>| {
            let path = mirror.path().join(key);
            std::fs::create_dir_all(path.parent().unwrap()).unwrap();
            std::fs::write(path, bytes).unwrap();
        };
        let published: Vec<Published> = blocks[..SPAN as usize]
            .iter()
            .map(|b| {
                let mut header = Vec::new();
                alloy_rlp::Encodable::encode(&b.header, &mut header);
                let mut empty = Vec::new();
                alloy_rlp::encode_list::<alloy_rpc_types_eth::Withdrawal, _>(&[], &mut empty);
                Published {
                    number: b.header.number,
                    header,
                    withdrawals: Some(empty),
                }
            })
            .collect();
        put(partition_key(1, start), encode(&published).unwrap());
        put(
            catalogue_key(1, start),
            encode_catalogue(&rows[..SPAN as usize]).unwrap(),
        );
        let bridge: BTreeMap<u64, Value> = blocks[SPAN as usize..]
            .iter()
            .map(|b| (b.header.number, header_json(&b.header)))
            .collect();
        let validator = rows[5].proposer_index;
        let run = |finalized: alloy_consensus::Header| {
            let dir = tempfile::tempdir().unwrap();
            let store = Store::open(&dir.path().join("t.redb")).unwrap();
            let history = AddressHistory::open(store.clone(), 1, &[]).unwrap();
            let validators =
                crate::address_validators::Validators::open(store, &[validator]).unwrap();
            let source = Arc::new(
                Source::open(
                    mirror.path().to_str().unwrap(),
                    1,
                    dir.path().join("cache"),
                    1 << 30,
                )
                .unwrap(),
            );
            let state = ModeState::new(history, 1, Duration::from_secs(300), true)
                .with_range(start, None, 64)
                .with_validators(validators.clone(), None)
                .with_mirror(source.clone(), (start, end));
            let make = || Headers {
                by_number: bridge.clone(),
                finalized: header_json(&finalized),
            };
            let d = Discoverer::new(Counted::new(make()), Counted::new(make()));
            async move {
                let done = mirror_spans(
                    &d,
                    &state,
                    &validators,
                    &[validator],
                    &source,
                    (start, end),
                    &finalized,
                    finalized.number - 1,
                    None,
                )
                .await;
                (
                    done,
                    validators.covers(validator, (start, end)).unwrap(),
                    dir,
                )
            }
        };
        let (done, covered, _dir) = run(anchor.clone()).await;
        // The span is proven; the catalogue's first span, under it, is not in this test mirror.
        assert!(covered);
        assert!(
            format!("{:#}", done.unwrap_err()).contains("no block partition for [23550000"),
            "only the absent first span fails"
        );
        // A mirror whose from_block starts inside the span still proves it: from_block bounds only
        // what address history reads.
        {
            let dir = tempfile::tempdir().unwrap();
            let store = Store::open(&dir.path().join("t.redb")).unwrap();
            let history = AddressHistory::open(store.clone(), 1, &[]).unwrap();
            let validators =
                crate::address_validators::Validators::open(store, &[validator]).unwrap();
            let source = Arc::new(
                Source::open(
                    mirror.path().to_str().unwrap(),
                    1,
                    dir.path().join("cache"),
                    1 << 30,
                )
                .unwrap(),
            );
            let state = ModeState::new(history, 1, Duration::from_secs(300), true)
                .with_range(start + 5_000, None, 64)
                .with_validators(validators.clone(), None)
                .with_mirror(source.clone(), (start + 5_000, end));
            let make = || Headers {
                by_number: bridge.clone(),
                finalized: header_json(&anchor),
            };
            let d = Discoverer::new(Counted::new(make()), Counted::new(make()));
            // The spans under this one are not in the test mirror; this one is proven first.
            let _ = mirror_spans(
                &d,
                &state,
                &validators,
                &[validator],
                &source,
                (start + 5_000, end),
                &anchor,
                anchor.number - 1,
                None,
            )
            .await;
            assert!(validators.covers(validator, (start, end)).unwrap());
        }
        // A whole validator pass with end_block inside the partition still proves the span: proofs
        // run to the finalized block, and end_block bounds only what is served.
        {
            let dir = tempfile::tempdir().unwrap();
            let store = Store::open(&dir.path().join("t.redb")).unwrap();
            let history = AddressHistory::open(store.clone(), 1, &[]).unwrap();
            let validators =
                crate::address_validators::Validators::open(store, &[validator]).unwrap();
            let source = Arc::new(
                Source::open(
                    mirror.path().to_str().unwrap(),
                    1,
                    dir.path().join("cache"),
                    1 << 30,
                )
                .unwrap(),
            );
            let state = ModeState::new(history, 1, Duration::from_secs(300), true)
                .with_range(start, Some(start + 5), 64)
                .with_validators(validators.clone(), None)
                .with_mirror(source, (start, end));
            let make = || Headers {
                by_number: bridge.clone(),
                finalized: header_json(&anchor),
            };
            let d = Discoverer::new(Counted::new(make()), Counted::new(make()));
            validator_pass(&d, &state).await;
            assert!(validators.covers(validator, (start, end)).unwrap());
        }
        // The mirror and the numbered blocks agree with each other, but the finalized block is on
        // another chain: nothing is proven.
        let mut other = anchor.clone();
        other.parent_hash = alloy_primitives::B256::repeat_byte(4);
        let (done, covered, _dir) = run(other).await;
        assert!(format!("{:#}", done.unwrap_err()).contains("do not lead to it"));
        assert!(!covered);
    }

    #[tokio::test]
    async fn the_bridge_above_the_mirror_is_read_once() {
        use crate::address_partitions::{encode, partition_key, Published, Source, SPAN};
        use crate::address_proposers::{
            catalogue_key, encode as encode_catalogue, tests as pt, Beacon,
        };
        let start = 23_560_000u64;
        let end = start + SPAN - 1;
        // The span, the two blocks above it, and the finalized block.
        let (blocks, rows, anchor) = pt::chain_from(start, 12_800_000, SPAN as usize + 2);
        let mirror = tempfile::tempdir().unwrap();
        let put = |key: String, bytes: Vec<u8>| {
            let path = mirror.path().join(key);
            std::fs::create_dir_all(path.parent().unwrap()).unwrap();
            std::fs::write(path, bytes).unwrap();
        };
        let published: Vec<Published> = blocks[..SPAN as usize]
            .iter()
            .map(|b| {
                let mut header = Vec::new();
                alloy_rlp::Encodable::encode(&b.header, &mut header);
                let mut empty = Vec::new();
                alloy_rlp::encode_list::<alloy_rpc_types_eth::Withdrawal, _>(&[], &mut empty);
                Published {
                    number: b.header.number,
                    header,
                    withdrawals: Some(empty),
                }
            })
            .collect();
        put(partition_key(1, start), encode(&published).unwrap());
        put(
            catalogue_key(1, start),
            encode_catalogue(&rows[..SPAN as usize]).unwrap(),
        );
        let bridge: BTreeMap<u64, Value> = blocks[SPAN as usize..]
            .iter()
            .map(|b| (b.header.number, header_json(&b.header)))
            .collect();
        // The file is named from the first row's slot, and the span's slots are an earlier window.
        let cache = tempfile::tempdir().unwrap();
        pt::write_cache(cache.path(), &rows[SPAN as usize..], &[]);
        let beacon = Arc::new(Beacon::new(None, Some(cache.path().to_path_buf())).unwrap());
        // Proposed none of these blocks, so the proof records coverage and reads no reward.
        let validator = 1;
        let dir = tempfile::tempdir().unwrap();
        let store = Store::open(&dir.path().join("t.redb")).unwrap();
        let history = AddressHistory::open(store.clone(), 1, &[]).unwrap();
        let validators = crate::address_validators::Validators::open(store, &[validator]).unwrap();
        let source = Arc::new(
            Source::open(
                mirror.path().to_str().unwrap(),
                1,
                dir.path().join("cache"),
                1 << 30,
            )
            .unwrap(),
        );
        let state = ModeState::new(history, 1, Duration::from_secs(300), true)
            .with_range(start, None, 64)
            .with_validators(validators.clone(), Some(beacon))
            .with_mirror(source, (start, end));
        let make = || Headers {
            by_number: bridge.clone(),
            finalized: header_json(&anchor),
        };
        let d = Discoverer::new(Counted::new(make()), Counted::new(make()));
        validator_pass(&d, &state).await;
        assert_eq!(
            d.main.calls().get("eth_getBlockByNumber").copied(),
            Some(3),
            "the finalized block, then the two above the mirror, and not again to link them"
        );
        assert_eq!(d.main.calls().get("eth_getBlockByHash").copied(), None);
        assert!(validators.covers(validator, (start, end)).unwrap());
        assert!(validators
            .covers(validator, (end + 1, anchor.number - 1))
            .unwrap());
    }

    #[tokio::test]
    async fn a_scanned_bridge_is_not_fetched_again() {
        use crate::address_proposers::{
            tests as pt, Beacon, CATALOGUE_FROM_BLOCK, CATALOGUE_FROM_SLOT,
        };
        // At the catalogue, not at Dencun: a scan below the catalogue never starts a proof.
        // The body pace starts at 250, so the finalized block is a second window.
        let (blocks, rows, anchor) = pt::chain_from(CATALOGUE_FROM_BLOCK, CATALOGUE_FROM_SLOT, 250);
        let cache = tempfile::tempdir().unwrap();
        pt::write_cache(cache.path(), &rows, &[]);
        let beacon = Arc::new(Beacon::new(None, Some(cache.path().to_path_buf())).unwrap());
        let mut by_number: BTreeMap<u64, Value> = blocks
            .iter()
            .map(|b| (b.header.number, header_json(&b.header)))
            .collect();
        by_number.insert(anchor.number, header_json(&anchor));
        for v in by_number.values_mut() {
            // Past Shanghai a body without its withdrawals array fails the scan.
            v["withdrawals"] = json!([]);
        }
        let dir = tempfile::tempdir().unwrap();
        let store = Store::open(&dir.path().join("t.redb")).unwrap();
        let history = AddressHistory::open(store.clone(), 1, &[ALICE.into()]).unwrap();
        // Proposed none of these blocks, so the proof records coverage and reads no reward.
        let validators = crate::address_validators::Validators::open(store, &[1]).unwrap();
        let state = ModeState::new(history, 1, Duration::from_secs(300), true)
            .with_range(CATALOGUE_FROM_BLOCK, None, 64)
            .with_validators(validators.clone(), Some(beacon));
        let make = || Headers {
            by_number: by_number.clone(),
            finalized: {
                let mut v = header_json(&anchor);
                v["withdrawals"] = json!([]);
                v
            },
        };
        let d = Discoverer::new(Counted::new(make()), Counted::new(make()));
        scan_bodies(&d, &state, &[ALICE.to_string()], anchor.number, None).await;
        let before = d
            .main
            .calls()
            .get("eth_getBlockByNumber")
            .copied()
            .unwrap_or(0);
        validator_pass(&d, &state).await;
        let after = d
            .main
            .calls()
            .get("eth_getBlockByNumber")
            .copied()
            .unwrap_or(0);
        assert_eq!(
            after - before,
            1,
            "the finalized tag, and not the blocks the body scan already read"
        );
        assert_eq!(d.main.calls().get("eth_getBlockByHash").copied(), None);
        assert!(validators
            .covers(1, (CATALOGUE_FROM_BLOCK, anchor.number - 1))
            .unwrap());
    }

    #[tokio::test]
    async fn a_block_written_with_relays_off_is_asked_once_they_are_on() {
        let dir = tempfile::tempdir().unwrap();
        let store = Store::open(&dir.path().join("t.redb")).unwrap();
        let history = AddressHistory::open(store.clone(), 1, &[]).unwrap();
        let validators = crate::address_validators::Validators::open(store, &[7]).unwrap();
        let mut row = crate::address_mev::no_mev();
        row.insert("blockHash".into(), json!("0xbb"));
        row.insert(
            "feeRecipient".into(),
            json!("0x00000000000000000000000000000000000000f1"),
        );
        row.insert("rewardStatus".into(), json!("verified"));
        let mut found = BTreeMap::new();
        found.insert(
            7,
            vec![crate::address_validators::Stored {
                block: 100,
                row,
                meta: serde_json::Map::new(),
            }],
        );
        validators
            .record_span((100, 199), &[7], &found, validators.generation().unwrap())
            .unwrap();
        let relays: crate::address_mev::Relays = Arc::new(|name: &'static str, block: u64| {
            Box::pin(async move {
                Ok(if name == "ultrasound" {
                    vec![
                        json!({"block_number": block.to_string(), "block_hash": "0xbb",
                        "proposer_fee_recipient": "0x00000000000000000000000000000000000000a1",
                        "value": "9", "builder_pubkey": "0xb", "proposer_pubkey": "0xp"}),
                    ]
                } else {
                    vec![]
                })
            })
        });
        let make = || Headers {
            by_number: BTreeMap::new(),
            finalized: Value::Null,
        };
        let d = Discoverer::new(Counted::new(make()), Counted::new(make()));
        let mev = |v: &crate::address_validators::Validators| {
            v.all().unwrap()[0].1.row["mev"]
                .as_str()
                .unwrap()
                .to_string()
        };
        // Relays off: nothing to ask, nothing changes.
        let off = ModeState::new(history.clone(), 1, Duration::from_secs(300), true)
            .with_validators(validators.clone(), None);
        validator_retries(&d, &off, &validators).await;
        assert_eq!(mev(&validators), "unrecorded");
        // Relays on: the block that was never asked is asked now.
        let on = ModeState::new(history, 1, Duration::from_secs(300), true)
            .with_validators(validators.clone(), None)
            .with_relays(relays);
        validator_retries(&d, &on, &validators).await;
        let stored = &validators.all().unwrap()[0].1;
        assert_eq!(stored.row["mev"], "relay");
        assert_eq!(stored.row["mevReward"], "9");
        assert!(stored.meta.contains_key("mevFirstAsked"));
        // The trace for its payment could not be read: owed, not claimed absent.
        assert_eq!(stored.meta["paymentOwed"], true);
    }

    #[tokio::test]
    async fn rows_that_keep_failing_do_not_starve_the_rest() {
        let dir = tempfile::tempdir().unwrap();
        let store = Store::open(&dir.path().join("t.redb")).unwrap();
        let history = AddressHistory::open(store.clone(), 1, &[]).unwrap();
        let validators = crate::address_validators::Validators::open(store, &[7]).unwrap();
        let row = |owed: bool| {
            let mut row = crate::address_mev::no_mev();
            row.insert("blockHash".into(), json!("0xbb"));
            row.insert("rewardStatus".into(), json!("verified"));
            let mut meta = serde_json::Map::new();
            if owed {
                // Relay evidence already in, its payment unreadable for good.
                row.insert("mev".into(), json!("relay"));
                row.insert("mevRecipient".into(), json!("0xa1"));
                meta.insert("paymentOwed".into(), json!(true));
            }
            (row, meta)
        };
        let mut stored: Vec<crate::address_validators::Stored> = (1..=70)
            .map(|b| {
                let (row, meta) = row(true);
                crate::address_validators::Stored {
                    block: b,
                    row,
                    meta,
                }
            })
            .collect();
        let (row, meta) = row(false);
        stored.push(crate::address_validators::Stored {
            block: 150,
            row,
            meta,
        });
        let mut found = BTreeMap::new();
        found.insert(7, stored);
        validators
            .record_span((1, 199), &[7], &found, validators.generation().unwrap())
            .unwrap();
        let relays: crate::address_mev::Relays = Arc::new(|name: &'static str, b: u64| {
            Box::pin(async move {
                Ok(if name == "titan" {
                    vec![json!({"block_number": b.to_string(), "block_hash": "0xbb",
                        "proposer_fee_recipient": "0xa1", "value": "3",
                        "builder_pubkey": "0xb", "proposer_pubkey": "0xp"})]
                } else {
                    vec![]
                })
            })
        });
        let state = ModeState::new(history, 1, Duration::from_secs(300), true)
            .with_validators(validators.clone(), None)
            .with_relays(relays);
        let make = || Headers {
            by_number: BTreeMap::new(),
            finalized: Value::Null,
        };
        let d = Discoverer::new(Counted::new(make()), Counted::new(make()));
        for _ in 0..2 {
            validator_retries(&d, &state, &validators).await;
        }
        let late = validators
            .all()
            .unwrap()
            .into_iter()
            .find(|(_, s)| s.block == 150)
            .unwrap()
            .1;
        assert_eq!(late.row["mev"], "relay", "the late row was reached");
    }

    #[tokio::test]
    async fn an_open_mirror_reads_as_far_as_it_has_published() {
        use crate::address_partitions::{partition_key, Source, SPAN};
        let mirror = tempfile::tempdir().unwrap();
        let publish = |spans: std::ops::Range<u64>| {
            for i in spans {
                let path = mirror.path().join(partition_key(1, i * SPAN));
                std::fs::create_dir_all(path.parent().unwrap()).unwrap();
                std::fs::write(path, b"x").unwrap();
            }
        };
        publish(0..38);
        let dir = tempfile::tempdir().unwrap();
        let source = Arc::new(
            Source::open(
                mirror.path().to_str().unwrap(),
                1,
                dir.path().join("c"),
                1 << 30,
            )
            .unwrap(),
        );
        assert_eq!(source.published_end(0).await.unwrap(), Some(38 * SPAN - 1));
        assert_eq!(source.published_end(40 * SPAN).await.unwrap(), None);
        let history = AddressHistory::open(
            Store::open(&dir.path().join("t.redb")).unwrap(),
            1,
            &[ALICE.into()],
        )
        .unwrap();
        // Withdrawals the RPC scan covered above what the mirror had published.
        let g = history.generation().unwrap();
        history
            .record(
                crate::address_history::Action::BeaconWithdrawals,
                ALICE,
                &[],
                (38 * SPAN, 41 * SPAN - 1),
                g,
            )
            .unwrap();
        let s = ModeState::new(history.clone(), 1, Duration::from_secs(300), true)
            .with_open_mirror(source.clone(), 0, Some(38 * SPAN - 1));
        assert_eq!(s.mirror_blocks().unwrap().1, (0, 38 * SPAN - 1));
        // The publisher extends the mirror; the next poll reads further.
        publish(38..41);
        s.refresh_mirror().await;
        assert_eq!(s.mirror_blocks().unwrap().1, (0, 41 * SPAN - 1));
        // The RPC-scanned coverage there is set aside as the mirror takes those blocks over.
        assert!(history
            .coverage(crate::address_history::Action::BeaconWithdrawals, ALICE)
            .unwrap()
            .is_empty());
    }

    #[tokio::test]
    async fn the_bridge_is_linked_across_its_chunks() {
        use crate::address_proposers::{linked_to, tests as pt};
        let (blocks, _, anchor) = pt::chain_from(23_560_000, 12_800_000, 1_500);
        let map = |bs: &[crate::address_partitions::VerifiedBlock]| -> BTreeMap<u64, Value> {
            bs.iter()
                .map(|b| (b.header.number, header_json(&b.header)))
                .collect()
        };
        let rpc = |by_number: BTreeMap<u64, Value>| Headers {
            by_number,
            finalized: Value::Null,
        };
        let (lo, hi) = (23_560_000, 23_561_499);
        let served = AtomicU64::new(0);
        let between = || -> futures::future::BoxFuture<'_, ()> {
            served.fetch_add(1, Ordering::Relaxed);
            Box::pin(async {})
        };
        let first = linked_to(&rpc(map(&blocks)), (lo, hi), &anchor, &between)
            .await
            .unwrap();
        assert_eq!(first.number, lo);
        // 1,500 blocks in chunks of 128: waiting requests are served after each of the 12.
        assert_eq!(served.load(Ordering::Relaxed), 12);
        // Block 23,561,024 opens a chunk. The second chunk is a chain of its own leading to
        // its own anchor: only the link between the chunks is broken.
        let mut forked = blocks.clone();
        let mut parent = alloy_primitives::B256::repeat_byte(5);
        for b in forked[1_024..].iter_mut() {
            b.header.parent_hash = parent;
            b.hash = b.header.hash_slow();
            parent = b.hash;
        }
        let mut other = anchor.clone();
        other.parent_hash = parent;
        let e = linked_to(&rpc(map(&forked)), (lo, hi), &other, &|| Box::pin(async {}))
            .await
            .unwrap_err();
        assert!(format!("{e:#}").contains("does not follow"), "{e:#}");
    }

    #[tokio::test]
    async fn a_locally_built_tail_must_reach_the_finalized_block() {
        use crate::address_proposers::{tests as pt, Beacon, DENEB_BLOCK};
        // Three chunks: 128, 128 and 44 blocks, newest first.
        let (blocks, rows, successor) = pt::chain(300);
        let keep: Vec<u64> = rows.iter().map(|r| r.proposer_index).collect();
        let dir = tempfile::tempdir().unwrap();
        pt::write_cache(dir.path(), &rows, &[]);
        let beacon = Beacon::new(None, Some(dir.path().to_path_buf())).unwrap();
        let by_number: BTreeMap<u64, Value> = blocks
            .iter()
            .map(|b| (b.header.number, header_json(&b.header)))
            .collect();
        let rpc = |finalized: &alloy_consensus::Header| {
            let make = || Headers {
                by_number: by_number.clone(),
                finalized: header_json(finalized),
            };
            Discoverer::new(Counted::new(make()), Counted::new(make()))
        };
        let served = AtomicU64::new(0);
        let between = || -> futures::future::BoxFuture<'_, ()> {
            served.fetch_add(1, Ordering::Relaxed);
            Box::pin(async {})
        };
        let (to, proven, _) = prove_tail(
            &rpc(&successor),
            &beacon,
            DENEB_BLOCK,
            DENEB_BLOCK + 299,
            &successor,
            &keep,
            &between,
        )
        .await
        .unwrap();
        assert_eq!(to, DENEB_BLOCK + 299);
        assert_eq!(proven.len(), 300);
        assert_eq!(
            served.load(Ordering::Relaxed),
            2,
            "waiting requests served between the chunks"
        );
        // The RPC served a self-consistent range, but the finalized block is on another chain.
        let mut other = successor.clone();
        other.parent_hash = alloy_primitives::B256::repeat_byte(3);
        let e = prove_tail(
            &rpc(&other),
            &beacon,
            DENEB_BLOCK,
            DENEB_BLOCK + 299,
            &other,
            &keep,
            &|| Box::pin(async {}),
        )
        .await
        .unwrap_err();
        assert!(format!("{e:#}").contains("not the successor"), "{e:#}");
    }

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

    /// rotki asks from block 0, or names no start: every account action must answer in the wording
    /// rotki reads to ask again from the configured start.
    #[tokio::test]
    async fn a_range_before_the_configured_history_names_where_it_starts() {
        let dir = tempfile::tempdir().unwrap();
        let store = Store::open(&dir.path().join("t.redb")).unwrap();
        let history = AddressHistory::open(store, 1, &[ALICE.into()]).unwrap();
        let s = Arc::new(
            ModeState::new(history, 1, Duration::from_secs(300), true)
                .with_range(25_700_000, None, 64),
        );
        let actions = [
            "txlist",
            "txlistinternal",
            "tokentx",
            "tokennfttx",
            "token1155tx",
            "txsBeaconWithdrawal",
            "getminedblocks",
            "nuthatchProducedBlocks",
        ];
        // A start above end_block is refused too, never answered "No transactions found".
        let ended = Arc::new(
            ModeState::new(s.history.clone(), 1, Duration::from_secs(300), true).with_range(
                25_700_000,
                Some(25_800_000),
                64,
            ),
        );
        for action in actions {
            let (_, v) = call(
                router(ended.clone()),
                "GET",
                &format!("/api?module=account&action={action}&address={ALICE}&startblock=25800001"),
                None,
            )
            .await;
            assert!(
                v["result"]
                    .as_str()
                    .unwrap_or_default()
                    .contains("history ends at block 25800000"),
                "{action}: {v}"
            );
        }
        for action in actions {
            for start in ["", "&startblock=0", "&startblock=25699999"] {
                let (_, v) = call(
                    router(s.clone()),
                    "GET",
                    &format!("/api?module=account&action={action}&address={ALICE}{start}"),
                    None,
                )
                .await;
                let result = v["result"].as_str().unwrap_or_default().to_string();
                // rotki's pattern: `history starts at block (\d+)`.
                let n = result.split("history starts at block ").nth(1).map(|r| {
                    r.chars()
                        .take_while(char::is_ascii_digit)
                        .collect::<String>()
                });
                assert_eq!(n.as_deref(), Some("25700000"), "{action}{start}: {v}");
            }
        }
    }

    /// The deadline is one ceiling from the request's arrival: a slow hash check, then a queue behind
    /// a background window that overruns, together end in a refusal at the ceiling and never later.
    #[tokio::test]
    async fn a_request_is_refused_at_its_ceiling_while_a_window_overruns() {
        let dir = tempfile::tempdir().unwrap();
        let store = Store::open(&dir.path().join("t.redb")).unwrap();
        let history = AddressHistory::open(store, 1, &[ALICE.into()]).unwrap();
        history.set_head(100).unwrap();
        let mut record = serde_json::Map::new();
        record.insert("blockNumber".into(), json!("150"));
        let rows = vec![crate::address_history::Row {
            block: 150,
            tx_index: 0,
            position: 0,
            record,
        }];
        let g = history.generation().unwrap();
        history
            .record_anchored(
                crate::address_history::Action::TxList,
                &ALICE.to_lowercase(),
                &rows,
                (0, 200),
                g,
                Some((200, [7; 32])),
            )
            .unwrap();
        // The hash moved, after 600 ms of the 1 s ceiling: the request goes to the cursor.
        let moved: HashAt = Arc::new(|_| {
            Box::pin(async {
                tokio::time::sleep(Duration::from_millis(600)).await;
                Ok([8; 32])
            })
        });
        let s = Arc::new(
            ModeState::new(history, 1, Duration::from_secs(300), true)
                .with_deadline(Duration::from_secs(1))
                .with_hash_at(moved),
        );
        // The cursor is three seconds into a window before it looks at the queue.
        let cursor = s.clone();
        tokio::spawn(async move {
            tokio::time::sleep(Duration::from_secs(3)).await;
            serve_queued(&cursor, &()).await;
        });
        let path =
            format!("/api?module=account&action=txlist&address={ALICE}&startblock=0&endblock=180");
        let started = Instant::now();
        let (_, v) = call(router(s), "GET", &path, None).await;
        let took = started.elapsed();
        assert!(
            v["result"].as_str().unwrap().contains("deadline"),
            "{v} after {took:?}"
        );
        assert!(
            took >= Duration::from_millis(900) && took < Duration::from_millis(1_300),
            "{took:?}"
        );
    }

    /// A covered range above the finalized head is served after one hash check, without waiting
    /// for the cursor (none runs here); a moved hash sends it to the cursor, which cannot answer
    /// within the deadline, so it is refused.
    #[tokio::test]
    async fn a_covered_range_above_finality_answers_on_one_hash_check() {
        let dir = tempfile::tempdir().unwrap();
        let store = Store::open(&dir.path().join("t.redb")).unwrap();
        let history = AddressHistory::open(store, 1, &[ALICE.into()]).unwrap();
        history.set_head(100).unwrap();
        let mut record = serde_json::Map::new();
        record.insert("blockNumber".into(), json!("150"));
        record.insert("hash".into(), json!("0x01"));
        let rows = vec![crate::address_history::Row {
            block: 150,
            tx_index: 0,
            position: 0,
            record,
        }];
        let g = history.generation().unwrap();
        history
            .record_anchored(
                crate::address_history::Action::TxList,
                &ALICE.to_lowercase(),
                &rows,
                (0, 200),
                g,
                Some((200, [7; 32])),
            )
            .unwrap();
        let asked = Arc::new(AtomicU64::new(0));
        let make = |hash: [u8; 32]| -> HashAt {
            let asked = asked.clone();
            Arc::new(move |_| {
                asked.fetch_add(1, Ordering::Relaxed);
                Box::pin(async move { Ok(hash) })
            })
        };
        let path =
            format!("/api?module=account&action=txlist&address={ALICE}&startblock=0&endblock=180");
        let s = Arc::new(
            ModeState::new(history.clone(), 1, Duration::from_secs(300), true)
                .with_deadline(Duration::from_millis(300))
                .with_hash_at(make([7; 32])),
        );
        let started = Instant::now();
        let (_, v) = call(router(s), "GET", &path, None).await;
        assert_eq!(v["status"], "1", "{v}");
        assert_eq!(v["result"][0]["blockNumber"], "150");
        assert!(
            started.elapsed() < Duration::from_millis(250),
            "{:?}",
            started.elapsed()
        );
        assert_eq!(asked.load(Ordering::Relaxed), 1, "one hash check");
        let s = Arc::new(
            ModeState::new(history.clone(), 1, Duration::from_secs(300), true)
                .with_deadline(Duration::from_millis(300))
                .with_hash_at(make([8; 32])),
        );
        let (_, v) = call(router(s), "GET", &path, None).await;
        assert!(
            v["result"]
                .as_str()
                .unwrap()
                .starts_with("NUTHATCH_INCOMPLETE"),
            "{v}"
        );
        // A write lands while the hash is checked: the answer read before it is read again.
        let writes = Arc::new(AtomicU64::new(0));
        let racing: HashAt = {
            let (h, writes, asked) = (history.clone(), writes.clone(), asked.clone());
            Arc::new(move |_| {
                asked.fetch_add(1, Ordering::Relaxed);
                if writes.fetch_add(1, Ordering::Relaxed) == 0 {
                    let mut record = serde_json::Map::new();
                    record.insert("delivered".into(), json!([]));
                    let mut row = serde_json::Map::new();
                    row.insert("blockNumber".into(), json!("150"));
                    let rows = vec![crate::address_history::Row {
                        block: 150,
                        tx_index: 0,
                        position: 0,
                        record: row,
                    }];
                    let g = h.generation().unwrap();
                    h.apply_reask(150, &record, &[(ALICE.to_lowercase(), rows)], false, 1, g)
                        .unwrap();
                }
                Box::pin(async move { Ok([7; 32]) })
            })
        };
        asked.store(0, Ordering::Relaxed);
        let s = Arc::new(
            ModeState::new(history, 1, Duration::from_secs(300), true)
                .with_deadline(Duration::from_millis(300))
                .with_hash_at(racing),
        );
        let (_, v) = call(router(s), "GET", &path, None).await;
        assert_eq!(v["status"], "1", "{v}");
        assert_eq!(
            asked.load(Ordering::Relaxed),
            2,
            "checked again after the write"
        );
    }

    /// A request whose uncovered gap the cursor cannot cover in time is refused at once with how far
    /// history is covered; a tip-sized gap still goes to the cursor.
    #[tokio::test]
    async fn a_gap_beyond_the_deadline_is_refused_at_once() {
        let dir = tempfile::tempdir().unwrap();
        let store = Store::open(&dir.path().join("t.redb")).unwrap();
        let history = AddressHistory::open(store, 1, &[ALICE.into()]).unwrap();
        history.set_head(200_000).unwrap();
        let s = Arc::new(
            ModeState::new(history, 1, Duration::from_secs(300), true)
                .with_deadline(Duration::from_millis(500)),
        );
        let ask = |from: u64, to: u64| {
            format!(
                "/api?module=account&action=txlist&address={ALICE}&startblock={from}&endblock={to}"
            )
        };
        let started = Instant::now();
        let (_, v) = call(router(s.clone()), "GET", &ask(0, 100_000), None).await;
        assert!(
            v["result"]
                .as_str()
                .unwrap()
                .contains("covered through block"),
            "{v}"
        );
        assert!(
            started.elapsed() < Duration::from_millis(200),
            "{:?}",
            started.elapsed()
        );
        // A tip-sized gap waits for the cursor (none runs here, so it is refused at the deadline).
        let started = Instant::now();
        let (_, v) = call(router(s), "GET", &ask(199_990, 199_999), None).await;
        assert!(
            !v["result"]
                .as_str()
                .unwrap()
                .contains("covered through block"),
            "{v}"
        );
        assert!(started.elapsed() >= Duration::from_millis(475));
    }

    /// A startblock above the chain head is not coverage and not a backfill. The blocks are not
    /// there, and the refusal must not name a block above the head.
    #[tokio::test]
    async fn a_start_above_the_chain_head_is_not_called_backfill() {
        let dir = tempfile::tempdir().unwrap();
        let store = Store::open(&dir.path().join("t.redb")).unwrap();
        let history = AddressHistory::open(store, 1, &[ALICE.into()]).unwrap();
        history.set_head(100).unwrap();
        let s = Arc::new(ModeState::new(history, 1, Duration::from_secs(300), true));
        s.latest.store(100, Ordering::Relaxed);
        s.backfill_rate.store(1, Ordering::Relaxed);
        let path = format!(
            "/api?module=account&action=txlist&address={ALICE}&startblock=150&endblock=400"
        );
        let started = Instant::now();
        let (_, v) = call(router(s), "GET", &path, None).await;
        let msg = v["result"].as_str().unwrap();
        assert!(msg.contains("chain head is block 100"), "{v}");
        assert!(msg.contains("starts at block 150"), "{v}");
        assert!(!msg.contains("covered through block"), "{v}");
        // 200ms of wall clock fails on a full shard. The deadline is 8s; a second still proves
        // this did not wait it out.
        assert!(
            started.elapsed() < Duration::from_secs(1),
            "{:?}",
            started.elapsed()
        );
    }

    /// A measured rate under a block a minute is still measured: it is never read as no rate.
    #[test]
    fn a_slow_measured_rate_is_never_read_as_unmeasured() {
        let dir = tempfile::tempdir().unwrap();
        let store = Store::open(&dir.path().join("t.redb")).unwrap();
        let history = AddressHistory::open(store, 1, &[]).unwrap();
        let s = ModeState::new(history, 1, Duration::from_secs(300), true);
        let rate = AtomicU64::new(per_minute(1, Duration::from_secs(600)));
        assert_eq!(rate.load(Ordering::Relaxed), 1);
        assert_eq!(reach(&s, &rate, COVER_PRODUCED), 0);
        assert_eq!(per_minute(50, Duration::from_secs(30)), 100);
    }

    /// The produced-block pass measures its rate in blocks per minute, and admission reads it: a gap
    /// within reach goes to the cursor, a slow rate refuses it, and owed relay re-asks count as blocks.
    #[tokio::test]
    async fn the_produced_block_rate_is_measured_and_admits_by_it() {
        use crate::address_history::Action;
        let dir = tempfile::tempdir().unwrap();
        let store = Store::open(&dir.path().join("t.redb")).unwrap();
        let history = AddressHistory::open(store, 1, &[ALICE.into()]).unwrap();
        history.set_head(300_000).unwrap();
        let alice = ALICE.to_lowercase();
        for action in crate::address_discovery::DISCOVERED
            .into_iter()
            .chain(crate::address_discovery::BLOCK_SCANNED)
        {
            let g = history.generation().unwrap();
            history
                .record(action, &alice, &[], (0, 300_000), g)
                .unwrap();
        }
        let relays: crate::address_mev::Relays = Arc::new(|_, _| Box::pin(async { Ok(vec![]) }));
        let s = Arc::new(
            ModeState::new(history.clone(), 1, Duration::from_secs(300), true)
                .with_deadline(Duration::from_secs(2))
                .with_relays(relays),
        );
        let make = || Headers {
            by_number: BTreeMap::new(),
            finalized: Value::Null,
        };
        let d = Discoverer::new(Counted::new(make()), Counted::new(make()));
        let started = Instant::now();
        mev_passes(&d, &s, std::slice::from_ref(&alice), 199_999).await;
        let took = started.elapsed().as_millis().max(1) as u64;
        assert_eq!(
            history.coverage(Action::ProducedBlocks, &alice).unwrap(),
            vec![(0, 199_999)]
        );
        // Four whole 50,000-block steps: the last took no longer than all four, so its rate in blocks
        // per minute is at least this.
        let measured = s.produced_rate.load(Ordering::Relaxed);
        assert!(
            measured >= 50_000 * 60_000 / took,
            "{measured} after {took} ms"
        );
        // Admission reads the measured rate: a gap within its reach goes to the cursor.
        let want = |to: u64| (alice.clone(), Need::Produced, 0, to);
        let r = reach(&s, &s.produced_rate, COVER_PRODUCED);
        assert_eq!(
            beyond_reach(&s, &want(199_999 + r.min(100_000))).await,
            None
        );
        if r < 100_000 {
            assert!(beyond_reach(&s, &want(199_999 + r + 1)).await.is_some());
        }
        // 6,000,000 a minute reaches 100,000 blocks in half of 2 s: a 50,000-block gap is admitted.
        s.produced_rate.store(6_000_000, Ordering::Relaxed);
        assert_eq!(beyond_reach(&s, &want(249_999)).await, None);
        // A valid u64 range boundary is refused without overflowing the work estimate.
        assert!(beyond_reach(&s, &want(u64::MAX)).await.is_some());
        // One block a minute is slow, not unmeasured: a 1,000-block gap is refused, where the
        // unmeasured floor would admit it.
        s.produced_rate.store(1, Ordering::Relaxed);
        assert!(beyond_reach(&s, &want(200_999)).await.is_some());
        // It reaches no block at all in time, so not even one is admitted.
        assert!(beyond_reach(&s, &want(200_000)).await.is_some());
        s.produced_rate.store(0, Ordering::Relaxed);
        assert_eq!(beyond_reach(&s, &want(200_999)).await, None);
        // An owed re-ask costs as much as a block: one overdue tips the 1,000-block gap past reach.
        history.mev_absent(5, 0).unwrap();
        assert!(beyond_reach(&s, &want(200_999)).await.is_some());
    }

    /// Admission is by the slowest layer a request needs: fast trace discovery does not admit a
    /// withdrawals request the slow body scan cannot finish in time.
    #[tokio::test]
    async fn each_layer_admits_only_what_it_can_finish() {
        let dir = tempfile::tempdir().unwrap();
        let store = Store::open(&dir.path().join("t.redb")).unwrap();
        let history = AddressHistory::open(store, 1, &[ALICE.into()]).unwrap();
        history.set_head(200_000).unwrap();
        let s = Arc::new(
            ModeState::new(history, 1, Duration::from_secs(300), true)
                .with_deadline(Duration::from_secs(2)),
        );
        // Blocks per minute: with a 2 s deadline a layer reaches its rate / 60 blocks.
        s.backfill_rate.store(6_000_000, Ordering::Relaxed);
        s.body_rate.store(600, Ordering::Relaxed);
        let ask = |action: &str| {
            format!("/api?module=account&action={action}&address={ALICE}&startblock=190000&endblock=190999")
        };
        let started = Instant::now();
        let (_, v) = call(router(s.clone()), "GET", &ask("txsBeaconWithdrawal"), None).await;
        assert!(
            v["result"]
                .as_str()
                .unwrap()
                .contains("covered through block"),
            "{v}"
        );
        // 200ms of wall clock fails on a full shard. The admitted path waits out the 2s
        // deadline, so a second and a half still shows this was refused without that wait.
        assert!(
            started.elapsed() < Duration::from_millis(1_500),
            "{:?}",
            started.elapsed()
        );
        // The same gap for transactions alone is within discovery's reach: it goes to the cursor
        // (none runs here, so it waits out the deadline).
        let started = Instant::now();
        let s2 = s.clone();
        let (_, v) = call(router(s), "GET", &ask("txlist"), None).await;
        assert!(
            !v["result"]
                .as_str()
                .unwrap()
                .contains("covered through block"),
            "{v}"
        );
        assert!(started.elapsed() >= Duration::from_millis(1_900));
        // Each layer alone could cover its 1,000 blocks in 60% of the time, but they run one after
        // the other: together they cannot, and the request is refused at once.
        s2.backfill_rate.store(99_960, Ordering::Relaxed);
        s2.body_rate.store(99_960, Ordering::Relaxed);
        let started = Instant::now();
        let (_, v) = call(router(s2), "GET", &ask("txsBeaconWithdrawal"), None).await;
        assert!(
            v["result"]
                .as_str()
                .unwrap()
                .contains("covered through block"),
            "{v}"
        );
        assert!(
            started.elapsed() < Duration::from_millis(1_500),
            "{:?}",
            started.elapsed()
        );
    }

    /// A timestamp search after one that outran its half of the deadline is refused at once.
    #[tokio::test]
    async fn a_timestamp_search_is_refused_at_once_when_the_last_ran_over() {
        let dir = tempfile::tempdir().unwrap();
        let store = Store::open(&dir.path().join("t.redb")).unwrap();
        let history = AddressHistory::open(store, 1, &[ALICE.into()]).unwrap();
        let forward: crate::address_proxy::Forward = Arc::new(|_m: String, _p: Value| {
            Box::pin(async {
                tokio::time::sleep(Duration::from_secs(30)).await;
                Ok(Value::Null)
            })
        });
        let s = Arc::new(
            ModeState::new(history, 1, Duration::from_secs(300), true)
                .with_deadline(Duration::from_secs(2))
                .with_forward(forward),
        );
        s.search_ms.store(5_000, Ordering::Relaxed);
        let started = Instant::now();
        let (_, v) = call(
            router(s),
            "GET",
            "/api?module=block&action=getblocknobytime&timestamp=1700000000&closest=before",
            None,
        )
        .await;
        assert!(
            v["result"]
                .as_str()
                .unwrap()
                .contains("last timestamp search"),
            "{v}"
        );
        // The busy path would wait the one-second search budget. A full shard spent about half
        // a second on the refusal itself.
        assert!(
            started.elapsed() < Duration::from_millis(900),
            "{:?}",
            started.elapsed()
        );
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

    /// A pass that has the cursor is named on /ready, and a poll that is due runs during it, so the
    /// head does not sit where the pass started.
    #[tokio::test]
    async fn a_pass_is_named_and_a_due_poll_runs_during_it() {
        let dir = tempfile::tempdir().unwrap();
        // Long enough that opening the store and one reconcile do not look like three missed polls,
        // and that the second call is still inside the interval the first one just started.
        let interval = Duration::from_secs(2);
        let s = Arc::new(ModeState::new(
            state(dir.path(), true).history.clone(),
            1,
            interval,
            true,
        ));
        s.history.set_head(4).unwrap();
        let hold = s.begin_pass("validators");
        let (_, r) = call(router(s.clone()), "GET", "/ready", None).await;
        assert_eq!(r["holding"]["pass"], "validators", "{r}");
        assert_eq!(r["stalled"], false, "{r}");
        drop(hold);
        let (_, r) = call(router(s.clone()), "GET", "/ready", None).await;
        assert!(r["holding"].is_null(), "{r}");

        // 1 is "polled as the cursor started". Waiting out the interval is what makes the next one due.
        s.last_poll.store(1, Ordering::Relaxed);
        tokio::time::sleep(interval).await;
        let source = fixed(50, 1);
        resume_head(&s, source.as_ref(), &()).await;
        assert_eq!(
            source.reads.load(Ordering::Relaxed),
            1,
            "one poll, once due"
        );
        assert_eq!(s.history.head().unwrap(), Some(50));
        assert_eq!(s.polls.load(Ordering::Relaxed), 1);
        resume_head(&s, source.as_ref(), &()).await;
        assert_eq!(
            source.reads.load(Ordering::Relaxed),
            1,
            "not again before the interval"
        );
    }

    /// A read that is still in flight does not hold the queue until it finishes.
    #[tokio::test]
    async fn a_slow_read_serves_what_is_waiting() {
        let dir = tempfile::tempdir().unwrap();
        let s = state(dir.path(), true);
        let (tx, rx) = tokio::sync::oneshot::channel();
        s.requests
            .send(Request::Cover {
                address: ALICE.into(),
                need: Need::Discovered,
                from: 1,
                to: 2,
                deadline: Instant::now() + Duration::from_secs(2),
                reply: tx,
            })
            .await
            .unwrap();
        let served = AtomicU64::new(0);
        let out = while_serving(
            async {
                tokio::time::sleep(Duration::from_millis(180)).await;
                7
            },
            &|| {
                served.fetch_add(1, Ordering::Relaxed);
                Box::pin(serve_queued(&s, &()))
            },
        )
        .await;
        assert_eq!(out, 7);
        assert!(
            served.load(Ordering::Relaxed) >= 2,
            "the queue was served while the read was in flight"
        );
        let answer = rx.await.unwrap();
        assert!(answer.is_err(), "{answer:?}");
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

    /// Only an address asked for withdrawals or produced blocks joins the background body scan.
    #[tokio::test]
    async fn a_withdrawals_request_asks_for_bodies_and_a_txlist_does_not() {
        let dir = tempfile::tempdir().unwrap();
        let s = state(dir.path(), true);
        let q = |action: &str| {
            format!("/api?module=account&action={action}&address={ALICE}&startblock=0&endblock=1")
        };
        call(router(s.clone()), "GET", &q("txlist"), None).await;
        assert!(!s.history.bodies_wanted(ALICE).unwrap());
        call(router(s.clone()), "GET", &q("txsBeaconWithdrawal"), None).await;
        assert!(s.history.bodies_wanted(ALICE).unwrap());
    }

    /// A batch is added in one go and answered with counts; one bad address refuses the whole batch
    /// before anything is written (RFC-0064 S1).
    #[tokio::test]
    async fn a_batch_of_addresses_is_watched_all_or_none() {
        let dir = tempfile::tempdir().unwrap();
        let s = state(dir.path(), true);
        let carol = "0x00000000000000000000000000000000000000ca";
        let (st, r) = call(
            router(s.clone()),
            "POST",
            "/api/watch",
            Some(json!({"addresses": [BOB, carol, BOB]})),
        )
        .await;
        assert_eq!(st, StatusCode::OK, "{r}");
        assert_eq!(r, json!({"added": 2, "watched": 3}));
        let (st, r) = call(
            router(s.clone()),
            "POST",
            "/api/watch",
            Some(json!({"addresses": ["0x00000000000000000000000000000000000000dd", "nope"]})),
        )
        .await;
        assert_eq!(st, StatusCode::BAD_REQUEST, "{r}");
        assert_eq!(
            s.history.watched_count().unwrap(),
            3,
            "nothing from the refused batch"
        );
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
            validators: vec![],
            beacon_url: None,
            sweep: None,
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
        // One discovery window per address, as the call count below assumes.
        let state = ModeState::new(history.clone(), 1, Duration::from_secs(300), true)
            .with_range(5, Some(2 * SPAN - 1), 0)
            .with_mirror(Arc::new(source), (0, 2 * SPAN - 1))
            .with_first_window(crate::address_discovery::OUTER_WINDOW);
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
        /// No rows anywhere: discovery finds nothing to hydrate.
        quiet: std::sync::atomic::AtomicBool,
        /// Microseconds a trace_filter call takes per block of its span, as a provider's does.
        trace_us_per_block: AtomicU64,
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
            !self.quiet.load(Ordering::Relaxed) && self.hash(b)[0].is_multiple_of(3)
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
                    let per = c.trace_us_per_block.load(Ordering::Relaxed);
                    std::thread::sleep(Duration::from_micros(per * (e + 1).saturating_sub(s)));
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

    /// A node for the sweep: full blocks, and a `Transfer` to ALICE in every block
    /// [`Chain::row_at`] marks, its transaction hash that block's, so a reorg changes both.
    fn sweep_node(
        chain: &Arc<Chain>,
    ) -> Discoverer<
        Counted<crate::address_partitions::tests::Fake>,
        Counted<crate::address_partitions::tests::Fake>,
    > {
        let c = chain.clone();
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
                    let mut block = c.block_json(b);
                    block["transactions"] = json!([]);
                    block
                };
                c.after_read(b);
                Ok(answer)
            }
            "eth_getLogs" => {
                let f = &p[0];
                let s = crate::address_discovery::hex_u64(&f["fromBlock"])?;
                let e = crate::address_discovery::hex_u64(&f["toBlock"])?.min(c.latest());
                let to = format!(
                    "0x{:0>64}",
                    ALICE.trim_start_matches("0x").to_ascii_lowercase()
                );
                Ok(json!((s..=e)
                    .filter(|b| c.row_at(*b))
                    .map(|b| json!({"address": "0x00000000000000000000000000000000000000c0",
                        "topics": ["0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef",
                                   format!("0x{:0>64}", "3"), to],
                        "data": format!("0x{:0>64}", "5"), "blockNumber": format!("0x{b:x}"),
                        "transactionHash": c.hex(b), "transactionIndex": "0x0", "logIndex": "0x0",
                        "blockHash": c.hex(b), "removed": false}))
                    .collect::<Vec<_>>()))
            }
            other => bail!("unexpected {other}"),
        });
        let trace = crate::address_partitions::tests::Fake::new(|m, _| bail!("unexpected {m}"));
        Discoverer::new(Counted::new(main), Counted::new(trace))
    }

    proptest::proptest! {
        #![proptest_config(proptest::prelude::ProptestConfig::with_cases(64))]
        /// RFC-0064's reorg gate: random reorgs above finality, finality moving, sweeps read at the
        /// tip, some with a reorg landing during the read, and polls, in any order. After the last
        /// poll every swept block holds exactly the canonical chain's transfers, and the run swept
        /// something, so a sweep that records nothing cannot pass.
        #[test]
        fn a_sweep_under_reorgs_converges_to_the_canonical_chain(
            ops in proptest::collection::vec((0u8..5, 1u64..10), 1..40)
        ) {
            use crate::address_history::Action;
            let rt = tokio::runtime::Builder::new_current_thread().enable_all().build().unwrap();
            rt.block_on(async {
                let dir = tempfile::tempdir().unwrap();
                let store = Store::open(&dir.path().join("t.redb")).unwrap();
                // Chain 100: the mock chain is shorter than mainnet's Byzantium guard.
                let history = AddressHistory::open(store, 100, &[ALICE.into()]).unwrap();
                let s = Arc::new(
                    ModeState::new(history, 100, Duration::from_secs(300), true)
                        .with_range(0, None, 20),
                );
                let chain = Chain::new(60, 30);
                let d = sweep_node(&chain);
                let set: std::collections::HashSet<String> =
                    [ALICE.to_ascii_lowercase()].into();
                d.reconcile(&s, chain.latest()).await.unwrap();
                let tail = |chain: &Chain| chain.latest() - chain.finalized.load(Ordering::Relaxed);
                let mut swept_any = false;
                for (op, n) in ops.into_iter().chain([(2, 1)]) {
                    let next = s.history.swept().unwrap().map_or(0, |(_, t)| t + 1);
                    match op {
                        0 => chain.reorg(n.min(tail(&chain))),
                        1 => {
                            chain.grow(n);
                            chain.finalized.store(chain.latest() - 20, Ordering::Relaxed);
                        }
                        2 | 3 if next <= chain.latest() => {
                            if op == 3 {
                                *chain.reorg_on_read.lock().unwrap() =
                                    Some((chain.latest(), 1, n.min(tail(&chain))));
                            }
                            let latest = chain.latest();
                            swept_any |= sweep_window(&d, &s, &set, (next, latest)).await.is_ok();
                            *chain.reorg_on_read.lock().unwrap() = None;
                        }
                        2 | 3 => {}
                        _ => {
                            d.reconcile(&s, chain.latest()).await.unwrap();
                        }
                    }
                }
                d.reconcile(&s, chain.latest()).await.unwrap();
                let (held, wanted) = held_and_wanted_of(&chain, &s.history, Action::TokenTx);
                proptest::prop_assert_eq!(held, wanted);
                proptest::prop_assert!(swept_any, "nothing was ever swept");
                proptest::prop_assert!(s.cursor_error.lock().unwrap().is_none());
                Ok(())
            })?;
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

    /// A 5.1 store (a committed head, no finalized hashes) is kept aside intact and a fresh one put
    /// in its place, the accounts it watched kept; a current store, a store with no head, and no
    /// store, are left where they are, and the probe leaves nothing behind.
    #[test]
    fn a_pre_finality_store_is_moved_aside_and_rebuilt() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join(crate::config::DB_FILE);
        let late = "0x00000000000000000000000000000000000000cc";
        let files = || {
            let mut f: Vec<String> = std::fs::read_dir(dir.path())
                .unwrap()
                .map(|e| e.unwrap().file_name().to_string_lossy().into_owned())
                .collect();
            f.sort();
            f
        };
        assert_eq!(
            move_aside_pre_finality(&path, 1, &[ALICE.into()]).unwrap(),
            [ALICE]
        );
        drop(AddressHistory::open(Store::open(&path).unwrap(), 1, &[ALICE.into()]).unwrap());
        assert_eq!(
            move_aside_pre_finality(&path, 1, &[ALICE.into()]).unwrap(),
            [ALICE]
        );
        assert_eq!(
            files(),
            [crate::config::DB_FILE],
            "no head: left alone, no probe left"
        );
        {
            let h = AddressHistory::open(Store::open(&path).unwrap(), 1, &[ALICE.into()]).unwrap();
            h.watch(late).unwrap();
            h.finalize(70).unwrap();
        }
        let before = std::fs::read(&path).unwrap();
        // Copies kept in the seconds around this one are never replaced.
        let now = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_secs();
        for t in now - 1..now + 3 {
            std::fs::write(with_suffix(&path, &format!(".pre-5.2-{t}")), b"earlier").unwrap();
        }
        let addresses = move_aside_pre_finality(&path, 1, &[ALICE.into()]).unwrap();
        assert_eq!(addresses, [ALICE]);
        let kept: Vec<String> = files()
            .into_iter()
            .filter(|f| f.contains(".pre-5.2-"))
            .collect();
        assert_eq!(kept.len(), 5, "{kept:?}");
        let moved: Vec<&String> = kept
            .iter()
            .filter(|f| std::fs::read(dir.path().join(f)).unwrap() != b"earlier")
            .collect();
        assert_eq!(moved.len(), 1, "{kept:?}");
        assert_eq!(
            std::fs::read(dir.path().join(moved[0])).unwrap(),
            before,
            "kept intact"
        );
        let fresh = AddressHistory::open(Store::open(&path).unwrap(), 1, &[]).unwrap();
        assert_eq!(fresh.head().unwrap(), None, "history is read again");
        assert_eq!(
            fresh.watched().unwrap().len(),
            2,
            "the accounts came across"
        );
        fresh.finalize_at(80, [1; 32]).unwrap();
        drop(fresh);
        assert_eq!(
            move_aside_pre_finality(&path, 1, &[ALICE.into()]).unwrap(),
            [ALICE]
        );
        assert!(!files()
            .iter()
            .any(|f| f.ends_with(".new") || f.ends_with(".probe")));
    }

    /// An account removed through the API stays removed across the move, though configuration still
    /// names it; and a store another process holds open is refused, not moved.
    #[test]
    fn the_move_keeps_removals_and_refuses_a_store_in_use() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join(crate::config::DB_FILE);
        let gone = "0x00000000000000000000000000000000000000dd";
        let seed = [ALICE.to_string(), gone.to_string()];
        {
            let h = AddressHistory::open(Store::open(&path).unwrap(), 1, &seed).unwrap();
            h.unwatch(gone).unwrap();
            h.finalize(70).unwrap();
            let err = move_aside_pre_finality(&path, 1, &seed).unwrap_err();
            assert!(err.to_string().contains("another process"), "{err:#}");
            assert!(path.exists());
        }
        let addresses = move_aside_pre_finality(&path, 1, &seed).unwrap();
        for _ in 0..2 {
            let h = AddressHistory::open(Store::open(&path).unwrap(), 1, &addresses).unwrap();
            assert_eq!(h.watched().unwrap(), [ALICE.to_ascii_lowercase()]);
        }
    }

    /// A start interrupted after the old store was linked aside, with the replacement half made,
    /// finishes the move on the next start: both names keep the old store, and the accounts come
    /// across.
    #[test]
    fn an_interrupted_move_finishes_on_the_next_start() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join(crate::config::DB_FILE);
        let late = "0x00000000000000000000000000000000000000cc";
        {
            let h = AddressHistory::open(Store::open(&path).unwrap(), 1, &[ALICE.into()]).unwrap();
            h.watch(late).unwrap();
            h.finalize(70).unwrap();
        }
        let before = std::fs::read(&path).unwrap();
        std::fs::hard_link(&path, with_suffix(&path, ".pre-5.2-1")).unwrap();
        std::fs::write(with_suffix(&path, ".new"), b"half a store").unwrap();
        move_aside_pre_finality(&path, 1, &[]).unwrap();
        assert_eq!(
            std::fs::read(with_suffix(&path, ".pre-5.2-1")).unwrap(),
            before
        );
        let fresh = AddressHistory::open(Store::open(&path).unwrap(), 1, &[]).unwrap();
        assert_eq!(fresh.head().unwrap(), None);
        assert!(fresh.watched().unwrap().iter().any(|a| a == late));
    }

    /// A head that reads but a backfill that cannot, a trace endpoint down, is reported stalled until
    /// a window is read again.
    #[tokio::test]
    async fn a_failing_backfill_is_stalled_though_the_head_reads() {
        let dir = tempfile::tempdir().unwrap();
        let s = reorg_state(dir.path());
        let chain = Chain::new(100, 70);
        let broken = chain.discoverer();
        broken
            .catch_up(&s, broken.reconcile(&s, chain.latest()).await.unwrap())
            .await;
        assert!(s.stalled(), "the trace endpoint answers nothing");
        let (_, r) = call(router(s.clone()), "GET", "/ready", None).await;
        assert_eq!(r["stalled"], true, "{r}");
        s.window_done();
        assert!(!s.stalled(), "a window read since clears it");
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

    #[test]
    fn a_window_shrinks_when_slow_and_grows_back_when_quick() {
        let mut p = Pace::new(1_000, 64, 50_000, Duration::from_secs(20));
        p.took(Duration::from_secs(6));
        assert_eq!(p.span(), 500, "over a quarter of the deadline halves it");
        p.took(Duration::from_secs(3));
        assert_eq!(p.span(), 500, "between half and the whole target it holds");
        // A window that took 2 s, as the fixed checks alone can on a slow RPC, still grows it.
        p.took(Duration::from_millis(2_000));
        assert_eq!(p.span(), 1_000);
        p.took(Duration::from_millis(100));
        assert_eq!(p.span(), 2_000, "a quick window doubles it");
        for _ in 0..20 {
            p.took(Duration::from_secs(60));
        }
        assert_eq!(p.span(), 64, "never under the floor");
    }

    /// Measured 2026-10-14 over Alchemy: 2.3 s a window whatever its size, plus 0.32 ms a block.
    /// On the default deadline that pinned the backfill at 64 blocks a window.
    #[test]
    fn a_window_whose_cost_is_fixed_grows_on_the_default_deadline() {
        let deadline = DEFAULT_DEADLINE;
        let mut p = Pace::new(128, 64, 50_000, deadline);
        let cost = |span: u64| Duration::from_millis(2_300) + Duration::from_micros(320 * span);
        for _ in 0..20 {
            let span = p.span();
            p.took(cost(span));
        }
        assert!(p.span() >= 4_096, "{}", p.span());
        assert!(cost(p.span()) <= deadline, "{}", p.span());
    }

    /// Measured 2026-10-14 over GraphOps: three getLogs at about 1.5 s each put 4.5 s on every
    /// window, more than half the deadline, and the window stayed at its floor.
    #[test]
    fn a_fixed_cost_over_half_the_deadline_still_lets_a_window_grow() {
        let deadline = DEFAULT_DEADLINE;
        let mut p = Pace::new(128, 64, 50_000, deadline);
        let cost = |span: u64| Duration::from_millis(4_500) + Duration::from_micros(500 * span);
        for _ in 0..20 {
            let span = p.span();
            p.took(cost(span));
        }
        assert!(p.span() >= 2_048, "{}", p.span());
        assert!(cost(p.span()) <= deadline, "{}", p.span());
    }

    /// Accounts take turns: a second account gets coverage while the first still has a backlog.
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn accounts_take_turns_in_the_backfill() {
        let dir = tempfile::tempdir().unwrap();
        let store = Store::open(&dir.path().join("t.redb")).unwrap();
        let history = AddressHistory::open(store, 1, &[ALICE.into(), BOB.into()]).unwrap();
        let s = Arc::new(
            ModeState::new(history, 1, Duration::from_secs(300), true).with_range(0, None, 20),
        );
        let chain = Chain::new(30_000, 29_000);
        chain.quiet.store(true, Ordering::Relaxed);
        chain.trace_us_per_block.store(100, Ordering::Relaxed);
        let d = Arc::new(chain.node());
        d.reconcile(&s, chain.latest()).await.unwrap();
        let backfill = tokio::spawn({
            let (d, s) = (d.clone(), s.clone());
            async move { d.catch_up(&s, 29_000).await }
        });
        let covered = |a: &str| {
            s.history
                .coverage(crate::address_history::Action::TxList, a)
                .unwrap()
                .first()
                .map(|(_, t)| *t)
        };
        // BOB sorts first. When ALICE gets her first window, BOB must still have a backlog: one
        // account's whole backlog first would have taken BOB to 29,000.
        let started = Instant::now();
        while covered(ALICE).is_none() && started.elapsed() < Duration::from_secs(20) {
            tokio::time::sleep(Duration::from_millis(2)).await;
        }
        let (a, b) = (covered(ALICE), covered(BOB));
        assert!(
            a.is_some() && b.is_some_and(|b| b < 29_000),
            "ALICE started while BOB still had a backlog: {a:?} {b:?}"
        );
        backfill.abort();
    }

    /// A request for history above what is covered is served inside a long backfill: the cursor's
    /// windows are paced to stay well under the deadline, so the request waits for one short window,
    /// not for a window spanning the whole backlog.
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn a_tip_request_is_served_inside_a_long_backfill() {
        let dir = tempfile::tempdir().unwrap();
        let store = Store::open(&dir.path().join("t.redb")).unwrap();
        let history = AddressHistory::open(store, 1, &[ALICE.into()]).unwrap();
        let s = Arc::new(
            ModeState::new(history, 1, Duration::from_secs(300), true)
                .with_range(0, None, 20)
                .with_deadline(Duration::from_secs(2)),
        );
        let chain = Chain::new(30_000, 29_000);
        chain.quiet.store(true, Ordering::Relaxed);
        // A 29,000-block window would take about 6 s a side: three times the deadline.
        chain.trace_us_per_block.store(200, Ordering::Relaxed);
        let d = Arc::new(chain.node());
        d.reconcile(&s, chain.latest()).await.unwrap();
        let backfill = tokio::spawn({
            let (d, s) = (d.clone(), s.clone());
            async move { d.catch_up(&s, 29_000).await }
        });
        tokio::time::sleep(Duration::from_millis(300)).await;
        let started = Instant::now();
        let (_, v) = call(
            router(s.clone()),
            "GET",
            &format!(
                "/api?module=account&action=txlist&address={ALICE}&startblock=29990&endblock=29999"
            ),
            None,
        )
        .await;
        let waited = started.elapsed();
        assert!(
            !v["result"]
                .as_str()
                .is_some_and(|r| r.starts_with("NUTHATCH_INCOMPLETE")),
            "{v}"
        );
        assert!(waited < Duration::from_secs(2), "{waited:?}");
        assert!(!backfill.is_finished(), "the backfill was still running");
        // The pace the backfill settled on is kept for the next poll.
        assert!(s.backfill_span.load(Ordering::Relaxed) > s.first_window);
        backfill.abort();
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
        let err = body_window(&d, &s, vec![ALICE.to_string()], (101, 120), deadline, false)
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
        // Fifty milliseconds loses to the blocking read on a full shard, and the outer
        // timeout then says "0s". Two seconds lets the cursor's own refusal answer.
        let s2 = Arc::new(
            ModeState::new(s.history.clone(), 1, Duration::from_secs(300), true)
                .with_deadline(Duration::from_secs(2)),
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
            took >= Duration::from_millis(190) && took < Duration::from_secs(2),
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

    /// A recent timestamp is found within the four-second search budget on a mainnet-sized chain,
    /// without assuming its block interval. Both boundaries remain exact and a broken link refuses.
    #[tokio::test]
    async fn a_recent_timestamp_is_searched_near_the_head() {
        let head = 26_000_000;
        for (timestamp, after, expected) in [
            (1000 + 12 * (head - 2) + 5, false, head - 2),
            (1000 + 12 * (head - 2) + 5, true, head - 1),
            (1000 + 12 * (head - 2), false, head - 2),
            (1000 + 12 * (head - 2), true, head - 2),
        ] {
            let reads = Arc::new(AtomicU64::new(0));
            let counted = reads.clone();
            let chain = timed(head, None);
            let forward: crate::address_proxy::Forward = Arc::new(move |m, p| {
                let (chain, counted) = (chain.clone(), counted.clone());
                Box::pin(async move {
                    counted.fetch_add(1, Ordering::Relaxed);
                    tokio::time::sleep(Duration::from_millis(150)).await;
                    chain(m, p).await
                })
            });
            let result = tokio::time::timeout(Duration::from_secs(4), async {
                let (block, latest, proof) = search_by_time(&forward, timestamp, after)
                    .await?
                    .map_err(anyhow::Error::msg)?;
                assert_eq!(block, expected);
                assert!(descends(&forward, block, latest, &proof).await?);
                Ok::<_, anyhow::Error>(())
            })
            .await;
            assert!(matches!(result, Ok(Ok(()))), "{result:?}");
            assert!(reads.load(Ordering::Relaxed) <= 10);
        }
        let broken = timed(head, Some(head - 1));
        assert!(search_by_time(&broken, 1000 + 12 * (head - 2) + 5, false)
            .await
            .is_err());
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
