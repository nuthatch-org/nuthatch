//! RFC-0076 FR18: the buffered cursor.
//!
//! Still one cursor per chain: one fetch position, one finality view, one reorg authority, one
//! failure boundary for extraction. Extraction fetches each range once into a bounded window buffer,
//! and each nest is a task of its own, the single writer of its dataset, reading the buffer from its
//! own watermark through the unchanged [`NestIngest::process_window`]. A nest behind the buffer's
//! oldest window fetches for itself until it reaches it, so neither a slow nest nor a re-admitted one
//! moves extraction back.

use super::*;
use std::collections::{HashSet, VecDeque};
use std::sync::atomic::{AtomicBool, AtomicU64, AtomicU8, Ordering};
use std::sync::Mutex;

/// `NUTHATCH_CURSOR_BUFFER`: unset, the buffered cursor with [`DEFAULT_BUFFER_WINDOWS`]; a number,
/// with that many windows at most; `0` or `off`, the shared cursor, kept for 5.14 only.
pub(crate) const BUFFER_ENV: &str = "NUTHATCH_CURSOR_BUFFER";
/// The window count when the buffered cursor runs without a number. The byte bound binds first on
/// any chain whose windows carry logs; this caps a buffer of empty windows.
pub(crate) const DEFAULT_BUFFER_WINDOWS: usize = 64;
/// `window_buffer_mb` at the default 2048 MB ceiling, scaling with it (RFC-0076 §3.2).
pub(crate) const WINDOW_BUFFER_MB_AT_DEFAULT: u64 = 64;
/// How much more than the memory bound may be spilled to the scratch directory.
const SPILL_FACTOR: u64 = 4;
/// Reorg markers kept. A nest that has missed older ones checks its own checkpoints instead.
const MARKERS_KEPT: usize = 64;
/// Window ends extraction remembers for its own reorg detection.
const HEADERS_KEPT: usize = 256;
/// Windows extraction holds at or ahead of its fastest nest: the one it is processing and the next.
const LOOKAHEAD: usize = 2;

/// The window count the buffered cursor runs with, or `None` for the shared cursor.
pub(crate) fn buffer_windows() -> Option<usize> {
    parse_buffer_windows(std::env::var(BUFFER_ENV).ok().as_deref())
}

pub(crate) fn parse_buffer_windows(raw: Option<&str>) -> Option<usize> {
    match raw.map(str::trim) {
        None | Some("") => Some(DEFAULT_BUFFER_WINDOWS),
        Some("0" | "off" | "shared") => None,
        Some(v) => Some(v.parse::<usize>().ok().filter(|&w| w > 0).unwrap_or_else(|| {
            tracing::warn!("{BUFFER_ENV}={v} is not a window count; using {DEFAULT_BUFFER_WINDOWS}");
            DEFAULT_BUFFER_WINDOWS
        })),
    }
}

/// What a buffered cursor's admission is charged for its buffer, in MB, at `ceiling_mb`; 0 when the
/// shared cursor runs.
pub fn window_buffer_mb(ceiling_mb: u64) -> u64 {
    if buffer_windows().is_none() {
        return 0;
    }
    window_buffer_mb_at(ceiling_mb)
}

pub(crate) fn window_buffer_mb_at(ceiling_mb: u64) -> u64 {
    WINDOW_BUFFER_MB_AT_DEFAULT.saturating_mul(ceiling_mb) / crate::runtime::DEFAULT_MAX_RSS_MB
}

/// The per-cursor ceiling buffers are bounded by: one runtime per process, so one value, and
/// `PUT /_admin/runtime` moves it for running cursors too.
static CEILING_MB: AtomicU64 = AtomicU64::new(crate::runtime::DEFAULT_MAX_RSS_MB);

/// Set the per-cursor ceiling every buffered cursor's byte bound follows.
pub fn set_cursor_ceiling_mb(mb: u64) {
    CEILING_MB.store(mb, Ordering::Relaxed);
}

/// `<runtime>/cursors/<chain>/buffer/`: scratch, never data.
pub(crate) fn scratch_dir(runtime: &Path, chain: &str) -> PathBuf {
    let chain: String = chain
        .chars()
        .map(|c| {
            if c.is_ascii_alphanumeric() || c == '-' || c == '_' {
                c
            } else {
                '_'
            }
        })
        .collect();
    runtime.join("cursors").join(chain).join("buffer")
}

#[derive(Clone)]
pub(crate) struct BufferOptions {
    /// Windows extraction may hold ahead of its nests.
    pub(crate) windows: usize,
    /// Bytes of windows held in memory before the buffer spills; `None` follows the live ceiling.
    pub(crate) memory_bytes: Option<u64>,
    /// Where windows past the memory bound go, and how many bytes may go there; `None` is
    /// [`SPILL_FACTOR`] times the memory bound.
    pub(crate) spill: Option<(PathBuf, Option<u64>)>,
    /// Nests committing a window at once.
    pub(crate) ingest_concurrency: usize,
    pub(crate) stats: Arc<BufferStats>,
    pub(crate) chain: String,
}

#[derive(Default)]
pub(crate) struct BufferStats {
    pub(crate) windows_fetched: AtomicU64,
    pub(crate) factory_refetches: AtomicU64,
    pub(crate) own_fetches: AtomicU64,
    pub(crate) evicted: AtomicU64,
    pub(crate) reorg_markers: AtomicU64,
    pub(crate) markers_applied: AtomicU64,
    pub(crate) spilled_windows: AtomicU64,
    pub(crate) peak_memory_bytes: AtomicU64,
    pub(crate) largest_window_bytes: AtomicU64,
}

fn default_ingest_concurrency() -> usize {
    let cores = std::thread::available_parallelism().map_or(1, |n| n.get());
    std::env::var("NUTHATCH_INGEST_CONCURRENCY")
        .ok()
        .and_then(|v| v.parse::<usize>().ok())
        .filter(|&c| c > 0)
        .unwrap_or(cores.min(4))
}

impl BufferOptions {
    /// The options for `chain`'s cursor, when the buffered cursor runs. The scratch directory is
    /// emptied here, at the cursor's start: what it held belonged to a process that has gone.
    pub(crate) fn for_cursor(
        chain: &str,
        max_rss_mb: u64,
        runtime_dir: Option<&Path>,
    ) -> Option<Self> {
        let windows = buffer_windows()?;
        set_cursor_ceiling_mb(max_rss_mb);
        let spill = runtime_dir.and_then(|r| {
            let dir = scratch_dir(r, chain);
            match std::fs::remove_dir_all(&dir) {
                Ok(()) => {}
                Err(e) if e.kind() == std::io::ErrorKind::NotFound => {}
                Err(e) => tracing::warn!("could not empty {}: {e}", dir.display()),
            }
            match std::fs::create_dir_all(&dir) {
                Ok(()) => Some((dir, None)),
                Err(e) => {
                    tracing::warn!(
                        "the {chain} cursor cannot spill its buffer to {}: {e}; it waits once its \
                         memory bound is full",
                        dir.display()
                    );
                    None
                }
            }
        });
        Some(Self {
            windows,
            memory_bytes: None,
            spill,
            ingest_concurrency: default_ingest_concurrency(),
            stats: Arc::default(),
            chain: chain.to_string(),
        })
    }

    /// Bytes of windows the buffer may hold in memory.
    pub(crate) fn memory_bound(&self) -> u64 {
        self.memory_bytes
            .unwrap_or_else(|| window_buffer_mb_at(CEILING_MB.load(Ordering::Relaxed)) << 20)
    }

    fn spill_bound(&self) -> Option<u64> {
        self.spill.as_ref().map(|(_, cap)| {
            cap.unwrap_or_else(|| self.memory_bound().saturating_mul(SPILL_FACTOR))
        })
    }

    #[cfg(test)]
    pub(crate) fn for_test(windows: usize) -> Self {
        Self {
            windows,
            memory_bytes: Some(u64::MAX / 4),
            spill: None,
            ingest_concurrency: 4,
            stats: Arc::default(),
            chain: format!("test-{}", TEST_CHAIN.fetch_add(1, Ordering::Relaxed)),
        }
    }
}

#[cfg(test)]
static TEST_CHAIN: AtomicU64 = AtomicU64::new(0);

enum WindowLogs {
    Memory(Vec<crate::rpc::Log>),
    Spilled(PathBuf),
}

struct BufferedWindow {
    from: u64,
    to: u64,
    tip: u64,
    logs: WindowLogs,
    /// The addresses the fetch asked for, lowercased; `None` when it asked by topic0 alone, which
    /// already covers any child a nest discovers later.
    asked: Option<HashSet<String>>,
    /// Hash and timestamp of `to`, read by extraction after the logs, as a window's checkpoint is.
    header: Option<(String, Option<u64>)>,
    /// One block over the provider's cap: no logs, and each nest asks for it by its own filter.
    over_cap: bool,
    bytes: u64,
}

impl Drop for BufferedWindow {
    fn drop(&mut self) {
        if let WindowLogs::Spilled(path) = &self.logs {
            let _ = std::fs::remove_file(path);
        }
    }
}

/// What a window costs to hold: its logs' strings and their vectors.
fn window_bytes(logs: &[crate::rpc::Log], header: &Option<(String, Option<u64>)>) -> u64 {
    let s = std::mem::size_of::<String>() as u64;
    let per_log = std::mem::size_of::<crate::rpc::Log>() as u64;
    let logs: u64 = logs
        .iter()
        .map(|l| {
            per_log
                + (l.address.len() + l.data.len() + l.block_hash.len() + l.tx_hash.len()) as u64
                + l.topics.iter().map(|t| s + t.len() as u64).sum::<u64>()
        })
        .sum();
    logs + header.as_ref().map_or(0, |(h, _)| s + h.len() as u64)
        + std::mem::size_of::<BufferedWindow>() as u64
}

struct WindowBuffer {
    windows: VecDeque<Arc<BufferedWindow>>,
    /// The first block extraction has not fetched; `None` until a nest has a position.
    next_fetch: Option<u64>,
    tip: u64,
    memory: u64,
    spilled: u64,
    /// Reorg markers: `(id, ancestor)`, ids increasing.
    markers: VecDeque<(u64, u64)>,
    marker_id: u64,
    spill_id: u64,
}

impl WindowBuffer {
    fn release(&mut self, w: &BufferedWindow) {
        match w.logs {
            WindowLogs::Memory(_) => self.memory -= w.bytes,
            WindowLogs::Spilled(_) => self.spilled -= w.bytes,
        }
    }

    fn pop_front(&mut self) -> Option<Arc<BufferedWindow>> {
        let w = self.windows.pop_front()?;
        self.release(&w);
        Some(w)
    }
}

/// What extraction reads of a nest it no longer holds: the nest belongs to its ingest task.
struct NestView {
    addresses: Vec<String>,
    topic0s: Vec<String>,
    start_block: Option<u64>,
    registry: Arc<DecodeRegistry>,
    factory: Option<Arc<FactorySet>>,
    children: ChildRegistry,
    /// Weak, so an unmounted nest's store is released when its task ends, whatever extraction holds.
    store: std::sync::Weak<dyn crate::store::HotStore>,
    metrics: Arc<crate::metrics::NestMetrics>,
    window_ceiling: Option<u64>,
    blocks: bool,
    finality: Finality,
}

impl NestView {
    fn of(n: &NestIngest) -> Self {
        Self {
            addresses: n.addresses.clone(),
            topic0s: n.topic0s.clone(),
            start_block: n.start_block,
            registry: n.registry.clone(),
            factory: n.factory.clone(),
            children: n.children.clone(),
            store: Arc::downgrade(&n.store),
            metrics: n.metrics.clone(),
            window_ceiling: n.window_ceiling,
            blocks: n.registry.blocks(),
            finality: n.finality,
        }
    }
}

const LIVE: u8 = 0;
const QUARANTINED: u8 = 1;
const TERMINAL: u8 = 2;
const RETIRED: u8 = 3;

struct NestSlot {
    name: Mutex<String>,
    view: Mutex<NestView>,
    /// The first block this nest has not committed.
    next: AtomicU64,
    /// Whether `next` is a position at all: a nest whose `prepare` failed has none.
    prepared: AtomicBool,
    status: AtomicU8,
    reason: Mutex<Option<String>>,
    stop: AtomicBool,
}

impl NestSlot {
    fn name(&self) -> String {
        self.name.lock().unwrap().clone()
    }

    fn live(&self) -> bool {
        self.status.load(Ordering::Acquire) == LIVE && self.prepared.load(Ordering::Acquire)
    }
}

struct BufferShared {
    buffer: Mutex<WindowBuffer>,
    /// Every nest on the cursor, retired ones removed.
    slots: Mutex<Vec<Arc<NestSlot>>>,
    changed: tokio::sync::watch::Sender<u64>,
    metrics: Arc<crate::metrics::CursorMetrics>,
    opts: BufferOptions,
    /// Held while extraction reads a nest's store; an unmount is acknowledged under it.
    reading_store: Arc<tokio::sync::Mutex<()>>,
}

enum MarkerCheck {
    None,
    /// Older markers than this nest has seen were dropped: it checks its own checkpoints.
    Missed(u64),
    Apply { id: u64, ancestor: u64 },
}

impl BufferShared {
    fn bump(&self) {
        self.changed.send_modify(|v| *v = v.wrapping_add(1));
    }

    fn slots(&self) -> Vec<Arc<NestSlot>> {
        self.slots.lock().unwrap().clone()
    }

    fn live(&self) -> Vec<Arc<NestSlot>> {
        self.slots().into_iter().filter(|s| s.live()).collect()
    }

    fn publish(&self, b: &WindowBuffer) {
        self.metrics.set_buffer_bytes(b.memory, b.spilled);
        self.opts
            .stats
            .peak_memory_bytes
            .fetch_max(b.memory, Ordering::Relaxed);
    }

    /// Drop every window no live nest still needs.
    fn trim(&self) {
        let floor = self
            .live()
            .iter()
            .map(|s| s.next.load(Ordering::Acquire))
            .min();
        let mut b = self.buffer.lock().unwrap();
        if let Some(floor) = floor {
            while b.windows.front().is_some_and(|w| w.to < floor) {
                b.pop_front();
            }
        }
        self.publish(&b);
    }

    fn latest_marker(&self) -> u64 {
        self.buffer.lock().unwrap().marker_id
    }

    fn markers_since(&self, seen: u64) -> MarkerCheck {
        let b = self.buffer.lock().unwrap();
        if b.marker_id <= seen {
            return MarkerCheck::None;
        }
        if b.markers.front().is_none_or(|(id, _)| *id > seen + 1) {
            return MarkerCheck::Missed(b.marker_id);
        }
        let ancestor = b
            .markers
            .iter()
            .filter(|(id, _)| *id > seen)
            .map(|(_, a)| *a)
            .min()
            .expect("a marker newer than `seen` is present");
        MarkerCheck::Apply {
            id: b.marker_id,
            ancestor,
        }
    }

    /// A reorg to `ancestor`, decided once by extraction: every buffered window above it goes, and a
    /// marker tells each nest to roll back from its own watermark.
    fn reorg(&self, ancestor: u64) {
        let mut b = self.buffer.lock().unwrap();
        let (kept, dropped): (VecDeque<_>, VecDeque<_>) =
            b.windows.drain(..).partition(|w| w.to <= ancestor);
        for w in &dropped {
            b.release(w);
        }
        b.windows = kept;
        b.next_fetch = b.next_fetch.map(|n| n.min(ancestor + 1));
        b.marker_id += 1;
        let id = b.marker_id;
        b.markers.push_back((id, ancestor));
        while b.markers.len() > MARKERS_KEPT {
            b.markers.pop_front();
        }
        self.opts
            .stats
            .reorg_markers
            .fetch_add(1, Ordering::Relaxed);
        self.publish(&b);
        drop(b);
        self.bump();
    }
}

/// What every nest task shares.
struct TaskCtx {
    source: Arc<dyn Source>,
    health: Arc<crate::health::RuntimeHealth>,
    fail_fast: bool,
    backfill: Option<u64>,
    seal_direct: bool,
    concurrency: usize,
    window: u64,
    permits: tokio::sync::Semaphore,
}

/// RFC-0076 §3.2: the cursor as an extraction stage feeding a bounded window buffer, and one ingest
/// task per nest. Lifecycle commands are applied as they arrive; a nest's own task picks them up at
/// its next window boundary.
#[allow(clippy::too_many_arguments)]
pub(super) async fn runtime_index_loop_buffered(
    source: Arc<dyn Source>,
    nests: Vec<NestIngest>,
    backfill: Option<u64>,
    seal_direct: bool,
    concurrency: usize,
    window: u64,
    health: Arc<crate::health::RuntimeHealth>,
    fail_fast: bool,
    mut lifecycle: Option<CursorCommands>,
    opts: BufferOptions,
) -> Result<()> {
    if nests.is_empty() {
        return Ok(());
    }
    // Phase 0, as the shared cursor does it: each nest backfills its own history, in turn.
    let mut prepared = Vec::new();
    for mut nest in nests {
        let mut sup = Supervisor::new(vec![nest.name.clone()], health.clone(), fail_fast);
        let next = match contained(nest.prepare(
            source.as_ref(),
            backfill,
            seal_direct,
            concurrency,
            window,
        ))
        .await
        {
            Ok(next) => {
                sup.prepared[0] = true;
                Some(next)
            }
            Err(e) => {
                sup.quarantine(0, &e)?;
                None
            }
        };
        prepared.push((nest, sup, next));
    }
    let freshness = cursor_freshness(prepared.iter().map(|(n, _, _)| n));
    let documents = prepared.iter().any(|(n, _, _)| n.ipfs_gate.is_some());
    let start = prepared.iter().filter_map(|(_, _, n)| *n).min();
    let (changed, _) = tokio::sync::watch::channel(0u64);
    let shared = Arc::new(BufferShared {
        buffer: Mutex::new(WindowBuffer {
            windows: VecDeque::new(),
            next_fetch: start,
            tip: 0,
            memory: 0,
            spilled: 0,
            markers: VecDeque::new(),
            marker_id: 0,
            spill_id: 0,
        }),
        slots: Mutex::new(Vec::new()),
        changed,
        metrics: METRICS.cursor(&opts.chain),
        opts: opts.clone(),
        reading_store: Arc::default(),
    });
    let ctx = Arc::new(TaskCtx {
        source: source.clone(),
        health: health.clone(),
        fail_fast,
        backfill,
        seal_direct,
        concurrency,
        window,
        permits: tokio::sync::Semaphore::new(opts.ingest_concurrency),
    });
    let mut tasks = tokio::task::JoinSet::new();
    let mut task_slots: std::collections::HashMap<tokio::task::Id, Arc<NestSlot>> =
        Default::default();
    for (nest, sup, next) in prepared {
        let (id, slot) = spawn_nest(&shared, &ctx, &mut tasks, nest, sup, next, 0);
        task_slots.insert(id, slot);
    }
    let mut unmounting: std::collections::HashMap<
        tokio::task::Id,
        Option<tokio::sync::oneshot::Sender<()>>,
    > = Default::default();
    let mut ext = Extraction {
        source: source.as_ref(),
        shared: &shared,
        window,
        documents,
        freshness,
        headers: VecDeque::new(),
        topic0_refused: false,
    };
    let extraction = ext.run();
    tokio::pin!(extraction);
    loop {
        tokio::select! {
            r = &mut extraction => return r,
            cmd = async {
                match lifecycle.as_mut() {
                    Some(rx) => rx.recv().await,
                    None => std::future::pending().await,
                }
            } => match cmd {
                Some(cmd) => apply_command(
                    cmd,
                    &shared,
                    &ctx,
                    &mut tasks,
                    &mut task_slots,
                    &mut unmounting,
                ),
                None => lifecycle = None,
            },
            Some(joined) = tasks.join_next_with_id() => {
                let (id, result) = match joined {
                    Ok((id, r)) => (id, r),
                    Err(e) => {
                        let id = e.id();
                        if let Some(slot) = task_slots.get(&id) {
                            let reason = if e.is_panic() {
                                format!("{:#}", panicked(e.into_panic()))
                            } else {
                                "this nest's ingest task was cancelled".to_string()
                            };
                            if fail_fast {
                                anyhow::bail!("--fail-fast: nest '{}' faulted: {reason}", slot.name());
                            }
                            health.quarantine_nest(&slot.name(), reason.clone(), 0, None);
                            *slot.reason.lock().unwrap() = Some(reason);
                            slot.status.store(TERMINAL, Ordering::Release);
                            shared.bump();
                        }
                        (id, Ok(()))
                    }
                };
                task_slots.remove(&id);
                if let Some(ack) = unmounting.remove(&id).flatten() {
                    // The task has dropped the store; extraction may not be reading it either.
                    let reading = shared.reading_store.clone();
                    tokio::spawn(async move {
                        let _held = reading.lock().await;
                        let _ = ack.send(());
                    });
                }
                result?;
            }
        }
    }
}

#[allow(clippy::too_many_arguments)]
fn spawn_nest(
    shared: &Arc<BufferShared>,
    ctx: &Arc<TaskCtx>,
    tasks: &mut tokio::task::JoinSet<Result<()>>,
    nest: NestIngest,
    sup: Supervisor,
    next: Option<u64>,
    seen_marker: u64,
) -> (tokio::task::Id, Arc<NestSlot>) {
    let status = match &sup.states[0] {
        NestState::Live => LIVE,
        NestState::Quarantined { retry_at: None, .. } => TERMINAL,
        NestState::Quarantined { .. } => QUARANTINED,
        NestState::Retired => RETIRED,
    };
    let slot = Arc::new(NestSlot {
        name: Mutex::new(nest.name.clone()),
        view: Mutex::new(NestView::of(&nest)),
        next: next.unwrap_or(0).into(),
        prepared: next.is_some().into(),
        status: status.into(),
        reason: Mutex::new(None),
        stop: false.into(),
    });
    shared.slots.lock().unwrap().push(slot.clone());
    let handle = tasks.spawn(nest_task(
        shared.clone(),
        ctx.clone(),
        slot.clone(),
        nest,
        sup,
        seen_marker,
    ));
    shared.bump();
    (handle.id(), slot)
}

fn find_slot(shared: &BufferShared, name: &str) -> Option<Arc<NestSlot>> {
    shared
        .slots
        .lock()
        .unwrap()
        .iter()
        .find(|s| *s.name.lock().unwrap() == name)
        .cloned()
}

/// A lifecycle command, as [`drain_lifecycle`] applies it to the shared cursor. An unmount is
/// acknowledged once the nest's task has ended and dropped its store.
fn apply_command(
    cmd: CursorCommand,
    shared: &Arc<BufferShared>,
    ctx: &Arc<TaskCtx>,
    tasks: &mut tokio::task::JoinSet<Result<()>>,
    task_slots: &mut std::collections::HashMap<tokio::task::Id, Arc<NestSlot>>,
    unmounting: &mut std::collections::HashMap<
        tokio::task::Id,
        Option<tokio::sync::oneshot::Sender<()>>,
    >,
) {
    let retire = |slot: &Arc<NestSlot>| {
        slot.status.store(RETIRED, Ordering::Release);
        slot.stop.store(true, Ordering::Release);
        shared
            .slots
            .lock()
            .unwrap()
            .retain(|s| !Arc::ptr_eq(s, slot));
        let name = slot.name();
        ctx.health.retire_nest(&name);
        tracing::info!("nest '{name}' retired from this cursor at the operator's request");
        shared.bump();
        shared.trim();
    };
    match cmd {
        CursorCommand::Unmount { name, ack } => {
            if ack.as_ref().is_some_and(|a| a.is_closed()) {
                tracing::warn!("unmount of '{name}' was abandoned; leaving it on the cursor");
                return;
            }
            let Some(slot) = find_slot(shared, &name) else {
                tracing::debug!("unmount for '{name}' is not for this cursor; ignoring");
                if let Some(ack) = ack {
                    let _ = ack.send(());
                }
                return;
            };
            retire(&slot);
            match task_slots
                .iter()
                .find(|(_, s)| Arc::ptr_eq(s, &slot))
                .map(|(id, _)| *id)
            {
                Some(id) => {
                    unmounting.insert(id, ack);
                }
                None => {
                    if let Some(ack) = ack {
                        let _ = ack.send(());
                    }
                }
            }
        }
        CursorCommand::Mount { nest, next, ack } => {
            let name = nest.name.clone();
            if ack.as_ref().is_some_and(|a| a.is_closed()) {
                tracing::warn!(
                    "mount of '{name}' was abandoned before the cursor reached it; not admitting"
                );
                return;
            }
            if find_slot(shared, &name).is_some() {
                tracing::warn!("nest '{name}' is already on this cursor; ignoring the mount");
                if let Some(ack) = ack {
                    let _ = ack.send(());
                }
                return;
            }
            let mut sup = Supervisor::new(vec![name.clone()], ctx.health.clone(), ctx.fail_fast);
            sup.prepared[0] = true;
            ctx.health.mark_indexing(&name);
            let (id, slot) = spawn_nest(
                shared,
                ctx,
                tasks,
                *nest,
                sup,
                Some(next),
                shared.latest_marker(),
            );
            task_slots.insert(id, slot.clone());
            tracing::info!("nest '{name}' mounted onto this cursor at block {next}");
            if let Some(ack) = ack {
                if ack.send(()).is_err() {
                    retire(&slot);
                }
            }
        }
        CursorCommand::Rename { from, to, ack } => {
            let applied = match (find_slot(shared, &from), find_slot(shared, &to)) {
                (Some(slot), None) => {
                    *slot.name.lock().unwrap() = to.clone();
                    crate::metrics::METRICS.rename_nest(&from, &to);
                    tracing::info!("nest '{from}' is now '{to}' on this cursor");
                    shared.bump();
                    true
                }
                (_, Some(_)) => {
                    tracing::warn!("cannot rename '{from}' to '{to}': '{to}' is on this cursor");
                    false
                }
                (None, _) => false,
            };
            if let Some(ack) = ack {
                let _ = ack.send(applied);
            }
        }
    }
}

/// A factory nest's filter inputs, cloned out of its view so no lock is held across a fetch.
struct ExtractInputs {
    addresses: Vec<String>,
    topic0s: Vec<String>,
    start_block: Option<u64>,
    factory: Option<(Arc<DecodeRegistry>, Arc<FactorySet>, ChildRegistry)>,
}

impl ExtractInputs {
    fn narrowed(&self) -> Vec<String> {
        match &self.factory {
            Some((reg, _, children)) => reg
                .addresses()
                .iter()
                .map(|a| format!("0x{}", hex::encode(a)))
                .chain(children.addresses().iter().map(|c| c.to_string()))
                .collect(),
            None => self.addresses.clone(),
        }
    }
}

struct Extraction<'a> {
    source: &'a dyn Source,
    shared: &'a BufferShared,
    window: u64,
    documents: bool,
    freshness: crate::freshness::Freshness,
    /// The ends of the windows extraction fetched, with the hash it read for each, ascending.
    headers: VecDeque<(u64, String)>,
    topic0_refused: bool,
}

impl Extraction<'_> {
    async fn run(&mut self) -> Result<()> {
        let mut chunker = {
            let slots = self.shared.slots();
            let views: Vec<_> = slots.iter().map(|s| s.view.lock().unwrap()).collect();
            tip_window(
                self.documents,
                views.iter().any(|v| v.blocks),
                self.window,
                views.iter().filter_map(|v| v.window_ceiling).min(),
            )
        };
        let mut poll_failures = 0u32;
        let mut heartbeat = crate::progress::TipHeartbeat::new();
        let mut reorg_gate = ReorgGate::default();
        let mut rx = self.shared.changed.subscribe();
        loop {
            let slots = self.shared.slots();
            if slots.is_empty() {
                // Every nest was unmounted: idle rather than return, as the shared cursor does (#1545).
                sleep_secs(1).await;
                continue;
            }
            if slots
                .iter()
                .all(|s| s.status.load(Ordering::Acquire) == TERMINAL)
            {
                let reasons: Vec<String> = slots
                    .iter()
                    .map(|s| {
                        format!(
                            "{}: {}",
                            s.name(),
                            s.reason.lock().unwrap().clone().unwrap_or_default()
                        )
                    })
                    .collect();
                anyhow::bail!(TerminalFault(format!(
                    "every nest on this cursor is terminally quarantined - {}",
                    reasons.join("; ")
                )));
            }
            let live: Vec<Arc<NestSlot>> = slots.into_iter().filter(|s| s.live()).collect();
            if live.is_empty() {
                wait_changed(&mut rx, std::time::Duration::from_secs(1)).await;
                continue;
            }
            let metrics: Vec<_> = live
                .iter()
                .map(|s| s.view.lock().unwrap().metrics.clone())
                .collect();
            let tip = match self.source.tip().await {
                Ok(t) => {
                    poll_failures = 0;
                    metrics.iter().for_each(|m| m.mark_poll_ok());
                    t
                }
                Err(e) => {
                    metrics.iter().for_each(|m| m.mark_poll_failed());
                    poll_failures = escalate_stall(poll_failures, &e);
                    sleep_secs(3).await;
                    continue;
                }
            };
            let finality = live[0].view.lock().unwrap().finality;
            let ceiling = cursor_ceiling(self.source, finality, self.freshness, tip).await;
            for m in &metrics {
                m.set_tip(tip);
                m.set_ceiling(ceiling);
            }
            // Never back: a nest behind extraction fetches for itself. Forward past what no live
            // nest needs, which forgets the header chain those windows were checked against.
            let floor = live
                .iter()
                .map(|s| s.next.load(Ordering::Acquire))
                .min()
                .expect("live is not empty");
            let next = {
                let mut b = self.shared.buffer.lock().unwrap();
                match b.next_fetch {
                    Some(n) if n >= floor => {}
                    _ => {
                        b.next_fetch = Some(floor);
                        while b.windows.front().is_some_and(|w| w.to < floor) {
                            b.pop_front();
                        }
                        self.headers.clear();
                        self.shared.publish(&b);
                    }
                }
                b.tip = tip;
                b.next_fetch.expect("set above")
            };
            heartbeat.maybe_log(floor, tip);
            if next > 0
                && reorg_check_due(self.freshness, next, ceiling)
                && reorg_gate.due(tip, next, next > ceiling)
            {
                match self.detect(&live, next - 1).await {
                    Ok(Some(ancestor)) => {
                        tracing::warn!(
                            "mounts reorg to block {ancestor}: every nest rolls back from its own \
                             position"
                        );
                        self.source.forget_cached_above(ancestor);
                        while self.headers.back().is_some_and(|(b, _)| *b > ancestor) {
                            self.headers.pop_back();
                        }
                        self.shared.reorg(ancestor);
                        continue;
                    }
                    Ok(None) => reorg_gate.checked(tip, next),
                    Err(e) => {
                        tracing::warn!(
                            "mounts reorg check could not complete: {e:#}; not fetching until it does"
                        );
                        sleep_secs(2).await;
                        continue;
                    }
                }
            }
            if next > ceiling {
                sleep_for(self.freshness.poll_interval).await;
                continue;
            }
            if !self.room(&mut rx, None).await {
                continue;
            }
            let (inputs, window_cap) = {
                let views: Vec<_> = live.iter().map(|s| s.view.lock().unwrap()).collect();
                let cap = if views.iter().any(|v| v.blocks) {
                    crate::chunker::HEADER_WINDOW_CAP
                } else {
                    crate::chunker::MAX_WINDOW
                };
                let cap = views
                    .iter()
                    .filter_map(|v| v.window_ceiling)
                    .fold(cap, u64::min);
                let inputs: Vec<ExtractInputs> = views
                    .iter()
                    .map(|v| ExtractInputs {
                        addresses: v.addresses.clone(),
                        topic0s: v.topic0s.clone(),
                        start_block: v.start_block,
                        factory: v
                            .factory
                            .clone()
                            .map(|fs| (v.registry.clone(), fs, v.children.clone())),
                    })
                    .collect();
                (inputs, cap)
            };
            chunker.set_max(window_cap);
            let to = (next + chunker.window() - 1).min(ceiling);
            metrics
                .iter()
                .for_each(|m| m.set_fetch_window(chunker.window()));
            match self.fetch(&inputs, next, to).await {
                Ok((logs, asked)) => {
                    chunker.observed(logs.len() as u64);
                    // After the logs, as a window's checkpoint is read, so its hash is of the chain
                    // they came from or later.
                    let header = self.source.block_record(to).await.ok().flatten();
                    // Read after both, the previous window's end settles that this window extends
                    // the chain extraction holds, as a nest's parent check does (#2146). Checked
                    // only before the fetch, a reorg between the two left that chain mixed.
                    if let Some((prev, hash)) = self.headers.back().cloned().filter(|(b, _)| *b + 1 == next)
                    {
                        match self.source.block_hash(prev).await {
                            Ok(Some(now)) if now.eq_ignore_ascii_case(&hash) => {}
                            Ok(Some(_)) => match self.detect(&live, prev).await {
                                Ok(Some(ancestor)) => {
                                    tracing::warn!(
                                        "mounts reorg to block {ancestor}, found extending block \
                                         {prev}: every nest rolls back from its own position"
                                    );
                                    self.source.forget_cached_above(ancestor);
                                    while self.headers.back().is_some_and(|(b, _)| *b > ancestor) {
                                        self.headers.pop_back();
                                    }
                                    self.shared.reorg(ancestor);
                                    continue;
                                }
                                _ => {
                                    sleep_secs(2).await;
                                    continue;
                                }
                            },
                            _ => {
                                tracing::warn!(
                                    "the source has no hash for block {prev}; not buffering \
                                     {next}..={to} until it answers"
                                );
                                sleep_secs(2).await;
                                continue;
                            }
                        }
                    }
                    if let Some((hash, _)) = &header {
                        self.headers.push_back((to, hash.clone()));
                        while self.headers.len() > HEADERS_KEPT {
                            self.headers.pop_front();
                        }
                    }
                    let bytes = window_bytes(&logs, &header);
                    let w = BufferedWindow {
                        from: next,
                        to,
                        tip,
                        logs: WindowLogs::Memory(logs),
                        asked,
                        header,
                        over_cap: false,
                        bytes,
                    };
                    if !self.push(&mut rx, w).await {
                        continue;
                    }
                    // A pushed window counts as a check at its tip: its header was just read there.
                    reorg_gate.checked(tip, to + 1);
                    if to == ceiling {
                        sleep_for(self.freshness.poll_interval).await;
                    }
                }
                Err(e) if narrowing_can_help(&e, next, to) => {
                    if next >= to {
                        // COR-5 for the buffered cursor: each nest asks for the block by its own
                        // filter, and only a nest still over the cap is faulted.
                        let w = BufferedWindow {
                            from: next,
                            to: next,
                            tip,
                            logs: WindowLogs::Memory(Vec::new()),
                            asked: None,
                            header: None,
                            over_cap: true,
                            bytes: window_bytes(&[], &None),
                        };
                        tracing::warn!(
                            "block {next} broke the provider's getLogs cap on the union fetch; each \
                             nest asks for it alone: {e:#}"
                        );
                        self.push(&mut rx, w).await;
                        continue;
                    }
                    chunker.too_large();
                    tracing::debug!("range {next}..={to} refused; shrinking and retrying");
                }
                Err(e) => {
                    tracing::warn!("buffered get_logs {next}..={to} failed: {e:#}; retrying");
                    sleep_secs(3).await;
                }
            }
        }
    }

    /// The union fetch, as the shared cursor makes it, with the addresses it asked for.
    async fn fetch(
        &mut self,
        inputs: &[ExtractInputs],
        next: u64,
        to: u64,
    ) -> Result<(Vec<crate::rpc::Log>, Option<HashSet<String>>)> {
        let (u_addrs, u_topics) = union_filter(
            inputs
                .iter()
                .map(|n| (n.addresses.as_slice(), n.topic0s.as_slice())),
        );
        let fetch_from = overlap_from(
            next,
            inputs
                .iter()
                .map(|n| n.start_block.unwrap_or(0))
                .min()
                .unwrap_or(0),
        );
        let factories = inputs.iter().filter(|n| n.factory.is_some()).count();
        let narrowed = if factories == 0 {
            None
        } else {
            let mut addrs: Vec<String> = Vec::new();
            for a in inputs.iter().flat_map(ExtractInputs::narrowed) {
                if !addr_in(&addrs, &a) {
                    addrs.push(a);
                }
            }
            let wide = !self.topic0_refused
                && (addrs.len() > FACTORY_FLIP_THRESHOLD
                    || inputs
                        .iter()
                        .filter_map(|n| n.factory.as_ref())
                        .any(|(_, f, _)| f.force_topic0()));
            (!addrs.is_empty() && !wide).then_some(addrs)
        };
        let lower = |a: &String| a.to_ascii_lowercase();
        match (&narrowed, LogFilter::new(&u_addrs, &u_topics)) {
            (Some(addrs), _) => {
                let mut asked: HashSet<String> = addrs.iter().map(lower).collect();
                let logs = fetch_discovering(self.source, addrs, &u_topics, fetch_from, to, |logs| {
                    let mut known = Vec::new();
                    for (reg, fs, children) in inputs.iter().filter_map(|n| n.factory.as_ref()) {
                        known.extend(discover_children(reg, fs, children, logs));
                    }
                    asked.extend(known.iter().map(lower));
                    known
                })
                .await?;
                Ok((logs, Some(asked)))
            }
            (None, Some(f)) => match self.source.logs(&f, fetch_from, to).await {
                Ok(logs) => {
                    let asked = (!u_addrs.is_empty()).then(|| u_addrs.iter().map(lower).collect());
                    Ok((logs, asked))
                }
                Err(e) if u_addrs.is_empty() && crate::rpc::filter_refused(&e) => {
                    tracing::warn!(
                        "the endpoint refuses getLogs without an address list ({e:#}); this \
                         cursor asks by factory and child address from now on"
                    );
                    self.topic0_refused = true;
                    Err(e)
                }
                Err(e) => Err(e),
            },
            (None, None) => Ok((Vec::new(), None)),
        }
    }

    /// Whether `last`, the end of the last window extraction fetched, is still canonical, and if
    /// not the deepest block that is. Read from extraction's own window ends; with none to read, or
    /// none of them canonical, from the most caught-up nest's checkpoints, as the shared cursor does.
    async fn detect(&mut self, live: &[Arc<NestSlot>], last: u64) -> Result<Option<u64>> {
        if let Some((block, hash)) = self.headers.back().cloned().filter(|(b, _)| *b == last) {
            let Some(canonical) = self.source.block_hash(block).await? else {
                anyhow::bail!("the source has no hash for block {block}");
            };
            if canonical.eq_ignore_ascii_case(&hash) {
                return Ok(None);
            }
            for (b, h) in self.headers.iter().rev().skip(1) {
                let Some(canon) = self.source.block_hash(*b).await? else {
                    anyhow::bail!(
                        "block {block} no longer matches what extraction read, but the source has \
                         no hash for block {b}; not rolling back until it answers"
                    );
                };
                if canon.eq_ignore_ascii_case(h) {
                    return Ok(Some(*b));
                }
            }
        }
        let reference = live
            .iter()
            .max_by_key(|s| s.next.load(Ordering::Acquire))
            .expect("live is not empty");
        let _reading = self.shared.reading_store.lock().await;
        let (store, at) = (
            reference.view.lock().unwrap().store.upgrade(),
            reference.next.load(Ordering::Acquire),
        );
        let Some(store) = store.filter(|_| at > 0) else {
            return Ok(None);
        };
        let through = at.min(last + 1) - 1;
        let aligned = self.headers.back().is_some_and(|(b, _)| *b == last);
        match detect_reorg(self.source, store.as_ref(), through).await? {
            Some(a) => Ok(Some(a)),
            // Every window end extraction holds is off the chain and that nest's newest checkpoint
            // is not: the fork is above what it committed, and only buffered windows are stale.
            None if aligned => Ok(Some(through)),
            None => Ok(None),
        }
    }

    /// Wait until the buffer has room for another window, or for `w` when given. A full buffer
    /// waits for its slowest nest unless some nest has drained it, in which case the oldest window
    /// goes and whoever still needed it fetches it for itself. `false` when the wait was cut short
    /// by a change the caller should look at first.
    async fn room(
        &self,
        rx: &mut tokio::sync::watch::Receiver<u64>,
        w: Option<&BufferedWindow>,
    ) -> bool {
        let opts = &self.shared.opts;
        loop {
            rx.borrow_and_update();
            {
                let mut b = self.shared.buffer.lock().unwrap();
                let counted = b.windows.len() < opts.windows;
                let fits = match w {
                    // Windows are held for the nests behind the fastest; ahead of it, one is enough
                    // to overlap a fetch with its processing, and more is memory for nobody.
                    None => {
                        let lead = self
                            .shared
                            .live()
                            .iter()
                            .map(|s| s.next.load(Ordering::Acquire))
                            .max()
                            .unwrap_or(0);
                        let ahead = b.windows.iter().filter(|w| w.to >= lead).count();
                        ahead < LOOKAHEAD
                            && (b.memory < opts.memory_bound() || self.spill_room(&b, 1))
                    }
                    Some(w) => {
                        b.windows.is_empty()
                            || b.memory + w.bytes <= opts.memory_bound()
                            || self.spill_room(&b, w.bytes)
                    }
                };
                if counted && fits {
                    return true;
                }
                let next_fetch = b.next_fetch.unwrap_or(0);
                let starved = self
                    .shared
                    .live()
                    .iter()
                    .any(|s| s.next.load(Ordering::Acquire) >= next_fetch);
                if starved && !b.windows.is_empty() {
                    b.pop_front();
                    opts.stats.evicted.fetch_add(1, Ordering::Relaxed);
                    self.shared.publish(&b);
                    drop(b);
                    self.shared.bump();
                    continue;
                }
            }
            if !wait_changed(rx, std::time::Duration::from_secs(1)).await {
                return false;
            }
        }
    }

    fn spill_room(&self, b: &WindowBuffer, bytes: u64) -> bool {
        self.shared
            .opts
            .spill_bound()
            .is_some_and(|cap| b.spilled + bytes <= cap)
    }

    /// Put a fetched window in the buffer: in memory while it fits, spilled past that, and waiting
    /// for room past both. `false` when a reorg or a move of the floor overtook it.
    async fn push(
        &self,
        rx: &mut tokio::sync::watch::Receiver<u64>,
        mut w: BufferedWindow,
    ) -> bool {
        let opts = &self.shared.opts;
        if !self.room(rx, Some(&w)).await {
            return false;
        }
        let spill_to = {
            let mut b = self.shared.buffer.lock().unwrap();
            if b.next_fetch != Some(w.from) {
                return false;
            }
            if b.windows.is_empty() || b.memory + w.bytes <= opts.memory_bound() {
                None
            } else {
                b.spill_id += 1;
                opts.spill
                    .as_ref()
                    .map(|(dir, _)| dir.join(format!("{}-{}-{}.json", w.from, w.to, b.spill_id)))
            }
        };
        if let Some(path) = spill_to {
            let WindowLogs::Memory(logs) = std::mem::replace(&mut w.logs, WindowLogs::Spilled(path.clone()))
            else {
                unreachable!("a fetched window is in memory");
            };
            let written = tokio::task::spawn_blocking(move || -> Result<()> {
                std::fs::write(&path, serde_json::to_vec(&logs)?)?;
                Ok(())
            })
            .await
            .map_err(anyhow::Error::from)
            .and_then(|r| r);
            if let Err(e) = written {
                tracing::warn!("could not spill a window to the cursor's scratch directory: {e:#}");
                return false;
            }
            opts.stats.spilled_windows.fetch_add(1, Ordering::Relaxed);
        }
        {
            let mut b = self.shared.buffer.lock().unwrap();
            if b.next_fetch != Some(w.from) {
                return false;
            }
            match w.logs {
                WindowLogs::Memory(_) => b.memory += w.bytes,
                WindowLogs::Spilled(_) => b.spilled += w.bytes,
            }
            opts.stats
                .largest_window_bytes
                .fetch_max(w.bytes, Ordering::Relaxed);
            b.next_fetch = Some(w.to + 1);
            b.windows.push_back(Arc::new(w));
            self.shared.publish(&b);
        }
        opts.stats.windows_fetched.fetch_add(1, Ordering::Relaxed);
        self.shared.bump();
        true
    }
}

/// Wait for a change on the cursor, or `most`. `false` once the cursor has gone.
async fn wait_changed(
    rx: &mut tokio::sync::watch::Receiver<u64>,
    most: std::time::Duration,
) -> bool {
    tokio::select! {
        r = rx.changed() => r.is_ok(),
        _ = tokio::time::sleep(most) => true,
    }
}

/// A window's logs, read back from the scratch directory when they were spilled.
async fn read_logs(w: &BufferedWindow) -> Result<std::borrow::Cow<'_, [crate::rpc::Log]>> {
    match &w.logs {
        WindowLogs::Memory(logs) => Ok(std::borrow::Cow::Borrowed(logs)),
        WindowLogs::Spilled(path) => {
            let path = path.clone();
            let logs = tokio::task::spawn_blocking(move || -> Result<Vec<crate::rpc::Log>> {
                Ok(serde_json::from_slice(&std::fs::read(&path)?)?)
            })
            .await??;
            Ok(std::borrow::Cow::Owned(logs))
        }
    }
}

/// The logs a nest takes from a buffered window: its own, from its watermark's overlap tail, plus
/// the logs of any child it knows of that extraction did not ask for when it fetched the window.
/// A nest's logs for one window: the buffered window itself when the nest takes every log in it,
/// which a single-nest cursor always does, or the nest's own copy.
enum NestLogs {
    Window(Arc<BufferedWindow>),
    Own(Vec<crate::rpc::Log>),
}

impl std::ops::Deref for NestLogs {
    type Target = [crate::rpc::Log];
    fn deref(&self) -> &[crate::rpc::Log] {
        match self {
            NestLogs::Window(w) => match &w.logs {
                WindowLogs::Memory(logs) => logs,
                WindowLogs::Spilled(_) => unreachable!("only an in-memory window is lent"),
            },
            NestLogs::Own(logs) => logs,
        }
    }
}

async fn window_logs(
    nest: &NestIngest,
    source: &dyn Source,
    w: Arc<BufferedWindow>,
    next: u64,
    stats: &BufferStats,
) -> Result<NestLogs> {
    let from = overlap_from(next, nest.start_block.unwrap_or(0));
    let refetch = match (nest.factory.as_deref(), &w.asked) {
        (Some(_), Some(asked)) => nest
            .children
            .addresses()
            .into_iter()
            .any(|c| !asked.contains(&c.to_ascii_lowercase())),
        _ => false,
    };
    if let WindowLogs::Memory(all) = &w.logs {
        if !refetch && all.iter().all(|l| l.block_number >= from && nest.owns(l)) {
            return Ok(NestLogs::Window(w));
        }
    }
    let mut logs: Vec<crate::rpc::Log> = read_logs(&w)
        .await?
        .iter()
        .filter(|l| l.block_number >= from && nest.owns(l))
        .cloned()
        .collect();
    let (Some(fs), Some(asked)) = (nest.factory.as_deref(), &w.asked) else {
        return Ok(NestLogs::Own(logs));
    };
    let missing: Vec<String> = nest
        .children
        .addresses()
        .into_iter()
        .filter(|c| !asked.contains(&c.to_ascii_lowercase()))
        .map(str::to_string)
        .collect();
    if missing.is_empty() {
        return Ok(NestLogs::Own(logs));
    }
    stats.factory_refetches.fetch_add(1, Ordering::Relaxed);
    let extra = fetch_discovering(source, &missing, &nest.topic0s, from, w.to, |seen| {
        discover_children(&nest.registry, fs, &nest.children, seen)
            .into_iter()
            .filter(|c| !asked.contains(&c.to_ascii_lowercase()))
            .collect()
    })
    .await?;
    logs.extend(extra.into_iter().filter(|l| nest.owns(l)));
    dedup_logs(&mut logs);
    Ok(NestLogs::Own(logs))
}

/// COR-5 for one nest: a block the union fetch could not fit, asked for by this nest's own filter,
/// then for a factory nest by `base ∪ children` with discovery. A nest neither fits is faulted
/// terminally, with what the shared cursor tells its operator.
async fn fetch_over_cap(
    nest: &mut NestIngest,
    source: &dyn Source,
    block: u64,
) -> Result<Vec<crate::rpc::Log>> {
    let cause = match nest.fetch_window(source, block, block).await {
        Ok(logs) => return Ok(logs),
        Err(e) if narrowing_can_help(&e, block, block) => e,
        Err(e) => return Err(e),
    };
    let Some(fs) = nest.factory.clone() else {
        anyhow::bail!(TerminalFault(format!(
            "{}, even asked for this nest's own contracts alone: {cause:#}",
            single_block_over_cap(block)
        )));
    };
    let narrowed: Vec<String> = nest
        .registry
        .addresses()
        .iter()
        .map(|a| format!("0x{}", hex::encode(a)))
        .chain(nest.children.addresses().iter().map(|c| c.to_string()))
        .collect();
    if !narrowed.is_empty() {
        match fetch_discovering(source, &narrowed, &nest.topic0s, block, block, |logs| {
            discover_children(&nest.registry, &fs, &nest.children, logs)
        })
        .await
        {
            Ok(logs) => {
                tracing::warn!(
                    "block {block} broke the provider's getLogs cap; nest '{}' refetched it \
                     address-filtered ({} addresses, {} logs) and carried on",
                    nest.name,
                    narrowed.len(),
                    logs.len()
                );
                return Ok(logs);
            }
            Err(e) if narrowing_can_help(&e, block, block) => {
                tracing::warn!("block {block}: the address-filtered fallback failed as well: {e:#}")
            }
            Err(e) => return Err(e),
        }
    }
    anyhow::bail!(TerminalFault(format!(
        "{}, and an address-filtered refetch of it did not fit either - raise the provider's \
         getLogs cap, or narrow this nest's topic0 set with an `events` allowlist: {cause:#}",
        single_block_over_cap(block)
    )))
}

/// A window's source with `to`'s header answered from what extraction read. Everything else,
/// the hashes that settle a window against the chain included, goes to the source.
struct HeaderedSource<'a> {
    inner: &'a dyn Source,
    to: u64,
    header: &'a (String, Option<u64>),
}

#[async_trait::async_trait]
impl Source for HeaderedSource<'_> {
    async fn tip(&self) -> Result<u64> {
        self.inner.tip().await
    }
    async fn block_hash(&self, number: u64) -> Result<Option<String>> {
        if number == self.to {
            return Ok(Some(self.header.0.clone()));
        }
        self.inner.block_hash(number).await
    }
    async fn block_hashes(
        &self,
        blocks: &[u64],
    ) -> Result<std::collections::HashMap<u64, String>> {
        self.inner.block_hashes(blocks).await
    }
    async fn finalized(&self) -> Result<Option<u64>> {
        self.inner.finalized().await
    }
    async fn block_timestamps(
        &self,
        blocks: &[u64],
    ) -> Result<std::collections::HashMap<u64, u64>> {
        self.inner.block_timestamps(blocks).await
    }
    async fn block_record(&self, number: u64) -> Result<Option<(String, Option<u64>)>> {
        if number == self.to {
            return Ok(Some(self.header.clone()));
        }
        self.inner.block_record(number).await
    }
    async fn block_headers(
        &self,
        blocks: &[u64],
    ) -> Result<std::collections::HashMap<u64, serde_json::Value>> {
        self.inner.block_headers(blocks).await
    }
    async fn block_bodies(
        &self,
        blocks: &[u64],
    ) -> Result<std::collections::HashMap<u64, serde_json::Value>> {
        self.inner.block_bodies(blocks).await
    }
    fn forget_cached_above(&self, block: u64) {
        self.inner.forget_cached_above(block)
    }
    async fn logs(&self, filter: &LogFilter, from: u64, to: u64) -> Result<Vec<crate::rpc::Log>> {
        self.inner.logs(filter, from, to).await
    }
}

/// One nest's ingest: the single writer of its dataset. Its quarantine, backoff and re-admission are
/// its own, and a re-admitted nest resumes from its own position, behind the buffer if need be.
async fn nest_task(
    shared: Arc<BufferShared>,
    ctx: Arc<TaskCtx>,
    slot: Arc<NestSlot>,
    mut nest: NestIngest,
    mut sup: Supervisor,
    mut seen_marker: u64,
) -> Result<()> {
    enum Pick {
        Wait,
        Buffered(Arc<BufferedWindow>),
        Own { to: u64, tip: u64 },
        OverCap { block: u64, tip: u64 },
    }
    let source = ctx.source.as_ref();
    let stats = shared.opts.stats.clone();
    let mut rx = shared.changed.subscribe();
    let mut next = slot.next.load(Ordering::Acquire);
    let mut chunker = tip_window(
        nest.ipfs_gate.is_some(),
        nest.registry.blocks(),
        ctx.window,
        nest.window_ceiling,
    );
    // A refused window is asked for again by this nest alone, as the shared cursor's refetch does.
    let mut refused = false;
    let publish = |slot: &NestSlot, next: u64, status: u8| {
        slot.next.store(next, Ordering::Release);
        slot.status.store(status, Ordering::Release);
    };
    loop {
        rx.borrow_and_update();
        if slot.stop.load(Ordering::Acquire) {
            return Ok(());
        }
        let name = slot.name();
        if name != nest.name {
            nest.name = name.clone();
            sup.names[0] = name;
        }
        match &sup.states[0] {
            NestState::Live => {}
            NestState::Retired => return Ok(()),
            NestState::Quarantined { reason, retry_at } => {
                *slot.reason.lock().unwrap() = Some(reason.clone());
                match retry_at {
                    None => {
                        publish(&slot, next, TERMINAL);
                        shared.bump();
                        shared.trim();
                        wait_changed(&mut rx, std::time::Duration::from_secs(1)).await;
                    }
                    Some(at) => {
                        let at = *at;
                        if slot.status.swap(QUARANTINED, Ordering::AcqRel) != QUARANTINED {
                            shared.bump();
                            shared.trim();
                        }
                        let now = std::time::Instant::now();
                        if now < at {
                            wait_changed(&mut rx, (at - now).min(std::time::Duration::from_secs(1)))
                                .await;
                            continue;
                        }
                        // Back from its own position, never extraction's: behind the buffer it
                        // fetches for itself until it reaches the buffer's oldest window.
                        sup.readmit_due(now);
                        refused = false;
                        publish(&slot, next, LIVE);
                        shared.bump();
                    }
                }
                continue;
            }
        }
        if !sup.prepared[0] {
            match contained(nest.prepare(
                source,
                ctx.backfill,
                ctx.seal_direct,
                ctx.concurrency,
                ctx.window,
            ))
            .await
            {
                Ok(n) => {
                    next = n;
                    sup.prepared[0] = true;
                    sup.mark_progress(0);
                    slot.next.store(next, Ordering::Release);
                    slot.prepared.store(true, Ordering::Release);
                    shared.bump();
                }
                Err(e) => sup.quarantine(0, &e)?,
            }
            continue;
        }
        if !sup.reorg_checked[0] {
            // Its own checkpoints first, as `check_joined_nests` does: a nest out of the working set
            // missed every marker issued meanwhile, and one just mounted has seen none.
            seen_marker = shared.latest_marker();
            let name = nest.name.clone();
            let rolled = contained(async {
                let found = match next {
                    0 => None,
                    next => detect_reorg(source, nest.store.as_ref(), next - 1).await?,
                };
                let Some(ancestor) = found else {
                    return Ok(None);
                };
                tracing::warn!(
                    "nest '{name}' rejoined behind a reorg to block {ancestor}: rolling it back"
                );
                source.forget_cached_above(ancestor);
                nest.rollback_reorg(ancestor).map(Some)
            })
            .await;
            match rolled {
                Ok(resume) => {
                    if let Some(resume) = resume {
                        next = next.min(resume);
                        slot.next.store(next, Ordering::Release);
                    }
                    sup.reorg_checked[0] = true;
                }
                Err(e) => sup.quarantine(0, &e)?,
            }
            continue;
        }
        match shared.markers_since(seen_marker) {
            MarkerCheck::None => {}
            MarkerCheck::Missed(id) => {
                seen_marker = id;
                sup.reorg_checked[0] = false;
                continue;
            }
            MarkerCheck::Apply { id, ancestor } => {
                seen_marker = id;
                refused = false;
                stats.markers_applied.fetch_add(1, Ordering::Relaxed);
                match contained_sync(|| nest.rollback_reorg(ancestor)) {
                    Ok(resume) => {
                        next = next.min(resume);
                        slot.next.store(next, Ordering::Release);
                        slot.view.lock().unwrap().children = nest.children.clone();
                    }
                    Err(e) => sup.quarantine(0, &e)?,
                }
                shared.bump();
                continue;
            }
        }
        let pick = {
            let b = shared.buffer.lock().unwrap();
            let next_fetch = b.next_fetch.unwrap_or(next);
            nest.metrics
                .set_behind_extraction(next_fetch.saturating_sub(next));
            match b.windows.iter().find(|w| w.from <= next && next <= w.to) {
                Some(w) if w.over_cap => Pick::OverCap {
                    block: w.from,
                    tip: w.tip,
                },
                Some(w) if refused => Pick::Own {
                    to: w.to,
                    tip: w.tip,
                },
                Some(w) => Pick::Buffered(w.clone()),
                // Behind every buffered window at or after it: detached, fetching for itself.
                None if next < next_fetch => {
                    let limit = b
                        .windows
                        .iter()
                        .map(|w| w.from)
                        .filter(|&f| f > next)
                        .min()
                        .unwrap_or(next_fetch)
                        - 1;
                    Pick::Own {
                        to: (next + chunker.window() - 1).min(limit),
                        tip: b.tip,
                    }
                }
                None => Pick::Wait,
            }
        };
        let (logs, to, tip, header) = match pick {
            Pick::Wait => {
                if let Err(e) = contained_sync(|| nest.ensure_views_healthy()) {
                    sup.quarantine(0, &e)?;
                    continue;
                }
                let waited = std::time::Instant::now();
                wait_changed(&mut rx, std::time::Duration::from_secs(1)).await;
                nest.metrics.add_extraction_wait(waited.elapsed());
                continue;
            }
            Pick::Buffered(w) => {
                let (to, tip, header) = (w.to, w.tip, w.header.clone());
                (window_logs(&nest, source, w, next, &stats).await, to, tip, header)
            }
            Pick::Own { to, tip } => {
                stats.own_fetches.fetch_add(1, Ordering::Relaxed);
                nest.metrics.set_fetch_window(to - next + 1);
                let from = overlap_from(next, nest.start_block.unwrap_or(0));
                match nest.fetch_window(source, from, to).await {
                    Err(e) if narrowing_can_help(&e, next, to) && next < to => {
                        chunker.too_large();
                        continue;
                    }
                    Err(e) if narrowing_can_help(&e, next, to) => {
                        (fetch_over_cap(&mut nest, source, next).await.map(NestLogs::Own), next, tip, None)
                    }
                    r => {
                        if let Ok(logs) = &r {
                            chunker.observed(logs.len() as u64);
                        }
                        (r.map(NestLogs::Own), to, tip, None)
                    }
                }
            }
            Pick::OverCap { block, tip } => {
                (fetch_over_cap(&mut nest, source, block).await.map(NestLogs::Own), block, tip, None)
            }
        };
        let logs = match logs {
            Ok(l) => l,
            Err(e) if is_terminal(&e) => {
                sup.quarantine(0, &e)?;
                continue;
            }
            Err(e) => {
                tracing::warn!(
                    "nest '{}' fetch {next}..={to} failed: {e:#}; retrying",
                    nest.name
                );
                sleep_secs(3).await;
                continue;
            }
        };
        let permit = ctx.permits.acquire().await.expect("never closed");
        let done = match &header {
            Some(h) => {
                let headered = HeaderedSource {
                    inner: source,
                    to,
                    header: h,
                };
                contained(nest.process_window(&headered, &logs, next, to, tip)).await
            }
            None => contained(nest.process_window(source, &logs, next, to, tip)).await,
        };
        drop(permit);
        match done {
            Ok(Some(_)) => {
                next = to + 1;
                refused = false;
                sup.mark_progress(0);
                slot.view.lock().unwrap().children = nest.children.clone();
            }
            Ok(None) => {
                if let Some(b) = nest.rewound.take() {
                    next = b;
                }
                refused = true;
            }
            Err(e) => {
                if let Some(b) = nest.rewound.take() {
                    next = b;
                }
                sup.quarantine(0, &e)?;
            }
        }
        slot.next.store(next, Ordering::Release);
        shared.trim();
        shared.bump();
    }
}

#[cfg(test)]
pub(super) fn window_bytes_for_test(logs: &[crate::rpc::Log]) -> u64 {
    window_bytes(logs, &None)
}

#[cfg(test)]
mod tests {
    use super::*;

    /// `window_buffer_mb` is 64 MiB at the default ceiling and scales with it, rounded down, as
    /// the entity cap does.
    #[test]
    fn the_buffer_bound_scales_with_the_ceiling() {
        assert_eq!(window_buffer_mb_at(crate::runtime::DEFAULT_MAX_RSS_MB), 64);
        assert_eq!(window_buffer_mb_at(1024), 32);
        assert_eq!(window_buffer_mb_at(4096), 128);
        assert_eq!(window_buffer_mb_at(2047), 63, "rounded down, never up");
    }

    #[test]
    fn the_switch_reads_a_window_count_or_off() {
        assert_eq!(parse_buffer_windows(None), Some(DEFAULT_BUFFER_WINDOWS));
        for off in ["0", "off", "shared", " off "] {
            assert_eq!(parse_buffer_windows(Some(off)), None, "{off}");
        }
        assert_eq!(parse_buffer_windows(Some("8")), Some(8));
        assert_eq!(
            parse_buffer_windows(Some("lots")),
            Some(DEFAULT_BUFFER_WINDOWS)
        );
    }
}
