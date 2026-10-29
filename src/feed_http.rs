//! `GET /_admin/feed` (RFC-0073 FR1): the writer's hot-state feed, served on the runtime's admin
//! listener behind the admin token.
//!
//! One connection carries every mount: a `hello`, a `mount` line per route, then per dataset a
//! `snapshot` followed by its `delta`s, with `mount`/`unmount` lines as the mount set changes and a
//! `heartbeat` every second. A follower that falls more than [`crate::feed::DELTA_BACKLOG`] deltas
//! behind on a dataset is sent a fresh snapshot of it; nothing it does can hold up a commit.

use crate::feed::Line;
use crate::store::HotStore;
use axum::extract::{Query, State};
use axum::http::{HeaderMap, StatusCode};
use axum::response::{IntoResponse, Response};
use std::collections::{BTreeMap, HashMap};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::mpsc;

/// How often the mount set is compared, and how often a heartbeat goes out.
const WATCH_EVERY: Duration = Duration::from_millis(250);
const HEARTBEAT_EVERY: Duration = Duration::from_secs(1);
/// Encoded feed bytes one connection may have queued. A snapshot's chunks wait for room; anything
/// else that finds none drops the follower, which reconnects and takes fresh snapshots.
pub const FEED_QUEUE_BYTES: u64 = 8 << 20;
/// Encoded feed bytes every connection together may have queued, paid from the ingestion reservation
/// beside the hot-tip copies.
pub const FEED_QUEUE_TOTAL_BYTES: u64 = 64 << 20;
/// What a snapshot or delta is encoded into and queued in.
const CHUNK: usize = 64 << 10;
/// How long a snapshot waits for room before its follower is taken to have stalled and is dropped,
/// which also ends the read transaction it is taken in.
const SNAPSHOT_STALL: Duration = Duration::from_secs(30);
/// Ops past which a delta's v2 body is encoded off the async workers.
const ENCODE_INLINE_OPS: usize = 512;

/// The bounds on queued feed bytes.
#[derive(Clone, Copy, Debug)]
pub struct QueueLimits {
    pub per_connection: u64,
    pub total: u64,
    pub stall: Duration,
}

impl Default for QueueLimits {
    fn default() -> Self {
        QueueLimits {
            per_connection: FEED_QUEUE_BYTES,
            total: FEED_QUEUE_TOTAL_BYTES,
            stall: SNAPSHOT_STALL,
        }
    }
}

/// Feed bytes queued across every connection, and the most there have been.
#[derive(Default, Debug)]
pub struct FeedMemory {
    queued: AtomicU64,
    peak: AtomicU64,
    /// Signalled whenever bytes are given back anywhere: a snapshot short of the total waits on any
    /// connection's reader, not only its own.
    room: (std::sync::Mutex<()>, std::sync::Condvar),
}

impl FeedMemory {
    pub fn queued(&self) -> u64 {
        self.queued.load(Ordering::SeqCst)
    }

    pub fn peak(&self) -> u64 {
        self.peak.load(Ordering::SeqCst)
    }
}

/// One connection's queue: its bytes, and whether its follower was dropped.
struct Conn {
    queued: Arc<AtomicU64>,
    peak: AtomicU64,
    /// Whether a snapshot is waiting for room on this connection.
    waiting: AtomicBool,
    closed: tokio::sync::watch::Sender<bool>,
    memory: Arc<FeedMemory>,
    limits: QueueLimits,
}

impl Conn {
    fn reserve(&self, len: u64) -> bool {
        let fits = |cap: u64| move |q: u64| (q + len <= cap).then_some(q + len);
        let Ok(was) = self.queued.fetch_update(
            Ordering::SeqCst,
            Ordering::SeqCst,
            fits(self.limits.per_connection),
        ) else {
            return false;
        };
        match self.memory.queued.fetch_update(
            Ordering::SeqCst,
            Ordering::SeqCst,
            fits(self.limits.total),
        ) {
            Ok(all) => {
                self.peak.fetch_max(was + len, Ordering::SeqCst);
                self.memory.peak.fetch_max(all + len, Ordering::SeqCst);
                true
            }
            Err(_) => {
                self.queued.fetch_sub(len, Ordering::SeqCst);
                false
            }
        }
    }

    fn release(&self, len: u64) {
        self.queued.fetch_sub(len, Ordering::SeqCst);
        self.memory.queued.fetch_sub(len, Ordering::SeqCst);
        let _held = self.memory.room.0.lock().unwrap_or_else(|e| e.into_inner());
        self.memory.room.1.notify_all();
    }

    fn is_closed(&self) -> bool {
        *self.closed.borrow()
    }

    fn close(&self, why: &str) {
        if !self.closed.send_replace(true) {
            tracing::info!("dropping a follower: {why}; it reconnects and resnapshots");
        }
        let _held = self.memory.room.0.lock().unwrap_or_else(|e| e.into_inner());
        self.memory.room.1.notify_all();
    }
}

/// Bytes on a connection's queue, given back when the body takes them or the queue goes.
struct Charged {
    bytes: bytes::Bytes,
    conn: Arc<Conn>,
}

impl Drop for Charged {
    fn drop(&mut self) {
        self.conn.release(self.bytes.len() as u64);
    }
}

#[derive(Clone)]
struct Queue {
    conn: Arc<Conn>,
    tx: mpsc::UnboundedSender<Charged>,
    /// Held while a line is queued, since a line may go in several chunks.
    line: Arc<tokio::sync::Mutex<()>>,
}

impl Queue {
    fn new(
        memory: Arc<FeedMemory>,
        limits: QueueLimits,
    ) -> (Queue, mpsc::UnboundedReceiver<Charged>) {
        let (tx, rx) = mpsc::unbounded_channel();
        let conn = Arc::new(Conn {
            queued: Arc::default(),
            peak: AtomicU64::new(0),
            waiting: AtomicBool::new(false),
            closed: tokio::sync::watch::Sender::new(false),
            memory,
            limits,
        });
        let line = Arc::default();
        (Queue { conn, tx, line }, rx)
    }

    /// Queue one whole line of a single chunk or less, or say the follower is gone.
    async fn line(&self, bytes: bytes::Bytes) -> bool {
        let _line = self.line.lock().await;
        self.send(bytes)
    }

    /// Resolves once the body is gone or the follower was dropped.
    async fn gone(&self) {
        let mut closed = self.conn.closed.subscribe();
        tokio::select! {
            _ = self.tx.closed() => {}
            _ = closed.wait_for(|c| *c) => {}
        }
    }

    /// Queue `bytes` if there is room. A follower with none is dropped, never waited for, so nothing
    /// it does holds up a commit.
    fn send(&self, bytes: bytes::Bytes) -> bool {
        if self.conn.is_closed() {
            return false;
        }
        if !self.conn.reserve(bytes.len() as u64) {
            self.conn.close("its feed queue is full");
            return false;
        }
        self.charge(bytes)
    }

    /// Queue `bytes`, waiting for room. Only a snapshot's chunks wait, on the snapshot's own thread.
    fn send_waiting(&self, bytes: bytes::Bytes) -> bool {
        let deadline = std::time::Instant::now() + self.conn.limits.stall;
        let room = &self.conn.memory.room;
        let mut held = room.0.lock().unwrap_or_else(|e| e.into_inner());
        let queued = loop {
            if self.conn.is_closed() {
                break false;
            }
            if self.conn.reserve(bytes.len() as u64) {
                break true;
            }
            let left = deadline.saturating_duration_since(std::time::Instant::now());
            if left.is_zero() {
                drop(held);
                self.conn.close("a snapshot found no room in its queue");
                self.conn.waiting.store(false, Ordering::SeqCst);
                return false;
            }
            self.conn.waiting.store(true, Ordering::SeqCst);
            held = room
                .1
                .wait_timeout(held, left)
                .unwrap_or_else(|e| e.into_inner())
                .0;
        };
        drop(held);
        self.conn.waiting.store(false, Ordering::SeqCst);
        queued && self.charge(bytes)
    }

    fn charge(&self, bytes: bytes::Bytes) -> bool {
        let charged = Charged {
            bytes,
            conn: self.conn.clone(),
        };
        self.tx.send(charged).is_ok()
    }
}

/// A line written onto a queue in [`CHUNK`]s as it is encoded, so no whole encoded copy of it exists.
struct Chunks<'q> {
    queue: &'q Queue,
    buf: Vec<u8>,
    wait: bool,
    sent: bool,
    /// Set when the forwarder that asked for this line is gone.
    cancelled: Option<Arc<AtomicBool>>,
    /// Feed v2: each chunk goes as a part frame, and [`Chunks::end`] closes the run.
    framed: bool,
}

impl<'q> Chunks<'q> {
    fn new(queue: &'q Queue, wait: bool) -> Self {
        Chunks {
            queue,
            buf: Vec::new(),
            wait,
            sent: false,
            cancelled: None,
            framed: false,
        }
    }

    fn framed(queue: &'q Queue, wait: bool, framed: bool) -> Self {
        Chunks {
            framed,
            ..Chunks::new(queue, wait)
        }
    }

    /// The line is written: send what is left, and under v2 the frame that ends its parts.
    fn end(&mut self) -> std::io::Result<()> {
        use std::io::Write;
        if !self.framed {
            self.write_all(b"\n")?;
            return self.flush();
        }
        self.flush()?;
        self.put(bytes::Bytes::copy_from_slice(&crate::feed_wire::header(
            crate::feed_wire::END,
            0,
        )))
    }

    fn put(&mut self, bytes: bytes::Bytes) -> std::io::Result<()> {
        self.sent = true;
        let queued = if self.wait {
            self.queue.send_waiting(bytes)
        } else {
            self.queue.send(bytes)
        };
        if queued {
            Ok(())
        } else {
            Err(std::io::Error::other("the follower was dropped"))
        }
    }
}

/// Set when a forwarder is aborted, so a snapshot it started on another thread stops.
#[derive(Default)]
struct Cancelled(Arc<AtomicBool>);

impl Drop for Cancelled {
    fn drop(&mut self) {
        self.0.store(true, Ordering::SeqCst);
    }
}

impl std::io::Write for Chunks<'_> {
    fn write(&mut self, data: &[u8]) -> std::io::Result<usize> {
        let n = data.len().min(CHUNK - self.buf.len());
        self.buf.extend_from_slice(&data[..n]);
        if self.buf.len() == CHUNK {
            self.flush()?;
        }
        Ok(n)
    }

    fn flush(&mut self) -> std::io::Result<()> {
        if self.buf.is_empty() {
            return Ok(());
        }
        if self
            .cancelled
            .as_ref()
            .is_some_and(|c| c.load(Ordering::SeqCst))
        {
            if self.sent {
                self.queue
                    .conn
                    .close("its dataset went part-way through a snapshot");
            }
            return Err(std::io::Error::other("the forwarder went"));
        }
        let chunk = std::mem::take(&mut self.buf);
        let bytes = if self.framed {
            crate::feed_wire::frame(crate::feed_wire::PART, &chunk).into()
        } else {
            bytes::Bytes::from(chunk)
        };
        self.put(bytes)
    }
}

/// Counts what is written, to size a line before any of it is queued.
#[derive(Default)]
struct Counted(u64);

impl std::io::Write for Counted {
    fn write(&mut self, data: &[u8]) -> std::io::Result<usize> {
        self.0 += data.len() as u64;
        Ok(data.len())
    }

    fn flush(&mut self) -> std::io::Result<()> {
        Ok(())
    }
}

/// One served route as the feed needs it.
#[derive(Clone)]
pub struct Served {
    pub route: String,
    pub dir: PathBuf,
    pub nid: Option<String>,
    pub surface: crate::allowlist::Surface,
    pub tenant: Option<String>,
    pub share: Option<crate::runtime::TenantShare>,
    pub limits: crate::limits::Limits,
    pub chain: String,
    pub isolation: Option<String>,
    pub store: Arc<dyn HotStore>,
}

impl Served {
    /// `state` as served under `route`, with its tenant's share from `shares`.
    pub fn of(
        route: &str,
        state: &crate::serve::AppState,
        shares: &BTreeMap<String, crate::runtime::TenantShare>,
    ) -> Served {
        let tenant = state.tenant.as_ref().map(|s| s.tenant().to_string());
        Served {
            route: route.to_string(),
            dir: state.dir.clone(),
            nid: state.nid.as_deref().map(str::to_string),
            surface: (*state.surface).clone(),
            share: tenant.as_ref().and_then(|t| shares.get(t).copied()),
            tenant,
            limits: state.limits,
            chain: state.chain.clone(),
            isolation: state.isolation.as_deref().map(str::to_string),
            store: state.store.clone(),
        }
    }
}

/// What is mounted now.
pub type Roster =
    Arc<dyn Fn() -> futures::future::BoxFuture<'static, Vec<Served>> + Send + Sync + 'static>;

/// The roster of a running runtime.
pub fn runtime_roster(handles: Arc<tokio::sync::Mutex<crate::runtime::RuntimeHandles>>) -> Roster {
    Arc::new(move || {
        let handles = handles.clone();
        Box::pin(async move {
            let h = handles.lock().await;
            h.states
                .iter()
                .map(|(route, s)| Served::of(route, s, &h.tenant_shares))
                .collect()
        })
    })
}

#[derive(Clone)]
struct FeedState {
    roster: Roster,
    root: PathBuf,
    token: Option<String>,
    memory: Arc<FeedMemory>,
    limits: QueueLimits,
}

#[derive(serde::Deserialize)]
struct TokenQuery {
    token: Option<String>,
    /// Datasets, comma-separated, whose snapshots and deltas the follower cannot hold: their mounts
    /// are still sent, their data is not.
    exclude: Option<String>,
    /// The highest feed format the follower reads; 2 asks for frames ([`crate::feed_wire`]).
    feed: Option<u32>,
}

/// The feed's route. Absent, like the rest of the admin surface, when the admin API is off.
pub fn routes(
    roster: Roster,
    root: PathBuf,
    admin_enabled: bool,
    token: Option<String>,
) -> axum::Router {
    if !admin_enabled {
        return axum::Router::new();
    }
    axum::Router::new()
        .route("/_admin/feed", axum::routing::get(feed))
        .route("/_admin/feed/applied", axum::routing::post(applied))
        .with_state(FeedState {
            roster,
            root,
            token,
            memory: Arc::default(),
            limits: QueueLimits::default(),
        })
}

/// A dataset's name on the feed: its directory under the runtime root.
pub fn dataset_key(root: &Path, dir: &Path) -> String {
    dir.strip_prefix(root)
        .unwrap_or(dir)
        .to_string_lossy()
        .into_owned()
}

fn line(l: &Line) -> bytes::Bytes {
    let mut out = serde_json::to_vec(l).unwrap_or_default();
    out.push(b'\n');
    out.into()
}

/// `l` as the connection's format carries it: a line, or one JSON frame.
fn encode(l: &Line, v2: bool) -> bytes::Bytes {
    if !v2 {
        return line(l);
    }
    crate::feed_wire::frame(
        crate::feed_wire::JSON,
        &serde_json::to_vec(l).unwrap_or_default(),
    )
    .into()
}

/// A dataset's data identity, the prefix its publisher mirrors it under, as the publisher works it
/// out. Worked out at most every [`IDENTITY_TTL`] per dataset, since every connection asks.
fn data_identity(dir: &Path) -> Option<String> {
    type Cache = std::sync::Mutex<HashMap<PathBuf, (std::time::Instant, Option<String>)>>;
    static CACHE: std::sync::OnceLock<Cache> = std::sync::OnceLock::new();
    let cache = CACHE.get_or_init(Default::default);
    if let Some((at, id)) = cache.lock().unwrap_or_else(|e| e.into_inner()).get(dir) {
        if at.elapsed() < IDENTITY_TTL {
            return id.clone();
        }
    }
    let id = crate::blob::build_manifest(dir, None)
        .ok()
        .map(|m| m.data_identity());
    cache
        .lock()
        .unwrap_or_else(|e| e.into_inner())
        .insert(dir.to_path_buf(), (std::time::Instant::now(), id.clone()));
    id
}

const IDENTITY_TTL: Duration = Duration::from_secs(30);

async fn feed(
    State(s): State<FeedState>,
    Query(q): Query<TokenQuery>,
    headers: HeaderMap,
) -> Response {
    if !crate::serve::token_ok(s.token.as_deref(), q.token.as_deref(), &headers) {
        return (
            StatusCode::UNAUTHORIZED,
            axum::Json(serde_json::json!({"error": "admin token required"})),
        )
            .into_response();
    }
    let (tx, rx) = Queue::new(s.memory.clone(), s.limits);
    let excluded = q
        .exclude
        .as_deref()
        .unwrap_or("")
        .split(',')
        .filter(|d| !d.is_empty())
        .map(str::to_string)
        .collect();
    let closed = tx.conn.closed.subscribe();
    let v2 = q.feed.is_some_and(|v| v >= crate::feed_wire::FEED_V2);
    tokio::spawn(serve_connection(s, tx, excluded, v2));
    let body = futures::stream::unfold((rx, closed), |(mut rx, mut closed)| async move {
        // A dropped follower's queue is discarded, not drained: what is left may be part of a line.
        let next = tokio::select! {
            biased;
            _ = closed.wait_for(|c| *c) => None,
            next = rx.recv() => next,
        };
        next.map(|c| {
            let bytes = c.bytes.clone();
            drop(c);
            (Ok::<_, std::convert::Infallible>(bytes), (rx, closed))
        })
    });
    let content_type = if v2 {
        crate::feed_wire::CONTENT_TYPE
    } else {
        "application/x-ndjson"
    };
    (
        [(axum::http::header::CONTENT_TYPE, content_type)],
        axum::body::Body::from_stream(body),
    )
        .into_response()
}

#[derive(serde::Deserialize)]
struct AppliedQuery {
    token: Option<String>,
    conn: u64,
    /// The round trip the follower last measured for one of these reports, in microseconds, which
    /// visibility lag takes half of.
    rtt_us: Option<u64>,
}

/// `POST /_admin/feed/applied?conn=<n>`: what a follower has applied, per dataset, as
/// `{"<dataset>": [epoch, generation]}`, so a segment it may still name is kept until it has moved on.
async fn applied(
    State(s): State<FeedState>,
    Query(q): Query<AppliedQuery>,
    headers: HeaderMap,
    axum::Json(positions): axum::Json<BTreeMap<String, (u64, u64)>>,
) -> Response {
    if !crate::serve::token_ok(s.token.as_deref(), q.token.as_deref(), &headers) {
        return (
            StatusCode::UNAUTHORIZED,
            axum::Json(serde_json::json!({"error": "admin token required"})),
        )
            .into_response();
    }
    let now = std::time::Instant::now();
    let rtt = q.rtt_us.map(Duration::from_micros);
    for (dataset, at) in positions {
        crate::feed_metrics::metrics().acked(q.conn, &dataset, at, rtt, now);
        crate::seal::follower_applied(&s.root.join(dataset), q.conn, at);
    }
    crate::seal::release_retained();
    StatusCode::NO_CONTENT.into_response()
}

/// Aborts the forwarders when the connection ends, however it ends.
struct Forwarders(HashMap<String, tokio::task::JoinHandle<()>>);

impl Drop for Forwarders {
    fn drop(&mut self) {
        for (_, h) in self.0.drain() {
            h.abort();
        }
    }
}

/// The datasets one connection's follower follows, for the segments kept for it; it stops counting
/// when the connection ends.
struct Following {
    conn: u64,
    dirs: std::collections::BTreeSet<PathBuf>,
}

impl Following {
    fn follow(&mut self, dir: PathBuf) {
        crate::seal::follower_connected(&dir, self.conn);
        self.dirs.insert(dir);
    }

    fn unfollow(&mut self, dir: &Path) {
        crate::seal::follower_gone(dir, self.conn);
        self.dirs.remove(dir);
    }
}

impl Drop for Following {
    fn drop(&mut self) {
        for dir in &self.dirs {
            crate::seal::follower_gone(dir, self.conn);
        }
    }
}

/// Forgets a connection's figures when it ends.
struct Gone(u64);

impl Drop for Gone {
    fn drop(&mut self) {
        crate::feed_metrics::metrics().gone(self.0);
    }
}

async fn serve_connection(
    s: FeedState,
    tx: Queue,
    excluded: std::collections::BTreeSet<String>,
    v2: bool,
) {
    static CONNS: AtomicU64 = AtomicU64::new(1);
    let conn = CONNS.fetch_add(1, Ordering::Relaxed);
    crate::feed_metrics::metrics().connection(conn, &tx.conn.queued);
    let _gone = Gone(conn);
    let hello = Line::Hello {
        version: env!("CARGO_PKG_VERSION").to_string(),
        feed: if v2 {
            crate::feed_wire::FEED_V2
        } else {
            crate::feed::FEED_VERSION
        },
        conn: Some(conn),
    };
    if !tx.line(encode(&hello, v2)).await {
        return;
    }
    let mut following = Following {
        conn,
        dirs: Default::default(),
    };
    let mut routes: BTreeMap<String, Vec<u8>> = BTreeMap::new();
    let mut announced = false;
    let mut forwarders = Forwarders(HashMap::new());
    let mut stores: BTreeMap<String, Arc<dyn HotStore>> = BTreeMap::new();
    let mut heartbeat = tokio::time::interval(HEARTBEAT_EVERY);
    let mut watch = tokio::time::interval(WATCH_EVERY);
    loop {
        tokio::select! {
            _ = watch.tick() => {
                let served = (s.roster)().await;
                let now: BTreeMap<String, &Served> =
                    served.iter().map(|m| (m.route.clone(), m)).collect();
                for gone in routes.keys().filter(|r| !now.contains_key(*r)).cloned().collect::<Vec<_>>() {
                    routes.remove(&gone);
                    if !tx.line(encode(&Line::Unmount { route: gone }, v2)).await {
                        return;
                    }
                }
                let dirs: Vec<PathBuf> = now.values().map(|m| m.dir.clone()).collect();
                let identities: HashMap<PathBuf, Option<String>> =
                    tokio::task::spawn_blocking(move || {
                        dirs.into_iter()
                            .map(|d| {
                                let id = data_identity(&d);
                                (d, id)
                            })
                            .collect()
                    })
                    .await
                    .unwrap_or_default();
                for (route, m) in &now {
                    let mount = encode(&Line::Mount {
                        route: route.clone(),
                        dataset: dataset_key(&s.root, &m.dir),
                        nid: m.nid.clone(),
                        sql: m.surface.access,
                        queries: m.surface.queries.clone(),
                        tenant: m.tenant.clone(),
                        share: m.share,
                        limits: Some((&m.limits).into()),
                        chain: Some(m.chain.clone()),
                        isolation: m.isolation.clone(),
                        data_identity: identities.get(&m.dir).cloned().flatten(),
                    }, v2);
                    if routes.get(route).is_some_and(|sent| sent[..] == mount[..]) {
                        continue;
                    }
                    routes.insert(route.clone(), mount.to_vec());
                    if !tx.line(mount).await {
                        return;
                    }
                }
                if !announced {
                    announced = true;
                    if !tx.line(encode(&Line::Mounted, v2)).await {
                        return;
                    }
                }
                let live: BTreeMap<String, Arc<dyn HotStore>> = served
                    .iter()
                    .map(|m| (dataset_key(&s.root, &m.dir), m.store.clone()))
                    .filter(|(d, _)| !excluded.contains(d))
                    .collect();
                for (dataset, store) in &live {
                    let same = stores.get(dataset).is_some_and(|old| Arc::ptr_eq(old, store));
                    if !same {
                        if let Some(h) = forwarders.0.remove(dataset) {
                            h.abort();
                        }
                        following.follow(s.root.join(dataset));
                        let task = tokio::spawn(forward(dataset.clone(), store.clone(), tx.clone(), v2));
                        forwarders.0.insert(dataset.clone(), task);
                    }
                }
                for gone in stores.keys().filter(|d| !live.contains_key(*d)).cloned().collect::<Vec<_>>() {
                    if let Some(h) = forwarders.0.remove(&gone) {
                        h.abort();
                    }
                    following.unfollow(&s.root.join(&gone));
                }
                stores = live;
            }
            _ = heartbeat.tick() => {
                crate::seal::release_retained();
                let positions = stores
                    .iter()
                    .filter_map(|(d, st)| Some((d.clone(), st.feed_position()?)))
                    .collect();
                // Skipped while a snapshot streams: the next one says the same.
                let Ok(_line) = tx.line.try_lock() else {
                    continue;
                };
                if !tx.send(encode(&Line::Heartbeat { positions }, v2)) {
                    return;
                }
            }
            _ = tx.gone() => return,
        }
    }
}

/// One dataset's snapshot and deltas onto the connection, resnapshotting whenever the deltas cannot
/// be relied on to follow: the follower fell behind, or a commit could not be described.
async fn forward(dataset: String, store: Arc<dyn HotStore>, tx: Queue, v2: bool) {
    use std::io::Write;
    use tokio::sync::broadcast::error::RecvError;
    let name = serde_json::Value::String(dataset.clone()).to_string();
    let cancelled = Cancelled::default();
    'snapshot: loop {
        let st = store.clone();
        let q = tx.clone();
        let flag = cancelled.0.clone();
        let head = format!("{{\"snapshot\":{{\"dataset\":{name},\"snapshot\":");
        let taken = tokio::task::spawn_blocking(move || {
            let _line = q.line.blocking_lock();
            let mut out = Chunks::framed(&q, true, v2);
            out.cancelled = Some(flag);
            out.buf.extend_from_slice(head.as_bytes());
            let streamed = st.feed_stream(&mut out).and_then(|deltas| {
                if deltas.is_some() {
                    out.write_all(b"}}")?;
                    out.end()?;
                }
                Ok(deltas)
            });
            if streamed.is_err() && out.sent {
                // Part of the line is on the wire, and the rest never will be.
                q.conn.close("its snapshot stopped part-way");
            }
            (streamed, out.sent)
        })
        .await;
        let mut deltas = match taken {
            Ok((Ok(Some(deltas)), _)) => deltas,
            Ok((Ok(None), _)) => {
                tracing::warn!("{dataset}: this hot store has no feed; not followed");
                return;
            }
            Ok((Err(e), false)) if !tx.conn.is_closed() => {
                tracing::warn!("{dataset}: feed snapshot failed, retrying: {e:#}");
                tokio::time::sleep(Duration::from_secs(1)).await;
                continue;
            }
            _ => return,
        };
        loop {
            match deltas.recv().await {
                Ok(delta) if delta.resync => continue 'snapshot,
                Ok(delta) => {
                    if let Some(at) = delta.committed {
                        let kind = if delta.catalogue.is_some() {
                            crate::feed_metrics::Kind::Seal
                        } else {
                            crate::feed_metrics::Kind::Tip
                        };
                        crate::feed_metrics::metrics().committed(
                            &dataset,
                            delta.epoch,
                            delta.generation,
                            at,
                            kind,
                        );
                    }
                    if v2 {
                        fn encoded(delta: &crate::feed::Delta) -> bytes::Bytes {
                            delta
                                .wire
                                .get_or_init(|| {
                                    let started = std::time::Instant::now();
                                    let body = crate::feed_wire::encode_delta(delta)
                                        .map(bytes::Bytes::from)
                                        .unwrap_or_default();
                                    crate::feed_metrics::metrics()
                                        .encoded("v2", started.elapsed());
                                    body
                                })
                                .clone()
                        }
                        // A backfill window takes tens of milliseconds to encode: not on a worker
                        // the runtime's other tasks are waiting for.
                        let body = if delta.ops.len() > ENCODE_INLINE_OPS && delta.wire.get().is_none() {
                            let d = delta.clone();
                            tokio::task::spawn_blocking(move || encoded(&d))
                                .await
                                .unwrap_or_default()
                        } else {
                            encoded(&delta)
                        };
                        if body.is_empty() {
                            continue 'snapshot;
                        }
                        let frame = crate::feed_wire::delta_frame(&dataset, &body);
                        if frame.len() as u64 <= tx.conn.limits.per_connection {
                            crate::feed_metrics::metrics().delta_sent("v2", frame.len());
                            if !tx.line(frame.into()).await {
                                return;
                            }
                            continue;
                        }
                    }
                    let head = format!("{{\"delta\":{{\"dataset\":{name},\"delta\":");
                    let started = std::time::Instant::now();
                    let mut size = Counted(head.len() as u64 + 3);
                    if serde_json::to_writer(&mut size, &*delta).is_err() {
                        continue 'snapshot;
                    }
                    if !v2 {
                        crate::feed_metrics::metrics().encoded("v1", started.elapsed());
                        crate::feed_metrics::metrics().delta_sent("v1", size.0 as usize);
                    }
                    let write = move |q: &Queue, wait: bool| {
                        // Under v2, a delta too large for one frame goes as v1's JSON in parts.
                        let mut out = Chunks::framed(q, wait, v2);
                        out.buf.extend_from_slice(head.as_bytes());
                        serde_json::to_writer(&mut out, &*delta)
                            .map_err(std::io::Error::from)
                            .and_then(|()| out.write_all(b"}}"))
                            .and_then(|()| out.end())
                    };
                    let sent = if size.0 <= tx.conn.limits.per_connection && !v2 {
                        let _line = tx.line.lock().await;
                        write(&tx, false)
                    } else {
                        // One the queue could never hold whole streams as a snapshot does.
                        let q = tx.clone();
                        let flag = cancelled.0.clone();
                        tokio::task::spawn_blocking(move || {
                            let _line = q.line.blocking_lock();
                            if flag.load(Ordering::SeqCst) {
                                return Ok(());
                            }
                            let sent = write(&q, true);
                            if sent.is_err() {
                                q.conn.close("a delta stopped part-way");
                            }
                            sent
                        })
                        .await
                        .unwrap_or_else(|e| Err(std::io::Error::other(e)))
                    };
                    if sent.is_err() {
                        return;
                    }
                }
                Err(RecvError::Lagged(n)) => {
                    tracing::info!("{dataset}: a follower fell {n} deltas behind; resnapshotting");
                    continue 'snapshot;
                }
                Err(RecvError::Closed) => return,
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::store::Store;

    const KIB: u64 = 1 << 10;

    /// A store holding `rows` rows of 32 KiB each.
    fn store(dir: &Path, rows: usize) -> Arc<Store> {
        let store = Arc::new(Store::open(&dir.join("nuthatch.redb")).unwrap());
        for i in 0..rows {
            let row =
                serde_json::json!({"table": "t", "block_number": 1, "pad": "x".repeat(32 << 10)});
            store
                .put_entity(&format!("{i:012}-000000"), &row.to_string())
                .unwrap();
        }
        store
    }

    /// A stall well past [`read_line`]'s wait, so a snapshot that moves only when its stall ends,
    /// rather than when a reader gives room back, fails the read.
    fn limits(per_connection: u64, total: u64) -> QueueLimits {
        QueueLimits {
            per_connection,
            total,
            stall: Duration::from_secs(60),
        }
    }

    /// Reads one line off `rx`, giving each chunk back as it goes.
    async fn read_line(rx: &mut mpsc::UnboundedReceiver<Charged>) -> Vec<u8> {
        let mut got = Vec::new();
        while !got.ends_with(b"\n") {
            let chunk = tokio::time::timeout(Duration::from_secs(15), rx.recv())
                .await
                .expect("the line stalled")
                .expect("the line ended early");
            got.extend_from_slice(&chunk.bytes);
        }
        got
    }

    /// Waits, with nothing read meanwhile, until every one of `queues` has a snapshot blocked for
    /// room: each has queued all the budgets let it.
    async fn blocked(queues: &[&Queue]) {
        let deadline = std::time::Instant::now() + Duration::from_secs(15);
        while !queues.iter().all(|q| q.conn.waiting.load(Ordering::SeqCst)) {
            assert!(
                std::time::Instant::now() < deadline,
                "a snapshot never ran out of room"
            );
            tokio::task::yield_now().await;
        }
    }

    fn expected(store: &Store, dataset: &str) -> Vec<u8> {
        line(&Line::Snapshot {
            dataset: crate::feed::DatasetName(dataset.to_string()),
            snapshot: store.feed_subscribe().unwrap().snapshot,
        })
        .to_vec()
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn a_snapshot_larger_than_its_queue_streams_through_within_it() {
        let tmp = tempfile::tempdir().unwrap();
        let store = store(tmp.path(), 64);
        let limits = limits(256 * KIB, 1 << 20);
        let (q, mut rx) = Queue::new(Arc::default(), limits);
        let task = tokio::spawn(forward("data/a".into(), store.clone(), q.clone(), false));
        // Nothing is read until the queue holds all it may.
        blocked(&[&q]).await;
        assert_eq!(q.conn.queued.load(Ordering::SeqCst), limits.per_connection);
        let got = read_line(&mut rx).await;
        assert_eq!(
            got,
            expected(&store, "data/a"),
            "the snapshot line, byte for byte"
        );
        assert!(got.len() as u64 > 4 * limits.per_connection);
        let peak = q.conn.peak.load(Ordering::SeqCst);
        assert_eq!(peak, limits.per_connection, "never past its queue's limit");
        task.abort();
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn a_stalled_follower_is_dropped_once_its_queue_fills_and_commits_go_on() {
        let tmp = tempfile::tempdir().unwrap();
        let store = store(tmp.path(), 1);
        let memory = Arc::<FeedMemory>::default();
        let limits = limits(256 * KIB, 1 << 20);
        let (q, mut rx) = Queue::new(memory.clone(), limits);
        let task = tokio::spawn(forward("data/a".into(), store.clone(), q.clone(), false));
        read_line(&mut rx).await;
        // The follower reads no further while the writer goes on committing.
        let started = std::time::Instant::now();
        for i in 1..=64 {
            let row =
                serde_json::json!({"table": "t", "block_number": 2, "pad": "y".repeat(16 << 10)});
            store
                .put_entity(&format!("{i:012}-000001"), &row.to_string())
                .unwrap();
        }
        // Dropped before a snapshot's stall would be up, so not by waiting for room.
        let mut closed = q.conn.closed.subscribe();
        let dropped = tokio::time::timeout(limits.stall / 2, closed.wait_for(|c| *c)).await;
        assert!(
            dropped.is_ok(),
            "the stalled follower was dropped, not waited for: {:?}",
            started.elapsed()
        );
        assert!(q.conn.peak.load(Ordering::SeqCst) <= limits.per_connection);
        drop(rx);
        assert_eq!(
            memory.queued(),
            0,
            "a dropped follower's queue is given back"
        );
        task.abort();
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn every_followers_queue_together_stays_within_the_total() {
        let tmp = tempfile::tempdir().unwrap();
        let store = store(tmp.path(), 64);
        let memory = Arc::<FeedMemory>::default();
        let limits = limits(256 * KIB, 512 * KIB);
        let want = expected(&store, "data/a");
        let (open, gate) = tokio::sync::watch::channel(false);
        let mut followers = Vec::new();
        let mut queues = Vec::new();
        for _ in 0..4 {
            let (q, mut rx) = Queue::new(memory.clone(), limits);
            let task = tokio::spawn(forward("data/a".into(), store.clone(), q.clone(), false));
            let mut gate = gate.clone();
            followers.push(tokio::spawn(async move {
                gate.wait_for(|open| *open).await.unwrap();
                let got = read_line(&mut rx).await;
                task.abort();
                got
            }));
            queues.push(q);
        }
        // Nobody reads until every queue has taken what the budgets allow.
        let all = limits.total.min(4 * limits.per_connection);
        blocked(&queues.iter().collect::<Vec<_>>()).await;
        assert_eq!(memory.queued(), all, "the total, and no more, is queued");
        for q in &queues {
            assert!(q.conn.queued.load(Ordering::SeqCst) <= limits.per_connection);
        }
        open.send_replace(true);
        for f in followers {
            assert_eq!(
                f.await.unwrap(),
                want,
                "every follower had the whole snapshot"
            );
        }
        assert_eq!(memory.peak(), all, "never past the total");
    }
}
