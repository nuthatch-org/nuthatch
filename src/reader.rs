//! `nuthatch serve --runtime <dir> --follow <writer-url>` (RFC-0073 FR1, FR7): a read-only process
//! serving every mount of a runtime beside the writer that indexes it.
//!
//! Sealed segments are read straight from the runtime directory, since they are immutable files.
//! redb is never opened. The hot window comes from the writer's `GET /_admin/feed`: a snapshot per
//! dataset, then its deltas, applied to an in-memory [`MemStore`]. A route answers `503` with
//! `reason: "catching_up"` until its dataset's snapshot is applied, then with the provenance of the
//! generation applied.
//!
//! When the writer goes away, the follower keeps serving the sealed data and the last hot state it
//! applied, reports how long the writer has been absent, and reconnects. A restarted writer is a new
//! epoch, so the follower takes fresh snapshots, swapping each in only once it is complete.
//!
//! State the writer derives in memory, the built-in balance, exposure and velocity views and
//! RFC-0041's entities, the follower derives itself: rebuilt from its snapshot and the sealed
//! segments as a writer's restart rebuilds it, then fed the rows each window stores or a reorg
//! drops ([`crate::indexer::FollowerDerived`]), entities reading offchain tables brought up to the
//! dataset's snapshots after each window as ingest does. Derived state a feed tore part-way, or that
//! a seal outran, is rebuilt; until it is whole its routes answer `503` with `reason: "catching_up"`.

use crate::feed::{Line, MemStore};
use crate::serve::{AppState, LiveRuntime, SharedNest};
use crate::store::HotStore;
use anyhow::{bail, Context, Result};
use axum::extract::Request;
use axum::http::StatusCode;
use axum::middleware::Next;
use axum::response::{IntoResponse, Response};
use axum::Json;
use std::collections::{BTreeMap, BTreeSet, HashMap};
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

/// A connection that has carried nothing for this long, heartbeats included, is treated as gone.
const SILENCE: Duration = Duration::from_secs(5);
/// How often a follower tells the writer what it has applied.
const REPORT_EVERY: Duration = Duration::from_secs(1);

/// Lines the parser has handed on and the handler has applied, so that a line's cap is worked out
/// only once every line before it, its dataset's `mount` among them, has been applied.
#[derive(Default)]
struct Applied {
    /// Handed on, applied, and whether the handler has stopped.
    counts: Mutex<(u64, u64, bool)>,
    moved: std::sync::Condvar,
}

impl Applied {
    fn counts(&self) -> std::sync::MutexGuard<'_, (u64, u64, bool)> {
        self.counts.lock().unwrap_or_else(|e| e.into_inner())
    }

    fn sent(&self) {
        self.counts().0 += 1;
    }

    fn handled(&self) {
        self.counts().1 += 1;
        self.moved.notify_all();
    }

    fn done(&self) {
        self.counts().2 = true;
        self.moved.notify_all();
    }

    /// Wait until every line handed on has been applied; false if the handler stopped first, when
    /// nothing more will be applied on this connection.
    fn caught_up(&self) -> bool {
        let mut c = self.counts();
        while c.1 < c.0 && !c.2 {
            c = self.moved.wait(c).unwrap_or_else(|e| e.into_inner());
        }
        !c.2
    }
}

struct AbortOnDrop(tokio::task::JoinHandle<()>);

impl Drop for AbortOnDrop {
    fn drop(&mut self) {
        self.0.abort();
    }
}
const BACKOFF_FIRST: Duration = Duration::from_millis(200);
const BACKOFF_MAX: Duration = Duration::from_secs(5);
/// The most a request body is read to look for an entity name.
const BODY_LIMIT: usize = 1024 * 1024;

/// The admin token to present to the writer: `NUTHATCH_ADMIN_TOKEN`, else the one the writer keeps in
/// the runtime directory. Never a flag, so it stays out of process listings.
pub fn writer_token(root: &Path) -> Result<String> {
    if let Some(t) = std::env::var("NUTHATCH_ADMIN_TOKEN")
        .ok()
        .filter(|t| !t.trim().is_empty())
    {
        return Ok(t);
    }
    crate::admin_token::read(root).with_context(|| {
        format!(
            "no admin token for the writer: set NUTHATCH_ADMIN_TOKEN, or run beside a writer that \
             keeps one at {}",
            crate::admin_token::path(root).display()
        )
    })
}

pub async fn serve(args: crate::cli::ServeArgs) -> Result<()> {
    let root = PathBuf::from(
        args.runtime
            .as_deref()
            .context("--follow needs --runtime")?,
    );
    let writer = args
        .follow
        .clone()
        .context("--runtime needs --follow <writer-url>")?;
    if !root.join(crate::runtime::MOUNTS_FILE).is_file() {
        bail!(
            "'{}' is not a runtime directory (no {})",
            root.display(),
            crate::runtime::MOUNTS_FILE
        );
    }
    let cors = crate::serve::cors_layer(&args.cors)?;
    let token = writer_token(&root)?;
    let follower = Follower::start(root, writer, token);
    tracing::info!(
        listen = %args.listen,
        writer = %follower.writer,
        "serving read-only, following the writer's hot-state feed (RFC-0073 FR1)"
    );
    crate::serve::bind_and_serve(&args.listen, follower.service(), cors).await
}

/// What the writer serves a route under, as its `mount` line says.
#[derive(Clone, PartialEq)]
struct Spec {
    dataset: String,
    nid: Option<String>,
    surface: crate::allowlist::Surface,
    tenant: Option<String>,
    share: Option<crate::runtime::TenantShare>,
    limits: Option<crate::feed::WireLimits>,
    chain: Option<String>,
    isolation: Option<String>,
}

struct Route {
    spec: Spec,
    served: Option<std::result::Result<SharedNest, String>>,
}

struct Link {
    connected: bool,
    since: Instant,
    version: Option<String>,
    /// The writer's name for this connection, under which what is applied is reported.
    conn: Option<u64>,
}

/// One dataset as a follower holds it: the hot copy, and what it derives from the stored rows.
#[derive(Clone)]
struct Data {
    store: Arc<MemStore>,
    /// Where each delta's copy and derived state are published together for its routes (#2467).
    snapshots: crate::serve::Snapshots,
    /// Why there is none, when it could not be built; those routes are then the writer's.
    derived: std::result::Result<Arc<crate::indexer::FollowerDerived>, Refused>,
    /// Why the derived state is being rebuilt, while it is: it is not served until it is whole.
    building: Option<String>,
}

/// Derived state that could not be built, with its own empty entity list so a swap can tell it apart.
#[derive(Clone)]
struct Refused {
    why: String,
    entities: Arc<Vec<crate::entity_view::EntityView>>,
    /// A segment the copy names was missing: not the writer's to answer, but this follower's to
    /// rebuild once it has a fresh snapshot.
    gone: bool,
}

/// Build `dir`'s derived state over `store`, reading sealed history through `store`'s catalogue,
/// or say why not.
fn derive(
    dir: &Path,
    store: &Arc<MemStore>,
) -> std::result::Result<Arc<crate::indexer::FollowerDerived>, Refused> {
    let built = crate::seal::within(dir, store.clone(), || {
        crate::config::Config::load(dir).and_then(|mut config| {
            config.read_only_role = true;
            crate::indexer::FollowerDerived::build(dir, &config, store)
        })
    });
    built.map(Arc::new).map_err(|e| {
        let gone = e.chain().any(|c| c.is::<crate::seal::SegmentGone>());
        tracing::error!("{}: derived state not built: {e:#}", dir.display());
        Refused {
            why: format!("its derived state could not be built: {e:#}"),
            entities: Arc::default(),
            gone,
        }
    })
}

/// Why derived state is not served while this follower takes a fresh snapshot, if that is why.
fn rebuilding(
    derived: &std::result::Result<Arc<crate::indexer::FollowerDerived>, Refused>,
) -> Option<String> {
    derived
        .as_ref()
        .err()
        .filter(|r| r.gone)
        .map(|r| r.why.clone())
}

/// Point `state` at `data`'s derived views, or at inert ones when it has none.
fn with_derived(state: &mut AppState, data: &Data) {
    match &data.derived {
        Ok(d) => {
            state.balances = d.balances.clone();
            state.exposure = d.exposure.clone();
            state.velocity = d.velocity.clone();
            state.entities = d.entities.clone();
        }
        Err(r) => {
            state.balances = crate::views::BalanceView::inert();
            state.exposure = crate::exposure::ExposureView::inert();
            state.velocity = crate::velocity::VelocityView::inert();
            state.entities = r.entities.clone();
        }
    }
    state.snapshots = Some(data.snapshots.clone());
}

impl Data {
    /// What versions of the derived state requests still read hold beside its latest, in bytes.
    fn derived_retained(&self) -> u64 {
        let Ok(d) = &self.derived else {
            return 0;
        };
        d.balances.retained_bytes()
            + d.exposure.retained_bytes()
            + d.velocity.retained_bytes()
            + d.entities.iter().map(|e| e.retained_bytes()).sum::<u64>()
    }

    /// Publish the copy and derived state as they stand as the snapshot every route of this dataset
    /// reads (#2467). The derived state has folded everything it was fed.
    fn publish(&self) -> Result<()> {
        let store: Arc<dyn HotStore> = self.store.clone();
        let snapshot = match &self.derived {
            Ok(d) => crate::serve::Snapshot::take(
                &store,
                &d.balances,
                &d.exposure,
                &d.velocity,
                &d.entities,
            )?,
            Err(r) => crate::serve::Snapshot::take(
                &store,
                &crate::views::BalanceView::inert(),
                &crate::exposure::ExposureView::inert(),
                &crate::velocity::VelocityView::inert(),
                &r.entities,
            )?,
        };
        self.snapshots.store(snapshot.map(Arc::new));
        Ok(())
    }

    fn entities(&self) -> Arc<Vec<crate::entity_view::EntityView>> {
        match &self.derived {
            Ok(d) => d.entities.clone(),
            Err(r) => r.entities.clone(),
        }
    }
}

struct Inner {
    routes: BTreeMap<String, Route>,
    datasets: HashMap<String, Data>,
    /// The writer's `(epoch, generation)` per dataset at its last heartbeat.
    positions: HashMap<String, (u64, u64)>,
    link: Link,
    /// Whether a connection has delivered the writer's whole mount set, so that a route not in it
    /// does not exist rather than not having arrived yet.
    mounts_known: bool,
    /// The per-cursor budget, `[runtime] max_rss_mb`, as the writer's admission reads it.
    max_rss_mb: u64,
    /// What admission charges each dataset's nest besides its hot rows, in bytes.
    estimates: HashMap<String, u64>,
    /// Datasets whose hot copy would not fit their cursor's budget, with why.
    over_budget: HashMap<String, String>,
    /// Datasets whose snapshot ran past its byte limit, which the writer is asked not to send
    /// until `max_rss_mb` changes or the mount goes.
    excluded: BTreeSet<String>,
    /// Copies and derived state replaced or released but possibly still referenced by a request in
    /// flight. Each counts against its cursor's budget until its last reference drops.
    retiring: Vec<Retiring>,
    /// Datasets refused while retiring copies held part of their cursor's budget, with that chain
    /// and what those copies came to. Each is asked for again once that falls.
    awaiting_room: HashMap<String, (Option<String>, u64)>,
    /// Times a held copy was released so that a new snapshot of it could load under the full budget.
    reloads: u64,
    /// Copies and derived states ever handed to `retiring`.
    retired: u64,
    /// The most any cursor here has been charged, in bytes.
    peak: u64,
}

impl Inner {
    fn chain_of(&self, dataset: &str) -> Option<Option<String>> {
        self.routes
            .values()
            .find(|r| r.spec.dataset == dataset)
            .map(|r| r.spec.chain.clone())
    }

    /// What `chain`'s cursor is charged now: every held copy, its nest's estimate, and every
    /// retiring copy still referenced.
    fn charged(&self, chain: &Option<String>) -> u64 {
        let held: u64 = self
            .routes
            .values()
            .filter(|r| &r.spec.chain == chain)
            .map(|r| r.spec.dataset.as_str())
            .collect::<BTreeSet<_>>()
            .into_iter()
            .filter_map(|d| self.held(d))
            .sum();
        held + self.retiring_on(chain)
    }

    fn retiring_on(&self, chain: &Option<String>) -> u64 {
        self.retiring
            .iter()
            .filter(|r| &r.chain == chain)
            .map(Retiring::bytes)
            .sum()
    }

    /// What `dataset`'s held copy and derived state are charged, which a replacement is read beside.
    fn held(&self, dataset: &str) -> Option<u64> {
        let data = self.datasets.get(dataset)?;
        Some(
            data.store.bytes()
                + data.derived_retained()
                + self.estimates.get(dataset).copied().unwrap_or(0),
        )
    }

    /// Hand a copy, its derived state, or both, no longer served to the retiring list, each charged
    /// until it is dropped. Returns it, to wait on.
    fn retire(
        &mut self,
        dataset: &str,
        store: Option<&Arc<MemStore>>,
        derived: Option<&Derived>,
    ) -> Retiring {
        let chain = self.chain_of(dataset).unwrap_or_default();
        self.retire_on(chain, dataset, store, derived)
    }

    /// [`Inner::retire`] onto `chain`, for a dataset whose routes are already gone.
    fn retire_on(
        &mut self,
        chain: Option<String>,
        dataset: &str,
        store: Option<&Arc<MemStore>>,
        derived: Option<&Derived>,
    ) -> Retiring {
        self.retiring.retain(Retiring::live);
        let retiring = Retiring {
            chain,
            store: store.map(|s| s.copy()),
            derived: derived.map(|d| Arc::downgrade(&d.entities)),
            estimate: self.estimates.get(dataset).copied().unwrap_or(0),
        };
        self.retiring.push(retiring.clone());
        self.retired += 1;
        retiring
    }

    fn note_peak(&mut self, dataset: &str) {
        let chain = self.chain_of(dataset).unwrap_or_default();
        self.peak = self.peak.max(self.charged(&chain));
    }
}

type Derived = Arc<crate::indexer::FollowerDerived>;

/// A copy or derived state that is no longer served, charged to `chain` while referenced.
#[derive(Clone)]
struct Retiring {
    chain: Option<String>,
    store: Option<std::sync::Weak<crate::feed::MemCopy>>,
    /// Every serving state built from derived views shares their entity list, so this lives exactly
    /// as long as the views do.
    derived: Option<std::sync::Weak<Vec<crate::entity_view::EntityView>>>,
    /// The derived state's charge: its nest's admission estimate.
    estimate: u64,
}

impl Retiring {
    fn bytes(&self) -> u64 {
        let store = self.store.as_ref().and_then(|s| s.upgrade());
        let derived = self.derived.as_ref().is_some_and(|d| d.strong_count() > 0);
        store.map_or(0, |s| s.bytes()) + if derived { self.estimate } else { 0 }
    }

    fn live(&self) -> bool {
        self.store.as_ref().is_some_and(|s| s.strong_count() > 0)
            || self.derived.as_ref().is_some_and(|d| d.strong_count() > 0)
    }

    /// Wait, at most `RELEASE_WAIT`, for requests still on it to finish. Whatever is still
    /// referenced when the wait ends stays charged.
    async fn released(&self) {
        let deadline = Instant::now() + RELEASE_WAIT;
        while self.live() && Instant::now() < deadline {
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    }
}

/// How long a released copy is waited for, while requests that started on it finish, before its
/// replacement is read under whatever budget is then left.
const RELEASE_WAIT: Duration = Duration::from_secs(10);

const MIB: u64 = 1024 * 1024;

/// The runtime's per-cursor budget in MiB, read from its `mounts.toml` as the writer reads it.
fn max_rss_mb(root: &Path) -> u64 {
    crate::runtime::MountTable::load(root)
        .ok()
        .and_then(|t| t.runtime.max_rss_mb)
        .unwrap_or(crate::runtime::DEFAULT_MAX_RSS_MB)
}

/// What the writer's admission charges `dir`'s nest besides its store, in bytes.
fn nest_estimate(dir: &Path) -> u64 {
    let Ok(config) = crate::config::Config::load(dir) else {
        return 0;
    };
    let (count, rows) = crate::runtime::declared_entities(dir);
    let labels = !crate::labels::load(dir).is_empty();
    crate::runtime::estimate_nest_rss_mb(&config, labels, count, rows) * MIB
}

fn budget_reason(dataset: &str, held: u64, cap: u64, max_rss_mb: u64) -> String {
    format!(
        "{dataset}'s hot rows come to {held} bytes, over the {cap} its cursor has left of \
         max_rss_mb {max_rss_mb} here; the writer still serves it"
    )
}

/// The feed's body as a blocking reader over the chunks the connection delivers, so a line is parsed
/// as it arrives and its text is never held whole.
struct ChunkReader {
    rx: tokio::sync::mpsc::Receiver<bytes::Bytes>,
    cur: bytes::Bytes,
}

impl std::io::Read for ChunkReader {
    fn read(&mut self, buf: &mut [u8]) -> std::io::Result<usize> {
        while self.cur.is_empty() {
            match self.rx.blocking_recv() {
                Some(chunk) => self.cur = chunk,
                None => return Ok(0),
            }
        }
        let n = buf.len().min(self.cur.len());
        buf[..n].copy_from_slice(&self.cur.split_to(n));
        Ok(n)
    }
}

/// A running follower: its state, and the composition it serves.
pub struct Follower {
    root: PathBuf,
    writer: String,
    token: String,
    live: LiveRuntime,
    inner: Mutex<Inner>,
    /// One analytical gate per chain, as the writer has one per cursor.
    gates: Mutex<HashMap<String, crate::serve::SqlGate>>,
    /// A test's hold on applying deltas, so a copy stays at a generation while the writer moves on.
    paused: tokio::sync::watch::Sender<bool>,
    /// Feed connections made, each of which takes fresh snapshots.
    connects: std::sync::atomic::AtomicU64,
}

impl Follower {
    /// Start following `writer` for the runtime at `root`.
    pub fn start(root: PathBuf, writer: String, token: String) -> Arc<Follower> {
        Follower::start_with(root, writer, token, false)
    }

    /// [`Follower::start`], applying nothing the feed sends until [`Follower::pause`] lets it go.
    #[doc(hidden)]
    pub fn start_paused(root: PathBuf, writer: String, token: String) -> Arc<Follower> {
        Follower::start_with(root, writer, token, true)
    }

    fn start_with(root: PathBuf, writer: String, token: String, paused: bool) -> Arc<Follower> {
        let follower = Arc::new(Follower {
            writer: writer.trim_end_matches('/').to_string(),
            token,
            live: LiveRuntime::new(axum::Router::new()),
            inner: Mutex::new(Inner {
                routes: BTreeMap::new(),
                datasets: HashMap::new(),
                positions: HashMap::new(),
                mounts_known: false,
                max_rss_mb: max_rss_mb(&root),
                estimates: HashMap::new(),
                over_budget: HashMap::new(),
                excluded: BTreeSet::new(),
                retiring: Vec::new(),
                awaiting_room: HashMap::new(),
                reloads: 0,
                retired: 0,
                peak: 0,
                link: Link {
                    connected: false,
                    since: Instant::now(),
                    version: None,
                    conn: None,
                },
            }),
            root,
            gates: Mutex::default(),
            paused: tokio::sync::watch::Sender::new(paused),
            connects: Default::default(),
        });
        follower.recompose();
        tokio::spawn(follower.clone().run());
        follower
    }

    /// What to serve: resolves the current composition per request.
    pub fn service(&self) -> axum::Router {
        self.live.service()
    }

    fn lock(&self) -> std::sync::MutexGuard<'_, Inner> {
        self.inner.lock().unwrap_or_else(|e| e.into_inner())
    }

    async fn run(self: Arc<Self>) {
        let client = reqwest::Client::builder()
            .connect_timeout(Duration::from_secs(5))
            .build()
            .expect("an HTTP client with no custom TLS builds");
        let mut backoff = BACKOFF_FIRST;
        loop {
            match self.follow_once(&client).await {
                Ok(()) => tracing::warn!("the writer's feed ended; reconnecting"),
                Err(e) => tracing::warn!("following the writer: {e:#}"),
            }
            {
                let mut inner = self.lock();
                if inner.link.connected {
                    inner.link.connected = false;
                    inner.link.since = Instant::now();
                    backoff = BACKOFF_FIRST;
                }
                // The gone writer's positions say nothing about the next one: no lag until it reports.
                inner.positions.clear();
                inner.link.conn = None;
            }
            tokio::time::sleep(backoff).await;
            backoff = (backoff * 2).min(BACKOFF_MAX);
        }
    }

    /// Tell the writer, every [`REPORT_EVERY`], what each copy has applied, so it keeps the
    /// segments a copy may still name until it has moved on. Best effort: unreported, the writer
    /// keeps them until its grace and most run out, never less.
    async fn report_applied(self: Arc<Self>, client: reqwest::Client) {
        let mut every = tokio::time::interval(REPORT_EVERY);
        loop {
            every.tick().await;
            let (conn, applied) = {
                let inner = self.lock();
                let applied: BTreeMap<String, (u64, u64)> = inner
                    .datasets
                    .iter()
                    .map(|(d, copy)| (d.clone(), copy.store.applied()))
                    .collect();
                (inner.link.conn, applied)
            };
            let Some(conn) = conn else {
                continue;
            };
            let sent = client
                .post(format!("{}/_admin/feed/applied", self.writer))
                .query(&[("conn", conn)])
                .bearer_auth(&self.token)
                .timeout(REPORT_EVERY)
                .json(&applied)
                .send()
                .await;
            if let Err(e) = sent {
                tracing::debug!("reporting what is applied: {}", e.without_url());
            }
        }
    }

    async fn follow_once(self: &Arc<Self>, client: &reqwest::Client) -> Result<()> {
        use futures::StreamExt;
        let excluded = self
            .lock()
            .excluded
            .iter()
            .cloned()
            .collect::<Vec<_>>()
            .join(",");
        let resp = client
            .get(format!("{}/_admin/feed", self.writer))
            .query(&[("exclude", excluded)])
            .bearer_auth(&self.token)
            .send()
            .await
            .map_err(reqwest::Error::without_url)
            .context("connecting to the writer")?;
        if !resp.status().is_success() {
            bail!("the writer refused the feed: {}", resp.status());
        }
        {
            let mut inner = self.lock();
            inner.link.connected = true;
            inner.link.since = Instant::now();
        }
        self.connects
            .fetch_add(1, std::sync::atomic::Ordering::SeqCst);
        let (chunk_tx, chunk_rx) = tokio::sync::mpsc::channel::<bytes::Bytes>(16);
        let (line_tx, mut line_rx) = tokio::sync::mpsc::channel::<Line>(16);
        let mut body = resp.bytes_stream();
        let pump = tokio::spawn(async move {
            loop {
                let chunk = match tokio::time::timeout(SILENCE, body.next()).await {
                    Err(_) => bail!("the writer has been silent for {}s", SILENCE.as_secs()),
                    Ok(None) => return Ok(()),
                    Ok(Some(chunk)) => chunk
                        .map_err(reqwest::Error::without_url)
                        .context("reading the feed")?,
                };
                if chunk_tx.send(chunk).await.is_err() {
                    return Ok(());
                }
            }
        });
        let weak = Arc::downgrade(self);
        let applied = Arc::new(Applied::default());
        let (caps, sends) = (applied.clone(), applied.clone());
        let parser = tokio::task::spawn_blocking(move || {
            let reader = std::io::BufReader::new(ChunkReader {
                rx: chunk_rx,
                cur: bytes::Bytes::new(),
            });
            let cap_for: crate::feed::CapFor = Box::new(move |dataset, kind| {
                // Read ahead of the handler, a snapshot would find no `mount` for its dataset yet,
                // and so no cursor to charge it to and no cap at all.
                if !caps.caught_up() {
                    return Some(0);
                }
                let f = weak.upgrade()?;
                let cap = f.cap_for(dataset).unwrap_or(0);
                let inner = f.lock();
                Some(match kind {
                    // A replacement and its derived state are built beside the copy and derived
                    // state they replace, which are served until the swap and counted until their
                    // last request finishes. `cap_for` has already reserved the replacement's own
                    // derived estimate, so all four must fit or the old ones are released first.
                    crate::feed::Kind::Snapshot => {
                        cap.saturating_sub(inner.held(dataset).unwrap_or(0))
                    }
                    // A dataset not held has nothing left to grow into: its deltas are skipped.
                    crate::feed::Kind::Delta => cap.saturating_sub(
                        inner.datasets.get(dataset).map_or(cap, |s| s.store.bytes()),
                    ),
                })
            });
            crate::feed::read_lines(reader, cap_for, |line| {
                sends.sent();
                line_tx.blocking_send(line).is_ok()
            })
        });
        let reporter = tokio::spawn(self.clone().report_applied(client.clone()));
        let _reporter = AbortOnDrop(reporter);
        // Datasets snapshotted on this connection: a delta for any other is a gap.
        let mut fresh: BTreeSet<String> = BTreeSet::new();
        let handled = async {
            while let Some(line) = line_rx.recv().await {
                self.handle(line, &mut fresh).await?;
                applied.handled();
            }
            Ok::<_, anyhow::Error>(())
        }
        .await;
        applied.done();
        drop(line_rx);
        pump.abort();
        handled?;
        let pumped = pump.await.unwrap_or(Ok(()));
        if let Err(e) = parser.await.context("the feed parser stopped")? {
            if let Some(over) = e.downcast_ref::<crate::feed::LineOverBudget>() {
                let dataset = over.dataset.clone();
                let held = self.lock().datasets.contains_key(&dataset);
                if over.kind == crate::feed::Kind::Snapshot && held {
                    // It did not fit beside the copy it replaces: drop that, then ask again.
                    self.release_for_reload(&dataset).await;
                    return Err(e);
                }
                // Not read to the end, so the stream cannot go on: reconnect without it.
                let max = {
                    let mut inner = self.lock();
                    inner.excluded.insert(dataset.clone());
                    inner.max_rss_mb
                };
                self.refuse(
                    &dataset,
                    format!(
                        "{over}, set by what its cursor has left of max_rss_mb {max} here; the \
                         writer still serves it, and this follower stops asking for it"
                    ),
                );
            }
            return Err(e);
        }
        pumped
    }

    /// The most `dataset`'s snapshot may hold: its cursor's budget, less what the other datasets held
    /// on that cursor take and what admission charges this one's nest. `None` before its mount is
    /// known.
    fn cap_for(&self, dataset: &str) -> Option<u64> {
        let inner = self.lock();
        let chain = inner
            .routes
            .values()
            .find(|r| r.spec.dataset == dataset)?
            .spec
            .chain
            .clone();
        let others: u64 = inner
            .routes
            .values()
            .filter(|r| r.spec.chain == chain && r.spec.dataset != dataset)
            .map(|r| r.spec.dataset.as_str())
            .collect::<BTreeSet<_>>()
            .into_iter()
            .filter_map(|d| {
                let held = inner.datasets.get(d)?;
                Some(
                    held.store.bytes()
                        + held.derived_retained()
                        + inner.estimates.get(d).copied().unwrap_or(0),
                )
            })
            .sum();
        let own = inner.estimates.get(dataset).copied().unwrap_or(0)
            + inner
                .datasets
                .get(dataset)
                .map_or(0, Data::derived_retained);
        let retiring = inner.retiring_on(&chain);
        Some(
            inner
                .max_rss_mb
                .saturating_mul(MIB)
                .saturating_sub(others + own + retiring),
        )
    }

    /// Stop serving `dataset`'s copy so its replacement can load under the full budget: its routes
    /// answer `catching_up`, and this waits, bounded, for requests still on the old copy to finish.
    /// Whatever is still referenced when the wait ends stays charged.
    async fn release_for_reload(self: &Arc<Self>, dataset: &str) {
        let released = {
            let mut inner = self.lock();
            let Some(old) = inner.datasets.remove(dataset) else {
                return;
            };
            inner.reloads += 1;
            for r in inner.routes.values_mut() {
                if r.spec.dataset == dataset {
                    r.served = None;
                }
            }
            inner.retire(dataset, Some(&old.store), old.derived.as_ref().ok())
        };
        tracing::info!(
            "{dataset}: its new snapshot does not fit beside the copy it replaces; releasing that \
             copy first, its routes catching up meanwhile"
        );
        self.recompose();
        released.released().await;
    }

    /// The most any cursor has been charged here, in bytes, and how many copies were released so
    /// that their replacement could load.
    pub fn charges(&self) -> (u64, u64) {
        let inner = self.lock();
        (inner.peak, inner.reloads)
    }

    /// How many copies and derived states have stopped being served, and what those still referenced
    /// are charged, in bytes.
    pub fn retiring(&self) -> (u64, u64) {
        let inner = self.lock();
        let bytes = inner.retiring.iter().map(Retiring::bytes).sum();
        (inner.retired, bytes)
    }

    /// How many feed connections this follower has made, each taking fresh snapshots.
    #[doc(hidden)]
    pub fn connects(&self) -> u64 {
        self.connects.load(std::sync::atomic::Ordering::SeqCst)
    }

    /// Stop applying what the feed sends, or go on, as a follower that has fallen behind would.
    #[doc(hidden)]
    pub fn pause(&self, paused: bool) {
        self.paused.send_replace(paused);
    }

    /// `dataset`'s copy, held as a request in flight would hold it.
    #[doc(hidden)]
    pub fn hold(&self, dataset: &str) -> Option<Arc<MemStore>> {
        self.lock().datasets.get(dataset).map(|d| d.store.clone())
    }

    /// Stop holding `dataset`, and serve its routes the reason.
    fn refuse(self: &Arc<Self>, dataset: &str, why: String) {
        tracing::warn!("{dataset}: {why}");
        {
            let mut inner = self.lock();
            // Refused while retiring copies held part of the budget: asked for again once they go.
            let chain = inner.chain_of(dataset).unwrap_or_default();
            let retiring = inner.retiring_on(&chain);
            if retiring > 0 {
                inner
                    .awaiting_room
                    .insert(dataset.to_string(), (chain, retiring));
            } else {
                inner.awaiting_room.remove(dataset);
            }
            if let Some(old) = inner.datasets.remove(dataset) {
                inner.retire(dataset, Some(&old.store), old.derived.as_ref().ok());
            }
            inner.over_budget.insert(dataset.to_string(), why);
        }
        self.recompose();
    }

    async fn handle(self: &Arc<Self>, line: Line, fresh: &mut BTreeSet<String>) -> Result<()> {
        let _ = self.paused.subscribe().wait_for(|p| !*p).await;
        match line {
            Line::Hello {
                version,
                feed,
                conn,
            } => {
                if feed != crate::feed::FEED_VERSION {
                    bail!(
                        "the writer ({version}) speaks feed version {feed} and this follower ({}) \
                         speaks {}: upgrade them to one release",
                        env!("CARGO_PKG_VERSION"),
                        crate::feed::FEED_VERSION
                    );
                }
                let mut inner = self.lock();
                inner.link.version = Some(version);
                inner.link.conn = conn;
            }
            Line::Mount {
                route,
                dataset,
                nid,
                sql,
                queries,
                tenant,
                share,
                limits,
                chain,
                isolation,
            } => {
                let estimate = nest_estimate(&self.root.join(&dataset));
                let spec = Spec {
                    dataset,
                    nid,
                    surface: crate::allowlist::Surface {
                        access: sql,
                        queries,
                    },
                    tenant,
                    share,
                    limits,
                    chain,
                    isolation,
                };
                {
                    let mut inner = self.lock();
                    inner.estimates.insert(spec.dataset.clone(), estimate);
                    // A reconnect repeats every mount; only a changed one is rebuilt.
                    if inner.routes.get(&route).is_some_and(|r| r.spec == spec) {
                        return Ok(());
                    }
                    inner.routes.insert(route, Route { spec, served: None });
                }
                self.realise();
                self.recompose();
            }
            Line::Mounted => {
                {
                    let mut inner = self.lock();
                    inner.mounts_known = true;
                    let max = max_rss_mb(&self.root);
                    // A new budget may fit what an old one could not: ask again on the next connection.
                    if max != inner.max_rss_mb {
                        inner.excluded.clear();
                    }
                    inner.max_rss_mb = max;
                }
                self.recompose();
            }
            Line::Unmount { route } => {
                {
                    let mut inner = self.lock();
                    if let Some(gone) = inner.routes.remove(&route) {
                        let dataset = gone.spec.dataset;
                        if !inner.routes.values().any(|r| r.spec.dataset == dataset) {
                            if let Some(old) = inner.datasets.remove(&dataset) {
                                let derived = old.derived.as_ref().ok();
                                inner.retire_on(
                                    gone.spec.chain,
                                    &dataset,
                                    Some(&old.store),
                                    derived,
                                );
                            }
                            inner.positions.remove(&dataset);
                            inner.excluded.remove(&dataset);
                            inner.awaiting_room.remove(&dataset);
                        }
                    }
                }
                self.recompose();
            }
            Line::Snapshot { dataset, snapshot } => {
                let dataset = dataset.0;
                fresh.insert(dataset.clone());
                // Read under its cap, so a snapshot over budget arrives holding no rows.
                if let Some(held) = snapshot.over {
                    if self.lock().datasets.contains_key(&dataset) {
                        // It did not fit beside the copy it replaces: drop that, then ask again.
                        self.release_for_reload(&dataset).await;
                        bail!("{dataset}: reloading its snapshot with the old copy released");
                    }
                    let cap = self.cap_for(&dataset).unwrap_or(0);
                    let max = self.lock().max_rss_mb;
                    self.refuse(&dataset, budget_reason(&dataset, held, cap, max));
                    return Ok(());
                }
                let dir = self.root.join(&dataset);
                let data = tokio::task::spawn_blocking(move || {
                    let store = Arc::new(MemStore::from_snapshot(&dir, snapshot));
                    let derived = derive(&dir, &store);
                    let data = Data {
                        building: rebuilding(&derived),
                        store,
                        snapshots: Default::default(),
                        derived,
                    };
                    data.publish().map(|()| data)
                })
                .await
                .context("building a snapshot's store")??;
                let lifted = {
                    let mut inner = self.lock();
                    if let Some(old) = inner.datasets.insert(dataset.clone(), data) {
                        inner.retire(&dataset, Some(&old.store), old.derived.as_ref().ok());
                    }
                    inner.note_peak(&dataset);
                    inner.awaiting_room.remove(&dataset);
                    inner.over_budget.remove(&dataset).is_some()
                };
                self.realise();
                if lifted {
                    self.recompose();
                }
            }
            Line::Delta { dataset, delta } => {
                let dataset = dataset.0;
                let (data, refused) = {
                    let inner = self.lock();
                    (
                        inner.datasets.get(&dataset).cloned(),
                        inner.over_budget.contains_key(&dataset),
                    )
                };
                if refused {
                    return Ok(());
                }
                let data = match (fresh.contains(&dataset), data) {
                    (true, Some(data)) => data,
                    _ => bail!("{dataset}: a delta before its snapshot; resnapshotting"),
                };
                // Charged before it is applied, at its peak, so the copy never holds more than its
                // budget even part-way through a delta.
                let peak = data.store.peak_with(&delta);
                if let Some(cap) = self.cap_for(&dataset).filter(|cap| peak > *cap) {
                    let max = {
                        let mut inner = self.lock();
                        inner.excluded.insert(dataset.clone());
                        inner.max_rss_mb
                    };
                    self.refuse(
                        &dataset,
                        format!(
                            "{}; refused before it was applied",
                            budget_reason(&dataset, peak, cap, max)
                        ),
                    );
                    return Ok(());
                }
                // The old state keeps serving while a new connection takes fresh snapshots.
                let changes = data
                    .store
                    .apply(&delta)
                    .with_context(|| format!("{dataset}: resnapshotting"))?;
                self.lock().note_peak(&dataset);
                if let Some(delay) = crate::indexer::feed_delay(&self.root.join(&dataset)) {
                    tokio::time::sleep(delay).await;
                }
                let torn = if changes.sealed_unseen {
                    // Rows sealed straight to segments never crossed the feed: derive again.
                    Some("rows were sealed that never passed through hot".to_string())
                } else if let (Ok(derived), Some(through)) = (&data.derived, changes.through) {
                    let (derived, store) = (derived.clone(), data.store.clone());
                    let dir = self.root.join(&dataset);
                    // Fed and then flushed, so every view has published the delta before it is.
                    tokio::task::spawn_blocking(move || {
                        crate::seal::within(&dir, store.clone(), || {
                            derived.apply(
                                store.as_ref(),
                                &changes.removed,
                                &changes.added,
                                through,
                            )?;
                            derived.flush();
                            Ok::<_, anyhow::Error>(())
                        })
                    })
                    .await
                    .context("feeding derived state")?
                    .err()
                    .map(|e| format!("feeding it failed part-way: {e:#}"))
                } else {
                    None
                };
                if let Some(why) = &torn {
                    // Marked before the delta is published, so no request reads the torn state.
                    if let Some(d) = self.lock().datasets.get_mut(&dataset) {
                        d.building = Some(why.clone());
                    }
                }
                data.publish()
                    .with_context(|| format!("{dataset}: publishing a delta"))?;
                if let Some(why) = torn {
                    let store = data.store.clone();
                    drop(data);
                    self.rederive(&dataset, store, why).await?;
                }
            }
            Line::Heartbeat { positions } => {
                let roomier = {
                    let mut inner = self.lock();
                    inner.positions = positions.into_iter().collect();
                    let roomier: Vec<String> = inner
                        .awaiting_room
                        .iter()
                        .filter(|(_, (chain, was))| inner.retiring_on(chain) < *was)
                        .map(|(d, _)| d.clone())
                        .collect();
                    for d in &roomier {
                        inner.awaiting_room.remove(d);
                        inner.excluded.remove(d);
                        inner.over_budget.remove(d);
                    }
                    roomier
                };
                if !roomier.is_empty() {
                    self.recompose();
                    // The feed sends a snapshot only on connecting, so ask again by reconnecting.
                    bail!(
                        "room came back for {}; asking for it again",
                        roomier.join(", ")
                    );
                }
                let gone: Vec<String> = self
                    .lock()
                    .datasets
                    .iter()
                    .filter(|(_, copy)| copy.store.take_missing())
                    .map(|(d, _)| d.clone())
                    .collect();
                if !gone.is_empty() {
                    bail!(
                        "{}: a segment the copy names is gone; taking fresh snapshots",
                        gone.join(", ")
                    );
                }
            }
        }
        Ok(())
    }

    /// Rebuild `dataset`'s derived state from its store and segments, its derived routes answering
    /// `catching_up` meanwhile. Nothing else is applied until it is done, so it rebuilds a fixed state.
    /// The torn state is never served again, so it is released before its replacement is built.
    async fn rederive(
        self: &Arc<Self>,
        dataset: &str,
        store: Arc<MemStore>,
        why: String,
    ) -> Result<()> {
        tracing::warn!("{dataset}: rebuilding its derived state: {why}");
        let released = {
            let mut inner = self.lock();
            let Some(d) = inner.datasets.get_mut(dataset) else {
                return Ok(());
            };
            let why = format!("{why}; rebuilding it from the sealed history");
            d.building = Some(why.clone());
            let old = std::mem::replace(
                &mut d.derived,
                Err(Refused {
                    why,
                    entities: Arc::default(),
                    gone: false,
                }),
            );
            inner.retire(dataset, None, old.as_ref().ok())
        };
        self.realise();
        released.released().await;
        let dir = self.root.join(dataset);
        let derived = tokio::task::spawn_blocking(move || derive(&dir, &store))
            .await
            .context("rebuilding derived state")?;
        if let Some(d) = self.lock().datasets.get_mut(dataset) {
            d.building = rebuilding(&derived);
            d.derived = derived;
            d.publish()
                .with_context(|| format!("{dataset}: publishing rebuilt derived state"))?;
        }
        self.realise();
        Ok(())
    }

    /// Serve every route whose dataset has a snapshot, and point existing ones at the current store.
    fn realise(self: &Arc<Self>) {
        let mut changed = false;
        {
            let mut inner = self.lock();
            let Inner {
                routes, datasets, ..
            } = &mut *inner;
            for (route, r) in routes.iter_mut() {
                let Some(data) = datasets.get(&r.spec.dataset).cloned() else {
                    continue;
                };
                match &r.served {
                    Some(Ok(shared)) => {
                        let current = shared.current();
                        let store: Arc<dyn HotStore> = data.store.clone();
                        if !Arc::ptr_eq(&current.store, &store)
                            || !Arc::ptr_eq(&current.entities, &data.entities())
                        {
                            let mut next = AppState {
                                store,
                                ..(*current).clone()
                            };
                            with_derived(&mut next, &data);
                            shared.swap(next);
                        }
                    }
                    Some(Err(_)) => {}
                    None => {
                        r.served = Some(
                            self.build(route, &r.spec, &data)
                                .map(SharedNest::new)
                                .map_err(|e| format!("{e:#}")),
                        );
                        if let Some(Err(why)) = &r.served {
                            tracing::error!("{route}: cannot be served: {why}");
                        }
                        changed = true;
                    }
                }
            }
        }
        if changed {
            self.recompose();
        }
    }

    fn gate(&self, chain: &str) -> crate::serve::SqlGate {
        self.gates
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .entry(chain.to_string())
            .or_insert_with(crate::serve::new_sql_gate)
            .clone()
    }

    /// A route's serving state over `store`: what `build_nest` assembles for a nest it does not
    /// index, with no circuit, no ingest and no admin surface.
    fn build(&self, route: &str, r: &Spec, data: &Data) -> Result<AppState> {
        let store: Arc<dyn HotStore> = data.store.clone();
        let dir = self.root.join(&r.dataset);
        let mut config = crate::config::Config::load(&dir)
            .with_context(|| format!("no nest at '{}'", dir.display()))?;
        config.read_only_role = true;
        let registry = crate::registry::from_nest(&dir, &config)?;
        let identity = hex::encode(crate::project::decode_identity(&dir, &config, &registry)?);
        let seal_span = crate::chains::lookup(&config.nest.chain)
            .map(|c| c.seal_span)
            .unwrap_or(crate::chains::DEFAULT_SEAL_SPAN);
        let health = Arc::new(crate::health::RuntimeHealth::new());
        health.register(route, &config.nest.chain);
        let nest_info = serde_json::json!({
            "name": config.nest.name,
            "chain": config.nest.chain,
            "chain_id": config.nest.chain_id,
            "nid": r.nid,
            "registry_hash": format!("0x{identity}"),
            "table_count": registry.tables().len(),
            "contracts": config.contracts.iter()
                .map(|c| serde_json::json!({ "alias": c.alias, "address": c.address }))
                .collect::<Vec<_>>(),
            "templates": config.templates,
            "factories": config.factories,
            "follower": true,
        });
        let sql_gate = self.gate(&config.nest.chain);
        let sql_response_bytes = crate::serve::ResponseBytes::default();
        let tenant = r.tenant.as_deref().map(|t| {
            Arc::new(crate::fair_share::Seat::new(
                t,
                &dir,
                &sql_gate,
                &sql_response_bytes.0,
                r.share.unwrap_or_default(),
            ))
        });
        let process = crate::limits::Limits::process();
        let mut state = AppState {
            freshness: config.freshness,
            store,
            address: config.contracts.first().map(|c| c.address.clone()),
            chain: config.nest.chain.clone(),
            dir,
            balances: crate::views::BalanceView::inert(),
            exposure: crate::exposure::ExposureView::start(false)?,
            velocity: crate::velocity::VelocityView::start(false)?,
            #[cfg(feature = "folds")]
            folds: None,
            entities: Arc::new(Vec::new()),
            threshold: config.flags.threshold_amount(),
            velocity_threshold: config.flags.velocity().map(|(amt, _)| amt),
            tables: Arc::new(crate::indexer::full_schema(&registry, &config)),
            sql_gate,
            tenant,
            sql_queued: Default::default(),
            memo_gate: Default::default(),
            sql_response_bytes,
            limits: r.limits.map_or(process, |l| l.within(process)),
            cursorless: true,
            seal_span,
            surface: Arc::new(r.surface.clone()),
            #[cfg(feature = "counter")]
            counter: None,
            nid: r.nid.as_deref().map(Arc::from),
            isolation: r.isolation.as_deref().map(Arc::from),
            runtime_health: Some((route.to_string(), health)),
            address_history: None,
            snapshots: None,
            admin_enabled: false,
            admin_token: None,
            nest_info: Arc::new(nest_info),
        };
        with_derived(&mut state, data);
        Ok(state)
    }

    fn recompose(self: &Arc<Self>) {
        let inner = self.lock();
        let ready = self.clone();
        let nests = self.clone();
        let mut app = axum::Router::new()
            .route("/health", axum::routing::get(|| async { "ok" }))
            .route(
                "/ready",
                axum::routing::get(move || {
                    let f = ready.clone();
                    async move { f.root_ready() }
                }),
            )
            .route(
                "/nests",
                axum::routing::get(move || {
                    let f = nests.clone();
                    async move { Json(f.roster()) }
                }),
            );
        for (route, r) in &inner.routes {
            if let Some(why) = inner.over_budget.get(&r.spec.dataset) {
                app = app.nest(
                    &format!("/{route}"),
                    refusing("over_budget", format!("'{route}' is not held here: {why}")),
                );
                continue;
            }
            let sub = match &r.served {
                Some(Ok(shared)) => {
                    let entities = crate::entities::load(&self.root.join(&r.spec.dataset))
                        .unwrap_or_default()
                        .into_iter()
                        .map(|e| e.name.to_ascii_lowercase())
                        .collect();
                    guarded(
                        self.clone(),
                        route.clone(),
                        Arc::new(entities),
                        crate::serve::mount_router(shared.clone(), route),
                    )
                }
                Some(Err(why)) => refusing(
                    "unservable",
                    format!("'{route}' cannot be served by this follower: {why}"),
                ),
                None => refusing(
                    "catching_up",
                    format!("'{route}' is catching up: waiting for its snapshot from the writer"),
                ),
            };
            app = app.nest(&format!("/{route}"), sub);
        }
        if !inner.mounts_known {
            app = app.merge(refusing(
                "catching_up",
                "this follower has not yet heard the writer's mounts".to_string(),
            ));
        }
        self.live.swap(app);
    }

    fn data(&self, route: &str) -> Option<Data> {
        let inner = self.lock();
        let dataset = &inner.routes.get(route)?.spec.dataset;
        inner.datasets.get(dataset).cloned()
    }

    /// How far this route's dataset trails the writer, as `/ready` reports it.
    fn lag(&self, inner: &Inner, dataset: &str) -> serde_json::Value {
        let applied = inner.datasets.get(dataset).map(|d| d.store.applied());
        let writer = inner.positions.get(dataset).copied();
        let lag = match (applied, writer) {
            (Some((ae, ag)), Some((we, wg))) if ae == we => Some(wg.saturating_sub(ag)),
            _ => None,
        };
        let mut out = serde_json::json!({
            "writer": if inner.link.connected { "connected" } else { "absent" },
            "applied_generation": applied.map(|a| a.1),
            "writer_generation": writer.map(|w| w.1),
            "lag_generations": lag,
            "writer_version": inner.link.version,
        });
        if !inner.link.connected {
            out["absent_for_secs"] = inner.link.since.elapsed().as_secs().into();
        }
        out
    }

    fn route_ready(&self, route: &str) -> Response {
        let inner = self.lock();
        let Some(r) = inner.routes.get(route) else {
            return StatusCode::NOT_FOUND.into_response();
        };
        if let Some(why) = inner.over_budget.get(&r.spec.dataset) {
            return (
                StatusCode::SERVICE_UNAVAILABLE,
                Json(serde_json::json!({
                    "ready": false, "nest": route, "reason": "over_budget", "error": why,
                })),
            )
                .into_response();
        }
        let Some(store) = inner.datasets.get(&r.spec.dataset).map(|d| &d.store) else {
            return (
                StatusCode::SERVICE_UNAVAILABLE,
                Json(serde_json::json!({"ready": false, "nest": route, "reason": "catching_up"})),
            )
                .into_response();
        };
        let as_of = store
            .get_meta("last_block")
            .ok()
            .flatten()
            .and_then(|v| v.parse::<u64>().ok());
        (
            StatusCode::OK,
            Json(serde_json::json!({
                "ready": true,
                "nest": route,
                "as_of": as_of,
                "sealed_through": store.sealed_through(),
                "follower": self.lag(&inner, &r.spec.dataset),
                "derived": inner.datasets.get(&r.spec.dataset).map(|d| match (&d.building, &d.derived) {
                    (Some(why), _) => serde_json::json!({"state": "catching_up", "reason": why}),
                    (None, Err(r)) => serde_json::json!({"state": "writer_only", "reason": r.why}),
                    (None, Ok(_)) => serde_json::json!({"state": "ready"}),
                }),
            })),
        )
            .into_response()
    }

    fn root_ready(&self) -> Response {
        let inner = self.lock();
        let mut nests = serde_json::Map::new();
        let mut all = inner.mounts_known;
        for (route, r) in &inner.routes {
            let refused = inner.over_budget.get(&r.spec.dataset);
            let ready = refused.is_none()
                && inner.datasets.contains_key(&r.spec.dataset)
                && matches!(r.served, Some(Ok(_)));
            all &= ready;
            let mut v = self.lag(&inner, &r.spec.dataset);
            v["ready"] = ready.into();
            if let Some(why) = refused {
                v["reason"] = "over_budget".into();
                v["error"] = why.as_str().into();
            }
            nests.insert(route.clone(), v);
        }
        let status = if all {
            StatusCode::OK
        } else {
            StatusCode::SERVICE_UNAVAILABLE
        };
        (
            status,
            Json(serde_json::json!({
                "ready": all,
                "follower": true,
                "writer": if inner.link.connected { "connected" } else { "absent" },
                "mounts_known": inner.mounts_known,
                "nests": nests,
            })),
        )
            .into_response()
    }

    fn roster(&self) -> serde_json::Value {
        let inner = self.lock();
        serde_json::json!({
            "follower": true,
            "nests": inner.routes.iter().map(|(route, r)| serde_json::json!({
                "name": route,
                "nid": r.spec.nid,
                "ready": inner.datasets.contains_key(&r.spec.dataset),
            })).collect::<Vec<_>>(),
        })
    }
}

fn refusing(reason: &'static str, error: String) -> axum::Router {
    let body = serde_json::json!({"error": error, "reason": reason, "ready": false});
    axum::Router::new().fallback(move || {
        let body = body.clone();
        async move { (StatusCode::SERVICE_UNAVAILABLE, Json(body)) }
    })
}

/// Routes answered from derived state, which a follower serves only once it has built its own.
fn writer_only(path: &str) -> bool {
    matches!(path, "/balances" | "/flags" | "/derived")
        || ["/balance/", "/exposure/", "/derived/"]
            .iter()
            .any(|p| path.starts_with(p))
}

fn derived_refused(reason: &str, what: &str, why: &str) -> Response {
    let error = match reason {
        "catching_up" => format!("{what} waits for this nest's derived state: {why}"),
        _ => format!("{what} is served by the writer only on this nest: {why}"),
    };
    (
        StatusCode::SERVICE_UNAVAILABLE,
        Json(serde_json::json!({"error": error, "reason": reason})),
    )
        .into_response()
}

/// Percent-decoding for a query string, enough to find identifiers in it.
fn decode_query(raw: &str) -> String {
    let bytes = raw.as_bytes();
    let mut out = Vec::with_capacity(bytes.len());
    let mut i = 0;
    while i < bytes.len() {
        match bytes[i] {
            b'+' => out.push(b' '),
            b'%' => match raw
                .get(i + 1..i + 3)
                .and_then(|h| u8::from_str_radix(h, 16).ok())
            {
                Some(b) => {
                    out.push(b);
                    i += 2;
                }
                None => out.push(b'%'),
            },
            b => out.push(b),
        }
        i += 1;
    }
    String::from_utf8_lossy(&out).into_owned()
}

/// Whether `text` names any of `entities` as a whole identifier.
fn names_entity(text: &str, entities: &BTreeSet<String>) -> bool {
    text.to_ascii_lowercase()
        .split(|c: char| !(c.is_ascii_alphanumeric() || c == '_'))
        .any(|word| entities.contains(word))
}

fn guarded(
    follower: Arc<Follower>,
    route: String,
    declared: Arc<BTreeSet<String>>,
    router: axum::Router,
) -> axum::Router {
    router.layer(axum::middleware::from_fn(
        move |req: Request, next: Next| {
            let follower = follower.clone();
            let route = route.clone();
            let declared = declared.clone();
            async move {
                let path = req.uri().path().to_string();
                if path == "/ready" {
                    return follower.route_ready(&route);
                }
                // Derived state being rebuilt, or that could not be built, is never served in part.
                let refusal = match follower.data(&route) {
                    Some(Data {
                        building: Some(why),
                        ..
                    }) => Some(("catching_up", why)),
                    Some(Data {
                        derived: Err(r), ..
                    }) => Some(("writer_only", r.why)),
                    _ => None,
                };
                let Some((reason, why)) = refusal else {
                    return next.run(req).await;
                };
                if writer_only(&path) {
                    return derived_refused(reason, &path, &why);
                }
                let reads_sql = matches!(path.as_str(), "/sql" | "/explain" | "/graphql")
                    || path.starts_with("/q/")
                    || path.starts_with("/subgraphs/");
                if declared.is_empty() || !reads_sql {
                    return next.run(req).await;
                }
                // A named query's text is the nest's, not the request's, so it is not read here.
                if path.starts_with("/q/") {
                    return derived_refused(reason, "a named query on this nest", &why);
                }
                let entities = &*declared;
                let (parts, body) = req.into_parts();
                let Ok(bytes) = axum::body::to_bytes(body, BODY_LIMIT).await else {
                    return StatusCode::PAYLOAD_TOO_LARGE.into_response();
                };
                let text = format!(
                    "{} {}",
                    decode_query(parts.uri.query().unwrap_or("")),
                    String::from_utf8_lossy(&bytes)
                );
                if names_entity(&text, entities) {
                    return derived_refused(reason, "SQL naming an entity", &why);
                }
                next.run(Request::from_parts(parts, axum::body::Body::from(bytes)))
                    .await
            }
        },
    ))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn an_entity_is_found_however_the_query_was_encoded() {
        let entities: BTreeSet<String> = ["holders".to_string()].into();
        for q in [
            "q=select+*+from+holders",
            "q=select%20*%20from%20Holders",
            "q=SELECT%20count(*)%20FROM%20%22holders%22",
        ] {
            assert!(names_entity(&decode_query(q), &entities), "{q}");
        }
        assert!(!names_entity(
            &decode_query("q=select+*+from+holders_by_day"),
            &entities
        ));
        assert_eq!(decode_query("a%2"), "a%2");
    }

    #[test]
    fn derived_routes_are_the_writers() {
        for p in [
            "/balances",
            "/balance/0xab",
            "/exposure/0xab",
            "/flags",
            "/derived",
            "/derived/e/k",
        ] {
            assert!(writer_only(p), "{p}");
        }
        for p in [
            "/sql",
            "/tables",
            "/entity/1",
            "/entities",
            "/ready",
            "/table/t",
        ] {
            assert!(!writer_only(p), "{p}");
        }
    }
}
