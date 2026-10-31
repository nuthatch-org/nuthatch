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
use crate::store::{CoreHotStore as _, HotStore};
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

/// One feed connection's own state.
struct Cx {
    /// Datasets snapshotted on this connection: a delta for any other is a gap.
    fresh: BTreeSet<String>,
    /// A fetch from the mirror sends its dataset here once every file it waited for is in place.
    ready: tokio::sync::mpsc::Sender<String>,
    /// Fetches in flight, which end with the connection.
    fetches: Vec<AbortOnDrop>,
    /// The format this connection speaks, which its `hello` must name.
    feed: u32,
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

/// The admin token from `NUTHATCH_ADMIN_TOKEN` alone, for a follower with no directory of the
/// writer's to find one in.
fn writer_token_from_env() -> Result<String> {
    std::env::var("NUTHATCH_ADMIN_TOKEN")
        .ok()
        .filter(|t| !t.trim().is_empty())
        .context(
            "no admin token for the writer: a follower on another host takes it from \
             NUTHATCH_ADMIN_TOKEN, and only from there",
        )
}

pub async fn serve(args: crate::cli::ServeArgs) -> Result<()> {
    let writer = args
        .follow
        .clone()
        .context("a follower needs --follow <writer-url>")?;
    let cors = crate::serve::cors_layer(&args.cors)?;
    let options = Options {
        feed: args.feed_version,
        paused: false,
        standby: args.standby,
    };
    let follower = match &args.mirror {
        Some(mirror) => {
            let root = PathBuf::from(
                args.cache
                    .as_deref()
                    .context("--mirror needs --cache <dir>, the follower's own directory")?,
            );
            std::fs::create_dir_all(&root)
                .with_context(|| format!("creating {}", root.display()))?;
            let registry = args
                .registry
                .as_deref()
                .context("--mirror needs --registry, where the writer's nests are published")?;
            let max = args
                .max_rss_mb
                .unwrap_or(crate::runtime::DEFAULT_MAX_RSS_MB);
            let remote = crate::remote_follow::Remote::open(mirror, registry, max)?;
            let token = writer_token_from_env()?;
            Follower::start_remote(root, writer, token, Arc::new(remote), options)
        }
        None => {
            let root =
                PathBuf::from(args.runtime.as_deref().context(
                    "--follow needs --runtime, or --mirror for a follower on another host",
                )?);
            if !root.join(crate::runtime::MOUNTS_FILE).is_file() {
                bail!(
                    "'{}' is not a runtime directory (no {})",
                    root.display(),
                    crate::runtime::MOUNTS_FILE
                );
            }
            let token = writer_token(&root)?;
            Follower::start_with(root, writer, token, None, options)
        }
    };
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
    data_identity: Option<String>,
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
    /// A follower on another host: per dataset, what waits on the mirror for files its catalogue
    /// names, and everything that arrived for the dataset behind it, in order.
    pending: HashMap<String, Pending>,
    /// The highest writer fence accepted per dataset, kept across connections (RFC-0078 §5.3).
    fences: HashMap<String, u64>,
    /// With `--standby`: per dataset, the writer lease as last read, for `/ready`.
    standby: HashMap<String, serde_json::Value>,
    /// Datasets whose lease a standby task is watching.
    watching: BTreeSet<String>,
}

/// A dataset's lines held while the files the first one names are fetched.
struct Pending {
    items: std::collections::VecDeque<Line>,
    /// What the queued lines would hold once applied, charged to the dataset meanwhile.
    bytes: u64,
    since: Instant,
    /// The files waited for, and why the last attempt did not get them.
    waiting_for: Vec<String>,
    error: Option<String>,
}

/// What a queued line is charged: its keys and values, as [`crate::feed::row_bytes`] counts them.
fn queued_bytes(line: &Line) -> u64 {
    match line {
        Line::Snapshot { snapshot, .. } => snapshot.bytes(),
        Line::Delta { delta, .. } => delta.bytes(),
        _ => 0,
    }
}

/// The catalogue a snapshot or delta carries.
fn catalogue_of(line: &Line) -> Option<&str> {
    match line {
        Line::Snapshot { snapshot, .. } => snapshot.catalogue.as_deref(),
        Line::Delta { delta, .. } => delta.catalogue.as_deref(),
        _ => None,
    }
}

impl Inner {
    fn pending_bytes(&self, dataset: &str) -> u64 {
        self.pending.get(dataset).map_or(0, |p| p.bytes)
    }

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

    /// What `dataset`'s held copy and derived state are charged, which a replacement is read beside,
    /// with whatever waits on the mirror for it.
    fn held(&self, dataset: &str) -> Option<u64> {
        let pending = self.pending_bytes(dataset);
        let Some(data) = self.datasets.get(dataset) else {
            return (pending > 0).then_some(pending);
        };
        Some(
            data.store.bytes()
                + data.derived_retained()
                + self.estimates.get(dataset).copied().unwrap_or(0)
                + pending,
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

/// How often a follower on another host brings offchain snapshots up to the mirror's.
const OFFCHAIN_EVERY: Duration = Duration::from_secs(5);

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
    /// A test's ask for fresh snapshots.
    resnapshot: std::sync::atomic::AtomicBool,
    /// Where a follower on another host takes what one beside the writer reads from its directory.
    remote: Option<Arc<crate::remote_follow::Remote>>,
    /// The feed format asked for.
    feed: u32,
    /// Woken when a delta is applied, so the writer hears of it within 10 ms.
    ack: tokio::sync::Notify,
    /// On another host: cached segments no copy names, with when that was first seen.
    unnamed: Mutex<HashMap<String, Instant>>,
    /// `--standby`: watch each dataset's writer lease and report when it could be taken over.
    standby: bool,
}

/// How a follower is started.
#[derive(Clone, Copy, Debug)]
pub struct Options {
    /// The feed format to ask for: 2, or 1 for newline-delimited JSON.
    pub feed: u32,
    /// Apply nothing the feed sends until [`Follower::pause`] lets it go.
    pub paused: bool,
    /// Watch each dataset's writer lease in the mirror and report it (RFC-0078 S1). Takes nothing.
    pub standby: bool,
}

impl Default for Options {
    fn default() -> Self {
        Options {
            feed: crate::feed_wire::FEED_V2,
            paused: false,
            standby: false,
        }
    }
}

impl Follower {
    /// Start following `writer` for the runtime at `root`.
    pub fn start(root: PathBuf, writer: String, token: String) -> Arc<Follower> {
        Follower::start_with(root, writer, token, None, Options::default())
    }

    /// [`Follower::start`], applying nothing the feed sends until [`Follower::pause`] lets it go.
    #[doc(hidden)]
    pub fn start_paused(root: PathBuf, writer: String, token: String) -> Arc<Follower> {
        let options = Options {
            paused: true,
            ..Options::default()
        };
        Follower::start_with(root, writer, token, None, options)
    }

    /// Follow `writer` from another host: `root` is this follower's own directory, filled from
    /// `remote`'s mirror and registry.
    pub fn start_remote(
        root: PathBuf,
        writer: String,
        token: String,
        remote: Arc<crate::remote_follow::Remote>,
        options: Options,
    ) -> Arc<Follower> {
        Follower::start_with(root, writer, token, Some(remote), options)
    }

    pub fn start_with(
        root: PathBuf,
        writer: String,
        token: String,
        remote: Option<Arc<crate::remote_follow::Remote>>,
        options: Options,
    ) -> Arc<Follower> {
        let budget = match &remote {
            Some(r) => r.max_rss_mb,
            None => max_rss_mb(&root),
        };
        let follower = Arc::new(Follower {
            writer: writer.trim_end_matches('/').to_string(),
            token,
            live: LiveRuntime::new(axum::Router::new()),
            remote,
            feed: options.feed,
            ack: tokio::sync::Notify::new(),
            unnamed: Mutex::default(),
            standby: options.standby,
            inner: Mutex::new(Inner {
                pending: HashMap::new(),
                fences: HashMap::new(),
                standby: HashMap::new(),
                watching: BTreeSet::new(),
                routes: BTreeMap::new(),
                datasets: HashMap::new(),
                positions: HashMap::new(),
                mounts_known: false,
                max_rss_mb: budget,
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
            paused: tokio::sync::watch::Sender::new(options.paused),
            connects: Default::default(),
            resnapshot: Default::default(),
        });
        follower.recompose();
        tokio::spawn(follower.clone().run());
        if follower.remote.is_some() {
            tokio::spawn(follower.clone().keep_offchain());
        }
        follower
    }

    /// The per-cursor budget in MiB: the runtime's `mounts.toml` beside the writer, the follower's
    /// own `--max-rss-mb` on another host.
    fn budget_mb(&self) -> u64 {
        match &self.remote {
            Some(r) => r.max_rss_mb,
            None => max_rss_mb(&self.root),
        }
    }

    /// The mirror prefix the writer named for `dataset`.
    fn prefix_of(&self, dataset: &str) -> Option<String> {
        self.lock()
            .routes
            .values()
            .find(|r| r.spec.dataset == dataset)
            .and_then(|r| r.spec.data_identity.clone())
    }

    /// Delete cached segments that no copy here names, served, retiring or waiting on the mirror,
    /// once none has named them for `grace`: what a fold replaced, or an unmounted dataset left.
    fn sweep_cache(&self, grace: Duration) -> Result<()> {
        let mut named: BTreeSet<String> = BTreeSet::new();
        let mut name = |raw: Option<&str>| {
            if let Some(m) = raw.and_then(|r| serde_json::from_str::<crate::seal::Manifest>(r).ok())
            {
                named.extend(m.tables.into_values().flatten().map(|s| s.hash));
            }
        };
        {
            let inner = self.lock();
            // A request still on an older version of a copy names at most what the grace covers.
            let copies = inner
                .datasets
                .values()
                .filter_map(|d| d.store.copy().upgrade())
                .chain(
                    inner
                        .retiring
                        .iter()
                        .filter_map(|r| r.store.as_ref()?.upgrade()),
                );
            for copy in copies {
                name(copy.catalogue().as_deref());
            }
            for p in inner.pending.values() {
                for line in &p.items {
                    name(catalogue_of(line));
                }
            }
        }
        let dir = self.root.join(crate::seal::SEGMENTS_DIR);
        let entries = match std::fs::read_dir(&dir) {
            Ok(e) => e,
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Ok(()),
            Err(e) => return Err(e).with_context(|| format!("listing {}", dir.display())),
        };
        let mut unnamed = self.unnamed.lock().unwrap_or_else(|e| e.into_inner());
        let mut seen = BTreeSet::new();
        for entry in entries {
            let entry = entry?;
            let file = entry.file_name().to_string_lossy().into_owned();
            let Some(hash) = file.strip_suffix(".parquet") else {
                continue;
            };
            if named.contains(hash) {
                continue;
            }
            seen.insert(hash.to_string());
            let since = *unnamed.entry(hash.to_string()).or_insert_with(Instant::now);
            if since.elapsed() >= grace {
                match std::fs::remove_file(entry.path()) {
                    Ok(()) => tracing::debug!("{file}: no copy names it; removed from the cache"),
                    Err(e) if e.kind() == std::io::ErrorKind::NotFound => {}
                    Err(e) => return Err(e).with_context(|| format!("removing {file}")),
                }
            }
        }
        unnamed.retain(|h, _| seen.contains(h));
        Ok(())
    }

    /// Keep every held dataset's offchain snapshots at the mirror's, as a writer's pulls move them.
    /// Entities reading them catch up after the next window, by the code the writer runs.
    async fn keep_offchain(self: Arc<Self>) {
        let Some(remote) = self.remote.clone() else {
            return;
        };
        let period = OFFCHAIN_EVERY
            .min(remote.cache_grace)
            .max(Duration::from_millis(50));
        let mut every = tokio::time::interval(period);
        loop {
            every.tick().await;
            if let Err(e) = self.sweep_cache(remote.cache_grace) {
                tracing::warn!("sweeping the follower's segment cache: {e:#}");
            }
            // A nest the registry did not have is asked for again, as an operator publishes it.
            let unserved: Vec<(String, Spec)> = self
                .lock()
                .routes
                .iter()
                .filter(|(_, r)| {
                    matches!(r.served, Some(Err(_)))
                        && !self
                            .root
                            .join(&r.spec.dataset)
                            .join(crate::config::CONFIG_FILE)
                            .exists()
                })
                .map(|(route, r)| (route.clone(), r.spec.clone()))
                .collect();
            for (route, spec) in unserved {
                if self.install(&remote, &spec).await.is_ok() {
                    let estimate = nest_estimate(&self.root.join(&spec.dataset));
                    let mut inner = self.lock();
                    inner.estimates.insert(spec.dataset.clone(), estimate);
                    if let Some(r) = inner.routes.get_mut(&route).filter(|r| r.spec == spec) {
                        r.served = None;
                    }
                    drop(inner);
                    self.realise();
                    self.recompose();
                }
            }
            let held: Vec<(String, String)> = {
                let inner = self.lock();
                inner
                    .routes
                    .values()
                    .filter_map(|r| Some((r.spec.dataset.clone(), r.spec.data_identity.clone()?)))
                    .collect::<BTreeSet<_>>()
                    .into_iter()
                    .collect()
            };
            for (dataset, prefix) in held {
                if let Err(e) = remote.sync_offchain(&self.root, &dataset, &prefix).await {
                    tracing::warn!("{dataset}: offchain snapshots from the mirror: {e:#}");
                }
            }
        }
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

    /// Tell the writer, every [`REPORT_EVERY`] and within 10 ms of each delta applied, what each
    /// copy has applied, as [`report_loop`] does.
    async fn report_applied(self: Arc<Self>, client: reqwest::Client) {
        let url = format!("{}/_admin/feed/applied", self.writer);
        report_loop(&client, &url, &self.token, &self.ack, REPORT_EVERY, || {
            let inner = self.lock();
            let applied = inner
                .datasets
                .iter()
                .map(|(d, copy)| (d.clone(), copy.store.applied()))
                .collect();
            Some((inner.link.conn?, applied))
        })
        .await
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
            .query(&[("exclude", excluded), ("feed", self.feed.to_string())])
            .bearer_auth(&self.token)
            .send()
            .await
            .map_err(reqwest::Error::without_url)
            .context("connecting to the writer")?;
        if !resp.status().is_success() {
            bail!("the writer refused the feed: {}", resp.status());
        }
        // A writer that predates v2 ignores the ask and answers v1, which is read as v1.
        let framed = resp
            .headers()
            .get(reqwest::header::CONTENT_TYPE)
            .is_some_and(|t| t.as_bytes() == crate::feed_wire::CONTENT_TYPE.as_bytes());
        // Whatever waited on the mirror belonged to the last connection's snapshots.
        self.lock().pending.clear();
        // Fresh snapshots check every cached file they name again: the cache is a directory anyone
        // with access may have written to since.
        if let Some(remote) = &self.remote {
            remote.reverify();
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
                    // One whose lines wait on the mirror grows into what they leave.
                    crate::feed::Kind::Delta => match inner.pending.get(dataset) {
                        Some(p) => cap.saturating_sub(
                            inner.datasets.get(dataset).map_or(0, |s| s.store.bytes()) + p.bytes,
                        ),
                        None => cap.saturating_sub(
                            inner.datasets.get(dataset).map_or(cap, |s| s.store.bytes()),
                        ),
                    },
                })
            });
            let each = |line| {
                sends.sent();
                line_tx.blocking_send(line).is_ok()
            };
            if framed {
                crate::feed::read_frames(reader, cap_for, each)
            } else {
                crate::feed::read_lines(reader, cap_for, each)
            }
        });
        let reporter = tokio::spawn(self.clone().report_applied(client.clone()));
        let _reporter = AbortOnDrop(reporter);
        let (ready, mut fetched) = tokio::sync::mpsc::channel::<String>(64);
        let mut cx = Cx {
            fresh: BTreeSet::new(),
            ready,
            fetches: Vec::new(),
            feed: if framed {
                crate::feed_wire::FEED_V2
            } else {
                crate::feed::FEED_VERSION
            },
        };
        let handled = async {
            loop {
                tokio::select! {
                    line = line_rx.recv() => {
                        let Some(line) = line else { break };
                        self.handle(line, &mut cx).await?;
                        applied.handled();
                    }
                    Some(dataset) = fetched.recv() => self.drain(&dataset, &mut cx).await?,
                }
            }
            Ok::<_, anyhow::Error>(())
        }
        .await;
        drop(cx);
        self.lock().pending.clear();
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
                let pending = inner.pending_bytes(d);
                let Some(held) = inner.datasets.get(d) else {
                    return (pending > 0).then_some(pending);
                };
                Some(
                    held.store.bytes()
                        + held.derived_retained()
                        + inner.estimates.get(d).copied().unwrap_or(0)
                        + pending,
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

    /// Take fresh snapshots at the next heartbeat, as a writer restart would make it.
    #[doc(hidden)]
    pub fn resnapshot(&self) {
        self.resnapshot
            .store(true, std::sync::atomic::Ordering::SeqCst);
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

    async fn handle(self: &Arc<Self>, line: Line, cx: &mut Cx) -> Result<()> {
        let _ = self.paused.subscribe().wait_for(|p| !*p).await;
        match line {
            Line::Hello {
                version,
                feed,
                conn,
            } => {
                if feed != cx.feed {
                    bail!(
                        "the writer ({version}) speaks feed version {feed} and this follower ({}) \
                         expected {}: upgrade them to one release",
                        env!("CARGO_PKG_VERSION"),
                        cx.feed
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
                data_identity,
                fence,
            } => {
                if let Some(fence) = fence {
                    let key = data_identity.clone().unwrap_or_else(|| dataset.clone());
                    let mut inner = self.lock();
                    let seen = inner.fences.entry(key.clone()).or_insert(fence);
                    if fence < *seen {
                        bail!(
                            "the writer holds {key} at fence {fence}, below the {} this follower has \
                             already followed: it lost the writer lease and must not be followed",
                            *seen
                        );
                    }
                    *seen = fence;
                }
                if let (true, Some(identity)) = (self.standby, &data_identity) {
                    if self.remote.is_some() && self.lock().watching.insert(dataset.clone()) {
                        tokio::spawn(Follower::watch_lease(
                            Arc::downgrade(self),
                            dataset.clone(),
                            identity.clone(),
                        ));
                    }
                }
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
                    data_identity,
                };
                // A reconnect repeats every mount; only a changed one is rebuilt, or one that could
                // not be installed, which is tried again.
                // The mirror prefix is where fetches go, not how the route is served: a dataset
                // whose identity moves keeps its route, and only the prefix is updated.
                let unchanged = {
                    let mut inner = self.lock();
                    inner.routes.get_mut(&route).is_some_and(|r| {
                        let same = Spec {
                            data_identity: r.spec.data_identity.clone(),
                            ..spec.clone()
                        } == r.spec;
                        if same {
                            r.spec.data_identity = spec.data_identity.clone();
                        }
                        same && !(self.remote.is_some() && matches!(r.served, Some(Err(_))))
                    })
                };
                if unchanged {
                    let estimate = nest_estimate(&self.root.join(&spec.dataset));
                    self.lock().estimates.insert(spec.dataset.clone(), estimate);
                    return Ok(());
                }
                let installed = match &self.remote {
                    Some(remote) => self.install(remote, &spec).await,
                    None => Ok(()),
                };
                let estimate = nest_estimate(&self.root.join(&spec.dataset));
                {
                    let mut inner = self.lock();
                    inner.estimates.insert(spec.dataset.clone(), estimate);
                    let served = installed.err().map(Err);
                    inner.routes.insert(route, Route { spec, served });
                }
                self.realise();
                self.recompose();
            }
            Line::Mounted => {
                {
                    let mut inner = self.lock();
                    inner.mounts_known = true;
                    let max = self.budget_mb();
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
                cx.fresh.insert(dataset.clone());
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
                let line = Line::Snapshot {
                    dataset: crate::feed::DatasetName(dataset.clone()),
                    snapshot,
                };
                let Some(line) = self.waits_on_mirror(&dataset, line, cx).await? else {
                    return Ok(());
                };
                let Line::Snapshot { snapshot, .. } = line else {
                    unreachable!("a snapshot was put in")
                };
                self.apply_snapshot(&dataset, snapshot).await?;
            }
            Line::Delta { dataset, delta } => {
                let dataset = dataset.0;
                let refused = self.lock().over_budget.contains_key(&dataset);
                if refused {
                    return Ok(());
                }
                if !cx.fresh.contains(&dataset) {
                    bail!("{dataset}: a delta before its snapshot; resnapshotting");
                }
                let line = Line::Delta {
                    dataset: crate::feed::DeltaDataset(dataset.clone()),
                    delta,
                };
                let Some(line) = self.waits_on_mirror(&dataset, line, cx).await? else {
                    return Ok(());
                };
                let Line::Delta { delta, .. } = line else {
                    unreachable!("a delta was put in")
                };
                self.apply_delta(&dataset, delta).await?;
            }
            Line::Heartbeat { positions } => self.heartbeat(positions)?,
        }
        Ok(())
    }

    /// Install `spec`'s nest from the registry, so its config, labels and entities are read as a
    /// follower beside the writer reads them from the runtime directory.
    async fn install(
        &self,
        remote: &crate::remote_follow::Remote,
        spec: &Spec,
    ) -> std::result::Result<(), String> {
        let nid = spec
            .nid
            .as_deref()
            .ok_or("the writer names no NID for it, so it cannot be fetched from a registry")?;
        remote
            .install(&self.root, &spec.dataset, nid)
            .await
            .map_err(|e| format!("{e:#}"))?;
        if let Some(prefix) = &spec.data_identity {
            if let Err(e) = remote
                .sync_offchain(&self.root, &spec.dataset, prefix)
                .await
            {
                tracing::warn!(
                    "{}: offchain snapshots from the mirror: {e:#}",
                    spec.dataset
                );
            }
        }
        Ok(())
    }

    /// On another host, hold `line` back if its dataset already has lines waiting on the mirror, or
    /// if its catalogue names a segment this follower does not hold yet; start fetching for it. A
    /// dataset whose held lines would pass its budget is resnapshotted, as a full queue is. The line
    /// back when it was not held; it is moved, never copied, into the queue when it is.
    async fn waits_on_mirror(
        self: &Arc<Self>,
        dataset: &str,
        line: Line,
        cx: &mut Cx,
    ) -> Result<Option<Line>> {
        if self.remote.is_none() {
            return Ok(Some(line));
        }
        let queued = self.lock().pending.contains_key(dataset);
        let needs = match (queued, catalogue_of(&line)) {
            (true, _) | (_, None) => Vec::new(),
            (false, Some(c)) => self.missing(c.to_string()).await?,
        };
        if !queued && needs.is_empty() {
            return Ok(Some(line));
        }
        let bytes = queued_bytes(&line);
        let cap = self.cap_for(dataset).unwrap_or(0);
        {
            let mut inner = self.lock();
            let held = inner.datasets.get(dataset).map_or(0, |d| d.store.bytes());
            let pending = inner.pending_bytes(dataset);
            if held + pending + bytes > cap {
                inner.pending.remove(dataset);
                drop(inner);
                bail!(
                    "{dataset}: what arrived behind a seal waiting on the mirror passed the {cap} \
                     bytes it has left; resnapshotting"
                );
            }
            let p = inner
                .pending
                .entry(dataset.to_string())
                .or_insert_with(|| Pending {
                    items: Default::default(),
                    bytes: 0,
                    since: Instant::now(),
                    waiting_for: Vec::new(),
                    error: None,
                });
            p.items.push_back(line);
            p.bytes += bytes;
        }
        if !needs.is_empty() {
            self.fetch(dataset, needs, cx);
        }
        Ok(None)
    }

    /// Fetch `segments` for `dataset` from the mirror until every one is in place, then say so on
    /// the connection's ready channel. Retried with backoff for as long as the connection lasts.
    fn fetch(
        self: &Arc<Self>,
        dataset: &str,
        segments: Vec<(String, crate::seal::Segment)>,
        cx: &mut Cx,
    ) {
        let (Some(remote), Some(prefix)) = (self.remote.clone(), self.prefix_of(dataset)) else {
            if let Some(p) = self.lock().pending.get_mut(dataset) {
                p.error = Some(
                    "the writer names no data identity for it, so its segments cannot be found on \
                     a mirror; is the writer older than this follower?"
                        .into(),
                );
            }
            return;
        };
        if let Some(p) = self.lock().pending.get_mut(dataset) {
            p.waiting_for = segments.iter().map(|(_, s)| s.file.clone()).collect();
        }
        let me = Arc::downgrade(self);
        let ready = cx.ready.clone();
        let dataset = dataset.to_string();
        let root = self.root.clone();
        let task = tokio::spawn(async move {
            let started = Instant::now();
            let mut backoff = Duration::from_millis(100);
            loop {
                let got = remote.fetch(&root, &prefix, &segments).await;
                let mut error = got.as_ref().err().map(|e| format!("{e:#}"));
                // A mirror that never publishes for followers would otherwise read as a slow one.
                if got.is_err()
                    && !remote
                        .publishes_for_followers(&prefix)
                        .await
                        .unwrap_or(true)
                {
                    error = Some(format!(
                        "the mirror does not publish for followers on other hosts, so it will never \
                         have the files this seal names: set `followers = true` in the writer's \
                         [runtime.publish] or [mounts.publish] ({})",
                        error.unwrap_or_default()
                    ));
                }
                let Some(me) = me.upgrade() else { return };
                if let Some(p) = me.lock().pending.get_mut(&dataset) {
                    p.error = error;
                }
                if got.is_ok() {
                    crate::feed_metrics::metrics().seal_waited(started.elapsed());
                    let _ = ready.send(dataset).await;
                    return;
                }
                drop(me);
                tokio::time::sleep(backoff).await;
                backoff = (backoff * 2).min(Duration::from_secs(2));
            }
        });
        cx.fetches.push(AbortOnDrop(task));
    }

    /// What `catalogue` names that the cache does not hold, checked off the async workers, since a
    /// cached file is hashed on its first use after each connection.
    async fn missing(&self, catalogue: String) -> Result<Vec<(String, crate::seal::Segment)>> {
        let Some(remote) = self.remote.clone() else {
            return Ok(Vec::new());
        };
        let root = self.root.clone();
        tokio::task::spawn_blocking(move || {
            crate::remote_follow::missing(&remote, &root, &catalogue)
        })
        .await
        .context("checking the cache")?
    }

    /// Apply what waited on the mirror for `dataset`, in order, until a line names a file still to
    /// fetch or none is left.
    async fn drain(self: &Arc<Self>, dataset: &str, cx: &mut Cx) -> Result<()> {
        loop {
            let catalogue = {
                let mut inner = self.lock();
                let Some(p) = inner.pending.get_mut(dataset) else {
                    return Ok(());
                };
                let Some(front) = p.items.front() else {
                    inner.pending.remove(dataset);
                    return Ok(());
                };
                catalogue_of(front).map(str::to_string)
            };
            let next = {
                let needs = match catalogue {
                    Some(c) => self.missing(c).await?,
                    None => Vec::new(),
                };
                if !needs.is_empty() {
                    self.fetch(dataset, needs, cx);
                    return Ok(());
                }
                let mut inner = self.lock();
                let Some(p) = inner.pending.get_mut(dataset) else {
                    return Ok(());
                };
                let line = p.items.pop_front().expect("a front was there");
                p.bytes = p.bytes.saturating_sub(queued_bytes(&line));
                p.waiting_for.clear();
                p.error = None;
                if p.items.is_empty() {
                    inner.pending.remove(dataset);
                }
                line
            };
            match next {
                Line::Snapshot { snapshot, .. } => self.apply_snapshot(dataset, snapshot).await?,
                Line::Delta { delta, .. } => {
                    if !cx.fresh.contains(dataset) {
                        bail!("{dataset}: a delta before its snapshot; resnapshotting");
                    }
                    self.apply_delta(dataset, delta).await?
                }
                _ => {}
            }
        }
    }

    /// Build `dataset`'s copy and derived state from `snapshot` and serve it.
    async fn apply_snapshot(
        self: &Arc<Self>,
        dataset: &str,
        snapshot: crate::feed::Snapshot,
    ) -> Result<()> {
        let dataset = dataset.to_string();
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
        Ok(())
    }

    /// Apply the next delta to `dataset`'s copy and feed its derived state.
    async fn apply_delta(self: &Arc<Self>, dataset: &str, delta: crate::feed::Delta) -> Result<()> {
        let dataset = dataset.to_string();
        let started = Instant::now();
        let data = self.lock().datasets.get(&dataset).cloned();
        let Some(data) = data else {
            bail!("{dataset}: a delta before its snapshot; resnapshotting");
        };
        // Charged before it is applied, at its peak, so the copy never holds more than its
        // budget even part-way through a delta.
        // The decoded delta itself is alive beside the copy until the apply is done.
        let peak = data.store.peak_with(&delta) + delta.bytes();
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
                    derived.apply(store.as_ref(), &changes.removed, &changes.added, through)?;
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
        crate::feed_metrics::metrics().applied(started.elapsed());
        self.ack.notify_one();
        if let Some(why) = torn {
            let store = data.store.clone();
            drop(data);
            self.rederive(&dataset, store, why).await?;
        }
        Ok(())
    }

    fn heartbeat(self: &Arc<Self>, positions: BTreeMap<String, (u64, u64)>) -> Result<()> {
        if self
            .resnapshot
            .swap(false, std::sync::atomic::Ordering::SeqCst)
        {
            bail!("asked to take fresh snapshots");
        }
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
            exposure: crate::exposure::ExposureView::start(crate::drive::Driver::Threaded, false)?,
            velocity: crate::velocity::VelocityView::start(crate::drive::Driver::Threaded, false)?,
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
            )
            .route(
                "/metrics",
                axum::routing::get(|| async {
                    let mut s = String::new();
                    crate::feed_metrics::metrics().render(&mut s);
                    (
                        [(
                            axum::http::header::CONTENT_TYPE,
                            "text/plain; version=0.0.4",
                        )],
                        s,
                    )
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
    /// RFC-0078 S1: read `dataset`'s writer lease every second and record what a standby would do.
    /// It takes nothing: promotion is S2.
    async fn watch_lease(me: std::sync::Weak<Follower>, dataset: String, identity: String) {
        let key = crate::lease::lease_key(&identity);
        let mut watch = crate::lease::Watch::new();
        let mut lapsed = false;
        loop {
            let Some(follower) = me.upgrade() else { return };
            let Some(remote) = follower.remote.clone() else {
                return;
            };
            let view = match crate::lease::observe(remote.mirror(), &key).await {
                Ok(seen) => {
                    watch.see(&seen);
                    let doc = seen.doc();
                    let term = std::time::Duration::from_secs(doc.map_or(0, |d| d.term_secs));
                    let takeable = watch.takeable(term / 2);
                    let next = doc.map_or(1, |d| d.fence + 1);
                    if takeable && !lapsed {
                        tracing::warn!(
                            "standby: the writer lease for {identity} is free or has lapsed; this \
                             follower would take it at fence {next}"
                        );
                    }
                    lapsed = takeable;
                    serde_json::json!({
                        "holder": doc.and_then(|d| d.owner.clone()),
                        "fence": doc.map(|d| d.fence),
                        "takeable": takeable,
                        "would_take_fence": takeable.then_some(next),
                    })
                }
                Err(e) => serde_json::json!({ "error": format!("{e:#}") }),
            };
            follower.lock().standby.insert(dataset.clone(), view);
            drop(follower);
            tokio::time::sleep(std::time::Duration::from_secs(1)).await;
        }
    }

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
        // On another host: a seal whose files the mirror has not yet served. What is served
        // meanwhile is the generation before it, whole.
        if let Some(p) = inner.pending.get(dataset) {
            out["mirror"] = serde_json::json!({
                "waiting_for": p.waiting_for,
                "queued": p.items.len(),
                "waiting_secs": p.since.elapsed().as_secs_f64(),
                "error": p.error,
            });
        }
        // Cached files that did not hash to their names, removed and fetched again.
        if let Some(refused) = self
            .remote
            .as_ref()
            .map(|r| r.refused())
            .filter(|r| !r.is_empty())
        {
            out["cache_refused"] = refused.into();
        }
        if let Some(view) = inner.standby.get(dataset) {
            out["standby"] = view.clone();
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

/// Most reports in flight at once, so a report waiting on the writer never holds the next back.
const REPORTS_IN_FLIGHT: usize = 8;

/// Each copy's applied `(epoch, generation)`, by dataset.
type Positions = BTreeMap<String, (u64, u64)>;
/// A report's connection and positions.
type Said = (u64, Positions);

/// Report `positions` to the writer at `url` on each tick of `every` and each `ack`, so it keeps the
/// segments a copy may still name until it has moved on. Best effort: unreported, the writer keeps
/// them until its grace and most run out, never less.
///
/// Reports are numbered and go concurrently, and each carries the round trips of the reports that
/// finished since the last, from which the writer works out visibility lag on its own clock
/// (RFC-0076 FR17). `rtt_us`, the last round trip, is for a writer of 5.14.
async fn report_loop(
    client: &reqwest::Client,
    url: &str,
    token: &str,
    ack: &tokio::sync::Notify,
    every: Duration,
    positions: impl Fn() -> Option<Said>,
) {
    use futures::StreamExt;
    let mut tick = tokio::time::interval(every);
    tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
    let mut in_flight = futures::stream::FuturesUnordered::new();
    let mut seq = 0u64;
    let mut trips = std::collections::VecDeque::<(u64, Duration)>::new();
    let mut last: Option<Duration> = None;
    // What the writer last answered a report of: a report saying anything else may be first with news.
    let mut answered: Option<Said> = None;
    loop {
        let room = in_flight.len() < REPORTS_IN_FLIGHT;
        tokio::select! {
            _ = tick.tick(), if room => {}
            _ = ack.notified(), if room => {}
            Some((n, said, news, took)) = in_flight.next() => {
                if let Some(took) = took {
                    trips.push_back((n, took));
                    last = Some(took);
                    answered = Some(said);
                    // Its trip is carried back now rather than on the next tick.
                    if news {
                        ack.notify_one();
                    }
                }
                continue;
            }
        }
        let Some(said) = positions() else {
            continue;
        };
        seq += 1;
        while trips.len() > crate::feed_metrics::TRIPS_CARRIED {
            trips.pop_front();
        }
        let mut query = vec![("conn", said.0.to_string()), ("seq", seq.to_string())];
        if let Some(last) = last {
            query.push(("rtt_us", last.as_micros().to_string()));
        }
        if !trips.is_empty() {
            let carried: Vec<String> = trips
                .drain(..)
                .map(|(n, t)| format!("{n}:{}", t.as_micros()))
                .collect();
            query.push(("rtts", carried.join(",")));
        }
        let news = answered.as_ref() != Some(&said);
        let sent = client
            .post(url)
            .query(&query)
            .bearer_auth(token)
            .timeout(every)
            .json(&said.1);
        let n = seq;
        in_flight.push(async move {
            let started = Instant::now();
            let took = match sent.send().await {
                Ok(resp) if resp.status().is_success() => Some(started.elapsed()),
                Ok(_) => None,
                Err(e) => {
                    tracing::debug!("reporting what is applied: {}", e.without_url());
                    None
                }
            };
            (n, said, news, took)
        });
    }
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

    /// #2467: what versions of a dataset's derived state still being read hold is charged to its
    /// cursor: in what the dataset holds, and in the room left for its next delta and for every other
    /// dataset on the cursor.
    /// RFC-0078 S1: a standby reports the lease's holder, and that it could take over only once the
    /// lease has gone unrenewed for its term and half again.
    #[tokio::test(start_paused = true)]
    async fn a_standby_reports_the_holder_and_then_the_lapse() {
        let tmp = tempfile::tempdir().unwrap();
        let mirror = tmp.path().join("mirror");
        let registry = tmp.path().join("registry");
        std::fs::create_dir_all(&mirror).unwrap();
        std::fs::create_dir_all(&registry).unwrap();
        let doc = crate::lease::LeaseDoc {
            owner: Some("writer-a".into()),
            fence: 2,
            term_secs: 10,
            renewal: 7,
        };
        let at = mirror.join(crate::lease::lease_key("ident"));
        std::fs::create_dir_all(at.parent().unwrap()).unwrap();
        std::fs::write(&at, serde_json::to_vec(&doc).unwrap()).unwrap();
        let remote = crate::remote_follow::Remote::open(
            mirror.to_str().unwrap(),
            registry.to_str().unwrap(),
            2048,
        )
        .unwrap();
        let follower = Follower::start_remote(
            tmp.path().join("cache"),
            "http://127.0.0.1:9".into(),
            "t".into(),
            Arc::new(remote),
            Options {
                standby: true,
                ..Options::default()
            },
        );
        let (ready, _) = tokio::sync::mpsc::channel(1);
        let mut cx = Cx {
            fresh: Default::default(),
            ready,
            fetches: Vec::new(),
            feed: crate::feed_wire::FEED_V2,
        };
        let _ = follower
            .handle(
                Line::Mount {
                    route: "/default/ds".into(),
                    dataset: "data/ds".into(),
                    nid: None,
                    sql: Default::default(),
                    queries: Vec::new(),
                    tenant: None,
                    share: None,
                    limits: None,
                    chain: Some("mainnet".into()),
                    isolation: None,
                    data_identity: Some("ident".into()),
                    fence: Some(2),
                },
                &mut cx,
            )
            .await;
        let view = |f: &Arc<Follower>| f.lock().standby.get("data/ds").cloned().unwrap();
        tokio::time::sleep(std::time::Duration::from_secs(2)).await;
        let live = view(&follower);
        assert_eq!(live["holder"], "writer-a");
        assert_eq!(live["fence"], 2);
        assert_eq!(live["takeable"], false, "{live}");
        tokio::time::sleep(std::time::Duration::from_secs(14)).await;
        let lapsed = view(&follower);
        assert_eq!(lapsed["takeable"], true, "{lapsed}");
        assert_eq!(lapsed["would_take_fence"], 3);
    }

    /// RFC-0078 §5.3: once a follower has followed a dataset at fence N it refuses any writer below
    /// N, so a writer paused through a takeover cannot feed it when it wakes.
    #[tokio::test]
    async fn a_follower_refuses_a_writer_below_the_highest_fence_it_followed() {
        let tmp = tempfile::tempdir().unwrap();
        let follower = Follower::start_paused(
            tmp.path().to_path_buf(),
            "http://127.0.0.1:9".into(),
            "t".into(),
        );
        follower.pause(false);
        let (ready, _) = tokio::sync::mpsc::channel(1);
        let mut cx = Cx {
            fresh: Default::default(),
            ready,
            fetches: Vec::new(),
            feed: crate::feed::FEED_VERSION,
        };
        let mount = |fence: u64| Line::Mount {
            route: "/default/ds".into(),
            dataset: "data/ds".into(),
            nid: None,
            sql: Default::default(),
            queries: Vec::new(),
            tenant: None,
            share: None,
            limits: None,
            chain: Some("mainnet".into()),
            isolation: None,
            data_identity: Some("ident".into()),
            fence: Some(fence),
        };
        let fenced = |r: Result<()>| r.err().is_some_and(|e| format!("{e:#}").contains("fence"));
        assert!(!fenced(follower.handle(mount(2), &mut cx).await));
        let refused = follower.handle(mount(1), &mut cx).await.unwrap_err();
        assert!(
            format!("{refused:#}").contains("below the 2"),
            "{refused:#}"
        );
        assert!(
            !fenced(follower.handle(mount(2), &mut cx).await),
            "the holder again is fine"
        );
        assert!(
            !fenced(follower.handle(mount(3), &mut cx).await),
            "a later holder is followed"
        );
        assert!(
            fenced(follower.handle(mount(2), &mut cx).await),
            "and then 2 is stale too"
        );
    }

    #[tokio::test]
    async fn derived_versions_still_read_are_charged_to_the_cursor() {
        let tmp = tempfile::tempdir().unwrap();
        let dir = tmp.path().join("ds");
        std::fs::create_dir_all(&dir).unwrap();
        std::fs::write(
            dir.join(crate::config::CONFIG_FILE),
            "[nest]\nname = \"ds\"\nchain = \"mainnet\"\nchain_id = 1\nrpc_urls = []\n\n\
             [[contracts]]\nalias = \"t\"\naddress = \"0x0000000000000000000000000000000000000001\"\n\
             abi = \"abi.json\"\n",
        )
        .unwrap();
        std::fs::write(dir.join("abi.json"), "[]").unwrap();
        let writer = crate::store::Store::open(&dir.join(crate::config::DB_FILE)).unwrap();
        let store = Arc::new(MemStore::from_snapshot(
            &dir,
            crate::store::feed_subscribe(&writer).unwrap().snapshot,
        ));
        let config = crate::config::Config::load(&dir).unwrap();
        let derived = Arc::new(
            crate::indexer::FollowerDerived::build(&dir, &config, store.as_ref()).unwrap(),
        );
        derived.balances.apply(
            (0..2_000)
                .flat_map(|i| crate::views::transfer_deltas("0xm", &format!("0x{i:040x}"), 1, 1))
                .collect(),
        );
        derived.balances.flush();
        let follower = Follower::start_paused(
            tmp.path().to_path_buf(),
            "http://127.0.0.1:9".into(),
            "t".into(),
        );
        {
            let mut inner = follower.lock();
            inner.routes.insert(
                "ds".into(),
                Route {
                    spec: Spec {
                        dataset: "ds".into(),
                        nid: None,
                        surface: Default::default(),
                        tenant: None,
                        share: None,
                        limits: None,
                        chain: Some("mainnet".into()),
                        isolation: None,
                        data_identity: None,
                    },
                    served: None,
                },
            );
            let mut beside = inner.routes["ds"].spec.clone();
            beside.dataset = "beside".into();
            inner.routes.insert(
                "beside".into(),
                Route {
                    spec: beside,
                    served: None,
                },
            );
            inner.datasets.insert(
                "beside".into(),
                Data {
                    store: store.clone(),
                    snapshots: Default::default(),
                    derived: Err(Refused {
                        why: "not built here".into(),
                        entities: Arc::default(),
                        gone: false,
                    }),
                    building: None,
                },
            );
            inner.datasets.insert(
                "ds".into(),
                Data {
                    store,
                    snapshots: Default::default(),
                    derived: Ok(derived.clone()),
                    building: None,
                },
            );
        }
        let held = || follower.lock().held("ds").unwrap();
        let room = |dataset: &str| follower.cap_for(dataset).unwrap();
        let (held_before, room_before, beside_before) = (held(), room("ds"), room("beside"));

        let pinned = derived.balances.pinned();
        derived
            .balances
            .apply(crate::views::transfer_deltas("0xm", "0xnew", 1, 1));
        derived.balances.flush();
        let retained = derived.balances.retained_bytes();
        assert!(
            retained > 0,
            "the pinned version keeps what the batch replaced"
        );
        assert_eq!(held(), held_before + retained);
        assert_eq!(room("ds"), room_before - retained);
        assert_eq!(
            room("beside"),
            beside_before - retained,
            "a dataset beside it on the cursor has that much less room"
        );

        drop(pinned);
        assert_eq!(
            (held(), room("ds"), room("beside")),
            (held_before, room_before, beside_before)
        );
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

    type Arrived = tokio::sync::mpsc::UnboundedReceiver<(HashMap<String, String>, Positions)>;

    /// The query of the first report to arrive that `f` accepts.
    async fn report_where(
        arrived: &mut Arrived,
        what: &str,
        f: impl Fn(&HashMap<String, String>, &Positions) -> bool,
    ) -> HashMap<String, String> {
        loop {
            let (q, body) = tokio::time::timeout(Duration::from_secs(5), arrived.recv())
                .await
                .unwrap_or_else(|_| panic!("no report {what}"))
                .unwrap();
            if f(&q, &body) {
                return q;
            }
        }
    }

    /// #2527: a report the writer is slow to answer does not hold back the next one, and its own
    /// round trip is carried back as soon as it is known.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn a_report_waiting_on_the_writer_never_holds_the_next_back() {
        use axum::extract::{Query, State};
        use std::sync::atomic::{AtomicBool, Ordering};
        #[derive(Clone)]
        struct Writer {
            arrived: tokio::sync::mpsc::UnboundedSender<(HashMap<String, String>, Positions)>,
            release: Arc<tokio::sync::Notify>,
            holding: Arc<AtomicBool>,
            held: Arc<Mutex<Option<Duration>>>,
        }
        async fn applied(
            State(w): State<Writer>,
            Query(mut q): Query<HashMap<String, String>>,
            Json(body): Json<Positions>,
        ) -> StatusCode {
            let hold = body.keys().eq(["a"]) && !w.holding.swap(true, Ordering::SeqCst);
            if hold {
                q.insert("held".into(), String::new());
            }
            w.arrived.send((q, body)).unwrap();
            if hold {
                let started = Instant::now();
                let _ = tokio::time::timeout(Duration::from_secs(10), w.release.notified()).await;
                *w.held.lock().unwrap() = Some(started.elapsed());
            }
            StatusCode::NO_CONTENT
        }
        let (tx, mut arrived) = tokio::sync::mpsc::unbounded_channel();
        let writer = Writer {
            arrived: tx,
            release: Arc::default(),
            holding: Arc::default(),
            held: Arc::default(),
        };
        let app = axum::Router::new()
            .route("/applied", axum::routing::post(applied))
            .with_state(writer.clone());
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let url = format!("http://{}/applied", listener.local_addr().unwrap());
        tokio::spawn(async move { axum::serve(listener, app).await });

        let positions: Arc<Mutex<Positions>> = Arc::default();
        let ack = Arc::new(tokio::sync::Notify::new());
        let (p, a) = (positions.clone(), ack.clone());
        let reporter = tokio::spawn(async move {
            let client = reqwest::Client::new();
            let every = Duration::from_secs(3600);
            report_loop(&client, &url, "t", &a, every, || {
                Some((1, p.lock().unwrap().clone()))
            })
            .await
        });

        positions.lock().unwrap().insert("a".into(), (1, 1));
        ack.notify_one();
        let first = report_where(&mut arrived, "for a", |q, _| q.contains_key("held")).await;
        positions.lock().unwrap().insert("b".into(), (1, 1));
        ack.notify_one();
        report_where(&mut arrived, "for b while a's waits", |_, b| {
            b.contains_key("b")
        })
        .await;
        assert!(
            writer.held.lock().unwrap().is_none(),
            "a's report was answered"
        );

        writer.release.notify_one();
        let trip = format!("{}:", first["seq"]);
        let carried = report_where(&mut arrived, "carrying a's round trip", |q, _| {
            q.get("rtts")
                .is_some_and(|t| t.split(',').any(|t| t.starts_with(&trip)))
        })
        .await;
        let held = writer.held.lock().unwrap().unwrap();
        let us: u128 = carried["rtts"]
            .split(',')
            .find_map(|t| t.strip_prefix(&trip))
            .unwrap()
            .parse()
            .unwrap();
        assert!(us >= held.as_micros(), "{us} µs against {held:?} held");
        reporter.abort();
    }
}
