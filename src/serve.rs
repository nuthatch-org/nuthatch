//! The API surface. Point-reads hit redb directly (the hot path). Everything is local; nothing
//! phones home. This is where the MCP server and SQL surface will grow in later slices.

use anyhow::{Context, Result};
#[cfg(feature = "graph")]
use axum::routing::post;
use axum::{
    extract::{Path, Query, State},
    http::StatusCode,
    response::sse::{Event, KeepAlive, Sse},
    response::IntoResponse,
    routing::get,
    Json, Router,
};
use futures::stream;
use serde::Deserialize;
use serde_json::{json, Value};
use std::path::PathBuf;
use std::time::Duration;

use crate::analytics;
use crate::exposure::ExposureView;
use crate::registry::TableSchema;
use crate::velocity::VelocityView;
use crate::views::BalanceView;
use std::sync::Arc;
use tokio::sync::Semaphore;

/// How many analytical (Burrmill) queries may run at once across `/sql`, cold `/table` and a cold
/// `/entity` (#1657). Each Burrmill query is capped at `analytics.memory_limit` /
/// `analytics.threads` (defaults 512 MB / 2;
/// see `analytics_budget`), so this bounds the whole analytical surface's worst-case footprint - the
/// real DoS multiplier is *concurrency*, not any one query. Kept small to stay well inside the
/// embedded RAM budget; this is node self-protection, not per-caller rate-limiting (that needs
/// identity and belongs in a gateway). The permit count is not an unconstrained config key.
pub const SQL_MAX_CONCURRENCY: usize = 2;

/// How long a connection may take to send a complete request head before it is closed (#1660). A
/// real client sends it in one write; ten seconds covers a slow link and still frees a stalled socket.
pub const HEADER_READ_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(10);

/// How long a stopping server waits for open connections before dropping them (#1964). Inside
/// Docker's default 10 s stop grace, so the ingest is still stopped by us and not by SIGKILL.
const SHUTDOWN_GRACE: Duration = Duration::from_secs(5);

/// Hard ceiling on the override below.
///
/// The permit count is a **memory** bound, not a throughput one (#1006, and the correction in
/// RFC-0042 §14): concurrent queries do not serialise, but each one that misses the connection cache
/// opens its own session. Measured on a 32-core box under DuckDB (before 4.1), unbounded at 32 clients reached **1,313 MB - 64%
/// of one cursor's entire 2 GB budget**, shared across every nest on that cursor. So this is capped
/// rather than free-form: an operator raising it is trading RAM they may not have, and the
/// non-negotiable budget is per cursor rather than per nest.
pub const SQL_MAX_CONCURRENCY_CEILING: usize = 16;

/// The live permit count: [`SQL_MAX_CONCURRENCY`] unless `NUTHATCH_SQL_MAX_CONCURRENCY` overrides it.
///
/// **This exists so the value can be measured, not so it can be tuned casually.** #1006 asks for the
/// throughput/RSS curve at 1/2/4/8/16 permits on the box that enforces the RAM budget, and with a
/// bare `const` that needs five separate builds - which is how a ceiling ends up being set from
/// whichever box was convenient. One binary, five settings, measured where it is enforced.
///
/// Refusals are loud and the value is clamped rather than silently accepted: a benchmark that
/// requested 64 and quietly got 2 would publish a curve that is flat for the wrong reason.
/// One analytical gate **per cursor**, shared by every nest on it.
///
/// # Why the cursor is the right unit, and why per-nest was wrong
///
/// [`SQL_MAX_CONCURRENCY`]'s own description has always said it "bounds the whole analytical
/// surface's worst-case footprint". It did not: `build_nest` constructed a **separate** semaphore per
/// nest, so a runtime hosting six nests admitted `6 x permits` concurrent Burrmill queries.
///
/// Survivable at a hardcoded 2; a foot-gun the moment the value became settable, because
/// `NUTHATCH_SQL_MAX_CONCURRENCY=16` across six nests would admit **96**, each able to open its own
/// session - precisely the per-cursor budget the override's warning claims to protect. Caught in
/// review of #1006 (#1024), and the same shape as everything else this sprint found: a comment
/// asserting a property the code did not deliver.
///
/// **The cursor, not the process.** CLAUDE.md's non-negotiable 2 is per *active-chain cursor*, shared
/// across the nests on it - and `group_by_chain` gives one `spawn_runtime` per chain, so one call is
/// one cursor. A process-global gate was the first attempt and is *too* blunt: it would make two
/// unrelated cursors in a multichain runtime contend for one budget they do not share. The gate is
/// therefore created by whoever knows the cursor boundary and passed down.
///
/// **The trade, stated rather than buried:** a multi-nest cursor's effective analytical concurrency
/// drops from `N x permits` to `permits`. That is the documented behaviour finally being true, and it
/// is the safe direction, but it is a real reduction for a dense cursor.
/// `docs/bench/1006-sql-concurrency-hetzner.md` measures what a permit buys and recommends raising
/// the default; that is a separate board decision and is not taken here.
pub fn new_sql_gate() -> Arc<Semaphore> {
    Arc::new(Semaphore::new(sql_max_concurrency()))
}

pub fn sql_max_concurrency() -> usize {
    match std::env::var("NUTHATCH_SQL_MAX_CONCURRENCY") {
        Err(_) => SQL_MAX_CONCURRENCY,
        Ok(raw) => match raw.trim().parse::<usize>() {
            Ok(0) | Err(_) => {
                tracing::warn!(
                    value = %raw,
                    default = SQL_MAX_CONCURRENCY,
                    "NUTHATCH_SQL_MAX_CONCURRENCY is not a positive integer; using the default"
                );
                SQL_MAX_CONCURRENCY
            }
            Ok(n) if n > SQL_MAX_CONCURRENCY_CEILING => {
                tracing::warn!(
                    requested = n,
                    ceiling = SQL_MAX_CONCURRENCY_CEILING,
                    "NUTHATCH_SQL_MAX_CONCURRENCY above the ceiling; clamping. Each concurrent query \
                     can open its own session, and the per-cursor budget is 2 GB shared across every \
                     nest on the cursor (#1006)"
                );
                SQL_MAX_CONCURRENCY_CEILING
            }
            Ok(n) => n,
        },
    }
}
/// Wall-clock deadline for a single analytical query; a runaway (e.g. cartesian) is interrupted.
///
/// Ten times that under test: the tests through the router are built unoptimised, where the engine
/// plans the network nest's thirty-view queries in tens of seconds and not the release build's one.
/// They test answers, not latency, as `tests/network_contract.rs` says of its own allowance.
const SQL_TIMEOUT: Duration = Duration::from_secs(if cfg!(test) { 300 } else { 30 });
/// How long a request waits for an analytical permit before it is told the node is busy (#1319).
///
/// The permit count still caps what *runs*; this only caps how long a caller waits to be told no.
/// It exists because a burst is not the same thing as sustained load: the hackathon builder's agent
/// polled once and then fanned out six follow-ups at once, which against the default two permits
/// produced 128 rejections of 725 queries on Arc and 139 of 648 on Sepolia - roughly one in five,
/// every one a client-side retry for a query that would have succeeded milliseconds later.
///
/// The wait is charged against [`SQL_TIMEOUT`], so queuing cannot extend a request's total
/// deadline; a query admitted after 250 ms gets 29.75 s to run rather than a fresh 30.
const SQL_ADMISSION_WAIT: Duration = Duration::from_millis(250);
/// How many requests may be parked waiting for a permit at once, per nest (#1319).
///
/// **A time limit on each waiter is not a limit on the number of waiters**, which was the flaw in
/// the first cut of the wait: depth is arrival rate multiplied by the window, and nothing in
/// [`SQL_ADMISSION_WAIT`] bounds it. Fail-fast was O(1) parked requests by construction; the wait
/// gives that up, so it has to be bought back explicitly. Past this cap a request is refused
/// immediately, exactly as every over-limit request was before the wait existed.
///
/// A parked request holds its query string, capped at [`SQL_MAX_QUERY_LEN`] (16 KiB), so the worst
/// case here is roughly 4 MB of parked requests per nest. The cap is per-nest rather than
/// per-cursor - unlike the permit count, which is shared because it bounds Burrmill - because what
/// this protects is the memory of requests that are not running at all.
const SQL_MAX_QUEUED: usize = 256;

/// A reservation to *wait* for an analytical permit, released however the wait ends (#1319).
///
/// Only the saturated path takes one: a request that gets a permit on the first try never touches
/// the counter, so the uncontended path is unchanged.
struct QueueSlot(Arc<std::sync::atomic::AtomicUsize>);

impl QueueSlot {
    /// `None` when this nest already has [`SQL_MAX_QUEUED`] requests parked, which is an immediate
    /// refusal rather than a wait.
    fn take(queued: &Arc<std::sync::atomic::AtomicUsize>) -> Option<Self> {
        use std::sync::atomic::Ordering;
        let mut cur = queued.load(Ordering::Relaxed);
        loop {
            if cur >= SQL_MAX_QUEUED {
                return None;
            }
            match queued.compare_exchange_weak(cur, cur + 1, Ordering::AcqRel, Ordering::Relaxed) {
                Ok(_) => return Some(Self(queued.clone())),
                Err(actual) => cur = actual,
            }
        }
    }
}

impl Drop for QueueSlot {
    fn drop(&mut self) {
        self.0.fetch_sub(1, std::sync::atomic::Ordering::AcqRel);
    }
}
/// Cap on rows materialised from one analytical query - bounds the Rust-side result buffer, which
/// lives outside Burrmill's own memory limit. Beyond this the result is truncated and flagged.
const SQL_MAX_ROWS: usize = 50_000;

/// The most unsealed rows `/sql` will materialise for one query.
///
/// The hot store holds everything between the sealed watermark and the tip, and every `/sql` call
/// parses all of it into memory. On a deep-finality chain with a busy contract that is the largest RAM
/// risk the process carries - and in a runtime it is a co-tenant's problem too, because the budget is
/// per cursor. This is the ceiling that turns "the box fell over" into a `503` naming the reason.
///
/// It is generous on purpose: a nest at tip on a normal chain is nowhere near it, so the guard is
/// invisible until something is genuinely wrong. Under the CLAUDE.md division of labour resource
/// safety is the node's job, not the gateway's - which is why this is a hard limit here rather than
/// advice in the docs.
///
/// `pub(crate)` rather than private: it is the default [`AppState::sql_max_hot_rows`] is built
/// with, both in `indexer.rs` and in `test_state` (#378) - a real handler test can lower the seam
/// instead of genuinely putting two million rows in the hot store to reach the 503 arm.
pub(crate) const SQL_MAX_HOT_ROWS: usize = 2_000_000;

/// Bytes of stored row JSON a `/sql` or `/explain` will copy from the hot tip, beside the row cap.
/// Rows are parsed per request, and typed rows from IPFS documents measured about 4.1 KB each once
/// parsed (2026-09-13): 850,879 of them grew the heap 3.36 GB in one request. Past this the request is
/// refused with `503`, never answered from sealed data alone.
pub(crate) const SQL_MAX_HOT_BYTES: u64 = 64 * 1024 * 1024;
/// Independent source-byte guard for declared queries. This is not a RAM conversion: it bounds
/// bytes read from immutable segments plus the serialized hot snapshot before the statement runs.
pub(crate) const SQL_MAX_NAMED_SCAN_BYTES: u64 = 512 * 1024 * 1024;
/// Reject absurdly long query strings before they reach the planner.
const SQL_MAX_QUERY_LEN: usize = 16 * 1024;

#[derive(Clone)]
pub struct AppState {
    /// The hot store behind [`crate::store::HotStore`] (RFC-0022 slice 1). `Arc<dyn _>` rather than a
    /// concrete `Store` because serving must not care which backend answers - that is the whole point
    /// of the seam, and an FE node under RFC-0022 §1 will be handed a Postgres-backed one.
    pub store: std::sync::Arc<dyn crate::store::HotStore>,
    /// The first declared contract's address - `None` for a nest that declares none.
    ///
    /// A contract-free `[extract] blocks = true` nest is a supported shape (RFC-0036 §4.2, OBIB
    /// case 3), and this field being `String` is what refused to let one start: `build_nest` read
    /// it through `Config::primary()?`, which errors "nest has no contracts", so the operator-facing
    /// path rejected an entire configuration over a field only `GET /`'s summary reads. Optional
    /// rather than an arbitrary pick, because a nest with no contracts has no address to name and
    /// saying so is the honest answer; a nest with several has always shown only its first here.
    pub address: Option<String>,
    pub chain: String,
    pub dir: PathBuf,
    pub balances: BalanceView,
    /// Direct counterparty-exposure to the labeled set (RFC-0008 C1) - served at `/exposure/{addr}`.
    pub exposure: ExposureView,
    /// Windowed per-address velocity view (RFC-0008 C3) - served at `/flags?kind=velocity`.
    pub velocity: VelocityView,
    /// The nest's authored incremental entities (RFC-0041), the same handles the ingest loop feeds.
    ///
    /// Shared rather than cloned: an `EntityView` owns a thread and a channel, and a second handle
    /// would be a second writer to one circuit. Serving reads each entity's own applied-through
    /// watermark rather than the nest's head - an entity behind the dataset is an ordinary state
    /// during backfill, and answering for the nest's head while holding this one's rows is exactly
    /// how a partial relation gets stamped current (§5.1, #866).
    pub entities: Arc<Vec<crate::entity_view::EntityView>>,
    /// RFC-0059 §5: the seal loop's checkpoint writer, when the nest ships `folds/`.
    #[cfg(feature = "folds")]
    pub folds: Option<Arc<crate::folds::Writer>>,
    /// Single-transfer threshold in base units, if configured (RFC-0008 C3) - for `/`'s flag summary.
    pub threshold: Option<i128>,
    /// Velocity flag threshold in base units, if configured - the cutoff `/flags?kind=velocity` uses.
    pub velocity_threshold: Option<i128>,
    /// Whether the built-in admin UI (`/_admin/`) is served (RFC-0010 Part A).
    pub admin_enabled: bool,
    /// When the bind is off-localhost, the token a request must present (`?token=…`) to reach the admin
    /// UI (SEC-5). `None` on a localhost bind (open) - the env var merely *enabling* the route off-
    /// localhost, without checking it per request, was security theater.
    pub admin_token: Option<String>,
    /// Static nest metadata for the admin UI's Nest tab (`/nest`): contracts, templates, factories,
    /// webhooks, registry hash. Computed once at startup.
    pub nest_info: Arc<serde_json::Value>,
    /// The nest's table schemas (from the decode registry) - the source of truth for `/tables`.
    pub tables: Arc<Vec<TableSchema>>,
    /// Admission control for the analytical (Burrmill) surface: bounds how many `/sql` and cold
    /// `/table` queries run at once so a burst can't multiply Burrmill's per-query footprint past the
    /// process budget. Constructed with [`SQL_MAX_CONCURRENCY`] permits.
    pub sql_gate: Arc<Semaphore>,
    /// Requests currently parked waiting for [`Self::sql_gate`], bounded by [`SQL_MAX_QUEUED`].
    ///
    /// Per-nest and not shared across a cursor, unlike the gate itself: the gate bounds what runs
    /// in Burrmill, this bounds the memory held by requests that are not running at all (#1319).
    pub sql_queued: Arc<std::sync::atomic::AtomicUsize>,
    /// The most unsealed rows `/sql` and `/explain` will materialise for one query before refusing
    /// with a `503` (`HotScanTooLarge`) - the live value [`SQL_MAX_HOT_ROWS`] documents. A field
    /// rather than the handlers reading the const directly, so a test can lower the ceiling instead
    /// of genuinely putting two million rows in the hot store to reach the refusal arm (#378).
    pub sql_max_hot_rows: usize,
    /// [`SQL_MAX_HOT_BYTES`] in production; lowered by tests.
    pub sql_max_hot_bytes: u64,
    pub sql_max_named_scan_bytes: u64,
    /// This process owns **no cursor**: it serves a nest it does not index (`nuthatch serve`).
    ///
    /// `/ready`'s liveness terms are all about a cursor - has it polled recently, has `last_block`
    /// advanced, did the first poll fail. A role that deliberately never polls fails every one of
    /// them forever (#1025): `graph-staking-legacy-readonly` on the Lodestar box answered 162
    /// queries correctly while reporting `ready:false, stalled:true` continuously from 2026-08-24,
    /// because `poll_stalled` falls back to `started_at` when `last_poll` is 0 and the grace then
    /// expires and never returns.
    ///
    /// That is the mirror of #1020. There an unpopulated gauge rendered as perfect health and no
    /// alert could fire; here a healthy service renders as permanently stalled, so any alert on it
    /// fires forever and gets muted - and a muted alert is not an alert. It also makes the nest
    /// unusable behind anything that gates on `/ready`.
    ///
    /// **Not a new flag.** `serve_role` already knows - its own comment says "No `Source` is ever
    /// polled on this role" - it simply never told this endpoint.
    pub cursorless: bool,
    /// The chain's `seal_span` (#1199). `/ready` calls a tip seal stalled only once a span cut is
    /// overdue in blocks, not merely because a sparse nest has been quiet for hours (#1341).
    pub seal_span: u64,
    /// The cursor's freshness dial (RFC-0040), so `/ready` can say how stale the nest is meant to be
    /// and scale its stall thresholds to the interval rather than reporting a five-minute cursor
    /// stalled at ninety seconds.
    pub freshness: crate::freshness::Freshness,
    /// The SQL surface this mount exposes (RFC-0034). Default is [`Open`](crate::allowlist::SqlAccess::Open) -
    /// arbitrary `/sql`, exactly as before - because a local `nuthatch dev` is an exploration tool and
    /// a security control that turns itself on is a support ticket.
    pub surface: Arc<crate::allowlist::Surface>,
    /// The optional local x402 counter for this mount. Only a binary explicitly built with the
    /// `counter` feature carries it; default self-hosting has no payment path at all (#1217).
    #[cfg(feature = "counter")]
    pub counter: Option<Arc<crate::counter::Config>>,
    /// The identity of the dataset serving this mount (RFC-0032 §3), stamped into `provenance` so an
    /// answer can be cited against the data that produced it. A mounted nest carries its mounted
    /// dataset identity; a solo `dev` nest carries the NID it computed from its manifest. `None`
    /// only when that manifest could not be built.
    pub nid: Option<Arc<str>>,
    /// This nest's name and the runtime health surface it should answer `/ready` from (RFC-0026 §5).
    /// `None` for a solo `dev` nest, which has no mounts around it and falls back to the global
    /// poll-freshness check.
    pub runtime_health: Option<(String, Arc<crate::health::RuntimeHealth>)>,
    /// RFC-0063: the watched-address history `/api` serves. `None` on a nest without
    /// `[address_history]`, which answers every `/api` request unsupported.
    pub address_history: Option<crate::address_history::AddressHistory>,
}

/// A hot-swappable handle to the `AppState` backing one served endpoint (RFC-0020 slice 2). The router
/// binds this instead of a fixed `AppState`; every request resolves the *current* version through
/// [`FromRef`](axum::extract::FromRef), so a compatible upgrade can **atomically re-point** the
/// endpoint at a new version with no rebind and no dropped request (in-flight requests finish against
/// the version they started on). Cheap: a lock-free atomic load plus the same per-request `AppState`
/// clone axum already does. The flip is the mechanism slice 2b's "index new, swap when caught up"
/// orchestration drives; this slice lands and proves the mechanism.
#[derive(Clone)]
pub struct SharedNest(Arc<arc_swap::ArcSwap<AppState>>);

impl SharedNest {
    /// A handle initially backing `state`.
    pub fn new(state: AppState) -> SharedNest {
        SharedNest(Arc::new(arc_swap::ArcSwap::from_pointee(state)))
    }

    /// Atomically re-point this endpoint at a new backing version. The next request sees it.
    pub fn swap(&self, state: AppState) {
        self.0.store(Arc::new(state));
    }

    /// The current backing (a cheap `Arc` clone) - what serving resolves per request.
    pub fn current(&self) -> Arc<AppState> {
        self.0.load_full()
    }
}

// Lets every existing `State<AppState>` handler stay untouched: axum resolves `AppState` from the
// swappable `SharedNest` per request, always seeing the current version.
impl axum::extract::FromRef<SharedNest> for AppState {
    fn from_ref(shared: &SharedNest) -> AppState {
        (*shared.0.load_full()).clone()
    }
}

/// Build a nest's router - every per-nest route plus the request-count layer, bound to a swappable
/// [`SharedNest`]. Split out of [`run`] so a runtime (RFC-0012) can mount many of these under
/// `/<nest>/…` prefixes; a solo `dev` serves exactly one at the root. Identical routes either way - a
/// nest can't tell it's co-hosted, nor that its backing can be hot-swapped underneath it.
///
/// The one route set that is *not* identical either way is the admin UI: with the surface disabled
/// (`--no-admin`, or a public bind with no token) the `/_admin*` routes are never registered, so those
/// paths fall through to the ordinary not-found - which is what `--no-admin` promises and what the
/// runtime's [`crate::runtime::lifecycle_routes`] already did for the mount/unmount half. Handing back
/// a 404 *in the admin UI's own words* still tells an unauthenticated caller that this is a nuthatch
/// with the admin surface switched off (#292).
pub fn router(backing: SharedNest) -> Router {
    // Read once, at composition time: `admin_enabled` is derived from the process's flags and bind
    // (`indexer::admin_enabled`), so it is constant for the life of the endpoint - a hot swap (RFC-0020
    // slice 2) re-points the *data*, never the admin decision.
    let admin_enabled = backing.current().admin_enabled;
    // RFC-0060 §5.6: graph-facing routes are registered only in a `graph` build. That includes the
    // RFC-0053 GraphQL routes (#1440); the porting commands stay in the default build.
    let graph_facade = |r: Router<SharedNest>| {
        #[cfg(feature = "graph")]
        let r = r
            .route("/graph/status", get(graph_status))
            // RFC-0053 S1 (#1265). Both shapes: a plain endpoint an operator points people at, and
            // the subgraph URL form so a client's existing URL needs only its host changed.
            .route("/graphql", post(graph_graphql))
            .route("/subgraphs/id/{id}", post(graph_graphql))
            .route("/subgraphs/name/{*name}", post(graph_graphql));
        r
    };
    let admin = |r: Router<SharedNest>| {
        if !admin_enabled {
            return r;
        }
        r.route("/_admin", get(admin_index))
            .route("/_admin/", get(admin_index))
            .route("/_admin/events", get(admin_events))
            .route("/_admin/config", get(admin_config))
            .route("/_admin/storage", get(admin_storage))
    };
    admin(graph_facade(
        Router::new()
            .route("/", get(summary))
            .route("/health", get(|| async { "ok" }))
            .route("/ready", get(ready))
            .route("/metrics", get(metrics_handler))
            .route("/tables", get(tables))
            .route("/schema", get(schema_doc))
            .route("/table/{name}", get(table))
            .route("/entities", get(entities))
            .route("/ipfs/gave-up", get(ipfs_gave_up))
            .route("/entity/{id}", get(entity))
            .route("/sql", get(sql))
            .route("/explain", get(explain))
            .route("/queries", get(queries))
            .route("/q/{name}", get(named_query))
            .route("/derived", get(derived_index))
            .route("/derived/{entity}", get(derived_all))
            .route("/derived/{entity}/{key}", get(derived_key))
            .route("/balances", get(balances))
            .route("/balance/{address}", get(balance))
            .route("/exposure/{address}", get(exposure))
            .route("/flags", get(flags))
            .route("/nest", get(nest))
            .route("/shape", get(shape))
            .route("/api", get(etherscan_api)),
    ))
    // Count every served request for `/metrics` (the operator's billing signal).
    .layer(axum::middleware::from_fn(count_request))
    .with_state(backing)
}

pub async fn run(
    listen: &str,
    state: AppState,
    cors: Option<tower_http::cors::CorsLayer>,
) -> Result<()> {
    run_shared(listen, SharedNest::new(state), cors).await
}

/// Serve a caller-held [`SharedNest`] - the variant a hot upgrade uses so it can keep the handle and
/// atomically flip the backing (RFC-0020 slice 2b) while serving stays up on the same listener.
pub async fn run_shared(
    listen: &str,
    shared: SharedNest,
    cors: Option<tower_http::cors::CorsLayer>,
) -> Result<()> {
    bind_and_serve(listen, router(shared), cors).await
}

/// Serve **two versions** of a nest on distinct endpoints behind one listener (RFC-0020 slice 3, the
/// breaking path). The OLD version stays at its existing **root** path - unchanged, so current
/// consumers keep working - but every response now carries a `Deprecation: true` header and a `Link`
/// to its successor; the NEW version is served under `new_prefix` (e.g. `/next`). Both index
/// concurrently and neither flips: downstream migrate from the old endpoint to the new on their own
/// clock, then the operator sunsets the old. The deprecation signal is standards-shaped (RFC 8594).
pub async fn run_two_versions(
    listen: &str,
    old: SharedNest,
    new_prefix: &str,
    new: SharedNest,
    cors: Option<tower_http::cors::CorsLayer>,
) -> Result<()> {
    bind_and_serve(listen, two_version_router(old, new_prefix, new), cors).await
}

/// The two-version app (RFC-0020 slice 3): old at the root, wrapped in a `Deprecation`/`Link` layer;
/// new nested under `new_prefix`. Split out of [`run_two_versions`] so it's testable over HTTP.
pub fn two_version_router(old: SharedNest, new_prefix: &str, new: SharedNest) -> Router {
    let successor = format!("{new_prefix}/");
    let deprecate = axum::middleware::from_fn(
        move |req: axum::extract::Request, next: axum::middleware::Next| {
            let successor = successor.clone();
            async move {
                let mut resp = next.run(req).await;
                let headers = resp.headers_mut();
                headers.insert("deprecation", axum::http::HeaderValue::from_static("true"));
                if let Ok(link) = axum::http::HeaderValue::from_str(&format!(
                    "<{successor}>; rel=\"successor-version\""
                )) {
                    headers.insert("link", link);
                }
                resp
            }
        },
    );
    Router::new()
        .nest(new_prefix, router(new))
        .merge(router(old).layer(deprecate))
}

/// Serve many nests behind one listener (RFC-0012 mounts, slice 1): a `/nests` roster plus every nest's
/// full API under its `/<name>/…` prefix. Chain identity and the cursor are still per-nest at this
/// slice (the shared cursor is slice 2); this lands the routing + per-nest isolation of the serving
/// surface first. Each nest's routes are byte-identical to a solo `dev`, just prefixed.
pub async fn run_runtime(
    listen: &str,
    roster: serde_json::Value,
    nests: Vec<(String, AppState)>,
    health: Arc<crate::health::RuntimeHealth>,
    cors: Option<tower_http::cors::CorsLayer>,
) -> Result<()> {
    let live = LiveRuntime::new(compose_runtime(roster, nests, health));
    bind_and_serve(listen, live.service(), cors).await
}

/// The first path segments [`compose_runtime`] serves at the root. A mount named one of these would
/// nest its own `/` onto the root's route, which `Router::nest` panics on (#2130).
pub const RUNTIME_ROOT_ROUTES: [&str; 4] = ["health", "nests", "metrics", "ready"];

/// Compose the runtime's routes for a given nest set: the root endpoints plus every nest nested under
/// its `/<name>` prefix.
///
/// Split out of [`run_runtime`] so the set can be re-composed at runtime (RFC-0027). Routing semantics
/// are unchanged from the static version - still `Router::nest` - which is what makes the parity test
/// meaningful rather than a tautology.
pub fn compose_runtime(
    roster: serde_json::Value,
    nests: Vec<(String, AppState)>,
    health: Arc<crate::health::RuntimeHealth>,
) -> Router {
    let roster = Arc::new(roster);
    let roster_health = health.clone();
    let ready_health = health.clone();
    // Every nest's handle is built **before** the root routes, so the root `/ready` can hold them and
    // judge each nest with [`nest_readiness`] - the same function that nest's own `/ready` calls
    // (#1204). Holding `SharedNest`s rather than `AppState`s keeps the root honest across a hot swap:
    // it reads whatever version is current, exactly as the nest's own endpoint does.
    let shared: Vec<(String, SharedNest)> = nests
        .into_iter()
        .map(|(name, mut state)| {
            // A nest served through this composition answers `/ready` and `/metrics` from `health`'s
            // per-nest counters, never the process-global aggregate - `spawn_runtime` and `mount()`
            // stamp `runtime_health` before a real nest ever reaches here. Filled in here too, only if
            // the caller left it unset, so a fixture (or any future caller) that forgets the stamp
            // cannot silently fall back to the solo path - the fault that let three fixtures in a row
            // (#292, #356, #388) prove the handler and never the wiring. `get_or_insert_with` rather
            // than an unconditional assignment: an alias mount's state is deliberately pre-stamped
            // with its *canonical* nest's name (`fan_out_aliases`), and that must survive composition
            // unchanged.
            state
                .runtime_health
                .get_or_insert_with(|| (name.clone(), health.clone()));
            (name, SharedNest::new(state))
        })
        .collect();
    let ready_nests = Arc::new(shared.clone());
    let metrics_nests = ready_nests.clone();
    let metrics_health = health.clone();
    let mut app = Router::new()
        .route("/health", get(|| async { "ok" }))
        // `GET /nests` - the roster (name, chain, registry hash, table count) across mounted nests,
        // merged per request with each nest's LIVE health (RFC-0026 §5). The static half is computed
        // once at startup; the health half must not be, or a quarantined nest would keep reporting the
        // state it had when the process booted.
        .route(
            "/nests",
            get(move || {
                let r = roster.clone();
                let h = roster_health.clone();
                async move { Json(merge_roster_health(&r, &h)) }
            }),
        )
        // `GET /metrics` at the root, so a runtime is scrapeable before anything is mounted and a
        // scrape target does not depend on which mounts exist. Every mount's `/metrics` serves this too.
        .route(
            "/metrics",
            get(move || {
                let h = metrics_health.clone();
                let n = metrics_nests.clone();
                async move { runtime_metrics(&h, &n) }
            }),
        )
        // `GET /ready` at the runtime root - the runtime-wide readiness a supervisor polls. The per-nest
        // `/ready` under `/<name>/` answers only for that nest.
        .route(
            "/ready",
            get(move || {
                let h = ready_health.clone();
                let n = ready_nests.clone();
                async move { roost_ready(&h, &n) }
            }),
        );
    for (name, nest) in shared {
        // `Router::nest` re-roots the whole per-nest router under `/<name>`, so `/lodestar/tables`,
        // `/lodestar/sql`, `/lodestar/_admin/` … all resolve to that nest's isolated state.
        app = app.nest(&format!("/{name}"), router(nest));
    }
    app
}

/// A mounts's routes behind a swappable handle (RFC-0027 slice 1).
///
/// Today a runtime's nest set is frozen at boot: `axum::Router` is composed before it is served and has
/// no insertion point afterwards, so adding or removing a nest means restarting the process - which
/// stops every *co-tenant* nest too. That makes the blast radius of a configuration change larger than
/// the blast radius of a fault, which RFC-0026 already fixed.
///
/// This holds the composed router in an [`arc_swap::ArcSwap`] and serves a thin outer router that
/// delegates every request to whatever is current. Swapping the whole composed router - rather than
/// dispatching on the first path segment by hand - keeps `Router::nest` doing the routing, so the
/// serving semantics are *identical* to the static path by construction rather than by careful
/// re-implementation. Re-composition is rare (a mount or unmount), so rebuilding is not a hot path.
///
/// **Slice 1 changes no behaviour**: nothing calls [`LiveRuntime::swap`] yet. It exists so the lifecycle
/// slices have somewhere to stand, and so the parity test can prove the indirection is free.
pub struct LiveRuntime {
    current: Arc<arc_swap::ArcSwap<Router>>,
}

impl LiveRuntime {
    pub fn new(router: Router) -> Self {
        Self {
            current: Arc::new(arc_swap::ArcSwap::from_pointee(router)),
        }
    }

    /// Replace the served routes. The next request sees the new set; requests already in flight
    /// complete against the router they started on.
    pub fn swap(&self, router: Router) {
        self.current.store(Arc::new(router));
    }

    /// The router to serve: a fallback that resolves the current composition per request.
    pub fn service(&self) -> Router {
        let current = self.current.clone();
        Router::new().fallback(move |mut req: axum::extract::Request| {
            let current = current.clone();
            async move {
                use tower::ServiceExt;
                // `nest("/usdc")` matches `/usdc` and `/usdc/x` but not `/usdc/`, which fell through
                // to an empty 404 whatever the mount's state (#1588).
                trim_trailing_slash(&mut req);
                // `Router::clone` is cheap (its state is behind an `Arc`), and `oneshot` drives this
                // request through the composed router exactly as `axum::serve` would have.
                let router = (**current.load()).clone();
                match router.oneshot(req).await {
                    Ok(resp) => resp,
                    // `Router`'s error type is `Infallible`, so this is unreachable; matching keeps it
                    // honest rather than unwrapping.
                    Err(e) => match e {},
                }
            }
        })
    }
}

fn trim_trailing_slash(req: &mut axum::extract::Request) {
    let path = req.uri().path();
    if path.len() <= 1 || !path.ends_with('/') {
        return;
    }
    let trimmed = path.trim_end_matches('/');
    let trimmed = if trimmed.is_empty() { "/" } else { trimmed };
    let rebuilt = match req.uri().query() {
        Some(q) => format!("{trimmed}?{q}"),
        None => trimmed.to_string(),
    };
    let mut parts = req.uri().clone().into_parts();
    if let Ok(pq) = rebuilt.parse() {
        parts.path_and_query = Some(pq);
        if let Ok(uri) = axum::http::Uri::from_parts(parts) {
            *req.uri_mut() = uri;
        }
    }
}

/// Merge each roster entry's live health into the startup snapshot (RFC-0026 §5). A nest that is
/// indexing carries `"health": "indexing"` and no `quarantine` key; a quarantined one carries the
/// reason, class, and retry deadline an operator needs to act on.
fn merge_roster_health(
    roster: &serde_json::Value,
    health: &crate::health::RuntimeHealth,
) -> serde_json::Value {
    let mut out = roster.clone();
    if let Some(entries) = out.get_mut("nests").and_then(|n| n.as_array_mut()) {
        for e in entries {
            let Some(name) = e.get("name").and_then(|n| n.as_str()).map(str::to_string) else {
                continue;
            };
            let (status, quarantine) = health.json_for(&name);
            if let Some(obj) = e.as_object_mut() {
                obj.insert("health".into(), json!(status));
                match quarantine {
                    Some(q) => obj.insert("quarantine".into(), q),
                    // Explicitly remove rather than leave a stale entry, so a recovered nest's roster
                    // row does not keep describing a quarantine that has since been lifted.
                    None => obj.remove("quarantine"),
                };
            }
        }
    }
    out["all_indexing"] = json!(health.all_indexing());
    out
}

/// MountTable-wide readiness (RFC-0026 §5): **200** while every mounted nest is indexing, **503** as
/// soon as any nest or cursor is quarantined **or any nest's own `/ready` would answer 503**, with
/// the offenders named.
///
/// 503-on-any-fault is the deliberately conservative choice. A supervisor should treat a
/// partly-broken mounts as not-ready and fetch a human, while the healthy nests carry on serving reads
/// to consumers who ask for them directly - readiness is advice to a supervisor, it does not gate
/// traffic. The healthy/unhealthy split stays visible per nest on `/nests` and `/<name>/ready`.
///
/// **The stall half is #1204**, and that same "advice, not a gate" reasoning is why it belongs here
/// rather than being kept out on blast-radius grounds: a stalled co-tenant costs a healthy nest
/// nothing, because nothing about this verdict stops it serving. Until 3.6.2 this consulted the
/// quarantine set alone, so `wedged`, `poll_stalled`, `initial_poll_failed`, `entities_stalled` and
/// `tip_seal_stalled` were all computed correctly per nest and none of them reached the root. A
/// runtime answered `{"quarantined":[],"ready":true}` while the `horizon` nest inside it sat 753,000
/// blocks behind on its seal for two days, on the surface that issues TAP receipts, and
/// `docs/operators.md` promised the opposite in the words an operator wires a supervisor to.
fn roost_ready(
    health: &crate::health::RuntimeHealth,
    nests: &[(String, SharedNest)],
) -> impl IntoResponse {
    let unhealthy = health.unhealthy();
    // A quarantined nest is unready and already named; it is not judged a second time on its stall
    // terms, which would report one fault twice under two vocabularies.
    let quarantined: std::collections::HashSet<&str> =
        unhealthy.iter().map(|(n, _)| n.as_str()).collect();
    // #1204: every nest's own verdict, reached with the same [`nest_readiness`] its `/<name>/ready`
    // uses. An alias mount is judged here too - its state carries its canonical nest's counters, so
    // it reports that dataset's health rather than a private, permanently-cheerful one.
    let stalled: Vec<serde_json::Value> = nests
        .iter()
        .filter(|(name, _)| !quarantined.contains(name.as_str()))
        .filter_map(|(name, nest)| {
            let v = nest_readiness(&nest.current());
            v.stalled
                .then(|| json!({"nest": name, "reasons": v.reasons()}))
        })
        .collect();
    let ready = unhealthy.is_empty() && stalled.is_empty();
    let body = json!({
        "version": env!("CARGO_PKG_VERSION"),
        "ready": ready,
        "quarantined": unhealthy.iter().map(|(n, r)| json!({"nest": n, "reason": r})).collect::<Vec<_>>(),
        "stalled": stalled,
    });
    let code = if ready {
        StatusCode::OK
    } else {
        StatusCode::SERVICE_UNAVAILABLE
    };
    (code, Json(body))
}

/// Is `o` a value an `Origin` header could actually carry (#1318)?
///
/// An origin is `scheme://host[:port]` and nothing else - no path, no query, no fragment, no
/// userinfo, no trailing slash. Anything wider is accepted by a laxer check, matches nothing at
/// request time, and surfaces as a generic browser CORS error that says nothing about which value
/// was wrong (Jules on #1384, who found `https://app.example.com/path` sailing through a check that
/// only looked at the scheme and the last character).
///
/// Deliberately not `url::Url`: that crate is optional here, gated behind the `object-store`
/// feature, and this must refuse the same values in every build. An origin is also a far narrower
/// grammar than a URL, so parsing it as one would accept more than it should.
fn check_origin(o: &str) -> Result<()> {
    let rest = o
        .strip_prefix("https://")
        .or_else(|| o.strip_prefix("http://"))
        .ok_or_else(|| {
            anyhow::anyhow!("--cors {o} is not an origin: it needs a scheme, e.g. https://{o}")
        })?;
    // `rest`, not `o`: a bare `https://` ends with a slash but its fault is the missing host, and
    // reporting the wrong one sends the operator to fix something that was never wrong.
    if rest.ends_with('/') {
        anyhow::bail!(
            "--cors {o} has a trailing slash, and an Origin header never does, so it would match \
             nothing"
        );
    }
    if let Some(bad) = rest.chars().find(|c| matches!(c, '/' | '?' | '#')) {
        anyhow::bail!(
            "--cors {o} carries a {} after the host, and an Origin header is only \
             scheme://host[:port], so it would match nothing. Use the origin alone.",
            match bad {
                '/' => "path",
                '?' => "query",
                _ => "fragment",
            }
        );
    }
    if rest.contains('@') {
        anyhow::bail!(
            "--cors {o} carries credentials, which an Origin header never does, so it would match \
             nothing"
        );
    }
    // A bracketed IPv6 literal is one host, colons and all - `http://[::1]:3000` is a perfectly
    // ordinary origin a browser will send, and splitting it on the first colon reads the host as
    // "[" and refuses it for a port it never had (Jules on #1384). A guard nobody can get through
    // is not a guard, it is a wall.
    let (host, port) = if let Some(inner) = rest.strip_prefix('[') {
        let (addr, tail) = inner.split_once(']').ok_or_else(|| {
            anyhow::anyhow!("--cors {o} opens a bracketed host and never closes it")
        })?;
        if addr.is_empty() {
            anyhow::bail!("--cors {o} names no host");
        }
        // Only the shape is checked, not the address: over-validating IPv6 is how the next false
        // refusal gets written, and a host that does not resolve is the operator's business.
        match tail {
            "" => (addr, None),
            t => match t.strip_prefix(':') {
                Some(p) => (addr, Some(p)),
                None => anyhow::bail!(
                    "--cors {o} has '{t}' after the host, which an Origin never carries"
                ),
            },
        }
    } else {
        match rest.split_once(':') {
            Some((h, p)) => (h, Some(p)),
            None => (rest, None),
        }
    };
    if host.is_empty() {
        anyhow::bail!("--cors {o} names no host");
    }
    // **An allowlist, not a list of forbidden characters** (Jules on #1384, third round on this
    // validator). Enumerating what is banned is how a guard over a growing vocabulary stays one
    // example behind its reviewer - a space slipped through twice here already. A host is either a
    // DNS name or a bracketed literal, and the DNS name's alphabet is small and closed, so state it
    // and refuse everything else.
    // A zone id lives after `%` and has its own alphabet; keeping it separate is what the fifth
    // round of this review found missing. Appending `is_ascii_alphanumeric` to the address alphabet
    // to admit `%eth0` made the whole predicate vacuous - `[zz]` and `[2001:db8::gg]` both passed -
    // which is the third time this validator has been widened by a term meant to narrow it.
    let (addr, zone) = match host.split_once('%') {
        Some((a, z)) => (a, Some(z)),
        None => (host, None),
    };
    let bad_char = if rest.starts_with('[') {
        // Inside the brackets: hex groups, `:` separators, and `.` for an embedded IPv4 tail. That
        // is the whole alphabet - still a shape check, still not a check that the address resolves.
        addr.chars()
            .find(|c| !(c.is_ascii_hexdigit() || matches!(c, ':' | '.')))
            // A zone id is an interface name: alphanumerics and the separators one may carry.
            .or_else(|| {
                zone.and_then(|z| {
                    z.chars()
                        .find(|c| !(c.is_ascii_alphanumeric() || matches!(c, '.' | '-' | '_')))
                })
            })
    } else {
        // A `%` has no meaning outside brackets, so `addr` is the whole host here and `zone` being
        // `Some` is itself the fault - caught by `%` failing the DNS alphabet below.
        host.chars()
            .find(|c| !(c.is_ascii_alphanumeric() || matches!(c, '.' | '-' | '_')))
    };
    if let Some(c) = bad_char {
        let named = if c == ' ' {
            "a space".to_string()
        } else {
            format!("'{c}'")
        };
        anyhow::bail!(
            "--cors {o} has {named} in its host, which an Origin header cannot carry, so it would \
             match nothing"
        );
    }
    if rest.starts_with('[') && zone.is_some_and(str::is_empty) {
        anyhow::bail!("--cors {o} has an empty zone id after '%'");
    }
    if let Some(p) = port {
        if p.is_empty() || !p.bytes().all(|b| b.is_ascii_digit()) {
            anyhow::bail!("--cors {o} has '{p}' where a port number should be");
        }
    }
    Ok(())
}

/// Build the CORS layer for `--cors` (#1318), or `None` when the flag was not given.
///
/// **Off by default, and that is the whole point of the shape.** A nest sends no
/// `Access-Control-Allow-Origin` unless an operator asks for one, so nothing about the default
/// surface changes and deleting this feature would cost a self-hoster nothing. What it replaces is
/// the README's honest but unhelpful advice to put a reverse proxy in front, which is a wall for
/// someone with a weekend.
///
/// `*` allows any origin; otherwise each value is matched exactly. `*` may not be combined with
/// named origins - the result is either redundant or a mistake, and there is no reading of it that
/// is obviously right, so it is refused rather than guessed at.
///
/// Methods are `GET`, `POST` and `OPTIONS`. **`POST` is not an exception to the read-only surface,
/// it is part of it**: the RFC-0053 GraphQL routes - `/graphql`, `/subgraphs/id/{id}`,
/// `/subgraphs/name/{*name}` - take a query in a `POST` body, which is how every GraphQL client on
/// the web speaks. A `GET`-only list reads as the safe choice and is in fact the broken one: the
/// browser preflights `Access-Control-Request-Method: POST`, is refused, and never sends the query,
/// so the flag appears to work everywhere except the surface a front end is most likely to want
/// (Jules on #1384). Nothing here grants a *write*: the runtime's lifecycle routes are writes, and
/// [`bind_and_serve`] keeps every `/_admin` path out of this layer (#1642).
///
/// Request headers are mirrored rather than set to `*`: equally safe with no credentials, better
/// supported, and it is what admits the `content-type: application/json` a GraphQL POST carries. The
/// `/sql` guards already bound what a browser can cost the box, so this grants reach, not resources.
pub fn cors_layer(origins: &[String]) -> Result<Option<tower_http::cors::CorsLayer>> {
    use axum::http::{HeaderValue, Method};
    use tower_http::cors::{AllowHeaders, AllowOrigin, CorsLayer};

    if origins.is_empty() {
        return Ok(None);
    }
    let wildcard = origins.iter().any(|o| o == "*");
    if wildcard && origins.len() > 1 {
        anyhow::bail!(
            "--cors * cannot be combined with named origins (got: {})",
            origins.join(", ")
        );
    }
    let allow = if wildcard {
        AllowOrigin::any()
    } else {
        let mut parsed = Vec::with_capacity(origins.len());
        for o in origins {
            // Every refusal here exists because the failure it prevents is silent: a value that
            // cannot match any `Origin` header produces a browser error reading "CORS is broken"
            // rather than "that value was wrong", and the operator has no way to tell which.
            check_origin(o)?;
            parsed.push(
                o.parse::<HeaderValue>()
                    .with_context(|| format!("--cors {o} is not a valid header value"))?,
            );
        }
        AllowOrigin::list(parsed)
    };
    Ok(Some(
        CorsLayer::new()
            .allow_origin(allow)
            .allow_methods([Method::GET, Method::POST, Method::OPTIONS])
            .allow_headers(AllowHeaders::mirror_request()),
    ))
}

/// Apply `--cors` to everything but `/_admin`, a nest's own included: a runtime's lifecycle routes
/// are writes, and no origin is ever granted them (#1642).
fn cors_outside_admin(app: Router, layer: tower_http::cors::CorsLayer) -> Router {
    use tower::ServiceExt;
    let open = app.clone().layer(layer);
    Router::new().fallback_service(tower::service_fn(move |req: axum::extract::Request| {
        let admin = req.uri().path().split('/').any(|s| s == "_admin");
        if admin { app.clone() } else { open.clone() }.oneshot(req)
    }))
}

/// Bind `listen` and serve `app` until a shutdown signal - the shared tail of [`run`]/[`run_runtime`].
///
/// `cors` is [`cors_layer`]'s result: `None` leaves the app exactly as it was composed, which is
/// what every caller that has not been given `--cors` passes.
pub async fn bind_and_serve(
    listen: &str,
    app: Router,
    cors: Option<tower_http::cors::CorsLayer>,
) -> Result<()> {
    let app = match cors {
        Some(layer) => cors_outside_admin(app, layer),
        None => app,
    };
    let listener = tokio::net::TcpListener::bind(listen)
        .await
        .with_context(|| format!("cannot bind {listen}"))?;
    tracing::info!("API live on http://{listen}  (try GET /  and  /metrics)");
    // A loud one-liner when bound off-localhost: the guards bound *how much*, but *who* is the
    // operator's gateway's job - never expose this straight to the internet without one.
    if !is_localhost(listen) {
        tracing::warn!(
            "listening on {listen} (not localhost): the /sql surface is guarded (timeout + row cap \
             + {SQL_MAX_CONCURRENCY} concurrent) but has NO authentication - put a gateway in front \
             before exposing it publicly. See docs/operators.md."
        );
    }
    serve_until(listener, app, shutdown_signal(), SHUTDOWN_GRACE).await;
    tracing::info!("shutdown signal received; API stopped");
    Ok(())
}

/// [`bind_and_serve`] for a caller that bound the listener itself, to read its address first.
pub async fn serve_bound(
    listener: tokio::net::TcpListener,
    app: Router,
    cors: Option<tower_http::cors::CorsLayer>,
) -> Result<()> {
    let app = match cors {
        Some(layer) => cors_outside_admin(app, layer),
        None => app,
    };
    tracing::info!("API live on http://{}", listener.local_addr()?);
    serve_until(listener, app, shutdown_signal(), SHUTDOWN_GRACE).await;
    tracing::info!("shutdown signal received; API stopped");
    Ok(())
}

/// Serve `app` on `listener` until `shutdown` resolves, then drain connections for at most `grace`.
async fn serve_until(
    mut listener: tokio::net::TcpListener,
    app: Router,
    shutdown: impl std::future::Future<Output = ()>,
    grace: Duration,
) {
    let (stop, stopping) = tokio::sync::watch::channel(false);
    let app = app
        .layer(axum::Extension(Stopping(stopping)))
        .layer(tower::util::MapRequestLayer::new(host_from_authority));
    // Not `axum::serve`: it gives hyper no timer, so the header-read timeout never fires (#1660).
    let mut http = hyper::server::conn::http1::Builder::new();
    http.timer(hyper_util::rt::TokioTimer::new())
        .header_read_timeout(HEADER_READ_TIMEOUT);
    // Graceful shutdown on SIGTERM/SIGINT: in-flight requests drain, then this returns so the
    // caller can abort the ingest task(s) (progress is checkpointed, so a restart resumes cleanly).
    let graceful = hyper_util::server::graceful::GracefulShutdown::new();
    let mut shutdown = std::pin::pin!(shutdown);
    let mut conns = tokio::task::JoinSet::new();
    loop {
        let (io, _) = tokio::select! {
            conn = axum::serve::Listener::accept(&mut listener) => conn,
            Some(_) = conns.join_next(), if !conns.is_empty() => continue,
            _ = &mut shutdown => break,
        };
        let service = hyper_util::service::TowerToHyperService::new(app.clone());
        let conn = graceful.watch(http.serve_connection(hyper_util::rt::TokioIo::new(io), service));
        conns.spawn(async move {
            if let Err(e) = conn.await {
                tracing::trace!("connection ended: {e:#}");
            }
        });
    }
    // A graceful drain only waits for responses to finish, and a stream must be told to (#1964).
    stop.send_replace(true);
    if tokio::time::timeout(grace, graceful.shutdown())
        .await
        .is_err()
    {
        tracing::warn!("connections still open {grace:?} after shutdown; dropping them");
    }
    conns.shutdown().await;
}

/// Set on every request by [`serve_until`]; resolves once the server has started stopping, which is
/// what ends a response that would otherwise never finish.
#[derive(Clone)]
struct Stopping(tokio::sync::watch::Receiver<bool>);

impl Stopping {
    async fn wait(mut self) {
        let _ = self.0.wait_for(|stopping| *stopping).await;
    }
}

/// The built-in admin UI (RFC-0010 Part A) - a single self-contained page, embedded in the binary.
const ADMIN_HTML: &str = include_str!("admin.html");

/// `GET /_admin/` - serve the admin UI when enabled, else 404 (it's off, or the bind is public with
/// no token). The page is read-only and talks only to this same-origin API; no external requests.
#[derive(Deserialize)]
struct AdminQuery {
    token: Option<String>,
}

/// Whether a request may reach the admin surface (SEC-5; header support is audit L7).
///
/// With no token configured (a localhost bind) the surface is open to requests addressed to loopback.
/// A configured token is required per request on every bind, and may arrive either way:
///
/// - **`?token=…`** - the original form, and it has to stay: the admin UI's own live updates use
///   `EventSource`, which cannot set request headers. It is already constant-time compared and
///   same-origin.
/// - **`Authorization: Bearer …`** - preferred wherever the caller *can* set headers, because a query
///   string leaks into proxy access logs, shell history and `Referer`, and a token in a log file
///   outlives the request that carried it.
///
/// The scheme is matched case-insensitively per RFC 7235; the token compare is constant-time either
/// way, so a timing side-channel cannot recover it byte-by-byte.
/// Whether a request carries the admin credential, as `?token=` or `Authorization: Bearer`.
///
/// Shared with the runtime's lifecycle routes (RFC-0027 §5) so mount/unmount are gated by exactly the
/// same rule as the admin UI - one credential, one comparison, no second auth concept to get subtly
/// wrong. `None` means a localhost bind with no token, open to loopback-addressed requests only.
pub fn token_ok(
    required: Option<&str>,
    token: Option<&str>,
    headers: &axum::http::HeaderMap,
) -> bool {
    let Some(required) = required else {
        return addressed_to_loopback(headers);
    };
    if token.is_some_and(|t| ct_eq(t, required)) {
        return true;
    }
    headers
        .get(axum::http::header::AUTHORIZATION)
        .and_then(|v| v.to_str().ok())
        .and_then(|v| {
            let (scheme, rest) = v.split_once(' ')?;
            scheme.eq_ignore_ascii_case("bearer").then(|| rest.trim())
        })
        .is_some_and(|t| ct_eq(t, required))
}

fn admin_authorized(state: &AppState, q: &AdminQuery, headers: &axum::http::HeaderMap) -> bool {
    token_ok(state.admin_token.as_deref(), q.token.as_deref(), headers)
}

/// An HTTP/2 request names its target in `:authority` and an absolute-form one in its URI, and the
/// target overrides `Host` (RFC 9112 §3.2.2), so the URI's authority is what [`token_ok`] must see.
pub fn host_from_authority<B>(mut req: axum::http::Request<B>) -> axum::http::Request<B> {
    if let Some(v) = req
        .uri()
        .authority()
        .and_then(|a| axum::http::HeaderValue::from_str(a.as_str()).ok())
    {
        req.headers_mut().insert(axum::http::header::HOST, v);
    }
    req
}

/// A DNS-rebinding page reaches 127.0.0.1 same-origin under its own hostname, so the open surface
/// answers only a loopback `Host` and, when present, a loopback `Origin` (#2136). A request with no
/// `Host` is refused: every server path fills it from the URI authority when one was sent.
fn addressed_to_loopback(headers: &axum::http::HeaderMap) -> bool {
    let loopback = |authority: &str| {
        let host = match authority.strip_prefix('[') {
            Some(v6) => v6.split(']').next().unwrap_or(""),
            None => authority.rsplit_once(':').map_or(authority, |(h, _)| h),
        };
        host.eq_ignore_ascii_case("localhost")
            || host
                .parse::<std::net::IpAddr>()
                .is_ok_and(|ip| ip.is_loopback())
    };
    let header = |name| {
        headers
            .get(name)
            .map(|v: &axum::http::HeaderValue| v.to_str().unwrap_or(""))
    };
    header(axum::http::header::HOST).is_some_and(loopback)
        && header(axum::http::header::ORIGIN).is_none_or(|o| {
            o.split_once("://")
                .is_some_and(|(_, authority)| loopback(authority))
        })
}

async fn admin_index(
    State(s): State<AppState>,
    Query(q): Query<AdminQuery>,
    headers: axum::http::HeaderMap,
) -> impl IntoResponse {
    // Not reachable over HTTP any more - [`router`] does not mount this handler when the surface is
    // disabled - and kept deliberately: the handler states its own precondition, so wiring it up from
    // somewhere new cannot serve the admin UI by omission.
    if !s.admin_enabled {
        return (StatusCode::NOT_FOUND, "admin UI disabled").into_response();
    }
    if !admin_authorized(&s, &q, &headers) {
        return (
            StatusCode::UNAUTHORIZED,
            "admin UI requires a valid ?token= or Authorization: Bearer token",
        )
            .into_response();
    }
    (
        [(axum::http::header::CONTENT_TYPE, "text/html; charset=utf-8")],
        ADMIN_HTML,
    )
        .into_response()
}

/// `GET /nest` - static nest metadata for the admin UI's Nest tab (RFC-0010 Part A).
async fn nest(State(s): State<AppState>) -> impl IntoResponse {
    Json((*s.nest_info).clone())
}

/// `GET /api` - the Etherscan-shaped account surface over the nest's watched addresses (RFC-0063).
async fn etherscan_api(
    State(s): State<AppState>,
    Query(q): Query<std::collections::HashMap<String, String>>,
) -> Json<Value> {
    let answer = tokio::task::spawn_blocking(move || {
        crate::address_history::respond(s.address_history.as_ref(), &q)
    })
    .await
    .unwrap_or_else(|e| {
        serde_json::json!({ "status": "0", "message": "NOTOK", "result": format!("Error! {e}") })
    });
    Json(answer)
}

/// The nest's capability *shape* (RFC-0025): which capability-gated MCP surfaces are live, so the
/// `nuthatch mcp` bridge advertises only the tools that will actually return data - never, say,
/// `top_balances` on a Uniswap-pool nest, which would answer `{"count":0}` and read to an agent as
/// "the index is empty". Derived from what the nest *has* - a transfer-shaped decoder (the same gate
/// the balance view uses) and whether RFC-0008 compliance is configured - so the surface stays honest
/// by construction, with no operator knob.
async fn shape(State(s): State<AppState>) -> Json<Value> {
    let transfers = s.tables.iter().any(|t| t.is_transfer_shaped());
    let compliance = s.threshold.is_some()
        || s.velocity_threshold.is_some()
        || s.dir.join(crate::labels::LABELS_DIR).is_dir()
        || s.dir.join(crate::lists::LISTS_DIR).is_dir();
    Json(json!({ "transfers": transfers, "compliance": compliance }))
}

/// Whether `listen` binds only the loopback interface.
pub fn is_localhost(listen: &str) -> bool {
    let host = listen.rsplit_once(':').map(|(h, _)| h).unwrap_or(listen);
    matches!(host, "127.0.0.1" | "::1" | "localhost" | "[::1]")
}

/// Resolves when the process is asked to stop - SIGTERM (systemd/Docker) or Ctrl-C.
async fn shutdown_signal() {
    let ctrl_c = async {
        let _ = tokio::signal::ctrl_c().await;
    };
    #[cfg(unix)]
    let term = async {
        if let Ok(mut s) = tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate())
        {
            s.recv().await;
        }
    };
    #[cfg(not(unix))]
    let term = std::future::pending::<()>();
    tokio::select! {
        _ = ctrl_c => {}
        _ = term => {}
    }
    crate::analytics::interrupt_for_shutdown();
}

/// Middleware: bump the request counter, then pass through.
async fn count_request(
    req: axum::extract::Request,
    next: axum::middleware::Next,
) -> axum::response::Response {
    crate::metrics::METRICS.inc_http();
    next.run(req).await
}

/// `GET /metrics` - Prometheus text exposition (RFC-0005 §6).
/// The runtime root's `/metrics`: the process registry, the health series and every mount's entity series.
fn runtime_metrics(
    health: &crate::health::RuntimeHealth,
    nests: &[(String, SharedNest)],
) -> impl IntoResponse {
    let mut body = crate::metrics::METRICS.render();
    body.push_str(&health.render_metrics());
    for (_, nest) in nests {
        body.push_str(&entity_metrics(&nest.current()));
    }
    (
        [(
            axum::http::header::CONTENT_TYPE,
            "text/plain; version=0.0.4",
        )],
        body,
    )
}

async fn metrics_handler(State(s): State<AppState>) -> impl IntoResponse {
    let mut body = crate::metrics::METRICS.render();
    // In a runtime, append the health series (RFC-0026 §5) so an operator can alert on "anything
    // quarantined" without polling `/nests`. Like the existing per-nest series, these describe the
    // whole mounts regardless of which nest's `/metrics` is scraped.
    if let Some((_, health)) = &s.runtime_health {
        body.push_str(&health.render_metrics());
    }
    body.push_str(&entity_metrics(&s));
    (
        [(
            axum::http::header::CONTENT_TYPE,
            "text/plain; version=0.0.4",
        )],
        body,
    )
}

/// The authored-entity series (RFC-0041). Everything here is already computed for `/ready`; this is
/// the same answers where a scrape can reach them.
///
/// **There were none until now**, which is why a two-day live run had to poll `/ready` and parse
/// JSON to find out whether the entity was keeping up. The four questions an operator has about a
/// maintained relation - is it current, how big is it, is it stuck, is it dead - had no series
/// between them, so neither alerting nor a dashboard could ask.
///
/// Labelled by entity name and emitted per nest, so a runtime hosting several says which is which.
/// Empty when the nest declares no entities, rather than a block of zeroes for a feature not in use.
/// The entity series, as `(name, help)`, in the order they are emitted.
///
/// **A table rather than six literals, because a gate reads it.**
/// `skill_refs::authored_files_only_mention_real_metrics` builds its canonical set from
/// `Metrics::render()`, which is where every series lived until these were appended alongside it -
/// so a name documented in the builder skill was reported as not existing. The emitter below and
/// that gate now read the same list, which is the only arrangement in which they cannot drift.
pub const ENTITY_SERIES: &[(&str, &str)] = &[
    (
        "nuthatch_entity_applied_through",
        "Last block folded into this maintained relation.",
    ),
    (
        "nuthatch_entity_current",
        "1 when the relation has caught up with the dataset head.",
    ),
    (
        "nuthatch_entity_rows",
        "Rows the maintained relation currently holds.",
    ),
    (
        "nuthatch_entity_faulted",
        "1 when the circuit has stopped. Terminal.",
    ),
    (
        "nuthatch_entity_unavailable",
        "1 when the relation holds no answer and is not served.",
    ),
    (
        "nuthatch_entity_seconds_since_progress",
        "Seconds since this relation's watermark last moved.",
    ),
    (
        "nuthatch_entity_state_bytes",
        "Bytes the entity's DBSP circuit held at its last sample.",
    ),
];

fn entity_metrics(s: &AppState) -> String {
    if s.entities.is_empty() {
        return String::new();
    }
    let nest = s
        .nest_info
        .get("name")
        .and_then(Value::as_str)
        .unwrap_or("");
    let head = dataset_head(s);
    let now = crate::metrics::now_unix();

    let mut out = String::new();
    out.push_str(
        "# HELP nuthatch_entity_applied_through Last block folded into this maintained relation.\n\
         # TYPE nuthatch_entity_applied_through gauge\n",
    );
    for e in s.entities.iter() {
        out.push_str(&format!(
            "nuthatch_entity_applied_through{{nest=\"{nest}\",entity=\"{}\"}} {}\n",
            e.name(),
            e.applied_through()
        ));
    }
    out.push_str(
        "# HELP nuthatch_entity_current 1 when the relation has caught up with the dataset head.\n\
         # TYPE nuthatch_entity_current gauge\n",
    );
    for e in s.entities.iter() {
        out.push_str(&format!(
            "nuthatch_entity_current{{nest=\"{nest}\",entity=\"{}\"}} {}\n",
            e.name(),
            u8::from(e.is_current(head))
        ));
    }
    out.push_str(
        "# HELP nuthatch_entity_rows Rows the maintained relation currently holds.\n\
         # TYPE nuthatch_entity_rows gauge\n",
    );
    for e in s.entities.iter() {
        out.push_str(&format!(
            "nuthatch_entity_rows{{nest=\"{nest}\",entity=\"{}\"}} {}\n",
            e.name(),
            e.len()
        ));
    }
    // Faulted and unavailable are different failures and an operator responds to them differently:
    // a fault is terminal and the nest is quarantined behind it, while unavailable means the
    // relation holds no answer and is not being served. Two series, not one `healthy`.
    out.push_str(
        "# HELP nuthatch_entity_faulted 1 when the circuit has stopped. Terminal.\n\
         # TYPE nuthatch_entity_faulted gauge\n",
    );
    for e in s.entities.iter() {
        out.push_str(&format!(
            "nuthatch_entity_faulted{{nest=\"{nest}\",entity=\"{}\"}} {}\n",
            e.name(),
            u8::from(e.fault().is_some())
        ));
    }
    out.push_str(
        "# HELP nuthatch_entity_unavailable 1 when the relation holds no answer and is not served.\n\
         # TYPE nuthatch_entity_unavailable gauge\n",
    );
    for e in s.entities.iter() {
        out.push_str(&format!(
            "nuthatch_entity_unavailable{{nest=\"{nest}\",entity=\"{}\"}} {}\n",
            e.name(),
            u8::from(e.unavailable().is_some())
        ));
    }
    // Seconds since the watermark last moved, which is what `entity_wedged` judges on. A duration
    // threshold on a legitimately-slow catch-up either fires on healthy nests or never fires, so the
    // series an operator alerts on is progress, not lag.
    out.push_str(
        "# HELP nuthatch_entity_seconds_since_progress Seconds since this relation's watermark last moved.\n\
         # TYPE nuthatch_entity_seconds_since_progress gauge\n",
    );
    for e in s.entities.iter() {
        let progress = e.last_progress();
        out.push_str(&format!(
            "nuthatch_entity_seconds_since_progress{{nest=\"{nest}\",entity=\"{}\"}} {}\n",
            e.name(),
            if progress == 0 {
                0
            } else {
                now.saturating_sub(progress)
            }
        ));
    }
    // Sampled on the circuit's thread at most every NUTHATCH_ENTITY_STATE_SAMPLE_SECS (#1834); an
    // entity not yet sampled has no line rather than a zero.
    out.push_str(
        "# HELP nuthatch_entity_state_bytes Bytes the entity's DBSP circuit held at its last sample.\n\
         # TYPE nuthatch_entity_state_bytes gauge\n",
    );
    for e in s.entities.iter() {
        if let Some(bytes) = e.state_bytes() {
            out.push_str(&format!(
                "nuthatch_entity_state_bytes{{nest=\"{nest}\",entity=\"{}\"}} {bytes}\n",
                e.name()
            ));
        }
    }
    out
}

/// No successful source poll within this many seconds ⇒ stalled. The tip loop polls every ~2-3 s even
/// when caught up, so a minute-plus of silence means every RPC endpoint is unreachable, not idleness.
const READINESS_STALL_SECS: u64 = 90;

/// Stalled = polled successfully at least once (`last_poll != 0`) but not within `threshold` seconds. A
/// never-polled node (`last_poll == 0`) is *starting up*, not stalled - it gets grace, but the grace is
/// bounded by `started_at` (#510): a pool that has been dead since before the very first poll must
/// eventually report stalled too, or `/ready` stays a false "ready:true" forever. `started_at == 0`
/// (not yet stamped, e.g. an un-stamped test fixture) falls back to the old unconditional grace.
fn poll_stalled(last_poll: u64, started_at: u64, now: u64, threshold: u64) -> bool {
    let since = if last_poll != 0 {
        last_poll
    } else {
        started_at
    };
    since != 0 && now.saturating_sub(since) > threshold
}

/// A source which has not answered successfully is allowed startup grace only until its first
/// failed poll. At that point there is evidence of an unusable endpoint pool, rather than merely an
/// in-flight connection, and returning 200 would make a bad `--rpc` look healthy.
fn initial_poll_failed(last_poll: u64, poll_failed: bool) -> bool {
    last_poll == 0 && poll_failed
}

/// No advance in `last_block` within this many seconds means the cursor is wedged (#578). Same value as
/// `READINESS_STALL_SECS` - a cursor that is actually getting somewhere, backfilling or at tip, advances
/// far more often than this on any chain with sub-minute blocks.
const READINESS_PROGRESS_STALL_SECS: u64 = 90;

/// How long a `--seal-direct` pass may go without its watermark moving before `/ready` calls it
/// wedged (#846).
///
/// **Deliberately an order of magnitude looser than the tip thresholds above**, and for a reason
/// that is not timidity: seal-direct fetches, decodes and writes a whole window of history per step,
/// against whatever archive endpoint the operator has, and a single wide window on a slow provider
/// legitimately takes minutes. The tip cursor's 90s says "a poll should have returned by now"; this
/// says "no window has completed in a quarter of an hour, which is not slowness".
///
/// It is a threshold on *progress*, not on duration - a pass may run for six hours and stay ready
/// throughout, as long as it keeps sealing.
const READINESS_SEAL_STALL_SECS: u64 = 900;

/// How long the **tip path's** seal may go without advancing the watermark before `/ready` calls it
/// stalled (#1199).
///
/// Twelve hours. Every *registered* chain's `seal_span` is sized to about six hours of its block
/// time, so on those a healthy nest cuts a segment at least that often, and twice the span leaves
/// room for a finality boundary that sulks or a poll interval widened under RFC-0040. An unregistered
/// chain gets `chains::DEFAULT_SEAL_SPAN`, which is no particular duration (36 hours on Sepolia), so
/// this clock is necessary but not sufficient: `nest_readiness` also requires the seal lag to exceed
/// twice the span before it calls the seal stalled (#1341).
///
/// **Not `READINESS_SEAL_STALL_SECS` (15 minutes).** That one judges an *active bulk backfill*,
/// which has work in hand and is never legitimately idle. A tip-path seal waits for finality and for
/// rows as a matter of course, and fifteen minutes would refuse service on every sparse nest on the
/// box.
const READINESS_TIP_SEAL_STALL_SECS: u64 = 43_200;

/// Has an active seal-direct pass stopped sealing?
///
/// Mirrors [`progress_stalled`] but takes no `lag` guard, and the difference is the point. A tip
/// cursor that is caught up has legitimately nothing to do, which is why `lag == 0` exempts it. An
/// *active* seal-direct pass by definition has not reached its target yet - `end_seal_direct` is
/// what marks arrival - so there is no such thing as a legitimately idle one.
fn seal_direct_stalled(last_seal_progress: u64, started_at: u64, now: u64, threshold: u64) -> bool {
    let since = if last_seal_progress != 0 {
        last_seal_progress
    } else {
        started_at
    };
    since != 0 && now.saturating_sub(since) > threshold
}

/// Wedged = the cursor is behind tip (`lag > 0`) and `last_block` moved at least once
/// (`last_progress != 0`) but not within `threshold` seconds, even while [`poll_stalled`] says the
/// source is still reachable. This is the case `poll_stalled` can't see: a source poll (tip fetch)
/// can succeed on schedule while `process_window` (`indexer.rs:3437`) holds the cursor's position
/// rather than seal a window it can't safely process - correct behaviour on its own, but not "ready"
/// while it lasts. Same bounded-grace shape as `poll_stalled` (#510): a cursor that has never yet
/// made progress is judged from `started_at`, so one wedged since block one eventually reports
/// unready too, not "just starting up" forever.
///
/// The `lag > 0` guard is load-bearing on its own: without it, a cursor that is caught up to tip -
/// which stops stamping `last_progress` the moment it arrives, because `set_last_block` only stamps
/// on an actual value change - reports wedged `threshold` seconds after catching up, even though it
/// is behaving exactly as intended. Same failure mode for a fixed-range nest (`end_block:
/// Option<u64>`, `src/subgraph_import.rs:68`) once its range completes: `last_block` is final by
/// design, and without the guard `/ready` would report unready forever.
fn progress_stalled(
    last_progress: u64,
    started_at: u64,
    now: u64,
    threshold: u64,
    lag: u64,
) -> bool {
    if lag == 0 {
        return false;
    }
    let since = if last_progress != 0 {
        last_progress
    } else {
        started_at
    };
    since != 0 && now.saturating_sub(since) > threshold
}

/// Readiness for a supervisor (k8s-style). `/health` is liveness - the process is up and serving, and
/// stays a plain `200 "ok"`. `/ready` answers "is it *healthy*" - still reaching the chain **and making
/// progress**: **200** with a status body when fresh, **503** when the source has stopped answering
/// ([`poll_stalled`]) or the cursor has stopped advancing while the source answers fine
/// ([`progress_stalled`], #578). A just-started node that has done neither yet is *not* stalled (grace).
/// Whether an entity that is behind has stopped advancing.
///
/// Its own function because the ways this is wrong are all off-by-one-ish and all silent: an entity
/// that has never folded a batch is waiting rather than wedged, and one level with the head is not
/// behind at all. Either, read wrong, produces a permanently-unready nest that is working fine.
///
/// A nest that has indexed nothing needs no guard of its own: its head is zero, and `applied < 0` is
/// unreachable for a `u64`. An explicit `head > 0` here survived every mutation because nothing could
/// reach it - armour that cannot be hit, in front of a case that cannot happen, is a third thing for
/// a reader to reason about and a place for a wrong belief to live.
fn entity_wedged(applied: u64, head: u64, progress: u64, now: u64) -> bool {
    applied < head && progress != 0 && now.saturating_sub(progress) > READINESS_PROGRESS_STALL_SECS
}

/// How each authored entity is doing, and whether any of them makes this nest unready (#866).
///
/// §5.1: *"A catching-up entity is reported as such; it never serves a plausible partial relation as
/// current."* Being behind is therefore **not** unready - it is the ordinary state during backfill
/// and after a definition change, and it can last hours. What is unready is an entity that has
/// stopped: faulted outright, or behind and no longer advancing.
///
/// Judged on **progress**, exactly as the seal pass is since #846. A duration threshold on a
/// legitimately-slow catch-up either fires on healthy nests or never fires at all, and the version
/// that never fires is the one that ships.
fn entity_readiness(s: &AppState, head: u64, now: u64) -> (Value, bool) {
    let mut stalled = false;
    let entities: Vec<Value> = s
        .entities
        .iter()
        .map(|e| {
            // #932: one acquisition for the pair, so `rows` and `applied_through` cannot describe
            // two different moments.
            let (row_count, applied) = e.len_and_watermark();
            let through = applied.through;
            let fault = e.fault();
            let unavailable = e.unavailable();
            let behind = through < head;
            let progress = e.last_progress();
            // A nest that has indexed nothing yet has no head to be behind, so an entity at zero is
            // waiting rather than wedged.
            let wedged = entity_wedged(through, head, progress, now);
            // Unavailable is unready too. It is not a fault - nothing died - but an entity holding
            // no answer must not be reachable behind a 200, which is the whole of §5.1's "never
            // serves a plausible partial relation as current" applied to the case where there is no
            // relation at all.
            if fault.is_some() || wedged || unavailable.is_some() {
                stalled = true;
            }
            with_offchain(
                json!({
                    "name": e.name(),
                    "applied_through": through,
                    "current": !behind,
                    "catching_up": behind && fault.is_none() && !wedged && unavailable.is_none(),
                    "rows": row_count,
                    "faulted": fault.is_some(),
                    "fault": fault,
                    "wedged": wedged,
                    "unavailable": unavailable,
                    "seconds_since_progress": (progress != 0).then(|| now.saturating_sub(progress)),
                }),
                e,
                &applied,
            )
        })
        .collect();
    (Value::Array(entities), stalled)
}

/// How the fold checkpoint writer is doing (RFC-0059 §5), and whether it makes this nest unready.
///
/// Being behind is not unready: a fresh nest, or one whose folds were edited, walks its whole sealed
/// history one window at a time, and that lag is the ordinary state for as long as it takes. What
/// is unready is a writer that has stopped on an error, because from then on every read pays a
/// window that only grows. A duration threshold on progress would call a large first window a
/// fault; the entity rule (`entity_wedged`) tolerates that because an entity's batches are small.
#[cfg(feature = "folds")]
fn fold_readiness(s: &AppState, now: u64) -> (Value, bool) {
    let Some(w) = &s.folds else {
        return (Value::Null, false);
    };
    let st = w.status();
    let faulted = st.fault.is_some();
    (
        json!({
            "checkpointed_through": st.checkpointed_through,
            "target": st.target,
            "lag_blocks": st.lag_blocks,
            "catching_up": st.lag_blocks > 0 && !faulted,
            "faulted": faulted,
            "fault": st.fault,
            "seconds_since_progress": (st.last_progress != 0).then(|| now.saturating_sub(st.last_progress)),
        }),
        faulted,
    )
}

/// RFC-0045 §6's second guarantee: rows that can be re-checked given the snapshots, but not
/// re-derived from chain.
const SNAPSHOT_REPRODUCIBLE: &str = "snapshot";

/// Label an entity that reads an offchain table as reproducible by snapshot, and name the snapshots
/// its rows were applied through (#1437). The label follows from what the entity reads, not from
/// what it has applied, so it is there before the first snapshot lands and nothing turns it off.
/// Both are absent, not null, for an entity that reads no offchain table.
fn with_offchain(
    mut v: Value,
    entity: &crate::entity_view::EntityView,
    applied: &crate::entity_view::Applied,
) -> Value {
    if let Some(obj) = v.as_object_mut() {
        if !entity.offchain_tables().is_empty() {
            obj.insert("reproducibility".into(), json!(SNAPSHOT_REPRODUCIBLE));
        }
        if let Some(offchain) = applied.offchain_json() {
            obj.insert("offchain".into(), offchain);
        }
    }
    v
}

/// The offchain views an answer read directly, each with the snapshots behind it.
fn read_offchain_json(read: &std::collections::BTreeMap<String, Vec<String>>) -> Option<Value> {
    (!read.is_empty()).then(|| {
        read.iter()
            .map(|(view, hashes)| {
                let mark = crate::entity_view::OffchainMark::of(hashes);
                (
                    view.clone(),
                    json!({ "snapshots": mark.snapshots, "latest": mark.latest }),
                )
            })
            .collect::<serde_json::Map<_, _>>()
            .into()
    })
}

/// One nest's readiness verdict, and the counters it was reached from.
///
/// **Extracted from [`ready`] so the runtime root can reach the same answer (#1204).** Before this
/// there were two definitions of "unready": the five terms below, and the runtime root's, which was
/// membership of the quarantine set and nothing else. Two definitions drift, and these two had -
/// `docs/operators.md` promised the root answered 200 "only when every cursor and nest is indexing"
/// while a nest 753,000 blocks behind on its seal left the root reporting ready. One function now,
/// called by both surfaces.
///
/// **Nothing here touches the store.** Every term is an in-memory counter (`METRICS.nest`) or an
/// entity handle on the `AppState`, which is what makes it cheap enough for the root to evaluate per
/// nest on a surface a supervisor polls every few seconds.
pub(crate) struct NestReadiness {
    pub stalled: bool,
    pub wedged: bool,
    pub initial_failure: bool,
    pub seal_stalled: bool,
    pub tip_seal_stalled: bool,
    pub seal_faulted: bool,
    pub seal_fault: Option<(u64, String)>,
    pub entities_stalled: bool,
    pub cursor_stalled: bool,
    pub tip: u64,
    pub last: u64,
    pub sealed: u64,
    pub lag: u64,
    pub now: u64,
    pub age: Option<u64>,
    pub last_poll: u64,
    pub last_seal_progress: u64,
    pub seal_direct_active: bool,
    pub seal_direct_origin: u64,
    pub seal_direct_completed: u64,
    pub seal_direct_target: u64,
    pub seal_direct_fetched: u64,
    pub seal_direct_concurrency: Option<(u64, u64)>,
    pub ipfs_window_deadline: Option<u64>,
    pub fetch_window: u64,
    pub entities: Value,
    #[cfg(feature = "folds")]
    pub folds: Value,
    #[cfg(feature = "folds")]
    pub folds_stalled: bool,
}

impl NestReadiness {
    /// The terms that took this nest unready, named. The runtime root reports these beside a
    /// quarantine so an operator learns *which* nest and *why* from one request, rather than being
    /// told the runtime is unwell and left to poll every nest by name to find out which.
    pub fn reasons(&self) -> Vec<&'static str> {
        let mut out = Vec::new();
        if self.cursor_stalled {
            out.push(if self.initial_failure {
                "initial_poll_failed"
            } else if self.wedged {
                "wedged"
            } else {
                "poll_stalled"
            });
        }
        if self.seal_stalled {
            out.push("seal_direct_stalled");
        }
        if self.tip_seal_stalled {
            out.push("tip_seal_stalled");
        }
        if self.seal_faulted {
            out.push("seal_faulted");
        }
        if self.entities_stalled {
            out.push("entities_stalled");
        }
        #[cfg(feature = "folds")]
        if self.folds_stalled {
            out.push("fold_writer_faulted");
        }
        out
    }
}

/// Compute [`NestReadiness`] for one nest. See that type for why it is not inline in [`ready`].
pub(crate) fn nest_readiness(s: &AppState) -> NestReadiness {
    use crate::metrics::{now_unix, METRICS};
    let nest = s
        .runtime_health
        .as_ref()
        .map(|(name, _)| METRICS.nest(name));
    let (last_poll, tip, last, sealed, started_at, last_progress, poll_failed, seal_direct) =
        match &nest {
            Some(m) => (
                m.last_poll_ok(),
                m.tip(),
                m.last_block(),
                m.sealed_through(),
                m.started_at(),
                m.last_progress(),
                m.poll_failed(),
                (
                    m.seal_direct_active(),
                    m.seal_direct_origin(),
                    m.seal_direct_completed(),
                    m.seal_direct_target(),
                    m.last_seal_progress(),
                    m.seal_direct_fetched(),
                    m.fetch_window(),
                    m.seal_direct_concurrency(),
                    m.ipfs_window_deadline(),
                ),
            ),
            None => (
                METRICS.last_poll_ok(),
                METRICS.tip_height(),
                METRICS.last_block(),
                METRICS.sealed_through_val(),
                METRICS.started_at(),
                METRICS.last_progress(),
                METRICS.poll_failed(),
                (
                    METRICS.seal_direct_active(),
                    METRICS.seal_direct_origin(),
                    METRICS.seal_direct_completed(),
                    METRICS.seal_direct_target(),
                    METRICS.last_seal_progress(),
                    METRICS.seal_direct_fetched(),
                    METRICS.fetch_window(),
                    METRICS.seal_direct_concurrency(),
                    METRICS.ipfs_window_deadline(),
                ),
            ),
        };
    let (
        seal_direct_active,
        seal_direct_origin,
        seal_direct_completed,
        seal_direct_target,
        last_seal_progress,
        seal_direct_fetched,
        fetch_window,
        seal_direct_concurrency,
        ipfs_window_deadline,
    ) = seal_direct;
    let now = now_unix();
    let age = (last_poll != 0).then(|| now.saturating_sub(last_poll));
    // Before its first commit a cursor stands just below its start block, not at block 0: a nest
    // waiting for finality to reach its start is not the whole chain behind.
    let position = if last == 0 {
        s.store
            .get_meta(crate::indexer::START_BLOCK_KEY)
            .ok()
            .flatten()
            .and_then(|v| v.parse::<u64>().ok())
            .map_or(0, |start| start.saturating_sub(1))
    } else {
        last
    };
    let lag = tip.saturating_sub(position);
    // A wedge is judged against where the cursor may go: under `--finality-only` that is the finality
    // boundary, and one waiting there for the next finalized block is caught up (#1575).
    let ceiling = nest
        .as_ref()
        .map_or_else(|| METRICS.ceiling(), |m| m.ceiling());
    let target = if ceiling == 0 { tip } else { ceiling };
    let wedged = !seal_direct_active
        && progress_stalled(
            last_progress,
            started_at,
            now,
            s.freshness
                .stall_threshold_secs(READINESS_PROGRESS_STALL_SECS),
            target.saturating_sub(position),
        );
    let initial_failure = initial_poll_failed(last_poll, poll_failed);
    // #846: `seal_direct_active` used to suppress every term above with nothing put in its place, so
    // a pass that had died reported ready indefinitely. Suppressing the *tip* thresholds during a
    // bulk seal is still right - #807 was correct that a working history pass is not a stalled
    // cursor - but the pass now has to answer for its own progress instead of being exempt.
    let seal_stalled = seal_direct_active
        && seal_direct_stalled(
            last_seal_progress,
            started_at,
            now,
            READINESS_SEAL_STALL_SECS,
        );
    // An entity that has stopped makes the nest unready whatever the cursor is doing: §5.2 calls
    // serving frozen derived state as healthy "a lie with a pleasant HTTP status", and a cursor
    // polling happily while an entity is dead is exactly that lie.
    let (entities, entities_stalled) = entity_readiness(s, last, now);
    #[cfg(feature = "folds")]
    let (folds, folds_stalled) = fold_readiness(s, now);
    // #1199: the tip path's own seal clock. `seal_stalled` above judges only a bulk backfill, so a
    // nest whose *ordinary* sealing had stopped answered `ready: true` indefinitely - measured at
    // 739,192 blocks behind on a cursor that was sitting at tip.
    //
    // Four guards, and each earns its place:
    // - `!s.cursorless`, because a frozen archive seals nothing by design (#1025);
    // - `!seal_direct_active`, because that pass owns the clock and `seal_stalled` already judges it;
    // - `last > sealed`, the analogue of `wedged`'s `lag > 0`. A seal caught up to the cursor has
    //   nothing to do and stops stamping, exactly as a cursor at tip stops stamping `last_progress`.
    //   Without it, a fixed-range nest (`end_block`) whose range has completed and sealed reports
    //   unready for ever - the same trap the `wedged` guard exists to avoid;
    // - a seal lag past twice the span. A sparse nest's only seal is the span cut, which is not due
    //   until the frontier is a whole span past the watermark, so quiet hours alone are not a stall.
    //   On Sepolia's default span that cut is 36 hours away, and the clock alone called a healthy
    //   nest dead for two thirds of every cycle (#1341).
    let tip_seal_stalled = !s.cursorless
        && !seal_direct_active
        && last > sealed
        && last.saturating_sub(sealed) > s.seal_span.saturating_mul(2)
        && seal_direct_stalled(
            last_seal_progress,
            started_at,
            now,
            READINESS_TIP_SEAL_STALL_SECS,
        );
    // **A role with no cursor is judged on what it serves, not on a poll it never makes** (#1025).
    //
    // `initial_failure`, `poll_stalled` and `wedged` are all statements about a cursor. `nuthatch
    // serve` deliberately has none - `serve_role`'s own comment says "No `Source` is ever polled on
    // this role" - so all three are permanently true for it, and `/ready` answered `stalled:true`
    // continuously on a nest answering queries correctly.
    //
    // What remains meaningful is the derived state: an entity that has stopped is still a lie with a
    // pleasant HTTP status, whoever is driving the cursor. That term is kept.
    let cursor_stalled = if s.cursorless {
        false
    } else {
        !seal_direct_active
            && (initial_failure
                || poll_stalled(
                    last_poll,
                    started_at,
                    now,
                    s.freshness.stall_threshold_secs(READINESS_STALL_SECS),
                )
                || wedged)
    };
    // #2262: a seal that errors is not holding by design, so neither the span guard nor the twelve
    // hours above apply to it.
    let seal_fault = nest.as_ref().and_then(|m| m.seal_fault());
    let seal_faulted = seal_fault.as_ref().is_some_and(|(since, _)| {
        now.saturating_sub(*since) > s.freshness.stall_threshold_secs(READINESS_SEAL_STALL_SECS)
    });
    let stalled =
        seal_stalled || tip_seal_stalled || seal_faulted || entities_stalled || cursor_stalled;
    #[cfg(feature = "folds")]
    let stalled = stalled || folds_stalled;
    NestReadiness {
        stalled,
        wedged,
        initial_failure,
        seal_stalled,
        tip_seal_stalled,
        seal_faulted,
        seal_fault,
        entities_stalled,
        cursor_stalled,
        tip,
        last,
        sealed,
        lag,
        now,
        age,
        last_poll,
        last_seal_progress,
        seal_direct_active,
        seal_direct_origin,
        seal_direct_completed,
        seal_direct_target,
        seal_direct_fetched,
        seal_direct_concurrency,
        ipfs_window_deadline,
        fetch_window,
        entities,
        #[cfg(feature = "folds")]
        folds,
        #[cfg(feature = "folds")]
        folds_stalled,
    }
}

async fn ready(State(s): State<AppState>) -> impl IntoResponse {
    // In a runtime, this nest answers for ITSELF (RFC-0026 §5): a consumer polling `/lodestar/ready`
    // must not be told the runtime is unwell because some unrelated co-tenant is quarantined - nor told
    // all is well when *this* nest is the one that is frozen.
    if let Some((name, health)) = &s.runtime_health {
        if let Some(q) = health.status(name) {
            return (
                StatusCode::SERVICE_UNAVAILABLE,
                Json(json!({
                    "version": env!("CARGO_PKG_VERSION"),
                    "ready": false,
                    "quarantined": true,
                    "kind": q.kind,
                    "class": q.class,
                    "reason": q.reason,
                    "since_unixtime": q.since_unixtime,
                    "next_retry_unixtime": q.next_retry_unixtime,
                })),
            )
                .into_response();
        }
    }
    // Answer from **this nest's** counters when it is one of several in a runtime. The process-global
    // gauges are shared by every cursor, so in a multichain runtime whichever cursor polled last wins -
    // and this endpoint then reports another chain's block heights. Observed live in a two-chain mounts:
    // the mainnet nest reported `tip: 488677305` (Arbitrum) while mainnet was at 25,632,906, alongside
    // a mainnet `sealed_through`. One body, two chains, no way for an operator to tell.
    //
    // A solo `dev` has exactly one nest feeding the globals, so both paths agree there.
    let NestReadiness {
        stalled,
        wedged,
        initial_failure,
        seal_stalled,
        tip_seal_stalled,
        seal_faulted,
        seal_fault,
        entities_stalled,
        cursor_stalled: _,
        tip,
        last,
        sealed,
        lag,
        now,
        age,
        last_poll,
        last_seal_progress,
        seal_direct_active,
        seal_direct_origin,
        seal_direct_completed,
        seal_direct_target,
        seal_direct_fetched,
        seal_direct_concurrency,
        ipfs_window_deadline,
        fetch_window,
        entities,
        #[cfg(feature = "folds")]
        folds,
        #[cfg(feature = "folds")]
            folds_stalled: _,
    } = nest_readiness(&s);
    let body = json!({
        "version": env!("CARGO_PKG_VERSION"),
        "ready": !stalled,
        "stalled": stalled,
        "wedged": wedged,
        "initial_poll_failed": initial_failure,
        // Null rather than 0 for a cursorless role: it has no tip and no lag, and **0 is a value**
        // an operator or a dashboard will read as "exactly at tip" (the #1020 lesson, on the other
        // surface). Absent is honest; zero is a claim.
        "tip": if s.cursorless { serde_json::Value::Null } else { tip.into() },
        "last_block": last,
        "lag_blocks": if s.cursorless { serde_json::Value::Null } else { lag.into() },
        "cursorless": s.cursorless,
        // RFC-0040 §4: a deliberately stale cursor says so here. `lag_blocks` above is then the
        // distance the operator chose, and the stall thresholds behind `ready` are scaled to
        // `poll_interval_secs`, so a quiet five-minute cursor is not reported as a dead one.
        "freshness": {
            "mode": s.freshness.mode(),
            "poll_interval_secs": s.freshness.poll_interval.as_secs(),
            "poll_interval_source": s.freshness.poll_interval_source(),
        },
        "sealed_through": sealed,
        // **How far the sealed watermark trails what the cursor has indexed** (#1199).
        //
        // `lag_blocks` above describes *following*, which is a different question, and on both Graph
        // protocol nests it read 0 while the seal sat 739,192 blocks back. A stalled seal and a
        // healthy one were indistinguishable through this endpoint, so Lodestar's health check passed
        // for days while every tattler receipt pinned a watermark two days stale.
        //
        // **A fact, not a verdict.** This deliberately does not feed `stalled`. `maybe_seal` holds a
        // finalized range until SEAL_DIRECT_BATCH rows accumulate, so on a sparse nest the lag grows
        // without bound *by construction* and any threshold worth having would refuse service on
        // nests that are answering correctly. The verdict belongs with the fix to that holding rule,
        // not ahead of it.
        //
        // Null rather than 0 when nothing has sealed: `sealed_through` defaults to 0, so the
        // subtraction would report a nest's entire indexed history as lag during its first minutes.
        // `sealed_through: 0` beside a null lag reads as "nothing sealed yet"; a 0 there would be a
        // claim that the seal is caught up (the #1020 lesson, on this surface).
        "seal_lag_blocks": if s.cursorless || sealed == 0 {
            serde_json::Value::Null
        } else {
            last.saturating_sub(sealed).into()
        },
        "last_poll_unixtime": last_poll,
        "seconds_since_poll": age,
        "seal_direct_active": seal_direct_active,
        "seal_direct_origin": seal_direct_origin,
        "seal_direct_completed": seal_direct_completed,
        // The pass's fetch position (#1169). `seal_direct_completed` is what a restart resumes from;
        // this is how far fetching has run ahead of it, which is exactly the work a restart redoes.
        // They were one number until a restart on the gns nest redid 47.6M blocks the number had
        // called done.
        "seal_direct_fetched": seal_direct_fetched,
        // The cursor's current getLogs span (#1170): a backfill crawling at ten blocks a window
        // after a rate-limited hour is visible here, not only in its ETA.
        "fetch_window_blocks": fetch_window,
        "seal_direct_target": seal_direct_target,
        // #1399: one `--rpc` quietly runs the pass a window at a time, which a startup warning alone
        // did not make visible. Null when no seal-direct pass recorded it.
        "seal_direct_concurrency": seal_direct_concurrency.map(|(requested, effective)| json!({
            "requested": requested,
            "effective": effective,
            "capped_by": (effective < requested).then_some("single_rpc_endpoint"),
        })),
        // How long a seal-direct window waits for one ipfs document, `0` for no limit. A document
        // given up on is absent from the sealed segment (#1410), so this is the operator's trade.
        // #1410: documents given up on, by either path, and so absent from sealed history. Listed at
        // `/ipfs/gave-up`.
        "ipfs_gave_up_documents": s
            .store
            .count_meta_with_prefix(crate::ipfs_resolve::GAVE_UP_PREFIX)
            .ok(),
        "seal_direct_ipfs_window_deadline_secs": if s.cursorless {
            serde_json::Value::Null
        } else {
            ipfs_window_deadline.into()
        },
        "seal_direct_stalled": seal_stalled,
        // The tip path's verdict, reported beside the backfill's so an operator can tell which term
        // took the nest unready without reading this source (#1199).
        "tip_seal_stalled": tip_seal_stalled,
        "seal_faulted": seal_faulted,
        // No longer gated on `seal_direct_active`: the tip path stamps this clock too now, and the
        // number was null on precisely the nests that needed it.
        "seconds_since_seal_progress": (last_seal_progress != 0)
            .then(|| now.saturating_sub(last_seal_progress)),
        "entities": entities,
        "entities_stalled": entities_stalled,
        // A fact, not a verdict: one broken view must not take a serving nest out of rotation (#1653).
        "views_failed": crate::analytics::view_failures(&s.dir)
            .into_iter()
            .map(|(file, view, error)| json!({ "file": file, "view": view, "error": error }))
            .collect::<Vec<_>>(),
    });
    #[cfg(feature = "folds")]
    let body = {
        let mut body = body;
        if !folds.is_null() {
            body["folds"] = folds;
        }
        body
    };
    let mut body = body;
    // #2262: the tip seal's error while every attempt fails, absent while it succeeds.
    if let Some((since, reason)) = seal_fault {
        body["seal_fault"] = json!({ "reason": reason, "since_unixtime": since });
    }
    // RFC-0062: absent unless the nest declares a maintained view.
    if crate::maintained::is_declared(&s.dir) {
        body["maintained"] = maintained_status(&s.dir);
    }
    let code = if stalled {
        StatusCode::SERVICE_UNAVAILABLE
    } else {
        StatusCode::OK
    };
    (code, Json(body)).into_response()
}

/// Each maintained view's copies and counters. A fact, not a verdict: a view with no copy still
/// answers, from its definition.
fn maintained_status(dir: &std::path::Path) -> Value {
    use std::sync::atomic::Ordering::Relaxed;
    crate::maintained::report(dir)
        .into_iter()
        .map(|(view, s, copies, bytes)| {
            json!({
                "view": view,
                "copies": copies,
                "copy_bytes": bytes,
                "hits": s.hits.load(Relaxed),
                "fallbacks": s.fallbacks.load(Relaxed),
                "builds": s.builds.load(Relaxed),
                "build_failures": s.build_failures.load(Relaxed),
                "last_build_ms": s.last_build_ms.load(Relaxed),
                "last_error": s.last_error(),
            })
        })
        .collect()
}

/// The live status payload - nest identity, tip/sealed watermarks, and the IVM view counts. Built once
/// here and served two ways: as the `GET /` JSON body, and as each frame of the `/_admin/events` SSE
/// stream, so the polled and pushed surfaces can never disagree.
fn summary_value(s: &AppState) -> Value {
    let count = s.store.count().unwrap_or(0);
    let last_block = s.store.get_meta("last_block").ok().flatten();
    json!({
        "name": "nuthatch",
        "chain": s.chain,
        "address": s.address,
        "entities": count,
        "last_block": last_block,
        "sealed_through": s.store.get_meta("sealed_through").ok().flatten(),
        "holders": s.balances.holders(),
        "exposure_entries": s.exposure.entries(),
        "velocity_buckets": s.velocity.entries(),
        "alert_outbox": s.store.outbox_len(),
        "tables": s.tables.len(),
        // What this nest's inputs hash to now (#1420): the package identity, and the coarser one that
        // decides whether a dataset may be adopted.
        "nid": s.nest_info["nid"],
        "data_identity": s.nest_info["data_identity"],
        "publish": publish_summary(s),
        "views": ["balances (IVM)", "exposure (IVM)", "velocity (IVM)"],
        "endpoints": [
            "/health",
            "/tables",
            "/table/{name}?limit=100",
            "/entities?limit=100",
            "/ipfs/gave-up?limit=100",
            "/entity/{block:012}-{log_index:06}",
            "/sql?q=SELECT count(*) FROM \"<alias>__<event>\"",
            "/balances?limit=100",
            "/balance/{address}",
            "/exposure/{address}",
            "/flags?kind=threshold|velocity",
        ],
    })
}

/// The admin row for a nest that mirrors itself (RFC-0052 §3.8), `null` for one that does not.
fn publish_summary(s: &AppState) -> Value {
    // The publisher reports where the cursor does: the route in a runtime (#1415), else the nest's name.
    let key = match &s.runtime_health {
        Some((route, _)) => Some(route.as_str()),
        None => s.nest_info["name"].as_str(),
    };
    let Some(m) = key.and_then(|k| crate::metrics::METRICS.nest_if_known(k)) else {
        return Value::Null;
    };
    let Some(target) = m.publish_target() else {
        return Value::Null;
    };
    json!({
        "target": target,
        "sealed_through": m.publish_sealed_through(),
        "lag_blocks": m.publish_lag_blocks(),
        "last_success_unixtime": (m.publish_last_success() != 0).then(|| m.publish_last_success()),
        "dead_letter": m.publish_dead_letter(),
    })
}

async fn summary(State(s): State<AppState>) -> impl IntoResponse {
    Json(summary_value(&s))
}

/// How often the `/_admin/events` SSE stream pushes a fresh status frame. Matches the 2 s cadence the
/// admin UI used to poll at, so the live view feels identical - just server-pushed, not client-pulled.
const SSE_INTERVAL: Duration = Duration::from_secs(2);

/// `GET /_admin/events` - a Server-Sent Events stream of the status summary, pushed every
/// [`SSE_INTERVAL`], so the admin UI updates live without polling (RFC-0010 Part A). Gated exactly like
/// the admin page: 404 when the admin UI is disabled, and off-localhost it requires a valid `?token=`.
/// Each frame is byte-identical to `GET /` (both call [`summary_value`]). Purely a serving-layer read -
/// nothing here touches the ingest/decode data path.
async fn admin_events(
    State(s): State<AppState>,
    Query(q): Query<AdminQuery>,
    headers: axum::http::HeaderMap,
    stopping: Option<axum::Extension<Stopping>>,
) -> impl IntoResponse {
    // Same as [`admin_index`]: unreachable while the route is unmounted, kept as the handler's own
    // precondition rather than a rule that lives only in the router.
    if !s.admin_enabled {
        return (StatusCode::NOT_FOUND, "admin UI disabled").into_response();
    }
    if !admin_authorized(&s, &q, &headers) {
        return (
            StatusCode::UNAUTHORIZED,
            "admin events require a valid ?token= or Authorization: Bearer token",
        )
            .into_response();
    }
    // `unfold` carries the state and a `first` flag: emit immediately, then once per interval. Cloning
    // `AppState` per frame is cheap - its fields are `Arc`/handle types, not owned data.
    let stream = stream::unfold((s, true), |(s, first)| async move {
        if !first {
            tokio::time::sleep(SSE_INTERVAL).await;
        }
        let ev = Event::default().data(summary_value(&s).to_string());
        Some((Ok::<_, std::convert::Infallible>(ev), (s, false)))
    });
    let stream = futures::StreamExt::take_until(stream, async move {
        match stopping {
            Some(axum::Extension(stopping)) => stopping.wait().await,
            None => std::future::pending().await,
        }
    });
    // Keep-alive comment every 15 s so an idle proxy doesn't close a quiet stream between frames.
    Sse::new(stream)
        .keep_alive(KeepAlive::new().interval(Duration::from_secs(15)))
        .into_response()
}

/// The files a nest is authored in, as the admin UI's Config tab shows them. `views/*.sql` beside.
const ADMIN_CONFIG_FILES: &[&str] = &[
    "nuthatch.toml",
    "semantic.toml",
    "entities.toml",
    crate::allowlist::CEILING_FILE,
];

/// Config text with credentials taken out: every URL cut to `scheme://host`, and the value of any
/// `secret`/`token`/`password`/`api_key` key replaced. On the text rather than the parsed config so
/// comments survive, and so a URL in a comment is cut too.
fn redact_config_text(text: &str) -> String {
    const SECRET_KEYS: &[&str] = &["secret", "token", "password", "api_key", "apikey"];
    let mut out = String::with_capacity(text.len());
    for line in text.split_inclusive('\n') {
        let body = line.trim_end_matches(['\n', '\r']);
        let ending = &line[body.len()..];
        let key = body.split_once('=').map(|(k, _)| k.trim().to_lowercase());
        let secret_key = key.is_some_and(|k| {
            !k.starts_with('#') && SECRET_KEYS.iter().any(|needle| k.contains(needle))
        });
        if secret_key {
            let (k, _) = body.split_once('=').expect("a key was found");
            out.push_str(k);
            out.push_str("= \"<redacted>\"");
        } else {
            out.push_str(&redact_urls(body));
        }
        out.push_str(ending);
    }
    out
}

fn redact_urls(line: &str) -> String {
    let mut out = String::with_capacity(line.len());
    let mut rest = line;
    while let Some(at) = rest.find("://") {
        let scheme_start = rest[..at]
            .rfind(|c: char| !(c.is_ascii_alphanumeric() || matches!(c, '+' | '.' | '-')))
            .map_or(0, |i| i + 1);
        let after = &rest[at + 3..];
        let end = after
            .find(|c: char| {
                c.is_whitespace() || matches!(c, '"' | '\'' | '`' | ',' | ')' | ']' | '>')
            })
            .unwrap_or(after.len());
        let authority_end = after[..end].find(['/', '?', '#']).unwrap_or(end);
        let authority = &after[..authority_end];
        let host = authority.rsplit('@').next().unwrap_or(authority);
        out.push_str(&rest[..scheme_start]);
        out.push_str(&rest[scheme_start..at + 3]);
        out.push_str(host);
        if authority_end + 1 < end {
            out.push_str("/…");
        }
        rest = &after[end..];
    }
    out.push_str(rest);
    out
}

/// `GET /_admin/config` - what this nest was authored as and is running with, for the admin UI.
/// Gated like the page. File text is read per request, so an edit shows before the restart that
/// applies it; `running` is this process.
async fn admin_config(
    State(s): State<AppState>,
    Query(q): Query<AdminQuery>,
    headers: axum::http::HeaderMap,
) -> impl IntoResponse {
    if !s.admin_enabled {
        return (StatusCode::NOT_FOUND, "admin UI disabled").into_response();
    }
    if !admin_authorized(&s, &q, &headers) {
        return (StatusCode::UNAUTHORIZED, "admin token required").into_response();
    }
    let dir = s.dir.clone();
    let files = tokio::task::spawn_blocking(move || {
        let mut names: Vec<String> = ADMIN_CONFIG_FILES.iter().map(|f| f.to_string()).collect();
        if let Ok(entries) = std::fs::read_dir(dir.join("views")) {
            let mut views: Vec<String> = entries
                .flatten()
                .filter_map(|e| e.file_name().into_string().ok())
                .filter(|n| n.ends_with(".sql"))
                .map(|n| format!("views/{n}"))
                .collect();
            views.sort();
            names.extend(views);
        }
        names
            .into_iter()
            .filter_map(|name| {
                let text = std::fs::read_to_string(dir.join(&name)).ok()?;
                Some(json!({ "name": name, "text": redact_config_text(&text) }))
            })
            .collect::<Vec<_>>()
    })
    .await
    .unwrap_or_default();
    Json(json!({
        "files": files,
        "running": {
            "version": env!("CARGO_PKG_VERSION"),
            "chain": s.chain,
            "cursorless": s.cursorless,
            "seal_span": s.seal_span,
            "seal_table_floor": crate::seal::SEAL_TABLE_FLOOR,
            "sql": {
                "max_concurrency": sql_max_concurrency(),
                "max_queued": SQL_MAX_QUEUED,
                "max_rows": SQL_MAX_ROWS,
                "max_query_len": SQL_MAX_QUERY_LEN,
                "max_hot_rows": s.sql_max_hot_rows,
                "max_hot_bytes": s.sql_max_hot_bytes,
                "max_named_scan_bytes": s.sql_max_named_scan_bytes,
            },
        },
    }))
    .into_response()
}

#[derive(Deserialize)]
struct AdminStorageQuery {
    token: Option<String>,
    table: Option<String>,
}

/// `GET /_admin/storage` - the sealed catalogue per table: segment count, rows, bytes on disk and
/// block range. `?table=` adds that table's segments, newest first. Gated like the page.
async fn admin_storage(
    State(s): State<AppState>,
    Query(q): Query<AdminStorageQuery>,
    headers: axum::http::HeaderMap,
) -> impl IntoResponse {
    if !s.admin_enabled {
        return (StatusCode::NOT_FOUND, "admin UI disabled").into_response();
    }
    let auth = AdminQuery { token: q.token };
    if !admin_authorized(&s, &auth, &headers) {
        return (StatusCode::UNAUTHORIZED, "admin token required").into_response();
    }
    let dir = s.dir.clone();
    let wanted = q.table;
    let built = tokio::task::spawn_blocking(move || -> Result<Value> {
        let manifest = crate::seal::load_manifest(&dir)?;
        let size = |seg: &crate::seal::Segment| {
            std::fs::metadata(crate::seal::segment_path(&dir, &seg.file, &seg.hash))
                .map(|m| m.len())
                .ok()
        };
        let mut tables = Vec::with_capacity(manifest.tables.len());
        let mut detail = Value::Null;
        for (table, segments) in &manifest.tables {
            let mut bytes = 0u64;
            // A file the catalogue names and the disk does not hold is the finding, so count it.
            let mut missing = 0usize;
            let sizes: Vec<Option<u64>> = segments.iter().map(size).collect();
            for size in &sizes {
                match size {
                    Some(n) => bytes += n,
                    None => missing += 1,
                }
            }
            tables.push(json!({
                "table": table,
                "segments": segments.len(),
                "provisional": segments.iter().filter(|seg| seg.provisional).count(),
                "rows": segments.iter().map(|seg| seg.rows as u64).sum::<u64>(),
                "bytes": bytes,
                "missing_files": missing,
                "from_block": segments.iter().map(|seg| seg.from_block).min(),
                "to_block": segments.iter().map(|seg| seg.to_block).max(),
            }));
            if wanted.as_deref() == Some(table.as_str()) {
                let mut rows: Vec<Value> = segments
                    .iter()
                    .zip(&sizes)
                    .map(|(seg, size)| {
                        json!({
                            "hash": seg.hash,
                            "from_block": seg.from_block,
                            "to_block": seg.to_block,
                            "rows": seg.rows,
                            "bytes": size,
                            "provisional": seg.provisional,
                            "writer_profile": seg.writer_profile,
                        })
                    })
                    .collect();
                rows.reverse();
                detail = json!({ "table": table, "segments": rows });
            }
        }
        Ok(json!({
            "manifest_version": manifest.manifest_version,
            "tables": tables,
            "detail": detail,
        }))
    })
    .await;
    match built {
        Ok(Ok(body)) => Json(body).into_response(),
        Ok(Err(e)) => (
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(json!({ "error": format!("{e:#}") })),
        )
            .into_response(),
        Err(e) => (
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(json!({ "error": e.to_string() })),
        )
            .into_response(),
    }
}

/// List every table and its columns (the decoded data model).
async fn tables(State(s): State<AppState>) -> impl IntoResponse {
    Json(json!({ "count": s.tables.len(), "tables": &*s.tables }))
}

#[derive(Deserialize)]
struct TableQuery {
    limit: Option<usize>,
    from_block: Option<u64>,
    to_block: Option<u64>,
}

#[cfg(all(test, feature = "graph"))]
thread_local! {
    static GRAPH_SQL_CALLS: std::cell::Cell<usize> = const { std::cell::Cell::new(0) };
    static GRAPH_PERMITS: std::cell::Cell<usize> = const { std::cell::Cell::new(0) };
    static GRAPH_CLOCK_SKEW: std::cell::Cell<Option<Duration>> =
        const { std::cell::Cell::new(None) };
    static GRAPH_BYTE_CAP: std::cell::Cell<Option<usize>> = const { std::cell::Cell::new(None) };
    static GRAPH_LAST_LIMIT: std::cell::Cell<usize> = const { std::cell::Cell::new(0) };
    static GRAPH_GUARD_TIMEOUT: std::cell::Cell<Option<Duration>> =
        const { std::cell::Cell::new(None) };
}

#[cfg(feature = "graph")]
fn graph_sql_root(name: &str) -> bool {
    !matches!(name, "__schema" | "__type" | "_meta")
}

/// One clock for every root. A fresh instant per root is how one request runs for hours (#1658).
#[cfg(feature = "graph")]
fn graph_remaining(started: std::time::Instant) -> Duration {
    let elapsed = started.elapsed();
    #[cfg(all(test, feature = "graph"))]
    let elapsed = elapsed.saturating_add(GRAPH_CLOCK_SKEW.with(|c| c.get()).unwrap_or_default());
    SQL_TIMEOUT.saturating_sub(elapsed)
}

/// The enriched schema document (RFC-0016 §2): the composition of registry **structure**, the
/// authored **meaning** from `semantic.toml`, the derived **footguns**, and live **coverage** (the
/// hot/cold seam as numbers). Assembled per call from this running nest - the MCP `schema` tool
/// relays it, so an agent reads *this* nest's data model, not a static string. Plain text.
/// The Graph-compatible GraphQL surface (RFC-0053 S1 #1265, S2 #1266).
///
/// Three things behind one path. **Introspection** first, because a generated client fetches the
/// schema and validates against it before it will send anything useful. **`_meta`**, answered from
/// the nest's own head rather than compiled, since a nest runs no mapping and so has no indexing
/// error to report. Everything else goes through [`crate::graph_query`], which lowers what it can
/// lower exactly and **refuses the rest by name in the Graph error envelope** - a client can read an
/// envelope, and a dropped `where` clause returns more rows than were asked for.
///
/// The generated schema comes from `graph/schema.graphql` in the nest, written there by `port-emit`.
/// A nest that was not produced from a subgraph has no such file and this endpoint says so rather
/// than inventing a schema.
#[cfg(feature = "graph")]
async fn graph_graphql(
    State(s): State<AppState>,
    Json(body): Json<serde_json::Value>,
) -> impl IntoResponse {
    let query = body
        .get("query")
        .and_then(|q| q.as_str())
        .unwrap_or_default()
        .to_string();
    // **`operationName` is read, not ignored.** A document may carry several operations and every
    // generated client sends the name of the one it wants; refusing the document outright refused
    // requests graph-node answers (Jules on #1282). An empty or absent value means "no name given".
    let operation_name = body
        .get("operationName")
        .and_then(|n| n.as_str())
        .filter(|n| !n.is_empty())
        .map(str::to_string);
    // `variables` is how every generated client passes its arguments. A value this dialect cannot
    // represent is left unbound rather than coerced, so the operation is refused by variable name.
    let vars: std::collections::BTreeMap<String, crate::graph_query::Value> = body
        .get("variables")
        .and_then(|v| v.as_object())
        .map(|o| {
            o.iter()
                // **A supplied null is bound, not dropped.** `filter_map` used to discard it, so a
                // variable the client did set re-surfaced as `unbound variable` - a refusal naming the
                // wrong cause, and one a client cannot act on (Jules on #1282). `from_json` now carries
                // null through as `Value::Null`, and the lowering refuses it by name.
                .filter_map(|(k, v)| {
                    crate::graph_query::Value::from_json(v).map(|v| (k.clone(), v))
                })
                .collect()
        })
        .unwrap_or_default();

    let path = s.dir.join("graph").join("schema.graphql");
    let Ok(text) = std::fs::read_to_string(&path) else {
        return (
            StatusCode::OK,
            Json(serde_json::json!({"errors":[{"message":
                "this nest carries no Graph schema; run `nuthatch port-emit` against the \
                 subgraph source to write graph/schema.graphql"}]})),
        );
    };
    let schema = match crate::graph_schema::parse(&text) {
        Ok(sc) => sc,
        Err(e) => {
            return (
                StatusCode::OK,
                Json(serde_json::json!({"errors":[{"message":
                    format!("graph/schema.graphql does not parse: {e}")}]})),
            )
        }
    };

    // The operation is parsed **before** anything is routed. Deciding by searching the raw text for
    // `__schema` meant a filter value of `"__schema"` - a perfectly ordinary thing to store in a
    // `hooks` column - was answered with the schema document instead of rows (Jules on #1282). A root
    // field name is a structural fact and a string literal is not, so read the structure.
    let mut roots = match crate::graph_query::parse_named(&query, &vars, operation_name.as_deref())
    {
        Ok(r) => r,
        Err(e) => return (StatusCode::OK, Json(gql_error(&e.to_string()))),
    };
    // RFC-0060 §5.6: read per request, so a default build must not read it at all. Refusing it at
    // load does not cover a file added while the node runs.
    let history = if cfg!(feature = "graph") {
        match crate::graph_history::Policy::load(&path.with_file_name("history.toml")) {
            Ok(policy) => policy,
            Err(e) => return (StatusCode::OK, Json(gql_error(&e.to_string()))),
        }
    } else {
        None
    };
    let mut historical_blocks = std::collections::BTreeMap::new();
    let mut history_fence = None;
    if let Some(policy) = &history {
        let Some(generation) = s.store.write_generation() else {
            return (
                StatusCode::OK,
                Json(gql_error(
                    "historical read cannot obtain a stable store generation; retry",
                )),
            );
        };
        history_fence = Some((generation, s.store.sealed_through()));
        let now = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_secs();
        let head = match graph_history_head(&s, policy, now) {
            Ok(head) => head,
            Err(e) => return (StatusCode::OK, Json(gql_error(&e.to_string()))),
        };
        for root in &mut roots {
            if root.name.starts_with("__") {
                continue;
            }
            // `_meta` and entity reads share one data bound. graph-node accepts a numeric metadata
            // selector between retained checkpoints: its `_Block_` carries the requested number,
            // with `hash` and `timestamp` null because there is no exact checkpoint. Hash selectors
            // still require a retained canonical checkpoint.
            let selected =
                policy.select_data_block(s.store.as_ref(), &head, root.args.get("block"));
            let block = match selected {
                Ok(block) => block,
                Err(e) => return (StatusCode::OK, Json(gql_error(&e.to_string()))),
            };
            historical_blocks.insert(root.key.clone(), block);
            root.args.remove("block");
        }
    }
    if let Err(e) = crate::graph_query::operation_bounds(&schema, &roots) {
        return (StatusCode::OK, Json(gql_error(&e.to_string())));
    }
    // Taken once for the document. Re-acquiring per root resets the 30s clock (#1658).
    let started = std::time::Instant::now();
    let _operation_permit = if roots.iter().any(|r| graph_sql_root(&r.name)) {
        let permit = match Arc::clone(&s.sql_gate).try_acquire_owned() {
            Ok(p) => p,
            Err(_) => {
                let Some(_slot) = QueueSlot::take(&s.sql_queued) else {
                    return (
                        StatusCode::OK,
                        Json(gql_error("server busy: too many concurrent SQL queries")),
                    );
                };
                match tokio::time::timeout(
                    SQL_ADMISSION_WAIT,
                    Arc::clone(&s.sql_gate).acquire_owned(),
                )
                .await
                {
                    Ok(Ok(p)) => p,
                    _ => {
                        return (
                            StatusCode::OK,
                            Json(gql_error("server busy: too many concurrent SQL queries")),
                        );
                    }
                }
            }
        };
        #[cfg(all(test, feature = "graph"))]
        GRAPH_PERMITS.with(|c| c.set(c.get() + 1));
        Some(permit)
    } else {
        None
    };
    let mut data = serde_json::Map::new();
    #[cfg(all(test, feature = "graph"))]
    let byte_cap = GRAPH_BYTE_CAP
        .with(|c| c.get())
        .unwrap_or(crate::engine::SQL_MAX_RESULT_BYTES);
    #[cfg(not(all(test, feature = "graph")))]
    let byte_cap = crate::engine::SQL_MAX_RESULT_BYTES;
    let mut bytes_left = byte_cap;
    // Rendered at most once, and only if a root asks for it.
    let doc = if roots
        .iter()
        .any(|r| matches!(r.name.as_str(), "__schema" | "__type"))
    {
        match crate::graph_schema::introspection::render(&schema) {
            Ok(d) => Some(d),
            Err(e) => return (StatusCode::OK, Json(gql_error(&e.to_string()))),
        }
    } else {
        None
    };
    let no_types = Vec::new();
    let types = doc
        .as_ref()
        .and_then(|d| d["__schema"]["types"].as_array())
        .unwrap_or(&no_types);
    // **Every root field is answered.** Routing on `any(… == "__schema")` and returning early meant
    // `{ __schema { queryType { name } } pools { id } }` came back without `pools` at all - a field
    // the operation asked for, silently missing from the response (Jules on #1282). Introspection is
    // not an exclusive mode, it is two more root fields.
    for root in &roots {
        match (root.name.as_str(), &doc) {
            // The introspection surface a generated client fetches before it sends anything useful.
            // `render` produces `{"__schema": …}`, so the inner value is what belongs under the key.
            ("__schema", Some(d)) => {
                match project_introspection(&d["__schema"], "__Schema", &root.sel, types) {
                    Ok(v) => data.insert(root.key.clone(), v),
                    Err(e) => return (StatusCode::OK, Json(gql_error(&e.to_string()))),
                };
                continue;
            }
            // The other standard introspection operation. The name comes from the parsed argument,
            // and one the schema does not declare is `null` rather than an error - that is what
            // introspection says, and a client uses it to test whether a type exists.
            ("__type", Some(_)) => {
                let found = root
                    .args
                    .get("name")
                    .and_then(|v| match v {
                        crate::graph_query::Value::Str(s) => Some(s.as_str()),
                        _ => None,
                    })
                    .and_then(|want| types.iter().find(|t| t["name"].as_str() == Some(want)));
                let v = match found {
                    Some(t) => project_introspection(t, "__Type", &root.sel, types),
                    None => {
                        check_introspection("__Type", &root.sel).map(|()| serde_json::Value::Null)
                    }
                };
                match v {
                    Ok(v) => data.insert(root.key.clone(), v),
                    Err(e) => return (StatusCode::OK, Json(gql_error(&e.to_string()))),
                };
                continue;
            }
            // `_meta` is the nest's own head, not a compiled query. A nest runs no mapping, so
            // `hasIndexingErrors` is false as a fact rather than as a convenience - there is no
            // handler that could have aborted.
            ("_meta", _) => {
                // Its one argument is `block`, and ignoring it answered a past or future block with
                // today's head.
                for (name, value) in &root.args {
                    if name != "block" {
                        return (
                            StatusCode::OK,
                            Json(gql_error(&format!("unsupported _meta argument `{name}`"))),
                        );
                    }
                    let unmet = match crate::graph_query::minimum_block(
                        value,
                        &crate::graph_query::Capabilities::BUILD,
                    ) {
                        Ok(min) => min.and_then(|min| unmet_minimum_block(&s, min)),
                        Err(e) => Some(e.to_string()),
                    };
                    if let Some(e) = unmet {
                        return (StatusCode::OK, Json(gql_error(&e)));
                    }
                }
                let last = historical_blocks.get(&root.key).copied().or_else(|| {
                    s.store
                        .get_meta("last_block")
                        .ok()
                        .flatten()
                        .and_then(|v| v.parse::<u64>().ok())
                });
                // Rendered complete, then narrowed by the selection, exactly as `__schema` and
                // `__type` are. It used to insert this object whole, so `{ _meta { block { number } } }`
                // came back carrying `deployment` and `hasIndexingErrors` the client had not asked
                // for and ignoring every alias. Probed against the live reference: graph-node returns
                // only the selected fields, and honours an alias on the root, on a leaf and on the
                // scalars. It does *not* honour one on `block` itself - `{ _meta { b: block { number } } }`
                // answers `internal error resolving _Block_.block: expected prefetched result, but
                // found nothing`, which is a graph-node defect rather than a contract. Projecting
                // uniformly answers that correctly; no client can be relying on the error.
                //
                // `hash` and `parentHash` from the checkpoint table; `timestamp` from the same
                // record once ingestion has stored it (#1289).
                let meta = serde_json::json!({
                    "block": {
                        "number": last,
                        "hash": last.and_then(|n| s.store.get_block_hash(n).ok().flatten()),
                        "parentHash": last
                            .and_then(|n| n.checked_sub(1))
                            .and_then(|n| s.store.get_block_hash(n).ok().flatten()),
                        "timestamp": last
                            .and_then(|n| s.store.get_block_timestamp(n).ok().flatten()),
                    },
                    // `deployment` is `String!`. `nid` is `None` for a nest mounted by alias with no
                    // content address (`MountTable::nest_dir`), and answering `null` against a
                    // non-null field is a document a strict client rejects outright. The nest's own
                    // name is the honest stand-in: a mismatch against an expected CID is then visible
                    // rather than a schema violation.
                    "deployment": s.nid.as_deref().map(|n| n.to_string()).unwrap_or_else(|| {
                        s.nest_info["name"].as_str().unwrap_or("nuthatch").to_string()
                    }),
                    "hasIndexingErrors": false
                });
                data.insert(root.key.clone(), project(&meta, &root.sel));
                continue;
            }
            _ => {}
        }
        let compiled = match crate::graph_query::compile_with(
            &schema,
            root,
            &crate::graph_query::Capabilities::BUILD,
        ) {
            Ok(c) => c,
            Err(e) => return (StatusCode::OK, Json(gql_error(&e.to_string()))),
        };
        if let Some(e) = compiled
            .min_block
            .and_then(|min| unmet_minimum_block(&s, min))
        {
            return (StatusCode::OK, Json(gql_error(&e)));
        }
        if graph_remaining(started).is_zero() {
            return (
                StatusCode::OK,
                Json(gql_error(&format!(
                    "the operation exceeded its {}s budget",
                    SQL_TIMEOUT.as_secs()
                ))),
            );
        }
        if bytes_left == 0 {
            return (
                StatusCode::OK,
                Json(gql_error("the operation exceeds its result budget")),
            );
        }
        match graph_rows(
            &s,
            &compiled,
            historical_blocks.get(&root.key).copied(),
            started,
            bytes_left,
        )
        .await
        {
            Ok((rows, used)) => {
                bytes_left = bytes_left.saturating_sub(used);
                let shaped: Result<Vec<serde_json::Value>, String> =
                    rows.iter().map(|r| graph_shape(&compiled, r)).collect();
                let shaped = match shaped {
                    Ok(v) => v,
                    Err(e) => return (StatusCode::OK, Json(gql_error(&e))),
                };
                if history.is_some() {
                    for row in &shaped {
                        if let Err(e) = crate::graph_history::validate_result(
                            &schema,
                            &compiled.entity,
                            &root.sel,
                            row,
                        ) {
                            return (StatusCode::OK, Json(gql_error(&e.to_string())));
                        }
                    }
                }
                let value = if compiled.singular {
                    shaped.into_iter().next().unwrap_or(serde_json::Value::Null)
                } else {
                    serde_json::Value::Array(shaped)
                };
                data.insert(root.key.clone(), value);
            }
            Err(msg) => {
                let msg = graph_unserved(&schema, root, &compiled, &msg).unwrap_or(msg);
                return (StatusCode::OK, Json(gql_error(&msg)));
            }
        }
    }
    if let Some((generation, sealed)) = history_fence {
        if s.store.write_generation() != Some(generation) || s.store.sealed_through() != sealed {
            return (
                StatusCode::OK,
                Json(gql_error(
                    "indexed state changed while reading the GraphQL operation; retry",
                )),
            );
        }
        let now = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_secs();
        if let Some(policy) = &history {
            if let Err(e) = graph_history_head(&s, policy, now) {
                return (StatusCode::OK, Json(gql_error(&e.to_string())));
            }
        }
    }
    (StatusCode::OK, Json(serde_json::json!({"data": data})))
}

/// Narrow a rendered value to exactly the fields a selection asked for.
///
/// A GraphQL response carries the selected fields and no others. The introspection branches used to hand
/// back the whole rendered document whatever was selected, so `{ __schema { types { name } } }` came back
/// with all eight keys of every type (Jules on #1282). Harmless to the clients that ignore extras, wrong
/// for any that do not, and trivially avoidable now the parser hands over a resolved selection tree.
///
/// A selected field the document does not carry becomes `null` rather than being dropped: a client that
/// asked for a key should find it there.
///
/// Arguments on an introspection field are ignored on purpose. The only one a client sends is
/// `includeDeprecated`, and a generated subgraph schema deprecates nothing, so both values answer the
/// same list.
#[cfg(feature = "graph")]
fn project(value: &serde_json::Value, sel: &[crate::graph_query::Selection]) -> serde_json::Value {
    if sel.is_empty() {
        return value.clone();
    }
    match value {
        serde_json::Value::Array(items) => {
            serde_json::Value::Array(items.iter().map(|v| project(v, sel)).collect())
        }
        serde_json::Value::Object(o) => {
            let mut out = serde_json::Map::new();
            for s in sel {
                let v = o.get(&s.name).cloned().unwrap_or(serde_json::Value::Null);
                out.insert(s.key.clone(), project(&v, &s.sub));
            }
            serde_json::Value::Object(out)
        }
        // A scalar with a sub-selection is not a legal query; pass it through rather than invent an
        // object for it.
        _ => value.clone(),
    }
}

/// The fields graph-node's `introspection.graphql` gives each introspection type, with the
/// introspection type a composite one returns. `None` for a field the type has not got.
#[cfg(feature = "graph")]
fn introspection_field(ty: &str, field: &str) -> Option<Option<&'static str>> {
    const TYPE: Option<&str> = Some("__Type");
    Some(match (ty, field) {
        ("__Schema", "types" | "queryType" | "mutationType" | "subscriptionType") => TYPE,
        ("__Schema", "directives") => Some("__Directive"),
        ("__Schema", "description") => None,
        ("__Type", "fields") => Some("__Field"),
        ("__Type", "interfaces" | "possibleTypes" | "ofType") => TYPE,
        ("__Type", "enumValues") => Some("__EnumValue"),
        ("__Type", "inputFields") => Some("__InputValue"),
        ("__Type", "kind" | "name" | "description" | "specifiedByURL") => None,
        ("__Field" | "__InputValue", "type") => TYPE,
        ("__Field" | "__Directive", "args") => Some("__InputValue"),
        ("__Field" | "__InputValue" | "__EnumValue" | "__Directive", "name" | "description") => {
            None
        }
        ("__Field" | "__InputValue" | "__EnumValue", "isDeprecated" | "deprecationReason") => None,
        ("__InputValue", "defaultValue") => None,
        ("__Directive", "locations" | "isRepeatable") => None,
        _ => return None,
    })
}

/// Refuse a selection naming a field its introspection type has not got, as graph-node's validation
/// does, before anything is answered.
#[cfg(feature = "graph")]
fn check_introspection(
    ty: &str,
    sel: &[crate::graph_query::Selection],
) -> Result<(), crate::graph_query::Unsupported> {
    for s in sel {
        if s.name == "__typename" {
            continue;
        }
        match introspection_field(ty, &s.name) {
            Some(Some(inner)) => check_introspection(inner, &s.sub)?,
            Some(None) => {}
            None => {
                return Err(crate::graph_query::Unsupported::UnknownField {
                    entity: ty.to_string(),
                    field: s.name.clone(),
                })
            }
        }
    }
    Ok(())
}

/// Narrow an introspection value to `sel`, as `project` does, against the introspection schema.
///
/// The rendered document stores the type a field or argument points at as a `{kind, name}`
/// reference, and `queryType` as `{name}`. Selecting `fields` through one answered `null` (#1941), so
/// a reference to a named type is answered from that type's own declaration in `types`.
#[cfg(feature = "graph")]
fn project_introspection(
    value: &serde_json::Value,
    ty: &str,
    sel: &[crate::graph_query::Selection],
    types: &[serde_json::Value],
) -> Result<serde_json::Value, crate::graph_query::Unsupported> {
    use serde_json::Value;
    check_introspection(ty, sel)?;
    fn walk(
        value: &Value,
        ty: &str,
        sel: &[crate::graph_query::Selection],
        types: &[Value],
    ) -> Value {
        if sel.is_empty() {
            return value.clone();
        }
        match value {
            Value::Array(items) => {
                Value::Array(items.iter().map(|v| walk(v, ty, sel, types)).collect())
            }
            Value::Object(o) => {
                let decl = match (ty, o.get("name").and_then(Value::as_str)) {
                    ("__Type", Some(name)) => types.iter().find(|t| t["name"] == name),
                    _ => None,
                };
                let mut out = serde_json::Map::new();
                for s in sel {
                    let v = if s.name == "__typename" {
                        Value::String(ty.to_string())
                    } else {
                        match o.get(&s.name).or_else(|| decl.and_then(|d| d.get(&s.name))) {
                            Some(v) => match introspection_field(ty, &s.name).flatten() {
                                Some(inner) => walk(v, inner, &s.sub, types),
                                None => v.clone(),
                            },
                            // Declared by the introspection schema, absent from the document: a
                            // generated subgraph schema deprecates nothing and repeats no directive.
                            None if matches!(s.name.as_str(), "isDeprecated" | "isRepeatable") => {
                                Value::Bool(false)
                            }
                            None => Value::Null,
                        }
                    };
                    out.insert(s.key.clone(), v);
                }
                Value::Object(out)
            }
            _ => value.clone(),
        }
    }
    Ok(walk(value, ty, sel, types))
}

/// The refusal for a query whose engine error is a missing view of an entity the schema declares
/// (#1941): the collection is named, not the table the engine could not find.
#[cfg(feature = "graph")]
fn graph_unserved(
    schema: &crate::graph_schema::Schema,
    root: &crate::graph_query::RootField,
    compiled: &crate::graph_query::Compiled,
    err: &str,
) -> Option<String> {
    let view = crate::analytics::missing_table_of(err)?;
    let entity = schema
        .entities
        .iter()
        .find(|e| crate::subgraph_import::to_alias(&e.name) == view)?;
    let what = if entity.name == compiled.entity {
        &root.name
    } else {
        &entity.name
    };
    // A view that exists and failed to build is a different fault, and the hint already names it.
    let built = format!("view `{view}` failed to build");
    let why = match err.split_once("\n\nhint: ") {
        Some((_, hint)) if hint.starts_with(&built) => hint.to_string(),
        _ => format!("it has no `{view}` view"),
    };
    Some(format!("`{what}` is not served by this nest: {why}"))
}

/// Build one response object from one SQL row.
///
/// A relation traversal is flattened into the row by the join that lowered it, so `token0 { symbol }`
/// arrives as a column called `j0__symbol` and has to be put back under `token0`. Only
/// [`crate::graph_query::Compiled::shape`] knows that mapping.
///
/// A to-one reference whose target row is missing comes back as all-null from the `LEFT JOIN`, and the
/// relation is then `null` rather than an object of nulls - which is what graph-node answers, and the
/// difference a client can actually see.
#[cfg(feature = "graph")]
fn graph_shape(
    compiled: &crate::graph_query::Compiled,
    row: &serde_json::Map<String, serde_json::Value>,
) -> Result<serde_json::Value, String> {
    use crate::graph_query::Shape;
    let mut out = serde_json::Map::new();
    for sh in &compiled.shape {
        match sh {
            // `key` is what the caller asked to see it under, `col` is where it arrives. They differ
            // when an alias was written - `{ p: pools { n: id } }`.
            Shape::Scalar { key, col } => {
                out.insert(
                    key.clone(),
                    row.get(col).cloned().unwrap_or(serde_json::Value::Null),
                );
            }
            // A `@derivedFrom` list arrives as one JSON array in one column.
            Shape::List { key, col } => {
                let v = row.get(col);
                let list = match v {
                    // Already parsed: the row serialiser may hand back a `json` column as a value
                    // rather than a string. Both are accepted rather than one assumed.
                    Some(serde_json::Value::Array(a)) => serde_json::Value::Array(a.clone()),
                    Some(serde_json::Value::String(t)) => match serde_json::from_str(t) {
                        Ok(serde_json::Value::Array(a)) => serde_json::Value::Array(a),
                        // Not an array and not parseable is a bug in the SQL this module wrote, not
                        // something to paper over with `[]` - a caller told an empty list is
                        // indistinguishable from one told the truth.
                        _ => return Err(format!("`{key}` did not come back as a JSON array: {t}")),
                    },
                    // `coalesce(…, '[]')` means this cannot be null, so null is also a bug.
                    other => return Err(format!("`{key}` is missing from the row: {other:?}")),
                };
                out.insert(key.clone(), list);
            }
            Shape::Json { key, col } => {
                let v = match row.get(col) {
                    None | Some(serde_json::Value::Null) => serde_json::Value::Null,
                    Some(serde_json::Value::String(t)) => serde_json::from_str(t)
                        .map_err(|_| format!("`{key}` did not come back as JSON: {t}"))?,
                    Some(other) => other.clone(),
                };
                out.insert(key.clone(), v);
            }
            Shape::DerivedOne {
                key,
                col,
                field,
                child,
                back,
            } => {
                let items = match row.get(col) {
                    Some(serde_json::Value::Array(a)) => a.clone(),
                    Some(serde_json::Value::String(t)) => match serde_json::from_str(t) {
                        Ok(serde_json::Value::Array(a)) => a,
                        _ => return Err(format!("`{key}` did not come back as a JSON array: {t}")),
                    },
                    other => return Err(format!("`{key}` is missing from the row: {other:?}")),
                };
                if items.len() > 1 {
                    // graph-node's wording, `graph/src/data/query/error.rs` AmbiguousDerivedFromResult.
                    return Err(format!(
                        "Ambiguous result for derived field `{field}`: Multiple `{child}` entities refer back via `{back}`"
                    ));
                }
                out.insert(
                    key.clone(),
                    items.into_iter().next().unwrap_or(serde_json::Value::Null),
                );
            }
            Shape::Object {
                key,
                marker,
                fields,
                lists,
                objects,
            } => {
                let mut inner = serde_json::Map::new();
                for (sub_key, col) in fields {
                    let v = row.get(col).cloned().unwrap_or(serde_json::Value::Null);
                    inner.insert(sub_key.clone(), v);
                }
                for o in objects {
                    let present = row.get(&o.marker).is_some_and(|v| !v.is_null());
                    let v = if present {
                        serde_json::Value::Object(
                            o.fields
                                .iter()
                                .map(|(k, c)| {
                                    (
                                        k.clone(),
                                        row.get(c).cloned().unwrap_or(serde_json::Value::Null),
                                    )
                                })
                                .collect(),
                        )
                    } else {
                        serde_json::Value::Null
                    };
                    inner.insert(o.key.clone(), v);
                }
                for (sub_key, col) in lists {
                    let list = match row.get(col) {
                        Some(serde_json::Value::Array(items)) => {
                            serde_json::Value::Array(items.clone())
                        }
                        Some(serde_json::Value::String(text)) => match serde_json::from_str(text) {
                            Ok(serde_json::Value::Array(items)) => serde_json::Value::Array(items),
                            _ => {
                                return Err(format!(
                                    "`{sub_key}` did not come back as a JSON array: {text}"
                                ))
                            }
                        },
                        other => {
                            return Err(format!(
                                "`{sub_key}` is missing from the related object: {other:?}"
                            ))
                        }
                    };
                    inner.insert(sub_key.clone(), list);
                }
                // The marker is the target's id, so it is non-null exactly when the `LEFT JOIN` found a
                // row. Deciding from the *selected* values instead answered `null` for a relation that
                // existed but whose selected fields were all null - a real object reported as absent.
                let present = row.get(marker).is_some_and(|v| !v.is_null());
                out.insert(
                    key.clone(),
                    if present {
                        serde_json::Value::Object(inner)
                    } else {
                        serde_json::Value::Null
                    },
                );
            }
        }
    }
    Ok(serde_json::Value::Object(out))
}

/// The Graph error envelope. A client reads `errors` and does not read an HTTP status, which is why
/// every refusal here returns `200` with this body rather than a 4xx.
#[cfg(feature = "graph")]
fn gql_error(message: &str) -> serde_json::Value {
    serde_json::json!({"errors":[{"message": message}]})
}

/// **`block: { number_gte: N }` is a precondition on the head**, checked here because the head is
/// runtime state the compiler cannot see. graph-node answers on the latest block when the deployment
/// has reached `N` and refuses otherwise, and its refusal is quoted rather than paraphrased:
/// `DeploymentState::block_queryable` in `graph/src/data/subgraph/mod.rs:1362`.
///
/// A nest with no head has indexed nothing, so it has not reached any block: reported as 0, which is
/// what the message then says.
#[cfg(feature = "graph")]
fn unmet_minimum_block(s: &AppState, min: u64) -> Option<String> {
    let head = s
        .store
        .get_meta("last_block")
        .ok()
        .flatten()
        .and_then(|v| v.parse::<u64>().ok())
        .unwrap_or(0);
    if head >= min {
        return None;
    }
    let id = s.nid.as_deref().map(str::to_string).unwrap_or_else(|| {
        s.nest_info["name"]
            .as_str()
            .unwrap_or("nuthatch")
            .to_string()
    });
    Some(format!(
        "subgraph {id} has only indexed up to block number {head} and data for block number {min} \
         is therefore not yet available"
    ))
}

/// Operational freshness, not a claim that every Graph entity has passed reference parity.
#[cfg(feature = "graph")]
async fn graph_status(State(state): State<AppState>) -> impl IntoResponse {
    let unavailable = |reason: String| {
        (
            StatusCode::SERVICE_UNAVAILABLE,
            Json(json!({"fresh":false,"reason":reason})),
        )
    };
    let policy = match crate::graph_history::Policy::load(&state.dir.join("graph/history.toml")) {
        Ok(Some(policy)) => policy,
        Ok(None) => {
            return unavailable("historical Graph freshness policy is not configured".into())
        }
        Err(error) => return unavailable(format!("invalid Graph freshness policy: {error}")),
    };
    let generation = state.store.write_generation();
    let sealed = state.store.sealed_through();
    let now = crate::metrics::now_unix();
    let snapshot = match policy.indexed_head(state.store.as_ref()) {
        Ok(snapshot) => snapshot,
        Err(error) => return unavailable(error.to_string()),
    };
    let (tip, last_poll) = match &state.runtime_health {
        Some((name, _)) => {
            let metrics = crate::metrics::METRICS.nest(name);
            (metrics.tip(), metrics.last_poll_ok())
        }
        None => (
            crate::metrics::METRICS.tip_height(),
            crate::metrics::METRICS.last_poll_ok(),
        ),
    };
    let checked = graph_history_head(&state, &policy, now);
    if generation.is_none()
        || state.store.write_generation() != generation
        || state.store.sealed_through() != sealed
    {
        return unavailable("indexed state changed during freshness check; retry".into());
    }
    let fresh = checked.is_ok();
    (
        if fresh {
            StatusCode::OK
        } else {
            StatusCode::SERVICE_UNAVAILABLE
        },
        Json(json!({
            "fresh":fresh,
            "indexed":{"number":snapshot.number,"hash":snapshot.hash,"timestamp":snapshot.timestamp},
            "observedHead":(tip != 0).then_some(tip),
            "lastHeadObservation":(last_poll != 0).then_some(last_poll),
            "blockDistance":(tip != 0).then(|| tip.checked_sub(snapshot.number)).flatten(),
            "ageSeconds":now.checked_sub(snapshot.timestamp),
            "limits":{"maxAgeSeconds":policy.max_head_age_seconds,"maxBlockDistance":policy.max_block_distance},
            "reason":checked.err().map(|error| error.to_string())
        })),
    )
}

#[cfg(feature = "graph")]
fn graph_history_head(
    state: &AppState,
    policy: &crate::graph_history::Policy,
    now: u64,
) -> anyhow::Result<crate::graph_history::Snapshot> {
    use crate::metrics::METRICS;
    let head = policy.head(state.store.as_ref(), now)?;
    let (tip, last_poll) = match &state.runtime_health {
        Some((name, health)) => {
            if let Some(fault) = health.status(name) {
                anyhow::bail!("Graph history is quarantined: {}", fault.reason);
            }
            let metrics = METRICS.nest(name);
            (metrics.tip(), metrics.last_poll_ok())
        }
        None => (METRICS.tip_height(), METRICS.last_poll_ok()),
    };
    policy.check_observed_head(head.number, tip, last_poll, now)?;
    Ok(head)
}

/// Run a compiled query through the same analytical path `/sql` uses, so a Graph query inherits
/// RFC-0034's admission bounds rather than opening a second unbounded door into Burrmill.
#[cfg(feature = "graph")]
async fn graph_rows(
    s: &AppState,
    compiled: &crate::graph_query::Compiled,
    historical_block: Option<u64>,
    started: std::time::Instant,
    byte_budget: usize,
) -> Result<(Vec<serde_json::Map<String, serde_json::Value>>, usize), String> {
    if graph_remaining(started).is_zero() {
        return Err(format!(
            "the operation exceeded its {}s budget",
            SQL_TIMEOUT.as_secs()
        ));
    }
    #[cfg(all(test, feature = "graph"))]
    {
        GRAPH_SQL_CALLS.with(|c| c.set(c.get() + 1));
        GRAPH_LAST_LIMIT.with(|c| c.set(byte_budget));
    }
    let resp = run_sql_query_at(
        s.clone(),
        compiled.sql.clone(),
        None,
        historical_block,
        Some(graph_remaining(started)),
    )
    .await;
    let body = axum::body::to_bytes(resp.into_response().into_body(), byte_budget)
        .await
        .map_err(|e| format!("the operation exceeds its result budget ({e})"))?;
    let v: serde_json::Value =
        serde_json::from_slice(&body).map_err(|e| format!("decoding the query result: {e}"))?;
    graph_result_rows(&v).map(|rows| (rows, body.len()))
}

/// SQL may return a useful partial answer with explicit warnings. GraphQL clients do not see that
/// envelope, so stripping its warnings would turn incomplete data into truth.
#[cfg(feature = "graph")]
fn graph_result_rows(
    v: &serde_json::Value,
) -> Result<Vec<serde_json::Map<String, serde_json::Value>>, String> {
    if let Some(err) = v.get("error").and_then(|e| e.as_str()) {
        return Err(err.to_string());
    }
    for flag in ["truncated", "degraded", "tip_unavailable"] {
        if v.get(flag)
            .is_some_and(|value| value != &serde_json::Value::Bool(false))
        {
            return Err(format!("GraphQL requires a complete query result: {flag}"));
        }
    }
    let rows = v
        .get("rows")
        .and_then(|r| r.as_array())
        .ok_or_else(|| "query result has no rows array".to_string())?;
    rows.iter()
        .map(|row| {
            row.as_object()
                .cloned()
                .ok_or_else(|| "query result contains a non-object row".to_string())
        })
        .collect()
}

async fn schema_doc(State(s): State<AppState>) -> impl IntoResponse {
    let sem = crate::semantic::load(&s.dir).ok().flatten();
    if let Some(sem) = &sem {
        for w in crate::semantic::drift(&s.tables, sem) {
            tracing::warn!("semantic.toml drift: {w}");
        }
    }
    let coverage = crate::semantic::Coverage {
        sealed_through: s.store.sealed_through(),
        tip: s
            .store
            .get_meta("last_block")
            .ok()
            .flatten()
            .and_then(|v| v.parse().ok())
            .unwrap_or(0),
    };
    // #822's surface bullet: `/schema` (which the MCP `schema` tool relays verbatim) must say that
    // a relation is incremental and name its applied-through block.
    let head = dataset_head(&s);
    let maintained: Vec<crate::semantic::MaintainedRelation> = s
        .entities
        .iter()
        .map(|e| {
            // #932: ONE acquisition. Calling the accessor per field would be three, which is the
            // bug it exists to fix wearing a different hat.
            let (rows, applied) = e.len_and_watermark();
            crate::semantic::MaintainedRelation {
                name: e.name().to_string(),
                columns: e.columns().to_vec(),
                applied_through: applied.through,
                current: applied.through >= head,
                unavailable: e.unavailable().map(str::to_string),
                fault: e.fault(),
                rows,
                offchain: e
                    .offchain_tables()
                    .iter()
                    .map(|t| format!("{}{t}", crate::entity_offchain::OFFCHAIN_NAMESPACE))
                    .collect(),
            }
        })
        .collect();
    let doc = crate::semantic::compose(&s.tables, sem.as_ref(), Some(&coverage), &maintained);
    (
        [(
            axum::http::header::CONTENT_TYPE,
            "text/plain; charset=utf-8",
        )],
        doc,
    )
}

/// Recent rows of one table, merged across the hot store and the sealed cold segments.
async fn table(
    State(s): State<AppState>,
    Path(name): Path<String>,
    Query(q): Query<TableQuery>,
) -> impl IntoResponse {
    if !s.tables.iter().any(|t| t.table == name) {
        return not_found(&name);
    }
    let limit = q.limit.unwrap_or(100).min(1000);

    // Hot rows (tip), newest first.
    let hot = if q.from_block.is_none() && q.to_block.is_none() {
        s.store.recent_by_table(&name, limit).and_then(|rows| {
            rows.iter()
                .map(|r| serde_json::from_str::<Value>(r).context("unparseable hot row"))
                .collect()
        })
    } else {
        hot_rows_in_window(
            &*s.store,
            &name,
            limit,
            q.from_block.unwrap_or(0),
            q.to_block.unwrap_or(u64::MAX),
        )
    };
    let mut items = match hot {
        Ok(items) => items,
        Err(e) => return error(format!("{e:#}")),
    };

    // Fill from cold (sealed segments) if the hot store didn't satisfy the limit. This runs under
    // the same analytical admission gate as `/sql`; if it's saturated we serve the hot rows we
    // already have rather than pile more scan-heavy work on - but say so, in `/sql`'s terms, so a
    // hot-only answer is never read as the merged table (#1659).
    let mut degraded_tables = std::collections::BTreeSet::new();
    if items.len() < limit {
        let cold = match Arc::clone(&s.sql_gate).try_acquire_owned() {
            Err(_) => Err("the SQL gate is saturated".to_string()),
            Ok(permit) => {
                let need = limit - items.len();
                let mut where_ = String::new();
                if let Some(f) = q.from_block {
                    where_.push_str(&format!(" AND block_number >= {f}"));
                }
                if let Some(t) = q.to_block {
                    where_.push_str(&format!(" AND block_number <= {t}"));
                }
                let sql = format!(
                    "SELECT * FROM \"{name}\" WHERE 1=1{where_} ORDER BY block_number DESC, log_index DESC LIMIT {need}"
                );
                let dir = s.dir.clone();
                let declared = Arc::clone(&s.tables);
                let guard = analytics::QueryGuard {
                    timeout: SQL_TIMEOUT,
                    max_rows: need,
                };
                match tokio::task::spawn_blocking(move || {
                    let _permit = permit; // held for the whole blocking query
                                          // Declared, so a table nothing has been sealed for yet reads empty, not failed.
                    let hot = analytics::HotRows::new();
                    analytics::query_hot_cold(&dir, &sql, guard, &hot, u64::MAX, &declared)
                })
                .await
                {
                    Ok(Ok(out)) => Ok(out),
                    Ok(Err(e)) => Err(format!("{e:#}")),
                    Err(e) => Err(format!("{e}")),
                }
            }
        };
        match cold {
            Ok(out) => {
                degraded_tables.extend(out.degraded_tables);
                items.extend(out.rows);
            }
            Err(why) => {
                tracing::warn!("/table/{name} served without its sealed rows: {why}");
                degraded_tables.insert(name.clone());
            }
        }
    }

    // Dedup by (block, log_index); hot wins over cold.
    let mut seen = std::collections::HashSet::new();
    items.retain(|v| {
        let id = (
            v.get("block_number").and_then(Value::as_u64),
            v.get("log_index").and_then(Value::as_u64),
        );
        seen.insert(id)
    });
    items.truncate(limit);
    Json(json!({
        "table": name,
        "count": items.len(),
        "items": items,
        "degraded": !degraded_tables.is_empty(),
        "degraded_tables": degraded_tables,
    }))
    .into_response()
}

/// The newest `limit` hot rows of `table` with a block in `[from, to]`, newest first. The window is
/// applied before the limit, or a window below the newest `limit` hot rows would find none (#1659).
fn hot_rows_in_window(
    store: &dyn crate::store::HotStore,
    table: &str,
    limit: usize,
    from: u64,
    to: u64,
) -> Result<Vec<Value>> {
    let mut newest = std::collections::VecDeque::with_capacity(limit.min(1024));
    let mut unparseable = None;
    store.scan_entities_in_range(from, to, &mut |_, raw| {
        match serde_json::from_str::<Value>(raw) {
            Ok(v) if v.get("table").and_then(Value::as_str) == Some(table) => {
                newest.push_back(v);
                if newest.len() > limit {
                    newest.pop_front();
                }
            }
            Ok(_) => {}
            Err(e) => {
                unparseable = Some(e);
                return false;
            }
        }
        true
    })?;
    if let Some(e) = unparseable {
        return Err(e).context("unparseable hot row");
    }
    Ok(newest.into_iter().rev().collect())
}

#[derive(Deserialize)]
struct EntitiesQuery {
    limit: Option<usize>,
}

async fn entities(State(s): State<AppState>, Query(q): Query<EntitiesQuery>) -> impl IntoResponse {
    let limit = q.limit.unwrap_or(100).min(1000);
    match s.store.recent(limit) {
        Ok(rows) => {
            let items: Vec<Value> = rows
                .iter()
                .filter_map(|r| serde_json::from_str::<Value>(r).ok())
                .collect();
            Json(json!({ "count": items.len(), "items": items })).into_response()
        }
        Err(e) => error(format!("{e:#}")),
    }
}

/// Documents either resolving path gave up on, in block order (#1410). Each is absent from sealed history.
async fn ipfs_gave_up(
    State(s): State<AppState>,
    Query(q): Query<EntitiesQuery>,
) -> impl IntoResponse {
    use crate::ipfs_resolve::{GaveUp, GAVE_UP_PREFIX};
    let limit = q.limit.unwrap_or(100).min(1000);
    let read = s
        .store
        .meta_with_prefix(GAVE_UP_PREFIX, limit)
        .and_then(|rows| Ok((s.store.count_meta_with_prefix(GAVE_UP_PREFIX)?, rows)));
    match read {
        Ok((total, rows)) => {
            let items: Vec<GaveUp> = rows
                .iter()
                .filter_map(|(k, v)| GaveUp::parse(k, v))
                .collect();
            Json(json!({ "total": total, "count": items.len(), "items": items })).into_response()
        }
        Err(e) => error(format!("{e:#}")),
    }
}

async fn entity(State(s): State<AppState>, Path(id): Path<String>) -> impl IntoResponse {
    // Hot path first (redb). On a miss, fall back to the sealed segments (Burrmill), so a point-read
    // keeps working across the hot→cold seam even after the hot store has been pruned.
    match s.store.get_entity(&id) {
        Ok(Some(raw)) => match serde_json::from_str::<Value>(&raw) {
            Ok(v) => return Json(v).into_response(),
            Err(e) => return error(format!("{e:#}")),
        },
        Ok(None) => {}
        Err(e) => return error(format!("{e:#}")),
    }

    match parse_id(&id) {
        Some((block, log_index)) => {
            // A hot miss scans sealed segments. Same admission and deadline as `/sql` (#1657).
            let admission = std::time::Instant::now();
            let busy = || {
                crate::metrics::METRICS.inc_sql_rejected(crate::metrics::SqlRejection::Busy);
                (
                    StatusCode::SERVICE_UNAVAILABLE,
                    Json(json!({ "error": "server busy: too many concurrent SQL queries" })),
                )
                    .into_response()
            };
            let permit = match Arc::clone(&s.sql_gate).try_acquire_owned() {
                Ok(p) => p,
                Err(_) => {
                    let Some(_slot) = QueueSlot::take(&s.sql_queued) else {
                        return busy();
                    };
                    match tokio::time::timeout(
                        SQL_ADMISSION_WAIT,
                        Arc::clone(&s.sql_gate).acquire_owned(),
                    )
                    .await
                    {
                        Ok(Ok(p)) => p,
                        _ => return busy(),
                    }
                }
            };
            let timeout = SQL_TIMEOUT.saturating_sub(admission.elapsed());
            let dir = s.dir.clone();
            let sealed = tokio::task::spawn_blocking(move || {
                let _permit = permit;
                analytics::get_row_guarded(
                    &dir,
                    block,
                    log_index,
                    analytics::QueryGuard {
                        timeout,
                        max_rows: 1,
                    },
                )
            })
            .await;
            match sealed {
                Ok(Ok(Some(v))) => Json(v).into_response(),
                Ok(Ok(None)) => not_found(&id),
                Ok(Err(e)) => error(format!("{e:#}")),
                Err(e) => error(format!("{e}")),
            }
        }
        None => not_found(&id),
    }
}

#[derive(Deserialize)]
struct SqlQuery {
    q: String,
    /// Optional row cap for this request, clamped to `[1, SQL_MAX_ROWS]`. The MCP bridge passes a
    /// small value (default 200) so an agent's context isn't flooded (RFC-0016 §4); curl gets the
    /// full cap. Absent → the node cap.
    #[serde(default)]
    max_rows: Option<usize>,
}

/// The refusal a bounded mount gives free-form SQL (RFC-0034 §2).
///
/// `403`, not `400`: the query may be perfectly valid: this nest simply does not answer arbitrary
/// SQL. And it names what *can* be asked, in RFC-0016's errors-as-prompts style, so an agent hitting
/// a bounded nest is told the surface rather than left guessing at it.
fn refuse_free_form(surface: &crate::allowlist::Surface) -> axum::response::Response {
    crate::metrics::METRICS.inc_sql_rejected(crate::metrics::SqlRejection::Bounded);
    let names = surface.names();
    let body = if names.is_empty() {
        json!({
            "error": "this nest does not answer SQL",
            "hint": "the operator has disabled /sql and /explain for this mount; the typed routes \
                     (/tables, /table/{name}, /entity/{id}, /balances) still work",
        })
    } else {
        json!({
            "error": "this nest answers only its declared queries, not arbitrary SQL",
            "allowed_queries": names,
            "hint": "call GET /q/{name} with the query's parameters as the query string; \
                     GET /queries lists them with their parameters",
        })
    };
    (StatusCode::FORBIDDEN, Json(body)).into_response()
}

/// `GET /queries` - what this mount will answer, and with what parameters.
///
/// Always present, whatever the mode, so a caller can discover a *bounded* nest's surface without
/// first getting refused by it.
async fn queries(State(s): State<AppState>) -> impl IntoResponse {
    Json(json!({
        "sql": match s.surface.access {
            crate::allowlist::SqlAccess::Open => "open",
            crate::allowlist::SqlAccess::Deny => "deny",
            crate::allowlist::SqlAccess::Allowlist => "allowlist",
        },
        "free_form": s.surface.free_form_allowed(),
        "queries": s.surface.queries.iter().map(|q| json!({
            "name": q.name,
            "params": q.param_list(),
            "path": format!("/q/{}", q.name),
        })).collect::<Vec<_>>(),
    }))
}

/// `GET /q/{name}?param=value` - run one declared query.
///
/// The caller sends a **name and arguments, never SQL**. Everything the node's own guards do to
/// `/sql` still applies here: a declared query can still be expensive, and the timeout, row cap and
/// concurrency limit are what stop it.
async fn named_query(
    State(s): State<AppState>,
    axum::extract::Path(name): axum::extract::Path<String>,
    Query(args): Query<std::collections::HashMap<String, String>>,
    headers: axum::http::HeaderMap,
) -> impl IntoResponse {
    #[cfg(not(feature = "counter"))]
    let _ = &headers;
    use crate::metrics::METRICS;
    let Some(q) = s.surface.get(&name) else {
        METRICS.inc_sql_rejected(crate::metrics::SqlRejection::Admission);
        return (
            StatusCode::NOT_FOUND,
            Json(json!({
                "error": format!("no query named '{name}' on this nest"),
                "allowed_queries": s.surface.names(),
            })),
        )
            .into_response();
    };
    let sql = match q.render(&args) {
        Ok(sql) => sql,
        Err(e) => {
            METRICS.inc_sql_rejected(crate::metrics::SqlRejection::Invalid);
            return (
                StatusCode::BAD_REQUEST,
                Json(json!({ "error": format!("{e:#}"), "query": name })),
            )
                .into_response();
        }
    };
    #[cfg(feature = "counter")]
    let mut pin: Option<(Option<String>, u64)> = None;
    #[cfg(not(feature = "counter"))]
    let pin: Option<(Option<String>, u64)> = None;
    #[cfg(feature = "counter")]
    if let Some(counter) = &s.counter {
        let resource = format!("/q/{name}");
        if !crate::counter::payment_verifies(counter, &headers) {
            // Unpaid: quote what the answer would read, or refuse a query that could not be served.
            return match named_scan(&s, sql.clone(), None, false).await {
                Ok(planned) => crate::counter::challenge(
                    counter,
                    &resource,
                    &name,
                    Some(&planned.quote(counter)),
                ),
                Err(failure) => named_failure(&s, failure, &sql),
            };
        }
        if let Some(value) = headers.get(crate::counter::QUOTE_HEADER) {
            match value.to_str().ok().and_then(crate::counter::parse_quote_id) {
                Some(quoted) => pin = Some(quoted),
                None => {
                    return (
                        StatusCode::BAD_REQUEST,
                        Json(json!({
                            "error": format!("{} is not a quote id this node issues", crate::counter::QUOTE_HEADER),
                        })),
                    )
                        .into_response()
                }
            }
        }
    }
    let run = match named_scan(&s, sql.clone(), pin, true).await {
        Ok(run) => run,
        Err(failure) => {
            #[cfg(feature = "counter")]
            if let (Some(counter), NamedFailure::Query(e)) = (&s.counter, &failure) {
                if let Some(crate::analytics::AdmissionRefusal::StaleCatalogue { .. }) =
                    e.downcast_ref()
                {
                    crate::metrics::METRICS.inc_named_scan_refused();
                    let fresh = named_scan(&s, sql.clone(), None, false)
                        .await
                        .ok()
                        .map(|planned| planned.quote(counter));
                    return (
                        StatusCode::CONFLICT,
                        Json(json!({ "error": e.to_string(), "quote": fresh })),
                    )
                        .into_response();
                }
            }
            return named_failure(&s, failure, &sql);
        }
    };
    #[cfg(feature = "counter")]
    if let Some(counter) = &s.counter {
        let resource = format!("/q/{name}");
        // Nothing has been sent to the caller yet. Concurrent replays may compute, but the atomic
        // nonce check lets only one record and release its answer.
        if let Err(refusal) = crate::counter::admit(
            &s.dir,
            counter,
            &headers,
            &resource,
            &name,
            Some(&run.quote(counter)),
        ) {
            return *refusal;
        }
    }
    if let Some(bound) = &run.out.scan_bound {
        crate::metrics::METRICS
            .observe_named_scan(bound.cold_bytes.saturating_add(bound.hot_bytes));
    }
    sql_response(
        &s,
        &run.out,
        SqlRows::Values(&run.out.rows),
        &run.watermarks,
        run.provenance,
        false,
    )
}

/// A declared query taken through RFC-0048 admission.
struct NamedRun {
    out: crate::analytics::QueryOutput,
    watermarks: crate::entity_view::Watermarks,
    provenance: (Option<u64>, u64),
    /// The boundary the statement was planned against, which a quote names.
    #[cfg_attr(not(feature = "counter"), allow(dead_code))]
    sealed_through: u64,
    #[cfg_attr(not(feature = "counter"), allow(dead_code))]
    hot_rows: u64,
}

impl NamedRun {
    #[cfg(feature = "counter")]
    fn quote(&self, counter: &crate::counter::Config) -> serde_json::Value {
        crate::counter::quote(
            counter,
            &self.out.scan_bound.clone().unwrap_or_default(),
            self.sealed_through,
            self.hot_rows,
        )
    }
}

enum NamedFailure {
    Busy,
    Query(anyhow::Error),
    Task(String),
}

/// Copy the tip against the byte cap, then bound the plan on the connection that would run it. The
/// cold reservation comes from this statement's last plan against the same catalogue: it only
/// decides how much hot copying is worth attempting, because the plan that runs is bounded again.
/// `execute: false` stops at the bound, which is what a quote is.
async fn named_scan(
    s: &AppState,
    sql: String,
    pin: Option<(Option<String>, u64)>,
    execute: bool,
) -> Result<NamedRun, NamedFailure> {
    use crate::analytics::{AdmissionRefusal, NamedAdmission, QueryGuard, ScanBound};
    let permit = Arc::clone(&s.sql_gate)
        .try_acquire_owned()
        .map_err(|_| NamedFailure::Busy)?;
    crate::metrics::METRICS.inc_sql();
    let dir = s.dir.clone();
    let store = s.store.clone();
    let tables = s.tables.clone();
    let entities = s.entities.clone();
    let cap = s.sql_max_named_scan_bytes;
    let max_hot_rows = s.sql_max_hot_rows;
    tokio::task::spawn_blocking(move || -> anyhow::Result<NamedRun> {
        let _permit = permit;
        let started = std::time::Instant::now();
        let catalogue_hash = crate::seal::catalogue_hash(&dir)?;
        // A stale quote answers 409 before a budget can refuse it, and is checked again after the
        // copy, because the boundary the query runs against is read only then.
        let stale = |sealed_through: u64| {
            pin.as_ref()
                .filter(|(catalogue, sealed)| {
                    *sealed != sealed_through || *catalogue != catalogue_hash
                })
                .map(|(catalogue, _)| AdmissionRefusal::StaleCatalogue {
                    quoted: catalogue.clone(),
                    current: catalogue_hash.clone(),
                })
        };
        if let Some(refusal) = stale(store.sealed_through()) {
            return Err(refusal.into());
        }
        let reserved = crate::analytics::remembered_scan_bound(&dir, &sql, &catalogue_hash);
        if let Some(bound) = reserved.as_ref().filter(|b| b.cold_bytes > cap) {
            return Err(AdmissionRefusal::OverCap(ScanBound {
                hot_bytes: 0,
                cap,
                ..bound.clone()
            })
            .into());
        }
        let budget = cap - reserved.map(|b| b.cold_bytes).unwrap_or(0);
        // No cold-only fallback: an admitted query never answers from an incomplete relation.
        let snapshot = store.hot_rows_by_table_bounded_with_bytes(max_hot_rows, budget)?;
        let mut hot = snapshot.rows;
        let mut hot_rows = snapshot.source_rows;
        let mut hot_bytes = snapshot.source_bytes;
        let mut watermarks = std::collections::BTreeMap::new();
        // Maintained relations are copied into the connection per request, so they spend the same
        // budget as the tip.
        for entity in entities.iter() {
            if entity.unavailable().is_some() || entity.fault().is_some() {
                continue;
            }
            // Refuse on the count before serialising a relation too large to copy.
            if hot_rows.saturating_add(entity.len() as u64) > max_hot_rows as u64 {
                return Err(crate::store::HotScanTooLarge { cap: max_hot_rows }.into());
            }
            let (rows, applied) = entity.rows_as_json_with_watermark();
            hot_rows += rows.len() as u64;
            if hot_rows > max_hot_rows as u64 {
                return Err(crate::store::HotScanTooLarge { cap: max_hot_rows }.into());
            }
            hot_bytes = rows
                .iter()
                .fold(hot_bytes, |sum, row| sum.saturating_add(json_len(row)));
            if hot_bytes > budget {
                return Err(crate::store::HotScanBudgetExceeded {
                    source_bytes: hot_bytes,
                    budget,
                }
                .into());
            }
            watermarks.insert(entity.name().to_string(), applied);
            hot.insert(entity.name().to_string(), rows);
        }
        let sealed_through = store.sealed_through();
        if let Some(refusal) = stale(sealed_through) {
            return Err(refusal.into());
        }
        let timeout = SQL_TIMEOUT.saturating_sub(started.elapsed());
        if timeout.is_zero() {
            return Err(crate::analytics::QueryBudgetExceeded {
                secs: SQL_TIMEOUT.as_secs(),
            }
            .into());
        }
        let admission = NamedAdmission {
            cap,
            hot_bytes,
            pinned_catalogue: pin.map(|(catalogue, _)| catalogue),
            execute,
        };
        let out = crate::analytics::query_named(
            &dir,
            &sql,
            QueryGuard {
                timeout,
                max_rows: SQL_MAX_ROWS,
            },
            &hot,
            sealed_through,
            &tables,
            &admission,
        )?;
        let as_of = store
            .get_meta("last_block")
            .ok()
            .flatten()
            .and_then(|v| v.parse::<u64>().ok());
        Ok(NamedRun {
            out,
            watermarks,
            provenance: (as_of, store.sealed_through()),
            sealed_through,
            hot_rows,
        })
    })
    .await
    .map_err(|e| NamedFailure::Task(e.to_string()))?
    .map_err(NamedFailure::Query)
}

fn named_failure(s: &AppState, failure: NamedFailure, sql: &str) -> axum::response::Response {
    use crate::analytics::AdmissionRefusal;
    use crate::metrics::METRICS;
    let e = match failure {
        NamedFailure::Busy => {
            METRICS.inc_sql_rejected(crate::metrics::SqlRejection::Busy);
            return (
                StatusCode::SERVICE_UNAVAILABLE,
                Json(json!({ "error": "server busy: too many concurrent SQL queries" })),
            )
                .into_response();
        }
        NamedFailure::Task(e) => return error(e),
        NamedFailure::Query(e) => e,
    };
    if let Some(refusal) = e.downcast_ref::<AdmissionRefusal>() {
        METRICS.inc_sql_rejected(crate::metrics::SqlRejection::Admission);
        METRICS.inc_named_scan_refused();
        let (status, bound) = match refusal {
            AdmissionRefusal::OverCap(bound) => (StatusCode::UNPROCESSABLE_ENTITY, Some(bound)),
            AdmissionRefusal::Unboundable(_) => (StatusCode::UNPROCESSABLE_ENTITY, None),
            AdmissionRefusal::StaleCatalogue { .. } => (StatusCode::CONFLICT, None),
        };
        return (
            status,
            Json(json!({ "error": refusal.to_string(), "bound": bound })),
        )
            .into_response();
    }
    if let Some(over) = e.downcast_ref::<crate::store::HotScanBudgetExceeded>() {
        METRICS.inc_sql_rejected(crate::metrics::SqlRejection::Admission);
        METRICS.inc_named_scan_refused();
        return (
            StatusCode::UNPROCESSABLE_ENTITY,
            Json(json!({
                "error": over.to_string(),
                "hot_source_bytes": over.source_bytes,
                "budget": over.budget,
                "cap": s.sql_max_named_scan_bytes,
            })),
        )
            .into_response();
    }
    sql_error_response(s, e, sql)
}

/// Serialized length of a row without building the string.
fn json_len(value: &serde_json::Value) -> u64 {
    struct Count(u64);
    impl std::io::Write for Count {
        fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
            self.0 += buf.len() as u64;
            Ok(buf.len())
        }
        fn flush(&mut self) -> std::io::Result<()> {
            Ok(())
        }
    }
    let mut count = Count(0);
    let _ = serde_json::to_writer(&mut count, value);
    count.0
}

/// Read-only analytical SQL over the sealed segments; one view per `{alias}__{event}` table.
///
/// Self-protecting, so a public `/sql` isn't a DoS vector: bounded concurrency (503 when the
/// analytical surface is saturated - a growing backlog is itself the attack), a per-query wall-clock
/// budget (a runaway is interrupted), a max query length, and a row cap (bounds the result buffer).
/// What's deliberately *absent* is authn / per-caller quotas: those need caller identity a sovereign
/// node doesn't have, so gating *who* may query and *how much* is a gateway's job, not the node's.
/// **Bounded surfaces (RFC-0034).** What the guards above cannot express is *which* queries a nest
/// will answer at all. A mount may set `sql = "deny"` or `sql = "allowlist"`, and this route then
/// refuses free-form text and points at `/q/{name}` instead.
async fn sql(State(s): State<AppState>, Query(q): Query<SqlQuery>) -> impl IntoResponse {
    use crate::metrics::METRICS;
    if !s.surface.free_form_allowed() {
        return refuse_free_form(&s.surface);
    }
    if q.q.len() > SQL_MAX_QUERY_LEN {
        METRICS.inc_sql_rejected(crate::metrics::SqlRejection::Invalid);
        return (
            StatusCode::BAD_REQUEST,
            Json(json!({ "error": format!("query too long: {} bytes (max {SQL_MAX_QUERY_LEN})", q.q.len()) })),
        )
            .into_response();
    }
    run_sql_query(s, q.q, q.max_rows).await
}

/// Execute SQL and shape the response - shared by `/sql` (caller-supplied text, only when the
/// surface is open) and `/q/{name}` (a declared query, rendered from typed arguments).
///
/// **Every node guard lives here**, so a declared query is bounded exactly as free-form SQL is. A
/// name on the allowlist says the operator is willing to answer it, not that it is cheap.
async fn run_sql_query(
    s: AppState,
    sql_text: String,
    requested_max_rows: Option<usize>,
) -> axum::response::Response {
    run_sql_query_at(s, sql_text, requested_max_rows, None, None).await
}

/// The memo key for `sql` against the nest as it stands, with the rows generation, served segments
/// and entity watermarks it was built from. `None` when the answer is not memoised at all.
fn memo_identity(
    s: &AppState,
    sql: &str,
    max_rows: usize,
    historical_block: Option<u64>,
) -> Option<(
    crate::sqlmemo::Key,
    u64,
    String,
    crate::entity_view::Watermarks,
)> {
    let generation = s
        .store
        .rows_generation()
        .filter(|_| historical_block.is_none())
        .filter(|_| crate::sqlmemo::is_deterministic(sql))?;
    let sealed = crate::sqlmemo::sealed_segments(&s.dir, s.store.sealed_through())?;
    let watermarks: crate::entity_view::Watermarks = s
        .entities
        .iter()
        .filter(|e| e.unavailable().is_none() && e.fault().is_none())
        .map(|e| (e.name().to_string(), e.fence_watermark()))
        .collect();
    let files = crate::analytics::cache_inputs(&s.dir);
    let key = crate::sqlmemo::Inputs {
        dir: &s.dir,
        sql,
        max_rows,
        sealed: &sealed,
        rows_generation: generation,
        entity_watermarks: &watermarks,
        files: &files,
    }
    .key();
    Some((key, generation, sealed, watermarks))
}

async fn run_sql_query_at(
    s: AppState,
    sql_text: String,
    requested_max_rows: Option<usize>,
    historical_block: Option<u64>,
    operation_timeout: Option<Duration>,
) -> axum::response::Response {
    use crate::metrics::METRICS;
    let q = SqlQuery {
        q: sql_text,
        max_rows: requested_max_rows,
    };
    // Per-request row cap (RFC-0016 §4): the MCP bridge asks for a small number so an agent's context
    // isn't flooded; curl omits it and gets the node cap. Clamped so it can only ever tighten.
    let max_rows = q.max_rows.unwrap_or(SQL_MAX_ROWS).clamp(1, SQL_MAX_ROWS);
    // The deterministic memo (#1186): the identity of this answer is the statement plus every input
    // it reads - the served segments, the hot rows' generation, entity watermarks, authored files. A
    // remembered answer for that identity is the answer, and it is returned before the permit gate,
    // because a hit costs no Burrmill and the gate exists to bound Burrmill. `None` where the store
    // cannot report a rows generation; that backend simply computes every time.
    let memo = memo_identity(&s, &q.q, max_rows, historical_block);
    if let Some((key, generation, sealed, before)) = &memo {
        if let Some(hit) = crate::sqlmemo::get(key) {
            // Re-read the fence after the lookup, as the computing path does after its query: a
            // commit that landed between building the key and finding the entry has moved the store
            // past the state this entry describes, and the request computes instead. What remains
            // is the interval between this check and the response, which is the interval every
            // computed answer has between its last read and its response - no memo could narrow it
            // further, and no caller could tell the two apart (Jules on #1189).
            let still: crate::entity_view::Watermarks = s
                .entities
                .iter()
                .filter(|e| e.unavailable().is_none() && e.fault().is_none())
                .map(|e| (e.name().to_string(), e.fence_watermark()))
                .collect();
            // The head the hit cites is read before the fence, so the fence covers it: a commit that
            // changed a row by `as_of` has already moved the generation (#1951, Jules on #1955).
            let as_of = s
                .store
                .get_meta("last_block")
                .ok()
                .flatten()
                .and_then(|v| v.parse::<u64>().ok());
            let sealed_through = s.store.sealed_through();
            if s.store.rows_generation() == Some(*generation)
                && crate::sqlmemo::sealed_segments(&s.dir, sealed_through).as_ref()
                    == Some(sealed)
                && still == *before
                // And the cold side the answer was computed over is the one still on disk: a
                // sealed segment is immutable by construction, so nothing the node does changes
                // it, but a disk fault or a half-finished restore does - and the answer over it
                // then differs while every input the node knows about is unchanged. See
                // `sqlmemo::segment_stamps`.
                && crate::sqlmemo::segment_stamps(&s.dir, hit.out.referenced_tables.as_ref())
                    == hit.segments
            {
                METRICS.inc_sql();
                return sql_response(
                    &s,
                    &hit.out,
                    SqlRows::Serialized {
                        json: &hit.rows,
                        count: hit.count,
                    },
                    &hit.watermarks,
                    (as_of, sealed_through),
                    true,
                );
            }
        }
    }
    // A GraphQL operation acquires once and passes the time it has left. Taking another permit
    // here would start that root's 30s from now (#1658). `/sql` still admits itself.
    let (permit, sql_timeout) = if let Some(timeout) = operation_timeout {
        if timeout.is_zero() {
            return (
                StatusCode::GATEWAY_TIMEOUT,
                Json(json!({
                    "error": format!(
                        "the operation exceeded its {}s budget",
                        SQL_TIMEOUT.as_secs()
                    ),
                    "timeout_secs": SQL_TIMEOUT.as_secs(),
                })),
            )
                .into_response();
        }
        #[cfg(all(test, feature = "graph"))]
        GRAPH_GUARD_TIMEOUT.with(|c| c.set(Some(timeout)));
        (None, timeout)
    } else {
        // Admission is two bounds, and it needs both. `SQL_ADMISSION_WAIT` bounds how long any one
        // request waits; `SQL_MAX_QUEUED` bounds how many wait at once. Without the second, a burst
        // parks arrival-rate-times-250 ms requests before any of them time out, which is a worse
        // failure than the bouncing this change exists to stop. The permit count is untouched and
        // still decides how many queries run at once (#1319).
        let admission = std::time::Instant::now();
        let busy = || {
            METRICS.inc_sql_rejected(crate::metrics::SqlRejection::Busy);
            (
                StatusCode::SERVICE_UNAVAILABLE,
                Json(json!({ "error": "server busy: too many concurrent SQL queries" })),
            )
                .into_response()
        };
        let permit = match Arc::clone(&s.sql_gate).try_acquire_owned() {
            // A free permit needs no queue at all, so the uncontended path never touches
            // the counter.
            Ok(p) => p,
            Err(_) => {
                // Saturated: park, but only if this nest has room to park another.
                let Some(_slot) = QueueSlot::take(&s.sql_queued) else {
                    return busy();
                };
                match tokio::time::timeout(
                    SQL_ADMISSION_WAIT,
                    Arc::clone(&s.sql_gate).acquire_owned(),
                )
                .await
                {
                    Ok(Ok(p)) => p,
                    // Elapsed, or the semaphore was closed on shutdown. Only a request that
                    // gets the 503 is counted; one that waited and was then admitted is an accepted
                    // query.
                    _ => return busy(),
                }
            }
        };
        // Charged against the query's own deadline, so queuing can never extend the total time a
        // request occupies the node.
        (
            Some(permit),
            SQL_TIMEOUT.saturating_sub(admission.elapsed()),
        )
    };
    METRICS.inc_sql();
    let dir = s.dir.clone();
    let sql = q.q.clone();
    let store = s.store.clone();
    let sql_max_hot_rows = s.sql_max_hot_rows;
    let sql_max_hot_bytes = s.sql_max_hot_bytes;
    // The live, registry-derived schema - every table the config declares, whether or not it has
    // populated yet. Threaded into `define_views` so a declared-but-never-fired event still gets an
    // empty typed view instead of the whole nest view failing to bind on a missing table (#663).
    let tables = s.tables.clone();
    let declared_entities = s.entities.clone();
    let result = tokio::task::spawn_blocking(move || {
        let _permit = permit; // held for the whole blocking query, released on return
                              // Scan the hot tip (redb, blocking) inside the same blocking task, so `/sql` sees the unsealed
                              // rows alongside the sealed segments (RFC-0013). A scan failure degrades to cold-only.
                              // A scan failure degrades to cold-only, *except* an over-budget tip: that must surface, or a
                              // query would quietly answer from sealed data alone and report a different number.
        let (hot, tip_unavailable) = match store
            .hot_rows_by_table_bounded_with_bytes(sql_max_hot_rows, sql_max_hot_bytes)
            .map(|snapshot| snapshot.rows)
        {
            Ok(hot) => (hot, false),
            Err(e)
                if e.downcast_ref::<crate::store::HotScanTooLarge>().is_some()
                    || e.downcast_ref::<crate::store::HotScanBudgetExceeded>()
                        .is_some() =>
            {
                return Err(e)
            }
            // Same level as a segment read failure (#472): a hot store that will not scan is at least
            // as serious, and the fallback below must not be the only trace of it.
            Err(e) => {
                tracing::error!("hot-tip scan failed - serving cold-only for this query: {e:#}");
                (Default::default(), true)
            }
        };
        // §5.4: the maintained relation is exposed under its declared name, alongside the decoded
        // tables. **This copies every entity row into the connection, per request** - #822 criterion
        // 7's separate term, and it is what this seam is: `hot` is the map `analytics::run` defines
        // its views from.
        //
        // **Measured, and it is not the term that matters** (2026-08-27, `tests/seed_scale.rs` at `ede03ca7`,
        // against a 38,428-segment Horizon nest). A top-20 over a relation of 309,549 maintained
        // rows took 2,487 ms; `SELECT 1`, which reads nothing at all, took 2,465 ms on the same
        // nest. The copy is the 22 ms difference. What the request actually pays for is
        // `define_views` rebuilding a view for every table in the manifest, at roughly 62 µs per
        // sealed segment, whether or not the query names it - #896. Optimising this copy would
        // have bought a one-percent improvement and cost the persistent-catalogue rewrite the
        // criterion warns against, which is exactly why it says measure first.
        //
        // An entity holding no answer contributes no table at all rather than an empty one. An empty
        // relation and an unavailable one are different facts, and a query cannot tell them apart
        // from zero rows - `/derived` is where the reason lives.
        let mut hot = hot;
        // #932: take the rows and the watermark that describes them in one acquisition, and carry
        // that watermark into the provenance below. Re-reading `applied_through()` after the query
        // ran is a race - measured at 1 in 12 on a 0.25s-block chain - and it reports an answer as
        // more current than the rows it is made of.
        let mut watermarks: crate::entity_view::Watermarks = Default::default();
        for entity in declared_entities.iter() {
            if historical_block.is_some() {
                continue;
            }
            if entity.unavailable().is_some() || entity.fault().is_some() {
                continue;
            }
            let (rows, applied) = entity.rows_as_json_with_watermark();
            watermarks.insert(entity.name().to_string(), applied);
            hot.insert(entity.name().to_string(), rows);
        }
        let sealed_through = store.sealed_through();
        let guard = analytics::QueryGuard {
            timeout: sql_timeout,
            max_rows,
        };
        let mut out = if let Some(block) = historical_block {
            analytics::query_hot_cold_at(&dir, &sql, guard, &hot, sealed_through, &tables, block)?
        } else {
            analytics::query_hot_cold(&dir, &sql, guard, &hot, sealed_through, &tables)?
        };
        out.tip_unavailable = tip_unavailable;
        // The state after the query, for the memo: an answer is remembered only if nothing it reads
        // moved while it ran, so a remembered answer always describes exactly the state its key names.
        let sealed_after = store.sealed_through();
        let after = (
            store.rows_generation(),
            sealed_after,
            crate::sqlmemo::sealed_segments(&dir, sealed_after),
        );
        // Provenance from the same task as the query, for the same reason as the watermarks: read
        // out on the response path it can name a newer state than the rows came from.
        let as_of = historical_block.or_else(|| {
            store
                .get_meta("last_block")
                .ok()
                .flatten()
                .and_then(|v| v.parse::<u64>().ok())
        });
        // The watermarks ride out with the result: they describe the rows this query was answered
        // from, and re-reading them out here is the race #932 is about.
        Ok((out, watermarks, after, as_of))
    })
    .await;
    match result {
        // Provenance stamp (RFC-0016 §4): an agent can cite its answer against content-addressed data -
        // as of which block, what's sealed, and the registry it decoded with.
        Ok(Ok((out, watermarks, after, as_of))) => {
            let provenance = (as_of, after.1);
            if let Some((key, generation, sealed, before)) = memo {
                if after.0 == Some(generation) && after.2 == Some(sealed) && watermarks == before {
                    let segments =
                        crate::sqlmemo::segment_stamps(&s.dir, out.referenced_tables.as_ref());
                    crate::sqlmemo::put(key, &out, &watermarks, segments);
                }
            }
            sql_response(
                &s,
                &out,
                SqlRows::Values(&out.rows),
                &watermarks,
                provenance,
                false,
            )
        }
        Ok(Err(e)) => sql_error_response(&s, e, &q.q),
        Err(e) => error(format!("{e}")),
    }
}

fn sql_error_response(s: &AppState, e: anyhow::Error, sql: &str) -> axum::response::Response {
    use crate::metrics::METRICS;
    // The guard's deadline, not a bad query. A 504 so a gateway retries or degrades instead of
    // reporting a client error, and its own rejection reason so a slow nest is visible as one.
    if let Some(cut) = e.downcast_ref::<crate::analytics::QueryBudgetExceeded>() {
        METRICS.inc_sql_rejected(crate::metrics::SqlRejection::Timeout);
        return (
            StatusCode::GATEWAY_TIMEOUT,
            Json(json!({ "error": cut.to_string(), "timeout_secs": cut.secs })),
        )
            .into_response();
    }
    if let Some(cut) = e.downcast_ref::<crate::analytics::QuerySpillExceeded>() {
        METRICS.inc_sql_rejected(crate::metrics::SqlRejection::TooLarge);
        return (
            StatusCode::INSUFFICIENT_STORAGE,
            Json(json!({ "error": cut.to_string(), "spill_cap_bytes": cut.cap_bytes })),
        )
            .into_response();
    }
    let reason = if e.downcast_ref::<crate::store::HotScanTooLarge>().is_some()
        || e.downcast_ref::<crate::store::HotScanBudgetExceeded>()
            .is_some()
    {
        crate::metrics::SqlRejection::TooLarge
    } else if crate::sql_errors::is_out_of_memory(&format!("{e:#}")) {
        crate::metrics::SqlRejection::OutOfMemory
    } else {
        crate::metrics::SqlRejection::Invalid
    };
    METRICS.inc_sql_rejected(reason);
    // The tip is too large to serve in one scan. A `503` rather than a `400`: the query is fine,
    // the node is refusing to spend the memory - so a caller should retry later or narrow to
    // sealed data, not rewrite their SQL.
    if e.downcast_ref::<crate::store::HotScanTooLarge>().is_some() {
        return (
            StatusCode::SERVICE_UNAVAILABLE,
            Json(json!({
                "error": format!("{e}"),
                "sealed_through": s.store.sealed_through(),
            })),
        )
            .into_response();
    }
    if let Some(over) = e.downcast_ref::<crate::store::HotScanBudgetExceeded>() {
        return (
            StatusCode::SERVICE_UNAVAILABLE,
            Json(json!({
                "error": over.to_string(),
                "hot_source_bytes": over.source_bytes,
                "budget": over.budget,
                "sealed_through": s.store.sealed_through(),
            })),
        )
            .into_response();
    }
    // A guard rejection (timeout / interrupt) or a bad query - counted as a rejection.
    // Errors as prompts (RFC-0016 §3): classify the failure against the schema and append an
    // actionable hint so an agent (or the REPL user) self-corrects in one round-trip. The raw
    // engine message is preserved but path-scrubbed (SEC review) - the engine embeds absolute
    // segment paths, which would leak the on-disk layout; the useful table/column detail stays.
    let raw = sanitize_sql_error(&format!("{e:#}"), &s.dir);
    let msg = match crate::analytics::enrich_query_error(&s.dir, &raw, sql, &s.tables) {
        Some(hint) => format!("{raw}\n\nhint: {hint}"),
        None => raw,
    };
    (StatusCode::BAD_REQUEST, Json(json!({ "error": msg }))).into_response()
}

/// `GET /explain?q=…` - validate a query without executing it (RFC-0016 §3): bind it (catching
/// unknown tables/columns/type errors, with the same enriched hints as `/sql`) but scan nothing: the
/// statement is planned and not run. An agent checks a query's shape before spending a
/// concurrency slot on the real thing. Returns `{valid:true}` or the enriched error.
async fn explain(State(s): State<AppState>, Query(q): Query<SqlQuery>) -> impl IntoResponse {
    use crate::metrics::METRICS;
    // `/explain` gets the same treatment as `/sql`. It plans caller-supplied SQL, so leaving it open
    // on a bounded mount would leak the schema and the cost model of every table. Bounding the
    // surface closes that; the hot-row ceiling below closes the scan cost (#293).
    if !s.surface.free_form_allowed() {
        return refuse_free_form(&s.surface);
    }
    if q.q.len() > SQL_MAX_QUERY_LEN {
        return (
            StatusCode::BAD_REQUEST,
            Json(json!({ "valid": false, "error": "query too long" })),
        )
            .into_response();
    }
    let permit = match Arc::clone(&s.sql_gate).try_acquire_owned() {
        Ok(p) => p,
        Err(_) => {
            return (
                StatusCode::SERVICE_UNAVAILABLE,
                Json(json!({ "valid": false, "error": "server busy" })),
            )
                .into_response()
        }
    };
    let dir = s.dir.clone();
    let query = q.q.clone();
    let store = s.store.clone();
    let sql_max_hot_rows = s.sql_max_hot_rows;
    let sql_max_hot_bytes = s.sql_max_hot_bytes;
    let tables = s.tables.clone();
    let declared_entities = s.entities.clone();
    let result = tokio::task::spawn_blocking(move || {
        let _permit = permit;
        // Planning materialises no result rows, but the tip is still parsed
        // into temp tables first - so `/explain` carries the full scan cost. Without the same
        // ceiling `/sql` enforces, an endpoint reachable by anyone who can reach `/sql` could push
        // the process past the budget `/sql` refuses to cross (#293). As there, an over-budget tip
        // surfaces rather than degrading to cold-only: answering "valid" off sealed data alone
        // would bind against a narrower schema than the one a subsequent `/sql` would see.
        let (hot, tip_unavailable) = match store
            .hot_rows_by_table_bounded_with_bytes(sql_max_hot_rows, sql_max_hot_bytes)
            .map(|snapshot| snapshot.rows)
        {
            Ok(hot) => (hot, false),
            Err(e)
                if e.downcast_ref::<crate::store::HotScanTooLarge>().is_some()
                    || e.downcast_ref::<crate::store::HotScanBudgetExceeded>()
                        .is_some() =>
            {
                return Err(e)
            }
            // Same level as a segment read failure (#472): a hot store that will not scan is at
            // least as serious, and the fallback below must not be the only trace of it - #528.
            Err(e) => {
                tracing::error!("hot-tip scan failed - serving cold-only for this query: {e:#}");
                (Default::default(), true)
            }
        };
        // **The same relations `/sql` would see.** Without this, `/explain` answers a question about a
        // different database than the one that runs the query, and the maintained relations are
        // exactly where the two sets differ.
        //
        // It did not merely omit them, which would at least have been consistently wrong. Sessions
        // are pooled and `define_views` only refreshes tables in the *current* set, so a relation
        // defined by an earlier `/sql` on that session was still bound: the identical
        // request returned `400 Table with name received does not exist` on a cold connection and
        // `200 valid` once any `/sql` had warmed one. An agent validating its query before running
        // it was told a good query was invalid, or told it was valid for the wrong reason, depending
        // on which of the pool it landed on.
        let mut hot = hot;
        for entity in declared_entities.iter() {
            if entity.unavailable().is_some() || entity.fault().is_some() {
                continue;
            }
            hot.insert(entity.name().to_string(), entity.rows_as_json());
        }
        let sealed_through = store.sealed_through();
        let mut out = analytics::plan_hot_cold(
            &dir,
            &query,
            analytics::QueryGuard {
                timeout: SQL_TIMEOUT,
                max_rows: 1,
            },
            &hot,
            sealed_through,
            &tables,
        )?;
        out.tip_unavailable = tip_unavailable;
        Ok(out)
    })
    .await;
    match result {
        Ok(Ok(out)) => Json(json!({
            "valid": true,
            "note": "query binds; run it with the sql tool",
            // Same field, same meaning as `/sql` (#528): the hot-tip scan failed, so this bind
            // answer is against sealed history alone and may miss a table/column the tip would add.
            "tip_unavailable": out.tip_unavailable,
        }))
        .into_response(),
        // Same contract as `/sql`: a `503`, not a `400`. The query may well be valid - the node is
        // refusing to spend the memory to find out, so a caller should retry later, not rewrite
        // their SQL. `valid` is deliberately absent rather than `false`: bindability is unknown
        // here, and reporting `false` would tell a caller their query is broken when it is not.
        Ok(Err(e)) if e.downcast_ref::<crate::store::HotScanTooLarge>().is_some() => {
            METRICS.inc_sql_rejected(crate::metrics::SqlRejection::TooLarge);
            (
                StatusCode::SERVICE_UNAVAILABLE,
                Json(json!({
                    "error": format!("{e}"),
                    "sealed_through": s.store.sealed_through(),
                })),
            )
                .into_response()
        }
        Ok(Err(e))
            if e.downcast_ref::<crate::store::HotScanBudgetExceeded>()
                .is_some() =>
        {
            METRICS.inc_sql_rejected(crate::metrics::SqlRejection::TooLarge);
            let over = e
                .downcast_ref::<crate::store::HotScanBudgetExceeded>()
                .expect("matched by the guard");
            (
                StatusCode::SERVICE_UNAVAILABLE,
                Json(json!({
                    "error": over.to_string(),
                    "hot_source_bytes": over.source_bytes,
                    "budget": over.budget,
                    "sealed_through": s.store.sealed_through(),
                })),
            )
                .into_response()
        }
        Ok(Err(e)) => {
            METRICS.inc_sql_rejected(crate::metrics::SqlRejection::Invalid);
            let raw = sanitize_sql_error(&format!("{e:#}"), &s.dir);
            let msg = match crate::analytics::enrich_query_error(&s.dir, &raw, &q.q, &s.tables) {
                Some(hint) => format!("{raw}\n\nhint: {hint}"),
                None => raw,
            };
            (
                StatusCode::BAD_REQUEST,
                Json(json!({ "valid": false, "error": msg })),
            )
                .into_response()
        }
        Err(e) => error(format!("{e}")),
    }
}

/// Top balances from the IVM view, descending. Balances are in i64 token base units.
async fn balances(State(s): State<AppState>, Query(q): Query<EntitiesQuery>) -> impl IntoResponse {
    let limit = q.limit.unwrap_or(100).min(1000);
    // Balances are i128 base units, serialised as decimal strings - JSON numbers can't carry i128
    // losslessly, and a client parsing a huge balance as an f64 would silently corrupt it.
    let items: Vec<Value> = s
        .balances
        .top(limit)
        .into_iter()
        .map(|(address, balance)| json!({ "address": address, "balance": balance.to_string() }))
        .collect();
    // COR-8 (#814): a transfer whose value exceeds 38 digits is dropped from these balances - both
    // legs, deliberately, because dropping one would invent value. Saying so is the whole fix: the
    // numbers stay what they are, and a caller can tell an incomplete answer from a complete one.
    // Zero is the ordinary case and is reported rather than omitted, so its absence means an old
    // build rather than a clean nest.
    Json(json!({
        "holders": s.balances.holders(),
        "count": items.len(),
        "items": items,
        "dropped_over_i128": s.balances.dropped_over_i128(),
    }))
}

/// The nest's authored incremental entities, and how current each one is (RFC-0041 §5.4, #822).
///
/// `/entities` keeps its existing meaning - decoded event rows - so maintained relations live under
/// `/derived`. Two names for two different things beats one name that quietly changed.
async fn derived_index(State(s): State<AppState>) -> impl IntoResponse {
    let head = dataset_head(&s);
    let items: Vec<Value> = s
        .entities
        .iter()
        .map(|e| {
            // #932: the pair under one lock.
            let (row_count, applied) = e.len_and_watermark();
            with_offchain(
                json!({
                    "name": e.name(),
                    "rows": row_count,
                    "incremental": true,
                    "applied_through": applied.through,
                    "current": applied.through >= head,
                    "available": e.unavailable().is_none() && e.fault().is_none(),
                }),
                e,
                &applied,
            )
        })
        .collect();
    Json(json!({ "count": items.len(), "head": head, "entities": items }))
}

/// Why an entity cannot answer right now, if it cannot.
///
/// **§5.1: "A catching-up entity is reported as such; it never serves a plausible partial relation as
/// current."** Criterion 8 asks the keyed and SQL routes to refuse or report not-current
/// *consistently*, so the decision lives in one function rather than being made twice and drifting.
///
/// A faulted or unavailable entity is a **503**: it holds no answer and will not without operator
/// action. A catching-up one is **not** an error - it is the ordinary state during backfill - but it
/// is served with `current: false` and its own watermark, never stamped with the nest's head.
fn derived_refusal(entity: &crate::entity_view::EntityView) -> Option<(StatusCode, Value)> {
    if let Some(why) = entity.unavailable() {
        return Some((
            StatusCode::SERVICE_UNAVAILABLE,
            json!({ "error": "entity unavailable", "entity": entity.name(), "reason": why }),
        ));
    }
    if let Some(why) = entity.fault() {
        return Some((
            StatusCode::SERVICE_UNAVAILABLE,
            json!({ "error": "entity faulted", "entity": entity.name(), "reason": why }),
        ));
    }
    None
}

fn find_entity<'a>(
    s: &'a AppState,
    name: &str,
) -> Result<&'a crate::entity_view::EntityView, Box<axum::response::Response>> {
    s.entities.iter().find(|e| e.name() == name).ok_or_else(|| {
        let known: Vec<&str> = s.entities.iter().map(|e| e.name()).collect();
        (
            StatusCode::NOT_FOUND,
            Json(json!({
                "error": "no such entity",
                "entity": name,
                "entities": known,
            })),
        )
            .into_response()
            .into()
    })
}

/// Provenance for a derived answer (criterion 9): which entity, how far it has folded, and that the
/// answer came from maintained state rather than a scan.
/// The dataset's head block, as the hot store records it. `0` where it has none yet, which reads
/// correctly through `EntityView::is_current`: an entity applied through nothing is current with a
/// dataset that holds nothing.
fn dataset_head(s: &AppState) -> u64 {
    s.store
        .get_meta("last_block")
        .ok()
        .flatten()
        .and_then(|v| v.parse::<u64>().ok())
        .unwrap_or(0)
}

/// **#822 criterion 9 for `/sql`.** Which maintained relations answered this statement, how far each
/// is applied, and whether that is current.
///
/// Built from the referenced-table set the security walk already produced rather than by matching
/// entity names against the SQL text, so the provenance and the control that admits the query agree
/// by construction. Returns `Value::Null` when no entity was referenced or when the set is unknown:
/// an empty array would assert "this query touched no maintained state", which is a claim the `None`
/// case has no basis to make.
/// The `/sql` response for an answer, computed or remembered. One place, so a memo hit and a fresh
/// computation cannot drift apart in shape or provenance (#1186).
fn sql_response(
    s: &AppState,
    out: &crate::analytics::QueryOutput,
    rows: SqlRows<'_>,
    watermarks: &crate::entity_view::Watermarks,
    (as_of, sealed_through): (Option<u64>, u64),
    cached: bool,
) -> axum::response::Response {
    let mut response = json!({
        "count": rows.count(),
        "truncated": out.truncated,
        // Cold data was incomplete when this answer was computed (#435): a sealed segment the
        // manifest lists could not be read, so its table was *reduced* and the query succeeded
        // with quietly less data. Reduction is the right policy (#430) - a bad segment must not
        // delete a table - but it is only defensible if the caller is told, or `SUM(value)`
        // comes back wrong rather than absent. Always present, so a caller cannot mistake the
        // healthy shape for an older build that never reported it.
        //
        // `degraded_tables` names the tables and nothing else. Table names are already public on
        // this surface (`/schema` lists them, and you must name one to query it); segment paths
        // and content addresses are not, and must never appear here - `/sql` is untrusted, which
        // is the same reason errors go through `sanitize_sql_error`.
        "degraded": out.degraded(),
        "degraded_tables": out.degraded_tables,
        // Tip failure, not per-table cold reduction (#472): the hot-tip scan itself errored, so
        // this answer is sealed-history-only regardless of what `degraded_tables` says. Distinct
        // cause (a damaged/unreadable hot store, not a bad segment) and distinct remedy, so it does
        // not belong inside `degraded_tables` - see `QueryOutput::tip_unavailable`.
        "tip_unavailable": out.tip_unavailable,
        // The query's column order (#1609), which the row objects do not keep.
        "columns": out.columns,
        "rows": match rows {
            SqlRows::Values(v) => json!(v),
            // Written in place when the body is serialized, below.
            SqlRows::Serialized { .. } => Value::Null,
        },
        // Answered from the deterministic memo (#1186): the same rows this statement produced the
        // last time every input it reads was in this state. Never stale by construction; here so a
        // caller measuring the nest can tell a computed answer from a remembered one.
        "cached": cached,
        // Provenance (RFC-0016 §4, extended by RFC-0035 §3). `registry_hash` says *how* the rows
        // were decoded; it does not say **which dataset answered**, and since RFC-0033's early
        // cutoff a result may legitimately come from data a *different* identity produced - the
        // adoption is correct, and the old stamp could not express it. `nid` closes that: an agent
        // citing an answer can now name the dataset, not just the decode.
        "provenance": {
            "as_of": as_of,
            "sealed_through": sealed_through,
            // Cold-only when the hot scan failed (#1935): the field named for the answer's source must
            // not say the tip was read when `tip_unavailable` says it was not.
            "source": if out.tip_unavailable { "sealed" } else { "hot+sealed" },
            "registry_hash": s.nest_info.get("registry_hash").and_then(Value::as_str),
            "nid": s.nid.as_deref(),
            // **#822 criterion 9, on the analytical route.** `source: hot+sealed` describes the
            // fact tables; it says nothing about a maintained relation, whose rows came from a
            // circuit rather than from a scan and are current only as far as its own watermark.
            // A caller citing this answer needs to know both. Absent when the statement
            // referenced no entity, and absent when the parse was unavailable - see
            // `QueryOutput::referenced_tables`, which is why this is not an empty array.
            "entities": sql_entity_provenance(s, out.referenced_tables.as_ref(), watermarks),
        },
    });
    // #1437, RFC-0045 stage 1's "provenance visible in the result": an answer that read an offchain
    // table, directly or through an entity, is reproducible by snapshot and says which snapshots.
    let read = out.offchain.as_ref().and_then(read_offchain_json);
    let through_entity = out.referenced_tables.as_ref().is_some_and(|names| {
        s.entities.iter().any(|e| {
            names.contains(&e.name().to_ascii_lowercase()) && !e.offchain_tables().is_empty()
        })
    });
    if let Some(provenance) = response["provenance"].as_object_mut() {
        if read.is_some() || through_entity {
            provenance.insert("reproducibility".into(), json!(SNAPSHOT_REPRODUCIBLE));
        }
        if let Some(read) = read {
            provenance.insert("offchain".into(), read);
        }
    }
    match rows {
        SqlRows::Values(_) => Json(response).into_response(),
        SqlRows::Serialized { json, .. } => match serde_json::to_vec(&WithRows(&response, json)) {
            Ok(body) => (
                [(axum::http::header::CONTENT_TYPE, "application/json")],
                body,
            )
                .into_response(),
            Err(e) => (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(json!({ "error": format!("serializing a remembered answer: {e}") })),
            )
                .into_response(),
        },
    }
}

/// The rows of a `/sql` answer: computed, or remembered already serialized (#2072).
#[derive(Clone, Copy)]
enum SqlRows<'a> {
    Values(&'a [Value]),
    Serialized {
        json: &'a serde_json::value::RawValue,
        count: usize,
    },
}

impl SqlRows<'_> {
    fn count(&self) -> usize {
        match self {
            SqlRows::Values(v) => v.len(),
            SqlRows::Serialized { count, .. } => *count,
        }
    }
}

/// A response object with its `rows` key's value written from stored JSON, in the key's own place.
struct WithRows<'a>(&'a Value, &'a serde_json::value::RawValue);

impl serde::Serialize for WithRows<'_> {
    fn serialize<S: serde::Serializer>(&self, ser: S) -> Result<S::Ok, S::Error> {
        use serde::ser::SerializeMap;
        let Some(fields) = self.0.as_object() else {
            return self.0.serialize(ser);
        };
        let mut map = ser.serialize_map(Some(fields.len()))?;
        for (k, v) in fields {
            if k == "rows" {
                map.serialize_entry(k, self.1)?;
            } else {
                map.serialize_entry(k, v)?;
            }
        }
        map.end()
    }
}

fn sql_entity_provenance(
    s: &AppState,
    referenced: Option<&std::collections::BTreeSet<String>>,
    watermarks: &crate::entity_view::Watermarks,
) -> Value {
    let Some(referenced) = referenced else {
        return Value::Null;
    };
    let head = dataset_head(s);
    let used: Vec<Value> = s
        .entities
        .iter()
        .filter(|e| referenced.contains(&e.name().to_ascii_lowercase()))
        .map(|e| {
            // #932: the watermark captured with the rows, never a fresh read. Falling back to a
            // fresh read for an entity that was not registered (faulted or unavailable, so it
            // contributed no rows) is correct - there are no rows for it to disagree with.
            let captured = watermarks.get(e.name());
            let through = captured
                .map(|a| a.through)
                .unwrap_or_else(|| e.applied_through());
            let v = json!({
                "entity": e.name(),
                "incremental": true,
                "applied_through": through,
                "current": through >= head,
            });
            with_offchain(
                v,
                e,
                captured.unwrap_or(&crate::entity_view::Applied::default()),
            )
        })
        .collect();
    if used.is_empty() {
        return Value::Null;
    }
    Value::Array(used)
}

/// #932: `applied_through` is a **parameter**, not a fresh read.
///
/// Every caller serves rows and then builds this block. Reading the watermark here would be a second
/// acquisition after the rows were taken, which is exactly how an answer came to be labelled more
/// current than the rows it was made of. Passing it in makes forgetting to capture it a compile
/// error rather than a rare wrong number.
fn derived_provenance(
    s: &AppState,
    entity: &crate::entity_view::EntityView,
    head: u64,
    applied: &crate::entity_view::Applied,
) -> Value {
    with_offchain(
        json!({
            "nid": s.nid,
            "entity": entity.name(),
            "incremental": true,
            "from_maintained_state": true,
            "applied_through": applied.through,
            "dataset_head": head,
            "current": applied.through >= head,
        }),
        entity,
        applied,
    )
}

/// **Criterion 2: a direct keyed read does not invoke Burrmill or scan canonical fact history.**
///
/// It is a `BTreeMap` lookup against the circuit's own output. There is no connection, no
/// `read_parquet`, and no hot-store scan on this path - which is the whole claim RFC-0041 makes, and
/// the reason this route exists rather than routing keyed reads through `/sql`.
async fn derived_key(
    State(s): State<AppState>,
    Path((name, key)): Path<(String, String)>,
) -> impl IntoResponse {
    let entity = match find_entity(&s, &name) {
        Ok(e) => e,
        Err(r) => return *r,
    };
    if let Some((code, body)) = derived_refusal(entity) {
        return (code, Json(body)).into_response();
    }
    let head = dataset_head(&s);

    // The key is a single column here. A composite key arrives as its parts joined by the unit
    // separator, the same character the built-in views use, so a key containing a comma or a slash
    // cannot be mistaken for a delimiter.
    let wanted = crate::entity_row::Row(
        key.split('\u{1f}')
            .map(|p| crate::entity_row::Scalar::Str(p.to_string()))
            .collect(),
    );
    let (found, applied) = entity.get_with_watermark(&wanted);
    match found {
        Some(row) => Json(json!({
            "key": key,
            "row": row.0.iter().map(|c| c.to_string()).collect::<Vec<_>>(),
            "provenance": derived_provenance(&s, entity, head, &applied),
        }))
        .into_response(),
        None => (
            StatusCode::NOT_FOUND,
            Json(json!({
                "error": "no such key",
                "entity": name,
                "key": key,
                "provenance": derived_provenance(&s, entity, head, &applied),
            })),
        )
            .into_response(),
    }
}

/// The whole maintained relation, bounded.
///
/// **Criterion 6 lives here**: returning every maintained row still pays for those rows. The limit is
/// the same shape the other listing routes use, and the response says how many rows the relation
/// holds so a caller can tell "all of it" from "the first page of it".
async fn derived_all(
    State(s): State<AppState>,
    Path(name): Path<String>,
    Query(q): Query<EntitiesQuery>,
) -> impl IntoResponse {
    let entity = match find_entity(&s, &name) {
        Ok(e) => e,
        Err(r) => return *r,
    };
    if let Some((code, body)) = derived_refusal(entity) {
        return (code, Json(body)).into_response();
    }
    let head = dataset_head(&s);
    let limit = q.limit.unwrap_or(100).min(1000);
    let (rows, first, applied) = entity.head_rows_with_watermark(limit);
    let items: Vec<Value> = first
        .iter()
        .map(|(k, v)| {
            json!({
                "key": k.0.iter().map(|c| c.to_string()).collect::<Vec<_>>(),
                "row": v.0.iter().map(|c| c.to_string()).collect::<Vec<_>>(),
            })
        })
        .collect();
    Json(json!({
        "entity": name,
        "rows": rows,
        "returned": items.len(),
        "items": items,
        "provenance": derived_provenance(&s, entity, head, &applied),
    }))
    .into_response()
}

/// Point-read a single address's derived balance.
async fn balance(State(s): State<AppState>, Path(address): Path<String>) -> impl IntoResponse {
    let address = address.to_ascii_lowercase();
    match s.balances.balance(&address) {
        // Same COR-8 caveat as `/balances`: a single address's balance is as incomplete as the set
        // it came from, and a caller reading one address should not have to query another endpoint
        // to learn that.
        Some(b) => Json(json!({
            "address": address,
            "balance": b.to_string(),
            "dropped_over_i128": s.balances.dropped_over_i128(),
        }))
        .into_response(),
        None => (
            StatusCode::NOT_FOUND,
            Json(json!({ "error": "no balance", "address": address })),
        )
            .into_response(),
    }
}

/// Direct counterparty-exposure for an address: how much it has transacted, directly, with the
/// labeled set (RFC-0008 C1). Amounts are i128 base units, serialised as decimal strings (same reason
/// as balances). A reorg retracts through the IVM view, so this is always the canonical-chain figure.
async fn exposure(State(s): State<AppState>, Path(address): Path<String>) -> impl IntoResponse {
    let address = address.to_ascii_lowercase();
    let items: Vec<Value> = s
        .exposure
        .exposure(&address)
        .into_iter()
        .map(|r| {
            json!({
                "label": r.label,
                "direction": r.direction,
                "count": r.count.to_string(),
                "amount": r.amount.to_string(),
            })
        })
        .collect();
    Json(json!({ "address": address, "count": items.len(), "exposure": items }))
}

#[derive(Deserialize)]
struct FlagsQuery {
    kind: Option<String>,
    limit: Option<usize>,
}

/// Compliance flags (RFC-0008 C3). `?kind=velocity` returns the live windowed velocity flags (address
/// volume ≥ the configured threshold within a block-window); `?kind=threshold` returns recent
/// `threshold_flag` annotations (hot store; the full sealed history is at `/sql SELECT * FROM
/// threshold_flag`). Omit `kind` for both. Amounts are i128 base units, serialised as decimal strings.
async fn flags(State(s): State<AppState>, Query(q): Query<FlagsQuery>) -> impl IntoResponse {
    let limit = q.limit.unwrap_or(100).min(1000);
    let kind = q.kind.as_deref();

    let velocity = |s: &AppState| -> Vec<Value> {
        let threshold = s.velocity_threshold.unwrap_or(i128::MIN);
        s.velocity
            .flags(threshold)
            .into_iter()
            .take(limit)
            .map(|f| {
                json!({
                    "address": f.address,
                    "window_start": f.window_start,
                    "count": f.count.to_string(),
                    "volume": f.volume.to_string(),
                })
            })
            .collect()
    };
    // Recent threshold_flag annotations from the hot store (newest first). Sealed history via /sql.
    let threshold = |s: &AppState| -> Vec<Value> {
        s.store
            .recent_by_table("threshold_flag", limit)
            .unwrap_or_default()
            .iter()
            .filter_map(|r| serde_json::from_str::<Value>(r).ok())
            .collect()
    };

    match kind {
        Some("velocity") => Json(json!({
            "kind": "velocity",
            "threshold": s.velocity_threshold.map(|t| t.to_string()),
            "flags": velocity(&s),
        }))
        .into_response(),
        Some("threshold") => Json(json!({
            "kind": "threshold",
            "threshold": s.threshold.map(|t| t.to_string()),
            "flags": threshold(&s),
            "note": "recent hot flags; full sealed history: /sql?q=SELECT * FROM threshold_flag",
        }))
        .into_response(),
        Some(other) => (
            StatusCode::BAD_REQUEST,
            Json(json!({ "error": format!("unknown flag kind '{other}' (want: threshold, velocity)") })),
        )
            .into_response(),
        None => Json(json!({
            "threshold": { "configured": s.threshold.map(|t| t.to_string()), "flags": threshold(&s) },
            "velocity": { "configured": s.velocity_threshold.map(|t| t.to_string()), "flags": velocity(&s) },
        }))
        .into_response(),
    }
}

/// Parse an entity id `{block:012}-{log_index:06}` back into its components.
fn parse_id(id: &str) -> Option<(u64, u64)> {
    let (b, l) = id.split_once('-')?;
    Some((b.parse().ok()?, l.parse().ok()?))
}

fn not_found(id: &str) -> axum::response::Response {
    (
        StatusCode::NOT_FOUND,
        Json(json!({ "error": "not found", "id": id })),
    )
        .into_response()
}

fn error(msg: String) -> axum::response::Response {
    // SEC-11: internal error chains include absolute segment/dir paths (a filesystem-layout disclosure).
    // Log the detail server-side; hand the client a generic message.
    tracing::warn!("internal error serving request: {msg}");
    (
        StatusCode::INTERNAL_SERVER_ERROR,
        Json(json!({ "error": "internal error" })),
    )
        .into_response()
}

/// Constant-time string equality for the admin token - no early return on the first differing byte, so
/// an attacker can't time-recover the secret. The length check leaks only the length, which is fixed
/// for a random token.
fn ct_eq(a: &str, b: &str) -> bool {
    let (a, b) = (a.as_bytes(), b.as_bytes());
    if a.len() != b.len() {
        return false;
    }
    let mut diff = 0u8;
    for (x, y) in a.iter().zip(b) {
        diff |= x ^ y;
    }
    diff == 0
}

/// Redact the nest-directory prefix from an engine error before it's relayed to a `/sql`/`/explain`
/// client (SEC review). Engine errors embed segment paths (`…/mynest/segments/foo.parquet`) in
/// read errors; the useful part for the caller is the table/column/type detail, not the on-disk
/// layout. We keep the message and replace only the dir prefix with `<nest>`.
fn sanitize_sql_error(raw: &str, dir: &std::path::Path) -> String {
    let mut out = raw.to_string();
    // A runtime dataset lives at `<root>/data/<nid>`, and its shared segments under `<root>/segments`.
    let root = dir
        .parent()
        .filter(|p| p.file_name().is_some_and(|n| n == crate::runtime::DATA_DIR))
        .and_then(|p| p.parent());
    // Both the canonical and as-configured forms - the error could carry either. The dataset first:
    // it is the longer prefix, and the runtime root is inside it.
    let mut prefixes: Vec<(std::path::PathBuf, &str)> = Vec::new();
    for (p, label) in [(Some(dir), "<nest>"), (root, "<runtime>")] {
        if let Some(p) = p {
            prefixes.extend(p.canonicalize().ok().map(|c| (c, label)));
            prefixes.push((p.to_path_buf(), label));
        }
    }
    for (p, label) in prefixes {
        // Only an *absolute* prefix is worth redacting, and only an absolute one is safe to. The
        // default `--dir` is `.`, which as a plain `replace` target matches every full stop in the
        // message: `1.5` became `1<nest>5` and an `...` ellipsis became `<nest><nest><nest>`. The
        // engine embeds the absolute form, or that form with its `/` stripped (below), so nothing is
        // lost by skipping the relative one.
        if !p.is_absolute() {
            continue;
        }
        let s = p.display().to_string();
        out = out.replace(&s, label);
        // object_store prints a filesystem path with its leading `/` stripped (#1651). Only a path of
        // two or more components: `/data` stripped is the word `data`, which errors use freely.
        let bare = s.trim_start_matches('/');
        if bare.contains('/') {
            out = out.replace(bare, label);
        }
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::store::Store;

    #[cfg(feature = "graph")]
    #[test]
    fn a_derived_single_and_a_second_to_one_shape_as_graph_node_answers() {
        use crate::graph_query::{Compiled, Nested, Shape};

        let compiled = Compiled {
            sql: String::new(),
            shape: vec![
                Shape::Object {
                    key: "gameToken".into(),
                    marker: "gt".into(),
                    fields: vec![],
                    lists: vec![],
                    objects: vec![Nested {
                        key: "token".into(),
                        marker: "t".into(),
                        fields: vec![("symbol".into(), "t_symbol".into())],
                    }],
                },
                Shape::DerivedOne {
                    key: "weightedGameBet".into(),
                    col: "w".into(),
                    field: "weightedGameBet".into(),
                    child: "WeightedGameBet".into(),
                    back: "bet".into(),
                },
            ],
            entity: "Bet".into(),
            singular: false,
            min_block: None,
        };
        let shape = |row: serde_json::Value| graph_shape(&compiled, row.as_object().unwrap());
        assert_eq!(
            shape(serde_json::json!({"gt": "Dice-0x0", "t": "0x0", "t_symbol": "ETH", "w": "[]"}))
                .unwrap(),
            serde_json::json!({"gameToken": {"token": {"symbol": "ETH"}}, "weightedGameBet": null})
        );
        assert_eq!(
            shape(serde_json::json!({"gt": "Dice-0x0", "t": null, "t_symbol": null, "w": r#"[{"id":"7"}]"#}))
                .unwrap(),
            serde_json::json!({"gameToken": {"token": null}, "weightedGameBet": {"id": "7"}}),
            "an absent second relation is null, not an object of nulls"
        );
        let lists = Compiled {
            sql: String::new(),
            shape: vec![Shape::Json {
                key: "rolled".into(),
                col: "a0".into(),
            }],
            entity: "Bet".into(),
            singular: false,
            min_block: None,
        };
        let list_row = |v: serde_json::Value| {
            graph_shape(&lists, serde_json::json!({ "a0": v }).as_object().unwrap()).unwrap()
        };
        assert_eq!(
            list_row(serde_json::json!(r#"["3","5"]"#)),
            serde_json::json!({"rolled": ["3", "5"]}),
            "a stored list is an array, not the JSON text it travelled as"
        );
        assert_eq!(
            list_row(serde_json::Value::Null),
            serde_json::json!({"rolled": null})
        );
        let err =
            shape(serde_json::json!({"gt": null, "t": null, "w": r#"[{"id":"7"},{"id":"8"}]"#}))
                .unwrap_err();
        assert_eq!(
            err,
            "Ambiguous result for derived field `weightedGameBet`: Multiple `WeightedGameBet` entities refer back via `bet`"
        );
    }

    #[cfg(feature = "graph")]
    #[test]
    fn a_related_derived_list_is_shaped_inside_its_parent_object() {
        use crate::graph_query::{Compiled, Shape};

        let compiled = Compiled {
            sql: String::new(),
            shape: vec![Shape::Object {
                key: "payer".into(),
                marker: "payer_present".into(),
                fields: vec![("id".into(), "payer_id".into())],
                lists: vec![("signers".into(), "payer_signers".into())],
                objects: vec![],
            }],
            entity: "PaymentsEscrowAccount".into(),
            singular: false,
            min_block: None,
        };
        let row = serde_json::json!({
            "payer_present": "0xpayer",
            "payer_id": "0xpayer",
            "payer_signers": r#"[{"id":"0xsigner"}]"#,
        });
        let shaped = graph_shape(&compiled, row.as_object().unwrap()).unwrap();
        assert_eq!(
            shaped,
            serde_json::json!({
                "payer": {"id": "0xpayer", "signers": [{"id": "0xsigner"}]}
            }),
            "an escrow payer's derived signers list must remain a list, not a JSON string"
        );
    }

    #[test]
    fn readiness_stall_logic() {
        let now = 1_000_000u64;
        // Never polled, just started → starting up, not stalled (grace).
        assert!(!poll_stalled(0, now - 10, now, 90));
        // Polled recently → healthy.
        assert!(!poll_stalled(now - 10, now - 1_000, now, 90));
        // Polled long ago → stalled.
        assert!(poll_stalled(now - 100, now - 1_000, now, 90));
        // Exactly at the threshold is not yet stalled (strictly greater).
        assert!(!poll_stalled(now - 90, now - 1_000, now, 90));
        // Neither timestamp stamped (an un-stamped fixture) → old unconditional grace, not stalled.
        assert!(!poll_stalled(0, 0, now, 90));
    }

    #[test]
    fn a_failed_first_poll_is_unready_without_waiting_out_startup_grace() {
        assert!(initial_poll_failed(0, true));
        assert!(!initial_poll_failed(0, false));
        assert!(!initial_poll_failed(123, true));
    }

    /// #510: a pool that has been unreachable since before the very first poll must eventually report
    /// `stalled`, not stay a permanent "just starting up". Never-polled (`last_poll == 0`) alone used to
    /// mean forever-fresh; the fix bounds that grace by how long ago the nest started trying.
    #[test]
    fn a_pool_dead_since_cold_start_eventually_stalls() {
        let now = 2_000_000u64;
        // Started 91s ago, never once polled successfully - past the 90s grace, so stalled.
        assert!(poll_stalled(0, now - 91, now, 90));
        // Started 10s ago, never polled - still within grace.
        assert!(!poll_stalled(0, now - 10, now, 90));
    }

    /// #578: the same shape as `readiness_stall_logic`, but for the dimension `poll_stalled` cannot see -
    /// whether the cursor is actually advancing, not just whether the source answered.
    #[test]
    fn progress_stall_logic() {
        let now = 1_000_000u64;
        // Never advanced, just started → starting up, not wedged (grace).
        assert!(!progress_stalled(0, now - 10, now, 90, 5));
        // Advanced recently → healthy.
        assert!(!progress_stalled(now - 10, now - 1_000, now, 90, 5));
        // Advanced long ago and nothing since, and still behind tip → wedged.
        assert!(progress_stalled(now - 100, now - 1_000, now, 90, 5));
        // Exactly at the threshold is not yet wedged (strictly greater).
        assert!(!progress_stalled(now - 90, now - 1_000, now, 90, 5));
    }

    /// #583/#589: a cursor level with tip must never be wedged, no matter how stale `last_progress` is -
    /// it stops stamping the moment it catches up, by design (`set_last_block` only stamps on an actual
    /// advance). Without the `lag > 0` guard this fires `threshold` seconds after every caught-up nest
    /// arrives at tip, and forever on a completed fixed-range nest.
    #[test]
    fn a_caught_up_cursor_is_never_wedged() {
        let now = 1_000_000u64;
        assert!(!progress_stalled(now - 1_000, now - 1_000, now, 90, 0));
        assert!(!progress_stalled(0, now - 1_000, now, 90, 0));
    }

    /// #510's analogue for progress: a cursor wedged since before it ever advanced once must eventually
    /// report unready too, not stay "just starting up" forever.
    #[test]
    fn a_cursor_wedged_since_cold_start_eventually_stalls() {
        let now = 2_000_000u64;
        assert!(progress_stalled(0, now - 91, now, 90, 5));
        assert!(!progress_stalled(0, now - 10, now, 90, 5));
    }

    #[test]
    fn ct_eq_matches_only_identical_strings() {
        assert!(ct_eq("s3cret", "s3cret"));
        assert!(!ct_eq("s3cret", "s3creT"));
        assert!(!ct_eq("s3cret", "s3cre")); // length differs
        assert!(!ct_eq("", "x"));
        assert!(ct_eq("", ""));
    }

    #[test]
    fn sanitize_sql_error_redacts_the_nest_dir() {
        let dir = std::path::Path::new("/var/lib/nuthatch/mynest");
        let raw = "Object Store error: Object at location \
                   /var/lib/nuthatch/mynest/segments/usdc__transfer-abc.parquet not found: No such \
                   file or directory (os error 2)";
        let out = sanitize_sql_error(raw, dir);
        assert!(
            !out.contains("/var/lib/nuthatch/mynest"),
            "dir prefix redacted"
        );
        assert!(out.contains("<nest>/segments/usdc__transfer-abc.parquet"));
        // The useful detail (the message + filename) survives.
        assert!(out.contains("not found"));
    }

    /// #1651: object_store prints a filesystem path without its leading `/`, and a runtime dataset's
    /// shared segments live two levels up from it. Neither form may reach a caller.
    #[test]
    fn sanitize_sql_error_redacts_object_store_paths_and_the_shared_store() {
        let dir = std::path::Path::new("/srv/nuthatch/runtime/data/abc123");
        let own = sanitize_sql_error(
            "Object at location srv/nuthatch/runtime/data/abc123/segments/t.parquet not found",
            dir,
        );
        assert_eq!(
            own,
            "Object at location <nest>/segments/t.parquet not found"
        );
        let shared = sanitize_sql_error(
            "Object at location srv/nuthatch/runtime/segments/f00d.parquet not found",
            dir,
        );
        assert_eq!(
            shared,
            "Object at location <runtime>/segments/f00d.parquet not found"
        );
        let slashed =
            sanitize_sql_error("no file /srv/nuthatch/runtime/segments/f00d.parquet", dir);
        assert_eq!(slashed, "no file <runtime>/segments/f00d.parquet");
        let short = std::path::Path::new("/data");
        assert_eq!(
            sanitize_sql_error("column data not found in /data/x.parquet", short),
            "column data not found in <nest>/x.parquet"
        );
    }

    /// The default `--dir` is `.`, and a bare `replace(".", "<nest>")` corrupted every message that
    /// contained a full stop - decimals, qualified names, and an `...` ellipsis alike. Observed
    /// live on `nuthatch dev`: `SELECT 1.5 + bogus` came back as `SELECT 1<nest>5`.
    #[test]
    fn sanitize_sql_error_leaves_full_stops_alone_for_a_relative_dir() {
        let raw = "Binder Error: Referenced column \"bogus\" not found!\n\
                   LINE 1: SELECT 1.5 + bogus FROM usdc__transfer ORDER BY block_number DESC...";
        for dir in [".", "..", "./"] {
            let out = sanitize_sql_error(raw, std::path::Path::new(dir));
            assert_eq!(
                out, raw,
                "a relative --dir ({dir}) must not rewrite the message"
            );
        }
        // ...and the redaction it exists for still happens via the canonical (absolute) form.
        let tmp = std::env::temp_dir();
        let raw_abs = format!(
            "IO Error: no such file \"{}/segments/x.parquet\"",
            tmp.display()
        );
        let out = sanitize_sql_error(&raw_abs, &tmp);
        assert!(
            out.contains("<nest>/segments/x.parquet"),
            "absolute prefix still redacted"
        );
    }

    /// A minimal but real `AppState` - enough to drive the analytical handlers directly (no HTTP
    /// harness). `permits` seeds the admission gate so a test can saturate it.
    /// #866's readiness rule, at its edges. Every one of these reading wrong gives a nest that is
    /// working fine and permanently unready, which is the failure operators learn to ignore.
    #[test]
    fn an_entity_is_wedged_only_when_it_is_behind_and_has_stopped_moving() {
        let stall = READINESS_PROGRESS_STALL_SECS;
        let now = 10_000;

        assert!(
            entity_wedged(5, 100, now - stall - 1, now),
            "behind, and no progress for longer than the threshold"
        );
        assert!(
            !entity_wedged(5, 100, now - stall + 1, now),
            "behind but still advancing is catching up, not wedged - and it may be for hours"
        );
        assert!(
            !entity_wedged(100, 100, now - stall - 1, now),
            "level with the head is not behind, however long ago that happened"
        );
        assert!(
            !entity_wedged(101, 100, now - stall - 1, now),
            "ahead of the head is not behind either"
        );
        assert!(
            !entity_wedged(0, 0, now - stall - 1, now),
            "a nest that has indexed nothing has no head to be behind"
        );
        assert!(
            !entity_wedged(0, 100, 0, now),
            "an entity that has never folded a batch is waiting, not wedged"
        );
    }

    /// **#822 criterion 8**, at the one place both routes consult.
    ///
    /// The keyed route and the listing route must refuse *consistently*, which they can only do by
    /// sharing the decision. This asserts the decision itself: an entity holding no answer is a 503
    /// with a reason, and a healthy one is not refused at all.
    ///
    /// Being *behind* is deliberately not a refusal - that is the ordinary state during backfill and
    /// can last hours - so it is served with `current: false` and the entity's own watermark. The
    /// failure this guards is the other one: refusing nothing and serving an empty relation as though
    /// it were the truth.
    #[test]
    fn a_derived_route_refuses_an_entity_that_holds_no_answer() {
        fn cols() -> Vec<String> {
            vec!["to".into(), "sum_value".into()]
        }

        use crate::entity_expr::Expr;
        use crate::entity_plan::{Agg, Plan, Source};

        let abi: alloy_json_abi::JsonAbi = serde_json::from_str(
            r#"[{"type":"event","name":"Transfer","inputs":[
                {"name":"from","type":"address","indexed":true},
                {"name":"to","type":"address","indexed":true},
                {"name":"value","type":"uint256","indexed":false}],"anonymous":false}]"#,
        )
        .unwrap();
        let reg = crate::registry::DecodeRegistry::build(vec![crate::registry::ContractSpec {
            alias: "usdc".into(),
            address: "0xa0b86991c6218b36c1d19d4a2e9eb0ce3606eb48"
                .parse()
                .unwrap(),
            abi,
            events: Vec::new(),
        }])
        .unwrap();
        let plan = Plan {
            left: Source {
                table: "usdc__transfer".into(),
                columns: vec!["to".into(), "value".into()],
            },
            left_filter: None,
            join: None,
            key: vec![Expr::Column(0)],
            aggregates: vec![Agg::Sum(Expr::Column(1))],
        };

        // Warm-started: no state, and no way to rebuild it until the seed runs.
        let unavailable =
            crate::entity_view::EntityView::start("e", &plan, &cols(), &reg, 1_000, true).unwrap();
        let (code, body) = derived_refusal(&unavailable).expect("an unavailable entity is refused");
        assert_eq!(code, StatusCode::SERVICE_UNAVAILABLE);
        assert_eq!(body["error"], "entity unavailable");
        assert!(
            body["reason"].as_str().is_some_and(|r| !r.is_empty()),
            "the refusal must carry why, not merely that: {body}"
        );

        // Cold-started: nothing folded yet, but nothing wrong either.
        let healthy =
            crate::entity_view::EntityView::start("e", &plan, &cols(), &reg, 1_000, false).unwrap();
        assert!(
            derived_refusal(&healthy).is_none(),
            "an empty-but-healthy entity answers; being behind is not an error"
        );

        // Faulted: a *different* refusal, and it has to be reached through its own branch. Testing
        // only the unavailable case left a mutation that stops refusing faulted entities alive - a
        // dead circuit would have gone on serving whatever it held when it died.
        let dead =
            crate::entity_view::EntityView::start("e", &plan, &cols(), &reg, 1, false).unwrap();
        let row = |to: &str, v: i128| {
            crate::entity_row::Row(vec![
                crate::entity_row::Scalar::Str(to.into()),
                crate::entity_row::Scalar::Int(v),
            ])
        };
        dead.apply(
            crate::entity_view::Batch {
                left: vec![(row("0xa", 1), 1), (row("0xb", 2), 1)],
                right: Vec::new(),
            },
            10,
        );
        dead.flush();
        let (code, body) = derived_refusal(&dead).expect("a faulted entity is refused");
        assert_eq!(code, StatusCode::SERVICE_UNAVAILABLE);
        assert_eq!(body["error"], "entity faulted");
        assert!(
            body["reason"]
                .as_str()
                .is_some_and(|r| r.contains("max_rows")),
            "the refusal must name the cause: {body}"
        );
    }

    /// #1437: readiness and `/derived` provenance name the offchain snapshots an entity's rows were
    /// applied through, and a chain-only entity's output is unchanged.
    #[test]
    fn an_offchain_entity_names_the_snapshots_it_applied() {
        use crate::entity_plan::{Agg, Plan, Source};
        let dir = tempfile::tempdir().unwrap();
        let csv = dir.path().join("tiers.csv");
        std::fs::write(&csv, "account,tier\n0xa,gold\n").unwrap();
        crate::offchain::drop_file(dir.path(), &csv, "tiers").unwrap();
        let offchain = crate::entity_offchain::Tables::load(dir.path()).unwrap();
        let tiers = offchain.table("tiers").unwrap().unwrap();
        let reg = crate::registry::DecodeRegistry::build(Vec::new()).unwrap();
        let plan = Plan {
            left: Source {
                table: "offchain__tiers".into(),
                columns: vec!["tier".into()],
            },
            left_filter: None,
            join: None,
            key: vec![crate::entity_expr::Expr::Column(0)],
            aggregates: vec![Agg::Count],
        };
        let binding =
            crate::entity_bind::Binding::bind_with_offchain(&plan, &reg, &offchain).unwrap();
        let columns = vec!["tier".to_string(), "n".to_string()];
        let entity = crate::entity_view::EntityView::start_bound(
            "by_tier", &plan, binding, &columns, 100, false,
        )
        .unwrap();
        entity
            .apply_offchain("tiers", tiers.snapshots(), &tiers.version())
            .unwrap();
        entity.flush();
        let state = AppState {
            entities: Arc::new(vec![entity]),
            #[cfg(feature = "folds")]
            folds: None,
            ..test_state(dir.path(), 1)
        };

        let (ready, _) = entity_readiness(&state, 0, 0);
        let label = &ready[0]["offchain"]["offchain__tiers"];
        assert_eq!(label["snapshots"], 1, "{ready}");
        assert_eq!(label["latest"], tiers.version()[0].as_str(), "{ready}");

        let (_, applied) = state.entities[0].len_and_watermark();
        let provenance = derived_provenance(&state, &state.entities[0], 0, &applied);
        assert_eq!(provenance["offchain"], ready[0]["offchain"], "{provenance}");

        let referenced: std::collections::BTreeSet<String> = ["by_tier".to_string()].into();
        let captured: crate::entity_view::Watermarks =
            [("by_tier".to_string(), applied.clone())].into();
        let sql = sql_entity_provenance(&state, Some(&referenced), &captured);
        assert_eq!(sql[0]["offchain"], ready[0]["offchain"], "{sql}");

        assert_eq!(ready[0]["reproducibility"], "snapshot", "{ready}");
        assert_eq!(provenance["reproducibility"], "snapshot", "{provenance}");
        // The label follows from what the entity reads, so it is there before anything is applied.
        let unapplied = derived_provenance(
            &state,
            &state.entities[0],
            0,
            &crate::entity_view::Applied::default(),
        );
        assert_eq!(unapplied["reproducibility"], "snapshot", "{unapplied}");
        assert!(unapplied.get("offchain").is_none(), "{unapplied}");

        let erc20: alloy_json_abi::JsonAbi = serde_json::from_str(
            r#"[{"type":"event","name":"Transfer","anonymous":false,"inputs":[
                {"name":"from","type":"address","indexed":true},
                {"name":"to","type":"address","indexed":true},
                {"name":"value","type":"uint256","indexed":false}]}]"#,
        )
        .unwrap();
        let chain_reg =
            crate::registry::DecodeRegistry::build(vec![crate::registry::ContractSpec {
                alias: "usdc".into(),
                address: alloy_primitives::Address::from([0x11; 20]),
                abi: erc20,
                events: Vec::new(),
            }])
            .unwrap();
        let chain_plan = Plan {
            left: Source {
                table: "usdc__transfer".into(),
                columns: vec!["to".into()],
            },
            ..plan
        };
        let chain_only = crate::entity_view::EntityView::start(
            "e",
            &chain_plan,
            &columns,
            &chain_reg,
            100,
            false,
        )
        .unwrap();
        let chain = derived_provenance(
            &state,
            &chain_only,
            0,
            &crate::entity_view::Applied::default(),
        );
        assert!(chain.get("reproducibility").is_none(), "{chain}");
        assert!(chain.get("offchain").is_none(), "{chain}");
    }

    /// The `/sql` concurrency sweep through the router: the permits, the
    /// 250 ms admission wait and the 503 past it, as production serves them. Every authored view is
    /// read whole, each client starting at its own offset. Ignored unless `NUTHATCH_SWEEP_NEST`
    /// names a nest; run it on a copy, since the store is opened inside it. With the memo on, a
    /// repeated view never reaches the engine, so `NUTHATCH_SQL_MEMO_BYTES=0` measures the engine.
    ///
    ///     NUTHATCH_SWEEP_NEST=/copy/of/nest CLIENTS=1,8,32 \
    ///         cargo test --release --lib sql_sweep -- --ignored --nocapture
    #[test]
    #[ignore]
    fn sql_sweep_over_a_nest() {
        use std::sync::atomic::Ordering;
        use tower::ServiceExt;
        let Ok(nest) = std::env::var("NUTHATCH_SWEEP_NEST") else {
            return;
        };
        let dir = std::path::PathBuf::from(nest);
        let secs: u64 = std::env::var("SECONDS")
            .ok()
            .and_then(|s| s.parse().ok())
            .unwrap_or(10);
        let counts: Vec<usize> = std::env::var("CLIENTS")
            .unwrap_or_else(|_| "1,2,4,8,16,32".into())
            .split(',')
            .filter_map(|c| c.trim().parse().ok())
            .collect();
        let mut views: Vec<String> = crate::analytics::nest_view_files(&dir)
            .iter()
            .flat_map(|f| crate::analytics::split_sql_statements(&f.sql))
            .filter_map(|stmt| crate::analytics::view_name(&stmt))
            .collect();
        views.sort();
        let encode = |sql: &str| -> String {
            sql.bytes()
                .map(|b| match b {
                    b'A'..=b'Z' | b'a'..=b'z' | b'0'..=b'9' | b'-' | b'_' | b'.' => {
                        (b as char).to_string()
                    }
                    b => format!("%{b:02X}"),
                })
                .collect()
        };
        let uris: Arc<Vec<String>> = Arc::new(
            views
                .iter()
                .map(|v| format!("/sql?q={}", encode(&format!("SELECT * FROM \"{v}\""))))
                .collect(),
        );
        let rt = tokio::runtime::Builder::new_multi_thread()
            .enable_all()
            .build()
            .unwrap();
        // The declared tables as production passes them, so a view over an event that never fired
        // binds to an empty typed table (#663) rather than failing.
        let mut state = test_state(&dir, SQL_MAX_CONCURRENCY);
        let config = crate::config::Config::load(&dir).unwrap();
        let registry = crate::registry::from_nest(&dir, &config).unwrap();
        state.tables = Arc::new(crate::indexer::full_schema(&registry, &config));
        let app = router(SharedNest::new(state));
        println!(
            "SWEEP\tviews={}\tpermits={SQL_MAX_CONCURRENCY}\tseconds={secs}",
            views.len()
        );
        // One pass first, so no point pays the first open of the nest's session.
        rt.block_on(async {
            for u in uris.iter() {
                let req = axum::http::Request::builder()
                    .uri(u)
                    .body(axum::body::Body::empty())
                    .unwrap();
                let resp = app.clone().oneshot(req).await.unwrap();
                let status = resp.status();
                let body = axum::body::to_bytes(resp.into_body(), usize::MAX)
                    .await
                    .unwrap();
                assert_eq!(
                    status,
                    StatusCode::OK,
                    "{u}: {}",
                    String::from_utf8_lossy(&body)
                );
            }
        });
        let peak = Arc::new(std::sync::atomic::AtomicU64::new(0));
        for &n in &counts {
            let stop = std::time::Instant::now() + std::time::Duration::from_secs(secs);
            let sampling = Arc::new(std::sync::atomic::AtomicBool::new(true));
            let sampler = {
                let (peak, sampling) = (peak.clone(), sampling.clone());
                std::thread::spawn(move || {
                    while sampling.load(Ordering::Relaxed) {
                        peak.fetch_max(crate::metrics::rss_bytes(), Ordering::Relaxed);
                        std::thread::sleep(std::time::Duration::from_millis(20));
                    }
                })
            };
            let results: Vec<(Vec<u128>, u64, u64)> = rt.block_on(async {
                let tasks: Vec<_> = (0..n)
                    .map(|c| {
                        let (app, uris) = (app.clone(), uris.clone());
                        tokio::spawn(async move {
                            let (mut ok, mut busy, mut other) = (Vec::new(), 0u64, 0u64);
                            let mut i = c;
                            while std::time::Instant::now() < stop {
                                let req = axum::http::Request::builder()
                                    .uri(&uris[i % uris.len()])
                                    .body(axum::body::Body::empty())
                                    .unwrap();
                                i += 1;
                                let t = std::time::Instant::now();
                                let resp = app.clone().oneshot(req).await.unwrap();
                                let status = resp.status();
                                let _ = axum::body::to_bytes(resp.into_body(), usize::MAX).await;
                                match status {
                                    StatusCode::OK => ok.push(t.elapsed().as_millis()),
                                    StatusCode::SERVICE_UNAVAILABLE => busy += 1,
                                    _ => other += 1,
                                }
                            }
                            (ok, busy, other)
                        })
                    })
                    .collect();
                let mut out = Vec::new();
                for t in tasks {
                    out.push(t.await.unwrap());
                }
                out
            });
            sampling.store(false, Ordering::Relaxed);
            sampler.join().unwrap();
            let mut all: Vec<u128> = results.iter().flat_map(|r| r.0.iter().copied()).collect();
            all.sort_unstable();
            let pct = |p: f64| {
                all.get(((all.len() as f64 - 1.0) * p).round() as usize)
                    .copied()
                    .unwrap_or(0)
            };
            let busy: u64 = results.iter().map(|r| r.1).sum();
            let other: u64 = results.iter().map(|r| r.2).sum();
            let per: Vec<usize> = results.iter().map(|r| r.0.len()).collect();
            println!(
                "POINT\tclients={n}\tok_qps={:.1}\tp50_ms={}\tp99_ms={}\tmax_ms={}\tbusy_503={busy}\tother={other}\tok_per_client={}..{}\tpeak_rss_mb={}",
                all.len() as f64 / secs as f64,
                pct(0.50),
                pct(0.99),
                all.last().copied().unwrap_or(0),
                per.iter().min().unwrap_or(&0),
                per.iter().max().unwrap_or(&0),
                peak.load(Ordering::Relaxed) / (1024 * 1024),
            );
        }
    }

    fn test_state(dir: &std::path::Path, permits: usize) -> AppState {
        AppState {
            store: std::sync::Arc::new(Store::open(&dir.join("t.redb")).unwrap()),
            address: Some("0x0".into()),
            chain: "ethereum".into(),
            dir: dir.to_path_buf(),
            balances: BalanceView::start().unwrap(),
            exposure: ExposureView::start(true).unwrap(),
            velocity: VelocityView::start(true).unwrap(),
            entities: Arc::new(Vec::new()),
            #[cfg(feature = "folds")]
            folds: None,
            threshold: None,
            velocity_threshold: None,
            tables: Arc::new(vec![]),
            sql_gate: Arc::new(Semaphore::new(permits)),
            cursorless: false,
            seal_span: crate::chains::DEFAULT_SEAL_SPAN,
            freshness: Default::default(),
            sql_queued: Default::default(),
            sql_max_hot_rows: SQL_MAX_HOT_ROWS,
            sql_max_hot_bytes: SQL_MAX_HOT_BYTES,
            sql_max_named_scan_bytes: SQL_MAX_NAMED_SCAN_BYTES,
            surface: Arc::new(crate::allowlist::Surface::default()),
            #[cfg(feature = "counter")]
            counter: None,
            nid: None,
            admin_enabled: true,
            admin_token: None,
            nest_info: Arc::new(json!({ "name": "t" })),
            runtime_health: None,
            address_history: None,
        }
    }

    fn rejections(reason: &str) -> Option<u64> {
        let needle = format!("nuthatch_sql_rejections_total{{reason=\"{reason}\"}} ");
        crate::metrics::METRICS
            .render()
            .lines()
            .find_map(|l| l.strip_prefix(needle.as_str())?.parse().ok())
    }

    /// #1714: the 4.1.1 hash-join OOM on Lodestar was counted as `invalid`, a caller mistake, so no
    /// alert could tell the engine running out of memory from a typo.
    #[test]
    fn an_engine_out_of_memory_is_counted_as_its_own_rejection() {
        let dir = tempfile::tempdir().unwrap();
        let s = test_state(dir.path(), 1);
        let before = rejections("out_of_memory").unwrap_or(0);
        let oom = anyhow::anyhow!(
            "refused executing: substrate error: Out of Memory Error: Failed to allocate additional \
             2012.3 KB for HashJoinInput[7] with 1973.3 KB already allocated for this reservation"
        );
        let _ = sql_error_response(&s, oom, "SELECT 1");
        let after = rejections("out_of_memory").expect("an out_of_memory reason series");
        assert!(
            after > before,
            "OOM must count as out_of_memory: {before} -> {after}"
        );

        let echo = anyhow::anyhow!(
            r#"Binder Error: Referenced column "Out of Memory Error" not found in FROM clause!"#
        );
        let _ = sql_error_response(&s, echo, r#"SELECT "Out of Memory Error""#);
        assert_eq!(
            rejections("out_of_memory"),
            Some(after),
            "a binder echo of the phrase is the caller's text, not an OOM"
        );
    }

    #[cfg(feature = "graph")]
    #[tokio::test]
    async fn graph_history_pins_entities_and_meta_to_the_same_checkpoint() {
        let dir = tempfile::tempdir().unwrap();
        std::fs::create_dir_all(dir.path().join("graph")).unwrap();
        std::fs::create_dir_all(dir.path().join("views")).unwrap();
        std::fs::write(
            dir.path().join("graph/schema.graphql"),
            "type Account @entity { id: ID! balance: BigInt! }",
        )
        .unwrap();
        std::fs::write(
            dir.path().join("graph/history.toml"),
            "version = 1\nfirst_block = 10\nmax_head_age_seconds = 15\n",
        )
        .unwrap();
        std::fs::write(dir.path().join("schema.json"), r#"{"tables":[{"table":"changes","columns":[{"name":"block_number","storage":"u64"},{"name":"value","storage":"varchar"}]}]}"#).unwrap();
        std::fs::write(dir.path().join("views/account.sql"), "CREATE VIEW account AS SELECT 'a' AS id, sum(CAST(value AS BIGINT)) AS balance FROM changes;").unwrap();
        let state = test_state(dir.path(), SQL_MAX_CONCURRENCY);
        let old_hash = format!("0x{}", "11".repeat(32));
        let hash = format!("0x{}", "22".repeat(32));
        let now = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_secs();
        for (block, value, hash) in [(10, "5", &old_hash), (20, "7", &hash)] {
            state
                .store
                .put_entity(
                    &Store::entity_key(block, 0),
                    &json!({"table":"changes","block_number":block,"value":value}).to_string(),
                )
                .unwrap();
            state.store.set_block_hash(block, hash).unwrap();
            state.store.set_block_timestamp(block, now).unwrap();
        }
        state.store.set_meta("last_block", "20").unwrap();
        state
            .store
            .put_entity(
                &Store::entity_key(15, 0),
                &json!({"table":"changes","block_number":15,"value":"4"}).to_string(),
            )
            .unwrap();
        let answer = graph_ask(
            "/graphql",
            &format!(
                r#"{{
            old: account(id: "a", block: {{hash: "{old_hash}"}}) {{ balance }}
            before: account(id: "a", block: {{number: 14}}) {{ balance }}
            between: account(id: "a", block: {{number: 15}}) {{ balance }}
            latest: account(id: "a") {{ balance }}
            meta: _meta(block: {{number: 10}}) {{ block {{ number hash }} }}
        }}"#
            ),
            state.clone(),
        )
        .await;
        assert_eq!(
            answer,
            json!({"data": {"old":{"balance":"5"},"before":{"balance":"5"},"between":{"balance":"9"},"latest":{"balance":"16"},
            "meta":{"block":{"number":10,"hash":old_hash}}}})
        );
        let numeric_meta = graph_ask(
            "/graphql",
            "{ account(id: \"a\", block: {number: 15}) { balance } _meta(block: {number: 15}) { block { number hash } } }",
            state.clone(),
        )
        .await;
        assert_eq!(
            numeric_meta,
            json!({"data":{"account":{"balance":"9"},"_meta":{"block":{"number":15,"hash":null}}}})
        );
        state.store.set_block_timestamp(20, now - 60).unwrap();
        let stale = graph_ask(
            "/graphql",
            "{ accounts { id } _meta { block { number } } }",
            state,
        )
        .await;
        assert!(stale["errors"][0]["message"]
            .as_str()
            .unwrap()
            .contains("stale"));
        assert!(stale.get("data").is_none());
    }

    #[cfg(feature = "graph")]
    #[test]
    fn graph_history_freshness_uses_its_own_cursor_and_refuses_quarantine() {
        let dir = tempfile::tempdir().unwrap();
        let mut state = test_state(dir.path(), SQL_MAX_CONCURRENCY);
        let health = Arc::new(crate::health::RuntimeHealth::new());
        let name = format!("network-history-{}", dir.path().display());
        health.register(&name, "arbitrum-one");
        state.runtime_health = Some((name.clone(), health.clone()));
        let now = crate::metrics::now_unix();
        state.store.set_meta("last_block", "100").unwrap();
        state
            .store
            .set_block_hash(100, &format!("0x{}", "11".repeat(32)))
            .unwrap();
        state.store.set_block_timestamp(100, now).unwrap();
        let own = crate::metrics::METRICS.nest(&name);
        own.mark_poll_ok();
        own.set_tip(160);
        crate::metrics::METRICS.set_tip(9999999);
        let policy = crate::graph_history::Policy {
            version: 1,
            first_block: 0,
            max_head_age_seconds: 60,
            max_block_distance: Some(60),
        };
        assert!(graph_history_head(&state, &policy, now).is_ok());
        own.set_tip(161);
        assert!(graph_history_head(&state, &policy, now)
            .unwrap_err()
            .to_string()
            .contains("61 blocks"));
        own.set_tip(100);
        health.quarantine_cursor("arbitrum-one", "reorg beyond finality".into());
        assert!(graph_history_head(&state, &policy, now)
            .unwrap_err()
            .to_string()
            .contains("quarantined"));
        crate::metrics::METRICS.remove_nest(&name);
    }

    #[cfg(feature = "graph")]
    #[tokio::test]
    async fn graph_status_reports_the_stale_checkpoint_instead_of_a_healthy_chain_tip() {
        use tower::ServiceExt;
        let dir = tempfile::tempdir().unwrap();
        let mut state = test_state(dir.path(), SQL_MAX_CONCURRENCY);
        let health = Arc::new(crate::health::RuntimeHealth::new());
        let name = format!("graph-status-{}", dir.path().display());
        health.register(&name, "arbitrum-one");
        state.runtime_health = Some((name.clone(), health));
        let app = router(SharedNest::new(state.clone()));
        let request = || {
            axum::http::Request::builder()
                .uri("/graph/status")
                .body(axum::body::Body::empty())
                .unwrap()
        };
        assert_eq!(
            app.clone().oneshot(request()).await.unwrap().status(),
            StatusCode::SERVICE_UNAVAILABLE
        );
        std::fs::create_dir_all(dir.path().join("graph")).unwrap();
        std::fs::write(
            dir.path().join("graph/history.toml"),
            "version=1\nfirst_block=1\nmax_head_age_seconds=60\nmax_block_distance=60\n",
        )
        .unwrap();
        let now = crate::metrics::now_unix();
        state.store.set_meta("last_block", "100").unwrap();
        state
            .store
            .set_block_hash(100, &format!("0x{}", "11".repeat(32)))
            .unwrap();
        state.store.set_block_timestamp(100, now).unwrap();
        let metrics = crate::metrics::METRICS.nest(&name);
        metrics.mark_poll_ok();
        metrics.set_tip(110);
        assert_eq!(
            app.clone().oneshot(request()).await.unwrap().status(),
            StatusCode::OK
        );
        metrics.set_tip(200);
        let response = app.oneshot(request()).await.unwrap();
        assert_eq!(response.status(), StatusCode::SERVICE_UNAVAILABLE);
        let bytes = axum::body::to_bytes(response.into_body(), 16384)
            .await
            .unwrap();
        let body: serde_json::Value = serde_json::from_slice(&bytes).unwrap();
        assert_eq!(body["fresh"], false);
        assert_eq!(body["indexed"]["number"], 100);
        assert_eq!(body["observedHead"], 200);
        assert_eq!(body["blockDistance"], 100);
        crate::metrics::METRICS.remove_nest(&name);
    }

    /// Clock-derived fields, which legitimately differ between two calls a moment apart.
    ///
    /// `/ready` reports `seconds_since_poll` and `last_poll_unixtime`; comparing those byte-for-byte
    /// across two sequential requests asserts that no second boundary was crossed between them, which
    /// is a statement about scheduling rather than about routing. It duly failed in CI on
    /// `"seconds_since_poll":5` vs `6`.
    const VOLATILE: &[&str] = &["seconds_since_poll", "last_poll_unixtime"];

    /// `/ready` computes an age from its own clock read, so a second can tick between the test's `now`
    /// and the handler's: assert the age set, allowing that tick.
    fn assert_age(json: &serde_json::Value, key: &str, set: u64) {
        let got = json[key]
            .as_u64()
            .unwrap_or_else(|| panic!("{key} is not a number: {json}"));
        assert!(
            (set..=set + 2).contains(&got),
            "{key}: set {set}s ago, reported {got}: {json}"
        );
    }

    /// Drive one GET through a router and return `(status, body)` - the whole observable response, so
    /// a parity assertion cannot pass on status alone.
    ///
    /// JSON bodies are normalised by dropping [`VOLATILE`] keys, so the comparison stays byte-exact on
    /// everything the router actually decides and blind only to the wall clock.
    async fn get(router: Router, path: &str) -> (StatusCode, Vec<u8>) {
        use tower::ServiceExt;
        let req = axum::http::Request::builder()
            .uri(path)
            .body(axum::body::Body::empty())
            .unwrap();
        let resp = router.oneshot(req).await.unwrap();
        let status = resp.status();
        let body = axum::body::to_bytes(resp.into_body(), usize::MAX)
            .await
            .unwrap()
            .to_vec();
        // Normalise only if it parses as a JSON object; anything else is compared verbatim.
        let body = match serde_json::from_slice::<serde_json::Value>(&body) {
            Ok(mut v) if v.is_object() => {
                let map = v.as_object_mut().unwrap();
                for k in VOLATILE {
                    map.remove(*k);
                }
                serde_json::to_vec(&v).unwrap()
            }
            _ => body,
        };
        (status, body)
    }

    /// RFC-0021's fourth testing criterion, and the one nothing covered (#356): **stall isolation
    /// across cursors**. The RFC's words are "killing chain A's RPC escalates for A only; B keeps
    /// serving and ingesting", and the serving half is where an actual mechanism can be broken.
    ///
    /// The trap this guards is specific and was live once. `/ready`'s stall verdict comes from
    /// `last_poll_ok`, and the process-global aggregate is written by *whichever cursor polled last*.
    /// A runtime hosting a dead chain alongside a healthy one therefore has a permanently fresh global
    /// - so reading it would report the dead chain **ready**, and an operator watching
    /// `/<dead-chain>/ready` would see 200 while nothing was being indexed. The handler reads the
    /// per-nest counter instead, and this pins that.
    ///
    /// Note the shape of the assertions: "chain B still answers 200" is the half that passes trivially
    /// when the mechanism is missing (a fresh global says ready to everyone). The load-bearing half is
    /// that chain **A** answers 503 *at the same time*, which only per-nest attribution can produce.
    ///
    /// Deliberately built from **un-stamped** [`test_state`] - `runtime_health` is left `None`, same as
    /// every other test in this module - and composed via [`compose_runtime`] rather than stamped by
    /// hand. That is the fix for #388: three fixtures in a row (#292, #356, this one) forgot to
    /// replicate `spawn_runtime`'s stamp and silently proved the solo path instead, so the stamp now
    /// lives once in `compose_runtime` itself and this test exists to pin that it actually fires - a
    /// version of this test that reverted `compose_runtime`'s `get_or_insert_with` back to a no-op would
    /// turn red here, both nests reporting via the process globals.
    #[tokio::test]
    async fn one_chains_dead_endpoints_leave_its_co_tenant_serving_and_report_only_itself_stalled()
    {
        let dir = tempfile::tempdir().unwrap();
        // Names unique to this test: `METRICS.nest(..)` is a process-global map shared by every test
        // in the binary, so reusing `alpha`/`beta` would have them writing each other's counters.
        let (dead, live) = ("stall-iso-dead", "stall-iso-live");
        std::fs::create_dir_all(dir.path().join(dead)).unwrap();
        std::fs::create_dir_all(dir.path().join(live)).unwrap();
        let roster = json!({"runtime": "t", "nests": [{"name": dead}, {"name": live}]});
        let health = Arc::new(crate::health::RuntimeHealth::new());
        let nests: Vec<(String, AppState)> = [dead, live]
            .into_iter()
            .map(|name| {
                let st = test_state(&dir.path().join(name), 4);
                (name.to_string(), st)
            })
            .collect();
        let router = compose_runtime(roster, nests, health);

        // Chain A's endpoints went dark ten minutes ago. Chain B polled a second ago - and, being
        // healthy, is still writing the global aggregate. That is the trap, set deliberately.
        let now = crate::metrics::now_unix();
        crate::metrics::METRICS
            .nest(dead)
            .set_last_poll_ok_for_test(now.saturating_sub(600));
        crate::metrics::METRICS
            .nest(live)
            .set_last_poll_ok_for_test(now.saturating_sub(1));
        crate::metrics::METRICS.mark_poll_ok();

        let (code_dead, body_dead) = get(router.clone(), &format!("/{dead}/ready")).await;
        let (code_live, body_live) = get(router, &format!("/{live}/ready")).await;
        let json_of = |b: &[u8]| serde_json::from_slice::<serde_json::Value>(b).unwrap();
        let (dead_json, live_json) = (json_of(&body_dead), json_of(&body_live));

        // The load-bearing half: the dead chain is reported dead, despite a fresh global.
        assert_eq!(
            code_dead,
            StatusCode::SERVICE_UNAVAILABLE,
            "the stalled cursor must fail readiness: {dead_json}"
        );
        assert_eq!(dead_json["stalled"], json!(true));
        assert_eq!(dead_json["ready"], json!(false));

        // …and the isolation half: its co-tenant is untouched by the neighbour's dead provider.
        assert_eq!(
            code_live,
            StatusCode::OK,
            "the healthy cursor must keep serving: {live_json}"
        );
        assert_eq!(live_json["stalled"], json!(false));
        assert_eq!(live_json["ready"], json!(true));
    }

    /// #1204: the runtime **root** `/ready` must see a per-nest stall, not only a quarantine.
    ///
    /// Until this landed, `roost_ready` read the quarantine set and nothing else, so a runtime
    /// answered `{"quarantined":[],"ready":true}` while a nest inside it had stopped. That is what
    /// happened to the `horizon` nest behind `nuthatch-ds-upstream`: 753,000 blocks behind on its
    /// seal, ready at the root throughout, on the surface that issues TAP receipts. The two solo
    /// nests with the identical defect were caught within a day because for a solo nest the root
    /// `/ready` *is* the per-nest one; the runtime root was the only place it could hide.
    ///
    /// **Nothing is quarantined in this test, deliberately.** A quarantine would take the root to 503
    /// through the pre-existing path and prove nothing about the new one, so the `quarantined` array
    /// is asserted empty: the 503 can only have come from the stall verdict.
    ///
    /// The blast-radius half is asserted too, because it is the thing the conservative design was
    /// worried about: the healthy co-tenant's own `/ready` still answers 200 and keeps serving.
    /// Readiness is advice to a supervisor, not a gate on traffic.
    ///
    /// Composed through [`compose_runtime`] from un-stamped [`test_state`], for the #388 reason: a
    /// hand-stamped fixture proves the handler and never the wiring, which is the shape that let this
    /// defect live.
    #[tokio::test]
    async fn the_runtime_root_reports_a_stalled_nest_that_is_not_quarantined() {
        let dir = tempfile::tempdir().unwrap();
        // Names unique to this test: `METRICS.nest(..)` is a process-global map shared by every test
        // in the binary.
        let (dead, live) = ("root-agg-dead", "root-agg-live");
        std::fs::create_dir_all(dir.path().join(dead)).unwrap();
        std::fs::create_dir_all(dir.path().join(live)).unwrap();
        let roster = json!({"runtime": "t", "nests": [{"name": dead}, {"name": live}]});
        let health = Arc::new(crate::health::RuntimeHealth::new());
        let nests: Vec<(String, AppState)> = [dead, live]
            .into_iter()
            .map(|name| (name.to_string(), test_state(&dir.path().join(name), 4)))
            .collect();
        let router = compose_runtime(roster, nests, health);

        let now = crate::metrics::now_unix();
        crate::metrics::METRICS
            .nest(dead)
            .set_last_poll_ok_for_test(now.saturating_sub(600));
        crate::metrics::METRICS
            .nest(live)
            .set_last_poll_ok_for_test(now.saturating_sub(1));
        // The healthy nest also writes the process-global aggregate, as a real one would. A root that
        // consulted the globals rather than each nest would read this and answer 200.
        crate::metrics::METRICS.mark_poll_ok();

        let (root_code, root_body) = get(router.clone(), "/ready").await;
        let (live_code, live_body) = get(router, &format!("/{live}/ready")).await;
        let json_of = |b: &[u8]| serde_json::from_slice::<serde_json::Value>(b).unwrap();
        let (root, live_json) = (json_of(&root_body), json_of(&live_body));

        assert_eq!(
            root_code,
            StatusCode::SERVICE_UNAVAILABLE,
            "a stalled nest must take the runtime root unready: {root}"
        );
        assert_eq!(root["ready"], json!(false));
        assert_eq!(
            root["quarantined"].as_array().unwrap().len(),
            0,
            "nothing is quarantined here - the 503 must come from the stall verdict: {root}"
        );
        let stalled = root["stalled"].as_array().unwrap();
        assert_eq!(stalled.len(), 1, "only the dead nest is stalled: {root}");
        assert_eq!(stalled[0]["nest"], json!(dead));
        assert!(
            !stalled[0]["reasons"].as_array().unwrap().is_empty(),
            "the offender is named with a reason, not merely counted: {root}"
        );

        assert_eq!(
            live_code,
            StatusCode::OK,
            "a co-tenant must not be evicted by its neighbour's stall: {live_json}"
        );
        assert_eq!(live_json["ready"], json!(true));
    }

    /// #1204, the alias case, raised in review: a quarantined dataset reached through **two** mounts
    /// must be reported once, as a quarantine, under both mount names - never as a quarantine under
    /// the canonical name and a *stall* under the alias, which would be one fault wearing two
    /// vocabularies.
    ///
    /// The mechanism that makes this hold is worth pinning because it is not obvious from
    /// `roost_ready`: `register_alias` calls `register`, so an alias is a key in `chain_of`, and
    /// `unhealthy()` resolves every one of those keys through `status()` - which follows `shares` to
    /// the canonical mount. Both names are therefore already in the quarantine set the stall pass
    /// filters against, and the alias is skipped rather than judged on the canonical nest's counters.
    ///
    /// The alias's state is pre-stamped with the **canonical** nest's name, which is what
    /// `fan_out_aliases` produces when it clones the canonical state (`runtime.rs`, RFC-0032 §4). The
    /// per-nest assertion at the end is what proves that stamp is behaving as the real path does.
    #[tokio::test]
    async fn a_quarantined_dataset_is_named_once_per_mount_and_never_as_a_stall() {
        use crate::health::RuntimeHealth;
        let dir = tempfile::tempdir().unwrap();
        let (canonical, alias) = ("alias-q-canonical", "alias-q-alias");
        std::fs::create_dir_all(dir.path().join(canonical)).unwrap();
        let health = Arc::new(RuntimeHealth::new());
        health.register(canonical, "arbitrum-one");
        health.register_alias(alias, canonical, "arbitrum-one");

        let mut state = test_state(&dir.path().join(canonical), 4);
        state.runtime_health = Some((canonical.to_string(), health.clone()));
        let nests = vec![
            (canonical.to_string(), state.clone()),
            // The alias carries the canonical nest's counters, exactly as `fan_out_aliases` leaves it.
            (alias.to_string(), state),
        ];
        let roster = json!({"runtime": "t", "nests": [{"name": canonical}, {"name": alias}]});
        let router = compose_runtime(roster, nests, health.clone());

        // The dataset is quarantined *and* its counters are stale, so a root that judged the alias on
        // its stall terms would have something to report and would report it.
        crate::metrics::METRICS
            .nest(canonical)
            .set_last_poll_ok_for_test(crate::metrics::now_unix().saturating_sub(600));
        health.quarantine_nest(
            canonical,
            "the endpoint pool is terminally dead".into(),
            1,
            None,
        );

        let (root_code, root_body) = get(router.clone(), "/ready").await;
        let root: Value = serde_json::from_slice(&root_body).unwrap();
        assert_eq!(root_code, StatusCode::SERVICE_UNAVAILABLE);
        let names: Vec<&str> = root["quarantined"]
            .as_array()
            .unwrap()
            .iter()
            .map(|q| q["nest"].as_str().unwrap())
            .collect();
        assert!(
            names.contains(&canonical) && names.contains(&alias),
            "both mounts of a quarantined dataset are named: {root}"
        );
        assert_eq!(
            root["stalled"].as_array().unwrap().len(),
            0,
            "a quarantined dataset must not also be reported stalled, under either name: {root}"
        );

        // …and the alias's own endpoint agrees, which is what proves the canonical stamp is live.
        let (alias_code, alias_body) = get(router, &format!("/{alias}/ready")).await;
        let alias_json: Value = serde_json::from_slice(&alias_body).unwrap();
        assert_eq!(alias_code, StatusCode::SERVICE_UNAVAILABLE);
        assert_eq!(alias_json["quarantined"], json!(true));
    }

    /// #510: a nest whose RPC pool has been dead since before its very first successful poll must
    /// eventually answer `/ready` with `stalled`, not a permanent healthy-looking
    /// `{"stalled":false,"last_poll_unixtime":0}` - the exact body a fully dead pool served forever
    /// before this fix (the process used to crash before this endpoint even got a chance to answer).
    #[tokio::test]
    async fn a_never_polled_nest_stalls_once_its_start_grace_expires() {
        let dir = tempfile::tempdir().unwrap();
        let name = "cold-start-dead";
        std::fs::create_dir_all(dir.path().join(name)).unwrap();
        let roster = json!({"runtime": "t", "nests": [{"name": name}]});
        let health = Arc::new(crate::health::RuntimeHealth::new());
        let nests = vec![(name.to_string(), test_state(&dir.path().join(name), 4))];
        let router = compose_runtime(roster, nests, health);

        let now = crate::metrics::now_unix();
        let handle = crate::metrics::METRICS.nest(name);
        // Never successfully polled, and started well past the 90s grace window.
        handle.set_started_at_for_test(now.saturating_sub(200));
        assert_eq!(handle.last_poll_ok(), 0, "premise: this nest never polled");

        let (code, body) = get(router, &format!("/{name}/ready")).await;
        let json = serde_json::from_slice::<serde_json::Value>(&body).unwrap();
        assert_eq!(
            code,
            StatusCode::SERVICE_UNAVAILABLE,
            "a pool dead since before the first poll must fail readiness: {json}"
        );
        assert_eq!(json["stalled"], json!(true));
        assert_eq!(json["ready"], json!(false));
    }

    /// #799: a nest whose first poll failed must immediately answer `/ready` with 503 and
    /// `ready: false`, without waiting out the 90s startup grace window.
    #[tokio::test]
    async fn a_nest_with_failed_first_poll_is_unready_immediately() {
        let dir = tempfile::tempdir().unwrap();
        let name = "unreachable-first-poll";
        std::fs::create_dir_all(dir.path().join(name)).unwrap();
        let roster = json!({"runtime": "t", "nests": [{"name": name}]});
        let health = Arc::new(crate::health::RuntimeHealth::new());
        let nests = vec![(name.to_string(), test_state(&dir.path().join(name), 4))];
        let router = compose_runtime(roster, nests, health);

        let now = crate::metrics::now_unix();
        let handle = crate::metrics::METRICS.nest(name);
        handle.set_started_at_for_test(now.saturating_sub(5));
        handle.set_poll_failed_for_test(true);
        assert_eq!(handle.last_poll_ok(), 0, "premise: this nest never polled");
        assert!(handle.poll_failed(), "premise: poll marked failed");

        let (code, body) = get(router, &format!("/{name}/ready")).await;
        let json = serde_json::from_slice::<serde_json::Value>(&body).unwrap();
        assert_eq!(
            code,
            StatusCode::SERVICE_UNAVAILABLE,
            "a nest with failed first poll must immediately fail readiness: {json}"
        );
        assert_eq!(json["stalled"], json!(true));
        assert_eq!(json["ready"], json!(false));
        assert_eq!(json["initial_poll_failed"], json!(true));
    }

    /// #578, reproduced from the GH issue verbatim: a cursor whose source keeps answering (so
    /// `poll_stalled` alone sees nothing wrong) but whose `last_block` never leaves zero, holding
    /// position on an unfetchable block timestamp rather than seal it (correct - `indexer.rs:3437`).
    /// `poll_stalled` alone reported this `ready:true` with `lag_blocks` in the millions in the same
    /// body; this is the mutation `progress_stalled` exists to catch: assert the status **code**, not
    /// just `lag_blocks` in the body, from a fixture whose cursor is pinned while polls succeed.
    #[tokio::test]
    async fn a_cursor_pinned_at_zero_while_polling_succeeds_is_not_ready() {
        let dir = tempfile::tempdir().unwrap();
        let name = "wedged-at-zero";
        std::fs::create_dir_all(dir.path().join(name)).unwrap();
        let roster = json!({"runtime": "t", "nests": [{"name": name}]});
        let health = Arc::new(crate::health::RuntimeHealth::new());
        let nests = vec![(name.to_string(), test_state(&dir.path().join(name), 4))];
        let router = compose_runtime(roster, nests, health);

        let now = crate::metrics::now_unix();
        let handle = crate::metrics::METRICS.nest(name);
        // The source is healthy and answering right now - the exact trap: `poll_stalled` alone sees
        // "fine". `last_block` has never moved, and the nest was built well past the progress grace.
        handle.set_tip(25_754_377);
        handle.mark_poll_ok();
        handle.set_started_at_for_test(now.saturating_sub(200));
        assert_eq!(handle.last_block(), 0, "premise: never indexed anything");
        assert_eq!(handle.last_progress(), 0, "premise: never advanced");

        let (code, body) = get(router, &format!("/{name}/ready")).await;
        let json = serde_json::from_slice::<serde_json::Value>(&body).unwrap();
        assert_eq!(
            code,
            StatusCode::SERVICE_UNAVAILABLE,
            "a cursor stuck at zero while polling succeeds must fail readiness: {json}"
        );
        assert_eq!(json["ready"], json!(false));
        assert_eq!(json["stalled"], json!(true));
        assert_eq!(json["wedged"], json!(true));
    }

    /// The same trap, but the cursor had made real progress once before wedging - proving the check
    /// catches a stall mid-backfill, not just one that never got off the ground.
    #[tokio::test]
    async fn a_cursor_that_advanced_once_then_froze_is_not_ready() {
        let dir = tempfile::tempdir().unwrap();
        let name = "wedged-mid-backfill";
        std::fs::create_dir_all(dir.path().join(name)).unwrap();
        let roster = json!({"runtime": "t", "nests": [{"name": name}]});
        let health = Arc::new(crate::health::RuntimeHealth::new());
        let nests = vec![(name.to_string(), test_state(&dir.path().join(name), 4))];
        let router = compose_runtime(roster, nests, health);

        let now = crate::metrics::now_unix();
        let handle = crate::metrics::METRICS.nest(name);
        handle.set_last_block(12_000_000);
        handle.set_tip(25_754_377);
        handle.mark_poll_ok();
        // Progress happened, but a long time ago - the source has kept polling fine since.
        handle.set_last_progress_for_test(now.saturating_sub(200));

        let (code, body) = get(router, &format!("/{name}/ready")).await;
        let json = serde_json::from_slice::<serde_json::Value>(&body).unwrap();
        assert_eq!(
            code,
            StatusCode::SERVICE_UNAVAILABLE,
            "a cursor frozen mid-backfill must fail readiness: {json}"
        );
        assert_eq!(json["wedged"], json!(true));
    }

    /// #1575: under `--finality-only` a cursor waits at the finality boundary, below the tip, for the
    /// next finalized block, which on Arbitrum takes minutes. Level with its ceiling it is caught up;
    /// behind it and not moving, it is still wedged.
    #[tokio::test]
    async fn a_finality_cursor_at_its_ceiling_is_ready_and_one_behind_it_is_wedged() {
        let dir = tempfile::tempdir().unwrap();
        let name = "finality-cursor";
        std::fs::create_dir_all(dir.path().join(name)).unwrap();
        let roster = json!({"runtime": "t", "nests": [{"name": name}]});
        let health = Arc::new(crate::health::RuntimeHealth::new());
        let nests = vec![(name.to_string(), test_state(&dir.path().join(name), 4))];
        let router = compose_runtime(roster, nests, health);

        let now = crate::metrics::now_unix();
        let handle = crate::metrics::METRICS.nest(name);
        handle.set_last_block(1_000);
        handle.set_tip(1_100);
        handle.set_ceiling(1_000);
        handle.mark_poll_ok();
        handle.set_last_progress_for_test(now.saturating_sub(200));
        let (code, body) = get(router.clone(), &format!("/{name}/ready")).await;
        let json = serde_json::from_slice::<serde_json::Value>(&body).unwrap();
        assert_eq!(
            code,
            StatusCode::OK,
            "waiting at finality is caught up: {json}"
        );
        assert_eq!(json["wedged"], json!(false));

        handle.set_ceiling(1_050);
        let (code, body) = get(router, &format!("/{name}/ready")).await;
        let json = serde_json::from_slice::<serde_json::Value>(&body).unwrap();
        assert_eq!(code, StatusCode::SERVICE_UNAVAILABLE, "{json}");
        assert_eq!(json["wedged"], json!(true));
    }

    /// #2130: every root route answers, and a mount of its name panics `Router::nest`, which is why
    /// the runtime refuses exactly these names.
    #[tokio::test]
    async fn every_root_route_answers_and_collides_with_a_mount_of_its_name() {
        let dir = tempfile::tempdir().unwrap();
        let health = Arc::new(crate::health::RuntimeHealth::new());
        let roster = json!({"runtime": "t", "nests": []});
        for segment in RUNTIME_ROOT_ROUTES {
            let root = compose_runtime(roster.clone(), Vec::new(), health.clone());
            let (code, _) = get(root, &format!("/{segment}")).await;
            assert_ne!(code, StatusCode::NOT_FOUND, "/{segment}");
            std::fs::create_dir_all(dir.path().join(segment)).unwrap();
            let state = test_state(&dir.path().join(segment), 1);
            let composed = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                compose_runtime(
                    roster.clone(),
                    vec![(segment.to_string(), state)],
                    health.clone(),
                )
            }));
            assert!(composed.is_err(), "a mount named '{segment}' composed");
        }
    }

    /// #1575, found on Base: a new finality-only nest whose start block is above the finality boundary
    /// has committed nothing, so its position is just below its start, not block 0. Waiting for
    /// finality to reach it is not being the whole chain behind.
    #[tokio::test]
    async fn a_new_finality_cursor_waiting_below_its_start_is_ready() {
        let dir = tempfile::tempdir().unwrap();
        let name = "finality-cursor-fresh";
        std::fs::create_dir_all(dir.path().join(name)).unwrap();
        let state = test_state(&dir.path().join(name), 4);
        state
            .store
            .set_meta(crate::indexer::START_BLOCK_KEY, "1200")
            .unwrap();
        let roster = json!({"runtime": "t", "nests": [{"name": name}]});
        let health = Arc::new(crate::health::RuntimeHealth::new());
        let router = compose_runtime(roster, vec![(name.to_string(), state)], health);

        let now = crate::metrics::now_unix();
        let handle = crate::metrics::METRICS.nest(name);
        handle.set_tip(1_300);
        handle.set_ceiling(1_000);
        handle.mark_poll_ok();
        handle.set_last_progress_for_test(now.saturating_sub(200));
        let (code, body) = get(router, &format!("/{name}/ready")).await;
        let json = serde_json::from_slice::<serde_json::Value>(&body).unwrap();
        assert_eq!(
            code,
            StatusCode::OK,
            "waiting for finality to reach its start: {json}"
        );
        assert_eq!(json["wedged"], json!(false));
        assert_eq!(
            json["lag_blocks"],
            json!(101),
            "lag is from its position, not block 0"
        );
    }

    /// #583/#589: a cursor level with tip - caught up, doing exactly what it should - must stay ready
    /// even though `last_progress` is stale well past the threshold. `set_last_block` only stamps
    /// `last_progress` on an actual advance, so a caught-up cursor stops stamping the moment it
    /// arrives; without the `lag > 0` guard on `progress_stalled` this reports `wedged: true` and
    /// 503 on a perfectly healthy nest. Same shape covers a completed fixed-range nest
    /// (`end_block: Option<u64>`, `src/subgraph_import.rs:68`), whose `last_block` is final by design.
    ///
    /// Mutation check: remove the `lag > 0` guard (`progress_stalled` returning early on `lag == 0`)
    /// and this test reds - the wedged path alone can't tell a caught-up cursor from a stuck one.
    #[tokio::test]
    async fn a_caught_up_cursor_is_ready_despite_stale_progress() {
        let dir = tempfile::tempdir().unwrap();
        let name = "caught-up-cursor";
        std::fs::create_dir_all(dir.path().join(name)).unwrap();
        let roster = json!({"runtime": "t", "nests": [{"name": name}]});
        let health = Arc::new(crate::health::RuntimeHealth::new());
        let nests = vec![(name.to_string(), test_state(&dir.path().join(name), 4))];
        let router = compose_runtime(roster, nests, health);

        let now = crate::metrics::now_unix();
        let handle = crate::metrics::METRICS.nest(name);
        handle.set_tip(25_754_377);
        handle.set_last_block(25_754_377); // level with tip: lag == 0
        handle.mark_poll_ok();
        handle.set_started_at_for_test(now.saturating_sub(1_000));
        // Stale well past READINESS_PROGRESS_STALL_SECS - it stopped stamping the moment it caught up.
        handle.set_last_progress_for_test(now.saturating_sub(200));

        let (code, body) = get(router, &format!("/{name}/ready")).await;
        let json = serde_json::from_slice::<serde_json::Value>(&body).unwrap();
        assert_eq!(
            code,
            StatusCode::OK,
            "a cursor caught up to tip must stay ready despite stale last_progress: {json}"
        );
        assert_eq!(json["ready"], json!(true));
        assert_eq!(json["stalled"], json!(false));
        assert_eq!(json["wedged"], json!(false));
        assert_eq!(json["lag_blocks"], json!(0));
        // An undialled test state: the interval is the chain default, and /ready says so (#1497).
        assert_eq!(json["freshness"]["poll_interval_secs"], json!(2));
        assert_eq!(
            json["freshness"]["poll_interval_source"],
            json!("block_time")
        );
    }

    /// #807: a seal-direct pass that has actually sealed rows must not look like WAITING.
    #[tokio::test]
    async fn seal_direct_ready_reports_progress_not_waiting() {
        use crate::metrics::METRICS;
        let dir = tempfile::tempdir().unwrap();
        let name = "sealing";
        std::fs::create_dir_all(dir.path().join(name)).unwrap();
        let roster = json!({"runtime": "t", "nests": [{"name": name}]});
        let health = Arc::new(crate::health::RuntimeHealth::new());
        let nests = vec![(name.to_string(), test_state(&dir.path().join(name), 4))];
        let router = compose_runtime(roster, nests, health);

        let now = crate::metrics::now_unix();
        let handle = METRICS.nest(name);
        handle.set_started_at_for_test(now.saturating_sub(200));
        handle.begin_seal_direct(1_000, 2_000);
        handle.set_seal_direct_completed(1_500);

        let (code, body) = get(router, &format!("/{name}/ready")).await;
        let json = serde_json::from_slice::<serde_json::Value>(&body).unwrap();
        assert_eq!(
            code,
            StatusCode::OK,
            "seal-direct progress is ready: {json}"
        );
        assert_eq!(json["ready"], json!(true));
        assert_eq!(json["stalled"], json!(false));
        assert_eq!(json["seal_direct_active"], json!(true));
        assert_eq!(json["seal_direct_origin"], json!(1000));
        assert_eq!(json["seal_direct_completed"], json!(1500));
        assert_eq!(json["seal_direct_target"], json!(2000));
        handle.end_seal_direct();
    }

    /// #846: a seal-direct pass that has stopped sealing must stop reporting ready.
    ///
    /// Before this, `seal_direct_active` gated every stall term and nothing replaced them, so the
    /// measured answer after ten hours frozen was `HTTP 200 {"ready":true,"stalled":false}`. An
    /// orchestrator believes that endpoint.
    #[tokio::test]
    async fn a_seal_direct_that_stopped_sealing_reports_unready() {
        use crate::metrics::METRICS;
        let dir = tempfile::tempdir().unwrap();
        let name = "frozen-seal";
        std::fs::create_dir_all(dir.path().join(name)).unwrap();
        let roster = json!({"runtime": "t", "nests": [{"name": name}]});
        let health = Arc::new(crate::health::RuntimeHealth::new());
        let nests = vec![(name.to_string(), test_state(&dir.path().join(name), 4))];
        let router = compose_runtime(roster, nests, health);

        let now = crate::metrics::now_unix();
        let handle = METRICS.nest(name);
        handle.set_started_at_for_test(now.saturating_sub(36_000));
        handle.begin_seal_direct(1_000, 2_000);
        handle.set_seal_direct_completed(1_500);
        // Sealed once, then froze ten hours ago - well past READINESS_SEAL_STALL_SECS.
        handle.set_last_seal_progress_for_test(now.saturating_sub(36_000));

        let (code, body) = get(router, &format!("/{name}/ready")).await;
        let json = serde_json::from_slice::<serde_json::Value>(&body).unwrap();
        assert_eq!(
            code,
            StatusCode::SERVICE_UNAVAILABLE,
            "a seal-direct frozen for 10h must not read ready: {json}"
        );
        assert_eq!(json["ready"], json!(false));
        assert_eq!(json["stalled"], json!(true));
        assert_eq!(json["seal_direct_stalled"], json!(true));
        // The operator can tell slow from dead without a second tool.
        assert_age(&json, "seconds_since_seal_progress", 36_000);
        handle.end_seal_direct();
    }

    /// The control, and the half that must not regress: a pass that is still sealing stays ready
    /// however long it has been running. #807's whole point was that a working history pass is not a
    /// stalled cursor, and #846 must not undo it by turning duration into a failure.
    #[tokio::test]
    async fn a_seal_direct_still_sealing_stays_ready_however_long_it_has_run() {
        use crate::metrics::METRICS;
        let dir = tempfile::tempdir().unwrap();
        let name = "slow-seal";
        std::fs::create_dir_all(dir.path().join(name)).unwrap();
        let roster = json!({"runtime": "t", "nests": [{"name": name}]});
        let health = Arc::new(crate::health::RuntimeHealth::new());
        let nests = vec![(name.to_string(), test_state(&dir.path().join(name), 4))];
        let router = compose_runtime(roster, nests, health);

        let now = crate::metrics::now_unix();
        let handle = METRICS.nest(name);
        // Six hours in, and no successful *poll* for six hours either - the tip thresholds would
        // both fire, and are correctly suppressed while a history pass is the thing running.
        handle.set_started_at_for_test(now.saturating_sub(21_600));
        handle.set_last_poll_ok_for_test(now.saturating_sub(21_600));
        handle.set_last_progress_for_test(now.saturating_sub(21_600));
        handle.begin_seal_direct(1_000, 2_000);
        handle.set_seal_direct_completed(1_500);
        // ...but it sealed a window a minute ago.
        handle.set_last_seal_progress_for_test(now.saturating_sub(60));

        let (code, body) = get(router, &format!("/{name}/ready")).await;
        let json = serde_json::from_slice::<serde_json::Value>(&body).unwrap();
        assert_eq!(
            code,
            StatusCode::OK,
            "a pass that is still sealing is ready: {json}"
        );
        assert_eq!(json["ready"], json!(true));
        assert_eq!(json["seal_direct_stalled"], json!(false));
        assert_age(&json, "seconds_since_seal_progress", 60);
        handle.end_seal_direct();
    }

    /// #1199: **a stalled seal and a healthy one read identically through `/ready`.** Reproduces the
    /// live shape measured on `graph-allocations-nest` at 2026-09-07 16:20 UTC: the cursor at the
    /// chain's tip, `lag_blocks` 0, and the sealed watermark 739,192 blocks (~51 h) behind it. Every
    /// seal field on this endpoint belongs to the seal-*direct* backfill, which was never running, so
    /// `seal_direct_stalled` was false because the pass had not started rather than because it was
    /// keeping up - and Lodestar's `check-nest-health` cron passed on that.
    ///
    /// The assertion here is the number, not the verdict. `ready` stays true on purpose: see the
    /// `seal_lag_blocks` comment in `ready` for why the verdict cannot land before `maybe_seal`'s
    /// holding rule is fixed.
    #[tokio::test]
    async fn ready_reports_how_far_the_seal_trails_the_cursor() {
        use crate::metrics::METRICS;
        let dir = tempfile::tempdir().unwrap();
        let name = "trailing-seal";
        std::fs::create_dir_all(dir.path().join(name)).unwrap();
        let roster = json!({"runtime": "t", "nests": [{"name": name}]});
        let health = Arc::new(crate::health::RuntimeHealth::new());
        let nests = vec![(name.to_string(), test_state(&dir.path().join(name), 4))];
        let router = compose_runtime(roster, nests, health);

        let handle = METRICS.nest(name);
        handle.set_tip(502_732_913);
        handle.set_last_block(502_732_913);
        handle.set_sealed_through(501_993_721);
        handle.mark_poll_ok();
        handle.set_last_progress_for_test(crate::metrics::now_unix());

        let (code, body) = get(router, &format!("/{name}/ready")).await;
        let json = serde_json::from_slice::<serde_json::Value>(&body).unwrap();
        assert_eq!(code, StatusCode::OK, "{json}");
        assert_eq!(
            json["lag_blocks"],
            json!(0),
            "the cursor is at tip - this is the field that read healthy throughout"
        );
        assert_eq!(
            json["seal_lag_blocks"],
            json!(739_192),
            "a caller must be able to tell a stalled seal from a healthy one without comparing \
             three ports by hand (#1199): {json}"
        );
        assert_eq!(
            json["seal_direct_stalled"],
            json!(false),
            "the seal-direct fields describe a pass that never started, and always did"
        );
    }

    /// RFC-0062: `/ready` names each maintained view's copies and counters, and carries no such key
    /// for a nest that declares none.
    #[tokio::test]
    async fn ready_reports_maintained_views_only_where_declared() {
        let dir = tempfile::tempdir().unwrap();
        for (name, declare) in [("plain-ready", false), ("maintained-ready", true)] {
            let nest = dir.path().join(name);
            std::fs::create_dir_all(nest.join("views")).unwrap();
            std::fs::write(
                nest.join("views/v.sql"),
                "CREATE VIEW bet AS SELECT 1 AS id;",
            )
            .unwrap();
            if declare {
                std::fs::write(
                    nest.join(crate::maintained::DECLARATION_FILE),
                    "[[view]]\nname = \"bet\"\n",
                )
                .unwrap();
            }
            crate::maintained::load(&nest).unwrap();
            let app = router(SharedNest::new(test_state(&nest, 2)));
            let (_, body) = get(app, "/ready").await;
            let json = serde_json::from_slice::<serde_json::Value>(&body).unwrap();
            if declare {
                assert_eq!(json["maintained"][0]["view"], json!("bet"), "{json}");
                assert_eq!(json["maintained"][0]["copies"], json!(0), "{json}");
                assert_eq!(json["maintained"][0]["last_error"], Value::Null, "{json}");
            } else {
                assert!(json.get("maintained").is_none(), "{json}");
            }
        }
    }

    /// RFC-0063: `/api` is routed, answers from the nest's address history, and says unsupported on a
    /// nest without one rather than an empty success rotki would take as no history.
    #[tokio::test]
    async fn the_etherscan_api_route_serves_address_history() {
        use crate::address_history::{Action, AddressHistory, Row};
        let dir = tempfile::tempdir().unwrap();
        let who = "0xd8dA6BF26964aF9D7eEd9e03E53415D37aA96045";
        let path =
            format!("/api?module=account&action=txlist&address={who}&startblock=0&endblock=20");

        let bare = router(SharedNest::new(test_state(dir.path(), 2)));
        let (status, body) = get(bare, &path).await;
        assert_eq!(status, StatusCode::OK);
        let json: Value = serde_json::from_slice(&body).unwrap();
        assert_eq!(json["status"], "0", "{json}");
        assert!(
            json["result"]
                .as_str()
                .unwrap()
                .starts_with("NUTHATCH_UNSUPPORTED:"),
            "{json}"
        );

        let history = AddressHistory::open(
            Store::open(&dir.path().join("ah.redb")).unwrap(),
            1,
            &[who.into()],
        )
        .unwrap();
        let mut record = serde_json::Map::new();
        record.insert("blockNumber".into(), json!("7"));
        record.insert("hash".into(), json!("0xabc"));
        let row = Row {
            block: 7,
            tx_index: 0,
            position: 0,
            record,
        };
        let generation = history.generation().unwrap();
        history
            .record(Action::TxList, who, &[row], (0, 20), generation)
            .unwrap();
        let mut state = test_state(dir.path(), 2);
        state.address_history = Some(history);
        let (_, body) = get(router(SharedNest::new(state)), &path).await;
        let json: Value = serde_json::from_slice(&body).unwrap();
        assert_eq!(
            json,
            json!({
                "status": "1",
                "message": "OK",
                "result": [{"blockNumber": "7", "hash": "0xabc"}],
                "generation": "0",
            })
        );
    }

    /// #1399: with one `--rpc`, `--seal-direct --concurrency 6` ran one window at a time, and the only
    /// sign was a startup warning, noticed after a restart.
    #[tokio::test]
    async fn ready_says_when_the_seal_direct_concurrency_was_capped() {
        use crate::metrics::METRICS;
        let dir = tempfile::tempdir().unwrap();
        let cases = [
            (
                "capped-seal-direct",
                Some((6, 1)),
                json!({"requested": 6, "effective": 1, "capped_by": "single_rpc_endpoint"}),
            ),
            (
                "parallel-seal-direct",
                Some((6, 6)),
                json!({"requested": 6, "effective": 6, "capped_by": null}),
            ),
            ("no-seal-direct", None, Value::Null),
        ];
        for (name, recorded, want) in cases {
            std::fs::create_dir_all(dir.path().join(name)).unwrap();
            let roster = json!({"runtime": "t", "nests": [{"name": name}]});
            let health = Arc::new(crate::health::RuntimeHealth::new());
            let nests = vec![(name.to_string(), test_state(&dir.path().join(name), 4))];
            let router = compose_runtime(roster, nests, health);
            if let Some((requested, effective)) = recorded {
                METRICS
                    .nest(name)
                    .set_seal_direct_concurrency(requested, effective);
            }
            let (_code, body) = get(router, &format!("/{name}/ready")).await;
            let json = serde_json::from_slice::<serde_json::Value>(&body).unwrap();
            assert_eq!(json["seal_direct_concurrency"], want, "{name}: {json}");
        }
    }

    /// #1410: what either path gave up on is counted on `/ready` and listed at `/ipfs/gave-up` in block
    /// order, including a record an earlier build wrote as the bare CID.
    #[tokio::test]
    async fn gave_up_documents_are_counted_on_ready_and_listed() {
        use crate::ipfs_resolve::{record_gave_up, GaveUp};
        let dir = tempfile::tempdir().unwrap();
        let st = test_state(dir.path(), 4);
        let record = |block: u64| GaveUp {
            cid: format!("Qm{block}"),
            block,
            slot: 0,
            declaration: Some("qos_payload".into()),
            error: Some("not fetched within the 300s a document is tried for".into()),
            at: Some(1_757_900_000),
        };
        assert!(record_gave_up(st.store.as_ref(), &record(48_000_010)).unwrap());
        assert!(record_gave_up(st.store.as_ref(), &record(48_000_004)).unwrap());
        st.store
            .set_meta("ipfs_gave_up:000048000020:1", "QmLegacy")
            .unwrap();
        let router = router(SharedNest::new(st));

        let (_code, body) = get(router.clone(), "/ready").await;
        let ready = serde_json::from_slice::<Value>(&body).unwrap();
        assert_eq!(ready["ipfs_gave_up_documents"], json!(3), "{ready}");

        let (code, body) = get(router.clone(), "/ipfs/gave-up?limit=2").await;
        assert_eq!(code, StatusCode::OK);
        let page = serde_json::from_slice::<Value>(&body).unwrap();
        assert_eq!(
            (page["total"].clone(), page["count"].clone()),
            (json!(3), json!(2)),
            "{page}"
        );
        assert_eq!(page["items"][0]["block"], json!(48_000_004), "{page}");
        assert_eq!(page["items"][0]["declaration"], json!("qos_payload"));

        let (_code, body) = get(router, "/ipfs/gave-up").await;
        let all = serde_json::from_slice::<Value>(&body).unwrap();
        assert_eq!(
            all["items"][2],
            json!({"cid": "QmLegacy", "block": 48_000_020, "slot": 1, "declaration": null, "error": null, "at": null}),
            "{all}"
        );
    }

    /// #1966: the llms.txt `init` writes sent agents to `/queries` for the nest's `views/*.sql`, which
    /// lists the mount's declared queries instead. Every route it names must be routed, and a line
    /// that says it serves the authored views must answer with one.
    #[tokio::test]
    async fn the_scaffolded_llms_txt_names_only_routes_that_answer_as_described() {
        let dir = tempfile::tempdir().unwrap();
        let contracts: Vec<crate::config::Contract> = vec![serde_json::from_value(json!({
            "alias": "usdc", "address": "0xa0b8", "abi": "abis/usdc.json"
        }))
        .unwrap()];
        let schema: Vec<crate::registry::TableSchema> = vec![serde_json::from_value(
            json!({"table": "usdc__transfer", "alias": "usdc", "event": "Transfer", "columns": []}),
        )
        .unwrap()];
        crate::project::scaffold_ai_surface(dir.path(), "mainnet", &contracts, &schema).unwrap();
        let llms = std::fs::read_to_string(dir.path().join("llms.txt")).unwrap();
        std::fs::create_dir_all(dir.path().join("views")).unwrap();
        std::fs::write(
            dir.path().join("views/10-whales.sql"),
            "CREATE VIEW whales AS SELECT 'from-the-view' AS marker;",
        )
        .unwrap();

        let placeholders = |route: &str, with: &str| {
            route
                .split('/')
                .map(|seg| if seg.starts_with('{') { with } else { seg })
                .collect::<Vec<_>>()
                .join("/")
        };
        let nest = dir.path();
        let ask = |path: String| async move {
            get(router(SharedNest::new(test_state(nest, 4))), &path).await
        };

        let mut entries: Vec<String> = Vec::new();
        for line in llms.lines() {
            match entries.last_mut() {
                Some(entry) if line.starts_with(' ') => {
                    entry.push(' ');
                    entry.push_str(line.trim());
                }
                _ => entries.push(line.to_string()),
            }
        }
        let mut routes = 0;
        let mut views_claimed = false;
        for line in &entries {
            let named: Vec<&str> = line
                .split("`GET ")
                .skip(1)
                .filter_map(|rest| rest.split('`').next())
                .collect();
            for route in &named {
                routes += 1;
                let path = placeholders(route.split('?').next().unwrap(), "x");
                let (status, body) = ask(path.clone()).await;
                assert!(
                    !(status == StatusCode::NOT_FOUND && body.is_empty()),
                    "llms.txt names `GET {route}`, which no route answers: {line}"
                );
            }
            if !line.contains("views/*.sql") {
                continue;
            }
            views_claimed = true;
            let mut answered = Vec::new();
            for route in &named {
                let path = match route.split_once("?q=") {
                    Some((path, _)) => format!("{path}?q=SELECT%20*%20FROM%20whales"),
                    None => placeholders(route, "whales"),
                };
                let (status, body) = ask(path.clone()).await;
                let body = String::from_utf8_lossy(&body).into_owned();
                answered.push((status, body.contains("from-the-view"), path, body));
            }
            assert!(
                answered
                    .iter()
                    .any(|(s, view, ..)| *s == StatusCode::OK && *view),
                "llms.txt says `{line}`, and no route on it answers with the view: {answered:?}"
            );
        }
        assert!(
            routes >= 10,
            "found only {routes} routes in llms.txt:\n{llms}"
        );
        assert!(
            views_claimed,
            "llms.txt no longer says where views/*.sql are served:\n{llms}"
        );
    }

    /// #1653: an authored view the engine refuses at define time vanished from `/sql` with a debug
    /// line. `/ready` names it, and stops once it builds.
    #[tokio::test]
    async fn ready_names_an_authored_view_that_will_not_build() {
        let dir = tempfile::tempdir().unwrap();
        std::fs::create_dir_all(dir.path().join("views")).unwrap();
        let view = dir.path().join("views/10-whales.sql");
        let broken = "CREATE VIEW whales AS SELECT 1 AS n WHERE 1 > 100000000000000000000;";
        let ask = || {
            let guard = crate::analytics::QueryGuard {
                timeout: std::time::Duration::from_secs(5),
                max_rows: 10,
            };
            let hot = crate::analytics::HotRows::new();
            crate::analytics::query_hot_cold(
                dir.path(),
                "SELECT n FROM whales",
                guard,
                &hot,
                0,
                &[],
            )
        };
        let failed = || async {
            let router = router(SharedNest::new(test_state(dir.path(), 4)));
            let (_, body) = get(router, "/ready").await;
            serde_json::from_slice::<serde_json::Value>(&body).unwrap()["views_failed"].clone()
        };

        let named = || async {
            let named = failed().await;
            assert_eq!(
                (named[0]["file"].as_str(), named[0]["view"].as_str()),
                (Some("10-whales.sql"), Some("whales")),
                "the refused view is not on /ready: {named}"
            );
            assert!(named[0]["error"].is_string(), "{named}");
        };

        // At startup, before any statement asks for it.
        std::fs::write(&view, broken).unwrap();
        assert_eq!(
            crate::analytics::validate_nest_views(dir.path(), &[]).len(),
            1
        );
        named().await;

        std::fs::write(&view, "CREATE VIEW whales AS SELECT 1 AS n;").unwrap();
        assert!(ask().is_ok());
        assert_eq!(
            failed().await,
            json!([]),
            "a view that builds is not reported"
        );

        // And when a statement defines it.
        std::fs::write(&view, broken).unwrap();
        assert!(ask().is_err());
        named().await;
    }

    /// #1399: `--ipfs-window-deadline` is on `/ready`, `0` for none and null before a nest records it.
    #[tokio::test]
    async fn ready_reports_the_ipfs_window_deadline() {
        use crate::metrics::METRICS;
        let dir = tempfile::tempdir().unwrap();
        let cases = [
            ("default-window-deadline", Some(300), json!(300)),
            ("no-window-deadline", Some(0), json!(0)),
            ("unrecorded-window-deadline", None, Value::Null),
        ];
        for (name, recorded, want) in cases {
            std::fs::create_dir_all(dir.path().join(name)).unwrap();
            let roster = json!({"runtime": "t", "nests": [{"name": name}]});
            let health = Arc::new(crate::health::RuntimeHealth::new());
            let nests = vec![(name.to_string(), test_state(&dir.path().join(name), 4))];
            let router = compose_runtime(roster, nests, health);
            if let Some(secs) = recorded {
                METRICS
                    .nest(name)
                    .set_ipfs_window_deadline(std::time::Duration::from_secs(secs));
            }
            let (_code, body) = get(router, &format!("/{name}/ready")).await;
            let json = serde_json::from_slice::<serde_json::Value>(&body).unwrap();
            assert_eq!(
                json["seal_direct_ipfs_window_deadline_secs"], want,
                "{name}: {json}"
            );
        }
    }

    /// #1199 ask 2: a tip-path seal that has stopped advancing must stop reporting ready.
    ///
    /// Before this, every seal term on `/ready` was gated on `seal_direct_active`, so a nest doing
    /// ordinary tip-following sealing had no clock at all. Measured live: two nests 739,192 and
    /// 405,093 blocks behind on `sealed_through`, both answering `ready: true`, both passing a cron
    /// health check for days while their tattler receipts pinned a two-day-old watermark.
    #[tokio::test]
    async fn a_tip_seal_that_stopped_advancing_reports_unready() {
        use crate::metrics::METRICS;
        let dir = tempfile::tempdir().unwrap();
        let name = "frozen-tip-seal";
        std::fs::create_dir_all(dir.path().join(name)).unwrap();
        let roster = json!({"runtime": "t", "nests": [{"name": name}]});
        let health = Arc::new(crate::health::RuntimeHealth::new());
        let nests = vec![(name.to_string(), test_state(&dir.path().join(name), 4))];
        let router = compose_runtime(roster, nests, health);

        let now = crate::metrics::now_unix();
        let handle = METRICS.nest(name);
        handle.set_tip(502_732_913);
        handle.set_last_block(502_732_913);
        handle.set_sealed_through(501_993_721);
        handle.mark_poll_ok();
        handle.set_last_progress_for_test(now);
        // Sealed once, then nothing for thirteen hours - past READINESS_TIP_SEAL_STALL_SECS. Set
        // after `set_sealed_through`, which now stamps this clock itself.
        handle.set_last_seal_progress_for_test(now.saturating_sub(46_800));

        let (code, body) = get(router, &format!("/{name}/ready")).await;
        let json = serde_json::from_slice::<serde_json::Value>(&body).unwrap();
        assert_eq!(
            code,
            StatusCode::SERVICE_UNAVAILABLE,
            "a seal frozen for 13h on a cursor at tip must not read ready: {json}"
        );
        assert_eq!(json["tip_seal_stalled"], json!(true));
        assert_eq!(json["seal_lag_blocks"], json!(739_192));
        // The tip path must report its own seal clock, not null: it was null on exactly the nests
        // that needed it.
        assert_age(&json, "seconds_since_seal_progress", 46_800);
        assert_eq!(
            json["seal_direct_stalled"],
            json!(false),
            "the backfill term must stay false: no pass was ever running, and conflating the two \
             is what hid this"
        );
    }

    /// #2262: a seal that fails on every attempt says why at once, and takes the nest unready once
    /// it has failed for longer than a quarter of an hour, however small the seal lag is.
    #[tokio::test]
    async fn a_seal_that_keeps_failing_says_why_and_reports_unready() {
        use crate::metrics::METRICS;
        let dir = tempfile::tempdir().unwrap();
        let name = "failing-tip-seal";
        std::fs::create_dir_all(dir.path().join(name)).unwrap();
        let roster = json!({"runtime": "t", "nests": [{"name": name}]});
        let health = Arc::new(crate::health::RuntimeHealth::new());
        let nests = vec![(name.to_string(), test_state(&dir.path().join(name), 4))];
        let router = compose_runtime(roster, nests, health);

        let now = crate::metrics::now_unix();
        let handle = METRICS.nest(name);
        handle.set_tip(1_000_100);
        handle.set_last_block(1_000_100);
        handle.set_sealed_through(1_000_000);
        handle.mark_poll_ok();
        handle.set_last_progress_for_test(now);
        let ready = || {
            let router = router.clone();
            async move {
                let (code, body) = get(router, &format!("/{name}/ready")).await;
                (code, serde_json::from_slice::<Value>(&body).unwrap())
            }
        };

        handle.record_seal(Some("reading segment t-1.parquet: not found".into()));
        let (code, json) = ready().await;
        assert_eq!(
            code,
            StatusCode::OK,
            "one failure is not yet a stall: {json}"
        );
        assert_eq!(json["seal_faulted"], json!(false), "{json}");
        assert_eq!(
            json["seal_fault"]["reason"],
            json!("reading segment t-1.parquet: not found"),
            "{json}"
        );

        handle.set_seal_fault_since_for_test(now.saturating_sub(1_000));
        let (code, json) = ready().await;
        assert_eq!(code, StatusCode::SERVICE_UNAVAILABLE, "{json}");
        assert_eq!(json["seal_faulted"], json!(true), "{json}");
        assert_eq!(json["tip_seal_stalled"], json!(false), "{json}");
        assert!(METRICS.render().contains(&format!(
            "nuthatch_nest_seal_fault_seconds{{nest=\"{name}\"}} 10"
        )));

        handle.record_seal(None);
        let (code, json) = ready().await;
        assert_eq!(code, StatusCode::OK, "{json}");
        assert!(json.get("seal_fault").is_none(), "{json}");
    }

    /// #1341: a sparse nest whose only seal is the span cut is not stalled for being quiet. Numbers
    /// measured on `hackathon-arcaidia-v2-sepolia`, 3.6.1: 5,974 blocks behind, 62,848 s since a seal,
    /// and `/ready` said 503 for a nest serving every row.
    #[tokio::test]
    async fn a_sparse_nest_short_of_its_span_cut_is_not_stalled() {
        use crate::metrics::METRICS;
        let dir = tempfile::tempdir().unwrap();
        let name = "sparse-tip-seal";
        std::fs::create_dir_all(dir.path().join(name)).unwrap();
        let roster = json!({"runtime": "t", "nests": [{"name": name}]});
        let health = Arc::new(crate::health::RuntimeHealth::new());
        let nests = vec![(name.to_string(), test_state(&dir.path().join(name), 4))];
        let router = compose_runtime(roster, nests, health);
        let span = crate::chains::DEFAULT_SEAL_SPAN;
        let tip = 11_694_278;
        let handle = METRICS.nest(name);
        handle.set_tip(tip);
        handle.set_last_block(tip);
        handle.mark_poll_ok();
        let status_at = |sealed: u64| {
            let now = crate::metrics::now_unix();
            handle.set_sealed_through(sealed);
            handle.set_last_progress_for_test(now);
            handle.set_last_seal_progress_for_test(now.saturating_sub(62_848));
            let router = router.clone();
            async move {
                let (code, body) = get(router, &format!("/{name}/ready")).await;
                (
                    code,
                    serde_json::from_slice::<serde_json::Value>(&body).unwrap(),
                )
            }
        };

        let (code, json) = status_at(11_688_304).await;
        assert_eq!(code, StatusCode::OK, "{json}");
        assert_eq!(json["tip_seal_stalled"], json!(false), "{json}");

        let (code, json) = status_at(tip - 2 * span).await;
        assert_eq!(
            code,
            StatusCode::OK,
            "exactly twice the span is not yet overdue: {json}"
        );

        let (code, json) = status_at(tip - 2 * span - 1).await;
        assert_eq!(code, StatusCode::SERVICE_UNAVAILABLE, "{json}");
        assert_eq!(json["tip_seal_stalled"], json!(true), "{json}");
    }

    /// The half that must not regress, and the one that would take a healthy nest down.
    ///
    /// A seal caught up to the cursor has nothing left to seal, so it stops stamping its clock -
    /// exactly as a cursor at tip stops stamping `last_progress`. A fixed-range nest (`end_block`)
    /// that has finished and sealed sits here permanently. Without the `last > sealed` guard it
    /// would report unready twelve hours later and stay that way, which is the trap `wedged`'s
    /// `lag > 0` guard already exists to avoid.
    #[tokio::test]
    async fn a_seal_caught_up_to_the_cursor_is_not_stalled() {
        use crate::metrics::METRICS;
        let dir = tempfile::tempdir().unwrap();
        let name = "caught-up-seal";
        std::fs::create_dir_all(dir.path().join(name)).unwrap();
        let roster = json!({"runtime": "t", "nests": [{"name": name}]});
        let health = Arc::new(crate::health::RuntimeHealth::new());
        let nests = vec![(name.to_string(), test_state(&dir.path().join(name), 4))];
        let router = compose_runtime(roster, nests, health);

        let now = crate::metrics::now_unix();
        let handle = METRICS.nest(name);
        handle.set_tip(1_000_000);
        handle.set_last_block(1_000_000);
        handle.set_sealed_through(1_000_000);
        handle.mark_poll_ok();
        handle.set_last_progress_for_test(now);
        // A whole day since anything sealed, because there has been nothing to seal.
        handle.set_last_seal_progress_for_test(now.saturating_sub(86_400));

        let (code, body) = get(router, &format!("/{name}/ready")).await;
        let json = serde_json::from_slice::<serde_json::Value>(&body).unwrap();
        assert_eq!(
            code,
            StatusCode::OK,
            "a seal with nothing left to seal is not a stalled one: {json}"
        );
        assert_eq!(json["tip_seal_stalled"], json!(false));
        assert_eq!(json["seal_lag_blocks"], json!(0));
    }

    /// The half that would have made the new field useless: a nest that has sealed nothing yet must
    /// not report its whole indexed history as seal lag. `sealed_through` is 0 by default, and a
    /// subtraction against it would have read 502,732,913 blocks behind on a healthy first minute -
    /// which is how a new field becomes a false alarm nobody trusts and everybody filters out.
    #[tokio::test]
    async fn ready_reports_no_seal_lag_before_anything_has_sealed() {
        use crate::metrics::METRICS;
        let dir = tempfile::tempdir().unwrap();
        let name = "nothing-sealed";
        std::fs::create_dir_all(dir.path().join(name)).unwrap();
        let roster = json!({"runtime": "t", "nests": [{"name": name}]});
        let health = Arc::new(crate::health::RuntimeHealth::new());
        let nests = vec![(name.to_string(), test_state(&dir.path().join(name), 4))];
        let router = compose_runtime(roster, nests, health);

        let handle = METRICS.nest(name);
        handle.set_tip(502_732_913);
        handle.set_last_block(502_732_913);
        handle.mark_poll_ok();
        handle.set_last_progress_for_test(crate::metrics::now_unix());

        let (_, body) = get(router, &format!("/{name}/ready")).await;
        let json = serde_json::from_slice::<serde_json::Value>(&body).unwrap();
        assert_eq!(json["sealed_through"], json!(0));
        assert_eq!(
            json["seal_lag_blocks"],
            serde_json::Value::Null,
            "nothing sealed yet is not a 502-million-block lag: {json}"
        );
    }

    /// The watermark is a block number and carries no clock of its own, so the stamp must move only
    /// when the number does. A pass that keeps re-reporting the same block is not making progress
    /// and must not refresh its own deadline by saying so.
    #[test]
    fn re_reporting_the_same_block_does_not_refresh_the_seal_clock() {
        use crate::metrics::METRICS;
        let handle = METRICS.nest("stamp-check");
        handle.begin_seal_direct(1_000, 2_000);
        handle.set_seal_direct_completed(1_500);
        handle.set_last_seal_progress_for_test(1_000);

        handle.set_seal_direct_completed(1_500);
        assert_eq!(
            handle.last_seal_progress(),
            1_000,
            "the same block again is not progress"
        );

        handle.set_seal_direct_completed(1_501);
        assert!(
            handle.last_seal_progress() > 1_000,
            "an advancing block must stamp the clock"
        );
        handle.end_seal_direct();
    }

    // ---------------------------------------------------------------------------------------
    // #1025 - a role with no cursor is judged on what it serves.
    //
    // `graph-staking-legacy-readonly` on the Lodestar box is a sealed-history shadow started with
    // `nuthatch serve`. It owns no cursor, makes zero RPC calls, answered 162 queries correctly,
    // and reported `ready:false, stalled:true` continuously from 2026-08-24 - because
    // `poll_stalled` falls back to `started_at` when `last_poll` is 0, so the startup grace expires
    // and never returns.
    //
    // The mirror of #1020: there an unpopulated gauge read as perfect health and no alert could
    // fire; here a healthy service reads as permanently stalled, so any alert fires forever and
    // gets muted. These drive the real router, because the defect was in the endpoint's verdict.
    // ---------------------------------------------------------------------------------------

    /// **Per-nest metrics, not the process globals.**
    ///
    /// The first version of these tests stamped `METRICS.set_started_at_for_test` - a *process*
    /// global - and read `/ready`'s `None` branch. Cargo runs tests in parallel, so they raced each
    /// other's stamp and **both mutations of the fix survived**: reverting the verdict and reverting
    /// the null-vs-zero rendering each left every case green. A test whose fixture is shared mutable
    /// state is timing the scheduler.
    ///
    /// Setting `runtime_health` puts `/ready` on its per-nest branch, so each case owns a
    /// uniquely-named `NestMetrics` and nothing is shared.
    async fn ready_of(cursorless: bool, started_secs_ago: u64) -> serde_json::Value {
        use std::sync::atomic::{AtomicU64, Ordering};
        static N: AtomicU64 = AtomicU64::new(0);
        let name = format!("cursorless-probe-{}", N.fetch_add(1, Ordering::SeqCst));

        let dir = tempfile::tempdir().unwrap();
        let mut st = test_state(dir.path(), SQL_MAX_CONCURRENCY);
        st.cursorless = cursorless;
        let health = Arc::new(crate::health::RuntimeHealth::default());
        health.register(&name, "arbitrum-one");
        st.runtime_health = Some((name.clone(), health));
        let router = router(SharedNest::new(st));

        let now = crate::metrics::now_unix();
        let m = crate::metrics::METRICS.nest(&name);
        // Never polled, started long enough ago that any grace has expired - the live shape.
        m.set_started_at_for_test(now.saturating_sub(started_secs_ago));
        assert_eq!(m.last_poll_ok(), 0, "premise: this nest has never polled");

        let (_code, body) = get(router, "/ready").await;
        serde_json::from_slice(&body).unwrap()
    }

    #[tokio::test]
    async fn a_cursorless_role_that_never_polls_is_ready() {
        let b = ready_of(true, 86_400).await;
        assert_eq!(
            b["ready"],
            json!(true),
            "a `nuthatch serve` nest never polls by design; judging it on a poll clock leaves it \
             permanently unready while it answers queries correctly (#1025): {b}"
        );
        assert_eq!(b["stalled"], json!(false), "{b}");
    }

    /// The control that stops the fix being a blanket exemption.
    #[tokio::test]
    async fn a_cursor_owning_role_that_never_polls_is_still_stalled() {
        let b = ready_of(false, 86_400).await;
        assert_eq!(
            b["ready"],
            json!(false),
            "a role that owns a cursor and has not polled within the grace window must still be \
             unready. If this passes, #1025 has disabled the check for everyone rather than for the \
             role that has no cursor: {b}"
        );
        assert_eq!(b["stalled"], json!(true), "{b}");
    }

    /// Absent, not zero - the other half of #1020's lesson on this surface.
    ///
    /// `0` is a **value**: a dashboard reads `lag_blocks: 0` as "exactly at tip". A role with no
    /// cursor has no lag at all, and saying so is honest where saying zero is a claim.
    #[tokio::test]
    async fn a_cursorless_role_reports_no_tip_rather_than_a_tip_of_zero() {
        let b = ready_of(true, 86_400).await;
        assert!(
            b["tip"].is_null(),
            "`tip` must be null for a role with no cursor, not 0 - 0 reads as a block height: {b}"
        );
        assert!(
            b["lag_blocks"].is_null(),
            "`lag_blocks` must be null, not 0. 0 is the healthiest possible reading, which is \
             exactly how #1020 hid a real fault on the metrics surface: {b}"
        );
        assert_eq!(
            b["cursorless"],
            json!(true),
            "the body must say which shape it is, or an operator cannot tell an absent tip from a \
             broken one: {b}"
        );
    }

    #[tokio::test]
    async fn a_cursor_owning_role_still_reports_its_numbers() {
        let b = ready_of(false, 86_400).await;
        assert!(
            b["tip"].is_number() && b["lag_blocks"].is_number(),
            "nulling these for a cursor-owning role would remove the figures an operator uses: {b}"
        );
    }

    #[test]
    fn seal_stall_logic() {
        let now = 100_000u64;
        // No stamp yet: fall back to start time, same as progress_stalled.
        assert!(seal_direct_stalled(0, now - 1_000, now, 900));
        assert!(!seal_direct_stalled(0, now - 100, now, 900));
        // Stamped: judge on the stamp, not on how long the pass has run.
        assert!(!seal_direct_stalled(now - 100, now - 86_400, now, 900));
        assert!(seal_direct_stalled(now - 1_000, now - 86_400, now, 900));
        // Nothing known at all is not a stall - a just-started pass gets grace.
        assert!(!seal_direct_stalled(0, 0, now, 900));
    }

    /// A two-nest mounts composition, built the same way `run_runtime` builds one.
    fn two_nest_roost(dir: &std::path::Path, health: Arc<crate::health::RuntimeHealth>) -> Router {
        let roster = json!({"runtime": "t", "nests": [{"name": "alpha"}, {"name": "beta"}]});
        let nests = vec![
            ("alpha".to_string(), test_state(&dir.join("a"), 4)),
            ("beta".to_string(), test_state(&dir.join("b"), 4)),
        ];
        compose_runtime(roster, nests, health)
    }

    /// RFC-0027 slice 1: serving through the swappable handle must be **indistinguishable** from
    /// serving the composed router directly.
    ///
    /// This is the whole point of slice 1. The lifecycle slices are only safe to build on top of an
    /// indirection that provably changes nothing, and "it looked fine when I clicked around" is not
    /// that proof - so every root route, a per-nest route under its prefix, and a 404 are compared
    /// **status and body bytes**, both ways.
    #[tokio::test]
    async fn the_dispatcher_serves_byte_identically_to_the_static_router() {
        let tmp = tempfile::tempdir().unwrap();
        std::fs::create_dir_all(tmp.path().join("a")).unwrap();
        std::fs::create_dir_all(tmp.path().join("b")).unwrap();
        let health = Arc::new(crate::health::RuntimeHealth::new());
        health.register("alpha", "mainnet");
        health.register("beta", "mainnet");

        for path in [
            "/health",
            "/ready",
            "/nests",
            "/alpha/health",
            "/beta/health",
            "/alpha/ready",
            "/nope",         // 404 at the root
            "/alpha/nope",   // 404 within a mounted nest
            "/gamma/health", // 404 for a nest that isn't mounted
        ] {
            let direct = get(two_nest_roost(tmp.path(), health.clone()), path).await;
            let dispatched = get(
                LiveRuntime::new(two_nest_roost(tmp.path(), health.clone())).service(),
                path,
            )
            .await;
            assert_eq!(
                direct, dispatched,
                "{path} must be identical through the dispatcher; got {direct:?} vs {dispatched:?}"
            );
        }
    }

    #[test]
    fn a_trailing_slash_is_trimmed_and_the_query_kept() {
        for (from, to) in [
            ("/usdc/", "/usdc"),
            ("/usdc/sql/?q=SELECT%201", "/usdc/sql?q=SELECT%201"),
            ("/acme/usdc//", "/acme/usdc"),
            ("/", "/"),
            ("/usdc", "/usdc"),
        ] {
            let mut req = axum::http::Request::builder()
                .uri(from)
                .body(axum::body::Body::empty())
                .unwrap();
            trim_trailing_slash(&mut req);
            assert_eq!(req.uri().to_string(), to, "{from}");
        }
    }

    /// The capability slice 1 exists to enable, proven now so the lifecycle slices inherit it rather
    /// than discover it: swapping the composition changes what is served, without rebinding anything.
    #[tokio::test]
    async fn swapping_the_composition_changes_what_is_served() {
        let tmp = tempfile::tempdir().unwrap();
        std::fs::create_dir_all(tmp.path().join("a")).unwrap();
        std::fs::create_dir_all(tmp.path().join("b")).unwrap();
        let health = Arc::new(crate::health::RuntimeHealth::new());
        health.register("alpha", "mainnet");
        health.register("beta", "mainnet");

        let live = LiveRuntime::new(two_nest_roost(tmp.path(), health.clone()));
        let (status, _) = get(live.service(), "/beta/health").await;
        assert_eq!(status, StatusCode::OK, "beta is mounted to begin with");

        // Re-compose with beta absent - the shape an unmount will take.
        //
        // The replacement `alpha` state opens a *different* store directory, because redb is
        // single-writer and the original composition still holds `a/t.redb`. That is not a test
        // artefact: it is exactly why RFC-0027 §6 makes unmount a **drain** - the old nest's store has
        // to be closed cleanly before anything reopens it, and a half-closed store is the one way a
        // lifecycle operation could corrupt data that faults never do.
        std::fs::create_dir_all(tmp.path().join("a2")).unwrap();
        let roster = json!({"runtime": "t", "nests": [{"name": "alpha"}]});
        let only_alpha = compose_runtime(
            roster,
            vec![("alpha".to_string(), test_state(&tmp.path().join("a2"), 4))],
            health.clone(),
        );
        live.swap(only_alpha);

        let (alpha, _) = get(live.service(), "/alpha/health").await;
        let (beta, _) = get(live.service(), "/beta/health").await;
        assert_eq!(alpha, StatusCode::OK, "alpha keeps serving across the swap");
        assert_eq!(beta, StatusCode::NOT_FOUND, "beta is gone from the new set");
    }

    /// RFC-0026 §5: the roster is a *live* view. The static half is computed at startup, but health is
    /// merged per request - and a quarantined nest must never come back reported as indexing.
    #[test]
    fn the_roster_reports_live_health_per_nest() {
        use crate::health::RuntimeHealth;
        let health = RuntimeHealth::new();
        health.register("alpha", "base");
        health.register("beta", "base");
        let roster = json!({
            "runtime": "test",
            "nests": [ {"name": "alpha", "chain": "base"}, {"name": "beta", "chain": "base"} ],
        });

        // All healthy: every entry says so, and nothing carries a quarantine block.
        let merged = merge_roster_health(&roster, &health);
        assert_eq!(merged["nests"][0]["health"], "indexing");
        assert!(merged["nests"][0].get("quarantine").is_none());
        assert_eq!(merged["all_indexing"], json!(true));

        // Quarantine one: only that entry changes, and it carries the reason an operator needs.
        health.quarantine_nest(
            "alpha",
            "the balance IVM circuit thread has died".into(),
            0,
            None,
        );
        let merged = merge_roster_health(&roster, &health);
        assert_eq!(merged["nests"][0]["health"], "quarantined");
        assert_eq!(merged["nests"][0]["quarantine"]["class"], "terminal");
        assert!(merged["nests"][0]["quarantine"]["reason"]
            .as_str()
            .unwrap()
            .contains("balance IVM"));
        assert_eq!(merged["nests"][1]["health"], "indexing");
        assert_eq!(merged["all_indexing"], json!(false));

        // Recovery clears the quarantine block rather than leaving a stale one behind.
        health.mark_indexing("alpha");
        let merged = merge_roster_health(&roster, &health);
        assert_eq!(merged["nests"][0]["health"], "indexing");
        assert!(merged["nests"][0].get("quarantine").is_none());
    }

    /// RFC-0026 §5: mounts-root `/ready` is 200 only while **everything** is indexing, and 503 names the
    /// offenders. Conservative on purpose - a supervisor should treat a partly-broken mounts as
    /// not-ready, while the healthy nests carry on serving anyone who asks for them directly.
    #[tokio::test]
    async fn roost_readiness_goes_unavailable_as_soon_as_anything_is_quarantined() {
        use crate::health::RuntimeHealth;
        let health = RuntimeHealth::new();
        health.register("alpha", "base");
        health.register("beta", "arbitrum-one");
        assert_eq!(
            roost_ready(&health, &[]).into_response().status(),
            StatusCode::OK
        );

        // One chain's cursor dies: the runtime is no longer ready, even though `beta` is perfectly fine.
        health.quarantine_cursor(
            "base",
            "every nest on this cursor is terminally quarantined".into(),
        );
        let resp = roost_ready(&health, &[]).into_response();
        assert_eq!(resp.status(), StatusCode::SERVICE_UNAVAILABLE);
        let body = axum::body::to_bytes(resp.into_body(), 64 * 1024)
            .await
            .unwrap();
        let v: Value = serde_json::from_slice(&body).unwrap();
        assert_eq!(v["ready"], json!(false));
        assert_eq!(v["version"], json!(env!("CARGO_PKG_VERSION")));
        assert_eq!(v["quarantined"][0]["nest"], "alpha");
        assert_eq!(
            v["quarantined"].as_array().unwrap().len(),
            1,
            "beta is fine"
        );
    }

    /// #445: a contract-free blocks nest has no address to name, and `GET /` says so with `null`
    /// rather than inventing one. The key stays present either way - a client reading `.address`
    /// gets a value it can test, not a missing field it has to guess about.
    #[test]
    fn the_summary_names_no_address_for_a_contract_free_nest() {
        let dir = tempfile::tempdir().unwrap();
        let mut s = test_state(dir.path(), 1);
        s.address = None;
        let v = summary_value(&s);
        assert!(
            v.get("address").is_some(),
            "the address key stays present for a contract-free nest"
        );
        assert!(
            v["address"].is_null(),
            "and its value is null, not an invented address: {}",
            v["address"]
        );
    }

    /// The RFC-0020 hot-swap mechanism: re-pointing a `SharedNest` atomically changes what serving
    /// resolves - both `current()` and the per-request `FromRef` a handler goes through - with no
    /// rebind. This is what lets a compatible upgrade flip an endpoint's backing underneath live
    /// traffic (slice 2b drives the flip; this proves the mechanism).
    #[tokio::test]
    async fn shared_nest_swaps_the_backing_atomically() {
        use axum::extract::FromRef;
        let d1 = tempfile::tempdir().unwrap();
        let mut v1 = test_state(d1.path(), 1);
        v1.address = Some("0xv1".into());
        let shared = SharedNest::new(v1);
        assert_eq!(shared.current().address.as_deref(), Some("0xv1"));
        assert_eq!(AppState::from_ref(&shared).address.as_deref(), Some("0xv1"));

        // Flip to a new backing version - same handle, next request sees v2.
        let d2 = tempfile::tempdir().unwrap();
        let mut v2 = test_state(d2.path(), 1);
        v2.address = Some("0xv2".into());
        shared.swap(v2);
        assert_eq!(shared.current().address.as_deref(), Some("0xv2"));
        assert_eq!(AppState::from_ref(&shared).address.as_deref(), Some("0xv2"));
    }

    /// A gate that stays saturated still answers 503 - but only after the bounded wait, not
    /// immediately (#1319). The permit count is what caps concurrency; the wait only caps how long
    /// a caller hangs on before being told no.
    #[tokio::test]
    async fn sql_returns_503_when_gate_stays_saturated() {
        let tmp = tempfile::tempdir().unwrap();
        let state = test_state(tmp.path(), 1);
        // Hold the only permit for the whole call - nothing can free it, so the wait must elapse.
        let held = Arc::clone(&state.sql_gate).try_acquire_owned().unwrap();
        let started = std::time::Instant::now();
        let resp = sql(
            State(state.clone()),
            Query(SqlQuery {
                q: "SELECT 1".into(),
                max_rows: None,
            }),
        )
        .await
        .into_response();
        let waited = started.elapsed();
        assert_eq!(resp.status(), StatusCode::SERVICE_UNAVAILABLE);
        // A literal floor, deliberately not `SQL_ADMISSION_WAIT`: written against the constant this
        // assertion still passes when the constant is mutated to zero, which is precisely the
        // regression it exists to catch. 200 ms sits just under the 250 ms the constant declares.
        assert!(
            waited >= Duration::from_millis(200),
            "refused after {waited:?}, so the request was never queued"
        );
        drop(held);
    }

    /// #1657: a hot miss on `/entity` is a cold scan, so it waits on the same gate as `/sql`.
    #[tokio::test]
    async fn entity_cold_read_waits_on_the_sql_gate() {
        let tmp = tempfile::tempdir().unwrap();
        let state = test_state(tmp.path(), 1);
        let held = Arc::clone(&state.sql_gate).try_acquire_owned().unwrap();
        let started = std::time::Instant::now();
        let resp = entity(State(state.clone()), Path("1-1".into()))
            .await
            .into_response();
        let waited = started.elapsed();
        assert_eq!(resp.status(), StatusCode::SERVICE_UNAVAILABLE);
        assert!(
            waited >= Duration::from_millis(200),
            "refused after {waited:?}, so the cold read never queued"
        );
        drop(held);
    }

    /// #1657: the cold read that does run is the guarded one. A permit alone still leaves the scan
    /// without a deadline.
    #[tokio::test]
    async fn entity_cold_read_uses_the_query_guard() {
        let tmp = tempfile::tempdir().unwrap();
        let state = test_state(tmp.path(), 1);
        let before = crate::analytics::entity_guarded_reads();
        let resp = entity(State(state), Path("1-1".into()))
            .await
            .into_response();
        assert_eq!(resp.status(), StatusCode::NOT_FOUND);
        assert_eq!(crate::analytics::entity_guarded_reads(), before + 1);
    }

    /// `table` sealed at `sealed`, hot at `hot`, one row per block, served through `router()`.
    fn table_fixture(
        dir: &std::path::Path,
        sealed: std::ops::RangeInclusive<u64>,
        hot: std::ops::RangeInclusive<u64>,
    ) -> AppState {
        let row = |b: u64| json!({"table":"t","block_number":b,"log_index":0,"v":"x"}).to_string();
        let cold: Vec<String> = sealed.clone().map(row).collect();
        crate::seal::seal_range(dir, &cold, *sealed.start(), *sealed.end()).unwrap();
        let mut state = test_state(dir, 1);
        for b in hot {
            state
                .store
                .put_entity(&Store::entity_key(b, 0), &row(b))
                .unwrap();
        }
        state.tables = Arc::new(vec![crate::registry::TableSchema {
            table: "t".into(),
            alias: "t".into(),
            kind: crate::registry::TableKind::Event,
            function: String::new(),
            selector: String::new(),
            event: "T".into(),
            topic0: "0x".into(),
            columns: crate::registry::implicit_columns(false),
        }]);
        state
    }

    async fn get_json(state: AppState, uri: &str) -> (StatusCode, Value) {
        use tower::ServiceExt;
        let req = axum::http::Request::builder()
            .uri(uri)
            .body(axum::body::Body::empty())
            .unwrap();
        let resp = router(SharedNest::new(state)).oneshot(req).await.unwrap();
        let status = resp.status();
        let body = axum::body::to_bytes(resp.into_body(), usize::MAX)
            .await
            .unwrap();
        (status, serde_json::from_slice(&body).unwrap())
    }

    /// #1659: with the SQL gate saturated `/table` cannot read the sealed rows, and its answer must
    /// A redb store whose byte-exact tip scan fails, as a damaged hot store would (#472). Every
    /// other call reaches the real store.
    struct TipFails(Store);

    #[async_trait::async_trait]
    impl crate::store::HotStore for TipFails {
        fn write_generation(&self) -> Option<u64> {
            crate::store::HotStore::write_generation(&self.0)
        }
        fn rows_generation(&self) -> Option<u64> {
            crate::store::HotStore::rows_generation(&self.0)
        }
        fn put_entity(&self, key: &str, json: &str) -> Result<()> {
            crate::store::HotStore::put_entity(&self.0, key, json)
        }
        fn put_entity_if_named(
            &self,
            key: &str,
            json: &str,
            source_key: &str,
            block_hash: &str,
        ) -> Result<bool> {
            self.0
                .put_entity_if_named(key, json, source_key, block_hash)
        }
        fn put_entities_if_named(
            &self,
            entries: &[(String, String)],
            source_key: &str,
            block_hash: &str,
        ) -> Result<bool> {
            self.0
                .put_entities_if_named(entries, source_key, block_hash)
        }
        fn get_entity(&self, key: &str) -> Result<Option<String>> {
            crate::store::HotStore::get_entity(&self.0, key)
        }
        fn count(&self) -> Result<u64> {
            crate::store::HotStore::count(&self.0)
        }
        fn recent(&self, limit: usize) -> Result<Vec<String>> {
            crate::store::HotStore::recent(&self.0, limit)
        }
        fn recent_by_table(&self, table: &str, limit: usize) -> Result<Vec<String>> {
            crate::store::HotStore::recent_by_table(&self.0, table, limit)
        }
        fn hot_rows_by_table(&self) -> Result<std::collections::HashMap<String, Vec<Value>>> {
            crate::store::HotStore::hot_rows_by_table(&self.0)
        }
        fn hot_rows_by_table_bounded(
            &self,
            max_rows: usize,
        ) -> Result<std::collections::HashMap<String, Vec<Value>>> {
            crate::store::HotStore::hot_rows_by_table_bounded(&self.0, max_rows)
        }
        fn hot_rows_by_table_bounded_with_bytes(
            &self,
            _max_rows: usize,
            _max_bytes: u64,
        ) -> Result<crate::store::HotRowsSnapshot> {
            anyhow::bail!("hot store would not scan")
        }
        fn entities_in_range(&self, from: u64, to: u64) -> Result<Vec<String>> {
            crate::store::HotStore::entities_in_range(&self.0, from, to)
        }
        fn sample_entity_keys(&self, limit: usize) -> Result<Vec<String>> {
            crate::store::HotStore::sample_entity_keys(&self.0, limit)
        }
        fn get_meta(&self, key: &str) -> Result<Option<String>> {
            crate::store::HotStore::get_meta(&self.0, key)
        }
        fn set_meta(&self, key: &str, value: &str) -> Result<()> {
            crate::store::HotStore::set_meta(&self.0, key, value)
        }
        fn meta_with_prefix(&self, prefix: &str, limit: usize) -> Result<Vec<(String, String)>> {
            crate::store::HotStore::meta_with_prefix(&self.0, prefix, limit)
        }
        fn count_meta_with_prefix(&self, prefix: &str) -> Result<u64> {
            crate::store::HotStore::count_meta_with_prefix(&self.0, prefix)
        }
        fn indexed_head(&self) -> Result<Option<u64>> {
            crate::store::HotStore::indexed_head(&self.0)
        }
        fn sealed_through(&self) -> u64 {
            crate::store::HotStore::sealed_through(&self.0)
        }
        fn set_block_hash(&self, block: u64, hash: &str) -> Result<()> {
            crate::store::HotStore::set_block_hash(&self.0, block, hash)
        }
        fn get_block_hash(&self, block: u64) -> Result<Option<String>> {
            crate::store::HotStore::get_block_hash(&self.0, block)
        }
        fn set_block_timestamp(&self, block: u64, timestamp: u64) -> Result<()> {
            crate::store::HotStore::set_block_timestamp(&self.0, block, timestamp)
        }
        fn get_block_timestamp(&self, block: u64) -> Result<Option<u64>> {
            crate::store::HotStore::get_block_timestamp(&self.0, block)
        }
        fn checkpoints_desc(&self) -> Result<Vec<(u64, String)>> {
            crate::store::HotStore::checkpoints_desc(&self.0)
        }
        fn commit_window(
            &self,
            entities: &[(String, String)],
            checkpoint: Option<(u64, &str)>,
            last_block: u64,
        ) -> Result<()> {
            crate::store::HotStore::commit_window(&self.0, entities, checkpoint, last_block)
        }
        async fn commit_window_blocking(
            &self,
            entities: Vec<(String, String)>,
            outbox: Vec<String>,
            checkpoint: Option<(u64, String)>,
            last_block: u64,
        ) -> Result<()> {
            crate::store::HotStore::commit_window_blocking(
                &self.0, entities, outbox, checkpoint, last_block,
            )
            .await
        }
        async fn settle_commits(&self) {
            crate::store::HotStore::settle_commits(&self.0).await
        }
        fn rollback_to(&self, block: u64) -> Result<u64> {
            crate::store::HotStore::rollback_to(&self.0, block)
        }
        fn rollback_to_and_set_meta(
            &self,
            block: u64,
            meta_key: &str,
            meta_val: &str,
            outbox: &[String],
        ) -> Result<u64> {
            crate::store::HotStore::rollback_to_and_set_meta(
                &self.0, block, meta_key, meta_val, outbox,
            )
        }
        fn prune_range(&self, from: u64, to: u64) -> Result<u64> {
            crate::store::HotStore::prune_range(&self.0, from, to)
        }
        fn prune_and_set_meta(
            &self,
            from: u64,
            to: u64,
            meta_key: &str,
            meta_val: &str,
        ) -> Result<u64> {
            crate::store::HotStore::prune_and_set_meta(&self.0, from, to, meta_key, meta_val)
        }
        fn claim(&self, owner: &str) -> Result<u64> {
            crate::store::HotStore::claim(&self.0, owner)
        }
        fn acquire_lease(&self, owner: &str, ttl_secs: u64) -> Result<crate::store::Lease> {
            crate::store::HotStore::acquire_lease(&self.0, owner, ttl_secs)
        }
        fn renew_lease(&self, ttl_secs: u64) -> Result<crate::store::Lease> {
            crate::store::HotStore::renew_lease(&self.0, ttl_secs)
        }
        fn release_lease(&self) -> Result<()> {
            crate::store::HotStore::release_lease(&self.0)
        }
        fn current_lease(&self) -> Result<Option<crate::store::Lease>> {
            crate::store::HotStore::current_lease(&self.0)
        }
        fn current_fence(&self) -> Result<u64> {
            crate::store::HotStore::current_fence(&self.0)
        }
        fn held_fence(&self) -> u64 {
            crate::store::HotStore::held_fence(&self.0)
        }
        fn outbox_push(&self, payload: &str) -> Result<u64> {
            crate::store::HotStore::outbox_push(&self.0, payload)
        }
        fn outbox_pending(&self, limit: usize) -> Result<Vec<(u64, String)>> {
            crate::store::HotStore::outbox_pending(&self.0, limit)
        }
        fn outbox_remove(&self, seq: u64) -> Result<()> {
            crate::store::HotStore::outbox_remove(&self.0, seq)
        }
        async fn outbox_remove_batch_blocking(&self, seqs: Vec<u64>) -> Result<()> {
            crate::store::HotStore::outbox_remove_batch_blocking(&self.0, seqs).await
        }
        fn outbox_len(&self) -> u64 {
            crate::store::HotStore::outbox_len(&self.0)
        }
        fn outbox_trim(&self, max: u64) -> Result<u64> {
            crate::store::HotStore::outbox_trim(&self.0, max)
        }
        fn released(&self) -> futures::future::BoxFuture<'static, ()> {
            crate::store::HotStore::released(&self.0)
        }
    }

    /// #1935: an answer served from sealed segments alone, because the hot store would not scan,
    /// must say so in the field named for it and not only in `tip_unavailable`.
    #[tokio::test]
    async fn a_cold_only_sql_answer_says_its_source_is_sealed() {
        let tmp = tempfile::tempdir().unwrap();
        let mut state = table_fixture(tmp.path(), 1..=3, 4..=5);
        state.store =
            std::sync::Arc::new(TipFails(Store::open(&tmp.path().join("f.redb")).unwrap()));
        let (status, body) = get_json(state, "/sql?q=SELECT%20count(*)%20AS%20n%20FROM%20t").await;
        assert_eq!(status, StatusCode::OK, "{body}");
        assert_eq!(body["tip_unavailable"], true, "{body}");
        assert_eq!(body["provenance"]["source"], "sealed", "{body}");
    }

    /// say it is degraded rather than pass the hot rows off as the merged table.
    #[tokio::test]
    async fn a_table_answer_without_its_sealed_rows_says_it_is_degraded() {
        let tmp = tempfile::tempdir().unwrap();
        let state = table_fixture(tmp.path(), 1..=3, 4..=5);
        let held = Arc::clone(&state.sql_gate).try_acquire_owned().unwrap();
        let (status, body) = get_json(state.clone(), "/table/t?limit=10").await;
        drop(held);
        assert_eq!(status, StatusCode::OK, "{body}");
        assert_eq!(body["count"], 2, "{body}");
        assert_eq!(
            body["degraded"], true,
            "hot-only rows reported as the merged table: {body}"
        );
        assert_eq!(body["degraded_tables"], json!(["t"]), "{body}");

        let (status, body) = get_json(state, "/table/t?limit=10").await;
        assert_eq!(status, StatusCode::OK, "{body}");
        assert_eq!(body["count"], 5, "hot and sealed merge: {body}");
        assert_eq!(body["degraded"], false, "{body}");
        assert_eq!(body["degraded_tables"], json!([]), "{body}");
    }

    /// A table nothing has been sealed for yet is complete in the hot store, not degraded.
    #[tokio::test]
    async fn a_table_with_nothing_sealed_is_not_degraded() {
        let tmp = tempfile::tempdir().unwrap();
        let mut state = table_fixture(tmp.path(), 1..=1, 4..=5);
        state.dir = tmp.path().join("young");
        std::fs::create_dir_all(&state.dir).unwrap();
        let (status, body) = get_json(state, "/table/t?limit=10").await;
        assert_eq!(status, StatusCode::OK, "{body}");
        assert_eq!(body["count"], 2, "{body}");
        assert_eq!(body["degraded"], false, "{body}");
    }

    /// #1659: a block window is applied to the hot rows before the limit, so a window older than
    /// the newest `limit` hot rows still finds the rows inside it.
    #[tokio::test]
    async fn a_table_window_below_the_newest_hot_rows_finds_them() {
        let tmp = tempfile::tempdir().unwrap();
        let state = table_fixture(tmp.path(), 1..=3, 4..=9);
        let (status, body) = get_json(state, "/table/t?limit=2&from_block=4&to_block=5").await;
        assert_eq!(status, StatusCode::OK, "{body}");
        let blocks: Vec<u64> = body["items"]
            .as_array()
            .unwrap()
            .iter()
            .map(|v| v["block_number"].as_u64().unwrap())
            .collect();
        assert_eq!(blocks, vec![5, 4], "{body}");
    }

    /// The case the wait exists for: a permit freed while the caller is queued admits it, where
    /// fail-fast answered 503 for a query that would have run milliseconds later. This is the one
    /// in five the hackathon nests were bouncing.
    #[tokio::test]
    async fn a_permit_freed_during_the_wait_admits_the_query() {
        let tmp = tempfile::tempdir().unwrap();
        let state = test_state(tmp.path(), 1);
        let held = Arc::clone(&state.sql_gate).try_acquire_owned().unwrap();
        // Freed well inside `SQL_ADMISSION_WAIT`, as a burst's first query finishing would free it.
        tokio::spawn(async move {
            tokio::time::sleep(Duration::from_millis(40)).await;
            drop(held);
        });
        let resp = sql(
            State(state.clone()),
            Query(SqlQuery {
                q: "SELECT 1319 AS n".into(),
                max_rows: None,
            }),
        )
        .await
        .into_response();
        assert_eq!(
            resp.status(),
            StatusCode::OK,
            "a permit freed during the wait should have admitted this query, not refused it"
        );
    }

    /// The wait bounds how *long* one request waits; this bounds how *many* wait at once. Without
    /// it a burst parks arrival-rate-times-250 ms requests before any of them time out, which is
    /// the protection fail-fast used to give for free (Jules on #1383).
    #[test]
    fn the_parked_request_count_is_capped() {
        use std::sync::atomic::Ordering;
        let queued: Arc<std::sync::atomic::AtomicUsize> = Default::default();
        let held: Vec<QueueSlot> = (0..SQL_MAX_QUEUED)
            .map(|_| QueueSlot::take(&queued).expect("under the cap"))
            .collect();
        assert_eq!(queued.load(Ordering::Relaxed), SQL_MAX_QUEUED);
        assert!(
            QueueSlot::take(&queued).is_none(),
            "the {SQL_MAX_QUEUED}th+1 request must be refused rather than parked"
        );
        // A slot is released however its wait ended, so the queue recovers rather than wedging.
        drop(held);
        assert_eq!(queued.load(Ordering::Relaxed), 0);
        assert!(QueueSlot::take(&queued).is_some());
    }

    /// A request refused because the queue is full is refused *immediately* - it must not also sit
    /// through the wait, which would be the worst of both designs.
    #[tokio::test]
    async fn a_full_queue_refuses_without_waiting() {
        use std::sync::atomic::Ordering;
        let tmp = tempfile::tempdir().unwrap();
        let state = test_state(tmp.path(), 1);
        let held = Arc::clone(&state.sql_gate).try_acquire_owned().unwrap();
        // This nest already has its cap parked, so there is nowhere for another to wait.
        state.sql_queued.store(SQL_MAX_QUEUED, Ordering::Relaxed);
        let started = std::time::Instant::now();
        let resp = sql(
            State(state.clone()),
            Query(SqlQuery {
                q: "SELECT 1".into(),
                max_rows: None,
            }),
        )
        .await
        .into_response();
        let took = started.elapsed();
        assert_eq!(resp.status(), StatusCode::SERVICE_UNAVAILABLE);
        assert!(
            took < Duration::from_millis(100),
            "refused after {took:?}: a full queue must not also serve out the wait"
        );
        drop(held);
    }

    // ---------------------------------------------------------------------------------------
    // `--cors` (#1318). Driven through the real `router()` with the layer applied exactly as
    // `bind_and_serve` applies it, not through a hand-built app: three fixtures in this file's
    // history proved a handler and never the wiring, and this is a flag whose entire content is
    // wiring.
    // ---------------------------------------------------------------------------------------

    /// Drive one request through `router()` with `origins` configured, returning the response
    /// headers - `None` for the `Origin` header means an ordinary same-origin-ish request.
    async fn cors_get(
        state: AppState,
        origins: &[&str],
        method: &str,
        origin: Option<&str>,
    ) -> (StatusCode, axum::http::HeaderMap) {
        use tower::ServiceExt;
        let owned: Vec<String> = origins.iter().map(|s| s.to_string()).collect();
        let app = match cors_layer(&owned).unwrap() {
            Some(layer) => router(SharedNest::new(state)).layer(layer),
            None => router(SharedNest::new(state)),
        };
        let mut req = axum::http::Request::builder().uri("/tables").method(method);
        if let Some(o) = origin {
            req = req.header("origin", o);
            if method == "OPTIONS" {
                req = req.header("access-control-request-method", "GET");
            }
        }
        let resp = app
            .oneshot(req.body(axum::body::Body::empty()).unwrap())
            .await
            .unwrap();
        (resp.status(), resp.headers().clone())
    }

    /// The deletion test the sprint rules ask of every optional feature: with the flag absent the
    /// binary is byte-for-byte the nest it was, and a self-hoster who never heard of `--cors` loses
    /// nothing and gains no header.
    #[tokio::test]
    async fn without_the_flag_a_nest_sends_no_cors_headers() {
        assert!(
            cors_layer(&[]).unwrap().is_none(),
            "no --cors must compose no layer at all, not a permissive one"
        );
        let tmp = tempfile::tempdir().unwrap();
        let (st, headers) = cors_get(
            test_state(tmp.path(), 2),
            &[],
            "GET",
            Some("https://app.example.com"),
        )
        .await;
        assert_eq!(st, StatusCode::OK);
        assert!(
            headers.get("access-control-allow-origin").is_none(),
            "a default nest answered a cross-origin request with {headers:?}"
        );
    }

    #[tokio::test]
    async fn a_wildcard_origin_is_allowed_for_any_caller() {
        let tmp = tempfile::tempdir().unwrap();
        let (st, headers) = cors_get(
            test_state(tmp.path(), 2),
            &["*"],
            "GET",
            Some("https://anything.example"),
        )
        .await;
        assert_eq!(st, StatusCode::OK);
        assert_eq!(
            headers.get("access-control-allow-origin").unwrap(),
            "*",
            "--cors '*' must allow the caller"
        );
    }

    /// A named origin is a real restriction, not decoration: the origin that was named gets the
    /// header and one that was not gets nothing, which is the only thing making `--cors` anything
    /// other than a synonym for `*`.
    #[tokio::test]
    async fn a_named_origin_allows_only_itself() {
        let tmp = tempfile::tempdir().unwrap();
        let allowed = "https://app.example.com";
        let (st, headers) =
            cors_get(test_state(tmp.path(), 2), &[allowed], "GET", Some(allowed)).await;
        assert_eq!(st, StatusCode::OK);
        assert_eq!(headers.get("access-control-allow-origin").unwrap(), allowed);

        let tmp2 = tempfile::tempdir().unwrap();
        let (st, headers) = cors_get(
            test_state(tmp2.path(), 2),
            &[allowed],
            "GET",
            Some("https://evil.example.com"),
        )
        .await;
        assert_eq!(st, StatusCode::OK);
        assert!(
            headers.get("access-control-allow-origin").is_none(),
            "an origin that was not named was allowed anyway: {headers:?}"
        );
    }

    /// A browser sends `OPTIONS` before the real call; if that is not answered the front end never
    /// makes the request at all, so the flag would look broken while every GET worked in curl.
    #[tokio::test]
    async fn a_preflight_is_answered() {
        let tmp = tempfile::tempdir().unwrap();
        let (st, headers) = cors_get(
            test_state(tmp.path(), 2),
            &["*"],
            "OPTIONS",
            Some("https://anything.example"),
        )
        .await;
        assert!(
            st.is_success(),
            "preflight answered {st}, so a browser would never send the GET"
        );
        assert_eq!(headers.get("access-control-allow-origin").unwrap(), "*");
        let methods = headers
            .get("access-control-allow-methods")
            .unwrap()
            .to_str()
            .unwrap()
            .to_ascii_uppercase();
        assert!(methods.contains("GET"), "preflight allowed {methods}");
    }

    /// Preflight `path` asking permission for `want_method`, exactly as a browser does before a
    /// cross-origin request it cannot send speculatively.
    async fn cors_preflight(
        state: AppState,
        origins: &[&str],
        path: &str,
        want_method: &str,
    ) -> (StatusCode, axum::http::HeaderMap) {
        use tower::ServiceExt;
        let owned: Vec<String> = origins.iter().map(|s| s.to_string()).collect();
        let app = match cors_layer(&owned).unwrap() {
            Some(layer) => router(SharedNest::new(state)).layer(layer),
            None => router(SharedNest::new(state)),
        };
        let req = axum::http::Request::builder()
            .uri(path)
            .method("OPTIONS")
            .header("origin", "https://app.example.com")
            .header("access-control-request-method", want_method)
            .header("access-control-request-headers", "content-type")
            .body(axum::body::Body::empty())
            .unwrap();
        let resp = app.oneshot(req).await.unwrap();
        (resp.status(), resp.headers().clone())
    }

    /// **The RFC-0053 GraphQL routes take a `POST` body**, which is how every GraphQL client on the
    /// web speaks, so a `GET`-only allow-list makes this flag useless for the surface a front end is
    /// most likely to want. The browser preflights `POST`, is refused, and never sends the query -
    /// and the failure looks like "CORS is broken" rather than "that method was not allowed".
    ///
    /// The first cut of this PR allowed `GET,OPTIONS` on the reasoning that the surface is
    /// read-only. It is read-only *and* it answers POST; those are not the same statement.
    #[tokio::test]
    async fn a_graphql_post_is_preflight_authorised() {
        for path in ["/graphql", "/subgraphs/id/QmAbc", "/subgraphs/name/a/b"] {
            let tmp = tempfile::tempdir().unwrap();
            let (st, headers) = cors_preflight(
                test_state(tmp.path(), 2),
                &["https://app.example.com"],
                path,
                "POST",
            )
            .await;
            assert!(st.is_success(), "{path} preflight answered {st}");
            assert_eq!(
                headers.get("access-control-allow-origin").unwrap(),
                "https://app.example.com",
                "{path}"
            );
            let methods = headers
                .get("access-control-allow-methods")
                .unwrap()
                .to_str()
                .unwrap()
                .to_ascii_uppercase();
            assert!(
                methods.contains("POST"),
                "{path} preflight allowed {methods}, so a browser GraphQL client never sends the \
                 query"
            );
            // The body carries JSON, so the header that names it must survive the preflight too.
            let allowed = headers
                .get("access-control-allow-headers")
                .map(|v| v.to_str().unwrap().to_ascii_lowercase())
                .unwrap_or_default();
            assert!(
                allowed.contains("content-type"),
                "{path} refused content-type: {allowed}"
            );
        }
    }

    /// Every one of these refusals exists because the failure it prevents is *silent*: the browser
    /// reports a generic CORS error and the operator cannot tell a wrong value from a broken flag.
    #[test]
    fn an_origin_that_could_never_match_is_refused_at_startup() {
        let err = |v: &[&str]| {
            cors_layer(&v.iter().map(|s| s.to_string()).collect::<Vec<_>>())
                .expect_err("should have been refused")
                .to_string()
        };
        // Each pair is (value, the word the message must carry) - the message has to name what is
        // wrong, or the operator is back to guessing which of their origins the browser dislikes.
        for (value, word) in [
            ("app.example.com", "needs a scheme"),
            ("ftp://app.example.com", "needs a scheme"),
            ("https://app.example.com/", "trailing slash"),
            // Jules on #1384: these passed a check that looked only at the scheme and the last
            // character, then matched nothing at request time.
            ("https://app.example.com/path", "path"),
            ("https://app.example.com?x=1", "query"),
            ("https://app.example.com#frag", "fragment"),
            ("https://user:pw@app.example.com", "credentials"),
            ("https://", "names no host"),
            ("http://[]", "names no host"),
            ("http://[::1", "never closes it"),
            ("http://[::1]x", "after the host"),
            ("http://[::1]:x", "port number"),
            // Jules, third round: a denylist of bad characters stays one example behind. These are
            // the ones that got through before the alphabet was stated as an allowlist.
            ("https://app example.com", "a space"),
            ("https://app\texample.com", "in its host"),
            ("https://app|example.com", "in its host"),
            ("https://app,example.com", "in its host"),
            // Jules, fifth round: the bracketed alphabet had an `is_ascii_alphanumeric` term meant
            // to admit a zone id, which admitted every letter and made the hex check vacuous.
            ("http://[zz]", "in its host"),
            ("http://[2001:db8::gg]", "in its host"),
            ("http://[2001:db8::1%]", "empty zone id"),
            ("http://[2001:db8::1%et h0]", "a space"),
            ("https://exam%ple.com", "'%'"),
            ("https://app.example.com:", "port number"),
            ("https://app.example.com:http", "port number"),
        ] {
            let got = err(&[value]);
            assert!(got.contains(word), "{value} was refused as: {got}");
        }
        let mixed = err(&["*", "https://app.example.com"]);
        assert!(mixed.contains("cannot be combined"), "{mixed}");
        // And the values that are fine stay fine, or the guard above is just a wall.
        for ok in [
            vec!["*"],
            vec!["http://localhost:3000"],
            vec!["https://a.example.com", "https://b.example.com"],
            vec!["http://127.0.0.1:8288"],
            vec!["https://sub.domain.example.com:8443"],
            // IPv6, bracketed, with and without a port - the binary takes IPv6 listen addresses, so
            // a front end on one is not an exotic case.
            vec!["http://[::1]:3000"],
            vec!["http://[::1]"],
            vec!["https://[2001:db8::1]:8443"],
            vec!["http://[::ffff:192.168.0.1]"],
            vec!["http://[fe80::1%eth0]:3000"],
        ] {
            let owned: Vec<String> = ok.iter().map(|s| s.to_string()).collect();
            assert!(
                cors_layer(&owned).unwrap().is_some(),
                "{ok:?} should have been accepted"
            );
        }
    }

    /// A multichain runtime runs one cursor per chain, so `/<nest>/ready` must answer from **that
    /// nest's** counters.
    ///
    /// Found by the RFC-0021 live two-chain run rather than by any test: a mainnet nest reported
    /// `tip: 488677305` - an Arbitrum height - while mainnet was at 25,632,906, next to a mainnet
    /// `sealed_through`. One body, two chains, and nothing in it to tell an operator which was which.
    /// Whichever cursor polled last simply overwrote the shared gauge.
    #[tokio::test]
    async fn per_nest_readiness_does_not_report_another_chains_tip() {
        use crate::metrics::METRICS;
        let tmp = tempfile::tempdir().unwrap();
        let health = Arc::new(crate::health::RuntimeHealth::new());
        health.register("on-mainnet", "mainnet");
        health.register("on-arbitrum", "arbitrum-one");

        // Two cursors publishing wildly different heights, as two real chains do.
        let eth = METRICS.nest("on-mainnet");
        let arb = METRICS.nest("on-arbitrum");
        eth.set_last_block(25_632_840);
        eth.set_tip(25_632_906);
        eth.mark_poll_ok();
        // Arbitrum polls *after* mainnet, so it is the one that owns the global gauge.
        arb.set_last_block(488_677_300);
        arb.set_tip(488_677_305);
        arb.mark_poll_ok();

        let mut state = test_state(tmp.path(), 1);
        state.runtime_health = Some(("on-mainnet".to_string(), health.clone()));
        let body = ready(State(state)).await.into_response();
        let bytes = axum::body::to_bytes(body.into_body(), usize::MAX)
            .await
            .unwrap();
        let v: serde_json::Value = serde_json::from_slice(&bytes).unwrap();

        assert_eq!(v["version"], json!(env!("CARGO_PKG_VERSION")));
        assert_eq!(
            v["tip"].as_u64(),
            Some(25_632_906),
            "the mainnet nest must report mainnet's tip, not whichever cursor polled last: {v}"
        );
        assert_eq!(v["last_block"].as_u64(), Some(25_632_840));
        assert_eq!(
            v["lag_blocks"].as_u64(),
            Some(66),
            "lag computed across two chains is meaningless - it was ~463 million before this fix"
        );
    }

    fn named_surface(sql: &str) -> Arc<crate::allowlist::Surface> {
        Arc::new(crate::allowlist::Surface {
            access: crate::allowlist::SqlAccess::Allowlist,
            queries: vec![crate::allowlist::NamedQuery {
                name: "answer".into(),
                sql: sql.into(),
                params: Default::default(),
            }],
        })
    }

    fn named_request(headers: &[(&str, &str)]) -> axum::http::Request<axum::body::Body> {
        let mut request = axum::http::Request::builder().uri("/q/answer");
        for (name, value) in headers {
            request = request.header(*name, *value);
        }
        request.body(axum::body::Body::empty()).unwrap()
    }

    async fn call_named(
        app: &axum::Router,
        request: axum::http::Request<axum::body::Body>,
    ) -> (StatusCode, axum::http::HeaderMap, serde_json::Value) {
        use tower::ServiceExt;
        let response = app.clone().oneshot(request).await.unwrap();
        let status = response.status();
        let headers = response.headers().clone();
        let body = axum::body::to_bytes(response.into_body(), usize::MAX)
            .await
            .unwrap();
        (
            status,
            headers,
            serde_json::from_slice(&body).unwrap_or(serde_json::Value::Null),
        )
    }

    #[tokio::test]
    async fn a_named_query_over_its_scan_budget_is_refused_whole() {
        let tmp = tempfile::tempdir().unwrap();
        let mut state = test_state(tmp.path(), SQL_MAX_CONCURRENCY);
        state.sql_max_named_scan_bytes = 16;
        state
            .store
            .put_entity(
                "k1",
                r#"{"table":"t","block_number":1,"payload":"too wide"}"#,
            )
            .unwrap();
        // Warm the ordinary-query memo with the same statement. A hit must not evade admission.
        let warm = run_sql_query(state.clone(), "SELECT 1 AS answer".into(), None).await;
        assert_eq!(warm.status(), StatusCode::OK);
        state.surface = named_surface("SELECT 1 AS answer");
        let app = router(SharedNest::new(state));
        let (status, _, body) = call_named(&app, named_request(&[])).await;
        assert_eq!(status, StatusCode::UNPROCESSABLE_ENTITY, "{body}");
        assert!(
            body["error"].as_str().unwrap().contains("scan budget"),
            "{body}"
        );
        assert_eq!(body["budget"], 16, "{body}");
        assert!(body.get("rows").is_none(), "{body}");
    }

    #[tokio::test]
    async fn a_named_query_that_cannot_be_bounded_is_refused_before_it_runs() {
        let tmp = tempfile::tempdir().unwrap();
        let mut state = test_state(tmp.path(), SQL_MAX_CONCURRENCY);
        for block in 1..=3 {
            state
                .store
                .put_entity(
                    &format!("{block:012}-000000"),
                    &format!(r#"{{"table":"t","block_number":{block}}}"#),
                )
                .unwrap();
        }
        state.surface = named_surface(
            "SELECT error('evaluated') FROM t a WHERE a.block_number = \
             (SELECT max(b.block_number) FROM t b WHERE b.block_number < a.block_number)",
        );
        let app = router(SharedNest::new(state));
        let (status, _, body) = call_named(&app, named_request(&[])).await;
        assert_eq!(status, StatusCode::UNPROCESSABLE_ENTITY, "{body}");
        let error = body["error"].as_str().unwrap();
        assert!(
            (error.contains("DELIM") || error.contains("NestedLoopJoinExec"))
                && !error.contains("evaluated"),
            "{body}"
        );
    }

    #[tokio::test]
    async fn maintained_relation_rows_spend_the_named_scan_budget() {
        use crate::entity_expr::Expr;
        use crate::entity_plan::{Agg, Plan, Source};
        let tmp = tempfile::tempdir().unwrap();
        let mut state = test_state(tmp.path(), SQL_MAX_CONCURRENCY);
        let abi: alloy_json_abi::JsonAbi = serde_json::from_str(
            r#"[{"type":"event","name":"Transfer","inputs":[
                {"name":"from","type":"address","indexed":true},
                {"name":"to","type":"address","indexed":true},
                {"name":"value","type":"uint256","indexed":false}],"anonymous":false}]"#,
        )
        .unwrap();
        let reg = crate::registry::DecodeRegistry::build(vec![crate::registry::ContractSpec {
            alias: "usdc".into(),
            address: "0xa0b86991c6218b36c1d19d4a2e9eb0ce3606eb48"
                .parse()
                .unwrap(),
            abi,
            events: Vec::new(),
        }])
        .unwrap();
        let plan = Plan {
            left: Source {
                table: "usdc__transfer".into(),
                columns: vec!["to".into(), "value".into()],
            },
            left_filter: None,
            join: None,
            key: vec![Expr::Column(0)],
            aggregates: vec![Agg::Sum(Expr::Column(1))],
        };
        let columns = vec!["to".to_string(), "sum_value".to_string()];
        let entity =
            crate::entity_view::EntityView::start("e", &plan, &columns, &reg, 1_000, false)
                .unwrap();
        let row = |to: &str, v: i128| {
            crate::entity_row::Row(vec![
                crate::entity_row::Scalar::Str(to.into()),
                crate::entity_row::Scalar::Int(v),
            ])
        };
        entity.apply(
            crate::entity_view::Batch {
                left: vec![(row("0xa", 1), 1), (row("0xb", 2), 1)],
                right: Vec::new(),
            },
            10,
        );
        entity.flush();
        let (rows, _) = entity.rows_as_json_with_watermark();
        assert_eq!(rows.len(), 2);
        let bytes: u64 = rows.iter().map(json_len).sum();
        state.entities = Arc::new(vec![entity]);
        state.surface = named_surface("SELECT count(*) AS n FROM e");

        state.sql_max_named_scan_bytes = bytes;
        let app = router(SharedNest::new(state.clone()));
        let (status, _, body) = call_named(&app, named_request(&[])).await;
        assert_eq!(status, StatusCode::OK, "{body}");
        assert_eq!(body["rows"][0]["n"], 2, "{body}");

        state.sql_max_named_scan_bytes = bytes - 1;
        let app = router(SharedNest::new(state.clone()));
        let (status, _, body) = call_named(&app, named_request(&[])).await;
        assert_eq!(status, StatusCode::UNPROCESSABLE_ENTITY, "{body}");
        assert_eq!(body["hot_source_bytes"], bytes, "{body}");

        // They count against the hot-row cap the tip copy respects, too.
        state.sql_max_named_scan_bytes = bytes;
        state.sql_max_hot_rows = 1;
        let app = router(SharedNest::new(state));
        let (status, _, body) = call_named(&app, named_request(&[])).await;
        assert_eq!(status, StatusCode::SERVICE_UNAVAILABLE, "{body}");
    }

    #[cfg(feature = "counter")]
    #[tokio::test]
    async fn an_unpaid_named_query_is_quoted_with_the_bound_it_planned_to() {
        use base64::Engine;
        let dir = tempfile::tempdir().unwrap();
        let mut state = test_state(dir.path(), SQL_MAX_CONCURRENCY);
        let row = r#"{"table":"t","block_number":1,"v":"1"}"#;
        state.store.put_entity("k1", row).unwrap();
        state.surface = named_surface("SELECT count(*) AS n FROM t");
        let (config, _) = crate::counter::x402::tests::payment_for_http_test();
        let price = config.price.clone();
        state.counter = Some(Arc::new(config));
        let app = router(SharedNest::new(state));

        let (status, headers, body) = call_named(&app, named_request(&[])).await;
        assert_eq!(status, StatusCode::PAYMENT_REQUIRED, "{body}");
        let quote = &body["quote"];
        assert_eq!(quote["id"], "none.0", "{body}");
        assert_eq!(quote["tier"], "flat");
        assert_eq!(quote["ceiling"]["amount"], price.as_str());
        assert_eq!(quote["bound"]["hot_bytes"], row.len() as u64, "{body}");
        assert_eq!(quote["bound"]["cold_bytes"], 0, "{body}");
        assert_eq!(quote["snapshot"]["hot_rows"], 1, "{body}");
        assert_eq!(quote["snapshot"]["catalogue_hash"], serde_json::Value::Null);
        assert_eq!(quote["admission_cap_bytes"], SQL_MAX_NAMED_SCAN_BYTES);
        let header = headers["payment-required"].to_str().unwrap();
        let challenge: serde_json::Value = serde_json::from_slice(
            &base64::engine::general_purpose::STANDARD
                .decode(header)
                .unwrap(),
        )
        .unwrap();
        assert_eq!(challenge["accepts"][0]["extra"]["nuthatchQuote"], "none.0");
        assert!(!dir.path().join("authorisations.jsonl").exists());
    }

    #[cfg(feature = "counter")]
    #[tokio::test]
    async fn a_paid_named_query_is_held_to_the_quote_it_names() {
        let dir = tempfile::tempdir().unwrap();
        let mut state = test_state(dir.path(), SQL_MAX_CONCURRENCY);
        state
            .store
            .put_entity("k1", r#"{"table":"t","block_number":1}"#)
            .unwrap();
        state.surface = named_surface("SELECT count(*) AS n FROM t");
        let (config, payment) = crate::counter::x402::tests::payment_for_http_test();
        state.counter = Some(Arc::new(config));
        let app = router(SharedNest::new(state));
        let log = dir.path().join("authorisations.jsonl");
        let quoted = |id: &str| {
            named_request(&[
                ("Payment-Signature", payment.as_str()),
                (crate::counter::QUOTE_HEADER, id),
            ])
        };

        let (status, _, body) = call_named(&app, quoted("none.7")).await;
        assert_eq!(status, StatusCode::CONFLICT, "{body}");
        assert_eq!(
            body["quote"]["id"], "none.0",
            "a stale quote is answered with a fresh one"
        );
        assert!(!log.exists(), "a refused answer records nothing");

        let (status, _, body) = call_named(&app, quoted("not-a-quote")).await;
        assert_eq!(status, StatusCode::BAD_REQUEST, "{body}");
        assert!(!log.exists());

        let (status, _, body) = call_named(&app, quoted("none.0")).await;
        assert_eq!(status, StatusCode::OK, "{body}");
        assert_eq!(body["rows"][0]["n"], 1, "{body}");
        assert_eq!(std::fs::read_to_string(&log).unwrap().lines().count(), 1);

        // Staleness outranks the budget: a tip too wide to copy still answers 409, not 422.
        let tight = tempfile::tempdir().unwrap();
        let mut state = test_state(tight.path(), SQL_MAX_CONCURRENCY);
        state
            .store
            .put_entity("k1", r#"{"table":"t","block_number":1}"#)
            .unwrap();
        state.surface = named_surface("SELECT count(*) AS n FROM t");
        state.sql_max_named_scan_bytes = 1;
        let (config, _) = crate::counter::x402::tests::payment_for_http_test();
        state.counter = Some(Arc::new(config));
        let app = router(SharedNest::new(state));
        let (status, _, body) = call_named(&app, quoted("none.7")).await;
        assert_eq!(status, StatusCode::CONFLICT, "{body}");
    }

    #[cfg(feature = "counter")]
    #[tokio::test]
    async fn refused_paid_query_keeps_its_authorisation_for_a_successful_retry() {
        use tower::ServiceExt;
        let dir = tempfile::tempdir().unwrap();
        let mut state = test_state(dir.path(), SQL_MAX_CONCURRENCY);
        state.sql_max_named_scan_bytes = 1;
        state
            .store
            .put_entity("k1", r#"{"table":"t","block_number":1}"#)
            .unwrap();
        state.surface = named_surface("SELECT 1 AS answer");
        let (config, payment) = crate::counter::x402::tests::payment_for_http_test();
        state.counter = Some(Arc::new(config));
        let request = || named_request(&[("Payment-Signature", payment.as_str())]);
        let refused = router(SharedNest::new(state.clone()))
            .oneshot(request())
            .await
            .unwrap();
        assert_eq!(refused.status(), StatusCode::UNPROCESSABLE_ENTITY);
        assert!(!dir.path().join("authorisations.jsonl").exists());
        state.sql_max_named_scan_bytes = SQL_MAX_NAMED_SCAN_BYTES;
        let app = router(SharedNest::new(state));
        let (first, second) = tokio::join!(
            app.clone().oneshot(request()),
            app.clone().oneshot(request())
        );
        let statuses = [first.unwrap().status(), second.unwrap().status()];
        assert_eq!(statuses.iter().filter(|&&s| s == StatusCode::OK).count(), 1);
        assert_eq!(
            statuses
                .iter()
                .filter(|&&s| s == StatusCode::PAYMENT_REQUIRED)
                .count(),
            1
        );
        let replay = app.oneshot(request()).await.unwrap();
        assert_eq!(replay.status(), StatusCode::PAYMENT_REQUIRED);
        assert_eq!(
            std::fs::read_to_string(dir.path().join("authorisations.jsonl"))
                .unwrap()
                .lines()
                .count(),
            1
        );
    }

    /// The `/sql` RAM guard (the "node owns resource safety" half of the CLAUDE.md division of
    /// labour): the unsealed tip is materialised per query, so an unbounded one is the largest RAM
    /// risk the process carries - and in a runtime it is a co-tenant's problem too.
    ///
    /// It must **fail**, not truncate. Serving a partial tip would silently change the answer to an
    /// aggregate, and a `count(*)` quietly missing rows is far worse than a query that refuses.
    #[tokio::test]
    async fn an_oversized_hot_tip_is_refused_rather_than_partially_served() {
        let tmp = tempfile::tempdir().unwrap();
        let state = test_state(tmp.path(), SQL_MAX_CONCURRENCY);
        // Three rows in the hot store, then a cap of two.
        for b in 1..=3u64 {
            state
                .store
                .put_entity(
                    &format!("k{b}"),
                    &json!({"table": "t", "block_number": b}).to_string(),
                )
                .unwrap();
        }
        let err = state
            .store
            .hot_rows_by_table_bounded(2)
            .expect_err("a tip over the cap must refuse");
        assert!(
            err.downcast_ref::<crate::store::HotScanTooLarge>().is_some(),
            "the refusal must be typed so the handler can map it to 503 without matching prose: {err:#}"
        );
        // Under the cap it behaves exactly as the unbounded scan does.
        let ok = state
            .store
            .hot_rows_by_table_bounded(3)
            .expect("at the cap");
        assert_eq!(ok.get("t").map(|v| v.len()), Some(3));
    }

    /// No request handler may reach the tip through the unbounded scan (#293).
    ///
    /// `/sql` has always been bounded; `/explain` was not, and because it returns no rows
    /// the gap was invisible from the outside while still parsing
    /// the whole tip into temp tables. The cap itself is 2,000,000 rows, far too many to
    /// materialise in a test, so the invariant is checked where it is actually expressible: no
    /// handler in this file calls the unbounded variant at all.
    ///
    /// `main.rs` (the local REPL backend) and `bench.rs` legitimately still use it — neither is
    /// reachable over the network, which is the whole distinction this guard draws.
    #[test]
    fn no_request_path_uses_the_unbounded_hot_scan() {
        let src = include_str!("serve.rs");
        // Split so the needle never appears verbatim in this file - otherwise the scan matches
        // the line it is written on and the test fails against itself.
        let needle = concat!("hot_rows_by_table", "()");
        let offenders: Vec<usize> = src
            .lines()
            .enumerate()
            .filter(|(_, l)| l.contains(needle) && !l.trim_start().starts_with("//"))
            .map(|(i, _)| i + 1)
            .collect();
        assert!(
            offenders.is_empty(),
            "serve.rs serves HTTP, so every hot scan here must be bounded by SQL_MAX_HOT_ROWS; \
             unbounded call(s) at line(s) {offenders:?}. Use hot_rows_by_table_bounded and map \
             HotScanTooLarge to a 503, as /sql and /explain do."
        );
    }

    /// An over-length query string is rejected (400) before it ever reaches the planner.
    #[tokio::test]
    async fn sql_rejects_overlong_query() {
        let tmp = tempfile::tempdir().unwrap();
        let state = test_state(tmp.path(), SQL_MAX_CONCURRENCY);
        let long = format!("SELECT {}", "1,".repeat(SQL_MAX_QUERY_LEN)); // well past the cap
        let resp = sql(
            State(state),
            Query(SqlQuery {
                q: long,
                max_rows: None,
            }),
        )
        .await
        .into_response();
        assert_eq!(resp.status(), StatusCode::BAD_REQUEST);
    }

    /// A well-formed query passes the gate and runs (200) - the guard doesn't block legitimate use.
    #[tokio::test]
    async fn sql_serves_a_normal_query() {
        let tmp = tempfile::tempdir().unwrap();
        let state = test_state(tmp.path(), SQL_MAX_CONCURRENCY);
        let resp = sql(
            State(state),
            Query(SqlQuery {
                q: "SELECT 1 AS n".into(),
                max_rows: None,
            }),
        )
        .await
        .into_response();
        assert_eq!(resp.status(), StatusCode::OK);
    }

    /// #1609: the row objects cannot carry the projection order, so the response names it.
    #[tokio::test]
    async fn sql_response_names_its_columns_in_the_querys_order() {
        let tmp = tempfile::tempdir().unwrap();
        let state = test_state(tmp.path(), SQL_MAX_CONCURRENCY);
        let (status, body) = get(
            router(SharedNest::new(state)),
            "/sql?q=SELECT%201%20AS%20z,%202%20AS%20a,%203%20AS%20m",
        )
        .await;
        assert_eq!(status, StatusCode::OK);
        let body: Value = serde_json::from_slice(&body).unwrap();
        assert_eq!(body["columns"], json!(["z", "a", "m"]), "{body}");
        assert_eq!(body["rows"], json!([{"z": 1, "a": 2, "m": 3}]), "{body}");
    }

    /// A typed-rows QoS nest held 850,879 rows hot behind outstanding documents, under the two-million
    /// row cap, and one `/sql` parsed all of them: 3.36 GB. The byte cap refuses before parsing past it,
    /// on both handlers, and says how much was there.
    #[tokio::test]
    async fn a_hot_tip_over_the_byte_cap_is_refused_by_sql_and_explain() {
        let tmp = tempfile::tempdir().unwrap();
        let mut state = test_state(tmp.path(), SQL_MAX_CONCURRENCY);
        state.sql_max_hot_bytes = 64;
        for b in 1..=3u64 {
            state
                .store
                .put_entity(
                    &format!("k{b}"),
                    &json!({"table": "t", "block_number": b, "pad": "x".repeat(40)}).to_string(),
                )
                .unwrap();
        }
        let q = || {
            Query(SqlQuery {
                q: "SELECT 1 AS n".into(),
                max_rows: None,
            })
        };
        for (name, resp) in [
            ("/sql", sql(State(state.clone()), q()).await.into_response()),
            (
                "/explain",
                explain(State(state.clone()), q()).await.into_response(),
            ),
        ] {
            assert_eq!(resp.status(), StatusCode::SERVICE_UNAVAILABLE, "{name}");
            let body = axum::body::to_bytes(resp.into_body(), usize::MAX)
                .await
                .unwrap();
            let v: Value = serde_json::from_slice(&body).unwrap();
            assert_eq!(v["budget"], json!(64), "{name} body: {v}");
            assert!(
                v["hot_source_bytes"].as_u64().unwrap() > 64,
                "{name} body: {v}"
            );
        }
    }

    /// #378: an over-budget tip is refused by both `/sql` and `/explain`, not just by the store layer
    /// underneath them. Neither handler was ever driven directly by a test before this - the typed
    /// error (`HotScanTooLarge`) and the call site (`no_request_path_uses_the_unbounded_hot_scan`)
    /// were each covered, but the mapping between them - `503`, this exact body shape - was not, so
    /// deleting either handler's refusal arm left the suite green.
    ///
    /// `SQL_MAX_HOT_ROWS` is `2_000_000`; reaching the refusal for real would mean putting two
    /// million rows in the hot store. `AppState::sql_max_hot_rows` exists so this test can lower the
    /// seam instead, exactly as production leaves it at the real constant (`test_state`, `indexer.rs`).
    #[tokio::test]
    async fn a_hot_tip_over_the_configured_cap_is_refused_by_sql_and_explain() {
        let tmp = tempfile::tempdir().unwrap();
        let mut state = test_state(tmp.path(), SQL_MAX_CONCURRENCY);
        state.sql_max_hot_rows = 2;
        for b in 1..=3u64 {
            state
                .store
                .put_entity(
                    &format!("k{b}"),
                    &json!({"table": "t", "block_number": b}).to_string(),
                )
                .unwrap();
        }
        let q = || {
            Query(SqlQuery {
                q: "SELECT 1 AS n".into(),
                max_rows: None,
            })
        };

        let resp = sql(State(state.clone()), q()).await.into_response();
        assert_eq!(resp.status(), StatusCode::SERVICE_UNAVAILABLE, "/sql");
        let body = axum::body::to_bytes(resp.into_body(), usize::MAX)
            .await
            .unwrap();
        let v: Value = serde_json::from_slice(&body).unwrap();
        assert!(
            v["error"]
                .as_str()
                .unwrap()
                .contains("will not materialise"),
            "/sql body: {v}"
        );
        assert_eq!(v["sealed_through"], json!(0));

        let resp = explain(State(state), q()).await.into_response();
        assert_eq!(resp.status(), StatusCode::SERVICE_UNAVAILABLE, "/explain");
        let body = axum::body::to_bytes(resp.into_body(), usize::MAX)
            .await
            .unwrap();
        let v: Value = serde_json::from_slice(&body).unwrap();
        assert!(
            v["error"]
                .as_str()
                .unwrap()
                .contains("will not materialise"),
            "/explain body: {v}"
        );
        assert_eq!(v["sealed_through"], json!(0));
        // `valid` is deliberately absent, not `false` - bindability is unknown, not disproved.
        assert!(
            v.get("valid").is_none(),
            "/explain must omit `valid` on refusal: {v}"
        );
    }

    /// #1775: `/explain` plans the statement `/sql` runs. It used to wrap it in a derived table,
    /// where an ORDER BY on a column its own alias shadows binds as ambiguous: the allocations
    /// nest's `indexer.delegators_page`, reduced.
    #[tokio::test]
    async fn explain_plans_what_sql_answers() {
        let tmp = tempfile::tempdir().unwrap();
        let state = test_state(tmp.path(), SQL_MAX_CONCURRENCY);
        state
            .store
            .put_entity(
                "k1",
                &json!({"table": "t", "block_number": 1, "who": "a", "n": "5"}).to_string(),
            )
            .unwrap();
        let statement = "SELECT p.who, CAST(p.n AS VARCHAR) AS n FROM t p ORDER BY p.n DESC, p.who";
        let q = || {
            Query(SqlQuery {
                q: statement.into(),
                max_rows: None,
            })
        };
        // What /sql runs, called below the handler: its memo is process-global, and an answer left
        // there breaks a neighbour that counts the entries.
        let hot = state.store.hot_rows_by_table_bounded(10).unwrap();
        let guard = crate::analytics::QueryGuard {
            timeout: SQL_TIMEOUT,
            max_rows: SQL_MAX_ROWS,
        };
        crate::analytics::query_hot_cold(tmp.path(), statement, guard, &hot, 0, &[])
            .expect("premise: /sql answers it");

        let resp = explain(State(state.clone()), q()).await.into_response();
        let status = resp.status();
        let body = axum::body::to_bytes(resp.into_body(), usize::MAX)
            .await
            .unwrap();
        let v: Value = serde_json::from_slice(&body).unwrap();
        assert_eq!(status, StatusCode::OK, "/explain body: {v}");
        assert_eq!(v["valid"], json!(true), "{v}");

        // Planned, not run: a statement that binds and fails only on its rows is valid.
        let dies = || {
            Query(SqlQuery {
                q: "SELECT CAST(who AS INTEGER) AS n FROM t".into(),
                max_rows: None,
            })
        };
        let resp = sql(State(state.clone()), dies()).await.into_response();
        assert_eq!(
            resp.status(),
            StatusCode::BAD_REQUEST,
            "premise: it dies running"
        );
        let resp = explain(State(state), dies()).await.into_response();
        assert_eq!(resp.status(), StatusCode::OK, "/explain ran the statement");
    }

    /// RFC-0010 Part A: the admin UI serves when enabled and 404s when disabled (`--no-admin` or a
    /// public bind without a token). `/nest` returns the static nest metadata either way.
    #[tokio::test]
    async fn admin_ui_gated_and_nest_metadata() {
        let tmp = tempfile::tempdir().unwrap();
        let mut state = test_state(tmp.path(), SQL_MAX_CONCURRENCY);
        let no_tok = || Query(AdminQuery { token: None });
        let no_hdr = || {
            let mut h = axum::http::HeaderMap::new();
            h.insert(axum::http::header::HOST, "127.0.0.1:8288".parse().unwrap());
            h
        };

        // Localhost (admin_token None): open.
        let resp = admin_index(State(state.clone()), no_tok(), no_hdr())
            .await
            .into_response();
        assert_eq!(resp.status(), StatusCode::OK, "admin UI open on localhost");

        state.admin_enabled = false;
        let resp = admin_index(State(state.clone()), no_tok(), no_hdr())
            .await
            .into_response();
        assert_eq!(
            resp.status(),
            StatusCode::NOT_FOUND,
            "admin UI 404s when disabled"
        );

        let resp = nest(State(state)).await.into_response();
        assert_eq!(resp.status(), StatusCode::OK);
    }

    #[test]
    fn config_text_is_shown_without_its_credentials() {
        let text = concat!(
            "rpc_urls = [\"https://arb-mainnet.g.alchemy.com/v2/KEYKEYKEY\"] # see https://u:p@docs.example/x?k=1\n",
            "url = \"https://hooks.slack.com/services/T00/B00/XXXX\"\n",
            "secret = \"hunter2\"\n",
            "api_key=\"abc\"\n",
            "key = [\"indexer\"]\n",
            "plain = \"https://rpc.example\"\n",
        );
        assert_eq!(
            redact_config_text(text),
            concat!(
                "rpc_urls = [\"https://arb-mainnet.g.alchemy.com/…\"] # see https://docs.example/…\n",
                "url = \"https://hooks.slack.com/…\"\n",
                "secret = \"<redacted>\"\n",
                "api_key= \"<redacted>\"\n",
                "key = [\"indexer\"]\n",
                "plain = \"https://rpc.example\"\n",
            )
        );
    }

    #[tokio::test]
    async fn admin_config_and_storage_are_gated_and_report_the_nest() {
        let tmp = tempfile::tempdir().unwrap();
        std::fs::write(
            tmp.path().join("nuthatch.toml"),
            "rpc_urls = [\"https://rpc.example/v2/KEYKEYKEY\"]\n",
        )
        .unwrap();
        std::fs::create_dir_all(tmp.path().join("views")).unwrap();
        std::fs::write(tmp.path().join("views/10-a.sql"), "SELECT 1").unwrap();
        let segments = tmp.path().join(crate::seal::SEGMENTS_DIR);
        std::fs::create_dir_all(&segments).unwrap();
        std::fs::write(segments.join("a.parquet"), [0u8; 7]).unwrap();
        let seg = |file: &str, from: u64, to: u64, rows: usize| json!({ "hash": file.trim_end_matches(".parquet"), "from_block": from, "to_block": to, "rows": rows, "file": file });
        std::fs::write(
            segments.join(crate::seal::MANIFEST_FILE),
            json!({ "tables": { "t__e": [seg("a.parquet", 5, 9, 3), seg("gone.parquet", 10, 12, 4)] } })
                .to_string(),
        )
        .unwrap();

        let mut state = test_state(tmp.path(), SQL_MAX_CONCURRENCY);
        state.admin_token = Some("s3cret".into());
        let body = |resp: axum::response::Response| async move {
            let bytes = axum::body::to_bytes(resp.into_body(), usize::MAX)
                .await
                .unwrap();
            serde_json::from_slice::<Value>(&bytes).unwrap()
        };
        let config = |s: AppState, token: Option<&str>| {
            let q = Query(AdminQuery {
                token: token.map(str::to_string),
            });
            async move {
                admin_config(State(s), q, axum::http::HeaderMap::new())
                    .await
                    .into_response()
            }
        };
        let storage = |s: AppState, token: Option<&str>, table: Option<&str>| {
            let q = Query(AdminStorageQuery {
                token: token.map(str::to_string),
                table: table.map(str::to_string),
            });
            async move {
                admin_storage(State(s), q, axum::http::HeaderMap::new())
                    .await
                    .into_response()
            }
        };

        assert_eq!(
            config(state.clone(), None).await.status(),
            StatusCode::UNAUTHORIZED
        );
        assert_eq!(
            storage(state.clone(), None, None).await.status(),
            StatusCode::UNAUTHORIZED
        );

        let v = body(config(state.clone(), Some("s3cret")).await).await;
        let files = v["files"].as_array().unwrap();
        let names: Vec<&str> = files.iter().map(|f| f["name"].as_str().unwrap()).collect();
        assert_eq!(names, ["nuthatch.toml", "views/10-a.sql"]);
        assert_eq!(files[0]["text"], "rpc_urls = [\"https://rpc.example/…\"]\n");
        assert_eq!(v["running"]["sql"]["max_rows"], SQL_MAX_ROWS);

        let v = body(storage(state.clone(), Some("s3cret"), Some("t__e")).await).await;
        assert_eq!(
            v["tables"],
            json!([{ "table": "t__e", "segments": 2, "provisional": 0, "rows": 7, "bytes": 7,
                     "missing_files": 1, "from_block": 5, "to_block": 12 }])
        );
        let detail = v["detail"]["segments"].as_array().unwrap();
        assert_eq!(detail[0]["hash"], "gone", "newest first");
        assert_eq!(detail[0]["bytes"], Value::Null);
        assert_eq!(detail[1]["bytes"], 7);

        state.admin_enabled = false;
        assert_eq!(
            config(state.clone(), Some("s3cret")).await.status(),
            StatusCode::NOT_FOUND
        );
        assert_eq!(
            storage(state, Some("s3cret"), None).await.status(),
            StatusCode::NOT_FOUND
        );
    }

    /// #2136: with no token the surface is open only to requests addressed to loopback. The lifecycle
    /// routes and the control plane share `token_ok`, so this is their gate too.
    #[test]
    fn an_open_surface_checks_host_and_origin() {
        let hdr = |pairs: &[(axum::http::HeaderName, &str)]| {
            let mut h = axum::http::HeaderMap::new();
            for (k, v) in pairs {
                h.insert(k.clone(), axum::http::HeaderValue::from_str(v).unwrap());
            }
            h
        };
        use axum::http::header::{HOST, ORIGIN};
        for host in [
            "127.0.0.1:8288",
            "localhost:8288",
            "LOCALHOST",
            "[::1]:8288",
            "127.0.0.2",
        ] {
            assert!(
                token_ok(None, None, &hdr(&[(HOST, host)])),
                "{host} is loopback"
            );
        }
        assert!(
            !token_ok(None, None, &hdr(&[])),
            "no Host and no authority names no target, so it is refused"
        );
        let h2 = host_from_authority(
            axum::http::Request::get("http://attacker.example:8288/_admin/")
                .body(())
                .unwrap(),
        );
        assert!(
            !token_ok(None, None, h2.headers()),
            "an HTTP/2 or absolute-form target must be checked like a Host"
        );
        let mut abs = axum::http::Request::get("http://attacker.example/")
            .body(())
            .unwrap();
        abs.headers_mut()
            .insert(HOST, axum::http::HeaderValue::from_static("127.0.0.1:8288"));
        assert!(
            !token_ok(None, None, host_from_authority(abs).headers()),
            "the request target overrides a loopback Host"
        );
        assert!(token_ok(
            None,
            None,
            &hdr(&[(HOST, "127.0.0.1:8288"), (ORIGIN, "http://localhost:8288")])
        ));
        for host in [
            "attacker.example:8288",
            "localhost.attacker.example",
            "10.0.0.1:8288",
        ] {
            assert!(
                !token_ok(None, None, &hdr(&[(HOST, host)])),
                "{host} must be refused"
            );
        }
        for origin in ["http://attacker.example", "null"] {
            assert!(
                !token_ok(
                    None,
                    None,
                    &hdr(&[(HOST, "127.0.0.1:8288"), (ORIGIN, origin)])
                ),
                "Origin {origin} must be refused"
            );
        }
        assert!(
            token_ok(
                Some("s3cret"),
                Some("s3cret"),
                &hdr(&[(HOST, "nuthatch.example.com")])
            ),
            "a token-gated surface is reached through a proxy under any name"
        );
    }

    /// #2219: a cell's `html` member is trusted only from the page's own formatters. A named tuple
    /// decodes to a JSON object in `/table`, so a component called `html` arrives as one.
    #[test]
    fn admin_grid_trusts_html_only_from_a_formatter() {
        assert!(
            !ADMIN_HTML.contains("text && text.html ?"),
            "the grid renders any cell's `html` member raw"
        );
        assert!(ADMIN_HTML
            .contains("const trusted = c.fmt && v && typeof v === 'object' && 'html' in v;"));
    }

    #[tokio::test]
    async fn admin_token_enforced_off_localhost() {
        // SEC-5: with a required token set (off-localhost), the route must actually CHECK it per
        // request - not merely be enabled by the env var's presence.
        let tmp = tempfile::tempdir().unwrap();
        let mut state = test_state(tmp.path(), SQL_MAX_CONCURRENCY);
        state.admin_token = Some("s3cret".into());

        let tok = |t: Option<&str>| {
            Query(AdminQuery {
                token: t.map(str::to_string),
            })
        };
        let no_hdr = axum::http::HeaderMap::new;
        /// An `Authorization` header carrying `value`.
        fn bearer(value: &str) -> axum::http::HeaderMap {
            let mut h = axum::http::HeaderMap::new();
            h.insert(
                axum::http::header::AUTHORIZATION,
                axum::http::HeaderValue::from_str(value).unwrap(),
            );
            h
        }
        let status = |s: AppState, q, h| async move {
            admin_index(State(s), q, h).await.into_response().status()
        };

        // --- The query-parameter form (SEC-5), unchanged. ---
        assert_eq!(
            status(state.clone(), tok(None), no_hdr()).await,
            StatusCode::UNAUTHORIZED,
            "no token"
        );
        assert_eq!(
            status(state.clone(), tok(Some("nope")), no_hdr()).await,
            StatusCode::UNAUTHORIZED,
            "wrong token"
        );
        assert_eq!(
            status(state.clone(), tok(Some("s3cret")), no_hdr()).await,
            StatusCode::OK,
            "correct token"
        );

        // --- The header form (audit L7): a caller that can set headers need not put the secret in a
        // query string, where it would land in proxy logs and `Referer`. ---
        assert_eq!(
            status(state.clone(), tok(None), bearer("Bearer s3cret")).await,
            StatusCode::OK,
            "Authorization: Bearer must be accepted"
        );
        // RFC 7235: the scheme is case-insensitive.
        assert_eq!(
            status(state.clone(), tok(None), bearer("bearer s3cret")).await,
            StatusCode::OK,
            "the scheme is case-insensitive"
        );
        // …but nothing else is loosened.
        for bad in [
            "Bearer nope",
            "Basic s3cret",
            "s3cret",
            "Bearer",
            "Bearer ",
            "Bearer s3cret extra",
        ] {
            assert_eq!(
                status(state.clone(), tok(None), bearer(bad)).await,
                StatusCode::UNAUTHORIZED,
                "must reject Authorization: {bad}"
            );
        }
        // A valid header still wins when the query param is wrong (either channel may authorise).
        assert_eq!(
            status(state, tok(Some("nope")), bearer("Bearer s3cret")).await,
            StatusCode::OK
        );
    }

    #[test]
    fn admin_html_is_embedded_and_bounded() {
        assert!(ADMIN_HTML.contains("<title>nuthatch</title>"));
        // The RFC-0010 budget: the embedded UI stays well under 150 KB and pulls in nothing external.
        assert!(ADMIN_HTML.len() < 150 * 1024, "admin UI ≤ 150 KB");
        assert!(
            !ADMIN_HTML.contains("http://") && !ADMIN_HTML.contains("https://"),
            "admin UI makes no external requests (same-origin only)"
        );
        // The status view is server-pushed (SSE) with a polling fallback - not poll-only.
        assert!(ADMIN_HTML.contains("EventSource"), "admin UI uses SSE");
        assert!(
            ADMIN_HTML.contains("_admin/events"),
            "admin UI subscribes to the events stream"
        );
        assert!(
            ADMIN_HTML.contains("startPolling"),
            "admin UI keeps a polling fallback"
        );
        // Root-relative fetches answer from whatever sits at a proxy's root: another nest's data.
        assert!(
            !ADMIN_HTML.contains("fetch('/") && !ADMIN_HTML.contains("api('/"),
            "admin UI fetches relative to its own path"
        );
        // The #435 caveat, pinned the same way and for the same reason as the three above: substring,
        // no render. Of the four surfaces carrying the reduced-cold-data signal this is the only one
        // with no coverage of any kind, so deleting the caveat here would be silent.
        assert!(
            ADMIN_HTML.contains("degraded_tables"),
            "admin UI renders the reduced-cold-data caveat (#435)"
        );
        // Same reasoning, same pin, for the other kind of incomplete (#472): a lost tip is nest-wide
        // and every-table, not per-table, so it needs its own substring or a deleted caveat here is
        // just as silent as the arm that started this.
        assert!(
            ADMIN_HTML.contains("tip_unavailable"),
            "admin UI renders the lost-tip caveat (#472)"
        );
        assert!(
            ADMIN_HTML.contains("s.publish") && ADMIN_HTML.contains("last_success_unixtime"),
            "admin UI renders the publish row (RFC-0052 §3.8)"
        );
        assert!(
            ADMIN_HTML.contains("nuthatch_rpc_methods_total")
                && ADMIN_HTML.contains("nuthatch_start_time_seconds"),
            "admin UI prices RPC from the method counters and the start time"
        );
    }

    #[test]
    fn the_status_summary_carries_a_publish_row_only_for_a_publishing_nest() {
        let tmp = tempfile::tempdir().unwrap();
        let mut state = test_state(tmp.path(), SQL_MAX_CONCURRENCY);
        state.nest_info = Arc::new(json!({ "name": "admin-publish-row" }));
        assert!(summary_value(&state)["publish"].is_null());

        let m = crate::metrics::METRICS.nest("admin-publish-row");
        m.set_publish_enabled("s3://bucket/mirror");
        assert!(summary_value(&state)["publish"]["last_success_unixtime"].is_null());
        m.publish_succeeded(Some(12));

        let row = &summary_value(&state)["publish"];
        assert_eq!(row["target"], "s3://bucket/mirror");
        assert_eq!(row["sealed_through"], 12);
        assert!(row["lag_blocks"].is_u64(), "{row}");
        assert!(row["last_success_unixtime"].as_u64().unwrap() > 0, "{row}");
        assert_eq!(row["dead_letter"], false);
    }

    #[tokio::test]
    async fn admin_events_gated_like_the_admin_page() {
        let tmp = tempfile::tempdir().unwrap();
        let mut state = test_state(tmp.path(), SQL_MAX_CONCURRENCY);

        // A pushed frame is byte-identical to `GET /`: both go through `summary_value`.
        let v = summary_value(&state);
        assert_eq!(v["name"], "nuthatch");
        assert!(
            v.get("last_block").is_some(),
            "frame carries the tip watermark"
        );
        assert!(v["views"].is_array(), "frame lists the IVM views");

        let tok = |t: Option<&str>| {
            Query(AdminQuery {
                token: t.map(str::to_string),
            })
        };

        // Localhost, no token required → the stream opens.
        assert_eq!(
            admin_events(
                State(state.clone()),
                tok(None),
                {
                    let mut h = axum::http::HeaderMap::new();
                    h.insert(axum::http::header::HOST, "127.0.0.1:8288".parse().unwrap());
                    h
                },
                None
            )
            .await
            .into_response()
            .status(),
            StatusCode::OK
        );
        // Admin disabled → 404, exactly like the page.
        state.admin_enabled = false;
        assert_eq!(
            admin_events(
                State(state.clone()),
                tok(None),
                axum::http::HeaderMap::new(),
                None
            )
            .await
            .into_response()
            .status(),
            StatusCode::NOT_FOUND
        );
        // Off-localhost token enforced (SEC-5): missing → 401, correct → 200.
        state.admin_enabled = true;
        state.admin_token = Some("s3cret".into());
        assert_eq!(
            admin_events(
                State(state.clone()),
                tok(None),
                axum::http::HeaderMap::new(),
                None
            )
            .await
            .into_response()
            .status(),
            StatusCode::UNAUTHORIZED
        );
        assert_eq!(
            admin_events(
                State(state.clone()),
                tok(Some("s3cret")),
                axum::http::HeaderMap::new(),
                None
            )
            .await
            .into_response()
            .status(),
            StatusCode::OK
        );
        // The SSE stream accepts the header form too (audit L7), even though the admin UI's own
        // `EventSource` cannot send one - a scripted consumer can.
        let mut hdr = axum::http::HeaderMap::new();
        hdr.insert(
            axum::http::header::AUTHORIZATION,
            axum::http::HeaderValue::from_static("Bearer s3cret"),
        );
        assert_eq!(
            admin_events(State(state), tok(None), hdr, None)
                .await
                .into_response()
                .status(),
            StatusCode::OK
        );
    }

    /// Serve `app` on an ephemeral port behind [`serve_until`]; the sender is the shutdown signal.
    async fn served(
        app: Router,
        grace: Duration,
    ) -> (
        std::net::SocketAddr,
        tokio::sync::oneshot::Sender<()>,
        tokio::task::JoinHandle<()>,
    ) {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let (fire, fired) = tokio::sync::oneshot::channel::<()>();
        let server = tokio::spawn(serve_until(
            listener,
            app,
            async {
                let _ = fired.await;
            },
            grace,
        ));
        (addr, fire, server)
    }

    /// #1964: an admin tab holding `/_admin/events` kept SIGTERM from ever stopping the nest. The
    /// grace here is far longer than the test's bound, so only the stream ending can pass it.
    #[tokio::test]
    async fn shutdown_ends_an_open_admin_events_stream() {
        let tmp = tempfile::tempdir().unwrap();
        let app = router(SharedNest::new(test_state(tmp.path(), SQL_MAX_CONCURRENCY)));
        let (addr, fire, server) = served(app, Duration::from_secs(600)).await;

        let mut events = reqwest::get(format!("http://{addr}/_admin/events"))
            .await
            .unwrap();
        assert_eq!(events.status(), StatusCode::OK);
        assert!(events.chunk().await.unwrap().is_some(), "first frame");

        fire.send(()).unwrap();
        tokio::time::timeout(Duration::from_secs(5), server)
            .await
            .expect("serve still waiting on the events stream after shutdown")
            .unwrap();
        let rest = tokio::time::timeout(Duration::from_secs(5), async {
            while let Ok(Some(_)) = events.chunk().await {}
        })
        .await;
        assert!(rest.is_ok(), "the events stream did not end");
    }

    /// The backstop: a connection that never finishes is dropped once the grace runs out, so the
    /// caller still gets to stop the ingest.
    #[tokio::test]
    async fn shutdown_drops_a_connection_that_never_finishes_after_the_grace() {
        let stuck = Router::new().route(
            "/stuck",
            axum::routing::get(|| async {
                axum::body::Body::from_stream(futures::stream::pending::<
                    Result<String, std::convert::Infallible>,
                >())
            }),
        );
        let (addr, fire, server) = served(stuck, Duration::from_millis(200)).await;

        let mut held = reqwest::get(format!("http://{addr}/stuck")).await.unwrap();
        assert_eq!(held.status(), StatusCode::OK);

        fire.send(()).unwrap();
        tokio::time::timeout(Duration::from_secs(5), server)
            .await
            .expect("serve ignored its grace and waited on a connection that never ends")
            .unwrap();
        let cut = tokio::time::timeout(Duration::from_secs(5), held.chunk()).await;
        assert!(
            matches!(cut, Ok(Err(_)) | Ok(Ok(None))),
            "the stuck connection was left open: {cut:?}"
        );
    }

    async fn sql_json(state: &AppState, q: &str) -> (StatusCode, Value) {
        let resp = sql(
            State(state.clone()),
            Query(SqlQuery {
                q: q.into(),
                max_rows: None,
            }),
        )
        .await
        .into_response();
        let status = resp.status();
        let body = axum::body::to_bytes(resp.into_body(), usize::MAX)
            .await
            .unwrap();
        (status, serde_json::from_slice(&body).unwrap())
    }

    /// A remembered answer is the computed one byte for byte bar `cached`, rows written from the
    /// stored JSON in the same place (#2072).
    #[tokio::test]
    async fn a_memo_hit_is_the_computed_body_but_for_cached() {
        let tmp = tempfile::tempdir().unwrap();
        let state = test_state(tmp.path(), 2);
        for (b, who) in [(1u64, "a \"quoted\" \\ name\n"), (2, "ünïcode")] {
            state
                .store
                .put_entity(
                    &format!("k{b}"),
                    &json!({"table": "t", "block_number": b, "who": who, "v": "123456789012345678901234567890"}).to_string(),
                )
                .unwrap();
        }
        let q =
            "SELECT block_number, who, CAST(v AS DECIMAL(38, 0)) AS v FROM t ORDER BY block_number";
        let raw = |state: AppState| async move {
            let resp = sql(
                State(state),
                Query(SqlQuery {
                    q: q.into(),
                    max_rows: None,
                }),
            )
            .await
            .into_response();
            assert_eq!(resp.status(), StatusCode::OK);
            assert_eq!(
                resp.headers()[axum::http::header::CONTENT_TYPE],
                "application/json"
            );
            axum::body::to_bytes(resp.into_body(), usize::MAX)
                .await
                .unwrap()
        };
        let first = raw(state.clone()).await;
        let second = raw(state.clone()).await;
        let first = String::from_utf8(first.to_vec()).unwrap();
        let second = String::from_utf8(second.to_vec()).unwrap();
        assert!(first.contains("\"cached\":false"), "{first}");
        assert!(second.contains("\"cached\":true"), "{second}");
        assert_eq!(second.replace("\"cached\":true", "\"cached\":false"), first);
    }

    /// **#1186, the memo's contract.** A repeated statement over an unchanged store is answered from
    /// the memo, and one committed write is enough to make the next request compute again. The third
    /// request asserts the *rows*, not only the `cached` flag: with the write generation left out of
    /// the key, or never bumped, the flag would still read `true` and the count would still read 2 -
    /// which is the stale answer this memo must never give.
    #[tokio::test]
    async fn a_repeated_statement_is_remembered_until_the_store_moves() {
        let tmp = tempfile::tempdir().unwrap();
        let state = test_state(tmp.path(), 2);
        for b in 1..=2u64 {
            state
                .store
                .put_entity(
                    &format!("k{b}"),
                    &json!({"table": "t", "block_number": b}).to_string(),
                )
                .unwrap();
        }
        let q = "SELECT count(*) AS n FROM t";

        let (st, first) = sql_json(&state, q).await;
        assert_eq!(st, StatusCode::OK, "{first}");
        assert_eq!(first["cached"], false);
        assert_eq!(first["rows"][0]["n"], 2);

        let (st, second) = sql_json(&state, q).await;
        assert_eq!(st, StatusCode::OK);
        assert_eq!(second["cached"], true, "same inputs, remembered answer");
        assert_eq!(second["rows"], first["rows"]);

        state
            .store
            .put_entity("k3", &json!({"table": "t", "block_number": 3}).to_string())
            .unwrap();
        let (st, third) = sql_json(&state, q).await;
        assert_eq!(st, StatusCode::OK);
        assert_eq!(third["cached"], false, "one commit is a new identity");
        assert_eq!(
            third["rows"][0]["n"], 3,
            "and the answer is the new state, not the old one"
        );
    }

    /// #1951: a poll's commit and a watermark that passes no row change nothing a statement reads, so
    /// the memo keeps its answer; a segment the watermark admits with nothing hot behind it, as
    /// `--seal-direct` writes them, does change the rows and must be a new identity.
    #[tokio::test]
    async fn a_statement_is_remembered_across_commits_that_change_no_row() {
        let tmp = tempfile::tempdir().unwrap();
        let state = test_state(tmp.path(), 2);
        let row = |b: u64| json!({"table": "t", "block_number": b, "log_index": 0}).to_string();
        for b in [10u64, 11] {
            state
                .store
                .put_entity(&Store::entity_key(b, 0), &row(b))
                .unwrap();
        }
        let q = "SELECT count(*) AS n FROM t";
        let (st, first) = sql_json(&state, q).await;
        assert_eq!(st, StatusCode::OK, "{first}");
        assert_eq!(
            (first["cached"].clone(), first["rows"][0]["n"].clone()),
            (json!(false), json!(2))
        );

        state
            .store
            .commit_window(&[], Some((12, "0xaa")), 12)
            .unwrap();
        state.store.set_meta("sealed_through", "5").unwrap();
        let (_, polled) = sql_json(&state, q).await;
        assert_eq!(
            polled["cached"], true,
            "a poll and a watermark that passed no row: {polled}"
        );
        assert_eq!(polled["rows"][0]["n"], 2);
        assert_eq!(
            (
                polled["provenance"]["as_of"].clone(),
                polled["provenance"]["sealed_through"].clone()
            ),
            (json!(12), json!(5)),
            "a hit cites the head it was checked at, not the one it was computed at"
        );

        let sealed: Vec<String> = (6..=8).map(row).collect();
        crate::seal::seal_range(tmp.path(), &sealed, 6, 8).unwrap();
        let (_, written) = sql_json(&state, q).await;
        assert_eq!(
            written["rows"][0]["n"], 2,
            "a segment above the watermark is not served yet"
        );

        state.store.set_meta("sealed_through", "8").unwrap();
        let (_, admitted) = sql_json(&state, q).await;
        assert_eq!(
            admitted["cached"], false,
            "the watermark admitted a segment: {admitted}"
        );
        assert_eq!(admitted["rows"][0]["n"], 5, "and the answer reads it");
    }

    /// A statement whose value is not a function of the indexed state is computed every time: the
    /// memo would otherwise hand the first `random()` to every later caller as a fact.
    #[tokio::test]
    async fn a_volatile_statement_is_never_remembered() {
        let tmp = tempfile::tempdir().unwrap();
        let state = test_state(tmp.path(), 2);
        let (st, first) = sql_json(&state, "SELECT random() AS r").await;
        assert_eq!(st, StatusCode::OK, "{first}");
        assert_eq!(first["cached"], false);
        let (st, second) = sql_json(&state, "SELECT random() AS r").await;
        assert_eq!(st, StatusCode::OK);
        assert_eq!(
            second["cached"], false,
            "random() is not a fact about the nest"
        );
        let (_, third) = sql_json(&state, "SELECT current_timestamp AS t").await;
        assert_eq!(third["cached"], false);
        let (_, fourth) = sql_json(&state, "SELECT current_timestamp AS t").await;
        assert_eq!(fourth["cached"], false);
    }

    /// **Jules on #1189, and the answer is the ordering.** The concern was that a statement reading
    /// something outside the nest - `read_csv_auto('/tmp/x.csv')` and friends - has no stamp in the
    /// memo key, so changing that file would leave a remembered answer standing.
    ///
    /// It cannot, because such a statement never produces an answer to remember: `/sql` refuses every
    /// file-reading table function (SEC-2's denylist and the parser-derived allowlist, both in
    /// `analytics::attempt`), and only the `Ok` arm calls `sqlmemo::put`. Confirmed against the live
    /// Lodestar nest on 2026-09-06 - `read_csv_auto`, `read_parquet`, `glob` and `read_text` each
    /// answered `400`.
    ///
    /// That is an argument about the order of two guards, which is exactly the kind that stops being
    /// true when someone moves one. So this pins it: the statement is refused, and nothing is
    /// remembered under it, asserted through the handler rather than by reading the code.
    #[tokio::test]
    async fn a_statement_reading_outside_the_nest_is_refused_and_never_remembered() {
        let tmp = tempfile::tempdir().unwrap();
        let state = test_state(tmp.path(), 2);
        let outside = tmp.path().join("outside.csv");
        std::fs::write(&outside, "n\n1\n").unwrap();
        let q = format!(
            "SELECT count(*) AS n FROM read_csv_auto('{}')",
            outside.display()
        );

        // By this statement's own key, not the global count: the memo is process-wide, and any other
        // test's successful /sql moves the count (#1812).
        let key = |sql: &str| {
            memo_identity(&state, sql, SQL_MAX_ROWS, None)
                .expect("a deterministic statement against this store is memoised")
                .0
        };
        let (st, body) = sql_json(&state, &q).await;
        assert_eq!(
            st,
            StatusCode::BAD_REQUEST,
            "a file-reading statement must be refused: {body}"
        );
        assert!(
            !crate::sqlmemo::contains(&key(&q)),
            "a refused statement must leave nothing in the memo"
        );

        // The same lookup does see an answer that was remembered, so the absence above means something.
        let (st, body) = sql_json(&state, "SELECT 7 AS n").await;
        assert_eq!(st, StatusCode::OK, "{body}");
        assert!(
            crate::sqlmemo::contains(&key("SELECT 7 AS n")),
            "an answered statement is remembered under the key this test computes"
        );

        // And again, so a second identical request cannot be answered from an entry the first left.
        let (st, body) = sql_json(&state, &q).await;
        assert_eq!(st, StatusCode::BAD_REQUEST, "{body}");
        assert_eq!(
            body["cached"],
            Value::Null,
            "a refusal carries no cached flag: {body}"
        );
    }

    /// A remembered answer costs no Burrmill, so it is served past a saturated permit gate - that is
    /// most of the point under a dashboard's burst. A statement with no remembered answer is still
    /// refused, so the gate still bounds what it was built to bound.
    #[tokio::test]
    async fn a_memo_hit_needs_no_permit() {
        let tmp = tempfile::tempdir().unwrap();
        let state = test_state(tmp.path(), 1);
        let (st, first) = sql_json(&state, "SELECT 41 + 1 AS n").await;
        assert_eq!(st, StatusCode::OK, "{first}");
        assert_eq!(first["cached"], false);

        let _held = Arc::clone(&state.sql_gate).try_acquire_owned().unwrap();
        let (st, again) = sql_json(&state, "SELECT 41 + 1 AS n").await;
        assert_eq!(
            st,
            StatusCode::OK,
            "a hit is answered with the only permit held elsewhere"
        );
        assert_eq!(again["cached"], true);
        assert_eq!(again["rows"][0]["n"], 42);

        let (st, fresh) = sql_json(&state, "SELECT 43 AS n").await;
        assert_eq!(
            st,
            StatusCode::SERVICE_UNAVAILABLE,
            "a statement that must compute still waits on the gate: {fresh}"
        );
    }

    // ---------------------------------------------------------------------------------------
    // RFC-0046 S0 (#1217) - payment is absent, so the default surface does not charge.
    //
    // Driven through [`router`] (solo `nuthatch dev`) and [`compose_runtime`] (a mounts), not
    // the handlers: a 402 layer on the composition is how a default-on price would actually
    // land, and calling `sql(State(..))` would not see it.
    // ---------------------------------------------------------------------------------------

    const ENTITY_ID: &str = "k1";
    const SQL_PATH: &str = "/sql?q=SELECT%201%20AS%20n";
    const PAYMENT_REQ: &[(&str, &str)] = &[
        (
            "PAYMENT-SIGNATURE",
            "eyJhbGciOiJub3QtYS1yZWFsLXBheW1lbnQifQ",
        ),
        ("payment-required", "should-be-ignored-when-unconfigured"),
    ];

    fn unpriced_state(dir: &std::path::Path, name: &str) -> AppState {
        let mut st = test_state(dir, SQL_MAX_CONCURRENCY);
        st.store
            .put_entity(
                ENTITY_ID,
                &json!({"id": ENTITY_ID, "table": "t"}).to_string(),
            )
            .unwrap();
        // Per-nest `/ready`, not the process globals: other tests stamp those and would
        // make an unstamped fixture flake as 503, which is not a payment failure.
        let health = Arc::new(crate::health::RuntimeHealth::new());
        health.register(name, "ethereum");
        st.runtime_health = Some((name.to_string(), health));
        st
    }

    fn header_is_payment(name: &str) -> bool {
        let n = name.to_ascii_lowercase();
        n.contains("payment") || n.contains("x402")
    }

    fn assert_no_payment_headers(headers: &axum::http::HeaderMap, path: &str) {
        let hits: Vec<&str> = headers
            .keys()
            .map(|k| k.as_str())
            .filter(|k| header_is_payment(k))
            .collect();
        assert!(
            hits.is_empty(),
            "{path} sent a payment header with payment unconfigured: {hits:?}"
        );
    }

    async fn probe(
        router: Router,
        path: &str,
        headers: &[(&str, &str)],
    ) -> (StatusCode, axum::http::HeaderMap, Vec<u8>) {
        use tower::ServiceExt;
        let mut req = axum::http::Request::builder().uri(path);
        for (k, v) in headers {
            req = req.header(*k, *v);
        }
        let req = req.body(axum::body::Body::empty()).unwrap();
        let resp = router.oneshot(req).await.unwrap();
        let status = resp.status();
        let headers = resp.headers().clone();
        let body = axum::body::to_bytes(resp.into_body(), usize::MAX)
            .await
            .unwrap()
            .to_vec();
        (status, headers, body)
    }

    fn body_for_compare(body: &[u8]) -> Vec<u8> {
        match serde_json::from_slice::<serde_json::Value>(body) {
            Ok(mut v) if v.is_object() => {
                let map = v.as_object_mut().unwrap();
                for k in VOLATILE {
                    map.remove(*k);
                }
                serde_json::to_vec(&v).unwrap()
            }
            _ => body.to_vec(),
        }
    }

    async fn assert_unpriced(plain: Router, paying: Router, path: &str, want: StatusCode) {
        let (s0, h0, b0) = probe(plain, path, &[]).await;
        let (s1, h1, b1) = probe(paying, path, PAYMENT_REQ).await;
        assert_ne!(
            s0,
            StatusCode::PAYMENT_REQUIRED,
            "{path} returned 402 with payment unconfigured: {}",
            String::from_utf8_lossy(&b0)
        );
        assert_eq!(
            s0,
            want,
            "{path} must still serve, not merely avoid 402: {} {}",
            s0,
            String::from_utf8_lossy(&b0)
        );
        assert_no_payment_headers(&h0, path);
        assert_eq!(
            s0, s1,
            "{path} changed status when a payment header was sent, so unconfigured payment code \
             was reached: {s0} vs {s1}"
        );
        assert_eq!(
            body_for_compare(&b0),
            body_for_compare(&b1),
            "{path} changed body when a payment header was sent"
        );
        assert_no_payment_headers(&h1, path);
        assert_ne!(
            s1,
            StatusCode::PAYMENT_REQUIRED,
            "{path} 402ed a paid request"
        );
    }

    /// RFC-0046 S0 (#1217). An unconfigured nest does not charge on `/sql`, `/ready`, or
    /// entity point-reads, through the real composition. A default-on 402 on any of those
    /// turns this red; so does a layer that only fires when a payment header is present.
    #[tokio::test]
    async fn an_unpriced_nest_does_not_charge_on_the_default_surface() {
        let entity = format!("/entity/{ENTITY_ID}");
        let solo_paths = [
            ("/health", StatusCode::OK),
            ("/ready", StatusCode::OK),
            (SQL_PATH, StatusCode::OK),
            (entity.as_str(), StatusCode::OK),
        ];

        let a = tempfile::tempdir().unwrap();
        let b = tempfile::tempdir().unwrap();
        let plain = router(SharedNest::new(unpriced_state(a.path(), "pay-abs-solo-a")));
        let paying = router(SharedNest::new(unpriced_state(b.path(), "pay-abs-solo-b")));
        for (path, want) in solo_paths {
            assert_unpriced(plain.clone(), paying.clone(), path, want).await;
        }

        // Unique names: NestMetrics is process-global and shared by every test in the binary.
        let name = "pay-abs-runtime";
        let ca = tempfile::tempdir().unwrap();
        let cb = tempfile::tempdir().unwrap();
        std::fs::create_dir_all(ca.path().join(name)).unwrap();
        std::fs::create_dir_all(cb.path().join(name)).unwrap();
        let compose = |dir: &std::path::Path, stamp: &str| {
            let health = Arc::new(crate::health::RuntimeHealth::new());
            let roster = json!({"runtime": "t", "nests": [{"name": name}]});
            compose_runtime(
                roster,
                vec![(name.to_string(), unpriced_state(&dir.join(name), stamp))],
                health,
            )
        };
        let plain = compose(ca.path(), "pay-abs-rt-a");
        let paying = compose(cb.path(), "pay-abs-rt-b");
        let runtime_paths = [
            "/health".to_string(),
            "/ready".to_string(),
            "/nests".to_string(),
            format!("/{name}/health"),
            format!("/{name}/ready"),
            format!("/{name}{SQL_PATH}"),
            format!("/{name}{entity}"),
        ];
        for path in &runtime_paths {
            assert_unpriced(plain.clone(), paying.clone(), path, StatusCode::OK).await;
        }
    }

    /// S2's positive boundary: only a declared query on an explicitly priced mount asks for a
    /// payment. `/sql` and every default mount stay outside the counter.
    #[cfg(feature = "counter")]
    #[tokio::test]
    async fn a_priced_named_query_returns_the_local_challenge_before_serving() {
        use base64::Engine;
        use tower::ServiceExt;

        let d = tempfile::tempdir().unwrap();
        let mut state = test_state(d.path(), SQL_MAX_CONCURRENCY);
        state.surface = Arc::new(crate::allowlist::Surface {
            access: crate::allowlist::SqlAccess::Allowlist,
            queries: vec![crate::allowlist::NamedQuery {
                name: "answer".into(),
                sql: "SELECT 1 AS answer".into(),
                params: Default::default(),
            }],
        });
        state.counter = Some(Arc::new(crate::counter::Config {
            price: "1000".into(),
            recipient: "0x1111111111111111111111111111111111111111".into(),
            network: crate::counter::Network::Testnet,
        }));

        let response = router(SharedNest::new(state))
            .oneshot(
                axum::http::Request::builder()
                    .uri("/q/answer")
                    .body(axum::body::Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::PAYMENT_REQUIRED);
        let challenge = response
            .headers()
            .get("payment-required")
            .and_then(|v| v.to_str().ok())
            .expect("the x402 challenge header");
        let decoded = base64::engine::general_purpose::STANDARD
            .decode(challenge)
            .expect("base64 challenge");
        let body: serde_json::Value = serde_json::from_slice(&decoded).expect("JSON challenge");
        assert_eq!(body["accepts"][0]["amount"], "1000");
        assert_eq!(body["accepts"][0]["network"], "eip155:84532");
    }
    /// The nest every Graph-endpoint test below talks to: a schema, three views, and a head.
    ///
    /// Shared because these tests were one 421-line function with thirty assertions, and a Rust test
    /// stops at its **first** failure - so a mutation breaking an early assertion hid every later one,
    /// and three different mutations all reported the same line. Split so each lands on its own test.
    fn graph_fixture() -> (tempfile::TempDir, AppState) {
        let d = tempfile::tempdir().unwrap();
        std::fs::create_dir_all(d.path().join("graph")).unwrap();
        std::fs::write(
            d.path().join("graph/schema.graphql"),
            concat!(
                "type Pool @entity {\n  id: ID!\n  liquidity: BigInt!\n  hooks: String!\n",
                "  token0: Token!\n",
                "  swaps: [Swap!]! @derivedFrom(field: \"pool\")\n}\n",
                "type Swap @entity { id: ID! pool: Pool! amount: BigInt! }\n",
                // `decimals` is `Int!` and stays a JSON number; `totalSupply` is `BigInt!` and must be a
                // string. Both are here because the wire rule is per-scalar, and a fixture with only one
                // of them cannot tell "casts everything" from "casts the right things".
                //
                // `totalSupply`'s value is small on purpose. The first version used 1e24, which exceeds
                // `BIGINT` so the serialiser already rendered it as a string - and the mutation
                // dropping the cast stayed green against it. A value that fits in an integer is the one
                // that tells the cast from the storage type.
                "type Token @entity { id: ID! symbol: String! decimals: Int! totalSupply: BigInt! }\n",
            ),
        )
        .unwrap();
        // A view named for the entity, so the compiled SQL has something to read: this test is the
        // whole user story end to end, GraphQL in over HTTP and an entity row out.
        std::fs::create_dir_all(d.path().join("views")).unwrap();
        std::fs::write(
            d.path().join("views/pool.sql"),
            "CREATE VIEW pool AS SELECT '0xaaa' AS id, 42 AS liquidity, '0xhook' AS hooks, '0xt1' AS token0 \
             UNION ALL SELECT '0xbbb', 7, '0xhook2', '0xmissing' \
             UNION ALL SELECT '0xccc', 99, '0xhook3', '0xt2';\n",
        )
        .unwrap();
        std::fs::write(
            d.path().join("views/swap.sql"),
            "CREATE VIEW swap AS SELECT 's2' AS id, '0xaaa' AS pool, 7 AS amount \
             UNION ALL SELECT 's1', '0xaaa', 5;\n",
        )
        .unwrap();
        std::fs::write(
            d.path().join("views/token.sql"),
            // `0xt2` exists but has nothing in its selected fields: that is the row that tells a
            // missing join apart from an all-null one.
            "CREATE VIEW token AS SELECT '0xt1' AS id, 'WETH' AS symbol, 18 AS decimals, \
             21000000 AS \"totalSupply\" \
             UNION ALL SELECT '0xt2', NULL, NULL, NULL;\n",
        )
        .unwrap();
        let state = test_state(d.path(), SQL_MAX_CONCURRENCY);
        // `_meta` reports the nest's own head, so give it one to report - with the hash of the head
        // and of its parent, and the head's timestamp, which `_meta.block` is answered from.
        state.store.set_meta("last_block", "23456789").unwrap();
        state.store.set_block_hash(23_456_789, "0xhead").unwrap();
        state
            .store
            .set_block_timestamp(23_456_789, 1_700_000_000)
            .unwrap();
        state.store.set_block_hash(23_456_788, "0xparent").unwrap();

        (d, state)
    }

    /// POST one GraphQL operation and decode the envelope.
    #[cfg(feature = "graph")]
    async fn graph_ask(uri: &str, q: &str, st: AppState) -> serde_json::Value {
        graph_ask_variables(uri, q, json!({}), st).await
    }

    #[cfg(feature = "graph")]
    async fn graph_ask_variables(
        uri: &str,
        q: &str,
        variables: serde_json::Value,
        st: AppState,
    ) -> serde_json::Value {
        use tower::ServiceExt;
        let res = router(SharedNest::new(st))
            .oneshot(
                axum::http::Request::builder()
                    .method("POST")
                    .uri(uri)
                    .header("content-type", "application/json")
                    .body(axum::body::Body::from(
                        serde_json::json!({ "query": q, "variables": variables }).to_string(),
                    ))
                    .unwrap(),
            )
            .await
            .unwrap();
        let bytes = axum::body::to_bytes(res.into_body(), 4 << 20)
            .await
            .unwrap();
        serde_json::from_slice(&bytes).unwrap()
    }

    #[cfg(feature = "graph")]
    fn reset_graph_budget() {
        GRAPH_SQL_CALLS.with(|c| c.set(0));
        GRAPH_PERMITS.with(|c| c.set(0));
        GRAPH_CLOCK_SKEW.with(|c| c.set(None));
        GRAPH_BYTE_CAP.with(|c| c.set(None));
        GRAPH_LAST_LIMIT.with(|c| c.set(0));
        GRAPH_GUARD_TIMEOUT.with(|c| c.set(None));
    }

    #[cfg(feature = "graph")]
    struct GraphBudgetGuard;

    #[cfg(feature = "graph")]
    impl Drop for GraphBudgetGuard {
        fn drop(&mut self) {
            reset_graph_budget();
        }
    }

    #[cfg(feature = "graph")]
    fn account_nest() -> (tempfile::TempDir, AppState) {
        let dir = tempfile::tempdir().unwrap();
        std::fs::create_dir_all(dir.path().join("graph")).unwrap();
        std::fs::create_dir_all(dir.path().join("views")).unwrap();
        std::fs::write(
            dir.path().join("graph/schema.graphql"),
            "type Account @entity { id: ID! }",
        )
        .unwrap();
        std::fs::write(
            dir.path().join("views/account.sql"),
            "CREATE VIEW account AS SELECT 'a' AS id;",
        )
        .unwrap();
        let state = test_state(dir.path(), SQL_MAX_CONCURRENCY);
        (dir, state)
    }

    #[cfg(feature = "graph")]
    #[tokio::test]
    async fn a_bigint_list_answers_its_elements_as_strings() {
        reset_graph_budget();
        let _guard = GraphBudgetGuard;
        let dir = tempfile::tempdir().unwrap();
        std::fs::create_dir_all(dir.path().join("graph")).unwrap();
        std::fs::create_dir_all(dir.path().join("views")).unwrap();
        std::fs::write(
            dir.path().join("graph/schema.graphql"),
            "type Bet @entity { id: ID! rolled: [BigInt!]! weights: [Int!]! }",
        )
        .unwrap();
        std::fs::write(
            dir.path().join("views/bet.sql"),
            "CREATE VIEW bet AS SELECT '1' AS id, \
             [CAST(9007199254740993 AS BIGINT), CAST(5 AS BIGINT)] AS rolled, [1, 2] AS weights;",
        )
        .unwrap();
        let state = test_state(dir.path(), SQL_MAX_CONCURRENCY);
        let answer = graph_ask("/graphql", "{ bets { rolled weights } }", state).await;
        assert_eq!(
            answer["data"]["bets"][0],
            serde_json::json!({"rolled": ["9007199254740993", "5"], "weights": [1, 2]}),
            "graph-node sends BigInt as a string, inside a list too: {answer}"
        );
    }

    #[cfg(feature = "graph")]
    #[tokio::test]
    async fn a_graphql_operation_refuses_too_many_roots() {
        reset_graph_budget();
        let _guard = GraphBudgetGuard;
        let (_dir, state) = account_nest();
        let mut q = String::from("{ ");
        for i in 0..33 {
            q.push_str(&format!("a{i}: accounts {{ id }} "));
        }
        q.push('}');
        let answer = graph_ask("/graphql", &q, state).await;
        assert_eq!(GRAPH_SQL_CALLS.with(|c| c.get()), 0, "{answer}");
        assert_eq!(GRAPH_PERMITS.with(|c| c.get()), 0, "{answer}");
        assert!(
            answer["errors"][0]["message"]
                .as_str()
                .unwrap()
                .contains("32"),
            "{answer}"
        );
    }

    #[cfg(feature = "graph")]
    #[tokio::test]
    async fn a_graphql_operation_refuses_nine_full_pages() {
        reset_graph_budget();
        let _guard = GraphBudgetGuard;
        let (_dir, state) = account_nest();
        let mut q = String::from("{ ");
        for i in 0..9 {
            q.push_str(&format!("a{i}: accounts(first: 1000) {{ id }} "));
        }
        q.push('}');
        let answer = graph_ask("/graphql", &q, state).await;
        assert_eq!(GRAPH_SQL_CALLS.with(|c| c.get()), 0, "{answer}");
        assert!(
            answer["errors"][0]["message"]
                .as_str()
                .unwrap()
                .contains("8000"),
            "{answer}"
        );
    }

    #[cfg(feature = "graph")]
    #[tokio::test]
    async fn two_graphql_roots_share_one_permit_and_one_byte_budget() {
        reset_graph_budget();
        let _guard = GraphBudgetGuard;
        let (_dir, state) = account_nest();
        let answer = graph_ask(
            "/graphql",
            "{ a: accounts { id } b: accounts { id } }",
            state,
        )
        .await;
        assert!(answer.get("errors").is_none(), "{answer}");
        assert_eq!(answer["data"]["a"][0]["id"], "a", "{answer}");
        assert_eq!(GRAPH_SQL_CALLS.with(|c| c.get()), 2, "{answer}");
        assert_eq!(GRAPH_PERMITS.with(|c| c.get()), 1, "{answer}");
        assert!(
            GRAPH_LAST_LIMIT.with(|c| c.get()) < crate::engine::SQL_MAX_RESULT_BYTES,
            "the second root was given a fresh byte cap"
        );
    }

    #[cfg(feature = "graph")]
    #[tokio::test]
    async fn a_graphql_operation_stops_when_its_clock_is_spent() {
        reset_graph_budget();
        let _guard = GraphBudgetGuard;
        GRAPH_CLOCK_SKEW.with(|c| c.set(Some(SQL_TIMEOUT)));
        let (_dir, state) = account_nest();
        let answer = graph_ask(
            "/graphql",
            "{ a: accounts { id } b: accounts { id } }",
            state,
        )
        .await;
        assert_eq!(GRAPH_SQL_CALLS.with(|c| c.get()), 0, "{answer}");
        assert!(
            answer["errors"][0]["message"]
                .as_str()
                .unwrap()
                .contains("budget"),
            "{answer}"
        );
    }

    #[cfg(feature = "graph")]
    #[tokio::test]
    async fn a_graphql_root_keeps_the_operation_clock() {
        reset_graph_budget();
        let _guard = GraphBudgetGuard;
        GRAPH_CLOCK_SKEW.with(|c| c.set(Some(SQL_TIMEOUT.saturating_sub(Duration::from_secs(5)))));
        let (_dir, state) = account_nest();
        let answer = graph_ask("/graphql", "{ accounts { id } }", state).await;
        let timeout = GRAPH_GUARD_TIMEOUT.with(|c| c.get()).unwrap();
        assert!(timeout > Duration::from_secs(4), "{timeout:?} {answer}");
        assert!(timeout <= Duration::from_secs(5), "{timeout:?} {answer}");
    }

    #[cfg(feature = "graph")]
    #[tokio::test]
    async fn a_graphql_operation_stops_at_its_byte_budget() {
        reset_graph_budget();
        let _guard = GraphBudgetGuard;
        GRAPH_BYTE_CAP.with(|c| c.set(Some(1)));
        let (_dir, state) = account_nest();
        let answer = graph_ask(
            "/graphql",
            "{ a: accounts { id } b: accounts { id } }",
            state,
        )
        .await;
        assert_eq!(GRAPH_SQL_CALLS.with(|c| c.get()), 1, "{answer}");
        assert!(
            answer["errors"][0]["message"]
                .as_str()
                .unwrap()
                .contains("result budget"),
            "{answer}"
        );
    }

    #[cfg(feature = "graph")]
    fn recorded_network_fixture(facts: &Value, block: u64) -> (tempfile::TempDir, AppState) {
        let source = std::path::PathBuf::from(env!("CARGO_MANIFEST_DIR"))
            .join("tests/fixtures/network-nest");
        let config = crate::config::Config::load(&source).unwrap();
        let registry = crate::registry::from_nest(&source, &config).unwrap();
        let mut tables = registry.schema();
        tables.extend(crate::calls::schema(&config.calls, true));
        let dir = tempfile::tempdir().unwrap();
        std::fs::create_dir_all(dir.path().join("graph")).unwrap();
        std::fs::create_dir_all(dir.path().join("views")).unwrap();
        std::fs::copy(
            source.join("graph/schema.graphql"),
            dir.path().join("graph/schema.graphql"),
        )
        .unwrap();
        std::fs::write(
            dir.path().join("graph/history.toml"),
            "version = 1\nfirst_block = 42440000\nmax_head_age_seconds = 600\n",
        )
        .unwrap();
        for entry in std::fs::read_dir(source.join("views")).unwrap() {
            let entry = entry.unwrap();
            std::fs::copy(
                entry.path(),
                dir.path().join("views").join(entry.file_name()),
            )
            .unwrap();
        }
        let mut state = test_state(dir.path(), SQL_MAX_CONCURRENCY);
        state.tables = Arc::new(tables);
        for (table, rows) in facts["tables"].as_object().unwrap() {
            for (i, row) in rows.as_array().unwrap().iter().enumerate() {
                let mut row = row.clone();
                row["table"] = json!(table);
                state
                    .store
                    .put_entity(&format!("fixture:{table}:{i}"), &row.to_string())
                    .unwrap();
            }
        }
        if let Some(logs) = facts["poiLogs"].as_array() {
            for raw in logs {
                let log: crate::rpc::Log = serde_json::from_value(raw.clone()).unwrap();
                let mut row = registry.decode(&log).unwrap().unwrap();
                row.block_timestamp = raw["block_timestamp"].as_u64().unwrap();
                assert_eq!(row.table, "subgraph_service__p_o_i_presented");
                state
                    .store
                    .put_entity(
                        &Store::entity_key(row.block_number, row.log_index),
                        &row.to_json().to_string(),
                    )
                    .unwrap();
            }
        }
        // Synthetic fresh checkpoint for admission only. The recorded query selects
        // entity fields, not _meta; this does not claim historical header parity.
        let now = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_secs();
        state
            .store
            .set_meta("last_block", &block.to_string())
            .unwrap();
        state
            .store
            .set_block_hash(block, &format!("0x{}", "11".repeat(32)))
            .unwrap();
        state.store.set_block_timestamp(block, now).unwrap();
        (dir, state)
    }

    #[cfg(feature = "graph")]
    #[tokio::test]
    async fn recorded_network_allocation_query_matches_reference_through_http() {
        let facts: Value = serde_json::from_str(include_str!(
            "../tests/fixtures/network-clients/legacy-allocation-facts.json"
        ))
        .unwrap();
        let reference: Value = serde_json::from_str(include_str!(
            "../tests/fixtures/network-nest/validation/legacy-allocations-reference.json"
        ))
        .unwrap();
        let (_dir, state) = recorded_network_fixture(&facts, 200_000_000);
        let response = graph_ask("/graphql", reference["query"].as_str().unwrap(), state).await;
        assert!(response.get("errors").is_none(), "{response}");
        assert_eq!(response["data"], reference["response"]["data"]);
        assert_eq!(response["data"]["active"].as_array().unwrap().len(), 2);
        assert_eq!(response["data"]["closed"].as_array().unwrap().len(), 2);
    }

    #[cfg(feature = "graph")]
    #[tokio::test]
    async fn recorded_horizon_allocations_and_poi_logs_match_reference_through_http() {
        let facts: Value = serde_json::from_str(include_str!(
            "../tests/fixtures/network-clients/horizon-allocation-facts.json"
        ))
        .unwrap();
        let reference: Value = serde_json::from_str(include_str!(
            "../tests/fixtures/network-nest/validation/horizon-allocations-reference.json"
        ))
        .unwrap();
        assert_eq!(facts["poiLogs"].as_array().unwrap().len(), 2);
        // This recorded number-scoped query must also work when its block is not a checkpoint.
        let (_dir, state) = recorded_network_fixture(&facts, 506_000_001);
        assert!(state.store.get_block_hash(506_000_000).unwrap().is_none());
        let response = graph_ask(
            "/graphql",
            reference["query"].as_str().unwrap(),
            state.clone(),
        )
        .await;
        assert!(response.get("errors").is_none(), "{response}");
        assert_eq!(response["data"], reference["response"]["data"]);
        assert_eq!(response["data"]["active"].as_array().unwrap().len(), 2);
        assert_eq!(response["data"]["closed"].as_array().unwrap().len(), 2);
        let rewards: Value = serde_json::from_str(include_str!(
            "../tests/fixtures/network-nest/validation/horizon-reward-distribution-reference.json"
        ))
        .unwrap();
        let response = graph_ask("/graphql", rewards["query"].as_str().unwrap(), state).await;
        assert!(response.get("errors").is_none(), "{response}");
        assert_eq!(response["data"], rewards["response"]["data"]);
    }

    #[cfg(feature = "graph")]
    #[tokio::test]
    async fn every_captured_network_document_binds_through_http() {
        let facts: Value = serde_json::from_str(include_str!(
            "../tests/fixtures/network-clients/genesis-facts.json"
        ))
        .unwrap();
        let (_dir, state) = recorded_network_fixture(&facts, 42_460_000);
        let variables = json!({
            "indexer":"0x1111111111111111111111111111111111111111",
            "receiver":"0x1111111111111111111111111111111111111111",
            "payer":"0x2222222222222222222222222222222222222222",
            "collector":"0x3333333333333333333333333333333333333333",
            "closedAtThreshold":1789650000,"thawEndTimestamp":"1789650000",
            "first":200,"last":"","minBalance":"100000000000000000","block":null,
            "allocation_ids":[],"allocationIds":[],
            "allocation":"0x1111111111111111111111111111111111111111",
            "status":"Active","lastId":"","dataService":"0x3333333333333333333333333333333333333333",
            "epochs":[1],"closedAtEpochThreshold":1,"subgraphDeploymentId":"0x00",
            "subgraphs":[],"ipfsHash":"QmExample","disputableEpoch":1,
            "minimumQueryFeesCollected":"0","deployments":[],"minimumAllocation":0,"zeroPOI":"0x00"
        });
        let corpus =
            PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("tests/fixtures/network-clients");
        let mut count = 0;
        for file in std::fs::read_dir(corpus).unwrap() {
            let path = file.unwrap().path();
            if path.extension().and_then(|s| s.to_str()) != Some("graphql")
                || path.file_name().unwrap() == "schema.graphql"
            {
                continue;
            }
            let document = std::fs::read_to_string(&path).unwrap();
            // This corpus checks document compatibility, not elapsed freshness. Keep the synthetic
            // head current for each request: on CI the preceding documents can take minutes, and one
            // alone over 60 seconds unoptimised, which is why the fixture's limit is 600.
            state
                .store
                .set_block_timestamp(42_460_000, crate::metrics::now_unix())
                .unwrap();
            let response =
                graph_ask_variables("/graphql", &document, variables.clone(), state.clone()).await;
            assert!(
                response.get("errors").is_none(),
                "{}: {response}",
                path.display()
            );
            assert!(
                response.get("data").is_some(),
                "{}: {response}",
                path.display()
            );
            count += 1;
        }
        assert_eq!(count, 21);
        // Most entities do not exist at genesis. This verifies real SQL binding,
        // not complete-history semantics or current-head client compatibility.
    }

    #[cfg(feature = "graph")]
    #[tokio::test]
    async fn recorded_signers_match_the_reference_through_http() {
        let facts: Value = serde_json::from_str(include_str!(
            "../tests/fixtures/network-clients/signer-facts.json"
        ))
        .unwrap();
        let reference: Value = serde_json::from_str(include_str!(
            "../tests/fixtures/network-nest/validation/signers-reference.json"
        ))
        .unwrap();
        let (_dir, state) = recorded_network_fixture(&facts, 502_165_911);
        let response = graph_ask(
            "/graphql",
            reference["referenceQuery"].as_str().unwrap(),
            state.clone(),
        )
        .await;
        assert!(response.get("errors").is_none(), "{response}");
        assert_eq!(response["data"], reference["reference"]);
        assert_eq!(response["data"]["signers"].as_array().unwrap().len(), 9);
        let signer = "0x7d14ae5f20cc2f6421317386aa8e79e8728353d9";
        let document = include_str!("../tests/fixtures/network-clients/signers_by_payer.graphql");
        for (deadline, last, expected) in [
            ("1781024226", "", json!([])),
            ("1781024227", "", json!([{ "id": signer }])),
            ("1781024227", signer, json!([])),
        ] {
            let response = graph_ask_variables(
                "/graphql",
                document,
                json!({"payer":signer,"first":1000,"last":last,
                    "thawEndTimestamp":deadline,"block":{"number":502165911}}),
                state.clone(),
            )
            .await;
            assert!(response.get("errors").is_none(), "{response}");
            assert_eq!(response["data"]["signers"], expected);
        }
    }

    #[cfg(feature = "graph")]
    #[tokio::test]
    async fn real_redemption_matches_the_reference_and_tap_document_through_http() {
        let facts: Value = serde_json::from_str(include_str!(
            "../tests/fixtures/network-clients/redemption-facts.json"
        ))
        .unwrap();
        let expected = &facts["reference"];
        let (_dir, state) = recorded_network_fixture(&facts, 506_000_000);
        let query = "{ paymentsEscrowTransactions(block:{number:506000000}) { id allocationId timestamp amount collector transactionGroupId type payer { id } receiver { id } escrowAccount { id } } }";
        let full = graph_ask("/graphql", query, state.clone()).await;
        assert!(full.get("errors").is_none(), "{full}");
        assert_eq!(full["data"]["paymentsEscrowTransactions"], *expected);
        let variables = json!({"payer":expected[0]["payer"]["id"],
            "receiver":expected[0]["receiver"]["id"],
            "allocationIds":[expected[0]["allocationId"]]});
        let document = include_str!(
            "../tests/fixtures/network-clients/payments_escrow_transactions_redeem.graphql"
        );
        let response = graph_ask_variables("/graphql", document, variables, state).await;
        assert!(response.get("errors").is_none(), "{response}");
        assert_eq!(
            response["data"]["paymentsEscrowTransactions"],
            json!([{
                "id":expected[0]["id"], "allocationId":expected[0]["allocationId"],
                "timestamp":expected[0]["timestamp"]
            }])
        );
    }

    #[cfg(feature = "graph")]
    #[tokio::test]
    async fn network_rust_allocation_pages_keep_the_first_page_snapshot_when_the_tip_advances() {
        let source = std::path::PathBuf::from(env!("CARGO_MANIFEST_DIR"))
            .join("tests/fixtures/network-nest");
        let config = crate::config::Config::load(&source).unwrap();
        let mut tables = crate::registry::from_nest(&source, &config)
            .unwrap()
            .schema();
        tables.extend(crate::calls::schema(&config.calls, true));
        let dir = tempfile::tempdir().unwrap();
        std::fs::create_dir_all(dir.path().join("graph")).unwrap();
        std::fs::create_dir_all(dir.path().join("views")).unwrap();
        std::fs::copy(
            source.join("graph/schema.graphql"),
            dir.path().join("graph/schema.graphql"),
        )
        .unwrap();
        std::fs::write(
            dir.path().join("graph/history.toml"),
            "version = 1\nfirst_block = 10\nmax_head_age_seconds = 600\n",
        )
        .unwrap();
        for entry in std::fs::read_dir(source.join("views")).unwrap() {
            let entry = entry.unwrap();
            std::fs::copy(
                entry.path(),
                dir.path().join("views").join(entry.file_name()),
            )
            .unwrap();
        }
        let mut state = test_state(dir.path(), SQL_MAX_CONCURRENCY);
        state.tables = Arc::new(tables);
        let indexer = format!("0x{}", "11".repeat(20));
        let deployment = format!("0x{}", "22".repeat(32));
        let ids = ["33", "44", "55"].map(|s| format!("0x{}", s.repeat(20)));
        let hash = format!("0x{}", "aa".repeat(32));
        let now = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_secs();
        let deposit = json!({"table":"staking_legacy__stake_deposited","block_number":10,
            "block_hash":hash,"block_timestamp":now,"log_index":0,"indexer":indexer,"tokens":"100"});
        state
            .store
            .put_entity(&Store::entity_key(10, 0), &deposit.to_string())
            .unwrap();
        let allocation = |id: &str, block: u64, log: u64, hash: &str| {
            json!({
                "table":"staking_legacy__allocation_created","block_number":block,"block_hash":hash,
                "block_timestamp":now,"log_index":log,"indexer":indexer,"allocationID":id,
                "subgraphDeploymentID":deployment,"tokens":"10","epoch":"1"
            })
        };
        for (i, id) in ids[..2].iter().enumerate() {
            state
                .store
                .put_entity(
                    &Store::entity_key(20, i as u64),
                    &allocation(id, 20, i as u64, &hash).to_string(),
                )
                .unwrap();
        }
        state.store.set_block_hash(20, &hash).unwrap();
        state.store.set_block_timestamp(20, now).unwrap();
        state.store.set_meta("last_block", "20").unwrap();
        let document = include_str!("../tests/fixtures/network-clients/allocations.graphql");
        let mut variables =
            json!({"indexer":indexer,"closedAtThreshold":0,"block":null,"first":1,"last":""});
        let first =
            graph_ask_variables("/graphql", document, variables.clone(), state.clone()).await;
        assert!(first.get("errors").is_none(), "{first}");
        assert_eq!(first["data"]["allocations"].as_array().unwrap().len(), 1);
        assert_eq!(first["data"]["allocations"][0]["id"], ids[0]);
        assert_eq!(first["data"]["allocations"][0]["createdAtEpoch"], 1);
        assert_eq!(first["data"]["allocations"][0]["allocatedTokens"], "10");
        assert_eq!(first["data"]["meta"]["block"]["hash"], hash);
        let next_hash = format!("0x{}", "bb".repeat(32));
        state
            .store
            .put_entity(
                &Store::entity_key(30, 0),
                &allocation(&ids[2], 30, 0, &next_hash).to_string(),
            )
            .unwrap();
        state.store.set_block_hash(30, &next_hash).unwrap();
        state
            .store
            .set_block_timestamp(30, crate::metrics::now_unix())
            .unwrap();
        state.store.set_meta("last_block", "30").unwrap();
        variables["last"] = json!(ids[0]);
        variables["first"] = json!(100);
        variables["block"] = json!({"hash":hash});
        let second =
            graph_ask_variables("/graphql", document, variables.clone(), state.clone()).await;
        assert!(second.get("errors").is_none(), "{second}");
        assert_eq!(second["data"]["allocations"].as_array().unwrap().len(), 1);
        assert_eq!(second["data"]["allocations"][0]["id"], ids[1]);
        assert_eq!(second["data"]["meta"]["block"]["number"], 20);
        variables["block"] = serde_json::Value::Null;
        // Prior queries may take longer than the fixture's freshness window on a busy runner.
        // Refresh admission metadata only; block hashes and allocation history remain fixed.
        state
            .store
            .set_block_timestamp(30, crate::metrics::now_unix())
            .unwrap();
        let latest = graph_ask_variables("/graphql", document, variables, state).await;
        assert!(latest.get("errors").is_none(), "{latest}");
        assert_eq!(latest["data"]["allocations"].as_array().unwrap().len(), 2);
        assert_eq!(latest["data"]["meta"]["block"]["number"], 30);
    }

    /// The network dialect lets an explicit `block: null` select the head, as the Rust monitor's
    /// first page sends it.
    #[cfg(feature = "graph")]
    #[tokio::test]
    async fn graph_meta_accepts_a_null_block_as_the_head() {
        let (_d, state) = graph_fixture();
        let answer = graph_ask(
            "/graphql",
            "{ _meta(block: null) { block { number } } }",
            state,
        )
        .await;
        assert_eq!(
            answer["data"]["_meta"]["block"]["number"], 23_456_789,
            "{answer}"
        );
    }

    #[cfg(feature = "graph")]
    #[tokio::test]
    async fn a_derived_list_can_page_and_select_its_to_one_relation() {
        let (_d, state) = graph_fixture();
        let answer = graph_ask(
            "/graphql",
            r#"{
            pool(id: "0xaaa") {
                swaps(first: 1, skip: 1, orderBy: id) { id pool { id liquidity } }
                first: swaps(first: 1, where: { amount_gt: "4" }) { id }
            }
        }"#,
            state,
        )
        .await;
        assert_eq!(
            answer,
            json!({"data": {"pool": {
                "swaps": [{"id": "s2", "pool": {"id": "0xaaa", "liquidity": "42"}}],
                "first": [{"id": "s1"}]
            }}})
        );
    }

    /// The Horizon escrow monitor asks for a payer's filtered, paged signer list below each account.
    /// This runs the actual GraphQL route over views, rather than merely asserting the compiler's SQL:
    /// the response must contain a list beneath `payer`, and only the authorised first signer.
    #[cfg(feature = "graph")]
    #[tokio::test]
    async fn an_escrow_payers_filtered_signers_answer_in_one_graphql_request() {
        let d = tempfile::tempdir().unwrap();
        std::fs::create_dir_all(d.path().join("graph")).unwrap();
        std::fs::write(
            d.path().join("graph/schema.graphql"),
            r#"
type PaymentsEscrowAccount @entity {
  id: ID!
  balance: BigInt!
  payer: Payer!
}
type Payer @entity {
  id: ID!
  signers: [Signer!]! @derivedFrom(field: "payer")
}
type Signer @entity {
  id: ID!
  payer: Payer!
  isAuthorized: Boolean!
}
"#,
        )
        .unwrap();
        std::fs::create_dir_all(d.path().join("views")).unwrap();
        std::fs::write(
            d.path().join("views/payments_escrow_account.sql"),
            "CREATE VIEW payments_escrow_account AS \
             SELECT '0xaccount' AS id, 101 AS balance, '0xpayer' AS payer;\n",
        )
        .unwrap();
        std::fs::write(
            d.path().join("views/payer.sql"),
            "CREATE VIEW payer AS SELECT '0xpayer' AS id;\n",
        )
        .unwrap();
        std::fs::write(
            d.path().join("views/signer.sql"),
            "CREATE VIEW signer AS \
             SELECT '0xsigner-a' AS id, '0xpayer' AS payer, true AS \"isAuthorized\" \
             UNION ALL SELECT '0xsigner-b', '0xpayer', true \
             UNION ALL SELECT '0xaaa-unauthorised', '0xpayer', false \
             UNION ALL SELECT '0xaaa-other-payer', '0xother', true;\n",
        )
        .unwrap();
        let state = test_state(d.path(), SQL_MAX_CONCURRENCY);
        let body = graph_ask(
            "/graphql",
            r#"{
              paymentsEscrowAccounts(where: { balance_gt: "100" }) {
                id balance payer {
                  id
                  signers(first: 1, where: { isAuthorized: true }) { id }
                  last: signers(first: 1, orderDirection: desc, where: { isAuthorized: true }) { id }
                }
              }
            }"#,
            state,
        )
        .await;
        assert_eq!(
            body,
            serde_json::json!({
                "data": {
                    "paymentsEscrowAccounts": [{
                        "id": "0xaccount", "balance": "101",
                        "payer": {"id": "0xpayer", "signers": [{"id": "0xsigner-a"}],
                                  "last": [{"id": "0xsigner-b"}]}
                    }]
                }
            }),
            "the nested signer filter and page limit must be applied before GraphQL shapes payer"
        );
    }

    /// RFC-0053 S1 (#1265): a client can introspect a nest over HTTP.
    ///
    /// This is the assertion that moves the compatibility surface from "generated" to "reachable".
    /// The renderer is diffed against a real graph-node in `tests/graph_schema_golden.rs`; this one
    /// proves a client can actually fetch it, at the subgraph URL shape as well as the plain one, and
    /// that anything which is not introspection is refused inside the Graph error envelope rather
    /// than as a bare status a client cannot read.
    #[cfg(feature = "graph")]
    #[tokio::test]
    async fn a_client_can_introspect_the_nest_over_http() {
        // Two requests below are built inline rather than through `graph_ask`, because they carry a
        // `variables` object the helper does not take.
        use tower::ServiceExt;

        let (d, state) = graph_fixture();
        let _ = &d;
        let ask = graph_ask;
        for uri in ["/graphql", "/subgraphs/id/QmWhatever"] {
            // The query asks for what this test goes on to read: the response is now projected to the
            // selection, so relying on unselected keys arriving anyway would be relying on a bug.
            let body = ask(
                uri,
                "{ __schema { types { name fields { name } } } }",
                state.clone(),
            )
            .await;
            let types = body["data"]["__schema"]["types"]
                .as_array()
                .unwrap_or_else(|| panic!("{uri} returned no types: {body}"));
            let names: Vec<&str> = types.iter().filter_map(|t| t["name"].as_str()).collect();
            for want in [
                "Pool",
                "Pool_filter",
                "Pool_orderBy",
                "Query",
                "BigInt",
                "_Meta_",
            ] {
                assert!(
                    names.contains(&want),
                    "{uri}: {want} missing from {names:?}"
                );
            }
            let query = types.iter().find(|t| t["name"] == "Query").unwrap();
            let roots: Vec<&str> = query["fields"]
                .as_array()
                .unwrap()
                .iter()
                .filter_map(|f| f["name"].as_str())
                .collect();
            assert!(
                roots.contains(&"pool") && roots.contains(&"pools") && roots.contains(&"_meta"),
                "{uri}: root fields were {roots:?}"
            );
        }

        // `_meta` is answered from the nest's own head and needs no view.
        let body = graph_ask(
            "/graphql",
            "{ _meta { block { number } hasIndexingErrors } }",
            state.clone(),
        )
        .await;
        assert_eq!(
            body["data"]["_meta"]["hasIndexingErrors"],
            serde_json::json!(false),
            "a nest runs no mapping, so it has no indexing error to report: {body}"
        );
        // The head is the nest's real one. Asserted because a client uses `_meta.block.number` to
        // decide whether the endpoint is caught up, and a hardcoded null reads as "never indexed".
        assert_eq!(
            body["data"]["_meta"]["block"]["number"],
            serde_json::json!(23_456_789u64),
            "_meta must report the nest's own head: {body}"
        );

        // An entity query compiles and answers (S2, #1266). Two rows, in id order, so this sees a
        // dropped ORDER BY as well as a dropped row.
        //
        // `liquidity` is a **string**. This assertion read `42` until the end-to-end test over indexed
        // data caught it: graph-node sends `BigInt` as `q::Value::String`
        // (`graph/src/data/store/mod.rs:568`), so a JSON number here is a value a client cannot hold
        // above 2^53 and parses differently below it.
        let body = ask("/graphql", "{ pools { id liquidity } }", state.clone()).await;
        assert_eq!(
            body["data"]["pools"],
            serde_json::json!([
                {"id": "0xaaa", "liquidity": "42"},
                {"id": "0xbbb", "liquidity": "7"},
                {"id": "0xccc", "liquidity": "99"},
            ]),
            "a plain collection must answer rows from the nest's view: {body}"
        );

        // And the arguments are not decoration: `first` bounds, `where` filters, and a singular
        // root takes one. Each of these silently returning the unfiltered set is the failure mode
        // that makes a drop-in endpoint worse than no endpoint.
        let body = ask("/graphql", "{ pools(first: 1) { id } }", state.clone()).await;
        assert_eq!(
            body["data"]["pools"],
            serde_json::json!([{"id": "0xaaa"}]),
            "first: 1 must return one row: {body}"
        );
        let body = graph_ask(
            "/graphql",
            r#"{ pools(where: { liquidity_lt: "10" }) { id } }"#,
            state.clone(),
        )
        .await;
        assert_eq!(
            body["data"]["pools"],
            serde_json::json!([{"id": "0xbbb"}]),
            "a where filter must actually filter: {body}"
        );
        let body = graph_ask(
            "/graphql",
            r#"{ pool(id: "0xbbb") { hooks } }"#,
            state.clone(),
        )
        .await;
        assert_eq!(
            body["data"]["pool"],
            serde_json::json!({"hooks": "0xhook2"}),
            "a singular root must answer one object, not a list: {body}"
        );

        // A client-shaped request: a named operation with variables, passed in the request body's
        // `variables` object rather than inlined in the query. Asserted over HTTP because the
        // parser test cannot see whether the handler actually reads that field.
        let res = router(SharedNest::new(state.clone()))
            .oneshot(
                axum::http::Request::builder()
                    .method("POST")
                    .uri("/graphql")
                    .header("content-type", "application/json")
                    .body(axum::body::Body::from(
                        serde_json::json!({
                            "query": "query Pools($n: Int!, $min: BigInt!) \
                                      { pools(first: $n, where: { liquidity_gt: $min }) { id } }",
                            "variables": { "n": 5, "min": "10" },
                        })
                        .to_string(),
                    ))
                    .unwrap(),
            )
            .await
            .unwrap();
        let body: serde_json::Value = serde_json::from_slice(
            &axum::body::to_bytes(res.into_body(), 4 << 20)
                .await
                .unwrap(),
        )
        .unwrap();
        // Both bound variables show: `liquidity_gt: 10` excludes `0xbbb` at 7 and keeps the two above
        // it, and `first: 5` is wide enough not to be what trimmed the list.
        assert_eq!(
            body["data"]["pools"],
            serde_json::json!([{"id": "0xaaa"}, {"id": "0xccc"}]),
            "variables from the request body must bind: {body}"
        );
    }

    /// `block: { number_gte: N }` is answered, not refused (RFC-0053 S3, #1267).
    ///
    /// It is the argument a client sends for read-your-writes: *"execute on the latest block only if the
    /// subgraph has progressed to or past the minimum block number"* (graph-node,
    /// `graph/src/schema/api.rs:1189`). That is a precondition on the head, not a request for a past
    /// state, so a nest can answer it exactly with no history stored - and refusing it refused a query
    /// graph-node answers.
    ///
    /// `number` and `hash` are real time travel and stay refused.
    #[cfg(feature = "graph")]
    #[tokio::test]
    async fn a_block_number_gte_within_the_head_is_answered() {
        let (_d, state) = graph_fixture();
        // The fixture's head is 23,456,789.
        let body = graph_ask(
            "/graphql",
            "{ pools(where: { id: \"0xaaa\" }, block: { number_gte: 1000 }) { id } }",
            state.clone(),
        )
        .await;
        assert_eq!(
            body["data"]["pools"],
            serde_json::json!([{"id": "0xaaa"}]),
            "a head past the minimum answers on the latest block: {body}"
        );

        // The boundary: exactly the head is "progressed to", which the wording makes inclusive.
        let body = graph_ask(
            "/graphql",
            "{ pools(where: { id: \"0xaaa\" }, block: { number_gte: 23456789 }) { id } }",
            state.clone(),
        )
        .await;
        assert_eq!(
            body["data"]["pools"],
            serde_json::json!([{"id": "0xaaa"}]),
            "`to or past` includes the head itself: {body}"
        );

        // One past it is refused, in graph-node's own words
        // (`DeploymentState::block_queryable`, graph/src/data/subgraph/mod.rs:1362).
        let body = graph_ask(
            "/graphql",
            "{ pools(block: { number_gte: 23456790 }) { id } }",
            state.clone(),
        )
        .await;
        assert_eq!(
            body["errors"][0]["message"],
            "subgraph t has only indexed up to block number 23456789 and data for block number \
             23456790 is therefore not yet available",
            "the refusal is graph-node's, word for word: {body}"
        );
        assert!(body["data"].is_null(), "and it answers no rows: {body}");

        // Real time travel stays refused, and by its own name rather than this one.
        for arg in [
            "block: { number: 100 }",
            "block: { hash: \"0xabc\" }",
            "block: { number_gte: 1000, number: 100 }",
        ] {
            let body = graph_ask(
                "/graphql",
                &format!("{{ pools({arg}) {{ id }} }}"),
                state.clone(),
            )
            .await;
            let msg = body["errors"][0]["message"]
                .as_str()
                .unwrap_or_default()
                .to_string();
            assert!(
                msg.contains("block-ranged"),
                "`{arg}` needs a block-ranged store and must say so: {body}"
            );
        }

        // A negative is not a block, and `Int` lets a client send one.
        let body = graph_ask(
            "/graphql",
            "{ pools(block: { number_gte: -1 }) { id } }",
            state,
        )
        .await;
        assert!(
            body["errors"][0]["message"]
                .as_str()
                .unwrap_or_default()
                .contains("must not be negative"),
            "a negative minimum is named: {body}"
        );
    }

    /// `_meta` takes the same `block` argument. Ignoring it answered a past or future block with
    /// today's head.
    #[cfg(feature = "graph")]
    #[tokio::test]
    async fn graph_meta_never_discards_a_block_argument() {
        let (_d, state) = graph_fixture();
        let body = graph_ask(
            "/graphql",
            "{ _meta(block: { number_gte: 23456789 }) { block { number } } }",
            state.clone(),
        )
        .await;
        assert_eq!(
            body["data"]["_meta"]["block"]["number"], 23_456_789,
            "{body}"
        );
        let body = graph_ask(
            "/graphql",
            "{ _meta(block: { number_gte: 23456790 }) { block { number } } }",
            state.clone(),
        )
        .await;
        assert_eq!(
            body["errors"][0]["message"],
            "subgraph t has only indexed up to block number 23456789 and data for block number \
             23456790 is therefore not yet available",
            "{body}"
        );
        for arg in [
            "block: { number: 100 }",
            "block: { hash: \"0xabc\" }",
            "first: 1",
        ] {
            let body = graph_ask(
                "/graphql",
                &format!("{{ _meta({arg}) {{ block {{ number }} }} }}"),
                state.clone(),
            )
            .await;
            assert!(body["errors"].is_array(), "`{arg}` must be refused: {body}");
            assert!(body["data"].is_null(), "`{arg}` answered: {body}");
        }
    }

    #[cfg(feature = "graph")]
    #[test]
    fn graph_results_never_hide_partial_or_malformed_sql_answers() {
        for flag in ["truncated", "degraded", "tip_unavailable"] {
            let mut result = serde_json::json!({"rows": [{"id": "a"}]});
            result[flag] = serde_json::json!(true);
            assert!(graph_result_rows(&result).unwrap_err().contains(flag));
            result[flag] = serde_json::json!(false);
            assert_eq!(graph_result_rows(&result).unwrap().len(), 1);
        }
        assert!(graph_result_rows(&serde_json::json!({})).is_err());
        assert!(graph_result_rows(&serde_json::json!({"rows": [null]})).is_err());
        assert!(graph_result_rows(&serde_json::json!({"rows": [], "error": "busy"})).is_err());
        assert!(graph_result_rows(&serde_json::json!({"rows": []}))
            .unwrap()
            .is_empty());
    }

    /// Null equality is SQL null equality, from a literal and from a variable alike.
    ///
    /// The literal used to parse as the enum `null` and compile to `hooks = 'null'`, which matches rows
    /// whose `hooks` is the four-character string - a filter that quietly selects the wrong rows. The
    /// variable used to be dropped by `filter_map` and re-surfaced as `unbound variable`, which is a
    /// refusal naming the wrong cause.
    #[cfg(feature = "graph")]
    #[tokio::test]
    async fn a_null_filter_value_is_compared_as_null() {
        let (_d, state) = graph_fixture();

        let body = graph_ask(
            "/graphql",
            r#"{ pools(where: { hooks: null }) { id } }"#,
            state.clone(),
        )
        .await;
        assert_eq!(body, json!({"data": {"pools": []}}));
        let closed = graph_ask(
            "/graphql",
            "{ tokens(where: { symbol_not: null }) { id } }",
            state.clone(),
        )
        .await;
        assert_eq!(closed, json!({"data": {"tokens": [{"id": "0xt1"}]}}));

        // The same value through `variables`, which is how a generated client sends it.
        use tower::ServiceExt;
        let res = router(SharedNest::new(state))
            .oneshot(
                axum::http::Request::builder()
                    .method("POST")
                    .uri("/graphql")
                    .header("content-type", "application/json")
                    .body(axum::body::Body::from(
                        serde_json::json!({
                            "query": "query P($h: Bytes) { pools(where: { hooks: $h }) { id } }",
                            "variables": { "h": serde_json::Value::Null },
                        })
                        .to_string(),
                    ))
                    .unwrap(),
            )
            .await
            .unwrap();
        let body: serde_json::Value = serde_json::from_slice(
            &axum::body::to_bytes(res.into_body(), 4 << 20)
                .await
                .unwrap(),
        )
        .unwrap();
        assert_eq!(body, json!({"data": {"pools": []}}));
    }

    /// `operationName` selects among several operations in one document (Jules on #1282).
    ///
    /// The parser kept a single operation and refused the second, and the handler never read
    /// `operationName` at all - so `{"query": "query A {…} query B {…}", "operationName": "B"}`, which
    /// graph-node answers, came back refused. Asserted over HTTP because the finding was about the
    /// handler rather than the parser: a parser test passes with the field still ignored.
    #[cfg(feature = "graph")]
    #[tokio::test]
    async fn the_request_operation_name_selects_among_several_operations() {
        let (_d, state) = graph_fixture();
        use tower::ServiceExt;
        let post = |body: serde_json::Value, state: AppState| async move {
            let res = router(SharedNest::new(state))
                .oneshot(
                    axum::http::Request::builder()
                        .method("POST")
                        .uri("/graphql")
                        .header("content-type", "application/json")
                        .body(axum::body::Body::from(body.to_string()))
                        .unwrap(),
                )
                .await
                .unwrap();
            serde_json::from_slice::<serde_json::Value>(
                &axum::body::to_bytes(res.into_body(), 4 << 20)
                    .await
                    .unwrap(),
            )
            .unwrap()
        };
        let two =
            "query A { pool(id: \"0xaaa\") { hooks } } query B { pool(id: \"0xbbb\") { hooks } }";

        // The named one runs, and it is the named one rather than the first.
        let body = post(
            serde_json::json!({"query": two, "operationName": "B"}),
            state.clone(),
        )
        .await;
        assert_eq!(
            body["data"]["pool"],
            serde_json::json!({"hooks": "0xhook2"}),
            "`operationName` must select operation B: {body}"
        );
        let body = post(
            serde_json::json!({"query": two, "operationName": "A"}),
            state.clone(),
        )
        .await;
        assert_eq!(
            body["data"]["pool"],
            serde_json::json!({"hooks": "0xhook"}),
            "and A when A is asked for, so the answer is not simply the first operation: {body}"
        );

        // graph-node's two refusals, word for word (graph/src/data/query/error.rs:158).
        let body = post(serde_json::json!({"query": two}), state.clone()).await;
        assert_eq!(
            body["errors"][0]["message"], "Operation name required",
            "several operations and no name is a named refusal: {body}"
        );
        let body = post(
            serde_json::json!({"query": two, "operationName": "C"}),
            state.clone(),
        )
        .await;
        assert_eq!(
            body["errors"][0]["message"], "Operation name not found `C`",
            "a name nothing carries is a named refusal: {body}"
        );

        // One named operation and no `operationName` still runs: that is what a client sends when it has
        // one query in the document, and it is the commonest request of all.
        let body = post(
            serde_json::json!({"query": "query Only { pool(id: \"0xaaa\") { hooks } }"}),
            state.clone(),
        )
        .await;
        assert_eq!(
            body["data"]["pool"],
            serde_json::json!({"hooks": "0xhook"}),
            "one named operation needs no name to be selected: {body}"
        );

        // An anonymous operation carries no name, so a name selects nothing - even as the only operation.
        let body = post(
            serde_json::json!({
                "query": "{ pool(id: \"0xaaa\") { hooks } }",
                "operationName": "A",
            }),
            state.clone(),
        )
        .await;
        assert_eq!(
            body["errors"][0]["message"], "Operation name not found `A`",
            "the shorthand operation is anonymous and a name must not match it: {body}"
        );

        // An empty string is how some clients spell "no name". It must not be looked up as a name.
        let body = post(
            serde_json::json!({
                "query": "{ pool(id: \"0xbbb\") { hooks } }",
                "operationName": "",
            }),
            state,
        )
        .await;
        assert_eq!(
            body["data"]["pool"],
            serde_json::json!({"hooks": "0xhook2"}),
            "an empty `operationName` means no name given: {body}"
        );
    }

    /// The introspection document a generated client actually sends is built from fragments, and the
    /// endpoint answers every root field of a mixed operation rather than the first it recognises.
    /// The golden path serves in full on a nest that has never been ported, and the Graph routes say so.
    ///
    /// The serving half of `tests/core_stays_pristine.rs`, here because it has to bind the real router.
    /// Chief's constraint, 2026-09-11: the default and golden path stay the delightful core, and the
    /// Graph dialect is an optional lane. This asserts the lane is **inert** rather than merely unused -
    /// a route that answers an empty success on an unported nest would look like a working endpoint
    /// serving no data, which is the silent substitution this whole surface keeps producing.
    #[tokio::test]
    async fn the_core_serves_with_no_graph_schema_present() {
        let (d, state) = graph_fixture();
        // The fixture writes `graph/schema.graphql` so the dialect tests have something to read. The
        // golden path must not need it, so take it away.
        std::fs::remove_file(d.path().join("graph/schema.graphql")).expect("remove the schema");

        use tower::ServiceExt;
        // Every golden-path surface still answers.
        for uri in [
            "/",
            "/health",
            "/ready",
            "/tables",
            "/schema",
            "/entities",
            "/ipfs/gave-up",
            "/sql?q=SELECT%201",
        ] {
            let res = router(SharedNest::new(state.clone()))
                .oneshot(
                    axum::http::Request::builder()
                        .uri(uri)
                        .body(axum::body::Body::empty())
                        .unwrap(),
                )
                .await
                .expect("request");
            assert!(
                res.status().is_success(),
                "{uri} must serve on a nest that was never ported, got {}",
                res.status()
            );
        }

        // A default build has no GraphQL routes at all (#1440, RFC-0060 §5.6's deletion test).
        #[cfg(not(feature = "graph"))]
        for uri in [
            "/graphql",
            "/subgraphs/id/QmWhatever",
            "/subgraphs/name/a/b",
        ] {
            let res = router(SharedNest::new(state.clone()))
                .oneshot(
                    axum::http::Request::builder()
                        .method("POST")
                        .uri(uri)
                        .header("content-type", "application/json")
                        .body(axum::body::Body::from(r#"{"query":"{ pools { id } }"}"#))
                        .unwrap(),
                )
                .await
                .expect("request");
            assert_eq!(
                res.status(),
                StatusCode::NOT_FOUND,
                "{uri} must not exist in a default build"
            );
        }

        // In a `graph` build the lane refuses by name rather than answering an empty success.
        #[cfg(feature = "graph")]
        for uri in ["/graphql", "/subgraphs/id/QmWhatever"] {
            let body = graph_ask(uri, "{ pools { id } }", state.clone()).await;
            let msg = body["errors"][0]["message"]
                .as_str()
                .unwrap_or_else(|| panic!("{uri} answered without an error: {body}"));
            assert!(
                msg.contains("carries no Graph schema") && msg.contains("port-emit"),
                "{uri} must name the artefact and how to get one, got {msg:?}"
            );
            assert!(
                body.get("data").is_none_or(|d| d.is_null()),
                "{uri} must not answer data it cannot have: {body}"
            );
        }
    }

    /// `_meta` is narrowed by the selection like every other root.
    ///
    /// It used to be inserted whole, so `{ _meta { block { number } } }` answered with `deployment`
    /// and `hasIndexingErrors` attached and every alias ignored. Every expectation here was probed
    /// against the live reference endpoint rather than read off the spec.
    ///
    /// The one deliberate divergence: graph-node cannot alias `block` itself -
    /// `{ _meta { b: block { number } } }` answers `internal error resolving _Block_.block: expected
    /// prefetched result, but found nothing`. Projecting uniformly answers it, which is strictly more
    /// useful and which no client can be relying on the absence of.
    #[cfg(feature = "graph")]
    #[tokio::test]
    async fn the_meta_root_is_narrowed_to_the_selection() {
        let (_d, state) = graph_fixture();
        let ask = graph_ask;

        // Narrow: exactly the one field, nothing else. The reference answers
        // `{"_meta":{"block":{"number":25953333}}}` for this shape.
        let body = ask("/graphql", "{ _meta { block { number } } }", state.clone()).await;
        assert_eq!(
            body["data"]["_meta"],
            serde_json::json!({"block": {"number": 23_456_789u64}}),
            "_meta must carry only what was selected: {body}"
        );

        // Complete: every declared field of `_Meta_` and `_Block_`. The timestamp is the fixture
        // head's, not `is_some()` (#1289).
        let body = graph_ask(
            "/graphql",
            "{ _meta { block { number hash timestamp parentHash } deployment hasIndexingErrors } }",
            state.clone(),
        )
        .await;
        assert_eq!(
            body["data"]["_meta"]["block"],
            serde_json::json!({
                "number": 23_456_789u64,
                "hash": "0xhead",
                "timestamp": 1_700_000_000u64,
                "parentHash": "0xparent",
            }),
            "the block must carry the head, its hash and its parent's: {body}"
        );
        assert_eq!(
            body["data"]["_meta"]["hasIndexingErrors"],
            serde_json::json!(false),
            "a nest runs no mapping, so it has no indexing error: {body}"
        );

        // Aliases, at every depth. The reference honours the root, the leaf and the scalars.
        let body = graph_ask(
            "/graphql",
            "{ m: _meta { b: block { n: number h: hash } d: deployment e: hasIndexingErrors } }",
            state.clone(),
        )
        .await;
        assert_eq!(
            body["data"]["m"],
            serde_json::json!({
                "b": {"n": 23_456_789u64, "h": "0xhead"},
                "d": body["data"]["m"]["d"].clone(),
                "e": false,
            }),
            "every alias must become the response key: {body}"
        );
        // `deployment` is `String!`, and `nid` is `None` for a nest mounted by alias, so this is a
        // real production shape rather than a fixture quirk. Null here is a document a strict client
        // rejects before it reads a single row.
        assert!(
            body["data"]["m"]["d"].is_string(),
            "`deployment` is `String!` and must never answer null: {body}"
        );

        // A field the type does not have answers null rather than leaking the whole object.
        let body = ask("/graphql", "{ _meta { nonesuch } }", state.clone()).await;
        assert_eq!(
            body["data"]["_meta"],
            serde_json::json!({"nonesuch": serde_json::Value::Null}),
            "an unknown selected field must not widen the answer: {body}"
        );
    }

    #[cfg(feature = "graph")]
    #[tokio::test]
    async fn a_fragment_document_and_a_mixed_operation_answer_over_http() {
        let (d, state) = graph_fixture();
        let _ = &d;
        let ask = graph_ask;

        // The introspection document a generated client actually sends, fragments and all. Refusing
        // fragments refused the one request that has to work before any other can: a client will not
        // send a useful query until it has validated against the schema it fetched this way.
        let body = graph_ask(
            "/graphql",
            r#"query IntrospectionQuery {
                 __schema {
                   queryType { name }
                   types { ...FullType }
                 }
               }
               fragment FullType on __Type {
                 kind
                 name
                 fields(includeDeprecated: true) { name }
               }"#,
            state.clone(),
        )
        .await;
        assert!(
            body["errors"].is_null(),
            "a fragment-based introspection document must not be refused: {body}"
        );
        let names: Vec<&str> = body["data"]["__schema"]["types"]
            .as_array()
            .unwrap_or_else(|| panic!("no types: {body}"))
            .iter()
            .filter_map(|t| t["name"].as_str())
            .collect();
        for want in ["Pool", "Pool_filter", "Token", "_Meta_", "_Log_"] {
            assert!(names.contains(&want), "{want} missing from {names:?}");
        }

        // A response carries the selected fields and no others. The introspection branches used to hand
        // back the whole rendered document whatever was asked for, so a client selecting `name` received
        // all eight keys of every type.
        let body = ask("/graphql", "{ __schema { types { name } } }", state.clone()).await;
        let first = body["data"]["__schema"]["types"][0]
            .as_object()
            .unwrap_or_else(|| panic!("no types: {body}"));
        assert_eq!(
            first.keys().cloned().collect::<Vec<_>>(),
            ["name"],
            "a projected response carries only what was selected"
        );
        let top = body["data"]["__schema"].as_object().unwrap();
        assert_eq!(
            top.keys().cloned().collect::<Vec<_>>(),
            ["types"],
            "including at the `__schema` level"
        );

        // A field `__Type` does not have is refused by name, as graph-node's validation refuses it, not
        // answered with a null nobody can tell from a real one.
        let body = graph_ask(
            "/graphql",
            "{ __schema { types { name nope } } }",
            state.clone(),
        )
        .await;
        assert!(body["data"].is_null(), "{body}");
        assert_eq!(
            body["errors"][0]["message"], "`nope` is not a field of `__Type`",
            "{body}"
        );

        // An alias on an introspection field answers under the alias, and projection follows it down.
        let body = graph_ask(
            "/graphql",
            r#"{ s: __schema { t: types { n: name } } }"#,
            state.clone(),
        )
        .await;
        assert_eq!(
            body["data"]["s"]["t"][0],
            serde_json::json!({"n": "String"}),
            "aliases survive projection at every level: {body}"
        );

        // `__type` is projected too.
        let body = graph_ask(
            "/graphql",
            r#"{ __type(name: "Pool") { name } }"#,
            state.clone(),
        )
        .await;
        assert_eq!(
            body["data"]["__type"],
            serde_json::json!({"name": "Pool"}),
            "{body}"
        );

        // Introspection is not an exclusive mode, it is two more root fields. Returning early on the
        // first one meant a mixed operation came back without its data fields at all - present in the
        // request, silently missing from the response.
        let body = graph_ask(
            "/graphql",
            r#"{ __schema { queryType { name } } __type(name: "Pool") { name } _meta { hasIndexingErrors } pools(first: 1) { id } }"#,
            state.clone(),
        )
        .await;
        assert_eq!(
            body["data"]["__schema"]["queryType"]["name"], "Query",
            "the schema root still answers in a mixed operation: {body}"
        );
        assert_eq!(body["data"]["__type"]["name"], "Pool", "{body}");
        assert_eq!(body["data"]["_meta"]["hasIndexingErrors"], false, "{body}");
        assert_eq!(
            body["data"]["pools"],
            serde_json::json!([{"id": "0xaaa"}]),
            "and so does the data root, which used to be dropped entirely: {body}"
        );

        // Introspection is chosen from the parsed root field name, not by searching the text. A
        // filter value that happens to read `__schema` is an ordinary string, and answering it with
        // the schema document loses the caller's query entirely (Jules on #1282).
        let body = graph_ask(
            "/graphql",
            r#"{ pools(where: { hooks: "__schema" }) { id } }"#,
            state.clone(),
        )
        .await;
        assert!(
            body["data"]["__schema"].is_null(),
            "a string literal must not route to introspection: {body}"
        );
        assert_eq!(
            body["data"]["pools"],
            serde_json::json!([]),
            "it must run as the filter it is, and no pool has that hook: {body}"
        );
    }

    /// `__schema.queryType` is a whole `__Type`, as is every type a field or argument points at (#1941).
    /// The document stores those as `{kind, name}` references, so selecting `fields` through one
    /// answered a silent `null`.
    #[cfg(feature = "graph")]
    #[tokio::test]
    async fn an_introspection_type_reference_answers_as_the_type_it_names() {
        let (_d, state) = graph_fixture();
        let body = graph_ask(
            "/graphql",
            "{ __schema { __typename queryType { name kind fields { name } } } }",
            state.clone(),
        )
        .await;
        assert!(body["errors"].is_null(), "{body}");
        let q = &body["data"]["__schema"]["queryType"];
        assert_eq!(q["kind"], "OBJECT", "{body}");
        let names: Vec<&str> = q["fields"]
            .as_array()
            .unwrap_or_else(|| panic!("queryType.fields answered null: {body}"))
            .iter()
            .filter_map(|f| f["name"].as_str())
            .collect();
        for want in ["pool", "pools", "token", "_meta", "_logs"] {
            assert!(names.contains(&want), "{want} missing from {names:?}");
        }
        assert_eq!(body["data"]["__schema"]["__typename"], "__Schema", "{body}");

        // Through a field's type, two wrappers down: `Pool.token0: Token!`.
        let body = graph_ask(
            "/graphql",
            r#"{ __type(name: "Pool") { ofType { name } specifiedByURL
                 fields { name type { kind ofType { name fields { name } } } } } }"#,
            state.clone(),
        )
        .await;
        assert!(body["errors"].is_null(), "{body}");
        let t = &body["data"]["__type"];
        assert!(t["ofType"].is_null(), "a named type has no ofType: {body}");
        assert!(t["specifiedByURL"].is_null(), "{body}");
        let token0 = t["fields"]
            .as_array()
            .unwrap()
            .iter()
            .find(|f| f["name"] == "token0")
            .unwrap_or_else(|| panic!("no token0: {body}"));
        assert_eq!(token0["type"]["kind"], "NON_NULL", "{body}");
        assert_eq!(token0["type"]["ofType"]["name"], "Token", "{body}");
        let token_fields: Vec<&str> = token0["type"]["ofType"]["fields"]
            .as_array()
            .unwrap_or_else(|| panic!("Token.fields answered null through a reference: {body}"))
            .iter()
            .filter_map(|f| f["name"].as_str())
            .collect();
        assert!(token_fields.contains(&"symbol"), "{token_fields:?}");

        // Fields the introspection schema declares and the stored document leaves out.
        let body = graph_ask(
            "/graphql",
            "{ __schema { description directives { name isRepeatable args { name isDeprecated deprecationReason } } } }",
            state.clone(),
        )
        .await;
        assert!(body["errors"].is_null(), "{body}");
        assert!(body["data"]["__schema"]["description"].is_null(), "{body}");
        let skip = &body["data"]["__schema"]["directives"][0];
        assert_eq!(
            skip,
            &serde_json::json!({"name": "skip", "isRepeatable": false,
                "args": [{"name": "if", "isDeprecated": false, "deprecationReason": null}]}),
            "{body}"
        );

        let body = graph_ask("/graphql", "{ __schema { queryType { nope } } }", state).await;
        assert_eq!(
            body["errors"][0]["message"], "`nope` is not a field of `__Type`",
            "{body}"
        );
    }

    /// A collection the schema declares but no view serves is refused by name (#1941), not with the
    /// engine's "table does not exist" and a hint about some other table.
    #[cfg(feature = "graph")]
    #[tokio::test]
    async fn a_collection_with_no_view_is_refused_by_name() {
        let dir = tempfile::tempdir().unwrap();
        std::fs::create_dir_all(dir.path().join("graph")).unwrap();
        std::fs::create_dir_all(dir.path().join("views")).unwrap();
        std::fs::write(
            dir.path().join("graph/schema.graphql"),
            "type Pool @entity { id: ID! token: Token! }\ntype Token @entity { id: ID! }",
        )
        .unwrap();
        std::fs::write(
            dir.path().join("views/pool.sql"),
            "CREATE VIEW pool AS SELECT 'p' AS id, 't' AS token;",
        )
        .unwrap();
        let state = test_state(dir.path(), SQL_MAX_CONCURRENCY);

        let body = graph_ask("/graphql", "{ tokens { id } }", state.clone()).await;
        assert_eq!(
            body["errors"][0]["message"],
            "`tokens` is not served by this nest: it has no `token` view",
            "{body}"
        );
        let body = graph_ask("/graphql", r#"{ token(id: "t") { id } }"#, state.clone()).await;
        assert_eq!(
            body["errors"][0]["message"],
            "`token` is not served by this nest: it has no `token` view",
            "{body}"
        );
        // Reached through a relation, it is the entity that is not served.
        let body = graph_ask("/graphql", "{ pools { token { id } } }", state.clone()).await;
        assert_eq!(
            body["errors"][0]["message"],
            "`Token` is not served by this nest: it has no `token` view",
            "{body}"
        );
        // And the collection that has a view still answers.
        let body = graph_ask("/graphql", "{ pools { id } }", state).await;
        assert_eq!(body["data"]["pools"], json!([{"id": "p"}]), "{body}");
    }

    /// The S0 schemas whose descriptions hold colons (#1948): every request to the nest panicked in the
    /// handler and the connection closed with no response.
    #[cfg(feature = "graph")]
    #[tokio::test]
    async fn a_schema_whose_descriptions_hold_colons_answers() {
        let dir = tempfile::tempdir().unwrap();
        std::fs::create_dir_all(dir.path().join("graph")).unwrap();
        std::fs::create_dir_all(dir.path().join("views")).unwrap();
        std::fs::write(
            dir.path().join("graph/schema.graphql"),
            concat!(
                "\"Examples: AMM protocol fee\"\n",
                "type Node @entity {\n",
                "  id: ID!\n",
                "  \"the node's URL, e.g. http://mynode.com:3000\"\n",
                "  url: String!\n",
                "  \"mapping (uint32 => Stake)\"\n",
                "  stakes: BigInt!\n",
                "}\n",
            ),
        )
        .unwrap();
        std::fs::write(
            dir.path().join("views/node.sql"),
            "CREATE VIEW node AS SELECT 'n' AS id, 'http://mynode.com:3000' AS url, 7 AS stakes;",
        )
        .unwrap();
        let state = test_state(dir.path(), SQL_MAX_CONCURRENCY);
        let body = graph_ask(
            "/graphql",
            r#"{ __type(name: "Node") { fields { name } } nodes { id url stakes } }"#,
            state,
        )
        .await;
        assert!(body["errors"].is_null(), "{body}");
        assert_eq!(
            body["data"]["__type"]["fields"],
            json!([{"name": "id"}, {"name": "url"}, {"name": "stakes"}]),
            "{body}"
        );
        assert_eq!(
            body["data"]["nodes"],
            json!([{"id": "n", "url": "http://mynode.com:3000", "stakes": "7"}]),
            "{body}"
        );
    }

    /// A schema the renderer cannot complete is refused in the envelope, never a panic.
    #[cfg(feature = "graph")]
    #[tokio::test]
    async fn a_schema_that_cannot_render_is_refused_not_a_panic() {
        let dir = tempfile::tempdir().unwrap();
        std::fs::create_dir_all(dir.path().join("graph")).unwrap();
        std::fs::write(
            dir.path().join("graph/schema.graphql"),
            "interface Named { id: ID! }\ntype A @entity { id: ID! n: Named! }",
        )
        .unwrap();
        let state = test_state(dir.path(), SQL_MAX_CONCURRENCY);
        let body = graph_ask("/graphql", "{ __schema { queryType { name } } }", state).await;
        let msg = body["errors"][0]["message"].as_str().unwrap_or_default();
        assert!(msg.contains("Named"), "{body}");
    }

    /// The **canonical introspection document**, sent verbatim from the recording fixture, answered with
    /// the same shape graph-node answers it with.
    ///
    /// This is the end of the argument about whether a generated client can consume our introspection
    /// response. The query is the one graphql-js emits - `FullType`, `InputValue`, `TypeRef`, fragment
    /// spreads and all - and the assertion is that for every type the reference carries, our response
    /// carries the same keys, with the same keys on every field, argument and enum value. Not "the keys we
    /// thought to emit", and not "the keys the other tests happen to read": the reference's own.
    ///
    /// **This test deliberately does not cover projection**, and a mutation that stops projecting leaves
    /// it green. The canonical document selects every key, so narrowing the response to that selection
    /// yields the same document either way. Projection is covered where it can be seen - narrow
    /// selections, in `a_fragment_document_and_a_mixed_operation_answer_over_http`. Two tests, two
    /// properties: this one says the document is *complete*, that one says it is *no wider than asked*.
    #[cfg(feature = "graph")]
    #[tokio::test]
    async fn the_canonical_introspection_document_answers_with_the_references_shape() {
        // The **recorded Uniswap V4 schema**, not the small fixture nest: the reference document was
        // recorded from that deployment, so anything else is comparing two different schemas. No views
        // are needed - this test only introspects.
        let d = tempfile::tempdir().unwrap();
        std::fs::create_dir_all(d.path().join("graph")).unwrap();
        std::fs::write(
            d.path().join("graph/schema.graphql"),
            include_str!("../tests/fixtures/graph-node/uniswap-v4-schema.graphql"),
        )
        .unwrap();
        let state = test_state(d.path(), SQL_MAX_CONCURRENCY);
        let query: serde_json::Value = serde_json::from_str(include_str!(
            "../tests/fixtures/graph-node/introspection-query.gql.json"
        ))
        .expect("the recording query parses");
        let query = query["query"].as_str().expect("a query string");
        let body = graph_ask("/graphql", query, state).await;
        assert!(
            body["errors"].is_null(),
            "the canonical document must not be refused: {}",
            serde_json::to_string(&body["errors"]).unwrap_or_default()
        );

        let reference: serde_json::Value = serde_json::from_str(include_str!(
            "../tests/fixtures/graph-node/graph-node-introspection-uniswap-v4.json"
        ))
        .expect("the recorded reference parses");
        // **Values, not key sets.** Projection inserts `null` for any selected key, so the response's
        // keys always equal the selection and comparing them through this path is vacuous by
        // construction - my first attempt at this test passed with the normalisation pass removed *and*
        // with projection removed. Compare what the keys contain.
        let ours: std::collections::BTreeMap<String, serde_json::Value> = body["data"]["__schema"]
            ["types"]
            .as_array()
            .unwrap_or_else(|| panic!("no types: {body}"))
            .iter()
            .filter_map(|t| t["name"].as_str().map(|n| (n.to_string(), t.clone())))
            .collect();
        let mut compared = 0;
        for t in reference["__schema"]["types"].as_array().unwrap() {
            let name = t["name"].as_str().unwrap_or_default();
            if name.starts_with("__") {
                continue; // the meta-schema, which a nest does not model
            }
            let Some(m) = ours.get(name) else { continue };
            // `description` is a **declared divergence**: graph-node carries the schema author's doc
            // comments there, they are not part of the contract a client validates against, and this
            // renderer emits the key with `null`. The key's *presence* is what a client needs and is
            // asserted at the render level by the golden test; its text is not reproduced.
            for key in ["kind", "interfaces", "possibleTypes"] {
                assert_eq!(m[key], t[key], "{name}.{key}");
            }
            for list in ["fields", "inputFields", "enumValues"] {
                assert_eq!(
                    m[list].is_null(),
                    t[list].is_null(),
                    "{name}.{list}: null on one side only"
                );
                let (Some(a), Some(b)) = (t[list].as_array(), m[list].as_array()) else {
                    continue;
                };
                // **By name, not by position.** Declaration order is not something a client depends on,
                // and zipping the two lists positionally compared unrelated entries - it reported a
                // field with 35 arguments against one with 5.
                let by_name = |l: &[serde_json::Value]| -> std::collections::BTreeMap<String, serde_json::Value> {
                    l.iter()
                        .filter_map(|e| e["name"].as_str().map(|n| (n.to_string(), e.clone())))
                        .collect()
                };
                let (ra, ma) = (by_name(a), by_name(b));
                assert_eq!(
                    ra.keys().collect::<Vec<_>>(),
                    ma.keys().collect::<Vec<_>>(),
                    "{name}.{list} names"
                );
                for (entry, x) in &ra {
                    let y = &ma[entry];
                    for key in ["name", "isDeprecated", "deprecationReason", "type"] {
                        if x.get(key).is_none() {
                            continue; // not a key this list's entries carry
                        }
                        assert_eq!(y[key], x[key], "{name}.{list}.{entry}.{key}");
                    }
                    if let Some(xa) = x["args"].as_array() {
                        let ya = y["args"]
                            .as_array()
                            .unwrap_or_else(|| panic!("{name}.{list}.{entry} has no args"));
                        let (rg, mg) = (by_name(xa), by_name(ya));
                        assert_eq!(
                            rg.keys().collect::<Vec<_>>(),
                            mg.keys().collect::<Vec<_>>(),
                            "{name}.{list}.{entry} argument names"
                        );
                        for (arg, p) in &rg {
                            for key in ["name", "type", "defaultValue"] {
                                assert_eq!(
                                    mg[arg][key], p[key],
                                    "{name}.{list}.{entry}({arg}).{key}"
                                );
                            }
                        }
                    }
                    compared += 1;
                }
            }
        }
        // A floor, so this cannot pass by comparing nothing: 19 entities contribute an object, a filter
        // and an orderBy enum each.
        assert!(compared >= 500, "only {compared} entries compared");

        let names = |v: &serde_json::Value| -> Vec<String> {
            v["__schema"]["directives"]
                .as_array()
                .map(|a| {
                    a.iter()
                        .filter_map(|x| x["name"].as_str().map(str::to_string))
                        .collect()
                })
                .unwrap_or_default()
        };
        let mine = serde_json::json!({"__schema": body["data"]["__schema"].clone()});
        assert_eq!(
            names(&mine),
            names(&reference),
            "the directives a client reads"
        );
    }

    /// `__type`, the traversals, the text operators and every refusal, each in the Graph envelope.
    #[cfg(feature = "graph")]
    #[tokio::test]
    async fn the_graph_endpoint_answers_types_traversals_and_refusals() {
        let (d, state) = graph_fixture();
        let _ = &d;
        let ask = graph_ask;

        // Aliases, over HTTP: the handler routes on the schema field name and answers under the
        // caller's key. That split matters for the introspection roots too, which are routed by name.
        let body = graph_ask(
            "/graphql",
            r#"{ s: __schema { queryType { name } } m: _meta { hasIndexingErrors }
                 p: pools(first: 1) { n: id t: token0 { sym: symbol } } }"#,
            state.clone(),
        )
        .await;
        assert_eq!(body["data"]["s"]["queryType"]["name"], "Query", "{body}");
        assert_eq!(body["data"]["m"]["hasIndexingErrors"], false, "{body}");
        assert_eq!(
            body["data"]["p"],
            serde_json::json!([{"n": "0xaaa", "t": {"sym": "WETH"}}]),
            "an aliased entity root, scalar and traversal all answer under their keys: {body}"
        );
        assert!(
            body["data"]["__schema"].is_null() && body["data"]["pools"].is_null(),
            "nothing answers under the unaliased name: {body}"
        );

        // `__type` is the other standard introspection operation, and it has to answer under
        // `__type`. It used to fall into the `__schema` branch and return the whole schema document
        // under the wrong key, which is an invalid response to the query that was asked.
        let body = graph_ask(
            "/graphql",
            r#"{ __type(name: "Pool") { kind name } }"#,
            state.clone(),
        )
        .await;
        assert_eq!(
            body["data"]["__type"]["name"], "Pool",
            "__type must answer under __type: {body}"
        );
        assert_eq!(
            body["data"]["__type"]["kind"], "OBJECT",
            "an entity is an OBJECT, not a SCALAR: {body}"
        );
        assert!(
            body["data"]["__schema"].is_null(),
            "asking for __type must not return the whole schema: {body}"
        );
        // A name the schema does not declare is `null` rather than an error - that is what
        // introspection says, and a client uses it to test whether a type exists.
        let body = graph_ask(
            "/graphql",
            r#"{ __type(name: "Nope") { name } }"#,
            state.clone(),
        )
        .await;
        assert!(
            body["data"]["__type"].is_null() && body["errors"].is_null(),
            "an undeclared type is null, not an error: {body}"
        );

        // A `@derivedFrom` list, aggregated into one JSON column and read back as an array. `0xbbb`
        // has no swaps, so it must answer `[]` - `list()` over zero rows is NULL in Burrmill, which
        // would have served null for a field the schema types `[Swap!]!`.
        //
        // `amount` is a **string**, for the same reason a top-level `BigInt` is: this asserted `5` until
        // the wire-type rule reached the packed struct, which is the second test on this branch found to
        // have the wrong type pinned.
        // **An alias inside an aggregated list answers under the alias.** The packed struct keyed by the
        // field name rather than the selection key, so `sid: id` came back as `id` - a key the client never
        // asked for, in the one place the root-level alias test could not see (Jules on #1282).
        let body = graph_ask(
            "/graphql",
            "{ pools { id swaps { sid: id amt: amount } } }",
            state.clone(),
        )
        .await;
        assert_eq!(
            body["data"]["pools"][0]["swaps"],
            serde_json::json!([{"sid": "s1", "amt": "5"}, {"sid": "s2", "amt": "7"}]),
            "a child alias must survive the aggregation: {body}"
        );

        // **`orderBy` on a relation is refused.** graph-node's generated enum includes every field of the
        // entity, `@derivedFrom` lists among them, so this is a value the schema advertises and the parent
        // view has no column for - `ORDER BY b."swaps"` either failed in the engine or sorted by a JSON
        // aggregate (Jules on #1282).
        let body = graph_ask(
            "/graphql",
            "{ pools(orderBy: swaps) { id } }",
            state.clone(),
        )
        .await;
        let msg = body["errors"][0]["message"]
            .as_str()
            .unwrap_or_default()
            .to_string();
        assert!(
            msg.contains("orderBy `swaps`") && msg.contains("relation"),
            "ordering by a relation must be refused by name: {body}"
        );

        let body = graph_ask(
            "/graphql",
            "{ pools { id swaps { id amount } } }",
            state.clone(),
        )
        .await;
        assert_eq!(
            body["data"]["pools"],
            serde_json::json!([
                {"id": "0xaaa", "swaps": [{"id": "s1", "amount": "5"}, {"id": "s2", "amount": "7"}]},
                {"id": "0xbbb", "swaps": []},
                {"id": "0xccc", "swaps": []},
            ]),
            "a derived list must nest, in child id order, and be [] when empty: {body}"
        );

        // The canonical shape: a relation traversal, lowered to a LEFT JOIN and put back under the
        // field name it was asked for. `0xbbb` points at a token that is not there, so its relation
        // must be `null` rather than an object of nulls, and the pool itself must still be in the
        // answer - an INNER JOIN would have dropped it silently.
        let body = graph_ask(
            "/graphql",
            "{ pools { id token0 { symbol decimals totalSupply } } }",
            state.clone(),
        )
        .await;
        assert_eq!(
            body["data"]["pools"],
            serde_json::json!([
                // `decimals` is a number and `totalSupply` a string, from the same row: the wire rule is
                // per-scalar, and a child field reaches the client through this join as much as through a
                // top-level selection (Jules on #1282).
                {"id": "0xaaa", "token0": {
                    "symbol": "WETH", "decimals": 18, "totalSupply": "21000000",
                }},
                {"id": "0xbbb", "token0": null},
                // `0xccc`'s token exists but its selected fields are all null. An object of nulls,
                // not `null`: deciding presence from the selected values reported a real row as
                // absent, which is why the join now carries the target's id as a marker.
                {"id": "0xccc", "token0": {"symbol": null, "decimals": null, "totalSupply": null}},
            ]),
            "a to-one traversal must nest, and a missing target must not drop the parent: {body}"
        );

        // An unlowerable operation is refused **in the Graph envelope** rather than as a bare
        // status a client cannot read.
        let body = ask("/graphql", "{ nope { id } }", state.clone()).await;
        assert!(
            body["errors"][0]["message"]
                .as_str()
                .is_some_and(|m| m.contains("nope")),
            "an unknown root must be refused by name in the envelope: {body}"
        );
        assert!(
            !body["errors"][0]["message"]
                .as_str()
                .unwrap_or_default()
                .contains("introspection only"),
            "the S2 refusal should be gone now that the compiler exists: {body}"
        );

        // A text operator filters for real over HTTP, through the engine's own `LIKE`.
        let body = graph_ask(
            "/graphql",
            r#"{ pools(where: { hooks_contains: "hook2" }) { id } }"#,
            state.clone(),
        )
        .await;
        assert_eq!(
            body["data"]["pools"],
            serde_json::json!([{"id": "0xbbb"}]),
            "_contains must match the one pool whose hooks contain it: {body}"
        );
        // And the caller's own `%` is data: no pool's hooks contain a literal percent sign, so a
        // pattern that was not escaped would match everything instead of nothing.
        let body = graph_ask(
            "/graphql",
            r#"{ pools(where: { hooks_contains: "%" }) { id } }"#,
            state.clone(),
        )
        .await;
        assert_eq!(
            body["data"]["pools"],
            serde_json::json!([]),
            "an unescaped wildcard would have matched every row: {body}"
        );

        // A nested relation filter, through the engine's own EXISTS. `0xaaa`'s token0 is WETH and
        // `0xbbb` points at a token that is not there, so exactly one pool can match.
        let body = graph_ask(
            "/graphql",
            r#"{ pools(where: { token0_: { symbol: "WETH" } }) { id } }"#,
            state.clone(),
        )
        .await;
        assert_eq!(
            body["data"]["pools"],
            serde_json::json!([{"id": "0xaaa"}]),
            "a nested relation filter must filter on the child: {body}"
        );
        // And a child condition nothing satisfies returns nothing, rather than ignoring the clause -
        // which is the failure that makes a dropped filter worse than an error.
        let body = graph_ask(
            "/graphql",
            r#"{ pools(where: { token0_: { symbol: "NOPE" } }) { id } }"#,
            state.clone(),
        )
        .await;
        assert_eq!(body["data"]["pools"], serde_json::json!([]), "{body}");

        // A nested filter across a list relation is refused by name, with the reason, rather than as
        // an unknown field.
        let body = graph_ask(
            "/graphql",
            r#"{ pools(where: { swaps_: { id: "s1" } }) { id } }"#,
            state.clone(),
        )
        .await;
        let msg = body["errors"][0]["message"].as_str().unwrap_or_default();
        assert!(
            msg.contains("swaps_") && msg.contains("child-existence"),
            "the refusal must name the field and the reason: {body}"
        );

        // An operator the *schema* does not declare for that field's type is still refused by name,
        // because a dropped filter returns more rows than were asked for.
        let body = graph_ask(
            "/graphql",
            r#"{ pools(where: { liquidity_contains: "ab" }) { id } }"#,
            state.clone(),
        )
        .await;
        let msg = body["errors"][0]["message"].as_str().unwrap_or_default();
        assert!(
            msg.contains("liquidity_contains"),
            "an unlowerable operator must be named: {body}"
        );

        // And `block:` says why rather than answering as of head while implying otherwise.
        let body = graph_ask(
            "/graphql",
            "{ pools(block: { number: 1 }) { id } }",
            state.clone(),
        )
        .await;
        assert!(
            body["errors"][0]["message"]
                .as_str()
                .unwrap_or_default()
                .contains("1267"),
            "time travel must name the issue that tracks it: {body}"
        );

        // A nest with no Graph schema says so rather than inventing one.
        let bare = tempfile::tempdir().unwrap();
        let body = graph_ask(
            "/graphql",
            "{ __schema { types { name } } }",
            test_state(bare.path(), SQL_MAX_CONCURRENCY),
        )
        .await;
        assert!(
            body["errors"][0]["message"]
                .as_str()
                .unwrap_or_default()
                .contains("no Graph schema"),
            "a nest without graph/schema.graphql must say so, got {body}"
        );
    }
}
