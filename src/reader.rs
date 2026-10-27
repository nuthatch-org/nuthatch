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
//! What a follower does not serve: state the writer derives in memory rather than stores, which is
//! the built-in balance, exposure and velocity views and RFC-0041's entities. Those routes, and SQL
//! that names an entity, answer `503` with `reason: "writer_only"` rather than an empty answer.

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

struct Route {
    dataset: String,
    nid: Option<String>,
    surface: crate::allowlist::Surface,
    served: Option<std::result::Result<SharedNest, String>>,
}

struct Link {
    connected: bool,
    since: Instant,
    version: Option<String>,
}

struct Inner {
    routes: BTreeMap<String, Route>,
    datasets: HashMap<String, Arc<MemStore>>,
    /// The writer's `(epoch, generation)` per dataset at its last heartbeat.
    positions: HashMap<String, (u64, u64)>,
    link: Link,
}

/// A running follower: its state, and the composition it serves.
pub struct Follower {
    root: PathBuf,
    writer: String,
    token: String,
    live: LiveRuntime,
    inner: Mutex<Inner>,
    /// One analytical gate per chain, as the writer has one per cursor.
    gates: Mutex<HashMap<String, Arc<tokio::sync::Semaphore>>>,
}

impl Follower {
    /// Start following `writer` for the runtime at `root`.
    pub fn start(root: PathBuf, writer: String, token: String) -> Arc<Follower> {
        let follower = Arc::new(Follower {
            root,
            writer: writer.trim_end_matches('/').to_string(),
            token,
            live: LiveRuntime::new(axum::Router::new()),
            inner: Mutex::new(Inner {
                routes: BTreeMap::new(),
                datasets: HashMap::new(),
                positions: HashMap::new(),
                link: Link {
                    connected: false,
                    since: Instant::now(),
                    version: None,
                },
            }),
            gates: Mutex::default(),
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
            }
            tokio::time::sleep(backoff).await;
            backoff = (backoff * 2).min(BACKOFF_MAX);
        }
    }

    async fn follow_once(self: &Arc<Self>, client: &reqwest::Client) -> Result<()> {
        use futures::StreamExt;
        let resp = client
            .get(format!("{}/_admin/feed", self.writer))
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
        let mut body = resp.bytes_stream();
        let mut buf: Vec<u8> = Vec::new();
        // Datasets snapshotted on this connection: a delta for any other is a gap.
        let mut fresh: BTreeSet<String> = BTreeSet::new();
        loop {
            let chunk = match tokio::time::timeout(SILENCE, body.next()).await {
                Err(_) => bail!("the writer has been silent for {}s", SILENCE.as_secs()),
                Ok(None) => return Ok(()),
                Ok(Some(chunk)) => chunk
                    .map_err(reqwest::Error::without_url)
                    .context("reading the feed")?,
            };
            buf.extend_from_slice(&chunk);
            while let Some(end) = buf.iter().position(|b| *b == b'\n') {
                let raw: Vec<u8> = buf.drain(..=end).collect();
                let line: Line =
                    serde_json::from_slice(&raw).context("an unreadable line on the feed")?;
                self.handle(line, &mut fresh).await?;
            }
        }
    }

    async fn handle(self: &Arc<Self>, line: Line, fresh: &mut BTreeSet<String>) -> Result<()> {
        match line {
            Line::Hello { version, feed } => {
                if feed != crate::feed::FEED_VERSION {
                    bail!(
                        "the writer ({version}) speaks feed version {feed} and this follower ({}) \
                         speaks {}: upgrade them to one release",
                        env!("CARGO_PKG_VERSION"),
                        crate::feed::FEED_VERSION
                    );
                }
                self.lock().link.version = Some(version);
            }
            Line::Mount {
                route,
                dataset,
                nid,
                sql,
                queries,
            } => {
                {
                    let mut inner = self.lock();
                    inner.routes.insert(
                        route,
                        Route {
                            dataset,
                            nid,
                            surface: crate::allowlist::Surface {
                                access: sql,
                                queries,
                            },
                            served: None,
                        },
                    );
                }
                self.realise();
            }
            Line::Unmount { route } => {
                {
                    let mut inner = self.lock();
                    if let Some(gone) = inner.routes.remove(&route) {
                        if !inner.routes.values().any(|r| r.dataset == gone.dataset) {
                            inner.datasets.remove(&gone.dataset);
                            inner.positions.remove(&gone.dataset);
                            crate::seal::unpin_catalogue(&self.root.join(&gone.dataset));
                        }
                    }
                }
                self.recompose();
            }
            Line::Snapshot { dataset, snapshot } => {
                let dir = self.root.join(&dataset);
                let store = tokio::task::spawn_blocking(move || {
                    Arc::new(MemStore::from_snapshot(&dir, snapshot))
                })
                .await
                .context("building a snapshot's store")?;
                self.lock().datasets.insert(dataset.clone(), store);
                fresh.insert(dataset);
                self.realise();
            }
            Line::Delta { dataset, delta } => {
                let store = self.lock().datasets.get(&dataset).cloned();
                let applied = match (fresh.contains(&dataset), store) {
                    (true, Some(store)) => store.apply(&delta).map_err(anyhow::Error::from),
                    _ => Err(anyhow::anyhow!("a delta before its snapshot")),
                };
                // The old state keeps serving while a new connection takes fresh snapshots.
                applied.with_context(|| format!("{dataset}: resnapshotting"))?;
            }
            Line::Heartbeat { positions } => {
                self.lock().positions = positions.into_iter().collect();
            }
        }
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
                let Some(store) = datasets.get(&r.dataset).cloned() else {
                    continue;
                };
                let store: Arc<dyn HotStore> = store;
                match &r.served {
                    Some(Ok(shared)) => {
                        let current = shared.current();
                        if !Arc::ptr_eq(&current.store, &store) || *current.surface != r.surface {
                            shared.swap(AppState {
                                store,
                                surface: Arc::new(r.surface.clone()),
                                ..(*current).clone()
                            });
                        }
                    }
                    Some(Err(_)) => {}
                    None => {
                        r.served = Some(
                            self.build(route, r, store)
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

    fn gate(&self, chain: &str) -> Arc<tokio::sync::Semaphore> {
        self.gates
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .entry(chain.to_string())
            .or_insert_with(crate::serve::new_sql_gate)
            .clone()
    }

    /// A route's serving state over `store`: what `build_nest` assembles for a nest it does not
    /// index, with no circuit, no ingest and no admin surface.
    fn build(&self, route: &str, r: &Route, store: Arc<dyn HotStore>) -> Result<AppState> {
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
        Ok(AppState {
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
            sql_gate: self.gate(&config.nest.chain),
            sql_queued: Default::default(),
            memo_gate: Default::default(),
            sql_response_bytes: Default::default(),
            sql_max_hot_rows: crate::serve::SQL_MAX_HOT_ROWS,
            sql_max_hot_bytes: crate::serve::SQL_MAX_HOT_BYTES,
            sql_max_named_scan_bytes: crate::serve::SQL_MAX_NAMED_SCAN_BYTES,
            cursorless: true,
            seal_span,
            surface: Arc::new(r.surface.clone()),
            #[cfg(feature = "counter")]
            counter: None,
            nid: r.nid.as_deref().map(Arc::from),
            runtime_health: Some((route.to_string(), health)),
            address_history: None,
            admin_enabled: false,
            admin_token: None,
            nest_info: Arc::new(nest_info),
        })
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
            let sub = match &r.served {
                Some(Ok(shared)) => {
                    let entities = crate::entities::load(&self.root.join(&r.dataset))
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
        self.live.swap(app);
    }

    /// How far this route's dataset trails the writer, as `/ready` reports it.
    fn lag(&self, inner: &Inner, dataset: &str) -> serde_json::Value {
        let applied = inner.datasets.get(dataset).map(|s| s.applied());
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
        let Some(store) = inner.datasets.get(&r.dataset) else {
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
                "follower": self.lag(&inner, &r.dataset),
            })),
        )
            .into_response()
    }

    fn root_ready(&self) -> Response {
        let inner = self.lock();
        let mut nests = serde_json::Map::new();
        let mut all = true;
        for (route, r) in &inner.routes {
            let ready = inner.datasets.contains_key(&r.dataset) && matches!(r.served, Some(Ok(_)));
            all &= ready;
            let mut v = self.lag(&inner, &r.dataset);
            v["ready"] = ready.into();
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
                "nid": r.nid,
                "ready": inner.datasets.contains_key(&r.dataset),
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

/// Routes answered from state the writer derives in memory, which a follower does not have.
fn writer_only(path: &str) -> bool {
    matches!(path, "/balances" | "/flags" | "/derived")
        || ["/balance/", "/exposure/", "/derived/"]
            .iter()
            .any(|p| path.starts_with(p))
}

fn writer_only_response(what: &str) -> Response {
    (
        StatusCode::SERVICE_UNAVAILABLE,
        Json(serde_json::json!({
            "error": format!("{what} is served by the writer only: a follower holds no derived state"),
            "reason": "writer_only",
        })),
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
    entities: Arc<BTreeSet<String>>,
    router: axum::Router,
) -> axum::Router {
    router.layer(axum::middleware::from_fn(
        move |req: Request, next: Next| {
            let follower = follower.clone();
            let route = route.clone();
            let entities = entities.clone();
            async move {
                let path = req.uri().path().to_string();
                if path == "/ready" {
                    return follower.route_ready(&route);
                }
                if writer_only(&path) {
                    return writer_only_response(&path);
                }
                let reads_sql = matches!(path.as_str(), "/sql" | "/explain" | "/graphql")
                    || path.starts_with("/q/")
                    || path.starts_with("/subgraphs/");
                if entities.is_empty() || !reads_sql {
                    return next.run(req).await;
                }
                // A named query's text is the nest's, not the request's, so it is not read here.
                if path.starts_with("/q/") {
                    return writer_only_response("a named query on a nest with entities");
                }
                let (parts, body) = req.into_parts();
                let Ok(bytes) = axum::body::to_bytes(body, BODY_LIMIT).await else {
                    return StatusCode::PAYLOAD_TOO_LARGE.into_response();
                };
                let text = format!(
                    "{} {}",
                    decode_query(parts.uri.query().unwrap_or("")),
                    String::from_utf8_lossy(&bytes)
                );
                if names_entity(&text, &entities) {
                    return writer_only_response("SQL naming an entity");
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
