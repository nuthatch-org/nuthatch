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
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::mpsc;

/// How often the mount set is compared, and how often a heartbeat goes out.
const WATCH_EVERY: Duration = Duration::from_millis(250);
const HEARTBEAT_EVERY: Duration = Duration::from_secs(1);
/// Lines queued for one connection before its writers wait, and their datasets start to lag.
const QUEUED_LINES: usize = 256;

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
}

#[derive(serde::Deserialize)]
struct TokenQuery {
    token: Option<String>,
    /// Datasets, comma-separated, whose snapshots and deltas the follower cannot hold: their mounts
    /// are still sent, their data is not.
    exclude: Option<String>,
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
        .with_state(FeedState {
            roster,
            root,
            token,
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
    let (tx, rx) = mpsc::channel::<bytes::Bytes>(QUEUED_LINES);
    let excluded = q
        .exclude
        .as_deref()
        .unwrap_or("")
        .split(',')
        .filter(|d| !d.is_empty())
        .map(str::to_string)
        .collect();
    tokio::spawn(serve_connection(s, tx, excluded));
    let body = futures::stream::unfold(rx, |mut rx| async move {
        rx.recv()
            .await
            .map(|b| (Ok::<_, std::convert::Infallible>(b), rx))
    });
    (
        [(axum::http::header::CONTENT_TYPE, "application/x-ndjson")],
        axum::body::Body::from_stream(body),
    )
        .into_response()
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

async fn serve_connection(
    s: FeedState,
    tx: mpsc::Sender<bytes::Bytes>,
    excluded: std::collections::BTreeSet<String>,
) {
    let hello = Line::Hello {
        version: env!("CARGO_PKG_VERSION").to_string(),
        feed: crate::feed::FEED_VERSION,
    };
    if tx.send(line(&hello)).await.is_err() {
        return;
    }
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
                    if tx.send(line(&Line::Unmount { route: gone })).await.is_err() {
                        return;
                    }
                }
                for (route, m) in &now {
                    let mount = line(&Line::Mount {
                        route: route.clone(),
                        dataset: dataset_key(&s.root, &m.dir),
                        nid: m.nid.clone(),
                        sql: m.surface.access,
                        queries: m.surface.queries.clone(),
                        tenant: m.tenant.clone(),
                        share: m.share,
                        limits: Some((&m.limits).into()),
                        chain: Some(m.chain.clone()),
                    });
                    if routes.get(route).is_some_and(|sent| sent[..] == mount[..]) {
                        continue;
                    }
                    routes.insert(route.clone(), mount.to_vec());
                    if tx.send(mount).await.is_err() {
                        return;
                    }
                }
                if !announced {
                    announced = true;
                    if tx.send(line(&Line::Mounted)).await.is_err() {
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
                        let task = tokio::spawn(forward(dataset.clone(), store.clone(), tx.clone()));
                        forwarders.0.insert(dataset.clone(), task);
                    }
                }
                for gone in stores.keys().filter(|d| !live.contains_key(*d)).cloned().collect::<Vec<_>>() {
                    if let Some(h) = forwarders.0.remove(&gone) {
                        h.abort();
                    }
                }
                stores = live;
            }
            _ = heartbeat.tick() => {
                let positions = stores
                    .iter()
                    .filter_map(|(d, st)| Some((d.clone(), st.feed_position()?)))
                    .collect();
                if tx.send(line(&Line::Heartbeat { positions })).await.is_err() {
                    return;
                }
            }
            _ = tx.closed() => return,
        }
    }
}

/// One dataset's snapshot and deltas onto the connection, resnapshotting whenever the deltas cannot
/// be relied on to follow: the follower fell behind, or a commit could not be described.
async fn forward(dataset: String, store: Arc<dyn HotStore>, tx: mpsc::Sender<bytes::Bytes>) {
    use tokio::sync::broadcast::error::RecvError;
    'snapshot: loop {
        let st = store.clone();
        let ds = dataset.clone();
        let taken = tokio::task::spawn_blocking(move || {
            let sub = st.feed_subscribe()?;
            Ok::<_, anyhow::Error>(sub.map(|sub| {
                let bytes = line(&Line::Snapshot {
                    dataset: crate::feed::DatasetName(ds),
                    snapshot: sub.snapshot,
                });
                (bytes, sub.deltas)
            }))
        })
        .await;
        let (bytes, mut deltas) = match taken {
            Ok(Ok(Some(taken))) => taken,
            Ok(Ok(None)) => {
                tracing::warn!("{dataset}: this hot store has no feed; not followed");
                return;
            }
            Ok(Err(e)) => {
                tracing::warn!("{dataset}: feed snapshot failed, retrying: {e:#}");
                tokio::time::sleep(Duration::from_secs(1)).await;
                continue;
            }
            Err(_) => return,
        };
        if tx.send(bytes).await.is_err() {
            return;
        }
        loop {
            match deltas.recv().await {
                Ok(delta) if delta.resync => continue 'snapshot,
                Ok(delta) => {
                    let mut out = format!(
                        "{{\"delta\":{{\"dataset\":{},\"delta\":",
                        serde_json::Value::String(dataset.clone())
                    )
                    .into_bytes();
                    if serde_json::to_writer(&mut out, &*delta).is_err() {
                        continue 'snapshot;
                    }
                    out.extend_from_slice(b"}}\n");
                    if tx.send(out.into()).await.is_err() {
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
