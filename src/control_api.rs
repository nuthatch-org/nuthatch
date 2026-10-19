//! The control-plane HTTP API (RFC-0022 §3): declare what should run, without restarting anything.
//!
//! Deliberately thin. It writes **intent** to the control plane and reads it back; the reconcile loop
//! already converges on that intent independently, so nothing here commands a worker, waits for one,
//! or tracks one. An operator adding a nest is making a statement about the desired world, and the
//! fleet arrives at it on its own tick.
//!
//! That is why there is no "start this nest on worker w3" endpoint. It would be the one call capable
//! of putting a cursor somewhere the scheduler did not choose and the lease did not arbitrate - the
//! shape of every distributed-systems bug this design spent four slices avoiding.
//!
//! ## `GET /plan` is a mirror, not a command
//!
//! It runs the same pure [`crate::scheduler::plan`] the workers run and shows the result. Calling it
//! changes nothing. It exists because "why is my nest not running?" is the question an operator
//! actually asks, and `unplaceable` with a reason answers it directly - rather than leaving them to
//! infer it from the absence of rows.
//!
//! ## Exposure
//!
//! Same posture as the admin UI: bound to localhost it is open; bound anywhere else it demands
//! `NUTHATCH_CONTROL_TOKEN`, and **refuses to start without one**. A control plane is strictly more
//! dangerous than the read API - it decides what the fleet runs - so an unauthenticated one on a
//! public interface is not a warning, it is a refusal.
//!
//! The guard is a [`route_layer`](Router::route_layer) over every route except `/health`, so a route
//! is authenticated by virtue of being registered there. It used to be four lines at the top of each
//! handler, which made "every route is guarded" a claim ten separate copies had to keep - and the
//! test that asserted it named five of them, leaving the secrets API's guard deletable with the suite
//! green until #412 completed the list.

use std::sync::Arc;

use anyhow::{bail, Result};
use axum::extract::{Path, Query, Request, State};
use axum::http::StatusCode;
use axum::middleware::{self, Next};
use axum::response::{IntoResponse, Response};
use axum::routing::{delete, get};
use axum::{Json, Router};
use serde::{Deserialize, Serialize};

use crate::controlplane::{ControlPlane, DEFAULT_WORKER_TTL_SECS};
use crate::scheduler::{plan, DesiredNest};

#[derive(Clone)]
struct Api {
    cp: Arc<ControlPlane>,
    /// `None` on a localhost bind (open), `Some` otherwise - and startup refuses the latter without
    /// a token, so this is never `None` off-localhost.
    token: Option<String>,
}

#[derive(Deserialize)]
struct TokenQuery {
    token: Option<String>,
}

#[derive(Deserialize)]
struct PinBody {
    version: String,
    bundle_hash: String,
}

#[derive(Deserialize)]
struct SecretBody {
    key: String,
    value: String,
}

#[derive(Deserialize)]
struct DeclareBody {
    name: String,
    chain: String,
    /// Projected footprint. Optional: an operator who does not know should not be blocked, and the
    /// mounts's own per-nest baseline is a better guess than zero - zero would make the cursor look
    /// free and let the scheduler overcommit a worker.
    estimated_rss_mb: Option<u64>,
}

#[derive(Serialize)]
struct PlanView {
    assign: Vec<AssignView>,
    drain: Vec<AssignView>,
    unplaceable: Vec<UnplaceableView>,
}

#[derive(Serialize)]
struct AssignView {
    chain: String,
    worker: String,
}

#[derive(Serialize)]
struct UnplaceableView {
    chain: String,
    rss_mb: u64,
    /// Machine-readable, so a caller can branch without matching on prose.
    reason: String,
    /// Human-readable, because the two unplaceable reasons demand different actions and an operator
    /// reading a bare enum would not know which.
    detail: String,
}

/// Per-nest baseline when the caller does not supply an estimate - the same figure `mounts` uses for a
/// nest with no views, so an unestimated nest is costed as a small one rather than a free one.
const DEFAULT_NEST_RSS_MB: u64 = 90;

/// The token guard, applied to the whole API rather than repeated inside it.
///
/// Sits under `route_layer`, so it runs only for routes this router actually matches: an unknown path
/// still 404s without being told whether a token would have helped.
async fn require_token(State(api): State<Api>, req: Request, next: Next) -> Response {
    let from_query = Query::<TokenQuery>::try_from_uri(req.uri())
        .ok()
        .and_then(|Query(q)| q.token);
    if crate::serve::token_ok(api.token.as_deref(), from_query.as_deref(), req.headers()) {
        next.run(req).await
    } else {
        unauthorised().into_response()
    }
}

fn unauthorised() -> (StatusCode, Json<serde_json::Value>) {
    (
        StatusCode::UNAUTHORIZED,
        Json(serde_json::json!({ "error": "a control-plane token is required" })),
    )
}

async fn list_nests(State(api): State<Api>) -> (StatusCode, Json<serde_json::Value>) {
    match api.cp.desired() {
        Ok(nests) => (
            StatusCode::OK,
            Json(serde_json::json!({
                "count": nests.len(),
                "nests": nests.iter().map(|n| serde_json::json!({
                    "name": n.name, "chain": n.chain, "estimated_rss_mb": n.estimated_rss_mb
                })).collect::<Vec<_>>(),
            })),
        ),
        Err(e) => internal(e),
    }
}

async fn declare(
    State(api): State<Api>,
    Json(body): Json<DeclareBody>,
) -> (StatusCode, Json<serde_json::Value>) {
    let nest = DesiredNest {
        name: body.name,
        chain: body.chain,
        estimated_rss_mb: body.estimated_rss_mb.unwrap_or(DEFAULT_NEST_RSS_MB),
    };
    match api.cp.declare_nest(&nest) {
        Ok(()) => (
            StatusCode::OK,
            Json(serde_json::json!({
                "declared": nest.name,
                "chain": nest.chain,
                "estimated_rss_mb": nest.estimated_rss_mb,
                // Said explicitly because the API returning 200 does *not* mean the nest is running:
                // it means the fleet has been told to run it. Conflating the two is how an operator
                // ends up staring at an empty table wondering what broke.
                "note": "recorded as desired state - a worker will pick it up on its next tick",
            })),
        ),
        // A validation failure is the caller's, not the server's.
        Err(e) => (
            StatusCode::BAD_REQUEST,
            Json(serde_json::json!({ "error": format!("{e:#}") })),
        ),
    }
}

async fn undeclare(
    State(api): State<Api>,
    Path(name): Path<String>,
) -> (StatusCode, Json<serde_json::Value>) {
    match api.cp.undeclare_nest(&name) {
        Ok(true) => (
            StatusCode::OK,
            Json(serde_json::json!({
                "undeclared": name,
                "note": "its cursor drains on the owning worker's next tick",
            })),
        ),
        // 404 rather than a cheerful 200: an operator who typo'd a name must find out.
        Ok(false) => (
            StatusCode::NOT_FOUND,
            Json(serde_json::json!({ "error": format!("no nest named '{name}' is declared") })),
        ),
        Err(e) => internal(e),
    }
}

async fn list_workers(State(api): State<Api>) -> (StatusCode, Json<serde_json::Value>) {
    match api.cp.live_workers(DEFAULT_WORKER_TTL_SECS) {
        Ok(ws) => (
            StatusCode::OK,
            Json(serde_json::json!({
                "count": ws.len(),
                "ttl_secs": DEFAULT_WORKER_TTL_SECS,
                "workers": ws.iter().map(|w| serde_json::json!({
                    "id": w.id, "budget_mb": w.budget_mb
                })).collect::<Vec<_>>(),
            })),
        ),
        Err(e) => internal(e),
    }
}

async fn current_plan(State(api): State<Api>) -> (StatusCode, Json<serde_json::Value>) {
    let (desired, workers) = match (
        api.cp.desired(),
        api.cp.live_workers(DEFAULT_WORKER_TTL_SECS),
    ) {
        (Ok(d), Ok(w)) => (d, w),
        (Err(e), _) | (_, Err(e)) => return internal(e),
    };
    // Planned from an empty `current`, so this shows where cursors *would* go from a standing start.
    // It is a mirror of the decision, not a report of the world - the leases are the world, and they
    // live on the workers.
    let p = plan(&workers, &desired, &[]);
    let view = PlanView {
        assign: p
            .assign
            .iter()
            .map(|a| AssignView {
                chain: a.chain.clone(),
                worker: a.worker.clone(),
            })
            .collect(),
        drain: p
            .drain
            .iter()
            .map(|a| AssignView {
                chain: a.chain.clone(),
                worker: a.worker.clone(),
            })
            .collect(),
        unplaceable: p
            .unplaceable
            .iter()
            .map(|u| UnplaceableView {
                chain: u.chain.clone(),
                rss_mb: u.rss_mb,
                reason: format!("{:?}", u.reason).to_lowercase(),
                detail: u.reason.to_string(),
            })
            .collect(),
    };
    (
        StatusCode::OK,
        Json(serde_json::to_value(view).unwrap_or_default()),
    )
}

/// Pin the version an endpoint serves, fleet-wide - **and the bundle any worker pulls**.
///
/// The second half arrived with RFC-0019's worker-side pull. A pinned `bundle_hash` is fetched by
/// content address, so the registry's mutable index is bypassed entirely: re-tagging a version cannot
/// redirect a worker. Pinning is therefore the difference between "the fleet runs what we chose" and
/// "the fleet runs whatever the registry says today".
async fn pin(
    State(api): State<Api>,
    Path(name): Path<String>,
    Json(body): Json<PinBody>,
) -> (StatusCode, Json<serde_json::Value>) {
    match api.cp.pin_version(&name, &body.version, &body.bundle_hash) {
        Ok(true) => (
            StatusCode::OK,
            Json(serde_json::json!({
                "endpoint": name,
                "version": body.version,
                "bundle_hash": body.bundle_hash,
                "note": "every FE node resolves this endpoint to this version, and any worker \
                         pulling this nest fetches exactly this bundle",
            })),
        ),
        Ok(false) => (
            StatusCode::NOT_FOUND,
            Json(serde_json::json!({ "error": format!("no nest named '{name}' is declared") })),
        ),
        Err(e) => (
            StatusCode::BAD_REQUEST,
            Json(serde_json::json!({ "error": format!("{e:#}") })),
        ),
    }
}

/// What an endpoint resolves to. The call an FE node makes before serving.
async fn resolve(
    State(api): State<Api>,
    Path(name): Path<String>,
) -> (StatusCode, Json<serde_json::Value>) {
    match api.cp.resolve(&name) {
        Ok(Some(r)) => (
            StatusCode::OK,
            Json(serde_json::json!({
                "endpoint": r.endpoint,
                "chain": r.chain,
                "version": r.version,
                "bundle_hash": r.bundle_hash,
                "servable": r.is_servable(),
            })),
        ),
        Ok(None) => (
            StatusCode::NOT_FOUND,
            Json(serde_json::json!({ "error": format!("no endpoint named '{name}'") })),
        ),
        Err(e) => internal(e),
    }
}

/// Store a secret for a nest. **No matching read endpoint exists** - see the module docs on
/// write-only handling. The response deliberately echoes the key and never the value, so a secret
/// cannot end up in a shell history, a proxy log or a terminal scrollback by way of the reply.
async fn put_secret(
    State(api): State<Api>,
    Path(nest): Path<String>,
    Json(body): Json<SecretBody>,
) -> (StatusCode, Json<serde_json::Value>) {
    match api.cp.set_secret(&nest, &body.key, &body.value) {
        Ok(()) => (
            StatusCode::OK,
            Json(serde_json::json!({
                "nest": nest,
                "key": body.key,
                "note": "injected at mount - this changes no bundle hash",
            })),
        ),
        Err(e) => (
            StatusCode::BAD_REQUEST,
            Json(serde_json::json!({ "error": format!("{e:#}") })),
        ),
    }
}

/// The key *names* a nest has secrets for. Never the values.
async fn list_secret_keys(
    State(api): State<Api>,
    Path(nest): Path<String>,
) -> (StatusCode, Json<serde_json::Value>) {
    match api.cp.secret_keys(&nest) {
        Ok(keys) => (
            StatusCode::OK,
            Json(serde_json::json!({ "nest": nest, "keys": keys })),
        ),
        Err(e) => internal(e),
    }
}

async fn drop_secret(
    State(api): State<Api>,
    Path((nest, key)): Path<(String, String)>,
) -> (StatusCode, Json<serde_json::Value>) {
    match api.cp.delete_secret(&nest, &key) {
        Ok(true) => (StatusCode::OK, Json(serde_json::json!({ "deleted": key }))),
        Ok(false) => (
            StatusCode::NOT_FOUND,
            Json(serde_json::json!({ "error": format!("no secret '{key}' for nest '{nest}'") })),
        ),
        Err(e) => internal(e),
    }
}

fn internal(e: anyhow::Error) -> (StatusCode, Json<serde_json::Value>) {
    (
        StatusCode::INTERNAL_SERVER_ERROR,
        Json(serde_json::json!({ "error": format!("{e:#}") })),
    )
}

/// The router, exposed so tests can drive it without binding a port.
///
/// Everything except `/health` is registered on `guarded`, which carries [`require_token`]. Adding a
/// route there is the whole of authenticating it; the only way to ship an open one is to register it
/// outside that block, next to the route whose doc comment says it is unauthenticated on purpose.
pub fn routes(cp: Arc<ControlPlane>, token: Option<String>) -> Router {
    let api = Api { cp, token };
    let guarded = Router::new()
        .route("/nests", get(list_nests).post(declare))
        .route("/nests/{name}", delete(undeclare))
        .route("/nests/{name}/resolve", get(resolve))
        .route("/nests/{name}/pin", axum::routing::put(pin))
        .route(
            "/nests/{name}/secrets",
            get(list_secret_keys).put(put_secret),
        )
        .route("/nests/{name}/secrets/{key}", delete(drop_secret))
        .route("/workers", get(list_workers))
        .route("/plan", get(current_plan))
        .route_layer(middleware::from_fn_with_state(api.clone(), require_token));

    Router::new()
        // Unauthenticated on purpose: a load balancer must be able to probe it, and it reveals
        // nothing about the fleet. The only route outside `guarded`.
        .route("/health", get(|| async { "ok" }))
        .merge(guarded)
        .with_state(api)
}

/// Serve the control-plane API.
pub async fn run(listen: &str, db_url: &str) -> Result<()> {
    let token = std::env::var("NUTHATCH_CONTROL_TOKEN")
        .ok()
        .filter(|t| !t.trim().is_empty());
    if !crate::serve::is_localhost(listen) && token.is_none() {
        // A refusal, not a warning. This endpoint decides what an entire fleet runs; leaving it open
        // on a public interface is not a configuration choice anyone makes on purpose.
        bail!(
            "refusing to serve the control-plane API on '{listen}' without a token - set \
             NUTHATCH_CONTROL_TOKEN, or bind to 127.0.0.1 and reach it through a tunnel"
        );
    }

    let cp = Arc::new(ControlPlane::connect(db_url)?);
    let app = routes(cp, token.clone());
    tracing::info!(
        listen = %listen,
        auth = %if token.is_some() { "token" } else { "open (localhost)" },
        "control-plane API listening (RFC-0022 §3)"
    );
    let listener = tokio::net::TcpListener::bind(listen).await?;
    crate::serve::serve_bound(listener, app, None).await
}
