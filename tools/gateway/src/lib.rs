//! A reference read gateway for nuthatch (#3094): it verifies an OIDC/JWT bearer token on every
//! request, checks the token's read scopes against the nest and table the request reaches, through
//! the HTTP routes, `/sql`, `/q/*` and MCP, and forwards what passes to a nuthatch it alone can reach.
//!
//! nuthatch itself has no notion of identity and this does not give it one: the token stops here,
//! and nuthatch sees the request it would have seen without the gateway.
//!
//! The only network calls are to the configured `upstream` and, when set, the configured `jwks_url`.
//! Neither follows redirects or an `HTTP(S)_PROXY` from the environment.

pub mod jwt;
pub mod policy;

use std::path::PathBuf;
use std::sync::Arc;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use axum::body::{Body, Bytes};
use axum::extract::{Request, State};
use axum::http::{header, HeaderMap, HeaderName, HeaderValue, Method, StatusCode};
use axum::response::{IntoResponse, Response};
use serde::Deserialize;
use serde_json::{json, Value};
use tokio::sync::RwLock;

use jwt::{Expect, KeySet, Refusal};
use policy::Need;

/// Request bodies are read whole before they are forwarded (an MCP message must be, to be judged).
/// nuthatch's own limits are far below this.
const MAX_BODY: usize = 4 << 20;
/// A token naming an unknown `kid` re-fetches the JWKS at most this often (key rotation).
const JWKS_MIN_REFETCH: Duration = Duration::from_secs(30);
/// Keys older than this are re-fetched before use; on failure the old ones are kept.
const JWKS_MAX_AGE: Duration = Duration::from_secs(3600);
const JWKS_DEFAULT_TIMEOUT_SECS: u64 = 10;

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct NestConf {
    /// The name scopes use: `read:<name>`, `read:<name>/table/<t>`.
    pub name: String,
    /// The path nuthatch serves it under: `/<alias>` or `/<tenant>/<alias>` on a runtime, `""` for
    /// a solo `nuthatch dev`.
    pub prefix: String,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Config {
    pub listen: String,
    /// nuthatch's base URL, e.g. `http://127.0.0.1:8080`. Bind nuthatch where only the gateway can
    /// reach it, or the gateway guards nothing.
    pub upstream: String,
    pub issuer: String,
    pub audience: String,
    #[serde(default = "default_leeway")]
    pub leeway_secs: u64,
    pub jwks_url: Option<String>,
    pub jwks_file: Option<PathBuf>,
    /// Total time one JWKS fetch may take (connect, headers and body). A stalled IdP is cut off
    /// here and the keys already held stay in use.
    #[serde(default = "default_jwks_timeout")]
    pub jwks_timeout_secs: u64,
    /// An unknown `kid` re-fetches the JWKS at most this often.
    #[serde(default = "default_jwks_min_refetch")]
    pub jwks_min_refetch_secs: u64,
    /// The name of the environment variable holding an HS256 secret (never the secret itself).
    pub hs256_secret_env: Option<String>,
    #[serde(rename = "nest", default)]
    pub nests: Vec<NestConf>,
}

fn default_jwks_timeout() -> u64 {
    JWKS_DEFAULT_TIMEOUT_SECS
}

fn default_jwks_min_refetch() -> u64 {
    JWKS_MIN_REFETCH.as_secs()
}

fn default_leeway() -> u64 {
    60
}

impl Config {
    pub fn parse(text: &str) -> Result<Config, String> {
        let cfg: Config = toml::from_str(text).map_err(|e| format!("config: {e}"))?;
        let sources = [
            cfg.jwks_url.is_some(),
            cfg.jwks_file.is_some(),
            cfg.hs256_secret_env.is_some(),
        ];
        if sources.iter().filter(|s| **s).count() != 1 {
            return Err("config: set exactly one of jwks_url, jwks_file, hs256_secret_env".into());
        }
        if cfg.nests.is_empty() {
            return Err("config: no [[nest]]: the gateway would refuse everything".into());
        }
        for (i, n) in cfg.nests.iter().enumerate() {
            let name_ok = !n.name.is_empty()
                && n.name
                    .bytes()
                    .all(|b| b.is_ascii_alphanumeric() || b == b'_' || b == b'-');
            if !name_ok {
                return Err(format!(
                    "config: nest name {:?} is not [A-Za-z0-9_-]+",
                    n.name
                ));
            }
            if !(n.prefix.is_empty() || policy::canonical(&n.prefix) && !n.prefix.ends_with('/')) {
                return Err(format!(
                    "config: nest {} prefix {:?} must be \"\" or /a[/b] with no trailing slash",
                    n.name, n.prefix
                ));
            }
            if cfg.nests[..i]
                .iter()
                .any(|m| m.name == n.name || m.prefix == n.prefix)
            {
                return Err(format!("config: nest {} repeats a name or prefix", n.name));
            }
        }
        Ok(cfg)
    }
}

enum Keys {
    Static(KeySet),
    Jwks {
        url: String,
        client: reqwest::Client,
        held: RwLock<(KeySet, Instant)>,
        min_refetch: Duration,
        /// Held by the one request that is fetching. The network wait never holds `held`.
        refreshing: tokio::sync::Mutex<()>,
    },
}

async fn fetch_jwks(client: &reqwest::Client, url: &str) -> Result<KeySet, String> {
    let resp = client
        .get(url)
        .send()
        .await
        .map_err(|e| format!("JWKS fetch from jwks_url failed: {}", e.without_url()))?;
    if !resp.status().is_success() {
        return Err(format!("JWKS fetch from jwks_url: HTTP {}", resp.status()));
    }
    let body = resp
        .bytes()
        .await
        .map_err(|e| format!("JWKS fetch from jwks_url failed: {}", e.without_url()))?;
    KeySet::from_jwks(&body)
}

fn client(total_timeout: Option<Duration>) -> reqwest::Client {
    let mut b = reqwest::Client::builder()
        .redirect(reqwest::redirect::Policy::none())
        .no_proxy()
        .connect_timeout(Duration::from_secs(10));
    if let Some(t) = total_timeout {
        b = b.timeout(t);
    }
    b.build().expect("reqwest client")
}

pub struct Gateway {
    upstream: String,
    expect: Expect,
    keys: Keys,
    client: reqwest::Client,
    /// Longest prefix first, so `/acme/usdc` wins over `/acme`.
    nests: Vec<NestConf>,
}

impl Gateway {
    /// Load the keys (fetching the JWKS once: a gateway that cannot verify should not start).
    pub async fn new(mut cfg: Config) -> Result<Gateway, String> {
        let keys = if let Some(url) = cfg.jwks_url.take() {
            let client = client(Some(Duration::from_secs(cfg.jwks_timeout_secs.max(1))));
            let set = fetch_jwks(&client, &url).await?;
            Keys::Jwks {
                url,
                client,
                held: RwLock::new((set, Instant::now())),
                min_refetch: Duration::from_secs(cfg.jwks_min_refetch_secs),
                refreshing: Default::default(),
            }
        } else if let Some(path) = &cfg.jwks_file {
            let doc =
                std::fs::read(path).map_err(|e| format!("jwks_file {}: {e}", path.display()))?;
            Keys::Static(KeySet::from_jwks(&doc)?)
        } else {
            let var = cfg.hs256_secret_env.as_deref().unwrap_or_default();
            let secret = std::env::var(var)
                .map_err(|_| format!("hs256_secret_env: {var} is unset or not UTF-8"))?;
            // RFC 7518 §3.2: an HS256 key is at least the hash's 256 bits.
            if secret.len() < 32 {
                return Err(format!("hs256_secret_env: {var} is shorter than 32 bytes"));
            }
            Keys::Static(KeySet::hs256(secret.as_bytes()))
        };
        cfg.nests.sort_by_key(|n| std::cmp::Reverse(n.prefix.len()));
        Ok(Gateway {
            upstream: cfg.upstream.trim_end_matches('/').to_string(),
            expect: Expect {
                issuer: cfg.issuer,
                audience: cfg.audience,
                leeway: cfg.leeway_secs,
            },
            keys,
            client: client(None),
            nests: cfg.nests,
        })
    }

    pub fn router(self) -> axum::Router {
        axum::Router::new()
            .fallback(handle)
            .with_state(Arc::new(self))
    }

    async fn verify(&self, token: &str) -> Result<jwt::Claims, Refusal> {
        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map(|d| d.as_secs())
            .unwrap_or(0);
        match &self.keys {
            Keys::Static(set) => jwt::verify(token, set, &self.expect, now),
            Keys::Jwks {
                url,
                client,
                held,
                min_refetch,
                refreshing,
            } => {
                let (stale, first) = {
                    let h = held.read().await;
                    let first = jwt::verify(token, &h.0, &self.expect, now);
                    let age = h.1.elapsed();
                    let rotate = matches!(first, Err(Refusal::UnknownKey)) && age >= *min_refetch;
                    (rotate || age >= JWKS_MAX_AGE, first)
                };
                if !stale {
                    return first;
                }
                // One request fetches, outside the key lock; the rest verify against the keys held.
                if let Ok(_turn) = refreshing.try_lock() {
                    let due = held.read().await.1.elapsed() >= *min_refetch;
                    if due {
                        let fetched = fetch_jwks(client, url).await;
                        let mut h = held.write().await;
                        match fetched {
                            Ok(set) => *h = (set, Instant::now()),
                            Err(e) => {
                                eprintln!("nuthatch-gateway: keeping the keys held: {e}");
                                h.1 = Instant::now();
                            }
                        }
                    }
                }
                let h = held.read().await;
                jwt::verify(token, &h.0, &self.expect, now)
            }
        }
    }
}

fn refuse(status: StatusCode, code: &str, method: &Method, path: &str, extra: Value) -> Response {
    // The path, never the query string (it can carry SQL) and never a header (it carries the token).
    eprintln!("nuthatch-gateway: refused {code} {method} {path}");
    let mut body = json!({ "error": code });
    if let (Some(b), Value::Object(x)) = (body.as_object_mut(), extra) {
        b.extend(x);
    }
    let mut resp = (status, axum::Json(body)).into_response();
    let challenge = match (status, code) {
        (StatusCode::UNAUTHORIZED, "token_missing") => Some(r#"Bearer realm="nuthatch""#.into()),
        (StatusCode::UNAUTHORIZED, _) => Some(format!(
            r#"Bearer realm="nuthatch", error="invalid_token", error_description="{code}""#
        )),
        (StatusCode::FORBIDDEN, "scope_missing") => {
            Some(r#"Bearer realm="nuthatch", error="insufficient_scope""#.into())
        }
        _ => None,
    };
    if let Some(v) = challenge.and_then(|c| HeaderValue::from_str(&c).ok()) {
        resp.headers_mut().insert(header::WWW_AUTHENTICATE, v);
    }
    resp
}

fn bearer(headers: &HeaderMap) -> Result<&str, Refusal> {
    let mut all = headers.get_all(header::AUTHORIZATION).iter();
    let Some(value) = all.next() else {
        return Err(Refusal::Missing);
    };
    if all.next().is_some() {
        return Err(Refusal::Malformed);
    }
    let value = value.to_str().map_err(|_| Refusal::Malformed)?;
    match value.split_once(' ') {
        Some((scheme, token))
            if scheme.eq_ignore_ascii_case("bearer") && !token.trim().is_empty() =>
        {
            Ok(token.trim())
        }
        _ => Err(Refusal::Malformed),
    }
}

async fn handle(State(g): State<Arc<Gateway>>, req: Request) -> Response {
    let (parts, body) = req.into_parts();
    let (method, path) = (&parts.method, parts.uri.path());
    if path == "/_gateway/health" {
        return "ok".into_response();
    }
    if !policy::canonical(path) {
        return refuse(
            StatusCode::BAD_REQUEST,
            "path_not_canonical",
            method,
            path,
            json!({}),
        );
    }
    let nest = g.nests.iter().find(|n| {
        path.strip_prefix(n.prefix.as_str())
            .is_some_and(|rest| rest.is_empty() || rest.starts_with('/'))
    });
    let Some(nest) = nest else {
        return refuse(StatusCode::NOT_FOUND, "not_routed", method, path, json!({}));
    };
    let rest = match &path[nest.prefix.len()..] {
        "" => "/",
        r => r,
    };
    let need = policy::need(&nest.name, rest);
    if let Need::Refused(code) = need {
        let status = match code {
            "not_routed" => StatusCode::NOT_FOUND,
            _ => StatusCode::FORBIDDEN,
        };
        return refuse(status, code, method, path, json!({}));
    }
    let claims = match bearer(&parts.headers) {
        Ok(token) => g.verify(token).await,
        Err(e) => Err(e),
    };
    let held = match claims {
        Ok(c) => c.scopes(),
        Err(e) => return refuse(StatusCode::UNAUTHORIZED, e.code(), method, path, json!({})),
    };
    let Ok(bytes) = axum::body::to_bytes(body, MAX_BODY).await else {
        return refuse(
            StatusCode::PAYLOAD_TOO_LARGE,
            "body_too_large",
            method,
            path,
            json!({}),
        );
    };
    let whole = vec![format!("read:{}", nest.name)];
    let (needed, forwarded) = match need {
        Need::AnyOf(v) => (v, bytes),
        Need::Mcp if *method == Method::POST => match serde_json::from_slice::<Value>(&bytes) {
            // What is forwarded is what was judged: the parsed message, re-serialised, so no
            // difference between two JSON parsers (duplicate keys, say) can split the two.
            Ok(msg) => {
                let needed = policy::mcp(&nest.name, &msg);
                if !policy::granted(&held, &needed) {
                    eprintln!("nuthatch-gateway: refused scope_missing {method} {path}");
                    let error = json!({ "jsonrpc": "2.0", "id": msg.get("id").cloned().unwrap_or(Value::Null),
                        "error": { "code": -32001, "message": format!("scope_missing: this call needs one of {}", needed.join(", ")) } });
                    let mut resp = (StatusCode::FORBIDDEN, axum::Json(error)).into_response();
                    resp.headers_mut().insert(
                        header::WWW_AUTHENTICATE,
                        HeaderValue::from_static(
                            r#"Bearer realm="nuthatch", error="insufficient_scope""#,
                        ),
                    );
                    return resp;
                }
                (
                    needed,
                    Bytes::from(serde_json::to_vec(&msg).expect("a Value serialises")),
                )
            }
            // Not JSON: nuthatch answers it with a parse error, so only a whole-nest token gets there.
            Err(_) => (whole, bytes),
        },
        Need::Mcp => (whole, bytes),
        Need::Refused(_) => unreachable!("returned above"),
    };
    if !policy::granted(&held, &needed) {
        return refuse(
            StatusCode::FORBIDDEN,
            "scope_missing",
            method,
            path,
            json!({ "needs": needed }),
        );
    }
    forward(&g, parts, forwarded).await
}

/// RFC 9110 §7.6.1's connection-specific fields, which a proxy does not forward.
fn hop_by_hop(name: &HeaderName) -> bool {
    matches!(
        name.as_str(),
        "connection"
            | "keep-alive"
            | "proxy-authenticate"
            | "proxy-authorization"
            | "proxy-connection"
            | "te"
            | "trailer"
            | "transfer-encoding"
            | "upgrade"
    )
}

async fn forward(g: &Gateway, parts: axum::http::request::Parts, body: Bytes) -> Response {
    let target = parts
        .uri
        .path_and_query()
        .map(|p| p.as_str())
        .unwrap_or("/");
    let mut out = g
        .client
        .request(parts.method.clone(), format!("{}{target}", g.upstream));
    for (name, value) in &parts.headers {
        // The token stops here: nuthatch has no use for it, and would only log or echo it.
        let dropped = hop_by_hop(name)
            || *name == header::AUTHORIZATION
            || *name == header::HOST
            || *name == header::CONTENT_LENGTH;
        if !dropped {
            out = out.header(name, value);
        }
    }
    let resp = match out.body(body).send().await {
        Ok(r) => r,
        Err(e) => {
            // Never the URL: it carries the query string, which can be SQL.
            eprintln!(
                "nuthatch-gateway: upstream unreachable: {}",
                e.without_url()
            );
            return (
                StatusCode::BAD_GATEWAY,
                axum::Json(json!({ "error": "upstream_unreachable" })),
            )
                .into_response();
        }
    };
    let mut back = Response::builder().status(resp.status());
    for (name, value) in resp.headers() {
        if !hop_by_hop(name) && *name != header::CONTENT_LENGTH {
            back = back.header(name, value);
        }
    }
    back.body(Body::from_stream(resp.bytes_stream()))
        .unwrap_or_else(|_| StatusCode::BAD_GATEWAY.into_response())
}
